from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import Event, Thread
from time import monotonic, sleep

import pytest

from omi_collector.capture.adapters.collector_state_db import StateDB, StateKind
from omi_collector.capture.adapters.firmware_observations import (
    FirmwareObservationError,
    FirmwareObservationStore,
    FirmwareObservationWriter,
    parse_legacy_firmware,
    read_firmware_observations,
)
from omi_collector.capture.application.storage_fault import FatalInternalAudioStorageError
from omi_collector.capture.domain.ring_protocol import RingInfo
from omi_collector.config import FirmwareObservationConfig


def _info(dropped: int) -> RingInfo:
    return RingInfo(10, 20, 100, dropped, 444)


def _db(tmp_path: Path) -> StateDB:
    return StateDB(tmp_path / "collector-state.sqlite3")


def test_store_keeps_latest_snapshot_and_aggregates_counter_changes(tmp_path: Path) -> None:
    db = _db(tmp_path)
    store = FirmwareObservationStore(db)

    assert store.record(_info(4))
    changed = RingInfo(11, 21, 100, 4, 444)
    assert store.record(changed)
    assert store.record(changed) is False
    assert store.record(_info(7))
    assert store.record(_info(2))

    (observation,) = read_firmware_observations(db)
    assert observation.dropped_packets == 2
    assert observation.observation_count == 3
    assert observation.initial == 4
    assert observation.observed_increase == 3
    assert observation.regression_count == 1
    assert observation.epoch_count == 2
    assert observation.latest == 2
    assert observation.read_sequence == 10


def test_database_transaction_prevents_lost_concurrent_counter_updates(tmp_path: Path) -> None:
    db = _db(tmp_path)
    store = FirmwareObservationStore(db)
    with ThreadPoolExecutor(max_workers=8) as executor:
        results = tuple(executor.map(store.record, (_info(value) for value in range(1, 33))))

    assert all(results)
    (observation,) = read_firmware_observations(db)
    assert observation.observation_count == 32
    assert observation.dropped_packets in range(1, 33)


def test_reader_returns_empty_tuple_when_database_record_is_absent(tmp_path: Path) -> None:
    assert read_firmware_observations(_db(tmp_path)) == ()


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("schema_version", True),
        ("schema_version", 2),
        ("latest", []),
        ("metrics", []),
        ("latest", {"read_sequence": True}),
    ],
)
def test_reader_rejects_malformed_database_payload(tmp_path: Path, field: str, value: object) -> None:
    db = _db(tmp_path)
    FirmwareObservationStore(db).record(_info(1))
    payload = db.get(StateKind.FIRMWARE, "latest")
    assert payload is not None
    payload[field] = value
    db.put(StateKind.FIRMWARE, "latest", payload)

    with pytest.raises(FirmwareObservationError):
        read_firmware_observations(db)


def test_writer_persists_latest_observation_before_close_returns(tmp_path: Path) -> None:
    db = _db(tmp_path)
    writer = FirmwareObservationWriter(FirmwareObservationStore(db))
    writer.observe(_info(1))
    writer.observe(_info(2))
    writer.close()

    (observation,) = read_firmware_observations(db)
    assert observation.dropped_packets == 2


def test_writer_close_waits_for_inflight_database_write_and_final_mailbox_value(tmp_path: Path) -> None:
    db = _db(tmp_path)
    entered = Event()
    release = Event()
    closed = Event()

    class StalledStore(FirmwareObservationStore):
        first = True

        def record(self, info: RingInfo) -> bool:
            if self.first:
                self.first = False
                entered.set()
                assert release.wait(1)
            return super().record(info)

    writer = FirmwareObservationWriter(StalledStore(db), config=FirmwareObservationConfig(close_timeout_seconds=0.01))
    writer.observe(_info(1))
    assert entered.wait(1)
    writer.observe(_info(2))

    def close_writer() -> None:
        try:
            writer.close()
        finally:
            closed.set()

    closer = Thread(target=close_writer)
    closer.start()
    try:
        assert not closed.wait(0.05)
    finally:
        release.set()
        closer.join(1)

    assert not closer.is_alive()
    assert closed.is_set()
    (observation,) = read_firmware_observations(db)
    assert observation.dropped_packets == 2
    assert observation.observation_count == 2


def test_writer_retries_database_failure_and_reports_one_error_per_episode(tmp_path: Path) -> None:
    db = _db(tmp_path)
    errors: list[Exception] = []

    class FlakyStore(FirmwareObservationStore):
        failures = 2

        def record(self, info: RingInfo) -> bool:
            if self.failures:
                self.failures -= 1
                raise FirmwareObservationError("database busy")
            return super().record(info)

    writer = FirmwareObservationWriter(
        FlakyStore(db),
        config=FirmwareObservationConfig(retry_backoff_seconds=(0.001,), close_timeout_seconds=1),
        on_error=errors.append,
    )
    writer.observe(_info(3))
    deadline = monotonic() + 2
    while not read_firmware_observations(db) and monotonic() < deadline:
        sleep(0.005)
    writer.close()

    (observation,) = read_firmware_observations(db)
    assert observation.dropped_packets == 3
    assert len(errors) == 1


def test_writer_reports_corruption_once_and_does_not_retry(tmp_path: Path) -> None:
    fatal = FatalInternalAudioStorageError("canonical audio database is corrupt", sqlite_errorcode=11)
    reported = Event()
    calls: list[RingInfo] = []
    errors: list[Exception] = []

    class CorruptStore(FirmwareObservationStore):
        def record(self, info: RingInfo) -> bool:
            calls.append(info)
            raise fatal

    def on_error(error: Exception) -> None:
        errors.append(error)
        reported.set()

    writer = FirmwareObservationWriter(CorruptStore(_db(tmp_path)), on_error=on_error)
    writer.observe(_info(1))
    assert reported.wait(1)
    writer.observe(_info(2))
    writer.close()
    assert calls == [_info(1)]
    assert errors == [fatal]


def test_writer_replaces_stalled_mailbox_with_latest_observation(tmp_path: Path) -> None:
    db = _db(tmp_path)
    entered = Event()
    release = Event()

    class StalledStore(FirmwareObservationStore):
        first = True

        def record(self, info: RingInfo) -> bool:
            if self.first:
                self.first = False
                entered.set()
                assert release.wait(1)
            return super().record(info)

    writer = FirmwareObservationWriter(StalledStore(db), config=FirmwareObservationConfig(close_timeout_seconds=1))
    writer.observe(_info(1))
    assert entered.wait(1)
    writer.observe(_info(2))
    writer.observe(_info(3))
    release.set()
    writer.close()

    (observation,) = read_firmware_observations(db)
    assert observation.dropped_packets == 3
    assert observation.observation_count == 2


def test_legacy_parser_is_separate_from_runtime_database_reader(tmp_path: Path) -> None:
    legacy = tmp_path / "device.json"
    legacy.write_text(
        '{"latest":{"capacity_packets":100,"dropped_packets":3,"packet_size":444,'
        '"read_sequence":10,"write_sequence":20},"metrics":{"initial":3,'
        '"observation_count":1,"observed_increase":0,"regression_count":0},"schema_version":3}',
        encoding="utf-8",
    )
    (observation,) = parse_legacy_firmware(legacy)
    assert observation.dropped_packets == 3
