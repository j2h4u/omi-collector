from __future__ import annotations

import json
import threading
import time
from pathlib import Path
from typing import cast

import pytest

from omi_collector.capture.adapters.firmware_observations import (
    FirmwareObservationError,
    FirmwareObservationStore,
    FirmwareObservationWriter,
    read_firmware_observations,
)
from omi_collector.capture.domain.ring_protocol import RingInfo
from omi_collector.config import FirmwareObservationConfig


def _info(dropped: int) -> RingInfo:
    return RingInfo(10, 20, 100, dropped, 444)


_BOUNDS = {
    "read_sequence": (1 << 64) - 1,
    "write_sequence": (1 << 64) - 1,
    "capacity_packets": (1 << 32) - 1,
    "dropped_packets": (1 << 64) - 1,
    "packet_size": (1 << 16) - 1,
}


def _with_field(info: RingInfo, field: str, value: int) -> RingInfo:
    values = {
        "read_sequence": info.read_sequence,
        "write_sequence": info.write_sequence,
        "capacity_packets": info.capacity_packets,
        "dropped_packets": info.dropped_packets,
        "packet_size": info.packet_size,
    }
    values[field] = value
    return RingInfo(**values)


def test_store_keeps_latest_snapshot_and_aggregates_counter_changes(tmp_path: Path) -> None:
    store = FirmwareObservationStore(tmp_path / "device.json")

    assert store.record("omi", _info(4))
    assert store.record("omi", RingInfo(11, 21, 100, 4, 444))
    assert not store.record("omi", RingInfo(11, 21, 100, 4, 444))
    assert store.record("omi", _info(7))
    assert store.record("omi", _info(2))

    observations = read_firmware_observations(tmp_path / "device.json", "omi")
    assert [item.dropped_packets for item in observations] == [2]
    assert observations[0].observation_count == 3
    assert observations[0].observed_increase == 3
    assert observations[0].regression_count == 1
    assert observations[0].read_sequence == 10


@pytest.mark.parametrize(("field", "maximum"), _BOUNDS.items())
@pytest.mark.parametrize("offset", [-1, 1])
def test_store_and_observe_reject_firmware_values_outside_field_bounds(
    tmp_path: Path, field: str, maximum: int, offset: int
) -> None:
    info = _with_field(_info(1), field, maximum + offset if offset > 0 else offset)
    with pytest.raises(ValueError):
        FirmwareObservationStore(tmp_path / "device.json").record("omi", info)

    writer = FirmwareObservationWriter(FirmwareObservationStore(tmp_path / "device.json"))
    try:
        with pytest.raises(ValueError):
            writer.observe("omi", info)
    finally:
        writer.close()


@pytest.mark.parametrize(("field", "maximum"), _BOUNDS.items())
def test_store_and_observe_accept_firmware_field_limits(tmp_path: Path, field: str, maximum: int) -> None:
    info = _with_field(_info(1), field, maximum)
    assert FirmwareObservationStore(tmp_path / "device.json").record("omi", info)

    writer = FirmwareObservationWriter(FirmwareObservationStore(tmp_path / "writer" / "device.json"))
    try:
        writer.observe("omi", info)
    finally:
        writer.close()


@pytest.mark.parametrize(("field", "maximum"), _BOUNDS.items())
def test_reader_rejects_hash_valid_out_of_bounds_firmware_field(tmp_path: Path, field: str, maximum: int) -> None:
    store = FirmwareObservationStore(tmp_path / "device.json")
    store.record("omi", _info(1))
    path = tmp_path / "device.json"
    document = cast(dict[str, object], json.loads(path.read_text(encoding="utf-8")))
    observation = cast(dict[str, object], document["latest"])
    observation[field] = maximum + 1
    path.write_text(json.dumps(document, sort_keys=True, separators=(",", ":")), encoding="utf-8")
    with pytest.raises(FirmwareObservationError):
        read_firmware_observations(path, "omi")


def test_reader_rejects_corruption_fork_and_symlink(tmp_path: Path) -> None:
    store = FirmwareObservationStore(tmp_path / "device.json")
    store.record("omi", _info(1))
    store.record("omi", _info(2))
    path = tmp_path / "device.json"

    original = path.read_bytes()
    path.write_bytes(original + b"\n")
    with pytest.raises(FirmwareObservationError):
        read_firmware_observations(path, "omi")
    path.write_bytes(original)

    document = cast(dict[str, object], json.loads(path.read_text(encoding="utf-8")))
    cast(dict[str, object], document["metrics"])["observation_count"] = 0
    path.write_text(json.dumps(document, sort_keys=True, separators=(",", ":")), encoding="utf-8")
    with pytest.raises(FirmwareObservationError):
        read_firmware_observations(path, "omi")

    path.unlink()
    path.symlink_to(tmp_path / "missing")
    with pytest.raises(FirmwareObservationError):
        read_firmware_observations(path, "omi")


def test_writer_replaces_stalled_mailbox_and_retries(tmp_path: Path) -> None:
    class SlowStore(FirmwareObservationStore):
        def __init__(self) -> None:
            super().__init__(tmp_path / "device.json")
            self.started = threading.Event()
            self.release = threading.Event()
            self.calls: list[int] = []
            self.failures = 1

        def record(self, device_slug: str, info: RingInfo) -> bool:
            self.calls.append(info.dropped_packets)
            self.started.set()
            if not self.release.wait(1):
                raise TimeoutError("test stall")
            if self.failures:
                self.failures -= 1
                raise OSError("test failure")
            return super().record(device_slug, info)

    store = SlowStore()
    errors: list[Exception] = []
    writer = FirmwareObservationWriter(store, on_error=errors.append)
    writer.observe("omi", _info(1))
    assert store.started.wait(1)
    writer.observe("omi", _info(2))
    writer.observe("omi", _info(3))
    store.release.set()
    deadline = time.monotonic() + 2
    while len(store.calls) < 3 and time.monotonic() < deadline:
        time.sleep(0.01)
    writer.close()
    assert store.calls == [1, 3]
    assert errors


def test_writer_keeps_stalled_observations_for_each_device(tmp_path: Path) -> None:
    class SlowStore(FirmwareObservationStore):
        def __init__(self) -> None:
            super().__init__(tmp_path / "device.json")
            self.started = threading.Event()
            self.release = threading.Event()
            self.calls: list[tuple[str, int]] = []

        def record(self, device_slug: str, info: RingInfo) -> bool:
            self.calls.append((device_slug, info.dropped_packets))
            if len(self.calls) == 1:
                self.started.set()
                assert self.release.wait(1)
            return True

    store = SlowStore()
    writer = FirmwareObservationWriter(store)
    writer.observe("omi-a", _info(1))
    assert store.started.wait(1)
    writer.observe("omi-b", _info(2))
    store.release.set()
    deadline = time.monotonic() + 2
    while len(store.calls) < 2 and time.monotonic() < deadline:
        time.sleep(0.01)
    writer.close()

    assert store.calls == [("omi-a", 1), ("omi-b", 2)]


def test_writer_reports_one_error_per_failure_episode(tmp_path: Path) -> None:
    class SequencedStore(FirmwareObservationStore):
        def __init__(self) -> None:
            super().__init__(tmp_path / "device.json")
            self.calls: list[int] = []
            self.outcomes: list[Exception | None] = [
                OSError("first"),
                OSError("retry"),
                None,
                OSError("new"),
                OSError("new retry"),
            ]

        def record(self, device_slug: str, info: RingInfo) -> bool:
            self.calls.append(info.dropped_packets)
            outcome = self.outcomes.pop(0)
            if outcome is not None:
                raise outcome
            return super().record(device_slug, info)

    store = SequencedStore()
    errors: list[Exception] = []
    writer = FirmwareObservationWriter(
        store,
        config=FirmwareObservationConfig(retry_backoff_seconds=(0.01,), close_timeout_seconds=0.2),
        on_error=errors.append,
    )
    writer.observe("omi", _info(1))
    deadline = time.monotonic() + 2
    while len(store.calls) < 3 and time.monotonic() < deadline:
        time.sleep(0.01)
    assert store.calls[:3] == [1, 1, 1]
    assert len(errors) == 1

    writer.observe("omi", _info(2))
    deadline = time.monotonic() + 2
    while len(store.calls) < 5 and time.monotonic() < deadline:
        time.sleep(0.01)
    assert store.calls[:5] == [1, 1, 1, 2, 2]
    assert len(errors) == 2
    writer.close()


def test_writer_close_is_bounded_when_store_stalls(tmp_path: Path) -> None:
    entered = threading.Event()
    release = threading.Event()

    class BlockingStore(FirmwareObservationStore):
        def record(self, device_slug: str, info: RingInfo) -> bool:
            entered.set()
            release.wait(1)
            return bool(device_slug) and info.packet_size >= 0

    writer = FirmwareObservationWriter(BlockingStore(tmp_path / "device.json"))
    writer.observe("omi", _info(1))
    assert entered.wait(1)
    started = time.monotonic()
    writer.close()
    assert time.monotonic() - started < 0.8
    release.set()
