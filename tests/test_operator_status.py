from __future__ import annotations

import json
import sqlite3
import threading
from collections.abc import Callable
from contextlib import closing
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path
from typing import cast

import pytest

import omi_collector.operator_status as status_module
from omi_collector.capture.adapters.collector_state_db import StateDB, StateKind
from omi_collector.capture.adapters.confirmed_loss import ConfirmedLossLedger, read_confirmed_losses
from omi_collector.capture.adapters.debug_logging import (
    write_runtime_status_event,
)
from omi_collector.capture.adapters.firmware_observations import FirmwareObservationStore
from omi_collector.capture.adapters.internal_audio_store import AUDIO_RECORD_SIZE, DrainCheckpoint, InternalAudioStore
from omi_collector.capture.adapters.operational_status import OperationalIdentity, OperationalStatusStore
from omi_collector.capture.adapters.quality_metrics import (
    QualityMetricsError,
    SQLiteQualityMetrics,
    import_quality_events,
    quality_event_id,
)
from omi_collector.capture.adapters.ready_bundles import (
    PublicAudioV2Options,
    SqlitePublicationSource,
    finalize_public_audio_v2,
)
from omi_collector.capture.application.quality_metrics import (
    AdvertisementMetric,
    ClockCorrectionMetric,
    SequenceLossMetric,
    TransferSessionMetric,
)
from omi_collector.capture.application.storage_fault import (
    FatalInternalAudioStorageError,
    TransientInternalAudioStorageError,
)
from omi_collector.capture.domain.clock_state import ClockState
from omi_collector.capture.domain.operational_status_machine import OperationalDimension, OperationalSignal
from omi_collector.capture.domain.ring_protocol import RECORD_SIZE, RingInfo
from omi_collector.config import QualityMetricsConfig, ReadyConfig
from omi_collector.operator_status import OperatorStatusError, collect_operator_status
from omi_collector.spool_metrics import FirmwareLifetimeMetrics, SpoolMetrics, SpoolWindowMetrics
from omi_collector.state_migration import open_runtime_state
from omi_collector.storage_layout import StorageLayout, load_operator_config


def test_legacy_debug_parser_accepts_records_and_rejects_malformed_payload(tmp_path: Path) -> None:
    path = tmp_path / "debug.jsonl"
    row = {"timestamp": "2026-01-01T00:00:00+00:00", "event": "ble_link_rssi_observed", "fields": {"rssi_dbm": -71}}
    path.write_text(json.dumps(row) + "\n", encoding="utf-8")
    assert status_module._debug_rows(path) == [row]
    assert status_module._decode_connection_rssi(row) == (datetime(2026, 1, 1, tzinfo=UTC), -71)

    path.write_text("{malformed}\n", encoding="utf-8")
    with pytest.raises(OperatorStatusError, match="malformed JSON"):
        status_module._debug_rows(path)
    path.write_text("[]\n", encoding="utf-8")
    with pytest.raises(OperatorStatusError, match="not an object"):
        status_module._debug_rows(path)


def test_legacy_rssi_projection_rejects_malformed_observation() -> None:
    baseline = {"timestamp": "2026-01-01T00:00:00+00:00", "event": "ble_link_rssi_observed"}
    assert status_module._decode_connection_rssi({**baseline, "event": "other"}) is None
    assert status_module._decode_connection_rssi({**baseline, "fields": {"rssi_dbm": None}}) is None
    for fields in (None, {"rssi_dbm": True}, {"rssi_dbm": "-70"}):
        with pytest.raises(OperatorStatusError, match="invalid"):
            status_module._decode_connection_rssi({**baseline, "fields": fields})


def _layout(tmp_path: Path) -> StorageLayout:
    path = tmp_path / "config.toml"
    path.write_text(
        'pendant_address = "AA:BB:CC:DD:EE:FF"\nbundle_min_audio_seconds = 3600.0\n',
        encoding="utf-8",
    )
    (tmp_path / "collector").mkdir()
    return load_operator_config(path).storage


def _spool() -> SpoolMetrics:
    return SpoolMetrics(
        SpoolWindowMetrics(2, 30, 13_320, 0, 0, 0.0),
        FirmwareLifetimeMetrics(1, 0, 0, 0, 0, 1),
    )


def _state_db(layout: StorageLayout) -> StateDB:
    if not layout.collector.state_db.exists():
        database = StateDB(layout.collector.state_db)
        database.put(StateKind.MIGRATION, "internal_audio_cutover", {"format": 1, "phase": "COMPLETE", "fresh": True})
    return open_runtime_state(layout)


def test_operator_status_reads_ready_metadata_without_opening_audio(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    layout = _layout(tmp_path)
    layout.publication.root.mkdir(parents=True)
    layout.publication.root.chmod(0o2750)
    database = _state_db(layout)
    store = InternalAudioStore(database)
    raw = (10).to_bytes(4, "big") + bytes((2, 8, 1)) + bytes(AUDIO_RECORD_SIZE - 7)
    source_id = "a" * 32
    store.create_source(source_id, "attempt", 10, 11)
    source = store.prepare_leg(source_id, 10, 1)
    source = store.record_read_begin(source_id, 10, 1)
    store.commit_drain(source_id, 0, (raw,), DrainCheckpoint(source.current_leg_generation))
    source = store.get_source(source_id)
    assert source is not None
    store.seal_source(
        source_id,
        leg_generation=source.current_leg_generation,
        expected_count=source.committed_count,
        expected_sha256=source.committed_sha256,
    )
    outcome = finalize_public_audio_v2(
        store,
        (SqlitePublicationSource(store, source_id),),
        layout.publication.root,
        PublicAudioV2Options(ReadyConfig(target_audio_seconds=0.02), ClockState(), 11, True, 128),
    )
    assert len(outcome.published) == 1
    bundle = outcome.published[0].path
    ledger = database.get(StateKind.READY_PUBLICATION, bundle.name)
    assert ledger is not None and ledger["format"] == "omi-audio-v2"
    assert cast(int, ledger["packet_count"]) > 0
    original_open = Path.open

    def guarded_open(path: Path, *args: object, **kwargs: object) -> object:
        if path.name == "audio.ogg":
            raise AssertionError("operator status must not open audio payloads")
        return cast(Callable[..., object], original_open)(path, *args, **kwargs)

    monkeypatch.setattr(Path, "open", guarded_open)

    status = collect_operator_status(layout, hours=24, now=datetime(2026, 9, 8, 10, tzinfo=UTC))

    publication = cast(dict[str, object], status["publication"])
    physical_records = cast(int, ledger["next_sequence"]) - cast(int, ledger["start_sequence"])
    assert publication["bundle_count"] == 1
    assert publication["downloaded_records"] == physical_records
    assert publication["downloaded_raw_bytes"] == physical_records * RECORD_SIZE
    store.close()


def test_operator_status_reads_committed_state_during_runtime_write(tmp_path: Path) -> None:
    layout = _layout(tmp_path)
    layout.publication.root.mkdir(parents=True)
    db = _state_db(layout)
    _operational_status(layout, {})
    db.put(StateKind.CLOCK, "current", {"status": "old"})
    writer_ready = threading.Event()
    finish_write = threading.Event()

    def write_uncommitted_state() -> None:
        with db.transaction() as transaction:
            transaction.put(StateKind.CLOCK, "current", {"status": "new"})
            writer_ready.set()
            if not finish_write.wait(5):
                raise TimeoutError("test writer was not released")

    thread = threading.Thread(target=write_uncommitted_state)
    thread.start()
    try:
        assert writer_ready.wait(5)
        status = collect_operator_status(layout, hours=24, now=datetime(2026, 9, 8, 10, tzinfo=UTC))
        assert isinstance(status, dict)
        with closing(sqlite3.connect(layout.collector.state_db)) as connection:
            assert connection.execute("SELECT payload FROM state WHERE kind='clock' AND id='current'").fetchone() == (
                '{"status":"old"}',
            )
    finally:
        finish_write.set()
        thread.join(5)
    assert not thread.is_alive()
    assert db.get(StateKind.CLOCK, "current") == {"status": "new"}


def test_operator_status_marks_open_ble_scan_incident_as_attention(tmp_path: Path) -> None:
    layout = _layout(tmp_path)
    layout.publication.root.mkdir()
    db = _state_db(layout)
    db.put(
        StateKind.BLE_SCAN_INCIDENT,
        "open",
        {
            "incident_id": "ble-scan-test",
            "detected_at": "2026-09-08T10:00:00+00:00",
            "status": "open",
            "context": {
                "reason": "scan_silence",
                "active_seconds": 180.0,
                "bleak_callbacks": 0,
                "last_callback_age_seconds": None,
                "threshold_seconds": 180.0,
            },
            "diagnostics": {
                "hci": {"status": "degraded", "failure": "cap_net_raw_unavailable", "counts": {}},
                "bluez": {"available": True, "powered": True, "discovering": False, "devices_count": 0},
            },
        },
    )

    status = collect_operator_status(layout, hours=24, now=datetime(2026, 9, 8, 10, tzinfo=UTC))

    assert status["status"] == "attention"
    incident = cast(dict[str, object], status["ble_scan_incident"])
    assert incident["incident_id"] == "ble-scan-test"
    assert incident["fault_category"] == "host_ble_receive_unavailable"
    assert cast(dict[str, object], incident["bluez"])["devices_count"] == 0
    assert "diagnostics" not in incident


def _put_quality_events(layout: StorageLayout, events: tuple[dict[str, object], ...] | list[dict[str, object]]) -> None:
    db = _state_db(layout)
    rows = tuple((quality_event_id(event, suffix=f"{index:08d}"), event) for index, event in enumerate(events))
    import_quality_events(db, rows)


def _put_raw_quality_event(layout: StorageLayout, event: dict[str, object]) -> None:
    db = _state_db(layout)
    timestamp_key = str(event.get("recorded_at", event.get("completed_at", event.get("occurred_at", ""))))
    sortable = datetime.fromisoformat(timestamp_key).astimezone(UTC).strftime("%Y%m%d%H%M%S%f")
    db.put(StateKind.QUALITY_EVENT, f"{sortable}|malformed", event)


def _put_runtime_events(layout: StorageLayout, rows: tuple[dict[str, object], ...] | list[dict[str, object]]) -> None:
    db = _state_db(layout)
    for row in rows:
        if row.get("event") in {"sync_progress", "ble_link_rssi_observed"}:
            write_runtime_status_event(
                db, str(row["event"]), cast(dict[str, object], row["fields"]), str(row["timestamp"])
            )


def _put_raw_runtime_projection(layout: StorageLayout, projection: dict[str, object]) -> None:
    _state_db(layout).put(StateKind.RUNTIME_STATUS, "current", projection)


def _operational_status(
    layout: StorageLayout, states: dict[OperationalDimension, OperationalSignal]
) -> OperationalIdentity:
    identity = OperationalIdentity("00000000-0000-0000-0000-000000000001", "00000000000000000000000000000001")
    db = _state_db(layout)
    ConfirmedLossLedger(db).initialize(allow_create=True)
    store = OperationalStatusStore(db, identity)
    store.initialize()
    for dimension, signal in states.items():
        store.update(dimension, signal)
    return identity


def test_confirmed_loss_status_unions_intervals_and_deduplicates_keyed_projections(tmp_path: Path) -> None:
    layout = _layout(tmp_path)
    ledger = ConfirmedLossLedger(_state_db(layout))
    ledger.initialize(allow_create=True)
    first_id = ledger.record("0123456789abcdef0123456789abcdef", 100, 110, "2026-09-08T09:30:00+00:00")
    extended_id = ledger.record("0123456789abcdef0123456789abcdef", 105, 115, "2026-09-08T09:31:00+00:00")
    second_id = ledger.record("1123456789abcdef0123456789abcdef", 105, 115, "2026-09-08T09:30:00+00:00")
    rows = [
        SequenceLossMetric(
            "2026-09-08T09:30:00+00:00",
            "session-1",
            10,
            10 * RECORD_SIZE,
            "device_cursor_advanced_before_host_durable_prefix",
            "1.2.3",
            None,
            None,
            first_id,
        ).as_dict(),
        SequenceLossMetric(
            "2026-09-08T09:31:00+00:00",
            "session-1",
            10,
            10 * RECORD_SIZE,
            "device_cursor_advanced_before_host_durable_prefix",
            "1.2.3",
            None,
            None,
            extended_id,
        ).as_dict(),
        SequenceLossMetric(
            "2026-09-08T09:30:00+00:00",
            "session-2",
            10,
            10 * RECORD_SIZE,
            "device_cursor_advanced_before_host_durable_prefix",
            "1.2.3",
            None,
            None,
            second_id,
        ).as_dict(),
        SequenceLossMetric(
            "2026-09-08T09:30:00+00:00",
            "session-legacy",
            3,
            3 * RECORD_SIZE,
            "legacy loss",
            "1.2.3",
            None,
            None,
        ).as_dict(),
    ]
    _put_quality_events(layout, rows)

    quality = status_module._quality_window(
        _state_db(layout),
        datetime(2026, 9, 8, 9, tzinfo=UTC),
        datetime(2026, 9, 8, 10, tzinfo=UTC),
        confirmed_losses=read_confirmed_losses(_state_db(layout)),
    )

    assert quality.loss_events == 3
    assert quality.missing_records == 28
    assert quality.missing_raw_bytes == 28 * RECORD_SIZE


def test_durable_loss_remains_visible_without_degrading_clear_status(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    layout = _layout(tmp_path)
    ledger = ConfirmedLossLedger(_state_db(layout))
    ledger.initialize(allow_create=True)
    ledger.record("0123456789abcdef0123456789abcdef", 100, 110, "2026-09-08T09:30:00+00:00")
    assert len(read_confirmed_losses(_state_db(layout))) == 1
    _operational_status(
        layout,
        {
            OperationalDimension.QUALITY: OperationalSignal.CLEAR,
            OperationalDimension.PUBLICATION: OperationalSignal.CLEAR,
            OperationalDimension.CLOCK: OperationalSignal.CLEAR,
        },
    )
    monkeypatch.setattr(status_module, "collect_spool_metrics", lambda *_args, **_kwargs: _spool())
    monkeypatch.setattr(status_module, "read_firmware_observations", lambda *_args, **_kwargs: ())
    _put_quality_events(
        layout,
        [_transfer("2026-09-08T09:45:00+00:00", outcome="ok", termination_class="completed", written_raw_bytes=444)],
    )

    result = collect_operator_status(
        layout,
        hours=24,
        now=datetime(2026, 9, 8, 10, tzinfo=UTC),
        identity=OperationalIdentity("00000000-0000-0000-0000-000000000001", "00000000000000000000000000000001"),
    )

    assert result["status"] == "ok"
    quality = cast(dict[str, object], result["quality_window"])
    assert quality["confirmed_loss_events"] == 1
    assert quality["confirmed_lost_records"] == 10
    assert quality["confirmed_lost_raw_bytes"] == 10 * RECORD_SIZE


def test_status_counts_persistent_custody_holds_without_changing_health(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    layout = _layout(tmp_path)
    db = _state_db(layout)
    monkeypatch.setattr(status_module, "collect_spool_metrics", lambda *_args, **_kwargs: _spool())
    now = datetime(2026, 9, 8, 10, tzinfo=UTC)
    assert collect_operator_status(layout, hours=24, now=now)["custody_holds"] == 0

    with InternalAudioStore(db) as store:
        store.create_source("a" * 32, "attempt", 100, 101)
        store.create_source("b" * 32, "attempt", 101, 102)
    with closing(db.open_connection()) as connection:
        connection.execute(
            "UPDATE capture_source SET source_type='quarantine', lifecycle='blocked', "
            "block_reason='ready_overlap_conflict' WHERE source_id=?",
            ("a" * 32,),
        )
        connection.execute(
            "UPDATE capture_source SET source_type='quarantine', lifecycle='blocked', "
            "block_reason='source_authentication_mismatch' WHERE source_id=?",
            ("b" * 32,),
        )

    result = collect_operator_status(layout, hours=24, now=now)
    assert result["custody_holds"] == 2
    assert result["status"] == "unknown"


@pytest.mark.parametrize(
    ("sqlite_errorcode", "error_type"),
    (
        (sqlite3.SQLITE_CORRUPT, FatalInternalAudioStorageError),
        (sqlite3.SQLITE_NOTADB, FatalInternalAudioStorageError),
        (sqlite3.SQLITE_BUSY, TransientInternalAudioStorageError),
    ),
)
def test_custody_hold_inventory_classifies_raw_sqlite_fault(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, sqlite_errorcode: int, error_type: type[Exception]
) -> None:
    db = StateDB(tmp_path / "state.sqlite3")
    failure = sqlite3.DatabaseError("private SQLite detail")
    failure.sqlite_errorcode = sqlite_errorcode

    def fail_open(_self: StateDB, **_kwargs: object) -> None:
        raise failure

    monkeypatch.setattr(StateDB, "open_connection", fail_open)
    with pytest.raises(error_type) as raised:
        status_module._custody_hold_count(db)
    assert raised.value.sqlite_errorcode == sqlite_errorcode  # type: ignore[attr-defined]
    assert "private SQLite detail" not in str(raised.value)


def test_confirmed_loss_with_blocked_publication_remains_attention(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    layout = _layout(tmp_path)
    ledger = ConfirmedLossLedger(_state_db(layout))
    ledger.initialize(allow_create=True)
    ledger.record("0123456789abcdef0123456789abcdef", 100, 110, "2026-09-08T09:30:00+00:00")
    identity = _operational_status(layout, {OperationalDimension.PUBLICATION: OperationalSignal.BLOCK})
    monkeypatch.setattr(status_module, "collect_spool_metrics", lambda *_args, **_kwargs: _spool())
    monkeypatch.setattr(status_module, "read_firmware_observations", lambda *_args, **_kwargs: ())
    _put_quality_events(
        layout,
        [_transfer("2026-09-08T09:45:00+00:00", outcome="ok", termination_class="completed", written_raw_bytes=444)],
    )

    result = collect_operator_status(
        layout,
        hours=24,
        now=datetime(2026, 9, 8, 10, tzinfo=UTC),
        identity=identity,
    )

    assert result["status"] == "attention"
    quality = cast(dict[str, object], result["quality_window"])
    assert quality["confirmed_loss_events"] == 1
    assert quality["confirmed_lost_records"] == 10
    assert quality["confirmed_lost_raw_bytes"] == 10 * RECORD_SIZE


def test_status_fails_closed_when_initialized_loss_ledger_is_missing(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    layout = _layout(tmp_path)
    _operational_status(layout, {})
    _state_db(layout).delete(StateKind.CONFIRMED_LOSS, "ledger")
    monkeypatch.setattr(status_module, "collect_spool_metrics", lambda *_args, **_kwargs: _spool())
    monkeypatch.setattr(status_module, "read_firmware_observations", lambda *_args, **_kwargs: ())

    with pytest.raises(OperatorStatusError, match="missing"):
        collect_operator_status(layout, hours=24, now=datetime(2026, 9, 8, 10, tzinfo=UTC))


def _advertisement(timestamp: str) -> dict[str, object]:
    return AdvertisementMetric(timestamp, "session-1", -91, "1.2.3", "abcdef123456", "auto").as_dict()


def _transfer(timestamp: str, *, outcome: str, termination_class: str, written_raw_bytes: int) -> dict[str, object]:
    return TransferSessionMetric(
        timestamp,
        f"session-{timestamp[-2:]}",
        outcome,
        termination_class,
        2_000 if termination_class == "completed" else 1_000,
        10,
        written_raw_bytes,
        written_raw_bytes,
        written_raw_bytes,
        "1.2.3",
        "abcdef123456",
        "1.0.0",
        "auto",
        -91,
    ).as_dict()


def _loss(timestamp: str) -> dict[str, object]:
    return SequenceLossMetric(
        timestamp,
        "session-loss",
        2,
        888,
        "device_cursor_advanced_before_host_durable_prefix",
        "1.2.3",
        "abcdef123456",
        "1.0.0",
    ).as_dict()


def _clock_correction(timestamp: str) -> dict[str, object]:
    return ClockCorrectionMetric(
        timestamp,
        "session-clock",
        2359.68,
        1789128032,
        5898589,
        5898591,
        "1.2.3",
        "abcdef123456",
        "1.0.0",
    ).as_dict()


def test_status_summarizes_backlog_transfer_quality_and_loss(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    layout = _layout(tmp_path)
    FirmwareObservationStore(_state_db(layout)).record(RingInfo(10, 25, 100, 2, RECORD_SIZE))
    events = (
        _advertisement("2026-09-08T09:00:00+00:00"),
        _transfer(
            "2026-09-08T09:05:00+00:00", outcome="collected", termination_class="completed", written_raw_bytes=4_440
        ),
        _transfer(
            "2026-09-08T09:10:00+00:00",
            outcome="connected_interrupted",
            termination_class="retryable_error",
            written_raw_bytes=444,
        ),
        _loss("2026-09-08T09:15:00+00:00"),
        _clock_correction("2026-09-08T09:20:00+00:00"),
    )
    _put_quality_events(layout, events)
    monkeypatch.setattr(status_module, "collect_spool_metrics", lambda *_args, **_kwargs: _spool())

    result = collect_operator_status(layout, hours=24, now=datetime(2026, 9, 8, 10, tzinfo=UTC))

    assert result["status"] == "unknown"
    quality = cast(dict[str, object], result["quality_window"])
    assert quality["clock_corrections"] == 1
    assert quality["last_clock_correction_at"] == "2026-09-08T09:20:00.000+00:00"
    assert quality["last_clock_correction_drift_seconds"] == 2359.68
    assert result["device"] == {
        "capacity_packets": 100,
        "dropped_packets": 2,
        "read_sequence": 10,
        "unread_bytes": 6_660,
        "unread_packets": 15,
        "write_sequence": 25,
    }
    assert result["runtime"] == {
        "attention_reasons": [],
        "battery_observed_at": None,
        "battery_percent": None,
        "connection_rssi_dbm": None,
        "connection_rssi_observed_at": None,
        "firmware": None,
        "last_error": None,
        "last_link_observation": None,
        "last_observed_phy": None,
        "link_observation_state": {"state": "unknown", "observed_at": None, "reason": None},
        "operational_status": {"clock": "unknown", "publication": "unknown", "quality": "unknown"},
        "state": "unknown",
        "transfer": None,
        "updated_age_seconds": None,
        "updated_at": None,
    }
    assert result["schema_version"] == 2
    assert result["publication"] == _spool().current_window.as_dict()
    assert result["quality_window"] == {
        "advertisements": 1,
        "completed_transfers": 1,
        "confirmed_loss_events": 1,
        "confirmed_lost_raw_bytes": 888,
        "confirmed_lost_records": 2,
        "clock_corrections": 1,
        "last_advertisement_at": "2026-09-08T09:00:00.000+00:00",
        "last_advertisement_rssi_dbm": -91,
        "last_clock_correction_at": "2026-09-08T09:20:00.000+00:00",
        "last_clock_correction_drift_seconds": 2359.68,
        "last_successful_transfer_at": "2026-09-08T09:05:00.000+00:00",
        "last_transfer_at": "2026-09-08T09:10:00.000+00:00",
        "last_transfer_outcome": "connected_interrupted",
        "last_transfer_termination_class": "retryable_error",
        "pooled_written_bytes_per_second": 1628.0,
        "retryable_transfers": 1,
        "transfer_outcomes": {"collected": 1, "connected_interrupted": 1},
        "transfer_sessions": 2,
        "transfer_termination_classes": {"completed": 1, "retryable_error": 1},
        "written_raw_bytes": 4884,
    }


def test_status_marks_persistent_runtime_failures_for_attention(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    layout = _layout(tmp_path)
    rows = (
        {"event": "quality_metrics_configuration_error", "fields": {}, "timestamp": "2026-09-08T09:00:00+00:00"},
        {"event": "ready_publication_blocked", "fields": {}, "timestamp": "2026-09-08T09:01:00+00:00"},
        {
            "event": "sync_progress",
            "fields": {
                "progress": {
                    "event": "pendant_clock_sync",
                    "outcome": "intent_persist_failed",
                    "status": "operational",
                }
            },
            "timestamp": "2026-09-08T09:02:00+00:00",
        },
    )
    _put_runtime_events(layout, rows)
    identity = _operational_status(
        layout,
        {
            OperationalDimension.QUALITY: OperationalSignal.BLOCK,
            OperationalDimension.PUBLICATION: OperationalSignal.BLOCK,
            OperationalDimension.CLOCK: OperationalSignal.BLOCK,
        },
    )
    monkeypatch.setattr(status_module, "collect_spool_metrics", lambda *_args, **_kwargs: _spool())

    result = collect_operator_status(layout, hours=24, now=datetime(2026, 9, 8, 10, tzinfo=UTC), identity=identity)

    assert result["status"] == "attention"
    runtime = cast(dict[str, object], result["runtime"])
    assert runtime["attention_reasons"] == [
        "clock_correction_blocked",
        "quality_metrics_unavailable",
        "ready_publication_blocked",
    ]


def test_status_omits_legacy_lock_context_from_session_error(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    layout = _layout(tmp_path)
    context = {
        "requested_operation": "capture_batch",
        "holder_operation": "quarantine_pending",
        "holder_pid": 321,
        "holder_thread_id": 654,
        "holder_age_seconds": 1.25,
        "holder_scope": "other_process",
        "metadata_status": "valid",
    }
    row = {
        "event": "sync_progress",
        "fields": {
            "progress": {
                "status": "session_error",
                "phase": "read/reconcile",
                "error_type": "DeviceAlreadyRunningError",
                "error_message": "pendant recovery is already active",
                "lock_context": context,
            }
        },
        "timestamp": "2026-09-08T09:00:00+00:00",
    }
    _put_runtime_events(layout, [row])
    monkeypatch.setattr(status_module, "collect_spool_metrics", lambda *_args, **_kwargs: _spool())

    result = collect_operator_status(layout, hours=24, now=datetime(2026, 9, 8, 10, tzinfo=UTC))

    runtime = cast(dict[str, object], result["runtime"])
    error = cast(dict[str, object], runtime["last_error"])
    assert error["lock_context"] is None
    assert error["error_message"] is None
    assert error["error_type"] == "DeviceAlreadyRunningError"
    assert error["phase"] == "read/reconcile"


def test_status_clears_ready_publication_block_after_successful_noop_recovery(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    layout = _layout(tmp_path)
    rows = (
        {"event": "ready_publication_blocked", "fields": {}, "timestamp": "2026-09-08T09:00:00+00:00"},
        {"event": "ready_publication_recovered", "fields": {}, "timestamp": "2026-09-08T09:01:00+00:00"},
    )
    _put_runtime_events(layout, rows)
    monkeypatch.setattr(status_module, "collect_spool_metrics", lambda *_args, **_kwargs: _spool())

    result = collect_operator_status(layout, hours=24, now=datetime(2026, 9, 8, 10, tzinfo=UTC))

    runtime = cast(dict[str, object], result["runtime"])
    assert runtime["attention_reasons"] == []
    assert result["status"] == "unknown"


def test_status_treats_expected_pendant_absence_as_unknown_without_completed_clear_evidence(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    layout = _layout(tmp_path)
    row = {
        "event": "sync_progress",
        "fields": {"progress": {"status": "away"}},
        "timestamp": "2026-09-08T09:00:00+00:00",
    }
    _put_runtime_events(layout, [row])
    monkeypatch.setattr(status_module, "collect_spool_metrics", lambda *_args, **_kwargs: _spool())

    result = collect_operator_status(layout, hours=24, now=datetime(2026, 9, 8, 10, tzinfo=UTC))

    assert result["status"] == "unknown"
    assert cast(dict[str, object], result["runtime"])["state"] == "away"


def test_status_is_ok_only_after_a_completed_transfer_and_all_current_dimensions_clear(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    layout = _layout(tmp_path)
    identity = _operational_status(
        layout,
        dict.fromkeys(OperationalDimension, OperationalSignal.CLEAR),
    )
    _put_quality_events(
        layout,
        [
            _transfer(
                "2026-09-08T09:05:00+00:00",
                outcome="collected",
                termination_class="completed",
                written_raw_bytes=4_440,
            )
        ],
    )
    monkeypatch.setattr(status_module, "collect_spool_metrics", lambda *_args, **_kwargs: _spool())

    result = collect_operator_status(layout, hours=24, now=datetime(2026, 9, 8, 10, tzinfo=UTC), identity=identity)

    assert result["status"] == "ok"


def test_debug_log_history_cannot_override_current_snapshot_state(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    layout = _layout(tmp_path)
    identity = _operational_status(
        layout,
        dict.fromkeys(OperationalDimension, OperationalSignal.CLEAR),
    )
    row = {
        "event": "quality_metrics_configuration_error",
        "fields": {},
        "timestamp": "2026-09-08T09:00:00+00:00",
    }
    _put_runtime_events(layout, [row])
    monkeypatch.setattr(status_module, "collect_spool_metrics", lambda *_args, **_kwargs: _spool())

    result = collect_operator_status(layout, hours=24, now=datetime(2026, 9, 8, 10, tzinfo=UTC), identity=identity)

    runtime = cast(dict[str, object], result["runtime"])
    assert runtime["attention_reasons"] == []
    assert result["status"] == "unknown"


def test_status_rejects_malformed_quality_evidence(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    layout = _layout(tmp_path)
    _put_raw_quality_event(layout, {"event": "malformed", "occurred_at": "2026-09-08T09:00:00+00:00"})
    monkeypatch.setattr(status_module, "collect_spool_metrics", lambda *_args, **_kwargs: _spool())

    with pytest.raises(OperatorStatusError, match="quality event history is invalid"):
        collect_operator_status(layout, hours=24)


def test_status_accepts_quality_record_without_deployment_revision(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    layout = _layout(tmp_path)
    event = _advertisement("2026-09-08T09:00:00+00:00")
    event["source_revision"] = None
    _put_quality_events(layout, [event])
    monkeypatch.setattr(status_module, "collect_spool_metrics", lambda *_args, **_kwargs: _spool())

    result = collect_operator_status(layout, hours=24, now=datetime(2026, 9, 8, 10, tzinfo=UTC))

    quality = cast(dict[str, object], result["quality_window"])
    assert quality["advertisements"] == 1


def test_status_uses_current_runtime_projection(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    layout = _layout(tmp_path)
    row = {
        "event": "sync_progress",
        "fields": {"progress": {"status": "away"}},
        "timestamp": "2026-09-08T09:00:00+00:00",
    }
    _put_runtime_events(layout, [row])
    monkeypatch.setattr(status_module, "collect_spool_metrics", lambda *_args, **_kwargs: _spool())

    result = collect_operator_status(layout, hours=24, now=datetime(2026, 9, 8, 10, tzinfo=UTC))

    assert cast(dict[str, object], result["runtime"])["state"] == "away"


def test_status_reports_latest_battery_and_active_transfer(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    layout = _layout(tmp_path)
    rows = (
        {
            "event": "ble_link_rssi_observed",
            "fields": {"rssi_dbm": -47, "status_hex": "0x00", "status_name": "success"},
            "timestamp": "2026-09-08T09:00:04+00:00",
        },
        {
            "event": "sync_progress",
            "fields": {
                "progress": {
                    "status": "observed",
                    "event": "pendant_observation",
                    "battery_percent": 96,
                    "firmware": "3.0.21",
                }
            },
            "timestamp": "2026-09-08T09:00:00+00:00",
        },
        {
            "event": "sync_progress",
            "fields": {"progress": {"status": "observed", "event": "pendant_observation", "firmware": "3.0.22"}},
            "timestamp": "2026-09-08T09:00:01+00:00",
        },
        {
            "event": "sync_progress",
            "fields": {
                "progress": {
                    "status": "progress",
                    "invocation_id": "00000000000000000000000000000001",
                    "bytes_per_second": 64_000.0,
                    "eta_seconds": 10.0,
                    "payload_bytes": 444,
                    "records_completed": 1,
                    "records_per_second": 144.0,
                    "records_total": 4,
                    "remaining_bytes": 1_332,
                    "remaining_packets": 3,
                    "total_bytes": 1_776,
                }
            },
            "timestamp": "2026-09-08T09:00:05+00:00",
        },
    )
    _put_runtime_events(layout, rows)
    monkeypatch.setattr(status_module, "collect_spool_metrics", lambda *_args, **_kwargs: _spool())

    result = collect_operator_status(
        layout,
        hours=24,
        now=datetime(2026, 9, 8, 9, 0, 10, tzinfo=UTC),
        identity=OperationalIdentity("00000000-0000-0000-0000-000000000001", "00000000000000000000000000000001"),
    )

    runtime = cast(dict[str, object], result["runtime"])
    assert runtime["battery_percent"] == 96
    assert runtime["battery_observed_at"] == "2026-09-08T09:00:00.000+00:00"
    assert runtime["connection_rssi_dbm"] == -47
    assert runtime["connection_rssi_observed_at"] == "2026-09-08T09:00:04.000+00:00"
    assert runtime["firmware"] == "3.0.22"
    assert runtime["last_error"] is None
    assert runtime["state"] == "transferring"
    assert runtime["updated_age_seconds"] == 5
    transfer = cast(dict[str, object], runtime["transfer"])
    assert transfer["fraction_complete"] == 0.25
    assert transfer["remaining_bytes"] == 1_332


@pytest.mark.parametrize(
    ("progress_invocation", "current_invocation", "expected_state"),
    [
        ("00000000000000000000000000000001", "00000000000000000000000000000001", "transferring"),
        ("00000000000000000000000000000002", "00000000000000000000000000000001", "unknown"),
        (None, "00000000000000000000000000000001", "unknown"),
        ("00000000000000000000000000000001", None, "unknown"),
    ],
)
def test_status_only_reports_progress_for_current_invocation(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    progress_invocation: str | None,
    current_invocation: str | None,
    expected_state: str,
) -> None:
    layout = _layout(tmp_path)
    _put_runtime_events(
        layout,
        [
            {
                "event": "sync_progress",
                "fields": {"progress": {"status": "observed", "event": "pendant_observation", "battery_percent": 73}},
                "timestamp": "2026-09-08T09:00:00+00:00",
            },
            {
                "event": "sync_progress",
                "fields": {"progress": {"status": "progress", "invocation_id": progress_invocation}},
                "timestamp": "2026-09-08T09:00:01+00:00",
            },
        ],
    )
    monkeypatch.setattr(status_module, "collect_spool_metrics", lambda *_args, **_kwargs: _spool())

    result = collect_operator_status(
        layout,
        hours=24,
        now=datetime(2026, 9, 8, 10, tzinfo=UTC),
        identity=OperationalIdentity("00000000-0000-0000-0000-000000000001", current_invocation),
    )

    runtime = cast(dict[str, object], result["runtime"])
    assert runtime["state"] == expected_state
    assert (runtime["transfer"] is not None) == (expected_state == "transferring")
    assert runtime["battery_percent"] == 73


def test_status_rejects_invalid_runtime_rssi_type(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    layout = _layout(tmp_path)
    _put_raw_runtime_projection(
        layout,
        {
            "battery_observation": None,
            "battery_observed_at": None,
            "connection_rssi_dbm": "not-an-integer",
            "connection_rssi_observed_at": "2026-09-08T09:00:00+00:00",
            "last_error": None,
            "last_error_at": None,
            "observation": None,
            "observation_at": None,
            "progress": None,
            "updated_at": None,
        },
    )
    monkeypatch.setattr(status_module, "collect_spool_metrics", lambda *_args, **_kwargs: _spool())

    with pytest.raises(OperatorStatusError, match="invalid connection RSSI"):
        collect_operator_status(layout, hours=24, now=datetime(2026, 9, 8, 10, tzinfo=UTC))


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("firmware_version", 42, "quality event history is invalid"),
        ("advertisement_rssi_dbm", True, "quality event history is invalid"),
        ("advertisement_rssi_dbm", "not-an-integer", "quality event history is invalid"),
    ],
)
def test_status_rejects_invalid_optional_quality_fields(
    field: str,
    value: object,
    message: str,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    layout = _layout(tmp_path)
    event = _transfer(
        "2026-09-08T09:05:00+00:00", outcome="collected", termination_class="completed", written_raw_bytes=444
    )
    event[field] = value
    _put_raw_quality_event(layout, event)
    monkeypatch.setattr(status_module, "collect_spool_metrics", lambda *_args, **_kwargs: _spool())

    with pytest.raises(OperatorStatusError, match=message):
        collect_operator_status(layout, hours=24, now=datetime(2026, 9, 8, 10, tzinfo=UTC))


def test_status_reports_quality_from_state_db_writer_and_rejects_oversized_event(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    layout = _layout(tmp_path)
    limit = 512
    quality_config = QualityMetricsConfig(max_bytes=limit, backup_count=1, max_record_bytes=limit)
    metric = TransferSessionMetric(
        "2026-09-08T09:05:00+00:00",
        "boundary",
        "collected",
        "completed",
        1_000,
        1,
        444,
        444,
        444,
        "1.2.3",
        "abcdef123456",
        "1.0.0",
        "auto",
        -91,
    )

    def encoded_record(value: TransferSessionMetric) -> bytes:
        return json.dumps(value.as_dict(), ensure_ascii=False, separators=(",", ":"), allow_nan=False).encode() + b"\n"

    padding = limit - len(encoded_record(metric))
    assert padding >= 0
    metric = replace(metric, session_id=metric.session_id + "s" * padding)
    assert len(encoded_record(metric)) == limit
    writer = SQLiteQualityMetrics(_state_db(layout), release_version="1.2.3", config=quality_config)
    writer.record_transfer_session(metric)
    assert writer.close()
    stored = _state_db(layout).list(StateKind.QUALITY_EVENT)
    assert len(stored) == 1
    assert stored[0][1]["event"] == "transfer_session"
    assert stored[0][1]["session_id"] == metric.session_id
    assert stored[0][1]["written_raw_bytes"] == 444

    with pytest.raises(QualityMetricsError, match="exceeds configured limit"):
        writer.record_transfer_session(replace(metric, session_id=metric.session_id + "s"))
    monkeypatch.setattr(status_module, "collect_spool_metrics", lambda *_args, **_kwargs: _spool())

    result = collect_operator_status(layout, hours=24, now=datetime(2026, 9, 8, 10, tzinfo=UTC))

    quality = cast(dict[str, object], result["quality_window"])
    assert quality["transfer_sessions"] == 1
    assert quality["written_raw_bytes"] == 444


def test_status_rejects_non_mapping_sync_progress_as_handled_status_error(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    layout = _layout(tmp_path)
    _put_raw_runtime_projection(
        layout,
        {
            "battery_observation": None,
            "battery_observed_at": None,
            "connection_rssi_dbm": None,
            "connection_rssi_observed_at": None,
            "last_error": None,
            "last_error_at": None,
            "observation": None,
            "observation_at": None,
            "progress": "malformed",
            "updated_at": "2026-09-08T09:00:00+00:00",
        },
    )
    monkeypatch.setattr(status_module, "collect_spool_metrics", lambda *_args, **_kwargs: _spool())

    with pytest.raises(OperatorStatusError, match="invalid progress"):
        collect_operator_status(layout, hours=24, now=datetime(2026, 9, 8, 10, tzinfo=UTC))


def test_status_marks_latest_fatal_transfer_as_attention_and_excludes_future_rows(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    layout = _layout(tmp_path)
    events = (
        _transfer(
            "2026-09-08T09:05:00+00:00", outcome="collected", termination_class="completed", written_raw_bytes=4_440
        ),
        _transfer(
            "2026-09-08T09:10:00+00:00",
            outcome="failed",
            termination_class="fatal_error",
            written_raw_bytes=444,
        ),
        _transfer(
            "2026-09-08T11:00:00+00:00",
            outcome="cancelled",
            termination_class="cancelled",
            written_raw_bytes=0,
        ),
    )
    _put_quality_events(layout, events)
    monkeypatch.setattr(status_module, "collect_spool_metrics", lambda *_args, **_kwargs: _spool())

    result = collect_operator_status(layout, hours=24, now=datetime(2026, 9, 8, 10, tzinfo=UTC))

    assert result["status"] == "attention"
    quality = cast(dict[str, object], result["quality_window"])
    assert quality["transfer_sessions"] == 2
    assert quality["last_transfer_outcome"] == "failed"
    assert quality["last_transfer_termination_class"] == "fatal_error"
    assert quality["transfer_outcomes"] == {"collected": 1, "failed": 1}
    assert quality["transfer_termination_classes"] == {"completed": 1, "fatal_error": 1}


@pytest.mark.parametrize("hours", [0, -1, True])
def test_status_rejects_nonpositive_or_boolean_window(hours: object, tmp_path: Path) -> None:
    layout = _layout(tmp_path)

    with pytest.raises(OperatorStatusError, match="hours must be positive"):
        collect_operator_status(layout, hours=cast(int, hours))


def test_status_accepts_one_hour_window(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    layout = _layout(tmp_path)
    event = _advertisement("2026-09-08T09:00:00+00:00")
    _put_quality_events(layout, [event])
    monkeypatch.setattr(status_module, "collect_spool_metrics", lambda *_args, **_kwargs: _spool())

    result = collect_operator_status(layout, hours=1, now=datetime(2026, 9, 8, 10, tzinfo=UTC))

    assert result["window_hours"] == 1
    assert cast(dict[str, object], result["quality_window"])["advertisements"] == 1


def test_status_uses_event_timestamps_when_runtime_journal_is_out_of_order(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    layout = _layout(tmp_path)
    rows = (
        {
            "event": "sync_progress",
            "fields": {"progress": {"status": "session_error", "error_message": "newer"}},
            "timestamp": "2026-09-08T09:03:00+00:00",
        },
        {"event": "ble_link_rssi_observed", "fields": {"rssi_dbm": -42}, "timestamp": "2026-09-08T09:02:00+00:00"},
        {
            "event": "sync_progress",
            "fields": {"progress": {"status": "observed", "event": "pendant_observation", "battery_percent": 88}},
            "timestamp": "2026-09-08T09:01:00+00:00",
        },
        {"event": "quality_metrics_configuration_error", "fields": {}, "timestamp": "2026-09-08T09:00:00+00:00"},
        {"event": "quality_metrics_ready", "fields": {}, "timestamp": "2026-09-08T09:04:00+00:00"},
        {
            "event": "sync_progress",
            "fields": {"progress": {"status": "session_error", "error_message": "older"}},
            "timestamp": "2026-09-08T08:59:00+00:00",
        },
        {"event": "ble_link_rssi_observed", "fields": {"rssi_dbm": -70}, "timestamp": "2026-09-08T08:58:00+00:00"},
        {
            "event": "sync_progress",
            "fields": {"progress": {"status": "observed", "event": "pendant_observation", "battery_percent": 12}},
            "timestamp": "2026-09-08T08:57:00+00:00",
        },
    )
    _put_runtime_events(layout, rows)
    monkeypatch.setattr(status_module, "collect_spool_metrics", lambda *_args, **_kwargs: _spool())

    result = collect_operator_status(layout, hours=24, now=datetime(2026, 9, 8, 10, tzinfo=UTC))

    runtime = cast(dict[str, object], result["runtime"])
    assert runtime["battery_percent"] == 88
    assert runtime["connection_rssi_dbm"] == -42
    last_error = cast(dict[str, object], runtime["last_error"])
    assert last_error["error_message"] is None
    assert last_error["occurred_at"] == "2026-09-08T09:03:00.000+00:00"
    assert runtime["attention_reasons"] == []


def test_status_summarizes_latest_quality_events_in_file_order_independently(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    layout = _layout(tmp_path)
    events = (
        _advertisement("2026-09-08T09:04:00+00:00"),
        _transfer(
            "2026-09-08T09:03:00+00:00", outcome="newer", termination_class="retryable_error", written_raw_bytes=1
        ),
        _clock_correction("2026-09-08T09:02:00+00:00"),
        _advertisement("2026-09-08T09:01:00+00:00"),
        _transfer("2026-09-08T09:00:00+00:00", outcome="older", termination_class="completed", written_raw_bytes=1),
        _clock_correction("2026-09-08T08:59:00+00:00"),
    )
    _put_quality_events(layout, events)
    monkeypatch.setattr(status_module, "collect_spool_metrics", lambda *_args, **_kwargs: _spool())

    result = collect_operator_status(layout, hours=24, now=datetime(2026, 9, 8, 10, tzinfo=UTC))

    quality = cast(dict[str, object], result["quality_window"])
    assert quality["last_advertisement_at"] == "2026-09-08T09:04:00.000+00:00"
    assert quality["last_transfer_outcome"] == "newer"
    assert quality["last_clock_correction_at"] == "2026-09-08T09:02:00.000+00:00"


def test_status_merges_equal_timestamp_runtime_events_into_one_current_projection(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    layout = _layout(tmp_path)
    timestamp = "2026-09-08T09:00:00+00:00"
    rows = (
        {
            "event": "sync_progress",
            "fields": {"progress": {"status": "observed", "event": "pendant_observation", "firmware": "1.0.0"}},
            "timestamp": timestamp,
        },
        {
            "event": "sync_progress",
            "fields": {
                "progress": {
                    "status": "observed",
                    "event": "pendant_observation",
                    "firmware": "1.0.1",
                    "battery_percent": 88,
                }
            },
            "timestamp": timestamp,
        },
        {
            "event": "sync_progress",
            "fields": {
                "progress": {
                    "status": "observed",
                    "event": "pendant_observation",
                    "firmware": "1.0.2",
                    "battery_percent": 12,
                }
            },
            "timestamp": timestamp,
        },
        {
            "event": "sync_progress",
            "fields": {"progress": {"status": "session_error", "error_message": "first-error"}},
            "timestamp": timestamp,
        },
        {
            "event": "sync_progress",
            "fields": {"progress": {"status": "session_error", "error_message": "later-error"}},
            "timestamp": timestamp,
        },
        {"event": "ble_link_rssi_observed", "fields": {"rssi_dbm": -42}, "timestamp": timestamp},
        {"event": "ble_link_rssi_observed", "fields": {"rssi_dbm": -70}, "timestamp": timestamp},
        {"event": "sync_progress", "fields": {"progress": {"status": "away"}}, "timestamp": timestamp},
        {"event": "sync_progress", "fields": {"progress": {"status": "drained"}}, "timestamp": timestamp},
    )
    _put_runtime_events(layout, rows)
    monkeypatch.setattr(status_module, "collect_spool_metrics", lambda *_args, **_kwargs: _spool())

    result = collect_operator_status(layout, hours=24, now=datetime(2026, 9, 8, 10, tzinfo=UTC))

    runtime = cast(dict[str, object], result["runtime"])
    assert runtime["state"] == "drained"
    assert runtime["firmware"] == "1.0.2"
    assert runtime["battery_percent"] == 12
    assert cast(dict[str, object], runtime["last_error"])["error_message"] is None
    assert runtime["connection_rssi_dbm"] == -70
    rows = _state_db(layout).list(StateKind.RUNTIME_STATUS)
    assert len(rows) == 1
    assert rows[0][0] == "current"


def test_status_keeps_first_quality_observation_at_equal_timestamps(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    layout = _layout(tmp_path)
    timestamp = "2026-09-08T09:00:00.000+00:00"
    events = (
        AdvertisementMetric(timestamp, "first-ad", -81, "1.2.3", "abcdef123456", "auto").as_dict(),
        AdvertisementMetric(timestamp, "later-ad", -22, "1.2.3", "abcdef123456", "auto").as_dict(),
        TransferSessionMetric(
            timestamp,
            "first-transfer",
            "first-outcome",
            "retryable_error",
            2_000,
            10,
            1,
            1,
            1,
            "1.2.3",
            "abcdef123456",
            "1.0.0",
            "auto",
            -91,
        ).as_dict(),
        TransferSessionMetric(
            timestamp,
            "later-transfer",
            "later-outcome",
            "fatal_error",
            2_000,
            10,
            2,
            2,
            2,
            "1.2.3",
            "abcdef123456",
            "1.0.0",
            "auto",
            -91,
        ).as_dict(),
        ClockCorrectionMetric(timestamp, "first-clock", 1.25, 1, 4, 4, "1.2.3", None, "1.0.0").as_dict(),
        ClockCorrectionMetric(timestamp, "later-clock", 2.5, 1, 4, 4, "1.2.3", None, "1.0.0").as_dict(),
    )
    _put_quality_events(layout, events)
    monkeypatch.setattr(status_module, "collect_spool_metrics", lambda *_args, **_kwargs: _spool())

    result = collect_operator_status(layout, hours=24, now=datetime(2026, 9, 8, 10, tzinfo=UTC))

    quality = cast(dict[str, object], result["quality_window"])
    assert result["schema_version"] == 2
    assert quality["advertisements"] == 2
    assert quality["last_advertisement_rssi_dbm"] == -81
    assert quality["last_transfer_outcome"] == "first-outcome"
    assert quality["last_transfer_termination_class"] == "retryable_error"
    assert quality["last_clock_correction_drift_seconds"] == 1.25
    assert result["status"] == "unknown"


def test_status_reads_one_current_runtime_projection_from_state_db(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    layout = _layout(tmp_path)
    _put_runtime_events(
        layout,
        [
            {
                "event": "sync_progress",
                "fields": {"progress": {"status": "away"}},
                "timestamp": "2026-09-08T09:00:00+00:00",
            }
        ],
    )
    monkeypatch.setattr(status_module, "collect_spool_metrics", lambda *_args, **_kwargs: _spool())

    result = collect_operator_status(layout, hours=24, now=datetime(2026, 9, 8, 10, tzinfo=UTC))

    assert cast(dict[str, object], result["runtime"])["state"] == "away"
    rows = _state_db(layout).list(StateKind.RUNTIME_STATUS)
    assert len(rows) == 1
    assert rows[0][0] == "current"


def test_status_includes_both_quality_window_edges_and_equal_clock_sequences(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    layout = _layout(tmp_path)
    start = _advertisement("2026-09-08T09:00:00+00:00")
    end = _advertisement("2026-09-08T10:00:00+00:00")
    correction = _clock_correction("2026-09-08T10:00:00+00:00")
    correction["boundary_sequence_min"] = 4
    correction["boundary_sequence_max"] = 4
    _put_quality_events(layout, (start, end, correction))
    monkeypatch.setattr(status_module, "collect_spool_metrics", lambda *_args, **_kwargs: _spool())

    result = collect_operator_status(layout, hours=1, now=datetime(2026, 9, 8, 10, tzinfo=UTC))

    quality = cast(dict[str, object], result["quality_window"])
    assert quality["advertisements"] == 2
    assert quality["clock_corrections"] == 1


def test_status_returns_empty_quality_window_when_no_events_are_stored(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    layout = _layout(tmp_path)
    _state_db(layout)
    monkeypatch.setattr(status_module, "collect_spool_metrics", lambda *_args, **_kwargs: _spool())

    result = collect_operator_status(layout, hours=24, now=datetime(2026, 9, 8, 10, tzinfo=UTC))

    quality = cast(dict[str, object], result["quality_window"])
    assert quality["advertisements"] == 0
    assert quality["transfer_sessions"] == 0


@pytest.mark.parametrize(
    ("completed", "total", "expected"),
    [(1, 1, 1), (0, 0, None), (1, "1", None)],
)
def test_status_reports_fraction_only_for_positive_integer_totals(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, completed: object, total: object, expected: object
) -> None:
    layout = _layout(tmp_path)
    row = {
        "event": "sync_progress",
        "fields": {
            "progress": {
                "status": "progress",
                "invocation_id": "00000000000000000000000000000001",
                "records_completed": completed,
                "records_total": total,
            }
        },
        "timestamp": "2026-09-08T09:03:00+00:00",
    }
    _put_runtime_events(layout, [row])
    monkeypatch.setattr(status_module, "collect_spool_metrics", lambda *_args, **_kwargs: _spool())

    result = collect_operator_status(
        layout,
        hours=24,
        now=datetime(2026, 9, 8, 10, tzinfo=UTC),
        identity=OperationalIdentity("00000000-0000-0000-0000-000000000001", "00000000000000000000000000000001"),
    )

    transfer = cast(dict[str, object], cast(dict[str, object], result["runtime"])["transfer"])
    assert transfer["fraction_complete"] == expected


@pytest.mark.parametrize(
    ("factory", "key", "value"),
    [
        (_advertisement, "schema_version", 3),
        (_advertisement, "session_id", 42),
        (_advertisement, "advertisement_rssi_dbm", True),
        (_advertisement, "source_revision", 42),
        (
            lambda timestamp: _transfer(
                timestamp, outcome="collected", termination_class="completed", written_raw_bytes=1
            ),
            "requested_record_count",
            True,
        ),
        (_clock_correction, "drift_seconds", True),
    ],
)
def test_status_rejects_quality_events_with_malformed_field_types(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    factory: Callable[[str], dict[str, object]],
    key: str,
    value: object,
) -> None:
    layout = _layout(tmp_path)
    event = factory("2026-09-08T09:00:00+00:00")
    event[key] = value
    _put_raw_quality_event(layout, event)
    monkeypatch.setattr(status_module, "collect_spool_metrics", lambda *_args, **_kwargs: _spool())

    with pytest.raises(OperatorStatusError, match="invalid"):
        collect_operator_status(layout, hours=24, now=datetime(2026, 9, 8, 10, tzinfo=UTC))
