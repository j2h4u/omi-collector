from __future__ import annotations

import json
import sqlite3
import threading
import time
from collections.abc import Callable
from contextlib import closing
from dataclasses import replace
from datetime import UTC, datetime, timedelta, tzinfo
from pathlib import Path
from typing import cast

import pytest

import omi_collector.operator_status as status_module
from omi_collector.capture.adapters.collector_state_db import StateDB, StateKind
from omi_collector.capture.adapters.confirmed_loss import (
    ConfirmedLossLedger,
    read_confirmed_losses,
)
from omi_collector.capture.adapters.debug_logging import (
    write_runtime_status_event,
)
from omi_collector.capture.adapters.firmware_observations import FirmwareObservationStore
from omi_collector.capture.adapters.internal_audio_store import (
    AUDIO_RECORD_SIZE,
    CaptureTail,
    DrainCheckpoint,
    InternalAudioStore,
)
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
from omi_collector.state_migration import StateMigrationError, open_runtime_state, open_status_state
from omi_collector.storage_layout import StorageLayout, load_operator_config


def _sqlite_row(values: tuple[object, ...]) -> sqlite3.Row:
    connection = sqlite3.connect(":memory:")
    connection.row_factory = sqlite3.Row
    try:
        row = cast(
            sqlite3.Row | None,
            connection.execute("SELECT " + ",".join("?" for _ in values), values).fetchone(),
        )
    finally:
        connection.close()
    assert row is not None
    return row


def _valid_framing_source_row() -> tuple[object, ...]:
    return ("incident", "source", (10).to_bytes(8, "big"), 3, 1, (10).to_bytes(8, "big"), 0, None, None, None)


def _valid_framing_observation_row() -> tuple[object, ...]:
    return (
        "incident",
        "source",
        1,
        1,
        (10).to_bytes(8, "big"),
        (10).to_bytes(8, "big"),
        2,
        None,
        2,
        2 * RECORD_SIZE,
        0,
        None,
        None,
    )


@pytest.mark.parametrize(
    ("index", "value"),
    (
        (0, 1),
        (1, None),
        (2, b"short"),
        (3, -1),
        (3, 1.0),
        (4, -1),
        (4, 4),
        (4, "1"),
        (5, b"short"),
        (6, 2),
        (6, 1.0),
        (7, -1),
        (7, 1.0),
        (8, -1),
        (8, 1.0),
        (9, 0),
        (9, RECORD_SIZE),
        (9, 1.0),
    ),
    ids=(
        "incident-type",
        "source-type",
        "sequence-start-encoding",
        "negative-committed-count",
        "noninteger-committed-count",
        "negative-verified-count",
        "verified-count-exceeds-committed",
        "noninteger-verified-count",
        "frontier-encoding",
        "invalid-conflict-flag",
        "noninteger-conflict-flag",
        "negative-tail-generation",
        "noninteger-tail-generation",
        "negative-tail-offset",
        "noninteger-tail-offset",
        "empty-tail",
        "oversized-tail",
        "noninteger-tail-size",
    ),
)
def test_framing_source_decoder_rejects_invalid_persisted_metadata(index: int, value: object) -> None:
    values = list(_valid_framing_source_row())
    values[index] = value
    with pytest.raises(OperatorStatusError, match="framing source metadata is invalid"):
        status_module._decode_framing_source(_sqlite_row(tuple(values)))


@pytest.mark.parametrize("tail", ((1, None, None), (None, 1, None), (None, None, 1)))
def test_framing_source_decoder_requires_a_complete_tail_reference(tail: tuple[int | None, ...]) -> None:
    values = list(_valid_framing_source_row())
    values[7:] = tail
    with pytest.raises(OperatorStatusError, match="framing source metadata is invalid"):
        status_module._decode_framing_source(_sqlite_row(tuple(values)))


def test_framing_source_decoder_projects_complete_tail_reference() -> None:
    values = list(_valid_framing_source_row())
    values[7:] = (1, 20, 17)
    decoded = status_module._decode_framing_source(_sqlite_row(tuple(values)))
    assert (decoded.tail_generation, decoded.tail_offset, decoded.tail_size) == (1, 20, 17)


def test_framing_source_decoder_rejects_wrong_row_width() -> None:
    with pytest.raises(OperatorStatusError, match="framing source metadata is invalid"):
        status_module._decode_framing_source(_sqlite_row(_valid_framing_source_row()[:-1]))


@pytest.mark.parametrize(
    ("changes", "message"),
    (
        (((0, 1),), "framing observation metadata is invalid"),
        (((1, None),), "framing observation metadata is invalid"),
        (((2, 1.0),), "framing observation metadata is invalid"),
        (((3, 2),), "framing observation metadata is invalid"),
        (((3, 1.0),), "framing observation metadata is invalid"),
        (((6, 0),), "framing observation metadata is invalid"),
        (((6, 1.0),), "framing observation metadata is invalid"),
        (((7, "0"),), "framing observation metadata is invalid"),
        (((8, -1),), "framing observation metadata is invalid"),
        (((8, 3),), "framing observation metadata is invalid"),
        (((8, 1.0),), "framing observation metadata is invalid"),
        (((9, -1),), "framing observation metadata is invalid"),
        (((9, 1.0),), "framing observation metadata is invalid"),
        (((10, -1),), "framing observation metadata is invalid"),
        (((10, RECORD_SIZE),), "framing observation metadata is invalid"),
        (((10, 1.0),), "framing observation metadata is invalid"),
        (((4, b"short"),), "framing observation READ_BEGIN is invalid"),
        (((5, b"short"),), "framing observation READ_BEGIN is invalid"),
        (((9, 16), (10, 17)), "framing observation byte extent is invalid"),
        (((7, 2 * RECORD_SIZE),), "framing observation byte extent is invalid"),
        (((8, 1), (9, RECORD_SIZE - 1)), "framing observation prefix extent exceeds its READ"),
        (((11, 1),), "framing observation chunk extent is invalid"),
        (((12, 0),), "framing observation chunk extent is invalid"),
        (((11, RECORD_SIZE - 1), (12, 1)), "framing observation chunk extent is outside the READ"),
        (((11, RECORD_SIZE), (12, 2 * RECORD_SIZE)), "framing observation chunk extent is outside the READ"),
        (((5, (9).to_bytes(8, "big")),), "framing observation READ_BEGIN precedes its source"),
    ),
)
def test_framing_observation_decoder_rejects_invalid_persisted_metadata(
    changes: tuple[tuple[int, object], ...], message: str
) -> None:
    values = list(_valid_framing_observation_row())
    for index, value in changes:
        values[index] = value
    with pytest.raises(OperatorStatusError, match=message):
        status_module._decode_framing_row(_sqlite_row(tuple(values)))


def test_framing_observation_decoder_projects_partial_tail_and_chunk_ranges() -> None:
    values = list(_valid_framing_observation_row())
    values[8] = 1
    values[9] = 2 * RECORD_SIZE + 17
    values[10] = 17
    values[11] = RECORD_SIZE
    values[12] = RECORD_SIZE
    row = status_module._decode_framing_row(_sqlite_row(tuple(values)))
    assert row.tail_range == (2 * RECORD_SIZE, 2 * RECORD_SIZE + 17)
    assert row.chunk_range == (RECORD_SIZE, 2 * RECORD_SIZE)


def test_framing_observation_decoder_rejects_wrong_row_width() -> None:
    with pytest.raises(OperatorStatusError, match="framing observation row is invalid"):
        status_module._decode_framing_row(_sqlite_row(_valid_framing_observation_row()[:-1]))


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


def test_status_dates_progress_written_during_report(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    layout = _layout(tmp_path)
    database = _state_db(layout)
    started = datetime.now(UTC).replace(microsecond=123000)
    progress = started + timedelta(seconds=1)
    finished = started + timedelta(seconds=5)
    readings = iter((started, finished))

    class ReportClock(datetime):
        @classmethod
        def now(cls, tz: tzinfo | None = None) -> ReportClock:
            assert tz == UTC
            return cls.fromtimestamp(next(readings).timestamp(), tz)

    def spool_during_progress(*_args: object, **_kwargs: object) -> SpoolMetrics:
        write_runtime_status_event(database, "sync_progress", {"progress": {"status": "away"}}, progress.isoformat())
        return _spool()

    monkeypatch.setattr(status_module, "datetime", ReportClock)
    monkeypatch.setattr(status_module, "collect_spool_metrics", spool_during_progress)
    result = collect_operator_status(layout, hours=24)
    runtime = cast(dict[str, object], result["runtime"])
    assert datetime.fromisoformat(cast(str, result["generated_at"])) == finished
    assert datetime.fromisoformat(cast(str, runtime["updated_at"])) == progress
    assert runtime["updated_age_seconds"] == 4


def test_status_skips_writer_admission_but_rejects_unsafe_database(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    layout = _layout(tmp_path)
    _state_db(layout)
    layout.publication.root.mkdir()

    def fail_audit(*_args: object) -> None:
        pytest.fail("status ran writer admission audit")

    monkeypatch.setattr("omi_collector.state_migration._validate_v2_only_publication", fail_audit)
    monkeypatch.setattr("omi_collector.state_migration._assert_private_audio_gone", fail_audit)
    assert collect_operator_status(layout, hours=24)["schema_version"] == 4
    path = layout.collector.state_db
    target = path.with_name("other.db")
    path.rename(target)
    path.symlink_to(target)
    with pytest.raises(StateMigrationError, match="regular file"):
        open_status_state(layout)


def test_status_bounds_sqlite_busy_wait_during_open_and_reads(tmp_path: Path) -> None:
    layout = _layout(tmp_path)
    _state_db(layout)
    reader = open_status_state(layout)
    with closing(reader.open_connection(read_only=True)) as connection:
        assert connection.execute("PRAGMA busy_timeout").fetchone()[0] == 1_000
    with closing(sqlite3.connect(layout.collector.state_db)) as writer:
        writer.execute("BEGIN EXCLUSIVE")
        for read in (lambda: open_status_state(layout), lambda: reader.get(StateKind.MIGRATION, "missing")):
            started = time.monotonic()
            with pytest.raises(TransientInternalAudioStorageError, match="busy"):
                read()
            assert time.monotonic() - started >= 0.8


@pytest.mark.parametrize("version", [1, 2, 3, 4])
def test_status_reports_old_schema_version(tmp_path: Path, version: int) -> None:
    layout = _layout(tmp_path)
    _state_db(layout)
    with closing(sqlite3.connect(layout.collector.state_db)) as connection:
        connection.execute(f"PRAGMA user_version={version}")
    message = "v1 state is unsupported" if version == 1 else f"v{version} state requires stopped-service"
    with pytest.raises(StateMigrationError, match=message):
        open_status_state(layout)


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
    assert store.finish_read(source_id, 11, AUDIO_RECORD_SIZE) is True
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


def test_saved_unfinished_records_tracks_resumable_capture(tmp_path: Path) -> None:
    layout = _layout(tmp_path)
    layout.publication.root.mkdir(parents=True)
    database = _state_db(layout)
    store = InternalAudioStore(database)
    source_id = "a" * 32
    raw = (10).to_bytes(4, "big") + bytes((2, 8, 1)) + bytes(AUDIO_RECORD_SIZE - 7)

    def saved() -> object:
        status = collect_operator_status(layout, hours=24, now=datetime(2026, 9, 8, 10, tzinfo=UTC))
        return cast(dict[str, object], status["runtime"])["saved_unfinished_records"]

    assert saved() == 0
    store.create_source(source_id, "attempt", 10, 12)
    assert saved() == 0
    store.prepare_leg(source_id, 10, 2)
    source = store.record_read_begin(source_id, 10, 2)
    store.commit_drain(source_id, 0, (raw,), DrainCheckpoint(source.current_leg_generation))
    assert saved() == 1
    store.interrupt(source_id)
    assert saved() == 1
    store.prepare_leg(source_id, 10, 2)
    source = store.record_read_begin(source_id, 10, 2)
    store.commit_drain(
        source_id, 0, (raw, (11).to_bytes(4, "big") + raw[4:]), DrainCheckpoint(source.current_leg_generation)
    )
    assert store.finish_read(source_id, 12, 2 * AUDIO_RECORD_SIZE)
    source = store.get_source(source_id)
    assert source is not None
    store.seal_source(
        source_id,
        leg_generation=source.current_leg_generation,
        expected_count=source.committed_count,
        expected_sha256=source.committed_sha256,
    )
    assert saved() == 0
    store.close()


def test_saved_unfinished_records_rejects_multiple_resumable_captures(tmp_path: Path) -> None:
    layout = _layout(tmp_path)
    database = _state_db(layout)
    store = InternalAudioStore(database)
    store.create_source("a" * 32, "attempt", 10, 12)
    store.create_source("b" * 32, "attempt", 20, 22)
    with pytest.raises(OperatorStatusError, match="multiple unfinished captures"):
        status_module._audio_custody_counts(database)
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


def test_operator_status_separates_confirmed_bad_observations_from_unresolved_bytes(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    layout = _layout(tmp_path)
    layout.publication.root.mkdir(parents=True)
    db = _state_db(layout)
    _operational_status(layout, {})
    damaged_id, incomplete_id, tail, incomplete_tail = _seed_status_framing_cases(db)
    with closing(db.open_connection()) as connection, db.access(connection):
        connection.execute(
            "INSERT INTO capture_conflict(source_id,chunk_ordinal,leg_generation,absolute_byte_offset,reason,bytes) "
            "VALUES (?,?,?,?,?,?)",
            (incomplete_id, 99, 1, 0, "interrupted_provisional", b"x" * 100),
        )

    monkeypatch.setattr(status_module, "collect_spool_metrics", lambda *_args, **_kwargs: _spool())
    monkeypatch.setattr(status_module, "read_firmware_observations", lambda *_args, **_kwargs: ())
    sql: list[str] = []
    original_open = StateDB.open_connection

    def traced_open(self: StateDB, *, read_only: bool = False) -> sqlite3.Connection:
        connection = original_open(self, read_only=read_only)
        connection.set_trace_callback(sql.append)
        return connection

    monkeypatch.setattr(StateDB, "open_connection", traced_open)
    status = collect_operator_status(layout, hours=24, now=datetime(2026, 9, 8, 10, tzinfo=UTC))
    framing = cast(dict[str, object], status["audio_integrity"])

    assert status["status"] == "attention"
    _assert_operator_audio_integrity(framing, damaged_id, incomplete_id, tail, incomplete_tail)
    assert json.dumps(status, sort_keys=True)
    framing_query = next(statement for statement in sql if "capture_observation AS observation" in statement)
    assert "length(observation.tail)" in framing_query.lower()
    assert "length(chunk.bytes)" in framing_query.lower()
    assert "select chunk.bytes" not in framing_query.lower()
    conflict_query = next(
        statement.lower() for statement in sql if "from capture_conflict as conflict" in statement.lower()
    )
    assert "length(conflict.bytes)" in conflict_query.lower()
    assert "select conflict.bytes" not in conflict_query.lower()
    assert "incident.incident_id=observation.incident_id" in framing_query.lower()
    assert "where incident.status='unresolved'" in framing_query.lower()


def _assert_operator_audio_integrity(
    framing: dict[str, object], damaged_id: str, incomplete_id: str, tail: bytes, incomplete_tail: bytes
) -> None:
    assert framing["confirmed_malformed_observations"] == 1
    assert framing["unresolved_observation_count"] == 2
    assert framing["unresolved_conflict_count"] == 1
    assert (
        framing["unresolved_retained_storage_bytes"]
        == 3 * AUDIO_RECORD_SIZE + (3 * AUDIO_RECORD_SIZE + len(tail)) + (2 * len(incomplete_tail)) + 100
    )
    observations = cast(list[dict[str, object]], framing["unresolved_observations"])
    damaged, incomplete = observations
    assert damaged["local_reference"] == {"source_id": damaged_id, "leg_generation": 1}
    assert damaged["read_begin"] == {"start_sequence": 100, "packet_count": 5}
    assert damaged["fault_offset_read_relative"] == AUDIO_RECORD_SIZE
    assert damaged["prefix_record_count"] == 1
    assert damaged["prefix_source_relative_byte_range"] == {"start": 0, "end": AUDIO_RECORD_SIZE}
    assert damaged["retained_raw_bytes"] == 3 * AUDIO_RECORD_SIZE + len(tail)
    assert damaged["read_relative_byte_ranges"] == [
        {"start": AUDIO_RECORD_SIZE, "end": 4 * AUDIO_RECORD_SIZE + len(tail)}
    ]
    assert incomplete["local_reference"] == {"source_id": incomplete_id, "leg_generation": 1}
    assert incomplete["fault_offset_read_relative"] is None
    assert incomplete["prefix_record_count"] == 2
    assert incomplete["prefix_source_relative_byte_range"] == {"start": 0, "end": 2 * AUDIO_RECORD_SIZE}
    assert incomplete["retained_raw_bytes"] == len(incomplete_tail)
    assert incomplete["read_relative_byte_ranges"] == [
        {"start": 2 * AUDIO_RECORD_SIZE, "end": 2 * AUDIO_RECORD_SIZE + len(incomplete_tail)}
    ]
    sources = cast(list[dict[str, object]], framing["unresolved_sources"])
    damaged_source, incomplete_source = sources
    assert damaged_source["local_reference"] == {"source_id": damaged_id}
    assert damaged_source["canonical_unverified_records"] == 1
    assert damaged_source["canonical_unverified_byte_range"] == {"start": 0, "end": AUDIO_RECORD_SIZE}
    assert incomplete_source["local_reference"] == {"source_id": incomplete_id}
    assert incomplete_source["canonical_unverified_records"] == 2
    assert incomplete_source["canonical_unverified_byte_range"] == {
        "start": 0,
        "end": 2 * AUDIO_RECORD_SIZE,
    }
    assert cast(dict[str, object], incomplete_source["tail"])["retained_raw_bytes"] == len(incomplete_tail)
    conflicts = cast(list[dict[str, object]], framing["unresolved_conflicts"])
    assert any(
        item["local_reference"] == {"source_id": incomplete_id, "leg_generation": 1}
        and item["source_relative_byte_ranges"] == [{"start": 0, "end": 100}]
        for item in conflicts
    )
    assert "x" * 100 not in json.dumps(framing)
    assert "missing_records" not in framing and "end_sequence" not in json.dumps(framing)


def test_operator_status_reports_quarantine_source_without_observation(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    layout = _layout(tmp_path)
    layout.publication.root.mkdir(parents=True)
    db = _state_db(layout)
    _operational_status(layout, {})
    source_id = "c" * 32
    tail = b"tail"
    with InternalAudioStore(db, chunk_limit=8) as store:
        store.create_source(source_id, "attempt", 300, 303)
        store.prepare_leg(source_id, 300, 3)
        store.record_read_begin(source_id, 300, 3)
        store.commit_checked_drain(
            source_id,
            0,
            (bytes(AUDIO_RECORD_SIZE), bytes(AUDIO_RECORD_SIZE)),
            DrainCheckpoint(1, CaptureTail(1, 2, 2 * AUDIO_RECORD_SIZE + len(tail), 2 * AUDIO_RECORD_SIZE, tail)),
        )
        store.interrupt(source_id)
    with InternalAudioStore(db, chunk_limit=8) as store:
        assert store.quarantine_unconfirmed(source_id) == 0

    monkeypatch.setattr(status_module, "collect_spool_metrics", lambda *_args, **_kwargs: _spool())
    monkeypatch.setattr(status_module, "read_firmware_observations", lambda *_args, **_kwargs: ())
    status = collect_operator_status(layout, hours=24, now=datetime(2026, 9, 8, 10, tzinfo=UTC))

    framing = cast(dict[str, object], status["audio_integrity"])
    assert status["status"] == "attention"
    assert framing["unresolved_source_count"] == 1
    assert framing["unresolved_observation_count"] == 0
    assert framing["unresolved_retained_storage_bytes"] == 2 * AUDIO_RECORD_SIZE + len(tail)
    source = cast(list[dict[str, object]], framing["unresolved_sources"])[0]
    assert source["local_reference"] == {"source_id": source_id}
    assert source["canonical_unverified_byte_range"] == {
        "start": 0,
        "end": 2 * AUDIO_RECORD_SIZE,
    }
    assert cast(dict[str, object], source["tail"])["source_relative_byte_range"] == {
        "start": 2 * AUDIO_RECORD_SIZE,
        "end": 2 * AUDIO_RECORD_SIZE + len(tail),
    }


def test_operator_status_includes_verified_conflict_frontier_in_unverified_suffix() -> None:
    source = status_module._FramingSourceRow(
        incident_id="incident",
        source_id="source",
        sequence_start=100,
        committed_count=5,
        verified_count=4,
        confirmed_frontier=102,
        verified_conflict=True,
        tail_generation=None,
        tail_offset=None,
        tail_size=None,
    )

    projection = status_module._project_framing_source((source,))

    assert projection["canonical_unverified_records"] == 3
    assert projection["canonical_unverified_byte_range"] == {
        "start": 2 * AUDIO_RECORD_SIZE,
        "end": 5 * AUDIO_RECORD_SIZE,
    }


def test_operator_status_joins_retry_observations_to_their_original_incident(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    layout = _layout(tmp_path)
    layout.publication.root.mkdir(parents=True)
    db = _state_db(layout)
    _operational_status(layout, {})
    original, retry = "a1" * 16, "a2" * 16
    malformed = bytes([255]) * AUDIO_RECORD_SIZE
    with InternalAudioStore(db, chunk_limit=8) as store:
        for source_id in (original, retry):
            store.create_source(source_id, "attempt", 100, 102)
            store.prepare_leg(source_id, 100, 2)
            store.record_read_begin(source_id, 100, 2)
            store.commit_checked_drain(source_id, 0, (malformed,), DrainCheckpoint(1))
            assert not store.finish_read(source_id, 102, AUDIO_RECORD_SIZE)

    monkeypatch.setattr(status_module, "collect_spool_metrics", lambda *_args, **_kwargs: _spool())
    monkeypatch.setattr(status_module, "read_firmware_observations", lambda *_args, **_kwargs: ())
    status = collect_operator_status(layout, hours=24, now=datetime(2026, 9, 8, 10, tzinfo=UTC))

    framing = cast(dict[str, object], status["audio_integrity"])
    observations = cast(list[dict[str, object]], framing["unresolved_observations"])
    assert framing["confirmed_malformed_observations"] == 2
    assert framing["unresolved_observation_count"] == 2
    assert framing["unresolved_retained_storage_bytes"] == 2 * AUDIO_RECORD_SIZE
    assert {item["incident_id"] for item in observations} == {original}
    assert {cast(dict[str, object], item["local_reference"])["source_id"] for item in observations} == {
        original,
        retry,
    }


def _seed_status_framing_cases(db: StateDB) -> tuple[str, str, bytes, bytes]:
    good, bad, tail, incomplete_tail = bytes(AUDIO_RECORD_SIZE), bytes([255]) * AUDIO_RECORD_SIZE, b"partial", b"rest"
    damaged_id, incomplete_id = "a" * 32, "b" * 32
    with InternalAudioStore(db, chunk_limit=8) as store:
        store.create_source(damaged_id, "attempt", 100, 105)
        store.prepare_leg(damaged_id, 100, 5)
        store.record_read_begin(damaged_id, 100, 5)
        assert store.commit_checked_drain(damaged_id, 0, (good, bad, good), DrainCheckpoint(1)) == 1
        checkpoint = DrainCheckpoint(
            1, CaptureTail(1, 4, 4 * AUDIO_RECORD_SIZE + len(tail), 4 * AUDIO_RECORD_SIZE, tail)
        )
        assert store.commit_checked_drain(damaged_id, 3, (good,), checkpoint) == 1

        store.create_source(incomplete_id, "attempt", 200, 203)
        store.prepare_leg(incomplete_id, 200, 3)
        store.record_read_begin(incomplete_id, 200, 3)
        checkpoint = DrainCheckpoint(
            1,
            CaptureTail(1, 2, 2 * AUDIO_RECORD_SIZE + len(incomplete_tail), 2 * AUDIO_RECORD_SIZE, incomplete_tail),
        )
        store.commit_checked_drain(incomplete_id, 0, (good, good), checkpoint)
        assert store.finish_read(incomplete_id, 203, 2 * AUDIO_RECORD_SIZE + len(incomplete_tail)) is False
    return damaged_id, incomplete_id, tail, incomplete_tail


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
        status_module._audio_custody_counts(db)
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
        "saved_unfinished_records": 0,
        "state": "unknown",
        "transfer": None,
        "updated_age_seconds": None,
        "updated_at": None,
    }
    assert result["schema_version"] == 4
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
    spool = SpoolMetrics(SpoolWindowMetrics(2, 30, 13_320, 10, 4_440, 0.25), _spool().firmware_lifetime)
    monkeypatch.setattr(status_module, "collect_spool_metrics", lambda *_args, **_kwargs: spool)

    result = collect_operator_status(layout, hours=24, now=datetime(2026, 9, 8, 10, tzinfo=UTC), identity=identity)

    assert result["status"] == "ok"
    publication = cast(dict[str, object], result["publication"])
    quality = cast(dict[str, object], result["quality_window"])
    assert publication["unconfirmed_gap_records"] == 10
    assert quality["confirmed_lost_records"] == 0


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
    assert result["schema_version"] == 4
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
