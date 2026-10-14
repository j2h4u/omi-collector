from __future__ import annotations

import json
import logging
from datetime import UTC, datetime
from pathlib import Path
from threading import Event, Thread

import pytest

from omi_collector.capture.adapters.collector_state_db import StateDB, StateKind
from omi_collector.capture.adapters.quality_metrics import (
    QualityMetricsError,
    SQLiteQualityMetrics,
    import_quality_event,
    import_quality_events,
    quality_event_id,
    read_quality_events,
    validate_quality_event,
)
from omi_collector.capture.application.quality_metrics import ClockCorrectionMetric, SequenceLossMetric
from omi_collector.capture.application.storage_fault import FatalInternalAudioStorageError
from omi_collector.capture.domain.operational_status_machine import OperationalSignal
from omi_collector.config import QualityMetricsConfig


def _database(tmp_path: Path) -> StateDB:
    return StateDB(tmp_path / "collector-state.db")


def _loss(index: int, *, reason: str = "cursor advanced") -> SequenceLossMetric:
    return SequenceLossMetric(
        f"2026-10-11T01:02:{index:02}.456+00:00",
        f"session-{index}",
        1,
        444,
        reason,
        "1.2.3",
        "a" * 12,
        "1.0.0",
    )


def test_quality_events_are_queued_validated_and_read_in_time_order(tmp_path: Path) -> None:
    database = _database(tmp_path)
    writer = SQLiteQualityMetrics(database, release_version="1.2.3", source_revision="a" * 40)
    writer.record_sequence_loss(_loss(2))
    writer.record_sequence_loss(_loss(1))

    assert writer.close(timeout_seconds=2)
    rows = read_quality_events(
        database,
        datetime(2026, 10, 11, 1, 2, 1, tzinfo=UTC),
        datetime(2026, 10, 11, 1, 2, 3, tzinfo=UTC),
    )

    assert [row["session_id"] for row in rows] == ["session-1", "session-2"]
    assert all(validate_quality_event(row).tzinfo is not None for row in rows)
    assert len(database.list(StateKind.QUALITY_EVENT)) == 2


def test_quality_history_prunes_by_the_existing_total_rotation_budget(tmp_path: Path) -> None:
    database = _database(tmp_path)
    config = QualityMetricsConfig(max_bytes=350, backup_count=1, max_record_bytes=350)
    writer = SQLiteQualityMetrics(database, release_version="1.2.3", config=config)
    for index in range(10):
        writer.record_sequence_loss(_loss(index))
    assert writer.close(timeout_seconds=2)

    retained = database.list(StateKind.QUALITY_EVENT)
    retained_bytes = sum(len(json.dumps(row, separators=(",", ":")).encode()) + 1 for _, row in retained)
    assert 0 < len(retained) < 10
    assert retained_bytes <= config.max_bytes * (config.backup_count + 1)
    assert max(str(row["session_id"]) for _, row in retained) == "session-9"


def test_database_failure_signals_quality_block(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    database = _database(tmp_path)
    signals: list[OperationalSignal] = []

    def fail_transaction():
        raise OSError("database unavailable")

    monkeypatch.setattr(database, "transaction", fail_transaction)
    caplog.set_level(logging.WARNING)
    writer = SQLiteQualityMetrics(database, release_version="1.2.3", config=QualityMetricsConfig(queue_max_records=2))
    writer.set_operational_signal(signals.append)
    writer.record_sequence_loss(_loss(1))
    assert writer.close(timeout_seconds=2)

    assert OperationalSignal.BLOCK in signals
    assert "quality metrics write_failed" in caplog.text
    assert writer.dropped_records == 0


def test_background_fatal_is_retained_and_raised_by_close(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    database = _database(tmp_path)
    writer = SQLiteQualityMetrics(database, release_version="1.2.3")
    fatal = FatalInternalAudioStorageError("canonical audio database is corrupt")
    written = Event()

    def fail_write(_item_id: str, _payload: dict[str, object]) -> None:
        written.set()
        raise fatal

    monkeypatch.setattr(writer, "_put_quality_event", fail_write)
    writer.record_sequence_loss(_loss(1))
    assert written.wait(2)

    with pytest.raises(FatalInternalAudioStorageError, match="corrupt"):
        writer.close(timeout_seconds=2)
    assert not writer._thread.is_alive()


def test_close_waits_for_inflight_database_work_and_joins_before_return(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    database = _database(tmp_path)
    writer = SQLiteQualityMetrics(database, release_version="1.2.3")
    write_started = Event()
    release_write = Event()
    original_write = writer._put_quality_event

    def blocked_write(item_id: str, payload: dict[str, object]) -> None:
        write_started.set()
        assert release_write.wait(5)
        original_write(item_id, payload)

    monkeypatch.setattr(writer, "_put_quality_event", blocked_write)
    writer.record_sequence_loss(_loss(1))
    assert write_started.wait(1)

    results: list[bool] = []
    close_finished = Event()

    def close_writer() -> None:
        results.append(writer.close(timeout_seconds=0.01))
        close_finished.set()

    closer = Thread(target=close_writer)
    closer.start()
    assert not close_finished.wait(0.05)
    release_write.set()
    closer.join(2)

    assert not closer.is_alive()
    assert results == [True]
    assert not writer._thread.is_alive()
    assert len(database.list(StateKind.QUALITY_EVENT)) == 1


def test_quality_writer_rejects_nonfinite_payload_before_queueing(tmp_path: Path) -> None:
    database = _database(tmp_path)
    writer = SQLiteQualityMetrics(database, release_version="1.2.3")
    event = ClockCorrectionMetric(
        "2026-10-11T01:02:03.456+00:00", "session", float("nan"), 1, 1, 1, "1.2.3", None, "1.0.0"
    )
    try:
        with pytest.raises(QualityMetricsError, match="cannot encode"):
            writer.record_clock_correction(event)
    finally:
        assert writer.close(timeout_seconds=2)
    assert database.list(StateKind.QUALITY_EVENT) == ()


def test_legacy_import_uses_a_stable_sortable_id_and_validates_schema(tmp_path: Path) -> None:
    database = _database(tmp_path)
    row = _loss(1).as_dict()
    item_id = quality_event_id(row, "legacy-000001")

    import_quality_event(database, row, item_id=item_id)

    assert database.get(StateKind.QUALITY_EVENT, item_id) == row
    with pytest.raises(QualityMetricsError, match="timestamp"):
        import_quality_event(database, row, item_id="legacy-000001")


def test_legacy_import_batches_rows_and_maintains_transactional_budget(tmp_path: Path) -> None:
    database = _database(tmp_path)
    rows = [(_loss(index).as_dict(), f"legacy-{index:03}") for index in range(1, 4)]
    import_quality_events(
        database,
        ((quality_event_id(row, suffix=suffix), row) for row, suffix in rows),
    )

    stored = database.list(StateKind.QUALITY_EVENT)
    budget = database.get(StateKind.QUALITY_EVENT_BUDGET, "current")
    assert len(stored) == 3
    assert budget is not None and isinstance(budget["value"], int) and budget["value"] > 0

    writer = SQLiteQualityMetrics(database, release_version="1.2.3")
    writer.record_sequence_loss(_loss(4))
    assert writer.close(timeout_seconds=2)
    assert database.get(StateKind.QUALITY_EVENT_BUDGET, "current") != budget
