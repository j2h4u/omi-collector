from __future__ import annotations

import json
import os
import threading
import time
from pathlib import Path
from typing import cast

import pytest

import omi_collector.capture.adapters.quality_metrics as quality_metrics_module
from omi_collector.capture.adapters.quality_metrics import (
    JsonlQualityMetrics,
    QualityMetricsError,
    normalize_source_revision,
    source_revision_from_environment,
)
from omi_collector.capture.application.quality_metrics import SequenceLossMetric, TransferSessionMetric
from omi_collector.config import QualityMetricsConfig


def _journal(tmp_path: Path) -> JsonlQualityMetrics:
    return JsonlQualityMetrics(tmp_path, release_version="1.2.3", source_revision="abcdef1234567890")


def test_append_only_jsonl_retains_complete_durable_low_rate_events(tmp_path: Path) -> None:
    journal = _journal(tmp_path)
    journal.record_transfer_session(
        TransferSessionMetric(
            "2026-09-02T01:02:03.456+00:00",
            "session-1",
            "omi",
            "collected",
            "completed",
            1234,
            2,
            888,
            888,
            888,
            journal.release_version,
            journal.source_revision,
            "1.0.0",
            "force_1m",
            -73,
        )
    )
    journal.record_sequence_loss(
        SequenceLossMetric(
            "2026-09-02T01:02:04.456+00:00",
            "session-1",
            "omi",
            3,
            1332,
            "device_cursor_advanced_before_host_durable_prefix",
            journal.release_version,
            journal.source_revision,
            "1.0.0",
        )
    )
    assert journal.close()

    lines = journal.path.read_text(encoding="utf-8").splitlines()
    transfer, loss = (cast(dict[str, object], json.loads(line)) for line in lines)
    assert transfer == {
        "schema_version": 1,
        "event": "transfer_session",
        "completed_at": "2026-09-02T01:02:03.456+00:00",
        "session_id": "session-1",
        "device_slug": "omi",
        "outcome": "collected",
        "termination_class": "completed",
        "active_read_elapsed_ms": 1234,
        "requested_record_count": 2,
        "record_size_bytes": 444,
        "received_raw_bytes": 888,
        "submitted_raw_bytes": 888,
        "written_raw_bytes": 888,
        "release_version": "1.2.3",
        "source_revision": "abcdef123456",
        "firmware_version": "1.0.0",
        "phy_policy": "force_1m",
        "advertisement_rssi_dbm": -73,
    }
    assert loss == {
        "schema_version": 1,
        "event": "sequence_loss",
        "occurred_at": "2026-09-02T01:02:04.456+00:00",
        "session_id": "session-1",
        "device_slug": "omi",
        "missing_record_count": 3,
        "missing_raw_bytes": 1332,
        "reason": "device_cursor_advanced_before_host_durable_prefix",
        "release_version": "1.2.3",
        "source_revision": "abcdef123456",
        "firmware_version": "1.0.0",
    }
    assert "loss_seconds" not in journal.path.read_text(encoding="utf-8")


@pytest.mark.parametrize("value", ["ABCDEF1", "abcdef", "abcdef1-", "abcdef1 "])
def test_source_revision_requires_deployment_provided_lowercase_hex(value: str) -> None:
    with pytest.raises(ValueError, match="lowercase hexadecimal"):
        normalize_source_revision(value)


def test_source_revision_is_read_only_from_deployment_environment() -> None:
    assert source_revision_from_environment({"OMI_COLLECTOR_SOURCE_REVISION": "abcdef1234567890"}) == "abcdef123456"
    assert source_revision_from_environment({}) is None


def test_journal_write_failure_is_reported_as_a_visible_diagnostic(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    journal = _journal(tmp_path)

    def fail_open(*_args: object, **_kwargs: object) -> int:
        raise OSError("full")

    monkeypatch.setattr(os, "open", fail_open)
    journal.record_sequence_loss(
        SequenceLossMetric("2026-09-02T01:02:04.456+00:00", "s", "omi", 1, 444, "reason", "1", None, None)
    )
    assert journal.close()
    assert "quality metrics write_failed" in caplog.text


def test_journal_rotates_complete_records_with_bounded_retention(tmp_path: Path) -> None:
    journal = JsonlQualityMetrics(
        tmp_path,
        release_version="1.2.3",
        config=QualityMetricsConfig(max_bytes=400, backup_count=2, max_record_bytes=399),
    )
    metric = SequenceLossMetric("2026-09-02T01:02:04.456+00:00", "s", "omi", 1, 444, "reason", "1", None, None)

    journal.record_sequence_loss(metric)
    journal.record_sequence_loss(metric)
    assert journal.close()

    assert journal.path.is_file()
    assert journal.path.with_name("quality.jsonl.1").is_file()
    for path in (journal.path, journal.path.with_name("quality.jsonl.1")):
        lines = path.read_bytes().splitlines(keepends=True)
        assert len(lines) == 1
        assert lines[0].endswith(b"\n")
        json.loads(lines[0])


def test_journal_rejects_oversized_record_before_opening_file(tmp_path: Path) -> None:
    journal = JsonlQualityMetrics(
        tmp_path,
        release_version="1.2.3",
        config=QualityMetricsConfig(max_bytes=100, backup_count=1, max_record_bytes=10),
    )
    with pytest.raises(QualityMetricsError, match="exceeds configured limit"):
        journal.record_sequence_loss(
            SequenceLossMetric("2026-09-02T01:02:04.456+00:00", "s", "omi", 1, 444, "reason", "1", None, None)
        )
    assert not journal.path.exists()
    assert journal.close()


def test_first_journal_creation_fsyncs_parent_after_file(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    journal = _journal(tmp_path)
    events: list[str] = []

    def observe_fsync(_descriptor: int) -> None:
        events.append("file")

    def observe_parent(_path: Path) -> None:
        events.append("parent")

    monkeypatch.setattr(os, "fsync", observe_fsync)
    monkeypatch.setattr(quality_metrics_module, "_fsync_parent", observe_parent)
    journal.record_sequence_loss(
        SequenceLossMetric("2026-09-02T01:02:04.456+00:00", "s", "omi", 1, 444, "reason", "1", None, None)
    )
    assert journal.close()
    assert events == ["file", "parent"]


def test_record_does_not_wait_for_a_blocked_filesystem_writer(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    journal = _journal(tmp_path)
    started = threading.Event()
    release = threading.Event()
    original_append = journal._append

    def blocked_append(line: bytes) -> None:
        started.set()
        release.wait(5)
        original_append(line)

    monkeypatch.setattr(journal, "_append", blocked_append)
    metric = SequenceLossMetric("2026-09-02T01:02:04.456+00:00", "s", "omi", 1, 444, "reason", "1", None, None)
    start = time.monotonic()
    journal.record_sequence_loss(metric)
    elapsed = time.monotonic() - start
    assert elapsed < 0.1
    assert started.wait(1)
    assert not journal.close(timeout_seconds=0.01)
    release.set()
    assert journal.close(timeout_seconds=1)


def test_close_races_with_enqueue_without_losing_the_ordered_stop(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    journal = _journal(tmp_path)
    entered = threading.Event()
    continue_put = threading.Event()
    original_put = journal._queue.put_nowait

    def paused_put(item: bytes | None) -> None:
        entered.set()
        assert continue_put.wait(1)
        original_put(item)

    monkeypatch.setattr(journal._queue, "put_nowait", paused_put)
    metric = SequenceLossMetric("2026-09-02T01:02:04.456+00:00", "s", "omi", 1, 444, "reason", "1", None, None)
    producer = threading.Thread(target=journal.record_sequence_loss, args=(metric,))
    producer.start()
    assert entered.wait(1)
    closer = threading.Thread(target=journal.close)
    closer.start()
    continue_put.set()
    producer.join(1)
    closer.join(1)
    assert not producer.is_alive()
    assert not closer.is_alive()
    assert journal.close()
    assert len(journal.path.read_text(encoding="utf-8").splitlines()) == 1


def test_full_queue_drops_auxiliary_event_without_blocking(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    config = QualityMetricsConfig(queue_max_records=1)
    journal = JsonlQualityMetrics(tmp_path, release_version="1.2.3", config=config)
    started = threading.Event()
    release = threading.Event()

    def blocked_append(_line: bytes) -> None:
        started.set()
        release.wait(5)

    monkeypatch.setattr(journal, "_append", blocked_append)
    metric = SequenceLossMetric("2026-09-02T01:02:04.456+00:00", "s", "omi", 1, 444, "reason", "1", None, None)
    journal.record_sequence_loss(metric)
    assert started.wait(1)
    journal.record_sequence_loss(metric)
    journal.record_sequence_loss(metric)
    assert journal.dropped_records == 1
    assert "quality metrics queue_full" in caplog.text
    assert not journal.close(timeout_seconds=0.01)
    release.set()
    assert journal.close(timeout_seconds=1)
