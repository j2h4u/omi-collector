from __future__ import annotations

import json
import os
import stat
import threading
import time
from multiprocessing import get_context
from multiprocessing.connection import Connection
from pathlib import Path
from types import SimpleNamespace
from typing import cast

import pytest

import omi_collector.capture.adapters.quality_metrics as quality_metrics_module
from omi_collector.capture.adapters.quality_metrics import (
    JsonlQualityMetrics,
    QualityMetricsError,
    normalize_source_revision,
    source_revision_from_release_metadata,
)
from omi_collector.capture.application.quality_metrics import (
    AdvertisementMetric,
    ClockCorrectionMetric,
    SequenceLossMetric,
    TransferSessionMetric,
)
from omi_collector.config import QualityMetricsConfig


def _journal(tmp_path: Path) -> JsonlQualityMetrics:
    return JsonlQualityMetrics(tmp_path, release_version="1.2.3", source_revision="a" * 40)


def _loss_metric(session_id: str = "s", *, reason: str = "reason") -> SequenceLossMetric:
    return SequenceLossMetric("2026-09-02T01:02:04.456+00:00", session_id, 1, 444, reason, "1", None, None)


def _is_journal_fd(descriptor: int, journal: JsonlQualityMetrics) -> bool:
    try:
        return Path(f"/proc/self/fd/{descriptor}").samefile(journal.path)
    except OSError:
        return False


def _quality_close_child(root: str, sender: Connection) -> None:
    journal = JsonlQualityMetrics(Path(root), release_version="1.2.3", source_revision="a" * 40)
    entered_fsync = threading.Event()
    never_release = threading.Event()
    original_fsync = os.fsync

    def block_journal_fsync(descriptor: int) -> None:
        if _is_journal_fd(descriptor, journal):
            entered_fsync.set()
            never_release.wait()
        else:
            original_fsync(descriptor)

    os.fsync = block_journal_fsync
    try:
        journal.record_sequence_loss(_loss_metric("process-exit"))
        if not entered_fsync.wait(timeout=2):
            raise AssertionError("quality writer did not reach fsync")
        sender.send(journal.close(timeout_seconds=0.01))
    finally:
        os.fsync = original_fsync
        sender.close()


def test_bounded_close_does_not_leave_quality_writer_pinning_process_exit(tmp_path: Path) -> None:
    context = get_context("fork")
    receiver, sender = context.Pipe(duplex=False)
    process = context.Process(target=_quality_close_child, args=(str(tmp_path), sender))
    try:
        process.start()
        sender.close()
        assert receiver.poll(5), "child did not report the bounded close result"
        assert receiver.recv() is False
        process.join(timeout=5)
        assert process.exitcode == 0
    finally:
        sender.close()
        receiver.close()
        if process.is_alive():
            process.terminate()
            process.join(timeout=5)
        if process.is_alive():
            process.kill()
            process.join(timeout=5)
        if process.pid is not None and not process.is_alive():
            process.close()


def test_append_only_jsonl_retains_complete_durable_low_rate_events(tmp_path: Path) -> None:
    journal = _journal(tmp_path)
    journal.record_advertisement(
        AdvertisementMetric(
            "2026-09-02T01:02:02.456+00:00",
            "session-1",
            -73,
            journal.release_version,
            journal.source_revision,
            "force_1m",
        )
    )
    journal.record_transfer_session(
        TransferSessionMetric(
            "2026-09-02T01:02:03.456+00:00",
            "session-1",
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
            3,
            1332,
            "device_cursor_advanced_before_host_durable_prefix",
            journal.release_version,
            journal.source_revision,
            "1.0.0",
        )
    )
    journal.record_clock_correction(
        ClockCorrectionMetric(
            "2026-09-02T01:02:05.456+00:00",
            "session-1",
            2359.68,
            1789128032,
            5898589,
            5898591,
            journal.release_version,
            journal.source_revision,
            "1.0.0",
        )
    )
    assert journal.close(timeout_seconds=5)

    lines = journal.path.read_text(encoding="utf-8").splitlines()
    advertisement, transfer, loss, correction = (cast(dict[str, object], json.loads(line)) for line in lines)
    assert advertisement == {
        "schema_version": 2,
        "event": "advertisement_observation",
        "recorded_at": "2026-09-02T01:02:02.456+00:00",
        "session_id": "session-1",
        "advertisement_rssi_dbm": -73,
        "release_version": "1.2.3",
        "source_revision": "aaaaaaaaaaaa",
        "phy_policy": "force_1m",
    }
    assert transfer == {
        "schema_version": 2,
        "event": "transfer_session",
        "completed_at": "2026-09-02T01:02:03.456+00:00",
        "session_id": "session-1",
        "outcome": "collected",
        "termination_class": "completed",
        "active_read_elapsed_ms": 1234,
        "requested_record_count": 2,
        "record_size_bytes": 444,
        "received_raw_bytes": 888,
        "submitted_raw_bytes": 888,
        "written_raw_bytes": 888,
        "release_version": "1.2.3",
        "source_revision": "aaaaaaaaaaaa",
        "firmware_version": "1.0.0",
        "phy_policy": "force_1m",
        "advertisement_rssi_dbm": -73,
    }
    assert loss == {
        "schema_version": 2,
        "event": "sequence_loss",
        "occurred_at": "2026-09-02T01:02:04.456+00:00",
        "session_id": "session-1",
        "missing_record_count": 3,
        "missing_raw_bytes": 1332,
        "reason": "device_cursor_advanced_before_host_durable_prefix",
        "release_version": "1.2.3",
        "source_revision": "aaaaaaaaaaaa",
        "firmware_version": "1.0.0",
    }
    assert correction == {
        "schema_version": 2,
        "event": "clock_correction",
        "occurred_at": "2026-09-02T01:02:05.456+00:00",
        "session_id": "session-1",
        "drift_seconds": 2359.68,
        "target_epoch": 1789128032,
        "boundary_sequence_min": 5898589,
        "boundary_sequence_max": 5898591,
        "release_version": "1.2.3",
        "source_revision": "aaaaaaaaaaaa",
        "firmware_version": "1.0.0",
    }
    assert "loss_seconds" not in journal.path.read_text(encoding="utf-8")
    assert stat.S_IMODE(journal.path.stat().st_mode) == 0o600


def test_nonfinite_clock_correction_is_rejected_without_persisting_invalid_jsonl(tmp_path: Path) -> None:
    journal = _journal(tmp_path)
    metric = ClockCorrectionMetric(
        "2026-09-02T01:02:05.456+00:00",
        "nonfinite-drift",
        float("nan"),
        1789128032,
        5898589,
        5898591,
        journal.release_version,
        journal.source_revision,
        "1.0.0",
    )

    try:
        with pytest.raises(QualityMetricsError, match="cannot encode quality metrics") as raised:
            journal.record_clock_correction(metric)
        assert isinstance(raised.value.__cause__, ValueError)
        assert journal.close(timeout_seconds=1)
    finally:
        assert journal.close(timeout_seconds=1)

    assert not journal.path.exists()


@pytest.mark.parametrize("value", ["A" * 40, "a" * 39, "a" * 65, "a" * 39 + "-"])
def test_source_revision_requires_deployment_provided_lowercase_hex(value: str) -> None:
    with pytest.raises(ValueError, match="lowercase hexadecimal"):
        normalize_source_revision(value)


def test_source_revision_is_read_from_release_metadata(tmp_path: Path) -> None:
    path = tmp_path / "release.json"
    path.write_text(json.dumps({"source_revision": "a" * 40}), encoding="utf-8")
    revision = source_revision_from_release_metadata(path)
    assert revision == "a" * 40
    journal = JsonlQualityMetrics(tmp_path, release_version="1.2.3", source_revision=revision)
    assert journal.source_revision == "a" * 12
    assert journal.close()
    assert source_revision_from_release_metadata(tmp_path / "missing.json") is None


def test_source_revision_defaults_to_release_metadata_under_python_prefix(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    prefix = tmp_path / "injected-prefix"
    metadata = prefix / "share" / "omi-collector" / "release.json"
    metadata.parent.mkdir(parents=True)
    metadata.write_text(json.dumps({"source_revision": "b" * 40}), encoding="utf-8")
    monkeypatch.setattr(quality_metrics_module, "sys", SimpleNamespace(prefix=str(prefix)))

    assert source_revision_from_release_metadata() == "b" * 40


def test_release_metadata_rejects_extra_keys_with_valid_revision(tmp_path: Path) -> None:
    path = tmp_path / "release.json"
    path.write_text(json.dumps({"source_revision": "a" * 40, "channel": "stable"}), encoding="utf-8")

    with pytest.raises(ValueError, match="schema is invalid"):
        source_revision_from_release_metadata(path)


def test_empty_release_version_is_rejected_before_writer_thread_starts(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    def reject_thread_start(_thread: threading.Thread) -> None:
        raise AssertionError("quality writer started before constructor validation")

    monkeypatch.setattr(quality_metrics_module.Thread, "start", reject_thread_start)

    with pytest.raises(ValueError, match="release version must be a non-empty string"):
        JsonlQualityMetrics(tmp_path, release_version="")


def test_journal_write_failure_is_reported_as_a_visible_diagnostic(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    journal = _journal(tmp_path)
    real_open = os.open

    def fail_journal_open(path: str | os.PathLike[str], flags: int, mode: int | None = None) -> int:
        if Path(path).resolve() == journal.path.resolve():
            raise OSError("full")
        if mode is None:
            return real_open(path, flags)
        return real_open(path, flags, mode)

    monkeypatch.setattr(os, "open", fail_journal_open)
    try:
        journal.record_sequence_loss(_loss_metric())
        assert journal.close()
        assert "quality metrics write_failed" in caplog.text
    finally:
        assert journal.close()


def test_journal_rotates_complete_records_with_bounded_retention(tmp_path: Path) -> None:
    journal = JsonlQualityMetrics(
        tmp_path,
        release_version="1.2.3",
        config=QualityMetricsConfig(max_bytes=400, backup_count=2, max_record_bytes=399),
    )
    metric = SequenceLossMetric("2026-09-02T01:02:04.456+00:00", "s", 1, 444, "reason", "1", None, None)

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


def test_rotation_caps_real_serialized_bytes_and_preserves_retained_event_order(tmp_path: Path) -> None:
    def line_for(index: int) -> tuple[SequenceLossMetric, bytes]:
        metric = _loss_metric(f"event-{index:02}", reason="reason é")
        line = (
            json.dumps(metric.as_dict(), ensure_ascii=False, separators=(",", ":"), allow_nan=False).encode("utf-8")
            + b"\n"
        )
        return metric, line

    first_metric, first_line = line_for(0)
    record_bytes = len(first_line)
    journal = JsonlQualityMetrics(
        tmp_path,
        release_version="1.2.3",
        config=QualityMetricsConfig(max_bytes=2 * record_bytes, backup_count=2, max_record_bytes=record_bytes),
    )
    try:
        journal.record_sequence_loss(first_metric)
        journal.record_sequence_loss(line_for(1)[0])
        deadline = time.monotonic() + 1
        while time.monotonic() < deadline and (
            not journal.path.exists() or journal.path.stat().st_size != 2 * record_bytes
        ):
            time.sleep(0.01)
        assert journal.path.stat().st_size == 2 * record_bytes
        assert not journal.path.with_name(f"{journal.path.name}.1").exists()

        for index in range(2, 6):
            journal.record_sequence_loss(line_for(index)[0])
        assert journal.close(timeout_seconds=1)
    finally:
        assert journal.close(timeout_seconds=1)

    retained = (
        journal.path.with_name(f"{journal.path.name}.2"),
        journal.path.with_name(f"{journal.path.name}.1"),
        journal.path,
    )
    assert all(path.stat().st_size <= 2 * record_bytes for path in retained)
    retained_ids: list[str] = []
    for path in retained:
        records = [cast(dict[str, object], json.loads(line)) for line in path.read_text(encoding="utf-8").splitlines()]
        assert len(records) == 2
        retained_ids.extend(cast(str, record["session_id"]) for record in records)
    assert retained_ids == [f"event-{index:02}" for index in range(6)]


def test_restart_rotates_one_byte_interrupted_journal_before_max_size_record(tmp_path: Path) -> None:
    metric = _loss_metric("restart-boundary")
    calibration = _journal(tmp_path / "calibration")
    calibration.record_sequence_loss(metric)
    assert calibration.close(timeout_seconds=1)
    incoming_line = calibration.path.read_bytes()
    max_bytes = len(incoming_line)

    journal = JsonlQualityMetrics(
        tmp_path / "restarted",
        release_version="1.2.3",
        source_revision="a" * 40,
        config=QualityMetricsConfig(max_bytes=max_bytes, backup_count=1, max_record_bytes=max_bytes),
    )
    journal.path.parent.mkdir(parents=True, exist_ok=True)
    journal.path.write_bytes(b"x")

    try:
        journal.record_sequence_loss(metric)
        assert journal.close(timeout_seconds=1)
    finally:
        assert journal.close(timeout_seconds=1)

    backup = journal.path.with_name(f"{journal.path.name}.1")
    assert backup.read_bytes() == b"x"
    assert journal.path.stat().st_size == max_bytes
    assert journal.path.read_bytes() == incoming_line
    assert json.loads(journal.path.read_bytes()) == metric.as_dict()


def test_rotation_preserves_neighbor_beyond_configured_backup_count(tmp_path: Path) -> None:
    first_metric = _loss_metric("rotate-a")
    next_metric = _loss_metric("rotate-b")
    calibration = _journal(tmp_path / "calibration")
    calibration.record_sequence_loss(next_metric)
    assert calibration.close(timeout_seconds=1)
    max_bytes = calibration.path.stat().st_size

    root = tmp_path / "journal"
    config = QualityMetricsConfig(max_bytes=max_bytes, backup_count=3, max_record_bytes=max_bytes)
    initial = JsonlQualityMetrics(
        root,
        release_version="1.2.3",
        source_revision="a" * 40,
        config=config,
    )
    initial.record_sequence_loss(first_metric)
    assert initial.close(timeout_seconds=1)
    current_bytes = initial.path.read_bytes()

    seed_backups = [json.dumps({"seed": index}).encode() + b"\n" for index in range(1, 5)]
    for index, seed in enumerate(seed_backups, start=1):
        initial.path.with_name(f"{initial.path.name}.{index}").write_bytes(seed)

    restarted = JsonlQualityMetrics(
        root,
        release_version="1.2.3",
        source_revision="a" * 40,
        config=config,
    )
    try:
        restarted.record_sequence_loss(next_metric)
        assert restarted.close(timeout_seconds=1)
    finally:
        assert restarted.close(timeout_seconds=1)

    assert restarted.path.with_name(f"{restarted.path.name}.1").read_bytes() == current_bytes
    assert restarted.path.with_name(f"{restarted.path.name}.2").read_bytes() == seed_backups[0]
    assert restarted.path.with_name(f"{restarted.path.name}.3").read_bytes() == seed_backups[1]
    assert restarted.path.with_name(f"{restarted.path.name}.4").read_bytes() == seed_backups[3]


def test_journal_rejects_oversized_record_before_opening_file(tmp_path: Path) -> None:
    journal = JsonlQualityMetrics(
        tmp_path,
        release_version="1.2.3",
        config=QualityMetricsConfig(max_bytes=100, backup_count=1, max_record_bytes=10),
    )
    with pytest.raises(QualityMetricsError, match="exceeds configured limit"):
        journal.record_sequence_loss(
            SequenceLossMetric("2026-09-02T01:02:04.456+00:00", "s", 1, 444, "reason", "1", None, None)
        )
    assert not journal.path.exists()
    assert journal.close()


def test_journal_creates_missing_collector_parents_and_appends_jsonl(tmp_path: Path) -> None:
    root = tmp_path / "missing" / "collector" / "nested"
    journal = JsonlQualityMetrics(root, release_version="1.2.3", source_revision="a" * 40)
    metric = _loss_metric("nested-parent")
    expected = (
        json.dumps(metric.as_dict(), ensure_ascii=False, separators=(",", ":"), allow_nan=False).encode("utf-8") + b"\n"
    )
    try:
        journal.record_sequence_loss(metric)
        assert journal.close(timeout_seconds=1)
    finally:
        assert journal.close(timeout_seconds=1)

    assert journal.path.parent.is_dir()
    assert journal.path.read_bytes() == expected


def test_short_journal_writes_complete_the_exact_jsonl_record(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    journal = _journal(tmp_path)
    real_write = os.write

    def short_journal_write(descriptor: int, payload: bytes) -> int:
        if _is_journal_fd(descriptor, journal) and payload:
            return real_write(descriptor, payload[:1])
        return real_write(descriptor, payload)

    monkeypatch.setattr(os, "write", short_journal_write)
    metric = _loss_metric("short-write")
    expected = (
        json.dumps(metric.as_dict(), ensure_ascii=False, separators=(",", ":"), allow_nan=False).encode("utf-8") + b"\n"
    )
    try:
        journal.record_sequence_loss(metric)
        assert journal.close(timeout_seconds=1)
    finally:
        assert journal.close(timeout_seconds=1)

    assert journal.path.read_bytes() == expected


def test_zero_progress_write_is_reported_without_hanging_or_writing_a_partial_line(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    journal = _journal(tmp_path)
    real_write = os.write
    first_journal_write = True

    def zero_then_write(descriptor: int, payload: bytes) -> int:
        nonlocal first_journal_write
        if _is_journal_fd(descriptor, journal) and first_journal_write:
            first_journal_write = False
            return 0
        return real_write(descriptor, payload)

    monkeypatch.setattr(os, "write", zero_then_write)
    try:
        journal.record_sequence_loss(_loss_metric("zero-write"))
        assert journal.close(timeout_seconds=1)
    finally:
        assert journal.close(timeout_seconds=1)

    assert "quality metrics write_failed" in caplog.text
    assert journal.path.read_bytes() == b""


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
        SequenceLossMetric("2026-09-02T01:02:04.456+00:00", "s", 1, 444, "reason", "1", None, None)
    )
    assert journal.close()
    assert events == ["file", "parent"]


def test_record_does_not_wait_for_blocked_writer_and_repeated_close_preserves_stop(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    journal = JsonlQualityMetrics(
        tmp_path,
        release_version="1.2.3",
        source_revision="a" * 40,
        config=QualityMetricsConfig(queue_max_records=1),
    )
    started = threading.Event()
    release = threading.Event()
    real_write = os.write

    def blocked_write(descriptor: int, payload: bytes) -> int:
        if _is_journal_fd(descriptor, journal) and not started.is_set():
            started.set()
            assert release.wait(5)
        return real_write(descriptor, payload)

    monkeypatch.setattr(os, "write", blocked_write)
    try:
        start = time.monotonic()
        journal.record_sequence_loss(_loss_metric("blocked-write"))
        elapsed = time.monotonic() - start
        assert elapsed < 0.1
        assert started.wait(1)
        assert not journal.close(timeout_seconds=0.01)
        assert journal.dropped_records == 0
        assert "quality metrics shutdown_timeout" in caplog.text
        assert not journal.close(timeout_seconds=0.01)
        assert journal.dropped_records == 0
    finally:
        release.set()
        assert journal.close(timeout_seconds=1)

    assert len(journal.path.read_text(encoding="utf-8").splitlines()) == 1


def test_unblocked_close_does_not_warn_and_post_close_records_are_counted(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    journal = _journal(tmp_path)
    metric = _loss_metric("after-close")
    try:
        journal.record_sequence_loss(metric)
        assert journal.close(timeout_seconds=1)
        durable_bytes = journal.path.read_bytes()
        assert "quality metrics shutdown_timeout" not in caplog.text

        journal.record_sequence_loss(metric)
        assert journal.dropped_records == 1
        assert "quality metrics writer_closed" in caplog.text
        assert journal.path.read_bytes() == durable_bytes
    finally:
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
    metric = SequenceLossMetric("2026-09-02T01:02:04.456+00:00", "s", 1, 444, "reason", "1", None, None)
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


def test_concurrent_close_waits_until_the_stop_sentinel_is_queued(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    journal = _journal(tmp_path)
    stop_entered = threading.Event()
    release_stop = threading.Event()
    second_done = threading.Event()
    results: list[bool] = []
    original_put = journal._queue.put_nowait

    def paused_stop(item: bytes | None) -> None:
        if item is None:
            stop_entered.set()
            assert release_stop.wait(1)
        original_put(item)

    def close_second() -> None:
        results.append(journal.close(timeout_seconds=1))
        second_done.set()

    monkeypatch.setattr(journal._queue, "put_nowait", paused_stop)
    first = threading.Thread(target=lambda: results.append(journal.close(timeout_seconds=1)))
    second = threading.Thread(target=close_second)
    first.start()
    assert stop_entered.wait(1)
    second.start()
    assert not second_done.wait(0.05)
    release_stop.set()
    first.join(1)
    second.join(1)
    assert not first.is_alive()
    assert not second.is_alive()
    assert results == [True, True]


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
    metric = SequenceLossMetric("2026-09-02T01:02:04.456+00:00", "s", 1, 444, "reason", "1", None, None)
    try:
        journal.record_sequence_loss(metric)
        assert started.wait(1)
        journal.record_sequence_loss(metric)
        journal.record_sequence_loss(metric)
        assert journal.dropped_records == 1
        assert "quality metrics queue_full" in caplog.text
        assert not journal.close(timeout_seconds=0.01)
    finally:
        release.set()
        assert journal.close(timeout_seconds=1)


def test_async_write_failure_blocks_and_a_later_durable_append_clears(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from omi_collector.capture.domain.operational_status_machine import OperationalSignal

    signals: list[OperationalSignal] = []
    recovered = threading.Event()

    def record(signal: OperationalSignal) -> None:
        signals.append(signal)
        if signal is OperationalSignal.CLEAR:
            recovered.set()

    journal = JsonlQualityMetrics(tmp_path, release_version="1.2.3")
    journal.set_operational_signal(record)
    real_append = journal._append
    attempts = 0

    def fail_once(line: bytes) -> None:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise OSError("disk unavailable")
        real_append(line)

    monkeypatch.setattr(journal, "_append", fail_once)
    try:
        journal.record_sequence_loss(_loss_metric("first"))
        journal.record_sequence_loss(_loss_metric("recovery"))
        assert recovered.wait(2)
    finally:
        assert journal.close(timeout_seconds=2)

    assert signals == [OperationalSignal.CONFIGURED, OperationalSignal.BLOCK, OperationalSignal.CLEAR]
