from __future__ import annotations

import json
from collections.abc import Callable
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path
from typing import cast

import pytest

import omi_collector.operator_status as status_module
from omi_collector.capture.adapters.firmware_observations import FirmwareObservationStore
from omi_collector.capture.adapters.quality_metrics import JsonlQualityMetrics
from omi_collector.capture.application.quality_metrics import (
    AdvertisementMetric,
    ClockCorrectionMetric,
    SequenceLossMetric,
    TransferSessionMetric,
)
from omi_collector.capture.domain.ring_protocol import RECORD_SIZE, RingInfo
from omi_collector.config import DEFAULT_CONFIG, QualityMetricsConfig
from omi_collector.operator_status import OperatorStatusError, collect_operator_status
from omi_collector.spool_metrics import FirmwareLifetimeMetrics, SpoolMetrics, SpoolWindowMetrics
from omi_collector.storage_layout import load_operator_config


def _layout(tmp_path: Path):
    path = tmp_path / "config.toml"
    path.write_text(
        '[pendant]\naddress = "AA:BB:CC:DD:EE:FF"\n'
        "[ready]\ntarget_audio_seconds = 3600.0\nmax_wait_seconds = 86400.0\n",
        encoding="utf-8",
    )
    (tmp_path / "collector").mkdir()
    return load_operator_config(path).storage


def _spool() -> SpoolMetrics:
    return SpoolMetrics(
        SpoolWindowMetrics(2, 30, 13_320, 0, 0, 0.0),
        FirmwareLifetimeMetrics(1, 0, 0, 0, 0, 1),
    )


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
    FirmwareObservationStore(layout.collector.device_state).record(RingInfo(10, 25, 100, 2, RECORD_SIZE))
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
    (layout.collector.root / "quality.jsonl").write_text(
        "".join(f"{json.dumps(event)}\n" for event in events), encoding="utf-8"
    )
    monkeypatch.setattr(status_module, "collect_spool_metrics", lambda *_args, **_kwargs: _spool())

    result = collect_operator_status(layout, hours=24, now=datetime(2026, 9, 8, 10, tzinfo=UTC))

    assert result["status"] == "attention"
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
    layout.collector.debug_log.write_text("".join(f"{json.dumps(row)}\n" for row in rows), encoding="utf-8")
    monkeypatch.setattr(status_module, "collect_spool_metrics", lambda *_args, **_kwargs: _spool())

    result = collect_operator_status(layout, hours=24, now=datetime(2026, 9, 8, 10, tzinfo=UTC))

    assert result["status"] == "attention"
    runtime = cast(dict[str, object], result["runtime"])
    assert runtime["attention_reasons"] == [
        "clock_correction_blocked",
        "quality_metrics_unavailable",
        "ready_publication_blocked",
    ]


def test_status_preserves_lock_context_from_session_error(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
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
    layout.collector.debug_log.write_text(f"{json.dumps(row)}\n", encoding="utf-8")
    monkeypatch.setattr(status_module, "collect_spool_metrics", lambda *_args, **_kwargs: _spool())

    result = collect_operator_status(layout, hours=24, now=datetime(2026, 9, 8, 10, tzinfo=UTC))

    runtime = cast(dict[str, object], result["runtime"])
    error = cast(dict[str, object], runtime["last_error"])
    assert error["lock_context"] == context


def test_status_clears_ready_publication_block_after_successful_noop_recovery(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    layout = _layout(tmp_path)
    rows = (
        {"event": "ready_publication_blocked", "fields": {}, "timestamp": "2026-09-08T09:00:00+00:00"},
        {"event": "ready_publication_recovered", "fields": {}, "timestamp": "2026-09-08T09:01:00+00:00"},
    )
    layout.collector.debug_log.write_text("".join(f"{json.dumps(row)}\n" for row in rows), encoding="utf-8")
    monkeypatch.setattr(status_module, "collect_spool_metrics", lambda *_args, **_kwargs: _spool())

    result = collect_operator_status(layout, hours=24, now=datetime(2026, 9, 8, 10, tzinfo=UTC))

    runtime = cast(dict[str, object], result["runtime"])
    assert runtime["attention_reasons"] == []
    assert result["status"] == "unknown"


def test_status_treats_expected_pendant_absence_as_healthy(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    layout = _layout(tmp_path)
    row = {
        "event": "sync_progress",
        "fields": {"progress": {"status": "away"}},
        "timestamp": "2026-09-08T09:00:00+00:00",
    }
    layout.collector.debug_log.write_text(f"{json.dumps(row)}\n", encoding="utf-8")
    monkeypatch.setattr(status_module, "collect_spool_metrics", lambda *_args, **_kwargs: _spool())

    result = collect_operator_status(layout, hours=24, now=datetime(2026, 9, 8, 10, tzinfo=UTC))

    assert result["status"] == "ok"
    assert cast(dict[str, object], result["runtime"])["state"] == "away"


def test_status_rejects_malformed_quality_evidence(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    layout = _layout(tmp_path)
    (layout.collector.root / "quality.jsonl").write_text("not-json\n", encoding="utf-8")
    monkeypatch.setattr(status_module, "collect_spool_metrics", lambda *_args, **_kwargs: _spool())

    with pytest.raises(OperatorStatusError, match="malformed JSON"):
        collect_operator_status(layout, hours=24)


def test_status_accepts_quality_record_without_deployment_revision(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    layout = _layout(tmp_path)
    event = _advertisement("2026-09-08T09:00:00+00:00")
    event["source_revision"] = None
    (layout.collector.root / "quality.jsonl").write_text(f"{json.dumps(event)}\n", encoding="utf-8")
    monkeypatch.setattr(status_module, "collect_spool_metrics", lambda *_args, **_kwargs: _spool())

    result = collect_operator_status(layout, hours=24, now=datetime(2026, 9, 8, 10, tzinfo=UTC))

    quality = cast(dict[str, object], result["quality_window"])
    assert quality["advertisements"] == 1


def test_status_accepts_nul_prefix_before_valid_debug_row(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    layout = _layout(tmp_path)
    row = {
        "event": "sync_progress",
        "fields": {"progress": {"status": "away"}},
        "timestamp": "2026-09-08T09:00:00+00:00",
    }
    layout.collector.debug_log.write_bytes(b"\x00" * 2_195 + json.dumps(row).encode() + b"\n")
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
            "fields": {"progress": {"event": "pendant_observation", "battery_percent": 96, "firmware": "3.0.21"}},
            "timestamp": "2026-09-08T09:00:00+00:00",
        },
        {
            "event": "sync_progress",
            "fields": {"progress": {"event": "pendant_observation", "firmware": "3.0.22"}},
            "timestamp": "2026-09-08T09:00:01+00:00",
        },
        {
            "event": "sync_progress",
            "fields": {
                "progress": {
                    "status": "progress",
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
    layout.collector.debug_log.write_text("".join(f"{json.dumps(row)}\n" for row in rows), encoding="utf-8")
    monkeypatch.setattr(status_module, "collect_spool_metrics", lambda *_args, **_kwargs: _spool())

    result = collect_operator_status(layout, hours=24, now=datetime(2026, 9, 8, 9, 0, 10, tzinfo=UTC))

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


def test_status_rejects_invalid_runtime_rssi_type(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    layout = _layout(tmp_path)
    row = {
        "event": "ble_link_rssi_observed",
        "fields": {"rssi_dbm": "not-an-integer"},
        "timestamp": "2026-09-08T09:00:00+00:00",
    }
    layout.collector.debug_log.write_text(f"{json.dumps(row)}\n", encoding="utf-8")
    monkeypatch.setattr(status_module, "collect_spool_metrics", lambda *_args, **_kwargs: _spool())

    with pytest.raises(OperatorStatusError, match="invalid rssi_dbm"):
        collect_operator_status(layout, hours=24, now=datetime(2026, 9, 8, 10, tzinfo=UTC))


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("firmware_version", 42, "invalid firmware_version"),
        ("advertisement_rssi_dbm", True, "invalid advertisement_rssi_dbm"),
        ("advertisement_rssi_dbm", "not-an-integer", "invalid advertisement_rssi_dbm"),
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
    (layout.collector.root / "quality.jsonl").write_text(f"{json.dumps(event)}\n", encoding="utf-8")
    monkeypatch.setattr(status_module, "collect_spool_metrics", lambda *_args, **_kwargs: _spool())

    with pytest.raises(OperatorStatusError, match=message):
        collect_operator_status(layout, hours=24, now=datetime(2026, 9, 8, 10, tzinfo=UTC))


def test_status_reports_exact_limit_quality_journal_from_real_writer(
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

    def encoded_line(value: TransferSessionMetric) -> bytes:
        return json.dumps(value.as_dict(), ensure_ascii=False, separators=(",", ":"), allow_nan=False).encode() + b"\n"

    padding = limit - len(encoded_line(metric))
    assert padding >= 0
    metric = replace(metric, session_id=metric.session_id + "s" * padding)
    expected_line = encoded_line(metric)
    assert len(expected_line) == limit

    writer = JsonlQualityMetrics(layout.collector.root, release_version="1.2.3", config=quality_config)
    writer.record_transfer_session(metric)
    assert writer.close()
    written_line = writer.path.read_bytes()
    assert len(written_line) == limit
    written_event = cast(dict[str, object], json.loads(written_line))
    assert written_event["event"] == "transfer_session"
    assert written_event["session_id"] == metric.session_id
    assert written_event["written_raw_bytes"] == 444
    monkeypatch.setattr(
        status_module,
        "DEFAULT_CONFIG",
        replace(
            DEFAULT_CONFIG,
            observability=replace(DEFAULT_CONFIG.observability, quality_metrics=quality_config),
        ),
    )
    monkeypatch.setattr(status_module, "collect_spool_metrics", lambda *_args, **_kwargs: _spool())

    result = collect_operator_status(layout, hours=24, now=datetime(2026, 9, 8, 10, tzinfo=UTC))

    quality = cast(dict[str, object], result["quality_window"])
    assert quality["transfer_sessions"] == 1
    assert quality["written_raw_bytes"] == 444


def test_status_rejects_non_mapping_sync_progress_as_handled_status_error(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    layout = _layout(tmp_path)
    row = {
        "event": "sync_progress",
        "fields": {"progress": "malformed"},
        "timestamp": "2026-09-08T09:00:00+00:00",
    }
    layout.collector.debug_log.write_text(f"{json.dumps(row)}\n", encoding="utf-8")
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
    (layout.collector.root / "quality.jsonl").write_text(
        "".join(f"{json.dumps(event)}\n" for event in events), encoding="utf-8"
    )
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
    (layout.collector.root / "quality.jsonl").write_text(f"{json.dumps(event)}\n", encoding="utf-8")
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
            "fields": {"progress": {"event": "pendant_observation", "battery_percent": 88}},
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
            "fields": {"progress": {"event": "pendant_observation", "battery_percent": 12}},
            "timestamp": "2026-09-08T08:57:00+00:00",
        },
    )
    layout.collector.debug_log.write_text("".join(f"{json.dumps(row)}\n" for row in rows), encoding="utf-8")
    monkeypatch.setattr(status_module, "collect_spool_metrics", lambda *_args, **_kwargs: _spool())

    result = collect_operator_status(layout, hours=24, now=datetime(2026, 9, 8, 10, tzinfo=UTC))

    runtime = cast(dict[str, object], result["runtime"])
    assert runtime["battery_percent"] == 88
    assert runtime["connection_rssi_dbm"] == -42
    assert cast(dict[str, object], runtime["last_error"])["error_message"] == "newer"
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
    (layout.collector.root / "quality.jsonl").write_text(
        "".join(f"{json.dumps(event)}\n" for event in events), encoding="utf-8"
    )
    monkeypatch.setattr(status_module, "collect_spool_metrics", lambda *_args, **_kwargs: _spool())

    result = collect_operator_status(layout, hours=24, now=datetime(2026, 9, 8, 10, tzinfo=UTC))

    quality = cast(dict[str, object], result["quality_window"])
    assert quality["last_advertisement_at"] == "2026-09-08T09:04:00.000+00:00"
    assert quality["last_transfer_outcome"] == "newer"
    assert quality["last_clock_correction_at"] == "2026-09-08T09:02:00.000+00:00"


def test_status_includes_both_quality_window_edges_and_equal_clock_sequences(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    layout = _layout(tmp_path)
    start = _advertisement("2026-09-08T09:00:00+00:00")
    end = _advertisement("2026-09-08T10:00:00+00:00")
    correction = _clock_correction("2026-09-08T10:00:00+00:00")
    correction["boundary_sequence_min"] = 4
    correction["boundary_sequence_max"] = 4
    (layout.collector.root / "quality.jsonl").write_text(
        "".join(f"{json.dumps(event)}\n" for event in (start, end, correction)), encoding="utf-8"
    )
    monkeypatch.setattr(status_module, "collect_spool_metrics", lambda *_args, **_kwargs: _spool())

    result = collect_operator_status(layout, hours=1, now=datetime(2026, 9, 8, 10, tzinfo=UTC))

    quality = cast(dict[str, object], result["quality_window"])
    assert quality["advertisements"] == 2
    assert quality["clock_corrections"] == 1


def test_status_returns_empty_quality_window_when_collector_root_is_absent(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    layout = _layout(tmp_path)
    layout.collector.root.rmdir()
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
        "fields": {"progress": {"status": "progress", "records_completed": completed, "records_total": total}},
        "timestamp": "2026-09-08T09:03:00+00:00",
    }
    layout.collector.debug_log.write_text(f"{json.dumps(row)}\n", encoding="utf-8")
    monkeypatch.setattr(status_module, "collect_spool_metrics", lambda *_args, **_kwargs: _spool())

    result = collect_operator_status(layout, hours=24, now=datetime(2026, 9, 8, 10, tzinfo=UTC))

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
    (layout.collector.root / "quality.jsonl").write_text(f"{json.dumps(event)}\n", encoding="utf-8")
    monkeypatch.setattr(status_module, "collect_spool_metrics", lambda *_args, **_kwargs: _spool())

    with pytest.raises(OperatorStatusError, match="invalid"):
        collect_operator_status(layout, hours=24, now=datetime(2026, 9, 8, 10, tzinfo=UTC))
