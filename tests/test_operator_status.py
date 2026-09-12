from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import cast

import pytest

import omi_collector.operator_status as status_module
from omi_collector.capture.adapters.firmware_observations import FirmwareObservationStore
from omi_collector.capture.application.quality_metrics import (
    AdvertisementMetric,
    SequenceLossMetric,
    TransferSessionMetric,
)
from omi_collector.capture.domain.ring_protocol import RECORD_SIZE, RingInfo
from omi_collector.operator_status import OperatorStatusError, collect_operator_status
from omi_collector.spool_metrics import FirmwareLifetimeMetrics, SpoolMetrics, SpoolWindowMetrics
from omi_collector.storage_layout import load_storage_layout


def _layout(tmp_path: Path) -> Path:
    path = tmp_path / "layout.toml"
    path.write_text(
        """version = 2

[collector]
root = "collector"
attempts = "attempts"
quarantine = "quarantine"
lock = "collector.lock"
device_state = "device.json"
debug_log = "debug.jsonl"

[publication]
root = "source"
""",
        encoding="utf-8",
    )
    (tmp_path / "collector").mkdir()
    return path


def _spool() -> SpoolMetrics:
    return SpoolMetrics(
        SpoolWindowMetrics(2, 30, 13_320, 0, 0, 0.0),
        FirmwareLifetimeMetrics(1, 0, 0, 0, 0, 1),
    )


def _advertisement(timestamp: str) -> dict[str, object]:
    return AdvertisementMetric(timestamp, "session-1", "omi", -91, "1.2.3", "abcdef123456", "auto").as_dict()


def _transfer(timestamp: str, *, outcome: str, termination_class: str, written_raw_bytes: int) -> dict[str, object]:
    return TransferSessionMetric(
        timestamp,
        f"session-{timestamp[-2:]}",
        "omi",
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
        "omi",
        2,
        888,
        "device_cursor_advanced_before_host_durable_prefix",
        "1.2.3",
        "abcdef123456",
        "1.0.0",
    ).as_dict()


def test_status_summarizes_backlog_transfer_quality_and_loss(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    layout = load_storage_layout(_layout(tmp_path))
    FirmwareObservationStore(layout.collector.device_state).record("omi", RingInfo(10, 25, 100, 2, RECORD_SIZE))
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
    )
    (layout.collector.root / "quality.jsonl").write_text(
        "".join(f"{json.dumps(event)}\n" for event in events), encoding="utf-8"
    )
    monkeypatch.setattr(status_module, "collect_spool_metrics", lambda *_args, **_kwargs: _spool())

    result = collect_operator_status(layout, "omi", hours=24, now=datetime(2026, 9, 8, 10, tzinfo=UTC))

    assert result["status"] == "attention"
    assert result["device"] == {
        "capacity_packets": 100,
        "dropped_packets": 2,
        "read_sequence": 10,
        "unread_bytes": 6_660,
        "unread_packets": 15,
        "write_sequence": 25,
    }
    assert result["runtime"] == {
        "battery_observed_at": None,
        "battery_percent": None,
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
        "last_advertisement_at": "2026-09-08T09:00:00.000+00:00",
        "last_advertisement_rssi_dbm": -91,
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


def test_status_rejects_malformed_quality_evidence(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    layout = load_storage_layout(_layout(tmp_path))
    (layout.collector.root / "quality.jsonl").write_text("not-json\n", encoding="utf-8")
    monkeypatch.setattr(status_module, "collect_spool_metrics", lambda *_args, **_kwargs: _spool())

    with pytest.raises(OperatorStatusError, match="malformed JSON"):
        collect_operator_status(layout, "omi", hours=24)


def test_status_reports_latest_battery_and_active_transfer(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    layout = load_storage_layout(_layout(tmp_path))
    rows = (
        {
            "event": "sync_progress",
            "fields": {"progress": {"event": "pendant_observation", "battery_percent": 96, "firmware": "3.0.21"}},
            "timestamp": "2026-09-08T09:00:00+00:00",
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

    result = collect_operator_status(layout, "omi", hours=24, now=datetime(2026, 9, 8, 9, 0, 10, tzinfo=UTC))

    runtime = cast(dict[str, object], result["runtime"])
    assert runtime["battery_percent"] == 96
    assert runtime["battery_observed_at"] == "2026-09-08T09:00:00.000+00:00"
    assert runtime["firmware"] == "3.0.21"
    assert runtime["last_error"] is None
    assert runtime["state"] == "transferring"
    assert runtime["updated_age_seconds"] == 5
    transfer = cast(dict[str, object], runtime["transfer"])
    assert transfer["fraction_complete"] == 0.25
    assert transfer["remaining_bytes"] == 1_332


def test_status_marks_latest_fatal_transfer_as_attention_and_excludes_future_rows(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    layout = load_storage_layout(_layout(tmp_path))
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

    result = collect_operator_status(layout, "omi", hours=24, now=datetime(2026, 9, 8, 10, tzinfo=UTC))

    assert result["status"] == "attention"
    quality = cast(dict[str, object], result["quality_window"])
    assert quality["transfer_sessions"] == 2
    assert quality["last_transfer_outcome"] == "failed"
    assert quality["last_transfer_termination_class"] == "fatal_error"
    assert quality["transfer_outcomes"] == {"collected": 1, "failed": 1}
    assert quality["transfer_termination_classes"] == {"completed": 1, "fatal_error": 1}
