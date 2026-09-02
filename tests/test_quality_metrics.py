from __future__ import annotations

import json
import os
from pathlib import Path
from typing import cast

import pytest

from omi_collector.capture.adapters.quality_metrics import (
    JsonlQualityMetrics,
    QualityMetricsError,
    normalize_source_revision,
    source_revision_from_environment,
)
from omi_collector.capture.application.quality_metrics import SequenceLossMetric, TransferSessionMetric


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


def test_journal_write_failure_is_reported_to_its_caller(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    journal = _journal(tmp_path)

    def fail_open(*_args: object, **_kwargs: object) -> int:
        raise OSError("full")

    monkeypatch.setattr(os, "open", fail_open)
    with pytest.raises(QualityMetricsError, match="cannot append"):
        journal.record_sequence_loss(
            SequenceLossMetric("2026-09-02T01:02:04.456+00:00", "s", "omi", 1, 444, "reason", "1", None, None)
        )
