from __future__ import annotations

import gc
import logging
import weakref
from pathlib import Path
from typing import cast

import pytest

from omi_collector.capture.adapters.collector_state_db import StateDB
from omi_collector.capture.adapters.debug_logging import (
    _validate_runtime_event,
    close_debug_logging,
    configure_debug_logging,
    debug_event,
    debug_exception,
    import_legacy_runtime_status,
    legacy_runtime_projection,
    merge_runtime_status,
    read_runtime_status,
    set_runtime_status_sink,
    write_runtime_status_event,
)
from omi_collector.capture.adapters.quality_metrics import SQLiteQualityMetrics
from omi_collector.config import DebugLogConfig


def test_debug_events_are_redacted_before_they_reach_root_logging(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    secret = "secret-value-123"
    config = DebugLogConfig(logger_name="tests.debug.redaction")
    logger = configure_debug_logging(tmp_path, config)
    caplog.set_level(logging.DEBUG)
    try:
        debug_event(
            "unsafe_event",
            f"Bearer {secret} https://example.test/?access_token={secret}",
            logger=logger,
            token=secret,
            api_key=secret,
            **{
                "api_key=secret-value-123": "visible",
                "nested": {"access_key": secret, "signature": secret, f"signature={secret}": "visible"},
                "path": Path(f"/tmp/api_key={secret}"),
            },
            visible="safe detail",
        )
        try:
            raise RuntimeError(f"password={secret}")
        except RuntimeError as error:
            debug_exception("unsafe_exception", error, logger=logger, exc_info=object())
    finally:
        close_debug_logging(logger)

    assert secret not in caplog.text
    assert "<redacted>" in caplog.text
    assert "safe detail" in caplog.text
    assert "RuntimeError" in caplog.text
    assert "secret-value-123" not in caplog.text
    assert list(tmp_path.iterdir()) == []


def test_runtime_projection_coalesces_aggregate_status_and_keeps_latest_observations(tmp_path: Path) -> None:
    database = StateDB(tmp_path / "collector-state.db")
    writer = SQLiteQualityMetrics(database, release_version="1.2.3")
    logger = configure_debug_logging(tmp_path, DebugLogConfig(logger_name="tests.debug.runtime_projection"))
    timestamps: list[str] = []

    def enqueue(event: str, fields: dict[str, object], timestamp: str) -> bool:
        timestamps.append(timestamp)
        return writer.record_runtime_status(event, fields, timestamp)

    set_runtime_status_sink(logger, enqueue)
    progress = {
        "status": "progress",
        "records_completed": 2,
        "records_total": 4,
        "payload_bytes": 888,
        "remaining_packets": 2,
    }
    try:
        debug_event("sync_progress", logger=logger, progress=progress)
        debug_event("sync_progress", logger=logger, progress=progress)
        debug_event(
            "sync_progress",
            logger=logger,
            progress={
                "event": "pendant_observation",
                "status": "operational",
                "battery_percent": 71,
                "firmware": "1.2.3",
            },
        )
        debug_event("ble_link_rssi_observed", logger=logger, rssi_dbm=-68)
        assert writer.close(timeout_seconds=2)
    finally:
        close_debug_logging(logger)

    projection = cast(dict[str, object], read_runtime_status(database))
    assert timestamps[0] < timestamps[1]
    assert projection["updated_at"] == timestamps[2]
    observation = cast(dict[str, object], projection["observation"])
    battery = cast(dict[str, object], projection["battery_observation"])
    assert projection["connection_rssi_dbm"] == -68
    assert projection["connection_rssi_observed_at"]
    assert observation["firmware"] == "1.2.3"
    assert battery["battery_percent"] == 71
    assert cast(dict[str, object], projection["progress"])["status"] == "operational"


def test_runtime_projection_does_not_store_unrelated_debug_events(tmp_path: Path) -> None:
    database = StateDB(tmp_path / "collector-state.db")
    writer = SQLiteQualityMetrics(database, release_version="1.2.3")
    logger = configure_debug_logging(tmp_path, DebugLogConfig(logger_name="tests.debug.no_trace_history"))
    set_runtime_status_sink(logger, writer.record_runtime_status)
    try:
        debug_event("ble_link_session", logger=logger, record={"events": [1, 2, 3]})
    finally:
        close_debug_logging(logger)
        assert writer.close(timeout_seconds=2)

    assert read_runtime_status(database) is None


def test_runtime_sink_does_not_retain_a_closed_quality_writer(tmp_path: Path) -> None:
    database = StateDB(tmp_path / "collector-state.db")
    writer = SQLiteQualityMetrics(database, release_version="1.2.3")
    writer_reference = weakref.ref(writer)
    logger = configure_debug_logging(tmp_path, DebugLogConfig(logger_name="tests.debug.weak_sink"))
    set_runtime_status_sink(logger, writer.record_runtime_status)
    assert writer.close(timeout_seconds=2)
    assert not writer.record_runtime_status(
        "sync_progress", {"progress": {"status": "progress"}}, "2026-10-11T01:02:03+00:00"
    )

    del writer
    gc.collect()
    assert writer_reference() is None


def test_runtime_projection_ignores_older_delayed_updates_and_refreshes_repeated_progress(tmp_path: Path) -> None:
    database = StateDB(tmp_path / "collector-state.db")
    fields = {"progress": {"status": "reading", "records_completed": 4}}
    write_runtime_status_event(database, "sync_progress", fields, "2026-10-11T01:02:03.000000+00:00")
    write_runtime_status_event(database, "sync_progress", fields, "2026-10-11T01:02:04.000000+00:00")
    projection = cast(dict[str, object], read_runtime_status(database))
    assert projection["updated_at"] == "2026-10-11T01:02:04.000000+00:00"

    older = merge_runtime_status(
        projection,
        "sync_progress",
        {"progress": {"status": "session_error"}},
        "2026-10-11T01:02:03.500000+00:00",
    )
    assert older is not None
    assert older["updated_at"] == "2026-10-11T01:02:04.000000+00:00"
    assert cast(dict[str, object], older["progress"])["status"] == "reading"


def test_legacy_runtime_projection_matches_import_and_preserves_source_timestamps(tmp_path: Path) -> None:
    rows = (
        {
            "event": "sync_progress",
            "fields": {"progress": {"status": "progress", "records_completed": 3}},
            "timestamp": "2026-10-11T06:02:03.123456+03:00",
        },
        {
            "event": "sync_progress",
            "fields": {
                "progress": {
                    "event": "pendant_observation",
                    "status": "operational",
                    "battery_percent": 71,
                    "firmware": "1.2.3",
                }
            },
            "timestamp": "2026-10-11T03:02:04.654321+00:00",
        },
        {
            "event": "ble_link_rssi_observed",
            "fields": {"rssi_dbm": -68},
            "timestamp": "2026-10-11T03:02:05.000000+00:00",
        },
    )
    expected = legacy_runtime_projection(rows)
    database = StateDB(tmp_path / "collector-state.db")
    import_legacy_runtime_status(database, rows)

    assert expected is not None
    assert read_runtime_status(database) == expected
    assert expected["updated_at"] == "2026-10-11T03:02:04.654321+00:00"
    assert expected["observation_at"] == "2026-10-11T03:02:04.654321+00:00"
    assert expected["battery_observed_at"] == "2026-10-11T03:02:04.654321+00:00"
    assert expected["connection_rssi_observed_at"] == "2026-10-11T03:02:05.000000+00:00"


@pytest.mark.parametrize(
    "value",
    [
        pytest.param({}, id="missing-envelope-fields"),
        pytest.param(
            {"event": "sync_progress", "fields": [], "timestamp": "2026-10-11T01:02:03+00:00"},
            id="fields-not-an-object",
        ),
        pytest.param(
            {"event": "sync_progress", "fields": {}, "timestamp": "not-a-timestamp"},
            id="invalid-timestamp",
        ),
        pytest.param(
            {"event": "sync_progress", "fields": {}, "timestamp": "2026-10-11T01:02:03"},
            id="timezone-naive-timestamp",
        ),
        pytest.param(
            {
                "event": "sync_progress",
                "fields": {"progress": []},
                "timestamp": "2026-10-11T01:02:03+00:00",
            },
            id="progress-not-an-object",
        ),
        pytest.param(
            {
                "event": "sync_progress",
                "fields": {"progress": {"status": 3}},
                "timestamp": "2026-10-11T01:02:03+00:00",
            },
            id="status-not-text",
        ),
        pytest.param(
            {
                "event": "sync_progress",
                "fields": {"progress": {"status": "progress", "event": 3}},
                "timestamp": "2026-10-11T01:02:03+00:00",
            },
            id="observation-type-not-text",
        ),
        pytest.param(
            {
                "event": "sync_progress",
                "fields": {"progress": {"status": "progress", "battery_percent": True}},
                "timestamp": "2026-10-11T01:02:03+00:00",
            },
            id="boolean-battery-is-not-an-integer-reading",
        ),
        pytest.param(
            {
                "event": "sync_progress",
                "fields": {"progress": {"status": "progress", "battery_percent": "71"}},
                "timestamp": "2026-10-11T01:02:03+00:00",
            },
            id="text-battery-is-not-an-integer-reading",
        ),
        pytest.param(
            {
                "event": "sync_progress",
                "fields": {"progress": {"status": "progress", "firmware": 1}},
                "timestamp": "2026-10-11T01:02:03+00:00",
            },
            id="firmware-not-text",
        ),
        pytest.param(
            {
                "event": "ble_link_rssi_observed",
                "fields": {"rssi_dbm": False},
                "timestamp": "2026-10-11T01:02:03+00:00",
            },
            id="boolean-rssi-is-not-an-integer-reading",
        ),
        pytest.param(
            {
                "event": "ble_link_rssi_observed",
                "fields": {"rssi_dbm": "-68"},
                "timestamp": "2026-10-11T01:02:03+00:00",
            },
            id="text-rssi-is-not-an-integer-reading",
        ),
        pytest.param(
            {
                "event": "unsupported",
                "fields": {},
                "timestamp": "2026-10-11T01:02:03+00:00",
            },
            id="unsupported-event",
        ),
    ],
)
def test_runtime_event_validator_rejects_malformed_event_boundaries(value: dict[str, object]) -> None:
    with pytest.raises(ValueError):
        _validate_runtime_event(value)
