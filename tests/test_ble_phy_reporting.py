import asyncio
import logging
from pathlib import Path
from typing import cast

import pytest

from omi_collector.capture.adapters.ble_link_observability import BleLinkObserver
from omi_collector.capture.adapters.debug_logging import set_runtime_status_sink


def _packet(event: int, payload: bytes) -> bytes:
    return bytes((4, event, len(payload))) + payload


def _connect() -> bytes:
    body = bytearray(18)
    body[1:3] = (0x42).to_bytes(2, "little")
    body[5:11] = b"\x06\x05\x04\x03\x02\x01"
    return _packet(0x3E, b"\x01" + body)


def _read_phy(tx: int, rx: int) -> bytes:
    return _packet(0x0E, b"\x01\x30\x20\x00\x42\x00" + bytes((tx, rx)))


def _update_phy(tx: int, rx: int) -> bytes:
    return _packet(0x3E, b"\x0c\x00\x42\x00" + bytes((tx, rx)))


def test_real_hci_observations_reach_status_and_default_info(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    monkeypatch.setenv("INVOCATION_ID", "1" * 32)
    logger = logging.getLogger("tests.phy.reporting")
    events: list[tuple[str, dict[str, object], str]] = []
    set_runtime_status_sink(logger, lambda event, fields, at: not events.append((event, fields, at)))
    observer = BleLinkObserver("01:02:03:04:05:06", debug_logger=logger)
    with caplog.at_level(logging.INFO):
        observer.handle_packet(_connect())
        observer.handle_packet(_read_phy(1, 2))
        observer.handle_packet(_update_phy(2, 1))
        observer.handle_packet(_packet(0x05, b"\x00\x42\x00\x13"))
    asyncio.run(observer.close())
    set_runtime_status_sink(logger, None)
    statuses = [fields for event, fields, _ in events if event == "ble_link_status"]
    assert [item["state"] for item in statuses] == ["connected", "observed", "observed", "disconnected"]
    assert [(item["tx_phy"], item["rx_phy"], item["source"]) for item in statuses[1:3]] == [
        ("1M", "2M", "read_phy"),
        ("2M", "1M", "phy_update"),
    ]
    info = "\n".join(record.getMessage() for record in caplog.records if record.levelno == logging.INFO)
    assert "1M" in info and "2M" in info
    assert "01:02:03:04:05:06" not in info
    assert "0x42" not in info


def test_rejected_status_is_visible_without_private_link_details(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    monkeypatch.setenv("INVOCATION_ID", "1" * 32)
    logger = logging.getLogger("tests.phy.rejected")
    set_runtime_status_sink(logger, lambda _event, _fields, _at: False)
    observer = BleLinkObserver("01:02:03:04:05:06", debug_logger=logger)
    with caplog.at_level(logging.INFO, logger="omi_collector.ble_link_status"):
        observer.handle_packet(_connect())
        observer.handle_packet(_read_phy(1, 2))
    asyncio.run(observer.close())
    set_runtime_status_sink(logger, None)
    text = "\n".join(record.getMessage() for record in caplog.records)
    assert "projection missed" in text
    assert "1M" in text and "2M" in text
    assert "01:02:03:04:05:06" not in text
    assert "0x42" not in text
    assert "1" * 32 not in text


def test_full_quality_queue_warns_and_keeps_last_observation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    from threading import Event

    from omi_collector.capture.adapters.collector_state_db import StateDB
    from omi_collector.capture.adapters.debug_logging import read_runtime_status
    from omi_collector.capture.adapters.quality_metrics import SQLiteQualityMetrics
    from omi_collector.config import QualityMetricsConfig

    monkeypatch.setenv("INVOCATION_ID", "1" * 32)
    db = StateDB(tmp_path / "state.sqlite3")
    writer = SQLiteQualityMetrics(db, release_version="1.2.3", config=QualityMetricsConfig(queue_max_records=1))
    entered, release = Event(), Event()
    original = db.transaction

    def blocked_transaction():
        entered.set()
        assert release.wait(5)
        return original()

    monkeypatch.setattr(db, "transaction", blocked_transaction)
    logger = logging.getLogger("tests.phy.full_queue")
    set_runtime_status_sink(logger, writer.record_runtime_status)
    observer = BleLinkObserver("01:02:03:04:05:06", debug_logger=logger)
    try:
        observer.handle_packet(_connect())
        assert entered.wait(2)
        observer.handle_packet(_read_phy(1, 2))
        with caplog.at_level(logging.WARNING, logger="omi_collector.ble_link_status"):
            observer.handle_packet(_packet(0x05, b"\x00\x42\x00\x13"))
        assert "projection missed" in caplog.text
    finally:
        release.set()
        import time

        deadline = time.monotonic() + 2
        while time.monotonic() < deadline:
            projection = read_runtime_status(db)
            if projection and cast(dict[str, object], projection.get("ble_link_status", {})).get("last_observed_phy"):
                break
            time.sleep(0.01)
        asyncio.run(observer.close())
        set_runtime_status_sink(logger, None)
        assert writer.close(timeout_seconds=2)
    projection = read_runtime_status(db)
    assert projection is not None
    assert (
        cast(dict[str, object], cast(dict[str, object], projection["ble_link_status"])["last_observed_phy"])["tx_phy"]
        == "1M"
    )


def test_service_logging_in_fresh_process_emits_one_sanitized_observation() -> None:
    import json
    import subprocess
    import sys

    code = r"""
import logging
from omi_collector.cli import _ble_link_logging
from omi_collector.capture.adapters.debug_logging import debug_event

logger = logging.getLogger("omi_collector.ble_link_status")
original = (logger.level, logger.propagate, tuple(logger.handlers))
try:
    with _ble_link_logging():
        with _ble_link_logging():
            debug_event("ble_link_status", invocation_id="1" * 32, link_id="2" * 32,
                        state="observed", phy_policy="auto", tx_phy="1M", rx_phy="1M",
                        source="read_phy", reason=None)
            logging.getLogger("omi_collector.capture").debug("private debug message")
        raise RuntimeError("cleanup test")
except RuntimeError:
    pass
assert (logger.level, logger.propagate, tuple(logger.handlers)) == original
"""
    result = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, check=True, timeout=10)
    lines = [line for line in result.stderr.splitlines() if line.startswith("BLE link observation ")]
    assert len(lines) == 1
    payload = cast(dict[str, object], json.loads(lines[0].removeprefix("BLE link observation ")))
    assert (payload["tx_phy"], payload["rx_phy"], payload["phy_policy"], payload["source"]) == (
        "1M",
        "1M",
        "auto",
        "read_phy",
    )
    assert "1" * 32 not in result.stderr
    assert "2" * 32 not in result.stderr
    assert "01:02:03:04:05:06" not in result.stderr
    assert "0x42" not in result.stderr
    assert "private debug message" not in result.stderr
    assert result.stdout == ""
