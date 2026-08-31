from __future__ import annotations

import json
import logging
import os
import stat
import threading
import time
from collections.abc import Mapping
from pathlib import Path
from typing import cast

import pytest

from omi_collector.capture.adapters import debug_logging
from omi_collector.capture.adapters.debug_logging import (
    close_debug_logging,
    configure_debug_logging,
    debug_event,
    debug_exception,
)
from omi_collector.capture.application.collector import CollectorTimeoutError
from omi_collector.config import DebugLogConfig


def test_debug_ring_rotates_with_bounded_file_count(tmp_path: Path) -> None:
    config = DebugLogConfig(max_bytes=512, backup_count=2, max_record_bytes=512, logger_name="tests.debug.rotation")
    logger = configure_debug_logging(tmp_path, config)
    for index in range(16):
        debug_event("retry", "retrying bounded operation U0001f642", logger=logger, attempt=index, reason="temporary")
    close_debug_logging(logger)

    paths = sorted(tmp_path.glob("debug.jsonl*"))

    assert 1 <= len(paths) <= config.backup_count + 1
    assert all(path.stat().st_size <= config.max_bytes for path in paths)
    assert stat.S_IMODE(tmp_path.stat().st_mode) == 0o750
    assert all(stat.S_IMODE(path.stat().st_mode) == 0o640 for path in paths)


def test_debug_ring_appends_after_a_process_style_restart(tmp_path: Path) -> None:
    config = DebugLogConfig(logger_name="tests.debug.restart")
    first_logger = configure_debug_logging(tmp_path, config)
    debug_event("first_start", logger=first_logger, phase="startup")
    close_debug_logging(first_logger)

    second_logger = configure_debug_logging(tmp_path, config)
    debug_event("second_start", logger=second_logger, phase="startup")
    close_debug_logging(second_logger)

    entries = [
        cast(Mapping[str, object], json.loads(line))
        for line in (tmp_path / "debug.jsonl").read_text(encoding="utf-8").splitlines()
    ]

    assert [entry["event"] for entry in entries] == ["first_start", "second_start"]


def test_debug_ring_preserves_exception_cause_and_does_not_reach_journal(
    caplog: pytest.LogCaptureFixture, tmp_path: Path
) -> None:
    config = DebugLogConfig(logger_name="tests.debug.exception")
    caplog.set_level(logging.DEBUG)
    logger = configure_debug_logging(tmp_path, config)
    try:
        try:
            raise ValueError("low-level cause")
        except ValueError as cause:
            raise RuntimeError("top-level failure") from cause
    except RuntimeError as error:
        debug_exception("writer_close_failed", error, logger=logger, payload=b"must-not-appear")
    close_debug_logging(logger)

    entry = cast(Mapping[str, object], json.loads((tmp_path / "debug.jsonl").read_text(encoding="utf-8")))
    fields = cast(Mapping[str, object], entry["fields"])
    traceback = cast(str, entry["traceback"])

    assert entry["level"] == "DEBUG"
    assert entry["logger"] == config.logger_name
    assert entry["process_id"] == os.getpid()
    assert entry["process_name"]
    assert entry["thread_id"] == threading.get_ident()
    assert entry["thread_name"] == threading.current_thread().name
    assert entry["event"] == "writer_close_failed"
    assert fields["payload"] == "<redacted>"
    assert "ValueError: low-level cause" in traceback
    assert "RuntimeError: top-level failure" in traceback
    assert caplog.records == []


def test_debug_exception_does_not_call_broken_str_and_redacts_traceback_secrets(tmp_path: Path) -> None:
    sentinel = "s3nt1nel-secret"

    class BrokenError(RuntimeError):
        def __str__(self) -> str:
            raise AssertionError("must run only behind listener protection")

    config = DebugLogConfig(logger_name="tests.debug.redaction")
    logger = configure_debug_logging(tmp_path, config)
    debug_exception("broken", BrokenError(), logger=logger)
    try:
        raise RuntimeError(
            f"Bearer {sentinel} password={sentinel} https://example/?x-goog-signature={sentinel}&access_token={sentinel}&GoogleAccessId={sentinel}&Key-Pair-Id={sentinel}"
        )
    except RuntimeError as error:
        debug_exception("secret_exception", error, logger=logger, message=f"Authorization: Bearer {sentinel}")
    close_debug_logging(logger)

    content = (tmp_path / "debug.jsonl").read_text(encoding="utf-8")

    assert sentinel not in content
    assert "<redacted>" in content


def test_connect_timeout_is_a_concise_debug_event_without_traceback(tmp_path: Path) -> None:
    config = DebugLogConfig(logger_name="tests.debug.connect_timeout")
    logger = configure_debug_logging(tmp_path, config)
    try:
        try:
            raise CollectorTimeoutError("opportunistic operation timed out")
        except CollectorTimeoutError as error:
            debug_exception("session_error", error, logger=logger, phase="connect")
    finally:
        close_debug_logging(logger)

    entry = cast(Mapping[str, object], json.loads((tmp_path / "debug.jsonl").read_text(encoding="utf-8")))
    fields = cast(Mapping[str, object], entry["fields"])

    assert entry["event"] == "session_error"
    assert entry["message"] == "CollectorTimeoutError"
    assert fields["phase"] == "connect"
    assert "traceback" not in entry


@pytest.mark.parametrize(
    ("event", "phase", "error_type"),
    [
        ("session_error", "read/reconcile", CollectorTimeoutError),
        ("session_error", "connect", RuntimeError),
        ("session_error", "telemetry", CollectorTimeoutError),
    ],
)
def test_non_absence_failures_keep_debug_tracebacks(
    tmp_path: Path,
    event: str,
    phase: str,
    error_type: type[Exception],
) -> None:
    config = DebugLogConfig(logger_name=f"tests.debug.traceback.{phase.replace('/', '_')}")
    logger = configure_debug_logging(tmp_path, config)
    try:
        try:
            raise error_type("diagnostic failure")
        except Exception as error:  # noqa: BLE001 - exercise each diagnostic exception type
            debug_exception(event, error, logger=logger, phase=phase)
    finally:
        close_debug_logging(logger)

    entry = cast(Mapping[str, object], json.loads((tmp_path / "debug.jsonl").read_text(encoding="utf-8")))

    assert "traceback" in entry
    assert error_type.__name__ in cast(str, entry["traceback"])


def test_debug_ring_truncates_huge_exception_to_complete_max_record_bytes(tmp_path: Path) -> None:
    config = DebugLogConfig(max_bytes=512, max_record_bytes=512, logger_name="tests.debug.truncate")
    logger = configure_debug_logging(tmp_path, config)
    try:
        raise RuntimeError("x" * 50_000)
    except RuntimeError as error:
        debug_exception("huge_exception", error, logger=logger)
    close_debug_logging(logger)

    written = (tmp_path / "debug.jsonl").read_bytes()
    entry = cast(Mapping[str, object], json.loads(written))

    assert written.endswith(b"\n")
    assert len(written) <= config.max_record_bytes
    assert entry["truncated"] is True


def test_debug_ring_enforces_minimum_record_budget_at_logging_boundary(tmp_path: Path) -> None:
    config = DebugLogConfig(max_record_bytes=511)

    with pytest.raises(ValueError, match="at least 512"):
        configure_debug_logging(tmp_path, config)


def test_debug_ring_drops_full_queue_and_bounds_shutdown(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    entered = threading.Event()
    release = threading.Event()
    original_handle = debug_logging._SafeRotatingFileHandler.handle

    def blocked_handle(self: debug_logging._SafeRotatingFileHandler, record: logging.LogRecord) -> bool:
        entered.set()
        release.wait(1)
        return original_handle(self, record)

    monkeypatch.setattr(debug_logging._SafeRotatingFileHandler, "handle", blocked_handle)
    config = DebugLogConfig(queue_max_records=1, shutdown_join_seconds=0.01, logger_name="tests.debug.stalled")
    logger = configure_debug_logging(tmp_path, config)
    handler = next(handler for handler in logger.handlers if hasattr(handler, "_omi_collector_debug_sink"))
    sink = cast(debug_logging._RingSink, object.__getattribute__(handler, "_omi_collector_debug_sink"))
    debug_event("first", logger=logger)
    assert entered.wait(1)
    debug_event("queued", logger=logger)
    debug_event("dropped", logger=logger)

    started = time.monotonic()
    dropped = close_debug_logging(logger)

    assert time.monotonic() - started < 0.1
    assert dropped >= 1
    assert configure_debug_logging(tmp_path, config) is logger
    assert logger.handlers == []
    release.set()
    deadline = time.monotonic() + 1
    while sink.listener.is_alive() and time.monotonic() < deadline:
        time.sleep(0.01)
    assert not sink.listener.is_alive()
