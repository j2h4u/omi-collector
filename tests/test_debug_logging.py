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
    debug_log_path,
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


def test_debug_exception_uses_custom_detail_or_exception_class_name(tmp_path: Path) -> None:
    config = DebugLogConfig(logger_name="tests.debug.exception_message")
    logger = configure_debug_logging(tmp_path, config)
    try:
        try:
            raise RuntimeError("implementation detail")
        except RuntimeError as error:
            debug_exception("custom_failure", error, message="safe operator detail", logger=logger)
        try:
            raise LookupError("another implementation detail")
        except LookupError as error:
            debug_exception("default_failure", error, logger=logger)
    finally:
        close_debug_logging(logger)

    entries = [
        cast(Mapping[str, object], json.loads(line))
        for line in (tmp_path / "debug.jsonl").read_text(encoding="utf-8").splitlines()
    ]

    assert [entry["message"] for entry in entries] == ["safe operator detail", "LookupError"]


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


def test_configure_creates_nested_collector_root_and_writes_jsonl(tmp_path: Path) -> None:
    root = tmp_path / "state" / "collector" / "nested"
    config = DebugLogConfig(logger_name="tests.debug.nested_root")
    logger = configure_debug_logging(root, config)
    try:
        debug_event("nested_root_ready", logger=logger, phase="startup")
    finally:
        close_debug_logging(logger)

    entry = cast(Mapping[str, object], json.loads((root / config.file_name).read_text(encoding="utf-8")))
    assert entry["event"] == "nested_root_ready"
    assert entry["logger"] == config.logger_name


def test_ascii_encoded_debug_json_round_trips_unicode_message_and_field(tmp_path: Path) -> None:
    config = DebugLogConfig(encoding="ascii", logger_name="tests.debug.ascii_unicode")
    logger = configure_debug_logging(tmp_path, config)
    message = "Привет, мир 🐾"
    try:
        debug_event("unicode_roundtrip", message, logger=logger, greeting=message)
    finally:
        close_debug_logging(logger)

    raw_line = (tmp_path / "debug.jsonl").read_bytes()
    entry = cast(Mapping[str, object], json.loads(raw_line.decode("ascii")))
    fields = cast(Mapping[str, object], entry["fields"])

    assert entry["message"] == message
    assert fields["greeting"] == message


def test_logger_normalizes_malformed_and_recursive_public_debug_fields(tmp_path: Path) -> None:
    config = DebugLogConfig(logger_name="tests.debug.fields_fallback")
    logger = configure_debug_logging(tmp_path, config)
    recursive_fields: dict[str, object] = {}
    recursive_fields["self"] = recursive_fields
    try:
        logger.debug("non-mapping fields", extra={"debug_event": "non_mapping", "debug_fields": None})
        logger.debug(
            "recursive fields",
            extra={"debug_event": "recursive_mapping", "debug_fields": recursive_fields},
        )
    finally:
        close_debug_logging(logger)

    entries = [
        cast(Mapping[str, object], json.loads(line))
        for line in (tmp_path / "debug.jsonl").read_text(encoding="utf-8").splitlines()
    ]

    assert cast(Mapping[str, object], entries[0]["fields"]) == {}
    assert cast(Mapping[str, object], entries[1]["fields"]) == {"unavailable": True}


def test_debug_ring_drops_full_queue_and_bounds_shutdown(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    entered = threading.Event()
    release = threading.Event()
    listener_threads: list[threading.Thread] = []
    original_handle = debug_logging._SafeRotatingFileHandler.handle

    def blocked_handle(self: debug_logging._SafeRotatingFileHandler, record: logging.LogRecord) -> bool:
        listener_threads.append(threading.current_thread())
        entered.set()
        release.wait(1)
        return original_handle(self, record)

    monkeypatch.setattr(debug_logging._SafeRotatingFileHandler, "handle", blocked_handle)
    config = DebugLogConfig(queue_max_records=1, shutdown_join_seconds=0.01, logger_name="tests.debug.stalled")
    logger = configure_debug_logging(tmp_path, config)
    try:
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
        assert len(listener_threads) == 1
        listener_threads[0].join(1)
        assert not listener_threads[0].is_alive()
        configure_debug_logging(tmp_path, config)
        assert logger.handlers
        debug_event("after_resume", logger=logger, phase="ready")
        deadline = time.monotonic() + 1
        path = tmp_path / "debug.jsonl"
        while time.monotonic() < deadline and (
            not path.exists() or "after_resume" not in path.read_text(encoding="utf-8")
        ):
            time.sleep(0.01)
        close_debug_logging(logger)
    finally:
        release.set()
        close_debug_logging(logger)

    events = [
        cast(Mapping[str, object], json.loads(line))["event"]
        for line in (tmp_path / "debug.jsonl").read_text(encoding="utf-8").splitlines()
    ]
    assert events.count("after_resume") == 1


def test_configure_defers_until_listener_thread_can_finish_close(tmp_path: Path) -> None:
    entered = threading.Event()
    release = threading.Event()
    listener_threads: list[threading.Thread] = []
    config = DebugLogConfig(shutdown_join_seconds=0.01, logger_name="tests.debug.listener_close")
    logger = configure_debug_logging(tmp_path, config)

    class CloseFromListener:
        def __str__(self) -> str:
            listener_threads.append(threading.current_thread())
            close_debug_logging(logger)
            entered.set()
            release.wait(1)
            return "close requested"

    try:
        logger.debug(CloseFromListener(), extra={"debug_event": "listener_close", "debug_fields": {}})
        assert entered.wait(1)
        assert configure_debug_logging(tmp_path, config) is logger
        debug_event("while_pending", logger=logger)
        release.set()
        assert len(listener_threads) == 1
        listener_threads[0].join(1)
        assert not listener_threads[0].is_alive()
        assert configure_debug_logging(tmp_path, config) is logger
        debug_event("after_listener_recovery", logger=logger)
    finally:
        release.set()
        if listener_threads:
            listener_threads[0].join(1)
        close_debug_logging(logger)

    events = [
        cast(Mapping[str, object], json.loads(line))["event"]
        for line in (tmp_path / config.file_name).read_text(encoding="utf-8").splitlines()
    ]
    assert "after_listener_recovery" in events
    assert "while_pending" not in events


def test_configure_returns_logger_when_sink_close_times_out(tmp_path: Path) -> None:
    entered = threading.Event()
    release = threading.Event()
    listener_threads: list[threading.Thread] = []
    config = DebugLogConfig(shutdown_join_seconds=0.01, logger_name="tests.debug.configure_timeout")
    logger = configure_debug_logging(tmp_path, config)

    class BlockOnListener:
        def __str__(self) -> str:
            listener_threads.append(threading.current_thread())
            entered.set()
            release.wait(1)
            return "blocked diagnostic"

    try:
        logger.debug(BlockOnListener(), extra={"debug_event": "blocked", "debug_fields": {}})
        assert entered.wait(1)
        assert configure_debug_logging(tmp_path, config) is logger
        release.set()
        assert len(listener_threads) == 1
        listener_threads[0].join(1)
        assert not listener_threads[0].is_alive()
        assert configure_debug_logging(tmp_path, config) is logger
        debug_event("configure_recovered", logger=logger)
    finally:
        release.set()
        if listener_threads:
            listener_threads[0].join(1)
        close_debug_logging(logger)

    events = [
        cast(Mapping[str, object], json.loads(line))["event"]
        for line in (tmp_path / config.file_name).read_text(encoding="utf-8").splitlines()
    ]
    assert "configure_recovered" in events


def test_utf16_ring_rotation_counts_encoded_bytes_and_keeps_jsonl_complete(tmp_path: Path) -> None:
    config = DebugLogConfig(
        max_bytes=2048,
        backup_count=2,
        max_record_bytes=1024,
        encoding="utf-16",
        logger_name="tests.debug.utf16",
    )
    logger = configure_debug_logging(tmp_path, config)
    try:
        for index in range(18):
            debug_event("utf16_event", "ordinary diagnostic text " * 6, logger=logger, attempt=index)
    finally:
        close_debug_logging(logger)

    paths = sorted(tmp_path.glob("debug.jsonl*"))

    assert 1 < len(paths) <= config.backup_count + 1
    for path in paths:
        assert path.stat().st_size <= config.max_bytes
        entries = [
            cast(Mapping[str, object], json.loads(line)) for line in path.read_text(encoding="utf-16").splitlines()
        ]
        assert entries
        assert all(entry["event"] == "utf16_event" for entry in entries)


def test_debug_log_path_uses_default_name_and_explicit_override(tmp_path: Path) -> None:
    config = DebugLogConfig(file_name="collector-debug.jsonl", logger_name="tests.debug.path")
    logger = configure_debug_logging(tmp_path, config, file_name="override.jsonl")
    try:
        debug_event("path_check", logger=logger)
    finally:
        close_debug_logging(logger)

    assert debug_log_path(tmp_path, config) == tmp_path / "collector-debug.jsonl"
    assert debug_log_path(tmp_path, config, file_name="override.jsonl") == tmp_path / "override.jsonl"
    assert (tmp_path / "override.jsonl").is_file()
    assert not (tmp_path / "collector-debug.jsonl").exists()


def test_debug_event_sanitizes_nested_values_and_mixed_case_secrets(tmp_path: Path) -> None:
    sentinel = "nested-secret-value"
    config = DebugLogConfig(logger_name="tests.debug.nested")
    logger = configure_debug_logging(tmp_path, config)
    try:
        debug_event(
            "nested_values",
            logger=logger,
            number=17,
            enabled=False,
            missing=None,
            location=tmp_path / "capture.bin",
            values=["ordinary", b"private bytes", {"AcCeSs-ToKeN": sentinel, "note": "ordinary text"}],
            nested={"SeCrEtName": sentinel, "path": tmp_path / "nested.bin"},
            query=f"https://example.test/?X-GoOg-SiGnAtUrE={sentinel}",
        )
    finally:
        close_debug_logging(logger)

    content = (tmp_path / "debug.jsonl").read_text(encoding="utf-8")
    entry = cast(Mapping[str, object], json.loads(content))
    fields = cast(Mapping[str, object], entry["fields"])
    values = cast(list[object], fields["values"])
    nested = cast(Mapping[str, object], fields["nested"])

    assert sentinel not in content
    assert fields["number"] == 17
    assert fields["enabled"] is False
    assert fields["missing"] is None
    assert fields["location"] == str(tmp_path / "capture.bin")
    assert values[0] == "ordinary"
    assert values[1] == "<redacted>"
    assert cast(Mapping[str, object], values[2])["AcCeSs-ToKeN"] == "<redacted>"
    assert cast(Mapping[str, object], values[2])["note"] == "ordinary text"
    assert nested["SeCrEtName"] == "<redacted>"
    assert nested["path"] == str(tmp_path / "nested.bin")
    assert "<redacted>" in cast(str, fields["query"])


def test_debug_event_survives_a_message_whose_string_conversion_raises(tmp_path: Path) -> None:
    class BrokenMessage:
        def __str__(self) -> str:
            raise RuntimeError("message rendering failed")

    config = DebugLogConfig(logger_name="tests.debug.broken_message")
    logger = configure_debug_logging(tmp_path, config)
    try:
        logger.debug(
            BrokenMessage(),
            extra={"debug_event": "broken_message", "debug_fields": {"phase": "capture"}},
        )
    finally:
        close_debug_logging(logger)

    entry = cast(Mapping[str, object], json.loads((tmp_path / "debug.jsonl").read_text(encoding="utf-8")))
    fields = cast(Mapping[str, object], entry["fields"])

    assert entry["event"] == "broken_message"
    assert entry["message"] == "<message unavailable>"
    assert fields["phase"] == "capture"


def test_long_ordinary_event_stays_bounded_without_traceback(tmp_path: Path) -> None:
    config = DebugLogConfig(max_bytes=512, max_record_bytes=512, logger_name="tests.debug.long_event")
    logger = configure_debug_logging(tmp_path, config)
    message = "0123456789abcde" + "Z" * 10_000
    try:
        debug_event("large_event_name", message, logger=logger, phase="connect")
    finally:
        close_debug_logging(logger)

    written = (tmp_path / "debug.jsonl").read_bytes()
    entry = cast(Mapping[str, object], json.loads(written))
    fields = cast(Mapping[str, object], entry["fields"])

    assert written.endswith(b"\n")
    assert len(written) <= 512
    assert entry["level"] == "DEBUG"
    assert entry["event"] == "large_event_name"
    assert entry["message"] == "0123456789abcde…"
    assert fields == {"truncated": True}
    assert entry["truncated"] is True
    assert "traceback" not in entry


def test_long_exception_keeps_a_bounded_traceback_marker(tmp_path: Path) -> None:
    config = DebugLogConfig(max_bytes=512, max_record_bytes=512, logger_name="tests.debug.long_traceback")
    logger = configure_debug_logging(tmp_path, config)
    try:
        try:
            raise RuntimeError("trace detail " * 10_000)
        except RuntimeError as error:
            debug_exception("long_exception", error, logger=logger, phase="capture")
    finally:
        close_debug_logging(logger)

    written = (tmp_path / "debug.jsonl").read_bytes()
    entry = cast(Mapping[str, object], json.loads(written))
    traceback = cast(str, entry["traceback"])

    assert written.endswith(b"\n")
    assert len(written) <= 512
    assert entry["truncated"] is True
    assert traceback
    assert len(traceback) < 100
    assert "…" in traceback


def test_debug_ring_opens_file_only_after_first_record(tmp_path: Path) -> None:
    config = DebugLogConfig(logger_name="tests.debug.lazy_file")
    path = tmp_path / config.file_name
    logger = configure_debug_logging(tmp_path, config)
    try:
        assert not path.exists()
        debug_event("first_record", logger=logger)
    finally:
        close_debug_logging(logger)

    assert path.is_file()
    assert json.loads(path.read_text(encoding=config.encoding))["event"] == "first_record"


def test_debug_exception_keeps_traceback_unavailable_marker(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    def broken_format_exception(self: logging.Formatter, exc_info: object) -> str:
        _ = self, exc_info
        raise RuntimeError("traceback formatting failed")

    monkeypatch.setattr(logging.Formatter, "formatException", broken_format_exception)
    config = DebugLogConfig(logger_name="tests.debug.traceback_unavailable")
    logger = configure_debug_logging(tmp_path, config)
    try:
        debug_exception("broken_traceback", RuntimeError("failure"), logger=logger)
    finally:
        close_debug_logging(logger)

    entry = cast(Mapping[str, object], json.loads((tmp_path / config.file_name).read_text(encoding="utf-8")))
    assert entry["traceback"] == "<traceback unavailable>"


def test_large_budget_compacts_long_event_without_fallback(tmp_path: Path) -> None:
    config = DebugLogConfig(max_bytes=4096, max_record_bytes=1024, logger_name="tests.debug.long_event_1024")
    logger = configure_debug_logging(tmp_path, config)
    try:
        debug_event("large_event_name", "x" * 10_000, logger=logger, phase="connect")
    finally:
        close_debug_logging(logger)

    written = (tmp_path / config.file_name).read_bytes()
    entry = cast(Mapping[str, object], json.loads(written))
    fields = cast(Mapping[str, object], entry["fields"])
    assert written.endswith(b"\n")
    assert len(written) <= config.max_record_bytes
    message = cast(str, entry["message"])
    assert message.startswith("x")
    assert message.endswith("…")
    assert fields == {"truncated": True}
    assert entry["truncated"] is True


def test_debug_rotation_uses_at_least_boundary(tmp_path: Path) -> None:
    calibration_root = tmp_path / "calibration"
    calibration_config = DebugLogConfig(logger_name="tests.debug.rotation_boundary")
    calibration_logger = configure_debug_logging(calibration_root, calibration_config)
    try:
        debug_event("same_size", logger=calibration_logger, attempt=0)
    finally:
        close_debug_logging(calibration_logger)
    record_bytes = (calibration_root / calibration_config.file_name).stat().st_size

    root = tmp_path / "boundary"
    config = DebugLogConfig(
        max_bytes=record_bytes * 2,
        backup_count=1,
        max_record_bytes=record_bytes * 2,
        logger_name="tests.debug.rotation_boundary",
    )
    logger = configure_debug_logging(root, config)
    try:
        debug_event("same_size", logger=logger, attempt=1)
        debug_event("same_size", logger=logger, attempt=2)
    finally:
        close_debug_logging(logger)

    current = [json.loads(line) for line in (root / config.file_name).read_text(encoding="utf-8").splitlines()]
    rotated = [json.loads(line) for line in (root / f"{config.file_name}.1").read_text(encoding="utf-8").splitlines()]
    assert len(current) == len(rotated) == 1
    assert cast(Mapping[str, object], current[0]["fields"])["attempt"] == 2
    assert cast(Mapping[str, object], rotated[0]["fields"])["attempt"] == 1


@pytest.mark.parametrize("encoding", ["utf-8", "utf-16"])
def test_record_one_byte_over_cap_is_compacted(encoding: str, tmp_path: Path) -> None:
    calibration_root = tmp_path / "calibration"
    calibration_config = DebugLogConfig(
        max_bytes=4096,
        max_record_bytes=4096,
        encoding=encoding,
        logger_name=f"tests.debug.record_boundary.{encoding.replace('-', '_')}",
    )
    calibration_logger = configure_debug_logging(calibration_root, calibration_config)
    try:
        debug_event("boundary", "x" * 700, logger=calibration_logger)
    finally:
        close_debug_logging(calibration_logger)
    calibrated = (calibration_root / calibration_config.file_name).read_bytes()
    full_record_body_bytes = len(calibrated) - len("\n".encode(encoding))

    config = DebugLogConfig(
        max_bytes=4096,
        max_record_bytes=full_record_body_bytes - 1,
        encoding=encoding,
        logger_name=f"tests.debug.record_boundary.{encoding.replace('-', '_')}",
    )
    logger = configure_debug_logging(tmp_path / "bounded", config)
    try:
        debug_event("boundary", "x" * 700, logger=logger)
    finally:
        close_debug_logging(logger)

    written = (tmp_path / "bounded" / config.file_name).read_bytes()
    entry = cast(Mapping[str, object], json.loads(written.decode(encoding)))
    assert written.decode(encoding).endswith("\n")
    assert len(written) <= config.max_record_bytes
    assert entry["truncated"] is True
    assert entry["message"] != "x" * 700


def test_record_exactly_at_cap_keeps_full_event(tmp_path: Path) -> None:
    calibration_root = tmp_path / "calibration"
    logger_name = "tests.debug.record_equal_boundary"
    calibration_config = DebugLogConfig(max_bytes=4096, max_record_bytes=4096, logger_name=logger_name)
    calibration_logger = configure_debug_logging(calibration_root, calibration_config)
    try:
        debug_event("equal_boundary", "complete message " * 20, logger=calibration_logger, code=7)
    finally:
        close_debug_logging(calibration_logger)
    exact_record_bytes = (calibration_root / calibration_config.file_name).stat().st_size

    config = DebugLogConfig(
        max_bytes=exact_record_bytes,
        max_record_bytes=exact_record_bytes,
        logger_name=logger_name,
    )
    logger = configure_debug_logging(tmp_path / "exact", config)
    try:
        debug_event("equal_boundary", "complete message " * 20, logger=logger, code=7)
    finally:
        close_debug_logging(logger)

    written = (tmp_path / "exact" / config.file_name).read_bytes()
    entry = cast(Mapping[str, object], json.loads(written))
    assert len(written) == exact_record_bytes
    assert entry["message"] == "complete message " * 20
    assert cast(Mapping[str, object], entry["fields"]) == {"code": 7}
    assert "truncated" not in entry


def test_utf16_oversized_record_uses_truncated_fallback(tmp_path: Path) -> None:
    config = DebugLogConfig(
        max_bytes=1024,
        max_record_bytes=512,
        encoding="utf-16",
        logger_name="tests.debug.utf16_fallback",
    )
    logger = configure_debug_logging(tmp_path, config)
    try:
        debug_event("fallback", "x" * 50_000, logger=logger, phase="capture")
    finally:
        close_debug_logging(logger)

    written = (tmp_path / config.file_name).read_bytes()
    entry = cast(Mapping[str, object], json.loads(written.decode("utf-16")))
    assert written.decode("utf-16").endswith("\n")
    assert len(written) <= config.max_record_bytes
    assert entry == {"message": "diagnostic truncated", "truncated": True}


def test_compacted_utf16_record_exactly_at_cap_keeps_structured_payload(tmp_path: Path) -> None:
    logger_name = "tests.debug.compacted_equal_boundary"
    record_budget = 1024
    for attempt in range(8):
        calibration_root = tmp_path / f"calibration_{attempt}"
        calibration_config = DebugLogConfig(
            max_bytes=4096,
            max_record_bytes=record_budget,
            encoding="utf-16",
            logger_name=logger_name,
        )
        calibration_logger = configure_debug_logging(calibration_root, calibration_config)
        try:
            debug_event("compact_boundary", "x" * 10_000, logger=calibration_logger, phase="capture")
        finally:
            close_debug_logging(calibration_logger)

        written = (calibration_root / calibration_config.file_name).read_bytes()
        entry = cast(Mapping[str, object], json.loads(written.decode("utf-16")))
        assert "event" in entry
        next_budget = len(written) + len("".encode("utf-16"))
        if next_budget == record_budget:
            break
        record_budget = next_budget
    else:
        pytest.fail("public compaction calibration did not reach an exact budget")

    config = DebugLogConfig(
        max_bytes=4096,
        max_record_bytes=record_budget,
        encoding="utf-16",
        logger_name=logger_name,
    )
    logger = configure_debug_logging(tmp_path / "exact", config)
    try:
        debug_event("compact_boundary", "x" * 10_000, logger=logger, phase="capture")
    finally:
        close_debug_logging(logger)

    final_entry = cast(
        Mapping[str, object],
        json.loads((tmp_path / "exact" / config.file_name).read_text(encoding="utf-16")),
    )
    assert final_entry["event"] == "compact_boundary"
    assert final_entry["truncated"] is True
    assert cast(str, final_entry["message"]).endswith("…")
