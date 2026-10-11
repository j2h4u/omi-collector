"""Redacted opt-in DEBUG diagnostics and a compact runtime-status projection."""

from __future__ import annotations

import json
import logging
import re
import sys
import weakref
from collections.abc import Callable, Iterable, Mapping, Sequence
from contextlib import suppress
from datetime import UTC, datetime
from math import isfinite
from pathlib import Path
from types import TracebackType
from typing import cast

from omi_collector.capture.application import collector

from ...config import DEFAULT_CONFIG, DebugLogConfig
from .collector_state_db import StateDB, StateKind

_FILTER_ATTRIBUTE = "_omi_collector_debug_filter"
_RUNTIME_SINK_ATTRIBUTE = "_omi_collector_runtime_status_sink"
_EXCEPTION_INFO_LENGTH = 3
_SENSITIVE_FIELD_PARTS = frozenset(
    {
        "access_key",
        "access_token",
        "api_key",
        "audio",
        "authorization",
        "client_secret",
        "cookie",
        "credential",
        "password",
        "private_key",
        "secret",
        "secret_key",
        "signature",
        "token",
    }
)
_REDACTION_PATTERNS = (
    (re.compile(r"(?i)\bbearer\s+[^\s,;]+"), "Bearer <redacted>"),
    (re.compile(r"(?im)^(\s*(?:authorization|cookie)\s*:\s*)[^\r\n]*$"), r"\1<redacted>"),
    (
        re.compile(
            r"(?i)\b(authorization|cookie|password|secret|token|api[_-]?key|access[_-]?key|access[_-]?token|"
            r"client[_-]?secret|private[_-]?key|secret[_-]?key|signature)\b\s*([:=])\s*[^\s,;]+"
        ),
        r"\1\2<redacted>",
    ),
    (
        re.compile(
            r"(?i)([?&](?:x-amz-(?:credential|signature|security-token)|x-goog-(?:credential|signature)|signature|sig|token|access_token|refresh_token|id_token|api[_-]?key|awsaccesskeyid|googleaccessid|key-pair-id)=)[^&#\s]+"
        ),
        r"\1<redacted>",
    ),
)

type ExceptionInfo = bool | BaseException | tuple[type[BaseException], BaseException, TracebackType | None] | None
type LoggedExceptionInfo = tuple[type[BaseException], BaseException, TracebackType | None] | tuple[None, None, None]


class _RedactingDebugFilter(logging.Filter):
    """Sanitize all debug records before they can reach a journal handler."""

    def filter(self, record: logging.LogRecord) -> bool:
        event = _safe_text(getattr(record, "debug_event", None), "debug_event")
        message = _safe_record_message(record)
        fields = _safe_record_fields(record)
        if record.exc_info:
            traceback = _safe_traceback(logging.Formatter(), record.exc_info)
            message = f"{message}\n{traceback}"
        rendered_fields = json.dumps(fields, ensure_ascii=True, separators=(",", ":"), sort_keys=True)
        record.msg = f"{event}: {message} fields={rendered_fields}"
        record.args = ()
        record.exc_info = None
        return True


def configure_debug_logging(
    collector_root: Path,
    config: DebugLogConfig = DEFAULT_CONFIG.observability.debug_log,
    *,
    file_name: str | None = None,
) -> logging.Logger:
    """Route redacted opt-in diagnostics through the configured root logger."""
    del collector_root, file_name
    logger = logging.getLogger(config.logger_name)
    logger.setLevel(logging.DEBUG)
    logger.propagate = True
    _install_filter(logger)
    return logger


def set_runtime_status_sink(logger: logging.Logger, sink: Callable[[str, dict[str, object], str], bool] | None) -> None:
    """Attach the existing bounded quality worker as the status projection sink."""
    if sink is None:
        if hasattr(logger, _RUNTIME_SINK_ATTRIBUTE):
            delattr(logger, _RUNTIME_SINK_ATTRIBUTE)
    else:
        try:
            stored_sink: object = weakref.WeakMethod(sink)  # type: ignore[arg-type]
        except TypeError:
            stored_sink = sink
        setattr(logger, _RUNTIME_SINK_ATTRIBUTE, stored_sink)


def _install_filter(logger: logging.Logger) -> None:
    existing = getattr(logger, _FILTER_ATTRIBUTE, None)
    if not isinstance(existing, _RedactingDebugFilter):
        filter_ = _RedactingDebugFilter()
        logger.addFilter(filter_)
        setattr(logger, _FILTER_ATTRIBUTE, filter_)


def close_debug_logging(logger: logging.Logger) -> int:
    """Detach the runtime projection sink; the quality worker owns queue shutdown."""
    set_runtime_status_sink(logger, None)
    return 0


def debug_event(
    event: str,
    message: str | None = None,
    *,
    logger: logging.Logger | None = None,
    exc_info: ExceptionInfo = None,
    **fields: object,
) -> None:
    """Emit a redacted diagnostic and enqueue only status-changing callbacks."""
    _emit_debug_event(event, message, logger, fields, exc_info)


def _emit_debug_event(
    event: str,
    message: str | None,
    logger: logging.Logger | None,
    fields: Mapping[str, object],
    exc_info: ExceptionInfo,
) -> None:
    """Sanitize one event before handing it to any logger or runtime sink."""
    target = logger or logging.getLogger(DEFAULT_CONFIG.observability.debug_log.logger_name)
    _install_filter(target)
    try:
        safe_fields = _sanitize_fields(fields)
    except Exception:  # noqa: BLE001 - diagnostic fields are best effort
        safe_fields = {"unavailable": True}
    safe_event = _redact_text(event)
    safe_message = _redact_text(message or event)
    if exc_info:
        safe_message = f"{safe_message}\n{_safe_traceback(logging.Formatter(), _exception_info(exc_info))}"
    with suppress(Exception):
        target.debug(
            safe_message,
            extra={"debug_event": safe_event, "debug_fields": safe_fields},
            exc_info=None,
        )
    sink = getattr(target, _RUNTIME_SINK_ATTRIBUTE, None)
    if isinstance(sink, weakref.WeakMethod):
        sink = sink()
    if callable(sink) and event in {"sync_progress", "ble_link_rssi_observed"}:
        with suppress(Exception):
            sink(event, safe_fields, datetime.now(UTC).isoformat(timespec="microseconds"))


def debug_exception(
    event: str,
    error: BaseException,
    message: str | None = None,
    *,
    logger: logging.Logger | None = None,
    **fields: object,
) -> None:
    """Queue an exception without exposing unsanitized text to stderr or journald."""
    detail = message or type(error).__name__
    if not _is_expected_connect_timeout(event, error, fields):
        detail = f"{detail}\n{_exception_traceback(error)}"
    _emit_debug_event(event, detail, logger, fields, None)


def read_runtime_status(state_db: StateDB) -> dict[str, object] | None:
    """Return the validated current runtime projection for device status."""
    value = state_db.get(StateKind.RUNTIME_STATUS, "current")
    if value is None:
        return None
    _validate_runtime_projection(value)
    return value


def merge_runtime_status(
    current: dict[str, object] | None,
    event: str,
    fields: Mapping[str, object],
    timestamp: str,
) -> dict[str, object] | None:
    """Merge one validated current-status callback, independently by observation time."""
    parsed = _parse_timestamp(timestamp)
    if parsed is None:
        raise ValueError("runtime status event timestamp is invalid")
    _validate_runtime_event({"event": event, "fields": dict(fields), "timestamp": timestamp})
    current = current or {
        "battery_observation": None,
        "battery_observed_at": None,
        "connection_rssi_dbm": None,
        "connection_rssi_observed_at": None,
        "last_error": None,
        "last_error_at": None,
        "observation": None,
        "observation_at": None,
        "progress": None,
        "updated_at": None,
    }
    if event == "sync_progress":
        progress = cast(dict[str, object], fields["progress"])
        if _is_newer(timestamp, current.get("updated_at")):
            current["progress"] = progress
            current["updated_at"] = timestamp
        if progress.get("event") == "pendant_observation" and _is_newer(timestamp, current.get("observation_at")):
            current["observation"] = progress
            current["observation_at"] = timestamp
            battery = progress.get("battery_percent")
            if isinstance(battery, int) and not isinstance(battery, bool):
                current["battery_observation"] = progress
                current["battery_observed_at"] = timestamp
        if progress.get("status") == "session_error" and _is_newer(timestamp, current.get("last_error_at")):
            current["last_error"] = progress
            current["last_error_at"] = timestamp
    elif event == "ble_link_rssi_observed":
        rssi = cast(int, fields["rssi_dbm"])
        if _is_newer(timestamp, current.get("connection_rssi_observed_at")):
            current["connection_rssi_dbm"] = rssi
            current["connection_rssi_observed_at"] = timestamp
    _validate_runtime_projection(current)
    return current


def write_runtime_status_event(state_db: StateDB, event: str, fields: Mapping[str, object], timestamp: str) -> None:
    """Merge one queued projection event atomically with any concurrent status writer."""
    with state_db.transaction() as transaction:
        current = transaction.get(StateKind.RUNTIME_STATUS, "current")
        merged = merge_runtime_status(current, event, fields, timestamp)
        if merged is not None:
            transaction.put(StateKind.RUNTIME_STATUS, "current", merged)


def import_legacy_runtime_status(state_db: StateDB, rows: Iterable[Mapping[str, object]]) -> None:
    """Reduce validated legacy debug rows to the same current projection in one write."""
    projection = legacy_runtime_projection(rows)
    if projection is not None:
        with state_db.transaction() as transaction:
            transaction.put(StateKind.RUNTIME_STATUS, "current", projection)


def legacy_runtime_projection(rows: Iterable[Mapping[str, object]]) -> dict[str, object] | None:
    """Build the current status projection from legacy rows for migration parity."""
    projection: dict[str, object] | None = None
    for row in rows:
        event = row.get("event")
        if event not in {"sync_progress", "ble_link_rssi_observed"}:
            continue
        timestamp = row.get("timestamp")
        fields = row.get("fields")
        if not isinstance(timestamp, str) or not isinstance(fields, dict):
            raise ValueError("legacy debug status row is invalid")
        projection = merge_runtime_status(projection, cast(str, event), fields, timestamp)
    return projection


def _is_newer(candidate: str, current: object) -> bool:
    previous = _parse_timestamp(current) if isinstance(current, str) else None
    timestamp = _parse_timestamp(candidate)
    return timestamp is not None and (previous is None or timestamp >= previous)


def _validate_runtime_event(value: Mapping[str, object]) -> None:
    if set(value) != {"event", "fields", "timestamp"} or not isinstance(value.get("fields"), dict):
        raise ValueError("runtime status event fields are invalid")
    if _parse_timestamp(value.get("timestamp")) is None:  # type: ignore[arg-type]
        raise ValueError("runtime status event timestamp is invalid")
    event = value.get("event")
    fields = cast(Mapping[str, object], value["fields"])
    if event == "sync_progress":
        progress = fields.get("progress")
        if not isinstance(progress, dict) or not isinstance(progress.get("status"), str):
            raise ValueError("runtime progress event is invalid")
        if progress.get("event") is not None and not isinstance(progress["event"], str):
            raise ValueError("runtime progress observation is invalid")
        battery = progress.get("battery_percent")
        if battery is not None and (isinstance(battery, bool) or not isinstance(battery, int)):
            raise ValueError("runtime battery observation is invalid")
        if progress.get("firmware") is not None and not isinstance(progress["firmware"], str):
            raise ValueError("runtime firmware observation is invalid")
    elif event == "ble_link_rssi_observed":
        rssi = fields.get("rssi_dbm")
        if isinstance(rssi, bool) or not isinstance(rssi, int):
            raise ValueError("runtime RSSI event is invalid")
    else:
        raise ValueError("runtime status event type is unsupported")


def _validate_runtime_projection(value: Mapping[str, object]) -> None:
    expected = {
        "battery_observation",
        "battery_observed_at",
        "connection_rssi_dbm",
        "connection_rssi_observed_at",
        "last_error",
        "last_error_at",
        "observation",
        "observation_at",
        "progress",
        "updated_at",
    }
    if set(value) != expected:
        raise ValueError("runtime status projection fields are invalid")
    for name in (
        "updated_at",
        "observation_at",
        "battery_observed_at",
        "last_error_at",
        "connection_rssi_observed_at",
    ):
        timestamp = value[name]
        if timestamp is not None and (not isinstance(timestamp, str) or _parse_timestamp(timestamp) is None):
            raise ValueError(f"runtime status projection has invalid {name}")
    rssi = value["connection_rssi_dbm"]
    if rssi is not None and (isinstance(rssi, bool) or not isinstance(rssi, int)):
        raise ValueError("runtime status projection has invalid connection RSSI")
    for name in ("progress", "observation", "battery_observation", "last_error"):
        item = value[name]
        if item is not None and not isinstance(item, dict):
            raise ValueError(f"runtime status projection has invalid {name}")


def _parse_timestamp(value: object) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    return parsed.astimezone(UTC) if parsed.tzinfo is not None else None


def _exception_info(value: ExceptionInfo) -> LoggedExceptionInfo:
    if isinstance(value, tuple) and len(value) == _EXCEPTION_INFO_LENGTH:
        return cast(LoggedExceptionInfo, value)
    if isinstance(value, BaseException):
        return type(value), value, value.__traceback__
    if value is True:
        info = sys.exc_info()
        return cast(LoggedExceptionInfo, info)
    return None, None, None


def _exception_traceback(error: BaseException) -> str:
    return _safe_traceback(logging.Formatter(), (type(error), error, error.__traceback__))


def _is_expected_connect_timeout(event: str, error: BaseException, fields: Mapping[str, object]) -> bool:
    return (
        event == "session_error"
        and fields.get("phase") == "connect"
        and isinstance(error, collector.CollectorTimeoutError)
    )


def _safe_record_message(record: logging.LogRecord) -> str:
    try:
        return _safe_text(record.getMessage(), "<message unavailable>")
    except Exception:  # noqa: BLE001 - user exception __str__ is not trusted
        return "<message unavailable>"


def _safe_traceback(formatter: logging.Formatter, exc_info: LoggedExceptionInfo) -> str:
    try:
        return _safe_text(formatter.formatException(exc_info), "<traceback unavailable>")
    except Exception:  # noqa: BLE001 - user exception __str__ is not trusted
        return "<traceback unavailable>"


def _safe_record_fields(record: logging.LogRecord) -> dict[str, object]:
    fields = getattr(record, "debug_fields", {})
    if not isinstance(fields, Mapping):
        return {}
    try:
        return _sanitize_fields(fields)
    except Exception:  # noqa: BLE001 - diagnostic fields are best effort
        return {"unavailable": True}


def _sanitize_fields(fields: Mapping[str, object]) -> dict[str, object]:
    return {
        _redact_text(_field_name(name)): _sanitize_value(value, _field_name(name)) for name, value in fields.items()
    }


def _sanitize_value(value: object, field_name: str) -> object:
    if _is_sensitive_field(field_name) or isinstance(value, bytes):
        result: object = "<redacted>"
    elif value is None or isinstance(value, bool | int):
        result = value
    elif isinstance(value, str):
        result = _redact_text(value)
    elif isinstance(value, float):
        result = value if isfinite(value) else str(value)
    elif isinstance(value, Path):
        result = _redact_text(str(value))
    elif isinstance(value, Mapping):
        result = {
            _redact_text(_field_name(name)): _sanitize_value(item, _field_name(name)) for name, item in value.items()
        }
    elif isinstance(value, Sequence):
        result = [_sanitize_value(item, field_name) for item in value]
    else:
        result = f"<{type(value).__name__}>"
    return result


def _safe_text(value: object, fallback: str) -> str:
    return _redact_text(value) if isinstance(value, str) else fallback


def _field_name(value: object) -> str:
    return value if isinstance(value, str) else "<non_string>"


def _redact_text(value: str) -> str:
    redacted = value
    for pattern, replacement in _REDACTION_PATTERNS:
        redacted = pattern.sub(replacement, redacted)
    return redacted


def _is_sensitive_field(field_name: str) -> bool:
    normalized = field_name.casefold().replace("-", "_")
    return any(part in normalized for part in _SENSITIVE_FIELD_PARTS)
