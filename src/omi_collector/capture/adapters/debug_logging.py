"""Redacted opt-in DEBUG diagnostics and a compact runtime-status projection."""

from __future__ import annotations

import json
import logging
import re
import sys
import traceback
import weakref
from collections.abc import Callable, Iterable, Mapping, Sequence
from contextlib import suppress
from dataclasses import dataclass
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
_MAX_JOURNAL_CODE_PARTS = 8
# Journal diagnostics are a projection, not a dump of caller-provided text.
# Every string that survives this boundary is assembled from fixed code values.
_JOURNAL_CODE_PARTS = {
    part: part
    for part in (
        "1m",
        "acquired",
        "address",
        "att",
        "attempt",
        "auto",
        "available",
        "away",
        "ble",
        "blocked",
        "callback",
        "central",
        "change",
        "characteristics",
        "clean",
        "clock",
        "close",
        "command",
        "configuration",
        "connect",
        "connecting",
        "connection",
        "cooldown",
        "data",
        "degraded",
        "deleted",
        "device",
        "disable",
        "disconnect",
        "download",
        "drain",
        "drained",
        "duplicate",
        "enable",
        "enabled",
        "error",
        "event",
        "factory",
        "failed",
        "failure",
        "fatal",
        "feature",
        "firmware",
        "force",
        "gatt",
        "hci",
        "host",
        "identity",
        "initiated",
        "intent",
        "invalid",
        "length",
        "link",
        "local",
        "lock",
        "malformed",
        "mark",
        "marker",
        "metrics",
        "mismatch",
        "mtu",
        "none",
        "not",
        "notifications",
        "observation",
        "observed",
        "observer",
        "operational",
        "outcome",
        "overflow",
        "packet",
        "parameter",
        "parser",
        "partial",
        "pendant",
        "peripheral",
        "phy",
        "prefix",
        "preserved",
        "processor",
        "progress",
        "public",
        "publication",
        "publish",
        "published",
        "quality",
        "quarantine",
        "query",
        "queue",
        "random",
        "reading",
        "ready",
        "receive",
        "recovery",
        "released",
        "remote",
        "request",
        "requested",
        "required",
        "resolved",
        "retired",
        "retry",
        "retryable",
        "returned",
        "rssi",
        "scan",
        "services",
        "session",
        "setup",
        "shutdown",
        "socket",
        "start",
        "started",
        "status",
        "stopped",
        "storage",
        "success",
        "sweep",
        "sync",
        "teardown",
        "terminal",
        "timeout",
        "transient",
        "transition",
        "truncated",
        "unexpected",
        "unknown",
        "unsupported",
        "update",
        "validated",
        "wait",
        "waiting",
        "write",
        "writer",
    )
}
_JOURNAL_TEXT_FIELDS = frozenset(
    {
        "detail",
        "disconnect_class",
        "disconnect_reason_name",
        "event",
        "observer_status",
        "operation",
        "peer_address_type",
        "phase",
        "phy_policy",
        "reason",
        "role",
        "rx_phy",
        "status",
        "status_name",
        "tx_phy",
    }
)
_JOURNAL_FIELDS = frozenset(
    {
        "attempt_id",
        "battery_percent",
        "bytes_per_second",
        "connection_parameter_requests",
        "connection_parameter_updates",
        "connection_rssi_dbm",
        "data_length",
        "data_length_transitions",
        "detail",
        "disconnect_class",
        "disconnect_reason_hex",
        "disconnect_reason_name",
        "dropped_packets",
        "duration_seconds",
        "effective_parameters",
        "effective_phy",
        "elapsed_seconds",
        "error_name",
        "error_type",
        "eta_seconds",
        "event",
        "fields",
        "final_connection_parameters",
        "firmware",
        "handle",
        "initial_connection_parameters",
        "initial_phy_snapshot",
        "interval_ms",
        "latency",
        "lock_context",
        "max_interval_ms",
        "max_rx_octets",
        "max_rx_time",
        "max_tx_octets",
        "max_tx_time",
        "min_interval_ms",
        "next_attempt_in_seconds",
        "observer_status",
        "operation",
        "packet_bytes",
        "payload_bytes",
        "peer_address_type",
        "peer_address_type_hex",
        "phase",
        "phy_policy",
        "phy_transitions",
        "phy_update_outcomes",
        "progress",
        "reason",
        "record",
        "records_completed",
        "records_per_second",
        "records_total",
        "remaining_bytes",
        "remaining_packets",
        "retry_seconds",
        "role",
        "role_hex",
        "rssi_dbm",
        "rx_phy",
        "source",
        "status",
        "status_hex",
        "status_name",
        "supervision_timeout_ms",
        "total_bytes",
        "tx_phy",
        "unavailable",
    }
)
_JOURNAL_FIELD_KEYS = {name: name for name in _JOURNAL_FIELDS}
_JOURNAL_HEX_FIELDS = frozenset({"disconnect_reason_hex", "peer_address_type_hex", "role_hex", "status_hex"})
_RUNTIME_NUMERIC_FIELDS = frozenset(
    {
        "battery_percent",
        "bytes_per_second",
        "duration_seconds",
        "elapsed_seconds",
        "eta_seconds",
        "next_attempt_in_seconds",
        "payload_bytes",
        "records_completed",
        "records_per_second",
        "records_total",
        "remaining_bytes",
        "remaining_packets",
        "retry_seconds",
        "total_bytes",
    }
)
_RUNTIME_CODE_FIELDS = frozenset({"event", "outcome", "phase", "reason", "status"})
_RUNTIME_ERROR_TYPES = {
    part: part
    for part in (
        "BleakError",
        "CollectorTimeoutError",
        "ConnectionError",
        "DeviceAlreadyRunningError",
        "OSError",
        "PermissionError",
        "RingTransportDisconnected",
        "RingTransportUnavailable",
        "RuntimeError",
        "TimeoutError",
        "UnexpectedError",
    )
}
_FIRMWARE_VERSION = re.compile(r"v?\d{1,3}(?:\.\d{1,3}){1,3}")

type ExceptionInfo = bool | BaseException | tuple[type[BaseException], BaseException, TracebackType | None] | None
type LoggedExceptionInfo = tuple[type[BaseException], BaseException, TracebackType | None] | tuple[None, None, None]


@dataclass(frozen=True, slots=True)
class _SafeTraceback:
    text: str


class _RedactingDebugFilter(logging.Filter):
    """Accept only projected fields and internally constructed stack summaries."""

    def filter(self, record: logging.LogRecord) -> bool:
        event = _journal_code(getattr(record, "debug_event", None))
        fields = _journal_fields(getattr(record, "debug_fields", {}))
        safe_traceback = getattr(record, "debug_traceback", None)
        detail = safe_traceback.text if isinstance(safe_traceback, _SafeTraceback) else ""
        rendered_fields = json.dumps(fields, ensure_ascii=True, separators=(",", ":"), sort_keys=True)
        record.msg = f"{event}: {detail} fields={rendered_fields}"
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
    """Project one event before handing it to a logger or runtime sink."""
    target = logger or logging.getLogger(DEFAULT_CONFIG.observability.debug_log.logger_name)
    _install_filter(target)
    del message  # Caller text is untrusted, even when common credential patterns are redacted.
    journal_fields = _journal_fields(fields)
    safe_traceback = _exception_summary(_exception_info(exc_info)) if exc_info else None
    with suppress(Exception):
        target.debug(
            "diagnostic",
            extra={
                "debug_event": _journal_code(event),
                "debug_fields": journal_fields,
                "debug_traceback": safe_traceback,
            },
            exc_info=None,
        )
    sink = getattr(target, _RUNTIME_SINK_ATTRIBUTE, None)
    if isinstance(sink, weakref.WeakMethod):
        sink = sink()
    if callable(sink) and event in {"sync_progress", "ble_link_rssi_observed"}:
        with suppress(Exception):
            sink(event, _runtime_fields(event, fields), datetime.now(UTC).isoformat(timespec="microseconds"))


def debug_exception(
    event: str,
    error: BaseException,
    message: str | None = None,
    *,
    logger: logging.Logger | None = None,
    **fields: object,
) -> None:
    """Log exception type and stack without backend-controlled exception text."""
    info: ExceptionInfo = None if _is_expected_connect_timeout(event, error, fields) else error
    _emit_debug_event(event, message, logger, fields, info)


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
    fields = _runtime_fields(event, fields)
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


def _is_expected_connect_timeout(event: str, error: BaseException, fields: Mapping[str, object]) -> bool:
    return (
        event == "session_error"
        and fields.get("phase") == "connect"
        and isinstance(error, collector.CollectorTimeoutError)
    )


def _exception_summary(exc_info: LoggedExceptionInfo) -> _SafeTraceback:
    error_type, _, traceback_ = exc_info
    name = error_type.__name__ if error_type is not None else "Exception"
    package_root = Path(__file__).resolve().parents[2]
    frames: list[str] = []
    if traceback_ is not None:
        for frame in traceback.extract_tb(traceback_, limit=16):
            try:
                source = Path(frame.filename).relative_to(package_root).as_posix()
            except ValueError:
                source = "<external>"
            frames.append(f"{source}:{frame.lineno}")
    return _SafeTraceback(f"{name} at {' <- '.join(frames) if frames else '<no stack>'}")


def _journal_code(value: object) -> str:
    if not isinstance(value, str):
        return "<omitted>"
    if value == "read/reconcile":
        return "read/reconcile"
    parts = value.split("_")
    if not parts or len(parts) > _MAX_JOURNAL_CODE_PARTS or any(part not in _JOURNAL_CODE_PARTS for part in parts):
        return "<omitted>"
    return "_".join(_JOURNAL_CODE_PARTS[part] for part in parts)


def _journal_fields(fields: object) -> dict[str, object]:
    if not isinstance(fields, Mapping):
        return {}
    try:
        return {
            _JOURNAL_FIELD_KEYS[name]: _journal_value(value, name)
            for name, value in fields.items()
            if isinstance(name, str) and name in _JOURNAL_FIELDS and name != "address"
        }
    except Exception:  # noqa: BLE001 - diagnostic fields are best effort
        return {"unavailable": True}


def _journal_value(value: object, field_name: str) -> object:
    if value is None:
        result: object = None
    elif isinstance(value, bool):
        result = bool(value)
    elif isinstance(value, int):
        result = int(value)
    elif isinstance(value, float):
        result = float(value) if isfinite(value) else None
    elif isinstance(value, Mapping):
        result = _journal_fields(value)
    elif isinstance(value, Sequence) and not isinstance(value, str | bytes):
        result = [_journal_value(item, field_name) for item in value]
    elif isinstance(value, str) and field_name in _JOURNAL_TEXT_FIELDS:
        result = _journal_code(value)
    elif isinstance(value, str) and field_name in _JOURNAL_HEX_FIELDS:
        try:
            result = int(value, 16) if re.fullmatch(r"0x[0-9a-fA-F]{2}", value) else "<omitted>"
        except ValueError:
            result = "<omitted>"
    else:
        result = "<omitted>"
    return result


def _runtime_fields(event: str, fields: Mapping[str, object]) -> dict[str, object]:
    if event == "ble_link_rssi_observed":
        rssi = fields.get("rssi_dbm")
        return {"rssi_dbm": rssi} if isinstance(rssi, int) and not isinstance(rssi, bool) else {}
    progress = fields.get("progress")
    if event != "sync_progress" or not isinstance(progress, Mapping):
        return {}
    safe_progress: dict[str, object] = {}
    for name, value in progress.items():
        projected = _runtime_progress_value(name, value)
        if projected is not None:
            safe_progress[name] = projected
    safe_progress.setdefault("status", "unknown")
    return {"progress": safe_progress}


def _runtime_progress_value(name: object, value: object) -> object | None:
    if name in _RUNTIME_NUMERIC_FIELDS and isinstance(value, int | float) and not isinstance(value, bool):
        return value if isfinite(value) else None
    if name in _RUNTIME_CODE_FIELDS:
        code = _journal_code(value)
        return code if code != "<omitted>" else None
    if name == "error_type" and isinstance(value, str):
        return _RUNTIME_ERROR_TYPES.get(value)
    if name == "firmware" and isinstance(value, str) and _FIRMWARE_VERSION.fullmatch(value):
        return value
    return None
