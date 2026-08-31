"""Bounded, structured DEBUG diagnostics kept out of the system journal."""

from __future__ import annotations

import copy
import json
import logging
import queue
import re
from collections.abc import Mapping, Sequence
from contextlib import suppress
from dataclasses import dataclass
from datetime import UTC, datetime
from logging.handlers import QueueHandler, RotatingFileHandler
from math import isfinite
from pathlib import Path
from threading import Lock, Thread
from types import TracebackType

from omi_collector.capture.application import collector

from ...config import DEFAULT_CONFIG, DebugLogConfig

_SINK_ATTRIBUTE = "_omi_collector_debug_sink"
_PENDING_SINK_ATTRIBUTE = "_omi_collector_pending_debug_sink"
_MIN_DEBUG_RECORD_BYTES = 512
_SENSITIVE_FIELD_PARTS = frozenset({"audio", "authorization", "cookie", "credential", "password", "secret", "token"})
_REDACTION_PATTERNS = (
    (re.compile(r"(?i)\bbearer\s+[^\s,;]+"), "Bearer <redacted>"),
    (re.compile(r"(?im)^(\s*(?:authorization|cookie)\s*:\s*)[^\r\n]*$"), r"\1<redacted>"),
    (
        re.compile(r"(?i)\b(authorization|cookie|password|secret|token|api[_-]?key)\b\s*([:=])\s*[^\s,;]+"),
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


class _RingQueueHandler(QueueHandler):
    """Bound diagnostics retained by producer threads while disk is stalled."""

    def __init__(self, record_queue: queue.Queue[logging.LogRecord | None]) -> None:
        super().__init__(record_queue)
        self._dropped_records = 0
        self._drop_lock = Lock()

    @property
    def dropped_records(self) -> int:
        with self._drop_lock:
            return self._dropped_records

    def prepare(self, record: logging.LogRecord) -> logging.LogRecord:
        """Preserve exception formatting for the listener, never the caller."""
        return copy.copy(record)

    def enqueue(self, record: logging.LogRecord) -> None:
        try:
            self.queue.put_nowait(record)
        except queue.Full:
            with self._drop_lock:
                self._dropped_records += 1

    def handleError(self, record: logging.LogRecord) -> None:
        """Diagnostic failure must never surface through the collector."""

    def record_discarded(self, count: int) -> None:
        with self._drop_lock:
            self._dropped_records += count


class _SafeRotatingFileHandler(RotatingFileHandler):
    """Keep an unavailable diagnostic filesystem from affecting collection."""

    def handleError(self, record: logging.LogRecord) -> None:
        """Discard a failed diagnostic write without emitting to stderr."""

    def _open(self):
        stream = super()._open()
        with suppress(OSError):
            Path(self.baseFilename).chmod(0o640)
        return stream

    def shouldRollover(self, record: logging.LogRecord) -> bool:
        if self.maxBytes <= 0:
            return False
        encoded = (self.format(record) + self.terminator).encode(self.encoding or "utf-8", errors="replace")
        try:
            current_bytes = Path(self.baseFilename).stat().st_size
        except OSError:
            current_bytes = 0
        return current_bytes + len(encoded) >= self.maxBytes


class _RingListener:
    """A daemon listener whose shutdown wait is strictly bounded."""

    def __init__(
        self,
        record_queue: queue.Queue[logging.LogRecord | None],
        file_handler: _SafeRotatingFileHandler,
        queue_handler: _RingQueueHandler,
    ) -> None:
        self._queue = record_queue
        self._file_handler = file_handler
        self._queue_handler = queue_handler
        self._thread = Thread(target=self._run, name="omi-debug-log", daemon=True)

    def start(self) -> None:
        self._thread.start()

    def is_alive(self) -> bool:
        return self._thread.is_alive()

    def stop(self, timeout_seconds: float) -> bool:
        discarded = 0
        while True:
            try:
                self._queue.put_nowait(None)
                break
            except queue.Full:
                try:
                    discarded_record = self._queue.get_nowait()
                except queue.Empty:
                    continue
                if discarded_record is not None:
                    discarded += 1
        self._queue_handler.record_discarded(discarded)
        self._thread.join(timeout_seconds)
        return not self._thread.is_alive()

    def _run(self) -> None:
        try:
            while True:
                record = self._queue.get()
                if record is None:
                    return
                self._file_handler.handle(record)
        finally:
            self._file_handler.close()


class _JsonLineFormatter(logging.Formatter):
    """Render records as bounded, redacted, self-contained JSONL diagnostics."""

    def __init__(self, config: DebugLogConfig, terminator_bytes: int) -> None:
        super().__init__()
        _validate_debug_record_config(config)
        self._max_record_bytes = config.max_record_bytes - terminator_bytes
        self._encoding = config.encoding

    def format(self, record: logging.LogRecord) -> str:
        try:
            return self._render_bounded(self._payload(record))
        except Exception:  # noqa: BLE001 - logging must not kill its listener
            return self._render_bounded(_fallback_payload())

    def _payload(self, record: logging.LogRecord) -> dict[str, object]:
        payload: dict[str, object] = {
            "timestamp": datetime.fromtimestamp(record.created, UTC).isoformat(timespec="milliseconds"),
            "level": _safe_text(record.levelname, "DEBUG"),
            "logger": _safe_text(record.name, "unknown"),
            "process_id": record.process,
            "process_name": _safe_text(record.processName, "unknown"),
            "thread_id": record.thread,
            "thread_name": _safe_text(record.threadName, "unknown"),
            "event": _safe_text(getattr(record, "debug_event", None), "debug_event"),
            "message": _safe_record_message(record),
            "fields": _safe_record_fields(record),
        }
        if record.exc_info:
            payload["traceback"] = _safe_traceback(self, record.exc_info)
        return payload

    def _render_bounded(self, payload: dict[str, object]) -> str:
        rendered = _json_text(payload)
        if _encoded_size(rendered, self._encoding) <= self._max_record_bytes:
            return rendered
        compact = _compact_payload(payload, self._max_record_bytes)
        rendered = _json_text(compact)
        if _encoded_size(rendered, self._encoding) <= self._max_record_bytes:
            return rendered
        return _json_text({"message": "diagnostic truncated", "truncated": True})


@dataclass(frozen=True, slots=True)
class _RingSink:
    queue_handler: _RingQueueHandler
    listener: _RingListener
    file_handler: _SafeRotatingFileHandler
    shutdown_join_seconds: float

    def close(self) -> tuple[int, bool]:
        try:
            stopped = self.listener.stop(self.shutdown_join_seconds)
        except Exception:  # noqa: BLE001 - diagnostics must not affect collection
            stopped = False
        return self.queue_handler.dropped_records, stopped


def debug_log_path(
    collector_root: Path,
    config: DebugLogConfig = DEFAULT_CONFIG.observability.debug_log,
    *,
    file_name: str | None = None,
) -> Path:
    """Return the dedicated debug-ring path directly beneath collector state."""
    return Path(collector_root) / (file_name or config.file_name)


def configure_debug_logging(
    collector_root: Path,
    config: DebugLogConfig = DEFAULT_CONFIG.observability.debug_log,
    *,
    file_name: str | None = None,
) -> logging.Logger:
    """Configure the independent DEBUG ring, degrading safely if storage fails."""
    _validate_debug_record_config(config)
    logger = logging.getLogger(config.logger_name)
    logger.setLevel(logging.DEBUG)
    logger.propagate = False
    if _pending_sink_is_alive(logger):
        return logger
    _close_existing_ring_sinks(logger)
    if _pending_sink_is_alive(logger):
        return logger
    file_handler: _SafeRotatingFileHandler | None = None
    try:
        path = debug_log_path(collector_root, config, file_name=file_name)
        path.parent.mkdir(mode=0o750, parents=True, exist_ok=True)
        path.parent.chmod(0o750)
        file_handler = _SafeRotatingFileHandler(
            path,
            maxBytes=config.max_bytes,
            backupCount=config.backup_count,
            encoding=config.encoding,
            delay=True,
        )
        file_handler.setLevel(logging.DEBUG)
        terminator_bytes = len(file_handler.terminator.encode(config.encoding, errors="replace"))
        file_handler.setFormatter(_JsonLineFormatter(config, terminator_bytes))
        record_queue: queue.Queue[logging.LogRecord | None] = queue.Queue(maxsize=config.queue_max_records)
        queue_handler = _RingQueueHandler(record_queue)
        queue_handler.setLevel(logging.DEBUG)
        listener = _RingListener(record_queue, file_handler, queue_handler)
        listener.start()
        sink = _RingSink(queue_handler, listener, file_handler, config.shutdown_join_seconds)
        setattr(queue_handler, _SINK_ATTRIBUTE, sink)
        logger.addHandler(queue_handler)
    except Exception:  # noqa: BLE001 - diagnostics must not affect collection
        if file_handler is not None:
            file_handler.close()
    return logger


def _validate_debug_record_config(config: DebugLogConfig) -> None:
    """Enforce the formatter's minimum complete-record budget at its owner."""
    if config.max_record_bytes < _MIN_DEBUG_RECORD_BYTES:
        raise ValueError(f"max_record_bytes must be at least {_MIN_DEBUG_RECORD_BYTES}")


def close_debug_logging(logger: logging.Logger) -> int:
    """Detach this ring and return its producer-side drop count without indefinite waiting."""
    return _close_existing_ring_sinks(logger)


def debug_event(
    event: str,
    message: str | None = None,
    *,
    logger: logging.Logger | None = None,
    exc_info: ExceptionInfo = None,
    **fields: object,
) -> None:
    """Queue one safe lifecycle/control diagnostic without blocking its caller."""
    try:
        target = logger or logging.getLogger(DEFAULT_CONFIG.observability.debug_log.logger_name)
        target.debug(
            message or event,
            extra={"debug_event": event, "debug_fields": _sanitize_fields(fields)},
            exc_info=exc_info,
        )
    except Exception:  # noqa: BLE001 - diagnostics must not affect collection
        return


def debug_exception(
    event: str,
    error: BaseException,
    message: str | None = None,
    *,
    logger: logging.Logger | None = None,
    **fields: object,
) -> None:
    """Queue an exception without calling its ``__str__`` on the caller thread."""
    if _is_expected_connect_timeout(event, error, fields):
        debug_event(event, message or type(error).__name__, logger=logger, exc_info=None, **fields)
        return
    debug_event(event, message or type(error).__name__, logger=logger, exc_info=error, **fields)


def _is_expected_connect_timeout(event: str, error: BaseException, fields: Mapping[str, object]) -> bool:
    """Keep routine pendant absence as a concise event instead of a traceback."""
    return (
        event == "session_error"
        and fields.get("phase") == "connect"
        and isinstance(error, collector.CollectorTimeoutError)
    )


def _close_existing_ring_sinks(logger: logging.Logger) -> int:
    dropped_records = 0
    for handler in tuple(logger.handlers):
        sink = getattr(handler, _SINK_ATTRIBUTE, None)
        if not isinstance(sink, _RingSink):
            continue
        logger.removeHandler(handler)
        dropped, stopped = sink.close()
        dropped_records += dropped
        if not stopped:
            setattr(logger, _PENDING_SINK_ATTRIBUTE, sink)
    return dropped_records


def _pending_sink_is_alive(logger: logging.Logger) -> bool:
    sink = getattr(logger, _PENDING_SINK_ATTRIBUTE, None)
    if not isinstance(sink, _RingSink):
        return False
    if sink.listener.is_alive():
        return True
    delattr(logger, _PENDING_SINK_ATTRIBUTE)
    return False


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
    return {_field_name(name): _sanitize_value(value, _field_name(name)) for name, value in fields.items()}


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
        result = str(value)
    elif isinstance(value, Mapping):
        result = {_field_name(name): _sanitize_value(item, _field_name(name)) for name, item in value.items()}
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


def _compact_payload(payload: Mapping[str, object], max_record_bytes: int) -> dict[str, object]:
    text_limit = max(16, max_record_bytes // 32)
    compact: dict[str, object] = {
        "timestamp": _compact_text(payload.get("timestamp"), text_limit),
        "level": _compact_text(payload.get("level"), text_limit),
        "logger": _compact_text(payload.get("logger"), text_limit),
        "process_id": payload.get("process_id", 0),
        "process_name": _compact_text(payload.get("process_name"), text_limit),
        "thread_id": payload.get("thread_id", 0),
        "thread_name": _compact_text(payload.get("thread_name"), text_limit),
        "event": _compact_text(payload.get("event"), text_limit),
        "message": _compact_text(payload.get("message"), text_limit),
        "fields": {"truncated": True},
        "truncated": True,
    }
    if "traceback" in payload:
        compact["traceback"] = _compact_text(payload["traceback"], text_limit)
    return compact


def _compact_text(value: object, limit: int) -> str:
    if not isinstance(value, str):
        return "<unavailable>"
    if len(value) <= limit:
        return value
    return value[: max(1, limit - 1)] + "…"


def _fallback_payload() -> dict[str, object]:
    return {"message": "diagnostic formatting failed", "truncated": True}


def _json_text(payload: Mapping[str, object]) -> str:
    return json.dumps(payload, ensure_ascii=True, separators=(",", ":"), sort_keys=True)


def _encoded_size(value: str, encoding: str) -> int:
    return len(value.encode(encoding, errors="replace"))
