"""Append-only filesystem adapter for low-rate transfer-quality JSONL."""

from __future__ import annotations

import json
import logging
import os
import queue
import re
from pathlib import Path
from threading import Lock, Thread

from ...config import DEFAULT_CONFIG, QualityMetricsConfig
from ..application.quality_metrics import QualityMetricsPort, SequenceLossMetric, TransferSessionMetric

_REVISION = re.compile(r"[0-9a-f]{7,40}")


class QualityMetricsError(RuntimeError):
    """The evidence journal could not accept a complete durable event."""


class JsonlQualityMetrics(QualityMetricsPort):
    """Queue complete fsynced lines without holding up capture."""

    def __init__(
        self,
        collector_root: Path,
        *,
        release_version: str,
        source_revision: str | None = None,
        config: QualityMetricsConfig = DEFAULT_CONFIG.observability.quality_metrics,
        diagnostic_logger: logging.Logger | None = None,
    ) -> None:
        self._path = Path(collector_root) / config.file_name
        self._max_bytes = config.max_bytes
        self._backup_count = config.backup_count
        self._max_record_bytes = config.max_record_bytes
        self._encoding = config.encoding
        self._write_lock = Lock()
        self._queue: queue.Queue[bytes | None] = queue.Queue(maxsize=config.queue_max_records)
        self._state_lock = Lock()
        self._closed = False
        self._stop_enqueued = False
        self._dropped_records = 0
        self._shutdown_timeout = config.shutdown_join_seconds
        self._logger = diagnostic_logger or logging.getLogger(__name__)
        self.release_version = _require_release_version(release_version)
        self.source_revision = normalize_source_revision(source_revision)
        self._thread = Thread(target=self._run, name="omi-quality-metrics", daemon=True)
        self._thread.start()

    @property
    def path(self) -> Path:
        return self._path

    def record_transfer_session(self, metric: TransferSessionMetric) -> None:
        self._enqueue(metric.as_dict())

    def record_sequence_loss(self, metric: SequenceLossMetric) -> None:
        self._enqueue(metric.as_dict())

    def close(self, timeout_seconds: float | None = None) -> bool:
        """Stop the daemon writer, waiting only for the configured bounded interval."""
        with self._state_lock:
            self._closed = True
            timeout = self._shutdown_timeout if timeout_seconds is None else timeout_seconds
            enqueue_stop = not self._stop_enqueued
            self._stop_enqueued = True
        if enqueue_stop:
            while True:
                try:
                    self._queue.put_nowait(None)
                    break
                except queue.Full:
                    try:
                        self._queue.get_nowait()
                    except queue.Empty:
                        continue
                    self._record_drop("shutdown_queue_overflow")
        self._thread.join(timeout)
        stopped = not self._thread.is_alive()
        if not stopped:
            self._diagnose("shutdown_timeout")
        return stopped

    @property
    def dropped_records(self) -> int:
        with self._state_lock:
            return self._dropped_records

    def _enqueue(self, value: dict[str, object]) -> None:
        try:
            line = (
                json.dumps(value, ensure_ascii=False, separators=(",", ":"), allow_nan=False).encode(self._encoding)
                + b"\n"
            )
            if len(line) > self._max_record_bytes:
                raise QualityMetricsError("quality metric record exceeds configured limit")
        except QualityMetricsError:
            raise
        except (TypeError, ValueError, UnicodeError) as error:
            raise QualityMetricsError("cannot encode quality metrics") from error
        drop_reason: str | None = None
        with self._state_lock:
            if self._closed:
                drop_reason = "writer_closed"
            else:
                try:
                    self._queue.put_nowait(line)
                except queue.Full:
                    drop_reason = "queue_full"
        if drop_reason is not None:
            self._record_drop(drop_reason)

    def _run(self) -> None:
        while True:
            line = self._queue.get()
            if line is None:
                return
            try:
                self._append(line)
            except Exception as error:  # noqa: BLE001 - auxiliary storage must not kill collection
                self._diagnose("write_failed", error)

    def _record_drop(self, reason: str) -> None:
        with self._state_lock:
            self._dropped_records += 1
        self._diagnose(reason)

    def _diagnose(self, event: str, error: BaseException | None = None) -> None:
        try:
            if error is None:
                self._logger.warning("quality metrics %s", event)
            else:
                self._logger.warning("quality metrics %s: %s", event, error)
        except Exception:  # noqa: BLE001 - diagnostics must not affect collection
            return

    def _append(self, line: bytes) -> None:
        try:
            if len(line) > self._max_record_bytes:
                raise QualityMetricsError("quality metric record exceeds configured limit")
            with self._write_lock:
                self._path.parent.mkdir(mode=0o750, parents=True, exist_ok=True)
                self._rotate_if_needed(len(line))
                current_created = not self._path.exists()
                descriptor = os.open(self._path, os.O_APPEND | os.O_CREAT | os.O_WRONLY, 0o640)
                try:
                    _write_all(descriptor, line)
                    os.fsync(descriptor)
                finally:
                    os.close(descriptor)
                if current_created:
                    _fsync_parent(self._path)
        except QualityMetricsError:
            raise
        except (OSError, TypeError, ValueError, UnicodeError) as error:
            raise QualityMetricsError(f"cannot append quality metrics: {self._path.name}") from error

    def _rotate_if_needed(self, incoming_bytes: int) -> None:
        current_size = self._path.stat().st_size if self._path.exists() else 0
        if current_size == 0 or current_size + incoming_bytes <= self._max_bytes:
            return
        oldest = self._path.with_name(f"{self._path.name}.{self._backup_count}")
        if oldest.exists():
            oldest.unlink()
        for index in range(self._backup_count - 1, 0, -1):
            source = self._path.with_name(f"{self._path.name}.{index}")
            if source.exists():
                source.replace(self._path.with_name(f"{self._path.name}.{index + 1}"))
        self._path.replace(self._path.with_name(f"{self._path.name}.1"))
        _fsync_parent(self._path)


def source_revision_from_environment(
    environ: dict[str, str] | None = None,
    config: QualityMetricsConfig = DEFAULT_CONFIG.observability.quality_metrics,
) -> str | None:
    """Read deployment provenance only; never inspect a runtime working tree."""
    values = os.environ if environ is None else environ
    return normalize_source_revision(values.get(config.source_revision_env))


def normalize_source_revision(value: str | None) -> str | None:
    if value is None or value == "":
        return None
    if _REVISION.fullmatch(value) is None:
        raise ValueError("source revision must be 7 to 40 lowercase hexadecimal characters")
    return value[:12]


def _require_release_version(value: str) -> str:
    if not isinstance(value, str) or not value:
        raise ValueError("release version must be a non-empty string")
    return value


def _write_all(descriptor: int, payload: bytes) -> None:
    offset = 0
    while offset < len(payload):
        written = os.write(descriptor, payload[offset:])
        if written <= 0:
            raise OSError("quality metrics write made no progress")
        offset += written


def _fsync_parent(path: Path) -> None:
    directory = os.open(path.parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(directory)
    finally:
        os.close(directory)
