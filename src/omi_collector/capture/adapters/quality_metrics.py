"""Append-only filesystem adapter for low-rate transfer-quality JSONL."""

from __future__ import annotations

import json
import os
import re
from pathlib import Path

from ...config import DEFAULT_CONFIG, QualityMetricsConfig
from ..application.quality_metrics import QualityMetricsPort, SequenceLossMetric, TransferSessionMetric

_REVISION = re.compile(r"[0-9a-f]{7,40}")


class QualityMetricsError(RuntimeError):
    """The evidence journal could not accept a complete durable event."""


class JsonlQualityMetrics(QualityMetricsPort):
    """Synchronously append one complete fsynced line from the service process."""

    def __init__(
        self,
        collector_root: Path,
        *,
        release_version: str,
        source_revision: str | None = None,
        config: QualityMetricsConfig = DEFAULT_CONFIG.observability.quality_metrics,
    ) -> None:
        self._path = Path(collector_root) / config.file_name
        self._encoding = config.encoding
        self.release_version = _require_release_version(release_version)
        self.source_revision = normalize_source_revision(source_revision)

    @property
    def path(self) -> Path:
        return self._path

    def record_transfer_session(self, metric: TransferSessionMetric) -> None:
        self._append(metric.as_dict())

    def record_sequence_loss(self, metric: SequenceLossMetric) -> None:
        self._append(metric.as_dict())

    def _append(self, value: dict[str, object]) -> None:
        try:
            line = (
                json.dumps(value, ensure_ascii=False, separators=(",", ":"), allow_nan=False).encode(self._encoding)
                + b"\n"
            )
            self._path.parent.mkdir(mode=0o750, parents=True, exist_ok=True)
            descriptor = os.open(self._path, os.O_APPEND | os.O_CREAT | os.O_WRONLY, 0o640)
            try:
                _write_all(descriptor, line)
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
        except (OSError, TypeError, ValueError, UnicodeError) as error:
            raise QualityMetricsError(f"cannot append quality metrics: {self._path.name}") from error


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
