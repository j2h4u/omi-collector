"""Small typed contract for durable, low-rate transfer quality evidence."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Protocol
from uuid import uuid4

from ..domain.ring_protocol import RECORD_SIZE


def utc_timestamp(epoch_seconds: float) -> str:
    """Render an explicit UTC completion timestamp with millisecond resolution."""
    return datetime.fromtimestamp(epoch_seconds, UTC).isoformat(timespec="milliseconds")


@dataclass(slots=True)
class SessionQuality:
    """Mutable accounting limited to one connected physical session."""

    device_slug: str
    advertisement_rssi_dbm: int | None
    phy_policy: str
    session_id: str = ""
    active_read_elapsed_ms: int = 0
    requested_record_count: int = 0
    received_raw_bytes: int = 0
    submitted_raw_bytes: int = 0
    written_raw_bytes: int = 0
    firmware_version: str | None = None
    attempted_read: bool = False

    def __post_init__(self) -> None:
        if not self.session_id:
            self.session_id = uuid4().hex

    def note_read(self, elapsed_seconds: float, requested_records: int) -> None:
        self.attempted_read = True
        self.active_read_elapsed_ms += max(0, round(elapsed_seconds * 1000))
        self.requested_record_count += requested_records

    def note_counters(self, received: int, submitted: int, written: int) -> None:
        self.received_raw_bytes += max(0, received)
        self.submitted_raw_bytes += max(0, submitted)
        self.written_raw_bytes += max(0, written)


@dataclass(frozen=True, slots=True)
class TransferSessionMetric:
    """One terminal quality event for a physical session that attempted READ."""

    completed_at: str
    session_id: str
    device_slug: str
    outcome: str
    termination_class: str
    active_read_elapsed_ms: int
    requested_record_count: int
    received_raw_bytes: int
    submitted_raw_bytes: int
    written_raw_bytes: int
    release_version: str
    source_revision: str | None
    firmware_version: str | None
    phy_policy: str
    advertisement_rssi_dbm: int | None

    def as_dict(self) -> dict[str, object]:
        return {
            "schema_version": 1,
            "event": "transfer_session",
            "completed_at": self.completed_at,
            "session_id": self.session_id,
            "device_slug": self.device_slug,
            "outcome": self.outcome,
            "termination_class": self.termination_class,
            "active_read_elapsed_ms": self.active_read_elapsed_ms,
            "requested_record_count": self.requested_record_count,
            "record_size_bytes": RECORD_SIZE,
            "received_raw_bytes": self.received_raw_bytes,
            "submitted_raw_bytes": self.submitted_raw_bytes,
            "written_raw_bytes": self.written_raw_bytes,
            "release_version": self.release_version,
            "source_revision": self.source_revision,
            "firmware_version": self.firmware_version,
            "phy_policy": self.phy_policy,
            "advertisement_rssi_dbm": self.advertisement_rssi_dbm,
        }


@dataclass(frozen=True, slots=True)
class SequenceLossMetric:
    """One confirmed cursor-ahead loss event; missing identities stay private."""

    occurred_at: str
    session_id: str
    device_slug: str
    missing_record_count: int
    missing_raw_bytes: int
    reason: str
    release_version: str
    source_revision: str | None
    firmware_version: str | None

    def as_dict(self) -> dict[str, object]:
        return {
            "schema_version": 1,
            "event": "sequence_loss",
            "occurred_at": self.occurred_at,
            "session_id": self.session_id,
            "device_slug": self.device_slug,
            "missing_record_count": self.missing_record_count,
            "missing_raw_bytes": self.missing_raw_bytes,
            "reason": self.reason,
            "release_version": self.release_version,
            "source_revision": self.source_revision,
            "firmware_version": self.firmware_version,
        }


class QualityMetricsPort(Protocol):
    """Best-effort boundary; callers catch failures before collection can see them."""

    release_version: str
    source_revision: str | None

    def record_transfer_session(self, metric: TransferSessionMetric) -> None: ...

    def record_sequence_loss(self, metric: SequenceLossMetric) -> None: ...
