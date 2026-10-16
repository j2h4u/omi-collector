"""Persisted staging schema, value objects, and scalar validation."""

from __future__ import annotations

from dataclasses import dataclass

from ..application.storage_fault import FatalInternalAudioStorageError as FatalInternalAudioStorageError
from ..domain.ring_protocol import RECORD_SIZE

_ATTEMPT_ID_LENGTH = 32
_MAX_READ_PACKET_COUNT = (1 << 32) - 1


class StagingError(RuntimeError):
    """Base error for local staging failures."""


class DiskSpaceError(StagingError):
    """The destination filesystem cannot safely hold the requested batch."""


class AttemptStateError(StagingError):
    """An operation does not fit the persisted transfer state."""


class CollisionError(StagingError):
    """A final bundle name exists but does not contain the same bundle."""


class PendingAttemptError(StagingError):
    """Partial staging evidence blocks another READ for the same device."""


@dataclass(frozen=True, slots=True)
class LockContext:
    """Safe attribution captured when another process owns the collector lock."""

    requested_operation: str
    holder_operation: str | None
    holder_pid: int | None
    holder_thread_id: int | None
    holder_age_seconds: float | None
    holder_scope: str
    metadata_status: str

    def as_dict(self) -> dict[str, object]:
        return {
            "requested_operation": self.requested_operation,
            "holder_operation": self.holder_operation,
            "holder_pid": self.holder_pid,
            "holder_thread_id": self.holder_thread_id,
            "holder_age_seconds": self.holder_age_seconds,
            "holder_scope": self.holder_scope,
            "metadata_status": self.metadata_status,
        }


class DeviceAlreadyRunningError(StagingError):
    """Another coordinator holds the exclusive spool lock for this device."""

    def __init__(self, message: str = "pendant recovery is already active", *, lock_context: LockContext | None = None):
        super().__init__(message)
        self.lock_context = lock_context


@dataclass(frozen=True, slots=True)
class DurablePrefix:
    """The byte-verified prefix retained in a streaming attempt."""

    start_sequence: int
    next_sequence: int
    record_count: int
    raw_sha256: str


@dataclass(frozen=True, slots=True)
class AttemptDescriptor:
    """The fsynced transfer intent needed before a caller may issue READ."""

    attempt_id: str
    schema_version: int
    start_sequence: int
    packet_count: int
    record_size: int = RECORD_SIZE
    read_begin_start: int | None = None
    read_begin_count: int | None = None


def _validate_attempt_id(attempt_id: str) -> None:
    if len(attempt_id) != _ATTEMPT_ID_LENGTH or any(char not in "0123456789abcdef" for char in attempt_id):
        raise AttemptStateError("attempt id is invalid")


def _validate_int(value: int, label: str) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise AttemptStateError(f"{label} must be a non-negative integer")


def _validate_count(value: int) -> None:
    _validate_int(value, "packet_count")
    if value == 0:
        raise AttemptStateError("packet_count must be positive")
    if value > _MAX_READ_PACKET_COUNT:
        raise AttemptStateError("packet_count exceeds the protocol uint32 limit")
