"""Persisted staging schema, value objects, and scalar validation."""

from __future__ import annotations

from dataclasses import dataclass
from hashlib import sha256
from json import JSONDecodeError, loads
from pathlib import Path
from typing import cast

from ..domain.ring_protocol import RECORD_SIZE

_DESCRIPTOR_NAME = "attempt.json"
_RAW_NAME = "records.bin"
_MANIFEST_NAME = "manifest.json"
_RECEIPT_NAME = "receipt.json"
_CHECKPOINT_NAME = "checkpoint.json"
_PREFIX_PUBLICATION_NAME = "prefix-publication.json"
_TERMINAL_RETIRED_NAME = "terminal-retired.json"
_TERMINAL_RETIRED_STATE = "terminal-retired"
_TERMINAL_RETIREMENT_VERSION = 1
_PUBLISHED_QUARANTINE_NAME = "published.json"
_UNPROCESSABLE_QUARANTINE_NAME = "unprocessable.json"
_PUBLISHED_QUARANTINE_STATE = "published"
_UNPROCESSABLE_QUARANTINE_STATE = "unprocessable"
_ATTEMPT_ID_LENGTH = 32
_UUID_HEX_LENGTH = 32
_SHA256_LENGTH = 64


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


class DeviceAlreadyRunningError(StagingError):
    """Another coordinator holds the exclusive spool lock for this device."""


class MaintenanceDeferredError(Exception):
    """Cooperative stop between bounded maintenance I/O quanta."""


@dataclass(frozen=True, slots=True)
class DurablePrefix:
    """The byte-verified prefix retained in a streaming attempt."""

    start_sequence: int
    next_sequence: int
    record_count: int
    raw_sha256: str


@dataclass(frozen=True, slots=True)
class StreamingCheckpoint:
    """The only durable proof of a streaming attempt's local byte prefix."""

    version: int
    attempt_id: str
    record_count: int
    raw_sha256: str


@dataclass(frozen=True, slots=True)
class AttemptDescriptor:
    """The fsynced transfer intent needed before a caller may issue READ."""

    attempt_id: str
    device_slug: str
    start_sequence: int
    packet_count: int
    record_size: int = RECORD_SIZE
    read_begin_start: int | None = None
    read_begin_count: int | None = None


def _validate_attempt_id(attempt_id: str) -> None:
    if len(attempt_id) != _ATTEMPT_ID_LENGTH or any(char not in "0123456789abcdef" for char in attempt_id):
        raise AttemptStateError("attempt id is invalid")


def _validate_slug(device_slug: str) -> None:
    allowed = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-"
    if not device_slug or any(char not in allowed for char in device_slug):
        raise AttemptStateError("device slug may contain only letters, digits, underscores, and hyphens")


def _validate_int(value: int, label: str) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise AttemptStateError(f"{label} must be a non-negative integer")


def _validate_count(value: int) -> None:
    _validate_int(value, "packet_count")
    if value == 0:
        raise AttemptStateError("packet_count must be positive")


def _validate_terminalized_at(value: int) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise AttemptStateError("terminalized_at_unix_ns must be a positive integer")


def _is_sha256(value: str) -> bool:
    return len(value) == _SHA256_LENGTH and all(char in "0123456789abcdef" for char in value)


def _validate_descriptor(descriptor: AttemptDescriptor, path_id: str) -> None:
    _validate_attempt_id(descriptor.attempt_id)
    _validate_slug(descriptor.device_slug)
    _validate_int(descriptor.start_sequence, "start_sequence")
    _validate_count(descriptor.packet_count)
    if descriptor.attempt_id != path_id or descriptor.record_size != RECORD_SIZE:
        raise AttemptStateError("attempt descriptor identity or record size is invalid")
    if (descriptor.read_begin_start is None) != (descriptor.read_begin_count is None):
        raise AttemptStateError("attempt descriptor has incomplete READ_BEGIN")
    if descriptor.read_begin_start is not None and descriptor.read_begin_count is not None:
        _validate_int(descriptor.read_begin_start, "read_begin_start")
        _validate_count(descriptor.read_begin_count)
        if (descriptor.read_begin_start, descriptor.read_begin_count) != (
            descriptor.start_sequence,
            descriptor.packet_count,
        ):
            raise AttemptStateError("attempt descriptor READ_BEGIN does not match requested range")


def _required_str(raw: dict[str, object], key: str) -> str:
    value = raw.get(key)
    if not isinstance(value, str):
        raise ValueError(key)
    return value


def _required_int(raw: dict[str, object], key: str) -> int:
    value = raw.get(key)
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(key)
    return value


def _optional_int(raw: dict[str, object], key: str) -> int | None:
    value = raw.get(key)
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(key)
    return value


def _descriptor_from_json(raw: dict[str, object]) -> AttemptDescriptor:
    if set(raw) != {
        "attempt_id",
        "device_slug",
        "start_sequence",
        "packet_count",
        "record_size",
        "read_begin_start",
        "read_begin_count",
    }:
        raise ValueError("attempt descriptor schema")
    return AttemptDescriptor(
        attempt_id=_required_str(raw, "attempt_id"),
        device_slug=_required_str(raw, "device_slug"),
        start_sequence=_required_int(raw, "start_sequence"),
        packet_count=_required_int(raw, "packet_count"),
        record_size=_required_int(raw, "record_size"),
        read_begin_start=_optional_int(raw, "read_begin_start"),
        read_begin_count=_optional_int(raw, "read_begin_count"),
    )


def _is_int(value: object, expected: int) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and value == expected


def _checkpoint_from_json(raw: dict[str, object], attempt_id: str) -> StreamingCheckpoint:
    if set(raw) != {"version", "attempt_id", "record_count", "raw_sha256"}:
        raise ValueError("checkpoint schema")
    checkpoint = StreamingCheckpoint(
        _required_int(raw, "version"),
        _required_str(raw, "attempt_id"),
        _required_int(raw, "record_count"),
        _required_str(raw, "raw_sha256"),
    )
    if checkpoint.version != 1 or checkpoint.attempt_id != attempt_id:
        raise ValueError("checkpoint identity or version")
    if checkpoint.record_count < 0 or not _is_sha256(checkpoint.raw_sha256):
        raise ValueError("checkpoint range or hash")
    return checkpoint


def _read_checkpoint(path: Path, attempt_id: str) -> StreamingCheckpoint:
    try:
        raw = cast(object, loads(path.read_text(encoding="utf-8")))
        if not isinstance(raw, dict):
            raise ValueError("checkpoint object")
        return _checkpoint_from_json(cast(dict[str, object], raw), attempt_id)
    except (OSError, JSONDecodeError, UnicodeDecodeError, TypeError, ValueError) as error:
        raise AttemptStateError("streaming checkpoint is malformed") from error


def _manifest_prefix(descriptor: AttemptDescriptor, prefix: DurablePrefix) -> dict[str, object]:
    return {
        "device_slug": descriptor.device_slug,
        "start_sequence": prefix.start_sequence,
        "next_sequence": prefix.next_sequence,
        "record_count": prefix.record_count,
        "raw_sha256": prefix.raw_sha256,
    }


def _sha256_bytes(payload: bytes) -> str:
    return sha256(payload).hexdigest()


def _as_object(value: object) -> dict[str, object]:
    if not isinstance(value, dict):
        raise TypeError("expected JSON object")
    return cast(dict[str, object], value)
