"""Durable, typed evidence for pendant clock observations.

Clock observations are deliberately separate from the operational/debug journal.
The journal is a useful projection; this append-only ledger is the authority used
when deciding whether a clock correction can be reconciled after a restart.
"""

# The JSON boundary is validated immediately below; decoded values are Any by
# definition until those checks complete.
# pyright: reportAny=false
# pyright: reportArgumentType=false

from __future__ import annotations

import json
import math
import os
from dataclasses import asdict, dataclass, replace
from pathlib import Path
from typing import Final, TextIO, cast
from uuid import uuid4

try:
    import fcntl
except ImportError:  # pragma: no cover - Windows is not a supported runtime.
    fcntl = None  # type: ignore[assignment]


class ClockObservationError(ValueError):
    """The clock-evidence ledger or an observation is invalid."""


_SCHEMA_VERSION: Final = 1
_KINDS: Final = frozenset({"native_trusted", "anchored_monotonic"})
_ROLES: Final = frozenset({"standalone", "initial", "later"})
_MAX_UINT64: Final = (1 << 64) - 1
_MAX_TEXT: Final = 256
_MAX_RAW_TIMESTAMP: Final = (1 << 32) - 1
_SHA256_HEX_LENGTH: Final = 64


@dataclass(frozen=True, slots=True)
class ClockObservation:
    """One immutable observation, ordered by causal insertion order."""

    version: int
    observation_id: str
    causal_order: int
    evidence_kind: str
    session_id: str
    host_boot_id: str
    host_realtime_start: float
    host_realtime_end: float
    host_monotonic_start: float
    host_monotonic_end: float
    device_epoch: int
    info_sequence_min: int
    info_sequence_max: int
    operation_id: str | None = None
    effective_boundary_sequence: int | None = None
    observation_role: str = "standalone"
    parent_observation_id: str | None = None
    raw_timestamp: int | None = None
    raw_timestamp_hash: str | None = None

    def __post_init__(self) -> None:
        _validate_observation(self)

    @property
    def observation_boundary(self) -> int:
        """The sequence frontier observed, not the correction's effective edge."""
        return self.info_sequence_max


class ClockObservationStore:
    """Append-only JSON records with strict canonical decoding and fsync."""

    def __init__(self, device_state_path: Path) -> None:
        self._root = Path(device_state_path).parent / "clock-observations"

    def append(  # noqa: PLR0913 - the schema fields are intentionally explicit
        self,
        *,
        evidence_kind: str,
        session_id: str,
        host_boot_id: str,
        host_realtime_start: float,
        host_realtime_end: float,
        host_monotonic_start: float,
        host_monotonic_end: float,
        device_epoch: int,
        info_sequence_min: int,
        info_sequence_max: int,
        operation_id: str | None = None,
        effective_boundary_sequence: int | None = None,
        observation_id: str | None = None,
        observation_role: str = "standalone",
        parent_observation_id: str | None = None,
        raw_timestamp: int | None = None,
        raw_timestamp_hash: str | None = None,
    ) -> ClockObservation:
        """Persist one observation before consumers may use it."""
        records = self.records()
        order = records[-1].causal_order + 1 if records else 0
        parent_observation_id, observation_role = _causal_fields(
            records,
            operation_id,
            observation_role,
            parent_observation_id,
            (host_boot_id, info_sequence_min, info_sequence_max, device_epoch),
        )
        observation = ClockObservation(
            _SCHEMA_VERSION,
            observation_id or uuid4().hex,
            order,
            evidence_kind,
            session_id,
            host_boot_id,
            host_realtime_start,
            host_realtime_end,
            host_monotonic_start,
            host_monotonic_end,
            device_epoch,
            info_sequence_min,
            info_sequence_max,
            operation_id,
            effective_boundary_sequence,
            observation_role,
            parent_observation_id,
            raw_timestamp,
            raw_timestamp_hash,
        )
        return self._write_new(observation)

    def native_trusted(self, **values: object) -> ClockObservation:
        """Convenience constructor for a host-trusted native sample."""
        values["evidence_kind"] = "native_trusted"
        return self.append(**cast(dict[str, object], values))

    def anchored_monotonic(self, **values: object) -> ClockObservation:
        """Convenience constructor for bounded historical evidence."""
        values["evidence_kind"] = "anchored_monotonic"
        return self.append(**cast(dict[str, object], values))

    def establish_effective_boundary(
        self, observation: ClockObservation, boundary_sequence: int, *, operation_id: str | None = None
    ) -> ClockObservation:
        """Record a causal boundary decision without rewriting source evidence."""
        if observation not in self.records():
            raise ClockObservationError("observation is not in this ledger")
        if boundary_sequence < 0 or boundary_sequence > observation.info_sequence_max:
            raise ClockObservationError("effective boundary is outside observation frontier")
        if observation.effective_boundary_sequence is not None:
            if observation.effective_boundary_sequence != boundary_sequence:
                raise ClockObservationError("effective boundary conflicts with durable evidence")
            return observation
        return self.append(
            evidence_kind=observation.evidence_kind,
            session_id=observation.session_id,
            host_boot_id=observation.host_boot_id,
            host_realtime_start=observation.host_realtime_start,
            host_realtime_end=observation.host_realtime_end,
            host_monotonic_start=observation.host_monotonic_start,
            host_monotonic_end=observation.host_monotonic_end,
            device_epoch=observation.device_epoch,
            info_sequence_min=observation.info_sequence_min,
            info_sequence_max=observation.info_sequence_max,
            operation_id=operation_id or observation.operation_id,
            effective_boundary_sequence=boundary_sequence,
            observation_role=observation.observation_role,
            parent_observation_id=observation.observation_id,
            raw_timestamp=observation.raw_timestamp,
            raw_timestamp_hash=observation.raw_timestamp_hash,
            observation_id=f"{observation.observation_id}-{boundary_sequence:016x}",
        )

    def records(self) -> tuple[ClockObservation, ...]:
        if not self._root.exists():
            return ()
        paths = sorted(self._root.glob("*.json"))
        records = tuple(sorted((self._read(path) for path in paths), key=lambda item: item.causal_order))
        previous = -1
        for item in records:
            if item.causal_order != previous + 1:
                raise ClockObservationError("clock observation causal order has a gap")
            previous = item.causal_order
        return records

    def for_operation(self, operation_id: str) -> tuple[ClockObservation, ...]:
        if not operation_id:
            raise ValueError("operation_id is required")
        return tuple(item for item in self.records() if item.operation_id == operation_id)

    def _write_new(self, observation: ClockObservation) -> ClockObservation:
        try:
            existed = self._root.exists()
            self._root.mkdir(mode=0o750, parents=True, exist_ok=True)
            if not existed:
                _sync_directory(self._root.parent)
        except OSError as error:
            raise ClockObservationError("clock observation directory is not durable") from error
        path = self._root / f"{observation.observation_id}.json"
        temporary = path.with_name(f".{path.name}.{uuid4().hex}.tmp")
        payload = _canonical(asdict(observation))
        with _file_lock(self._root / ".lock"):
            if path.exists():
                existing = self._read(path)
                if not _same_observation(existing, observation):
                    raise ClockObservationError("clock observation identity conflicts with durable evidence")
                return existing
            records = self.records()
            expected_order = records[-1].causal_order + 1 if records else 0
            if observation.causal_order != expected_order:
                observation = ClockObservation(
                    observation.version,
                    observation.observation_id,
                    expected_order,
                    observation.evidence_kind,
                    observation.session_id,
                    observation.host_boot_id,
                    observation.host_realtime_start,
                    observation.host_realtime_end,
                    observation.host_monotonic_start,
                    observation.host_monotonic_end,
                    observation.device_epoch,
                    observation.info_sequence_min,
                    observation.info_sequence_max,
                    observation.operation_id,
                    observation.effective_boundary_sequence,
                    observation.observation_role,
                    observation.parent_observation_id,
                    observation.raw_timestamp,
                    observation.raw_timestamp_hash,
                )
                payload = _canonical(asdict(observation))
            try:
                with temporary.open("xb") as stream:
                    stream.write(payload)
                    stream.flush()
                    os.fsync(stream.fileno())
                temporary.replace(path)
                _sync_directory(self._root)
            except OSError as error:
                temporary.unlink(missing_ok=True)
                raise ClockObservationError("clock observation is not durable") from error
        return observation

    @staticmethod
    def _read(path: Path) -> ClockObservation:
        try:
            raw = path.read_bytes()
            document = cast(object, json.loads(raw.decode("utf-8")))
            if not isinstance(document, dict) or raw != _canonical(document):
                raise ValueError("non-canonical JSON")
            expected = {
                "version",
                "observation_id",
                "causal_order",
                "evidence_kind",
                "session_id",
                "host_boot_id",
                "host_realtime_start",
                "host_realtime_end",
                "host_monotonic_start",
                "host_monotonic_end",
                "device_epoch",
                "info_sequence_min",
                "info_sequence_max",
                "operation_id",
                "effective_boundary_sequence",
                "observation_role",
                "parent_observation_id",
                "raw_timestamp",
                "raw_timestamp_hash",
            }
            if set(document) != expected:
                raise ValueError("unexpected clock observation fields")
            item = ClockObservation(**cast(dict[str, object], document))
            if path.stem != item.observation_id:
                raise ValueError("clock observation filename mismatch")
            return item
        except (OSError, UnicodeDecodeError, json.JSONDecodeError, TypeError, ValueError) as error:
            raise ClockObservationError(f"clock observation ledger is invalid: {path.name}") from error


def _validate_observation(item: ClockObservation) -> None:
    _validate_identity(item)
    _validate_intervals(item)
    _validate_sequences(item)
    _validate_references(item)
    _validate_raw_evidence(item)
    _validate_effective_boundary(item)


def _validate_identity(item: ClockObservation) -> None:
    if item.version != _SCHEMA_VERSION or item.evidence_kind not in _KINDS:
        raise ClockObservationError("clock observation schema or evidence kind is invalid")
    for name in ("observation_id", "session_id", "host_boot_id"):
        value = getattr(item, name)
        if not isinstance(value, str) or not value or len(value) > _MAX_TEXT:
            raise ClockObservationError(f"clock observation {name} is invalid")
    if isinstance(item.causal_order, bool) or not isinstance(item.causal_order, int) or item.causal_order < 0:
        raise ClockObservationError("clock observation causal order is invalid")


def _validate_intervals(item: ClockObservation) -> None:
    for name in ("host_realtime_start", "host_realtime_end", "host_monotonic_start", "host_monotonic_end"):
        value = getattr(item, name)
        if isinstance(value, bool) or not isinstance(value, int | float) or not math.isfinite(float(value)):
            raise ClockObservationError(f"clock observation {name} is invalid")
    if item.host_realtime_end < item.host_realtime_start or item.host_monotonic_end < item.host_monotonic_start:
        raise ClockObservationError("clock observation interval is reversed")


def _validate_sequences(item: ClockObservation) -> None:
    for name in ("device_epoch", "info_sequence_min", "info_sequence_max"):
        value = getattr(item, name)
        if isinstance(value, bool) or not isinstance(value, int) or not 0 <= value <= _MAX_UINT64:
            raise ClockObservationError(f"clock observation {name} is invalid")
    if item.info_sequence_max < item.info_sequence_min:
        raise ClockObservationError("clock observation sequence bounds are invalid")


def _validate_references(item: ClockObservation) -> None:
    if item.operation_id is not None and (not isinstance(item.operation_id, str) or not item.operation_id):
        raise ClockObservationError("clock observation operation reference is invalid")
    if item.observation_role not in _ROLES:
        raise ClockObservationError("clock observation role is invalid")
    if item.parent_observation_id is not None and (
        not isinstance(item.parent_observation_id, str) or not item.parent_observation_id
    ):
        raise ClockObservationError("clock observation parent reference is invalid")


def _validate_raw_evidence(item: ClockObservation) -> None:
    if item.raw_timestamp is not None and (
        isinstance(item.raw_timestamp, bool)
        or not isinstance(item.raw_timestamp, int)
        or not 0 <= item.raw_timestamp <= _MAX_RAW_TIMESTAMP
    ):
        raise ClockObservationError("clock observation raw timestamp is invalid")
    if item.raw_timestamp_hash is not None and (
        not isinstance(item.raw_timestamp_hash, str)
        or len(item.raw_timestamp_hash) != _SHA256_HEX_LENGTH
        or any(char not in "0123456789abcdef" for char in item.raw_timestamp_hash)
    ):
        raise ClockObservationError("clock observation raw timestamp hash is invalid")
    if (item.raw_timestamp is None) != (item.raw_timestamp_hash is None):
        raise ClockObservationError("clock observation raw timestamp evidence is incomplete")


def _validate_effective_boundary(item: ClockObservation) -> None:
    boundary = item.effective_boundary_sequence
    if boundary is not None and (isinstance(boundary, bool) or not isinstance(boundary, int)):
        raise ClockObservationError("clock observation effective boundary is invalid")
    if boundary is not None and not 0 <= boundary <= item.info_sequence_max:
        raise ClockObservationError("clock observation effective boundary is outside bounds")


def _causal_fields(
    records: tuple[ClockObservation, ...],
    operation_id: str | None,
    observation_role: str,
    parent_observation_id: str | None,
    identity: tuple[str, int, int, int],
) -> tuple[str | None, str]:
    if operation_id is None:
        return parent_observation_id, observation_role
    host_boot_id, info_sequence_min, info_sequence_max, device_epoch = identity
    parent_observation_id = parent_observation_id or _matching_standalone_parent(
        records, host_boot_id, info_sequence_min, info_sequence_max, device_epoch
    )
    if observation_role == "standalone":
        observation_role = _infer_operation_role(records, operation_id, parent_observation_id)
    if observation_role == "later" and parent_observation_id is None:
        parent_observation_id = _matching_initial_parent(records, operation_id)
    return parent_observation_id, observation_role


def _matching_standalone_parent(
    records: tuple[ClockObservation, ...],
    host_boot_id: str,
    info_sequence_min: int,
    info_sequence_max: int,
    device_epoch: int,
) -> str | None:
    parent = next(
        (
            item
            for item in reversed(records)
            if item.operation_id is None
            and item.host_boot_id == host_boot_id
            and item.info_sequence_min == info_sequence_min
            and item.info_sequence_max == info_sequence_max
            and item.device_epoch == device_epoch
        ),
        None,
    )
    return parent.observation_id if parent is not None else None


def _infer_operation_role(
    records: tuple[ClockObservation, ...], operation_id: str, parent_observation_id: str | None
) -> str:
    if parent_observation_id is not None:
        return "initial"
    return (
        "later"
        if any(item.operation_id == operation_id and item.observation_role == "initial" for item in records)
        else "standalone"
    )


def _matching_initial_parent(records: tuple[ClockObservation, ...], operation_id: str) -> str | None:
    parent = next(
        (
            item
            for item in reversed(records)
            if item.operation_id == operation_id and item.observation_role == "initial"
        ),
        None,
    )
    return parent.observation_id if parent is not None else None


def _canonical(value: object) -> bytes:
    return (json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8")


def _same_observation(left: ClockObservation, right: ClockObservation) -> bool:
    return replace(left, causal_order=right.causal_order) == right


def _sync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


class _file_lock:
    """Advisory lock for one append; readers can remain lock-free."""

    def __init__(self, path: Path) -> None:
        self._path = path
        self._stream: TextIO | None = None

    def __enter__(self) -> _file_lock:
        if fcntl is None:
            return self
        stream = self._path.open("a+", encoding="ascii")
        fcntl.flock(stream.fileno(), fcntl.LOCK_EX)
        self._stream = stream
        return self

    def __exit__(self, _type: object, _value: object, _traceback: object) -> None:
        if self._stream is not None and fcntl is not None:
            fcntl.flock(self._stream.fileno(), fcntl.LOCK_UN)
            self._stream.close()
