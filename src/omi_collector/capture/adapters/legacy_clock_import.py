"""Read-only parsers for the retired JSON clock ledgers used during import."""

from __future__ import annotations

import json
import math
from dataclasses import dataclass
from itertools import pairwise
from pathlib import Path
from typing import cast

from .clock_segments import ClockEpochMembership, ClockSegment, ClockSegmentMap, segments_with_estimates

# The JSON boundary starts as untrusted object data before strict decoding.
# pyright: reportAny=false
# pyright: reportArgumentType=false

_OBSERVATION_FIELDS = frozenset(
    {
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
)
_CORRECTION_FIELDS = frozenset(
    {
        "version",
        "operation_id",
        "state",
        "observed_epoch",
        "target_epoch",
        "drift_seconds",
        "boundary_sequence_min",
        "boundary_sequence_max",
        "verified_epoch",
    }
)
_MEMBERSHIP_FIELDS = frozenset({"observation_id", "session_id", "start_sequence", "next_sequence"})
_VALID_STATES = frozenset({"prepared", "not_written", "unresolved", "not_applied", "applied", "resolved", "unknown"})
_OBSERVATION_VERSION = 1
_CORRECTION_VERSION = 2
_MAX_TEXT = 256
_SHA256_HEX_LENGTH = 64


class LegacyClockImportError(ValueError):
    """Legacy clock evidence is malformed or inconsistent."""


@dataclass(frozen=True, slots=True)
class ClockObservation:
    """Immutable observation decoded from one old canonical JSON record."""

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


@dataclass(frozen=True, slots=True)
class ClockCorrection:
    """Immutable correction record decoded from the old ledger."""

    version: int
    operation_id: str
    state: str
    observed_epoch: int
    target_epoch: int
    drift_seconds: float
    boundary_sequence_min: int
    boundary_sequence_max: int | None = None
    verified_epoch: int | None = None


@dataclass(frozen=True, slots=True)
class ClockMembership:
    """Explicit old same-session interval membership."""

    observation_id: str
    session_id: str
    start_sequence: int
    next_sequence: int


@dataclass(frozen=True, slots=True)
class LegacyClockImport:
    """Validated clock rows and their old UTC projection, read without writes."""

    observations: tuple[ClockObservation, ...]
    corrections: tuple[ClockCorrection, ...]
    memberships: tuple[ClockMembership, ...]
    segments: ClockSegmentMap


def read_legacy_clock(root: Path) -> LegacyClockImport:
    """Read old clock directories beneath a Collector root without mutating them."""
    root = Path(root)
    observations = _read_observations(root / "clock-observations")
    corrections = _read_corrections(root / "clock-corrections")
    memberships = _read_memberships(root / "clock-memberships")
    by_id = {item.observation_id: item for item in observations}
    segments: list[ClockSegment] = []
    for membership in memberships:
        observation = by_id.get(membership.observation_id)
        if observation is None or observation.session_id != membership.session_id:
            raise LegacyClockImportError("clock membership has no durable observation")
        segments.append(
            ClockSegment.from_confirmed_membership(
                observation,
                ClockEpochMembership(membership.observation_id, membership.start_sequence, membership.next_sequence),
            )
        )
    confirmed = ClockSegmentMap(tuple(segments))
    verified = frozenset(item.operation_id for item in corrections if item.state in {"applied", "resolved"})
    return LegacyClockImport(
        observations,
        corrections,
        memberships,
        segments_with_estimates(observations, confirmed, verified),
    )


def _read_observations(root: Path) -> tuple[ClockObservation, ...]:
    rows = tuple(sorted((_read_observation(path) for path in _json_files(root)), key=lambda item: item.causal_order))
    if any(right.causal_order != left.causal_order + 1 for left, right in pairwise(rows)) or (
        rows and rows[0].causal_order != 0
    ):
        raise LegacyClockImportError("clock observation causal order has a gap")
    return rows


def _read_observation(path: Path) -> ClockObservation:
    try:
        raw = path.read_bytes()
        value = cast(object, json.loads(raw.decode("utf-8")))
        if not isinstance(value, dict) or raw != _canonical_observation(value) or set(value) != _OBSERVATION_FIELDS:
            raise ValueError("clock observation JSON is not canonical")
        row = ClockObservation(**cast(dict[str, object], value))
        if path.stem != row.observation_id:
            raise ValueError("clock observation filename mismatch")
        return row
    except (OSError, UnicodeDecodeError, json.JSONDecodeError, TypeError, ValueError) as error:
        raise LegacyClockImportError(f"clock observation ledger is invalid: {path.name}") from error


def _validate_observation(row: ClockObservation) -> None:
    _validate_identity(row)
    _validate_intervals(row)
    _validate_sequences(row)
    _validate_references(row)
    _validate_raw_evidence(row)
    _validate_effective_boundary(row)


def _validate_identity(row: ClockObservation) -> None:
    if row.version != _OBSERVATION_VERSION or row.evidence_kind not in {"native_trusted", "anchored_monotonic"}:
        raise ValueError("clock observation schema is invalid")
    if any(
        not isinstance(item, str) or not item or len(item) > _MAX_TEXT
        for item in (row.observation_id, row.session_id, row.host_boot_id)
    ):
        raise ValueError("clock observation identity is invalid")
    if isinstance(row.causal_order, bool) or not isinstance(row.causal_order, int) or row.causal_order < 0:
        raise ValueError("clock observation order is invalid")


def _validate_intervals(row: ClockObservation) -> None:
    for value in (row.host_realtime_start, row.host_realtime_end, row.host_monotonic_start, row.host_monotonic_end):
        if isinstance(value, bool) or not isinstance(value, int | float) or not math.isfinite(float(value)):
            raise ValueError("clock observation time is invalid")
    if row.host_realtime_end < row.host_realtime_start or row.host_monotonic_end < row.host_monotonic_start:
        raise ValueError("clock observation interval is reversed")


def _validate_sequences(row: ClockObservation) -> None:
    for value in (row.device_epoch, row.info_sequence_min, row.info_sequence_max):
        if isinstance(value, bool) or not isinstance(value, int) or not 0 <= value <= (1 << 64) - 1:
            raise ValueError("clock observation sequence is invalid")
    if row.info_sequence_max < row.info_sequence_min:
        raise ValueError("clock observation sequence bounds are invalid")


def _validate_references(row: ClockObservation) -> None:
    if row.operation_id is not None and (not isinstance(row.operation_id, str) or not row.operation_id):
        raise ValueError("clock observation operation reference is invalid")
    if row.observation_role not in {"standalone", "initial", "later"}:
        raise ValueError("clock observation role is invalid")
    if row.parent_observation_id is not None and (
        not isinstance(row.parent_observation_id, str) or not row.parent_observation_id
    ):
        raise ValueError("clock observation parent reference is invalid")


def _validate_raw_evidence(row: ClockObservation) -> None:
    if row.raw_timestamp is not None and (
        isinstance(row.raw_timestamp, bool)
        or not isinstance(row.raw_timestamp, int)
        or not 0 <= row.raw_timestamp <= (1 << 32) - 1
    ):
        raise ValueError("clock observation raw timestamp is invalid")
    if row.raw_timestamp_hash is not None and (
        not isinstance(row.raw_timestamp_hash, str)
        or len(row.raw_timestamp_hash) != _SHA256_HEX_LENGTH
        or any(char not in "0123456789abcdef" for char in row.raw_timestamp_hash)
    ):
        raise ValueError("clock observation raw timestamp hash is invalid")
    if (row.raw_timestamp is None) != (row.raw_timestamp_hash is None):
        raise ValueError("clock observation raw timestamp evidence is incomplete")


def _validate_effective_boundary(row: ClockObservation) -> None:
    boundary = row.effective_boundary_sequence
    if boundary is not None and (
        isinstance(boundary, bool) or not isinstance(boundary, int) or not 0 <= boundary <= row.info_sequence_max
    ):
        raise ValueError("clock observation effective boundary is invalid")


def _read_corrections(root: Path) -> tuple[ClockCorrection, ...]:
    return tuple(_read_correction(path) for path in _json_files(root))


def _read_correction(path: Path) -> ClockCorrection:
    try:
        value = cast(object, json.loads(path.read_text(encoding="utf-8")))
        if (
            not isinstance(value, dict)
            or set(value) != _CORRECTION_FIELDS
            or value.get("version") != _CORRECTION_VERSION
        ):
            raise ValueError("clock correction schema is invalid")
        correction = ClockCorrection(
            _integer(value, "version"),
            _text(value, "operation_id"),
            _text(value, "state"),
            _integer(value, "observed_epoch"),
            _integer(value, "target_epoch"),
            _number(value, "drift_seconds"),
            _integer(value, "boundary_sequence_min"),
            None if value.get("boundary_sequence_max") is None else _integer(value, "boundary_sequence_max"),
            None if value.get("verified_epoch") is None else _integer(value, "verified_epoch"),
        )
        if correction.boundary_sequence_min < 0 or (
            correction.boundary_sequence_max is not None
            and correction.boundary_sequence_max < correction.boundary_sequence_min
        ):
            raise ValueError("clock correction boundary is invalid")
        if correction.state not in _VALID_STATES:
            raise ValueError("clock correction state is invalid")
        if correction.state in {"prepared", "not_written", "unresolved"} and (
            correction.boundary_sequence_max is not None or correction.verified_epoch is not None
        ):
            raise ValueError("clock correction state fields are invalid")
        if correction.state == "not_applied" and correction.verified_epoch is not None:
            raise ValueError("clock correction state fields are invalid")
        if correction.state == "unknown" and (
            correction.boundary_sequence_max is None or correction.verified_epoch is not None
        ):
            raise ValueError("unknown clock correction fields are invalid")
        if correction.state in {"applied", "resolved"} and (
            correction.boundary_sequence_max is None or correction.verified_epoch is None
        ):
            raise ValueError("confirmed clock correction has no verified evidence")
        return correction
    except (OSError, UnicodeDecodeError, json.JSONDecodeError, TypeError, ValueError) as error:
        raise LegacyClockImportError("clock correction ledger is invalid") from error


def _read_memberships(root: Path) -> tuple[ClockMembership, ...]:
    rows = tuple(_read_membership(path) for path in _json_files(root))
    if any(left.next_sequence > right.start_sequence for left, right in pairwise(rows)):
        raise LegacyClockImportError("clock memberships overlap")
    return rows


def _read_membership(path: Path) -> ClockMembership:
    try:
        value = cast(object, json.loads(path.read_text(encoding="utf-8")))
        if not isinstance(value, dict) or set(value) != _MEMBERSHIP_FIELDS:
            raise ValueError("clock membership schema is invalid")
        row = ClockMembership(
            _text(value, "observation_id"),
            _text(value, "session_id"),
            _integer(value, "start_sequence"),
            _integer(value, "next_sequence"),
        )
        if row.start_sequence < 0 or row.next_sequence <= row.start_sequence or path.name != _membership_name(row):
            raise ValueError("clock membership values are invalid")
        return row
    except (OSError, UnicodeDecodeError, json.JSONDecodeError, TypeError, ValueError) as error:
        raise LegacyClockImportError("clock membership ledger is invalid") from error


def _json_files(root: Path) -> tuple[Path, ...]:
    try:
        return tuple(sorted(root.glob("*.json"))) if root.exists() else ()
    except OSError as error:
        raise LegacyClockImportError("legacy clock directory cannot be read") from error


def _membership_name(row: ClockMembership) -> str:
    return f"{row.start_sequence:020d}-{row.next_sequence:020d}-{row.observation_id}.json"


def _integer(value: dict[str, object], key: str) -> int:
    item = value.get(key)
    if isinstance(item, bool) or not isinstance(item, int):
        raise ValueError(f"{key} is not an integer")
    return item


def _text(value: dict[str, object], key: str) -> str:
    item = value.get(key)
    if not isinstance(item, str) or not item:
        raise ValueError(f"{key} is not text")
    return item


def _number(value: dict[str, object], key: str) -> float:
    item = value.get(key)
    if isinstance(item, bool) or not isinstance(item, int | float) or not math.isfinite(float(item)):
        raise ValueError(f"{key} is not finite numeric data")
    return float(item)


def _canonical_observation(value: object) -> bytes:
    return (json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":"), allow_nan=False) + "\n").encode(
        "utf-8"
    )
