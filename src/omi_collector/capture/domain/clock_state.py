"""One durable snapshot of clock ranges that can still normalize audio."""

from __future__ import annotations

import math
from dataclasses import asdict, dataclass, replace
from typing import cast

from .ring_protocol import RingInfo

_SCHEMA_VERSION = 1
_CONFIDENCE = frozenset({"confirmed", "approximate", "unknown"})
_ANCHOR_CONFIDENCE = frozenset({"confirmed", "approximate"})


class ClockStateError(ValueError):
    """Clock state is malformed or could not be made durable."""


@dataclass(frozen=True, slots=True)
class ClockRange:
    start_sequence: int
    next_sequence: int
    offset_seconds: float | None
    uncertainty_seconds: float | None
    confidence: str
    source_id: str

    def __post_init__(self) -> None:
        numeric_offset = self.offset_seconds is None or (
            isinstance(self.offset_seconds, (int, float))
            and not isinstance(self.offset_seconds, bool)
            and math.isfinite(self.offset_seconds)
        )
        numeric_uncertainty = self.uncertainty_seconds is None or (
            isinstance(self.uncertainty_seconds, (int, float))
            and not isinstance(self.uncertainty_seconds, bool)
            and math.isfinite(self.uncertainty_seconds)
        )
        valid = (
            type(self.start_sequence) is int
            and type(self.next_sequence) is int
            and self.start_sequence >= 0
            and self.next_sequence > self.start_sequence
            and isinstance(self.confidence, str)
            and self.confidence in _CONFIDENCE
            and isinstance(self.source_id, str)
            and bool(self.source_id)
            and numeric_offset
            and numeric_uncertainty
            and (self.uncertainty_seconds is None or self.uncertainty_seconds >= 0)
            and ((self.offset_seconds is None) == (self.confidence == "unknown"))
            and ((self.offset_seconds is None) == (self.uncertainty_seconds is None))
        )
        if not valid:
            raise ClockStateError("clock range is invalid")


@dataclass(frozen=True, slots=True)
class ClockIntent:
    operation_id: str
    phase: str
    observed_epoch: int
    target_epoch: int
    host_midpoint: float
    write_sequence_before: int
    write_sequence_after: int | None = None

    def __post_init__(self) -> None:
        valid = (
            isinstance(self.operation_id, str)
            and bool(self.operation_id)
            and isinstance(self.phase, str)
            and self.phase in {"prepared", "uncertain"}
            and type(self.observed_epoch) is int
            and type(self.target_epoch) is int
            and 0 <= self.observed_epoch <= 2**32 - 1
            and 0 <= self.target_epoch <= 2**32 - 1
            and isinstance(self.host_midpoint, (int, float))
            and not isinstance(self.host_midpoint, bool)
            and math.isfinite(self.host_midpoint)
            and type(self.write_sequence_before) is int
            and self.write_sequence_before >= 0
            and (
                self.write_sequence_after is None
                or (type(self.write_sequence_after) is int and self.write_sequence_after >= self.write_sequence_before)
            )
        )
        if not valid:
            raise ClockStateError("clock intent is invalid")


@dataclass(frozen=True, slots=True)
class ClockAnchor:
    sequence_frontier: int
    offset_seconds: float
    uncertainty_seconds: float
    confidence: str
    source_id: str

    def __post_init__(self) -> None:
        valid = (
            _valid_anchor_frontier(self.sequence_frontier)
            and _finite_number(self.offset_seconds)
            and _nonnegative_finite_number(self.uncertainty_seconds)
            and _valid_anchor_identity(self.confidence, self.source_id)
        )
        if not valid:
            raise ClockStateError("clock anchor is invalid")


def _fields(value: dict[object, object], required: set[str], optional: frozenset[str] | set[str] = frozenset()) -> None:
    if not required <= value.keys() or value.keys() - required - optional:
        raise ValueError("clock state fields are invalid")


def _int_field(value: object) -> int:
    if not isinstance(value, int):
        raise ValueError("clock state integer is invalid")
    return value


def _number_field(value: object) -> float:
    if not isinstance(value, (int, float)):
        raise ValueError("clock state number is invalid")
    return value


def _text_field(value: object) -> str:
    if not isinstance(value, str):
        raise ValueError("clock state text is invalid")
    return value


def _parse_range(value: dict[object, object]) -> ClockRange:
    _fields(
        value, {"start_sequence", "next_sequence", "offset_seconds", "uncertainty_seconds", "confidence", "source_id"}
    )
    offset, uncertainty = value["offset_seconds"], value["uncertainty_seconds"]
    return ClockRange(
        _int_field(value["start_sequence"]),
        _int_field(value["next_sequence"]),
        None if offset is None else _number_field(offset),
        None if uncertainty is None else _number_field(uncertainty),
        _text_field(value["confidence"]),
        _text_field(value["source_id"]),
    )


def _parse_intent(value: dict[object, object]) -> ClockIntent:
    _fields(
        value,
        {"operation_id", "phase", "observed_epoch", "target_epoch", "host_midpoint", "write_sequence_before"},
        {"write_sequence_after"},
    )
    after = value.get("write_sequence_after")
    return ClockIntent(
        _text_field(value["operation_id"]),
        _text_field(value["phase"]),
        _int_field(value["observed_epoch"]),
        _int_field(value["target_epoch"]),
        _number_field(value["host_midpoint"]),
        _int_field(value["write_sequence_before"]),
        None if after is None else _int_field(after),
    )


def _parse_anchor(value: dict[object, object]) -> ClockAnchor:
    _fields(value, {"sequence_frontier", "offset_seconds", "uncertainty_seconds", "confidence", "source_id"})
    return ClockAnchor(
        _int_field(value["sequence_frontier"]),
        _number_field(value["offset_seconds"]),
        _number_field(value["uncertainty_seconds"]),
        _text_field(value["confidence"]),
        _text_field(value["source_id"]),
    )


def _valid_anchor_frontier(value: object) -> bool:
    return type(value) is int and value >= 0


def _finite_number(value: object) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value)


def _nonnegative_finite_number(value: object) -> bool:
    return _finite_number(value) and cast(int | float, value) >= 0


def _valid_anchor_identity(confidence: object, source_id: object) -> bool:
    return (
        isinstance(confidence, str)
        and confidence in _ANCHOR_CONFIDENCE
        and isinstance(source_id, str)
        and bool(source_id)
    )


def _valid_causal_interval(item: object) -> bool:
    if not isinstance(item, tuple):
        return False
    values = cast(tuple[object, ...], item)
    match values:
        case (start, end):
            return type(start) is int and type(end) is int and 0 <= start <= end
        case _:
            return False


@dataclass(frozen=True, slots=True)
class ClockState:
    ranges: tuple[ClockRange, ...] = ()
    intent: ClockIntent | None = None
    anchor: ClockAnchor | None = None
    causal_uncertainty: tuple[tuple[int, int], ...] = ()

    def __post_init__(self) -> None:
        if (
            not isinstance(self.ranges, tuple)
            or any(not isinstance(item, ClockRange) for item in self.ranges)
            or any(
                left.next_sequence > right.start_sequence
                for left, right in zip(self.ranges, self.ranges[1:], strict=False)
            )
        ):
            raise ClockStateError("clock ranges overlap or are unordered")
        if self.intent is not None and not isinstance(self.intent, ClockIntent):
            raise ClockStateError("clock intent is invalid")
        if self.anchor is not None and not isinstance(self.anchor, ClockAnchor):
            raise ClockStateError("clock anchor is invalid")
        if not isinstance(self.causal_uncertainty, tuple) or any(
            not _valid_causal_interval(item) for item in self.causal_uncertainty
        ):
            raise ClockStateError("clock causal uncertainty is invalid")

    def add_range(self, incoming: ClockRange) -> ClockState:
        """Add evidence only to uncovered sequences, keeping prior audio timing fixed."""
        ranges = list(self.ranges)
        uncovered = [(incoming.start_sequence, incoming.next_sequence)]
        for existing in self.ranges:
            next_uncovered: list[tuple[int, int]] = []
            for start, end in uncovered:
                if existing.next_sequence <= start or existing.start_sequence >= end:
                    next_uncovered.append((start, end))
                else:
                    if start < existing.start_sequence:
                        next_uncovered.append((start, existing.start_sequence))
                    if end > existing.next_sequence:
                        next_uncovered.append((existing.next_sequence, end))
            uncovered = next_uncovered
        ranges.extend(
            replace(incoming, start_sequence=start, next_sequence=end) for start, end in uncovered if end > start
        )
        ranges.sort(key=lambda item: item.start_sequence)
        merged: list[ClockRange] = []
        for item in ranges:
            if (
                merged
                and merged[-1].next_sequence == item.start_sequence
                and replace(merged[-1], start_sequence=item.start_sequence, next_sequence=item.next_sequence) == item
            ):
                merged[-1] = replace(merged[-1], next_sequence=item.next_sequence)
            else:
                merged.append(item)
        return ClockState(tuple(merged), self.intent, self.anchor, self.causal_uncertainty)

    def observe(
        self,
        info: RingInfo,
        *,
        offset_seconds: float,
        uncertainty_seconds: float,
        source_id: str,
        tolerance_seconds: float,
    ) -> ClockState:
        """Calculate the next clock state from one trusted observation."""
        state = self
        anchor = state.anchor
        read_sequence, write_sequence = info.read_sequence, info.write_sequence
        if anchor is None and read_sequence < write_sequence:
            state = state.add_range(ClockRange(read_sequence, write_sequence, None, None, "unknown", source_id))
        elif anchor is not None and write_sequence > anchor.sequence_frontier:
            continuous = state.intent is None and abs(offset_seconds - anchor.offset_seconds) <= tolerance_seconds
            state = state.add_range(
                ClockRange(
                    anchor.sequence_frontier,
                    write_sequence,
                    anchor.offset_seconds if continuous else None,
                    anchor.uncertainty_seconds + abs(offset_seconds - anchor.offset_seconds) if continuous else None,
                    "approximate" if continuous else "unknown",
                    anchor.source_id,
                )
            )
        next_anchor = ClockAnchor(write_sequence, offset_seconds, uncertainty_seconds, "approximate", source_id)
        if anchor != next_anchor and (
            anchor is None
            or write_sequence != anchor.sequence_frontier
            or abs(offset_seconds - anchor.offset_seconds) > tolerance_seconds
        ):
            state = replace(state, anchor=next_anchor)
        return state

    def to_payload(self) -> dict[str, object]:
        payload: dict[str, object] = {
            "schema_version": _SCHEMA_VERSION,
            "ranges": [asdict(item) for item in self.ranges],
            "intent": asdict(self.intent) if self.intent is not None else None,
            "anchor": asdict(self.anchor) if self.anchor is not None else None,
        }
        if self.causal_uncertainty:
            payload["causal_uncertainty"] = [list(item) for item in self.causal_uncertainty]
        return payload

    @classmethod
    def from_payload(cls, payload: dict[str, object]) -> ClockState:
        required = {"schema_version", "ranges", "intent", "anchor"}
        if (
            not isinstance(payload, dict)
            or not required <= set(payload)
            or set(payload) - required - {"causal_uncertainty"}
        ):
            raise ClockStateError("clock state payload is invalid")
        if type(payload["schema_version"]) is not int or payload["schema_version"] != _SCHEMA_VERSION:
            raise ClockStateError("clock state schema is unsupported")
        values = payload["ranges"]
        intent_value = payload["intent"]
        anchor_value = payload["anchor"]
        if (
            not isinstance(values, list)
            or (intent_value is not None and not isinstance(intent_value, dict))
            or (anchor_value is not None and not isinstance(anchor_value, dict))
        ):
            raise ClockStateError("clock state payload is invalid")
        try:
            values = cast(list[object], values)
            ranges = tuple(
                _parse_range(cast(dict[object, object], value)) for value in values if isinstance(value, dict)
            )
            if len(ranges) != len(values):
                raise ClockStateError("clock state range is invalid")
            intent = _parse_intent(cast(dict[object, object], intent_value)) if isinstance(intent_value, dict) else None
            anchor = _parse_anchor(cast(dict[object, object], anchor_value)) if isinstance(anchor_value, dict) else None
            uncertainty = payload.get("causal_uncertainty", [])
            if not isinstance(uncertainty, list):
                raise ClockStateError("clock causal uncertainty is invalid")
            items = cast(list[object], uncertainty)
            if any(not isinstance(item, list) for item in items):
                raise ClockStateError("clock causal uncertainty is invalid")
            lists = cast(list[list[object]], items)
            intervals = cast(tuple[tuple[int, int], ...], tuple(tuple(item) for item in lists))
            return cls(ranges, intent, anchor, intervals)
        except (TypeError, ValueError) as error:
            raise ClockStateError("clock state payload is invalid") from error

    def begin_intent(self, intent: ClockIntent) -> ClockState:
        if self.intent is not None:
            raise ClockStateError("clock write is already pending")
        return replace(self, intent=intent)

    def with_anchor(self, anchor: ClockAnchor) -> ClockState:
        return replace(self, anchor=anchor)

    def mark_uncertain(self, operation_id: str, *, write_sequence_after: int | None = None) -> ClockState:
        if self.intent is None or self.intent.operation_id != operation_id:
            raise ClockStateError("clock intent transition is invalid")
        if self.intent.phase == "uncertain":
            if self.intent.write_sequence_after is None:
                return replace(
                    self,
                    intent=replace(self.intent, write_sequence_after=write_sequence_after),
                )
            if self.intent.write_sequence_after == write_sequence_after:
                return self
            raise ClockStateError("clock intent transition is invalid")
        if self.intent.phase != "prepared":
            raise ClockStateError("clock intent transition is invalid")
        return replace(
            self,
            intent=replace(self.intent, phase="uncertain", write_sequence_after=write_sequence_after),
        )

    def cancel_prepared_intent(self, operation_id: str) -> ClockState:
        if self.intent is None or self.intent.operation_id != operation_id or self.intent.phase != "prepared":
            raise ClockStateError("clock intent transition is invalid")
        return replace(self, intent=None)

    def finish_intent(self, operation_id: str, ranges: tuple[ClockRange, ...]) -> ClockState:
        if self.intent is None or self.intent.operation_id != operation_id or self.intent.phase != "uncertain":
            raise ClockStateError("clock intent transition is invalid")
        if not isinstance(ranges, tuple) or any(not isinstance(item, ClockRange) for item in ranges):
            raise ClockStateError("clock intent ranges are invalid")
        boundary = self.intent.write_sequence_after
        if boundary is None:
            raise ClockStateError("clock write boundary is unresolved")
        if boundary > self.intent.write_sequence_before:
            _require_unknown_coverage(self.intent.write_sequence_before, boundary, ranges)
        completed = self
        for item in ranges:
            completed = completed.add_range(item)
        if boundary > self.intent.write_sequence_before:
            _require_unknown_coverage(self.intent.write_sequence_before, boundary, completed.ranges)
        return replace(
            completed,
            intent=None,
            causal_uncertainty=tuple(
                sorted({*completed.causal_uncertainty, (self.intent.write_sequence_before, boundary)})
            ),
        )


def _require_unknown_coverage(start: int, end: int, ranges: tuple[ClockRange, ...]) -> None:
    cursor = start
    for item in sorted(ranges, key=lambda candidate: candidate.start_sequence):
        if item.next_sequence <= cursor:
            continue
        if item.start_sequence > cursor:
            raise ClockStateError("clock write boundary coverage is incomplete")
        if item.offset_seconds is not None:
            raise ClockStateError("clock write boundary must remain unknown")
        cursor = min(end, item.next_sequence)
        if cursor == end:
            return
    raise ClockStateError("clock write boundary coverage is incomplete")
