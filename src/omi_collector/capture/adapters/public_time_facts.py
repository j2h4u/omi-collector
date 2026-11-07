"""Freeze one physical record's public time from durable clock evidence."""

from __future__ import annotations

import math
from bisect import bisect_right
from dataclasses import dataclass

from ..domain.clock_state import ClockState
from ..domain.public_audio_v2 import FrozenPublicTime, Quality

_UINT32_MAX = (1 << 32) - 1
_UINT64_MAX = (1 << 64) - 1


@dataclass(frozen=True, slots=True)
class FrozenTimePredecessor:
    """The immediately preceding physical record and its already-frozen time."""

    sequence: int
    time: FrozenPublicTime

    def __post_init__(self) -> None:
        _uint(self.sequence, _UINT64_MAX, "predecessor sequence")
        if not isinstance(self.time, FrozenPublicTime):
            raise TypeError("predecessor time must be FrozenPublicTime")


@dataclass(frozen=True, slots=True)
class PublicTimeProjection:
    """Prepared lookup for one immutable clock evidence snapshot."""

    state: ClockState
    range_starts: tuple[int, ...]
    components: tuple[tuple[int, int], ...]
    component_starts: tuple[int, ...]

    @classmethod
    def from_state(cls, state: ClockState) -> PublicTimeProjection:
        if not isinstance(state, ClockState):
            raise TypeError("state must be ClockState")
        components: list[tuple[int, int]] = []
        boundaries = _causal_boundaries(state)
        for start, end in _mapped_intervals(state):
            if components and components[-1][1] == start and start not in boundaries:
                components[-1] = (components[-1][0], end)
            else:
                components.append((start, end))
        frozen_components = tuple(components)
        return cls(
            state,
            tuple(item.start_sequence for item in state.ranges),
            frozen_components,
            tuple(start for start, _ in frozen_components),
        )

    def freeze(
        self, *, sequence: int, raw_timestamp: int, predecessor: FrozenTimePredecessor | None = None
    ) -> FrozenPublicTime:
        """Freeze one label and its causal comparability before publication Prepare."""
        _uint(sequence, _UINT64_MAX, "sequence")
        _uint(raw_timestamp, _UINT32_MAX, "raw_timestamp")
        if predecessor is not None and not isinstance(predecessor, FrozenTimePredecessor):
            raise TypeError("predecessor must be FrozenTimePredecessor")
        quality, seconds, uncertainty, membership = _public_label(self, sequence, raw_timestamp)
        comparable = (
            predecessor is not None
            and predecessor.sequence + 1 == sequence
            and _same_causal_component(self, sequence, membership, predecessor)
        )
        return FrozenPublicTime(quality, seconds, uncertainty, membership, comparable)


def _same_causal_component(
    projection: PublicTimeProjection,
    sequence: int,
    membership: str,
    predecessor: FrozenTimePredecessor,
) -> bool:
    if any(start <= sequence <= end for start, end in projection.state.causal_uncertainty):
        return False
    previous_membership = predecessor.time.causal_membership
    if previous_membership == membership:
        return True
    kind = membership.partition(":")[0]
    if kind != "mapped" or previous_membership.partition(":")[0] != kind:
        return False
    start, end = _mapped_component(projection, sequence)
    return start <= predecessor.sequence < end


def _public_label(
    projection: PublicTimeProjection, sequence: int, raw_timestamp: int
) -> tuple[Quality, int, float | None, str]:
    intent = projection.state.intent
    if (
        intent is not None
        and intent.phase == "uncertain"
        and sequence >= intent.write_sequence_before
        and (intent.write_sequence_after is None or sequence < intent.write_sequence_after)
    ):
        end = "open" if intent.write_sequence_after is None else str(intent.write_sequence_after)
        return (
            "unknown",
            raw_timestamp,
            None,
            f"unknown-intent:{intent.write_sequence_before}:{end}:{intent.operation_id}:{sequence}",
        )

    index = bisect_right(projection.range_starts, sequence) - 1
    clock_range = projection.state.ranges[index] if index >= 0 else None
    if clock_range is not None and sequence >= clock_range.next_sequence:
        clock_range = None
    if clock_range is None:
        return "unknown", raw_timestamp, None, f"unknown-uncovered:{sequence}"
    if clock_range.offset_seconds is None:
        return (
            "unknown",
            raw_timestamp,
            None,
            f"unknown-range:{clock_range.start_sequence}:{clock_range.next_sequence}:{clock_range.source_id}",
        )

    component = _mapped_component(projection, sequence)
    membership = f"mapped:{component[0]}:{component[1]}"
    normalized = math.floor(raw_timestamp + clock_range.offset_seconds + 0.5)
    if not 0 <= normalized <= _UINT32_MAX:
        offset = float(clock_range.offset_seconds).hex()
        return (
            "unknown",
            raw_timestamp,
            None,
            f"mapped-raw:{clock_range.start_sequence}:{clock_range.next_sequence}:{offset}",
        )
    quality = "approximate" if clock_range.confidence == "approximate" else "known"
    return quality, normalized, clock_range.uncertainty_seconds, membership


def _mapped_component(projection: PublicTimeProjection, sequence: int) -> tuple[int, int]:
    index = bisect_right(projection.component_starts, sequence) - 1
    if index >= 0:
        start, end = projection.components[index]
        if sequence < end:
            return start, end
    raise ValueError("mapped clock range has no effective causal interval")


def _causal_boundaries(state: ClockState) -> frozenset[int]:
    intent = state.intent
    if (
        intent is not None
        and intent.phase == "uncertain"
        and intent.write_sequence_after == intent.write_sequence_before
    ):
        return frozenset((intent.write_sequence_before,))
    return frozenset()


def _mapped_intervals(state: ClockState) -> tuple[tuple[int, int], ...]:
    uncertain = state.intent if state.intent is not None and state.intent.phase == "uncertain" else None
    result: list[tuple[int, int]] = []
    for item in state.ranges:
        if item.offset_seconds is None:
            continue
        if uncertain is None or item.next_sequence <= uncertain.write_sequence_before:
            result.append((item.start_sequence, item.next_sequence))
            continue
        uncertain_end = uncertain.write_sequence_after
        if uncertain_end is not None and item.start_sequence >= uncertain_end:
            result.append((item.start_sequence, item.next_sequence))
            continue
        if item.start_sequence < uncertain.write_sequence_before:
            result.append((item.start_sequence, uncertain.write_sequence_before))
        if uncertain_end is not None and uncertain_end < item.next_sequence:
            result.append((uncertain_end, item.next_sequence))
    return tuple(result)


def _uint(value: object, maximum: int, name: str) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or not 0 <= value <= maximum:
        raise ValueError(f"{name} must be an unsigned integer")
