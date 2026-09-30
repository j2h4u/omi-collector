"""UTC estimates for sequence ranges whose clock epoch is already confirmed."""

from __future__ import annotations

import math
from dataclasses import dataclass
from itertools import pairwise

from .clock_observations import ClockObservation

_RTC_QUANTIZATION_UNCERTAINTY_SECONDS = 1.0


class ClockSegmentError(ValueError):
    """A proposed clock segment is ambiguous or invalid."""


@dataclass(frozen=True, slots=True)
class ClockEpochMembership:
    """An explicit decision that a sequence range belongs to one RTC epoch."""

    observation_id: str
    start_sequence: int
    next_sequence: int

    def __post_init__(self) -> None:
        if not self.observation_id or self.start_sequence < 0 or self.next_sequence <= self.start_sequence:
            raise ClockSegmentError("clock epoch membership is invalid")


@dataclass(frozen=True, slots=True)
class ClockSegment:
    """One sequence range with a durable RTC anchor."""

    observation_id: str
    start_sequence: int
    next_sequence: int
    utc_offset_seconds: float
    uncertainty_seconds: float
    confidence: str = "confirmed"

    @classmethod
    def from_confirmed_membership(cls, observation: ClockObservation, membership: ClockEpochMembership) -> ClockSegment:
        """Anchor an explicitly confirmed native epoch to its RTC-read midpoint."""
        midpoint = (observation.host_realtime_start + observation.host_realtime_end) / 2.0
        if observation.evidence_kind != "native_trusted" or membership.observation_id != observation.observation_id:
            raise ClockSegmentError("clock membership does not have a native RTC anchor")
        return cls(
            observation.observation_id,
            membership.start_sequence,
            membership.next_sequence,
            midpoint - observation.device_epoch,
            (observation.host_realtime_end - observation.host_realtime_start) / 2.0
            + _RTC_QUANTIZATION_UNCERTAINTY_SECONDS,
        )

    @classmethod
    def from_approximate_observation(
        cls, observation: ClockObservation, start_sequence: int, next_sequence: int
    ) -> ClockSegment:
        """Estimate an unconfirmed range from an INFO read and its host-time midpoint."""
        if observation.evidence_kind != "native_trusted":
            raise ClockSegmentError("approximate clock range has no native RTC anchor")
        return cls(
            observation.observation_id,
            start_sequence,
            next_sequence,
            (observation.host_realtime_start + observation.host_realtime_end) / 2.0 - observation.device_epoch,
            (observation.host_realtime_end - observation.host_realtime_start) / 2.0
            + _RTC_QUANTIZATION_UNCERTAINTY_SECONDS,
            "approximate",
        )

    def __post_init__(self) -> None:
        valid = (
            bool(self.observation_id),
            self.start_sequence >= 0,
            self.next_sequence > self.start_sequence,
            math.isfinite(self.utc_offset_seconds),
            math.isfinite(self.uncertainty_seconds),
            self.uncertainty_seconds >= 0,
            self.confidence in {"confirmed", "approximate"},
        )
        if not all(valid):
            raise ClockSegmentError("clock segment is invalid")

    def utc_for(self, sequence: int, raw_timestamp: int) -> float | None:
        """Return no UTC outside this confirmed epoch."""
        if not self.start_sequence <= sequence < self.next_sequence:
            return None
        return raw_timestamp + self.utc_offset_seconds


@dataclass(frozen=True, slots=True)
class ClockSegmentMap:
    """Non-overlapping confirmed segments; all uncovered records remain unknown."""

    segments: tuple[ClockSegment, ...]

    def __post_init__(self) -> None:
        ordered = tuple(sorted(self.segments, key=lambda item: item.start_sequence))
        if ordered != self.segments or any(
            left.next_sequence > right.start_sequence for left, right in pairwise(ordered)
        ):
            raise ClockSegmentError("clock segments overlap or are unordered")

    def utc_for(self, sequence: int, raw_timestamp: int) -> float | None:
        """Return UTC only when one confirmed segment owns the record."""
        for segment in self.segments:
            utc = segment.utc_for(sequence, raw_timestamp)
            if utc is not None:
                return utc
        return None


def segments_with_estimates(
    observations: tuple[ClockObservation, ...],
    confirmed: ClockSegmentMap,
    verified_operations: frozenset[str] = frozenset(),
) -> ClockSegmentMap:
    """Use the latest applicable native read for uncovered ranges, preserving confirmed ranges."""
    by_id = {item.observation_id: item for item in observations}
    boundaries = _verified_boundaries(observations, by_id, verified_operations)
    estimates = _approximate_segments(observations, by_id, boundaries)
    if not estimates:
        return confirmed
    segments: list[ClockSegment] = []
    for _order, estimate in sorted(estimates, key=lambda item: item[0]):
        segments = _overlay(segments, estimate)
    for segment in confirmed.segments:
        segments = _overlay(segments, segment)
    return ClockSegmentMap(tuple(sorted(segments, key=lambda item: item.start_sequence)))


def _approximate_segments(
    observations: tuple[ClockObservation, ...],
    by_id: dict[str, ClockObservation],
    boundaries: tuple[tuple[int, int], ...],
) -> tuple[tuple[int, ClockSegment], ...]:
    estimates: list[tuple[int, ClockSegment]] = []
    for observation in observations:
        if observation.evidence_kind != "native_trusted":
            continue
        bounds = _approximate_bounds(observation, observations, by_id, boundaries)
        if bounds is not None:
            estimates.append(
                (observation.causal_order, ClockSegment.from_approximate_observation(observation, *bounds))
            )
    return tuple(estimates)


def _approximate_bounds(
    observation: ClockObservation,
    observations: tuple[ClockObservation, ...],
    by_id: dict[str, ClockObservation],
    boundaries: tuple[tuple[int, int], ...],
) -> tuple[int, int] | None:
    start, end = observation.info_sequence_min, observation.info_sequence_max
    if observation.observation_role == "initial":
        later = next(
            (
                item
                for item in observations
                if item.operation_id == observation.operation_id
                and item.observation_role == "later"
                and item.parent_observation_id == observation.observation_id
                and item.effective_boundary_sequence is not None
            ),
            None,
        )
        if later is not None and later.effective_boundary_sequence is not None:
            end = min(end, later.effective_boundary_sequence)
    elif observation.observation_role == "later" and observation.parent_observation_id:
        parent = by_id.get(observation.parent_observation_id)
        if parent is not None:
            start = (
                observation.effective_boundary_sequence
                if observation.effective_boundary_sequence is not None
                else max(start, parent.info_sequence_max)
            )
    for boundary_order, boundary in boundaries:
        if boundary_order < observation.causal_order and start < boundary:
            if end <= boundary:
                return None
            start = max(start, boundary)
    return (start, end) if end > start else None


def _verified_boundaries(
    observations: tuple[ClockObservation, ...],
    by_id: dict[str, ClockObservation],
    verified_operations: frozenset[str],
) -> tuple[tuple[int, int], ...]:
    return tuple(
        (item.causal_order, item.effective_boundary_sequence)
        for item in observations
        if item.operation_id in verified_operations
        and item.observation_role == "later"
        and item.parent_observation_id in by_id
        and by_id[item.parent_observation_id].observation_role == "initial"
        and item.effective_boundary_sequence is not None
    )


def _overlay(segments: list[ClockSegment], incoming: ClockSegment) -> list[ClockSegment]:
    output = []
    for item in segments:
        if item.next_sequence <= incoming.start_sequence or item.start_sequence >= incoming.next_sequence:
            output.append(item)
            continue
        if item.start_sequence < incoming.start_sequence:
            output.append(_slice(item, item.start_sequence, incoming.start_sequence))
        if item.next_sequence > incoming.next_sequence:
            output.append(_slice(item, incoming.next_sequence, item.next_sequence))
    output.append(incoming)
    return output


def _slice(segment: ClockSegment, start: int, end: int) -> ClockSegment:
    return ClockSegment(
        segment.observation_id,
        start,
        end,
        segment.utc_offset_seconds,
        segment.uncertainty_seconds,
        segment.confidence,
    )
