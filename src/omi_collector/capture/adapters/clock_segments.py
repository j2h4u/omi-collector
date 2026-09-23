"""UTC estimates for sequence ranges whose clock epoch is already confirmed."""

from __future__ import annotations

import math
from dataclasses import dataclass
from itertools import pairwise

from .clock_observations import ClockObservation


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
    """One confirmed raw-clock epoch, anchored by a durable RTC observation."""

    observation_id: str
    start_sequence: int
    next_sequence: int
    utc_offset_seconds: float
    uncertainty_seconds: float

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
            (observation.host_realtime_end - observation.host_realtime_start) / 2.0,
        )

    def __post_init__(self) -> None:
        valid = (
            bool(self.observation_id),
            self.start_sequence >= 0,
            self.next_sequence > self.start_sequence,
            math.isfinite(self.utc_offset_seconds),
            math.isfinite(self.uncertainty_seconds),
            self.uncertainty_seconds >= 0,
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
