"""Durable, same-session clock epoch memberships."""

from __future__ import annotations

import json
import os
from dataclasses import asdict, dataclass
from itertools import pairwise
from pathlib import Path
from typing import cast
from uuid import uuid4

from .clock_observations import ClockObservation
from .clock_segments import ClockEpochMembership, ClockSegment, ClockSegmentMap


class ClockMembershipError(ValueError):
    """A clock membership is malformed or conflicts with durable evidence."""


@dataclass(frozen=True, slots=True)
class ClockMembership:
    """One interval confirmed by two successful INFO reads in one BLE session."""

    observation_id: str
    session_id: str
    start_sequence: int
    next_sequence: int

    def __post_init__(self) -> None:
        if (
            not self.observation_id
            or not self.session_id
            or self.start_sequence < 0
            or self.next_sequence <= self.start_sequence
        ):
            raise ClockMembershipError("clock membership is invalid")


class ClockMembershipStore:
    """Append-only membership decisions, written before ready publication."""

    def __init__(self, device_state_path: Path) -> None:
        self._root = Path(device_state_path).parent / "clock-memberships"

    def record(self, membership: ClockMembership) -> ClockMembership:
        self._root.mkdir(mode=0o750, parents=True, exist_ok=True)
        path = self._root / _name(membership)
        payload = _canonical(asdict(membership))
        if path.exists():
            if path.read_bytes() != payload:
                raise ClockMembershipError("clock membership conflicts with durable evidence")
            return membership
        temporary = path.with_name(f".{path.name}.{uuid4().hex}.tmp")
        try:
            with temporary.open("xb") as stream:
                stream.write(payload)
                stream.flush()
                os.fsync(stream.fileno())
            temporary.replace(path)
            _sync_directory(self._root)
        finally:
            temporary.unlink(missing_ok=True)
        return membership

    def record_membership(self, observation_id: str, session_id: str, start_sequence: int, next_sequence: int) -> None:
        self.record(ClockMembership(observation_id, session_id, start_sequence, next_sequence))

    def records(self) -> tuple[ClockMembership, ...]:
        if not self._root.exists():
            return ()
        rows = tuple(_read(path) for path in sorted(self._root.glob("*.json")))
        if any(left.next_sequence > right.start_sequence for left, right in pairwise(rows)):
            raise ClockMembershipError("clock memberships overlap")
        return rows

    def segments(self, observations: tuple[ClockObservation, ...]) -> ClockSegmentMap:
        by_id = {item.observation_id: item for item in observations}
        segments: list[ClockSegment] = []
        for membership in self.records():
            observation = by_id.get(membership.observation_id)
            if observation is None or observation.session_id != membership.session_id:
                raise ClockMembershipError("clock membership has no durable observation")
            segments.append(
                ClockSegment.from_confirmed_membership(
                    observation,
                    ClockEpochMembership(
                        membership.observation_id, membership.start_sequence, membership.next_sequence
                    ),
                )
            )
        return ClockSegmentMap(tuple(segments))


def _name(membership: ClockMembership) -> str:
    return f"{membership.start_sequence:020d}-{membership.next_sequence:020d}-{membership.observation_id}.json"


def _read(path: Path) -> ClockMembership:
    try:
        value = cast(object, json.loads(path.read_text(encoding="utf-8")))
        if not isinstance(value, dict) or set(value) != {
            "observation_id",
            "session_id",
            "start_sequence",
            "next_sequence",
        }:
            raise ValueError
        membership = ClockMembership(**value)
        if path.name != _name(membership):
            raise ValueError
        return membership
    except (OSError, TypeError, ValueError, json.JSONDecodeError) as error:
        raise ClockMembershipError("clock membership ledger is invalid") from error


def _canonical(value: object) -> bytes:
    return json.dumps(value, separators=(",", ":"), sort_keys=True, allow_nan=False).encode()


def _sync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
