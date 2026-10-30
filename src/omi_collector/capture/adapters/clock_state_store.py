"""Durable active clock ranges and one outstanding SET_TIME intent."""

from __future__ import annotations

from ..domain.clock_state import ClockIntent, ClockRange, ClockState
from ..domain.ring_protocol import RingInfo
from .clock_segments import ClockSegmentMap
from .collector_state_db import StateDB, StateKind

_CLOCK_ID = "current"


class ClockStateStore:
    """Keep a bounded clock projection in the shared Collector database."""

    def __init__(self, db: StateDB) -> None:
        self.db = db

    def state(self) -> ClockState:
        payload = self.db.get(StateKind.CLOCK, _CLOCK_ID)
        return ClockState.from_payload(payload) if payload is not None else ClockState()

    def segment_map(self) -> ClockSegmentMap:
        return ClockSegmentMap.from_state(self.state())

    def observe(
        self,
        info: RingInfo,
        *,
        offset_seconds: float,
        uncertainty_seconds: float,
        source_id: str,
        tolerance_seconds: float,
    ) -> ClockState:
        """Map new sequences only when successive trusted anchors agree."""
        with self.db.transaction() as tx:
            payload = tx.get(StateKind.CLOCK, _CLOCK_ID)
            state = ClockState.from_payload(payload) if payload is not None else ClockState()
            state = state.observe(
                info,
                offset_seconds=offset_seconds,
                uncertainty_seconds=uncertainty_seconds,
                source_id=source_id,
                tolerance_seconds=tolerance_seconds,
            )
            if state.to_payload() != payload:
                tx.put(StateKind.CLOCK, _CLOCK_ID, state.to_payload())
            return state

    def prepare(self, intent: ClockIntent) -> ClockState:
        with self.db.transaction() as tx:
            payload = tx.get(StateKind.CLOCK, _CLOCK_ID)
            state = ClockState.from_payload(payload) if payload is not None else ClockState()
            state = state.begin_intent(intent)
            tx.put(StateKind.CLOCK, _CLOCK_ID, state.to_payload())
            return state

    def mark_uncertain(self, operation_id: str, *, write_sequence_after: int | None = None) -> ClockState:
        with self.db.transaction() as tx:
            payload = tx.get(StateKind.CLOCK, _CLOCK_ID)
            if payload is None:
                raise ValueError("clock intent is missing")
            state = ClockState.from_payload(payload).mark_uncertain(
                operation_id, write_sequence_after=write_sequence_after
            )
            tx.put(StateKind.CLOCK, _CLOCK_ID, state.to_payload())
            return state

    def cancel_prepared(self, operation_id: str) -> ClockState:
        with self.db.transaction() as tx:
            payload = tx.get(StateKind.CLOCK, _CLOCK_ID)
            if payload is None:
                raise ValueError("clock intent is missing")
            state = ClockState.from_payload(payload).cancel_prepared_intent(operation_id)
            tx.put(StateKind.CLOCK, _CLOCK_ID, state.to_payload())
            return state

    def finish(self, operation_id: str, ranges: tuple[ClockRange, ...]) -> ClockState:
        with self.db.transaction() as tx:
            payload = tx.get(StateKind.CLOCK, _CLOCK_ID)
            if payload is None:
                raise ValueError("clock intent is missing")
            state = ClockState.from_payload(payload).finish_intent(operation_id, ranges)
            tx.put(StateKind.CLOCK, _CLOCK_ID, state.to_payload())
            return state
