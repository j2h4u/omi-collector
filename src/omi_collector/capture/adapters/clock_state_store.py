"""Durable active clock ranges and one outstanding SET_TIME intent."""

from __future__ import annotations

from dataclasses import replace

from ..domain.clock_state import ClockIntent, ClockRange, ClockState
from ..domain.ring_protocol import RingInfo
from .clock_segments import ClockSegmentMap
from .collector_state_db import StateDB, StateKind, StateTransaction

_CLOCK_ID = "current"
_CAUSAL_MIGRATION = "clock-causal-uncertainty-v1"


class ClockStateStore:
    """Keep a bounded clock projection in the shared Collector database."""

    def __init__(self, db: StateDB) -> None:
        self.db = db

    def state(self) -> ClockState:
        migrated = self.db.get(StateKind.MIGRATION, _CAUSAL_MIGRATION) is not None
        payload = self.db.get(StateKind.CLOCK, _CLOCK_ID)
        state = ClockState.from_payload(payload) if payload is not None else ClockState()
        return state if migrated else _annotate_unknown(state)

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
            state = _load_current(tx)
            before = state.to_payload()
            state = state.observe(
                info,
                offset_seconds=offset_seconds,
                uncertainty_seconds=uncertainty_seconds,
                source_id=source_id,
                tolerance_seconds=tolerance_seconds,
            )
            if state.to_payload() != before:
                tx.put(StateKind.CLOCK, _CLOCK_ID, state.to_payload())
            return state

    def prepare(self, intent: ClockIntent) -> ClockState:
        with self.db.transaction() as tx:
            state = _load_current(tx).begin_intent(intent)
            tx.put(StateKind.CLOCK, _CLOCK_ID, state.to_payload())
            return state

    def mark_uncertain(self, operation_id: str, *, write_sequence_after: int | None = None) -> ClockState:
        with self.db.transaction() as tx:
            state = _load_current(tx).mark_uncertain(operation_id, write_sequence_after=write_sequence_after)
            tx.put(StateKind.CLOCK, _CLOCK_ID, state.to_payload())
            return state

    def cancel_prepared(self, operation_id: str) -> ClockState:
        with self.db.transaction() as tx:
            state = _load_current(tx).cancel_prepared_intent(operation_id)
            tx.put(StateKind.CLOCK, _CLOCK_ID, state.to_payload())
            return state

    def finish(self, operation_id: str, ranges: tuple[ClockRange, ...]) -> ClockState:
        with self.db.transaction() as tx:
            state = _load_current(tx).finish_intent(operation_id, ranges)
            tx.put(StateKind.CLOCK, _CLOCK_ID, state.to_payload())
            return state


def _load_current(tx: StateTransaction) -> ClockState:
    payload = tx.get(StateKind.CLOCK, _CLOCK_ID)
    state = ClockState.from_payload(payload) if payload is not None else ClockState()
    if tx.get(StateKind.MIGRATION, _CAUSAL_MIGRATION) is None:
        state = _annotate_unknown(state)
        if payload is not None:
            tx.put(StateKind.CLOCK, _CLOCK_ID, state.to_payload())
        tx.put(StateKind.MIGRATION, _CAUSAL_MIGRATION, {"complete": True})
    return state


def _annotate_unknown(state: ClockState) -> ClockState:
    uncertain = tuple((item.start_sequence, item.next_sequence) for item in state.ranges if item.offset_seconds is None)
    return replace(state, causal_uncertainty=tuple(sorted({*state.causal_uncertainty, *uncertain})))
