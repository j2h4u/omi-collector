"""Bounded cleanup ordering contract checked against its runtime transition.

Run: uv run --no-sync pytest -q tests/test_fsm_cleanup_model.py --hypothesis-show-statistics
A cycle starts waiting, wakes into ACK, then publication, raw, expiry, vacuum,
and waiting. Advance while waiting is invalid; defer ends a running cycle.
Generated event sequences compare the runtime phase to an independent ordered
obligation list; each example resets both. Time advances by two cleanup intervals
per event, so every wake is due and every next cycle can eventually start. The
model checks phase order only: actual deletion, capture preemption, deadlines,
concurrency, fairness and process crashes require separate integration tests.
"""

import pytest
from hypothesis import event, given, settings
from hypothesis import strategies as st

from omi_collector.capture.application.quarantine_maintenance import (
    _cleanup_transition,
    _CleanupEvent,
    _CleanupPhase,
    _CleanupState,
)
from omi_collector.config import CLEANUP_INTERVAL_SECONDS


@settings(max_examples=40, deadline=None)
@given(st.lists(st.sampled_from(("wake", "advance", "defer")), min_size=1, max_size=15))
def test_generated_cleanup_phase_order(actions: list[str]) -> None:
    order = ("waiting", "ack", "publication", "raw", "expiry", "vacuum")
    position = 0
    state = _CleanupState(_CleanupPhase.WAITING, 0.0)
    for index, action in enumerate(actions):
        now = float(index * CLEANUP_INTERVAL_SECONDS * 2)
        prior = order[position]
        if action == "advance" and position == 0:
            with pytest.raises(ValueError):
                _cleanup_transition(state, _CleanupEvent.ADVANCE, now)
        else:
            if action == "wake" and position == 0:
                position = 1
            elif action == "advance":
                position = (position + 1) % len(order)
            elif action == "defer":
                position = 0
            state = _cleanup_transition(state, _CleanupEvent(action), now)
            assert state.phase.value == order[position]
        event(f"{prior}:{action}:{order[position]}")


def test_bounded_cleanup_reachability_and_guards() -> None:
    """Enumerate six-step traces; fair advance reaches the next waiting cycle."""
    from itertools import product

    visited: set[tuple[str, str, str]] = set()
    for actions in product(_CleanupEvent, repeat=6):
        state = _CleanupState(_CleanupPhase.WAITING, 10.0)
        for index, action in enumerate(actions):
            now = 10.0 + index * CLEANUP_INTERVAL_SECONDS * 2
            before = state.phase.value
            if state.phase is _CleanupPhase.WAITING and action is _CleanupEvent.ADVANCE:
                with pytest.raises(ValueError):
                    _cleanup_transition(state, action, now)
                visited.add((before, action.value, "invalid"))
                continue
            state = _cleanup_transition(state, action, now)
            visited.add((before, action.value, state.phase.value))
        # A fair suffix supplies due wake and successful advances, with no capture.
        now += CLEANUP_INTERVAL_SECONDS * 2
        if state.phase is _CleanupPhase.WAITING:
            state = _cleanup_transition(state, _CleanupEvent.WAKE, now)
        for _ in range(5):
            if state.phase is _CleanupPhase.WAITING:
                break
            state = _cleanup_transition(state, _CleanupEvent.ADVANCE, now)
        assert state.phase is _CleanupPhase.WAITING
        assert state.due_at > now
    required = (
        ("waiting", "wake", "ack"),
        ("ack", "advance", "publication"),
        ("publication", "advance", "raw"),
        ("raw", "advance", "expiry"),
        ("expiry", "advance", "vacuum"),
        ("vacuum", "advance", "waiting"),
        ("waiting", "advance", "invalid"),
    )
    assert set(required) <= visited
    initial = _CleanupState(_CleanupPhase.WAITING, 10.0)
    assert _cleanup_transition(initial, _CleanupEvent.WAKE, 9.0) == initial
    admitted = _cleanup_transition(initial, _CleanupEvent.WAKE, 10.0)
    assert admitted.phase is _CleanupPhase.ACK
    assert admitted.deadline > 10.0
    assert _cleanup_transition(initial, _CleanupEvent.DEFER, 10.0) == initial
