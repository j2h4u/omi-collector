"""Bounded cleanup ordering contract checked against its runtime transition.

Run: uv run --no-sync pytest -q tests/test_fsm_cleanup_model.py --hypothesis-show-statistics
A cycle starts waiting, wakes into ACK, then a publication attempt, raw,
expiry, vacuum, and waiting. A transient publication does not prohibit
independently authorized reclamation; blocked publication and capture defer it.
Advance while waiting is invalid; defer ends a running cycle.
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
        now = 10.0
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


def test_publication_attempt_outcomes_preserve_cleanup_obligations() -> None:
    visited: set[tuple[str, str]] = set()
    for outcome in ("published", "waiting", "transient", "blocked", "capture"):
        state = _CleanupState(_CleanupPhase.PUBLICATION, 0.0, 600.0)
        action = _CleanupEvent.DEFER if outcome in {"blocked", "capture"} else _CleanupEvent.ADVANCE
        state = _cleanup_transition(state, action, 1.0)
        visited.add((outcome, state.phase.value))
    assert visited == {
        ("published", "raw"),
        ("waiting", "raw"),
        ("transient", "raw"),
        ("blocked", "waiting"),
        ("capture", "waiting"),
    }


@pytest.mark.parametrize("boundary", ("none", "change", "capture", "shutdown"))
@pytest.mark.parametrize("timer_at", (5.0, 15.0))
def test_retry_compaction_boundary_traces(boundary: str, timer_at: float) -> None:
    """The publication deadline survives an independent quiet compaction."""
    _check_retry_compaction_trace(boundary, timer_at)


@settings(max_examples=40, deadline=None)
@given(
    st.lists(
        st.tuples(
            st.sampled_from(("none", "change", "capture", "shutdown")),
            st.floats(min_value=0, max_value=20, allow_nan=False, allow_infinity=False),
        ),
        min_size=1,
        max_size=8,
    )
)
def test_generated_retry_compaction_boundaries(traces: list[tuple[str, float]]) -> None:
    for boundary, timer_at in traces:
        _check_retry_compaction_trace(boundary, timer_at)
        event(f"{boundary}:timer-{timer_at}")


def _check_retry_compaction_trace(boundary: str, timer_at: float) -> None:
    from typing import cast

    from omi_collector.capture.domain.ready_machine import (
        CaptureBegin,
        CaptureEnd,
        CompactionDue,
        CompactionFinished,
        InputChanged,
        PublicationAction,
        PublicationEvent,
        PublicationMode,
        PublicationResult,
        PublicationState,
        Quiesced,
        RetryWait,
        Shutdown,
        TimerFired,
        Wake,
        publication_transition,
    )

    retry = RetryWait("old", "transient", 10.0, 1)
    state, command = publication_transition(PublicationState(work=retry), CompactionDue("new", 7))
    assert command.action is PublicationAction.COMPACT and command.token is not None
    with pytest.raises(ValueError):
        publication_transition(state, cast(PublicationEvent, object()))
    token = command.token
    for stale in (CompactionFinished(token + 1, 7, "new", PublicationResult.SETTLED, 1.0),):
        unchanged, command = publication_transition(state, stale)
        assert unchanged == state and command.action is PublicationAction.NONE
    invalid_debt, command = publication_transition(
        state, CompactionFinished(token, 8, "new", PublicationResult.SETTLED, 1.0)
    )
    assert invalid_debt.needs_check and command.action is PublicationAction.CHECK_INPUT
    state, command = publication_transition(state, TimerFired(0, 10.0, timer_at))
    assert command.action is PublicationAction.NONE
    if boundary == "change":
        state, _ = publication_transition(state, InputChanged())
    elif boundary == "capture":
        state, command = publication_transition(state, CaptureBegin())
        assert command.action is PublicationAction.CANCEL_AND_JOIN
    elif boundary == "shutdown":
        state, command = publication_transition(state, Shutdown())
        assert command.action is PublicationAction.CANCEL_AND_JOIN
    state, command = publication_transition(state, CompactionFinished(token, 7, "new", PublicationResult.SETTLED, 16.0))
    if boundary == "none":
        assert state.work == retry and command.action is PublicationAction.ARM and command.deadline == 10.0
        _, early = publication_transition(state, Wake("old", 9.0))
        assert early.action is PublicationAction.ARM
        _, due = publication_transition(state, Wake("old", 10.0))
        assert due.action is PublicationAction.RUN
    elif boundary == "change":
        assert command.action is PublicationAction.CHECK_INPUT
    else:
        assert command.action is PublicationAction.NONE
        state, _ = publication_transition(state, Quiesced())
        state, command = publication_transition(state, CaptureEnd())
        assert state.mode is (PublicationMode.AVAILABLE if boundary == "capture" else PublicationMode.CLOSED)
        assert command.action is (PublicationAction.CHECK_INPUT if boundary == "capture" else PublicationAction.NONE)
