"""Finite runtime comparisons; counts describe these abstractions, not all schedules.

B assumes successful sink operations and that the scheduler filters callbacks from
failed-start generations. C assumes committed serialization and a known boundary.
P assumes effects finish, retry deadlines pass, and capture eventually releases.
No concurrent callbacks, storage failures, compaction, or crash cuts are modeled.
"""

import asyncio
from itertools import product

import pytest

from omi_collector.capture.application.ble_scan_health import ScanHealthMonitor
from omi_collector.capture.domain.clock_state import ClockIntent, ClockRange, ClockState, ClockStateError
from omi_collector.capture.domain.ready_machine import (
    CaptureBegin,
    CaptureEnd,
    Finished,
    PublicationAction,
    PublicationCommand,
    PublicationResult,
    PublicationState,
    Quiesced,
    Running,
    Settled,
    TimerFired,
    Wake,
    publication_transition,
)
from test_fsm_scan_model import Diagnostics, Sink


def test_bounded_monitor_chronology() -> None:
    async def check() -> None:
        edges: set[tuple[bool, str, bool]] = set()
        states: set[bool] = set()
        actions = ("failure", "old", "equal", "new", "foreign")
        for trace in product(actions, repeat=3):
            now = [0.0]
            sink = Sink()
            monitor = ScanHealthMonitor(sink, Diagnostics(), 1000.0, clock=lambda now=now: now[0])
            await monitor.prepare(1)
            active = False
            failure = -1.0
            states.add(active)
            for index, action in enumerate(trace):
                now[0] = float(10 * (index + 1))
                before = active
                if action == "failure":
                    await monitor.scan_stop_failed(1, OSError("failure"))
                    active, failure = True, now[0]
                else:
                    observed = {"old": failure - 1, "equal": failure, "new": failure + 1, "foreign": now[0]}[action]
                    monitor.radio_activity(2 if action == "foreign" else 1, observed)
                    await monitor.flush_recovery()
                    if action == "new" and failure >= 0:
                        active = False
                assert sink.is_active is active
                states.add(active)
                edges.add((before, action, active))
            if active:
                monitor.radio_activity(1, failure + 1)
                await monitor.flush_recovery()
                assert not sink.is_active
        assert {(True, action, True) for action in ("old", "equal", "foreign")} <= edges
        assert {(False, "failure", True), (True, "failure", True), (True, "new", False)} <= edges
        print(f"B paths=125 states={len(states)} edges={len(edges)}")

    asyncio.run(check())


def test_bounded_clock_intent() -> None:
    edges: set[tuple[str, str, str]] = set()
    states = {"prepared"}
    actions = ("mark", "finish", "reload", "wrong_id")
    ranges = (ClockRange(3, 4, None, None, "unknown", "write"),)
    for trace in product(actions, repeat=3):
        state = ClockState().begin_intent(ClockIntent("write", "prepared", 0, 0, 0.0, 3))
        phase = "prepared"
        for action in trace:
            before = phase
            if action == "reload":
                state = ClockState.from_payload(state.to_payload())
            elif action == "wrong_id" or (action == "mark" and phase == "completed"):
                with pytest.raises(ClockStateError):
                    state.mark_uncertain("other" if action == "wrong_id" else "write", write_sequence_after=4)
                edges.add((before, action, "rejected"))
                continue
            elif action == "mark":
                state = state.mark_uncertain("write", write_sequence_after=4)
                phase = "uncertain"
            elif phase == "uncertain":
                state = state.finish_intent("write", ranges)
                phase = "completed"
            else:
                with pytest.raises(ClockStateError):
                    state.finish_intent("write", ranges)
                edges.add((before, action, "rejected"))
                continue
            assert (state.intent.phase if state.intent else "completed") == phase
            if phase == "completed":
                assert state.causal_uncertainty == ((3, 4),)
            states.add(phase)
            edges.add((before, action, phase))
        if phase == "prepared":
            state = state.mark_uncertain("write", write_sequence_after=4)
        if phase != "completed":
            state = state.finish_intent("write", ranges)
        assert ClockState.from_payload(state.to_payload()).causal_uncertainty == ((3, 4),)
    assert {
        ("prepared", "mark", "uncertain"),
        ("uncertain", "finish", "completed"),
        ("prepared", "finish", "rejected"),
        ("completed", "wrong_id", "rejected"),
        ("uncertain", "reload", "uncertain"),
        ("completed", "reload", "completed"),
        ("completed", "mark", "rejected"),
    } <= edges
    print(f"C paths=64 states={len(states)} edges={len(edges)}")


def _finish_publication(
    state: PublicationState, action: str, mode: str, now: float
) -> tuple[PublicationState, PublicationCommand, str, PublicationAction]:
    assert isinstance(state.work, Running)
    result = PublicationResult.TRANSIENT if action == "transient" else PublicationResult.SETTLED
    state, command = publication_transition(state, Finished(state.work.token, 1, None, result, now, (1.0,)))
    if mode == "capture":
        return state, command, "idle", PublicationAction.NONE
    if action == "transient":
        return state, command, "retry", PublicationAction.ARM
    return state, command, "settled", PublicationAction.NONE


def _publication_fair_suffix(state: PublicationState, mode: str) -> None:
    if mode == "capture":
        state, _ = publication_transition(state, Quiesced())
        state, _ = publication_transition(state, CaptureEnd())
    if isinstance(state.work, Running):
        state, _ = publication_transition(state, Finished(state.work.token, 1, None, PublicationResult.SETTLED, 1000.0))
    elif not isinstance(state.work, Settled):
        state, command = publication_transition(state, Wake(1, 1000.0))
        assert command.action is PublicationAction.RUN and command.token is not None
        state, _ = publication_transition(state, Finished(command.token, 1, None, PublicationResult.SETTLED, 1000.0))
    _, command = publication_transition(state, Wake(1, 1000.0))
    assert command.action is PublicationAction.NONE


def _publication_trace(
    trace: tuple[str, ...],
    states: set[tuple[str, str]],
    edges: set[tuple[tuple[str, str], str, tuple[str, str], str]],
) -> None:
    state = PublicationState()
    mode, work = "available", "idle"
    states.add((mode, work))
    for index, action in enumerate(trace):
        before = (mode, work)
        now = float(index * 10)
        expected = PublicationAction.NONE
        if action == "capture":
            if mode == "capture":
                with pytest.raises(ValueError):
                    publication_transition(state, CaptureBegin())
                edges.add((before, action, before, "rejected"))
                continue
            mode = "capture"
            state, command = publication_transition(state, CaptureBegin())
            expected = PublicationAction.CANCEL_AND_JOIN
        elif action == "release":
            if mode != "capture":
                edges.add((before, action, before, "rejected"))
                continue
            state, _ = publication_transition(state, Quiesced())
            state, command = publication_transition(state, CaptureEnd())
            mode, work = "available", "idle"
            expected = PublicationAction.CHECK_INPUT
        elif action == "wake":
            state, command = publication_transition(state, Wake(1, now))
            if mode == "available" and work in {"idle", "retry"}:
                work, expected = "running", PublicationAction.RUN
        elif work == "running":
            state, command, work, expected = _finish_publication(state, action, mode, now)
        else:
            state, command = publication_transition(state, Finished(-1, 1, None, PublicationResult.SETTLED, now))
        assert command.action is expected
        assert state.mode.value == mode
        assert type(state.work).__name__.lower().replace("retrywait", "retry") == work
        states.add((mode, work))
        edges.add((before, action, (mode, work), expected.value))
    _publication_fair_suffix(state, mode)


def test_bounded_publication_ownership() -> None:
    actions = ("wake", "transient", "success", "capture", "release")
    states: set[tuple[str, str]] = set()
    edges: set[tuple[tuple[str, str], str, tuple[str, str], str]] = set()
    for trace in product(actions, repeat=3):
        _publication_trace(trace, states, edges)
    assert (("available", "running"), "transient", ("available", "retry"), "arm") in edges
    assert (("available", "running"), "success", ("available", "settled"), "none") in edges
    assert (("capture", "idle"), "capture", ("capture", "idle"), "rejected") in edges
    assert (("capture", "idle"), "release", ("available", "idle"), "check_input") in edges
    assert (("available", "retry"), "wake", ("available", "running"), "run") in edges
    assert (("capture", "idle"), "wake", ("capture", "idle"), "none") in edges
    print(f"P paths=125 states={len(states)} edges={len(edges)}")


def test_bounded_publication_token_and_deadline_guards() -> None:
    state, command = publication_transition(PublicationState(), Wake(1, 0.0))
    assert command.token is not None
    unchanged, stale = publication_transition(
        state, Finished(command.token + 1, 1, None, PublicationResult.SETTLED, 0.0)
    )
    assert unchanged == state and stale.action is PublicationAction.NONE
    state, command = publication_transition(
        state, Finished(command.token, 1, None, PublicationResult.TRANSIENT, 0.0, (1.0,))
    )
    assert command.deadline == 1.0
    unchanged, stale = publication_transition(state, TimerFired(state.generation + 1, 1.0, 1.0))
    assert unchanged == state and stale.action is PublicationAction.NONE
    unchanged, early = publication_transition(state, TimerFired(state.generation, 1.0, 0.5))
    assert unchanged == state and early.action is PublicationAction.ARM
    _, due = publication_transition(state, TimerFired(state.generation, 1.0, 1.0))
    assert due.action is PublicationAction.CHECK_INPUT
