from __future__ import annotations

from itertools import product
from typing import Literal

import pytest

from omi_collector.capture.application.session_machine import (
    AfterTeardown,
    CancellationObserved,
    CheckpointResolved,
    Connected,
    EffectFailed,
    InfoResolved,
    OutcomeReturned,
    PreflightResolved,
    ReadResolved,
    SessionCommand,
    SessionState,
    SessionTransitionError,
    TeardownResolved,
    initial_state,
    transition,
)

OUTCOMES = ("drained", "collected", "retry", "candidate_unavailable", "connected_interrupted")
RETRY_OUTCOMES = ("retry", "candidate_unavailable", "connected_interrupted")
STATES = (
    *(
        SessionState(command)
        for command in (
            SessionCommand.CONNECT,
            SessionCommand.INFO,
            SessionCommand.PREFLIGHT,
            SessionCommand.READ,
        )
    ),
    *(SessionState(SessionCommand.TEARDOWN, AfterTeardown.CHECKPOINT, outcome=outcome) for outcome in OUTCOMES),
    *(SessionState(SessionCommand.TEARDOWN, after) for after in (AfterTeardown.FAILED, AfterTeardown.CANCELLED)),
    *(
        SessionState(command, outcome=outcome, teardown_interrupted=interrupted)
        for command in (SessionCommand.CHECKPOINT, SessionCommand.FINISHED, SessionCommand.RETURNED)
        for outcome in OUTCOMES
        for interrupted in (False, True)
        if not interrupted or outcome == "connected_interrupted"
    ),
    SessionState(SessionCommand.FAILED),
    SessionState(SessionCommand.CANCELLED),
)
EVENTS = (
    Connected(),
    InfoResolved(),
    *(PreflightResolved(result) for result in ("disabled", "completed", "degraded")),
    *(ReadResolved(result) for result in ("pending", "drained", "collected")),
    *(EffectFailed(outcome) for outcome in (*RETRY_OUTCOMES, None)),
    CancellationObserved(),
    *(TeardownResolved(interrupted) for interrupted in (False, True)),
    CheckpointResolved(),
    *(OutcomeReturned(outcome) for outcome in OUTCOMES),
)
VALID_EVENT_TYPES = {
    SessionCommand.CONNECT: (Connected, EffectFailed, CancellationObserved),
    SessionCommand.INFO: (InfoResolved, EffectFailed, CancellationObserved),
    SessionCommand.PREFLIGHT: (PreflightResolved, EffectFailed, CancellationObserved),
    SessionCommand.READ: (ReadResolved, EffectFailed, CancellationObserved),
    SessionCommand.TEARDOWN: (TeardownResolved, EffectFailed, CancellationObserved),
    SessionCommand.CHECKPOINT: (CheckpointResolved, EffectFailed, CancellationObserved),
    SessionCommand.FINISHED: (OutcomeReturned,),
    SessionCommand.RETURNED: (),
    SessionCommand.FAILED: (),
    SessionCommand.CANCELLED: (),
}


def test_every_state_and_event_pair_is_explicit() -> None:
    for state, event in product(STATES, EVENTS):
        valid = type(event) in VALID_EVENT_TYPES[state.command]
        if isinstance(event, EffectFailed) and state.command in {
            SessionCommand.TEARDOWN,
            SessionCommand.CHECKPOINT,
        }:
            valid = valid and event.retry_outcome is None
        if isinstance(event, EffectFailed) and state.command is SessionCommand.CONNECT:
            valid = valid and event.retry_outcome != "connected_interrupted"
        if isinstance(event, OutcomeReturned):
            valid = valid and event.outcome == state.outcome
        if valid:
            assert isinstance(transition(state, event), SessionState)
        else:
            with pytest.raises(SessionTransitionError):
                transition(state, event)


def test_success_path_requires_preflight_read_teardown_and_checkpoint_receipts() -> None:
    state = initial_state()
    state = transition(state, Connected())
    state = transition(state, InfoResolved())
    state = transition(state, PreflightResolved("completed"))
    state = transition(state, ReadResolved("pending"))
    state = transition(state, ReadResolved("drained"))
    state = transition(state, TeardownResolved(False))
    state = transition(state, CheckpointResolved())
    state = transition(state, OutcomeReturned("drained"))
    assert state == SessionState(SessionCommand.RETURNED, outcome="drained")

    with pytest.raises(SessionTransitionError, match="invalid while connect"):
        transition(initial_state(), InfoResolved())
    with pytest.raises(SessionTransitionError, match="invalid while info"):
        transition(SessionState(SessionCommand.INFO), ReadResolved("drained"))


@pytest.mark.parametrize("preflight", ("disabled", "degraded"))
def test_optional_preflight_resolution_still_precedes_read(preflight: Literal["disabled", "degraded"]) -> None:
    state = transition(initial_state(), Connected())
    state = transition(state, InfoResolved())
    state = transition(state, PreflightResolved(preflight))
    assert state.command is SessionCommand.READ


@pytest.mark.parametrize("command", (SessionCommand.CONNECT, SessionCommand.INFO, SessionCommand.READ))
def test_retryable_failure_reaches_checkpoint_only_after_required_teardown(command: SessionCommand) -> None:
    state = SessionState(command)
    state = transition(state, EffectFailed("retry"))
    if command is SessionCommand.CONNECT:
        assert state == SessionState(SessionCommand.CHECKPOINT, outcome="retry")
    else:
        assert state == SessionState(
            SessionCommand.TEARDOWN,
            AfterTeardown.CHECKPOINT,
            outcome="retry",
        )
        state = transition(state, TeardownResolved(False))
        assert state == SessionState(SessionCommand.CHECKPOINT, outcome="retry")
    state = transition(state, CheckpointResolved())
    assert transition(state, OutcomeReturned("retry")).command is SessionCommand.RETURNED


def test_fatal_failure_and_cancellation_do_not_become_success() -> None:
    state = transition(SessionState(SessionCommand.READ), EffectFailed(None))
    assert state == SessionState(SessionCommand.TEARDOWN, AfterTeardown.FAILED)
    assert transition(state, TeardownResolved(False)).command is SessionCommand.FAILED

    state = transition(SessionState(SessionCommand.READ), CancellationObserved())
    assert state == SessionState(SessionCommand.TEARDOWN, AfterTeardown.CANCELLED)
    assert transition(state, TeardownResolved(False)).command is SessionCommand.CANCELLED


@pytest.mark.parametrize(
    ("command", "expected_command", "continuation"),
    [
        (SessionCommand.CONNECT, SessionCommand.CANCELLED, None),
        (SessionCommand.INFO, SessionCommand.TEARDOWN, AfterTeardown.CANCELLED),
        (SessionCommand.PREFLIGHT, SessionCommand.TEARDOWN, AfterTeardown.CANCELLED),
    ],
)
def test_cancellation_uses_session_aware_teardown(
    command: SessionCommand,
    expected_command: SessionCommand,
    continuation: AfterTeardown | None,
) -> None:
    state = transition(SessionState(command), CancellationObserved())
    assert state.command is expected_command
    assert state.after_teardown is continuation
    assert state.outcome is None
    if continuation is AfterTeardown.CANCELLED:
        cancelled = transition(state, TeardownResolved(False))
        assert cancelled.command is SessionCommand.CANCELLED
        assert cancelled.after_teardown is None
        assert cancelled.outcome is None


def test_pending_read_repeats_and_collected_result_survives_teardown_checkpoint() -> None:
    reading = SessionState(SessionCommand.READ)
    assert transition(reading, ReadResolved("pending")) == reading

    teardown = transition(reading, ReadResolved("collected"))
    assert teardown.command is SessionCommand.TEARDOWN
    assert teardown.after_teardown is AfterTeardown.CHECKPOINT
    assert teardown.outcome == "collected"

    checkpoint = transition(teardown, TeardownResolved(False))
    assert checkpoint.command is SessionCommand.CHECKPOINT
    assert checkpoint.after_teardown is None
    assert checkpoint.outcome == "collected"
    assert checkpoint.teardown_interrupted is False
    finished = transition(checkpoint, CheckpointResolved())
    assert finished.command is SessionCommand.FINISHED
    assert finished.outcome == "collected"


def test_checkpoint_cancellation_is_terminal() -> None:
    state = transition(SessionState(SessionCommand.CHECKPOINT, outcome="collected"), CancellationObserved())
    assert state.command is SessionCommand.CANCELLED
    assert state.after_teardown is None
    assert state.outcome is None


def test_retryable_teardown_failure_overrides_success_and_binds_outcome() -> None:
    state = transition(initial_state(), Connected())
    state = transition(state, InfoResolved())
    state = transition(state, PreflightResolved("completed"))
    state = transition(state, ReadResolved("drained"))
    state = transition(state, TeardownResolved(True))
    assert state == SessionState(
        SessionCommand.CHECKPOINT,
        outcome="connected_interrupted",
        teardown_interrupted=True,
    )
    state = transition(state, CheckpointResolved())
    assert state == SessionState(
        SessionCommand.FINISHED,
        outcome="connected_interrupted",
        teardown_interrupted=True,
    )
    assert transition(state, OutcomeReturned("connected_interrupted")).command is SessionCommand.RETURNED
    with pytest.raises(SessionTransitionError, match="does not match"):
        transition(state, OutcomeReturned("drained"))


def test_impossible_machine_metadata_is_rejected() -> None:
    with pytest.raises(ValueError, match="continuation"):
        SessionState(SessionCommand.TEARDOWN)
    with pytest.raises(ValueError, match="invalid outcome metadata"):
        SessionState(SessionCommand.CHECKPOINT)
    with pytest.raises(ValueError, match="only interrupted outcomes"):
        SessionState(SessionCommand.CHECKPOINT, outcome="drained", teardown_interrupted=True)
    with pytest.raises(ValueError, match="only teardown"):
        SessionState(SessionCommand.INFO, after_teardown=AfterTeardown.CHECKPOINT)
