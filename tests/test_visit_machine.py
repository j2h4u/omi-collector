from __future__ import annotations

import pytest

from omi_collector.capture.application.visit_machine import (
    AttemptGranted,
    Attempting,
    CandidateUnavailable,
    CloseFailed,
    Closing,
    ClosureCommitted,
    CommitClosure,
    DrainConfirmed,
    FinishVisit,
    Idle,
    InspectRecovery,
    Interrupted,
    NoOp,
    OperatorBatchCompleted,
    PreserveAndStop,
    Recovering,
    RecoveryEnded,
    RecoveryLoaded,
    RunAttempt,
    SessionFinished,
    Shutdown,
    Stopped,
    TransitionResult,
    VisitTransitionError,
    WaitForAttempt,
    Waiting,
    initial_transition,
    transition,
)


def test_startup_recovery_disposition_and_interrupted_close_loop() -> None:
    assert initial_transition() == TransitionResult(Recovering(), InspectRecovery())
    assert transition(Recovering(), RecoveryLoaded("empty")) == TransitionResult(Idle(), WaitForAttempt())
    assert transition(Recovering(), RecoveryLoaded("resumable")) == TransitionResult(
        Waiting(), WaitForAttempt(arm_restored=True)
    )
    closing = transition(Recovering(), RecoveryLoaded("needs_interrupted_close"))
    assert closing == TransitionResult(Closing("restart_interrupted"), CommitClosure("restart_interrupted"))
    assert transition(closing.state, ClosureCommitted()) == TransitionResult(Recovering(), InspectRecovery())


@pytest.mark.parametrize("reason", ["absence", "recovery_exhausted"])
def test_waiting_recovery_end_commits_closure(reason: str) -> None:
    result = transition(Waiting(), RecoveryEnded(reason))  # type: ignore[arg-type]

    assert result == TransitionResult(Closing(reason), CommitClosure(reason))  # type: ignore[arg-type]


@pytest.mark.parametrize("durable_progress", [False, True])
def test_interruption_returns_to_waiting_with_outcome(durable_progress: bool) -> None:
    outcome = Interrupted(connected=True, durable_progress=durable_progress)

    result = transition(Attempting(Waiting()), SessionFinished(outcome))

    assert result == TransitionResult(Waiting(), WaitForAttempt(previous_outcome=outcome))


@pytest.mark.parametrize("origin", [Idle(), Waiting()])
def test_candidate_unavailable_restores_attempt_origin(origin: Idle | Waiting) -> None:
    granted = transition(origin, AttemptGranted())
    result = transition(granted.state, SessionFinished(CandidateUnavailable()))

    assert granted == TransitionResult(Attempting(origin), RunAttempt())
    assert result == TransitionResult(origin, WaitForAttempt(previous_outcome=CandidateUnavailable()))


def test_drain_requires_closure_before_idle_or_stop() -> None:
    closing = transition(Attempting(Idle()), SessionFinished(DrainConfirmed()))

    assert closing == TransitionResult(Closing("drained"), CommitClosure("drained"))
    assert transition(closing.state, ClosureCommitted()) == TransitionResult(Idle(), FinishVisit("drained", False))
    assert transition(closing.state, ClosureCommitted(), stop_after_drained=True) == TransitionResult(
        Stopped(), FinishVisit("drained", True)
    )


def test_interrupted_restart_close_continues_even_when_drain_would_stop() -> None:
    closing = Closing("restart_interrupted")

    assert transition(closing, ClosureCommitted(), stop_after_drained=True) == TransitionResult(
        Recovering(), InspectRecovery()
    )


def test_operator_limit_is_a_distinct_stopping_reason() -> None:
    closing = transition(Attempting(Waiting()), SessionFinished(OperatorBatchCompleted()))

    assert closing == TransitionResult(Closing("operator_limit"), CommitClosure("operator_limit"))
    assert transition(closing.state, ClosureCommitted()) == TransitionResult(
        Stopped(), FinishVisit("operator_limit", True)
    )


def test_close_failure_shutdown_and_stopped_late_events_preserve_evidence() -> None:
    assert transition(Closing("absence"), CloseFailed()) == TransitionResult(Stopped(), PreserveAndStop())
    assert transition(Waiting(), Shutdown()) == TransitionResult(Stopped(), PreserveAndStop())
    late_events = (
        RecoveryLoaded("empty"),
        AttemptGranted(),
        SessionFinished(DrainConfirmed()),
        RecoveryEnded("absence"),
        ClosureCommitted(),
        CloseFailed(),
        Shutdown(),
    )
    for event in late_events:
        assert transition(Stopped(), event) == TransitionResult(Stopped(), NoOp())


@pytest.mark.parametrize(
    ("state", "event"),
    [
        (Recovering(), SessionFinished(DrainConfirmed())),
        (Idle(), SessionFinished(DrainConfirmed())),
        (Waiting(), SessionFinished(DrainConfirmed())),
        (Attempting(Idle()), ClosureCommitted()),
        (Closing("drained"), AttemptGranted()),
        (Idle(), RecoveryEnded("absence")),
    ],
)
def test_invalid_events_raise_visit_transition_error(state: object, event: object) -> None:
    with pytest.raises(VisitTransitionError):
        transition(state, event)  # type: ignore[arg-type]
