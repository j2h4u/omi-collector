from __future__ import annotations

from dataclasses import FrozenInstanceError, fields
from itertools import product
from typing import Literal, Protocol, cast, get_args

import pytest

from omi_collector.capture.application.visit_machine import (
    AttemptGranted,
    Attempting,
    CandidateUnavailable,
    CloseFailed,
    CloseReason,
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
    RecoveryDisposition,
    RecoveryEnded,
    RecoveryEndReason,
    RecoveryLoaded,
    RunAttempt,
    SessionFinished,
    SessionOutcome,
    Shutdown,
    Stopped,
    TransitionResult,
    VisitCommand,
    VisitEvent,
    VisitState,
    VisitTransitionError,
    WaitForAttempt,
    Waiting,
    initial_transition,
    transition,
)


class _AliasWithValue(Protocol):
    __value__: object


def _alias_args(alias: object) -> tuple[object, ...]:
    return cast(tuple[object, ...], get_args(cast(_AliasWithValue, alias).__value__))


CLOSE_REASONS = cast(tuple[CloseReason, ...], _alias_args(CloseReason))
RECOVERY_DISPOSITIONS = cast(tuple[RecoveryDisposition, ...], _alias_args(RecoveryDisposition))
RECOVERY_END_REASONS = cast(tuple[RecoveryEndReason, ...], _alias_args(RecoveryEndReason))
STOP_POLICIES = (False, True)
COMMAND_TYPES = (InspectRecovery, WaitForAttempt, RunAttempt, CommitClosure, FinishVisit, PreserveAndStop, NoOp)
STATES = (
    Recovering(),
    Idle(),
    Waiting(),
    Attempting(Idle()),
    Attempting(Waiting()),
    *(Closing(reason) for reason in CLOSE_REASONS),
    Stopped(),
)
OUTCOMES = (
    DrainConfirmed(),
    *(Interrupted(connected, durable) for connected, durable in product((False, True), repeat=2)),
    CandidateUnavailable(),
    OperatorBatchCompleted(),
)
EVENTS: tuple[VisitEvent, ...] = (
    *(RecoveryLoaded(disposition) for disposition in RECOVERY_DISPOSITIONS),
    AttemptGranted(),
    *(SessionFinished(outcome) for outcome in OUTCOMES),
    *(RecoveryEnded(reason) for reason in RECOVERY_END_REASONS),
    ClosureCommitted(),
    CloseFailed(),
    Shutdown(),
)
LEGAL_EVENT_TYPES: dict[type[object], frozenset[type[object]]] = {
    Recovering: frozenset({RecoveryLoaded, Shutdown}),
    Idle: frozenset({AttemptGranted, Shutdown}),
    Waiting: frozenset({AttemptGranted, RecoveryEnded, Shutdown}),
    Attempting: frozenset({SessionFinished, Shutdown}),
    Closing: frozenset({ClosureCommitted, CloseFailed, Shutdown}),
    Stopped: frozenset(type(event) for event in EVENTS),
}

EXPECTED_CLOSURE_RESULTS = {
    "drained": {
        False: TransitionResult(Idle(), FinishVisit("drained", False)),
        True: TransitionResult(Stopped(), FinishVisit("drained", True)),
    },
    "absence": {
        False: TransitionResult(Idle(), FinishVisit("absence", False)),
        True: TransitionResult(Idle(), FinishVisit("absence", False)),
    },
    "recovery_exhausted": {
        False: TransitionResult(Idle(), FinishVisit("recovery_exhausted", False)),
        True: TransitionResult(Idle(), FinishVisit("recovery_exhausted", False)),
    },
    "restart_interrupted": {
        False: TransitionResult(Recovering(), InspectRecovery()),
        True: TransitionResult(Recovering(), InspectRecovery()),
    },
    "operator_limit": {
        False: TransitionResult(Stopped(), FinishVisit("operator_limit", True)),
        True: TransitionResult(Stopped(), FinishVisit("operator_limit", True)),
    },
}


def _expected_transitions() -> list[tuple[tuple[VisitState, VisitEvent, bool], TransitionResult]]:
    rows: list[tuple[tuple[VisitState, VisitEvent, bool], TransitionResult]] = []
    for stop_after_drained in STOP_POLICIES:
        rows.extend(
            [
                (
                    (Recovering(), RecoveryLoaded("empty"), stop_after_drained),
                    TransitionResult(Idle(), WaitForAttempt(arm_restored=False)),
                ),
                (
                    (Recovering(), RecoveryLoaded("resumable"), stop_after_drained),
                    TransitionResult(Waiting(), WaitForAttempt(arm_restored=True)),
                ),
                (
                    (Recovering(), RecoveryLoaded("needs_interrupted_close"), stop_after_drained),
                    TransitionResult(Closing("restart_interrupted"), CommitClosure("restart_interrupted")),
                ),
                ((Idle(), AttemptGranted(), stop_after_drained), TransitionResult(Attempting(Idle()), RunAttempt())),
                (
                    (Waiting(), AttemptGranted(), stop_after_drained),
                    TransitionResult(Attempting(Waiting()), RunAttempt()),
                ),
                (
                    (Waiting(), RecoveryEnded("absence"), stop_after_drained),
                    TransitionResult(Closing("absence"), CommitClosure("absence")),
                ),
                (
                    (Waiting(), RecoveryEnded("recovery_exhausted"), stop_after_drained),
                    TransitionResult(Closing("recovery_exhausted"), CommitClosure("recovery_exhausted")),
                ),
                (
                    (Closing("drained"), ClosureCommitted(), stop_after_drained),
                    EXPECTED_CLOSURE_RESULTS["drained"][stop_after_drained],
                ),
                (
                    (Closing("absence"), ClosureCommitted(), stop_after_drained),
                    EXPECTED_CLOSURE_RESULTS["absence"][stop_after_drained],
                ),
                (
                    (Closing("recovery_exhausted"), ClosureCommitted(), stop_after_drained),
                    EXPECTED_CLOSURE_RESULTS["recovery_exhausted"][stop_after_drained],
                ),
                (
                    (Closing("restart_interrupted"), ClosureCommitted(), stop_after_drained),
                    EXPECTED_CLOSURE_RESULTS["restart_interrupted"][stop_after_drained],
                ),
                (
                    (Closing("operator_limit"), ClosureCommitted(), stop_after_drained),
                    EXPECTED_CLOSURE_RESULTS["operator_limit"][stop_after_drained],
                ),
            ]
        )
        for origin in (Idle(), Waiting()):
            rows.extend(
                [
                    (
                        (Attempting(origin), SessionFinished(DrainConfirmed()), stop_after_drained),
                        TransitionResult(Closing("drained"), CommitClosure("drained")),
                    ),
                    (
                        (
                            Attempting(origin),
                            SessionFinished(CandidateUnavailable()),
                            stop_after_drained,
                        ),
                        TransitionResult(
                            origin,
                            WaitForAttempt(arm_restored=False, previous_outcome=CandidateUnavailable()),
                        ),
                    ),
                    (
                        (
                            Attempting(origin),
                            SessionFinished(OperatorBatchCompleted()),
                            stop_after_drained,
                        ),
                        TransitionResult(Closing("operator_limit"), CommitClosure("operator_limit")),
                    ),
                ]
            )
            for outcome in OUTCOMES:
                if isinstance(outcome, Interrupted):
                    rows.append(
                        (
                            (Attempting(origin), SessionFinished(outcome), stop_after_drained),
                            TransitionResult(
                                Waiting(),
                                WaitForAttempt(arm_restored=False, previous_outcome=outcome),
                            ),
                        )
                    )
        for state in STATES:
            if not isinstance(state, Stopped):
                rows.append(((state, Shutdown(), stop_after_drained), TransitionResult(Stopped(), PreserveAndStop())))
            else:
                for event in EVENTS:
                    rows.append(((state, event, stop_after_drained), TransitionResult(state, NoOp())))
        for state in (Closing(reason) for reason in CLOSE_REASONS):
            rows.append(((state, CloseFailed(), stop_after_drained), TransitionResult(Stopped(), PreserveAndStop())))
    return rows


FIELD_INVENTORY = {
    Recovering: (),
    Idle: (),
    Waiting: (),
    Attempting: ("origin",),
    Closing: ("reason",),
    Stopped: (),
    DrainConfirmed: (),
    Interrupted: ("connected", "durable_progress"),
    CandidateUnavailable: (),
    OperatorBatchCompleted: (),
    RecoveryLoaded: ("disposition",),
    AttemptGranted: (),
    SessionFinished: ("outcome",),
    RecoveryEnded: ("reason",),
    ClosureCommitted: (),
    CloseFailed: (),
    Shutdown: (),
    InspectRecovery: (),
    WaitForAttempt: ("arm_restored", "previous_outcome"),
    RunAttempt: (),
    CommitClosure: ("reason",),
    FinishVisit: ("reason", "stop"),
    PreserveAndStop: (),
    NoOp: (),
    TransitionResult: ("state", "command"),
}


def test_visit_machine_values_are_immutable() -> None:
    for record_type in FIELD_INVENTORY:
        record = record_type(*([None] * len(fields(record_type))))
        hash(record)
        if record_fields := fields(record_type):
            with pytest.raises(FrozenInstanceError):
                setattr(record, record_fields[0].name, object())


def test_startup_recovery_disposition_and_interrupted_close_loop() -> None:
    assert initial_transition() == TransitionResult(Recovering(), InspectRecovery())
    assert transition(Recovering(), RecoveryLoaded("empty")) == TransitionResult(
        Idle(), WaitForAttempt(arm_restored=False)
    )
    assert transition(Recovering(), RecoveryLoaded("resumable")) == TransitionResult(
        Waiting(), WaitForAttempt(arm_restored=True)
    )
    closing = transition(Recovering(), RecoveryLoaded("needs_interrupted_close"))
    assert closing == TransitionResult(Closing("restart_interrupted"), CommitClosure("restart_interrupted"))
    assert transition(closing.state, ClosureCommitted()) == TransitionResult(Recovering(), InspectRecovery())


def test_restored_startup_arms_waiting_but_empty_and_interrupted_visits_do_not() -> None:
    empty = transition(Recovering(), RecoveryLoaded("empty"))
    restored = transition(Recovering(), RecoveryLoaded("resumable"))
    interrupted_outcome = Interrupted(connected=True, durable_progress=False)
    interrupted = transition(Attempting(Waiting()), SessionFinished(interrupted_outcome))

    assert empty == TransitionResult(Idle(), WaitForAttempt(arm_restored=False, previous_outcome=None))
    assert restored == TransitionResult(Waiting(), WaitForAttempt(arm_restored=True, previous_outcome=None))
    assert interrupted == TransitionResult(
        Waiting(),
        WaitForAttempt(arm_restored=False, previous_outcome=interrupted_outcome),
    )


@pytest.mark.parametrize("reason", ["absence", "recovery_exhausted"])
def test_waiting_recovery_end_commits_closure(reason: str) -> None:
    result = transition(Waiting(), RecoveryEnded(reason))  # type: ignore[arg-type]

    assert result == TransitionResult(Closing(reason), CommitClosure(reason))  # type: ignore[arg-type]


@pytest.mark.parametrize("durable_progress", [False, True])
def test_interruption_returns_to_waiting_with_outcome(durable_progress: bool) -> None:
    outcome = Interrupted(connected=True, durable_progress=durable_progress)

    result = transition(Attempting(Waiting()), SessionFinished(outcome))

    assert result == TransitionResult(Waiting(), WaitForAttempt(arm_restored=False, previous_outcome=outcome))


@pytest.mark.parametrize("origin", [Idle(), Waiting()])
def test_candidate_unavailable_restores_attempt_origin(origin: Idle | Waiting) -> None:
    granted = transition(origin, AttemptGranted())
    result = transition(granted.state, SessionFinished(CandidateUnavailable()))

    assert granted == TransitionResult(Attempting(origin), RunAttempt())
    assert result == TransitionResult(
        origin,
        WaitForAttempt(arm_restored=False, previous_outcome=CandidateUnavailable()),
    )


def test_drain_requires_closure_before_idle_or_stop() -> None:
    closing = transition(Attempting(Idle()), SessionFinished(DrainConfirmed()))

    assert closing == TransitionResult(Closing("drained"), CommitClosure("drained"))
    assert transition(closing.state, ClosureCommitted()) == TransitionResult(Idle(), FinishVisit("drained", False))
    assert transition(closing.state, ClosureCommitted(), stop_after_drained=True) == TransitionResult(
        Stopped(), FinishVisit("drained", True)
    )


@pytest.mark.parametrize("reason", ["absence", "recovery_exhausted"])
def test_committed_non_drained_closure_never_stops(
    reason: Literal["absence", "recovery_exhausted"],
) -> None:
    assert transition(Closing(reason), ClosureCommitted(), stop_after_drained=True) == TransitionResult(
        Idle(), FinishVisit(reason, False)
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


def test_finite_model_is_exhaustive_and_reachable() -> None:
    expected_transitions = _expected_transitions()
    assert set(_alias_args(VisitState)) == {Recovering, Idle, Waiting, Attempting, Closing, Stopped}
    assert set(_alias_args(VisitEvent)) == {
        RecoveryLoaded,
        AttemptGranted,
        SessionFinished,
        RecoveryEnded,
        ClosureCommitted,
        CloseFailed,
        Shutdown,
    }
    assert set(_alias_args(SessionOutcome)) == {
        DrainConfirmed,
        Interrupted,
        CandidateUnavailable,
        OperatorBatchCompleted,
    }
    assert set(_alias_args(VisitCommand)) == {
        InspectRecovery,
        WaitForAttempt,
        RunAttempt,
        CommitClosure,
        FinishVisit,
        PreserveAndStop,
        NoOp,
    }
    assert {type(state) for state in STATES} == set(_alias_args(VisitState))
    assert {type(event) for event in EVENTS} == set(_alias_args(VisitEvent))
    assert {type(outcome) for outcome in OUTCOMES} == set(_alias_args(SessionOutcome))
    for dataclass_type, field_names in FIELD_INVENTORY.items():
        assert tuple(field.name for field in fields(dataclass_type)) == field_names
    assert len(STATES) == 11
    assert len(EVENTS) == 16
    assert len(STATES) * len(EVENTS) * len(STOP_POLICIES) == 352
    assert len(expected_transitions) == 114
    legal_keys = [
        (state, event, stop_after_drained)
        for state, event, stop_after_drained in product(STATES, EVENTS, STOP_POLICIES)
        if type(event) in LEGAL_EVENT_TYPES[type(state)]
    ]
    assert len(legal_keys) == 114
    assert all(
        sum(expected_key == legal_key for expected_key, _ in expected_transitions) == 1 for legal_key in legal_keys
    )
    assert {state.reason for state in STATES if isinstance(state, Closing)} == set(CLOSE_REASONS)
    assert {event.disposition for event in EVENTS if isinstance(event, RecoveryLoaded)} == set(RECOVERY_DISPOSITIONS)
    assert {event.reason for event in EVENTS if isinstance(event, RecoveryEnded)} == set(RECOVERY_END_REASONS)
    assert {
        (event.outcome.connected, event.outcome.durable_progress)
        for event in EVENTS
        if isinstance(event, SessionFinished) and isinstance(event.outcome, Interrupted)
    } == set(product((False, True), repeat=2))

    accepted = rejected = 0
    for state, event, stop_after_drained in product(STATES, EVENTS, STOP_POLICIES):
        legal = type(event) in LEGAL_EVENT_TYPES[type(state)]
        try:
            result = transition(state, event, stop_after_drained=stop_after_drained)
        except VisitTransitionError as error:
            assert type(error) is VisitTransitionError
            assert not legal
            rejected += 1
            continue
        assert legal
        accepted += 1
        assert result.state in STATES
        assert isinstance(result.command, COMMAND_TYPES)
        expected_matches = [
            expected
            for expected_key, expected in expected_transitions
            if expected_key == (state, event, stop_after_drained)
        ]
        assert len(expected_matches) == 1
        assert result == expected_matches[0]
        _assert_finite_invariants(state, event, result, stop_after_drained)

    assert (accepted, rejected) == (114, 238)
    _assert_all_states_reachable()


@pytest.mark.parametrize(
    ("value", "field", "replacement"),
    (
        (Attempting(Idle()), "origin", Waiting()),
        (Closing("drained"), "reason", "absence"),
        (Interrupted(True, False), "connected", False),
        (Interrupted(True, False), "durable_progress", True),
        (RecoveryLoaded("empty"), "disposition", "resumable"),
        (SessionFinished(DrainConfirmed()), "outcome", CandidateUnavailable()),
        (RecoveryEnded("absence"), "reason", "recovery_exhausted"),
        (WaitForAttempt(), "arm_restored", True),
        (WaitForAttempt(), "previous_outcome", Interrupted(True, False)),
        (CommitClosure("drained"), "reason", "absence"),
        (FinishVisit("drained", False), "reason", "absence"),
        (FinishVisit("drained", False), "stop", True),
        (TransitionResult(Recovering(), InspectRecovery()), "state", Idle()),
        (TransitionResult(Recovering(), InspectRecovery()), "command", NoOp()),
    ),
)
def test_machine_records_reject_field_reassignment(value: object, field: str, replacement: object) -> None:
    with pytest.raises(FrozenInstanceError):
        setattr(value, field, replacement)


def _assert_finite_invariants(
    state: VisitState,
    event: VisitEvent,
    result: TransitionResult,
    stop_after_drained: bool,
) -> None:
    if isinstance(state, Stopped):
        assert result == TransitionResult(state, NoOp())
        return
    if isinstance(result.command, CommitClosure):
        assert isinstance(result.state, Closing)
        assert result.command.reason == result.state.reason
    if isinstance(result.command, RunAttempt):
        assert isinstance(state, (Idle, Waiting))
        assert isinstance(event, AttemptGranted)
        assert result.state == Attempting(state)
    if isinstance(result.command, FinishVisit):
        assert isinstance(state, Closing)
        assert isinstance(event, ClosureCommitted)
        assert result.command.reason == state.reason
        assert result.command.stop == (
            state.reason == "operator_limit" or (state.reason == "drained" and stop_after_drained)
        )
        if state.reason in {"absence", "recovery_exhausted"}:
            assert result.command.stop is False
    if isinstance(event, SessionFinished) and isinstance(event.outcome, DrainConfirmed):
        assert isinstance(state, Attempting)
        assert result.state == Closing("drained")
    if isinstance(event, SessionFinished) and isinstance(event.outcome, CandidateUnavailable):
        assert isinstance(state, Attempting)
        assert result.state == state.origin
    if isinstance(event, (CloseFailed, Shutdown)):
        assert isinstance(result.state, Stopped)
        assert isinstance(result.command, PreserveAndStop)


def _assert_all_states_reachable() -> None:
    for stop_after_drained in STOP_POLICIES:
        pending = [initial_transition().state]
        reached = [pending[0]]
        while pending:
            state = pending.pop()
            for event in EVENTS:
                try:
                    result = transition(state, event, stop_after_drained=stop_after_drained)
                except VisitTransitionError:
                    continue
                assert result.state in STATES
                if result.state not in reached:
                    reached.append(result.state)
                    pending.append(result.state)
        assert len(reached) == len(STATES)
        assert all(state in reached for state in STATES)
