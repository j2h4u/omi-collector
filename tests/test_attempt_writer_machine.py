"""Exhaustive pure-transition coverage for the attempt-writer lifecycle."""

from __future__ import annotations

import pytest

from omi_collector.capture.adapters.attempt_writer_machine import (
    Admit,
    AttemptWriterMachineEvent,
    AttemptWriterMachineState,
    CheckpointRequested,
    CheckpointSucceeded,
    Closed,
    CloseFailed,
    CloseRequested,
    CloseSucceeded,
    Closing,
    CommandFailed,
    Constructed,
    Failed,
    Finalized,
    FinalizeRequested,
    FinalizeSucceeded,
    Finalizing,
    Ignore,
    LegRequested,
    LegSucceeded,
    Prepared,
    PrepareSucceeded,
    Preparing,
    PublishRequested,
    ReadBeginRequested,
    ReadBeginSucceeded,
    Reading,
    Reject,
    RejectionKind,
    ReuseClose,
    StartRequested,
    failure_of,
    transition,
)

ERROR = OSError("target failed")
CLOSE_ERROR = OSError("close failed")
STATES: tuple[AttemptWriterMachineState, ...] = (
    Constructed(),
    Preparing(),
    Prepared(),
    Reading(),
    Finalizing(),
    Finalized(),
    Failed(ERROR),
    Closing(None, False, True),
    Closed(ERROR, True),
)
EVENTS: tuple[AttemptWriterMachineEvent, ...] = (
    StartRequested(),
    LegRequested(),
    ReadBeginRequested(),
    CheckpointRequested(),
    FinalizeRequested(),
    CloseRequested(),
    PublishRequested(),
    PrepareSucceeded(),
    LegSucceeded(),
    ReadBeginSucceeded(),
    CheckpointSucceeded(),
    FinalizeSucceeded(),
    CommandFailed(ERROR),
    CloseSucceeded(),
    CloseFailed(CLOSE_ERROR),
)


@pytest.mark.parametrize("state", STATES, ids=[type(state).__name__ for state in STATES])
@pytest.mark.parametrize("event", EVENTS, ids=[type(event).__name__ for event in EVENTS])
def test_every_state_event_pair_is_pure_and_total(
    state: AttemptWriterMachineState, event: AttemptWriterMachineEvent
) -> None:
    """Every closed-union pair yields a directive without mutating its input."""
    before = repr(state)

    result = transition(state, event)

    assert repr(state) == before
    assert isinstance(
        result.state,
        type(state) | Constructed | Preparing | Prepared | Reading | Finalizing | Finalized | Failed | Closing | Closed,
    )
    assert isinstance(result.directive, Admit | Ignore | Reject | ReuseClose)


def test_request_admissions_and_rejections_are_exact() -> None:
    assert transition(Constructed(), StartRequested()).state == Preparing()
    assert isinstance(transition(Constructed(), StartRequested()).directive, Admit)
    assert transition(Prepared(), ReadBeginRequested()).directive == Admit()
    assert transition(Reading(), CheckpointRequested()).directive == Admit()
    assert transition(Reading(), FinalizeRequested()) == type(transition(Reading(), FinalizeRequested()))(
        Finalizing(), Admit()
    )
    assert transition(Finalizing(), FinalizeRequested()).directive == Reject(RejectionKind.FINALIZE_PENDING)
    assert transition(Finalized(), PublishRequested()).directive == Reject(RejectionKind.FINALIZED)
    assert transition(Failed(ERROR), CheckpointRequested()).directive == Reject(RejectionKind.FAILED)
    assert transition(Closed(ERROR, True), StartRequested()).directive == Reject(RejectionKind.FAILED)


def test_worker_results_drive_the_single_lifecycle_value() -> None:
    assert transition(Preparing(), PrepareSucceeded()).state == Prepared()
    assert transition(Prepared(), ReadBeginSucceeded()).state == Reading()
    assert transition(Reading(), LegSucceeded()).state == Prepared()
    assert transition(Finalizing(), FinalizeSucceeded()).state == Finalized()
    assert transition(Reading(), CheckpointSucceeded()).state == Reading()
    assert transition(Reading(), CommandFailed(ERROR)).state == Failed(ERROR)
    assert failure_of(Failed(ERROR)) is ERROR


def test_closing_retains_first_failure_and_read_begin_can_enable_drain() -> None:
    close = transition(Prepared(), CloseRequested())
    assert close == type(close)(Closing(None, False, True), Admit())
    after_read_begin = transition(close.state, ReadBeginSucceeded())
    assert after_read_begin == type(after_read_begin)(Closing(None, True, True), Ignore())
    failed = transition(after_read_begin.state, CommandFailed(ERROR))
    assert failed == type(failed)(Closing(ERROR, False, True), Ignore())
    completed = transition(failed.state, CloseSucceeded())
    assert completed == type(completed)(Closed(ERROR, True), Ignore())
    assert transition(Closing(ERROR, True, True), ReadBeginSucceeded()) == type(completed)(
        Closing(ERROR, False, True), Ignore()
    )


def test_close_is_reused_and_close_failure_is_retained_when_first() -> None:
    pending = Closing(None, True, True)

    assert transition(pending, CloseRequested()).directive == ReuseClose()
    assert transition(pending, CloseFailed(CLOSE_ERROR)).state == Closed(CLOSE_ERROR, True)
    assert failure_of(Closed(CLOSE_ERROR, True)) is CLOSE_ERROR


def test_failure_results_are_first_wins_and_latch_in_unexpected_states() -> None:
    assert transition(Failed(ERROR), CommandFailed(CLOSE_ERROR)).state == Failed(ERROR)
    assert transition(Closed(None, False), CommandFailed(ERROR)).state == Closed(ERROR, False)
    assert transition(Constructed(), CloseFailed(CLOSE_ERROR)).state == Closed(CLOSE_ERROR, False)


@pytest.mark.parametrize(
    "state",
    (
        Preparing(),
        Prepared(),
        Reading(),
        Finalizing(),
        Finalized(),
        Closing(None, False, True),
        Closed(None, True),
    ),
)
def test_start_is_a_noop_after_prepare_admission_without_a_failure(state: AttemptWriterMachineState) -> None:
    assert transition(state, StartRequested()).directive == Ignore()


@pytest.mark.parametrize("state", (Failed(ERROR), Closing(ERROR, False, True), Closed(ERROR, True)))
def test_start_rejects_with_failed_category_when_state_retains_failure(state: AttemptWriterMachineState) -> None:
    assert transition(state, StartRequested()).directive == Reject(RejectionKind.FAILED)


@pytest.mark.parametrize(
    "state",
    (
        Closing(None, False, False),
        Closed(None, False),
        Closing(ERROR, False, False),
        Closed(ERROR, False),
    ),
)
def test_start_after_close_before_admission_keeps_closed_category(state: AttemptWriterMachineState) -> None:
    assert transition(state, StartRequested()).directive == Reject(RejectionKind.CLOSED)
