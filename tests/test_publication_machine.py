"""Bounded transition checks for ready-publication lifecycle admission."""

from __future__ import annotations

from dataclasses import FrozenInstanceError
from typing import TypeAliasType, cast, get_args, get_type_hints

import pytest

from omi_collector.capture.domain.ready_machine import (
    CaptureBegin,
    CaptureEnd,
    Compacting,
    CompactionDue,
    CompactionFinished,
    CompactionProbeFailed,
    Finished,
    Idle,
    InputChanged,
    PublicationAction,
    PublicationEvent,
    PublicationMode,
    PublicationResult,
    PublicationState,
    Quiesced,
    Quiescing,
    RetryWait,
    Running,
    Settled,
    Shutdown,
    TimerFired,
    Wake,
    publication_transition,
)

type PublicationWork = Idle | Running | Settled | RetryWait | Compacting | Quiescing


def test_declared_work_states_admit_only_the_expected_wake_action() -> None:
    outcome = object()
    rows = (
        ("idle", PublicationState(), Wake("r1", 1.0), PublicationAction.RUN),
        (
            "running-same-input",
            PublicationState(work=Running(1, "r1", 0), next_token=1),
            Wake("r1", 1.0),
            PublicationAction.NONE,
        ),
        (
            "running-changed-input",
            PublicationState(work=Running(1, "r1", 0), next_token=1),
            Wake("r2", 1.0),
            PublicationAction.NONE,
        ),
        (
            "settled-same-input",
            PublicationState(work=Settled("r1", outcome)),
            Wake("r1", 1.0),
            PublicationAction.NONE,
        ),
        (
            "settled-changed-input",
            PublicationState(work=Settled("r1", outcome)),
            Wake("r2", 1.0),
            PublicationAction.RUN,
        ),
        (
            "retry-same-before-deadline",
            PublicationState(work=RetryWait("r1", outcome, 5.0, 1)),
            Wake("r1", 4.0),
            PublicationAction.ARM,
        ),
        (
            "retry-changed-input",
            PublicationState(work=RetryWait("r1", outcome, 5.0, 1)),
            Wake("r2", 4.0),
            PublicationAction.RUN,
        ),
        (
            "unprobed-retry-before-deadline",
            PublicationState(work=RetryWait("r1", outcome, 5.0, 1)),
            Wake(None, 4.0),
            PublicationAction.ARM,
        ),
        (
            "capture",
            PublicationState(mode=PublicationMode.CAPTURE),
            Wake("r1", 1.0),
            PublicationAction.NONE,
        ),
        (
            "closed",
            PublicationState(mode=PublicationMode.CLOSED),
            Wake("r1", 1.0),
            PublicationAction.NONE,
        ),
    )

    for name, state, event, expected in rows:
        next_state, command = publication_transition(state, event)
        assert command.action is expected, name
        if expected is PublicationAction.NONE:
            assert command.token is None, name
        if expected is PublicationAction.RUN:
            assert command.token == state.next_token + 1, name
            assert isinstance(next_state.work, Running), name
            assert next_state.next_token == command.token, name
        if (
            isinstance(state.work, RetryWait)
            and event.revision == state.work.revision
            and event.now < state.work.deadline
        ):
            assert command.deadline == state.work.deadline, name


def test_publication_event_work_and_result_inventories_are_explicit() -> None:
    event_types = {
        Wake,
        InputChanged,
        Finished,
        CompactionDue,
        CompactionFinished,
        CompactionProbeFailed,
        TimerFired,
        CaptureBegin,
        CaptureEnd,
        Shutdown,
        Quiesced,
    }
    production_events = _type_alias_members(cast(TypeAliasType, PublicationEvent))
    production_work = set(get_args(get_type_hints(PublicationState)["work"]))

    assert production_events == event_types
    assert production_work == {Idle, Running, Settled, RetryWait, Compacting, Quiescing}
    assert {type(event) for event in _matrix_events(object())} == production_events
    assert {type(work) for _name, work in _matrix_work_phases(object())} == production_work
    assert tuple(PublicationResult) == (PublicationResult.SETTLED, PublicationResult.TRANSIENT)

    with pytest.raises(ValueError, match="unsupported publication event"):
        publication_transition(PublicationState(), cast(PublicationEvent, object()))
    running = PublicationState(work=Running(1, "rev", 0), next_token=1)
    with pytest.raises(ValueError, match="unsupported publication result"):
        publication_transition(running, Finished(1, "rev", object(), cast(PublicationResult, "future"), 1.0))


def _type_alias_members(alias: TypeAliasType) -> set[type[object]]:
    members = cast(tuple[object, ...], get_args(cast(object, alias.__value__)))
    assert members
    result: set[type[object]] = set()
    for member in members:
        assert isinstance(member, type)
        result.add(cast(type[object], member))
    return result


def _matrix_events(outcome: object) -> tuple[PublicationEvent, ...]:
    return (
        Wake("rev", 4.0),
        InputChanged(),
        Finished(3, "rev", outcome, PublicationResult.SETTLED, 5.0),
        CompactionDue("rev", 7),
        CompactionFinished(3, 7, "rev", PublicationResult.SETTLED, 5.0),
        TimerFired(0, 5.0, 5.0),
        CaptureBegin(),
        CaptureEnd(),
        Shutdown(),
        Quiesced(),
        CompactionProbeFailed("rev", 5.0, (2.0,)),
    )


def _matrix_work_phases(outcome: object) -> tuple[tuple[str, PublicationWork], ...]:
    return (
        ("idle", Idle()),
        ("running", Running(3, "rev", 0)),
        ("settled", Settled("rev", outcome)),
        ("retry-wait", RetryWait("rev", outcome, 5.0, 1)),
        ("compacting", Compacting(3, "rev", 0, 7)),
        ("quiescing", Quiescing(3, "rev", 0, 7)),
    )


def _matrix_expected_actions() -> dict[tuple[PublicationMode, str], tuple[PublicationAction | None, ...]]:
    none = PublicationAction.NONE
    cancel = PublicationAction.CANCEL_AND_JOIN
    check = PublicationAction.CHECK_INPUT
    run = PublicationAction.RUN
    arm = PublicationAction.ARM
    compact = PublicationAction.COMPACT
    invalid = None
    # Each row lists independent expected commands in event order above.
    return {
        (PublicationMode.AVAILABLE, "idle"): (run, check, none, compact, none, none, cancel, none, cancel, none, arm),
        (PublicationMode.AVAILABLE, "running"): (none, none, none, none, none, none, cancel, none, cancel, none, none),
        (PublicationMode.AVAILABLE, "settled"): (
            none,
            check,
            none,
            compact,
            none,
            none,
            cancel,
            none,
            cancel,
            none,
            arm,
        ),
        (PublicationMode.AVAILABLE, "retry-wait"): (
            arm,
            check,
            none,
            none,
            none,
            check,
            cancel,
            none,
            cancel,
            none,
            none,
        ),
        (PublicationMode.AVAILABLE, "compacting"): (
            none,
            none,
            none,
            none,
            none,
            none,
            cancel,
            none,
            cancel,
            none,
            none,
        ),
        (PublicationMode.AVAILABLE, "quiescing"): (
            none,
            none,
            none,
            none,
            none,
            none,
            cancel,
            none,
            cancel,
            none,
            none,
        ),
        (PublicationMode.CAPTURE, "idle"): (none, none, none, none, none, none, invalid, check, cancel, none, none),
        (PublicationMode.CAPTURE, "running"): (none, none, none, none, none, none, invalid, check, cancel, none, none),
        (PublicationMode.CAPTURE, "settled"): (none, none, none, none, none, none, invalid, check, cancel, none, none),
        (PublicationMode.CAPTURE, "retry-wait"): (
            none,
            none,
            none,
            none,
            none,
            none,
            invalid,
            check,
            cancel,
            none,
            none,
        ),
        (PublicationMode.CAPTURE, "compacting"): (
            none,
            none,
            none,
            none,
            none,
            none,
            invalid,
            none,
            cancel,
            none,
            none,
        ),
        (PublicationMode.CAPTURE, "quiescing"): (none, none, none, none, none, none, invalid, none, cancel, none, none),
        (PublicationMode.CLOSED, "idle"): (none, none, none, none, none, none, invalid, none, cancel, none, none),
        (PublicationMode.CLOSED, "running"): (none, none, none, none, none, none, invalid, none, cancel, none, none),
        (PublicationMode.CLOSED, "settled"): (none, none, none, none, none, none, invalid, none, cancel, none, none),
        (PublicationMode.CLOSED, "retry-wait"): (none, none, none, none, none, none, invalid, none, cancel, none, none),
        (PublicationMode.CLOSED, "compacting"): (none, none, none, none, none, none, invalid, none, cancel, none, none),
        (PublicationMode.CLOSED, "quiescing"): (none, none, none, none, none, none, invalid, none, cancel, none, none),
    }


def _evaluate_lifecycle_matrix() -> tuple[int, int]:
    """Evaluate 198 mode, phase, and event cells against independent expectations."""
    events = _matrix_events(object())
    expected = _matrix_expected_actions()
    invalid = None
    successful_cells = 0
    invalid_cells = 0
    for mode in PublicationMode:
        for phase, work in _matrix_work_phases(object()):
            state = PublicationState(mode=mode, work=work, generation=0, next_token=3)
            row = expected[(mode, phase)]
            for event, expected_action in zip(events, row, strict=True):
                if expected_action is invalid:
                    with pytest.raises(ValueError, match="capture cannot begin"):
                        publication_transition(state, event)
                    invalid_cells += 1
                    continue
                _, command = publication_transition(state, event)
                assert command.action is expected_action, (mode, phase, type(event).__name__)
                successful_cells += 1
    return successful_cells, invalid_cells


def test_bounded_lifecycle_matrix_covers_all_mode_work_and_event_cells() -> None:
    """Direct representative states cover 3 modes x 6 work phases x 11 event families."""
    successful_cells, invalid_cells = _evaluate_lifecycle_matrix()
    assert successful_cells == 186
    assert invalid_cells == 12
    assert successful_cells + invalid_cells == 198


def test_compaction_admission_settlement_retry_and_stale_completion() -> None:
    started, command = publication_transition(PublicationState(), CompactionDue("rev", 8))
    assert command.action is PublicationAction.COMPACT
    assert (command.token, command.debt_generation) == (1, 8)
    assert started.work == Compacting(1, "rev", 0, 8)

    for event in (Wake("other", 1.0), Wake(None, 1.0), InputChanged(), CompactionDue("rev", 8)):
        unchanged, command = publication_transition(started, event)
        assert command.action is PublicationAction.NONE
        assert unchanged.work == started.work

    stale, command = publication_transition(started, CompactionFinished(2, 8, "rev", PublicationResult.SETTLED, 2.0))
    assert stale is started and command.action is PublicationAction.NONE

    changed_debt, command = publication_transition(
        started, CompactionFinished(1, 9, "rev", PublicationResult.SETTLED, 2.0)
    )
    assert command.action is PublicationAction.CHECK_INPUT
    assert isinstance(changed_debt.work, Idle)

    retry, command = publication_transition(
        started, CompactionFinished(1, 8, "rev", PublicationResult.TRANSIENT, 2.0, (3.0,))
    )
    assert command.action is PublicationAction.ARM and command.deadline == 5.0
    assert isinstance(retry.work, RetryWait) and retry.work.failure_count == 1

    settled, command = publication_transition(started, CompactionFinished(1, 8, "rev", PublicationResult.SETTLED, 2.0))
    assert command.action is PublicationAction.NONE
    assert settled.work == Settled("rev", PublicationResult.SETTLED)


def test_compaction_probe_failure_uses_existing_retry_wait() -> None:
    event = CompactionProbeFailed("rev", 4.0, (2.5, 5.0))
    for work in (Idle(), Settled("rev", "published")):
        state = PublicationState(work=work)
        retry, command = publication_transition(state, event)
        assert command.action is PublicationAction.ARM and command.deadline == 6.5
        assert retry.work == RetryWait("rev", PublicationResult.TRANSIENT, 6.5, 1)


def test_compaction_completion_checks_for_input_changed_during_work() -> None:
    compacting, _ = publication_transition(PublicationState(), CompactionDue("rev", 8))
    dirty, command = publication_transition(compacting, InputChanged())
    assert isinstance(dirty.work, Compacting)
    assert command.action is PublicationAction.NONE and dirty.needs_check

    settled, command = publication_transition(dirty, CompactionFinished(1, 8, "rev", PublicationResult.SETTLED, 2.0))
    assert command.action is PublicationAction.CHECK_INPUT
    assert settled.mode is PublicationMode.AVAILABLE
    assert settled.work == Settled("rev", PublicationResult.SETTLED)
    assert settled.needs_check

    compacting, _ = publication_transition(PublicationState(), CompactionDue("rev", 8))
    dirty, _ = publication_transition(compacting, InputChanged())
    waiting, command = publication_transition(
        dirty, CompactionFinished(1, 8, "rev", PublicationResult.TRANSIENT, 2.0, (3.0,))
    )
    assert command.action is PublicationAction.ARM and command.deadline == 5.0
    assert isinstance(waiting.work, RetryWait)
    assert not waiting.needs_check

    due, command = publication_transition(waiting, TimerFired(waiting.generation, 5.0, 5.0))
    assert command.action is PublicationAction.CHECK_INPUT
    assert due.needs_check


def test_same_revision_wake_consumes_deferred_check_but_changed_revision_runs() -> None:
    compacting, _ = publication_transition(PublicationState(), CompactionDue("rev", 8))
    dirty, _ = publication_transition(compacting, InputChanged())
    settled, command = publication_transition(dirty, CompactionFinished(1, 8, "rev", PublicationResult.SETTLED, 2.0))
    assert command.action is PublicationAction.CHECK_INPUT and settled.needs_check

    checked, command = publication_transition(settled, Wake("rev", 3.0))
    assert command.action is PublicationAction.NONE
    assert checked.work == settled.work
    assert not checked.needs_check

    changed, command = publication_transition(checked, Wake("new-rev", 4.0))
    assert command.action is PublicationAction.RUN
    assert isinstance(changed.work, Running)
    assert changed.work.revision == "new-rev"


def test_capture_quiesces_compaction_and_shutdown_cancellation_is_idempotent() -> None:
    compacting, _ = publication_transition(PublicationState(), CompactionDue("rev", 8))
    capturing, command = publication_transition(compacting, CaptureBegin())
    assert capturing.mode is PublicationMode.CAPTURE
    assert capturing.work == Quiescing(1, "rev", 0, 8)
    assert command.action is PublicationAction.CANCEL_AND_JOIN

    repeated, command = publication_transition(capturing, Shutdown())
    assert repeated.mode is PublicationMode.CLOSED
    assert repeated.work == capturing.work
    assert command.action is PublicationAction.CANCEL_AND_JOIN
    again, command = publication_transition(repeated, Shutdown())
    assert again == repeated
    assert command.action is PublicationAction.CANCEL_AND_JOIN

    finished, command = publication_transition(
        repeated, CompactionFinished(1, 8, "rev", PublicationResult.SETTLED, 2.0)
    )
    assert finished.mode is PublicationMode.CLOSED
    assert finished.work is repeated.work
    assert command.action is PublicationAction.NONE

    quiesced, command = publication_transition(repeated, Quiesced())
    assert quiesced.mode is PublicationMode.CLOSED and isinstance(quiesced.work, Idle)
    assert command.action is PublicationAction.NONE


def test_running_work_is_single_token_and_stale_completion_requests_one_input_check() -> None:
    state, start = publication_transition(PublicationState(), Wake("old", 0.0))
    assert start.action is PublicationAction.RUN and start.token == 1
    original = state

    same_state, repeated = publication_transition(state, Wake("old", 1.0))
    assert repeated.action is PublicationAction.NONE
    assert same_state is state

    dirty, changed = publication_transition(state, Wake("new", 2.0))
    assert changed.action is PublicationAction.NONE
    assert isinstance(dirty.work, Running) and dirty.work.token == 1
    assert dirty.generation == original.generation + 1
    assert dirty.needs_check

    stale, followup = publication_transition(
        dirty,
        Finished(1, "old", "published", PublicationResult.SETTLED, 3.0),
    )
    assert followup.action is PublicationAction.CHECK_INPUT
    assert isinstance(stale.work, Idle)
    assert stale.needs_check

    ignored, wrong_token = publication_transition(
        state,
        Finished(2, "old", "published", PublicationResult.SETTLED, 3.0),
    )
    assert wrong_token.action is PublicationAction.NONE
    assert ignored is state


def _start_retry_wait(running: PublicationState, now: float, backoff: tuple[float, ...]) -> PublicationState:
    assert isinstance(running.work, Running)
    finished, command = publication_transition(
        running,
        Finished(running.work.token, "same", "busy", PublicationResult.TRANSIENT, now, backoff),
    )
    assert command.action is PublicationAction.ARM
    assert isinstance(finished.work, RetryWait)
    return finished


def _wait_out_retry(waiting: PublicationState, now: float, backoff: tuple[float, ...]) -> PublicationState:
    assert isinstance(waiting.work, RetryWait)
    timer = TimerFired(waiting.generation, waiting.work.deadline, now)
    due, command = publication_transition(waiting, timer)
    assert command.action is PublicationAction.CHECK_INPUT
    retrying, command = publication_transition(due, Wake("same", now))
    assert command.action is PublicationAction.RUN
    return _start_retry_wait(retrying, now, backoff)


def _assert_retry_deadline_is_stable(waiting: PublicationState) -> None:
    assert isinstance(waiting.work, RetryWait)
    for wake in (Wake("same", 10.5), Wake(None, 11.5)):
        unchanged, command = publication_transition(waiting, wake)
        assert command.action is PublicationAction.ARM
        assert command.deadline == 12.0
        assert unchanged.work == waiting.work

    early, command = publication_transition(waiting, TimerFired(waiting.generation, 12.0, 11.9))
    assert command.action is PublicationAction.ARM
    assert command.deadline == 12.0
    assert early.work == waiting.work

    for stale_timer in (
        TimerFired(waiting.generation + 1, 12.0, 12.0),
        TimerFired(waiting.generation, 11.0, 12.0),
    ):
        stale, command = publication_transition(waiting, stale_timer)
        assert command.action is PublicationAction.NONE
        assert stale is waiting


def test_transient_wakes_keep_one_absolute_deadline_and_retry_count() -> None:
    backoff = (2.0, 4.0)
    started, command = publication_transition(PublicationState(), Wake("same", 0.0))
    assert command.action is PublicationAction.RUN
    first_wait = _start_retry_wait(started, 10.0, backoff)
    assert isinstance(first_wait.work, RetryWait)
    assert first_wait.work.deadline == 12.0
    assert first_wait.work.failure_count == 1
    _assert_retry_deadline_is_stable(first_wait)

    second_wait = _wait_out_retry(first_wait, 12.0, backoff)
    assert isinstance(second_wait.work, RetryWait)
    assert second_wait.work.deadline == 16.0
    assert second_wait.work.failure_count == 2

    third_wait = _wait_out_retry(second_wait, 16.0, backoff)
    assert isinstance(third_wait.work, RetryWait)
    assert third_wait.work.deadline == 20.0
    assert third_wait.work.failure_count == 3


def test_matching_successful_completion_settles_the_revision() -> None:
    running, start = publication_transition(PublicationState(), Wake("rev", 1.0))
    assert start.action is PublicationAction.RUN and start.token == 1

    settled, command = publication_transition(
        running,
        Finished(1, "rev", "published", PublicationResult.SETTLED, 2.0),
    )

    assert command.action is PublicationAction.NONE
    assert isinstance(settled.work, Settled)
    assert settled.work.revision == "rev"
    assert settled.work.outcome == "published"


def test_capture_cancels_then_quiesces_before_a_new_input_check() -> None:
    running, _ = publication_transition(PublicationState(), Wake("r1", 0.0))
    capturing, cancel = publication_transition(running, CaptureBegin())
    assert capturing.mode is PublicationMode.CAPTURE
    assert cancel.action is PublicationAction.CANCEL_AND_JOIN
    for event in (Wake("r1", 1.0), Wake(None, 1.0)):
        _, command = publication_transition(capturing, event)
        assert command.action is PublicationAction.NONE

    quiesced, command = publication_transition(capturing, Quiesced())
    assert quiesced.mode is PublicationMode.CAPTURE
    assert isinstance(quiesced.work, Idle)
    assert command.action is PublicationAction.NONE
    available, ended = publication_transition(quiesced, CaptureEnd())
    assert available.mode is PublicationMode.AVAILABLE
    assert available.needs_check
    assert ended.action is PublicationAction.CHECK_INPUT


def test_shutdown_is_terminal_for_every_declared_work_phase() -> None:
    outcome = object()
    states = (
        PublicationState(),
        PublicationState(work=Running(1, "r", 0), next_token=1),
        PublicationState(work=Settled("r", outcome)),
        PublicationState(work=RetryWait("r", outcome, 5.0, 1)),
        PublicationState(mode=PublicationMode.CAPTURE),
    )
    for state in states:
        closed, command = publication_transition(state, Shutdown())
        assert closed.mode is PublicationMode.CLOSED
        assert command.action is PublicationAction.CANCEL_AND_JOIN
        events = [Wake("r", 10.0), InputChanged(), TimerFired(closed.generation, 5.0, 10.0), CaptureEnd()]
        if isinstance(closed.work, Running):
            events.append(Finished(closed.work.token, "r", outcome, PublicationResult.SETTLED, 10.0))
        for event in events:
            _, after = publication_transition(closed, event)
            assert after.action not in {PublicationAction.RUN, PublicationAction.ARM, PublicationAction.CHECK_INPUT}


def test_capture_begin_rejects_duplicate_or_closed_lifecycle_and_state_is_frozen() -> None:
    with pytest.raises(ValueError):
        publication_transition(PublicationState(mode=PublicationMode.CAPTURE), CaptureBegin())
    with pytest.raises(ValueError):
        publication_transition(PublicationState(mode=PublicationMode.CLOSED), CaptureBegin())
    with pytest.raises(FrozenInstanceError):
        PublicationState().mode = PublicationMode.CLOSED  # type: ignore[misc]
