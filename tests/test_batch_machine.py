from __future__ import annotations

from dataclasses import FrozenInstanceError
from itertools import product

import pytest

from omi_collector.capture.application.batch_machine import (
    _READ_TRANSITION_TABLE,
    Command,
    CursorAction,
    Event,
    Milestone,
    ReadAction,
    ReadEvent,
    ReadPhase,
    decide_read_event,
    derive_milestone,
    read_transition,
    transition,
)

_CURSORS = (
    CursorAction.REPEAT,
    CursorAction.CONFIRMED,
    CursorAction.AHEAD,
    CursorAction.REGRESSED,
    CursorAction.EXPIRED,
)
_MILESTONES = (
    Milestone.RETIRED,
    Milestone.ADMITTED,
    Milestone.PREFIX_DURABLE,
    Milestone.FULL_DURABLE,
    Milestone.SEALED,
)
_EVENTS = (
    Event.SEAL_REQUEST,
    Event.SEAL,
    Event.FRESH_INFO,
    Event.ADVANCE_ACK_INFO,
    Event.ADVANCE_UNCERTAIN,
    Event.WRITER_CLOSED,
)


def test_receipts_derive_all_batch_milestones() -> None:
    assert derive_milestone(None, None, None, False) is Milestone.RETIRED
    assert derive_milestone(10, 12, 10, False) is Milestone.ADMITTED
    assert derive_milestone(10, 12, 11, False) is Milestone.PREFIX_DURABLE
    assert derive_milestone(10, 12, 12, False) is Milestone.FULL_DURABLE
    assert derive_milestone(10, 12, 12, True) is Milestone.SEALED
    for next_sequence, sealed in product((9, 13), (False, True)):
        with pytest.raises(ValueError):
            derive_milestone(10, 12, next_sequence, sealed)
    with pytest.raises(ValueError):
        derive_milestone(10, 12, 11, True)
    with pytest.raises(ValueError):
        derive_milestone(None, None, 10, False)
    for start, end in ((None, 12), (10, None)):
        with pytest.raises(ValueError, match="retired batch cannot retain writer receipts"):
            derive_milestone(start, end, None, False)


def test_transition_matrix_rejects_every_unlisted_pair() -> None:
    assert tuple(Milestone) == _MILESTONES
    assert tuple(Event) == _EVENTS
    assert tuple(CursorAction) == _CURSORS
    allowed: dict[tuple[Milestone, Event, CursorAction | None], tuple[Milestone, Command]] = {
        (Milestone.FULL_DURABLE, Event.SEAL_REQUEST, None): (Milestone.FULL_DURABLE, Command.NONE),
        (Milestone.FULL_DURABLE, Event.SEAL, None): (Milestone.SEALED, Command.NONE),
        (Milestone.SEALED, Event.ADVANCE_UNCERTAIN, None): (Milestone.SEALED, Command.KEEP),
        (Milestone.SEALED, Event.WRITER_CLOSED, None): (Milestone.RETIRED, Command.RETIRE),
    }
    for action in _CURSORS:
        allowed[Milestone.SEALED, Event.FRESH_INFO, action] = (
            Milestone.SEALED,
            Command.ADVANCE
            if action is CursorAction.REPEAT
            else Command.REJECT
            if action in (CursorAction.REGRESSED, CursorAction.EXPIRED)
            else Command.CLOSE,
        )
        allowed[Milestone.SEALED, Event.ADVANCE_ACK_INFO, action] = (
            Milestone.SEALED,
            Command.KEEP
            if action is CursorAction.REPEAT
            else Command.REJECT
            if action in (CursorAction.REGRESSED, CursorAction.EXPIRED)
            else Command.CLOSE,
        )

    for milestone, event, cursor in product(_MILESTONES, _EVENTS, (None, *_CURSORS)):
        expected = allowed.get((milestone, event, cursor))
        if expected is None:
            with pytest.raises(ValueError):
                transition(milestone, event, cursor)
        else:
            result = transition(milestone, event, cursor)
            assert (result.milestone, result.command) == expected


def test_legal_lifecycle_reaches_retired_only_after_close() -> None:
    admitted = derive_milestone(10, 12, 10, False)
    prefix = derive_milestone(10, 12, 11, False)
    full = derive_milestone(10, 12, 12, False)
    assert admitted is Milestone.ADMITTED
    assert prefix is Milestone.PREFIX_DURABLE
    sealed = transition(full, Event.SEAL).milestone

    assert transition(sealed, Event.FRESH_INFO, CursorAction.REPEAT).command is Command.ADVANCE
    assert transition(sealed, Event.FRESH_INFO, CursorAction.EXPIRED).command is Command.REJECT
    assert transition(sealed, Event.FRESH_INFO, CursorAction.REGRESSED).command is Command.REJECT
    assert transition(sealed, Event.ADVANCE_UNCERTAIN).command is Command.KEEP
    assert transition(sealed, Event.ADVANCE_ACK_INFO, CursorAction.REPEAT).command is Command.KEEP
    assert transition(sealed, Event.ADVANCE_ACK_INFO, CursorAction.EXPIRED).command is Command.REJECT
    assert transition(sealed, Event.ADVANCE_ACK_INFO, CursorAction.REGRESSED).command is Command.REJECT
    assert transition(sealed, Event.ADVANCE_ACK_INFO, CursorAction.CONFIRMED).command is Command.CLOSE
    assert transition(sealed, Event.WRITER_CLOSED).milestone is Milestone.RETIRED


def test_returned_transition_is_immutable_and_shared_transition_stays_sealed() -> None:
    result = transition(Milestone.FULL_DURABLE, Event.SEAL)

    assert (result.milestone, result.command) == (Milestone.SEALED, Command.NONE)
    with pytest.raises(FrozenInstanceError):
        result.__setattr__("command", Command.ADVANCE)

    repeated = transition(Milestone.FULL_DURABLE, Event.SEAL)
    assert (repeated.milestone, repeated.command) == (Milestone.SEALED, Command.NONE)


def test_unknown_tags_are_rejected() -> None:
    with pytest.raises(ValueError, match="unknown cursor"):
        transition(Milestone.SEALED, Event.FRESH_INFO, "future-cursor")  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="cursor classification is required"):
        transition(Milestone.SEALED, Event.FRESH_INFO)
    with pytest.raises(ValueError, match="unknown batch"):
        transition("future-milestone", Event.SEAL)  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="unknown batch"):
        transition(Milestone.SEALED, "future-event")  # type: ignore[arg-type]


def test_read_transition_is_total_and_typed() -> None:
    assert len(tuple(ReadPhase)) == 5
    assert len(tuple(ReadEvent)) == 10
    assert len(tuple(ReadAction)) == 9
    decisions = [read_transition(phase, event) for phase, event in product(ReadPhase, ReadEvent)]
    assert set(_READ_TRANSITION_TABLE) == set(product(ReadPhase, ReadEvent))
    assert len(decisions) == 50
    assert all(isinstance(decision.next_phase, ReadPhase) for decision in decisions)
    assert all(isinstance(decision.action, ReadAction) for decision in decisions)
    assert all(decision.reason for decision in decisions)


def test_read_transition_handles_begin_completion_and_failures() -> None:
    assert read_transition(ReadPhase.AWAIT_BEGIN, ReadEvent.ACK).action is ReadAction.CONSUME
    assert read_transition(ReadPhase.AWAIT_BEGIN, ReadEvent.BEGIN).next_phase is ReadPhase.RECEIVING
    assert read_transition(ReadPhase.AWAIT_BEGIN, ReadEvent.DATA).action is ReadAction.REJECT
    assert read_transition(ReadPhase.RECEIVING, ReadEvent.BEGIN).action is ReadAction.REJECT
    assert read_transition(ReadPhase.RECEIVING, ReadEvent.DONE).action is ReadAction.BARRIER
    assert read_transition(ReadPhase.DONE, ReadEvent.BARRIER_OK).action is ReadAction.COMPLETE
    assert read_transition(ReadPhase.DONE, ReadEvent.BARRIER_FAILED).action is ReadAction.PROPAGATE
    assert read_transition(ReadPhase.STOPPING, ReadEvent.CANCELLED).action is ReadAction.LATCH_CANCELLATION
    assert read_transition(ReadPhase.STOPPING, ReadEvent.STOP_SETTLED).next_phase is ReadPhase.EXITED


def test_read_event_priority_preserves_latched_writer_failure() -> None:
    events = (ReadEvent.DATA, ReadEvent.DONE, ReadEvent.CANCELLED)
    assert decide_read_event(events, writer_failed=True) is ReadEvent.WRITER_FAILED
    assert decide_read_event((*events, ReadEvent.WRITER_FAILED)) is ReadEvent.WRITER_FAILED
    assert decide_read_event((ReadEvent.DATA, ReadEvent.DONE)) is ReadEvent.DATA
    assert decide_read_event(()) is None
