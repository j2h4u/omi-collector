from __future__ import annotations

from itertools import product

import pytest

from omi_collector.capture.application.batch_machine import (
    Command,
    CursorAction,
    Event,
    Milestone,
    derive_milestone,
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
            Command.ADVANCE if action is CursorAction.REPEAT else Command.CLOSE,
        )
        allowed[Milestone.SEALED, Event.ADVANCE_ACK_INFO, action] = (
            Milestone.SEALED,
            Command.KEEP if action is CursorAction.REPEAT else Command.CLOSE,
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
    assert transition(sealed, Event.ADVANCE_UNCERTAIN).command is Command.KEEP
    assert transition(sealed, Event.ADVANCE_ACK_INFO, CursorAction.REPEAT).command is Command.KEEP
    assert transition(sealed, Event.ADVANCE_ACK_INFO, CursorAction.CONFIRMED).command is Command.CLOSE
    assert transition(sealed, Event.WRITER_CLOSED).milestone is Milestone.RETIRED


def test_unknown_tags_are_rejected() -> None:
    with pytest.raises(ValueError, match="unknown cursor"):
        transition(Milestone.SEALED, Event.FRESH_INFO, "future-cursor")  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="cursor classification is required"):
        transition(Milestone.SEALED, Event.FRESH_INFO)
    with pytest.raises(ValueError, match="unknown batch"):
        transition("future-milestone", Event.SEAL)  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="unknown batch"):
        transition(Milestone.SEALED, "future-event")  # type: ignore[arg-type]
