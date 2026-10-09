"""Pure lifecycle rules derived from batch receipts and command outcomes."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum


class Milestone(StrEnum):
    RETIRED = "retired"
    ADMITTED = "admitted"
    PREFIX_DURABLE = "prefix_durable"
    FULL_DURABLE = "full_durable"
    SEALED = "sealed"


class Event(StrEnum):
    SEAL_REQUEST = "seal_request"
    SEAL = "seal"
    FRESH_INFO = "fresh_info"
    ADVANCE_ACK_INFO = "advance_ack_info"
    ADVANCE_UNCERTAIN = "advance_uncertain"
    WRITER_CLOSED = "writer_closed"


class CursorAction(StrEnum):
    REPEAT = "repeat"
    CONFIRMED = "confirmed"
    AHEAD = "ahead"
    REGRESSED = "regressed"
    EXPIRED = "expired"


class Command(StrEnum):
    NONE = "none"
    ADVANCE = "advance"
    CLOSE = "close"
    RETIRE = "retire"
    KEEP = "keep"
    REJECT = "reject"


@dataclass(frozen=True, slots=True)
class Transition:
    milestone: Milestone
    command: Command = Command.NONE


_TRANSITIONS = {
    (Milestone.FULL_DURABLE, Event.SEAL_REQUEST): Transition(Milestone.FULL_DURABLE),
    (Milestone.FULL_DURABLE, Event.SEAL): Transition(Milestone.SEALED),
    (Milestone.SEALED, Event.ADVANCE_UNCERTAIN): Transition(Milestone.SEALED, Command.KEEP),
    (Milestone.SEALED, Event.WRITER_CLOSED): Transition(Milestone.RETIRED, Command.RETIRE),
}
_CURSOR_COMMANDS = {
    (Event.FRESH_INFO, action): (
        Command.ADVANCE
        if action is CursorAction.REPEAT
        else Command.REJECT
        if action in (CursorAction.REGRESSED, CursorAction.EXPIRED)
        else Command.CLOSE
    )
    for action in CursorAction
} | {
    (Event.ADVANCE_ACK_INFO, action): (
        Command.KEEP
        if action is CursorAction.REPEAT
        else Command.REJECT
        if action in (CursorAction.REGRESSED, CursorAction.EXPIRED)
        else Command.CLOSE
    )
    for action in CursorAction
}


def derive_milestone(
    start: int | None,
    end: int | None,
    durable_next: int | None,
    sealed: bool,
) -> Milestone:
    """Classify only receipts already present on the live batch."""
    if start is None or end is None:
        if start is not None or end is not None or durable_next is not None or sealed:
            raise ValueError("retired batch cannot retain writer receipts")
        return Milestone.RETIRED
    if end < start or durable_next is None or not start <= durable_next <= end:
        raise ValueError("batch receipts describe an impossible durable prefix")
    if sealed:
        if durable_next != end:
            raise ValueError("batch cannot be sealed before its full prefix is durable")
        return Milestone.SEALED
    if durable_next == end:
        return Milestone.FULL_DURABLE
    if durable_next > start:
        return Milestone.PREFIX_DURABLE
    return Milestone.ADMITTED


def transition(
    milestone: Milestone,
    event: Event,
    cursor: CursorAction | None = None,
) -> Transition:
    """Validate one lifecycle event and return its only legal next action."""
    if not isinstance(milestone, Milestone) or not isinstance(event, Event):
        raise ValueError("unknown batch milestone or event")
    cursor_event = event in (Event.FRESH_INFO, Event.ADVANCE_ACK_INFO)
    if cursor is not None and (not cursor_event or not isinstance(cursor, CursorAction)):
        raise ValueError("unknown cursor classification")
    if cursor_event:
        if cursor is None:
            raise ValueError("cursor classification is required")
        command = _CURSOR_COMMANDS.get((event, cursor))
        if milestone is not Milestone.SEALED or command is None:
            raise ValueError(f"invalid batch transition: {milestone.value} + {event.value}")
        return Transition(milestone, command)
    result = _TRANSITIONS.get((milestone, event))
    if result is None:
        raise ValueError(f"invalid batch transition: {milestone.value} + {event.value}")
    return result
