"""Pure lifecycle rules derived from batch receipts and command outcomes."""

from __future__ import annotations

from collections.abc import Iterable
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


class ReadPhase(StrEnum):
    AWAIT_BEGIN = "await_begin"
    RECEIVING = "receiving"
    DONE = "done"
    STOPPING = "stopping"
    EXITED = "exited"


class ReadEvent(StrEnum):
    ACK = "ack"
    BEGIN = "begin"
    DATA = "data"
    DONE = "done"
    WRITER_FAILED = "writer_failed"
    READ_FAILED = "read_failed"
    CANCELLED = "cancelled"
    STOP_SETTLED = "stop_settled"
    BARRIER_OK = "barrier_ok"
    BARRIER_FAILED = "barrier_failed"


class ReadAction(StrEnum):
    CONSUME = "consume"
    BARRIER = "barrier"
    STOP = "stop"
    LATCH_FAILURE = "latch_failure"
    PROPAGATE = "propagate"
    COMPLETE = "complete"
    REJECT = "reject"
    IGNORE = "ignore"


@dataclass(frozen=True, slots=True)
class ReadDecision:
    next_phase: ReadPhase
    action: ReadAction
    reason: str


def _read_decision(next_phase: ReadPhase, action: ReadAction, reason: str) -> ReadDecision:
    return ReadDecision(next_phase, action, reason)


_READ_TRANSITION_TABLE = {
    (ReadPhase.AWAIT_BEGIN, ReadEvent.ACK): _read_decision(
        ReadPhase.AWAIT_BEGIN, ReadAction.CONSUME, "acknowledged before read begin"
    ),
    (ReadPhase.AWAIT_BEGIN, ReadEvent.BEGIN): _read_decision(ReadPhase.RECEIVING, ReadAction.CONSUME, "read began"),
    (ReadPhase.AWAIT_BEGIN, ReadEvent.DATA): _read_decision(
        ReadPhase.AWAIT_BEGIN, ReadAction.REJECT, "data arrived before read begin"
    ),
    (ReadPhase.AWAIT_BEGIN, ReadEvent.DONE): _read_decision(
        ReadPhase.AWAIT_BEGIN, ReadAction.REJECT, "done arrived before read begin"
    ),
    (ReadPhase.AWAIT_BEGIN, ReadEvent.WRITER_FAILED): _read_decision(
        ReadPhase.STOPPING, ReadAction.STOP, "writer failed before read began"
    ),
    (ReadPhase.AWAIT_BEGIN, ReadEvent.READ_FAILED): _read_decision(
        ReadPhase.STOPPING, ReadAction.STOP, "read failed before read began"
    ),
    (ReadPhase.AWAIT_BEGIN, ReadEvent.CANCELLED): _read_decision(
        ReadPhase.STOPPING, ReadAction.STOP, "read cancelled before read began"
    ),
    (ReadPhase.AWAIT_BEGIN, ReadEvent.STOP_SETTLED): _read_decision(
        ReadPhase.AWAIT_BEGIN, ReadAction.REJECT, "stop settled before stopping"
    ),
    (ReadPhase.AWAIT_BEGIN, ReadEvent.BARRIER_OK): _read_decision(
        ReadPhase.AWAIT_BEGIN, ReadAction.REJECT, "barrier completed before read"
    ),
    (ReadPhase.AWAIT_BEGIN, ReadEvent.BARRIER_FAILED): _read_decision(
        ReadPhase.AWAIT_BEGIN, ReadAction.REJECT, "barrier failed before read"
    ),
    (ReadPhase.RECEIVING, ReadEvent.ACK): _read_decision(
        ReadPhase.RECEIVING, ReadAction.CONSUME, "acknowledged while receiving"
    ),
    (ReadPhase.RECEIVING, ReadEvent.BEGIN): _read_decision(
        ReadPhase.RECEIVING, ReadAction.REJECT, "duplicate read begin"
    ),
    (ReadPhase.RECEIVING, ReadEvent.DATA): _read_decision(ReadPhase.RECEIVING, ReadAction.CONSUME, "data received"),
    (ReadPhase.RECEIVING, ReadEvent.DONE): _read_decision(
        ReadPhase.DONE, ReadAction.BARRIER, "read completed; await barrier"
    ),
    (ReadPhase.RECEIVING, ReadEvent.WRITER_FAILED): _read_decision(
        ReadPhase.STOPPING, ReadAction.STOP, "writer failed while receiving"
    ),
    (ReadPhase.RECEIVING, ReadEvent.READ_FAILED): _read_decision(
        ReadPhase.STOPPING, ReadAction.STOP, "read failed while receiving"
    ),
    (ReadPhase.RECEIVING, ReadEvent.CANCELLED): _read_decision(
        ReadPhase.STOPPING, ReadAction.STOP, "read cancelled while receiving"
    ),
    (ReadPhase.RECEIVING, ReadEvent.STOP_SETTLED): _read_decision(
        ReadPhase.RECEIVING, ReadAction.REJECT, "stop settled before stopping"
    ),
    (ReadPhase.RECEIVING, ReadEvent.BARRIER_OK): _read_decision(
        ReadPhase.RECEIVING, ReadAction.REJECT, "barrier completed while receiving"
    ),
    (ReadPhase.RECEIVING, ReadEvent.BARRIER_FAILED): _read_decision(
        ReadPhase.RECEIVING, ReadAction.REJECT, "barrier failed while receiving"
    ),
    (ReadPhase.DONE, ReadEvent.ACK): _read_decision(
        ReadPhase.DONE, ReadAction.REJECT, "late acknowledgement after read completion"
    ),
    (ReadPhase.DONE, ReadEvent.BEGIN): _read_decision(
        ReadPhase.DONE, ReadAction.REJECT, "read begin repeated after completion"
    ),
    (ReadPhase.DONE, ReadEvent.DATA): _read_decision(
        ReadPhase.DONE, ReadAction.REJECT, "data arrived after read completion"
    ),
    (ReadPhase.DONE, ReadEvent.DONE): _read_decision(ReadPhase.DONE, ReadAction.REJECT, "duplicate read completion"),
    (ReadPhase.DONE, ReadEvent.WRITER_FAILED): _read_decision(
        ReadPhase.EXITED, ReadAction.PROPAGATE, "writer failed after read completion"
    ),
    (ReadPhase.DONE, ReadEvent.READ_FAILED): _read_decision(
        ReadPhase.DONE, ReadAction.REJECT, "late read failure after completion"
    ),
    (ReadPhase.DONE, ReadEvent.CANCELLED): _read_decision(
        ReadPhase.EXITED, ReadAction.PROPAGATE, "read cancelled after completion"
    ),
    (ReadPhase.DONE, ReadEvent.STOP_SETTLED): _read_decision(
        ReadPhase.DONE, ReadAction.REJECT, "stop settled before barrier"
    ),
    (ReadPhase.DONE, ReadEvent.BARRIER_OK): _read_decision(ReadPhase.EXITED, ReadAction.COMPLETE, "barrier completed"),
    (ReadPhase.DONE, ReadEvent.BARRIER_FAILED): _read_decision(
        ReadPhase.EXITED, ReadAction.PROPAGATE, "barrier failed"
    ),
    (ReadPhase.STOPPING, ReadEvent.ACK): _read_decision(
        ReadPhase.STOPPING, ReadAction.IGNORE, "late acknowledgement while stopping"
    ),
    (ReadPhase.STOPPING, ReadEvent.BEGIN): _read_decision(
        ReadPhase.STOPPING, ReadAction.IGNORE, "late read begin while stopping"
    ),
    (ReadPhase.STOPPING, ReadEvent.DATA): _read_decision(
        ReadPhase.STOPPING, ReadAction.IGNORE, "late data while stopping"
    ),
    (ReadPhase.STOPPING, ReadEvent.DONE): _read_decision(
        ReadPhase.STOPPING, ReadAction.IGNORE, "late read completion while stopping"
    ),
    (ReadPhase.STOPPING, ReadEvent.WRITER_FAILED): _read_decision(
        ReadPhase.STOPPING, ReadAction.LATCH_FAILURE, "writer failure already latched"
    ),
    (ReadPhase.STOPPING, ReadEvent.READ_FAILED): _read_decision(
        ReadPhase.STOPPING, ReadAction.IGNORE, "read failure already latched"
    ),
    (ReadPhase.STOPPING, ReadEvent.CANCELLED): _read_decision(
        ReadPhase.STOPPING, ReadAction.IGNORE, "cancellation already latched"
    ),
    (ReadPhase.STOPPING, ReadEvent.STOP_SETTLED): _read_decision(
        ReadPhase.EXITED, ReadAction.PROPAGATE, "stop settled after failure"
    ),
    (ReadPhase.STOPPING, ReadEvent.BARRIER_OK): _read_decision(
        ReadPhase.STOPPING, ReadAction.REJECT, "barrier completion while stopping"
    ),
    (ReadPhase.STOPPING, ReadEvent.BARRIER_FAILED): _read_decision(
        ReadPhase.STOPPING, ReadAction.REJECT, "barrier failure while stopping"
    ),
    (ReadPhase.EXITED, ReadEvent.ACK): _read_decision(
        ReadPhase.EXITED, ReadAction.REJECT, "acknowledgement after read exit"
    ),
    (ReadPhase.EXITED, ReadEvent.BEGIN): _read_decision(
        ReadPhase.EXITED, ReadAction.REJECT, "read begin after read exit"
    ),
    (ReadPhase.EXITED, ReadEvent.DATA): _read_decision(ReadPhase.EXITED, ReadAction.REJECT, "data after read exit"),
    (ReadPhase.EXITED, ReadEvent.DONE): _read_decision(
        ReadPhase.EXITED, ReadAction.REJECT, "read completion after read exit"
    ),
    (ReadPhase.EXITED, ReadEvent.WRITER_FAILED): _read_decision(
        ReadPhase.EXITED, ReadAction.IGNORE, "writer failure after read exit"
    ),
    (ReadPhase.EXITED, ReadEvent.READ_FAILED): _read_decision(
        ReadPhase.EXITED, ReadAction.IGNORE, "read failure after read exit"
    ),
    (ReadPhase.EXITED, ReadEvent.CANCELLED): _read_decision(
        ReadPhase.EXITED, ReadAction.IGNORE, "cancellation after read exit"
    ),
    (ReadPhase.EXITED, ReadEvent.STOP_SETTLED): _read_decision(
        ReadPhase.EXITED, ReadAction.IGNORE, "stop settlement after read exit"
    ),
    (ReadPhase.EXITED, ReadEvent.BARRIER_OK): _read_decision(
        ReadPhase.EXITED, ReadAction.IGNORE, "barrier completion after read exit"
    ),
    (ReadPhase.EXITED, ReadEvent.BARRIER_FAILED): _read_decision(
        ReadPhase.EXITED, ReadAction.IGNORE, "barrier failure after read exit"
    ),
}


def read_transition(phase: ReadPhase, event: ReadEvent) -> ReadDecision:
    """Return the explicit decision for one read lifecycle cell."""
    if not isinstance(phase, ReadPhase) or not isinstance(event, ReadEvent):
        raise ValueError("unknown read phase or event")
    try:
        return _READ_TRANSITION_TABLE[(phase, event)]
    except KeyError as error:
        raise ValueError(f"missing read transition: {phase.value} + {event.value}") from error


def decide_read_event(events: Iterable[ReadEvent], *, writer_failed: bool = False) -> ReadEvent | None:
    """Choose the event whose signal must win when callbacks arrive together."""
    pending = frozenset(events)
    if any(not isinstance(event, ReadEvent) for event in pending):
        raise ValueError("unknown read event")
    if writer_failed:
        return ReadEvent.WRITER_FAILED
    for event in (
        ReadEvent.WRITER_FAILED,
        ReadEvent.CANCELLED,
        ReadEvent.READ_FAILED,
        ReadEvent.DATA,
        ReadEvent.DONE,
        ReadEvent.ACK,
        ReadEvent.BEGIN,
        ReadEvent.STOP_SETTLED,
        ReadEvent.BARRIER_FAILED,
        ReadEvent.BARRIER_OK,
    ):
        if event in pending:
            return event
    return None


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
