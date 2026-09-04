"""Pure lifecycle decisions for :mod:`attempt_writer`.

The writer driver owns all effects.  This module only maps an immutable state
and an event to a new immutable state plus one directive.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import cast


@dataclass(frozen=True, slots=True)
class Constructed:
    """No prepare command has been admitted."""


@dataclass(frozen=True, slots=True)
class Preparing:
    """A prepare command is pending its target result."""


@dataclass(frozen=True, slots=True)
class Prepared:
    """The target exists, but no successful READ_BEGIN is active."""


@dataclass(frozen=True, slots=True)
class Reading:
    """Data draining, checkpoints, and finalization are allowed."""


@dataclass(frozen=True, slots=True)
class Finalizing:
    """One finalization command is pending."""


@dataclass(frozen=True, slots=True)
class Finalized:
    """A finalization command completed successfully."""


@dataclass(frozen=True, slots=True)
class Failed:
    """The first target failure is retained."""

    error: BaseException


@dataclass(frozen=True, slots=True)
class Closing:
    """A shared close is pending, with its drain and start-admission decisions."""

    failure: BaseException | None
    drain: bool
    start_admitted: bool


@dataclass(frozen=True, slots=True)
class Closed:
    """Terminal state retaining failure and whether start was ever admitted."""

    failure: BaseException | None
    start_admitted: bool


type AttemptWriterMachineState = (
    Constructed | Preparing | Prepared | Reading | Finalizing | Finalized | Failed | Closing | Closed
)


@dataclass(frozen=True, slots=True)
class StartRequested:
    """Request target preparation."""


@dataclass(frozen=True, slots=True)
class LegRequested:
    """Request one target leg."""


@dataclass(frozen=True, slots=True)
class ReadBeginRequested:
    """Request a READ_BEGIN target call."""


@dataclass(frozen=True, slots=True)
class CheckpointRequested:
    """Request a checkpoint target call."""


@dataclass(frozen=True, slots=True)
class FinalizeRequested:
    """Request sealing or durable-prefix publication."""


@dataclass(frozen=True, slots=True)
class CloseRequested:
    """Request the single shared close."""


@dataclass(frozen=True, slots=True)
class PublishRequested:
    """Request data high-water publication without a control command."""


@dataclass(frozen=True, slots=True)
class PrepareSucceeded:
    """The target prepare call completed."""


@dataclass(frozen=True, slots=True)
class LegSucceeded:
    """The target leg call completed."""


@dataclass(frozen=True, slots=True)
class ReadBeginSucceeded:
    """The target READ_BEGIN call completed."""


@dataclass(frozen=True, slots=True)
class CheckpointSucceeded:
    """The target checkpoint call completed."""


@dataclass(frozen=True, slots=True)
class FinalizeSucceeded:
    """The target finalization call completed."""


@dataclass(frozen=True, slots=True)
class CommandFailed:
    """A non-close target call failed."""

    error: BaseException


@dataclass(frozen=True, slots=True)
class CloseSucceeded:
    """The target close call completed."""


@dataclass(frozen=True, slots=True)
class CloseFailed:
    """The target close call failed."""

    error: BaseException


type AttemptWriterMachineEvent = (
    StartRequested
    | LegRequested
    | ReadBeginRequested
    | CheckpointRequested
    | FinalizeRequested
    | CloseRequested
    | PublishRequested
    | PrepareSucceeded
    | LegSucceeded
    | ReadBeginSucceeded
    | CheckpointSucceeded
    | FinalizeSucceeded
    | CommandFailed
    | CloseSucceeded
    | CloseFailed
)


class RejectionKind(Enum):
    """Driver-mapped public rejection categories."""

    NOT_STARTED = "not_started"
    PREPARE_PENDING = "prepare_pending"
    READ_PENDING = "read_pending"
    FINALIZING = "finalizing"
    FINALIZED = "finalized"
    FINALIZE_PENDING = "finalize_pending"
    CLOSED = "closed"
    FAILED = "failed"


@dataclass(frozen=True, slots=True)
class Admit:
    """The driver may enqueue its corresponding operation."""


@dataclass(frozen=True, slots=True)
class ReuseClose:
    """The driver must return its existing close future."""


@dataclass(frozen=True, slots=True)
class Ignore:
    """The operation is already represented by the current state."""


@dataclass(frozen=True, slots=True)
class Reject:
    """The driver must raise the mapped public error."""

    kind: RejectionKind


type AttemptWriterDirective = Admit | ReuseClose | Ignore | Reject


@dataclass(frozen=True, slots=True)
class TransitionResult:
    """A state transition and one effect-free driver directive."""

    state: AttemptWriterMachineState
    directive: AttemptWriterDirective


def failure_of(state: AttemptWriterMachineState) -> BaseException | None:
    """Project the retained failure, if the state carries one."""
    if isinstance(state, Failed):
        return state.error
    if isinstance(state, Closing | Closed):
        return state.failure
    return None


def transition(state: AttemptWriterMachineState, event: AttemptWriterMachineEvent) -> TransitionResult:
    """Apply one pure lifecycle transition."""
    if isinstance(event, StartRequested | CloseRequested | CommandFailed | CloseFailed):
        return _handle_priority_event(state, event)
    if isinstance(state, Closed):
        return TransitionResult(state, Ignore() if _is_result(event) else Reject(RejectionKind.CLOSED))
    if isinstance(state, Closing):
        return _handle_closing(state, event)
    return _handle_open_state(state, event)


def _handle_priority_event(
    state: AttemptWriterMachineState,
    event: StartRequested | CloseRequested | CommandFailed | CloseFailed,
) -> TransitionResult:
    if isinstance(event, StartRequested):
        return _handle_start_request(state)
    if isinstance(event, CloseRequested):
        return _handle_close_request(state)
    if isinstance(event, CommandFailed):
        return _handle_command_failure(state, event)
    return _handle_close_failure(state, event)


def _handle_start_request(state: AttemptWriterMachineState) -> TransitionResult:
    if isinstance(state, Constructed):
        return TransitionResult(Preparing(), Admit())
    if isinstance(state, Closing | Closed) and not state.start_admitted:
        return TransitionResult(state, Reject(RejectionKind.CLOSED))
    if failure_of(state) is not None:
        return TransitionResult(state, Reject(RejectionKind.FAILED))
    return TransitionResult(state, Ignore())


def _handle_open_state(state: AttemptWriterMachineState, event: AttemptWriterMachineEvent) -> TransitionResult:
    if isinstance(event, CloseSucceeded):
        return TransitionResult(state, Ignore())
    if isinstance(state, Failed):
        return TransitionResult(state, Ignore() if _is_result(event) else Reject(RejectionKind.FAILED))
    return _handle_healthy(cast("Constructed | Preparing | Prepared | Reading | Finalizing | Finalized", state), event)


def _handle_command_failure(state: AttemptWriterMachineState, event: CommandFailed) -> TransitionResult:
    failure = failure_of(state) or event.error
    if isinstance(state, Closing):
        return TransitionResult(Closing(failure, False, state.start_admitted), Ignore())
    if isinstance(state, Closed):
        return TransitionResult(Closed(failure, state.start_admitted), Ignore())
    return TransitionResult(Failed(failure), Ignore())


def _handle_close_failure(state: AttemptWriterMachineState, event: CloseFailed) -> TransitionResult:
    failure = failure_of(state) or event.error
    start_admitted = state.start_admitted if isinstance(state, Closing | Closed) else not isinstance(state, Constructed)
    return TransitionResult(Closed(failure, start_admitted), Ignore())


def _handle_healthy(
    state: Constructed | Preparing | Prepared | Reading | Finalizing | Finalized,
    event: AttemptWriterMachineEvent,
) -> TransitionResult:
    if isinstance(state, Constructed):
        return _handle_constructed(state, event)
    if isinstance(state, Preparing):
        return _handle_preparing(state, event)
    if isinstance(state, Prepared):
        return _handle_prepared(state, event)
    if isinstance(state, Reading):
        return _handle_reading(state, event)
    return _handle_finalizing(state, event) if isinstance(state, Finalizing) else _handle_finalized(state, event)


def _handle_close_request(state: AttemptWriterMachineState) -> TransitionResult:
    if isinstance(state, Closed | Closing):
        return TransitionResult(state, ReuseClose())
    if isinstance(state, Failed):
        return TransitionResult(Closing(state.error, False, True), Admit())
    return TransitionResult(
        Closing(
            None,
            isinstance(state, Reading | Finalizing | Finalized),
            not isinstance(state, Constructed),
        ),
        Admit(),
    )


def _handle_closing(state: Closing, event: AttemptWriterMachineEvent) -> TransitionResult:
    if isinstance(event, CloseSucceeded):
        return TransitionResult(Closed(state.failure, state.start_admitted), Ignore())
    if isinstance(event, ReadBeginSucceeded):
        return TransitionResult(Closing(state.failure, state.failure is None, state.start_admitted), Ignore())
    if _is_result(event):
        return TransitionResult(state, Ignore())
    return TransitionResult(state, Reject(RejectionKind.CLOSED))


def _handle_constructed(state: Constructed, event: AttemptWriterMachineEvent) -> TransitionResult:
    if isinstance(event, PublishRequested):
        return TransitionResult(state, Admit())
    return _request_rejection(state, event, RejectionKind.NOT_STARTED)


def _handle_preparing(state: Preparing, event: AttemptWriterMachineEvent) -> TransitionResult:
    if isinstance(event, PrepareSucceeded):
        return TransitionResult(Prepared(), Ignore())
    if isinstance(event, PublishRequested):
        return TransitionResult(state, Admit())
    return _request_rejection(state, event, RejectionKind.PREPARE_PENDING)


def _handle_prepared(state: Prepared, event: AttemptWriterMachineEvent) -> TransitionResult:
    if isinstance(event, LegRequested | ReadBeginRequested | PublishRequested):
        return TransitionResult(state, Admit())
    if isinstance(event, ReadBeginSucceeded):
        return TransitionResult(Reading(), Ignore())
    if isinstance(event, LegSucceeded):
        return TransitionResult(state, Ignore())
    return _request_rejection(state, event, RejectionKind.READ_PENDING)


def _handle_reading(state: Reading, event: AttemptWriterMachineEvent) -> TransitionResult:
    if isinstance(event, FinalizeRequested):
        return TransitionResult(Finalizing(), Admit())
    if isinstance(event, LegRequested | ReadBeginRequested | CheckpointRequested | PublishRequested):
        return TransitionResult(state, Admit())
    if isinstance(event, LegSucceeded):
        return TransitionResult(Prepared(), Ignore())
    if isinstance(event, ReadBeginSucceeded):
        return TransitionResult(state, Ignore())
    return _request_rejection(state, event, RejectionKind.READ_PENDING)


def _handle_finalizing(state: Finalizing, event: AttemptWriterMachineEvent) -> TransitionResult:
    if isinstance(event, FinalizeSucceeded):
        return TransitionResult(Finalized(), Ignore())
    if isinstance(event, FinalizeRequested):
        return TransitionResult(state, Reject(RejectionKind.FINALIZE_PENDING))
    if _is_result(event):
        return TransitionResult(state, Ignore())
    return TransitionResult(state, Reject(RejectionKind.FINALIZING))


def _handle_finalized(state: Finalized, event: AttemptWriterMachineEvent) -> TransitionResult:
    if _is_result(event):
        return TransitionResult(state, Ignore())
    return TransitionResult(state, Reject(RejectionKind.FINALIZED))


def _request_rejection(
    state: AttemptWriterMachineState,
    event: AttemptWriterMachineEvent,
    kind: RejectionKind,
) -> TransitionResult:
    if _is_result(event):
        return TransitionResult(state, Ignore())
    return TransitionResult(state, Reject(kind))


def _is_result(event: AttemptWriterMachineEvent) -> bool:
    return isinstance(
        event,
        PrepareSucceeded
        | LegSucceeded
        | ReadBeginSucceeded
        | CheckpointSucceeded
        | FinalizeSucceeded
        | CommandFailed
        | CloseSucceeded
        | CloseFailed,
    )


__all__ = [
    "Admit",
    "AttemptWriterDirective",
    "AttemptWriterMachineEvent",
    "AttemptWriterMachineState",
    "CheckpointRequested",
    "CheckpointSucceeded",
    "CloseFailed",
    "CloseRequested",
    "CloseSucceeded",
    "Closed",
    "Closing",
    "CommandFailed",
    "Constructed",
    "Failed",
    "FinalizeRequested",
    "FinalizeSucceeded",
    "Finalized",
    "Finalizing",
    "Ignore",
    "LegRequested",
    "LegSucceeded",
    "PrepareSucceeded",
    "Prepared",
    "Preparing",
    "PublishRequested",
    "ReadBeginRequested",
    "ReadBeginSucceeded",
    "Reading",
    "Reject",
    "RejectionKind",
    "ReuseClose",
    "StartRequested",
    "TransitionResult",
    "failure_of",
    "transition",
]
