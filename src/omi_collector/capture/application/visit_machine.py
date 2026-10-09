"""Pure lifecycle policy for one opportunistic pendant visit."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

type CloseReason = Literal[
    "drained",
    "absence",
    "recovery_exhausted",
    "restart_interrupted",
    "operator_limit",
]
type RecoveryDisposition = Literal["empty", "resumable", "needs_interrupted_close"]
type RecoveryEndReason = Literal["absence", "recovery_exhausted"]


@dataclass(frozen=True, slots=True)
class Recovering:
    """Loading durable visit evidence before opening a new attempt."""


@dataclass(frozen=True, slots=True)
class Idle:
    """No retained visit is waiting for a pendant."""


@dataclass(frozen=True, slots=True)
class Waiting:
    """A visit is retained while waiting for another attempt permit."""


@dataclass(frozen=True, slots=True)
class Attempting:
    """One attempt permit is active."""

    origin: Idle | Waiting


@dataclass(frozen=True, slots=True)
class Closing:
    """The visit is closed and awaits durable closure acknowledgement."""

    reason: CloseReason
    drain_cursor: int | None = None

    def __post_init__(self) -> None:
        _validate_drain_cursor(self.reason, self.drain_cursor)


@dataclass(frozen=True, slots=True)
class Stopped:
    """The supervisor has stopped this visit machine."""


type VisitState = Recovering | Idle | Waiting | Attempting | Closing | Stopped


@dataclass(frozen=True, slots=True)
class DrainConfirmed:
    """A fresh final INFO confirmed the ring is drained after teardown."""

    cursor: int

    def __post_init__(self) -> None:
        if type(self.cursor) is not int or self.cursor < 0:
            raise ValueError("drain cursor must be a nonnegative integer")


@dataclass(frozen=True, slots=True)
class Interrupted:
    """An attempt ended before authoritative drain confirmation."""

    connected: bool
    durable_progress: bool


@dataclass(frozen=True, slots=True)
class CandidateUnavailable:
    """The selected scanner candidate could not be used."""


@dataclass(frozen=True, slots=True)
class OperatorBatchCompleted:
    """The operator limit ended the visit."""


type SessionOutcome = DrainConfirmed | Interrupted | CandidateUnavailable | OperatorBatchCompleted


@dataclass(frozen=True, slots=True)
class RecoveryLoaded:
    disposition: RecoveryDisposition


@dataclass(frozen=True, slots=True)
class AttemptGranted:
    pass


@dataclass(frozen=True, slots=True)
class SessionFinished:
    outcome: SessionOutcome


@dataclass(frozen=True, slots=True)
class RecoveryEnded:
    reason: RecoveryEndReason


@dataclass(frozen=True, slots=True)
class ClosureCommitted:
    pass


@dataclass(frozen=True, slots=True)
class CloseFailed:
    pass


@dataclass(frozen=True, slots=True)
class Shutdown:
    pass


type VisitEvent = (
    RecoveryLoaded | AttemptGranted | SessionFinished | RecoveryEnded | ClosureCommitted | CloseFailed | Shutdown
)


@dataclass(frozen=True, slots=True)
class InspectRecovery:
    pass


@dataclass(frozen=True, slots=True)
class WaitForAttempt:
    arm_restored: bool = False
    previous_outcome: Interrupted | CandidateUnavailable | None = None


@dataclass(frozen=True, slots=True)
class RunAttempt:
    pass


@dataclass(frozen=True, slots=True)
class CommitClosure:
    reason: CloseReason
    drain_cursor: int | None = None

    def __post_init__(self) -> None:
        _validate_drain_cursor(self.reason, self.drain_cursor)


@dataclass(frozen=True, slots=True)
class FinishVisit:
    reason: CloseReason
    stop: bool


@dataclass(frozen=True, slots=True)
class PreserveAndStop:
    pass


@dataclass(frozen=True, slots=True)
class NoOp:
    pass


type VisitCommand = InspectRecovery | WaitForAttempt | RunAttempt | CommitClosure | FinishVisit | PreserveAndStop | NoOp


@dataclass(frozen=True, slots=True)
class TransitionResult:
    state: VisitState
    command: VisitCommand


class VisitTransitionError(RuntimeError):
    """An event does not belong to the current visit state."""


def initial_transition() -> TransitionResult:
    """Begin by inspecting durable recovery evidence."""
    return TransitionResult(Recovering(), InspectRecovery())


def transition(state: VisitState, event: VisitEvent, *, stop_after_drained: bool = False) -> TransitionResult:
    """Apply one closed-union visit transition without side effects."""
    if isinstance(state, Stopped):
        return TransitionResult(state, NoOp())
    if not isinstance(state, (Recovering, Idle, Waiting, Attempting, Closing)):
        raise VisitTransitionError(f"unsupported visit state: {type(state).__name__}")
    if isinstance(event, Shutdown):
        return TransitionResult(Stopped(), PreserveAndStop())
    if isinstance(state, Recovering):
        return _recovering_transition(event)
    if isinstance(state, (Idle, Waiting)):
        return _waiting_transition(state, event)
    if isinstance(state, Attempting):
        return _attempting_transition(state, event)
    if isinstance(state, Closing):
        return _closing_transition(state, event, stop_after_drained=stop_after_drained)
    raise VisitTransitionError(f"unsupported visit state: {type(state).__name__}")


def _recovering_transition(event: VisitEvent) -> TransitionResult:
    if not isinstance(event, RecoveryLoaded):
        raise VisitTransitionError(f"{type(event).__name__} is invalid while recovering")
    if event.disposition == "empty":
        return TransitionResult(Idle(), WaitForAttempt())
    if event.disposition == "resumable":
        return TransitionResult(Waiting(), WaitForAttempt(arm_restored=True))
    if event.disposition == "needs_interrupted_close":
        return TransitionResult(Closing("restart_interrupted"), CommitClosure("restart_interrupted"))
    raise VisitTransitionError(f"unsupported recovery disposition: {event.disposition!r}")


def _waiting_transition(state: Idle | Waiting, event: VisitEvent) -> TransitionResult:
    if isinstance(event, AttemptGranted):
        return TransitionResult(Attempting(state), RunAttempt())
    if isinstance(state, Waiting) and isinstance(event, RecoveryEnded):
        return TransitionResult(Closing(event.reason), CommitClosure(event.reason))
    raise VisitTransitionError(f"{type(event).__name__} is invalid while {type(state).__name__}")


def _attempting_transition(state: Attempting, event: VisitEvent) -> TransitionResult:
    if not isinstance(event, SessionFinished):
        raise VisitTransitionError(f"{type(event).__name__} is invalid while attempting")
    outcome = event.outcome
    if isinstance(outcome, Interrupted):
        return TransitionResult(Waiting(), WaitForAttempt(previous_outcome=outcome))
    if isinstance(outcome, CandidateUnavailable):
        return TransitionResult(state.origin, WaitForAttempt(previous_outcome=outcome))
    if isinstance(outcome, DrainConfirmed):
        return TransitionResult(Closing("drained", outcome.cursor), CommitClosure("drained", outcome.cursor))
    if isinstance(outcome, OperatorBatchCompleted):
        return TransitionResult(Closing("operator_limit"), CommitClosure("operator_limit"))
    raise VisitTransitionError(f"unsupported session outcome: {type(outcome).__name__}")


def _validate_drain_cursor(reason: CloseReason, cursor: int | None) -> None:
    if reason not in {"drained", "absence", "recovery_exhausted", "restart_interrupted", "operator_limit"}:
        raise ValueError("unsupported closure reason")
    if reason == "drained":
        if type(cursor) is not int or cursor < 0:
            raise ValueError("drained closure requires a nonnegative confirmed cursor")
    elif cursor is not None:
        raise ValueError("only drained closure may carry a confirmed cursor")


def _closing_transition(state: Closing, event: VisitEvent, *, stop_after_drained: bool) -> TransitionResult:
    if isinstance(event, CloseFailed):
        return TransitionResult(Stopped(), PreserveAndStop())
    if not isinstance(event, ClosureCommitted):
        raise VisitTransitionError(f"{type(event).__name__} is invalid while closing")
    if state.reason == "restart_interrupted":
        return TransitionResult(Recovering(), InspectRecovery())
    stop = state.reason == "operator_limit" or (state.reason == "drained" and stop_after_drained)
    if stop:
        return TransitionResult(Stopped(), FinishVisit(state.reason, True))
    return TransitionResult(Idle(), FinishVisit(state.reason, False))
