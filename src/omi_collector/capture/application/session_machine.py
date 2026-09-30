"""Finite effect-order contract for one connected physical session."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum, auto
from typing import Literal

type SessionOutcome = Literal["drained", "collected", "retry", "candidate_unavailable", "connected_interrupted"]
type PreflightResult = Literal["disabled", "completed", "degraded"]
type ReadResult = Literal["pending", "drained", "collected"]


class SessionCommand(Enum):
    CONNECT = auto()
    INFO = auto()
    PREFLIGHT = auto()
    READ = auto()
    TEARDOWN = auto()
    CHECKPOINT = auto()
    FINISHED = auto()
    RETURNED = auto()
    FAILED = auto()
    CANCELLED = auto()


class AfterTeardown(Enum):
    CHECKPOINT = auto()
    FAILED = auto()
    CANCELLED = auto()


@dataclass(frozen=True, slots=True)
class SessionState:
    command: SessionCommand
    after_teardown: AfterTeardown | None = None
    outcome: SessionOutcome | None = None
    teardown_interrupted: bool = False

    def __post_init__(self) -> None:
        _validate_state_tags(self)
        _validate_state_shape(self)


def _validate_state_tags(state: SessionState) -> None:
    if not isinstance(state.command, SessionCommand):
        raise ValueError(f"unsupported session command: {state.command!r}")
    if state.after_teardown is not None and not isinstance(state.after_teardown, AfterTeardown):
        raise ValueError(f"unsupported teardown continuation: {state.after_teardown!r}")
    outcomes = {"drained", "collected", "retry", "candidate_unavailable", "connected_interrupted"}
    if state.outcome is not None and state.outcome not in outcomes:
        raise ValueError(f"unsupported session outcome: {state.outcome!r}")
    if not isinstance(state.teardown_interrupted, bool):
        raise ValueError(f"unsupported teardown receipt: {state.teardown_interrupted!r}")


def _validate_state_shape(state: SessionState) -> None:
    command = state.command
    if command is SessionCommand.TEARDOWN:
        if state.after_teardown is None:
            raise ValueError("teardown state requires a continuation")
        if (state.after_teardown is AfterTeardown.CHECKPOINT) != (state.outcome is not None):
            raise ValueError("only checkpoint continuation carries an expected outcome")
    elif state.after_teardown is not None:
        raise ValueError("only teardown state may carry a continuation")
    needs_outcome = command in {
        SessionCommand.CHECKPOINT,
        SessionCommand.FINISHED,
        SessionCommand.RETURNED,
    } or (command is SessionCommand.TEARDOWN and state.after_teardown is AfterTeardown.CHECKPOINT)
    if needs_outcome != (state.outcome is not None):
        raise ValueError(f"{command.name.lower()} state has invalid outcome metadata")
    post_teardown = {SessionCommand.CHECKPOINT, SessionCommand.FINISHED, SessionCommand.RETURNED}
    if state.teardown_interrupted and (command not in post_teardown or state.outcome != "connected_interrupted"):
        raise ValueError("only interrupted outcomes may record interrupted teardown")


@dataclass(frozen=True, slots=True)
class Connected:
    pass


@dataclass(frozen=True, slots=True)
class InfoResolved:
    pass


@dataclass(frozen=True, slots=True)
class PreflightResolved:
    result: PreflightResult


@dataclass(frozen=True, slots=True)
class ReadResolved:
    result: ReadResult


@dataclass(frozen=True, slots=True)
class EffectFailed:
    retry_outcome: SessionOutcome | None


@dataclass(frozen=True, slots=True)
class CancellationObserved:
    pass


@dataclass(frozen=True, slots=True)
class TeardownResolved:
    interrupted: bool


@dataclass(frozen=True, slots=True)
class CheckpointResolved:
    pass


@dataclass(frozen=True, slots=True)
class OutcomeReturned:
    outcome: SessionOutcome


type SessionEvent = (
    Connected
    | InfoResolved
    | PreflightResolved
    | ReadResolved
    | EffectFailed
    | CancellationObserved
    | TeardownResolved
    | CheckpointResolved
    | OutcomeReturned
)


class SessionTransitionError(RuntimeError):
    """An effect result does not belong to the session's pending command."""


def initial_state() -> SessionState:
    """Request connection entry as the first awaited effect."""
    return SessionState(SessionCommand.CONNECT)


def require_command(state: SessionState, command: SessionCommand) -> None:
    """Reject an effect before it runs unless its command is pending."""
    if state.command is not command:
        raise SessionTransitionError(
            f"cannot run {command.name.lower()} effect while {state.command.name.lower()} is pending"
        )


def transition(state: SessionState, event: SessionEvent) -> SessionState:
    """Acknowledge one completed effect and return the next pending command."""
    handler = _HANDLERS.get((state.command, type(event)))
    if handler is None:
        raise SessionTransitionError(f"{type(event).__name__} is invalid while {state.command.name.lower()}")
    return handler(state, event)


def _connect(state: SessionState, event: SessionEvent) -> SessionState:
    del state
    if isinstance(event, Connected):
        return SessionState(SessionCommand.INFO)
    if isinstance(event, EffectFailed):
        return _failed_before_teardown(event, has_session=False)
    return SessionState(SessionCommand.CANCELLED)


def _info(state: SessionState, event: SessionEvent) -> SessionState:
    del state
    if isinstance(event, InfoResolved):
        return SessionState(SessionCommand.PREFLIGHT)
    if isinstance(event, EffectFailed):
        return _failed_before_teardown(event, has_session=True)
    return SessionState(SessionCommand.TEARDOWN, AfterTeardown.CANCELLED)


def _preflight(state: SessionState, event: SessionEvent) -> SessionState:
    del state
    if isinstance(event, PreflightResolved):
        if event.result not in {"disabled", "completed", "degraded"}:
            raise SessionTransitionError(f"unsupported preflight result: {event.result!r}")
        return SessionState(SessionCommand.READ)
    if isinstance(event, EffectFailed):
        return _failed_before_teardown(event, has_session=True)
    return SessionState(SessionCommand.TEARDOWN, AfterTeardown.CANCELLED)


def _read(state: SessionState, event: SessionEvent) -> SessionState:
    del state
    if isinstance(event, ReadResolved):
        if event.result == "pending":
            return SessionState(SessionCommand.READ)
        if event.result in {"drained", "collected"}:
            return SessionState(
                SessionCommand.TEARDOWN,
                AfterTeardown.CHECKPOINT,
                outcome=event.result,
            )
        raise SessionTransitionError(f"unsupported read result: {event.result!r}")
    if isinstance(event, EffectFailed):
        return _failed_before_teardown(event, has_session=True)
    return SessionState(SessionCommand.TEARDOWN, AfterTeardown.CANCELLED)


def _teardown(state: SessionState, event: SessionEvent) -> SessionState:
    if isinstance(event, TeardownResolved):
        if not isinstance(event.interrupted, bool):
            raise SessionTransitionError(f"unsupported teardown result: {event.interrupted!r}")
        if state.after_teardown is AfterTeardown.CHECKPOINT:
            if state.outcome is None:
                raise SessionTransitionError("checkpoint continuation has no expected outcome")
            outcome: SessionOutcome = "connected_interrupted" if event.interrupted else state.outcome
            return SessionState(
                SessionCommand.CHECKPOINT,
                outcome=outcome,
                teardown_interrupted=event.interrupted,
            )
        if state.after_teardown is AfterTeardown.CANCELLED:
            return SessionState(SessionCommand.CANCELLED)
        return SessionState(SessionCommand.FAILED)
    if isinstance(event, EffectFailed):
        if event.retry_outcome is not None:
            raise SessionTransitionError("teardown failure cannot carry a retry outcome")
        return SessionState(SessionCommand.FAILED)
    return SessionState(SessionCommand.CANCELLED)


def _checkpoint(state: SessionState, event: SessionEvent) -> SessionState:
    if isinstance(event, CheckpointResolved):
        return SessionState(
            SessionCommand.FINISHED,
            outcome=state.outcome,
            teardown_interrupted=state.teardown_interrupted,
        )
    if isinstance(event, EffectFailed):
        if event.retry_outcome is not None:
            raise SessionTransitionError("checkpoint failure cannot carry a retry outcome")
        return SessionState(SessionCommand.FAILED)
    return SessionState(SessionCommand.CANCELLED)


def _finished(state: SessionState, event: SessionEvent) -> SessionState:
    if not isinstance(event, OutcomeReturned) or event.outcome != state.outcome:
        raise SessionTransitionError("returned outcome does not match completed session")
    if state.teardown_interrupted and event.outcome != "connected_interrupted":
        raise SessionTransitionError("interrupted teardown requires interrupted outcome")
    return SessionState(
        SessionCommand.RETURNED,
        outcome=event.outcome,
        teardown_interrupted=state.teardown_interrupted,
    )


def _failed_before_teardown(event: EffectFailed, *, has_session: bool) -> SessionState:
    if event.retry_outcome is None:
        return (
            SessionState(
                SessionCommand.TEARDOWN,
                AfterTeardown.FAILED,
            )
            if has_session
            else SessionState(SessionCommand.FAILED)
        )
    if event.retry_outcome not in {"retry", "candidate_unavailable", "connected_interrupted"} or (
        not has_session and event.retry_outcome == "connected_interrupted"
    ):
        raise SessionTransitionError(f"unsupported retry outcome: {event.retry_outcome!r}")
    if has_session:
        return SessionState(
            SessionCommand.TEARDOWN,
            AfterTeardown.CHECKPOINT,
            outcome=event.retry_outcome,
        )
    return SessionState(SessionCommand.CHECKPOINT, outcome=event.retry_outcome)


_HANDLERS = {
    (SessionCommand.CONNECT, Connected): _connect,
    (SessionCommand.CONNECT, EffectFailed): _connect,
    (SessionCommand.CONNECT, CancellationObserved): _connect,
    (SessionCommand.INFO, InfoResolved): _info,
    (SessionCommand.INFO, EffectFailed): _info,
    (SessionCommand.INFO, CancellationObserved): _info,
    (SessionCommand.PREFLIGHT, PreflightResolved): _preflight,
    (SessionCommand.PREFLIGHT, EffectFailed): _preflight,
    (SessionCommand.PREFLIGHT, CancellationObserved): _preflight,
    (SessionCommand.READ, ReadResolved): _read,
    (SessionCommand.READ, EffectFailed): _read,
    (SessionCommand.READ, CancellationObserved): _read,
    (SessionCommand.TEARDOWN, TeardownResolved): _teardown,
    (SessionCommand.TEARDOWN, EffectFailed): _teardown,
    (SessionCommand.TEARDOWN, CancellationObserved): _teardown,
    (SessionCommand.CHECKPOINT, CheckpointResolved): _checkpoint,
    (SessionCommand.CHECKPOINT, EffectFailed): _checkpoint,
    (SessionCommand.CHECKPOINT, CancellationObserved): _checkpoint,
    (SessionCommand.FINISHED, OutcomeReturned): _finished,
}
