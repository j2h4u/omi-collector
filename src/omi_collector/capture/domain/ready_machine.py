"""Pure ready-publication decision from durable capture facts."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum


class ReadyState(StrEnum):
    WAITING_FOR_DRAIN = "waiting_for_drain"
    WAITING_FOR_THRESHOLD = "waiting_for_threshold"
    READY_TO_PUBLISH = "ready_to_publish"


class ReadyCommand(StrEnum):
    WAIT = "wait"
    PUBLISH = "publish"


@dataclass(frozen=True, slots=True)
class ReadyDecision:
    state: ReadyState
    command: ReadyCommand


def decide_ready(*, drained: bool, has_audio: bool, threshold_met: bool) -> ReadyDecision:
    """Derive the only allowed next action without adding durable state."""
    if threshold_met and not has_audio:
        raise ValueError("an empty audio set cannot meet the publication threshold")
    if not drained:
        return ReadyDecision(ReadyState.WAITING_FOR_DRAIN, ReadyCommand.WAIT)
    if not has_audio or not threshold_met:
        return ReadyDecision(ReadyState.WAITING_FOR_THRESHOLD, ReadyCommand.WAIT)
    return ReadyDecision(ReadyState.READY_TO_PUBLISH, ReadyCommand.PUBLISH)


class PublicationMode(StrEnum):
    AVAILABLE = "available"
    CAPTURE = "capture"
    CLOSED = "closed"


class PublicationResult(StrEnum):
    SETTLED = "settled"
    TRANSIENT = "transient"


@dataclass(frozen=True, slots=True)
class Idle:
    pass


@dataclass(frozen=True, slots=True)
class Running:
    token: int
    revision: object
    generation: int
    failure_count: int = 0


@dataclass(frozen=True, slots=True)
class Settled:
    revision: object
    outcome: object


@dataclass(frozen=True, slots=True)
class RetryWait:
    revision: object
    outcome: object
    deadline: float
    failure_count: int


@dataclass(frozen=True, slots=True)
class PublicationState:
    mode: PublicationMode = PublicationMode.AVAILABLE
    work: Idle | Running | Settled | RetryWait = Idle()
    generation: int = 0
    next_token: int = 0
    needs_check: bool = False


class PublicationAction(StrEnum):
    NONE = "none"
    RUN = "run"
    ARM = "arm"
    CHECK_INPUT = "check_input"
    CANCEL_AND_JOIN = "cancel_and_join"


@dataclass(frozen=True, slots=True)
class PublicationCommand:
    action: PublicationAction = PublicationAction.NONE
    token: int | None = None
    deadline: float | None = None


@dataclass(frozen=True, slots=True)
class Wake:
    revision: object
    now: float


@dataclass(frozen=True, slots=True)
class InputChanged:
    pass


@dataclass(frozen=True, slots=True)
class Finished:
    token: int
    revision: object
    outcome: object
    result: PublicationResult
    now: float
    backoff: tuple[float, ...] = ()


@dataclass(frozen=True, slots=True)
class TimerFired:
    generation: int
    deadline: float
    now: float


@dataclass(frozen=True, slots=True)
class CaptureBegin:
    pass


@dataclass(frozen=True, slots=True)
class CaptureEnd:
    pass


@dataclass(frozen=True, slots=True)
class Shutdown:
    pass


@dataclass(frozen=True, slots=True)
class Quiesced:
    pass


type PublicationEvent = Wake | InputChanged | Finished | TimerFired | CaptureBegin | CaptureEnd | Shutdown | Quiesced


def publication_transition(
    state: PublicationState, event: PublicationEvent
) -> tuple[PublicationState, PublicationCommand]:
    """Decide publication admission, settlement, and retries without I/O."""
    if isinstance(event, Wake):
        return _wake(state, event)
    if isinstance(event, Finished):
        return _finished(state, event)
    if isinstance(event, TimerFired):
        return _timer(state, event)
    return _lifecycle(state, event)


def _lifecycle(
    state: PublicationState, event: InputChanged | CaptureBegin | CaptureEnd | Shutdown | Quiesced
) -> tuple[PublicationState, PublicationCommand]:
    if isinstance(event, Shutdown):
        updated = PublicationState(PublicationMode.CLOSED, state.work, state.generation + 1, state.next_token)
        return updated, PublicationCommand(PublicationAction.CANCEL_AND_JOIN)
    if isinstance(event, CaptureBegin):
        if state.mode is not PublicationMode.AVAILABLE:
            raise ValueError("capture cannot begin twice or after shutdown")
        updated = PublicationState(PublicationMode.CAPTURE, state.work, state.generation + 1, state.next_token)
        return updated, PublicationCommand(PublicationAction.CANCEL_AND_JOIN)
    if isinstance(event, Quiesced):
        return PublicationState(state.mode, Idle(), state.generation, state.next_token), PublicationCommand()
    if isinstance(event, CaptureEnd):
        if state.mode is not PublicationMode.CAPTURE:
            return state, PublicationCommand()
        updated = PublicationState(PublicationMode.AVAILABLE, Idle(), state.generation + 1, state.next_token, True)
        return updated, PublicationCommand(PublicationAction.CHECK_INPUT)
    work = state.work if isinstance(state.work, Running) else Idle()
    updated = PublicationState(state.mode, work, state.generation + 1, state.next_token, True)
    action = (
        PublicationAction.CHECK_INPUT
        if state.mode is PublicationMode.AVAILABLE and not isinstance(work, Running)
        else PublicationAction.NONE
    )
    return updated, PublicationCommand(action)


def _finished(state: PublicationState, event: Finished) -> tuple[PublicationState, PublicationCommand]:
    work = state.work
    if not isinstance(work, Running) or work.token != event.token:
        return state, PublicationCommand()
    if state.mode is not PublicationMode.AVAILABLE:
        return PublicationState(state.mode, Idle(), state.generation, state.next_token), PublicationCommand()
    if work.generation != state.generation or event.revision != work.revision:
        updated = PublicationState(state.mode, Idle(), state.generation, state.next_token, True)
        return updated, PublicationCommand(PublicationAction.CHECK_INPUT)
    if event.result is PublicationResult.TRANSIENT:
        delay = event.backoff[min(work.failure_count, len(event.backoff) - 1)] if event.backoff else 1.0
        deadline = event.now + delay
        updated = PublicationState(
            state.mode,
            RetryWait(event.revision, event.outcome, deadline, work.failure_count + 1),
            state.generation,
            state.next_token,
        )
        return updated, PublicationCommand(PublicationAction.ARM, deadline=deadline)
    updated = PublicationState(state.mode, Settled(event.revision, event.outcome), state.generation, state.next_token)
    return updated, PublicationCommand()


def _timer(state: PublicationState, event: TimerFired) -> tuple[PublicationState, PublicationCommand]:
    work = state.work
    if not isinstance(work, RetryWait) or state.mode is not PublicationMode.AVAILABLE:
        return state, PublicationCommand()
    if event.generation != state.generation or event.deadline != work.deadline:
        return state, PublicationCommand()
    if event.now < work.deadline:
        return state, PublicationCommand(PublicationAction.ARM, deadline=work.deadline)
    updated = PublicationState(state.mode, work, state.generation, state.next_token, True)
    return updated, PublicationCommand(PublicationAction.CHECK_INPUT)


def _wake(state: PublicationState, event: Wake) -> tuple[PublicationState, PublicationCommand]:
    if state.mode is not PublicationMode.AVAILABLE:
        return state, PublicationCommand()
    if event.revision is None:
        return _wake_unprobed(state, event.now)
    return _wake_probed(state, event)


def _wake_unprobed(state: PublicationState, now: float) -> tuple[PublicationState, PublicationCommand]:
    work = state.work
    if isinstance(work, Running):
        return state, PublicationCommand(PublicationAction.CHECK_INPUT)
    if isinstance(work, RetryWait) and now < work.deadline:
        return state, PublicationCommand(PublicationAction.ARM, deadline=work.deadline)
    return state, PublicationCommand(PublicationAction.CHECK_INPUT)


def _wake_probed(state: PublicationState, event: Wake) -> tuple[PublicationState, PublicationCommand]:
    work = state.work
    if isinstance(work, Running):
        if event.revision != work.revision and work.generation == state.generation:
            return PublicationState(
                state.mode, work, state.generation + 1, state.next_token, True
            ), PublicationCommand()
        return state, PublicationCommand()
    if isinstance(work, Settled) and event.revision == work.revision:
        return state, PublicationCommand()
    if isinstance(work, RetryWait) and event.revision == work.revision and event.now < work.deadline:
        return state, PublicationCommand(PublicationAction.ARM, deadline=work.deadline)
    token = state.next_token + 1
    failures = work.failure_count if isinstance(work, RetryWait) and event.revision == work.revision else 0
    updated = PublicationState(
        state.mode, Running(token, event.revision, state.generation, failures), state.generation, token
    )
    return updated, PublicationCommand(PublicationAction.RUN, token=token)
