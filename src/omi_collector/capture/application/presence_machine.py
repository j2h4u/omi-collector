"""Pure policy for opportunistic pendant-presence scheduling.

This module deliberately knows neither how time advances nor how an observer
or a GATT session is driven.  Its callers supply monotonic timestamps and
interpret the single directive returned by each transition.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal


@dataclass(frozen=True, slots=True)
class PresenceMachinePolicy:
    """Already-validated scheduling bounds, expressed in seconds."""

    absence_seconds: float
    scan_recheck_seconds: float
    drain_cooldown_seconds: float
    rapid_backoff: tuple[float, ...]
    arrival_stability_seconds: float = 30.0
    arrival_max_gap_seconds: float = 10.0


@dataclass(frozen=True, slots=True)
class Advertisement:
    """An externally validated scanner observation."""

    candidate: object = field(repr=False)
    observed_at: float
    rssi_dbm: int | None = None


@dataclass(frozen=True, slots=True)
class Searching:
    """Observing for a return while a scanner recheck remains armed."""

    timer_epoch: int
    scan_recheck_at: float
    advertisement: Advertisement | None = None
    arrival_started_at: float | None = None


@dataclass(frozen=True, slots=True)
class CoolingDown:
    """A clean drain suppresses continuous-presence wakeups."""

    timer_epoch: int
    cooldown_at: float
    recheck_at: float
    advertisement: Advertisement | None
    arrival_started_at: float | None = None


@dataclass(frozen=True, slots=True)
class RetryWaiting:
    """A nearby interrupted attempt is waiting for a bounded retry."""

    timer_epoch: int
    retry_at: float | None
    scan_recheck_at: float
    retry_index: int
    advertisement: Advertisement | None
    arrival_started_at: float | None = None
    absence_at: float | None = None


@dataclass(frozen=True, slots=True)
class AdvertisementTrigger:
    """Release an attempt from a matching advertisement."""

    advertisement: Advertisement


@dataclass(frozen=True, slots=True)
class RapidRetryTrigger:
    """Release a retry from a current scanner observation."""

    advertisement: Advertisement


type AttemptTrigger = AdvertisementTrigger | RapidRetryTrigger
type VisitEndReason = Literal["absence", "recovery_exhausted"]
# Waiting values are constructed only by this module's transitions.  Their
# immutable fields therefore need no defensive constructor validation.
type WaitingState = Searching | CoolingDown | RetryWaiting


@dataclass(frozen=True, slots=True)
class Attempting:
    """One permit has been issued and no second permit may be released."""

    trigger: AttemptTrigger
    waiting: WaitingState


@dataclass(frozen=True, slots=True)
class Closed:
    """Terminal state which absorbs late events."""


type PresenceState = Searching | CoolingDown | RetryWaiting | Attempting | Closed
_PRESENCE_STATE_TYPES = (Searching, CoolingDown, RetryWaiting, Attempting, Closed)


@dataclass(frozen=True, slots=True)
class AdvertisementObserved:
    """A matching advertisement whose scanner generation was validated outside."""

    advertisement: Advertisement
    processed_at: float | None = None


@dataclass(frozen=True, slots=True)
class ScannerInterrupted:
    """The active scanner stopped or failed before admission completed."""

    at: float


@dataclass(frozen=True, slots=True)
class ResumeInterruptedVisit:
    """Arm absence recovery for a startup visit with unfinished durable work."""

    at: float


@dataclass(frozen=True, slots=True)
class TimerFired:
    """A timer callback carrying both its original deadline and timer epoch."""

    at: float
    deadline: float
    timer_epoch: int


@dataclass(frozen=True, slots=True)
class CleanDrain:
    """The session completed and the final INFO proved a clean drain."""


@dataclass(frozen=True, slots=True)
class NotConnected:
    """No connection was established; earlier durable work may have completed."""

    durable_progress: bool


@dataclass(frozen=True, slots=True)
class ConnectedInterruption:
    """A connection proved presence but the session did not complete."""

    durable_progress: bool


@dataclass(frozen=True, slots=True)
class CandidateUnavailable:
    """The advisory candidate cannot be used and must be discarded atomically."""


type AttemptOutcome = CleanDrain | NotConnected | ConnectedInterruption | CandidateUnavailable
_ATTEMPT_OUTCOME_TYPES = (CleanDrain, NotConnected, ConnectedInterruption, CandidateUnavailable)


@dataclass(frozen=True, slots=True)
class AttemptFinished:
    """The sole outcome for a previously issued attempt permit."""

    at: float
    outcome: AttemptOutcome


@dataclass(frozen=True, slots=True)
class Shutdown:
    """A driver shutdown request at a monotonic timestamp."""

    at: float


type PresenceEvent = (
    AdvertisementObserved | ScannerInterrupted | ResumeInterruptedVisit | TimerFired | AttemptFinished | Shutdown
)
_PRESENCE_EVENT_TYPES = (
    AdvertisementObserved,
    ScannerInterrupted,
    ResumeInterruptedVisit,
    TimerFired,
    AttemptFinished,
    Shutdown,
)


@dataclass(frozen=True, slots=True)
class Observe:
    """Continue observation until the supplied absolute monotonic deadline."""

    until: float


@dataclass(frozen=True, slots=True)
class StopAndBeginAttempt:
    """Stop scanning successfully before exposing this attempt trigger."""

    trigger: AttemptTrigger


@dataclass(frozen=True, slots=True)
class Stop:
    """Stop observation for terminal shutdown."""


@dataclass(frozen=True, slots=True)
class NoOperation:
    """Do nothing for a stale or late event."""


@dataclass(frozen=True, slots=True)
class EndVisit:
    """Close the interrupted visit without issuing another GATT permit."""

    reason: VisitEndReason


type PresenceDirective = Observe | StopAndBeginAttempt | Stop | NoOperation | EndVisit


@dataclass(frozen=True, slots=True)
class TransitionResult:
    """The next immutable state and exactly one driver directive."""

    state: PresenceState
    directive: PresenceDirective


class UnexpectedAttemptOutcomeError(RuntimeError):
    """An outcome arrived when no permit was outstanding.

    The async driver must install ``closed_state`` and interpret ``directive``
    before reporting and raising this error.
    """

    closed_state: Closed
    directive: Stop

    def __init__(self, state: PresenceState) -> None:
        super().__init__(f"attempt outcome received outside Attempting: {type(state).__name__}")
        self.closed_state = Closed()
        self.directive = Stop()


def initial_state(started_at: float, policy: PresenceMachinePolicy) -> Searching:
    """Create the initial scanner-recheck state without reading a clock."""
    return Searching(timer_epoch=0, scan_recheck_at=started_at + policy.scan_recheck_seconds)


def armed_deadline(state: WaitingState) -> float:
    """Project the one currently armed deadline for a waiting state."""
    if isinstance(state, Searching):
        return state.scan_recheck_at
    if isinstance(state, CoolingDown):
        return max(state.cooldown_at, state.recheck_at)
    deadlines = [state.scan_recheck_at]
    if state.retry_at is not None:
        deadlines.append(state.retry_at)
    if state.absence_at is not None:
        deadlines.append(state.absence_at)
    return min(deadlines)


def drained_cooldown_remaining_seconds(state: PresenceState, *, at: float) -> float:
    """Project remaining clean-drain cooldown for telemetry without clock reads."""
    if not isinstance(state, CoolingDown):
        return 0.0
    return max(0.0, state.cooldown_at - at)


def transition(
    state: PresenceState,
    event: PresenceEvent,
    policy: PresenceMachinePolicy,
) -> TransitionResult:
    """Apply one pure, closed-union transition."""
    _validate_transition_input(state, event)
    if isinstance(state, Closed):
        result = TransitionResult(state, NoOperation())
    elif isinstance(event, Shutdown):
        result = TransitionResult(Closed(), Stop())
    elif isinstance(event, AttemptFinished):
        if not isinstance(state, Attempting):
            raise UnexpectedAttemptOutcomeError(state)
        result = _handle_attempting(state, event, policy)
    else:
        result = _transition_active(state, event, policy)
    return result


def _validate_transition_input(state: PresenceState, event: PresenceEvent) -> None:
    if not isinstance(state, _PRESENCE_STATE_TYPES):
        raise TypeError(f"unsupported presence state: {type(state).__name__}")
    if not isinstance(event, _PRESENCE_EVENT_TYPES):
        raise TypeError(f"unsupported presence event: {type(event).__name__}")
    if isinstance(event, AttemptFinished) and not isinstance(event.outcome, _ATTEMPT_OUTCOME_TYPES):
        raise TypeError(f"unsupported attempt outcome: {type(event.outcome).__name__}")


def _transition_active(
    state: WaitingState | Attempting,
    event: PresenceEvent,
    policy: PresenceMachinePolicy,
) -> TransitionResult:
    if isinstance(event, ResumeInterruptedVisit):
        if isinstance(state, Attempting):
            return TransitionResult(
                Attempting(state.trigger, _resume_waiting(state.waiting, event, policy)),
                NoOperation(),
            )
        return _handle_resume_interrupted_visit(state, event, policy)
    if isinstance(state, Attempting):
        return TransitionResult(state, NoOperation())
    if isinstance(event, ScannerInterrupted):
        return _handle_scanner_interrupted(state, event, policy)
    if isinstance(event, AdvertisementObserved):
        return _handle_advertisement(state, event, policy)
    if isinstance(event, TimerFired):
        return _handle_timer(state, event, policy)
    raise TypeError(f"unsupported presence event: {type(event).__name__}")


def _handle_advertisement(
    state: WaitingState,
    event: AdvertisementObserved,
    policy: PresenceMachinePolicy,
) -> TransitionResult:
    processed_at = event.processed_at if event.processed_at is not None else event.advertisement.observed_at
    if (
        processed_at < event.advertisement.observed_at
        or processed_at >= event.advertisement.observed_at + policy.arrival_max_gap_seconds
    ):
        return _observe(_clear_arrival(state))
    state = _refresh_retry_absence(state, event.advertisement.observed_at, policy)
    if isinstance(state, Searching):
        result = _admit_advertisement(state, event.advertisement, policy=policy)
    elif isinstance(state, CoolingDown):
        if event.advertisement.observed_at < state.cooldown_at:
            result = _observe(state)
        else:
            result = _admit_advertisement(state, event.advertisement, policy=policy)
    elif state.retry_at is not None and event.advertisement.observed_at < state.retry_at:
        result = _observe(state)
    else:
        result = _admit_advertisement(state, event.advertisement, policy=policy)
        if isinstance(result.state, Attempting) and isinstance(result.state.trigger, AdvertisementTrigger):
            result = TransitionResult(
                result.state,
                StopAndBeginAttempt(RapidRetryTrigger(result.state.trigger.advertisement)),
            )
    return result


def _handle_timer(
    state: WaitingState,
    event: TimerFired,
    policy: PresenceMachinePolicy,
) -> TransitionResult:
    if event.timer_epoch != state.timer_epoch or event.deadline != armed_deadline(state):
        return TransitionResult(state, NoOperation())
    if isinstance(state, Searching):
        if event.at < state.scan_recheck_at:
            return _observe(state)
        refreshed = Searching(
            timer_epoch=state.timer_epoch + 1,
            scan_recheck_at=event.at + policy.scan_recheck_seconds,
            advertisement=state.advertisement,
            arrival_started_at=state.arrival_started_at,
        )
        return _observe(refreshed)
    if isinstance(state, CoolingDown):
        return _handle_cooldown_timer(state, event, policy)
    return _handle_retry_timer(state, event, policy)


def _handle_cooldown_timer(
    state: CoolingDown,
    event: TimerFired,
    policy: PresenceMachinePolicy,
) -> TransitionResult:
    if event.at < state.cooldown_at:
        return _observe(state)
    return _observe(
        Searching(
            timer_epoch=state.timer_epoch + 1,
            scan_recheck_at=event.at + policy.scan_recheck_seconds,
        )
    )


def _handle_retry_timer(
    state: RetryWaiting,
    event: TimerFired,
    policy: PresenceMachinePolicy,
) -> TransitionResult:
    if event.at < armed_deadline(state):
        return _observe(state)
    if state.absence_at is not None and event.at >= state.absence_at:
        return _end_visit(state, at=event.at, reason="absence", policy=policy)
    if state.retry_at is not None and event.at < state.retry_at:
        return _observe(
            RetryWaiting(
                timer_epoch=state.timer_epoch + 1,
                retry_at=state.retry_at,
                scan_recheck_at=event.at + policy.scan_recheck_seconds,
                retry_index=state.retry_index,
                advertisement=state.advertisement,
                arrival_started_at=state.arrival_started_at,
                absence_at=state.absence_at,
            )
        )
    if state.retry_at is not None:
        return _observe(
            RetryWaiting(
                timer_epoch=state.timer_epoch + 1,
                retry_at=None,
                scan_recheck_at=state.scan_recheck_at
                if state.scan_recheck_at > event.at
                else event.at + policy.scan_recheck_seconds,
                retry_index=state.retry_index,
                advertisement=state.advertisement,
                arrival_started_at=state.arrival_started_at,
                absence_at=state.absence_at,
            )
        )
    return _observe(
        RetryWaiting(
            timer_epoch=state.timer_epoch + 1,
            scan_recheck_at=state.scan_recheck_at
            if state.scan_recheck_at > event.at
            else event.at + policy.scan_recheck_seconds,
            retry_at=None,
            retry_index=state.retry_index,
            advertisement=state.advertisement,
            arrival_started_at=state.arrival_started_at,
            absence_at=state.absence_at,
        )
    )


def _handle_attempting(
    state: Attempting,
    event: AttemptFinished,
    policy: PresenceMachinePolicy,
) -> TransitionResult:
    if isinstance(event.outcome, CleanDrain):
        cooled = CoolingDown(
            timer_epoch=state.waiting.timer_epoch + 1,
            cooldown_at=event.at + policy.drain_cooldown_seconds,
            recheck_at=event.at + policy.drain_cooldown_seconds,
            advertisement=None,
        )
        return _observe(cooled)
    if isinstance(event.outcome, CandidateUnavailable):
        if isinstance(state.waiting, RetryWaiting):
            return _observe(_clear_arrival(state.waiting))
        return _observe(Searching(state.waiting.timer_epoch + 1, event.at + policy.scan_recheck_seconds))
    if isinstance(event.outcome, ConnectedInterruption):
        return _retry_waiting(
            state.waiting,
            at=event.at,
            durable_progress=event.outcome.durable_progress,
            policy=policy,
        )
    return _retry_waiting(
        state.waiting,
        at=event.at,
        durable_progress=event.outcome.durable_progress,
        policy=policy,
    )


def _retry_waiting(
    waiting: WaitingState,
    *,
    at: float,
    durable_progress: bool,
    policy: PresenceMachinePolicy,
) -> TransitionResult:
    previous_count = waiting.retry_index if isinstance(waiting, RetryWaiting) else 0
    retry_index = 0 if durable_progress else previous_count + 1
    # The first interrupted encounter earns one reconnect confirmation. The
    # configured backoff length counts retries after that initial failure.
    if not durable_progress and previous_count >= len(policy.rapid_backoff):
        return _end_visit(waiting, at=at, reason="recovery_exhausted", policy=policy)
    delay_index = 0 if durable_progress else retry_index - 1
    delay = policy.rapid_backoff[min(delay_index, len(policy.rapid_backoff) - 1)]
    retry = RetryWaiting(
        timer_epoch=waiting.timer_epoch + 1,
        retry_at=at + delay,
        scan_recheck_at=at + policy.scan_recheck_seconds,
        retry_index=retry_index,
        advertisement=None,
        absence_at=at + policy.absence_seconds,
    )
    return _observe(retry)


def _end_visit(
    waiting: WaitingState,
    *,
    at: float,
    reason: VisitEndReason,
    policy: PresenceMachinePolicy,
) -> TransitionResult:
    return TransitionResult(
        Searching(waiting.timer_epoch + 1, at + policy.scan_recheck_seconds),
        EndVisit(reason),
    )


def _begin(waiting: WaitingState, trigger: AttemptTrigger) -> TransitionResult:
    return TransitionResult(Attempting(trigger=trigger, waiting=waiting), StopAndBeginAttempt(trigger))


def _observe(state: WaitingState) -> TransitionResult:
    return TransitionResult(state, Observe(armed_deadline(state)))


def _admit_advertisement(
    state: WaitingState,
    advertisement: Advertisement,
    *,
    policy: PresenceMachinePolicy,
) -> TransitionResult:
    """Accumulate one stable encounter and issue only a candidate-backed permit."""
    previous = state.advertisement
    if (
        previous is None
        or advertisement.observed_at <= previous.observed_at
        or advertisement.observed_at - previous.observed_at >= policy.arrival_max_gap_seconds
    ):
        first_at = advertisement.observed_at
    else:
        first_at = _arrival_started_at(state)
    if first_at is None:
        first_at = advertisement.observed_at
    if advertisement.observed_at - first_at >= policy.arrival_stability_seconds:
        trigger: AttemptTrigger = AdvertisementTrigger(advertisement)
        if isinstance(state, RetryWaiting):
            trigger = RapidRetryTrigger(advertisement)
        return _begin(state, trigger)
    refreshed = _replace_advertisement(state, advertisement, first_at)
    return _observe(refreshed)


def _arrival_started_at(state: WaitingState) -> float | None:
    return state.arrival_started_at


def _refresh_retry_absence(
    state: WaitingState,
    observed_at: float,
    policy: PresenceMachinePolicy,
) -> WaitingState:
    if not isinstance(state, RetryWaiting):
        return state
    absence_at = observed_at + policy.absence_seconds
    if state.absence_at is not None:
        absence_at = max(absence_at, state.absence_at)
    return RetryWaiting(
        state.timer_epoch,
        state.retry_at,
        state.scan_recheck_at,
        state.retry_index,
        state.advertisement,
        state.arrival_started_at,
        absence_at,
    )


def _replace_advertisement(state: WaitingState, advertisement: Advertisement, first_at: float) -> WaitingState:
    if isinstance(state, Searching):
        return Searching(state.timer_epoch, state.scan_recheck_at, advertisement, first_at)
    if isinstance(state, CoolingDown):
        return CoolingDown(state.timer_epoch, state.cooldown_at, state.recheck_at, advertisement, first_at)
    return RetryWaiting(
        state.timer_epoch,
        state.retry_at,
        state.scan_recheck_at,
        state.retry_index,
        advertisement,
        first_at,
        state.absence_at,
    )


def _clear_arrival(state: WaitingState) -> WaitingState:
    if isinstance(state, Searching):
        return Searching(state.timer_epoch, state.scan_recheck_at)
    if isinstance(state, CoolingDown):
        return CoolingDown(state.timer_epoch, state.cooldown_at, state.recheck_at, None)
    return RetryWaiting(
        state.timer_epoch,
        state.retry_at,
        state.scan_recheck_at,
        state.retry_index,
        None,
        None,
        state.absence_at,
    )


def _handle_scanner_interrupted(
    state: WaitingState, event: ScannerInterrupted, policy: PresenceMachinePolicy
) -> TransitionResult:
    del event, policy
    return _observe(_clear_arrival(state))


def _handle_resume_interrupted_visit(
    state: WaitingState,
    event: ResumeInterruptedVisit,
    policy: PresenceMachinePolicy,
) -> TransitionResult:
    if isinstance(state, CoolingDown):
        return TransitionResult(state, NoOperation())
    return _observe(_resume_waiting(state, event, policy))


def _resume_waiting(
    state: WaitingState,
    event: ResumeInterruptedVisit,
    policy: PresenceMachinePolicy,
) -> RetryWaiting:
    return RetryWaiting(
        timer_epoch=state.timer_epoch,
        retry_at=state.retry_at if isinstance(state, RetryWaiting) else None,
        scan_recheck_at=state.recheck_at if isinstance(state, CoolingDown) else state.scan_recheck_at,
        retry_index=state.retry_index if isinstance(state, RetryWaiting) else 0,
        advertisement=state.advertisement,
        arrival_started_at=state.arrival_started_at,
        absence_at=event.at + policy.absence_seconds,
    )
