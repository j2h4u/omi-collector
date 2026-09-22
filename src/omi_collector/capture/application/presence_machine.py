"""Pure policy for opportunistic pendant-presence scheduling.

This module deliberately knows neither how time advances nor how an observer
or a GATT session is driven.  Its callers supply monotonic timestamps and
interpret the single directive returned by each transition.
"""

from __future__ import annotations

from dataclasses import dataclass, field


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


@dataclass(frozen=True, slots=True)
class AdvertisementTrigger:
    """Release an attempt from a matching advertisement."""

    advertisement: Advertisement


@dataclass(frozen=True, slots=True)
class RapidRetryTrigger:
    """Release a retry from a current scanner observation."""

    advertisement: Advertisement


type AttemptTrigger = AdvertisementTrigger | RapidRetryTrigger
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


@dataclass(frozen=True, slots=True)
class AttemptFinished:
    """The sole outcome for a previously issued attempt permit."""

    at: float
    outcome: AttemptOutcome


@dataclass(frozen=True, slots=True)
class Shutdown:
    """A driver shutdown request at a monotonic timestamp."""

    at: float


type PresenceEvent = AdvertisementObserved | ScannerInterrupted | TimerFired | AttemptFinished | Shutdown


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


type PresenceDirective = Observe | StopAndBeginAttempt | Stop | NoOperation


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
    if state.retry_at is None:
        return state.scan_recheck_at
    return min(state.retry_at, state.scan_recheck_at)


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
    if isinstance(state, Closed):
        result = TransitionResult(state, NoOperation())
    elif isinstance(event, Shutdown):
        result = TransitionResult(Closed(), Stop())
    elif isinstance(event, AttemptFinished):
        if not isinstance(state, Attempting):
            raise UnexpectedAttemptOutcomeError(state)
        result = _handle_attempting(state, event, policy)
    elif isinstance(state, Attempting):
        result = TransitionResult(state, NoOperation())
    elif isinstance(event, ScannerInterrupted):
        result = _handle_scanner_interrupted(state, event, policy)
    elif isinstance(event, AdvertisementObserved):
        result = _handle_advertisement(state, event, policy)
    else:
        result = _handle_timer(state, event, policy)
    return result


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
    if state.retry_at is not None and event.at < state.retry_at:
        return _observe(
            RetryWaiting(
                timer_epoch=state.timer_epoch + 1,
                retry_at=state.retry_at,
                scan_recheck_at=event.at + policy.scan_recheck_seconds,
                retry_index=state.retry_index,
                advertisement=state.advertisement,
                arrival_started_at=state.arrival_started_at,
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
    retry_index = 0 if durable_progress or not isinstance(waiting, RetryWaiting) else waiting.retry_index
    delay = policy.rapid_backoff[min(retry_index, len(policy.rapid_backoff) - 1)]
    retry = RetryWaiting(
        timer_epoch=waiting.timer_epoch + 1,
        retry_at=at + delay,
        scan_recheck_at=at + policy.scan_recheck_seconds,
        retry_index=retry_index + 1,
        advertisement=None,
    )
    return _observe(retry)


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
    )


def _handle_scanner_interrupted(
    state: WaitingState, event: ScannerInterrupted, policy: PresenceMachinePolicy
) -> TransitionResult:
    del event, policy
    return _observe(_clear_arrival(state))
