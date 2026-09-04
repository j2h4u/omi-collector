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
    fallback_seconds: float
    drained_fallback_seconds: float
    rapid_backoff: tuple[float, ...]


@dataclass(frozen=True, slots=True)
class Advertisement:
    """An externally validated scanner observation."""

    candidate: object = field(repr=False)
    observed_at: float
    rssi_dbm: int | None = None


@dataclass(frozen=True, slots=True)
class Searching:
    """Observing for a return while an ordinary fallback remains armed."""

    timer_epoch: int
    fallback_at: float


@dataclass(frozen=True, slots=True)
class CoolingDown:
    """A clean drain suppresses continuous-presence wakeups."""

    timer_epoch: int
    cooldown_at: float
    recheck_at: float
    gatt_at: float | None
    advertisement: Advertisement | None


@dataclass(frozen=True, slots=True)
class RetryWaiting:
    """A nearby interrupted attempt is waiting for a bounded retry."""

    timer_epoch: int
    retry_at: float
    fallback_at: float
    retry_index: int
    gatt_at: float | None
    advertisement: Advertisement | None


@dataclass(frozen=True, slots=True)
class AdvertisementTrigger:
    """Release an attempt from a matching advertisement."""

    advertisement: Advertisement


@dataclass(frozen=True, slots=True)
class FallbackTrigger:
    """Release an ordinary fallback, optionally with a fresh candidate."""

    advertisement: Advertisement | None


@dataclass(frozen=True, slots=True)
class RapidRetryTrigger:
    """Release a retry while presence evidence remains fresh."""

    advertisement: Advertisement | None


type AttemptTrigger = AdvertisementTrigger | FallbackTrigger | RapidRetryTrigger
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


type PresenceEvent = AdvertisementObserved | TimerFired | AttemptFinished | Shutdown


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
    """Create the initial fallback search state without reading a clock."""
    return Searching(timer_epoch=0, fallback_at=started_at + policy.fallback_seconds)


def armed_deadline(state: WaitingState) -> float:
    """Project the one currently armed deadline for a waiting state."""
    if isinstance(state, Searching):
        return state.fallback_at
    if isinstance(state, CoolingDown):
        return max(state.cooldown_at, state.recheck_at)
    return min(state.retry_at, state.fallback_at)


def latest_presence_at(gatt_at: float | None, advertisement: Advertisement | None) -> float | None:
    """Return the later GATT or advertisement proof, if either exists."""
    advertisement_at = advertisement.observed_at if advertisement is not None else None
    if gatt_at is None:
        return advertisement_at
    if advertisement_at is None:
        return gatt_at
    return max(gatt_at, advertisement_at)


def evidence_is_fresh(
    gatt_at: float | None,
    advertisement: Advertisement | None,
    *,
    at: float,
    policy: PresenceMachinePolicy,
) -> bool:
    """Apply the one strict freshness rule; equality is already stale."""
    evidence_at = latest_presence_at(gatt_at, advertisement)
    return evidence_at is not None and at < evidence_at + policy.absence_seconds


def fresh_advertisement(
    advertisement: Advertisement | None,
    *,
    at: float,
    policy: PresenceMachinePolicy,
) -> Advertisement | None:
    """Return only a candidate that remains fresh at the decision timestamp."""
    if advertisement is None or at >= advertisement.observed_at + policy.absence_seconds:
        return None
    return advertisement


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
        return TransitionResult(state, NoOperation())
    if isinstance(event, Shutdown):
        return TransitionResult(Closed(), Stop())
    if isinstance(event, AttemptFinished):
        if not isinstance(state, Attempting):
            raise UnexpectedAttemptOutcomeError(state)
        return _handle_attempting(state, event, policy)
    if isinstance(state, Attempting):
        return TransitionResult(state, NoOperation())
    if isinstance(event, AdvertisementObserved):
        return _handle_advertisement(state, event, policy)
    return _handle_timer(state, event, policy)


def _handle_advertisement(
    state: WaitingState,
    event: AdvertisementObserved,
    policy: PresenceMachinePolicy,
) -> TransitionResult:
    if isinstance(state, Searching):
        return _begin(state, AdvertisementTrigger(event.advertisement))
    if isinstance(state, CoolingDown):
        if event.advertisement.observed_at >= state.cooldown_at:
            return _begin(state, AdvertisementTrigger(event.advertisement))
        refreshed = CoolingDown(
            timer_epoch=state.timer_epoch,
            cooldown_at=state.cooldown_at,
            recheck_at=state.recheck_at,
            gatt_at=state.gatt_at,
            advertisement=event.advertisement,
        )
        return _observe(refreshed)
    if not evidence_is_fresh(state.gatt_at, state.advertisement, at=event.advertisement.observed_at, policy=policy):
        restarted = RetryWaiting(
            timer_epoch=state.timer_epoch,
            retry_at=state.retry_at,
            fallback_at=state.fallback_at,
            retry_index=0,
            gatt_at=state.gatt_at,
            advertisement=event.advertisement,
        )
        return _begin(restarted, AdvertisementTrigger(event.advertisement))
    refreshed = RetryWaiting(
        timer_epoch=state.timer_epoch,
        retry_at=state.retry_at,
        fallback_at=state.fallback_at,
        retry_index=state.retry_index,
        gatt_at=state.gatt_at,
        advertisement=event.advertisement,
    )
    return _observe(refreshed)


def _handle_timer(
    state: WaitingState,
    event: TimerFired,
    policy: PresenceMachinePolicy,
) -> TransitionResult:
    if event.timer_epoch != state.timer_epoch or event.deadline != armed_deadline(state):
        return TransitionResult(state, NoOperation())
    if isinstance(state, Searching):
        if event.at < state.fallback_at:
            return _observe(state)
        return _begin(state, FallbackTrigger(None))
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
    if evidence_is_fresh(state.gatt_at, state.advertisement, at=event.at, policy=policy):
        trigger = FallbackTrigger(fresh_advertisement(state.advertisement, at=event.at, policy=policy))
        return _begin(state, trigger)
    quiet = CoolingDown(
        timer_epoch=state.timer_epoch + 1,
        cooldown_at=state.cooldown_at,
        recheck_at=event.at + policy.absence_seconds,
        gatt_at=state.gatt_at,
        advertisement=state.advertisement,
    )
    return _observe(quiet)


def _handle_retry_timer(
    state: RetryWaiting,
    event: TimerFired,
    policy: PresenceMachinePolicy,
) -> TransitionResult:
    if event.at < armed_deadline(state):
        return _observe(state)
    if state.fallback_at < state.retry_at and event.at >= state.fallback_at:
        trigger = FallbackTrigger(fresh_advertisement(state.advertisement, at=event.at, policy=policy))
        return _begin(state, trigger)
    if event.at < state.retry_at:
        return _observe(state)
    if evidence_is_fresh(state.gatt_at, state.advertisement, at=event.at, policy=policy):
        trigger = RapidRetryTrigger(fresh_advertisement(state.advertisement, at=event.at, policy=policy))
        return _begin(state, trigger)
    if event.at >= state.fallback_at:
        trigger = FallbackTrigger(fresh_advertisement(state.advertisement, at=event.at, policy=policy))
        return _begin(state, trigger)
    return _observe(Searching(timer_epoch=state.timer_epoch + 1, fallback_at=state.fallback_at))


def _handle_attempting(
    state: Attempting,
    event: AttemptFinished,
    policy: PresenceMachinePolicy,
) -> TransitionResult:
    evidence = _attempt_evidence(state)
    if isinstance(event.outcome, CleanDrain):
        cooled = CoolingDown(
            timer_epoch=state.waiting.timer_epoch + 1,
            cooldown_at=event.at + policy.drained_fallback_seconds,
            recheck_at=event.at + policy.drained_fallback_seconds,
            gatt_at=event.at,
            advertisement=evidence.advertisement,
        )
        return _observe(cooled)
    if isinstance(event.outcome, CandidateUnavailable):
        return _observe(Searching(state.waiting.timer_epoch + 1, event.at + policy.fallback_seconds))
    if isinstance(event.outcome, ConnectedInterruption):
        return _retry_waiting(
            state.waiting,
            at=event.at,
            evidence=_Evidence(gatt_at=event.at, advertisement=evidence.advertisement),
            durable_progress=event.outcome.durable_progress,
            policy=policy,
        )
    if evidence_is_fresh(evidence.gatt_at, evidence.advertisement, at=event.at, policy=policy):
        return _retry_waiting(
            state.waiting,
            at=event.at,
            evidence=evidence,
            durable_progress=event.outcome.durable_progress,
            policy=policy,
        )
    return _observe(Searching(state.waiting.timer_epoch + 1, event.at + policy.fallback_seconds))


@dataclass(frozen=True, slots=True)
class _Evidence:
    gatt_at: float | None
    advertisement: Advertisement | None


def _attempt_evidence(state: Attempting) -> _Evidence:
    if isinstance(state.waiting, Searching):
        gatt_at = None
        advertisement = None
    else:
        gatt_at = state.waiting.gatt_at
        advertisement = state.waiting.advertisement
    trigger_advertisement = _trigger_advertisement(state.trigger)
    if trigger_advertisement is not None and (
        advertisement is None or trigger_advertisement.observed_at >= advertisement.observed_at
    ):
        advertisement = trigger_advertisement
    return _Evidence(gatt_at=gatt_at, advertisement=advertisement)


def _retry_waiting(
    waiting: WaitingState,
    evidence: _Evidence,
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
        fallback_at=at + policy.fallback_seconds,
        retry_index=retry_index + 1,
        gatt_at=evidence.gatt_at,
        advertisement=evidence.advertisement,
    )
    return _observe(retry)


def _trigger_advertisement(trigger: AttemptTrigger) -> Advertisement | None:
    return trigger.advertisement


def _begin(waiting: WaitingState, trigger: AttemptTrigger) -> TransitionResult:
    return TransitionResult(Attempting(trigger=trigger, waiting=waiting), StopAndBeginAttempt(trigger))


def _observe(state: WaitingState) -> TransitionResult:
    return TransitionResult(state, Observe(armed_deadline(state)))
