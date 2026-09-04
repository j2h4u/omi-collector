from __future__ import annotations

import pytest

from omi_collector.capture.application.presence_machine import (
    Advertisement,
    AdvertisementObserved,
    AdvertisementTrigger,
    AttemptFinished,
    Attempting,
    CandidateUnavailable,
    CleanDrain,
    Closed,
    ConnectedInterruption,
    CoolingDown,
    FallbackTrigger,
    NoOperation,
    NotConnected,
    Observe,
    PresenceEvent,
    PresenceMachinePolicy,
    RapidRetryTrigger,
    RetryWaiting,
    Searching,
    Shutdown,
    Stop,
    StopAndBeginAttempt,
    TimerFired,
    UnexpectedAttemptOutcomeError,
    armed_deadline,
    drained_cooldown_remaining_seconds,
    evidence_is_fresh,
    fresh_advertisement,
    initial_state,
    latest_presence_at,
    transition,
)

POLICY = PresenceMachinePolicy(
    absence_seconds=10.0,
    fallback_seconds=100.0,
    drained_fallback_seconds=50.0,
    rapid_backoff=(2.0, 4.0),
)


def _advertisement(at: float, candidate: object | None = None) -> Advertisement:
    return Advertisement(candidate=object() if candidate is None else candidate, observed_at=at, rssi_dbm=-72)


def _timer(state: Searching | CoolingDown | RetryWaiting, at: float) -> TimerFired:
    return TimerFired(at=at, deadline=armed_deadline(state), timer_epoch=state.timer_epoch)


def test_initial_search_releases_only_one_fallback_at_its_exact_deadline() -> None:
    state = initial_state(5.0, POLICY)

    early = transition(state, _timer(state, 104.9), POLICY)
    ready = transition(state, _timer(state, 105.0), POLICY)

    assert early == type(early)(state, Observe(105.0))
    assert isinstance(ready.state, Attempting)
    assert isinstance(ready.directive, StopAndBeginAttempt)
    assert ready.directive.trigger == FallbackTrigger(None)


def test_search_advertisement_releases_candidate_and_stale_timer_is_inert() -> None:
    state = Searching(timer_epoch=3, fallback_at=100.0)
    advertisement = _advertisement(9.0)

    stale = transition(state, TimerFired(at=100.0, deadline=100.0, timer_epoch=2), POLICY)
    released = transition(state, AdvertisementObserved(advertisement), POLICY)

    assert stale.state is state
    assert isinstance(stale.directive, NoOperation)
    assert released.directive == StopAndBeginAttempt(AdvertisementTrigger(advertisement))


def test_freshness_uses_later_evidence_and_is_strict_at_the_boundary() -> None:
    advertisement = _advertisement(20.0)

    assert latest_presence_at(15.0, advertisement) == 20.0
    assert evidence_is_fresh(15.0, advertisement, at=29.999, policy=POLICY)
    assert not evidence_is_fresh(15.0, advertisement, at=30.0, policy=POLICY)
    assert fresh_advertisement(advertisement, at=29.999, policy=POLICY) is advertisement
    assert fresh_advertisement(advertisement, at=30.0, policy=POLICY) is None
    assert "candidate=" not in repr(advertisement)


def test_later_gatt_proof_wins_over_an_older_advertisement() -> None:
    advertisement = _advertisement(20.0)

    assert latest_presence_at(30.0, advertisement) == 30.0
    assert evidence_is_fresh(30.0, advertisement, at=39.999, policy=POLICY)
    assert not evidence_is_fresh(30.0, advertisement, at=40.0, policy=POLICY)


def test_cooldown_continuous_advertising_cannot_bypass_and_quiet_rechecks() -> None:
    state = CoolingDown(timer_epoch=4, cooldown_at=50.0, recheck_at=50.0, gatt_at=0.0, advertisement=None)
    advertisement = _advertisement(25.0)

    refreshed = transition(state, AdvertisementObserved(advertisement), POLICY)
    assert isinstance(refreshed.state, CoolingDown)
    quiet = transition(refreshed.state, _timer(refreshed.state, 50.0), POLICY)

    assert refreshed.directive == Observe(50.0)
    assert isinstance(quiet.state, CoolingDown)
    assert quiet.state.timer_epoch == 5
    assert quiet.state.cooldown_at == 50.0
    assert quiet.state.recheck_at == 60.0
    assert quiet.directive == Observe(60.0)
    assert drained_cooldown_remaining_seconds(quiet.state, at=55.0) == 0.0


def test_cooldown_fresh_evidence_uses_fallback_and_returning_advertisement_wakes() -> None:
    fresh = CoolingDown(timer_epoch=1, cooldown_at=50.0, recheck_at=50.0, gatt_at=45.0, advertisement=None)
    fallback = transition(fresh, _timer(fresh, 50.0), POLICY)
    quiet = CoolingDown(timer_epoch=2, cooldown_at=50.0, recheck_at=60.0, gatt_at=0.0, advertisement=None)
    returning = _advertisement(55.0)
    wake = transition(quiet, AdvertisementObserved(returning), POLICY)

    assert fallback.directive == StopAndBeginAttempt(FallbackTrigger(None))
    assert wake.directive == StopAndBeginAttempt(AdvertisementTrigger(returning))


def test_retry_advertisement_refreshes_or_resets_progress_after_confirmed_absence() -> None:
    state = RetryWaiting(
        timer_epoch=1,
        retry_at=20.0,
        fallback_at=100.0,
        retry_index=3,
        gatt_at=5.0,
        advertisement=None,
    )
    refreshed_advertisement = _advertisement(10.0)
    refreshed = transition(state, AdvertisementObserved(refreshed_advertisement), POLICY)
    absent_advertisement = _advertisement(20.0)
    restarted = transition(state, AdvertisementObserved(absent_advertisement), POLICY)

    assert isinstance(refreshed.state, RetryWaiting)
    assert refreshed.state.retry_index == 3
    assert refreshed.state.retry_at == 20.0
    assert refreshed.directive == Observe(20.0)
    assert isinstance(restarted.state, Attempting)
    assert isinstance(restarted.state.waiting, RetryWaiting)
    assert restarted.state.waiting.retry_index == 0
    assert restarted.directive == StopAndBeginAttempt(AdvertisementTrigger(absent_advertisement))


def test_retry_deadline_prefers_rapid_when_fresh_and_fallback_when_stale_at_tie() -> None:
    fresh = RetryWaiting(
        timer_epoch=4,
        retry_at=20.0,
        fallback_at=20.0,
        retry_index=1,
        gatt_at=15.0,
        advertisement=None,
    )
    stale = RetryWaiting(
        timer_epoch=4,
        retry_at=20.0,
        fallback_at=20.0,
        retry_index=1,
        gatt_at=10.0,
        advertisement=None,
    )

    rapid = transition(fresh, _timer(fresh, 20.0), POLICY)
    fallback = transition(stale, _timer(stale, 20.0), POLICY)

    assert rapid.directive == StopAndBeginAttempt(RapidRetryTrigger(None))
    assert fallback.directive == StopAndBeginAttempt(FallbackTrigger(None))


def test_stale_retry_before_fallback_returns_to_search_with_retained_fallback() -> None:
    state = RetryWaiting(
        timer_epoch=4,
        retry_at=20.0,
        fallback_at=100.0,
        retry_index=1,
        gatt_at=10.0,
        advertisement=None,
    )

    result = transition(state, _timer(state, 20.0), POLICY)

    assert result == type(result)(Searching(timer_epoch=5, fallback_at=100.0), Observe(100.0))


@pytest.mark.parametrize(
    "state",
    (
        CoolingDown(timer_epoch=2, cooldown_at=50.0, recheck_at=50.0, gatt_at=45.0, advertisement=None),
        RetryWaiting(
            timer_epoch=3,
            retry_at=50.0,
            fallback_at=100.0,
            retry_index=1,
            gatt_at=45.0,
            advertisement=None,
        ),
    ),
)
def test_timer_epoch_and_scheduled_deadline_must_both_match(state: CoolingDown | RetryWaiting) -> None:
    stale_epoch = transition(
        state, TimerFired(at=50.0, deadline=armed_deadline(state), timer_epoch=state.timer_epoch - 1), POLICY
    )
    stale_deadline = transition(state, TimerFired(at=50.0, deadline=49.0, timer_epoch=state.timer_epoch), POLICY)

    assert stale_epoch.state is state and isinstance(stale_epoch.directive, NoOperation)
    assert stale_deadline.state is state and isinstance(stale_deadline.directive, NoOperation)


def test_deadline_projection_and_noop_transition_do_not_mutate_waiting_state() -> None:
    state = CoolingDown(timer_epoch=2, cooldown_at=50.0, recheck_at=60.0, gatt_at=0.0, advertisement=None)
    before = repr(state)

    first = armed_deadline(state)
    second = armed_deadline(state)
    result = transition(state, TimerFired(at=50.0, deadline=50.0, timer_epoch=1), POLICY)

    assert first == second == 60.0
    assert result.state is state
    assert repr(state) == before


@pytest.mark.parametrize(
    "state",
    (
        CoolingDown(timer_epoch=2, cooldown_at=50.0, recheck_at=60.0, gatt_at=0.0, advertisement=None),
        RetryWaiting(
            timer_epoch=3,
            retry_at=50.0,
            fallback_at=100.0,
            retry_index=1,
            gatt_at=0.0,
            advertisement=None,
        ),
    ),
)
def test_shutdown_closes_each_waiting_state(state: CoolingDown | RetryWaiting) -> None:
    result = transition(state, Shutdown(at=20.0), POLICY)

    assert result == type(result)(Closed(), Stop())


def test_ordinary_fallback_wins_when_it_precedes_retry() -> None:
    state = RetryWaiting(
        timer_epoch=4,
        retry_at=80.0,
        fallback_at=50.0,
        retry_index=1,
        gatt_at=49.0,
        advertisement=None,
    )

    result = transition(state, _timer(state, 50.0), POLICY)

    assert result.directive == StopAndBeginAttempt(FallbackTrigger(None))


def test_clean_drain_anchors_cooldown_with_gatt_proof() -> None:
    waiting = Searching(timer_epoch=2, fallback_at=100.0)
    attempting = Attempting(FallbackTrigger(None), waiting)

    result = transition(attempting, AttemptFinished(at=30.0, outcome=CleanDrain()), POLICY)

    assert result == type(result)(
        CoolingDown(timer_epoch=3, cooldown_at=80.0, recheck_at=80.0, gatt_at=30.0, advertisement=None),
        Observe(80.0),
    )


def test_retry_outcomes_schedule_backoff_and_durable_progress_resets_it() -> None:
    waiting = RetryWaiting(
        timer_epoch=3,
        retry_at=10.0,
        fallback_at=100.0,
        retry_index=1,
        gatt_at=9.0,
        advertisement=None,
    )
    interrupted = transition(
        Attempting(RapidRetryTrigger(None), waiting),
        AttemptFinished(at=10.0, outcome=ConnectedInterruption(durable_progress=False)),
        POLICY,
    )
    after_progress = transition(
        Attempting(RapidRetryTrigger(None), waiting),
        AttemptFinished(at=10.0, outcome=ConnectedInterruption(durable_progress=True)),
        POLICY,
    )

    assert interrupted == type(interrupted)(RetryWaiting(4, 14.0, 110.0, 2, 10.0, None), Observe(14.0))
    assert after_progress == type(after_progress)(RetryWaiting(4, 12.0, 110.0, 1, 10.0, None), Observe(12.0))


def test_not_connected_uses_trigger_evidence_but_candidate_unavailable_clears_it() -> None:
    advertisement = _advertisement(10.0)
    attempting = Attempting(AdvertisementTrigger(advertisement), Searching(0, 100.0))

    retry = transition(attempting, AttemptFinished(15.0, NotConnected(durable_progress=False)), POLICY)
    unavailable = transition(attempting, AttemptFinished(15.0, CandidateUnavailable()), POLICY)

    assert isinstance(retry.state, RetryWaiting)
    assert retry.state.advertisement is advertisement
    assert unavailable == type(unavailable)(Searching(1, 115.0), Observe(115.0))


def test_not_connected_without_fresh_evidence_returns_to_search() -> None:
    attempting = Attempting(FallbackTrigger(None), Searching(3, 100.0))

    result = transition(attempting, AttemptFinished(15.0, NotConnected(durable_progress=False)), POLICY)

    assert result == type(result)(Searching(4, 115.0), Observe(115.0))


def test_attempting_blocks_late_timer_and_advertisement_until_one_outcome() -> None:
    state = Attempting(FallbackTrigger(None), Searching(1, 100.0))

    timer = transition(state, TimerFired(100.0, 100.0, 1), POLICY)
    advertisement = transition(state, AdvertisementObserved(_advertisement(20.0)), POLICY)

    assert timer.state is state and isinstance(timer.directive, NoOperation)
    assert advertisement.state is state and isinstance(advertisement.directive, NoOperation)


def test_shutdown_is_idempotent_closed_absorbs_late_events_and_invalid_outcome_closes() -> None:
    state = Searching(timer_epoch=1, fallback_at=100.0)
    closed = transition(state, Shutdown(10.0), POLICY)
    late = transition(closed.state, AttemptFinished(11.0, CleanDrain()), POLICY)

    with pytest.raises(UnexpectedAttemptOutcomeError) as error:
        transition(state, AttemptFinished(11.0, CleanDrain()), POLICY)

    assert closed == type(closed)(Closed(), Stop())
    assert late == type(late)(Closed(), NoOperation())
    assert error.value.closed_state == Closed()
    assert error.value.directive == Stop()


@pytest.mark.parametrize(
    "event",
    (
        AdvertisementObserved(_advertisement(1.0)),
        TimerFired(at=1.0, deadline=1.0, timer_epoch=0),
        AttemptFinished(at=1.0, outcome=CandidateUnavailable()),
        Shutdown(at=1.0),
    ),
)
def test_closed_absorbs_every_event_class(event: PresenceEvent) -> None:
    result = transition(Closed(), event, POLICY)

    assert result == type(result)(Closed(), NoOperation())
