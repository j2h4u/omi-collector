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
    NoOperation,
    NotConnected,
    Observe,
    PresenceEvent,
    PresenceMachinePolicy,
    RapidRetryTrigger,
    RetryWaiting,
    ScannerInterrupted,
    Searching,
    Shutdown,
    Stop,
    StopAndBeginAttempt,
    TimerFired,
    UnexpectedAttemptOutcomeError,
    armed_deadline,
    drained_cooldown_remaining_seconds,
    transition,
)

POLICY = PresenceMachinePolicy(
    absence_seconds=10.0,
    scan_recheck_seconds=100.0,
    drain_cooldown_seconds=50.0,
    rapid_backoff=(2.0, 4.0),
    arrival_stability_seconds=5.0,
    arrival_max_gap_seconds=10.0,
)


def _advertisement(at: float, candidate: object | None = None) -> Advertisement:
    return Advertisement(candidate=object() if candidate is None else candidate, observed_at=at, rssi_dbm=-72)


def _timer(state: Searching | CoolingDown | RetryWaiting, at: float) -> TimerFired:
    return TimerFired(at=at, deadline=armed_deadline(state), timer_epoch=state.timer_epoch)


def test_search_advertisement_requires_stable_repeated_visibility() -> None:
    state = Searching(timer_epoch=3, scan_recheck_at=100.0)
    advertisement = _advertisement(9.0)

    stale = transition(state, TimerFired(at=100.0, deadline=100.0, timer_epoch=2), POLICY)
    waiting = transition(state, AdvertisementObserved(advertisement), POLICY)
    stable = _advertisement(15.0)
    released = transition(waiting.state, AdvertisementObserved(stable), POLICY)

    assert stale.state is state
    assert isinstance(stale.directive, NoOperation)
    assert waiting.directive == Observe(100.0)
    assert released.directive == StopAndBeginAttempt(AdvertisementTrigger(stable))


def test_arrival_gap_boundary_resets_qualification() -> None:
    state = Searching(timer_epoch=0, scan_recheck_at=100.0)
    first = _advertisement(0.0)
    boundary = _advertisement(10.0)
    restarted = transition(
        transition(state, AdvertisementObserved(first), POLICY).state,
        AdvertisementObserved(boundary),
        POLICY,
    )

    assert isinstance(restarted.state, Searching)
    assert restarted.state.arrival_started_at == 10.0
    assert not isinstance(restarted.directive, StopAndBeginAttempt)


def test_scanner_interruption_clears_unfinished_arrival() -> None:
    state = Searching(timer_epoch=0, scan_recheck_at=100.0)
    observed = transition(state, AdvertisementObserved(_advertisement(1.0)), POLICY)

    interrupted = transition(observed.state, ScannerInterrupted(at=2.0), POLICY)

    assert interrupted.state == Searching(timer_epoch=0, scan_recheck_at=100.0)
    assert interrupted.directive == Observe(100.0)


def test_queued_stale_observation_requires_a_new_stable_encounter() -> None:
    state = Searching(timer_epoch=0, scan_recheck_at=100.0)
    observed = transition(
        state,
        AdvertisementObserved(_advertisement(1.0), processed_at=1.0 + POLICY.arrival_max_gap_seconds),
        POLICY,
    )
    first = _advertisement(20.0)
    stable = _advertisement(25.0)
    waiting = transition(observed.state, AdvertisementObserved(first, processed_at=20.0), POLICY)
    released = transition(waiting.state, AdvertisementObserved(stable, processed_at=25.0), POLICY)

    assert observed.state == state
    assert waiting.directive == Observe(100.0)
    assert released.directive == StopAndBeginAttempt(AdvertisementTrigger(stable))


def test_cooldown_continuous_advertising_waits_for_post_cooldown_encounter() -> None:
    state = CoolingDown(timer_epoch=4, cooldown_at=50.0, recheck_at=50.0, advertisement=None)
    advertisement = _advertisement(25.0)

    refreshed = transition(state, AdvertisementObserved(advertisement), POLICY)
    assert isinstance(refreshed.state, CoolingDown)
    quiet = transition(refreshed.state, _timer(refreshed.state, 50.0), POLICY)

    assert refreshed.directive == Observe(50.0)
    assert isinstance(quiet.state, Searching)
    assert quiet.state.timer_epoch == 5
    assert quiet.directive == Observe(150.0)
    assert drained_cooldown_remaining_seconds(quiet.state, at=55.0) == 0.0
    first_after_cooldown = _advertisement(60.0)
    stable_after_cooldown = _advertisement(65.0)
    waiting_after_cooldown = transition(quiet.state, AdvertisementObserved(first_after_cooldown), POLICY)
    released = transition(waiting_after_cooldown.state, AdvertisementObserved(stable_after_cooldown), POLICY)
    assert released.directive == StopAndBeginAttempt(AdvertisementTrigger(stable_after_cooldown))


def test_retry_advertisement_begins_a_fresh_encounter_after_backoff() -> None:
    state = RetryWaiting(
        timer_epoch=1,
        retry_at=20.0,
        scan_recheck_at=100.0,
        retry_index=3,
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
    assert isinstance(restarted.state, RetryWaiting)
    assert restarted.state.advertisement is absent_advertisement
    assert restarted.state.arrival_started_at == 20.0
    assert restarted.directive == Observe(20.0)


def test_retry_backoff_returns_to_scanning_with_retained_recheck() -> None:
    state = RetryWaiting(
        timer_epoch=4,
        retry_at=20.0,
        scan_recheck_at=100.0,
        retry_index=1,
        advertisement=None,
    )

    result = transition(state, _timer(state, 20.0), POLICY)

    assert result == type(result)(
        RetryWaiting(5, None, 100.0, 1, None),
        Observe(100.0),
    )


@pytest.mark.parametrize(
    "state",
    (
        RetryWaiting(
            timer_epoch=3,
            retry_at=50.0,
            scan_recheck_at=100.0,
            retry_index=1,
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
    state = CoolingDown(timer_epoch=2, cooldown_at=50.0, recheck_at=60.0, advertisement=None)
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
        RetryWaiting(
            timer_epoch=3,
            retry_at=50.0,
            scan_recheck_at=100.0,
            retry_index=1,
            advertisement=None,
        ),
    ),
)
def test_shutdown_closes_each_waiting_state(state: CoolingDown | RetryWaiting) -> None:
    result = transition(state, Shutdown(at=20.0), POLICY)

    assert result == type(result)(Closed(), Stop())


def test_clean_drain_anchors_fifteen_minute_cooldown() -> None:
    waiting = Searching(timer_epoch=2, scan_recheck_at=100.0)
    attempting = Attempting(AdvertisementTrigger(_advertisement(20.0)), waiting)

    result = transition(attempting, AttemptFinished(at=30.0, outcome=CleanDrain()), POLICY)

    assert result == type(result)(
        CoolingDown(3, 80.0, 80.0, None),
        Observe(80.0),
    )


def test_retry_outcomes_schedule_backoff_and_durable_progress_resets_it() -> None:
    waiting = RetryWaiting(
        timer_epoch=3,
        retry_at=10.0,
        scan_recheck_at=100.0,
        retry_index=1,
        advertisement=None,
    )
    interrupted = transition(
        Attempting(RapidRetryTrigger(_advertisement(9.0)), waiting),
        AttemptFinished(at=10.0, outcome=ConnectedInterruption(durable_progress=False)),
        POLICY,
    )
    after_progress = transition(
        Attempting(RapidRetryTrigger(_advertisement(9.0)), waiting),
        AttemptFinished(at=10.0, outcome=ConnectedInterruption(durable_progress=True)),
        POLICY,
    )

    assert interrupted == type(interrupted)(RetryWaiting(4, 14.0, 110.0, 2, None), Observe(14.0))
    assert after_progress == type(after_progress)(RetryWaiting(4, 12.0, 110.0, 1, None), Observe(12.0))


def test_not_connected_requires_a_fresh_candidate_after_backoff() -> None:
    advertisement = _advertisement(10.0)
    attempting = Attempting(AdvertisementTrigger(advertisement), Searching(0, 100.0))

    retry = transition(attempting, AttemptFinished(15.0, NotConnected(durable_progress=False)), POLICY)
    unavailable = transition(attempting, AttemptFinished(15.0, CandidateUnavailable()), POLICY)

    assert isinstance(retry.state, RetryWaiting)
    assert retry.state.advertisement is None
    assert retry.state.retry_at == 17.0
    assert unavailable == type(unavailable)(Searching(1, 115.0), Observe(115.0))


def test_not_connected_without_fresh_evidence_still_waits_for_backoff() -> None:
    attempting = Attempting(AdvertisementTrigger(_advertisement(10.0)), Searching(3, 100.0))

    result = transition(attempting, AttemptFinished(15.0, NotConnected(durable_progress=False)), POLICY)

    assert result == type(result)(RetryWaiting(4, 17.0, 115.0, 1, None, None), Observe(17.0))


def test_attempting_blocks_late_timer_and_advertisement_until_one_outcome() -> None:
    state = Attempting(AdvertisementTrigger(_advertisement(10.0)), Searching(1, 100.0))

    timer = transition(state, TimerFired(100.0, 100.0, 1), POLICY)
    advertisement = transition(state, AdvertisementObserved(_advertisement(20.0)), POLICY)

    assert timer.state is state and isinstance(timer.directive, NoOperation)
    assert advertisement.state is state and isinstance(advertisement.directive, NoOperation)


def test_shutdown_is_idempotent_closed_absorbs_late_events_and_invalid_outcome_closes() -> None:
    state = Searching(timer_epoch=1, scan_recheck_at=100.0)
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
        ScannerInterrupted(at=1.0),
        TimerFired(at=1.0, deadline=1.0, timer_epoch=0),
        AttemptFinished(at=1.0, outcome=CandidateUnavailable()),
        Shutdown(at=1.0),
    ),
)
def test_closed_absorbs_every_event_class(event: PresenceEvent) -> None:
    result = transition(Closed(), event, POLICY)

    assert result == type(result)(Closed(), NoOperation())
