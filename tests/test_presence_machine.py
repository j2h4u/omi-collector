from __future__ import annotations

from collections.abc import Callable
from dataclasses import FrozenInstanceError, fields
from itertools import product
from types import UnionType
from typing import cast

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
    EndVisit,
    NoOperation,
    NotConnected,
    Observe,
    PresenceEvent,
    PresenceMachinePolicy,
    PresenceState,
    RapidRetryTrigger,
    ResumeInterruptedVisit,
    RetryWaiting,
    ScannerInterrupted,
    Searching,
    Shutdown,
    Stop,
    StopAndBeginAttempt,
    TimerFired,
    TransitionResult,
    UnexpectedAttemptOutcomeError,
    armed_deadline,
    drained_cooldown_remaining_seconds,
    initial_state,
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


@pytest.mark.parametrize(
    ("record_type", "field_count", "field_name"),
    [
        (PresenceMachinePolicy, 6, "absence_seconds"),
        (Advertisement, 3, "observed_at"),
        (RetryWaiting, 7, "retry_at"),
        (ResumeInterruptedVisit, 1, "at"),
        (NotConnected, 1, "durable_progress"),
        (AttemptFinished, 2, "at"),
        (Observe, 1, "until"),
        (TransitionResult, 2, "state"),
    ],
)
def test_presence_machine_values_are_immutable(
    record_type: type[object], field_count: int, field_name: str
) -> None:
    record_factory = cast(Callable[..., object], record_type)
    record = record_factory(*([None] * field_count))

    with pytest.raises(FrozenInstanceError):
        setattr(record, field_name, object())


def _advertisement(at: float, candidate: object | None = None) -> Advertisement:
    return Advertisement(candidate=object() if candidate is None else candidate, observed_at=at, rssi_dbm=-72)


def _timer(state: Searching | CoolingDown | RetryWaiting, at: float) -> TimerFired:
    return TimerFired(at=at, deadline=armed_deadline(state), timer_epoch=state.timer_epoch)


def test_initial_state_uses_the_public_scan_recheck_interval() -> None:
    assert initial_state(10.0, POLICY) == Searching(timer_epoch=0, scan_recheck_at=110.0)


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


def test_resume_interrupted_visit_arms_absence_without_erasing_arrival() -> None:
    advertisement = _advertisement(9.0)
    state = Searching(timer_epoch=3, scan_recheck_at=100.0, advertisement=advertisement, arrival_started_at=9.0)

    resumed = transition(state, ResumeInterruptedVisit(at=20.0), POLICY)

    assert resumed == type(resumed)(
        RetryWaiting(3, None, 100.0, 0, advertisement, 9.0, 30.0),
        Observe(30.0),
    )


@pytest.mark.parametrize(
    "waiting",
    [Searching(timer_epoch=1, scan_recheck_at=100.0), CoolingDown(1, 9.0, 100.0, None)],
)
def test_resume_interrupted_visit_preserves_an_outstanding_attempt(waiting: Searching | CoolingDown) -> None:
    attempting = Attempting(AdvertisementTrigger(_advertisement(10.0)), waiting)

    resumed = transition(attempting, ResumeInterruptedVisit(at=20.0), POLICY)

    assert isinstance(resumed.state, Attempting)
    assert isinstance(resumed.state.waiting, RetryWaiting)
    assert resumed.state.waiting.scan_recheck_at == 100.0
    assert resumed.state.trigger is attempting.trigger
    assert isinstance(resumed.directive, NoOperation)


def test_resume_during_attempt_preserves_permit_then_closes_after_unavailable() -> None:
    attempting = Attempting(
        AdvertisementTrigger(_advertisement(10.0)),
        Searching(timer_epoch=1, scan_recheck_at=100.0),
    )

    resumed = transition(attempting, ResumeInterruptedVisit(at=20.0), POLICY)

    assert isinstance(resumed.state, Attempting)
    assert isinstance(resumed.state.waiting, RetryWaiting)
    assert resumed.state.trigger is attempting.trigger
    assert isinstance(resumed.directive, NoOperation)

    unavailable = transition(
        resumed.state,
        AttemptFinished(at=21.0, outcome=CandidateUnavailable()),
        POLICY,
    )
    assert isinstance(unavailable.state, RetryWaiting)
    ended = transition(unavailable.state, _timer(unavailable.state, 30.0), POLICY)

    assert ended == type(ended)(Searching(2, 130.0), EndVisit("absence"))


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


def test_cooldown_ad_at_exact_boundary_starts_a_fresh_stability_window() -> None:
    state = CoolingDown(timer_epoch=4, cooldown_at=50.0, recheck_at=50.0, advertisement=None)
    at_boundary = _advertisement(50.0)

    refreshed = transition(state, AdvertisementObserved(at_boundary), POLICY)

    assert isinstance(refreshed.state, CoolingDown)
    assert refreshed.state.advertisement is at_boundary
    assert refreshed.state.arrival_started_at == 50.0
    assert refreshed.directive == Observe(50.0)

    stable = _advertisement(55.0)
    released = transition(refreshed.state, AdvertisementObserved(stable), POLICY)

    assert isinstance(released.state, Attempting)
    assert released.state.trigger == AdvertisementTrigger(stable)
    assert released.directive == StopAndBeginAttempt(AdvertisementTrigger(stable))


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


def test_fresh_retry_advertisements_refresh_absence_and_reject_old_timer() -> None:
    state = RetryWaiting(
        timer_epoch=1,
        retry_at=None,
        scan_recheck_at=100.0,
        retry_index=1,
        advertisement=None,
        absence_at=20.0,
    )

    refreshed = transition(state, AdvertisementObserved(_advertisement(15.0)), POLICY)
    assert isinstance(refreshed.state, RetryWaiting)
    stale = transition(
        refreshed.state,
        TimerFired(at=20.0, deadline=20.0, timer_epoch=refreshed.state.timer_epoch),
        POLICY,
    )

    assert refreshed.state.absence_at == 25.0
    assert refreshed.directive == Observe(25.0)
    assert stale.state is refreshed.state and isinstance(stale.directive, NoOperation)


def test_retry_absence_ends_visit_without_an_attempt_permit() -> None:
    waiting = RetryWaiting(
        timer_epoch=1,
        retry_at=None,
        scan_recheck_at=100.0,
        retry_index=1,
        advertisement=None,
        absence_at=20.0,
    )

    ended = transition(waiting, _timer(waiting, 20.0), POLICY)

    assert ended == type(ended)(Searching(2, 120.0), EndVisit("absence"))


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


def test_search_timer_at_its_exact_deadline_advances_epoch_and_recheck() -> None:
    state = Searching(timer_epoch=4, scan_recheck_at=100.0)

    result = transition(state, TimerFired(at=100.0, deadline=100.0, timer_epoch=4), POLICY)

    assert result == type(result)(Searching(timer_epoch=5, scan_recheck_at=200.0), Observe(200.0))


def test_early_cooldown_timer_with_matching_deadline_keeps_cooling_down() -> None:
    state = CoolingDown(timer_epoch=4, cooldown_at=50.0, recheck_at=50.0, advertisement=None)

    result = transition(state, TimerFired(at=49.0, deadline=50.0, timer_epoch=4), POLICY)

    assert result.state is state
    assert result.directive == Observe(50.0)


def test_scan_recheck_before_retry_preserves_retry_deadline() -> None:
    state = RetryWaiting(
        timer_epoch=4,
        retry_at=30.0,
        scan_recheck_at=10.0,
        retry_index=1,
        advertisement=None,
        absence_at=200.0,
    )

    result = transition(state, TimerFired(at=10.0, deadline=10.0, timer_epoch=4), POLICY)

    assert result == type(result)(RetryWaiting(5, 30.0, 110.0, 1, None, None, 200.0), Observe(30.0))


def test_equal_retry_and_scan_deadlines_clear_retry_and_recheck_from_callback() -> None:
    state = RetryWaiting(
        timer_epoch=4,
        retry_at=10.0,
        scan_recheck_at=10.0,
        retry_index=1,
        advertisement=None,
        absence_at=200.0,
    )

    result = transition(state, TimerFired(at=10.0, deadline=10.0, timer_epoch=4), POLICY)

    assert result == type(result)(RetryWaiting(5, None, 110.0, 1, None, None, 200.0), Observe(110.0))


@pytest.mark.parametrize(("at", "expected_recheck"), [(10.0, 110.0), (11.0, 111.0)])
def test_retry_without_a_retry_deadline_rechecks_at_or_after_scan_deadline(at: float, expected_recheck: float) -> None:
    state = RetryWaiting(
        timer_epoch=4,
        retry_at=None,
        scan_recheck_at=10.0,
        retry_index=1,
        advertisement=None,
        absence_at=200.0,
    )

    result = transition(state, TimerFired(at=at, deadline=10.0, timer_epoch=4), POLICY)

    assert result == type(result)(
        RetryWaiting(5, None, expected_recheck, 1, None, None, 200.0), Observe(expected_recheck)
    )


def test_duplicate_advertisement_timestamp_restarts_arrival_stability_window() -> None:
    state: PresenceState = Searching(timer_epoch=0, scan_recheck_at=100.0)
    observations = [_advertisement(at) for at in (1.0, 4.0, 4.0, 6.0)]
    result = None

    for advertisement in observations:
        result = transition(state, AdvertisementObserved(advertisement), POLICY)
        assert not isinstance(result.state, Attempting)
        assert not isinstance(result.directive, StopAndBeginAttempt)
        state = result.state

    assert isinstance(state, Searching)
    assert state.arrival_started_at == 4.0
    assert result is not None and result.directive == Observe(100.0)

    stable = _advertisement(9.0)
    released = transition(state, AdvertisementObserved(stable), POLICY)

    assert isinstance(released.state, Attempting)
    assert released.state.trigger == AdvertisementTrigger(stable)
    assert released.directive == StopAndBeginAttempt(AdvertisementTrigger(stable))


@pytest.mark.parametrize(
    "state",
    (
        Searching(timer_epoch=3, scan_recheck_at=50.0),
        CoolingDown(timer_epoch=3, cooldown_at=20.0, recheck_at=50.0, advertisement=None),
        RetryWaiting(
            timer_epoch=3,
            retry_at=50.0,
            scan_recheck_at=100.0,
            retry_index=1,
            advertisement=None,
        ),
    ),
)
def test_timer_epoch_and_scheduled_deadline_must_both_match(state: Searching | CoolingDown | RetryWaiting) -> None:
    stale_epoch = transition(
        state, TimerFired(at=50.0, deadline=armed_deadline(state), timer_epoch=state.timer_epoch - 1), POLICY
    )
    stale_deadline = transition(state, TimerFired(at=50.0, deadline=49.0, timer_epoch=state.timer_epoch), POLICY)

    assert stale_epoch.state is state and isinstance(stale_epoch.directive, NoOperation)
    assert stale_deadline.state is state and isinstance(stale_deadline.directive, NoOperation)


def test_late_search_timer_refreshes_the_recheck_from_callback_time() -> None:
    state = Searching(timer_epoch=4, scan_recheck_at=100.0)

    result = transition(state, TimerFired(at=101.0, deadline=100.0, timer_epoch=4), POLICY)

    assert result == type(result)(Searching(5, 201.0), Observe(201.0))


def test_late_cooldown_timer_returns_to_searching_from_callback_time() -> None:
    state = CoolingDown(timer_epoch=6, cooldown_at=50.0, recheck_at=50.0, advertisement=None)

    result = transition(state, TimerFired(at=51.0, deadline=50.0, timer_epoch=6), POLICY)

    assert result == type(result)(Searching(7, 151.0), Observe(151.0))


def test_late_retry_absence_timer_ends_visit_without_a_permit() -> None:
    state = RetryWaiting(
        timer_epoch=8,
        retry_at=None,
        scan_recheck_at=100.0,
        retry_index=1,
        advertisement=None,
        absence_at=20.0,
    )

    result = transition(state, TimerFired(at=21.0, deadline=20.0, timer_epoch=8), POLICY)

    assert result == type(result)(Searching(9, 121.0), EndVisit("absence"))


def test_delayed_search_observations_within_gap_retain_the_trigger_candidate() -> None:
    state = Searching(timer_epoch=2, scan_recheck_at=100.0)
    first = _advertisement(1.0)
    candidate = object()
    second = _advertisement(6.0, candidate=candidate)

    waiting = transition(state, AdvertisementObserved(first, processed_at=2.0), POLICY)
    released = transition(waiting.state, AdvertisementObserved(second, processed_at=7.0), POLICY)

    assert waiting.directive == Observe(100.0)
    assert isinstance(released.directive, StopAndBeginAttempt)
    assert isinstance(released.directive.trigger, AdvertisementTrigger)
    assert released.directive.trigger.advertisement is second
    assert released.directive.trigger.advertisement.candidate is candidate


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

    assert interrupted == type(interrupted)(RetryWaiting(4, 14.0, 110.0, 2, None, None, 20.0), Observe(14.0))
    assert isinstance(interrupted.state, RetryWaiting)
    exhausted = transition(
        Attempting(RapidRetryTrigger(_advertisement(14.0)), interrupted.state),
        AttemptFinished(at=14.0, outcome=ConnectedInterruption(durable_progress=False)),
        POLICY,
    )
    assert exhausted == type(exhausted)(Searching(5, 114.0), EndVisit("recovery_exhausted"))
    assert after_progress == type(after_progress)(RetryWaiting(4, 12.0, 110.0, 0, None, None, 20.0), Observe(12.0))


def test_durable_progress_resets_exhaustion_counter_for_next_failure() -> None:
    first = transition(
        Attempting(AdvertisementTrigger(_advertisement(10.0)), Searching(0, 100.0)),
        AttemptFinished(10.0, NotConnected(durable_progress=False)),
        POLICY,
    )
    assert isinstance(first.state, RetryWaiting)
    progress = transition(
        Attempting(RapidRetryTrigger(_advertisement(11.0)), first.state),
        AttemptFinished(11.0, ConnectedInterruption(durable_progress=True)),
        POLICY,
    )
    assert isinstance(progress.state, RetryWaiting)
    next_failure = transition(
        Attempting(RapidRetryTrigger(_advertisement(12.0)), progress.state),
        AttemptFinished(12.0, ConnectedInterruption(durable_progress=False)),
        POLICY,
    )

    assert progress.state.retry_index == 0
    assert isinstance(next_failure.state, RetryWaiting)
    assert next_failure.state.retry_index == 1


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

    assert result == type(result)(RetryWaiting(4, 17.0, 115.0, 1, None, None, 25.0), Observe(17.0))


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


def test_finite_model_is_exhaustive_reachable_and_rejects_invalid_outcomes() -> None:
    states = _model_states()
    events = _model_events(states)
    assert {type(state) for state in states} == _type_alias_members(cast(object, PresenceState.__value__))
    assert {type(event) for event in events} == _type_alias_members(cast(object, PresenceEvent.__value__))
    assert (len(states), len(events)) == (10, 37)
    assert len(states) == len(set(states))
    assert len(events) == len(set(events))
    assert {type(event.outcome) for event in events if isinstance(event, AttemptFinished)} == {
        CleanDrain,
        NotConnected,
        ConnectedInterruption,
        CandidateUnavailable,
    }
    assert {
        event.outcome.durable_progress
        for event in events
        if isinstance(event, AttemptFinished) and isinstance(event.outcome, (NotConnected, ConnectedInterruption))
    } == {False, True}
    assert {event.processed_at for event in events if isinstance(event, AdvertisementObserved)} == {
        None,
        1.0,
        0.0,
        11.0,
        6.0,
    }
    assert tuple(field.name for field in fields(TimerFired)) == ("at", "deadline", "timer_epoch")

    rejected = 0
    # Exercise every representative state/event pair, including invalid outcomes.
    for state, event in product(states, events):
        should_reject = isinstance(event, AttemptFinished) and not isinstance(state, (Attempting, Closed))
        try:
            result = transition(state, event, POLICY)
        except UnexpectedAttemptOutcomeError as error:
            assert should_reject
            assert error.closed_state == Closed()
            assert error.directive == Stop()
            rejected += 1
            continue
        assert not should_reject
        assert isinstance(result.state, (Searching, CoolingDown, RetryWaiting, Attempting, Closed))
        assert isinstance(result.directive, (Observe, StopAndBeginAttempt, Stop, NoOperation, EndVisit))
        if isinstance(state, Attempting) and not isinstance(event, (AttemptFinished, Shutdown, ResumeInterruptedVisit)):
            assert result == type(result)(state, NoOperation())
        if isinstance(state, Closed):
            assert result == type(result)(state, NoOperation())

    assert rejected == sum(1 for state in states if not isinstance(state, (Attempting, Closed))) * sum(
        1 for event in events if isinstance(event, AttemptFinished)
    )
    _assert_reachable_state_types()


def _model_states() -> tuple[PresenceState, ...]:
    advertisement = _advertisement(1.0)
    search = Searching(0, 100.0)
    cooled = CoolingDown(1, 20.0, 100.0, None)
    retry = RetryWaiting(2, 10.0, 100.0, 0, advertisement, 1.0, 50.0)
    return (
        search,
        Searching(1, 100.0, advertisement, 1.0),
        cooled,
        CoolingDown(2, 20.0, 100.0, advertisement, 1.0),
        RetryWaiting(3, None, 100.0, 0, None),
        retry,
        RetryWaiting(4, 10.0, 100.0, len(POLICY.rapid_backoff), None, None, 50.0),
        Attempting(AdvertisementTrigger(advertisement), search),
        Attempting(RapidRetryTrigger(advertisement), retry),
        Closed(),
    )


def _model_events(states: tuple[PresenceState, ...]) -> tuple[PresenceEvent, ...]:
    events: list[PresenceEvent] = [
        AdvertisementObserved(_advertisement(1.0)),
        AdvertisementObserved(_advertisement(1.0), processed_at=1.0),
        AdvertisementObserved(_advertisement(1.0), processed_at=0.0),
        AdvertisementObserved(_advertisement(1.0), processed_at=11.0),
        AdvertisementObserved(_advertisement(1.0), processed_at=6.0),
        AdvertisementObserved(_advertisement(6.0)),
        AdvertisementObserved(_advertisement(6.0), processed_at=6.0),
        ScannerInterrupted(1.0),
        ResumeInterruptedVisit(20.0),
        AttemptFinished(10.0, CleanDrain()),
        AttemptFinished(10.0, NotConnected(False)),
        AttemptFinished(10.0, NotConnected(True)),
        AttemptFinished(10.0, ConnectedInterruption(False)),
        AttemptFinished(10.0, ConnectedInterruption(True)),
        AttemptFinished(10.0, CandidateUnavailable()),
        Shutdown(20.0),
    ]
    for state in states:
        if isinstance(state, (Searching, CoolingDown, RetryWaiting)):
            deadline = armed_deadline(state)
            events.extend(
                (
                    TimerFired(deadline, deadline, state.timer_epoch),
                    TimerFired(deadline, deadline, state.timer_epoch + 1),
                    TimerFired(deadline, deadline - 1.0, state.timer_epoch),
                    TimerFired(deadline - 1.0, deadline, state.timer_epoch),
                )
            )
    return tuple(dict.fromkeys(events))


def _assert_reachable_state_types() -> None:
    initial = initial_state(0.0, POLICY)
    observed = transition(initial, AdvertisementObserved(_advertisement(1.0)), POLICY).state
    attempting = transition(observed, AdvertisementObserved(_advertisement(6.0)), POLICY).state
    assert isinstance(attempting, Attempting)
    drained = transition(attempting, AttemptFinished(6.0, CleanDrain()), POLICY).state
    interrupted = transition(attempting, AttemptFinished(6.0, ConnectedInterruption(False)), POLICY).state
    closed = transition(interrupted, Shutdown(7.0), POLICY).state
    assert {type(initial), type(attempting), type(drained), type(interrupted), type(closed)} == set(
        _type_alias_members(cast(object, PresenceState.__value__))
    )


def _type_alias_members(value: object) -> set[type]:
    if not isinstance(value, UnionType):
        raise TypeError("expected a union type alias")
    arguments = cast(tuple[object, ...], value.__args__)
    return {member for member in arguments if isinstance(member, type)}


def test_retry_failures_reach_a_bounded_terminal_path_under_timer_fairness() -> None:
    state: Searching | RetryWaiting = Searching(0, 100.0)
    elapsed = 0.0
    for retry_number, delay in enumerate((*POLICY.rapid_backoff, 0.0)):
        failed = transition(
            Attempting(RapidRetryTrigger(_advertisement(elapsed)), state),
            AttemptFinished(elapsed, ConnectedInterruption(durable_progress=False)),
            POLICY,
        )
        if retry_number == len(POLICY.rapid_backoff):
            assert failed.directive == EndVisit("recovery_exhausted")
            assert isinstance(failed.state, Searching)
            assert failed.state.scan_recheck_at == elapsed + POLICY.scan_recheck_seconds
            break
        assert isinstance(failed.state, RetryWaiting)
        assert failed.state.retry_at == elapsed + delay
        assert failed.state.retry_at is not None
        timer = transition(failed.state, _timer(failed.state, failed.state.retry_at), POLICY)
        assert isinstance(timer.state, RetryWaiting)
        assert timer.state.retry_at is None
        state = timer.state
        elapsed += delay
    assert elapsed == sum(POLICY.rapid_backoff)


def test_unknown_event_is_rejected_even_after_shutdown() -> None:
    for state in (Searching(0, 100.0), Closed()):
        with pytest.raises(TypeError, match="unsupported presence event: object"):
            transition(state, object(), POLICY)  # type: ignore[arg-type]


def test_unknown_state_and_attempt_outcome_are_rejected() -> None:
    with pytest.raises(TypeError, match="unsupported presence state: object"):
        transition(object(), Shutdown(1.0), POLICY)  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="unsupported attempt outcome: object"):
        transition(
            Attempting(AdvertisementTrigger(_advertisement(1.0)), Searching(0, 100.0)),
            AttemptFinished(2.0, object()),  # type: ignore[arg-type]
            POLICY,
        )
