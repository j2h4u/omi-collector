from __future__ import annotations

import asyncio
from collections.abc import Callable
from dataclasses import FrozenInstanceError

import pytest

from omi_collector.capture.application import presence as presence_module
from omi_collector.capture.application.presence import (
    PresenceAdvertisement,
    PresenceEnd,
    PresencePolicy,
    PresenceScanStopError,
    PresenceScanTransitionError,
    PresenceScheduler,
    PresenceWake,
)
from omi_collector.capture.application.presence_machine import (
    CandidateUnavailable,
    CleanDrain,
    ConnectedInterruption,
    NotConnected,
)
from omi_collector.config import PresenceConfig

_READINESS_TIMEOUT_SECONDS = 5.0


def _run(coro: object) -> object:
    return asyncio.run(coro)  # type: ignore[arg-type]


class FakeObserver:
    def __init__(self) -> None:
        self.callback: Callable[[object], object] | None = None
        self.callbacks: list[Callable[[object], object]] = []
        self.events: list[str] = []
        self.active = False

    async def start(self, callback: Callable[[object], object]) -> None:
        self.callback = callback
        self.callbacks.append(callback)
        self.events.append("start")
        self.active = True

    async def stop(self) -> None:
        self.events.append("stop")
        self.active = False

    def emit(self, candidate: object | None = None) -> None:
        assert self.callback is not None
        self.callback(PresenceAdvertisement(object() if candidate is None else candidate, -72))


def _test_policy(**kwargs: object) -> PresencePolicy:
    kwargs.setdefault("arrival_stability_seconds", 0.000001)
    kwargs.setdefault("arrival_max_gap_seconds", 1.0)
    return PresencePolicy(**kwargs)  # type: ignore[arg-type]


async def _emit_stable(
    observer: FakeObserver, candidate: object | None = None, *, now: list[float] | None = None
) -> None:
    for _ in range(2):
        if now is not None:
            now[0] += 0.00001
        observer.emit(candidate)
        await asyncio.sleep(0)


def _emit_pair(
    callback: Callable[[object], object], candidate: object, now: list[float], first: float, second: float
) -> None:
    now[0] = first
    callback(PresenceAdvertisement(candidate, -72))
    now[0] = second
    callback(PresenceAdvertisement(candidate, -72))


def _emit_at(callback: Callable[[object], object], candidate: object, now: list[float], at: float) -> None:
    now[0] = at
    callback(PresenceAdvertisement(candidate, -72))


def _after_loop_turns(turns: int, callback: Callable[[], None]) -> None:
    loop = asyncio.get_running_loop()

    def hop(remaining: int) -> None:
        if remaining == 0:
            callback()
        else:
            loop.call_soon(hop, remaining - 1)

    loop.call_soon(hop, turns - 1)


async def _cancel_waiter_and_close[T](
    scheduler: PresenceScheduler,
    waiter: asyncio.Task[T],
    closing: asyncio.Task[None] | None = None,
) -> None:
    if not waiter.done():
        waiter.cancel()
    if closing is not None and not closing.done():
        closing.cancel()
    await asyncio.gather(waiter, return_exceptions=True)
    if closing is not None:
        await asyncio.gather(closing, return_exceptions=True)
    await scheduler.close()


async def _wait_started(
    observer: FakeObserver,
    scheduler: PresenceScheduler,
    waiter: asyncio.Task[object],
    starts: int = 1,
) -> None:
    try:
        async with asyncio.timeout(_READINESS_TIMEOUT_SECONDS):
            while len(observer.callbacks) < starts:
                await asyncio.sleep(0)
    except TimeoutError:
        await _cancel_waiter_and_close(scheduler, waiter)
        raise
    except asyncio.CancelledError:
        await _cancel_waiter_and_close(scheduler, waiter)
        raise


def test_bleak_observer_requests_duplicate_data_and_filters_exact_address() -> None:
    pytest.importorskip("bleak")
    from omi_collector.capture.adapters.bleak_transport import BleakPresenceObserver

    async def scenario() -> None:
        kwargs: dict[str, object] = {}
        events: list[str] = []

        class Device:
            def __init__(self, address: str) -> None:
                self.address = address

        class Scanner:
            def __init__(self, options: dict[str, object]) -> None:
                self.callback = options["detection_callback"]

            async def start(self) -> None:
                events.append("start")

            async def stop(self) -> None:
                events.append("stop")

        def factory(**options: object) -> Scanner:
            kwargs.update(options)
            return Scanner(options)

        observer = BleakPresenceObserver("AA:BB", "hci0", scanner_factory=factory)
        seen: list[object] = []
        await observer.start(seen.append)
        callback = kwargs["detection_callback"]
        assert callable(callback)

        class Advertisement:
            rssi = -73

        callback(Device("AA:CC"), Advertisement())  # type: ignore[operator]
        matching = Device("aa:bb")
        callback(matching, Advertisement())  # type: ignore[operator]
        await observer.stop()

        assert seen == [PresenceAdvertisement(matching, -73)]
        assert kwargs["bluez"] == {"adapter": "hci0", "filters": {"DuplicateData": True}}
        assert events == ["start", "stop"]

    _run(scenario())


def test_bleak_observer_retains_scanner_when_failed_start_cleanup_fails() -> None:
    pytest.importorskip("bleak")
    from omi_collector.capture.adapters.bleak_transport import BleakPresenceObserver

    async def scenario() -> None:
        class Scanner:
            def __init__(self, *, fail_start: bool) -> None:
                self.fail_start = fail_start
                self.stop_calls = 0

            async def start(self) -> None:
                if self.fail_start:
                    raise RuntimeError("scanner start failed")

            async def stop(self) -> None:
                self.stop_calls += 1
                if self.fail_start and self.stop_calls == 1:
                    raise RuntimeError("scanner cleanup stop failed")

        scanners: list[Scanner] = []

        def factory(**options: object) -> Scanner:
            del options
            scanner = Scanner(fail_start=not scanners)
            scanners.append(scanner)
            return scanner

        observer = BleakPresenceObserver("AA:BB", scanner_factory=factory)
        with pytest.raises(RuntimeError, match="scanner start failed"):
            await observer.start(lambda _observation: None)

        assert len(scanners) == 1
        assert scanners[0].stop_calls == 1
        with pytest.raises(RuntimeError, match="already active"):
            await observer.start(lambda _observation: None)
        assert len(scanners) == 1

        await observer.stop()
        assert scanners[0].stop_calls == 2
        await observer.start(lambda _observation: None)
        assert len(scanners) == 2
        await observer.stop()

    _run(scenario())


def test_bleak_observer_retains_scanner_after_stop_failure_until_retry() -> None:
    pytest.importorskip("bleak")
    from omi_collector.capture.adapters.bleak_transport import BleakPresenceObserver

    async def scenario() -> None:
        class Scanner:
            def __init__(self, *, fail_stop: bool) -> None:
                self.fail_stop = fail_stop
                self.stop_calls = 0

            async def start(self) -> None:
                return None

            async def stop(self) -> None:
                self.stop_calls += 1
                if self.fail_stop and self.stop_calls == 1:
                    raise RuntimeError("scanner stop failed")

        scanners: list[Scanner] = []

        def factory(**options: object) -> Scanner:
            del options
            scanner = Scanner(fail_stop=not scanners)
            scanners.append(scanner)
            return scanner

        observer = BleakPresenceObserver("AA:BB", scanner_factory=factory)
        await observer.start(lambda _observation: None)
        with pytest.raises(RuntimeError, match="scanner stop failed"):
            await observer.stop()
        with pytest.raises(RuntimeError, match="already active"):
            await observer.start(lambda _observation: None)
        assert len(scanners) == 1

        await observer.stop()
        assert scanners[0].stop_calls == 2
        await observer.start(lambda _observation: None)
        assert len(scanners) == 2
        await observer.stop()

    _run(scenario())


def test_callback_during_scanner_start_is_buffered_and_redeemed_after_stop() -> None:
    async def scenario() -> None:
        candidate = object()

        class StartCallback(FakeObserver):
            async def start(self, callback: Callable[[object], object]) -> None:
                await super().start(callback)
                callback(PresenceAdvertisement(candidate, -67))
                callback(PresenceAdvertisement(candidate, -67))

        observer = StartCallback()
        scheduler = PresenceScheduler(observer, policy=_test_policy())
        try:
            wake = await asyncio.wait_for(scheduler.wait_for_attempt(), timeout=_READINESS_TIMEOUT_SECONDS)

            assert wake.reason == "advertisement"
            assert wake.candidate is candidate
            assert observer.events == ["start", "stop"]
        finally:
            await scheduler.close()

    _run(scenario())


def test_scheduler_releases_only_after_stable_visibility() -> None:
    async def scenario() -> None:
        now = [0.0]
        observer = FakeObserver()
        scheduler = PresenceScheduler(
            observer,
            policy=PresencePolicy(
                arrival_stability_seconds=5.0,
                arrival_max_gap_seconds=3.0,
                scan_recheck_seconds=60.0,
                drain_cooldown_seconds=60.0,
            ),
            clock=lambda: now[0],
        )
        waiter = asyncio.create_task(scheduler.wait_for_attempt())
        await _wait_started(observer, scheduler, waiter)
        candidate = object()
        for at in (0.0, 2.0, 4.0):
            now[0] = at
            observer.emit(candidate)
            await asyncio.sleep(0)
            await asyncio.sleep(0)
        assert not waiter.done()
        now[0] = 5.0
        observer.emit(candidate)

        wake = await waiter

        assert wake.reason == "advertisement"
        assert wake.candidate is candidate
        await scheduler.close()

    _run(scenario())


def test_old_scanner_generation_cannot_release_a_later_attempt() -> None:
    async def scenario() -> None:
        observer = FakeObserver()
        scheduler = PresenceScheduler(observer, policy=_test_policy(scan_recheck_seconds=60, drain_cooldown_seconds=60))
        first = asyncio.create_task(scheduler.wait_for_attempt())
        await _wait_started(observer, scheduler, first)
        await _emit_stable(observer)
        await first
        stale_callback = observer.callbacks[0]
        await scheduler.attempt_finished(CandidateUnavailable())
        stale_callback(PresenceAdvertisement(object(), -80))
        second = asyncio.create_task(scheduler.wait_for_attempt())
        await asyncio.sleep(0)
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(asyncio.shield(second), timeout=0.01)
        await _emit_stable(observer)
        assert (await second).reason == "advertisement"
        await scheduler.close()

    _run(scenario())


def test_stop_completes_before_permit_is_returned() -> None:
    async def scenario() -> None:
        stopped = asyncio.Event()

        class DelayedStop(FakeObserver):
            async def stop(self) -> None:
                await stopped.wait()
                await super().stop()

        observer = DelayedStop()
        scheduler = PresenceScheduler(observer, policy=_test_policy())
        waiter = asyncio.create_task(scheduler.wait_for_attempt())
        await _wait_started(observer, scheduler, waiter)
        await _emit_stable(observer)
        await asyncio.sleep(0)
        assert not waiter.done()
        stopped.set()
        assert (await waiter).reason == "advertisement"
        await scheduler.close()

    _run(scenario())


def test_close_during_redeem_does_not_return_a_stopped_permit() -> None:
    async def scenario() -> None:
        entered_stop = asyncio.Event()
        release_stop = asyncio.Event()

        class BlockingStop(FakeObserver):
            async def stop(self) -> None:
                entered_stop.set()
                await release_stop.wait()
                await super().stop()

        observer = BlockingStop()
        scheduler = PresenceScheduler(observer, policy=_test_policy())
        waiter = asyncio.create_task(scheduler.wait_for_attempt())
        closing: asyncio.Task[None] | None = None
        try:
            await _wait_started(observer, scheduler, waiter)
            await _emit_stable(observer)
            await asyncio.wait_for(entered_stop.wait(), timeout=_READINESS_TIMEOUT_SECONDS)
            closing = asyncio.create_task(scheduler.close())
            await asyncio.sleep(0)
            release_stop.set()

            with pytest.raises(RuntimeError, match="closed"):
                await waiter
            await closing
            assert not observer.active
        finally:
            release_stop.set()
            await _cancel_waiter_and_close(scheduler, waiter, closing)

    _run(scenario())


def test_stop_failure_closes_machine_without_releasing_permit() -> None:
    async def scenario() -> None:
        class BrokenStop(FakeObserver):
            async def stop(self) -> None:
                raise TimeoutError("scanner stop failed")

        observer = BrokenStop()
        scheduler = PresenceScheduler(observer, policy=_test_policy())
        waiter = asyncio.create_task(scheduler.wait_for_attempt())
        await _wait_started(observer, scheduler, waiter)
        await _emit_stable(observer)
        with pytest.raises(PresenceScanStopError):
            await waiter
        with pytest.raises(RuntimeError, match="closed"):
            await scheduler.wait_for_attempt()

    _run(scenario())


def test_only_one_waiter_is_accepted() -> None:
    async def scenario() -> None:
        observer = FakeObserver()
        scheduler = PresenceScheduler(observer, policy=_test_policy())
        first = asyncio.create_task(scheduler.wait_for_attempt())
        try:
            await _wait_started(observer, scheduler, first)
            with pytest.raises(RuntimeError, match="one presence waiter"):
                await asyncio.wait_for(scheduler.wait_for_attempt(), timeout=_READINESS_TIMEOUT_SECONDS)
            await _emit_stable(observer)
            await first
        finally:
            await _cancel_waiter_and_close(scheduler, first)

    _run(scenario())


def test_outstanding_permit_rejects_second_wait_without_scanner_effects() -> None:
    async def scenario() -> None:
        observer = FakeObserver()
        scheduler = PresenceScheduler(observer, policy=_test_policy())
        first = asyncio.create_task(scheduler.wait_for_attempt())
        await _wait_started(observer, scheduler, first)
        await _emit_stable(observer)
        await first
        effects = tuple(observer.events)

        with pytest.raises(RuntimeError, match="permit is already outstanding"):
            await scheduler.wait_for_attempt()

        assert tuple(observer.events) == effects
        assert not observer.active
        await scheduler.close()

    _run(scenario())


def test_presence_policy_accepts_effective_config_maxima_above_defaults() -> None:
    config = PresenceConfig(
        scan_recheck_seconds=301.0,
        max_scan_recheck_seconds=301.0,
        drain_cooldown_seconds=901.0,
        max_drain_cooldown_seconds=901.0,
    )

    policy = PresencePolicy(
        scan_recheck_seconds=config.scan_recheck_seconds,
        drain_cooldown_seconds=config.drain_cooldown_seconds,
    )

    assert policy.scan_recheck_seconds == config.scan_recheck_seconds
    assert policy.drain_cooldown_seconds == config.drain_cooldown_seconds


@pytest.mark.parametrize("absence_seconds", (0.0, -0.5))
def test_presence_policy_rejects_nonpositive_absence_bounds(absence_seconds: float) -> None:
    with pytest.raises(ValueError, match=r"^presence policy bounds must be positive$"):
        PresencePolicy(absence_seconds=absence_seconds)


def test_presence_policy_accepts_equal_scan_cancel_grace_bounds() -> None:
    policy = PresencePolicy(
        scan_cancel_grace_min_seconds=0.1,
        scan_cancel_grace_max_seconds=0.1,
    )

    assert policy.scan_cancel_grace_min_seconds == 0.1
    assert policy.scan_cancel_grace_max_seconds == 0.1


@pytest.mark.parametrize("fraction", (0.0, 1.0))
def test_presence_policy_accepts_scan_cancel_grace_fraction_boundaries(fraction: float) -> None:
    policy = PresencePolicy(scan_cancel_grace_fraction=fraction)

    assert policy.scan_cancel_grace_fraction == fraction


@pytest.mark.parametrize("fraction", (-0.5, 1.5))
def test_presence_policy_rejects_scan_cancel_grace_fraction_outside_bounds(fraction: float) -> None:
    with pytest.raises(
        ValueError,
        match=r"^scan cancellation fraction must be between zero and one$",
    ):
        PresencePolicy(scan_cancel_grace_fraction=fraction)


@pytest.mark.parametrize("rapid_backoff", ((0.0,), (-0.5,)))
def test_presence_policy_rejects_nonpositive_rapid_backoff(rapid_backoff: tuple[float, ...]) -> None:
    with pytest.raises(ValueError, match=r"^rapid retry delays must be positive$"):
        PresencePolicy(rapid_backoff=rapid_backoff)


def test_presence_policy_rejects_empty_rapid_backoff() -> None:
    with pytest.raises(ValueError, match=r"^rapid retry delays must be positive$"):
        PresencePolicy(rapid_backoff=())


def test_scheduler_keeps_derived_timing_when_policy_assignment_is_rejected() -> None:
    async def scenario() -> None:
        now = [0.0]
        delays: list[float] = []

        async def sleep(delay: float) -> None:
            delays.append(delay)
            now[0] += delay

        policy = _test_policy(absence_seconds=7.0, scan_recheck_seconds=30.0)
        scheduler = PresenceScheduler(FakeObserver(), policy=policy, clock=lambda: now[0], sleep=sleep)
        with pytest.raises(FrozenInstanceError):
            scheduler.policy.absence_seconds = 700.0  # type: ignore[reportAttributeAccessIssue]
        assert scheduler.policy is policy

        scheduler.resume_interrupted_visit()
        try:
            end = await scheduler.wait_for_attempt()
            assert isinstance(end, PresenceEnd)
            assert end.reason == "absence"
            assert delays == [7.0]
        finally:
            await scheduler.close()

    _run(scenario())


def test_effective_grace_values_bound_resistant_cancellation(monkeypatch: pytest.MonkeyPatch) -> None:
    async def scenario() -> None:
        started = asyncio.Event()
        cancelled = asyncio.Event()
        release = asyncio.Event()

        async def resist_cancellation() -> None:
            started.set()
            try:
                await asyncio.Future()
            except asyncio.CancelledError:
                cancelled.set()
                await release.wait()

        observed_grace: list[float | None] = []

        policy = PresencePolicy(
            scan_transition_seconds=2.0,
            scan_cancel_grace_min_seconds=0.2,
            scan_cancel_grace_max_seconds=0.3,
            scan_cancel_grace_fraction=0.125,
        )
        task = asyncio.create_task(resist_cancellation())
        await started.wait()

        async def incomplete_wait(
            tasks: set[asyncio.Task[object]], *, timeout: float | None = None
        ) -> tuple[set[asyncio.Task[object]], set[asyncio.Task[object]]]:
            del tasks
            observed_grace.append(timeout)
            return set(), set()

        monkeypatch.setattr(presence_module.asyncio, "wait", incomplete_wait)

        assert not await presence_module._cancel_bounded(task, policy)
        assert observed_grace == [0.25]
        await asyncio.sleep(0)
        assert cancelled.is_set()
        release.set()
        await task

    _run(scenario())


def test_failed_start_is_replaced_by_a_new_scanner_that_can_wake() -> None:
    async def scenario() -> None:
        starts = 0

        class FailOnce(FakeObserver):
            async def start(self, callback: Callable[[object], object]) -> None:
                nonlocal starts
                starts += 1
                if starts == 1:
                    raise RuntimeError("temporary adapter refusal")
                await super().start(callback)

        observer = FailOnce()
        scheduler = PresenceScheduler(
            observer,
            policy=_test_policy(scan_recheck_seconds=0.001, drain_cooldown_seconds=5),
        )
        waiter = asyncio.create_task(scheduler.wait_for_attempt())
        await _wait_started(observer, scheduler, waiter)
        await _emit_stable(observer)

        assert (await waiter).reason == "advertisement"
        assert starts == 2
        await scheduler.close()

    _run(scenario())


def test_drained_quiet_recovery_wakes_after_a_soft_scanner_start_failure() -> None:
    async def scenario() -> None:
        starts = 0

        class FailAfterPermit(FakeObserver):
            async def start(self, callback: Callable[[object], object]) -> None:
                nonlocal starts
                starts += 1
                if starts == 2:
                    raise RuntimeError("temporary adapter refusal")
                await super().start(callback)

        observer = FailAfterPermit()
        scheduler = PresenceScheduler(
            observer,
            policy=_test_policy(absence_seconds=0.001, scan_recheck_seconds=0.001, drain_cooldown_seconds=0.001),
        )
        first = asyncio.create_task(scheduler.wait_for_attempt())
        await _wait_started(observer, scheduler, first)
        await _emit_stable(observer)
        await asyncio.wait_for(first, timeout=1.0)
        await scheduler.attempt_finished(CleanDrain())
        await asyncio.sleep(0.005)
        second = asyncio.create_task(scheduler.wait_for_attempt())
        await asyncio.wait_for(_wait_started(observer, scheduler, second, starts=2), timeout=1.0)
        await asyncio.sleep(0.005)
        await _emit_stable(observer)

        assert (await asyncio.wait_for(second, timeout=1.0)).reason == "advertisement"
        assert starts == 3
        await scheduler.close()

    _run(scenario())


def test_active_scanner_releases_after_stable_visibility() -> None:
    async def scenario() -> None:
        now = [0.0]
        candidate = object()
        observer = FakeObserver()

        async def sleep(delay: float) -> None:
            now[0] += delay
            observer.emit(candidate)
            await asyncio.sleep(0)
            now[0] += 0.00001
            observer.emit(candidate)
            await asyncio.sleep(0)

        scheduler = PresenceScheduler(
            observer,
            policy=_test_policy(scan_recheck_seconds=5, drain_cooldown_seconds=5),
            clock=lambda: now[0],
            sleep=sleep,
        )

        try:
            wake = await asyncio.wait_for(scheduler.wait_for_attempt(), timeout=_READINESS_TIMEOUT_SECONDS)

            assert wake.reason == "advertisement"
            assert wake.candidate is candidate
            assert observer.events == ["start", "stop"]
            with pytest.raises(RuntimeError, match="permit is already outstanding"):
                await scheduler.wait_for_attempt()
        finally:
            await scheduler.close()

    _run(scenario())


def test_wait_cancellation_stops_scan_and_leaves_waiting_state_reusable() -> None:
    async def scenario() -> None:
        observer = FakeObserver()
        scheduler = PresenceScheduler(observer, policy=_test_policy(scan_recheck_seconds=60, drain_cooldown_seconds=60))
        waiter = asyncio.create_task(scheduler.wait_for_attempt())
        await _wait_started(observer, scheduler, waiter)
        waiter.cancel()
        with pytest.raises(asyncio.CancelledError):
            await waiter
        assert not observer.active
        retry = asyncio.create_task(scheduler.wait_for_attempt())
        await _wait_started(observer, scheduler, retry, starts=2)
        await _emit_stable(observer)
        assert (await retry).reason == "advertisement"
        await scheduler.close()

    _run(scenario())


def test_cancellation_while_redeeming_a_permit_fails_closed() -> None:
    async def scenario() -> None:
        entered_stop = asyncio.Event()

        class HangingStop(FakeObserver):
            async def stop(self) -> None:
                entered_stop.set()
                await asyncio.Future()

        observer = HangingStop()
        scheduler = PresenceScheduler(observer, policy=_test_policy(scan_transition_seconds=1.0))
        waiter = asyncio.create_task(scheduler.wait_for_attempt())
        await _wait_started(observer, scheduler, waiter)
        await _emit_stable(observer)
        await entered_stop.wait()
        waiter.cancel()
        with pytest.raises(asyncio.CancelledError):
            await waiter
        with pytest.raises(RuntimeError, match="closed"):
            await scheduler.wait_for_attempt()

    _run(scenario())


def test_uncertain_start_cancellation_closes_before_propagating_failure() -> None:
    async def scenario() -> None:
        started = asyncio.Event()
        release = asyncio.Event()

        class ResistantStart(FakeObserver):
            async def start(self, callback: Callable[[object], object]) -> None:
                del callback
                started.set()
                try:
                    await asyncio.Future()
                except asyncio.CancelledError:
                    await release.wait()

        scheduler = PresenceScheduler(ResistantStart(), policy=PresencePolicy(scan_transition_seconds=0.01))
        waiter = asyncio.create_task(scheduler.wait_for_attempt())
        try:
            await asyncio.wait_for(started.wait(), timeout=_READINESS_TIMEOUT_SECONDS)
            waiter.cancel()
            with pytest.raises(PresenceScanTransitionError, match="uncertain"):
                await waiter
            with pytest.raises(RuntimeError, match="closed"):
                await scheduler.wait_for_attempt()
        finally:
            release.set()
            await _cancel_waiter_and_close(scheduler, waiter)

    _run(scenario())


def test_close_is_idempotent_and_wakes_a_waiter() -> None:
    async def scenario() -> None:
        observer = FakeObserver()
        scheduler = PresenceScheduler(observer, policy=_test_policy())
        waiter = asyncio.create_task(scheduler.wait_for_attempt())
        await _wait_started(observer, scheduler, waiter)

        await scheduler.close()
        await scheduler.close()

        with pytest.raises(RuntimeError, match="closed"):
            await waiter
        assert observer.events == ["start", "stop"]

    _run(scenario())


def test_close_racing_with_start_stops_the_completed_scanner() -> None:
    async def scenario() -> None:
        entered_start = asyncio.Event()
        release_start = asyncio.Event()

        class BlockingStart(FakeObserver):
            async def start(self, callback: Callable[[object], object]) -> None:
                self.callback = callback
                self.callbacks.append(callback)
                self.events.append("start")
                entered_start.set()
                await release_start.wait()
                self.active = True

        observer = BlockingStart()
        scheduler = PresenceScheduler(observer)
        waiter = asyncio.create_task(scheduler.wait_for_attempt())
        closing: asyncio.Task[None] | None = None
        try:
            await asyncio.wait_for(entered_start.wait(), timeout=_READINESS_TIMEOUT_SECONDS)
            closing = asyncio.create_task(scheduler.close())
            await asyncio.sleep(0)
            assert not closing.done()
            release_start.set()
            await closing

            with pytest.raises(RuntimeError, match="closed"):
                await waiter
            assert not observer.active
            assert observer.events == ["start", "stop"]
        finally:
            release_start.set()
            await _cancel_waiter_and_close(scheduler, waiter, closing)

    _run(scenario())


def test_typed_outcome_restarts_observation_and_no_candidate_invalidation_api_remains() -> None:
    async def scenario() -> None:
        observer = FakeObserver()
        scheduler = PresenceScheduler(observer, policy=_test_policy(scan_recheck_seconds=60, drain_cooldown_seconds=60))
        first = asyncio.create_task(scheduler.wait_for_attempt())
        await _wait_started(observer, scheduler, first)
        await _emit_stable(observer)
        await first
        await scheduler.attempt_finished(ConnectedInterruption(durable_progress=False))

        assert observer.active
        assert not hasattr(scheduler, "invalidate_candidate")
        await scheduler.close()

    _run(scenario())


def test_absence_end_returns_without_a_gatt_permit() -> None:
    async def scenario() -> None:
        observer = FakeObserver()
        scheduler = PresenceScheduler(
            observer,
            policy=_test_policy(absence_seconds=0.01, rapid_backoff=(0.001, 0.002)),
        )
        first = asyncio.create_task(scheduler.wait_for_attempt())
        await _wait_started(observer, scheduler, first)
        await _emit_stable(observer)
        assert not isinstance(await first, PresenceEnd)
        assert await scheduler.attempt_finished(NotConnected(durable_progress=False)) is None

        ended = await scheduler.wait_for_attempt()

        assert ended == PresenceEnd("absence")
        assert not observer.active
        await scheduler.close()

    _run(scenario())


def test_recovery_exhaustion_returns_end_without_restarting_scanner() -> None:
    async def scenario() -> None:
        observer = FakeObserver()
        scheduler = PresenceScheduler(observer, policy=_test_policy(rapid_backoff=(0.01,)))
        first = asyncio.create_task(scheduler.wait_for_attempt())
        await _wait_started(observer, scheduler, first)
        await _emit_stable(observer)
        assert not isinstance(await first, PresenceEnd)

        assert await scheduler.attempt_finished(NotConnected(durable_progress=False)) is None
        retry = asyncio.create_task(scheduler.wait_for_attempt())
        await _wait_started(observer, scheduler, retry)
        await asyncio.sleep(0.02)
        await _emit_stable(observer)
        assert not isinstance(await asyncio.wait_for(retry, 1.0), PresenceEnd)
        ended = await scheduler.attempt_finished(NotConnected(durable_progress=False))

        assert ended == PresenceEnd("recovery_exhausted")
        assert not observer.active
        await scheduler.close()

    _run(scenario())


def test_startup_interrupted_visit_ends_on_absence_without_a_permit() -> None:
    async def scenario() -> None:
        observer = FakeObserver()
        scheduler = PresenceScheduler(
            observer,
            policy=_test_policy(absence_seconds=0.01, scan_recheck_seconds=60.0),
        )
        scheduler.resume_interrupted_visit()

        try:
            ended = await asyncio.wait_for(scheduler.wait_for_attempt(), timeout=_READINESS_TIMEOUT_SECONDS)

            assert ended == PresenceEnd("absence")
            assert observer.events == ["start", "stop"]
        finally:
            await scheduler.close()

    _run(scenario())


def test_startup_interrupted_visit_can_resume_from_a_fresh_advertisement() -> None:
    async def scenario() -> None:
        candidate = object()
        observer = FakeObserver()
        scheduler = PresenceScheduler(
            observer,
            policy=_test_policy(absence_seconds=0.05, scan_recheck_seconds=60.0),
        )
        scheduler.resume_interrupted_visit()
        waiter = asyncio.create_task(scheduler.wait_for_attempt())
        await _wait_started(observer, scheduler, waiter)
        await _emit_stable(observer, candidate)

        wake = await waiter

        assert not isinstance(wake, PresenceEnd)
        assert wake.candidate is candidate
        await scheduler.close()

    _run(scenario())


def test_startup_resume_refreshes_an_existing_waiters_timer() -> None:
    async def scenario() -> None:
        observer = FakeObserver()
        scheduler = PresenceScheduler(
            observer,
            policy=_test_policy(absence_seconds=0.01, scan_recheck_seconds=60.0),
        )
        waiter = asyncio.create_task(scheduler.wait_for_attempt())
        await _wait_started(observer, scheduler, waiter)

        scheduler.resume_interrupted_visit()
        ended = await asyncio.wait_for(waiter, timeout=1.0)

        assert ended == PresenceEnd("absence")
        assert observer.events == ["start", "stop"]
        await scheduler.close()

    _run(scenario())


def test_startup_rearm_keeps_absence_after_an_early_unavailable_permit() -> None:
    async def scenario() -> None:
        observer = FakeObserver()
        scheduler = PresenceScheduler(
            observer,
            policy=_test_policy(absence_seconds=0.01, scan_recheck_seconds=60.0),
        )
        first = asyncio.create_task(scheduler.wait_for_attempt())
        await _wait_started(observer, scheduler, first)
        await _emit_stable(observer)
        assert not isinstance(await first, PresenceEnd)

        scheduler.resume_interrupted_visit()
        assert not observer.active
        assert await scheduler.attempt_finished(CandidateUnavailable()) is None
        ended = await asyncio.wait_for(scheduler.wait_for_attempt(), timeout=1.0)

        assert ended == PresenceEnd("absence")
        await scheduler.close()

    _run(scenario())


def test_resume_with_expired_absence_returns_end_without_sleeping() -> None:
    async def scenario() -> None:
        now = [0.0]
        sleep_calls: list[float] = []
        policy = _test_policy(absence_seconds=5.0, scan_recheck_seconds=60.0)
        observer = FakeObserver()

        async def sleep(delay: float) -> None:
            sleep_calls.append(delay)

        scheduler = PresenceScheduler(observer, policy=policy, clock=lambda: now[0], sleep=sleep)
        scheduler.resume_interrupted_visit()
        now[0] = policy.absence_seconds

        try:
            async with asyncio.timeout(_READINESS_TIMEOUT_SECONDS):
                ended = await scheduler.wait_for_attempt()

            assert ended == PresenceEnd("absence")
            assert observer.events == ["start", "stop"]
            assert sleep_calls == []
        finally:
            await scheduler.close()

    _run(scenario())


def test_cancelling_partial_scan_start_stops_before_scheduler_reuse() -> None:
    async def scenario() -> None:
        now = [0.0]
        first_start = asyncio.Event()
        retry_start = asyncio.Event()
        candidate = object()

        class PartialStart(FakeObserver):
            blocked_once = False

            async def start(self, callback: Callable[[object], object]) -> None:
                if not self.blocked_once:
                    self.blocked_once = True
                    self.callback = callback
                    self.callbacks.append(callback)
                    self.events.append("start")
                    self.active = True
                    first_start.set()
                    await asyncio.Future()
                    return
                await super().start(callback)
                retry_start.set()
                _emit_pair(callback, candidate, now, 0.0, 0.02)

        observer = PartialStart()
        scheduler = PresenceScheduler(
            observer,
            policy=_test_policy(
                scan_transition_seconds=0.1,
                scan_cancel_grace_min_seconds=0.01,
                scan_cancel_grace_max_seconds=0.01,
                scan_cancel_grace_fraction=1.0,
            ),
            clock=lambda: now[0],
        )
        waiter = asyncio.create_task(scheduler.wait_for_attempt())
        retry: asyncio.Task[PresenceWake | PresenceEnd] | None = None
        try:
            async with asyncio.timeout(_READINESS_TIMEOUT_SECONDS):
                await first_start.wait()
            waiter.cancel()
            with pytest.raises(asyncio.CancelledError):
                async with asyncio.timeout(_READINESS_TIMEOUT_SECONDS):
                    await waiter

            assert observer.events == ["start", "stop"]
            assert not observer.active

            retry = asyncio.create_task(scheduler.wait_for_attempt())
            async with asyncio.timeout(_READINESS_TIMEOUT_SECONDS):
                await retry_start.wait()
                wake = await retry
            assert isinstance(wake, PresenceWake)
            assert wake.candidate is candidate
        finally:
            if not waiter.done():
                waiter.cancel()
            if retry is not None and not retry.done():
                retry.cancel()
            await asyncio.gather(waiter, *(() if retry is None else (retry,)), return_exceptions=True)
            await scheduler.close()

    _run(scenario())


def test_uncertain_partial_scan_start_stops_before_propagating_transition_error() -> None:
    async def scenario() -> None:
        entered_start = asyncio.Event()
        cancelled_start = asyncio.Event()
        release_start = asyncio.Event()
        start_finished = asyncio.Event()

        class ResistantStart(FakeObserver):
            async def start(self, callback: Callable[[object], object]) -> None:
                await super().start(callback)
                entered_start.set()
                try:
                    await asyncio.Future()
                except asyncio.CancelledError:
                    cancelled_start.set()
                    await release_start.wait()
                finally:
                    start_finished.set()

        observer = ResistantStart()
        scheduler = PresenceScheduler(
            observer,
            policy=_test_policy(
                scan_transition_seconds=0.01,
                scan_cancel_grace_min_seconds=0.002,
                scan_cancel_grace_max_seconds=0.002,
                scan_cancel_grace_fraction=1.0,
            ),
        )
        waiter = asyncio.create_task(scheduler.wait_for_attempt())
        try:
            async with asyncio.timeout(_READINESS_TIMEOUT_SECONDS):
                await entered_start.wait()
            waiter.cancel()
            with pytest.raises(PresenceScanTransitionError, match="start cancellation is uncertain"):
                async with asyncio.timeout(_READINESS_TIMEOUT_SECONDS):
                    await waiter

            assert cancelled_start.is_set()
            assert not observer.active
            assert observer.events == ["start", "stop"]
            with pytest.raises(RuntimeError, match="closed"):
                await scheduler.wait_for_attempt()
        finally:
            release_start.set()
            async with asyncio.timeout(_READINESS_TIMEOUT_SECONDS):
                await start_finished.wait()
            if not waiter.done():
                waiter.cancel()
            await asyncio.gather(waiter, return_exceptions=True)
            await scheduler.close()

    _run(scenario())


def test_normal_partial_start_failure_stops_before_fresh_scan_can_wake() -> None:
    async def scenario() -> None:
        now = [0.0]
        second_start = asyncio.Event()
        candidate = object()
        sleep_calls: list[float] = []

        class FailFirstStart(FakeObserver):
            def __init__(self) -> None:
                super().__init__()
                self.failed = False
                self.active_before_start: list[bool] = []

            async def start(self, callback: Callable[[object], object]) -> None:
                self.active_before_start.append(self.active)
                self.callback = callback
                self.callbacks.append(callback)
                self.events.append("start")
                self.active = True
                if not self.failed:
                    self.failed = True
                    raise RuntimeError("partial scanner start failed")
                second_start.set()
                _emit_pair(callback, candidate, now, now[0], now[0] + 0.02)

        async def sleep(delay: float) -> None:
            sleep_calls.append(delay)
            now[0] += delay

        observer = FailFirstStart()
        scheduler = PresenceScheduler(
            observer,
            policy=_test_policy(scan_recheck_seconds=0.01, drain_cooldown_seconds=60.0),
            clock=lambda: now[0],
            sleep=sleep,
        )
        waiter = asyncio.create_task(scheduler.wait_for_attempt())
        try:
            async with asyncio.timeout(_READINESS_TIMEOUT_SECONDS):
                await second_start.wait()
                wake = await waiter

            assert isinstance(wake, PresenceWake)
            assert wake.candidate is candidate
            assert observer.active_before_start == [False, False]
            assert observer.events == ["start", "stop", "start", "stop"]
            assert len(sleep_calls) == 1
        finally:
            if not waiter.done():
                waiter.cancel()
            await asyncio.gather(waiter, return_exceptions=True)
            await scheduler.close()

    _run(scenario())


@pytest.mark.parametrize("failure", ["cancel", "timeout", "oserror"])
def test_close_retries_observer_stop_after_first_attempt_fails(failure: str) -> None:
    async def scenario() -> None:
        entered_stop = asyncio.Event()
        scan_started = asyncio.Event()

        class FailFirstStop(FakeObserver):
            stop_calls = 0

            async def start(self, callback: Callable[[object], object]) -> None:
                await super().start(callback)
                scan_started.set()

            async def stop(self) -> None:
                self.stop_calls += 1
                self.events.append("stop")
                entered_stop.set()
                if self.stop_calls == 1:
                    if failure in {"cancel", "timeout"}:
                        await asyncio.Future()
                    raise OSError("first scanner stop failed")
                self.active = False

        observer = FailFirstStop()
        scheduler = PresenceScheduler(
            observer,
            policy=_test_policy(
                scan_transition_seconds=0.01,
                scan_cancel_grace_min_seconds=0.002,
                scan_cancel_grace_max_seconds=0.002,
                scan_cancel_grace_fraction=1.0,
            ),
        )
        waiter = asyncio.create_task(scheduler.wait_for_attempt())
        closing: asyncio.Task[None] | None = None
        try:
            async with asyncio.timeout(_READINESS_TIMEOUT_SECONDS):
                await scan_started.wait()

            closing = asyncio.create_task(scheduler.close())
            async with asyncio.timeout(_READINESS_TIMEOUT_SECONDS):
                await entered_stop.wait()
            if failure == "cancel":
                closing.cancel()
                await asyncio.gather(closing, return_exceptions=True)
            else:
                async with asyncio.timeout(_READINESS_TIMEOUT_SECONDS):
                    await closing

            assert observer.active
            assert observer.stop_calls == 1
            with pytest.raises(RuntimeError, match="closed"):
                async with asyncio.timeout(_READINESS_TIMEOUT_SECONDS):
                    await waiter

            await scheduler.close()
            assert observer.stop_calls == 2
            assert not observer.active
            assert observer.events == ["start", "stop", "stop"]
        finally:
            if closing is not None and not closing.done():
                closing.cancel()
            if not waiter.done():
                waiter.cancel()
            await asyncio.gather(waiter, *(() if closing is None else (closing,)), return_exceptions=True)
            await scheduler.close()

    _run(scenario())


def test_stop_failure_retries_cleanup_and_fails_closed_before_returning() -> None:
    async def scenario() -> None:
        now = [0.0]
        started = asyncio.Event()
        candidate = object()

        class FailOnceStop(FakeObserver):
            stop_calls = 0

            async def start(self, callback: Callable[[object], object]) -> None:
                await super().start(callback)
                started.set()
                _emit_pair(callback, candidate, now, 0.0, 0.02)

            async def stop(self) -> None:
                self.stop_calls += 1
                self.events.append("stop")
                if self.stop_calls == 1:
                    raise OSError("first scanner stop failed")
                self.active = False

        observer = FailOnceStop()
        scheduler = PresenceScheduler(
            observer,
            policy=_test_policy(arrival_stability_seconds=0.01, arrival_max_gap_seconds=0.5),
            clock=lambda: now[0],
        )
        waiter = asyncio.create_task(scheduler.wait_for_attempt())
        try:
            async with asyncio.timeout(_READINESS_TIMEOUT_SECONDS):
                await started.wait()
                with pytest.raises(PresenceScanStopError):
                    await waiter

            assert observer.stop_calls == 2
            assert observer.events == ["start", "stop", "stop"]
            assert not observer.active
            with pytest.raises(RuntimeError, match="closed"):
                await scheduler.wait_for_attempt()
        finally:
            if not waiter.done():
                waiter.cancel()
            await asyncio.gather(waiter, return_exceptions=True)
            await scheduler.close()

    _run(scenario())


def test_timer_winner_drains_current_stable_advertisement_after_getter_cancellation() -> None:
    async def scenario() -> None:
        now = [0.0]
        timer_started = [asyncio.Event(), asyncio.Event()]
        sleep_calls = 0
        candidate = object()

        async def controlled_sleep(delay: float) -> None:
            nonlocal sleep_calls
            sleep_calls += 1
            timer_started[min(sleep_calls - 1, 1)].set()
            if sleep_calls == 1:
                await asyncio.Future()
                return
            now[0] = delay
            callback = observer.callback
            assert callback is not None

            def deliver() -> None:
                now[0] = delay + 0.02
                callback(PresenceAdvertisement(candidate, -72))

            _after_loop_turns(3, deliver)

        observer = FakeObserver()
        scheduler = PresenceScheduler(
            observer,
            policy=_test_policy(scan_recheck_seconds=0.01),
            clock=lambda: now[0],
            sleep=controlled_sleep,
        )
        waiter = asyncio.create_task(scheduler.wait_for_attempt())
        try:
            async with asyncio.timeout(_READINESS_TIMEOUT_SECONDS):
                await timer_started[0].wait()
            observer.emit(candidate)
            async with asyncio.timeout(_READINESS_TIMEOUT_SECONDS):
                await timer_started[1].wait()
                wake = await waiter

            assert isinstance(wake, PresenceWake)
            assert wake.candidate is candidate
            assert observer.events == ["start", "stop"]
        finally:
            if not waiter.done():
                waiter.cancel()
            await asyncio.gather(waiter, return_exceptions=True)
            await scheduler.close()

    _run(scenario())


def test_expired_absence_wins_over_advertisement_during_wait_cleanup() -> None:
    async def scenario() -> None:
        now = [0.0]
        candidate = object()

        def schedule_pair(delay: float) -> None:
            callback = observer.callback
            assert callback is not None

            def deliver() -> None:
                now[0] = delay + 0.01
                callback(PresenceAdvertisement(candidate, -72))
                now[0] = delay + 0.03
                callback(PresenceAdvertisement(candidate, -72))

            _after_loop_turns(5, deliver)

        async def expire_absence(delay: float) -> None:
            now[0] = delay
            schedule_pair(delay)

        observer = FakeObserver()
        scheduler = PresenceScheduler(
            observer,
            policy=_test_policy(absence_seconds=0.01, scan_recheck_seconds=60.0),
            clock=lambda: now[0],
            sleep=expire_absence,
        )
        scheduler.resume_interrupted_visit()
        waiter = asyncio.create_task(scheduler.wait_for_attempt())
        try:
            async with asyncio.timeout(_READINESS_TIMEOUT_SECONDS):
                result = await waiter

            assert result == PresenceEnd("absence")
            assert observer.events == ["start", "stop"]
        finally:
            if not waiter.done():
                waiter.cancel()
            await asyncio.gather(waiter, return_exceptions=True)
            await scheduler.close()

    _run(scenario())


def test_expired_resumed_absence_preserves_partial_scanner_cleanup_for_close() -> None:
    async def scenario() -> None:
        now = [0.0]
        stop_attempts = 0

        class PartialObserver(FakeObserver):
            async def start(self, callback: Callable[[object], object]) -> None:
                self.callback = callback
                self.callbacks.append(callback)
                self.events.append("start")
                self.active = True
                raise RuntimeError("partial start")

            async def stop(self) -> None:
                nonlocal stop_attempts
                stop_attempts += 1
                self.events.append("stop")
                if stop_attempts == 1:
                    raise OSError("partial stop")
                self.active = False

        async def expire(delay: float) -> None:
            now[0] = delay

        observer = PartialObserver()
        scheduler = PresenceScheduler(
            observer,
            policy=_test_policy(absence_seconds=0.01, scan_recheck_seconds=60.0),
            clock=lambda: now[0],
            sleep=expire,
        )
        scheduler.resume_interrupted_visit()
        try:
            async with asyncio.timeout(_READINESS_TIMEOUT_SECONDS):
                result = await scheduler.wait_for_attempt()

            assert result == PresenceEnd("absence")
            assert observer.active
            await scheduler.close()
            assert not observer.active
            assert observer.events == ["start", "stop", "stop"]
        finally:
            await scheduler.close()

    _run(scenario())


def test_two_old_generation_callbacks_cannot_release_a_waiting_attempt() -> None:
    async def scenario() -> None:
        now = [0.0]
        second_scan_started = asyncio.Event()
        first_scan_started = asyncio.Event()
        waiting_for_advertisement = asyncio.Event()
        first_candidate = object()
        stale_candidate = object()
        fresh_candidate = object()

        class TwoGenerationObserver(FakeObserver):
            async def start(self, callback: Callable[[object], object]) -> None:
                await super().start(callback)
                if len(self.callbacks) == 1:
                    first_scan_started.set()
                elif len(self.callbacks) == 2:
                    second_scan_started.set()

        async def wait_for_ads(_delay: float) -> None:
            waiting_for_advertisement.set()
            await asyncio.Future()

        observer = TwoGenerationObserver()
        scheduler = PresenceScheduler(
            observer,
            policy=_test_policy(arrival_stability_seconds=0.01, arrival_max_gap_seconds=0.5),
            clock=lambda: now[0],
            sleep=wait_for_ads,
        )
        first = asyncio.create_task(scheduler.wait_for_attempt())
        second: asyncio.Task[PresenceWake | PresenceEnd] | None = None
        try:
            async with asyncio.timeout(_READINESS_TIMEOUT_SECONDS):
                await first_scan_started.wait()
            stale_callback = observer.callbacks[0]
            _emit_pair(stale_callback, first_candidate, now, 0.0, 0.02)
            async with asyncio.timeout(_READINESS_TIMEOUT_SECONDS):
                first_wake = await first
            assert isinstance(first_wake, PresenceWake)
            assert first_wake.candidate is first_candidate
            assert await scheduler.attempt_finished(CandidateUnavailable()) is None

            async with asyncio.timeout(_READINESS_TIMEOUT_SECONDS):
                await second_scan_started.wait()
            waiting_for_advertisement.clear()
            second = asyncio.create_task(scheduler.wait_for_attempt())
            async with asyncio.timeout(_READINESS_TIMEOUT_SECONDS):
                await waiting_for_advertisement.wait()

            current_callback = observer.callbacks[1]
            _emit_at(current_callback, fresh_candidate, now, 0.1)
            waiting_for_advertisement.clear()
            async with asyncio.timeout(_READINESS_TIMEOUT_SECONDS):
                await waiting_for_advertisement.wait()

            _emit_pair(stale_callback, stale_candidate, now, 0.2, 0.22)
            _emit_at(current_callback, fresh_candidate, now, 0.3)
            async with asyncio.timeout(_READINESS_TIMEOUT_SECONDS):
                second_wake = await second

            assert isinstance(second_wake, PresenceWake)
            assert second_wake.candidate is fresh_candidate
        finally:
            for task in (first, second):
                if task is not None and not task.done():
                    task.cancel()
            await asyncio.gather(first, *(() if second is None else (second,)), return_exceptions=True)
            await scheduler.close()

    _run(scenario())
