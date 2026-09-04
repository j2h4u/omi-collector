from __future__ import annotations

import asyncio
from collections.abc import Callable

import pytest

from omi_collector.capture.application import presence as presence_module
from omi_collector.capture.application.presence import (
    PresenceAdvertisement,
    PresencePolicy,
    PresenceScanStopError,
    PresenceScanTransitionError,
    PresenceScheduler,
)
from omi_collector.capture.application.presence_machine import CandidateUnavailable, CleanDrain, ConnectedInterruption
from omi_collector.config import PresenceConfig


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


async def _wait_started(observer: FakeObserver, starts: int = 1) -> None:
    while len(observer.callbacks) < starts:
        await asyncio.sleep(0)


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

        observer = StartCallback()
        scheduler = PresenceScheduler(observer)
        wake = await scheduler.wait_for_attempt()

        assert wake.reason == "advertisement"
        assert wake.candidate is candidate
        assert observer.events == ["start", "stop"]
        await scheduler.close()

    _run(scenario())


def test_old_scanner_generation_cannot_release_a_later_attempt() -> None:
    async def scenario() -> None:
        observer = FakeObserver()
        scheduler = PresenceScheduler(observer, policy=PresencePolicy(fallback_seconds=60, drained_fallback_seconds=60))
        first = asyncio.create_task(scheduler.wait_for_attempt())
        await _wait_started(observer)
        observer.emit()
        await first
        stale_callback = observer.callbacks[0]
        await scheduler.attempt_finished(CandidateUnavailable())
        stale_callback(PresenceAdvertisement(object(), -80))
        second = asyncio.create_task(scheduler.wait_for_attempt())
        await asyncio.sleep(0)
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(asyncio.shield(second), timeout=0.01)
        observer.emit()
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
        scheduler = PresenceScheduler(observer)
        waiter = asyncio.create_task(scheduler.wait_for_attempt())
        await _wait_started(observer)
        observer.emit()
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
        scheduler = PresenceScheduler(observer)
        waiter = asyncio.create_task(scheduler.wait_for_attempt())
        await _wait_started(observer)
        observer.emit()
        await entered_stop.wait()
        closing = asyncio.create_task(scheduler.close())
        await asyncio.sleep(0)
        release_stop.set()

        with pytest.raises(RuntimeError, match="closed"):
            await waiter
        await closing
        assert not observer.active

    _run(scenario())


def test_stop_failure_closes_machine_without_releasing_permit() -> None:
    async def scenario() -> None:
        class BrokenStop(FakeObserver):
            async def stop(self) -> None:
                raise TimeoutError("scanner stop failed")

        observer = BrokenStop()
        scheduler = PresenceScheduler(observer)
        waiter = asyncio.create_task(scheduler.wait_for_attempt())
        await _wait_started(observer)
        observer.emit()
        with pytest.raises(PresenceScanStopError):
            await waiter
        with pytest.raises(RuntimeError, match="closed"):
            await scheduler.wait_for_attempt()

    _run(scenario())


def test_only_one_waiter_is_accepted() -> None:
    async def scenario() -> None:
        observer = FakeObserver()
        scheduler = PresenceScheduler(observer)
        first = asyncio.create_task(scheduler.wait_for_attempt())
        await _wait_started(observer)
        with pytest.raises(RuntimeError, match="one presence waiter"):
            await scheduler.wait_for_attempt()
        observer.emit()
        await first
        await scheduler.close()

    _run(scenario())


def test_outstanding_permit_rejects_second_wait_without_scanner_effects() -> None:
    async def scenario() -> None:
        observer = FakeObserver()
        scheduler = PresenceScheduler(observer)
        first = asyncio.create_task(scheduler.wait_for_attempt())
        await _wait_started(observer)
        observer.emit()
        await first
        effects = tuple(observer.events)

        with pytest.raises(RuntimeError, match="permit is already outstanding"):
            await scheduler.wait_for_attempt()

        assert tuple(observer.events) == effects
        assert not observer.active
        await scheduler.close()

    _run(scenario())


def test_simultaneous_advertisement_and_fallback_prefers_advertisement() -> None:
    async def scenario() -> None:
        now = [0.0]
        candidate = object()

        class ReadyAtDeadline(FakeObserver):
            async def start(self, callback: Callable[[object], object]) -> None:
                await super().start(callback)
                now[0] = 5.0
                callback(PresenceAdvertisement(candidate, -75))

        scheduler = PresenceScheduler(
            ReadyAtDeadline(),
            policy=PresencePolicy(fallback_seconds=5, drained_fallback_seconds=5),
            clock=lambda: now[0],
        )
        wake = await scheduler.wait_for_attempt()

        assert wake.reason == "advertisement"
        assert wake.candidate is candidate
        await scheduler.close()

    _run(scenario())


def test_soft_start_failure_waits_for_the_existing_timer_without_hot_loop() -> None:
    async def scenario() -> None:
        now = [0.0]
        calls = 0

        async def sleep(delay: float) -> None:
            now[0] += delay

        class RefusingStart(FakeObserver):
            async def start(self, callback: Callable[[object], object]) -> None:
                nonlocal calls
                del callback
                calls += 1
                raise RuntimeError("adapter temporarily unavailable")

        scheduler = PresenceScheduler(
            RefusingStart(),
            policy=PresencePolicy(fallback_seconds=5, drained_fallback_seconds=5),
            clock=lambda: now[0],
            sleep=sleep,
        )
        wake = await scheduler.wait_for_attempt()

        assert wake.reason == "fallback"
        assert calls == 1
        await scheduler.close()

    _run(scenario())


def test_presence_policy_accepts_effective_config_maxima_above_defaults() -> None:
    config = PresenceConfig(
        fallback_seconds=301.0,
        max_fallback_seconds=301.0,
        drained_fallback_seconds=901.0,
        max_drained_fallback_seconds=901.0,
    )

    policy = PresencePolicy(
        fallback_seconds=config.fallback_seconds,
        drained_fallback_seconds=config.drained_fallback_seconds,
    )

    assert policy.fallback_seconds == config.fallback_seconds
    assert policy.drained_fallback_seconds == config.drained_fallback_seconds


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
        now = [0.0]
        starts = 0

        async def sleep(delay: float) -> None:
            now[0] += delay

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
            policy=PresencePolicy(fallback_seconds=5, drained_fallback_seconds=5),
            clock=lambda: now[0],
            sleep=sleep,
        )
        assert (await scheduler.wait_for_attempt()).reason == "fallback"
        await scheduler.attempt_finished(CandidateUnavailable())
        observer.emit()

        assert (await scheduler.wait_for_attempt()).reason == "advertisement"
        assert starts == 2
        await scheduler.close()

    _run(scenario())


def test_drained_quiet_recovery_wakes_after_a_soft_scanner_start_failure() -> None:
    async def scenario() -> None:
        now = [0.0]
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
            policy=PresencePolicy(absence_seconds=1, fallback_seconds=5, drained_fallback_seconds=5),
            clock=lambda: now[0],
        )
        first = asyncio.create_task(scheduler.wait_for_attempt())
        await _wait_started(observer)
        observer.emit()
        await first
        await scheduler.attempt_finished(CleanDrain())
        now[0] = 5.0
        second = asyncio.create_task(scheduler.wait_for_attempt())
        await _wait_started(observer, starts=2)
        observer.emit()

        assert (await second).reason == "advertisement"
        assert starts == 3
        await scheduler.close()

    _run(scenario())


def test_bounded_hanging_start_cleans_up_then_releases_fallback() -> None:
    async def scenario() -> None:
        now = [0.0]

        async def sleep(delay: float) -> None:
            now[0] += delay

        class HangingStart(FakeObserver):
            async def start(self, callback: Callable[[object], object]) -> None:
                del callback
                self.events.append("start")
                await asyncio.Future()

        observer = HangingStart()
        scheduler = PresenceScheduler(
            observer,
            policy=PresencePolicy(fallback_seconds=5, drained_fallback_seconds=5, scan_transition_seconds=0.01),
            clock=lambda: now[0],
            sleep=sleep,
        )

        assert (await scheduler.wait_for_attempt()).reason == "fallback"
        assert observer.events == ["start", "stop"]
        assert not observer.active
        await scheduler.close()

    _run(scenario())


def test_idle_scanner_fallback_stops_before_releasing_its_permit() -> None:
    async def scenario() -> None:
        now = [0.0]

        async def sleep(delay: float) -> None:
            now[0] += delay

        observer = FakeObserver()
        scheduler = PresenceScheduler(
            observer,
            policy=PresencePolicy(fallback_seconds=5, drained_fallback_seconds=5),
            clock=lambda: now[0],
            sleep=sleep,
        )

        wake = await scheduler.wait_for_attempt()

        assert wake.reason == "fallback"
        assert observer.events == ["start", "stop"]
        assert not observer.active
        await scheduler.close()

    _run(scenario())


def test_active_scanner_advertisement_wins_when_timer_completes_simultaneously() -> None:
    async def scenario() -> None:
        now = [0.0]
        candidate = object()
        observer = FakeObserver()

        async def sleep(delay: float) -> None:
            now[0] += delay
            observer.emit(candidate)
            await asyncio.sleep(0)

        scheduler = PresenceScheduler(
            observer,
            policy=PresencePolicy(fallback_seconds=5, drained_fallback_seconds=5),
            clock=lambda: now[0],
            sleep=sleep,
        )

        wake = await scheduler.wait_for_attempt()

        assert wake.reason == "advertisement"
        assert wake.candidate is candidate
        assert observer.events == ["start", "stop"]
        with pytest.raises(RuntimeError, match="permit is already outstanding"):
            await scheduler.wait_for_attempt()
        await scheduler.close()

    _run(scenario())


def test_wait_cancellation_stops_scan_and_leaves_waiting_state_reusable() -> None:
    async def scenario() -> None:
        observer = FakeObserver()
        scheduler = PresenceScheduler(observer, policy=PresencePolicy(fallback_seconds=60, drained_fallback_seconds=60))
        waiter = asyncio.create_task(scheduler.wait_for_attempt())
        await _wait_started(observer)
        waiter.cancel()
        with pytest.raises(asyncio.CancelledError):
            await waiter
        assert not observer.active
        retry = asyncio.create_task(scheduler.wait_for_attempt())
        await _wait_started(observer, starts=2)
        observer.emit()
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
        scheduler = PresenceScheduler(observer, policy=PresencePolicy(scan_transition_seconds=1.0))
        waiter = asyncio.create_task(scheduler.wait_for_attempt())
        await _wait_started(observer)
        observer.emit()
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
        await started.wait()
        waiter.cancel()
        with pytest.raises(PresenceScanTransitionError, match="uncertain"):
            await waiter
        with pytest.raises(RuntimeError, match="closed"):
            await scheduler.wait_for_attempt()
        release.set()

    _run(scenario())


def test_close_is_idempotent_and_wakes_a_waiter() -> None:
    async def scenario() -> None:
        observer = FakeObserver()
        scheduler = PresenceScheduler(observer)
        waiter = asyncio.create_task(scheduler.wait_for_attempt())
        await _wait_started(observer)

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
        await entered_start.wait()
        closing = asyncio.create_task(scheduler.close())
        await asyncio.sleep(0)
        assert not closing.done()
        release_start.set()
        await closing

        with pytest.raises(RuntimeError, match="closed"):
            await waiter
        assert not observer.active
        assert observer.events == ["start", "stop"]

    _run(scenario())


def test_typed_outcome_restarts_observation_and_no_candidate_invalidation_api_remains() -> None:
    async def scenario() -> None:
        observer = FakeObserver()
        scheduler = PresenceScheduler(observer, policy=PresencePolicy(fallback_seconds=60, drained_fallback_seconds=60))
        first = asyncio.create_task(scheduler.wait_for_attempt())
        await _wait_started(observer)
        observer.emit()
        await first
        await scheduler.attempt_finished(ConnectedInterruption(durable_progress=False))

        assert observer.active
        assert not hasattr(scheduler, "invalidate_candidate")
        await scheduler.close()

    _run(scenario())
