import asyncio
from collections.abc import Callable

import pytest
from bleak.backends.device import BLEDevice

from omi_collector.capture.adapters.bleak_transport import BleakPresenceObserver
from omi_collector.capture.application.presence import (
    PresenceAdvertisement,
    PresencePolicy,
    PresenceScanStopError,
    PresenceScanTransitionError,
    PresenceScheduler,
)
from omi_collector.config import DEFAULT_CONFIG


class FakeObserver:
    def __init__(self) -> None:
        self.callback: Callable[[object], object] | None = None
        self.starts = 0
        self.stops = 0
        self.active = False

    async def start(self, callback: Callable[[object], object]) -> None:
        self.callback = callback
        self.starts += 1
        self.active = True

    async def stop(self) -> None:
        self.stops += 1
        self.active = False

    def emit(self, candidate: object | None = None) -> None:
        assert self.callback is not None
        self.callback(candidate or BLEDevice("AA:BB", "omi", object()))


def _run(coro: object) -> object:
    return asyncio.run(coro)  # type: ignore[arg-type]


def test_presence_policy_rejects_fallback_above_contract() -> None:
    defaults = PresencePolicy()
    presence = DEFAULT_CONFIG.presence
    assert defaults.absence_seconds == presence.absence_seconds
    assert defaults.fallback_seconds == presence.fallback_seconds
    assert defaults.drained_fallback_seconds == presence.drained_fallback_seconds
    assert defaults.scan_transition_seconds == presence.scan_transition_seconds
    assert defaults.rapid_backoff == DEFAULT_CONFIG.retry.rapid_backoff
    with pytest.raises(ValueError):
        PresencePolicy(fallback_seconds=presence.max_fallback_seconds + 0.1)
    with pytest.raises(ValueError):
        PresencePolicy(drained_fallback_seconds=presence.max_drained_fallback_seconds + 0.1)
    with pytest.raises(ValueError):
        PresencePolicy(absence_seconds=0)


def test_bleak_observer_requests_duplicate_data_and_filters_exact_address() -> None:
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
            def __init__(self, rssi: int) -> None:
                self.rssi = rssi

        callback(Device("AA:CC"), Advertisement(-42))  # type: ignore[call-arg]
        matching = Device("aa:bb")
        callback(matching, Advertisement(-73))  # type: ignore[call-arg]
        await observer.stop()

        assert seen == [PresenceAdvertisement(matching, -73)]
        assert kwargs["bluez"] == {"adapter": "hci0", "filters": {"DuplicateData": True}}
        assert events == ["start", "stop"]

    _run(scenario())


def test_bleak_start_failure_falls_back_and_retries_with_fresh_scanner() -> None:
    async def scenario() -> None:
        events: list[tuple[str, int]] = []
        scanners: list[object] = []

        class Scanner:
            def __init__(self, number: int) -> None:
                self.number = number

            async def start(self) -> None:
                events.append(("start", self.number))
                if self.number == 0:
                    raise RuntimeError("Bluetooth unavailable")

            async def stop(self) -> None:
                events.append(("stop", self.number))
                if self.number == 0:
                    raise RuntimeError("scanner cleanup unavailable")

        def factory(**_options: object) -> Scanner:
            scanner = Scanner(len(scanners))
            scanners.append(scanner)
            return scanner

        observer = BleakPresenceObserver("AA:BB", scanner_factory=factory)
        scheduler = PresenceScheduler(
            observer,
            policy=PresencePolicy(fallback_seconds=0.01, drained_fallback_seconds=0.01, scan_transition_seconds=0.01),
        )

        first_wake = await scheduler.wait_for_attempt()
        assert first_wake.reason == "fallback"
        await scheduler.attempt_finished("clean")
        second_wake = await asyncio.wait_for(scheduler.wait_for_attempt(), timeout=0.2)
        assert second_wake.reason == "fallback"
        await scheduler.close()

        assert len(scanners) == 2
        assert events == [("start", 0), ("stop", 0), ("start", 1), ("stop", 1)]

    _run(scenario())


def test_ad_wake_retains_exact_scanner_candidate_and_stops_own_scan() -> None:
    async def scenario() -> None:
        observer = FakeObserver()
        scheduler = PresenceScheduler(observer)
        wait = asyncio.create_task(scheduler.wait_for_attempt())
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        candidate = BLEDevice("AA:BB", "omi", object())
        observer.emit(candidate)
        wake = await wait
        assert wake.reason == "advertisement"
        assert wake.candidate is candidate
        assert not observer.active
        await scheduler.close()

    _run(scenario())


def test_new_advertisement_replaces_consumed_candidate_and_timestamp() -> None:
    async def scenario() -> None:
        now = [0.0]
        observer = FakeObserver()
        scheduler = PresenceScheduler(
            observer, policy=PresencePolicy(drained_fallback_seconds=30), clock=lambda: now[0]
        )

        first = asyncio.create_task(scheduler.wait_for_attempt())
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        candidate_a = BLEDevice("AA:BB", "omi", object())
        observer.emit(candidate_a)
        wake_a = await first
        assert wake_a.candidate is candidate_a
        await scheduler.attempt_finished("clean")

        second = asyncio.create_task(scheduler.wait_for_attempt())
        await asyncio.sleep(0)
        now[0] = 60.0
        candidate_b = BLEDevice("AA:BB", "omi", object())
        observer.emit(candidate_b)
        wake_b = await second
        assert wake_b.candidate is candidate_b
        await scheduler.close()

    _run(scenario())


def test_expired_candidate_snapshot_returns_no_candidate_or_timestamp() -> None:
    async def scenario() -> None:
        now = [0.0]
        observer = FakeObserver()
        scheduler = PresenceScheduler(
            observer, policy=PresencePolicy(drained_fallback_seconds=30), clock=lambda: now[0]
        )
        first = asyncio.create_task(scheduler.wait_for_attempt())
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        observer.emit(BLEDevice("AA:BB", "omi", object()))
        await first
        await scheduler.attempt_finished("clean")
        now[0] = 61.0
        assert scheduler._fresh_candidate_snapshot() == (None, None, None)
        await scheduler.close()

    _run(scenario())


def test_startup_scans_before_the_first_thirty_second_fallback() -> None:
    async def scenario() -> None:
        now = [0.0]

        async def sleep_clock(delay: float) -> None:
            now[0] += delay

        observer = FakeObserver()
        scheduler = PresenceScheduler(observer, clock=lambda: now[0], sleep=sleep_clock)
        wake = await scheduler.wait_for_attempt()
        assert wake.reason == "fallback"
        assert now[0] == 30.0
        assert observer.starts == 1
        assert observer.stops == 1
        await scheduler.close()

    _run(scenario())


def test_hanging_scan_start_is_bounded_and_falls_back_after_cleanup() -> None:
    async def scenario() -> None:
        events: list[str] = []

        class HangingStart:
            async def start(self, callback: Callable[[object], object]) -> None:
                del callback
                events.append("start")
                await asyncio.Future()

            async def stop(self) -> None:
                events.append("stop")

        scheduler = PresenceScheduler(
            HangingStart(),
            policy=PresencePolicy(fallback_seconds=0.02, drained_fallback_seconds=0.02, scan_transition_seconds=0.01),
        )
        assert (await scheduler.wait_for_attempt()).reason == "fallback"
        await scheduler.attempt_finished("clean")
        wake = await asyncio.wait_for(scheduler.wait_for_attempt(), timeout=0.2)

        assert wake.reason == "fallback"
        assert events == ["start", "stop", "start", "stop", "start", "stop"]
        await scheduler.close()

    _run(scenario())


def test_hanging_scan_stop_fails_closed_without_releasing_attempt() -> None:
    async def scenario() -> None:
        events: list[str] = []

        class HangingStop:
            async def start(self, callback: Callable[[object], object]) -> None:
                del callback
                events.append("start")

            async def stop(self) -> None:
                events.append("stop")
                await asyncio.Future()

        scheduler = PresenceScheduler(
            HangingStop(),
            policy=PresencePolicy(fallback_seconds=0.02, drained_fallback_seconds=0.02, scan_transition_seconds=0.005),
        )
        with pytest.raises(PresenceScanStopError):
            await asyncio.wait_for(scheduler.wait_for_attempt(), timeout=0.2)

        assert events == ["start", "stop"]
        await scheduler.close()

    _run(scenario())


def test_cancellation_resistant_scan_start_never_releases_gatt_fallback() -> None:
    async def scenario() -> None:
        release = asyncio.Event()
        events: list[str] = []

        class ResistantStart:
            async def start(self, callback: Callable[[object], object]) -> None:
                del callback
                events.append("start")
                try:
                    await asyncio.Future()
                except asyncio.CancelledError:
                    await release.wait()

            async def stop(self) -> None:
                events.append("stop")

        scheduler = PresenceScheduler(
            ResistantStart(),
            policy=PresencePolicy(fallback_seconds=0.02, scan_transition_seconds=0.005),
        )
        with pytest.raises(PresenceScanTransitionError):
            await asyncio.wait_for(scheduler.attempt_finished("clean"), timeout=0.2)
        assert events == ["start", "stop"]
        release.set()
        await asyncio.sleep(0)
        await scheduler.close()

    _run(scenario())


def test_fallback_stops_scanner_only_when_attempt_is_due() -> None:
    async def scenario() -> None:
        now = [0.0]
        stop_at: list[float] = []

        async def sleep_clock(delay: float) -> None:
            now[0] += delay

        class Observer:
            async def start(self, callback: Callable[[object], object]) -> None:
                del callback

            async def stop(self) -> None:
                stop_at.append(now[0])

        scheduler = PresenceScheduler(
            Observer(),
            policy=PresencePolicy(fallback_seconds=30.0, drained_fallback_seconds=30.0, scan_transition_seconds=0.1),
            clock=lambda: now[0],
            sleep=sleep_clock,
        )
        await scheduler.wait_for_attempt()
        await scheduler.attempt_finished("clean")
        assert now[0] == 30.0
        assert scheduler._next_deadline(59.89) == pytest.approx(60.0)
        assert scheduler._next_deadline(59.9) == pytest.approx(60.0)
        assert scheduler._next_deadline(59.91) == pytest.approx(60.0)
        wake = await scheduler.wait_for_attempt()

        assert wake.reason == "fallback"
        assert now[0] == 60.0
        assert stop_at == [30.0, 60.0]
        await scheduler.close()

    _run(scenario())


def test_ad_during_start_cannot_leave_a_stale_wake_before_retry_deadline() -> None:
    async def scenario() -> None:
        now = [0.0]
        timer_released = asyncio.Event()

        async def sleep_until_released(_delay: float) -> None:
            await timer_released.wait()
            timer_released.clear()

        class EmitsDuringStart:
            async def start(self, callback: Callable[[object], object]) -> None:
                callback(BLEDevice("AA:BB", "omi", object()))

            async def stop(self) -> None:
                pass

        scheduler = PresenceScheduler(
            EmitsDuringStart(),
            policy=PresencePolicy(fallback_seconds=300.0, rapid_backoff=(10.0,), scan_transition_seconds=0.01),
            clock=lambda: now[0],
            sleep=sleep_until_released,
        )
        scheduler._fallback_deadline = 0.001
        assert (await scheduler.wait_for_attempt()).reason == "advertisement"
        await scheduler.attempt_finished("connected_interrupted")
        retry_wait = asyncio.create_task(scheduler.wait_for_attempt())
        await asyncio.sleep(0)
        assert not retry_wait.done()
        now[0] = 10.0
        timer_released.set()
        assert (await retry_wait).reason == "rapid_retry"
        await scheduler.close()

    _run(scenario())


def test_ad_wake_stops_scan_before_provider_construction() -> None:
    async def scenario() -> None:
        observer = FakeObserver()
        now = [0.0]
        scheduler = PresenceScheduler(
            observer, policy=PresencePolicy(drained_fallback_seconds=30), clock=lambda: now[0]
        )
        first = asyncio.create_task(scheduler.wait_for_attempt())
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        observer.emit()
        await first
        await scheduler.attempt_finished("clean")
        order: list[str] = []

        async def wait_and_construct() -> None:
            await scheduler.wait_for_attempt()
            order.append("provider")

        task = asyncio.create_task(wait_and_construct())
        await asyncio.sleep(0)
        now[0] = 60.0
        observer.emit()
        await task
        order.insert(0, "scan-stopped" if not observer.active else "scan-active")
        assert order == ["scan-stopped", "provider"]
        await scheduler.close()

    _run(scenario())


def test_returning_pendant_cannot_bypass_active_drained_cooldown() -> None:
    async def scenario() -> None:
        observer = FakeObserver()
        now = [0.0]
        timer_released = asyncio.Event()

        async def sleep_until_released(_delay: float) -> None:
            await timer_released.wait()
            timer_released.clear()

        scheduler = PresenceScheduler(
            observer,
            policy=PresencePolicy(fallback_seconds=30.0, drained_fallback_seconds=900.0),
            clock=lambda: now[0],
            sleep=sleep_until_released,
        )
        scheduler._fallback_deadline = 0.0
        initial = asyncio.create_task(scheduler.wait_for_attempt())
        await asyncio.sleep(0)
        timer_released.set()
        assert (await initial).reason == "fallback"
        await scheduler.attempt_finished("clean")
        wait = asyncio.create_task(scheduler.wait_for_attempt())
        await asyncio.sleep(0)
        now[0] = 90.0
        observer.emit()
        await asyncio.sleep(0)
        assert not wait.done()
        now[0] = 899.0
        observer.emit()
        await asyncio.sleep(0)
        assert not wait.done()
        now[0] = 900.0
        observer.emit()
        wake = await wait
        assert wake.reason == "advertisement"
        await scheduler.close()

    _run(scenario())


def test_return_after_long_absence_wakes_once_drained_cooldown_elapsed() -> None:
    async def scenario() -> None:
        observer = FakeObserver()
        now = [0.0]
        scheduler = PresenceScheduler(
            observer,
            policy=PresencePolicy(fallback_seconds=30.0, drained_fallback_seconds=900.0),
            clock=lambda: now[0],
        )
        scheduler._fallback_deadline = 0.0
        assert (await scheduler.wait_for_attempt()).reason == "fallback"
        await scheduler.attempt_finished("clean")
        wait = asyncio.create_task(scheduler.wait_for_attempt())
        await asyncio.sleep(0)
        now[0] = 3600.0
        observer.emit()
        wake = await wait
        assert wake.reason == "advertisement"
        await scheduler.close()

    _run(scenario())


def test_expired_drained_cooldown_absence_uses_future_timer() -> None:
    now = [901.0]
    scheduler = PresenceScheduler(
        FakeObserver(),
        policy=PresencePolicy(drained_fallback_seconds=900.0),
        clock=lambda: now[0],
    )
    scheduler._drained_mode = True
    scheduler._fallback_deadline = 900.0
    scheduler._last_matching = 0.0

    assert scheduler._due_reason(now[0]) is None
    assert scheduler._next_deadline(now[0]) == 961.0


def test_ads_refresh_rapid_retry_freshness_without_bypassing_backoff() -> None:
    async def scenario() -> None:
        observer = FakeObserver()
        now = [0.0]
        timer_released = asyncio.Event()

        async def sleep_until_released(_delay: float) -> None:
            await timer_released.wait()
            timer_released.clear()

        scheduler = PresenceScheduler(
            observer,
            policy=PresencePolicy(fallback_seconds=300.0, drained_fallback_seconds=30.0, rapid_backoff=(10.0, 20.0)),
            clock=lambda: now[0],
            sleep=sleep_until_released,
        )
        scheduler._fallback_deadline = 0.0
        await scheduler.wait_for_attempt()
        await scheduler.attempt_finished("clean")
        first = asyncio.create_task(scheduler.wait_for_attempt())
        await asyncio.sleep(0)
        now[0] = 60.0
        observer.emit()
        assert (await first).reason == "advertisement"

        await scheduler.attempt_finished("retry")
        retry_wait = asyncio.create_task(scheduler.wait_for_attempt())
        await asyncio.sleep(0)
        now[0] = 61.0
        observer.emit()
        await asyncio.sleep(0)
        assert not retry_wait.done()
        now[0] = 69.0
        observer.emit()
        await asyncio.sleep(0)
        assert not retry_wait.done()

        now[0] = 70.0
        observer.emit()
        timer_released.set()
        assert (await retry_wait).reason == "rapid_retry"

        await scheduler.attempt_finished("retry")
        absence_wait = asyncio.create_task(scheduler.wait_for_attempt())
        await asyncio.sleep(0)
        now[0] = 130.0
        observer.emit()
        assert (await absence_wait).reason == "advertisement"
        await scheduler.close()

    _run(scenario())


def test_retry_after_progress_restarts_at_first_rapid_delay() -> None:
    async def scenario() -> None:
        observer = FakeObserver()
        now = [0.0]
        timer_released = asyncio.Event()

        async def sleep_until_released(_delay: float) -> None:
            await timer_released.wait()
            timer_released.clear()

        scheduler = PresenceScheduler(
            observer,
            policy=PresencePolicy(fallback_seconds=300.0, drained_fallback_seconds=30.0, rapid_backoff=(10.0, 20.0)),
            clock=lambda: now[0],
            sleep=sleep_until_released,
        )
        scheduler._fallback_deadline = 0.0
        await scheduler.wait_for_attempt()
        await scheduler.attempt_finished("clean")
        first = asyncio.create_task(scheduler.wait_for_attempt())
        await asyncio.sleep(0)
        now[0] = 60.0
        observer.emit()
        await first

        await scheduler.attempt_finished("retry")
        second = asyncio.create_task(scheduler.wait_for_attempt())
        await asyncio.sleep(0)
        now[0] = 70.0
        timer_released.set()
        assert (await second).reason == "rapid_retry"

        await scheduler.attempt_finished("retry")
        third = asyncio.create_task(scheduler.wait_for_attempt())
        await asyncio.sleep(0)
        now[0] = 90.0
        timer_released.set()
        assert (await third).reason == "rapid_retry"

        await scheduler.attempt_finished("retry_after_progress")
        fourth = asyncio.create_task(scheduler.wait_for_attempt())
        await asyncio.sleep(0)
        now[0] = 100.0
        timer_released.set()
        assert (await fourth).reason == "rapid_retry"
        await scheduler.close()

    _run(scenario())


def test_continuous_advisory_ads_do_not_reconnect_before_fallback() -> None:
    async def scenario() -> None:
        observer = FakeObserver()
        scheduler = PresenceScheduler(
            observer,
            policy=PresencePolicy(fallback_seconds=0.05, drained_fallback_seconds=0.05, scan_transition_seconds=0.001),
        )
        await scheduler.wait_for_attempt()
        await scheduler.attempt_finished("clean")
        task = asyncio.create_task(scheduler.wait_for_attempt())
        for _ in range(3):
            await asyncio.sleep(0.005)
            observer.emit()
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(asyncio.shield(task), timeout=0.01)
        wake = await task
        assert wake.reason == "fallback"
        await scheduler.close()

    _run(scenario())


def test_cancellation_stops_active_scan() -> None:
    async def scenario() -> None:
        observer = FakeObserver()
        scheduler = PresenceScheduler(
            observer,
            policy=PresencePolicy(fallback_seconds=1, drained_fallback_seconds=1, scan_transition_seconds=0.001),
        )
        await scheduler.wait_for_attempt()
        await scheduler.attempt_finished("clean")
        task = asyncio.create_task(scheduler.wait_for_attempt())
        await asyncio.sleep(0)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert not observer.active
        assert observer.stops >= 1
        await scheduler.close()

    _run(scenario())


def test_cancellation_with_resistant_scan_stop_is_bounded_and_preserves_cancelled_error() -> None:
    async def scenario() -> None:
        release = asyncio.Event()
        active = False

        class ResistantStop:
            async def start(self, callback: Callable[[object], object]) -> None:
                nonlocal active
                del callback
                active = True

            async def stop(self) -> None:
                try:
                    await asyncio.Future()
                except asyncio.CancelledError:
                    await release.wait()
                finally:
                    nonlocal active
                    active = False

        scheduler = PresenceScheduler(
            ResistantStop(),
            policy=PresencePolicy(fallback_seconds=1, drained_fallback_seconds=1, scan_transition_seconds=0.005),
        )
        scheduler._fallback_deadline = 0.0
        await scheduler.wait_for_attempt()
        await scheduler.attempt_finished("clean")
        wait_task = asyncio.create_task(scheduler.wait_for_attempt())
        await asyncio.sleep(0)
        wait_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(wait_task, timeout=0.2)
        assert active
        release.set()
        await asyncio.sleep(0)
        await scheduler.close()
        assert not active

    _run(scenario())
