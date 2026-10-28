from __future__ import annotations

import asyncio
from collections.abc import Callable

import pytest

from omi_collector.capture.application import presence_machine as machine
from omi_collector.capture.application.ble_scan_health import ScanHealthMonitor, ScanIncidentContext, ScanRadioActivity
from omi_collector.capture.application.presence import (
    PresenceAdvertisement,
    PresenceEnd,
    PresencePolicy,
    PresenceScanStopError,
    PresenceScheduler,
    PresenceWake,
)
from omi_collector.capture.application.presence_machine import CleanDrain, NotConnected


class ManualClock:
    def __init__(self) -> None:
        self.now = 0.0
        self.sleepers: list[tuple[float, asyncio.Future[None]]] = []

    def __call__(self) -> float:
        return self.now

    async def sleep(self, delay: float) -> None:
        if delay <= 0:
            return
        future = asyncio.get_running_loop().create_future()
        entry = (self.now + delay, future)
        self.sleepers.append(entry)
        try:
            await future
        finally:
            self.sleepers.remove(entry)

    def advance(self, seconds: float) -> None:
        self.now += seconds
        for deadline, future in tuple(self.sleepers):
            if deadline <= self.now and not future.done():
                future.set_result(None)


class FakeObserver:
    def __init__(self, clock: ManualClock, *, start_seconds: float = 0.0, stop_seconds: float = 0.0) -> None:
        self.clock = clock
        self.start_seconds = start_seconds
        self.stop_seconds = stop_seconds
        self.events: list[str] = []
        self.callbacks: list[Callable[[object], object]] = []
        self.on_start: Callable[[Callable[[object], object]], None] | None = None

    async def start(self, callback: Callable[[object], object]) -> None:
        self.callbacks.append(callback)
        self.events.append("start")
        if self.on_start is not None:
            self.on_start(callback)
        self.clock.advance(self.start_seconds)

    async def stop(self) -> None:
        self.events.append("stop")
        self.clock.advance(self.stop_seconds)

    def emit(self, observation: object, generation: int = -1) -> None:
        callback = self.callbacks[generation]
        callback(observation)


class SilentDiagnostics:
    async def prepare(self, generation: int) -> None:
        del generation

    async def close(self, generation: int) -> None:
        del generation

    async def snapshot(self, generation: int) -> dict[str, object]:
        return {"generation": generation}

    def healthy(self, generation: int) -> None:
        del generation


class IncidentSink:
    def __init__(self) -> None:
        self.opened: list[ScanIncidentContext] = []

    async def active(self) -> bool:
        return False

    async def open(self, context: ScanIncidentContext) -> None:
        self.opened.append(context)

    async def resolve(self, generation: int, reason: str) -> None:
        del generation, reason


async def _settle() -> None:
    for _ in range(5):
        await asyncio.sleep(0)


async def _wait_for_events(observer: FakeObserver, count: int) -> None:
    async with asyncio.timeout(1.0):
        while len(observer.events) < count:
            await asyncio.sleep(0)


def _policy(**kwargs: object) -> PresencePolicy:
    return PresencePolicy(
        scan_recheck_seconds=300.0,
        scan_active_seconds=5.0,
        scan_pause_seconds=10.0,
        **kwargs,  # type: ignore[arg-type]
    )


def test_scan_pause_starts_after_confirmed_stop_and_restart_starts_new_window() -> None:
    async def scenario() -> None:
        clock = ManualClock()
        observer = FakeObserver(clock, start_seconds=2.0, stop_seconds=3.0)
        scheduler = PresenceScheduler(observer, policy=_policy(), clock=clock, sleep=clock.sleep)
        waiter = asyncio.create_task(scheduler.wait_for_attempt())
        try:
            await _wait_for_events(observer, 1)
            await _settle()
            observer.emit(ScanRadioActivity(clock()))
            await _settle()

            clock.advance(5.0)
            await _wait_for_events(observer, 2)
            await _settle()
            assert observer.events == ["start", "stop"]
            assert clock() == 10.0

            clock.advance(9.0)
            await _settle()
            assert observer.events == ["start", "stop"]

            clock.advance(1.0)
            await _wait_for_events(observer, 3)
            await _settle()
            assert observer.events == ["start", "stop", "start"]
            assert clock() == 22.0

            old_callback = observer.callbacks[0]
            old_callback(ScanRadioActivity(clock()))
            await _settle()
            clock.advance(30.0)
            await _settle()
            assert observer.events == ["start", "stop", "start"]
        finally:
            waiter.cancel()
            await asyncio.gather(waiter, return_exceptions=True)
            await scheduler.close()

    asyncio.run(scenario())


def test_silent_scan_keeps_one_generation_until_the_existing_health_deadline() -> None:
    async def scenario() -> None:
        clock = ManualClock()
        observer = FakeObserver(clock)
        sink = IncidentSink()
        monitor = ScanHealthMonitor(sink, SilentDiagnostics(), 180.0, clock=clock)
        scheduler = PresenceScheduler(observer, policy=_policy(), scan_health=monitor, clock=clock, sleep=clock.sleep)
        waiter = asyncio.create_task(scheduler.wait_for_attempt())
        try:
            await _wait_for_events(observer, 1)
            await _settle()
            clock.advance(5.0)
            await _settle()
            assert observer.events == ["start"]
            assert not sink.opened

            clock.advance(175.0)
            async with asyncio.timeout(1.0):
                while not sink.opened:
                    await asyncio.sleep(0)
            assert observer.events == ["start"]
            assert sink.opened[0].reason == "scan_silence"
            assert sink.opened[0].generation == 0
            assert sink.opened[0].active_seconds == 180.0
        finally:
            waiter.cancel()
            await asyncio.gather(waiter, return_exceptions=True)
            await scheduler.close()

    asyncio.run(scenario())


def test_first_radio_activity_after_silent_window_wakes_the_same_generation_for_pause() -> None:
    async def scenario() -> None:
        clock = ManualClock()
        observer = FakeObserver(clock)
        scheduler = PresenceScheduler(observer, policy=_policy(), clock=clock, sleep=clock.sleep)
        waiter = asyncio.create_task(scheduler.wait_for_attempt())
        try:
            await _wait_for_events(observer, 1)
            await _settle()
            clock.advance(7.0)
            observer.emit(ScanRadioActivity(clock()))
            await _wait_for_events(observer, 2)
            await _settle()
            assert observer.events == ["start", "stop"]
        finally:
            waiter.cancel()
            await asyncio.gather(waiter, return_exceptions=True)
            await scheduler.close()

    asyncio.run(scenario())


def test_elapsed_cadence_deadline_during_wait_setup_still_triggers_pause() -> None:
    async def scenario() -> None:
        clock = ManualClock()
        observer = FakeObserver(clock)
        scheduler = PresenceScheduler(observer, policy=_policy(), clock=clock, sleep=clock.sleep)
        waiter = asyncio.create_task(scheduler.wait_for_attempt())
        try:
            await _wait_for_events(observer, 1)
            await _settle()
            original_wait = scheduler._wait_until_event
            crossed_boundary = False

            async def cross_boundary(deadline: float) -> PresenceWake | PresenceEnd | None:
                nonlocal crossed_boundary
                if not crossed_boundary:
                    crossed_boundary = True
                    clock.advance(5.0)
                return await original_wait(deadline)

            scheduler._wait_until_event = cross_boundary
            observer.emit(ScanRadioActivity(clock()))
            await _wait_for_events(observer, 2)
            await _settle()
            assert crossed_boundary
            assert observer.events == ["start", "stop"]
        finally:
            waiter.cancel()
            await asyncio.gather(waiter, return_exceptions=True)
            await scheduler.close()

    asyncio.run(scenario())


def test_boundary_weak_candidate_latches_until_the_existing_max_gap_expires() -> None:
    async def scenario() -> None:
        clock = ManualClock()
        observer = FakeObserver(clock)
        scheduler = PresenceScheduler(observer, policy=_policy(), clock=clock, sleep=clock.sleep)
        waiter = asyncio.create_task(scheduler.wait_for_attempt())
        try:
            await _wait_for_events(observer, 1)
            await _settle()
            clock.advance(5.0)
            callback = observer.callbacks[0]
            candidate = object()
            callback(ScanRadioActivity(clock()))
            callback(PresenceAdvertisement(candidate, None))
            await _settle()
            assert observer.events == ["start"]
            assert isinstance(scheduler._state, machine.Searching)
            assert scheduler._state.advertisement is not None
            assert scheduler._state.advertisement.candidate is candidate
            assert all(deadline > clock() for deadline, _ in clock.sleepers)

            clock.advance(9.999)
            await _settle()
            assert observer.events == ["start"]

            clock.advance(0.001)
            await _wait_for_events(observer, 2)
            await _settle()
            assert observer.events == ["start", "stop"]
        finally:
            waiter.cancel()
            await asyncio.gather(waiter, return_exceptions=True)
            await scheduler.close()

    asyncio.run(scenario())


def test_target_with_fresh_radio_pairs_still_requires_existing_30_second_stability() -> None:
    async def scenario() -> None:
        clock = ManualClock()
        observer = FakeObserver(clock)
        scheduler = PresenceScheduler(observer, policy=_policy(), clock=clock, sleep=clock.sleep)
        waiter = asyncio.create_task(scheduler.wait_for_attempt())
        candidate = object()
        try:
            await _wait_for_events(observer, 1)
            await _settle()
            callback = observer.callbacks[0]
            for observed_at in (0.0, 5.0, 10.0, 15.0, 20.0, 25.0):
                clock.advance(observed_at - clock())
                callback(ScanRadioActivity(clock()))
                callback(PresenceAdvertisement(candidate, None))
                await _settle()
                assert not waiter.done()
                assert observer.events == ["start"]

            clock.advance(5.0)
            callback(ScanRadioActivity(clock()))
            callback(PresenceAdvertisement(candidate, None))
            wake = await asyncio.wait_for(waiter, timeout=1.0)

            assert isinstance(wake, PresenceWake)
            assert wake.observed_at == 30.0
            assert wake.candidate is candidate
            assert observer.events == ["start", "stop"]
        finally:
            if not waiter.done():
                waiter.cancel()
            await asyncio.gather(waiter, return_exceptions=True)
            await scheduler.close()

    asyncio.run(scenario())


def test_activity_during_start_counts_and_close_interrupts_pause() -> None:
    async def scenario() -> None:
        clock = ManualClock()
        observer = FakeObserver(clock, start_seconds=1.0)

        def emit_during_start(callback: Callable[[object], object]) -> None:
            callback(ScanRadioActivity(clock()))

        observer.on_start = emit_during_start
        scheduler = PresenceScheduler(observer, policy=_policy(), clock=clock, sleep=clock.sleep)
        waiter = asyncio.create_task(scheduler.wait_for_attempt())
        await _wait_for_events(observer, 1)
        await _settle()

        clock.advance(5.0)
        await _wait_for_events(observer, 2)
        await _settle()
        assert observer.events == ["start", "stop"]

        await scheduler.close()
        try:
            await waiter
        except RuntimeError as error:
            assert "closed" in str(error)
        else:
            raise AssertionError("closing during pause did not stop the waiter")

    asyncio.run(scenario())


def test_cancelling_during_pause_does_not_restart_scanning() -> None:
    async def scenario() -> None:
        clock = ManualClock()
        observer = FakeObserver(clock)

        def emit_during_start(callback: Callable[[object], object]) -> None:
            callback(ScanRadioActivity(clock()))

        observer.on_start = emit_during_start
        scheduler = PresenceScheduler(observer, policy=_policy(), clock=clock, sleep=clock.sleep)
        waiter = asyncio.create_task(scheduler.wait_for_attempt())
        await _wait_for_events(observer, 1)
        await _settle()
        clock.advance(5.0)
        await _wait_for_events(observer, 2)
        await _settle()

        waiter.cancel()
        await asyncio.gather(waiter, return_exceptions=True)
        clock.advance(20.0)
        await _settle()
        assert observer.events == ["start", "stop"]
        await scheduler.close()

    asyncio.run(scenario())


def test_planned_stop_failure_closes_without_restarting() -> None:
    async def scenario() -> None:
        clock = ManualClock()

        class StopFailure(FakeObserver):
            async def stop(self) -> None:
                self.events.append("stop-failed")
                raise RuntimeError("stop refused")

        observer = StopFailure(clock)

        def emit_during_start(callback: Callable[[object], object]) -> None:
            callback(ScanRadioActivity(clock()))

        observer.on_start = emit_during_start
        scheduler = PresenceScheduler(observer, policy=_policy(), clock=clock, sleep=clock.sleep)
        waiter = asyncio.create_task(scheduler.wait_for_attempt())
        await _wait_for_events(observer, 1)
        await _settle()
        clock.advance(5.0)

        try:
            await asyncio.wait_for(waiter, timeout=1.0)
        except PresenceScanStopError:
            pass
        else:
            raise AssertionError("a failed planned stop did not fail closed")
        assert observer.events == ["start", "stop-failed", "stop-failed"]
        await scheduler.close()

    asyncio.run(scenario())


@pytest.mark.parametrize("outcome", [CleanDrain(), NotConnected(durable_progress=False)])
def test_retry_and_cooldown_scans_do_not_enter_idle_pause(outcome: CleanDrain | NotConnected) -> None:
    async def scenario() -> None:
        clock = ManualClock()
        observer = FakeObserver(clock)
        policy = PresencePolicy(
            scan_recheck_seconds=300.0,
            scan_active_seconds=5.0,
            scan_pause_seconds=10.0,
            arrival_stability_seconds=0.01,
            drain_cooldown_seconds=60.0,
            rapid_backoff=(60.0,),
        )
        scheduler = PresenceScheduler(observer, policy=policy, clock=clock, sleep=clock.sleep)
        first = asyncio.create_task(scheduler.wait_for_attempt())
        second: asyncio.Task[PresenceWake | PresenceEnd] | None = None
        try:
            await _wait_for_events(observer, 1)
            await _settle()
            candidate = object()
            callback = observer.callbacks[0]
            callback(ScanRadioActivity(clock()))
            callback(PresenceAdvertisement(candidate, None))
            await _settle()

            clock.advance(0.01)
            callback(ScanRadioActivity(clock()))
            callback(PresenceAdvertisement(candidate, None))
            wake = await asyncio.wait_for(first, timeout=1.0)
            assert isinstance(wake, PresenceWake)
            assert wake.candidate is candidate
            assert observer.events == ["start", "stop"]

            await scheduler.attempt_finished(outcome)
            second = asyncio.create_task(scheduler.wait_for_attempt())
            await _wait_for_events(observer, 3)
            await _settle()
            observer.emit(ScanRadioActivity(clock()))
            await _settle()
            clock.advance(6.0)
            await _settle()
            assert observer.events == ["start", "stop", "start"]
        finally:
            if not first.done():
                first.cancel()
            if second is not None and not second.done():
                second.cancel()
            await asyncio.gather(*(task for task in (first, second) if task is not None), return_exceptions=True)
            await scheduler.close()

    asyncio.run(scenario())
