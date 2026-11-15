from __future__ import annotations

import asyncio
from collections.abc import Callable

from omi_collector.capture.application.ble_scan_health import ScanHealthMonitor, ScanIncidentContext, ScanRadioActivity
from omi_collector.capture.application.presence import (
    PresenceAdvertisement,
    PresencePolicy,
    PresenceScheduler,
    PresenceWake,
)


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


def _policy() -> PresencePolicy:
    return PresencePolicy(scan_recheck_seconds=300.0)


def test_ambient_radio_activity_keeps_one_searching_scan_without_stop() -> None:
    async def scenario() -> None:
        clock = ManualClock()
        observer = FakeObserver(clock)
        scheduler = PresenceScheduler(observer, policy=_policy(), clock=clock, sleep=clock.sleep)
        waiter = asyncio.create_task(scheduler.wait_for_attempt())
        try:
            await _wait_for_events(observer, 1)
            await _settle()
            for _ in range(60):
                observer.emit(ScanRadioActivity(clock()))
                clock.advance(1.0)
                await _settle()
            assert observer.events == ["start"]
            assert not waiter.done()
        finally:
            waiter.cancel()
            await asyncio.gather(waiter, return_exceptions=True)
            await scheduler.close()

    asyncio.run(scenario())


def test_silent_scan_keeps_one_generation_until_the_health_deadline() -> None:
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


def test_target_with_fresh_radio_pairs_requires_30_second_stability() -> None:
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
