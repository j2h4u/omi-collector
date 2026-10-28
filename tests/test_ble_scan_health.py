from __future__ import annotations

import asyncio
from collections.abc import Callable

from omi_collector.capture.application.ble_scan_health import (
    ScanHealthMonitor,
    ScanIncidentContext,
    ScanRadioActivity,
)
from omi_collector.capture.application.presence import PresencePolicy, PresenceScheduler
from omi_collector.config import PresenceConfig


class FakeDiagnostics:
    def __init__(self, events: list[str] | None = None) -> None:
        self.events = events if events is not None else []
        self.healthy_generations: list[int] = []

    async def prepare(self, generation: int) -> None:
        self.events.append(f"prepare:{generation}")

    async def close(self, generation: int) -> None:
        self.events.append(f"close:{generation}")

    async def snapshot(self, generation: int) -> dict[str, object]:
        return {"generation": generation}

    def healthy(self, generation: int) -> None:
        self.healthy_generations.append(generation)


class FakeSink:
    def __init__(self, *, active: bool = False) -> None:
        self.is_active = active
        self.opened: list[ScanIncidentContext] = []
        self.resolved: list[tuple[int, str]] = []
        self.active_calls = 0

    async def active(self) -> bool:
        self.active_calls += 1
        return self.is_active

    async def open(self, context: ScanIncidentContext) -> None:
        self.opened.append(context)
        self.is_active = True

    async def resolve(self, generation: int, reason: str) -> None:
        self.resolved.append((generation, reason))
        self.is_active = False


class FakeObserver:
    def __init__(self, events: list[str] | None = None) -> None:
        self.events = events if events is not None else []
        self.callback: Callable[[object], object] | None = None

    async def start(self, callback: Callable[[object], object]) -> None:
        self.events.append("start")
        self.callback = callback

    async def stop(self) -> None:
        self.events.append("stop")


def test_presence_config_defaults_and_validates_scan_silence_threshold() -> None:
    assert PresenceConfig().scan_silence_seconds == 180.0

    for value in (0.0, -1.0):
        try:
            PresenceConfig(scan_silence_seconds=value)
        except ValueError as error:
            assert "scan_silence_seconds" in str(error)
        else:
            raise AssertionError("non-positive silence threshold was accepted")


def test_scheduler_prepares_diagnostics_before_scan_and_opens_on_all_device_silence() -> None:
    async def scenario() -> None:
        now = [10.0]
        events: list[str] = []
        diagnostics = FakeDiagnostics(events)
        sink = FakeSink()
        monitor = ScanHealthMonitor(sink, diagnostics, 180.0, clock=lambda: now[0])
        observer = FakeObserver(events)

        async def sleep(delay: float) -> None:
            now[0] += delay
            await asyncio.sleep(0)

        scheduler = PresenceScheduler(
            observer,
            policy=PresencePolicy(scan_recheck_seconds=300.0),
            scan_health=monitor,
            clock=lambda: now[0],
            sleep=sleep,
        )
        waiter = asyncio.create_task(scheduler.wait_for_attempt())
        try:
            async with asyncio.timeout(1.0):
                while not sink.opened:
                    await asyncio.sleep(0)
            assert events[:2] == ["prepare:0", "start"]
            incident = sink.opened[0]
            assert incident.generation == 0
            assert incident.reason == "scan_silence"
            assert incident.active_seconds == 180.0
            assert incident.bleak_callbacks == 0
            assert incident.threshold_seconds == 180.0
            assert monitor.next_deadline is None
        finally:
            waiter.cancel()
            await asyncio.gather(waiter, return_exceptions=True)
            await scheduler.close()

    asyncio.run(scenario())


def test_packet_during_scan_start_resolves_persisted_incident() -> None:
    async def scenario() -> None:
        now = [0.0]
        sink = FakeSink(active=True)
        monitor = ScanHealthMonitor(sink, FakeDiagnostics(), 180.0, clock=lambda: now[0])

        class PacketAtStart(FakeObserver):
            async def start(self, callback: Callable[[object], object]) -> None:
                await super().start(callback)
                callback(ScanRadioActivity(now[0]))

        observer = PacketAtStart()
        scheduler = PresenceScheduler(observer, scan_health=monitor, clock=lambda: now[0])
        waiter = asyncio.create_task(scheduler.wait_for_attempt())
        try:
            async with asyncio.timeout(1.0):
                while not sink.resolved:
                    await asyncio.sleep(0)
            assert sink.resolved == [(0, "advertisements_resumed")]
        finally:
            waiter.cancel()
            await asyncio.gather(waiter, return_exceptions=True)
            await scheduler.close()

    asyncio.run(scenario())


def test_cancelled_scan_start_closes_passive_diagnostics_after_stop_attempt() -> None:
    async def scenario() -> None:
        events: list[str] = []
        started = asyncio.Event()

        class CancelledStart(FakeObserver):
            async def start(self, callback: Callable[[object], object]) -> None:
                await super().start(callback)
                started.set()
                await asyncio.Future()

        diagnostics = FakeDiagnostics(events)
        monitor = ScanHealthMonitor(FakeSink(), diagnostics, 180.0)
        scheduler = PresenceScheduler(CancelledStart(events), scan_health=monitor)
        starting = asyncio.create_task(scheduler._start_scan())
        try:
            async with asyncio.timeout(1.0):
                await started.wait()
            starting.cancel()
            try:
                await starting
            except asyncio.CancelledError:
                pass
            else:
                raise AssertionError("cancelled start did not propagate cancellation")
            assert events == ["prepare:0", "start", "stop", "close:0"]
        finally:
            await asyncio.gather(starting, return_exceptions=True)
            await scheduler.close()

    asyncio.run(scenario())


def test_cancelled_scan_stop_closes_passive_diagnostics() -> None:
    async def scenario() -> None:
        events: list[str] = []
        stopping = asyncio.Event()
        stop_calls = 0

        class CancelledStop(FakeObserver):
            async def start(self, callback: Callable[[object], object]) -> None:
                await super().start(callback)

            async def stop(self) -> None:
                nonlocal stop_calls
                stop_calls += 1
                self.events.append("stop")
                if stop_calls == 1:
                    stopping.set()
                    await asyncio.Future()

        diagnostics = FakeDiagnostics(events)
        sink = FakeSink()
        monitor = ScanHealthMonitor(sink, diagnostics, 180.0)
        scheduler = PresenceScheduler(CancelledStop(events), scan_health=monitor)
        await scheduler._start_scan()
        stopping_task = asyncio.create_task(scheduler._stop_scan())
        try:
            async with asyncio.timeout(1.0):
                await stopping.wait()
            stopping_task.cancel()
            async with asyncio.timeout(1.0):
                try:
                    await stopping_task
                except asyncio.CancelledError:
                    pass
                else:
                    raise AssertionError("cancelled stop did not propagate cancellation")
            assert events == ["prepare:0", "start", "stop", "close:0"]
        finally:
            await asyncio.gather(stopping_task, return_exceptions=True)
            await scheduler.close()
        assert events == ["prepare:0", "start", "stop", "close:0", "stop"]
        assert sink.opened == []

    asyncio.run(scenario())


def test_radio_activity_resets_deadline_and_recovers_prior_incident_once() -> None:
    async def scenario() -> None:
        now = [0.0]
        sink = FakeSink(active=True)
        diagnostics = FakeDiagnostics()
        monitor = ScanHealthMonitor(sink, diagnostics, 5.0, clock=lambda: now[0])
        await monitor.prepare(4)
        now[0] = 2.0
        assert monitor.radio_activity(4, 2.0)
        assert not monitor.radio_activity(4, 2.0)
        monitor.scan_started(4)
        await monitor.flush_recovery()
        assert sink.active_calls == 1
        assert sink.resolved == [(4, "advertisements_resumed")]
        assert monitor.next_deadline == 7.0
        assert not monitor.radio_activity(3, 2.5)
        assert monitor._callback_count == 2
        assert diagnostics.healthy_generations == [4, 4]

    asyncio.run(scenario())


def test_ambient_callbacks_keep_scan_healthy_while_pendant_is_absent() -> None:
    async def scenario() -> None:
        now = [0.0]
        sink = FakeSink()
        monitor = ScanHealthMonitor(sink, FakeDiagnostics(), 10.0, clock=lambda: now[0])
        await monitor.prepare(6)
        monitor.scan_started(6)
        for at in (3.0, 6.0, 9.0, 12.0):
            now[0] = at
            assert not monitor.radio_activity(6, at)
            assert not await monitor.check_deadline(at)
        assert sink.opened == []
        assert monitor.next_deadline == 22.0

    asyncio.run(scenario())


def test_open_and_resolve_failures_retry_without_deadline_spin_or_losing_state() -> None:
    async def scenario() -> None:
        now = [0.0]

        class FlakySink(FakeSink):
            fail_open = True
            fail_resolve = True

            async def open(self, context: ScanIncidentContext) -> None:
                if self.fail_open:
                    self.fail_open = False
                    raise OSError("write failed")
                await super().open(context)

            async def resolve(self, generation: int, reason: str) -> None:
                if self.fail_resolve:
                    self.fail_resolve = False
                    raise OSError("resolve failed")
                await super().resolve(generation, reason)

        sink = FlakySink()
        monitor = ScanHealthMonitor(sink, FakeDiagnostics(), 3.0, clock=lambda: now[0])
        await monitor.prepare(1)
        monitor.scan_started(1)
        now[0] = 3.0
        assert await monitor.check_deadline()
        assert monitor.next_deadline == 4.0
        assert not await monitor.check_deadline(3.5)
        now[0] = 4.0
        assert await monitor.check_deadline()
        assert len(sink.opened) == 1
        assert monitor.next_deadline is None
        assert monitor.radio_activity(1, 4.1)
        await monitor.flush_recovery()
        assert monitor.next_deadline == 5.0
        now[0] = 5.0
        assert await monitor.check_deadline()
        assert sink.resolved == [(1, "advertisements_resumed")]
        assert monitor.next_deadline == 7.1

    asyncio.run(scenario())


def test_first_advertisement_during_incident_open_is_resolved_after_open() -> None:
    async def scenario() -> None:
        now = [0.0]
        opening = asyncio.Event()
        resume_open = asyncio.Event()

        class DelayedSink(FakeSink):
            async def open(self, context: ScanIncidentContext) -> None:
                opening.set()
                await resume_open.wait()
                await super().open(context)

        sink = DelayedSink()
        monitor = ScanHealthMonitor(sink, FakeDiagnostics(), 2.0, clock=lambda: now[0])
        await monitor.prepare(5)
        monitor.scan_started(5)
        now[0] = 2.0
        opening_task = asyncio.create_task(monitor.check_deadline())
        await opening.wait()
        assert not monitor.radio_activity(5, 2.1)
        resume_open.set()
        assert await opening_task
        assert len(sink.opened) == 1
        assert sink.resolved == [(5, "advertisements_resumed")]
        assert monitor.next_deadline == 4.1

    asyncio.run(scenario())


def test_stop_and_gatt_pause_clear_silence_baseline_and_stale_generation_is_ignored() -> None:
    async def scenario() -> None:
        now = [0.0]
        sink = FakeSink()
        diagnostics = FakeDiagnostics()
        monitor = ScanHealthMonitor(sink, diagnostics, 5.0, clock=lambda: now[0])
        await monitor.prepare(2)
        monitor.scan_started(2)
        now[0] = 4.0
        monitor.scan_stopping(2)
        await monitor.scan_stopped(2)
        now[0] = 100.0
        assert not await monitor.check_deadline()
        await monitor.prepare(3)
        monitor.radio_activity(3, 100.0)
        monitor.scan_started(3)
        now[0] = 102.0
        assert not monitor.radio_activity(2, 102.0)
        assert monitor.radio_activity(3, 102.0) is False
        assert monitor.next_deadline == 107.0
        assert sink.opened == []

    asyncio.run(scenario())


def test_start_and_stop_failure_open_incidents_and_diagnostic_capture_is_fail_open() -> None:
    async def scenario() -> None:
        now = [10.0]
        sink = FakeSink()

        class BrokenDiagnostics(FakeDiagnostics):
            async def prepare(self, generation: int) -> None:
                self.events.append(f"prepare:{generation}")
                raise OSError("capture unavailable")

        monitor = ScanHealthMonitor(sink, BrokenDiagnostics(), 5.0, clock=lambda: now[0])
        await monitor.prepare(8)
        await monitor.scan_start_failed(8, RuntimeError("start"))
        assert sink.opened[0].reason == "scan_start_failed"
        assert sink.opened[0].error_kind == "RuntimeError"

        sink = FakeSink()
        diagnostics = FakeDiagnostics()
        monitor = ScanHealthMonitor(sink, diagnostics, 5.0, clock=lambda: now[0])
        await monitor.prepare(9)
        monitor.scan_started(9)
        now[0] += 1.0
        await monitor.scan_stop_failed(9, RuntimeError("stop"))
        assert sink.opened[0].reason == "scan_stop_failed"
        assert sink.opened[0].active_seconds == 1.0
        assert diagnostics.events == ["prepare:9", "close:9"]

    asyncio.run(scenario())


def test_stop_failure_after_silence_keeps_original_incident_and_closes_capture() -> None:
    async def scenario() -> None:
        now = [0.0]
        sink = FakeSink()
        diagnostics = FakeDiagnostics()
        monitor = ScanHealthMonitor(sink, diagnostics, 2.0, clock=lambda: now[0])
        await monitor.prepare(11)
        monitor.scan_started(11)
        now[0] = 2.0
        assert await monitor.check_deadline()
        assert sink.opened[0].reason == "scan_silence"
        monitor.scan_stopping(11)
        await monitor.scan_stop_failed(11, RuntimeError("stop"))
        assert len(sink.opened) == 1
        assert sink.opened[0].reason == "scan_silence"
        assert diagnostics.events == ["prepare:11", "close:11"]

    asyncio.run(scenario())


def test_successful_inactive_state_lookup_clears_startup_unknown_state() -> None:
    async def scenario() -> None:
        class FlakyStateSink(FakeSink):
            async def active(self) -> bool:
                self.active_calls += 1
                if self.active_calls == 1:
                    raise OSError("database unavailable")
                return False

        now = [0.0]
        sink = FlakyStateSink()
        monitor = ScanHealthMonitor(sink, FakeDiagnostics(), 10.0, clock=lambda: now[0])
        await monitor.prepare(1)
        await monitor.scan_stopped(1)
        await monitor.prepare(2)
        monitor.scan_started(2)
        assert not monitor._incident_state_unknown
        assert not monitor.radio_activity(2, 0.0)
        await monitor.flush_recovery()
        assert sink.active_calls == 2
        assert sink.resolved == []

    asyncio.run(scenario())


def test_cancelled_start_failure_persistence_still_closes_diagnostics() -> None:
    async def scenario() -> None:
        opening = asyncio.Event()

        class BlockingSink(FakeSink):
            async def open(self, context: ScanIncidentContext) -> None:
                del context
                opening.set()
                await asyncio.Future()

        diagnostics = FakeDiagnostics()
        monitor = ScanHealthMonitor(BlockingSink(), diagnostics, 10.0)
        await monitor.prepare(12)
        failure = asyncio.create_task(monitor.scan_start_failed(12, RuntimeError("start")))
        async with asyncio.timeout(1.0):
            await opening.wait()
        failure.cancel()
        try:
            await failure
        except asyncio.CancelledError:
            pass
        else:
            raise AssertionError("cancelled persistence did not propagate cancellation")
        assert diagnostics.events == ["prepare:12", "close:12"]

    asyncio.run(scenario())


def test_unavailable_persisted_state_lookup_is_retried_on_next_prepare() -> None:
    async def scenario() -> None:
        class FlakyStateSink(FakeSink):
            async def active(self) -> bool:
                self.active_calls += 1
                if self.active_calls == 1:
                    raise OSError("database unavailable")
                return True

        sink = FlakyStateSink()
        monitor = ScanHealthMonitor(sink, FakeDiagnostics(), 10.0)
        await monitor.prepare(1)
        await monitor.scan_stopped(1)
        await monitor.prepare(2)
        monitor.scan_started(2)
        assert monitor.radio_activity(2, monitor._clock())
        await monitor.flush_recovery()
        assert sink.active_calls == 2
        assert sink.resolved == [(2, "advertisements_resumed")]

    asyncio.run(scenario())


def test_scan_radio_activity_is_frozen_and_does_not_expose_device_details() -> None:
    marker = ScanRadioActivity(1.0)
    assert repr(marker) == "ScanRadioActivity(observed_at=1.0)"

    try:
        marker.observed_at = 2.0  # type: ignore[misc]
    except AttributeError:
        pass
    else:
        raise AssertionError("scan radio activity marker was mutable")
