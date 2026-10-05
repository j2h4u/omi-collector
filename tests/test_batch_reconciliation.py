"""Startup closure behavior for durable unpublished streaming attempts."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import suppress
from dataclasses import dataclass, replace
from functools import wraps
from json import loads
from pathlib import Path
from struct import pack
from threading import Thread
from typing import cast, override

import pytest

from fakes import DelayedNotification, ScriptedRingSession, WriteStep
from omi_collector.capture.adapters.attempt_writer import WriterError, WriterFailedError
from omi_collector.capture.adapters.opportunistic_runtime import OpportunisticRuntime
from omi_collector.capture.adapters.publication import SealResult
from omi_collector.capture.adapters.staging_contract import AttemptDescriptor, StagingError
from omi_collector.capture.adapters.staging_store import StagingStore
from omi_collector.capture.application.batch_reconciliation import BatchReconciler, CursorConsistencyError
from omi_collector.capture.application.collector import (
    CollectionResult,
    CollectorTimeoutError,
    ProgressEvent,
    RingTransferError,
    TransferInterruptedError,
    TransferTimeouts,
)
from omi_collector.capture.application.ports import (
    BatchWriterPort,
    DurablePrefixShape,
    SealResultShape,
    StagingPort,
    WriterProgressShape,
)
from omi_collector.capture.application.quality_metrics import SessionQuality
from omi_collector.capture.application.session_lifecycle import OpportunisticOptions, RetryPolicy, SessionPhaseState
from omi_collector.capture.domain.ring_protocol import (
    RECORD_SIZE,
    DoneNotification,
    ReadBeginNotification,
    RingInfo,
    RingStatus,
)
from omi_collector.config import DEFAULT_CONFIG, WriterConfig


def _record(value: int) -> bytes:
    return pack(">I", value) + bytes((value % 256,)) * (RECORD_SIZE - 4)


def _async_test[**P, T](function: Callable[P, Awaitable[T]]) -> Callable[P, T]:
    @wraps(function)
    def run(*args: P.args, **kwargs: P.kwargs) -> T:
        return asyncio.run(function(*args, **kwargs))

    return run


def _seed_partial(spool: Path, capture_root: Path) -> tuple[StagingStore, Path, bytes, bytes]:
    store = StagingStore(spool, capture_root)
    attempt = store.prepare_streaming_attempt(100, 2)
    attempt.record_read_begin(ReadBeginNotification(100, 2))
    attempt.accept_chunk(100, _record(100))
    prefix = attempt.checkpoint()
    attempt.close(durable=True)

    assert prefix.next_sequence == 101
    descriptor = store.pending_attempts()[0]
    attempt_path = store.attempts_root / descriptor.attempt_id
    return (
        store,
        attempt_path,
        (attempt_path / "records.bin").read_bytes(),
        (attempt_path / "checkpoint.json").read_bytes(),
    )


@_async_test
async def test_regressed_pending_prefix_keeps_authenticated_visit_frontier(tmp_path: Path) -> None:
    store, _attempt_path, _raw, _checkpoint = _seed_partial(tmp_path / "spool", tmp_path / "captures")
    descriptor = store.pending_attempts()[0]
    durable_next = store.open_attempt(descriptor.attempt_id).recover().valid_records + descriptor.start_sequence
    assert durable_next == 101
    runtime = _Runtime()
    options = _options()

    async def quarantine(attempt_id: str) -> None:
        await asyncio.to_thread(store.quarantine_attempt_source, attempt_id)

    reconciler = BatchReconciler(store, options, runtime, quarantine)
    reconciler.set_startup_state(descriptor, durable_next)
    current = RingInfo(99, 102, 100, 0, RECORD_SIZE)
    session = ScriptedRingSession(
        RingStatus(0, 0, 0, 1),
        (
            WriteStep(b"\x11" + (99).to_bytes(8, "big") + (2).to_bytes(4, "big"), (_wire_begin(99, 2),)),
            WriteStep(b"\x13"),
        ),
    )

    async def info(_session: object) -> RingInfo:
        return current

    task = asyncio.create_task(reconciler.connected_step(session, current, info, SessionPhaseState("read/reconcile")))
    try:
        async with asyncio.timeout(5):
            while not runtime.proxies or not runtime.proxies[0].read_begin_complete.is_set():
                await asyncio.sleep(0)
        task.cancel()
        with suppress(asyncio.CancelledError):
            await task
        assert reconciler.pending_descriptor is None
        assert reconciler.durable_progress() == 101
        await reconciler.close_visit("absence")
        assert loads(store.ready_closures_path.read_text(encoding="utf-8"))["closures"] == [
            {"next_sequence": 101, "reason": "absence"}
        ]
    finally:
        if not task.done():
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        await _close_real_writers(runtime)
        await session.close()


@_async_test
async def test_cursor_ahead_with_metrics_disabled_has_no_metrics_error_event(tmp_path: Path) -> None:
    store, _path, _raw, _checkpoint = _seed_partial(tmp_path / "spool", tmp_path / "captures")
    descriptor = store.pending_attempts()[0]
    durable_next = store.open_attempt(descriptor.attempt_id).recover().valid_records + descriptor.start_sequence
    runtime = _Runtime()

    async def quarantine(_attempt_id: str) -> None:
        return None

    operational_events: list[dict[str, object]] = []

    def emit_operational(event: dict[str, object]) -> None:
        operational_events.append(event)

    options = replace(_options(), operational=emit_operational)
    reconciler = BatchReconciler(store, options, runtime, quarantine)
    reconciler.set_startup_state(descriptor, durable_next)
    current = RingInfo(103, 103, 100, 0, RECORD_SIZE)
    session = ScriptedRingSession(RingStatus(0, 0, 0, 1))

    async def info(_session: object) -> RingInfo:
        return current

    try:
        disposition, _ = await reconciler.connected_step(
            session, current, info, SessionPhaseState("read/reconcile", SessionQuality(None, "test"))
        )
        assert disposition == "drained"
        result = reconciler.drained_result()
        assert isinstance(result, CollectionResult)
        assert result.packet_count == 1
        assert [event.get("event") for event in operational_events] == ["loss_detected"]
        assert all(event != "quality_metrics_write_error" for event, _error, _fields in runtime.debug_errors)
        assert len(tuple(store.capture_root.glob("100-101-*"))) == 1
    finally:
        await _close_real_writers(runtime)
        await session.close()


def _reconciler(store: StagingStore, descriptor: AttemptDescriptor, durable_next: int) -> BatchReconciler:
    async def quarantine(_attempt_id: str) -> None:
        return None

    reconciler = BatchReconciler(
        store,
        OpportunisticOptions(
            timeouts=_options().timeouts,
            policy=RetryPolicy(backoff=(0.001,), batch_records=2, stop_after_drained=True),
        ),
        OpportunisticRuntime(),
        quarantine,
    )
    reconciler.set_startup_state(descriptor, durable_next)
    return reconciler


def test_restart_interrupted_preserves_unpublished_partial_and_clears_visit(tmp_path: Path) -> None:
    store, attempt_path, raw_before, checkpoint_before = _seed_partial(tmp_path / "spool", tmp_path / "captures")
    descriptor = store.pending_attempts()[0]
    durable_next = store.open_attempt(descriptor.attempt_id).recover().valid_records + descriptor.start_sequence
    reconciler = _reconciler(store, descriptor, durable_next)

    assert reconciler.pending_descriptor == descriptor
    assert reconciler.pending_durable_next == 101
    assert reconciler.durable_progress() == 101

    asyncio.run(reconciler.close_visit("restart_interrupted"))

    assert (attempt_path / "records.bin").read_bytes() == raw_before
    assert (attempt_path / "checkpoint.json").read_bytes() == checkpoint_before
    assert not (attempt_path / "prefix-publication.json").exists()
    assert not (attempt_path / "terminal-retired.json").exists()
    assert store.pending_attempts() == (descriptor,)
    assert not store.ready_closures_path.exists()
    assert reconciler.pending_descriptor is None
    assert reconciler.pending_durable_next is None
    assert reconciler.durable_progress() == 0


def test_absence_publishes_authenticated_partial_and_closes_visit(tmp_path: Path) -> None:
    store, attempt_path, _raw_before, _checkpoint_before = _seed_partial(tmp_path / "spool", tmp_path / "captures")
    descriptor = store.pending_attempts()[0]
    durable_next = store.open_attempt(descriptor.attempt_id).recover().valid_records + descriptor.start_sequence
    reconciler = _reconciler(store, descriptor, durable_next)

    asyncio.run(reconciler.close_visit("absence"))

    assert (attempt_path / "prefix-publication.json").is_file()
    assert (attempt_path / "terminal-retired.json").is_file()
    assert store.pending_attempts() == ()
    assert loads(store.ready_closures_path.read_text(encoding="utf-8"))["closures"] == [
        {"next_sequence": 101, "reason": "absence"}
    ]
    assert reconciler.pending_descriptor is None
    assert reconciler.pending_durable_next is None
    assert reconciler.durable_progress() == 0
    assert tuple((tmp_path / "captures").glob("100-101-*"))


@_async_test
async def test_empty_positive_admission_cancellation_creates_no_absence_closure(tmp_path: Path) -> None:
    runtime = _Runtime()
    store, reconciler = _make_reconciler(tmp_path, runtime, _options())
    current = RingInfo(100, 102, 100, 0, RECORD_SIZE)
    read = WriteStep(
        b"\x11" + (100).to_bytes(8, "big") + (2).to_bytes(4, "big"),
        (_wire_begin(100, 2),),
    )
    session = ScriptedRingSession(RingStatus(0, 0, 0, 1), (read, WriteStep(b"\x13")))

    async def info(_session: object) -> RingInfo:
        return current

    task = asyncio.create_task(reconciler.connected_step(session, current, info, SessionPhaseState("read/reconcile")))
    try:
        async with asyncio.timeout(5):
            while not runtime.proxies or not runtime.proxies[0].read_begin_complete.is_set():
                await asyncio.sleep(0)
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        else:
            raise AssertionError("positive admission should still be waiting for DATA")
        await reconciler.close_visit("absence")
        assert reconciler.durable_progress() == 0
        assert not store.ready_closures_path.exists()
        assert not tuple(store.capture_root.iterdir())
        assert not runtime.proxies[0].thread.is_alive()
        with store.device_lock():
            pass
    finally:
        if not task.done():
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        await _close_real_writers(runtime)
        await session.close()


class _WriterProxy:
    """Fault boundary around a real writer; all storage work stays real."""

    def __init__(self, writer: BatchWriterPort) -> None:
        self.writer = writer
        self.seal_fault: BaseException | None = None
        self.prepare_fault: BaseException | None = None
        self.close_fault: BaseException | None = None
        self.failure_fault: BaseException | None = None
        self.checkpoint_fault: BaseException | None = None
        self.late_seal_fault: BaseException | None = None
        self.seal_result_calls = 0
        self.checkpoint_calls = 0
        self.checkpoint_hook: Callable[[], None] | None = None
        self.restore_clock: Callable[[], None] | None = None
        self.read_begin_complete = asyncio.Event()

    @property
    def attempt_id(self) -> str:
        return self.writer.attempt_id

    @property
    def thread(self) -> Thread:
        return self.writer.thread

    @property
    def progress(self) -> WriterProgressShape:
        return self.writer.progress

    @property
    def failure(self) -> BaseException | None:
        return self.failure_fault or self.writer.failure

    @property
    def submitted_high_water(self) -> int:
        return self.writer.submitted_high_water

    @property
    def written_high_water(self) -> int:
        return self.writer.written_high_water

    async def start(self) -> None:
        await self.writer.start()

    async def prepare_leg(self, start_sequence: int, record_count: int) -> DurablePrefixShape:
        if self.prepare_fault is not None:
            raise self.prepare_fault
        return await self.writer.prepare_leg(start_sequence, record_count)

    async def read_begin(self, notice: ReadBeginNotification) -> object:
        result = await self.writer.read_begin(notice)
        self.read_begin_complete.set()
        return result

    async def checkpoint(self) -> DurablePrefixShape:
        self.checkpoint_calls += 1
        if self.checkpoint_fault is not None:
            error, self.checkpoint_fault = self.checkpoint_fault, None
            if self.checkpoint_hook is not None:
                hook, self.checkpoint_hook = self.checkpoint_hook, None
                hook()
            raise error
        return await self.writer.checkpoint()

    async def barrier(self) -> DurablePrefixShape:
        return await self.writer.barrier()

    async def seal(self, done_notice: DoneNotification) -> SealResultShape:
        result = await self.writer.seal(done_notice)
        if self.seal_fault is not None:
            error, self.seal_fault = self.seal_fault, None
            raise error
        return result

    async def await_seal_result(self) -> SealResultShape | None:
        self.seal_result_calls += 1
        if self.seal_result_calls > 1 and self.late_seal_fault is not None:
            raise self.late_seal_fault
        return await self.writer.await_seal_result()

    async def publish_prefix(self) -> SealResultShape | None:
        return await self.writer.publish_prefix()

    async def close(self, *, timeout: float) -> None:
        if self.restore_clock is not None:
            restore, self.restore_clock = self.restore_clock, None
            restore()
        await self.writer.close(timeout=timeout)
        if self.close_fault is not None:
            raise self.close_fault

    def publish(self, high_water: int) -> bool:
        return self.writer.publish(high_water)

    def submit_read_begin(self, notice: ReadBeginNotification) -> object:
        future = cast(asyncio.Future[object], self.writer.submit_read_begin(notice))
        future.add_done_callback(lambda _done: self.read_begin_complete.set())
        return future


class _Runtime(OpportunisticRuntime):
    def __init__(self) -> None:
        self.proxies: list[_WriterProxy] = []
        self.debug_errors: list[tuple[str, BaseException, dict[str, object]]] = []
        self.seal_fault: BaseException | None = None
        self.prepare_fault: BaseException | None = None
        self.close_fault: BaseException | None = None
        self.failure_fault: BaseException | None = None
        self.checkpoint_fault: BaseException | None = None
        self.late_seal_fault: BaseException | None = None
        self.checkpoint_hook: Callable[[], None] | None = None
        self.restore_clock: Callable[[], None] | None = None

    @override
    def make_batch_writer(
        self,
        staging: StagingPort,
        start: int,
        count: int,
        *,
        source_start: int,
        source: memoryview,
        config: WriterConfig,
    ) -> BatchWriterPort:
        proxy = _WriterProxy(
            super().make_batch_writer(staging, start, count, source_start=source_start, source=source, config=config)
        )
        proxy.seal_fault = self.seal_fault
        self.seal_fault = None
        proxy.prepare_fault = self.prepare_fault
        proxy.close_fault = self.close_fault
        proxy.failure_fault = self.failure_fault
        proxy.checkpoint_fault = self.checkpoint_fault
        proxy.late_seal_fault = self.late_seal_fault
        proxy.checkpoint_hook = self.checkpoint_hook
        proxy.restore_clock = self.restore_clock
        self.proxies.append(proxy)
        return proxy

    @override
    def debug_exception(self, event: str, error: BaseException, **fields: object) -> None:
        self.debug_errors.append((event, error, fields))


async def _close_real_writers(runtime: _Runtime) -> None:
    for proxy in runtime.proxies:
        if proxy.thread.is_alive():
            await proxy.writer.close(timeout=DEFAULT_CONFIG.transfer.sync_timeout_seconds)
        assert not proxy.thread.is_alive()


def _wire_info(read: int, write: int) -> bytes:
    return b"\x02" + pack(">QQIQH", read, write, 100, 0, RECORD_SIZE)


def _wire_record(value: int) -> bytes:
    return b"\x03" + _record(value)


def _wire_begin(start: int, count: int) -> bytes:
    return b"\x05" + start.to_bytes(8, "big") + count.to_bytes(4, "big")


def _wire_done(end: int) -> bytes:
    return b"\x04" + b"\x00" + end.to_bytes(8, "big")


def _options(*, advance: bool = True) -> OpportunisticOptions:
    return OpportunisticOptions(
        timeouts=TransferTimeouts(
            info=DEFAULT_CONFIG.transfer.info_timeout_seconds,
            transfer=DEFAULT_CONFIG.transfer.sync_timeout_seconds,
        ),
        policy=RetryPolicy(backoff=(0.001,), batch_records=2, stop_after_drained=True, advance_enabled=advance),
    )


def _real_batch_steps() -> tuple[WriteStep, ...]:
    return (
        WriteStep(
            b"\x11" + (100).to_bytes(8, "big") + (2).to_bytes(4, "big"),
            (_wire_begin(100, 2), _wire_record(100), _wire_record(101), _wire_done(102)),
        ),
    )


@dataclass
class _CadenceRecorder:
    events: list[ProgressEvent]
    first_reported: asyncio.Event
    loop: asyncio.AbstractEventLoop
    original_time: Callable[[], float]
    fake_now: list[float]
    monkeypatch: pytest.MonkeyPatch

    def __call__(self, event: ProgressEvent) -> None:
        self.events.append(event)
        if len(self.events) == 1:
            self.fake_now[0] = self.original_time()
            self.monkeypatch.setattr(self.loop, "time", lambda: self.fake_now[0])
            self.first_reported.set()
        elif len(self.events) == 2:
            self.monkeypatch.setattr(self.loop, "time", self.original_time)


def _make_reconciler(
    tmp_path: Path, runtime: OpportunisticRuntime, options: OpportunisticOptions
) -> tuple[StagingStore, BatchReconciler]:
    store = StagingStore(tmp_path / "spool", tmp_path / "captures")

    async def quarantine(_attempt_id: str) -> None:
        return None

    return store, BatchReconciler(store, options, runtime, quarantine)


@_async_test
async def test_collect_only_adopts_seal_completed_before_ack_timeout(tmp_path: Path) -> None:
    runtime = _Runtime()
    _store, reconciler = _make_reconciler(tmp_path, runtime, _options(advance=False))
    session = ScriptedRingSession(RingStatus(0, 0, 0, 1), _real_batch_steps())
    current = RingInfo(100, 102, 100, 0, RECORD_SIZE)

    async def info(_session: object) -> RingInfo:
        return current

    try:
        # Let seal publish the real bundle, then model a lost bounded ACK.
        runtime.seal_fault = CollectorTimeoutError("simulated lost seal acknowledgement")
        try:
            await reconciler.connected_step(session, current, info, SessionPhaseState("read/reconcile"))
        except CollectorTimeoutError:
            pass
        else:
            raise AssertionError("expected bounded seal acknowledgement timeout")
        runtime_writer = runtime.proxies[0]
        await reconciler.checkpoint_after_session()
        adopted = await runtime_writer.await_seal_result()
        assert adopted is not None
        result, seen = await reconciler.connected_step(session, current, info, SessionPhaseState("read/reconcile"))
        assert (result, seen) == ("collected", current)
        collected = reconciler.drained_result()
        assert isinstance(collected, CollectionResult)
        assert collected.advance_confirmed is False
        assert all(command[0] != 0x12 for command in session.writes)
        assert adopted.bundle_path.is_dir()
        assert (adopted.bundle_path / "records.bin").read_bytes() == _record(100) + _record(101)
        assert not runtime_writer.thread.is_alive()
    finally:
        await _close_real_writers(runtime)
        await session.close()


@_async_test
async def test_fresh_info_regression_keeps_sealed_batch_without_advance(tmp_path: Path) -> None:
    runtime = _Runtime()
    _store, reconciler = _make_reconciler(tmp_path, runtime, _options())
    session = ScriptedRingSession(RingStatus(0, 0, 0, 1), _real_batch_steps())
    current = RingInfo(100, 102, 100, 0, RECORD_SIZE)
    fresh = RingInfo(99, 102, 100, 0, RECORD_SIZE)
    calls = 0

    async def info(_session: object) -> RingInfo:
        nonlocal calls
        calls += 1
        return fresh

    try:
        disposition, _ = await reconciler.connected_step(session, current, info, SessionPhaseState("read/reconcile"))
        assert disposition is None
        result = reconciler.drained_result()
        assert isinstance(result, CollectionResult)
        assert result.advance_confirmed is False
        assert result.next_sequence == 102
        assert all(command[0] != 0x12 for command in session.writes)
        assert calls == 1
        assert len(runtime.proxies) == 1
        assert not runtime.proxies[0].thread.is_alive()
        seal = result.seal
        assert isinstance(seal, SealResult)
        assert seal.bundle_path.is_dir()
        assert (seal.bundle_path / "records.bin").read_bytes() == _record(100) + _record(101)
    finally:
        await _close_real_writers(runtime)
        await session.close()


@_async_test
async def test_acknowledged_advance_with_old_cursor_repeats_without_reread(tmp_path: Path) -> None:
    runtime = _Runtime()
    _store, reconciler = _make_reconciler(tmp_path, runtime, _options())
    end = 102
    current = RingInfo(100, end, 100, 0, RECORD_SIZE)
    old_info = RingInfo(100, end, 100, 0, RECORD_SIZE)
    confirmed_info = RingInfo(end, end, 100, 0, RECORD_SIZE)
    info_values = iter((old_info, old_info, old_info, confirmed_info))

    async def info(_session: object) -> RingInfo:
        return next(info_values)

    session = ScriptedRingSession(
        RingStatus(0, 0, 0, 1),
        (
            _real_batch_steps()[0],
            WriteStep(b"\x12" + end.to_bytes(8, "big"), (b"\x01\x00",)),
            WriteStep(b"\x12" + end.to_bytes(8, "big"), (b"\x01\x00",)),
        ),
    )
    try:
        first = await reconciler.connected_step(session, current, info, SessionPhaseState("read/reconcile"))
        assert first == (None, old_info)
        assert reconciler.completed_batches == 0
        second = await reconciler.connected_step(session, old_info, info, SessionPhaseState("read/reconcile"))
        assert second == (None, confirmed_info)
        result = reconciler.drained_result()
        assert isinstance(result, CollectionResult)
        assert result.advance_confirmed is True
        assert sum(command[0] == 0x11 for command in session.writes) == 1
        assert sum(command[0] == 0x12 for command in session.writes) == 2
        assert len(runtime.proxies) == 1
        assert not runtime.proxies[0].thread.is_alive()
    finally:
        await _close_real_writers(runtime)
        await session.close()


@_async_test
async def test_connected_read_emits_second_nonterminal_progress_after_cadence(tmp_path: Path) -> None:
    runtime = _Runtime()
    events: list[ProgressEvent] = []
    config = replace(
        DEFAULT_CONFIG,
        transfer=replace(DEFAULT_CONFIG.transfer, progress_interval_seconds=0.02),
    )
    options = replace(
        _options(advance=False),
        policy=replace(_options(advance=False).policy, batch_records=3),
        config=config,
        progress=events.append,
    )
    _store, reconciler = _make_reconciler(tmp_path, runtime, options)
    current = RingInfo(100, 103, 100, 0, RECORD_SIZE)
    end = 103
    read = WriteStep(
        b"\x11" + (100).to_bytes(8, "big") + (3).to_bytes(4, "big"),
        (
            _wire_begin(100, 3),
            _wire_record(100),
            DelayedNotification(0.05, _wire_record(101)),
            DelayedNotification(0.05, _wire_record(102)),
            DelayedNotification(0.05, _wire_done(end)),
        ),
    )
    session = ScriptedRingSession(RingStatus(0, 0, 0, 1), (read,))

    async def info(_session: object) -> RingInfo:
        return current

    try:
        disposition, _ = await reconciler.connected_step(session, current, info, SessionPhaseState("read/reconcile"))
        assert disposition == "collected"
        assert len(events) >= 3
        nonterminal = [event for event in events if event.records_completed < event.records_total]
        assert [event.records_completed for event in nonterminal] == [1, 2]
        assert events[-1].records_completed == 3
        assert not runtime.proxies[0].thread.is_alive()
    finally:
        await _close_real_writers(runtime)
        await session.close()


@pytest.mark.parametrize("overshoot", [0.0, 0.01], ids=["exact-cadence", "past-cadence"])
@_async_test
async def test_progress_due_snapshot_precedes_competing_data(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, overshoot: float
) -> None:
    runtime = _Runtime()
    interval = 0.02
    events: list[ProgressEvent] = []
    first_reported = asyncio.Event()
    loop = asyncio.get_running_loop()
    original_time = loop.time
    fake_now = [original_time()]

    progress = _CadenceRecorder(events, first_reported, loop, original_time, fake_now, monkeypatch)

    options = replace(
        _options(advance=False),
        policy=replace(_options(advance=False).policy, batch_records=4),
        config=replace(DEFAULT_CONFIG, transfer=replace(DEFAULT_CONFIG.transfer, progress_interval_seconds=interval)),
        progress=progress,
    )
    _store, reconciler = _make_reconciler(tmp_path, runtime, options)

    class InterleavedSession:
        def __init__(self) -> None:
            self.writes: list[bytes] = []
            self.closed = False

        async def read_status(self) -> RingStatus:
            return RingStatus(0, 0, 0, 1)

        def notifications(self) -> AsyncIterator[bytes]:
            return self._notifications()

        async def write_control(self, payload: bytes) -> None:
            expected = b"\x11" + (100).to_bytes(8, "big") + (4).to_bytes(4, "big")
            assert payload == expected
            self.writes.append(payload)

        async def close(self) -> None:
            self.closed = True

        async def _notifications(self) -> AsyncIterator[bytes]:
            yield _wire_begin(100, 4)
            yield _wire_record(100)
            await first_reported.wait()
            fake_now[0] += interval + overshoot
            yield _wire_record(101)
            await asyncio.sleep(0)
            yield _wire_record(102)
            await asyncio.sleep(0)
            yield _wire_record(103)
            await asyncio.sleep(0)
            yield _wire_done(104)

    session = InterleavedSession()
    current = RingInfo(100, 104, 100, 0, RECORD_SIZE)

    async def info(_session: object) -> RingInfo:
        return current

    monkeypatch.setattr(loop, "time", lambda: fake_now[0])
    try:
        disposition, _ = await reconciler.connected_step(session, current, info, SessionPhaseState("read/reconcile"))
        assert disposition == "collected"
        assert [event.records_completed for event in events[:2]] == [1, 2]
        assert events[-1].records_completed == 4
        assert not runtime.proxies[0].thread.is_alive()
    finally:
        monkeypatch.setattr(loop, "time", original_time)
        await _close_real_writers(runtime)
        await session.close()


@_async_test
async def test_read_failure_preserves_transfer_error_while_progress_callback_is_pending(tmp_path: Path) -> None:
    runtime = _Runtime()
    callback_started = asyncio.Event()
    release_callback = asyncio.Event()

    async def hold_progress(_event: ProgressEvent) -> None:
        callback_started.set()
        await release_callback.wait()

    options = replace(_options(advance=False), progress=hold_progress)
    _store, reconciler = _make_reconciler(tmp_path, runtime, options)
    current = RingInfo(100, 102, 100, 0, RECORD_SIZE)
    session = ScriptedRingSession(
        RingStatus(0, 0, 0, 1),
        (
            WriteStep(
                b"\x11" + (100).to_bytes(8, "big") + (2).to_bytes(4, "big"),
                (_wire_begin(100, 2), _wire_record(100)),
            ),
        ),
    )

    async def info(_session: object) -> RingInfo:
        return current

    task = asyncio.create_task(reconciler.connected_step(session, current, info, SessionPhaseState("read/reconcile")))
    try:
        await asyncio.wait_for(callback_started.wait(), timeout=5)
        session.emit(b"\xff")
        with pytest.raises(TransferInterruptedError) as caught:
            await task
        error = caught.value
        assert isinstance(error.__cause__, RingTransferError)
        assert str(error.__cause__) == "unexpected notification during READ"
        assert error.received_records == 1
    finally:
        release_callback.set()
        if not task.done():
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        await _close_real_writers(runtime)
        await session.close()


@pytest.mark.parametrize(
    ("primary_kind", "close_error", "expected_kind"),
    [
        ("ordinary", WriterError("recognized close failure"), "primary"),
        ("writer", TimeoutError("recognized close timeout"), "cause"),
        ("ordinary", RuntimeError("unexpected close failure"), "close"),
        ("ordinary", WriterFailedError("recognized writer-failed close"), "primary"),
    ],
)
@_async_test
async def test_admission_error_precedence_closes_real_writer(
    tmp_path: Path, primary_kind: str, close_error: BaseException, expected_kind: str
) -> None:
    runtime = _Runtime()
    cause = StagingError("writer target failed")
    ordinary_primary = CursorConsistencyError("prepare rejected by test boundary")
    runtime.prepare_fault = WriterFailedError("writer target failed") if primary_kind == "writer" else ordinary_primary
    runtime.failure_fault = cause if primary_kind == "writer" else None
    runtime.close_fault = close_error
    _store, reconciler = _make_reconciler(tmp_path, runtime, _options())
    current = RingInfo(100, 102, 100, 0, RECORD_SIZE)
    session = ScriptedRingSession(RingStatus(0, 0, 0, 1))

    async def info(_session: object) -> RingInfo:
        return current

    try:
        try:
            await reconciler.connected_step(session, current, info, SessionPhaseState("read/reconcile"))
        except (RuntimeError, TimeoutError) as error:
            if expected_kind == "primary":
                assert error is ordinary_primary
            elif expected_kind == "cause":
                assert error is cause
            else:
                assert error is close_error
        else:
            raise AssertionError("expected admission failure")
        assert len(runtime.proxies) == 1
        assert not runtime.proxies[0].thread.is_alive()
    finally:
        await _close_real_writers(runtime)
        await session.close()


async def _admit_partial_and_cancel(reconciler: BatchReconciler, runtime: _Runtime) -> ScriptedRingSession:
    read = WriteStep(
        b"\x11" + (100).to_bytes(8, "big") + (2).to_bytes(4, "big"),
        (_wire_begin(100, 2), _wire_record(100)),
    )
    session = ScriptedRingSession(RingStatus(0, 0, 0, 1), (read, WriteStep(b"\x13")))
    current = RingInfo(100, 102, 100, 0, RECORD_SIZE)

    async def info(_session: object) -> RingInfo:
        return current

    task = asyncio.create_task(reconciler.connected_step(session, current, info, SessionPhaseState("read/reconcile")))
    try:
        async with asyncio.timeout(5):
            while not runtime.proxies or runtime.proxies[0].progress.submitted == 0:
                if task.done():
                    await task
                    raise AssertionError("connected collection completed before writer submission")
                await asyncio.sleep(0)
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            return session
        raise AssertionError("connected collection should remain active until cancelled")
    except BaseException:
        if not task.done():
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        await _close_real_writers(runtime)
        await session.close()
        raise


@pytest.mark.parametrize("late_fault", [None, OSError("late seal lookup failed")], ids=["no-seal", "late-error"])
@_async_test
async def test_finalize_retains_first_checkpoint_error_when_late_seal_is_absent(
    tmp_path: Path, late_fault: BaseException | None
) -> None:
    runtime = _Runtime()
    first = OSError("checkpoint failed")
    runtime.checkpoint_fault = first
    runtime.late_seal_fault = late_fault
    _store, reconciler = _make_reconciler(tmp_path, runtime, _options())
    session = await _admit_partial_and_cancel(reconciler, runtime)
    try:
        result = await reconciler.finalize_active()
        assert result.checkpoint_error is first
        assert result.close_error is None
        assert result.preserved_kind == "partial collection"
        assert not runtime.proxies[0].thread.is_alive()
    finally:
        await _close_real_writers(runtime)
        await session.close()


@pytest.mark.parametrize(
    "late_error",
    [
        pytest.param(StagingError("late staging failure"), id="staging-only"),
        pytest.param(OSError("late storage failure"), id="storage"),
        pytest.param(WriterError("late writer failure"), id="writer-only"),
    ],
)
@_async_test
async def test_finalize_adopts_recognized_late_error_after_close(tmp_path: Path, late_error: BaseException) -> None:
    runtime = _Runtime()
    runtime.late_seal_fault = late_error
    _store, reconciler = _make_reconciler(tmp_path, runtime, _options())
    session = await _admit_partial_and_cancel(reconciler, runtime)
    try:
        result = await reconciler.finalize_active()
        assert result.checkpoint_error is late_error
        assert result.close_error is None
        assert result.preserved_kind == "partial collection"
        assert not runtime.proxies[0].thread.is_alive()
    finally:
        await _close_real_writers(runtime)
        await session.close()


@_async_test
async def test_finalize_adopts_late_collector_timeout_after_close(tmp_path: Path) -> None:
    runtime = _Runtime()
    late = CollectorTimeoutError("late bounded seal lookup timed out")
    runtime.late_seal_fault = late
    _store, reconciler = _make_reconciler(tmp_path, runtime, _options())
    session = await _admit_partial_and_cancel(reconciler, runtime)
    try:
        result = await reconciler.finalize_active()
        assert result.checkpoint_error is late
        assert result.close_error is None
        assert not runtime.proxies[0].thread.is_alive()
    finally:
        await _close_real_writers(runtime)
        await session.close()


@_async_test
async def test_finalize_propagates_unexpected_late_error_after_releasing_writer(tmp_path: Path) -> None:
    runtime = _Runtime()
    unexpected = RuntimeError("unexpected late seal result error")
    runtime.late_seal_fault = unexpected
    _store, reconciler = _make_reconciler(tmp_path, runtime, _options())
    session = await _admit_partial_and_cancel(reconciler, runtime)
    try:
        try:
            await reconciler.finalize_active()
        except RuntimeError as error:
            assert error is unexpected
        else:
            raise AssertionError("unexpected late error should propagate")
        assert not runtime.proxies[0].thread.is_alive()
    finally:
        await _close_real_writers(runtime)
        await session.close()


@_async_test
async def test_finalize_does_not_checkpoint_empty_positive_admission(tmp_path: Path) -> None:
    runtime = _Runtime()
    store, reconciler = _make_reconciler(tmp_path, runtime, _options())
    current = RingInfo(100, 102, 100, 0, RECORD_SIZE)
    session = ScriptedRingSession(
        RingStatus(0, 0, 0, 1),
        (
            WriteStep(b"\x11" + (100).to_bytes(8, "big") + (2).to_bytes(4, "big"), (_wire_begin(100, 2),)),
            WriteStep(b"\x13"),
        ),
    )

    async def info(_session: object) -> RingInfo:
        return current

    task = asyncio.create_task(reconciler.connected_step(session, current, info, SessionPhaseState("read/reconcile")))
    try:
        async with asyncio.timeout(5):
            while not runtime.proxies or not runtime.proxies[0].read_begin_complete.is_set():
                await asyncio.sleep(0)
        task.cancel()
        with suppress(asyncio.CancelledError):
            await task
        result = await reconciler.finalize_active()
        proxy = runtime.proxies[0]
        assert result.checkpoint_error is None
        assert result.close_error is None
        assert proxy.checkpoint_calls == 0
        assert proxy.seal_result_calls == 2
        assert not store.ready_closures_path.exists()
    finally:
        if not task.done():
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        await _close_real_writers(runtime)
        await session.close()


@_async_test
async def test_finalize_sealed_batch_skips_checkpoint_and_second_seal_adoption(tmp_path: Path) -> None:
    runtime = _Runtime()
    close_error = OSError("close failed after seal")
    runtime.seal_fault = CollectorTimeoutError("lost seal acknowledgement")
    runtime.close_fault = close_error
    runtime.late_seal_fault = RuntimeError("must not adopt after failed close")
    _store, reconciler = _make_reconciler(tmp_path, runtime, _options())
    current = RingInfo(100, 102, 100, 0, RECORD_SIZE)
    session = ScriptedRingSession(RingStatus(0, 0, 0, 1), _real_batch_steps())

    async def info(_session: object) -> RingInfo:
        return current

    try:
        with pytest.raises(CollectorTimeoutError):
            await reconciler.connected_step(session, current, info, SessionPhaseState("read/reconcile"))
        await reconciler.checkpoint_after_session()
        proxy = runtime.proxies[0]
        checkpoint_calls = proxy.checkpoint_calls
        result = await reconciler.finalize_active()
        assert result.checkpoint_error is None
        assert result.close_error is close_error
        assert result.preserved_kind == "sealed bundle"
        assert proxy.checkpoint_calls == checkpoint_calls
        assert proxy.seal_result_calls == 1
        assert not proxy.thread.is_alive()
    finally:
        await _close_real_writers(runtime)
        await session.close()


@pytest.mark.parametrize(
    "close_error",
    [OSError("close failed"), CollectorTimeoutError("close timed out"), WriterError("writer close failed")],
    ids=["oserror", "timeout", "writer"],
)
@_async_test
async def test_failed_close_does_not_adopt_seal_again(tmp_path: Path, close_error: BaseException) -> None:
    runtime = _Runtime()
    checkpoint_error = OSError("checkpoint failed")
    runtime.checkpoint_fault = checkpoint_error
    runtime.close_fault = close_error
    runtime.late_seal_fault = RuntimeError("late adoption is forbidden after failed close")
    _store, reconciler = _make_reconciler(tmp_path, runtime, _options())
    session = await _admit_partial_and_cancel(reconciler, runtime)
    try:
        result = await reconciler.finalize_active()
        proxy = runtime.proxies[0]
        assert result.checkpoint_error is checkpoint_error
        assert result.close_error is close_error
        assert proxy.seal_result_calls == 1
        assert not proxy.thread.is_alive()
    finally:
        await _close_real_writers(runtime)
        await session.close()


@_async_test
async def test_checkpoint_after_session_times_out_at_exact_writer_deadline(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    runtime = _Runtime()
    timeout = 1.0
    sleep_calls: list[float] = []

    async def forbidden_sleep(delay: float) -> None:
        sleep_calls.append(delay)
        raise AssertionError("checkpoint retried after its exact deadline")

    options = replace(_options(), timeouts=TransferTimeouts(1, timeout), sleep=forbidden_sleep)
    _store, reconciler = _make_reconciler(tmp_path, runtime, options)
    session = await _admit_partial_and_cancel(reconciler, runtime)
    loop = asyncio.get_running_loop()
    original_time = loop.time
    fake_now = [original_time()]
    proxy = runtime.proxies[0]
    not_ready = WriterError("READ_BEGIN not ready")
    proxy.checkpoint_fault = not_ready
    proxy.checkpoint_hook = lambda: fake_now.__setitem__(0, fake_now[0] + timeout)
    monkeypatch.setattr(loop, "time", lambda: fake_now[0])
    try:
        with pytest.raises(CollectorTimeoutError) as caught:
            await reconciler.checkpoint_after_session()
        assert caught.value.__cause__ is not_ready
        assert sleep_calls == []
        assert proxy.checkpoint_calls == 1
        assert proxy.thread.is_alive()
    finally:
        monkeypatch.setattr(loop, "time", original_time)
        await _close_real_writers(runtime)
        await session.close()


@_async_test
async def test_finalize_times_out_at_exact_writer_deadline_without_retry(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    runtime = _Runtime()
    timeout = 1.0
    sleep_calls: list[float] = []

    async def forbidden_sleep(delay: float) -> None:
        sleep_calls.append(delay)
        raise AssertionError("final checkpoint retried after its exact deadline")

    options = replace(_options(), timeouts=TransferTimeouts(1, timeout), sleep=forbidden_sleep)
    _store, reconciler = _make_reconciler(tmp_path, runtime, options)
    session = await _admit_partial_and_cancel(reconciler, runtime)
    loop = asyncio.get_running_loop()
    original_time = loop.time
    fake_now = [original_time()]
    proxy = runtime.proxies[0]
    not_ready = WriterError("READ_BEGIN not ready")
    proxy.checkpoint_fault = not_ready
    proxy.checkpoint_hook = lambda: fake_now.__setitem__(0, fake_now[0] + timeout)
    proxy.restore_clock = lambda: monkeypatch.setattr(loop, "time", original_time)
    monkeypatch.setattr(loop, "time", lambda: fake_now[0])
    try:
        result = await reconciler.finalize_active()
        assert isinstance(result.checkpoint_error, CollectorTimeoutError)
        assert result.checkpoint_error.__cause__ is not_ready
        assert result.close_error is None
        assert sleep_calls == []
        assert not proxy.thread.is_alive()
    finally:
        monkeypatch.setattr(loop, "time", original_time)
        await _close_real_writers(runtime)
        await session.close()
