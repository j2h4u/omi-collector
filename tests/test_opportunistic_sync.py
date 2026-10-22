from __future__ import annotations

import asyncio
import sqlite3
import threading
import time
from collections.abc import AsyncIterator, Awaitable, Callable, Mapping
from contextlib import AbstractAsyncContextManager, asynccontextmanager, closing
from dataclasses import FrozenInstanceError, replace
from datetime import UTC, datetime
from functools import wraps
from hashlib import sha256
from pathlib import Path
from shutil import rmtree
from struct import pack
from types import SimpleNamespace
from typing import ParamSpec, TypeVar, cast

import pytest
from bleak.backends.device import BLEDevice
from bleak.exc import BleakDBusError, BleakError

from fakes import DelayedNotification, ScriptedRingSession, WriteStep
from omi_collector.capture.adapters import internal_audio_store, opportunistic_runtime, ready_closures
from omi_collector.capture.adapters.attempt_writer import AttemptWriter, WriterFailedError, WriterProgress
from omi_collector.capture.adapters.attempts import DurableSqliteSeal
from omi_collector.capture.adapters.clock_state_store import ClockStateStore
from omi_collector.capture.adapters.collector_state_db import StateDB, StateKind
from omi_collector.capture.adapters.confirmed_loss import read_confirmed_losses
from omi_collector.capture.adapters.internal_audio_store import (
    AUDIO_RECORD_SIZE,
    CaptureSource,
    InternalAudioStore,
    read_capture_source,
)
from omi_collector.capture.adapters.opportunistic_runtime import OpportunisticRuntime
from omi_collector.capture.adapters.publication import SealResult
from omi_collector.capture.adapters.quality_metrics import SQLiteQualityMetrics, read_quality_events
from omi_collector.capture.adapters.quarantine import QuarantineSalvageDeferredError, SweepResult
from omi_collector.capture.adapters.ready_bundles import ReadyOutcome, ReadyOutcomeState
from omi_collector.capture.adapters.staging_contract import (
    AttemptDescriptor,
    DeviceAlreadyRunningError,
    DiskSpaceError,
    DurablePrefix,
    LockContext,
    StagingError,
)
from omi_collector.capture.adapters.staging_filesystem import StagingIO
from omi_collector.capture.adapters.staging_writer import StagingWriter
from omi_collector.capture.application import batch_reconciliation, session_lifecycle
from omi_collector.capture.application.batch_reconciliation import (
    BatchReconciler,
    BatchUnavailableError,
    CursorConsistencyError,
)
from omi_collector.capture.application.collector import (
    CollectionResult,
    NoDataResult,
    ProgressEvent,
    ProgressMailbox,
    ProgressSnapshot,
    RingAcknowledgementError,
    TransferCounters,
    TransferInterruptedError,
    TransferTimeouts,
)
from omi_collector.capture.application.operational_telemetry import (
    BATTERY_UUID,
    TIME_READ_UUID,
)
from omi_collector.capture.application.opportunistic_sync import (
    CollectionPreservedCancelledError,
    _PublicDrainCoordinator,
)
from omi_collector.capture.application.opportunistic_sync import (
    run_opportunistic_collector as _run_opportunistic_collector,
)
from omi_collector.capture.application.ports import (
    BatchWriterPort,
    CaptureRuntimePort,
    StagingPort,
    StorageLeaseContext,
)
from omi_collector.capture.application.presence import (
    PresenceCallback,
    PresencePolicy,
    PresenceScheduler,
    PresenceWake,
)
from omi_collector.capture.application.presence_machine import AttemptOutcome
from omi_collector.capture.application.quarantine_maintenance import QuarantineMaintenance
from omi_collector.capture.application.ring_transport import (
    CandidateUnavailableError,
    NotificationOverflowError,
    RingSession,
    RingTransportDisconnectedError,
    RingTransportUnavailableError,
)
from omi_collector.capture.application.session_lifecycle import (
    ActivityEvent,
    OpportunisticOptions,
    RetryPolicy,
    SessionLifecycle,
    SessionPhaseState,
    report_session_error,
    session_retry_outcome,
    validate_presence_policy,
)
from omi_collector.capture.application.storage_fault import FatalInternalAudioStorageError
from omi_collector.capture.domain.ring_protocol import (
    RECORD_SIZE,
    STATUS_STORAGE_NOT_READY,
    DoneNotification,
    ReadBeginNotification,
    RingInfo,
    RingStatus,
    encode_advance_command,
    encode_read_command,
    encode_stop_command,
)
from omi_collector.capture.domain.transfer_arena import TransferArena
from omi_collector.config import (
    DEFAULT_CONFIG,
    CollectorConfig,
    ReadyConfig,
    RetryConfig,
    StagingRetentionConfig,
    TelemetryConfig,
)
from staging_test_factory import StagingStore, storage_call

P = ParamSpec("P")
T = TypeVar("T")


async def _finish_task[T](task: asyncio.Task[T], *, cancel: bool = False) -> None:
    if cancel:
        task.cancel()
    done, pending = await asyncio.wait({task}, timeout=5.0)
    assert task in done and not pending
    await asyncio.gather(task, return_exceptions=True)


def _runtime() -> OpportunisticRuntime:
    """Build the concrete runtime explicitly for coordinator tests."""
    return OpportunisticRuntime()


async def run_opportunistic_collector(
    provider: Callable[[object | None], AbstractAsyncContextManager[RingSession]],
    staging: StagingStore,
    options: OpportunisticOptions,
) -> CollectionResult | NoDataResult:
    """Test helper that injects the concrete runtime at every call site."""
    return await _run_opportunistic_collector(provider, staging, options, runtime=_runtime())


def _async_test[**P, T](function: Callable[P, Awaitable[T]]) -> Callable[P, T]:
    @wraps(function)
    def wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
        return asyncio.run(function(*args, **kwargs))

    return wrapper


def _record(value: int) -> bytes:
    return pack(">I", value) + bytes((value % 256,)) * (RECORD_SIZE - 4)


def _records(start: int, count: int) -> bytes:
    return b"".join(_record(start + index) for index in range(count))


_CAPTURE_ROOTS: set[Path] = set()


def _capture_root(tmp_path: Path) -> Path:
    root = tmp_path.parent / f"{tmp_path.name}-captures"
    if tmp_path not in _CAPTURE_ROOTS:
        rmtree(root, ignore_errors=True)
        _CAPTURE_ROOTS.add(tmp_path)
    return root


_test_state_databases: dict[Path, StateDB] = {}


def _staging_store(
    spool: Path,
    capture_root: Path,
    *,
    config: CollectorConfig = DEFAULT_CONFIG,
) -> StagingStore:
    """Build staging fixtures with their required state database."""
    spool = Path(spool)
    spool.mkdir(parents=True, exist_ok=True)
    state_db = _test_state_databases.get(spool)
    if state_db is None or not state_db.path.is_file():
        state_db = storage_call(lambda: StateDB(spool / "collector-state.sqlite3"))
        _test_state_databases[spool] = state_db
    return StagingStore(spool, capture_root, state_db=state_db, io=StagingIO(config=config))


def _quality_metrics(state_db: StateDB) -> SQLiteQualityMetrics:
    return SQLiteQualityMetrics(state_db, release_version="test-version", source_revision="a" * 40)


def _quality_events(journal: SQLiteQualityMetrics, state_db: StateDB) -> tuple[dict[str, object], ...]:
    def read() -> tuple[dict[str, object], ...]:
        assert journal.close()
        return read_quality_events(state_db, datetime.min.replace(tzinfo=UTC), datetime.max.replace(tzinfo=UTC))

    return storage_call(read)


def _ready_closure_rows(store: StagingStore) -> list[dict[str, object]]:
    return storage_call(
        lambda: [
            {"next_sequence": closure.next_sequence, "reason": closure.reason}
            for closure in ready_closures.load(store.state_db)
        ]
    )


def _attempt_source(store: StagingStore, source_id: str) -> CaptureSource | None:
    return storage_call(lambda: read_capture_source(store.state_db, source_id))


def _sealed_records(store: StagingStore, seal: DurableSqliteSeal) -> bytes:
    return _source_records(store, seal.source_id)


def _source_records(store: StagingStore, source_id: str) -> bytes:
    def read() -> bytes:
        with InternalAudioStore(store.state_db) as audio_store:
            payloads: list[bytes] = []
            after_ordinal = -1
            while records := audio_store.read_record_page(source_id, after_ordinal):
                payloads.extend(record.raw_bytes for record in records)
                after_ordinal = records[-1].ordinal
            return b"".join(payloads)

    return storage_call(read)


@pytest.fixture(autouse=True)
def _isolate_capture_root(tmp_path: Path) -> None:
    rmtree(_capture_root(tmp_path), ignore_errors=True)


def _status() -> RingStatus:
    return RingStatus(0, 0, 0, 1)


def _info(read: int, write: int, dropped: int = 0) -> bytes:
    return b"\x02" + pack(">QQIQH", read, write, 10000, dropped, RECORD_SIZE)


def _patch_read_begin_completion(monkeypatch: pytest.MonkeyPatch) -> threading.Event:
    read_begin_completed = threading.Event()
    original_submit_read_begin = AttemptWriter.submit_read_begin

    def observed_submit_read_begin(self: AttemptWriter, notice: object) -> asyncio.Future[object]:
        future = original_submit_read_begin(self, notice)
        future.add_done_callback(lambda _: read_begin_completed.set())
        return future

    monkeypatch.setattr(
        "omi_collector.capture.adapters.attempt_writer.AttemptWriter.submit_read_begin",
        observed_submit_read_begin,
    )
    return read_begin_completed


def _begin(start: int, count: int) -> bytes:
    return b"\x05" + start.to_bytes(8, "big") + count.to_bytes(4, "big")


def _data(data: bytes) -> bytes:
    return b"\x03" + data


@_async_test
async def test_session_error_includes_explicit_bleak_bluez_cause_chain() -> None:
    bluez = BleakDBusError("org.bluez.Error.Failed", ["Connection attempt failed"])
    bleak = BleakError("backend lost connection")
    bleak.__cause__ = bluez
    transport = RingTransportDisconnectedError("Omi disconnected during ring write")
    transport.__cause__ = bleak
    activity: list[ActivityEvent] = []

    await report_session_error(activity.append, "read/reconcile", transport, _runtime())

    error = activity[0]
    assert error.error_type == "RingTransportDisconnectedError"
    assert error.error_message == (
        "RingTransportDisconnectedError: Omi disconnected during ring write"
        " <- BleakError: backend lost connection"
        " <- BleakDBusError: [org.bluez.Error.Failed] Connection attempt failed"
    )


@_async_test
async def test_session_error_redacts_ble_address_without_hiding_transport_cause() -> None:
    bleak = BleakError("Device with address AA:BB:CC:DD:EE:FF at /org/bluez/hci0/dev_AA_BB_CC_DD_EE_FF was not found")
    transport = RingTransportUnavailableError("connection failed")
    transport.__cause__ = bleak
    activity: list[ActivityEvent] = []

    await report_session_error(activity.append, "connect", transport, _runtime())

    message = activity[0].error_message
    assert message is not None
    assert "AA:BB:CC:DD:EE:FF" not in message
    assert "dev_AA_BB_CC_DD_EE_FF" not in message
    assert message.count("[BLE address]") == 2
    assert "BleakError" in message


@_async_test
async def test_session_error_keeps_raw_exception_for_debug_ring(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: list[tuple[str, BaseException, dict[str, object]]] = []
    raw_error = RuntimeError("unredacted diagnostic cause")

    def record(_runtime_instance: OpportunisticRuntime, event: str, error: BaseException, **fields: object) -> None:
        captured.append((event, error, fields))

    monkeypatch.setattr(OpportunisticRuntime, "debug_exception", record)
    await report_session_error(None, "connect", raw_error, _runtime())

    assert captured == [("session_error", raw_error, {"phase": "connect"})]


@_async_test
async def test_session_error_serializes_lock_context_in_normal_progress_line() -> None:
    context = LockContext("capture_batch", "capture_batch", 321, 654, 1.25, "other_process", "valid")
    activity: list[ActivityEvent] = []
    await report_session_error(
        activity.append,
        "connect",
        DeviceAlreadyRunningError(lock_context=context),
        _runtime(),
    )

    from omi_collector.capture import cli as device_cli

    updates: list[object] = []
    callback = device_cli._activity_callback(updates.append)
    assert callback is not None
    await cast(Awaitable[None], callback(activity[0]))
    progress = cast(device_cli.DownloadProgress, updates[0])
    progress = progress.as_dict()
    assert progress["status"] == "session_error"
    assert progress["lock_context"] == context.as_dict()


def _done(next_sequence: int) -> bytes:
    return b"\x04\x00" + next_sequence.to_bytes(8, "big")


def _real_ring_gap_session(cursor: int, write: int) -> ScriptedRingSession:
    continuation = (
        (cursor, 4096),
        (cursor + 4096, 4096),
        (cursor + 8192, write - cursor - 8192),
    )
    steps: list[WriteStep] = [WriteStep(b"\x10", (_info(cursor, write),))]
    steps.append(WriteStep(b"\x10", (_info(cursor, write),)))
    for leg_start, leg_count in continuation:
        leg_end = leg_start + leg_count
        steps.extend(
            [
                WriteStep(
                    encode_read_command(leg_start, leg_count),
                    (_begin(leg_start, leg_count), _data(_records(leg_start, leg_count)), _done(leg_end)),
                ),
                WriteStep(b"\x10", (_info(leg_start, write),)),
                WriteStep(encode_advance_command(leg_end), (b"\x01\x00",)),
                WriteStep(b"\x10", (_info(leg_end, write),)),
            ]
        )
    return ScriptedRingSession(_status(), tuple(steps))


class Provider:
    def __init__(self, sessions: list[ScriptedRingSession]) -> None:
        self.sessions = sessions
        self.opened = 0

    def __call__(self, _candidate: object | None = None):
        session = self.sessions.pop(0)

        @asynccontextmanager
        async def context() -> AsyncIterator[ScriptedRingSession]:
            self.opened += 1
            yield session

        return context()


class Clock:
    def __init__(self) -> None:
        self.value = 0.0

    def __call__(self) -> float:
        return self.value

    async def sleep(self, delay: float) -> None:
        self.value += delay


def _options(
    *,
    batch_records: int = 2,
    clock: Clock | None = None,
    activity: list[ActivityEvent] | None = None,
    progress: list[ProgressEvent] | None = None,
) -> OpportunisticOptions:
    local_clock = clock or Clock()
    return OpportunisticOptions(
        timeouts=TransferTimeouts(
            info=DEFAULT_CONFIG.transfer.info_timeout_seconds,
            transfer=DEFAULT_CONFIG.transfer.sync_timeout_seconds,
        ),
        policy=RetryPolicy(backoff=(0.001,), batch_records=batch_records, stop_after_drained=True),
        activity=activity.append if activity is not None else None,
        progress=progress.append if progress is not None else None,
        clock=local_clock,
        sleep=local_clock.sleep,
    )


@_async_test
async def test_public_drain_coordinator_flushes_latest_samples_when_closed() -> None:
    class PublicDrain:
        def __init__(self) -> None:
            self.snapshot: dict[str, object] = {
                "ring": None,
                "battery": None,
                "last_drain": {"event_id": "stable", "completed_at": "earlier"},
            }
            self.wake_callback: Callable[[], None] | None = None

        def set_wake_callback(self, callback: Callable[[], None] | None) -> None:
            self.wake_callback = callback

        def update_observations(
            self,
            *,
            ring: tuple[int, str] | None = None,
            battery: tuple[int, str] | None = None,
        ) -> None:
            if ring is not None:
                self.snapshot["ring"] = ring
            if battery is not None:
                self.snapshot["battery"] = battery

        def publish_snapshot(self) -> None:
            if self.wake_callback is not None:
                self.wake_callback()

        def arm_pending(self, source_id: str, durable_next_sequence: int) -> None:
            del source_id, durable_next_sequence
            raise AssertionError("shutdown test does not arm a drain")

    public_drain = PublicDrain()
    coordinator = _PublicDrainCoordinator(
        public_drain,
        cast(CaptureRuntimePort, SimpleNamespace(debug_exception=lambda *_args, **_kwargs: None)),
    )
    coordinator.start()
    coordinator.observe_info(RingInfo(8, 11, 10000, 0, RECORD_SIZE), "ring-now")
    coordinator.observe_battery(63, "battery-now")

    await coordinator.close()

    assert public_drain.snapshot == {
        "ring": (3, "ring-now"),
        "battery": (63, "battery-now"),
        "last_drain": {"event_id": "stable", "completed_at": "earlier"},
    }
    assert public_drain.wake_callback is None


class _BlockingSealTarget:
    attempt_id = "a" * 32

    def __init__(self, bundle_path: Path) -> None:
        self.bundle_path = bundle_path
        self.seal_started = threading.Event()
        self.release_seal = threading.Event()
        self.seal_calls = 0

    def prepare(self) -> None:
        return None

    def prepare_leg(self, start_sequence: int, record_count: int) -> DurablePrefix:
        del start_sequence, record_count
        return DurablePrefix(10, 10, 0, "a" * 64)

    def read_begin(self, notice: object) -> None:
        del notice

    def append_chunk(self, offset: int, chunk: memoryview, tail: bytes = b"") -> None:
        del offset, chunk, tail

    def interrupt(self) -> None:
        return None

    def checkpoint(self) -> DurablePrefix:
        return DurablePrefix(10, 11, 1, "a" * 64)

    def seal(self, done_notice: object) -> SealResult:
        del done_notice
        self.seal_calls += 1
        self.seal_started.set()
        assert self.release_seal.wait(1)
        return SealResult(self.bundle_path, False)

    def publish_prefix(self) -> None:
        pass

    def close(self) -> None:
        pass


class _SealResultBatchWriter:
    def __init__(self, target: _BlockingSealTarget) -> None:
        self._target = target
        self.writer = AttemptWriter(target, bytearray(RECORD_SIZE))
        self.close_requested = threading.Event()

    @property
    def attempt_id(self) -> str:
        return self._target.attempt_id

    @property
    def thread(self) -> threading.Thread:
        return self.writer.thread

    @property
    def progress(self) -> WriterProgress:
        return self.writer.progress

    @property
    def failure(self) -> BaseException | None:
        return self.writer.failure

    @property
    def settled_prefix(self) -> None:
        return None

    async def checkpoint(self) -> DurablePrefix:
        return cast(DurablePrefix, await self.writer.checkpoint())

    async def await_seal_result(self) -> SealResult | None:
        return cast(SealResult | None, await self.writer.await_seal_result())

    async def close(self) -> None:
        self.close_requested.set()
        await self.writer.close()


@asynccontextmanager
async def _real_batch_writer(
    store: StagingStore, start: int, count: int, *, prepared: bool = True
) -> AsyncIterator[BatchWriterPort]:
    writer = _runtime().make_batch_writer(
        store,
        start,
        count,
        source_start=start,
        source=memoryview(_records(start, count)),
        config=DEFAULT_CONFIG.writer,
    )
    try:
        await writer.start()
        if prepared:
            await writer.prepare_leg(start, count)
            await writer.read_begin(ReadBeginNotification(start, count))
        yield writer
    finally:
        try:
            await writer.close()
        finally:
            await asyncio.to_thread(writer.thread.join, DEFAULT_CONFIG.transfer.sync_timeout_seconds)
            assert not writer.thread.is_alive()


@_async_test
async def test_real_runtime_writer_start_persists_pending_attempt_before_leg_work(tmp_path: Path) -> None:
    store = _staging_store(tmp_path, _capture_root(tmp_path))
    async with _real_batch_writer(store, 100, 2, prepared=False) as writer:
        pending = store.pending_attempts()
        assert len(pending) == 1
        assert pending[0].attempt_id == writer.attempt_id
        assert pending[0].start_sequence == 100
        assert pending[0].packet_count == 2
        assert pending[0].read_begin_start is None
        assert pending[0].read_begin_count is None


@_async_test
async def test_real_runtime_writer_barrier_returns_persisted_durable_prefix(tmp_path: Path) -> None:
    store = _staging_store(tmp_path, _capture_root(tmp_path))
    record = _record(100)
    async with _real_batch_writer(store, 100, 2) as writer:
        assert writer.publish(RECORD_SIZE)
        durable = await writer.barrier()

        assert durable.start_sequence == 100
        assert durable.next_sequence == 101
        assert durable.record_count == 1
        assert _source_records(store, writer.attempt_id) == record
        source = _attempt_source(store, writer.attempt_id)
        assert source is not None
        assert source.committed_count == 1
        assert source.committed_sha256 == sha256(record).hexdigest()
        assert durable.raw_sha256 == source.committed_sha256


@_async_test
async def test_real_runtime_writer_retains_seal_bundle_result(tmp_path: Path) -> None:
    store = _staging_store(tmp_path, _capture_root(tmp_path))
    records = _records(100, 2)
    async with _real_batch_writer(store, 100, 2) as writer:
        assert writer.publish(len(records))
        sealed = cast(DurableSqliteSeal, await writer.seal(DoneNotification(0, 102)))
        retained = await writer.await_seal_result()

        assert retained == sealed
        assert sealed.source_id == writer.attempt_id
        assert sealed.start_sequence == 100
        assert sealed.next_sequence == 102
        assert sealed.record_count == 2
        assert sealed.raw_sha256 == sha256(records).hexdigest()
        source = _attempt_source(store, sealed.source_id)
        assert source is not None
        assert source.lifecycle == "sealed_waiting"


@_async_test
async def test_real_runtime_classifies_deferred_quarantine_failures() -> None:
    runtime = _runtime()
    errors: tuple[BaseException, ...] = (
        QuarantineSalvageDeferredError("publication should wait"),
        DeviceAlreadyRunningError(),
        StagingError("local staging is temporarily unavailable"),
    )
    assert tuple(runtime.classify_quarantine_error(error) for error in errors) == ("deferred",) * len(errors)


@_async_test
async def test_incomplete_staging_composition_is_rejected_before_provider_open() -> None:
    with pytest.raises(TypeError, match="complete staging capability"):
        await _run_opportunistic_collector(
            cast(Callable[[object | None], AbstractAsyncContextManager[RingSession]], object()),
            cast(StagingPort, object()),
            _options(),
            runtime=_runtime(),
        )


@_async_test
async def test_coordinator_wires_clock_telemetry_to_storage_lease(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = _staging_store(tmp_path, _capture_root(tmp_path))
    lease_entries: list[str] = []
    original_lease = store.clock_mutation_lease

    def clock_mutation_lease() -> StorageLeaseContext:
        lease_entries.append("created")
        return original_lease()

    async def observe_options(self: SessionLifecycle) -> NoDataResult:
        lease = self.run.options.clock_lease
        assert lease is not None
        with lease():
            lease_entries.append("entered")
        return NoDataResult(RingInfo(0, 0, 0, 0, RECORD_SIZE))

    monkeypatch.setattr(store, "clock_mutation_lease", clock_mutation_lease)
    monkeypatch.setattr(SessionLifecycle, "run_direct", observe_options)

    result = await run_opportunistic_collector(Provider([]), store, _options())

    assert result.info.read_sequence == 0
    assert lease_entries == ["created", "entered"]


@_async_test
async def test_coordinator_wires_real_clock_state_store(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    store = _staging_store(tmp_path, _capture_root(tmp_path))
    observed: list[object] = []

    async def observe_options(self: SessionLifecycle) -> NoDataResult:
        observed.append(self.run.options.clock_state_store)
        return NoDataResult(RingInfo(0, 0, 0, 0, RECORD_SIZE))

    monkeypatch.setattr(SessionLifecycle, "run_direct", observe_options)

    await run_opportunistic_collector(Provider([]), store, _options())

    [clock_state_store] = observed
    assert isinstance(clock_state_store, ClockStateStore)
    assert clock_state_store.db is store.state_db


@_async_test
async def test_empty_pending_startup_disposition_cannot_be_rewritten(tmp_path: Path) -> None:
    staging = _staging_store(tmp_path / "spool", tmp_path / "captures")
    maintenance = QuarantineMaintenance(staging, None, _runtime())
    try:
        initial = await maintenance.prepare_pending_startup()

        assert initial.pending is None
        assert initial.durable_next is None
        assert initial.disposition == "empty"
        with pytest.raises(FrozenInstanceError):
            initial.disposition = "needs_interrupted_close"  # type: ignore[reportAttributeAccessIssue]

        repeated = await maintenance.prepare_pending_startup()
        assert repeated is initial
        assert repeated.disposition == "empty"
    finally:
        await maintenance.close()


@_async_test
async def test_coordinator_keeps_supplied_clock_state_store(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    store = _staging_store(tmp_path, _capture_root(tmp_path))
    clock_state_store = ClockStateStore(store.state_db)
    observed: list[object] = []

    async def observe_options(self: SessionLifecycle) -> NoDataResult:
        observed.append(self.run.options.clock_state_store)
        return NoDataResult(RingInfo(0, 0, 0, 0, RECORD_SIZE))

    monkeypatch.setattr(SessionLifecycle, "run_direct", observe_options)

    await run_opportunistic_collector(Provider([]), store, replace(_options(), clock_state_store=clock_state_store))

    assert observed == [clock_state_store]


@_async_test
async def test_progress_pump_coalesces_slow_callbacks_and_ignores_callback_failures() -> None:
    def event(records: int) -> ProgressEvent:
        return ProgressEvent(records, 3, records * RECORD_SIZE, 1.0, 1.0, float(RECORD_SIZE), None)

    mailbox = ProgressMailbox()
    entered = asyncio.Event()
    release = asyncio.Event()
    completed: list[int] = []
    active = 0
    max_active = 0

    async def slow_callback(progress: ProgressEvent) -> None:
        nonlocal active, max_active
        active += 1
        max_active = max(max_active, active)
        completed.append(progress.records_completed)
        if progress.records_completed == 1:
            entered.set()
            await release.wait()
        active -= 1

    async with asyncio.TaskGroup() as tasks:
        pump = tasks.create_task(batch_reconciliation._pump_progress(mailbox, slow_callback, 60.0))
        mailbox.publish(event(1))
        await asyncio.wait_for(entered.wait(), 5)
        mailbox.publish(event(2))
        mailbox.publish(event(3), terminal=True)
        release.set()
        await pump

        assert completed == [1, 3]
        assert max_active == 1
        mailbox.publish(event(3), terminal=True)
        assert completed == [1, 3]

        failing = ProgressMailbox()

        def broken_callback(_event: ProgressEvent) -> None:
            raise OSError("progress sink unavailable")

        failure_pump = tasks.create_task(batch_reconciliation._pump_progress(failing, broken_callback, 1.0))
        failing.publish(event(3), terminal=True)
        await failure_pump


@_async_test
async def test_progress_pump_coalesces_arbitrary_revisions_before_cadence_release() -> None:
    def event(records: int) -> ProgressEvent:
        return ProgressEvent(records, 3, records * RECORD_SIZE, 1.0, 1.0, float(RECORD_SIZE), None)

    class SequencedMailbox(ProgressMailbox):
        def __init__(self) -> None:
            super().__init__()
            self.release_terminal = asyncio.Event()

        async def wait_for_change(self, revision: int) -> ProgressSnapshot:
            if revision == 3:
                await self.release_terminal.wait()
                self._snapshot = ProgressSnapshot(event(3), 4, True)
                return self._snapshot
            next_revision = revision + 1
            self._snapshot = ProgressSnapshot(event(next_revision), next_revision, False)
            return self._snapshot

    mailbox = SequencedMailbox()
    delivered: list[int] = []

    def callback(progress: ProgressEvent) -> None:
        delivered.append(progress.records_completed)

    async with asyncio.TaskGroup() as tasks:
        pump = tasks.create_task(batch_reconciliation._pump_progress(mailbox, callback, 0.2))
        await asyncio.sleep(0)
        assert delivered == [1]

        mailbox.release_terminal.set()
        await pump
    assert delivered == [1, 3]


@_async_test
async def test_delayed_read_reports_progress_before_done(tmp_path: Path) -> None:
    reported = asyncio.Event()
    progress: list[ProgressEvent] = []

    async def report(event: ProgressEvent) -> None:
        progress.append(event)
        reported.set()

    session = ScriptedRingSession(
        _status(),
        (
            WriteStep(b"\x10", (_info(10, 12),)),
            WriteStep(
                encode_read_command(10, 2),
                (
                    _begin(10, 2),
                    _data(_record(10)),
                    _data(_record(11)),
                    DelayedNotification(0.05, _done(12)),
                ),
            ),
            WriteStep(b"\x10", (_info(10, 12),)),
            WriteStep(encode_advance_command(12), (b"\x01\x00",)),
            WriteStep(b"\x10", (_info(12, 12),)),
        ),
    )
    task = asyncio.create_task(
        run_opportunistic_collector(
            Provider([session]),
            _staging_store(tmp_path, _capture_root(tmp_path)),
            replace(_options(), progress=report),
        )
    )

    await asyncio.wait_for(reported.wait(), 1)
    assert progress[-1].records_completed > 0
    assert not task.done()
    await task
    assert progress[-1].records_completed == progress[-1].records_total == 2


def _patch_observation_writer(
    monkeypatch: pytest.MonkeyPatch,
    observed: list[RingInfo],
    close_calls: list[None],
    *,
    observe_error: bool = False,
    close_error: bool | BaseException = False,
) -> None:
    class FakeObservationWriter:
        def __init__(self, *_args: object, **_kwargs: object) -> None:
            pass

        def observe(self, info: RingInfo) -> None:
            if observe_error:
                raise RuntimeError("observation unavailable")
            observed.append(info)

        def close(self) -> None:
            close_calls.append(None)
            if isinstance(close_error, BaseException):
                raise close_error
            if close_error:
                raise RuntimeError("observation close unavailable")

    def make_observation_writer(
        _runtime_instance: OpportunisticRuntime,
        _staging: StagingStore,
        _config: object,
        _on_error: Callable[[Exception], None],
    ) -> FakeObservationWriter:
        return FakeObservationWriter()

    monkeypatch.setattr(OpportunisticRuntime, "make_observation_writer", make_observation_writer)


def _seed_streaming_partial(root: Path, *, count: int, persisted: int, start: int = 100) -> bytes:
    def seed() -> bytes:
        store = _staging_store(root, _capture_root(root))
        attempt = store.prepare_streaming_attempt(start, count)
        attempt.prepare_leg(start, count)
        attempt.record_read_begin(ReadBeginNotification(start, count))
        records = _records(start, count)
        if persisted:
            attempt.accept_chunk(start, memoryview(records[: persisted * RECORD_SIZE]))
        attempt.checkpoint()
        attempt.close(durable=True)
        return records

    return storage_call(seed)


def test_retry_policy_requires_explicit_drain_cooldown() -> None:
    assert RetryPolicy().drain_cooldown_seconds == 900
    explicit = RetryPolicy(drain_cooldown_seconds=300)
    explicit_values = RetryPolicy(backoff=(1,), batch_records=4096, stop_after_drained=True, drain_cooldown_seconds=30)

    assert explicit_values.backoff == (1,)
    assert explicit_values.drain_cooldown_seconds == 30
    assert explicit_values.batch_records == 4096
    assert explicit_values.stop_after_drained
    assert explicit.drain_cooldown_seconds == 300


def test_presence_validation_uses_clean_drain_cooldown() -> None:
    class Observer:
        async def start(self, callback: Callable[[object], object]) -> None:
            del callback

        async def stop(self) -> None:
            return None

    presence = PresenceScheduler(
        Observer(),
        policy=PresencePolicy(scan_recheck_seconds=30, drain_cooldown_seconds=900),
    )
    valid = OpportunisticOptions(
        TransferTimeouts(1, 1),
        RetryPolicy(backoff=presence.policy.rapid_backoff, drain_cooldown_seconds=900),
        presence=presence,
    )
    validate_presence_policy(valid)
    with pytest.raises(ValueError):
        validate_presence_policy(
            replace(valid, policy=RetryPolicy(backoff=presence.policy.rapid_backoff, drain_cooldown_seconds=30))
        )


@_async_test
async def test_drained_info_reaches_observation_writer_and_writer_closes_once(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    observed: list[RingInfo] = []
    close_calls: list[None] = []
    _patch_observation_writer(monkeypatch, observed, close_calls)
    session = ScriptedRingSession(_status(), (WriteStep(b"\x10", (_info(100, 100),)),))

    result = await run_opportunistic_collector(
        Provider([session]), _staging_store(tmp_path, _capture_root(tmp_path)), _options()
    )

    assert isinstance(result, NoDataResult)
    assert observed == [RingInfo(100, 100, 10000, 0, RECORD_SIZE)]
    assert close_calls == [None]


@pytest.mark.parametrize("hang_battery", (False, True))
@_async_test
async def test_empty_batch_reconciler_keeps_info_for_final_battery_observation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, hang_battery: bool
) -> None:
    class BatterySession(ScriptedRingSession):
        def __init__(self) -> None:
            super().__init__(_status(), (WriteStep(b"\x10", (_info(100, 100),)),))
            self.battery_reads: list[str] = []
            self.battery_cancelled = False

        async def read_optional_characteristic(self, uuid: str) -> bytes:
            self.battery_reads.append(uuid)
            if hang_battery:
                try:
                    await asyncio.Future()
                except asyncio.CancelledError:
                    self.battery_cancelled = True
                    raise
            return bytes((74,))

    async def skip_initial_telemetry(*_args: object, **_kwargs: object) -> None:
        return None

    monkeypatch.setattr(
        "omi_collector.capture.application.session_lifecycle.collect_operational_telemetry", skip_initial_telemetry
    )
    collect_battery_observation = session_lifecycle.collect_battery_observation
    operation_timeouts: list[float] = []

    async def record_battery_timeout(
        battery_session: object,
        info: RingInfo,
        emit: Callable[[Mapping[str, object]], object],
        *,
        operation_timeout: float,
        observe_battery: Callable[[int], object] | None = None,
    ) -> None:
        operation_timeouts.append(operation_timeout)
        await collect_battery_observation(
            battery_session,
            info,
            emit,
            operation_timeout=operation_timeout,
            observe_battery=observe_battery,
        )

    monkeypatch.setattr(session_lifecycle, "collect_battery_observation", record_battery_timeout)
    session = BatterySession()
    observations: list[dict[str, object]] = []
    config = CollectorConfig(retry=RetryConfig(presence_preflight_budget_seconds=0.05))
    options = replace(
        _options(),
        operational=lambda event: observations.append(dict(event)),
        config=config,
    )
    result = await asyncio.wait_for(
        run_opportunistic_collector(Provider([session]), _staging_store(tmp_path, _capture_root(tmp_path)), options),
        timeout=2.0,
    )

    assert isinstance(result, NoDataResult)
    assert session.battery_reads == [BATTERY_UUID]
    assert operation_timeouts == [0.05]
    assert len(observations) == 1
    assert observations[0]["event"] == "pendant_observation"
    if hang_battery:
        assert "battery_percent" not in observations[0]
        outcomes = observations[0]["optional_outcomes"]
        assert isinstance(outcomes, dict)
        assert outcomes["battery"] == "timeout"
        assert session.battery_cancelled
    else:
        assert observations[0]["battery_percent"] == 74
        assert not session.battery_cancelled
    assert observations[0]["read_sequence"] == 100
    assert observations[0]["write_sequence"] == 100


@_async_test
async def test_successful_info_values_reach_observation_writer_across_batch_lifecycle(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    observed: list[RingInfo] = []
    close_calls: list[None] = []
    _patch_observation_writer(monkeypatch, observed, close_calls)
    session = ScriptedRingSession(
        _status(),
        (
            WriteStep(b"\x10", (_info(10, 11, dropped=5),)),
            WriteStep(encode_read_command(10, 1), (_begin(10, 1), _data(_record(10)), _done(11))),
            WriteStep(b"\x10", (_info(10, 11, dropped=6),)),
            WriteStep(encode_advance_command(11), (b"\x01\x00",)),
            WriteStep(b"\x10", (_info(11, 11, dropped=0),)),
        ),
    )

    result = await run_opportunistic_collector(
        Provider([session]), _staging_store(tmp_path, _capture_root(tmp_path)), _options()
    )

    assert isinstance(result, CollectionResult)
    assert observed == [
        RingInfo(10, 11, 10000, 5, RECORD_SIZE),
        RingInfo(10, 11, 10000, 6, RECORD_SIZE),
        RingInfo(11, 11, 10000, 0, RECORD_SIZE),
    ]
    assert close_calls == [None]
    assert session.writes == [b"\x10", encode_read_command(10, 1), b"\x10", encode_advance_command(11), b"\x10"]


@_async_test
@pytest.mark.parametrize("failure", ["observe", "close"])
async def test_observation_writer_failures_do_not_block_collection(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, failure: str
) -> None:
    observed: list[RingInfo] = []
    close_calls: list[None] = []
    debug_records: list[tuple[str, BaseException, dict[str, object]]] = []

    def record_debug(event: str, error: BaseException, **fields: object) -> None:
        debug_records.append((event, error, fields))

    def runtime_debug_exception(
        _runtime_instance: OpportunisticRuntime, event: str, error: BaseException, **fields: object
    ) -> None:
        record_debug(event, error, **fields)

    monkeypatch.setattr(OpportunisticRuntime, "debug_exception", runtime_debug_exception)
    _patch_observation_writer(
        monkeypatch, observed, close_calls, observe_error=failure == "observe", close_error=failure == "close"
    )
    session = ScriptedRingSession(
        _status(),
        (
            WriteStep(b"\x10", (_info(10, 11),)),
            WriteStep(encode_read_command(10, 1), (_begin(10, 1), _data(_record(10)), _done(11))),
            WriteStep(b"\x10", (_info(10, 11),)),
            WriteStep(encode_advance_command(11), (b"\x01\x00",)),
            WriteStep(b"\x10", (_info(11, 11),)),
        ),
    )

    result = await run_opportunistic_collector(
        Provider([session]), _staging_store(tmp_path, _capture_root(tmp_path)), _options()
    )

    assert isinstance(result, CollectionResult)
    assert result.next_sequence == 11
    assert session.writes == [b"\x10", encode_read_command(10, 1), b"\x10", encode_advance_command(11), b"\x10"]
    assert close_calls == [None]
    assert debug_records[0][0] == "firmware_observation_writer_error"
    assert debug_records[0][2]["operation"] == failure


@pytest.mark.parametrize("failure_stage", ("collection", "finalization"))
@_async_test
async def test_observation_close_cancellation_does_not_mask_primary_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, failure_stage: str
) -> None:
    close_calls: list[None] = []
    primary = RuntimeError(f"primary {failure_stage} failure")
    _patch_observation_writer(monkeypatch, [], close_calls, close_error=asyncio.CancelledError())

    if failure_stage == "collection":

        def provider(_candidate: object | None = None) -> AbstractAsyncContextManager[RingSession]:
            raise primary

        call = run_opportunistic_collector(provider, _staging_store(tmp_path, _capture_root(tmp_path)), _options())
    else:
        session = ScriptedRingSession(_status(), (WriteStep(b"\x10", (_info(10, 10),)),))

        async def fail_finalization(_reconciler: BatchReconciler) -> object:
            raise primary

        monkeypatch.setattr(BatchReconciler, "finalize_active", fail_finalization)
        call = run_opportunistic_collector(
            Provider([session]), _staging_store(tmp_path, _capture_root(tmp_path)), _options()
        )

    with pytest.raises(RuntimeError) as raised:
        await call

    assert raised.value is primary
    assert close_calls == [None]


@_async_test
async def test_cancellation_during_observation_close_propagates(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    close_started = threading.Event()
    release_close = threading.Event()

    class BlockingObservationWriter:
        def __init__(self, *_args: object, **_kwargs: object) -> None:
            pass

        def observe(self, _info: RingInfo) -> None:
            return None

        def close(self) -> None:
            close_started.set()
            release_close.wait(2)

    def make_blocking_observation_writer(
        _runtime_instance: OpportunisticRuntime,
        _staging: StagingStore,
        _config: object,
        _on_error: Callable[[Exception], None],
    ) -> BlockingObservationWriter:
        return BlockingObservationWriter()

    monkeypatch.setattr(OpportunisticRuntime, "make_observation_writer", make_blocking_observation_writer)
    session = ScriptedRingSession(_status(), (WriteStep(b"\x10", (_info(100, 100),)),))
    task = asyncio.create_task(
        run_opportunistic_collector(Provider([session]), _staging_store(tmp_path, _capture_root(tmp_path)), _options())
    )

    try:
        assert await asyncio.to_thread(close_started.wait, 1)
        task.cancel()
        release_close.set()
        with pytest.raises(asyncio.CancelledError):
            await task
    finally:
        release_close.set()


@_async_test
@pytest.mark.parametrize("kind", ["single", "multiple"])
async def test_partial_evidence_is_quarantined_before_provider_or_gatt(tmp_path: Path, kind: str) -> None:
    store = _staging_store(tmp_path, _capture_root(tmp_path))
    ranges = ((100, 2),) if kind == "single" else ((100, 2), (102, 2))
    for start, count in ranges:
        storage_call(lambda start=start, count=count: store.prepare_streaming_attempt(start, count).close())
    provider = Provider([ScriptedRingSession(_status(), (WriteStep(b"\x10", (_info(100, 100),)),))])
    result = await run_opportunistic_collector(provider, store, _options())

    assert isinstance(result, NoDataResult)
    assert provider.opened == 1
    assert not store.pending_attempts()


@_async_test
async def test_corrupt_committed_record_is_held_before_provider_read(tmp_path: Path) -> None:
    records = _seed_streaming_partial(tmp_path, count=2, persisted=1)
    store = _staging_store(tmp_path, _capture_root(tmp_path))
    source_id = store.pending_attempts()[0].attempt_id
    changed_payload = b"x" * (AUDIO_RECORD_SIZE - 4)
    assert records[4:RECORD_SIZE] != changed_payload

    def corrupt() -> int:
        with closing(store.state_db.open_connection()) as connection:
            updated = connection.execute(
                "UPDATE capture_record SET payload = ? WHERE source_id = ? AND ordinal = 0",
                (changed_payload, source_id),
            )
            connection.commit()
            return updated.rowcount

    assert storage_call(corrupt) == 1
    session = ScriptedRingSession(_status(), (WriteStep(b"\x10", (_info(101, 101),)),))
    provider = Provider([session])
    result = await run_opportunistic_collector(provider, _staging_store(tmp_path, _capture_root(tmp_path)), _options())

    assert isinstance(result, NoDataResult)
    assert provider.opened == 1
    assert not store.pending_attempts()
    assert store.quarantined_attempts() == ()

    def read_source() -> tuple[object, ...] | None:
        with closing(store.state_db.open_connection(read_only=True)) as connection:
            return cast(
                tuple[object, ...] | None,
                connection.execute(
                    "SELECT source_type, lifecycle, block_reason, quarantine_disposition, "
                    "certified_prefix_count, coverage_digest, terminal_time, retention_deadline "
                    "FROM capture_source WHERE source_id = ?",
                    (source_id,),
                ).fetchone(),
            )

    row = storage_call(read_source)
    assert row is not None
    assert tuple(row) == (
        "quarantine",
        "blocked",
        "source_authentication_mismatch",
        "unclassified",
        None,
        None,
        None,
        None,
    )
    assert encode_advance_command(102) not in session.writes


@_async_test
async def test_resumed_complete_prefix_at_device_end_seals_without_read_or_advance(tmp_path: Path) -> None:
    records = _seed_streaming_partial(tmp_path, count=2, persisted=2)
    # The second INFO is fresh after sealing and gates retirement even at end.
    session = ScriptedRingSession(
        _status(), (WriteStep(b"\x10", (_info(102, 102),)), WriteStep(b"\x10", (_info(102, 102),)))
    )

    result = await run_opportunistic_collector(
        Provider([session]), _staging_store(tmp_path, _capture_root(tmp_path)), _options()
    )

    assert isinstance(result, CollectionResult)
    assert isinstance(result.seal, DurableSqliteSeal)
    assert session.writes == [b"\x10", b"\x10"]
    assert result.seal.source_id
    assert _sealed_records(_staging_store(tmp_path, _capture_root(tmp_path)), result.seal) == records


@_async_test
async def test_collect_mode_seals_without_advance_or_second_info(tmp_path: Path) -> None:
    session = ScriptedRingSession(
        _status(),
        (
            WriteStep(b"\x10", (_info(10, 11),)),
            WriteStep(encode_read_command(10, 1), (_begin(10, 1), _data(_record(10)), _done(11))),
        ),
    )

    options = _options(batch_records=1)
    options = replace(options, policy=replace(options.policy, advance_enabled=False))
    result = await run_opportunistic_collector(
        Provider([session]), _staging_store(tmp_path, _capture_root(tmp_path)), options
    )

    assert isinstance(result, CollectionResult)
    assert not result.advance_confirmed
    assert session.writes == [b"\x10", encode_read_command(10, 1)]
    assert all(command[:1] != b"\x12" for command in session.writes)


@_async_test
async def test_restart_hydrates_checkpoint_and_resumes_at_durable_prefix(tmp_path: Path) -> None:
    records = _seed_streaming_partial(tmp_path, count=2, persisted=1)
    store = _staging_store(tmp_path, _capture_root(tmp_path))
    session = ScriptedRingSession(
        _status(),
        (
            WriteStep(b"\x10", (_info(100, 102),)),
            WriteStep(
                encode_read_command(101, 1),
                (_begin(101, 1), _data(_record(101)), _done(102)),
            ),
            WriteStep(b"\x10", (_info(100, 102),)),
            WriteStep(encode_advance_command(102), (b"\x01\x00",)),
            WriteStep(b"\x10", (_info(102, 102),)),
            WriteStep(b"\x10", (_info(102, 102),)),
        ),
    )

    result = await run_opportunistic_collector(
        Provider([session]),
        store,
        replace(_options(), public_drain=store.public_drain),
    )

    assert isinstance(result, CollectionResult)
    assert isinstance(result.seal, DurableSqliteSeal)
    assert session.writes == [
        b"\x10",
        encode_read_command(101, 1),
        b"\x10",
        encode_advance_command(102),
        b"\x10",
        b"\x10",
    ]
    assert _sealed_records(_staging_store(tmp_path, _capture_root(tmp_path)), result.seal) == records
    drain_state = storage_call(lambda: store.state_db.get(StateKind.PENDANT_DRAIN, "current"))
    assert drain_state is not None and drain_state["last_drain"] is not None


@_async_test
async def test_live_partial_ahead_of_fresh_cursor_resumes_at_durable_prefix(tmp_path: Path) -> None:
    start, durable_next, cursor = 1580377, 1583030, 1583025
    end = durable_next + 1
    records = _seed_streaming_partial(tmp_path, start=start, count=end - start, persisted=durable_next - start)
    session = ScriptedRingSession(
        _status(),
        (
            WriteStep(b"\x10", (_info(cursor, end),)),
            WriteStep(
                encode_read_command(durable_next, end - durable_next),
                (
                    _begin(durable_next, end - durable_next),
                    _data(_records(durable_next, end - durable_next)),
                    _done(end),
                ),
            ),
            WriteStep(b"\x10", (_info(cursor, end),)),
            WriteStep(encode_advance_command(end), (b"\x01\x00",)),
            WriteStep(b"\x10", (_info(end, end),)),
        ),
    )

    result = await run_opportunistic_collector(
        Provider([session]), _staging_store(tmp_path, _capture_root(tmp_path)), _options(batch_records=end - start)
    )

    assert isinstance(result, CollectionResult)
    assert isinstance(result.seal, DurableSqliteSeal)
    assert session.writes[1] == encode_read_command(durable_next, end - durable_next)
    assert _sealed_records(_staging_store(tmp_path, _capture_root(tmp_path)), result.seal) == records


@_async_test
async def test_restart_after_seal_reads_fresh_cursor_before_any_advance(tmp_path: Path) -> None:
    records = _seed_streaming_partial(tmp_path, count=2, persisted=2)
    store = _staging_store(tmp_path, _capture_root(tmp_path))
    attempt_id = store.pending_attempts()[0].attempt_id

    def seal_resumed() -> DurableSqliteSeal:
        with store.device_lock() as lease:
            sealed = store.resume_streaming_attempt(lease)
            assert sealed is not None
            try:
                result = sealed.seal(DoneNotification(0, 102))
                assert isinstance(result, DurableSqliteSeal)
                return result
            finally:
                sealed.close()

    sealed_result = storage_call(seal_resumed)
    assert sealed_result.source_id == attempt_id
    assert _attempt_source(store, sealed_result.source_id) is not None
    assert storage_call(lambda: store.state_db.get(StateKind.PENDANT_DRAIN, "current")) is None
    session = ScriptedRingSession(
        _status(),
        (
            WriteStep(b"\x10", (_info(100, 102),)),
            WriteStep(encode_read_command(100, 2), (_begin(100, 2), _data(records), _done(102))),
            WriteStep(b"\x10", (_info(100, 102),)),
            WriteStep(encode_advance_command(102), (b"\x01\x00",)),
            WriteStep(b"\x10", (_info(102, 102),)),
            WriteStep(b"\x10", (_info(102, 102),)),
        ),
    )

    result = await run_opportunistic_collector(
        Provider([session]), store, replace(_options(), public_drain=store.public_drain)
    )

    assert isinstance(result, CollectionResult)
    assert isinstance(result.seal, DurableSqliteSeal)
    assert result.seal.source_id != attempt_id
    original = _attempt_source(store, attempt_id)
    assert original is not None and original.lifecycle == "sealed_waiting"
    assert session.writes[1] == encode_read_command(100, 2)
    assert session.writes[3] == encode_advance_command(102)
    drain_state = storage_call(lambda: store.state_db.get(StateKind.PENDANT_DRAIN, "current"))
    assert drain_state is not None and drain_state["last_drain"] is not None


@_async_test
async def test_post_seal_cursor_ahead_keeps_old_bundle_and_starts_at_fresh_cursor(tmp_path: Path) -> None:
    session = ScriptedRingSession(
        _status(),
        (
            WriteStep(b"\x10", (_info(100, 102),)),
            WriteStep(encode_read_command(100, 2), (_begin(100, 2), _data(_records(100, 2)), _done(102))),
            WriteStep(b"\x10", (_info(103, 104),)),
            WriteStep(encode_read_command(103, 1), (_begin(103, 1), _data(_record(103)), _done(104))),
            WriteStep(b"\x10", (_info(103, 104),)),
            WriteStep(encode_advance_command(104), (b"\x01\x00",)),
            WriteStep(b"\x10", (_info(104, 104),)),
        ),
    )

    result = await run_opportunistic_collector(
        Provider([session]), _staging_store(tmp_path, _capture_root(tmp_path)), _options()
    )

    assert isinstance(result, CollectionResult)
    assert result.next_sequence == 104
    assert encode_advance_command(102) not in session.writes
    store = _staging_store(tmp_path, _capture_root(tmp_path))

    def read_sources() -> list[tuple[str, bytes, bytes, int]]:
        with closing(store.state_db.open_connection(read_only=True)) as connection:
            return cast(
                list[tuple[str, bytes, bytes, int]],
                connection.execute(
                    "SELECT source_id, sequence_start, sequence_limit, committed_count "
                    "FROM capture_source WHERE source_type = 'attempt' ORDER BY sequence_start"
                ).fetchall(),
            )

    sources = storage_call(read_sources)
    assert [(int.from_bytes(row[1], "big"), int.from_bytes(row[2], "big"), row[3]) for row in sources] == [
        (100, 102, 2),
        (103, 104, 1),
    ]
    assert _source_records(store, sources[0][0]) == _records(100, 2)
    assert _source_records(store, sources[1][0]) == _record(103)


@_async_test
async def test_acknowledged_advance_with_ahead_cursor_is_not_reported_confirmed(tmp_path: Path) -> None:
    session = ScriptedRingSession(
        _status(),
        (
            WriteStep(b"\x10", (_info(10, 11),)),
            WriteStep(encode_read_command(10, 1), (_begin(10, 1), _data(_record(10)), _done(11))),
            WriteStep(b"\x10", (_info(10, 11),)),
            WriteStep(encode_advance_command(11), (b"\x01\x00",)),
            WriteStep(b"\x10", (_info(12, 12),)),
        ),
    )

    result = await run_opportunistic_collector(
        Provider([session]), _staging_store(tmp_path, _capture_root(tmp_path)), _options(batch_records=1)
    )

    assert isinstance(result, CollectionResult)
    assert not result.advance_confirmed


@_async_test
async def test_restart_cursor_after_prefix_publishes_gap_and_continues_at_cursor(tmp_path: Path) -> None:
    _seed_streaming_partial(tmp_path, count=3, persisted=1)
    store = _staging_store(tmp_path, _capture_root(tmp_path))
    store.preflight_storage()
    store.initialize_confirmed_loss_ledger()
    public_dir = store.state_db.path.parent / "public"
    public_dir.mkdir(mode=0o2750)
    public_dir.chmod(0o2750)
    session = ScriptedRingSession(
        _status(),
        (
            WriteStep(b"\x10", (_info(102, 103),)),
            WriteStep(b"\x10", (_info(102, 103),)),
            WriteStep(encode_read_command(102, 1), (_begin(102, 1), _data(_record(102)), _done(103))),
            WriteStep(b"\x10", (_info(102, 103),)),
            WriteStep(encode_advance_command(103), (b"\x01\x00",)),
            WriteStep(b"\x10", (_info(103, 103),)),
        ),
    )

    result = await run_opportunistic_collector(Provider([session]), store, _options(batch_records=3))

    assert isinstance(result, CollectionResult)
    assert session.writes[2] == encode_read_command(102, 1)
    assert encode_advance_command(102) not in session.writes


@_async_test
async def test_capture_space_exhaustion_stops_before_read_or_advance(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(
        internal_audio_store.os,
        "statvfs",
        lambda _path: SimpleNamespace(f_bavail=0, f_frsize=4096),
    )
    session = ScriptedRingSession(
        _status(),
        (WriteStep(b"\x10", (_info(100, 101),)),),
    )
    store = _staging_store(tmp_path, _capture_root(tmp_path))

    with pytest.raises(DiskSpaceError, match="capture space"):
        await run_opportunistic_collector(Provider([session]), store, _options(batch_records=1))

    assert session.writes == [b"\x10"]
    [pending] = store.pending_attempts()
    assert (pending.start_sequence, pending.packet_count) == (100, 1)


@_async_test
async def test_fresh_restart_cursor_ahead_publishes_prefix_and_reads_from_current_cursor(tmp_path: Path) -> None:
    _seed_streaming_partial(tmp_path, count=2, persisted=1)
    store = _staging_store(tmp_path, _capture_root(tmp_path))
    attempt_id = store.pending_attempts()[0].attempt_id
    store.preflight_storage()
    store.initialize_confirmed_loss_ledger()
    session = ScriptedRingSession(
        _status(),
        (
            WriteStep(b"\x10", (_info(103, 104),)),
            WriteStep(b"\x10", (_info(103, 104),)),
            WriteStep(encode_read_command(103, 1), (_begin(103, 1), _data(_record(103)), _done(104))),
            WriteStep(b"\x10", (_info(103, 104),)),
            WriteStep(encode_advance_command(104), (b"\x01\x00",)),
            WriteStep(b"\x10", (_info(104, 104),)),
        ),
    )

    result = await run_opportunistic_collector(Provider([session]), store, _options())

    assert isinstance(result, CollectionResult)
    assert encode_read_command(103, 1) in session.writes
    assert encode_advance_command(102) not in session.writes
    source = _attempt_source(store, attempt_id)
    assert source is not None
    assert source.lifecycle == "sealed_waiting"
    assert (source.sequence_start, source.sequence_limit, source.committed_count) == (100, 102, 1)
    assert _source_records(store, attempt_id) == _record(100)


@_async_test
async def test_recovered_confirmed_loss_completes_public_drain_token(tmp_path: Path) -> None:
    store = _staging_store(tmp_path, _capture_root(tmp_path))
    store.preflight_storage()
    store.initialize_confirmed_loss_ledger()
    first = ScriptedRingSession(
        _status(),
        (
            WriteStep(b"\x10", (_info(100, 102),)),
            WriteStep(
                encode_read_command(100, 2),
                (_begin(100, 2), _data(_record(100)), RingTransportDisconnectedError("gone")),
            ),
            WriteStep(encode_stop_command()),
        ),
    )
    recovered = ScriptedRingSession(
        _status(),
        (
            WriteStep(b"\x10", (_info(103, 103),)),
            WriteStep(b"\x10", (_info(103, 103),)),
            WriteStep(b"\x10", (_info(103, 103),)),
        ),
    )

    class StopAfterAwayError(Exception):
        pass

    def stop_after_away(event: ActivityEvent) -> None:
        if event.state == "away":
            raise StopAfterAwayError

    with pytest.raises(StopAfterAwayError):
        await asyncio.wait_for(
            run_opportunistic_collector(
                Provider([first]),
                store,
                replace(_options(), activity=stop_after_away, public_drain=store.public_drain),
            ),
            20,
        )

    reopened = _staging_store(tmp_path, _capture_root(tmp_path))
    result = await run_opportunistic_collector(
        Provider([recovered]),
        reopened,
        replace(_options(), public_drain=reopened.public_drain),
    )

    assert isinstance(result, CollectionResult)
    assert result.next_sequence == 101
    assert _ready_closure_rows(store) == [{"next_sequence": 101, "reason": "drained"}]
    [loss] = storage_call(lambda: read_confirmed_losses(reopened.state_db))
    assert (loss.start_sequence, loss.end_sequence) == (101, 103)
    assert _source_records(reopened, loss.attempt_id) == _record(100)
    assert recovered.writes == [b"\x10"] * 3
    drain = storage_call(lambda: reopened.state_db.get(StateKind.PENDANT_DRAIN, "current"))
    assert drain is not None and drain["last_drain"] is not None
    assert drain["pending_event_id"] is None

    restarted_again = _staging_store(tmp_path, _capture_root(tmp_path))
    reopened_drain = storage_call(lambda: restarted_again.state_db.get(StateKind.PENDANT_DRAIN, "current"))
    assert reopened_drain is not None and reopened_drain["last_drain"] == drain["last_drain"]


@_async_test
async def test_startup_sweep_retains_aged_sealed_source_until_publication_ack(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    now = 1_000_000_000

    def wall_clock_ns() -> int:
        return now

    monkeypatch.setattr("omi_collector.capture.adapters.quarantine._wall_clock_ns", wall_clock_ns)
    store = _staging_store(
        tmp_path,
        _capture_root(tmp_path),
        config=CollectorConfig(staging_retention=StagingRetentionConfig(terminal_retention_seconds=1.0)),
    )

    def seed() -> str:
        attempt = store.prepare_streaming_attempt(100, 1)
        attempt.prepare_leg(100, 1)
        attempt.record_read_begin(ReadBeginNotification(100, 1))
        attempt.accept_chunk(100, _record(100))
        attempt.checkpoint()
        attempt.publish_prefix()
        attempt.close(durable=True)
        return attempt.attempt_id

    attempt_id = storage_call(seed)
    storage_call(lambda: store.terminalize_prefix_attempt(attempt_id))
    now += 1_000_000_000
    session = ScriptedRingSession(_status(), (WriteStep(b"\x10", (_info(101, 101),)),))

    result = await run_opportunistic_collector(Provider([session]), store, _options())

    assert isinstance(result, NoDataResult)
    source = _attempt_source(store, attempt_id)
    assert source is not None and source.lifecycle == "sealed_waiting"
    assert _source_records(store, attempt_id) == _record(100)


@_async_test
async def test_maintenance_cadence_retains_sealed_source_until_publication_ack(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    now = 1_000_000_000
    maintenance_now = 0.0

    def wall_clock_ns() -> int:
        return now

    async def advance_wall_clock(_delay: float) -> None:
        nonlocal now, maintenance_now
        now += 1_000_000_000
        maintenance_now += 1.0

    monkeypatch.setattr("omi_collector.capture.adapters.quarantine._wall_clock_ns", wall_clock_ns)
    monkeypatch.setattr("omi_collector.capture.application.quarantine_maintenance.monotonic", lambda: maintenance_now)
    store = _staging_store(
        tmp_path,
        _capture_root(tmp_path),
        config=CollectorConfig(
            retry=RetryConfig(maintenance_interval_seconds=1.0),
            staging_retention=StagingRetentionConfig(terminal_retention_seconds=1.0),
        ),
    )

    def seed() -> str:
        attempt = store.prepare_streaming_attempt(100, 1)
        attempt.prepare_leg(100, 1)
        attempt.record_read_begin(ReadBeginNotification(100, 1))
        attempt.accept_chunk(100, _record(100))
        attempt.checkpoint()
        attempt.publish_prefix()
        attempt.close(durable=True)
        return attempt.attempt_id

    attempt_id = storage_call(seed)
    storage_call(lambda: store.terminalize_prefix_attempt(attempt_id))
    drained = Provider([ScriptedRingSession(_status(), (WriteStep(b"\x10", (_info(101, 101),)),))])

    class RetryThenDrain:
        calls = 0

        def __call__(self, candidate: object | None = None):
            del candidate
            self.calls += 1
            if self.calls == 1:
                raise RingTransportUnavailableError("simulated retry")
            return drained()

    provider = RetryThenDrain()
    result = await run_opportunistic_collector(
        provider,
        store,
        replace(
            _options(),
            sleep=advance_wall_clock,
            config=CollectorConfig(
                retry=RetryConfig(maintenance_interval_seconds=1.0),
                staging_retention=StagingRetentionConfig(terminal_retention_seconds=1.0),
            ),
        ),
    )

    assert isinstance(result, NoDataResult)
    assert provider.calls == 2
    source = _attempt_source(store, attempt_id)
    assert source is not None and source.lifecycle == "sealed_waiting"
    assert _source_records(store, attempt_id) == _record(100)


@_async_test
async def test_terminal_retired_sweep_failure_is_structured_and_does_not_stop_collection(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = _staging_store(tmp_path, _capture_root(tmp_path))
    events: list[tuple[str, BaseException, dict[str, object]]] = []

    def fail_sweep(*, should_defer: Callable[[], bool]) -> SweepResult:
        del should_defer
        raise OSError("simulated sweep failure")

    def record(event: str, error: BaseException, **fields: object) -> None:
        events.append((event, error, fields))

    monkeypatch.setattr(store, "sweep_terminal_quarantine", fail_sweep)

    def runtime_debug_exception(
        _runtime_instance: OpportunisticRuntime, event: str, error: BaseException, **fields: object
    ) -> None:
        record(event, error, **fields)

    monkeypatch.setattr(OpportunisticRuntime, "debug_exception", runtime_debug_exception)
    session = ScriptedRingSession(_status(), (WriteStep(b"\x10", (_info(100, 100),)),))

    result = await run_opportunistic_collector(Provider([session]), store, _options())

    assert isinstance(result, NoDataResult)
    assert len(events) == 1
    for event, error, fields in events:
        assert event == "terminal_quarantine_sweep_failed"
        assert isinstance(error, OSError)
        assert fields == {}


@_async_test
async def test_startup_scan_wake_defers_and_joins_quarantine_before_provider(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    scan_started = asyncio.Event()
    maintenance_started = asyncio.Event()
    maintenance_stopped = asyncio.Event()
    release_wake = asyncio.Event()
    release_maintenance = asyncio.Event()

    class BlockingPresence:
        closed = False
        waiter_task: asyncio.Task[PresenceWake] | None = None
        policy = PresencePolicy(
            scan_recheck_seconds=30.0,
            rapid_backoff=(0.001,),
            drain_cooldown_seconds=DEFAULT_CONFIG.presence.drain_cooldown_seconds,
        )

        async def wait_for_attempt(self) -> PresenceWake:
            current = asyncio.current_task()
            assert current is not None
            self.waiter_task = cast(asyncio.Task[PresenceWake], current)
            scan_started.set()
            await release_wake.wait()
            return PresenceWake(
                "advertisement", candidate=object(), observed_at=time.monotonic(), advertisement_rssi_dbm=-72
            )

        def resume_interrupted_visit(self) -> None:
            raise AssertionError("fresh collector should not resume an interrupted visit")

        async def attempt_finished(self, _outcome: AttemptOutcome) -> None:
            return None

        async def close(self) -> None:
            self.closed = True

    presence = BlockingPresence()
    store = _staging_store(tmp_path, _capture_root(tmp_path))

    async def slow_maintenance(
        _maintenance: QuarantineMaintenance,
        should_defer: Callable[[], bool],
    ) -> None:
        assert scan_started.is_set()
        maintenance_started.set()
        try:
            while not release_maintenance.is_set() and not should_defer():
                await asyncio.sleep(0)
        finally:
            maintenance_stopped.set()

    class OrderedProvider(Provider):
        def __call__(self, candidate: object | None = None):
            assert maintenance_stopped.is_set()
            return super().__call__(candidate)

    monkeypatch.setattr(QuarantineMaintenance, "run_once", slow_maintenance)
    session = ScriptedRingSession(_status(), (WriteStep(b"\x10", (_info(100, 100),)),))
    journal = _quality_metrics(store.state_db)
    task = asyncio.create_task(
        run_opportunistic_collector(
            OrderedProvider([session]),
            store,
            replace(
                _options(),
                presence=cast(PresenceScheduler, presence),
                quality_metrics=journal,
                host_time=lambda: 1000.0,
            ),
        )
    )
    maintenance_ready = asyncio.create_task(maintenance_started.wait())
    try:
        done, _ = await asyncio.wait({maintenance_ready, task}, timeout=5.0, return_when=asyncio.FIRST_COMPLETED)
        assert maintenance_ready in done, "collector completed before startup maintenance started"
        assert maintenance_ready.result()
        release_wake.set()
        done, pending = await asyncio.wait({task}, timeout=5.0)
        assert task in done and not pending
        assert isinstance(task.result(), NoDataResult)
        assert maintenance_stopped.is_set()
        assert presence.closed
        [advertisement] = _quality_events(journal, store.state_db)
        assert advertisement["event"] == "advertisement_observation"
        assert advertisement["recorded_at"] == "1970-01-01T00:16:40.000+00:00"
        assert advertisement["advertisement_rssi_dbm"] == -72
    finally:
        release_wake.set()
        release_maintenance.set()
        if presence.waiter_task is not None:
            presence.waiter_task.cancel()
        if presence.waiter_task is not None:
            await _finish_task(presence.waiter_task)
        await _finish_task(maintenance_ready, cancel=True)
        await _finish_task(task, cancel=True)


@_async_test
async def test_coordinator_cancellation_joins_quarantine_maintenance(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    worker_started = threading.Event()
    worker_stopped = threading.Event()
    release_worker = threading.Event()
    waiter_task: asyncio.Task[PresenceWake] | None = None

    class BlockingPresence:
        closed = False
        policy = PresencePolicy(
            scan_recheck_seconds=30.0,
            rapid_backoff=(0.001,),
            drain_cooldown_seconds=DEFAULT_CONFIG.presence.drain_cooldown_seconds,
        )

        async def wait_for_attempt(self) -> PresenceWake:
            nonlocal waiter_task
            current = asyncio.current_task()
            assert current is not None
            waiter_task = cast(asyncio.Task[PresenceWake], current)
            await asyncio.Event().wait()
            raise AssertionError("unreachable")

        def resume_interrupted_visit(self) -> None:
            raise AssertionError("fresh collector should not resume an interrupted visit")

        async def close(self) -> None:
            self.closed = True

    def maintenance_worker(should_defer: Callable[[], bool]) -> None:
        worker_started.set()
        try:
            while not release_worker.is_set() and not should_defer():
                time.sleep(0.001)
        finally:
            worker_stopped.set()

    async def blocking_maintenance(
        _maintenance: QuarantineMaintenance,
        should_defer: Callable[[], bool],
    ) -> None:
        await asyncio.to_thread(maintenance_worker, should_defer)

    presence = BlockingPresence()
    monkeypatch.setattr(QuarantineMaintenance, "run_once", blocking_maintenance)
    task = asyncio.create_task(
        run_opportunistic_collector(
            Provider([]),
            _staging_store(tmp_path, _capture_root(tmp_path)),
            replace(_options(), presence=cast(PresenceScheduler, presence)),
        )
    )
    worker_ready = asyncio.create_task(asyncio.to_thread(worker_started.wait, 1))
    try:
        done, _ = await asyncio.wait({worker_ready, task}, timeout=5.0, return_when=asyncio.FIRST_COMPLETED)
        assert worker_ready in done, "maintenance worker did not start before owner completed"
        assert worker_ready.result()
        task.cancel()
        done, pending = await asyncio.wait({task}, timeout=5.0)
        assert not pending
        with pytest.raises(asyncio.CancelledError):
            task.result()
        assert worker_stopped.is_set()
        assert presence.closed
    finally:
        release_worker.set()
        worker_ready.cancel()
        if waiter_task is not None:
            waiter_task.cancel()
        task.cancel()
        done, pending = await asyncio.wait({task}, timeout=5.0)
        assert task in done and not pending
        if waiter_task is not None:
            done, pending = await asyncio.wait({waiter_task}, timeout=5.0)
            assert waiter_task in done and not pending
            await asyncio.gather(waiter_task, return_exceptions=True)
        done, pending = await asyncio.wait({worker_ready}, timeout=5.0)
        assert worker_ready in done and not pending
        await asyncio.gather(worker_ready, return_exceptions=True)
        await asyncio.gather(task, return_exceptions=True)


@_async_test
async def test_fresh_restart_cursor_ahead_at_write_watermark_publishes_prefix_and_drains(tmp_path: Path) -> None:
    _seed_streaming_partial(tmp_path, count=2, persisted=1)
    store = _staging_store(tmp_path, _capture_root(tmp_path))
    attempt_id = store.pending_attempts()[0].attempt_id
    store.preflight_storage()
    store.initialize_confirmed_loss_ledger()
    session = ScriptedRingSession(
        _status(),
        (
            WriteStep(b"\x10", (_info(103, 103),)),
            WriteStep(b"\x10", (_info(103, 103),)),
        ),
    )

    result = await run_opportunistic_collector(Provider([session]), store, _options())

    assert isinstance(result, CollectionResult)
    assert session.writes == [b"\x10", b"\x10"]
    assert encode_advance_command(102) not in session.writes
    assert result.advance_confirmed is False
    source = _attempt_source(store, attempt_id)
    assert source is not None
    assert source.lifecycle == "sealed_waiting"
    assert (source.sequence_start, source.sequence_limit, source.committed_count) == (100, 102, 1)
    assert _source_records(store, attempt_id) == _record(100)


@_async_test
async def test_three_storage_not_ready_infos_reconnect_before_read(tmp_path: Path) -> None:
    config = CollectorConfig(
        retry=RetryConfig(
            rapid_backoff=(0.007,),
            storage_not_ready_backoff=(0.125, 0.25),
            max_storage_not_ready_responses=2,
        )
    )
    first = ScriptedRingSession(
        _status(),
        (
            WriteStep(b"\x10", (b"\x01\x09",)),
            WriteStep(b"\x10", (b"\x01\x09",)),
            WriteStep(b"\x10", (b"\x01\x09",)),
        ),
    )
    second = ScriptedRingSession(
        _status(),
        (
            WriteStep(b"\x10", (_info(10, 11),)),
            WriteStep(encode_read_command(10, 1), (_begin(10, 1), _data(_record(10)), _done(11))),
            WriteStep(b"\x10", (_info(10, 11),)),
            WriteStep(encode_advance_command(11), (b"\x01\x00",)),
            WriteStep(b"\x10", (_info(11, 11),)),
        ),
    )
    clock = Clock()
    activity: list[ActivityEvent] = []

    class ClosingProvider(Provider):
        def __call__(self, _candidate: object | None = None):
            session = self.sessions.pop(0)

            @asynccontextmanager
            async def context() -> AsyncIterator[ScriptedRingSession]:
                try:
                    yield session
                finally:
                    await session.close()

            return context()

    result = await run_opportunistic_collector(
        ClosingProvider([first, second]),
        _staging_store(tmp_path, _capture_root(tmp_path)),
        OpportunisticOptions(
            _options().timeouts,
            RetryPolicy(backoff=config.retry.rapid_backoff, batch_records=1, stop_after_drained=True),
            activity=activity.append,
            clock=clock,
            sleep=clock.sleep,
            config=config,
        ),
    )

    assert isinstance(result, CollectionResult)
    assert result.packet_count == 1
    assert result.next_sequence == 11
    assert first.closed
    assert first.writes == [b"\x10", b"\x10"]
    assert all(write == b"\x10" for write in first.writes)
    assert second.closed
    assert second.writes == [b"\x10", encode_read_command(10, 1), b"\x10", encode_advance_command(11), b"\x10"]
    assert [event.retry_seconds for event in activity if event.state == "storage_wait"] == [0.125]
    assert [event.retry_seconds for event in activity if event.state == "away"] == [0.007]
    assert clock.value == pytest.approx(0.132)


@_async_test
@pytest.mark.parametrize(
    ("read", "write", "persisted", "error"),
    [
        (101, 102, 1, None),  # C=P: append only the remaining range.
        (102, 102, 1, None),  # C=E while P<E: publish, then continue at C.
        (100, 102, 2, None),  # P=E and C<E: seal the durable full prefix.
        (101, 101, 1, BatchUnavailableError),  # W<E: the bounded batch expired.
    ],
)
async def test_resume_cursor_matrix(
    tmp_path: Path, read: int, write: int, persisted: int, error: type[Exception] | None
) -> None:
    _seed_streaming_partial(tmp_path, count=2, persisted=persisted)
    store = _staging_store(tmp_path, _capture_root(tmp_path))
    store.preflight_storage()
    store.initialize_confirmed_loss_ledger()
    read_start = max(read, 100 + persisted)
    requested = 102 - read_start
    notifications = (_begin(read_start, requested), _data(_records(read_start, requested)), _done(102))
    steps = [WriteStep(b"\x10", (_info(read, write),))]
    if error is None and requested:
        steps.extend(
            [
                WriteStep(encode_read_command(read_start, requested), notifications),
                WriteStep(b"\x10", (_info(read, write),)),
                WriteStep(encode_advance_command(102), (b"\x01\x00",)),
                WriteStep(b"\x10", (_info(102, 102),)),
            ]
        )
    elif error is None and read == write and persisted == 1:
        steps.append(WriteStep(b"\x10", (_info(read, write),)))
    elif error is None:
        steps.extend(
            [
                WriteStep(b"\x10", (_info(read, write),)),
                WriteStep(encode_advance_command(102), (b"\x01\x00",)),
                WriteStep(b"\x10", (_info(102, 102),)),
            ]
        )
    session = ScriptedRingSession(_status(), tuple(steps))

    if error is not None:
        provider = Provider([session])
        result = await run_opportunistic_collector(provider, store, _options())
        assert isinstance(result, NoDataResult)
        assert provider.opened == 1
        return

    activity: list[ActivityEvent] = []
    try:
        result = await run_opportunistic_collector(
            Provider([session]),
            store,
            _options(activity=activity),
        )
    except IndexError as cause:
        failures = [event for event in activity if event.state == "session_error"]
        raise AssertionError(f"unexpected retry after {failures!r}; writes={session.writes!r}") from cause

    assert isinstance(result, CollectionResult)
    if requested:
        assert session.writes[1] == encode_read_command(read_start, requested)
        assert session.writes[3] == encode_advance_command(102)
    else:
        assert read in (100, 102)


@_async_test
async def test_aligned_raw_tail_is_promoted_before_resume_arena_is_bound(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = _staging_store(tmp_path, _capture_root(tmp_path))

    def seed() -> str:
        attempt = store.prepare_streaming_attempt(100, 3)
        attempt.prepare_leg(100, 3)
        attempt.record_read_begin(ReadBeginNotification(100, 3))
        attempt.accept_chunk(100, _record(100))
        attempt.checkpoint()
        attempt.accept_chunk(101, _record(101))
        attempt.close(durable=True)
        return attempt.attempt_id

    attempt_id = storage_call(seed)

    original_arena = batch_reconciliation.TransferArena
    checkpoint_counts_at_allocation: list[int] = []

    def observe_arena(
        start_sequence: int,
        total_records: int,
        *,
        max_bytes: int,
        record_size: int = RECORD_SIZE,
    ) -> TransferArena:
        source = _attempt_source(store, attempt_id)
        assert source is not None
        checkpoint_counts_at_allocation.append(source.committed_count)
        return original_arena(start_sequence, total_records, max_bytes=max_bytes, record_size=record_size)

    monkeypatch.setattr(batch_reconciliation, "TransferArena", observe_arena)
    session = ScriptedRingSession(
        _status(),
        (
            WriteStep(b"\x10", (_info(102, 103),)),
            WriteStep(encode_read_command(102, 1), (_begin(102, 1), _data(_record(102)), _done(103))),
            WriteStep(b"\x10", (_info(102, 103),)),
            WriteStep(encode_advance_command(103), (b"\x01\x00",)),
            WriteStep(b"\x10", (_info(103, 103),)),
        ),
    )

    result = await run_opportunistic_collector(Provider([session]), store, _options(batch_records=3))

    assert isinstance(result, CollectionResult)
    assert result.next_sequence == 103
    assert checkpoint_counts_at_allocation == [2]
    assert session.writes[1] == encode_read_command(102, 1)


@_async_test
async def test_pending_cursor_ahead_publishes_prefix_and_reads_from_current_cursor(tmp_path: Path) -> None:
    _seed_streaming_partial(tmp_path, count=2, persisted=1)
    store = _staging_store(tmp_path, _capture_root(tmp_path))
    attempt_id = store.pending_attempts()[0].attempt_id
    store.preflight_storage()
    store.initialize_confirmed_loss_ledger()
    session = ScriptedRingSession(
        _status(),
        (
            WriteStep(b"\x10", (_info(103, 104),)),
            WriteStep(b"\x10", (_info(103, 104),)),
            WriteStep(encode_read_command(103, 1), (_begin(103, 1), _data(_record(103)), _done(104))),
            WriteStep(b"\x10", (_info(103, 104),)),
            WriteStep(encode_advance_command(104), (b"\x01\x00",)),
            WriteStep(b"\x10", (_info(104, 104),)),
        ),
    )

    result = await run_opportunistic_collector(Provider([session]), store, _options())

    assert isinstance(result, CollectionResult)
    assert encode_read_command(103, 1) in session.writes
    assert not store.pending_attempts()
    source = _attempt_source(store, attempt_id)
    assert source is not None
    assert source.lifecycle == "sealed_waiting"
    assert _source_records(store, attempt_id) == _record(100)


@_async_test
async def test_live_cursor_ahead_publishes_prefix_event_and_reads_remaining_audio(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _seed_streaming_partial(tmp_path, count=4, persisted=0)
    store = _staging_store(tmp_path, _capture_root(tmp_path))
    empty_source_id = store.pending_attempts()[0].attempt_id
    store.preflight_storage()
    store.initialize_confirmed_loss_ledger()
    activity: list[ActivityEvent] = []
    operational_events: list[dict[str, object]] = []
    journal = _quality_metrics(store.state_db)

    def fail_operational(event: Mapping[str, object]) -> None:
        operational_events.append(dict(event))
        raise RuntimeError("observer unavailable")

    session = ScriptedRingSession(
        _status(),
        (
            WriteStep(b"\x10", (_info(101, 104),)),
            WriteStep(b"\x10", (_info(101, 104),)),
            WriteStep(
                encode_read_command(101, 3),
                (_begin(101, 3), _data(_records(101, 3)), _done(104)),
            ),
            WriteStep(b"\x10", (_info(101, 104),)),
            WriteStep(encode_advance_command(104), (b"\x01\x00",)),
            WriteStep(b"\x10", (_info(104, 104),)),
        ),
    )

    result = await run_opportunistic_collector(
        Provider([session]),
        store,
        replace(
            _options(activity=activity, batch_records=4),
            operational=fail_operational,
            quality_metrics=journal,
            host_time=lambda: 1000.0,
        ),
    )

    assert isinstance(result, CollectionResult)
    assert encode_read_command(101, 3) in session.writes
    assert ActivityEvent("prefix_retired") in activity
    empty_source = _attempt_source(store, empty_source_id)
    assert empty_source is not None
    assert (empty_source.source_type, empty_source.lifecycle, empty_source.committed_count) == (
        "quarantine",
        "blocked",
        0,
    )

    def read_terminal() -> tuple[object, object] | None:
        with closing(store.state_db.open_connection(read_only=True)) as connection:
            return cast(
                tuple[object, object] | None,
                connection.execute(
                    "SELECT quarantine_disposition, retention_deadline FROM capture_source WHERE source_id = ?",
                    (empty_source_id,),
                ).fetchone(),
            )

    terminal = storage_call(read_terminal)
    assert terminal is not None and terminal[0] == "unprocessable" and terminal[1] is not None
    assert any(event.get("event") == "loss_detected" for event in operational_events)
    metrics = _quality_events(journal, store.state_db)
    loss = next(item for item in metrics if item["event"] == "sequence_loss")
    loss_id = loss["loss_id"]
    assert isinstance(loss_id, str) and len(loss_id) == 64
    assert all(character in "0123456789abcdef" for character in loss_id)
    assert loss == {
        "schema_version": 2,
        "event": "sequence_loss",
        "occurred_at": "1970-01-01T00:16:40.000+00:00",
        "session_id": loss["session_id"],
        "loss_id": loss_id,
        "missing_record_count": 1,
        "missing_raw_bytes": RECORD_SIZE,
        "reason": "device_cursor_advanced_before_host_durable_prefix",
        "release_version": "test-version",
        "source_revision": "a" * 12,
        "firmware_version": None,
    }
    deadline = int(cast(str, terminal[1]))
    monkeypatch.setattr("omi_collector.capture.adapters.quarantine._wall_clock_ns", lambda: deadline)
    sweep = storage_call(store.sweep_terminal_quarantine)
    assert sweep.removed_source_ids == (empty_source_id,)
    assert _attempt_source(store, empty_source_id) is None


@_async_test
async def test_quality_metrics_failure_does_not_change_completed_audio_collection(tmp_path: Path) -> None:
    class FailingMetrics:
        release_version = "test-version"
        source_revision = None

        def record_advertisement(self, _metric: object) -> None:
            raise OSError("quality filesystem unavailable")

        def record_transfer_session(self, _metric: object) -> None:
            raise OSError("quality filesystem unavailable")

        def record_sequence_loss(self, _metric: object) -> None:
            raise OSError("quality filesystem unavailable")

    session = ScriptedRingSession(
        _status(),
        (
            WriteStep(b"\x10", (_info(10, 11),)),
            WriteStep(encode_read_command(10, 1), (_begin(10, 1), _data(_record(10)), _done(11))),
        ),
    )
    clock = Clock()
    options = OpportunisticOptions(
        _options().timeouts,
        RetryPolicy(backoff=(0.001,), batch_records=1, advance_enabled=False, stop_after_drained=True),
        clock=clock,
        sleep=clock.sleep,
        host_time=lambda: 1000.0,
        quality_metrics=FailingMetrics(),  # type: ignore[arg-type]
    )

    result = await run_opportunistic_collector(
        Provider([session]), _staging_store(tmp_path, _capture_root(tmp_path)), options
    )

    assert isinstance(result, CollectionResult)
    assert result.packet_count == 1


@_async_test
async def test_completed_read_emits_one_terminal_transfer_session_with_raw_counters(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = _staging_store(tmp_path, _capture_root(tmp_path))
    journal = _quality_metrics(store.state_db)
    metric_threads: list[threading.Thread] = []
    original_put = journal._put_quality_event

    def observe_put(item_id: str, value: dict[str, object]) -> None:
        metric_threads.append(threading.current_thread())
        original_put(item_id, value)

    monkeypatch.setattr(journal, "_put_quality_event", observe_put)
    session = ScriptedRingSession(
        _status(),
        (
            WriteStep(b"\x10", (_info(10, 11),)),
            WriteStep(encode_read_command(10, 1), (_begin(10, 1), _data(_record(10)), _done(11))),
        ),
    )
    clock = Clock()
    options = OpportunisticOptions(
        _options().timeouts,
        RetryPolicy(backoff=(0.001,), batch_records=1, advance_enabled=False, stop_after_drained=True),
        clock=clock,
        sleep=clock.sleep,
        host_time=lambda: 1000.0,
        quality_metrics=journal,
        phy_policy="force_1m",
    )

    result = await run_opportunistic_collector(Provider([session]), store, options)

    assert isinstance(result, CollectionResult)
    [metric] = _quality_events(journal, store.state_db)
    assert metric["event"] == "transfer_session"
    assert metric["completed_at"] == "1970-01-01T00:16:40.000+00:00"
    assert metric["outcome"] == "collected"
    assert metric["termination_class"] == "completed"
    assert metric["requested_record_count"] == 1
    assert metric["record_size_bytes"] == RECORD_SIZE
    assert metric["received_raw_bytes"] == RECORD_SIZE
    assert metric["submitted_raw_bytes"] == RECORD_SIZE
    assert metric["written_raw_bytes"] == RECORD_SIZE
    assert metric["phy_policy"] == "force_1m"
    assert metric["advertisement_rssi_dbm"] is None
    assert metric_threads
    assert metric_threads[0] is not threading.main_thread()


@_async_test
async def test_real_ring_cursor_ahead_publishes_hash_bound_prefix_and_continues(tmp_path: Path) -> None:
    start, end, prefix_end, cursor, write = 1560520, 1564616, 1561108, 1561130, 1570627
    _seed_streaming_partial(tmp_path, count=end - start, persisted=prefix_end - start, start=start)
    store = _staging_store(tmp_path, _capture_root(tmp_path))
    attempt_id = store.pending_attempts()[0].attempt_id
    store.preflight_storage()
    store.initialize_confirmed_loss_ledger()
    operational_events: list[dict[str, object]] = []

    def emit_operational(event: Mapping[str, object]) -> None:
        operational_events.append(dict(event))

    session = _real_ring_gap_session(cursor, write)

    result = await run_opportunistic_collector(
        Provider([session]),
        store,
        replace(_options(batch_records=4096), operational=emit_operational),
    )
    assert isinstance(result, CollectionResult)

    prefix_bytes = _records(start, prefix_end - start)
    prefix_hash = sha256(prefix_bytes).hexdigest()
    source = _attempt_source(store, attempt_id)
    assert source is not None
    assert source.lifecycle == "sealed_waiting"
    assert (source.sequence_start, source.committed_count, source.committed_sha256) == (
        start,
        prefix_end - start,
        prefix_hash,
    )
    assert _source_records(store, attempt_id) == prefix_bytes
    loss_events = [event for event in operational_events if event.get("event") == "loss_detected"]
    assert loss_events == [
        {
            "event": "loss_detected",
            "missing_record_count": 22,
            "missing_raw_bytes": 22 * RECORD_SIZE,
            "reason": "device cursor advanced before host durable prefix",
        }
    ]


@_async_test
async def test_durable_prefix_resume_never_replays_old_records(tmp_path: Path) -> None:
    _seed_streaming_partial(tmp_path, count=2, persisted=1)
    operational_events: list[dict[str, object]] = []

    def emit_operational(event: Mapping[str, object]) -> None:
        operational_events.append(dict(event))

    session = ScriptedRingSession(
        _status(),
        (
            WriteStep(b"\x10", (_info(100, 102),)),
            WriteStep(encode_read_command(101, 1), (_begin(101, 1), _data(_record(101)), _done(102))),
            WriteStep(b"\x10", (_info(100, 102),)),
            WriteStep(encode_advance_command(102), (b"\x01\x00",)),
            WriteStep(b"\x10", (_info(102, 102),)),
        ),
    )

    result = await run_opportunistic_collector(
        Provider([session]),
        _staging_store(tmp_path, _capture_root(tmp_path)),
        replace(_options(), operational=emit_operational),
    )

    assert isinstance(result, CollectionResult)
    assert session.writes[1] == encode_read_command(101, 1)
    assert not [event for event in operational_events if event.get("event") == "loss_detected"]


@_async_test
async def test_clean_drain_disconnects_before_exactly_300_second_cooldown(tmp_path: Path) -> None:
    events: list[object] = []
    clock = Clock()

    class Context:
        async def __aenter__(self) -> ScriptedRingSession:
            events.append("connect")
            return ScriptedRingSession(_status(), (WriteStep(b"\x10", (_info(100, 100),)),))

        async def __aexit__(self, _type: object, _value: object, _traceback: object) -> None:
            events.append("disconnect")

    class StopAfterSleepError(Exception):
        pass

    def sleep(delay: float) -> None:
        events.append(delay)
        raise StopAfterSleepError

    with pytest.raises(StopAfterSleepError):
        await run_opportunistic_collector(
            lambda _candidate: Context(),
            _staging_store(tmp_path, _capture_root(tmp_path)),
            OpportunisticOptions(
                TransferTimeouts(1, 1),
                RetryPolicy(backoff=(1,), drain_cooldown_seconds=300),
                clock=clock,
                sleep=sleep,
            ),
        )

    assert events == ["connect", "disconnect", 300]


@_async_test
async def test_clean_drain_reports_full_cooldown_after_callback_work(tmp_path: Path) -> None:
    activity: list[ActivityEvent] = []
    clock = Clock()
    cooldown_seconds = 7.5
    callback_elapsed_seconds = 2.0
    sleeps: list[float] = []

    class Context:
        async def __aenter__(self) -> ScriptedRingSession:
            return ScriptedRingSession(_status(), (WriteStep(b"\x10", (_info(100, 100),)),))

        async def __aexit__(self, _type: object, _value: object, _traceback: object) -> None:
            return None

    class StopAfterSleepError(Exception):
        pass

    def sleep(delay: float) -> None:
        sleeps.append(delay)
        raise StopAfterSleepError

    async def report_activity(event: ActivityEvent) -> None:
        activity.append(event)
        if event.state == "cooldown_started":
            clock.value += callback_elapsed_seconds

    with pytest.raises(StopAfterSleepError):
        await run_opportunistic_collector(
            lambda _candidate: Context(),
            _staging_store(tmp_path, _capture_root(tmp_path)),
            OpportunisticOptions(
                TransferTimeouts(1, 1),
                RetryPolicy(backoff=(1,), drain_cooldown_seconds=cooldown_seconds),
                activity=report_activity,
                clock=clock,
                sleep=sleep,
            ),
        )

    cooldowns = [event for event in activity if event.state == "cooldown_started"]
    assert len(cooldowns) == 1
    assert cooldowns[0].reason == "clean_drain"
    assert cooldowns[0].duration_seconds == cooldown_seconds
    assert cooldowns[0].next_attempt_in_seconds == cooldown_seconds
    assert clock.value == callback_elapsed_seconds
    assert sleeps == [cooldown_seconds]


@_async_test
async def test_presence_clean_drain_reports_actual_remaining_cooldown(tmp_path: Path) -> None:
    clock = Clock()
    activity: list[ActivityEvent] = []
    cooldown_seconds = 9.0
    elapsed_during_scan_start = 2.0
    visibility_elapsed = 0.001

    class Observer:
        async def start(self, callback: PresenceCallback) -> None:
            callback(object())
            clock.value += visibility_elapsed
            callback(object())

        async def stop(self) -> None:
            return None

    class Context:
        async def __aenter__(self) -> ScriptedRingSession:
            return ScriptedRingSession(_status(), (WriteStep(b"\x10", (_info(100, 100),)),))

        async def __aexit__(self, _type: object, _value: object, _traceback: object) -> None:
            return None

    class StopAfterCooldownError(Exception):
        pass

    async def report_activity(event: ActivityEvent) -> None:
        if event.state == "drained":
            clock.value += elapsed_during_scan_start
            return
        if event.state == "cooldown_started":
            activity.append(event)
            raise StopAfterCooldownError

    presence = PresenceScheduler(
        Observer(),
        policy=PresencePolicy(
            scan_recheck_seconds=1.0,
            drain_cooldown_seconds=cooldown_seconds,
            arrival_stability_seconds=0.0005,
            arrival_max_gap_seconds=1.0,
            rapid_backoff=(1.0,),
        ),
        clock=clock,
        sleep=clock.sleep,
    )
    watchdog = asyncio.timeout(5.0)
    try:
        with pytest.raises(StopAfterCooldownError):
            async with watchdog:
                await run_opportunistic_collector(
                    lambda _candidate: Context(),
                    _staging_store(tmp_path, _capture_root(tmp_path)),
                    OpportunisticOptions(
                        TransferTimeouts(1, 1),
                        RetryPolicy(backoff=(1,), drain_cooldown_seconds=cooldown_seconds),
                        activity=report_activity,
                        clock=clock,
                        sleep=clock.sleep,
                        presence=presence,
                    ),
                )
        assert not watchdog.expired()
    finally:
        await presence.close()

    cooldowns = [event for event in activity if event.state == "cooldown_started"]
    assert len(cooldowns) == 1
    assert cooldowns[0].reason == "clean_drain"
    assert cooldowns[0].duration_seconds == cooldown_seconds
    assert cooldowns[0].next_attempt_in_seconds == cooldown_seconds - elapsed_during_scan_start - visibility_elapsed


@_async_test
async def test_absence_backoff_stops_at_30_and_never_uses_drain_cooldown(tmp_path: Path) -> None:
    clock = Clock()
    activity: list[ActivityEvent] = []
    calls = 0

    def provider(_candidate: object | None = None) -> AbstractAsyncContextManager[ScriptedRingSession]:
        nonlocal calls
        calls += 1
        if calls <= 6:
            raise RingTransportUnavailableError("away")

        @asynccontextmanager
        async def context() -> AsyncIterator[ScriptedRingSession]:
            yield ScriptedRingSession(_status(), (WriteStep(b"\x10", (_info(100, 100),)),))

        return context()

    result = await run_opportunistic_collector(
        provider,
        _staging_store(tmp_path, _capture_root(tmp_path)),
        OpportunisticOptions(
            TransferTimeouts(1, 1),
            RetryPolicy(backoff=(1, 2, 4, 8, 16, 30), stop_after_drained=True),
            activity=activity.append,
            clock=clock,
            sleep=clock.sleep,
        ),
    )

    assert isinstance(result, NoDataResult)
    assert [event.retry_seconds for event in activity if event.state == "away"] == [1, 2, 4, 8, 16, 30]
    assert clock.value == 61
    assert 300 not in [event.retry_seconds for event in activity if event.state == "away"]


@_async_test
async def test_notification_overflow_checkpoints_and_reconnects_with_fresh_info(tmp_path: Path) -> None:
    first = ScriptedRingSession(
        _status(),
        (
            WriteStep(b"\x10", (_info(10, 12),)),
            WriteStep(
                encode_read_command(10, 2),
                (_begin(10, 2), _data(_record(10)), NotificationOverflowError("buffer overflowed")),
            ),
            WriteStep(encode_stop_command()),
        ),
    )
    second = ScriptedRingSession(
        _status(),
        (
            WriteStep(b"\x10", (_info(10, 12),)),
            WriteStep(encode_read_command(11, 1), (_begin(11, 1), _data(_record(11)), _done(12))),
            WriteStep(b"\x10", (_info(10, 12),)),
            WriteStep(encode_advance_command(12), (b"\x01\x00",)),
            WriteStep(b"\x10", (_info(12, 12),)),
        ),
    )

    class ClosingProvider(Provider):
        def __call__(self, _candidate: object | None = None):
            session = self.sessions.pop(0)

            @asynccontextmanager
            async def context() -> AsyncIterator[ScriptedRingSession]:
                try:
                    yield session
                finally:
                    await session.close()

            return context()

    result = await run_opportunistic_collector(
        ClosingProvider([first, second]), _staging_store(tmp_path, _capture_root(tmp_path)), _options()
    )

    assert isinstance(result, CollectionResult)
    assert result.packet_count == 2
    assert isinstance(result.seal, DurableSqliteSeal)
    assert result.advance_confirmed
    assert result.next_sequence == 12
    assert _sealed_records(_staging_store(tmp_path, _capture_root(tmp_path)), result.seal) == _records(10, 2)
    assert first.writes == [b"\x10", encode_read_command(10, 2), encode_stop_command()]
    assert first.closed
    assert second.closed
    assert second.writes == [b"\x10", encode_read_command(11, 1), b"\x10", encode_advance_command(12), b"\x10"]


@_async_test
async def test_presence_coordinator_forwards_exact_wake_candidate(tmp_path: Path) -> None:
    candidate = BLEDevice("AA:BB", "omi", object())
    session = ScriptedRingSession(_status(), (WriteStep(b"\x10", (_info(100, 100),)),))
    seen: list[object | None] = []

    class Observer:
        async def start(self, callback: Callable[[object], object]) -> None:
            callback(candidate)
            await asyncio.sleep(0)
            callback(candidate)

        async def stop(self) -> None:
            return None

    def provider(wake_candidate: object | None) -> AbstractAsyncContextManager[ScriptedRingSession]:
        seen.append(wake_candidate)

        @asynccontextmanager
        async def context() -> AsyncIterator[ScriptedRingSession]:
            yield session

        return context()

    presence = PresenceScheduler(
        Observer(),
        policy=PresencePolicy(
            scan_recheck_seconds=1,
            drain_cooldown_seconds=1,
            arrival_stability_seconds=0.000001,
            arrival_max_gap_seconds=1,
            rapid_backoff=(1,),
        ),
    )
    try:
        async with asyncio.timeout(5.0) as watchdog:
            result = await run_opportunistic_collector(
                provider,
                _staging_store(tmp_path, _capture_root(tmp_path)),
                OpportunisticOptions(
                    TransferTimeouts(1, 1),
                    RetryPolicy(backoff=(1,), drain_cooldown_seconds=1, stop_after_drained=True),
                    presence=presence,
                ),
            )
            assert not watchdog.expired()
    finally:
        await presence.close()

    assert isinstance(result, NoDataResult)
    assert seen == [candidate]


@_async_test
async def test_stale_candidate_restarts_scan_for_next_stable_candidate(tmp_path: Path) -> None:
    candidate_a = BLEDevice("AA:BB", "omi-a", object())
    candidate_b = BLEDevice("AA:BB", "omi-b", object())
    session = ScriptedRingSession(_status(), (WriteStep(b"\x10", (_info(100, 100),)),))
    seen: list[object | None] = []

    class Observer:
        starts = 0

        async def start(self, callback: Callable[[object], object]) -> None:
            candidate = candidate_a if self.starts == 0 else candidate_b
            self.starts += 1
            callback(candidate)
            await asyncio.sleep(0)
            callback(candidate)

        async def stop(self) -> None:
            return None

    def provider(candidate: object | None) -> AbstractAsyncContextManager[ScriptedRingSession]:
        seen.append(candidate)

        @asynccontextmanager
        async def context() -> AsyncIterator[ScriptedRingSession]:
            if candidate is candidate_a:
                raise CandidateUnavailableError("stale scanner candidate")
            yield session

        return context()

    presence = PresenceScheduler(
        Observer(),
        policy=PresencePolicy(
            scan_recheck_seconds=1,
            drain_cooldown_seconds=1,
            arrival_stability_seconds=0.000001,
            arrival_max_gap_seconds=1,
            rapid_backoff=(1,),
        ),
    )
    try:
        async with asyncio.timeout(5.0) as watchdog:
            result = await run_opportunistic_collector(
                provider,
                _staging_store(tmp_path, _capture_root(tmp_path)),
                OpportunisticOptions(
                    TransferTimeouts(1, 1),
                    RetryPolicy(backoff=(1,), drain_cooldown_seconds=1, stop_after_drained=True),
                    presence=presence,
                ),
            )
            assert not watchdog.expired()
    finally:
        await presence.close()

    assert isinstance(result, NoDataResult)
    assert seen == [candidate_a, candidate_b]


async def _run_teardown_failure_case(tmp_path: Path, *, with_batch: bool) -> None:
    clock = Clock()
    opened = 0

    class Observer:
        async def start(self, callback: Callable[[object], object]) -> None:
            callback(object())
            await asyncio.sleep(0)
            clock.value += 0.001
            callback(object())
            await asyncio.sleep(0)
            clock.value += 0.001
            callback(object())

        async def stop(self) -> None:
            pass

    if with_batch:
        first = ScriptedRingSession(
            _status(),
            (
                WriteStep(b"\x10", (_info(100, 101),)),
                WriteStep(encode_read_command(100, 1), (_begin(100, 1), _data(_record(100)), _done(101))),
                WriteStep(b"\x10", (_info(100, 101),)),
                WriteStep(encode_advance_command(101), (b"\x01\x00",)),
                WriteStep(b"\x10", (_info(101, 101),)),
            ),
        )
    else:
        first = ScriptedRingSession(_status(), (WriteStep(b"\x10", (_info(100, 100),)),))
    second = ScriptedRingSession(_status(), (WriteStep(b"\x10", (_info(101, 101),)),))

    def provider(_candidate: object | None = None) -> AbstractAsyncContextManager[ScriptedRingSession]:
        nonlocal opened
        opened += 1
        session = first if opened == 1 else second

        @asynccontextmanager
        async def context() -> AsyncIterator[ScriptedRingSession]:
            yield session
            if opened == 1:
                raise RingTransportUnavailableError("disconnect cleanup failed")

        return context()

    presence = PresenceScheduler(
        Observer(),
        policy=PresencePolicy(
            scan_recheck_seconds=30,
            drain_cooldown_seconds=30,
            arrival_stability_seconds=0.0005,
            arrival_max_gap_seconds=1,
            rapid_backoff=(0.0005,),
        ),
        clock=clock,
        sleep=clock.sleep,
    )
    timeouts = _options().timeouts if with_batch else TransferTimeouts(1, 1)
    try:
        async with asyncio.timeout(5.0) as watchdog:
            result = await run_opportunistic_collector(
                provider,
                _staging_store(tmp_path, _capture_root(tmp_path)),
                OpportunisticOptions(
                    timeouts,
                    RetryPolicy(
                        backoff=(0.0005,),
                        batch_records=1,
                        drain_cooldown_seconds=30,
                        stop_after_drained=True,
                    ),
                    clock=clock,
                    sleep=clock.sleep,
                    presence=presence,
                ),
            )
            assert not watchdog.expired()
    finally:
        await presence.close()

    assert isinstance(result, (NoDataResult, CollectionResult))
    assert opened == 2
    assert clock.value < 1


@_async_test
async def test_teardown_failure_after_info_uses_first_rapid_retry(tmp_path: Path) -> None:
    await _run_teardown_failure_case(tmp_path, with_batch=False)


@_async_test
async def test_teardown_failure_after_completed_batch_resets_rapid_retry(tmp_path: Path) -> None:
    await _run_teardown_failure_case(tmp_path, with_batch=True)


@_async_test
async def test_three_batches_share_one_presence_no_double_info_and_defer_tail(tmp_path: Path) -> None:
    session = ScriptedRingSession(
        _status(),
        (
            WriteStep(b"\x10", (_info(10, 14),)),
            WriteStep(encode_read_command(10, 2), (_begin(10, 2), _data(_records(10, 2)), _done(12))),
            WriteStep(b"\x10", (_info(10, 14),)),
            WriteStep(encode_advance_command(12), (b"\x01\x00",)),
            WriteStep(b"\x10", (_info(12, 16),)),
            WriteStep(encode_read_command(12, 2), (_begin(12, 2), _data(_records(12, 2)), _done(14))),
            WriteStep(b"\x10", (_info(12, 16),)),
            WriteStep(encode_advance_command(14), (b"\x01\x00",)),
            WriteStep(b"\x10", (_info(14, 16),)),
            WriteStep(encode_read_command(14, 2), (_begin(14, 2), _data(_records(14, 2)), _done(16))),
            WriteStep(b"\x10", (_info(14, 16),)),
            WriteStep(encode_advance_command(16), (b"\x01\x00",)),
            WriteStep(b"\x10", (_info(16, 16),)),
        ),
    )

    provider = Provider([session])
    result = await run_opportunistic_collector(provider, _staging_store(tmp_path, _capture_root(tmp_path)), _options())

    assert isinstance(result, CollectionResult)
    assert result.next_sequence == 16
    assert provider.opened == 1
    assert session.writes == [
        b"\x10",
        encode_read_command(10, 2),
        b"\x10",
        encode_advance_command(12),
        b"\x10",
        encode_read_command(12, 2),
        b"\x10",
        encode_advance_command(14),
        b"\x10",
        encode_read_command(14, 2),
        b"\x10",
        encode_advance_command(16),
        b"\x10",
    ]


@_async_test
async def test_repeated_short_visits_accumulate_durable_prefix_until_snapshot_publishes(tmp_path: Path) -> None:
    first = ScriptedRingSession(
        _status(),
        (
            WriteStep(b"\x10", (_info(10, 16),)),
            WriteStep(
                encode_read_command(10, 6),
                (_begin(10, 6), _data(_records(10, 2)), RingTransportDisconnectedError("visit ended")),
            ),
            WriteStep(encode_stop_command()),
        ),
    )
    second = ScriptedRingSession(
        _status(),
        (
            WriteStep(b"\x10", (_info(10, 16),)),
            WriteStep(
                encode_read_command(12, 4),
                (_begin(12, 4), _data(_records(12, 2)), RingTransportDisconnectedError("visit ended")),
            ),
            WriteStep(encode_stop_command()),
        ),
    )
    third = ScriptedRingSession(
        _status(),
        (
            WriteStep(b"\x10", (_info(10, 16),)),
            WriteStep(encode_read_command(14, 2), (_begin(14, 2), _data(_records(14, 2)), _done(16))),
            WriteStep(b"\x10", (_info(10, 16),)),
            WriteStep(encode_advance_command(16), (b"\x01\x00",)),
            WriteStep(b"\x10", (_info(16, 16),)),
        ),
    )
    capture_root = _capture_root(tmp_path)
    durable_after_visit: list[int] = []
    published_after_visit: list[int] = []

    store = _staging_store(tmp_path, capture_root)

    def observe(event: ActivityEvent) -> None:
        if event.state != "away":
            return
        attempts = store.pending_attempts()
        assert len(attempts) == 1
        source = _attempt_source(store, attempts[0].attempt_id)
        assert source is not None
        durable_after_visit.append(source.committed_count)
        published_after_visit.append(int(source.lifecycle == "sealed_waiting"))

    provider = Provider([first, second, third])
    result = await run_opportunistic_collector(
        provider,
        store,
        replace(_options(batch_records=6), activity=observe),
    )

    assert isinstance(result, CollectionResult)
    assert isinstance(result.seal, DurableSqliteSeal)
    assert result.next_sequence == 16
    assert provider.opened == 3
    assert durable_after_visit == [2, 4]
    assert published_after_visit == [0, 0]
    assert result.seal.start_sequence == 10
    assert result.seal.next_sequence == 16


@_async_test
async def test_default_preflight_skips_cached_status_without_telemetry(tmp_path: Path) -> None:
    class HangingStatusSession(ScriptedRingSession):
        async def read_status(self) -> RingStatus:
            self.status_reads += 1
            await asyncio.Future()
            raise AssertionError("hanging status read unexpectedly completed")

    session = HangingStatusSession(
        _status(),
        (
            WriteStep(b"\x10", (_info(10, 11),)),
            WriteStep(encode_read_command(10, 1), (_begin(10, 1), _data(_record(10)), _done(11))),
            WriteStep(b"\x10", (_info(10, 11),)),
            WriteStep(encode_advance_command(11), (b"\x01\x00",)),
            WriteStep(b"\x10", (_info(11, 11),)),
        ),
    )
    options = OpportunisticOptions(
        _options().timeouts,
        policy=RetryPolicy(backoff=(0.001,), batch_records=1, stop_after_drained=True),
    )

    result = await run_opportunistic_collector(
        Provider([session]), _staging_store(tmp_path, _capture_root(tmp_path)), options
    )

    assert isinstance(result, CollectionResult)
    assert session.status_reads == 0
    assert session.writes[0] == b"\x10"


@_async_test
async def test_ble_optional_reads_keep_operation_bounds_without_preflight_storage_deadline(
    tmp_path: Path,
) -> None:
    config = CollectorConfig(
        retry=RetryConfig(presence_preflight_budget_seconds=0.001),
        telemetry=TelemetryConfig(optional_operation_timeout_seconds=0.02),
    )
    order: list[str] = []
    cancellation: list[str] = []

    class SlowPreflightSession(ScriptedRingSession):
        async def read_status(self) -> RingStatus:
            order.append("status")
            try:
                await asyncio.sleep(config.telemetry.optional_operation_timeout_seconds * 0.75)
                return self._status
            finally:
                cancellation.append("status")

        async def read_optional_characteristic(self, uuid: str) -> bytes | None:
            del uuid
            order.append("optional")
            try:
                await asyncio.Future()
                return None
            finally:
                cancellation.append("optional")

        async def write_control(self, payload: bytes) -> None:
            if payload == b"\x10":
                order.append("info")
            await super().write_control(payload)

    session = SlowPreflightSession(_status(), (WriteStep(b"\x10", (_info(10, 10),)),))

    async def emit(_event: Mapping[str, object]) -> None:
        await asyncio.sleep(0.01)

    result = await asyncio.wait_for(
        run_opportunistic_collector(
            Provider([session]),
            _staging_store(tmp_path, _capture_root(tmp_path)),
            OpportunisticOptions(
                TransferTimeouts(info=30.0, transfer=600.0),
                policy=RetryPolicy(backoff=(0.001,), stop_after_drained=True),
                operational=emit,
                host_clock_synchronized=lambda _timeout: False,
                config=config,
            ),
        ),
        timeout=0.5,
    )

    assert isinstance(result, NoDataResult)
    assert order[:3] == ["info", "optional", "status"]
    assert "optional" in order
    assert cancellation.count("status") == 1
    assert cancellation.count("optional") >= 1


@_async_test
async def test_blocking_host_trust_probe_cannot_delay_later_gatt(tmp_path: Path) -> None:
    started = threading.Event()
    release = threading.Event()
    finished = threading.Event()
    config = CollectorConfig(retry=RetryConfig(presence_preflight_budget_seconds=1.0))

    class BlockingTrustSession(ScriptedRingSession):
        async def read_optional_characteristic(self, uuid: str) -> bytes | None:
            if uuid == TIME_READ_UUID:
                return pack("<I", 0)
            return None

    session = BlockingTrustSession(
        _status(),
        (
            WriteStep(b"\x10", (_info(10, 11),)),
            WriteStep(encode_read_command(10, 1), (_begin(10, 1), _data(_record(10)), _done(11))),
            WriteStep(b"\x10", (_info(10, 11),)),
            WriteStep(encode_advance_command(11), (b"\x01\x00",)),
            WriteStep(b"\x10", (_info(11, 11),)),
        ),
    )

    def blocking_trust_probe(timeout: float) -> bool:
        started.set()
        if not release.wait(timeout):
            finished.set()
            raise TimeoutError
        finished.set()
        return False

    collection = asyncio.create_task(
        run_opportunistic_collector(
            Provider([session]),
            _staging_store(tmp_path, _capture_root(tmp_path)),
            OpportunisticOptions(
                TransferTimeouts(info=30.0, transfer=600.0),
                policy=RetryPolicy(backoff=(0.001,), stop_after_drained=True),
                operational=lambda _event: None,
                host_clock_synchronized=blocking_trust_probe,
                config=config,
            ),
        )
    )
    try:
        assert await asyncio.wait_for(asyncio.to_thread(started.wait), timeout=5.0)
        # The callback honors its own supplied timeout and the joined worker
        # finishes before collection proceeds.
        result = await asyncio.wait_for(collection, timeout=5.0)
        assert not release.is_set()
        assert finished.is_set()
    finally:
        release.set()
        assert await asyncio.wait_for(asyncio.to_thread(finished.wait), timeout=5.0)

    assert isinstance(result, CollectionResult)
    assert session.writes == [
        b"\x10",
        encode_read_command(10, 1),
        b"\x10",
        encode_advance_command(11),
        b"\x10",
    ]


class _PublicationClockSession(ScriptedRingSession):
    def __init__(self, steps: tuple[WriteStep, ...], events: list[str]) -> None:
        super().__init__(_status(), steps)
        self.events = events
        self.clock_read = False

    async def read_optional_characteristic(self, uuid: str) -> bytes | None:
        if uuid == TIME_READ_UUID:
            self.clock_read = True
            return pack("<I", 10_000)
        return None

    async def write_control(self, payload: bytes) -> None:
        if payload == encode_read_command(10, 1):
            await asyncio.sleep(0.01)
            assert self.events == []
            self.events.append("read")
        await super().write_control(payload)


@_async_test
async def test_clock_telemetry_defers_publication_and_retry_until_after_capture_closure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    ready = tmp_path / "ready"
    ready.mkdir(mode=0o2750)
    ready.chmod(0o2750)
    config = CollectorConfig(ready=ReadyConfig(target_audio_seconds=0.02), retry=RetryConfig(rapid_backoff=(0.001,)))
    initial_store = _staging_store(tmp_path / "spool", _capture_root(tmp_path))
    store = StagingStore.from_paths(
        initial_store.paths,
        state_db=initial_store.state_db,
        publication_root=ready,
        io=StagingIO(config=config),
    )
    events: list[str] = []
    original_publish = store._recover_and_publish_unlocked
    original_closure = BatchReconciler.close_visit
    original_background = store.recover_and_publish
    published = threading.Event()
    failed = False

    def publish() -> ReadyOutcome:
        nonlocal failed
        if session.clock_read and not failed:
            assert events == ["read", "closure"]
            failed = True
            events.append("publication_transient")
            raise OSError("ready projection failed")
        return original_publish()

    def background() -> ReadyOutcome:
        if failed:
            events.append("retry")
        outcome = original_background()
        if outcome.state is ReadyOutcomeState.PUBLISHED:
            events.append("publication_success")
            published.set()
        return outcome

    async def close_visit(reconciler: BatchReconciler, reason: str, drain_cursor: int | None = None) -> None:
        await original_closure(reconciler, reason, drain_cursor)
        assert _ready_closure_rows(store) == [{"next_sequence": 11, "reason": "drained"}]
        events.append("closure")

    monkeypatch.setattr(store, "_recover_and_publish_unlocked", publish)
    monkeypatch.setattr(store, "recover_and_publish", background)
    monkeypatch.setattr(BatchReconciler, "close_visit", close_visit)

    session = _PublicationClockSession(
        (
            WriteStep(b"\x10", (_info(10, 11),)),
            WriteStep(
                encode_read_command(10, 1),
                (_begin(10, 1), _data(pack(">I", 10) + bytes((2, 8, 10)) + bytes(RECORD_SIZE - 7)), _done(11)),
            ),
            WriteStep(b"\x10", (_info(10, 11),)),
            WriteStep(encode_advance_command(11), (b"\x01\x00",)),
            WriteStep(b"\x10", (_info(11, 11),)),
        ),
        events,
    )

    async def activity(event: ActivityEvent) -> None:
        if event.state == "drained":
            assert await asyncio.to_thread(published.wait, 5)

    result = await run_opportunistic_collector(
        Provider([session]),
        store,
        replace(
            _options(batch_records=1),
            operational=lambda _event: None,
            host_time=lambda: 10_000.0,
            host_clock_synchronized=lambda _timeout: True,
            activity=activity,
            config=config,
        ),
    )

    assert isinstance(result, CollectionResult)
    assert session.clock_read
    clock_state = storage_call(lambda: ClockStateStore(store.state_db).state())
    assert clock_state.intent is None
    assert clock_state.anchor is not None
    assert clock_state.anchor.sequence_frontier == 11
    assert events[:5] == ["read", "closure", "publication_transient", "retry", "publication_success"]
    assert events[5:] in ([], ["retry"])
    assert len(tuple(ready.iterdir())) == 1


@_async_test
async def test_disconnect_next_day_replays_overlap_and_progress_counts_unique_bytes(tmp_path: Path) -> None:
    first = ScriptedRingSession(
        _status(),
        (
            WriteStep(b"\x10", (_info(10, 12),)),
            WriteStep(
                encode_read_command(10, 2),
                (_begin(10, 2), _data(_record(10)), RingTransportDisconnectedError("gone")),
            ),
            WriteStep(encode_stop_command()),
        ),
    )
    unavailable = [ScriptedRingSession(_status(), ()) for _ in range(3)]
    complete = ScriptedRingSession(
        _status(),
        (
            WriteStep(b"\x10", (_info(10, 12),)),
            WriteStep(encode_read_command(11, 1), (_begin(11, 1), _data(_record(11)), _done(12))),
            WriteStep(b"\x10", (_info(10, 12),)),
            WriteStep(encode_advance_command(12), (b"\x01\x00",)),
            WriteStep(b"\x10", (_info(12, 12),)),
        ),
    )
    clock = Clock()
    activity: list[ActivityEvent] = []
    progress: list[ProgressEvent] = []

    class AwayProvider(Provider):
        def __call__(self, candidate: object | None = None):
            if self.sessions and self.sessions[0] in unavailable:
                self.sessions.pop(0)
                raise RingTransportUnavailableError("away")
            return super().__call__(candidate)

    result = await run_opportunistic_collector(
        AwayProvider([first, *unavailable, complete]),
        _staging_store(tmp_path, _capture_root(tmp_path)),
        OpportunisticOptions(
            _options().timeouts,
            RetryPolicy(backoff=(1, 2, 4, 8, 16, 30), batch_records=2, stop_after_drained=True),
            progress=progress.append,
            activity=activity.append,
            clock=clock,
            sleep=clock.sleep,
        ),
    )

    assert isinstance(result, CollectionResult)
    assert complete.writes[1] == encode_read_command(11, 1)
    assert clock.value == 15
    assert [event.retry_seconds for event in activity if event.state == "away"] == [1, 2, 4, 8]
    assert progress[-1].records_completed == progress[-1].records_total == 2
    assert progress[-1].bytes_completed == 2 * RECORD_SIZE
    assert progress[-1].eta in (0.0, None)


@_async_test
async def test_reconnect_quality_counts_each_physical_leg_once(tmp_path: Path) -> None:
    first = ScriptedRingSession(
        _status(),
        (
            WriteStep(b"\x10", (_info(10, 12),)),
            WriteStep(
                encode_read_command(10, 2),
                (_begin(10, 2), _data(_record(10)), RingTransportDisconnectedError("gone")),
            ),
            WriteStep(encode_stop_command()),
        ),
    )
    complete = ScriptedRingSession(
        _status(),
        (
            WriteStep(b"\x10", (_info(10, 12),)),
            WriteStep(encode_read_command(11, 1), (_begin(11, 1), _data(_record(11)), _done(12))),
            WriteStep(b"\x10", (_info(10, 12),)),
            WriteStep(encode_advance_command(12), (b"\x01\x00",)),
            WriteStep(b"\x10", (_info(12, 12),)),
        ),
    )

    class ReadClock(Clock):
        def __call__(self) -> float:
            current = self.value
            self.value += 1
            return current

    store = _staging_store(tmp_path, _capture_root(tmp_path))
    journal = _quality_metrics(store.state_db)
    clock = ReadClock()
    result = await run_opportunistic_collector(
        Provider([first, complete]),
        store,
        OpportunisticOptions(
            _options().timeouts,
            RetryPolicy(backoff=(0.001,), batch_records=2, stop_after_drained=True),
            clock=clock,
            sleep=clock.sleep,
            host_time=lambda: 1000.0,
            quality_metrics=journal,
        ),
    )
    metrics = sorted(
        _quality_events(journal, store.state_db),
        key=lambda metric: metric["outcome"] != "connected_interrupted",
    )
    assert isinstance(result, CollectionResult)
    assert result.packet_count == 2
    assert [metric["requested_record_count"] for metric in metrics] == [2, 1]
    assert [metric["outcome"] for metric in metrics] == ["connected_interrupted", "drained"]
    assert [metric["termination_class"] for metric in metrics] == ["retryable_error", "completed"]
    assert [metric["received_raw_bytes"] for metric in metrics] == [RECORD_SIZE, RECORD_SIZE]
    assert [metric["submitted_raw_bytes"] for metric in metrics] == [RECORD_SIZE, RECORD_SIZE]
    assert [metric["written_raw_bytes"] for metric in metrics] == [RECORD_SIZE, RECORD_SIZE]
    assert [metric["active_read_elapsed_ms"] for metric in metrics] == [1000, 1000]
    assert sum(cast(int, metric["written_raw_bytes"]) for metric in metrics) == 2 * RECORD_SIZE
    assert (
        sum(cast(int, metric["written_raw_bytes"]) for metric in metrics)
        * 1000
        / sum(cast(int, metric["active_read_elapsed_ms"]) for metric in metrics)
        == RECORD_SIZE
    )


@_async_test
async def test_same_process_recovery_disconnect_rebinds_original_read_begin_for_gap(
    tmp_path: Path,
) -> None:
    first = ScriptedRingSession(
        _status(),
        (
            WriteStep(b"\x10", (_info(10, 13),)),
            WriteStep(
                encode_read_command(10, 3),
                (_begin(10, 3), _data(_records(10, 2)), RingTransportDisconnectedError("gone")),
            ),
            WriteStep(encode_stop_command()),
        ),
    )
    recovery = ScriptedRingSession(
        _status(),
        (
            WriteStep(b"\x10", (_info(10, 13),)),
            WriteStep(encode_read_command(12, 1), (_begin(12, 1), RingTransportDisconnectedError("gone"))),
            WriteStep(encode_stop_command()),
        ),
    )
    gapped = ScriptedRingSession(
        _status(),
        (
            WriteStep(b"\x10", (_info(13, 13),)),
            WriteStep(b"\x10", (_info(13, 13),)),
        ),
    )

    store = _staging_store(tmp_path, _capture_root(tmp_path))
    store.preflight_storage()
    store.initialize_confirmed_loss_ledger()
    result = await run_opportunistic_collector(
        Provider([first, recovery, gapped]),
        store,
        _options(batch_records=3),
    )

    assert isinstance(result, CollectionResult)
    assert recovery.writes[1] == encode_read_command(12, 1)
    assert gapped.writes == [b"\x10", b"\x10"]
    assert isinstance(result.seal, DurableSqliteSeal)
    assert result.seal.source_id


@_async_test
async def test_storage_not_ready_info_after_disconnect_retries_on_same_session(tmp_path: Path) -> None:
    first = ScriptedRingSession(
        _status(),
        (
            WriteStep(b"\x10", (_info(10, 12),)),
            WriteStep(
                encode_read_command(10, 2),
                (_begin(10, 2), _data(_record(10)), RingTransportDisconnectedError("gone")),
            ),
            WriteStep(encode_stop_command(), error=RingTransportDisconnectedError("gone")),
        ),
    )
    recovered = ScriptedRingSession(
        _status(),
        (
            WriteStep(b"\x10", (b"\x01\x09",)),
            WriteStep(b"\x10", (_info(10, 12),)),
            WriteStep(encode_read_command(11, 1), (_begin(11, 1), _data(_record(11)), _done(12))),
            WriteStep(b"\x10", (_info(10, 12),)),
            WriteStep(encode_advance_command(12), (b"\x01\x00",)),
            WriteStep(b"\x10", (_info(12, 12),)),
        ),
    )
    clock = Clock()
    activity: list[ActivityEvent] = []

    result = await run_opportunistic_collector(
        Provider([first, recovered]),
        _staging_store(tmp_path, _capture_root(tmp_path)),
        _options(clock=clock, activity=activity),
    )

    assert isinstance(result, CollectionResult)
    assert recovered.writes[:2] == [b"\x10", b"\x10"]
    assert recovered.writes[2] == encode_read_command(11, 1)
    assert [event.retry_seconds for event in activity if event.state == "storage_wait"] == [1]
    assert clock.value == pytest.approx(1.001)


@_async_test
async def test_storage_not_ready_read_ack_reconnects_and_continues_collection(tmp_path: Path) -> None:
    first = ScriptedRingSession(
        _status(),
        (
            WriteStep(b"\x10", (_info(10, 11),)),
            WriteStep(encode_read_command(10, 1), (b"\x01" + bytes((STATUS_STORAGE_NOT_READY,)),)),
            WriteStep(encode_stop_command()),
        ),
    )
    recovered = ScriptedRingSession(
        _status(),
        (
            WriteStep(b"\x10", (_info(10, 11),)),
            WriteStep(encode_read_command(10, 1), (_begin(10, 1), _data(_record(10)), _done(11))),
            WriteStep(b"\x10", (_info(10, 11),)),
            WriteStep(encode_advance_command(11), (b"\x01\x00",)),
            WriteStep(b"\x10", (_info(11, 11),)),
        ),
    )

    result = await run_opportunistic_collector(
        Provider([first, recovered]), _staging_store(tmp_path, _capture_root(tmp_path)), _options()
    )

    assert isinstance(result, CollectionResult)
    assert result.next_sequence == 11
    assert first.writes == [b"\x10", encode_read_command(10, 1), encode_stop_command()]
    assert recovered.writes == [
        b"\x10",
        encode_read_command(10, 1),
        b"\x10",
        encode_advance_command(11),
        b"\x10",
    ]


def test_session_retry_outcome_only_storage_not_ready_ack_is_retryable() -> None:
    assert session_retry_outcome(RingAcknowledgementError(STATUS_STORAGE_NOT_READY), connected=False) == "retry"
    assert session_retry_outcome(RingAcknowledgementError(1), connected=False) is None


def test_session_retry_outcome_retries_nested_transfer_cause() -> None:
    error = TransferInterruptedError("timed out", TransferCounters(0, 0, 0), cause=TimeoutError())

    assert session_retry_outcome(error, connected=True) == "connected_interrupted"


@_async_test
@pytest.mark.parametrize(
    ("case", "read", "write"),
    [("gap", 13, 13), ("regression", 9, 12), ("expired", 10, 11)],
)
async def test_reconnect_discontinuities_quarantine_source_without_metadata(
    tmp_path: Path, case: str, read: int, write: int, monkeypatch: pytest.MonkeyPatch
) -> None:
    ordering: list[str] = []
    original_close = StagingWriter.close
    original_quarantine = StagingStore.quarantine_attempt_source

    def close(writer: StagingWriter) -> None:
        ordering.append("close")
        original_close(writer)

    def quarantine_source(store: StagingStore, attempt_id: str) -> str:
        ordering.append("quarantine")
        return original_quarantine(store, attempt_id)

    monkeypatch.setattr(StagingWriter, "close", close)
    monkeypatch.setattr(StagingStore, "quarantine_attempt_source", quarantine_source)
    first = ScriptedRingSession(
        _status(),
        (
            WriteStep(b"\x10", (_info(10, 12),)),
            WriteStep(encode_read_command(10, 2), (RingTransportDisconnectedError("gone"),)),
            WriteStep(encode_stop_command()),
        ),
    )
    if case == "gap":
        steps = [WriteStep(b"\x10", (_info(read, write),)), WriteStep(b"\x10", (_info(read, write),))]
    elif case == "regression":
        steps = [
            WriteStep(b"\x10", (_info(read, write),)),
            WriteStep(b"\x10", (_info(read, write),)),
            WriteStep(encode_read_command(read, 3), (_begin(read, 3), _data(_records(read, 3)), _done(read + 3))),
            WriteStep(b"\x10", (_info(read, write),)),
            WriteStep(encode_advance_command(read + 3), (b"\x01\x00",)),
            WriteStep(b"\x10", (_info(read + 3, write),)),
        ]
    else:
        steps = [
            WriteStep(b"\x10", (_info(read, write),)),
            WriteStep(b"\x10", (_info(read, write),)),
            WriteStep(encode_read_command(read, 1), (_begin(read, 1), _data(_records(read, 1)), _done(read + 1))),
            WriteStep(b"\x10", (_info(read, write),)),
            WriteStep(encode_advance_command(read + 1), (b"\x01\x00",)),
            WriteStep(b"\x10", (_info(read + 1, write),)),
        ]
    second = ScriptedRingSession(_status(), tuple(steps))

    store = _staging_store(tmp_path, _capture_root(tmp_path))
    store.preflight_storage()
    store.initialize_confirmed_loss_ledger()
    result = await run_opportunistic_collector(Provider([first, second]), store, _options(batch_records=3))
    assert isinstance(result, (CollectionResult, NoDataResult))
    assert not _staging_store(tmp_path, _capture_root(tmp_path)).pending_attempts()
    assert second.writes[0] == b"\x10"
    if case == "gap":
        assert second.writes == [b"\x10", b"\x10"]
        quarantined = store.quarantined_attempts()
        assert len(quarantined) == 1
        source_id = quarantined[0]
        source = _attempt_source(store, source_id)
        assert source is not None
        assert (source.source_type, source.lifecycle, source.committed_count) == ("quarantine", "blocked", 0)
        assert source.committed_count == 0
        assert _source_records(store, source_id) == b""

        def read_terminal() -> tuple[object, object, object] | None:
            with closing(store.state_db.open_connection(read_only=True)) as connection:
                return cast(
                    tuple[object, object, object] | None,
                    connection.execute(
                        "SELECT quarantine_disposition, terminal_reason, retention_deadline "
                        "FROM capture_source WHERE source_id = ?",
                        (source_id,),
                    ).fetchone(),
                )

        terminal = storage_call(read_terminal)
        assert terminal is not None
        assert terminal[0] == "unprocessable" and isinstance(terminal[1], str) and terminal[1]
        assert terminal[2] is not None
        with store.device_lock():
            pass
    else:
        quarantined = store.quarantined_attempts()
        assert len(quarantined) == 1
        source = _attempt_source(store, quarantined[0])
        assert source is not None and source.source_type == "quarantine"
        assert ordering.index("close") < ordering.index("quarantine")


@_async_test
@pytest.mark.parametrize(
    ("read", "write", "count"),
    [(9, 12, 3), (10, 11, 1)],
)
async def test_startup_discontinuity_defers_quarantine_salvage_until_next_maintenance(
    tmp_path: Path, read: int, write: int, count: int
) -> None:
    _seed_streaming_partial(tmp_path, count=2, persisted=1)
    store = _staging_store(tmp_path, _capture_root(tmp_path))
    source_id = store.pending_attempts()[0].attempt_id
    end = read + count
    session = ScriptedRingSession(
        _status(),
        (
            WriteStep(b"\x10", (_info(read, write),)),
            WriteStep(
                encode_read_command(read, count), (_begin(read, count), _data(_records(read, count)), _done(end))
            ),
            WriteStep(b"\x10", (_info(read, write),)),
            WriteStep(encode_advance_command(end), (b"\x01\x00",)),
            WriteStep(b"\x10", (_info(end, write),)),
        ),
    )

    result = await run_opportunistic_collector(Provider([session]), store, _options(batch_records=3))

    assert isinstance(result, CollectionResult)
    assert store.quarantined_attempts() == (source_id,)
    source = _attempt_source(store, source_id)
    assert source is not None and source.source_type == "quarantine"
    assert _source_records(store, source_id) == _record(100)


@_async_test
async def test_admission_prefix_mismatch_closes_writer_and_releases_lease(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _seed_streaming_partial(tmp_path, count=2, persisted=1)
    original_threads = {thread for thread in threading.enumerate() if thread.name == "omi-attempt-writer"}

    original_prepare = opportunistic_runtime._StagingWriterAdapter.prepare

    def mismatched_prepare(self: object) -> DurablePrefix:
        original_prepare(cast(opportunistic_runtime._StagingWriterAdapter, self))
        return DurablePrefix(100, 100, 0, "mismatch")

    monkeypatch.setattr(
        "omi_collector.capture.adapters.opportunistic_runtime._StagingWriterAdapter.prepare",
        mismatched_prepare,
    )
    session = ScriptedRingSession(_status(), (WriteStep(b"\x10", (_info(100, 102),)),))

    with pytest.raises(CursorConsistencyError, match="prepared durable prefix"):
        await run_opportunistic_collector(
            Provider([session]), _staging_store(tmp_path, _capture_root(tmp_path)), _options()
        )

    assert _staging_store(tmp_path, _capture_root(tmp_path)).pending_attempts()
    assert not ({thread for thread in threading.enumerate() if thread.name == "omi-attempt-writer"} - original_threads)


@_async_test
async def test_cursor_regression_after_advance_ack_rejects_drain_and_keeps_sqlite_source(tmp_path: Path) -> None:
    session = ScriptedRingSession(
        _status(),
        (
            WriteStep(b"\x10", (_info(100, 102),)),
            WriteStep(encode_read_command(100, 2), (_begin(100, 2), _data(_records(100, 2)), _done(102))),
            WriteStep(b"\x10", (_info(100, 102),)),
            WriteStep(encode_advance_command(102), (b"\x01\x00",)),
            WriteStep(b"\x10", (_info(99, 99),)),
        ),
    )

    store = _staging_store(tmp_path, _capture_root(tmp_path))
    with pytest.raises(CursorConsistencyError, match=r"INFO after ADVANCE acknowledgment classified.*expired"):
        await run_opportunistic_collector(Provider([session]), store, _options(batch_records=3))

    assert session.writes.count(encode_advance_command(102)) == 1

    def read_source_rows() -> list[tuple[str]]:
        with closing(store.state_db.open_connection(read_only=True)) as connection:
            return cast(
                list[tuple[str]],
                connection.execute("SELECT source_id FROM capture_source WHERE source_type='attempt'").fetchall(),
            )

    source_rows = storage_call(read_source_rows)
    assert len(source_rows) == 1
    source_id = source_rows[0][0]
    source = _attempt_source(store, source_id)
    assert source is not None and source.lifecycle == "sealed_waiting"
    assert source.committed_count == 2
    assert _source_records(store, source_id) == _records(100, 2)
    assert not store.pending_attempts()
    assert _ready_closure_rows(store) == []


@_async_test
async def test_seal_failure_never_advances(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    session = ScriptedRingSession(
        _status(),
        (
            WriteStep(b"\x10", (_info(10, 11),)),
            WriteStep(encode_read_command(10, 1), (_begin(10, 1), _data(_record(10)), _done(11))),
        ),
    )

    def fail_seal(self: object, _done_notice: object) -> object:
        del self, _done_notice
        raise OSError("disk fault")

    monkeypatch.setattr("omi_collector.capture.adapters.attempts.SqliteStagedAttempt.seal", fail_seal)
    with pytest.raises(OSError, match="disk fault"):
        await run_opportunistic_collector(
            Provider([session]), _staging_store(tmp_path, _capture_root(tmp_path)), _options(batch_records=1)
        )
    assert encode_advance_command(11) not in session.writes


@_async_test
async def test_fatal_writer_failure_after_data_survives_barrier_and_preserves_source(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fatal = FatalInternalAudioStorageError("canonical audio database is corrupt", sqlite_errorcode=11)
    append = StagingWriter.append_chunk

    def fail_after_commit(self: StagingWriter, offset: int, chunk: memoryview, tail: bytes = b"") -> None:
        append(self, offset, chunk, tail)
        raise fatal

    monkeypatch.setattr(StagingWriter, "append_chunk", fail_after_commit)
    session = ScriptedRingSession(
        _status(),
        (
            WriteStep(b"\x10", (_info(10, 11),)),
            WriteStep(encode_read_command(10, 1), (_begin(10, 1), _data(_record(10)), _done(11))),
        ),
    )
    store = _staging_store(tmp_path, _capture_root(tmp_path))
    with pytest.raises(FatalInternalAudioStorageError) as raised:
        await run_opportunistic_collector(Provider([session]), store, _options(batch_records=1))
    assert raised.value is fatal
    assert encode_advance_command(11) not in session.writes
    pending = store.pending_attempts()
    assert len(pending) == 1
    source = _attempt_source(store, pending[0].attempt_id)
    assert source is not None and source.committed_count == 1
    assert _source_records(store, source.source_id) == _record(10)


@_async_test
@pytest.mark.parametrize("next_data", [False, True], ids=["quiet-read", "next-data"])
async def test_active_read_fatal_writer_failure_stops_without_advance(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, next_data: bool
) -> None:
    fatal = FatalInternalAudioStorageError("canonical audio database is corrupt", sqlite_errorcode=11)
    sql_error = sqlite3.DatabaseError("file is not a database")
    fatal.__cause__ = sql_error
    failure_latched = threading.Event()
    original_append = StagingWriter.append_chunk
    original_record_failure = AttemptWriter._record_failure

    def fail_append(self: StagingWriter, offset: int, chunk: memoryview, tail: bytes = b"") -> None:
        original_append(self, offset, chunk, tail)
        raise fatal

    def observe_failure(self: AttemptWriter, error: BaseException, *, close_failed: bool = False) -> BaseException:
        latched = original_record_failure(self, error, close_failed=close_failed)
        failure_latched.set()
        return latched

    monkeypatch.setattr(StagingWriter, "append_chunk", fail_append)
    monkeypatch.setattr(AttemptWriter, "_record_failure", observe_failure)

    class QuietReadSession(ScriptedRingSession):
        def __init__(self) -> None:
            super().__init__(
                _status(),
                (
                    WriteStep(b"\x10", (_info(10, 12),)),
                    WriteStep(encode_read_command(10, 2), (_begin(10, 2), _data(_record(10)))),
                    WriteStep(encode_stop_command(), error=OSError("STOP failed")),
                ),
            )
            self._stream_calls = 0

        def notifications(self) -> AsyncIterator[bytes]:
            self._stream_calls += 1
            if self._stream_calls == 1:
                return super().notifications()

            async def stream() -> AsyncIterator[bytes]:
                yield _begin(10, 2)
                yield _data(_record(10))
                await asyncio.to_thread(failure_latched.wait, 5)
                if next_data:
                    yield _data(_record(11))
                await asyncio.Event().wait()

            return stream()

    session = QuietReadSession()
    store = _staging_store(tmp_path, _capture_root(tmp_path))
    task = asyncio.create_task(run_opportunistic_collector(Provider([session]), store, _options(batch_records=2)))
    with pytest.raises(FatalInternalAudioStorageError) as raised:
        await asyncio.wait_for(task, 5)

    assert raised.value is fatal
    assert raised.value.sqlite_errorcode == 11
    assert raised.value.__cause__ is sql_error
    assert session.writes == [b"\x10", encode_read_command(10, 2), encode_stop_command()]
    assert encode_advance_command(12) not in session.writes


@_async_test
async def test_latched_writer_failure_wins_when_done_arrives_same_turn(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fatal = FatalInternalAudioStorageError("canonical audio database is corrupt", sqlite_errorcode=11)
    failure_latched = threading.Event()
    original_append = StagingWriter.append_chunk
    original_record_failure = AttemptWriter._record_failure

    def fail_append(self: StagingWriter, offset: int, chunk: memoryview, tail: bytes = b"") -> None:
        original_append(self, offset, chunk, tail)
        raise fatal

    def observe_failure(self: AttemptWriter, error: BaseException, *, close_failed: bool = False) -> BaseException:
        latched = original_record_failure(self, error, close_failed=close_failed)
        failure_latched.set()
        return latched

    monkeypatch.setattr(StagingWriter, "append_chunk", fail_append)
    monkeypatch.setattr(AttemptWriter, "_record_failure", observe_failure)

    class DoneAfterFailureSession(ScriptedRingSession):
        def __init__(self) -> None:
            super().__init__(
                _status(),
                (
                    WriteStep(b"\x10", (_info(10, 12),)),
                    WriteStep(encode_read_command(10, 2)),
                    WriteStep(encode_stop_command()),
                ),
            )
            self._stream_calls = 0

        def notifications(self) -> AsyncIterator[bytes]:
            self._stream_calls += 1
            if self._stream_calls == 1:
                return super().notifications()

            async def stream() -> AsyncIterator[bytes]:
                yield _begin(10, 2)
                yield _data(_record(10))
                await asyncio.to_thread(failure_latched.wait, 5)
                yield _done(12)

            return stream()

    session = DoneAfterFailureSession()
    task = asyncio.create_task(
        run_opportunistic_collector(
            Provider([session]),
            _staging_store(tmp_path, _capture_root(tmp_path)),
            _options(batch_records=2),
        )
    )
    with pytest.raises(FatalInternalAudioStorageError) as raised:
        await asyncio.wait_for(task, 5)

    assert raised.value is fatal
    assert raised.value.sqlite_errorcode == 11
    assert session.writes == [b"\x10", encode_read_command(10, 2), encode_stop_command()]
    assert encode_advance_command(12) not in session.writes


@_async_test
async def test_post_session_checkpoint_surfaces_latched_writer_failure_identity(tmp_path: Path) -> None:
    latched = OSError("latched disk failure")
    wrapped = WriterFailedError("writer boundary failed")

    class FailedWriter:
        progress = WriterProgress(RECORD_SIZE, 0)
        failure = latched
        settled_prefix = None

        async def await_seal_result(self) -> object | None:
            return None

        async def checkpoint(self) -> object:
            raise wrapped

    async def quarantine(_attempt_id: str) -> None:
        return None

    reconciler = BatchReconciler(
        _staging_store(tmp_path, _capture_root(tmp_path)),
        _options(),
        _runtime(),
        quarantine,
    )
    reconciler._state.batch = batch_reconciliation._Batch(
        RingInfo(10, 11, 10000, 0, RECORD_SIZE),
        10,
        11,
        TransferArena(10, 1, max_bytes=RECORD_SIZE),
        cast(BatchWriterPort, FailedWriter()),
    )

    with pytest.raises(OSError) as raised:
        await reconciler.checkpoint_after_session()
    assert raised.value is latched


def test_durable_progress_excludes_empty_authenticated_frontier(tmp_path: Path) -> None:
    async def quarantine(_attempt_id: str) -> None:
        return None

    reconciler = BatchReconciler(
        _staging_store(tmp_path, _capture_root(tmp_path)),
        _options(),
        _runtime(),
        quarantine,
    )
    descriptor = AttemptDescriptor("a" * 32, 2, 100, 2)
    reconciler.set_startup_state(descriptor, 100)
    assert reconciler.durable_progress() == 0
    reconciler.set_startup_state(descriptor, 101)
    assert reconciler.durable_progress() == 101

    empty_reconciler = BatchReconciler(
        _staging_store(tmp_path / "empty", _capture_root(tmp_path / "empty")),
        _options(),
        _runtime(),
        quarantine,
    )
    empty_reconciler._state.batch = batch_reconciliation._Batch(
        RingInfo(100, 100, 10000, 0, RECORD_SIZE),
        100,
        100,
        TransferArena(100, 1, max_bytes=RECORD_SIZE),
        cast(BatchWriterPort, object()),
        DurablePrefix(100, 100, 0, "a" * 64),
    )
    assert empty_reconciler.durable_progress() == 0
    empty_reconciler._state.batch.durable = DurablePrefix(100, 101, 1, "b" * 64)
    assert empty_reconciler.durable_progress() == 101


@_async_test
async def test_close_visit_absence_publishes_partial_prefix_before_terminal_closure(tmp_path: Path) -> None:
    store = _staging_store(tmp_path, _capture_root(tmp_path))
    async with _real_batch_writer(store, 100, 2) as writer:
        assert writer.publish(RECORD_SIZE)
        attempt_id = writer.attempt_id
        batch = batch_reconciliation._Batch(
            RingInfo(100, 102, 10000, 0, RECORD_SIZE),
            100,
            102,
            TransferArena(100, 2, max_bytes=RECORD_SIZE * 2),
            writer,
        )

        async def quarantine(_attempt_id: str) -> None:
            return None

        reconciler = BatchReconciler(store, _options(), _runtime(), quarantine)
        reconciler._state.batch = batch
        await reconciler.close_visit("absence")

        assert reconciler._state.batch is None
        assert reconciler.durable_progress() == 0
        assert not writer.thread.is_alive()
        source = _attempt_source(store, attempt_id)
        assert source is not None
        assert source.lifecycle == "sealed_waiting"
        assert source.committed_count == 1
        assert _ready_closure_rows(store) == [{"next_sequence": 101, "reason": "absence"}]


@_async_test
async def test_close_visit_sealed_batch_appends_terminal_closure_after_writer_close(tmp_path: Path) -> None:
    store = _staging_store(tmp_path, _capture_root(tmp_path))
    async with _real_batch_writer(store, 100, 2) as writer:
        assert writer.publish(RECORD_SIZE * 2)
        seal = cast(DurableSqliteSeal, await writer.seal(DoneNotification(0, 102)))
        assert seal is not None
        batch = batch_reconciliation._Batch(
            RingInfo(100, 102, 10000, 0, RECORD_SIZE),
            100,
            102,
            TransferArena(100, 2, max_bytes=RECORD_SIZE * 2),
            writer,
            seal=seal,
        )

        async def quarantine(_attempt_id: str) -> None:
            return None

        reconciler = BatchReconciler(store, _options(), _runtime(), quarantine)
        reconciler._state.batch = batch
        await reconciler.close_visit("absence")

        assert not writer.thread.is_alive()
        assert seal.source_id == writer.attempt_id
        assert _attempt_source(store, seal.source_id) is not None
        assert _ready_closure_rows(store) == [{"next_sequence": 102, "reason": "absence"}]


@_async_test
async def test_close_visit_empty_batch_closes_writer_without_creating_visit_closure(tmp_path: Path) -> None:
    store = _staging_store(tmp_path, _capture_root(tmp_path))
    async with _real_batch_writer(store, 100, 1) as writer:
        batch = batch_reconciliation._Batch(
            RingInfo(100, 100, 10000, 0, RECORD_SIZE),
            100,
            100,
            TransferArena(100, 0, max_bytes=0),
            writer,
        )

        async def quarantine(_attempt_id: str) -> None:
            return None

        reconciler = BatchReconciler(store, _options(), _runtime(), quarantine)
        reconciler._state.batch = batch
        await reconciler.close_visit("absence")

        assert reconciler._state.batch is None
        assert not writer.thread.is_alive()
        assert _ready_closure_rows(store) == []


@_async_test
async def test_failed_writer_close_keeps_sealed_batch_active(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    store = _staging_store(tmp_path, _capture_root(tmp_path))
    async with _real_batch_writer(store, 100, 1) as writer:
        assert writer.publish(RECORD_SIZE)
        seal = cast(DurableSqliteSeal, await writer.seal(DoneNotification(0, 101)))
        assert seal is not None
        batch = batch_reconciliation._Batch(
            RingInfo(100, 101, 10000, 0, RECORD_SIZE),
            100,
            101,
            TransferArena(100, 1, max_bytes=RECORD_SIZE),
            writer,
            seal=seal,
        )

        async def quarantine(_attempt_id: str) -> None:
            return None

        reconciler = BatchReconciler(store, _options(), _runtime(), quarantine)
        reconciler._state.batch = batch

        async def fail_close() -> None:
            raise TimeoutError("writer close did not complete")

        with monkeypatch.context() as patch:
            patch.setattr(writer, "close", fail_close)
            with pytest.raises(TimeoutError, match="writer close"):
                await batch_reconciliation._complete_batch(reconciler._state, batch)

        assert reconciler._state.batch is batch
        assert reconciler._state.last_result is None
        assert reconciler.completed_batches == 0


@_async_test
async def test_cancelled_owned_seal_is_adopted_before_uncertain_advance_retries(tmp_path: Path) -> None:
    target = _BlockingSealTarget(tmp_path / "sealed")
    writer = _SealResultBatchWriter(target)
    try:
        await writer.writer.start()
        await writer.writer.read_begin(ReadBeginNotification(10, 1))
        assert writer.writer.publish(RECORD_SIZE)
        durable = await writer.checkpoint()
        batch = batch_reconciliation._Batch(
            RingInfo(10, 11, 10000, 0, RECORD_SIZE),
            10,
            11,
            TransferArena(10, 1, max_bytes=RECORD_SIZE),
            cast(BatchWriterPort, writer),
            durable,
        )

        sealing = asyncio.create_task(writer.writer.seal(DoneNotification(0, 11)))
        assert await asyncio.to_thread(target.seal_started.wait, 1)
        sealing.cancel()
        sealing.cancel()
        await asyncio.sleep(0)
        assert not sealing.done()
        assert writer.thread.is_alive()

        async def quarantine(_attempt_id: str) -> None:
            return None

        reconciler = BatchReconciler(
            _staging_store(tmp_path, _capture_root(tmp_path)),
            _options(),
            _runtime(),
            quarantine,
        )
        reconciler._state.batch = batch
        adoption = asyncio.create_task(reconciler.checkpoint_after_session())
        await asyncio.sleep(0)
        assert not adoption.done()
        assert target.seal_calls == 1
        target.release_seal.set()
        with pytest.raises(asyncio.CancelledError):
            await sealing
        await adoption
        assert batch.seal == SealResult(tmp_path / "sealed", False)

        current = RingInfo(10, 11, 10000, 0, RECORD_SIZE)

        async def info(_session: RingSession) -> RingInfo:
            return current

        first = ScriptedRingSession(
            _status(),
            (WriteStep(encode_advance_command(11), error=RingTransportDisconnectedError("unknown")),),
        )
        with pytest.raises(batch_reconciliation.collector.AdvanceUncertainError):
            await reconciler.connected_step(first, current, info, SessionPhaseState("advance"))

        second = ScriptedRingSession(_status(), (WriteStep(encode_advance_command(11), (b"\x01\x00",)),))
        confirmed_info = RingInfo(11, 11, 10000, 0, RECORD_SIZE)
        second_infos = iter((current, confirmed_info))

        async def second_info_reader(_session: RingSession) -> RingInfo:
            return next(second_infos)

        outcome, confirmed = await reconciler.connected_step(
            second, current, second_info_reader, SessionPhaseState("advance")
        )
        assert outcome is None
        assert confirmed == confirmed_info
        assert target.seal_calls == 1
        await asyncio.to_thread(writer.thread.join, 1)
        assert not writer.thread.is_alive()

    finally:
        target.release_seal.set()
        if writer.thread.is_alive():
            await writer.close()


@_async_test
async def test_finalization_joins_pending_seal_and_reports_bundle(tmp_path: Path) -> None:
    target = _BlockingSealTarget(tmp_path / "sealed")
    writer = _SealResultBatchWriter(target)
    try:
        await writer.writer.start()
        await writer.writer.read_begin(ReadBeginNotification(10, 1))
        assert writer.writer.publish(RECORD_SIZE)
        durable = await writer.checkpoint()
        batch = batch_reconciliation._Batch(
            RingInfo(10, 11, 10000, 0, RECORD_SIZE),
            10,
            11,
            TransferArena(10, 1, max_bytes=RECORD_SIZE),
            cast(BatchWriterPort, writer),
            durable,
        )
        sealing = asyncio.create_task(writer.writer.seal(DoneNotification(0, 11)))
        assert await asyncio.to_thread(target.seal_started.wait, 1)
        sealing.cancel()
        sealing.cancel()
        await asyncio.sleep(0)
        assert not sealing.done()
        assert writer.thread.is_alive()

        async def quarantine(_attempt_id: str) -> None:
            return None

        reconciler = BatchReconciler(
            _staging_store(tmp_path, _capture_root(tmp_path)),
            replace(_options(), timeouts=TransferTimeouts(1, 0.01)),
            _runtime(),
            quarantine,
        )
        reconciler._state.batch = batch
        finalizing = asyncio.create_task(reconciler.finalize_active())
        await asyncio.sleep(0)
        assert not finalizing.done()
        assert target.seal_calls == 1
        target.release_seal.set()
        with pytest.raises(asyncio.CancelledError):
            await sealing
        result = await finalizing
        assert result.checkpoint_error is None
        assert result.close_error is None
        assert writer.close_requested.is_set()
        assert result.preserved_path == tmp_path / "sealed"
        assert result.preserved_kind == "sealed bundle"
        await asyncio.to_thread(writer.thread.join, 1)
        assert not writer.thread.is_alive()

    finally:
        target.release_seal.set()
        if writer.thread.is_alive():
            await writer.close()


@_async_test
@pytest.mark.parametrize(
    ("cursor", "write", "repeat", "error"),
    [
        (11, 12, True, None),
        (12, 12, False, None),
        (13, 13, False, None),
    ],
)
async def test_uncertain_advance_reconciles_cursor_safely(
    tmp_path: Path, cursor: int, write: int, repeat: bool, error: type[Exception] | None
) -> None:
    activity: list[ActivityEvent] = []
    first = ScriptedRingSession(
        _status(),
        (
            WriteStep(b"\x10", (_info(10, 12),)),
            WriteStep(encode_read_command(10, 2), (_begin(10, 2), _data(_records(10, 2)), _done(12))),
            WriteStep(b"\x10", (_info(10, 12),)),
            WriteStep(encode_advance_command(12), error=RingTransportDisconnectedError("unknown")),
        ),
    )
    steps = [WriteStep(b"\x10", (_info(cursor, write),)), WriteStep(b"\x10", (_info(cursor, write),))]
    if repeat:
        steps.extend([WriteStep(encode_advance_command(12), (b"\x01\x00",)), WriteStep(b"\x10", (_info(12, 12),))])
    second = ScriptedRingSession(_status(), tuple(steps))

    if error is not None:
        with pytest.raises(error):
            await run_opportunistic_collector(
                Provider([first, second]),
                _staging_store(tmp_path, _capture_root(tmp_path)),
                _options(activity=activity),
            )
    else:
        result = await run_opportunistic_collector(
            Provider([first, second]),
            _staging_store(tmp_path, _capture_root(tmp_path)),
            _options(activity=activity),
        )
        assert isinstance(result, CollectionResult)
        assert result.next_sequence == 12
        assert (encode_advance_command(12) in second.writes) is repeat
        if cursor > 12:
            assert encode_advance_command(12) not in second.writes
    errors = [event for event in activity if event.state == "session_error"]
    assert errors[0].phase == "advance"


@_async_test
async def test_default_arena_budget_admits_a_full_pendant_snapshot_without_allocating_it() -> None:
    policy = RetryPolicy()
    arena_max_bytes = DEFAULT_CONFIG.memory.arena_max_bytes
    batch_records = arena_max_bytes // RECORD_SIZE

    assert policy.backoff == DEFAULT_CONFIG.retry.rapid_backoff
    assert policy.drain_cooldown_seconds == DEFAULT_CONFIG.presence.drain_cooldown_seconds
    assert policy.arena_max_bytes == arena_max_bytes
    assert policy.batch_records == batch_records
    assert policy.batch_records * RECORD_SIZE <= policy.arena_max_bytes
    assert (policy.batch_records + 1) * RECORD_SIZE > policy.arena_max_bytes
    assert policy.arena_max_bytes >= 470 * 1024 * 1024


@_async_test
async def test_writer_lease_blocks_second_coordinator_after_batch_admission(tmp_path: Path) -> None:
    entered = asyncio.Event()

    class BlockingReadSession(ScriptedRingSession):
        async def write_control(self, payload: bytes) -> None:
            await super().write_control(payload)
            if payload == encode_read_command(10, 1):
                entered.set()

    first = BlockingReadSession(
        _status(),
        (
            WriteStep(b"\x10", (_info(10, 11),)),
            WriteStep(encode_read_command(10, 1), (_begin(10, 1),)),
        ),
    )
    second = ScriptedRingSession(_status(), (WriteStep(b"\x10", (_info(10, 11),)),))
    recovered = ScriptedRingSession(
        _status(),
        (
            WriteStep(b"\x10", (_info(10, 11),)),
            WriteStep(encode_read_command(10, 1), (_begin(10, 1), _data(_record(10)), _done(11))),
            WriteStep(b"\x10", (_info(10, 11),)),
            WriteStep(encode_advance_command(11), (b"\x01\x00",)),
            WriteStep(b"\x10", (_info(11, 11),)),
        ),
    )
    store = _staging_store(tmp_path, _capture_root(tmp_path))
    task = asyncio.create_task(run_opportunistic_collector(Provider([first]), store, _options(batch_records=1)))
    await entered.wait()
    contender = Provider([second])
    with pytest.raises(DeviceAlreadyRunningError) as raised:
        await run_opportunistic_collector(contender, store, _options(batch_records=1))
    assert contender.opened == 0
    assert second.writes == []
    # In-process lease contention has no cross-process holder attribution.
    assert raised.value.lock_context is None
    task.cancel()
    with pytest.raises(CollectionPreservedCancelledError):
        await task
    result = await run_opportunistic_collector(
        Provider([recovered]), _staging_store(tmp_path, _capture_root(tmp_path)), _options(batch_records=1)
    )
    assert isinstance(result, CollectionResult)
    assert recovered.writes == [
        b"\x10",
        encode_read_command(10, 1),
        b"\x10",
        encode_advance_command(11),
        b"\x10",
    ]


@_async_test
async def test_machine_closes_published_restart_prefix_before_waiting_without_device(tmp_path: Path) -> None:
    records = _seed_streaming_partial(tmp_path, count=2, persisted=2)
    store = _staging_store(tmp_path, _capture_root(tmp_path))
    attempt_id = store.pending_attempts()[0].attempt_id

    def close_published() -> None:
        with store.device_lock() as lease:
            attempt = store.resume_streaming_attempt(lease)
            assert attempt is not None
            attempt.publish_prefix()
            store.terminalize_prefix_attempt_held(attempt_id, lease)
            attempt.close()

    storage_call(close_published)
    scanning = asyncio.Event()

    class Observer:
        async def start(self, callback: Callable[[object], object]) -> None:
            del callback
            scanning.set()

        async def stop(self) -> None:
            return None

    provider = Provider([])
    presence = PresenceScheduler(Observer(), policy=PresencePolicy(rapid_backoff=(0.001,)))
    task = asyncio.create_task(run_opportunistic_collector(provider, store, replace(_options(), presence=presence)))
    scan_ready = asyncio.create_task(scanning.wait())
    checks_completed = False
    try:
        done, _ = await asyncio.wait({scan_ready, task}, timeout=2.0, return_when=asyncio.FIRST_COMPLETED)
        assert scan_ready in done, "collector completed before scanning started"
        scan_ready.result()
        assert not task.done()
        assert provider.opened == 0
        pending, recovery = storage_call(lambda: (store.pending_attempts(), store.inspect_recovery()))
        assert pending == ()
        source = _attempt_source(store, attempt_id)
        assert source is not None
        assert source.lifecycle == "sealed_waiting"
        assert source.committed_count == 2
        assert source.committed_sha256 == sha256(records).hexdigest()
        assert not recovery[1], "durable closure must cover retained drafts"
        checks_completed = True
    finally:
        await _finish_task(task, cancel=True)
        await _finish_task(scan_ready, cancel=True)
        if checks_completed:
            with pytest.raises(asyncio.CancelledError):
                task.result()


@_async_test
@pytest.mark.parametrize(
    ("phase", "expected_count"),
    [("read", 1), ("advance", 2), ("confirm", 2)],
)
async def test_cancellation_reports_typed_source_custody(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, phase: str, expected_count: int
) -> None:
    entered = asyncio.Event()
    release_advance = asyncio.Event()
    committed = threading.Event()

    if phase == "read":
        append = StagingWriter.append_chunk

        def note_commit(self: StagingWriter, offset: int, chunk: memoryview, tail: bytes = b"") -> None:
            append(self, offset, chunk, tail)
            committed.set()

        monkeypatch.setattr(StagingWriter, "append_chunk", note_commit)

    class GatedSession(ScriptedRingSession):
        async def write_control(self, payload: bytes) -> None:
            await super().write_control(payload)
            if phase == "read" and payload == encode_read_command(10, 2):
                entered.set()
            if phase == "advance" and payload == encode_advance_command(12):
                entered.set()
                await release_advance.wait()
            if phase == "confirm" and payload == b"\x10" and len(self.writes) == 5:
                entered.set()

    if phase == "read":
        read_notifications = (_begin(10, 2), _data(_record(10)))
        pre_advance_notifications: tuple[bytes, ...] = ()
        advance_notifications: tuple[bytes, ...] = ()
        confirm_notifications: tuple[bytes, ...] = ()
    elif phase == "advance":
        read_notifications = (_begin(10, 2), _data(_records(10, 2)), _done(12))
        pre_advance_notifications = (_info(10, 12),)
        advance_notifications = ()
        confirm_notifications = ()
    else:
        read_notifications = (_begin(10, 2), _data(_records(10, 2)), _done(12))
        pre_advance_notifications = (_info(10, 12),)
        advance_notifications = (b"\x01\x00",)
        confirm_notifications = (_info(12, 12),)
    session = GatedSession(
        _status(),
        (
            WriteStep(b"\x10", (_info(10, 12),)),
            WriteStep(encode_read_command(10, 2), read_notifications),
            WriteStep(b"\x10", pre_advance_notifications),
            WriteStep(encode_advance_command(12), advance_notifications),
            WriteStep(b"\x10", confirm_notifications),
        ),
    )
    store = _staging_store(tmp_path, _capture_root(tmp_path))
    task = asyncio.create_task(run_opportunistic_collector(Provider([session]), store, _options()))
    try:
        await asyncio.wait_for(entered.wait(), 5)
        if phase == "read":
            assert await asyncio.to_thread(committed.wait, 5)
        task.cancel()

        with pytest.raises(CollectionPreservedCancelledError) as raised:
            await task
        assert raised.value.kind == "internal audio source"
        assert raised.value.preserved_source_id is not None
        source = _attempt_source(store, raised.value.preserved_source_id)
        assert source is not None
        assert source.committed_count == expected_count
        assert _source_records(store, source.source_id) == _records(10, expected_count)
    finally:
        release_advance.set()
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


@_async_test
async def test_background_quality_corruption_stops_active_read_after_preserving_source(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    entered = asyncio.Event()

    class GatedSession(ScriptedRingSession):
        async def write_control(self, payload: bytes) -> None:
            await super().write_control(payload)
            if payload == encode_read_command(10, 2):
                entered.set()

    session = GatedSession(
        _status(),
        (
            WriteStep(b"\x10", (_info(10, 12),)),
            WriteStep(encode_read_command(10, 2), (_begin(10, 2), _data(_record(10)))),
        ),
    )
    store = _staging_store(tmp_path, _capture_root(tmp_path))
    journal = _quality_metrics(store.state_db)
    fatal = FatalInternalAudioStorageError("canonical audio database is corrupt", sqlite_errorcode=11)

    def fail_quality(_item_id: str, _payload: dict[str, object]) -> None:
        raise fatal

    monkeypatch.setattr(journal, "_put_quality_event", fail_quality)
    task = asyncio.create_task(
        run_opportunistic_collector(
            Provider([session]), store, replace(_options(), quality_metrics=journal, host_time=lambda: 1000.0)
        )
    )
    try:
        await asyncio.wait_for(entered.wait(), 5)

        async def wait_for_durable_record() -> None:
            while True:
                pending = store.pending_attempts()
                if pending:
                    source = _attempt_source(store, pending[0].attempt_id)
                    if source is not None and source.committed_count == 1:
                        return
                await asyncio.sleep(0.01)

        await asyncio.wait_for(wait_for_durable_record(), 5)
        journal._queue.put(("quality", {}, "fault"))
        with pytest.raises(FatalInternalAudioStorageError) as raised:
            await asyncio.wait_for(task, 5)
        assert raised.value is fatal
        pending = store.pending_attempts()
        assert len(pending) == 1
        source = _attempt_source(store, pending[0].attempt_id)
        assert source is not None
        assert source.committed_count == 1
        assert _source_records(store, source.source_id) == _record(10)
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


@_async_test
async def test_firmware_corruption_reported_during_writer_close_is_fatal_after_join(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fatal = FatalInternalAudioStorageError("canonical audio database is corrupt", sqlite_errorcode=11)
    closed = threading.Event()

    class FailingObservationWriter:
        def __init__(self, on_error: Callable[[Exception], None]) -> None:
            self.on_error = on_error

        def observe(self, _info: RingInfo) -> None:
            return None

        def close(self) -> None:
            self.on_error(fatal)
            closed.set()

    def make_writer(
        _runtime: OpportunisticRuntime,
        _staging: StagingStore,
        _config: object,
        on_error: Callable[[Exception], None],
    ) -> FailingObservationWriter:
        return FailingObservationWriter(on_error)

    monkeypatch.setattr(OpportunisticRuntime, "make_observation_writer", make_writer)
    session = ScriptedRingSession(_status(), (WriteStep(b"\x10", (_info(10, 10),)),))
    with pytest.raises(FatalInternalAudioStorageError) as raised:
        await run_opportunistic_collector(
            Provider([session]), _staging_store(tmp_path, _capture_root(tmp_path)), _options()
        )
    assert raised.value is fatal
    assert closed.is_set()


@_async_test
async def test_corrupt_clock_state_read_stops_preflight_before_audio_read(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fatal = FatalInternalAudioStorageError("canonical audio database is corrupt", sqlite_errorcode=11)

    def fail_state(_store: ClockStateStore) -> object:
        raise fatal

    monkeypatch.setattr(ClockStateStore, "state", fail_state)

    class ClockSession(ScriptedRingSession):
        async def read_optional_characteristic(self, uuid: str) -> bytes | None:
            if uuid == TIME_READ_UUID:
                return pack("<I", 1010)
            return None

    session = ClockSession(_status(), (WriteStep(b"\x10", (_info(10, 11),)),))
    store = _staging_store(tmp_path, _capture_root(tmp_path))
    with pytest.raises(FatalInternalAudioStorageError) as raised:
        await run_opportunistic_collector(
            Provider([session]),
            store,
            replace(_options(), host_time=lambda: 1000.0, host_clock_synchronized=lambda _timeout: True),
        )
    assert raised.value is fatal
    assert session.writes == [b"\x10"]
    assert store.pending_attempts() == ()


class _StalledFinalCheckpointProbe:
    def __init__(self, monkeypatch: pytest.MonkeyPatch, activity: list[ActivityEvent]) -> None:
        self.checkpoint_started = threading.Event()
        self.release_checkpoint = threading.Event()
        self.close_attempted = threading.Event()
        self.target_closed = threading.Event()
        self.data_submitted = threading.Event()
        self.closed_writers: list[AttemptWriter] = []
        self.activity = activity
        self.original_checkpoint = StagingWriter.checkpoint
        self.original_target_close = StagingWriter.close
        self.original_writer_close = AttemptWriter.close
        self.original_publish = AttemptWriter.publish
        self.read_begin_completed = _patch_read_begin_completion(monkeypatch)
        self.entered = asyncio.Event()

        def stalled_checkpoint(writer: StagingWriter) -> object:
            self.checkpoint_started.set()
            assert self.release_checkpoint.wait(2)
            return self.original_checkpoint(writer)

        async def failed_interrupt_admission(writer: AttemptWriter) -> object:
            del writer
            raise OSError("interrupt was not admitted")

        def observed_target_close(writer: StagingWriter) -> object:
            try:
                return self.original_target_close(writer)
            finally:
                self.target_closed.set()

        async def observed_writer_close(writer: AttemptWriter) -> object:
            self.close_attempted.set()
            self.closed_writers.append(writer)
            return await self.original_writer_close(writer)

        def observed_publish(writer: AttemptWriter, high_water: int, tail: bytes = b"") -> bool:
            published = self.original_publish(writer, high_water, tail)
            if published:
                self.data_submitted.set()
            return published

        monkeypatch.setattr(StagingWriter, "checkpoint", stalled_checkpoint)
        monkeypatch.setattr(StagingWriter, "close", observed_target_close)
        monkeypatch.setattr(AttemptWriter, "interrupt", failed_interrupt_admission)
        monkeypatch.setattr(AttemptWriter, "close", observed_writer_close)
        monkeypatch.setattr(AttemptWriter, "publish", observed_publish)


@_async_test
async def test_repeated_cancel_during_interrupt_retains_custody_receipt(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    entered = asyncio.Event()
    interrupted = threading.Event()
    release_interrupt = threading.Event()
    submitted = threading.Event()
    read_begin_completed = _patch_read_begin_completion(monkeypatch)
    original_interrupt = StagingWriter.interrupt
    original_publish = AttemptWriter.publish

    def held_interrupt(writer: StagingWriter) -> DurablePrefix:
        interrupted.set()
        assert release_interrupt.wait(2)
        return original_interrupt(writer)

    def observed_publish(writer: AttemptWriter, high_water: int, tail: bytes = b"") -> bool:
        published = original_publish(writer, high_water, tail)
        if published:
            submitted.set()
        return published

    monkeypatch.setattr(StagingWriter, "interrupt", held_interrupt)
    monkeypatch.setattr(AttemptWriter, "publish", observed_publish)

    class BlockingReadSession(ScriptedRingSession):
        async def write_control(self, payload: bytes) -> None:
            await super().write_control(payload)
            if payload == encode_read_command(10, 1):
                entered.set()

    session = BlockingReadSession(
        _status(),
        (
            WriteStep(b"\x10", (_info(10, 11),)),
            WriteStep(encode_read_command(10, 1), (_begin(10, 1), _data(_record(10)))),
        ),
    )
    store = _staging_store(tmp_path, _capture_root(tmp_path))
    task = asyncio.create_task(run_opportunistic_collector(Provider([session]), store, _options(batch_records=1)))
    try:
        await asyncio.wait_for(entered.wait(), 5)
        assert await asyncio.to_thread(submitted.wait, 5)
        assert await asyncio.to_thread(read_begin_completed.wait, 5)
        task.cancel()
        assert await asyncio.to_thread(interrupted.wait, 5)
        task.cancel()
        await asyncio.sleep(0)
        assert not task.done()
        release_interrupt.set()
        with pytest.raises(CollectionPreservedCancelledError) as raised:
            await task
        source_id = raised.value.preserved_source_id
        assert source_id is not None
        source = _attempt_source(store, source_id)
        assert source is not None and source.committed_count == 1
        assert _source_records(store, source_id) == _record(10)
        assert encode_advance_command(11) not in session.writes
        with store.device_lock():
            pass
    finally:
        release_interrupt.set()
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


@_async_test
async def test_slow_final_checkpoint_joins_close_and_releases_lease(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    activity: list[ActivityEvent] = []
    probe = _StalledFinalCheckpointProbe(monkeypatch, activity)

    class BlockingReadSession(ScriptedRingSession):
        async def write_control(self, payload: bytes) -> None:
            await super().write_control(payload)
            if payload == encode_read_command(10, 1):
                probe.entered.set()

    session = BlockingReadSession(
        _status(),
        (
            WriteStep(b"\x10", (_info(10, 11),)),
            WriteStep(encode_read_command(10, 1), (_begin(10, 1), _data(_record(10)))),
        ),
    )
    store = _staging_store(tmp_path, _capture_root(tmp_path))
    task = asyncio.create_task(
        run_opportunistic_collector(Provider([session]), store, _options(batch_records=1, activity=activity))
    )
    try:
        await asyncio.wait_for(probe.entered.wait(), 5)
        assert await asyncio.to_thread(probe.data_submitted.wait, 5)
        assert await asyncio.to_thread(probe.read_begin_completed.wait, 5)
        task.cancel()

        assert await asyncio.to_thread(probe.checkpoint_started.wait, 5)
        await asyncio.sleep(0.05)
        assert not task.done(), "admitted checkpoint must finish before cancellation returns"
        assert not probe.target_closed.is_set()
        probe.release_checkpoint.set()
        with pytest.raises(OSError, match="interrupt was not admitted"):
            await task

        assert probe.close_attempted.is_set()
        assert probe.target_closed.is_set()
        assert not any(event.error_type == "CollectorTimeoutError" for event in activity)
    finally:
        task.cancel()
        probe.release_checkpoint.set()
        await asyncio.gather(task, return_exceptions=True)
        for writer in probe.closed_writers:
            await asyncio.to_thread(writer.thread.join, 5)

    assert await asyncio.to_thread(probe.target_closed.wait, 1)
    assert len(probe.closed_writers) == 1
    assert await asyncio.to_thread(probe.closed_writers[0].thread.join, 1) is None
    assert not probe.closed_writers[0].thread.is_alive()
    with _staging_store(tmp_path, _capture_root(tmp_path)).device_lock():
        pass

    def read_source_rows() -> list[tuple[str]]:
        with closing(store.state_db.open_connection(read_only=True)) as connection:
            return cast(
                list[tuple[str]],
                connection.execute("SELECT source_id FROM capture_source WHERE source_type='attempt'").fetchall(),
            )

    source_rows = storage_call(read_source_rows)
    assert len(source_rows) == 1
    source_id = source_rows[0][0]
    source = _attempt_source(store, source_id)
    assert source is not None
    assert source.lifecycle in {"receiving", "interrupted"}
    assert source.committed_count == 1
    assert _source_records(store, source_id) == _record(10)


@_async_test
async def test_no_data_async_activity_timeout_retry_and_policy_validation(tmp_path: Path) -> None:
    events: list[str] = []

    async def activity(event: ActivityEvent) -> None:
        await asyncio.sleep(0)
        events.append(event.state)

    session = ScriptedRingSession(_status(), (WriteStep(b"\x10", (_info(10, 10),)),))
    result = await run_opportunistic_collector(
        Provider([session]),
        _staging_store(tmp_path, _capture_root(tmp_path)),
        OpportunisticOptions(
            TransferTimeouts(1, 1), RetryPolicy(backoff=(0.001,), stop_after_drained=True), activity=activity
        ),
    )
    assert result.info.read_sequence == 10
    assert events == ["connecting", "drained"]

    for policy in (RetryPolicy(backoff=()), RetryPolicy(backoff=(0,)), RetryPolicy(drain_cooldown_seconds=0)):
        with pytest.raises(ValueError, match="policy"):
            await run_opportunistic_collector(
                Provider([]),
                _staging_store(tmp_path / str(policy), tmp_path / f"{policy}-captures"),
                OpportunisticOptions(TransferTimeouts(1, 1), policy),
            )

    timed_out = ScriptedRingSession(_status(), (WriteStep(b"\x10"),))
    drained = ScriptedRingSession(_status(), (WriteStep(b"\x10", (_info(10, 10),)),))
    result = await run_opportunistic_collector(
        Provider([timed_out, drained]),
        _staging_store(tmp_path / "timeout", tmp_path / "timeout-captures"),
        OpportunisticOptions(
            TransferTimeouts(0.01, 1), RetryPolicy(backoff=(0.001,), stop_after_drained=True), sleep=asyncio.sleep
        ),
    )
    assert result.info.read_sequence == 10
