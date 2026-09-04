from __future__ import annotations

import asyncio
import threading
import time
from collections.abc import AsyncIterator, Awaitable, Callable, Mapping
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from dataclasses import replace
from functools import wraps
from hashlib import sha256
from json import loads
from pathlib import Path
from shutil import rmtree
from struct import pack
from typing import ParamSpec, TypeVar, cast

import pytest
from bleak.backends.device import BLEDevice
from bleak.exc import BleakDBusError, BleakError

from fakes import DelayedNotification, ScriptedRingSession, WriteStep
from omi_collector.capture.adapters.attempt_writer import AttemptWriter, WriterFailedError, WriterProgress
from omi_collector.capture.adapters.opportunistic_runtime import OpportunisticRuntime
from omi_collector.capture.adapters.publication import SealResult
from omi_collector.capture.adapters.quality_metrics import JsonlQualityMetrics
from omi_collector.capture.adapters.staging_contract import DeviceAlreadyRunningError, DurablePrefix
from omi_collector.capture.adapters.staging_store import StagingStore
from omi_collector.capture.adapters.staging_writer import StagingWriter
from omi_collector.capture.application import batch_reconciliation
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
from omi_collector.capture.application.operational_telemetry import TIME_READ_UUID
from omi_collector.capture.application.opportunistic_sync import CollectionPreservedCancelledError
from omi_collector.capture.application.opportunistic_sync import (
    run_opportunistic_collector as _run_opportunistic_collector,
)
from omi_collector.capture.application.ports import BatchWriterPort
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
    report_session_error,
    retryable,
    validate_presence_policy,
)
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
from omi_collector.config import DEFAULT_CONFIG, CollectorConfig, RetryConfig, StagingRetentionConfig

P = ParamSpec("P")
T = TypeVar("T")


def _runtime() -> OpportunisticRuntime:
    """Build the concrete runtime explicitly for coordinator tests."""
    return OpportunisticRuntime()


async def run_opportunistic_collector(
    provider: Callable[[object | None], AbstractAsyncContextManager[RingSession]],
    staging: StagingStore,
    device_slug: str,
    options: OpportunisticOptions,
) -> CollectionResult | NoDataResult:
    """Test helper that injects the concrete runtime at every call site."""
    return await _run_opportunistic_collector(provider, staging, device_slug, options, runtime=_runtime())


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
        timeouts=TransferTimeouts(1, 1),
        policy=RetryPolicy(backoff=(0.001,), batch_records=batch_records, stop_after_drained=True),
        activity=activity.append if activity is not None else None,
        progress=progress.append if progress is not None else None,
        clock=local_clock,
        sleep=local_clock.sleep,
    )


@_async_test
async def test_progress_pump_coalesces_slow_callbacks_and_ignores_callback_failures() -> None:
    def event(records: int) -> ProgressEvent:
        return ProgressEvent(records, 3, records * RECORD_SIZE, 3 * RECORD_SIZE, 1.0, 1.0, float(RECORD_SIZE), None)

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

    pump = asyncio.create_task(batch_reconciliation._pump_progress(mailbox, slow_callback, 60.0))
    mailbox.publish(event(1))
    await entered.wait()
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

    failure_pump = asyncio.create_task(batch_reconciliation._pump_progress(failing, broken_callback, 1.0))
    failing.publish(event(3), terminal=True)
    await failure_pump


@_async_test
async def test_progress_pump_coalesces_arbitrary_revisions_before_cadence_release() -> None:
    def event(records: int) -> ProgressEvent:
        return ProgressEvent(records, 3, records * RECORD_SIZE, 3 * RECORD_SIZE, 1.0, 1.0, float(RECORD_SIZE), None)

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

    pump = asyncio.create_task(batch_reconciliation._pump_progress(mailbox, callback, 0.2))
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
            StagingStore(tmp_path, _capture_root(tmp_path)),
            "omi",
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
    observed: list[tuple[str, RingInfo]],
    close_calls: list[None],
    *,
    observe_error: bool = False,
    close_error: bool = False,
) -> None:
    class FakeObservationWriter:
        def __init__(self, *_args: object, **_kwargs: object) -> None:
            pass

        def observe(self, device_slug: str, info: RingInfo) -> None:
            if observe_error:
                raise RuntimeError("observation unavailable")
            observed.append((device_slug, info))

        def close(self) -> None:
            close_calls.append(None)
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
    store = StagingStore(root, _capture_root(root))
    attempt = store.prepare_streaming_attempt("omi", start, count)
    attempt.record_read_begin(ReadBeginNotification(start, count))
    records = _records(start, count)
    if persisted:
        attempt.accept_chunk(start, memoryview(records[: persisted * RECORD_SIZE]))
    attempt.checkpoint()
    attempt.close(durable=True)
    return records


def test_retry_policy_requires_explicit_drain_cooldown() -> None:
    assert RetryPolicy().drain_cooldown_seconds == 300
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
        policy=PresencePolicy(fallback_seconds=30, drained_fallback_seconds=900),
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
    observed: list[tuple[str, RingInfo]] = []
    close_calls: list[None] = []
    _patch_observation_writer(monkeypatch, observed, close_calls)
    session = ScriptedRingSession(_status(), (WriteStep(b"\x10", (_info(100, 100),)),))

    result = await run_opportunistic_collector(
        Provider([session]), StagingStore(tmp_path, _capture_root(tmp_path)), "omi", _options()
    )

    assert isinstance(result, NoDataResult)
    assert observed == [("omi", RingInfo(100, 100, 10000, 0, RECORD_SIZE))]
    assert close_calls == [None]


@_async_test
async def test_successful_info_values_reach_observation_writer_across_batch_lifecycle(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    observed: list[tuple[str, RingInfo]] = []
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
        Provider([session]), StagingStore(tmp_path, _capture_root(tmp_path)), "omi", _options()
    )

    assert isinstance(result, CollectionResult)
    assert observed == [
        ("omi", RingInfo(10, 11, 10000, 5, RECORD_SIZE)),
        ("omi", RingInfo(10, 11, 10000, 6, RECORD_SIZE)),
        ("omi", RingInfo(11, 11, 10000, 0, RECORD_SIZE)),
    ]
    assert close_calls == [None]
    assert session.writes == [b"\x10", encode_read_command(10, 1), b"\x10", encode_advance_command(11), b"\x10"]


@_async_test
@pytest.mark.parametrize("failure", ["observe", "close"])
async def test_observation_writer_failures_do_not_block_collection(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, failure: str
) -> None:
    observed: list[tuple[str, RingInfo]] = []
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
        Provider([session]), StagingStore(tmp_path, _capture_root(tmp_path)), "omi", _options()
    )

    assert isinstance(result, CollectionResult)
    assert result.next_sequence == 11
    assert session.writes == [b"\x10", encode_read_command(10, 1), b"\x10", encode_advance_command(11), b"\x10"]
    assert close_calls == [None]
    assert debug_records[0][0] == "firmware_observation_writer_error"
    assert debug_records[0][2]["operation"] == failure


@_async_test
async def test_cancellation_during_observation_close_propagates(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    close_started = threading.Event()
    release_close = threading.Event()

    class BlockingObservationWriter:
        def __init__(self, *_args: object, **_kwargs: object) -> None:
            pass

        def observe(self, _device_slug: str, _info: RingInfo) -> None:
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
        run_opportunistic_collector(
            Provider([session]), StagingStore(tmp_path, _capture_root(tmp_path)), "omi", _options()
        )
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
@pytest.mark.parametrize("kind", ["legacy", "multiple"])
async def test_partial_evidence_is_quarantined_before_provider_or_gatt(tmp_path: Path, kind: str) -> None:
    store = StagingStore(tmp_path, _capture_root(tmp_path))
    if kind == "legacy":
        store.prepare_streaming_attempt("omi", 100, 2)
    else:
        store.prepare_streaming_attempt("omi", 100, 2)
        store.prepare_streaming_attempt("omi", 102, 2)
    provider = Provider([ScriptedRingSession(_status(), (WriteStep(b"\x10", (_info(100, 100),)),))])
    result = await run_opportunistic_collector(provider, store, "omi", _options())

    assert isinstance(result, NoDataResult)
    assert provider.opened == 1
    assert not store.pending_attempts("omi")


@_async_test
@pytest.mark.parametrize("damage", ["checkpoint", "raw"])
async def test_malformed_resume_evidence_is_quarantined_before_provider(tmp_path: Path, damage: str) -> None:
    _seed_streaming_partial(tmp_path, count=2, persisted=1)
    partial = next((tmp_path / "attempts").iterdir())
    evidence = partial / ("checkpoint.json" if damage == "checkpoint" else "records.bin")
    if damage == "checkpoint":
        evidence.write_text("{malformed", encoding="utf-8")
    else:
        evidence.write_bytes(evidence.read_bytes() + b"torn")
    provider = Provider([ScriptedRingSession(_status(), (WriteStep(b"\x10", (_info(100, 100),)),))])
    result = await run_opportunistic_collector(
        provider, StagingStore(tmp_path, _capture_root(tmp_path)), "omi", _options()
    )

    assert isinstance(result, NoDataResult)
    assert provider.opened == 1
    assert not partial.exists()
    assert not evidence.exists()
    assert list((tmp_path / "quarantine" / "omi").iterdir())


@_async_test
async def test_resumed_complete_prefix_at_device_end_seals_without_read_or_advance(tmp_path: Path) -> None:
    records = _seed_streaming_partial(tmp_path, count=2, persisted=2)
    session = ScriptedRingSession(_status(), (WriteStep(b"\x10", (_info(102, 102),)),))

    result = await run_opportunistic_collector(
        Provider([session]), StagingStore(tmp_path, _capture_root(tmp_path)), "omi", _options()
    )

    assert isinstance(result, CollectionResult)
    assert isinstance(result.seal, SealResult)
    assert session.writes == [b"\x10"]
    assert not result.seal.deduplicated
    assert result.seal.bundle_path.joinpath("records.bin").read_bytes() == records


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
        Provider([session]), StagingStore(tmp_path, _capture_root(tmp_path)), "omi", options
    )

    assert isinstance(result, CollectionResult)
    assert not result.advance_confirmed
    assert session.writes == [b"\x10", encode_read_command(10, 1)]
    assert all(command[:1] != b"\x12" for command in session.writes)


@_async_test
async def test_restart_hydrates_checkpoint_and_resumes_at_durable_prefix(tmp_path: Path) -> None:
    records = _seed_streaming_partial(tmp_path, count=2, persisted=1)
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
        ),
    )

    result = await run_opportunistic_collector(
        Provider([session]), StagingStore(tmp_path, _capture_root(tmp_path)), "omi", _options()
    )

    assert isinstance(result, CollectionResult)
    assert isinstance(result.seal, SealResult)
    assert session.writes == [
        b"\x10",
        encode_read_command(101, 1),
        b"\x10",
        encode_advance_command(102),
        b"\x10",
    ]
    assert result.seal.bundle_path.joinpath("records.bin").read_bytes() == records


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
        Provider([session]), StagingStore(tmp_path, _capture_root(tmp_path)), "omi", _options(batch_records=end - start)
    )

    assert isinstance(result, CollectionResult)
    assert isinstance(result.seal, SealResult)
    assert session.writes[1] == encode_read_command(durable_next, end - durable_next)
    assert result.seal.bundle_path.joinpath("records.bin").read_bytes() == records


@_async_test
async def test_restart_after_seal_reads_fresh_cursor_before_any_advance(tmp_path: Path) -> None:
    records = _seed_streaming_partial(tmp_path, count=2, persisted=2)
    store = StagingStore(tmp_path, _capture_root(tmp_path))
    with store.device_lock("omi") as lease:
        sealed = store.resume_streaming_attempt("omi", lease)
        assert sealed is not None
        sealed_result = sealed.seal(DoneNotification(0, 102))
    assert sealed_result.bundle_path.exists()
    session = ScriptedRingSession(
        _status(),
        (
            WriteStep(b"\x10", (_info(100, 102),)),
            WriteStep(encode_read_command(100, 2), (_begin(100, 2), _data(records), _done(102))),
            WriteStep(b"\x10", (_info(100, 102),)),
            WriteStep(encode_advance_command(102), (b"\x01\x00",)),
            WriteStep(b"\x10", (_info(102, 102),)),
        ),
    )

    result = await run_opportunistic_collector(
        Provider([session]), StagingStore(tmp_path, _capture_root(tmp_path)), "omi", _options()
    )

    assert isinstance(result, CollectionResult)
    assert isinstance(result.seal, SealResult)
    assert result.seal.deduplicated
    assert session.writes[1] == encode_read_command(100, 2)
    assert session.writes[3] == encode_advance_command(102)


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
        Provider([session]), StagingStore(tmp_path, _capture_root(tmp_path)), "omi", _options()
    )

    assert isinstance(result, CollectionResult)
    assert result.next_sequence == 104
    assert encode_advance_command(102) not in session.writes
    bundles = tuple(path.name for path in (_capture_root(tmp_path) / "omi").iterdir() if path.is_dir())
    assert any(name.startswith("100-102-") for name in bundles)
    assert any(name.startswith("103-104-") for name in bundles)


@_async_test
async def test_restart_cursor_after_prefix_publishes_gap_and_continues_at_cursor(tmp_path: Path) -> None:
    _seed_streaming_partial(tmp_path, count=3, persisted=1)
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

    result = await run_opportunistic_collector(
        Provider([session]), StagingStore(tmp_path, _capture_root(tmp_path)), "omi", _options(batch_records=3)
    )

    assert isinstance(result, CollectionResult)
    assert session.writes[2] == encode_read_command(102, 1)
    assert encode_advance_command(102) not in session.writes


@_async_test
async def test_fresh_restart_cursor_ahead_publishes_prefix_and_reads_from_current_cursor(tmp_path: Path) -> None:
    _seed_streaming_partial(tmp_path, count=2, persisted=1)
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

    result = await run_opportunistic_collector(
        Provider([session]), StagingStore(tmp_path, _capture_root(tmp_path)), "omi", _options()
    )

    assert isinstance(result, CollectionResult)
    assert encode_read_command(103, 1) in session.writes
    assert encode_advance_command(102) not in session.writes
    prefix_bundles = tuple(
        path for path in (_capture_root(tmp_path) / "omi").iterdir() if path.name.startswith("100-101-")
    )
    assert len(prefix_bundles) == 1
    assert not (prefix_bundles[0] / "gap.json").exists()
    assert "gap_sha256" not in loads((prefix_bundles[0] / "receipt.json").read_text(encoding="utf-8"))
    retired = tuple((tmp_path / "attempts").iterdir())
    assert len(retired) == 1
    assert loads((retired[0] / "terminal-retired.json").read_text(encoding="utf-8"))["state"] == "terminal-retired"


@_async_test
async def test_startup_sweep_removes_only_aged_terminal_retired_partials(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    now = 1_000_000_000

    def wall_clock_ns() -> int:
        return now

    monkeypatch.setattr("omi_collector.capture.adapters.quarantine._wall_clock_ns", wall_clock_ns)
    store = StagingStore(
        tmp_path,
        _capture_root(tmp_path),
        config=CollectorConfig(staging_retention=StagingRetentionConfig(terminal_retention_seconds=1.0)),
    )
    attempt = store.prepare_streaming_attempt("omi", 100, 1)
    attempt.record_read_begin(ReadBeginNotification(100, 1))
    attempt.append_record(0, 100, _record(100))
    attempt.checkpoint()
    assert attempt.publish_prefix() is not None
    attempt.close(durable=True)
    store.terminalize_prefix_attempt("omi", attempt.attempt_id)
    now += 1_000_000_000
    session = ScriptedRingSession(_status(), (WriteStep(b"\x10", (_info(100, 100),)),))

    result = await run_opportunistic_collector(Provider([session]), store, "omi", _options())

    assert isinstance(result, NoDataResult)
    assert not attempt.path.exists()


@_async_test
async def test_legacy_attempt_cadence_sweeps_terminal_retired_partials_after_startup(
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
    store = StagingStore(
        tmp_path,
        _capture_root(tmp_path),
        config=CollectorConfig(
            retry=RetryConfig(maintenance_interval_seconds=1.0),
            staging_retention=StagingRetentionConfig(terminal_retention_seconds=1.0),
        ),
    )
    attempt = store.prepare_streaming_attempt("omi", 100, 1)
    attempt.record_read_begin(ReadBeginNotification(100, 1))
    attempt.append_record(0, 100, _record(100))
    attempt.checkpoint()
    assert attempt.publish_prefix() is not None
    attempt.close(durable=True)
    store.terminalize_prefix_attempt("omi", attempt.attempt_id)
    drained = Provider([ScriptedRingSession(_status(), (WriteStep(b"\x10", (_info(100, 100),)),))])

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
        "omi",
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
    assert not attempt.path.exists()


@_async_test
async def test_terminal_retired_sweep_failure_is_structured_and_does_not_stop_collection(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = StagingStore(tmp_path, _capture_root(tmp_path))
    events: list[tuple[str, BaseException, dict[str, object]]] = []

    def fail_sweep(_device_slug: str, *, should_defer: Callable[[], bool]) -> tuple[Path, ...]:
        del should_defer
        raise OSError("simulated sweep failure")

    def record(event: str, error: BaseException, **fields: object) -> None:
        events.append((event, error, fields))

    monkeypatch.setattr(store, "sweep_terminal_retired", fail_sweep)

    def runtime_debug_exception(
        _runtime_instance: OpportunisticRuntime, event: str, error: BaseException, **fields: object
    ) -> None:
        record(event, error, **fields)

    monkeypatch.setattr(OpportunisticRuntime, "debug_exception", runtime_debug_exception)
    session = ScriptedRingSession(_status(), (WriteStep(b"\x10", (_info(100, 100),)),))

    result = await run_opportunistic_collector(Provider([session]), store, "omi", _options())

    assert isinstance(result, NoDataResult)
    assert len(events) == 1
    for event, error, fields in events:
        assert event == "terminal_retired_sweep_failed"
        assert isinstance(error, OSError)
        assert fields == {"device_slug": "omi"}


@_async_test
async def test_startup_scan_wake_defers_and_joins_quarantine_before_provider(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    scan_started = asyncio.Event()
    maintenance_started = asyncio.Event()
    maintenance_stopped = asyncio.Event()
    release_wake = asyncio.Event()

    class BlockingPresence:
        closed = False
        policy = PresencePolicy(
            fallback_seconds=30.0,
            rapid_backoff=(0.001,),
            drained_fallback_seconds=DEFAULT_CONFIG.presence.fallback_seconds,
        )

        async def wait_for_attempt(self) -> PresenceWake:
            scan_started.set()
            await release_wake.wait()
            return PresenceWake("advertisement")

        async def attempt_finished(self, _outcome: AttemptOutcome) -> None:
            return None

        async def close(self) -> None:
            self.closed = True

    presence = BlockingPresence()
    store = StagingStore(tmp_path, _capture_root(tmp_path))

    async def slow_maintenance(
        _maintenance: QuarantineMaintenance,
        should_defer: Callable[[], bool],
    ) -> None:
        assert scan_started.is_set()
        maintenance_started.set()
        while not should_defer():
            await asyncio.sleep(0)
        maintenance_stopped.set()

    class OrderedProvider(Provider):
        def __call__(self, candidate: object | None = None):
            assert maintenance_stopped.is_set()
            return super().__call__(candidate)

    monkeypatch.setattr(QuarantineMaintenance, "run_once", slow_maintenance)
    session = ScriptedRingSession(_status(), (WriteStep(b"\x10", (_info(100, 100),)),))
    task = asyncio.create_task(
        run_opportunistic_collector(
            OrderedProvider([session]),
            store,
            "omi",
            replace(_options(), presence=cast(PresenceScheduler, presence)),
        )
    )
    await maintenance_started.wait()
    release_wake.set()

    assert isinstance(await task, NoDataResult)
    assert maintenance_stopped.is_set()
    assert presence.closed


@_async_test
async def test_coordinator_cancellation_joins_quarantine_maintenance(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    worker_started = threading.Event()
    worker_stopped = threading.Event()

    class BlockingPresence:
        closed = False
        policy = PresencePolicy(
            fallback_seconds=30.0,
            rapid_backoff=(0.001,),
            drained_fallback_seconds=DEFAULT_CONFIG.presence.fallback_seconds,
        )

        async def wait_for_attempt(self) -> PresenceWake:
            await asyncio.Event().wait()
            raise AssertionError("unreachable")

        async def close(self) -> None:
            self.closed = True

    def maintenance_worker(should_defer: Callable[[], bool]) -> None:
        worker_started.set()
        while not should_defer():
            time.sleep(0.001)
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
            StagingStore(tmp_path, _capture_root(tmp_path)),
            "omi",
            replace(_options(), presence=cast(PresenceScheduler, presence)),
        )
    )
    assert await asyncio.to_thread(worker_started.wait, 1)
    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task
    assert worker_stopped.is_set()
    assert presence.closed


@_async_test
async def test_fresh_restart_cursor_ahead_at_write_watermark_publishes_prefix_and_drains(tmp_path: Path) -> None:
    _seed_streaming_partial(tmp_path, count=2, persisted=1)
    session = ScriptedRingSession(
        _status(),
        (
            WriteStep(b"\x10", (_info(103, 103),)),
            WriteStep(b"\x10", (_info(103, 103),)),
        ),
    )

    result = await run_opportunistic_collector(
        Provider([session]), StagingStore(tmp_path, _capture_root(tmp_path)), "omi", _options()
    )

    assert isinstance(result, CollectionResult)
    assert session.writes == [b"\x10", b"\x10"]
    assert encode_advance_command(102) not in session.writes
    prefix_bundles = tuple(
        path for path in (_capture_root(tmp_path) / "omi").iterdir() if path.name.startswith("100-101-")
    )
    assert len(prefix_bundles) == 1
    assert not (prefix_bundles[0] / "gap.json").exists()


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
        StagingStore(tmp_path, _capture_root(tmp_path)),
        "omi",
        OpportunisticOptions(
            TransferTimeouts(1, 1),
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
        result = await run_opportunistic_collector(
            provider, StagingStore(tmp_path, _capture_root(tmp_path)), "omi", _options()
        )
        assert isinstance(result, NoDataResult)
        assert provider.opened == 1
        return

    result = await run_opportunistic_collector(
        Provider([session]), StagingStore(tmp_path, _capture_root(tmp_path)), "omi", _options()
    )

    assert isinstance(result, CollectionResult)
    if requested:
        assert session.writes[1] == encode_read_command(read_start, requested)
        assert session.writes[3] == encode_advance_command(102)
    else:
        assert read in (100, 102)


@_async_test
async def test_pending_cursor_ahead_publishes_prefix_and_reads_from_current_cursor(tmp_path: Path) -> None:
    _seed_streaming_partial(tmp_path, count=2, persisted=1)
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

    result = await run_opportunistic_collector(
        Provider([session]), StagingStore(tmp_path, _capture_root(tmp_path)), "omi", _options()
    )

    assert isinstance(result, CollectionResult)
    assert encode_read_command(103, 1) in session.writes
    assert not StagingStore(tmp_path, _capture_root(tmp_path)).pending_attempts("omi")
    assert any(path.name.startswith("100-101-") for path in (_capture_root(tmp_path) / "omi").iterdir())


@_async_test
async def test_live_cursor_ahead_publishes_prefix_event_and_reads_remaining_audio(tmp_path: Path) -> None:
    _seed_streaming_partial(tmp_path, count=4, persisted=0)
    activity: list[ActivityEvent] = []
    operational_events: list[dict[str, object]] = []
    journal = JsonlQualityMetrics(tmp_path, release_version="test-version", source_revision="abcdef1")

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
        StagingStore(tmp_path, _capture_root(tmp_path)),
        "omi",
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
    assert any(event.get("event") == "loss_detected" for event in operational_events)
    metrics = [cast(dict[str, object], loads(line)) for line in journal.path.read_text(encoding="utf-8").splitlines()]
    loss = next(item for item in metrics if item["event"] == "sequence_loss")
    assert loss == {
        "schema_version": 1,
        "event": "sequence_loss",
        "occurred_at": "1970-01-01T00:16:40.000+00:00",
        "session_id": loss["session_id"],
        "device_slug": "omi",
        "missing_record_count": 1,
        "missing_raw_bytes": RECORD_SIZE,
        "reason": "device_cursor_advanced_before_host_durable_prefix",
        "release_version": "test-version",
        "source_revision": "abcdef1",
        "firmware_version": None,
    }


@_async_test
async def test_quality_metrics_failure_does_not_change_completed_audio_collection(tmp_path: Path) -> None:
    class FailingMetrics:
        release_version = "test-version"
        source_revision = None

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
        TransferTimeouts(1, 1),
        RetryPolicy(backoff=(0.001,), batch_records=1, advance_enabled=False, stop_after_drained=True),
        clock=clock,
        sleep=clock.sleep,
        host_time=lambda: 1000.0,
        quality_metrics=FailingMetrics(),  # type: ignore[arg-type]
    )

    result = await run_opportunistic_collector(
        Provider([session]), StagingStore(tmp_path, _capture_root(tmp_path)), "omi", options
    )

    assert isinstance(result, CollectionResult)
    assert result.packet_count == 1


@_async_test
async def test_completed_read_emits_one_terminal_transfer_session_with_raw_counters(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    journal = JsonlQualityMetrics(tmp_path, release_version="test-version", source_revision="abcdef1")
    metric_threads: list[threading.Thread] = []
    original_append = journal._append

    def observe_append(value: bytes) -> None:
        metric_threads.append(threading.current_thread())
        original_append(value)

    monkeypatch.setattr(journal, "_append", observe_append)
    session = ScriptedRingSession(
        _status(),
        (
            WriteStep(b"\x10", (_info(10, 11),)),
            WriteStep(encode_read_command(10, 1), (_begin(10, 1), _data(_record(10)), _done(11))),
        ),
    )
    clock = Clock()
    options = OpportunisticOptions(
        TransferTimeouts(1, 1),
        RetryPolicy(backoff=(0.001,), batch_records=1, advance_enabled=False, stop_after_drained=True),
        clock=clock,
        sleep=clock.sleep,
        host_time=lambda: 1000.0,
        quality_metrics=journal,
        phy_policy="force_1m",
    )

    result = await run_opportunistic_collector(
        Provider([session]), StagingStore(tmp_path, _capture_root(tmp_path)), "omi", options
    )

    assert isinstance(result, CollectionResult)
    [metric] = [cast(dict[str, object], loads(line)) for line in journal.path.read_text(encoding="utf-8").splitlines()]
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
    operational_events: list[dict[str, object]] = []

    def emit_operational(event: Mapping[str, object]) -> None:
        operational_events.append(dict(event))

    session = _real_ring_gap_session(cursor, write)

    result = await run_opportunistic_collector(
        Provider([session]),
        StagingStore(tmp_path, _capture_root(tmp_path)),
        "omi",
        replace(_options(batch_records=4096), operational=emit_operational),
    )
    assert isinstance(result, CollectionResult)

    prefix_bytes = _records(start, prefix_end - start)
    prefix_hash = sha256(prefix_bytes).hexdigest()
    prefix_bundle = _capture_root(tmp_path) / "omi" / f"{start}-{prefix_end}-{prefix_hash[:16]}"
    receipt = cast(dict[str, object], loads(prefix_bundle.joinpath("receipt.json").read_text()))
    assert prefix_bundle.joinpath("records.bin").read_bytes() == prefix_bytes
    assert not prefix_bundle.joinpath("gap.json").exists()
    assert "gap_sha256" not in receipt
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
        StagingStore(tmp_path, _capture_root(tmp_path)),
        "omi",
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
            StagingStore(tmp_path, _capture_root(tmp_path)),
            "omi",
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
            StagingStore(tmp_path, _capture_root(tmp_path)),
            "omi",
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

    class Observer:
        async def start(self, callback: PresenceCallback) -> None:
            del callback

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
        policy=PresencePolicy(fallback_seconds=1.0, drained_fallback_seconds=cooldown_seconds, rapid_backoff=(1.0,)),
        clock=clock,
        sleep=clock.sleep,
    )
    with pytest.raises(StopAfterCooldownError):
        await run_opportunistic_collector(
            lambda _candidate: Context(),
            StagingStore(tmp_path, _capture_root(tmp_path)),
            "omi",
            OpportunisticOptions(
                TransferTimeouts(1, 1),
                RetryPolicy(backoff=(1,), drain_cooldown_seconds=cooldown_seconds),
                activity=report_activity,
                clock=clock,
                sleep=clock.sleep,
                presence=presence,
            ),
        )

    cooldowns = [event for event in activity if event.state == "cooldown_started"]
    assert len(cooldowns) == 1
    assert cooldowns[0].reason == "clean_drain"
    assert cooldowns[0].duration_seconds == cooldown_seconds
    assert cooldowns[0].next_attempt_in_seconds == cooldown_seconds - elapsed_during_scan_start


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
        StagingStore(tmp_path, _capture_root(tmp_path)),
        "omi",
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
async def test_connected_interruption_after_initial_fallback_uses_first_rapid_retry(tmp_path: Path) -> None:
    clock = Clock()
    opened = 0

    class Observer:
        async def start(self, callback: Callable[[object], object]) -> None:
            del callback

        async def stop(self) -> None:
            """Stop the fake scan."""

    first = ScriptedRingSession(_status(), (WriteStep(b"\x10", error=RingTransportDisconnectedError("gone")),))
    second = ScriptedRingSession(_status(), (WriteStep(b"\x10", (_info(100, 100),)),))

    def provider(_candidate: object | None = None) -> AbstractAsyncContextManager[ScriptedRingSession]:
        nonlocal opened
        opened += 1
        session = first if opened == 1 else second

        @asynccontextmanager
        async def context() -> AsyncIterator[ScriptedRingSession]:
            yield session

        return context()

    presence = PresenceScheduler(
        Observer(),
        policy=PresencePolicy(fallback_seconds=30, drained_fallback_seconds=30, rapid_backoff=(1,)),
        clock=clock,
        sleep=clock.sleep,
    )
    result = await run_opportunistic_collector(
        provider,
        StagingStore(tmp_path, _capture_root(tmp_path)),
        "omi",
        OpportunisticOptions(
            TransferTimeouts(1, 1),
            RetryPolicy(backoff=(1,), drain_cooldown_seconds=30, stop_after_drained=True),
            clock=clock,
            sleep=clock.sleep,
            presence=presence,
        ),
    )

    assert isinstance(result, NoDataResult)
    assert opened == 2
    assert clock.value == 31


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
        ClosingProvider([first, second]), StagingStore(tmp_path, _capture_root(tmp_path)), "omi", _options()
    )

    assert isinstance(result, CollectionResult)
    assert result.packet_count == 2
    assert isinstance(result.seal, SealResult)
    assert result.advance_confirmed
    assert result.next_sequence == 12
    assert result.seal.bundle_path.joinpath("records.bin").read_bytes() == _records(10, 2)
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
        policy=PresencePolicy(fallback_seconds=1, drained_fallback_seconds=1, rapid_backoff=(1,)),
    )
    result = await run_opportunistic_collector(
        provider,
        StagingStore(tmp_path, _capture_root(tmp_path)),
        "omi",
        OpportunisticOptions(
            TransferTimeouts(1, 1),
            RetryPolicy(backoff=(1,), drain_cooldown_seconds=1, stop_after_drained=True),
            presence=presence,
        ),
    )

    assert isinstance(result, NoDataResult)
    assert seen == [candidate]


@_async_test
async def test_stale_candidate_restarts_scan_for_next_candidate_without_address_fallback(tmp_path: Path) -> None:
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
        policy=PresencePolicy(fallback_seconds=1, drained_fallback_seconds=1, rapid_backoff=(1,)),
    )
    result = await run_opportunistic_collector(
        provider,
        StagingStore(tmp_path, _capture_root(tmp_path)),
        "omi",
        OpportunisticOptions(
            TransferTimeouts(1, 1),
            RetryPolicy(backoff=(1,), drain_cooldown_seconds=1, stop_after_drained=True),
            presence=presence,
        ),
    )

    assert isinstance(result, NoDataResult)
    assert seen == [candidate_a, candidate_b]


@_async_test
async def test_connect_failure_without_presence_keeps_fallback_not_rapid_retry(tmp_path: Path) -> None:
    clock = Clock()
    opened = 0

    class Observer:
        async def start(self, callback: Callable[[object], object]) -> None:
            del callback

        async def stop(self) -> None:
            """Stop the fake scan."""

    second = ScriptedRingSession(_status(), (WriteStep(b"\x10", (_info(100, 100),)),))

    def provider(_candidate: object | None = None) -> AbstractAsyncContextManager[ScriptedRingSession]:
        nonlocal opened
        opened += 1
        if opened == 1:
            raise RingTransportUnavailableError("away")

        @asynccontextmanager
        async def context() -> AsyncIterator[ScriptedRingSession]:
            yield second

        return context()

    presence = PresenceScheduler(
        Observer(),
        policy=PresencePolicy(fallback_seconds=30, drained_fallback_seconds=30, rapid_backoff=(1,)),
        clock=clock,
        sleep=clock.sleep,
    )
    result = await run_opportunistic_collector(
        provider,
        StagingStore(tmp_path, _capture_root(tmp_path)),
        "omi",
        OpportunisticOptions(
            TransferTimeouts(1, 1),
            RetryPolicy(backoff=(1,), drain_cooldown_seconds=30, stop_after_drained=True),
            clock=clock,
            sleep=clock.sleep,
            presence=presence,
        ),
    )

    assert isinstance(result, NoDataResult)
    assert opened == 2
    assert clock.value == 60


async def _run_teardown_failure_case(tmp_path: Path, *, with_batch: bool) -> None:
    clock = Clock()
    opened = 0

    class Observer:
        async def start(self, callback: Callable[[object], object]) -> None:
            del callback

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
    second = ScriptedRingSession(_status(), (WriteStep(b"\x10", (_info(100, 100),)),))

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
        policy=PresencePolicy(fallback_seconds=30, drained_fallback_seconds=30, rapid_backoff=(1,)),
        clock=clock,
        sleep=clock.sleep,
    )
    result = await run_opportunistic_collector(
        provider,
        StagingStore(tmp_path, _capture_root(tmp_path)),
        "omi",
        OpportunisticOptions(
            TransferTimeouts(1, 1),
            RetryPolicy(backoff=(1,), batch_records=1, drain_cooldown_seconds=30, stop_after_drained=True),
            clock=clock,
            sleep=clock.sleep,
            presence=presence,
        ),
    )

    assert isinstance(result, (NoDataResult, CollectionResult))
    assert opened == 2
    assert clock.value == 31


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
    result = await run_opportunistic_collector(
        provider, StagingStore(tmp_path, _capture_root(tmp_path)), "omi", _options()
    )

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

    def observe(event: ActivityEvent) -> None:
        if event.state != "away":
            return
        checkpoints = tuple(tmp_path.rglob("checkpoint.json"))
        assert len(checkpoints) == 1
        checkpoint_value = cast(object, loads(checkpoints[0].read_text(encoding="utf-8")))
        assert isinstance(checkpoint_value, dict)
        checkpoint = cast(dict[str, object], checkpoint_value)
        durable_after_visit.append(cast(int, checkpoint["record_count"]))
        device_root = capture_root / "omi"
        published_after_visit.append(
            sum(1 for path in device_root.iterdir() if path.is_dir()) if device_root.is_dir() else 0
        )

    provider = Provider([first, second, third])
    result = await run_opportunistic_collector(
        provider,
        StagingStore(tmp_path, capture_root),
        "omi",
        replace(_options(batch_records=6), activity=observe),
    )

    assert isinstance(result, CollectionResult)
    assert isinstance(result.seal, SealResult)
    assert result.next_sequence == 16
    assert provider.opened == 3
    assert durable_after_visit == [2, 4]
    assert published_after_visit == [0, 0]
    assert result.seal.bundle_path.name.startswith("10-16-")


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
        TransferTimeouts(30, 1),
        policy=RetryPolicy(backoff=(0.001,), batch_records=1, stop_after_drained=True),
    )

    result = await run_opportunistic_collector(
        Provider([session]), StagingStore(tmp_path, _capture_root(tmp_path)), "omi", options
    )

    assert isinstance(result, CollectionResult)
    assert session.status_reads == 0
    assert session.writes[0] == b"\x10"


@_async_test
async def test_presence_preflight_budget_covers_status_and_optional_reads(tmp_path: Path) -> None:
    config = CollectorConfig(retry=RetryConfig(presence_preflight_budget_seconds=0.05))
    order: list[str] = []

    class SlowPreflightSession(ScriptedRingSession):
        async def read_status(self) -> RingStatus:
            order.append("status")
            await asyncio.sleep(config.retry.presence_preflight_budget_seconds * 0.75)
            return self._status

        async def read_optional_characteristic(self, uuid: str) -> bytes | None:
            del uuid
            order.append("optional")
            await asyncio.Future()
            return None

        async def write_control(self, payload: bytes) -> None:
            if payload == b"\x10":
                order.append("info")
            await super().write_control(payload)

    session = SlowPreflightSession(_status(), (WriteStep(b"\x10", (_info(10, 10),)),))

    def emit(_event: Mapping[str, object]) -> None:
        return None

    started_at = time.monotonic()
    result = await asyncio.wait_for(
        run_opportunistic_collector(
            Provider([session]),
            StagingStore(tmp_path, _capture_root(tmp_path)),
            "omi",
            OpportunisticOptions(
                TransferTimeouts(info=30.0, transfer=600.0),
                policy=RetryPolicy(backoff=(0.001,), stop_after_drained=True),
                operational=emit,
                host_clock_synchronized=lambda: False,
                config=config,
            ),
        ),
        timeout=0.5,
    )
    elapsed = time.monotonic() - started_at

    assert isinstance(result, NoDataResult)
    assert order[:2] == ["info", "status"]
    assert "optional" in order
    assert elapsed < 0.2


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

    def blocking_trust_probe() -> bool:
        started.set()
        release.wait()
        finished.set()
        return False

    collection = asyncio.create_task(
        run_opportunistic_collector(
            Provider([session]),
            StagingStore(tmp_path, _capture_root(tmp_path)),
            "omi",
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
        # Collection must finish after the configured preflight bound expires,
        # even though this trust probe remains blocked until the test releases it.
        result = await asyncio.wait_for(collection, timeout=5.0)
        assert not release.is_set()
        assert not finished.is_set()
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
        StagingStore(tmp_path, _capture_root(tmp_path)),
        "omi",
        OpportunisticOptions(
            TransferTimeouts(1, 1),
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

    result = await run_opportunistic_collector(
        Provider([first, recovery, gapped]),
        StagingStore(tmp_path, _capture_root(tmp_path)),
        "omi",
        _options(batch_records=3),
    )

    assert isinstance(result, CollectionResult)
    assert recovery.writes[1] == encode_read_command(12, 1)
    assert gapped.writes == [b"\x10", b"\x10"]
    assert isinstance(result.seal, SealResult)
    assert not (result.seal.bundle_path / "gap.json").exists()


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
        StagingStore(tmp_path, _capture_root(tmp_path)),
        "omi",
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
        Provider([first, recovered]), StagingStore(tmp_path, _capture_root(tmp_path)), "omi", _options()
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


def test_only_storage_not_ready_ack_is_retryable() -> None:
    assert retryable(RingAcknowledgementError(STATUS_STORAGE_NOT_READY))
    assert not retryable(RingAcknowledgementError(1))


def test_interrupted_read_timeout_is_retryable() -> None:
    error = TransferInterruptedError("timed out", TransferCounters(0, 0, 0), cause=TimeoutError())

    assert retryable(error)


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

    def quarantine_source(store: StagingStore, device_slug: str, attempt_id: str) -> Path:
        ordering.append("quarantine")
        return original_quarantine(store, device_slug, attempt_id)

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

    result = await run_opportunistic_collector(
        Provider([first, second]), StagingStore(tmp_path, _capture_root(tmp_path)), "omi", _options(batch_records=3)
    )
    assert isinstance(result, (CollectionResult, NoDataResult))
    assert not StagingStore(tmp_path, _capture_root(tmp_path)).pending_attempts("omi")
    assert second.writes[0] == b"\x10"
    quarantine = tmp_path / "quarantine" / "omi"
    if case == "gap":
        assert not quarantine.exists()
    else:
        destinations = tuple(quarantine.iterdir())
        assert len(destinations) == 1
        destination = destinations[0]
        assert destination.is_dir()
        assert {path.name for path in destination.iterdir()} == {
            "attempt.json",
            "records.bin",
            "checkpoint.json",
        }
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

    result = await run_opportunistic_collector(
        Provider([session]), StagingStore(tmp_path, _capture_root(tmp_path)), "omi", _options(batch_records=3)
    )

    assert isinstance(result, CollectionResult)
    quarantine = tmp_path / "quarantine" / "omi"
    destinations = tuple(quarantine.iterdir())
    assert len(destinations) == 1
    assert {path.name for path in destinations[0].iterdir()} == {
        "attempt.json",
        "records.bin",
        "checkpoint.json",
    }


@_async_test
async def test_admission_prefix_mismatch_closes_writer_and_releases_lease(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _seed_streaming_partial(tmp_path, count=2, persisted=1)
    original_threads = {thread for thread in threading.enumerate() if thread.name == "omi-attempt-writer"}

    def mismatched_prepare_leg(self: object, _start: int, _count: int) -> DurablePrefix:
        del self
        return DurablePrefix(100, 100, 0, "mismatch")

    monkeypatch.setattr(
        "omi_collector.capture.adapters.opportunistic_runtime._StagingWriterAdapter.prepare_leg",
        mismatched_prepare_leg,
    )
    session = ScriptedRingSession(_status(), (WriteStep(b"\x10", (_info(100, 102),)),))

    with pytest.raises(CursorConsistencyError, match="prepared durable prefix"):
        await run_opportunistic_collector(
            Provider([session]), StagingStore(tmp_path, _capture_root(tmp_path)), "omi", _options()
        )

    assert StagingStore(tmp_path, _capture_root(tmp_path)).pending_attempts("omi")
    assert not ({thread for thread in threading.enumerate() if thread.name == "omi-attempt-writer"} - original_threads)


@_async_test
async def test_cursor_regression_after_advance_ack_keeps_bundle_and_continues(tmp_path: Path) -> None:
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

    result = await run_opportunistic_collector(
        Provider([session]), StagingStore(tmp_path, _capture_root(tmp_path)), "omi", _options(batch_records=3)
    )

    assert isinstance(result, CollectionResult)
    assert session.writes.count(encode_advance_command(102)) == 1
    assert result.next_sequence == 102


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

    monkeypatch.setattr("omi_collector.capture.adapters.attempts.StagedAttempt.seal", fail_seal)
    with pytest.raises(OSError, match="disk fault"):
        await run_opportunistic_collector(
            Provider([session]), StagingStore(tmp_path, _capture_root(tmp_path)), "omi", _options(batch_records=1)
        )
    assert encode_advance_command(11) not in session.writes


@_async_test
async def test_post_session_checkpoint_surfaces_latched_writer_failure_identity(tmp_path: Path) -> None:
    latched = OSError("latched disk failure")
    wrapped = WriterFailedError("writer boundary failed")

    class FailedWriter:
        progress = WriterProgress(RECORD_SIZE, 0)
        failure = latched

        async def checkpoint(self) -> object:
            raise wrapped

    async def quarantine(_attempt_id: str) -> None:
        return None

    reconciler = BatchReconciler(
        StagingStore(tmp_path, _capture_root(tmp_path)),
        "omi",
        _options(),
        _runtime(),
        quarantine,
    )
    reconciler._state.batch = batch_reconciliation._Batch(
        RingInfo(10, 11, 10000, 0, RECORD_SIZE),
        "omi",
        10,
        11,
        TransferArena(10, 1, max_bytes=RECORD_SIZE),
        cast(BatchWriterPort, FailedWriter()),
    )

    with pytest.raises(OSError) as raised:
        await reconciler.checkpoint_after_session()
    assert raised.value is latched


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
                StagingStore(tmp_path, _capture_root(tmp_path)),
                "omi",
                _options(activity=activity),
            )
    else:
        result = await run_opportunistic_collector(
            Provider([first, second]),
            StagingStore(tmp_path, _capture_root(tmp_path)),
            "omi",
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
    assert policy.drain_cooldown_seconds == DEFAULT_CONFIG.presence.fallback_seconds
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
    task = asyncio.create_task(
        run_opportunistic_collector(
            Provider([first]), StagingStore(tmp_path, _capture_root(tmp_path)), "omi", _options(batch_records=1)
        )
    )
    await entered.wait()
    with pytest.raises(DeviceAlreadyRunningError):
        await run_opportunistic_collector(
            Provider([second]), StagingStore(tmp_path, _capture_root(tmp_path)), "omi", _options(batch_records=1)
        )
    task.cancel()
    with pytest.raises(CollectionPreservedCancelledError):
        await task


@_async_test
@pytest.mark.parametrize(
    ("phase", "expected_kind"),
    [("read", "partial collection"), ("advance", "sealed bundle"), ("confirm", "sealed bundle")],
)
async def test_cancellation_reports_existing_partial_or_bundle_path(
    tmp_path: Path, phase: str, expected_kind: str
) -> None:
    entered = asyncio.Event()

    class GatedSession(ScriptedRingSession):
        async def write_control(self, payload: bytes) -> None:
            await super().write_control(payload)
            if phase == "read" and payload == encode_read_command(10, 2):
                entered.set()
            if phase == "advance" and payload == encode_advance_command(12):
                entered.set()
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
    task = asyncio.create_task(
        run_opportunistic_collector(
            Provider([session]), StagingStore(tmp_path, _capture_root(tmp_path)), "omi", _options()
        )
    )
    await entered.wait()
    for _ in range(3):
        await asyncio.sleep(0)
    task.cancel()

    with pytest.raises(CollectionPreservedCancelledError) as raised:
        await task
    assert raised.value.kind == expected_kind
    assert raised.value.preserved_path.exists()
    if phase == "read":
        assert raised.value.preserved_path == tmp_path / "attempts"
    else:
        assert raised.value.preserved_path.parent == _capture_root(tmp_path) / "omi"


@_async_test
async def test_stalled_final_checkpoint_still_attempts_close_and_releases_lease(  # noqa: PLR0915
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    checkpoint_started = threading.Event()
    release_checkpoint = threading.Event()
    close_attempted = threading.Event()
    target_closed = threading.Event()
    data_submitted = threading.Event()
    closed_writers: list[AttemptWriter] = []
    activity: list[ActivityEvent] = []
    original_checkpoint = StagingWriter.checkpoint
    original_target_close = StagingWriter.close
    original_writer_close = AttemptWriter.close
    read_begin_completed = _patch_read_begin_completion(monkeypatch)

    def stalled_checkpoint(self: StagingWriter) -> object:
        checkpoint_started.set()
        assert release_checkpoint.wait(2)
        return original_checkpoint(self)

    def observed_target_close(self: StagingWriter) -> object:
        try:
            return original_target_close(self)
        finally:
            target_closed.set()

    async def observed_writer_close(self: AttemptWriter, *, timeout: float | None = None) -> object:
        close_attempted.set()
        closed_writers.append(self)
        return await original_writer_close(self, timeout=timeout)

    def observed_publish(
        self: AttemptWriter,
        high_water: int,
        original_publish: Callable[[AttemptWriter, int], bool] = AttemptWriter.publish,
    ) -> bool:
        published = original_publish(self, high_water)
        if published:
            data_submitted.set()
        return published

    monkeypatch.setattr("omi_collector.capture.adapters.staging_writer.StagingWriter.checkpoint", stalled_checkpoint)
    monkeypatch.setattr("omi_collector.capture.adapters.staging_writer.StagingWriter.close", observed_target_close)
    monkeypatch.setattr("omi_collector.capture.adapters.attempt_writer.AttemptWriter.close", observed_writer_close)
    monkeypatch.setattr("omi_collector.capture.adapters.attempt_writer.AttemptWriter.publish", observed_publish)

    entered = asyncio.Event()

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
    task = asyncio.create_task(
        run_opportunistic_collector(
            Provider([session]),
            StagingStore(tmp_path, _capture_root(tmp_path)),
            "omi",
            replace(_options(batch_records=1, activity=activity), timeouts=TransferTimeouts(0.03, 0.03)),
        )
    )
    await entered.wait()
    assert await asyncio.to_thread(data_submitted.wait, 1)
    assert await asyncio.to_thread(read_begin_completed.wait, 1)
    task.cancel()

    try:
        assert await asyncio.to_thread(checkpoint_started.wait, 1)
        with pytest.raises(asyncio.CancelledError):
            await task
        assert close_attempted.is_set()
        assert not target_closed.is_set()
        errors = {event.state: event for event in activity}
        assert errors["writer_checkpoint_error"].error_type == "CollectorTimeoutError"
        assert errors["writer_close_error"].error_type == "CollectorTimeoutError"
    finally:
        release_checkpoint.set()

    assert await asyncio.to_thread(target_closed.wait, 1)
    assert len(closed_writers) == 1
    assert await asyncio.to_thread(closed_writers[0].thread.join, 1) is None
    assert not closed_writers[0].thread.is_alive()
    with StagingStore(tmp_path, _capture_root(tmp_path)).device_lock("omi"):
        pass


@_async_test
async def test_no_data_async_activity_timeout_retry_and_policy_validation(tmp_path: Path) -> None:
    events: list[str] = []

    async def activity(event: ActivityEvent) -> None:
        await asyncio.sleep(0)
        events.append(event.state)

    session = ScriptedRingSession(_status(), (WriteStep(b"\x10", (_info(10, 10),)),))
    result = await run_opportunistic_collector(
        Provider([session]),
        StagingStore(tmp_path, _capture_root(tmp_path)),
        "omi",
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
                StagingStore(tmp_path / str(policy), tmp_path / f"{policy}-captures"),
                "omi",
                OpportunisticOptions(TransferTimeouts(1, 1), policy),
            )

    timed_out = ScriptedRingSession(_status(), (WriteStep(b"\x10"),))
    drained = ScriptedRingSession(_status(), (WriteStep(b"\x10", (_info(10, 10),)),))
    result = await run_opportunistic_collector(
        Provider([timed_out, drained]),
        StagingStore(tmp_path / "timeout", tmp_path / "timeout-captures"),
        "omi",
        OpportunisticOptions(
            TransferTimeouts(0.01, 1), RetryPolicy(backoff=(0.001,), stop_after_drained=True), sleep=asyncio.sleep
        ),
    )
    assert result.info.read_sequence == 10
