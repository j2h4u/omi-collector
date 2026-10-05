"""Focused contracts for the physical-session lifecycle boundary."""

from __future__ import annotations

import asyncio
import time
from collections.abc import Callable, Coroutine, Iterator, Mapping
from contextlib import AbstractAsyncContextManager
from dataclasses import FrozenInstanceError, replace
from pathlib import Path
from struct import pack
from typing import Literal, Never, cast

import pytest

from omi_collector.capture.adapters.clock_corrections import ClockCorrectionStore
from omi_collector.capture.adapters.opportunistic_runtime import OpportunisticRuntime
from omi_collector.capture.adapters.staging_contract import DeviceAlreadyRunningError
from omi_collector.capture.adapters.staging_store import StagingStore
from omi_collector.capture.application.collector import (
    AdvanceUncertainError,
    CollectorTimeoutError,
    NoDataResult,
    TransferCounters,
    TransferInterruptedError,
    TransferTimeouts,
)
from omi_collector.capture.application.operational_telemetry import TIME_READ_UUID, ClockCorrectionSink, TelemetryClock
from omi_collector.capture.application.ports import CaptureRuntimePort
from omi_collector.capture.application.presence import (
    PresenceAdvertisement,
    PresenceEnd,
    PresencePolicy,
    PresenceScheduler,
    PresenceWake,
)
from omi_collector.capture.application.presence_machine import (
    AttemptOutcome,
    CandidateUnavailable,
    CleanDrain,
    ConnectedInterruption,
    NotConnected,
)
from omi_collector.capture.application.quality_metrics import ClockCorrectionMetric, TransferSessionMetric
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
    InfoReader,
    OpportunisticOptions,
    RetryPolicy,
    SessionLifecycle,
    SessionLifecycleCallbacks,
    SessionLifecycleRun,
    SessionPhaseState,
    bounded,
    exit_context,
    presence_attempt_outcome,
    recoverable_session_outcome,
    report_session_error,
    storage_not_ready_delay,
    teardown_was_interrupted,
    validate_policy,
)
from omi_collector.capture.application.visit_machine import DrainConfirmed, RecoveryDisposition
from omi_collector.capture.domain.ring_protocol import RECORD_SIZE, RingInfo
from omi_collector.config import DEFAULT_CONFIG, CollectorConfig, TelemetryConfig


def _run(coroutine: Coroutine[object, object, object]) -> object:
    return asyncio.run(coroutine)


class _ScriptedPresence:
    policy = PresencePolicy(rapid_backoff=(0.01,))
    drained_cooldown_remaining_seconds = 0.0

    def __init__(
        self,
        wakes: list[PresenceWake | PresenceEnd | BaseException],
        end_results: list[PresenceEnd | None] | None = None,
    ) -> None:
        self._wakes: Iterator[PresenceWake | PresenceEnd | BaseException] = iter(wakes)
        self._end_results: Iterator[PresenceEnd | None] = iter(end_results or [])
        self.wake_count = 0
        self.outcomes: list[AttemptOutcome] = []
        self.resumed = 0
        self.closed = False

    def resume_interrupted_visit(self) -> None:
        self.resumed += 1

    async def wait_for_attempt(self) -> PresenceWake | PresenceEnd:
        self.wake_count += 1
        value = next(self._wakes)
        if isinstance(value, BaseException):
            raise value
        return value

    async def attempt_finished(self, outcome: AttemptOutcome) -> PresenceEnd | None:
        self.outcomes.append(outcome)
        try:
            return next(self._end_results)
        except StopIteration:
            return None

    async def close(self) -> None:
        self.closed = True


class _NoopRingContext:
    def __init__(self, session: RingSession | None = None) -> None:
        self.session = session or cast(RingSession, object())

    async def __aenter__(self) -> RingSession:
        return self.session

    async def __aexit__(self, _type: object, _value: object, _traceback: object) -> None:
        return None


class _ClockCaseSession:
    def __init__(self, clock_case: str, writes: list[tuple[str, int]]) -> None:
        self.clock_case = clock_case
        self.writes = writes
        self.time_reads = 0

    async def read_status(self) -> None:
        return None

    async def read_optional_characteristic(self, uuid: str) -> bytes | None:
        if uuid != TIME_READ_UUID:
            return None
        self.time_reads += 1
        if self.clock_case == "unavailable":
            return None
        return pack("<I", 1000 if self.time_reads > 1 else 100)

    async def write_optional_characteristic(self, uuid: str, value: bytes) -> bool:
        self.writes.append((uuid, int.from_bytes(value, "little")))
        return True


class _TerminalMetricSession:
    async def write_control(self, _payload: bytes) -> None:
        return None


class _TerminalMetricContext:
    def __init__(self, case: str, session: _TerminalMetricSession, secondary: BaseException) -> None:
        self.case = case
        self.session = session
        self.secondary = secondary

    async def __aenter__(self) -> RingSession:
        return cast(RingSession, self.session)

    async def __aexit__(self, _type: object, _value: object, _traceback: object) -> None:
        if self.case in {"teardown", "fatal_with_teardown"}:
            raise self.secondary


class _TerminalMetricConnectedStep:
    def __init__(self, case: str, primary: BaseException) -> None:
        self.case = case
        self.primary = primary

    async def __call__(
        self,
        _session: RingSession,
        current: RingInfo | None,
        _read_info: InfoReader,
        phase: SessionPhaseState,
    ) -> tuple[str, RingInfo | None]:
        assert phase.quality is not None
        phase.quality.note_read(0.25, 3)
        if self.case == "retryable":
            raise CollectorTimeoutError("READ timed out")
        if self.case in {"fatal", "fatal_with_teardown"}:
            raise self.primary
        return "drained", current


class _RecordingLifecycleMetrics:
    release_version = "test"
    source_revision = None

    def __init__(self) -> None:
        self.corrections: list[ClockCorrectionMetric] = []
        self.transfers: list[TransferSessionMetric] = []

    def record_advertisement(self, _metric: object) -> None:
        return None

    def record_transfer_session(self, metric: TransferSessionMetric) -> None:
        self.transfers.append(metric)

    def record_clock_correction(self, metric: ClockCorrectionMetric) -> None:
        self.corrections.append(metric)


def _unexpected_startup_recovery() -> None:
    raise AssertionError("startup recovery is outside this lifecycle check")


def test_capture_priority_covers_closure_and_releases_after_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    events: list[str] = []

    async def attempt(_self: SessionLifecycle, *, with_presence: bool) -> DrainConfirmed:
        assert not with_presence
        events.append("attempt")
        return DrainConfirmed()

    async def enter() -> None:
        events.append("enter")

    async def close(_reason: str) -> None:
        events.append("close")
        raise OSError("closure failed")

    async def connected_step(
        _session: RingSession, _current: RingInfo | None, _read_info: InfoReader, _phase: SessionPhaseState
    ) -> tuple[str, RingInfo | None]:
        raise AssertionError("attempt was replaced")

    callbacks = SessionLifecycleCallbacks(
        before_direct_attempt=_noop,
        wait_presence_attempt=_wait,
        connected_step=connected_step,
        post_session_checkpoint=_noop,
        completed_batch_query=lambda: 0,
        drained_result=lambda: NoDataResult(RingInfo(10, 10, 100, 0, 512)),
        close_visit=close,
        enter_capture_priority=enter,
        exit_capture_priority=lambda: events.append("exit"),
    )
    with pytest.raises(FrozenInstanceError):
        callbacks.exit_capture_priority = None  # type: ignore[reportAttributeAccessIssue]
    with pytest.raises(ValueError, match="supplied together"):
        replace(callbacks, exit_capture_priority=None)
    run = SessionLifecycleRun(
        provider=lambda _candidate: cast(AbstractAsyncContextManager[RingSession], object()),
        options=OpportunisticOptions(TransferTimeouts(1, 1), RetryPolicy(stop_after_drained=True)),
        runtime=OpportunisticRuntime(),
        callbacks=callbacks,
    )
    monkeypatch.setattr(SessionLifecycle, "_attempt_visit", attempt)
    with pytest.raises(OSError, match="closure failed"):
        _run(SessionLifecycle(run).run_direct())
    assert events == ["enter", "attempt", "close", "exit"]


@pytest.mark.parametrize(
    ("error", "expected"),
    [
        (CollectorTimeoutError("timeout"), "operation timed out"),
        (AdvanceUncertainError("unknown advance"), "advance acknowledgement uncertain"),
        (NotificationOverflowError("queue full"), "notification queue overflow"),
    ],
)
def test_report_session_error_keeps_stable_operator_message(error: Exception, expected: str) -> None:
    async def scenario() -> None:
        activity: list[ActivityEvent] = []
        await report_session_error(activity.append, "read/reconcile", error, OpportunisticRuntime())
        assert len(activity) == 1
        event = activity[0]
        assert event.state == "session_error"
        assert event.error_message == expected

    _run(scenario())


def test_report_session_error_bounds_distinct_transport_cause_chain() -> None:
    async def scenario() -> None:
        max_entries = DEFAULT_CONFIG.observability.max_error_chain_entries
        root = RingTransportDisconnectedError("link to AA:BB:CC:DD:EE:FF lost")
        previous: BaseException = root
        for index in range(max_entries):
            cause = RuntimeError(f"cause-{index}")
            previous.__cause__ = cause
            previous = cause

        activity: list[ActivityEvent] = []
        await report_session_error(activity.append, "read/reconcile", root, OpportunisticRuntime())
        event = activity[0]
        assert event.error_message is not None
        assert event.error_message.split(" <- ") == [
            "RingTransportDisconnectedError: link to [BLE address] lost",
            *(f"RuntimeError: cause-{index}" for index in range(max_entries - 1)),
        ]

    _run(scenario())


def test_report_session_error_bounds_long_type_name_at_operator_boundary() -> None:
    async def scenario() -> None:
        max_chars = DEFAULT_CONFIG.observability.max_error_entry_chars
        long_type = cast(type[Exception], type("E" * (max_chars + 10), (Exception,), {}))
        cause: Exception = long_type("details")
        transport = RingTransportDisconnectedError("device link lost")
        transport.__cause__ = cause
        activity: list[ActivityEvent] = []

        await report_session_error(activity.append, "connect", transport, OpportunisticRuntime())

        event = activity[0]
        assert event.error_message is not None
        _, bounded_cause = event.error_message.split(" <- ")
        assert bounded_cause == f"{'E' * (max_chars - 2)}: "
        assert len(bounded_cause) == max_chars

    _run(scenario())


def test_report_session_error_truncates_long_redacted_message_to_entry_limit() -> None:
    async def scenario() -> None:
        max_chars = DEFAULT_CONFIG.observability.max_error_entry_chars
        address = "AA:BB:CC:DD:EE:FF"
        cause = RuntimeError(f"device {address} failed: " + "x" * (max_chars * 2))
        transport = RingTransportDisconnectedError("device link lost")
        transport.__cause__ = cause
        activity: list[ActivityEvent] = []

        await report_session_error(activity.append, "connect", transport, OpportunisticRuntime())

        event = activity[0]
        assert event.error_message is not None
        _, bounded_cause = event.error_message.split(" <- ")
        expected_prefix = "RuntimeError: device [BLE address] failed: "
        assert bounded_cause == expected_prefix + "x" * (max_chars - len(expected_prefix))
        assert len(bounded_cause) == max_chars
        assert address not in event.error_message

    _run(scenario())


def test_validate_policy_accepts_exact_fit_large_integer_capacity() -> None:
    records = 2**53 + 1
    validate_policy(RetryPolicy(batch_records=records, arena_max_bytes=records * RECORD_SIZE))


def test_deferred_retry_waits_until_visit_closure(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    async def scenario() -> None:
        events: list[str] = []
        store = StagingStore(tmp_path / "spool", tmp_path / "captures")

        def publish() -> None:
            events.append("publish")

        monkeypatch.setattr(store, "recover_and_publish", publish)
        maintenance = QuarantineMaintenance(store, None, OpportunisticRuntime())
        attempt_calls = 0

        async def unexpected_cooldown_sleep(_seconds: float) -> None:
            raise AssertionError("stop_after_drained must not sleep for cooldown")

        async def attempt(_self: SessionLifecycle, *, with_presence: bool) -> DrainConfirmed:
            nonlocal attempt_calls
            attempt_calls += 1
            if attempt_calls > 1:
                raise AssertionError("stop_after_drained must prevent a second attempt")
            assert not with_presence
            maintenance.schedule_publication_retry()
            await asyncio.sleep(0)
            events.append("attempt")
            return DrainConfirmed()

        async def close(_reason: str) -> None:
            await asyncio.sleep(0)
            events.append("closure")
            assert "publish" not in events

        async def connected_step(
            _session: RingSession, _current: RingInfo | None, _read_info: InfoReader, _phase: SessionPhaseState
        ) -> tuple[str, RingInfo | None]:
            raise AssertionError("attempt was replaced")

        callbacks = SessionLifecycleCallbacks(
            before_direct_attempt=_noop,
            wait_presence_attempt=_wait,
            connected_step=connected_step,
            post_session_checkpoint=_noop,
            completed_batch_query=lambda: 0,
            drained_result=lambda: NoDataResult(RingInfo(10, 10, 100, 0, 512)),
            close_visit=close,
            enter_capture_priority=maintenance.enter_capture_priority,
            exit_capture_priority=maintenance.exit_capture_priority,
        )
        run = SessionLifecycleRun(
            provider=lambda _candidate: cast(AbstractAsyncContextManager[RingSession], object()),
            options=OpportunisticOptions(
                TransferTimeouts(1, 1), RetryPolicy(stop_after_drained=True), sleep=unexpected_cooldown_sleep
            ),
            runtime=OpportunisticRuntime(),
            callbacks=callbacks,
        )
        monkeypatch.setattr(SessionLifecycle, "_attempt_visit", attempt)
        try:
            await SessionLifecycle(run).run_direct()
            for _ in range(20):
                if "publish" in events:
                    break
                await asyncio.sleep(0.001)
            assert events == ["attempt", "closure", "publish"]
        finally:
            await maintenance.close()

    _run(scenario())


def test_restart_closure_keeps_priority_through_followup_inspection() -> None:
    events: list[str] = []

    async def load_recovery() -> RecoveryDisposition:
        events.append("inspect")
        return "needs_interrupted_close" if events.count("inspect") == 1 else "empty"

    async def close(_reason: str) -> None:
        events.append("closure")

    async def enter() -> None:
        events.append("enter")

    async def stop() -> None:
        raise RuntimeError("stop after restart inspection")

    async def connected_step(
        _session: RingSession, _current: RingInfo | None, _read_info: InfoReader, _phase: SessionPhaseState
    ) -> tuple[str, RingInfo | None]:
        raise AssertionError("attempt must not start")

    callbacks = SessionLifecycleCallbacks(
        before_direct_attempt=stop,
        wait_presence_attempt=_wait,
        connected_step=connected_step,
        post_session_checkpoint=_noop,
        completed_batch_query=lambda: 0,
        drained_result=lambda: NoDataResult(RingInfo(10, 10, 100, 0, 512)),
        close_visit=close,
        load_recovery=load_recovery,
        enter_capture_priority=enter,
        exit_capture_priority=lambda: events.append("exit"),
    )
    run = SessionLifecycleRun(
        provider=lambda _candidate: cast(AbstractAsyncContextManager[RingSession], object()),
        options=OpportunisticOptions(TransferTimeouts(1, 1)),
        runtime=OpportunisticRuntime(),
        callbacks=callbacks,
    )
    with pytest.raises(RuntimeError, match="stop after restart inspection"):
        _run(SessionLifecycle(run).run_direct())
    assert events == ["inspect", "enter", "closure", "inspect", "exit"]


class _Lease:
    def __enter__(self) -> _Lease:
        return self

    def __exit__(self, _type: object, _value: object, _traceback: object) -> None:
        return None

    def require_active(self) -> None:
        return None


def test_teardown_precedes_post_session_checkpoint(monkeypatch: pytest.MonkeyPatch) -> None:
    events: list[str] = []
    info = RingInfo(10, 10, 100, 0, 512)

    class Context:
        async def __aenter__(self) -> RingSession:
            return cast(RingSession, object())

        async def __aexit__(self, _type: object, _value: object, _traceback: object) -> None:
            events.append("teardown")

    def provider(_candidate: object | None) -> Context:
        return Context()

    async def connected_step(
        _session: RingSession, current: RingInfo | None, _read_info: InfoReader, _phase: SessionPhaseState
    ) -> tuple[str, RingInfo | None]:
        events.append("connected")
        return "drained", current

    async def info_reader(_session: RingSession, *, timeout: float) -> RingInfo:
        del timeout
        return info

    async def checkpoint() -> None:
        events.append("checkpoint")

    callbacks = SessionLifecycleCallbacks(
        before_direct_attempt=_noop,
        wait_presence_attempt=_wait,
        connected_step=connected_step,
        post_session_checkpoint=checkpoint,
        completed_batch_query=lambda: 0,
        drained_result=lambda: NoDataResult(info),
    )
    options = OpportunisticOptions(TransferTimeouts(1, 1), RetryPolicy(backoff=(1,), stop_after_drained=True))
    run = SessionLifecycleRun(
        provider=provider,
        options=options,
        runtime=cast(CaptureRuntimePort, object()),
        callbacks=callbacks,
    )

    async def scenario() -> None:
        monkeypatch.setattr("omi_collector.capture.application.collector.ring_info", info_reader)
        await SessionLifecycle(run).run_session(Context())

    _run(scenario())
    assert events == ["connected", "teardown", "checkpoint"]


def test_missing_preflight_acknowledgement_blocks_read_and_still_tears_down(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[str] = []
    info = RingInfo(10, 10, 100, 0, 512)

    class Context:
        async def __aenter__(self) -> RingSession:
            return cast(RingSession, object())

        async def __aexit__(self, _type: object, _value: object, _traceback: object) -> None:
            events.append("teardown")

    async def connected_step(
        _session: RingSession, current: RingInfo | None, _read_info: InfoReader, _phase: SessionPhaseState
    ) -> tuple[str, RingInfo | None]:
        events.append("read")
        return "drained", current

    async def info_reader(_session: RingSession, *, timeout: float) -> RingInfo:
        del timeout
        return info

    async def omitted_preflight(*_args: object) -> None:
        return None

    callbacks = SessionLifecycleCallbacks(
        before_direct_attempt=_noop,
        wait_presence_attempt=_wait,
        connected_step=connected_step,
        post_session_checkpoint=lambda: _noop(),
        completed_batch_query=lambda: 0,
        drained_result=lambda: NoDataResult(info),
    )
    run = SessionLifecycleRun(
        provider=lambda _candidate: Context(),
        options=OpportunisticOptions(TransferTimeouts(1, 1), RetryPolicy(backoff=(1,))),
        runtime=OpportunisticRuntime(),
        callbacks=callbacks,
    )

    async def scenario() -> None:
        monkeypatch.setattr("omi_collector.capture.application.collector.ring_info", info_reader)
        monkeypatch.setattr(SessionLifecycle, "_collect_telemetry", omitted_preflight)
        with pytest.raises(RuntimeError, match="unsupported preflight result"):
            await SessionLifecycle(run).run_session(Context())

    _run(scenario())
    assert events == ["teardown"]


def test_retryable_read_failure_is_torn_down_and_checkpoints_before_return(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[str] = []
    info = RingInfo(10, 10, 100, 0, 512)

    class Session:
        async def write_control(self, _command: bytes) -> None:
            return None

    class Context:
        async def __aenter__(self) -> RingSession:
            return cast(RingSession, Session())

        async def __aexit__(self, _type: object, _value: object, _traceback: object) -> None:
            events.append("teardown")

    async def connected_step(
        _session: RingSession, _current: RingInfo | None, _read_info: InfoReader, _phase: SessionPhaseState
    ) -> tuple[str, RingInfo | None]:
        events.append("read")
        raise CollectorTimeoutError("read timed out")

    async def info_reader(_session: RingSession, *, timeout: float) -> RingInfo:
        del timeout
        return info

    async def checkpoint() -> None:
        events.append("checkpoint")

    callbacks = SessionLifecycleCallbacks(
        before_direct_attempt=_noop,
        wait_presence_attempt=_wait,
        connected_step=connected_step,
        post_session_checkpoint=checkpoint,
        completed_batch_query=lambda: 0,
        drained_result=lambda: NoDataResult(info),
    )
    run = SessionLifecycleRun(
        provider=lambda _candidate: Context(),
        options=OpportunisticOptions(TransferTimeouts(1, 1), RetryPolicy(backoff=(1,))),
        runtime=OpportunisticRuntime(),
        callbacks=callbacks,
    )

    async def scenario() -> None:
        monkeypatch.setattr("omi_collector.capture.application.collector.ring_info", info_reader)
        assert await SessionLifecycle(run).run_session(Context()) == "connected_interrupted"

    _run(scenario())
    assert events == ["read", "teardown", "checkpoint"]


def test_successful_drain_refreshes_battery_before_disconnect(monkeypatch: pytest.MonkeyPatch) -> None:
    info = RingInfo(10, 12, 100, 1, 512)
    order: list[str] = []
    observations: list[dict[str, object]] = []

    class Session:
        async def read_status(self) -> None:
            return None

        async def read_optional_characteristic(self, _uuid: str) -> bytes:
            order.append("battery")
            return bytes((74,))

    session = Session()

    class Context:
        async def __aenter__(self) -> RingSession:
            return cast(RingSession, session)

        async def __aexit__(self, _type: object, _value: object, _traceback: object) -> None:
            order.append("disconnect")

    async def connected_step(
        _session: RingSession, current: RingInfo | None, _read_info: InfoReader, _phase: SessionPhaseState
    ) -> tuple[str, RingInfo | None]:
        order.append("drained")
        return "drained", current

    async def info_reader(_session: RingSession, *, timeout: float) -> RingInfo:
        del timeout
        return info

    async def telemetry(
        _session: object,
        _status: object,
        _info: RingInfo,
        emit: Callable[[Mapping[str, object]], object],
        *,
        clock: TelemetryClock,
    ) -> None:
        del clock
        emit({"event": "pendant_observation", "firmware": "3.0.21", "battery_percent": 68})
        emit({"event": "pendant_clock_sync", "action": "none", "outcome": "host_unsynchronized"})

    callbacks = SessionLifecycleCallbacks(
        before_direct_attempt=_noop,
        wait_presence_attempt=_wait,
        connected_step=connected_step,
        post_session_checkpoint=_noop,
        completed_batch_query=lambda: 0,
        drained_result=lambda: NoDataResult(info),
    )
    options = OpportunisticOptions(
        TransferTimeouts(1, 1),
        RetryPolicy(backoff=(1,), stop_after_drained=True),
        operational=lambda event: observations.append(dict(event)),
    )
    run = SessionLifecycleRun(
        provider=lambda _candidate: Context(),
        options=options,
        runtime=OpportunisticRuntime(),
        callbacks=callbacks,
    )

    async def scenario() -> None:
        monkeypatch.setattr("omi_collector.capture.application.collector.ring_info", info_reader)
        monkeypatch.setattr(
            "omi_collector.capture.application.session_lifecycle.collect_operational_telemetry", telemetry
        )
        await SessionLifecycle(run).run_session(Context())

    _run(scenario())

    assert order == ["drained", "battery", "disconnect"]
    assert observations[0]["firmware"] == "3.0.21"
    assert observations[1] == {
        "event": "pendant_clock_sync",
        "action": "none",
        "outcome": "host_unsynchronized",
    }
    assert observations[-1] == {
        "event": "pendant_observation",
        "firmware": "3.0.21",
        "battery_percent": 74,
        "read_sequence": 10,
        "write_sequence": 12,
        "capacity_packets": 100,
        "dropped_packets": 1,
        "packet_size": 512,
        "optional_outcomes": {"battery": "ok"},
    }


def test_battery_refresh_failure_does_not_interrupt_successful_drain(monkeypatch: pytest.MonkeyPatch) -> None:
    info = RingInfo(10, 12, 100, 0, 512)
    order: list[str] = []

    class Session:
        async def read_status(self) -> None:
            return None

        async def read_optional_characteristic(self, _uuid: str) -> bytes:
            order.append("battery")
            raise OSError("battery unavailable")

    class Context:
        async def __aenter__(self) -> RingSession:
            return cast(RingSession, Session())

        async def __aexit__(self, _type: object, _value: object, _traceback: object) -> None:
            order.append("disconnect")

    async def connected_step(
        _session: RingSession, current: RingInfo | None, _read_info: InfoReader, _phase: SessionPhaseState
    ) -> tuple[str, RingInfo | None]:
        order.append("drained")
        return "drained", current

    async def info_reader(_session: RingSession, *, timeout: float) -> RingInfo:
        del timeout
        return info

    async def telemetry(*_args: object, **_kwargs: object) -> None:
        return None

    callbacks = SessionLifecycleCallbacks(
        before_direct_attempt=_noop,
        wait_presence_attempt=_wait,
        connected_step=connected_step,
        post_session_checkpoint=_noop,
        completed_batch_query=lambda: 0,
        drained_result=lambda: NoDataResult(info),
    )
    options = OpportunisticOptions(
        TransferTimeouts(1, 1),
        RetryPolicy(backoff=(1,), stop_after_drained=True),
        operational=lambda _event: None,
    )
    run = SessionLifecycleRun(
        provider=lambda _candidate: Context(),
        options=options,
        runtime=OpportunisticRuntime(),
        callbacks=callbacks,
    )

    async def scenario() -> None:
        monkeypatch.setattr("omi_collector.capture.application.collector.ring_info", info_reader)
        monkeypatch.setattr(
            "omi_collector.capture.application.session_lifecycle.collect_operational_telemetry", telemetry
        )
        result = await SessionLifecycle(run).run_session(Context())
        assert result == "drained"

    _run(scenario())
    assert order == ["drained", "battery", "disconnect"]


def test_clock_transport_fence_requires_error_free_context_close(monkeypatch: pytest.MonkeyPatch) -> None:
    receipts: list[str] = []

    class Context:
        def __init__(self, *, fails_close: bool) -> None:
            self.fails_close = fails_close

        async def __aenter__(self) -> RingSession:
            return cast(RingSession, object())

        async def __aexit__(self, _type: object, _value: object, _traceback: object) -> None:
            if self.fails_close:
                raise RingTransportUnavailableError("disconnect failed")

    async def ignore_session_error(*_args: object, **_kwargs: object) -> None:
        return None

    async def scenario() -> None:
        monkeypatch.setattr(
            "omi_collector.capture.application.session_lifecycle.report_session_error", ignore_session_error
        )
        retryable_failed_close_interrupted = await teardown_was_interrupted(
            Context(fails_close=True),
            None,
            1.0,
            None,
            OpportunisticRuntime(),
            on_transport_closed=lambda: receipts.append("retryable-failed"),
        )
        failed_close_interrupted = await teardown_was_interrupted(
            Context(fails_close=True),
            RuntimeError("transfer already failed"),
            1.0,
            None,
            OpportunisticRuntime(),
            on_transport_closed=lambda: receipts.append("failed"),
        )
        successful_close_interrupted = await teardown_was_interrupted(
            Context(fails_close=False),
            None,
            1.0,
            None,
            OpportunisticRuntime(),
            on_transport_closed=lambda: receipts.append("closed"),
        )

        assert retryable_failed_close_interrupted is True
        assert failed_close_interrupted is False
        assert successful_close_interrupted is False

    _run(scenario())
    assert receipts == ["closed"]


def test_clock_mutation_lease_is_passed_to_telemetry(monkeypatch: pytest.MonkeyPatch) -> None:
    info = RingInfo(10, 10, 100, 0, 512)
    activity: list[object] = []
    observed: list[tuple[object, float, object]] = []
    errors: list[BaseException] = []

    class Context:
        async def __aenter__(self) -> RingSession:
            return cast(RingSession, object())

        async def __aexit__(self, _type: object, _value: object, _traceback: object) -> None:
            return None

    async def connected_step(
        _session: RingSession, current: RingInfo | None, _read_info: InfoReader, _phase: SessionPhaseState
    ) -> tuple[str, RingInfo | None]:
        return "drained", current

    async def info_reader(_session: RingSession, *, timeout: float) -> RingInfo:
        del timeout
        return info

    async def telemetry(*_args: object, clock: TelemetryClock, **_kwargs: object) -> None:
        observed.append((clock.mutation_lease, clock.host_clock_probe_timeout, clock.synchronized))

    async def observe_error(_activity: object, _phase: object, error: BaseException, _runtime: object) -> None:
        errors.append(error)

    class CorrectionSink:
        @staticmethod
        def note_transport_closed() -> None:
            return None

    callbacks = SessionLifecycleCallbacks(
        before_direct_attempt=_noop,
        wait_presence_attempt=_wait,
        connected_step=connected_step,
        post_session_checkpoint=_noop,
        completed_batch_query=lambda: 0,
        drained_result=lambda: NoDataResult(info),
    )
    options = OpportunisticOptions(
        TransferTimeouts(1, 1),
        RetryPolicy(backoff=(1,), stop_after_drained=True),
        activity=activity.append,
        clock_correction_sink=cast(ClockCorrectionSink, CorrectionSink()),
        clock_lease=lambda: _Lease(),
        config=CollectorConfig(telemetry=TelemetryConfig(host_clock_probe_timeout_seconds=0.25)),
    )
    run = SessionLifecycleRun(
        provider=lambda _candidate: Context(),
        options=options,
        runtime=OpportunisticRuntime(),
        callbacks=callbacks,
    )

    async def scenario() -> None:
        monkeypatch.setattr("omi_collector.capture.application.collector.ring_info", info_reader)
        monkeypatch.setattr(
            "omi_collector.capture.application.session_lifecycle.collect_operational_telemetry", telemetry
        )
        monkeypatch.setattr("omi_collector.capture.application.session_lifecycle.report_session_error", observe_error)
        await SessionLifecycle(run).run_session(Context())

    _run(scenario())
    assert activity == [], errors
    assert observed == [(options.clock_lease, 0.25, None)]


def test_connected_step_cancellation_identity_reaches_context_exit(monkeypatch: pytest.MonkeyPatch) -> None:
    info = RingInfo(10, 10, 100, 0, 512)
    cancelled = asyncio.CancelledError("connected step cancelled")
    events: list[str] = []
    seen: dict[str, object] = {}
    drained_calls = 0

    class Context:
        async def __aenter__(self) -> RingSession:
            return cast(RingSession, object())

        async def __aexit__(self, exc_type: object, value: object, traceback: object) -> None:
            events.append("teardown")
            seen.update(type=exc_type, value=value, traceback=traceback)

    def provider(_candidate: object | None) -> Context:
        return Context()

    async def connected_step(
        _session: RingSession, _current: RingInfo | None, _read_info: InfoReader, _phase: SessionPhaseState
    ) -> tuple[str, RingInfo | None]:
        events.append("connected")
        raise cancelled

    async def info_reader(_session: RingSession, *, timeout: float) -> RingInfo:
        del timeout
        return info

    def drained_result() -> NoDataResult:
        nonlocal drained_calls
        drained_calls += 1
        return NoDataResult(info)

    async def checkpoint() -> None:
        events.append("checkpoint")

    callbacks = SessionLifecycleCallbacks(
        before_direct_attempt=_noop,
        wait_presence_attempt=_wait,
        connected_step=connected_step,
        post_session_checkpoint=checkpoint,
        completed_batch_query=lambda: 0,
        drained_result=drained_result,
    )
    run = SessionLifecycleRun(
        provider=provider,
        options=OpportunisticOptions(TransferTimeouts(1, 1), RetryPolicy(backoff=(1,))),
        runtime=cast(CaptureRuntimePort, object()),
        callbacks=callbacks,
    )

    async def scenario() -> None:
        monkeypatch.setattr("omi_collector.capture.application.collector.ring_info", info_reader)
        with pytest.raises(asyncio.CancelledError) as raised:
            await SessionLifecycle(run).run_session(Context())
        assert raised.value is cancelled

    _run(scenario())
    assert events == ["connected", "teardown"]
    assert seen["type"] is asyncio.CancelledError
    assert seen["value"] is cancelled
    assert seen["traceback"] is not None
    assert drained_calls == 0


async def _noop() -> None:
    return None


async def _wait() -> PresenceWake:
    return PresenceWake("test", candidate=object(), observed_at=time.monotonic())


def test_context_exit_receives_exact_primary_cancellation() -> None:
    primary = asyncio.CancelledError("stop")
    seen: dict[str, object] = {}

    class Context:
        async def __aenter__(self) -> RingSession:
            return cast(RingSession, object())

        async def __aexit__(self, exc_type: object, value: object, traceback: object) -> None:
            seen.update(type=exc_type, value=value, traceback=traceback)

    async def scenario() -> None:
        await exit_context(Context(), primary, 1)

    _run(scenario())
    assert seen["type"] is asyncio.CancelledError
    assert seen["value"] is primary
    assert seen["traceback"] is None


def test_presence_setup_failure_closes_issued_permit_before_propagation(monkeypatch: pytest.MonkeyPatch) -> None:
    events: list[str] = []

    class Presence:
        policy = PresencePolicy(rapid_backoff=(1.0,))
        drained_cooldown_remaining_seconds = 0.0
        wake_calls = 0

        resume_interrupted_visit = staticmethod(_unexpected_startup_recovery)

        async def wait_for_attempt(self) -> PresenceWake:
            self.wake_calls += 1
            if self.wake_calls > 1:
                raise AssertionError("setup failure must propagate before a second permit")
            return PresenceWake("test", candidate=object(), observed_at=time.monotonic())

        async def attempt_finished(self, outcome: AttemptOutcome) -> None:
            del outcome
            events.append("outcome")

        async def close(self) -> None:
            events.append("close")

    async def broken_open(_candidate: object | None) -> object:
        raise RuntimeError("provider setup failed")

    def unused_provider(_candidate: object | None) -> Never:
        raise AssertionError("provider must not run after setup failure")

    async def connected_step(
        _session: RingSession, current: RingInfo | None, _read_info: InfoReader, _phase: SessionPhaseState
    ) -> tuple[str, RingInfo | None]:
        return "drained", current

    presence = Presence()
    callbacks = SessionLifecycleCallbacks(
        before_direct_attempt=_noop,
        wait_presence_attempt=presence.wait_for_attempt,
        connected_step=connected_step,
        post_session_checkpoint=_noop,
        completed_batch_query=lambda: 0,
        drained_result=lambda: NoDataResult(RingInfo(10, 10, 100, 0, 512)),
    )
    run = SessionLifecycleRun(
        provider=unused_provider,
        options=OpportunisticOptions(TransferTimeouts(1, 1), RetryPolicy(backoff=(1,)), presence=presence),
        runtime=cast(CaptureRuntimePort, object()),
        callbacks=callbacks,
    )
    lifecycle = SessionLifecycle(run)
    monkeypatch.setattr(lifecycle, "_open_context", broken_open)

    async def scenario() -> None:
        with pytest.raises(RuntimeError, match="provider setup failed"):
            await lifecycle.run_with_presence()
        events.append("propagated")

    _run(scenario())
    assert events == ["close", "propagated"]


def test_real_presence_setup_failure_closes_issued_permit(monkeypatch: pytest.MonkeyPatch) -> None:
    class Observer:
        def __init__(self) -> None:
            self.callback: Callable[[object], object] | None = None
            self.active = False

        async def start(self, callback: Callable[[object], object]) -> None:
            self.callback = callback
            self.active = True

        async def stop(self) -> None:
            self.active = False

        def advertise(self) -> None:
            assert self.callback is not None
            self.callback(PresenceAdvertisement(object(), -72))

    observer = Observer()
    presence = PresenceScheduler(
        observer,
        policy=PresencePolicy(
            rapid_backoff=(1.0,),
            arrival_stability_seconds=0.001,
            arrival_max_gap_seconds=1.0,
        ),
    )

    async def broken_open(_candidate: object | None) -> object:
        raise RuntimeError("provider setup failed")

    def unused_provider(_candidate: object | None) -> Never:
        raise AssertionError("provider must not run after setup failure")

    async def connected_step(
        _session: RingSession, current: RingInfo | None, _read_info: InfoReader, _phase: SessionPhaseState
    ) -> tuple[str, RingInfo | None]:
        return "drained", current

    callbacks = SessionLifecycleCallbacks(
        before_direct_attempt=_noop,
        wait_presence_attempt=presence.wait_for_attempt,
        connected_step=connected_step,
        post_session_checkpoint=_noop,
        completed_batch_query=lambda: 0,
        drained_result=lambda: NoDataResult(RingInfo(10, 10, 100, 0, 512)),
    )
    run = SessionLifecycleRun(
        provider=unused_provider,
        options=OpportunisticOptions(TransferTimeouts(1, 1), RetryPolicy(backoff=(1,)), presence=presence),
        runtime=cast(CaptureRuntimePort, object()),
        callbacks=callbacks,
    )
    lifecycle = SessionLifecycle(run)
    monkeypatch.setattr(lifecycle, "_open_context", broken_open)

    async def scenario() -> None:
        running = asyncio.create_task(lifecycle.run_with_presence())
        try:
            async with asyncio.timeout(5.0):
                while observer.callback is None:
                    await asyncio.sleep(0)
            observer.advertise()
            await asyncio.sleep(0.002)
            observer.advertise()
            with pytest.raises(RuntimeError, match="provider setup failed"):
                await running
            assert not observer.active
            with pytest.raises(RuntimeError, match="closed"):
                await presence.wait_for_attempt()
        finally:
            running.cancel()
            await asyncio.gather(running, return_exceptions=True)
            await presence.close()

    _run(scenario())


def test_presence_outcome_follows_gatt_teardown_and_checkpoint(monkeypatch: pytest.MonkeyPatch) -> None:
    events: list[str] = []
    info = RingInfo(10, 10, 100, 0, 512)

    class Presence:
        policy = PresencePolicy(rapid_backoff=(1.0,))
        drained_cooldown_remaining_seconds = 1.0

        resume_interrupted_visit = staticmethod(_unexpected_startup_recovery)

        async def wait_for_attempt(self) -> PresenceWake:
            return PresenceWake("test", candidate=object(), observed_at=time.monotonic())

        async def attempt_finished(self, outcome: object) -> None:
            assert isinstance(outcome, CleanDrain)
            events.append("outcome")

        async def close(self) -> None:
            return None

    class Context:
        async def __aenter__(self) -> RingSession:
            return cast(RingSession, object())

        async def __aexit__(self, _type: object, _value: object, _traceback: object) -> None:
            events.append("teardown")

    async def connected_step(
        _session: RingSession, current: RingInfo | None, _read_info: InfoReader, _phase: SessionPhaseState
    ) -> tuple[str, RingInfo | None]:
        events.append("connected")
        return "drained", current

    async def info_reader(_session: RingSession, *, timeout: float) -> RingInfo:
        del timeout
        return info

    async def checkpoint() -> None:
        events.append("checkpoint")

    presence = Presence()
    callbacks = SessionLifecycleCallbacks(
        before_direct_attempt=_noop,
        wait_presence_attempt=presence.wait_for_attempt,
        connected_step=connected_step,
        post_session_checkpoint=checkpoint,
        completed_batch_query=lambda: 0,
        drained_result=lambda: NoDataResult(info),
    )
    run = SessionLifecycleRun(
        provider=lambda _candidate: Context(),
        options=OpportunisticOptions(
            TransferTimeouts(1, 1), RetryPolicy(backoff=(1,), stop_after_drained=True), presence=presence
        ),
        runtime=cast(CaptureRuntimePort, object()),
        callbacks=callbacks,
    )

    async def scenario() -> None:
        monkeypatch.setattr("omi_collector.capture.application.collector.ring_info", info_reader)
        await SessionLifecycle(run).run_with_presence()

    _run(scenario())
    assert events == ["connected", "teardown", "checkpoint", "outcome"]


@pytest.mark.parametrize(
    ("outcome", "durable_progress", "expected_type"),
    [
        ("drained", False, CleanDrain),
        ("collected", True, CleanDrain),
        ("retry", False, NotConnected),
        ("connected_interrupted", True, ConnectedInterruption),
        ("candidate_unavailable", False, CandidateUnavailable),
    ],
)
def test_presence_outcomes_are_canonical(outcome: str, durable_progress: bool, expected_type: type[object]) -> None:
    result = presence_attempt_outcome(outcome, durable_progress)

    assert isinstance(result, expected_type)
    if isinstance(result, (NotConnected, ConnectedInterruption)):
        assert result.durable_progress is durable_progress


@pytest.mark.parametrize("timeout", (0.0, -0.5))
def test_bounded_rejects_nonpositive_timeout_without_consuming_futures(timeout: float) -> None:
    async def scenario() -> None:
        loop = asyncio.get_running_loop()
        completed: asyncio.Future[str] = loop.create_future()
        completed.set_result("ready")
        unrelated: asyncio.Future[None] = loop.create_future()

        with pytest.raises(ValueError, match="timeouts must be positive"):
            await bounded(completed, timeout)

        assert completed.done() and not completed.cancelled()
        assert completed.result() == "ready"
        assert not unrelated.done() and not unrelated.cancelled()
        unrelated.cancel()

    _run(scenario())


def test_storage_not_ready_delay_saturates_after_configured_backoff() -> None:
    backoff = DEFAULT_CONFIG.retry.storage_not_ready_backoff

    assert [storage_not_ready_delay(index) for index in range(len(backoff) + 3)] == [
        *backoff,
        backoff[-1],
        backoff[-1],
        backoff[-1],
    ]


@pytest.mark.parametrize(
    "policy",
    (
        RetryPolicy(batch_records=0),
        RetryPolicy(batch_records=-1),
        RetryPolicy(batch_records=2, arena_max_bytes=RECORD_SIZE),
    ),
)
def test_validate_policy_rejects_empty_or_oversized_batch_capacity(policy: RetryPolicy) -> None:
    with pytest.raises(ValueError, match="policy values must be positive"):
        validate_policy(policy)


def test_validate_policy_accepts_exact_fit_batch_capacity() -> None:
    validate_policy(RetryPolicy(batch_records=2, arena_max_bytes=2 * RECORD_SIZE))


def test_recoverable_session_outcome_keeps_contended_device_lease_retryable(tmp_path: Path) -> None:
    store = StagingStore(tmp_path / "spool", tmp_path / "captures")
    activity: list[ActivityEvent] = []
    options = OpportunisticOptions(TransferTimeouts(1, 1), activity=activity.append)
    runtime = OpportunisticRuntime()

    with (
        store.device_lock(operation="held"),
        pytest.raises(DeviceAlreadyRunningError) as raised,
        store.device_lock(operation="contender"),
    ):
        raise AssertionError("contended lock must fail before acquisition")

    outcome = _run(recoverable_session_outcome(None, "connect", raised.value, options, runtime))

    assert outcome == "retry"
    assert [event.state for event in activity] == ["session_error"]
    assert all(event.state != "fatal" for event in activity)


def test_recoverable_session_outcome_does_not_retry_a_fatal_error_with_timeout_cause() -> None:
    async def scenario() -> None:
        fatal = ValueError("invalid state")
        fatal.__cause__ = TimeoutError("nested timeout")
        activity: list[ActivityEvent] = []
        options = OpportunisticOptions(TransferTimeouts(1, 1), activity=activity.append)
        runtime = OpportunisticRuntime()

        with pytest.raises(ValueError) as raised:
            await recoverable_session_outcome(None, "connect", fatal, options, runtime)
        assert raised.value is fatal
        assert [event.state for event in activity] == ["session_error", "fatal"]

        retry_activity: list[ActivityEvent] = []
        wrapped = TransferInterruptedError("transfer stopped", TransferCounters(0, 0, 0))
        wrapped.__cause__ = TimeoutError("operation timed out")
        outcome = await recoverable_session_outcome(
            None,
            "read/reconcile",
            wrapped,
            OpportunisticOptions(TransferTimeouts(1, 1), activity=retry_activity.append),
            runtime,
        )
        assert outcome == "retry"
        assert [event.state for event in retry_activity] == ["session_error"]

    _run(scenario())


@pytest.mark.parametrize(
    ("end_boundary", "reason"),
    (("wait", "absence"), ("attempt_finished", "recovery_exhausted")),
)
def test_presence_end_closes_restored_visit_without_opening_another_provider(end_boundary: str, reason: str) -> None:
    class StopAfterClosureError(RuntimeError):
        pass

    end = PresenceEnd(cast(Literal["absence", "recovery_exhausted"], reason))
    wake = PresenceWake("restored", candidate="candidate", observed_at=100.0)
    presence = _ScriptedPresence(
        [end, StopAfterClosureError("closed visit observed")]
        if end_boundary == "wait"
        else [wake, StopAfterClosureError("closed visit observed")],
        [] if end_boundary == "wait" else [end],
    )
    provider_candidates: list[object | None] = []
    closures: list[str] = []

    def provider(candidate: object | None) -> Never:
        provider_candidates.append(candidate)
        raise RingTransportUnavailableError("provider must not open after the end")

    async def close_visit(value: str) -> None:
        closures.append(value)

    async def connected_step(
        _session: RingSession,
        current: RingInfo | None,
        _read_info: InfoReader,
        _phase: SessionPhaseState,
    ) -> tuple[str, RingInfo | None]:
        return "drained", current

    callbacks = SessionLifecycleCallbacks(
        before_direct_attempt=_noop,
        wait_presence_attempt=presence.wait_for_attempt,
        connected_step=connected_step,
        post_session_checkpoint=_noop,
        completed_batch_query=lambda: 0,
        drained_result=lambda: NoDataResult(RingInfo(10, 10, 100, 0, 512)),
        close_visit=close_visit,
        load_recovery=lambda: _resumable_recovery(),
    )
    run = SessionLifecycleRun(
        provider,
        OpportunisticOptions(
            TransferTimeouts(1, 1),
            RetryPolicy(backoff=(0.01,)),
            presence=presence,
            clock=lambda: 100.0,
        ),
        OpportunisticRuntime(),
        callbacks,
    )

    async def scenario() -> None:
        with pytest.raises(StopAfterClosureError):
            await SessionLifecycle(run).run_with_presence()

    _run(scenario())
    assert closures == [reason]
    assert provider_candidates == ([] if end_boundary == "wait" else ["candidate"])
    assert presence.resumed == 1
    assert presence.closed


async def _resumable_recovery() -> RecoveryDisposition:
    return "resumable"


@pytest.mark.parametrize("clock_case", ("verified", "unsynchronized", "unavailable"))
def test_run_session_records_only_verified_clock_corrections(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, clock_case: str
) -> None:
    info = RingInfo(10, 12, 100, 1, 512)
    emitted: list[dict[str, object]] = []
    writes: list[tuple[str, int]] = []
    activity: list[ActivityEvent] = []

    session = _ClockCaseSession(clock_case, writes)
    context = _NoopRingContext(cast(RingSession, session))

    async def connected_step(
        _session: RingSession,
        current: RingInfo | None,
        _read_info: InfoReader,
        _phase: SessionPhaseState,
    ) -> tuple[str, RingInfo | None]:
        return "drained", current

    async def info_reader(_session: RingSession, *, timeout: float) -> RingInfo:
        del timeout
        return info

    callbacks = SessionLifecycleCallbacks(
        before_direct_attempt=_noop,
        wait_presence_attempt=_wait,
        connected_step=connected_step,
        post_session_checkpoint=_noop,
        completed_batch_query=lambda: 0,
        drained_result=lambda: NoDataResult(info),
    )
    metrics = _RecordingLifecycleMetrics()
    correction_store = ClockCorrectionStore(tmp_path / "device.json")
    options = OpportunisticOptions(
        TransferTimeouts(1, 1),
        RetryPolicy(backoff=(0.01,), stop_after_drained=True),
        operational=lambda event: emitted.append(dict(event)),
        activity=activity.append,
        host_time=lambda: 1000.0,
        host_clock_synchronized=lambda: clock_case == "verified",
        quality_metrics=metrics,  # type: ignore[arg-type]
        clock_correction_sink=correction_store,
    )
    run = SessionLifecycleRun(lambda _candidate: context, options, OpportunisticRuntime(), callbacks)

    monkeypatch.setattr("omi_collector.capture.application.collector.ring_info", info_reader)
    assert _run(SessionLifecycle(run).run_session(context)) == "drained"

    correction_events = [event for event in emitted if event.get("event") == "pendant_clock_sync"]
    assert len(correction_events) == 1, (activity, writes, session.time_reads)
    if clock_case == "verified":
        [metric] = metrics.corrections
        assert writes == [("19b10031-e8f2-537e-4f6c-d104768a1214", 1000)]
        assert correction_events[0]["outcome"] == "verified"
        assert metric.drift_seconds == -900.0
        assert metric.target_epoch == 1000
        assert metric.boundary_sequence_min == metric.boundary_sequence_max == 12
    else:
        assert metrics.corrections == []
        assert writes == []
        assert correction_events[0]["outcome"] in {"host_unsynchronized", "device_time_malformed"}


@pytest.mark.parametrize("terminal_case", ("retryable", "fatal", "teardown", "fatal_with_teardown"))
def test_run_session_records_one_terminal_transfer_metric_without_masking_session_result(
    monkeypatch: pytest.MonkeyPatch, terminal_case: str
) -> None:
    info = RingInfo(10, 12, 100, 1, 512)
    primary = ValueError("fatal READ failure")
    secondary = RingTransportUnavailableError("close interrupted")
    session = _TerminalMetricSession()

    async def info_reader(_session: RingSession, *, timeout: float) -> RingInfo:
        del timeout
        return info

    callbacks = SessionLifecycleCallbacks(
        before_direct_attempt=_noop,
        wait_presence_attempt=_wait,
        connected_step=_TerminalMetricConnectedStep(terminal_case, primary),
        post_session_checkpoint=_noop,
        completed_batch_query=lambda: 0,
        drained_result=lambda: NoDataResult(info),
    )
    metrics = _RecordingLifecycleMetrics()
    run = SessionLifecycleRun(
        lambda _candidate: _TerminalMetricContext(terminal_case, session, secondary),
        OpportunisticOptions(
            TransferTimeouts(1, 1),
            RetryPolicy(backoff=(0.01,)),
            quality_metrics=metrics,  # type: ignore[arg-type]
        ),
        OpportunisticRuntime(),
        callbacks,
    )
    monkeypatch.setattr("omi_collector.capture.application.collector.ring_info", info_reader)

    if terminal_case in {"fatal", "fatal_with_teardown"}:
        with pytest.raises(ValueError) as raised:
            _run(SessionLifecycle(run).run_session(_TerminalMetricContext(terminal_case, session, secondary)))
        assert raised.value is primary
    else:
        result = _run(SessionLifecycle(run).run_session(_TerminalMetricContext(terminal_case, session, secondary)))
        assert result == "connected_interrupted"

    assert len(metrics.transfers) == 1
    [metric] = metrics.transfers
    assert metric.requested_record_count == 3
    assert metric.active_read_elapsed_ms == 250
    expected = {
        "retryable": ("connected_interrupted", "retryable_error"),
        "fatal": ("failed", "fatal_error"),
        "teardown": ("connected_interrupted", "teardown_interrupted"),
        "fatal_with_teardown": ("failed", "fatal_error"),
    }[terminal_case]
    assert (metric.outcome, metric.termination_class) == expected


@pytest.mark.parametrize("progress_kind", ("unchanged", "completed_batch", "durable_frontier"))
def test_presence_interruption_reports_only_durable_progress_and_retries(
    progress_kind: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    presence = _ScriptedPresence(
        [PresenceWake(f"wake-{number}", candidate=number, observed_at=100.0) for number in (1, 2)]
    )
    completed_batches = 0
    durable_frontier = 0
    connected_calls = 0

    def provider(_candidate: object | None) -> _NoopRingContext:
        if presence.wake_count == 1 and progress_kind == "unchanged":
            raise RingTransportUnavailableError("disconnected before connect")
        return _NoopRingContext()

    async def connected_step(
        _session: RingSession,
        current: RingInfo | None,
        _read_info: InfoReader,
        _phase: SessionPhaseState,
    ) -> tuple[str, RingInfo | None]:
        nonlocal completed_batches, durable_frontier, connected_calls
        connected_calls += 1
        if connected_calls == 1 and progress_kind != "unchanged":
            if progress_kind == "completed_batch":
                completed_batches += 1
            elif progress_kind == "durable_frontier":
                durable_frontier += 1
            raise CollectorTimeoutError("interrupted after public progress boundary")
        return "drained", current

    callbacks = SessionLifecycleCallbacks(
        before_direct_attempt=_noop,
        wait_presence_attempt=presence.wait_for_attempt,
        connected_step=connected_step,
        post_session_checkpoint=_noop,
        completed_batch_query=lambda: completed_batches,
        durable_progress_query=lambda: durable_frontier,
        drained_result=lambda: NoDataResult(RingInfo(10, 10, 100, 0, 512)),
    )
    run = SessionLifecycleRun(
        provider,
        OpportunisticOptions(
            TransferTimeouts(1, 1),
            RetryPolicy(backoff=(0.01,), stop_after_drained=True),
            presence=presence,
            clock=lambda: 100.0,
        ),
        OpportunisticRuntime(),
        callbacks,
    )

    async def info_reader(_session: RingSession, *, timeout: float) -> RingInfo:
        del timeout
        return RingInfo(10, 10, 100, 0, 512)

    monkeypatch.setattr("omi_collector.capture.application.collector.ring_info", info_reader)

    assert _run(SessionLifecycle(run).run_with_presence()) == NoDataResult(RingInfo(10, 10, 100, 0, 512))
    assert len(presence.outcomes) == 2
    first = presence.outcomes[0]
    if progress_kind == "unchanged":
        assert isinstance(first, NotConnected) and first.durable_progress is False
    else:
        assert isinstance(first, ConnectedInterruption) and first.durable_progress is True
    assert isinstance(presence.outcomes[1], CleanDrain)
    assert presence.wake_count == 2
    assert presence.closed


def test_presence_batch_complete_activity_only_follows_completed_batch_interruption(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    presence = _ScriptedPresence([PresenceWake("wake", candidate=number, observed_at=100.0) for number in (1, 2, 3)])
    calls = 0
    completed_batches = 0
    activity: list[ActivityEvent] = []

    async def connected_step(
        _session: RingSession,
        current: RingInfo | None,
        _read_info: InfoReader,
        _phase: SessionPhaseState,
    ) -> tuple[str, RingInfo | None]:
        nonlocal calls, completed_batches
        calls += 1
        if calls == 1:
            raise CollectorTimeoutError("interrupted without completed batch")
        if calls == 2:
            completed_batches += 1
            raise CollectorTimeoutError("interrupted after completed batch")
        return "drained", current

    callbacks = SessionLifecycleCallbacks(
        before_direct_attempt=_noop,
        wait_presence_attempt=presence.wait_for_attempt,
        connected_step=connected_step,
        post_session_checkpoint=_noop,
        completed_batch_query=lambda: completed_batches,
        drained_result=lambda: NoDataResult(RingInfo(10, 10, 100, 0, 512)),
    )
    run = SessionLifecycleRun(
        lambda _candidate: _NoopRingContext(),
        OpportunisticOptions(
            TransferTimeouts(1, 1),
            RetryPolicy(backoff=(0.01,), stop_after_drained=True),
            activity=activity.append,
            presence=presence,
            clock=lambda: 100.0,
        ),
        OpportunisticRuntime(),
        callbacks,
    )

    async def info_reader(_session: RingSession, *, timeout: float) -> RingInfo:
        del timeout
        return RingInfo(10, 10, 100, 0, 512)

    monkeypatch.setattr("omi_collector.capture.application.collector.ring_info", info_reader)

    _run(SessionLifecycle(run).run_with_presence())

    assert [event.state for event in activity].count("batch_complete") == 1
    assert [event.state for event in activity].count("drained") == 1
    assert len(presence.outcomes) == 3
    assert isinstance(presence.outcomes[0], ConnectedInterruption)
    assert presence.outcomes[0].durable_progress is False
    assert isinstance(presence.outcomes[1], ConnectedInterruption)
    assert presence.outcomes[1].durable_progress is True
    assert isinstance(presence.outcomes[2], CleanDrain)


def test_direct_retry_backoff_resets_after_completed_batch_then_interruption(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    info = RingInfo(10, 10, 100, 0, 512)
    provider_calls = 0
    connected_calls = 0
    completed_batches = 0
    delays: list[float] = []
    activity: list[ActivityEvent] = []

    class Context:
        async def __aenter__(self) -> RingSession:
            return cast(RingSession, object())

        async def __aexit__(self, _type: object, _value: object, _traceback: object) -> None:
            return None

    def provider(_candidate: object | None) -> Context:
        nonlocal provider_calls
        provider_calls += 1
        if provider_calls <= 2:
            raise RingTransportUnavailableError("connect interrupted")
        return Context()

    async def connected_step(
        _session: RingSession,
        current: RingInfo | None,
        _read_info: InfoReader,
        _phase: SessionPhaseState,
    ) -> tuple[str, RingInfo | None]:
        nonlocal connected_calls, completed_batches
        connected_calls += 1
        if connected_calls == 1:
            completed_batches += 1
            raise CollectorTimeoutError("interrupted after completed batch")
        return "drained", current

    async def sleep(delay: float) -> None:
        delays.append(delay)

    callbacks = SessionLifecycleCallbacks(
        before_direct_attempt=_noop,
        wait_presence_attempt=_wait,
        connected_step=connected_step,
        post_session_checkpoint=_noop,
        completed_batch_query=lambda: completed_batches,
        drained_result=lambda: NoDataResult(info),
    )
    run = SessionLifecycleRun(
        provider,
        OpportunisticOptions(
            TransferTimeouts(1, 1),
            RetryPolicy(backoff=(0.1, 0.2, 0.4), stop_after_drained=True),
            activity=activity.append,
            sleep=sleep,
        ),
        OpportunisticRuntime(),
        callbacks,
    )

    async def info_reader(_session: RingSession, *, timeout: float) -> RingInfo:
        del timeout
        return info

    monkeypatch.setattr("omi_collector.capture.application.collector.ring_info", info_reader)

    assert _run(SessionLifecycle(run).run_direct()) == NoDataResult(info)
    assert delays == [0.1, 0.2, 0.1]
    assert "batch_complete" not in [event.state for event in activity]


def test_presence_rejects_expired_and_untimed_wakes_before_provider_then_accepts_fresh_candidate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    presence = _ScriptedPresence(
        [
            PresenceWake("exact-expiry", candidate="exact", observed_at=90.0),
            PresenceWake("expired", candidate="late", observed_at=89.9),
            PresenceWake("missing-time", candidate="untimed", observed_at=None),
            PresenceWake("fresh", candidate="fresh", observed_at=99.9),
        ]
    )
    provider_candidates: list[object | None] = []

    def provider(candidate: object | None) -> _NoopRingContext:
        provider_candidates.append(candidate)
        return _NoopRingContext()

    async def connected_step(
        _session: RingSession,
        current: RingInfo | None,
        _read_info: InfoReader,
        _phase: SessionPhaseState,
    ) -> tuple[str, RingInfo | None]:
        return "drained", current

    callbacks = SessionLifecycleCallbacks(
        before_direct_attempt=_noop,
        wait_presence_attempt=presence.wait_for_attempt,
        connected_step=connected_step,
        post_session_checkpoint=_noop,
        completed_batch_query=lambda: 0,
        drained_result=lambda: NoDataResult(RingInfo(10, 10, 100, 0, 512)),
    )
    run = SessionLifecycleRun(
        provider,
        OpportunisticOptions(
            TransferTimeouts(1, 1),
            RetryPolicy(backoff=(0.01,), stop_after_drained=True),
            presence=presence,
            clock=lambda: 100.0,
        ),
        OpportunisticRuntime(),
        callbacks,
    )

    async def info_reader(_session: RingSession, *, timeout: float) -> RingInfo:
        del timeout
        return RingInfo(10, 10, 100, 0, 512)

    monkeypatch.setattr("omi_collector.capture.application.collector.ring_info", info_reader)
    _run(SessionLifecycle(run).run_with_presence())

    assert provider_candidates == ["fresh"]
    assert presence.outcomes == [CandidateUnavailable(), CandidateUnavailable(), CandidateUnavailable(), CleanDrain()]


def test_provider_candidate_unavailable_is_reported_and_next_wake_can_complete(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    presence = _ScriptedPresence(
        [
            PresenceWake("fresh", candidate="candidate-1", observed_at=100.0),
            PresenceWake("fresh", candidate="candidate-2", observed_at=100.0),
        ]
    )
    candidates: list[object | None] = []

    def provider(candidate: object | None) -> _NoopRingContext:
        candidates.append(candidate)
        if len(candidates) == 1:
            raise CandidateUnavailableError("candidate disappeared before connection")
        return _NoopRingContext()

    async def connected_step(
        _session: RingSession,
        current: RingInfo | None,
        _read_info: InfoReader,
        _phase: SessionPhaseState,
    ) -> tuple[str, RingInfo | None]:
        return "drained", current

    callbacks = SessionLifecycleCallbacks(
        before_direct_attempt=_noop,
        wait_presence_attempt=presence.wait_for_attempt,
        connected_step=connected_step,
        post_session_checkpoint=_noop,
        completed_batch_query=lambda: 0,
        drained_result=lambda: NoDataResult(RingInfo(10, 10, 100, 0, 512)),
    )
    run = SessionLifecycleRun(
        provider,
        OpportunisticOptions(
            TransferTimeouts(1, 1),
            RetryPolicy(backoff=(0.01,), stop_after_drained=True),
            presence=presence,
            clock=lambda: 100.0,
        ),
        OpportunisticRuntime(),
        callbacks,
    )

    async def info_reader(_session: RingSession, *, timeout: float) -> RingInfo:
        del timeout
        return RingInfo(10, 10, 100, 0, 512)

    monkeypatch.setattr("omi_collector.capture.application.collector.ring_info", info_reader)
    _run(SessionLifecycle(run).run_with_presence())

    assert candidates == ["candidate-1", "candidate-2"]
    assert presence.outcomes == [CandidateUnavailable(), CleanDrain()]


def test_presence_reports_disconnected_and_connected_interruption_kinds_in_order(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    presence = _ScriptedPresence(
        [
            PresenceWake("fresh", candidate="candidate-1", observed_at=100.0),
            PresenceWake("fresh", candidate="candidate-2", observed_at=100.0),
            PresenceWake("fresh", candidate="candidate-3", observed_at=100.0),
        ]
    )
    provider_calls = 0
    connected_calls = 0

    def provider(_candidate: object | None) -> _NoopRingContext:
        nonlocal provider_calls
        provider_calls += 1
        if provider_calls == 1:
            raise RingTransportUnavailableError("disconnected before a GATT session")
        return _NoopRingContext()

    async def connected_step(
        _session: RingSession,
        current: RingInfo | None,
        _read_info: InfoReader,
        _phase: SessionPhaseState,
    ) -> tuple[str, RingInfo | None]:
        nonlocal connected_calls
        connected_calls += 1
        if connected_calls == 1:
            raise CollectorTimeoutError("connected READ interrupted")
        return "drained", current

    callbacks = SessionLifecycleCallbacks(
        before_direct_attempt=_noop,
        wait_presence_attempt=presence.wait_for_attempt,
        connected_step=connected_step,
        post_session_checkpoint=_noop,
        completed_batch_query=lambda: 0,
        drained_result=lambda: NoDataResult(RingInfo(10, 10, 100, 0, 512)),
    )
    run = SessionLifecycleRun(
        provider,
        OpportunisticOptions(
            TransferTimeouts(1, 1),
            RetryPolicy(backoff=(0.01,), stop_after_drained=True),
            presence=presence,
            clock=lambda: 100.0,
        ),
        OpportunisticRuntime(),
        callbacks,
    )

    async def info_reader(_session: RingSession, *, timeout: float) -> RingInfo:
        del timeout
        return RingInfo(10, 10, 100, 0, 512)

    monkeypatch.setattr("omi_collector.capture.application.collector.ring_info", info_reader)
    _run(SessionLifecycle(run).run_with_presence())

    assert presence.outcomes == [NotConnected(False), ConnectedInterruption(False), CleanDrain()]
