"""Focused contracts for the physical-session lifecycle boundary."""

from __future__ import annotations

import asyncio
import time
from collections.abc import Callable, Coroutine, Mapping
from contextlib import AbstractAsyncContextManager
from dataclasses import replace
from pathlib import Path
from typing import Never, cast

import pytest

from omi_collector.capture.adapters.opportunistic_runtime import OpportunisticRuntime
from omi_collector.capture.adapters.staging_store import StagingStore
from omi_collector.capture.application.collector import CollectorTimeoutError, NoDataResult, TransferTimeouts
from omi_collector.capture.application.operational_telemetry import ClockCorrectionSink, TelemetryClock
from omi_collector.capture.application.ports import CaptureRuntimePort
from omi_collector.capture.application.presence import (
    PresenceAdvertisement,
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
from omi_collector.capture.application.quarantine_maintenance import QuarantineMaintenance
from omi_collector.capture.application.ring_transport import RingSession, RingTransportUnavailableError
from omi_collector.capture.application.session_lifecycle import (
    InfoReader,
    OpportunisticOptions,
    RetryPolicy,
    SessionLifecycle,
    SessionLifecycleCallbacks,
    SessionLifecycleRun,
    SessionPhaseState,
    exit_context,
    presence_attempt_outcome,
    teardown_was_interrupted,
)
from omi_collector.capture.application.visit_machine import DrainConfirmed, RecoveryDisposition
from omi_collector.capture.domain.ring_protocol import RingInfo
from omi_collector.config import CollectorConfig, TelemetryConfig


def _run(coroutine: Coroutine[object, object, object]) -> object:
    return asyncio.run(coroutine)


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


def test_deferred_retry_waits_until_visit_closure(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    async def scenario() -> None:
        events: list[str] = []
        store = StagingStore(tmp_path / "spool", tmp_path / "captures")

        def publish() -> None:
            events.append("publish")

        monkeypatch.setattr(store, "recover_and_publish", publish)
        maintenance = QuarantineMaintenance(store, None, OpportunisticRuntime())

        async def attempt(_self: SessionLifecycle, *, with_presence: bool) -> DrainConfirmed:
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
            options=OpportunisticOptions(TransferTimeouts(1, 1), RetryPolicy(stop_after_drained=True)),
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

        resume_interrupted_visit = staticmethod(_unexpected_startup_recovery)

        async def wait_for_attempt(self) -> PresenceWake:
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
