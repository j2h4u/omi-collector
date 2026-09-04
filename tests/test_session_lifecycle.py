"""Focused contracts for the physical-session lifecycle boundary."""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Coroutine
from typing import Never, cast

import pytest

from omi_collector.capture.application.collector import NoDataResult, TransferTimeouts
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
from omi_collector.capture.application.ring_transport import RingSession
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
)
from omi_collector.capture.domain.ring_protocol import RingInfo


def _run(coroutine: Coroutine[object, object, object]) -> object:
    return asyncio.run(coroutine)


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
        before_legacy_attempt=_noop,
        wait_presence_attempt=_wait,
        connected_step=connected_step,
        post_session_checkpoint=checkpoint,
        completed_batch_query=lambda: 0,
        drained_result=lambda: NoDataResult(info),
    )
    options = OpportunisticOptions(TransferTimeouts(1, 1), RetryPolicy(backoff=(1,), stop_after_drained=True))
    run = SessionLifecycleRun(
        provider=provider,
        device_slug="omi",
        options=options,
        runtime=cast(CaptureRuntimePort, object()),
        callbacks=callbacks,
    )

    async def scenario() -> None:
        monkeypatch.setattr("omi_collector.capture.application.collector.ring_info", info_reader)
        await SessionLifecycle(run).run_session(Context())

    _run(scenario())
    assert events == ["connected", "teardown", "checkpoint"]


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
        before_legacy_attempt=_noop,
        wait_presence_attempt=_wait,
        connected_step=connected_step,
        post_session_checkpoint=checkpoint,
        completed_batch_query=lambda: 0,
        drained_result=drained_result,
    )
    run = SessionLifecycleRun(
        provider=provider,
        device_slug="omi",
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
    return PresenceWake("test")


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

        async def wait_for_attempt(self) -> PresenceWake:
            return PresenceWake("test")

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
        before_legacy_attempt=_noop,
        wait_presence_attempt=presence.wait_for_attempt,
        connected_step=connected_step,
        post_session_checkpoint=_noop,
        completed_batch_query=lambda: 0,
        drained_result=lambda: NoDataResult(RingInfo(10, 10, 100, 0, 512)),
    )
    run = SessionLifecycleRun(
        provider=unused_provider,
        device_slug="omi",
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
    assert events == ["close", "close", "propagated"]


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
    presence = PresenceScheduler(observer, policy=PresencePolicy(rapid_backoff=(1.0,)))

    async def broken_open(_candidate: object | None) -> object:
        raise RuntimeError("provider setup failed")

    def unused_provider(_candidate: object | None) -> Never:
        raise AssertionError("provider must not run after setup failure")

    async def connected_step(
        _session: RingSession, current: RingInfo | None, _read_info: InfoReader, _phase: SessionPhaseState
    ) -> tuple[str, RingInfo | None]:
        return "drained", current

    callbacks = SessionLifecycleCallbacks(
        before_legacy_attempt=_noop,
        wait_presence_attempt=presence.wait_for_attempt,
        connected_step=connected_step,
        post_session_checkpoint=_noop,
        completed_batch_query=lambda: 0,
        drained_result=lambda: NoDataResult(RingInfo(10, 10, 100, 0, 512)),
    )
    run = SessionLifecycleRun(
        provider=unused_provider,
        device_slug="omi",
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

        async def wait_for_attempt(self) -> PresenceWake:
            return PresenceWake("test")

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
        before_legacy_attempt=_noop,
        wait_presence_attempt=presence.wait_for_attempt,
        connected_step=connected_step,
        post_session_checkpoint=checkpoint,
        completed_batch_query=lambda: 0,
        drained_result=lambda: NoDataResult(info),
    )
    run = SessionLifecycleRun(
        provider=lambda _candidate: Context(),
        device_slug="omi",
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
