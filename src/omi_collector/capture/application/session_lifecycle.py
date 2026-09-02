"""Physical-session lifecycle for opportunistic capture.

This module owns the connection boundary: context entry/exit, bounded INFO
recovery, telemetry, retry classification, activity reporting, and the
presence/legacy retry loops.  Batch admission and durability decisions stay in
``opportunistic_sync`` and are supplied as a small callback bundle.
"""

from __future__ import annotations

import asyncio
import inspect
import time
from collections.abc import Awaitable, Callable, Mapping
from contextlib import AbstractAsyncContextManager, suppress
from dataclasses import KW_ONLY, dataclass
from typing import Literal

from ...config import DEFAULT_CONFIG, CollectorConfig, RetryConfig
from ..domain.ring_protocol import RECORD_SIZE, STATUS_STORAGE_NOT_READY, RingInfo, RingStatus, encode_stop_command
from . import collector
from .operational_telemetry import (
    OperationalEmitter,
    TelemetryClock,
    collect_operational_telemetry,
    system_host_clock_synchronized,
)
from .ports import CaptureRuntimePort
from .presence import PresenceScheduler, PresenceWake
from .quality_metrics import QualityMetricsPort, SessionQuality, TransferSessionMetric, utc_timestamp
from .ring_transport import (
    CandidateUnavailableError,
    NotificationOverflowError,
    RingSession,
    RingTransportDisconnectedError,
    RingTransportUnavailableError,
)


class OpportunisticSyncError(RuntimeError):
    """Base error for a fatal opportunistic collection mismatch."""


class StorageNotReadySessionError(OpportunisticSyncError):
    """The session exhausted bounded recovery for a remounting storage device."""


type SessionProvider = Callable[[object | None], AbstractAsyncContextManager[RingSession]]
type ActivityCallback = Callable[["ActivityEvent"], object]
type SessionPhase = Literal["connect", "preflight", "info", "telemetry", "read/reconcile", "advance", "teardown"]
type InfoReader = Callable[[RingSession], Awaitable[RingInfo]]
type ConnectedStep = Callable[
    [RingSession, RingInfo | None, InfoReader, "SessionPhaseState"], Awaitable[tuple[str | None, RingInfo | None]]
]


@dataclass(frozen=True, slots=True)
class RetryPolicy:
    """Bounded per-operation timings, never a global presence deadline."""

    backoff: tuple[float, ...] = DEFAULT_CONFIG.retry.rapid_backoff
    idle_poll_seconds: float | None = None
    batch_records: int = DEFAULT_CONFIG.memory.arena_max_bytes // RECORD_SIZE
    stop_after_drained: bool = False
    _: KW_ONLY
    drain_cooldown_seconds: float = DEFAULT_CONFIG.presence.fallback_seconds
    arena_max_bytes: int = DEFAULT_CONFIG.memory.arena_max_bytes
    advance_enabled: bool = True

    def __post_init__(self) -> None:
        if self.idle_poll_seconds is not None:
            object.__setattr__(self, "drain_cooldown_seconds", self.idle_poll_seconds)

    def delay_for(self, retry_number: int) -> float:
        if not self.backoff:
            return 0.0
        return self.backoff[min(retry_number, len(self.backoff) - 1)]


@dataclass(frozen=True, slots=True)
class OpportunisticOptions:
    timeouts: collector.TransferTimeouts
    policy: RetryPolicy = RetryPolicy()
    progress: collector.ProgressCallback | None = None
    activity: ActivityCallback | None = None
    operational: OperationalEmitter | None = None
    host_time: Callable[[], float] = time.time
    host_clock_synchronized: Callable[[], bool] | None = None
    clock: Callable[[], float] = time.monotonic
    sleep: Callable[[float], object] = asyncio.sleep
    presence: PresenceScheduler | None = None
    quality_metrics: QualityMetricsPort | None = None
    phy_policy: str = "auto"
    config: CollectorConfig = DEFAULT_CONFIG


@dataclass(frozen=True, slots=True)
class ActivityEvent:
    """Low-volume lifecycle signal for operators; READ speed is a ProgressEvent."""

    state: str
    retry_seconds: float | None = None
    phase: SessionPhase | None = None
    error_type: str | None = None
    error_message: str | None = None
    reason: str | None = None
    duration_seconds: float | None = None
    next_attempt_in_seconds: float | None = None


@dataclass(frozen=True, slots=True)
class SessionLifecycleCallbacks:
    """Coordinator closures used by the physical-session lifecycle."""

    before_legacy_attempt: Callable[[], Awaitable[None]]
    wait_presence_attempt: Callable[[], Awaitable[PresenceWake]]
    connected_step: ConnectedStep
    post_session_checkpoint: Callable[[], Awaitable[None]]
    completed_batch_query: Callable[[], int]
    drained_result: Callable[[], collector.CollectResult]
    observe_info: Callable[[RingInfo], object] | None = None


@dataclass(frozen=True, slots=True)
class SessionLifecycleRun:
    provider: SessionProvider
    device_slug: str
    options: OpportunisticOptions
    runtime: CaptureRuntimePort
    callbacks: SessionLifecycleCallbacks


@dataclass(slots=True)
class SessionPhaseState:
    value: SessionPhase
    quality: SessionQuality | None = None


class SessionLifecycle:
    """Run physical sessions while delegating transfer policy to callbacks."""

    def __init__(self, run: SessionLifecycleRun) -> None:
        self.run = run
        self._storage_not_ready_responses = 0

    async def run_legacy(self) -> collector.CollectResult:
        retry = 0
        while True:
            await self.run.callbacks.before_legacy_attempt()
            await report_activity(self.run.options.activity, "connecting")
            completed_before = self.run.callbacks.completed_batch_query()
            context, outcome = await self._open_context(None)
            if context is not None:
                outcome = await self.run_session(context)
            if outcome == "drained":
                retry = 0
                await report_activity(self.run.options.activity, "drained")
                if self.run.options.policy.stop_after_drained:
                    return self.run.callbacks.drained_result()
                await report_cooldown_started(
                    self.run.options.activity,
                    self.run.options.policy.drain_cooldown_seconds,
                    self.run.options.policy.drain_cooldown_seconds,
                )
                await sleep(self.run.options.sleep, self.run.options.policy.drain_cooldown_seconds)
                continue
            if outcome == "collected":
                return self.run.callbacks.drained_result()
            if self.run.callbacks.completed_batch_query() > completed_before:
                retry = 0
            delay = self.run.options.policy.delay_for(retry)
            retry += 1
            await report_activity(self.run.options.activity, "away", delay)
            await sleep(self.run.options.sleep, delay)

    async def run_with_presence(self) -> collector.CollectResult:
        presence = self.run.options.presence
        assert presence is not None
        try:
            while True:
                wake = await self.run.callbacks.wait_presence_attempt()
                completed_before = self.run.callbacks.completed_batch_query()
                context, outcome = await self._open_context(wake.candidate)
                if context is not None:
                    outcome = await self.run_session(context, wake.advertisement_rssi_dbm)
                candidate_failed = outcome == "candidate_unavailable"
                if outcome == "drained":
                    await presence.attempt_finished("clean")
                    await report_activity(self.run.options.activity, "drained")
                    if self.run.options.policy.stop_after_drained:
                        return self.run.callbacks.drained_result()
                    await report_cooldown_started(
                        self.run.options.activity,
                        presence.policy.drained_fallback_seconds,
                        presence.drained_cooldown_remaining_seconds,
                    )
                    continue
                if outcome == "collected":
                    await presence.attempt_finished("clean")
                    return self.run.callbacks.drained_result()
                if outcome == "connected_interrupted":
                    retry_outcome = (
                        "connected_interrupted_after_progress"
                        if self.run.callbacks.completed_batch_query() > completed_before
                        else "connected_interrupted"
                    )
                else:
                    retry_outcome = (
                        "retry_after_progress"
                        if self.run.callbacks.completed_batch_query() > completed_before
                        else "retry"
                    )
                if candidate_failed:
                    presence.invalidate_candidate()
                await presence.attempt_finished(retry_outcome)
                if self.run.callbacks.completed_batch_query() > completed_before:
                    await report_activity(self.run.options.activity, "batch_complete")
                await report_activity(self.run.options.activity, "away")
        finally:
            await presence.close()

    async def _open_context(
        self, candidate: object | None
    ) -> tuple[AbstractAsyncContextManager[RingSession] | None, str]:
        try:
            return self.run.provider(candidate), "retry"
        except BaseException as error:
            await report_session_error(self.run.options.activity, "connect", error, self.run.runtime)
            if isinstance(error, CandidateUnavailableError):
                return None, "candidate_unavailable"
            if not retryable(error):
                await report_activity(self.run.options.activity, "fatal")
                raise
            return None, "retry"

    async def run_session(
        self, context: AbstractAsyncContextManager[RingSession], advertisement_rssi_dbm: int | None = None
    ) -> str:
        """Run and terminally account for one connected physical session."""
        session: RingSession | None = None
        primary: BaseException | None = None
        terminal_error: BaseException | None = None
        current: RingInfo | None = None
        outcome: str | None = None
        teardown_error = False
        quality = SessionQuality(self.run.device_slug, advertisement_rssi_dbm, self.run.options.phy_policy)
        phase = SessionPhaseState("connect", quality)
        self._storage_not_ready_responses = 0
        try:
            try:
                session = await bounded(context.__aenter__(), self.run.options.timeouts.info)
                phase.value = "info"
                current = await self._info(session)
                await self._collect_telemetry(session, current, phase)
                while True:
                    phase.value = "read/reconcile"
                    outcome, current = await self.run.callbacks.connected_step(session, current, self._info, phase)
                    if outcome in ("drained", "collected"):
                        break
            except asyncio.CancelledError as error:
                primary = error
                raise
            except Exception as error:  # noqa: BLE001 - backend errors reach retry policy
                primary = error
                outcome = await recoverable_session_outcome(
                    session, phase.value, error, self.run.options, self.run.runtime
                )
            finally:
                if session is not None:
                    teardown_error = await teardown_was_interrupted(
                        context, primary, self.run.options.timeouts.info, self.run.options.activity, self.run.runtime
                    )
            await self.run.callbacks.post_session_checkpoint()
            if teardown_error:
                return "connected_interrupted"
            if outcome is not None:
                return outcome
            raise RuntimeError("opportunistic session ended without an outcome")
        except BaseException as error:
            terminal_error = error
            raise
        finally:
            self._record_session_quality(quality, outcome, teardown_error, terminal_error)

    def _record_session_quality(
        self,
        quality: SessionQuality,
        outcome: str | None,
        teardown_error: bool,
        error: BaseException | None,
    ) -> None:
        """Metrics are auxiliary evidence and must never alter capture control flow."""
        metrics = self.run.options.quality_metrics
        if metrics is None or not quality.attempted_read:
            return
        termination_class, terminal_outcome = _quality_terminal(outcome, teardown_error, error)
        try:
            metrics.record_transfer_session(
                TransferSessionMetric(
                    utc_timestamp(self.run.options.host_time()),
                    quality.session_id,
                    quality.device_slug,
                    terminal_outcome,
                    termination_class,
                    quality.active_read_elapsed_ms,
                    quality.requested_record_count,
                    quality.received_raw_bytes,
                    quality.submitted_raw_bytes,
                    quality.written_raw_bytes,
                    metrics.release_version,  # type: ignore[attr-defined]
                    metrics.source_revision,  # type: ignore[attr-defined]
                    quality.firmware_version,
                    quality.phy_policy,
                    quality.advertisement_rssi_dbm,
                )
            )
        except Exception as metrics_error:  # noqa: BLE001 - metrics cannot stop audio capture
            self.run.runtime.debug_exception(
                "quality_metrics_write_error", metrics_error, event_type="transfer_session"
            )

    async def _info(self, session: RingSession) -> RingInfo:
        while True:
            try:
                info = await bounded(
                    collector.ring_info(session, timeout=self.run.options.timeouts.info),
                    self.run.options.timeouts.info,
                )
                if self.run.callbacks.observe_info is not None:
                    try:
                        result = self.run.callbacks.observe_info(info)
                        if inspect.isawaitable(result):
                            await result
                    except Exception as error:  # noqa: BLE001 - observation is best effort
                        self.run.runtime.debug_exception(
                            "firmware_observation_writer_error", error, operation="observe"
                        )
                return info
            except collector.RingAcknowledgementError as error:
                if error.status != STATUS_STORAGE_NOT_READY:
                    raise
                self._storage_not_ready_responses += 1
                if self._storage_not_ready_responses >= self.run.options.config.retry.max_storage_not_ready_responses:
                    raise StorageNotReadySessionError(
                        "INFO returned STORAGE_NOT_READY too many times in this physical session"
                    ) from error
                delay = storage_not_ready_delay(self._storage_not_ready_responses - 1, self.run.options.config.retry)
                await report_activity(self.run.options.activity, "storage_wait", delay)
                await sleep(self.run.options.sleep, delay)

    async def _collect_telemetry(self, session: RingSession, info: RingInfo, phase: SessionPhaseState) -> None:
        options = self.run.options
        if options.operational is None:
            return
        deadline = asyncio.get_running_loop().time() + options.config.retry.presence_preflight_budget_seconds
        phase.value = "telemetry"
        status: object | None = None
        timeout = remaining_budget(deadline)
        if timeout > 0:
            try:
                status = await bounded(session.read_status(), timeout)
            except asyncio.CancelledError:
                raise
            except Exception as error:  # noqa: BLE001 - optional telemetry
                await report_session_error(options.activity, "telemetry", error, self.run.runtime)
        try:
            timeout = remaining_budget(deadline)
            if timeout <= 0:
                return
            await bounded(
                collect_operational_telemetry(
                    session,
                    status if isinstance(status, RingStatus) else None,
                    info,
                    _quality_aware_operational_emitter(options.operational, phase.quality),
                    clock=TelemetryClock(
                        options.host_time,
                        options.host_clock_synchronized or system_host_clock_synchronized,
                        remaining_budget(deadline),
                    ),
                ),
                timeout,
            )
        except asyncio.CancelledError:
            raise
        except Exception as error:  # noqa: BLE001 - optional telemetry
            await report_session_error(options.activity, "telemetry", error, self.run.runtime)


def _quality_aware_operational_emitter(
    emitter: OperationalEmitter, quality: SessionQuality | None
) -> OperationalEmitter:
    """Copy only the already-collected firmware dimension into session evidence."""

    def emit(event: Mapping[str, object]) -> object:
        if quality is not None and event.get("event") == "pendant_observation":
            firmware = event.get("firmware")
            quality.firmware_version = firmware if isinstance(firmware, str) else None
        return emitter(event)

    return emit


def _quality_terminal(outcome: str | None, teardown_error: bool, error: BaseException | None) -> tuple[str, str]:
    if isinstance(error, asyncio.CancelledError):
        return "cancelled", "cancelled"
    if error is not None:
        return ("retryable_error" if retryable(error) else "fatal_error"), "failed"
    if teardown_error:
        return "teardown_interrupted", "connected_interrupted"
    if outcome in {"drained", "collected"}:
        return "completed", outcome
    return "retryable_error", outcome or "interrupted"


async def recoverable_session_outcome(
    session: RingSession | None,
    phase: SessionPhase,
    error: BaseException,
    options: OpportunisticOptions,
    runtime: CaptureRuntimePort,
) -> str:
    await report_session_error(options.activity, phase, error, runtime)
    outcome = session_retry_outcome(error, session is not None)
    if outcome is None:
        await report_activity(options.activity, "fatal")
        raise error
    if session is not None and phase == "read/reconcile":
        await stop_after_interruption(session, options.timeouts.info)
    return outcome


async def teardown_was_interrupted(
    context: AbstractAsyncContextManager[RingSession],
    primary: BaseException | None,
    timeout: float,
    activity: ActivityCallback | None,
    runtime: CaptureRuntimePort,
) -> bool:
    secondary: list[BaseException] = []
    try:
        await exit_context(context, primary, timeout, secondary)
    except asyncio.CancelledError:
        raise
    except BaseException as error:
        await report_session_error(activity, "teardown", error, runtime)
        if retryable(error):
            return True
        if primary is None:
            await report_activity(activity, "fatal")
        raise
    for error in secondary:
        await report_session_error(activity, "teardown", error, runtime)
    return False


async def stop_after_interruption(session: RingSession, timeout: float) -> None:
    with suppress(Exception):
        await bounded(session.write_control(encode_stop_command()), timeout)


def session_retry_outcome(error: BaseException, connected: bool) -> str | None:
    if not retryable(error):
        return None
    if isinstance(error, CandidateUnavailableError):
        return "candidate_unavailable"
    return "connected_interrupted" if connected else "retry"


def retryable(error: BaseException) -> bool:
    if isinstance(error, collector.RingAcknowledgementError):
        return error.status == STATUS_STORAGE_NOT_READY
    if isinstance(error, collector.TransferInterruptedError) and error.__cause__ is not None:
        return retryable(error.__cause__)
    return isinstance(
        error,
        (
            collector.CollectorTimeoutError,
            collector.AdvanceUncertainError,
            StorageNotReadySessionError,
            NotificationOverflowError,
            RingTransportDisconnectedError,
            RingTransportUnavailableError,
        ),
    )


def storage_not_ready_delay(retry_number: int, retry_config: RetryConfig = DEFAULT_CONFIG.retry) -> float:
    backoff = retry_config.storage_not_ready_backoff
    return backoff[min(retry_number, len(backoff) - 1)]


async def bounded[T](awaitable: Awaitable[T], timeout: float) -> T:
    if timeout <= 0:
        raise ValueError("transfer timeouts must be positive")
    task = asyncio.ensure_future(awaitable)
    try:
        done, _ = await asyncio.wait({task}, timeout=timeout)
    except asyncio.CancelledError:
        await cancel_task(task)
        raise
    if not done:
        await cancel_task(task)
        raise collector.CollectorTimeoutError("opportunistic operation timed out")
    return task.result()


async def exit_context(
    context: AbstractAsyncContextManager[RingSession],
    primary: BaseException | None,
    timeout: float,
    secondary: list[BaseException] | None = None,
) -> None:
    try:
        await bounded(
            context.__aexit__(
                type(primary) if primary is not None else None,
                primary,
                primary.__traceback__ if primary is not None else None,
            ),
            timeout,
        )
    except asyncio.CancelledError:
        raise
    except BaseException as error:
        if primary is None:
            raise
        if secondary is not None:
            secondary.append(error)


async def cancel_task[T](task: asyncio.Future[T]) -> None:
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)


async def report_activity(
    callback: ActivityCallback | None,
    state: str,
    retry_seconds: float | None = None,
    *,
    event: ActivityEvent | None = None,
) -> None:
    if callback is None:
        return
    result = callback(event or ActivityEvent(state, retry_seconds))
    if inspect.isawaitable(result):
        await result


async def report_cooldown_started(
    callback: ActivityCallback | None, duration_seconds: float, next_attempt_in_seconds: float
) -> None:
    await report_activity(
        callback,
        "cooldown_started",
        event=ActivityEvent(
            "cooldown_started",
            reason="clean_drain",
            duration_seconds=duration_seconds,
            next_attempt_in_seconds=next_attempt_in_seconds,
        ),
    )


async def report_session_error(
    callback: ActivityCallback | None,
    phase: SessionPhase,
    error: BaseException,
    runtime: CaptureRuntimePort,
) -> None:
    runtime.debug_exception("session_error", error, phase=phase)
    cause = session_error_cause(error)
    await report_activity(
        callback,
        "session_error",
        event=ActivityEvent(
            "session_error",
            phase=phase,
            error_type=type(cause).__name__,
            error_message=sanitize_error_message(cause),
        ),
    )


async def report_finalization_error(
    callback: ActivityCallback | None,
    operation: Literal["checkpoint", "close"],
    error: BaseException,
    runtime: CaptureRuntimePort,
) -> None:
    """Report teardown failures without allowing observability to skip close."""
    runtime.debug_exception(f"writer_{operation}_error", error, operation=operation, phase="teardown")
    try:
        await report_activity(
            callback,
            f"writer_{operation}_error",
            event=ActivityEvent(
                f"writer_{operation}_error",
                phase="teardown",
                error_type=type(error).__name__,
                error_message=sanitize_error_message(error),
            ),
        )
    except Exception:  # noqa: BLE001 - a failing observer must not retain a writer lease
        return


def session_error_cause(error: BaseException) -> BaseException:
    cause = error
    while isinstance(cause, collector.TransferInterruptedError) and cause.__cause__ is not None:
        cause = cause.__cause__
    return cause


def sanitize_error_message(error: BaseException) -> str:
    if isinstance(error, collector.CollectorTimeoutError):
        return "operation timed out"
    if isinstance(error, collector.AdvanceUncertainError):
        return "advance acknowledgement uncertain"
    if isinstance(error, NotificationOverflowError):
        return "notification queue overflow"
    if isinstance(error, (RingTransportDisconnectedError, RingTransportUnavailableError)):
        return transport_error_chain_message(error)
    return "session operation failed"


def transport_error_chain_message(error: BaseException) -> str:
    summaries: list[str] = []
    seen_exceptions: set[int] = set()
    seen_summaries: set[str] = set()
    cause: BaseException | None = error
    while cause is not None and len(summaries) < DEFAULT_CONFIG.observability.max_error_chain_entries:
        if id(cause) in seen_exceptions:
            break
        seen_exceptions.add(id(cause))
        summary = bounded_error_summary(cause)
        if summary not in seen_summaries:
            summaries.append(summary)
            seen_summaries.add(summary)
        cause = cause.__cause__
    return " <- ".join(summaries)


def bounded_error_summary(error: BaseException) -> str:
    max_chars = DEFAULT_CONFIG.observability.max_error_entry_chars
    type_name = type(error).__name__[: max_chars - 2]
    message_limit = max_chars - len(type_name) - 2
    return f"{type_name}: {str(error)[:message_limit]}"


def remaining_budget(deadline: float) -> float:
    return max(0.0, deadline - asyncio.get_running_loop().time())


async def sleep(sleep_fn: Callable[[float], object], delay: float) -> None:
    result = sleep_fn(delay)
    if inspect.isawaitable(result):
        await result


def validate_policy(
    policy: RetryPolicy, max_drained_fallback_seconds: float = DEFAULT_CONFIG.presence.max_drained_fallback_seconds
) -> None:
    if (
        not policy.backoff
        or policy.drain_cooldown_seconds <= 0
        or policy.drain_cooldown_seconds > max_drained_fallback_seconds
    ):
        raise ValueError("opportunistic recovery policy values must be positive")
    if (
        policy.batch_records <= 0
        or policy.arena_max_bytes <= 0
        or policy.batch_records > policy.arena_max_bytes // RECORD_SIZE
    ):
        raise ValueError("opportunistic recovery policy values must be positive")
    if not isinstance(policy.advance_enabled, bool) or any(delay <= 0 for delay in policy.backoff):
        raise ValueError("opportunistic recovery policy values must be positive")


def validate_presence_policy(options: OpportunisticOptions) -> None:
    presence = options.presence
    if presence is None:
        return
    if (
        options.policy.backoff != presence.policy.rapid_backoff
        or options.policy.drain_cooldown_seconds != presence.policy.drained_fallback_seconds
    ):
        raise ValueError("opportunistic and presence timing policies must match")
