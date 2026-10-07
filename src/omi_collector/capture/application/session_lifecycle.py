"""Physical-session lifecycle for opportunistic capture.

This module owns the connection boundary: context entry/exit, bounded INFO
recovery, telemetry, retry classification, activity reporting, and the
presence/direct retry loops. Batch admission and durability decisions stay in
``opportunistic_sync`` and are supplied as a small callback bundle.
"""

from __future__ import annotations

import asyncio
import inspect
import re
import time
from collections.abc import Awaitable, Callable, Mapping
from contextlib import AbstractAsyncContextManager, suppress
from dataclasses import KW_ONLY, dataclass
from typing import Literal, Protocol, cast

from ...config import DEFAULT_CONFIG, CollectorConfig, RetryConfig
from ..domain.ring_protocol import RECORD_SIZE, STATUS_STORAGE_NOT_READY, RingInfo, RingStatus, encode_stop_command
from . import collector
from .operational_telemetry import (
    ClockCorrectionSink,
    ClockObservationSink,
    OperationalEmitter,
    TelemetryClock,
    collect_battery_observation,
    collect_operational_telemetry,
)
from .ports import CaptureRuntimePort, ClockMembershipPort, StorageLeaseFactory
from .presence import PresenceEnd, PresencePolicy, PresenceWake
from .presence_machine import AttemptOutcome, CandidateUnavailable, CleanDrain, ConnectedInterruption, NotConnected
from .quality_metrics import (
    AdvertisementMetric,
    ClockCorrectionMetric,
    QualityMetricsPort,
    SessionQuality,
    TransferSessionMetric,
    utc_timestamp,
)
from .ring_transport import (
    CandidateUnavailableError,
    NotificationOverflowError,
    RingSession,
    RingTransportDisconnectedError,
    RingTransportUnavailableError,
)
from .session_machine import (
    CancellationObserved,
    CheckpointResolved,
    Connected,
    EffectFailed,
    InfoResolved,
    OutcomeReturned,
    PreflightResolved,
    ReadResolved,
    SessionCommand,
    SessionState,
    TeardownResolved,
)
from .session_machine import (
    SessionOutcome as PhysicalSessionOutcome,
)
from .session_machine import (
    initial_state as initial_session_state,
)
from .session_machine import (
    require_command as require_session_command,
)
from .session_machine import (
    transition as transition_session,
)
from .visit_machine import (
    AttemptGranted,
    CloseFailed,
    ClosureCommitted,
    CommitClosure,
    DrainConfirmed,
    FinishVisit,
    InspectRecovery,
    Interrupted,
    NoOp,
    OperatorBatchCompleted,
    PreserveAndStop,
    RecoveryEnded,
    RecoveryLoaded,
    RunAttempt,
    SessionFinished,
    SessionOutcome,
    Shutdown,
    TransitionResult,
    VisitCommand,
    VisitEvent,
    VisitState,
    WaitForAttempt,
    initial_transition,
    transition,
)
from .visit_machine import (
    CandidateUnavailable as MachineCandidateUnavailable,
)
from .visit_machine import RecoveryDisposition as VisitRecoveryDisposition


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


class PresenceSchedulerPort(Protocol):
    """The lifecycle's narrow ownership boundary for one issued permit."""

    @property
    def policy(self) -> PresencePolicy: ...

    @property
    def drained_cooldown_remaining_seconds(self) -> float: ...

    async def wait_for_attempt(self) -> PresenceWake | PresenceEnd: ...

    async def attempt_finished(self, outcome: AttemptOutcome) -> PresenceEnd | None: ...

    def resume_interrupted_visit(self) -> None: ...

    async def close(self) -> None: ...


_BLE_ADDRESS = re.compile(r"(?i)(?<![0-9a-f])(?:[0-9a-f]{2}[:_-]){5}[0-9a-f]{2}(?![0-9a-f])")


@dataclass(frozen=True, slots=True)
class RetryPolicy:
    """Bounded per-operation timings, never a global presence deadline."""

    backoff: tuple[float, ...] = DEFAULT_CONFIG.retry.rapid_backoff
    batch_records: int = DEFAULT_CONFIG.memory.arena_max_bytes // RECORD_SIZE
    stop_after_drained: bool = False
    _: KW_ONLY
    drain_cooldown_seconds: float = DEFAULT_CONFIG.presence.drain_cooldown_seconds
    arena_max_bytes: int = DEFAULT_CONFIG.memory.arena_max_bytes
    advance_enabled: bool = True

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
    presence: PresenceSchedulerPort | None = None
    quality_metrics: QualityMetricsPort | None = None
    clock_correction_sink: ClockCorrectionSink | None = None
    clock_observation_sink: ClockObservationSink | None = None
    clock_membership_store: ClockMembershipPort | None = None
    clock_lease: StorageLeaseFactory | None = None
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
    lock_context: Mapping[str, object] | None = None


@dataclass(frozen=True, slots=True)
class SessionLifecycleCallbacks:
    """Coordinator closures used by the physical-session lifecycle."""

    before_direct_attempt: Callable[[], Awaitable[None]]
    wait_presence_attempt: Callable[[], Awaitable[PresenceWake | PresenceEnd]]
    connected_step: ConnectedStep
    post_session_checkpoint: Callable[[], Awaitable[None]]
    completed_batch_query: Callable[[], int]
    drained_result: Callable[[], collector.CollectResult]
    observe_info: Callable[[RingInfo], object] | None = None
    durable_progress_query: Callable[[], int] | None = None
    close_visit: Callable[[str], Awaitable[None]] | None = None
    load_recovery: Callable[[], Awaitable[VisitRecoveryDisposition]] | None = None
    invalidate_recovery: Callable[[], None] | None = None
    enter_capture_priority: Callable[[], Awaitable[None]] | None = None
    exit_capture_priority: Callable[[], None] | None = None

    def __post_init__(self) -> None:
        if (self.enter_capture_priority is None) != (self.exit_capture_priority is None):
            raise ValueError("capture priority callbacks must be supplied together")


@dataclass(frozen=True, slots=True)
class SessionLifecycleRun:
    provider: SessionProvider
    options: OpportunisticOptions
    runtime: CaptureRuntimePort
    callbacks: SessionLifecycleCallbacks


@dataclass(slots=True)
class _SessionExecution:
    machine: SessionState
    session: RingSession | None = None
    primary: BaseException | None = None
    outcome: str | None = None
    teardown_error: bool = False


@dataclass(slots=True)
class SessionPhaseState:
    value: SessionPhase
    quality: SessionQuality | None = None


class SessionLifecycle:
    """Run physical sessions while delegating transfer policy to callbacks."""

    def __init__(self, run: SessionLifecycleRun) -> None:
        self.run = run
        self._storage_not_ready_responses = 0
        self._pending_wake: PresenceWake | None = None
        self._direct_retry = 0
        self._pending_presence_outcome: CleanDrain | None = None

    async def run_direct(self) -> collector.CollectResult:
        return await self._run_visit_machine(with_presence=False)

    async def run_with_presence(self) -> collector.CollectResult:
        return await self._run_visit_machine(with_presence=True)

    async def _run_visit_machine(self, *, with_presence: bool) -> collector.CollectResult:
        """Execute effects, then feed their completed facts to the pure policy."""
        presence = self.run.options.presence if with_presence else None
        result = initial_transition()
        capture_priority_active = False
        try:
            while True:
                command = result.command
                if isinstance(command, FinishVisit):
                    await self._finish_visit(command.reason, command.stop, presence)
                    if command.stop:
                        return self.run.callbacks.drained_result()
                    result = TransitionResult(result.state, WaitForAttempt())
                    continue
                if isinstance(command, PreserveAndStop):
                    raise asyncio.CancelledError("visit lifecycle stopped before closure acknowledgement")
                if isinstance(command, NoOp):
                    return self.run.callbacks.drained_result()
                if self._should_enter_capture_priority(command, capture_priority_active):
                    capture_priority_active = True
                    enter_capture_priority = self.run.callbacks.enter_capture_priority
                    assert enter_capture_priority is not None
                    await enter_capture_priority()
                event = await self._execute_visit_command(command, result.state, with_presence=with_presence)
                result = transition(result.state, event, stop_after_drained=self.run.options.policy.stop_after_drained)
                capture_priority_active = self._release_capture_priority_after(result.command, capture_priority_active)
        except asyncio.CancelledError:
            transition(result.state, Shutdown())
            raise
        finally:
            self._release_capture_priority_after(None, capture_priority_active)
            if presence is not None:
                await presence.close()

    def _should_enter_capture_priority(self, command: VisitCommand, active: bool) -> bool:
        return (
            isinstance(command, (RunAttempt, CommitClosure))
            and not active
            and self.run.callbacks.enter_capture_priority is not None
        )

    def _release_capture_priority_after(self, next_command: VisitCommand | None, active: bool) -> bool:
        if active and not isinstance(next_command, (CommitClosure, InspectRecovery)):
            if self.run.callbacks.exit_capture_priority is not None:
                self.run.callbacks.exit_capture_priority()
            return False
        return active

    async def _execute_visit_command(
        self, command: VisitCommand, state: VisitState, *, with_presence: bool
    ) -> VisitEvent:
        try:
            return await self._run_visit_command(command, with_presence=with_presence)
        except BaseException:
            if isinstance(command, CommitClosure):
                transition(state, CloseFailed())
            raise

    async def _run_visit_command(self, command: VisitCommand, *, with_presence: bool) -> VisitEvent:
        if isinstance(command, InspectRecovery):
            return RecoveryLoaded(await self._load_recovery())
        if isinstance(command, WaitForAttempt):
            return await self._wait_for_visit_attempt(command, with_presence=with_presence)
        if isinstance(command, RunAttempt):
            return SessionFinished(await self._attempt_visit(with_presence=with_presence))
        if isinstance(command, CommitClosure):
            await self._close_visit(command.reason)
            invalidate = self.run.callbacks.invalidate_recovery
            if command.reason == "restart_interrupted" and invalidate is not None:
                invalidate()
            return ClosureCommitted()
        raise RuntimeError(f"unhandled visit command: {type(command).__name__}")

    async def _wait_for_visit_attempt(
        self, command: WaitForAttempt, *, with_presence: bool
    ) -> AttemptGranted | RecoveryEnded:
        if not with_presence:
            if command.previous_outcome is not None:
                delay = self.run.options.policy.delay_for(self._direct_retry)
                self._direct_retry += 1
                await report_activity(self.run.options.activity, "away", delay)
                await sleep(self.run.options.sleep, delay)
            await self.run.callbacks.before_direct_attempt()
            return AttemptGranted()
        presence = self.run.options.presence
        assert presence is not None
        if command.arm_restored:
            presence.resume_interrupted_visit()
        end = await self._submit_presence_outcome(command.previous_outcome, presence)
        if end is not None:
            return _visit_recovery_end(end)
        if command.previous_outcome is not None:
            await report_activity(self.run.options.activity, "away")
        wake = await self.run.callbacks.wait_presence_attempt()
        if isinstance(wake, PresenceEnd):
            return _visit_recovery_end(wake)
        self._pending_wake = wake
        return AttemptGranted()

    async def _attempt_visit(self, *, with_presence: bool) -> SessionOutcome:
        completed_before = self.run.callbacks.completed_batch_query()
        progress_before = self._durable_progress()
        outcome = await self._run_one_attempt(with_presence)
        durable_progress = (
            self.run.callbacks.completed_batch_query() > completed_before or self._durable_progress() > progress_before
        )
        event = self._session_event(outcome, durable_progress)
        if isinstance(event, DrainConfirmed):
            self._pending_presence_outcome = CleanDrain() if with_presence else None
            self._direct_retry = 0
        elif self.run.callbacks.completed_batch_query() > completed_before:
            self._direct_retry = 0
        if (
            with_presence
            and isinstance(event, Interrupted)
            and self.run.callbacks.completed_batch_query() > completed_before
        ):
            await report_activity(self.run.options.activity, "batch_complete")
        return event

    async def _load_recovery(self) -> VisitRecoveryDisposition:
        callback = self.run.callbacks.load_recovery
        if callback is None:
            return "empty"
        return await callback()

    def _take_wake(self) -> PresenceWake | None:
        wake = self._pending_wake
        self._pending_wake = None
        return wake

    async def _submit_presence_outcome(
        self, previous: Interrupted | MachineCandidateUnavailable | None, presence: PresenceSchedulerPort
    ) -> PresenceEnd | None:
        if previous is None:
            return None
        if isinstance(previous, Interrupted):
            outcome: AttemptOutcome = (
                ConnectedInterruption(previous.durable_progress)
                if previous.connected
                else NotConnected(previous.durable_progress)
            )
        else:
            outcome = CandidateUnavailable()
        return await presence.attempt_finished(outcome)

    async def _run_one_attempt(self, with_presence: bool) -> str:
        wake = self._take_wake() if with_presence else None
        candidate = wake.candidate if isinstance(wake, PresenceWake) else None
        advertisement = wake.advertisement_rssi_dbm if isinstance(wake, PresenceWake) else None
        if with_presence and (
            wake is None
            or not _wake_is_fresh(
                wake, self.run.options.clock(), self.run.options.config.presence.arrival_max_gap_seconds
            )
        ):
            return "candidate_unavailable"
        await report_activity(self.run.options.activity, "connecting")
        context, outcome = await self._open_context(candidate)
        if context is not None:
            outcome = await self.run_session(context, advertisement)
        return outcome

    @staticmethod
    def _session_event(
        outcome: str, durable_progress: bool
    ) -> DrainConfirmed | Interrupted | MachineCandidateUnavailable | OperatorBatchCompleted:
        if outcome == "drained":
            return DrainConfirmed()
        if outcome == "collected":
            return OperatorBatchCompleted()
        if outcome == "candidate_unavailable":
            return MachineCandidateUnavailable()
        if outcome in {"retry", "connected_interrupted"}:
            return Interrupted(outcome == "connected_interrupted", durable_progress)
        raise RuntimeError(f"unknown lifecycle attempt outcome: {outcome}")

    async def _finish_visit(self, reason: str, stop: bool, presence: PresenceSchedulerPort | None) -> None:
        if reason != "drained":
            return
        if presence is not None and self._pending_presence_outcome is not None:
            await presence.attempt_finished(self._pending_presence_outcome)
            self._pending_presence_outcome = None
        await report_activity(self.run.options.activity, "drained")
        if not stop:
            cooldown = (
                presence.policy.drain_cooldown_seconds
                if presence is not None
                else self.run.options.policy.drain_cooldown_seconds
            )
            remaining = presence.drained_cooldown_remaining_seconds if presence is not None else cooldown
            await report_cooldown_started(self.run.options.activity, cooldown, remaining)
            if presence is None:
                await sleep(self.run.options.sleep, cooldown)

    async def _close_visit(self, reason: str) -> None:
        callback = self.run.callbacks.close_visit
        if callback is not None:
            await callback(reason)

    def _durable_progress(self) -> int:
        query = self.run.callbacks.durable_progress_query
        return query() if query is not None else 0

    async def _open_context(
        self, candidate: object | None
    ) -> tuple[AbstractAsyncContextManager[RingSession] | None, str]:
        try:
            return self.run.provider(candidate), "retry"
        except BaseException as error:
            await report_session_error(self.run.options.activity, "connect", error, self.run.runtime)
            if isinstance(error, CandidateUnavailableError):
                return None, "candidate_unavailable"
            if not _retryable_for_runtime(error, self.run.runtime):
                await report_activity(self.run.options.activity, "fatal")
                raise
            return None, "retry"

    async def run_session(
        self, context: AbstractAsyncContextManager[RingSession], advertisement_rssi_dbm: int | None = None
    ) -> str:
        """Run and terminally account for one connected physical session."""
        execution = _SessionExecution(initial_session_state())
        terminal_error: BaseException | None = None
        quality = SessionQuality(advertisement_rssi_dbm, self.run.options.phy_policy)
        phase = SessionPhaseState("connect", quality)
        self._storage_not_ready_responses = 0
        self._record_advertisement_quality(quality)
        try:
            await self._run_session_effects(context, execution, phase)
            require_session_command(execution.machine, SessionCommand.CHECKPOINT)
            await self.run.callbacks.post_session_checkpoint()
            execution.machine = transition_session(execution.machine, CheckpointResolved())
            if execution.teardown_error:
                transition_session(execution.machine, OutcomeReturned("connected_interrupted"))
                return "connected_interrupted"
            if execution.outcome is not None:
                transition_session(
                    execution.machine,
                    OutcomeReturned(cast(PhysicalSessionOutcome, execution.outcome)),
                )
                return execution.outcome
            raise RuntimeError("opportunistic session ended without an outcome")
        except BaseException as error:
            terminal_error = error
            if execution.machine.command not in {
                SessionCommand.FAILED,
                SessionCommand.CANCELLED,
                SessionCommand.RETURNED,
            }:
                event = CancellationObserved() if isinstance(error, asyncio.CancelledError) else EffectFailed(None)
                execution.machine = transition_session(execution.machine, event)
            raise
        finally:
            await self._record_session_quality(quality, execution.outcome, execution.teardown_error, terminal_error)

    async def _run_session_effects(
        self,
        context: AbstractAsyncContextManager[RingSession],
        execution: _SessionExecution,
        phase: SessionPhaseState,
    ) -> None:
        try:
            require_session_command(execution.machine, SessionCommand.CONNECT)
            execution.session = await bounded(context.__aenter__(), self.run.options.timeouts.info)
            execution.machine = transition_session(execution.machine, Connected())
            phase.value = "info"
            require_session_command(execution.machine, SessionCommand.INFO)
            info = await self._info(execution.session)
            execution.machine = transition_session(execution.machine, InfoResolved())
            require_session_command(execution.machine, SessionCommand.PREFLIGHT)
            preflight = await self._collect_telemetry(execution.session, info, phase)
            execution.machine = transition_session(execution.machine, PreflightResolved(preflight))
            execution.outcome, execution.machine = await self._read_to_terminal(
                execution.session, info, execution.machine, phase
            )
        except asyncio.CancelledError as error:
            execution.primary = error
            execution.machine = transition_session(execution.machine, CancellationObserved())
            raise
        except Exception as error:  # noqa: BLE001 - backend errors reach retry policy
            execution.primary = error
            await self._resolve_session_error(error, execution, phase)
        finally:
            if execution.session is not None:
                require_session_command(execution.machine, SessionCommand.TEARDOWN)
                correction_sink = self.run.options.clock_correction_sink
                execution.teardown_error = await teardown_was_interrupted(
                    context,
                    execution.primary,
                    self.run.options.timeouts.info,
                    self.run.options.activity,
                    self.run.runtime,
                    on_transport_closed=(correction_sink.note_transport_closed if correction_sink else None),
                )
                execution.machine = transition_session(execution.machine, TeardownResolved(execution.teardown_error))

    async def _resolve_session_error(
        self, error: Exception, execution: _SessionExecution, phase: SessionPhaseState
    ) -> None:
        try:
            execution.outcome = await recoverable_session_outcome(
                execution.session, phase.value, error, self.run.options, self.run.runtime
            )
        except asyncio.CancelledError:
            execution.machine = transition_session(execution.machine, CancellationObserved())
            raise
        except BaseException:
            execution.machine = transition_session(execution.machine, EffectFailed(None))
            raise
        execution.machine = transition_session(
            execution.machine,
            EffectFailed(cast(PhysicalSessionOutcome, execution.outcome)),
        )

    async def _read_to_terminal(
        self,
        session: RingSession,
        current: RingInfo | None,
        machine: SessionState,
        phase: SessionPhaseState,
    ) -> tuple[str, SessionState]:
        while True:
            phase.value = "read/reconcile"
            require_session_command(machine, SessionCommand.READ)
            outcome, current = await self.run.callbacks.connected_step(session, current, self._info, phase)
            if outcome not in ("drained", "collected"):
                machine = transition_session(machine, ReadResolved("pending"))
                continue
            if current is not None:
                await self._refresh_battery(session, current, phase)
            machine = transition_session(machine, ReadResolved(cast(Literal["drained", "collected"], outcome)))
            return outcome, machine

    def _record_advertisement_quality(self, quality: SessionQuality) -> None:
        """Persist scanner evidence immediately without delaying connection setup."""
        metrics = self.run.options.quality_metrics
        if metrics is None or quality.advertisement_rssi_dbm is None:
            return
        try:
            metrics.record_advertisement(
                AdvertisementMetric(
                    utc_timestamp(self.run.options.host_time()),
                    quality.session_id,
                    quality.advertisement_rssi_dbm,
                    metrics.release_version,
                    metrics.source_revision,
                    quality.phy_policy,
                )
            )
        except Exception as metrics_error:  # noqa: BLE001 - metrics cannot stop audio capture
            self.run.runtime.debug_exception(
                "quality_metrics_write_error", metrics_error, event_type="advertisement_observation"
            )

    async def _record_session_quality(
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
        termination_class, terminal_outcome = _quality_terminal(outcome, teardown_error, error, self.run.runtime)
        try:
            metric = TransferSessionMetric(
                utc_timestamp(self.run.options.host_time()),
                quality.session_id,
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
            metrics.record_transfer_session(metric)
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

    async def _collect_telemetry(
        self, session: RingSession, info: RingInfo, phase: SessionPhaseState
    ) -> Literal["disabled", "completed", "degraded"]:
        options = self.run.options
        if options.operational is None and options.clock_correction_sink is None:
            return "disabled"
        deadline = asyncio.get_running_loop().time() + options.config.retry.presence_preflight_budget_seconds
        phase.value = "telemetry"
        emitter = _quality_aware_operational_emitter(
            options.operational or (lambda _event: None), phase.quality, options.quality_metrics, options.host_time
        )
        status: object | None = None
        timeout = remaining_budget(deadline)
        try:
            timeout = max(remaining_budget(deadline), 0.001)
            # Reserve the latter half of this bounded preflight for status and
            # metadata after the independent clock stage has completed.
            operation_timeout = min(options.config.telemetry.optional_operation_timeout_seconds, timeout / 2)

            async def run_telemetry() -> None:
                await bounded(
                    collect_operational_telemetry(
                        session,
                        status if isinstance(status, RingStatus) else None,
                        info,
                        emitter,
                        clock=TelemetryClock(
                            now=options.host_time,
                            synchronized=options.host_clock_synchronized,
                            operation_timeout=operation_timeout,
                            host_clock_probe_timeout=options.config.telemetry.host_clock_probe_timeout_seconds,
                            info_reader=lambda: self._info(session),
                            status_reader=session.read_status if options.operational is not None else None,
                            correction_sink=options.clock_correction_sink,
                            observation_sink=options.clock_observation_sink,
                            membership_store=options.clock_membership_store,
                            monotonic=options.clock,
                            session_id=phase.quality.session_id if phase.quality is not None else "native",
                            mutation_lease=options.clock_lease,
                        ),
                    ),
                    timeout,
                )

            await run_telemetry()
            return "completed"
        except asyncio.CancelledError:
            raise
        except Exception as error:  # noqa: BLE001 - optional telemetry
            await report_session_error(options.activity, "telemetry", error, self.run.runtime)
            return "degraded"

    async def _refresh_battery(self, session: RingSession, info: RingInfo, phase: SessionPhaseState) -> None:
        options = self.run.options
        if options.operational is None:
            return
        emitter = _quality_aware_operational_emitter(
            options.operational, phase.quality, options.quality_metrics, options.host_time
        )
        try:
            await collect_battery_observation(
                session,
                info,
                emitter,
                operation_timeout=min(
                    options.config.telemetry.optional_operation_timeout_seconds,
                    options.config.retry.presence_preflight_budget_seconds,
                ),
            )
        except asyncio.CancelledError:
            raise
        except Exception as error:  # noqa: BLE001 - battery refresh is optional
            await report_session_error(options.activity, "telemetry", error, self.run.runtime)


def _quality_aware_operational_emitter(
    emitter: OperationalEmitter,
    quality: SessionQuality | None,
    metrics: QualityMetricsPort | None = None,
    host_time: Callable[[], float] = time.time,
) -> OperationalEmitter:
    """Copy only the already-collected firmware dimension into session evidence."""

    def emit(event: Mapping[str, object]) -> object:
        emitted_event = event
        if quality is not None and event.get("event") == "pendant_observation":
            firmware = event.get("firmware")
            if isinstance(firmware, str):
                quality.firmware_version = firmware
            elif quality.firmware_version is not None:
                emitted_event = {**event, "firmware": quality.firmware_version}
        if quality is not None and metrics is not None:
            _record_clock_correction(emitted_event, quality, metrics, host_time)
        return emitter(emitted_event)

    return emit


def _record_clock_correction(
    event: Mapping[str, object],
    quality: SessionQuality,
    metrics: QualityMetricsPort,
    host_time: Callable[[], float],
) -> None:
    drift = event.get("drift_seconds")
    target = event.get("target_epoch")
    boundary_min = event.get("boundary_sequence_min")
    boundary_max = event.get("boundary_sequence_max")
    if not (
        event.get("event") == "pendant_clock_sync"
        and event.get("outcome") == "verified"
        and isinstance(drift, float)
        and isinstance(target, int)
        and isinstance(boundary_min, int)
        and isinstance(boundary_max, int)
    ):
        return
    try:
        metrics.record_clock_correction(
            ClockCorrectionMetric(
                utc_timestamp(host_time()),
                quality.session_id,
                drift,
                target,
                boundary_min,
                boundary_max,
                metrics.release_version,
                metrics.source_revision,
                quality.firmware_version,
            )
        )
    except Exception:  # noqa: BLE001 - metrics cannot stop audio capture
        return


def _quality_terminal(
    outcome: str | None,
    teardown_error: bool,
    error: BaseException | None,
    runtime: CaptureRuntimePort,
) -> tuple[str, str]:
    if isinstance(error, asyncio.CancelledError):
        return "cancelled", "cancelled"
    if error is not None:
        return ("retryable_error" if _retryable_for_runtime(error, runtime) else "fatal_error"), "failed"
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
    outcome = session_retry_outcome(error, session is not None, is_device_busy=runtime.is_device_busy_error)
    if outcome is None:
        await report_activity(options.activity, "fatal")
        raise error
    if session is not None and phase == "read/reconcile":
        await stop_after_interruption(session, options.timeouts.info)
    return outcome


async def teardown_was_interrupted(  # noqa: PLR0913 - close receipt belongs at the physical-session boundary
    context: AbstractAsyncContextManager[RingSession],
    primary: BaseException | None,
    timeout: float,
    activity: ActivityCallback | None,
    runtime: CaptureRuntimePort,
    *,
    on_transport_closed: Callable[[], None] | None = None,
) -> bool:
    secondary: list[BaseException] = []
    try:
        await exit_context(context, primary, timeout, secondary)
    except asyncio.CancelledError:
        raise
    except BaseException as error:
        await report_session_error(activity, "teardown", error, runtime)
        if _retryable_for_runtime(error, runtime):
            return True
        if primary is None:
            await report_activity(activity, "fatal")
        raise
    for error in secondary:
        await report_session_error(activity, "teardown", error, runtime)
    if on_transport_closed is not None and not secondary:
        try:
            on_transport_closed()
        except Exception as error:  # noqa: BLE001 - fencing failure keeps clock reconciliation conservative
            await report_session_error(activity, "teardown", error, runtime)
    return False


async def stop_after_interruption(session: RingSession, timeout: float) -> None:
    with suppress(Exception):
        await bounded(session.write_control(encode_stop_command()), timeout)


def session_retry_outcome(
    error: BaseException,
    connected: bool,
    *,
    is_device_busy: Callable[[BaseException], bool] | None = None,
) -> str | None:
    if not _retryable(error, is_device_busy=is_device_busy):
        return None
    if isinstance(error, CandidateUnavailableError):
        return "candidate_unavailable"
    return "connected_interrupted" if connected else "retry"


def presence_attempt_outcome(outcome: str, durable_progress: bool) -> AttemptOutcome:
    """Translate completed lifecycle work into the scheduler's closed outcome union."""
    if outcome in {"drained", "collected"}:
        return CleanDrain()
    if outcome == "candidate_unavailable":
        return CandidateUnavailable()
    if outcome == "connected_interrupted":
        return ConnectedInterruption(durable_progress)
    if outcome == "retry":
        return NotConnected(durable_progress)
    raise RuntimeError(f"unknown lifecycle attempt outcome: {outcome}")


def _visit_recovery_end(end: PresenceEnd) -> RecoveryEnded:
    if end.reason not in {"absence", "recovery_exhausted"}:
        raise ValueError(f"unknown presence end reason: {end.reason}")
    return RecoveryEnded(cast(Literal["absence", "recovery_exhausted"], end.reason))


def _wake_is_fresh(wake: PresenceWake, now: float, max_gap: float) -> bool:
    return wake.observed_at is not None and now < wake.observed_at + max_gap


def _retryable_for_runtime(error: BaseException, runtime: CaptureRuntimePort) -> bool:
    return _retryable(error, is_device_busy=runtime.is_device_busy_error)


def _retryable(
    error: BaseException,
    *,
    is_device_busy: Callable[[BaseException], bool] | None = None,
) -> bool:
    if is_device_busy is not None and is_device_busy(error):
        return True
    if isinstance(error, collector.RingAcknowledgementError):
        return error.status == STATUS_STORAGE_NOT_READY
    if isinstance(error, collector.TransferInterruptedError) and error.__cause__ is not None:
        return _retryable(error.__cause__, is_device_busy=is_device_busy)
    return isinstance(
        error,
        (
            collector.CollectorTimeoutError,
            collector.AdvanceUncertainError,
            StorageNotReadySessionError,
            NotificationOverflowError,
            RingTransportDisconnectedError,
            RingTransportUnavailableError,
            TimeoutError,
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


async def joined_to_thread[T](function: Callable[..., T], *args: object, **kwargs: object) -> T:
    """Join a storage mutation before propagating cancellation to its caller."""
    return await join_owned(asyncio.to_thread(function, *args, **kwargs))


async def join_owned[T](awaitable: Awaitable[T]) -> T:
    """Keep ownership through repeated cancellation until the task settles."""
    task = asyncio.ensure_future(awaitable)
    try:
        return await asyncio.shield(task)
    except asyncio.CancelledError as cancelled:
        while not task.done():
            with suppress(BaseException):
                await asyncio.shield(task)
        try:
            task.result()
        except BaseException as mutation_error:
            raise cancelled from mutation_error
        raise cancelled


async def cancel_task[T](task: asyncio.Future[T]) -> None:
    task.cancel()
    await join_owned(asyncio.gather(task, return_exceptions=True))


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
    lock_context = _lock_context(cause)
    await report_activity(
        callback,
        "session_error",
        event=ActivityEvent(
            "session_error",
            phase=phase,
            error_type=type(cause).__name__,
            error_message=sanitize_error_message(cause),
            lock_context=lock_context,
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


def _lock_context(error: BaseException) -> Mapping[str, object] | None:
    value = getattr(error, "lock_context", None)
    serializer = getattr(value, "as_dict", None)
    if not callable(serializer):
        return None
    result = cast(Callable[[], object], serializer)()
    return result if isinstance(result, Mapping) else None


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
    message = _BLE_ADDRESS.sub("[BLE address]", str(error))
    return f"{type_name}: {message[:message_limit]}"


def remaining_budget(deadline: float) -> float:
    return max(0.0, deadline - asyncio.get_running_loop().time())


async def sleep(sleep_fn: Callable[[float], object], delay: float) -> None:
    result = sleep_fn(delay)
    if inspect.isawaitable(result):
        await result


def validate_policy(
    policy: RetryPolicy, max_drain_cooldown_seconds: float = DEFAULT_CONFIG.presence.max_drain_cooldown_seconds
) -> None:
    if (
        not policy.backoff
        or policy.drain_cooldown_seconds <= 0
        or policy.drain_cooldown_seconds > max_drain_cooldown_seconds
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
        or options.policy.drain_cooldown_seconds != presence.policy.drain_cooldown_seconds
    ):
        raise ValueError("opportunistic and presence timing policies must match")
