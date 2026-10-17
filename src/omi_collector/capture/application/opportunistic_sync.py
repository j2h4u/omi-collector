"""Long-lived opportunistic coordination for bounded Omi ring batches.

The coordinator owns presence, retry, and cursor policy. ``collector`` stays
the only owner of the wire protocol and ``staging`` stays the durability
boundary. Partial attempts remain preserved across restarts; only an explicit
checkpoint-prefix publication marker allows one to be retired from admission.
"""

from __future__ import annotations

import asyncio
import threading
from collections.abc import Callable
from contextlib import suppress
from dataclasses import dataclass, replace
from functools import partial
from pathlib import Path

from ..domain.ring_protocol import RingInfo
from . import collector
from .batch_reconciliation import BatchReconciler
from .owned_work import invoke_owned, join_owned, joined_to_thread
from .ports import CaptureRuntimePort, ObservationWriterPort, PublicDrainPort, StagingPort
from .presence import PresenceEnd, PresenceWake
from .quarantine_maintenance import PendingStartupState, QuarantineMaintenance
from .session_lifecycle import (
    OpportunisticOptions,
    SessionLifecycle,
    SessionLifecycleCallbacks,
    SessionLifecycleRun,
    SessionProvider,
    validate_policy,
    validate_presence_policy,
)
from .storage_fault import FatalInternalAudioStorageError
from .visit_machine import RecoveryDisposition

# Cached status and optional characteristics are useful observations, but they
# must never hold up the first audio operation for the 30-second INFO timeout.


class CollectionPreservedCancelledError(asyncio.CancelledError):
    """Cancellation completed cleanup while retaining durable local custody."""

    def __init__(self, preserved_path: Path | None, kind: str, *, preserved_source_id: str | None = None) -> None:
        if (preserved_path is None) == (preserved_source_id is None):
            raise ValueError("preserved custody requires exactly one path or source ID")
        location = str(preserved_path) if preserved_path is not None else f"SQL source {preserved_source_id}"
        super().__init__(f"{kind} preserved at {location}")
        self.preserved_path = preserved_path
        self.preserved_source_id = preserved_source_id
        self.kind = kind


@dataclass(frozen=True, slots=True)
class _Run:
    provider: SessionProvider
    staging: StagingPort
    options: OpportunisticOptions
    observation_writer: ObservationWriterPort
    runtime: CaptureRuntimePort
    maintenance: QuarantineMaintenance
    observation_error_relay: _ObservationErrorRelay
    public_drain: _PublicDrainCoordinator | None = None


@dataclass(slots=True)
class _ObservationErrorRelay:
    lock: threading.Lock
    pending: bool = False
    closing: bool = False
    task: asyncio.Task[None] | None = None
    error: Exception | None = None


@dataclass(slots=True)
class _BackgroundFatalOwner:
    maintenance: QuarantineMaintenance
    task: asyncio.Task[object]
    cleanup_started: bool = False
    notified: bool = False

    def record(self, error: FatalInternalAudioStorageError) -> None:
        self.maintenance.record_storage_fatal(error)
        if self.notified:
            return
        self.notified = True
        if not self.cleanup_started:
            self.task.cancel()


class _PublicDrainCoordinator:
    """Coalesce public observations and serialize replayable file exports."""

    def __init__(self, port: PublicDrainPort, runtime: CaptureRuntimePort) -> None:
        self.port = port
        self.runtime = runtime
        self.wake = asyncio.Event()
        self._db_lock = asyncio.Lock()
        self._ring: tuple[int, str] | None = None
        self._battery: tuple[int, str] | None = None
        self._stopping = False
        self._task: asyncio.Task[None] | None = None
        self._loop = asyncio.get_running_loop()
        self.port.set_wake_callback(self._wake_on_loop)

    def start(self) -> None:
        self._task = asyncio.create_task(self._run())
        self.wake.set()

    def _wake_on_loop(self) -> None:
        self._loop.call_soon_threadsafe(self.wake.set)

    def observe_info(self, info: RingInfo, observed_at: str) -> None:
        self._ring = (info.unread_packets, observed_at)
        self.wake.set()

    def observe_battery(self, percent: int, observed_at: str) -> None:
        self._battery = (percent, observed_at)
        self.wake.set()

    async def before_closure(self) -> None:
        async with self._db_lock:
            ring, battery = None, self._battery
            self._ring = None
            if ring is None and battery is None:
                return
            try:
                await joined_to_thread(self.port.update_observations, ring=ring, battery=battery)
            except Exception as error:  # noqa: BLE001 - public observations are best effort
                self._ring = None
                self._log(error, "observation")
            else:
                if self._ring == ring:
                    self._ring = None
                if self._battery == battery:
                    self._battery = None

    async def publish_after_closure(self) -> None:
        self._ring = None
        self.wake.set()

    async def close(self) -> None:
        self._stopping = True
        self.wake.set()
        try:
            if self._task is not None:
                await join_owned(self._task)
        finally:
            self.port.set_wake_callback(None)

    async def _run(self) -> None:
        while True:
            with suppress(TimeoutError):
                await asyncio.wait_for(self.wake.wait(), timeout=30.0)
            self.wake.clear()
            if self._stopping:
                await self._flush_observations()
                try:
                    await joined_to_thread(self.port.publish_snapshot)
                except Exception as error:  # noqa: BLE001 - final export remains repairable
                    self._log(error, "publish")
                return
            await self._flush_observations()
            try:
                await joined_to_thread(self.port.publish_snapshot)
            except Exception as error:  # noqa: BLE001 - JSON is a repairable projection
                self._log(error, "publish")

    async def _flush_observations(self) -> None:
        async with self._db_lock:
            ring, battery = self._ring, self._battery
            if ring is None and battery is None:
                return
            try:
                await joined_to_thread(self.port.update_observations, ring=ring, battery=battery)
            except Exception as error:  # noqa: BLE001 - public observations are best effort
                self._log(error, "observation")
                return
            if self._ring == ring:
                self._ring = None
            if self._battery == battery:
                self._battery = None

    def _log(self, error: Exception, operation: str) -> None:
        try:
            self.runtime.debug_exception("public_drain_projection_error", error, operation=operation)
        except Exception:  # noqa: BLE001 - diagnostics cannot affect capture
            return


async def run_opportunistic_collector(
    provider: SessionProvider,
    staging: StagingPort,
    options: OpportunisticOptions,
    runtime: CaptureRuntimePort,
) -> collector.CollectResult:
    """Keep draining bounded batches whenever the pendant is present.

    A staging lease belongs to the dedicated writer for one active batch. A
    transport and optional PHY guard belong to a single presence session and
    are recreated only after that session ends.
    """
    _validate_composition(staging, runtime)
    validate_policy(options.policy, options.config.presence.max_drain_cooldown_seconds)
    validate_presence_policy(options)
    if options.stop_intent is not None:
        options.stop_intent.check()
    loop = asyncio.get_running_loop()
    owner = asyncio.current_task()
    assert owner is not None

    options = _configure_clock_state(options, staging, runtime)
    maintenance = QuarantineMaintenance(staging, options.activity, runtime, config=options.config)
    fatal_owner = _BackgroundFatalOwner(maintenance, owner)
    _bind_metrics_fatal(options, loop, fatal_owner.record)
    if options.clock_lease is None:
        options = replace(options, clock_lease=staging.clock_mutation_lease)

    relay = _ObservationErrorRelay(threading.Lock())
    observation_writer = runtime.make_observation_writer(
        staging,
        options.config.firmware_observations,
        _observation_error_reporter(options, runtime, loop, fatal_owner, relay),
    )
    public_drain = options.public_drain
    public_coordinator = _PublicDrainCoordinator(public_drain, runtime) if public_drain is not None else None
    run = _Run(provider, staging, options, observation_writer, runtime, maintenance, relay, public_coordinator)
    reconciler = BatchReconciler(staging, options, runtime, maintenance.quarantine_attempt_source)
    lifecycle = _make_session_lifecycle(run, reconciler)
    cancelled = False
    unwinding = False
    primary_error: BaseException | None = None
    if public_coordinator is not None:
        public_coordinator.start()
    try:
        if options.presence is not None:
            return await lifecycle.run_with_presence()
        return await lifecycle.run_direct()
    except BaseException as error:
        primary_error = error
        unwinding = True
        if isinstance(error, asyncio.CancelledError):
            maintenance._raise_publication_fatal()
            cancelled = True
        raise
    finally:
        fatal_owner.cleanup_started = True
        try:
            await _finalize_run(run, reconciler, cancelled=cancelled, unwinding=unwinding, primary_error=primary_error)
        finally:
            if public_coordinator is not None:
                await public_coordinator.close()


async def _finalize_run(
    run: _Run,
    reconciler: BatchReconciler,
    *,
    cancelled: bool,
    unwinding: bool,
    primary_error: BaseException | None,
) -> None:
    errors: list[BaseException] = []

    async def attempt(operation: Callable[[], object]) -> object | None:
        try:
            result = operation()
            if asyncio.iscoroutine(result) or isinstance(result, asyncio.Future):
                return await result
            return result
        except BaseException as error:  # noqa: BLE001 - cleanup must continue through cancellation
            errors.append(error)
            return None

    preserved_cancel = await _finalize_active(reconciler, cancelled=cancelled, errors=errors)
    await attempt(lambda: _close_observation_writer(run, unwinding=unwinding or bool(errors)))
    await attempt(run.maintenance.close)
    await attempt(lambda: _close_observation_error_relay(run))
    await attempt(lambda: _close_quality_metrics(run))
    await attempt(lambda: asyncio.sleep(0))
    try:
        run.maintenance._raise_publication_fatal()
    except BaseException as error:  # noqa: BLE001 - preserve fatal publication state
        errors.append(error)

    fatal_errors = [error for error in (primary_error, *errors) if isinstance(error, FatalInternalAudioStorageError)]
    if fatal_errors:
        raise fatal_errors[0]
    if preserved_cancel is not None and isinstance(primary_error, asyncio.CancelledError):
        raise preserved_cancel
    if primary_error is not None:
        raise primary_error
    if preserved_cancel is not None:
        raise preserved_cancel
    if errors:
        raise errors[0]


async def _finalize_active(
    reconciler: BatchReconciler,
    *,
    cancelled: bool,
    errors: list[BaseException],
) -> CollectionPreservedCancelledError | None:
    preserved_cancel: CollectionPreservedCancelledError | None = None
    try:
        finalization = await reconciler.finalize_active()
        finalization_succeeded = finalization.checkpoint_error is None and finalization.close_error is None
        custody_retained = finalization.preserved_path is not None or finalization.preserved_source_id is not None
        if cancelled and finalization_succeeded and custody_retained and finalization.preserved_kind is not None:
            preserved_cancel = CollectionPreservedCancelledError(
                finalization.preserved_path,
                finalization.preserved_kind,
                preserved_source_id=finalization.preserved_source_id,
            )
    except BaseException as error:  # noqa: BLE001 - cleanup must continue through cancellation
        errors.append(error)
        return None
    return preserved_cancel


def _observation_error_reporter(
    options: OpportunisticOptions,
    runtime: CaptureRuntimePort,
    loop: asyncio.AbstractEventLoop,
    owner: _BackgroundFatalOwner,
    relay: _ObservationErrorRelay,
) -> Callable[[Exception], None]:
    def report(error: Exception) -> None:
        if isinstance(error, FatalInternalAudioStorageError):
            loop.call_soon_threadsafe(owner.record, error)
            return
        with relay.lock:
            if relay.closing or relay.pending:
                return
            relay.pending = True
            relay.error = error
            loop.call_soon_threadsafe(_start_observation_error_task, options, runtime, relay, owner)

    return report


def _configure_clock_state(
    options: OpportunisticOptions, staging: StagingPort, runtime: CaptureRuntimePort
) -> OpportunisticOptions:
    """Supply the one active clock state capability to each session."""
    return replace(options, clock_state_store=options.clock_state_store or runtime.make_clock_state_store(staging))


def _bind_metrics_fatal(
    options: OpportunisticOptions,
    loop: asyncio.AbstractEventLoop,
    callback: Callable[[FatalInternalAudioStorageError], None],
) -> None:
    register = getattr(options.quality_metrics, "set_fatal_callback", None)
    if callable(register):
        register(lambda error: loop.call_soon_threadsafe(callback, error))


def _validate_composition(staging: object, runtime: object) -> None:
    """Reject an incomplete production composition before storage or BLE work begins."""
    if not isinstance(staging, StagingPort):
        raise TypeError("opportunistic collection requires the complete staging capability")
    if not isinstance(runtime, CaptureRuntimePort):
        raise TypeError("opportunistic collection requires the complete capture runtime capability")


def _make_session_lifecycle(run: _Run, reconciler: BatchReconciler) -> SessionLifecycle:
    """Bind coordinator-owned decisions to one physical-session lifecycle."""

    async def wait_presence_attempt() -> PresenceWake | PresenceEnd:
        presence = run.options.presence
        assert presence is not None

        return await run.maintenance.wait_for_presence_attempt(
            presence,
            lambda _state: None,
        )

    async def load_recovery() -> RecoveryDisposition:
        state = await run.maintenance.prepare_pending_startup()
        _bind_startup_state(reconciler, state)
        return state.disposition

    def invalidate_recovery() -> None:
        run.maintenance.invalidate_startup_state()

    async def observe_info(info: RingInfo) -> None:
        try:
            run.observation_writer.observe(info)
        except FatalInternalAudioStorageError:
            raise
        except Exception as error:  # noqa: BLE001 - observations are best effort
            await _schedule_observation_error(run.options, error, run.runtime)

    async def post_session_checkpoint() -> None:
        await reconciler.checkpoint_after_session()

    async def close_visit(reason: str, drain_cursor: int | None) -> None:
        await reconciler.close_visit(reason, drain_cursor)
        run.maintenance.schedule_publication_retry()

    async def before_direct_attempt() -> None:
        run.staging.publication_capture_intent()
        await run.maintenance.ensure_publication_ready()
        await run.maintenance.run_once(lambda: False)

    callbacks = SessionLifecycleCallbacks(
        before_direct_attempt=before_direct_attempt,
        wait_presence_attempt=wait_presence_attempt,
        connected_step=reconciler.connected_step,
        post_session_checkpoint=post_session_checkpoint,
        completed_batch_query=lambda: reconciler.completed_batches,
        durable_progress_query=reconciler.durable_progress,
        drained_result=reconciler.drained_result,
        observe_info=observe_info,
        observe_public_info=run.public_drain.observe_info if run.public_drain is not None else None,
        observe_public_battery=run.public_drain.observe_battery if run.public_drain is not None else None,
        close_verified_drain=partial(_close_verified_drain, run, reconciler) if run.public_drain is not None else None,
        close_visit=close_visit,
        load_recovery=load_recovery,
        invalidate_recovery=invalidate_recovery,
        enter_capture_priority=run.maintenance.enter_capture_priority,
        exit_capture_priority=run.maintenance.exit_capture_priority,
    )
    return SessionLifecycle(SessionLifecycleRun(run.provider, run.options, run.runtime, callbacks))


async def _close_verified_drain(
    run: _Run,
    reconciler: BatchReconciler,
    info: RingInfo,
    observed_at: str,
) -> None:
    assert run.public_drain is not None
    await run.public_drain.before_closure()
    await reconciler.close_visit("drained", info.read_sequence, verified_drain=(info, observed_at))
    run.maintenance.schedule_publication_retry()
    await run.public_drain.publish_after_closure()


def _bind_startup_state(reconciler: BatchReconciler, state: PendingStartupState) -> None:
    reconciler.set_startup_state(state.pending, state.durable_next)


async def _schedule_observation_error(
    options: OpportunisticOptions, error: Exception, runtime: CaptureRuntimePort
) -> None:
    """Report rejected input without blocking the event loop."""
    await _report_observation_error(options, runtime, error)


def _start_observation_error_task(
    options: OpportunisticOptions,
    runtime: CaptureRuntimePort,
    relay: _ObservationErrorRelay,
    owner: _BackgroundFatalOwner,
) -> None:
    with relay.lock:
        if relay.closing:
            relay.pending = False
            return
        error = relay.error
    if error is None:
        with relay.lock:
            relay.pending = False
        return
    task = asyncio.create_task(_report_observation_error(options, runtime, error))
    relay.task = task
    task.add_done_callback(lambda completed: _consume_observation_task(completed, relay, owner))


async def _report_observation_error(
    options: OpportunisticOptions, runtime: CaptureRuntimePort, error: Exception
) -> None:
    await invoke_owned(runtime.debug_exception, "firmware_observation_writer_error", error, operation="observe")
    if options.operational is None:
        return
    try:
        await invoke_owned(options.operational, {"event": "firmware_observation_error", "outcome": "retrying"})
    except asyncio.CancelledError:
        raise
    except FatalInternalAudioStorageError:
        raise
    except Exception:  # noqa: BLE001 - observability must not affect collection
        return


def _consume_observation_task(
    task: asyncio.Task[None], relay: _ObservationErrorRelay, owner: _BackgroundFatalOwner
) -> None:
    with relay.lock:
        relay.pending = False
        relay.task = None
    try:
        task.result()
    except FatalInternalAudioStorageError as error:
        owner.record(error)
    except asyncio.CancelledError:
        return
    except Exception:  # noqa: BLE001 - nonfatal status delivery is best effort
        return


async def _close_observation_writer(run: _Run, *, unwinding: bool) -> None:
    """Join the writer before lock release, regardless of storage latency."""
    try:
        await joined_to_thread(run.observation_writer.close)
    except asyncio.CancelledError:
        if unwinding:
            # Preserve the cancellation or error already being unwound by the
            # collector; observation teardown must never replace it.
            return
        raise
    except FatalInternalAudioStorageError:
        raise
    except Exception as error:  # noqa: BLE001 - observation teardown is best effort
        await invoke_owned(run.runtime.debug_exception, "firmware_observation_writer_error", error, operation="close")
        return


async def _close_observation_error_relay(run: _Run) -> None:
    relay = run.observation_error_relay
    with relay.lock:
        relay.closing = True
    await asyncio.sleep(0)
    task = relay.task
    if task is not None:
        await join_owned(task)


async def _close_quality_metrics(run: _Run) -> None:
    """Join auxiliary storage before the outer Collector guard is released."""
    close: Callable[[], object] | None = getattr(run.options.quality_metrics, "close", None)
    if close is None:
        return
    try:
        await joined_to_thread(close)
    except Exception as error:
        await invoke_owned(run.runtime.debug_exception, "quality_metrics_writer_error", error, operation="close")
        raise
