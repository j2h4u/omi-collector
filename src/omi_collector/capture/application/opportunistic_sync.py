"""Long-lived opportunistic coordination for bounded Omi ring batches.

The coordinator owns presence, retry, and cursor policy. ``collector`` stays
the only owner of the wire protocol and ``staging`` stays the durability
boundary. Partial attempts remain preserved across restarts; only an explicit
checkpoint-prefix publication marker allows one to be retired from admission.
"""

from __future__ import annotations

import asyncio
import inspect
from contextlib import suppress
from dataclasses import dataclass
from pathlib import Path

from ..domain.ring_protocol import RingInfo
from . import collector
from .batch_reconciliation import BatchReconciler
from .operational_telemetry import OperationalEmitter
from .ports import CaptureRuntimePort, ObservationWriterPort, StagingPort
from .presence import PresenceWake
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
from .session_lifecycle import (
    bounded as _bounded,
)

# Cached status and optional characteristics are useful observations, but they
# must never hold up the first audio operation for the 30-second INFO timeout.


class CollectionPreservedCancelledError(asyncio.CancelledError):
    """Cancellation completed cleanup while retaining an operator-inspectable path."""

    def __init__(self, preserved_path: Path, kind: str) -> None:
        super().__init__(f"{kind} preserved at {preserved_path}")
        self.preserved_path = preserved_path
        self.kind = kind


@dataclass(frozen=True, slots=True)
class _Run:
    provider: SessionProvider
    staging: StagingPort
    device_slug: str
    options: OpportunisticOptions
    observation_writer: ObservationWriterPort
    runtime: CaptureRuntimePort
    maintenance: QuarantineMaintenance


async def run_opportunistic_collector(
    provider: SessionProvider,
    staging: StagingPort,
    device_slug: str,
    options: OpportunisticOptions,
    runtime: CaptureRuntimePort,
) -> collector.CollectResult:
    """Keep draining bounded batches whenever the pendant is present.

    A staging lease belongs to the dedicated writer for one active batch. A
    transport and optional PHY guard belong to a single presence session and
    are recreated only after that session ends.
    """
    validate_policy(options.policy, options.config.presence.max_drained_fallback_seconds)
    validate_presence_policy(options)
    loop = asyncio.get_running_loop()

    def report_observation_error(error: Exception) -> None:
        runtime.debug_exception("firmware_observation_writer_error", error, operation="observe")
        if options.operational is None:
            return
        try:
            loop.call_soon_threadsafe(_start_observation_error_task, options)
        except RuntimeError:
            return

    observation_writer = runtime.make_observation_writer(
        staging, options.config.firmware_observations, report_observation_error
    )

    maintenance = QuarantineMaintenance(staging, device_slug, options.activity, runtime, config=options.config)
    run = _Run(provider, staging, device_slug, options, observation_writer, runtime, maintenance)
    reconciler = BatchReconciler(staging, device_slug, options, runtime, maintenance.quarantine_attempt_source)
    lifecycle = _make_session_lifecycle(run, reconciler)
    cancelled = False
    unwinding = False
    try:
        if options.presence is not None:
            return await lifecycle.run_with_presence()
        state = await maintenance.prepare_pending_startup()
        _bind_startup_state(reconciler, state)
        return await lifecycle.run_legacy()
    except BaseException as error:
        cancelled = isinstance(error, asyncio.CancelledError)
        unwinding = True
        raise
    finally:
        try:
            finalization = await reconciler.finalize_active()
            if (
                cancelled
                and finalization.checkpoint_error is None
                and finalization.close_error is None
                and finalization.preserved_path is not None
                and finalization.preserved_kind is not None
            ):
                raise CollectionPreservedCancelledError(finalization.preserved_path, finalization.preserved_kind)
        except BaseException:
            unwinding = True
            raise
        finally:
            try:
                await _close_observation_writer(run, unwinding=unwinding)
            finally:
                _close_quality_metrics(run)


def _make_session_lifecycle(run: _Run, reconciler: BatchReconciler) -> SessionLifecycle:
    """Bind coordinator-owned decisions to one physical-session lifecycle."""

    async def wait_presence_attempt() -> PresenceWake:
        presence = run.options.presence
        assert presence is not None
        return await run.maintenance.wait_for_presence_attempt(
            presence,
            lambda state: _bind_startup_state(reconciler, state),
        )

    def observe_info(info: RingInfo) -> None:
        try:
            run.observation_writer.observe(run.device_slug, info)
        except Exception as error:  # noqa: BLE001 - observations are best effort
            _schedule_observation_error(run.options, error, run.runtime)

    async def post_session_checkpoint() -> None:
        await reconciler.checkpoint_after_session()

    callbacks = SessionLifecycleCallbacks(
        before_legacy_attempt=lambda: run.maintenance.run_once(lambda: False),
        wait_presence_attempt=wait_presence_attempt,
        connected_step=reconciler.connected_step,
        post_session_checkpoint=post_session_checkpoint,
        completed_batch_query=lambda: reconciler.completed_batches,
        drained_result=reconciler.drained_result,
        observe_info=observe_info,
    )
    return SessionLifecycle(SessionLifecycleRun(run.provider, run.device_slug, run.options, run.runtime, callbacks))


def _bind_startup_state(reconciler: BatchReconciler, state: PendingStartupState) -> None:
    reconciler.set_startup_state(state.pending, state.durable_next)


def _schedule_observation_error(options: OpportunisticOptions, error: Exception, runtime: CaptureRuntimePort) -> None:
    """Relay a writer failure on the event loop without blocking its thread."""
    runtime.debug_exception("firmware_observation_writer_error", error, operation="observe")
    if options.operational is None:
        return
    _start_observation_error_task(options)


def _start_observation_error_task(options: OpportunisticOptions) -> None:
    task = asyncio.create_task(_report_observation_error(options))
    task.add_done_callback(_consume_observation_task)


async def _invoke_operational(emitter: OperationalEmitter, event: dict[str, object]) -> None:
    result = emitter(event)
    if inspect.isawaitable(result):
        await result


async def _report_observation_error(options: OpportunisticOptions) -> None:
    if options.operational is None:
        return
    try:
        await _bounded(
            _invoke_operational(
                options.operational,
                {"event": "firmware_observation_error", "outcome": "retrying"},
            ),
            options.config.retry.presence_preflight_budget_seconds,
        )
    except asyncio.CancelledError:
        raise
    except Exception:  # noqa: BLE001 - observability must not affect collection
        return


def _consume_observation_task(task: asyncio.Task[None]) -> None:
    with suppress(asyncio.CancelledError, Exception):
        task.result()


async def _close_observation_writer(run: _Run, *, unwinding: bool) -> None:
    """Close the run-scoped writer without changing collection outcomes."""
    timeout = run.options.config.firmware_observations.close_timeout_seconds
    try:
        await _bounded(asyncio.to_thread(run.observation_writer.close), timeout)
    except asyncio.CancelledError:
        if unwinding:
            # Preserve the cancellation or error already being unwound by the
            # collector; observation teardown must never replace it.
            return
        raise
    except Exception as error:  # noqa: BLE001 - observation teardown is best effort
        run.runtime.debug_exception("firmware_observation_writer_error", error, operation="close")
        return


def _close_quality_metrics(run: _Run) -> None:
    """Stop auxiliary metric storage with its own bounded daemon join."""
    close = getattr(run.options.quality_metrics, "close", None)
    if close is None:
        return
    try:
        close()
    except Exception as error:  # noqa: BLE001 - metrics teardown is best effort
        run.runtime.debug_exception("quality_metrics_writer_error", error, operation="close")
