"""Startup recovery and cooperative quarantine maintenance for capture runs."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from dataclasses import dataclass
from threading import Event
from time import monotonic
from typing import Protocol

from ...config import DEFAULT_CONFIG, CollectorConfig
from ..domain.quarantine_machine import (
    QuarantineAction,
    QuarantineEvent,
    QuarantineState,
)
from ..domain.quarantine_machine import (
    transition as quarantine_transition,
)
from ..domain.ring_protocol import RECORD_SIZE
from .ports import (
    AttemptDescriptorShape,
    CaptureRuntimePort,
    RecoveryShape,
    StagingPort,
)
from .presence import PresenceEnd, PresenceWake
from .session_lifecycle import (
    ActivityCallback,
    OpportunisticSyncError,
    join_owned,
    joined_to_thread,
    report_activity,
)
from .storage_fault import FatalInternalAudioStorageError
from .visit_machine import RecoveryDisposition


class PresenceWaiterPort(Protocol):
    """The maintenance race owns only permit acquisition and fail-closed close."""

    async def wait_for_attempt(self) -> PresenceWake | PresenceEnd: ...

    async def close(self) -> None: ...


@dataclass(frozen=True, slots=True)
class PendingStartupState:
    """Validated restart state bound before the first provider attempt.

    ``pending`` being ``None`` is a completed, valid startup result,
    not an indication that startup preparation has not happened yet.
    """

    pending: AttemptDescriptorShape | None
    durable_next: int | None
    disposition: RecoveryDisposition = "empty"


class QuarantineMaintenance:
    """Own restart evidence, quarantine salvage, and presence coordination."""

    def __init__(
        self,
        staging: StagingPort,
        activity: ActivityCallback | None,
        runtime: CaptureRuntimePort,
        *,
        config: CollectorConfig = DEFAULT_CONFIG,
    ) -> None:
        self._staging = staging
        self._activity = activity
        self._runtime = runtime
        self._config = config
        self._startup_state: PendingStartupState | None = None
        self._startup_state_bound = False
        self._maintenance_not_before = 0.0
        self._quarantine_retry_not_before = 0.0
        self._quarantine_retry_number = 0
        self._publication_retry_handle: asyncio.TimerHandle | None = None
        self._publication_retry_task: asyncio.Task[None] | None = None
        self._publication_effect_tasks: set[asyncio.Task[object]] = set()
        self._publication_fatal: FatalInternalAudioStorageError | None = None
        self._publication_fatal_event = asyncio.Event()

    def _raise_publication_fatal(self) -> None:
        if self._publication_fatal is not None:
            raise self._publication_fatal

    def record_storage_fatal(self, error: FatalInternalAudioStorageError) -> None:
        """Wake the owner when an auxiliary writer observes corrupt SQLite."""

        if self._publication_fatal is None:
            self._publication_fatal = error
            self._publication_fatal_event.set()

    async def prepare_pending_startup(self) -> PendingStartupState:
        """Inspect and validate restart evidence exactly once."""
        if self._startup_state is not None:
            return self._startup_state

        await self._recover_and_publish()

        prefix, orphaned = await joined_to_thread(self._staging.inspect_recovery)
        if prefix:
            state = PendingStartupState(None, None, "needs_interrupted_close")
            self._startup_state = state
            return state

        try:
            pending = await self._pending_descriptor()
        except OpportunisticSyncError as error:
            await self._quarantine_pending(str(error), original_error=error)
            pending = None
        except Exception as error:
            if not self._runtime.is_staging_error(error):
                raise
            await self._quarantine_pending(str(error), original_error=error)
            pending = None
        if pending is None:
            disposition: RecoveryDisposition = "needs_interrupted_close" if orphaned else "empty"
            state = PendingStartupState(None, None, disposition)
        else:
            try:
                durable_next = await self._validate_pending_evidence(pending)
            except Exception as error:
                if not self._runtime.is_staging_error(error):
                    raise
                await self._quarantine_pending(str(error), original_error=error)
                state = PendingStartupState(None, None, "empty")
            else:
                state = PendingStartupState(pending, durable_next, "resumable")

        self._startup_state = state
        return state

    def invalidate_startup_state(self) -> None:
        """Force the next machine recovery inspection to reread local evidence."""
        self._startup_state = None
        self._startup_state_bound = False

    async def _recover_and_publish(self) -> None:
        self._raise_publication_fatal()
        task = asyncio.create_task(joined_to_thread(self._staging.recover_and_publish))
        self._publication_effect_tasks.add(task)
        try:
            outcome = await join_owned(task)
        finally:
            self._publication_effect_tasks.discard(task)
        state = getattr(outcome, "state", None)
        if state == "published":
            self._runtime.debug_event("ready_publication_published")
        elif state == "blocked":
            self._runtime.debug_event("ready_publication_blocked", reason=getattr(outcome, "reason", None))
        elif state == "transient":
            self._runtime.debug_event("ready_publication_transient", reason=getattr(outcome, "reason", None))
            self._arm_publication_timer()
        else:
            self._runtime.debug_event("ready_publication_waiting", reason=getattr(outcome, "reason", None))

    async def ensure_publication_ready(self) -> None:
        """Run one bounded local recovery attempt without blocking BLE forever."""
        await self._recover_and_publish()

    def schedule_publication_retry(self) -> None:
        """Dispatch the canonical owner after a durable input event."""
        self._raise_publication_fatal()
        if self._staging.publication_wake_admitted():
            self._start_publication_retry()

    def _arm_publication_timer(self) -> None:
        schedule = self._staging.publication_retry_schedule()
        if schedule is None:
            return
        generation, deadline = schedule
        if self._publication_retry_handle is not None:
            self._publication_retry_handle.cancel()
        self._publication_retry_handle = asyncio.get_running_loop().call_at(
            deadline, self._publication_timer_fired, generation, deadline
        )

    def _publication_timer_fired(self, generation: int, deadline: float) -> None:
        self._publication_retry_handle = None
        if self._publication_fatal is not None:
            return
        if self._staging.publication_timer_fired(generation, deadline):
            self._start_publication_retry()
        else:
            self._arm_publication_timer()

    def _start_publication_retry(self) -> None:
        if self._publication_fatal is not None:
            return
        if self._publication_retry_task is not None and not self._publication_retry_task.done():
            return
        task = asyncio.create_task(self._recover_and_publish())
        self._publication_retry_task = task
        task.add_done_callback(self._consume_publication_retry)

    def _consume_publication_retry(self, task: asyncio.Task[None]) -> None:
        if self._publication_retry_task is task:
            self._publication_retry_task = None
        try:
            task.result()
        except FatalInternalAudioStorageError as error:
            self.record_storage_fatal(error)
            return
        except asyncio.CancelledError:
            pass
        except Exception as error:  # noqa: BLE001 - unexpected retry failures remain visible
            self._runtime.debug_exception("ready_publication_retry_failed", error)
        if self._staging.publication_followup_due():
            self._start_publication_retry()

    async def enter_capture_priority(self) -> None:
        """Stop background publication and revoke stale drain permission before capture."""
        self._raise_publication_fatal()
        self._staging.publication_capture_begin()
        try:
            if self._publication_retry_handle is not None:
                self._publication_retry_handle.cancel()
                self._publication_retry_handle = None
            task = self._publication_retry_task
            if task is not None and not task.done():
                task.cancel()
            active = tuple(self._publication_effect_tasks)
            for effect in active:
                effect.cancel()
            if task is not None or active:
                await join_owned(
                    asyncio.gather(*(active + ((task,) if task is not None else ())), return_exceptions=True)
                )
            self._raise_publication_fatal()
            self._staging.publication_quiesced()
            await joined_to_thread(self._staging.begin_ready_visit)
        except BaseException:
            self.exit_capture_priority()
            raise

    def exit_capture_priority(self) -> None:
        """Release foreground priority and resume one deferred publication."""
        if self._staging.publication_capture_end():
            self._start_publication_retry()

    async def close(self) -> None:
        """Cancel and join local publication retry work before loop shutdown."""
        self._staging.publication_shutdown()
        if self._publication_retry_handle is not None:
            self._publication_retry_handle.cancel()
            self._publication_retry_handle = None
        task = self._publication_retry_task
        self._publication_retry_task = None
        if task is not None and not task.done():
            task.cancel()
        active = tuple(self._publication_effect_tasks)
        for effect in active:
            effect.cancel()
        if task is not None or active:
            await join_owned(asyncio.gather(*(active + ((task,) if task is not None else ())), return_exceptions=True))

    async def run_once(self, should_defer: Callable[[], bool]) -> None:
        """Run one cooperative terminal sweep and quarantine salvage pass."""
        if not self._maintenance_due(should_defer):
            return
        await self._recover_and_publish()
        while self._staging.publication_followup_due() and not should_defer():
            await self._recover_and_publish()
        if should_defer():
            return
        await self._refresh_utc_projection()
        if should_defer():
            return
        await self._sweep_terminal_quarantine(should_defer)
        if should_defer():
            return
        if not self._salvage_due(should_defer):
            return
        await self._salvage_quarantine(should_defer)

    def _maintenance_due(self, should_defer: Callable[[], bool]) -> bool:
        if should_defer():
            return False
        now = monotonic()
        if now < self._maintenance_not_before:
            return False
        self._maintenance_not_before = now + self._config.retry.maintenance_interval_seconds
        return True

    def _salvage_due(self, should_defer: Callable[[], bool]) -> bool:
        if should_defer():
            return False
        return monotonic() >= self._quarantine_retry_not_before

    async def _refresh_utc_projection(self) -> None:
        try:
            await joined_to_thread(self._staging.refresh_utc_projection)
        except FatalInternalAudioStorageError:
            raise
        except Exception as error:  # noqa: BLE001 - projection is retryable idle metadata
            self._runtime.debug_exception("utc_projection_refresh_failed", error)

    async def _sweep_terminal_quarantine(self, should_defer: Callable[[], bool]) -> None:
        while not should_defer():
            try:
                result = await joined_to_thread(
                    self._staging.sweep_terminal_quarantine,
                    should_defer=should_defer,
                )
            except FatalInternalAudioStorageError:
                raise
            except Exception as error:  # noqa: BLE001 - expiry failures preserve evidence
                if not self._runtime.is_device_busy_error(error):
                    self._runtime.debug_exception("terminal_quarantine_sweep_failed", error)
                return
            for source_id in result.removed_source_ids:
                self._runtime.debug_event("terminal_quarantine_deleted", source=source_id)
            if not result.progressed:
                return
            await asyncio.sleep(0)

    async def _salvage_quarantine(self, should_defer: Callable[[], bool]) -> None:
        try:
            sources = await joined_to_thread(
                self._staging.quarantined_attempts,
                should_defer=should_defer,
            )
        except FatalInternalAudioStorageError:
            raise
        except Exception as error:  # noqa: BLE001 - no unsafe inference from unreadable quarantine
            self._runtime.debug_exception("quarantine_scan_failed", error)
            return
        for source in sources:
            if should_defer() or not await self._salvage_quarantined_prefix(source, should_defer):
                return

    async def quarantine_attempt_source(self, attempt_id: str) -> None:
        """Move a discontinuous source aside without diagnostic metadata."""
        await joined_to_thread(self._staging.quarantine_attempt_source, attempt_id)
        await report_activity(self._activity, "evidence_quarantined")
        await self._recover_and_publish()

    async def wait_for_presence_attempt(
        self,
        presence: PresenceWaiterPort,
        bind_startup_state: Callable[[PendingStartupState], None],
    ) -> PresenceWake | PresenceEnd:
        """Race scanning with maintenance while joining both scoped tasks.

        Startup inspection, validation, quarantine, and state binding are never
        deferred. Terminal sweeps and salvage yield to a fresh presence wake.
        """
        defer_requested = Event()
        presence_task = asyncio.create_task(presence.wait_for_attempt())
        fatal_task = asyncio.create_task(self._publication_fatal_event.wait())
        maintenance_task: asyncio.Task[None] | None = None
        primary: BaseException | None = None
        permit_returned = False
        try:
            # Give the scheduler its first turn so scanner startup begins before
            # any filesystem maintenance is scheduled.
            await asyncio.sleep(0)
            maintenance_task = asyncio.create_task(self._prepare_and_run(defer_requested, bind_startup_state))
            while True:
                if maintenance_task is None:
                    done, _ = await asyncio.wait(
                        {presence_task, fatal_task},
                        timeout=self._config.retry.maintenance_interval_seconds,
                        return_when=asyncio.FIRST_COMPLETED,
                    )
                    self._raise_publication_fatal()
                    if presence_task in done:
                        wake = presence_task.result()
                        await self.ensure_publication_ready()
                        permit_returned = True
                        defer_requested.set()
                        return wake
                    maintenance_task = asyncio.create_task(self.run_once(defer_requested.is_set))
                    continue

                done, _ = await asyncio.wait(
                    {presence_task, maintenance_task, fatal_task}, return_when=asyncio.FIRST_COMPLETED
                )
                self._raise_publication_fatal()
                if presence_task in done:
                    wake = presence_task.result()
                    # A scanner wake is not permission to open BLE while local
                    # clock publication is still retrying.
                    await self.ensure_publication_ready()
                    permit_returned = True
                    defer_requested.set()
                    await join_owned(maintenance_task)
                    return wake
                maintenance_task.result()
                maintenance_task = None
        except BaseException as error:
            primary = error
            defer_requested.set()
            try:
                await join_owned(
                    self._join_presence_tasks(
                        presence, presence_task, maintenance_task, permit_returned=permit_returned
                    )
                )
            except BaseException:
                if isinstance(error, asyncio.CancelledError):
                    raise error from None
                raise
            raise
        finally:
            fatal_task.cancel()
            await asyncio.gather(fatal_task, return_exceptions=True)
            # ``wait`` returns with one task complete, but always consume the
            # other result too. This keeps task identity scoped to this call.
            if primary is None and maintenance_task is not None:
                await asyncio.gather(presence_task, maintenance_task, return_exceptions=True)

    async def _join_presence_tasks(
        self,
        presence: PresenceWaiterPort,
        presence_task: asyncio.Task[PresenceWake | PresenceEnd],
        maintenance_task: asyncio.Task[None] | None,
        *,
        permit_returned: bool,
    ) -> None:
        if not presence_task.done():
            presence_task.cancel()
        try:
            await asyncio.gather(presence_task, return_exceptions=True)
            if maintenance_task is not None:
                await maintenance_task
        finally:
            if permit_returned:
                await presence.close()

    async def _prepare_and_run(
        self,
        defer_requested: Event,
        bind_startup_state: Callable[[PendingStartupState], None],
    ) -> None:
        state = await self.prepare_pending_startup()
        if not self._startup_state_bound:
            bind_startup_state(state)
            self._startup_state_bound = True
        await self.run_once(defer_requested.is_set)

    async def _pending_descriptor(self) -> AttemptDescriptorShape | None:
        pending = await joined_to_thread(self._staging.pending_attempts)
        descriptors = pending
        if len(descriptors) > 1:
            raise OpportunisticSyncError("multiple partial attempts block resume")
        return descriptors[0] if descriptors else None

    async def _validate_pending_evidence(self, descriptor: AttemptDescriptorShape) -> int:
        attempt = await joined_to_thread(self._staging.open_attempt, descriptor.attempt_id)
        try:
            recovery = await joined_to_thread(attempt.recover)
            return self._durable_sql_frontier(descriptor, recovery)
        finally:
            await joined_to_thread(attempt.close)

    @staticmethod
    def _durable_sql_frontier(descriptor: AttemptDescriptorShape, recovery: RecoveryShape) -> int:
        if recovery.raw_bytes % RECORD_SIZE:
            raise OpportunisticSyncError("typed recovery returned an unaligned durable frontier")
        raw_records = recovery.raw_bytes // RECORD_SIZE
        if recovery.valid_records != raw_records or raw_records > descriptor.packet_count:
            raise OpportunisticSyncError("typed recovery returned inconsistent durable bounds")
        return descriptor.start_sequence + recovery.valid_records

    async def _salvage_quarantined_prefix(self, source: str, should_defer: Callable[[], bool]) -> bool:
        source_state = await joined_to_thread(self._staging.quarantine_state, source)
        if source_state in {QuarantineState.PUBLISHED, QuarantineState.UNPROCESSABLE}:
            return True
        recovery_event = {
            QuarantineState.RETRYABLE: QuarantineEvent.RECOVERED_RETRYABLE,
            QuarantineState.INVALID_EVIDENCE: QuarantineEvent.RECOVERED_INVALID,
        }[source_state]
        state = quarantine_transition(QuarantineState.RECOVERING, recovery_event).state
        try:
            admission = await joined_to_thread(
                self._runtime.admit_quarantined_prefix,
                source,
                self._staging,
                should_defer=should_defer,
            )
        except asyncio.CancelledError:
            quarantine_transition(state, QuarantineEvent.CANCEL)
            raise
        except FatalInternalAudioStorageError:
            raise
        except Exception as error:  # noqa: BLE001 - the runtime classifies supported salvage failures
            return await self._handle_quarantine_publish_error(source, state, error)
        self._quarantine_retry_number = 0
        self._quarantine_retry_not_before = 0.0
        self._runtime.debug_event(
            "quarantine_prefix_admitted",
            source=source,
            start_sequence=admission.start_sequence,
            next_sequence=admission.next_sequence,
            record_count=admission.record_count,
        )
        await report_activity(self._activity, "prefix_admitted")
        await self._recover_and_publish()
        return True

    async def _handle_quarantine_publish_error(
        self,
        source: str,
        state: QuarantineState,
        error: Exception,
    ) -> bool:
        kind = self._runtime.classify_quarantine_error(error)
        if kind is None:
            raise error
        if kind == "deferred":
            deferred = quarantine_transition(state, QuarantineEvent.DEFER)
            return deferred.action is not QuarantineAction.WAIT
        if kind == "unprocessable":
            pending = quarantine_transition(state, QuarantineEvent.CLASSIFIED_UNPROCESSABLE)
            if pending.action is not QuarantineAction.MARK_UNPROCESSABLE:
                return True
            marked = await self._mark_quarantine(
                source,
                self._staging.mark_quarantine_unprocessable,
                "quarantine_unprocessable",
                str(error),
            )
            quarantine_transition(
                pending.state,
                QuarantineEvent.MARK_COMMITTED if marked else QuarantineEvent.MARK_FAILED,
            )
            return True
        retry = quarantine_transition(state, QuarantineEvent.RETRYABLE_FAILURE)
        if retry.action is not QuarantineAction.KEEP:
            return True
        self._runtime.debug_exception("quarantine_prefix_publish_retryable", error, source=source)
        backoff = self._config.retry.quarantine_publish_backoff_seconds
        self._quarantine_retry_not_before = monotonic() + backoff[min(self._quarantine_retry_number, len(backoff) - 1)]
        self._quarantine_retry_number += 1
        return False

    async def _mark_quarantine(
        self,
        source: str,
        marker: Callable[[str, str], None],
        event: str,
        reason: str,
    ) -> bool:
        try:
            await joined_to_thread(marker, source, reason)
        except FatalInternalAudioStorageError:
            raise
        except Exception as marking_error:  # noqa: BLE001 - source remains safe
            self._runtime.debug_exception(
                "quarantine_invalid_marker_preserved",
                marking_error,
                source=source,
            )
            return False
        else:
            self._runtime.debug_event(event, source=source)
            return True

    async def _quarantine_pending(
        self,
        reason: str,
        *,
        original_error: BaseException | None = None,
    ) -> None:
        moved = await joined_to_thread(self._staging.quarantine_pending, reason)
        if not moved:
            if original_error is not None:
                raise original_error
            raise OpportunisticSyncError("unable to quarantine blocking evidence")
        await report_activity(self._activity, "evidence_quarantined")
        await self._recover_and_publish()
