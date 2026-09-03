"""Startup recovery and cooperative quarantine maintenance for capture runs."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from threading import Event
from typing import cast

from .ports import (
    AttemptDescriptorShape,
    CaptureRuntimePort,
    QuarantinePublicationShape,
    StagedAttemptShape,
    StagingPort,
)
from .presence import PresenceScheduler, PresenceWake
from .session_lifecycle import (
    ActivityCallback,
    OpportunisticSyncError,
    report_activity,
)


@dataclass(frozen=True, slots=True)
class PendingStartupState:
    """Validated restart state bound before the first provider attempt.

    ``pending`` being ``None`` is a completed, valid startup result,
    not an indication that startup preparation has not happened yet.
    """

    pending: AttemptDescriptorShape | None
    durable_next: int | None


class QuarantineMaintenance:
    """Own restart evidence, quarantine salvage, and presence coordination."""

    def __init__(
        self,
        staging: StagingPort,
        device_slug: str,
        activity: ActivityCallback | None,
        runtime: CaptureRuntimePort,
    ) -> None:
        self._staging = staging
        self._device_slug = device_slug
        self._activity = activity
        self._runtime = runtime
        self._startup_state: PendingStartupState | None = None

    async def prepare_pending_startup(self) -> PendingStartupState:
        """Inspect and validate restart evidence exactly once."""
        if self._startup_state is not None:
            return self._startup_state

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
            state = PendingStartupState(None, None)
        elif getattr(pending, "mode", None) != "streaming":
            await self._quarantine_pending("non-streaming partial evidence")
            state = PendingStartupState(None, None)
        else:
            try:
                durable_next = await self._validate_pending_evidence(pending)
            except Exception as error:
                if not self._runtime.is_staging_error(error):
                    raise
                await self._quarantine_pending(str(error), original_error=error)
                state = PendingStartupState(None, None)
            else:
                state = PendingStartupState(pending, durable_next)

        self._startup_state = state
        return state

    async def run_once(self, should_defer: Callable[[], bool]) -> None:
        """Run one cooperative terminal sweep and quarantine salvage pass."""
        if should_defer():
            return
        await self._sweep_terminal_retired(should_defer)
        if should_defer():
            return
        try:
            removed = await asyncio.to_thread(
                self._staging.sweep_terminal_quarantine,
                self._device_slug,
                should_defer=should_defer,
            )
        except Exception as error:  # noqa: BLE001 - expiry failures preserve evidence
            self._runtime.debug_exception("terminal_quarantine_sweep_failed", error, device_slug=self._device_slug)
        else:
            for path in removed:
                self._runtime.debug_event(
                    "terminal_quarantine_deleted", device_slug=self._device_slug, source=path.name
                )
        if should_defer():
            return
        try:
            sources = await asyncio.to_thread(
                self._staging.quarantined_attempts,
                self._device_slug,
                should_defer=should_defer,
            )
        except Exception as error:  # noqa: BLE001 - no unsafe inference from unreadable quarantine
            self._runtime.debug_exception("quarantine_scan_failed", error, device_slug=self._device_slug)
            return
        for source in sources:
            if should_defer() or not await self._salvage_quarantined_prefix(source, should_defer):
                return

    async def quarantine_attempt_source(self, attempt_id: str) -> None:
        """Move a discontinuous source aside without diagnostic metadata."""
        await asyncio.to_thread(self._staging.quarantine_attempt_source, self._device_slug, attempt_id)
        await report_activity(self._activity, "evidence_quarantined")

    async def wait_for_presence_attempt(
        self,
        presence: PresenceScheduler,
        bind_startup_state: Callable[[PendingStartupState], None],
    ) -> PresenceWake:
        """Race scanning with maintenance while joining both scoped tasks.

        Startup inspection, validation, quarantine, and state binding are never
        deferred. Terminal sweeps and salvage yield to a fresh presence wake.
        """
        defer_requested = Event()
        presence_task = asyncio.create_task(presence.wait_for_attempt())
        maintenance_task: asyncio.Task[None] | None = None
        primary: BaseException | None = None
        try:
            # Give the scheduler its first turn so scanner startup begins before
            # any filesystem maintenance is scheduled.
            await asyncio.sleep(0)
            maintenance_task = asyncio.create_task(self._prepare_and_run(defer_requested, bind_startup_state))
            done, _ = await asyncio.wait({presence_task, maintenance_task}, return_when=asyncio.FIRST_COMPLETED)
            if presence_task in done:
                wake = presence_task.result()
                defer_requested.set()
                await asyncio.shield(maintenance_task)
                return wake
            maintenance_task.result()
            return await presence_task
        except BaseException as error:
            primary = error
            defer_requested.set()
            if not presence_task.done():
                presence_task.cancel()
            await asyncio.gather(presence_task, return_exceptions=True)
            if maintenance_task is not None and not maintenance_task.done():
                try:
                    await asyncio.shield(maintenance_task)
                except BaseException:
                    if isinstance(error, asyncio.CancelledError):
                        raise error from None
                    raise
            raise
        finally:
            # ``wait`` returns with one task complete, but always consume the
            # other result too. This keeps task identity scoped to this call.
            if primary is None and maintenance_task is not None:
                await asyncio.gather(presence_task, maintenance_task, return_exceptions=True)

    async def _prepare_and_run(
        self,
        defer_requested: Event,
        bind_startup_state: Callable[[PendingStartupState], None],
    ) -> None:
        state = await self.prepare_pending_startup()
        bind_startup_state(state)
        await self.run_once(defer_requested.is_set)

    async def _pending_descriptor(self) -> AttemptDescriptorShape | None:
        pending = await asyncio.to_thread(self._staging.pending_attempts, self._device_slug)
        descriptors = cast(tuple[AttemptDescriptorShape, ...], pending)
        if len(descriptors) > 1:
            raise OpportunisticSyncError(f"multiple partial attempts block resume for {self._device_slug}")
        return descriptors[0] if descriptors else None

    async def _validate_pending_evidence(self, descriptor: AttemptDescriptorShape) -> int:
        attempt = cast(
            StagedAttemptShape,
            await asyncio.to_thread(self._staging.open_attempt, descriptor.attempt_id),
        )
        try:
            recovery = await asyncio.to_thread(attempt.recover)
        finally:
            await asyncio.to_thread(attempt.close)
        return descriptor.start_sequence + recovery.valid_records

    async def _sweep_terminal_retired(self, should_defer: Callable[[], bool]) -> None:
        try:
            removed = await asyncio.to_thread(
                self._staging.sweep_terminal_retired,
                self._device_slug,
                should_defer=should_defer,
            )
        except Exception as error:  # noqa: BLE001 - sweep failure preserves collection and evidence
            self._runtime.debug_exception("terminal_retired_sweep_failed", error, device_slug=self._device_slug)
            return
        for path in removed:
            self._runtime.debug_event(
                "terminal_retired_partial_deleted", device_slug=self._device_slug, attempt_id=path.name
            )

    async def _salvage_quarantined_prefix(self, source: Path, should_defer: Callable[[], bool]) -> bool:
        try:
            result = await asyncio.to_thread(
                self._runtime.publish_quarantined_prefix,
                source,
                self._staging,
                self._device_slug,
                should_defer=should_defer,
            )
        except Exception as error:
            kind = self._runtime.classify_quarantine_error(error)
            if kind is None:
                raise
            if kind == "deferred":
                return False
            if kind == "unprocessable":
                await self._mark_quarantine(
                    source,
                    self._staging.mark_quarantine_unprocessable,
                    "quarantine_unprocessable",
                    str(error),
                )
                return True
            await self._mark_quarantine(
                source,
                self._staging.mark_quarantine_salvage_pending,
                "quarantine_salvage_pending",
                str(error),
            )
            return True
        try:
            await asyncio.to_thread(self._staging.mark_quarantine_published, self._device_slug, source)
        except Exception as error:  # noqa: BLE001 - retain source until a later lifecycle pass
            self._runtime.debug_exception(
                "quarantine_terminal_mark_failed", error, device_slug=self._device_slug, source=source.name
            )
            return True
        publication = cast(QuarantinePublicationShape, result)
        self._runtime.debug_event(
            "quarantine_prefix_published",
            device_slug=self._device_slug,
            source=source.name,
            bundle=publication.bundle_path.name,
            deduplicated=publication.deduplicated,
        )
        await report_activity(self._activity, "prefix_published")
        return True

    async def _mark_quarantine(
        self,
        source: Path,
        marker: Callable[[str, Path, str], None],
        event: str,
        reason: str,
    ) -> None:
        try:
            await asyncio.to_thread(marker, self._device_slug, source, reason)
        except Exception as marking_error:  # noqa: BLE001 - source remains safe
            self._runtime.debug_exception(
                "quarantine_classification_failed",
                marking_error,
                device_slug=self._device_slug,
                source=source.name,
            )
        else:
            self._runtime.debug_event(event, device_slug=self._device_slug, source=source.name)

    async def _quarantine_pending(
        self,
        reason: str,
        *,
        original_error: BaseException | None = None,
    ) -> None:
        moved = await asyncio.to_thread(self._staging.quarantine_pending, self._device_slug, reason)
        if not moved:
            if original_error is not None:
                raise original_error
            raise OpportunisticSyncError(f"unable to quarantine blocking evidence for {self._device_slug}")
        await report_activity(self._activity, "evidence_quarantined")
