"""Batch reconciliation and active-batch finalization for opportunistic capture.

The reconciler owns one active batch state machine: durable checkpoints,
cursor reconciliation, explicit ADVANCE confirmation, progress reporting, and
active writer finalization. The coordinator supplies runtime ports and a
narrow discontinuity quarantine callback.
"""

from __future__ import annotations

import asyncio
import inspect
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Never, cast

from ..domain.ring_protocol import RECORD_SIZE, DoneNotification, ReadBeginNotification, RingInfo
from ..domain.transfer_arena import TransferArena
from . import collector
from .operational_telemetry import OperationalEmitter
from .ports import (
    AttemptDescriptorShape,
    BatchWriterPort,
    CaptureRuntimePort,
    DurablePrefixShape,
    SealResultShape,
    StagingPort,
)
from .quality_metrics import SequenceLossMetric, SessionQuality, utc_timestamp
from .ring_transport import RingSession
from .session_lifecycle import (
    InfoReader,
    OpportunisticOptions,
    OpportunisticSyncError,
    RetryPolicy,
    SessionPhaseState,
)
from .session_lifecycle import bounded as _bounded
from .session_lifecycle import report_activity as _report_activity
from .session_lifecycle import report_finalization_error as _report_finalization_error
from .session_lifecycle import sleep as _sleep


class CursorConsistencyError(OpportunisticSyncError):
    """The device cursor is inconsistent with the active transfer."""


class CursorRegressionError(OpportunisticSyncError):
    """The device cursor regressed before an active batch start."""


class BatchUnavailableError(OpportunisticSyncError):
    """The device no longer exposes the complete active batch."""


class ConcurrentAdvanceError(OpportunisticSyncError):
    """Another actor advanced beyond a locally sealed batch."""


@dataclass(slots=True)
class _Batch:
    info: RingInfo | None
    device_slug: str
    start: int
    end: int
    arena: TransferArena
    writer: BatchWriterPort
    durable: DurablePrefixShape | None = None
    seal: SealResultShape | None = None

    @property
    def count(self) -> int:
        return self.end - self.start


class _ReconcileRestart:
    """A preserved attempt requires a fresh INFO before collection resumes."""


@dataclass(frozen=True, slots=True)
class _PrefixPublished:
    """A durable prefix was published before collection continues at the cursor."""

    seal: SealResultShape | None


@dataclass(slots=True)
class _State:
    batch: _Batch | None = None
    last_result: collector.CollectResult | None = None
    completed_batches: int = 0
    pending_descriptor: AttemptDescriptorShape | None = None
    pending_durable_next: int | None = None


@dataclass(frozen=True, slots=True)
class _Run:
    staging: StagingPort
    device_slug: str
    options: OpportunisticOptions
    runtime: CaptureRuntimePort
    quarantine_attempt: Callable[[str], Awaitable[None]]


@dataclass(frozen=True, slots=True)
class FinalizationResult:
    """Independent writer teardown outcomes and any preserved active evidence."""

    checkpoint_error: BaseException | None
    close_error: BaseException | None
    preserved_path: Path | None = None
    preserved_kind: str | None = None


class BatchReconciler:
    """Own one active batch and its durable cursor reconciliation state."""

    def __init__(
        self,
        staging: StagingPort,
        device_slug: str,
        options: OpportunisticOptions,
        runtime: CaptureRuntimePort,
        quarantine_attempt: Callable[[str], Awaitable[None]],
    ) -> None:
        self._run = _Run(staging, device_slug, options, runtime, quarantine_attempt)
        self._state = _State()

    @property
    def pending_descriptor(self) -> AttemptDescriptorShape | None:
        return self._state.pending_descriptor

    @property
    def pending_durable_next(self) -> int | None:
        return self._state.pending_durable_next

    @property
    def completed_batches(self) -> int:
        return self._state.completed_batches

    def set_startup_state(self, pending: AttemptDescriptorShape | None, durable_next: int | None) -> None:
        self._state.pending_descriptor = pending
        self._state.pending_durable_next = durable_next

    async def connected_step(
        self,
        session: RingSession,
        current: RingInfo | None,
        info: InfoReader,
        phase: SessionPhaseState,
    ) -> tuple[str | None, RingInfo | None]:
        return await _run_connected_step(
            _CoordinatorContext(self._run, self._state), session, current, info=info, phase=phase
        )

    async def checkpoint_after_session(self) -> None:
        await _checkpoint_after_session(self._state, self._run)

    async def finalize_active(self) -> FinalizationResult:
        batch = self._state.batch
        if batch is None:
            return FinalizationResult(None, None)
        preserved_path = batch.seal.bundle_path if batch.seal is not None else self._run.staging.attempts_root
        preserved_kind = "sealed bundle" if batch.seal is not None else "partial collection"
        checkpoint_error, close_error = await _finalize_batch(batch, self._run.options, self._run.runtime)
        return FinalizationResult(checkpoint_error, close_error, preserved_path, preserved_kind)

    def drained_result(self) -> collector.CollectResult:
        return _drained_result(self._state)


@dataclass(frozen=True, slots=True)
class _CoordinatorContext:
    run: _Run
    state: _State


async def _checkpoint_after_session(state: _State, run: _Run) -> None:
    batch = state.batch
    if batch is None or batch.seal is not None or not batch.writer.progress.submitted:
        return
    # A transport-originated interruption can arrive immediately after
    # READ_BEGIN was queued.  Do not mutate the staging target to synchronize:
    # a second prepare would reset its read-begin state.  Instead wait only for
    # the writer boundary's queued begin command, under the normal transfer
    # deadline, then issue the checkpoint.
    deadline = asyncio.get_running_loop().time() + run.options.timeouts.transfer
    while True:
        try:
            await _checkpoint_batch(batch, run.options)
            if batch.durable is not None:
                state.pending_durable_next = batch.durable.next_sequence
            return
        except BaseException as error:
            if run.runtime.is_writer_failed(error):
                _raise_writer_cause(batch.writer, error)
            if not run.runtime.is_writer_error(error):
                raise
            if asyncio.get_running_loop().time() >= deadline:
                raise collector.CollectorTimeoutError("writer did not complete READ_BEGIN before checkpoint") from error
            await _sleep(run.options.sleep, run.options.config.writer.state_poll_seconds)


async def _run_connected_step(
    context: _CoordinatorContext,
    session: RingSession,
    current: RingInfo | None,
    *,
    info: InfoReader,
    phase: SessionPhaseState,
) -> tuple[str | None, RingInfo | None]:
    """Finish one in-memory batch or decide the connected session is drained."""
    if current is None:
        phase.value = "info"
        current = await info(session)
    phase.value = "read/reconcile"
    run = context.run
    state = context.state
    batch = await _ensure_batch(current, run, state)
    if batch is None:
        return "drained", None
    if batch.info is None:
        # A resumed attempt has no trusted historical INFO.  The first fresh
        # INFO observed in this presence session becomes its result metadata;
        # the device cursor below remains the sole cursor authority.
        batch.info = current

    if batch.seal is None:
        already_confirmed = await _read_and_seal(context, session, current, batch, phase)
        if already_confirmed is None:
            phase.value = "info"
            fresh = await info(session)
            return await _run_connected_step(context, session, fresh, info=info, phase=phase)
        if not run.options.policy.advance_enabled or already_confirmed:
            await _complete_batch(
                state,
                batch,
                run.options,
                advance_confirmed=run.options.policy.advance_enabled,
            )
            return ("collected" if not run.options.policy.advance_enabled else None), current
        phase.value = "advance"
        return await _advance_batch(context, session, current, batch, info=info)

    if not run.options.policy.advance_enabled:
        await _complete_batch(state, batch, run.options, advance_confirmed=False)
        return "collected", current
    phase.value = "advance"
    return await _advance_batch(context, session, current, batch, info=info)


async def _ensure_batch(current: RingInfo, run: _Run, state: _State) -> _Batch | None:
    batch = state.batch
    if batch is not None:
        return batch
    pending = state.pending_descriptor
    if pending is not None and _pending_incompatible(current, pending, state.pending_durable_next):
        await run.quarantine_attempt(pending.attempt_id)
        state.pending_durable_next = None
        state.pending_descriptor = None
        pending = None
    if current.unread_packets == 0 and pending is None:
        if state.last_result is None:
            state.last_result = collector.NoDataResult(current)
        return None
    start, end = _batch_bounds(current, run.options.policy, pending, state.pending_durable_next)
    batch = await _admit_batch(current, run, start, end, state.pending_durable_next)
    state.batch = batch
    state.pending_descriptor = pending
    return batch


def _pending_incompatible(current: RingInfo, pending: AttemptDescriptorShape, durable_next: int | None) -> bool:
    end = pending.start_sequence + pending.packet_count
    cursor_ahead = (
        durable_next is not None
        and durable_next < current.read_sequence <= current.write_sequence
        and end <= current.write_sequence
    )
    return (
        current.read_sequence < pending.start_sequence
        or (current.read_sequence > end and not cursor_ahead)
        or current.write_sequence < end
        or (current.unread_packets == 0 and current.read_sequence != end and not cursor_ahead)
    )


def _batch_bounds(
    current: RingInfo,
    policy: RetryPolicy,
    pending: AttemptDescriptorShape | None,
    durable_next: int | None,
) -> tuple[int, int]:
    if pending is None:
        start = current.read_sequence
        return start, min(current.write_sequence, start + policy.batch_records)
    start = pending.start_sequence
    end = start + pending.packet_count
    if current.read_sequence < start:
        raise CursorRegressionError(f"device cursor {current.read_sequence} regressed before batch start {start}")
    if current.read_sequence > end and not (
        durable_next is not None
        and durable_next < current.read_sequence <= current.write_sequence
        and end <= current.write_sequence
    ):
        raise ConcurrentAdvanceError(f"device cursor {current.read_sequence} passed batch end {end}")
    if current.write_sequence < end:
        raise BatchUnavailableError("device write sequence fell below the bounded batch end")
    cursor_ahead = (
        durable_next is not None
        and durable_next < current.read_sequence <= current.write_sequence
        and end <= current.write_sequence
    )
    if current.unread_packets == 0 and current.read_sequence != end and not cursor_ahead:
        raise CursorConsistencyError("pending batch is not at its complete device end")
    return start, end


async def _admit_batch(  # noqa: C901 - admission and writer cleanup preserve ordering invariants
    current: RingInfo, run: _Run, start: int, end: int, durable_next: int | None
) -> _Batch:
    arena_start = current.read_sequence
    if durable_next is not None and current.read_sequence <= durable_next:
        arena_start = durable_next
    elif current.read_sequence > end:
        # A restarted partial whose bounded range was overtaken needs no arena
        # bytes: publication consumes only its durable prefix.  The next fresh
        # INFO creates the real C..W arena after the old attempt is retired.
        arena_start = end
    arena_count = end - arena_start
    if arena_count < 0:
        raise CursorRegressionError("device cursor is outside the pending batch")
    arena = TransferArena(arena_start, arena_count, max_bytes=run.options.policy.arena_max_bytes)
    writer = run.runtime.make_batch_writer(
        run.staging,
        run.device_slug,
        start,
        end - start,
        source_start=arena_start,
        source=arena.readonly_source(),
        config=run.options.config.writer,
    )
    try:
        await writer.start()
        prepared = await writer.prepare_leg(start, end - start)
        if not _is_durable_prefix(prepared):
            raise CursorConsistencyError("staging writer returned an invalid durable prefix")
        prepared_prefix = cast(DurablePrefixShape, prepared)
        if durable_next is not None and prepared_prefix.next_sequence != durable_next:
            raise CursorConsistencyError("prepared durable prefix changed before recovery")
    except BaseException as error:
        if run.runtime.is_writer_failed(error):
            try:
                await writer.close(timeout=run.options.timeouts.transfer)
            except BaseException as close_error:
                if not (run.runtime.is_writer_error(close_error) or isinstance(close_error, asyncio.TimeoutError)):
                    raise
            _raise_writer_cause(writer, error)
        try:
            await writer.close(timeout=run.options.timeouts.transfer)
        except BaseException as close_error:
            if not (run.runtime.is_writer_error(close_error) or isinstance(close_error, asyncio.TimeoutError)):
                raise
        raise
    return _Batch(current, run.device_slug, start, end, arena, writer, prepared_prefix)


async def _read_and_seal(
    context: _CoordinatorContext,
    session: RingSession,
    current: RingInfo,
    batch: _Batch,
    phase: SessionPhaseState,
) -> bool | None:
    run = context.run
    reconciliation = await _read_or_reconcile(session, current, batch, run, phase)
    if isinstance(reconciliation, _PrefixPublished):
        await _continue_after_prefix(context.state, batch, reconciliation, run.staging, run.options)
        return None
    if isinstance(reconciliation, _ReconcileRestart):
        _continue_after_reconcile_restart(context.state)
        return None
    prefix = batch.durable
    if prefix is None:
        raise CursorConsistencyError("writer checkpoint did not return a durable prefix")
    if prefix.next_sequence != batch.end:
        raise CursorConsistencyError("READ did not produce the bounded batch prefix")
    await _report_activity(run.options.activity, "sealing")
    try:
        sealed = await _bounded(batch.writer.seal(DoneNotification(0, batch.end)), run.options.timeouts.transfer)
    except BaseException as error:
        if run.runtime.is_writer_failed(error):
            _raise_writer_cause(batch.writer, error)
        raise
    if not _is_seal_result(sealed):
        raise CursorConsistencyError("staging writer returned an invalid seal result")
    batch.seal = cast(SealResultShape, sealed)
    return current.read_sequence == batch.end


async def _advance_batch(
    context: _CoordinatorContext,
    session: RingSession,
    current: RingInfo,
    batch: _Batch,
    *,
    info: InfoReader,
) -> tuple[str | None, RingInfo | None]:
    """Confirm or idempotently repeat ADVANCE after a sealed batch."""
    run = context.run
    state = context.state
    assert batch.seal is not None
    # Never make an ADVANCE decision from the INFO that admitted the batch.
    # The writer has sealed first; this INFO is the sole cursor observation for
    # the destructive command.
    current = await info(session)
    await _report_activity(run.options.activity, "advancing")
    action = _advance_action(current, batch)
    if action == "confirmed":
        await _complete_batch(state, batch, run.options)
        return None, current
    if action in ("ahead", "regressed", "expired"):
        # The immutable bundle is already safe.  Do not issue an old ADVANCE
        # when fresh INFO proves another actor/firmware state has moved on;
        # release the lease and let the next step start at the live cursor.
        await _complete_batch(state, batch, run.options, advance_confirmed=False)
        return None, current
    await _bounded(
        collector.advance_leg(session, batch.end, timeout=run.options.timeouts.info), run.options.timeouts.info
    )
    confirmed = await info(session)
    action = _advance_action(confirmed, batch)
    if action == "confirmed":
        await _complete_batch(state, batch, run.options)
        return None, confirmed
    if action in ("ahead", "regressed", "expired"):
        # The acknowledgement was uncertain, but fresh INFO proves this
        # sealed bundle is no longer the live cursor range.  Retire the
        # immutable bundle without repeating its old ADVANCE.
        await _complete_batch(state, batch, run.options, advance_confirmed=False)
        return None, confirmed
    # Cursor below end means ADVANCE was not applied. The next connected step
    # does another fresh INFO and repeats only this idempotent command.
    return None, confirmed


async def _read_or_reconcile(
    session: RingSession,
    current: RingInfo,
    batch: _Batch,
    run: _Run,
    phase: SessionPhaseState,
) -> _PrefixPublished | _ReconcileRestart | None:
    options = run.options
    prefix = batch.durable
    if prefix is None:
        raise CursorConsistencyError("batch has no prepared durable prefix")
    cursor = current.read_sequence
    # A cursor beyond INFO's write watermark is not a recoverable ring state;
    # keep the active evidence and reject it rather than guessing.
    if cursor > current.write_sequence:
        raise CursorConsistencyError(f"device cursor {cursor} passed write sequence {current.write_sequence}")
    if cursor < batch.start:
        await _quarantine_discontinuous_attempt(run, batch)
        return _ReconcileRestart()
    if current.write_sequence < batch.end:
        await _quarantine_discontinuous_attempt(run, batch)
        return _ReconcileRestart()
    if cursor > prefix.next_sequence:
        return await _publish_cursor_ahead(current, batch, run, prefix.next_sequence, phase.quality)
    if cursor == batch.end:
        # A restarted, fully checkpointed attempt has a persisted original
        # READ_BEGIN but no live writer command yet.  Rebind it through the
        # adapter so seal remains a writer-thread-only staging operation.
        await _rebind_durable_batch(batch, options)
        return None
    if prefix.next_sequence == batch.end:
        await _rebind_durable_batch(batch, options)
        return None
    # A prior process may have sealed/persisted all bytes before ADVANCE was
    # observed. Fresh INFO still governs; replaying the exact range is
    # duplicate-safe and prevents a blind disk-journal ADVANCE.
    await _report_activity(options.activity, "reading")
    read_start = max(cursor, prefix.next_sequence)
    await _bounded(
        batch.writer.prepare_leg(read_start, batch.end - read_start),
        options.timeouts.transfer,
    )
    await _read_leg_with_progress(session, batch, read_start, options, phase.quality)
    prefix = await _checkpoint_batch(batch, options)
    if current.read_sequence > prefix.next_sequence:
        raise CursorConsistencyError("device cursor passed the writer checkpoint")
    return None


async def _read_leg_with_progress(
    session: RingSession,
    batch: _Batch,
    read_start: int,
    options: OpportunisticOptions,
    quality: SessionQuality | None,
) -> collector.ReadLegResult:
    """Run one READ beside a coalescing, best-effort progress pump."""
    mailbox = collector.ProgressMailbox()
    read_options = collector.ReadLegOptions(
        options.timeouts.transfer,
        cleanup_timeout=options.timeouts.info,
        progress_mailbox=mailbox,
    )
    requested_records = batch.end - read_start
    started_at = options.clock()
    try:
        if options.progress is None:
            result = await collector.read_leg(
                session,
                batch.arena,
                batch.writer,
                read_start,
                requested_records,
                read_options,
            )
        else:
            read = asyncio.create_task(
                collector.read_leg(session, batch.arena, batch.writer, read_start, requested_records, read_options)
            )
            pump = asyncio.create_task(
                _pump_progress(mailbox, options.progress, options.config.transfer.progress_interval_seconds)
            )
            try:
                result = await read
            except BaseException:
                pump.cancel()
                await asyncio.gather(pump, return_exceptions=True)
                raise
            await pump
    except collector.TransferInterruptedError as error:
        _note_quality_counters(quality, error.counters)
        raise
    else:
        _note_quality_counters(quality, result.counters)
        return result
    finally:
        if quality is not None:
            quality.note_read(options.clock() - started_at, requested_records)


async def _pump_progress(
    mailbox: collector.ProgressMailbox,
    callback: collector.ProgressCallback,
    interval: float,
) -> None:
    """Deliver latest snapshots serially; a terminal snapshot bypasses cadence."""
    revision = 0
    last_reported_at: float | None = None
    loop = asyncio.get_running_loop()
    while True:
        snapshot = await mailbox.wait_for_change(revision)
        if not snapshot.terminal and last_reported_at is not None:
            deadline = last_reported_at + interval
            while True:
                remaining = deadline - loop.time()
                if remaining <= 0:
                    snapshot = mailbox.snapshot
                    break
                try:
                    snapshot = await asyncio.wait_for(mailbox.wait_for_change(snapshot.revision), remaining)
                except TimeoutError:
                    snapshot = mailbox.snapshot
                    break
                if snapshot.terminal:
                    break
        if snapshot.event is not None:
            await _report_progress_safely(callback, snapshot.event)
            last_reported_at = loop.time()
        revision = snapshot.revision
        if snapshot.terminal:
            return


async def _report_progress_safely(callback: collector.ProgressCallback, event: collector.ProgressEvent) -> None:
    try:
        result = callback(event)
        if inspect.isawaitable(result):
            await result
    except Exception:  # noqa: BLE001 - progress is intentionally best effort
        return


async def _rebind_durable_batch(batch: _Batch, options: OpportunisticOptions) -> None:
    """Rebind a fully durable attempt before sealing it in this process."""
    await _bounded(batch.writer.read_begin(ReadBeginNotification(batch.start, batch.count)), options.timeouts.transfer)


async def _publish_cursor_ahead(
    current: RingInfo,
    batch: _Batch,
    run: _Run,
    durable_next: int,
    quality: SessionQuality | None,
) -> _PrefixPublished:
    options = run.options
    cursor = current.read_sequence
    if not durable_next < cursor <= current.write_sequence or batch.end > current.write_sequence:
        raise CursorConsistencyError(f"device cursor {cursor} passed durable prefix {durable_next}")
    # No READ is legal after the device cursor passed the durable prefix.
    # Activate the persisted original READ_BEGIN only so the writer can
    # durably publish its checkpoint-authenticated prefix on its own thread.
    await _rebind_durable_batch(batch, options)
    published = await _bounded(batch.writer.publish_prefix(), options.timeouts.transfer)
    if published is not None and not _is_seal_result(published):
        raise CursorConsistencyError("staging writer returned an invalid prefix publication")
    await _report_loss_detected(
        options,
        {
            "event": "loss_detected",
            "missing_record_count": cursor - durable_next,
            "missing_raw_bytes": (cursor - durable_next) * RECORD_SIZE,
            "reason": "device cursor advanced before host durable prefix",
        },
    )
    if quality is not None and options.quality_metrics is not None:
        try:
            options.quality_metrics.record_sequence_loss(
                SequenceLossMetric(
                    utc_timestamp(options.host_time()),
                    quality.session_id,
                    batch.device_slug,
                    cursor - durable_next,
                    (cursor - durable_next) * RECORD_SIZE,
                    "device_cursor_advanced_before_host_durable_prefix",
                    options.quality_metrics.release_version,
                    options.quality_metrics.source_revision,
                    quality.firmware_version,
                )
            )
        except Exception as error:  # noqa: BLE001 - metrics cannot stop audio capture
            run.runtime.debug_exception("quality_metrics_write_error", error, event_type="sequence_loss")
    return _PrefixPublished(cast(SealResultShape, published) if published is not None else None)


def _note_quality_counters(quality: SessionQuality | None, counters: collector.TransferCounters) -> None:
    if quality is not None:
        quality.note_counters(counters.received_bytes, counters.submitted_bytes, counters.written_bytes)


async def _report_loss_detected(options: OpportunisticOptions, event: dict[str, object]) -> None:
    """Publish one loss diagnostic without making collection depend on observers."""
    if options.operational is None:
        return
    try:
        await _bounded(
            _invoke_operational(options.operational, event),
            min(options.timeouts.transfer, options.config.retry.presence_preflight_budget_seconds),
        )
    except asyncio.CancelledError:
        raise
    except Exception:  # noqa: BLE001 - operational telemetry is best effort
        return


async def _invoke_operational(emitter: OperationalEmitter, event: dict[str, object]) -> None:
    result = emitter(event)
    if inspect.isawaitable(result):
        await result


async def _checkpoint_batch(batch: _Batch, options: OpportunisticOptions) -> DurablePrefixShape:
    result = await _bounded(batch.writer.checkpoint(), options.timeouts.transfer)
    if not _is_durable_prefix(result):
        raise CursorConsistencyError("writer checkpoint did not return a durable prefix")
    durable = cast(DurablePrefixShape, result)
    batch.durable = durable
    return durable


async def _finalize_batch(
    batch: _Batch, options: OpportunisticOptions, runtime: CaptureRuntimePort
) -> tuple[BaseException | None, BaseException | None]:
    """Attempt checkpoint and close independently during coordinator teardown.

    A blocked checkpoint must not prevent the close command from being queued.
    The writer owns the lease until that close command completes and its thread
    joins, so failures remain observable without claiming the batch was closed.
    """
    checkpoint_error: BaseException | None = None
    close_error: BaseException | None = None
    try:
        if batch.seal is None and batch.writer.progress.submitted:
            try:
                await _checkpoint_for_finalization(batch, options, runtime)
            except (OSError, collector.CollectorTimeoutError) as error:
                checkpoint_error = error
                await _report_finalization_error(options.activity, "checkpoint", error, runtime)
            except Exception as error:
                if not (runtime.is_staging_error(error) or runtime.is_writer_error(error)):
                    raise
                checkpoint_error = error
                await _report_finalization_error(options.activity, "checkpoint", error, runtime)
    finally:
        if batch.writer.thread.is_alive():
            try:
                await _bounded(batch.writer.close(timeout=options.timeouts.transfer), options.timeouts.transfer)
            except (OSError, collector.CollectorTimeoutError) as error:
                close_error = error
                await _report_finalization_error(options.activity, "close", error, runtime)
            except Exception as error:
                if not (runtime.is_staging_error(error) or runtime.is_writer_error(error)):
                    raise
                close_error = error
                await _report_finalization_error(options.activity, "close", error, runtime)
    return checkpoint_error, close_error


async def _checkpoint_for_finalization(
    batch: _Batch, options: OpportunisticOptions, runtime: CaptureRuntimePort
) -> DurablePrefixShape:
    """Wait briefly for queued READ_BEGIN before the final durability barrier."""
    deadline = asyncio.get_running_loop().time() + options.timeouts.transfer
    while True:
        try:
            return await _checkpoint_batch(batch, options)
        except BaseException as error:
            if runtime.is_writer_failed(error):
                raise
            if not runtime.is_writer_error(error):
                raise
            if asyncio.get_running_loop().time() >= deadline:
                raise collector.CollectorTimeoutError(
                    "writer did not complete READ_BEGIN before final checkpoint"
                ) from error
            await _sleep(options.sleep, options.config.writer.state_poll_seconds)


async def _continue_after_prefix(
    state: _State,
    batch: _Batch,
    publication: _PrefixPublished,
    staging: StagingPort,
    options: OpportunisticOptions,
) -> None:
    """Retire a prefix-publication attempt while keeping the session available for READ."""
    prefix = batch.durable
    if prefix is None:
        raise CursorConsistencyError("cursor-ahead state has no writer checkpoint")
    assert batch.info is not None
    # Stock firmware advances its cursor from BLE TX-confirmed packets before
    # the host can persist every notification:
    # https://github.com/BasedHardware/omi/blob/6f7c57ac1545c1931c806a01605646405d398198/omi/firmware/omi/src/lib/core/storage.c#L245-L253
    # https://github.com/BasedHardware/omi/blob/6f7c57ac1545c1931c806a01605646405d398198/omi/firmware/omi/src/lib/core/storage.c#L407-L433
    # The missing range is not recoverable, but the remaining ring is still
    # valuable. Never ADVANCE this batch: close it to release its lease, then
    # let the next fresh INFO start a new batch at the device cursor.
    await _report_activity(options.activity, "prefix_published" if publication.seal else "prefix_retired")
    if publication.seal is not None:
        state.last_result = collector.CollectionResult(
            batch.info, prefix.record_count, publication.seal, prefix.next_sequence, False
        )
    attempt_id = batch.writer.attempt_id
    await _bounded(batch.writer.close(timeout=options.timeouts.transfer), options.timeouts.transfer)
    await asyncio.to_thread(staging.terminalize_prefix_attempt, batch.device_slug, attempt_id)
    state.batch = None
    state.pending_descriptor = None
    state.pending_durable_next = None


async def _quarantine_discontinuous_attempt(
    run: _Run,
    batch: _Batch,
) -> None:
    """Release and quarantine a discontinuous attempt without diagnostic metadata."""
    attempt_id = batch.writer.attempt_id
    await _bounded(batch.writer.close(timeout=run.options.timeouts.transfer), run.options.timeouts.transfer)
    await run.quarantine_attempt(attempt_id)


def _continue_after_reconcile_restart(state: _State) -> None:
    """Retire preserved evidence; the caller immediately obtains INFO again."""
    state.batch = None
    state.pending_descriptor = None
    state.pending_durable_next = None
    # This is diagnostic lifecycle state, not confirmed packet loss.
    # The next connected step obtains a fresh INFO and starts at its cursor.


def _advance_action(current: RingInfo, batch: _Batch) -> str:
    if current.read_sequence > current.write_sequence:
        raise ConcurrentAdvanceError("device cursor passed its write sequence")
    if current.write_sequence < batch.end:
        return "expired"
    cursor = current.read_sequence
    if cursor < batch.start:
        return "regressed"
    if cursor == batch.end:
        return "confirmed"
    if cursor > batch.end:
        return "ahead"
    return "repeat"


async def _complete_batch(
    state: _State, batch: _Batch, options: OpportunisticOptions, *, advance_confirmed: bool = True
) -> None:
    assert batch.seal is not None
    assert batch.info is not None
    state.last_result = collector.CollectionResult(batch.info, batch.count, batch.seal, batch.end, advance_confirmed)
    await _bounded(batch.writer.close(timeout=options.timeouts.transfer), options.timeouts.transfer)
    state.batch = None
    state.pending_descriptor = None
    state.pending_durable_next = None
    state.completed_batches += 1


def _drained_result(state: _State) -> collector.CollectResult:
    if state.last_result is None:
        raise RuntimeError("drained collection has no INFO result")
    return state.last_result


def _is_durable_prefix(value: object) -> bool:
    return all(hasattr(value, field) for field in ("start_sequence", "next_sequence", "record_count", "raw_sha256"))


def _is_seal_result(value: object) -> bool:
    return hasattr(value, "bundle_path") and hasattr(value, "deduplicated")


def _raise_writer_cause(writer: BatchWriterPort, error: BaseException) -> Never:
    """Preserve a concrete staging failure from the writer boundary."""
    if writer.failure is not None:
        raise writer.failure
    raise error
