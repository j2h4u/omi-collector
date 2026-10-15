"""Pure BLE ingest for bounded Omi ring transfers.

The notification loop owns protocol validation and a preallocated
``TransferArena``.  It never waits for a target writer, touches a path, or
invokes a progress callback.  Complete-record high-water marks are published
to an ``AttemptWriter`` synchronously; the writer's barrier is awaited only
after DONE or an ingest error has made the transfer terminal.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import suppress
from dataclasses import dataclass
from typing import Protocol, cast

from ..domain.ring_protocol import (
    NOTIFY_ACK,
    NOTIFY_DATA,
    NOTIFY_DONE,
    NOTIFY_INFO,
    NOTIFY_READ_BEGIN,
    RECORD_SIZE,
    STATUS_STORAGE_NOT_READY,
    AckNotification,
    DoneNotification,
    ReadBeginNotification,
    RingInfo,
    RingProtocolError,
    RingStatus,
    encode_advance_command,
    encode_info_command,
    encode_read_command,
    encode_stop_command,
    parse_ack_notification,
    parse_data_notification,
    parse_done_notification,
    parse_info_notification,
    parse_read_begin_notification,
)
from ..domain.transfer_arena import TransferArena
from . import batch_machine
from .owned_work import join_owned
from .ring_transport import RingSession, RingTransportDisconnectedError, RingTransportError
from .stop_intent import StopIntent

_OPTIONAL_OPTIONS_ARG_COUNT = 3


class CollectorError(RuntimeError):
    """Base error for a collector state-machine failure."""


class _WriterIngestError(CollectorError):
    """The target failed while a physical READ still owns the BLE session."""

    def __init__(self, failure: BaseException) -> None:
        self.failure = failure
        super().__init__("writer failed during READ")


class CollectorTimeoutError(CollectorError, TimeoutError):
    """A required notification did not arrive before its deadline."""


class RingAcknowledgementError(CollectorError):
    """The pendant rejected a command."""

    def __init__(self, status: int, *, command: str = "ring command") -> None:
        self.status = status
        self.command = command
        super().__init__(f"{command} was rejected with status {status}")


class RingTransferError(CollectorError):
    """The notification stream violated the bounded transfer contract."""


class RingAdvanceError(CollectorError):
    """An ADVANCE command was not confirmed."""


class AdvanceRejectedError(RingAdvanceError):
    """ADVANCE was deterministically rejected or malformed."""


class AdvanceUncertainError(RingAdvanceError):
    """ADVANCE may have reached the device but its outcome is unknown."""


@dataclass(frozen=True, slots=True)
class TransferCounters:
    """O(1) counters safe to report after an interrupted ingest."""

    received_bytes: int
    submitted_bytes: int
    written_bytes: int

    @property
    def received_records(self) -> int:
        return self.received_bytes // RECORD_SIZE

    @property
    def submitted_records(self) -> int:
        return self.submitted_bytes // RECORD_SIZE

    @property
    def written_records(self) -> int:
        return self.written_bytes // RECORD_SIZE


class TransferInterruptedError(RingTransferError):
    """Ingest stopped before a valid DONE, with counters attached."""

    def __init__(self, message: str, counters: TransferCounters, *, cause: BaseException | None = None) -> None:
        self.counters = counters
        self.received_bytes = counters.received_bytes
        self.submitted_bytes = counters.submitted_bytes
        self.written_bytes = counters.written_bytes
        self.received_records = counters.received_records
        self.submitted_records = counters.submitted_records
        self.written_records = counters.written_records
        super().__init__(message)
        if cause is not None:
            self.__cause__ = cause


@dataclass(frozen=True, slots=True)
class NoDataResult:
    info: RingInfo


@dataclass(frozen=True, slots=True)
class CollectionResult:
    info: RingInfo
    packet_count: int
    seal: object
    next_sequence: int | None = None
    advance_confirmed: bool = False


type CollectResult = NoDataResult | CollectionResult


@dataclass(frozen=True, slots=True)
class TransferTimeouts:
    info: float
    transfer: float


@dataclass(frozen=True, slots=True)
class ProgressEvent:
    records_completed: int
    records_total: int
    bytes_completed: int
    elapsed: float
    records_per_second: float
    bytes_per_second: float
    eta: float | None


@dataclass(frozen=True, slots=True)
class ProgressSnapshot:
    """One latest-value progress sample for a single READ."""

    event: ProgressEvent | None
    revision: int
    terminal: bool


class ProgressMailbox:
    """Event-loop-local, latest-value progress handoff owned by one READ."""

    def __init__(self) -> None:
        self._changed = asyncio.Event()
        self._snapshot = ProgressSnapshot(None, 0, False)

    @property
    def snapshot(self) -> ProgressSnapshot:
        return self._snapshot

    def publish(self, event: ProgressEvent, *, terminal: bool = False) -> None:
        """Synchronously replace the latest immutable sample."""
        current = self._snapshot
        if current.terminal:
            return
        self._snapshot = ProgressSnapshot(event, current.revision + 1, terminal)
        self._changed.set()

    def finish(self) -> None:
        """Mark an interrupted READ terminal without inventing progress."""
        current = self._snapshot
        if current.terminal:
            return
        self._snapshot = ProgressSnapshot(current.event, current.revision + 1, True)
        self._changed.set()

    async def wait_for_change(self, revision: int) -> ProgressSnapshot:
        while self._snapshot.revision == revision:
            self._changed.clear()
            if self._snapshot.revision != revision:
                break
            await self._changed.wait()
        return self._snapshot


ProgressCallback = Callable[[ProgressEvent], object]


@dataclass(frozen=True, slots=True)
class ReadLegOptions:
    timeout: float
    progress: ProgressCallback | None = None
    cleanup_timeout: float | None = None
    progress_mailbox: ProgressMailbox | None = None
    stop_intent: StopIntent | None = None


@dataclass(frozen=True, slots=True)
class ReadLegResult:
    start_sequence: int
    next_sequence: int
    packet_count: int
    received_bytes: int = 0
    submitted_bytes: int = 0
    written_bytes: int = 0
    progress: ProgressEvent | None = None

    @property
    def counters(self) -> TransferCounters:
        return TransferCounters(self.received_bytes, self.submitted_bytes, self.written_bytes)


class IngestWriter(Protocol):
    """Minimum nonblocking/control surface consumed by the BLE half."""

    @property
    def submitted_high_water(self) -> int: ...

    @property
    def written_high_water(self) -> int: ...

    @property
    def failure(self) -> BaseException | None: ...

    async def wait_failure(self) -> BaseException: ...

    async def start(self) -> object: ...

    async def prepare_leg(self, start_sequence: int, record_count: int) -> object: ...

    def submit_read_begin(self, notice: ReadBeginNotification) -> Awaitable[object] | object: ...

    def publish(self, high_water: int, tail: bytes = b"") -> object: ...

    async def barrier(self) -> object: ...

    async def interrupt(self) -> object: ...


@dataclass(frozen=True, slots=True)
class _LegBaselines:
    """Lifetime watermarks captured before one physical READ leg."""

    received_bytes: int
    submitted_bytes: int
    written_bytes: int


@dataclass(slots=True)
class _IngestState:
    start: int
    count: int
    began_at: float
    progress_mailbox: ProgressMailbox
    initial_received_records: int
    baselines: _LegBaselines
    read_begin_future: asyncio.Future[object] | None = None
    phase: batch_machine.ReadPhase = batch_machine.ReadPhase.AWAIT_BEGIN
    latest_progress: ProgressEvent | None = None
    failure_message: str = "READ interrupted"
    failure_cause: BaseException | None = None


async def probe(session: RingSession) -> RingStatus:
    return await session.read_status()


async def ring_info(session: RingSession, *, timeout: float, stop_intent: StopIntent | None = None) -> RingInfo:
    _require_timeout(timeout)
    stream = session.notifications()
    if stop_intent is not None:
        stop_intent.check()
    await session.write_control(encode_info_command())
    try:
        async with asyncio.timeout(timeout):
            while True:
                payload = await anext(stream)
                opcode = _opcode(payload)
                if opcode == NOTIFY_ACK:
                    _require_ok_ack(parse_ack_notification(payload), command="INFO")
                elif opcode == NOTIFY_INFO:
                    info = parse_info_notification(payload)
                    _validate_info(info)
                    return info
                else:
                    raise RingTransferError("unexpected notification while waiting for INFO")
    except TimeoutError as error:
        raise CollectorTimeoutError("timed out waiting for INFO notification") from error
    except RingProtocolError as error:
        raise RingTransferError("received a malformed INFO notification") from error


async def read_leg(
    session: RingSession,
    arena: TransferArena,
    writer: IngestWriter,
    *leg_args: object,
    options: ReadLegOptions | None = None,
    **leg_keywords: object,
) -> ReadLegResult:
    """Ingest one exact READ range without awaiting disk during DATA."""
    start, count, options = _parse_read_leg_call(leg_args, leg_keywords, options)
    _require_positive(count, "count")
    _require_timeout(options.timeout)
    baselines = _capture_leg_baselines(arena, writer)
    arena.begin_leg(start, count)

    # Preparation is deliberately before issuing READ.  Once READ is issued,
    # only BLE notification/control awaits are permitted until terminal DONE.
    if options.stop_intent is not None:
        options.stop_intent.check()
    await writer.start()
    if options.stop_intent is not None:
        options.stop_intent.check()
    await writer.prepare_leg(start, count)
    stream = session.notifications()
    state = _IngestState(
        start,
        count,
        time.monotonic(),
        options.progress_mailbox or ProgressMailbox(),
        arena.received_records,
        baselines,
    )
    try:
        if options.stop_intent is not None:
            options.stop_intent.check()
        await _await_with_timeout(session.write_control(encode_read_command(start, count)), options.timeout)
        await _ingest_notifications(stream, state, arena, writer, options.timeout)
    except TimeoutError as error:
        state.failure_message = "timed out during READ"
        state.failure_cause = error
        state.progress_mailbox.finish()
        interrupted = await _interrupt(session, state, arena, writer, options)
        raise interrupted from interrupted.__cause__
    except _WriterIngestError as error:
        state.progress_mailbox.finish()
        state.failure_cause = error.failure
        raise await _interrupt_failed_writer(session, state, arena, writer, options) from error.failure
    except asyncio.CancelledError:
        state.progress_mailbox.finish()
        await _handle_read_cancelled(session, state, arena, writer, options)
        raise
    except (RingProtocolError, RingTransferError, RingTransportError, MemoryError, ValueError, OSError) as error:
        state.failure_message = str(error)
        state.failure_cause = error
        state.progress_mailbox.finish()
        interrupted = await _interrupt(session, state, arena, writer, options)
        raise interrupted from interrupted.__cause__

    if state.phase is not batch_machine.ReadPhase.DONE:  # defensive; only validated DONE exits the loop
        raise RingTransferError("READ ended without terminal DONE")
    # These waits are intentionally after DONE.  READ_BEGIN remains ordered
    # ahead of the barrier in the writer's command queue.
    await _finish_read(state, arena, writer)
    counters = _leg_counters(arena, writer, baselines)
    return ReadLegResult(
        start,
        start + count,
        count,
        received_bytes=counters.received_bytes,
        submitted_bytes=counters.submitted_bytes,
        written_bytes=counters.written_bytes,
        progress=state.latest_progress,
    )


async def _ingest_notifications(
    stream: object,
    state: _IngestState,
    arena: TransferArena,
    writer: IngestWriter,
    timeout: float,
) -> None:
    notifications = cast(AsyncIterator[bytes], stream)
    loop = asyncio.get_running_loop()
    failure_wait = asyncio.create_task(writer.wait_failure())
    try:
        async with asyncio.timeout(timeout) as deadline:
            while True:
                payload = await _next_notification_or_failure(notifications, failure_wait)
                deadline.reschedule(loop.time() + timeout)
                selected = batch_machine.decide_read_event((), writer_failed=writer.failure is not None)
                if selected is batch_machine.ReadEvent.WRITER_FAILED:
                    assert writer.failure is not None
                    raise _WriterIngestError(writer.failure)
                event = _read_event_for_opcode(_opcode(payload))
                selected = batch_machine.decide_read_event((event,), writer_failed=writer.failure is not None)
                if selected is batch_machine.ReadEvent.WRITER_FAILED:
                    assert writer.failure is not None
                    raise _WriterIngestError(writer.failure)
                try:
                    if _consume_notification(payload, event, state, arena, writer):
                        selected = batch_machine.decide_read_event((event,), writer_failed=writer.failure is not None)
                        if selected is batch_machine.ReadEvent.WRITER_FAILED:
                            assert writer.failure is not None
                            raise _WriterIngestError(writer.failure)
                        return
                except Exception as error:
                    if writer.failure is not None and not isinstance(error, _WriterIngestError):
                        raise _WriterIngestError(writer.failure) from error
                    raise
    finally:
        await _cancel_and_join(failure_wait)


async def _next_notification_or_failure(
    notifications: AsyncIterator[bytes], failure_wait: asyncio.Task[BaseException]
) -> bytes:
    notification = asyncio.ensure_future(anext(notifications))
    try:
        done, _pending = await asyncio.wait((notification, failure_wait), return_when=asyncio.FIRST_COMPLETED)
        if failure_wait in done:
            raise _WriterIngestError(failure_wait.result())
        return notification.result()
    except asyncio.CancelledError:
        if failure_wait.done() and not failure_wait.cancelled():
            raise _WriterIngestError(failure_wait.result()) from None
        raise
    finally:
        if not notification.done():
            await _cancel_and_join(notification)


async def _cancel_and_join[T](task: asyncio.Future[T]) -> None:
    if task.done():
        with suppress(BaseException):
            task.result()
        return
    task.cancel()
    await join_owned(asyncio.gather(task, return_exceptions=True))


def _parse_read_leg_call(
    leg_args: tuple[object, ...], leg_keywords: dict[str, object], options: ReadLegOptions | None
) -> tuple[int, int, ReadLegOptions]:
    if leg_keywords:
        if leg_args or set(leg_keywords) != {"start", "count"}:
            raise TypeError("read_leg expects start and count exactly once")
        leg_args = (leg_keywords["start"], leg_keywords["count"])
    return _parse_leg_args(leg_args, options)


def _parse_leg_args(leg_args: tuple[object, ...], options: ReadLegOptions | None) -> tuple[int, int, ReadLegOptions]:
    if len(leg_args) not in (2, _OPTIONAL_OPTIONS_ARG_COUNT):
        raise TypeError("read_leg expects start, count, and optional ReadLegOptions")
    start = cast(int, leg_args[0])
    count = cast(int, leg_args[1])
    if len(leg_args) == _OPTIONAL_OPTIONS_ARG_COUNT:
        if options is not None or not isinstance(leg_args[2], ReadLegOptions):
            raise TypeError("third positional read_leg argument must be ReadLegOptions")
        options = leg_args[2]
    if options is None:
        raise TypeError("read_leg requires ReadLegOptions")
    return start, count, options


async def report_progress(result: ReadLegResult, callback: ProgressCallback) -> None:
    """Report completed ingest progress outside the BLE notification loop."""
    if result.progress is None:
        return
    callback_result = callback(result.progress)
    if isinstance(callback_result, Awaitable):
        await callback_result


async def advance_leg(
    session: RingSession, next_sequence: int, *, timeout: float, stop_intent: StopIntent | None = None
) -> None:
    """Issue one ADVANCE, distinguishing rejection from unknown outcome."""
    _require_timeout(timeout)
    if isinstance(next_sequence, bool) or not isinstance(next_sequence, int):
        raise TypeError("next_sequence must be an integer")
    try:
        stream = session.notifications()
        if stop_intent is not None:
            stop_intent.check()
        await session.write_control(encode_advance_command(next_sequence))
        async with asyncio.timeout(timeout):
            payload = await anext(stream)
        if _opcode(payload) != NOTIFY_ACK:
            raise AdvanceRejectedError("unexpected notification while waiting for ADVANCE acknowledgement")
        _require_ok_ack(parse_ack_notification(payload), command="ADVANCE")
    except asyncio.CancelledError:
        raise
    except (TimeoutError, RingTransportDisconnectedError, ConnectionError) as error:
        raise AdvanceUncertainError("ADVANCE outcome is unknown") from error
    except RingAcknowledgementError as error:
        if error.status == STATUS_STORAGE_NOT_READY:
            raise AdvanceUncertainError("ADVANCE is temporarily unavailable") from error
        raise AdvanceRejectedError("ADVANCE was rejected") from error
    except RingProtocolError as error:
        raise AdvanceRejectedError("ADVANCE was rejected") from error


def _consume_notification(
    payload: bytes,
    event: batch_machine.ReadEvent,
    state: _IngestState,
    arena: TransferArena,
    writer: IngestWriter,
) -> bool:
    decision = batch_machine.read_transition(state.phase, event)
    if decision.action is batch_machine.ReadAction.REJECT:
        raise RingTransferError(decision.reason)
    if event is batch_machine.ReadEvent.ACK:
        _require_ok_ack(parse_ack_notification(payload), command="READ")
    elif event is batch_machine.ReadEvent.BEGIN:
        _consume_read_begin(payload, state, writer)
    elif event is batch_machine.ReadEvent.DATA:
        _consume_data(payload, state, arena, writer)
    elif event is batch_machine.ReadEvent.DONE:
        _validate_done(parse_done_notification(payload), arena, state.start, state.count)
        _publish_progress(state, arena, terminal=True)
    else:
        raise AssertionError("read notification event has no parser")
    state.phase = decision.next_phase
    return decision.action is batch_machine.ReadAction.BARRIER


def _read_event_for_opcode(opcode: int) -> batch_machine.ReadEvent:
    events = {
        NOTIFY_ACK: batch_machine.ReadEvent.ACK,
        NOTIFY_READ_BEGIN: batch_machine.ReadEvent.BEGIN,
        NOTIFY_DATA: batch_machine.ReadEvent.DATA,
        NOTIFY_DONE: batch_machine.ReadEvent.DONE,
    }
    try:
        return events[opcode]
    except KeyError as error:
        raise RingTransferError("unexpected notification during READ") from error


def _consume_read_begin(payload: bytes, state: _IngestState, writer: IngestWriter) -> None:
    notice = parse_read_begin_notification(payload)
    if (notice.transfer_start_sequence, notice.packet_count) != (state.start, state.count):
        raise RingTransferError("READ_BEGIN does not match requested range")
    state.read_begin_future = _enqueue_read_begin(writer, notice)


def _consume_data(payload: bytes, state: _IngestState, arena: TransferArena, writer: IngestWriter) -> None:
    arena.append(parse_data_notification(payload))
    complete_bytes = arena.complete_records * RECORD_SIZE
    tail = arena.partial_tail()
    if complete_bytes > writer_high_water(writer) or tail:
        arena.submit_prefix()
        writer.publish(complete_bytes, tail)
    # The terminal 100% snapshot is published only after DONE validates the
    # complete transfer.  An in-flight DATA notification must not look like a
    # completed read to progress consumers.
    if arena.received_records < arena.total_records:
        _publish_progress(state, arena)


async def _interrupt(
    session: RingSession,
    state: _IngestState,
    arena: TransferArena,
    writer: IngestWriter,
    options: ReadLegOptions,
) -> TransferInterruptedError:
    if state.phase is not batch_machine.ReadPhase.STOPPING:
        selected = batch_machine.decide_read_event(
            (batch_machine.ReadEvent.READ_FAILED,), writer_failed=writer.failure is not None
        )
        if selected is batch_machine.ReadEvent.WRITER_FAILED:
            state.failure_cause = writer.failure
            return await _interrupt_failed_writer(session, state, arena, writer, options)
        assert selected is batch_machine.ReadEvent.READ_FAILED
        _move_read(state, selected, batch_machine.ReadAction.STOP)
    cancellation: asyncio.CancelledError | None = None
    try:
        await join_owned(_stop_best_effort(session, options.cleanup_timeout))
    except asyncio.CancelledError as error:
        cancellation = error
        _move_read(state, batch_machine.ReadEvent.CANCELLED, batch_machine.ReadAction.LATCH_CANCELLATION)
    if writer.failure is None:
        try:
            await join_owned(_persist_interrupted_writer(state, writer))
        except asyncio.CancelledError as error:
            cancellation = error
            _move_read(state, batch_machine.ReadEvent.CANCELLED, batch_machine.ReadAction.LATCH_CANCELLATION)
        except BaseException:
            if writer.failure is None:
                raise
    if writer.failure is not None:
        _move_read(state, batch_machine.ReadEvent.WRITER_FAILED, batch_machine.ReadAction.LATCH_FAILURE)
        state.failure_cause = writer.failure
    _move_read(state, batch_machine.ReadEvent.STOP_SETTLED, batch_machine.ReadAction.PROPAGATE)
    if cancellation is not None and writer.failure is None:
        raise cancellation
    return TransferInterruptedError(
        state.failure_message, _leg_counters(arena, writer, state.baselines), cause=state.failure_cause
    )


async def _handle_read_cancelled(
    session: RingSession,
    state: _IngestState,
    arena: TransferArena,
    writer: IngestWriter,
    options: ReadLegOptions,
) -> None:
    selected = batch_machine.decide_read_event(
        (batch_machine.ReadEvent.CANCELLED,), writer_failed=writer.failure is not None
    )
    if selected is batch_machine.ReadEvent.WRITER_FAILED:
        failure = writer.failure
        assert failure is not None
        state.failure_cause = failure
        raise await _interrupt_failed_writer(session, state, arena, writer, options) from failure
    assert selected is batch_machine.ReadEvent.CANCELLED
    if state.phase is batch_machine.ReadPhase.DONE:
        # DONE admitted the writer barrier. Settle it without issuing STOP; the
        # caller still propagates cancellation and cannot seal or ADVANCE.
        await join_owned(_finish_read(state, arena, writer, cancelled=True))
        return
    _move_read(state, selected, batch_machine.ReadAction.STOP)
    interrupted = await _interrupt(session, state, arena, writer, options)
    if writer.failure is not None:
        raise interrupted from writer.failure


async def _interrupt_failed_writer(
    session: RingSession,
    state: _IngestState,
    arena: TransferArena,
    writer: IngestWriter,
    options: ReadLegOptions,
) -> TransferInterruptedError:
    decision = batch_machine.read_transition(state.phase, batch_machine.ReadEvent.WRITER_FAILED)
    state.phase = decision.next_phase
    if decision.action is batch_machine.ReadAction.STOP:
        try:
            await join_owned(_stop_best_effort(session, options.cleanup_timeout))
        except asyncio.CancelledError:
            _move_read(state, batch_machine.ReadEvent.CANCELLED, batch_machine.ReadAction.LATCH_CANCELLATION)
    elif decision.action is not batch_machine.ReadAction.PROPAGATE:
        raise RuntimeError("writer failure has no READ exit decision")
    # A failed writer cannot admit INTERRUPT. Its existing durable frontier is
    # reconciled by the coordinator's owned close, after this BLE STOP.
    if state.read_begin_future is not None:
        with suppress(BaseException):
            await join_owned(state.read_begin_future)
    if state.phase is batch_machine.ReadPhase.STOPPING:
        _move_read(state, batch_machine.ReadEvent.STOP_SETTLED, batch_machine.ReadAction.PROPAGATE)
    assert state.failure_cause is not None
    return TransferInterruptedError(
        "writer failed during READ", _leg_counters(arena, writer, state.baselines), cause=state.failure_cause
    )


def _move_read(state: _IngestState, event: batch_machine.ReadEvent, expected: batch_machine.ReadAction) -> None:
    decision = batch_machine.read_transition(state.phase, event)
    if decision.action is not expected:
        raise RuntimeError(f"READ transition expected {expected.value}: {decision.reason}")
    state.phase = decision.next_phase


async def _persist_interrupted_writer(
    state: _IngestState,
    writer: IngestWriter,
) -> None:
    """Wait for queued READ_BEGIN, exact frontier, and interruption in order."""
    if state.read_begin_future is None:
        return
    await _await_read_begin(state.read_begin_future)
    await _await_writer_interrupt(writer)


def _capture_leg_baselines(arena: TransferArena, writer: IngestWriter) -> _LegBaselines:
    """Snapshot lifetime progress before the next physical connection READ."""
    return _LegBaselines(arena.received_bytes, writer_high_water(writer), _writer_written(writer, None))


def _leg_counters(arena: TransferArena, writer: IngestWriter, baselines: _LegBaselines) -> TransferCounters:
    """Return the leg delta without disturbing lifetime recovery watermarks."""
    return TransferCounters(
        arena.received_bytes - baselines.received_bytes,
        writer_high_water(writer) - baselines.submitted_bytes,
        _writer_written(writer, None) - baselines.written_bytes,
    )


def _enqueue_read_begin(writer: IngestWriter, notice: ReadBeginNotification) -> asyncio.Future[object]:
    result = writer.submit_read_begin(notice)
    if isinstance(result, asyncio.Future):
        return result
    if isinstance(result, Awaitable):
        return asyncio.ensure_future(result)
    completed: asyncio.Future[object] = asyncio.get_running_loop().create_future()
    completed.set_result(result)
    return completed


async def _await_read_begin(future: asyncio.Future[object] | None) -> None:
    if future is not None:
        await join_owned(future)


async def _await_with_timeout(awaitable: Awaitable[object], timeout: float) -> object:
    async with asyncio.timeout(timeout):
        return await awaitable


async def _finish_read(
    state: _IngestState,
    arena: TransferArena,
    writer: IngestWriter,
    *,
    cancelled: bool = False,
) -> object:
    try:
        await _await_read_begin(state.read_begin_future)
        result = await _await_writer_barrier(writer)
    except asyncio.CancelledError:
        event = batch_machine.decide_read_event(
            (batch_machine.ReadEvent.CANCELLED,), writer_failed=writer.failure is not None
        )
        assert event is not None
        _move_read(state, event, batch_machine.ReadAction.PROPAGATE)
        if writer.failure is not None:
            raise TransferInterruptedError(
                "writer failed after READ terminal", _leg_counters(arena, writer, state.baselines), cause=writer.failure
            ) from writer.failure
        raise
    except (OSError, RuntimeError) as error:
        event = batch_machine.decide_read_event(
            (batch_machine.ReadEvent.BARRIER_FAILED,), writer_failed=writer.failure is not None
        )
        assert event is not None
        _move_read(state, event, batch_machine.ReadAction.PROPAGATE)
        cause = writer.failure or error
        raise TransferInterruptedError(
            "writer failed after READ terminal", _leg_counters(arena, writer, state.baselines), cause=cause
        ) from cause
    if writer.failure is not None:
        _move_read(state, batch_machine.ReadEvent.WRITER_FAILED, batch_machine.ReadAction.PROPAGATE)
        raise TransferInterruptedError(
            "writer failed after READ terminal", _leg_counters(arena, writer, state.baselines), cause=writer.failure
        ) from writer.failure
    if cancelled:
        _move_read(state, batch_machine.ReadEvent.CANCELLED, batch_machine.ReadAction.PROPAGATE)
    else:
        _move_read(state, batch_machine.ReadEvent.BARRIER_OK, batch_machine.ReadAction.COMPLETE)
    return result


async def _await_writer_barrier(writer: IngestWriter) -> object:
    return await writer.barrier()


async def _await_writer_interrupt(writer: IngestWriter) -> object:
    return await writer.interrupt()


async def _stop_best_effort(session: RingSession, timeout: float | None) -> None:
    try:
        if timeout is None:
            await session.write_control(encode_stop_command())
        else:
            _require_timeout(timeout)
            async with asyncio.timeout(timeout):
                await session.write_control(encode_stop_command())
    except asyncio.CancelledError:
        raise
    except Exception:  # noqa: BLE001 - cleanup must not mask primary failure
        return


def _validate_done(done: DoneNotification, arena: TransferArena, start: int, count: int) -> None:
    if not done.is_ok:
        raise RingTransferError(f"DONE reported nonzero status {done.status}")
    if done.next_sequence != start + count:
        raise RingTransferError("DONE next sequence does not match requested range")
    if arena.leg_received_bytes != count * RECORD_SIZE or arena.leg_complete_records != count:
        raise RingTransferError("DONE arrived before all requested records")


def _publish_progress(state: _IngestState, arena: TransferArena, *, terminal: bool = False) -> None:
    now = time.monotonic()
    records = arena.received_records
    total = arena.total_records
    elapsed = max(0.0, now - state.began_at)
    leg_records = max(0, records - state.initial_received_records)
    rate = leg_records / elapsed if elapsed else 0.0
    event = ProgressEvent(
        records,
        total,
        records * RECORD_SIZE,
        elapsed,
        rate,
        rate * RECORD_SIZE,
        (total - records) / rate if rate else None,
    )
    state.latest_progress = event
    state.progress_mailbox.publish(event, terminal=terminal)


def writer_high_water(writer: IngestWriter) -> int:
    return writer.submitted_high_water


def _writer_written(writer: IngestWriter, barrier_result: object | None) -> int:
    del barrier_result
    return writer.written_high_water


def _opcode(payload: bytes) -> int:
    if not payload:
        raise RingTransferError("received an empty ring notification")
    return payload[0]


def _require_ok_ack(ack: AckNotification, *, command: str = "ring command") -> None:
    if not ack.is_ok:
        raise RingAcknowledgementError(ack.status, command=command)


def _validate_info(info: RingInfo) -> None:
    if info.write_sequence < info.read_sequence:
        raise RingTransferError("INFO write sequence precedes read sequence")
    if info.packet_size != RECORD_SIZE:
        raise RingTransferError("INFO packet size is not the required ring record size")


def _require_timeout(timeout: float) -> None:
    if timeout <= 0:
        raise ValueError("timeout must be positive")


def _require_positive(value: int, label: str) -> None:
    if value <= 0:
        raise ValueError(f"{label} must be positive")


__all__ = [
    "AdvanceRejectedError",
    "AdvanceUncertainError",
    "CollectResult",
    "CollectionResult",
    "CollectorError",
    "CollectorTimeoutError",
    "IngestWriter",
    "NoDataResult",
    "ProgressCallback",
    "ProgressEvent",
    "ProgressMailbox",
    "ProgressSnapshot",
    "ReadLegOptions",
    "ReadLegResult",
    "RingAcknowledgementError",
    "RingAdvanceError",
    "RingTransferError",
    "TransferCounters",
    "TransferInterruptedError",
    "TransferTimeouts",
    "advance_leg",
    "probe",
    "read_leg",
    "report_progress",
    "ring_info",
    "writer_high_water",
]
