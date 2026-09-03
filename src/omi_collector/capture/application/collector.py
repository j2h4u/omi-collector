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
from .ring_transport import RingSession, RingTransportDisconnectedError

_OPTIONAL_OPTIONS_ARG_COUNT = 3


class CollectorError(RuntimeError):
    """Base error for a collector state-machine failure."""


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
    bytes_total: int
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


@dataclass(frozen=True, slots=True)
class ReadLegResult:
    start_sequence: int
    next_sequence: int
    packet_count: int
    records_replayed: int = 0
    records_appended: int = 0
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

    async def start(self) -> None: ...

    async def prepare_leg(self, start_sequence: int, record_count: int) -> object: ...

    def submit_read_begin(self, notice: ReadBeginNotification) -> Awaitable[object] | object: ...

    def publish(self, high_water: int) -> object: ...

    async def barrier(self) -> object: ...


@dataclass(slots=True)
class _IngestState:
    start: int
    count: int
    began_at: float
    progress_mailbox: ProgressMailbox
    read_begin_future: asyncio.Future[object] | None = None
    started: bool = False
    latest_progress: ProgressEvent | None = None
    failure_message: str = "READ interrupted"
    failure_cause: BaseException | None = None


async def probe(session: RingSession) -> RingStatus:
    return await session.read_status()


async def ring_info(session: RingSession, *, timeout: float) -> RingInfo:
    _require_timeout(timeout)
    stream = session.notifications()
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
    if leg_keywords:
        if leg_args or set(leg_keywords) != {"start", "count"}:
            raise TypeError("read_leg expects start and count exactly once")
        leg_args = (leg_keywords["start"], leg_keywords["count"])
    start, count, options = _parse_leg_args(leg_args, options)
    _require_positive(count, "count")
    _require_timeout(options.timeout)
    arena.begin_leg(start, count)

    # Preparation is deliberately before issuing READ.  Once READ is issued,
    # only BLE notification/control awaits are permitted until terminal DONE.
    await writer.start()
    await writer.prepare_leg(start, count)
    stream = session.notifications()
    state = _IngestState(start, count, time.monotonic(), options.progress_mailbox or ProgressMailbox())
    terminal = False
    try:
        await _await_with_timeout(session.write_control(encode_read_command(start, count)), options.timeout)
        terminal = await _ingest_notifications(stream, state, arena, writer, options.timeout)
    except TimeoutError as error:
        state.failure_message = "timed out during READ"
        state.failure_cause = error
        state.progress_mailbox.finish()
        raise await _interrupt(session, state, arena, writer, options) from error
    except asyncio.CancelledError:
        state.progress_mailbox.finish()
        await _stop_best_effort(session, options.cleanup_timeout)
        raise
    except (RingProtocolError, RingTransferError, MemoryError, ValueError, OSError) as error:
        state.failure_message = str(error)
        state.failure_cause = error
        state.progress_mailbox.finish()
        raise await _interrupt(session, state, arena, writer, options) from error

    if not terminal:  # defensive; the loop only exits for a validated DONE
        raise RingTransferError("READ ended without terminal DONE")
    # These waits are intentionally after DONE.  READ_BEGIN remains ordered
    # ahead of the barrier in the writer's command queue.
    barrier_result = await _finish_read(session, state, arena, writer, options)
    return ReadLegResult(
        start,
        start + count,
        count,
        received_bytes=arena.received_bytes,
        submitted_bytes=writer_high_water(writer),
        written_bytes=_writer_written(writer, barrier_result),
        progress=state.latest_progress,
    )


async def _ingest_notifications(
    stream: object,
    state: _IngestState,
    arena: TransferArena,
    writer: IngestWriter,
    timeout: float,
) -> bool:
    notifications = cast(AsyncIterator[bytes], stream)
    loop = asyncio.get_running_loop()
    async with asyncio.timeout(timeout) as deadline:
        while True:
            payload = await anext(notifications)
            deadline.reschedule(loop.time() + timeout)
            if _consume_notification(payload, state, arena, writer):
                return True


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


async def advance_leg(session: RingSession, next_sequence: int, *, timeout: float) -> None:
    """Issue one ADVANCE, distinguishing rejection from unknown outcome."""
    _require_timeout(timeout)
    if isinstance(next_sequence, bool) or not isinstance(next_sequence, int):
        raise TypeError("next_sequence must be an integer")
    try:
        stream = session.notifications()
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
    state: _IngestState,
    arena: TransferArena,
    writer: IngestWriter,
) -> bool:
    opcode = _opcode(payload)
    if opcode == NOTIFY_ACK:
        _require_ok_ack(parse_ack_notification(payload), command="READ")
        return False
    if opcode == NOTIFY_READ_BEGIN:
        _consume_read_begin(payload, state, writer)
        return False
    if opcode == NOTIFY_DATA:
        _consume_data(payload, state, arena, writer)
        return False
    if opcode == NOTIFY_DONE:
        if not state.started:
            raise RingTransferError("received DONE before READ_BEGIN")
        _validate_done(parse_done_notification(payload), arena, state.start, state.count)
        _publish_progress(state, arena, terminal=True)
        return True
    raise RingTransferError("unexpected notification during READ")


def _consume_read_begin(payload: bytes, state: _IngestState, writer: IngestWriter) -> None:
    if state.started:
        raise RingTransferError("received duplicate READ_BEGIN")
    notice = parse_read_begin_notification(payload)
    if (notice.transfer_start_sequence, notice.packet_count) != (state.start, state.count):
        raise RingTransferError("READ_BEGIN does not match requested range")
    state.read_begin_future = _enqueue_read_begin(writer, notice)
    state.started = True


def _consume_data(payload: bytes, state: _IngestState, arena: TransferArena, writer: IngestWriter) -> None:
    if not state.started:
        raise RingTransferError("received DATA before READ_BEGIN")
    arena.append(parse_data_notification(payload))
    complete_bytes = arena.complete_records * RECORD_SIZE
    if complete_bytes > writer_high_water(writer):
        arena.submit_prefix()
        writer.publish(complete_bytes)
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
    await _stop_best_effort(session, options.cleanup_timeout)
    wait_timeout = options.cleanup_timeout if options.cleanup_timeout is not None else options.timeout
    with suppress(BaseException):
        await _await_with_timeout(_await_read_begin(state.read_begin_future), wait_timeout)
    with suppress(BaseException):
        await _await_with_timeout(_await_writer_barrier(writer), wait_timeout)
    return TransferInterruptedError(state.failure_message, _counters(arena, writer), cause=state.failure_cause)


def _counters(arena: TransferArena, writer: IngestWriter) -> TransferCounters:
    return TransferCounters(arena.received_bytes, writer_high_water(writer), _writer_written(writer, None))


def _enqueue_read_begin(writer: IngestWriter, notice: ReadBeginNotification) -> asyncio.Future[object] | None:
    result = writer.submit_read_begin(notice)
    if isinstance(result, asyncio.Future):
        return result
    if isinstance(result, Awaitable):
        return asyncio.ensure_future(result)
    return None


async def _await_read_begin(future: asyncio.Future[object] | None) -> None:
    if future is not None:
        await future


async def _await_with_timeout(awaitable: Awaitable[object], timeout: float) -> object:
    async with asyncio.timeout(timeout):
        return await awaitable


async def _finish_read(
    session: RingSession,
    state: _IngestState,
    arena: TransferArena,
    writer: IngestWriter,
    options: ReadLegOptions,
) -> object:
    try:
        await _await_with_timeout(_await_read_begin(state.read_begin_future), options.timeout)
        return await _await_with_timeout(_await_writer_barrier(writer), options.timeout)
    except TimeoutError as error:
        state.failure_message = "timed out waiting for writer after READ"
        state.failure_cause = error
        raise await _interrupt(session, state, arena, writer, options) from error
    except asyncio.CancelledError:
        raise
    except BaseException as error:
        raise TransferInterruptedError(
            "writer failed after READ terminal", _counters(arena, writer), cause=error
        ) from error


async def _await_writer_barrier(writer: IngestWriter) -> object:
    return await writer.barrier()


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
    rate = records / elapsed if elapsed else 0.0
    event = ProgressEvent(
        records,
        total,
        records * RECORD_SIZE,
        total * RECORD_SIZE,
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
