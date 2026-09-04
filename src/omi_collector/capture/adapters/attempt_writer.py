"""A single-owner, asynchronous boundary around one staged-attempt writer.

The BLE callback side should call :meth:`AttemptWriter.publish` with the byte
high-water mark of a shared preallocated arena.  Publishing only updates a
counter; it never puts one queue item per notification and never calls the
target.  A dedicated thread drains that counter and is the sole caller of the
target protocol.  Control operations (READ_BEGIN, checkpoints, sealing, and
close) are bounded commands with awaitable completion.  The producer must not
mutate bytes at or below the submitted high-water mark.

This module deliberately does not know about :class:`StagedAttempt`.  The
worker that owns the attempt can provide a small adapter later.
"""

from __future__ import annotations

import asyncio
from collections import deque
from collections.abc import Callable
from dataclasses import dataclass
from enum import Enum
from threading import Event, Lock, Thread
from typing import Protocol, cast

from ...config import DEFAULT_CONFIG, WriterConfig
from ..domain.ring_protocol import RECORD_SIZE
from .attempt_writer_machine import (
    Admit,
    AttemptWriterMachineEvent,
    AttemptWriterMachineState,
    CheckpointRequested,
    CheckpointSucceeded,
    Closed,
    CloseFailed,
    CloseRequested,
    CloseSucceeded,
    Closing,
    CommandFailed,
    Constructed,
    Failed,
    Finalized,
    FinalizeRequested,
    FinalizeSucceeded,
    Finalizing,
    Ignore,
    LegRequested,
    LegSucceeded,
    PrepareSucceeded,
    PublishRequested,
    ReadBeginRequested,
    ReadBeginSucceeded,
    Reading,
    Reject,
    RejectionKind,
    ReuseClose,
    StartRequested,
    failure_of,
    transition,
)
from .debug_logging import debug_exception


class WriterTarget(Protocol):
    """Synchronous operations owned exclusively by the writer thread."""

    def prepare(self) -> object:
        """Prepare the target attempt before any leg is accepted."""

    def prepare_leg(self, start_sequence: int, record_count: int) -> object:
        """Prepare one target range before READ_BEGIN or DATA is accepted."""

    def read_begin(self, notice: object) -> object:
        """Persist the beginning of the read represented by ``notice``."""

    def append_chunk(self, offset: int, chunk: memoryview) -> object:
        """Append one contiguous slice beginning at ``offset``."""

    def checkpoint(self) -> object:
        """Durably checkpoint all bytes written so far."""

    def seal(self, done_notice: object) -> object:
        """Seal and publish the target for a completed device read."""

    def publish_prefix(self) -> object:
        """Publish the checkpoint-authenticated prefix through the target thread."""

    def close(self) -> object:
        """Release target resources."""


class WriterState(Enum):
    """Externally useful lifecycle states."""

    CREATED = "created"
    STARTED = "started"
    FAILED = "failed"
    SEALING = "sealing"
    SEALED = "sealed"
    CLOSING = "closing"
    CLOSED = "closed"


@dataclass(frozen=True, slots=True)
class WriterProgress:
    """In-memory byte progress, with no target or disk observation."""

    submitted: int
    written: int


@dataclass(frozen=True, slots=True)
class WriterSnapshot:
    """Submitted/written bytes plus durable sequence acknowledgment, if known."""

    submitted: int
    written: int
    durable_next_sequence: int | None = None
    durable_record_count: int | None = None


class WriterError(RuntimeError):
    """Base error for the writer boundary."""


class WriterClosedError(WriterError):
    """The writer no longer accepts data or control commands."""


class WriterFailedError(WriterError):
    """The target failed; sealing and further data publication are forbidden."""


class WriterQueueFullError(WriterError):
    """The bounded control-command queue is full."""


class WriterShutdownTimeoutError(WriterError):
    """The target did not finish orderly close within the requested bound."""


@dataclass(frozen=True, slots=True)
class PrepareCommand:
    """Prepare the target attempt."""


@dataclass(frozen=True, slots=True)
class PrepareLegCommand:
    """Prepare one target leg."""

    start_sequence: int
    record_count: int


@dataclass(frozen=True, slots=True)
class ReadBeginCommand:
    """Record the target's READ_BEGIN notice."""

    notice: object


@dataclass(frozen=True, slots=True)
class CheckpointCommand:
    """Flush bytes through this high-water mark, then checkpoint."""

    high_water: int


@dataclass(frozen=True, slots=True)
class SealCommand:
    """Flush bytes through this high-water mark, then seal."""

    high_water: int
    done_notice: object


@dataclass(frozen=True, slots=True)
class PublishPrefixCommand:
    """Flush published bytes, then publish the durable prefix."""

    high_water: int


@dataclass(frozen=True, slots=True)
class CloseCommand:
    """Flush bytes through this high-water mark, then close."""

    high_water: int


type WriterCommand = (
    PrepareCommand
    | PrepareLegCommand
    | ReadBeginCommand
    | CheckpointCommand
    | SealCommand
    | PublishPrefixCommand
    | CloseCommand
)
type WriterResult = object


@dataclass(slots=True)
class _Pending:
    command: WriterCommand
    future: asyncio.Future[object]


class AttemptWriter:
    """Write one shared arena through one explicit worker thread.

    ``AttemptWriter`` starts its dedicated thread immediately, but the target
    is untouched until :meth:`start` is awaited.  This lets the coordinator
    construct and publish a source before the target's ``prepare`` command is
    issued while retaining strict target ownership.
    """

    def __init__(  # noqa: PLR0913 - independent compatibility knobs are explicit
        self,
        target: WriterTarget,
        source: bytes | bytearray | memoryview,
        *,
        loop: asyncio.AbstractEventLoop | None = None,
        config: WriterConfig = DEFAULT_CONFIG.writer,
        max_control_commands: int | None = None,
        chunk_size: int | None = None,
    ) -> None:
        if max_control_commands is None:
            max_control_commands = config.max_control_commands
        if chunk_size is None:
            chunk_size = config.chunk_records * RECORD_SIZE
        if max_control_commands < 1:
            raise ValueError("max_control_commands must be positive")
        if chunk_size < 1:
            raise ValueError("chunk_size must be positive")
        if chunk_size % RECORD_SIZE:
            raise ValueError(f"chunk_size must be a multiple of record size ({RECORD_SIZE})")
        self._target = target
        arena = memoryview(source)
        if not arena.c_contiguous:
            raise ValueError("source arena must be C-contiguous")
        self._arena = arena.cast("B") if arena.format != "B" else arena
        self._loop = loop
        self._config = config
        self._max_controls = max_control_commands
        self._chunk_size = chunk_size
        self._commands: deque[_Pending] = deque()
        self._lock = Lock()
        self._wake = Event()
        self._published = 0
        self._written = 0
        self._durable_next_sequence: int | None = None
        self._durable_record_count: int | None = None
        self._state: AttemptWriterMachineState = Constructed()
        self._close_pending: asyncio.Future[object] | None = None
        self._thread = Thread(target=self._run, name="omi-attempt-writer", daemon=False)
        self._thread.start()

    @property
    def thread(self) -> Thread:
        """Return the dedicated thread, primarily for diagnostics and tests."""
        return self._thread

    @property
    def state(self) -> WriterState:
        """Return a point-in-time lifecycle state."""
        with self._lock:
            return self._writer_state_locked()

    @property
    def published_high_water(self) -> int:
        """Return the largest source offset submitted by the event-loop side."""
        with self._lock:
            return self._published

    @property
    def submitted_high_water(self) -> int:
        """Return the monotonic byte high-water mark submitted by the producer."""
        return self.published_high_water

    @property
    def submitted_bytes(self) -> int:
        """Return submitted byte progress without touching the target."""
        return self.published_high_water

    @property
    def written_high_water(self) -> int:
        """Return the largest source offset appended by the writer thread."""
        with self._lock:
            return self._written

    @property
    def written_bytes(self) -> int:
        """Return written byte progress without touching the target."""
        return self.written_high_water

    @property
    def source_capacity(self) -> int:
        """Return the pre-admitted arena capacity in bytes."""
        return len(self._arena)

    @property
    def progress(self) -> WriterProgress:
        """Return submitted and written bytes from the in-memory counters."""
        with self._lock:
            return WriterProgress(self._published, self._written)

    @property
    def snapshot(self) -> WriterSnapshot:
        """Return byte progress and the last checkpoint acknowledgment."""
        with self._lock:
            return WriterSnapshot(
                self._published,
                self._written,
                self._durable_next_sequence,
                self._durable_record_count,
            )

    @property
    def failure(self) -> BaseException | None:
        """Return the latched target exception, if any."""
        with self._lock:
            return failure_of(self._state)

    def publish(self, high_water: int) -> bool:
        """Publish a monotonic source byte offset without waiting for disk I/O.

        The return value is ``True`` only when the offset advances the current
        high-water mark.  Repeated or older offsets are harmless no-ops.
        """
        if not 0 <= high_water <= len(self._arena):
            raise ValueError(f"high_water must be between 0 and {len(self._arena)}")
        if high_water % RECORD_SIZE:
            raise ValueError(f"high_water must be record-aligned ({RECORD_SIZE} bytes)")
        with self._lock:
            result = transition(self._state, PublishRequested())
            self._raise_rejection_locked(result.directive, publish=True)
            if high_water <= self._published:
                return False
            self._published = high_water
            self._wake.set()
            return True

    async def start(self) -> None:
        """Prepare the target attempt on the dedicated thread."""
        future = self._admit(StartRequested(), lambda _high_water: PrepareCommand())
        if future is not None:
            await future

    async def prepare_leg(self, start_sequence: int, record_count: int) -> object:
        """Prepare one target leg on the dedicated thread."""
        if start_sequence < 0 or record_count < 0:
            raise ValueError("leg sequence and record count must be non-negative")
        future = self._admit(LegRequested(), lambda _high_water: PrepareLegCommand(start_sequence, record_count))
        if future is None:
            return None
        return await future

    async def read_begin(self, notice: object) -> object:
        """Record READ_BEGIN through the target thread."""
        return await self.submit_read_begin(notice)

    def submit_read_begin(self, notice: object) -> asyncio.Future[object]:
        """Queue READ_BEGIN without waiting for target or disk progress."""
        try:
            future = self._admit(ReadBeginRequested(), lambda _high_water: ReadBeginCommand(notice))
        except WriterFailedError as error:
            with self._lock:
                future = self._new_future_locked()
            future.set_exception(error)
            return future
        if future is None:
            with self._lock:
                future = self._new_future_locked()
            future.set_result(None)
        return future

    async def checkpoint(self) -> object:
        """Wait for published bytes, then invoke target checkpoint."""
        future = self._admit(CheckpointRequested(), CheckpointCommand)
        if future is None:
            return None
        return await future

    async def barrier(self) -> object:
        """Alias for :meth:`checkpoint`, emphasizing ordering semantics."""
        return await self.checkpoint()

    async def seal(self, done_notice: object = None) -> object:
        """Flush published bytes and seal, unless the target has failed."""
        future = self._admit(
            FinalizeRequested(),
            lambda high_water: SealCommand(high_water, done_notice),
            finalization_message="writer seal is already pending",
        )
        if future is None:
            return None
        return await future

    async def publish_prefix(self) -> object:
        """Flush published bytes and publish the durable prefix on the writer thread."""
        future = self._admit(
            FinalizeRequested(),
            PublishPrefixCommand,
            finalization_message="writer publication is already pending",
        )
        if future is None:
            return None
        return await future

    async def close(self, *, timeout: float | None = None) -> object:
        """Orderly-close the target, with idempotent bounded waiting.

        If the caller is cancelled, the close command remains owned by the
        writer thread and is given the same bounded opportunity to finish;
        cancellation is then re-raised.  A target that remains blocked yields
        :class:`WriterShutdownTimeoutError` rather than being silently abandoned.
        """
        if timeout is None:
            timeout = self._config.close_timeout_seconds
        if timeout <= 0:
            raise ValueError("timeout must be positive")
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout
        future = self._admit(CloseRequested(), CloseCommand)
        assert future is not None
        try:
            await asyncio.wait_for(asyncio.shield(future), timeout)
        except asyncio.CancelledError:
            remaining = deadline - loop.time()
            if remaining <= 0:
                raise WriterShutdownTimeoutError("writer close timed out after cancellation") from None
            try:
                result = await asyncio.wait_for(asyncio.shield(future), remaining)
            except TimeoutError as exc:
                raise WriterShutdownTimeoutError("writer close timed out after cancellation") from exc
            except WriterFailedError:
                await self._join_until(deadline)
                raise
            await self._join_until(deadline)
            del result
            raise
        except TimeoutError as exc:
            raise WriterShutdownTimeoutError(f"writer close timed out after {timeout:.3f}s") from exc
        except WriterFailedError:
            await self._join_until(deadline)
            raise
        return await self._join_until(deadline, result=future)

    async def _join_until(
        self,
        deadline: float,
        *,
        result: asyncio.Future[object] | None = None,
    ) -> object:
        """Join without blocking the event loop, preserving the close deadline."""
        while self._thread.is_alive():
            remaining = deadline - asyncio.get_running_loop().time()
            if remaining <= 0:
                raise WriterShutdownTimeoutError("writer thread did not exit before close deadline")
            await asyncio.sleep(min(self._config.join_poll_seconds, remaining))
        if result is None:
            return None
        return result.result()

    def _loop_for_completion(self) -> asyncio.AbstractEventLoop:
        if self._loop is not None:
            return self._loop
        self._loop = asyncio.get_running_loop()
        return self._loop

    def _new_future_locked(self) -> asyncio.Future[object]:
        return self._loop_for_completion().create_future()

    def _admit(
        self,
        event: AttemptWriterMachineEvent,
        command_factory: Callable[[int], WriterCommand],
        *,
        finalization_message: str | None = None,
    ) -> asyncio.Future[object] | None:
        """Atomically decide, capacity-check, and enqueue one control operation."""
        with self._lock:
            result = transition(self._state, event)
            self._raise_rejection_locked(result.directive, finalization_message=finalization_message)
            if isinstance(result.directive, Ignore):
                return None
            if isinstance(result.directive, ReuseClose):
                assert self._close_pending is not None
                return self._close_pending
            assert isinstance(result.directive, Admit)
            if not isinstance(event, CloseRequested) and len(self._commands) >= self._max_controls:
                raise WriterQueueFullError("writer control queue is full")
            command = command_factory(self._published)
            future = self._new_future_locked()
            self._commands.append(_Pending(command, future))
            self._state = result.state
            if isinstance(command, CloseCommand):
                self._close_pending = future
            self._wake.set()
            return future

    def _raise_rejection_locked(
        self,
        directive: object,
        *,
        publish: bool = False,
        finalization_message: str | None = None,
    ) -> None:
        if not isinstance(directive, Reject):
            return
        if directive.kind is RejectionKind.NOT_STARTED:
            raise WriterError("writer has not been started")
        if directive.kind is RejectionKind.PREPARE_PENDING:
            raise WriterError("writer preparation has not completed")
        if directive.kind is RejectionKind.READ_PENDING:
            raise WriterError("READ_BEGIN has not completed")
        if directive.kind is RejectionKind.FINALIZE_PENDING:
            assert finalization_message is not None
            raise WriterClosedError(finalization_message)
        if directive.kind is RejectionKind.FINALIZED:
            if publish:
                raise WriterClosedError("writer is sealing or sealed")
            raise WriterClosedError("writer is already sealed")
        if directive.kind is RejectionKind.FINALIZING:
            raise WriterClosedError("writer is sealing or sealed")
        if directive.kind is RejectionKind.CLOSED:
            raise WriterClosedError("writer is closed")
        assert directive.kind is RejectionKind.FAILED
        failure = failure_of(self._state)
        assert failure is not None
        raise WriterFailedError("writer target failed") from failure

    def _writer_state_locked(self) -> WriterState:
        state = self._state
        if isinstance(state, Closed):
            writer_state = WriterState.CLOSED
        elif failure_of(state) is not None:
            writer_state = WriterState.FAILED
        elif isinstance(state, Closing):
            writer_state = WriterState.CLOSING
        elif isinstance(state, Finalized):
            writer_state = WriterState.SEALED
        elif isinstance(state, Finalizing):
            writer_state = WriterState.SEALING
        elif isinstance(state, Constructed):
            writer_state = WriterState.CREATED
        else:
            writer_state = WriterState.STARTED
        return writer_state

    def _run(self) -> None:
        while True:
            pending = self._next_command()
            if pending is None:
                if self._run_idle():
                    continue
                return
            if self._run_command(pending):
                return

    def _run_idle(self) -> bool:
        with self._lock:
            failed = failure_of(self._state) is not None
            ready = isinstance(self._state, Reading)
        if failed or not ready:
            self._wake.wait()
            self._wake.clear()
            return True
        try:
            if self._drain_data(None):
                return True
        except Exception as exc:  # noqa: BLE001 - adapter failures must latch at this boundary
            self._record_failure(exc)
            return True
        self._wake.wait()
        self._wake.clear()
        return True

    def _run_command(self, pending: _Pending) -> bool:
        command = pending.command
        with self._lock:
            failure = failure_of(self._state)
        if failure is not None and not isinstance(command, CloseCommand):
            self._complete(pending.future, None, WriterFailedError("writer target failed", failure))
            return False
        try:
            result = self._execute(command)
        except Exception as exc:  # noqa: BLE001 - adapter failures must latch at this boundary
            failure = self._record_failure(exc, close_failed=isinstance(command, CloseCommand))
            self._complete(pending.future, None, WriterFailedError("writer target failed", failure))
            return isinstance(command, CloseCommand)
        with self._lock:
            self._state = transition(self._state, self._success_event(command)).state
            failure = failure_of(self._state)
        if failure is None and isinstance(command, CheckpointCommand):
            self._remember_durable_result(result)
        if failure is not None:
            self._complete(pending.future, None, WriterFailedError("writer target failed", failure))
        else:
            self._complete(pending.future, result, None)
        return isinstance(command, CloseCommand)

    def _remember_durable_result(self, result: object) -> None:
        next_sequence = getattr(result, "next_sequence", None)
        record_count = getattr(result, "record_count", None)
        if not isinstance(next_sequence, int) or not isinstance(record_count, int):
            return
        if next_sequence < 0 or record_count < 0:
            return
        with self._lock:
            self._durable_next_sequence = next_sequence
            self._durable_record_count = record_count

    def _next_command(self) -> _Pending | None:
        with self._lock:
            if self._commands:
                return self._commands.popleft()
            return None

    def _execute(self, command: WriterCommand) -> object:
        if isinstance(command, PrepareCommand):
            result = self._target.prepare()
        elif isinstance(command, PrepareLegCommand):
            result = self._target.prepare_leg(command.start_sequence, command.record_count)
        elif isinstance(command, ReadBeginCommand):
            result = self._target.read_begin(command.notice)
        elif isinstance(command, CheckpointCommand):
            self._drain_data(command.high_water, exact=True)
            result = self._target.checkpoint()
        elif isinstance(command, SealCommand):
            self._drain_data(command.high_water, exact=True)
            result = self._target.seal(command.done_notice)
        elif isinstance(command, PublishPrefixCommand):
            self._drain_data(command.high_water, exact=True)
            result = self._target.publish_prefix()
        else:
            with self._lock:
                state = self._state
            if isinstance(state, Closing) and state.failure is None and state.drain:
                self._drain_data(command.high_water, exact=True)
            result = self._target.close()
        return result

    def _drain_data(self, limit: int | None, *, exact: bool = False) -> bool:
        with self._lock:
            target = self._published if limit is None else min(limit, self._published)
            start = self._written
        wrote = False
        while start < target:
            end = target
            end = min(end, start + self._chunk_size)
            chunk = self._arena[start:end].toreadonly()
            self._target.append_chunk(start, chunk)
            with self._lock:
                self._written = end
            wrote = True
            start = end
            if not exact:
                break
        return wrote

    def _success_event(self, command: WriterCommand) -> AttemptWriterMachineEvent:
        if isinstance(command, PrepareCommand):
            return PrepareSucceeded()
        if isinstance(command, PrepareLegCommand):
            return LegSucceeded()
        if isinstance(command, ReadBeginCommand):
            return ReadBeginSucceeded()
        if isinstance(command, CheckpointCommand):
            return CheckpointSucceeded()
        if isinstance(command, SealCommand | PublishPrefixCommand):
            return FinalizeSucceeded()
        if isinstance(command, CloseCommand):
            return CloseSucceeded()
        raise TypeError(f"unknown writer command: {type(command).__name__}")

    def _record_failure(self, error: BaseException, *, close_failed: bool = False) -> BaseException:
        with self._lock:
            newly_latched = failure_of(self._state) is None
            event = CloseFailed(error) if close_failed else CommandFailed(error)
            self._state = transition(self._state, event).state
            latched = failure_of(self._state)
            if latched is None:
                self._state = Failed(error)
                latched = error
            submitted_high_water = self._published
            written_high_water = self._written
            pending = list(self._commands)
            self._commands.clear()
        if newly_latched:
            debug_exception(
                "attempt_writer_failed",
                cast(BaseException, latched),
                writer_state=WriterState.FAILED.value,
                submitted_high_water=submitted_high_water,
                written_high_water=written_high_water,
            )
        for item in pending:
            if isinstance(item.command, CloseCommand):
                with self._lock:
                    self._commands.appendleft(item)
                break
            self._complete(item.future, None, WriterFailedError("writer target failed", cast(BaseException, latched)))
        return latched

    def _complete(self, future: asyncio.Future[object], result: object | None, error: BaseException | None) -> None:
        try:
            loop = future.get_loop()
            loop.call_soon_threadsafe(self._finish_future, future, result, error)
        except RuntimeError:
            # The event loop may be gone during interpreter shutdown.  There is
            # no safe completion callback left; target ownership still remains
            # on this thread and close is bounded while the loop is alive.
            return

    @staticmethod
    def _finish_future(future: asyncio.Future[object], result: object | None, error: BaseException | None) -> None:
        if future.done():
            return
        if error is not None:
            future.set_exception(error)
        else:
            future.set_result(result)


__all__ = [
    "AttemptWriter",
    "CheckpointCommand",
    "CloseCommand",
    "PrepareCommand",
    "PrepareLegCommand",
    "PublishPrefixCommand",
    "ReadBeginCommand",
    "SealCommand",
    "WriterClosedError",
    "WriterCommand",
    "WriterError",
    "WriterFailedError",
    "WriterProgress",
    "WriterQueueFullError",
    "WriterResult",
    "WriterShutdownTimeoutError",
    "WriterSnapshot",
    "WriterState",
    "WriterTarget",
]
