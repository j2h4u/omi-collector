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
from dataclasses import dataclass
from enum import Enum
from threading import Event, Lock, Thread
from typing import Protocol, cast

from ...config import DEFAULT_CONFIG, WriterConfig
from ..domain.ring_protocol import RECORD_SIZE
from .debug_logging import debug_exception


class WriterTarget(Protocol):
    """Synchronous operations owned exclusively by the writer thread."""

    def prepare(self) -> object:
        """Prepare the target attempt before any leg is accepted."""

    def prepare_leg(self, start_sequence: int, record_count: int) -> object:
        """Prepare one target leg before READ_BEGIN or DATA is accepted."""

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
        self._failure: BaseException | None = None
        self._started = False
        self._prepared = False
        self._read_started = False
        self._sealing = False
        self._sealed = False
        self._closing = False
        self._closed = False
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
            if self._closed:
                state = WriterState.CLOSED
            elif self._failure is not None:
                state = WriterState.FAILED
            elif self._closing:
                state = WriterState.CLOSING
            elif self._sealed:
                state = WriterState.SEALED
            elif self._sealing:
                state = WriterState.SEALING
            elif self._started:
                state = WriterState.STARTED
            else:
                state = WriterState.CREATED
        return state

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
            return self._failure

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
            self._require_accepting_data_locked()
            if high_water <= self._published:
                return False
            self._published = high_water
            self._wake.set()
            return True

    async def start(self) -> None:
        """Prepare the target attempt on the dedicated thread."""
        with self._lock:
            if self._started:
                failure = self._failure
                if failure is not None:
                    raise WriterFailedError("writer target failed") from failure
                return
            self._require_open_locked()
            self._started = True
        try:
            await self._submit(PrepareCommand())
        except BaseException:
            with self._lock:
                self._started = False
            raise

    async def prepare_leg(self, start_sequence: int, record_count: int) -> object:
        """Prepare one target leg on the dedicated thread."""
        if start_sequence < 0 or record_count < 0:
            raise ValueError("leg sequence and record count must be non-negative")
        with self._lock:
            self._require_prepared_locked()
        return await self._submit(PrepareLegCommand(start_sequence, record_count))

    async def read_begin(self, notice: object) -> object:
        """Record READ_BEGIN through the target thread."""
        return await self.submit_read_begin(notice)

    def submit_read_begin(self, notice: object) -> asyncio.Future[object]:
        """Queue READ_BEGIN without waiting for target or disk progress."""
        with self._lock:
            self._require_prepared_locked()
        return self._enqueue(ReadBeginCommand(notice))

    async def checkpoint(self) -> object:
        """Wait for published bytes, then invoke target checkpoint."""
        with self._lock:
            self._require_ready_locked()
            self._raise_failure_locked()
            high_water = self._published
        return await self._submit(CheckpointCommand(high_water))

    async def barrier(self) -> object:
        """Alias for :meth:`checkpoint`, emphasizing ordering semantics."""
        return await self.checkpoint()

    async def seal(self, done_notice: object = None) -> object:
        """Flush published bytes and seal, unless the target has failed."""
        with self._lock:
            self._require_ready_locked()
            self._raise_failure_locked()
            if self._sealed:
                raise WriterClosedError("writer is already sealed")
            if self._sealing:
                raise WriterClosedError("writer seal is already pending")
            self._sealing = True
            high_water = self._published
        try:
            result = await self._submit(SealCommand(high_water, done_notice))
        except BaseException:
            with self._lock:
                self._sealing = False
            raise
        with self._lock:
            self._sealed = True
        return result

    async def publish_prefix(self) -> object:
        """Flush published bytes and publish the durable prefix on the writer thread."""
        with self._lock:
            self._require_ready_locked()
            self._raise_failure_locked()
            if self._sealed:
                raise WriterClosedError("writer is already sealed")
            if self._sealing:
                raise WriterClosedError("writer publication is already pending")
            self._sealing = True
            high_water = self._published
        try:
            result = await self._submit(PublishPrefixCommand(high_water))
        except BaseException:
            with self._lock:
                self._sealing = False
            raise
        with self._lock:
            self._sealing = False
            self._sealed = True
        return result

    async def close(self, *, timeout: float | None = None) -> object:
        """Orderly-close the target, with idempotent bounded waiting.

        If the caller is cancelled, the close command remains owned by the
        writer thread and is given the same bounded opportunity to finish;
        cancellation is then re-raised.  A target that remains blocked yields
        :class:`WriterShutdownTimeout` rather than being silently abandoned.
        """
        if timeout is None:
            timeout = self._config.close_timeout_seconds
        if timeout <= 0:
            raise ValueError("timeout must be positive")
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout
        with self._lock:
            if self._close_pending is None:
                self._require_open_locked()
                self._closing = True
                high_water = self._published
                self._close_pending = self._new_future_locked()
                self._commands.append(_Pending(CloseCommand(high_water), self._close_pending))
                self._wake.set()
            future = self._close_pending
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

    def _enqueue(self, command: WriterCommand) -> asyncio.Future[object]:
        with self._lock:
            if command.__class__ is not CloseCommand:
                self._require_open_locked()
            if len(self._commands) >= self._max_controls:
                raise WriterQueueFullError("writer control queue is full")
            future = self._new_future_locked()
            self._commands.append(_Pending(command, future))
            self._wake.set()
        return future

    async def _submit(self, command: WriterCommand) -> object:
        return await self._enqueue(command)

    def _require_open_locked(self) -> None:
        if self._closed or self._closing:
            raise WriterClosedError("writer is closed")

    def _require_accepting_data_locked(self) -> None:
        self._require_open_locked()
        if self._sealed or self._sealing:
            raise WriterClosedError("writer is sealing or sealed")
        self._raise_failure_locked()

    def _require_started_locked(self) -> None:
        self._require_open_locked()
        if not self._started:
            raise WriterError("writer has not been started")

    def _require_prepared_locked(self) -> None:
        self._require_started_locked()
        if not self._prepared:
            raise WriterError("writer preparation has not completed")

    def _require_ready_locked(self) -> None:
        self._require_prepared_locked()
        self._raise_failure_locked()
        if not self._read_started:
            raise WriterError("READ_BEGIN has not completed")

    def _raise_failure_locked(self) -> None:
        if self._failure is not None:
            raise WriterFailedError("writer target failed") from self._failure

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
            failed = self._failure is not None
            ready = self._prepared and self._read_started
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
            failure = self._failure
        if failure is not None and not isinstance(command, CloseCommand):
            self._complete(pending.future, None, WriterFailedError("writer target failed", failure))
            return False
        try:
            result = self._execute(command)
        except Exception as exc:  # noqa: BLE001 - adapter failures must latch at this boundary
            self._record_failure(exc)
            self._complete(pending.future, None, WriterFailedError("writer target failed", exc))
            if isinstance(command, CloseCommand):
                with self._lock:
                    self._closing = False
                    self._closed = True
            return isinstance(command, CloseCommand)
        with self._lock:
            failure = self._failure
        if failure is None and isinstance(command, CheckpointCommand):
            self._remember_durable_result(result)
        if failure is not None:
            self._complete(pending.future, None, WriterFailedError("writer target failed", failure))
        else:
            self._complete(pending.future, result, None)
        if isinstance(command, CloseCommand):
            with self._lock:
                self._closing = False
                self._closed = True
            return True
        return False

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
            with self._lock:
                self._prepared = True
        elif isinstance(command, PrepareLegCommand):
            result = self._target.prepare_leg(command.start_sequence, command.record_count)
        elif isinstance(command, ReadBeginCommand):
            result = self._target.read_begin(command.notice)
            with self._lock:
                self._read_started = True
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
                failed = self._failure is not None
                ready = self._prepared and self._read_started
            if not failed and ready:
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

    def _record_failure(self, error: BaseException) -> None:
        with self._lock:
            newly_latched = self._failure is None
            if self._failure is None:
                self._failure = error
            latched = self._failure
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
    "WriterShutdownTimeout",
    "WriterShutdownTimeoutError",
    "WriterSnapshot",
    "WriterState",
    "WriterTarget",
]

# Compatibility spelling kept short for callers that use the public error
# name from the initial boundary draft; the class itself follows N818.
WriterShutdownTimeout = WriterShutdownTimeoutError
