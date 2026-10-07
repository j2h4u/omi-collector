"""Focused concurrency and failure tests for the standalone attempt writer."""

from __future__ import annotations

import asyncio
import multiprocessing
import os
import threading
import time
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager, suppress
from dataclasses import FrozenInstanceError, dataclass, field
from multiprocessing.connection import Connection
from pathlib import Path
from typing import Protocol
from unittest.mock import patch

import pytest

from omi_collector.capture.adapters import attempt_writer, attempt_writer_machine
from omi_collector.capture.adapters.attempt_writer import (
    AttemptWriter,
    WriterClosedError,
    WriterFailedError,
    WriterQueueFullError,
    WriterShutdownTimeoutError,
    WriterState,
)
from omi_collector.capture.application.session_lifecycle import bounded
from omi_collector.capture.domain.ring_protocol import RECORD_SIZE
from omi_collector.config import DEFAULT_CONFIG, WriterConfig


@dataclass(frozen=True)
class DurableMarker:
    next_sequence: int
    record_count: int


@dataclass(frozen=True)
class UnvalidatedMarker:
    next_sequence: object
    record_count: object


@dataclass
class FakeTarget:
    calls: list[tuple[str, int, bytes | object]] = field(default_factory=list)
    thread_ids: set[int] = field(default_factory=set)
    append_started: threading.Event = field(default_factory=threading.Event)
    release_append: threading.Event = field(default_factory=threading.Event)
    block_append: bool = False
    fail_append: bool = False
    fail_append_after_block: bool = False
    block_read_begin: bool = False
    fail_read_begin: bool = False
    fail_read_begin_after_block: bool = False
    release_read_begin: threading.Event = field(default_factory=threading.Event)
    read_begin_started: threading.Event = field(default_factory=threading.Event)
    block_prepare: bool = False
    release_prepare: threading.Event = field(default_factory=threading.Event)
    prepare_started: threading.Event = field(default_factory=threading.Event)
    block_seal: bool = False
    release_seal: threading.Event = field(default_factory=threading.Event)
    seal_started: threading.Event = field(default_factory=threading.Event)
    block_close: bool = False
    fail_close: bool = False
    release_close: threading.Event = field(default_factory=threading.Event)
    close_started: threading.Event = field(default_factory=threading.Event)
    seal_calls: int = 0
    close_calls: int = 0
    checkpoint_result: object = "checkpointed"
    prepare_leg_result: object = "prepared"
    append_readonly: list[bool] = field(default_factory=list)
    append_offsets: list[int] = field(default_factory=list)
    second_append_started: threading.Event = field(default_factory=threading.Event)

    def _record(self, name: str, value: bytes | object = None) -> None:
        self.thread_ids.add(threading.get_ident())
        self.calls.append((name, 0, value))

    def prepare(self) -> object:
        self._record("prepare")
        self.prepare_started.set()
        if self.block_prepare:
            self.release_prepare.wait(5)
        return "prepared"

    def prepare_leg(self, start_sequence: int, record_count: int) -> object:
        self._record("prepare_leg", (start_sequence, record_count))
        return self.prepare_leg_result

    def read_begin(self, notice: object) -> object:
        self._record("read_begin", notice)
        self.read_begin_started.set()
        if self.fail_read_begin:
            raise OSError("read begin failed")
        if self.block_read_begin:
            self.release_read_begin.wait(5)
        if self.fail_read_begin_after_block:
            raise OSError("read begin failed after close")
        return notice

    def append_chunk(self, offset: int, chunk: memoryview) -> object:
        self.thread_ids.add(threading.get_ident())
        self.calls.append(("append", offset, bytes(chunk)))
        self.append_readonly.append(chunk.readonly)
        self.append_offsets.append(offset)
        if len(self.append_offsets) == 2:
            self.second_append_started.set()
        self.append_started.set()
        if self.fail_append:
            raise OSError("disk full")
        if self.block_append:
            self.release_append.wait(5)
        if self.fail_append_after_block:
            raise OSError("disk full after append barrier")
        return None

    def checkpoint(self) -> object:
        self._record("checkpoint")
        return self.checkpoint_result

    def seal(self, done_notice: object) -> object:
        self._record("seal", done_notice)
        self.seal_calls += 1
        self.seal_started.set()
        if self.block_seal:
            self.release_seal.wait(5)
        return "sealed"

    def publish_prefix(self) -> object:
        self._record("publish_prefix")
        return "prefix"

    def close(self) -> object:
        self._record("close")
        self.close_calls += 1
        self.close_started.set()
        if self.block_close:
            self.release_close.wait(5)
        if self.fail_close:
            raise OSError("close failed")
        return "closed"


class _ProcessEvent(Protocol):
    def set(self) -> None: ...

    def wait(self, timeout: float | None = None) -> bool: ...


class _ProcessRelease(Protocol):
    def poll(self, timeout: float | None = None) -> bool: ...

    def recv(self) -> object: ...


class _ProcessFlags(Protocol):
    def __getitem__(self, index: int) -> int: ...

    def __setitem__(self, index: int, value: int) -> None: ...


def _test_daemon_thread_factory(target: Callable[..., object], name: str, daemon: bool) -> threading.Thread:
    assert daemon is False
    return threading.Thread(target=target, name=name, daemon=True)


class ProcessLifetimeTarget:
    def __init__(self, entered: _ProcessEvent, release: _ProcessRelease, flags: _ProcessFlags) -> None:
        self.entered = entered
        self.release = release
        self.flags = flags

    def prepare(self) -> object:
        return None

    def prepare_leg(self, start_sequence: int, record_count: int) -> object:
        del start_sequence, record_count
        return None

    def read_begin(self, notice: object) -> object:
        del notice
        self.flags[0] = 1
        return None

    def append_chunk(self, offset: int, chunk: memoryview) -> object:
        del offset, chunk
        self.entered.set()
        if not self.release.poll(15):
            raise TimeoutError("parent did not release the writer target")
        self.release.recv()
        return None

    def checkpoint(self) -> object:
        self.flags[1] = 1
        return None

    def seal(self, done_notice: object) -> object:
        del done_notice
        return None

    def publish_prefix(self) -> object:
        return None

    def close(self) -> object:
        self.flags[2] = 1
        return None


def _run_writer_lifecycle_in_daemon_owner(entered: _ProcessEvent, release: Connection, flags: _ProcessFlags) -> None:
    async def lifecycle() -> None:
        writer = AttemptWriter(ProcessLifetimeTarget(entered, release, flags), bytes(RECORD_SIZE))
        try:
            assert writer.thread.daemon is False
            await writer.start()
            await writer.prepare_leg(10, 1)
            await writer.read_begin("begin")
            writer.publish(RECORD_SIZE)
            await writer.checkpoint()
            await writer.close(timeout=5)
        finally:
            if writer.state is not WriterState.CLOSED:
                await writer.close(timeout=5)

    def owner() -> None:
        try:
            asyncio.run(lifecycle())
        except Exception:  # noqa: BLE001 - the child reports owner-thread failures through shared state
            flags[3] = 1

    owner_thread = threading.Thread(target=owner, name="test-writer-owner", daemon=True)
    owner_thread.start()
    if not entered.wait(10):
        flags[3] = 1


@asynccontextmanager
async def _owned_writer(
    target: FakeTarget,
    source: bytes | bytearray | memoryview,
    *,
    config: WriterConfig = DEFAULT_CONFIG.writer,
    expect_failure: bool = False,
) -> AsyncIterator[AttemptWriter]:
    with patch.object(attempt_writer, "Thread", _test_daemon_thread_factory):
        writer = AttemptWriter(target, source, config=config)
    try:
        yield writer
    finally:
        for barrier in (
            target.release_prepare,
            target.release_read_begin,
            target.release_append,
            target.release_seal,
            target.release_close,
        ):
            barrier.set()
        try:
            await writer.close(timeout=1)
        except WriterFailedError:
            if not expect_failure or writer.failure is None:
                raise
        finally:
            await asyncio.to_thread(writer.thread.join, 1)
            assert not writer.thread.is_alive()


@asynccontextmanager
async def _started(
    target: FakeTarget,
    source: bytes | bytearray,
    *,
    chunk_records: int = 1,
    expect_failure: bool = False,
) -> AsyncIterator[AttemptWriter]:
    async with _owned_writer(
        target,
        source,
        config=WriterConfig(chunk_records=chunk_records),
        expect_failure=expect_failure,
    ) as writer:
        await writer.start()
        await writer.read_begin("begin")
        yield writer


def _thread_cpu_ticks(native_id: int) -> int:
    fields = Path(f"/proc/self/task/{native_id}/stat").read_text(encoding="ascii").rsplit(") ", maxsplit=1)[1].split()
    return int(fields[11]) + int(fields[12])


def test_arena_is_shared_and_data_waits_for_read_begin() -> None:
    asyncio.run(_test_arena_is_shared_and_data_waits_for_read_begin())


@pytest.mark.parametrize(
    ("value", "field", "replacement"),
    (
        (attempt_writer.WriterProgress(1, 0), "submitted", 2),
        (attempt_writer.WriterSnapshot(1, 0), "submitted", 2),
        (attempt_writer.PrepareLegCommand(1, 1), "record_count", 2),
        (attempt_writer.ReadBeginCommand("begin"), "notice", "changed"),
        (attempt_writer.CheckpointCommand(64), "high_water", 128),
        (attempt_writer.SealCommand(64, "done"), "done_notice", "changed"),
        (attempt_writer.PublishPrefixCommand(64), "high_water", 128),
        (attempt_writer.CloseCommand(64), "high_water", 128),
    ),
)
def test_writer_progress_and_commands_reject_field_reassignment(
    value: object, field: str, replacement: object
) -> None:
    with pytest.raises(FrozenInstanceError):
        setattr(value, field, replacement)


def test_writer_config_controls_writer_settings() -> None:
    config = WriterConfig(chunk_records=2, max_control_commands=3, join_poll_seconds=0.123)

    async def exercise() -> None:
        async with _owned_writer(FakeTarget(), bytearray(RECORD_SIZE * 5), config=config) as writer:
            assert writer._config is config
            assert writer._config.join_poll_seconds == 0.123

    asyncio.run(exercise())


def test_non_daemon_writer_keeps_process_alive_until_admitted_work_finishes() -> None:
    context = multiprocessing.get_context("fork")
    entered = context.Event()
    release_reader, release_writer = context.Pipe(duplex=False)
    flags = context.Array("i", [0, 0, 0, 0])
    process = context.Process(target=_run_writer_lifecycle_in_daemon_owner, args=(entered, release_reader, flags))
    started = False
    released = False

    try:
        process.start()
        started = True
        release_reader.close()
        assert entered.wait(5), "writer did not enter its blocked append"
        process.join(timeout=1)
        assert process.is_alive(), "process exited while writer-owned work was blocked"
        assert flags[0] == 1
        assert flags[1] == 0
        assert flags[2] == 0

        release_writer.send(None)
        released = True
        process.join(timeout=5)
        assert not process.is_alive(), "writer workflow did not finish after target release"
        assert process.exitcode == 0
        assert flags[1] == 1
        assert flags[2] == 1
        assert flags[3] == 0
    finally:
        if not released:
            with suppress(BrokenPipeError, EOFError, OSError):
                release_writer.send(None)
        if started and process.is_alive():
            process.join(timeout=2)
        if started and process.is_alive():
            process.terminate()
            process.join(timeout=2)
        if started and process.is_alive():
            process.kill()
            process.join(timeout=2)
        release_reader.close()
        release_writer.close()
        if started and not process.is_alive():
            process.close()


def test_writer_config_controls_control_capacity() -> None:
    async def exercise() -> None:
        target = FakeTarget(block_read_begin=True)
        async with _owned_writer(target, bytearray(RECORD_SIZE), config=WriterConfig(max_control_commands=1)) as writer:
            await writer.start()
            first = writer.submit_read_begin("first")
            assert await asyncio.to_thread(target.read_begin_started.wait, 1)
            second = writer.submit_read_begin("second")
            with pytest.raises(WriterQueueFullError):
                writer.submit_read_begin("third")
            target.release_read_begin.set()
            assert await first == "first"
            assert await second == "second"
            await writer.close()

    asyncio.run(exercise())


def test_writer_config_controls_default_close_timeout() -> None:
    async def exercise() -> None:
        target = FakeTarget(block_close=True)
        config = WriterConfig(close_timeout_seconds=0.01, join_poll_seconds=0.001)
        async with _owned_writer(target, bytearray(), config=config) as writer:
            await writer.start()
            await writer.read_begin("begin")

            with pytest.raises(WriterShutdownTimeoutError):
                await writer.close()
            assert target.close_calls == 1
            target.release_close.set()
            assert await writer.close(timeout=1) == "closed"

    asyncio.run(exercise())


async def _test_arena_is_shared_and_data_waits_for_read_begin() -> None:
    target = FakeTarget()
    arena = bytearray(RECORD_SIZE * 2)
    async with _owned_writer(target, arena, config=WriterConfig(chunk_records=1)) as writer:
        assert writer.state is WriterState.CREATED
        await writer.start()
        assert writer.state is WriterState.STARTED
        arena[:RECORD_SIZE] = b"x" * RECORD_SIZE
        writer.publish(RECORD_SIZE)
        await asyncio.sleep(0.02)
        assert not [call for call in target.calls if call[0] == "append"]

        await writer.read_begin("begin")
        await writer.barrier()
        assert target.calls[2] == ("append", 0, b"x" * RECORD_SIZE)
        assert writer.progress.submitted == RECORD_SIZE
        assert writer.progress.written == RECORD_SIZE
        await writer.close()


def test_submit_read_begin_is_nonblocking_and_orders_data() -> None:
    asyncio.run(_test_submit_read_begin_is_nonblocking_and_orders_data())


async def _test_submit_read_begin_is_nonblocking_and_orders_data() -> None:
    target = FakeTarget()
    async with _owned_writer(
        target,
        bytearray(RECORD_SIZE),
        config=WriterConfig(chunk_records=1),
    ) as writer:
        await writer.start()
        target.block_read_begin = True
        began = time.monotonic()
        future = writer.submit_read_begin("queued")
        elapsed = time.monotonic() - began
        assert elapsed < 0.05
        assert not future.done()
        writer.publish(RECORD_SIZE)
        await asyncio.sleep(0.01)
        assert not [call for call in target.calls if call[0] == "append"]
        target.release_read_begin.set()
        assert await future == "queued"
        await writer.barrier()
        names = [call[0] for call in target.calls]
        assert names.index("read_begin") < names.index("append")
        await writer.close()


def test_prefix_and_done_seal_are_worker_owned_and_ordered() -> None:
    asyncio.run(_test_prefix_and_done_seal_are_worker_owned_and_ordered())


async def _test_prefix_and_done_seal_are_worker_owned_and_ordered() -> None:
    target = FakeTarget()
    async with _owned_writer(target, bytearray(RECORD_SIZE * 2), config=WriterConfig(chunk_records=1)) as writer:
        await writer.start()
        assert await writer.prepare_leg(100, 2) == "prepared"
        await writer.read_begin("begin")
        writer.publish(RECORD_SIZE)
        assert await writer.publish_prefix() == "prefix"
        assert [call[0] for call in target.calls] == [
            "prepare",
            "prepare_leg",
            "read_begin",
            "append",
            "publish_prefix",
        ]
        assert target.calls[-1] == ("publish_prefix", 0, None)
        assert len(target.thread_ids) == 1
        assert writer.state is WriterState.SEALED
        await writer.close()


def test_sealing_state_is_visible_until_target_seal_completes() -> None:
    asyncio.run(_test_sealing_state_is_visible_until_target_seal_completes())


async def _test_sealing_state_is_visible_until_target_seal_completes() -> None:
    target = FakeTarget(block_seal=True)
    async with _started(target, bytearray(RECORD_SIZE)) as writer:
        seal_task = asyncio.create_task(writer.seal("done"))
        assert await asyncio.to_thread(target.seal_started.wait, 1)
        assert writer.state is WriterState.SEALING
        target.release_seal.set()
        assert await seal_task == "sealed"
        assert writer.state is WriterState.SEALED
        await writer.close()


def test_read_begin_failure_latches_and_forbids_later_controls() -> None:
    asyncio.run(_test_read_begin_failure_latches_and_forbids_later_controls())


async def _test_read_begin_failure_latches_and_forbids_later_controls() -> None:
    target = FakeTarget(fail_read_begin=True)
    async with _owned_writer(
        target,
        bytearray(RECORD_SIZE),
        config=WriterConfig(chunk_records=1),
        expect_failure=True,
    ) as writer:
        await writer.start()
        future = writer.submit_read_begin("bad")
        with pytest.raises(WriterFailedError, match="target failed"):
            await future
        with pytest.raises(WriterFailedError, match="target failed"):
            await writer.checkpoint()
        with pytest.raises(WriterFailedError, match="target failed"):
            await writer.seal("done")
        assert not [call for call in target.calls if call[0] == "seal"]
        with pytest.raises(WriterFailedError, match="target failed"):
            await writer.close()


def test_publish_is_nonblocking_while_target_append_is_slow() -> None:
    asyncio.run(_test_publish_is_nonblocking_while_target_append_is_slow())


async def _test_publish_is_nonblocking_while_target_append_is_slow() -> None:
    target = FakeTarget(block_append=True)
    async with _started(target, bytearray(RECORD_SIZE * 10)) as writer:
        heartbeat = 0

        async def tick() -> None:
            nonlocal heartbeat
            for _ in range(8):
                await asyncio.sleep(0.005)
                heartbeat += 1

        task = asyncio.create_task(tick())
        assert writer.publish(RECORD_SIZE)
        assert target.append_started.wait(1)
        began = time.monotonic()
        for high_water in range(2, 11):
            assert writer.publish(high_water * RECORD_SIZE)
        elapsed = time.monotonic() - began
        await task
        assert elapsed < 0.05
        assert heartbeat == 8
        target.release_append.set()
        await writer.close()


def test_publish_high_water_is_record_aligned() -> None:
    async def exercise() -> None:
        async with _owned_writer(FakeTarget(), bytearray(RECORD_SIZE), config=WriterConfig(chunk_records=1)) as writer:
            with pytest.raises(ValueError, match="record-aligned"):
                writer.publish(1)
            await writer.close()

    asyncio.run(exercise())


def test_publish_zero_and_repeated_high_water_are_noops() -> None:
    async def exercise() -> None:
        target = FakeTarget()
        async with _started(target, bytearray(b"x" * RECORD_SIZE)) as writer:
            try:
                assert writer.publish(0) is False
                assert writer.publish(RECORD_SIZE)
                assert writer.publish(RECORD_SIZE) is False
                assert writer.publish(0) is False
                await writer.barrier()

                assert [call for call in target.calls if call[0] == "append"] == [("append", 0, b"x" * RECORD_SIZE)]
            finally:
                await writer.close()

    asyncio.run(exercise())


def test_contiguous_non_byte_memoryview_is_published_as_bytes() -> None:
    async def exercise() -> None:
        target = FakeTarget()
        source = memoryview(b"x" * RECORD_SIZE).cast("I")
        async with _owned_writer(target, source, config=WriterConfig(chunk_records=1)) as writer:
            try:
                await writer.start()
                await writer.read_begin("begin")

                assert writer.publish(RECORD_SIZE)
                await writer.barrier()

                assert ("append", 0, b"x" * RECORD_SIZE) in target.calls
            finally:
                await writer.close()

    asyncio.run(exercise())


def test_high_water_coalesces_data_and_barrier_orders_writes() -> None:
    asyncio.run(_test_high_water_coalesces_data_and_barrier_orders_writes())


async def _test_high_water_coalesces_data_and_barrier_orders_writes() -> None:
    target = FakeTarget()
    async with _started(target, bytearray(RECORD_SIZE * 10), chunk_records=3) as writer:
        writer.publish(RECORD_SIZE * 2)
        writer.publish(RECORD_SIZE * 7)
        writer.publish(RECORD_SIZE * 10)
        assert await writer.barrier() == "checkpointed"

        assert [(name, offset, value) for name, offset, value in target.calls] == [
            ("prepare", 0, None),
            ("read_begin", 0, "begin"),
            ("append", 0, bytes(RECORD_SIZE * 3)),
            ("append", RECORD_SIZE * 3, bytes(RECORD_SIZE * 3)),
            ("append", RECORD_SIZE * 6, bytes(RECORD_SIZE * 3)),
            ("append", RECORD_SIZE * 9, bytes(RECORD_SIZE)),
            ("checkpoint", 0, None),
        ]
        assert all(target.append_readonly)
        assert len([call for call in target.calls if call[0] == "append"]) == 4
        await writer.close()


@pytest.mark.parametrize("operation", ("seal", "publish_prefix", "close"))
def test_drain_integrity_finalizers_flush_the_submitted_high_water(operation: str) -> None:
    asyncio.run(_test_drain_integrity_finalizers_flush_the_submitted_high_water(operation))


async def _test_drain_integrity_finalizers_flush_the_submitted_high_water(operation: str) -> None:
    target = FakeTarget(block_append=True)
    async with _started(target, bytearray(RECORD_SIZE * 3), chunk_records=1) as writer:
        finalizer: asyncio.Task[object] | None = None
        try:
            assert writer.publish(RECORD_SIZE * 3)
            assert await asyncio.to_thread(target.append_started.wait, 3)

            if operation == "seal":
                finalizer = asyncio.create_task(writer.seal("done"))
                expected_state = WriterState.SEALING
            elif operation == "publish_prefix":
                finalizer = asyncio.create_task(writer.publish_prefix())
                expected_state = WriterState.SEALING
            else:
                finalizer = asyncio.create_task(writer.close(timeout=1))
                expected_state = WriterState.CLOSING

            # Yield once so the public operation admits its command while append is
            # still held at the first chunk; the state is the public admission signal.
            await asyncio.sleep(0)
            assert writer.state is expected_state
            target.release_append.set()
            await finalizer

            assert target.append_offsets == [0, RECORD_SIZE, 2 * RECORD_SIZE]
            names = [call[0] for call in target.calls]
            terminal = {"seal": "seal", "publish_prefix": "publish_prefix", "close": "close"}[operation]
            assert names[-1] == terminal
        finally:
            target.release_append.set()
            if finalizer is not None and not finalizer.done():
                await finalizer
            if writer.thread.is_alive():
                await writer.close(timeout=1)


def test_drain_integrity_idle_writer_continues_after_first_chunk() -> None:
    asyncio.run(_test_drain_integrity_idle_writer_continues_after_first_chunk())


async def _test_drain_integrity_idle_writer_continues_after_first_chunk() -> None:
    target = FakeTarget()
    async with _started(target, bytearray(RECORD_SIZE * 3), chunk_records=1) as writer:
        try:
            assert writer.publish(RECORD_SIZE * 3)
            assert await asyncio.to_thread(target.second_append_started.wait, 3)
            assert target.append_offsets[:2] == [0, RECORD_SIZE]
        finally:
            await writer.close(timeout=1)


def test_idle_writer_consumes_negligible_thread_cpu() -> None:
    asyncio.run(_test_idle_writer_consumes_negligible_thread_cpu())


async def _test_idle_writer_consumes_negligible_thread_cpu() -> None:
    async with _owned_writer(FakeTarget(), bytes(RECORD_SIZE)) as writer:
        await writer.start()
        await writer.read_begin("begin")
        native_id = writer.thread.native_id
        assert native_id is not None
        ticks_per_second = os.sysconf("SC_CLK_TCK")
        before = _thread_cpu_ticks(native_id)
        await asyncio.sleep(1.0)
        elapsed_cpu = (_thread_cpu_ticks(native_id) - before) / ticks_per_second

        assert elapsed_cpu <= 0.05, f"idle writer consumed {elapsed_cpu:.3f}s CPU"


def test_snapshot_records_durable_checkpoint_ack_without_target_inspection() -> None:
    asyncio.run(_test_snapshot_records_durable_checkpoint_ack_without_target_inspection())


async def _test_snapshot_records_durable_checkpoint_ack_without_target_inspection() -> None:
    target = FakeTarget(checkpoint_result=DurableMarker(102, 2))
    async with _started(target, bytearray(RECORD_SIZE * 2)) as writer:
        writer.publish(RECORD_SIZE * 2)
        await writer.checkpoint()
        assert writer.snapshot.submitted == RECORD_SIZE * 2
        assert writer.snapshot.written == RECORD_SIZE * 2
        assert writer.snapshot.durable_next_sequence == 102
        assert writer.snapshot.durable_record_count == 2
        await writer.close()


def test_prepare_leg_accepts_zero_count_and_forwards_target_receipt() -> None:
    async def exercise() -> None:
        target = FakeTarget()
        async with _owned_writer(target, bytearray()) as writer:
            await writer.start()

            result = await writer.prepare_leg(0, 0)

            assert result == "prepared"
            assert target.calls[-1] == ("prepare_leg", 0, (0, 0))
            await writer.close()

    asyncio.run(exercise())


@pytest.mark.parametrize("start_sequence,record_count", [(-1, 0), (0, -1)])
def test_prepare_leg_rejects_negative_values_before_target_call(start_sequence: int, record_count: int) -> None:
    async def exercise() -> None:
        target = FakeTarget()
        async with _started(target, bytearray()) as writer:
            calls_before = tuple(target.calls)

            with pytest.raises(ValueError):
                await writer.prepare_leg(start_sequence, record_count)

            assert tuple(target.calls) == calls_before
            await writer.close()

    asyncio.run(exercise())


@pytest.mark.parametrize("timeout", [0, -0.5])
def test_invalid_close_timeout_leaves_writer_usable(timeout: float) -> None:
    async def exercise() -> None:
        target = FakeTarget()
        async with _started(target, bytearray(RECORD_SIZE)) as writer:
            with pytest.raises(ValueError, match="timeout must be positive"):
                await writer.close(timeout=timeout)

            assert target.close_calls == 0
            assert writer.publish(RECORD_SIZE)
            assert await writer.barrier() == "checkpointed"
            assert writer.written_bytes == RECORD_SIZE
            await writer.close()

    asyncio.run(exercise())


def test_progress_byte_properties_track_snapshot_around_blocked_append() -> None:
    async def exercise() -> None:
        target = FakeTarget(block_append=True)
        async with _started(target, bytearray(RECORD_SIZE)) as writer:
            before = writer.snapshot
            assert (writer.submitted_bytes, writer.written_bytes) == (before.submitted, before.written) == (0, 0)
            assert writer.publish(RECORD_SIZE)
            assert await asyncio.to_thread(target.append_started.wait, 1)

            blocked = writer.snapshot
            assert (writer.submitted_bytes, writer.written_bytes) == (blocked.submitted, blocked.written)
            assert (blocked.submitted, blocked.written) == (RECORD_SIZE, 0)

            target.release_append.set()
            await writer.barrier()
            completed = writer.snapshot
            assert (writer.submitted_bytes, writer.written_bytes) == (completed.submitted, completed.written)
            assert (completed.submitted, completed.written) == (RECORD_SIZE, RECORD_SIZE)
            await writer.close()

    asyncio.run(exercise())


@pytest.mark.parametrize(
    "receipt",
    [
        DurableMarker(-1, 2),
        DurableMarker(102, -1),
        UnvalidatedMarker("102", 2),
        UnvalidatedMarker(102, "2"),
    ],
)
def test_invalid_checkpoint_receipt_preserves_last_valid_snapshot(receipt: object) -> None:
    async def exercise() -> None:
        target = FakeTarget(checkpoint_result=DurableMarker(102, 2))
        async with _started(target, bytearray()) as writer:
            await writer.checkpoint()
            acknowledged = writer.snapshot
            assert (acknowledged.durable_next_sequence, acknowledged.durable_record_count) == (102, 2)

            target.checkpoint_result = receipt
            assert await asyncio.wait_for(writer.checkpoint(), timeout=1) == receipt
            assert writer.snapshot == acknowledged
            await writer.close()

    asyncio.run(exercise())


def test_zero_checkpoint_receipt_replaces_previous_acknowledgment() -> None:
    async def exercise() -> None:
        target = FakeTarget(checkpoint_result=DurableMarker(102, 2))
        async with _started(target, bytearray()) as writer:
            await writer.checkpoint()
            target.checkpoint_result = DurableMarker(0, 0)

            assert await writer.checkpoint() == DurableMarker(0, 0)
            assert (writer.snapshot.durable_next_sequence, writer.snapshot.durable_record_count) == (0, 0)
            await writer.close()

    asyncio.run(exercise())


def test_prepare_leg_receipt_does_not_replace_checkpoint_acknowledgment() -> None:
    async def exercise() -> None:
        target = FakeTarget(checkpoint_result=DurableMarker(102, 2))
        target.prepare_leg_result = DurableMarker(103, 3)
        async with _started(target, bytearray()) as writer:
            await writer.checkpoint()
            acknowledged = writer.snapshot

            assert await writer.prepare_leg(100, 3) == DurableMarker(103, 3)
            assert writer.snapshot == acknowledged
            await writer.close()

    asyncio.run(exercise())


def test_target_failure_latches_and_prevents_seal() -> None:
    asyncio.run(_test_target_failure_latches_and_prevents_seal())


async def _test_target_failure_latches_and_prevents_seal() -> None:
    target = FakeTarget(fail_append=True)
    async with _started(target, bytearray(RECORD_SIZE), expect_failure=True) as writer:
        writer.publish(RECORD_SIZE)

        with pytest.raises(WriterFailedError, match="target failed"):
            await writer.barrier()
        assert writer.state is WriterState.FAILED
        with pytest.raises(WriterFailedError, match="target failed"):
            await writer.seal("done")
        assert target.seal_calls == 0
        with pytest.raises(WriterFailedError, match="target failed"):
            await writer.close()
        with pytest.raises(WriterFailedError, match="target failed"):
            await writer.close()
        assert target.close_calls == 1


def test_writer_failure_sends_latched_raw_exception_to_debug_ring(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: list[tuple[str, BaseException, dict[str, object]]] = []

    def record(event: str, error: BaseException, **fields: object) -> None:
        captured.append((event, error, fields))

    monkeypatch.setattr(attempt_writer, "debug_exception", record)

    async def exercise() -> None:
        async with _started(FakeTarget(fail_append=True), bytearray(RECORD_SIZE), expect_failure=True) as writer:
            writer.publish(RECORD_SIZE)
            with pytest.raises(WriterFailedError, match="target failed"):
                await writer.barrier()
            with pytest.raises(WriterFailedError, match="target failed"):
                await writer.close()

    asyncio.run(exercise())

    event, error, fields = captured[0]
    assert event == "attempt_writer_failed"
    assert isinstance(error, OSError)
    assert fields == {
        "writer_state": "failed",
        "submitted_high_water": RECORD_SIZE,
        "written_high_water": 0,
    }


def test_close_is_idempotent_and_target_calls_stay_on_one_thread() -> None:
    asyncio.run(_test_close_is_idempotent_and_target_calls_stay_on_one_thread())


async def _test_close_is_idempotent_and_target_calls_stay_on_one_thread() -> None:
    target = FakeTarget()
    async with _started(target, bytearray(RECORD_SIZE)) as writer:
        writer.publish(RECORD_SIZE)

        assert await writer.close() == "closed"
        assert await writer.close() == "closed"
        assert target.close_calls == 1
        assert len(target.thread_ids) == 1
        assert writer.thread.ident not in {None, threading.get_ident()}
        assert not writer.thread.is_alive()


def test_close_timeout_is_reported_until_blocking_target_is_released() -> None:
    asyncio.run(_test_close_timeout_is_reported_until_blocking_target_is_released())


async def _test_close_timeout_is_reported_until_blocking_target_is_released() -> None:
    target = FakeTarget(block_close=True)
    async with _started(target, bytearray()) as writer:
        with pytest.raises(WriterShutdownTimeoutError):
            await writer.close(timeout=0.01)
        assert target.close_calls == 1
        assert writer.state is WriterState.CLOSING
        assert writer.thread.is_alive()
        target.release_close.set()
        assert await writer.close(timeout=1) == "closed"
        assert writer.state is WriterState.CLOSED
        assert not writer.thread.is_alive()


def test_close_deadline_includes_writer_thread_exit(monkeypatch: pytest.MonkeyPatch) -> None:
    async def exercise() -> None:
        target = FakeTarget()
        async with _started(target, bytearray()) as writer:
            worker_paused = threading.Event()
            release_worker = threading.Event()
            complete = writer._complete

            def pause_after_completion(
                future: asyncio.Future[object],
                result: object | None,
                error: BaseException | None,
            ) -> None:
                complete(future, result, error)
                worker_paused.set()
                release_worker.wait(5)

            monkeypatch.setattr(writer, "_complete", pause_after_completion)
            closing = asyncio.create_task(writer.close(timeout=0.5))
            try:
                assert await asyncio.to_thread(worker_paused.wait, 1)
                assert not closing.done()
                with pytest.raises(WriterShutdownTimeoutError):
                    await asyncio.wait_for(asyncio.shield(closing), timeout=1)
            finally:
                release_worker.set()
                monkeypatch.setattr(writer, "_complete", complete)
                await writer.close(timeout=1)
            if not closing.done():
                await closing

    asyncio.run(exercise())


def test_bounded_close_survives_repeated_cancellation_until_target_release() -> None:
    async def exercise() -> None:
        target = FakeTarget(block_close=True)
        async with _started(target, bytearray()) as writer:
            owner = asyncio.create_task(bounded(writer.close(timeout=3), 3))
            for _ in range(100):
                if target.close_calls == 1:
                    break
                await asyncio.sleep(0.001)
            assert target.close_calls == 1

            owner.cancel()
            await asyncio.sleep(0)
            owner.cancel()
            await asyncio.sleep(0)
            assert not owner.done()
            assert writer.thread.is_alive()

            target.release_close.set()
            with pytest.raises(asyncio.CancelledError):
                await owner
            assert not writer.thread.is_alive()
            assert writer.state is WriterState.CLOSED

    asyncio.run(exercise())


def test_cancelled_close_keeps_positive_remaining_shutdown_budget() -> None:
    async def exercise() -> None:
        target = FakeTarget(block_close=True)
        async with _started(target, bytearray()) as writer:
            closing = asyncio.create_task(writer.close(timeout=0.9))
            try:
                assert await asyncio.to_thread(target.close_started.wait, 1)
                closing.cancel()
                target.release_close.set()
                with pytest.raises(asyncio.CancelledError):
                    await asyncio.wait_for(closing, timeout=1)
                assert target.close_calls == 1
                assert not writer.thread.is_alive()
                assert writer.state is WriterState.CLOSED
            finally:
                target.release_close.set()
                if not closing.done():
                    closing.cancel()
                await asyncio.gather(closing, return_exceptions=True)
                await asyncio.to_thread(writer.thread.join, 1)

    asyncio.run(exercise())


@pytest.mark.parametrize("after_deadline", [0.0, 0.25])
def test_cancelled_close_reports_expired_remaining_budget(
    after_deadline: float, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def exercise() -> None:
        target = FakeTarget(block_close=True)
        async with _started(target, bytearray()) as writer:
            loop = asyncio.get_running_loop()
            base_time = loop.time()
            fake_time = [base_time]
            closing: asyncio.Task[object] | None = None
            try:
                with monkeypatch.context() as patch:
                    patch.setattr(loop, "time", lambda: fake_time[0])
                    closing = asyncio.create_task(writer.close(timeout=0.5))
                    assert await asyncio.to_thread(target.close_started.wait, 1)
                    target.release_close.set()
                    writer.thread.join(1)
                    assert not writer.thread.is_alive()
                    fake_time[0] = base_time + 0.5 + after_deadline
                    closing.cancel()
                    with pytest.raises(WriterShutdownTimeoutError):
                        await closing
            finally:
                target.release_close.set()
                if closing is not None and not closing.done():
                    closing.cancel()
                if closing is not None:
                    await asyncio.wait_for(asyncio.gather(closing, return_exceptions=True), timeout=1)
                await asyncio.to_thread(writer.thread.join, 1)
                assert target.close_calls == 1

    asyncio.run(exercise())


def test_close_deadline_rejects_extra_join_turn(monkeypatch: pytest.MonkeyPatch) -> None:
    async def exercise() -> None:
        target = FakeTarget()
        async with _started(target, bytearray()) as writer:
            loop = asyncio.get_running_loop()
            base_time = loop.time()
            fake_time = [base_time]
            release_worker = threading.Event()
            worker_paused = threading.Event()
            complete = writer._complete
            join_until = writer._join_until
            closing: asyncio.Task[object] | None = None

            def pause_after_completion(
                future: asyncio.Future[object],
                result: object | None,
                error: BaseException | None,
            ) -> None:
                complete(future, result, error)
                worker_paused.set()
                release_worker.wait(5)

            def release_and_join_worker() -> None:
                release_worker.set()
                writer.thread.join(1)

            async def join_at_deadline(
                deadline: float,
                *,
                result: asyncio.Future[object] | None = None,
            ) -> object:
                async def reject_poll_after_deadline(delay: float) -> None:
                    del delay
                    assert loop.time() < deadline, "writer must not poll after its close deadline"

                with monkeypatch.context() as clock_patch:
                    clock_patch.setattr(loop, "time", lambda: deadline)
                    clock_patch.setattr(attempt_writer.asyncio, "sleep", reject_poll_after_deadline)
                    loop.call_soon(release_and_join_worker)
                    return await join_until(deadline, result=result)

            try:
                with monkeypatch.context() as patch:
                    patch.setattr(loop, "time", lambda: fake_time[0])
                    patch.setattr(writer, "_complete", pause_after_completion)
                    patch.setattr(writer, "_join_until", join_at_deadline)
                    closing = asyncio.create_task(writer.close(timeout=0.5))
                    assert await asyncio.to_thread(worker_paused.wait, 1)
                    with pytest.raises(WriterShutdownTimeoutError):
                        await closing
            finally:
                release_worker.set()
                if closing is not None and not closing.done():
                    closing.cancel()
                if closing is not None:
                    await asyncio.wait_for(asyncio.gather(closing, return_exceptions=True), timeout=1)
                await asyncio.to_thread(writer.thread.join, 1)
                assert not writer.thread.is_alive()
                await writer.close(timeout=1)

    asyncio.run(exercise())


def test_cancelled_seal_remains_admitted_and_converges_to_one_target_call() -> None:
    async def exercise() -> None:
        target = FakeTarget(block_seal=True)
        async with _started(target, bytearray(RECORD_SIZE)) as writer:
            sealing = asyncio.create_task(writer.seal("done"))
            assert await asyncio.to_thread(target.seal_started.wait, 1)
            sealing.cancel()
            with pytest.raises(asyncio.CancelledError):
                await sealing
            assert writer.state is WriterState.SEALING
            with pytest.raises(WriterClosedError, match="seal is already pending"):
                await writer.seal("again")
            target.release_seal.set()
            assert await writer.await_seal_result() == "sealed"
            assert await writer.await_seal_result() == "sealed"
            await writer.close()
            assert target.seal_calls == 1

    asyncio.run(exercise())


def test_cancelled_start_remains_admitted_and_second_start_is_idempotent() -> None:
    async def exercise() -> None:
        target = FakeTarget(block_prepare=True)
        async with _owned_writer(target, bytearray()) as writer:
            starting = asyncio.create_task(writer.start())
            assert await asyncio.to_thread(target.prepare_started.wait, 1)
            starting.cancel()
            with pytest.raises(asyncio.CancelledError):
                await starting
            await writer.start()
            target.release_prepare.set()
            for _ in range(20):
                if writer.state is WriterState.STARTED:
                    break
                await asyncio.sleep(0.005)
            assert writer.state is WriterState.STARTED
            await writer.close()
            assert [call[0] for call in target.calls].count("prepare") == 1

    asyncio.run(exercise())


def test_close_after_queued_read_begin_drains_when_worker_reports_success() -> None:
    async def exercise() -> None:
        target = FakeTarget(block_read_begin=True)
        async with _owned_writer(target, bytearray(b"x" * RECORD_SIZE), config=WriterConfig(chunk_records=1)) as writer:
            await writer.start()
            read_begin = writer.submit_read_begin("begin")
            assert await asyncio.to_thread(target.read_begin_started.wait, 1)
            writer.publish(RECORD_SIZE)
            closing = asyncio.create_task(writer.close())
            target.release_read_begin.set()
            assert await read_begin == "begin"
            assert await closing == "closed"
            names = [call[0] for call in target.calls]
            assert names.index("append") < names.index("close")

    asyncio.run(exercise())


def test_failed_submit_read_begin_returns_an_already_failed_future() -> None:
    async def exercise() -> None:
        target = FakeTarget(fail_read_begin=True)
        async with _owned_writer(target, bytearray(), expect_failure=True) as writer:
            await writer.start()
            first = writer.submit_read_begin("begin")
            with pytest.raises(WriterFailedError, match="target failed"):
                await first
            failed = writer.submit_read_begin("again")
            assert failed.done()
            with pytest.raises(WriterFailedError, match="target failed"):
                await failed
            with pytest.raises(WriterFailedError, match="target failed"):
                await writer.close()

    asyncio.run(exercise())


def test_queue_rejection_does_not_commit_finalizing_state() -> None:
    async def exercise() -> None:
        target = FakeTarget(block_append=True)
        async with _owned_writer(target, bytearray(RECORD_SIZE), config=WriterConfig(max_control_commands=1)) as writer:
            await writer.start()
            await writer.read_begin("begin")
            writer.publish(RECORD_SIZE)
            assert await asyncio.to_thread(target.append_started.wait, 1)
            checkpoint = asyncio.create_task(writer.checkpoint())
            await asyncio.sleep(0)
            with pytest.raises(WriterQueueFullError):
                await writer.seal("done")
            assert writer.state is WriterState.STARTED
            target.release_append.set()
            await checkpoint
            await writer.close()

    asyncio.run(exercise())


def test_pre_start_publication_waits_for_successful_read_begin() -> None:
    async def exercise() -> None:
        target = FakeTarget()
        async with _owned_writer(target, bytearray(b"x" * RECORD_SIZE), config=WriterConfig(chunk_records=1)) as writer:
            assert writer.publish(RECORD_SIZE)
            await writer.start()
            await asyncio.sleep(0)
            assert not [call for call in target.calls if call[0] == "append"]
            await writer.read_begin("begin")
            await writer.barrier()
            assert ("append", 0, b"x" * RECORD_SIZE) in target.calls
            await writer.close()

    asyncio.run(exercise())


def test_finalizing_rejects_controls_and_data_before_they_reach_the_target() -> None:
    async def exercise() -> None:
        target = FakeTarget(block_seal=True)
        async with _started(target, bytearray(RECORD_SIZE)) as writer:
            sealing = asyncio.create_task(writer.seal("done"))
            assert await asyncio.to_thread(target.seal_started.wait, 1)
            with pytest.raises(WriterClosedError, match="sealing or sealed"):
                writer.publish(RECORD_SIZE)
            with pytest.raises(WriterClosedError, match="sealing or sealed"):
                await writer.checkpoint()
            with pytest.raises(WriterClosedError, match="sealing or sealed"):
                await writer.prepare_leg(1, 1)
            with pytest.raises(WriterClosedError, match="sealing or sealed"):
                writer.submit_read_begin("again")
            target.release_seal.set()
            assert await sealing == "sealed"
            await writer.close()

    asyncio.run(exercise())


def test_repeated_start_is_a_noop_through_finalization_and_normal_close() -> None:
    async def exercise() -> None:
        target = FakeTarget(block_seal=True)
        async with _started(target, bytearray()) as writer:
            sealing = asyncio.create_task(writer.seal("done"))
            assert await asyncio.to_thread(target.seal_started.wait, 1)
            await writer.start()
            target.release_seal.set()
            assert await sealing == "sealed"
            await writer.start()
            assert await writer.close() == "closed"
            await writer.start()
            assert [call[0] for call in target.calls].count("prepare") == 1

    asyncio.run(exercise())


def test_repeated_start_preserves_chained_failure_while_closing_and_closed() -> None:
    async def exercise() -> None:
        target = FakeTarget(block_read_begin=True, block_close=True)
        async with _owned_writer(target, bytearray(), expect_failure=True) as writer:
            await writer.start()
            read_begin = writer.submit_read_begin("begin")
            assert await asyncio.to_thread(target.read_begin_started.wait, 1)
            closing = asyncio.create_task(writer.close())
            target.fail_read_begin_after_block = True
            target.release_read_begin.set()
            with pytest.raises(WriterFailedError, match="target failed"):
                await read_begin
            with pytest.raises(WriterFailedError, match="target failed") as closing_start:
                await writer.start()
            assert isinstance(closing_start.value.__cause__, OSError)
            target.release_close.set()
            with pytest.raises(WriterFailedError, match="target failed"):
                await closing
            with pytest.raises(WriterFailedError, match="target failed") as closed_start:
                await writer.start()
            assert isinstance(closed_start.value.__cause__, OSError)

    asyncio.run(exercise())


def test_start_after_close_before_admission_keeps_closed_error() -> None:
    async def exercise() -> None:
        target = FakeTarget()
        async with _owned_writer(target, bytearray()) as writer:
            assert await writer.close() == "closed"
            with pytest.raises(WriterClosedError, match="writer is closed"):
                await writer.start()
            assert not [call for call in target.calls if call[0] == "prepare"]

    asyncio.run(exercise())


def test_start_after_failed_close_before_admission_keeps_closed_error() -> None:
    async def exercise() -> None:
        target = FakeTarget(fail_close=True)
        async with _owned_writer(target, bytearray(), expect_failure=True) as writer:
            with pytest.raises(WriterFailedError, match="target failed"):
                await writer.close()
            with pytest.raises(WriterClosedError, match="writer is closed"):
                await writer.start()
            assert not [call for call in target.calls if call[0] == "prepare"]

    asyncio.run(exercise())


def test_worker_success_events_are_explicit_for_every_writer_command() -> None:
    async def exercise() -> None:
        async with _owned_writer(FakeTarget(), bytearray()) as writer:
            command_events = (
                (attempt_writer.PrepareCommand(), attempt_writer.PrepareSucceeded),
                (attempt_writer.PrepareLegCommand(1, 1), attempt_writer.LegSucceeded),
                (attempt_writer.ReadBeginCommand("begin"), attempt_writer.ReadBeginSucceeded),
                (attempt_writer.CheckpointCommand(0), attempt_writer.CheckpointSucceeded),
                (attempt_writer.SealCommand(0, "done"), attempt_writer.FinalizeSucceeded),
                (attempt_writer.PublishPrefixCommand(0), attempt_writer.FinalizeSucceeded),
                (attempt_writer.CloseCommand(0), attempt_writer.CloseSucceeded),
            )

            for command, event_type in command_events:
                assert isinstance(writer._success_event(command), event_type)
            await writer.close()

    asyncio.run(exercise())


def test_unlatchable_failure_result_completes_pending_future_and_preserves_worker_cleanup(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def exercise() -> None:
        target = FakeTarget()
        async with _owned_writer(target, bytearray(), expect_failure=True) as writer:
            error = OSError("impossible machine result")

            def discard_failure(
                state: attempt_writer_machine.AttemptWriterMachineState,
                _event: attempt_writer_machine.AttemptWriterMachineEvent,
            ) -> attempt_writer_machine.TransitionResult:
                return attempt_writer_machine.TransitionResult(state, attempt_writer_machine.Ignore())

            with monkeypatch.context() as patch:
                patch.setattr(attempt_writer, "transition", discard_failure)
                with writer._lock:
                    pending = writer._new_future_locked()
                    writer._commands.append(attempt_writer._Pending(attempt_writer.PrepareCommand(), pending))
                assert writer._record_failure(error) is error
                assert writer.failure is error
                with pytest.raises(WriterFailedError, match="target failed"):
                    await pending
            with pytest.raises(WriterFailedError, match="target failed"):
                await writer.close()
            assert not writer.thread.is_alive()

    asyncio.run(exercise())


def test_failure_lifecycle_idle_append_error_keeps_close_owned_and_failed_state_visible() -> None:
    async def exercise() -> None:
        target = FakeTarget(
            block_append=True,
            fail_append_after_block=True,
            block_close=True,
        )
        async with _started(target, bytearray(RECORD_SIZE), expect_failure=True) as writer:
            allow_close = asyncio.Event()

            async def close_after_barrier() -> object:
                await allow_close.wait()
                return await writer.close(timeout=1)

            closing = asyncio.create_task(close_after_barrier())
            close_outcome: object | None = None
            close_started = False
            try:
                writer.publish(RECORD_SIZE)
                assert await asyncio.to_thread(target.append_started.wait, 1)
                allow_close.set()
                # Yield once so close is admitted while the append is still blocked.
                await asyncio.sleep(0)
                target.release_append.set()
                close_started = await asyncio.to_thread(target.close_started.wait, 1)
                assert close_started
                assert isinstance(writer.failure, OSError)
                assert writer.state is WriterState.FAILED
            finally:
                allow_close.set()
                await asyncio.sleep(0)
                target.release_append.set()
                target.release_close.set()
                close_outcome = (await asyncio.gather(closing, return_exceptions=True))[0]
                await asyncio.to_thread(writer.thread.join, 1)

            assert isinstance(close_outcome, WriterFailedError)
            assert target.close_calls == 1
            assert not writer.thread.is_alive()

    asyncio.run(exercise())
