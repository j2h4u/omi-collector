"""Focused concurrency and failure tests for the standalone attempt writer."""

from __future__ import annotations

import asyncio
import threading
import time
from dataclasses import dataclass, field

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
from omi_collector.capture.domain.ring_protocol import RECORD_SIZE
from omi_collector.config import DEFAULT_CONFIG, WriterConfig


@dataclass(frozen=True)
class DurableMarker:
    next_sequence: int
    record_count: int


@dataclass
class FakeTarget:
    calls: list[tuple[str, int, bytes | object]] = field(default_factory=list)
    thread_ids: set[int] = field(default_factory=set)
    append_started: threading.Event = field(default_factory=threading.Event)
    release_append: threading.Event = field(default_factory=threading.Event)
    block_append: bool = False
    fail_append: bool = False
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
    seal_calls: int = 0
    close_calls: int = 0
    checkpoint_result: object = "checkpointed"
    append_readonly: list[bool] = field(default_factory=list)
    append_offsets: list[int] = field(default_factory=list)

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
        return "prepared"

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
        self.append_started.set()
        if self.fail_append:
            raise OSError("disk full")
        if self.block_append:
            self.release_append.wait(5)
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
        if self.block_close:
            self.release_close.wait(5)
        if self.fail_close:
            raise OSError("close failed")
        return "closed"


async def _started(target: FakeTarget, source: bytes | bytearray, *, chunk_size: int = RECORD_SIZE) -> AttemptWriter:
    writer = AttemptWriter(target, source, chunk_size=chunk_size)
    await writer.start()
    await writer.read_begin("begin")
    return writer


def test_arena_is_shared_and_data_waits_for_read_begin() -> None:
    asyncio.run(_test_arena_is_shared_and_data_waits_for_read_begin())


def test_writer_defaults_come_from_runtime_config() -> None:
    writer = AttemptWriter(FakeTarget(), bytearray(RECORD_SIZE))
    assert writer._max_controls == DEFAULT_CONFIG.writer.max_control_commands
    assert writer._chunk_size == DEFAULT_CONFIG.writer.chunk_records * RECORD_SIZE
    asyncio.run(writer.close())


def test_writer_config_controls_defaults_and_explicit_overrides() -> None:
    config = WriterConfig(chunk_records=2, max_control_commands=3, join_poll_seconds=0.123)
    writer = AttemptWriter(FakeTarget(), bytearray(RECORD_SIZE * 5), config=config)
    assert writer._config is config
    assert writer._max_controls == 3
    assert writer._chunk_size == RECORD_SIZE * 2
    assert writer._config.join_poll_seconds == 0.123
    asyncio.run(writer.close())

    overridden = AttemptWriter(
        FakeTarget(),
        bytearray(RECORD_SIZE * 5),
        config=config,
        max_control_commands=7,
        chunk_size=RECORD_SIZE * 4,
    )
    assert overridden._max_controls == 7
    assert overridden._chunk_size == RECORD_SIZE * 4
    asyncio.run(overridden.close())


def test_writer_config_controls_control_capacity() -> None:
    async def exercise() -> None:
        target = FakeTarget(block_read_begin=True)
        writer = AttemptWriter(
            target,
            bytearray(RECORD_SIZE),
            config=WriterConfig(max_control_commands=1),
        )
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
        writer = AttemptWriter(target, bytearray(), config=config)
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
    writer = AttemptWriter(target, arena, chunk_size=RECORD_SIZE)
    assert writer.state is WriterState.CREATED
    assert writer.source_capacity == RECORD_SIZE * 2
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
    writer = AttemptWriter(target, bytearray(RECORD_SIZE), chunk_size=RECORD_SIZE)
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
    writer = AttemptWriter(target, bytearray(RECORD_SIZE * 2), chunk_size=RECORD_SIZE)
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
    writer = await _started(target, bytearray(RECORD_SIZE))
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
    writer = AttemptWriter(target, bytearray(RECORD_SIZE), chunk_size=RECORD_SIZE)
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
    writer = await _started(target, bytearray(RECORD_SIZE * 10))
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


def test_chunk_and_publish_high_water_are_record_aligned() -> None:
    with pytest.raises(ValueError, match="chunk_size"):
        AttemptWriter(FakeTarget(), bytearray(RECORD_SIZE), chunk_size=RECORD_SIZE + 1)

    async def exercise() -> None:
        writer = AttemptWriter(FakeTarget(), bytearray(RECORD_SIZE), chunk_size=RECORD_SIZE)
        with pytest.raises(ValueError, match="record-aligned"):
            writer.publish(1)
        await writer.close()

    asyncio.run(exercise())


def test_high_water_coalesces_data_and_barrier_orders_writes() -> None:
    asyncio.run(_test_high_water_coalesces_data_and_barrier_orders_writes())


async def _test_high_water_coalesces_data_and_barrier_orders_writes() -> None:
    target = FakeTarget()
    writer = await _started(target, bytearray(RECORD_SIZE * 10), chunk_size=RECORD_SIZE * 3)

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


def test_snapshot_records_durable_checkpoint_ack_without_target_inspection() -> None:
    asyncio.run(_test_snapshot_records_durable_checkpoint_ack_without_target_inspection())


async def _test_snapshot_records_durable_checkpoint_ack_without_target_inspection() -> None:
    target = FakeTarget(checkpoint_result=DurableMarker(102, 2))
    writer = await _started(target, bytearray(RECORD_SIZE * 2))
    writer.publish(RECORD_SIZE * 2)
    await writer.checkpoint()
    assert writer.snapshot.submitted == RECORD_SIZE * 2
    assert writer.snapshot.written == RECORD_SIZE * 2
    assert writer.snapshot.durable_next_sequence == 102
    assert writer.snapshot.durable_record_count == 2
    await writer.close()


def test_target_failure_latches_and_prevents_seal() -> None:
    asyncio.run(_test_target_failure_latches_and_prevents_seal())


async def _test_target_failure_latches_and_prevents_seal() -> None:
    target = FakeTarget(fail_append=True)
    writer = await _started(target, bytearray(RECORD_SIZE))
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
        writer = await _started(FakeTarget(fail_append=True), bytearray(RECORD_SIZE))
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
    writer = await _started(target, bytearray(RECORD_SIZE))
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
    writer = await _started(target, bytearray())

    with pytest.raises(WriterShutdownTimeoutError):
        await writer.close(timeout=0.01)
    assert target.close_calls == 1
    assert writer.state is WriterState.CLOSING
    assert writer.thread.is_alive()
    target.release_close.set()
    assert await writer.close(timeout=1) == "closed"
    assert writer.state is WriterState.CLOSED
    assert not writer.thread.is_alive()


def test_cancelled_seal_remains_admitted_and_converges_to_one_target_call() -> None:
    async def exercise() -> None:
        target = FakeTarget(block_seal=True)
        writer = await _started(target, bytearray(RECORD_SIZE))
        sealing = asyncio.create_task(writer.seal("done"))
        assert await asyncio.to_thread(target.seal_started.wait, 1)
        sealing.cancel()
        with pytest.raises(asyncio.CancelledError):
            await sealing
        assert writer.state is WriterState.SEALING
        with pytest.raises(WriterClosedError, match="seal is already pending"):
            await writer.seal("again")
        target.release_seal.set()
        await asyncio.sleep(0)
        await writer.close()
        assert target.seal_calls == 1

    asyncio.run(exercise())


def test_cancelled_start_remains_admitted_and_second_start_is_idempotent() -> None:
    async def exercise() -> None:
        target = FakeTarget(block_prepare=True)
        writer = AttemptWriter(target, bytearray())
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
        writer = AttemptWriter(target, bytearray(b"x" * RECORD_SIZE), chunk_size=RECORD_SIZE)
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
        writer = AttemptWriter(target, bytearray())
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
        writer = AttemptWriter(target, bytearray(RECORD_SIZE), max_control_commands=1)
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
        writer = AttemptWriter(target, bytearray(b"x" * RECORD_SIZE), chunk_size=RECORD_SIZE)
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
        writer = await _started(target, bytearray(RECORD_SIZE))
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
        writer = await _started(target, bytearray())
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
        writer = AttemptWriter(target, bytearray())
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
        writer = AttemptWriter(target, bytearray())
        assert await writer.close() == "closed"
        with pytest.raises(WriterClosedError, match="writer is closed"):
            await writer.start()
        assert not [call for call in target.calls if call[0] == "prepare"]

    asyncio.run(exercise())


def test_start_after_failed_close_before_admission_keeps_closed_error() -> None:
    async def exercise() -> None:
        target = FakeTarget(fail_close=True)
        writer = AttemptWriter(target, bytearray())
        with pytest.raises(WriterFailedError, match="target failed"):
            await writer.close()
        with pytest.raises(WriterClosedError, match="writer is closed"):
            await writer.start()
        assert not [call for call in target.calls if call[0] == "prepare"]

    asyncio.run(exercise())


def test_worker_success_events_are_explicit_for_every_writer_command() -> None:
    async def exercise() -> None:
        writer = AttemptWriter(FakeTarget(), bytearray())
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
        writer = AttemptWriter(target, bytearray())
        error = OSError("impossible machine result")

        def discard_failure(
            state: attempt_writer_machine.AttemptWriterMachineState,
            _event: attempt_writer_machine.AttemptWriterMachineEvent,
        ) -> attempt_writer_machine.TransitionResult:
            return attempt_writer_machine.TransitionResult(state, attempt_writer_machine.Ignore())

        original_transition = attempt_writer.transition
        monkeypatch.setattr(attempt_writer, "transition", discard_failure)
        with writer._lock:
            pending = writer._new_future_locked()
            writer._commands.append(attempt_writer._Pending(attempt_writer.PrepareCommand(), pending))
        assert writer._record_failure(error) is error
        assert writer.failure is error
        with pytest.raises(WriterFailedError, match="target failed"):
            await pending
        monkeypatch.setattr(attempt_writer, "transition", original_transition)
        with pytest.raises(WriterFailedError, match="target failed"):
            await writer.close()
        assert not writer.thread.is_alive()

    asyncio.run(exercise())
