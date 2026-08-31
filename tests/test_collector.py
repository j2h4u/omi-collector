from __future__ import annotations

import asyncio
import threading
from collections.abc import AsyncIterator, Iterable
from dataclasses import dataclass, field
from struct import pack

import pytest

from omi_collector.capture.adapters.attempt_writer import AttemptWriter
from omi_collector.capture.application.collector import (
    ProgressEvent,
    ProgressMailbox,
    ReadLegOptions,
    TransferInterruptedError,
    read_leg,
)
from omi_collector.capture.domain.ring_protocol import (
    RECORD_SIZE,
    RingStatus,
    encode_read_command,
    encode_stop_command,
)
from omi_collector.capture.domain.transfer_arena import TransferArena


def _record(value: int) -> bytes:
    return pack(">I", value) + bytes((value,)) * (RECORD_SIZE - 4)


def _begin(start: int, count: int) -> bytes:
    return b"\x05" + start.to_bytes(8, "big") + count.to_bytes(4, "big")


def _data(data: bytes) -> bytes:
    return b"\x03" + data


def _done(next_sequence: int) -> bytes:
    return b"\x04\x00" + next_sequence.to_bytes(8, "big")


class BurstSession:
    def __init__(self, notifications: Iterable[bytes], *, start: int = 10, count: int = 2, delay: float = 0.0) -> None:
        self.notifications_to_emit = tuple(notifications)
        self.start = start
        self.count = count
        self.delay = delay
        self.writes: list[bytes] = []
        self.consumed = 0

    def notifications(self) -> AsyncIterator[bytes]:
        async def stream() -> AsyncIterator[bytes]:
            for payload in self.notifications_to_emit:
                await asyncio.sleep(self.delay)
                self.consumed += 1
                yield payload

        return stream()

    async def read_status(self) -> RingStatus:
        return RingStatus(0, 0, 0, 1)

    async def close(self) -> None:
        return

    async def write_control(self, payload: bytes) -> None:
        self.writes.append(payload)
        if payload == encode_stop_command():
            return
        if payload != encode_read_command(self.start, self.count):
            raise AssertionError(f"unexpected command: {payload!r}")


class DelayedSession(BurstSession):
    def __init__(self, notifications: Iterable[bytes], delays: tuple[float, ...]) -> None:
        super().__init__(notifications)
        self.delays = delays

    def notifications(self) -> AsyncIterator[bytes]:
        async def stream() -> AsyncIterator[bytes]:
            for delay, payload in zip(self.delays, self.notifications_to_emit, strict=True):
                await asyncio.sleep(delay)
                self.consumed += 1
                yield payload

        return stream()


class BlockingReadSession(BurstSession):
    def __init__(self) -> None:
        super().__init__(())
        self.read_started = asyncio.Event()
        self.release_read = asyncio.Event()

    async def write_control(self, payload: bytes) -> None:
        self.writes.append(payload)
        if payload == encode_stop_command():
            return
        if payload != encode_read_command(self.start, self.count):
            raise AssertionError(f"unexpected command: {payload!r}")
        self.read_started.set()
        await self.release_read.wait()


@dataclass
class FakeWriter:
    calls: list[str] = field(default_factory=list)
    published_high_water: int = 0
    written_high_water: int = 0
    fail_barrier: bool = False
    target_threads: list[int] = field(default_factory=list)
    expected_start: int = 10
    expected_count: int = 2

    async def start(self) -> None:
        self.calls.append("start")

    async def prepare_leg(self, start_sequence: int, record_count: int) -> None:
        assert (start_sequence, record_count) == (self.expected_start, self.expected_count)
        self.calls.append("prepare")

    def submit_read_begin(self, notice: object) -> asyncio.Future[object]:
        del notice
        self.calls.append("read_begin")
        future: asyncio.Future[object] = asyncio.get_running_loop().create_future()
        future.set_result(None)
        return future

    def publish(self, high_water: int) -> None:
        self.calls.append(f"publish:{high_water}")
        self.published_high_water = high_water

    async def barrier(self) -> str:
        self.calls.append("barrier")
        if self.fail_barrier:
            raise OSError("blocked target failed")
        await asyncio.to_thread(self._target_write)
        return "checkpointed"

    def _target_write(self) -> None:
        self.target_threads.append(threading.get_ident())
        self.written_high_water = self.published_high_water


class PendingReadBeginWriter(FakeWriter):
    def submit_read_begin(self, notice: object) -> asyncio.Future[object]:
        del notice
        self.calls.append("read_begin")
        return asyncio.get_running_loop().create_future()


class PendingBarrierWriter(FakeWriter):
    def __init__(self) -> None:
        super().__init__()
        self.release_barrier = asyncio.Event()

    async def barrier(self) -> str:
        self.calls.append("barrier")
        await self.release_barrier.wait()
        return "checkpointed"


@dataclass
class BlockingWriterTarget:
    append_started: threading.Event = field(default_factory=threading.Event)
    release_append: threading.Event = field(default_factory=threading.Event)
    calls: list[str] = field(default_factory=list)
    append_threads: list[int] = field(default_factory=list)

    def prepare(self) -> object:
        self.calls.append("prepare")
        return None

    def prepare_leg(self, start_sequence: int, record_count: int) -> object:
        del start_sequence, record_count
        self.calls.append("prepare_leg")
        return None

    def read_begin(self, notice: object) -> object:
        del notice
        self.calls.append("read_begin")
        return None

    def append_chunk(self, offset: int, chunk: memoryview) -> object:
        del offset, chunk
        self.calls.append("append")
        self.append_threads.append(threading.get_ident())
        if len(self.append_threads) == 1:
            self.append_started.set()
            if not self.release_append.wait(5):
                raise TimeoutError("test target was not released")
        return None

    def checkpoint(self) -> None:
        self.calls.append("checkpoint")

    def seal(self, done_notice: object) -> object:
        del done_notice
        self.calls.append("seal")
        return None

    def publish_prefix(self) -> object:
        self.calls.append("publish_prefix")
        return None

    def close(self) -> None:
        self.calls.append("close")


def test_burst_reaches_arena_while_heartbeat_runs_and_disk_is_after_done() -> None:
    async def scenario() -> None:
        first, second = _record(1), _record(2)
        session = BurstSession((_begin(10, 2), _data((first + second)[:443]), _data((first + second)[443:]), _done(12)))
        writer = FakeWriter()
        arena = TransferArena(10, 2, max_bytes=2 * RECORD_SIZE)
        heartbeat = 0

        async def tick() -> None:
            nonlocal heartbeat
            for _ in range(20):
                await asyncio.sleep(0)
                heartbeat += 1

        progress_calls = 0

        def progress(_event: object) -> None:
            nonlocal progress_calls
            progress_calls += 1

        task = asyncio.create_task(tick())
        result = await read_leg(session, arena, writer, 10, 2, ReadLegOptions(1, progress))
        await task

        assert arena.received_bytes == 2 * RECORD_SIZE
        assert result.submitted_bytes == 2 * RECORD_SIZE
        assert result.written_bytes == 2 * RECORD_SIZE
        assert heartbeat > 0
        assert progress_calls == 0
        assert writer.calls[:3] == ["start", "prepare", "read_begin"]
        assert writer.calls[-1] == "barrier"
        assert writer.target_threads and writer.target_threads[0] != threading.get_ident()
        assert session.writes == [encode_read_command(10, 2)]

    asyncio.run(scenario())


def test_read_mailbox_preserves_intermediate_and_terminal_progress_on_success_and_interrupt() -> None:
    async def scenario() -> None:
        record = _record(1)
        published: list[tuple[int, bool]] = []

        class RecordingMailbox(ProgressMailbox):
            def publish(self, event: ProgressEvent, *, terminal: bool = False) -> None:
                published.append((event.records_completed, terminal))
                super().publish(event, terminal=terminal)

        mailbox = RecordingMailbox()
        session = DelayedSession(
            (_begin(10, 2), _data(record), _data(record), _done(12)),
            (0.0, 0.0, 0.02, 0.02),
        )
        writer = FakeWriter()
        arena = TransferArena(10, 2, max_bytes=2 * RECORD_SIZE)
        task = asyncio.create_task(read_leg(session, arena, writer, 10, 2, ReadLegOptions(1, progress_mailbox=mailbox)))

        intermediate = await mailbox.wait_for_change(0)
        assert intermediate.event is not None
        assert intermediate.event.records_completed == 1
        assert not intermediate.terminal
        await task
        assert mailbox.snapshot.terminal
        assert mailbox.snapshot.revision > intermediate.revision
        assert mailbox.snapshot.event is not None
        assert mailbox.snapshot.event.records_completed == 2
        assert published == [(1, False), (2, True)]

        interrupted = ProgressMailbox()
        broken = BurstSession((_begin(10, 2), _data(record), _done(12)))
        with pytest.raises(TransferInterruptedError):
            await read_leg(
                broken,
                TransferArena(10, 2, max_bytes=2 * RECORD_SIZE),
                FakeWriter(),
                10,
                2,
                ReadLegOptions(1, progress_mailbox=interrupted),
            )
        assert interrupted.snapshot.terminal
        assert interrupted.snapshot.event is not None
        assert interrupted.snapshot.event.records_completed == 1

    asyncio.run(scenario())


def test_read_leg_timeout_is_reset_by_continuous_notifications() -> None:
    async def scenario() -> None:
        record = _record(1)
        notifications = (_begin(10, 2), _data(record), _data(record), _done(12))
        session = DelayedSession(notifications, (0.02, 0.02, 0.02, 0.02))
        writer = FakeWriter()
        arena = TransferArena(10, 2, max_bytes=2 * RECORD_SIZE)

        result = await read_leg(session, arena, writer, 10, 2, ReadLegOptions(0.03))

        assert result.received_bytes == 2 * RECORD_SIZE

    asyncio.run(scenario())


def test_read_leg_timeout_still_fails_after_idle_gap() -> None:
    async def scenario() -> None:
        record = _record(1)
        notifications = (_begin(10, 2), _data(record), _data(record), _done(12))
        session = DelayedSession(notifications, (0.0, 0.04, 0.0, 0.0))
        writer = FakeWriter()
        arena = TransferArena(10, 2, max_bytes=2 * RECORD_SIZE)

        with pytest.raises(TransferInterruptedError, match="timed out during READ"):
            await read_leg(session, arena, writer, 10, 2, ReadLegOptions(0.03))

    asyncio.run(scenario())


def test_read_control_write_timeout_stops_and_reports_counters() -> None:
    async def scenario() -> None:
        session = BlockingReadSession()
        writer = FakeWriter()
        arena = TransferArena(10, 2, max_bytes=2 * RECORD_SIZE)

        with pytest.raises(TransferInterruptedError, match="timed out during READ") as caught:
            await asyncio.wait_for(
                read_leg(session, arena, writer, 10, 2, ReadLegOptions(0.02)),
                1,
            )

        assert caught.value.received_bytes == 0
        assert session.writes == [encode_read_command(10, 2), encode_stop_command()]

    asyncio.run(scenario())


def test_read_begin_wait_timeout_stops_and_reports_counters() -> None:
    async def scenario() -> None:
        record = _record(1)
        session = BurstSession((_begin(10, 2), _data(record), _data(record), _done(12)))
        writer = PendingReadBeginWriter()
        arena = TransferArena(10, 2, max_bytes=2 * RECORD_SIZE)

        with pytest.raises(TransferInterruptedError, match="timed out waiting for writer after READ") as caught:
            await asyncio.wait_for(
                read_leg(session, arena, writer, 10, 2, ReadLegOptions(0.02)),
                1,
            )

        assert caught.value.received_records == 2
        assert session.writes == [encode_read_command(10, 2), encode_stop_command()]

    asyncio.run(scenario())


def test_writer_barrier_timeout_stops_and_reports_counters() -> None:
    async def scenario() -> None:
        record = _record(1)
        session = BurstSession((_begin(10, 2), _data(record), _data(record), _done(12)))
        writer = PendingBarrierWriter()
        arena = TransferArena(10, 2, max_bytes=2 * RECORD_SIZE)

        with pytest.raises(TransferInterruptedError, match="timed out waiting for writer after READ") as caught:
            await asyncio.wait_for(
                read_leg(session, arena, writer, 10, 2, ReadLegOptions(0.02)),
                1,
            )

        assert caught.value.received_records == 2
        assert session.writes == [encode_read_command(10, 2), encode_stop_command()]

    asyncio.run(scenario())


def test_read_leg_done_validates_current_continuation_leg() -> None:
    async def scenario() -> None:
        first, second = _record(1), _record(2)
        first_writer = FakeWriter(expected_count=1)
        arena = TransferArena(10, 2, max_bytes=2 * RECORD_SIZE)

        first_session = BurstSession((_begin(10, 1), _data(first), _done(11)), start=10, count=1)
        await read_leg(first_session, arena, first_writer, 10, 1, ReadLegOptions(1))

        continuation_session = BurstSession((_begin(11, 1), _data(second), _done(12)), start=11, count=1)
        continuation_writer = FakeWriter(expected_start=11, expected_count=1)
        result = await read_leg(continuation_session, arena, continuation_writer, 11, 1, ReadLegOptions(1))

        assert result.received_bytes == 2 * RECORD_SIZE
        assert result.next_sequence == 12
        assert result.progress is not None
        assert result.progress.records_completed == 2
        assert result.progress.records_total == 2
        assert result.progress.eta is None or result.progress.eta >= 0

    asyncio.run(scenario())


def test_read_begin_is_queued_before_any_data_publication() -> None:
    async def scenario() -> None:
        record = _record(7)
        session = BurstSession((_begin(10, 2), _data(record), _data(record), _done(12)))
        writer = FakeWriter()
        arena = TransferArena(10, 2, max_bytes=2 * RECORD_SIZE)
        await read_leg(session, arena, writer, 10, 2, ReadLegOptions(1))
        assert writer.calls.index("read_begin") < writer.calls.index(f"publish:{RECORD_SIZE}")

    asyncio.run(scenario())


def test_real_attempt_writer_stall_does_not_stop_ble_ingest() -> None:
    async def scenario() -> None:
        first, second = _record(1), _record(2)
        notifications = (_begin(10, 2), _data(first), _data(second), _done(12))
        session = BurstSession(notifications)
        arena = TransferArena(10, 2, max_bytes=2 * RECORD_SIZE)
        target = BlockingWriterTarget()
        writer = AttemptWriter(target, arena.readonly_source(), chunk_size=RECORD_SIZE)
        heartbeat = 0

        async def tick() -> None:
            nonlocal heartbeat
            while True:
                await asyncio.sleep(0)
                heartbeat += 1

        ticker = asyncio.create_task(tick())
        ingest = asyncio.create_task(read_leg(session, arena, writer, 10, 2, ReadLegOptions(2)))
        try:
            await asyncio.to_thread(target.append_started.wait, 1)
            for _ in range(100):
                if session.consumed == len(notifications):
                    break
                await asyncio.sleep(0)
            assert session.consumed == len(notifications)
            assert arena.received_bytes == 2 * RECORD_SIZE
            assert writer.published_high_water == 2 * RECORD_SIZE
            assert heartbeat > 0
            assert not ingest.done()
            target.release_append.set()
            result = await asyncio.wait_for(ingest, 1)
            assert result.written_bytes == 2 * RECORD_SIZE
        finally:
            target.release_append.set()
            if not ingest.done():
                await asyncio.wait_for(ingest, 1)
            await writer.close()
            ticker.cancel()
            await asyncio.gather(ticker, return_exceptions=True)
        assert not writer.thread.is_alive()
        assert target.calls[:4] == ["prepare", "prepare_leg", "read_begin", "append"]
        assert target.calls[-2:] == ["checkpoint", "close"]
        assert target.append_threads and target.append_threads[0] != threading.get_ident()

    asyncio.run(scenario())


@pytest.mark.parametrize(
    ("notifications", "expected"),
    [
        ((_begin(10, 2), _data(_record(1)[:10]), _done(12)), 10),
        ((_begin(10, 2), _data(_record(1) + b"x"), _done(12)), RECORD_SIZE + 1),
    ],
)
def test_torn_tail_and_overflow_fail_closed_without_advance(notifications: tuple[bytes, ...], expected: int) -> None:
    async def scenario() -> None:
        session = BurstSession(notifications)
        writer = FakeWriter()
        arena = TransferArena(10, 2, max_bytes=2 * RECORD_SIZE)
        with pytest.raises(TransferInterruptedError) as caught:
            await read_leg(session, arena, writer, 10, 2, ReadLegOptions(1))
        assert caught.value.received_bytes == expected
        assert encode_stop_command() in session.writes
        assert not any(command.startswith(b"\x12") for command in session.writes)

    asyncio.run(scenario())


def test_writer_failure_is_reported_with_counters_after_terminal_done() -> None:
    async def scenario() -> None:
        record = _record(1)
        session = BurstSession((_begin(10, 2), _data(record), _data(record), _done(12)))
        writer = FakeWriter(fail_barrier=True)
        arena = TransferArena(10, 2, max_bytes=2 * RECORD_SIZE)
        with pytest.raises(TransferInterruptedError) as caught:
            await read_leg(session, arena, writer, 10, 2, ReadLegOptions(1))
        assert caught.value.received_records == 2
        assert caught.value.submitted_records == 2
        assert writer.calls[-1] == "barrier"
        assert session.writes == [encode_read_command(10, 2)]

    asyncio.run(scenario())
