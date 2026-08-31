from __future__ import annotations

from itertools import cycle

import pytest

from omi_collector.capture.domain import transfer_arena
from omi_collector.capture.domain.ring_protocol import RECORD_SIZE
from omi_collector.capture.domain.transfer_arena import (
    ArenaCapacityError,
    ArenaDataMismatchError,
    ArenaOverrunError,
    ArenaPublicationError,
    ArenaSequenceError,
    TransferArena,
    TransferSnapshot,
    WriterCheckpoint,
)


def _records(count: int, *, marker: int = 0) -> bytes:
    return b"".join(bytes(((marker + index) % 256,)) * RECORD_SIZE for index in range(count))


def test_snapshot_and_admission_precede_exact_allocation(monkeypatch: pytest.MonkeyPatch) -> None:
    calls = 0

    def fail_if_allocated(size: int) -> bytearray:
        nonlocal calls
        calls += 1
        return bytearray(size)

    monkeypatch.setattr(transfer_arena, "bytearray", fail_if_allocated, raising=False)
    with pytest.raises(ArenaCapacityError, match="max_bytes"):
        TransferArena(10, 2, max_bytes=2 * RECORD_SIZE - 1)
    assert calls == 0

    snapshot = TransferSnapshot(10, 2)
    assert snapshot.total_bytes == 2 * RECORD_SIZE
    assert snapshot.end_sequence == 12


def test_arbitrary_fragments_keep_record_alignment_and_constant_counters() -> None:
    payload = _records(3, marker=7)
    arena = TransferArena(100, 3, max_bytes=len(payload))
    offset = 0
    for size in cycle((1, 17, 443, 2, 89, 701)):
        if offset == len(payload):
            break
        fragment = payload[offset : offset + size]
        arena.append(fragment)
        offset += len(fragment)
        assert arena.received_bytes == offset
        assert arena.complete_records == offset // RECORD_SIZE
        assert arena.received_records == offset // RECORD_SIZE

    assert arena.received_bytes == len(payload)
    assert arena.complete_records == 3
    assert arena.next_sequence == 103


def test_overrun_is_rejected_before_copying() -> None:
    arena = TransferArena(0, 1, max_bytes=RECORD_SIZE)
    with pytest.raises(ArenaOverrunError):
        arena.append(b"x" * (RECORD_SIZE + 1))
    assert arena.received_bytes == 0


def test_readonly_source_is_full_capacity_live_view_without_cursor_changes() -> None:
    arena = TransferArena(10, 2, max_bytes=2 * RECORD_SIZE)
    source = arena.readonly_source()
    assert source.readonly
    assert len(source) == 2 * RECORD_SIZE
    assert source.obj is arena._buffer
    assert (arena.received_bytes, arena.submitted_bytes, arena.durable_bytes) == (0, 0, 0)

    payload = _records(1, marker=23)
    arena.append(payload)
    assert bytes(source[:RECORD_SIZE]) == payload
    assert (arena.received_bytes, arena.submitted_bytes, arena.durable_bytes) == (RECORD_SIZE, 0, 0)
    with pytest.raises(TypeError):
        source[0] = 0


def test_reconnect_leg_allows_resident_overlap_before_durable() -> None:
    arena = TransferArena(100, 4, max_bytes=4 * RECORD_SIZE)
    arena.append(_records(2))
    arena.begin_leg(100, 2)
    arena.append(_records(2))

    arena.submit_prefix(1)
    assert arena.submitted_records == 1
    assert arena.durable_records == 0

    arena.begin_leg(101, 1)
    with pytest.raises(ArenaDataMismatchError):
        arena.append(b"z" * RECORD_SIZE)
    arena.begin_leg(101, 1)
    arena.append(_records(1, marker=1))

    arena.acknowledge_durable(WriterCheckpoint(101))
    assert arena.durable_records == 1
    arena.begin_leg(101, 3)
    arena.append(_records(3, marker=1))
    assert arena.received_bytes == 4 * RECORD_SIZE

    gap_arena = TransferArena(100, 4, max_bytes=4 * RECORD_SIZE)
    gap_arena.append(_records(2))
    with pytest.raises(ArenaSequenceError, match="next expected"):
        gap_arena.begin_leg(103, 1)


def test_replayed_overlap_is_compared_and_durable_prefix_never_mutates() -> None:
    first = _records(2, marker=11)
    arena = TransferArena(50, 3, max_bytes=3 * RECORD_SIZE)
    arena.append(first)
    arena.submit_prefix()
    assert arena.durable_records == 0
    submitted = arena.submitted_prefix()
    assert submitted.readonly
    assert bytes(submitted) == first
    with pytest.raises(TypeError):
        submitted[0] = 0

    arena.acknowledge_durable(WriterCheckpoint(52))
    durable = arena.durable_prefix()
    assert bytes(durable) == first

    arena.begin_leg(51, 2)
    arena.append(first[RECORD_SIZE:] + _records(1, marker=33))
    assert bytes(durable) == first
    assert bytes(arena.durable_prefix()) == first

    arena.begin_leg(51, 1)
    with pytest.raises(ArenaDataMismatchError):
        arena.append(b"z" * RECORD_SIZE)
    assert bytes(arena.durable_prefix()) == first


def test_submitted_chunks_are_read_only_and_record_aligned() -> None:
    payload = _records(5)
    arena = TransferArena(1, 5, max_bytes=len(payload))
    arena.append(payload)
    arena.submit_prefix(4)
    chunks = tuple(arena.submitted_chunks(500))
    assert [len(chunk) for chunk in chunks] == [500, 500, 500, 276]
    assert all(chunk.readonly for chunk in chunks)
    assert sum(map(len, chunks)) == 4 * RECORD_SIZE
    with pytest.raises(ArenaPublicationError):
        arena.submit_prefix(3)


def test_only_explicit_writer_checkpoint_advances_durable_cursor() -> None:
    arena = TransferArena(10, 3, max_bytes=3 * RECORD_SIZE)
    arena.append(_records(3))
    arena.submit_prefix()
    assert arena.submitted_bytes == 3 * RECORD_SIZE
    assert arena.durable_bytes == 0

    with pytest.raises(ArenaPublicationError, match="submitted prefix"):
        arena.acknowledge_durable(WriterCheckpoint(14))
    assert arena.durable_bytes == 0

    arena.acknowledge_durable(WriterCheckpoint(11))
    assert arena.durable_records == 1
    assert arena.durable_next_sequence == 11


def test_full_synthetic_burst_uses_one_backing_buffer() -> None:
    record_count = 4096
    payload = _records(record_count, marker=19)
    arena = TransferArena(9000, record_count, max_bytes=len(payload))
    for offset in range(0, len(payload), 251):
        arena.append(payload[offset : offset + 251])
    assert arena.received_bytes == len(payload)
    assert arena.complete_records == record_count
    assert isinstance(arena._buffer, bytearray)
    assert len(arena._buffer) == len(payload)
    assert tuple(value for value in arena.__dict__ if value == "_buffer") == ("_buffer",)
