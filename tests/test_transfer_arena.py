from __future__ import annotations

from itertools import cycle

import pytest

from omi_collector.capture.domain import transfer_arena
from omi_collector.capture.domain.ring_protocol import RECORD_SIZE
from omi_collector.capture.domain.transfer_arena import (
    ArenaCapacityError,
    ArenaDataMismatchError,
    ArenaLegError,
    ArenaOverrunError,
    ArenaPublicationError,
    ArenaSequenceError,
    TransferArena,
    TransferSnapshot,
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
    assert (arena.received_bytes, arena.submitted_bytes) == (0, 0)

    payload = _records(1, marker=23)
    arena.append(payload)
    assert bytes(source[:RECORD_SIZE]) == payload
    assert (arena.received_bytes, arena.submitted_bytes) == (RECORD_SIZE, 0)
    with pytest.raises(TypeError):
        source[0] = 0


def test_reconnect_leg_allows_resident_overlap() -> None:
    arena = TransferArena(100, 4, max_bytes=4 * RECORD_SIZE)
    arena.append(_records(2))
    arena.begin_leg(100, 2)
    arena.append(_records(2))

    arena.submit_prefix(1)
    assert arena.submitted_records == 1

    arena.begin_leg(101, 1)
    with pytest.raises(ArenaDataMismatchError):
        arena.append(b"z" * RECORD_SIZE)
    arena.begin_leg(101, 1)
    arena.append(_records(1, marker=1))

    arena.begin_leg(101, 3)
    arena.append(_records(3, marker=1))
    assert arena.received_bytes == 4 * RECORD_SIZE

    gap_arena = TransferArena(100, 4, max_bytes=4 * RECORD_SIZE)
    gap_arena.append(_records(2))
    with pytest.raises(ArenaSequenceError, match="next expected"):
        gap_arena.begin_leg(103, 1)


def test_replayed_overlap_is_compared_and_submitted_prefix_never_mutates() -> None:
    first = _records(2, marker=11)
    arena = TransferArena(50, 3, max_bytes=3 * RECORD_SIZE)
    arena.append(first)
    arena.submit_prefix()
    submitted = arena.submitted_prefix()
    assert submitted.readonly
    assert bytes(submitted) == first
    with pytest.raises(TypeError):
        submitted[0] = 0

    arena.begin_leg(51, 2)
    arena.append(first[RECORD_SIZE:] + _records(1, marker=33))
    assert bytes(submitted) == first

    arena.begin_leg(51, 1)
    with pytest.raises(ArenaDataMismatchError):
        arena.append(b"z" * RECORD_SIZE)
    assert bytes(submitted) == first


def test_submitted_prefix_is_read_only_and_cannot_move_backward() -> None:
    payload = _records(5)
    arena = TransferArena(1, 5, max_bytes=len(payload))
    arena.append(payload)
    submitted = arena.submit_prefix(4)
    assert submitted.readonly
    assert bytes(submitted) == payload[: 4 * RECORD_SIZE]
    with pytest.raises(TypeError):
        submitted[0] = 0
    with pytest.raises(ArenaPublicationError):
        arena.submit_prefix(3)


def test_submission_accepts_only_received_prefix_and_repeats_idempotently() -> None:
    arena = TransferArena(20, 3, max_bytes=3 * RECORD_SIZE)
    empty = arena.submit_prefix(0)
    assert empty.readonly
    assert bytes(empty) == b""
    assert (arena.received_bytes, arena.submitted_bytes) == (0, 0)

    payload = _records(2, marker=31)
    arena.append(payload[: RECORD_SIZE + 13])
    submitted = arena.submit_prefix(1)
    assert bytes(submitted) == payload[:RECORD_SIZE]
    assert bytes(arena.submit_prefix(1)) == bytes(submitted)
    assert arena.submitted_bytes == RECORD_SIZE

    source_before = bytes(arena.readonly_source())
    watermarks_before = (arena.received_bytes, arena.submitted_bytes)
    with pytest.raises(ArenaPublicationError):
        arena.submit_prefix(2)
    with pytest.raises(ArenaPublicationError):
        arena.submit_prefix(-1)
    assert (arena.received_bytes, arena.submitted_bytes) == watermarks_before
    assert bytes(arena.readonly_source()) == source_before


def test_reconnect_leg_overrun_uses_remaining_leg_capacity() -> None:
    first = _records(1, marker=41)
    second = _records(1, marker=42)
    arena = TransferArena(100, 4, max_bytes=4 * RECORD_SIZE)
    arena.append(first)
    arena.begin_leg(101, 1)
    partial = 100
    arena.append(second[:partial])

    source = arena.readonly_source()
    source_before = bytes(source)
    watermarks_before = (
        arena.leg_received_bytes,
        arena.received_bytes,
        arena.submitted_bytes,
    )
    # The byte beyond this leg still fits within the larger snapshot.
    with pytest.raises(ArenaOverrunError):
        arena.append(second[partial:] + b"x")
    assert (
        arena.leg_received_bytes,
        arena.received_bytes,
        arena.submitted_bytes,
    ) == watermarks_before
    assert bytes(source) == source_before

    arena.append(second[partial:])
    assert arena.leg_received_bytes == RECORD_SIZE
    assert arena.received_bytes == 2 * RECORD_SIZE
    assert bytes(source[: 2 * RECORD_SIZE]) == first + second


def test_begin_leg_rejects_zero_records_and_accepts_one_record() -> None:
    arena = TransferArena(100, 2, max_bytes=2 * RECORD_SIZE)
    with pytest.raises(ArenaLegError):
        arena.begin_leg(100, 0)
    assert (arena.leg_received_bytes, arena.received_bytes) == (0, 0)

    arena.begin_leg(100, 1)
    arena.append(_records(1, marker=51))
    assert arena.leg_received_bytes == RECORD_SIZE
    assert arena.received_bytes == RECORD_SIZE


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


@pytest.mark.parametrize(
    ("field", "replacement"),
    [("start_sequence", 99), ("total_records", 99)],
)
def test_snapshot_bounds_are_immutable_and_keep_live_arena_bounds(field: str, replacement: int) -> None:
    arena = TransferArena(10, 2, max_bytes=2 * RECORD_SIZE)
    snapshot = arena.snapshot
    original = {
        "start_sequence": snapshot.start_sequence,
        "total_records": snapshot.total_records,
        "record_size": snapshot.record_size,
        "total_bytes": snapshot.total_bytes,
        "end_sequence": snapshot.end_sequence,
    }
    assert original == {
        "start_sequence": 10,
        "total_records": 2,
        "record_size": RECORD_SIZE,
        "total_bytes": 2 * RECORD_SIZE,
        "end_sequence": 12,
    }
    expected_live_bounds = (arena.next_sequence, arena.total_bytes)

    try:
        setattr(snapshot, field, replacement)
    except AttributeError:
        mutation_rejected = True
    else:
        mutation_rejected = False
    bounds_after_attempt = (arena.next_sequence, arena.total_bytes)
    if not mutation_rejected:
        setattr(snapshot, field, original[field])

    assert mutation_rejected
    assert bounds_after_attempt == expected_live_bounds
    assert (arena.next_sequence, arena.total_bytes) == expected_live_bounds


@pytest.mark.parametrize(
    ("start_sequence", "total_records", "max_bytes"),
    [
        (False, 1, RECORD_SIZE),
        (-1, 1, RECORD_SIZE),
        (0, True, RECORD_SIZE),
        (0, -1, RECORD_SIZE),
        (0, 1, False),
        (0, 1, -1),
    ],
)
def test_invalid_arena_bounds_fail_before_allocation(
    monkeypatch: pytest.MonkeyPatch, start_sequence: int, total_records: int, max_bytes: int
) -> None:
    calls = 0

    def track_allocation(size: int) -> bytearray:
        nonlocal calls
        calls += 1
        return bytearray(size)

    monkeypatch.setattr(transfer_arena, "bytearray", track_allocation, raising=False)
    with pytest.raises(ValueError):
        TransferArena(start_sequence, total_records, max_bytes=max_bytes)

    assert calls == 0


def test_partial_leg_counts_only_complete_records_and_submitted_records_are_integer() -> None:
    arena = TransferArena(10, 2, max_bytes=2 * RECORD_SIZE)
    arena.append(_records(1) + b"x" * 13)

    assert arena.leg_received_bytes == RECORD_SIZE + 13
    assert arena.leg_complete_records == 1
    assert arena.complete_records == 1
    assert arena.next_sequence == 11
    arena.submit_prefix()
    assert arena.submitted_records == 1
    assert isinstance(arena.submitted_records, int)


def test_begin_leg_beyond_snapshot_preserves_source_and_counters() -> None:
    arena = TransferArena(10, 2, max_bytes=2 * RECORD_SIZE)
    first = _records(1, marker=71)
    arena.append(first)
    arena.submit_prefix(1)
    source = arena.readonly_source()
    source_before = bytes(source)
    counters_before = (
        arena.next_sequence,
        arena.total_bytes,
        arena.leg_received_bytes,
        arena.received_bytes,
        arena.submitted_bytes,
    )

    with pytest.raises(ArenaOverrunError):
        arena.begin_leg(11, 2)

    assert bytes(source) == source_before
    assert (
        arena.next_sequence,
        arena.total_bytes,
        arena.leg_received_bytes,
        arena.received_bytes,
        arena.submitted_bytes,
    ) == counters_before

    arena.begin_leg(11, 1)
    arena.append(_records(1, marker=72))
    assert bytes(source[: 2 * RECORD_SIZE]) == first + _records(1, marker=72)
