from __future__ import annotations

from collections.abc import Iterable
from hashlib import sha256

import pytest

from omi_collector.capture.domain.physical_recovery import PhysicalSegment, PhysicalSpan, iter_physical_records


def test_ordered_spans_join_only_explicit_continuation_and_keep_duplicate_content() -> None:
    first, repeated, last = bytes([1]) * 444, bytes([2]) * 444, bytes([3]) * 444
    original = first + repeated + repeated + last[:356]
    independent = repeated + repeated + last
    sources = {"original": original, "repeat": independent}
    segment = PhysicalSegment((PhysicalSpan("original", 0, len(original)), PhysicalSpan("repeat", 2 * 444 + 356, 88)))

    def read(span: PhysicalSpan) -> Iterable[bytes]:
        data = sources[span.source_id][span.offset : span.offset + span.length]
        return (data[offset : offset + 127] for offset in range(0, len(data), 127))

    records = tuple(iter_physical_records(segment, read))
    assert records == (first, repeated, repeated, last)
    assert sha256(b"".join(records)).digest() == sha256(first + repeated + repeated + last).digest()


@pytest.mark.parametrize("actual", [b"x" * 443, b"x" * 445])
def test_reader_rejects_truncated_or_extra_bytes(actual: bytes) -> None:
    segment = PhysicalSegment((PhysicalSpan("one", 0, 444),))
    with pytest.raises(ValueError, match="declared length"):
        tuple(iter_physical_records(segment, lambda _span: (actual,)))


def test_unknown_seam_cannot_be_concatenated_by_accident() -> None:
    with pytest.raises(ValueError, match="complete record"):
        PhysicalSegment((PhysicalSpan("first", 0, 356),))
    with pytest.raises(ValueError, match="complete record"):
        PhysicalSegment((PhysicalSpan("second", 0, 88),))


def test_unique_tail_overlap_keeps_one_copy_and_recovers_continuation() -> None:
    from omi_collector.capture.domain.physical_recovery import anchored_tail_segment, find_byte_occurrences

    prefix = bytes([7]) * (3 * 444)
    anchor = bytes((index * 17 + index // 256) % 256 for index in range(6 * 444 + 356))
    independent = anchor + bytes([9]) * 88
    original = prefix + anchor
    chunks = (original[index : index + 127] for index in range(0, len(original), 127))
    matches = tuple(find_byte_occurrences(chunks, anchor))
    segment = anchored_tail_segment(
        PhysicalSpan("original", 0, len(original)),
        PhysicalSpan("independent", 0, len(independent)),
        len(anchor),
        matches,
    )
    sources = {"original": original, "independent": independent}
    records = tuple(
        iter_physical_records(segment, lambda span: (sources[span.source_id][span.offset : span.offset + span.length],))
    )
    assert b"".join(records) == original + independent[len(anchor) :]
    assert len(records) == 10
    shifted = tuple(find_byte_occurrences((original,), anchor, base_offset=123))
    assert shifted == (123 + len(prefix),)
    with pytest.raises(ValueError, match="unique original tail"):
        anchored_tail_segment(
            PhysicalSpan("original", 0, len(original)),
            PhysicalSpan("independent", 0, len(independent)),
            len(anchor),
            (len(prefix) - 1, len(prefix)),
        )


def test_only_packets_before_first_fault_are_recovered_at_known_boundary() -> None:
    from omi_collector.capture.domain.physical_recovery import inspect_partial_seam

    packet = bytes((8, 0x55))
    raw = bytearray(444)
    raw[4:14] = bytes((2,)) + packet + bytes((2,)) + packet + bytes((2,)) + packet + bytes((161,))
    prefix = inspect_partial_seam(bytes(raw[:27]), previous_overflow=2)
    assert [(item.offset, item.length) for item in prefix.packets] == [(5, 2), (8, 2), (11, 2)]
    assert prefix.first_size == 2
    assert prefix.invalid_offset == 13
    assert prefix.consumed_prefix_len == 13
    with pytest.raises(ValueError, match="preceding packet"):
        inspect_partial_seam(bytes(raw[:27]), previous_overflow=3)
    with pytest.raises(ValueError, match="preceding packet size"):
        inspect_partial_seam(bytes(raw[:27]), previous_overflow=0)


def test_incomplete_seam_packet_is_unresolved_not_invalid() -> None:
    from omi_collector.capture.domain.physical_recovery import inspect_partial_seam

    prefix = inspect_partial_seam(bytes(4) + bytes((2, 8)), previous_overflow=None)
    assert prefix.packets == ()
    assert prefix.consumed_prefix_len == 4
    assert prefix.invalid_offset is None
