"""Read explicitly selected physical byte spans without assigning device sequences."""

from __future__ import annotations

from collections.abc import Callable, Iterable, Iterator
from dataclasses import dataclass

RECORD_SIZE = 444
_TIMESTAMP_SIZE = 4


@dataclass(frozen=True, slots=True)
class PhysicalSpan:
    """A source-relative, half-open byte interval retained in original custody."""

    source_id: str
    offset: int
    length: int

    def __post_init__(self) -> None:
        if not isinstance(self.source_id, str) or not self.source_id:
            raise ValueError("physical source identity is invalid")
        if type(self.offset) is not int or self.offset < 0 or type(self.length) is not int or self.length <= 0:
            raise ValueError("physical span is invalid")


@dataclass(frozen=True, slots=True)
class PhysicalSegment:
    """One continuous byte stream; separate segments represent unknown seams."""

    spans: tuple[PhysicalSpan, ...]

    def __post_init__(self) -> None:
        if (
            not isinstance(self.spans, tuple)
            or not self.spans
            or not all(isinstance(span, PhysicalSpan) for span in self.spans)
        ):
            raise ValueError("physical segment has no valid spans")
        if sum(span.length for span in self.spans) % RECORD_SIZE:
            raise ValueError("physical segment does not end on a complete record")


def iter_physical_records(
    segment: PhysicalSegment, read_span: Callable[[PhysicalSpan], Iterable[bytes]]
) -> Iterator[bytes]:
    """Yield complete records from declared spans; exhaust before committing output."""
    buffer = bytearray()
    for span in segment.spans:
        consumed = 0
        for chunk in read_span(span):
            if not isinstance(chunk, bytes):
                raise ValueError("physical span reader returned non-bytes")
            consumed += len(chunk)
            if consumed > span.length:
                raise ValueError("physical span reader exceeded its declared length")
            buffer.extend(chunk)
            while len(buffer) >= RECORD_SIZE:
                yield bytes(buffer[:RECORD_SIZE])
                del buffer[:RECORD_SIZE]
        if consumed != span.length:
            raise ValueError("physical span reader ended before its declared length")
    if buffer:
        raise ValueError("physical segment ended with an incomplete record")


def find_byte_occurrences(chunks: Iterable[bytes], needle: bytes, *, base_offset: int = 0) -> Iterator[int]:
    """Find all exact byte matches across arbitrary chunk boundaries."""
    if not needle:
        raise ValueError("overlap anchor is empty")
    if type(base_offset) is not int or base_offset < 0:
        raise ValueError("overlap source offset is invalid")
    tail = b""
    offset = base_offset
    for chunk in chunks:
        if not isinstance(chunk, bytes):
            raise ValueError("overlap reader returned non-bytes")
        combined = tail + chunk
        cursor = 0
        while (found := combined.find(needle, cursor)) != -1:
            yield offset - len(tail) + found
            cursor = found + 1
        offset += len(chunk)
        tail = combined[-(len(needle) - 1) :] if len(needle) > 1 else b""


def anchored_tail_segment(
    original: PhysicalSpan, independent: PhysicalSpan, overlap_length: int, match_offsets: Iterable[int]
) -> PhysicalSegment:
    """Join a unique exact tail match; offsets must come from a complete original scan."""
    if type(overlap_length) is not int or not 0 < overlap_length <= min(original.length, independent.length):
        raise ValueError("overlap length is invalid")
    expected = original.offset + original.length - overlap_length
    if tuple(match_offsets) != (expected,):
        raise ValueError("independent overlap is not a unique original tail")
    remainder = independent.length - overlap_length
    spans = (original,)
    if remainder:
        spans += (PhysicalSpan(independent.source_id, independent.offset + overlap_length, remainder),)
    return PhysicalSegment(spans)


@dataclass(frozen=True, slots=True)
class PhysicalPacketSlice:
    """One complete packet's byte range relative to its physical record."""

    offset: int
    length: int


@dataclass(frozen=True, slots=True)
class PhysicalRecordPrefix:
    packets: tuple[PhysicalPacketSlice, ...]
    consumed_prefix_len: int
    invalid_offset: int | None
    first_size: int


def inspect_partial_seam(data: bytes, previous_overflow: int | None) -> PhysicalRecordPrefix:
    """Inspect the known record start through its first fault, checking packet continuity."""
    from .opus_duration import _MAX_PACKET_SIZE, _is_20ms

    if len(data) <= _TIMESTAMP_SIZE:
        raise ValueError("physical seam lacks a record header")
    first_size = data[_TIMESTAMP_SIZE]
    if previous_overflow is not None and (
        type(previous_overflow) is not int or not 0 < previous_overflow <= _MAX_PACKET_SIZE
    ):
        raise ValueError("preceding packet size is invalid")
    if previous_overflow is not None and previous_overflow != first_size:
        raise ValueError("physical seam does not continue the preceding packet")
    record_end = min(len(data), RECORD_SIZE)
    offset = _TIMESTAMP_SIZE
    packets: list[PhysicalPacketSlice] = []
    while offset < min(record_end, RECORD_SIZE - 1):
        marker = offset
        size = data[offset]
        offset += 1
        if size == 0:
            continue
        end = offset + size
        if size > _MAX_PACKET_SIZE:
            return PhysicalRecordPrefix(tuple(packets), marker, marker, first_size)
        if end > record_end or (record_end == RECORD_SIZE and end == record_end):
            return PhysicalRecordPrefix(tuple(packets), marker, None, first_size)
        packet = data[offset:end]
        if not _is_20ms(packet) or packet[0] & 4:
            return PhysicalRecordPrefix(tuple(packets), marker, marker, first_size)
        packets.append(PhysicalPacketSlice(offset, size))
        offset = end
    return PhysicalRecordPrefix(tuple(packets), offset, None, first_size)
