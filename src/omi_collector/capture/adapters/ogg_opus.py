# Vendored from j2h4u/audio-primitives commit f6403162f01bb37e383d338e68a0d88baf996ec0.

"""Deterministic, dependency-free Ogg Opus muxing."""

from __future__ import annotations

import binascii
import struct
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from hashlib import sha256
from itertools import chain, islice
from os import fsync
from pathlib import Path
from typing import BinaryIO, Final, Protocol

_CAPTURE_PATTERN: Final = b"OggS"
_CONTINUED_PACKET: Final = 0x01
_BEGINNING_OF_STREAM: Final = 0x02
_END_OF_STREAM: Final = 0x04
_MAX_LACING_VALUES: Final = 255
_MAX_LACING_VALUE: Final = 255
_PAGE_BODY_TARGET: Final = 4_096
_SIGNED_SHORT_MIN: Final = -32_768
_SIGNED_SHORT_MAX: Final = 32_767
_UNSIGNED_BYTE_MAX: Final = 0xFF
_UNSIGNED_SHORT_MAX: Final = 0xFFFF
_SERIAL_MAX: Final = 0xFFFFFFFF
_UINT64_MAX: Final = 0xFFFFFFFFFFFFFFFF
_UINT32_MAX: Final = 0xFFFFFFFF
_REVERSED_BYTES: Final = bytes(int(f"{value:08b}"[::-1], 2) for value in range(256))


@dataclass(frozen=True, slots=True)
class OggOpusConfig:
    """Source-independent Opus stream and page batching parameters."""

    packets_per_flush: int = 50
    packet_samples: int = 960
    channels: int = 1
    pre_skip: int = 0
    input_sample_rate: int = 16_000
    output_gain: int = 0
    mapping_family: int = 0
    vendor: bytes = b""
    comments: tuple[bytes, ...] = ()

    def __post_init__(self) -> None:
        _positive_int(self.packets_per_flush, "packets_per_flush")
        _positive_int(self.packet_samples, "packet_samples")
        _positive_byte(self.channels, "channels")
        _unsigned_short(self.pre_skip, "pre_skip")
        _positive_int(self.input_sample_rate, "input_sample_rate")
        if (
            isinstance(self.output_gain, bool)
            or not isinstance(self.output_gain, int)
            or not _SIGNED_SHORT_MIN <= self.output_gain <= _SIGNED_SHORT_MAX
        ):
            raise ValueError("output_gain must be a signed 16-bit integer")
        _unsigned_byte(self.mapping_family, "mapping_family")
        if not isinstance(self.vendor, bytes):
            raise TypeError("vendor must be bytes")
        for index, comment in enumerate(self.comments):
            if not isinstance(comment, bytes):
                raise TypeError(f"comment {index} must be bytes")


def _positive_int(value: int, name: str) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError(f"{name} must be a positive integer")


def _unsigned_byte(value: int, name: str) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or not 0 <= value <= _UNSIGNED_BYTE_MAX:
        raise ValueError(f"{name} must be an unsigned 8-bit integer")


def _positive_byte(value: int, name: str) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or not 1 <= value <= _UNSIGNED_BYTE_MAX:
        raise ValueError(f"{name} must be a positive unsigned 8-bit integer")


def _unsigned_short(value: int, name: str) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or not 0 <= value <= _UNSIGNED_SHORT_MAX:
        raise ValueError(f"{name} must be an unsigned 16-bit integer")


DEFAULT_CONFIG: Final = OggOpusConfig()


class OggMuxError(RuntimeError):
    """The requested Ogg Opus stream could not be created."""


@dataclass(frozen=True, slots=True)
class OggWriteResult:
    """Durability and content summary for one written Ogg Opus file."""

    page_count: int
    bytes: int
    sha256: str


class _Digest(Protocol):
    def update(self, data: bytes, /) -> None: ...


@dataclass(frozen=True, slots=True)
class _Packet:
    payload: bytes
    granule_position: int


@dataclass(frozen=True, slots=True)
class _PageBatch:
    encoded: tuple[bytes, ...]
    next_sequence: int


@dataclass(frozen=True, slots=True)
class _PageIdentity:
    serial: int
    sequence: int


@dataclass(frozen=True, slots=True)
class _StreamContext:
    output: BinaryIO | None
    digest: _Digest
    config: OggOpusConfig


def _iter_validated_packets(packets: Iterable[bytes]) -> Iterable[bytes]:
    try:
        iterator = iter(packets)
    except TypeError as exc:
        raise TypeError("packets must be an iterable of bytes") from exc
    index = 0
    while True:
        try:
            packet = next(iterator)
        except StopIteration:
            return
        if not isinstance(packet, bytes):
            raise TypeError(f"packet {index} must be bytes")
        if not packet:
            raise ValueError(f"packet {index} must not be empty")
        yield packet
        index += 1


def _validate_serial(serial: int) -> None:
    if isinstance(serial, bool) or not isinstance(serial, int) or not 0 <= serial <= _SERIAL_MAX:
        raise ValueError("serial must be an unsigned 32-bit integer")


def _opus_head(config: OggOpusConfig) -> bytes:
    return struct.pack(
        "<8sBBHIhB",
        b"OpusHead",
        1,
        config.channels,
        config.pre_skip,
        config.input_sample_rate,
        config.output_gain,
        config.mapping_family,
    )


def _opus_tags(config: OggOpusConfig) -> bytes:
    result = bytearray(struct.pack("<8sI", b"OpusTags", len(config.vendor)))
    result.extend(config.vendor)
    result.extend(struct.pack("<I", len(config.comments)))
    for comment in config.comments:
        result.extend(struct.pack("<I", len(comment)))
        result.extend(comment)
    return bytes(result)


def _lacing_values(payload_size: int) -> tuple[int, ...]:
    full_segments, remainder = divmod(payload_size, _MAX_LACING_VALUE)
    return (*(_MAX_LACING_VALUE for _ in range(full_segments)), remainder)


def _ogg_crc(data: bytes | bytearray) -> int:
    """Return Ogg's non-reflected CRC using CPython's fast reflected CRC."""
    reflected = binascii.crc32(bytes(data).translate(_REVERSED_BYTES), _UINT32_MAX) ^ _UINT32_MAX
    reversed_bytes = reflected.to_bytes(4, "little").translate(_REVERSED_BYTES)
    return int.from_bytes(reversed_bytes, "big")


def _encode_page(
    *,
    flags: int,
    granule_position: int,
    identity: _PageIdentity,
    lacing: Sequence[int],
    body: bytes,
) -> bytes:
    if not lacing or len(lacing) > _MAX_LACING_VALUES or sum(lacing) != len(body):
        raise OggMuxError("invalid Ogg page segmentation")
    encoded = bytearray(
        struct.pack(
            "<4sBBQIIIB",
            _CAPTURE_PATTERN,
            0,
            flags,
            granule_position & _UINT64_MAX,
            identity.serial,
            identity.sequence,
            0,
            len(lacing),
        )
    )
    encoded.extend(lacing)
    encoded.extend(body)
    struct.pack_into("<I", encoded, 22, _ogg_crc(encoded))
    return bytes(encoded)


def _encode_pages(
    packets: Sequence[_Packet],
    *,
    serial: int,
    sequence: int,
    beginning_of_stream: bool = False,
    end_of_stream: bool = False,
) -> _PageBatch:
    pages: list[bytes] = []
    lacing: list[int] = []
    body = bytearray()
    continued_at_start = False
    page_granule = -1
    first_page = True

    def emit(*, final_packet_completed: bool) -> None:
        nonlocal body, continued_at_start, first_page, lacing, page_granule, sequence
        flags = _CONTINUED_PACKET if continued_at_start else 0
        if beginning_of_stream and first_page:
            flags |= _BEGINNING_OF_STREAM
        if end_of_stream and final_packet_completed:
            flags |= _END_OF_STREAM
        pages.append(
            _encode_page(
                flags=flags,
                granule_position=page_granule,
                identity=_PageIdentity(serial, sequence),
                lacing=lacing,
                body=bytes(body),
            )
        )
        sequence += 1
        first_page = False
        lacing = []
        body = bytearray()
        page_granule = -1

    for packet_index, packet in enumerate(packets):
        values = _lacing_values(len(packet.payload))
        payload_offset = 0
        for value_index, value in enumerate(values):
            if len(lacing) == _MAX_LACING_VALUES:
                emit(final_packet_completed=False)
                continued_at_start = value_index > 0
            lacing.append(value)
            body.extend(packet.payload[payload_offset : payload_offset + value])
            payload_offset += value
            packet_completed = value < _MAX_LACING_VALUE
            page_granule = packet.granule_position if packet_completed else page_granule
            is_final_packet = packet_index == len(packets) - 1
            if len(lacing) == _MAX_LACING_VALUES or len(body) > _PAGE_BODY_TARGET:
                emit(final_packet_completed=is_final_packet and packet_completed)
                continued_at_start = not packet_completed
        if payload_offset != len(packet.payload):
            raise OggMuxError("Ogg packet segmentation lost payload bytes")

    if lacing:
        emit(final_packet_completed=True)
    return _PageBatch(tuple(pages), sequence)


def _write_pages(context: _StreamContext, batch: _PageBatch) -> tuple[int, int]:
    byte_count = 0
    for encoded in batch.encoded:
        if context.output is not None:
            context.output.write(encoded)
        context.digest.update(encoded)
        byte_count += len(encoded)
    return len(batch.encoded), byte_count


def _write_file(
    path: Path,
    streams: Iterable[tuple[Iterable[bytes], int]],
    config: OggOpusConfig,
) -> OggWriteResult:
    digest = sha256()
    page_count = 0
    byte_count = 0
    with path.open("wb") as output:
        context = _StreamContext(output, digest, config)
        for stream, serial in streams:
            pages, bytes_written = _write_stream(context, stream, serial)
            page_count += pages
            byte_count += bytes_written
        output.flush()
        fsync(output.fileno())
    return OggWriteResult(page_count, byte_count, digest.hexdigest())


def write_ogg_opus(
    path: Path,
    packets: Iterable[bytes],
    serial: int,
    *,
    config: OggOpusConfig = DEFAULT_CONFIG,
) -> OggWriteResult:
    """Write encoded Opus packets as one deterministic Ogg logical stream."""
    stream = _validated_single_stream(packets, serial)
    return _write_file(path, ((stream, serial),), config)


def hash_ogg_opus(
    packets: Iterable[bytes],
    serial: int,
    *,
    config: OggOpusConfig = DEFAULT_CONFIG,
) -> OggWriteResult:
    """Hash one deterministic Ogg Opus stream without writing it to a file."""
    stream = _validated_single_stream(packets, serial)
    digest = sha256()
    context = _StreamContext(None, digest, config)
    page_count, byte_count = _write_stream(context, stream, serial)
    return OggWriteResult(page_count, byte_count, digest.hexdigest())


def _validated_single_stream(packets: Iterable[bytes], serial: int) -> Iterable[bytes]:
    validated_packets = iter(_iter_validated_packets(packets))
    try:
        first_packet = next(validated_packets)
    except StopIteration as exc:
        raise ValueError("packets must not be empty") from exc
    _validate_serial(serial)
    return chain((first_packet,), validated_packets)


def _write_stream(context: _StreamContext, packets: Iterable[bytes], serial: int) -> tuple[int, int]:
    page_count = 0
    byte_count = 0
    sequence = 0

    header = _encode_pages(
        (_Packet(_opus_head(context.config), 0),), serial=serial, sequence=sequence, beginning_of_stream=True
    )
    pages, bytes_written = _write_pages(context, header)
    page_count += pages
    byte_count += bytes_written
    sequence = header.next_sequence

    tags = _encode_pages((_Packet(_opus_tags(context.config), 0),), serial=serial, sequence=sequence)
    pages, bytes_written = _write_pages(context, tags)
    page_count += pages
    byte_count += bytes_written
    sequence = tags.next_sequence

    packet_iterator = iter(packets)
    payloads = tuple(islice(packet_iterator, context.config.packets_per_flush))
    if not payloads:
        raise ValueError("packets must not be empty")
    packet_index = 0
    while payloads:
        next_payloads = tuple(islice(packet_iterator, context.config.packets_per_flush))
        batch = tuple(
            _Packet(payload, (packet_index + index + 1) * context.config.packet_samples)
            for index, payload in enumerate(payloads)
        )
        final_batch = not next_payloads
        encoded = _encode_pages(
            batch,
            serial=serial,
            sequence=sequence,
            end_of_stream=final_batch,
        )
        pages, bytes_written = _write_pages(context, encoded)
        page_count += pages
        byte_count += bytes_written
        sequence = encoded.next_sequence
        packet_index += len(payloads)
        payloads = next_payloads
    return page_count, byte_count
