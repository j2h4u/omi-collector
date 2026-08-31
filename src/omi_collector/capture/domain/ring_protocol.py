"""Pure-data helpers for the Omi ring-buffer protocol.

The module deliberately has no BLE or host side effects.  In particular, it
does not expose helpers for the destructive ``CLEAR`` command.
"""

from __future__ import annotations

from dataclasses import dataclass
from struct import Struct
from typing import Literal

RECORD_SIZE = 444
TIMESTAMP_SIZE = 4
AUDIO_PAYLOAD_SIZE = RECORD_SIZE - TIMESTAMP_SIZE

NOTIFY_ACK = 0x01
NOTIFY_INFO = 0x02
NOTIFY_DATA = 0x03
NOTIFY_DONE = 0x04
NOTIFY_READ_BEGIN = 0x05

CMD_STOP = 0x03
CMD_INFO = 0x10
CMD_READ = 0x11
CMD_ADVANCE = 0x12

STATUS_STORAGE_NOT_READY = 9

_READ = Struct(">BQ")
_READ_WITH_COUNT = Struct(">BQI")
_ADVANCE = Struct(">BQ")


class RingProtocolError(ValueError):
    """Raised when a ring-protocol payload is malformed."""


@dataclass(frozen=True, slots=True)
class RingStatus:
    """Cached storage status read from the 16-byte status characteristic."""

    used_bytes: int
    unread_packets: int
    free_bytes: int
    rtc_valid: int

    @property
    def has_valid_rtc(self) -> bool:
        return self.rtc_valid != 0


@dataclass(frozen=True, slots=True)
class RingInfo:
    """Sequence and capacity metadata returned by an INFO notification."""

    read_sequence: int
    write_sequence: int
    capacity_packets: int
    dropped_packets: int
    packet_size: int

    @property
    def unread_packets(self) -> int:
        return self.write_sequence - self.read_sequence


@dataclass(frozen=True, slots=True)
class AckNotification:
    status: int

    @property
    def is_ok(self) -> bool:
        return self.status == 0


@dataclass(frozen=True, slots=True)
class DoneNotification:
    status: int
    next_sequence: int

    @property
    def is_ok(self) -> bool:
        return self.status == 0


@dataclass(frozen=True, slots=True)
class ReadBeginNotification:
    transfer_start_sequence: int
    packet_count: int


@dataclass(frozen=True, slots=True)
class RingRecord:
    timestamp: int
    audio_payload: bytes

    @classmethod
    def parse(cls, payload: bytes) -> RingRecord:
        _require_exact_length(payload, RECORD_SIZE, "ring record")
        return cls(
            timestamp=int.from_bytes(payload[:TIMESTAMP_SIZE], "big"),
            audio_payload=payload[TIMESTAMP_SIZE:],
        )

    def opus_frames(self) -> tuple[bytes, ...]:
        return parse_audio_payload(self.audio_payload)


def parse_status(payload: bytes) -> RingStatus:
    """Parse four little-endian unsigned integers from the status value."""
    _require_exact_length(payload, 16, "status")
    return RingStatus(
        used_bytes=_read_uint(payload, 0, 4, "little"),
        unread_packets=_read_uint(payload, 4, 4, "little"),
        free_bytes=_read_uint(payload, 8, 4, "little"),
        rtc_valid=_read_uint(payload, 12, 4, "little"),
    )


def parse_info_notification(payload: bytes) -> RingInfo:
    """Parse an INFO notification, whose multibyte fields are big-endian."""
    _require_exact_length(payload, 31, "INFO notification")
    _require_opcode(payload, NOTIFY_INFO, "INFO notification")
    return RingInfo(
        read_sequence=_read_uint(payload, 1, 8, "big"),
        write_sequence=_read_uint(payload, 9, 8, "big"),
        capacity_packets=_read_uint(payload, 17, 4, "big"),
        dropped_packets=_read_uint(payload, 21, 8, "big"),
        packet_size=_read_uint(payload, 29, 2, "big"),
    )


def parse_ack_notification(payload: bytes) -> AckNotification:
    """Parse an ACK notification containing a one-byte status."""
    _require_exact_length(payload, 2, "ACK notification")
    _require_opcode(payload, NOTIFY_ACK, "ACK notification")
    return AckNotification(status=payload[1])


def parse_done_notification(payload: bytes) -> DoneNotification:
    _require_exact_length(payload, 10, "DONE notification")
    _require_opcode(payload, NOTIFY_DONE, "DONE notification")
    return DoneNotification(status=payload[1], next_sequence=_read_uint(payload, 2, 8, "big"))


def parse_read_begin_notification(payload: bytes) -> ReadBeginNotification:
    _require_exact_length(payload, 13, "READ_BEGIN notification")
    _require_opcode(payload, NOTIFY_READ_BEGIN, "READ_BEGIN notification")
    return ReadBeginNotification(
        transfer_start_sequence=_read_uint(payload, 1, 8, "big"),
        packet_count=_read_uint(payload, 9, 4, "big"),
    )


def parse_data_notification(payload: bytes) -> bytes:
    """Strip the DATA opcode without assuming record-aligned notifications."""
    if not payload:
        raise RingProtocolError("DATA notification is empty")
    _require_opcode(payload, NOTIFY_DATA, "DATA notification")
    return payload[1:]


def encode_info_command() -> bytes:
    return bytes((CMD_INFO,))


def encode_stop_command() -> bytes:
    return bytes((CMD_STOP,))


def encode_read_command(start_sequence: int, packet_count: int | None = None) -> bytes:
    """Encode READ; absent or zero count means all currently available data."""
    _require_uint(start_sequence, 64, "start_sequence")
    if packet_count is None or packet_count == 0:
        return _READ.pack(CMD_READ, start_sequence)
    _require_uint(packet_count, 32, "packet_count")
    return _READ_WITH_COUNT.pack(CMD_READ, start_sequence, packet_count)


def encode_advance_command(new_read_sequence: int) -> bytes:
    _require_uint(new_read_sequence, 64, "new_read_sequence")
    return _ADVANCE.pack(CMD_ADVANCE, new_read_sequence)


def parse_audio_payload(payload: bytes) -> tuple[bytes, ...]:
    """Extract packed Opus frames from a record's 440-byte audio payload.

    A zero size byte is padding.  A frame reaching or crossing the payload
    boundary is incomplete and terminates parsing, matching firmware behavior.
    """
    _require_exact_length(payload, AUDIO_PAYLOAD_SIZE, "audio payload")
    frames: list[bytes] = []
    offset = 0
    while offset < len(payload) - 1:
        frame_size = payload[offset]
        if frame_size == 0:
            offset += 1
            continue
        frame_end = offset + 1 + frame_size
        if frame_end >= len(payload):
            break
        frames.append(payload[offset + 1 : frame_end])
        offset = frame_end
    return tuple(frames)


class RingRecordAssembler:
    """Reassemble arbitrarily split DATA bytes into fixed-size ring records."""

    def __init__(self) -> None:
        self._pending = bytearray()

    @property
    def pending_bytes(self) -> int:
        return len(self._pending)

    def append(self, payload: bytes) -> tuple[RingRecord, ...]:
        self._pending.extend(payload)
        records: list[RingRecord] = []
        while len(self._pending) >= RECORD_SIZE:
            record = bytes(self._pending[:RECORD_SIZE])
            del self._pending[:RECORD_SIZE]
            records.append(RingRecord.parse(record))
        return tuple(records)


def _require_exact_length(payload: bytes, expected: int, label: str) -> None:
    if len(payload) != expected:
        raise RingProtocolError(f"{label} must be exactly {expected} bytes; got {len(payload)}")


def _require_opcode(payload: bytes, expected: int, label: str) -> None:
    if payload[0] != expected:
        raise RingProtocolError(f"{label} has opcode 0x{payload[0]:02x}; expected 0x{expected:02x}")


def _require_uint(value: int, bits: int, label: str) -> None:
    if not 0 <= value < 1 << bits:
        raise RingProtocolError(f"{label} must fit in an unsigned {bits}-bit integer")


def _read_uint(payload: bytes, offset: int, size: int, byteorder: Literal["little", "big"]) -> int:
    return int.from_bytes(payload[offset : offset + size], byteorder)
