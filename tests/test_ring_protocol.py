from collections.abc import Callable
from struct import pack

import pytest

from omi_collector.capture.domain.ring_protocol import (
    AUDIO_PAYLOAD_SIZE,
    RECORD_SIZE,
    AckNotification,
    RingProtocolError,
    RingRecord,
    RingRecordAssembler,
    encode_advance_command,
    encode_info_command,
    encode_read_command,
    encode_stop_command,
    parse_ack_notification,
    parse_audio_payload,
    parse_data_notification,
    parse_done_notification,
    parse_info_notification,
    parse_read_begin_notification,
    parse_status,
)


def test_parse_status_uses_little_endian_fields() -> None:
    status = parse_status(pack("<IIII", 0x01020304, 12, 34, 1))

    assert (status.used_bytes, status.unread_packets, status.free_bytes) == (0x01020304, 12, 34)
    assert status.has_valid_rtc


@pytest.mark.parametrize("size", [0, 15, 17])
def test_parse_status_rejects_noncanonical_lengths(size: int) -> None:
    with pytest.raises(RingProtocolError, match="exactly 16 bytes"):
        parse_status(bytes(size))


def test_parse_info_notification_uses_big_endian_fields() -> None:
    info = parse_info_notification(pack(">BQQIQH", 0x02, 7, 19, 1024, 3, RECORD_SIZE))

    assert info.read_sequence == 7
    assert info.write_sequence == 19
    assert info.unread_packets == 12
    assert info.capacity_packets == 1024
    assert info.dropped_packets == 3
    assert info.packet_size == RECORD_SIZE


def test_parse_done_and_read_begin_notifications() -> None:
    done = parse_done_notification(pack(">BBQ", 0x04, 0, 99))
    started = parse_read_begin_notification(pack(">BQI", 0x05, 80, 19))

    assert done.is_ok
    assert done.next_sequence == 99
    assert started.transfer_start_sequence == 80
    assert started.packet_count == 19


def test_parse_ack_notification_returns_status_and_success() -> None:
    ack = parse_ack_notification(b"\x01\x00")

    assert isinstance(ack, AckNotification)
    assert ack.status == 0
    assert ack.is_ok


def test_parse_ack_notification_returns_nonzero_status() -> None:
    ack = parse_ack_notification(b"\x01\x09")

    assert ack.status == 9
    assert not ack.is_ok


@pytest.mark.parametrize(
    ("parser", "payload"),
    [
        (parse_info_notification, bytes(30)),
        (parse_ack_notification, bytes(1)),
        (parse_done_notification, bytes(9)),
        (parse_read_begin_notification, bytes(12)),
    ],
)
def test_notification_parsers_reject_truncation(parser: Callable[[bytes], object], payload: bytes) -> None:
    with pytest.raises(RingProtocolError, match="exactly"):
        parser(payload)


@pytest.mark.parametrize(
    ("parser", "payload"),
    [
        (parse_info_notification, pack(">BQQIQH", 0x01, 0, 0, 0, 0, RECORD_SIZE)),
        (parse_ack_notification, b"\x02\x00"),
        (parse_done_notification, pack(">BBQ", 0x03, 0, 0)),
        (parse_read_begin_notification, pack(">BQI", 0x04, 0, 0)),
    ],
)
def test_notification_parsers_reject_wrong_opcode(parser: Callable[[bytes], object], payload: bytes) -> None:
    with pytest.raises(RingProtocolError, match="opcode"):
        parser(payload)


def test_parse_ack_notification_rejects_overlong_payload() -> None:
    with pytest.raises(RingProtocolError, match="exactly 2 bytes"):
        parse_ack_notification(b"\x01\x00\x00")


def test_data_notification_strips_only_opcode() -> None:
    assert parse_data_notification(b"\x03\x00\xff") == b"\x00\xff"


@pytest.mark.parametrize("payload", [b"", b"\x02data"])
def test_data_notification_rejects_malformed_payload(payload: bytes) -> None:
    with pytest.raises(RingProtocolError):
        parse_data_notification(payload)


def test_command_encoders_preserve_wire_endianness_and_optional_count() -> None:
    assert encode_info_command() == b"\x10"
    assert encode_stop_command() == b"\x03"
    assert encode_read_command(0x0102030405060708) == bytes.fromhex("110102030405060708")
    assert encode_read_command(7, 0) == bytes.fromhex("110000000000000007")
    assert encode_read_command(7, 9) == bytes.fromhex("11000000000000000700000009")
    assert encode_advance_command(0x0102030405060708) == bytes.fromhex("120102030405060708")


@pytest.mark.parametrize(
    ("call", "match"),
    [
        (lambda: encode_read_command(-1), "start_sequence"),
        (lambda: encode_read_command(1 << 64), "start_sequence"),
        (lambda: encode_read_command(0, -1), "packet_count"),
        (lambda: encode_read_command(0, 1 << 32), "packet_count"),
        (lambda: encode_advance_command(1 << 64), "new_read_sequence"),
    ],
)
def test_command_encoders_reject_out_of_range_values(call: Callable[[], bytes], match: str) -> None:
    with pytest.raises(RingProtocolError, match=match):
        call()


def test_parse_audio_payload_handles_frames_and_zero_padding() -> None:
    payload = b"\x02ab\x00\x03cde" + bytes(AUDIO_PAYLOAD_SIZE - 8)

    assert parse_audio_payload(payload) == (b"ab", b"cde")


def test_parse_audio_payload_stops_at_terminal_size_byte() -> None:
    payload = bytearray(AUDIO_PAYLOAD_SIZE)
    payload[-2] = 1
    payload[-1] = ord("x")

    assert parse_audio_payload(bytes(payload)) == ()


def test_parse_audio_payload_stops_when_frame_crosses_boundary() -> None:
    payload = bytearray(AUDIO_PAYLOAD_SIZE)
    payload[-3] = 3
    payload[-2:] = b"xy"

    assert parse_audio_payload(bytes(payload)) == ()


def test_parse_audio_payload_rejects_wrong_size() -> None:
    with pytest.raises(RingProtocolError, match="exactly 440 bytes"):
        parse_audio_payload(bytes(AUDIO_PAYLOAD_SIZE - 1))


def _record(timestamp: int, marker: int) -> bytes:
    return timestamp.to_bytes(4, "big") + bytes((marker,)) * AUDIO_PAYLOAD_SIZE


def test_ring_record_parses_timestamp_and_payload() -> None:
    audio_payload = b"\x02ab" + bytes(AUDIO_PAYLOAD_SIZE - 3)
    record = RingRecord.parse(0x01020304.to_bytes(4, "big") + audio_payload)

    assert record.timestamp == 0x01020304
    assert record.audio_payload == audio_payload
    assert record.opus_frames() == (b"ab",)


def test_ring_record_rejects_wrong_size() -> None:
    with pytest.raises(RingProtocolError, match=f"exactly {RECORD_SIZE} bytes"):
        RingRecord.parse(bytes(RECORD_SIZE - 1))


def test_assembler_handles_unaligned_chunks_and_multiple_records() -> None:
    first = _record(1, 0x11)
    second = _record(2, 0x22)
    assembler = RingRecordAssembler()

    assert assembler.append(first[:100]) == ()
    assert assembler.pending_bytes == 100
    records = assembler.append(first[100:] + second + b"tail")

    assert [record.timestamp for record in records] == [1, 2]
    assert assembler.pending_bytes == 4


def test_assembler_accepts_empty_chunks_without_changing_state() -> None:
    assembler = RingRecordAssembler()
    assert assembler.append(b"") == ()
    assert assembler.pending_bytes == 0
