from __future__ import annotations

import pytest

from omi_collector.capture.domain.opus_duration import count_20ms_packets
from omi_collector.capture.domain.ring_protocol import RECORD_SIZE


def _record(packet: bytes) -> bytes:
    payload = bytes((len(packet),)) + packet
    return bytes(4) + payload + bytes(RECORD_SIZE - 4 - len(payload))


def test_counts_actual_valid_20ms_packets_including_multiframe_opus() -> None:
    single_frame = bytes((8, 0x55))
    two_10ms_frames = bytes((3, 2, 0x55, 0x66))
    two_10ms_vbr_frames = bytes((3, 0x82, 1, 0x55, 0x66))

    assert count_20ms_packets(_record(single_frame)) == 1
    assert count_20ms_packets(_record(two_10ms_frames)) == 1
    assert count_20ms_packets(_record(two_10ms_vbr_frames)) == 1
    assert count_20ms_packets(bytes(4) + bytes(RECORD_SIZE - 4)) == 0


def test_rejects_malformed_opus_framing_instead_of_guessing_duration() -> None:
    with pytest.raises(ValueError, match="20 ms"):
        count_20ms_packets(_record(bytes((3, 0))))


@pytest.mark.parametrize(
    "packet",
    (
        bytes((1, 0x55, 0x66)),  # Two 10 ms SILK frames, code 1.
        bytes((2, 1, 0x55, 0x66)),  # Two 10 ms SILK frames, code 2 VBR.
        bytes((104, 0x55)),  # One 20 ms hybrid frame.
        bytes((152, 0x55)),  # One 20 ms CELT frame.
        bytes((131, 8, *range(8))),  # Eight 2.5 ms CELT frames, code 3 CBR.
        bytes((0x0B, 0x01, 0x55)),  # One 20 ms SILK frame, code 3 CBR.
        bytes((0x0B, 0x81, 0x55)),  # One 20 ms SILK frame, code 3 VBR.
    ),
)
def test_counts_other_valid_20ms_opus_framings(packet: bytes) -> None:
    assert count_20ms_packets(_record(packet)) == 1


@pytest.mark.parametrize(
    "packet",
    (
        bytes((0, 0x55)),  # 10 ms.
        bytes((16, 0x55)),  # 40 ms.
        bytes((24, 0x55)),  # 60 ms.
        bytes((1, 0x55, 0x66, 0x77)),  # Odd code-1 frame body.
        bytes((2, 2, 0x55, 0x66)),  # VBR first frame consumes the whole body.
    ),
)
def test_rejects_wrong_duration_and_malformed_silk_framing(packet: bytes) -> None:
    with pytest.raises(ValueError, match="20 ms"):
        count_20ms_packets(_record(packet))


def test_accepts_maximum_packet_size_and_rejects_one_byte_over() -> None:
    maximum = bytes((8,)) + bytes(159)
    oversized = bytes((8,)) + bytes(160)

    assert count_20ms_packets(_record(maximum)) == 1
    with pytest.raises(ValueError):
        count_20ms_packets(_record(oversized))


def test_rejects_header_only_code3_padding() -> None:
    with pytest.raises(ValueError, match="20 ms"):
        count_20ms_packets(_record(bytes((0x0B, 0x41))))


@pytest.mark.parametrize(
    "packet",
    (
        bytes((2, 252, 0, 1)),
        bytes((2, 252, 32)) + bytes(157),
    ),
)
def test_rejects_extended_vbr_lengths_that_exceed_the_packet(packet: bytes) -> None:
    with pytest.raises(ValueError, match="20 ms"):
        count_20ms_packets(_record(packet))


def test_ignores_maximum_sized_packet_when_it_reaches_the_record_end() -> None:
    payload = bytes(279) + bytes((160,)) + bytes((8,)) + bytes(159)
    record = bytes(4) + payload

    assert len(record) == RECORD_SIZE
    assert count_20ms_packets(record) == 0
