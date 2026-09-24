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
