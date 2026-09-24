"""Count complete 20 ms Opus packets in one fixed Omi ring record."""

from __future__ import annotations

_PAYLOAD_DATA_LIMIT = 439
_MAX_PACKET_SIZE = 160
_PACKET_SAMPLES = 960
_SILK_CONFIG_LIMIT = 12
_HYBRID_CONFIG_LIMIT = 16
_MAX_OPUS_FRAMES = 48
_PADDING_CONTINUATION = 255
_LENGTH_EXTENDED = 252
_TWO_FRAMES = 2


def count_20ms_packets(record: bytes) -> int:
    """Count valid 20 ms Opus packets; ignore zero padding and overflow tails."""
    payload = record[4:]
    offset = 0
    count = 0
    while offset < _PAYLOAD_DATA_LIMIT:
        size = payload[offset]
        offset += 1
        if size == 0:
            continue
        end = offset + size
        if end >= len(payload):
            if size > _MAX_PACKET_SIZE:
                raise ValueError("invalid Opus packet size")
            break
        if size > _MAX_PACKET_SIZE or not _is_20ms(payload[offset:end]):
            raise ValueError("invalid 20 ms Opus packet")
        count += 1
        offset = end
    return count


def _is_20ms(packet: bytes) -> bool:
    if not packet:
        return False
    toc = packet[0]
    config, code = toc >> 3, toc & 3
    duration = _duration(config)
    frames = _frame_count(packet, code)
    return frames is not None and frames * duration == _PACKET_SAMPLES


def _duration(config: int) -> int:
    if config < _SILK_CONFIG_LIMIT:
        return (480, 960, 1920, 2880)[config & 3]
    if config < _HYBRID_CONFIG_LIMIT:
        return (480, 960)[config & 1]
    return (120, 240, 480, 960)[config & 3]


def _frame_count(packet: bytes, code: int) -> int | None:
    if code == 0:
        frames = 1 if len(packet) > 1 else None
    elif code == 1:
        frames = _TWO_FRAMES if len(packet) > _TWO_FRAMES and (len(packet) - 1) % _TWO_FRAMES == 0 else None
    elif code == _TWO_FRAMES:
        frames = _vbr_two_count(packet)
    else:
        frames = _multi_count(packet)
    return frames


def _vbr_two_count(packet: bytes) -> int | None:
    first, cursor = _length(packet, 1, len(packet))
    return 2 if first is not None and first > 0 and cursor < len(packet) and cursor + first < len(packet) else None


def _multi_count(packet: bytes) -> int | None:
    if len(packet) < _TWO_FRAMES:
        return None
    control = packet[1]
    count = control & 0x3F
    valid = 1 <= count <= _MAX_OPUS_FRAMES
    cursor, end = 0, 0
    if valid:
        cursor, padding = _padding(packet, control)
        end = len(packet) - padding
        valid = cursor <= end
    if valid:
        if control & 0x80:
            valid = _valid_vbr_frames(packet, count, cursor, end)
        else:
            valid = _valid_cbr_frames(count, cursor, end)
    return count if valid else None


def _valid_vbr_frames(packet: bytes, count: int, cursor: int, end: int) -> bool:
    sizes: list[int] = []
    for _ in range(count - 1):
        size, cursor = _length(packet, cursor, end)
        if size is None or size == 0:
            return False
        sizes.append(size)
    return end - cursor - sum(sizes) > 0


def _valid_cbr_frames(count: int, cursor: int, end: int) -> bool:
    body = end - cursor
    return body > 0 and body % count == 0


def _padding(packet: bytes, control: int) -> tuple[int, int]:
    cursor, padding = 2, 0
    if not control & 0x40:
        return cursor, padding
    while cursor < len(packet):
        value = packet[cursor]
        cursor += 1
        padding += value
        if value != _PADDING_CONTINUATION:
            return cursor, padding
    return len(packet) + 1, padding


def _length(packet: bytes, cursor: int, end: int) -> tuple[int | None, int]:
    if cursor >= end:
        return None, cursor
    value = packet[cursor]
    cursor += 1
    if value < _LENGTH_EXTENDED:
        return value, cursor
    if cursor >= end:
        return None, cursor
    return value + 4 * packet[cursor], cursor + 1
