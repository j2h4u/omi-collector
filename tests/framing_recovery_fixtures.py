"""Synthetic protocol inputs for damaged-read framing tests; contains no audio."""

from __future__ import annotations

from dataclasses import dataclass

from omi_collector.capture.domain.ring_protocol import NOTIFY_DATA, NOTIFY_DONE, NOTIFY_READ_BEGIN, RECORD_SIZE

NOV_6_BYTE_SHIFT = 88
NOV_6_BYTE_DEFICIT = 7_904
NOV_6_OVERLAP_BYTES = 6 * RECORD_SIZE + 356


@dataclass(frozen=True, slots=True)
class SyntheticRead:
    start_sequence: int
    packet_count: int
    done_sequence: int
    data_chunks: tuple[bytes, ...]

    @property
    def raw_bytes(self) -> bytes:
        return b"".join(self.data_chunks)

    @property
    def notifications(self) -> tuple[bytes, ...]:
        begin = (
            bytes((NOTIFY_READ_BEGIN,)) + self.start_sequence.to_bytes(8, "big") + self.packet_count.to_bytes(4, "big")
        )
        data = tuple(bytes((NOTIFY_DATA,)) + chunk for chunk in self.data_chunks)
        done = bytes((NOTIFY_DONE, 0)) + self.done_sequence.to_bytes(8, "big")
        return (begin, *data, done)


@dataclass(frozen=True, slots=True)
class FramingRecoveryCorpus:
    verified: bytes
    shifted_read: SyntheticRead
    exact_overlap_read: SyntheticRead
    ambiguous_read: SyntheticRead
    adjacent_history: bytes
    whole_block_loss_history: bytes


def _record(identity: int) -> bytes:
    packet = bytes((8, identity % 251 + 1))
    payload = bytes((len(packet),)) + packet
    return identity.to_bytes(4, "big") + payload + bytes(RECORD_SIZE - 4 - len(payload))


def _chunks(raw: bytes, sizes: tuple[int, ...]) -> tuple[bytes, ...]:
    chunks: list[bytes] = []
    offset = 0
    for size in sizes:
        if offset == len(raw):
            break
        end = min(offset + size, len(raw))
        chunks.append(raw[offset:end])
        offset = end
    if offset < len(raw):
        chunks.append(raw[offset:])
    return tuple(chunks)


def framing_recovery_corpus() -> FramingRecoveryCorpus:
    """Return wire-shaped Nov 6 cases with deterministic synthetic block bytes."""
    verified_records = tuple(_record(identity) for identity in range(100, 110))
    verified = b"".join(verified_records)
    overlap = verified[:NOV_6_OVERLAP_BYTES]
    shifted_raw = b"\xf0" * NOV_6_BYTE_SHIFT + overlap + b"\xf1" * NOV_6_BYTE_SHIFT
    shift_packet_count = (len(shifted_raw) + NOV_6_BYTE_DEFICIT) // RECORD_SIZE
    shifted = SyntheticRead(700, shift_packet_count, 700 + shift_packet_count, _chunks(shifted_raw, (443, 1, 1_777)))

    exact = SyntheticRead(800, 7, 807, _chunks(overlap, (443, 1, 1_777)))

    left = tuple(_record(identity) for identity in range(1_000, 1_003))
    repeated = _record(2_000)
    right = tuple(_record(identity) for identity in range(3_000, 3_003))
    adjacent_history = b"".join((*left, repeated, repeated, *right))
    whole_block_loss_history = b"".join((*left, repeated, repeated, repeated, repeated, *right))
    ambiguous = SyntheticRead(900, 10, 910, _chunks(adjacent_history, (1_331, 1, 889)))

    return FramingRecoveryCorpus(verified, shifted, exact, ambiguous, adjacent_history, whole_block_loss_history)
