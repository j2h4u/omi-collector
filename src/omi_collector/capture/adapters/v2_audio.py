"""Fixed-profile Ogg Opus hashing and writing for v2 publications."""

from __future__ import annotations

from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from pathlib import Path

from .ogg_opus import OggOpusConfig, OggWriteResult, hash_ogg_opus, write_ogg_opus

FIXED_V2_OGG_CONFIG = OggOpusConfig(50, 960, 1, 0, 16_000, 0, 0, b"", ())


@dataclass(frozen=True, slots=True)
class V2AudioDigest:
    packet_count: int
    page_count: int
    byte_count: int
    sha256: str


def hash_v2_audio(packets: Iterable[bytes]) -> V2AudioDigest:
    """Hash one stream and count packets without requiring a reusable iterable."""
    result, packet_count = _hash_counted(packets)
    return V2AudioDigest(packet_count, result.page_count, result.bytes, result.sha256)


def write_v2_audio(path: Path, packets: Iterable[bytes], expected: V2AudioDigest) -> V2AudioDigest:
    """Write one stream, then require every frozen digest field to match."""
    count = 0

    def counted() -> Iterator[bytes]:
        nonlocal count
        for packet in packets:
            count += 1
            yield packet

    result = write_ogg_opus(path, counted(), 0, config=FIXED_V2_OGG_CONFIG)
    actual = V2AudioDigest(count, result.page_count, result.bytes, result.sha256)
    if actual.packet_count == 0:
        raise ValueError("packets must not be empty")
    if actual != expected:
        raise ValueError("written Ogg audio does not match the frozen digest")
    return actual


def _hash_counted(packets: Iterable[bytes]) -> tuple[OggWriteResult, int]:
    count = 0

    def counted() -> Iterator[bytes]:
        nonlocal count
        for packet in packets:
            count += 1
            yield packet

    result = hash_ogg_opus(counted(), 0, config=FIXED_V2_OGG_CONFIG)
    if count == 0:
        raise ValueError("packets must not be empty")
    return result, count
