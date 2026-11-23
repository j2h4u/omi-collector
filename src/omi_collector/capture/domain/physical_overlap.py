"""Prove or reject exact overlap between independently retained physical reads."""

from __future__ import annotations

from collections.abc import Iterable
from hashlib import sha256

from .physical_recovery import RECORD_SIZE


def _record_hash(value: bytes) -> bytes:
    if not isinstance(value, bytes) or len(value) != RECORD_SIZE:
        raise ValueError("physical overlap record is invalid")
    return sha256(value).digest()


def _prefix_borders(pattern: list[bytes]) -> list[int]:
    borders = [0] * len(pattern)
    candidate = 0
    for index in range(1, len(pattern)):
        while candidate and pattern[index] != pattern[candidate]:
            candidate = borders[candidate - 1]
        if pattern[index] == pattern[candidate]:
            candidate += 1
        borders[index] = candidate
    return borders


def unique_suffix_prefix_overlap(previous: Iterable[bytes], following: Iterable[bytes]) -> int:
    """Find an unambiguous exact suffix/prefix using compact per-record fingerprints."""
    pattern = [_record_hash(record) for record in following]
    if not pattern:
        return 0
    borders = _prefix_borders(pattern)
    matched = 0
    complete_match = False
    for record in previous:
        value = _record_hash(record)
        if matched == len(pattern):
            complete_match = True
            matched = borders[matched - 1]
        while matched and value != pattern[matched]:
            matched = borders[matched - 1]
        if value == pattern[matched]:
            matched += 1
    if complete_match:
        raise ValueError("physical source is a complete replay within its predecessor")
    if matched and borders[matched - 1]:
        raise ValueError("physical overlap has multiple valid suffix anchors")
    return matched
