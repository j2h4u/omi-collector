"""Private physical-byte provenance for a public v2 recovery artifact."""

from __future__ import annotations

import re
from typing import cast

_RECORD_BYTES = 444
_SOURCE_ID = re.compile(r"[0-9a-f]{32}\Z")
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_FORMAT = "omi-audio-v2-physical-byte-span-v1"
_ORDERED_FORMAT = "omi-audio-v2-ordered-physical-v1"


def _mapping(value: object, fields: set[str]) -> dict[str, object]:
    if not isinstance(value, dict):
        raise ValueError("physical descriptor object is invalid")
    data = cast(dict[str, object], value)
    if set(data) != fields:
        raise ValueError("physical descriptor fields are invalid")
    return data


def _number(value: object, *, positive: bool = False) -> int:
    if type(value) is not int or value < (1 if positive else 0):
        raise ValueError("physical descriptor offset is invalid")
    return value


def _identity(value: object, pattern: re.Pattern[str]) -> str:
    if not isinstance(value, str) or pattern.fullmatch(value) is None:
        raise ValueError("physical descriptor identity is invalid")
    return value


def _ranges(value: object, *, seam: bool) -> list[tuple[int, int]]:
    if not isinstance(value, list):
        raise ValueError("physical descriptor ranges are invalid")
    items = cast(list[object], value)
    result: list[tuple[int, int]] = []
    for item in items:
        fields = {"byte_start", "byte_end", "sha256"}
        if seam:
            fields.update({"status", "decoded_prefix_bytes", "decoded_packet_count", "decoded_packets_sha256"})
        data = _mapping(item, fields)
        start = _number(data["byte_start"])
        end = _number(data["byte_end"])
        _identity(data["sha256"], _SHA256)
        if end <= start or (seam and data["status"] != "unresolved"):
            raise ValueError("physical descriptor range is invalid")
        if seam:
            prefix = _number(data["decoded_prefix_bytes"])
            count = _number(data["decoded_packet_count"])
            _identity(data["decoded_packets_sha256"], _SHA256)
            if prefix > end - start or (count == 0) != (prefix == 0):
                raise ValueError("physical seam output is invalid")
        if not seam and (end - start) % _RECORD_BYTES:
            raise ValueError("physical output span is not a complete record range")
        if result and start < result[-1][1]:
            raise ValueError("physical descriptor ranges are not ordered")
        result.append((start, end))
    return result


def validate_physical_descriptor(payload: object) -> dict[str, object]:
    """Validate complete source-byte coverage without assigning ring sequence IDs."""
    if not isinstance(payload, dict):
        raise ValueError("physical descriptor object is invalid")
    ordered = cast(dict[str, object], payload).get("format") == _ORDERED_FORMAT
    fields = {
        "format",
        "source_id",
        "source_sha256",
        "source_bytes",
        "spans",
        "record_count",
        "seams",
        "record_projection_sha256",
    }
    fields.update({"ordering", "overlap"} if ordered else {"anchor"})
    data = _mapping(cast(dict[str, object], payload), fields)
    if data["format"] not in {_FORMAT, _ORDERED_FORMAT}:
        raise ValueError("physical descriptor format is invalid")
    source_id = _identity(data["source_id"], _SOURCE_ID)
    _identity(data["source_sha256"], _SHA256)
    _identity(data["record_projection_sha256"], _SHA256)
    source_bytes = _number(data["source_bytes"], positive=True)
    records = _number(data["record_count"], positive=True)
    spans = _ranges(data["spans"], seam=False)
    seams = _ranges(data["seams"], seam=True)
    if not spans or sum(end - start for start, end in spans) != records * _RECORD_BYTES:
        raise ValueError("physical descriptor record count is invalid")
    if ordered and seams:
        raise ValueError("ordered physical source requires independently verified complete records")
    extra = _ordered_overlap(data, source_id) if ordered else _anchored_overlap(data, source_id)
    ranges = sorted((*spans, *seams, *extra))
    position = 0
    for lower, upper in ranges:
        if lower != position:
            raise ValueError("physical descriptor does not cover source bytes exactly")
        position = upper
    if position != source_bytes:
        raise ValueError("physical descriptor does not cover source bytes exactly")
    return data


def _anchored_overlap(data: dict[str, object], source_id: str) -> tuple[tuple[int, int], ...]:
    anchor = _mapping(
        data["anchor"],
        {"repeat_source_id", "repeat_source_sha256", "source_offset", "repeat_offset", "length", "sha256"},
    )
    if _identity(anchor["repeat_source_id"], _SOURCE_ID) == source_id:
        raise ValueError("physical anchor must use an independent source")
    _identity(anchor["repeat_source_sha256"], _SHA256)
    _identity(anchor["sha256"], _SHA256)
    start = _number(anchor["source_offset"])
    _number(anchor["repeat_offset"])
    length = _number(anchor["length"], positive=True)
    return ((start, start + length),)


def _ordered_overlap(data: dict[str, object], source_id: str) -> tuple[tuple[int, int], ...]:
    ordering = _mapping(
        data["ordering"],
        {
            "predecessor_artifact",
            "predecessor_source_id",
            "predecessor_sha256",
            "predecessor_start",
            "source_start",
            "read_begin",
            "read_count",
        },
    )
    _identity(ordering["predecessor_artifact"], _SHA256)
    if _identity(ordering["predecessor_source_id"], _SOURCE_ID) == source_id:
        raise ValueError("physical predecessor must be independent")
    _identity(ordering["predecessor_sha256"], _SHA256)
    before, current = _number(ordering["predecessor_start"]), _number(ordering["source_start"])
    _number(ordering["read_begin"])
    _number(ordering["read_count"], positive=True)
    if before >= current:
        raise ValueError("physical observation order is ambiguous")
    overlap = _mapping(data["overlap"], {"record_count", "sha256"})
    count = _number(overlap["record_count"])
    _identity(overlap["sha256"], _SHA256)
    return ((0, count * _RECORD_BYTES),) if count else ()
