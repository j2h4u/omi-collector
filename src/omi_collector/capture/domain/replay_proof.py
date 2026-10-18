"""Canonical normalization and page proofs for private replay data."""

from __future__ import annotations

import hashlib
import math
import struct
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from enum import IntEnum
from typing import cast

RECORD_BYTES = 444
HASH_BYTES = 32
MAX_PAGE_RECORDS = 128
_BOUNDARY_MASK = 0b11
_UINT32_MAX = (1 << 32) - 1
_UINT64_MAX = (1 << 64) - 1
_RUN = struct.Struct(">QQdBB6x")
_PAGE_INDEX_ENTRY = struct.Struct(">QH32s")
_PAGE_DOMAIN = b"omi-collector-replay-proof-page-v1\0"


class NormalizationMode(IntEnum):
    RAW = 0
    MAPPED = 1


@dataclass(frozen=True, slots=True)
class NormalizationRun:
    first_record_index: int
    next_record_index: int
    offset_seconds: float
    mode: NormalizationMode
    boundary_before: int


@dataclass(frozen=True, slots=True)
class ProofPage:
    first_record_index: int
    record_count: int
    hashes: bytes
    page_sha256: bytes


def pack_normalization_runs(runs: Iterable[NormalizationRun], *, record_count: int) -> bytes:
    _validate_count(record_count)
    packed = bytearray()
    next_index = 0
    for run in runs:
        _validate_run(run)
        if run.first_record_index != next_index or run.next_record_index > record_count:
            raise ValueError("normalization runs must be sorted and exhaustive")
        packed.extend(
            _RUN.pack(
                run.first_record_index,
                run.next_record_index,
                run.offset_seconds,
                int(run.mode),
                run.boundary_before,
            )
        )
        next_index = run.next_record_index
    if next_index != record_count:
        raise ValueError("normalization runs must cover every record")
    return bytes(packed)


def unpack_normalization_runs(blob: bytes, *, record_count: int) -> tuple[NormalizationRun, ...]:
    _validate_count(record_count)
    if not isinstance(blob, bytes) or len(blob) % _RUN.size:
        raise ValueError("normalization run blob has invalid length")
    runs: list[NormalizationRun] = []
    next_index = 0
    for offset in range(0, len(blob), _RUN.size):
        first, end, seconds, mode_value, boundary = cast(
            tuple[int, int, float, int, int], _RUN.unpack_from(blob, offset)
        )
        reserved = blob[offset + 26 : offset + _RUN.size]
        if reserved != bytes(6):
            raise ValueError("normalization run reserved bytes must be zero")
        try:
            mode = NormalizationMode(mode_value)
        except ValueError:
            raise ValueError("normalization mode is invalid") from None
        run = NormalizationRun(first, end, seconds, mode, boundary)
        _validate_run(run)
        if first != next_index or end > record_count:
            raise ValueError("normalization runs must be sorted and exhaustive")
        runs.append(run)
        next_index = end
    if next_index != record_count:
        raise ValueError("normalization runs must cover every record")
    return tuple(runs)


def normalize_record(record: bytes, run: NormalizationRun) -> bytes:
    _validate_run(run)
    if not isinstance(record, bytes) or len(record) != RECORD_BYTES:
        raise ValueError(f"record must be exactly {RECORD_BYTES} bytes")
    if run.mode is NormalizationMode.RAW:
        return record
    timestamp = int.from_bytes(record[:4], "big")
    normalized = math.floor(timestamp + run.offset_seconds + 0.5)
    if not 0 <= normalized <= _UINT32_MAX:
        raise ValueError("normalized timestamp is outside uint32")
    return normalized.to_bytes(4, "big") + record[4:]


def record_sha256(normalized_record: bytes) -> bytes:
    if not isinstance(normalized_record, bytes) or len(normalized_record) != RECORD_BYTES:
        raise ValueError(f"record must be exactly {RECORD_BYTES} bytes")
    return hashlib.sha256(normalized_record).digest()


def build_proof_pages(records: Iterable[bytes]) -> Iterator[ProofPage]:
    first_index = 0
    hashes = bytearray()
    count = 0
    for record in records:
        hashes.extend(record_sha256(record))
        count += 1
        if count == MAX_PAGE_RECORDS:
            yield _make_page(first_index, count, bytes(hashes))
            first_index += count
            hashes.clear()
            count = 0
    if count:
        yield _make_page(first_index, count, bytes(hashes))


def validate_proof_page(page: ProofPage, *, expected_first_record_index: int | None = None) -> tuple[bytes, ...]:
    if not isinstance(page, ProofPage):
        raise ValueError("proof page is invalid")
    _validate_index(page.first_record_index)
    if type(page.record_count) is not int or not 1 <= page.record_count <= MAX_PAGE_RECORDS:
        raise ValueError("proof page record count is invalid")
    if page.first_record_index + page.record_count > _UINT64_MAX:
        raise ValueError("proof page range is outside uint64")
    if expected_first_record_index is not None:
        _validate_index(expected_first_record_index)
        if page.first_record_index != expected_first_record_index:
            raise ValueError("proof page index is unexpected")
    if not isinstance(page.hashes, bytes) or len(page.hashes) != page.record_count * HASH_BYTES:
        raise ValueError("proof page hashes have invalid length")
    if not isinstance(page.page_sha256, bytes) or len(page.page_sha256) != HASH_BYTES:
        raise ValueError("proof page hash has invalid length")
    header = struct.pack(">QH", page.first_record_index, page.record_count)
    digest = hashlib.sha256(_PAGE_DOMAIN + header + page.hashes).digest()
    if digest != page.page_sha256:
        raise ValueError("proof page hash does not match its contents")
    return tuple(page.hashes[offset : offset + HASH_BYTES] for offset in range(0, len(page.hashes), HASH_BYTES))


def pack_page_index(pages: Iterable[ProofPage], *, record_count: int) -> bytes:
    _validate_count(record_count)
    packed = bytearray()
    next_index = 0
    for page in pages:
        validate_proof_page(page, expected_first_record_index=next_index)
        if page.first_record_index + page.record_count > record_count:
            raise ValueError("proof pages exceed record count")
        packed.extend(_PAGE_INDEX_ENTRY.pack(page.first_record_index, page.record_count, page.page_sha256))
        next_index += page.record_count
    if next_index != record_count:
        raise ValueError("proof pages must cover every record")
    return bytes(packed)


def unpack_page_index(blob: bytes, *, record_count: int) -> tuple[tuple[int, int, bytes], ...]:
    _validate_count(record_count)
    if not isinstance(blob, bytes) or len(blob) % _PAGE_INDEX_ENTRY.size:
        raise ValueError("page index blob has invalid length")
    pages: list[tuple[int, int, bytes]] = []
    next_index = 0
    for offset in range(0, len(blob), _PAGE_INDEX_ENTRY.size):
        first, count, digest = cast(tuple[int, int, bytes], _PAGE_INDEX_ENTRY.unpack_from(blob, offset))
        if first != next_index or not 1 <= count <= MAX_PAGE_RECORDS or first + count > record_count:
            raise ValueError("page index entries must be contiguous and exhaustive")
        pages.append((first, count, digest))
        next_index += count
    if next_index != record_count:
        raise ValueError("page index entries must cover every record")
    return tuple(pages)


def _make_page(first_index: int, count: int, hashes: bytes) -> ProofPage:
    header = struct.pack(">QH", first_index, count)
    return ProofPage(first_index, count, hashes, hashlib.sha256(_PAGE_DOMAIN + header + hashes).digest())


def _validate_run(run: NormalizationRun) -> None:
    if not isinstance(run, NormalizationRun):
        raise ValueError("normalization run is invalid")
    if (
        type(run.first_record_index) is not int
        or type(run.next_record_index) is not int
        or not 0 <= run.first_record_index < run.next_record_index <= _UINT64_MAX
    ):
        raise ValueError("normalization run is invalid")
    if type(run.offset_seconds) is not float or not math.isfinite(run.offset_seconds):
        raise ValueError("normalization run is invalid")
    if (
        not isinstance(run.mode, NormalizationMode)
        or type(run.boundary_before) is not int
        or not 0 <= run.boundary_before <= _BOUNDARY_MASK
    ):
        raise ValueError("normalization run is invalid")
    if run.mode is NormalizationMode.RAW and (run.offset_seconds != 0 or math.copysign(1.0, run.offset_seconds) < 0):
        raise ValueError("normalization run is invalid")


def _validate_count(count: int) -> None:
    if type(count) is not int or not 0 <= count <= _UINT64_MAX:
        raise ValueError("record count is outside uint64")


def _validate_index(index: int) -> None:
    if type(index) is not int or not 0 <= index <= _UINT64_MAX:
        raise ValueError("record index is outside uint64")
