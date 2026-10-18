from __future__ import annotations

import hashlib
import struct
from dataclasses import replace

import pytest

from omi_collector.capture.domain.replay_proof import (
    HASH_BYTES,
    MAX_PAGE_RECORDS,
    RECORD_BYTES,
    NormalizationMode,
    NormalizationRun,
    build_proof_pages,
    normalize_record,
    pack_normalization_runs,
    pack_page_index,
    record_sha256,
    unpack_normalization_runs,
    unpack_page_index,
    validate_proof_page,
)


def _record(timestamp: int = 0, *, tail: bytes = b"") -> bytes:
    return timestamp.to_bytes(4, "big") + tail[: RECORD_BYTES - 4].ljust(RECORD_BYTES - 4, b"\xa5")


def _mapped_run(offset: float = 0.0) -> NormalizationRun:
    return NormalizationRun(0, 1, offset, NormalizationMode.MAPPED, 0)


@pytest.mark.parametrize(
    "count,expected_counts,expected_starts",
    (
        (0, [], []),
        (1, [1], [0]),
        (MAX_PAGE_RECORDS, [MAX_PAGE_RECORDS], [0]),
        (MAX_PAGE_RECORDS + 1, [MAX_PAGE_RECORDS, 1], [0, MAX_PAGE_RECORDS]),
    ),
)
def test_proof_pages_cover_empty_single_and_page_boundaries(
    count: int, expected_counts: list[int], expected_starts: list[int]
) -> None:
    pages = tuple(build_proof_pages(_record(index) for index in range(count)))
    assert [page.record_count for page in pages] == expected_counts
    assert [page.first_record_index for page in pages] == expected_starts
    assert all(len(validate_proof_page(page)) == page.record_count for page in pages)
    assert sum(page.record_count for page in pages) == count


def test_proof_page_builder_consumes_only_one_page_before_yielding() -> None:
    consumed = 0

    def records():
        nonlocal consumed
        for _ in range(1000):
            consumed += 1
            yield bytes(RECORD_BYTES)

    pages = build_proof_pages(records())
    assert next(pages).record_count == MAX_PAGE_RECORDS
    assert consumed == MAX_PAGE_RECORDS


def test_proof_page_digest_detects_changed_hash_and_metadata() -> None:
    page = next(build_proof_pages((_record(),)))
    with pytest.raises(ValueError, match="does not match"):
        validate_proof_page(replace(page, hashes=bytes(HASH_BYTES)))
    with pytest.raises(ValueError, match="unexpected"):
        validate_proof_page(page, expected_first_record_index=1)
    with pytest.raises(ValueError, match="length"):
        validate_proof_page(replace(page, hashes=page.hashes[:-1]))


def test_normalization_runs_round_trip_and_reject_bad_reserved_or_coverage() -> None:
    runs = (
        NormalizationRun(0, 2, 0.0, NormalizationMode.RAW, 0),
        NormalizationRun(2, 4, -0.25, NormalizationMode.MAPPED, 3),
    )
    blob = pack_normalization_runs(runs, record_count=4)
    assert len(blob) == 2 * 32
    assert unpack_normalization_runs(blob, record_count=4) == runs
    with pytest.raises(ValueError, match="reserved"):
        unpack_normalization_runs(blob[:26] + b"\x01" + blob[27:], record_count=4)
    with pytest.raises(ValueError, match="exhaustive"):
        pack_normalization_runs((runs[1],), record_count=4)


@pytest.mark.parametrize("value", (True, -0.0, 1.0))
def test_raw_run_rejects_noncanonical_or_invalid_fields(value: float) -> None:
    with pytest.raises(ValueError):
        pack_normalization_runs((NormalizationRun(0, 1, value, NormalizationMode.RAW, 0),), record_count=1)


def test_normalization_uses_half_up_rounding_and_preserves_all_remaining_bytes() -> None:
    record = _record(10, tail=bytes(range(256)) * 2)
    normalized = normalize_record(record, _mapped_run(0.5))
    assert int.from_bytes(normalized[:4], "big") == 11
    assert normalized[4:] == record[4:]
    raw = NormalizationRun(0, 1, 0.0, NormalizationMode.RAW, 0)
    assert normalize_record(record, raw) == record


@pytest.mark.parametrize("timestamp,offset", ((0, -0.51), ((1 << 32) - 1, 0.5)))
def test_normalization_rejects_uint32_overflow(timestamp: int, offset: float) -> None:
    with pytest.raises(ValueError, match="uint32"):
        normalize_record(_record(timestamp), _mapped_run(offset))


def test_record_hash_and_page_index_round_trip() -> None:
    records = (_record(1), _record(2))
    assert record_sha256(records[0]) == hashlib.sha256(records[0]).digest()
    pages = tuple(build_proof_pages(records))
    blob = pack_page_index(pages, record_count=2)
    assert len(blob) == struct.calcsize(">QH32s")
    assert unpack_page_index(blob, record_count=2) == ((0, 2, pages[0].page_sha256),)
    with pytest.raises(ValueError, match="cover"):
        unpack_page_index(blob, record_count=3)


@pytest.mark.parametrize(
    "run",
    (
        NormalizationRun(True, 1, 0.0, NormalizationMode.MAPPED, 0),
        NormalizationRun(0, True, 0.0, NormalizationMode.MAPPED, 0),
        NormalizationRun(0, 1, float("inf"), NormalizationMode.MAPPED, 0),
        NormalizationRun(0, 1, 0.0, NormalizationMode.MAPPED, True),
        NormalizationRun(0, 1, 0.0, NormalizationMode(0), 4),
    ),
)
def test_run_validation_rejects_invalid_types_and_ranges(run: NormalizationRun) -> None:
    with pytest.raises(ValueError):
        pack_normalization_runs((run,), record_count=1)


def test_codec_rejects_bad_record_types_and_truncated_inputs() -> None:
    with pytest.raises(ValueError):
        record_sha256(bytearray(RECORD_BYTES))  # type: ignore[arg-type]
    with pytest.raises(ValueError):
        normalize_record(bytes(RECORD_BYTES - 1), _mapped_run())
    with pytest.raises(ValueError, match="length"):
        unpack_normalization_runs(bytes(1), record_count=0)
    with pytest.raises(ValueError, match="length"):
        unpack_page_index(bytes(1), record_count=0)


def test_proof_page_validation_rejects_tampered_page_hash() -> None:
    page = next(build_proof_pages((_record(),)))
    with pytest.raises(ValueError, match="does not match"):
        validate_proof_page(replace(page, page_sha256=bytes(HASH_BYTES)))
