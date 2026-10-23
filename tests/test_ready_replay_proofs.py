from __future__ import annotations

import sqlite3
from struct import pack

import pytest

from omi_collector.capture.adapters.internal_audio_store import AUDIO_RECORD_SIZE
from omi_collector.capture.adapters.ready_bundles import _proof_page_scratch, _replay_proof_page_values
from omi_collector.capture.domain.replay_proof import unpack_page_index


def _zero_audio_record(timestamp: int) -> bytes:
    return pack(">I", timestamp) + bytes(AUDIO_RECORD_SIZE - 4)


def test_proof_page_scratch_spills_to_file_and_closes_on_error() -> None:
    record_count = 10_000
    records = (_zero_audio_record(index) for index in range(record_count))
    scratch_connection: sqlite3.Connection | None = None

    with (
        pytest.raises(RuntimeError, match="leave scratch"),
        _proof_page_scratch(records, record_count=record_count) as (page_index, scratch),
    ):
        scratch_connection = scratch
        assert scratch.execute("PRAGMA temp_store").fetchone() == (1,)
        assert scratch.execute("PRAGMA temp.cache_size").fetchone() == (-256,)

        indexed_pages = unpack_page_index(page_index, record_count=record_count)
        stored_pages = tuple(_replay_proof_page_values(scratch))
        assert len(stored_pages) == len(indexed_pages)
        assert tuple((row[1], row[2], row[4]) for row in stored_pages) == indexed_pages
        raise RuntimeError("leave scratch")

    assert scratch_connection is not None
    with pytest.raises(sqlite3.ProgrammingError, match="closed database"):
        scratch_connection.execute("SELECT 1")
