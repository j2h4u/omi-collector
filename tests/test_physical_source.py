from __future__ import annotations

import sqlite3
from contextlib import closing
from hashlib import sha256

import pytest

from omi_collector.capture.adapters.internal_audio_store import CaptureSourceIntegrityError
from omi_collector.capture.adapters.physical_source import authenticate_source, read_physical_span, source_identity
from omi_collector.capture.domain.physical_recovery import PhysicalSpan


def test_source_span_reads_exact_bytes_across_record_and_page_boundaries() -> None:
    with closing(sqlite3.connect(":memory:")) as connection:
        connection.execute(
            "CREATE TABLE capture_source(source_id TEXT, committed_count INTEGER, committed_sha256 TEXT)"
        )
        connection.execute(
            "CREATE TABLE capture_record(source_id TEXT, ordinal INTEGER, device_timestamp INTEGER, payload BLOB)"
        )
        connection.execute("INSERT INTO capture_source VALUES ('source', 3, ?)", ("a" * 64,))
        for index in range(3):
            connection.execute(
                "INSERT INTO capture_record VALUES ('source', ?, ?, ?)", (index, index, bytes([index]) * 440)
            )
        expected = b"".join(index.to_bytes(4, "big") + bytes([index]) * 440 for index in range(3))
        assert source_identity(connection, "source") == (len(expected), "a" * 64)
        with pytest.raises(ValueError, match="digest"):
            authenticate_source(connection, "source")
        connection.execute("UPDATE capture_source SET committed_sha256=?", (sha256(expected).hexdigest(),))
        assert authenticate_source(connection, "source") == (len(expected), sha256(expected).hexdigest())
        assert b"".join(read_physical_span(connection, PhysicalSpan("source", 437, 460), 1)) == expected[437:897]
        with pytest.raises(ValueError, match="exceeds"):
            list(read_physical_span(connection, PhysicalSpan("source", 1331, 2)))
        connection.execute("DELETE FROM capture_record WHERE ordinal=1")
        with pytest.raises(CaptureSourceIntegrityError, match="ordinal"):
            list(read_physical_span(connection, PhysicalSpan("source", 0, 888)))
