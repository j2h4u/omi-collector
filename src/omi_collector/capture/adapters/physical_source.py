"""Read retained source bytes in bounded pages without assigning sequence IDs."""

from __future__ import annotations

import sqlite3
from collections.abc import Iterator
from contextlib import closing
from hashlib import sha256
from typing import cast

from omi_collector.capture.domain.physical_recovery import RECORD_SIZE, PhysicalSpan

from .internal_audio_store import _authenticated_record_bytes


def source_identity(connection: sqlite3.Connection, source_id: str) -> tuple[int, str]:
    row = cast(
        tuple[object, object] | None,
        connection.execute(
            "SELECT committed_count, committed_sha256 FROM capture_source WHERE source_id=?", (source_id,)
        ).fetchone(),
    )
    if row is None or type(row[0]) is not int or not isinstance(row[1], str):
        raise ValueError("physical source custody is unavailable")
    return row[0] * RECORD_SIZE, row[1]


def authenticate_source(connection: sqlite3.Connection, source_id: str) -> tuple[int, str]:
    """Authenticate all retained source bytes in the caller's read snapshot."""
    size, expected = source_identity(connection, source_id)
    digest = sha256()
    if size:
        for chunk in read_physical_span(connection, PhysicalSpan(source_id, 0, size)):
            digest.update(chunk)
    if digest.hexdigest() != expected:
        raise ValueError("physical source digest differs from custody")
    return size, expected


def read_physical_span(connection: sqlite3.Connection, span: PhysicalSpan, page_size: int = 128) -> Iterator[bytes]:
    """Yield an exact source-relative span from one caller-owned SQLite snapshot."""
    if type(page_size) is not int or page_size <= 0:
        raise ValueError("physical page size is invalid")
    source_bytes, _ = source_identity(connection, span.source_id)
    end = span.offset + span.length
    if end > source_bytes:
        raise ValueError("physical span exceeds source custody")
    ordinal, skip = divmod(span.offset, RECORD_SIZE)
    remaining = span.length
    while remaining:
        with closing(
            connection.execute(
                "SELECT ordinal, device_timestamp, payload FROM capture_record "
                "WHERE source_id=? AND ordinal>=? ORDER BY ordinal LIMIT ?",
                (span.source_id, ordinal, page_size),
            )
        ) as cursor:
            rows = cast(list[tuple[object, object, object]], cursor.fetchall())
        if not rows:
            raise ValueError("physical source ended early")
        for row in rows:
            record = _authenticated_record_bytes(row[0], row[1], row[2], ordinal)
            chunk = record[skip : skip + remaining]
            yield chunk
            remaining -= len(chunk)
            ordinal += 1
            skip = 0
            if not remaining:
                return
