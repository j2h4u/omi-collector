"""Durable SQLite compaction debt and worker-owned VACUUM execution."""

from __future__ import annotations

import json
import sqlite3
import threading
from contextlib import closing
from typing import Literal, cast

from ..application.storage_fault import FatalInternalAudioStorageError, TransientInternalAudioStorageError
from .collector_state_db import StateDB, StateDBError, StateKind, StateTransaction

_KEY = "current"
_MAX_GENERATION = 2**63 - 1


def _one(connection: sqlite3.Connection, query: str, parameters: tuple[object, ...] = ()) -> tuple[object, ...] | None:
    return cast(tuple[object, ...] | None, connection.execute(query, parameters).fetchone())


def _marker_row(row: tuple[object, ...] | None) -> tuple[int, bool] | None:
    if row is None:
        return None
    if not isinstance(row[0], str):
        raise StateDBError("compaction marker is invalid")
    return _marker(cast(object, json.loads(row[0])))


def _marker(value: object) -> tuple[int, bool] | None:
    if value is None:
        return None
    if not isinstance(value, dict):
        raise StateDBError("compaction marker is invalid")
    generation, pending = value.get("generation"), value.get("pending")
    if type(generation) is not int or generation < 0 or type(pending) is not bool:
        raise StateDBError("compaction marker is invalid")
    return generation, pending


def mark_retirement_compaction_pending(state: StateTransaction) -> None:
    """Advance compaction debt atomically with deletion of covered audio rows."""
    current = _marker(state.get(StateKind.COMPACTION, _KEY))
    generation = current[0] if current is not None else 0
    if generation == _MAX_GENERATION:
        raise StateDBError("compaction generation is exhausted")
    state.put(StateKind.COMPACTION, _KEY, {"generation": generation + 1, "pending": True})


def _bootstrap(state_db: StateDB) -> None:
    with closing(state_db.open_connection(read_only=True)) as connection, state_db.access(connection):
        existing = _one(connection, "SELECT 1 FROM state WHERE kind=? AND id=?", (StateKind.COMPACTION.value, _KEY))
        if existing is not None:
            return
        row = _one(connection, "PRAGMA freelist_count")
        if row is None or type(row[0]) is not int:
            raise StateDBError("SQLite freelist count is invalid")
        pending = row[0] > 0
    with state_db.transaction() as transaction:
        if transaction.get(StateKind.COMPACTION, _KEY) is None:
            transaction.put(StateKind.COMPACTION, _KEY, {"generation": 0, "pending": pending})


def pending_compaction_generation(state_db: StateDB) -> int | None:
    """Return pending debt only after actionable covered-row retirement drains."""
    _bootstrap(state_db)
    with closing(state_db.open_connection(read_only=True)) as connection, state_db.access(connection):
        marker_row = _one(
            connection, "SELECT payload FROM state WHERE kind=? AND id=?", (StateKind.COMPACTION.value, _KEY)
        )
        if marker_row is None:
            raise StateDBError("compaction marker disappeared")
        marker = _marker_row(marker_row)
        retiring = _one(
            connection,
            "SELECT 1 FROM capture_source WHERE lifecycle='retiring' "
            "AND retirement_cursor<committed_count AND NOT EXISTS "
            "(SELECT 1 FROM publication_cohort_member WHERE publication_cohort_member.source_id=capture_source.source_id) "
            "LIMIT 1",
        )
    assert marker is not None
    return marker[0] if marker[1] and retiring is None else None


def compaction_failure_outcome(error: BaseException, interrupt: threading.Event) -> Literal["interrupted", "deferred"]:
    """Classify retryable SQLite outcomes; unknown storage faults fail closed."""
    code = getattr(error, "sqlite_errorcode", None)
    primary = code & 0xFF if type(code) is int else None
    if primary in {sqlite3.SQLITE_CORRUPT, sqlite3.SQLITE_NOTADB}:
        raise FatalInternalAudioStorageError("canonical audio database is corrupt", sqlite_errorcode=code) from error
    if primary == sqlite3.SQLITE_IOERR:
        raise FatalInternalAudioStorageError("SQLite VACUUM outcome is uncertain", sqlite_errorcode=code) from error
    if primary in {sqlite3.SQLITE_BUSY, sqlite3.SQLITE_LOCKED, sqlite3.SQLITE_FULL, sqlite3.SQLITE_INTERRUPT}:
        return "interrupted" if interrupt.is_set() else "deferred"
    raise FatalInternalAudioStorageError("SQLite compaction outcome is uncertain", sqlite_errorcode=code) from error


def vacuum_pending(
    state_db: StateDB, generation: int, interrupt: threading.Event
) -> Literal["done", "interrupted", "deferred"]:
    """Run one cancellable autocommit VACUUM and clear only matching debt."""
    if type(generation) is not int or generation < 0:
        raise ValueError("compaction generation is invalid")
    if interrupt.is_set():
        return "interrupted"
    try:
        connection = state_db.open_connection()
        try:
            with state_db.access(connection):
                connection.set_progress_handler(lambda: int(interrupt.is_set()), 1000)
                try:
                    preflight = _vacuum_preflight(connection, generation, interrupt)
                    if preflight != "run":
                        return preflight
                    connection.execute("VACUUM")
                finally:
                    connection.set_progress_handler(None, 0)
        finally:
            connection.close()
    except (OSError, ValueError, sqlite3.DatabaseError, StateDBError, TransientInternalAudioStorageError) as error:
        return compaction_failure_outcome(error, interrupt)
    try:
        with state_db.transaction() as transaction:
            current = _marker(transaction.get(StateKind.COMPACTION, _KEY))
            if current == (generation, True):
                transaction.put(StateKind.COMPACTION, _KEY, {"generation": generation, "pending": False})
    except (OSError, ValueError, sqlite3.DatabaseError, StateDBError, TransientInternalAudioStorageError) as error:
        return compaction_failure_outcome(error, interrupt)
    return "done"


def _vacuum_preflight(
    connection: sqlite3.Connection, generation: int, interrupt: threading.Event
) -> Literal["run", "interrupted", "deferred"]:
    if interrupt.is_set():
        return "interrupted"
    marker_row = _one(connection, "SELECT payload FROM state WHERE kind=? AND id=?", (StateKind.COMPACTION.value, _KEY))
    marker = _marker_row(marker_row)
    if marker != (generation, True):
        return "deferred"
    retiring = _one(
        connection,
        "SELECT 1 FROM capture_source WHERE lifecycle='retiring' "
        "AND retirement_cursor<committed_count AND NOT EXISTS "
        "(SELECT 1 FROM publication_cohort_member WHERE publication_cohort_member.source_id=capture_source.source_id) "
        "LIMIT 1",
    )
    if retiring is not None:
        return "deferred"
    return "interrupted" if interrupt.is_set() else "run"
