"""Small SQLite store for Collector-owned mutable domain snapshots."""

from __future__ import annotations

import json
import os
import sqlite3
import stat
import threading
from collections.abc import Iterator
from contextlib import contextmanager, suppress
from enum import StrEnum
from pathlib import Path
from typing import cast
from urllib.parse import quote

_SCHEMA_VERSION = 2
_LEGACY_SCHEMA_VERSION = 1
_BUSY_TIMEOUT_MS = 5_000
_STATE_DB_MODE = 0o640


class StateKind(StrEnum):
    FIRMWARE = "firmware"
    OPERATIONAL_STATUS = "operational_status"
    CONFIRMED_LOSS = "confirmed_loss"
    ATTEMPT = "attempt"
    QUARANTINE = "quarantine"
    READY_PUBLICATION = "ready_publication"
    READY_CLOSURE = "ready_closure"
    CLOCK = "clock"
    PHY_RESTORATION = "phy_restoration"
    MIGRATION = "migration"
    PUBLICATION_GENERATION = "publication_generation"
    QUALITY_EVENT = "quality_event"
    RUNTIME_STATUS = "runtime_status"
    QUALITY_EVENT_BUDGET = "quality_event_budget"


class StateDBError(RuntimeError):
    """The Collector state database is unsupported, invalid, or unavailable."""

    def __init__(self, message: str, *, sqlite_errorcode: int | None = None) -> None:
        super().__init__(message)
        self.sqlite_errorcode = sqlite_errorcode


def _database_failure(message: str, error: BaseException) -> StateDBError:
    return StateDBError(message, sqlite_errorcode=getattr(error, "sqlite_errorcode", None))


class _Operations:
    """Operations bound to one connection, shared by DB and transaction handles."""

    def __init__(self, connection: sqlite3.Connection) -> None:
        self._connection = connection

    def get(self, kind: StateKind, item_id: str) -> dict[str, object] | None:
        _validate_key(kind, item_id)
        try:
            row = _fetchone(
                self._connection.execute("SELECT payload FROM state WHERE kind = ? AND id = ?", (kind.value, item_id))
            )
        except sqlite3.DatabaseError as error:
            raise _database_failure("state database read failed", error) from error
        return _decode(_row_text(row, 0)) if row is not None else None

    def list(self, kind: StateKind, *, limit: int | None = None) -> tuple[tuple[str, dict[str, object]], ...]:
        _validate_kind(kind)
        if limit is not None and (type(limit) is not int or limit < 0):
            raise StateDBError("state list limit is invalid")
        try:
            query = "SELECT id, payload FROM state WHERE kind = ? ORDER BY id"
            parameters: tuple[str | int, ...] = (kind.value,)
            if limit is not None:
                query += " LIMIT ?"
                parameters += (limit,)
            rows = _fetchall(self._connection.execute(query, parameters))
        except sqlite3.DatabaseError as error:
            raise _database_failure("state database list failed", error) from error
        return tuple((_row_text(row, 0), _decode(_row_text(row, 1))) for row in rows)

    def put(self, kind: StateKind, item_id: str, payload: dict[str, object]) -> None:
        _validate_key(kind, item_id)
        encoded = _encode(payload)
        try:
            row = _fetchone(
                self._connection.execute("SELECT payload FROM state WHERE kind = ? AND id = ?", (kind.value, item_id))
            )
            if row is not None:
                stored = _row_text(row, 0)
                _decode(stored)
                if stored == encoded:
                    return
            self._connection.execute(
                "INSERT INTO state(kind, id, payload) VALUES (?, ?, ?) "
                "ON CONFLICT(kind, id) DO UPDATE SET payload = excluded.payload",
                (kind.value, item_id, encoded),
            )
        except sqlite3.DatabaseError as error:
            raise _database_failure("state database write failed", error) from error

    def delete(self, kind: StateKind, item_id: str) -> bool:
        _validate_key(kind, item_id)
        try:
            cursor = self._connection.execute("DELETE FROM state WHERE kind = ? AND id = ?", (kind.value, item_id))
        except sqlite3.DatabaseError as error:
            raise _database_failure("state database delete failed", error) from error
        return cursor.rowcount == 1


class StateTransaction(_Operations):
    """Operations on the one connection held by a ``StateDB.transaction``."""

    def __init__(self, connection: sqlite3.Connection) -> None:
        super().__init__(connection)
        self._publication_changed = False

    def put(self, kind: StateKind, item_id: str, payload: dict[str, object]) -> None:
        before = self._connection.total_changes
        super().put(kind, item_id, payload)
        self._publication_changed |= self._connection.total_changes != before and _is_publication_input(kind)

    def delete(self, kind: StateKind, item_id: str) -> bool:
        deleted = super().delete(kind, item_id)
        self._publication_changed |= deleted and _is_publication_input(kind)
        return deleted

    def get_generation(self) -> int:
        row = super().get(StateKind.PUBLICATION_GENERATION, "current")
        return _generation_value(row)

    def mark_publication_changed(self) -> None:
        """Wake publication for a typed audio mutation in this transaction."""

        self._publication_changed = True

    def payload_bytes(self, kind: StateKind, *, row_overhead: int = 0) -> int:
        _validate_kind(kind)
        if type(row_overhead) is not int or row_overhead < 0:
            raise StateDBError("state payload row overhead is invalid")
        try:
            row = _fetchone(
                self._connection.execute(
                    "SELECT COALESCE(SUM(length(CAST(payload AS BLOB)) + ?), 0) FROM state WHERE kind = ?",
                    (row_overhead, kind.value),
                )
            )
        except sqlite3.DatabaseError as error:
            raise _database_failure("state payload size query failed", error) from error
        if row is None:
            raise StateDBError("state payload size is invalid")
        size = _row_int(row, 0)
        if size < 0:
            raise StateDBError("state payload size is invalid")
        return size

    def _bump_generation(self) -> None:
        generation = self.get_generation()
        if generation == 2**63 - 1:
            raise StateDBError("publication generation is exhausted")
        _Operations.put(self, StateKind.PUBLICATION_GENERATION, "current", {"value": generation + 1})

    def finish(self) -> None:
        """Apply the publication-generation change on this same open transaction."""

        if self._publication_changed:
            self._bump_generation()


class StateDB(_Operations):
    """Thread-safe facade; each call owns and closes its SQLite connection."""

    def __init__(self, path: Path, *, read_only: bool = False) -> None:
        self.path = Path(path).absolute()
        self.read_only = read_only
        self._active = threading.local()
        self._allow_create = not read_only and not self.path.exists()
        if read_only:
            _require_directory(self.path.parent)
            self._validate_existing()
        else:
            _ensure_directory(self.path.parent)
            self._initialize()

    def _connect(self) -> sqlite3.Connection:
        connection: sqlite3.Connection | None = None
        try:
            if self.path.is_symlink():
                raise StateDBError("state database must not be a symlink")
            if self.read_only and not self.path.is_file():
                raise StateDBError("state database does not exist")
            mode = "ro" if self.read_only else "rwc" if self._allow_create and not self.path.exists() else "rw"
            uri = f"file:{quote(self.path.as_posix(), safe='/')}?mode={mode}"
            connection = sqlite3.connect(uri, uri=True, timeout=_BUSY_TIMEOUT_MS / 1000, isolation_level=None)
            connection.row_factory = sqlite3.Row
            connection.execute(f"PRAGMA busy_timeout = {_BUSY_TIMEOUT_MS}")
            connection.execute("PRAGMA synchronous = EXTRA")
            connection.execute("PRAGMA foreign_keys = ON")
            journal_row = _fetchone(connection.execute("PRAGMA journal_mode"))
            if journal_row is None:
                raise StateDBError("state database journal mode is unavailable")
            journal_mode = _row_text(journal_row, 0)
            if journal_mode.lower() != "delete" and not self.read_only:
                connection.execute("PRAGMA journal_mode = DELETE")
            elif journal_mode.lower() != "delete":
                raise StateDBError("state database journal mode is unsupported")
            return connection
        except (OSError, sqlite3.DatabaseError) as error:
            if connection is not None:
                connection.close()
            raise _database_failure("state database is unavailable", error) from error
        except BaseException:
            if connection is not None:
                connection.close()
            raise

    def _initialize(self) -> None:
        connection = self._connect()
        try:
            mode = _fetchone(connection.execute("PRAGMA journal_mode = DELETE"))
            if mode is None or _row_text(mode, 0).lower() != "delete":
                raise StateDBError("state database journal mode is unsupported")
            connection.execute("BEGIN IMMEDIATE")
            version = _user_version(connection)
            tables = {
                name
                for row in _fetchall(connection.execute("SELECT name FROM sqlite_master WHERE type = 'table'"))
                if not (name := _row_text(row, 0)).startswith("sqlite_")
            }
            if version == 0 and not tables:
                connection.execute(
                    "CREATE TABLE state ("
                    "kind TEXT NOT NULL, id TEXT NOT NULL, payload TEXT NOT NULL, "
                    "PRIMARY KEY(kind, id)) WITHOUT ROWID"
                )
                connection.execute(f"PRAGMA user_version = {_LEGACY_SCHEMA_VERSION}")
                install_audio_schema(connection)
            elif version != _SCHEMA_VERSION or "state" not in tables:
                raise StateDBError("state database schema is unsupported")
            _validate_schema(connection)
            _validate_audio_schema(connection)
            connection.commit()
            self._allow_create = False
            mode = stat.S_IMODE(self.path.stat().st_mode)
            if mode != _STATE_DB_MODE:
                self.path.chmod(_STATE_DB_MODE)
        except (OSError, sqlite3.DatabaseError) as error:
            connection.rollback()
            raise _database_failure("state database initialization failed", error) from error
        except BaseException:
            connection.rollback()
            raise
        finally:
            connection.close()

    def _validate_existing(self) -> None:
        connection = self._connect()
        try:
            version = _user_version(connection)
            tables = {
                name
                for row in _fetchall(connection.execute("SELECT name FROM sqlite_master WHERE type = 'table'"))
                if not (name := _row_text(row, 0)).startswith("sqlite_")
            }
            if version != _SCHEMA_VERSION or "state" not in tables:
                raise StateDBError("state database schema is unsupported")
            _validate_schema(connection)
            _validate_audio_schema(connection)
        except sqlite3.DatabaseError as error:
            raise _database_failure("state database validation failed", error) from error
        finally:
            connection.close()

    def _ensure_outside_transaction(self) -> None:
        if getattr(self._active, "in_transaction", False):
            raise StateDBError("use the active state transaction handle")

    @contextmanager
    def _connection_for_call(self, *, write: bool = False) -> Iterator[sqlite3.Connection]:
        self._ensure_outside_transaction()
        if write and self.read_only:
            raise StateDBError("state database is read-only")
        connection = self._connect()
        try:
            if write:
                connection.execute("BEGIN IMMEDIATE")
            yield connection
            if write:
                connection.commit()
        except sqlite3.DatabaseError as error:
            if write:
                _rollback(connection)
            raise _database_failure("state database transaction failed", error) from error
        except BaseException:
            if write:
                _rollback(connection)
            raise
        finally:
            connection.close()

    def get(self, kind: StateKind, item_id: str) -> dict[str, object] | None:
        with self._connection_for_call() as connection:
            return _Operations(connection).get(kind, item_id)

    def list(self, kind: StateKind, *, limit: int | None = None) -> tuple[tuple[str, dict[str, object]], ...]:
        with self._connection_for_call() as connection:
            return _Operations(connection).list(kind, limit=limit)

    def put(self, kind: StateKind, item_id: str, payload: dict[str, object]) -> None:
        with self.transaction() as transaction:
            transaction.put(kind, item_id, payload)

    def delete(self, kind: StateKind, item_id: str) -> bool:
        with self.transaction() as transaction:
            return transaction.delete(kind, item_id)

    def get_generation(self) -> int:
        row = self.get(StateKind.PUBLICATION_GENERATION, "current")
        return _generation_value(row)

    @contextmanager
    def transaction(self) -> Iterator[StateTransaction]:
        self._ensure_outside_transaction()
        if self.read_only:
            raise StateDBError("state database is read-only")
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            self._active.in_transaction = True
            transaction = StateTransaction(connection)
            yield transaction
            transaction.finish()
            connection.commit()
        except sqlite3.DatabaseError as error:
            _rollback(connection)
            raise _database_failure("state database transaction failed", error) from error
        except BaseException:
            _rollback(connection)
            raise
        finally:
            self._active.in_transaction = False
            connection.close()

    def integrity_check(self) -> None:
        with self._connection_for_call() as connection:
            try:
                result = _fetchone(connection.execute("PRAGMA integrity_check"))
            except sqlite3.DatabaseError as error:
                raise _database_failure("state database integrity check failed", error) from error
        if result is None or _row_text(result, 0) != "ok":
            raise StateDBError("state database integrity check failed")

    def open_connection(self, *, read_only: bool = False) -> sqlite3.Connection:
        """Open a configured connection for a persistent typed-store caller."""

        if self.path.is_symlink():
            raise StateDBError("state database must not be a symlink")
        if read_only:
            if not self.path.is_file():
                raise StateDBError("state database does not exist")
            mode = "ro"
        else:
            if self.read_only:
                raise StateDBError("state database is read-only")
            mode = "rw"
        connection: sqlite3.Connection | None = None
        try:
            uri = f"file:{quote(self.path.as_posix(), safe='/')}?mode={mode}"
            connection = sqlite3.connect(uri, uri=True, timeout=_BUSY_TIMEOUT_MS / 1000, isolation_level=None)
            connection.row_factory = sqlite3.Row
            connection.execute(f"PRAGMA busy_timeout = {_BUSY_TIMEOUT_MS}")
            connection.execute("PRAGMA synchronous = EXTRA")
            connection.execute("PRAGMA foreign_keys = ON")
            journal_row = _fetchone(connection.execute("PRAGMA journal_mode"))
            if journal_row is None or _row_text(journal_row, 0).lower() != "delete":
                raise StateDBError("state database journal mode is unsupported")
            return connection
        except (OSError, sqlite3.DatabaseError) as error:
            if connection is not None:
                connection.close()
            raise _database_failure("state database is unavailable", error) from error
        except BaseException:
            if connection is not None:
                connection.close()
            raise


def _validate_kind(kind: StateKind) -> None:
    if not isinstance(kind, StateKind):
        raise StateDBError("state kind is invalid")


def _validate_key(kind: StateKind, item_id: str) -> None:
    _validate_kind(kind)
    if not isinstance(item_id, str) or not item_id:
        raise StateDBError("state identity is invalid")


def _is_publication_input(kind: StateKind) -> bool:
    return kind in {
        StateKind.ATTEMPT,
        StateKind.QUARANTINE,
        StateKind.READY_PUBLICATION,
        StateKind.READY_CLOSURE,
        StateKind.CLOCK,
    }


def _generation_value(row: dict[str, object] | None) -> int:
    if row is None:
        return 0
    value = row.get("value")
    if type(value) is not int or not 0 <= value <= 2**63 - 1:
        raise StateDBError("publication generation is invalid")
    return value


def _encode(payload: dict[str, object]) -> str:
    if not isinstance(payload, dict) or any(not isinstance(key, str) for key in payload):
        raise StateDBError("state payload must be an object with string keys")
    try:
        encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)
        encoded.encode("utf-8")
        return encoded
    except (TypeError, ValueError) as error:
        raise StateDBError("state payload is not canonical JSON") from error


def _decode(payload: str) -> dict[str, object]:
    try:
        value = cast(object, json.loads(payload, object_pairs_hook=_unique_keys))
    except (json.JSONDecodeError, TypeError, ValueError) as error:
        raise StateDBError("state payload is malformed") from error
    if not isinstance(value, dict) or any(not isinstance(key, str) for key in value):
        raise StateDBError("state payload is not an object")
    return cast(dict[str, object], value)


def _unique_keys(pairs: list[tuple[str, object]]) -> dict[str, object]:
    document: dict[str, object] = {}
    for key, value in pairs:
        if key in document:
            raise ValueError("duplicate state payload key")
        document[key] = value
    return document


def _fetchone(cursor: sqlite3.Cursor) -> sqlite3.Row | None:
    result = cast(object, cursor.fetchone())
    if result is not None and not isinstance(result, sqlite3.Row):
        raise StateDBError("state database returned an invalid row")
    return result


def _fetchall(cursor: sqlite3.Cursor) -> list[sqlite3.Row]:
    result = cast(object, cursor.fetchall())
    if not isinstance(result, list):
        raise StateDBError("state database returned invalid rows")
    rows: list[sqlite3.Row] = []
    for row in result:
        if not isinstance(row, sqlite3.Row):
            raise StateDBError("state database returned an invalid row")
        rows.append(row)
    return rows


def _row_text(row: sqlite3.Row, column: int | str) -> str:
    value = cast(object, row[column])
    if not isinstance(value, str):
        raise StateDBError("state database row text is invalid")
    return value


def _row_int(row: sqlite3.Row, column: int | str) -> int:
    value = cast(object, row[column])
    if type(value) is not int:
        raise StateDBError("state database row integer is invalid")
    return value


def _user_version(connection: sqlite3.Connection) -> int:
    row = _fetchone(connection.execute("PRAGMA user_version"))
    if row is None:
        raise StateDBError("state database schema version is unavailable")
    return _row_int(row, 0)


def _validate_schema(connection: sqlite3.Connection) -> None:
    rows = _fetchall(connection.execute("PRAGMA table_info(state)"))
    columns = tuple(
        (_row_text(row, "name"), _row_text(row, "type").upper(), _row_int(row, "notnull"), _row_int(row, "pk"))
        for row in rows
    )
    expected = (("kind", "TEXT", 1, 1), ("id", "TEXT", 1, 2), ("payload", "TEXT", 1, 0))
    if columns != expected:
        raise StateDBError("state database table is unsupported")


_AUDIO_TABLES = (
    "capture_source",
    "capture_record",
    "capture_tail",
    "capture_conflict",
    "capture_evidence",
    "capture_evidence_chunk",
    "publication_cohort",
    "publication_cohort_member",
    "ready_output_stage",
    "ready_stage_range",
)


def _ddl(*parts: str) -> str:
    return "".join(parts)


_AUDIO_DDL = (
    _ddl(
        "CREATE TABLE capture_source ("
        "source_id TEXT PRIMARY KEY NOT NULL CHECK (length(source_id) = 32 AND lower(source_id) = source_id AND source_id NOT GLOB '*[^0-9a-f]*'),"
        "source_type TEXT NOT NULL CHECK (source_type IN ('attempt','draft','quarantine')),"
        "lifecycle TEXT NOT NULL CHECK (lifecycle IN ('prepared','leg_prepared','receiving','interrupted','sealed_waiting','blocked','covered','retiring')),"
        "sequence_start BLOB NOT NULL CHECK (typeof(sequence_start) = 'blob' AND length(sequence_start) = 8),"
        "sequence_limit BLOB NOT NULL CHECK (typeof(sequence_limit) = 'blob' AND length(sequence_limit) = 8),"
        "prepared_start BLOB CHECK (prepared_start IS NULL OR (typeof(prepared_start) = 'blob' AND length(prepared_start) = 8)),"
        "prepared_count INTEGER CHECK (prepared_count IS NULL OR prepared_count BETWEEN 0 AND 4294967295),"
        "read_begin_start BLOB CHECK (read_begin_start IS NULL OR (typeof(read_begin_start) = 'blob' AND length(read_begin_start) = 8)),"
        "read_begin_count INTEGER CHECK (read_begin_count IS NULL OR read_begin_count BETWEEN 0 AND 4294967295),"
        "current_leg_generation INTEGER NOT NULL CHECK (current_leg_generation BETWEEN 0 AND 4294967295),"
        "current_leg_start BLOB NOT NULL CHECK (typeof(current_leg_start) = 'blob' AND length(current_leg_start) = 8),"
        "current_leg_count INTEGER NOT NULL CHECK (current_leg_count BETWEEN 0 AND 4294967295),"
        "committed_count INTEGER NOT NULL DEFAULT 0 CHECK (committed_count BETWEEN 0 AND 4294967295),"
        "committed_sha256 TEXT NOT NULL CHECK (length(committed_sha256) = 64 AND lower(committed_sha256) = committed_sha256 AND committed_sha256 NOT GLOB '*[^0-9a-f]*'),"
        "closure_frontier BLOB CHECK (closure_frontier IS NULL OR (typeof(closure_frontier) = 'blob' AND length(closure_frontier) = 8)),"
        "block_reason TEXT, quarantine_disposition TEXT CHECK (quarantine_disposition IS NULL OR quarantine_disposition IN ('unclassified','published_prefix','unprocessable')) ,"
        "quarantine_opaque INTEGER CHECK (quarantine_opaque IS NULL OR quarantine_opaque IN (0,1)),"
        "terminal_reason TEXT, terminal_time TEXT, retention_deadline TEXT,"
        "certified_prefix_count INTEGER CHECK (certified_prefix_count IS NULL OR certified_prefix_count BETWEEN 0 AND 4294967295),"
        "coverage_digest TEXT CHECK (coverage_digest IS NULL OR (length(coverage_digest) = 64 AND lower(coverage_digest) = coverage_digest AND coverage_digest NOT GLOB '*[^0-9a-f]*')),"
        "retirement_cursor INTEGER CHECK (retirement_cursor IS NULL OR retirement_cursor BETWEEN 0 AND 4294967295),"
        "CHECK (CAST(sequence_start AS BLOB) <= CAST(sequence_limit AS BLOB))"
        ") WITHOUT ROWID"
    ),
    _ddl(
        "CREATE TABLE capture_record ("
        "source_id TEXT NOT NULL, ordinal INTEGER NOT NULL CHECK (ordinal BETWEEN 0 AND 4294967295),"
        "device_timestamp INTEGER NOT NULL CHECK (device_timestamp BETWEEN 0 AND 4294967295),"
        "payload BLOB NOT NULL CHECK (typeof(payload) = 'blob' AND length(payload) = 440),"
        "utc_timestamp INTEGER CHECK (utc_timestamp IS NULL OR utc_timestamp BETWEEN 0 AND 4294967295),"
        "mapping_revision TEXT CHECK (mapping_revision IS NULL OR (length(mapping_revision) = 64 AND lower(mapping_revision) = mapping_revision AND mapping_revision NOT GLOB '*[^0-9a-f]*')),"
        "PRIMARY KEY (source_id, ordinal), FOREIGN KEY (source_id) REFERENCES capture_source(source_id) ON DELETE CASCADE"
        ") WITHOUT ROWID"
    ),
    _ddl(
        "CREATE TABLE capture_tail ("
        "source_id TEXT PRIMARY KEY NOT NULL, leg_generation INTEGER NOT NULL CHECK (leg_generation BETWEEN 0 AND 4294967295),"
        "complete_high_water INTEGER NOT NULL CHECK (complete_high_water BETWEEN 0 AND 4294967295),"
        "received_high_water INTEGER NOT NULL CHECK (received_high_water >= 0),"
        "byte_offset INTEGER NOT NULL CHECK (byte_offset >= 0), bytes BLOB NOT NULL CHECK (typeof(bytes) = 'blob' AND length(bytes) BETWEEN 1 AND 443),"
        "CHECK (received_high_water = byte_offset + length(bytes) AND byte_offset = complete_high_water * 444),"
        "FOREIGN KEY (source_id) REFERENCES capture_source(source_id)"
        ") WITHOUT ROWID"
    ),
    _ddl(
        "CREATE TABLE capture_conflict ("
        "source_id TEXT NOT NULL, chunk_ordinal INTEGER NOT NULL CHECK (chunk_ordinal BETWEEN 0 AND 4294967295),"
        "leg_generation INTEGER NOT NULL CHECK (leg_generation BETWEEN 0 AND 4294967295), absolute_byte_offset INTEGER NOT NULL CHECK (absolute_byte_offset >= 0),"
        "reason TEXT NOT NULL, bytes BLOB NOT NULL CHECK (typeof(bytes) = 'blob' AND length(bytes) BETWEEN 1 AND 56832),"
        "PRIMARY KEY (source_id, chunk_ordinal), FOREIGN KEY (source_id) REFERENCES capture_source(source_id)"
        ") WITHOUT ROWID"
    ),
    _ddl(
        "CREATE TABLE capture_evidence ("
        "evidence_id TEXT PRIMARY KEY NOT NULL, source_id TEXT, entry_kind TEXT NOT NULL CHECK (entry_kind IN ('regular','symlink','directory')) ,"
        "original_relative_path TEXT NOT NULL, device INTEGER NOT NULL CHECK (device >= 0), inode INTEGER NOT NULL CHECK (inode >= 0),"
        "mode INTEGER NOT NULL CHECK (mode >= 0), size INTEGER NOT NULL CHECK (size >= 0), sha256 TEXT, symlink_text BLOB,"
        "disposition TEXT CHECK (disposition IS NULL OR disposition IN ('unclassified','published_prefix','unprocessable')), terminal_time TEXT, retention_deadline TEXT, reason TEXT, terminal_reason TEXT,"
        "CHECK ((entry_kind = 'regular' AND sha256 IS NOT NULL AND length(sha256) = 64 AND lower(sha256) = sha256 AND sha256 NOT GLOB '*[^0-9a-f]*' AND symlink_text IS NULL) OR"
        " (entry_kind = 'symlink' AND symlink_text IS NOT NULL AND typeof(symlink_text) = 'blob' AND sha256 IS NULL) OR"
        " (entry_kind = 'directory' AND size = 0 AND sha256 IS NULL AND symlink_text IS NULL)),"
        "FOREIGN KEY (source_id) REFERENCES capture_source(source_id) ON DELETE SET NULL"
        ") WITHOUT ROWID"
    ),
    _ddl(
        "CREATE TABLE capture_evidence_chunk ("
        "evidence_id TEXT NOT NULL, chunk_ordinal INTEGER NOT NULL CHECK (chunk_ordinal BETWEEN 0 AND 4294967295),"
        "bytes BLOB NOT NULL CHECK (typeof(bytes) = 'blob' AND length(bytes) BETWEEN 1 AND 56832), PRIMARY KEY (evidence_id, chunk_ordinal),"
        "FOREIGN KEY (evidence_id) REFERENCES capture_evidence(evidence_id) ON DELETE CASCADE"
        ") WITHOUT ROWID"
    ),
    _ddl(
        "CREATE TABLE publication_cohort ("
        "cohort_id TEXT PRIMARY KEY NOT NULL, lifecycle TEXT NOT NULL CHECK (lifecycle IN ('unqualified','accepted','staging','complete')),"
        "accepted_permit TEXT, drained_threshold INTEGER CHECK (drained_threshold IS NULL OR drained_threshold >= 0), config_digest TEXT CHECK (config_digest IS NULL OR (length(config_digest) = 64 AND lower(config_digest) = config_digest AND config_digest NOT GLOB '*[^0-9a-f]*')),"
        "closure_frontier BLOB CHECK (closure_frontier IS NULL OR (typeof(closure_frontier) = 'blob' AND length(closure_frontier) = 8)), cursor INTEGER NOT NULL DEFAULT 0 CHECK (cursor >= 0),"
        "accepted_time TEXT"
        ") WITHOUT ROWID"
    ),
    _ddl(
        "CREATE TABLE publication_cohort_member ("
        "cohort_id TEXT NOT NULL, member_ordinal INTEGER NOT NULL CHECK (member_ordinal BETWEEN 0 AND 4294967295), source_id TEXT NOT NULL,"
        "slice_start INTEGER NOT NULL CHECK (slice_start BETWEEN 0 AND 4294967295), slice_count INTEGER NOT NULL CHECK (slice_count BETWEEN 0 AND 4294967295),"
        "admitted_lifecycle TEXT NOT NULL CHECK (admitted_lifecycle IN ('sealed_waiting','blocked')),"
        "admitted_count INTEGER NOT NULL CHECK (admitted_count BETWEEN 0 AND 4294967295),"
        "admitted_sha256 TEXT NOT NULL CHECK (length(admitted_sha256) = 64 AND lower(admitted_sha256) = admitted_sha256 AND admitted_sha256 NOT GLOB '*[^0-9a-f]*'),"
        "cursor INTEGER NOT NULL DEFAULT 0 CHECK (cursor BETWEEN 0 AND 4294967295), PRIMARY KEY (cohort_id, member_ordinal),"
        "FOREIGN KEY (cohort_id) REFERENCES publication_cohort(cohort_id) ON DELETE CASCADE, FOREIGN KEY (source_id) REFERENCES capture_source(source_id)"
        ") WITHOUT ROWID"
    ),
    _ddl(
        "CREATE TABLE ready_output_stage ("
        "stage_id TEXT PRIMARY KEY NOT NULL, group_identity TEXT NOT NULL UNIQUE, owned_path TEXT NOT NULL, expected_bundle_digest TEXT NOT NULL CHECK (length(expected_bundle_digest) = 64 AND lower(expected_bundle_digest) = expected_bundle_digest AND expected_bundle_digest NOT GLOB '*[^0-9a-f]*'),"
        "expected_record_count INTEGER NOT NULL CHECK (expected_record_count BETWEEN 0 AND 4294967295), expected_slice_count INTEGER NOT NULL CHECK (expected_slice_count >= 0),"
        "progress_records INTEGER NOT NULL DEFAULT 0 CHECK (progress_records BETWEEN 0 AND 4294967295), progress_slice INTEGER NOT NULL DEFAULT 0 CHECK (progress_slice >= 0),"
        "frozen_device_start INTEGER CHECK (frozen_device_start IS NULL OR frozen_device_start BETWEEN 0 AND 4294967295), frozen_device_end INTEGER CHECK (frozen_device_end IS NULL OR frozen_device_end BETWEEN 0 AND 4294967295),"
        "frozen_utc_start INTEGER CHECK (frozen_utc_start IS NULL OR frozen_utc_start BETWEEN 0 AND 4294967295), frozen_utc_end INTEGER CHECK (frozen_utc_end IS NULL OR frozen_utc_end BETWEEN 0 AND 4294967295),"
        "frozen_mapping_revision TEXT CHECK (frozen_mapping_revision IS NULL OR (length(frozen_mapping_revision) = 64 AND lower(frozen_mapping_revision) = frozen_mapping_revision AND frozen_mapping_revision NOT GLOB '*[^0-9a-f]*')), owner_token TEXT NOT NULL,"
        "failure_reason TEXT CHECK (failure_reason IS NULL OR failure_reason = 'durability_io_error')"
        ") WITHOUT ROWID"
    ),
    _ddl(
        "CREATE TABLE ready_stage_range ("
        "stage_id TEXT NOT NULL, range_ordinal INTEGER NOT NULL CHECK (range_ordinal BETWEEN 0 AND 4294967295), time_range_ordinal INTEGER NOT NULL CHECK (time_range_ordinal BETWEEN 0 AND 4294967295), source_id TEXT NOT NULL,"
        "start_ordinal INTEGER NOT NULL CHECK (start_ordinal BETWEEN 0 AND 4294967295), record_count INTEGER NOT NULL CHECK (record_count BETWEEN 0 AND 4294967295),"
        "sequence_start BLOB NOT NULL CHECK (length(sequence_start) = 8), next_sequence BLOB NOT NULL CHECK (length(next_sequence) = 8 AND next_sequence > sequence_start),"
        "device_start INTEGER NOT NULL CHECK (device_start BETWEEN 0 AND 4294967295), device_end INTEGER NOT NULL CHECK (device_end BETWEEN 0 AND 4294967295),"
        "utc_start INTEGER CHECK (utc_start IS NULL OR utc_start BETWEEN 0 AND 4294967295), utc_end INTEGER CHECK (utc_end IS NULL OR utc_end BETWEEN 0 AND 4294967295),"
        "utc_observation_id TEXT, utc_offset_seconds REAL, utc_uncertainty_seconds REAL,"
        "utc_confidence TEXT CHECK (utc_confidence IN ('confirmed','approximate')) ,"
        "CHECK ((utc_start IS NULL AND utc_end IS NULL AND utc_observation_id IS NULL AND utc_offset_seconds IS NULL AND utc_uncertainty_seconds IS NULL AND utc_confidence IS NULL) OR "
        "(utc_start IS NOT NULL AND utc_end IS NOT NULL AND utc_observation_id IS NOT NULL AND utc_offset_seconds IS NOT NULL AND utc_uncertainty_seconds IS NOT NULL AND utc_confidence IS NOT NULL)),"
        "PRIMARY KEY (stage_id, range_ordinal), FOREIGN KEY (stage_id) REFERENCES ready_output_stage(stage_id) ON DELETE CASCADE, FOREIGN KEY (source_id) REFERENCES capture_source(source_id)"
        ") WITHOUT ROWID"
    ),
)

_AUDIO_INDEXES = {
    "capture_record_mapping_revision_idx": (
        "CREATE INDEX capture_record_mapping_revision_idx ON capture_record (mapping_revision, source_id, ordinal)"
    ),
}


def install_audio_schema(connection: sqlite3.Connection) -> None:
    """Install typed audio tables on an offline v1 backup, or validate v2."""

    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys = ON")
    version = _user_version(connection)
    if version not in {_LEGACY_SCHEMA_VERSION, _SCHEMA_VERSION}:
        raise StateDBError("state database schema is unsupported")
    tables = {
        _row_text(row, 0)
        for row in _fetchall(connection.execute("SELECT name FROM sqlite_master WHERE type = 'table'"))
        if not _row_text(row, 0).startswith("sqlite_")
    }
    if "state" not in tables:
        raise StateDBError("state database table is unsupported")
    for table, ddl in zip(_AUDIO_TABLES, _AUDIO_DDL, strict=True):
        if table not in tables:
            connection.execute(ddl)
    indexes = {
        _row_text(row, 0)
        for row in _fetchall(connection.execute("SELECT name FROM sqlite_master WHERE type = 'index'"))
    }
    for name, ddl in _AUDIO_INDEXES.items():
        if name not in indexes:
            connection.execute(ddl)
    connection.execute(f"PRAGMA user_version = {_SCHEMA_VERSION}")
    _validate_audio_schema(connection)


def _validate_audio_schema(connection: sqlite3.Connection) -> None:
    rows = _fetchall(connection.execute("SELECT name, sql FROM sqlite_master WHERE type = 'table'"))
    user_tables = {_row_text(row, "name") for row in rows if not _row_text(row, "name").startswith("sqlite_")}
    expected_tables = {"state", *_AUDIO_TABLES}
    if user_tables != expected_tables:
        raise StateDBError("state database typed schema has unexpected tables")
    statements = {
        _row_text(row, "name"): _row_text(row, "sql") for row in rows if _row_text(row, "name") in _AUDIO_TABLES
    }
    if set(statements) != set(_AUDIO_TABLES):
        raise StateDBError("state database typed schema is incomplete")
    for table, ddl in zip(_AUDIO_TABLES, _AUDIO_DDL, strict=True):
        if _normalize_sql(statements[table]) != _normalize_sql(ddl):
            raise StateDBError("state database typed table is unsupported")
    for name, ddl in _AUDIO_INDEXES.items():
        row = _fetchone(connection.execute("SELECT sql FROM sqlite_master WHERE type = 'index' AND name = ?", (name,)))
        if row is None or _normalize_sql(_row_text(row, 0)) != _normalize_sql(ddl):
            raise StateDBError("state database typed projection index is unsupported")


def _normalize_sql(statement: str) -> str:
    return " ".join(statement.split()).lower()


def _ensure_directory(path: Path) -> None:
    missing: list[Path] = []
    current = Path(path.anchor)
    for component in path.parts[1:]:
        current /= component
        try:
            mode = current.lstat().st_mode
        except FileNotFoundError:
            missing.append(current)
            continue
        if stat.S_ISLNK(mode) or not stat.S_ISDIR(mode):
            raise StateDBError("state database directory must be a regular directory")
    for directory in missing:
        with suppress(FileExistsError):
            directory.mkdir(mode=0o750)
        _require_directory(directory)
        _sync_directory(directory.parent)


def _require_directory(path: Path) -> None:
    try:
        mode = path.lstat().st_mode
    except OSError as error:
        raise StateDBError("state database directory is unavailable") from error
    if stat.S_ISLNK(mode) or not stat.S_ISDIR(mode):
        raise StateDBError("state database directory must be a regular directory")


def _sync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _rollback(connection: sqlite3.Connection) -> None:
    with suppress(sqlite3.DatabaseError):
        connection.rollback()
