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

_SCHEMA_VERSION = 1
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
            raise StateDBError("state database read failed") from error
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
            raise StateDBError("state database list failed") from error
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
            raise StateDBError("state database write failed") from error

    def delete(self, kind: StateKind, item_id: str) -> bool:
        _validate_key(kind, item_id)
        try:
            cursor = self._connection.execute("DELETE FROM state WHERE kind = ? AND id = ?", (kind.value, item_id))
        except sqlite3.DatabaseError as error:
            raise StateDBError("state database delete failed") from error
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
            raise StateDBError("state payload size query failed") from error
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


class StateDB(_Operations):
    """Thread-safe facade; each call owns and closes its SQLite connection."""

    def __init__(self, path: Path, *, read_only: bool = False) -> None:
        self.path = Path(path).absolute()
        self.read_only = read_only
        self._active = threading.local()
        if read_only:
            _require_directory(self.path.parent)
            self._validate_existing()
        else:
            _ensure_directory(self.path.parent)
            self._initialize()

    def _connect(self) -> sqlite3.Connection:
        try:
            if self.path.is_symlink():
                raise StateDBError("state database must not be a symlink")
            if self.read_only and not self.path.is_file():
                raise StateDBError("state database does not exist")
            mode = "ro" if self.read_only else "rw" if self.path.exists() else "rwc"
            uri = f"file:{quote(self.path.as_posix(), safe='/')}?mode={mode}"
            connection = sqlite3.connect(uri, uri=True, timeout=_BUSY_TIMEOUT_MS / 1000, isolation_level=None)
            connection.row_factory = sqlite3.Row
            connection.execute(f"PRAGMA busy_timeout = {_BUSY_TIMEOUT_MS}")
            connection.execute("PRAGMA synchronous = EXTRA")
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
            raise StateDBError("state database is unavailable") from error

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
                connection.execute(f"PRAGMA user_version = {_SCHEMA_VERSION}")
            elif version != _SCHEMA_VERSION or tables != {"state"}:
                raise StateDBError("state database schema is unsupported")
            _validate_schema(connection)
            connection.commit()
            mode = stat.S_IMODE(self.path.stat().st_mode)
            if mode != _STATE_DB_MODE:
                self.path.chmod(_STATE_DB_MODE)
        except (OSError, sqlite3.DatabaseError) as error:
            connection.rollback()
            raise StateDBError("state database initialization failed") from error
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
            if version != _SCHEMA_VERSION or tables != {"state"}:
                raise StateDBError("state database schema is unsupported")
            _validate_schema(connection)
        except sqlite3.DatabaseError as error:
            raise StateDBError("state database validation failed") from error
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
            raise StateDBError("state database transaction failed") from error
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
            if transaction._publication_changed:
                transaction._bump_generation()
            connection.commit()
        except sqlite3.DatabaseError as error:
            _rollback(connection)
            raise StateDBError("state database transaction failed") from error
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
                raise StateDBError("state database integrity check failed") from error
        if result is None or _row_text(result, 0) != "ok":
            raise StateDBError("state database integrity check failed")


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
