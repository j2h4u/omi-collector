"""Runtime gate for current Collector SQLite state."""

from __future__ import annotations

import errno
import os
import re
import sqlite3
import stat
from contextlib import closing
from hashlib import sha256
from pathlib import Path
from typing import cast
from urllib.parse import quote

from omi_collector.capture.adapters.collector_state_db import (
    StateDB,
    StateDBError,
    _validate_audio_schema,
    _validate_schema,
    raise_if_sqlite_storage_fault,
)
from omi_collector.storage_layout import StorageLayout


class StateMigrationError(RuntimeError):
    """The installed Collector state is unavailable or unsafe to run."""


class InternalAudioCutoverRequiredError(StateMigrationError):
    """A known v1 or unsafe stopped-service state needs operator repair."""


def _validate_canonical(layout: StorageLayout, *, read_only: bool) -> None:
    path = layout.collector.state_db
    try:
        mode = path.lstat().st_mode
        if not stat.S_ISREG(mode):
            raise StateMigrationError("Collector SQLite path is not a regular file")
        _check_canonical_access(path, read_only=read_only)
        # A writable startup must let SQLite roll back an accessible hot
        # journal before the logical v2 checks inspect state rows.
        mode = "ro" if read_only else "rw"
        uri = f"file:{quote(path.absolute().as_posix(), safe='/')}?mode={mode}"
        with closing(sqlite3.connect(uri, uri=True, timeout=30 if read_only else 5)) as connection:
            _validate_current(connection)
            _assert_private_audio_gone(layout, connection)
    except FileNotFoundError as error:
        raise StateMigrationError("Collector SQLite database is missing; run stopped-service init-state") from error
    except PermissionError as error:
        raise InternalAudioCutoverRequiredError(
            "Collector SQLite or journal access needs stopped-service repair"
        ) from error
    except (OSError, sqlite3.DatabaseError, UnicodeError, ValueError, TypeError) as error:
        raise_if_sqlite_storage_fault(error)
        raise StateMigrationError("Collector SQLite state cannot be validated") from error


_TYPED_SCHEMA = 4
_PREVIOUS_TYPED_SCHEMA = 3
_LEGACY_TYPED_SCHEMA = 2


def _validate_current(connection: sqlite3.Connection) -> None:
    connection.row_factory = sqlite3.Row
    version_row = cast(tuple[int] | None, connection.execute("PRAGMA user_version").fetchone())
    if version_row is None:
        raise StateMigrationError("Collector SQLite schema version is unavailable")
    if version_row[0] == 1:
        raise InternalAudioCutoverRequiredError("Collector v1 state is unsupported by this release")
    if version_row[0] in {_LEGACY_TYPED_SCHEMA, _PREVIOUS_TYPED_SCHEMA}:
        raise InternalAudioCutoverRequiredError(
            f"Collector v{version_row[0]} state requires stopped-service init-state migration"
        )
    if version_row[0] != _TYPED_SCHEMA:
        raise StateMigrationError("Collector SQLite schema version is unsupported")
    try:
        _validate_schema(connection)
        _validate_audio_schema(connection)
    except StateDBError as error:
        raise StateMigrationError("Collector SQLite typed schema is invalid") from error
    if connection.execute("SELECT 1 FROM state WHERE kind IN ('attempt','quarantine') LIMIT 1").fetchone():
        raise InternalAudioCutoverRequiredError("legacy capture rows remain in typed state")


def _validate_v2(connection: sqlite3.Connection) -> None:
    """Validate the sole stopped-service source schema for the v3 migration."""

    connection.row_factory = sqlite3.Row
    version_row = cast(tuple[int] | None, connection.execute("PRAGMA user_version").fetchone())
    if version_row is None or version_row[0] != _LEGACY_TYPED_SCHEMA:
        raise StateMigrationError("Collector SQLite schema is not v2")
    try:
        _validate_schema(connection)
        _validate_audio_schema(connection, version=_LEGACY_TYPED_SCHEMA)
    except StateDBError as error:
        raise StateMigrationError("Collector SQLite v2 typed schema is invalid") from error
    if connection.execute("SELECT 1 FROM state WHERE kind IN ('attempt','quarantine') LIMIT 1").fetchone():
        raise InternalAudioCutoverRequiredError("legacy capture rows remain in v2 state")


def _validate_v3(connection: sqlite3.Connection) -> None:
    """Validate the stopped-service source schema for the v4 migration."""

    connection.row_factory = sqlite3.Row
    version_row = cast(tuple[int] | None, connection.execute("PRAGMA user_version").fetchone())
    if version_row is None or version_row[0] != _PREVIOUS_TYPED_SCHEMA:
        raise StateMigrationError("Collector SQLite schema is not v3")
    try:
        _validate_schema(connection)
        _validate_audio_schema(connection, version=_PREVIOUS_TYPED_SCHEMA)
    except StateDBError as error:
        raise StateMigrationError("Collector SQLite v3 typed schema is invalid") from error
    if connection.execute("SELECT 1 FROM state WHERE kind IN ('attempt','quarantine') LIMIT 1").fetchone():
        raise InternalAudioCutoverRequiredError("legacy capture rows remain in v3 state")


def open_runtime_state(layout: StorageLayout, *, read_only: bool = False) -> StateDB:
    """Open only a complete current database; runtime never creates or upgrades it."""

    _validate_canonical(layout, read_only=read_only)
    try:
        return StateDB(layout.collector.state_db, read_only=read_only)
    except StateDBError as error:
        try:
            _check_canonical_access(layout.collector.state_db, read_only=read_only)
        except PermissionError as denied:
            raise InternalAudioCutoverRequiredError(
                "Collector SQLite or journal access needs stopped-service repair"
            ) from denied
        raise StateMigrationError("Collector SQLite state cannot be opened") from error


def _check_canonical_access(path: Path, *, read_only: bool) -> None:
    flags = (os.O_RDONLY if read_only else os.O_RDWR) | os.O_NOFOLLOW | os.O_CLOEXEC
    descriptor = os.open(path, flags)
    os.close(descriptor)
    journal = path.parent / f"{path.name}-journal"
    try:
        descriptor = os.open(journal, flags | os.O_NONBLOCK)
    except FileNotFoundError:
        pass
    except OSError as error:
        if error.errno in {errno.ELOOP, errno.EISDIR}:
            raise InternalAudioCutoverRequiredError(
                "Collector SQLite journal is unsafe; stopped-service repair required"
            ) from error
        raise
    else:
        try:
            if not stat.S_ISREG(os.fstat(descriptor).st_mode):
                raise InternalAudioCutoverRequiredError(
                    "Collector SQLite journal is unsafe; stopped-service repair required"
                )
        finally:
            os.close(descriptor)
    if not read_only and not os.access(path.parent, os.W_OK | os.X_OK, effective_ids=True):
        raise PermissionError("Collector SQLite journal directory is not writable")


def _assert_private_audio_gone(layout: StorageLayout, connection: sqlite3.Connection) -> None:
    for root in (layout.collector.attempts, layout.collector.quarantine, layout.draft):
        try:
            mode = root.lstat().st_mode
        except FileNotFoundError:
            continue
        if not stat.S_ISDIR(mode):
            raise StateMigrationError(f"private audio root is unsafe: {root}")
        if any(root.iterdir()):
            raise InternalAudioCutoverRequiredError(f"private audio remains outside typed SQLite state: {root}")
    ready = layout.publication.root
    if not ready.exists() and not ready.is_symlink():
        return
    if ready.is_symlink() or not ready.is_dir():
        raise StateMigrationError("ready publication root is unsafe")
    for entry in ready.iterdir():
        if entry.name.startswith(".") and not _owned_typed_ready_stage(entry, connection):
            raise InternalAudioCutoverRequiredError("unowned ready scratch remains after cutover")


def _owned_typed_ready_stage(entry: Path, connection: sqlite3.Connection) -> bool:
    match = re.fullmatch(r"\.([0-9a-f]{64})\.([0-9a-f]{16})\.stage", entry.name)
    if match is None or not stat.S_ISDIR(entry.lstat().st_mode):
        return False
    return _owned_legacy_ready_stage(entry, match, connection) or _owned_v2_ready_stage(entry, match, connection)


def _owned_legacy_ready_stage(entry: Path, match: re.Match[str], connection: sqlite3.Connection) -> bool:
    rows = cast(
        list[tuple[str, str, str]],
        connection.execute(
            "SELECT stage_id,group_identity,owner_token FROM ready_output_stage WHERE owned_path=?",
            (str(entry),),
        ).fetchall(),
    )
    if len(rows) != 1:
        return False
    stage_id, identity, owner_token = rows[0]
    parts = re.fullmatch(r"([0-9a-f]{64}):([0-9]{1,19}):([0-9a-f]{64})", identity)
    if (
        parts is None
        or stage_id != sha256(identity.encode()).hexdigest()
        or owner_token != stage_id
        or match.groups() != (parts[3], stage_id[:16])
    ):
        return False
    if not _owned_stage_cohort(connection, parts):
        return False
    return all(
        child.name in {"records.bin", "manifest.json"} and stat.S_ISREG(child.lstat().st_mode)
        for child in entry.iterdir()
    )


def _owned_v2_ready_stage(entry: Path, match: re.Match[str], connection: sqlite3.Connection) -> bool:
    rows = cast(
        list[tuple[str, str, str, str]],
        connection.execute(
            "SELECT stage_id,group_identity,artifact_id,owner_token FROM ready_v2_output_stage WHERE owned_path=?",
            (str(entry),),
        ).fetchall(),
    )
    if len(rows) != 1:
        return False
    stage_id, identity, artifact_id, owner_token = rows[0]
    parts = re.fullmatch(r"([0-9a-f]{64}):([0-9]{1,19}):([0-9a-f]{64})", identity)
    if (
        parts is None
        or stage_id != sha256(identity.encode()).hexdigest()
        or owner_token != stage_id
        or artifact_id != parts[3]
        or match.groups() != (artifact_id, stage_id[:16])
    ):
        return False
    if not _owned_stage_cohort(connection, parts):
        return False
    return all(
        child.name in {"audio.ogg", "manifest.json"} and stat.S_ISREG(child.lstat().st_mode)
        for child in entry.iterdir()
    )


def _owned_stage_cohort(connection: sqlite3.Connection, parts: re.Match[str]) -> bool:
    cohort = cast(
        tuple[str, int] | None,
        connection.execute("SELECT lifecycle,cursor FROM publication_cohort WHERE cohort_id=?", (parts[1],)).fetchone(),
    )
    return cohort is not None and (cohort[0], cohort[1]) == ("staging", int(parts[2])) and str(cohort[1]) == parts[2]
