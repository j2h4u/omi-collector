"""Stopped-service state initialization, v2 migration, and journal recovery."""

from __future__ import annotations

import os
import sqlite3
import stat
from contextlib import closing
from pathlib import Path
from typing import cast
from urllib.parse import quote

from omi_collector.capture.adapters.collector_guard import collector_guard
from omi_collector.capture.adapters.collector_state_db import (
    StateDB,
    StateDBError,
    StateKind,
    install_public_audio_v2_schema_for_upgrade,
    install_replay_proof_schema_for_upgrade,
    raise_if_sqlite_storage_fault,
)
from omi_collector.capture.adapters.ready_bundles import ReadyBundleError, backfill_v2_replay_proofs
from omi_collector.state_migration import (
    StateMigrationError,
    _assert_private_audio_gone,
    _validate_current,
    _validate_v2,
    _validate_v3,
)
from omi_collector.storage_layout import StorageLayout


class InternalAudioMigrationError(RuntimeError):
    """Stopped-service state initialization or recovery was refused."""


_PREVIOUS_SCHEMA_VERSION = 3
_LEGACY_SCHEMA_VERSION = 2


def initialize_collector_state(layout: StorageLayout) -> dict[str, int]:
    """Initialize an empty installation or migrate a validated v2 database."""

    with collector_guard(layout.collector.lock):
        path = layout.collector.state_db
        _assert_canonical(path)
        if path.exists():
            before = _identity(path)
            inspection = inspect_collector_state(layout)
            if inspection.get("status") != "state_compatible":
                raise InternalAudioMigrationError("existing Collector state must pass read-only check-state first")
            if _identity(path) != before:
                raise InternalAudioMigrationError("canonical SQLite identity changed during state inspection")
            if inspection["schema_version"] == 0:
                StateDB(path)
            elif inspection["schema_version"] in {_LEGACY_SCHEMA_VERSION, _PREVIOUS_SCHEMA_VERSION}:
                _migrate_to_v4(layout, from_version=cast(int, inspection["schema_version"]))
            else:
                with closing(_connect(path, read_only=True)) as connection:
                    _validate_current(connection)
                    _assert_private_audio_gone(layout, connection)
            _handoff_canonical(path)
            return {"schema_version": 4}
        _validate_empty_install(layout)
        StateDB(path)
        _handoff_canonical(path)
        return {"schema_version": 4}


def inspect_collector_state(layout: StorageLayout) -> dict[str, object]:
    """Check whether stopped-service initialization can safely proceed."""

    try:
        path = layout.collector.state_db
        _assert_canonical(path)
        if not path.exists():
            _validate_empty_install(layout)
            return {"schema_version": None, "status": "state_compatible"}
        with closing(_connect(path, read_only=True)) as connection:
            version = cast(tuple[int] | None, connection.execute("PRAGMA user_version").fetchone())
            if version is not None and version[0] == 0:
                _validate_empty_schema(connection)
                _validate_empty_install(layout)
                return {"schema_version": 0, "status": "state_compatible"}
            version_value = None if version is None else version[0]
            if version_value == _LEGACY_SCHEMA_VERSION:
                _validate_v2(connection)
            elif version_value == _PREVIOUS_SCHEMA_VERSION:
                _validate_v3(connection)
            else:
                _validate_current(connection)
            _assert_private_audio_gone(layout, connection)
            return {"schema_version": version_value, "status": "state_compatible"}
    except _HotJournalError:
        return {"reason": "canonical_hot_journal", "status": "recovery_required"}
    except (InternalAudioMigrationError, StateMigrationError, StateDBError, OSError, sqlite3.DatabaseError) as error:
        raise_if_sqlite_storage_fault(error)
        return {"detail": str(error), "reason": "state_validation_failed", "status": "state_refused"}


def recover_canonical_journal(layout: StorageLayout) -> dict[str, object]:
    """Let SQLite recover its verified hot rollback journal under the stable guard."""

    with collector_guard(layout.collector.lock):
        result = inspect_collector_state(layout)
        if result.get("status") == "state_compatible":
            return result
        if result != {"reason": "canonical_hot_journal", "status": "recovery_required"}:
            raise InternalAudioMigrationError("canonical journal recovery was not approved by read-only inspection")
        path = layout.collector.state_db
        before = _canonical_journal_identity(path)
        with closing(_connect(path)):
            pass
        if _identity(path) != before:
            raise InternalAudioMigrationError("canonical SQLite identity changed during journal recovery")
        journal = path.with_name(f"{path.name}-journal")
        if journal.exists() or journal.is_symlink():
            raise InternalAudioMigrationError("canonical SQLite journal remains after recovery")
        result = inspect_collector_state(layout)
        if result.get("status") != "state_compatible":
            raise InternalAudioMigrationError("canonical state remains incompatible after SQLite recovery")
        return result


def _migrate_to_v4(layout: StorageLayout, *, from_version: int) -> None:
    path = layout.collector.state_db
    with closing(_connect(path)) as connection:
        try:
            connection.execute("PRAGMA temp_store = FILE")
            connection.execute("PRAGMA temp.cache_size = -256")
            connection.execute("BEGIN EXCLUSIVE")
            if from_version == _LEGACY_SCHEMA_VERSION:
                _validate_v2(connection)
            elif from_version == _PREVIOUS_SCHEMA_VERSION:
                _validate_v3(connection)
            else:
                raise InternalAudioMigrationError("Collector schema is not eligible for v4 migration")
            _assert_private_audio_gone(layout, connection)
            if from_version == _LEGACY_SCHEMA_VERSION:
                try:
                    install_replay_proof_schema_for_upgrade(connection)
                    backfill_v2_replay_proofs(layout.publication.root, connection)
                except (
                    OSError,
                    sqlite3.DatabaseError,
                    StateDBError,
                    ReadyBundleError,
                    ValueError,
                ) as error:
                    raise_if_sqlite_storage_fault(error)
                    raise InternalAudioMigrationError("Collector replay-proof migration failed") from error
                connection.execute("PRAGMA user_version = 3")
            install_public_audio_v2_schema_for_upgrade(connection)
            connection.execute(
                "INSERT INTO state(kind,id,payload) VALUES (?,?,?)",
                (
                    StateKind.PUBLICATION_CONTRACT.value,
                    "current",
                    '{"mode":"legacy_open","next_public_order":null,"version":1}',
                ),
            )
            connection.execute("PRAGMA user_version = 4")
            _validate_current(connection)
            connection.commit()
        except InternalAudioMigrationError:
            connection.rollback()
            raise
        except (
            OSError,
            sqlite3.DatabaseError,
            StateDBError,
            StateMigrationError,
            ReadyBundleError,
            ValueError,
        ) as error:
            connection.rollback()
            raise_if_sqlite_storage_fault(error)
            raise InternalAudioMigrationError("Collector public audio v2 schema migration failed") from error


class _HotJournalError(Exception):
    pass


def _assert_canonical(path: Path) -> None:
    try:
        mode = path.lstat().st_mode
    except FileNotFoundError:
        return
    if not stat.S_ISREG(mode):
        raise InternalAudioMigrationError("Collector SQLite path is not a regular file")


def _validate_empty_install(layout: StorageLayout) -> None:
    public_root = layout.collector.root / "public"
    for root in (
        layout.collector.attempts,
        layout.collector.quarantine,
        layout.draft,
        layout.publication.root,
        public_root,
    ):
        try:
            mode = root.lstat().st_mode
        except FileNotFoundError:
            continue
        if not stat.S_ISDIR(mode) or any(root.iterdir()):
            raise InternalAudioMigrationError("canonical DB is missing while Collector data exists")
    root = layout.collector.root
    if not root.exists():
        return
    for entry in root.iterdir():
        if entry not in {layout.collector.lock, layout.collector.state_db}:
            try:
                entry_mode = entry.lstat().st_mode
            except OSError as error:
                raise InternalAudioMigrationError("Collector control data is unavailable") from error
            if (
                entry.name not in {"attempts", "quarantine", "public"}
                or not stat.S_ISDIR(entry_mode)
                or any(entry.iterdir())
            ):
                raise InternalAudioMigrationError("canonical DB is missing while Collector control data exists")


def _identity(path: Path) -> tuple[int, int, int]:
    info = path.lstat()
    return info.st_dev, info.st_ino, info.st_uid


def _handoff_canonical(path: Path) -> None:
    parent = path.parent.lstat()
    if path.is_symlink() or not stat.S_ISREG(path.lstat().st_mode):
        raise InternalAudioMigrationError("canonical SQLite inode is unsafe for ownership handoff")
    os.chown(path, parent.st_uid, parent.st_gid, follow_symlinks=False)
    path.chmod(0o640, follow_symlinks=False)
    info = path.lstat()
    if (info.st_uid, info.st_gid, stat.S_IMODE(info.st_mode)) != (parent.st_uid, parent.st_gid, 0o640):
        raise InternalAudioMigrationError("canonical SQLite ownership handoff failed")
    descriptor = os.open(path, os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    _sync_directory(path.parent)


def _validate_empty_schema(connection: sqlite3.Connection) -> None:
    integrity = cast(tuple[str] | None, connection.execute("PRAGMA integrity_check").fetchone())
    pages = cast(tuple[int] | None, connection.execute("PRAGMA page_count").fetchone())
    free_pages = cast(tuple[int] | None, connection.execute("PRAGMA freelist_count").fetchone())
    objects = cast(tuple[int] | None, connection.execute("SELECT 1 FROM sqlite_master LIMIT 1").fetchone())
    if integrity is None or pages is None or free_pages is None:
        raise InternalAudioMigrationError("existing Collector SQLite v0 database is not empty")
    if integrity[0] != "ok" or pages[0] not in {0, 1} or free_pages[0] != 0 or objects is not None:
        raise InternalAudioMigrationError("existing Collector SQLite v0 database is not empty")


def _sync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC | os.O_NOFOLLOW)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _connect(path: Path, *, read_only: bool = False) -> sqlite3.Connection:
    if path.is_symlink() or not path.is_file():
        raise InternalAudioMigrationError("SQLite database path is unsafe")
    mode = "ro" if read_only else "rw"
    uri = f"file:{quote(path.absolute().as_posix(), safe='/')}?mode={mode}"
    connection = None
    try:
        connection = sqlite3.connect(uri, uri=True, timeout=5, isolation_level=None)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA busy_timeout = 5000")
        connection.execute("PRAGMA foreign_keys = ON")
        integrity = cast(tuple[str] | None, connection.execute("PRAGMA integrity_check").fetchone())
        if integrity is None or integrity[0] != "ok":
            raise InternalAudioMigrationError("Collector SQLite integrity check failed")
        return connection
    except sqlite3.DatabaseError as error:
        if connection is not None:
            connection.close()
        if getattr(error, "sqlite_errorcode", None) == sqlite3.SQLITE_READONLY_ROLLBACK:
            _canonical_journal_identity(path)
            raise _HotJournalError from error
        raise
    except BaseException:
        if connection is not None:
            connection.close()
        raise


def _canonical_journal_identity(path: Path) -> tuple[int, int, int]:
    journal = path.with_name(f"{path.name}-journal")
    try:
        database_info = path.lstat()
        journal_info = journal.lstat()
    except OSError as error:
        raise InternalAudioMigrationError("canonical SQLite rollback journal is unavailable") from error
    if not stat.S_ISREG(database_info.st_mode) or not stat.S_ISREG(journal_info.st_mode):
        raise InternalAudioMigrationError("canonical SQLite rollback journal is unsafe")
    if (journal_info.st_dev, journal_info.st_uid, journal_info.st_gid, journal_info.st_nlink) != (
        database_info.st_dev,
        database_info.st_uid,
        database_info.st_gid,
        1,
    ):
        raise InternalAudioMigrationError("canonical SQLite rollback journal is unsafe")
    return database_info.st_dev, database_info.st_ino, database_info.st_uid
