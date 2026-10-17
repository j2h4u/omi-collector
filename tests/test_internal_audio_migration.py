from __future__ import annotations

import sqlite3
import stat
import subprocess
import sys
from contextlib import closing
from pathlib import Path

import pytest

from omi_collector.capture.adapters.collector_state_db import StateDB, StateKind
from omi_collector.internal_audio_migration import (
    InternalAudioMigrationError,
    initialize_collector_state,
    inspect_collector_state,
)
from omi_collector.storage_layout import StorageLayout, load_operator_config


def _layout(tmp_path: Path) -> StorageLayout:
    config = tmp_path / "config.toml"
    config.write_text('pendant_address = "AA:BB:CC:DD:EE:FF"\nbundle_min_audio_seconds = 120\n', encoding="utf-8")
    return load_operator_config(config).storage


def test_fresh_initialization_creates_v2_without_migration_receipt(tmp_path: Path) -> None:
    layout = _layout(tmp_path)
    assert inspect_collector_state(layout) == {"schema_version": None, "status": "state_compatible"}
    assert initialize_collector_state(layout) == {"schema_version": 2}
    with closing(sqlite3.connect(layout.collector.state_db)) as connection:
        assert connection.execute("PRAGMA user_version").fetchone() == (2,)
        assert connection.execute("SELECT 1 FROM state WHERE id='internal_audio_cutover'").fetchone() is None
    database = layout.collector.state_db.stat()
    owner = layout.collector.root.stat()
    assert (database.st_uid, database.st_gid, stat.S_IMODE(database.st_mode)) == (
        owner.st_uid,
        owner.st_gid,
        0o640,
    )


def test_fresh_initialization_accepts_empty_public_directory(tmp_path: Path) -> None:
    layout = _layout(tmp_path)
    public_root = layout.collector.root / "public"
    public_root.mkdir(parents=True)

    assert inspect_collector_state(layout) == {"schema_version": None, "status": "state_compatible"}
    assert initialize_collector_state(layout) == {"schema_version": 2}
    assert layout.collector.state_db.is_file()


@pytest.mark.parametrize("public_entry", ["payload", "legacy.json"])
def test_missing_database_with_nonempty_public_directory_refuses(tmp_path: Path, public_entry: str) -> None:
    layout = _layout(tmp_path)
    public_root = layout.collector.root / "public"
    public_root.mkdir(parents=True)
    (public_root / public_entry).write_text("retained", encoding="utf-8")

    with pytest.raises(InternalAudioMigrationError, match="data exists"):
        initialize_collector_state(layout)
    assert not layout.collector.state_db.exists()


def test_missing_database_with_symlinked_public_directory_refuses(tmp_path: Path) -> None:
    layout = _layout(tmp_path)
    layout.collector.root.mkdir()
    outside = tmp_path / "outside-public"
    outside.mkdir()
    (layout.collector.root / "public").symlink_to(outside, target_is_directory=True)

    with pytest.raises(InternalAudioMigrationError, match="data exists"):
        initialize_collector_state(layout)
    assert not layout.collector.state_db.exists()


def test_existing_v2_without_receipt_is_accepted_without_rewrite(tmp_path: Path) -> None:
    layout = _layout(tmp_path)
    db = StateDB(layout.collector.state_db)
    db.put(StateKind.MIGRATION, "operator-fact", {"value": 7})
    before = layout.collector.state_db.read_bytes()
    assert inspect_collector_state(layout)["status"] == "state_compatible"
    assert initialize_collector_state(layout) == {"schema_version": 2}
    assert layout.collector.state_db.read_bytes() == before


def test_interrupted_empty_database_initializes_in_place(tmp_path: Path) -> None:
    layout = _layout(tmp_path)
    layout.collector.root.mkdir()
    path = layout.collector.state_db
    with closing(sqlite3.connect(path)):
        pass
    identity = path.stat().st_ino
    assert inspect_collector_state(layout) == {"schema_version": 0, "status": "state_compatible"}
    assert initialize_collector_state(layout) == {"schema_version": 2}
    assert path.stat().st_ino == identity


def test_unknown_existing_v0_database_is_refused_unchanged(tmp_path: Path) -> None:
    layout = _layout(tmp_path)
    layout.collector.root.mkdir()
    path = layout.collector.state_db
    with closing(sqlite3.connect(path)) as connection:
        connection.execute("CREATE TABLE unrelated(value TEXT)")
    before = path.read_bytes()
    assert inspect_collector_state(layout)["status"] == "state_refused"
    with pytest.raises(InternalAudioMigrationError):
        initialize_collector_state(layout)
    assert path.read_bytes() == before


def test_v1_is_refused_without_modification(tmp_path: Path) -> None:
    layout = _layout(tmp_path)
    layout.collector.root.mkdir()
    with closing(sqlite3.connect(layout.collector.state_db)) as connection:
        connection.execute("CREATE TABLE state(kind TEXT, id TEXT, payload TEXT)")
        connection.execute("PRAGMA user_version=1")
        connection.commit()
    before = layout.collector.state_db.read_bytes()
    with pytest.raises(InternalAudioMigrationError):
        initialize_collector_state(layout)
    assert layout.collector.state_db.read_bytes() == before


def test_missing_database_with_ready_data_refuses_creation(tmp_path: Path) -> None:
    layout = _layout(tmp_path)
    layout.publication.root.mkdir(parents=True)
    (layout.publication.root / "bundle").mkdir()
    with pytest.raises(InternalAudioMigrationError, match="data exists"):
        initialize_collector_state(layout)
    assert not layout.collector.state_db.exists()


@pytest.mark.parametrize("root_name", ["attempts", "quarantine", "draft", "ready", "unexpected"])
def test_missing_database_with_existing_private_or_control_data_refuses(tmp_path: Path, root_name: str) -> None:
    layout = _layout(tmp_path)
    root = {
        "attempts": layout.collector.attempts,
        "quarantine": layout.collector.quarantine,
        "draft": layout.draft,
        "ready": layout.publication.root,
        "unexpected": layout.collector.root / "unknown-control",
    }[root_name]
    root.mkdir(parents=True)
    (root / "artifact").write_bytes(b"retained")
    with pytest.raises(InternalAudioMigrationError):
        initialize_collector_state(layout)
    assert not layout.collector.state_db.exists()


def test_missing_database_with_symlinked_private_root_refuses(tmp_path: Path) -> None:
    layout = _layout(tmp_path)
    layout.collector.root.mkdir()
    outside = tmp_path / "outside"
    outside.mkdir()
    layout.collector.attempts.symlink_to(outside, target_is_directory=True)
    with pytest.raises(InternalAudioMigrationError):
        initialize_collector_state(layout)
    assert not layout.collector.state_db.exists()


def test_hot_journal_preflight_and_recovery_keep_v2_inode_and_rows(tmp_path: Path) -> None:
    layout = _layout(tmp_path)
    db = StateDB(layout.collector.state_db)
    db.put(StateKind.MIGRATION, "operator-fact", {"value": 7})
    path = layout.collector.state_db
    with closing(sqlite3.connect(path)) as connection:
        connection.executemany(
            "INSERT INTO state VALUES ('padding', ?, ?)",
            ((str(index), "x" * 1000) for index in range(1000)),
        )
        connection.commit()
    identity = path.stat().st_ino
    subprocess.run(
        (
            sys.executable,
            "-c",
            (
                "import os,sqlite3,sys; c=sqlite3.connect(sys.argv[1],isolation_level=None); "
                "c.execute('PRAGMA cache_size=1'); c.execute('BEGIN IMMEDIATE'); "
                "c.execute(\"UPDATE state SET payload='transient'\"); os._exit(0)"
            ),
            str(path),
        ),
        check=True,
        timeout=10,
    )
    journal = path.with_name(f"{path.name}-journal")
    assert journal.is_file()
    with (
        closing(sqlite3.connect(f"{path.as_uri()}?mode=ro", uri=True)) as connection,
        pytest.raises(sqlite3.DatabaseError) as error,
    ):
        connection.execute("PRAGMA integrity_check").fetchone()
    assert error.value.sqlite_errorcode == sqlite3.SQLITE_READONLY_ROLLBACK
    assert inspect_collector_state(layout) == {
        "reason": "canonical_hot_journal",
        "status": "recovery_required",
    }
    from omi_collector.internal_audio_migration import recover_canonical_journal

    assert recover_canonical_journal(layout) == {"schema_version": 2, "status": "state_compatible"}
    assert path.stat().st_ino == identity
    assert not journal.exists()
    assert StateDB(path, read_only=True).get(StateKind.MIGRATION, "operator-fact") == {"value": 7}
