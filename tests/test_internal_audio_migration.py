from __future__ import annotations

import sqlite3
import stat
import subprocess
import sys
from contextlib import closing
from hashlib import sha256
from pathlib import Path
from typing import cast

import pytest

from omi_collector.capture.adapters.collector_state_db import (
    _AUDIO_DDL,
    _V3_REPLAY_PROOF_DDL,
    StateDB,
    StateKind,
)
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


def test_fresh_initialization_creates_v4_without_migration_receipt(tmp_path: Path) -> None:
    layout = _layout(tmp_path)
    assert inspect_collector_state(layout) == {"schema_version": None, "status": "state_compatible"}
    assert initialize_collector_state(layout) == {"schema_version": 4}
    with closing(sqlite3.connect(layout.collector.state_db)) as connection:
        assert connection.execute("PRAGMA user_version").fetchone() == (4,)
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
    assert initialize_collector_state(layout) == {"schema_version": 4}
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


def test_existing_v4_without_receipt_is_accepted_without_rewrite(tmp_path: Path) -> None:
    layout = _layout(tmp_path)
    db = StateDB(layout.collector.state_db)
    db.put(StateKind.MIGRATION, "operator-fact", {"value": 7})
    before = layout.collector.state_db.read_bytes()
    assert inspect_collector_state(layout)["status"] == "state_compatible"
    assert initialize_collector_state(layout) == {"schema_version": 4}
    assert layout.collector.state_db.read_bytes() == before


def _downgrade_current_database_to_v3(path: Path, *, with_active_proof: bool = True) -> None:
    bundle_id = "a" * 64
    with closing(sqlite3.connect(path)) as connection:
        connection.execute("PRAGMA foreign_keys = OFF")
        connection.execute("DROP TABLE ready_v2_stage_range")
        connection.execute("DROP TABLE ready_v2_output_stage")
        connection.execute("DROP TABLE ready_replay_proof_page")
        connection.execute("DROP TABLE ready_replay_proof")
        connection.execute(_V3_REPLAY_PROOF_DDL)
        connection.execute(_AUDIO_DDL[11])
        if with_active_proof:
            connection.execute(
                "INSERT INTO ready_replay_proof VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                (
                    bundle_id,
                    "active",
                    "stage",
                    "owner",
                    129,
                    "b" * 64,
                    "c" * 64,
                    b"index",
                    b"runs",
                    b"metadata",
                    "d" * 64,
                ),
            )
            connection.executemany(
                "INSERT INTO ready_replay_proof_page VALUES (?,?,?,?,?,?)",
                (
                    (bundle_id, 0, 0, 128, b"x" * (128 * 32), b"y" * 32),
                    (bundle_id, 1, 128, 1, b"z" * 32, b"w" * 32),
                ),
            )
        connection.execute("DELETE FROM state WHERE kind=? AND id='current'", (StateKind.PUBLICATION_CONTRACT.value,))
        connection.execute("PRAGMA user_version = 3")
        connection.commit()


def _downgrade_current_database_to_v2(path: Path) -> None:
    with closing(sqlite3.connect(path)) as connection:
        connection.execute("PRAGMA foreign_keys = OFF")
        connection.execute("DROP TABLE ready_v2_stage_range")
        connection.execute("DROP TABLE ready_v2_output_stage")
        connection.execute("DROP TABLE ready_replay_proof_page")
        connection.execute("DROP TABLE ready_replay_proof")
        connection.execute("DELETE FROM state WHERE kind=? AND id='current'", (StateKind.PUBLICATION_CONTRACT.value,))
        connection.execute("PRAGMA user_version = 2")
        connection.commit()


def test_drained_v2_migration_seeds_v2_contract(tmp_path: Path) -> None:
    layout = _layout(tmp_path)
    StateDB(layout.collector.state_db)
    _downgrade_current_database_to_v2(layout.collector.state_db)

    assert initialize_collector_state(layout) == {"schema_version": 4}
    assert StateDB(layout.collector.state_db, read_only=True).get(StateKind.PUBLICATION_CONTRACT, "current") == {
        "version": 1,
        "mode": "v2_open",
        "next_public_order": 0,
    }


def test_v2_migration_refuses_active_legacy_publication_without_mutation(tmp_path: Path) -> None:
    layout = _layout(tmp_path)
    database = StateDB(layout.collector.state_db)
    digest = "a" * 64
    bundle_id = sha256(f"0:1:{digest}".encode()).hexdigest()
    database.put(
        StateKind.READY_PUBLICATION,
        bundle_id,
        {
            "records_sha256": "b" * 64,
            "state": "ready",
            "start_sequence": 0,
            "next_sequence": 1,
            "draft_raw_sha256": digest,
        },
    )
    _downgrade_current_database_to_v2(layout.collector.state_db)

    with pytest.raises(InternalAudioMigrationError, match="must retire"):
        initialize_collector_state(layout)

    with closing(sqlite3.connect(layout.collector.state_db)) as connection:
        assert connection.execute("PRAGMA user_version").fetchone() == (2,)
        assert connection.execute("SELECT payload FROM state WHERE id=?", (bundle_id,)).fetchone() is not None
        assert connection.execute("SELECT name FROM sqlite_master WHERE name='ready_replay_proof'").fetchone() is None


def test_v2_migration_refuses_legacy_stage_without_mutation(tmp_path: Path) -> None:
    layout = _layout(tmp_path)
    StateDB(layout.collector.state_db)
    _downgrade_current_database_to_v2(layout.collector.state_db)
    cohort_id, bundle_id = "a" * 64, "b" * 64
    identity = f"{cohort_id}:0:{bundle_id}"
    stage_id = "c" * 64
    with closing(sqlite3.connect(layout.collector.state_db)) as connection:
        connection.execute(
            "INSERT INTO publication_cohort(cohort_id,lifecycle,cursor) VALUES (?,'staging',0)", (cohort_id,)
        )
        connection.execute(
            "INSERT INTO ready_output_stage(stage_id,group_identity,owned_path,expected_bundle_digest,"
            "expected_record_count,expected_slice_count,owner_token) VALUES (?,?,?,?,1,1,?)",
            (stage_id, identity, str(layout.publication.root / "owned.stage"), "d" * 64, stage_id),
        )
        connection.commit()

    with pytest.raises(InternalAudioMigrationError, match="publication work must finish"):
        initialize_collector_state(layout)

    with closing(sqlite3.connect(layout.collector.state_db)) as connection:
        assert connection.execute("PRAGMA user_version").fetchone() == (2,)
        assert connection.execute("SELECT stage_id FROM ready_output_stage").fetchone() == (stage_id,)
        assert connection.execute("SELECT name FROM sqlite_master WHERE name='ready_replay_proof'").fetchone() is None


def test_drained_v3_migration_seeds_v2_contract_with_empty_proof_tables(tmp_path: Path) -> None:
    layout = _layout(tmp_path)
    StateDB(layout.collector.state_db)
    _downgrade_current_database_to_v3(layout.collector.state_db, with_active_proof=False)

    assert inspect_collector_state(layout) == {"schema_version": 3, "status": "state_compatible"}
    assert initialize_collector_state(layout) == {"schema_version": 4}

    with closing(sqlite3.connect(layout.collector.state_db)) as connection:
        assert connection.execute("PRAGMA user_version").fetchone() == (4,)
        assert connection.execute("SELECT count(*) FROM ready_replay_proof").fetchone() == (0,)
        assert connection.execute("SELECT count(*) FROM ready_replay_proof_page").fetchone() == (0,)
        v2_table_rows = cast(
            list[tuple[str]],
            connection.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'ready_v2_%'"
            ).fetchall(),
        )
        assert {row[0] for row in v2_table_rows} == {"ready_v2_output_stage", "ready_v2_stage_range"}
    assert StateDB(layout.collector.state_db, read_only=True).get(StateKind.PUBLICATION_CONTRACT, "current") == {
        "version": 1,
        "mode": "v2_open",
        "next_public_order": 0,
    }


def test_v3_migration_refuses_active_legacy_proof_without_any_mutation(tmp_path: Path) -> None:
    layout = _layout(tmp_path)
    StateDB(layout.collector.state_db)
    _downgrade_current_database_to_v3(layout.collector.state_db)
    before_bytes = layout.collector.state_db.read_bytes()
    with closing(sqlite3.connect(layout.collector.state_db)) as connection:
        before = (
            connection.execute("PRAGMA user_version").fetchone(),
            connection.execute("SELECT * FROM ready_replay_proof").fetchall(),
            connection.execute("SELECT * FROM ready_replay_proof_page ORDER BY page_ordinal").fetchall(),
        )

    with pytest.raises(InternalAudioMigrationError, match="replay-proof custody must resolve"):
        initialize_collector_state(layout)

    assert layout.collector.state_db.read_bytes() == before_bytes
    with closing(sqlite3.connect(layout.collector.state_db)) as connection:
        assert (
            connection.execute("PRAGMA user_version").fetchone(),
            connection.execute("SELECT * FROM ready_replay_proof").fetchall(),
            connection.execute("SELECT * FROM ready_replay_proof_page ORDER BY page_ordinal").fetchall(),
        ) == before
        assert (
            connection.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name='ready_v2_output_stage'"
            ).fetchone()
            is None
        )


@pytest.mark.parametrize(
    ("table", "reason"),
    (
        ("ready_replay_proof_page", "replay-proof custody must resolve"),
        ("publication_cohort_member", "publication work must finish"),
        ("ready_stage_range", "publication work must finish"),
    ),
)
def test_v3_migration_refuses_orphan_legacy_custody_without_mutation(tmp_path: Path, table: str, reason: str) -> None:
    layout = _layout(tmp_path)
    StateDB(layout.collector.state_db)
    _downgrade_current_database_to_v3(layout.collector.state_db, with_active_proof=False)
    statements: dict[str, tuple[str, tuple[object, ...]]] = {
        "ready_replay_proof_page": (
            "INSERT INTO ready_replay_proof_page VALUES (?,?,?,?,?,?)",
            ("a" * 64, 0, 0, 1, b"b" * 32, b"c" * 32),
        ),
        "publication_cohort_member": (
            "INSERT INTO publication_cohort_member VALUES (?,?,?,?,?,?,?,?,?)",
            ("orphan", 0, "a" * 32, 0, 0, "sealed_waiting", 0, "b" * 64, 0),
        ),
        "ready_stage_range": (
            "INSERT INTO ready_stage_range VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            ("orphan", 0, 0, "a" * 32, 0, 1, bytes(8), (1).to_bytes(8), 0, 0, None, None, None, None, None, None),
        ),
    }
    with closing(sqlite3.connect(layout.collector.state_db)) as connection:
        connection.execute(*statements[table])
        connection.commit()
    before = layout.collector.state_db.read_bytes()

    with pytest.raises(InternalAudioMigrationError, match=reason):
        initialize_collector_state(layout)

    assert layout.collector.state_db.read_bytes() == before
    with closing(sqlite3.connect(layout.collector.state_db)) as connection:
        assert connection.execute("PRAGMA user_version").fetchone() == (3,)
        assert connection.execute(f"SELECT count(*) FROM {table}").fetchone() == (1,)


def test_v3_migration_rolls_back_schema_on_failure(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    layout = _layout(tmp_path)
    StateDB(layout.collector.state_db)
    _downgrade_current_database_to_v3(layout.collector.state_db, with_active_proof=False)
    import omi_collector.internal_audio_migration as migration

    original = migration.install_public_audio_v2_schema_for_upgrade

    def fail_after_schema(connection: sqlite3.Connection) -> None:
        original(connection)
        raise sqlite3.OperationalError("injected migration failure")

    monkeypatch.setattr(migration, "install_public_audio_v2_schema_for_upgrade", fail_after_schema)
    with pytest.raises(InternalAudioMigrationError, match="schema migration failed"):
        initialize_collector_state(layout)

    with closing(sqlite3.connect(layout.collector.state_db)) as connection:
        assert connection.execute("PRAGMA user_version").fetchone() == (3,)
        assert connection.execute("SELECT count(*) FROM ready_replay_proof_page").fetchone() == (0,)
        assert (
            connection.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name='ready_v2_output_stage'"
            ).fetchone()
            is None
        )


def test_interrupted_empty_database_initializes_in_place(tmp_path: Path) -> None:
    layout = _layout(tmp_path)
    layout.collector.root.mkdir()
    path = layout.collector.state_db
    with closing(sqlite3.connect(path)):
        pass
    identity = path.stat().st_ino
    assert inspect_collector_state(layout) == {"schema_version": 0, "status": "state_compatible"}
    assert initialize_collector_state(layout) == {"schema_version": 4}
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


def test_hot_journal_preflight_and_recovery_keep_v4_inode_and_rows(tmp_path: Path) -> None:
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

    assert recover_canonical_journal(layout) == {"schema_version": 4, "status": "state_compatible"}
    assert path.stat().st_ino == identity
    assert not journal.exists()
    assert StateDB(path, read_only=True).get(StateKind.MIGRATION, "operator-fact") == {"value": 7}
