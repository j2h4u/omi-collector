from __future__ import annotations

import grp
import hashlib
import json
import os
import pwd
import sqlite3
import subprocess
import sys
from contextlib import closing
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from typing import cast

import pytest

import omi_collector.internal_audio_migration as migration
from omi_collector.capture.adapters.collector_state_db import StateDB, StateKind
from omi_collector.capture.adapters.legacy_audio_import import LegacyAudioImportError
from omi_collector.internal_audio_migration import InternalAudioMigrationError, migrate_internal_audio
from omi_collector.storage_layout import StorageLayout, load_operator_config

_REAL_HANDOFF_ACCESS = migration._verify_handoff_access


@pytest.fixture(autouse=True)
def _simulate_distinct_provisioned_accounts(monkeypatch: pytest.MonkeyPatch) -> None:
    """Temporary unit roots share one UID; production probes two principals."""

    monkeypatch.setattr(migration, "_verify_handoff_access", lambda _path: None)


@pytest.mark.skipif(os.geteuid() != 0, reason="real account probe requires interactive root execution")
def test_real_service_and_operator_handoff_access() -> None:
    """Exercise real account transitions only against this test's private tree."""

    service = pwd.getpwnam("omi-collector")
    operator = pwd.getpwnam("j2h4u")
    operator_group = grp.getgrnam("j2h4u")
    assert service.pw_uid != operator.pw_uid
    assert operator.pw_gid == operator_group.gr_gid
    with TemporaryDirectory(prefix="omi-collector-handoff-", dir="/var/tmp") as temporary:
        root = Path(temporary)
        database = root / "collector-state.sqlite3"
        StateDB(database).put(StateKind.MIGRATION, "test", {"version": 1})
        os.chown(root, service.pw_uid, operator_group.gr_gid)
        root.chmod(0o2750)
        os.chown(database, service.pw_uid, operator_group.gr_gid)
        database.chmod(0o640)

        _REAL_HANDOFF_ACCESS(database)

        assert not list(root.glob(".collector-state.access.*"))


def _layout(tmp_path: Path) -> StorageLayout:
    config = tmp_path / "config.toml"
    config.write_text(
        '[pendant]\naddress = "AA:BB:CC:DD:EE:FF"\n[ready]\ntarget_audio_seconds = 120\n', encoding="utf-8"
    )
    layout = load_operator_config(config).storage
    layout.collector.root.mkdir()
    layout.collector.root.chmod(0o2750)
    layout.draft.mkdir()
    layout.publication.root.mkdir()
    return layout


def _release(tmp_path: Path) -> Path:
    path = tmp_path / "release.json"
    path.write_text(json.dumps({"source_revision": "a" * 40}), encoding="utf-8")
    return path


def _v1(layout: StorageLayout) -> None:
    connection = sqlite3.connect(layout.collector.state_db)
    connection.execute(
        "CREATE TABLE state (kind TEXT NOT NULL, id TEXT NOT NULL, payload TEXT NOT NULL, PRIMARY KEY(kind,id)) WITHOUT ROWID"
    )
    connection.execute("PRAGMA user_version = 1")
    connection.execute(
        "INSERT INTO state VALUES ('migration','current',?)", (json.dumps({"phase": "complete", "version": 1}),)
    )
    connection.execute("INSERT INTO state VALUES ('runtime_status','current',?)", ('{"last_error":"тест"}',))
    connection.commit()
    connection.close()


def _draft(layout: StorageLayout, *, count: int = 257) -> tuple[Path, bytes]:
    raw = b"".join(index.to_bytes(4, "big") + bytes([index % 251]) * 440 for index in range(count))
    digest = hashlib.sha256(raw).hexdigest()
    path = layout.draft / f"700-{700 + count}-{digest[:16]}"
    path.mkdir()
    (path / "records.bin").write_bytes(raw)
    (path / "manifest.json").write_text(
        json.dumps(
            {
                "schema_version": 2,
                "start_sequence": 700,
                "next_sequence": 700 + count,
                "record_count": count,
                "record_size": 444,
                "raw_sha256": digest,
            },
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    return path, raw


@pytest.mark.parametrize("count", [0, 1, 128, 129, 4096])
def test_cutover_growth_stays_inside_reserved_page_bound(tmp_path: Path, count: int) -> None:
    layout = _layout(tmp_path)
    release = _release(tmp_path)
    _v1(layout)
    if count:
        _draft(layout, count=count)
    before = layout.collector.state_db.stat().st_size
    with closing(sqlite3.connect(layout.collector.state_db)) as connection:
        page_size = cast(int, connection.execute("PRAGMA page_size").fetchone()[0])

    result = migrate_internal_audio(layout, release_metadata=release)

    assert result["imported_sources"] == int(count > 0)
    with closing(sqlite3.connect(layout.collector.state_db)) as connection:
        assert connection.execute("SELECT count(*) FROM capture_record").fetchone() == (count,)
    assert layout.collector.state_db.stat().st_size - before <= (64 + 2 * count + 4 * int(count > 0)) * page_size


def test_one_way_cutover_imports_draft_preserves_other_rows_and_deletes_only_old_private_copy(tmp_path: Path) -> None:
    layout = _layout(tmp_path)
    release = _release(tmp_path)
    _v1(layout)
    draft, raw = _draft(layout)
    ready = layout.publication.root / "immutable-ready"
    ready.mkdir()
    (ready / "records.bin").write_bytes(b"ready-data")

    result = migrate_internal_audio(layout, release_metadata=release)

    assert result == {"imported_sources": 1, "removed_files": 2}
    assert not draft.exists()
    assert (ready / "records.bin").read_bytes() == b"ready-data"
    connection = sqlite3.connect(layout.collector.state_db)
    assert connection.execute("PRAGMA user_version").fetchone()[0] == 2
    assert json.loads(
        cast(str, connection.execute("SELECT payload FROM state WHERE kind='runtime_status'").fetchone()[0])
    ) == {"last_error": "тест"}
    cutover_receipt = cast(
        dict[str, object],
        json.loads(
            cast(
                str,
                connection.execute(
                    "SELECT payload FROM state WHERE kind='migration' AND id='internal_audio_cutover'"
                ).fetchone()[0],
            )
        ),
    )
    assert cutover_receipt["phase"] == "COMPLETE"
    assert set(cutover_receipt) == {"format", "phase", "source_revision", "source_sha256", "imported_sources"}
    rows = cast(
        list[tuple[int, bytes]],
        cast(
            object,
            connection.execute("SELECT device_timestamp,payload FROM capture_record ORDER BY ordinal").fetchall(),
        ),
    )
    assert b"".join(timestamp.to_bytes(4, "big") + payload for timestamp, payload in rows) == raw
    connection.close()
    assert migrate_internal_audio(layout, release_metadata=release) == {"imported_sources": 1, "removed_files": 0}


def test_cutover_receipt_binds_setgid_parent_and_resume_keeps_effective_migrator_identity(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    layout = _layout(tmp_path)
    release = _release(tmp_path)
    _v1(layout)
    _draft(layout)
    parent = layout.collector.root
    original_egid = os.getegid()
    candidate_gid = next((gid for gid in os.getgroups() if gid != original_egid), None)
    if candidate_gid is None:
        pytest.skip("test needs a distinct supplementary group")
    os.chown(parent, os.getuid(), candidate_gid)
    parent.chmod(0o2750)
    monkeypatch.setattr(migration.os, "getegid", lambda: original_egid + 1)
    activate = migration._activate_candidate

    def interrupt(*_args: object) -> dict[str, int]:
        raise RuntimeError("interrupted after candidate creation")

    monkeypatch.setattr(migration, "_activate_candidate", interrupt)
    with pytest.raises(RuntimeError, match="interrupted after candidate creation"):
        migrate_internal_audio(layout, release_metadata=release)
    candidate = next(parent.glob(".collector-state.sqlite3.migrating.*"))
    with closing(sqlite3.connect(layout.collector.state_db)) as connection:
        receipt = json.loads(
            cast(
                str,
                connection.execute(
                    "SELECT payload FROM state WHERE kind='migration' AND id='internal_audio_cutover'"
                ).fetchone()[0],
            )
        )
    assert receipt["uid"] == os.geteuid()
    assert receipt["gid"] == original_egid + 1
    assert receipt["candidate_gid"] == candidate_gid
    assert receipt["parent"] == {
        "dev": parent.stat().st_dev,
        "ino": parent.stat().st_ino,
        "uid": parent.stat().st_uid,
        "gid": candidate_gid,
        "mode": 0o2750,
    }
    assert candidate.stat().st_uid == os.geteuid()
    assert candidate.stat().st_gid == candidate_gid
    monkeypatch.setattr(migration, "_activate_candidate", activate)
    assert migrate_internal_audio(layout, release_metadata=release) == {"imported_sources": 1, "removed_files": 2}


@pytest.mark.parametrize("drift", ["parent", "candidate", "journal"])
def test_cutover_refuses_bound_identity_drift_before_removal(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, drift: str
) -> None:
    layout = _layout(tmp_path)
    release = _release(tmp_path)
    _v1(layout)
    _draft(layout)
    activate = migration._activate_candidate

    def interrupt(*_args: object) -> dict[str, int]:
        raise RuntimeError("interrupted after candidate creation")

    monkeypatch.setattr(migration, "_activate_candidate", interrupt)
    with pytest.raises(RuntimeError, match="interrupted after candidate creation"):
        migrate_internal_audio(layout, release_metadata=release)
    candidate = next(layout.collector.root.glob(".collector-state.sqlite3.migrating.*"))
    if drift == "parent":
        candidate.parent.chmod(0o0750)
    elif drift == "candidate":
        wrong_gid = next(gid for gid in os.getgroups() if gid != candidate.parent.stat().st_gid)
        os.chown(candidate, os.getuid(), wrong_gid)
    else:
        subprocess.run(
            (
                sys.executable,
                "-c",
                "import os, sqlite3, sys; c=sqlite3.connect(sys.argv[1], isolation_level=None); "
                "c.execute('BEGIN IMMEDIATE'); c.execute(\"UPDATE state SET payload='journal' "
                "WHERE kind='runtime_status' AND id='current'\"); os._exit(0)",
                str(candidate),
            ),
            check=True,
        )
        journal = candidate.parent / f"{candidate.name}-journal"
        assert journal.exists()
        wrong_gid = next(gid for gid in os.getgroups() if gid != candidate.parent.stat().st_gid)
        os.chown(journal, os.getuid(), wrong_gid)
    monkeypatch.setattr(migration, "_activate_candidate", activate)
    with pytest.raises(InternalAudioMigrationError):
        migrate_internal_audio(layout, release_metadata=release)
    assert candidate.exists()


def test_cutover_refuses_unknown_private_artifact_before_source_receipt(tmp_path: Path) -> None:
    layout = _layout(tmp_path)
    release = _release(tmp_path)
    _v1(layout)
    unknown = layout.collector.attempts / "unexpected"
    unknown.parent.mkdir()
    unknown.write_bytes(b"do not delete")

    with pytest.raises(InternalAudioMigrationError, match="needs typed conversion"):
        migrate_internal_audio(layout, release_metadata=release)

    assert unknown.read_bytes() == b"do not delete"
    connection = sqlite3.connect(layout.collector.state_db)
    assert connection.execute("PRAGMA user_version").fetchone()[0] == 1
    assert connection.execute("SELECT 1 FROM state WHERE id='internal_audio_cutover'").fetchone() is None
    connection.close()


def test_cutover_refuses_foreign_candidate_before_receipt(tmp_path: Path) -> None:
    layout = _layout(tmp_path)
    release = _release(tmp_path)
    _v1(layout)
    foreign = layout.collector.root / ".collector-state.sqlite3.migrating.foreign"
    foreign.write_bytes(b"unowned")

    with pytest.raises(InternalAudioMigrationError, match="foreign cutover candidate"):
        migrate_internal_audio(layout, release_metadata=release)

    assert foreign.read_bytes() == b"unowned"
    with closing(sqlite3.connect(layout.collector.state_db)) as connection:
        assert connection.execute("SELECT 1 FROM state WHERE id='internal_audio_cutover'").fetchone() is None


def test_cutover_refuses_unsupported_static_candidate_before_receipt(tmp_path: Path) -> None:
    layout = _layout(tmp_path)
    release = _release(tmp_path)
    _v1(layout)
    candidate = layout.collector.root / ".collector-state.sqlite3.migrating"
    candidate.write_bytes(b"unknown prior artifact")

    with pytest.raises(InternalAudioMigrationError, match="unsupported legacy cutover candidate"):
        migrate_internal_audio(layout, release_metadata=release)

    assert candidate.read_bytes() == b"unknown prior artifact"
    with closing(sqlite3.connect(layout.collector.state_db)) as connection:
        assert connection.execute("SELECT 1 FROM state WHERE id='internal_audio_cutover'").fetchone() is None


def test_cutover_space_failure_precedes_receipt_and_candidate(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    layout = _layout(tmp_path)
    release = _release(tmp_path)
    _v1(layout)
    draft, _ = _draft(layout)
    monkeypatch.setattr(migration.os, "statvfs", lambda _path: SimpleNamespace(f_bavail=0, f_frsize=4096))

    with pytest.raises(InternalAudioMigrationError, match="insufficient Collector SQLite filesystem space") as error:
        migrate_internal_audio(layout, release_metadata=release)

    assert "page_count=" in str(error.value)
    assert "candidate_base=" in str(error.value)
    assert "typed_growth=" in str(error.value)
    assert "journal_bound=" in str(error.value)
    assert "required=" in str(error.value)
    assert draft.exists()
    assert not tuple(layout.collector.root.glob(".collector-state.sqlite3.migrating.*"))
    with closing(sqlite3.connect(layout.collector.state_db)) as connection:
        assert connection.execute("PRAGMA user_version").fetchone() == (1,)
        assert connection.execute("SELECT 1 FROM state WHERE id='internal_audio_cutover'").fetchone() is None


def test_cutover_reuses_only_its_verified_prepared_candidate(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    layout = _layout(tmp_path)
    release = _release(tmp_path)
    _v1(layout)
    draft, _ = _draft(layout)
    activate = migration._activate_candidate

    def stop_after_prepare(*_args: object) -> dict[str, int]:
        raise RuntimeError("simulated prepared candidate crash")

    monkeypatch.setattr(migration, "_activate_candidate", stop_after_prepare)
    with pytest.raises(RuntimeError, match="prepared candidate crash"):
        migrate_internal_audio(layout, release_metadata=release)
    candidate = next(layout.collector.root.glob(".collector-state.sqlite3.migrating.*"))
    assert candidate.is_file()
    assert draft.exists()

    def reject_rebuild(*_args: object) -> None:
        raise AssertionError("prepared candidate must be reused")

    monkeypatch.setattr(migration, "_activate_candidate", activate)
    monkeypatch.setattr(migration, "_backup_and_convert", reject_rebuild)
    assert migrate_internal_audio(layout, release_metadata=release) == {"imported_sources": 1, "removed_files": 2}
    assert not candidate.exists()
    assert not draft.exists()


@pytest.mark.parametrize("wrong_ddl", [False, True])
def test_cutover_rebuilds_owned_candidate_with_wrong_typed_index(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, wrong_ddl: bool
) -> None:
    layout = _layout(tmp_path)
    release = _release(tmp_path)
    _v1(layout)
    activate = migration._activate_candidate

    def stop_after_prepare(*_args: object) -> dict[str, int]:
        raise RuntimeError("simulated prepared candidate crash")

    monkeypatch.setattr(migration, "_activate_candidate", stop_after_prepare)
    with pytest.raises(RuntimeError, match="prepared candidate crash"):
        migrate_internal_audio(layout, release_metadata=release)
    candidate = next(layout.collector.root.glob(".collector-state.sqlite3.migrating.*"))
    with closing(sqlite3.connect(candidate)) as connection:
        connection.execute("DROP INDEX capture_record_mapping_revision_idx")
        if wrong_ddl:
            connection.execute(
                "CREATE INDEX capture_record_mapping_revision_idx ON capture_record (source_id, ordinal)"
            )
        connection.commit()
    source_bytes = layout.collector.state_db.read_bytes()
    backup = migration._backup_and_convert
    backup_calls = 0

    def count_rebuild(
        source: sqlite3.Connection, candidate_path: Path, receipt: dict[str, object], inventory: migration._Inventory
    ) -> None:
        nonlocal backup_calls
        backup_calls += 1
        backup(source, candidate_path, receipt, inventory)

    def activate_after_verified_rebuild(
        release_layout: StorageLayout,
        source: sqlite3.Connection,
        owned_candidate: migration._OwnedCandidate,
        receipt: dict[str, object],
        inventory: migration._Inventory,
    ) -> dict[str, int]:
        assert layout.collector.state_db.read_bytes() == source_bytes
        return activate(release_layout, source, owned_candidate, receipt, inventory)

    monkeypatch.setattr(migration, "_backup_and_convert", count_rebuild)
    monkeypatch.setattr(migration, "_activate_candidate", activate_after_verified_rebuild)
    assert migrate_internal_audio(layout, release_metadata=release) == {"imported_sources": 0, "removed_files": 0}
    assert backup_calls == 1
    assert not candidate.exists()
    with closing(sqlite3.connect(layout.collector.state_db)) as connection:
        assert connection.execute("PRAGMA user_version").fetchone() == (2,)
        assert connection.execute(
            "SELECT sql FROM sqlite_master WHERE type='index' AND name='capture_record_mapping_revision_idx'"
        ).fetchone() == (
            "CREATE INDEX capture_record_mapping_revision_idx ON capture_record (mapping_revision, source_id, ordinal)",
        )


def test_cutover_refuses_symlink_replacing_its_owned_candidate(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    layout = _layout(tmp_path)
    release = _release(tmp_path)
    _v1(layout)
    _draft(layout)

    def stop_after_prepare(*_args: object) -> dict[str, int]:
        raise RuntimeError("simulated prepared candidate crash")

    monkeypatch.setattr(migration, "_activate_candidate", stop_after_prepare)
    with pytest.raises(RuntimeError, match="prepared candidate crash"):
        migrate_internal_audio(layout, release_metadata=release)
    candidate = next(layout.collector.root.glob(".collector-state.sqlite3.migrating.*"))
    candidate.unlink()
    sentinel = tmp_path / "do-not-touch"
    sentinel.write_bytes(b"keep me")
    candidate.symlink_to(sentinel)

    with pytest.raises(InternalAudioMigrationError, match="candidate is not owned"):
        migrate_internal_audio(layout, release_metadata=release)
    assert sentinel.read_bytes() == b"keep me"
    assert candidate.is_symlink()


@pytest.mark.parametrize("adopt", [False, True])
def test_cutover_refuses_candidate_inode_change_before_removal_or_adoption(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, adopt: bool
) -> None:
    layout = _layout(tmp_path)
    release = _release(tmp_path)
    _v1(layout)
    draft, _ = _draft(layout)

    def stop_after_prepare(*_args: object) -> dict[str, int]:
        raise RuntimeError("simulated prepared candidate crash")

    activate_candidate = migration._activate_candidate
    monkeypatch.setattr(migration, "_activate_candidate", stop_after_prepare)
    with pytest.raises(RuntimeError, match="prepared candidate crash"):
        migrate_internal_audio(layout, release_metadata=release)
    monkeypatch.setattr(migration, "_activate_candidate", activate_candidate)
    candidate = next(layout.collector.root.glob(".collector-state.sqlite3.migrating.*"))

    def swap_candidate(*_args: object) -> bool:
        replacement = candidate.with_name("replacement.sqlite3")
        replacement.write_bytes(candidate.read_bytes())
        replacement.replace(candidate)
        return adopt

    monkeypatch.setattr(migration, "_candidate_is_prepared", swap_candidate)
    with pytest.raises(InternalAudioMigrationError, match="candidate identity changed"):
        migrate_internal_audio(layout, release_metadata=release)
    assert draft.exists()
    assert candidate.is_file()
    with closing(sqlite3.connect(layout.collector.state_db)) as connection:
        assert connection.execute("PRAGMA user_version").fetchone() == (1,)


def test_cutover_rebuilds_nonadoptable_candidate_after_real_hot_journal(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    layout = _layout(tmp_path)
    release = _release(tmp_path)
    _v1(layout)
    draft, _ = _draft(layout)
    activate = migration._activate_candidate

    def stop_after_prepare(*_args: object) -> dict[str, int]:
        raise RuntimeError("simulated prepared candidate crash")

    monkeypatch.setattr(migration, "_activate_candidate", stop_after_prepare)
    with pytest.raises(RuntimeError, match="prepared candidate crash"):
        migrate_internal_audio(layout, release_metadata=release)
    candidate = next(layout.collector.root.glob(".collector-state.sqlite3.migrating.*"))
    with closing(sqlite3.connect(candidate)) as connection:
        row = cast(
            str,
            connection.execute(
                "SELECT payload FROM state WHERE kind='migration' AND id='internal_audio_cutover'"
            ).fetchone()[0],
        )
        receipt = cast(dict[str, object], json.loads(row))
        connection.execute(
            "UPDATE state SET payload=? WHERE kind='migration' AND id='internal_audio_cutover'",
            (json.dumps({**receipt, "phase": "BUILDING"}),),
        )
        connection.commit()
    child = subprocess.run(
        (
            sys.executable,
            "-c",
            """
import os
import sqlite3
import sys
connection = sqlite3.connect(sys.argv[1], isolation_level=None)
connection.execute("BEGIN IMMEDIATE")
connection.execute("UPDATE state SET payload='hot' WHERE kind='runtime_status' AND id='current'")
os._exit(0)
""",
            str(candidate),
        ),
        check=True,
        timeout=10,
    )
    assert child.returncode == 0
    assert (candidate.parent / f"{candidate.name}-journal").is_file()

    monkeypatch.setattr(migration, "_activate_candidate", activate)
    assert migrate_internal_audio(layout, release_metadata=release) == {"imported_sources": 1, "removed_files": 2}
    assert not draft.exists()
    assert not candidate.exists()
    assert not (candidate.parent / f"{candidate.name}-journal").exists()


def test_cutover_recovers_hot_canonical_v1_journal_before_inventory(tmp_path: Path) -> None:
    layout = _layout(tmp_path)
    release = _release(tmp_path)
    _v1(layout)
    _draft(layout)
    child = subprocess.run(
        (
            sys.executable,
            "-c",
            """
import os
import sqlite3
import sys
connection = sqlite3.connect(sys.argv[1], isolation_level=None)
connection.execute("BEGIN IMMEDIATE")
connection.execute("UPDATE state SET payload='transient' WHERE kind='runtime_status' AND id='current'")
os._exit(0)
""",
            str(layout.collector.state_db),
        ),
        check=True,
        timeout=10,
    )
    assert child.returncode == 0
    journal = layout.collector.root / "collector-state.sqlite3-journal"
    assert journal.is_file()

    assert migrate_internal_audio(layout, release_metadata=release) == {"imported_sources": 1, "removed_files": 2}

    assert not journal.exists()
    with closing(sqlite3.connect(layout.collector.state_db)) as connection:
        retained = cast(str, connection.execute("SELECT payload FROM state WHERE kind='runtime_status'").fetchone()[0])
    assert json.loads(retained) == {"last_error": "тест"}


def test_fresh_cutover_requires_empty_root_and_hands_off_database(tmp_path: Path) -> None:
    layout = _layout(tmp_path)
    release = _release(tmp_path)
    ready = layout.publication.root / "existing-ready"
    ready.mkdir()
    with pytest.raises(InternalAudioMigrationError, match="canonical DB is missing"):
        migrate_internal_audio(layout, release_metadata=release)
    assert not layout.collector.state_db.exists()

    ready.rmdir()
    assert migrate_internal_audio(layout, release_metadata=release) == {"imported_sources": 0, "removed_files": 0}
    database = layout.collector.state_db
    assert database.stat().st_uid == layout.collector.root.stat().st_uid
    assert database.stat().st_gid == layout.collector.root.stat().st_gid
    assert database.stat().st_mode & 0o777 == 0o640
    connection = sqlite3.connect(database)
    assert json.loads(
        cast(
            str,
            connection.execute(
                "SELECT payload FROM state WHERE kind='migration' AND id='internal_audio_cutover'"
            ).fetchone()[0],
        )
    ) == {"format": 1, "phase": "COMPLETE", "fresh": True}
    connection.close()


def test_fresh_complete_reentry_accepts_only_its_sql_owned_ready_stage(tmp_path: Path) -> None:
    layout = _layout(tmp_path)
    release = _release(tmp_path)
    assert migrate_internal_audio(layout, release_metadata=release) == {"imported_sources": 0, "removed_files": 0}

    cohort_id = "a" * 64
    bundle_digest = "b" * 64
    identity = f"{cohort_id}:0:{bundle_digest}"
    stage_id = hashlib.sha256(identity.encode()).hexdigest()
    stage = layout.publication.root / f".{bundle_digest}.{stage_id[:16]}.stage"
    stage.mkdir()
    (stage / "records.bin").write_bytes(b"partial ready output")
    with closing(sqlite3.connect(layout.collector.state_db)) as connection:
        connection.execute(
            "INSERT INTO publication_cohort(cohort_id,lifecycle,cursor) VALUES (?,'staging',0)", (cohort_id,)
        )
        connection.execute(
            "INSERT INTO ready_output_stage("
            "stage_id,group_identity,owned_path,expected_bundle_digest,expected_record_count,expected_slice_count,owner_token"
            ") VALUES (?,?,?,?,0,0,?)",
            (stage_id, identity, str(stage), bundle_digest, stage_id),
        )
        connection.commit()

    assert migrate_internal_audio(layout, release_metadata=release) == {"imported_sources": 0, "removed_files": 0}
    assert (stage / "records.bin").read_bytes() == b"partial ready output"

    foreign = layout.publication.root / ".foreign.stage"
    foreign.mkdir()
    with pytest.raises(InternalAudioMigrationError, match="unowned ready scratch"):
        migrate_internal_audio(layout, release_metadata=release)
    assert foreign.is_dir()
    assert (stage / "records.bin").read_bytes() == b"partial ready output"


def test_fresh_complete_reentry_refuses_legacy_mutable_rows(tmp_path: Path) -> None:
    layout = _layout(tmp_path)
    release = _release(tmp_path)
    migrate_internal_audio(layout, release_metadata=release)
    with closing(sqlite3.connect(layout.collector.state_db)) as connection:
        connection.execute("INSERT INTO state(kind,id,payload) VALUES ('attempt','stale','{}')")
        connection.commit()

    with pytest.raises(InternalAudioMigrationError, match="private legacy audio remains"):
        migrate_internal_audio(layout, release_metadata=release)
    with closing(sqlite3.connect(layout.collector.state_db)) as connection:
        assert connection.execute("SELECT 1 FROM state WHERE kind='attempt' AND id='stale'").fetchone() == (1,)


@pytest.mark.parametrize("wrong_ddl", [False, True])
def test_complete_reentry_refuses_wrong_typed_index(tmp_path: Path, wrong_ddl: bool) -> None:
    layout = _layout(tmp_path)
    release = _release(tmp_path)
    migrate_internal_audio(layout, release_metadata=release)
    with closing(sqlite3.connect(layout.collector.state_db)) as connection:
        connection.execute("DROP INDEX capture_record_mapping_revision_idx")
        if wrong_ddl:
            connection.execute(
                "CREATE INDEX capture_record_mapping_revision_idx ON capture_record (source_id, ordinal)"
            )
        connection.commit()
    damaged_bytes = layout.collector.state_db.read_bytes()

    with pytest.raises(InternalAudioMigrationError, match="activated database has unsupported typed schema"):
        migrate_internal_audio(layout, release_metadata=release)

    assert layout.collector.state_db.read_bytes() == damaged_bytes


def test_fresh_cutover_resumes_empty_v2_schema_before_receipt(tmp_path: Path) -> None:
    layout = _layout(tmp_path)
    release = _release(tmp_path)
    StateDB(layout.collector.state_db)

    assert migrate_internal_audio(layout, release_metadata=release) == {"imported_sources": 0, "removed_files": 0}
    with closing(sqlite3.connect(layout.collector.state_db)) as connection:
        receipt = cast(
            dict[str, object],
            json.loads(
                cast(
                    str,
                    connection.execute(
                        "SELECT payload FROM state WHERE kind='migration' AND id='internal_audio_cutover'"
                    ).fetchone()[0],
                )
            ),
        )
    assert receipt == {"format": 1, "fresh": True, "phase": "COMPLETE"}


def test_fresh_handoff_failure_never_writes_complete_and_retry_repairs(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    layout = _layout(tmp_path)
    release = _release(tmp_path)

    def deny(_path: Path) -> None:
        raise InternalAudioMigrationError("service access denied")

    monkeypatch.setattr(migration, "_verify_handoff_access", deny)
    with pytest.raises(InternalAudioMigrationError, match="service access denied"):
        migrate_internal_audio(layout, release_metadata=release)
    with closing(sqlite3.connect(layout.collector.state_db)) as connection:
        assert connection.execute("SELECT 1 FROM state WHERE id='internal_audio_cutover'").fetchone() is None

    monkeypatch.setattr(migration, "_verify_handoff_access", lambda _path: None)
    assert migrate_internal_audio(layout, release_metadata=release) == {"imported_sources": 0, "removed_files": 0}


def test_fresh_cutover_refuses_nonempty_v2_without_receipt(tmp_path: Path) -> None:
    layout = _layout(tmp_path)
    release = _release(tmp_path)
    StateDB(layout.collector.state_db).put(StateKind.RUNTIME_STATUS, "current", {"last_error": "retained"})

    with pytest.raises(InternalAudioMigrationError, match="contains state"):
        migrate_internal_audio(layout, release_metadata=release)
    with closing(sqlite3.connect(layout.collector.state_db)) as connection:
        assert (
            connection.execute("SELECT 1 FROM state WHERE kind='migration' AND id='internal_audio_cutover'").fetchone()
            is None
        )


def test_cutover_imports_v1_attempt_rows_and_complete_raw_tail(tmp_path: Path) -> None:
    layout = _layout(tmp_path)
    release = _release(tmp_path)
    _v1(layout)
    attempt_id = "a" * 32
    attempt_path = layout.collector.attempts / attempt_id
    attempt_path.mkdir(parents=True)
    complete = b"".join(index.to_bytes(4, "big") + bytes([index]) * 440 for index in range(3))
    raw = complete + b"partial"
    descriptor = {
        "attempt_id": attempt_id,
        "schema_version": 2,
        "start_sequence": 100,
        "packet_count": 4,
        "record_size": 444,
        "read_begin_start": 100,
        "read_begin_count": 4,
    }
    checkpoint = {
        "version": 1,
        "attempt_id": attempt_id,
        "record_count": 2,
        "raw_sha256": hashlib.sha256(complete[:888]).hexdigest(),
    }
    (attempt_path / "attempt.json").write_text(json.dumps(descriptor), encoding="utf-8")
    (attempt_path / "checkpoint.json").write_text(json.dumps(checkpoint), encoding="utf-8")
    (attempt_path / "records.bin").write_bytes(raw)
    with closing(sqlite3.connect(layout.collector.state_db)) as connection:
        connection.execute(
            "INSERT INTO state VALUES ('attempt',?,?)",
            (
                attempt_id,
                json.dumps(
                    {
                        "descriptor": descriptor,
                        "checkpoint": checkpoint,
                        "prefix_publication": None,
                        "terminal_retirement": None,
                    }
                ),
            ),
        )
        connection.commit()

    result = migrate_internal_audio(layout, release_metadata=release)

    assert result == {"imported_sources": 1, "removed_files": 3}
    assert not attempt_path.exists()
    with closing(sqlite3.connect(layout.collector.state_db)) as connection:
        assert connection.execute("SELECT 1 FROM state WHERE kind='attempt'").fetchone() is None
        source = cast(
            tuple[str, str, int, int] | None,
            connection.execute(
                "SELECT source_type,lifecycle,committed_count,certified_prefix_count FROM capture_source"
            ).fetchone(),
        )
        assert source == ("attempt", "interrupted", 3, 2)
        tail = cast(tuple[bytes] | None, connection.execute("SELECT bytes FROM capture_tail").fetchone())
        assert tail == (b"partial",)


@pytest.mark.parametrize("raw", (b"\x00\x00\x00\x01" + b"x" * 440, b"partial"))
def test_cutover_refuses_v1_private_bytes_without_read_begin_before_receipt(tmp_path: Path, raw: bytes) -> None:
    layout = _layout(tmp_path)
    release = _release(tmp_path)
    _v1(layout)
    attempt_id = "b" * 32
    attempt_path = layout.collector.attempts / attempt_id
    attempt_path.mkdir(parents=True)
    descriptor = {
        "attempt_id": attempt_id,
        "schema_version": 2,
        "start_sequence": 100,
        "packet_count": 1,
        "record_size": 444,
        "read_begin_start": None,
        "read_begin_count": None,
    }
    checkpoint = {
        "version": 1,
        "attempt_id": attempt_id,
        "record_count": 0,
        "raw_sha256": hashlib.sha256(b"").hexdigest(),
    }
    (attempt_path / "attempt.json").write_text(json.dumps(descriptor), encoding="utf-8")
    (attempt_path / "checkpoint.json").write_text(json.dumps(checkpoint), encoding="utf-8")
    (attempt_path / "records.bin").write_bytes(raw)
    with closing(sqlite3.connect(layout.collector.state_db)) as connection:
        connection.execute(
            "INSERT INTO state VALUES ('attempt',?,?)",
            (
                attempt_id,
                json.dumps(
                    {
                        "descriptor": descriptor,
                        "checkpoint": checkpoint,
                        "prefix_publication": None,
                        "terminal_retirement": None,
                    }
                ),
            ),
        )
        connection.commit()
    source_bytes = layout.collector.state_db.read_bytes()
    private_bytes = {path.name: path.read_bytes() for path in attempt_path.iterdir()}

    with pytest.raises(LegacyAudioImportError, match="without READ_BEGIN"):
        migrate_internal_audio(layout, release_metadata=release)

    assert layout.collector.state_db.read_bytes() == source_bytes
    assert {path.name: path.read_bytes() for path in attempt_path.iterdir()} == private_bytes
    assert not tuple(layout.collector.root.glob(".collector-state.sqlite3.migrating.*"))
    with closing(sqlite3.connect(layout.collector.state_db)) as connection:
        assert connection.execute("SELECT 1 FROM state WHERE id='internal_audio_cutover'").fetchone() is None


@pytest.mark.parametrize("missing_directory", [False, True])
def test_cutover_refuses_unresolved_v1_attempt_before_receipt(tmp_path: Path, missing_directory: bool) -> None:
    layout = _layout(tmp_path)
    release = _release(tmp_path)
    _v1(layout)
    attempt_id = "c" * 32
    attempt_path = layout.collector.attempts / attempt_id
    raw = (123).to_bytes(4, "big") + b"a" * 440
    descriptor = {
        "attempt_id": attempt_id,
        "schema_version": 2,
        "start_sequence": 10,
        "packet_count": 1,
        "record_size": 444,
        "read_begin_start": 10,
        "read_begin_count": 1,
    }
    checkpoint = {
        "version": 1,
        "attempt_id": attempt_id,
        "record_count": 1,
        "raw_sha256": hashlib.sha256(raw).hexdigest(),
    }
    prefix = {"version": 1, "state": "published"}
    terminal = {"version": 1, "state": "terminal-retired", "terminalized_at_unix_ns": 1}
    row = {
        "descriptor": descriptor,
        "checkpoint": checkpoint,
        "prefix_publication": prefix,
        "terminal_retirement": terminal,
    }
    if not missing_directory:
        attempt_path.mkdir(parents=True)
        (attempt_path / "attempt.json").write_text(json.dumps(descriptor), encoding="utf-8")
        (attempt_path / "checkpoint.json").write_text(json.dumps(checkpoint), encoding="utf-8")
        (attempt_path / "prefix-publication.json").write_text(json.dumps(prefix), encoding="utf-8")
        (attempt_path / "terminal-retired.json").write_text(json.dumps(terminal), encoding="utf-8")
        (attempt_path / "records.bin").write_bytes(raw)
    with closing(sqlite3.connect(layout.collector.state_db)) as connection:
        connection.execute("INSERT INTO state VALUES ('attempt',?,?)", (attempt_id, json.dumps(row)))
        connection.commit()

    message = "lacks its private directory" if missing_directory else "terminal-retired attempt"
    with pytest.raises(InternalAudioMigrationError, match=message):
        migrate_internal_audio(layout, release_metadata=release)
    with closing(sqlite3.connect(layout.collector.state_db)) as connection:
        assert connection.execute("PRAGMA user_version").fetchone() == (1,)
        assert connection.execute("SELECT 1 FROM state WHERE id='internal_audio_cutover'").fetchone() is None
    assert attempt_path.exists() is not missing_directory


def _quarantine_row(path: Path, *, opaque: bool, attempt_id: str | None = None) -> dict[str, object]:
    info = path.lstat()
    kind = "symlink" if path.is_symlink() else "directory" if path.is_dir() else "file"
    return {
        "physical_name": path.name,
        "source_name": path.name,
        "source_root": "attempts",
        "original_name": path.name,
        "attempt_id": attempt_id,
        "reason": "original cause",
        "opaque": opaque,
        "entry_kind": kind,
        "move_committed": True,
        "source_device": info.st_dev,
        "source_inode": info.st_ino,
        "deletion_pending": False,
        "terminal_state": "unprocessable",
        "terminal_at_unix_ns": 1,
        "terminal_reason": "classified cause",
    }


def _put_quarantine(layout: StorageLayout, path: Path, row: dict[str, object]) -> None:
    with closing(sqlite3.connect(layout.collector.state_db)) as connection:
        connection.execute("INSERT INTO state VALUES ('quarantine',?,?)", (path.name, json.dumps(row)))
        connection.commit()


def test_cutover_preserves_opaque_symlink_reasons_and_retention(tmp_path: Path) -> None:
    layout = _layout(tmp_path)
    release = _release(tmp_path)
    _v1(layout)
    layout.collector.quarantine.mkdir()
    entry = layout.collector.quarantine / "opaque-link"
    entry.symlink_to("missing-target")
    _put_quarantine(layout, entry, _quarantine_row(entry, opaque=True))

    assert migrate_internal_audio(layout, release_metadata=release) == {"imported_sources": 0, "removed_files": 1}
    assert not entry.is_symlink()
    with closing(sqlite3.connect(layout.collector.state_db)) as connection:
        row = cast(
            tuple[object, ...] | None,
            connection.execute(
                "SELECT entry_kind,symlink_text,disposition,reason,terminal_reason,terminal_time,retention_deadline "
                "FROM capture_evidence"
            ).fetchone(),
        )
    assert row == (
        "symlink",
        b"missing-target",
        "unprocessable",
        "original cause",
        "classified cause",
        "1",
        str(1 + 72 * 60 * 60 * 1_000_000_000),
    )


def test_cutover_preserves_opaque_empty_directory_as_typed_evidence(tmp_path: Path) -> None:
    layout = _layout(tmp_path)
    release = _release(tmp_path)
    _v1(layout)
    layout.collector.quarantine.mkdir()
    entry = layout.collector.quarantine / "empty-opaque"
    entry.mkdir()
    _put_quarantine(layout, entry, _quarantine_row(entry, opaque=True))

    assert migrate_internal_audio(layout, release_metadata=release) == {"imported_sources": 0, "removed_files": 0}
    assert not entry.exists()
    with closing(sqlite3.connect(layout.collector.state_db)) as connection:
        row = cast(
            tuple[object, ...] | None,
            connection.execute("SELECT entry_kind,size,sha256,symlink_text FROM capture_evidence").fetchone(),
        )
    assert row == ("directory", 0, None, None)


def test_cutover_refuses_attributable_quarantine_before_receipt(tmp_path: Path) -> None:
    layout = _layout(tmp_path)
    release = _release(tmp_path)
    _v1(layout)
    layout.collector.quarantine.mkdir()
    attempt_id = "b" * 32
    entry = layout.collector.quarantine / f"{attempt_id}-quarantined"
    entry.mkdir()
    raw = (1234).to_bytes(4, "big") + b"a" * 440
    descriptor = {
        "attempt_id": attempt_id,
        "schema_version": 2,
        "start_sequence": 10,
        "packet_count": 2,
        "record_size": 444,
        "read_begin_start": 10,
        "read_begin_count": 2,
    }
    checkpoint = {
        "version": 1,
        "attempt_id": attempt_id,
        "record_count": 1,
        "raw_sha256": hashlib.sha256(raw).hexdigest(),
    }
    (entry / "attempt.json").write_text(json.dumps(descriptor), encoding="utf-8")
    (entry / "checkpoint.json").write_text(json.dumps(checkpoint), encoding="utf-8")
    (entry / "records.bin").write_bytes(raw)
    row = _quarantine_row(entry, opaque=False, attempt_id=attempt_id)
    _put_quarantine(layout, entry, row)

    with pytest.raises(InternalAudioMigrationError, match="manual custody resolution"):
        migrate_internal_audio(layout, release_metadata=release)
    assert entry.exists()
    with closing(sqlite3.connect(layout.collector.state_db)) as connection:
        assert connection.execute("PRAGMA user_version").fetchone() == (1,)
        assert connection.execute("SELECT 1 FROM state WHERE kind='quarantine'").fetchone() == (1,)
        assert connection.execute("SELECT 1 FROM state WHERE id='internal_audio_cutover'").fetchone() is None


def test_cutover_refuses_spool_rooted_quarantine_before_receipt(tmp_path: Path) -> None:
    layout = _layout(tmp_path)
    release = _release(tmp_path)
    _v1(layout)
    entry = layout.collector.root / "opaque-spool-entry"
    entry.write_bytes(b"retained private evidence")
    original_stat = entry.lstat()
    row = _quarantine_row(entry, opaque=True)
    row["source_root"] = "spool"
    row["move_committed"] = False
    _put_quarantine(layout, entry, row)

    with pytest.raises(InternalAudioMigrationError, match="spool-rooted v1 quarantine"):
        migrate_internal_audio(layout, release_metadata=release)

    assert entry.read_bytes() == b"retained private evidence"
    assert entry.lstat().st_ino == original_stat.st_ino
    assert not tuple(layout.collector.root.glob(".collector-state.sqlite3.migrating.*"))
    with closing(sqlite3.connect(layout.collector.state_db)) as connection:
        assert connection.execute("PRAGMA user_version").fetchone() == (1,)
        assert connection.execute("SELECT 1 FROM state WHERE kind='quarantine'").fetchone() == (1,)
        assert connection.execute("SELECT 1 FROM state WHERE id='internal_audio_cutover'").fetchone() is None


def test_cleanup_resumes_only_the_original_private_inventory(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    layout = _layout(tmp_path)
    release = _release(tmp_path)
    _v1(layout)
    draft, _ = _draft(layout)
    cleanup = migration._cleanup_legacy

    def interrupt_after_one_file(storage: StorageLayout, expected: tuple[migration._FileIdentity, ...]) -> int:
        first = next(item for item in expected if item.kind == "regular")
        path = storage.root / first.path
        path.unlink()
        migration._fsync_directory(path.parent)
        raise RuntimeError("simulated interrupted cleanup")

    monkeypatch.setattr(migration, "_cleanup_legacy", interrupt_after_one_file)
    with pytest.raises(RuntimeError, match="interrupted cleanup"):
        migrate_internal_audio(layout, release_metadata=release)
    assert layout.collector.state_db.exists()
    assert draft.exists()

    monkeypatch.setattr(migration, "_cleanup_legacy", cleanup)
    assert migrate_internal_audio(layout, release_metadata=release) == {"imported_sources": 1, "removed_files": 1}
    assert not draft.exists()


def test_cleanup_access_failure_keeps_forward_receipt_until_retry(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    layout = _layout(tmp_path)
    release = _release(tmp_path)
    _v1(layout)
    draft, _ = _draft(layout)

    def deny_access(_path: Path) -> None:
        raise InternalAudioMigrationError("service cannot open migrated SQLite")

    monkeypatch.setattr(migration, "_verify_handoff_access", deny_access)
    with pytest.raises(InternalAudioMigrationError, match="service cannot open"):
        migrate_internal_audio(layout, release_metadata=release)
    assert not draft.exists()
    with closing(sqlite3.connect(layout.collector.state_db)) as connection:
        assert connection.execute("PRAGMA user_version").fetchone() == (2,)
        receipt = cast(
            dict[str, object],
            json.loads(
                cast(
                    str,
                    connection.execute(
                        "SELECT payload FROM state WHERE kind='migration' AND id='internal_audio_cutover'"
                    ).fetchone()[0],
                )
            ),
        )
    assert receipt["phase"] == "CLEANUP_PENDING"

    monkeypatch.setattr(migration, "_verify_handoff_access", lambda _path: None)
    assert migrate_internal_audio(layout, release_metadata=release) == {"imported_sources": 1, "removed_files": 0}
    with closing(sqlite3.connect(layout.collector.state_db)) as connection:
        receipt = cast(
            dict[str, object],
            json.loads(
                cast(
                    str,
                    connection.execute(
                        "SELECT payload FROM state WHERE kind='migration' AND id='internal_audio_cutover'"
                    ).fetchone()[0],
                )
            ),
        )
    assert receipt["phase"] == "COMPLETE"


def test_prepared_cutover_refuses_missing_file_before_cleanup_intent(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    layout = _layout(tmp_path)
    release = _release(tmp_path)
    _v1(layout)
    draft, _ = _draft(layout)
    put_receipt = migration._put_receipt

    def stop_before_cleanup_intent(connection: sqlite3.Connection, receipt: dict[str, object]) -> None:
        if receipt.get("phase") == "CLEANUP_PENDING":
            raise RuntimeError("simulated pre-cleanup crash")
        put_receipt(connection, receipt)

    monkeypatch.setattr(migration, "_put_receipt", stop_before_cleanup_intent)
    with pytest.raises(RuntimeError, match="pre-cleanup crash"):
        migrate_internal_audio(layout, release_metadata=release)
    (draft / "records.bin").unlink()
    monkeypatch.setattr(migration, "_put_receipt", put_receipt)

    with pytest.raises(InternalAudioMigrationError, match="prepared cutover lost private audio"):
        migrate_internal_audio(layout, release_metadata=release)
    assert draft.exists()
