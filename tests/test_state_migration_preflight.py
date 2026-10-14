from __future__ import annotations

import hashlib
import json
import sqlite3
from contextlib import closing
from pathlib import Path
from typing import cast

import pytest
from typer.testing import CliRunner

import omi_collector.cli as cli
import omi_collector.internal_audio_migration as migration
from omi_collector.capture.adapters.collector_state_db import raise_if_sqlite_storage_fault
from omi_collector.capture.application.storage_fault import (
    FatalInternalAudioStorageError,
    TransientInternalAudioStorageError,
)
from omi_collector.cli import app
from omi_collector.internal_audio_migration import (
    InternalAudioMigrationError,
    inspect_internal_audio_migration,
    migrate_internal_audio,
)
from omi_collector.storage_layout import StorageLayout, load_operator_config


@pytest.fixture(autouse=True)
def _skip_account_handoff(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(migration, "_verify_handoff_access", lambda _path: None)


def _layout(tmp_path: Path) -> StorageLayout:
    tmp_path.mkdir(parents=True, exist_ok=True)
    config = tmp_path / "config.toml"
    config.write_text(
        '[pendant]\naddress = "AA:BB:CC:DD:EE:FF"\n[ready]\ntarget_audio_seconds = 120\n', encoding="utf-8"
    )
    layout = load_operator_config(config).storage
    layout.collector.root.mkdir()
    layout.draft.mkdir()
    layout.publication.root.mkdir()
    return layout


def _release(tmp_path: Path) -> Path:
    path = tmp_path / "release.json"
    path.write_text(json.dumps({"source_revision": "a" * 40}), encoding="utf-8")
    return path


def _v1(layout: StorageLayout) -> None:
    with closing(sqlite3.connect(layout.collector.state_db)) as connection:
        connection.execute(
            "CREATE TABLE state (kind TEXT NOT NULL, id TEXT NOT NULL, payload TEXT NOT NULL, "
            "PRIMARY KEY(kind,id)) WITHOUT ROWID"
        )
        connection.execute("PRAGMA user_version = 1")
        connection.execute(
            "INSERT INTO state VALUES ('migration','current',?)",
            (json.dumps({"phase": "complete", "version": 1}),),
        )
        connection.commit()


def _draft(layout: StorageLayout) -> Path:
    raw = (1).to_bytes(4, "big") + b"x" * 440
    digest = hashlib.sha256(raw).hexdigest()
    path = layout.draft / f"700-701-{digest[:16]}"
    path.mkdir()
    (path / "records.bin").write_bytes(raw)
    (path / "manifest.json").write_text(
        json.dumps(
            {
                "schema_version": 2,
                "start_sequence": 700,
                "next_sequence": 701,
                "record_count": 1,
                "record_size": 444,
                "raw_sha256": digest,
            },
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    return path


def _snapshot(root: Path) -> dict[str, tuple[int, bytes]]:
    return {
        path.relative_to(root).as_posix(): (path.stat().st_mode, path.read_bytes())
        for path in sorted(root.rglob("*"))
        if path.is_file()
    }


def _assert_compatible(result: dict[str, object], schema_version: int | None) -> None:
    assert result == {"schema_version": schema_version, "status": "migration_compatible"}


@pytest.mark.parametrize(
    ("sqlite_errorcode", "error_type"),
    [
        (sqlite3.SQLITE_CORRUPT, FatalInternalAudioStorageError),
        (sqlite3.SQLITE_NOTADB, FatalInternalAudioStorageError),
        (sqlite3.SQLITE_BUSY, TransientInternalAudioStorageError),
        (sqlite3.SQLITE_LOCKED, TransientInternalAudioStorageError),
    ],
)
def test_state_db_classifies_direct_sqlite_fault_codes(sqlite_errorcode: int, error_type: type[Exception]) -> None:
    error = sqlite3.DatabaseError("sentinel")
    error.sqlite_errorcode = sqlite_errorcode

    with pytest.raises(error_type) as raised:
        raise_if_sqlite_storage_fault(error)

    assert raised.value.sqlite_errorcode == sqlite_errorcode  # type: ignore[attr-defined]


def test_actual_notadb_startup_exits_78_before_ble_composition(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    layout = _layout(tmp_path)
    config_path = tmp_path / "config.toml"
    layout.collector.state_db.write_bytes(b"not a SQLite database")

    monkeypatch.setattr(cli, "DEFAULT_CONFIG_PATH", tmp_path / "absent-installed-config.toml")
    monkeypatch.setattr(cli, "_capture_cli", lambda: pytest.fail("BLE composition must not start"))

    result = CliRunner().invoke(
        app,
        ["device", "sync", "--config", str(config_path), "--confirm-sync"],
    )

    assert result.exit_code == 78, result.output
    assert "database" in result.output.lower()


def test_preflight_accepts_empty_and_supported_v1_without_mutation(tmp_path: Path) -> None:
    release = _release(tmp_path)
    empty = _layout(tmp_path / "empty")

    _assert_compatible(inspect_internal_audio_migration(empty, release_metadata=release), None)
    assert not empty.collector.state_db.exists()

    root = tmp_path / "v1"
    root.mkdir()
    layout = _layout(root)
    _v1(layout)
    before = _snapshot(layout.root)

    _assert_compatible(inspect_internal_audio_migration(layout, release_metadata=release), 1)

    assert _snapshot(layout.root) == before


@pytest.mark.parametrize("artifact", ["unattributable_quarantine", "retired_attempt", "foreign_scratch"])
def test_preflight_refuses_unsupported_v1_artifacts_without_mutation(tmp_path: Path, artifact: str) -> None:
    layout = _layout(tmp_path)
    release = _release(tmp_path)
    _v1(layout)
    if artifact == "unattributable_quarantine":
        layout.collector.quarantine.mkdir()
        orphan = layout.collector.quarantine / "unattributed-entry"
        orphan.write_bytes(b"retained evidence")
    elif artifact == "retired_attempt":
        attempt_id = "c" * 32
        attempt = layout.collector.attempts / attempt_id
        attempt.mkdir(parents=True)
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
        (attempt / "attempt.json").write_text(json.dumps(descriptor), encoding="utf-8")
        (attempt / "checkpoint.json").write_text(json.dumps(checkpoint), encoding="utf-8")
        (attempt / "prefix-publication.json").write_text(json.dumps(prefix), encoding="utf-8")
        (attempt / "terminal-retired.json").write_text(
            json.dumps(terminal),
            encoding="utf-8",
        )
        (attempt / "records.bin").write_bytes(raw)
        with closing(sqlite3.connect(layout.collector.state_db)) as connection:
            connection.execute(
                "INSERT INTO state VALUES ('attempt',?,?)",
                (
                    attempt_id,
                    json.dumps(
                        {
                            "descriptor": descriptor,
                            "checkpoint": checkpoint,
                            "prefix_publication": prefix,
                            "terminal_retirement": terminal,
                        }
                    ),
                ),
            )
            connection.commit()
    else:
        (layout.collector.root / ".collector-state.sqlite3.migrating.foreign").write_bytes(b"foreign scratch")
    before = _snapshot(layout.root)

    result = inspect_internal_audio_migration(layout, release_metadata=release)

    assert result["status"] == "migration_refused"
    assert result["reason"] == "state_validation_failed"
    assert _snapshot(layout.root) == before


def test_preflight_accepts_inventoried_opaque_quarantine_without_mutation(tmp_path: Path) -> None:
    layout = _layout(tmp_path)
    release = _release(tmp_path)
    _v1(layout)
    layout.collector.attempts.mkdir()
    entry = layout.collector.attempts / "opaque-entry"
    entry.write_bytes(b"retained evidence")
    info = entry.lstat()
    row = {
        "physical_name": entry.name,
        "source_name": entry.name,
        "source_root": "attempts",
        "original_name": entry.name,
        "attempt_id": None,
        "reason": "original cause",
        "opaque": True,
        "entry_kind": "file",
        "move_committed": False,
        "source_device": info.st_dev,
        "source_inode": info.st_ino,
        "deletion_pending": False,
        "terminal_state": "unprocessable",
        "terminal_at_unix_ns": 1,
        "terminal_reason": "classified cause",
    }
    with closing(sqlite3.connect(layout.collector.state_db)) as connection:
        connection.execute("INSERT INTO state VALUES ('quarantine',?,?)", (entry.name, json.dumps(row)))
        connection.commit()
    before = _snapshot(layout.root)

    _assert_compatible(inspect_internal_audio_migration(layout, release_metadata=release), 1)

    assert _snapshot(layout.root) == before


def test_preflight_accepts_complete_and_resumable_v2_without_mutation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    release = _release(tmp_path)
    complete_root = tmp_path / "complete"
    complete_root.mkdir()
    complete_layout = _layout(complete_root)
    _v1(complete_layout)
    migrate_internal_audio(complete_layout, release_metadata=release)
    before = _snapshot(complete_layout.root)

    _assert_compatible(inspect_internal_audio_migration(complete_layout, release_metadata=release), 2)

    assert _snapshot(complete_layout.root) == before

    pending_root = tmp_path / "pending"
    pending_root.mkdir()
    pending_layout = _layout(pending_root)
    _v1(pending_layout)
    draft = _draft(pending_layout)

    def interrupt_cleanup(_layout: StorageLayout, _expected: tuple[migration._FileIdentity, ...]) -> int:
        raise InternalAudioMigrationError("simulated stop after CLEANUP_PENDING")

    monkeypatch.setattr(migration, "_cleanup_legacy", interrupt_cleanup)
    with pytest.raises(InternalAudioMigrationError, match="simulated stop"):
        migrate_internal_audio(pending_layout, release_metadata=release)
    assert draft.exists()
    before = _snapshot(pending_layout.root)

    _assert_compatible(inspect_internal_audio_migration(pending_layout, release_metadata=release), 2)

    assert _snapshot(pending_layout.root) == before
    with closing(sqlite3.connect(pending_layout.collector.state_db)) as connection:
        payload = cast(
            str,
            connection.execute(
                "SELECT payload FROM state WHERE kind='migration' AND id='internal_audio_cutover'"
            ).fetchone()[0],
        )
    assert json.loads(payload)["phase"] == "CLEANUP_PENDING"


def test_preflight_accepts_owned_building_candidate_without_requiring_preparation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    layout = _layout(tmp_path)
    release = _release(tmp_path)
    _v1(layout)
    _draft(layout)

    def leave_incomplete_candidate(
        _source: sqlite3.Connection,
        candidate: Path,
        _receipt: dict[str, object],
        _inventory: migration._Inventory,
    ) -> None:
        candidate.write_bytes(b"owned but incomplete")
        candidate.chmod(0o600)
        raise InternalAudioMigrationError("stop after BUILDING receipt")

    monkeypatch.setattr(migration, "_backup_and_convert", leave_incomplete_candidate)
    with pytest.raises(InternalAudioMigrationError, match="stop after BUILDING"):
        migrate_internal_audio(layout, release_metadata=release)

    before = _snapshot(layout.root)
    monkeypatch.setattr(
        migration,
        "_candidate_is_prepared",
        lambda *_args: pytest.fail("read-only preflight must not test mutator readiness"),
    )

    _assert_compatible(inspect_internal_audio_migration(layout, release_metadata=release), 1)

    assert _snapshot(layout.root) == before


def test_check_state_migration_cli_requires_stopped_service_only_when_requested(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = tmp_path / "config.toml"
    config.write_text(
        '[pendant]\naddress = "AA:BB:CC:DD:EE:FF"\n[ready]\ntarget_audio_seconds = 120\n', encoding="utf-8"
    )
    monkeypatch.setattr(cli, "_require_host_singleton", lambda _loaded: None)
    monkeypatch.setattr(cli, "_systemd_service_status", lambda: {"available": True, "active_state": "active"})
    calls: list[object] = []
    compatible = {"schema_version": None, "status": "migration_compatible"}
    monkeypatch.setattr(
        "omi_collector.internal_audio_migration.inspect_internal_audio_migration",
        lambda layout, *, release_metadata: calls.append((layout, release_metadata)) or compatible,
    )

    advisory = CliRunner().invoke(app, ["check-state-migration", "--config", str(config)])
    authoritative = CliRunner().invoke(app, ["check-state-migration", "--config", str(config), "--stopped"])

    assert advisory.exit_code == 0
    assert json.loads(advisory.output) == compatible
    assert authoritative.exit_code == 1
    assert json.loads(authoritative.output) == {
        "detail": "confirm the Collector service is stopped before state inspection",
        "reason": "inspection_failed",
        "status": "migration_refused",
    }
    assert len(calls) == 1


@pytest.mark.parametrize(
    ("failure", "exit_code"),
    (
        (FatalInternalAudioStorageError("canonical audio database is corrupt", sqlite_errorcode=26), 78),
        (TransientInternalAudioStorageError("canonical audio database is busy", sqlite_errorcode=5), 1),
    ),
)
def test_check_state_migration_reports_classified_fault_as_structured_refusal(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, failure: Exception, exit_code: int
) -> None:
    config = tmp_path / "config.toml"
    config.write_text(
        '[pendant]\naddress = "AA:BB:CC:DD:EE:FF"\n[ready]\ntarget_audio_seconds = 120\n', encoding="utf-8"
    )

    def fail_inspection(*_args: object, **_kwargs: object) -> None:
        raise failure

    monkeypatch.setattr(cli, "_require_host_singleton", lambda _loaded: None)
    monkeypatch.setattr(migration, "inspect_internal_audio_migration", fail_inspection)
    result = CliRunner().invoke(app, ["check-state-migration", "--config", str(config)])

    assert result.exit_code == exit_code
    assert isinstance(result.exception, SystemExit)
    assert json.loads(result.output) == {
        "detail": str(failure),
        "reason": "inspection_failed",
        "status": "migration_refused",
    }
