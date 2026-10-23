"""Runtime accepts only a safe typed Collector database."""

from __future__ import annotations

import errno
import os
import sqlite3
from contextlib import closing
from hashlib import sha256
from pathlib import Path

import pytest

from omi_collector import state_migration
from omi_collector.capture.adapters.collector_state_db import StateDB, StateKind
from omi_collector.state_migration import (
    InternalAudioCutoverRequiredError,
    StateMigrationError,
    _validate_current,
    open_runtime_state,
)
from omi_collector.storage_layout import StorageLayout, load_operator_config


def _layout(tmp_path: Path) -> StorageLayout:
    config = tmp_path / "config.toml"
    config.write_text('pendant_address = "AA:BB:CC:DD:EE:FF"\nbundle_min_audio_seconds = 120\n', encoding="utf-8")
    return load_operator_config(config).storage


def test_runtime_never_creates_missing_database(tmp_path: Path) -> None:
    layout = _layout(tmp_path)
    with pytest.raises(StateMigrationError, match="missing"):
        open_runtime_state(layout)
    assert not layout.collector.state_db.exists()


def test_known_v1_requires_stopped_cutover(tmp_path: Path) -> None:
    layout = _layout(tmp_path)
    layout.collector.root.mkdir()
    with closing(sqlite3.connect(layout.collector.state_db)) as connection:
        connection.execute("CREATE TABLE state(kind TEXT, id TEXT, payload TEXT)")
        connection.execute("PRAGMA user_version=1")
        connection.commit()
    with pytest.raises(InternalAudioCutoverRequiredError, match="v1"):
        open_runtime_state(layout)


def test_current_receipt_is_inert_and_private_audio_must_be_gone(tmp_path: Path) -> None:
    layout = _layout(tmp_path)
    db = StateDB(layout.collector.state_db)
    db.put(StateKind.MIGRATION, "internal_audio_cutover", {"format": 1, "phase": "CLEANUP_PENDING"})
    assert open_runtime_state(layout, read_only=True).path == db.path
    db.put(StateKind.MIGRATION, "internal_audio_cutover", {"format": 1, "phase": "COMPLETE", "fresh": True})
    assert open_runtime_state(layout, read_only=True).get(StateKind.MIGRATION, "internal_audio_cutover") is not None
    layout.draft.mkdir()
    (layout.draft / "leftover").write_bytes(b"raw")
    with pytest.raises(InternalAudioCutoverRequiredError, match="private audio"):
        open_runtime_state(layout)


def test_current_rejects_legacy_mutable_rows(tmp_path: Path) -> None:
    layout = _layout(tmp_path)
    db = StateDB(layout.collector.state_db)
    db.put(StateKind.MIGRATION, "internal_audio_cutover", {"format": 1, "phase": "COMPLETE", "fresh": True})
    db.put(StateKind.ATTEMPT, "a" * 32, {"descriptor": {"attempt_id": "a" * 32}})
    with pytest.raises(InternalAudioCutoverRequiredError, match="legacy capture rows"):
        open_runtime_state(layout)


def test_runtime_schema_validation_does_not_scan_retained_audio(tmp_path: Path) -> None:
    layout = _layout(tmp_path)
    db = StateDB(layout.collector.state_db)
    db.put(StateKind.MIGRATION, "internal_audio_cutover", {"format": 1, "phase": "COMPLETE", "fresh": True})
    statements: list[str] = []
    with closing(sqlite3.connect(db.path)) as connection:
        connection.set_trace_callback(statements.append)
        _validate_current(connection)
    assert statements
    assert all("quick_check" not in statement.lower() for statement in statements)
    assert all(
        "capture_record" not in statement.lower() or "sqlite_master" in statement.lower() for statement in statements
    )


@pytest.mark.parametrize(("read_only", "expected_timeout"), [(True, 30), (False, 5)])
def test_canonical_validation_uses_bounded_mode_specific_timeout(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, read_only: bool, expected_timeout: int
) -> None:
    layout = _layout(tmp_path)
    StateDB(layout.collector.state_db)
    timeouts: list[float] = []
    connect = sqlite3.connect

    def track_timeout(database: str, *, uri: bool = False, timeout: float = 5.0) -> sqlite3.Connection:
        timeouts.append(timeout)
        return connect(database, uri=uri, timeout=timeout)

    monkeypatch.setattr(state_migration, "_validate_current", lambda _connection: None)
    monkeypatch.setattr(state_migration, "_assert_private_audio_gone", lambda _layout, _connection: None)
    monkeypatch.setattr(sqlite3, "connect", track_timeout)

    state_migration._validate_canonical(layout, read_only=read_only)

    assert timeouts == [expected_timeout]


def test_current_runtime_accepts_only_sql_owned_partial_ready_stage(tmp_path: Path) -> None:
    layout = _layout(tmp_path)
    db = StateDB(layout.collector.state_db)
    db.put(StateKind.MIGRATION, "internal_audio_cutover", {"format": 1, "phase": "COMPLETE", "fresh": True})
    cohort_id, bundle_id = "a" * 64, "b" * 64
    identity = f"{cohort_id}:0:{bundle_id}"
    stage_id = sha256(identity.encode()).hexdigest()
    stage = layout.publication.root / f".{bundle_id}.{stage_id[:16]}.stage"
    stage.mkdir(parents=True)
    stage.joinpath("records.bin").write_bytes(b"partial")
    with closing(db.open_connection()) as connection:
        connection.execute(
            "INSERT INTO publication_cohort(cohort_id,lifecycle,cursor) VALUES (?,'staging',0)", (cohort_id,)
        )
        connection.execute(
            "INSERT INTO ready_output_stage(stage_id,group_identity,owned_path,expected_bundle_digest,"
            "expected_record_count,expected_slice_count,owner_token) VALUES (?,?,?,?,1,1,?)",
            (stage_id, identity, str(stage), "c" * 64, stage_id),
        )
        connection.commit()
    assert open_runtime_state(layout, read_only=True).path == db.path

    foreign = layout.publication.root / ".legacy-v1-scratch"
    foreign.mkdir()
    with pytest.raises(InternalAudioCutoverRequiredError, match="unowned ready scratch"):
        open_runtime_state(layout)
    foreign.rmdir()
    stage.joinpath("foreign-marker").write_bytes(b"foreign")
    with pytest.raises(InternalAudioCutoverRequiredError, match="unowned ready scratch"):
        open_runtime_state(layout)
    stage.joinpath("foreign-marker").unlink()
    assert open_runtime_state(layout).path == db.path


def test_current_runtime_rejects_unowned_stage_lookalike(tmp_path: Path) -> None:
    layout = _layout(tmp_path)
    db = StateDB(layout.collector.state_db)
    db.put(StateKind.MIGRATION, "internal_audio_cutover", {"format": 1, "phase": "COMPLETE", "fresh": True})
    stage = layout.publication.root / f".{('a' * 64)}.{('b' * 16)}.stage"
    stage.mkdir(parents=True)
    with pytest.raises(InternalAudioCutoverRequiredError, match="unowned ready scratch"):
        open_runtime_state(layout)


def test_current_runtime_accepts_owned_v2_stage_inventory_and_rejects_unsafe_entries(tmp_path: Path) -> None:
    layout = _layout(tmp_path)
    db = StateDB(layout.collector.state_db)
    cohort_id, artifact_id = "a" * 64, "b" * 64
    identity = f"{cohort_id}:0:{artifact_id}"
    stage_id = sha256(identity.encode()).hexdigest()
    stage = layout.publication.root / f".{artifact_id}.{stage_id[:16]}.stage"
    stage.mkdir(parents=True)
    audio = stage / "audio.ogg"
    manifest = stage / "manifest.json"
    audio.write_bytes(b"audio")
    with closing(db.open_connection()) as connection:
        connection.execute(
            "INSERT INTO publication_cohort(cohort_id,lifecycle,cursor) VALUES (?,'staging',0)", (cohort_id,)
        )
        connection.execute(
            "INSERT INTO ready_v2_output_stage(stage_id,group_identity,owned_path,stage_nonce,public_order,"
            "artifact_id,config_sha256,descriptor,descriptor_sha256,expected_audio_sha256,"
            "expected_manifest_sha256,expected_physical_count,expected_packet_count,expected_page_count,"
            "expected_audio_bytes,pending_overflow_size,owner_token) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                stage_id,
                identity,
                str(stage),
                bytes(32),
                bytes(8),
                artifact_id,
                "c" * 64,
                b"descriptor",
                "d" * 64,
                "e" * 64,
                "f" * 64,
                1,
                1,
                1,
                5,
                None,
                stage_id,
            ),
        )
        connection.commit()

    assert open_runtime_state(layout, read_only=True).path == db.path
    manifest.write_bytes(b"manifest")
    assert open_runtime_state(layout, read_only=True).path == db.path

    foreign = stage / "foreign-marker"
    foreign.write_bytes(b"foreign")
    with pytest.raises(InternalAudioCutoverRequiredError, match="unowned ready scratch"):
        open_runtime_state(layout)
    foreign.unlink()

    manifest.unlink()
    manifest.symlink_to(audio)
    with pytest.raises(InternalAudioCutoverRequiredError, match="unowned ready scratch"):
        open_runtime_state(layout)


def test_historical_migration_payload_is_inert(tmp_path: Path) -> None:
    layout = _layout(tmp_path)
    db = StateDB(layout.collector.state_db)
    db.put(
        StateKind.MIGRATION,
        "internal_audio_cutover",
        {
            "format": 1,
            "phase": "COMPLETE",
            "fresh": True,
            "files": ["old-raw-path"],
        },
    )
    assert open_runtime_state(layout, read_only=True).path == db.path


def test_identified_canonical_access_denial_requires_stopped_repair(tmp_path: Path) -> None:
    layout = _layout(tmp_path)
    db = StateDB(layout.collector.state_db)
    db.put(StateKind.MIGRATION, "internal_audio_cutover", {"format": 1, "phase": "COMPLETE", "fresh": True})
    layout.collector.state_db.chmod(0)
    try:
        with pytest.raises(InternalAudioCutoverRequiredError, match="access needs stopped-service repair"):
            open_runtime_state(layout)
    finally:
        layout.collector.state_db.chmod(0o640)


def test_optional_journal_disappearance_does_not_mask_canonical_access(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    database = tmp_path / "collector.sqlite3"
    database.write_bytes(b"database")
    journal = tmp_path / "collector.sqlite3-journal"
    real_open = os.open
    seen: list[Path] = []

    def open_with_disappearing_journal(path: str | os.PathLike[str], flags: int, mode: int = 0o777) -> int:
        if Path(path) == journal:
            seen.append(journal)
            raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), str(journal))
        return real_open(path, flags, mode)

    monkeypatch.setattr(state_migration.os, "open", open_with_disappearing_journal)
    state_migration._check_canonical_access(database, read_only=True)
    assert seen == [journal]


@pytest.mark.parametrize("error_number", [errno.EACCES, errno.EIO])
def test_optional_journal_access_error_is_not_ignored(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, error_number: int
) -> None:
    database = tmp_path / "collector.sqlite3"
    database.write_bytes(b"database")
    journal = tmp_path / "collector.sqlite3-journal"
    real_open = os.open

    def open_with_journal_error(path: str | os.PathLike[str], flags: int, mode: int = 0o777) -> int:
        if Path(path) == journal:
            raise OSError(error_number, os.strerror(error_number), str(journal))
        return real_open(path, flags, mode)

    monkeypatch.setattr(state_migration.os, "open", open_with_journal_error)
    with pytest.raises(OSError) as failure:
        state_migration._check_canonical_access(database, read_only=True)
    assert failure.value.errno == error_number


def test_optional_journal_refuses_symlink_and_fifo(tmp_path: Path) -> None:
    database = tmp_path / "collector.sqlite3"
    database.write_bytes(b"database")
    journal = tmp_path / "collector.sqlite3-journal"
    journal.symlink_to(database)
    with pytest.raises(InternalAudioCutoverRequiredError, match="journal is unsafe"):
        state_migration._check_canonical_access(database, read_only=True)
    journal.unlink()
    os.mkfifo(journal)
    with pytest.raises(InternalAudioCutoverRequiredError, match="journal is unsafe"):
        state_migration._check_canonical_access(database, read_only=True)


def test_unsupported_database_does_not_auto_upgrade(tmp_path: Path) -> None:
    layout = _layout(tmp_path)
    layout.collector.root.mkdir()
    with closing(sqlite3.connect(layout.collector.state_db)) as connection:
        connection.execute("PRAGMA user_version=99")
        connection.execute("CREATE TABLE state(kind TEXT, id TEXT, payload TEXT)")
        connection.commit()
    with pytest.raises(StateMigrationError, match="unsupported"):
        open_runtime_state(layout)
    with closing(sqlite3.connect(layout.collector.state_db)) as connection:
        assert connection.execute("PRAGMA user_version").fetchone() == (99,)
