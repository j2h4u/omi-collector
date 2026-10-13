"""Runtime accepts only a completed typed Collector database."""

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
    _validate_v2,
    open_runtime_state,
)
from omi_collector.storage_layout import StorageLayout, load_operator_config


def _layout(tmp_path: Path) -> StorageLayout:
    config = tmp_path / "config.toml"
    config.write_text(
        '[pendant]\naddress = "AA:BB:CC:DD:EE:FF"\n[ready]\ntarget_audio_seconds = 120\n', encoding="utf-8"
    )
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


def test_v2_receipt_must_be_complete_and_private_audio_gone(tmp_path: Path) -> None:
    layout = _layout(tmp_path)
    db = StateDB(layout.collector.state_db)
    with pytest.raises(InternalAudioCutoverRequiredError, match="missing"):
        open_runtime_state(layout)
    db.put(StateKind.MIGRATION, "internal_audio_cutover", {"format": 1, "phase": "CLEANUP_PENDING"})
    with pytest.raises(InternalAudioCutoverRequiredError, match="incomplete"):
        open_runtime_state(layout)
    db.put(StateKind.MIGRATION, "internal_audio_cutover", {"format": 1, "phase": "COMPLETE", "fresh": True})
    assert open_runtime_state(layout, read_only=True).get(StateKind.MIGRATION, "internal_audio_cutover") is not None
    layout.draft.mkdir()
    (layout.draft / "leftover").write_bytes(b"raw")
    with pytest.raises(InternalAudioCutoverRequiredError, match="private audio"):
        open_runtime_state(layout)


def test_v2_rejects_legacy_mutable_rows(tmp_path: Path) -> None:
    layout = _layout(tmp_path)
    db = StateDB(layout.collector.state_db)
    db.put(StateKind.MIGRATION, "internal_audio_cutover", {"format": 1, "phase": "COMPLETE", "fresh": True})
    db.put(StateKind.ATTEMPT, "a" * 32, {"descriptor": {"attempt_id": "a" * 32}})
    with pytest.raises(InternalAudioCutoverRequiredError, match="v1 capture rows"):
        open_runtime_state(layout)


def test_runtime_receipt_validation_never_scans_retained_audio(tmp_path: Path) -> None:
    layout = _layout(tmp_path)
    db = StateDB(layout.collector.state_db)
    db.put(StateKind.MIGRATION, "internal_audio_cutover", {"format": 1, "phase": "COMPLETE", "fresh": True})
    statements: list[str] = []
    with closing(sqlite3.connect(db.path)) as connection:
        connection.set_trace_callback(statements.append)
        _validate_v2(connection)
    assert statements
    assert all("quick_check" not in statement.lower() for statement in statements)
    assert all("capture_record" not in statement.lower() for statement in statements)


def test_v2_runtime_accepts_only_sql_owned_partial_ready_stage(tmp_path: Path) -> None:
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


def test_v2_runtime_rejects_unowned_stage_lookalike(tmp_path: Path) -> None:
    layout = _layout(tmp_path)
    db = StateDB(layout.collector.state_db)
    db.put(StateKind.MIGRATION, "internal_audio_cutover", {"format": 1, "phase": "COMPLETE", "fresh": True})
    stage = layout.publication.root / f".{('a' * 64)}.{('b' * 16)}.stage"
    stage.mkdir(parents=True)
    with pytest.raises(InternalAudioCutoverRequiredError, match="unowned ready scratch"):
        open_runtime_state(layout)


def test_v2_rejects_complete_label_with_unverified_inventory(tmp_path: Path) -> None:
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
    with pytest.raises(InternalAudioCutoverRequiredError, match="incomplete"):
        open_runtime_state(layout)


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
