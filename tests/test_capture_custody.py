from __future__ import annotations

import sqlite3
from contextlib import closing
from pathlib import Path
from typing import cast

import pytest

from omi_collector.capture.adapters.collector_state_db import StateDB
from omi_collector.capture.adapters.internal_audio_store import (
    AUDIO_RECORD_SIZE,
    DrainCheckpoint,
    InternalAudioStore,
    InternalAudioStoreError,
)
from omi_collector.capture.application.storage_fault import FatalInternalAudioStorageError


@pytest.mark.parametrize("rejected", [b"", b"x" * (AUDIO_RECORD_SIZE + 1)])
def test_rejected_chunk_bounds_are_checked_before_write(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, rejected: bytes
) -> None:
    db = StateDB(tmp_path / "state.sqlite3")
    with InternalAudioStore(db, chunk_limit=1) as store:
        source = store.create_source("a" * 32, "attempt", 0, 1)
        monkeypatch.setattr(store, "_start_write", lambda _connection: pytest.fail("write was started"))
        with pytest.raises(InternalAudioStoreError, match="rejected evidence is outside the bounded chunk"):
            store._block_replay_conflict(source, 0, rejected)


def test_exhausted_conflict_ordinal_leaves_source_and_evidence_unchanged(tmp_path: Path) -> None:
    db = StateDB(tmp_path / "state.sqlite3")
    with InternalAudioStore(db) as store:
        source_id = "a" * 32
        store.create_source(source_id, "attempt", 0, 1)
        store.prepare_leg(source_id, 0, 1)
        source = store.record_read_begin(source_id, 0, 1)
        with closing(db.open_connection()) as connection:
            connection.execute(
                "INSERT INTO capture_conflict(source_id, chunk_ordinal, leg_generation, absolute_byte_offset, "
                "reason, bytes) VALUES (?, ?, ?, ?, ?, ?)",
                (source_id, 2**32 - 1, 1, 0, "replay_mismatch", b"earlier"),
            )
            connection.commit()
        with pytest.raises(InternalAudioStoreError, match="conflict evidence ordinal is exhausted"):
            store._block_replay_conflict(source, 0, b"new")
        assert store.get_source(source_id) == source
        with closing(db.open_connection(read_only=True)) as connection:
            connection.row_factory = None
            rows = cast(
                list[tuple[int, bytes]],
                connection.execute("SELECT chunk_ordinal, bytes FROM capture_conflict").fetchall(),
            )
            assert rows == [(2**32 - 1, b"earlier")]


def test_failure_after_conflict_insert_rolls_back_evidence_and_source(tmp_path: Path) -> None:
    db = StateDB(tmp_path / "state.sqlite3")
    with InternalAudioStore(db) as store:
        source_id = "b" * 32
        store.create_source(source_id, "attempt", 0, 1)
        store.prepare_leg(source_id, 0, 1)
        source = store.record_read_begin(source_id, 0, 1)
        with closing(db.open_connection()) as connection:
            connection.execute(
                "CREATE TRIGGER reject_quarantine BEFORE UPDATE ON capture_source "
                "WHEN NEW.lifecycle = 'blocked' BEGIN SELECT RAISE(ABORT, 'injected failure'); END"
            )
            connection.commit()
        with pytest.raises(InternalAudioStoreError, match="replay conflict could not be retained"):
            store._block_replay_conflict(source, 0, b"rejected")
        assert store.get_source(source_id) == source
        with closing(db.open_connection(read_only=True)) as connection:
            assert connection.execute("SELECT COUNT(*) FROM capture_conflict").fetchone()[0] == 0


def test_ambiguous_drain_with_third_durable_source_fails_stopped(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    db = StateDB(tmp_path / "state.sqlite3")
    with InternalAudioStore(db) as store:
        source_id = "c" * 32
        store.create_source(source_id, "attempt", 0, 1)
        store.prepare_leg(source_id, 0, 1)
        store.record_read_begin(source_id, 0, 1)
        commit = store._commit

        def commit_then_change_source() -> None:
            commit()
            connection = store._require_connection()
            connection.execute("UPDATE capture_source SET lifecycle = 'blocked' WHERE source_id = ?", (source_id,))
            connection.commit()
            raise sqlite3.OperationalError("ambiguous commit")

        monkeypatch.setattr(store, "_commit", commit_then_change_source)
        with pytest.raises(FatalInternalAudioStorageError, match="ambiguous drain commit could not be reconciled"):
            store.commit_drain(source_id, 0, [bytes(AUDIO_RECORD_SIZE)], DrainCheckpoint(1))
