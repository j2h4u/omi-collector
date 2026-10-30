"""Behavioral edge tests for atomic quarantine admission and expiry."""

from __future__ import annotations

import sqlite3
from contextlib import closing
from pathlib import Path
from typing import cast

import pytest

from omi_collector.capture.adapters import collector_state_db, quarantine
from omi_collector.capture.adapters.attempts import SqliteStagedAttempt
from omi_collector.capture.adapters.collector_state_db import StateDB, StateKind
from omi_collector.capture.adapters.internal_audio_store import CaptureSource
from omi_collector.capture.adapters.staging_contract import AttemptStateError, StagingError
from omi_collector.capture.domain.ring_protocol import RECORD_SIZE, ReadBeginNotification
from staging_test_factory import StagingStore


def _store(tmp_path: Path) -> StagingStore:
    return StagingStore(tmp_path, tmp_path.parent / f"{tmp_path.name}-captures")


def _record(marker: int) -> bytes:
    return marker.to_bytes(4, "big") + bytes((marker,)) * (RECORD_SIZE - 4)


def _seed_quarantine(store: StagingStore, sequence: int = 100) -> tuple[SqliteStagedAttempt, str]:
    attempt = store.prepare_streaming_attempt(sequence, 1)
    attempt.prepare_leg(sequence, 1)
    attempt.record_read_begin(ReadBeginNotification(sequence, 1))
    attempt.accept_chunk(sequence, _record(1))
    source_id = store.quarantine_attempt_source(attempt.attempt_id)
    return attempt, source_id


def test_admission_rejects_changed_snapshot_after_authentication(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = _store(tmp_path)
    attempt, source_id = _seed_quarantine(store)
    authenticate = quarantine.authenticate_capture_source

    def authenticate_then_change(state_db: StateDB, source: str, *, page_limit: int) -> CaptureSource:
        authenticated = authenticate(state_db, source, page_limit=page_limit)
        with closing(store.state_db.open_connection()) as connection:
            connection.execute(
                "UPDATE capture_source SET committed_count = 2 WHERE source_id = ?", (source_id,)
            )
            connection.commit()
        return authenticated

    monkeypatch.setattr(quarantine, "authenticate_capture_source", authenticate_then_change)

    try:
        with pytest.raises(AttemptStateError, match="changed during prefix admission"):
            store.admit_quarantined_prefix(source_id)

        with closing(store.state_db.open_connection(read_only=True)) as connection:
            row = cast(
                tuple[object, ...] | None,
                connection.execute(
                    "SELECT committed_count, quarantine_disposition, certified_prefix_count, coverage_digest "
                    "FROM capture_source WHERE source_id = ?",
                    (source_id,),
                ).fetchone(),
            )
        assert row is not None and tuple(row) == (2, "unclassified", None, None)
    finally:
        attempt.close()


def test_sweep_commit_failure_rolls_back_expiry_and_compaction_debt(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = _store(tmp_path)
    attempt, source_id = _seed_quarantine(store)
    store.mark_quarantine_unprocessable(source_id, "expired quarantine evidence")
    store.state_db.put(StateKind.COMPACTION, "current", {"generation": 7, "pending": False})
    with closing(store.state_db.open_connection()) as connection:
        connection.execute(
            "UPDATE capture_source SET retention_deadline = '0' WHERE source_id = ?", (source_id,)
        )
        connection.commit()
    debt_before = store.state_db.get(StateKind.COMPACTION, "current")
    commit = collector_state_db._GuardedConnection.commit
    fail_commit = True

    def fail_sweep_commit(connection: collector_state_db._GuardedConnection) -> None:
        nonlocal fail_commit
        if fail_commit:
            fail_commit = False
            raise sqlite3.OperationalError("injected sweep COMMIT failure")
        commit(connection)

    monkeypatch.setattr(collector_state_db._GuardedConnection, "commit", fail_sweep_commit)
    try:
        with pytest.raises(StagingError, match="retention sweep failed"):
            store.sweep_terminal_quarantine()

        assert store.state_db.get(StateKind.COMPACTION, "current") == debt_before
        with closing(store.state_db.open_connection(read_only=True)) as connection:
            source_exists = cast(
                tuple[object, ...] | None,
                connection.execute("SELECT 1 FROM capture_source WHERE source_id = ?", (source_id,)).fetchone(),
            )
            record_count = cast(
                tuple[int],
                connection.execute("SELECT COUNT(*) FROM capture_record WHERE source_id = ?", (source_id,)).fetchone(),
            )[0]
        assert source_exists is not None
        assert record_count == 1
    finally:
        attempt.close()
