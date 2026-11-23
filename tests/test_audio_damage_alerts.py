from __future__ import annotations

import json
import sqlite3
from contextlib import closing
from pathlib import Path
from typing import cast

import pytest

from omi_collector.capture.adapters.collector_state_db import StateDB
from omi_collector.capture.adapters.framing_incidents import settle_recovered, settle_unavailable
from omi_collector.capture.adapters.internal_audio_store import DrainCheckpoint, InternalAudioStore


def _alert(db: StateDB) -> list[tuple[str, str]]:
    with closing(sqlite3.connect(db.path)) as connection:
        rows = cast(
            list[tuple[str, str]],
            connection.execute("SELECT id,payload FROM state WHERE kind='audio_damage_alert' ORDER BY id").fetchall(),
        )
    return [(source, str(cast(dict[str, object], json.loads(payload))["outcome"])) for source, payload in rows]


def _damaged(store: InternalAudioStore, source: str) -> None:
    store.create_source(source, "attempt", 100, 102)
    store.prepare_leg(source, 100, 2)
    store.record_read_begin(source, 100, 2)
    store.commit_checked_drain(source, 0, (bytes([255]) * 444,), DrainCheckpoint(1))
    assert not store.finish_read(source, 102, 444)


def _healthy(store: InternalAudioStore, source: str) -> None:
    store.create_source(source, "attempt", 100, 102)
    store.prepare_leg(source, 100, 2)
    store.record_read_begin(source, 100, 2)
    store.commit_checked_drain(source, 0, (bytes(444), bytes(444)), DrainCheckpoint(1))
    assert store.finish_read(source, 102, 888)
    current = store.get_source(source)
    assert current is not None
    store.seal_source(source, leg_generation=1, expected_count=2, expected_sha256=current.committed_sha256)


def test_recovered_outcome_is_durable_and_never_backfilled(tmp_path: Path) -> None:
    db = StateDB(tmp_path / "state.sqlite3")
    original, recovered = "a1" * 16, "a2" * 16
    with InternalAudioStore(db, chunk_limit=8) as store:
        _damaged(store, original)
        assert _alert(db) == []
        _healthy(store, recovered)
    assert settle_recovered(db, recovered) == (original,)
    assert _alert(StateDB(db.path)) == [(original, "recovered")]
    assert settle_recovered(db, recovered) == ()
    assert _alert(db) == [(original, "recovered")]


@pytest.mark.parametrize("first_outcome", ["interrupted", "quarantined", "short", "unavailable"])
def test_first_unsuccessful_outcome_remains_after_later_recovery(tmp_path: Path, first_outcome: str) -> None:
    db = StateDB(tmp_path / "state.sqlite3")
    original, retry, recovered = "b1" * 16, "b2" * 16, "b3" * 16
    with InternalAudioStore(db, chunk_limit=8) as store:
        _damaged(store, original)
        if first_outcome == "unavailable":
            assert settle_unavailable(db, 101, 102) == (original,)
        elif first_outcome == "short":
            _damaged(store, retry)
        else:
            store.create_source(retry, "attempt", 100, 102)
            store.prepare_leg(retry, 100, 2)
            store.record_read_begin(retry, 100, 2)
            if first_outcome == "quarantined":
                store.quarantine_unconfirmed(retry)
            else:
                store.interrupt(retry)
        assert _alert(db) == [(original, "quarantined")]
        if first_outcome != "unavailable":
            _healthy(store, recovered)
    if first_outcome != "unavailable":
        assert settle_recovered(db, recovered) == (original,)
    assert _alert(StateDB(db.path)) == [(original, "quarantined")]


def test_outcome_and_settlement_rollback_together(tmp_path: Path) -> None:
    db = StateDB(tmp_path / "state.sqlite3")
    original = "c1" * 16
    with InternalAudioStore(db, chunk_limit=8) as store:
        _damaged(store, original)
    with closing(sqlite3.connect(db.path)) as connection:
        connection.execute(
            "CREATE TRIGGER reject_settlement BEFORE UPDATE ON capture_framing_incident "
            "BEGIN SELECT RAISE(ABORT, 'test interruption'); END"
        )
        connection.commit()
    with pytest.raises(sqlite3.IntegrityError, match="test interruption"):
        settle_unavailable(db, 101, 102)
    assert _alert(StateDB(db.path)) == []
    with closing(sqlite3.connect(db.path)) as connection:
        assert connection.execute("SELECT status FROM capture_framing_incident").fetchone() == ("unresolved",)
        connection.execute("DROP TRIGGER reject_settlement")
        connection.commit()
    assert settle_unavailable(db, 101, 102) == (original,)
    assert _alert(db) == [(original, "quarantined")]


def test_incident_before_activation_is_not_notified_after_later_recovery(tmp_path: Path) -> None:
    db = StateDB(tmp_path / "state.sqlite3")
    original, retry = "d1" * 16, "d2" * 16
    with InternalAudioStore(db, chunk_limit=8) as store:
        _damaged(store, original)
    with closing(sqlite3.connect(db.path)) as connection:
        connection.execute("DELETE FROM state WHERE kind='audio_damage_eligible'")
        connection.commit()
    with InternalAudioStore(StateDB(db.path), chunk_limit=8) as store:
        _damaged(store, retry)
    assert _alert(db) == []
    assert settle_unavailable(db, 101, 102) == (original,)
    assert _alert(db) == []


def test_historical_custody_does_not_become_a_new_alert(tmp_path: Path) -> None:
    db = StateDB(tmp_path / "state.sqlite3")
    original = "d3" * 16
    with InternalAudioStore(db, chunk_limit=8) as store:
        store.create_source(original, "attempt", 100, 102)
        with closing(sqlite3.connect(db.path)) as connection:
            connection.execute(
                "INSERT INTO capture_historical_custody "
                "(source_id,prior_source_type,prior_lifecycle,prior_count,prior_sha256,disposition) "
                "VALUES (?,'attempt','prepared',0,?,'pending')",
                (original, "0" * 64),
            )
            connection.commit()
        store.prepare_leg(original, 100, 2)
        store.record_read_begin(original, 100, 2)
        store.commit_checked_drain(original, 0, (bytes([255]) * 444,), DrainCheckpoint(1))
        assert not store.finish_read(original, 102, 444)
    assert settle_unavailable(db, 101, 102) == (original,)
    assert _alert(db) == []
