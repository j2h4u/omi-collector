from __future__ import annotations

import sqlite3
import threading
from collections.abc import Iterator
from contextlib import closing, contextmanager
from pathlib import Path
from struct import pack
from typing import cast

import pytest

from omi_collector.capture.adapters import ready_bundles
from omi_collector.capture.adapters.collector_state_db import StateDB, StateKind
from omi_collector.capture.adapters.internal_audio_store import AUDIO_RECORD_SIZE, DrainCheckpoint, InternalAudioStore
from omi_collector.capture.adapters.ready_bundles import (
    PublicAudioV2Options,
    ReadyBundleError,
    SqlitePublicationSource,
    finalize_public_audio_v2,
    retire_one_covered_typed_source,
)
from omi_collector.capture.adapters.sqlite_compaction import pending_compaction_generation, vacuum_pending
from omi_collector.capture.domain.clock_state import ClockState
from omi_collector.config import ReadyConfig


@pytest.fixture
def audio_store(tmp_path: Path) -> Iterator[tuple[StateDB, InternalAudioStore]]:
    database = StateDB(tmp_path / "collector-state.sqlite3")
    store = InternalAudioStore(database)
    try:
        yield database, store
    finally:
        store.close()


def _record(timestamp: int) -> bytes:
    return pack(">I", timestamp) + bytes((2, 8, 1)) + bytes(AUDIO_RECORD_SIZE - 7)


def _covered_source(
    store: InternalAudioStore, root: Path, source_id: str = "a" * 32, count: int = 3, start: int = 10
) -> tuple[Path, str]:
    store.create_source(source_id, "attempt", start, start + count)
    source = store.prepare_leg(source_id, start, count)
    source = store.record_read_begin(source_id, start, count)
    store.commit_checked_drain(
        source_id,
        0,
        tuple(_record(100 + index) for index in range(count)),
        DrainCheckpoint(source.current_leg_generation),
    )
    assert store.finish_read(source_id, start + count, count * AUDIO_RECORD_SIZE)
    source = store.get_source(source_id)
    assert source is not None
    store.seal_source(
        source_id,
        leg_generation=source.current_leg_generation,
        expected_count=source.committed_count,
        expected_sha256=source.committed_sha256,
    )
    ready = root / "ready"
    ready.mkdir(mode=0o2750, exist_ok=True)
    ready.chmod(0o2750)
    options = PublicAudioV2Options(ReadyConfig(target_audio_seconds=0.02), ClockState(), start + count, True, 128)
    artifact = finalize_public_audio_v2(store, (SqlitePublicationSource(store, source_id),), ready, options).published[
        0
    ]
    return artifact.path, source_id


def test_retirement_preserves_source_with_framing_incident(
    tmp_path: Path, audio_store: tuple[StateDB, InternalAudioStore]
) -> None:
    database, store = audio_store
    _, source_id = _covered_source(store, tmp_path)
    with closing(database.open_connection()) as connection:
        connection.execute(
            "INSERT INTO capture_framing_incident(incident_id,confirmed_frontier,status,confirmed_damage) "
            "VALUES (?,?,'unresolved',1)",
            (source_id, (13).to_bytes(8, "big")),
        )
        connection.commit()

    assert not retire_one_covered_typed_source(store, 128)
    assert store.get_source(source_id) is not None
    assert tuple(record.raw_bytes for record in store.read_record_page(source_id, limit=8)) == tuple(
        _record(100 + index) for index in range(3)
    )
    with closing(database.open_connection(read_only=True)) as connection:
        assert connection.execute("SELECT count(*) FROM capture_framing_incident").fetchone()[0] == 1


def test_retirement_preserves_recovery_candidate_reference(
    tmp_path: Path, audio_store: tuple[StateDB, InternalAudioStore]
) -> None:
    database, store = audio_store
    _, source_id = _covered_source(store, tmp_path)
    incident_id = "f" * 32
    store.create_source(incident_id, "attempt", 20, 21)
    store.prepare_leg(incident_id, 20, 1)
    store.record_read_begin(incident_id, 20, 1)
    store.commit_checked_drain(incident_id, 0, (bytes([255]) * AUDIO_RECORD_SIZE,), DrainCheckpoint(1))
    with closing(database.open_connection()) as connection:
        connection.execute(
            "INSERT INTO capture_recovery_candidate(incident_id,source_id,leg_generation) VALUES (?,?,1)",
            (incident_id, source_id),
        )
        connection.commit()

    assert not retire_one_covered_typed_source(store, 128)
    assert store.get_source(source_id) is not None
    assert len(store.read_record_page(source_id, limit=8)) == 3
    with closing(database.open_connection(read_only=True)) as connection:
        assert connection.execute("SELECT count(*) FROM capture_recovery_candidate").fetchone()[0] == 1
    with closing(sqlite3.connect(database.path)) as connection:
        connection.execute("CREATE TABLE compaction_fixture (payload BLOB)")
        connection.executemany("INSERT INTO compaction_fixture VALUES (?)", [(bytes(3500),)] * 100)
        connection.commit()
        connection.execute("DROP TABLE compaction_fixture")
        connection.commit()
    database.put(StateKind.COMPACTION, "current", {"generation": 6, "pending": True})
    assert pending_compaction_generation(database) == 6
    assert vacuum_pending(database, 6, threading.Event()) == "done"
    assert len(store.read_record_page(source_id, limit=8)) == 3


def test_retirement_preserves_retry_observation_linked_to_another_incident(
    tmp_path: Path, audio_store: tuple[StateDB, InternalAudioStore]
) -> None:
    database, store = audio_store
    _, retry_id = _covered_source(store, tmp_path)
    incident_id = "f" * 32
    store.create_source(incident_id, "attempt", 20, 21)
    store.prepare_leg(incident_id, 20, 1)
    store.record_read_begin(incident_id, 20, 1)
    store.commit_checked_drain(incident_id, 0, (bytes([255]) * AUDIO_RECORD_SIZE,), DrainCheckpoint(1))
    with closing(database.open_connection()) as connection:
        connection.execute(
            "INSERT INTO capture_observation(source_id,incident_id,leg_generation,read_start,read_count,"
            "prefix_record_count,received_bytes,tail,done) VALUES (?,?,1,?,3,1,?,X'',1)",
            (retry_id, incident_id, (10).to_bytes(8, "big"), AUDIO_RECORD_SIZE),
        )
        connection.commit()

    assert not retire_one_covered_typed_source(store, 128)
    assert tuple(record.raw_bytes for record in store.read_record_page(retry_id, limit=8)) == tuple(
        _record(100 + index) for index in range(3)
    )


def test_retirement_rechecks_framing_evidence_after_selection(
    tmp_path: Path, audio_store: tuple[StateDB, InternalAudioStore], monkeypatch: pytest.MonkeyPatch
) -> None:
    database, store = audio_store
    _, source_id = _covered_source(store, tmp_path)
    advance = ready_bundles._advance_typed_retirement

    def insert_incident(audio: InternalAudioStore, step: ready_bundles._RetirementStep) -> bool:
        with closing(database.open_connection()) as connection:
            connection.execute(
                "INSERT INTO capture_framing_incident(incident_id,confirmed_frontier,status,confirmed_damage) "
                "VALUES (?,?,'unresolved',1)",
                (source_id, (13).to_bytes(8, "big")),
            )
            connection.commit()
        return advance(audio, step)

    monkeypatch.setattr(ready_bundles, "_advance_typed_retirement", insert_incident)
    assert not retire_one_covered_typed_source(store, 128)
    assert len(store.read_record_page(source_id, limit=8)) == 3


def test_held_source_does_not_block_retirement_of_another_source(
    tmp_path: Path, audio_store: tuple[StateDB, InternalAudioStore]
) -> None:
    database, store = audio_store
    _, held = _covered_source(store, tmp_path)
    _, other = _covered_source(store, tmp_path, source_id="b" * 32, start=20)
    with closing(database.open_connection()) as connection:
        connection.execute(
            "INSERT INTO capture_framing_incident(incident_id,confirmed_frontier,status,confirmed_damage) "
            "VALUES (?,?,'unresolved',1)",
            (held, (13).to_bytes(8, "big")),
        )
        connection.commit()
    assert retire_one_covered_typed_source(store, 128)
    assert retire_one_covered_typed_source(store, 128)
    assert not retire_one_covered_typed_source(store, 128)
    assert store.get_source(other) is None
    assert len(store.read_record_page(held, limit=8)) == 3


def _database_state(database: StateDB, source_id: str) -> tuple[object, ...]:
    with closing(database.open_connection(read_only=True)) as connection:
        source = cast(
            tuple[object, ...] | None,
            connection.execute(
                "SELECT retirement_cursor FROM capture_source WHERE source_id=?", (source_id,)
            ).fetchone(),
        )
        records = tuple(
            cast(
                list[tuple[object, ...]],
                connection.execute(
                    "SELECT ordinal,device_timestamp,payload FROM capture_record WHERE source_id=? ORDER BY ordinal",
                    (source_id,),
                ).fetchall(),
            )
        )
        summary = cast(
            tuple[object, ...] | None,
            connection.execute(
                "SELECT payload FROM state WHERE kind=? AND id=?",
                (StateKind.PACKET_SUMMARY.value, source_id),
            ).fetchone(),
        )
        marker = cast(
            tuple[object, ...] | None,
            connection.execute(
                "SELECT payload FROM state WHERE kind=? AND id='current'", (StateKind.COMPACTION.value,)
            ).fetchone(),
        )
    return (None if source is None else source[0], records, summary, marker)


def _artifact_snapshot(path: Path) -> tuple[tuple[str, bytes], ...]:
    return tuple(sorted((item.name, item.read_bytes()) for item in path.iterdir()))


@pytest.mark.parametrize(
    ("damage", "page_limit"),
    (("contiguous-short-page", 3), ("truncated-final-page", 2), ("beyond-coverage", 4)),
)
def test_typed_retirement_rejects_non_exact_bounded_prefix_atomically(
    tmp_path: Path,
    audio_store: tuple[StateDB, InternalAudioStore],
    damage: str,
    page_limit: int,
) -> None:
    database, store = audio_store
    artifact, source_id = _covered_source(store, tmp_path)
    if damage == "truncated-final-page":
        assert retire_one_covered_typed_source(store, page_limit)
    with closing(database.open_connection()) as connection:
        if damage in {"contiguous-short-page", "truncated-final-page"}:
            connection.execute("DELETE FROM capture_record WHERE source_id=? AND ordinal=2", (source_id,))
        else:
            record = _record(103)
            connection.execute(
                "INSERT INTO capture_record(source_id,ordinal,device_timestamp,payload) VALUES (?,?,?,?)",
                (source_id, 3, 103, record[4:]),
            )
        connection.commit()
    before = _database_state(database, source_id)
    artifact_before = _artifact_snapshot(artifact)

    with pytest.raises(ReadyBundleError, match=r"page does not match its bounded prefix|ended before coverage"):
        retire_one_covered_typed_source(store, page_limit)

    assert _database_state(database, source_id) == before
    assert _artifact_snapshot(artifact) == artifact_before


def _inject_final_delete_failure(
    monkeypatch: pytest.MonkeyPatch,
    store: InternalAudioStore,
    *,
    after_commit: bool,
    failures: int = 1,
) -> list[bool]:
    original = store.publication_transaction
    fired: list[bool] = []

    @contextmanager
    def transaction() -> Iterator[tuple[object, sqlite3.Connection]]:
        statements: list[str] = []
        with original() as (state, connection):
            connection.set_trace_callback(statements.append)
            try:
                yield cast(tuple[object, sqlite3.Connection], (state, connection))
            finally:
                connection.set_trace_callback(None)
            matched = len(fired) < failures and any(
                statement.lstrip().upper().startswith("DELETE FROM CAPTURE_SOURCE") for statement in statements
            )
            if matched and not after_commit:
                fired.append(True)
                raise sqlite3.OperationalError("injected rollback before final retirement commit")
        if matched and after_commit:
            fired.append(True)
            raise sqlite3.OperationalError("injected lost final retirement response")

    monkeypatch.setattr(store, "publication_transaction", transaction)
    return fired


@pytest.mark.parametrize("after_commit", (False, True), ids=("both-attempts-rollback", "lost-response"))
def test_typed_final_source_and_summary_deletion_are_atomic(
    tmp_path: Path,
    audio_store: tuple[StateDB, InternalAudioStore],
    monkeypatch: pytest.MonkeyPatch,
    after_commit: bool,
) -> None:
    database, store = audio_store
    artifact, source_id = _covered_source(store, tmp_path, count=1)
    database.put(StateKind.PACKET_SUMMARY, source_id, {"sentinel": "typed-retirement"})
    assert retire_one_covered_typed_source(store, 1)
    assert _database_state(database, source_id)[0] == 1
    before = _database_state(database, source_id)
    assert before[2] is not None
    artifact_before = _artifact_snapshot(artifact)
    fired = _inject_final_delete_failure(
        monkeypatch, store, after_commit=after_commit, failures=1 if after_commit else 2
    )

    if after_commit:
        assert retire_one_covered_typed_source(store, 1)
        final = _database_state(database, source_id)
        assert final == (None, (), None, before[3])
    else:
        with pytest.raises(ReadyBundleError, match="rolled back"):
            retire_one_covered_typed_source(store, 1)
        assert _database_state(database, source_id) == before

    assert len(fired) == (1 if after_commit else 2)
    assert _artifact_snapshot(artifact) == artifact_before
