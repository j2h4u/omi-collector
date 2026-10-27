from __future__ import annotations

import sqlite3
from collections.abc import Callable, Iterator, Mapping
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
    source_lineage,
)
from omi_collector.capture.application.storage_fault import FatalInternalAudioStorageError
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


def _ready_root(root: Path) -> Path:
    ready = root / "ready"
    ready.mkdir(mode=0o2750)
    ready.chmod(0o2750)
    return ready


def _record(timestamp: int, marker: int = 1) -> bytes:
    return pack(">I", timestamp) + bytes((2, 8, marker)) + bytes(AUDIO_RECORD_SIZE - 7)


def _source(
    store: InternalAudioStore, source_id: str, start: int, records: tuple[bytes, ...]
) -> SqlitePublicationSource:
    store.create_source(source_id, "attempt", start, start + len(records))
    source = store.prepare_leg(source_id, start, len(records))
    source = store.record_read_begin(source_id, start, len(records))
    store.commit_drain(source_id, 0, records, DrainCheckpoint(source.current_leg_generation))
    source = store.get_source(source_id)
    assert source is not None
    store.seal_source(
        source_id,
        leg_generation=source.current_leg_generation,
        expected_count=source.committed_count,
        expected_sha256=source.committed_sha256,
    )
    return SqlitePublicationSource(store, source_id)


def _options(frontier: int, *, threshold: float = 0.02, page_limit: int = 128) -> PublicAudioV2Options:
    return PublicAudioV2Options(ReadyConfig(target_audio_seconds=threshold), ClockState(), frontier, True, page_limit)


def _inject_publication_transaction_failure(
    monkeypatch: pytest.MonkeyPatch,
    store: InternalAudioStore,
    matches: Callable[[tuple[str, ...]], bool],
    *,
    after_commit: bool,
    failures: int = 1,
) -> list[bool]:
    original = store.publication_transaction
    fired: list[bool] = []

    @contextmanager
    def transaction() -> Iterator[tuple[object, sqlite3.Connection]]:
        statements: list[str] = []
        matched = False
        with original() as pair:
            state, connection = pair
            connection.set_trace_callback(statements.append)
            try:
                yield cast(tuple[object, sqlite3.Connection], (state, connection))
            finally:
                connection.set_trace_callback(None)
            matched = len(fired) < failures and matches(tuple(statements))
            if matched and not after_commit:
                fired.append(True)
                raise sqlite3.OperationalError("injected rollback before commit")
        if matched and after_commit:
            fired.append(True)
            raise sqlite3.OperationalError("injected ambiguity after commit")

    monkeypatch.setattr(store, "publication_transaction", transaction)
    return fired


def _one_statement(prefix: str) -> Callable[[tuple[str, ...]], bool]:
    return lambda statements: any(statement.lstrip().upper().startswith(prefix.upper()) for statement in statements)


def _source_state(database: StateDB, source_id: str) -> tuple[object, ...] | None:
    with closing(database.open_connection(read_only=True)) as connection:
        row = cast(
            tuple[object, ...] | sqlite3.Row | None,
            connection.execute(
                "SELECT lifecycle,source_type,block_reason,committed_count,retirement_cursor "
                "FROM capture_source WHERE source_id=?",
                (source_id,),
            ).fetchone(),
        )
        return None if row is None else tuple(row)


@pytest.mark.parametrize("after_commit", (False, True), ids=("rollback", "ambiguous-commit"))
def test_exact_below_threshold_replay_coverage_reconciles_and_retires(
    tmp_path: Path,
    audio_store: tuple[StateDB, InternalAudioStore],
    monkeypatch: pytest.MonkeyPatch,
    after_commit: bool,
) -> None:
    database, store = audio_store
    ready = _ready_root(tmp_path)
    records = (_record(100), _record(101, 2))
    finalize_public_audio_v2(store, (_source(store, "a" * 32, 10, records),), ready, _options(12, threshold=0.04))
    replay_id = "b" * 32
    replay = _source(store, replay_id, 10, records[:1])
    fired = _inject_publication_transaction_failure(
        monkeypatch,
        store,
        _one_statement("UPDATE capture_source SET lifecycle="),
        after_commit=after_commit,
    )

    finalize_public_audio_v2(store, (replay,), ready, _options(12, threshold=0.04))

    assert fired
    assert len(tuple(ready.iterdir())) == 1
    assert _source_state(database, replay_id) is not None
    with closing(database.open_connection(read_only=True)) as connection:
        assert connection.execute("SELECT count(*) FROM publication_cohort").fetchone()[0] == 0
        assert (
            connection.execute("SELECT count(*) FROM capture_record WHERE source_id=?", (replay_id,)).fetchone()[0] == 1
        )
    for _ in range(6):
        if _source_state(database, replay_id) is None:
            break
        assert retire_one_covered_typed_source(store, 128)
    assert _source_state(database, replay_id) is None
    with closing(database.open_connection(read_only=True)) as connection:
        assert (
            connection.execute("SELECT count(*) FROM capture_record WHERE source_id=?", (replay_id,)).fetchone()[0] == 0
        )


@pytest.mark.parametrize("after_commit", (False, True), ids=("rollback", "ambiguous-commit"))
def test_precohort_conflict_disposition_retries_and_preserves_unaffected_source(
    tmp_path: Path,
    audio_store: tuple[StateDB, InternalAudioStore],
    monkeypatch: pytest.MonkeyPatch,
    after_commit: bool,
) -> None:
    database, store = audio_store
    ready = _ready_root(tmp_path)
    finalize_public_audio_v2(store, (_source(store, "d" * 32, 10, (_record(100),)),), ready, _options(11))
    unaffected_id, conflict_id = "a" * 32, "b" * 32
    unaffected = _source(store, unaffected_id, 11, (_record(101),))
    conflicting = _source(store, conflict_id, 10, (_record(100, 9),))

    fired = _inject_publication_transaction_failure(
        monkeypatch,
        store,
        _one_statement("UPDATE capture_source SET source_type="),
        after_commit=after_commit,
    )
    with pytest.raises(ReadyBundleError) as failure:
        finalize_public_audio_v2(store, (unaffected, conflicting), ready, _options(12, threshold=0.04))

    assert fired, str(failure.value)
    assert _source_state(database, conflict_id) == ("blocked", "quarantine", "ready_overlap_conflict", 1, None)
    assert _source_state(database, unaffected_id) is not None
    with closing(database.open_connection(read_only=True)) as connection:
        assert (
            connection.execute("SELECT count(*) FROM capture_record WHERE source_id=?", (conflict_id,)).fetchone()[0]
            == 1
        )
        assert connection.execute("SELECT count(*) FROM publication_cohort").fetchone()[0] == 0

    original_read = store.read_record_page
    reads: list[str] = []

    def read_record_page(source_id: str, after_ordinal: int, limit: int):
        reads.append(source_id)
        if source_id == conflict_id:
            raise AssertionError("retry reread the blocked source")
        return original_read(source_id, after_ordinal, limit)

    monkeypatch.setattr(store, "read_record_page", read_record_page)
    outcome = finalize_public_audio_v2(store, (unaffected, conflicting), ready, _options(12, threshold=0.02))

    assert len(outcome.published) == 1
    assert conflict_id not in reads
    assert _source_state(database, conflict_id) == ("blocked", "quarantine", "ready_overlap_conflict", 1, None)


def test_precohort_conflict_beyond_ready_gap_blocks_source_without_accepting_cohort(
    tmp_path: Path,
    audio_store: tuple[StateDB, InternalAudioStore],
) -> None:
    database, store = audio_store
    ready = _ready_root(tmp_path)
    finalize_public_audio_v2(store, (_source(store, "a" * 32, 10, (_record(100),)),), ready, _options(11))
    conflict_id = "b" * 32
    conflict = _source(store, conflict_id, 9, (_record(99, 2), _record(100, 9)))

    with pytest.raises(ReadyBundleError):
        finalize_public_audio_v2(store, (conflict,), ready, _options(11, threshold=0.04))

    assert _source_state(database, conflict_id) == ("blocked", "quarantine", "ready_overlap_conflict", 2, None)
    with closing(database.open_connection(read_only=True)) as connection:
        assert connection.execute("SELECT count(*) FROM publication_cohort").fetchone()[0] == 0
        assert (
            connection.execute("SELECT count(*) FROM capture_record WHERE source_id=?", (conflict_id,)).fetchone()[0]
            == 2
        )


def test_accepted_cohort_with_impossible_complete_cursor_fails_stopped(
    tmp_path: Path,
    audio_store: tuple[StateDB, InternalAudioStore],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database, store = audio_store
    ready = _ready_root(tmp_path)
    source_id = "c" * 32
    source = _source(store, source_id, 20, (_record(200),))
    original = ready_bundles._ensure_v2_stage

    def interrupt_before_stage(*_args: object, **_kwargs: object) -> object:
        raise ReadyBundleError("injected restart before stage")

    monkeypatch.setattr(ready_bundles, "_ensure_v2_stage", interrupt_before_stage)
    with pytest.raises(ReadyBundleError, match="injected restart"):
        finalize_public_audio_v2(store, (source,), ready, _options(21))
    monkeypatch.setattr(ready_bundles, "_ensure_v2_stage", original)

    with closing(database.open_connection()) as connection:
        changed = connection.execute(
            "UPDATE publication_cohort_member SET cursor=admitted_count WHERE source_id=?",
            (source_id,),
        )
        assert changed.rowcount == 1
        connection.commit()

    with pytest.raises(FatalInternalAudioStorageError, match="no unique source slices"):
        finalize_public_audio_v2(store, (source,), ready, _options(21))

    assert store._failed is True
    with closing(database.open_connection(read_only=True)) as connection:
        assert connection.execute("SELECT count(*) FROM publication_cohort").fetchone()[0] == 1
        assert (
            connection.execute("SELECT count(*) FROM capture_record WHERE source_id=?", (source_id,)).fetchone()[0] == 1
        )


@pytest.mark.parametrize("after_commit", (False, True), ids=("rollback", "ambiguous-commit"))
def test_multi_page_private_retirement_reconciles_cursor_and_finishes_gc(
    tmp_path: Path,
    audio_store: tuple[StateDB, InternalAudioStore],
    monkeypatch: pytest.MonkeyPatch,
    after_commit: bool,
) -> None:
    database, store = audio_store
    ready = _ready_root(tmp_path)
    source_id = "c" * 32
    source = _source(store, source_id, 20, tuple(_record(100 + index, index + 1) for index in range(3)))
    outcome = finalize_public_audio_v2(store, (source,), ready, _options(23, page_limit=1))
    assert len(outcome.published) == 1
    assert _source_state(database, source_id) == ("retiring", "attempt", None, 3, 0)
    assert retire_one_covered_typed_source(store, 1)
    assert _source_state(database, source_id) == ("retiring", "attempt", None, 3, 1)

    original_transaction = store.publication_transaction
    fired = _inject_publication_transaction_failure(
        monkeypatch,
        store,
        _one_statement("DELETE FROM capture_record"),
        after_commit=after_commit,
    )
    assert retire_one_covered_typed_source(store, 1)

    assert fired
    assert _source_state(database, source_id) == ("retiring", "attempt", None, 3, 2)
    marker = database.get(StateKind.COMPACTION, "current")
    assert marker is not None and marker.get("pending") is True

    monkeypatch.setattr(store, "publication_transaction", original_transaction)
    for _ in range(3):
        retire_one_covered_typed_source(store, 1)
    assert _source_state(database, source_id) is None
    with closing(database.open_connection(read_only=True)) as connection:
        assert (
            connection.execute("SELECT count(*) FROM capture_record WHERE source_id=?", (source_id,)).fetchone()[0] == 0
        )


def _legacy_entry(gaps: object, sources: object) -> dict[str, object]:
    entry: dict[str, object] = {
        "records_sha256": "a" * 64,
        "state": "retired",
        "start_sequence": 100,
        "next_sequence": 106,
        "draft_raw_sha256": "b" * 64,
    }
    if gaps is not None:
        entry["gaps"] = gaps
    if sources is not None:
        entry["sources"] = sources
    return entry


def test_retired_legacy_lineage_gap_shapes_are_strictly_validated(tmp_path: Path) -> None:
    database = StateDB(tmp_path / "ledger.sqlite3")
    one_source = [{"start_sequence": 100, "next_sequence": 106, "raw_sha256": "c" * 64}]
    database.put(StateKind.READY_PUBLICATION, "d" * 64, _legacy_entry(None, one_source))
    persisted = cast(Mapping[str, object], database.get(StateKind.READY_PUBLICATION, "d" * 64))
    assert source_lineage(persisted) == ((100, 106, "c" * 64),)

    gap_sources = [
        {"start_sequence": 100, "next_sequence": 102, "raw_sha256": "e" * 64},
        {"start_sequence": 104, "next_sequence": 106, "raw_sha256": "f" * 64},
    ]
    database.put(
        StateKind.READY_PUBLICATION,
        "1" * 64,
        _legacy_entry([[102, 104]], gap_sources),
    )
    gap_entry = cast(Mapping[str, object], database.get(StateKind.READY_PUBLICATION, "1" * 64))
    assert source_lineage(gap_entry) == ((100, 102, "e" * 64), (104, 106, "f" * 64))

    for gaps in ([], "102-104", [[True, 104]], [[102, 106]], [[103, 104], [102, 103]], [[100, 102]]):
        with pytest.raises(ReadyBundleError, match="source lineage is invalid"):
            source_lineage(_legacy_entry(gaps, gap_sources))
