from __future__ import annotations

import shutil
import sqlite3
from collections.abc import Iterator
from contextlib import closing, contextmanager
from hashlib import sha256
from pathlib import Path
from struct import pack
from typing import cast

import pytest

import omi_collector.capture.adapters.ready_bundles as ready_bundles
from omi_collector.capture.adapters.clock_segments import ClockSegmentMap
from omi_collector.capture.adapters.collector_state_db import StateDB, StateKind
from omi_collector.capture.adapters.internal_audio_store import (
    AUDIO_RECORD_SIZE,
    DrainCheckpoint,
    InternalAudioStore,
    InternalAudioStoreError,
)
from omi_collector.capture.adapters.ready_bundles import (
    ReadyBundleError,
    ReadyOutcome,
    ReadyOutcomeState,
    _CustodyCommit,
    _RetirementResult,
    _RetirementStep,
    _TypedPage,
    finalize_typed_sources,
)
from omi_collector.capture.domain.internal_audio_machine import (
    AudioAction,
    AudioDecision,
    AudioEvent,
    AudioPhase,
    CohortAction,
    CohortDecision,
    CohortEvent,
    CohortPhase,
)
from omi_collector.config import ReadyConfig


class _Source:
    def __init__(self, store: InternalAudioStore, source_id: str) -> None:
        self._store = store
        self.source_id = source_id

    @property
    def source(self):
        source = self._store.get_source(self.source_id)
        assert source is not None
        return source

    def source_page(self, after_ordinal: int = -1, limit: int = 128) -> _TypedPage:
        return _TypedPage(
            self.source,
            after_ordinal,
            self._store.read_record_page(self.source_id, after_ordinal, limit),
        )


def _record(timestamp: int, fill: int) -> bytes:
    return pack(">I", timestamp) + bytes((2, 8, fill)) + bytes(AUDIO_RECORD_SIZE - 7)


def _source(
    store: InternalAudioStore,
    source_id: str,
    start: int,
    records: tuple[bytes, ...],
) -> _Source:
    store.create_source(source_id, "attempt", start, start + len(records))
    store.prepare_leg(source_id, start, len(records))
    store.record_read_begin(source_id, start, len(records))
    store.commit_drain(source_id, 0, records, DrainCheckpoint(1))
    with closing(store.state_db.open_connection()) as connection:
        connection.execute("BEGIN IMMEDIATE")
        connection.execute(
            "UPDATE capture_source SET lifecycle='sealed_waiting' WHERE source_id=?",
            (source_id,),
        )
        connection.commit()
    return _Source(store, source_id)


def _ready_root(path: Path) -> Path:
    path.mkdir()
    path.chmod(0o2750)
    return path


def _publish(
    store: InternalAudioStore,
    sources: tuple[_Source, ...],
    ready: Path,
    *,
    frontier: int,
    page_limit: int = 128,
) -> ReadyOutcome:
    return finalize_typed_sources(
        store,
        sources,
        ready,
        config=ReadyConfig(target_audio_seconds=0.02),
        clock_segments=ClockSegmentMap(()),
        frontier=frontier,
        drained=True,
        page_limit=page_limit,
    )


def _pause_after_cohort_acceptance(
    monkeypatch: pytest.MonkeyPatch,
    store: InternalAudioStore,
    source: _Source,
    ready: Path,
    *,
    frontier: int,
) -> None:
    original = ready_bundles._resume_typed_cohort

    def stop_after_acceptance(*_args: object, **_kwargs: object) -> tuple[list[object], None]:
        raise ReadyBundleError("test stop after durable cohort acceptance")

    monkeypatch.setattr(ready_bundles, "_resume_typed_cohort", stop_after_acceptance)
    with pytest.raises(ReadyBundleError, match="durable cohort acceptance"):
        _publish(store, (source,), ready, frontier=frontier)
    monkeypatch.setattr(ready_bundles, "_resume_typed_cohort", original)
    with closing(store.state_db.open_connection(read_only=True)) as connection:
        row = cast(
            tuple[object] | None,
            connection.execute("SELECT lifecycle FROM publication_cohort").fetchone(),
        )
        assert row is not None
        assert tuple(row) == ("accepted",)


def _publish_subthreshold(
    store: InternalAudioStore,
    sources: tuple[_Source, ...],
    ready: Path,
    frontier: int,
) -> ReadyOutcome:
    return finalize_typed_sources(
        store,
        sources,
        ready,
        config=ReadyConfig(target_audio_seconds=0.04),
        clock_segments=ClockSegmentMap(()),
        frontier=frontier,
        drained=True,
        page_limit=128,
    )


def _install_ready_custody(
    tmp_path: Path,
    database: StateDB,
    ready: Path,
    records: tuple[bytes, ...],
    *,
    start: int,
) -> Path:
    seed_database = StateDB(tmp_path / f"seed-{records[0][-1]}.sqlite3")
    with InternalAudioStore(seed_database) as seed_store:
        seed_source = _source(seed_store, "f" * 32, start, records)
        seed_ready = _ready_root(tmp_path / f"seed-ready-{records[0][-1]}")
        outcome = _publish(seed_store, (seed_source,), seed_ready, frontier=start + len(records))
        assert outcome.state is ReadyOutcomeState.PUBLISHED
        result = outcome.published[0]
        destination = ready / result.bundle_id
        shutil.copytree(result.path, destination)
    for item_id, payload in seed_database.list(StateKind.READY_PUBLICATION):
        database.put(StateKind.READY_PUBLICATION, item_id, payload)
    return destination


def _source_state(database: StateDB, source_id: str) -> tuple[object, ...] | None:
    with closing(database.open_connection(read_only=True)) as connection:
        row = cast(
            tuple[object, ...] | None,
            connection.execute(
                "SELECT lifecycle,block_reason,retirement_cursor,"
                "(SELECT count(*) FROM capture_record WHERE source_id=?) "
                "FROM capture_source WHERE source_id=?",
                (source_id, source_id),
            ).fetchone(),
        )
        return None if row is None else tuple(row)


def _conflict_state(database: StateDB, source_id: str) -> tuple[object, ...] | None:
    with closing(database.open_connection(read_only=True)) as connection:
        row = cast(
            tuple[object, ...] | None,
            connection.execute(
                "SELECT source_type,lifecycle,block_reason,quarantine_disposition,quarantine_opaque,"
                "terminal_reason,terminal_time,retention_deadline,certified_prefix_count,coverage_digest,"
                "retirement_cursor FROM capture_source WHERE source_id=?",
                (source_id,),
            ).fetchone(),
        )
    return None if row is None else tuple(row)


def _cohort_count(database: StateDB) -> int:
    with closing(database.open_connection(read_only=True)) as connection:
        row = cast(tuple[int], connection.execute("SELECT count(*) FROM publication_cohort").fetchone())
    return row[0]


def _active_cohort_snapshot(database: StateDB) -> tuple[tuple[object, ...], tuple[tuple[object, ...], ...]]:
    with closing(database.open_connection(read_only=True)) as connection:
        row = cast(
            tuple[object, ...],
            connection.execute(
                "SELECT cohort_id,lifecycle,accepted_permit,drained_threshold,config_digest,closure_frontier,"
                "cursor,accepted_time FROM publication_cohort"
            ).fetchone(),
        )
        members = cast(
            list[tuple[object, ...]],
            connection.execute(
                "SELECT member_ordinal,source_id,slice_start,slice_count,admitted_lifecycle,admitted_count,"
                "admitted_sha256,cursor FROM publication_cohort_member ORDER BY member_ordinal"
            ).fetchall(),
        )
    return tuple(row), tuple(tuple(member) for member in members)


@pytest.mark.parametrize("fault", ("rolled_back", "committed", "third_state"))
def test_ready_custody_cursor_commit_reconciles_exact_old_and_new_states(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    fault: str,
) -> None:
    database = StateDB(tmp_path / "state.sqlite3")
    with InternalAudioStore(database) as store:
        ready = _ready_root(tmp_path / "ready")
        prefix = _source(store, "a" * 32, 100, (_record(100, 1),))
        assert _publish(store, (prefix,), ready, frontier=101).state is ReadyOutcomeState.PUBLISHED
        replay = _source(store, "b" * 32, 100, (_record(100, 1), _record(101, 2)))
        _pause_after_cohort_acceptance(monkeypatch, store, replay, ready, frontier=102)

        original = ready_bundles._commit_typed_ready_custody_once
        calls = 0

        def inject(audio_store: InternalAudioStore, commit: _CustodyCommit) -> None:
            nonlocal calls
            calls += 1
            if calls > 1:
                original(audio_store, commit)
                return
            if fault == "rolled_back":
                raise sqlite3.DatabaseError("simulated custody rollback")
            original(audio_store, commit)
            if fault == "third_state":
                with closing(database.open_connection()) as connection:
                    connection.execute("BEGIN IMMEDIATE")
                    connection.execute(
                        "UPDATE publication_cohort_member SET cursor=2 WHERE cohort_id=? AND member_ordinal=0",
                        (commit.cohort.cohort_id,),
                    )
                    connection.commit()
            raise sqlite3.DatabaseError("simulated custody commit ambiguity")

        monkeypatch.setattr(ready_bundles, "_commit_typed_ready_custody_once", inject)
        if fault == "third_state":
            with pytest.raises(InternalAudioStoreError, match="third state"):
                _publish(store, (replay,), ready, frontier=102)
            assert store._failed is True
            assert calls == 1
            return

        outcome = _publish(store, (replay,), ready, frontier=102)
        assert outcome.state is ReadyOutcomeState.PUBLISHED
        assert outcome.published[0].path.joinpath("records.bin").read_bytes() == _record(101, 2)
        assert calls == (2 if fault == "rolled_back" else 1)


def test_fully_covered_accepted_cohort_commits_custody_without_duplicate_output(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database = StateDB(tmp_path / "state.sqlite3")
    with InternalAudioStore(database) as store:
        ready = _ready_root(tmp_path / "ready")
        raw = (_record(100, 1),)
        source = _source(store, "a" * 32, 100, raw)
        _pause_after_cohort_acceptance(monkeypatch, store, source, ready, frontier=101)
        installed = _install_ready_custody(tmp_path, database, ready, raw, start=100)

        original = ready_bundles._commit_typed_ready_custody_once

        def commit_then_raise(audio_store: InternalAudioStore, commit: _CustodyCommit) -> None:
            original(audio_store, commit)
            raise sqlite3.DatabaseError("simulated full-custody commit ambiguity")

        monkeypatch.setattr(ready_bundles, "_commit_typed_ready_custody_once", commit_then_raise)
        outcome = _publish(store, (source,), ready, frontier=101)

        assert outcome.state is ReadyOutcomeState.WAITING
        assert outcome.reason == "already_published"
        assert tuple(path for path in ready.iterdir() if not path.name.startswith(".")) == (installed,)
        assert _source_state(database, source.source_id) == ("retiring", None, 1, 0)
        assert _cohort_count(database) == 0


def test_restart_clears_completed_accepted_cohort_before_reselecting_source(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database = StateDB(tmp_path / "state.sqlite3")
    with InternalAudioStore(database) as store:
        ready = _ready_root(tmp_path / "ready")
        source = _source(store, "a" * 32, 100, (_record(100, 1),))
        _pause_after_cohort_acceptance(monkeypatch, store, source, ready, frontier=101)
        with closing(database.open_connection()) as connection:
            connection.execute("BEGIN IMMEDIATE")
            connection.execute("UPDATE publication_cohort_member SET cursor=admitted_count")
            connection.commit()

        reconciled = _publish(store, (source,), ready, frontier=101)
        assert reconciled.state is ReadyOutcomeState.WAITING
        assert reconciled.reason == "already_published"
        assert _cohort_count(database) == 0

        published = _publish(store, (source,), ready, frontier=101)
        assert published.state is ReadyOutcomeState.PUBLISHED
        assert published.published[0].record_count == 1


@pytest.mark.parametrize(
    "gaps",
    (
        "not-a-list",
        [],
        [1],
        [[1]],
        [[True, 2]],
        [[0, 1]],
        [[1, 3]],
        [[1, 2], [1, 2]],
    ),
)
def test_publisher_rejects_untrusted_retired_ledger_gaps(tmp_path: Path, gaps: object) -> None:
    database = StateDB(tmp_path / "state.sqlite3")
    raw_digest = "a" * 64
    database.put(
        StateKind.READY_PUBLICATION,
        "b" * 64,
        {
            "records_sha256": "c" * 64,
            "state": "retired",
            "start_sequence": 0,
            "next_sequence": 3,
            "draft_raw_sha256": raw_digest,
            "gaps": gaps,
        },
    )
    with InternalAudioStore(database) as store:
        ready = _ready_root(tmp_path / "ready")
        with pytest.raises(ReadyBundleError, match="ledger is invalid"):
            _publish(store, (), ready, frontier=0)


def test_publisher_accepts_canonical_retired_gap_identity(tmp_path: Path) -> None:
    database = StateDB(tmp_path / "state.sqlite3")
    raw_digest = "a" * 64
    bundle_id = sha256(f"0:3:{raw_digest}:1-2".encode()).hexdigest()
    database.put(
        StateKind.READY_PUBLICATION,
        bundle_id,
        {
            "records_sha256": "c" * 64,
            "state": "retired",
            "start_sequence": 0,
            "next_sequence": 3,
            "draft_raw_sha256": raw_digest,
            "gaps": [[1, 2]],
        },
    )
    with InternalAudioStore(database) as store:
        outcome = _publish(store, (), _ready_root(tmp_path / "ready"), frontier=0)
    assert outcome.state is ReadyOutcomeState.WAITING
    assert outcome.reason == "waiting_for_threshold"


def _leave_committed_destination_with_stage_intent(
    monkeypatch: pytest.MonkeyPatch,
    store: InternalAudioStore,
    source: _Source,
    ready: Path,
) -> tuple[Path, Path]:
    original = ready_bundles._commit_typed_stage_once

    def stop_before_database_commit(*_args: object, **_kwargs: object) -> None:
        raise ReadyBundleError("test stop after ready rename")

    monkeypatch.setattr(ready_bundles, "_commit_typed_stage_once", stop_before_database_commit)
    with pytest.raises(ReadyBundleError, match="after ready rename"):
        _publish(store, (source,), ready, frontier=101)
    monkeypatch.setattr(ready_bundles, "_commit_typed_stage_once", original)
    with closing(store.state_db.open_connection(read_only=True)) as connection:
        row = cast(
            tuple[str],
            connection.execute("SELECT owned_path FROM ready_output_stage").fetchone(),
        )
    stage = Path(row[0])
    visible = tuple(path for path in ready.iterdir() if not path.name.startswith("."))
    assert len(visible) == 1
    destination = visible[0]
    assert not stage.exists()
    return stage, destination


@pytest.mark.parametrize("residue", ("missing", "empty", "raw"))
def test_restart_discards_only_safe_sql_owned_stage_residue(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    residue: str,
) -> None:
    database = StateDB(tmp_path / "state.sqlite3")
    with InternalAudioStore(database) as store:
        ready = _ready_root(tmp_path / "ready")
        source = _source(store, "a" * 32, 100, (_record(100, 1),))
        stage, destination = _leave_committed_destination_with_stage_intent(monkeypatch, store, source, ready)
        if residue != "missing":
            stage.mkdir()
            if residue == "raw":
                stage.joinpath("records.bin").write_bytes(b"partial")

        outcome = _publish(store, (source,), ready, frontier=101)

        assert outcome.state is ReadyOutcomeState.PUBLISHED
        assert not stage.exists()
        assert destination.joinpath("records.bin").read_bytes() == _record(100, 1)


@pytest.mark.parametrize("residue", ("foreign", "symlink"))
def test_restart_preserves_foreign_entries_at_sql_owned_stage_path(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    residue: str,
) -> None:
    database = StateDB(tmp_path / "state.sqlite3")
    with InternalAudioStore(database) as store:
        ready = _ready_root(tmp_path / "ready")
        source = _source(store, "a" * 32, 100, (_record(100, 1),))
        stage, _destination = _leave_committed_destination_with_stage_intent(monkeypatch, store, source, ready)
        if residue == "foreign":
            stage.mkdir()
            stage.joinpath("foreign-marker").write_bytes(b"keep")
            retained = stage / "foreign-marker"
        else:
            target = tmp_path / "foreign-target"
            target.mkdir()
            stage.symlink_to(target, target_is_directory=True)
            retained = target

        with pytest.raises(ReadyBundleError, match="unsafe"):
            _publish(store, (source,), ready, frontier=101)

        assert retained.exists()


def test_ready_conflict_after_acceptance_blocks_source_and_releases_cohort(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database = StateDB(tmp_path / "state.sqlite3")
    with InternalAudioStore(database) as store:
        ready = _ready_root(tmp_path / "ready")
        source = _source(store, "a" * 32, 100, (_record(100, 9),))
        _pause_after_cohort_acceptance(monkeypatch, store, source, ready, frontier=101)
        _install_ready_custody(tmp_path, database, ready, (_record(100, 1),), start=100)

        with pytest.raises(ReadyBundleError, match="conflict"):
            _publish(store, (source,), ready, frontier=101)

        state = _source_state(database, source.source_id)
        assert state is not None
        assert state[:2] == ("blocked", "ready_overlap_conflict")
        assert _cohort_count(database) == 0


def test_partial_retired_ready_overlap_blocks_without_full_source_lineage(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database = StateDB(tmp_path / "state.sqlite3")
    with InternalAudioStore(database) as store:
        ready = _ready_root(tmp_path / "ready")
        source = _source(store, "a" * 32, 100, (_record(100, 1), _record(101, 2)))
        _pause_after_cohort_acceptance(monkeypatch, store, source, ready, frontier=102)
        installed = _install_ready_custody(tmp_path, database, ready, (_record(101, 7),), start=101)
        for item_id, payload in database.list(StateKind.READY_PUBLICATION):
            if item_id == "frontier":
                continue
            assert isinstance(payload, dict)
            database.put(StateKind.READY_PUBLICATION, item_id, {**payload, "state": "retired"})
        shutil.rmtree(installed)

        with pytest.raises(ReadyBundleError, match="retired custody"):
            _publish(store, (source,), ready, frontier=102)

        state = _conflict_state(database, source.source_id)
        assert state is not None
        assert state[:8] == ("quarantine", "blocked", "ready_overlap_conflict", "unclassified", 0, None, None, None)
        source_state = _source_state(database, source.source_id)
        assert source_state is not None
        assert source_state[-1] == 2
        assert _cohort_count(database) == 0


@pytest.mark.parametrize("commit_then_raise", (False, True))
def test_ready_conflict_preserves_accepted_subthreshold_remainder(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    commit_then_raise: bool,
) -> None:
    database = StateDB(tmp_path / "state.sqlite3")
    with InternalAudioStore(database) as store:
        ready = _ready_root(tmp_path / "ready")
        conflict = _source(store, "a" * 32, 100, (_record(100, 9),))
        remainder = _source(store, "b" * 32, 101, (_record(101, 2),))
        original_resume = ready_bundles._resume_typed_cohort

        def stop_after_acceptance(*_args: object, **_kwargs: object) -> tuple[list[object], None]:
            raise ReadyBundleError("test stop after durable cohort acceptance")

        monkeypatch.setattr(ready_bundles, "_resume_typed_cohort", stop_after_acceptance)
        with pytest.raises(ReadyBundleError, match="durable cohort acceptance"):
            _publish_subthreshold(store, (conflict, remainder), ready, 102)
        monkeypatch.setattr(ready_bundles, "_resume_typed_cohort", original_resume)
        old_cohort, old_members = _active_cohort_snapshot(database)
        _install_ready_custody(tmp_path, database, ready, (_record(100, 1),), start=100)
        original_transaction = store.publication_transaction
        if commit_then_raise:

            @contextmanager
            def injected() -> Iterator[tuple[object, sqlite3.Connection]]:
                with original_transaction() as transaction:
                    yield transaction
                raise sqlite3.DatabaseError("simulated remainder commit ambiguity")

            monkeypatch.setattr(store, "publication_transaction", injected)

        with pytest.raises(ReadyBundleError, match="conflict"):
            _publish_subthreshold(store, (conflict, remainder), ready, 102)
        monkeypatch.setattr(store, "publication_transaction", original_transaction)

        new_cohort, new_members = _active_cohort_snapshot(database)
        assert new_cohort[0] != old_cohort[0]
        assert new_cohort[1] == "accepted"
        assert new_cohort[2] == new_cohort[0]
        assert new_cohort[3:] == old_cohort[3:]
        assert tuple(member[1] for member in old_members) == (conflict.source_id, remainder.source_id)
        assert new_members == ((0, remainder.source_id, *old_members[1][2:]),)
        assert _conflict_state(database, conflict.source_id) == (
            "quarantine",
            "blocked",
            "ready_overlap_conflict",
            "unclassified",
            0,
            None,
            None,
            None,
            None,
            None,
            None,
        )

        store.close()
        with InternalAudioStore(database) as restarted:
            outcome = _publish_subthreshold(restarted, (_Source(restarted, remainder.source_id),), ready, 102)

        assert outcome.state is ReadyOutcomeState.PUBLISHED
        assert outcome.published[0].record_count == 1
        assert _cohort_count(database) == 0


def test_partial_retired_overlap_preserves_subthreshold_remainder_after_restart(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database = StateDB(tmp_path / "state.sqlite3")
    with InternalAudioStore(database) as store:
        ready = _ready_root(tmp_path / "ready")
        conflict = _source(store, "a" * 32, 100, (_record(100, 1), _record(101, 9)))
        remainder = _source(store, "b" * 32, 102, (_record(102, 2),))
        original_resume = ready_bundles._resume_typed_cohort

        def stop_after_acceptance(*_args: object, **_kwargs: object) -> tuple[list[object], None]:
            raise ReadyBundleError("test stop after durable cohort acceptance")

        monkeypatch.setattr(ready_bundles, "_resume_typed_cohort", stop_after_acceptance)
        with pytest.raises(ReadyBundleError, match="durable cohort acceptance"):
            _publish_subthreshold(store, (conflict, remainder), ready, 103)
        monkeypatch.setattr(ready_bundles, "_resume_typed_cohort", original_resume)
        old_cohort, old_members = _active_cohort_snapshot(database)
        installed = _install_ready_custody(tmp_path, database, ready, (_record(101, 7),), start=101)
        for item_id, payload in database.list(StateKind.READY_PUBLICATION):
            if item_id != "frontier":
                assert isinstance(payload, dict)
                database.put(StateKind.READY_PUBLICATION, item_id, {**payload, "state": "retired"})
        shutil.rmtree(installed)

        with pytest.raises(ReadyBundleError, match="retired custody"):
            _publish_subthreshold(store, (conflict, remainder), ready, 103)

        new_cohort, new_members = _active_cohort_snapshot(database)
        assert new_cohort[0] != old_cohort[0]
        assert new_cohort[1] == "accepted"
        assert new_cohort[3:] == old_cohort[3:]
        assert new_members == ((0, remainder.source_id, *old_members[1][2:]),)
        conflict_state = _conflict_state(database, conflict.source_id)
        assert conflict_state is not None
        assert conflict_state[:8] == (
            "quarantine",
            "blocked",
            "ready_overlap_conflict",
            "unclassified",
            0,
            None,
            None,
            None,
        )
        source_state = _source_state(database, conflict.source_id)
        assert source_state is not None and source_state[-1] == 2

        store.close()
        with InternalAudioStore(database) as restarted:
            outcome = _publish_subthreshold(restarted, (_Source(restarted, remainder.source_id),), ready, 103)
        assert outcome.state is ReadyOutcomeState.PUBLISHED
        assert outcome.published[0].record_count == 1
        assert _cohort_count(database) == 0


def test_ready_conflict_remainder_rejects_changed_acceptance_time(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database = StateDB(tmp_path / "state.sqlite3")
    with InternalAudioStore(database) as store:
        ready = _ready_root(tmp_path / "ready")
        conflict = _source(store, "a" * 32, 100, (_record(100, 9),))
        remainder = _source(store, "b" * 32, 101, (_record(101, 2),))
        original_resume = ready_bundles._resume_typed_cohort

        def stop_after_acceptance(*_args: object, **_kwargs: object) -> tuple[list[object], None]:
            raise ReadyBundleError("test stop after durable cohort acceptance")

        monkeypatch.setattr(ready_bundles, "_resume_typed_cohort", stop_after_acceptance)
        with pytest.raises(ReadyBundleError, match="durable cohort acceptance"):
            _publish_subthreshold(store, (conflict, remainder), ready, 102)
        monkeypatch.setattr(ready_bundles, "_resume_typed_cohort", original_resume)
        _install_ready_custody(tmp_path, database, ready, (_record(100, 1),), start=100)
        original_transaction = store.publication_transaction

        @contextmanager
        def injected() -> Iterator[tuple[object, sqlite3.Connection]]:
            with original_transaction() as transaction:
                yield transaction
            with closing(database.open_connection()) as connection:
                connection.execute("BEGIN IMMEDIATE")
                connection.execute("UPDATE publication_cohort SET accepted_time='third-state'")
                connection.commit()
            raise sqlite3.DatabaseError("simulated conflict commit ambiguity")

        monkeypatch.setattr(store, "publication_transaction", injected)
        with pytest.raises(InternalAudioStoreError, match="third state"):
            _publish_subthreshold(store, (conflict, remainder), ready, 102)
        assert store._failed is True


@pytest.mark.parametrize("fault", ("commit_then_raise", "rollback", "third_state"))
def test_ready_conflict_disposition_accepts_only_exact_committed_state(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    fault: str,
) -> None:
    database = StateDB(tmp_path / "state.sqlite3")
    with InternalAudioStore(database) as store:
        ready = _ready_root(tmp_path / "ready")
        _install_ready_custody(tmp_path, database, ready, (_record(100, 1),), start=100)
        source = _source(store, "a" * 32, 100, (_record(100, 9),))
        original = store.publication_transaction

        @contextmanager
        def injected() -> Iterator[tuple[object, sqlite3.Connection]]:
            if fault == "rollback":
                with original() as transaction:
                    yield transaction
                    raise sqlite3.DatabaseError("simulated conflict rollback")
                return
            with original() as transaction:
                yield transaction
            if fault == "third_state":
                with closing(database.open_connection()) as connection:
                    connection.execute("BEGIN IMMEDIATE")
                    connection.execute(
                        "UPDATE capture_source SET terminal_reason='corrupt' WHERE source_id=?",
                        (source.source_id,),
                    )
                    connection.commit()
            raise sqlite3.DatabaseError("simulated conflict commit ambiguity")

        monkeypatch.setattr(store, "publication_transaction", injected)
        if fault == "third_state":
            with pytest.raises(InternalAudioStoreError, match="third state"):
                _publish(store, (source,), ready, frontier=101)
            assert store._failed is True
            state = _conflict_state(database, source.source_id)
            assert state is not None
            assert state[:5] == ("quarantine", "blocked", "ready_overlap_conflict", "unclassified", 0)
            assert state[5] == "corrupt"
        elif fault == "rollback":
            with pytest.raises(ReadyBundleError, match="rolled back"):
                _publish(store, (source,), ready, frontier=101)
            assert store._failed is False
            state = _source_state(database, source.source_id)
            assert state is not None
            assert state[:2] == ("sealed_waiting", None)
            assert _cohort_count(database) == 0
        else:
            with pytest.raises(ReadyBundleError, match="conflict"):
                _publish(store, (source,), ready, frontier=101)
            assert store._failed is False
            assert _conflict_state(database, source.source_id) == (
                "quarantine",
                "blocked",
                "ready_overlap_conflict",
                "unclassified",
                0,
                None,
                None,
                None,
                None,
                None,
                None,
            )
            assert _cohort_count(database) == 0


def test_cohort_fsm_refusal_leaves_acceptance_unwritten(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database = StateDB(tmp_path / "state.sqlite3")
    with InternalAudioStore(database) as store:
        ready = _ready_root(tmp_path / "ready")
        source = _source(store, "a" * 32, 100, (_record(100, 1),))
        original_state = _source_state(database, source.source_id)

        monkeypatch.setattr(
            ready_bundles,
            "cohort_transition",
            lambda phase, _event: CohortDecision(phase, CohortAction.REFUSE, "test refusal"),
        )

        with pytest.raises(ReadyBundleError, match="FSM refused QualifiedDrained"):
            _publish(store, (source,), ready, frontier=101)

        assert _source_state(database, source.source_id) == original_state
        assert _cohort_count(database) == 0
        assert not tuple(ready.iterdir())


def test_cohort_conflict_refusal_keeps_source_and_accepted_cohort(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database = StateDB(tmp_path / "state.sqlite3")
    with InternalAudioStore(database) as store:
        ready = _ready_root(tmp_path / "ready")
        source = _source(store, "a" * 32, 100, (_record(100, 9),))
        _pause_after_cohort_acceptance(monkeypatch, store, source, ready, frontier=101)
        _install_ready_custody(tmp_path, database, ready, (_record(100, 1),), start=100)
        original_transition = ready_bundles.cohort_transition

        def refuse_conflict(phase: CohortPhase, event: CohortEvent) -> CohortDecision:
            if isinstance(event, ready_bundles.CohortConflict):
                return CohortDecision(phase, CohortAction.REFUSE, "test refusal")
            return original_transition(phase, event)

        monkeypatch.setattr(ready_bundles, "cohort_transition", refuse_conflict)

        with pytest.raises(ReadyBundleError, match="FSM refused CohortConflict"):
            _publish(store, (source,), ready, frontier=101)

        state = _source_state(database, source.source_id)
        assert state is not None
        assert state[:2] == ("sealed_waiting", None)
        assert _cohort_count(database) == 1


def test_source_custody_refusal_leaves_exact_replay_uncovered(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database = StateDB(tmp_path / "state.sqlite3")
    with InternalAudioStore(database) as store:
        ready = _ready_root(tmp_path / "ready")
        raw = (_record(100, 1),)
        _install_ready_custody(tmp_path, database, ready, raw, start=100)
        source = _source(store, "a" * 32, 100, raw)
        original_transition = ready_bundles.transition

        def refuse_custody(phase: AudioPhase | None, event: AudioEvent) -> AudioDecision:
            if isinstance(event, ready_bundles.ReadyCustody):
                return AudioDecision(phase, AudioAction.REFUSE, "test refusal")
            return original_transition(phase, event)

        monkeypatch.setattr(ready_bundles, "transition", refuse_custody)

        with pytest.raises(ReadyBundleError, match="FSM refused ReadyCustody"):
            _publish(store, (source,), ready, frontier=101)

        assert _source_state(database, source.source_id) == ("sealed_waiting", None, None, 1)
        assert _cohort_count(database) == 0


def _prepare_partly_retired_source(
    tmp_path: Path,
    store: InternalAudioStore,
    database: StateDB,
) -> tuple[_Source, Path]:
    ready = _ready_root(tmp_path / "ready")
    source = _source(store, "a" * 32, 100, (_record(100, 1), _record(101, 2)))
    assert _publish(store, (source,), ready, frontier=102, page_limit=1).state is ReadyOutcomeState.PUBLISHED
    assert _source_state(database, source.source_id) == ("retiring", None, 1, 1)
    return source, ready


@pytest.mark.parametrize("fault", ("rolled_back", "committed", "third_state"))
def test_retirement_step_reconciles_only_exact_cursor_states(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    fault: str,
) -> None:
    database = StateDB(tmp_path / "state.sqlite3")
    with InternalAudioStore(database) as store:
        source, ready = _prepare_partly_retired_source(tmp_path, store, database)
        original = ready_bundles._retire_typed_source_once
        calls = 0

        def inject(audio_store: InternalAudioStore, step: _RetirementStep) -> _RetirementResult:
            nonlocal calls
            calls += 1
            if calls > 1:
                return original(audio_store, step)
            if fault == "rolled_back":
                raise sqlite3.DatabaseError("simulated retirement rollback")
            original(audio_store, step)
            if fault == "third_state":
                with closing(database.open_connection()) as connection:
                    connection.execute("BEGIN IMMEDIATE")
                    connection.execute(
                        "UPDATE capture_source SET retirement_cursor=0 WHERE source_id=?",
                        (source.source_id,),
                    )
                    connection.commit()
            raise sqlite3.DatabaseError("simulated retirement commit ambiguity")

        monkeypatch.setattr(ready_bundles, "_retire_typed_source_once", inject)
        if fault == "third_state":
            with pytest.raises(InternalAudioStoreError, match="third state"):
                _publish(store, (), ready, frontier=102, page_limit=1)
            assert store._failed is True
            assert calls == 1
            return

        outcome = _publish(store, (), ready, frontier=102, page_limit=1)
        assert outcome.retirement_pending is True
        assert _source_state(database, source.source_id) == ("retiring", None, 2, 0)
        assert calls == (2 if fault == "rolled_back" else 1)


def test_retirement_source_removal_reconciles_commit_then_raise(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database = StateDB(tmp_path / "state.sqlite3")
    with InternalAudioStore(database) as store:
        ready = _ready_root(tmp_path / "ready")
        source = _source(store, "a" * 32, 100, (_record(100, 1),))
        assert _publish(store, (source,), ready, frontier=101).state is ReadyOutcomeState.PUBLISHED
        assert _source_state(database, source.source_id) == ("retiring", None, 1, 0)
        original = ready_bundles._retire_typed_source_once

        def commit_then_raise(audio_store: InternalAudioStore, step: _RetirementStep) -> _RetirementResult:
            original(audio_store, step)
            raise sqlite3.DatabaseError("simulated retirement removal ambiguity")

        monkeypatch.setattr(ready_bundles, "_retire_typed_source_once", commit_then_raise)
        outcome = _publish(store, (), ready, frontier=101)

        assert outcome.retirement_pending is True
        assert _source_state(database, source.source_id) is None
