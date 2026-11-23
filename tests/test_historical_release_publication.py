from __future__ import annotations

from contextlib import closing
from pathlib import Path
from typing import cast

import pytest

from omi_collector.capture.adapters import ready_bundles
from omi_collector.capture.adapters.collector_state_db import StateDB, StateKind
from omi_collector.capture.adapters.confirmed_loss import ConfirmedLossLedger
from omi_collector.capture.adapters.historical_reconciliation import _candidate, _loss_facts, _rows
from omi_collector.capture.adapters.internal_audio_store import InternalAudioStore
from omi_collector.capture.adapters.ready_bundles import ReadyBundleError, ReadyOutcomeState, finalize_public_audio_v2
from omi_collector.capture.domain.clock_state import ClockState
from test_historical_publication_order import _hold_historical_source
from test_public_audio_v2_integration import _options, _ready_root, _record, _source


def _classify(db: StateDB, held: frozenset[str] = frozenset()) -> None:
    ConfirmedLossLedger(db).initialize(allow_create=True)
    with closing(db.open_connection(read_only=True)) as connection:
        decisions = tuple(
            _candidate(row, held, _loss_facts(connection)) for row in _rows(connection) if row[19] == "pending"
        )
    for decision in decisions:
        db.put(StateKind.HISTORICAL_RESOLUTION, decision.source_id, decision.payload())


def test_verified_legacy_seal_publishes_before_held_boundary_without_done(tmp_path: Path) -> None:
    db = StateDB(tmp_path / "state.sqlite3")
    ready = _ready_root(tmp_path)
    with InternalAudioStore(db) as store:
        early = _source(store, "a" * 32, 10, (_record(10),))
        held = _source(store, "b" * 32, 20, (_record(20),))
        later = _source(store, "c" * 32, 30, (_record(30),))
        for item in (early, held, later):
            _hold_historical_source(db, item.source_id)
        with closing(db.open_connection()) as connection:
            connection.execute("DELETE FROM capture_read_leg")
            connection.commit()
        _classify(db, frozenset({held.source_id}))
        outcome = finalize_public_audio_v2(store, (early, held, later), ready, _options(ClockState(), 31))
        assert outcome.state is ReadyOutcomeState.PUBLISHED
        assert [item.next_sequence for item in outcome.published] == [11]
        assert finalize_public_audio_v2(store, (held, later), ready, _options(ClockState(), 31)).published == ()
        with closing(db.open_connection(read_only=True)) as connection:
            rows = cast(
                list[tuple[str, str]],
                connection.execute(
                    "SELECT source_id,disposition FROM capture_historical_custody ORDER BY source_id"
                ).fetchall(),
            )
            assert [tuple(row) for row in rows] == [
                (early.source_id, "admitted"),
                (held.source_id, "pending"),
                (later.source_id, "pending"),
            ]
            count = cast(tuple[int], connection.execute("SELECT count(*) FROM capture_read_leg").fetchone())
            assert count[0] == 0


@pytest.mark.parametrize("interrupted_method", ["_resume_v2_cohort", "_materialize_v2_stage_with_failure_latch"])
def test_accepted_legacy_cohort_resumes_after_interruption(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, interrupted_method: str
) -> None:
    db = StateDB(tmp_path / "state.sqlite3")
    ready = _ready_root(tmp_path)
    with InternalAudioStore(db) as store:
        facts = ready_bundles._BundleFacts(0, 1, 1, "a" * 64, ())
        legacy_id = ready_bundles._bundle_id(facts)
        legacy = ready_bundles._ledger_entry("b" * 64, "retired", facts)
        db.put(StateKind.READY_PUBLICATION, legacy_id, legacy)
        earlier = _source(store, "e" * 32, 9, (_record(9),))
        assert finalize_public_audio_v2(store, (earlier,), ready, _options(ClockState(), 10)).published
        source = _source(store, "d" * 32, 10, (_record(10),))
        _hold_historical_source(db, source.source_id)
        with closing(db.open_connection()) as connection:
            connection.execute("DELETE FROM capture_read_leg WHERE source_id=?", (source.source_id,))
            connection.commit()
        _classify(db)
        original = (
            ready_bundles._resume_v2_cohort
            if interrupted_method == "_resume_v2_cohort"
            else ready_bundles._materialize_v2_stage_with_failure_latch
        )

        def interrupt(*_args: object, **_kwargs: object) -> object:
            raise ReadyBundleError("injected restart")

        monkeypatch.setattr(ready_bundles, interrupted_method, interrupt)
        with pytest.raises(ReadyBundleError, match="injected restart"):
            finalize_public_audio_v2(store, (source,), ready, _options(ClockState(), 11))
        monkeypatch.setattr(ready_bundles, interrupted_method, original)
        outcome = finalize_public_audio_v2(store, (), ready, _options(ClockState(), 11))
        assert outcome.state is ReadyOutcomeState.PUBLISHED
        assert len(outcome.published) == 1 and outcome.published[0].next_sequence == 11
        assert db.get(StateKind.READY_PUBLICATION, legacy_id) == legacy
        assert not finalize_public_audio_v2(store, (), ready, _options(ClockState(), 11)).published


def test_coverage_chain_stops_at_first_unproved_position() -> None:
    intervals = [(110, 115), (100, 103), (103, 106), (106, 110), (116, 120)]
    assert ready_bundles._continuous_coverage_end(103, 120, intervals) == 115
    assert ready_bundles._continuous_coverage_end(103, 110, intervals) == 110
    assert ready_bundles._continuous_coverage_end(103, 120, [*intervals, (115, 116)]) == 120


def test_legacy_prefix_stops_at_unproved_suffix_after_covered_loss(tmp_path: Path) -> None:
    from omi_collector.capture.adapters.ready_bundles import SqlitePublicationSource, historical_admission_frontier

    db = StateDB(tmp_path / "state.sqlite3")
    ready = _ready_root(tmp_path)
    ledger = ConfirmedLossLedger(db)
    ledger.initialize(allow_create=True)
    with InternalAudioStore(db) as store:
        prefix = _source(store, "e" * 32, 10, (_record(10),))
        next_source = _source(store, "f" * 32, 13, (_record(13), _record(14)))
        future = _source(store, "9" * 32, 16, (_record(16),))
        with closing(db.open_connection()) as connection:
            connection.execute(
                "UPDATE capture_source SET source_type='quarantine',lifecycle='blocked',sequence_limit=?,"
                "block_reason='terminal prefix residual',quarantine_disposition='published_prefix',"
                "terminal_time='100',retention_deadline='200',certified_prefix_count=1,coverage_digest=? "
                "WHERE source_id=?",
                ((20).to_bytes(8, "big"), prefix.source.committed_sha256, prefix.source_id),
            )
            connection.execute("DELETE FROM capture_read_leg WHERE source_id=?", (prefix.source_id,))
            connection.commit()
        _hold_historical_source(db, prefix.source_id)
        ledger.record(prefix.source_id, 11, 13, "2026-01-01T00:00:00+00:00")
        _classify(db)
        prefix = SqlitePublicationSource(store, prefix.source_id)
        assert (
            historical_admission_frontier(
                db, 20, frozenset({next_source.source_id, future.source_id, prefix.source_id})
            )
            == 15
        )
        result = finalize_public_audio_v2(store, (prefix, next_source, future), ready, _options(ClockState(), 20))
        assert result.state is ReadyOutcomeState.PUBLISHED
        assert all(item.next_sequence is not None for item in result.published)
        assert max(item.next_sequence for item in result.published if item.next_sequence is not None) == 15
        assert historical_admission_frontier(StateDB(db.path), 20) == 15
        assert not ready_bundles.retire_one_covered_typed_source(store, 8)
        assert finalize_public_audio_v2(store, (future,), ready, _options(ClockState(), 20)).published == ()
        bridge = _source(store, "8" * 32, 15, (_record(15),))
        last = _source(store, "7" * 32, 17, (_record(17), _record(18), _record(19)))
        assert not finalize_public_audio_v2(store, (future,), ready, _options(ClockState(), 20)).published
        assert (
            historical_admission_frontier(
                db, 20, frozenset({bridge.source_id, future.source_id, last.source_id, prefix.source_id})
            )
            == 20
        )
        completed = finalize_public_audio_v2(store, (bridge, future, last), ready, _options(ClockState(), 20))
        assert all(item.next_sequence is not None for item in completed.published)
        assert max(item.next_sequence for item in completed.published if item.next_sequence is not None) == 20
        assert historical_admission_frontier(StateDB(db.path), 20) == 20
        for _ in range(20):
            if not ready_bundles.retire_one_covered_typed_source(store, 8):
                break
        assert store.get_source(prefix.source_id) is None
        assert db.get(StateKind.HISTORICAL_RESOLUTION, prefix.source_id) is None


def test_admission_rolls_back_with_failed_cohort_acceptance(tmp_path: Path) -> None:
    db = StateDB(tmp_path / "state.sqlite3")
    ready = _ready_root(tmp_path)
    with InternalAudioStore(db) as store:
        source = _source(store, "6" * 32, 10, (_record(10),))
        _hold_historical_source(db, source.source_id)
        with closing(db.open_connection()) as connection:
            connection.execute("DELETE FROM capture_read_leg WHERE source_id=?", (source.source_id,))
            connection.execute(
                "CREATE TRIGGER reject_cohort BEFORE INSERT ON publication_cohort "
                "BEGIN SELECT RAISE(ABORT, 'forced rollback'); END"
            )
            connection.commit()
        _classify(db)
        with pytest.raises(ReadyBundleError):
            finalize_public_audio_v2(store, (source,), ready, _options(ClockState(), 11))
        with closing(db.open_connection(read_only=True)) as connection:
            row = cast(
                tuple[str],
                connection.execute(
                    "SELECT disposition FROM capture_historical_custody WHERE source_id=?", (source.source_id,)
                ).fetchone(),
            )
            assert row[0] == "pending"
            assert cast(tuple[int], connection.execute("SELECT count(*) FROM publication_cohort").fetchone())[0] == 0


def test_legacy_seal_uses_existing_replay_and_conflict_checks(tmp_path: Path) -> None:
    db = StateDB(tmp_path / "state.sqlite3")
    ready = _ready_root(tmp_path)
    records = (_record(100), _record(101, 2))
    with InternalAudioStore(db) as store:
        legacy = _source(store, "1" * 32, 10, records)
        _hold_historical_source(db, legacy.source_id)
        with closing(db.open_connection()) as connection:
            connection.execute("DELETE FROM capture_read_leg WHERE source_id=?", (legacy.source_id,))
            connection.commit()
        _classify(db)
        assert finalize_public_audio_v2(store, (legacy,), ready, _options(ClockState(), 12)).published
        replay = _source(store, "2" * 32, 10, records)
        assert not finalize_public_audio_v2(store, (replay,), ready, _options(ClockState(), 12)).published
        conflicting = _source(store, "3" * 32, 11, (_record(101, 9),))
        with pytest.raises(ReadyBundleError):
            finalize_public_audio_v2(store, (conflicting,), ready, _options(ClockState(), 12))


def test_damaged_legacy_payload_cannot_accept_cohort_or_advance(tmp_path: Path) -> None:
    db = StateDB(tmp_path / "state.sqlite3")
    ready = _ready_root(tmp_path)
    with InternalAudioStore(db) as store:
        broken = _source(store, "4" * 32, 10, (_record(10),))
        later = _source(store, "5" * 32, 11, (_record(11),))
        _hold_historical_source(db, broken.source_id)
        with closing(db.open_connection()) as connection:
            connection.execute("DELETE FROM capture_read_leg WHERE source_id=?", (broken.source_id,))
            connection.execute(
                "UPDATE capture_record SET payload=? WHERE source_id=?", (bytes([255]) * 440, broken.source_id)
            )
            connection.commit()
        _classify(db)
        with pytest.raises((ReadyBundleError, ValueError)):
            finalize_public_audio_v2(store, (broken, later), ready, _options(ClockState(), 12))
        with closing(db.open_connection(read_only=True)) as connection:
            assert cast(tuple[int], connection.execute("SELECT count(*) FROM publication_cohort").fetchone())[0] == 0
            assert (
                cast(
                    tuple[str],
                    connection.execute(
                        "SELECT disposition FROM capture_historical_custody WHERE source_id=?", (broken.source_id,)
                    ).fetchone(),
                )[0]
                == "pending"
            )
        assert not db.list(StateKind.READY_PUBLICATION)


def test_pending_legacy_full_replay_transitions_custody_with_coverage(tmp_path: Path) -> None:
    db = StateDB(tmp_path / "state.sqlite3")
    ready = _ready_root(tmp_path)
    records = (_record(10), _record(11))
    with InternalAudioStore(db) as store:
        original = _source(store, "a" * 32, 10, records)
        assert finalize_public_audio_v2(store, (original,), ready, _options(ClockState(), 12)).published
        replay = _source(store, "b" * 32, 10, records)
        _hold_historical_source(db, replay.source_id)
        with closing(db.open_connection()) as connection:
            connection.execute("DELETE FROM capture_read_leg WHERE source_id=?", (replay.source_id,))
            connection.commit()
        _classify(db)
        assert not finalize_public_audio_v2(store, (replay,), ready, _options(ClockState(), 12)).published
        with closing(db.open_connection(read_only=True)) as connection:
            row = cast(
                tuple[str, str],
                connection.execute(
                    "SELECT h.disposition,s.lifecycle FROM capture_historical_custody h "
                    "JOIN capture_source s ON s.source_id=h.source_id WHERE h.source_id=?",
                    (replay.source_id,),
                ).fetchone(),
            )
            assert tuple(row) == ("admitted", "retiring")
        assert ready_bundles.historical_admission_frontier(StateDB(db.path), 12) == 12


def test_crossing_candidate_cannot_bridge_to_later_audio_when_excluded(tmp_path: Path) -> None:
    db = StateDB(tmp_path / "state.sqlite3")
    ready = _ready_root(tmp_path)
    ledger = ConfirmedLossLedger(db)
    ledger.initialize(allow_create=True)
    with InternalAudioStore(db) as store:
        prefix = _source(store, "a" * 32, 10, (_record(10),))
        bridge = _source(store, "b" * 32, 13, tuple(_record(n) for n in range(13, 17)))
        after_gap = _source(store, "c" * 32, 15, (_record(15),))
        held = _source(store, "d" * 32, 16, (_record(16),))
        with closing(db.open_connection()) as connection:
            connection.execute(
                "UPDATE capture_source SET source_type='quarantine',lifecycle='blocked',sequence_limit=?,"
                "block_reason='terminal prefix residual',quarantine_disposition='published_prefix',"
                "terminal_time='100',retention_deadline='200',certified_prefix_count=1,coverage_digest=? "
                "WHERE source_id=?",
                ((20).to_bytes(8, "big"), prefix.source.committed_sha256, prefix.source_id),
            )
            connection.execute("DELETE FROM capture_read_leg WHERE source_id=?", (prefix.source_id,))
            connection.commit()
        _hold_historical_source(db, prefix.source_id)
        _hold_historical_source(db, held.source_id)
        ledger.record(prefix.source_id, 11, 13, "2026-01-01T00:00:00+00:00")
        _classify(db, frozenset({held.source_id}))
        prefix = ready_bundles.SqlitePublicationSource(store, prefix.source_id)
        sources = (prefix, bridge, after_gap, held)
        assert ready_bundles.historical_admission_frontier(db, 20, frozenset(s.source_id for s in sources)) == 13
        result = finalize_public_audio_v2(store, sources, ready, _options(ClockState(), 20))
        assert [item.next_sequence for item in result.published] == [11]
        retained = store.get_source(after_gap.source_id)
        assert retained is not None and retained.lifecycle == "sealed_waiting"


def test_corrupt_loss_ledger_fails_closed_before_publication(tmp_path: Path) -> None:
    db = StateDB(tmp_path / "state.sqlite3")
    ready = _ready_root(tmp_path)
    with InternalAudioStore(db) as store:
        legacy = _source(store, "e" * 32, 10, (_record(10),))
        _hold_historical_source(db, legacy.source_id)
        _classify(db)
        db.put(StateKind.CONFIRMED_LOSS, "ledger", {"invalid": True})
        with pytest.raises(ReadyBundleError, match="historical loss evidence"):
            finalize_public_audio_v2(store, (legacy,), ready, _options(ClockState(), 11))
        with closing(db.open_connection(read_only=True)) as connection:
            row = cast(
                tuple[str],
                connection.execute(
                    "SELECT disposition FROM capture_historical_custody WHERE source_id=?", (legacy.source_id,)
                ).fetchone(),
            )
            assert row[0] == "pending"
        assert not db.list(StateKind.READY_PUBLICATION)
