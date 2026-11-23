from __future__ import annotations

from contextlib import closing
from pathlib import Path

import pytest

from omi_collector.capture.adapters.collector_state_db import StateDB
from omi_collector.capture.adapters.internal_audio_store import InternalAudioStore
from omi_collector.capture.adapters.ready_bundles import (
    ReadyBundleError,
    ReadyOutcomeState,
    finalize_public_audio_v2,
    publication_contract_state,
)
from omi_collector.capture.adapters.ready_closures import load as load_closures
from omi_collector.capture.domain.clock_state import ClockState
from test_public_audio_v2_integration import _options, _ready_root, _record, _source
from test_publication_fsm import _store


def _hold_historical_source(database: StateDB, source_id: str) -> None:
    with closing(database.open_connection()) as connection:
        connection.execute(
            "INSERT INTO capture_historical_custody "
            "SELECT source_id,source_type,lifecycle,committed_count,committed_sha256,"
            "certified_prefix_count,coverage_digest,'pending' FROM capture_source WHERE source_id=?",
            (source_id,),
        )
        connection.commit()


def test_pending_history_holds_later_audio_but_allows_earlier_audio(tmp_path: Path) -> None:
    database = StateDB(tmp_path / "state.sqlite3")
    ready = _ready_root(tmp_path)
    with InternalAudioStore(database) as store:
        earlier = _source(store, "a" * 32, 10, (_record(10),))
        historical = _source(store, "b" * 32, 20, (_record(20),))
        later = _source(store, "c" * 32, 30, (_record(30),))
        _hold_historical_source(database, historical.source_id)
        with closing(database.open_connection()) as connection:
            connection.execute("DELETE FROM capture_read_leg WHERE source_id=?", (historical.source_id,))
            connection.commit()

        first = finalize_public_audio_v2(store, (earlier, historical, later), ready, _options(ClockState(), 31))
        assert first.state is ReadyOutcomeState.PUBLISHED
        assert [item.next_sequence for item in first.published] == [11]
        held = finalize_public_audio_v2(store, (historical, later), ready, _options(ClockState(), 31))
        assert held.published == ()
        assert publication_contract_state(database).next_public_order == 1
        retained = store.get_source(later.source_id)
        assert retained is not None and retained.lifecycle == "sealed_waiting"

        with closing(database.open_connection()) as connection:
            connection.execute("DELETE FROM capture_historical_custody WHERE source_id=?", (historical.source_id,))
            connection.commit()
        resumed = finalize_public_audio_v2(store, (historical, later), ready, _options(ClockState(), 31))
        assert [item.next_sequence for item in resumed.published] == [31]


def test_pending_history_keeps_drain_permit_for_later_publication(tmp_path: Path) -> None:
    staging = _store(tmp_path)
    database = staging.state_db
    with InternalAudioStore(database) as store:
        _source(store, "a" * 32, 10, (_record(10),))
        historical = _source(store, "b" * 32, 20, (_record(20),))
        _source(store, "c" * 32, 30, (_record(30),))
        _hold_historical_source(database, historical.source_id)
    closure = staging.append_ready_closure(31, "drained")

    first = staging.recover_and_publish()
    assert [item.next_sequence for item in first.published] == [11]
    assert load_closures(database) == (closure,)
    restarted = type(staging).from_paths(staging.paths, publication_root=tmp_path / "ready", config=staging._config)
    assert restarted.recover_and_publish().published == ()
    assert load_closures(database) == (closure,)
    with closing(database.open_connection()) as connection:
        connection.execute("DELETE FROM capture_historical_custody WHERE source_id=?", (historical.source_id,))
        connection.commit()
    restarted.publication_input_changed()
    resumed = restarted.recover_and_publish()
    assert [item.next_sequence for item in resumed.published] == [21, 31]


def test_previously_accepted_cohort_can_resume_before_pending_history(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from omi_collector.capture.adapters import ready_bundles

    database = StateDB(tmp_path / "state.sqlite3")
    ready = _ready_root(tmp_path)
    with InternalAudioStore(database) as store:
        source = _source(store, "a" * 32, 30, (_record(30),))
        original = ready_bundles._materialize_v2_stage_with_failure_latch

        def interrupt(*_args: object, **_kwargs: object) -> object:
            raise ReadyBundleError("injected interruption")

        monkeypatch.setattr(ready_bundles, "_materialize_v2_stage_with_failure_latch", interrupt)
        with pytest.raises(ReadyBundleError, match="injected interruption"):
            finalize_public_audio_v2(store, (source,), ready, _options(ClockState(), 31))
        historical = _source(store, "b" * 32, 20, (_record(20),))
        _hold_historical_source(database, historical.source_id)
        monkeypatch.setattr(ready_bundles, "_materialize_v2_stage_with_failure_latch", original)
        resumed = finalize_public_audio_v2(store, (historical,), ready, _options(ClockState(), 31))
        assert resumed.state is ReadyOutcomeState.PUBLISHED
        assert resumed.published[0].next_sequence == 31
