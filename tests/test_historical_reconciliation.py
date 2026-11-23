from __future__ import annotations

from contextlib import closing
from pathlib import Path

import pytest

from omi_collector.capture.adapters.collector_state_db import StateDB, StateKind
from omi_collector.capture.adapters.confirmed_loss import ConfirmedLossLedger
from omi_collector.capture.adapters.historical_reconciliation import (
    historical_decision,
    historical_loss_end,
)
from omi_collector.capture.adapters.internal_audio_store import InternalAudioStore
from test_historical_publication_order import _hold_historical_source
from test_historical_release_publication import _classify
from test_public_audio_v2_integration import _record, _source


def test_prefix_requires_its_existing_confirmed_loss_and_keeps_boundary_evidence(tmp_path: Path) -> None:
    database = StateDB(tmp_path / "state.sqlite3")
    with InternalAudioStore(database) as store:
        source = _source(store, "c" * 32, 10, (_record(10),))
        digest = source.source.committed_sha256
        with closing(database.open_connection()) as connection:
            connection.execute(
                "UPDATE capture_source SET source_type='quarantine',lifecycle='blocked',sequence_limit=?,"
                "block_reason='terminal prefix residual',quarantine_disposition='published_prefix',"
                "terminal_time='100',retention_deadline='200',certified_prefix_count=1,coverage_digest=? "
                "WHERE source_id=?",
                ((20).to_bytes(8, "big"), digest, source.source_id),
            )
            connection.commit()
        _hold_historical_source(database, source.source_id)
        ledger = ConfirmedLossLedger(database)
        ledger.initialize(allow_create=True)
        _classify(database)
        with closing(database.open_connection(read_only=True)) as connection:
            assert historical_decision(connection, source.source_id) == "unknown_held"
        database.delete(StateKind.HISTORICAL_RESOLUTION, source.source_id)
        ledger.record(source.source_id, 11, 13, "2026-01-01T00:00:00+00:00")
        _classify(database)
        with closing(database.open_connection()) as connection:
            assert historical_loss_end(connection, source.source_id) == 13
            connection.execute(
                "UPDATE capture_historical_custody SET disposition='admitted' WHERE source_id=?", (source.source_id,)
            )
            connection.execute(
                "UPDATE capture_source SET lifecycle='covered',coverage_digest=? WHERE source_id=?",
                ("0" * 64, source.source_id),
            )
            connection.commit()
            assert historical_decision(connection, source.source_id) == "legacy_custody"
            assert historical_loss_end(connection, source.source_id) == 13


def test_stale_snapshot_is_rejected_and_failure_rolls_back_all_decisions(tmp_path: Path) -> None:
    database = StateDB(tmp_path / "state.sqlite3")
    ConfirmedLossLedger(database).initialize(allow_create=True)
    with InternalAudioStore(database) as store:
        one = _source(store, "a" * 32, 10, (_record(10),))
        two = _source(store, "b" * 32, 20, (_record(20),))
        _hold_historical_source(database, one.source_id)
        _hold_historical_source(database, two.source_id)
        _classify(database)
        with closing(database.open_connection()) as connection:
            connection.execute(
                "UPDATE capture_source SET sequence_limit=? WHERE source_id=?", ((12).to_bytes(8, "big"), one.source_id)
            )
            connection.commit()
        with (
            closing(database.open_connection(read_only=True)) as connection,
            pytest.raises(ValueError, match="no longer matches"),
        ):
            historical_decision(connection, one.source_id)
