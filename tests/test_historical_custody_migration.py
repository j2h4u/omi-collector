from __future__ import annotations

import sqlite3
from contextlib import closing
from pathlib import Path

from omi_collector.capture.adapters.collector_state_db import StateDB
from omi_collector.capture.adapters.internal_audio_store import DrainCheckpoint, InternalAudioStore
from omi_collector.internal_audio_migration import initialize_collector_state
from omi_collector.storage_layout import _storage_layout


def test_v4_unpublished_sources_keep_separate_historical_custody(tmp_path: Path) -> None:
    layout = _storage_layout(tmp_path)
    initialize_collector_state(layout)
    db = StateDB(layout.collector.state_db)
    before_id, historical_id, backlog_id, active_id = (character * 32 for character in "abcd")
    with InternalAudioStore(db, chunk_limit=128) as store:
        for source_id, count in ((before_id, 135), (historical_id, 4), (backlog_id, 5), (active_id, 1)):
            store.create_source(source_id, "attempt", 0, count)
            store.prepare_leg(source_id, 0, count)
            store.record_read_begin(source_id, 0, count)
            for offset in range(0, count, 128):
                store.commit_drain(source_id, offset, (bytes(444),) * min(128, count - offset), DrainCheckpoint(1))
    with closing(sqlite3.connect(db.path)) as connection:
        connection.execute(
            "UPDATE capture_source SET lifecycle='sealed_waiting' WHERE source_id IN (?,?)", (before_id, backlog_id)
        )
        connection.execute(
            "UPDATE capture_source SET source_type='quarantine',lifecycle='blocked', "
            "quarantine_disposition='published_prefix',certified_prefix_count=committed_count, "
            "coverage_digest=committed_sha256 WHERE source_id=?",
            (historical_id,),
        )
        before = connection.execute(
            "SELECT source_id,source_type,lifecycle,committed_count,committed_sha256,certified_prefix_count,coverage_digest "
            "FROM capture_source ORDER BY source_id"
        ).fetchall()
        for table in (
            "capture_observation_chunk",
            "capture_observation",
            "capture_recovery_candidate",
            "capture_framing_incident",
            "capture_read_leg",
            "capture_historical_custody",
        ):
            connection.execute(f"DROP TABLE {table}")
        connection.execute("PRAGMA user_version=4")
        connection.commit()
    assert initialize_collector_state(layout) == {"schema_version": 5}
    with closing(sqlite3.connect(db.path)) as connection:
        custody = connection.execute(
            "SELECT source_id,prior_source_type,prior_lifecycle,prior_count,prior_sha256, "
            "prior_certified_count,prior_coverage_digest FROM capture_historical_custody ORDER BY source_id"
        ).fetchall()
        assert custody == before
        assert connection.execute("SELECT DISTINCT disposition FROM capture_historical_custody").fetchall() == [
            ("pending",)
        ]
        assert connection.execute("SELECT count(*) FROM capture_record").fetchone() == (145,)
        assert connection.execute("SELECT count(*) FROM capture_read_leg").fetchone() == (0,)
    with InternalAudioStore(db, chunk_limit=128) as store:
        new_id = "e" * 32
        store.create_source(new_id, "attempt", 200, 201)
        store.prepare_leg(new_id, 200, 1)
        store.record_read_begin(new_id, 200, 1)
        store.commit_checked_drain(new_id, 0, (bytes(444),), DrainCheckpoint(1))
        assert store.finish_read(new_id, 201, 444)
        source = store.get_source(new_id)
        assert source is not None
        store.seal_source(new_id, leg_generation=1, expected_count=1, expected_sha256=source.committed_sha256)
    from omi_collector.capture.adapters.ready_bundles import (
        SqlitePublicationSource,
        _typed_source_admissions,
        historical_admission_frontier,
    )
    from staging_test_factory import StagingStore

    staging = StagingStore(tmp_path / "spool", tmp_path / "capture", state_db=db)
    candidates = staging._typed_publication_source_ids()
    assert candidates == (before_id, historical_id, backlog_id, new_id)
    with InternalAudioStore(db, chunk_limit=128) as store:
        sources = tuple(SqlitePublicationSource(store, source_id) for source_id in candidates)
        admitted = _typed_source_admissions(store, sources, None)
        assert tuple(item.snapshot.source_id for item in admitted) == (new_id,)
    assert historical_admission_frontier(db, 201, frozenset(candidates)) == 0
    assert staging.pending_attempts() == ()
    with closing(sqlite3.connect(db.path)) as connection:
        assert connection.execute(
            "SELECT count(*) FROM capture_historical_custody WHERE source_id=?", (new_id,)
        ).fetchone() == (0,)


def test_v4_accepted_cohort_remains_admitted_after_migration(tmp_path: Path) -> None:
    from omi_collector.capture.adapters.ready_bundles import _v2_config_sha256
    from staging_test_factory import StagingStore

    layout = _storage_layout(tmp_path)
    initialize_collector_state(layout)
    db = StateDB(layout.collector.state_db)
    source_id, cohort_id = "e" * 32, "f" * 64
    with InternalAudioStore(db, chunk_limit=128) as store:
        store.create_source(source_id, "attempt", 0, 1)
        store.prepare_leg(source_id, 0, 1)
        store.record_read_begin(source_id, 0, 1)
        store.commit_drain(source_id, 0, (bytes(444),), DrainCheckpoint(1))
        source = store.get_source(source_id)
        assert source is not None
        digest = source.committed_sha256
    with closing(sqlite3.connect(db.path)) as connection:
        connection.execute("UPDATE capture_source SET lifecycle='sealed_waiting' WHERE source_id=?", (source_id,))
        connection.execute(
            "INSERT INTO publication_cohort(cohort_id,lifecycle,accepted_permit,drained_threshold,cursor,config_digest) "
            "VALUES (?,'accepted',?,1,0,?)",
            (cohort_id, cohort_id, _v2_config_sha256()),
        )
        connection.execute(
            "INSERT INTO publication_cohort_member(cohort_id,member_ordinal,source_id,slice_start,slice_count, "
            "admitted_lifecycle,admitted_count,admitted_sha256,cursor) VALUES (?,0,?,0,1,'sealed_waiting',1,?,0)",
            (cohort_id, source_id, digest),
        )
        for table in (
            "capture_observation_chunk",
            "capture_observation",
            "capture_recovery_candidate",
            "capture_framing_incident",
            "capture_read_leg",
            "capture_historical_custody",
        ):
            connection.execute(f"DROP TABLE {table}")
        connection.execute("PRAGMA user_version=4")
        connection.commit()
    assert initialize_collector_state(layout) == {"schema_version": 5}
    with closing(sqlite3.connect(db.path)) as connection:
        assert connection.execute(
            "SELECT disposition FROM capture_historical_custody WHERE source_id=?", (source_id,)
        ).fetchone() == ("admitted",)
        assert connection.execute("SELECT source_id FROM publication_cohort_member").fetchone() == (source_id,)
    staging = StagingStore(tmp_path / "spool", tmp_path / "capture", state_db=db)
    assert staging._typed_publication_source_ids() == (source_id,)
