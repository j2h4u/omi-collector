from __future__ import annotations

import threading
from contextlib import closing
from hashlib import sha256
from pathlib import Path
from struct import pack
from typing import cast

from omi_collector.capture.adapters.collector_state_db import StateDB, StateKind
from omi_collector.capture.adapters.internal_audio_store import AUDIO_RECORD_SIZE, DrainCheckpoint, InternalAudioStore
from omi_collector.capture.adapters.sqlite_compaction import pending_compaction_generation, vacuum_pending


def test_compaction_ignores_retirement_blocked_by_active_cohort_member(tmp_path: Path) -> None:
    database = StateDB(tmp_path / "state.sqlite3")
    raw = pack(">I", 100) + bytes((2, 8, 1)) + bytes(AUDIO_RECORD_SIZE - 7)
    with InternalAudioStore(database) as store:
        store.create_source("a" * 32, "attempt", 100, 101)
        source = store.prepare_leg("a" * 32, 100, 1)
        source = store.record_read_begin("a" * 32, 100, 1)
        store.commit_drain("a" * 32, 0, (raw,), DrainCheckpoint(source.current_leg_generation))
        with closing(database.open_connection()) as connection:
            connection.execute("BEGIN IMMEDIATE")
            connection.execute(
                "UPDATE capture_source SET lifecycle='retiring',retirement_cursor=1 WHERE source_id=?",
                ("a" * 32,),
            )
            connection.execute("INSERT INTO publication_cohort(cohort_id,lifecycle) VALUES (?, 'staging')", ("b" * 32,))
            connection.execute(
                "INSERT INTO publication_cohort_member(cohort_id,member_ordinal,source_id,slice_start,slice_count,"
                "admitted_lifecycle,admitted_count,admitted_sha256) VALUES (?,0,?,0,1,'sealed_waiting',1,?)",
                ("b" * 32, "a" * 32, sha256(raw).hexdigest()),
            )
            connection.execute("CREATE TABLE reclaimable_fixture(payload BLOB)")
            connection.execute("INSERT INTO reclaimable_fixture VALUES (zeroblob(65536))")
            connection.execute("DROP TABLE reclaimable_fixture")
            connection.commit()

        with closing(database.open_connection(read_only=True)) as connection:
            row = cast(
                tuple[str, int] | None,
                connection.execute("SELECT lifecycle,retirement_cursor FROM capture_source").fetchone(),
            )
            assert row is not None and row[0] == "retiring" and row[1] == 1
        database.put(StateKind.COMPACTION, "current", {"generation": 6, "pending": True})
        assert pending_compaction_generation(database) == 6
        assert vacuum_pending(database, 6, threading.Event()) == "done"
        assert database.get(StateKind.COMPACTION, "current") == {"generation": 6, "pending": False}
