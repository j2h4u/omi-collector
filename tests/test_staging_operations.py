from __future__ import annotations

from collections.abc import Iterable
from contextlib import closing
from pathlib import Path
from typing import cast

from omi_collector.capture.adapters.collector_state_db import StateDB
from omi_collector.capture.adapters.staging_store import StagingStore
from omi_collector.capture.domain.ring_protocol import ReadBeginNotification


def test_partial_only_drain_commits_tail_without_advancing_records(tmp_path: Path) -> None:
    spool = tmp_path / "spool"
    store = StagingStore(spool, tmp_path / "captures", state_db=StateDB(spool / "state.sqlite3"))
    attempt = store.prepare_streaming_attempt(100, 2)
    partial = b"incomplete record bytes"
    try:
        attempt.prepare_leg(100, 2)
        attempt.record_read_begin(ReadBeginNotification(100, 2))
        attempt.accept_chunk(100, b"", partial)

        assert attempt.durable_prefix.record_count == 0
        with closing(store.state_db.open_connection(read_only=True)) as connection:
            assert connection.execute("SELECT COUNT(*) FROM capture_record").fetchone()[0] == 0
            tail = tuple(
                cast(Iterable[object], cast(object, connection.execute("SELECT bytes FROM capture_tail").fetchone()))
            )
            assert tail == (partial,)
    finally:
        attempt.close()
