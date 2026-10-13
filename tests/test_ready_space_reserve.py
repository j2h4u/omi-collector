"""Publication space admission depends on bounded metadata work, not unrelated audio pages."""

from __future__ import annotations

from contextlib import closing
from hashlib import sha256
from pathlib import Path
from struct import pack
from typing import cast

import pytest

from omi_collector.capture.adapters import ready_bundles
from omi_collector.capture.adapters.clock_segments import ClockSegmentMap
from omi_collector.capture.adapters.collector_state_db import StateDB
from omi_collector.capture.adapters.internal_audio_store import (
    AUDIO_PAYLOAD_SIZE,
    AUDIO_RECORD_SIZE,
    DrainCheckpoint,
    InternalAudioStore,
)
from omi_collector.capture.adapters.ready_bundles import (
    ReadyOutcomeState,
    SqlitePublicationSource,
    finalize_typed_sources,
)
from omi_collector.config import ReadyConfig

_FILLER_RECORDS = 20_000
_FREE_ABOVE_HEADROOM = 5 * 1024 * 1024


def _record(timestamp: int, fill: int) -> bytes:
    return pack(">I", timestamp) + bytes((2, 8, fill)) + bytes(AUDIO_RECORD_SIZE - 7)


def _inflate_unrelated_audio(store: InternalAudioStore) -> None:
    source_id = "f" * 32
    store.create_source(source_id, "attempt", 0, _FILLER_RECORDS)
    payload = bytes(AUDIO_PAYLOAD_SIZE)
    with closing(store.state_db.open_connection()) as connection:
        connection.execute("BEGIN IMMEDIATE")
        connection.executemany(
            "INSERT INTO capture_record(source_id,ordinal,device_timestamp,payload) VALUES (?,?,?,?)",
            ((source_id, ordinal, ordinal, payload) for ordinal in range(_FILLER_RECORDS)),
        )
        connection.execute(
            "UPDATE capture_source SET lifecycle='covered',committed_count=?,committed_sha256=? WHERE source_id=?",
            (_FILLER_RECORDS, "0" * 64, source_id),
        )
        connection.commit()


def _sealed_source(store: InternalAudioStore) -> SqlitePublicationSource:
    source_id = "a" * 32
    raw = _record(100, 1)
    source = store.create_source(source_id, "attempt", 100, 101)
    source = store.prepare_leg(source_id, 100, 1)
    store.record_read_begin(source_id, 100, 1)
    store.commit_drain(source_id, 0, (raw,), DrainCheckpoint(source.current_leg_generation))
    committed = store.get_source(source_id)
    assert committed is not None
    store.seal_source(
        source_id,
        leg_generation=committed.current_leg_generation,
        expected_count=1,
        expected_sha256=sha256(raw).hexdigest(),
    )
    return SqlitePublicationSource(store, source_id)


def test_unrelated_large_audio_database_does_not_set_publication_journal_reserve(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    database = StateDB(tmp_path / "state.sqlite3")
    store = InternalAudioStore(database)
    _inflate_unrelated_audio(store)
    source = _sealed_source(store)
    ready = tmp_path / "ready"
    ready.mkdir()
    ready.chmod(0o2750)

    with closing(database.open_connection(read_only=True)) as connection:
        page_size = cast(int, connection.execute("PRAGMA page_size").fetchone()[0])
        page_count = cast(int, connection.execute("PRAGMA page_count").fetchone()[0])
    database_bytes = max(database.path.stat().st_size, page_size * page_count)
    available = ready_bundles._TYPED_SPACE_HEADROOM + _FREE_ABOVE_HEADROOM
    old_whole_database_reserve = (
        ready_bundles._TYPED_SPACE_HEADROOM
        + database_bytes
        + ready_bundles._TYPED_JOURNAL_MARGIN_PAGES * page_size
        + AUDIO_RECORD_SIZE
    )
    assert old_whole_database_reserve > available

    class _Available:
        f_frsize = 4096
        f_bavail = available // f_frsize

    monkeypatch.setattr(ready_bundles.os, "statvfs", lambda _path: _Available())
    outcome = finalize_typed_sources(
        store,
        (source,),
        ready,
        config=ReadyConfig(target_audio_seconds=0.02),
        clock_segments=ClockSegmentMap(()),
        frontier=101,
        drained=True,
    )

    assert outcome.state is ReadyOutcomeState.PUBLISHED
    assert outcome.published[0].path.joinpath("records.bin").read_bytes() == _record(100, 1)
    store.close()
