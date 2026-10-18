from __future__ import annotations

import json
import sqlite3
from collections.abc import Iterable, Iterator
from contextlib import closing
from pathlib import Path
from struct import pack
from typing import cast

import pytest

from omi_collector.capture.adapters.clock_segments import ClockSegmentMap
from omi_collector.capture.adapters.collector_state_db import StateDB
from omi_collector.capture.adapters.internal_audio_store import AUDIO_RECORD_SIZE, DrainCheckpoint, InternalAudioStore
from omi_collector.capture.adapters.ready_bundles import (
    ReadyBundleError,
    ReadyOutcome,
    ReadyOutcomeState,
    SqlitePublicationSource,
    _proof_page_scratch,
    _replay_proof_page_values,
    finalize_typed_sources,
    retire_acknowledged,
)
from omi_collector.capture.application.storage_fault import FatalInternalAudioStorageError
from omi_collector.capture.domain.replay_proof import unpack_page_index
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
    ready.mkdir()
    ready.chmod(0o2750)
    return ready


def _audio_record(timestamp: int, marker: int = 1) -> bytes:
    return pack(">I", timestamp) + bytes((2, 8, marker)) + bytes(AUDIO_RECORD_SIZE - 7)


def _zero_audio_record(timestamp: int) -> bytes:
    return pack(">I", timestamp) + bytes(AUDIO_RECORD_SIZE - 4)


def _source(
    store: InternalAudioStore, source_id: str, start: int, records: tuple[bytes, ...]
) -> SqlitePublicationSource:
    store.create_source(source_id, "attempt", start, start + len(records))
    source = store.prepare_leg(source_id, start, len(records))
    source = store.record_read_begin(source_id, start, len(records))
    for offset in range(0, len(records), store.chunk_limit):
        store.commit_drain(
            source_id,
            offset,
            records[offset : offset + store.chunk_limit],
            DrainCheckpoint(source.current_leg_generation),
        )
    source = store.get_source(source_id)
    assert source is not None
    store.seal_source(
        source_id,
        leg_generation=source.current_leg_generation,
        expected_count=source.committed_count,
        expected_sha256=source.committed_sha256,
    )
    return SqlitePublicationSource(store, source_id)


def _publish(
    store: InternalAudioStore,
    sources: tuple[SqlitePublicationSource, ...],
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


def _row_values(row: Iterable[object]) -> tuple[object, ...]:
    return tuple(row)


def test_proof_page_scratch_spills_to_file_and_closes_on_error() -> None:
    record_count = 10_000
    records = (_zero_audio_record(index) for index in range(record_count))
    scratch_connection: sqlite3.Connection | None = None

    with (
        pytest.raises(RuntimeError, match="leave scratch"),
        _proof_page_scratch(records, record_count=record_count) as (page_index, scratch),
    ):
        scratch_connection = scratch
        assert scratch.execute("PRAGMA temp_store").fetchone() == (1,)
        assert scratch.execute("PRAGMA temp.cache_size").fetchone() == (-256,)

        indexed_pages = unpack_page_index(page_index, record_count=record_count)
        stored_pages = tuple(_replay_proof_page_values(scratch))
        assert len(stored_pages) == len(indexed_pages)
        assert tuple((row[1], row[2], row[4]) for row in stored_pages) == indexed_pages
        raise RuntimeError("leave scratch")

    assert scratch_connection is not None
    with pytest.raises(sqlite3.ProgrammingError, match="closed database"):
        scratch_connection.execute("SELECT 1")


def _row_counts(database: StateDB, source_id: str) -> tuple[int, int]:
    with closing(database.open_connection(read_only=True)) as connection:
        row = cast(
            Iterable[object],
            connection.execute(
                "SELECT committed_count,(SELECT count(*) FROM capture_record WHERE source_id=?) "
                "FROM capture_source WHERE source_id=?",
                (source_id, source_id),
            ).fetchone(),
        )
        return cast(tuple[int, int], _row_values(row))


def _retained_record_count(database: StateDB, source_id: str) -> int:
    with closing(database.open_connection(read_only=True)) as connection:
        row = cast(
            tuple[int],
            _row_values(
                cast(
                    Iterable[object],
                    connection.execute(
                        "SELECT count(*) FROM capture_record WHERE source_id=?", (source_id,)
                    ).fetchone(),
                )
            ),
        )
    return row[0]


def _source_status(database: StateDB, source_id: str) -> tuple[str, str | None]:
    with closing(database.open_connection(read_only=True)) as connection:
        row = connection.execute("SELECT lifecycle,block_reason FROM capture_source WHERE source_id=?", (source_id,))
        return cast(tuple[str, str | None], _row_values(cast(Iterable[object], row.fetchone())))


def test_publication_proves_all_130_physical_records_including_zero_audio(
    tmp_path: Path, audio_store: tuple[StateDB, InternalAudioStore]
) -> None:
    database, store = audio_store
    records = (_audio_record(100), *(_zero_audio_record(101 + index) for index in range(129)))
    source = _source(store, "a" * 32, 100, records)
    ready = _ready_root(tmp_path)

    outcome = _publish(store, (source,), ready, frontier=230)

    assert outcome.state is ReadyOutcomeState.PUBLISHED
    assert len(outcome.published) == 1
    bundle = outcome.published[0]
    assert bundle.record_count == 130
    assert (bundle.path / "records.bin").read_bytes() == b"".join(records)
    with closing(database.open_connection(read_only=True)) as connection:
        root_row = cast(
            Iterable[object],
            connection.execute(
                "SELECT record_count FROM ready_replay_proof WHERE bundle_id=?", (bundle.bundle_id,)
            ).fetchone(),
        )
        assert _row_values(root_row) == (130,)
        pages = cast(
            list[Iterable[object]],
            connection.execute(
                "SELECT first_record_index,record_count FROM ready_replay_proof_page "
                "WHERE bundle_id=? ORDER BY page_ordinal",
                (bundle.bundle_id,),
            ).fetchall(),
        )
        assert [_row_values(row) for row in pages] == [(0, 128), (128, 2)]


def test_partial_cross_page_replay_uses_private_proof_after_capture_rows_retire(
    tmp_path: Path, audio_store: tuple[StateDB, InternalAudioStore], monkeypatch: pytest.MonkeyPatch
) -> None:
    database, store = audio_store
    records = tuple(_audio_record(100 + index, index % 251 + 1) for index in range(260))
    source = _source(store, "a" * 32, 100, records)
    ready = _ready_root(tmp_path)
    published = _publish(store, (source,), ready, frontier=360)
    assert published.state is ReadyOutcomeState.PUBLISHED
    assert published.custody_cache is not None

    retirement = published
    for _ in range(5):
        if not retirement.retirement_pending:
            break
        retirement = _publish(
            store,
            (),
            ready,
            frontier=360,
        )
    else:
        pytest.fail("published source rows did not finish retiring")
    assert _retained_record_count(database, source.source_id) == 0
    authenticated = _publish(store, (), ready, frontier=360)
    assert authenticated.custody_cache is not None

    replay = _source(store, "b" * 32, 227, records[127:130])
    bundle_records = published.published[0].path / "records.bin"
    import omi_collector.capture.adapters.ready_bundles as ready_bundles

    original_digest = ready_bundles._digest
    statements: list[str] = []
    original_connection = StateDB.open_connection

    def reject_bundle_records_digest(path: Path) -> str:
        if path == bundle_records:
            raise AssertionError("partial replay opened public records.bin")
        return original_digest(path)

    def trace_connections(state_db: StateDB, *, read_only: bool = False) -> sqlite3.Connection:
        connection = original_connection(state_db, read_only=read_only)
        connection.set_trace_callback(statements.append)
        return connection

    monkeypatch.setattr(ready_bundles, "_digest", reject_bundle_records_digest)
    monkeypatch.setattr(StateDB, "open_connection", trace_connections)
    outcome = finalize_typed_sources(
        store,
        (replay,),
        ready,
        config=ReadyConfig(target_audio_seconds=1.0),
        clock_segments=ClockSegmentMap(()),
        frontier=360,
        drained=True,
        custody_cache=authenticated.custody_cache,
    )

    page_reads = [line for line in statements if "FROM ready_replay_proof_page WHERE bundle_id=" in line]
    assert outcome.state is ReadyOutcomeState.WAITING
    assert outcome.reason == "waiting_for_threshold"
    assert len(page_reads) == 2
    assert any("page_ordinal=0" in line for line in page_reads)
    assert any("page_ordinal=1" in line for line in page_reads)
    assert all("page_ordinal=2" not in line for line in page_reads)


@pytest.mark.parametrize("mismatch", ("normalized_header", "padding"))
def test_active_replay_conflicts_on_changed_normalized_header_or_padding(
    tmp_path: Path, audio_store: tuple[StateDB, InternalAudioStore], mismatch: str
) -> None:
    database, store = audio_store
    ready = _ready_root(tmp_path)
    original = _audio_record(100)
    published = _publish(store, (_source(store, "a" * 32, 100, (original,)),), ready, frontier=101)
    assert published.state is ReadyOutcomeState.PUBLISHED

    candidate = bytearray(original)
    if mismatch == "normalized_header":
        candidate[:4] = pack(">I", 101)
    else:
        candidate[-1] = 1
    replay = _source(store, "b" * 32, 100, (bytes(candidate),))

    with pytest.raises(ReadyBundleError, match="conflict"):
        _publish(store, (replay,), ready, frontier=101)
    assert _source_status(database, replay.source_id) == ("blocked", "ready_overlap_conflict")


@pytest.mark.parametrize("damage", ("missing_root", "missing_page", "corrupt_page"))
def test_broken_active_proof_fails_closed_as_storage_error(
    tmp_path: Path, audio_store: tuple[StateDB, InternalAudioStore], damage: str
) -> None:
    database, store = audio_store
    ready = _ready_root(tmp_path)
    record = _audio_record(100)
    published = _publish(store, (_source(store, "a" * 32, 100, (record,)),), ready, frontier=101)
    proof_cache = _publish(store, (), ready, frontier=101).custody_cache
    assert proof_cache is not None
    replay = _source(store, "b" * 32, 100, (record,))
    with closing(database.open_connection()) as connection, database.access(connection):
        if damage == "missing_root":
            connection.execute("DELETE FROM ready_replay_proof WHERE bundle_id=?", (published.published[0].bundle_id,))
        elif damage == "missing_page":
            connection.execute(
                "DELETE FROM ready_replay_proof_page WHERE bundle_id=? AND page_ordinal=0",
                (published.published[0].bundle_id,),
            )
        else:
            connection.execute(
                "UPDATE ready_replay_proof_page SET hashes=zeroblob(length(hashes)) "
                "WHERE bundle_id=? AND page_ordinal=0",
                (published.published[0].bundle_id,),
            )
        connection.commit()

    with pytest.raises(FatalInternalAudioStorageError, match="private replay proof"):
        finalize_typed_sources(
            store,
            (replay,),
            ready,
            config=ReadyConfig(target_audio_seconds=0.02),
            clock_segments=ClockSegmentMap(()),
            frontier=101,
            drained=True,
            custody_cache=proof_cache,
        )
    assert replay.source.lifecycle == "sealed_waiting"
    assert _row_counts(database, replay.source_id) == (1, 1)


def test_ack_deletes_private_proof_idempotently_without_revoking_pending_capture_gc(
    tmp_path: Path, audio_store: tuple[StateDB, InternalAudioStore]
) -> None:
    database, store = audio_store
    ready = _ready_root(tmp_path)
    records = (_audio_record(100), _audio_record(101, 2))
    source = _source(store, "a" * 32, 100, records)
    published = _publish(store, (source,), ready, frontier=102, page_limit=1)
    assert published.state is ReadyOutcomeState.PUBLISHED
    assert published.retirement_pending
    assert _row_counts(database, source.source_id) == (2, 1)
    bundle = published.published[0]
    checkpoint = tmp_path / "checkpoint.json"
    checkpoint.write_text(
        json.dumps(
            {
                "analysis_cursor": None,
                "vad_decisions": [],
                "open_speech_tail": None,
                "acknowledged": [{"bundle_id": bundle.bundle_id, "records_sha256": bundle.records_sha256}],
            }
        ),
        encoding="utf-8",
    )

    retired = retire_acknowledged(ready, database, checkpoint)
    assert retired[0].bundle_id == bundle.bundle_id
    assert not bundle.path.exists()
    with closing(database.open_connection(read_only=True)) as connection:
        proof_row = cast(
            Iterable[object],
            connection.execute(
                "SELECT count(*) FROM ready_replay_proof WHERE bundle_id=?", (bundle.bundle_id,)
            ).fetchone(),
        )
        page_row = cast(
            Iterable[object],
            connection.execute(
                "SELECT count(*) FROM ready_replay_proof_page WHERE bundle_id=?", (bundle.bundle_id,)
            ).fetchone(),
        )
        source_row = cast(
            Iterable[object],
            connection.execute(
                "SELECT lifecycle,coverage_digest=committed_sha256 FROM capture_source WHERE source_id=?",
                (source.source_id,),
            ).fetchone(),
        )
        assert _row_values(proof_row) == (0,)
        assert _row_values(page_row) == (0,)
        assert _row_values(source_row) == ("retiring", 1)

    assert retire_acknowledged(ready, database, checkpoint)[0].bundle_id == bundle.bundle_id
    cleanup = published
    for _ in range(4):
        if not cleanup.retirement_pending:
            break
        cleanup = _publish(store, (), ready, frontier=102, page_limit=1)
    else:
        pytest.fail("authorized capture coverage did not finish garbage collection after ACK")
    assert _retained_record_count(database, source.source_id) == 0
