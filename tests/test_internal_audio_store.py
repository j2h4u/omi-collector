from __future__ import annotations

import hashlib
import sqlite3
from collections.abc import Iterable
from contextlib import closing
from pathlib import Path
from types import SimpleNamespace
from typing import cast

import pytest

from omi_collector.capture.adapters import internal_audio_store
from omi_collector.capture.adapters.collector_state_db import StateDB, StateDBError, StateKind, install_audio_schema
from omi_collector.capture.adapters.internal_audio_store import (
    AUDIO_RECORD_SIZE,
    CaptureTail,
    DrainCheckpoint,
    InternalAudioStore,
    InternalAudioStoreError,
    capture_space_requirement,
    decode_uint64,
    encode_uint64,
    validate_source_stream,
)


def test_capture_space_reserve_uses_total_growth_and_one_bounded_journal(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    db = StateDB(tmp_path / "state.sqlite3")
    monkeypatch.setattr(
        internal_audio_store.os,
        "statvfs",
        lambda _path: SimpleNamespace(f_bavail=321, f_frsize=4096),
    )
    with closing(db.open_connection(read_only=True)) as connection:
        page_size = cast(int, connection.execute("PRAGMA page_size").fetchone()[0])
        freelist = cast(int, connection.execute("PRAGMA freelist_count").fetchone()[0])

    required, available = capture_space_requirement(db, 256, 128)

    assert required == max(0, 256 * 640 - freelist * page_size) + 128 * 1024 + 16 * page_size
    assert available == 321 * 4096


def _record(timestamp: int, fill: int) -> bytes:
    return timestamp.to_bytes(4, "big") + bytes([fill]) * 440


def _source(store: InternalAudioStore, *, limit: int = 100, source_id: str = "a" * 32) -> str:
    store.create_source(source_id, "attempt", 0, limit)
    store.prepare_leg(source_id, 0, limit)
    store.record_read_begin(source_id, 0, limit)
    return source_id


def test_fresh_database_is_v2_and_has_typed_tables(tmp_path: Path) -> None:
    path = tmp_path / "state.sqlite3"
    StateDB(path)
    connection = sqlite3.connect(path)
    assert connection.execute("PRAGMA user_version").fetchone()[0] == 2
    table_rows = cast(
        list[tuple[str]],
        cast(object, connection.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()),
    )
    names = {row[0] for row in table_rows}
    assert names == {
        "state",
        "capture_source",
        "capture_record",
        "capture_tail",
        "capture_conflict",
        "capture_evidence",
        "capture_evidence_chunk",
        "publication_cohort",
        "publication_cohort_member",
        "ready_output_stage",
        "ready_stage_range",
    }
    connection.close()


def test_existing_v1_is_refused_without_mutation(tmp_path: Path) -> None:
    path = tmp_path / "v1.sqlite3"
    connection = sqlite3.connect(path)
    connection.execute(
        "CREATE TABLE state (kind TEXT NOT NULL, id TEXT NOT NULL, payload TEXT NOT NULL, PRIMARY KEY(kind, id)) WITHOUT ROWID"
    )
    connection.execute("INSERT INTO state VALUES ('clock', 'current', '{}')")
    connection.execute("PRAGMA user_version = 1")
    connection.commit()
    connection.close()
    before = path.read_bytes()

    with pytest.raises(StateDBError, match="unsupported"):
        StateDB(path)

    assert path.read_bytes() == before
    check = sqlite3.connect(path)
    assert check.execute("PRAGMA user_version").fetchone() == (1,)
    assert check.execute("SELECT name FROM sqlite_master WHERE name = 'capture_source'").fetchone() is None
    check.close()


def test_missing_canonical_database_is_not_recreated_after_initialization(tmp_path: Path) -> None:
    path = tmp_path / "state.sqlite3"
    db = StateDB(path)
    path.unlink()

    with pytest.raises(StateDBError):
        db.get_generation()
    assert not path.exists()


def test_state_connections_refuse_symlink_swap(tmp_path: Path) -> None:
    path = tmp_path / "state.sqlite3"
    db = StateDB(path)
    target = tmp_path / "target.sqlite3"
    target.write_bytes(path.read_bytes())
    path.unlink()
    path.symlink_to(target)
    with pytest.raises(StateDBError, match="symlink"):
        db.open_connection()
    assert path.is_symlink() and path.resolve() == target


def test_v2_validation_rejects_unexpected_tables(tmp_path: Path) -> None:
    path = tmp_path / "state.sqlite3"
    StateDB(path)
    connection = sqlite3.connect(path)
    connection.execute("CREATE TABLE unexpected (value TEXT)")
    connection.commit()
    connection.close()
    with pytest.raises(StateDBError, match=r"unsupported|unexpected"):
        StateDB(path)


@pytest.mark.parametrize("value", [0, 1, 2**63 - 1, 2**63, 2**64 - 1])
def test_uint64_codec_round_trips_in_lexicographic_order(value: int) -> None:
    encoded = encode_uint64(value)
    assert len(encoded) == 8
    assert decode_uint64(encoded) == value


def test_uint64_codec_refuses_out_of_range_values() -> None:
    with pytest.raises(InternalAudioStoreError):
        encode_uint64(-1)
    with pytest.raises(InternalAudioStoreError):
        encode_uint64(2**64)
    with pytest.raises(InternalAudioStoreError):
        decode_uint64(b"short")


def test_exact_drain_checkpoint_tail_and_page_bounds(tmp_path: Path) -> None:
    db = StateDB(tmp_path / "state.sqlite3")
    first = _record(0, 1)
    second = _record(2**32 - 1, 2)
    assert len(first) == AUDIO_RECORD_SIZE
    digest = hashlib.sha256(first + second).hexdigest()
    with InternalAudioStore(db, chunk_limit=2) as store:
        source_id = _source(store)
        committed = store.commit_drain(
            source_id, 0, [first, second], DrainCheckpoint(1, CaptureTail(1, 2, 892, 888, b"tail"), digest)
        )
        assert committed == 2
        assert store.get_source(source_id).committed_count == 2  # type: ignore[union-attr]
        page = store.read_record_page(source_id, limit=2)
        assert tuple(record.raw_bytes for record in page) == (first, second)
        with pytest.raises(InternalAudioStoreError, match="limit"):
            store.read_record_page(source_id, limit=3)

    connection = sqlite3.connect(db.path)
    assert connection.execute("SELECT device_timestamp FROM capture_record ORDER BY ordinal").fetchall() == [
        (0,),
        (2**32 - 1,),
    ]
    assert connection.execute("SELECT length(bytes) FROM capture_tail").fetchone() == (4,)
    connection.close()


def test_create_ambiguity_reconciles_commit_and_rollback(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    db = StateDB(tmp_path / "state.sqlite3")
    with InternalAudioStore(db) as store:
        commit = store._commit

        def committed_then_ambiguous() -> None:
            commit()
            raise sqlite3.OperationalError("commit result is ambiguous")

        monkeypatch.setattr(store, "_commit", committed_then_ambiguous)
        source = store.create_source("b" * 32, "draft", 0, 3)
        assert source.source_id == "b" * 32

        def rolled_back() -> None:
            raise sqlite3.OperationalError("commit result is ambiguous")

        monkeypatch.setattr(store, "_commit", rolled_back)
        with pytest.raises(InternalAudioStoreError, match="rolled back"):
            store.create_source("c" * 32, "draft", 0, 3)


def test_drain_replay_is_idempotent_and_mismatch_is_refused(tmp_path: Path) -> None:
    db = StateDB(tmp_path / "state.sqlite3")
    raw = _record(10, 4)
    digest = hashlib.sha256(raw).hexdigest()
    with InternalAudioStore(db) as store:
        source_id = _source(store)
        assert store.commit_drain(source_id, 0, [raw], DrainCheckpoint(1, expected_sha256=digest)) == 1
        assert store.commit_drain(source_id, 0, [raw], DrainCheckpoint(1, expected_sha256=digest)) == 1
        with pytest.raises(InternalAudioStoreError, match="conflicts"):
            store.commit_drain(source_id, 0, [_record(10, 5)], DrainCheckpoint(1, expected_sha256=digest))
        blocked = store.get_source(source_id)
        assert blocked is not None and blocked.lifecycle == "blocked"
    conflict = sqlite3.connect(db.path)
    assert conflict.execute("SELECT length(bytes), reason FROM capture_conflict").fetchone() == (444, "replay_mismatch")
    conflict.close()


def test_drain_accepts_overlapping_prefix_and_appends_suffix(tmp_path: Path) -> None:
    db = StateDB(tmp_path / "state.sqlite3")
    first, second, third = _record(1, 1), _record(2, 2), _record(3, 3)
    digest = hashlib.sha256(first + second + third).hexdigest()
    with InternalAudioStore(db, chunk_limit=3) as store:
        source_id = _source(store)
        assert (
            store.commit_drain(
                source_id,
                0,
                [first, second],
                DrainCheckpoint(1, expected_sha256=hashlib.sha256(first + second).hexdigest()),
            )
            == 2
        )
        assert store.commit_drain(source_id, 0, [first, second, third], DrainCheckpoint(1, expected_sha256=digest)) == 3
        assert tuple(record.raw_bytes for record in store.read_record_page(source_id, limit=3)) == (
            first,
            second,
            third,
        )


def test_recovery_leg_replays_the_durable_prefix(tmp_path: Path) -> None:
    db = StateDB(tmp_path / "state.sqlite3")
    raw = _record(4, 4)
    digest = hashlib.sha256(raw).hexdigest()
    with InternalAudioStore(db) as store:
        source_id = _source(store)
        assert store.commit_drain(source_id, 0, [raw], DrainCheckpoint(1, expected_sha256=digest)) == 1
        store.interrupt(source_id)
        store.prepare_leg(source_id, 0, 100)
        store.record_read_begin(source_id, 0, 100)
        assert store.commit_drain(source_id, 0, [raw], DrainCheckpoint(2, expected_sha256=digest)) == 1


def test_mismatched_leg_is_refused_without_source_mutation(tmp_path: Path) -> None:
    db = StateDB(tmp_path / "state.sqlite3")
    with InternalAudioStore(db) as store:
        source_id = "d" * 32
        before = store.create_source(source_id, "attempt", 0, 100)
        with pytest.raises(InternalAudioStoreError, match="frontier"):
            store.prepare_leg(source_id, 1, 99)
        with pytest.raises(InternalAudioStoreError, match="initial leg"):
            store.prepare_leg(source_id, 0, 99)
        assert store.get_source(source_id) == before
        prepared = store.prepare_leg(source_id, 0, 100)
        with pytest.raises(InternalAudioStoreError, match="READ_BEGIN"):
            store.record_read_begin(source_id, 1, 100)
        assert store.get_source(source_id) == prepared


def test_receiving_prepare_leg_refuses_without_rows_or_tail_mutation(tmp_path: Path) -> None:
    db = StateDB(tmp_path / "state.sqlite3")
    raw = _record(54, 54)
    with InternalAudioStore(db) as store:
        source_id = _source(store)
        assert store.commit_drain(source_id, 0, [raw], DrainCheckpoint(1, CaptureTail(1, 1, 445, 444, b"x"))) == 1
        before = store.get_source(source_id)
        with closing(db.open_connection(read_only=True)) as connection:
            before_rows = cast(
                tuple[int], cast(object, connection.execute("SELECT COUNT(*) FROM capture_record").fetchone())
            )
            before_tail = cast(
                tuple[bytes], cast(object, connection.execute("SELECT bytes FROM capture_tail").fetchone())
            )
        with pytest.raises(InternalAudioStoreError):
            store.prepare_leg(source_id, 0, 100)
        assert store.get_source(source_id) == before
    with closing(db.open_connection(read_only=True)) as connection:
        assert connection.execute("SELECT COUNT(*) FROM capture_record").fetchone() == before_rows
        assert connection.execute("SELECT bytes FROM capture_tail").fetchone() == before_tail


def test_digest_cache_is_bounded_across_sources(tmp_path: Path) -> None:
    db = StateDB(tmp_path / "state.sqlite3")
    raw = _record(55, 55)
    digest = hashlib.sha256(raw).hexdigest()
    with InternalAudioStore(db) as store:
        first = _source(store)
        second = _source(store, source_id="b" * 32)
        store.commit_drain(first, 0, [raw], DrainCheckpoint(1, expected_sha256=digest))
        store.commit_drain(second, 0, [raw], DrainCheckpoint(1, expected_sha256=digest))
        assert len(store._digest_cache) <= 1


def test_large_digest_hydration_uses_one_read_connection(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    db = StateDB(tmp_path / "state.sqlite3")
    records = [_record(index, index % 251) for index in range(2176)]
    with InternalAudioStore(db, chunk_limit=128) as store:
        source_id = _source(store, limit=2176)
        for start in range(0, len(records), 128):
            store.commit_drain(source_id, start, records[start : start + 128], DrainCheckpoint(1))
        source = store.get_source(source_id)
        assert source is not None
        store._digest_cache.clear()
        calls: list[bool] = []
        original_open = db.open_connection

        def tracked_open(*, read_only: bool = False) -> sqlite3.Connection:
            calls.append(read_only)
            return original_open(read_only=read_only)

        monkeypatch.setattr(db, "open_connection", tracked_open)
        store._digest_state(source)
        assert calls == [True]


def test_ambiguous_reopen_failure_fail_stops_all_mutators(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    db = StateDB(tmp_path / "state.sqlite3")
    raw = _record(56, 56)
    with InternalAudioStore(db) as store:
        source_id = _source(store)
        commit = store._commit

        def ambiguous_commit() -> None:
            commit()
            raise sqlite3.OperationalError("commit result is ambiguous")

        def failed_reopen() -> None:
            raise sqlite3.OperationalError("reopen failed")

        monkeypatch.setattr(store, "_commit", ambiguous_commit)
        monkeypatch.setattr(store, "_reopen", failed_reopen)
        with pytest.raises(InternalAudioStoreError):
            store.commit_drain(source_id, 0, [raw], DrainCheckpoint(1))
        with pytest.raises(InternalAudioStoreError, match="fail-stopped"):
            store.commit_drain(source_id, 0, [raw], DrainCheckpoint(1))
        with pytest.raises(InternalAudioStoreError, match="fail-stopped"):
            store.create_source("e" * 32, "draft", 0, 2)


def test_uint64_high_start_accepts_max_span_and_refuses_plus_one(tmp_path: Path) -> None:
    db = StateDB(tmp_path / "state.sqlite3")
    start = 2**64 - 2**32
    with InternalAudioStore(db) as store:
        source_id = "f" * 32
        store.create_source(source_id, "attempt", start, 2**64 - 1)
        store.prepare_leg(source_id, start, 2**32 - 1)
        store.record_read_begin(source_id, start, 2**32 - 1)
        with pytest.raises(InternalAudioStoreError):
            store.create_source("e" * 32, "draft", 2**63, 2**63 + 2**32)


def test_omitted_sha_assertion_stores_derived_exact_digest(tmp_path: Path) -> None:
    db = StateDB(tmp_path / "state.sqlite3")
    raw = _record(57, 57)
    with InternalAudioStore(db) as store:
        source_id = _source(store)
        assert store.commit_drain(source_id, 0, [raw], DrainCheckpoint(1)) == 1
        source = store.get_source(source_id)
        assert source is not None and source.committed_sha256 == hashlib.sha256(raw).hexdigest()


def test_shorter_recovery_tail_does_not_replace_longer_tail(tmp_path: Path) -> None:
    db = StateDB(tmp_path / "state.sqlite3")
    raw = _record(5, 5)
    digest = hashlib.sha256(raw).hexdigest()
    with InternalAudioStore(db) as store:
        source_id = _source(store)
        assert (
            store.commit_drain(source_id, 0, [raw], DrainCheckpoint(1, CaptureTail(1, 1, 644, 444, b"x" * 200), digest))
            == 1
        )
        store.interrupt(source_id)
        store.prepare_leg(source_id, 0, 100)
        store.record_read_begin(source_id, 0, 100)
        assert (
            store.commit_drain(source_id, 0, [raw], DrainCheckpoint(2, CaptureTail(2, 1, 494, 444, b"x" * 50), digest))
            == 1
        )
    connection = sqlite3.connect(db.path)
    assert connection.execute("SELECT leg_generation, length(bytes) FROM capture_tail").fetchone() == (1, 200)
    connection.close()


def test_advanced_frontier_replaces_old_partial_tail(tmp_path: Path) -> None:
    db = StateDB(tmp_path / "state.sqlite3")
    first, second, third = _record(50, 50), _record(51, 51), _record(52, 52)
    with InternalAudioStore(db) as store:
        source_id = _source(store)
        first_digest = hashlib.sha256(first).hexdigest()
        assert (
            store.commit_drain(
                source_id, 0, [first], DrainCheckpoint(1, CaptureTail(1, 1, 644, 444, second[:200]), first_digest)
            )
            == 1
        )
        digest = hashlib.sha256(first + second).hexdigest()
        assert (
            store.commit_drain(
                source_id, 1, [second], DrainCheckpoint(1, CaptureTail(1, 2, 938, 888, third[:50]), digest)
            )
            == 2
        )
    connection = sqlite3.connect(db.path)
    assert connection.execute(
        "SELECT complete_high_water, byte_offset, length(bytes), bytes FROM capture_tail"
    ).fetchone() == (2, 888, 50, third[:50])
    connection.close()


def test_same_offset_mismatched_tail_blocks_and_retains_bytes(tmp_path: Path) -> None:
    db = StateDB(tmp_path / "state.sqlite3")
    raw = _record(52, 52)
    digest = hashlib.sha256(raw).hexdigest()
    with InternalAudioStore(db) as store:
        source_id = _source(store)
        assert (
            store.commit_drain(source_id, 0, [raw], DrainCheckpoint(1, CaptureTail(1, 1, 644, 444, b"x" * 200), digest))
            == 1
        )
        store.interrupt(source_id)
        store.prepare_leg(source_id, 0, 100)
        store.record_read_begin(source_id, 0, 100)
        with pytest.raises(InternalAudioStoreError, match="tail"):
            store.commit_drain(source_id, 0, [raw], DrainCheckpoint(2, CaptureTail(2, 1, 494, 444, b"z" * 50), digest))
        blocked = store.get_source(source_id)
        assert blocked is not None and blocked.lifecycle == "blocked"
    connection = sqlite3.connect(db.path)
    assert connection.execute("SELECT bytes FROM capture_conflict").fetchone()[0] == b"z" * 50
    connection.close()


def test_stale_leg_generation_is_refused_without_tail(tmp_path: Path) -> None:
    db = StateDB(tmp_path / "state.sqlite3")
    raw = _record(53, 53)
    with InternalAudioStore(db) as store:
        source_id = _source(store)
        with pytest.raises(InternalAudioStoreError, match="generation"):
            store.commit_drain(source_id, 0, [raw], DrainCheckpoint(0, expected_sha256=hashlib.sha256(raw).hexdigest()))
        source = store.get_source(source_id)
        assert source is not None and source.committed_count == 0 and source.lifecycle == "receiving"


def test_three_hundred_records_split_at_protocol_chunk_boundaries(tmp_path: Path) -> None:
    db = StateDB(tmp_path / "state.sqlite3")
    records = [_record(index, index % 251) for index in range(300)]
    digest = hashlib.sha256()
    with InternalAudioStore(db, chunk_limit=128) as store:
        source_id = _source(store, limit=300)
        for start, end in ((0, 128), (128, 256), (256, 300)):
            batch = records[start:end]
            digest.update(b"".join(batch))
            assert (
                store.commit_drain(source_id, start, batch, DrainCheckpoint(1, expected_sha256=digest.hexdigest()))
                == end
            )
        assert store.get_source(source_id).committed_count == 300  # type: ignore[union-attr]


def test_mixed_replay_mismatch_retains_exact_rejected_chunk(tmp_path: Path) -> None:
    db = StateDB(tmp_path / "state.sqlite3")
    first, second = _record(6, 6), _record(7, 7)
    with InternalAudioStore(db, chunk_limit=3) as store:
        source_id = _source(store)
        prefix_digest = hashlib.sha256(first + second).hexdigest()
        assert store.commit_drain(source_id, 0, [first, second], DrainCheckpoint(1, expected_sha256=prefix_digest)) == 2
        rejected = first + _record(7, 8) + _record(8, 9)
        with pytest.raises(InternalAudioStoreError, match="conflicts"):
            store.commit_drain(
                source_id, 0, [first, _record(7, 8), _record(8, 9)], DrainCheckpoint(1, expected_sha256=prefix_digest)
            )
    connection = sqlite3.connect(db.path)
    assert connection.execute("SELECT bytes FROM capture_conflict").fetchone()[0] == rejected
    assert connection.execute("SELECT COUNT(*) FROM capture_record").fetchone() == (2,)
    connection.close()


def test_direct_source_delete_is_blocked_by_residual_custody(tmp_path: Path) -> None:
    db = StateDB(tmp_path / "state.sqlite3")
    raw = _record(8, 8)
    with InternalAudioStore(db) as store:
        source_id = _source(store)
        digest = hashlib.sha256(raw).hexdigest()
        store.commit_drain(source_id, 0, [raw], DrainCheckpoint(1, CaptureTail(1, 1, 445, 444, b"x"), digest))
    with closing(db.open_connection()) as connection, pytest.raises(sqlite3.IntegrityError):
        connection.execute("DELETE FROM capture_source WHERE source_id = ?", (source_id,))


def test_stream_digest_cache_keeps_one_bounded_source_state(tmp_path: Path) -> None:
    db = StateDB(tmp_path / "state.sqlite3")
    records = [_record(index, index) for index in range(4)]
    with InternalAudioStore(db, chunk_limit=2) as store:
        source_id = _source(store)
        digest = hashlib.sha256()
        for start in range(0, len(records), 2):
            batch = records[start : start + 2]
            digest.update(b"".join(batch))
            assert (
                store.commit_drain(source_id, start, batch, DrainCheckpoint(1, expected_sha256=digest.hexdigest()))
                == start + 2
            )
        assert len(store._digest_cache) == 1
        assert store._digest_cache[source_id][:2] == (4, digest.hexdigest())


def test_wrong_digest_rolls_back_without_partial_rows(tmp_path: Path) -> None:
    db = StateDB(tmp_path / "state.sqlite3")
    raw = _record(9, 9)
    with InternalAudioStore(db) as store:
        source_id = _source(store)
        with pytest.raises(InternalAudioStoreError, match="digest"):
            store.commit_drain(
                source_id, 0, [raw], DrainCheckpoint(1, expected_sha256=hashlib.sha256(b"wrong").hexdigest())
            )
        source = store.get_source(source_id)
        assert (
            source is not None
            and source.committed_count == 0
            and source.committed_sha256 == hashlib.sha256(b"").hexdigest()
        )
        assert store.read_record_page(source_id) == ()


@pytest.mark.parametrize(
    "lifecycle", ["prepared", "leg_prepared", "interrupted", "sealed_waiting", "blocked", "covered", "retiring"]
)
def test_drain_requires_receiving_lifecycle(tmp_path: Path, lifecycle: str) -> None:
    db = StateDB(tmp_path / "state.sqlite3")
    raw = _record(1, 1)
    with InternalAudioStore(db) as store:
        source_id = _source(store)
        with closing(db.open_connection()) as connection:
            connection.execute("UPDATE capture_source SET lifecycle = ? WHERE source_id = ?", (lifecycle, source_id))
            connection.commit()
        with pytest.raises(InternalAudioStoreError, match="receiving"):
            store.commit_drain(source_id, 0, [raw], DrainCheckpoint(1, expected_sha256=hashlib.sha256(raw).hexdigest()))


def test_oversize_generator_is_bounded_to_limit_plus_one(tmp_path: Path) -> None:
    db = StateDB(tmp_path / "state.sqlite3")
    consumed = 0

    def records() -> Iterable[bytes]:
        nonlocal consumed
        for index in range(20):
            consumed += 1
            yield _record(index, index)

    with InternalAudioStore(db, chunk_limit=3) as store:
        source_id = _source(store)
        with pytest.raises(InternalAudioStoreError, match="bound"):
            store.commit_drain(
                source_id, 0, records(), DrainCheckpoint(1, expected_sha256=hashlib.sha256(b"").hexdigest())
            )
    assert consumed == 4


def test_tail_geometry_is_strict(tmp_path: Path) -> None:
    db = StateDB(tmp_path / "state.sqlite3")
    raw = _record(1, 1)
    digest = hashlib.sha256(raw).hexdigest()
    with InternalAudioStore(db) as store:
        source_id = _source(store)
        with pytest.raises(InternalAudioStoreError, match="byte offset"):
            store.commit_drain(source_id, 0, [raw], DrainCheckpoint(1, CaptureTail(1, 1, 444, 443, b"x"), digest))


def test_typed_sql_checks_reject_malformed_canonical_values(tmp_path: Path) -> None:
    db = StateDB(tmp_path / "state.sqlite3")
    with InternalAudioStore(db) as store:
        source_id = _source(store)
    with closing(db.open_connection()) as connection:
        with pytest.raises(sqlite3.IntegrityError):
            connection.execute(
                "UPDATE capture_source SET current_leg_start = ? WHERE source_id = ?", ("text", source_id)
            )
        with pytest.raises(sqlite3.IntegrityError):
            connection.execute(
                "UPDATE capture_source SET quarantine_disposition = 'unknown' WHERE source_id = ?", (source_id,)
            )
        with pytest.raises(sqlite3.IntegrityError):
            connection.execute("UPDATE capture_source SET quarantine_opaque = 2 WHERE source_id = ?", (source_id,))
        with pytest.raises(sqlite3.IntegrityError):
            connection.execute(
                "INSERT INTO capture_record(source_id, ordinal, device_timestamp, payload, mapping_revision) VALUES (?, 0, 0, ?, ?)",
                (source_id, bytes(440), "A" * 64),
            )
        with pytest.raises(sqlite3.IntegrityError):
            connection.execute(
                "INSERT INTO capture_evidence(evidence_id, source_id, entry_kind, original_relative_path, device, inode, mode, size, symlink_text) "
                "VALUES ('e1', ?, 'symlink', 'x', 1, 1, 0, 1, ?)",
                (source_id, "target"),
            )
        with pytest.raises(sqlite3.IntegrityError):
            connection.execute(
                "INSERT INTO capture_evidence(evidence_id, source_id, entry_kind, original_relative_path, device, inode, mode, size, sha256, disposition) "
                "VALUES ('e2', ?, 'regular', 'x', 1, 1, 0, 1, ?, 'other')",
                (source_id, hashlib.sha256(b"x").hexdigest()),
            )
        connection.execute(
            "INSERT INTO capture_evidence(evidence_id, source_id, entry_kind, original_relative_path, device, inode, mode, size, reason, terminal_reason) "
            "VALUES ('empty-dir', ?, 'directory', 'quarantine/empty', 1, 2, 493, 0, 'original', 'classified')",
            (source_id,),
        )
        evidence = cast(
            sqlite3.Row | None,
            connection.execute(
                "SELECT reason,terminal_reason FROM capture_evidence WHERE evidence_id='empty-dir'"
            ).fetchone(),
        )
        assert evidence is not None and tuple(evidence) == ("original", "classified")
        with pytest.raises(sqlite3.IntegrityError):
            connection.execute(
                "INSERT INTO capture_evidence(evidence_id, source_id, entry_kind, original_relative_path, device, inode, mode, size, sha256) "
                "VALUES ('bad-dir', ?, 'directory', 'quarantine/bad', 1, 3, 493, 0, ?)",
                (source_id, hashlib.sha256(b"").hexdigest()),
            )
        with pytest.raises(sqlite3.IntegrityError):
            connection.execute(
                "INSERT INTO capture_evidence(evidence_id, source_id, entry_kind, original_relative_path, device, inode, mode, size) "
                "VALUES ('large-dir', ?, 'directory', 'quarantine/large', 1, 4, 493, 1)",
                (source_id,),
            )
        with pytest.raises(sqlite3.IntegrityError):
            connection.execute(
                "INSERT INTO capture_evidence(evidence_id, source_id, entry_kind, original_relative_path, device, inode, mode, size, symlink_text) "
                "VALUES ('linked-dir', ?, 'directory', 'quarantine/link', 1, 5, 493, 0, ?)",
                (source_id, b"target"),
            )


def test_commit_ambiguity_reopens_and_accepts_exact_durable_result(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    db = StateDB(tmp_path / "state.sqlite3")
    raw = _record(11, 7)
    digest = hashlib.sha256(raw).hexdigest()
    with InternalAudioStore(db) as store:
        source_id = _source(store)
        commit = store._commit

        def ambiguous_commit() -> None:
            commit()
            raise sqlite3.OperationalError("commit result is ambiguous")

        monkeypatch.setattr(store, "_commit", ambiguous_commit)
        assert store.commit_drain(source_id, 0, [raw], DrainCheckpoint(1, expected_sha256=digest)) == 1
        assert store.get_source(source_id).committed_count == 1  # type: ignore[union-attr]


def test_stream_validation_is_bounded_to_count_and_digest() -> None:
    records = (_record(index, index) for index in range(3))
    digest = hashlib.sha256(b"".join(_record(index, index) for index in range(3))).hexdigest()
    assert validate_source_stream(records, expected_count=3, expected_sha256=digest) == (3, digest)


def test_offline_installer_converts_v1_backup_only_when_explicit(tmp_path: Path) -> None:
    path = tmp_path / "backup.sqlite3"
    connection = sqlite3.connect(path)
    connection.execute(
        "CREATE TABLE state (kind TEXT NOT NULL, id TEXT NOT NULL, payload TEXT NOT NULL, PRIMARY KEY(kind, id)) WITHOUT ROWID"
    )
    connection.execute("PRAGMA user_version = 1")
    connection.commit()
    install_audio_schema(connection)
    connection.commit()
    assert connection.execute("PRAGMA user_version").fetchone()[0] == 2
    connection.close()


def test_publication_ledger_and_typed_cursor_share_one_atomic_transaction(tmp_path: Path) -> None:
    db = StateDB(tmp_path / "state.sqlite3")
    cohort_id = "a" * 32
    with InternalAudioStore(db) as store:
        with store.publication_transaction() as (state, connection):
            state.put(StateKind.READY_PUBLICATION, "bundle", {"state": "ready"})
            connection.execute(
                "INSERT INTO publication_cohort(cohort_id,lifecycle,cursor) VALUES (?, 'accepted', 3)",
                (cohort_id,),
            )
        assert db.get(StateKind.READY_PUBLICATION, "bundle") == {"state": "ready"}
        assert db.get_generation() == 1
        with closing(db.open_connection(read_only=True)) as connection:
            assert (
                connection.execute("SELECT cursor FROM publication_cohort WHERE cohort_id=?", (cohort_id,)).fetchone()[
                    0
                ]
                == 3
            )

        with pytest.raises(ValueError, match="rollback"), store.publication_transaction() as (state, connection):
            state.put(StateKind.READY_PUBLICATION, "lost", {"state": "ready"})
            connection.execute("UPDATE publication_cohort SET cursor=4 WHERE cohort_id=?", (cohort_id,))
            raise ValueError("rollback")
        assert db.get(StateKind.READY_PUBLICATION, "lost") is None
        assert db.get_generation() == 1
        with closing(db.open_connection(read_only=True)) as connection:
            assert (
                connection.execute("SELECT cursor FROM publication_cohort WHERE cohort_id=?", (cohort_id,)).fetchone()[
                    0
                ]
                == 3
            )
