from __future__ import annotations

import hashlib
import sqlite3
from collections.abc import Iterable
from contextlib import closing
from pathlib import Path
from threading import Event, Thread
from types import SimpleNamespace
from typing import cast

import pytest

from omi_collector.capture.adapters import collector_state_db, internal_audio_store
from omi_collector.capture.adapters.collector_state_db import StateDB, StateDBError, StateKind, install_audio_schema
from omi_collector.capture.adapters.internal_audio_store import (
    AUDIO_RECORD_SIZE,
    CaptureSourceIntegrityError,
    CaptureSourceReadError,
    CaptureTail,
    DrainCheckpoint,
    InternalAudioStore,
    InternalAudioStoreError,
    authenticate_capture_source,
    capture_space_requirement,
    decode_uint64,
    encode_uint64,
    validate_source_stream,
)
from omi_collector.capture.adapters.staging_contract import FatalInternalAudioStorageError
from omi_collector.capture.application.storage_fault import TransientInternalAudioStorageError


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


def test_fresh_database_is_v4_and_has_typed_tables(tmp_path: Path) -> None:
    path = tmp_path / "state.sqlite3"
    StateDB(path)
    connection = sqlite3.connect(path)
    assert connection.execute("PRAGMA user_version").fetchone()[0] == 4
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
        "ready_replay_proof",
        "ready_replay_proof_page",
        "ready_v2_output_stage",
        "ready_v2_stage_range",
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


def test_v4_validation_rejects_unexpected_tables(tmp_path: Path) -> None:
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


def test_bounded_audio_append_waits_for_shared_reader_then_commits_once(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    db = StateDB(tmp_path / "state.sqlite3")
    commit_called = Event()
    append_started = Event()
    start_append = Event()
    armed = False
    base_connection = collector_state_db._GuardedConnection

    class SignalingConnection(base_connection):
        def commit(self) -> None:
            if armed:
                commit_called.set()
            super().commit()

    monkeypatch.setattr(collector_state_db, "_GuardedConnection", SignalingConnection)
    records = tuple(_record(index, index % 251) for index in range(128))
    result: list[int] = []
    errors: list[Exception] = []

    def append_batch() -> None:
        try:
            with InternalAudioStore(db, chunk_limit=128) as store:
                source_id = _source(store, limit=128)
                append_started.set()
                if not start_append.wait(5):
                    raise TimeoutError("test append was not released")
                result.append(store.commit_drain(source_id, 0, records, DrainCheckpoint(1)))
        except (sqlite3.DatabaseError, StateDBError, InternalAudioStoreError, TimeoutError) as error:
            errors.append(error)

    thread = Thread(target=append_batch)
    thread.start()
    reader = sqlite3.connect(db.path, isolation_level=None, timeout=0)
    try:
        assert append_started.wait(5)
        reader.execute("BEGIN")
        reader.execute("SELECT count(*) FROM capture_record").fetchone()
        armed = True
        start_append.set()
        assert commit_called.wait(5)
        reader.rollback()
        thread.join(5)
    finally:
        start_append.set()
        reader.rollback()
        reader.close()
        if thread.is_alive():
            thread.join(5)
    assert not thread.is_alive()
    assert errors == []
    assert result == [128]
    with closing(sqlite3.connect(db.path)) as connection:
        assert connection.execute("SELECT count(*) FROM capture_record").fetchone() == (128,)


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


def test_replay_conflict_with_uncheckpointed_canonical_row_is_an_indefinite_integrity_hold(tmp_path: Path) -> None:
    db = StateDB(tmp_path / "state.sqlite3")
    raw = _record(10, 4)
    with InternalAudioStore(db) as store:
        source_id = _source(store)
        with closing(db.open_connection()) as connection:
            connection.execute(
                "INSERT INTO capture_record(source_id, ordinal, device_timestamp, payload) VALUES (?, 0, ?, ?)",
                (source_id, 10, raw[4:]),
            )
            connection.commit()
        source = store.get_source(source_id)
        assert source is not None and source.committed_count == 0

        store._block_replay_conflict(source, 0, b"rejected")

    with closing(db.open_connection(read_only=True)) as connection:
        held = cast(
            tuple[object, ...],
            connection.execute(
                "SELECT lifecycle, block_reason, quarantine_disposition, terminal_reason, terminal_time, "
                "retention_deadline, certified_prefix_count, coverage_digest FROM capture_source WHERE source_id = ?",
                (source_id,),
            ).fetchone(),
        )
        assert tuple(held) == (
            "blocked",
            "source_authentication_mismatch",
            "unclassified",
            None,
            None,
            None,
            None,
            None,
        )
        count_row = cast(
            sqlite3.Row | None,
            connection.execute("SELECT COUNT(*) FROM capture_record WHERE source_id = ?", (source_id,)).fetchone(),
        )
        assert count_row is not None and count_row[0] == 1


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


def test_multi_page_authentication_reuses_one_read_connection(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    db = StateDB(tmp_path / "state.sqlite3")
    records = [_record(index, index % 251) for index in range(300)]
    with InternalAudioStore(db, chunk_limit=128) as store:
        source_id = _source(store, limit=len(records))
        for start in range(0, len(records), 128):
            store.commit_drain(source_id, start, records[start : start + 128], DrainCheckpoint(1))

    calls: list[bool] = []
    original_open = db.open_connection

    def tracked_open(*, read_only: bool = False) -> sqlite3.Connection:
        calls.append(read_only)
        return original_open(read_only=read_only)

    monkeypatch.setattr(db, "open_connection", tracked_open)

    authenticated = authenticate_capture_source(db, source_id, page_limit=128)

    assert authenticated.committed_count == len(records)
    assert authenticated.committed_sha256 == hashlib.sha256(b"".join(records)).hexdigest()
    assert calls == [True]


@pytest.mark.parametrize(
    ("mutation", "message", "ignore_constraints"),
    [
        ("DELETE FROM capture_record WHERE ordinal = 299", "count", False),
        ("UPDATE capture_record SET payload = zeroblob(440) WHERE ordinal = 299", "digest", False),
        ("UPDATE capture_record SET ordinal = 301 WHERE ordinal = 299", "ordinals", False),
        ("UPDATE capture_record SET payload = zeroblob(439) WHERE ordinal = 299", "layout", True),
    ],
)
def test_authentication_rejects_canonical_row_tampering(
    tmp_path: Path, mutation: str, message: str, *, ignore_constraints: bool
) -> None:
    db = StateDB(tmp_path / "state.sqlite3")
    records = [_record(index, index % 251) for index in range(300)]
    with InternalAudioStore(db, chunk_limit=128) as store:
        source_id = _source(store, limit=len(records))
        for start in range(0, len(records), 128):
            store.commit_drain(source_id, start, records[start : start + 128], DrainCheckpoint(1))
    with closing(db.open_connection()) as connection:
        if ignore_constraints:
            connection.execute("PRAGMA ignore_check_constraints = ON")
        connection.execute(mutation)
        connection.commit()

    with pytest.raises(CaptureSourceIntegrityError, match=message):
        authenticate_capture_source(db, source_id, page_limit=128)


def test_authentication_classifies_metadata_only_digest_mismatch_as_integrity(tmp_path: Path) -> None:
    db = StateDB(tmp_path / "state.sqlite3")
    raw = _record(63, 63)
    with InternalAudioStore(db) as store:
        source_id = _source(store)
        store.commit_drain(source_id, 0, [raw], DrainCheckpoint(1))
    with closing(db.open_connection()) as connection:
        connection.execute(
            "UPDATE capture_source SET committed_sha256 = ? WHERE source_id = ?",
            ("f" * 64, source_id),
        )
        connection.commit()

    with pytest.raises(CaptureSourceIntegrityError, match="digest"):
        authenticate_capture_source(db, source_id)


@pytest.mark.parametrize("sqlite_errorcode", [None, sqlite3.SQLITE_BUSY, sqlite3.SQLITE_CORRUPT, sqlite3.SQLITE_NOTADB])
def test_authentication_preserves_structured_sqlite_read_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, sqlite_errorcode: int | None
) -> None:
    db = StateDB(tmp_path / "state.sqlite3")
    failure = sqlite3.OperationalError("read failure")
    if sqlite_errorcode is not None:
        failure.sqlite_errorcode = sqlite_errorcode

    def fail_read(_connection: sqlite3.Connection, _source_id: str) -> internal_audio_store.CaptureSource | None:
        raise failure

    monkeypatch.setattr(internal_audio_store, "_read_capture_source", fail_read)

    expected = (
        FatalInternalAudioStorageError
        if sqlite_errorcode in {sqlite3.SQLITE_CORRUPT, sqlite3.SQLITE_NOTADB}
        else TransientInternalAudioStorageError
        if sqlite_errorcode == sqlite3.SQLITE_BUSY
        else CaptureSourceReadError
    )
    with pytest.raises(expected) as raised:
        authenticate_capture_source(db, "a" * 32)
    assert raised.value.sqlite_errorcode == sqlite_errorcode
    assert raised.value.__cause__ is failure


@pytest.mark.parametrize("sqlite_errorcode", [sqlite3.SQLITE_BUSY, sqlite3.SQLITE_CORRUPT, sqlite3.SQLITE_NOTADB])
def test_authentication_preserves_sqlite_code_from_open_connection(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, sqlite_errorcode: int
) -> None:
    db = StateDB(tmp_path / "state.sqlite3")
    failure = sqlite3.OperationalError("simulated SQLite open failure")
    failure.sqlite_errorcode = sqlite_errorcode

    def fail_open(*_args: object, **_kwargs: object) -> None:
        raise failure

    monkeypatch.setattr(collector_state_db.sqlite3, "connect", fail_open)
    expected = (
        FatalInternalAudioStorageError
        if sqlite_errorcode in {sqlite3.SQLITE_CORRUPT, sqlite3.SQLITE_NOTADB}
        else TransientInternalAudioStorageError
    )
    with pytest.raises(expected) as raised:
        authenticate_capture_source(db, "a" * 32)
    assert raised.value.sqlite_errorcode == sqlite_errorcode
    assert raised.value.__cause__ is failure


@pytest.mark.parametrize(
    ("sqlite_errorcode", "expected"),
    [
        (sqlite3.SQLITE_BUSY, TransientInternalAudioStorageError),
        (sqlite3.SQLITE_LOCKED, TransientInternalAudioStorageError),
        (sqlite3.SQLITE_CORRUPT, FatalInternalAudioStorageError),
        (sqlite3.SQLITE_NOTADB, FatalInternalAudioStorageError),
    ],
)
def test_publication_record_page_preserves_sqlite_fault(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, sqlite_errorcode: int, expected: type[Exception]
) -> None:
    db = StateDB(tmp_path / "state.sqlite3")
    failure = sqlite3.DatabaseError("simulated page fault")
    failure.sqlite_errorcode = sqlite_errorcode

    def fail_page(*_args: object) -> None:
        raise failure

    monkeypatch.setattr(internal_audio_store, "_read_record_page", fail_page)
    with InternalAudioStore(db) as store, store.publication_reader(), pytest.raises(expected) as raised:
        store.read_record_page("a" * 32)
    assert (
        cast(FatalInternalAudioStorageError | TransientInternalAudioStorageError, raised.value).sqlite_errorcode
        == sqlite_errorcode
    )
    assert raised.value.__cause__ is failure


def test_authentication_raw_oserror_is_not_a_structured_read_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    db = StateDB(tmp_path / "state.sqlite3")

    def fail_read(_connection: sqlite3.Connection, _source_id: str) -> internal_audio_store.CaptureSource | None:
        raise OSError("read failure")

    monkeypatch.setattr(internal_audio_store, "_read_capture_source", fail_read)

    with pytest.raises(InternalAudioStoreError) as raised:
        authenticate_capture_source(db, "a" * 32)
    assert type(raised.value) is InternalAudioStoreError


def test_authentication_rejects_source_change_after_record_scan(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    db = StateDB(tmp_path / "state.sqlite3")
    raw = _record(64, 64)
    with InternalAudioStore(db) as store:
        source_id = _source(store)
        store.commit_drain(source_id, 0, [raw], DrainCheckpoint(1))
    read_source = internal_audio_store._read_capture_source
    source_reads = 0

    def change_before_final_check(connection: sqlite3.Connection, requested_id: str):
        nonlocal source_reads
        source_reads += 1
        if source_reads == 2:
            with closing(sqlite3.connect(db.path)) as writer:
                writer.execute(
                    "UPDATE capture_source SET lifecycle = 'interrupted' WHERE source_id = ?",
                    (source_id,),
                )
                writer.commit()
        return read_source(connection, requested_id)

    monkeypatch.setattr(internal_audio_store, "_read_capture_source", change_before_final_check)

    with pytest.raises(InternalAudioStoreError, match="source changed") as raised:
        authenticate_capture_source(db, source_id)
    assert type(raised.value) is InternalAudioStoreError


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
            failure = sqlite3.OperationalError("reopen failed")
            failure.sqlite_errorcode = sqlite3.SQLITE_BUSY
            raise failure

        monkeypatch.setattr(store, "_commit", ambiguous_commit)
        monkeypatch.setattr(store, "_reopen", failed_reopen)
        with pytest.raises(FatalInternalAudioStorageError, match="ambiguous drain"):
            store.commit_drain(source_id, 0, [raw], DrainCheckpoint(1))
        with pytest.raises(InternalAudioStoreError, match="fail-stopped"):
            store.commit_drain(source_id, 0, [raw], DrainCheckpoint(1))
        with pytest.raises(InternalAudioStoreError, match="fail-stopped"):
            store.create_source("e" * 32, "draft", 0, 2)


def test_unsettled_writer_rollback_releases_shared_guard_and_retains_first_fatal(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    db = StateDB(tmp_path / "state.sqlite3")
    rollback_started, release_rollback, auxiliary_started = Event(), Event(), Event()
    fatal = FatalInternalAudioStorageError("original writer failure")
    writer_errors: list[BaseException] = []
    auxiliary_errors: list[BaseException] = []

    class FailingRollbackConnection(collector_state_db._GuardedConnection):
        def rollback(self) -> None:
            rollback_started.set()
            assert release_rollback.wait(5)
            raise sqlite3.OperationalError("rollback remained active")

    monkeypatch.setattr(collector_state_db, "_GuardedConnection", FailingRollbackConnection)

    def writer() -> None:
        try:
            with InternalAudioStore(db) as store, store.publication_transaction() as (state, _connection):
                state.put(StateKind.RUNTIME_STATUS, "writer", {"uncommitted": True})
                raise fatal
        except BaseException as error:  # noqa: BLE001 - assert the exact worker outcome
            writer_errors.append(error)

    def auxiliary() -> None:
        auxiliary_started.set()
        try:
            db.put(StateKind.FIRMWARE, "independent", {"committed": True})
        except BaseException as error:  # noqa: BLE001 - assert the independent writer outcome
            auxiliary_errors.append(error)

    writer_thread = Thread(target=writer, name="rollback-writer")
    auxiliary_thread = Thread(target=auxiliary, name="independent-writer")
    writer_thread.start()
    try:
        assert rollback_started.wait(5)
        auxiliary_thread.start()
        assert auxiliary_started.wait(5)
        auxiliary_thread.join(0.05)
        assert auxiliary_thread.is_alive(), "independent writer must wait for the active write guard"
    finally:
        release_rollback.set()
    writer_thread.join(5)
    auxiliary_thread.join(5)
    assert not writer_thread.is_alive() and not auxiliary_thread.is_alive()
    assert writer_errors == [fatal]
    assert auxiliary_errors == []
    assert db.get(StateKind.RUNTIME_STATUS, "writer") is None
    assert db.get(StateKind.FIRMWARE, "independent") == {"committed": True}


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


def test_short_exact_replay_authenticates_its_durable_prefix(tmp_path: Path) -> None:
    db = StateDB(tmp_path / "state.sqlite3")
    first, second, third = _record(60, 60), _record(61, 61), _record(62, 62)
    with InternalAudioStore(db, chunk_limit=3) as store:
        source_id = _source(store)
        full_digest = hashlib.sha256(first + second + third).hexdigest()
        assert (
            store.commit_drain(source_id, 0, [first, second, third], DrainCheckpoint(1, expected_sha256=full_digest))
            == 3
        )

        prefix_digest = hashlib.sha256(first + second).hexdigest()
        assert (
            store.commit_drain(
                source_id,
                0,
                [first, second],
                DrainCheckpoint(1, expected_sha256=prefix_digest),
            )
            == 3
        )
        with pytest.raises(InternalAudioStoreError, match="checkpoint conflicts"):
            store.commit_drain(
                source_id,
                0,
                [first, second],
                DrainCheckpoint(1, expected_sha256=full_digest),
            )


def test_long_prefix_replay_digest_reuses_one_connection_for_bounded_pages(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    db = StateDB(tmp_path / "state.sqlite3")
    records = [_record(index, index % 251) for index in range(130)]
    with InternalAudioStore(db) as store:
        source_id = _source(store, limit=len(records))
        first_digest = hashlib.sha256(b"".join(records[:128])).hexdigest()
        full_digest = hashlib.sha256(b"".join(records)).hexdigest()
        assert store.commit_drain(source_id, 0, records[:128], DrainCheckpoint(1, expected_sha256=first_digest)) == 128
        assert store.commit_drain(source_id, 128, records[128:], DrainCheckpoint(1, expected_sha256=full_digest)) == 130
        before = store.get_source(source_id)
        prefix_digest = hashlib.sha256(b"".join(records[:129])).hexdigest()
        page_calls: list[tuple[sqlite3.Connection, int, int]] = []
        original_read_page = internal_audio_store._read_record_page

        def tracked_read_page(
            connection: sqlite3.Connection,
            requested_source_id: str,
            after_ordinal: int,
            limit: int,
        ) -> tuple[internal_audio_store.CaptureRecord, ...]:
            page_calls.append((connection, after_ordinal, limit))
            return original_read_page(connection, requested_source_id, after_ordinal, limit)

        monkeypatch.setattr(internal_audio_store, "_read_record_page", tracked_read_page)

        assert (
            store.commit_drain(
                source_id,
                128,
                [records[128]],
                DrainCheckpoint(1, expected_sha256=prefix_digest),
            )
            == 130
        )
        assert [(after_ordinal, limit) for _, after_ordinal, limit in page_calls] == [
            (127, 1),
            (-1, 128),
            (127, 1),
        ]
        assert page_calls[0][0] is not page_calls[1][0]
        assert page_calls[1][0] is page_calls[2][0]
        assert store.get_source(source_id) == before


def test_tail_checkpoint_requires_the_current_durable_read_leg(tmp_path: Path) -> None:
    db = StateDB(tmp_path / "state.sqlite3")
    with InternalAudioStore(db) as store:
        source_id = _source(store)
        tail = CaptureTail(1, 0, 1, 0, b"x")
        assert store.commit_tail(source_id, 0, DrainCheckpoint(1, tail)) == 0

        with pytest.raises(InternalAudioStoreError, match="generation"):
            store.commit_tail(source_id, 0, DrainCheckpoint(0, tail))


@pytest.mark.parametrize(
    "mutation",
    [
        ("lifecycle = ?", ("interrupted",)),
        ("prepared_count = ?", (99,)),
        (
            (
                "prepared_start = ?, prepared_count = ?, read_begin_start = ?, read_begin_count = ?, "
                "current_leg_start = ?, current_leg_count = ?"
            ),
            (encode_uint64(1), 99, encode_uint64(1), 99, encode_uint64(1), 99),
        ),
    ],
)
def test_tail_checkpoint_rejects_corrupt_durable_read_state(
    tmp_path: Path,
    mutation: tuple[str, tuple[object, ...]],
) -> None:
    db = StateDB(tmp_path / "state.sqlite3")
    with InternalAudioStore(db) as store:
        source_id = _source(store)
        clause, values = mutation
        with closing(db.open_connection()) as connection:
            connection.execute(f"UPDATE capture_source SET {clause} WHERE source_id = ?", (*values, source_id))
            connection.commit()
        with pytest.raises(InternalAudioStoreError, match=r"receiving|bound READ leg"):
            store.commit_tail(source_id, 0, DrainCheckpoint(1, CaptureTail(1, 0, 1, 0, b"x")))


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


def test_tail_past_read_leg_is_refused_without_mutation(tmp_path: Path) -> None:
    db = StateDB(tmp_path / "state.sqlite3")
    first = _record(1, 1)
    second = _record(2, 2)
    source_id = "a" * 32
    with InternalAudioStore(db) as store:
        store.create_source(source_id, "attempt", 0, 2)
        store.prepare_leg(source_id, 0, 2)
        store.record_read_begin(source_id, 0, 2)
        store.commit_drain(
            source_id,
            0,
            [first],
            DrainCheckpoint(1, expected_sha256=hashlib.sha256(first).hexdigest()),
        )
        store.interrupt(source_id)
        store.prepare_leg(source_id, 1, 1)
        store.record_read_begin(source_id, 1, 1)
        before = store.get_source(source_id)
        tail = CaptureTail(2, 2, 2 * AUDIO_RECORD_SIZE + 1, 2 * AUDIO_RECORD_SIZE, b"x")

        with pytest.raises(InternalAudioStoreError, match="received frontier"):
            store.commit_drain(
                source_id,
                1,
                [second],
                DrainCheckpoint(2, tail, hashlib.sha256(first + second).hexdigest()),
            )

        assert store.get_source(source_id) == before
        assert tuple(record.raw_bytes for record in store.read_record_page(source_id)) == (first,)
    with closing(db.open_connection(read_only=True)) as connection:
        assert (
            connection.execute("SELECT COUNT(*) FROM capture_tail WHERE source_id = ?", (source_id,)).fetchone()[0] == 0
        )


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


@pytest.mark.parametrize("durable_state", ["proposed", "old", "third"])
def test_seal_ambiguity_reconciles_only_exact_old_or_proposed_state(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    durable_state: str,
) -> None:
    db = StateDB(tmp_path / f"{durable_state}.sqlite3")
    raw = _record(63, 63)
    digest = hashlib.sha256(raw).hexdigest()
    with InternalAudioStore(db) as store:
        source_id = _source(store, limit=1)
        store.commit_drain(source_id, 0, [raw], DrainCheckpoint(1, expected_sha256=digest))
        commit = store._commit

        def ambiguous_commit() -> None:
            if durable_state != "old":
                commit()
            if durable_state == "third":
                connection = store._require_connection()
                connection.execute(
                    "UPDATE capture_source SET lifecycle = 'blocked' WHERE source_id = ?",
                    (source_id,),
                )
                connection.commit()
            raise sqlite3.OperationalError("commit result is ambiguous")

        monkeypatch.setattr(store, "_commit", ambiguous_commit)
        if durable_state == "proposed":
            sealed = store.seal_source(
                source_id,
                leg_generation=1,
                expected_count=1,
                expected_sha256=digest,
            )
            assert sealed.lifecycle == "sealed_waiting"
        elif durable_state == "old":
            with pytest.raises(InternalAudioStoreError, match="rolled back"):
                store.seal_source(
                    source_id,
                    leg_generation=1,
                    expected_count=1,
                    expected_sha256=digest,
                )
        else:
            with pytest.raises(FatalInternalAudioStorageError, match="could not be reconciled"):
                store.seal_source(
                    source_id,
                    leg_generation=1,
                    expected_count=1,
                    expected_sha256=digest,
                )
            with pytest.raises(InternalAudioStoreError, match="fail-stopped"):
                store.interrupt(source_id)


def test_stream_validation_is_bounded_to_count_and_digest() -> None:
    records = (_record(index, index) for index in range(3))
    digest = hashlib.sha256(b"".join(_record(index, index) for index in range(3))).hexdigest()
    assert validate_source_stream(records, expected_count=3, expected_sha256=digest) == (3, digest)


def test_offline_installer_refuses_v1_database_without_mutation(tmp_path: Path) -> None:
    path = tmp_path / "v1.sqlite3"
    with closing(sqlite3.connect(path)) as connection:
        connection.execute(
            "CREATE TABLE state (kind TEXT NOT NULL, id TEXT NOT NULL, payload TEXT NOT NULL, PRIMARY KEY(kind, id)) WITHOUT ROWID"
        )
        connection.execute("INSERT INTO state VALUES ('migration', 'legacy', '{\"value\":1}')")
        connection.execute("PRAGMA user_version = 1")
        connection.commit()
    before = path.read_bytes()

    with closing(sqlite3.connect(path)) as connection, pytest.raises(StateDBError, match="unsupported"):
        install_audio_schema(connection)

    assert path.read_bytes() == before
    with closing(sqlite3.connect(path)) as connection:
        assert connection.execute("PRAGMA user_version").fetchone() == (1,)
        assert connection.execute("SELECT kind, id, payload FROM state").fetchone() == (
            "migration",
            "legacy",
            '{"value":1}',
        )
        assert connection.execute("PRAGMA integrity_check").fetchone() == ("ok",)
        assert connection.execute("SELECT name FROM sqlite_master WHERE name = 'capture_source'").fetchone() is None


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
