from __future__ import annotations

import multiprocessing
import shutil
import sqlite3
from collections.abc import Callable
from contextlib import closing, contextmanager
from hashlib import sha256
from multiprocessing.connection import Connection
from pathlib import Path
from struct import pack
from typing import cast

import pytest

from omi_collector.capture.adapters.clock_segments import ClockSegmentMap
from omi_collector.capture.adapters.collector_state_db import StateDB, StateKind
from omi_collector.capture.adapters.internal_audio_store import (
    AUDIO_RECORD_SIZE,
    DrainCheckpoint,
    InternalAudioStore,
    InternalAudioStoreError,
    encode_uint64,
)
from omi_collector.capture.adapters.ready_bundles import (
    ReadyBundleError,
    ReadyOutcome,
    ReadyOutcomeState,
    _TypedPage,
    _TypedStage,
    finalize_typed_sources,
)
from omi_collector.capture.adapters.staging_contract import FatalInternalAudioStorageError
from omi_collector.config import ReadyConfig
from omi_collector.state_migration import open_runtime_state
from omi_collector.storage_layout import load_operator_config


class _Source:
    def __init__(self, store: InternalAudioStore, source_id: str) -> None:
        self._store = store
        self._source_id = source_id
        self.source_reads = 0
        self.page_calls: list[tuple[int, int]] = []

    @property
    def source(self):
        self.source_reads += 1
        source = self._store.get_source(self._source_id)
        assert source is not None
        return source

    def source_page(self, after_ordinal: int = -1, limit: int = 128) -> _TypedPage:
        self.page_calls.append((after_ordinal, limit))
        return _TypedPage(
            self.source,
            after_ordinal,
            self._store.read_record_page(self._source_id, after_ordinal, limit),
        )


def _record(timestamp: int, fill: int) -> bytes:
    return pack(">I", timestamp) + bytes((2, 8, fill)) + bytes(AUDIO_RECORD_SIZE - 7)


def _source(
    store: InternalAudioStore,
    source_id: str,
    start: int,
    records: tuple[bytes, ...],
    *,
    lifecycle: str = "sealed_waiting",
) -> _Source:
    store.create_source(source_id, "attempt", start, start + len(records))
    store.prepare_leg(source_id, start, len(records))
    store.record_read_begin(source_id, start, len(records))
    store.commit_drain(source_id, 0, records, DrainCheckpoint(1))
    with closing(store.state_db.open_connection()) as connection:
        connection.execute("BEGIN IMMEDIATE")
        connection.execute("UPDATE capture_source SET lifecycle=? WHERE source_id=?", (lifecycle, source_id))
        connection.commit()
    return _Source(store, source_id)


def _certify(
    store: InternalAudioStore, source_id: str, records: tuple[bytes, ...], *, count: int | None = None
) -> None:
    certified = records if count is None else records[:count]
    digest = sha256(b"".join(certified)).hexdigest()
    with closing(store.state_db.open_connection()) as connection:
        connection.execute("BEGIN IMMEDIATE")
        connection.execute(
            "UPDATE capture_source SET source_type='quarantine', block_reason='test quarantine', "
            "certified_prefix_count=?, coverage_digest=?, quarantine_disposition='published_prefix', "
            "terminal_time='1', retention_deadline='2' WHERE source_id=?",
            (len(certified), digest, source_id),
        )
        connection.commit()


def _quarantine_unclassified(store: InternalAudioStore, source_id: str) -> None:
    with closing(store.state_db.open_connection()) as connection:
        connection.execute("BEGIN IMMEDIATE")
        connection.execute(
            "UPDATE capture_source SET source_type='quarantine', block_reason='test quarantine', "
            "quarantine_disposition='unclassified' WHERE source_id=?",
            (source_id,),
        )
        connection.commit()


def _ready_bytes(outcome: ReadyOutcome) -> bytes:
    published = outcome.published
    return b"".join(result.path.joinpath("records.bin").read_bytes() for result in published)


def _fault_transaction(store: InternalAudioStore, boundary: int, *, rollback: bool):
    original = store.publication_transaction
    calls = 0

    @contextmanager
    def injected():
        nonlocal calls
        calls += 1
        if calls != boundary:
            with original() as transaction:
                yield transaction
            return
        if rollback:
            with original() as transaction:
                yield transaction
                raise sqlite3.DatabaseError("simulated rollback")
        with original() as transaction:
            yield transaction
        raise sqlite3.DatabaseError("simulated commit ambiguity")

    return original, injected


def _third_state_transaction(store: InternalAudioStore, boundary: int):
    original = store.publication_transaction
    calls = 0

    @contextmanager
    def injected():
        nonlocal calls
        calls += 1
        with original() as transaction:
            yield transaction
        if calls != boundary:
            return
        if boundary in {1, 2}:
            with closing(store.state_db.open_connection()) as connection:
                connection.execute("BEGIN IMMEDIATE")
                changed = connection.execute(
                    "UPDATE publication_cohort SET cursor=1 WHERE lifecycle IN ('accepted', 'staging')"
                )
                assert changed.rowcount == 1
                connection.commit()
        else:
            ledger = store.state_db.list(StateKind.READY_PUBLICATION)
            bundle_id, payload = next(item for item in ledger if item[0] != "frontier")
            payload["records_sha256"] = "0" * 64
            store.state_db.put(StateKind.READY_PUBLICATION, bundle_id, payload)
        raise sqlite3.DatabaseError("simulated durable third state")

    return original, injected


def _publish(  # noqa: PLR0913 - the test boundary mirrors the publication contract
    store: InternalAudioStore,
    sources: tuple[_Source, ...],
    ready: Path,
    *,
    threshold: float = 0.02,
    page_limit: int = 128,
    frontier: int = 104,
):
    return finalize_typed_sources(
        store,
        sources,
        ready,
        config=ReadyConfig(target_audio_seconds=threshold),
        clock_segments=ClockSegmentMap(()),
        frontier=frontier,
        drained=True,
        page_limit=page_limit,
    )


def _pause_after_typed_stage(database_path: Path, ready: Path, sender: Connection) -> None:
    import omi_collector.capture.adapters.ready_bundles as ready_bundles

    store = InternalAudioStore(StateDB(database_path))
    original = ready_bundles._write_typed_stage

    def pause(stage: _TypedStage, page_limit: int) -> None:
        original(stage, page_limit)
        sender.send("staged")
        sender.recv()

    ready_bundles._write_typed_stage = pause
    try:
        _publish(store, (_Source(store, "a" * 32),), ready)
    finally:
        store.close()
        sender.close()


def _publication_database_state(
    database: StateDB, source_id: str
) -> tuple[tuple[str, int] | None, str, tuple[int, int], int, int]:
    with closing(database.open_connection(read_only=True)) as connection:
        cohort = cast(
            tuple[str, int] | None,
            connection.execute("SELECT lifecycle, cursor FROM publication_cohort").fetchone(),
        )
        source_lifecycle = cast(
            str,
            connection.execute("SELECT lifecycle FROM capture_source WHERE source_id=?", (source_id,)).fetchone()[0],
        )
        retirement = cast(
            tuple[int, int],
            tuple(
                cast(
                    tuple[object, ...],
                    connection.execute(
                        "SELECT retirement_cursor, (SELECT count(*) FROM capture_record WHERE source_id=?) "
                        "FROM capture_source WHERE source_id=?",
                        (source_id, source_id),
                    ).fetchone(),
                )
            ),
        )
        cohort_count = cast(int, connection.execute("SELECT count(*) FROM publication_cohort").fetchone()[0])
        stage_count = cast(int, connection.execute("SELECT count(*) FROM ready_output_stage").fetchone()[0])
    return cohort, source_lifecycle, retirement, cohort_count, stage_count


def test_sql_publication_preserves_ready_bytes_and_reports_gap_stats(tmp_path: Path) -> None:
    database = StateDB(tmp_path / "state.sqlite3")
    store = InternalAudioStore(database)
    first_raw = _record(100, 1)
    second_raw = _record(103, 2)
    first = _source(store, "a" * 32, 100, (first_raw,))
    second = _source(store, "b" * 32, 103, (second_raw,))
    ready = tmp_path / "ready"
    ready.mkdir()
    ready.chmod(0o2750)

    outcome = _publish(store, (first, second), ready)

    assert outcome.state is ReadyOutcomeState.PUBLISHED
    assert outcome.stats is not None
    assert outcome.stats.gap_count == 1
    assert outcome.stats.gap_records == 2
    assert len(outcome.published) == 2
    assert (
        b"".join(result.path.joinpath("records.bin").read_bytes() for result in outcome.published)
        == first_raw + second_raw
    )
    store.close()


def test_sql_publication_waits_for_threshold(tmp_path: Path) -> None:
    database = StateDB(tmp_path / "state.sqlite3")
    store = InternalAudioStore(database)
    source = _source(store, "a" * 32, 100, (_record(100, 1),))
    ready = tmp_path / "ready"
    ready.mkdir()
    ready.chmod(0o2750)

    outcome = _publish(store, (source,), ready, threshold=0.05)

    assert outcome.state is ReadyOutcomeState.WAITING
    assert outcome.reason == "waiting_for_threshold"
    assert not tuple(ready.iterdir())
    store.close()


def test_sql_stage_rebuilds_after_interruption(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    database = StateDB(tmp_path / "state.sqlite3")
    store = InternalAudioStore(database)
    source = _source(store, "a" * 32, 100, (_record(100, 1),))
    ready = tmp_path / "ready"
    ready.mkdir()
    ready.chmod(0o2750)
    import omi_collector.capture.adapters.ready_bundles as ready_bundles

    original = ready_bundles._write_typed_stage

    def interrupt(*args: object, **kwargs: object) -> None:
        del args, kwargs
        raise OSError("simulated interruption")

    monkeypatch.setattr(ready_bundles, "_write_typed_stage", interrupt)
    with pytest.raises(ReadyBundleError):
        _publish(store, (source,), ready)
    assert database.list(StateKind.READY_PUBLICATION) == ()
    with closing(database.open_connection(read_only=True)) as connection:
        assert connection.execute("SELECT count(*) FROM ready_output_stage").fetchone()[0] == 1

    monkeypatch.setattr(ready_bundles, "_write_typed_stage", original)
    outcome = _publish(store, (source,), ready)
    assert outcome.state is ReadyOutcomeState.PUBLISHED
    assert not tuple(path for path in ready.iterdir() if path.name.startswith("."))
    store.close()


@pytest.mark.parametrize("target", ("file", "manifest", "stage", "parent"))
def test_sql_ready_durability_failure_latches_stage_and_preserves_custody(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, target: str
) -> None:
    database = StateDB(tmp_path / "state.sqlite3")
    store = InternalAudioStore(database)
    raw = _record(100, 1)
    source = _source(store, "a" * 32, 100, (raw,))
    ready = tmp_path / "ready"
    ready.mkdir()
    ready.chmod(0o2750)
    import omi_collector.capture.adapters.ready_bundles as ready_bundles

    original_fsync = ready_bundles.os.fsync
    failed = False

    def fail_target(descriptor: int) -> None:
        nonlocal failed
        descriptor_path = Path(f"/proc/self/fd/{descriptor}").readlink()
        matches = {
            "file": descriptor_path.name == "records.bin",
            "manifest": descriptor_path.name == "manifest.json",
            "stage": descriptor_path.name.endswith(".stage"),
            "parent": descriptor_path == ready,
        }
        if matches[target] and not failed:
            failed = True
            raise OSError(5, "simulated READY durability failure")
        original_fsync(descriptor)

    monkeypatch.setattr(ready_bundles.os, "fsync", fail_target)
    with pytest.raises(ReadyBundleError, match="durability I/O failure"):
        _publish(store, (source,), ready)
    assert failed

    with closing(database.open_connection(read_only=True)) as connection:
        stage = cast(
            tuple[str, str],
            connection.execute(
                "SELECT p.lifecycle, s.failure_reason FROM publication_cohort AS p, ready_output_stage AS s"
            ).fetchone(),
        )
        source_rows = connection.execute(
            "SELECT lifecycle, committed_count, (SELECT count(*) FROM capture_record WHERE source_id=?) "
            "FROM capture_source WHERE source_id=?",
            (source._source_id, source._source_id),
        ).fetchone()
    assert stage == ("staging", "durability_io_error")
    assert source_rows == ("sealed_waiting", 1, 1)
    assert database.list(StateKind.READY_PUBLICATION) == ()

    with pytest.raises(ReadyBundleError, match="durability I/O failure"):
        _publish(store, (source,), ready)
    with closing(database.open_connection(read_only=True)) as connection:
        assert (
            connection.execute(
                "SELECT count(*) FROM capture_record WHERE source_id=?", (source._source_id,)
            ).fetchone()[0]
            == 1
        )
    store.close()


def test_sql_ready_durability_latch_failure_propagates_shared_fatal_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    database = StateDB(tmp_path / "state.sqlite3")
    store = InternalAudioStore(database)
    source = _source(store, "a" * 32, 100, (_record(100, 1),))
    ready = tmp_path / "ready"
    ready.mkdir()
    ready.chmod(0o2750)
    import omi_collector.capture.adapters.ready_bundles as ready_bundles

    def fail_fsync(_descriptor: int) -> None:
        raise OSError(5, "simulated EIO")

    monkeypatch.setattr(ready_bundles.os, "fsync", fail_fsync)
    original_commit = store._commit
    commits = 0

    def fail_latch_commit() -> None:
        nonlocal commits
        commits += 1
        if commits == 3:
            raise sqlite3.DatabaseError("simulated latch SQL failure")
        original_commit()

    monkeypatch.setattr(store, "_commit", fail_latch_commit)
    with pytest.raises(FatalInternalAudioStorageError):
        _publish(store, (source,), ready)
    assert commits == 3
    store.close()


def test_sql_restart_reuses_materialized_output_before_rejecting_second_copy_for_space(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    database = StateDB(tmp_path / "state.sqlite3")
    store = InternalAudioStore(database)
    first_raw = _record(100, 1)
    second_raw = _record(103, 2)
    first = _source(store, "a" * 32, 100, (first_raw,))
    second = _source(store, "b" * 32, 103, (second_raw,))
    ready = tmp_path / "ready"
    ready.mkdir()
    ready.chmod(0o2750)
    import omi_collector.capture.adapters.ready_bundles as ready_bundles

    original_commit = ready_bundles._commit_typed_stage_once

    def stop_after_materialization(*_args: object, **_kwargs: object) -> None:
        raise ReadyBundleError("simulated restart after materialization")

    monkeypatch.setattr(ready_bundles, "_commit_typed_stage_once", stop_after_materialization)
    with pytest.raises(ReadyBundleError, match="after materialization"):
        _publish(store, (first, second), ready, frontier=104)
    monkeypatch.setattr(ready_bundles, "_commit_typed_stage_once", original_commit)
    first_destination = tuple(path for path in ready.iterdir() if not path.name.startswith("."))
    assert len(first_destination) == 1
    assert first_destination[0].joinpath("records.bin").read_bytes() == first_raw

    class _Space:
        f_frsize = 4096

        def __init__(self, available: int) -> None:
            self.f_bavail = available

    statvfs_calls = 0

    def changing_space(_path: Path) -> _Space:
        nonlocal statvfs_calls
        statvfs_calls += 1
        return _Space(10**9 if statvfs_calls == 1 else 0)

    monkeypatch.setattr(ready_bundles.os, "statvfs", changing_space)
    with pytest.raises(ReadyBundleError, match="insufficient filesystem space"):
        _publish(store, (first, second), ready, frontier=104)
    assert statvfs_calls == 2
    assert first_destination[0].joinpath("records.bin").read_bytes() == first_raw
    with closing(database.open_connection(read_only=True)) as connection:
        assert connection.execute(
            "SELECT count(*) FROM capture_record WHERE source_id=?", (second._source_id,)
        ).fetchone()[0] == 1
    store.close()


def test_sql_admission_requires_sealed_or_certified_prefix(tmp_path: Path) -> None:
    database = StateDB(tmp_path / "state.sqlite3")
    store = InternalAudioStore(database)
    sealed_raw = (_record(100, 1),)
    certified_raw = (_record(103, 2),)
    sealed = _source(store, "a" * 32, 100, sealed_raw)
    receiving = _source(store, "b" * 32, 101, (_record(101, 3),), lifecycle="receiving")
    prepared = _source(store, "c" * 32, 102, (_record(102, 4),), lifecycle="prepared")
    blocked = _source(store, "d" * 32, 103, certified_raw, lifecycle="blocked")
    _certify(store, blocked._source_id, certified_raw)
    uncertified = _source(store, "e" * 32, 104, (_record(104, 5),), lifecycle="blocked")
    _quarantine_unclassified(store, uncertified._source_id)
    ready = tmp_path / "ready"
    ready.mkdir()
    ready.chmod(0o2750)

    outcome = _publish(store, (sealed, receiving, prepared, blocked, uncertified), ready)

    assert outcome.state is ReadyOutcomeState.PUBLISHED
    assert outcome.stats is not None
    assert outcome.stats.source_count == 2
    assert _ready_bytes(outcome) == sealed_raw[0] + certified_raw[0]
    store.close()


def test_sql_publication_normalizes_exact_overlap_and_rejects_conflicts(tmp_path: Path) -> None:
    database = StateDB(tmp_path / "state.sqlite3")
    store = InternalAudioStore(database)
    first_raw = (_record(100, 1), _record(101, 2))
    second_raw = (_record(101, 2), _record(102, 3))
    first = _source(store, "a" * 32, 100, first_raw)
    second = _source(store, "b" * 32, 101, second_raw)
    ready = tmp_path / "ready"
    ready.mkdir()
    ready.chmod(0o2750)

    outcome = _publish(store, (first, second), ready)

    assert outcome.state is ReadyOutcomeState.PUBLISHED
    assert outcome.stats is not None
    assert outcome.stats.record_count == 3
    assert _ready_bytes(outcome) == first_raw[0] + first_raw[1] + second_raw[1]
    store.close()

    conflict_database = StateDB(tmp_path / "conflict.sqlite3")
    conflict_store = InternalAudioStore(conflict_database)
    left = _source(conflict_store, "c" * 32, 100, first_raw)
    right = _source(conflict_store, "d" * 32, 101, (_record(101, 9), second_raw[1]))
    conflict_ready = tmp_path / "conflict-ready"
    conflict_ready.mkdir()
    conflict_ready.chmod(0o2750)

    with pytest.raises(ReadyBundleError, match=r"overlap|conflict"):
        _publish(conflict_store, (left, right), conflict_ready)
    with closing(conflict_database.open_connection(read_only=True)) as connection:
        rows = cast(
            list[tuple[str, str, int]],
            connection.execute(
                "SELECT source_id, lifecycle, committed_count FROM capture_source ORDER BY source_id"
            ).fetchall(),
        )
    assert [(row[1], row[2]) for row in rows] == [("sealed_waiting", 2), ("sealed_waiting", 2)]
    assert not tuple(conflict_ready.iterdir())
    conflict_store.close()


def test_sql_certified_blocked_prefix_keeps_residual_rows_and_is_not_republished(tmp_path: Path) -> None:
    database = StateDB(tmp_path / "state.sqlite3")
    store = InternalAudioStore(database)
    blocked_raw = (_record(100, 1), _record(101, 2), _record(102, 3))
    full_raw = (_record(200, 4),)
    blocked = _source(store, "a" * 32, 100, blocked_raw, lifecycle="blocked")
    _certify(store, blocked._source_id, blocked_raw, count=2)
    full = _source(store, "f" * 32, 200, full_raw)
    ready = tmp_path / "ready"
    ready.mkdir()
    ready.chmod(0o2750)

    outcome = _publish(store, (blocked, full), ready, frontier=201)

    assert outcome.state is ReadyOutcomeState.PUBLISHED
    assert _ready_bytes(outcome) == b"".join(blocked_raw[:2]) + full_raw[0]
    with closing(database.open_connection(read_only=True)) as connection:
        blocked_state = cast(
            tuple[str, int, str | None, int | None, int],
            tuple(
                cast(
                    tuple[object, ...],
                    connection.execute(
                        "SELECT lifecycle, committed_count, coverage_digest, retirement_cursor, "
                        "(SELECT count(*) FROM capture_record WHERE source_id=?) "
                        "FROM capture_source WHERE source_id=?",
                        (blocked._source_id, blocked._source_id),
                    ).fetchone(),
                )
            ),
        )
        residual = cast(
            tuple[tuple[int, int, bytes], ...],
            tuple(
                connection.execute(
                    "SELECT ordinal, device_timestamp, payload FROM capture_record WHERE source_id=? ORDER BY ordinal",
                    (blocked._source_id,),
                ).fetchall()
            ),
        )
        full_state = cast(
            tuple[str, int | None],
            tuple(
                cast(
                    tuple[object, ...],
                    connection.execute(
                        "SELECT lifecycle, retirement_cursor FROM capture_source WHERE source_id=?", (full._source_id,)
                    ).fetchone(),
                )
            ),
        )
    assert blocked_state == ("covered", 3, sha256(b"".join(blocked_raw[:2])).hexdigest(), None, 3)
    assert [(row[0], row[1], row[2]) for row in residual] == [
        (index, 100 + index, raw[4:]) for index, raw in enumerate(blocked_raw)
    ]
    assert full_state == ("retiring", 1)

    before = tuple(sorted(path.name for path in ready.iterdir()))
    repeat = _publish(store, (blocked,), ready, frontier=102)

    assert repeat.state is ReadyOutcomeState.WAITING
    assert repeat.published == ()
    assert tuple(sorted(path.name for path in ready.iterdir())) == before
    with closing(database.open_connection(read_only=True)) as connection:
        assert (
            connection.execute(
                "SELECT count(*) FROM capture_record WHERE source_id=?", (blocked._source_id,)
            ).fetchone()[0]
            == 3
        )
    store.close()


def test_sql_full_active_replay_below_threshold_retires_without_second_bundle(tmp_path: Path) -> None:
    database = StateDB(tmp_path / "state.sqlite3")
    store = InternalAudioStore(database)
    raw = _record(100, 1)
    original = _source(store, "a" * 32, 100, (raw,))
    ready = tmp_path / "ready"
    ready.mkdir()
    ready.chmod(0o2750)
    first = _publish(store, (original,), ready, frontier=101)
    assert first.state is ReadyOutcomeState.PUBLISHED

    replay = _source(store, "b" * 32, 100, (raw,))
    outcome = _publish(store, (replay,), ready, threshold=0.05, frontier=101)

    assert outcome.state is ReadyOutcomeState.WAITING
    assert outcome.reason == "waiting_for_threshold"
    assert outcome.published == ()
    assert len(tuple(ready.iterdir())) == 1
    with closing(database.open_connection(read_only=True)) as connection:
        replay_state = cast(
            tuple[str, int | None, int],
            tuple(
                cast(
                    tuple[object, ...],
                    connection.execute(
                        "SELECT lifecycle, retirement_cursor, "
                        "(SELECT count(*) FROM capture_record WHERE source_id=?) "
                        "FROM capture_source WHERE source_id=?",
                        (replay._source_id, replay._source_id),
                    ).fetchone(),
                )
            ),
        )
    assert replay_state == ("retiring", 1, 0)

    repeat = _publish(store, (), ready, threshold=0.05, frontier=101)
    assert repeat.state is ReadyOutcomeState.WAITING
    assert repeat.published == ()
    assert len(tuple(ready.iterdir())) == 1
    store.close()


def test_sql_active_prefix_replay_publishes_only_unique_suffix(tmp_path: Path) -> None:
    database = StateDB(tmp_path / "state.sqlite3")
    store = InternalAudioStore(database)
    first_raw = _record(100, 1)
    suffix_raw = _record(101, 2)
    original = _source(store, "a" * 32, 100, (first_raw,))
    ready = tmp_path / "ready"
    ready.mkdir()
    ready.chmod(0o2750)
    first = _publish(store, (original,), ready, frontier=101)
    assert first.state is ReadyOutcomeState.PUBLISHED

    replay = _source(store, "b" * 32, 100, (first_raw, suffix_raw))
    outcome = _publish(store, (replay,), ready, frontier=102)

    assert outcome.state is ReadyOutcomeState.PUBLISHED
    assert len(outcome.published) == 1
    assert outcome.published[0].record_count == 1
    assert outcome.published[0].path.joinpath("records.bin").read_bytes() == suffix_raw
    assert len(tuple(ready.iterdir())) == 2
    store.close()


def test_sql_conflicting_active_overlap_blocks_source_and_stops_page_reads(tmp_path: Path) -> None:
    database = StateDB(tmp_path / "state.sqlite3")
    store = InternalAudioStore(database)
    original_raw = _record(100, 1)
    original = _source(store, "a" * 32, 100, (original_raw,))
    ready = tmp_path / "ready"
    ready.mkdir()
    ready.chmod(0o2750)
    first = _publish(store, (original,), ready, frontier=101)
    assert first.state is ReadyOutcomeState.PUBLISHED

    conflict = _source(store, "b" * 32, 100, (_record(100, 9),))
    with pytest.raises(ReadyBundleError, match="conflict"):
        _publish(store, (conflict,), ready, frontier=101)
    pages_after_conflict = len(conflict.page_calls)
    with closing(database.open_connection(read_only=True)) as connection:
        state = cast(
            tuple[str, str | None],
            tuple(
                cast(
                    tuple[object, ...],
                    connection.execute(
                        "SELECT lifecycle, block_reason FROM capture_source WHERE source_id=?",
                        (conflict._source_id,),
                    ).fetchone(),
                )
            ),
        )
    assert state == ("blocked", "ready_overlap_conflict")

    repeat = _publish(store, (conflict,), ready, frontier=101)
    assert repeat.state is ReadyOutcomeState.WAITING
    assert len(conflict.page_calls) == pages_after_conflict
    store.close()


def test_sql_shared_device_reserve_failure_happens_before_stage_intent(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    database = StateDB(tmp_path / "state.sqlite3")
    store = InternalAudioStore(database)
    source = _source(store, "a" * 32, 100, (_record(100, 1),))
    ready = tmp_path / "ready"
    ready.mkdir()
    ready.chmod(0o2750)
    import omi_collector.capture.adapters.ready_bundles as ready_bundles

    class _NoSpace:
        f_bavail = 0
        f_frsize = 4096

    monkeypatch.setattr(ready_bundles.os, "statvfs", lambda _path: _NoSpace())
    with pytest.raises(ReadyBundleError, match="insufficient filesystem space"):
        _publish(store, (source,), ready)
    with closing(database.open_connection(read_only=True)) as connection:
        stage_count = cast(int, connection.execute("SELECT count(*) FROM ready_output_stage").fetchone()[0])
        cohort = tuple(
            cast(
                tuple[object, ...],
                connection.execute("SELECT lifecycle, cursor FROM publication_cohort").fetchone(),
            )
        )
    assert stage_count == 0
    assert cohort[0] == "accepted"
    assert cohort[1] == 0
    assert not tuple(ready.iterdir())
    store.close()


def test_sql_final_coverage_cas_rejects_mutated_source_identity(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    database = StateDB(tmp_path / "state.sqlite3")
    store = InternalAudioStore(database)
    source = _source(store, "a" * 32, 100, (_record(100, 1),))
    ready = tmp_path / "ready"
    ready.mkdir()
    ready.chmod(0o2750)
    import omi_collector.capture.adapters.ready_bundles as ready_bundles

    original = ready_bundles._commit_typed_stage_once
    typed_original = cast(Callable[..., object], original)

    def mutate_source(*args: object, **kwargs: object) -> object:
        with closing(store.state_db.open_connection()) as connection:
            connection.execute("BEGIN IMMEDIATE")
            connection.execute(
                "UPDATE capture_source SET closure_frontier=? WHERE source_id=?",
                (encode_uint64(999), source._source_id),
            )
            connection.commit()
        return typed_original(*args, **kwargs)

    monkeypatch.setattr(ready_bundles, "_commit_typed_stage_once", mutate_source)
    with pytest.raises(ReadyBundleError, match="coverage commit"):
        _publish(store, (source,), ready)
    with closing(database.open_connection(read_only=True)) as connection:
        state = cast(
            tuple[str, int | None, int],
            tuple(
                cast(
                    tuple[object, ...],
                    connection.execute(
                        "SELECT lifecycle, retirement_cursor, "
                        "(SELECT count(*) FROM capture_record WHERE source_id=?) "
                        "FROM capture_source WHERE source_id=?",
                        (source._source_id, source._source_id),
                    ).fetchone(),
                )
            ),
        )
    assert state == ("sealed_waiting", None, 1)
    assert database.list(StateKind.READY_PUBLICATION) == ()

    monkeypatch.setattr(ready_bundles, "_commit_typed_stage_once", original)
    with closing(database.open_connection()) as connection:
        connection.execute("BEGIN IMMEDIATE")
        connection.execute("UPDATE capture_source SET closure_frontier=NULL WHERE source_id=?", (source._source_id,))
        connection.commit()
    resumed = _publish(store, (source,), ready)
    assert resumed.state is ReadyOutcomeState.PUBLISHED
    store.close()


@pytest.mark.parametrize("boundary", (1, 2, 3), ids=("cohort", "stage", "final"))
def test_sql_durable_third_state_fail_stops_publication(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, boundary: int
) -> None:
    database = StateDB(tmp_path / "state.sqlite3")
    store = InternalAudioStore(database)
    source = _source(store, "a" * 32, 100, (_record(100, 1),))
    ready = tmp_path / "ready"
    ready.mkdir()
    ready.chmod(0o2750)
    _original, injected = _third_state_transaction(store, boundary)
    monkeypatch.setattr(store, "publication_transaction", injected)

    with pytest.raises(InternalAudioStoreError, match="third state"):
        _publish(store, (source,), ready)

    assert store._failed is True
    store.close()


def test_sql_restart_resumes_persisted_cohort_before_new_selection(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    database = StateDB(tmp_path / "state.sqlite3")
    store = InternalAudioStore(database)
    first_raw = (_record(100, 1),)
    second_raw = (_record(103, 2),)
    first = _source(store, "a" * 32, 100, first_raw)
    second = _source(store, "b" * 32, 103, second_raw)
    ready = tmp_path / "ready"
    ready.mkdir()
    ready.chmod(0o2750)
    import omi_collector.capture.adapters.ready_bundles as ready_bundles

    original = ready_bundles._publish_typed_group
    typed_original = cast(Callable[..., object], original)
    calls = 0

    def crash_after_first(*args: object, **kwargs: object) -> object:
        nonlocal calls
        calls += 1
        result = typed_original(*args, **kwargs)
        if calls == 1:
            raise OSError("crash after first committed group")
        return result

    monkeypatch.setattr(ready_bundles, "_publish_typed_group", crash_after_first)
    with pytest.raises(ReadyBundleError):
        _publish(store, (first, second), ready)
    monkeypatch.setattr(ready_bundles, "_publish_typed_group", original)

    new = _source(store, "c" * 32, 500, (_record(500, 9),))
    outcome = _publish(store, (first, second, new), ready, frontier=501)

    assert outcome.state is ReadyOutcomeState.PUBLISHED
    assert _ready_bytes(outcome) == second_raw[0]
    assert not any(b"\x00\x00\x01\xf4" in path.joinpath("records.bin").read_bytes() for path in ready.iterdir())
    with closing(database.open_connection(read_only=True)) as connection:
        row = cast(
            tuple[str, int] | None,
            connection.execute("SELECT lifecycle, cursor FROM publication_cohort").fetchone(),
        )
    assert row is None
    store.close()


def test_sql_split_source_stays_sealed_until_all_groups_are_published(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    database = StateDB(tmp_path / "state.sqlite3")
    store = InternalAudioStore(database)
    source = _source(store, "a" * 32, 100, (_record(100, 1), _record(101, 2)))
    ready = tmp_path / "ready"
    ready.mkdir()
    ready.chmod(0o2750)
    import omi_collector.capture.adapters.ready_bundles as ready_bundles
    from omi_collector.capture.domain.opus_duration import RecordAudioLayout

    def split_layout(raw: bytes) -> RecordAudioLayout:
        return RecordAudioLayout(1, 2 if raw[3] == 100 else 4, 3 if raw[3] == 100 else None)

    monkeypatch.setattr(ready_bundles, "inspect_20ms_record", split_layout)
    original = ready_bundles._publish_typed_group
    typed_original = cast(Callable[..., object], original)
    calls = 0

    def crash_after_first(*args: object, **kwargs: object) -> object:
        nonlocal calls
        calls += 1
        result = typed_original(*args, **kwargs)
        if calls == 1:
            raise OSError("crash after first source slice")
        return result

    monkeypatch.setattr(ready_bundles, "_publish_typed_group", crash_after_first)
    with pytest.raises(ReadyBundleError):
        _publish(store, (source,), ready, frontier=102)
    with closing(database.open_connection(read_only=True)) as connection:
        lifecycle, retirement_cursor = cast(
            tuple[str, int | None],
            connection.execute(
                "SELECT lifecycle, retirement_cursor FROM capture_source WHERE source_id=?", (source._source_id,)
            ).fetchone(),
        )
    assert lifecycle == "sealed_waiting"
    assert retirement_cursor is None

    monkeypatch.setattr(ready_bundles, "_publish_typed_group", original)
    outcome = _publish(store, (source,), ready, frontier=102)

    assert outcome.state is ReadyOutcomeState.PUBLISHED
    with closing(database.open_connection(read_only=True)) as connection:
        lifecycle = cast(
            str,
            connection.execute(
                "SELECT lifecycle FROM capture_source WHERE source_id=?", (source._source_id,)
            ).fetchone()[0],
        )
    assert lifecycle == "retiring"
    with closing(database.open_connection(read_only=True)) as connection:
        retirement_cursor = cast(
            int,
            connection.execute(
                "SELECT retirement_cursor FROM capture_source WHERE source_id=?", (source._source_id,)
            ).fetchone()[0],
        )
    assert retirement_cursor == 2
    _publish(store, (), ready, frontier=102)
    with closing(database.open_connection(read_only=True)) as connection:
        assert (
            connection.execute("SELECT 1 FROM capture_source WHERE source_id=?", (source._source_id,)).fetchone()
            is None
        )
    store.close()


def test_sql_stage_freezes_ranges_and_preserves_unowned_collision(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    database = StateDB(tmp_path / "state.sqlite3")
    store = InternalAudioStore(database)
    raw = (_record(100, 1),)
    source = _source(store, "a" * 32, 100, raw)
    ready = tmp_path / "ready"
    ready.mkdir()
    ready.chmod(0o2750)
    import omi_collector.capture.adapters.ready_bundles as ready_bundles

    original = ready_bundles._write_typed_stage

    def interrupt(*args: object, **kwargs: object) -> None:
        del args, kwargs
        raise OSError("after stage intent")

    monkeypatch.setattr(ready_bundles, "_write_typed_stage", interrupt)
    with pytest.raises(ReadyBundleError):
        _publish(store, (source,), ready)
    with closing(database.open_connection(read_only=True)) as connection:
        stage = cast(
            tuple[str, str] | None,
            connection.execute("SELECT stage_id, owned_path FROM ready_output_stage").fetchone(),
        )
        ranges = cast(
            list[tuple[str, int, int]],
            connection.execute("SELECT source_id, start_ordinal, record_count FROM ready_stage_range").fetchall(),
        )
    assert stage is not None
    assert [(row[0], row[1], row[2]) for row in ranges] == [(source._source_id, 0, 1)]
    owned_path = Path(stage[1])
    owned_path.mkdir()
    owned_path.joinpath("foreign-marker").write_bytes(b"keep")

    with closing(database.open_connection()) as connection:
        connection.execute("BEGIN IMMEDIATE")
        connection.execute("DELETE FROM ready_output_stage WHERE stage_id=?", (stage[0],))
        connection.commit()
    monkeypatch.setattr(ready_bundles, "_write_typed_stage", original)
    with pytest.raises(ReadyBundleError):
        _publish(store, (source,), ready)
    assert owned_path.joinpath("foreign-marker").read_bytes() == b"keep"
    store.close()


def test_runtime_reopens_sql_owned_ready_stage_after_sigkill_and_finishes_publication(tmp_path: Path) -> None:
    config = tmp_path / "config.toml"
    config.write_text(
        '[pendant]\naddress = "AA:BB:CC:DD:EE:FF"\n[ready]\ntarget_audio_seconds = 120\n',
        encoding="utf-8",
    )
    layout = load_operator_config(config).storage
    database = StateDB(layout.collector.state_db)
    database.put(StateKind.MIGRATION, "internal_audio_cutover", {"format": 1, "phase": "COMPLETE", "fresh": True})
    store = InternalAudioStore(database)
    raw = _record(100, 1)
    _source(store, "a" * 32, 100, (raw,))
    store.close()
    ready = layout.publication.root
    ready.mkdir()
    ready.chmod(0o2750)

    context = multiprocessing.get_context("fork")
    receiver, sender = context.Pipe()
    process = context.Process(target=_pause_after_typed_stage, args=(database.path, ready, sender))
    try:
        process.start()
        sender.close()
        assert receiver.poll(10), "publication did not reach the durable ready-stage barrier"
        assert receiver.recv() == "staged"
        process.kill()
        process.join(10)
        assert process.exitcode == -9
        hidden = tuple(path for path in ready.iterdir() if path.name.startswith("."))
        assert len(hidden) == 1
        assert hidden[0].joinpath("records.bin").read_bytes() == raw

        assert open_runtime_state(layout).path == database.path
        reopened = InternalAudioStore(database)
        try:
            outcome = _publish(reopened, (_Source(reopened, "a" * 32),), ready)
            assert outcome.state is ReadyOutcomeState.PUBLISHED
            assert _ready_bytes(outcome) == raw
            assert not hidden[0].exists()
        finally:
            reopened.close()
    finally:
        if process.is_alive():
            process.kill()
            process.join(10)
        receiver.close()
        sender.close()


def test_sql_stage_reuses_frozen_clock_mapping_after_restart(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    database = StateDB(tmp_path / "state.sqlite3")
    store = InternalAudioStore(database)
    source = _source(store, "a" * 32, 100, (_record(100, 1),))
    ready = tmp_path / "ready"
    ready.mkdir()
    ready.chmod(0o2750)
    import omi_collector.capture.adapters.ready_bundles as ready_bundles
    from omi_collector.capture.adapters.clock_segments import ClockSegment

    original = ready_bundles._write_typed_stage

    def interrupt(*args: object, **kwargs: object) -> None:
        del args, kwargs
        raise OSError("after stage intent")

    old_clock = ClockSegmentMap((ClockSegment("old", 100, 101, 0.0, 0.0),))
    new_clock = ClockSegmentMap((ClockSegment("new", 100, 101, 10.0, 0.0),))
    monkeypatch.setattr(ready_bundles, "_write_typed_stage", interrupt)
    with pytest.raises(ReadyBundleError):
        finalize_typed_sources(
            store,
            (source,),
            ready,
            config=ReadyConfig(target_audio_seconds=0.02),
            clock_segments=old_clock,
            frontier=101,
            drained=True,
        )
    monkeypatch.setattr(ready_bundles, "_write_typed_stage", original)
    outcome = finalize_typed_sources(
        store,
        (source,),
        ready,
        config=ReadyConfig(target_audio_seconds=0.02),
        clock_segments=new_clock,
        frontier=101,
        drained=True,
    )
    assert outcome.state is ReadyOutcomeState.PUBLISHED
    manifest = outcome.published[0].path.joinpath("manifest.json").read_bytes()
    assert b'"observation_id":"old"' in manifest
    assert b'"observation_id":"new"' not in manifest
    store.close()


def test_staging_restart_resumes_persisted_group_instead_of_replanning_ready_overlap(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    database = StateDB(tmp_path / "state.sqlite3")
    store = InternalAudioStore(database)
    records = (_record(100, 1), _record(101, 2), _record(102, 3))
    source = _source(store, "a" * 32, 100, records)
    ready = tmp_path / "ready"
    ready.mkdir()
    ready.chmod(0o2750)

    seed_database = StateDB(tmp_path / "seed.sqlite3")
    with InternalAudioStore(seed_database) as seed_store:
        middle = _source(seed_store, "f" * 32, 101, (records[1],))
        seed_ready = tmp_path / "seed-ready"
        seed_ready.mkdir()
        seed_ready.chmod(0o2750)
        seeded = _publish(seed_store, (middle,), seed_ready, frontier=102)
        installed = seeded.published[0]
        shutil.copytree(installed.path, ready / installed.bundle_id)
    for item_id, payload in seed_database.list(StateKind.READY_PUBLICATION):
        database.put(StateKind.READY_PUBLICATION, item_id, payload)

    import omi_collector.capture.adapters.ready_bundles as ready_bundles

    original = ready_bundles._write_typed_stage

    def stop_after_intent(*_args: object, **_kwargs: object) -> None:
        raise OSError("stop after authoritative stage intent")

    monkeypatch.setattr(ready_bundles, "_write_typed_stage", stop_after_intent)
    with pytest.raises(ReadyBundleError, match="could not be published"):
        _publish(store, (source,), ready, frontier=103)
    with closing(database.open_connection(read_only=True)) as connection:
        frozen = cast(
            tuple[int, int],
            connection.execute("SELECT start_ordinal,record_count FROM ready_stage_range").fetchone(),
        )
    assert frozen == (0, 1)
    store.close()

    monkeypatch.setattr(ready_bundles, "_write_typed_stage", original)
    with InternalAudioStore(database) as restarted:
        outcome = _publish(restarted, (_Source(restarted, source._source_id),), ready, frontier=103)

    assert outcome.state is ReadyOutcomeState.PUBLISHED
    assert tuple(result.path.joinpath("records.bin").read_bytes() for result in outcome.published) == (
        records[0],
        records[2],
    )


def test_sql_subrange_reads_bound_each_page_and_source_lookup_scales_with_pages(tmp_path: Path) -> None:
    database = StateDB(tmp_path / "state.sqlite3")
    store = InternalAudioStore(database)
    records = tuple(_record(100 + index, index & 0xFF) for index in range(5))
    source = _source(store, "a" * 32, 100, records)
    ready = tmp_path / "ready"
    ready.mkdir()
    ready.chmod(0o2750)

    outcome = _publish(store, (source,), ready, threshold=0.02, page_limit=3, frontier=105)

    assert outcome.state is ReadyOutcomeState.PUBLISHED
    assert source.page_calls
    assert all(limit <= 3 for _cursor, limit in source.page_calls)
    assert 2 in {limit for _cursor, limit in source.page_calls}
    assert source.source_reads <= len(source.page_calls) * 3 + 12
    store.close()


@pytest.mark.parametrize("boundary", (1, 2, 3), ids=("cohort", "stage", "final"))
@pytest.mark.parametrize("rollback", (False, True), ids=("commit_then_raise", "rollback_then_raise"))
def test_sql_publication_reconciles_ambiguous_transaction_boundaries(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, boundary: int, rollback: bool
) -> None:
    database = StateDB(tmp_path / "state.sqlite3")
    store = InternalAudioStore(database)
    source = _source(store, "a" * 32, 100, (_record(100, 1),))
    ready = tmp_path / "ready"
    ready.mkdir()
    ready.chmod(0o2750)
    original, injected = _fault_transaction(store, boundary, rollback=rollback)
    monkeypatch.setattr(store, "publication_transaction", injected)

    outcome = _publish(store, (source,), ready)
    monkeypatch.setattr(store, "publication_transaction", original)

    assert outcome.state is ReadyOutcomeState.PUBLISHED
    assert len(outcome.published) == 1
    result = outcome.published[0]
    raw = _record(100, 1)
    assert result.record_count == 1
    assert result.records_sha256 == sha256(raw).hexdigest()
    assert result.path.joinpath("records.bin").read_bytes() == raw
    assert len(tuple(ready.iterdir())) == 1
    ledger = database.list(StateKind.READY_PUBLICATION)
    assert len(ledger) == 2
    assert len({item_id for item_id, _payload in ledger}) == 2
    assert _publication_database_state(database, source._source_id) == (None, "retiring", (1, 0), 0, 0)
    retry = _publish(store, (), ready)
    assert retry.state is ReadyOutcomeState.WAITING
    assert len(tuple(ready.iterdir())) == 1
    with closing(database.open_connection(read_only=True)) as connection:
        assert (
            connection.execute("SELECT 1 FROM capture_source WHERE source_id=?", (source._source_id,)).fetchone()
            is None
        )
    store.close()
