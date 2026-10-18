from __future__ import annotations

import multiprocessing
import os
import shutil
import sqlite3
from collections.abc import Callable
from contextlib import closing, contextmanager
from hashlib import sha256
from json import loads
from multiprocessing.connection import Connection
from pathlib import Path
from struct import pack
from typing import cast

import pytest

import omi_collector.capture.adapters.ready_bundles as ready_bundles
from omi_collector.capture.adapters.clock_segments import ClockSegment, ClockSegmentMap
from omi_collector.capture.adapters.collector_state_db import StateDB, StateKind
from omi_collector.capture.adapters.internal_audio_store import (
    AUDIO_RECORD_SIZE,
    DrainCheckpoint,
    InternalAudioStore,
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
from omi_collector.capture.application.storage_fault import TransientInternalAudioStorageError
from omi_collector.capture.domain.clock_state import ClockIntent, ClockRange, ClockState
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


def _boundary_sources(store: InternalAudioStore, records: tuple[bytes, bytes, bytes]) -> tuple[_Source, _Source]:
    return (
        _source(store, "a" * 32, 100, records[:2]),
        _source(store, "b" * 32, 102, records[2:]),
    )


def _boundary_publish(
    store: InternalAudioStore, sources: tuple[_Source, _Source], ready: Path, clock: ClockSegmentMap
) -> ReadyOutcome:
    return finalize_typed_sources(
        store,
        sources,
        ready,
        config=ReadyConfig(target_audio_seconds=0.02),
        clock_segments=clock,
        frontier=103,
        drained=True,
    )


def _empty_window_clock() -> ClockSegmentMap:
    return ClockSegmentMap.from_state(
        ClockState(
            (ClockRange(100, 103, 0.0, 0.0, "confirmed", "same"),),
            ClockIntent("empty", "uncertain", 0, 0, 0.0, 101, 101),
        )
    )


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


def _assert_operator_access_bundle(path: Path, ready: Path) -> None:
    assert path.stat().st_mode & 0o7777 == 0o2750
    assert path.stat().st_gid == ready.stat().st_gid
    assert path.joinpath("manifest.json").stat().st_gid == ready.stat().st_gid
    assert path.joinpath("records.bin").stat().st_gid == ready.stat().st_gid


def test_operator_access_validation_rejects_directory_without_inherited_setgid(tmp_path: Path) -> None:
    ready = tmp_path / "ready"
    ready.mkdir()
    ready.chmod(0o2750)
    bundle = ready / "bundle"
    bundle.mkdir(mode=0o2750)
    bundle.chmod(0o750)

    with pytest.raises(ReadyBundleError, match="did not inherit operator access"):
        ready_bundles._require_operator_ready_directory(bundle)


def test_typed_bundle_sync_rejects_files_outside_operator_group(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    ready = tmp_path / "ready"
    ready.mkdir()
    ready.chmod(0o2750)
    bundle = ready / "bundle"
    bundle.mkdir(mode=0o2750)
    (bundle / "manifest.json").write_text("{}", encoding="utf-8")
    (bundle / "records.bin").write_bytes(b"synthetic")
    monkeypatch.setattr(ready_bundles, "_typed_files_match", lambda *_args, **_kwargs: True)
    original_stat = Path.stat

    def wrong_records_group(path: Path, *, follow_symlinks: bool = True) -> os.stat_result:
        result = original_stat(path, follow_symlinks=follow_symlinks)
        if path == bundle / "records.bin":
            fields = list(result)
            fields[5] = result.st_gid + 1
            return os.stat_result(fields)
        return result

    monkeypatch.setattr(Path, "stat", wrong_records_group)

    with pytest.raises(ReadyBundleError, match="do not inherit the operator group"):
        ready_bundles._sync_typed_bundle(bundle, cast(ready_bundles._TypedGroup, object()), require_bundle_name=True)


def _fault_transaction(
    store: InternalAudioStore,
    boundary: int,
    *,
    rollback: bool,
    sqlite_errorcode: int | None = None,
    classified_busy: bool = False,
):
    original = store.publication_transaction
    calls = 0

    def failure(message: str) -> sqlite3.DatabaseError | TransientInternalAudioStorageError:
        if classified_busy:
            return TransientInternalAudioStorageError(message, sqlite_errorcode=sqlite3.SQLITE_BUSY)
        error = sqlite3.DatabaseError(message)
        if sqlite_errorcode is not None:
            error.sqlite_errorcode = sqlite_errorcode
        return error

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
                raise failure("simulated rollback")
        with original() as transaction:
            yield transaction
        raise failure("simulated commit ambiguity")

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
    for result in outcome.published:
        _assert_operator_access_bundle(result.path, ready)
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
        source_rows = cast(
            tuple[str, int, int] | None,
            connection.execute(
                "SELECT lifecycle, committed_count, (SELECT count(*) FROM capture_record WHERE source_id=?) "
                "FROM capture_source WHERE source_id=?",
                (source._source_id, source._source_id),
            ).fetchone(),
        )
    assert tuple(stage) == ("staging", "durability_io_error")
    assert source_rows is not None and tuple(source_rows) == ("sealed_waiting", 1, 1)
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


def test_sql_ready_latch_does_not_swallow_corrupt_database_fault(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    database = StateDB(tmp_path / "state.sqlite3")
    with InternalAudioStore(database) as store:
        source = _source(store, "a" * 32, 100, (_record(100, 1),))
        ready = tmp_path / "ready"
        ready.mkdir()
        ready.chmod(0o2750)
        import omi_collector.capture.adapters.ready_bundles as ready_bundles

        monkeypatch.setattr(ready_bundles.os, "fsync", lambda _descriptor: (_ for _ in ()).throw(OSError(5, "EIO")))
        monkeypatch.setattr(ready_bundles, "_typed_durability_failure_is_latched", lambda *_args: True)
        original_commit = store._commit
        commits = 0
        fatal = FatalInternalAudioStorageError(
            "canonical audio database is corrupt", sqlite_errorcode=sqlite3.SQLITE_CORRUPT
        )

        def fail_latch_commit() -> None:
            nonlocal commits
            commits += 1
            if commits == 3:
                raise fatal
            original_commit()

        monkeypatch.setattr(store, "_commit", fail_latch_commit)
        with pytest.raises(FatalInternalAudioStorageError) as raised:
            _publish(store, (source,), ready)

        assert raised.value is fatal
        assert commits == 3


def test_sql_ready_eio_refusal_latch_readback_busy_remains_fatal(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    database = StateDB(tmp_path / "state.sqlite3")
    with InternalAudioStore(database) as store:
        source = _source(store, "a" * 32, 100, (_record(100, 1),))
        ready = tmp_path / "ready"
        ready.mkdir()
        ready.chmod(0o2750)
        import omi_collector.capture.adapters.ready_bundles as ready_bundles

        monkeypatch.setattr(
            ready_bundles.os,
            "fsync",
            lambda _descriptor: (_ for _ in ()).throw(OSError(5, "simulated EIO")),
        )
        original_commit = store._commit
        commits = 0
        latch_failed = False

        def fail_latch_commit() -> None:
            nonlocal commits, latch_failed
            commits += 1
            if commits == 3:
                latch_failed = True
                raise sqlite3.DatabaseError("simulated latch SQL failure")
            original_commit()

        monkeypatch.setattr(store, "_commit", fail_latch_commit)
        busy = TransientInternalAudioStorageError("database is busy", sqlite_errorcode=sqlite3.SQLITE_BUSY)
        original_open_connection = database.open_connection

        def fail_latch_readback(*, read_only: bool = False) -> sqlite3.Connection:
            if latch_failed and read_only:
                raise busy
            return original_open_connection(read_only=read_only)

        monkeypatch.setattr(database, "open_connection", fail_latch_readback)

        with pytest.raises(FatalInternalAudioStorageError):
            _publish(store, (source,), ready)


def test_sql_ready_rename_eio_latches_before_custody_commit(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    database = StateDB(tmp_path / "state.sqlite3")
    store = InternalAudioStore(database)
    source = _source(store, "a" * 32, 100, (_record(100, 1),))
    ready = tmp_path / "ready"
    ready.mkdir()
    ready.chmod(0o2750)
    original_rename = Path.rename

    def fail_stage_rename(path: Path, target: Path) -> Path:
        if path.name.endswith(".stage"):
            raise OSError(5, "simulated rename EIO")
        return original_rename(path, target)

    monkeypatch.setattr(Path, "rename", fail_stage_rename)
    with pytest.raises(ReadyBundleError, match="durability I/O failure"):
        _publish(store, (source,), ready)
    with closing(database.open_connection(read_only=True)) as connection:
        assert (
            connection.execute("SELECT failure_reason FROM ready_output_stage").fetchone()[0] == "durability_io_error"
        )
        assert (
            connection.execute(
                "SELECT count(*) FROM capture_record WHERE source_id=?", (source._source_id,)
            ).fetchone()[0]
            == 1
        )
    with pytest.raises(ReadyBundleError, match="durability I/O failure"):
        _publish(store, (source,), ready)
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
        assert (
            connection.execute(
                "SELECT count(*) FROM capture_record WHERE source_id=?", (second._source_id,)
            ).fetchone()[0]
            == 1
        )
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

    with pytest.raises(FatalInternalAudioStorageError, match="third state"):
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
        'pendant_address = "AA:BB:CC:DD:EE:FF"\nbundle_min_audio_seconds = 120\n',
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
        _assert_operator_access_bundle(hidden[0], ready)
        assert hidden[0].joinpath("records.bin").read_bytes() == raw

        assert open_runtime_state(layout).path == database.path
        reopened = InternalAudioStore(database)
        try:
            outcome = _publish(reopened, (_Source(reopened, "a" * 32),), ready)
            assert outcome.state is ReadyOutcomeState.PUBLISHED
            assert _ready_bytes(outcome) == raw
            _assert_operator_access_bundle(outcome.published[0].path, ready)
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


@pytest.mark.parametrize("boundary", ["empty_uncertain_window", "overflow_clear_next_to_unknown"])
def test_sql_stage_restarts_with_original_equal_mapping_boundaries(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, boundary: str
) -> None:
    if boundary == "empty_uncertain_window":
        records = (_record(100, 1), _record(101, 2), _record(102, 3))
        clock = _empty_window_clock()
    else:
        records = (_record((1 << 32) - 1, 1), _record(101, 2), _record(102, 3))
        clock = ClockSegmentMap((ClockSegment("overflow", 100, 101, 1.0, 0.0),))

    fresh_ready = tmp_path / "fresh-ready"
    fresh_ready.mkdir()
    fresh_ready.chmod(0o2750)
    with InternalAudioStore(StateDB(tmp_path / "fresh.sqlite3")) as fresh_store:
        fresh = _boundary_publish(fresh_store, _boundary_sources(fresh_store, records), fresh_ready, clock)
    assert fresh.state is ReadyOutcomeState.PUBLISHED
    fresh_bundle = fresh.published[0]
    expected_manifest = fresh_bundle.path.joinpath("manifest.json").read_bytes()
    expected_ranges = cast(list[dict[str, object]], cast(dict[str, object], loads(expected_manifest))["time_ranges"])
    assert [(item["start_sequence"], item["next_sequence"], item["utc"] is not None) for item in expected_ranges] == [
        (100, 101, boundary == "empty_uncertain_window"),
        (101, 103, boundary == "empty_uncertain_window"),
    ]

    database = StateDB(tmp_path / "restarted.sqlite3")
    ready = tmp_path / "restart-ready"
    ready.mkdir()
    ready.chmod(0o2750)
    import omi_collector.capture.adapters.ready_bundles as ready_bundles

    original = ready_bundles._write_typed_stage

    def stop_after_intent(*_args: object, **_kwargs: object) -> None:
        raise OSError("restart after durable stage intent")

    with InternalAudioStore(database) as store:
        sources = _boundary_sources(store, records)
        monkeypatch.setattr(ready_bundles, "_write_typed_stage", stop_after_intent)
        with pytest.raises(ReadyBundleError, match="could not be published"):
            _boundary_publish(store, sources, ready, clock)
    monkeypatch.setattr(ready_bundles, "_write_typed_stage", original)
    with closing(database.open_connection(read_only=True)) as connection:
        rows = cast(
            list[sqlite3.Row],
            connection.execute("SELECT time_range_ordinal FROM ready_stage_range ORDER BY range_ordinal").fetchall(),
        )
        frozen_ordinals = tuple(row[0] for row in rows)
    assert frozen_ordinals == (0, 1, 1)

    with InternalAudioStore(database) as restarted:
        resumed = _boundary_publish(
            restarted,
            (_Source(restarted, "a" * 32), _Source(restarted, "b" * 32)),
            ready,
            ClockSegmentMap(()),
        )
    assert resumed.state is ReadyOutcomeState.PUBLISHED
    resumed_bundle = resumed.published[0]
    assert resumed_bundle.bundle_id == fresh_bundle.bundle_id
    assert (
        resumed_bundle.path.joinpath("records.bin").read_bytes()
        == fresh_bundle.path.joinpath("records.bin").read_bytes()
    )
    assert resumed_bundle.path.joinpath("manifest.json").read_bytes() == expected_manifest


@pytest.mark.parametrize(
    ("mutation", "reason"),
    [
        ("skip_ordinal", "ordinals are invalid"),
        ("reorder_ordinal", "ordinals are invalid"),
        ("change_mapping_within_range", "continuation changed"),
    ],
)
def test_sql_stage_rejects_changed_frozen_range_partition(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mutation: str, reason: str
) -> None:
    database = StateDB(tmp_path / "state.sqlite3")
    ready = tmp_path / "ready"
    ready.mkdir()
    ready.chmod(0o2750)
    records = (_record(100, 1), _record(101, 2), _record(102, 3))
    import omi_collector.capture.adapters.ready_bundles as ready_bundles

    original = ready_bundles._write_typed_stage

    def stop_after_intent(*_args: object, **_kwargs: object) -> None:
        raise OSError("restart after durable stage intent")

    with InternalAudioStore(database) as store:
        monkeypatch.setattr(ready_bundles, "_write_typed_stage", stop_after_intent)
        with pytest.raises(ReadyBundleError, match="could not be published"):
            _boundary_publish(store, _boundary_sources(store, records), ready, _empty_window_clock())
    monkeypatch.setattr(ready_bundles, "_write_typed_stage", original)
    with closing(database.open_connection()) as connection:
        if mutation == "skip_ordinal":
            connection.execute("UPDATE ready_stage_range SET time_range_ordinal=2 WHERE range_ordinal=1")
        elif mutation == "reorder_ordinal":
            connection.execute("UPDATE ready_stage_range SET time_range_ordinal=1 WHERE range_ordinal=0")
        else:
            connection.execute(
                "UPDATE ready_stage_range SET utc_offset_seconds=1.0, utc_start=103, utc_end=103 WHERE range_ordinal=2"
            )
        connection.commit()

    with InternalAudioStore(database) as restarted:
        with pytest.raises(ReadyBundleError, match=reason):
            _boundary_publish(
                restarted,
                (_Source(restarted, "a" * 32), _Source(restarted, "b" * 32)),
                ready,
                ClockSegmentMap(()),
            )
        assert restarted.get_source("a" * 32) is not None
        assert restarted.get_source("b" * 32) is not None
    assert not tuple(ready.iterdir())


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
    assert tuple(frozen) == (0, 1)
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
@pytest.mark.parametrize("sqlite_errorcode", (None, sqlite3.SQLITE_BUSY), ids=("ordinary", "busy"))
def test_sql_publication_reconciles_ambiguous_transaction_boundaries(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, boundary: int, rollback: bool, sqlite_errorcode: int | None
) -> None:
    database = StateDB(tmp_path / "state.sqlite3")
    store = InternalAudioStore(database)
    source = _source(store, "a" * 32, 100, (_record(100, 1),))
    ready = tmp_path / "ready"
    ready.mkdir()
    ready.chmod(0o2750)
    original, injected = _fault_transaction(store, boundary, rollback=rollback, sqlite_errorcode=sqlite_errorcode)
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
    publication_entry = database.get(StateKind.READY_PUBLICATION, result.bundle_id)
    assert publication_entry is not None
    publication_dates = tuple(
        publication_entry[field] for field in ("formed_on_utc", "recorded_on_utc_min", "recorded_on_utc_max")
    )
    assert ready_bundles._valid_publication_dates(publication_entry)
    assert publication_dates[1:] == (None, None)
    ledger = database.list(StateKind.READY_PUBLICATION)
    assert len(ledger) == 2
    assert len({item_id for item_id, _payload in ledger}) == 2
    assert _publication_database_state(database, source._source_id) == (None, "retiring", (1, 0), 0, 0)
    retry = _publish(store, (), ready)
    assert retry.state is ReadyOutcomeState.WAITING
    retried_entry = database.get(StateKind.READY_PUBLICATION, result.bundle_id)
    assert retried_entry is not None
    assert (
        tuple(retried_entry[field] for field in ("formed_on_utc", "recorded_on_utc_min", "recorded_on_utc_max"))
        == publication_dates
    )
    assert len(tuple(ready.iterdir())) == 1
    with closing(database.open_connection(read_only=True)) as connection:
        assert (
            connection.execute("SELECT 1 FROM capture_source WHERE source_id=?", (source._source_id,)).fetchone()
            is None
        )
    store.close()


@pytest.mark.parametrize("boundary", (1, 2, 3), ids=("cohort", "stage", "final"))
@pytest.mark.parametrize("rollback", (False, True), ids=("commit_then_raise", "rollback_then_raise"))
def test_sql_publication_reconciles_classified_busy_at_transaction_boundaries(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, boundary: int, rollback: bool
) -> None:
    database = StateDB(tmp_path / "state.sqlite3")
    store = InternalAudioStore(database)
    source = _source(store, "a" * 32, 100, (_record(100, 1),))
    ready = tmp_path / "ready"
    ready.mkdir()
    ready.chmod(0o2750)
    original, injected = _fault_transaction(store, boundary, rollback=rollback, classified_busy=True)
    monkeypatch.setattr(store, "publication_transaction", injected)

    outcome = _publish(store, (source,), ready)
    monkeypatch.setattr(store, "publication_transaction", original)

    assert outcome.state is ReadyOutcomeState.PUBLISHED
    assert len(outcome.published) == 1
    assert outcome.published[0].path.joinpath("records.bin").read_bytes() == _record(100, 1)
    store.close()


@pytest.mark.parametrize("sqlite_errorcode", (sqlite3.SQLITE_CORRUPT, sqlite3.SQLITE_NOTADB))
def test_sql_publication_stops_on_structured_corrupt_commit(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, sqlite_errorcode: int
) -> None:
    database = StateDB(tmp_path / "state.sqlite3")
    with InternalAudioStore(database) as store:
        source = _source(store, "a" * 32, 100, (_record(100, 1),))
        ready = tmp_path / "ready"
        ready.mkdir()
        ready.chmod(0o2750)
        _original, injected = _fault_transaction(store, 1, rollback=True, sqlite_errorcode=sqlite_errorcode)
        monkeypatch.setattr(store, "publication_transaction", injected)

        with pytest.raises(FatalInternalAudioStorageError) as raised:
            _publish(store, (source,), ready)

        assert raised.value.sqlite_errorcode == sqlite_errorcode
        assert store.get_source(source._source_id) is not None
        assert not tuple(ready.iterdir())


def test_sql_publication_preserves_first_corruption_fatal_after_rollback_cleanup_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    database = StateDB(tmp_path / "state.sqlite3")
    with InternalAudioStore(database) as store:
        source = _source(store, "a" * 32, 100, (_record(100, 1),))
        ready = tmp_path / "ready"
        ready.mkdir()
        ready.chmod(0o2750)
        _original, injected = _fault_transaction(store, 1, rollback=True, sqlite_errorcode=sqlite3.SQLITE_CORRUPT)
        monkeypatch.setattr(store, "publication_transaction", injected)
        rollback = store._rollback

        def failing_cleanup() -> None:
            rollback()
            raise RuntimeError("later cleanup failure")

        monkeypatch.setattr(store, "_rollback", failing_cleanup)
        with pytest.raises(FatalInternalAudioStorageError) as raised:
            _publish(store, (source,), ready)
        assert raised.value.sqlite_errorcode == sqlite3.SQLITE_CORRUPT
        assert store.get_source(source._source_id) is not None


def test_sql_publication_read_helper_closes_materialized_cursor() -> None:
    connection = sqlite3.connect(":memory:")
    cursor = connection.execute("SELECT 1 UNION ALL SELECT 2")

    assert ready_bundles._sql_all(cursor) == ((1,), (2,))
    with pytest.raises(sqlite3.ProgrammingError):
        cursor.fetchall()
    connection.close()
