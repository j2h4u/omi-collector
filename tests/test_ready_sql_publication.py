from __future__ import annotations

import sqlite3
from contextlib import closing, contextmanager
from hashlib import sha256
from pathlib import Path
from struct import pack
from typing import cast

import pytest

import omi_collector.capture.adapters.ready_bundles as ready_bundles
from omi_collector.capture.adapters.collector_state_db import StateDB, StateKind
from omi_collector.capture.adapters.internal_audio_store import (
    AUDIO_RECORD_SIZE,
    InternalAudioStore,
)
from omi_collector.capture.adapters.ready_bundles import (
    ReadyBundleError,
    ReadyOutcome,
)
from omi_collector.capture.application.storage_fault import TransientInternalAudioStorageError


def _record(timestamp: int, fill: int) -> bytes:
    return pack(">I", timestamp) + bytes((2, 8, fill)) + bytes(AUDIO_RECORD_SIZE - 7)


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


def test_prepare_directory_inherits_setgid_from_parent(tmp_path: Path) -> None:
    ready = tmp_path / "ready"
    ready.mkdir()
    ready.chmod(0o2750)
    bundle = ready / "bundle"

    ready_bundles._prepare_directory(bundle)

    assert bundle.stat().st_mode & 0o7777 == 0o2750
    assert bundle.stat().st_gid == ready.stat().st_gid


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


def test_sql_publication_read_helper_closes_materialized_cursor() -> None:
    connection = sqlite3.connect(":memory:")
    cursor = connection.execute("SELECT 1 UNION ALL SELECT 2")

    assert ready_bundles._sql_all(cursor) == ((1,), (2,))
    with pytest.raises(sqlite3.ProgrammingError):
        cursor.fetchall()
    connection.close()
