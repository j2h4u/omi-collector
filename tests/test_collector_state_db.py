from __future__ import annotations

import asyncio
import sqlite3
import subprocess
import sys
import threading
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from pathlib import Path
from typing import NoReturn, cast

import pytest

from omi_collector.capture.adapters import collector_state_db
from omi_collector.capture.adapters.collector_state_db import StateDB, StateDBError, StateKind, _Operations
from omi_collector.capture.adapters.sqlite_admission import admit_exclusive, commit_with_busy_retry
from omi_collector.capture.application.storage_fault import (
    FatalInternalAudioStorageError,
    TransientInternalAudioStorageError,
)


@pytest.mark.parametrize(
    ("sqlite_errorcode", "expected"),
    [
        (None, StateDBError),
        (sqlite3.SQLITE_BUSY, TransientInternalAudioStorageError),
        (sqlite3.SQLITE_CORRUPT, FatalInternalAudioStorageError),
        (sqlite3.SQLITE_NOTADB, FatalInternalAudioStorageError),
    ],
)
@pytest.mark.parametrize("operation", ["get", "list", "put", "delete"])
def test_state_operation_classifies_sqlite_errorcode(
    operation: str, sqlite_errorcode: int | None, expected: type[Exception]
) -> None:
    failure = sqlite3.OperationalError("injected database failure")
    if sqlite_errorcode is not None:
        failure.sqlite_errorcode = sqlite_errorcode

    class FailingConnection:
        def execute(self, *_args: object, **_kwargs: object) -> NoReturn:
            raise failure

    operations = _Operations(cast(sqlite3.Connection, FailingConnection()))
    with pytest.raises(expected) as raised:
        if operation == "get":
            operations.get(StateKind.FIRMWARE, "current")
        elif operation == "list":
            operations.list(StateKind.FIRMWARE)
        elif operation == "put":
            operations.put(StateKind.FIRMWARE, "current", {"value": 1})
        else:
            operations.delete(StateKind.FIRMWARE, "current")
    assert getattr(raised.value, "sqlite_errorcode", None) == sqlite_errorcode
    assert raised.value.__cause__ is failure


def test_state_db_round_trip_is_canonical_and_reopens_in_a_fresh_process(tmp_path: Path) -> None:
    path = tmp_path / "collector state?.sqlite3"
    db = StateDB(path)
    db.put(StateKind.FIRMWARE, "latest", {"z": 1, "a": {"y": 2, "b": 3}, "unicode": "é"})
    db.integrity_check()

    assert db.get(StateKind.FIRMWARE, "latest") == {"a": {"b": 3, "y": 2}, "unicode": "é", "z": 1}
    connection = sqlite3.connect(path)
    assert connection.execute("SELECT payload FROM state").fetchone()[0] == '{"a":{"b":3,"y":2},"unicode":"é","z":1}'
    connection.close()
    code = (
        "from pathlib import Path; from omi_collector.capture.adapters.collector_state_db import StateDB, StateKind; "
        "assert StateDB(Path(__import__('sys').argv[1]), read_only=True).get(StateKind.FIRMWARE, 'latest') "
        "== {'a': {'b': 3, 'y': 2}, 'unicode': 'é', 'z': 1}"
    )
    subprocess.run([sys.executable, "-c", code, str(path)], check=True, timeout=10)


def test_transaction_commits_multiple_rows_or_rolls_everything_back(tmp_path: Path) -> None:
    db = StateDB(tmp_path / "state.sqlite3")
    db.put(StateKind.ATTEMPT, "attempt-a", {"checkpoint": 1})
    generation = db.get_generation()

    with pytest.raises(RuntimeError, match="abort"), db.transaction() as transaction:
        transaction.put(StateKind.ATTEMPT, "attempt-a", {"checkpoint": 2})
        transaction.put(StateKind.QUARANTINE, "quarantine-a", {"reason": "test"})
        transaction.delete(StateKind.ATTEMPT, "attempt-a")
        raise RuntimeError("abort")

    assert db.get(StateKind.ATTEMPT, "attempt-a") == {"checkpoint": 1}
    assert db.get(StateKind.QUARANTINE, "quarantine-a") is None
    assert db.get_generation() == generation

    with db.transaction() as transaction:
        transaction.put(StateKind.ATTEMPT, "attempt-a", {"checkpoint": 2})
        transaction.put(StateKind.READY_CLOSURE, "current", {"closures": []})
    assert db.get(StateKind.ATTEMPT, "attempt-a") == {"checkpoint": 2}
    assert db.get_generation() == generation + 1


def test_generation_changes_only_for_changed_publication_inputs(tmp_path: Path) -> None:
    db = StateDB(tmp_path / "state.sqlite3")
    db.put(StateKind.FIRMWARE, "latest", {"count": 1})
    db.put(StateKind.OPERATIONAL_STATUS, "current", {"state": "ok"})
    assert db.get_generation() == 0

    db.put(StateKind.CLOCK, "current", {"segment_count": 1})
    assert db.get_generation() == 1

    db.put(StateKind.ATTEMPT, "attempt-a", {"checkpoint": 1})
    assert db.get_generation() == 2
    db.put(StateKind.ATTEMPT, "attempt-a", {"checkpoint": 1})
    assert db.get_generation() == 2
    assert db.delete(StateKind.ATTEMPT, "attempt-a")
    assert db.get_generation() == 3
    assert not db.delete(StateKind.ATTEMPT, "attempt-a")
    assert db.get_generation() == 3


def test_concurrent_transactional_updates_do_not_lose_writes(tmp_path: Path) -> None:
    db = StateDB(tmp_path / "state.sqlite3")
    db.put(StateKind.CLOCK, "current", {"count": 0})

    def increment(_: int) -> None:
        for _ in range(10):
            with db.transaction() as transaction:
                current = transaction.get(StateKind.CLOCK, "current")
                assert current is not None
                count = current["count"]
                assert type(count) is int
                transaction.put(StateKind.CLOCK, "current", {"count": count + 1})

    with ThreadPoolExecutor(max_workers=8) as workers:
        tuple(workers.map(increment, range(8)))

    assert db.get(StateKind.CLOCK, "current") == {"count": 80}


def test_write_owner_rejects_a_new_or_preexisting_second_connection(tmp_path: Path) -> None:
    db = StateDB(tmp_path / "state.sqlite3")
    first = db.open_connection()
    second = db.open_connection()
    pending_cursor = second.execute("SELECT 1")
    try:
        with db.access(first):
            db.begin_write(first)
            first.execute("INSERT INTO state(kind,id,payload) VALUES ('firmware','owner','{}')").close()
            with pytest.raises(StateDBError, match="another state connection owns"):
                second.execute("SELECT 1")
            with pytest.raises(StateDBError, match="another state connection owns"):
                pending_cursor.fetchone()
            with pytest.raises(StateDBError, match="another state connection owns"), db.access(second):
                second.execute("SELECT 1")
            with pytest.raises(StateDBError, match="another state connection owns"):
                db.open_connection()
            with pytest.raises(StateDBError, match="another state connection owns"):
                db.get(StateKind.FIRMWARE, "owner")
            assert first.execute("SELECT payload FROM state WHERE id='owner'").fetchone()[0] == "{}"
            first.rollback()
            db.end_write(first)
        with db.access(second):
            assert second.execute("SELECT count(*) FROM state WHERE id='owner'").fetchone()[0] == 0
    finally:
        pending_cursor.close()
        first.close()
        second.close()


def test_read_connections_wait_longer_than_writer_connections(tmp_path: Path) -> None:
    db = StateDB(tmp_path / "state.sqlite3")

    with closing(db.open_connection(read_only=True)) as reader:
        assert reader.execute("PRAGMA busy_timeout").fetchone()[0] == 30_000
    with closing(db.open_connection()) as writer:
        assert writer.execute("PRAGMA busy_timeout").fetchone()[0] == 5_000


def test_external_shared_reader_allows_admission_but_delays_commit(tmp_path: Path) -> None:
    path = tmp_path / "state.sqlite3"
    db = StateDB(path)
    external = sqlite3.connect(path, isolation_level=None)
    external.execute("BEGIN")
    external.execute("SELECT count(*) FROM state").fetchone()
    started = threading.Event()
    finished = threading.Event()

    def write() -> None:
        started.set()
        db.put(StateKind.FIRMWARE, "after-reader", {"value": 1})
        finished.set()

    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(write)
        assert started.wait(2)
        assert not finished.wait(0.05)
        assert external.execute("SELECT count(*) FROM state WHERE id='after-reader'").fetchone()[0] == 0
        external.rollback()
        future.result(timeout=10)
    external.close()
    assert db.get(StateKind.FIRMWARE, "after-reader") == {"value": 1}


def test_immediate_writer_keeps_committed_rows_readable_until_commit(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "state.sqlite3"
    db = StateDB(path)
    db.put(StateKind.CLOCK, "current", {"value": "old"})
    commit_called = threading.Event()
    dml_done = threading.Event()
    armed = False
    base_connection = collector_state_db._GuardedConnection

    class SignalingConnection(base_connection):
        def commit(self) -> None:
            if armed:
                commit_called.set()
            super().commit()

    monkeypatch.setattr(collector_state_db, "_GuardedConnection", SignalingConnection)
    reader = sqlite3.connect(path, isolation_level=None, timeout=0)
    reader.execute("BEGIN")
    reader.execute("SELECT count(*) FROM state").fetchone()
    errors: list[Exception] = []

    def write() -> None:
        nonlocal armed
        try:
            with db.transaction() as transaction:
                transaction.put(StateKind.CLOCK, "current", {"value": "new"})
                armed = True
                dml_done.set()
        except (sqlite3.DatabaseError, StateDBError, TimeoutError) as error:
            errors.append(error)

    thread = threading.Thread(target=write)
    thread.start()
    try:
        assert dml_done.wait(5)
        assert commit_called.wait(5)
        assert reader.execute("SELECT payload FROM state WHERE id='current'").fetchone() == ('{"value":"old"}',)
        reader.rollback()
        thread.join(5)
    finally:
        reader.rollback()
        reader.close()
        if thread.is_alive():
            thread.join(5)
    assert not thread.is_alive()
    assert errors == []
    assert db.get(StateKind.CLOCK, "current") == {"value": "new"}
    assert db.get_generation() == 2


def test_loop_thread_state_access_fails_before_sql(tmp_path: Path) -> None:
    db = StateDB(tmp_path / "state.sqlite3")

    async def access_on_loop() -> None:
        with pytest.raises(StateDBError, match="event-loop thread"):
            db.get(StateKind.FIRMWARE, "missing")
        with pytest.raises(StateDBError, match="event-loop thread"):
            db.open_connection()

    asyncio.run(access_on_loop())


@pytest.mark.parametrize("code", [sqlite3.SQLITE_BUSY | (1 << 8), sqlite3.SQLITE_LOCKED])
def test_exclusive_admission_retries_only_exact_primary_busy(code: int) -> None:
    error = sqlite3.OperationalError("injected")
    error.sqlite_errorcode = code

    class FailingConnection:
        in_transaction = False
        calls = 0

        def execute(self, _query: str) -> NoReturn:
            self.calls += 1
            raise error

    connection = FailingConnection()
    with pytest.raises(sqlite3.OperationalError) as raised:
        admit_exclusive(cast(sqlite3.Connection, connection))
    assert raised.value is error
    assert connection.calls == 1


def test_commit_retries_direct_busy_once_without_replaying_sql() -> None:
    class Connection:
        in_transaction = True
        calls = 0

    connection = Connection()

    def commit() -> None:
        connection.calls += 1
        if connection.calls == 1:
            failure = sqlite3.OperationalError("busy")
            failure.sqlite_errorcode = sqlite3.SQLITE_BUSY
            raise failure
        connection.in_transaction = False

    commit_with_busy_retry(cast(sqlite3.Connection, connection), commit)
    assert connection.calls == 2


@pytest.mark.parametrize("code", [sqlite3.SQLITE_BUSY | (1 << 8), sqlite3.SQLITE_LOCKED])
def test_commit_propagates_extended_busy_and_locked(code: int) -> None:
    failure = sqlite3.OperationalError("injected")
    failure.sqlite_errorcode = code

    class Connection:
        in_transaction = True
        calls = 0

    connection = Connection()

    def commit() -> NoReturn:
        connection.calls += 1
        raise failure

    with pytest.raises(sqlite3.OperationalError) as raised:
        commit_with_busy_retry(cast(sqlite3.Connection, connection), commit)
    assert raised.value is failure
    assert connection.calls == 1


def test_commit_does_not_retry_busy_after_transaction_ended() -> None:
    failure = sqlite3.OperationalError("injected")
    failure.sqlite_errorcode = sqlite3.SQLITE_BUSY

    class Connection:
        in_transaction = False
        calls = 0

    connection = Connection()

    def commit() -> NoReturn:
        connection.calls += 1
        raise failure

    with pytest.raises(sqlite3.OperationalError) as raised:
        commit_with_busy_retry(cast(sqlite3.Connection, connection), commit)
    assert raised.value is failure
    assert connection.calls == 1


def test_rows_are_sorted_and_kind_and_identity_are_closed_and_validated(tmp_path: Path) -> None:
    db = StateDB(tmp_path / "state.sqlite3")
    db.put(StateKind.ATTEMPT, "b", {"n": 2})
    db.put(StateKind.ATTEMPT, "a", {"n": 1})

    assert db.list(StateKind.ATTEMPT) == (("a", {"n": 1}), ("b", {"n": 2}))
    with db.transaction() as transaction:
        assert transaction.list(StateKind.ATTEMPT, limit=1) == (("a", {"n": 1}),)
        assert transaction.list(StateKind.ATTEMPT, limit=0) == ()
        assert transaction.payload_bytes(StateKind.ATTEMPT) == len('{"n":1}') + len('{"n":2}')
        assert transaction.payload_bytes(StateKind.ATTEMPT, row_overhead=1) == len('{"n":1}') + len('{"n":2}') + 2
        with pytest.raises(StateDBError, match="limit is invalid"):
            transaction.list(StateKind.ATTEMPT, limit=-1)
        with pytest.raises(StateDBError, match="row overhead is invalid"):
            transaction.payload_bytes(StateKind.ATTEMPT, row_overhead=-1)
    with pytest.raises(StateDBError, match="kind is invalid"):
        db.get("attempt", "a")  # type: ignore[arg-type]
    with pytest.raises(StateDBError, match="identity is invalid"):
        db.put(StateKind.ATTEMPT, "", {})
    with pytest.raises(StateDBError, match="canonical JSON"):
        db.put(StateKind.ATTEMPT, "bad", {"value": float("nan")})
    with pytest.raises(StateDBError, match="use the active"), db.transaction():
        db.put(StateKind.CLOCK, "nested", {})


@pytest.mark.parametrize("version", [0, 2, 999])
def test_unsupported_database_state_fails_closed(tmp_path: Path, version: int) -> None:
    path = tmp_path / "unsupported.sqlite3"
    connection = sqlite3.connect(path)
    connection.execute(f"PRAGMA user_version = {version}")
    if version == 0:
        connection.execute("CREATE TABLE legacy (id TEXT)")
    connection.commit()
    connection.close()

    with pytest.raises(StateDBError):
        StateDB(path)


def test_malformed_payload_and_read_only_mode_fail_closed(tmp_path: Path) -> None:
    path = tmp_path / "state.sqlite3"
    db = StateDB(path)
    db.put(StateKind.CLOCK, "current", {"ok": True})
    connection = sqlite3.connect(path)
    connection.execute("UPDATE state SET payload = '{broken' WHERE kind = 'clock'")
    connection.commit()
    connection.close()

    with pytest.raises(StateDBError, match="malformed"):
        db.get(StateKind.CLOCK, "current")
    readonly = StateDB(path, read_only=True)
    with pytest.raises(StateDBError, match="malformed"):
        readonly.list(StateKind.CLOCK)
    with pytest.raises(StateDBError, match="does not exist"):
        StateDB(tmp_path / "missing.sqlite3", read_only=True)


def test_write_mode_creates_missing_directory_chain_without_following_symlinks(tmp_path: Path) -> None:
    nested_path = tmp_path / "new" / "nested" / "state.sqlite3"
    StateDB(nested_path)
    assert nested_path.parent.is_dir()

    real_directory = tmp_path / "real"
    real_directory.mkdir()
    linked_directory = tmp_path / "linked"
    linked_directory.symlink_to(real_directory, target_is_directory=True)
    with pytest.raises(StateDBError, match="regular directory"):
        StateDB(linked_directory / "nested" / "state.sqlite3")


@pytest.mark.parametrize("mode", [0o600, 0o666, 0o777])
def test_write_mode_corrects_database_permissions_and_read_only_does_not_mutate(tmp_path: Path, mode: int) -> None:
    path = tmp_path / "state.sqlite3"
    StateDB(path)
    path.chmod(mode)

    StateDB(path)
    assert path.stat().st_mode & 0o777 == 0o640

    path.chmod(0o600)
    StateDB(path, read_only=True)
    assert path.stat().st_mode & 0o777 == 0o600


def test_write_does_not_overwrite_a_malformed_existing_payload(tmp_path: Path) -> None:
    path = tmp_path / "state.sqlite3"
    db = StateDB(path)
    db.put(StateKind.CLOCK, "current", {"ok": True})
    connection = sqlite3.connect(path)
    connection.execute("UPDATE state SET payload = '{broken' WHERE kind = 'clock'")
    connection.commit()
    connection.close()

    with pytest.raises(StateDBError, match="malformed"):
        db.put(StateKind.CLOCK, "current", {"ok": False})


@pytest.mark.parametrize("failure_point", ["begin", "commit"])
def test_transaction_database_errors_are_normalized_and_rollback_cannot_mask_them(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, failure_point: str
) -> None:
    db = StateDB(tmp_path / "state.sqlite3")
    rollback_called = False

    class FailingConnection(collector_state_db._GuardedConnection):
        def execute(self, statement: str, parameters: object = (), /) -> sqlite3.Cursor:
            if failure_point == "begin" and statement in {"BEGIN EXCLUSIVE", "BEGIN IMMEDIATE"}:
                raise sqlite3.OperationalError("synthetic begin failure")
            return super().execute(statement, parameters)

        def commit(self) -> None:
            if failure_point == "commit":
                raise sqlite3.OperationalError("synthetic commit failure")
            super().commit()

        def rollback(self) -> None:
            nonlocal rollback_called
            rollback_called = True
            super().rollback()
            raise sqlite3.OperationalError("synthetic rollback failure")

    monkeypatch.setattr(collector_state_db, "_GuardedConnection", FailingConnection)
    with pytest.raises(StateDBError, match="transaction failed") as error, db.transaction():
        pass
    assert isinstance(error.value.__cause__, sqlite3.OperationalError)
    assert rollback_called


def test_unsettled_rollback_keeps_prior_fatal_and_closes_transaction(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    db = StateDB(tmp_path / "state.sqlite3")
    fatal = FatalInternalAudioStorageError("original corruption")

    class FailingRollbackConnection(collector_state_db._GuardedConnection):
        def rollback(self) -> None:
            raise sqlite3.OperationalError("rollback did not settle")

    monkeypatch.setattr(collector_state_db, "_GuardedConnection", FailingRollbackConnection)
    with pytest.raises(FatalInternalAudioStorageError) as raised, db.transaction() as transaction:
        transaction.put(StateKind.FIRMWARE, "current", {"value": 1})
        raise fatal
    assert raised.value is fatal
    assert db.get(StateKind.FIRMWARE, "current") is None
