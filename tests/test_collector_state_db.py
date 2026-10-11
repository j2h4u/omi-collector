from __future__ import annotations

import sqlite3
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest

from omi_collector.capture.adapters.collector_state_db import StateDB, StateDBError, StateKind


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
    original_connect = db._connect
    rollback_called = False

    class FailingConnection:
        def __init__(self) -> None:
            self.connection = original_connect()

        def execute(self, statement: str) -> sqlite3.Cursor:
            if failure_point == "begin" and statement == "BEGIN IMMEDIATE":
                raise sqlite3.OperationalError("synthetic begin failure")
            return self.connection.execute(statement)

        def commit(self) -> None:
            if failure_point == "commit":
                raise sqlite3.OperationalError("synthetic commit failure")
            self.connection.commit()

        def rollback(self) -> None:
            nonlocal rollback_called
            rollback_called = True
            self.connection.close()
            raise sqlite3.OperationalError("synthetic rollback failure")

        def close(self) -> None:
            self.connection.close()

    monkeypatch.setattr(db, "_connect", FailingConnection)
    with pytest.raises(StateDBError, match="transaction failed") as error, db.transaction():
        pass
    assert isinstance(error.value.__cause__, sqlite3.OperationalError)
    assert rollback_called
