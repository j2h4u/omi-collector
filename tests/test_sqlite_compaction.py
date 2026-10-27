from __future__ import annotations

import json
import os
import select
import signal
import sqlite3
import subprocess
import sys
import threading
from contextlib import closing, suppress
from pathlib import Path
from typing import cast

import pytest

from omi_collector.capture.adapters.collector_state_db import StateDB, StateDBError, StateKind
from omi_collector.capture.adapters.internal_audio_store import (
    AUDIO_RECORD_SIZE,
    CaptureTail,
    DrainCheckpoint,
    InternalAudioStore,
)
from omi_collector.capture.adapters.sqlite_compaction import (
    compaction_failure_outcome,
    pending_compaction_generation,
    vacuum_pending,
)
from omi_collector.capture.application.storage_fault import FatalInternalAudioStorageError

_KILL_DURING_VACUUM = r"""
import os
import sys
import threading
from pathlib import Path

from omi_collector.capture.adapters.collector_state_db import StateDB
from omi_collector.capture.adapters.sqlite_compaction import vacuum_pending

database = StateDB(Path(sys.argv[1]))
phase = sys.argv[2]
barrier_fd = int(sys.argv[3])
hold_fd = int(sys.argv[4])

def barrier():
    os.write(barrier_fd, b"V")
    os.read(hold_fd, 1)
    raise RuntimeError("SIGKILL barrier was released unexpectedly")

if phase == "during":
    original_open = database.open_connection

    class ProgressConnection:
        def __init__(self, inner):
            self.inner = inner
            self.in_vacuum = False
            self.progress_calls = 0

        def __getattr__(self, name):
            return getattr(self.inner, name)

        def execute(self, sql, parameters=()):
            if sql == "VACUUM":
                self.in_vacuum = True
            return self.inner.execute(sql, parameters)

        def set_progress_handler(self, callback, steps):
            if callback is None:
                return self.inner.set_progress_handler(None, 0)

            def progress():
                if self.in_vacuum:
                    self.progress_calls += 1
                    if self.progress_calls == 32:
                        barrier()
                return callback()

            return self.inner.set_progress_handler(progress, 1)

    def open_with_progress(*args, **kwargs):
        return ProgressConnection(original_open(*args, **kwargs))

    database.open_connection = open_with_progress
else:
    original_transaction = database.transaction

    def hold_marker_clear():
        barrier()
        return original_transaction()

    database.transaction = hold_marker_clear

vacuum_pending(database, 0, threading.Event())
raise RuntimeError("VACUUM escaped SIGKILL barrier")
"""


def _create_freelist(state_db: StateDB) -> None:
    with state_db.access():
        connection = state_db.open_connection()
        try:
            state_db.begin_write(connection)
            connection.executemany(
                "INSERT INTO state(kind,id,payload) VALUES(?,?,?)",
                (
                    (StateKind.QUALITY_EVENT.value, f"compaction-{index}", '{"value":"' + "x" * 3500 + '"}')
                    for index in range(1600)
                ),
            )
            connection.commit()
            state_db.end_write(connection)
            state_db.begin_write(connection)
            connection.execute("DELETE FROM state WHERE kind=?", (StateKind.QUALITY_EVENT.value,))
            connection.commit()
            state_db.end_write(connection)
        finally:
            state_db._close_managed_write(connection)


def _seed_live_audio(state_db: StateDB) -> tuple[str, bytes, bytes]:
    source_id = "a" * 32
    retained_tail = b"t" * 200
    record = (1).to_bytes(4, "big") + bytes((2, 8, 3)) + bytes(AUDIO_RECORD_SIZE - 7)
    with InternalAudioStore(state_db) as store:
        store.create_source(source_id, "attempt", 100, 102)
        store.prepare_leg(source_id, 100, 2)
        store.record_read_begin(source_id, 100, 2)
        store.commit_drain(
            source_id,
            0,
            (record,),
            DrainCheckpoint(1, CaptureTail(1, 1, 644, 444, retained_tail)),
        )
    return source_id, record, retained_tail


def _assert_live_audio(state_db: StateDB, source_id: str, record: bytes, retained_tail: bytes) -> None:
    with closing(state_db.open_connection(read_only=True)) as connection, state_db.access(connection):
        assert connection.execute("PRAGMA quick_check").fetchone()[0] == "ok"
        retained_record = cast(
            tuple[object, ...] | None,
            connection.execute(
                "SELECT device_timestamp,payload FROM capture_record WHERE source_id=? AND ordinal=0", (source_id,)
            ).fetchone(),
        )
        retained = cast(
            tuple[object, ...] | None,
            connection.execute("SELECT bytes FROM capture_tail WHERE source_id=?", (source_id,)).fetchone(),
        )
    assert retained_record is not None and retained is not None
    assert tuple(retained_record) == (1, record[4:])
    assert tuple(retained) == (retained_tail,)


def _kill_compaction_child(database_path: Path, phase: str) -> None:
    barrier_read, barrier_write = os.pipe()
    hold_read, hold_write = os.pipe()
    try:
        child = subprocess.Popen(
            (sys.executable, "-c", _KILL_DURING_VACUUM, str(database_path), phase, str(barrier_write), str(hold_read)),
            cwd=Path(__file__).resolve().parents[1],
            pass_fds=(barrier_write, hold_read),
            stderr=subprocess.PIPE,
        )
        try:
            os.close(barrier_write)
            os.close(hold_read)
            readable, _, _ = select.select((barrier_read,), (), (), 10)
            if not readable or os.read(barrier_read, 1) != b"V":
                raise AssertionError("child did not reach the VACUUM SIGKILL barrier")
        finally:
            if child.poll() is None:
                child.kill()
            try:
                _, stderr = child.communicate(timeout=5)
            except subprocess.TimeoutExpired:
                child.kill()
                _, stderr = child.communicate(timeout=5)
            assert child.returncode == -signal.SIGKILL, stderr.decode(errors="replace")
    finally:
        for descriptor in (barrier_read, barrier_write, hold_read, hold_write):
            with suppress(OSError):
                os.close(descriptor)


def test_vacuum_shrinks_database_and_preserves_live_state(tmp_path: Path) -> None:
    state_db = StateDB(tmp_path / "state.sqlite3")
    _create_freelist(state_db)
    source_id, record, retained_tail = _seed_live_audio(state_db)
    before = state_db.path.stat().st_size

    generation = pending_compaction_generation(state_db)
    assert generation == 0
    assert vacuum_pending(state_db, generation, threading.Event()) == "done"

    assert state_db.path.stat().st_size < before
    _assert_live_audio(state_db, source_id, record, retained_tail)
    assert state_db.get(StateKind.COMPACTION, "current") == {"generation": 0, "pending": False}
    assert pending_compaction_generation(StateDB(state_db.path)) is None


@pytest.mark.parametrize("phase", ("during", "after"))
def test_sigkill_restart_retains_audio_and_compaction_debt(tmp_path: Path, phase: str) -> None:
    database_path = tmp_path / "state.sqlite3"
    state_db = StateDB(database_path)
    _create_freelist(state_db)
    source_id, record, retained_tail = _seed_live_audio(state_db)
    ready_file = tmp_path / "ready" / "audio.opus"
    ready_file.parent.mkdir()
    ready_bytes = b"retained-ready-audio" * 100
    ready_file.write_bytes(ready_bytes)
    before = database_path.stat().st_size
    assert pending_compaction_generation(state_db) == 0

    _kill_compaction_child(database_path, phase)

    reopened = StateDB(database_path)
    _assert_live_audio(reopened, source_id, record, retained_tail)
    assert ready_file.read_bytes() == ready_bytes
    assert pending_compaction_generation(reopened) is (0 if phase == "during" else None)
    assert reopened.get(StateKind.COMPACTION, "current") == {"generation": 0, "pending": True}
    if phase == "after":
        assert database_path.stat().st_size < before
    assert vacuum_pending(reopened, 0, threading.Event()) == ("done" if phase == "during" else "parked")
    assert database_path.stat().st_size < before
    _assert_live_audio(StateDB(database_path), source_id, record, retained_tail)
    assert ready_file.read_bytes() == ready_bytes
    assert pending_compaction_generation(StateDB(database_path)) is None


def test_pre_cancelled_vacuum_retains_bootstrapped_debt(tmp_path: Path) -> None:
    database_path = tmp_path / "state.sqlite3"
    state_db = StateDB(database_path)
    _create_freelist(state_db)
    generation = pending_compaction_generation(state_db)
    assert generation == 0

    interrupt = threading.Event()
    interrupt.set()
    assert vacuum_pending(state_db, generation, interrupt) == "interrupted"
    assert state_db.get(StateKind.COMPACTION, "current") == {"generation": 0, "pending": True}
    assert pending_compaction_generation(StateDB(database_path)) == 0


def test_stale_vacuum_completion_does_not_clear_newer_generation(tmp_path: Path) -> None:
    state_db = StateDB(tmp_path / "state.sqlite3")
    state_db.put(StateKind.COMPACTION, "current", {"generation": 8, "pending": True})

    assert vacuum_pending(state_db, 7, threading.Event()) == "deferred"
    assert state_db.get(StateKind.COMPACTION, "current") == {"generation": 8, "pending": True}


class _InjectedSqliteError(sqlite3.OperationalError):
    def __init__(self, code: int) -> None:
        super().__init__("injected sqlite failure")
        self.sqlite_errorcode = code


class _Result:
    def __init__(self, value: tuple[object, ...] | None) -> None:
        self.value = value

    def fetchone(self) -> tuple[object, ...] | None:
        return self.value


class _VacuumConnection:
    def __init__(self, error: _InjectedSqliteError, interrupt: threading.Event, *, signal: bool) -> None:
        self.error = error
        self.interrupt = interrupt
        self.signal = signal
        self.closed = False

    def set_progress_handler(self, _callback: object, _steps: int) -> None:
        pass

    def execute(self, sql: str, _parameters: tuple[object, ...] = ()) -> _Result:
        if sql.startswith("SELECT payload FROM state"):
            return _Result((json.dumps({"generation": 5, "pending": True}),))
        if sql.startswith("SELECT 1 FROM capture_source"):
            return _Result(None)
        if sql == "PRAGMA freelist_count":
            return _Result((1,))
        if sql == "VACUUM":
            if self.signal:
                self.interrupt.set()
            raise self.error
        raise AssertionError(f"unexpected SQL: {sql}")

    def close(self) -> None:
        self.closed = True


@pytest.mark.parametrize(
    ("code", "expected", "fatal"),
    (
        (sqlite3.SQLITE_BUSY, "deferred", False),
        (sqlite3.SQLITE_LOCKED, "deferred", False),
        (sqlite3.SQLITE_FULL, "parked", False),
        (sqlite3.SQLITE_INTERRUPT, "deferred", False),
        (sqlite3.SQLITE_IOERR, None, True),
        (sqlite3.SQLITE_CORRUPT, None, True),
        (sqlite3.SQLITE_NOTADB, None, True),
    ),
)
def test_vacuum_sqlite_errors_are_classified_and_connection_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    code: int,
    expected: str | None,
    fatal: bool,
) -> None:
    state_db = StateDB(tmp_path / f"state-{code}.sqlite3")
    interrupt = threading.Event()
    connection = _VacuumConnection(_InjectedSqliteError(code), interrupt, signal=False)
    monkeypatch.setattr(StateDB, "open_connection", lambda _self: connection)

    if fatal:
        with pytest.raises(FatalInternalAudioStorageError):
            vacuum_pending(state_db, 5, interrupt)
    else:
        assert vacuum_pending(state_db, 5, interrupt) == expected
    assert connection.closed


def test_vacuum_interrupt_with_capture_intent_yields(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    state_db = StateDB(tmp_path / "state.sqlite3")
    interrupt = threading.Event()
    connection = _VacuumConnection(_InjectedSqliteError(sqlite3.SQLITE_INTERRUPT), interrupt, signal=True)
    monkeypatch.setattr(StateDB, "open_connection", lambda _self: connection)

    assert vacuum_pending(state_db, 5, interrupt) == "interrupted"
    assert connection.closed


@pytest.mark.parametrize(
    ("code", "expected"),
    ((sqlite3.SQLITE_BUSY, "deferred"), (sqlite3.SQLITE_LOCKED, "deferred"), (sqlite3.SQLITE_FULL, "parked")),
)
def test_retryable_vacuum_errors_defer_or_park_compaction(code: int, expected: str) -> None:
    assert compaction_failure_outcome(_InjectedSqliteError(code), threading.Event()) == expected


def test_pending_compaction_requires_freelist_and_vacuum_obeys_cooperative_budget(tmp_path: Path) -> None:
    state_db = StateDB(tmp_path / "state.sqlite3")
    state_db.put(StateKind.COMPACTION, "current", {"generation": 4, "pending": True})
    assert pending_compaction_generation(state_db) is None
    assert vacuum_pending(state_db, 4, threading.Event()) == "parked"

    _create_freelist(state_db)
    state_db.put(StateKind.COMPACTION, "current", {"generation": 5, "pending": True})
    assert pending_compaction_generation(state_db) == 5
    assert vacuum_pending(state_db, 5, threading.Event(), should_defer=lambda: True) == "deferred"


def test_cooperative_progress_interrupt_is_reported_as_interrupted(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    state_db = StateDB(tmp_path / "state.sqlite3")
    state_db.put(StateKind.COMPACTION, "current", {"generation": 5, "pending": True})
    interrupt = threading.Event()
    connection = _VacuumConnection(_InjectedSqliteError(sqlite3.SQLITE_INTERRUPT), interrupt, signal=False)
    monkeypatch.setattr(StateDB, "open_connection", lambda _self: connection)
    calls = 0

    def should_defer() -> bool:
        nonlocal calls
        calls += 1
        return calls >= 3

    assert vacuum_pending(state_db, 5, interrupt, should_defer) == "interrupted"
    assert connection.closed


@pytest.mark.parametrize("code", (sqlite3.SQLITE_IOERR, sqlite3.SQLITE_CORRUPT, sqlite3.SQLITE_NOTADB))
def test_uncertain_or_corrupt_vacuum_errors_are_fatal(code: int) -> None:
    with pytest.raises(FatalInternalAudioStorageError):
        compaction_failure_outcome(_InjectedSqliteError(code), threading.Event())


def test_unsignalled_sqlite_interrupt_defers_but_capture_interrupt_yields() -> None:
    error = _InjectedSqliteError(sqlite3.SQLITE_INTERRUPT)
    assert compaction_failure_outcome(error, threading.Event()) == "deferred"
    interrupt = threading.Event()
    interrupt.set()
    assert compaction_failure_outcome(error, interrupt) == "interrupted"


def test_sqlite_full_parks_without_writing_compaction_debt(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    state_db = StateDB(tmp_path / "state.sqlite3")
    state_db.put(StateKind.COMPACTION, "current", {"generation": 5, "pending": True})

    connection = _VacuumConnection(_InjectedSqliteError(sqlite3.SQLITE_FULL), threading.Event(), signal=False)
    monkeypatch.setattr(StateDB, "open_connection", lambda _self: connection)
    assert vacuum_pending(state_db, 5, threading.Event()) == "parked"
    assert connection.closed
    monkeypatch.undo()
    assert StateDB(state_db.path).get(StateKind.COMPACTION, "current") == {"generation": 5, "pending": True}


def test_malformed_json_marker_fails_closed_in_probe_and_vacuum(tmp_path: Path) -> None:
    state_db = StateDB(tmp_path / "state.sqlite3")
    state_db.put(StateKind.COMPACTION, "current", {"generation": 3, "pending": True})
    with closing(state_db.open_connection()) as connection:
        connection.execute(
            "UPDATE state SET payload='[]' WHERE kind=? AND id=?", (StateKind.COMPACTION.value, "current")
        )

    with pytest.raises(StateDBError, match="compaction marker is invalid"):
        pending_compaction_generation(state_db)
    with pytest.raises(FatalInternalAudioStorageError, match="compaction outcome is uncertain"):
        vacuum_pending(state_db, 3, threading.Event())
