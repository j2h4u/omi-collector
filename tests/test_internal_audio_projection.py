from __future__ import annotations

import hashlib
import json
import sqlite3
import threading
from contextlib import closing
from pathlib import Path
from typing import cast

import pytest

from omi_collector.capture.adapters.collector_state_db import StateDB
from omi_collector.capture.adapters.internal_audio_store import (
    DrainCheckpoint,
    InternalAudioStore,
    clock_mapping_revision,
    refresh_utc_projection,
)
from omi_collector.capture.domain.clock_state import ClockRange, ClockState


def _record(timestamp: int, fill: int) -> bytes:
    return timestamp.to_bytes(4, "big") + bytes([fill]) * 440


def _create_receiving_source(
    db: StateDB,
    records: tuple[bytes, ...],
    *,
    source_id: str = "a" * 32,
    sequence_start: int = 0,
    sequence_limit: int | None = None,
) -> str:
    limit = sequence_start + len(records) if sequence_limit is None else sequence_limit
    with InternalAudioStore(db) as store:
        store.create_source(source_id, "attempt", sequence_start, limit)
        store.prepare_leg(source_id, sequence_start, limit - sequence_start)
        store.record_read_begin(source_id, sequence_start, limit - sequence_start)
        if records:
            store.commit_drain(
                source_id,
                0,
                records,
                DrainCheckpoint(1, expected_sha256=hashlib.sha256(b"".join(records)).hexdigest()),
            )
    return source_id


def _clock_payload(*ranges: ClockRange) -> str:
    return json.dumps(ClockState(ranges).to_payload(), sort_keys=True, ensure_ascii=False, indent=1)


def _set_clock_path(path: Path, payload: str) -> None:
    connection = sqlite3.connect(path)
    try:
        connection.execute(
            "INSERT INTO state(kind,id,payload) VALUES ('clock','current',?) "
            "ON CONFLICT(kind,id) DO UPDATE SET payload=excluded.payload",
            (payload,),
        )
        connection.commit()
    finally:
        connection.close()


def _rows(db: StateDB) -> list[tuple[int, bytes, int | None, str | None]]:
    with closing(db.open_connection(read_only=True)) as connection:
        return cast(
            list[tuple[int, bytes, int | None, str | None]],
            connection.execute(
                "SELECT device_timestamp,payload,utc_timestamp,mapping_revision "
                "FROM capture_record ORDER BY source_id,ordinal"
            ).fetchall(),
        )


def _one(connection: sqlite3.Connection, query: str, parameters: tuple[object, ...]) -> tuple[object, ...]:
    row = cast(tuple[object, ...] | None, cast(object, connection.execute(query, parameters).fetchone()))
    assert row is not None
    return tuple(row)


def test_projection_tracks_exact_clock_revision_and_preserves_canonical_bytes(tmp_path: Path) -> None:
    db = StateDB(tmp_path / "state.sqlite3")
    records = tuple(_record(100 + index, index + 1) for index in range(3))
    _create_receiving_source(db, records, sequence_start=10)
    initial = _clock_payload(ClockRange(10, 13, 1000.4, 0.1, "confirmed", "initial"))
    _set_clock_path(db.path, initial)
    canonical_before = [(timestamp, payload) for timestamp, payload, _, _ in _rows(db)]
    generation_before = db.get_generation()

    initial_revision = hashlib.sha256(initial.encode()).hexdigest()
    assert clock_mapping_revision(db) == initial_revision
    assert refresh_utc_projection(db) == 3
    assert [row[2:] for row in _rows(db)] == [(1100 + index, initial_revision) for index in range(3)]
    assert [(timestamp, payload) for timestamp, payload, _, _ in _rows(db)] == canonical_before
    assert db.get_generation() == generation_before
    with closing(db.open_connection(read_only=True)) as connection:
        assert _one(
            connection,
            "SELECT count(*) FROM capture_record WHERE mapping_revision IS NOT ?",
            (initial_revision,),
        ) == (0,)

    changed = _clock_payload(ClockRange(10, 13, 2000.0, 0.1, "confirmed", "changed"))
    _set_clock_path(db.path, changed)
    changed_revision = hashlib.sha256(changed.encode()).hexdigest()
    with closing(db.open_connection(read_only=True)) as connection:
        assert _one(
            connection,
            "SELECT count(*) FROM capture_record WHERE mapping_revision IS NOT ?",
            (changed_revision,),
        ) == (3,)
    assert refresh_utc_projection(db) == 3
    assert [row[2:] for row in _rows(db)] == [(2100 + index, changed_revision) for index in range(3)]
    assert [(timestamp, payload) for timestamp, payload, _, _ in _rows(db)] == canonical_before


def test_projection_stamps_unknown_and_overflow_without_retry_churn(tmp_path: Path) -> None:
    db = StateDB(tmp_path / "state.sqlite3")
    _create_receiving_source(db, (_record(100, 1), _record(2**32 - 1, 2)))
    payload = _clock_payload(
        ClockRange(0, 1, None, None, "unknown", "unknown"),
        ClockRange(1, 2, 1.0, 0.1, "confirmed", "overflow"),
    )
    _set_clock_path(db.path, payload)
    revision = hashlib.sha256(payload.encode()).hexdigest()

    assert refresh_utc_projection(db) == 2
    assert [row[2:] for row in _rows(db)] == [(None, revision), (None, revision)]
    assert refresh_utc_projection(db) == 0


def test_projection_refresh_is_bounded_and_excludes_covered_sources(tmp_path: Path) -> None:
    db = StateDB(tmp_path / "state.sqlite3")
    records = tuple(_record(100 + index, index + 1) for index in range(5))
    source_id = _create_receiving_source(db, records)
    _create_receiving_source(db, (_record(200, 9),), source_id="b" * 32)
    payload = _clock_payload(ClockRange(0, 5, 1000.0, 0.1, "confirmed", "bounded"))
    _set_clock_path(db.path, payload)
    with closing(db.open_connection()) as connection:
        connection.execute("UPDATE capture_source SET lifecycle='covered' WHERE source_id=?", ("b" * 32,))
        connection.commit()

    assert [refresh_utc_projection(db, chunk_limit=2) for _ in range(4)] == [2, 2, 1, 0]
    with closing(db.open_connection(read_only=True)) as connection:
        assert _one(
            connection,
            "SELECT count(*) FROM capture_record WHERE source_id=? AND mapping_revision IS NOT NULL",
            (source_id,),
        ) == (5,)
        assert _one(
            connection,
            "SELECT utc_timestamp,mapping_revision FROM capture_record WHERE source_id=?",
            ("b" * 32,),
        ) == (None, None)


def test_projection_clock_change_before_write_is_a_noop(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    db = StateDB(tmp_path / "state.sqlite3")
    _create_receiving_source(db, (_record(100, 1),))
    old_payload = _clock_payload(ClockRange(0, 1, 1000.0, 0.1, "confirmed", "old"))
    new_payload = _clock_payload(ClockRange(0, 1, 2000.0, 0.1, "confirmed", "new"))
    _set_clock_path(db.path, old_payload)
    original_open = db.open_connection
    calls = 0

    def racing_open(*, read_only: bool = False) -> sqlite3.Connection:
        nonlocal calls
        calls += 1
        if calls == 2:
            _set_clock_path(db.path, new_payload)
        return original_open(read_only=read_only)

    monkeypatch.setattr(db, "open_connection", racing_open)
    assert refresh_utc_projection(db) == 0
    assert _rows(db)[0][2:] == (None, None)
    assert refresh_utc_projection(db) == 1
    assert _rows(db)[0][2:] == (2100, hashlib.sha256(new_payload.encode()).hexdigest())


def test_projection_and_concurrent_capture_serialize_without_losing_new_rows(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    db = StateDB(tmp_path / "state.sqlite3")
    first, second = _record(100, 1), _record(101, 2)
    source_id = _create_receiving_source(db, (first,), sequence_limit=2)
    _set_clock_path(db.path, _clock_payload(ClockRange(0, 2, 1000.0, 0.1, "confirmed", "concurrent")))
    projection_selected = threading.Event()
    release_projection = threading.Event()
    capture_started = threading.Event()
    original_open = db.open_connection

    def traced_open(*, read_only: bool = False) -> sqlite3.Connection:
        connection = original_open(read_only=read_only)
        if threading.current_thread().name == "projection" and not read_only:

            def trace(statement: str) -> None:
                if "FROM capture_record AS record" in statement:
                    projection_selected.set()
                    release_projection.wait(5)

            connection.set_trace_callback(trace)
        return connection

    monkeypatch.setattr(db, "open_connection", traced_open)
    projection_result: list[int] = []
    errors: list[BaseException] = []

    def project() -> None:
        try:
            projection_result.append(refresh_utc_projection(db))
        except Exception as error:  # noqa: BLE001 - forwarded to the test thread
            errors.append(error)

    def capture() -> None:
        try:
            with InternalAudioStore(db) as store:
                capture_started.set()
                store.commit_drain(
                    source_id,
                    1,
                    (second,),
                    DrainCheckpoint(1, expected_sha256=hashlib.sha256(first + second).hexdigest()),
                )
        except Exception as error:  # noqa: BLE001 - forwarded to the test thread
            errors.append(error)

    projection_thread = threading.Thread(target=project, name="projection")
    projection_thread.start()
    assert projection_selected.wait(5)
    capture_thread = threading.Thread(target=capture, name="capture")
    capture_thread.start()
    assert capture_started.wait(5)
    release_projection.set()
    projection_thread.join(5)
    capture_thread.join(5)

    assert not projection_thread.is_alive() and not capture_thread.is_alive()
    assert errors == []
    assert projection_result == [1]
    assert [row[2] for row in _rows(db)] == [1100, None]
    assert refresh_utc_projection(db) == 1
    assert [row[2] for row in _rows(db)] == [1100, 1101]


@pytest.mark.parametrize(
    ("predicate", "parameters"),
    [
        ("record.mapping_revision IS NULL", ()),
        ("record.mapping_revision < ?", ("f" * 64,)),
        ("record.mapping_revision > ?", ("0" * 64,)),
    ],
)
def test_projection_stale_seeks_use_mapping_revision_index(
    tmp_path: Path, predicate: str, parameters: tuple[str, ...]
) -> None:
    db = StateDB(tmp_path / "state.sqlite3")
    with closing(db.open_connection(read_only=True)) as connection:
        plan = cast(
            list[tuple[object, object, object, object]],
            cast(
                object,
                connection.execute(
                    "EXPLAIN QUERY PLAN SELECT record.source_id,record.ordinal,source.sequence_start,"
                    "record.device_timestamp,record.mapping_revision "
                    "FROM capture_record AS record JOIN capture_source AS source USING (source_id) "
                    f"WHERE {predicate} AND source.lifecycle NOT IN ('covered','retiring') "
                    "ORDER BY record.mapping_revision,record.source_id,record.ordinal LIMIT ?",
                    (*parameters, 128),
                ).fetchall(),
            ),
        )
        details = [cast(str, row[3]) for row in plan]
    assert any("capture_record_mapping_revision_idx" in detail for detail in details)
    assert all("SCAN record" not in detail for detail in details)
