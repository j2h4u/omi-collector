"""Focused SQL-backed attempt sequencing tests."""

from __future__ import annotations

import sqlite3
from contextlib import closing
from hashlib import sha256
from pathlib import Path
from typing import cast

import pytest

from omi_collector.capture.adapters.attempts import (
    RecordAcceptanceError,
    RecordRegressionError,
    SqliteStagedAttempt,
)
from omi_collector.capture.adapters.collector_state_db import StateDB
from omi_collector.capture.adapters.internal_audio_store import AUDIO_RECORD_SIZE, read_capture_source
from omi_collector.capture.adapters.staging_contract import AttemptStateError
from omi_collector.capture.adapters.staging_filesystem import StagingIO
from omi_collector.capture.adapters.staging_store import StagingStore
from omi_collector.capture.domain.ring_protocol import DoneNotification, ReadBeginNotification


def _record(marker: int) -> bytes:
    return marker.to_bytes(4, "big") + bytes((marker,)) * (AUDIO_RECORD_SIZE - 4)


def _store(tmp_path: Path) -> StagingStore:
    spool = tmp_path / "spool"
    return StagingStore(
        spool,
        tmp_path / "captures",
        state_db=StateDB(spool / "collector-state.sqlite3"),
        io=StagingIO(),
    )


def _attempt(store: StagingStore, count: int = 2) -> SqliteStagedAttempt:
    attempt = store.prepare_streaming_attempt(100, count)
    attempt.prepare_leg(100, count)
    attempt.record_read_begin(ReadBeginNotification(100, count))
    return attempt


def test_typed_attempt_commits_records_and_exposes_bounded_source_pages(tmp_path: Path) -> None:
    store = _store(tmp_path)
    attempt = _attempt(store)
    try:
        raw = _record(1) + _record(2)
        attempt.accept_chunk(100, raw)

        page = attempt.source_page(limit=2)

        assert attempt.path is None
        assert page.source.committed_count == 2
        assert page.source.committed_sha256 == sha256(raw).hexdigest()
        assert b"".join(record.raw_bytes for record in page.records) == raw
        assert not (store.attempts_root / attempt.attempt_id).exists()
    finally:
        attempt.close()


def test_typed_complete_seal_returns_durable_source_result_and_sealed_lifecycle(tmp_path: Path) -> None:
    store = _store(tmp_path)
    attempt = _attempt(store, 1)
    try:
        raw = _record(1)
        attempt.accept_chunk(100, raw)

        result = attempt.seal(DoneNotification(0, 101))

        assert result.source_id == attempt.attempt_id
        assert result.start_sequence == 100
        assert result.next_sequence == 101
        assert result.record_count == 1
        assert result.raw_sha256 == sha256(raw).hexdigest()
        assert attempt.source.lifecycle == "sealed_waiting"
    finally:
        attempt.close()


@pytest.mark.parametrize(
    ("damage", "reason"),
    (("payload", "digest"), ("missing", "count"), ("ordinal", "ordinal")),
)
def test_restart_inspection_and_seal_authenticate_canonical_records(tmp_path: Path, damage: str, reason: str) -> None:
    store = _store(tmp_path)
    attempt = _attempt(store, 1)
    try:
        attempt.accept_chunk(100, _record(1))
        with closing(store.state_db.open_connection()) as connection:
            if damage == "payload":
                connection.execute(
                    "UPDATE capture_record SET payload = ? WHERE source_id = ? AND ordinal = 0",
                    (bytes((9,)) * (AUDIO_RECORD_SIZE - 4), attempt.attempt_id),
                )
            elif damage == "missing":
                connection.execute(
                    "DELETE FROM capture_record WHERE source_id = ? AND ordinal = 0", (attempt.attempt_id,)
                )
            else:
                connection.execute(
                    "UPDATE capture_record SET ordinal = 1 WHERE source_id = ? AND ordinal = 0", (attempt.attempt_id,)
                )
            connection.commit()

        with pytest.raises(AttemptStateError, match=reason):
            store.open_attempt(attempt.attempt_id)
        with pytest.raises(AttemptStateError, match=reason):
            attempt.seal(DoneNotification(0, 101))

        source = read_capture_source(store.state_db, attempt.attempt_id)
        assert source is not None and source.lifecycle == "receiving"
    finally:
        attempt.close()


def test_typed_interruption_retains_partial_tail_and_restart_completes_exact_record(tmp_path: Path) -> None:
    store = _store(tmp_path)
    attempt = _attempt(store)
    first, second = _record(1), _record(2)
    try:
        attempt.accept_chunk(100, first, tail=second[:37])
        attempt.interrupt()
    finally:
        attempt.close()

    restarted = _store(tmp_path)
    with restarted.device_lock() as lease:
        resumed = restarted.resume_streaming_attempt(lease)
        assert resumed is not None
        try:
            resumed.prepare_leg(101, 1)
            resumed.record_read_begin(ReadBeginNotification(101, 1))
            resumed.accept_chunk(101, second)

            page = resumed.source_page(limit=2)
            assert tuple(record.raw_bytes for record in page.records) == (first, second)
            assert resumed.source.committed_count == 2
        finally:
            resumed.close()
    with closing(restarted.state_db.open_connection(read_only=True)) as connection:
        count = cast(sqlite3.Row, connection.execute("SELECT COUNT(*) FROM capture_tail").fetchone())
        assert count[0] == 0


def test_typed_process_death_recovers_receiving_source_and_partial_tail(tmp_path: Path) -> None:
    store = _store(tmp_path)
    attempt = _attempt(store)
    first, second = _record(7), _record(8)
    source_id = attempt.attempt_id
    attempt.accept_chunk(100, first, tail=second[:41])

    # Closing the process-owned connection without an interruption transition
    # models a hard exit after the durable drain commit.
    attempt.close()
    source = read_capture_source(store.state_db, source_id)
    assert source is not None and source.lifecycle == "receiving"

    restarted = _store(tmp_path)
    with restarted.device_lock() as lease:
        resumed = restarted.resume_streaming_attempt(lease)
        assert resumed is not None
        try:
            assert resumed.source.lifecycle == "interrupted"
            resumed.prepare_leg(101, 1)
            resumed.record_read_begin(ReadBeginNotification(101, 1))
            resumed.accept_chunk(101, second)
            assert tuple(record.raw_bytes for record in resumed.source_page(limit=2).records) == (first, second)
        finally:
            resumed.close()

    with closing(restarted.state_db.open_connection(read_only=True)) as connection:
        tail_count = cast(sqlite3.Row, connection.execute("SELECT COUNT(*) FROM capture_tail").fetchone())
    assert tail_count[0] == 0


def test_typed_restart_leg_recovery_from_interrupted_source_replays_in_order(tmp_path: Path) -> None:
    store = _store(tmp_path)
    attempt = _attempt(store)
    first, second = _record(3), _record(4)
    try:
        attempt.accept_chunk(100, first)
        attempt.interrupt()
    finally:
        attempt.close()

    restarted = _store(tmp_path)
    with restarted.device_lock() as lease:
        resumed = restarted.resume_streaming_attempt(lease)
        assert resumed is not None
        try:
            resumed.prepare_leg(101, 1)
            resumed.record_read_begin(ReadBeginNotification(101, 1))
            resumed.accept_chunk(101, second)
            assert resumed.durable_prefix.record_count == 2
        finally:
            resumed.close()


def test_typed_failed_drain_commit_cannot_advance_frontier_or_seal(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = _store(tmp_path)
    attempt = _attempt(store, 1)

    def fail_commit() -> None:
        raise sqlite3.OperationalError("simulated drain commit failure")

    monkeypatch.setattr(attempt._audio_store, "_commit", fail_commit)
    try:
        with pytest.raises(RecordAcceptanceError, match="ambiguous drain was rolled back"):
            attempt.accept_chunk(100, _record(5))
        assert attempt.durable_prefix.record_count == 0
        with pytest.raises(AttemptStateError, match="incomplete"):
            attempt.seal(DoneNotification(0, 101))
    finally:
        attempt.close()


def test_typed_attempt_rejects_data_before_read_begin_and_sequence_regression(tmp_path: Path) -> None:
    store = _store(tmp_path)
    attempt = store.prepare_streaming_attempt(100, 2)
    try:
        with pytest.raises(AttemptStateError, match="receiving"):
            attempt.accept_chunk(100, _record(1))
        attempt.prepare_leg(100, 2)
        attempt.record_read_begin(ReadBeginNotification(100, 2))
        with pytest.raises(RecordRegressionError, match="precedes"):
            attempt.accept_chunk(99, _record(1))
    finally:
        attempt.close()


def test_typed_replay_mismatch_retains_blocking_evidence(tmp_path: Path) -> None:
    store = _store(tmp_path)
    attempt = _attempt(store)
    try:
        attempt.accept_chunk(100, _record(1))

        with pytest.raises(RecordAcceptanceError, match="replay conflicts"):
            attempt.accept_chunk(100, _record(9))

        assert attempt.source.lifecycle == "blocked"
        with closing(store.state_db.open_connection(read_only=True)) as connection:
            count = cast(sqlite3.Row, connection.execute("SELECT COUNT(*) FROM capture_conflict").fetchone())
            assert count[0] == 1
    finally:
        attempt.close()
