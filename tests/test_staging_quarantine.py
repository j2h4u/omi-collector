"""Focused quarantine staging ownership tests."""

from __future__ import annotations

import sqlite3
import threading
from contextlib import closing
from hashlib import sha256
from pathlib import Path
from typing import cast

import pytest

from omi_collector.capture.adapters import collector_state_db
from omi_collector.capture.adapters import quarantine as quarantine_module
from omi_collector.capture.adapters.attempts import RecordAcceptanceError
from omi_collector.capture.adapters.clock_segments import ClockSegmentMap
from omi_collector.capture.adapters.collector_state_db import StateDBError, StateKind
from omi_collector.capture.adapters.internal_audio_store import InternalAudioStore, InternalAudioStoreError
from omi_collector.capture.adapters.opportunistic_runtime import OpportunisticRuntime
from omi_collector.capture.adapters.quarantine import QuarantineCustodyHoldError, QuarantinePublishError
from omi_collector.capture.adapters.ready_bundles import (
    ReadyOutcomeState,
    SqlitePublicationSource,
    finalize_typed_sources,
)
from omi_collector.capture.adapters.staging_contract import StagingError
from omi_collector.capture.application.storage_fault import (
    FatalInternalAudioStorageError,
    TransientInternalAudioStorageError,
)
from omi_collector.capture.domain.quarantine_machine import QuarantineState
from omi_collector.capture.domain.ring_protocol import RECORD_SIZE, DoneNotification, ReadBeginNotification
from omi_collector.config import CollectorConfig, ReadyConfig, StagingRetentionConfig, WriterConfig
from staging_test_factory import StagingStore, attempt_source


def _record(marker: int) -> bytes:
    return marker.to_bytes(4, "big") + bytes((marker,)) * (RECORD_SIZE - 4)


def _opus_record(timestamp: int, marker: int) -> bytes:
    return timestamp.to_bytes(4, "big") + bytes((2, 8, marker)) + bytes(RECORD_SIZE - 7)


def _capture_root(tmp_path: Path) -> Path:
    return tmp_path.parent / f"{tmp_path.name}-captures"


def _store(tmp_path: Path, **options: object) -> StagingStore:
    return StagingStore(tmp_path, _capture_root(tmp_path), **options)


def _attempt(store: StagingStore, start: int = 100, count: int = 2):
    attempt = store.prepare_streaming_attempt(start, count)
    attempt.prepare_leg(start, count)
    attempt.record_read_begin(ReadBeginNotification(start, count))
    return attempt


def _source_row(store: StagingStore, source_id: str) -> tuple[object, ...] | None:
    with closing(store.state_db.open_connection(read_only=True)) as connection:
        row = cast(
            sqlite3.Row | None,
            connection.execute(
                "SELECT source_type, lifecycle, block_reason, quarantine_disposition, "
                "quarantine_opaque, terminal_reason, retention_deadline, committed_count, committed_sha256 "
                "FROM capture_source WHERE source_id = ?",
                (source_id,),
            ).fetchone(),
        )
    return tuple(row) if row is not None else None


def test_typed_quarantine_writer_keeps_committed_rows_readable_until_commit(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = _store(tmp_path)
    store.state_db.put(StateKind.CLOCK, "current", {"value": "old"})
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
    reader = sqlite3.connect(store.state_db.path, isolation_level=None, timeout=0)
    reader.execute("BEGIN")
    reader.execute("SELECT count(*) FROM state").fetchone()
    errors: list[Exception] = []

    def write() -> None:
        nonlocal armed
        try:
            with quarantine_module._typed_transaction(store._filesystem) as connection:
                connection.execute(
                    "UPDATE state SET payload = '{\"value\":\"new\"}' WHERE kind='clock' AND id='current'"
                )
                armed = True
                dml_done.set()
        except (sqlite3.DatabaseError, StateDBError, TimeoutError) as error:
            errors.append(error)

    thread = threading.Thread(target=write)
    thread.start()
    try:
        assert dml_done.wait(5)
        assert commit_called.wait(5)
        assert reader.execute("SELECT payload FROM state WHERE kind='clock' AND id='current'").fetchone() == (
            '{"value":"old"}',
        )
        reader.rollback()
        thread.join(5)
    finally:
        reader.rollback()
        reader.close()
        if thread.is_alive():
            thread.join(5)
    assert not thread.is_alive()
    assert errors == []
    assert store.state_db.get(StateKind.CLOCK, "current") == {"value": "new"}


def test_typed_quarantine_changes_source_state_without_creating_private_entry(tmp_path: Path) -> None:
    store = _store(tmp_path)
    attempt = store.prepare_streaming_attempt(100, 1)
    attempt.prepare_leg(100, 1)
    attempt.record_read_begin(ReadBeginNotification(100, 1))
    attempt.accept_chunk(100, _record(1))

    logical = store.quarantine_attempt_source(attempt.attempt_id)

    assert logical == attempt.attempt_id
    assert not (tmp_path / "quarantine" / attempt.attempt_id).exists()
    row = _source_row(store, attempt.attempt_id)
    assert row is not None and row[:3] == (
        "quarantine",
        "blocked",
        "attempt source preserved",
    )
    attempt.close()


def test_quarantine_pending_only_blocks_active_attempt_sources(tmp_path: Path) -> None:
    store = _store(tmp_path)
    active = _attempt(store)
    active.accept_chunk(100, _record(1))

    sealed = _attempt(store, 200, 1)
    sealed.accept_chunk(200, _record(2))
    sealed.seal(DoneNotification(0, 201))
    sealed.close()
    covered = _attempt(store, 300, 1)
    retiring = _attempt(store, 400, 1)
    with closing(store.state_db.open_connection()) as connection:
        connection.execute("UPDATE capture_source SET lifecycle = 'covered' WHERE source_id = ?", (covered.attempt_id,))
        connection.execute(
            "UPDATE capture_source SET lifecycle = 'retiring' WHERE source_id = ?", (retiring.attempt_id,)
        )
        connection.commit()

    assert store.quarantine_pending("preserve active source") == (active.attempt_id,)
    active_row = _source_row(store, active.attempt_id)
    sealed_row = _source_row(store, sealed.attempt_id)
    covered_row = _source_row(store, covered.attempt_id)
    retiring_row = _source_row(store, retiring.attempt_id)
    assert active_row is not None and active_row[:3] == ("quarantine", "blocked", "preserve active source")
    assert sealed_row is not None and sealed_row[:2] == ("attempt", "sealed_waiting")
    assert covered_row is not None and covered_row[:2] == ("attempt", "covered")
    assert retiring_row is not None and retiring_row[:2] == ("attempt", "retiring")
    for attempt in active, covered, retiring:
        attempt.close()


def test_quarantine_preserves_exact_records_tail_and_conflict_evidence(tmp_path: Path) -> None:
    store = _store(tmp_path)
    attempt = _attempt(store)
    try:
        accepted = _record(1)
        tail = b"partial-audio-tail"
        attempt.accept_chunk(100, accepted, tail)
        with pytest.raises(RecordAcceptanceError, match="conflict"):
            attempt.accept_chunk(100, _record(9))
        source_id = attempt.attempt_id

        store.quarantine_attempt_source(source_id)

        with closing(store.state_db.open_connection(read_only=True)) as connection:
            record = cast(
                tuple[object, object] | None,
                connection.execute(
                    "SELECT device_timestamp, payload FROM capture_record WHERE source_id = ? AND ordinal = 0",
                    (source_id,),
                ).fetchone(),
            )
            saved_tail = cast(
                tuple[object] | None,
                connection.execute("SELECT bytes FROM capture_tail WHERE source_id = ?", (source_id,)).fetchone(),
            )
            conflicts = connection.execute(
                "SELECT bytes, reason FROM capture_conflict WHERE source_id = ?", (source_id,)
            ).fetchall()
        assert record is not None and saved_tail is not None
        assert tuple(record) == (1, accepted[4:])
        assert saved_tail[0] == tail
        assert [(row[0], row[1]) for row in cast(list[tuple[object, object]], conflicts)] == [
            (_record(9), "replay_mismatch")
        ]
        source = attempt_source(store, source_id)
        assert source is not None and source.committed_sha256 == sha256(accepted).hexdigest()
    finally:
        attempt.close()


def test_terminal_reason_is_distinct_from_original_quarantine_reason(tmp_path: Path) -> None:
    store = _store(tmp_path)
    attempt = _attempt(store)
    attempt.accept_chunk(100, _record(1))
    source_id = store.quarantine_attempt_source(attempt.attempt_id)

    store.mark_quarantine_unprocessable(source_id, "terminal proof failed")

    row = _source_row(store, source_id)
    assert row is not None and row[2] == "attempt source preserved"
    assert row is not None and row[3] == "unprocessable"
    assert row is not None and row[5] == "terminal proof failed"
    assert row is not None and row[6] is not None
    attempt.close()


def test_ready_custody_conflict_cannot_be_salvaged_or_terminally_deleted(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    now = 1_000_000_000
    monkeypatch.setattr("omi_collector.capture.adapters.quarantine._wall_clock_ns", lambda: now)
    store = _store(
        tmp_path,
        config=CollectorConfig(staging_retention=StagingRetentionConfig(terminal_retention_seconds=1.0)),
    )
    attempt = _attempt(store, count=1)
    try:
        raw = _record(1)
        attempt.accept_chunk(100, raw)
        source_id = store.quarantine_attempt_source(attempt.attempt_id)
        with closing(store.state_db.open_connection()) as connection:
            connection.execute(
                "UPDATE capture_source SET block_reason='ready_overlap_conflict', "
                "terminal_time=NULL, retention_deadline=NULL, "
                "certified_prefix_count=NULL, coverage_digest=NULL WHERE source_id=?",
                (source_id,),
            )
            connection.commit()
        before = _source_row(store, source_id)
        generation = store.state_db.get_generation()

        assert source_id not in store.quarantined_attempts()
        with pytest.raises(QuarantineCustodyHoldError, match="unresolved custody") as admission_error:
            store.admit_quarantined_prefix(source_id)
        assert OpportunisticRuntime().classify_quarantine_error(admission_error.value) == "deferred"
        with pytest.raises(QuarantineCustodyHoldError, match="unresolved custody"):
            store.mark_quarantine_unprocessable(source_id, "could not authenticate")
        now += 2_000_000_000
        assert store.sweep_terminal_quarantine().progressed is False
        assert _source_row(store, source_id) == before
        assert store.state_db.get_generation() == generation
        with closing(store.state_db.open_connection(read_only=True)) as connection:
            kept = cast(
                tuple[object, object] | None,
                connection.execute(
                    "SELECT device_timestamp, payload FROM capture_record WHERE source_id=? AND ordinal=0", (source_id,)
                ).fetchone(),
            )
        assert kept is not None and tuple(kept) == (1, raw[4:])
    finally:
        attempt.close()


def test_opaque_quarantine_sources_are_not_salvaged_and_expire_after_terminal_retention(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    now = 1_000_000_000
    monkeypatch.setattr("omi_collector.capture.adapters.quarantine._wall_clock_ns", lambda: now)
    store = _store(
        tmp_path,
        config=CollectorConfig(staging_retention=StagingRetentionConfig(terminal_retention_seconds=1.0)),
    )
    attempt = _attempt(store)
    try:
        source_id = store.quarantine_attempt_source(attempt.attempt_id)
        with closing(store.state_db.open_connection()) as connection:
            connection.execute("UPDATE capture_source SET quarantine_opaque = 1 WHERE source_id = ?", (source_id,))
            connection.commit()

        assert source_id not in store.quarantined_attempts()
        with pytest.raises(QuarantinePublishError, match="cannot be admitted"):
            store.admit_quarantined_prefix(source_id)
        store.mark_quarantine_unprocessable(source_id, "opaque evidence cannot be authenticated")
        now += 999_999_999
        assert store.sweep_terminal_quarantine().removed_source_ids == ()
        now += 1
        assert store.sweep_terminal_quarantine().removed_source_ids == (source_id,)
        assert attempt_source(store, source_id) is None
    finally:
        attempt.close()


def test_salvaged_prefix_remains_in_sql_until_below_threshold_packaging(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    now = 1_000_000_000
    monkeypatch.setattr("omi_collector.capture.adapters.quarantine._wall_clock_ns", lambda: now)
    store = _store(
        tmp_path,
        config=CollectorConfig(staging_retention=StagingRetentionConfig(terminal_retention_seconds=1.0)),
    )
    attempt = _attempt(store, count=2)
    try:
        raw = _record(1)
        residual_tail = b"residual-tail"
        attempt.accept_chunk(100, raw, residual_tail)
        source_id = store.quarantine_attempt_source(attempt.attempt_id)
        evidence_id = "e" * 64
        with closing(store.state_db.open_connection()) as connection:
            connection.execute(
                "INSERT INTO capture_evidence("
                "evidence_id, source_id, entry_kind, original_relative_path, device, inode, mode, size, "
                "sha256, disposition, reason) VALUES (?, ?, 'regular', ?, 1, 2, 384, 1, ?, 'unclassified', ?)",
                (evidence_id, source_id, "attempt/residual.bin", sha256(b"x").hexdigest(), "retained residual"),
            )
            connection.commit()

        admitted_at = now
        receipt = store.admit_quarantined_prefix(source_id)
        now += 1
        assert store.admit_quarantined_prefix(source_id) == receipt
        assert (receipt.source_id, receipt.start_sequence, receipt.next_sequence, receipt.record_count) == (
            source_id,
            100,
            101,
            1,
        )
        with closing(store.state_db.open_connection(read_only=True)) as connection:
            source = cast(
                tuple[object, ...] | None,
                connection.execute(
                    "SELECT lifecycle, quarantine_disposition, certified_prefix_count, closure_frontier, "
                    "coverage_digest, retirement_cursor, terminal_time, retention_deadline "
                    "FROM capture_source WHERE source_id = ?",
                    (source_id,),
                ).fetchone(),
            )
            record = cast(
                tuple[object, object] | None,
                connection.execute(
                    "SELECT device_timestamp, payload FROM capture_record WHERE source_id = ? AND ordinal = 0",
                    (source_id,),
                ).fetchone(),
            )
            tail = cast(
                tuple[object] | None,
                connection.execute("SELECT bytes FROM capture_tail WHERE source_id = ?", (source_id,)).fetchone(),
            )
            evidence = cast(
                tuple[object, object] | None,
                connection.execute(
                    "SELECT disposition, retention_deadline FROM capture_evidence WHERE evidence_id = ?",
                    (evidence_id,),
                ).fetchone(),
            )
        publication_rows = store.state_db.list(StateKind.READY_PUBLICATION)
        assert source is not None and tuple(source) == (
            "blocked",
            "published_prefix",
            1,
            None,
            sha256(raw).hexdigest(),
            None,
            str(admitted_at),
            str(admitted_at + 1_000_000_000),
        )
        assert record is not None and tuple(record) == (1, raw[4:])
        assert tail is not None and tuple(tail) == (residual_tail,)
        assert evidence is not None and tuple(evidence) == ("unclassified", None)
        assert publication_rows == ()

        now += 1_000_000_000
        assert store.sweep_terminal_quarantine().removed_source_ids == ()
        assert attempt_source(store, source_id) is not None
        with closing(store.state_db.open_connection(read_only=True)) as connection:
            tail = cast(
                tuple[object] | None,
                connection.execute("SELECT bytes FROM capture_tail WHERE source_id = ?", (source_id,)).fetchone(),
            )
            evidence = cast(
                sqlite3.Row | None,
                connection.execute("SELECT 1 FROM capture_evidence WHERE evidence_id = ?", (evidence_id,)).fetchone(),
            )
        assert tail is None
        assert evidence is None
        for _ in range(3):
            assert store.sweep_terminal_quarantine().removed_source_ids == ()
        assert attempt_source(store, source_id) is not None
        with closing(store.state_db.open_connection(read_only=True)) as connection:
            assert (
                connection.execute(
                    "SELECT 1 FROM capture_record WHERE source_id = ? AND ordinal = 0", (source_id,)
                ).fetchone()
                is not None
            )
    finally:
        attempt.close()


def test_replay_conflict_requires_quarantine_admission_before_publication(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    now = 1_000_000_000
    monkeypatch.setattr("omi_collector.capture.adapters.quarantine._wall_clock_ns", lambda: now)
    store = _store(
        tmp_path,
        config=CollectorConfig(staging_retention=StagingRetentionConfig(terminal_retention_seconds=1.0)),
    )
    attempt = _attempt(store)
    source_id = attempt.attempt_id
    raw = _opus_record(1, 1)
    attempt.accept_chunk(100, raw)
    with pytest.raises(RecordAcceptanceError, match="conflict"):
        attempt.accept_chunk(100, _record(9))
    attempt.close()
    ready = tmp_path / "ready"
    ready.mkdir(mode=0o2750)
    ready.chmod(0o2750)

    with InternalAudioStore(store.state_db) as audio_store:
        reader = SqlitePublicationSource(audio_store, source_id)
        waiting = finalize_typed_sources(
            audio_store,
            (reader,),
            ready,
            config=ReadyConfig(target_audio_seconds=0.02),
            clock_segments=ClockSegmentMap(()),
            frontier=101,
            drained=True,
        )
    assert waiting.state is ReadyOutcomeState.WAITING
    assert not tuple(ready.iterdir())
    before = _source_row(store, source_id)
    assert before is not None and before[3] == "unclassified"
    assert before[6] is None
    now += 1_000_000_000
    assert store.sweep_terminal_quarantine().removed_source_ids == ()
    retained = _source_row(store, source_id)
    assert retained is not None and retained[3] == "unclassified"
    with closing(store.state_db.open_connection(read_only=True)) as connection:
        assert (
            connection.execute("SELECT 1 FROM capture_conflict WHERE source_id = ?", (source_id,)).fetchone()
            is not None
        )

    admission = store.admit_quarantined_prefix(source_id)
    assert (admission.record_count, admission.raw_sha256) == (1, sha256(raw).hexdigest())
    with InternalAudioStore(store.state_db) as audio_store:
        published = finalize_typed_sources(
            audio_store,
            (SqlitePublicationSource(audio_store, source_id),),
            ready,
            config=ReadyConfig(target_audio_seconds=0.02),
            clock_segments=ClockSegmentMap(()),
            frontier=101,
            drained=True,
        )
    assert published.state is ReadyOutcomeState.PUBLISHED

    admitted = _source_row(store, source_id)
    assert admitted is not None and isinstance(admitted[6], str)
    assert admitted[6] == str(now + 1_000_000_000)
    assert admitted[3] == "published_prefix"


def test_standalone_evidence_owns_its_terminal_retention(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    now = 1_000_000_000
    monkeypatch.setattr("omi_collector.capture.adapters.quarantine._wall_clock_ns", lambda: now)
    store = _store(
        tmp_path,
        config=CollectorConfig(staging_retention=StagingRetentionConfig(terminal_retention_seconds=1.0)),
    )
    evidence_id = "f" * 64
    with closing(store.state_db.open_connection()) as connection:
        connection.execute(
            "INSERT INTO capture_evidence("
            "evidence_id, source_id, entry_kind, original_relative_path, device, inode, mode, size, "
            "sha256, disposition, reason) VALUES (?, NULL, 'regular', ?, 1, 2, 384, 1, ?, 'unclassified', ?)",
            (evidence_id, "quarantine/orphan.bin", sha256(b"x").hexdigest(), "orphaned evidence"),
        )
        connection.commit()

    assert store.sweep_terminal_quarantine().removed_source_ids == ()
    with closing(store.state_db.open_connection(read_only=True)) as connection:
        evidence = cast(
            tuple[object, object, object, object] | None,
            connection.execute(
                "SELECT disposition, terminal_reason, terminal_time, retention_deadline "
                "FROM capture_evidence WHERE evidence_id = ?",
                (evidence_id,),
            ).fetchone(),
        )
    assert evidence is not None and tuple(evidence) == (
        "unprocessable",
        "standalone typed quarantine evidence",
        str(now),
        str(now + 1_000_000_000),
    )

    now += 1_000_000_000
    assert store.sweep_terminal_quarantine().removed_source_ids == ()
    with closing(store.state_db.open_connection(read_only=True)) as connection:
        assert (
            connection.execute("SELECT 1 FROM capture_evidence WHERE evidence_id = ?", (evidence_id,)).fetchone()
            is None
        )


def test_corrupt_source_digest_becomes_indefinite_custody_hold(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    now = 1_000_000_000
    monkeypatch.setattr("omi_collector.capture.adapters.quarantine._wall_clock_ns", lambda: now)
    store = _store(tmp_path)
    attempt = _attempt(store, count=1)
    raw = _record(1)
    try:
        attempt.accept_chunk(100, raw)
        source_id = store.quarantine_attempt_source(attempt.attempt_id)
        with closing(store.state_db.open_connection()) as connection:
            connection.execute(
                "UPDATE capture_source SET committed_sha256 = ? WHERE source_id = ?", ("0" * 64, source_id)
            )
            connection.commit()

        with pytest.raises(QuarantineCustodyHoldError):
            store.admit_quarantined_prefix(source_id)

        source = attempt_source(store, source_id)
        assert source is not None and source.committed_count == 1
        with closing(store.state_db.open_connection(read_only=True)) as connection:
            certified = cast(
                tuple[object, ...] | None,
                connection.execute(
                    "SELECT source_type, lifecycle, block_reason, quarantine_disposition, "
                    "certified_prefix_count, coverage_digest, committed_sha256, terminal_time, retention_deadline "
                    "FROM capture_source WHERE source_id = ?",
                    (source_id,),
                ).fetchone(),
            )
            record = cast(
                tuple[object] | None,
                connection.execute(
                    "SELECT payload FROM capture_record WHERE source_id = ? AND ordinal = 0", (source_id,)
                ).fetchone(),
            )
        assert certified is not None and tuple(certified) == (
            "quarantine",
            "blocked",
            "source_authentication_mismatch",
            "unclassified",
            None,
            None,
            "0" * 64,
            None,
            None,
        )
        assert record is not None and record[0] == raw[4:]
        assert source_id not in store.quarantined_attempts()
        with pytest.raises(QuarantineCustodyHoldError):
            store.mark_quarantine_unprocessable(source_id, "bad source")
        monkeypatch.setattr(
            "omi_collector.capture.adapters.quarantine.authenticate_capture_source",
            lambda *_args, **_kwargs: pytest.fail("held source was rehashed"),
        )
        with pytest.raises(QuarantineCustodyHoldError):
            store.admit_quarantined_prefix(source_id)
        now += 73 * 3600 * 1_000_000_000
        assert not store.sweep_terminal_quarantine().progressed
        assert _source_row(store, source_id) is not None
    finally:
        attempt.close()


@pytest.mark.parametrize("durable_state", ["committed", "rolled_back", "partial"])
def test_authentication_hold_reconciles_complete_durable_row(tmp_path: Path, durable_state: str) -> None:
    store = _store(tmp_path)
    attempt = _attempt(store, count=1)
    raw = _record(1)
    try:
        attempt.accept_chunk(100, raw)
        source_id = store.quarantine_attempt_source(attempt.attempt_id)
        with closing(store.state_db.open_connection()) as connection:
            connection.execute(
                "UPDATE capture_source SET quarantine_disposition='published_prefix', "
                "certified_prefix_count=1, coverage_digest=?, terminal_time='1000', retention_deadline='2000' "
                "WHERE source_id=?",
                (sha256(raw).hexdigest(), source_id),
            )
            connection.commit()
            current = quarantine_module._source_row(connection, source_id)
            assert current is not None
            columns, before_row = current
            assert type(before_row) is tuple
            before = tuple(before_row)
            changes: dict[str, object] = {
                "source_type": "quarantine",
                "lifecycle": "blocked",
                "block_reason": "source_authentication_mismatch",
                "quarantine_disposition": "unclassified",
                "quarantine_opaque": 0,
                "terminal_reason": None,
                "terminal_time": None,
                "retention_deadline": None,
                "certified_prefix_count": None,
                "coverage_digest": None,
                "retirement_cursor": None,
            }
            proposed = tuple(changes.get(column, value) for column, value in zip(columns, before, strict=True))
            if durable_state == "committed":
                connection.execute(
                    "UPDATE capture_source SET block_reason='source_authentication_mismatch', "
                    "quarantine_disposition='unclassified', terminal_time=NULL, retention_deadline=NULL, "
                    "certified_prefix_count=NULL, coverage_digest=NULL, retirement_cursor=NULL "
                    "WHERE source_id=?",
                    (source_id,),
                )
            elif durable_state == "partial":
                connection.execute(
                    "UPDATE capture_source SET block_reason='source_authentication_mismatch' WHERE source_id=?",
                    (source_id,),
                )
            connection.commit()
            actual = quarantine_module._source_row(connection, source_id)
            assert actual is not None
            durable = tuple(actual[1])

        failure = sqlite3.OperationalError("injected ambiguous commit")
        if durable_state == "committed":
            quarantine_module._reconcile_source_authentication_hold(
                store._filesystem, source_id, before, proposed, failure
            )
            assert durable == proposed
            assert store.quarantined_attempts() == ()
        else:
            with pytest.raises(
                StagingError if durable_state == "rolled_back" else FatalInternalAudioStorageError,
                match="rolled back" if durable_state == "rolled_back" else "ambiguous third state",
            ) as raised:
                quarantine_module._reconcile_source_authentication_hold(
                    store._filesystem, source_id, before, proposed, failure
                )
            assert raised.value.__cause__ is failure
            assert durable != proposed
        with closing(store.state_db.open_connection(read_only=True)) as connection:
            after = quarantine_module._source_row(connection, source_id)
            retained = cast(
                sqlite3.Row | None,
                connection.execute(
                    "SELECT payload FROM capture_record WHERE source_id=? AND ordinal=0", (source_id,)
                ).fetchone(),
            )
        assert after is not None and tuple(after[1]) == durable
        assert retained is not None and retained[0] == raw[4:]
        if durable_state == "rolled_back":
            assert durable == before
        elif durable_state == "partial":
            assert durable[columns.index("terminal_time")] == "1000"
            assert durable[columns.index("certified_prefix_count")] == 1
        else:
            assert durable[columns.index("terminal_time")] is None
            assert durable[columns.index("certified_prefix_count")] is None
    finally:
        attempt.close()


def test_authentication_hold_unreadable_commit_outcome_stops_capture(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = _store(tmp_path)
    attempt = _attempt(store, count=1)
    try:
        source_id = store.quarantine_attempt_source(attempt.attempt_id)
        with closing(store.state_db.open_connection(read_only=True)) as connection:
            row = quarantine_module._source_row(connection, source_id)
        assert row is not None
        before = row[1]
        proposed = ("changed", *before[1:])
        busy = TransientInternalAudioStorageError("database busy", sqlite_errorcode=sqlite3.SQLITE_BUSY)

        def unreadable(*_args: object) -> None:
            raise busy

        with monkeypatch.context() as patch:
            patch.setattr(quarantine_module, "_source_row", unreadable)
            with pytest.raises(FatalInternalAudioStorageError, match="unreadable") as raised:
                quarantine_module._reconcile_source_authentication_hold(
                    store._filesystem, source_id, before, proposed, busy
                )
        assert raised.value.__cause__ is busy
    finally:
        attempt.close()


def test_authentication_read_failure_defers_without_terminal_classification(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = _store(tmp_path)
    attempt = _attempt(store, count=1)
    try:
        attempt.accept_chunk(100, _record(1))
        source_id = store.quarantine_attempt_source(attempt.attempt_id)
        before = _source_row(store, source_id)
        generation = store.state_db.get_generation()

        def fail_read(*_args: object, **_kwargs: object) -> None:
            raise InternalAudioStoreError("transient canonical read failure")

        monkeypatch.setattr("omi_collector.capture.adapters.quarantine.authenticate_capture_source", fail_read)
        with pytest.raises(StagingError, match="authentication is unavailable") as error:
            store.admit_quarantined_prefix(source_id)
        assert OpportunisticRuntime().classify_quarantine_error(error.value) == "deferred"
        assert _source_row(store, source_id) == before
        assert store.state_db.get_generation() == generation
    finally:
        attempt.close()


def test_zero_checkpoint_with_real_record_enters_hold_at_quarantine_open(tmp_path: Path) -> None:
    store = _store(tmp_path)
    attempt = _attempt(store, count=1)
    try:
        attempt.accept_chunk(100, _record(1))
        source_id = attempt.attempt_id
        with closing(store.state_db.open_connection()) as connection:
            connection.execute(
                "UPDATE capture_source SET committed_count=0, committed_sha256=? WHERE source_id=?",
                (sha256(b"").hexdigest(), source_id),
            )
            connection.commit()
        store.quarantine_attempt_source(source_id)
        row = _source_row(store, source_id)
        assert row is not None and row[:4] == (
            "quarantine",
            "blocked",
            "source_authentication_mismatch",
            "unclassified",
        )
        assert row[6] is None
        with closing(store.state_db.open_connection(read_only=True)) as connection:
            assert (
                connection.execute("SELECT COUNT(*) FROM capture_record WHERE source_id=?", (source_id,)).fetchone()[0]
                == 1
            )
    finally:
        attempt.close()


def test_terminalized_zero_tail_is_expired_while_sealed_source_publishes_after_restart(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    now = 1_000_000_000
    monkeypatch.setattr("omi_collector.capture.adapters.quarantine._wall_clock_ns", lambda: now)
    config = CollectorConfig(
        ready=ReadyConfig(target_audio_seconds=0.02),
        staging_retention=StagingRetentionConfig(terminal_retention_seconds=1.0),
    )
    bootstrap = StagingStore(tmp_path / "spool", tmp_path / "capture", config=config)
    ready = tmp_path / "ready"
    ready.mkdir(mode=0o2750)
    ready.chmod(0o2750)
    store = StagingStore.from_paths(bootstrap.paths, state_db=bootstrap.state_db, publication_root=ready, config=config)
    zero = _attempt(store, start=100, count=1)
    sealed = _attempt(store, start=101, count=1)
    try:
        zero.accept_chunk(100, b"", b"partial-tail")
        zero.interrupt()
        zero_id = zero.attempt_id
        zero.close(durable=True)
        store.terminalize_prefix_attempt(zero_id)

        raw = _opus_record(2, 2)
        sealed.accept_chunk(101, raw)
        sealed.seal(DoneNotification(0, 102))
        sealed_id = sealed.attempt_id
        sealed.close(durable=True)
        store.terminalize_prefix_attempt(sealed_id)
        store.append_ready_closure(102, "drained")

        zero_row = _source_row(store, zero_id)
        assert zero_row is not None and zero_row[:4] == (
            "quarantine",
            "blocked",
            "terminal prefix residual",
            "unprocessable",
        )
        assert zero_row[6] is not None
        now += 1_000_000_000
        assert store.sweep_terminal_quarantine().removed_source_ids == (zero_id,)
        published = store.recover_and_publish()
        assert published.state is ReadyOutcomeState.PUBLISHED
        assert published.published[0].path.joinpath("records.bin").read_bytes() == raw

        restarted = StagingStore.from_paths(
            bootstrap.paths, state_db=bootstrap.state_db, publication_root=ready, config=config
        )
        assert restarted.sweep_terminal_quarantine().removed_source_ids == ()
        assert attempt_source(restarted, zero_id) is None
        assert attempt_source(restarted, sealed_id) is not None
    finally:
        zero.close()
        sealed.close()


def test_unclassified_evidence_cannot_expire_or_starve_later_terminal_source(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    now = 1_000_000_000
    monkeypatch.setattr("omi_collector.capture.adapters.quarantine._wall_clock_ns", lambda: now)
    store = _store(
        tmp_path,
        config=CollectorConfig(staging_retention=StagingRetentionConfig(terminal_retention_seconds=1.0)),
    )
    unresolved = _attempt(store, start=100, count=2)
    terminal = _attempt(store, start=200, count=1)
    try:
        unresolved.accept_chunk(100, _record(1), b"uncertain tail")
        unresolved_id = store.quarantine_attempt_source(unresolved.attempt_id)
        terminal.accept_chunk(200, _record(2))
        terminal_id = store.quarantine_attempt_source(terminal.attempt_id)
        store.mark_quarantine_unprocessable(terminal_id, "explicit terminal disposition")
        with closing(store.state_db.open_connection()) as connection:
            connection.execute(
                "UPDATE capture_source SET terminal_time='1', retention_deadline='1' WHERE source_id=?",
                (unresolved_id,),
            )
            connection.commit()
        before = _source_row(store, unresolved_id)
        now += 1_000_000_000
        assert store.sweep_terminal_quarantine().removed_source_ids == (terminal_id,)
        assert _source_row(store, unresolved_id) == before
        with closing(store.state_db.open_connection(read_only=True)) as connection:
            assert (
                connection.execute(
                    "SELECT COUNT(*) FROM capture_record WHERE source_id=?", (unresolved_id,)
                ).fetchone()[0]
                == 1
            )
            assert (
                connection.execute("SELECT bytes FROM capture_tail WHERE source_id=?", (unresolved_id,)).fetchone()[0]
                == b"uncertain tail"
            )
    finally:
        unresolved.close()
        terminal.close()


def test_terminal_quarantine_obeys_retention_deadline_and_keeps_unterminalized_source(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    now = 1_000_000_000
    monkeypatch.setattr("omi_collector.capture.adapters.quarantine._wall_clock_ns", lambda: now)
    store = _store(
        tmp_path,
        config=CollectorConfig(staging_retention=StagingRetentionConfig(terminal_retention_seconds=72.0)),
    )
    aged = _attempt(store, 100, 2)
    live = _attempt(store, 200, 1)
    try:
        residual = _record(3)
        aged.accept_chunk(100, residual, b"retained tail")
        aged_id = store.quarantine_attempt_source(aged.attempt_id)
        store.mark_quarantine_unprocessable(aged_id, "cannot authenticate")
        live.accept_chunk(200, _record(4))
        live_id = store.quarantine_attempt_source(live.attempt_id)
        now += 72_000_000_000 - 1

        assert store.sweep_terminal_quarantine().removed_source_ids == ()
        assert attempt_source(store, aged_id) is not None
        with closing(store.state_db.open_connection(read_only=True)) as connection:
            row = cast(
                tuple[object] | None,
                connection.execute(
                    "SELECT payload FROM capture_record WHERE source_id = ? AND ordinal = 0", (aged_id,)
                ).fetchone(),
            )
        assert row is not None and row[0] == residual[4:]
        now += 1

        assert store.sweep_terminal_quarantine().removed_source_ids == (aged_id,)
        assert attempt_source(store, aged_id) is None
        assert attempt_source(store, live_id) is not None
    finally:
        aged.close()
        live.close()


def _quarantine_child_count(store: StagingStore, source_id: str) -> int:
    with closing(store.state_db.open_connection(read_only=True)) as connection:
        return sum(
            cast(
                int, connection.execute(f"SELECT COUNT(*) FROM {table} WHERE source_id = ?", (source_id,)).fetchone()[0]
            )
            for table in ("capture_record", "capture_tail", "capture_conflict", "capture_evidence")
        ) + cast(
            int,
            connection.execute(
                "SELECT COUNT(*) FROM capture_evidence_chunk c "
                "JOIN capture_evidence e ON e.evidence_id = c.evidence_id WHERE e.source_id = ?",
                (source_id,),
            ).fetchone()[0],
        )


def test_terminal_quarantine_sweep_k1_bounds_children_and_converges(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    now = 1_000_000_000
    monkeypatch.setattr("omi_collector.capture.adapters.quarantine._wall_clock_ns", lambda: now)
    config = CollectorConfig(
        staging_retention=StagingRetentionConfig(terminal_retention_seconds=1.0),
        writer=WriterConfig(chunk_records=1),
    )
    store = _store(tmp_path, config=config)
    attempt = _attempt(store, count=4)
    try:
        for ordinal in range(3):
            attempt.accept_chunk(100 + ordinal, _record(ordinal + 1), b"tail" if ordinal == 2 else b"")
        source_id = store.quarantine_attempt_source(attempt.attempt_id)
        store.mark_quarantine_unprocessable(source_id, "authentication failed")
        evidence_id = "e" * 64
        with closing(store.state_db.open_connection()) as connection:
            connection.execute(
                "INSERT INTO capture_evidence(evidence_id, source_id, entry_kind, original_relative_path, "
                "device, inode, mode, size, sha256, disposition, reason) "
                "VALUES (?, ?, 'regular', 'quarantine/remnant.bin', 1, 2, 384, 2, ?, 'unclassified', 'residual')",
                (evidence_id, source_id, sha256(b"xy").hexdigest()),
            )
            connection.executemany(
                "INSERT INTO capture_evidence_chunk(evidence_id, chunk_ordinal, bytes) VALUES (?, ?, ?)",
                ((evidence_id, 0, b"x"), (evidence_id, 1, b"y")),
            )
            connection.execute(
                "INSERT INTO capture_conflict(source_id, chunk_ordinal, leg_generation, absolute_byte_offset, reason, bytes) "
                "VALUES (?, 0, 1, 0, 'replay_mismatch', X'01')",
                (source_id,),
            )
            connection.commit()
        now += 1_000_000_000
        initial = _quarantine_child_count(store, source_id)
        assert initial == 8
        removed: list[str] = []
        for _ in range(initial):
            before = _quarantine_child_count(store, source_id)
            result = store.sweep_terminal_quarantine()
            assert result.progressed
            removed.extend(result.removed_source_ids)
            after = _quarantine_child_count(store, source_id)
            assert before - after == 1
            if after:
                assert store.quarantine_state(source_id) is QuarantineState.UNPROCESSABLE
        assert removed == [source_id]
        assert attempt_source(store, source_id) is None
        assert store.sweep_terminal_quarantine().progressed is False
    finally:
        attempt.close()


def test_terminal_quarantine_sweep_preserves_referenced_canonical_record(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    now = 1_000_000_000
    monkeypatch.setattr("omi_collector.capture.adapters.quarantine._wall_clock_ns", lambda: now)
    config = CollectorConfig(
        staging_retention=StagingRetentionConfig(terminal_retention_seconds=1.0),
        writer=WriterConfig(chunk_records=1),
    )
    store = _store(tmp_path, config=config)
    attempt = _attempt(store, count=2)
    try:
        raw = _record(1)
        attempt.accept_chunk(100, raw, b"tail")
        source_id = store.quarantine_attempt_source(attempt.attempt_id)
        store.mark_quarantine_unprocessable(source_id, "authentication failed")
        cohort_id = "c" * 64
        with closing(store.state_db.open_connection()) as connection:
            connection.execute(
                "INSERT INTO publication_cohort(cohort_id, lifecycle) VALUES (?, 'accepted')", (cohort_id,)
            )
            connection.execute(
                "INSERT INTO publication_cohort_member(cohort_id, member_ordinal, source_id, slice_start, "
                "slice_count, admitted_lifecycle, admitted_count, admitted_sha256) "
                "VALUES (?, 0, ?, 0, 1, 'blocked', 1, ?)",
                (cohort_id, source_id, sha256(raw).hexdigest()),
            )
            connection.commit()
        now += 1_000_000_000
        assert store.sweep_terminal_quarantine().removed_source_ids == ()
        assert _quarantine_child_count(store, source_id) == 1
        assert attempt_source(store, source_id) is not None
        with closing(store.state_db.open_connection()) as connection:
            connection.execute("DELETE FROM publication_cohort_member WHERE cohort_id = ?", (cohort_id,))
            connection.execute("DELETE FROM publication_cohort WHERE cohort_id = ?", (cohort_id,))
            connection.commit()
        assert store.sweep_terminal_quarantine().removed_source_ids == (source_id,)
        assert attempt_source(store, source_id) is None
    finally:
        attempt.close()


def test_corrupt_quarantine_state_is_retained_without_mutation(tmp_path: Path) -> None:
    store = _store(tmp_path)
    attempt = _attempt(store)
    source_id = store.quarantine_attempt_source(attempt.attempt_id)
    store.mark_quarantine_unprocessable(source_id, "terminal proof failed")
    with closing(store.state_db.open_connection()) as connection:
        connection.execute(
            "UPDATE capture_source SET retention_deadline = 'not-a-time' WHERE source_id = ?", (source_id,)
        )
        connection.commit()
    before = _source_row(store, source_id)

    assert store.sweep_terminal_quarantine().removed_source_ids == ()
    assert _source_row(store, source_id) == before
    attempt.close()


def test_quarantine_read_helper_closes_materialized_cursor() -> None:
    connection = sqlite3.connect(":memory:")
    cursor = connection.execute("SELECT 1")

    assert quarantine_module._db_one(cursor) == (1,)
    with pytest.raises(sqlite3.ProgrammingError):
        cursor.fetchone()
    connection.close()
