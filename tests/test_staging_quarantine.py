"""Focused quarantine staging ownership tests."""

from __future__ import annotations

import sqlite3
from contextlib import closing
from hashlib import sha256
from pathlib import Path
from typing import cast

import pytest

from omi_collector.capture.adapters.attempts import RecordAcceptanceError
from omi_collector.capture.adapters.clock_segments import ClockSegmentMap
from omi_collector.capture.adapters.collector_state_db import StateKind
from omi_collector.capture.adapters.internal_audio_store import InternalAudioStore, InternalAudioStoreError
from omi_collector.capture.adapters.opportunistic_runtime import OpportunisticRuntime
from omi_collector.capture.adapters.quarantine import QuarantineCustodyHoldError, QuarantinePublishError
from omi_collector.capture.adapters.ready_bundles import (
    ReadyOutcomeState,
    SqlitePublicationSource,
    finalize_typed_sources,
)
from omi_collector.capture.adapters.staging_contract import StagingError
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
        assert connection.execute("SELECT 1 FROM capture_conflict WHERE source_id = ?", (source_id,)).fetchone() is not None

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
