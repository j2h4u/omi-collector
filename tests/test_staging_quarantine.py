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
from omi_collector.capture.adapters.internal_audio_store import InternalAudioStore
from omi_collector.capture.adapters.quarantine import QuarantinePublishError
from omi_collector.capture.adapters.ready_bundles import (
    ReadyOutcomeState,
    SqlitePublicationSource,
    finalize_typed_sources,
)
from omi_collector.capture.domain.ring_protocol import RECORD_SIZE, DoneNotification, ReadBeginNotification
from omi_collector.config import CollectorConfig, ReadyConfig, StagingRetentionConfig
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
        assert store.sweep_terminal_quarantine() == ()
        now += 1
        assert store.sweep_terminal_quarantine() == (source_id,)
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
    attempt = _attempt(store, count=1)
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
        assert store.sweep_terminal_quarantine() == ()
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
    assert isinstance(before[6], str)
    now = int(before[6])
    assert store.sweep_terminal_quarantine() == ()
    retained = _source_row(store, source_id)
    assert retained is not None and retained[3] == "unclassified"
    with closing(store.state_db.open_connection(read_only=True)) as connection:
        assert connection.execute("SELECT 1 FROM capture_conflict WHERE source_id = ?", (source_id,)).fetchone() is None

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
    assert admitted[6] == before[6]
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

    assert store.sweep_terminal_quarantine() == ()
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
    assert store.sweep_terminal_quarantine() == ()
    with closing(store.state_db.open_connection(read_only=True)) as connection:
        assert (
            connection.execute("SELECT 1 FROM capture_evidence WHERE evidence_id = ?", (evidence_id,)).fetchone()
            is None
        )


def test_corrupt_source_digest_cannot_be_salvaged_or_discarded(tmp_path: Path) -> None:
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

        with pytest.raises(QuarantinePublishError):
            store.admit_quarantined_prefix(source_id)

        source = attempt_source(store, source_id)
        assert source is not None and source.committed_count == 1
        with closing(store.state_db.open_connection(read_only=True)) as connection:
            certified = cast(
                tuple[object, object, object] | None,
                connection.execute(
                    "SELECT certified_prefix_count, coverage_digest, committed_sha256 "
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
        assert certified is not None and tuple(certified) == (1, sha256(raw).hexdigest(), "0" * 64)
        assert record is not None and record[0] == raw[4:]
    finally:
        attempt.close()


def test_terminal_quarantine_obeys_retention_deadline_and_keeps_unterminalized_source(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    now = 1_000_000_000
    monkeypatch.setattr("omi_collector.capture.adapters.quarantine._wall_clock_ns", lambda: now)
    store = _store(
        tmp_path,
        config=CollectorConfig(staging_retention=StagingRetentionConfig(terminal_retention_seconds=72.0)),
    )
    aged = _attempt(store, 100, 1)
    live = _attempt(store, 200, 1)
    try:
        residual = _record(3)
        aged.accept_chunk(100, residual, b"retained tail")
        aged_id = store.quarantine_attempt_source(aged.attempt_id)
        store.mark_quarantine_unprocessable(aged_id, "cannot authenticate")
        live.accept_chunk(200, _record(4))
        live_id = store.quarantine_attempt_source(live.attempt_id)
        now += 72_000_000_000 - 1

        assert store.sweep_terminal_quarantine() == ()
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

        assert store.sweep_terminal_quarantine() == (aged_id,)
        assert attempt_source(store, aged_id) is None
        assert attempt_source(store, live_id) is not None
    finally:
        aged.close()
        live.close()


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

    assert store.sweep_terminal_quarantine() == ()
    assert _source_row(store, source_id) == before
    attempt.close()
