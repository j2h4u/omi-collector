"""Bounded-cost regressions for SQL ready publication and retirement."""

from __future__ import annotations

from contextlib import closing
from hashlib import sha256
from pathlib import Path
from typing import cast

import pytest

from omi_collector.capture.adapters import ready_bundles
from omi_collector.capture.adapters.collector_state_db import StateDB
from omi_collector.capture.adapters.internal_audio_store import AUDIO_RECORD_SIZE, InternalAudioStore
from omi_collector.capture.adapters.ready_bundles import ReadyOutcomeState
from omi_collector.capture.domain.ring_protocol import DoneNotification, ReadBeginNotification
from omi_collector.config import CollectorConfig, ReadyConfig, StagingRetentionConfig
from staging_test_factory import StagingStore, attempt_source
from test_publication_fsm import _store


def _record(sequence: int) -> bytes:
    return sequence.to_bytes(4, "big") + bytes((2, 8, sequence % 256)) + bytes(AUDIO_RECORD_SIZE - 7)


def _add_source(store: StagingStore, start: int, count: int) -> bytes:
    raw = b"".join(_record(sequence) for sequence in range(start, start + count))
    attempt = store.prepare_streaming_attempt(start, count)
    attempt.prepare_leg(start, count)
    attempt.record_read_begin(ReadBeginNotification(start, count))
    attempt.accept_chunk(start, raw)
    attempt.seal(DoneNotification(0, start + count))
    attempt.close()
    return raw


def test_retirement_reuses_authenticated_custody_and_keeps_immediate_progress(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    record_count = 1_024
    store = _store(tmp_path, threshold=0.02)
    expected = _add_source(store, 100, record_count)
    store.append_ready_closure(100 + record_count, "drained")
    digest_calls = 0
    page_calls = 0
    connection_calls = 0
    original_ready_source = ready_bundles._read_ready_source
    original_page = InternalAudioStore.read_record_page
    original_connection = StateDB.open_connection

    def counted_ready_source(path: Path):
        nonlocal digest_calls
        digest_calls += 1
        return original_ready_source(path)

    def counted_page(audio_store: InternalAudioStore, source_id: str, after_ordinal: int = -1, limit: int = 128):
        nonlocal page_calls
        page_calls += 1
        return original_page(audio_store, source_id, after_ordinal, limit)

    def counted_connection(state_db: StateDB, *, read_only: bool = False):
        nonlocal connection_calls
        connection_calls += 1
        return original_connection(state_db, read_only=read_only)

    monkeypatch.setattr(ready_bundles, "_read_ready_source", counted_ready_source)
    monkeypatch.setattr(InternalAudioStore, "read_record_page", counted_page)
    monkeypatch.setattr(StateDB, "open_connection", counted_connection)

    published = store.recover_and_publish()

    assert published.state is ReadyOutcomeState.PUBLISHED
    assert published.retirement_pending
    assert page_calls >= 3 * (record_count // 128)
    assert connection_calls < page_calls
    (bundle,) = published.published
    output = bundle.path / "records.bin"
    identity = output.stat().st_ino, output.stat().st_mtime_ns
    assert output.read_bytes() == expected

    followups = 0
    while store.publication_followup_due():
        followups += 1
        assert followups <= 10
        store.recover_and_publish()
        assert output.read_bytes() == expected
        assert (output.stat().st_ino, output.stat().st_mtime_ns) == identity

    assert followups == 9
    assert digest_calls == 1


def test_retirement_waits_for_retained_residue_then_resumes_after_expiry(
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
    store = StagingStore.from_paths(
        bootstrap.paths,
        state_db=bootstrap.state_db,
        publication_root=ready,
        config=config,
    )
    attempt = store.prepare_streaming_attempt(100, 1)
    attempt.prepare_leg(100, 1)
    attempt.record_read_begin(ReadBeginNotification(100, 1))
    raw = _record(100)
    attempt.accept_chunk(100, raw, b"retained-tail")
    source_id = store.quarantine_attempt_source(attempt.attempt_id)
    try:
        with closing(store.state_db.open_connection()) as connection:
            connection.execute(
                "INSERT INTO capture_evidence("
                "evidence_id,source_id,entry_kind,original_relative_path,device,inode,mode,size,"
                "sha256,disposition,reason) VALUES (?,?, 'regular',?,1,2,384,1,?,'unclassified',?)",
                (
                    "e" * 64,
                    source_id,
                    "attempt/residual.bin",
                    sha256(b"x").hexdigest(),
                    "retained residual",
                ),
            )
            connection.commit()
        store.admit_quarantined_prefix(source_id)
        store.append_ready_closure(101, "drained")

        published = store.recover_and_publish()

        assert published.state is ReadyOutcomeState.PUBLISHED
        assert published.retirement_pending
        assert store.publication_followup_due()
        generation = store.state_db.get_generation()

        stalled = store.recover_and_publish()

        assert not stalled.retirement_pending
        assert not store.publication_followup_due()
        assert store.state_db.get_generation() == generation
        retained = attempt_source(store, source_id)
        with closing(store.state_db.open_connection(read_only=True)) as connection:
            retirement_cursor = cast(
                tuple[object] | None,
                connection.execute(
                    "SELECT retirement_cursor FROM capture_source WHERE source_id=?", (source_id,)
                ).fetchone(),
            )
        assert retained is not None and retained.lifecycle == "retiring"
        assert retirement_cursor is not None and retirement_cursor[0] == 1

        now += 1_000_000_000
        assert store.sweep_terminal_quarantine() == ()
        assert store.state_db.get_generation() > generation
        assert store.publication_followup_due()

        resumed = store.recover_and_publish()

        assert resumed.retirement_pending
        assert attempt_source(store, source_id) is None
        assert store.publication_followup_due()
        settled = store.recover_and_publish()
        assert not settled.retirement_pending
        assert not store.publication_followup_due()
    finally:
        attempt.close()
