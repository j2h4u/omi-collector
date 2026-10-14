"""Bounded-cost regressions for SQL ready publication and retirement."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from contextlib import closing
from hashlib import sha256
from pathlib import Path
from threading import Event
from types import TracebackType
from typing import BinaryIO, TextIO, cast

import pytest

from omi_collector.capture.adapters import collector_state_db, ready_bundles
from omi_collector.capture.adapters.collector_state_db import StateDB, StateKind
from omi_collector.capture.adapters.internal_audio_store import AUDIO_RECORD_SIZE, InternalAudioStore
from omi_collector.capture.adapters.ready_bundles import ReadyOutcomeState
from omi_collector.capture.domain.ring_protocol import DoneNotification, ReadBeginNotification
from omi_collector.config import CollectorConfig, ReadyConfig, StagingRetentionConfig, WriterConfig
from staging_test_factory import StagingStore, attempt_source
from test_publication_fsm import _store


def _record(sequence: int) -> bytes:
    return sequence.to_bytes(4, "big") + bytes((2, 8, sequence % 256)) + bytes(AUDIO_RECORD_SIZE - 7)


class _HeldReadyOutput:
    def __init__(self, stream: BinaryIO, started: Event, release: Event) -> None:
        self.stream = stream
        self.started = started
        self.release = release

    def __enter__(self) -> _HeldReadyOutput:
        self.stream.__enter__()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> bool | None:
        return self.stream.__exit__(exc_type, exc_value, traceback)

    def write(self, data: bytes) -> int:
        if not self.started.is_set():
            self.started.set()
            if not self.release.wait(5):
                raise TimeoutError("test did not release held READY output write")
        return self.stream.write(data)

    def flush(self) -> None:
        self.stream.flush()

    def fileno(self) -> int:
        return self.stream.fileno()


def _add_source(store: StagingStore, start: int, count: int) -> bytes:
    raw = b"".join(_record(sequence) for sequence in range(start, start + count))
    attempt = store.prepare_streaming_attempt(start, count)
    attempt.prepare_leg(start, count)
    attempt.record_read_begin(ReadBeginNotification(start, count))
    attempt.accept_chunk(start, raw)
    attempt.seal(DoneNotification(0, start + count))
    attempt.close()
    return raw


def _add_quarantine_residue(store: StagingStore, source_id: str) -> None:
    evidence_id = "e" * 64
    with closing(store.state_db.open_connection()) as connection:
        connection.execute(
            "INSERT INTO capture_conflict("
            "source_id,chunk_ordinal,leg_generation,absolute_byte_offset,reason,bytes) "
            "VALUES (?,0,1,444,'retained conflict',?)",
            (source_id, b"conflict"),
        )
        connection.execute(
            "INSERT INTO capture_evidence("
            "evidence_id,source_id,entry_kind,original_relative_path,device,inode,mode,size,"
            "sha256,disposition,reason) VALUES (?,?, 'regular',?,1,2,384,1,?,'unclassified',?)",
            (
                evidence_id,
                source_id,
                "attempt/residual.bin",
                sha256(b"x").hexdigest(),
                "retained residual",
            ),
        )
        connection.execute(
            "INSERT INTO capture_evidence_chunk(evidence_id,chunk_ordinal,bytes) VALUES (?,0,?)",
            (evidence_id, b"x"),
        )
        connection.commit()


def _source_residue(store: StagingStore, source_id: str) -> tuple[int, int, int]:
    with closing(store.state_db.open_connection(read_only=True)) as connection:
        row = cast(
            tuple[int, int, int] | None,
            connection.execute(
                "SELECT "
                "(SELECT count(*) FROM capture_tail WHERE source_id=?),"
                "(SELECT count(*) FROM capture_conflict WHERE source_id=?),"
                "(SELECT count(*) FROM capture_evidence WHERE source_id=?)",
                (source_id, source_id, source_id),
            ).fetchone(),
        )
    assert row is not None
    return cast(tuple[int, int, int], tuple(row))


def _source_canonical_custody(store: StagingStore, source_id: str) -> tuple[int, int, int]:
    with closing(store.state_db.open_connection(read_only=True)) as connection:
        row = cast(
            tuple[int, int, int] | None,
            connection.execute(
                "SELECT "
                "(SELECT count(*) FROM capture_source WHERE source_id=?),"
                "(SELECT count(*) FROM capture_record WHERE source_id=?),"
                "(SELECT count(*) FROM publication_cohort_member WHERE source_id=?)",
                (source_id, source_id, source_id),
            ).fetchone(),
        )
    assert row is not None
    return cast(tuple[int, int, int], tuple(row))


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
    attempt = store.prepare_streaming_attempt(100, 2)
    attempt.prepare_leg(100, 2)
    attempt.record_read_begin(ReadBeginNotification(100, 2))
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
        assert store.sweep_terminal_quarantine().removed_source_ids == ()
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


def test_expired_quarantine_residue_cleanup_preserves_an_accepted_cohort(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    now = 1_000_000_000
    monkeypatch.setattr("omi_collector.capture.adapters.quarantine._wall_clock_ns", lambda: now)
    config = CollectorConfig(
        ready=ReadyConfig(target_audio_seconds=0.02),
        staging_retention=StagingRetentionConfig(terminal_retention_seconds=1.0),
        writer=WriterConfig(chunk_records=1),
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
    attempt = store.prepare_streaming_attempt(100, 2)
    attempt.prepare_leg(100, 2)
    attempt.record_read_begin(ReadBeginNotification(100, 2))
    raw = _record(100)
    attempt.accept_chunk(100, raw, b"retained-tail")
    source_id = store.quarantine_attempt_source(attempt.attempt_id)
    original_publish = ready_bundles._publish_typed_group

    def interrupt_publication(*_args: object, **_kwargs: object) -> object:
        raise OSError("publication interrupted after cohort acceptance")

    try:
        _add_quarantine_residue(store, source_id)
        store.admit_quarantined_prefix(source_id)
        store.append_ready_closure(101, "drained")
        monkeypatch.setattr(ready_bundles, "_publish_typed_group", interrupt_publication)

        interrupted = store.recover_and_publish()

        assert interrupted.state is ReadyOutcomeState.BLOCKED
        with closing(store.state_db.open_connection(read_only=True)) as connection:
            member = cast(
                tuple[str] | None,
                connection.execute(
                    "SELECT source_id FROM publication_cohort_member WHERE source_id=?",
                    (source_id,),
                ).fetchone(),
            )
        assert member is not None and tuple(member) == (source_id,)

        now += 1_000_000_000
        residue = _source_residue(store, source_id)
        for _ in range(8):
            store.sweep_terminal_quarantine()
            residue = _source_residue(store, source_id)
            if residue == (0, 0, 0):
                break
        assert residue == (0, 0, 0)
        assert _source_canonical_custody(store, source_id) == (1, 1, 1)

        monkeypatch.setattr(ready_bundles, "_publish_typed_group", original_publish)
        published = store.recover_and_publish()

        assert published.state is ReadyOutcomeState.PUBLISHED
        assert published.published[0].path.joinpath("records.bin").read_bytes() == raw
        for _ in range(4):
            if not store.publication_followup_due():
                break
            store.recover_and_publish()
        assert attempt_source(store, source_id) is None
    finally:
        attempt.close()


def test_ready_output_write_does_not_hold_shared_database_guard(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    asyncio.run(_ready_write_guard_scenario(tmp_path, monkeypatch))


async def _ready_write_guard_scenario(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    store = await asyncio.to_thread(_make_chunked_store, tmp_path)
    expected = b"".join((_record(100), _record(101)))
    await asyncio.to_thread(_add_source, store, 100, 2)
    await asyncio.to_thread(store.append_ready_closure, 102, "drained")
    output_write_started = Event()
    release_output_write = Event()
    open_page_cursors: set[int] = set()
    page_reads = 0
    original_execute = collector_state_db._GuardedCursor.execute
    original_close = collector_state_db._GuardedCursor.close

    def track_execute(cursor: collector_state_db._GuardedCursor, sql: str, parameters: object = ()):
        nonlocal page_reads
        result = original_execute(cursor, sql, parameters)
        if "FROM capture_record" in sql:
            page_reads += 1
            open_page_cursors.add(id(cursor))
        return result

    def track_close(cursor: collector_state_db._GuardedCursor) -> None:
        open_page_cursors.discard(id(cursor))
        original_close(cursor)

    monkeypatch.setattr(collector_state_db._GuardedCursor, "execute", track_execute)
    monkeypatch.setattr(collector_state_db._GuardedCursor, "close", track_close)
    monkeypatch.setattr(
        Path,
        "open",
        _hold_ready_output_write(
            cast(Callable[..., BinaryIO | TextIO], Path.open),
            tmp_path / "ready",
            output_write_started,
            release_output_write,
        ),
    )
    publication = asyncio.create_task(asyncio.to_thread(store.recover_and_publish))
    auxiliary: asyncio.Task[None] | None = None
    try:
        assert await asyncio.to_thread(output_write_started.wait, 3)
        assert page_reads >= 2 and not open_page_cursors
        auxiliary = asyncio.create_task(
            asyncio.to_thread(store.state_db.put, StateKind.QUALITY_EVENT, "independent", {"committed": True})
        )
        done, _ = await asyncio.wait({auxiliary}, timeout=3)
        assert auxiliary in done, "same-StateDB auxiliary transaction stalled during READY output write"
        await auxiliary
        release_output_write.set()
        done, _ = await asyncio.wait({publication}, timeout=3)
        assert publication in done, "READY publication did not finish after output write release"
        result = await publication
        assert result.state is ReadyOutcomeState.PUBLISHED
        assert await asyncio.to_thread(result.published[0].path.joinpath("records.bin").read_bytes) == expected
        assert await asyncio.to_thread(store.state_db.get, StateKind.QUALITY_EVENT, "independent") == {
            "committed": True
        }
    finally:
        release_output_write.set()
        if not publication.done():
            await asyncio.gather(publication, return_exceptions=True)
        if auxiliary is not None and not auxiliary.done():
            await asyncio.gather(auxiliary, return_exceptions=True)


def _make_chunked_store(tmp_path: Path) -> StagingStore:
    bootstrap = StagingStore(tmp_path / "spool", tmp_path / "capture")
    ready = tmp_path / "ready"
    ready.mkdir(mode=0o2750)
    ready.chmod(0o2750)
    config = CollectorConfig(ready=ReadyConfig(target_audio_seconds=0.02), writer=WriterConfig(chunk_records=1))
    return StagingStore.from_paths(
        bootstrap.paths,
        state_db=bootstrap.state_db,
        publication_root=ready,
        config=config,
    )


def _hold_ready_output_write(
    original_open: Callable[..., BinaryIO | TextIO], ready_root: Path, started: Event, release: Event
) -> Callable[..., BinaryIO | TextIO]:
    def open_output(path: Path, mode: str = "r", *args: object, **kwargs: object) -> BinaryIO | TextIO:
        stream = original_open(path, mode, *args, **kwargs)
        if path.name == ready_bundles._RAW_NAME and ready_root in path.parents and mode == "xb":
            return cast(BinaryIO, _HeldReadyOutput(cast(BinaryIO, stream), started, release))
        return stream

    return open_output


def test_terminalized_prefix_tail_enters_ready_custody_and_retires_after_restart(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    now = 1_000_000_000
    monkeypatch.setattr("omi_collector.capture.adapters.quarantine._wall_clock_ns", lambda: now)
    config = CollectorConfig(
        ready=ReadyConfig(target_audio_seconds=0.02),
        staging_retention=StagingRetentionConfig(terminal_retention_seconds=1.0),
        writer=WriterConfig(chunk_records=1),
    )
    bootstrap = StagingStore(tmp_path / "spool", tmp_path / "capture", config=config)
    ready = tmp_path / "ready"
    ready.mkdir(mode=0o2750)
    ready.chmod(0o2750)
    store = StagingStore.from_paths(bootstrap.paths, state_db=bootstrap.state_db, publication_root=ready, config=config)
    attempt = store.prepare_streaming_attempt(100, 3)
    attempt.prepare_leg(100, 3)
    attempt.record_read_begin(ReadBeginNotification(100, 3))
    raw = _record(100) + _record(101)
    try:
        attempt.accept_chunk(100, raw, b"retained-tail")
        source_id = attempt.attempt_id
        attempt.interrupt()
        attempt.close(durable=True)
        store.terminalize_prefix_attempt(source_id)
        _add_quarantine_residue(store, source_id)

        admission = store.admit_quarantined_prefix(source_id)
        assert (admission.record_count, admission.raw_sha256) == (2, sha256(raw).hexdigest())
        store.append_ready_closure(102, "drained")
        published = store.recover_and_publish()
        assert published.state is ReadyOutcomeState.PUBLISHED
        assert published.retirement_pending
        assert _source_canonical_custody(store, source_id)[:2] == (1, 1)
        assert _source_residue(store, source_id) == (1, 1, 1)

        now += 1_000_000_000
        restarted = StagingStore.from_paths(
            bootstrap.paths, state_db=bootstrap.state_db, publication_root=ready, config=config
        )
        for _ in range(8):
            restarted.sweep_terminal_quarantine()
            if _source_residue(restarted, source_id) == (0, 0, 0):
                break
        assert _source_residue(restarted, source_id) == (0, 0, 0)
        assert _source_canonical_custody(restarted, source_id)[:2] == (1, 1)
        assert restarted.publication_followup_due()
        for _ in range(8):
            restarted.recover_and_publish()
            if attempt_source(restarted, source_id) is None:
                break
        assert attempt_source(restarted, source_id) is None
        assert not restarted.recover_and_publish().retirement_pending
    finally:
        attempt.close()
