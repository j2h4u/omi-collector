from __future__ import annotations

import asyncio
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from struct import pack
from threading import Event, Thread

import pytest

from omi_collector.capture.adapters.collector_state_db import StateKind
from omi_collector.capture.adapters.opportunistic_runtime import OpportunisticRuntime, _StagingWriterAdapter
from omi_collector.capture.adapters.ready_bundles import ReadyOutcome, ReadyOutcomeState
from omi_collector.capture.adapters.ready_closures import ReadyClosure
from omi_collector.capture.adapters.staging_filesystem import DeviceLock
from omi_collector.capture.adapters.staging_writer import StagingWriter
from omi_collector.capture.application.quarantine_maintenance import QuarantineMaintenance
from omi_collector.capture.domain.ring_protocol import RECORD_SIZE, DoneNotification, ReadBeginNotification
from omi_collector.config import CollectorConfig, ReadyConfig, RetryConfig
from staging_test_factory import StagingStore, attempt_source, closure_payload


def _store(tmp_path: Path, *, retry_backoff: float | None = None) -> StagingStore:
    capture_root = tmp_path / "draft"
    capture_root.mkdir()
    bootstrap = StagingStore(tmp_path / "spool", capture_root)
    ready = tmp_path / "ready"
    ready.mkdir(mode=0o2750)
    ready.chmod(0o2750)
    return StagingStore.from_paths(
        bootstrap.paths,
        publication_root=ready,
        config=CollectorConfig(
            ready=ReadyConfig(target_audio_seconds=0.02),
            retry=RetryConfig(rapid_backoff=(retry_backoff,)) if retry_backoff is not None else RetryConfig(),
        ),
    )


def _record(value: int) -> bytes:
    return pack(">I", value) + bytes((2, 8, value % 256)) + bytes(RECORD_SIZE - 7)


def _seal_writer(store: StagingStore, start: int = 100) -> StagingWriter:
    writer = StagingWriter(store, start, 1)
    writer.prepare()
    writer.prepare_leg(start, 1)
    writer.read_begin(ReadBeginNotification(start, 1))
    writer.append_chunk(0, memoryview(_record(start)))
    writer.seal(DoneNotification(0, start + 1))
    return writer


def test_fresh_store_without_publication_boundary_is_a_noop(tmp_path: Path) -> None:
    capture_root = tmp_path / "draft"
    capture_root.mkdir()
    store = StagingStore(tmp_path / "spool", capture_root)

    assert store.publish_ready().state == "waiting"


def test_sealed_writer_publishes_with_its_held_lease(tmp_path: Path) -> None:
    store = _store(tmp_path)
    writer = _seal_writer(store)
    assert writer._lease is not None
    store._append_ready_closure_unlocked(writer._lease, 101, "drained")

    ready = writer.publish_ready()

    assert ready.state == "published"
    assert any(path.is_dir() and (path / "manifest.json").exists() for path in (tmp_path / "ready").iterdir())
    writer.close()


def test_restart_closes_and_persists_the_frontier_of_a_sealed_sql_attempt(tmp_path: Path) -> None:
    store = _store(tmp_path)
    writer = _seal_writer(store)
    attempt_id = writer.attempt_id
    writer.close()

    expected = ReadyClosure(101, "restart_interrupted")
    assert store.close_pending_prefix("restart_interrupted") == expected
    assert closure_payload(store) == {
        "version": 1,
        "closures": [{"next_sequence": expected.next_sequence, "reason": expected.reason}],
    }
    source = attempt_source(store, attempt_id)
    assert source is not None
    assert source.lifecycle == "sealed_waiting"
    assert source.committed_count == 1


def test_recovery_closes_each_new_sealed_frontier_once(tmp_path: Path) -> None:
    store = _store(tmp_path)
    first = _seal_writer(store)
    first.close()

    assert store.inspect_recovery() == (True, False)
    assert store.close_pending_prefix("restart_interrupted") == ReadyClosure(101, "restart_interrupted")
    assert store.inspect_recovery() == (False, False)

    second = _seal_writer(store, 101)
    second.close()

    assert store.inspect_recovery() == (True, False)
    assert store.close_pending_prefix("restart_interrupted") == ReadyClosure(102, "restart_interrupted")
    assert store.inspect_recovery() == (False, False)


def test_prefix_close_retires_before_ready_publication_and_keeps_lease(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = _store(tmp_path)
    writer = StagingWriter(store, 100, 1)
    writer.prepare()
    writer.prepare_leg(100, 1)
    writer.read_begin(ReadBeginNotification(100, 1))
    writer.append_chunk(0, memoryview(_record(100)))
    writer.checkpoint()
    attempt_id = writer.attempt_id

    terminalized: list[bool] = []
    original = store.terminalize_prefix_attempt_held

    def observe_terminalization(attempt_id: str, lease: DeviceLock) -> None:
        lease.require_active()
        source = attempt_source(store, attempt_id)
        assert source is not None
        terminalized.append(source.lifecycle == "receiving")
        original(attempt_id, lease)

    monkeypatch.setattr(store, "terminalize_prefix_attempt_held", observe_terminalization)
    writer.publish_prefix()
    source = attempt_source(store, attempt_id)
    assert source is not None and source.lifecycle == "receiving"

    writer.close()

    source = attempt_source(store, attempt_id)
    assert source is not None and source.lifecycle == "sealed_waiting"
    assert terminalized == [True]
    assert attempt_source(store, attempt_id) is not None
    writer.close()
    store.append_ready_closure(101, "drained")
    published = store.publish_ready()
    source = attempt_source(store, attempt_id)
    assert published.state is ReadyOutcomeState.PUBLISHED
    assert published.retirement_pending
    assert source is not None and source.lifecycle == "retiring"
    assert store.publish_ready().retirement_pending
    assert attempt_source(store, attempt_id) is None


def test_child_task_publishes_after_writer_releases(tmp_path: Path) -> None:
    store = _store(tmp_path)
    writer = _seal_writer(store)
    writer.close()
    store.append_ready_closure(101, "drained")

    async def publish_from_child_task() -> object | None:
        async def publish() -> object | None:
            return await asyncio.to_thread(store.publish_ready)

        return await asyncio.create_task(publish())

    assert asyncio.run(publish_from_child_task()) is not None
    assert any(path.is_dir() and (path / "manifest.json").exists() for path in (tmp_path / "ready").iterdir())


def test_clock_mutation_lease_reuses_active_writer_storage_lease(tmp_path: Path) -> None:
    store = _store(tmp_path)

    with (
        store.device_lock() as writer_lease,
        store.clock_mutation_lease() as clock_lease,
    ):
        assert clock_lease is writer_lease


def test_clock_mutation_lease_reuses_current_publication_lease_and_falls_back_after_release(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = _store(tmp_path)
    acquired: list[DeviceLock] = []
    publication_leases: list[DeviceLock | None] = []
    published = ReadyOutcome(ReadyOutcomeState.WAITING, reason="no_closed_frontier")
    original_device_lock = store.device_lock

    @contextmanager
    def tracked_device_lock(*, operation: str = "unknown") -> Iterator[DeviceLock]:
        with original_device_lock(operation=operation) as lease:
            acquired.append(lease)
            yield lease

    def publish_unlocked() -> ReadyOutcome:
        publication_leases.append(store._filesystem._active_lease)
        return published

    monkeypatch.setattr(store, "device_lock", tracked_device_lock)
    monkeypatch.setattr(store, "_recover_and_publish_unlocked", publish_unlocked)

    with store.clock_mutation_lease() as lease:
        assert store.publish_ready(lease) is published
        assert acquired == [lease]
        assert publication_leases == [lease]

    store.publication_input_changed()
    assert store.publish_ready() is published
    assert len(acquired) == 2
    assert publication_leases == [acquired[0], acquired[1]]


def test_failed_sealed_publication_retains_capture_for_retry(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    store = _store(tmp_path)
    writer = _seal_writer(store)
    original = store._recover_and_publish_unlocked

    def fail_publication() -> ReadyOutcome:
        raise OSError("source unavailable")

    monkeypatch.setattr(store, "_recover_and_publish_unlocked", fail_publication)
    assert writer.publish_ready().state is ReadyOutcomeState.TRANSIENT

    assert attempt_source(store, writer.attempt_id) is not None
    assert not tuple((tmp_path / "ready").glob("*/manifest.json"))
    monkeypatch.setattr(store, "_recover_and_publish_unlocked", original)

    assert writer._lease is not None
    store._append_ready_closure_unlocked(writer._lease, 101, "drained")
    published = writer.publish_ready()
    source = attempt_source(store, writer.attempt_id)
    assert published.state is ReadyOutcomeState.PUBLISHED
    assert published.retirement_pending
    assert source is not None and source.lifecycle == "retiring"
    assert any(path.is_dir() and (path / "manifest.json").exists() for path in (tmp_path / "ready").iterdir())
    assert writer.publish_ready().retirement_pending
    assert attempt_source(store, writer.attempt_id) is None
    writer.close()


def test_writer_seal_keeps_draft_until_closed_visit(tmp_path: Path) -> None:
    store = _store(tmp_path)
    writer = _StagingWriterAdapter(store.make_staging_writer(100, 1), 100)
    writer.prepare()
    writer.prepare_leg(100, 1)
    writer.read_begin(ReadBeginNotification(100, 1))
    writer.append_chunk(0, memoryview(_record(100)))

    writer.seal(DoneNotification(0, 101))

    attempt_id = writer.attempt_id
    source = attempt_source(store, attempt_id)
    assert source is not None and source.lifecycle == "sealed_waiting"
    assert not tuple((tmp_path / "ready").iterdir())
    writer.close()
    store.append_ready_closure(101, "drained")
    published = store.publish_ready()
    source = attempt_source(store, attempt_id)
    assert published.state is ReadyOutcomeState.PUBLISHED
    assert published.retirement_pending
    assert source is not None and source.lifecycle == "retiring"
    assert store.publish_ready().retirement_pending
    assert attempt_source(store, attempt_id) is None


def test_writer_seal_does_not_report_false_recovery(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    store = _store(tmp_path)
    events: list[str] = []
    monkeypatch.setattr(
        "omi_collector.capture.adapters.opportunistic_runtime.debug_event",
        lambda event, **_fields: events.append(event),
    )
    writer = _StagingWriterAdapter(store.make_staging_writer(100, 1), 100)
    writer.prepare()
    writer.prepare_leg(100, 1)
    writer.read_begin(ReadBeginNotification(100, 1))
    writer.append_chunk(0, memoryview(_record(100)))
    writer.seal(DoneNotification(0, 101))
    writer.close()
    assert events == []


def test_publication_owner_rejects_concurrent_projection(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    store = _store(tmp_path)
    writer = _seal_writer(store)
    writer.close()
    entered = Event()
    release = Event()
    errors: list[BaseException] = []

    def blocked_publication() -> ReadyOutcome:
        entered.set()
        assert release.wait(timeout=1)
        return ReadyOutcome(ReadyOutcomeState.WAITING, reason="no_closed_frontier")

    monkeypatch.setattr(store, "_recover_and_publish_unlocked", blocked_publication)

    def publish() -> None:
        try:
            store.publish_ready()
        except BaseException as error:  # noqa: BLE001 - test records the thread boundary
            errors.append(error)

    thread = Thread(target=publish)
    thread.start()
    assert entered.wait(timeout=1)
    assert store.publish_ready().reason == "publication_active"
    release.set()
    thread.join(timeout=1)

    assert not thread.is_alive()
    assert errors == []


def test_compaction_reuses_publication_lease(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    store = _store(tmp_path)
    store.state_db.put(StateKind.COMPACTION, "current", {"generation": 1, "pending": True})
    seen: list[DeviceLock | None] = []

    def vacuum(_state_db: object, _generation: int, _interrupt: Event) -> str:
        seen.append(store._filesystem._active_lease)
        return "done"

    monkeypatch.setattr("omi_collector.capture.adapters.staging_store.vacuum_pending", vacuum)
    with store.device_lock(operation="test_compaction") as lease:
        assert store.publish_ready(lease).state is ReadyOutcomeState.WAITING
        assert seen == [lease]


def test_capture_interrupts_and_joins_compaction_before_visit(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    store = _store(tmp_path)
    store.state_db.put(StateKind.COMPACTION, "current", {"generation": 1, "pending": True})
    maintenance = QuarantineMaintenance(store, None, OpportunisticRuntime())
    entered = Event()
    release = Event()
    settled = Event()
    visit_started = Event()
    original_visit = store.begin_ready_visit

    def vacuum(_state_db: object, _generation: int, interrupt: Event) -> str:
        entered.set()
        assert interrupt.wait(5)
        assert release.wait(5)
        settled.set()
        return "interrupted"

    def begin_visit() -> object:
        visit_started.set()
        return original_visit()

    monkeypatch.setattr("omi_collector.capture.adapters.staging_store.vacuum_pending", vacuum)
    monkeypatch.setattr(store, "begin_ready_visit", begin_visit)

    async def scenario() -> None:
        maintenance.schedule_publication_retry()
        assert await asyncio.to_thread(entered.wait, 5)
        capture = asyncio.create_task(maintenance.enter_capture_priority())
        try:
            assert await asyncio.to_thread(store._compaction_interrupt.wait, 5)
            await asyncio.sleep(0)
            assert not capture.done()
            assert not visit_started.is_set()
            capture.cancel()
            capture.cancel()
            await asyncio.sleep(0)
            assert not capture.done()
            assert not visit_started.is_set()
            release.set()
            await asyncio.gather(capture, return_exceptions=True)
            assert settled.is_set()
            assert not visit_started.is_set()
        finally:
            release.set()
            await maintenance.close()

    asyncio.run(scenario())


def test_deferred_compaction_retries_without_new_publication_input(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = _store(tmp_path, retry_backoff=0.05)
    store.state_db.put(StateKind.COMPACTION, "current", {"generation": 1, "pending": True})
    maintenance = QuarantineMaintenance(store, None, OpportunisticRuntime())
    first = Event()
    retried = Event()
    calls = 0

    def vacuum(_state_db: object, generation: int, _interrupt: Event) -> str:
        nonlocal calls
        calls += 1
        if calls == 1:
            first.set()
            return "deferred"
        assert generation == 1
        store.state_db.put(StateKind.COMPACTION, "current", {"generation": 1, "pending": False})
        retried.set()
        return "done"

    monkeypatch.setattr("omi_collector.capture.adapters.staging_store.vacuum_pending", vacuum)

    async def scenario() -> None:
        try:
            await maintenance.ensure_publication_ready()
            assert first.is_set()
            assert store.publication_retry_schedule() is not None
            assert await asyncio.to_thread(retried.wait, 5)
            assert calls == 2
            assert store.publication_retry_schedule() is None
        finally:
            await maintenance.close()

    asyncio.run(scenario())


@pytest.mark.parametrize("followup_state", (ReadyOutcomeState.WAITING, ReadyOutcomeState.BLOCKED))
def test_input_change_during_compaction_runs_followup_publication(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, followup_state: ReadyOutcomeState
) -> None:
    store = _store(tmp_path)
    store.state_db.put(StateKind.COMPACTION, "current", {"generation": 1, "pending": True})
    entered = Event()
    release = Event()
    publications = 0
    errors: list[BaseException] = []
    results: list[ReadyOutcome] = []
    revision = ["r1"]

    def publication() -> ReadyOutcome:
        nonlocal publications
        publications += 1
        state = ReadyOutcomeState.WAITING if publications == 1 else followup_state
        return ReadyOutcome(state, reason="followup" if publications > 1 else "idle")

    def vacuum(_state_db: object, _generation: int, _interrupt: Event) -> str:
        entered.set()
        assert release.wait(5)
        store.state_db.put(StateKind.COMPACTION, "current", {"generation": 1, "pending": False})
        return "done"

    monkeypatch.setattr(store, "_recover_and_publish_unlocked", publication)
    monkeypatch.setattr(store, "_publication_revision", lambda: revision[0])
    monkeypatch.setattr("omi_collector.capture.adapters.staging_store.vacuum_pending", vacuum)

    def publish() -> None:
        try:
            results.append(store.publish_ready())
        except BaseException as error:  # noqa: BLE001 - propagate worker failure to the main assertion
            errors.append(error)

    thread = Thread(target=publish)
    thread.start()
    try:
        assert entered.wait(5)
        assert store.publication_input_changed() is False
        revision[0] = "r2"
        release.set()
        thread.join(timeout=5)
        assert not thread.is_alive()
        assert errors == []
        assert publications == 2
        assert len(results) == 1 and results[0].state is followup_state
    finally:
        release.set()
        thread.join(timeout=5)
