from __future__ import annotations

import asyncio
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from struct import pack
from threading import Event, Thread

import pytest

from omi_collector.capture.adapters.opportunistic_runtime import _StagingWriterAdapter
from omi_collector.capture.adapters.ready_bundles import ReadyOutcome, ReadyOutcomeState
from omi_collector.capture.adapters.ready_closures import ReadyClosure
from omi_collector.capture.adapters.ready_closures import load as load_ready_closures
from omi_collector.capture.adapters.staging_filesystem import DeviceLock
from omi_collector.capture.adapters.staging_store import StagingStore
from omi_collector.capture.adapters.staging_writer import StagingWriter
from omi_collector.capture.domain.ring_protocol import RECORD_SIZE, DoneNotification, ReadBeginNotification
from omi_collector.config import CollectorConfig, ReadyConfig


def _store(tmp_path: Path) -> StagingStore:
    capture_root = tmp_path / "draft"
    capture_root.mkdir()
    bootstrap = StagingStore(tmp_path / "spool", capture_root)
    ready = tmp_path / "ready"
    ready.mkdir(mode=0o2750)
    ready.chmod(0o2750)
    return StagingStore.from_paths(
        bootstrap.paths,
        publication_root=ready,
        config=CollectorConfig(ready=ReadyConfig(target_audio_seconds=0.02)),
    )


def _record(value: int) -> bytes:
    return pack(">I", value) + bytes((2, 8, value % 256)) + bytes(RECORD_SIZE - 7)


def _seal_writer(store: StagingStore) -> StagingWriter:
    writer = StagingWriter(store, 100, 1)
    writer.prepare()
    writer.prepare_leg(100, 1)
    writer.read_begin(ReadBeginNotification(100, 1))
    writer.append_chunk(0, memoryview(_record(100)))
    writer.seal(DoneNotification(0, 101))
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


def test_restart_closes_and_persists_the_frontier_of_an_orphaned_sealed_draft(tmp_path: Path) -> None:
    store = _store(tmp_path)
    writer = _seal_writer(store)
    writer.close()

    expected = ReadyClosure(101, "restart_interrupted")
    assert store.close_orphaned_drafts("restart_interrupted") == expected
    assert load_ready_closures(store.ready_closures_path) == (expected,)
    drafts = tuple(store.capture_root.iterdir())
    assert len(drafts) == 1
    assert (drafts[0] / "records.bin").read_bytes() == _record(100)


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
    attempt_path = store.attempts_root / writer.attempt_id

    retired: list[bool] = []
    original = store.terminalize_prefix_attempt_held

    def observe_terminalization(attempt_id: str, lease: DeviceLock) -> None:
        lease.require_active()
        retired.append((attempt_path / "prefix-publication.json").is_file())
        original(attempt_id, lease)

    monkeypatch.setattr(store, "terminalize_prefix_attempt_held", observe_terminalization)
    assert writer.publish_prefix() is not None
    assert tuple(store.capture_root.iterdir())

    writer.close()

    assert retired == [True]
    assert (attempt_path / "terminal-retired.json").is_file()
    writer.close()
    store.append_ready_closure(101, "drained")
    assert store.publish_ready() is not None
    assert not tuple(store.capture_root.iterdir())
    assert store.sweep_terminal_retired() == ()


def test_child_task_publishes_after_writer_releases(tmp_path: Path) -> None:
    store = _store(tmp_path)
    writer = _seal_writer(store)
    writer.close()
    store.append_ready_closure(101, "drained")

    async def publish_from_child_task() -> object | None:
        async def publish() -> object | None:
            return store.publish_ready()

        return await asyncio.create_task(publish())

    assert asyncio.run(publish_from_child_task()) is not None
    assert any(path.is_dir() and (path / "manifest.json").exists() for path in (tmp_path / "ready").iterdir())


def test_clock_mutation_lease_reuses_active_writer_storage_lease(tmp_path: Path) -> None:
    store = _store(tmp_path)

    with (
        store.device_lock(recover_capture_temporaries=False) as writer_lease,
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
    def tracked_device_lock(
        *, recover_capture_temporaries: bool = True, operation: str = "unknown"
    ) -> Iterator[DeviceLock]:
        with original_device_lock(
            recover_capture_temporaries=recover_capture_temporaries, operation=operation
        ) as lease:
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

    assert tuple(store.capture_root.iterdir())
    assert not tuple((tmp_path / "ready").glob("*/manifest.json"))
    monkeypatch.setattr(store, "_recover_and_publish_unlocked", original)

    assert writer._lease is not None
    store._append_ready_closure_unlocked(writer._lease, 101, "drained")
    assert writer.publish_ready().state is ReadyOutcomeState.PUBLISHED
    assert any(path.is_dir() and (path / "manifest.json").exists() for path in (tmp_path / "ready").iterdir())
    writer.close()


def test_writer_seal_keeps_draft_until_closed_visit(tmp_path: Path) -> None:
    store = _store(tmp_path)
    writer = _StagingWriterAdapter(store.make_staging_writer(100, 1), 100)
    writer.prepare()
    writer.prepare_leg(100, 1)
    writer.read_begin(ReadBeginNotification(100, 1))
    writer.append_chunk(0, memoryview(_record(100)))

    writer.seal(DoneNotification(0, 101))

    assert tuple(store.capture_root.iterdir())
    assert not tuple((tmp_path / "ready").iterdir())
    writer.close()
    store.append_ready_closure(101, "drained")
    assert store.publish_ready().state is ReadyOutcomeState.PUBLISHED


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
