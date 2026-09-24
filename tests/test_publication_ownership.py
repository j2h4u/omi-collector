from __future__ import annotations

import asyncio
from pathlib import Path
from struct import pack
from threading import Event, Thread

import pytest

from omi_collector.capture.adapters.opportunistic_runtime import _StagingWriterAdapter
from omi_collector.capture.adapters.staging_contract import AttemptStateError
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


def test_sealed_writer_publishes_with_its_held_lease(tmp_path: Path) -> None:
    store = _store(tmp_path)
    authority = store.create_publication_authority()
    writer = _seal_writer(store)

    ready = writer.publish_ready()

    assert ready is not None
    assert any(path.is_dir() and (path / "manifest.json").exists() for path in (tmp_path / "ready").iterdir())
    writer.close()
    authority.close()


def test_authorized_clock_child_task_publishes_after_writer_releases(tmp_path: Path) -> None:
    store = _store(tmp_path)
    authority = store.create_publication_authority()
    writer = _seal_writer(store)
    writer.close()

    async def publish_from_child_task() -> object | None:
        async def publish() -> object | None:
            return authority.publish()

        return await asyncio.create_task(publish())

    assert asyncio.run(publish_from_child_task()) is not None
    assert any(path.is_dir() and (path / "manifest.json").exists() for path in (tmp_path / "ready").iterdir())
    authority.close()


def test_clock_mutation_lease_reuses_active_writer_storage_lease(tmp_path: Path) -> None:
    store = _store(tmp_path)

    with (
        store.device_lock(recover_capture_temporaries=False) as writer_lease,
        store.clock_mutation_lease() as clock_lease,
    ):
        assert clock_lease is writer_lease


def test_failed_sealed_publication_retains_capture_for_authorized_retry(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = _store(tmp_path)
    authority = store.create_publication_authority()
    writer = _seal_writer(store)
    original = store._recover_and_publish_unlocked

    def fail_publication() -> object:
        raise OSError("source unavailable")

    monkeypatch.setattr(store, "_recover_and_publish_unlocked", fail_publication)
    with pytest.raises(OSError, match="source unavailable"):
        writer.publish_ready()

    assert tuple(store.capture_root.iterdir())
    assert not tuple((tmp_path / "ready").glob("*/manifest.json"))
    monkeypatch.setattr(store, "_recover_and_publish_unlocked", original)

    assert authority.publish() is not None
    assert any(path.is_dir() and (path / "manifest.json").exists() for path in (tmp_path / "ready").iterdir())
    writer.close()
    authority.close()


def test_writer_publication_failure_schedules_local_retry(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    store = _store(tmp_path)
    retries: list[str] = []
    authority = store.create_publication_authority(lambda: retries.append("scheduled"))
    writer = _StagingWriterAdapter(store.make_staging_writer(100, 1), store.notify_publication_failure, 100)
    writer.prepare()
    writer.prepare_leg(100, 1)
    writer.read_begin(ReadBeginNotification(100, 1))
    writer.append_chunk(0, memoryview(_record(100)))

    def fail_publication() -> object:
        raise OSError("source unavailable")

    monkeypatch.setattr(store, "_recover_and_publish_unlocked", fail_publication)
    writer.seal(DoneNotification(0, 101))

    assert retries == ["scheduled"]
    assert tuple(store.capture_root.iterdir())
    writer.close()
    authority.close()


def test_publication_authority_rejects_duplicate_transfer_and_use_after_release(tmp_path: Path) -> None:
    store = _store(tmp_path)
    authority = store.create_publication_authority()

    with pytest.raises(AttemptStateError, match="already active"):
        store.create_publication_authority()
    with store.device_lock(recover_capture_temporaries=False) as lease:
        store.transfer_publication_authority(lease)
        with pytest.raises(AttemptStateError, match="already transferred"):
            store.transfer_publication_authority(lease)

    authority.close()

    with pytest.raises(AttemptStateError, match="revoked"):
        authority.publish()
    with pytest.raises(AttemptStateError, match="revoked"):
        authority.close()


def test_publication_authority_rejects_concurrent_projection(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    store = _store(tmp_path)
    authority = store.create_publication_authority()
    writer = _seal_writer(store)
    writer.close()
    entered = Event()
    release = Event()
    errors: list[BaseException] = []

    def blocked_publication() -> object:
        entered.set()
        assert release.wait(timeout=1)
        return object()

    monkeypatch.setattr(store, "_recover_and_publish_unlocked", blocked_publication)

    def publish() -> None:
        try:
            authority.publish()
        except BaseException as error:  # noqa: BLE001 - test records the thread boundary
            errors.append(error)

    thread = Thread(target=publish)
    thread.start()
    assert entered.wait(timeout=1)
    with pytest.raises(AttemptStateError, match="already active"):
        authority.publish()
    release.set()
    thread.join(timeout=1)

    assert not thread.is_alive()
    assert errors == []
    authority.close()
