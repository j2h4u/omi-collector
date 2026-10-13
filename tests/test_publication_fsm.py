"""Real store and async driver checks for publication admission and raw I/O."""

from __future__ import annotations

import asyncio
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from struct import pack
from threading import Event

import pytest

from omi_collector.capture.adapters.internal_audio_store import AUDIO_RECORD_SIZE
from omi_collector.capture.adapters.opportunistic_runtime import OpportunisticRuntime
from omi_collector.capture.adapters.ready_bundles import ReadyOutcome, ReadyOutcomeState
from omi_collector.capture.adapters.staging_contract import DeviceAlreadyRunningError
from omi_collector.capture.adapters.staging_filesystem import DeviceLock
from omi_collector.capture.application.quarantine_maintenance import QuarantineMaintenance
from omi_collector.capture.domain.ring_protocol import DoneNotification, ReadBeginNotification
from omi_collector.config import CollectorConfig, ReadyConfig, RetryConfig
from staging_test_factory import StagingStore


def _store(tmp_path: Path, *, threshold: float = 0.02, backoff: float = 0.05) -> StagingStore:
    draft = tmp_path / "draft"
    draft.mkdir()
    bootstrap = StagingStore(tmp_path / "spool", draft)
    ready = tmp_path / "ready"
    ready.mkdir(mode=0o2750)
    ready.chmod(0o2750)
    config = CollectorConfig(
        ready=ReadyConfig(target_audio_seconds=threshold), retry=RetryConfig(rapid_backoff=(backoff,))
    )
    return StagingStore.from_paths(bootstrap.paths, publication_root=ready, config=config)


def _record(sequence: int) -> bytes:
    return pack(">I", sequence) + bytes((2, 8, sequence % 256)) + bytes(AUDIO_RECORD_SIZE - 7)


def _add_source(store: StagingStore, start: int, count: int = 1) -> None:
    attempt = store.prepare_streaming_attempt(start, count)
    attempt.prepare_leg(start, count)
    attempt.record_read_begin(ReadBeginNotification(start, count))
    attempt.accept_chunk(start, b"".join(_record(sequence) for sequence in range(start, start + count)))
    attempt.seal(DoneNotification(0, start + count))
    attempt.close()


def test_transient_deadline_is_one_timer_and_no_immediate_retry(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = _store(tmp_path, threshold=0.04, backoff=0.08)
    _add_source(store, 100)
    store.append_ready_closure(101, "drained")
    original_lock = store.device_lock
    attempts = 0

    @contextmanager
    def busy_once(*, operation: str = "unknown") -> Iterator[DeviceLock]:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise DeviceAlreadyRunningError
        with original_lock(operation=operation) as lease:
            yield lease

    monkeypatch.setattr(store, "device_lock", busy_once)
    first = store.recover_and_publish()
    assert first.state is ReadyOutcomeState.TRANSIENT
    scheduled = store.publication_retry_schedule()
    assert scheduled is not None
    for _ in range(10):
        assert store.recover_and_publish().state is ReadyOutcomeState.TRANSIENT
    assert attempts == 1
    assert store.publication_retry_schedule() == scheduled

    maintenance = QuarantineMaintenance(store, None, OpportunisticRuntime())

    async def scenario() -> None:
        await maintenance.ensure_publication_ready()
        assert attempts == 1
        await asyncio.sleep(0.12)
        assert attempts == 2
        assert not tuple((tmp_path / "ready").iterdir())
        await maintenance.close()

    asyncio.run(scenario())


def test_capture_begin_fences_foreground_publication_before_durable_visit(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = _store(tmp_path)
    _add_source(store, 100)
    store.append_ready_closure(101, "drained")
    maintenance = QuarantineMaintenance(store, None, OpportunisticRuntime())
    entered = Event()
    release = Event()

    def slow_effect() -> ReadyOutcome:
        entered.set()
        assert release.wait(2)
        return ReadyOutcome(ReadyOutcomeState.WAITING, reason="test_effect")

    monkeypatch.setattr(store, "_recover_and_publish_unlocked", slow_effect)

    async def scenario() -> None:
        publication = asyncio.create_task(maintenance.ensure_publication_ready())
        assert await asyncio.to_thread(entered.wait, 2)
        capture = asyncio.create_task(maintenance.enter_capture_priority())
        await asyncio.sleep(0)
        assert store.recover_and_publish().reason == "capture_active"
        assert not capture.done()
        release.set()
        await capture
        assert store.recover_and_publish().reason == "capture_active"
        maintenance.exit_capture_priority()
        await maintenance.close()
        await asyncio.gather(publication, return_exceptions=True)

    try:
        asyncio.run(scenario())
    finally:
        release.set()
