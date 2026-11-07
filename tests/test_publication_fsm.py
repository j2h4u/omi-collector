"""Real store and async driver checks for publication admission and raw I/O."""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import replace
from pathlib import Path
from struct import pack
from threading import Event
from time import monotonic
from types import SimpleNamespace

import pytest

from omi_collector.capture.adapters import ready_bundles
from omi_collector.capture.adapters import staging_store as staging_store_module
from omi_collector.capture.adapters.internal_audio_store import AUDIO_RECORD_SIZE, CaptureRecord, InternalAudioStore
from omi_collector.capture.adapters.opportunistic_runtime import OpportunisticRuntime
from omi_collector.capture.adapters.ready_bundles import ReadyOutcome, ReadyOutcomeState
from omi_collector.capture.adapters.staging_contract import DeviceAlreadyRunningError
from omi_collector.capture.adapters.staging_filesystem import DeviceLock
from omi_collector.capture.application.quarantine_maintenance import QuarantineMaintenance
from omi_collector.capture.domain.ready_machine import PublicationCommand, PublicationEvent
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


def test_space_only_recovery_probes_without_rereading_payload(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    store = _store(tmp_path)
    _add_source(store, 100)
    store.append_ready_closure(101, "drained")
    now = [100.0]
    free = [0]
    reads = [0]
    original_read = InternalAudioStore.read_record_page
    monkeypatch.setattr(staging_store_module, "monotonic", lambda: now[0])

    def tracked(
        audio: InternalAudioStore, source_id: str, after_ordinal: int = -1, limit: int = 128
    ) -> tuple[CaptureRecord, ...]:
        reads[0] += 1
        return original_read(audio, source_id, after_ordinal, limit)

    def reserve(path: Path, required: int) -> None:
        if free[0] < required:
            raise ready_bundles.ReadySpaceError(path, required)

    monkeypatch.setattr(InternalAudioStore, "read_record_page", tracked)
    monkeypatch.setattr(ready_bundles, "_require_typed_filesystem_space", reserve)
    monkeypatch.setattr(staging_store_module.os, "statvfs", lambda _path: SimpleNamespace(f_bavail=free[0], f_frsize=1))
    assert store.recover_and_publish().state is ReadyOutcomeState.TRANSIENT
    initial_reads = reads[0]
    assert initial_reads > 0
    schedule = store.publication_retry_schedule()
    assert schedule is not None
    now[0] = schedule[1]
    assert store.publication_timer_fired(*schedule)
    assert store.recover_and_publish().state is ReadyOutcomeState.TRANSIENT
    assert reads[0] == initial_reads
    schedule = store.publication_retry_schedule()
    assert schedule is not None
    free[0] = 1 << 50
    now[0] = schedule[1]
    assert store.publication_timer_fired(*schedule)
    assert store.recover_and_publish().state is ReadyOutcomeState.PUBLISHED


def test_partial_stage_failures_keep_backoff(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    store = _store(tmp_path)
    store._config = replace(store._config, retry=RetryConfig(rapid_backoff=(1.0, 5.0)))
    _add_source(store, 100)
    store.append_ready_closure(101, "drained")
    now = [100.0]
    monkeypatch.setattr(staging_store_module, "monotonic", lambda: now[0])
    events: list[tuple[object, object]] = []
    event = store._publication_event

    def bounded_event(value: PublicationEvent) -> PublicationCommand:
        result = event(value)
        events.append((value, result))
        assert len(events) < 25, events
        return result

    monkeypatch.setattr(store, "_publication_event", bounded_event)

    def fail(stage: ready_bundles._V2Stage, _page_limit: int) -> ready_bundles.ReadyBundleResult:
        stage.path.mkdir(exist_ok=True)
        (stage.path / "partial").write_text(str(now[0]))
        raise OSError("partial write")

    monkeypatch.setattr(ready_bundles, "_materialize_v2_stage", fail)
    assert store.recover_and_publish().state is ReadyOutcomeState.TRANSIENT
    schedule = store.publication_retry_schedule()
    assert schedule is not None and schedule[1] == 101.0
    now[0] = schedule[1]
    assert store.publication_timer_fired(*schedule)
    assert store.recover_and_publish().state is ReadyOutcomeState.TRANSIENT
    schedule = store.publication_retry_schedule()
    assert schedule is not None and schedule[1] == 106.0


def test_changed_input_during_space_failure_does_not_park_new_work(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = _store(tmp_path)
    calls = 0

    def run() -> ReadyOutcome:
        nonlocal calls
        calls += 1
        if calls == 1:
            store.publication_input_changed()
            raise ready_bundles.ReadySpaceError(tmp_path, 1 << 60)
        return ReadyOutcome(ReadyOutcomeState.WAITING, reason="changed")

    monkeypatch.setattr(store, "_recover_and_publish_unlocked", run)
    assert store.recover_and_publish().reason == "changed"
    assert calls == 2
    assert store._publication_space is None


def _add_source(store: StagingStore, start: int, count: int = 1) -> None:
    attempt = store.prepare_streaming_attempt(start, count)
    attempt.prepare_leg(start, count)
    attempt.record_read_begin(ReadBeginNotification(start, count))
    attempt.accept_chunk(start, b"".join(_record(sequence) for sequence in range(start, start + count)))
    attempt.seal(DoneNotification(0, start + count))
    attempt.close()


def _hold_loop_timers(
    loop: asyncio.AbstractEventLoop, monkeypatch: pytest.MonkeyPatch
) -> tuple[list[tuple[float, Callable[..., None], tuple[object, ...]]], list[asyncio.TimerHandle]]:
    call_at = loop.call_at
    timers: list[tuple[float, Callable[..., None], tuple[object, ...]]] = []
    handles: list[asyncio.TimerHandle] = []

    def hold_timer(when: float, callback: Callable[..., None], *args: object) -> asyncio.TimerHandle:
        timers.append((when, callback, args))
        handle = call_at(loop.time() + 3600, lambda: None)
        handles.append(handle)
        return handle

    monkeypatch.setattr(loop, "call_at", hold_timer)
    return timers, handles


def test_transient_deadline_is_one_timer_and_no_immediate_retry(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = _store(tmp_path, threshold=0.04, backoff=0.08)
    _add_source(store, 100)
    store.append_ready_closure(101, "drained")
    now = [monotonic()]
    monkeypatch.setattr(staging_store_module, "monotonic", lambda: now[0])
    original_lock = store.device_lock
    attempts = 0
    retry_finished = Event()

    @contextmanager
    def busy_once(*, operation: str = "unknown") -> Iterator[DeviceLock]:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise DeviceAlreadyRunningError
        try:
            with original_lock(operation=operation) as lease:
                yield lease
        finally:
            retry_finished.set()

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
        loop = asyncio.get_running_loop()
        timers, held_handles = _hold_loop_timers(loop, monkeypatch)
        await maintenance.ensure_publication_ready()
        assert attempts == 1
        assert len(timers) == 1
        deadline, fire_timer, timer_args = timers[0]
        assert deadline == scheduled[1]
        assert deadline > now[0]
        now[0] = deadline
        fire_timer(*timer_args)
        assert await asyncio.to_thread(retry_finished.wait, 5)
        await maintenance.close()
        for handle in held_handles:
            handle.cancel()
        assert attempts == 2
        assert not tuple((tmp_path / "ready").iterdir())

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
        assert (await asyncio.to_thread(store.recover_and_publish)).reason == "capture_active"
        assert not capture.done()
        release.set()
        await capture
        assert (await asyncio.to_thread(store.recover_and_publish)).reason == "capture_active"
        await maintenance.exit_capture_priority()
        await maintenance.close()
        await asyncio.gather(publication, return_exceptions=True)

    try:
        asyncio.run(scenario())
    finally:
        release.set()
