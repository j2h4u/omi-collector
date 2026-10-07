"""Adversarial same-path, retry-deadline, and shutdown publication races."""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from hashlib import sha256
from json import dumps
from pathlib import Path
from threading import Event
from typing import cast

import pytest

from omi_collector.capture.adapters import staging_store as staging_store_module
from omi_collector.capture.adapters.bundle_contract import BundleManifest, SealedReceipt
from omi_collector.capture.adapters.opportunistic_runtime import OpportunisticRuntime
from omi_collector.capture.adapters.ready_bundles import ConflictingOverlapError, ReadyOutcome, ReadyOutcomeState
from omi_collector.capture.adapters.staging_contract import DeviceAlreadyRunningError
from omi_collector.capture.adapters.staging_filesystem import DeviceLock
from omi_collector.capture.adapters.staging_store import StagingStore
from omi_collector.capture.application.quarantine_maintenance import QuarantineMaintenance
from omi_collector.capture.domain.ring_protocol import RECORD_SIZE
from test_publication_fsm import _count_raw_reads, _draft, _store


def _conflicting_overlap(store: StagingStore) -> Path:
    _draft(store, "first", 100, 2)
    overlap = _draft(store, "overlap", 101)
    raw = bytearray((overlap / "records.bin").read_bytes())
    raw[-1] ^= 1
    digest = sha256(raw).hexdigest()
    (overlap / "records.bin").write_bytes(raw)
    (overlap / "manifest.json").write_text(
        dumps(BundleManifest(2, 101, 102, 1, RECORD_SIZE, digest).as_dict()), encoding="utf-8"
    )
    (overlap / "receipt.json").write_text(dumps(SealedReceipt("a" * 32, digest).as_dict()), encoding="utf-8")
    return overlap


def _replace_draft_files(store: StagingStore, path: Path, start: int, count: int) -> int:
    replacement = _draft(store, "replacement", start, count)
    previous_inode = (path / "records.bin").stat().st_ino
    for name in ("records.bin", "manifest.json", "receipt.json"):
        (replacement / name).replace(path / name)
    replacement.rmdir()
    return previous_inode


def _patch_finalize_to_resolve_overlap(
    monkeypatch: pytest.MonkeyPatch, store: StagingStore, overlap: Path
) -> tuple[list[str], list[int]]:
    original = cast(Callable[..., ReadyOutcome], staging_store_module.finalize_drafts)
    errors: list[str] = []
    calls = [0]

    def resolve_after_first_attempt(*args: object, **kwargs: object) -> ReadyOutcome:
        calls[0] += 1
        try:
            return original(*args, **kwargs)
        except ConflictingOverlapError as error:
            errors.append(str(error))
            if calls[0] == 1:
                _replace_draft_files(store, overlap, 102, 1)
            raise

    monkeypatch.setattr(staging_store_module, "finalize_drafts", resolve_after_first_attempt)
    return errors, calls


def _patch_finalize_to_pause(monkeypatch: pytest.MonkeyPatch, entered: Event, release: Event) -> list[int]:
    original = cast(Callable[..., ReadyOutcome], staging_store_module.finalize_drafts)
    calls = [0]

    def pause_after_real_finalization(*args: object, **kwargs: object) -> ReadyOutcome:
        outcome = original(*args, **kwargs)
        calls[0] += 1
        entered.set()
        if not release.wait(5):
            raise AssertionError("test did not release the in-flight publication")
        return outcome

    monkeypatch.setattr(staging_store_module, "finalize_drafts", pause_after_real_finalization)
    return calls


def test_replaced_same_path_draft_invalidates_blocked_revision(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    store = _store(tmp_path)
    overlap = _conflicting_overlap(store)
    store.append_ready_closure(103, "drained")
    counts = _count_raw_reads(monkeypatch)

    blocked = store.recover_and_publish()
    assert blocked.state is ReadyOutcomeState.BLOCKED
    assert blocked.reason is not None
    first_counts = tuple(counts)
    assert first_counts[0] > 0 and first_counts[1] > 0

    old_inode = _replace_draft_files(store, overlap, 102, 1)
    assert (overlap / "records.bin").stat().st_ino != old_inode
    assert overlap == store.capture_root / "overlap"

    published = store.recover_and_publish()
    assert published.state is ReadyOutcomeState.PUBLISHED
    assert len(published.published) == 1
    assert published.published[0].record_count == 3
    assert published.published[0].next_sequence == 103
    after_publish = tuple(counts)
    assert after_publish[1] > first_counts[1]

    assert store.recover_and_publish().state is ReadyOutcomeState.WAITING
    assert tuple(counts) == after_publish


def test_inflight_source_change_cannot_settle_the_old_revision(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    store = _store(tmp_path)
    overlap = _conflicting_overlap(store)
    store.append_ready_closure(103, "drained")
    counts = _count_raw_reads(monkeypatch)
    errors, calls = _patch_finalize_to_resolve_overlap(monkeypatch, store, overlap)

    outcome = store.recover_and_publish()

    assert calls == [2]
    assert errors == ["authenticated draft overlap has conflicting record bytes"]
    assert outcome.state is ReadyOutcomeState.PUBLISHED
    assert len(outcome.published) == 1
    assert outcome.published[0].record_count == 3
    assert outcome.published[0].next_sequence == 103
    assert counts[0] > 0 and counts[1] > 0
    assert len(tuple((tmp_path / "ready").iterdir())) == 1


def test_repeated_retry_wakes_keep_the_absolute_deadline_and_one_effect(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    backoff = 30.0
    store = _store(tmp_path, threshold=0.04, backoff=backoff)
    _draft(store, "only", 100)
    store.append_ready_closure(101, "drained")
    counts = _count_raw_reads(monkeypatch)
    original_lock = store.device_lock
    attempts = [0]

    @contextmanager
    def busy_once(*, recover_capture_temporaries: bool = True, operation: str = "unknown") -> Iterator[DeviceLock]:
        attempts[0] += 1
        if attempts[0] == 1:
            raise DeviceAlreadyRunningError
        with original_lock(recover_capture_temporaries=recover_capture_temporaries, operation=operation) as lease:
            yield lease

    monkeypatch.setattr(store, "device_lock", busy_once)
    clock = [0.0]
    monkeypatch.setattr(staging_store_module, "monotonic", lambda: clock[0])
    maintenance = QuarantineMaintenance(store, None, OpportunisticRuntime())

    asyncio.run(
        _exercise_retry_deadline(
            _RetryScenario(store, maintenance, counts, clock, backoff, attempts, tmp_path / "ready")
        )
    )


@dataclass
class _RetryScenario:
    store: StagingStore
    maintenance: QuarantineMaintenance
    counts: list[int]
    clock: list[float]
    backoff: float
    attempts: list[int]
    ready_root: Path


async def _exercise_retry_deadline(scenario: _RetryScenario) -> None:
    store, maintenance = scenario.store, scenario.maintenance
    counts, clock = scenario.counts, scenario.clock
    backoff, attempts, ready_root = scenario.backoff, scenario.attempts, scenario.ready_root
    clock[0] = asyncio.get_running_loop().time()
    assert store.recover_and_publish().state is ReadyOutcomeState.TRANSIENT
    schedule = store.publication_retry_schedule()
    assert schedule is not None
    generation, deadline = schedule
    assert deadline == clock[0] + backoff
    assert attempts[0] == 1
    await maintenance.ensure_publication_ready()
    for _ in range(4):
        maintenance.schedule_publication_retry()
        retry_task = maintenance._publication_retry_task
        assert retry_task is not None
        await retry_task
        await asyncio.sleep(0)
        await maintenance.ensure_publication_ready()
        handle = maintenance._publication_retry_handle
        assert store.publication_retry_schedule() == schedule
        assert handle is not None and handle.when() == deadline
        assert attempts[0] == 1 and tuple(counts) == (0, 0)
    clock[0] = deadline
    assert store.publication_timer_fired(generation, deadline)
    maintenance.schedule_publication_retry()
    retry_task = maintenance._publication_retry_task
    assert retry_task is not None
    await retry_task
    await asyncio.sleep(0)
    assert store.recover_and_publish().state is ReadyOutcomeState.WAITING
    await maintenance.close()
    assert attempts[0] == 2 and counts[0] > 0 and counts[1] > 0
    assert not tuple(ready_root.iterdir())
    final_counts = tuple(counts)
    assert not store.publication_timer_fired(generation, deadline)
    maintenance.schedule_publication_retry()
    for _ in range(3):
        assert store.recover_and_publish().state is ReadyOutcomeState.WAITING
    assert attempts[0] == 2 and tuple(counts) == final_counts


def test_shutdown_joins_owned_publication_and_ignores_late_completion(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = _store(tmp_path)
    _draft(store, "only", 100)
    store.append_ready_closure(101, "drained")
    counts = _count_raw_reads(monkeypatch)
    entered = Event()
    release = Event()
    calls = _patch_finalize_to_pause(monkeypatch, entered, release)
    maintenance = QuarantineMaintenance(store, None, OpportunisticRuntime())

    asyncio.run(
        _exercise_shutdown(_ShutdownScenario(store, maintenance, counts, calls, entered, release, tmp_path / "ready"))
    )


@dataclass
class _ShutdownScenario:
    store: StagingStore
    maintenance: QuarantineMaintenance
    counts: list[int]
    calls: list[int]
    entered: Event
    release: Event
    ready_root: Path


async def _exercise_shutdown(scenario: _ShutdownScenario) -> None:
    store, maintenance = scenario.store, scenario.maintenance
    counts, calls = scenario.counts, scenario.calls
    entered, release, ready_root = scenario.entered, scenario.release, scenario.ready_root
    maintenance.schedule_publication_retry()
    background = maintenance._publication_retry_task
    assert background is not None
    try:
        assert await asyncio.to_thread(entered.wait, 2)
        reads_at_shutdown = tuple(counts)
        assert calls == [1]
        closing = asyncio.create_task(maintenance.close())
        await asyncio.sleep(0)
        assert not store.publication_wake_admitted()
        for _ in range(2):
            closing.cancel()
            await asyncio.sleep(0)
            assert not closing.done()
        assert store.recover_and_publish().reason == "publication_closed"
        assert not store.publication_timer_fired(0, 0.0)
        maintenance.schedule_publication_retry()
        assert tuple(counts) == reads_at_shutdown and calls == [1]
        release.set()
        closed = await asyncio.gather(closing, return_exceptions=True)
        assert isinstance(closed[0], asyncio.CancelledError)
        await asyncio.gather(background, return_exceptions=True)
        assert maintenance._publication_retry_task is None and not maintenance._publication_effect_tasks
        assert maintenance._publication_retry_handle is None and calls == [1]
        assert len(tuple(ready_root.iterdir())) == 1
        closed_counts = tuple(counts)
        assert closed_counts[0] >= reads_at_shutdown[0] and closed_counts[1] >= reads_at_shutdown[1]
        assert store.recover_and_publish().reason == "publication_closed"
        assert not store.publication_timer_fired(0, 0.0)
        maintenance.schedule_publication_retry()
        assert tuple(counts) == closed_counts
    finally:
        release.set()
        if not background.done():
            background.cancel()
        await asyncio.gather(background, return_exceptions=True)
        await maintenance.close()
