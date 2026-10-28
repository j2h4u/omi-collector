from __future__ import annotations

import asyncio
import logging
import sqlite3
import threading
import time
from collections.abc import Callable
from contextlib import closing
from contextvars import Context
from pathlib import Path
from struct import pack
from typing import cast

import pytest

from omi_collector.capture.adapters import quarantine
from omi_collector.capture.adapters import staging_store as staging_store_module
from omi_collector.capture.adapters.collector_state_db import _GuardedConnection
from omi_collector.capture.adapters.opportunistic_runtime import OpportunisticRuntime
from omi_collector.capture.adapters.quarantine import QuarantineSalvageDeferredError, SweepResult
from omi_collector.capture.adapters.ready_bundles import ReadyBundleError, ReadyOutcome, ReadyOutcomeState
from omi_collector.capture.adapters.staging_contract import FatalInternalAudioStorageError
from omi_collector.capture.application.ports import StagingPort
from omi_collector.capture.application.presence import PresencePolicy, PresenceWake
from omi_collector.capture.application.quarantine_maintenance import (
    PendingStartupState,
    QuarantineMaintenance,
    _cleanup_transition,
    _CleanupEvent,
    _CleanupPhase,
    _CleanupState,
)
from omi_collector.capture.application.storage_fault import TransientInternalAudioStorageError
from omi_collector.capture.domain.ring_protocol import RECORD_SIZE, ReadBeginNotification
from omi_collector.config import CollectorConfig, RetryConfig
from staging_test_factory import StagingStore, attempt_source, closure_rows


def _run(coro: object) -> object:
    return asyncio.run(coro)  # type: ignore[arg-type]


def _store(tmp_path: Path) -> StagingStore:
    return StagingStore(tmp_path, tmp_path.parent / f"{tmp_path.name}-captures")


def _publication_store(tmp_path: Path, *, config: CollectorConfig | None = None) -> StagingStore:
    draft = tmp_path / "drafts"
    draft.mkdir(parents=True)
    bootstrap = StagingStore(tmp_path / "spool", draft, config=config or CollectorConfig())
    ready = tmp_path / "ready"
    ready.mkdir(mode=0o2750)
    ready.chmod(0o2750)
    return StagingStore.from_paths(bootstrap.paths, publication_root=ready, config=config or CollectorConfig())


def _record(value: int) -> bytes:
    return pack(">I", value) + bytes((value % 256,)) * (RECORD_SIZE - 4)


def _seed_streaming_partial(store: StagingStore, count: int, *, start_sequence: int = 100) -> bytes:
    attempt = store.prepare_streaming_attempt(start_sequence, count)
    attempt.prepare_leg(start_sequence, count)
    records = b"".join(_record(sequence) for sequence in range(start_sequence, start_sequence + count))
    attempt.record_read_begin(ReadBeginNotification(start_sequence, count))
    for index in range(count):
        attempt.accept_chunk(start_sequence + index, records[index * RECORD_SIZE : (index + 1) * RECORD_SIZE])
    attempt.checkpoint()
    attempt.close(durable=True)
    return records


def _quarantine_sources(store: StagingStore, counts: tuple[int, ...]) -> tuple[str, ...]:
    start = 100
    for count in counts:
        _seed_streaming_partial(store, count, start_sequence=start)
        start += count + 1
    for descriptor in store.pending_attempts():
        store.quarantine_attempt_source(descriptor.attempt_id)
    return store.quarantined_attempts()


def _corrupt_quarantined_digest(store: StagingStore, source_id: str) -> None:
    with closing(cast(sqlite3.Connection, store.state_db.open_connection())) as connection:
        connection.execute("UPDATE capture_source SET committed_sha256 = ? WHERE source_id = ?", ("0" * 64, source_id))
        connection.commit()


def _source_snapshot(store: StagingStore, source_id: str) -> dict[str, object] | None:
    if attempt_source(store, source_id) is None:
        return None
    with closing(cast(sqlite3.Connection, store.state_db.open_connection(read_only=True))) as connection:
        source = cast(
            tuple[object, ...] | None,
            connection.execute(
                "SELECT source_type, lifecycle, block_reason, quarantine_disposition, certified_prefix_count, "
                "coverage_digest, committed_count, committed_sha256 FROM capture_source WHERE source_id = ?",
                (source_id,),
            ).fetchone(),
        )
        records = cast(
            list[tuple[object, ...]],
            connection.execute(
                "SELECT device_timestamp, payload FROM capture_record WHERE source_id = ? ORDER BY ordinal",
                (source_id,),
            ).fetchall(),
        )
        tail = cast(
            tuple[object, ...] | None,
            connection.execute("SELECT bytes FROM capture_tail WHERE source_id = ?", (source_id,)).fetchone(),
        )
        conflicts = cast(
            list[tuple[object, ...]],
            connection.execute(
                "SELECT chunk_ordinal, reason, bytes FROM capture_conflict WHERE source_id = ? ORDER BY chunk_ordinal",
                (source_id,),
            ).fetchall(),
        )
    assert source is not None
    return {
        "source": source,
        "records": tuple(records),
        "tail": tail,
        "conflicts": tuple(conflicts),
    }


def _snapshot_raw_records(snapshot: dict[str, object]) -> bytes:
    records = cast(tuple[tuple[object, object], ...], snapshot["records"])
    return b"".join(cast(int, row[0]).to_bytes(4, "big") + cast(bytes, row[1]) for row in records)


def _terminal_state(store: StagingStore, source_id: str) -> str | None:
    snapshot = _source_snapshot(store, source_id)
    if snapshot is None:
        return None
    disposition = cast(str | None, cast(tuple[object, ...], snapshot["source"])[3])
    return {"published_prefix": "published", "unprocessable": "unprocessable"}.get(disposition) if disposition else None


class _BlockedRecovery:
    def __init__(self, recover: Callable[[], ReadyOutcome]) -> None:
        self.recover = recover
        self.started = threading.Event()
        self.release = threading.Event()
        self.finished = threading.Event()
        self.calls = 0
        self.active = 0
        self.maximum_active = 0

    def __call__(self) -> ReadyOutcome:
        self.calls += 1
        self.active += 1
        self.maximum_active = max(self.maximum_active, self.active)
        try:
            if self.calls == 1:
                self.started.set()
                if not self.release.wait(2):
                    raise TimeoutError("publication mutation was not released")
                self.finished.set()
            return self.recover()
        finally:
            self.active -= 1


def test_pending_startup_result_including_no_pending_is_memoized(tmp_path: Path, monkeypatch: object) -> None:
    store = _store(tmp_path)
    calls = 0
    original = store.pending_attempts

    def pending() -> tuple[object, ...]:
        nonlocal calls
        calls += 1
        return original()

    monkeypatch.setattr(store, "pending_attempts", pending)  # type: ignore[attr-defined]
    maintenance = QuarantineMaintenance(store, None, OpportunisticRuntime())

    first = _run(maintenance.prepare_pending_startup())
    second = _run(maintenance.prepare_pending_startup())

    assert isinstance(first, PendingStartupState)
    assert first is second
    assert first.pending is None
    assert first.durable_next is None
    assert calls == 1


def test_background_publication_fatal_stops_retry_and_reaches_presence_owner(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def scenario() -> None:
        store = _publication_store(tmp_path)
        calls = 0

        def fatal_publication() -> ReadyOutcome:
            nonlocal calls
            calls += 1
            raise FatalInternalAudioStorageError("READY latch failed")

        monkeypatch.setattr(store, "_recover_and_publish_unlocked", fatal_publication)
        maintenance = QuarantineMaintenance(store, None, OpportunisticRuntime())

        class Presence:
            async def wait_for_attempt(self) -> PresenceWake:
                await asyncio.Event().wait()
                raise AssertionError("unreachable")

            async def close(self) -> None:
                return None

        try:
            maintenance.schedule_publication_retry()
            await asyncio.wait_for(maintenance._publication_fatal_event.wait(), timeout=2)
            with pytest.raises(FatalInternalAudioStorageError, match="READY latch failed"):
                maintenance.schedule_publication_retry()
            with pytest.raises(FatalInternalAudioStorageError, match="READY latch failed"):
                await asyncio.wait_for(maintenance.wait_for_presence_attempt(Presence(), lambda _: None), timeout=2)
            assert calls == 1
        finally:
            await maintenance.close()

    _run(scenario())


def test_auxiliary_writer_fatal_wakes_idle_presence_owner(tmp_path: Path) -> None:
    async def scenario() -> None:
        maintenance = QuarantineMaintenance(_store(tmp_path), None, OpportunisticRuntime())
        fatal = FatalInternalAudioStorageError("canonical audio database is corrupt")

        async def wait_for_fatal(*_args: object) -> None:
            await maintenance._publication_fatal_event.wait()

        maintenance._prepare_and_run = wait_for_fatal  # type: ignore[method-assign]

        class Presence:
            closed = False

            async def wait_for_attempt(self) -> PresenceWake:
                await asyncio.Event().wait()
                raise AssertionError("unreachable")

            async def close(self) -> None:
                self.closed = True

        presence = Presence()
        owner = asyncio.create_task(maintenance.wait_for_presence_attempt(presence, lambda _: None))
        try:
            await asyncio.sleep(0)
            maintenance.record_storage_fatal(fatal)
            with pytest.raises(FatalInternalAudioStorageError, match="corrupt"):
                await asyncio.wait_for(owner, timeout=2)
        finally:
            owner.cancel()
            await asyncio.gather(owner, return_exceptions=True)
            await maintenance.close()

    _run(scenario())


def test_multiple_pending_partials_are_quarantined_without_losing_raw_records(tmp_path: Path) -> None:
    store = _store(tmp_path)
    expected_records = {
        _seed_streaming_partial(store, count=1),
        _seed_streaming_partial(store, count=2),
    }

    state = cast(
        PendingStartupState,
        _run(QuarantineMaintenance(store, None, OpportunisticRuntime()).prepare_pending_startup()),
    )

    assert state.pending is None
    assert state.durable_next is None
    assert state.disposition == "empty"
    assert store.pending_attempts() == ()
    quarantined_sources = store.quarantined_attempts()
    assert len(quarantined_sources) == 2
    snapshots = [_source_snapshot(store, source) for source in quarantined_sources]
    assert all(snapshot is not None for snapshot in snapshots)
    assert {
        _snapshot_raw_records(snapshot) for snapshot in cast(list[dict[str, object]], snapshots)
    } == expected_records


def test_two_pending_attempts_stay_blocked_under_public_promotion_contention(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = _store(tmp_path)
    expected_records = {
        _seed_streaming_partial(store, count=1),
        _seed_streaming_partial(store, count=2),
    }
    lock_held = threading.Event()
    release_lock = threading.Event()
    holder: threading.Thread | None = None
    original_pending = store.pending_attempts
    original_quarantine = store.quarantine_pending

    def hold_real_promotion_lock() -> None:
        with store.device_lock(operation="competing-promotion"):
            lock_held.set()
            release_lock.wait(5)

    def pending_while_real_lock_is_held() -> object:
        nonlocal holder
        descriptors = original_pending()
        holder = threading.Thread(target=hold_real_promotion_lock, name="test-promotion-lock")
        holder.start()
        if not lock_held.wait(1):
            release_lock.set()
            holder.join(1)
            raise TimeoutError("competing promotion lock was not acquired")
        return descriptors

    def release_lock_before_quarantine(reason: str) -> tuple[str, ...]:
        release_lock.set()
        if holder is not None:
            holder.join(1)
            if holder.is_alive():
                raise TimeoutError("competing promotion lock was not released")
        return original_quarantine(reason)

    monkeypatch.setattr(store, "pending_attempts", pending_while_real_lock_is_held)
    monkeypatch.setattr(store, "quarantine_pending", release_lock_before_quarantine)
    maintenance = QuarantineMaintenance(store, None, OpportunisticRuntime())
    try:
        state = cast(PendingStartupState, _run(maintenance.prepare_pending_startup()))
        assert state.pending is None
        assert state.durable_next is None
        assert state.disposition == "empty"
        assert store.pending_attempts() == ()
        quarantined_sources = store.quarantined_attempts()
        assert len(quarantined_sources) == 2
        snapshots = cast(list[dict[str, object]], [_source_snapshot(store, source) for source in quarantined_sources])
        assert {_snapshot_raw_records(snapshot) for snapshot in snapshots} == expected_records
    finally:
        release_lock.set()
        if holder is not None:
            holder.join(1)
            assert not holder.is_alive()
        _run(maintenance.close())
        if original_pending():
            original_quarantine("test cleanup")


def test_deferred_maintenance_is_retried_without_touching_quarantine(tmp_path: Path) -> None:
    store = _store(tmp_path)
    maintenance = QuarantineMaintenance(store, None, OpportunisticRuntime())
    calls: list[str] = []
    methods = (
        "recover_and_publish",
        "refresh_utc_projection",
        "quarantined_attempts",
    )
    originals = {name: cast(Callable[..., object], getattr(store, name)) for name in methods}
    for name, original in originals.items():

        def spy(*args: object, _name: str = name, _original: object = original, **kwargs: object) -> object:
            calls.append(_name)
            return _original(*args, **kwargs)  # type: ignore[operator]

        setattr(store, name, spy)

    _run(maintenance.run_once(lambda: True))
    assert calls == []
    _run(maintenance.run_once(lambda: False))
    assert calls == list(methods)


def test_maintenance_runs_again_at_exact_configured_monotonic_boundary(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = _store(tmp_path)
    config = CollectorConfig(retry=RetryConfig(maintenance_interval_seconds=10.0))
    maintenance = QuarantineMaintenance(store, None, OpportunisticRuntime(), config=config)
    now = 100.0
    monkeypatch.setattr("omi_collector.capture.application.quarantine_maintenance.monotonic", lambda: now)
    calls: list[str] = []
    methods = (
        "recover_and_publish",
        "refresh_utc_projection",
        "quarantined_attempts",
    )
    for name in methods:
        original = cast(Callable[..., object], getattr(store, name))

        def spy(*args: object, _name: str = name, _original: object = original, **kwargs: object) -> object:
            calls.append(_name)
            return _original(*args, **kwargs)  # type: ignore[operator]

        setattr(store, name, spy)

    _run(maintenance.run_once(lambda: False))
    assert calls == list(methods)
    _run(maintenance.run_once(lambda: False))
    assert calls == list(methods)
    now = 109.999
    _run(maintenance.run_once(lambda: False))
    assert calls == list(methods)
    now = 110.0
    _run(maintenance.run_once(lambda: False))
    assert calls == list(methods) * 2


def test_hourly_cleanup_orders_phases_and_ordinary_ticks_skip_physical_work(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = _store(tmp_path)
    maintenance = QuarantineMaintenance(store, None, OpportunisticRuntime())
    calls: list[str] = []

    def record(name: str, result: object) -> Callable[..., object]:
        def effect(*_args: object, **_kwargs: object) -> object:
            calls.append(name)
            return result

        return effect

    monkeypatch.setattr(store, "retire_acknowledged_ready", record("ack", 0))
    monkeypatch.setattr(store, "retire_covered_raw_page", record("raw", False))
    monkeypatch.setattr(store, "sweep_terminal_quarantine", record("expiry", SweepResult((), False)))
    monkeypatch.setattr(store, "compact_after_cleanup", record("vacuum", None))
    _run(maintenance.run_once(lambda: False))
    assert calls == []

    maintenance._maintenance_not_before = 0
    maintenance._cleanup_state = _CleanupState(_CleanupPhase.WAITING, 0)
    _run(maintenance.run_once(lambda: False))
    assert calls == ["ack", "raw", "expiry", "vacuum"]
    assert maintenance._cleanup_state.phase is _CleanupPhase.WAITING


def test_hourly_cleanup_yields_after_one_raw_page_on_capture_intent(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = _store(tmp_path)
    maintenance = QuarantineMaintenance(store, None, OpportunisticRuntime())
    capture_intent = threading.Event()
    calls: list[str] = []

    def raw_page(*, should_defer: Callable[[], bool]) -> bool:
        assert not should_defer()
        calls.append("raw")
        capture_intent.set()
        return True

    monkeypatch.setattr(store, "retire_acknowledged_ready", lambda **_kwargs: 0)
    monkeypatch.setattr(store, "retire_covered_raw_page", raw_page)
    monkeypatch.setattr(store, "sweep_terminal_quarantine", lambda **_kwargs: pytest.fail("expiry after capture"))
    maintenance._cleanup_state = _CleanupState(_CleanupPhase.WAITING, 0)
    _run(maintenance.run_once(capture_intent.is_set))
    assert calls == ["raw"]
    assert maintenance._cleanup_state.phase is _CleanupPhase.WAITING


@pytest.mark.parametrize("phase", tuple(_CleanupPhase))
@pytest.mark.parametrize("event", tuple(_CleanupEvent))
def test_hourly_cleanup_transition_covers_every_phase_event(phase: _CleanupPhase, event: _CleanupEvent) -> None:
    state = _CleanupState(phase, 100.0, 150.0)
    if phase is _CleanupPhase.WAITING and event is _CleanupEvent.ADVANCE:
        with pytest.raises(ValueError, match="cannot advance"):
            _cleanup_transition(state, event, 120.0)
        return
    result = _cleanup_transition(state, event, 120.0)
    if event is _CleanupEvent.WAKE:
        expected = _CleanupState(_CleanupPhase.PUBLICATION, 100.0, 720.0) if phase is _CleanupPhase.WAITING else state
    elif event is _CleanupEvent.DEFER:
        expected = state if phase is _CleanupPhase.WAITING else _CleanupState(_CleanupPhase.WAITING, 3700.0)
    else:
        next_phase = {
            _CleanupPhase.PUBLICATION: _CleanupPhase.ACK,
            _CleanupPhase.ACK: _CleanupPhase.RAW,
            _CleanupPhase.RAW: _CleanupPhase.EXPIRY,
            _CleanupPhase.EXPIRY: _CleanupPhase.VACUUM,
        }.get(phase)
        expected = (
            _CleanupState(next_phase, 100.0, 150.0) if next_phase else _CleanupState(_CleanupPhase.WAITING, 3700.0)
        )
    assert result == expected


def test_hourly_cleanup_wakes_at_due_and_rejects_unknown_state_or_event() -> None:
    waiting = _CleanupState(_CleanupPhase.WAITING, 100.0)
    assert _cleanup_transition(waiting, _CleanupEvent.WAKE, 99.999) == waiting
    assert _cleanup_transition(waiting, _CleanupEvent.WAKE, 100.0) == _CleanupState(
        _CleanupPhase.PUBLICATION, 100.0, 700.0
    )
    with pytest.raises(ValueError, match="unknown cleanup event"):
        _cleanup_transition(waiting, cast(_CleanupEvent, "invalid"), 100.0)
    with pytest.raises(ValueError, match="unknown cleanup phase"):
        _cleanup_transition(_CleanupState(cast(_CleanupPhase, "invalid"), 100.0), _CleanupEvent.WAKE, 100.0)


@pytest.mark.parametrize("event", (_CleanupEvent.DEFER, _CleanupEvent.ADVANCE))
def test_overdue_hourly_window_coalesces_to_next_future_epoch(event: _CleanupEvent) -> None:
    phase = _CleanupPhase.VACUUM if event is _CleanupEvent.ADVANCE else _CleanupPhase.RAW
    overdue = _CleanupState(phase, 100.0, 10_600.0)
    assert _cleanup_transition(overdue, event, 10_001.0) == _CleanupState(_CleanupPhase.WAITING, 10_900.0)


def test_unchanged_full_compaction_debt_parks_until_relevant_generation_changes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = _publication_store(tmp_path)
    store.recover_and_publish()
    generation = [5]
    available_space = [1_000_000]
    calls: list[int] = []
    monkeypatch.setattr(staging_store_module, "pending_compaction_generation", lambda _database: generation[0])
    monkeypatch.setattr(store, "_available_compaction_space", lambda: available_space[0])

    def full_vacuum(_database: object, debt_generation: int, _interrupt: object, _defer: object) -> str:
        calls.append(debt_generation)
        return "parked"

    monkeypatch.setattr(staging_store_module, "vacuum_pending", full_vacuum)
    first = store.compact_after_cleanup(should_defer=lambda: False)
    second = store.compact_after_cleanup(should_defer=lambda: False)
    assert first is not None and first.reason == "compaction_parked"
    assert second is not None and second.reason == "compaction_parked"
    assert calls == [5]
    required_release = max(store.state_db.path.stat().st_size, 1)
    available_space[0] += required_release - 1
    still_parked = store.compact_after_cleanup(should_defer=lambda: False)
    assert still_parked is not None and still_parked.reason == "compaction_parked"
    assert calls == [5]
    available_space[0] += 1
    released = store.compact_after_cleanup(should_defer=lambda: False)
    assert released is not None and released.reason == "compaction_parked"
    assert calls == [5, 5]
    generation[0] = 6
    changed = store.compact_after_cleanup(should_defer=lambda: False)
    assert changed is not None and changed.reason == "compaction_parked"
    assert calls == [5, 5, 6]


def test_busy_compaction_reports_deferred_without_publication_retry(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = _publication_store(tmp_path)
    store.recover_and_publish()
    monkeypatch.setattr(staging_store_module, "pending_compaction_generation", lambda _database: 7)
    calls = 0

    def busy_vacuum(_database: object, _generation: int, _interrupt: object, _defer: object) -> str:
        nonlocal calls
        calls += 1
        return "deferred"

    monkeypatch.setattr(staging_store_module, "vacuum_pending", busy_vacuum)
    first = store.compact_after_cleanup(should_defer=lambda: False)
    assert first is not None and first.reason == "compaction_deferred"
    assert store.publication_retry_schedule() is None
    second = store.compact_after_cleanup(should_defer=lambda: False)
    assert second is not None and second.reason == "compaction_deferred"
    assert calls == 2


def test_hourly_owner_reports_unfinished_compaction_phase(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    store = _store(tmp_path)
    runtime = OpportunisticRuntime()
    events: list[tuple[str, str | None]] = []
    monkeypatch.setattr(runtime, "debug_event", lambda event, **fields: events.append((event, fields.get("reason"))))
    monkeypatch.setattr(
        store,
        "compact_after_cleanup",
        lambda **_kwargs: ReadyOutcome(ReadyOutcomeState.WAITING, reason="compaction_parked"),
    )
    maintenance = QuarantineMaintenance(store, None, runtime)
    maintenance._cleanup_state = _CleanupState(_CleanupPhase.VACUUM, 0, float("inf"))
    assert _run(maintenance._run_cleanup_phase(lambda: False)) is False
    assert events == [("hourly_compaction_deferred", "compaction_parked")]


def test_writer_lock_defers_terminal_sweeps_without_reporting_failures(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = _store(tmp_path)
    runtime = OpportunisticRuntime()
    failures: list[str] = []
    monkeypatch.setattr(runtime, "debug_exception", lambda event, _error, **_fields: failures.append(event))
    maintenance = QuarantineMaintenance(store, None, runtime)

    with store.device_lock():
        _run(maintenance.run_once(lambda: False))

    assert failures == []


def test_terminal_sweep_repeats_only_while_progressing_and_stops_for_capture(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def scenario() -> None:
        store = _store(tmp_path)
        maintenance = QuarantineMaintenance(store, None, OpportunisticRuntime())
        calls = 0

        def sweep(*, should_defer: Callable[[], bool]) -> SweepResult:
            nonlocal calls
            assert not should_defer()
            calls += 1
            return SweepResult((), calls < 3)

        monkeypatch.setattr(store, "sweep_terminal_quarantine", sweep)
        await maintenance._sweep_terminal_quarantine(lambda: False)
        assert calls == 3

        calls = 0

        def one_chunk(*, should_defer: Callable[[], bool]) -> SweepResult:
            nonlocal calls
            assert not should_defer()
            calls += 1
            return SweepResult((), True)

        monkeypatch.setattr(store, "sweep_terminal_quarantine", one_chunk)
        await maintenance._sweep_terminal_quarantine(lambda: calls > 0)
        assert calls == 1

    _run(scenario())


def test_deferred_quarantine_is_retried_without_changing_source(tmp_path: Path) -> None:
    async def scenario() -> None:
        store = await asyncio.to_thread(_store, tmp_path)
        await asyncio.to_thread(_seed_streaming_partial, store, 2)
        attempt_id = (await asyncio.to_thread(store.pending_attempts))[0].attempt_id
        source = await asyncio.to_thread(store.quarantine_attempt_source, attempt_id)
        before = await asyncio.to_thread(_source_snapshot, store, source)

        maintenance = QuarantineMaintenance(store, None, OpportunisticRuntime())
        await maintenance.run_once(lambda: True)

        assert await asyncio.to_thread(_source_snapshot, store, source) == before
        await maintenance.run_once(lambda: False)
        assert await asyncio.to_thread(_terminal_state, store, source) == "published"
        admitted = await asyncio.to_thread(_source_snapshot, store, source)
        assert admitted is not None
        assert cast(tuple[object, ...], admitted["source"])[4] == 2

    _run(scenario())


def test_run_once_admits_two_real_sources_into_sql_and_runs_normal_recovery(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def scenario() -> None:
        store = await asyncio.to_thread(_store, tmp_path)
        sources = await asyncio.to_thread(_quarantine_sources, store, (1, 2))
        before = await asyncio.to_thread(lambda: {source: _source_snapshot(store, source) for source in sources})
        runtime = OpportunisticRuntime()
        original_admit = runtime.admit_quarantined_prefix
        calls: list[str] = []

        def record_admission(source: str, staging: StagingPort, *, should_defer: Callable[[], bool]) -> object:
            calls.append(source)
            return original_admit(source, staging, should_defer=should_defer)

        monkeypatch.setattr(runtime, "admit_quarantined_prefix", record_admission)
        maintenance = QuarantineMaintenance(store, None, runtime)
        try:
            await maintenance.run_once(lambda: False)
        finally:
            await maintenance.close()

        assert calls == list(sources)
        terminal_states = [await asyncio.to_thread(_terminal_state, store, source) for source in sources]
        assert all(state == "published" for state in terminal_states)
        for source in sources:
            prior = before[source]
            current = await asyncio.to_thread(_source_snapshot, store, source)
            assert prior is not None and current is not None
            assert current["records"] == prior["records"]
            row = cast(tuple[object, ...], current["source"])
            assert row[4] == len(cast(tuple[object, ...], prior["records"]))
            assert isinstance(row[5], str) and row[5]

    _run(scenario())


def test_salvage_skips_retained_terminal_sources_but_keeps_actionable_source(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = _store(tmp_path)
    published, unprocessable, actionable = _quarantine_sources(store, (1, 1, 1))
    store.admit_quarantined_prefix(published)
    store.mark_quarantine_unprocessable(unprocessable, "test terminal evidence")
    terminal_before = {source: _source_snapshot(store, source) for source in (published, unprocessable)}
    classified: list[str] = []
    original_classify = quarantine._quarantine_state

    def record_classification(filesystem: quarantine.StagingFilesystem, source_id: str) -> quarantine.QuarantineState:
        classified.append(source_id)
        return original_classify(filesystem, source_id)

    monkeypatch.setattr(quarantine, "_quarantine_state", record_classification)
    maintenance = QuarantineMaintenance(store, None, OpportunisticRuntime())
    try:
        _run(maintenance._salvage_quarantine(lambda: False))
    finally:
        _run(maintenance.close())

    assert classified == [actionable]
    assert store.quarantined_attempts() == ()
    assert {source: _source_snapshot(store, source) for source in terminal_before} == terminal_before


def test_integrity_hold_preserves_source_and_next_maintenance_pass_admits_other_source(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def scenario() -> None:
        store = await asyncio.to_thread(_store, tmp_path)
        first, second = await asyncio.to_thread(_quarantine_sources, store, (1, 2))
        await asyncio.to_thread(_corrupt_quarantined_digest, store, first)
        first_before = await asyncio.to_thread(_source_snapshot, store, first)
        second_before = await asyncio.to_thread(_source_snapshot, store, second)
        runtime = OpportunisticRuntime()
        original_admit = runtime.admit_quarantined_prefix
        calls: list[str] = []

        def record_admission(source: str, staging: StagingPort, *, should_defer: Callable[[], bool]) -> object:
            calls.append(source)
            return original_admit(source, staging, should_defer=should_defer)

        monkeypatch.setattr(runtime, "admit_quarantined_prefix", record_admission)
        maintenance = QuarantineMaintenance(store, None, runtime)
        try:
            await maintenance.run_once(lambda: False)
            assert calls == [first]
            maintenance._maintenance_not_before = 0
            await maintenance.run_once(lambda: False)
        finally:
            await maintenance.close()

        assert calls == [first, second]
        first_after = await asyncio.to_thread(_source_snapshot, store, first)
        assert first_before is not None and first_after is not None
        assert first_after["records"] == first_before["records"]
        assert cast(tuple[object, ...], first_after["source"])[1:4] == (
            "blocked",
            "source_authentication_mismatch",
            "unclassified",
        )
        assert await asyncio.to_thread(_terminal_state, store, first) is None
        assert await asyncio.to_thread(_terminal_state, store, second) == "published"
        second_after = await asyncio.to_thread(_source_snapshot, store, second)
        assert second_before is not None and second_after is not None
        assert second_after["records"] == second_before["records"]

    _run(scenario())


@pytest.mark.parametrize(
    ("failure_type", "reason"),
    [(OSError, "temporary admission failure"), (QuarantineSalvageDeferredError, "maintenance deferred")],
)
def test_run_once_stops_after_retryable_or_deferred_source_without_touching_the_next(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    failure_type: type[Exception],
    reason: str,
) -> None:
    async def scenario() -> None:
        store = await asyncio.to_thread(_store, tmp_path)
        sources = await asyncio.to_thread(_quarantine_sources, store, (1, 2))
        before = await asyncio.to_thread(lambda: {source: _source_snapshot(store, source) for source in sources})
        runtime = OpportunisticRuntime()
        original_admit = runtime.admit_quarantined_prefix
        calls: list[str] = []

        def fail_first(source: str, staging: StagingPort, *, should_defer: Callable[[], bool]) -> object:
            calls.append(source)
            if source == sources[0]:
                raise failure_type(reason)
            return original_admit(source, staging, should_defer=should_defer)

        monkeypatch.setattr(runtime, "admit_quarantined_prefix", fail_first)
        maintenance = QuarantineMaintenance(store, None, runtime)
        try:
            await maintenance.run_once(lambda: False)
        finally:
            await maintenance.close()

        assert calls == [sources[0]]
        snapshots = [await asyncio.to_thread(_source_snapshot, store, source) for source in sources]
        terminal_states = [await asyncio.to_thread(_terminal_state, store, source) for source in sources]
        assert all(snapshot == before[source] for source, snapshot in zip(sources, snapshots, strict=True))
        assert all(state is None for state in terminal_states)

    _run(scenario())


def test_pending_startup_hydration_streams_raw_evidence_for_lease_bound_promotion(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = _store(tmp_path)
    _seed_streaming_partial(store, count=2)
    monkeypatch.setattr(
        Path,
        "read_bytes",
        lambda _path: (_ for _ in ()).throw(AssertionError("resume hydration must stream raw evidence")),
    )
    maintenance = QuarantineMaintenance(store, None, OpportunisticRuntime())
    state = cast(PendingStartupState, _run(maintenance.prepare_pending_startup()))
    assert state.pending is not None
    with store.device_lock() as lease:
        resumed = store.resume_streaming_attempt(lease)
        assert resumed is not None
        resumed.close()


def test_presence_startup_state_binds_once_after_a_completed_attempt(tmp_path: Path) -> None:
    async def scenario() -> None:
        class Presence:
            async def wait_for_attempt(self) -> PresenceWake:
                return PresenceWake("advertisement")

            async def close(self) -> None:
                return None

        store = await asyncio.to_thread(_store, tmp_path)
        await asyncio.to_thread(_seed_streaming_partial, store, 1)
        maintenance = QuarantineMaintenance(store, None, OpportunisticRuntime())
        bound: list[PendingStartupState] = []

        await maintenance.wait_for_presence_attempt(Presence(), bound.append)
        pending = bound[0].pending
        assert pending is not None

        def complete_pending() -> None:
            with store.device_lock() as lease:
                completed = store.resume_streaming_attempt(lease)
                assert completed is not None
                completed.publish_prefix()
                completed.close(durable=True)
            store.terminalize_prefix_attempt(pending.attempt_id)

        await asyncio.to_thread(complete_pending)
        assert await asyncio.to_thread(store.pending_attempts) == ()

        # A second wake must not bind maintenance's cached, now-completed
        # descriptor back into the reconciler.
        await maintenance.wait_for_presence_attempt(Presence(), bound.append)

        assert len(bound) == 1
        assert bound[0].pending is not None

    _run(scenario())


def test_presence_idle_interval_cancellation_does_not_wait_for_interval(tmp_path: Path) -> None:
    async def scenario() -> None:
        started = asyncio.Event()

        class Presence:
            async def wait_for_attempt(self) -> PresenceWake:
                await asyncio.Event().wait()
                raise AssertionError("unreachable")

            async def close(self) -> None:
                return None

        maintenance = QuarantineMaintenance(
            _store(tmp_path),
            None,
            OpportunisticRuntime(),
            config=CollectorConfig(retry=RetryConfig(maintenance_interval_seconds=30.0)),
        )

        async def run_once(_should_defer: Callable[[], bool]) -> None:
            started.set()

        maintenance.run_once = run_once  # type: ignore[method-assign]
        owner = asyncio.create_task(maintenance.wait_for_presence_attempt(Presence(), lambda _: None))
        try:
            await asyncio.wait_for(started.wait(), timeout=2)
            owner.cancel()
            with pytest.raises(asyncio.CancelledError):
                await asyncio.wait_for(owner, timeout=0.5)
        finally:
            owner.cancel()
            await asyncio.gather(owner, return_exceptions=True)
            await maintenance.close()

    _run(scenario())


def test_transient_clock_publication_does_not_gate_startup_and_arms_retry(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def scenario() -> None:
        config = CollectorConfig(retry=RetryConfig(rapid_backoff=(0.02,)))
        store = _publication_store(tmp_path, config=config)
        now = asyncio.get_running_loop().time()
        monkeypatch.setattr(staging_store_module, "monotonic", lambda: now)
        attempts: list[int] = []

        def recover_and_publish() -> ReadyOutcome:
            attempts.append(1)
            return ReadyOutcome(ReadyOutcomeState.TRANSIENT, reason="storage_busy_or_io")

        monkeypatch.setattr(store, "_recover_and_publish_unlocked", recover_and_publish)
        maintenance = QuarantineMaintenance(store, None, OpportunisticRuntime(), config=config)
        armed: list[tuple[int, float] | None] = []
        monkeypatch.setattr(
            maintenance, "_arm_publication_timer", lambda: armed.append(store.publication_retry_schedule())
        )
        try:
            await maintenance.ensure_publication_ready()
            assert attempts == [1]
            schedule = store.publication_retry_schedule()
            assert schedule is not None
            assert schedule[1] == now + config.retry.rapid_backoff[0]
            assert armed == [schedule]
        finally:
            await maintenance.close()

    _run(scenario())


def test_transient_publication_retry_settles_and_clears_schedule(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def scenario() -> None:
        config = CollectorConfig(retry=RetryConfig(rapid_backoff=(0.01,)))
        store = _publication_store(tmp_path, config=config)
        settled = asyncio.Event()
        calls = 0
        events: list[tuple[str, str | None]] = []

        def recover_and_publish() -> ReadyOutcome:
            nonlocal calls
            calls += 1
            if calls == 1:
                return ReadyOutcome(ReadyOutcomeState.TRANSIENT, reason="storage_busy_or_io")
            return ReadyOutcome(ReadyOutcomeState.WAITING, reason="idle")

        monkeypatch.setattr(store, "_recover_and_publish_unlocked", recover_and_publish)
        runtime = OpportunisticRuntime()

        def record_event(event: str, **fields: object) -> None:
            reason = fields.get("reason")
            assert isinstance(reason, str)
            events.append((event, reason))
            if event == "ready_publication_waiting":
                settled.set()

        monkeypatch.setattr(
            runtime,
            "debug_event",
            record_event,
        )
        maintenance = QuarantineMaintenance(store, None, runtime, config=config)
        try:
            await maintenance.ensure_publication_ready()
            assert calls == 1
            assert store.publication_retry_schedule() is not None

            await asyncio.wait_for(settled.wait(), timeout=5)

            assert calls == 2
            assert store.publication_retry_schedule() is None
            assert not store.publication_followup_due()
            assert events == [
                ("ready_publication_transient", "storage_busy_or_io"),
                ("ready_publication_waiting", "idle"),
            ]
        finally:
            await maintenance.close()

    _run(scenario())


@pytest.mark.parametrize("phase", ["revision", "effect"])
def test_publication_busy_is_transient_and_keeps_input_unchanged(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, phase: str
) -> None:
    store = _publication_store(tmp_path)
    assert store._publication_root is not None
    before = tuple(store._publication_root.iterdir())
    busy = TransientInternalAudioStorageError("database is busy", sqlite_errorcode=sqlite3.SQLITE_BUSY)

    if phase == "revision":
        monkeypatch.setattr(store, "_publication_revision", lambda: (_ for _ in ()).throw(busy))
    else:
        monkeypatch.setattr(store, "_recover_and_publish_unlocked", lambda: (_ for _ in ()).throw(busy))

    outcome = store.publish_ready()

    assert outcome.state is ReadyOutcomeState.TRANSIENT
    assert store.publication_retry_schedule() is not None
    assert tuple(store._publication_root.iterdir()) == before


def test_publication_semantic_mismatch_is_blocked_without_retry(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = _publication_store(tmp_path)
    monkeypatch.setattr(
        store,
        "_recover_and_publish_unlocked",
        lambda: (_ for _ in ()).throw(ReadyBundleError("semantic mismatch")),
    )

    outcome = store.publish_ready()

    assert outcome.state is ReadyOutcomeState.BLOCKED
    assert outcome.reason == "semantic mismatch"
    assert store.publication_retry_schedule() is None


@pytest.mark.parametrize(
    ("successful_result", "success_event"),
    [
        (ReadyOutcome(ReadyOutcomeState.WAITING, reason="idle"), "ready_publication_waiting"),
        (ReadyOutcome(ReadyOutcomeState.PUBLISHED, reason="published"), "ready_publication_published"),
    ],
)
def test_typed_publication_results_are_reported_once(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    successful_result: ReadyOutcome,
    success_event: str,
) -> None:
    async def scenario() -> None:
        store = _publication_store(tmp_path)
        attempts = 0
        events: list[tuple[str, str, int | None]] = []

        def recover_and_publish() -> ReadyOutcome:
            nonlocal attempts
            attempts += 1
            return successful_result

        store._recover_and_publish_unlocked = recover_and_publish  # type: ignore[method-assign]
        runtime = OpportunisticRuntime()
        monkeypatch.setattr(runtime, "debug_event", lambda event, **_fields: events.append(("event", event, None)))
        maintenance = QuarantineMaintenance(store, None, runtime)

        assert await maintenance.ensure_publication_ready() is None

        assert attempts == 1
        assert events == [("event", success_event, None)]

    _run(scenario())


def test_unexpected_publication_exception_is_logged_and_settled_without_retry(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    async def scenario() -> None:
        caplog.set_level(logging.DEBUG, logger="omi_collector.debug")
        store = _publication_store(tmp_path)
        calls = 0
        events: list[tuple[str, str | None]] = []

        def fail_unexpectedly() -> ReadyOutcome:
            nonlocal calls
            calls += 1
            raise RuntimeError("unexpected publication fault")

        runtime = OpportunisticRuntime()
        monkeypatch.setattr(store, "_recover_and_publish_unlocked", fail_unexpectedly)
        monkeypatch.setattr(
            runtime,
            "debug_event",
            lambda event, **fields: events.append((event, fields.get("reason"))),
        )
        maintenance = QuarantineMaintenance(store, None, runtime)
        await maintenance.ensure_publication_ready()
        await asyncio.sleep(0)

        assert calls == 1
        assert events == [("ready_publication_blocked", "RuntimeError")]
        diagnostic = [
            record
            for record in caplog.records
            if record.name == "omi_collector.debug" and "ready_publication_failure" in record.getMessage()
        ]
        assert len(diagnostic) == 1
        assert diagnostic[0].levelno == logging.ERROR
        assert diagnostic[0].getMessage().startswith("ready_publication_failure: RuntimeError at ")
        assert '"phase":"publication_effect"' in diagnostic[0].getMessage()
        assert diagnostic[0].exc_info is None
        assert diagnostic[0].exc_text is None
        assert diagnostic[0].stack_info is None
        errors = [
            record
            for record in caplog.records
            if record.name == "omi_collector.debug"
            and record.levelno == logging.ERROR
            and "ready_publication_failure" in record.getMessage()
        ]
        assert len(errors) == 1
        assert "unexpected publication fault" not in caplog.text
        assert store.publication_retry_schedule() is None
        await maintenance.close()

        from omi_collector.capture.adapters.clock_state_store import ClockStateStore

        def bad_clock(_self: object) -> None:
            raise ValueError("clock metadata sentinel")

        monkeypatch.setattr(ClockStateStore, "state", bad_clock)
        with pytest.raises(ReadyBundleError, match="clock metadata unavailable for public audio v2") as failure:
            await asyncio.to_thread(type(store)._recover_and_publish_unlocked, store)
        assert isinstance(failure.value.__cause__, ValueError)
        assert str(failure.value.__cause__) == "clock metadata sentinel"

    _run(scenario())


def test_presence_scan_starts_before_startup_and_wake_waits_for_binding(tmp_path: Path) -> None:
    async def scenario() -> None:
        events: list[str] = []
        release_wake = asyncio.Event()
        startup_bound = asyncio.Event()

        class Presence:
            policy = PresencePolicy(rapid_backoff=(0.001,))
            closed = False

            async def wait_for_attempt(self) -> PresenceWake:
                events.append("scan")
                await release_wake.wait()
                events.append("wake")
                return PresenceWake("advertisement")

            async def close(self) -> None:
                self.closed = True

        store = _store(tmp_path)
        original = store.pending_attempts

        def pending() -> tuple[object, ...]:
            events.append("startup")
            return original()

        store.pending_attempts = pending  # type: ignore[method-assign]
        maintenance = QuarantineMaintenance(store, None, OpportunisticRuntime())

        def bind(state: PendingStartupState) -> None:
            assert state.pending is None
            events.append("bind")
            startup_bound.set()

        task = asyncio.create_task(maintenance.wait_for_presence_attempt(Presence(), bind))
        try:
            await asyncio.wait_for(startup_bound.wait(), timeout=5.0)
            release_wake.set()
            wake = await asyncio.wait_for(task, timeout=5.0)

            assert wake.reason == "advertisement"
            assert events.index("scan") < events.index("startup") < events.index("bind") < events.index("wake")
        finally:
            release_wake.set()
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            await maintenance.close()

    _run(scenario())


def test_presence_maintenance_cancellation_joins_cooperative_worker(tmp_path: Path) -> None:
    async def scenario() -> None:
        worker_started = threading.Event()
        worker_stopped = threading.Event()
        release_sweep = threading.Event()
        waiter_started = asyncio.Event()
        waiter_cancelled = asyncio.Event()
        waiter_task: asyncio.Task[object] | None = None

        class Presence:
            policy = PresencePolicy(rapid_backoff=(0.001,))
            closed = False

            async def wait_for_attempt(self) -> PresenceWake:
                nonlocal waiter_task
                waiter_task = asyncio.current_task()
                waiter_started.set()
                try:
                    await asyncio.Event().wait()
                except asyncio.CancelledError:
                    waiter_cancelled.set()
                    raise
                raise AssertionError("unreachable")

            async def close(self) -> None:
                self.closed = True

        store = _store(tmp_path)

        def sweep(*, should_defer: Callable[[], bool]) -> SweepResult:
            worker_started.set()
            while not release_sweep.is_set() and not should_defer():
                time.sleep(0.001)
            worker_stopped.set()
            return SweepResult((), False)

        store.sweep_terminal_quarantine = sweep  # type: ignore[method-assign]
        maintenance = QuarantineMaintenance(store, None, OpportunisticRuntime())
        maintenance._cleanup_state = _CleanupState(_CleanupPhase.WAITING, 0)
        task = asyncio.create_task(maintenance.wait_for_presence_attempt(Presence(), lambda _: None))
        try:
            await asyncio.wait_for(waiter_started.wait(), timeout=5.0)
            assert await asyncio.to_thread(worker_started.wait, 1)
            task.cancel()
            await asyncio.wait_for(waiter_cancelled.wait(), timeout=5.0)
            done, pending = await asyncio.wait({task}, timeout=5.0)
            assert task in done and not pending
            with pytest.raises(asyncio.CancelledError):
                task.result()
            assert worker_stopped.is_set()
        finally:
            if waiter_task is not None:
                waiter_task.cancel()
                done, pending = await asyncio.wait({waiter_task}, timeout=5.0)
                assert waiter_task in done and not pending
                await asyncio.gather(waiter_task, return_exceptions=True)
            release_sweep.set()
            task.cancel()
            done, pending = await asyncio.wait({task}, timeout=5.0)
            assert task in done and not pending
            await asyncio.gather(task, return_exceptions=True)
            await maintenance.close()

    _run(scenario())


def test_publication_shutdown_joins_mutation_after_repeated_cancellation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def scenario() -> None:
        started = threading.Event()
        release = threading.Event()
        finished = threading.Event()
        store = _publication_store(tmp_path)
        publications: list[ReadyOutcome] = []

        def publish() -> ReadyOutcome:
            publications.append(ReadyOutcome(ReadyOutcomeState.WAITING, reason="idle"))
            started.set()
            if not release.wait(2):
                raise TimeoutError("publication mutation was not released")
            finished.set()
            return publications[-1]

        monkeypatch.setattr(store, "_recover_and_publish_unlocked", publish)
        maintenance = QuarantineMaintenance(store, None, OpportunisticRuntime())
        maintenance.schedule_publication_retry()
        closing: asyncio.Task[None] | None = None
        try:
            assert await asyncio.to_thread(started.wait, 1)
            publication = maintenance._publication_retry_task
            assert publication is not None
            maintenance.schedule_publication_retry()
            maintenance.schedule_publication_retry()
            await asyncio.sleep(0.01)
            assert maintenance._publication_retry_task is publication
            assert len(publications) == 1
            closing = asyncio.create_task(maintenance.close())
            await asyncio.sleep(0.01)
            assert not closing.done()
            publication.cancel()
            await asyncio.sleep(0.01)
            assert not closing.done()
            release.set()
            await closing
            assert finished.is_set()
            assert publication.done()
            assert len(publications) == 1
            assert not store.publication_wake_admitted()
        finally:
            release.set()
            if closing is not None:
                await closing
            await maintenance.close()

    _run(scenario())


def test_capture_priority_joins_running_retry_and_defers_new_requests(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def scenario() -> None:
        store = _publication_store(tmp_path)
        recovery = _BlockedRecovery(store._recover_and_publish_unlocked)
        monkeypatch.setattr(store, "_recover_and_publish_unlocked", recovery)
        maintenance = QuarantineMaintenance(store, None, OpportunisticRuntime())
        maintenance.schedule_publication_retry()
        try:
            assert await asyncio.to_thread(recovery.started.wait, 1)
            entering = asyncio.create_task(maintenance.enter_capture_priority())
            await asyncio.sleep(0)
            maintenance.schedule_publication_retry()
            assert not entering.done()
            assert recovery.calls == 1
            recovery.release.set()
            await entering
            assert recovery.finished.is_set()
            await asyncio.sleep(0)
            assert recovery.calls == 1
            await maintenance.exit_capture_priority()
            for _ in range(20):
                if recovery.calls == 2:
                    break
                await asyncio.sleep(0.001)
            assert recovery.calls == 2
            assert recovery.maximum_active == 1
            await maintenance.close()
            maintenance.schedule_publication_retry()
            await asyncio.sleep(0)
            assert recovery.calls == 2
        finally:
            recovery.release.set()
            await maintenance.close()

    _run(scenario())


def test_capture_priority_resumes_running_retry_without_new_request(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def scenario() -> None:
        store = _publication_store(tmp_path)
        recovery = _BlockedRecovery(store._recover_and_publish_unlocked)
        monkeypatch.setattr(store, "_recover_and_publish_unlocked", recovery)
        maintenance = QuarantineMaintenance(store, None, OpportunisticRuntime())
        maintenance.schedule_publication_retry()
        entering: asyncio.Task[None] | None = None
        try:
            assert await asyncio.to_thread(recovery.started.wait, 1)
            entering = asyncio.create_task(maintenance.enter_capture_priority())
            await asyncio.sleep(0)
            assert not entering.done()
            assert recovery.calls == 1

            recovery.release.set()
            await entering
            assert recovery.finished.is_set()
            assert recovery.calls == 1

            await maintenance.exit_capture_priority()
            for _ in range(100):
                if recovery.calls == 2:
                    break
                await asyncio.sleep(0.001)
            assert recovery.calls == 2
            assert recovery.maximum_active == 1
        finally:
            recovery.release.set()
            if entering is not None:
                await asyncio.gather(entering, return_exceptions=True)
            await maintenance.close()

    _run(scenario())


def test_capture_priority_defers_pending_publication_timer_until_exit(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def scenario() -> None:
        config = CollectorConfig(retry=RetryConfig(rapid_backoff=(30.0,)))
        store = _publication_store(tmp_path, config=config)
        resumed = threading.Event()
        loop = asyncio.get_running_loop()
        calls = 0

        def publish() -> ReadyOutcome:
            nonlocal calls
            calls += 1
            if calls == 1:
                return ReadyOutcome(ReadyOutcomeState.TRANSIENT, reason="storage_busy_or_io")
            loop.call_soon_threadsafe(resumed.set)
            return ReadyOutcome(ReadyOutcomeState.WAITING, reason="idle")

        monkeypatch.setattr(store, "_recover_and_publish_unlocked", publish)
        maintenance = QuarantineMaintenance(store, None, OpportunisticRuntime(), config=config)
        try:
            await maintenance.ensure_publication_ready()
            assert calls == 1
            assert maintenance._publication_retry_handle is not None
            assert store.publication_retry_schedule() is not None

            await maintenance.enter_capture_priority()
            assert maintenance._publication_retry_handle is None
            assert store.publication_retry_schedule() is None

            await maintenance.exit_capture_priority()
            assert await asyncio.to_thread(resumed.wait, 1)
            assert calls == 2
        finally:
            await maintenance.close()

    _run(scenario())


def test_cancelled_capture_priority_entry_joins_retry_before_releasing_gate(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def scenario() -> None:
        started = threading.Event()
        release = threading.Event()
        finished = threading.Event()
        store = _publication_store(tmp_path)

        def publish() -> ReadyOutcome:
            started.set()
            if not release.wait(2):
                raise TimeoutError("publication mutation was not released")
            finished.set()
            return ReadyOutcome(ReadyOutcomeState.WAITING, reason="idle")

        monkeypatch.setattr(store, "_recover_and_publish_unlocked", publish)
        maintenance = QuarantineMaintenance(store, None, OpportunisticRuntime())
        maintenance.schedule_publication_retry()
        entering: asyncio.Task[None] | None = None
        try:
            assert await asyncio.to_thread(started.wait, 1)

            async def enter() -> None:
                try:
                    await maintenance.enter_capture_priority()
                finally:
                    await maintenance.exit_capture_priority()

            entering = asyncio.create_task(enter())
            await asyncio.sleep(0)
            entering.cancel()
            await asyncio.sleep(0)
            assert not entering.done()
            release.set()
            with pytest.raises(asyncio.CancelledError):
                await entering
            assert finished.is_set()
            await maintenance.close()
        finally:
            release.set()
            if entering is not None:
                await asyncio.gather(entering, return_exceptions=True)
            await maintenance.close()

    _run(scenario())


def test_visit_begin_failure_releases_capture_priority(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    async def scenario() -> None:
        store = await asyncio.to_thread(_publication_store, tmp_path)
        await asyncio.to_thread(store.append_ready_closure, 11, "drained")
        original = store.begin_ready_visit
        maintenance = QuarantineMaintenance(store, None, OpportunisticRuntime())
        monkeypatch.setattr(store, "begin_ready_visit", lambda: (_ for _ in ()).throw(OSError("storage unavailable")))

        with pytest.raises(OSError, match="storage unavailable"):
            await maintenance.enter_capture_priority()
        assert store.publication_wake_admitted()

        monkeypatch.setattr(store, "begin_ready_visit", original)
        await maintenance.enter_capture_priority()
        assert not store.publication_wake_admitted()
        await maintenance.exit_capture_priority()
        assert store.publication_wake_admitted()
        await maintenance.close()

    _run(scenario())


def test_cancelled_visit_begin_joins_storage_before_releasing_gate(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def scenario() -> None:
        store = await asyncio.to_thread(_publication_store, tmp_path)
        await asyncio.to_thread(store.append_ready_closure, 11, "drained")
        started = threading.Event()
        release = threading.Event()
        original = store.begin_ready_visit

        def blocked_begin() -> object:
            started.set()
            if not release.wait(2):
                raise TimeoutError("visit begin was not released")
            return original()

        monkeypatch.setattr(store, "begin_ready_visit", blocked_begin)
        maintenance = QuarantineMaintenance(store, None, OpportunisticRuntime())
        entering = asyncio.create_task(maintenance.enter_capture_priority())
        try:
            assert await asyncio.to_thread(started.wait, 1)
            entering.cancel()
            await asyncio.sleep(0)
            assert not entering.done()
            release.set()
            with pytest.raises(asyncio.CancelledError):
                await entering
            assert store.publication_wake_admitted()
            assert closure_rows(store)[0]["reason"] == "collecting"
        finally:
            release.set()
            await asyncio.gather(entering, return_exceptions=True)
            await maintenance.close()

    _run(scenario())


def test_presence_wait_joins_mutation_after_repeated_owner_cancellation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def scenario() -> None:
        started = threading.Event()
        release = threading.Event()
        finished = threading.Event()

        class Presence:
            policy = PresencePolicy(rapid_backoff=(0.001,))

            def __init__(self) -> None:
                self.waiter_task: asyncio.Task[PresenceWake] | None = None
                self.cancellation_observed = asyncio.Event()

            async def wait_for_attempt(self) -> PresenceWake:
                current = asyncio.current_task()
                assert current is not None
                self.waiter_task = cast(asyncio.Task[PresenceWake], current)
                try:
                    await asyncio.Event().wait()
                except asyncio.CancelledError:
                    self.cancellation_observed.set()
                    raise
                raise AssertionError("unreachable")

            async def close(self) -> None:
                return None

        def sweep(*, should_defer: Callable[[], bool]) -> SweepResult:
            del should_defer
            started.set()
            if not release.wait(2):
                raise TimeoutError("maintenance mutation was not released")
            finished.set()
            return SweepResult((), False)

        store = _store(tmp_path)
        monkeypatch.setattr(store, "sweep_terminal_quarantine", sweep)
        maintenance = QuarantineMaintenance(store, None, OpportunisticRuntime())
        maintenance._cleanup_state = _CleanupState(_CleanupPhase.WAITING, 0)
        presence = Presence()
        owner = asyncio.create_task(maintenance.wait_for_presence_attempt(presence, lambda _: None))
        try:
            assert await asyncio.to_thread(started.wait, 1)
            owner.cancel()
            await asyncio.wait_for(presence.cancellation_observed.wait(), timeout=5.0)
            await asyncio.sleep(0.01)
            owner.cancel()
            await asyncio.sleep(0.01)
            assert not owner.done()
            release.set()
            done, pending = await asyncio.wait({owner}, timeout=5.0)
            assert owner in done and not pending
            with pytest.raises(asyncio.CancelledError):
                owner.result()
            assert finished.is_set()
        finally:
            release.set()
            if presence.waiter_task is not None:
                presence.waiter_task.cancel()
                done, pending = await asyncio.wait({presence.waiter_task}, timeout=5.0)
                assert presence.waiter_task in done and not pending
                await asyncio.gather(presence.waiter_task, return_exceptions=True)
            owner.cancel()
            done, pending = await asyncio.wait({owner}, timeout=5.0)
            assert owner in done and not pending
            await asyncio.gather(owner, return_exceptions=True)
            await maintenance.close()

    _run(scenario())


def test_maintenance_failure_after_internal_permit_closes_presence(tmp_path: Path) -> None:
    async def scenario() -> None:
        class Presence:
            closed = False

            async def wait_for_attempt(self) -> PresenceWake:
                return PresenceWake("advertisement")

            async def close(self) -> None:
                self.closed = True

        async def fail_after_startup(*_args: object) -> None:
            raise RuntimeError("maintenance failed")

        maintenance = QuarantineMaintenance(_store(tmp_path), None, OpportunisticRuntime())
        maintenance._prepare_and_run = fail_after_startup  # type: ignore[method-assign]
        presence = Presence()

        with pytest.raises(RuntimeError, match="maintenance failed"):
            await maintenance.wait_for_presence_attempt(presence, lambda _: None)

        assert presence.closed

    _run(scenario())


def test_cancellation_after_internal_permit_closes_presence(tmp_path: Path) -> None:
    async def scenario() -> None:
        started = asyncio.Event()
        release_permit = asyncio.Event()
        deferral_started = asyncio.Event()
        finish_maintenance = asyncio.Event()

        class Presence:
            closed = False

            async def wait_for_attempt(self) -> PresenceWake:
                await release_permit.wait()
                return PresenceWake("advertisement")

            async def close(self) -> None:
                self.closed = True

        async def wait_for_deferral(
            defer_requested: threading.Event, _bind: Callable[[PendingStartupState], None]
        ) -> None:
            started.set()
            await asyncio.to_thread(defer_requested.wait)
            deferral_started.set()
            await finish_maintenance.wait()

        maintenance = QuarantineMaintenance(_store(tmp_path), None, OpportunisticRuntime())
        maintenance._prepare_and_run = wait_for_deferral  # type: ignore[method-assign]
        presence = Presence()
        task = asyncio.create_task(maintenance.wait_for_presence_attempt(presence, lambda _: None))
        try:
            await asyncio.wait_for(started.wait(), timeout=5.0)
            release_permit.set()
            await asyncio.wait_for(deferral_started.wait(), timeout=5.0)
            task.cancel()
            finish_maintenance.set()
            done, pending = await asyncio.wait({task}, timeout=5.0)
            assert task in done and not pending
            with pytest.raises(asyncio.CancelledError):
                task.result()
            assert presence.closed
        finally:
            release_permit.set()
            finish_maintenance.set()
            task.cancel()
            done, pending = await asyncio.wait({task}, timeout=5.0)
            assert task in done and not pending
            await asyncio.gather(task, return_exceptions=True)

    _run(scenario())


def test_invalidated_startup_state_prioritizes_sealed_prefix_over_new_pending_attempt(tmp_path: Path) -> None:
    async def scenario() -> None:
        class Presence:
            async def wait_for_attempt(self) -> PresenceWake:
                return PresenceWake("advertisement")

            async def close(self) -> None:
                return None

        store = await asyncio.to_thread(_store, tmp_path)
        await asyncio.to_thread(_seed_streaming_partial, store, 1)
        maintenance = QuarantineMaintenance(store, None, OpportunisticRuntime())
        bound: list[PendingStartupState] = []
        await maintenance.wait_for_presence_attempt(Presence(), bound.append)
        first = bound[0].pending
        assert first is not None

        def complete_first() -> None:
            with store.device_lock() as lease:
                completed = store.resume_streaming_attempt(lease)
                assert completed is not None
                completed.publish_prefix()
                completed.close(durable=True)
            store.terminalize_prefix_attempt(first.attempt_id)

        await asyncio.to_thread(complete_first)
        await asyncio.to_thread(_seed_streaming_partial, store, 1)

        maintenance.invalidate_startup_state()
        await maintenance.wait_for_presence_attempt(Presence(), bound.append)
        await maintenance.close()

        assert len(bound) == 2
        assert bound[0].pending is not None and bound[0].pending.attempt_id == first.attempt_id
        assert bound[1] == PendingStartupState(None, None, "needs_interrupted_close")

    _run(scenario())


def test_quarantine_retry_backoff_stays_at_last_delay_after_repeated_failures(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = _store(tmp_path)
    sources = _quarantine_sources(store, (1, 2))
    now = 100.0
    monkeypatch.setattr("omi_collector.capture.application.quarantine_maintenance.monotonic", lambda: now)
    backoff = (1.0, 3.0)
    config = CollectorConfig(
        retry=RetryConfig(maintenance_interval_seconds=0.1, quarantine_publish_backoff_seconds=backoff)
    )
    calls: list[str] = []
    runtime = OpportunisticRuntime()

    def keep_retryable_failure(
        source: str,
        _staging: StagingPort,
        *,
        should_defer: Callable[[], bool],
    ) -> object:
        assert not should_defer()
        calls.append(source)
        raise OSError("persistent temporary publication failure")

    monkeypatch.setattr(runtime, "admit_quarantined_prefix", keep_retryable_failure)
    maintenance = QuarantineMaintenance(store, None, runtime, config=config)
    total_attempts = len(backoff) + 3
    try:
        for attempt in range(total_attempts):
            if attempt:
                now += backoff[min(attempt - 1, len(backoff) - 1)] + 0.01
            _run(maintenance.run_once(lambda: False))
    finally:
        _run(maintenance.close())

    assert calls == [sources[0]] * total_attempts
    assert all(_terminal_state(store, source) is None for source in sources)
    assert not store.capture_root.exists() or tuple(store.capture_root.iterdir()) == ()


def test_startup_failure_before_presence_permit_leaves_presence_open(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def scenario() -> None:
        inspection_started = threading.Event()
        release_inspection = threading.Event()
        waiter_cancelled = asyncio.Event()
        waiter_task: asyncio.Task[PresenceWake] | None = None

        class Presence:
            closed = False

            async def wait_for_attempt(self) -> PresenceWake:
                nonlocal waiter_task
                current = asyncio.current_task()
                assert current is not None
                waiter_task = cast(asyncio.Task[PresenceWake], current)
                try:
                    await asyncio.Event().wait()
                except asyncio.CancelledError:
                    waiter_cancelled.set()
                    raise
                raise AssertionError("presence waiter should remain blocked")

            async def close(self) -> None:
                self.closed = True

        store = _store(tmp_path)
        original_pending = store.pending_attempts

        def fail_public_startup_inspection() -> object:
            inspection_started.set()
            if not release_inspection.wait(2):
                raise TimeoutError("startup inspection was not released")
            raise RuntimeError("startup inspection failed")

        monkeypatch.setattr(store, "pending_attempts", fail_public_startup_inspection)
        maintenance = QuarantineMaintenance(store, None, OpportunisticRuntime())
        presence = Presence()
        owner = asyncio.create_task(maintenance.wait_for_presence_attempt(presence, lambda _: None))
        try:
            assert await asyncio.to_thread(inspection_started.wait, 1)
            release_inspection.set()
            with pytest.raises(RuntimeError, match="startup inspection failed"):
                done, pending = await asyncio.wait({owner}, timeout=5.0)
                assert owner in done and not pending
                owner.result()
            assert waiter_cancelled.is_set()
            assert not presence.closed
            assert store.pending_attempts is fail_public_startup_inspection
        finally:
            release_inspection.set()
            if waiter_task is not None:
                waiter_task.cancel()
                done, pending = await asyncio.wait({waiter_task}, timeout=5.0)
                assert waiter_task in done and not pending
                await asyncio.gather(waiter_task, return_exceptions=True)
            owner.cancel()
            done, pending = await asyncio.wait({owner}, timeout=5.0)
            assert owner in done and not pending
            await asyncio.gather(owner, return_exceptions=True)
            await maintenance.close()
            monkeypatch.setattr(store, "pending_attempts", original_pending)

    _run(scenario())


def test_presence_wake_waits_until_public_maintenance_scan_finishes(tmp_path: Path) -> None:
    async def scenario() -> None:
        scan_finished = threading.Event()
        release_wake = asyncio.Event()
        bind_finished = asyncio.Event()

        class Presence:
            closed = False

            async def wait_for_attempt(self) -> PresenceWake:
                await release_wake.wait()
                return PresenceWake("advertisement")

            async def close(self) -> None:
                self.closed = True

        store = _store(tmp_path)
        original_scan = store.quarantined_attempts

        def record_final_public_scan(*, should_defer: Callable[[], bool] | None = None) -> tuple[str, ...]:
            sources = original_scan(should_defer=should_defer)
            scan_finished.set()
            return sources

        store.quarantined_attempts = record_final_public_scan  # type: ignore[method-assign]
        maintenance = QuarantineMaintenance(store, None, OpportunisticRuntime())
        bound: list[PendingStartupState] = []

        def bind(state: PendingStartupState) -> None:
            bound.append(state)
            bind_finished.set()

        owner = asyncio.create_task(maintenance.wait_for_presence_attempt(Presence(), bind))
        try:
            assert await asyncio.to_thread(scan_finished.wait, 2)
            assert bind_finished.is_set()
            for _ in range(5):
                await asyncio.sleep(0)
            assert not owner.done()
            release_wake.set()
            wake = await asyncio.wait_for(owner, timeout=2)
            assert wake == PresenceWake("advertisement")
            assert bound == [PendingStartupState(None, None, "empty")]
        finally:
            release_wake.set()
            owner.cancel()
            done, pending = await asyncio.wait({owner}, timeout=5.0)
            assert owner in done and not pending
            await asyncio.gather(owner, return_exceptions=True)
            await maintenance.close()

    _run(scenario())


def test_repeated_owner_cancellation_during_startup_failure_cleanup_preserves_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def scenario() -> None:
        inspection_started = threading.Event()
        release_inspection = threading.Event()
        cancellation_cleanup_started = asyncio.Event()
        waiter_task: asyncio.Task[PresenceWake] | None = None
        failure = RuntimeError("authoritative startup failure")
        owner: asyncio.Task[object] | None = None

        class Presence:
            async def wait_for_attempt(self) -> PresenceWake:
                nonlocal waiter_task
                current = asyncio.current_task()
                assert current is not None
                waiter_task = cast(asyncio.Task[PresenceWake], current)
                try:
                    await asyncio.Event().wait()
                except asyncio.CancelledError:
                    cancellation_cleanup_started.set()
                    current = asyncio.current_task()
                    assert current is not None
                    current.get_loop().call_soon(owner.cancel)  # type: ignore[union-attr]
                    current.get_loop().call_soon(owner.cancel)  # type: ignore[union-attr]
                    raise
                raise AssertionError("presence waiter should remain blocked")

            async def close(self) -> None:
                return None

        store = _store(tmp_path)

        def fail_after_gate() -> object:
            inspection_started.set()
            if not release_inspection.wait(2):
                raise TimeoutError("startup inspection was not released")
            raise failure

        monkeypatch.setattr(store, "pending_attempts", fail_after_gate)
        maintenance = QuarantineMaintenance(store, None, OpportunisticRuntime())
        owner = asyncio.create_task(maintenance.wait_for_presence_attempt(Presence(), lambda _: None))
        try:
            assert await asyncio.to_thread(inspection_started.wait, 1)
            release_inspection.set()
            with pytest.raises(RuntimeError) as error:
                done, pending = await asyncio.wait({owner}, timeout=5.0)
                assert owner in done and not pending
                owner.result()
            assert error.value is failure
            assert cancellation_cleanup_started.is_set()
            assert owner.done()
        finally:
            release_inspection.set()
            if waiter_task is not None:
                waiter_task.cancel()
                done, pending = await asyncio.wait({waiter_task}, timeout=5.0)
                assert waiter_task in done and not pending
                await asyncio.gather(waiter_task, return_exceptions=True)
            owner.cancel()
            done, pending = await asyncio.wait({owner}, timeout=5.0)
            assert owner in done and not pending
            await asyncio.gather(owner, return_exceptions=True)
            await maintenance.close()

    _run(scenario())


def test_successful_publication_retry_schedule_finishes_without_a_second_invocation(tmp_path: Path) -> None:
    async def scenario() -> None:
        store = _publication_store(tmp_path)
        started = threading.Event()
        calls: list[ReadyOutcome] = []

        def recovered() -> ReadyOutcome:
            outcome = ReadyOutcome(ReadyOutcomeState.WAITING, reason="idle")
            calls.append(outcome)
            started.set()
            return outcome

        store._recover_and_publish_unlocked = recovered  # type: ignore[method-assign]
        maintenance = QuarantineMaintenance(store, None, OpportunisticRuntime())
        try:
            maintenance.schedule_publication_retry()
            publication = maintenance._publication_retry_task
            assert publication is not None
            assert await asyncio.to_thread(started.wait, 1)
            await publication
            assert calls == [ReadyOutcome(ReadyOutcomeState.WAITING, reason="idle")]
        finally:
            await maintenance.close()

    _run(scenario())


def test_unexpected_publication_retry_failure_is_reported(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    async def scenario() -> None:
        store = _publication_store(tmp_path)
        failure = RuntimeError("unexpected publication failure")
        observed: list[tuple[str, BaseException]] = []

        async def fail_recover() -> None:
            raise failure

        runtime = OpportunisticRuntime()
        monkeypatch.setattr(runtime, "debug_exception", lambda event, error, **_fields: observed.append((event, error)))
        maintenance = QuarantineMaintenance(store, None, runtime)
        monkeypatch.setattr(maintenance, "_recover_and_publish", fail_recover)
        try:
            maintenance.schedule_publication_retry()
            task = maintenance._publication_retry_task
            assert task is not None
            assert (await asyncio.gather(task, return_exceptions=True)) == [failure]
            await asyncio.sleep(0)
            assert observed == [("ready_publication_retry_failed", failure)]
        finally:
            await maintenance.close()

    _run(scenario())


def test_publication_retry_timer_observes_exact_deadline_and_saturates_backoff(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def defer_loop_timers(
        loop: asyncio.AbstractEventLoop,
    ) -> list[tuple[float, Callable[..., object], tuple[object, ...], asyncio.TimerHandle]]:
        original_call_at = loop.call_at
        scheduled: list[tuple[float, Callable[..., object], tuple[object, ...], asyncio.TimerHandle]] = []

        def controlled_call_at(
            when: float, callback: Callable[..., object], *args: object, context: Context | None = None
        ) -> asyncio.TimerHandle:
            handle = original_call_at(loop.time() + 3600.0, callback, *args, context=context)
            scheduled.append((when, callback, args, handle))
            return handle

        monkeypatch.setattr(loop, "call_at", controlled_call_at)
        return scheduled

    async def scenario() -> None:
        config = CollectorConfig(retry=RetryConfig(rapid_backoff=(0.1, 0.2)))
        store = _publication_store(tmp_path, config=config)
        now = 100.0
        calls = 0
        monkeypatch.setattr("omi_collector.capture.application.quarantine_maintenance.monotonic", lambda: now)
        monkeypatch.setattr("omi_collector.capture.adapters.staging_store.monotonic", lambda: now)
        scheduled = defer_loop_timers(asyncio.get_running_loop())

        def transient() -> ReadyOutcome:
            nonlocal calls
            calls += 1
            return ReadyOutcome(ReadyOutcomeState.TRANSIENT, reason="storage_busy_or_io")

        monkeypatch.setattr(store, "_recover_and_publish_unlocked", transient)
        maintenance = QuarantineMaintenance(store, None, OpportunisticRuntime(), config=config)
        try:
            await maintenance.ensure_publication_ready()
            assert calls == 1
            assert len(scheduled) == 1
            assert scheduled[0][0] == 100.1
            scheduled[0][1](*scheduled[0][2])
            assert calls == 1
            assert len(scheduled) == 2
            scheduled[0][3].cancel()
            scheduled[1][3].cancel()
            now = scheduled[0][0]
            scheduled[1][1](*scheduled[1][2])
            retry_task = maintenance._publication_retry_task
            assert retry_task is not None
            await retry_task
            assert calls == 2
            assert scheduled[1][0] == now
            assert scheduled[-1][0] == now + config.retry.rapid_backoff[-1]
        finally:
            await maintenance.close()

    _run(scenario())


class _TwoBlockedRecoveries:
    def __init__(self, recover: Callable[[], ReadyOutcome]) -> None:
        self.recover = recover
        self.started = (threading.Event(), threading.Event())
        self.release = (threading.Event(), threading.Event())
        self.finished = (threading.Event(), threading.Event())
        self.lock = threading.Lock()
        self.calls = 0
        self.active = 0
        self.maximum_active = 0

    def __call__(self) -> ReadyOutcome:
        with self.lock:
            self.calls += 1
            index = self.calls - 1
            self.active += 1
            self.maximum_active = max(self.maximum_active, self.active)
        try:
            if index < 2:
                self.started[index].set()
                if not self.release[index].wait(2):
                    raise TimeoutError("publication recovery was not released")
            return self.recover()
        finally:
            if index < 2:
                self.finished[index].set()
            with self.lock:
                self.active -= 1


async def _wait_for_thread_event(event: threading.Event) -> None:
    assert await asyncio.to_thread(event.wait, 1)


async def _exercise_background_coalescing(maintenance: QuarantineMaintenance, recovery: _TwoBlockedRecoveries) -> None:
    try:
        maintenance.schedule_publication_retry()
        await _wait_for_thread_event(recovery.started[0])
        publication = maintenance._publication_retry_task
        assert publication is not None
        maintenance.schedule_publication_retry()
        maintenance.schedule_publication_retry()
        recovery.release[0].set()
        await publication
        assert recovery.calls == 1
        assert recovery.maximum_active == 1
    finally:
        recovery.release[0].set()
        recovery.release[1].set()
        await maintenance.close()


async def _exercise_capture_priority_coalescing(
    maintenance: QuarantineMaintenance, recovery: _TwoBlockedRecoveries
) -> None:
    entering: asyncio.Task[None] | None = None
    try:
        maintenance.schedule_publication_retry()
        await _wait_for_thread_event(recovery.started[0])
        entering = asyncio.create_task(maintenance.enter_capture_priority())
        await asyncio.sleep(0)
        maintenance.schedule_publication_retry()
        assert recovery.calls == 1
        recovery.release[0].set()
        await entering
        await maintenance.exit_capture_priority()
        await _wait_for_thread_event(recovery.started[1])
        publication = maintenance._publication_retry_task
        assert publication is not None
        recovery.release[1].set()
        await _wait_for_thread_event(recovery.finished[1])
        await publication
        assert recovery.calls == 2
        assert recovery.maximum_active == 1
    finally:
        recovery.release[0].set()
        recovery.release[1].set()
        if entering is not None:
            await asyncio.gather(entering, return_exceptions=True)
        await maintenance.close()


def test_background_publication_requests_coalesce_to_one_follow_up_in_both_priorities(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def scenario() -> None:
        store = _publication_store(tmp_path / "background")
        recovery = _TwoBlockedRecoveries(store._recover_and_publish_unlocked)
        monkeypatch.setattr(store, "_recover_and_publish_unlocked", recovery)
        maintenance = QuarantineMaintenance(store, None, OpportunisticRuntime())
        await _exercise_background_coalescing(maintenance, recovery)

        capture_store = _publication_store(tmp_path / "capture")
        capture_recovery = _TwoBlockedRecoveries(capture_store._recover_and_publish_unlocked)
        monkeypatch.setattr(capture_store, "_recover_and_publish_unlocked", capture_recovery)
        capture_maintenance = QuarantineMaintenance(capture_store, None, OpportunisticRuntime())
        await _exercise_capture_priority_coalescing(capture_maintenance, capture_recovery)

    _run(scenario())


@pytest.mark.parametrize("rollback_phase", ["before", "after"])
@pytest.mark.parametrize("sqlite_errorcode", [sqlite3.SQLITE_CORRUPT, sqlite3.SQLITE_NOTADB])
def test_quarantine_preserves_direct_corruption_when_rollback_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    rollback_phase: str,
    sqlite_errorcode: int,
) -> None:
    store = _store(tmp_path)
    _seed_streaming_partial(store, 1)
    source_id = store.pending_attempts()[0].attempt_id
    before = _source_snapshot(store, source_id)
    failure = sqlite3.DatabaseError("direct quarantine corruption")
    failure.sqlite_errorcode = sqlite_errorcode
    rollback_failure = sqlite3.DatabaseError("rollback did not settle")
    rollback_failure.sqlite_errorcode = sqlite3.SQLITE_BUSY
    rollback_failure.sqlite_errorname = "SQLITE_BUSY"
    original_row = quarantine._typed_quarantine_row
    original_rollback = sqlite3.Connection.rollback

    def fail_row(connection: sqlite3.Connection, source: str, lifecycle: str, reason: str, retention: float) -> None:
        original_row(connection, source, lifecycle, reason, retention)
        raise failure

    def failing_rollback(connection: sqlite3.Connection) -> None:
        if rollback_phase == "after":
            original_rollback(connection)
        raise rollback_failure

    monkeypatch.setattr(quarantine, "_typed_quarantine_row", fail_row)
    monkeypatch.setattr(_GuardedConnection, "rollback", failing_rollback)
    with pytest.raises(FatalInternalAudioStorageError) as raised:
        store.quarantine_pending("rollback fault")
    assert raised.value.sqlite_errorcode == sqlite_errorcode
    assert raised.value.__cause__ is failure
    assert failure.__notes__ == [
        "Rollback failed (DatabaseError, SQLite code=5, name=SQLITE_BUSY): rollback did not settle"
    ]
    assert _source_snapshot(store, source_id) == before
    assert store.pending_attempts()[0].attempt_id == source_id


def test_quarantine_unresolved_ordinary_rollback_is_fatal_and_leaves_no_mutation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = _store(tmp_path)
    _seed_streaming_partial(store, 1)
    source_id = store.pending_attempts()[0].attempt_id
    before = _source_snapshot(store, source_id)
    failure = sqlite3.DatabaseError("ordinary quarantine failure")
    rollback_failure = sqlite3.DatabaseError("rollback did not settle")
    rollback_failure.sqlite_errorcode = sqlite3.SQLITE_BUSY
    rollback_failure.sqlite_errorname = "SQLITE_BUSY"
    original_row = quarantine._typed_quarantine_row

    def fail_row(connection: sqlite3.Connection, source: str, lifecycle: str, reason: str, retention: float) -> None:
        original_row(connection, source, lifecycle, reason, retention)
        raise failure

    def failing_rollback(_connection: sqlite3.Connection) -> None:
        raise rollback_failure

    monkeypatch.setattr(quarantine, "_typed_quarantine_row", fail_row)
    monkeypatch.setattr(_GuardedConnection, "rollback", failing_rollback)
    with pytest.raises(FatalInternalAudioStorageError, match="could not be rolled back") as raised:
        store.quarantine_pending("rollback fault")
    assert raised.value.__cause__ is failure
    assert failure.__notes__ == [
        "Rollback failed (DatabaseError, SQLite code=5, name=SQLITE_BUSY): rollback did not settle"
    ]
    assert _source_snapshot(store, source_id) == before
    assert store.pending_attempts()[0].attempt_id == source_id
