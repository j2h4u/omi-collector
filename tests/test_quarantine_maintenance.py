from __future__ import annotations

import asyncio
import threading
import time
from collections.abc import Callable
from json import dumps
from pathlib import Path
from struct import pack
from typing import cast

import pytest

from omi_collector.capture.adapters import quarantine as quarantine_module
from omi_collector.capture.adapters.opportunistic_runtime import OpportunisticRuntime
from omi_collector.capture.adapters.staging_store import StagingStore
from omi_collector.capture.application.ports import StagingPort
from omi_collector.capture.application.presence import PresencePolicy, PresenceWake
from omi_collector.capture.application.quarantine_maintenance import (
    PendingStartupState,
    QuarantineMaintenance,
)
from omi_collector.capture.domain.ring_protocol import RECORD_SIZE, ReadBeginNotification
from omi_collector.config import CollectorConfig, RetryConfig, StagingRetentionConfig


def _run(coro: object) -> object:
    return asyncio.run(coro)  # type: ignore[arg-type]


def _store(tmp_path: Path) -> StagingStore:
    return StagingStore(tmp_path, tmp_path.parent / f"{tmp_path.name}-captures")


def _record(value: int) -> bytes:
    return pack(">I", value) + bytes((value % 256,)) * (RECORD_SIZE - 4)


def _seed_streaming_partial(store: StagingStore, count: int) -> bytes:
    attempt = store.prepare_streaming_attempt("omi", 100, count)
    records = b"".join(_record(sequence) for sequence in range(100, 100 + count))
    attempt.record_read_begin(ReadBeginNotification(100, count))
    for index in range(count):
        attempt.append_record(index, 100 + index, records[index * RECORD_SIZE : (index + 1) * RECORD_SIZE])
    attempt.checkpoint()
    attempt.close(durable=True)
    return records


def test_pending_startup_result_including_no_pending_is_memoized(tmp_path: Path, monkeypatch: object) -> None:
    store = _store(tmp_path)
    calls = 0
    original = store.pending_attempts

    def pending(device_slug: str) -> tuple[object, ...]:
        nonlocal calls
        calls += 1
        return original(device_slug)

    monkeypatch.setattr(store, "pending_attempts", pending)  # type: ignore[attr-defined]
    maintenance = QuarantineMaintenance(store, "omi", None, OpportunisticRuntime())

    first = _run(maintenance.prepare_pending_startup())
    second = _run(maintenance.prepare_pending_startup())

    assert isinstance(first, PendingStartupState)
    assert first is second
    assert first.pending is None
    assert first.durable_next is None
    assert calls == 1


def test_attributable_malformed_startup_evidence_is_quarantined_before_collection(tmp_path: Path) -> None:
    store = _store(tmp_path)
    malformed = tmp_path / "attempts" / ("f" * 32)
    malformed.mkdir(parents=True)
    (malformed / "attempt.json").write_text(
        dumps({"attempt_id": malformed.name, "device_slug": "omi"}), encoding="utf-8"
    )
    (malformed / "records.bin").write_bytes(b"preserve")

    maintenance = QuarantineMaintenance(store, "omi", None, OpportunisticRuntime())
    state = cast(PendingStartupState, _run(maintenance.prepare_pending_startup()))

    assert state == PendingStartupState(None, None)
    assert not malformed.exists()
    quarantined = tuple((tmp_path / "quarantine" / "omi").iterdir())
    assert len(quarantined) == 2
    source = next(path for path in quarantined if path.is_dir())
    assert (source / "records.bin").read_bytes() == b"preserve"


def test_deferred_maintenance_is_retried_without_touching_quarantine(tmp_path: Path) -> None:
    store = _store(tmp_path)
    maintenance = QuarantineMaintenance(store, "omi", None, OpportunisticRuntime())
    calls = 0
    original = store.sweep_terminal_retired

    def sweep(device_slug: str, *, should_defer: Callable[[], bool]) -> tuple[Path, ...]:
        nonlocal calls
        calls += 1
        return original(device_slug, should_defer=should_defer)

    store.sweep_terminal_retired = sweep  # type: ignore[method-assign]
    _run(maintenance.run_once(lambda: True))
    assert calls == 0
    _run(maintenance.run_once(lambda: False))
    assert calls == 1


def test_deferred_quarantine_is_retried_without_changing_source(tmp_path: Path) -> None:
    async def scenario() -> None:
        store = _store(tmp_path)
        expected = _seed_streaming_partial(store, count=2)
        attempt_id = next((tmp_path / "attempts").iterdir()).name
        source = store.quarantine_attempt_source("omi", attempt_id)
        before = {path.name: path.read_bytes() for path in source.iterdir()}

        maintenance = QuarantineMaintenance(store, "omi", None, OpportunisticRuntime())
        await maintenance.run_once(lambda: True)

        assert {path.name: path.read_bytes() for path in source.iterdir()} == before
        await maintenance.run_once(lambda: False)
        assert (source / "published.json").is_file()
        bundles = tuple((store.capture_root / "omi").iterdir())
        assert len(bundles) == 1
        assert (bundles[0] / "records.bin").read_bytes() == expected

    _run(scenario())


def test_pending_startup_hydration_is_reused_by_lease_bound_resume(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = _store(tmp_path)
    _seed_streaming_partial(store, count=2)
    monkeypatch.setattr(
        Path,
        "read_bytes",
        lambda _path: (_ for _ in ()).throw(AssertionError("resume hydration must stream raw evidence")),
    )
    maintenance = QuarantineMaintenance(store, "omi", None, OpportunisticRuntime())
    state = cast(PendingStartupState, _run(maintenance.prepare_pending_startup()))
    assert state.pending is not None
    with store.device_lock("omi") as lease:
        resumed = store.resume_streaming_attempt("omi", lease)
        assert resumed is not None
        resumed.close()


def test_pending_startup_inspection_does_not_promote_tail_without_lease(tmp_path: Path) -> None:
    store = _store(tmp_path)
    attempt = store.prepare_streaming_attempt("omi", 100, 3)
    attempt.record_read_begin(ReadBeginNotification(100, 3))
    first, second = _record(1), _record(2)
    attempt.append_record(0, 100, first)
    attempt.checkpoint()
    attempt.append_record(1, 101, second)
    attempt.close(durable=True)
    checkpoint = (attempt.path / "checkpoint.json").read_text(encoding="utf-8")
    raw = (attempt.path / "records.bin").read_bytes()

    state = cast(
        PendingStartupState,
        _run(QuarantineMaintenance(store, "omi", None, OpportunisticRuntime()).prepare_pending_startup()),
    )

    assert state.durable_next == 101
    assert (attempt.path / "checkpoint.json").read_text(encoding="utf-8") == checkpoint
    assert (attempt.path / "records.bin").read_bytes() == raw


def test_retryable_quarantine_publication_observes_configured_cooldown(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = _store(tmp_path)
    expected = _seed_streaming_partial(store, count=2)
    attempt_id = next((tmp_path / "attempts").iterdir()).name
    source = store.quarantine_attempt_source("omi", attempt_id)
    now = 100.0
    monkeypatch.setattr("omi_collector.capture.application.quarantine_maintenance.monotonic", lambda: now)
    config = CollectorConfig(
        retry=RetryConfig(maintenance_interval_seconds=1.0, quarantine_publish_backoff_seconds=(5.0,))
    )
    runtime = OpportunisticRuntime()
    original_publish = runtime.publish_quarantined_prefix
    failures = 0

    def fail_once(
        source_path: Path,
        staging_port: StagingPort,
        slug: str,
        *,
        should_defer: Callable[[], bool],
    ) -> object:
        nonlocal failures
        failures += 1
        if failures == 1:
            raise OSError("transient publication failure")
        return original_publish(source_path, staging_port, slug, should_defer=should_defer)

    monkeypatch.setattr(runtime, "publish_quarantined_prefix", fail_once)
    maintenance = QuarantineMaintenance(store, "omi", None, runtime, config=config)
    _run(maintenance.run_once(lambda: False))
    _run(maintenance.run_once(lambda: False))
    assert failures == 1
    assert not (source / "published.json").exists()
    now += 5.0
    _run(maintenance.run_once(lambda: False))
    assert failures == 2
    assert (source / "published.json").is_file()
    bundles = tuple((store.capture_root / "omi").iterdir())
    assert (bundles[0] / "records.bin").read_bytes() == expected


def test_quarantine_retry_cooldown_does_not_block_terminal_sweeps(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = _store(tmp_path)
    _seed_streaming_partial(store, count=2)
    now = 100.0
    monkeypatch.setattr("omi_collector.capture.application.quarantine_maintenance.monotonic", lambda: now)
    config = CollectorConfig(retry=RetryConfig(maintenance_interval_seconds=1.0))
    runtime = OpportunisticRuntime()
    terminal_sweeps = 0
    salvage_scans = 0
    original_terminal_sweep = store.sweep_terminal_retired

    def record_terminal_sweep(device_slug: str, *, should_defer: Callable[[], bool]) -> tuple[Path, ...]:
        nonlocal terminal_sweeps
        terminal_sweeps += 1
        return original_terminal_sweep(device_slug, should_defer=should_defer)

    def forbidden_salvage_scan(device_slug: str, *, should_defer: Callable[[], bool]) -> tuple[Path, ...]:
        del device_slug, should_defer
        nonlocal salvage_scans
        salvage_scans += 1
        raise AssertionError("salvage scan reached during retry cooldown")

    maintenance = QuarantineMaintenance(store, "omi", None, runtime, config=config)
    maintenance._quarantine_retry_not_before = 105.0
    monkeypatch.setattr(store, "sweep_terminal_retired", record_terminal_sweep)
    monkeypatch.setattr(store, "quarantined_attempts", forbidden_salvage_scan)

    _run(maintenance.run_once(lambda: False))
    assert terminal_sweeps == 1
    assert salvage_scans == 0
    now += 1.0
    _run(maintenance.run_once(lambda: False))
    assert terminal_sweeps == 2
    assert salvage_scans == 0


def test_quarantine_pending_keeps_valid_partial_salvageable_and_expires_opaque(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    now = 1_000_000_000
    monkeypatch.setattr(quarantine_module, "_wall_clock_ns", lambda: now)
    store = StagingStore(
        tmp_path,
        tmp_path.parent / f"{tmp_path.name}-captures",
        config=CollectorConfig(staging_retention=StagingRetentionConfig(terminal_retention_seconds=72.0)),
    )
    expected = _seed_streaming_partial(store, count=2)
    attempt_id = next((tmp_path / "attempts").iterdir()).name
    malformed = tmp_path / "attempts" / ("f" * 32)
    malformed.mkdir()
    moved = store.quarantine_pending("omi", "ambiguous recovery evidence")

    source = next(path for path in moved if path.name.startswith(attempt_id))
    opaque = next(path for path in moved if path != source)
    assert not (source.with_name(f"{source.name}.json")).exists()
    assert (opaque.with_name(f"{opaque.name}.json")).is_file()
    assert store.quarantined_attempts("omi") == (source,)

    _run(QuarantineMaintenance(store, "omi", None, OpportunisticRuntime()).run_once(lambda: False))
    assert (source / "published.json").is_file()
    bundles = tuple((store.capture_root / "omi").iterdir())
    assert len(bundles) == 1
    assert (bundles[0] / "records.bin").read_bytes() == expected

    now += 72_000_000_000
    assert set(store.sweep_terminal_quarantine("omi")) == {source, opaque}
    assert not source.exists()
    assert not opaque.exists()


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

        def pending(device_slug: str) -> tuple[object, ...]:
            events.append("startup")
            return original(device_slug)

        store.pending_attempts = pending  # type: ignore[method-assign]
        maintenance = QuarantineMaintenance(store, "omi", None, OpportunisticRuntime())

        def bind(state: PendingStartupState) -> None:
            assert state.pending is None
            events.append("bind")
            startup_bound.set()

        task = asyncio.create_task(maintenance.wait_for_presence_attempt(Presence(), bind))
        await startup_bound.wait()
        release_wake.set()
        wake = await task

        assert wake.reason == "advertisement"
        assert events.index("scan") < events.index("startup") < events.index("bind") < events.index("wake")

    _run(scenario())


def test_presence_maintenance_cancellation_joins_cooperative_worker(tmp_path: Path) -> None:
    async def scenario() -> None:
        worker_started = threading.Event()
        worker_stopped = threading.Event()

        class Presence:
            policy = PresencePolicy(rapid_backoff=(0.001,))
            closed = False

            async def wait_for_attempt(self) -> PresenceWake:
                await asyncio.Event().wait()
                raise AssertionError("unreachable")

            async def close(self) -> None:
                self.closed = True

        store = _store(tmp_path)

        def sweep(_device_slug: str, *, should_defer: Callable[[], bool]) -> tuple[Path, ...]:
            worker_started.set()
            while not should_defer():
                time.sleep(0.001)
            worker_stopped.set()
            return ()

        store.sweep_terminal_retired = sweep  # type: ignore[method-assign]
        maintenance = QuarantineMaintenance(store, "omi", None, OpportunisticRuntime())
        task = asyncio.create_task(maintenance.wait_for_presence_attempt(Presence(), lambda _: None))
        assert await asyncio.to_thread(worker_started.wait, 1)
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        else:
            raise AssertionError("cancellation was swallowed")
        assert worker_stopped.is_set()

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

        maintenance = QuarantineMaintenance(_store(tmp_path), "omi", None, OpportunisticRuntime())
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

        maintenance = QuarantineMaintenance(_store(tmp_path), "omi", None, OpportunisticRuntime())
        maintenance._prepare_and_run = wait_for_deferral  # type: ignore[method-assign]
        presence = Presence()
        task = asyncio.create_task(maintenance.wait_for_presence_attempt(presence, lambda _: None))
        await started.wait()
        release_permit.set()
        await deferral_started.wait()
        task.cancel()
        finish_maintenance.set()

        with pytest.raises(asyncio.CancelledError):
            await task
        assert presence.closed

    _run(scenario())
