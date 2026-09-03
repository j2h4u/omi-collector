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
from omi_collector.capture.application.presence import PresencePolicy, PresenceScheduler, PresenceWake
from omi_collector.capture.application.quarantine_maintenance import (
    PendingStartupState,
    QuarantineMaintenance,
)
from omi_collector.capture.domain.ring_protocol import RECORD_SIZE, ReadBeginNotification
from omi_collector.config import CollectorConfig, StagingRetentionConfig


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
    state = _run(maintenance.prepare_pending_startup())

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

            async def wait_for_attempt(self) -> PresenceWake:
                events.append("scan")
                await release_wake.wait()
                events.append("wake")
                return PresenceWake("advertisement")

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

        task = asyncio.create_task(maintenance.wait_for_presence_attempt(cast(PresenceScheduler, Presence()), bind))
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

            async def wait_for_attempt(self) -> PresenceWake:
                await asyncio.Event().wait()
                raise AssertionError("unreachable")

        store = _store(tmp_path)

        def sweep(_device_slug: str, *, should_defer: Callable[[], bool]) -> tuple[Path, ...]:
            worker_started.set()
            while not should_defer():
                time.sleep(0.001)
            worker_stopped.set()
            return ()

        store.sweep_terminal_retired = sweep  # type: ignore[method-assign]
        maintenance = QuarantineMaintenance(store, "omi", None, OpportunisticRuntime())
        task = asyncio.create_task(
            maintenance.wait_for_presence_attempt(cast(PresenceScheduler, Presence()), lambda _: None)
        )
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
