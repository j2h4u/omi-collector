from __future__ import annotations

import asyncio
import threading
import time
from collections.abc import Callable
from json import dumps, loads
from pathlib import Path
from struct import pack
from typing import cast

import pytest

from omi_collector.capture.adapters import quarantine as quarantine_module
from omi_collector.capture.adapters.opportunistic_runtime import OpportunisticRuntime
from omi_collector.capture.adapters.staging_contract import DeviceAlreadyRunningError
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
    attempt = store.prepare_streaming_attempt(100, count)
    records = b"".join(_record(sequence) for sequence in range(100, 100 + count))
    attempt.record_read_begin(ReadBeginNotification(100, count))
    for index in range(count):
        attempt.accept_chunk(100 + index, records[index * RECORD_SIZE : (index + 1) * RECORD_SIZE])
    attempt.checkpoint()
    attempt.close(durable=True)
    return records


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


def test_attributable_malformed_startup_evidence_is_quarantined_before_collection(tmp_path: Path) -> None:
    store = _store(tmp_path)
    malformed = tmp_path / "attempts" / ("f" * 32)
    malformed.mkdir(parents=True)
    (malformed / "attempt.json").write_text(
        dumps({"attempt_id": malformed.name, "schema_version": 2}), encoding="utf-8"
    )
    (malformed / "records.bin").write_bytes(b"preserve")

    maintenance = QuarantineMaintenance(store, None, OpportunisticRuntime())
    state = cast(PendingStartupState, _run(maintenance.prepare_pending_startup()))

    assert state == PendingStartupState(None, None)
    assert not malformed.exists()
    quarantined = tuple((tmp_path / "quarantine").iterdir())
    assert len(quarantined) == 2
    source = next(path for path in quarantined if path.is_dir())
    assert (source / "records.bin").read_bytes() == b"preserve"


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
    assert tuple(store.attempts_root.iterdir()) == ()
    quarantined_sources = tuple(path for path in (tmp_path / "quarantine").iterdir() if path.is_dir())
    assert len(quarantined_sources) == 2
    assert {(source / "records.bin").read_bytes() for source in quarantined_sources} == expected_records


def test_deferred_maintenance_is_retried_without_touching_quarantine(tmp_path: Path) -> None:
    store = _store(tmp_path)
    maintenance = QuarantineMaintenance(store, None, OpportunisticRuntime())
    calls: list[str] = []
    methods = (
        "recover_and_publish",
        "sweep_terminal_retired",
        "sweep_terminal_quarantine",
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
        "sweep_terminal_retired",
        "sweep_terminal_quarantine",
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


def test_deferred_quarantine_is_retried_without_changing_source(tmp_path: Path) -> None:
    async def scenario() -> None:
        store = _store(tmp_path)
        expected = _seed_streaming_partial(store, count=2)
        attempt_id = next((tmp_path / "attempts").iterdir()).name
        source = store.quarantine_attempt_source(attempt_id)
        before = {path.name: path.read_bytes() for path in source.iterdir()}

        maintenance = QuarantineMaintenance(store, None, OpportunisticRuntime())
        await maintenance.run_once(lambda: True)

        assert {path.name: path.read_bytes() for path in source.iterdir()} == before
        await maintenance.run_once(lambda: False)
        assert (source / "published.json").is_file()
        bundles = tuple((store.capture_root).iterdir())
        assert len(bundles) == 1
        assert (bundles[0] / "records.bin").read_bytes() == expected

    _run(scenario())


def test_failed_quarantine_published_marker_is_retried_after_restart(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = _store(tmp_path)
    expected = _seed_streaming_partial(store, count=2)
    store.quarantine_pending("restart recovery")
    source = next(path for path in (tmp_path / "quarantine").iterdir() if path.is_dir())
    original_mark = store.mark_quarantine_published
    failures = 0

    def fail_once(path: Path) -> None:
        nonlocal failures
        failures += 1
        if failures == 1:
            raise OSError("marker write failed")
        original_mark(path)

    monkeypatch.setattr(store, "mark_quarantine_published", fail_once)
    _run(QuarantineMaintenance(store, None, OpportunisticRuntime()).run_once(lambda: False))

    assert failures == 1
    assert source.is_dir()
    assert not (source / "published.json").exists()
    bundles = tuple(store.capture_root.iterdir())
    assert len(bundles) == 1
    assert (bundles[0] / "records.bin").read_bytes() == expected

    restarted = StagingStore(tmp_path, store.capture_root)
    _run(QuarantineMaintenance(restarted, None, OpportunisticRuntime()).run_once(lambda: False))

    assert (source / "published.json").is_file()
    assert tuple(restarted.capture_root.iterdir()) == bundles
    assert (bundles[0] / "records.bin").read_bytes() == expected


def test_invalid_terminal_marker_is_reauthenticated_but_never_replaced_or_expired(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = _store(tmp_path)
    expected = _seed_streaming_partial(store, count=2)
    attempt_id = next((tmp_path / "attempts").iterdir()).name
    source = store.quarantine_attempt_source(attempt_id)
    invalid_marker = source / "published.json"
    invalid_marker.write_bytes(b"{invalid json")
    original_marker = invalid_marker.read_bytes()

    assert store.quarantined_attempts() == (source,)
    _run(QuarantineMaintenance(store, None, OpportunisticRuntime()).run_once(lambda: False))
    bundles = tuple(store.capture_root.iterdir())
    assert len(bundles) == 1
    assert (bundles[0] / "records.bin").read_bytes() == expected
    assert invalid_marker.read_bytes() == original_marker
    assert source.is_dir()

    restarted = StagingStore(tmp_path, store.capture_root)
    _run(QuarantineMaintenance(restarted, None, OpportunisticRuntime()).run_once(lambda: False))
    assert tuple(restarted.capture_root.iterdir()) == bundles
    assert invalid_marker.read_bytes() == original_marker

    monkeypatch.setattr(quarantine_module, "_wall_clock_ns", lambda: 10**18)
    assert restarted.sweep_terminal_quarantine() == ()
    assert source.is_dir()


def test_terminal_state_must_match_marker_filename_before_retention(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = _store(tmp_path)
    _seed_streaming_partial(store, count=1)
    attempt_id = next((tmp_path / "attempts").iterdir()).name
    source = store.quarantine_attempt_source(attempt_id)
    marker = source / "published.json"
    marker.write_text(
        dumps(
            {
                "version": 1,
                "state": "unprocessable",
                "classified_at_unix_ns": 1,
                "reason": "wrong marker file",
            }
        ),
        encoding="utf-8",
    )
    original_marker = marker.read_bytes()
    monkeypatch.setattr(quarantine_module, "_wall_clock_ns", lambda: 10**18)

    assert store.sweep_terminal_quarantine() == ()
    assert source.is_dir()
    assert marker.read_bytes() == original_marker


def test_sidecar_must_match_quarantined_entry_and_integer_version(tmp_path: Path) -> None:
    store = _store(tmp_path)
    root = tmp_path / "quarantine"
    root.mkdir()
    source = root / f"opaque-entry-{'a' * 32}"
    source.mkdir()
    sidecar = source.with_name(f"{source.name}.json")
    sidecar.write_text(
        dumps(
            {
                "version": True,
                "state": "unprocessable",
                "classified_at_unix_ns": 0,
                "reason": "wrong source",
                "original_name": "someone-elses-entry",
            }
        ),
        encoding="utf-8",
    )
    original_sidecar = sidecar.read_bytes()

    assert store.sweep_terminal_quarantine() == ()
    assert source.is_dir()
    assert sidecar.read_bytes() == original_sidecar


def test_directly_classified_unsafe_quarantine_entry_expires_after_retention(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    now = 1_000_000_000
    monkeypatch.setattr(quarantine_module, "_wall_clock_ns", lambda: now)
    store = StagingStore(
        tmp_path,
        tmp_path.parent / f"{tmp_path.name}-captures",
        config=CollectorConfig(staging_retention=StagingRetentionConfig(terminal_retention_seconds=72.0)),
    )
    unsafe = tmp_path / "quarantine" / "unsafe-entry"
    unsafe.parent.mkdir()
    unsafe.write_bytes(b"preserve until terminal retention")
    name_collision = tmp_path / "quarantine" / "published"
    name_collision.write_bytes(b"same-name sidecar collision")

    assert store.sweep_terminal_quarantine() == ()
    assert unsafe.with_name("unsafe-entry.json").is_file()
    assert name_collision.with_name("published.json").is_file()
    now += 72_000_000_000

    assert set(store.sweep_terminal_quarantine()) == {unsafe, name_collision}
    assert not unsafe.exists()
    assert not name_collision.exists()


def test_terminal_marker_named_like_its_parent_is_not_mistaken_for_sidecar(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    now = 1_000_000_000
    monkeypatch.setattr(quarantine_module, "_wall_clock_ns", lambda: now)
    store = StagingStore(
        tmp_path,
        tmp_path.parent / f"{tmp_path.name}-captures",
        config=CollectorConfig(staging_retention=StagingRetentionConfig(terminal_retention_seconds=72.0)),
    )
    source = tmp_path / "quarantine" / "published"
    source.mkdir(parents=True)
    store.mark_quarantine_published(source)
    now += 72_000_000_000

    assert store.sweep_terminal_quarantine() == (source,)
    assert not source.exists()


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


def test_pending_startup_establishes_aligned_tail_under_a_lease_before_binding(tmp_path: Path) -> None:
    store = _store(tmp_path)
    attempt = store.prepare_streaming_attempt(100, 3)
    attempt.record_read_begin(ReadBeginNotification(100, 3))
    first, second = _record(1), _record(2)
    attempt.accept_chunk(100, first)
    attempt.checkpoint()
    attempt.accept_chunk(101, second)
    attempt.close(durable=True)
    checkpoint = (attempt.path / "checkpoint.json").read_text(encoding="utf-8")
    raw = (attempt.path / "records.bin").read_bytes()

    state = cast(
        PendingStartupState,
        _run(QuarantineMaintenance(store, None, OpportunisticRuntime()).prepare_pending_startup()),
    )

    assert state.durable_next == 102
    assert (attempt.path / "checkpoint.json").read_text(encoding="utf-8") != checkpoint
    assert loads((attempt.path / "checkpoint.json").read_text(encoding="utf-8"))["record_count"] == 2
    assert (attempt.path / "records.bin").read_bytes() == raw


def test_pending_startup_uses_authenticated_aligned_tail_when_promotion_lock_is_busy(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = _store(tmp_path)
    attempt = store.prepare_streaming_attempt(100, 3)
    attempt.record_read_begin(ReadBeginNotification(100, 3))
    first, second = _record(100), _record(101)
    attempt.accept_chunk(100, first)
    attempt.checkpoint()
    attempt.accept_chunk(101, second)
    attempt.close(durable=True)
    checkpoint_before = (attempt.path / "checkpoint.json").read_bytes()
    raw_before = (attempt.path / "records.bin").read_bytes()
    maintenance = QuarantineMaintenance(store, None, OpportunisticRuntime())

    def activation_busy(_descriptor: object) -> int:
        raise DeviceAlreadyRunningError("another writer owns the promotion lease")

    monkeypatch.setattr(maintenance, "_activate_pending_frontier", activation_busy)
    state = cast(PendingStartupState, _run(maintenance.prepare_pending_startup()))

    assert state.pending is not None
    assert type(state.durable_next) is int
    assert state.durable_next == 102
    assert (attempt.path / "records.bin").read_bytes() == raw_before == first + second
    assert (attempt.path / "checkpoint.json").read_bytes() == checkpoint_before
    assert loads(checkpoint_before.decode("utf-8"))["record_count"] == 1


@pytest.mark.parametrize("promotion_busy", [False, True])
def test_complete_pending_startup_keeps_authenticated_tail_at_packet_count_boundary(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, promotion_busy: bool
) -> None:
    store = _store(tmp_path)
    expected = _seed_streaming_partial(store, count=2)
    attempt_id = next((tmp_path / "attempts").iterdir()).name
    attempt_path = tmp_path / "attempts" / attempt_id
    checkpoint_before = (attempt_path / "checkpoint.json").read_bytes()
    raw_before = (attempt_path / "records.bin").read_bytes()
    original_lock = store.device_lock

    if promotion_busy:

        def busy_only_during_pending_promotion(
            *, recover_capture_temporaries: bool = True, operation: str = "unknown"
        ) -> object:
            if operation == "resume_pending_attempt":
                raise DeviceAlreadyRunningError("another writer owns the promotion lease")
            return original_lock(recover_capture_temporaries=recover_capture_temporaries, operation=operation)

        monkeypatch.setattr(store, "device_lock", busy_only_during_pending_promotion)

    maintenance = QuarantineMaintenance(store, None, OpportunisticRuntime())
    try:
        state = cast(PendingStartupState, _run(maintenance.prepare_pending_startup()))
        assert state.pending is not None
        assert state.pending.attempt_id == attempt_id
        assert state.durable_next == state.pending.start_sequence + state.pending.packet_count
        assert raw_before == expected
        assert (attempt_path / "records.bin").read_bytes() == raw_before
        if promotion_busy:
            assert (attempt_path / "checkpoint.json").read_bytes() == checkpoint_before
        else:
            assert loads((attempt_path / "checkpoint.json").read_text(encoding="utf-8"))["record_count"] == 2
        assert not (tmp_path / "quarantine").exists()

        monkeypatch.setattr(store, "device_lock", original_lock)
        with store.device_lock(operation="resume_pending_attempt") as lease:
            resumed = store.resume_streaming_attempt(lease)
            assert resumed is not None
            resumed.close()
    finally:
        _run(maintenance.close())


def test_pending_startup_requests_prefix_closure_without_mutating_visit(tmp_path: Path) -> None:
    store = _store(tmp_path)
    _seed_streaming_partial(store, count=2)
    attempt = store.open_attempt(next((tmp_path / "attempts").iterdir()).name)
    with store.device_lock() as lease:
        attempt.activate_for_resume(lease)
        publication = attempt.publish_prefix()
        assert publication is not None
        attempt.close(durable=True)
    publication.bundle_path.rename(tmp_path / "interrupted-ready")

    state = cast(
        PendingStartupState, _run(QuarantineMaintenance(store, None, OpportunisticRuntime()).prepare_pending_startup())
    )

    assert state == PendingStartupState(None, None, "needs_interrupted_close")
    assert not (attempt.path / "terminal-retired.json").exists()
    assert not store.ready_closures_path.exists()


def test_presence_startup_state_binds_once_after_a_completed_attempt(tmp_path: Path) -> None:
    async def scenario() -> None:
        class Presence:
            async def wait_for_attempt(self) -> PresenceWake:
                return PresenceWake("advertisement")

            async def close(self) -> None:
                return None

        store = _store(tmp_path)
        _seed_streaming_partial(store, count=1)
        maintenance = QuarantineMaintenance(store, None, OpportunisticRuntime())
        bound: list[PendingStartupState] = []

        await maintenance.wait_for_presence_attempt(Presence(), bound.append)
        pending = bound[0].pending
        assert pending is not None
        with store.device_lock() as lease:
            completed = store.resume_streaming_attempt(lease)
            assert completed is not None
            assert completed.publish_prefix() is not None
            completed.close(durable=True)
        store.terminalize_prefix_attempt(pending.attempt_id)
        assert store.pending_attempts() == ()

        # A second wake must not bind maintenance's cached, now-completed
        # descriptor back into the reconciler.
        await maintenance.wait_for_presence_attempt(Presence(), bound.append)

        assert len(bound) == 1
        assert bound[0].pending is not None

    _run(scenario())


def test_retryable_quarantine_publication_observes_configured_cooldown(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = _store(tmp_path)
    expected = _seed_streaming_partial(store, count=2)
    attempt_id = next((tmp_path / "attempts").iterdir()).name
    source = store.quarantine_attempt_source(attempt_id)
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
        *,
        should_defer: Callable[[], bool],
    ) -> object:
        nonlocal failures
        failures += 1
        if failures == 1:
            raise OSError("transient publication failure")
        return original_publish(source_path, staging_port, should_defer=should_defer)

    monkeypatch.setattr(runtime, "publish_quarantined_prefix", fail_once)
    maintenance = QuarantineMaintenance(store, None, runtime, config=config)
    _run(maintenance.run_once(lambda: False))
    now = 101.0
    _run(maintenance.run_once(lambda: False))
    assert failures == 1
    assert not (source / "published.json").exists()
    now = 105.0
    _run(maintenance.run_once(lambda: False))
    assert failures == 2
    assert (source / "published.json").is_file()
    bundles = tuple((store.capture_root).iterdir())
    assert (bundles[0] / "records.bin").read_bytes() == expected


def test_failed_clock_publication_does_not_gate_ble_and_retries_locally(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        store = _store(tmp_path)
        attempts: list[int] = []

        def recover_and_publish() -> None:
            attempts.append(1)
            if len(attempts) < 3:
                raise OSError("transient local publication failure")

        store.recover_and_publish = recover_and_publish  # type: ignore[method-assign]
        config = CollectorConfig(retry=RetryConfig(rapid_backoff=(0.001,)))
        maintenance = QuarantineMaintenance(store, None, OpportunisticRuntime(), config=config)

        await maintenance.ensure_publication_ready()
        first_return = len(attempts)
        await asyncio.sleep(0.01)

        assert first_return == len(config.retry.rapid_backoff) + 1
        assert len(attempts) >= 3

    _run(scenario())


@pytest.mark.parametrize(
    ("successful_result", "success_event"),
    [(None, "ready_publication_recovered"), (("ready",), "ready_publication_published")],
)
def test_successful_recovery_after_device_contention_clears_blocked_state(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    successful_result: object | None,
    success_event: str,
) -> None:
    async def scenario() -> None:
        store = _store(tmp_path)
        attempts = 0
        events: list[tuple[str, str]] = []

        def recover_and_publish() -> object | None:
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                raise DeviceAlreadyRunningError("recovery is already active")
            return successful_result

        store.recover_and_publish = recover_and_publish  # type: ignore[method-assign]
        runtime = OpportunisticRuntime()
        monkeypatch.setattr(
            runtime,
            "debug_exception",
            lambda event, _error, **_fields: events.append(("exception", event)),
        )
        monkeypatch.setattr(runtime, "debug_event", lambda event, **_fields: events.append(("event", event)))
        config = CollectorConfig(retry=RetryConfig(rapid_backoff=(0.001,)))
        maintenance = QuarantineMaintenance(store, None, runtime, config=config)

        assert await maintenance.ensure_publication_ready() is None

        assert attempts == 2
        assert events == [
            ("exception", "ready_publication_blocked"),
            ("event", success_event),
        ]

    _run(scenario())


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

    def record_terminal_sweep(*, should_defer: Callable[[], bool]) -> tuple[Path, ...]:
        nonlocal terminal_sweeps
        terminal_sweeps += 1
        return original_terminal_sweep(should_defer=should_defer)

    def forbidden_salvage_scan(*, should_defer: Callable[[], bool]) -> tuple[Path, ...]:
        del should_defer
        nonlocal salvage_scans
        salvage_scans += 1
        raise AssertionError("salvage scan reached during retry cooldown")

    maintenance = QuarantineMaintenance(store, None, runtime, config=config)
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
    moved = store.quarantine_pending("ambiguous recovery evidence")

    source = next(path for path in moved if path.name.startswith(attempt_id))
    opaque = next(path for path in moved if path != source)
    assert not (source.with_name(f"{source.name}.json")).exists()
    assert (opaque.with_name(f"{opaque.name}.json")).is_file()
    assert store.quarantined_attempts() == (source,)

    _run(QuarantineMaintenance(store, None, OpportunisticRuntime()).run_once(lambda: False))
    assert (source / "published.json").is_file()
    bundles = tuple((store.capture_root).iterdir())
    assert len(bundles) == 1
    assert (bundles[0] / "records.bin").read_bytes() == expected

    now += 72_000_000_000
    assert set(store.sweep_terminal_quarantine()) == {source, opaque}
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

        def sweep(*, should_defer: Callable[[], bool]) -> tuple[Path, ...]:
            worker_started.set()
            while not should_defer():
                time.sleep(0.001)
            worker_stopped.set()
            return ()

        store.sweep_terminal_retired = sweep  # type: ignore[method-assign]
        maintenance = QuarantineMaintenance(store, None, OpportunisticRuntime())
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


def test_publication_shutdown_joins_mutation_after_repeated_cancellation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def scenario() -> None:
        started = threading.Event()
        release = threading.Event()
        finished = threading.Event()
        store = _store(tmp_path)
        publications: list[None] = []

        def publish() -> None:
            publications.append(None)
            started.set()
            if not release.wait(2):
                raise TimeoutError("publication mutation was not released")
            finished.set()

        monkeypatch.setattr(store, "recover_and_publish", publish)
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
            assert publications == [None]
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
            assert publications == [None]
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
        started = threading.Event()
        release = threading.Event()
        finished = threading.Event()
        calls: list[int] = []
        store = _store(tmp_path)

        def publish() -> None:
            calls.append(1)
            if len(calls) == 1:
                started.set()
                if not release.wait(2):
                    raise TimeoutError("publication mutation was not released")
                finished.set()

        monkeypatch.setattr(store, "recover_and_publish", publish)
        maintenance = QuarantineMaintenance(store, None, OpportunisticRuntime())
        maintenance.schedule_publication_retry()
        try:
            assert await asyncio.to_thread(started.wait, 1)
            entering = asyncio.create_task(maintenance.enter_capture_priority())
            await asyncio.sleep(0)
            maintenance.schedule_publication_retry()
            assert not entering.done()
            assert calls == [1]
            release.set()
            await entering
            assert finished.is_set()
            await asyncio.sleep(0)
            assert calls == [1]
            maintenance.exit_capture_priority()
            for _ in range(20):
                if len(calls) == 2:
                    break
                await asyncio.sleep(0.001)
            assert calls == [1, 1]
            await maintenance.close()
            maintenance.schedule_publication_retry()
            await asyncio.sleep(0)
            assert calls == [1, 1]
        finally:
            release.set()
            await maintenance.close()

    _run(scenario())


def test_capture_priority_defers_pending_publication_timer_until_exit(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def scenario() -> None:
        store = _store(tmp_path)
        published = asyncio.Event()
        loop = asyncio.get_running_loop()
        calls: list[None] = []

        def publish() -> None:
            calls.append(None)
            loop.call_soon_threadsafe(published.set)

        monkeypatch.setattr(store, "recover_and_publish", publish)
        maintenance = QuarantineMaintenance(store, None, OpportunisticRuntime())
        maintenance.schedule_publication_retry()
        try:
            await maintenance.enter_capture_priority()
            assert calls == []

            maintenance.exit_capture_priority()
            await asyncio.wait_for(published.wait(), timeout=1)
            assert calls == [None]
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
        store = _store(tmp_path)

        def publish() -> None:
            started.set()
            if not release.wait(2):
                raise TimeoutError("publication mutation was not released")
            finished.set()

        monkeypatch.setattr(store, "recover_and_publish", publish)
        maintenance = QuarantineMaintenance(store, None, OpportunisticRuntime())
        maintenance.schedule_publication_retry()
        entering: asyncio.Task[None] | None = None
        try:
            assert await asyncio.to_thread(started.wait, 1)

            async def enter() -> None:
                try:
                    await maintenance.enter_capture_priority()
                finally:
                    maintenance.exit_capture_priority()

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
        store = _store(tmp_path)
        store.append_ready_closure(11, "drained")
        original = store.begin_ready_visit
        maintenance = QuarantineMaintenance(store, None, OpportunisticRuntime())
        monkeypatch.setattr(store, "begin_ready_visit", lambda: (_ for _ in ()).throw(OSError("storage unavailable")))

        with pytest.raises(OSError, match="storage unavailable"):
            await maintenance.enter_capture_priority()
        assert maintenance._publication_priority.name == "BACKGROUND_ALLOWED"
        assert loads(store.ready_closures_path.read_text())["closures"][0]["reason"] == "drained"

        monkeypatch.setattr(store, "begin_ready_visit", original)
        await maintenance.enter_capture_priority()
        assert loads(store.ready_closures_path.read_text())["closures"][0]["reason"] == "collecting"
        maintenance.exit_capture_priority()
        await maintenance.close()

    _run(scenario())


def test_cancelled_visit_begin_joins_storage_before_releasing_gate(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def scenario() -> None:
        store = _store(tmp_path)
        store.append_ready_closure(11, "drained")
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
            assert maintenance._publication_priority.name == "BACKGROUND_ALLOWED"
            assert loads(store.ready_closures_path.read_text())["closures"][0]["reason"] == "collecting"
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

            async def wait_for_attempt(self) -> PresenceWake:
                await asyncio.Event().wait()
                raise AssertionError("unreachable")

            async def close(self) -> None:
                return None

        def sweep(*, should_defer: Callable[[], bool]) -> tuple[Path, ...]:
            del should_defer
            started.set()
            if not release.wait(2):
                raise TimeoutError("maintenance mutation was not released")
            finished.set()
            return ()

        store = _store(tmp_path)
        monkeypatch.setattr(store, "sweep_terminal_retired", sweep)
        maintenance = QuarantineMaintenance(store, None, OpportunisticRuntime())
        owner = asyncio.create_task(maintenance.wait_for_presence_attempt(Presence(), lambda _: None))
        try:
            assert await asyncio.to_thread(started.wait, 1)
            owner.cancel()
            await asyncio.sleep(0.01)
            owner.cancel()
            await asyncio.sleep(0.01)
            assert not owner.done()
            release.set()
            with pytest.raises(asyncio.CancelledError):
                await owner
            assert finished.is_set()
        finally:
            release.set()
            owner.cancel()
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
        await started.wait()
        release_permit.set()
        await deferral_started.wait()
        task.cancel()
        finish_maintenance.set()

        with pytest.raises(asyncio.CancelledError):
            await task
        assert presence.closed

    _run(scenario())
