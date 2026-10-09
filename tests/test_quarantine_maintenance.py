from __future__ import annotations

import asyncio
import threading
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from contextvars import Context
from json import dumps, loads
from pathlib import Path
from struct import pack
from typing import cast

import pytest

from omi_collector.capture.adapters import quarantine as quarantine_module
from omi_collector.capture.adapters.opportunistic_runtime import OpportunisticRuntime
from omi_collector.capture.adapters.quarantine_publish import QuarantineSalvageDeferredError
from omi_collector.capture.adapters.ready_bundles import ReadyOutcome, ReadyOutcomeState
from omi_collector.capture.adapters.staging_contract import DeviceAlreadyRunningError
from omi_collector.capture.adapters.staging_store import StagingStore
from omi_collector.capture.application.ports import StagingPort
from omi_collector.capture.application.presence import PresencePolicy, PresenceWake
from omi_collector.capture.application.quarantine_maintenance import (
    OpportunisticSyncError,
    PendingStartupState,
    QuarantineMaintenance,
)
from omi_collector.capture.domain.ring_protocol import RECORD_SIZE, ReadBeginNotification
from omi_collector.config import CollectorConfig, RetryConfig, StagingRetentionConfig


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
    records = b"".join(_record(sequence) for sequence in range(start_sequence, start_sequence + count))
    attempt.record_read_begin(ReadBeginNotification(start_sequence, count))
    for index in range(count):
        attempt.accept_chunk(start_sequence + index, records[index * RECORD_SIZE : (index + 1) * RECORD_SIZE])
    attempt.checkpoint()
    attempt.close(durable=True)
    return records


def _quarantine_sources(store: StagingStore, counts: tuple[int, ...]) -> tuple[Path, ...]:
    for count in counts:
        _seed_streaming_partial(store, count)
    for attempt in tuple(store.attempts_root.iterdir()):
        store.quarantine_attempt_source(attempt.name)
    return store.quarantined_attempts()


def _source_snapshot(source: Path) -> dict[str, bytes]:
    return {path.name: path.read_bytes() for path in source.iterdir() if path.is_file()}


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
        with store.device_lock(recover_capture_temporaries=False, operation="competing-promotion"):
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

    def release_lock_before_quarantine(reason: str) -> tuple[Path, ...]:
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
        assert tuple(store.attempts_root.iterdir()) == ()
        quarantined_sources = tuple(path for path in store.quarantined_attempts() if path.is_dir())
        assert len(quarantined_sources) == 2
        assert {(source / "records.bin").read_bytes() for source in quarantined_sources} == expected_records
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


def test_run_once_salvages_two_real_sources_and_marks_each_publication(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def scenario() -> None:
        store = _store(tmp_path)
        sources = _quarantine_sources(store, (1, 2))
        before = {source: _source_snapshot(source) for source in sources}
        runtime = OpportunisticRuntime()
        original_publish = runtime.publish_quarantined_prefix
        calls: list[Path] = []

        def record_publish(
            source: Path,
            staging: StagingPort,
            *,
            should_defer: Callable[[], bool],
        ) -> object:
            calls.append(source)
            return original_publish(source, staging, should_defer=should_defer)

        monkeypatch.setattr(runtime, "publish_quarantined_prefix", record_publish)
        maintenance = QuarantineMaintenance(store, None, runtime)
        try:
            await maintenance.run_once(lambda: False)
        finally:
            await maintenance.close()

        assert calls == list(sources)
        assert all((source / "published.json").is_file() for source in sources)
        assert all(
            _source_snapshot(source) == before[source] | {"published.json": (source / "published.json").read_bytes()}
            for source in sources
        )
        bundles = tuple(store.capture_root.iterdir())
        assert len(bundles) == 2
        assert {bundle.joinpath("records.bin").read_bytes() for bundle in bundles} == {
            before[source]["records.bin"] for source in sources
        }

    _run(scenario())


@pytest.mark.parametrize("fail_unprocessable_marker", [False, True])
def test_run_once_continues_after_unprocessable_source_and_preserves_its_evidence(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, fail_unprocessable_marker: bool
) -> None:
    async def scenario() -> None:
        case = tmp_path / f"unprocessable-marker-failure-{fail_unprocessable_marker}"
        case.mkdir()
        store = _store(case)
        sources = _quarantine_sources(store, (1, 2))
        first, second = sources
        (first / "checkpoint.json").write_bytes(b"{invalid checkpoint")
        first_before = _source_snapshot(first)
        second_before = _source_snapshot(second)
        runtime = OpportunisticRuntime()
        original_publish = runtime.publish_quarantined_prefix
        publish_calls: list[Path] = []

        def record_publish(
            source: Path,
            staging: StagingPort,
            *,
            should_defer: Callable[[], bool],
        ) -> object:
            publish_calls.append(source)
            return original_publish(source, staging, should_defer=should_defer)

        monkeypatch.setattr(runtime, "publish_quarantined_prefix", record_publish)
        original_mark = store.mark_quarantine_unprocessable
        unprocessable_marks: list[Path] = []

        def mark_unprocessable(source: Path, reason: str) -> None:
            unprocessable_marks.append(source)
            if fail_unprocessable_marker and source == first:
                raise OSError("terminal marker write failed")
            original_mark(source, reason)

        monkeypatch.setattr(store, "mark_quarantine_unprocessable", mark_unprocessable)
        maintenance = QuarantineMaintenance(store, None, runtime)
        try:
            await maintenance.run_once(lambda: False)
        finally:
            await maintenance.close()

        assert publish_calls == list(sources)
        assert unprocessable_marks == [first]
        assert all((first / name).read_bytes() == value for name, value in first_before.items())
        assert not (first / "published.json").exists()
        assert (
            not (first / "unprocessable.json").exists()
            if fail_unprocessable_marker
            else (first / "unprocessable.json").is_file()
        )
        assert (second / "published.json").is_file()
        assert all((second / name).read_bytes() == value for name, value in second_before.items())
        assert len(tuple(store.capture_root.iterdir())) == 1

    _run(scenario())


@pytest.mark.parametrize(
    ("failure_type", "reason"),
    [(OSError, "temporary publication failure"), (QuarantineSalvageDeferredError, "maintenance deferred")],
)
def test_run_once_stops_after_retryable_or_deferred_source_without_touching_the_next(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    failure_type: type[Exception],
    reason: str,
) -> None:
    async def scenario() -> None:
        case = tmp_path / f"retry-failure-{reason.replace(' ', '-')}"
        case.mkdir()
        store = _store(case)
        sources = _quarantine_sources(store, (1, 2))
        before = {source: _source_snapshot(source) for source in sources}
        runtime = OpportunisticRuntime()
        original_publish = runtime.publish_quarantined_prefix
        calls: list[Path] = []

        def fail_first(
            source: Path,
            staging: StagingPort,
            *,
            should_defer: Callable[[], bool],
        ) -> object:
            calls.append(source)
            if source == sources[0]:
                raise failure_type(reason)
            return original_publish(source, staging, should_defer=should_defer)

        monkeypatch.setattr(runtime, "publish_quarantined_prefix", fail_first)
        maintenance = QuarantineMaintenance(store, None, runtime)
        try:
            await maintenance.run_once(lambda: False)
        finally:
            await maintenance.close()

        assert calls == [sources[0]]
        assert all(_source_snapshot(source) == before[source] for source in sources)
        assert all(not (source / "published.json").exists() for source in sources)
        assert all(not (source / "unprocessable.json").exists() for source in sources)
        assert tuple(store.capture_root.iterdir()) == ()

    _run(scenario())


def test_run_once_retries_quarantine_only_at_exact_monotonic_backoff_deadline(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def scenario() -> None:
        store = _store(tmp_path)
        sources = _quarantine_sources(store, (1, 2))
        runtime = OpportunisticRuntime()
        original_publish = runtime.publish_quarantined_prefix
        calls: list[Path] = []
        now = 100.0

        def monotonic() -> float:
            return now

        monkeypatch.setattr("omi_collector.capture.application.quarantine_maintenance.monotonic", monotonic)
        config = CollectorConfig(
            retry=RetryConfig(maintenance_interval_seconds=1.0, quarantine_publish_backoff_seconds=(5.0,))
        )

        def fail_once(
            source: Path,
            staging: StagingPort,
            *,
            should_defer: Callable[[], bool],
        ) -> object:
            calls.append(source)
            if source == sources[0] and calls.count(source) == 1:
                raise OSError("temporary publication failure")
            return original_publish(source, staging, should_defer=should_defer)

        monkeypatch.setattr(runtime, "publish_quarantined_prefix", fail_once)
        maintenance = QuarantineMaintenance(store, None, runtime, config=config)
        try:
            await maintenance.run_once(lambda: False)
            assert calls == [sources[0]]
            now = 101.0
            await maintenance.run_once(lambda: False)
            assert calls == [sources[0]]
            assert not (sources[0] / "published.json").exists()
            assert not (sources[1] / "published.json").exists()
            now = 105.0
            await maintenance.run_once(lambda: False)
        finally:
            await maintenance.close()

        assert calls == [sources[0], sources[0], sources[1]]
        assert all((source / "published.json").is_file() for source in sources)
        assert len(tuple(store.capture_root.iterdir())) == 2

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


def test_transient_clock_publication_does_not_gate_startup_and_arms_retry(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        config = CollectorConfig(retry=RetryConfig(rapid_backoff=(0.02,)))
        store = _publication_store(tmp_path, config=config)
        attempts: list[int] = []

        def recover_and_publish() -> ReadyOutcome:
            attempts.append(1)
            return ReadyOutcome(ReadyOutcomeState.TRANSIENT, reason="storage_busy_or_io")

        store._recover_and_publish_unlocked = recover_and_publish  # type: ignore[method-assign]
        maintenance = QuarantineMaintenance(store, None, OpportunisticRuntime(), config=config)

        await maintenance.ensure_publication_ready()
        assert attempts == [1]
        schedule = store.publication_retry_schedule()
        assert schedule is not None
        assert schedule[1] > time.monotonic()

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
        assert "unexpected publication fault" in caplog.text
        assert store.publication_retry_schedule() is None
        await maintenance.close()

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

        def sweep(*, should_defer: Callable[[], bool]) -> tuple[Path, ...]:
            worker_started.set()
            while not release_sweep.is_set() and not should_defer():
                time.sleep(0.001)
            worker_stopped.set()
            return ()

        store.sweep_terminal_retired = sweep  # type: ignore[method-assign]
        maintenance = QuarantineMaintenance(store, None, OpportunisticRuntime())
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
            maintenance.exit_capture_priority()
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

            maintenance.exit_capture_priority()
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

            maintenance.exit_capture_priority()
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
        store = _publication_store(tmp_path)
        store.append_ready_closure(11, "drained")
        original = store.begin_ready_visit
        maintenance = QuarantineMaintenance(store, None, OpportunisticRuntime())
        monkeypatch.setattr(store, "begin_ready_visit", lambda: (_ for _ in ()).throw(OSError("storage unavailable")))

        with pytest.raises(OSError, match="storage unavailable"):
            await maintenance.enter_capture_priority()
        assert store.publication_wake_admitted()
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
        store = _publication_store(tmp_path)
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
            assert store.publication_wake_admitted()
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


def test_invalidated_startup_state_rebinds_new_authenticated_pending_attempt(tmp_path: Path) -> None:
    async def scenario() -> None:
        class Presence:
            async def wait_for_attempt(self) -> PresenceWake:
                return PresenceWake("advertisement")

            async def close(self) -> None:
                return None

        store = _store(tmp_path)
        _seed_streaming_partial(store, 1)
        maintenance = QuarantineMaintenance(store, None, OpportunisticRuntime())
        bound: list[PendingStartupState] = []
        await maintenance.wait_for_presence_attempt(Presence(), bound.append)
        first = bound[0].pending
        assert first is not None

        with store.device_lock() as lease:
            completed = store.resume_streaming_attempt(lease)
            assert completed is not None
            assert completed.publish_prefix() is not None
            completed.close(durable=True)
        store.terminalize_prefix_attempt(first.attempt_id)
        _seed_streaming_partial(store, 1)
        second_expected = store.pending_attempts()[0]

        maintenance.invalidate_startup_state()
        await maintenance.wait_for_presence_attempt(Presence(), bound.append)
        await maintenance.close()

        assert len(bound) == 2
        assert bound[0].pending is not None and bound[0].pending.attempt_id == first.attempt_id
        assert bound[1].pending is not None and bound[1].pending.attempt_id == second_expected.attempt_id
        assert bound[1].pending.attempt_id != bound[0].pending.attempt_id

    _run(scenario())


def test_two_pending_startup_sources_remain_blocked_while_promotion_lease_is_busy(tmp_path: Path) -> None:
    async def scenario() -> None:
        store = _store(tmp_path)
        _seed_streaming_partial(store, 1)
        _seed_streaming_partial(store, 2)
        before = {path.name: _source_snapshot(path) for path in store.attempts_root.iterdir()}
        maintenance = QuarantineMaintenance(store, None, OpportunisticRuntime())
        try:
            with (
                store.device_lock(operation="competing-promotion"),
                pytest.raises(DeviceAlreadyRunningError),
            ):
                await maintenance.prepare_pending_startup()
        finally:
            await maintenance.close()

        assert {path.name: _source_snapshot(path) for path in store.attempts_root.iterdir()} == before
        assert len(store.pending_attempts()) == 2
        assert tuple(store.quarantined_attempts()) == ()

    _run(scenario())


def test_pending_descriptor_disappearance_between_inspection_and_open_is_rejected(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = _store(tmp_path)
    records = _seed_streaming_partial(store, 1)
    descriptor = store.pending_attempts()[0]
    original_lock = store.device_lock
    moved: list[Path] = []

    @contextmanager
    def move_before_resume(*, recover_capture_temporaries: bool = True, operation: str = "unknown") -> Iterator[object]:
        if operation == "resume_pending_attempt":
            moved.append(store.quarantine_attempt_source(descriptor.attempt_id))
        with original_lock(recover_capture_temporaries=recover_capture_temporaries, operation=operation) as lease:
            yield lease

    monkeypatch.setattr(store, "device_lock", move_before_resume)
    maintenance = QuarantineMaintenance(store, None, OpportunisticRuntime())

    with pytest.raises(OpportunisticSyncError, match="pending attempt disappeared"):
        _run(maintenance.prepare_pending_startup())

    assert len(moved) == 1
    assert not (store.attempts_root / descriptor.attempt_id).exists()
    assert moved[0].is_dir()
    assert (moved[0] / "records.bin").read_bytes() == records
    assert store.pending_attempts() == ()
    _run(maintenance.close())


@pytest.mark.parametrize("terminal_marker", ["published", "unprocessable"])
def test_salvage_skips_source_made_terminal_after_inventory_and_continues(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, terminal_marker: str
) -> None:
    store, first, second = _prepare_terminal_salvage_sources(tmp_path)
    first_before = _source_snapshot(first)
    original_inventory = store.quarantined_attempts
    original_mark_unprocessable = store.mark_quarantine_unprocessable
    unprocessable_marks: list[tuple[Path, str]] = []
    marked: list[Path] = []

    def record_unprocessable(source: Path, reason: str) -> None:
        unprocessable_marks.append((source, reason))
        original_mark_unprocessable(source, reason)

    monkeypatch.setattr(store, "mark_quarantine_unprocessable", record_unprocessable)

    def mark_terminal_after_inventory(*, should_defer: Callable[[], bool] | None = None) -> tuple[Path, ...]:
        sources = original_inventory(should_defer=should_defer)
        if first not in marked:
            marked.append(first)
            if terminal_marker == "published":
                store.mark_quarantine_published(first)
            else:
                store.mark_quarantine_unprocessable(first, "concurrent terminal classification")
        return sources

    monkeypatch.setattr(store, "quarantined_attempts", mark_terminal_after_inventory)
    runtime = OpportunisticRuntime()
    original_publish = runtime.publish_quarantined_prefix
    publish_calls: list[Path] = []

    def record_publish(source: Path, staging: StagingPort, *, should_defer: Callable[[], bool]) -> object:
        publish_calls.append(source)
        return original_publish(source, staging, should_defer=should_defer)

    monkeypatch.setattr(runtime, "publish_quarantined_prefix", record_publish)
    maintenance = QuarantineMaintenance(store, None, runtime)
    _run(maintenance.run_once(lambda: False))
    _run(maintenance.close())

    assert marked == [first]
    assert store.quarantine_state(first).name.upper() == terminal_marker.upper()
    assert _source_snapshot(first) == first_before | {
        "published.json" if terminal_marker == "published" else "unprocessable.json": (
            first / ("published.json" if terminal_marker == "published" else "unprocessable.json")
        ).read_bytes()
    }
    assert publish_calls == [second]
    assert unprocessable_marks == (
        [(first, "concurrent terminal classification")] if terminal_marker == "unprocessable" else []
    )
    assert (second / "published.json").is_file()
    assert len(tuple(store.capture_root.iterdir())) >= 1


def _prepare_terminal_salvage_sources(tmp_path: Path) -> tuple[StagingStore, Path, Path]:
    store = StagingStore(tmp_path / "spool", tmp_path / "captures")
    for count, start_sequence in ((1, 100), (2, 200)):
        _seed_streaming_partial(store, count, start_sequence=start_sequence)
        pending = store.pending_attempts()
        assert len(pending) == 1
        store.quarantine_attempt_source(pending[0].attempt_id)
        assert tuple(store.capture_root.iterdir()) == ()
    sources = store.quarantined_attempts()
    first, second = sources
    assert all(store.quarantine_state(source).name.upper() == "RETRYABLE" for source in sources)
    return store, first, second


def test_published_marker_failure_keeps_first_bundle_and_still_processes_second_source(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = _store(tmp_path)
    sources = _quarantine_sources(store, (1, 2))
    before = {source: _source_snapshot(source) for source in sources}
    original_mark = store.mark_quarantine_published
    failed: list[Path] = []

    def fail_first_marker(source: Path) -> None:
        if source == sources[0] and not failed:
            failed.append(source)
            raise OSError("published marker unavailable")
        original_mark(source)

    monkeypatch.setattr(store, "mark_quarantine_published", fail_first_marker)
    maintenance = QuarantineMaintenance(store, None, OpportunisticRuntime())
    _run(maintenance.run_once(lambda: False))
    _run(maintenance.close())

    assert failed == [sources[0]]
    assert _source_snapshot(sources[0]) == before[sources[0]]
    assert not (sources[0] / "published.json").exists()
    assert (sources[1] / "published.json").is_file()
    bundles = tuple(store.capture_root.iterdir())
    assert len(bundles) == 2
    assert {bundle.joinpath("records.bin").read_bytes() for bundle in bundles} == {
        before[source]["records.bin"] for source in sources
    }


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
    calls: list[Path] = []
    runtime = OpportunisticRuntime()

    def keep_retryable_failure(
        source: Path,
        _staging: StagingPort,
        *,
        should_defer: Callable[[], bool],
    ) -> object:
        assert not should_defer()
        calls.append(source)
        raise OSError("persistent temporary publication failure")

    monkeypatch.setattr(runtime, "publish_quarantined_prefix", keep_retryable_failure)
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
    assert all(not (source / "published.json").exists() for source in sources)
    assert all(not (source / "unprocessable.json").exists() for source in sources)
    assert tuple(store.capture_root.iterdir()) == ()


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

        def record_final_public_scan(*, should_defer: Callable[[], bool] | None = None) -> tuple[Path, ...]:
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
            with pytest.raises(asyncio.CancelledError) as error:
                done, pending = await asyncio.wait({owner}, timeout=5.0)
                assert owner in done and not pending
                owner.result()
            assert error.value.__cause__ is failure
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
        maintenance.exit_capture_priority()
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
