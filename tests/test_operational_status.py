from __future__ import annotations

import asyncio
import stat
from itertools import product
from pathlib import Path

import pytest

import omi_collector.capture.adapters.operational_status as status_module
from omi_collector.capture.adapters.operational_status import (
    OperationalIdentity,
    OperationalStatusError,
    OperationalStatusStore,
    read_operational_status,
)
from omi_collector.capture.domain.operational_status_machine import (
    OperationalDimension,
    OperationalSignal,
    OperationalState,
    check_transition_completeness,
    transition,
)

BOOT = "00000000-0000-0000-0000-000000000001"
INVOCATION = "00000000000000000000000000000001"


def test_every_state_and_typed_signal_has_an_explicit_transition() -> None:
    check_transition_completeness()
    for state, signal in product(OperationalState, OperationalSignal):
        assert isinstance(transition(state, signal), OperationalState)


def test_snapshot_restart_preserves_blocked_and_invalidates_clear(tmp_path: Path) -> None:
    path = tmp_path / "operational-status.json"
    first_identity = OperationalIdentity(BOOT, INVOCATION)
    first = OperationalStatusStore(path, first_identity)
    first.initialize()
    first.update(OperationalDimension.QUALITY, OperationalSignal.CLEAR)
    first.update(OperationalDimension.PUBLICATION, OperationalSignal.BLOCK)

    next_identity = OperationalIdentity(BOOT, "00000000000000000000000000000002")
    assert read_operational_status(path, next_identity) == {
        "quality": "unknown",
        "publication": "blocked",
        "clock": "unknown",
    }

    first.close()
    restarted = OperationalStatusStore(path, next_identity)
    restarted.initialize()
    assert restarted.as_dict() == {
        "quality": "unknown",
        "publication": "blocked",
        "clock": "unknown",
    }


def test_snapshot_writes_only_for_state_or_identity_changes(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    path = tmp_path / "operational-status.json"
    identity = OperationalIdentity(BOOT, INVOCATION)
    writes: list[bytes] = []
    real_write = status_module._write_snapshot

    def count_write(target: Path, payload: bytes) -> None:
        writes.append(payload)
        real_write(target, payload)

    monkeypatch.setattr(status_module, "_write_snapshot", count_write)
    store = OperationalStatusStore(path, identity)
    store.initialize()
    store.initialize()
    store.update(OperationalDimension.QUALITY, OperationalSignal.UNCHANGED)
    assert len(writes) == 1

    store.update(OperationalDimension.QUALITY, OperationalSignal.CLEAR)
    assert len(writes) == 2


def test_snapshot_and_existing_writer_lease_are_owner_only(tmp_path: Path) -> None:
    path = tmp_path / "operational-status.json"
    lock_path = path.with_name(f"{path.name}.lock")
    lock_path.write_bytes(b"")
    lock_path.chmod(0o640)

    store = OperationalStatusStore(path, OperationalIdentity(BOOT, INVOCATION))
    try:
        store.initialize()
        assert stat.S_IMODE(path.stat().st_mode) == 0o600
        assert stat.S_IMODE(lock_path.stat().st_mode) == 0o600
    finally:
        store.close()


@pytest.mark.parametrize("payload", [b"{", b"{}", b" " * 8_193])
def test_malformed_or_oversized_snapshot_fails_closed(tmp_path: Path, payload: bytes) -> None:
    path = tmp_path / "operational-status.json"
    path.write_bytes(payload)
    with pytest.raises(OperationalStatusError):
        read_operational_status(path, OperationalIdentity(BOOT, INVOCATION))


def test_symlink_snapshot_fails_closed(tmp_path: Path) -> None:
    target = tmp_path / "target.json"
    target.write_text("{}", encoding="utf-8")
    path = tmp_path / "operational-status.json"
    path.symlink_to(target)

    with pytest.raises(OperationalStatusError):
        read_operational_status(path, OperationalIdentity(BOOT, INVOCATION))


def test_publication_outcomes_have_explicit_state_semantics(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from omi_collector.capture.adapters.opportunistic_runtime import OpportunisticRuntime

    store = OperationalStatusStore(tmp_path / "status.json", OperationalIdentity(BOOT, INVOCATION))
    store.initialize()
    monkeypatch.setattr("omi_collector.capture.adapters.opportunistic_runtime.debug_event", lambda *_a, **_k: None)
    runtime = OpportunisticRuntime(store.record_publication_outcome)
    runtime.debug_event("ready_publication_transient")
    assert store.as_dict()[OperationalDimension.PUBLICATION.value] == OperationalState.UNKNOWN.value
    runtime.debug_event("ready_publication_blocked")
    runtime.debug_event("ready_publication_transient")
    assert store.as_dict()[OperationalDimension.PUBLICATION.value] == OperationalState.BLOCKED.value
    runtime.debug_event("ready_publication_published")
    assert store.as_dict()[OperationalDimension.PUBLICATION.value] == OperationalState.CLEAR.value
    runtime.debug_event("ready_publication_transient")
    assert store.as_dict()[OperationalDimension.PUBLICATION.value] == OperationalState.UNKNOWN.value
    runtime.debug_event("ready_publication_waiting")
    assert store.as_dict()[OperationalDimension.PUBLICATION.value] == OperationalState.CLEAR.value
    assert store.as_dict()[OperationalDimension.QUALITY.value] == OperationalState.UNKNOWN.value
    assert store.as_dict()[OperationalDimension.CLOCK.value] == OperationalState.UNKNOWN.value


def test_write_failure_latches_for_service_supervision_without_raising_from_callback(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "status.json"
    identity = OperationalIdentity(BOOT, INVOCATION)
    store = OperationalStatusStore(path, identity)
    store.initialize()
    store.update(OperationalDimension.QUALITY, OperationalSignal.CLEAR)

    def fail(_path: Path, _payload: bytes) -> None:
        raise OperationalStatusError("write failed")

    monkeypatch.setattr(status_module, "_write_snapshot", fail)
    store.update(OperationalDimension.QUALITY, OperationalSignal.BLOCK)

    assert store.failure is not None
    assert read_operational_status(path, identity)["quality"] == "clear"
    assert asyncio.run(store.wait_failure()) is store.failure


def test_supervisor_cancels_and_awaits_capture_after_snapshot_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from omi_collector.cli import _supervise_operational_status

    store = OperationalStatusStore(tmp_path / "status.json", OperationalIdentity(BOOT, INVOCATION))
    store.initialize()
    cleaned: list[bool] = []

    async def capture() -> None:
        try:
            await asyncio.Event().wait()
        finally:
            cleaned.append(True)

    def fail(_path: Path, _payload: bytes) -> None:
        raise OperationalStatusError("write failed")

    monkeypatch.setattr(status_module, "_write_snapshot", fail)

    async def run() -> None:
        operation = asyncio.create_task(_supervise_operational_status(capture(), store))
        await asyncio.sleep(0)
        store.update(OperationalDimension.QUALITY, OperationalSignal.BLOCK)
        with pytest.raises(OperationalStatusError, match="persistence failed"):
            await operation

    asyncio.run(run())
    assert cleaned == [True]


def test_quality_configuration_failure_marks_quality_blocked(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from omi_collector.capture import cli as capture_cli
    from omi_collector.capture.adapters.staging_store import StagingStore
    from omi_collector.config import DEFAULT_CONFIG

    signals: list[OperationalSignal] = []

    def fail_creation(*_args: object, **_kwargs: object) -> None:
        raise ValueError("invalid metrics configuration")

    monkeypatch.setattr(capture_cli, "JsonlQualityMetrics", fail_creation)
    result = capture_cli._quality_metrics(
        StagingStore(tmp_path / "spool", tmp_path / "capture"),
        DEFAULT_CONFIG,
        operational_signal=signals.append,
    )

    assert result is None
    assert signals == [OperationalSignal.BLOCK]


def test_clock_evidence_clears_only_after_conclusive_state_and_clean_ledger(tmp_path: Path) -> None:
    from omi_collector.capture.adapters.clock_corrections import ClockCorrectionStore
    from omi_collector.capture.adapters.staging_store import StagingStore
    from omi_collector.cli import SyncProgressReporter

    identity = OperationalIdentity(BOOT, INVOCATION)
    store = OperationalStatusStore(tmp_path / "status.json", identity)
    store.initialize()
    store.update(OperationalDimension.CLOCK, OperationalSignal.BLOCK)
    staging = StagingStore(tmp_path / "spool", tmp_path / "capture")
    reporter = SyncProgressReporter(operational_status=store, staging=staging)

    reporter._record_clock_status({"event": "pendant_clock_sync", "outcome": "within_threshold"})
    assert store.as_dict()[OperationalDimension.CLOCK.value] == OperationalState.CLEAR.value

    store.update(OperationalDimension.CLOCK, OperationalSignal.BLOCK)
    reporter._record_clock_status(
        {"event": "pendant_clock_sync", "outcome": "within_threshold", "reconciliation": "failed"}
    )
    assert store.as_dict()[OperationalDimension.CLOCK.value] == OperationalState.BLOCKED.value
    ledger = ClockCorrectionStore(staging.device_state_path, staging.attempts_root)
    correction = ledger.prepare(100, 110, 10.0, 5)
    ledger.mark_unresolved(correction)
    reporter._record_clock_status({"event": "pendant_clock_sync", "outcome": "verified"})
    assert store.as_dict()[OperationalDimension.CLOCK.value] == OperationalState.BLOCKED.value

    reporter._record_clock_status({"event": "pendant_clock_sync", "outcome": "intent_persist_failed"})
    assert store.as_dict()[OperationalDimension.CLOCK.value] == OperationalState.BLOCKED.value


def test_configured_quality_writer_preserves_a_prior_block(tmp_path: Path) -> None:
    from omi_collector.capture.adapters.quality_metrics import JsonlQualityMetrics

    store = OperationalStatusStore(tmp_path / "status.json", OperationalIdentity(BOOT, INVOCATION))
    store.initialize()
    store.update(OperationalDimension.QUALITY, OperationalSignal.BLOCK)
    metrics = JsonlQualityMetrics(tmp_path / "metrics", release_version="1.2.3", source_revision="a" * 40)
    metrics.set_operational_signal(lambda signal: store.update(OperationalDimension.QUALITY, signal))

    assert store.as_dict()[OperationalDimension.QUALITY.value] == OperationalState.BLOCKED.value
    assert metrics.close()


def test_writer_lease_rejects_second_writer_and_closed_writer_ignores_late_updates(tmp_path: Path) -> None:
    path = tmp_path / "status.json"
    first = OperationalStatusStore(path, OperationalIdentity(BOOT, INVOCATION))
    first.initialize()
    with pytest.raises(OperationalStatusError, match="already active"):
        OperationalStatusStore(path, OperationalIdentity(BOOT, INVOCATION))

    first.close()
    second = OperationalStatusStore(path, OperationalIdentity(BOOT, INVOCATION))
    second.initialize()
    second.update(OperationalDimension.QUALITY, OperationalSignal.BLOCK)
    after_second_writer = path.read_bytes()
    first.update(OperationalDimension.QUALITY, OperationalSignal.CLEAR)

    assert path.read_bytes() == after_second_writer


@pytest.mark.parametrize(
    ("boot_id", "invocation_id"),
    [
        ("unknown", INVOCATION),
        (BOOT, "invocation"),
    ],
)
def test_identity_rejects_noncanonical_values(boot_id: str, invocation_id: str) -> None:
    with pytest.raises(OperationalStatusError):
        OperationalIdentity(boot_id, invocation_id)
