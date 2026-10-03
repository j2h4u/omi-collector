"""Startup closure behavior for durable unpublished streaming attempts."""

from __future__ import annotations

import asyncio
from json import loads
from pathlib import Path
from struct import pack

from omi_collector.capture.adapters.opportunistic_runtime import OpportunisticRuntime
from omi_collector.capture.adapters.staging_contract import AttemptDescriptor
from omi_collector.capture.adapters.staging_store import StagingStore
from omi_collector.capture.application.batch_reconciliation import BatchReconciler
from omi_collector.capture.application.collector import TransferTimeouts
from omi_collector.capture.application.session_lifecycle import OpportunisticOptions, RetryPolicy
from omi_collector.capture.domain.ring_protocol import RECORD_SIZE, ReadBeginNotification


def _record(value: int) -> bytes:
    return pack(">I", value) + bytes((value % 256,)) * (RECORD_SIZE - 4)


def _seed_partial(spool: Path, capture_root: Path) -> tuple[StagingStore, Path, bytes, bytes]:
    store = StagingStore(spool, capture_root)
    attempt = store.prepare_streaming_attempt(100, 2)
    attempt.record_read_begin(ReadBeginNotification(100, 2))
    attempt.accept_chunk(100, _record(100))
    prefix = attempt.checkpoint()
    attempt.close(durable=True)

    assert prefix.next_sequence == 101
    descriptor = store.pending_attempts()[0]
    attempt_path = store.attempts_root / descriptor.attempt_id
    return (
        store,
        attempt_path,
        (attempt_path / "records.bin").read_bytes(),
        (attempt_path / "checkpoint.json").read_bytes(),
    )


def _reconciler(store: StagingStore, descriptor: AttemptDescriptor, durable_next: int) -> BatchReconciler:
    async def quarantine(_attempt_id: str) -> None:
        return None

    reconciler = BatchReconciler(
        store,
        OpportunisticOptions(
            timeouts=TransferTimeouts(1, 1),
            policy=RetryPolicy(backoff=(0.001,), batch_records=2, stop_after_drained=True),
        ),
        OpportunisticRuntime(),
        quarantine,
    )
    reconciler.set_startup_state(descriptor, durable_next)
    return reconciler


def test_restart_interrupted_preserves_unpublished_partial_and_clears_visit(tmp_path: Path) -> None:
    store, attempt_path, raw_before, checkpoint_before = _seed_partial(tmp_path / "spool", tmp_path / "captures")
    descriptor = store.pending_attempts()[0]
    durable_next = store.open_attempt(descriptor.attempt_id).recover().valid_records + descriptor.start_sequence
    reconciler = _reconciler(store, descriptor, durable_next)

    assert reconciler.pending_descriptor == descriptor
    assert reconciler.pending_durable_next == 101
    assert reconciler.durable_progress() == 101

    asyncio.run(reconciler.close_visit("restart_interrupted"))

    assert (attempt_path / "records.bin").read_bytes() == raw_before
    assert (attempt_path / "checkpoint.json").read_bytes() == checkpoint_before
    assert not (attempt_path / "prefix-publication.json").exists()
    assert not (attempt_path / "terminal-retired.json").exists()
    assert store.pending_attempts() == (descriptor,)
    assert not store.ready_closures_path.exists()
    assert reconciler.pending_descriptor is None
    assert reconciler.pending_durable_next is None
    assert reconciler.durable_progress() == 0


def test_absence_publishes_authenticated_partial_and_closes_visit(tmp_path: Path) -> None:
    store, attempt_path, _raw_before, _checkpoint_before = _seed_partial(tmp_path / "spool", tmp_path / "captures")
    descriptor = store.pending_attempts()[0]
    durable_next = store.open_attempt(descriptor.attempt_id).recover().valid_records + descriptor.start_sequence
    reconciler = _reconciler(store, descriptor, durable_next)

    asyncio.run(reconciler.close_visit("absence"))

    assert (attempt_path / "prefix-publication.json").is_file()
    assert (attempt_path / "terminal-retired.json").is_file()
    assert store.pending_attempts() == ()
    assert loads(store.ready_closures_path.read_text(encoding="utf-8"))["closures"] == [
        {"next_sequence": 101, "reason": "absence"}
    ]
    assert reconciler.pending_descriptor is None
    assert reconciler.pending_durable_next is None
    assert reconciler.durable_progress() == 0
    assert tuple((tmp_path / "captures").glob("100-101-*"))
