"""Durable physical-visit closure queue contracts."""

from __future__ import annotations

import asyncio
from hashlib import sha256
from json import dumps, loads
from pathlib import Path
from struct import pack

import pytest

from omi_collector.capture.adapters.attempts import StagedAttempt
from omi_collector.capture.adapters.bundle_contract import BundleManifest, SealedReceipt
from omi_collector.capture.adapters.opportunistic_runtime import OpportunisticRuntime
from omi_collector.capture.adapters.ready_closures import ReadyClosureError, append, load, remove
from omi_collector.capture.adapters.staging_store import StagingStore
from omi_collector.capture.application.quarantine_maintenance import QuarantineMaintenance
from omi_collector.capture.domain.ring_protocol import RECORD_SIZE, ReadBeginNotification
from omi_collector.config import CollectorConfig, ReadyConfig


def _record(sequence: int) -> bytes:
    return pack(">I", sequence) + bytes(RECORD_SIZE - 4)


def _partial(tmp_path: Path) -> tuple[StagingStore, StagedAttempt]:
    store = StagingStore(tmp_path / "spool", tmp_path / "capture")
    attempt = store.prepare_streaming_attempt(10, 2)
    attempt.record_read_begin(ReadBeginNotification(10, 2))
    attempt.accept_chunk(10, _record(10))
    attempt.checkpoint()
    attempt.close(durable=True)
    return store, attempt


def test_closures_coalesce_to_latest_watermark_and_keep_equal_drain(tmp_path: Path) -> None:
    path = tmp_path / "ready-closures.json"

    first = append(path, 10, "absence")
    assert append(path, 10, "recovery_exhausted") == first
    second = append(path, 20, "restart_interrupted")
    assert load(path) == (second,)
    drained = append(path, 20, "drained")
    assert drained.reason == "drained"
    assert append(path, 20, "restart_interrupted") == drained
    assert load(path) == (drained,)

    remove(path, drained)
    assert load(path) == ()
    assert loads(path.read_text(encoding="utf-8"))["version"] == 1


def test_new_visit_revokes_old_drain_through_clock_publication_and_restart(tmp_path: Path) -> None:
    base = StagingStore(tmp_path / "spool", tmp_path / "draft")
    ready = tmp_path / "ready"
    ready.mkdir(mode=0o2750)
    ready.chmod(0o2750)
    config = CollectorConfig(ready=ReadyConfig(target_audio_seconds=0.02))
    store = StagingStore.from_paths(base.paths, publication_root=ready, config=config)
    raw = bytes((0, 0, 0, 100, 2, 8, 0x55)) + bytes(RECORD_SIZE - 7)
    digest = sha256(raw).hexdigest()
    draft = store.capture_root / "one"
    draft.mkdir(parents=True)
    (draft / "records.bin").write_bytes(raw)
    (draft / "manifest.json").write_text(
        dumps(BundleManifest(2, 10, 11, 1, RECORD_SIZE, digest).as_dict()), encoding="utf-8"
    )
    (draft / "receipt.json").write_text(dumps(SealedReceipt("a" * 32, digest).as_dict()), encoding="utf-8")
    store.append_ready_closure(11, "drained")
    authority = store.create_publication_authority()
    maintenance = QuarantineMaintenance(store, None, OpportunisticRuntime())

    async def visit() -> None:
        await maintenance.enter_capture_priority()
        try:
            assert load(store.ready_closures_path)[0].reason == "collecting"
            assert authority.publish() is None
        finally:
            maintenance.exit_capture_priority()
            await maintenance.close()

    asyncio.run(visit())
    authority.close()
    restarted = StagingStore.from_paths(base.paths, publication_root=ready, config=config)
    assert restarted.recover_and_publish() is None
    assert draft.exists()
    assert not tuple(ready.iterdir())
    restarted.append_ready_closure(11, "drained")
    assert restarted.recover_and_publish() is not None
    assert not draft.exists()


def test_visit_begin_reuses_active_writer_lease_across_threads(tmp_path: Path) -> None:
    store = StagingStore(tmp_path / "spool", tmp_path / "draft")
    store.append_ready_closure(11, "drained")

    with store.device_lock(recover_capture_temporaries=False) as lease:
        marker = asyncio.run(asyncio.to_thread(store.begin_ready_visit))
        lease.require_active()

    assert marker is not None and marker.reason == "collecting"


def test_closures_reject_malformed_state(tmp_path: Path) -> None:
    path = tmp_path / "ready-closures.json"
    path.write_text('{"closures":[{"next_sequence":20,"reason":"absence"}],"version":1}', encoding="utf-8")

    with pytest.raises(ReadyClosureError, match="regressed"):
        append(path, 10, "absence")

    path.write_text(
        '{"closures":[{"next_sequence":20,"reason":"absence"},{"next_sequence":10,"reason":"absence"}],"version":1}',
        encoding="utf-8",
    )
    with pytest.raises(ReadyClosureError, match="out of order"):
        load(path)

    path.write_text('{"closures":[{"next_sequence":20}],"version":1}', encoding="utf-8")
    with pytest.raises(ReadyClosureError, match="entry schema"):
        load(path)


def test_legacy_non_drained_closure_does_not_authorize_prefix_publication(tmp_path: Path) -> None:
    base, _attempt = _partial(tmp_path)
    ready = tmp_path / "ready"
    ready.mkdir(mode=0o2750)
    ready.chmod(0o2750)
    store = StagingStore.from_paths(base.paths, publication_root=ready)
    with store.device_lock() as lease:
        resumed = store.resume_streaming_attempt(lease)
        assert resumed is not None
        publication = resumed.publish_prefix()
        assert publication is not None
        resumed.close(durable=True)
    store.append_ready_closure(11, "legacy_prefix_publication")
    assert store.recover_and_publish() is None
    assert publication.bundle_path.exists()
    assert not tuple(ready.iterdir())
    assert loads(store.ready_closures_path.read_text())["closures"] == [
        {"next_sequence": 11, "reason": "legacy_prefix_publication"}
    ]


def test_interrupted_close_publishes_ordinary_pending_prefix(tmp_path: Path) -> None:
    store, attempt = _partial(tmp_path)

    closure = store.close_pending_prefix("absence")

    assert closure is not None and closure.next_sequence == 11
    assert (attempt.path / "prefix-publication.json").is_file()
    assert (attempt.path / "terminal-retired.json").is_file()
    assert store.pending_attempts() == ()
