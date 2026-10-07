"""Durable physical-visit closure queue contracts."""

from __future__ import annotations

import asyncio
import os
from hashlib import sha256
from json import dumps, loads
from pathlib import Path
from struct import pack

import pytest

from omi_collector.capture.adapters.attempts import StagedAttempt
from omi_collector.capture.adapters.bundle_contract import BundleManifest, SealedReceipt
from omi_collector.capture.adapters.opportunistic_runtime import OpportunisticRuntime
from omi_collector.capture.adapters.ready_closures import (
    ReadyClosure,
    ReadyClosureError,
    append,
    begin_visit,
    coalesce,
    load,
    remove,
)
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


def test_new_visit_revokes_old_drain_through_publication_and_restart(tmp_path: Path) -> None:
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
    maintenance = QuarantineMaintenance(store, None, OpportunisticRuntime())

    async def visit() -> None:
        await maintenance.enter_capture_priority()
        try:
            assert load(store.ready_closures_path)[0].reason == "collecting"
            assert store.publish_ready().reason == "capture_active"
        finally:
            maintenance.exit_capture_priority()
            await maintenance.close()

    asyncio.run(visit())
    restarted = StagingStore.from_paths(base.paths, publication_root=ready, config=config)
    assert restarted.recover_and_publish().state == "waiting"
    assert draft.exists()
    assert not tuple(ready.iterdir())
    restarted.append_ready_closure(11, "drained")
    assert restarted.recover_and_publish().state == "published"
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


@pytest.mark.parametrize(
    "state",
    [
        pytest.param({"version": 1}, id="missing-closures"),
        pytest.param({"version": 1, "closures": [], "unexpected": True}, id="extra-field"),
    ],
)
def test_load_rejects_missing_or_extra_top_level_fields(tmp_path: Path, state: dict[str, object]) -> None:
    path = tmp_path / "ready-closures.json"
    path.write_text(dumps(state), encoding="utf-8")

    with pytest.raises(ReadyClosureError, match="schema is invalid"):
        load(path)


def test_append_creates_missing_nested_parent_directories(tmp_path: Path) -> None:
    path = tmp_path / "missing" / "nested" / "ready-closures.json"

    closure = append(path, 10, "absence")

    assert path.parent.is_dir()
    assert load(path) == (closure,)


@pytest.mark.parametrize("next_sequence", [-1, True, 1.5, "1"])
def test_append_rejects_invalid_frontier_without_changing_queue(tmp_path: Path, next_sequence: object) -> None:
    path = tmp_path / "ready-closures.json"
    original = append(path, 0, "absence")
    original_bytes = path.read_bytes()

    with pytest.raises(ReadyClosureError, match="frontier is invalid"):
        append(path, next_sequence, "restart_interrupted")  # type: ignore[arg-type]

    assert path.read_bytes() == original_bytes
    assert load(path) == (original,)


@pytest.mark.parametrize("reason", ["", 1, None])
def test_append_rejects_invalid_reason_without_changing_queue(tmp_path: Path, reason: object) -> None:
    path = tmp_path / "ready-closures.json"
    original = append(path, 0, "absence")
    original_bytes = path.read_bytes()

    with pytest.raises(ReadyClosureError, match="reason is invalid"):
        append(path, 1, reason)  # type: ignore[arg-type]

    assert path.read_bytes() == original_bytes
    assert load(path) == (original,)


def test_coalesce_keeps_latest_legacy_closure_and_handles_missing_or_singleton(tmp_path: Path) -> None:
    path = tmp_path / "ready-closures.json"
    legacy = {
        "version": 1,
        "closures": [
            {"next_sequence": 10, "reason": "absence"},
            {"next_sequence": 20, "reason": "restart_interrupted"},
        ],
    }
    path.write_text(dumps(legacy), encoding="utf-8")
    newest = ReadyClosure(20, "restart_interrupted")

    assert coalesce(path) == (newest,)
    assert load(path) == (newest,)
    assert loads(path.read_text(encoding="utf-8"))["closures"] == [
        {"next_sequence": 20, "reason": "restart_interrupted"}
    ]

    missing = tmp_path / "missing.json"
    assert coalesce(missing) == ()
    assert not missing.exists()

    singleton = tmp_path / "singleton.json"
    only = append(singleton, 30, "absence")
    singleton_bytes = singleton.read_bytes()
    assert coalesce(singleton) == (only,)
    assert load(singleton) == (only,)
    assert singleton.read_bytes() == singleton_bytes


def test_coalesce_accepts_legacy_fifo_with_equal_frontiers(tmp_path: Path) -> None:
    path = tmp_path / "equal-frontiers.json"
    path.write_text(
        dumps(
            {
                "version": 1,
                "closures": [
                    {"next_sequence": 30, "reason": "absence"},
                    {"next_sequence": 30, "reason": "recovery_exhausted"},
                ],
            }
        ),
        encoding="utf-8",
    )
    first = ReadyClosure(30, "absence")
    newest = ReadyClosure(30, "recovery_exhausted")

    assert load(path) == (first, newest)
    assert coalesce(path) == (newest,)
    assert load(path) == (newest,)


def test_singleton_coalesce_succeeds_when_durable_rewrite_is_unavailable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "ready-closures.json"
    only = append(path, 30, "absence")
    original_bytes = path.read_bytes()

    def reject_fsync(_fd: int) -> None:
        raise OSError("unexpected rewrite")

    monkeypatch.setattr(os, "fsync", reject_fsync)

    assert coalesce(path) == (only,)
    assert path.read_bytes() == original_bytes


def test_begin_visit_returns_existing_non_drained_closure_unchanged(tmp_path: Path) -> None:
    path = tmp_path / "ready-closures.json"
    previous = append(path, 30, "absence")
    original_bytes = path.read_bytes()

    assert begin_visit(path) == previous
    assert path.read_bytes() == original_bytes


@pytest.mark.parametrize("queue_state", ["empty", "mismatched_head"])
def test_remove_rejects_empty_or_mismatched_queue_without_changing_bytes(tmp_path: Path, queue_state: str) -> None:
    path = tmp_path / "ready-closures.json"
    queued = append(path, 30, "absence")
    if queue_state == "empty":
        remove(path, queued)
        rejected = queued
    else:
        rejected = ReadyClosure(31, "absence")
    original_bytes = path.read_bytes()

    with pytest.raises(ReadyClosureError):
        remove(path, rejected)

    assert path.read_bytes() == original_bytes


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
    assert store.recover_and_publish().state == "waiting"
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
