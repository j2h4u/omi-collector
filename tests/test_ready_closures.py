"""Durable physical-visit closure queue contracts."""

from __future__ import annotations

import asyncio
from json import loads
from pathlib import Path
from struct import pack

import pytest

from omi_collector.capture.adapters.attempts import StagedAttempt
from omi_collector.capture.adapters.opportunistic_runtime import OpportunisticRuntime
from omi_collector.capture.adapters.ready_closures import ReadyClosureError, append, load, remove
from omi_collector.capture.adapters.staging_store import StagingStore
from omi_collector.capture.application.quarantine_maintenance import QuarantineMaintenance
from omi_collector.capture.domain.ring_protocol import RECORD_SIZE, ReadBeginNotification


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


def test_closures_are_fifo_and_replay_safe(tmp_path: Path) -> None:
    path = tmp_path / "ready-closures.json"

    first = append(path, 10, "absence")
    assert append(path, 10, "recovery_exhausted") == first
    second = append(path, 20, "restart_interrupted")

    assert load(path) == (first, second)
    remove(path, first)
    assert load(path) == (second,)
    assert loads(path.read_text(encoding="utf-8"))["version"] == 1


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


def test_startup_replays_ready_prefix_when_draft_was_already_consumed(tmp_path: Path) -> None:
    base, attempt = _partial(tmp_path)
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
    digest = publication.bundle_path.joinpath("records.bin").read_bytes()
    # Recreate the legacy ordering: ready consumed the draft before retirement.
    store.append_ready_closure(11, "legacy_prefix_publication")
    store.recover_and_publish()
    assert not publication.bundle_path.exists()
    original_ready = {path.name: (path / "records.bin").read_bytes() for path in ready.iterdir()}

    async def prepare() -> None:
        state = await QuarantineMaintenance(store, None, OpportunisticRuntime()).prepare_pending_startup()
        assert state.pending is None

    asyncio.run(prepare())

    assert {path.name: (path / "records.bin").read_bytes() for path in ready.iterdir()} == original_ready
    assert (attempt.path / "records.bin").read_bytes() == digest
    assert not tuple(store.capture_root.iterdir())
    assert loads(store.ready_closures_path.read_text())["closures"] == []
    assert (attempt.path / "terminal-retired.json").is_file()
    assert store.pending_attempts() == ()


def test_interrupted_close_publishes_ordinary_pending_prefix(tmp_path: Path) -> None:
    store, attempt = _partial(tmp_path)

    closure = store.close_pending_prefix("absence")

    assert closure is not None and closure.next_sequence == 11
    assert (attempt.path / "prefix-publication.json").is_file()
    assert (attempt.path / "terminal-retired.json").is_file()
    assert store.pending_attempts() == ()
