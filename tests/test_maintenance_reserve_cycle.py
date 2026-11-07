from __future__ import annotations

import asyncio
import json
import os
from collections.abc import Callable
from pathlib import Path
from types import SimpleNamespace
from typing import cast

import pytest

from omi_collector.capture.adapters.collector_state_db import StateKind
from omi_collector.capture.adapters.internal_audio_store import InternalAudioStore
from omi_collector.capture.adapters.opportunistic_runtime import OpportunisticRuntime
from omi_collector.capture.adapters.ready_bundles import ReadyOutcome, ReadyOutcomeState
from omi_collector.capture.application.quarantine_maintenance import QuarantineMaintenance, _CleanupPhase, _CleanupState
from omi_collector.capture.application.session_lifecycle import joined_to_thread
from omi_collector.config import CollectorConfig, ReadyConfig
from staging_test_factory import StagingStore, attempt_source
from test_public_audio_v2_integration import _checkpoint, _ready_root, _record, _source


def test_ack_retirement_releases_reserve_for_waiting_publication(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = CollectorConfig(ready=ReadyConfig(target_audio_seconds=0.02))
    draft = tmp_path / "draft"
    draft.mkdir()
    bootstrap = StagingStore(tmp_path / "spool", draft, config=config)
    ready = _ready_root(tmp_path)
    store = StagingStore.from_paths(bootstrap.paths, state_db=bootstrap.state_db, publication_root=ready, config=config)
    with InternalAudioStore(store.state_db) as audio:
        _source(audio, "a" * 32, 10, (_record(100),))
    store.append_ready_closure(11, "drained")
    (first,) = store.recover_and_publish().published
    manifest = cast(object, json.loads((first.path / "manifest.json").read_text()))
    assert isinstance(manifest, dict)
    digest = cast(dict[str, object], manifest).get("audio_sha256")
    assert isinstance(digest, str)
    _checkpoint(tmp_path, [{"id": first.bundle_id, "audio_sha256": digest}])
    with InternalAudioStore(store.state_db) as audio:
        _source(audio, "b" * 32, 11, (_record(101),))
    store.append_ready_closure(12, "drained")
    statvfs = os.statvfs

    def space(path: str | bytes | os.PathLike[str] | os.PathLike[bytes]) -> object:
        if Path(os.fsdecode(path)) == ready:
            return SimpleNamespace(f_frsize=4096, f_bavail=0 if first.path.exists() else 10**8)
        return statvfs(path)

    monkeypatch.setattr("omi_collector.capture.adapters.ready_bundles.os.statvfs", space)
    waiting = store.recover_and_publish()
    assert waiting.state == "transient"
    assert waiting.reason == "insufficient filesystem space for typed ready publication"
    assert waiting.published == ()
    assert first.path.exists()
    assert list(ready.iterdir()) == [first.path]
    assert store._publication_space is not None
    assert any(path == ready and required > 0 for path, required in store._publication_space[1])

    async def run() -> None:
        maintenance = QuarantineMaintenance(store, None, OpportunisticRuntime())
        maintenance._cleanup_state = _CleanupState(_CleanupPhase.WAITING, 0)
        try:
            await maintenance.run_once(lambda: False)
        finally:
            await maintenance.close()

    asyncio.run(run())
    assert not first.path.exists()
    ledger = store.state_db.get(StateKind.READY_PUBLICATION, first.bundle_id)
    assert ledger is not None
    assert ledger["state"] == "retired"
    assert len(list(ready.iterdir())) == 1


@pytest.mark.parametrize(
    ("publication_state", "expected"), [("transient", ["raw", "expiry", "vacuum"]), ("blocked", [])]
)
def test_publication_failure_controls_independent_cleanup(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, publication_state: str, expected: list[str]
) -> None:
    draft = tmp_path / "draft"
    draft.mkdir()
    store = StagingStore(tmp_path / "spool", draft)
    maintenance = QuarantineMaintenance(store, None, OpportunisticRuntime())
    calls: list[str] = []

    async def publish() -> ReadyOutcome:
        return ReadyOutcome(ReadyOutcomeState(publication_state), reason="failure")

    async def phase(_should_defer: Callable[[], bool]) -> bool:
        calls.append(maintenance._cleanup_state.phase.value)
        return True

    monkeypatch.setattr(maintenance, "_recover_and_publish", publish)
    monkeypatch.setattr(maintenance, "_run_cleanup_phase", phase)
    maintenance._cleanup_state = _CleanupState(_CleanupPhase.PUBLICATION, 0, float("inf"))

    # Enter at publication after the already completed ACK phase.
    async def begin(_should_defer: Callable[[], bool]) -> None:
        return None

    monkeypatch.setattr(maintenance, "_begin_cleanup_window", begin)
    asyncio.run(maintenance.run_once(lambda: False))
    assert calls == expected


def test_transient_publication_allows_covered_reclamation_and_compaction(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = CollectorConfig(ready=ReadyConfig(target_audio_seconds=0.02))
    draft = tmp_path / "draft"
    draft.mkdir()
    bootstrap = StagingStore(tmp_path / "spool", draft, config=config)
    ready = _ready_root(tmp_path)
    store = StagingStore.from_paths(bootstrap.paths, state_db=bootstrap.state_db, publication_root=ready, config=config)
    with InternalAudioStore(store.state_db) as audio:
        _source(audio, "a" * 32, 10, tuple(_record(value) for value in range(100, 228)))
    store.append_ready_closure(138, "drained")
    (first,) = store.recover_and_publish().published
    with InternalAudioStore(store.state_db) as audio:
        _source(audio, "b" * 32, 138, (_record(228),))
    store.append_ready_closure(139, "drained")
    statvfs = os.statvfs
    reclaimed = False
    compacted: list[ReadyOutcome | None] = []
    compact = store.compact_after_cleanup

    def space(path: str | bytes | os.PathLike[str] | os.PathLike[bytes]) -> object:
        if Path(os.fsdecode(path)) == ready:
            return SimpleNamespace(f_frsize=4096, f_bavail=10**8 if reclaimed else 0)
        return statvfs(path)

    def compact_after_cleanup(*, should_defer: Callable[[], bool]) -> ReadyOutcome | None:
        nonlocal reclaimed
        assert attempt_source(store, "a" * 32) is None
        outcome = compact(should_defer=should_defer)
        compacted.append(outcome)
        reclaimed = True
        return outcome

    monkeypatch.setattr("omi_collector.capture.adapters.ready_bundles.os.statvfs", space)
    monkeypatch.setattr(store, "compact_after_cleanup", compact_after_cleanup)
    waiting = store.recover_and_publish()
    assert waiting.state == "transient"
    assert first.path.exists()

    async def run() -> None:
        maintenance = QuarantineMaintenance(store, None, OpportunisticRuntime())
        maintenance._cleanup_state = _CleanupState(_CleanupPhase.WAITING, 0)
        try:
            await maintenance.run_once(lambda: False)
            assert maintenance._cleanup_state.phase is _CleanupPhase.WAITING
            assert reclaimed and len(compacted) == 1
            assert compacted[0] is not None and compacted[0].reason == "compaction_done"
            assert first.path.exists()
            assert await joined_to_thread(attempt_source, store, "b" * 32) is not None
            schedule = store.publication_retry_schedule()
            assert schedule is not None
            assert maintenance._publication_retry_handle is not None
            monkeypatch.setattr("omi_collector.capture.adapters.staging_store.monotonic", lambda: schedule[1] + 1)
            assert (await joined_to_thread(store.recover_and_publish)).state == "published"
        finally:
            await maintenance.close()

    asyncio.run(run())
