from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path
from types import SimpleNamespace

import pytest

from omi_collector.capture.adapters.collector_state_db import StateKind
from omi_collector.capture.adapters.internal_audio_store import InternalAudioStore
from omi_collector.capture.adapters.opportunistic_runtime import OpportunisticRuntime
from omi_collector.capture.application.quarantine_maintenance import QuarantineMaintenance, _CleanupPhase, _CleanupState
from omi_collector.config import CollectorConfig, ReadyConfig
from staging_test_factory import StagingStore
from test_public_audio_v2_integration import _checkpoint, _ready_root, _record, _source


def test_ack_retirement_releases_reserve_for_waiting_publication(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    config = CollectorConfig(ready=ReadyConfig(target_audio_seconds=0.02))
    draft = tmp_path / "draft"
    draft.mkdir()
    bootstrap = StagingStore(tmp_path / "spool", draft, config=config)
    ready = _ready_root(tmp_path)
    store = StagingStore.from_paths(bootstrap.paths, state_db=bootstrap.state_db, publication_root=ready, config=config)
    with InternalAudioStore(store.state_db) as audio:
        _source(audio, "a" * 32, 10, (_record(100),))
    store.append_ready_closure(11, "drained")
    first, = store.recover_and_publish().published
    manifest = json.loads((first.path / "manifest.json").read_text())
    _checkpoint(tmp_path, [{"id": first.bundle_id, "audio_sha256": manifest["audio_sha256"]}])
    with InternalAudioStore(store.state_db) as audio:
        _source(audio, "b" * 32, 11, (_record(101),))
    store.append_ready_closure(12, "drained")
    statvfs = os.statvfs

    def space(path: str | bytes | os.PathLike[str] | os.PathLike[bytes]) -> object:
        if Path(path) == ready:
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
    assert store.state_db.get(StateKind.READY_PUBLICATION, first.bundle_id)["state"] == "retired"
    assert len(list(ready.iterdir())) == 1
