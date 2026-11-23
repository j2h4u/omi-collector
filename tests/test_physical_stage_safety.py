from __future__ import annotations

from collections.abc import Iterator
from contextlib import closing, contextmanager
from functools import partial
from pathlib import Path

import pytest

from omi_collector.capture.adapters import physical_stage, ready_bundles
from omi_collector.capture.adapters.collector_state_db import StateDB, StateKind
from omi_collector.capture.adapters.internal_audio_store import InternalAudioStore
from omi_collector.capture.adapters.physical_audio import prepare_physical_audio
from omi_collector.capture.adapters.physical_proof import prepare_physical_proof
from omi_collector.capture.adapters.physical_source import authenticate_source, read_physical_span
from omi_collector.capture.domain.clock_state import ClockState
from test_physical_stage_e2e import _assert_original_custody, _seed
from test_public_audio_v2_integration import _options, _ready_root, _record, _source


@contextmanager
def _reserved(
    root: Path,
) -> Iterator[
    tuple[
        StateDB, InternalAudioStore, Path, physical_stage.PhysicalStage, physical_stage.PhysicalStageInput, bytes, bytes
    ]
]:
    db = StateDB(root / "state.sqlite3")
    store = InternalAudioStore(db)
    try:
        ready = _ready_root(root)
        descriptor, original, repeated = _seed(store, db)
        prior = _source(store, "c" * 32, 99, (_record(4),))
        assert ready_bundles.finalize_public_audio_v2(store, (prior,), ready, _options(ClockState(), 100)).published
        snapshot = StateDB(db.path)
        with closing(snapshot.open_connection(read_only=True)) as connection, snapshot.access(connection):
            read = partial(read_physical_span, connection)
            identity = partial(authenticate_source, connection)
            prepared = prepare_physical_audio(descriptor, read, identity)
            descriptor["record_projection_sha256"] = prepared.record_projection_sha256
            with prepare_physical_proof(descriptor, read, identity, packet_count=3) as proof:
                stage = physical_stage.reserve_physical_stage(store, ready, descriptor, prepared, proof)
                source = physical_stage.PhysicalStageInput(descriptor, prepared, proof.records_sha256, read, identity)
                yield db, store, ready, stage, source, original, repeated
    finally:
        store.close()


@pytest.mark.parametrize("marker_state", ["missing", "changed"])
def test_resume_rejects_missing_or_changed_stage_marker_without_advancing(tmp_path: Path, marker_state: str) -> None:
    with _reserved(tmp_path) as (db, store, root, stage, _source_input, original, repeated):
        frontier = db.get(StateKind.PUBLICATION_V2_FRONTIER, "current")
        order = ready_bundles.publication_contract_state(db).next_public_order
        if marker_state == "missing":
            assert db.delete(StateKind.PHYSICAL_IMPORT, stage.stage_id)
        else:
            db.put(StateKind.PHYSICAL_IMPORT, stage.stage_id, {"state": "published"})
        with pytest.raises(ready_bundles.ReadyDurabilityFatalError, match="no durable owner"):
            physical_stage.resume_physical_stage(store, root)
        assert db.get(StateKind.READY_PUBLICATION, stage.artifact_id) is None
        assert db.get(StateKind.PUBLICATION_V2_FRONTIER, "current") == frontier
        assert ready_bundles.publication_contract_state(db).next_public_order == order
        _assert_original_custody(db, original, repeated)


@pytest.mark.parametrize("location", ["staging", "destination"])
def test_resume_rejects_staged_audio_symlink_without_activation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, location: str
) -> None:
    with _reserved(tmp_path) as (db, store, root, stage, source, original, repeated):
        frontier = db.get(StateKind.PUBLICATION_V2_FRONTIER, "current")
        order = ready_bundles.publication_contract_state(db).next_public_order
        with monkeypatch.context() as patch:
            patch.setattr(
                physical_stage, "_commit_physical_stage", lambda *_args: (_ for _ in ()).throw(OSError("crash"))
            )
            with pytest.raises(OSError, match="crash"):
                physical_stage.publish_physical_stage(store, stage, source)
        destination = root / stage.artifact_id
        if location == "staging":
            destination.rename(stage.path)
        audio = (stage.path if location == "staging" else destination) / "audio.ogg"
        audio.unlink()
        external = tmp_path / "external"
        external.write_bytes(b"unchanged")
        audio.symlink_to(external)
        with pytest.raises(ready_bundles.ReadyBundleError, match="unsafe"):
            physical_stage.resume_physical_stage(store, root)
        assert audio.is_symlink() and external.read_bytes() == b"unchanged"
        assert db.get(StateKind.READY_PUBLICATION, stage.artifact_id) is None
        assert db.get(StateKind.PUBLICATION_V2_FRONTIER, "current") == frontier
        assert ready_bundles.publication_contract_state(db).next_public_order == order
        _assert_original_custody(db, original, repeated)
