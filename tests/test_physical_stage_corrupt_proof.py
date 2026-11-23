from __future__ import annotations

from contextlib import closing
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
from test_public_audio_v2_integration import _checkpoint, _options, _ready_root, _record, _source


def _corrupt_first_proof_page(db: StateDB, artifact_id: str) -> None:
    with db.typed_write_transaction() as writer:
        changed = writer.execute(
            "UPDATE ready_replay_proof_page SET hashes=zeroblob(length(hashes)) WHERE bundle_id=? AND page_ordinal=0",
            (artifact_id,),
        )
        assert changed.rowcount == 1


def test_corrupt_reserved_physical_proof_blocks_ready_and_ack_preserves_raw(tmp_path: Path) -> None:
    db = StateDB(tmp_path / "state.sqlite3")
    store = InternalAudioStore(db)
    try:
        root = _ready_root(tmp_path)
        descriptor, original, repeated = _seed(store, db)
        prior = _source(store, "c" * 32, 99, (_record(4),))
        assert ready_bundles.finalize_public_audio_v2(store, (prior,), root, _options(ClockState(), 100)).published
        frontier = db.get(StateKind.PUBLICATION_V2_FRONTIER, "current")
        snapshot = StateDB(db.path)
        with closing(snapshot.open_connection(read_only=True)) as connection, snapshot.access(connection):
            connection.execute("BEGIN")
            read = partial(read_physical_span, connection)
            identity = partial(authenticate_source, connection)
            prepared = prepare_physical_audio(descriptor, read, identity)
            descriptor["record_projection_sha256"] = prepared.record_projection_sha256
            with prepare_physical_proof(descriptor, read, identity, packet_count=3) as proof:
                connection.rollback()
                stage = physical_stage.reserve_physical_stage(store, root, descriptor, prepared, proof)
                stage_input = physical_stage.PhysicalStageInput(
                    descriptor, prepared, proof.records_sha256, read, identity
                )
                _corrupt_first_proof_page(db, stage.artifact_id)
                with pytest.raises(ready_bundles.ReadyDurabilityFatalError, match="proof pages are corrupt"):
                    physical_stage.publish_physical_stage(store, stage, stage_input)
        assert not (root / stage.artifact_id).exists()
        assert db.get(StateKind.READY_PUBLICATION, stage.artifact_id) is None
        assert db.get(StateKind.PUBLICATION_V2_FRONTIER, "current") == frontier
        checkpoint = _checkpoint(tmp_path, [{"id": stage.artifact_id, "audio_sha256": prepared.audio.sha256}])
        with pytest.raises(ready_bundles.ReadyBundleError, match="not a published artifact"):
            ready_bundles.retire_acknowledged_public_audio_v2(root, db, checkpoint)
        _assert_original_custody(db, original, repeated)
        assert db.get(StateKind.PUBLICATION_V2_FRONTIER, "current") == frontier
    finally:
        store.close()
