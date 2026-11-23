from __future__ import annotations

import hashlib
import json
from contextlib import closing
from functools import partial
from pathlib import Path
from typing import cast

import pytest

from omi_collector.capture.adapters import physical_stage, ready_bundles
from omi_collector.capture.adapters.collector_state_db import StateDB, StateKind
from omi_collector.capture.adapters.internal_audio_store import DrainCheckpoint, InternalAudioStore
from omi_collector.capture.adapters.physical_audio import PreparedPhysicalAudio, prepare_physical_audio
from omi_collector.capture.adapters.physical_proof import prepare_physical_proof
from omi_collector.capture.adapters.physical_source import authenticate_source, read_physical_span
from omi_collector.capture.adapters.v2_audio import hash_v2_audio
from omi_collector.capture.domain.clock_state import ClockState
from omi_collector.capture.domain.physical_recovery import PhysicalSpan
from test_public_audio_v2_integration import _checkpoint, _options, _ready_root, _record, _source


def _sha(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _seed(store: InternalAudioStore, db: StateDB) -> tuple[dict[str, object], bytes, bytes]:
    first, last, repeated = _record(1, marker=10), _record(2, marker=11), _record(3, marker=12)
    packet = bytes((8, 42))
    seam = bytes(4) + bytes((2,)) + packet + bytes((255,)) + bytes(524)
    anchor = repeated[:356]
    original = first + seam + last + anchor
    source_id, repeat_id = "a" * 32, "b" * 32
    store.create_source(source_id, "attempt", 100, 104)
    store.prepare_leg(source_id, 100, 4)
    store.record_read_begin(source_id, 100, 4)
    store.commit_drain(source_id, 0, (original[i : i + 444] for i in range(0, len(original), 444)), DrainCheckpoint(1))
    _source(store, repeat_id, 200, (repeated,))
    with db.typed_write_transaction() as connection:
        connection.execute(
            "INSERT INTO capture_historical_custody(source_id,prior_source_type,prior_lifecycle,prior_count,"
            "prior_sha256,prior_certified_count,prior_coverage_digest,disposition) "
            "SELECT source_id,source_type,lifecycle,committed_count,committed_sha256,NULL,NULL,'pending' "
            "FROM capture_source WHERE source_id=?",
            (source_id,),
        )
        connection.execute(
            "INSERT INTO capture_historical_custody(source_id,prior_source_type,prior_lifecycle,prior_count,"
            "prior_sha256,prior_certified_count,prior_coverage_digest,disposition) "
            "SELECT source_id,source_type,lifecycle,committed_count,committed_sha256,NULL,NULL,'pending' "
            "FROM capture_source WHERE source_id=?",
            (repeat_id,),
        )
    packet_hash = _sha(b"omi-physical-seam-packets-v1\0" + len(packet).to_bytes(4, "big") + packet)
    descriptor: dict[str, object] = {
        "format": "omi-audio-v2-physical-byte-span-v1",
        "source_id": source_id,
        "source_sha256": _sha(original),
        "source_bytes": len(original),
        "spans": [
            {"byte_start": 0, "byte_end": 444, "sha256": _sha(first)},
            {"byte_start": 976, "byte_end": 1420, "sha256": _sha(last)},
        ],
        "record_count": 2,
        "seams": [
            {
                "byte_start": 444,
                "byte_end": 976,
                "sha256": _sha(seam),
                "status": "unresolved",
                "decoded_prefix_bytes": 7,
                "decoded_packet_count": 1,
                "decoded_packets_sha256": packet_hash,
            }
        ],
        "anchor": {
            "repeat_source_id": repeat_id,
            "repeat_source_sha256": _sha(repeated),
            "source_offset": 1420,
            "repeat_offset": 0,
            "length": len(anchor),
            "sha256": _sha(anchor),
        },
        "record_projection_sha256": "0" * 64,
    }
    return descriptor, original, repeated


def _assert_original_custody(db: StateDB, original: bytes, repeated: bytes) -> None:
    with closing(db.open_connection(read_only=True)) as connection:
        for source_id, expected in (("a" * 32, original), ("b" * 32, repeated)):
            assert b"".join(read_physical_span(connection, PhysicalSpan(source_id, 0, len(expected)))) == expected
        assert (
            connection.execute(
                "SELECT disposition FROM capture_historical_custody WHERE source_id=?", ("a" * 32,)
            ).fetchone()[0]
            == "pending"
        )


def _assert_duplicate_is_rejected(
    db: StateDB, store: InternalAudioStore, root: Path, descriptor: dict[str, object], prepared: PreparedPhysicalAudio
) -> None:
    with closing(db.open_connection(read_only=True)) as connection, db.access(connection):
        reader = partial(read_physical_span, connection)
        source_identity = partial(authenticate_source, connection)
        with (
            prepare_physical_proof(descriptor, reader, source_identity, packet_count=3) as proof,
            pytest.raises(ready_bundles.ReadyBundleError, match="overlaps existing custody"),
        ):
            physical_stage.reserve_physical_stage(store, root, descriptor, prepared, proof)


def _publish_prior(store: InternalAudioStore, root: Path) -> None:
    prior = _source(store, "c" * 32, 99, (_record(4),))
    earlier = ready_bundles.finalize_public_audio_v2(store, (prior,), root, _options(ClockState(), 100))
    assert len(earlier.published) == 1


def _assert_and_ack_physical(db: StateDB, root: Path, tmp_path: Path, result: ready_bundles.ReadyBundleResult) -> None:
    manifest = cast(dict[str, object], json.loads((result.path / "manifest.json").read_bytes()))
    assert manifest["packet_count"] == 3
    assert manifest["audio_sha256"] == hash_v2_audio((bytes((8, 10)), bytes((8, 42)), bytes((8, 11)))).sha256
    assert "source_id" not in manifest and "start_sequence" not in manifest
    checkpoint = _checkpoint(tmp_path, [{"id": result.bundle_id, "audio_sha256": cast(str, manifest["audio_sha256"])}])
    retired = ready_bundles.retire_acknowledged_public_audio_v2(root, db, checkpoint)
    assert len(retired) == 1 and retired[0].next_sequence is None
    assert ready_bundles.retire_acknowledged_public_audio_v2(root, db, checkpoint) == ()


@pytest.mark.parametrize("crash", [False, True])
def test_physical_stage_to_ready_ack_preserves_original_and_sequence_frontier(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, crash: bool
) -> None:
    db = StateDB(tmp_path / "state.sqlite3")
    store = InternalAudioStore(db)
    try:
        root = _ready_root(tmp_path)
        descriptor, original, repeated = _seed(store, db)
        snapshot_db = StateDB(db.path)
        with closing(snapshot_db.open_connection(read_only=True)) as connection, snapshot_db.access(connection):
            connection.execute("BEGIN")
            source_identity = partial(authenticate_source, connection)
            reader = partial(read_physical_span, connection)
            prepared = prepare_physical_audio(descriptor, reader, source_identity)
            descriptor["record_projection_sha256"] = prepared.record_projection_sha256
            with prepare_physical_proof(
                descriptor, reader, source_identity, packet_count=prepared.audio.packet_count
            ) as proof:
                connection.rollback()
                with pytest.raises(ready_bundles.ReadyBundleError, match="earlier audio must be published"):
                    physical_stage.reserve_physical_stage(store, root, descriptor, prepared, proof)
                _publish_prior(store, root)
                before_frontier = db.get(StateKind.PUBLICATION_V2_FRONTIER, "current")
                stage = physical_stage.reserve_physical_stage(store, root, descriptor, prepared, proof)
                stage_input = physical_stage.PhysicalStageInput(
                    descriptor, prepared, proof.records_sha256, reader, source_identity
                )
                if crash:
                    with monkeypatch.context() as patch:
                        patch.setattr(
                            physical_stage,
                            "_commit_physical_stage",
                            lambda *_args: (_ for _ in ()).throw(OSError("crash")),
                        )
                        with pytest.raises(OSError, match="crash"):
                            physical_stage.publish_physical_stage(store, stage, stage_input)
                    assert (root / stage.artifact_id).is_dir()
                    result = physical_stage.resume_physical_stage(store, root)
                    assert result is not None
                else:
                    result = physical_stage.publish_physical_stage(store, stage, stage_input)
        assert result.bundle_id == stage.artifact_id and result.next_sequence is None
        assert db.get(StateKind.PUBLICATION_V2_FRONTIER, "current") == before_frontier
        _assert_and_ack_physical(db, root, tmp_path, result)
        _assert_duplicate_is_rejected(snapshot_db, store, root, descriptor, prepared)
        _assert_original_custody(db, original, repeated)
        assert db.get(StateKind.PUBLICATION_V2_FRONTIER, "current") == before_frontier
    finally:
        store.close()
