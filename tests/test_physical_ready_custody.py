from __future__ import annotations

import json
from contextlib import closing
from copy import deepcopy
from pathlib import Path

import pytest

from omi_collector.capture.adapters import ready_bundles as ready
from omi_collector.capture.adapters.collector_state_db import StateDB, StateKind
from omi_collector.capture.adapters.internal_audio_store import InternalAudioStore
from omi_collector.capture.domain.clock_state import ClockState
from test_public_audio_v2_integration import _checkpoint, _options, _ready_root, _record, _source


def _physical_entry() -> dict[str, object]:
    return {
        "format": "omi-audio-v2",
        "state": "ready",
        "order": 0,
        "audio_sha256": "a" * 64,
        "audio_bytes": 100,
        "manifest_sha256": "b" * 64,
        "manifest_bytes": 100,
        "records_sha256": "c" * 64,
        "packet_count": 1,
        "lineage_kind": "physical_byte_span",
        "source_id": "d" * 32,
        "source_sha256": "e" * 64,
        "source_bytes": 891,
        "spans": [{"byte_start": 0, "byte_end": 444, "sha256": "f" * 64}],
        "seams": [
            {
                "byte_start": 444,
                "byte_end": 447,
                "sha256": "1" * 64,
                "status": "unresolved",
                "decoded_prefix_bytes": 2,
                "decoded_packet_count": 1,
                "decoded_packets_sha256": "2" * 64,
            }
        ],
        "record_count": 1,
    }


def test_physical_ledger_rejects_sequence_claims_and_duplicate_emitted_bytes() -> None:
    first = _physical_entry()
    assert ready._valid_ledger_entry("0" * 64, first)
    invalid = {**first, "next_sequence": 123}
    assert not ready._valid_ledger_entry("0" * 64, invalid)
    second = deepcopy(first)
    second["state"] = "retired"
    with pytest.raises(ready.ReadyBundleError, match="overlaps"):
        ready._validate_ledger_entries({"0" * 64: first, "1" * 64: second})
    second["spans"] = [{"byte_start": 445, "byte_end": 889, "sha256": "3" * 64}]
    second["seams"] = []
    with pytest.raises(ready.ReadyBundleError, match="overlaps"):
        ready._validate_ledger_entries({"0" * 64: first, "1" * 64: second})
    second["spans"] = [{"byte_start": 447, "byte_end": 891, "sha256": "3" * 64}]
    ready._validate_ledger_entries({"0" * 64: first, "1" * 64: second})
    second["source_sha256"] = "9" * 64
    with pytest.raises(ready.ReadyBundleError, match="identity conflicts"):
        ready._validate_ledger_entries({"0" * 64: first, "1" * 64: second})


def _convert_fixture(db: StateDB, artifact: ready.ReadyBundleResult) -> dict[str, object]:
    old = db.get(StateKind.READY_PUBLICATION, artifact.bundle_id)
    assert old is not None
    row = next(row for row in ready._v2_replay_proof_rows(db) if row[0] == artifact.bundle_id)
    proof = ready._parse_v2_active_proof_row(row, artifact.bundle_id)
    projection = ready._v2_descriptor_basis(proof.descriptor)[3]
    entry = {
        key: old[key]
        for key in (
            "format",
            "state",
            "order",
            "audio_sha256",
            "audio_bytes",
            "manifest_sha256",
            "manifest_bytes",
            "packet_count",
            "records_sha256",
        )
    }
    entry.update(
        {
            "lineage_kind": "physical_byte_span",
            "source_id": "a" * 32,
            "source_sha256": "b" * 64,
            "source_bytes": 447,
            "spans": [{"byte_start": 0, "byte_end": 444, "sha256": "c" * 64}],
            "seams": [],
            "record_count": 1,
        }
    )
    descriptor = {
        key: entry[key] for key in ("source_id", "source_sha256", "source_bytes", "spans", "seams", "record_count")
    }
    descriptor.update(
        {
            "format": "omi-audio-v2-physical-byte-span-v1",
            "record_projection_sha256": projection,
            "anchor": {
                "repeat_source_id": "f" * 32,
                "repeat_source_sha256": "1" * 64,
                "source_offset": 444,
                "repeat_offset": 0,
                "length": 3,
                "sha256": "2" * 64,
            },
        }
    )
    encoded = ready._canonical(descriptor)
    content = ready._V2ProofContent(
        artifact.bundle_id,
        proof.order,
        proof.audio_sha256,
        proof.manifest_sha256,
        proof.records_sha256,
        proof.record_count,
        proof.packet_count,
        projection,
        proof.page_index,
        proof.packed_runs,
        encoded,
    )
    with db.typed_write_transaction() as connection:
        connection.execute(
            "UPDATE state SET payload=? WHERE kind=? AND id=?",
            (
                json.dumps(entry, sort_keys=True, separators=(",", ":")),
                StateKind.READY_PUBLICATION.value,
                artifact.bundle_id,
            ),
        )
        connection.execute(
            "UPDATE ready_replay_proof SET immutable_metadata=?,proof_content_sha256=? WHERE bundle_id=?",
            (encoded, ready._v2_proof_content_sha256(content), artifact.bundle_id),
        )
    return entry


def test_physical_proof_rejects_changed_ledger_lineage(tmp_path: Path) -> None:
    db = StateDB(tmp_path / "state.sqlite3")
    store = InternalAudioStore(db)
    try:
        root = _ready_root(tmp_path)
        source = _source(store, "a" * 32, 10, (_record(100),))
        artifact = ready.finalize_public_audio_v2(store, (source,), root, _options(ClockState(), 11)).published[0]
        entry = _convert_fixture(db, artifact)
        db.put(StateKind.READY_PUBLICATION, artifact.bundle_id, {**entry, "source_sha256": "9" * 64})
        with pytest.raises(ready.ReadyDurabilityFatalError, match="active v2 replay proof is corrupt"):
            ready._v2_ready_custody(root, db)
        assert artifact.path.exists()
    finally:
        store.close()


@pytest.mark.parametrize("crash", (False, True))
def test_physical_ready_ack_preserves_raw_custody_and_resumes_after_crash(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, crash: bool
) -> None:
    db = StateDB(tmp_path / "state.sqlite3")
    store = InternalAudioStore(db)
    try:
        root = _ready_root(tmp_path)
        source = _source(store, "a" * 32, 10, (_record(100),))
        artifact = ready.finalize_public_audio_v2(store, (source,), root, _options(ClockState(), 11)).published[0]
        entry = _convert_fixture(db, artifact)
        with db.typed_write_transaction() as connection:
            connection.execute(
                "INSERT INTO capture_historical_custody(source_id,prior_source_type,prior_lifecycle,prior_count,"
                "prior_sha256,prior_certified_count,prior_coverage_digest,disposition) "
                "SELECT source_id,source_type,'sealed_waiting',committed_count,committed_sha256,NULL,NULL,'pending' "
                "FROM capture_source WHERE source_id=?",
                ("a" * 32,),
            )
        cache = ready._v2_ready_custody(root, db)
        assert cache.custody.active == () and artifact.bundle_id in cache.custody.proofs
        checkpoint = _checkpoint(tmp_path, [{"id": artifact.bundle_id, "audio_sha256": "0" * 64}])
        with pytest.raises(ready.ReadyBundleError, match="not a published artifact"):
            ready.retire_acknowledged_public_audio_v2(root, db, checkpoint)
        assert artifact.path.exists()
        checkpoint.write_text(
            json.dumps(
                {
                    "audio_contract": "omi-audio-v2",
                    "analysis_cursor": None,
                    "vad_decisions": [],
                    "open_speech_tail": None,
                    "acknowledged": [{"id": artifact.bundle_id, "audio_sha256": entry["audio_sha256"]}],
                }
            )
        )
        if crash:
            with monkeypatch.context() as patch:
                patch.setattr(ready, "_remove_v2_artifact", lambda *_args: (_ for _ in ()).throw(OSError("crash")))
                with pytest.raises(OSError, match="crash"):
                    ready.retire_acknowledged_public_audio_v2(root, db, checkpoint)
        else:
            retired = ready.retire_acknowledged_public_audio_v2(root, db, checkpoint)
            assert len(retired) == 1 and retired[0].next_sequence is None
        assert db.get(StateKind.READY_PUBLICATION, artifact.bundle_id) == {**entry, "state": "retired"}
        assert artifact.path.exists() is crash
        assert ready.retire_acknowledged_public_audio_v2(root, db, checkpoint) == ()
        assert artifact.path.exists() is crash
        ready.resume_retired_public_audio_v2(root, db)
        assert not artifact.path.exists()
        assert not ready.retire_one_covered_typed_source(store, 128)
        with closing(db.open_connection(read_only=True)) as connection:
            assert (
                connection.execute("SELECT count(*) FROM capture_record WHERE source_id=?", ("a" * 32,)).fetchone()[0]
                == 1
            )
    finally:
        store.close()
