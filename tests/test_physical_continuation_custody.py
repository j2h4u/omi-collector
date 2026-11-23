from __future__ import annotations

import hashlib
from contextlib import closing
from pathlib import Path
from typing import cast

import pytest

from omi_collector.capture.adapters.collector_state_db import StateDB, StateKind
from omi_collector.capture.adapters.internal_audio_store import InternalAudioStore
from omi_collector.capture.adapters.physical_continuation import physical_continuation
from omi_collector.capture.adapters.ready_bundles import _canonical
from test_physical_ready_custody import _physical_entry
from test_physical_stage_e2e import _seed


def _prepared(db: StateDB) -> tuple[str, dict[str, object], dict[str, object]]:
    with InternalAudioStore(db) as store:
        descriptor, _, _ = _seed(store, db)
    artifact_id = "9" * 64
    entry = _physical_entry()
    entry.update(
        {
            key: descriptor[key]
            for key in ("source_id", "source_sha256", "source_bytes", "spans", "seams", "record_count")
        }
    )
    digest = hashlib.sha256(_canonical(descriptor)).hexdigest()
    marker_id = hashlib.sha256(f"{digest}:0:{artifact_id}".encode()).hexdigest()
    marker = {
        "state": "published",
        "artifact_id": artifact_id,
        "source_id": descriptor["source_id"],
        "anchor": descriptor["anchor"],
        "record_projection_sha256": descriptor["record_projection_sha256"],
        "descriptor_sha256": digest,
    }
    db.put(StateKind.PHYSICAL_IMPORT, marker_id, marker)
    db.put(StateKind.READY_PUBLICATION, artifact_id, entry)
    return marker_id, marker, entry


@pytest.mark.parametrize(
    "defect",
    [
        "missing_marker",
        "unpublished",
        "artifact_type",
        "missing_ledger",
        "bad_ledger",
        "marker_source",
        "descriptor_hash",
        "stage_identity",
        "missing_repeat_custody",
        "prior_hash",
        "prior_lifecycle",
        "current_type",
        "current_lifecycle",
        "prior_count",
        "sequence_limit",
    ],
)
def test_continuation_refuses_changed_or_incomplete_custody(tmp_path: Path, defect: str) -> None:
    db = StateDB(tmp_path / "state.sqlite3")
    marker_id, marker, entry = _prepared(db)
    artifact_id = cast(str, marker["artifact_id"])
    if defect == "missing_marker":
        db.delete(StateKind.PHYSICAL_IMPORT, marker_id)
    elif defect == "unpublished":
        db.put(StateKind.PHYSICAL_IMPORT, marker_id, {**marker, "state": "staging"})
    elif defect == "artifact_type":
        db.put(StateKind.PHYSICAL_IMPORT, marker_id, {**marker, "artifact_id": 3})
    elif defect == "missing_ledger":
        db.delete(StateKind.READY_PUBLICATION, artifact_id)
    elif defect == "bad_ledger":
        db.put(StateKind.READY_PUBLICATION, artifact_id, {**entry, "audio_sha256": "invalid"})
    elif defect == "marker_source":
        db.put(StateKind.PHYSICAL_IMPORT, marker_id, {**marker, "source_id": "f" * 32})
    elif defect == "descriptor_hash":
        db.put(StateKind.PHYSICAL_IMPORT, marker_id, {**marker, "descriptor_sha256": "0" * 64})
    elif defect == "stage_identity":
        db.put(StateKind.PHYSICAL_IMPORT, "f" * 64, marker)
        marker_id = "f" * 64
    else:
        _damage_repeat(db, defect)
    with closing(db.open_connection(read_only=True)) as connection:
        connection.execute("BEGIN")
        with pytest.raises(ValueError):
            physical_continuation(connection, marker_id)


def _damage_repeat(db: StateDB, defect: str) -> None:
    repeat = "b" * 32
    with db.typed_write_transaction() as connection:
        if defect == "missing_repeat_custody":
            connection.execute("DELETE FROM capture_historical_custody WHERE source_id=?", (repeat,))
        elif defect == "prior_hash":
            connection.execute(
                "UPDATE capture_historical_custody SET prior_sha256=? WHERE source_id=?", ("0" * 64, repeat)
            )
        elif defect == "prior_lifecycle":
            connection.execute(
                "UPDATE capture_historical_custody SET prior_lifecycle='blocked' WHERE source_id=?", (repeat,)
            )
        elif defect == "current_type":
            connection.execute("UPDATE capture_source SET source_type='draft' WHERE source_id=?", (repeat,))
        elif defect == "current_lifecycle":
            connection.execute("UPDATE capture_source SET lifecycle='covered' WHERE source_id=?", (repeat,))
        elif defect == "prior_count":
            connection.execute("UPDATE capture_historical_custody SET prior_count=2 WHERE source_id=?", (repeat,))
        elif defect == "sequence_limit":
            connection.execute(
                "UPDATE capture_source SET sequence_limit=? WHERE source_id=?", ((202).to_bytes(8, "big"), repeat)
            )
        else:
            raise AssertionError("unknown defect")
