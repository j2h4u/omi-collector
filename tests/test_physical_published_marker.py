from __future__ import annotations

from copy import deepcopy
from hashlib import sha256
from pathlib import Path

import pytest

from omi_collector.capture.adapters import physical_stage, ready_bundles
from omi_collector.capture.adapters.collector_state_db import StateDB, StateKind
from test_physical_publication import _descriptor
from test_physical_ready_custody import _physical_entry


def _published(db: StateDB) -> tuple[dict[str, object], dict[str, object], dict[str, object]]:
    descriptor = _descriptor()
    entry = _physical_entry()
    entry.update(
        {
            key: descriptor[key]
            for key in ("source_id", "source_sha256", "source_bytes", "spans", "seams", "record_count")
        }
    )
    marker = {
        "state": "published",
        "artifact_id": "d" * 64,
        "source_id": descriptor["source_id"],
        "anchor": descriptor["anchor"],
        "record_projection_sha256": descriptor["record_projection_sha256"],
        "descriptor_sha256": sha256(ready_bundles._canonical(descriptor)).hexdigest(),
    }
    db.put(StateKind.PHYSICAL_IMPORT, "stage", marker)
    db.put(StateKind.READY_PUBLICATION, "d" * 64, entry)
    return descriptor, marker, entry


def test_published_physical_result_matches_before_and_after_ack(tmp_path: Path) -> None:
    db = StateDB(tmp_path / "state.sqlite3")
    descriptor, marker, entry = _published(db)
    db.put(StateKind.PHYSICAL_IMPORT, "other", {"state": "staging", "source_id": descriptor["source_id"]})
    db.put(StateKind.PHYSICAL_IMPORT, "unrelated", {"state": "published", "source_id": "f" * 32})
    assert physical_stage.published_physical_artifact(db, descriptor) == marker["artifact_id"]
    db.put(StateKind.READY_PUBLICATION, "d" * 64, {**entry, "state": "retired"})
    assert physical_stage.published_physical_artifact(db, descriptor) == marker["artifact_id"]
    other = {**descriptor, "source_id": "e" * 32}
    assert physical_stage.published_physical_artifact(db, other) is None


def test_published_physical_source_refuses_different_descriptor(tmp_path: Path) -> None:
    db = StateDB(tmp_path / "state.sqlite3")
    descriptor, _, _ = _published(db)
    different = deepcopy(descriptor)
    different["source_sha256"] = "9" * 64
    with pytest.raises(ready_bundles.ReadyBundleError, match="different publication"):
        physical_stage.published_physical_artifact(db, different)


@pytest.mark.parametrize(
    "damage", ["artifact_id", "missing_entry", "invalid_entry", "anchor", "projection", "lineage", "duplicate"]
)
def test_published_physical_marker_fails_closed_for_conflicting_evidence(tmp_path: Path, damage: str) -> None:
    db = StateDB(tmp_path / "state.sqlite3")
    descriptor, marker, entry = _published(db)
    if damage == "artifact_id":
        marker["artifact_id"] = None
    elif damage == "missing_entry":
        db.delete(StateKind.READY_PUBLICATION, "d" * 64)
    elif damage == "invalid_entry":
        db.put(StateKind.READY_PUBLICATION, "d" * 64, {**entry, "packet_count": 0})
    elif damage == "anchor":
        marker["anchor"] = {}
    elif damage == "projection":
        marker["record_projection_sha256"] = "0" * 64
    elif damage == "lineage":
        db.put(StateKind.READY_PUBLICATION, "d" * 64, {**entry, "source_sha256": "9" * 64})
    else:
        db.put(StateKind.PHYSICAL_IMPORT, "duplicate", marker)
    db.put(StateKind.PHYSICAL_IMPORT, "stage", marker)
    expected = ready_bundles.ReadyBundleError if damage == "lineage" else ready_bundles.ReadyDurabilityFatalError
    with pytest.raises(expected):
        physical_stage.published_physical_artifact(db, descriptor)
