from __future__ import annotations

import json
from pathlib import Path
from typing import cast

import pytest

from omi_collector.capture.adapters.collector_state_db import StateDB, StateKind
from omi_collector.capture.adapters.public_drain import PublicDrainProjection
from omi_collector.capture.domain.ring_protocol import RingInfo

_NOW = "2026-10-17T12:00:00+00:00"


def test_projection_starts_unknown_and_private_pending_token_is_never_exported(tmp_path: Path) -> None:
    projection = PublicDrainProjection(StateDB(tmp_path / "state.sqlite3"), tmp_path / "public" / "pendant-drain.json")
    projection.path.parent.mkdir(mode=0o2750)
    projection.path.parent.chmod(0o2750)

    projection.publish_snapshot()
    assert json.loads(projection.path.read_text(encoding="utf-8")) == {
        "version": 1,
        "ring": None,
        "last_drain": None,
        "battery": None,
    }
    projection.update_observations(ring=(3, _NOW), battery=(81, _NOW))
    projection.arm_pending("source-a", 10)
    initial_private = projection._state_db.get(StateKind.PENDANT_DRAIN, "current")
    assert initial_private is not None
    initial_event_id = initial_private["pending_event_id"]
    projection.arm_pending("source-a", 10)
    assert projection._state_db.get(StateKind.PENDANT_DRAIN, "current")["pending_event_id"] == initial_event_id  # type: ignore[index]
    projection.publish_snapshot()

    exported = cast(dict[str, object], json.loads(projection.path.read_text(encoding="utf-8")))
    private = projection._state_db.get(StateKind.PENDANT_DRAIN, "current")
    assert exported == {
        "version": 1,
        "ring": {"unread_packets": 3, "observed_at": _NOW},
        "last_drain": None,
        "battery": {"percent": 81, "observed_at": _NOW},
    }
    assert private is not None and isinstance(private["pending_event_id"], str)
    assert "pending_event_id" not in exported
    assert projection.path.stat().st_mode & 0o777 == 0o640


def test_completion_requires_zero_info_and_frontier_and_preserves_identity_without_token(tmp_path: Path) -> None:
    db = StateDB(tmp_path / "state.sqlite3")
    projection = PublicDrainProjection(db, tmp_path / "public" / "pendant-drain.json")
    with db.transaction() as transaction:
        projection.complete_in_transaction(
            transaction,
            verified_drain=(RingInfo(10, 10, 100, 0, 444), _NOW),
            completed_at=_NOW,
            closure_frontier=10,
            accepted_custody=("source-a", 10),
        )
    assert db.get(StateKind.PENDANT_DRAIN, "current")["last_drain"] is None  # type: ignore[index]

    projection.arm_pending("source-a", 10)
    pending = db.get(StateKind.PENDANT_DRAIN, "current")
    assert pending is not None
    event_id = pending["pending_event_id"]
    with pytest.raises(ValueError, match="zero unread"), db.transaction() as transaction:
        projection.complete_in_transaction(
            transaction,
            verified_drain=(RingInfo(10, 11, 100, 0, 444), _NOW),
            completed_at=_NOW,
            closure_frontier=10,
            accepted_custody=("source-a", 10),
        )
    with db.transaction() as transaction:
        projection.complete_in_transaction(
            transaction,
            verified_drain=(RingInfo(10, 10, 100, 0, 444), _NOW),
            completed_at=_NOW,
            closure_frontier=10,
            accepted_custody=("source-a", 10),
        )
    completed = db.get(StateKind.PENDANT_DRAIN, "current")
    assert completed is not None
    assert completed["last_drain"] == {"event_id": event_id, "completed_at": _NOW}
    projection.arm_pending("source-b", 5)
    assert db.get(StateKind.PENDANT_DRAIN, "current")["pending_event_id"] != event_id  # type: ignore[index]


def test_invalid_observation_does_not_overwrite_valid_battery(tmp_path: Path) -> None:
    db = StateDB(tmp_path / "state.sqlite3")
    projection = PublicDrainProjection(db, tmp_path / "public" / "pendant-drain.json")
    projection.update_observations(battery=(77, _NOW))

    for battery in ((True, _NOW), (101, _NOW), (77, "2026-10-17T12:00:00")):
        with pytest.raises(ValueError):
            projection.update_observations(battery=battery)  # type: ignore[arg-type]

    assert db.get(StateKind.PENDANT_DRAIN, "current")["battery"] == {"percent": 77, "observed_at": _NOW}  # type: ignore[index]


def test_empty_info_ahead_of_accepted_frontier_does_not_complete_event(tmp_path: Path) -> None:
    db = StateDB(tmp_path / "state.sqlite3")
    projection = PublicDrainProjection(db, tmp_path / "public" / "pendant-drain.json")
    projection.arm_pending("source-a", 10)

    with db.transaction() as transaction:
        projection.complete_in_transaction(
            transaction,
            verified_drain=(RingInfo(20, 20, 100, 0, 444), _NOW),
            completed_at=_NOW,
            closure_frontier=10,
            accepted_custody=("source-a", 10),
        )

    state = db.get(StateKind.PENDANT_DRAIN, "current")
    assert state is not None
    assert state["last_drain"] is None
    pending_id = state["pending_event_id"]
    assert isinstance(pending_id, str)
    assert state["ring"] == {"unread_packets": 0, "observed_at": _NOW}
    with db.transaction() as transaction:
        projection.complete_in_transaction(
            transaction,
            verified_drain=(RingInfo(10, 10, 100, 0, 444), _NOW),
            completed_at=_NOW,
            closure_frontier=10,
            accepted_custody=("source-a", 10),
        )
    retried = db.get(StateKind.PENDANT_DRAIN, "current")
    assert retried is not None and retried["last_drain"] == {"event_id": pending_id, "completed_at": _NOW}


def test_empty_info_behind_closure_refreshes_ring_without_failing_closure(tmp_path: Path) -> None:
    db = StateDB(tmp_path / "state.sqlite3")
    projection = PublicDrainProjection(db, tmp_path / "public" / "pendant-drain.json")

    with db.transaction() as transaction:
        projection.complete_in_transaction(
            transaction,
            verified_drain=(RingInfo(9, 9, 100, 0, 444), _NOW),
            completed_at=_NOW,
            closure_frontier=10,
            accepted_custody=None,
        )

    state = db.get(StateKind.PENDANT_DRAIN, "current")
    assert state is not None and state["ring"] == {"unread_packets": 0, "observed_at": _NOW}


def test_publication_replace_failure_keeps_old_file_and_removes_temporary_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    projection = PublicDrainProjection(StateDB(tmp_path / "state.sqlite3"), tmp_path / "public" / "pendant-drain.json")
    projection.path.parent.mkdir(mode=0o2750)
    projection.path.parent.chmod(0o2750)
    projection.publish_snapshot()
    old = projection.path.read_bytes()
    projection.update_observations(ring=(4, _NOW))

    def fail_replace(_path: Path, _destination: str | Path) -> Path:
        raise OSError("replace failed")

    monkeypatch.setattr(Path, "replace", fail_replace)
    with pytest.raises(OSError, match="replace failed"):
        projection.publish_snapshot()

    assert projection.path.read_bytes() == old
    assert list(projection.path.parent.iterdir()) == [projection.path]
