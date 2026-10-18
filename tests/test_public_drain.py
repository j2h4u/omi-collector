from __future__ import annotations

import json
import os
import stat
import subprocess
import sys
from pathlib import Path
from typing import cast

import pytest

from omi_collector.capture.adapters.collector_state_db import StateDB, StateKind
from omi_collector.capture.adapters.public_drain import AcceptedDrainCustody, PublicDrainProjection
from omi_collector.capture.domain.ring_protocol import RingInfo

_NOW = "2026-10-17T12:00:00+00:00"


def test_projection_starts_unknown_and_private_pending_token_is_never_exported(tmp_path: Path) -> None:
    projection = PublicDrainProjection(StateDB(tmp_path / "state.sqlite3"), tmp_path / "public" / "pendant-drain.json")
    projection.path.parent.mkdir(mode=0o2750)
    projection.path.parent.chmod(0o2750)

    projection.publish_snapshot()
    assert json.loads(projection.path.read_text(encoding="utf-8")) == {
        "version": 2,
        "ring": None,
        "last_drain": None,
        "battery": None,
        "bluetooth": None,
    }
    projection.update_observations(ring=(3, _NOW), battery=(81, _NOW))
    projection.arm_pending("source-a", 10)
    initial_private = projection._state_db.get(StateKind.PENDANT_DRAIN, "current")
    assert initial_private is not None
    assert initial_private["version"] == 1
    initial_event_id = initial_private["pending_event_id"]
    projection.arm_pending("source-a", 10)
    assert projection._state_db.get(StateKind.PENDANT_DRAIN, "current")["pending_event_id"] == initial_event_id  # type: ignore[index]
    projection.publish_snapshot()

    exported = cast(dict[str, object], json.loads(projection.path.read_text(encoding="utf-8")))
    private = projection._state_db.get(StateKind.PENDANT_DRAIN, "current")
    assert exported == {
        "version": 2,
        "ring": {"unread_packets": 3, "observed_at": _NOW},
        "last_drain": None,
        "battery": {"percent": 81, "observed_at": _NOW},
        "bluetooth": None,
    }
    assert private is not None and isinstance(private["pending_event_id"], str)
    assert "pending_event_id" not in exported
    assert projection.path.stat().st_mode & 0o777 == 0o640


def test_projection_exports_only_bounded_bluetooth_incident_summary(tmp_path: Path) -> None:
    db = StateDB(tmp_path / "state.sqlite3")
    projection = PublicDrainProjection(db, tmp_path / "public" / "pendant-drain.json")
    projection.path.parent.mkdir(mode=0o2750)
    projection.path.parent.chmod(0o2750)
    db.put(
        StateKind.BLE_SCAN_INCIDENT,
        "open",
        {
            "incident_id": "incident-1",
            "status": "open",
            "detected_at": _NOW,
            "context": {"reason": "scan_silence", "private_context": "omit me"},
            "diagnostics": {"controller": "omit me"},
        },
    )
    projection.publish_snapshot()
    opened = cast(dict[str, object], json.loads(projection.path.read_text(encoding="utf-8")))
    assert opened["version"] == 2
    assert opened["bluetooth"] == {
        "incident_id": "incident-1",
        "status": "open",
        "reason": "scan_silence",
        "detected_at": _NOW,
        "resolved_at": None,
    }
    assert "private_context" not in projection.path.read_text(encoding="utf-8")
    assert "diagnostics" not in projection.path.read_text(encoding="utf-8")

    db.delete(StateKind.BLE_SCAN_INCIDENT, "open")
    db.put(
        StateKind.BLE_SCAN_INCIDENT,
        "history",
        {
            "entries": [
                {
                    "incident_id": "incident-1",
                    "detected_at": _NOW,
                    "resolved_at": "2026-10-17T12:05:00+00:00",
                    "context": {"reason": "scan_silence"},
                    "diagnostics": {"private": "omit me"},
                }
            ]
        },
    )
    projection.publish_snapshot()
    resolved = cast(dict[str, object], json.loads(projection.path.read_text(encoding="utf-8")))
    assert resolved["bluetooth"] == {
        "incident_id": "incident-1",
        "status": "resolved",
        "reason": "scan_silence",
        "detected_at": _NOW,
        "resolved_at": "2026-10-17T12:05:00+00:00",
    }


def test_completion_requires_zero_info_and_frontier_and_preserves_identity_without_token(tmp_path: Path) -> None:
    db = StateDB(tmp_path / "state.sqlite3")
    projection = PublicDrainProjection(db, tmp_path / "public" / "pendant-drain.json")
    with db.transaction() as transaction:
        projection.complete_in_transaction(
            transaction,
            verified_drain=(RingInfo(10, 10, 100, 0, 444), _NOW),
            completed_at=_NOW,
            closure_frontier=10,
            accepted_custody=AcceptedDrainCustody("source-a", 10),
        )
    initial = db.get(StateKind.PENDANT_DRAIN, "current")
    assert initial is not None and initial["last_drain"] is None

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
            accepted_custody=AcceptedDrainCustody("source-a", 10),
        )
    with db.transaction() as transaction:
        projection.complete_in_transaction(
            transaction,
            verified_drain=(RingInfo(10, 10, 100, 0, 444), _NOW),
            completed_at=_NOW,
            closure_frontier=10,
            accepted_custody=AcceptedDrainCustody("source-a", 10),
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
            accepted_custody=AcceptedDrainCustody("source-a", 10),
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
            accepted_custody=AcceptedDrainCustody("source-a", 10),
        )
    retried = db.get(StateKind.PENDANT_DRAIN, "current")
    assert retried is not None and retried["last_drain"] == {"event_id": pending_id, "completed_at": _NOW}


@pytest.mark.parametrize("confirmed", [None, 19, 21])
def test_empty_info_ahead_requires_exact_confirmed_loss_endpoint(tmp_path: Path, confirmed: int | None) -> None:
    db = StateDB(tmp_path / "state.sqlite3")
    projection = PublicDrainProjection(db, tmp_path / "public" / "pendant-drain.json")
    projection.arm_pending("source-a", 10)

    with db.transaction() as transaction:
        projection.complete_in_transaction(
            transaction,
            verified_drain=(RingInfo(20, 20, 100, 0, 444), _NOW),
            completed_at=_NOW,
            closure_frontier=10,
            accepted_custody=AcceptedDrainCustody("source-a", 10, confirmed),
        )

    state = db.get(StateKind.PENDANT_DRAIN, "current")
    assert state is not None and state["last_drain"] is None


def test_empty_info_at_exact_confirmed_loss_endpoint_completes(tmp_path: Path) -> None:
    db = StateDB(tmp_path / "state.sqlite3")
    projection = PublicDrainProjection(db, tmp_path / "public" / "pendant-drain.json")
    projection.arm_pending("source-a", 10)

    with db.transaction() as transaction:
        projection.complete_in_transaction(
            transaction,
            verified_drain=(RingInfo(20, 20, 100, 0, 444), _NOW),
            completed_at=_NOW,
            closure_frontier=10,
            accepted_custody=AcceptedDrainCustody("source-a", 10, 20),
        )

    state = db.get(StateKind.PENDANT_DRAIN, "current")
    assert state is not None and isinstance(state["last_drain"], dict)
    assert state["last_drain"]["completed_at"] == _NOW


@pytest.mark.parametrize("endpoint", [True, -1])
def test_confirmed_loss_endpoint_must_be_nonnegative_exact_int(tmp_path: Path, endpoint: int) -> None:
    db = StateDB(tmp_path / "state.sqlite3")
    projection = PublicDrainProjection(db, tmp_path / "public" / "pendant-drain.json")
    with db.transaction() as transaction, pytest.raises(ValueError, match="accepted pendant custody binding"):
        projection.complete_in_transaction(
            transaction,
            verified_drain=(RingInfo(10, 10, 100, 0, 444), _NOW),
            completed_at=_NOW,
            closure_frontier=10,
            accepted_custody=AcceptedDrainCustody("source-a", 10, endpoint),  # type: ignore[arg-type]
        )


def test_publication_is_idempotent_and_repairs_target(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    projection = PublicDrainProjection(StateDB(tmp_path / "state.sqlite3"), tmp_path / "public" / "drain.json")
    projection.path.parent.mkdir(mode=0o2750)
    projection.path.parent.chmod(0o2750)
    real_fsync = os.fsync
    calls = 0

    def count_fsync(fd: int) -> None:
        nonlocal calls
        calls += 1
        real_fsync(fd)

    monkeypatch.setattr(os, "fsync", count_fsync)
    projection.publish_snapshot()
    assert calls == 2
    projection.publish_snapshot()
    assert calls == 2
    projection.update_observations(ring=(4, _NOW))
    projection.publish_snapshot()
    assert calls == 4

    projection.path.write_text("{}\n", encoding="utf-8")
    projection.publish_snapshot()
    assert calls == 6
    projection.path.chmod(0o600)
    projection.publish_snapshot()
    assert calls == 8
    projection.path.unlink()
    projection.publish_snapshot()
    assert calls == 10
    projection.path.unlink()
    os.mkfifo(projection.path)
    projection.publish_snapshot()
    assert calls == 12
    assert stat.S_ISREG(projection.path.stat().st_mode)


def test_directory_fsync_failure_forces_identical_retry_to_sync(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    projection = PublicDrainProjection(StateDB(tmp_path / "state.sqlite3"), tmp_path / "public" / "drain.json")
    projection.path.parent.mkdir(mode=0o2750)
    projection.path.parent.chmod(0o2750)
    real_fsync = os.fsync
    fail_directory = True
    calls = 0

    def fail_once(fd: int) -> None:
        nonlocal fail_directory, calls
        calls += 1
        if fail_directory and stat.S_ISDIR(os.fstat(fd).st_mode):
            fail_directory = False
            raise OSError("directory fsync failed")
        real_fsync(fd)

    monkeypatch.setattr(os, "fsync", fail_once)
    with pytest.raises(OSError, match="directory fsync"):
        projection.publish_snapshot()
    assert calls == 2
    projection.publish_snapshot()
    assert calls == 4


def test_publication_cleans_only_owned_safe_stale_files(tmp_path: Path) -> None:
    projection = PublicDrainProjection(StateDB(tmp_path / "state.sqlite3"), tmp_path / "public" / "drain.json")
    projection.path.parent.mkdir(mode=0o2750)
    projection.path.parent.chmod(0o2750)
    prefix = f".{projection.path.name}."
    stale = projection.path.parent / f"{prefix}abcdefgh"
    stale.write_bytes(b"orphan")
    stale.chmod(0o600)
    wrong_mode = projection.path.parent / f"{prefix}abcdefgi"
    wrong_mode.write_bytes(b"keep")
    wrong_mode.chmod(0o644)
    symlink = projection.path.parent / f"{prefix}abcdefgj"
    symlink.symlink_to(stale)
    outside = tmp_path / "outside"
    outside.write_bytes(b"keep")
    hardlink = projection.path.parent / f"{prefix}abcdefgk"
    os.link(outside, hardlink)
    lookalike = projection.path.parent / f"{prefix}backup.abcdefgh"
    lookalike.write_bytes(b"keep")
    lookalike.chmod(0o600)
    wrong_owner = projection.path.parent / f"{prefix}abcdefgl"
    wrong_owner.write_bytes(b"keep")
    wrong_owner.chmod(0o600)
    if os.geteuid() == 0:
        os.chown(wrong_owner, 65534, projection.path.parent.stat().st_gid)

    projection.publish_snapshot()
    assert not stale.exists()
    assert wrong_mode.exists()
    assert symlink.is_symlink()
    assert hardlink.exists()
    assert lookalike.exists()
    if os.geteuid() == 0:
        assert wrong_owner.exists()


@pytest.mark.parametrize("stop_at,mode", [("mkstemp", 0o600), ("file_fsync", 0o640)])
def test_actual_interrupted_publication_temp_is_removed_after_restart(tmp_path: Path, stop_at: str, mode: int) -> None:
    projection = PublicDrainProjection(StateDB(tmp_path / "state.sqlite3"), tmp_path / "public" / "drain.json")
    projection.path.parent.mkdir(mode=0o2750)
    projection.path.parent.chmod(0o2750)
    child_script = """
import os
import stat
import sys
import tempfile
from pathlib import Path
from omi_collector.capture.adapters.collector_state_db import StateDB
from omi_collector.capture.adapters.public_drain import PublicDrainProjection

path = Path(sys.argv[1])
real_mkstemp = tempfile.mkstemp
real_fsync = os.fsync

if sys.argv[3] == "mkstemp":
    def stop_after_mkstemp(*args, **kwargs):
        result = real_mkstemp(*args, **kwargs)
        os._exit(9)
    tempfile.mkstemp = stop_after_mkstemp
else:
    def stop_after_file_fsync(fd):
        real_fsync(fd)
        if stat.S_ISREG(os.fstat(fd).st_mode):
            os._exit(9)
    os.fsync = stop_after_file_fsync

PublicDrainProjection(StateDB(Path(sys.argv[2])), path).publish_snapshot()
"""
    child = subprocess.run(
        [
            sys.executable,
            "-c",
            child_script,
            str(projection.path),
            str(tmp_path / "state.sqlite3"),
            stop_at,
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    assert child.returncode == 9
    temporary_files = list(projection.path.parent.glob(f".{projection.path.name}.*"))
    assert len(temporary_files) == 1
    temporary = temporary_files[0]
    assert stat.S_IMODE(temporary.stat().st_mode) == mode

    PublicDrainProjection(projection._state_db, projection.path).publish_snapshot()
    assert not temporary.exists()


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
