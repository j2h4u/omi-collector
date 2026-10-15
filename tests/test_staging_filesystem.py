"""Focused filesystem ownership tests for the remaining staging boundary."""

from __future__ import annotations

from dataclasses import FrozenInstanceError
from pathlib import Path

import pytest

from omi_collector.capture.adapters.collector_state_db import StateDB
from omi_collector.capture.adapters.staging_contract import (
    DeviceAlreadyRunningError,
    StagingError,
)
from staging_test_factory import StagingStore


def test_resolved_staging_paths_are_immutable(tmp_path: Path) -> None:
    paths = StagingStore(tmp_path / "spool", tmp_path / "captures").paths
    field = "root"

    with pytest.raises(FrozenInstanceError):
        setattr(paths, field, tmp_path / "replacement")

    assert paths.root == tmp_path / "spool"


def test_device_lock_is_in_process_and_reports_lease_lifecycle(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from omi_collector.capture.adapters import staging_filesystem

    events: list[tuple[str, dict[str, object]]] = []
    monkeypatch.setattr(staging_filesystem, "debug_event", lambda name, **fields: events.append((name, fields)))
    store = StagingStore(tmp_path / "spool", tmp_path / "captures")

    with store.device_lock(operation="capture_batch"):
        with pytest.raises(DeviceAlreadyRunningError), store.device_lock(operation="resume_pending_attempt"):
            pass
        assert not store.paths.lock.exists()

    with store.device_lock(operation="resume_pending_attempt"):
        pass
    acquired = [fields for name, fields in events if name == "device_lock_acquired"]
    released = [fields for name, fields in events if name == "device_lock_released"]
    assert [event["operation"] for event in acquired] == ["capture_batch", "resume_pending_attempt"]
    assert [event["operation"] for event in released] == ["capture_batch", "resume_pending_attempt"]
    assert all(event["scope"] == event["metadata_status"] == "in_process" for event in acquired + released)
    assert isinstance(released[0]["duration_seconds"], float)


@pytest.mark.parametrize("layout", ["alias", "nested"])
def test_preflight_rejects_aliased_or_nested_roots(tmp_path: Path, layout: str) -> None:
    spool = tmp_path / "spool"
    spool.mkdir()
    if layout == "alias":
        capture_root = tmp_path / "capture-alias"
        capture_root.symlink_to(spool, target_is_directory=True)
    else:
        capture_root = spool / "captures"
        capture_root.mkdir()

    with pytest.raises(StagingError, match=r"real directory|distinct, non-nested"):
        StagingStore(spool, capture_root).preflight_storage()


@pytest.mark.parametrize("root_name", ["spool", "capture"])
def test_storage_preflight_rejects_symlink_roots(tmp_path: Path, root_name: str) -> None:
    target = tmp_path / f"{root_name}-target"
    target.mkdir()
    spool = tmp_path / "spool"
    capture_root = tmp_path / "capture"
    (spool if root_name == "spool" else capture_root).symlink_to(target, target_is_directory=True)

    with pytest.raises(StagingError, match="real directory"):
        StagingStore(
            spool,
            capture_root,
            state_db=StateDB(tmp_path / "state.sqlite3"),
        ).preflight_storage()


def test_storage_preflight_rejects_existing_non_directory_root(tmp_path: Path) -> None:
    root = tmp_path / "root"
    root.write_text("root is unexpectedly a file", encoding="utf-8")
    store = StagingStore(root, tmp_path / "captures", state_db=StateDB(tmp_path / "state.sqlite3"))

    with pytest.raises(StagingError, match="real directory"):
        store.preflight_storage()


def test_storage_preflight_leaves_missing_private_roots_absent(tmp_path: Path) -> None:
    spool = tmp_path / "collector"
    capture_root = tmp_path / "pipeline" / "raw"

    StagingStore(spool, capture_root).preflight_storage()

    assert spool.is_dir()
    assert not (spool / "attempts").exists()
    assert not (spool / "quarantine").exists()
    assert not capture_root.exists()
    assert not tuple(spool.glob(".storage-preflight-*.tmp"))


@pytest.mark.parametrize("root_name", ["attempts", "quarantine", "draft"])
def test_storage_preflight_rejects_legacy_private_root_symlink(tmp_path: Path, root_name: str) -> None:
    spool = tmp_path / "collector"
    capture_root = tmp_path / "draft"
    spool.mkdir()
    target = tmp_path / "target"
    target.mkdir()
    root = capture_root if root_name == "draft" else spool / root_name
    root.symlink_to(target, target_is_directory=True)

    with pytest.raises(StagingError, match=r"must (?:be a real directory|not be a symlink)"):
        StagingStore(spool, capture_root).preflight_storage()


def test_storage_preflight_preserves_existing_legacy_private_files(tmp_path: Path) -> None:
    spool = tmp_path / "collector"
    capture_root = tmp_path / "draft"
    spool.mkdir()
    attempts = spool / "attempts"
    attempts.mkdir()
    source = attempts / "pending-source"
    source.write_bytes(b"legacy attempt")

    StagingStore(spool, capture_root).preflight_storage()

    assert source.read_bytes() == b"legacy attempt"
    assert not (spool / "quarantine").exists()
    assert not capture_root.exists()


def test_storage_preflight_rejects_unwritable_directory_and_preserves_cause(tmp_path: Path) -> None:
    from omi_collector.capture.adapters.staging_filesystem import StagingIO

    spool = tmp_path / "spool"
    capture_root = tmp_path / "capture"
    spool.mkdir()
    (spool / "attempts").mkdir()
    (spool / "quarantine").mkdir()
    capture_root.mkdir()

    def fail_fsync(_: int) -> None:
        raise PermissionError("simulated durable-write denial")

    store = StagingStore(spool, capture_root, io=StagingIO(fsync_fn=fail_fsync))

    with pytest.raises(StagingError, match="writable and durable") as raised:
        store.preflight_storage()

    assert isinstance(raised.value.__cause__, PermissionError)
    assert "simulated durable-write denial" in str(raised.value.__cause__)
