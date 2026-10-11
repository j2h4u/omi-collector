"""Focused filesystem staging ownership tests."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import FrozenInstanceError
from errno import EXDEV
from multiprocessing import get_context
from multiprocessing.queues import Queue
from os import PathLike, fsync, mkfifo
from pathlib import Path
from shutil import rmtree
from typing import cast

import pytest

from omi_collector.capture.adapters import staging_filesystem, staging_store
from omi_collector.capture.adapters.attempts import StagedAttempt
from omi_collector.capture.adapters.collector_state_db import StateDB, StateKind
from omi_collector.capture.adapters.staging_contract import (
    AttemptStateError,
    CollisionError,
    DeviceAlreadyRunningError,
    StagingError,
)
from omi_collector.capture.domain.ring_protocol import RECORD_SIZE, DoneNotification, ReadBeginNotification
from staging_test_factory import StagingStore, staged_attempt_payload

_CAPTURE_ROOTS: set[Path] = set()


def _capture_root(tmp_path: Path) -> Path:
    root = tmp_path.parent / f"{tmp_path.name}-captures"
    if tmp_path not in _CAPTURE_ROOTS:
        rmtree(root, ignore_errors=True)
        _CAPTURE_ROOTS.add(tmp_path)
    return root


def test_resolved_staging_paths_are_immutable(tmp_path: Path) -> None:
    paths = StagingStore(tmp_path, _capture_root(tmp_path)).paths
    field = "root"

    with pytest.raises(FrozenInstanceError):
        setattr(paths, field, tmp_path / "replacement")

    assert paths.root == tmp_path


@pytest.fixture(autouse=True)
def _isolate_capture_root(tmp_path: Path) -> None:
    rmtree(_capture_root(tmp_path), ignore_errors=True)


def _record(marker: int) -> bytes:
    return marker.to_bytes(4, "big") + bytes((marker,)) * (RECORD_SIZE - 4)


def _started_attempt(tmp_path: Path, *, count: int = 2):
    attempt = StagingStore(tmp_path, _capture_root(tmp_path)).prepare_streaming_attempt(100, count)
    attempt.record_read_begin(ReadBeginNotification(100, count))
    return attempt


def _started_streaming_attempt(tmp_path: Path, *, count: int = 2, fsync_fn: Callable[[int], None] = fsync):
    attempt = StagingStore(tmp_path, _capture_root(tmp_path), fsync_fn=fsync_fn).prepare_streaming_attempt(100, count)
    attempt.record_read_begin(ReadBeginNotification(100, count))
    return attempt


def _rewrite_checkpoint(attempt: StagedAttempt, field: str, value: object) -> None:
    row = staged_attempt_payload(attempt)
    assert row is not None
    checkpoint = cast(dict[str, object], row["checkpoint"])
    checkpoint[field] = value
    attempt._filesystem.state_db.put(StateKind.ATTEMPT, attempt.attempt_id, row)


def _replace_with_symlink(path: Path, target: Path, payload: bytes | str) -> None:
    path.unlink()
    path.symlink_to(target)
    if isinstance(payload, bytes):
        target.write_bytes(payload)
    else:
        target.write_text(payload, encoding="utf-8")


def _guard_rename_to_capture(monkeypatch: pytest.MonkeyPatch) -> None:
    real_rename = staging_filesystem.os.rename

    def guarded_rename(
        source: str | bytes | PathLike[str] | PathLike[bytes],
        destination: str | bytes | PathLike[str] | PathLike[bytes],
        *,
        src_dir_fd: int | None = None,
        dst_dir_fd: int | None = None,
    ) -> None:
        if src_dir_fd is None or dst_dir_fd is None or src_dir_fd != dst_dir_fd:
            raise OSError(EXDEV, "simulated cross-mount rename")
        real_rename(source, destination, src_dir_fd=src_dir_fd, dst_dir_fd=dst_dir_fd)

    monkeypatch.setattr(staging_filesystem.os, "rename", guarded_rename)


def _open_attempt_in_child(
    spool: str,
    capture_root: str,
    attempt_id: str,
    expected_module_path: str,
    results: Queue[tuple[str, str]],
) -> None:
    from staging_test_factory import StagingStore as ChildStagingStore

    module_path = str(Path(staging_store.__file__).resolve())
    if module_path != expected_module_path:
        results.put(("wrong-source", module_path))
        return
    try:
        ChildStagingStore(Path(spool), Path(capture_root)).open_attempt(attempt_id)
    except AttemptStateError:
        results.put(("AttemptStateError", module_path))
    else:
        results.put(("accepted", module_path))


def test_device_lock_is_in_process_and_reports_lease_lifecycle(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    events: list[tuple[str, dict[str, object]]] = []
    monkeypatch.setattr(staging_filesystem, "debug_event", lambda name, **fields: events.append((name, fields)))
    store = StagingStore(tmp_path / "spool", _capture_root(tmp_path))

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


def test_open_rejects_missing_checkpoint_state_without_changing_raw_evidence(tmp_path: Path) -> None:
    attempt = _started_streaming_attempt(tmp_path, count=1)
    attempt.accept_chunk(100, _record(1))
    raw_before = (attempt.path / "records.bin").read_bytes()
    row = staged_attempt_payload(attempt)
    assert row is not None
    row["checkpoint"] = None
    attempt._filesystem.state_db.put(StateKind.ATTEMPT, attempt.attempt_id, row)
    attempt.close()

    with pytest.raises(AttemptStateError, match="streaming checkpoint is malformed"):
        StagingStore(tmp_path, _capture_root(tmp_path)).open_attempt(attempt.attempt_id)

    assert (attempt.path / "records.bin").read_bytes() == raw_before
    assert staged_attempt_payload(attempt) == row


def test_open_rejects_checkpoint_with_nonhex_hash_without_rewriting_raw(tmp_path: Path) -> None:
    attempt = _started_streaming_attempt(tmp_path, count=1)
    attempt.accept_chunk(100, _record(1))
    attempt.close()
    _rewrite_checkpoint(attempt, "raw_sha256", "z" * 64)
    raw_before = (attempt.path / "records.bin").read_bytes()
    state_before = staged_attempt_payload(attempt)

    with pytest.raises(AttemptStateError, match="checkpoint is malformed"):
        StagingStore(tmp_path, _capture_root(tmp_path)).open_attempt(attempt.attempt_id)

    assert (attempt.path / "records.bin").read_bytes() == raw_before
    assert staged_attempt_payload(attempt) == state_before


def test_open_attempt_rejects_raw_fifo_without_blocking(tmp_path: Path) -> None:
    attempt = _started_streaming_attempt(tmp_path, count=1)
    attempt.close()
    fifo_path = attempt.path / "records.bin"
    fifo_path.unlink()
    mkfifo(fifo_path)
    module_path = Path(staging_store.__file__).resolve()
    context = get_context("fork")
    results = context.Queue()
    child = context.Process(
        target=_open_attempt_in_child,
        args=(str(tmp_path), str(_capture_root(tmp_path)), attempt.attempt_id, str(module_path), results),
    )
    child.start()
    child.join(3)
    timed_out = child.is_alive()
    try:
        if timed_out:
            child.terminate()
            child.join(3)
        if child.is_alive():
            child.kill()
            child.join()
        assert not timed_out, "open_attempt blocked on a FIFO"
        assert child.exitcode == 0
        assert results.get(timeout=1) == ("AttemptStateError", str(module_path))
    finally:
        if child.is_alive():
            child.kill()
            child.join()
        child.close()
        results.close()
        results.join_thread()


def test_prefix_collision_preserves_longer_destination_and_source(tmp_path: Path) -> None:
    attempt = _started_streaming_attempt(tmp_path, count=1)
    attempt.accept_chunk(100, _record(1))
    attempt.checkpoint()
    result = attempt.publish_prefix()
    assert result is not None
    destination = result.bundle_path
    raw_destination = destination / "records.bin"
    raw_destination.write_bytes(raw_destination.read_bytes() + b"x")
    destination_before = {path.name: path.read_bytes() for path in destination.iterdir() if path.is_file()}
    source_before = {path.name: path.read_bytes() for path in attempt.path.iterdir() if path.is_file()}

    try:
        with pytest.raises(CollisionError, match="prefix collision"):
            attempt.publish_prefix()

        assert {path.name: path.read_bytes() for path in destination.iterdir() if path.is_file()} == destination_before
        assert {path.name: path.read_bytes() for path in attempt.path.iterdir() if path.is_file()} == source_before
    finally:
        attempt.close()


def test_public_checkpoint_rejects_corrupt_state_after_append_without_rewriting_evidence(
    tmp_path: Path,
) -> None:
    attempt = _started_streaming_attempt(tmp_path, count=1)
    attempt.accept_chunk(100, _record(1))
    _rewrite_checkpoint(attempt, "raw_sha256", "z" * 64)
    state_before = staged_attempt_payload(attempt)
    raw_before = (attempt.path / "records.bin").read_bytes()

    try:
        with pytest.raises(AttemptStateError, match="attempt state is malformed"):
            attempt.checkpoint()

        assert staged_attempt_payload(attempt) == state_before
        assert (attempt.path / "records.bin").read_bytes() == raw_before
    finally:
        attempt.close()


class _RecordingStream:
    def __init__(self, wrapped: object) -> None:
        self.wrapped = wrapped
        self.writes: list[bytes] = []

    def write(self, payload: bytes) -> int:
        self.writes.append(payload)
        return cast(int, self.wrapped.write(payload))  # type: ignore[attr-defined]

    def flush(self) -> None:
        self.wrapped.flush()  # type: ignore[attr-defined]

    def fileno(self) -> int:
        return cast(int, self.wrapped.fileno())  # type: ignore[attr-defined]

    def close(self) -> None:
        self.wrapped.close()  # type: ignore[attr-defined]


@pytest.mark.parametrize("layout", ["alias", "nested"])
def test_split_roots_reject_aliases_and_nesting(tmp_path: Path, layout: str) -> None:
    spool = tmp_path / "spool"
    spool.mkdir()
    if layout == "alias":
        capture_root = tmp_path / "capture-alias"
        capture_root.symlink_to(spool, target_is_directory=True)
    else:
        capture_root = spool / "captures"

    store = StagingStore(spool, capture_root)
    with pytest.raises(StagingError, match=r"real directory|distinct, non-nested"):
        store.prepare_streaming_attempt(100, 1)


def test_prepare_rejects_symlink_capture_root(tmp_path: Path) -> None:
    spool = tmp_path / "spool"
    capture_root = _capture_root(tmp_path)
    target = tmp_path / "target"
    target.mkdir()
    capture_root.symlink_to(target, target_is_directory=True)

    with pytest.raises(StagingError, match="capture root must be a real directory"):
        StagingStore(spool, capture_root).prepare_streaming_attempt(100, 1)


def test_prepare_rejects_existing_non_directory_root(tmp_path: Path) -> None:
    root = tmp_path / "root"
    root.write_text("root is unexpectedly a file", encoding="utf-8")
    with pytest.raises(StagingError, match="real directory"):
        StagingStore(root, root / "captures", state_db=StateDB(tmp_path / "state.sqlite3")).prepare_streaming_attempt(
            1, 1
        )


def test_storage_preflight_creates_and_durably_probes_missing_roots(tmp_path: Path) -> None:
    spool = tmp_path / "collector"
    capture_root = tmp_path / "pipeline" / "raw"

    StagingStore(spool, capture_root).preflight_storage()

    assert spool.is_dir()
    assert (spool / "attempts").is_dir()
    assert (spool / "quarantine").is_dir()
    assert capture_root.is_dir()
    assert not tuple(spool.glob(".storage-preflight-*.tmp"))
    assert not tuple(capture_root.glob(".storage-preflight-*.tmp"))


@pytest.mark.parametrize("root_name", ["spool", "capture"])
def test_storage_preflight_rejects_symlink_roots(tmp_path: Path, root_name: str) -> None:
    target = tmp_path / f"{root_name}-target"
    target.mkdir()
    spool = tmp_path / "spool"
    capture_root = tmp_path / "capture"
    (spool if root_name == "spool" else capture_root).symlink_to(target, target_is_directory=True)

    with pytest.raises(StagingError, match="real directory"):
        StagingStore(spool, capture_root, state_db=StateDB(tmp_path / "state.sqlite3")).preflight_storage()


def test_storage_preflight_rejects_unwritable_directory_and_preserves_cause(tmp_path: Path) -> None:
    spool = tmp_path / "spool"
    capture_root = tmp_path / "capture"
    spool.mkdir()
    (spool / "attempts").mkdir()
    (spool / "quarantine").mkdir()
    capture_root.mkdir()

    def fail_fsync(_: int) -> None:
        raise PermissionError("simulated durable-write denial")

    with pytest.raises(StagingError, match="writable and durable") as raised:
        StagingStore(spool, capture_root, fsync_fn=fail_fsync).preflight_storage()

    assert isinstance(raised.value.__cause__, PermissionError)
    assert "simulated durable-write denial" in str(raised.value.__cause__)


def test_invalid_utf8_descriptor_is_preserved_as_unreadable_evidence(tmp_path: Path) -> None:
    attempt_id = "a" * 32
    attempt_path = tmp_path / "attempts" / attempt_id
    attempt_path.mkdir(parents=True)
    (attempt_path / "records.bin").write_bytes(b"preserve")
    store = StagingStore(tmp_path, _capture_root(tmp_path))
    store.state_db.put(StateKind.ATTEMPT, attempt_id, {"descriptor": {"invalid": True}})
    with pytest.raises(AttemptStateError, match="attempt state is missing or malformed"):
        store.open_attempt(attempt_id)
    assert (attempt_path / "records.bin").read_bytes() == b"preserve"


def test_statvfs_and_atomic_write_errors_leave_evidence(tmp_path: Path) -> None:
    def fail_statvfs(_: str | Path) -> object:
        raise OSError("simulated statvfs failure")

    with pytest.raises(OSError, match="statvfs"):
        StagingStore(tmp_path, _capture_root(tmp_path), statvfs_fn=fail_statvfs).prepare_streaming_attempt(1, 1)

    calls = 0

    def fail_descriptor_sync(_: int) -> None:
        nonlocal calls
        calls += 1
        if calls >= 5:
            raise OSError("simulated atomic write failure")
        fsync(_)

    store = StagingStore(tmp_path, _capture_root(tmp_path), fsync_fn=fail_descriptor_sync)
    with pytest.raises(OSError, match="atomic write"):
        store.prepare_streaming_attempt(1, 1)
    assert store.state_db.list(StateKind.ATTEMPT) == ()


def test_public_recovery_accepts_matching_temporary_when_destination_is_regular(tmp_path: Path) -> None:
    capture_root = _capture_root(tmp_path)
    attempt = _started_attempt(tmp_path, count=1)
    attempt.accept_chunk(100, _record(1))
    bundle = attempt.seal(DoneNotification(0, 101)).bundle_path
    before = {path.name: path.read_bytes() for path in bundle.iterdir()}
    temporary = capture_root / f".{bundle.name}.{'a' * 32}.tmp"
    temporary.mkdir()
    for name, content in before.items():
        (temporary / name).write_bytes(content)

    with StagingStore(tmp_path, capture_root).device_lock(operation="recovery_test"):
        pass

    assert not temporary.exists()
    assert {path.name: path.read_bytes() for path in bundle.iterdir()} == before
