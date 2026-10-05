"""Focused filesystem staging ownership tests."""

from __future__ import annotations

from collections.abc import Callable
from errno import EXDEV
from json import dumps, loads
from multiprocessing import Event, Process, get_context
from multiprocessing.queues import Queue
from multiprocessing.synchronize import Event as EventType
from os import PathLike, fsync, mkfifo
from pathlib import Path
from shutil import rmtree
from typing import cast

import pytest

from omi_collector.capture.adapters import staging_filesystem, staging_store
from omi_collector.capture.adapters.staging_contract import (
    AttemptStateError,
    CollisionError,
    DeviceAlreadyRunningError,
    StagingError,
)
from omi_collector.capture.adapters.staging_store import StagingStore
from omi_collector.capture.domain.ring_protocol import RECORD_SIZE, DoneNotification, ReadBeginNotification

_CAPTURE_ROOTS: set[Path] = set()


def _capture_root(tmp_path: Path) -> Path:
    root = tmp_path.parent / f"{tmp_path.name}-captures"
    if tmp_path not in _CAPTURE_ROOTS:
        rmtree(root, ignore_errors=True)
        _CAPTURE_ROOTS.add(tmp_path)
    return root


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


def _rewrite_checkpoint(path: Path, field: str, value: object) -> None:
    checkpoint = cast(dict[str, object], loads(path.read_text(encoding="utf-8")))
    checkpoint[field] = value
    path.write_text(dumps(checkpoint), encoding="utf-8")


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


def _hold_device_lock(spool: str, capture_root: str, ready: EventType, release: EventType) -> None:
    with StagingStore(Path(spool), Path(capture_root)).device_lock(operation="capture_batch"):
        ready.set()
        release.wait(10)


def _open_attempt_in_child(
    spool: str,
    capture_root: str,
    attempt_id: str,
    expected_module_path: str,
    results: Queue[tuple[str, str]],
) -> None:
    from omi_collector.capture.adapters.staging_store import StagingStore as ChildStagingStore

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


def test_device_lock_contention_attributes_process_holder_and_reacquires(tmp_path: Path) -> None:
    spool = tmp_path / "spool"
    capture_root = _capture_root(tmp_path)
    ready = Event()
    release = Event()
    holder = Process(target=_hold_device_lock, args=(str(spool), str(capture_root), ready, release))
    holder.start()
    try:
        assert ready.wait(10)
        with (
            pytest.raises(DeviceAlreadyRunningError) as raised,
            StagingStore(spool, capture_root).device_lock(operation="resume_pending_attempt"),
        ):
            pass
        error = raised.value
        context = error.lock_context
        assert context is not None
        assert context.requested_operation == "resume_pending_attempt"
        assert context.holder_operation == "capture_batch"
        assert context.holder_pid == holder.pid
        assert context.holder_thread_id is not None
        assert context.holder_age_seconds is not None
        assert context.holder_age_seconds >= 0
        assert context.holder_scope == "other_process"
        assert context.metadata_status == "valid"
    finally:
        release.set()
        holder.join(10)
    assert holder.exitcode == 0
    with StagingStore(spool, capture_root).device_lock(operation="resume_pending_attempt"):
        pass
    assert (spool / "collector.lock").read_bytes() == b""


def test_device_lock_rejects_stale_owner_metadata(tmp_path: Path) -> None:
    spool = tmp_path / "spool"
    capture_root = _capture_root(tmp_path)
    store = StagingStore(spool, capture_root)
    with store.device_lock(operation="capture_batch"):
        (spool / "collector.lock").write_text(
            dumps(
                {
                    "version": 1,
                    "pid": 999_999_999,
                    "process_start": 1,
                    "thread_id": 1,
                    "operation": "old_operation",
                    "scope": "collector_lock",
                    "acquired_monotonic_ns": 1,
                }
            ),
            encoding="utf-8",
        )
        with (
            pytest.raises(DeviceAlreadyRunningError) as raised,
            StagingStore(spool, capture_root).device_lock(operation="capture_batch"),
        ):
            pass
    context = raised.value.lock_context
    assert context is not None
    assert context.metadata_status == "stale"
    assert context.holder_scope == "unknown"
    assert context.holder_pid is None


def test_device_lock_rejects_same_pid_with_mismatched_process_start(tmp_path: Path) -> None:
    spool = tmp_path / "spool"
    capture_root = _capture_root(tmp_path)
    store = StagingStore(spool, capture_root)
    with store.device_lock(operation="capture_batch"):
        (spool / "collector.lock").write_text(
            dumps(
                {
                    "version": 1,
                    "pid": staging_filesystem.os.getpid(),
                    "process_start": 1,
                    "thread_id": 1,
                    "operation": "old_operation",
                    "scope": "collector_lock",
                    "acquired_monotonic_ns": 1,
                }
            ),
            encoding="utf-8",
        )
        with (
            pytest.raises(DeviceAlreadyRunningError) as raised,
            StagingStore(spool, capture_root).device_lock(operation="capture_batch"),
        ):
            pass
    context = raised.value.lock_context
    assert context is not None
    assert context.metadata_status == "stale"
    assert context.holder_scope == "unknown"
    assert context.holder_pid is None


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("payload", b"{"),
        ("payload", b"\xff"),
        ("payload", b"[]"),
        ("version", 2),
        ("pid", True),
        ("pid", 0),
        ("thread_id", False),
        ("thread_id", 0),
        ("operation", None),
        ("scope", 1),
        ("acquired_monotonic_ns", True),
        ("acquired_monotonic_ns", 0),
    ],
)
def test_device_lock_contention_keeps_invalid_owner_metadata_unknown(tmp_path: Path, field: str, value: object) -> None:
    spool = tmp_path / "spool"
    capture_root = _capture_root(tmp_path)
    store = StagingStore(spool, capture_root)
    lock_path = spool / "collector.lock"

    with store.device_lock(operation="capture_batch"):
        metadata = cast(dict[str, object], loads(lock_path.read_text(encoding="utf-8")))
        if field == "payload":
            lock_path.write_bytes(cast(bytes, value))
        else:
            metadata[field] = value
            lock_path.write_text(dumps(metadata), encoding="utf-8")

        with (
            pytest.raises(DeviceAlreadyRunningError) as raised,
            StagingStore(spool, capture_root).device_lock(operation="resume_pending_attempt"),
        ):
            pass

    context = raised.value.lock_context
    assert context is not None
    assert context.requested_operation == "resume_pending_attempt"
    assert context.metadata_status == "invalid"
    assert context.holder_scope == "unknown"
    assert context.holder_operation is None
    assert context.holder_pid is None
    assert context.holder_thread_id is None
    assert context.holder_age_seconds is None


def test_device_lock_contention_reports_live_owner_and_exact_age(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    spool = tmp_path / "spool"
    capture_root = _capture_root(tmp_path)
    store = StagingStore(spool, capture_root)

    with store.device_lock(operation="capture_batch"):
        metadata = cast(dict[str, object], loads((spool / "collector.lock").read_text(encoding="utf-8")))
        acquired_ns = cast(int, metadata["acquired_monotonic_ns"])
        monkeypatch.setattr(staging_filesystem.time, "monotonic_ns", lambda: acquired_ns + 2_000_000_000)
        with (
            pytest.raises(DeviceAlreadyRunningError) as raised,
            StagingStore(spool, capture_root).device_lock(operation="resume_pending_attempt"),
        ):
            pass

    context = raised.value.lock_context
    assert context is not None
    assert context.requested_operation == "resume_pending_attempt"
    assert context.holder_operation == "capture_batch"
    assert context.holder_pid == staging_filesystem.os.getpid()
    assert context.holder_thread_id is not None
    assert context.holder_scope == "current_process"
    assert context.metadata_status == "valid"
    assert context.holder_age_seconds == pytest.approx(2.0)


def test_device_lock_contention_uses_live_pid_one_metadata(tmp_path: Path) -> None:
    spool = tmp_path / "spool"
    capture_root = _capture_root(tmp_path)
    store = StagingStore(spool, capture_root)
    with store.device_lock(operation="capture_batch"):
        stat_fields = Path("/proc/1/stat").read_text(encoding="ascii").split()
        lock_path = spool / "collector.lock"
        lock_path.write_text(
            dumps(
                {
                    "version": 1,
                    "pid": 1,
                    "process_start": int(stat_fields[21]),
                    "thread_id": 1,
                    "operation": "init",
                    "scope": "collector_lock",
                    "acquired_monotonic_ns": 1,
                }
            ),
            encoding="utf-8",
        )
        with (
            pytest.raises(DeviceAlreadyRunningError) as raised,
            StagingStore(spool, capture_root).device_lock(operation="resume_pending_attempt"),
        ):
            pass
    context = raised.value.lock_context
    assert context is not None
    assert context.metadata_status == "valid"
    assert context.holder_pid == 1
    assert context.holder_scope == "other_process"


def test_device_lock_contender_preserves_live_owner_metadata(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    events: list[tuple[str, dict[str, object]]] = []
    monkeypatch.setattr(
        staging_filesystem,
        "debug_event",
        lambda name, **fields: events.append((name, fields)),
    )
    spool = tmp_path / "spool"
    capture_root = _capture_root(tmp_path)
    store = StagingStore(spool, capture_root)
    lock_path = spool / "collector.lock"

    with store.device_lock(operation="capture_batch"):
        before = lock_path.read_bytes()
        with (
            pytest.raises(DeviceAlreadyRunningError),
            StagingStore(spool, capture_root).device_lock(operation="resume_pending_attempt"),
        ):
            pass
        assert lock_path.read_bytes() == before
        contender_events = tuple(events)
        assert not any(
            name == "device_lock_released" and fields["operation"] == "resume_pending_attempt"
            for name, fields in contender_events
        )

    with StagingStore(spool, capture_root).device_lock(operation="resume_pending_attempt"):
        pass
    released_operations = [fields["operation"] for name, fields in events if name == "device_lock_released"]
    assert released_operations == ["capture_batch", "resume_pending_attempt"]


@pytest.mark.parametrize("process_start", [None, "float"])
def test_device_lock_rejects_noncanonical_process_start_as_stale(tmp_path: Path, process_start: object) -> None:
    spool = tmp_path / "spool"
    capture_root = _capture_root(tmp_path)
    store = StagingStore(spool, capture_root)
    with store.device_lock(operation="capture_batch"):
        lock_path = spool / "collector.lock"
        metadata = cast(dict[str, object], loads(lock_path.read_text(encoding="utf-8")))
        actual_start = metadata["process_start"]
        metadata["process_start"] = None if process_start is None else float(cast(int, actual_start))
        lock_path.write_text(dumps(metadata), encoding="utf-8")
        with (
            pytest.raises(DeviceAlreadyRunningError) as raised,
            StagingStore(spool, capture_root).device_lock(operation="resume_pending_attempt"),
        ):
            pass
    context = raised.value.lock_context
    assert context is not None
    assert context.metadata_status == "stale"
    assert context.holder_scope == "unknown"
    assert context.holder_pid is None


def test_device_lock_diagnostics_report_bounded_lease_duration(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    events: list[tuple[str, dict[str, object]]] = []
    monkeypatch.setattr(
        staging_filesystem,
        "debug_event",
        lambda name, **fields: events.append((name, fields)),
    )
    store = StagingStore(tmp_path / "spool", _capture_root(tmp_path))
    started = staging_filesystem.time.monotonic_ns()

    with store.device_lock(operation="capture_batch"):
        pass

    elapsed = (staging_filesystem.time.monotonic_ns() - started) / 1_000_000_000
    acquired = next(fields for name, fields in events if name == "device_lock_acquired")
    released = next(fields for name, fields in events if name == "device_lock_released")
    assert acquired["operation"] == "capture_batch"
    assert released["operation"] == "capture_batch"
    assert acquired["metadata_status"] == released["metadata_status"] == "valid"
    duration = released["duration_seconds"]
    assert isinstance(duration, float)
    assert 0 <= duration <= elapsed + 0.1


@pytest.mark.parametrize("fault", ["short", "error"])
def test_device_lock_metadata_write_failure_is_diagnostic_only(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, fault: str
) -> None:
    events: list[tuple[str, dict[str, object]]] = []
    monkeypatch.setattr(
        staging_filesystem,
        "debug_event",
        lambda name, **fields: events.append((name, fields)),
    )
    lock_path = tmp_path / "spool" / "collector.lock"
    real_pwrite = staging_filesystem.os.pwrite

    def injected_pwrite(fd: int, payload: bytes, offset: int) -> int:
        if Path(f"/proc/self/fd/{fd}").resolve() == lock_path.resolve():
            if fault == "error":
                raise OSError("injected diagnostic metadata failure")
            return 0
        return real_pwrite(fd, payload, offset)

    monkeypatch.setattr(staging_filesystem.os, "pwrite", injected_pwrite)
    store = StagingStore(tmp_path / "spool", _capture_root(tmp_path))
    with store.device_lock(operation="capture_batch"):
        pass

    acquired = next(fields for name, fields in events if name == "device_lock_acquired")
    released = next(fields for name, fields in events if name == "device_lock_released")
    assert acquired["metadata_status"] == released["metadata_status"] == "write_failed"
    with store.device_lock(operation="retry"):
        pass


def test_public_checkpoint_rejects_missing_checkpoint_without_creating_it(tmp_path: Path) -> None:
    attempt = _started_streaming_attempt(tmp_path, count=1)
    attempt.accept_chunk(100, _record(1))
    checkpoint = attempt.path / "checkpoint.json"
    checkpoint.unlink()
    raw_before = (attempt.path / "records.bin").read_bytes()

    try:
        with pytest.raises(AttemptStateError, match="checkpoint is missing"):
            attempt.checkpoint()

        assert not checkpoint.exists()
        assert (attempt.path / "records.bin").read_bytes() == raw_before
    finally:
        attempt.close()


def test_open_rejects_checkpoint_with_nonhex_hash_without_rewriting_raw(tmp_path: Path) -> None:
    attempt = _started_streaming_attempt(tmp_path, count=1)
    attempt.accept_chunk(100, _record(1))
    attempt.close()
    checkpoint = attempt.path / "checkpoint.json"
    _rewrite_checkpoint(checkpoint, "raw_sha256", "z" * 64)
    raw_before = (attempt.path / "records.bin").read_bytes()
    checkpoint_before = checkpoint.read_bytes()

    with pytest.raises(AttemptStateError, match="checkpoint is malformed"):
        StagingStore(tmp_path, _capture_root(tmp_path)).open_attempt(attempt.attempt_id)

    assert (attempt.path / "records.bin").read_bytes() == raw_before
    assert checkpoint.read_bytes() == checkpoint_before


@pytest.mark.parametrize("fifo_name", ["records.bin", "attempt.json"])
def test_open_attempt_rejects_fifo_without_blocking(tmp_path: Path, fifo_name: str) -> None:
    attempt = _started_streaming_attempt(tmp_path, count=1)
    attempt.close()
    fifo_path = attempt.path / fifo_name
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


def test_public_checkpoint_rejects_corrupt_hash_after_append_without_rewriting_evidence(
    tmp_path: Path,
) -> None:
    attempt = _started_streaming_attempt(tmp_path, count=1)
    attempt.accept_chunk(100, _record(1))
    checkpoint = attempt.path / "checkpoint.json"
    _rewrite_checkpoint(checkpoint, "raw_sha256", "z" * 64)
    checkpoint_before = checkpoint.read_bytes()
    raw_before = (attempt.path / "records.bin").read_bytes()

    try:
        with pytest.raises(AttemptStateError, match="checkpoint is malformed"):
            attempt.checkpoint()

        assert checkpoint.read_bytes() == checkpoint_before
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
        StagingStore(root, root / "captures").prepare_streaming_attempt(1, 1)


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
        StagingStore(spool, capture_root).preflight_storage()


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
    (attempt_path / "attempt.json").write_bytes(b"\xff")
    with pytest.raises(AttemptStateError, match=r"cannot read attempt\.json"):
        StagingStore(tmp_path, _capture_root(tmp_path)).open_attempt(attempt_id)


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

    with pytest.raises(OSError, match="atomic write"):
        StagingStore(tmp_path, _capture_root(tmp_path), fsync_fn=fail_descriptor_sync).prepare_streaming_attempt(1, 1)
    assert list((tmp_path / "attempts").glob("*/.attempt.json.*.tmp"))


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
