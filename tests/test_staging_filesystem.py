"""Focused filesystem staging ownership tests."""

from __future__ import annotations

from collections.abc import Callable
from errno import EXDEV
from json import dumps, loads
from os import PathLike, fsync
from pathlib import Path
from shutil import rmtree
from typing import cast

import pytest

from omi_collector.capture.adapters import staging_filesystem
from omi_collector.capture.adapters.staging_contract import AttemptStateError, StagingError
from omi_collector.capture.adapters.staging_store import StagingStore
from omi_collector.capture.domain.ring_protocol import RECORD_SIZE, ReadBeginNotification

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
    attempt = StagingStore(tmp_path, _capture_root(tmp_path)).prepare_attempt("omi_cv1", 100, count)
    attempt.record_read_begin(ReadBeginNotification(100, count))
    return attempt


def _started_streaming_attempt(tmp_path: Path, *, count: int = 2, fsync_fn: Callable[[int], None] = fsync):
    attempt = StagingStore(tmp_path, _capture_root(tmp_path), fsync_fn=fsync_fn).prepare_streaming_attempt(
        "omi_cv1", 100, count
    )
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
        store.prepare_attempt("omi_cv1", 100, 1)


def test_prepare_rejects_symlink_capture_device_root(tmp_path: Path) -> None:
    spool = tmp_path / "spool"
    capture_root = _capture_root(tmp_path)
    capture_root.mkdir()
    target = tmp_path / "target"
    target.mkdir()
    (capture_root / "omi_cv1").symlink_to(target, target_is_directory=True)

    with pytest.raises(StagingError, match="capture device root must be a real directory"):
        StagingStore(spool, capture_root).prepare_attempt("omi_cv1", 100, 1)


def test_prepare_rejects_existing_non_directory_root(tmp_path: Path) -> None:
    root = tmp_path / "root"
    root.write_text("root is unexpectedly a file", encoding="utf-8")
    with pytest.raises(StagingError, match="real directory"):
        StagingStore(root, root / "captures").prepare_attempt("omi_cv1", 1, 1)


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
        StagingStore(tmp_path, _capture_root(tmp_path), statvfs_fn=fail_statvfs).prepare_attempt("omi_cv1", 1, 1)

    calls = 0

    def fail_descriptor_sync(_: int) -> None:
        nonlocal calls
        calls += 1
        if calls >= 5:
            raise OSError("simulated atomic write failure")
        fsync(_)

    with pytest.raises(OSError, match="atomic write"):
        StagingStore(tmp_path, _capture_root(tmp_path), fsync_fn=fail_descriptor_sync).prepare_attempt("omi_cv1", 1, 1)
    assert list((tmp_path / "attempts").glob("*/.attempt.json.*.tmp"))
