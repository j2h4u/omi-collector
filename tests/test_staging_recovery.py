"""Focused recovery staging ownership tests."""

from __future__ import annotations

from collections.abc import Callable
from errno import EXDEV
from hashlib import sha256
from json import dumps, loads
from os import PathLike, fsync
from pathlib import Path
from shutil import rmtree
from typing import cast

import pytest

from omi_collector.capture.adapters import quarantine, recovery, staging_filesystem
from omi_collector.capture.adapters.attempts import (
    RecordDisposition,
    RecordGapError,
    RecordMismatchError,
    RecordRegressionError,
)
from omi_collector.capture.adapters.staging_contract import AttemptStateError
from omi_collector.capture.adapters.staging_store import StagingStore
from omi_collector.capture.domain.ring_protocol import RECORD_SIZE, DoneNotification, ReadBeginNotification
from omi_collector.config import CollectorConfig, StagingRetentionConfig

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


def test_streaming_append_uses_one_buffer_and_no_per_record_recovery_or_fsync(tmp_path: Path) -> None:
    sync_calls = 0

    def track_sync(fd: int) -> None:
        nonlocal sync_calls
        sync_calls += 1
        fsync(fd)

    attempt = _started_streaming_attempt(tmp_path, count=3, fsync_fn=track_sync)
    before_records = sync_calls
    attempt.recover = lambda: (_ for _ in ()).throw(AssertionError("streaming append recovered"))
    for index in range(3):
        attempt.append_record(index, 100 + index, _record(index + 1))
    assert sync_calls == before_records
    attempt.close()
    assert (attempt.path / "records.bin").read_bytes() == _record(1) + _record(2) + _record(3)


@pytest.mark.parametrize("damage", ["terminal_prefix", "recoverable_prefix", "checkpoint"])
def test_terminal_retired_recognition_requires_matching_local_recovery_evidence(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, damage: str
) -> None:
    now = 1_000_000_000

    def wall_clock_ns() -> int:
        return now

    monkeypatch.setattr(quarantine, "_wall_clock_ns", wall_clock_ns)
    store = StagingStore(
        tmp_path,
        _capture_root(tmp_path),
        config=CollectorConfig(staging_retention=StagingRetentionConfig(terminal_retention_seconds=1.0)),
    )
    attempt = store.prepare_streaming_attempt("omi_cv1", 100, 1)
    attempt.record_read_begin(ReadBeginNotification(100, 1))
    attempt.append_record(0, 100, _record(1))
    attempt.checkpoint()
    assert attempt.publish_prefix() is not None
    attempt.close(durable=True)
    store.terminalize_prefix_attempt("omi_cv1", attempt.attempt_id)

    if damage == "terminal_prefix":
        marker = cast(dict[str, object], loads((attempt.path / "terminal-retired.json").read_text(encoding="utf-8")))
        marker["prefix"] = {**cast(dict[str, object], marker["prefix"]), "record_count": 0}
        (attempt.path / "terminal-retired.json").write_text(dumps(marker), encoding="utf-8")
    elif damage == "recoverable_prefix":
        (attempt.path / "prefix-publication.json").unlink()
    else:
        (attempt.path / "checkpoint.json").unlink()

    now += 1_000_000_000
    assert store.pending_attempts("omi_cv1") == (attempt.descriptor,)
    assert store.sweep_terminal_retired("omi_cv1") == ()
    assert attempt.path.exists()


def test_recovery_accepts_overlap_replay_then_exact_append(tmp_path: Path) -> None:
    attempt = _started_streaming_attempt(tmp_path, count=3)
    first, second, third = _record(1), _record(2), _record(3)
    attempt.append_record(0, 100, first)
    attempt.checkpoint()
    reopened = attempt
    reopened.begin_recovery(100, 3)
    assert reopened.accept_record(100, first) is RecordDisposition.REPLAYED
    assert reopened.accept_record(101, second) is RecordDisposition.APPENDED
    assert reopened.accept_record(102, third) is RecordDisposition.APPENDED
    reopened.checkpoint()
    assert (reopened.path / "records.bin").read_bytes() == first + second + third


def test_recovery_rejects_mismatch_gap_and_regression(tmp_path: Path) -> None:
    attempt = _started_streaming_attempt(tmp_path, count=3)
    first = _record(1)
    attempt.append_record(0, 100, first)
    attempt.checkpoint()
    reopened = attempt

    reopened.begin_recovery(100, 3)
    with pytest.raises(RecordMismatchError):
        reopened.accept_record(100, _record(9))
    with pytest.raises(RecordGapError):
        reopened.accept_record(102, _record(2))
    with pytest.raises(RecordRegressionError):
        reopened.accept_record(99, _record(9))


def test_recovery_metadata_is_durable_before_accepting_data(tmp_path: Path) -> None:
    attempt = _started_streaming_attempt(tmp_path, count=2)
    attempt.append_record(0, 100, _record(1))
    attempt.checkpoint()
    reopened = attempt
    reopened.begin_recovery(100, 2)
    persisted = cast(dict[str, object], loads((reopened.path / "attempt.json").read_text(encoding="utf-8")))

    assert persisted["recovery_leg"] == {"packet_count": 2, "start_sequence": 100}
    assert reopened.accept_record(100, _record(1)) is RecordDisposition.REPLAYED


def test_seal_after_live_recovery_keeps_manifest_identity_and_receipt_provenance(tmp_path: Path) -> None:
    attempt = _started_streaming_attempt(tmp_path, count=2)
    first, second = _record(1), _record(2)
    attempt.append_record(0, 100, first)
    attempt.checkpoint()
    reopened = attempt
    reopened.begin_recovery(100, 2)
    reopened.accept_record(100, first)
    reopened.accept_record(101, second)

    result = reopened.seal(DoneNotification(0, 102))
    manifest = cast(dict[str, object], loads((result.bundle_path / "manifest.json").read_text(encoding="utf-8")))
    receipt = cast(dict[str, object], loads((result.bundle_path / "receipt.json").read_text(encoding="utf-8")))

    assert "recovery_leg" not in manifest
    assert receipt["recovery_leg"] == {"packet_count": 2, "start_sequence": 100, "next_sequence": 102}


@pytest.mark.parametrize("damage", ["truncate", "tamper"])
def test_recovery_uses_only_valid_contiguous_prefix(tmp_path: Path, damage: str) -> None:
    attempt = _started_attempt(tmp_path, count=2)
    attempt.append_record(0, 100, _record(1))
    raw = attempt.path / "records.bin"
    if damage == "truncate":
        raw.write_bytes(raw.read_bytes()[:-1])
    else:
        raw.write_bytes(b"x" + raw.read_bytes()[1:])

    recovery = StagingStore(tmp_path, _capture_root(tmp_path)).recover_attempt(attempt.attempt_id)

    assert recovery.valid_records == 0
    assert not recovery.clean
    assert raw.exists()
    with pytest.raises(AttemptStateError, match="preserved"):
        StagingStore(tmp_path, _capture_root(tmp_path)).open_attempt(attempt.attempt_id).append_record(
            0, 100, _record(1)
        )


def test_recovery_preserves_torn_and_malformed_attempts(tmp_path: Path) -> None:
    attempt = _started_attempt(tmp_path, count=2)
    attempt.append_record(0, 100, _record(1))
    commits = attempt.path / "commits.jsonl"
    commits.write_text(commits.read_text() + "{bad\n", encoding="utf-8")
    partial = tmp_path / "attempts" / ("f" * 32)
    partial.mkdir()
    (partial / "attempt.json").write_text("{", encoding="utf-8")

    recovered = StagingStore(tmp_path, _capture_root(tmp_path)).recover_attempt(attempt.attempt_id)
    malformed = StagingStore(tmp_path, _capture_root(tmp_path)).recover_attempt("f" * 32)

    assert recovered.valid_records == 1
    assert not recovered.clean
    assert commits.read_text(encoding="utf-8").endswith("{bad\n")
    assert not malformed.clean
    assert malformed.issue is not None


def test_recovery_detects_commit_overflow_and_torn_commit_line(tmp_path: Path) -> None:
    attempt = _started_attempt(tmp_path, count=2)
    records = _record(1) + _record(2) + _record(3)
    attempt.path.joinpath("records.bin").write_bytes(records)
    commits = [
        {"index": index, "sequence": 100 + index, "sha256": sha256(record).hexdigest()}
        for index, record in enumerate((_record(1), _record(2), _record(3)))
    ]
    attempt.path.joinpath("commits.jsonl").write_text(
        "".join(dumps(commit, separators=(",", ":")) + "\n" for commit in commits), encoding="utf-8"
    )
    recovery = attempt.recover()
    assert recovery.valid_records == 2
    assert recovery.issue == "commit count exceeds READ_BEGIN"

    attempt.path.joinpath("commits.jsonl").write_bytes(b"torn commit")
    recovery = attempt.recover()
    assert recovery.valid_records == 0
    assert recovery.issue == "commit is malformed"


def test_validated_prefix_rejects_unbound_descriptor(tmp_path: Path) -> None:
    attempt = StagingStore(tmp_path, _capture_root(tmp_path)).prepare_attempt("omi_cv1", 100, 1)
    valid, consumed, issue = recovery._validated_prefix(attempt.descriptor, [], b"")
    assert (valid, consumed, issue) == (0, 0, "READ_BEGIN is not persisted")
