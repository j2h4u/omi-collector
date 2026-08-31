"""Focused publication staging ownership tests."""

from __future__ import annotations

from collections.abc import Callable
from errno import EXDEV
from hashlib import sha256
from json import dumps, loads
from os import PathLike, fsync
from pathlib import Path
from shutil import rmtree
from stat import S_IMODE
from typing import cast

import pytest

from omi_collector.capture.adapters import publication, quarantine
from omi_collector.capture.adapters.attempts import (
    StagedAttempt,
)
from omi_collector.capture.adapters.publication import PrefixPublicationEvidence, TerminalRetirementEvidence
from omi_collector.capture.adapters.staging_contract import (
    AttemptStateError,
    CollisionError,
    DiskSpaceError,
    DurablePrefix,
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


def test_publication_evidence_round_trips_exact_marker_wire_shape() -> None:
    prefix = DurablePrefix(100, 102, 2, "a" * 64)
    publication = PrefixPublicationEvidence(prefix, "omi_cv1/100-102-aaaaaaaaaaaaaaaa")
    terminal = TerminalRetirementEvidence("b" * 32, prefix, 1)

    assert publication.as_dict() == {
        "version": 1,
        "prefix": {
            "start_sequence": 100,
            "next_sequence": 102,
            "record_count": 2,
            "raw_sha256": "a" * 64,
        },
        "destination": "omi_cv1/100-102-aaaaaaaaaaaaaaaa",
    }
    assert PrefixPublicationEvidence.from_json(publication.as_dict()) == publication
    assert TerminalRetirementEvidence.from_json(terminal.as_dict()) == terminal


def test_publication_evidence_rejects_extra_marker_keys() -> None:
    with pytest.raises(AttemptStateError, match="schema"):
        PrefixPublicationEvidence.from_json(
            {
                "version": 1,
                "prefix": {
                    "start_sequence": 100,
                    "next_sequence": 102,
                    "record_count": 2,
                    "raw_sha256": "a" * 64,
                },
                "destination": None,
                "extra": True,
            }
        )


def test_publication_evidence_canonical_json_preserves_marker_bytes() -> None:
    prefix = DurablePrefix(100, 102, 2, "a" * 64)
    publication_evidence = PrefixPublicationEvidence(prefix, "omi_cv1/100-102-aaaaaaaaaaaaaaaa")
    terminal_evidence = TerminalRetirementEvidence("b" * 32, prefix, 1)

    assert publication._json_bytes(publication_evidence.as_dict()) == (
        b'{"destination":"omi_cv1/100-102-aaaaaaaaaaaaaaaa","prefix":'
        b'{"next_sequence":102,"raw_sha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",'
        b'"record_count":2,"start_sequence":100},"version":1}'
    )
    assert publication._json_bytes(terminal_evidence.as_dict()) == (
        b'{"attempt_id":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","prefix":'
        b'{"next_sequence":102,"raw_sha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",'
        b'"record_count":2,"start_sequence":100},"state":"terminal-retired",'
        b'"terminalized_at_unix_ns":1,"version":1}'
    )


def test_terminal_retirement_evidence_rejects_extra_marker_keys() -> None:
    with pytest.raises(AttemptStateError, match="schema"):
        TerminalRetirementEvidence.from_json(
            {
                "version": 1,
                "state": "terminal-retired",
                "attempt_id": "b" * 32,
                "prefix": {
                    "start_sequence": 100,
                    "next_sequence": 102,
                    "record_count": 2,
                    "raw_sha256": "a" * 64,
                },
                "terminalized_at_unix_ns": 1,
                "extra": True,
            }
        )


def _guard_rename_to_capture(monkeypatch: pytest.MonkeyPatch) -> None:
    real_rename = publication.os.rename

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

    monkeypatch.setattr(publication.os, "rename", guarded_rename)


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


def test_split_roots_publish_only_completed_bundles_to_capture_root(tmp_path: Path) -> None:
    spool = tmp_path / "spool"
    capture_root = _capture_root(tmp_path)
    attempt = StagingStore(spool, capture_root).prepare_attempt("omi_cv1", 100, 1)
    attempt.record_read_begin(ReadBeginNotification(100, 1))
    attempt.append_record(0, 100, _record(1))

    result = attempt.seal(DoneNotification(0, 101))

    assert result.bundle_path.is_relative_to(capture_root)
    assert not result.bundle_path.is_relative_to(spool)
    assert not (spool / "omi_cv1").exists()
    assert not tuple((capture_root / "omi_cv1").glob(".*.tmp"))


def test_prefix_publication_uses_capture_local_rename(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    capture_root = _capture_root(tmp_path)
    attempt = _started_streaming_attempt(tmp_path, count=2)
    attempt.append_record(0, 100, _record(1))
    attempt.checkpoint()
    source_raw = (attempt.path / "records.bin").read_bytes()

    _guard_rename_to_capture(monkeypatch)

    result = attempt.publish_prefix()

    assert result is not None
    assert attempt.path.exists()
    assert (attempt.path / "records.bin").read_bytes() == source_raw
    assert (attempt.path / "prefix-publication.json").exists()
    assert result.bundle_path.exists()
    assert not tuple((capture_root / "omi_cv1").glob(".*.tmp"))


def test_full_seal_uses_capture_local_rename(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    spool = tmp_path / "spool"
    capture_root = _capture_root(tmp_path)
    attempt = StagingStore(spool, capture_root).prepare_attempt("omi_cv1", 100, 1)
    attempt.record_read_begin(ReadBeginNotification(100, 1))
    attempt.append_record(0, 100, _record(1))

    _guard_rename_to_capture(monkeypatch)

    result = attempt.seal(DoneNotification(0, 101))

    assert result.bundle_path.exists()
    assert not attempt.path.exists()
    assert not tuple((capture_root / "omi_cv1").glob(".*.tmp"))


def test_full_seal_keeps_source_when_destination_copy_fails(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    spool = tmp_path / "spool"
    capture_root = _capture_root(tmp_path)
    attempt = StagingStore(spool, capture_root).prepare_attempt("omi_cv1", 100, 1)
    attempt.record_read_begin(ReadBeginNotification(100, 1))
    attempt.append_record(0, 100, _record(1))

    def fail_copy(*_: object) -> None:
        raise OSError("simulated destination-copy failure")

    monkeypatch.setattr(publication, "_copy_synced", fail_copy)

    with pytest.raises(OSError, match="destination-copy"):
        attempt.seal(DoneNotification(0, 101))

    assert attempt.path.exists()
    assert not tuple((capture_root / "omi_cv1").glob("100-101-*"))
    assert not tuple((capture_root / "omi_cv1").glob(".*.tmp"))


def test_seal_rejects_capture_root_symlink_swap_before_rename(tmp_path: Path) -> None:
    spool = tmp_path / "spool"
    capture_root = _capture_root(tmp_path)
    attempt = StagingStore(spool, capture_root).prepare_attempt("omi_cv1", 100, 1)
    attempt.record_read_begin(ReadBeginNotification(100, 1))
    attempt.append_record(0, 100, _record(1))
    outside = tmp_path / "outside"
    outside.mkdir()
    backup = tmp_path / "capture-backup"
    capture_root.rename(backup)
    capture_root.symlink_to(outside, target_is_directory=True)

    with pytest.raises(StagingError, match="capture root"):
        attempt.seal(DoneNotification(0, 101))

    assert not (outside / "omi_cv1").exists()
    assert attempt.path.exists()


def test_prefix_publication_rejects_capture_root_symlink_swap_before_rename(tmp_path: Path) -> None:
    spool = tmp_path / "spool"
    capture_root = _capture_root(tmp_path)
    attempt = StagingStore(spool, capture_root).prepare_streaming_attempt("omi_cv1", 100, 1)
    attempt.record_read_begin(ReadBeginNotification(100, 1))
    attempt.append_record(0, 100, _record(1))
    attempt.checkpoint()
    outside = tmp_path / "outside"
    outside.mkdir()
    backup = tmp_path / "capture-backup"
    capture_root.rename(backup)
    capture_root.symlink_to(outside, target_is_directory=True)

    with pytest.raises(StagingError, match="capture root"):
        attempt.publish_prefix()

    assert not (outside / "omi_cv1").exists()
    assert attempt.path.exists()


def test_streaming_seal_is_the_raw_durability_boundary(tmp_path: Path) -> None:
    sync_calls = 0

    def track_sync(fd: int) -> None:
        nonlocal sync_calls
        sync_calls += 1
        fsync(fd)

    attempt = _started_streaming_attempt(tmp_path, count=2, fsync_fn=track_sync)
    attempt.append_record(0, 100, _record(1))
    attempt.append_record(1, 101, _record(2))
    before_seal = sync_calls

    result = attempt.seal(DoneNotification(0, 102))

    assert sync_calls > before_seal
    assert (result.bundle_path / "records.bin").read_bytes() == _record(1) + _record(2)
    assert (result.bundle_path / "manifest.json").is_file()
    assert (result.bundle_path / "receipt.json").is_file()


def test_cursor_ahead_publishes_ordinary_prefix_and_preserves_source_raw(tmp_path: Path) -> None:
    attempt = _started_streaming_attempt(tmp_path, count=4)
    first = _record(1)
    tail = _record(2)
    attempt.append_record(0, 100, first)
    attempt.checkpoint()
    attempt.append_record(1, 101, tail)
    source_raw = (attempt.path / "records.bin").read_bytes()

    result = attempt.publish_prefix()

    assert result is not None
    assert result.bundle_path.joinpath("records.bin").read_bytes() == first
    assert loads(result.bundle_path.joinpath("manifest.json").read_text()) == {
        "device_slug": "omi_cv1",
        "start_sequence": 100,
        "next_sequence": 101,
        "record_count": 1,
        "record_size": RECORD_SIZE,
        "raw_sha256": sha256(first).hexdigest(),
    }
    assert not result.bundle_path.joinpath("gap.json").exists()
    assert "gap_sha256" not in loads(result.bundle_path.joinpath("receipt.json").read_text())
    assert (attempt.path / "records.bin").read_bytes() == source_raw
    assert not tuple((_capture_root(tmp_path) / "omi_cv1").glob(".*.tmp"))
    assert StagingStore(tmp_path, _capture_root(tmp_path)).pending_attempts("omi_cv1") == ()
    marker = cast(dict[str, object], loads((attempt.path / "prefix-publication.json").read_text()))
    assert set(marker) == {"version", "prefix", "destination"}
    duplicate = attempt.publish_prefix()
    assert duplicate is not None
    assert duplicate.deduplicated


def test_cursor_ahead_publication_uses_only_checkpoint_prefix(tmp_path: Path) -> None:
    attempt = _started_streaming_attempt(tmp_path, count=2)
    attempt.append_record(0, 100, _record(1))
    attempt.checkpoint()

    result = attempt.publish_prefix()

    assert result is not None
    assert result.bundle_path.joinpath("records.bin").read_bytes() == _record(1)
    assert not (result.bundle_path / "gap.json").exists()
    assert StagingStore(tmp_path, _capture_root(tmp_path)).pending_attempts("omi_cv1") == ()


def test_zero_prefix_publication_waits_for_terminalization_without_audio_artifact(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(quarantine, "_wall_clock_ns", lambda: 1)
    store = StagingStore(tmp_path, _capture_root(tmp_path))
    attempt = store.prepare_streaming_attempt("omi_cv1", 100, 2)
    attempt.record_read_begin(ReadBeginNotification(100, 2))

    result = attempt.publish_prefix()

    assert result is None
    assert attempt.path.exists()
    assert (attempt.path / "prefix-publication.json").is_file()
    attempt.close(durable=True)
    store.terminalize_prefix_attempt("omi_cv1", attempt.attempt_id)
    assert (attempt.path / "terminal-retired.json").is_file()
    assert not tuple(StagingStore(tmp_path, _capture_root(tmp_path)).pending_attempts("omi_cv1"))


def test_zero_prefix_preserves_nonempty_raw_evidence(tmp_path: Path) -> None:
    store = StagingStore(tmp_path, _capture_root(tmp_path))
    attempt = store.prepare_streaming_attempt("omi_cv1", 100, 2)
    attempt.record_read_begin(ReadBeginNotification(100, 2))
    attempt.append_record(0, 100, _record(1))
    raw = (attempt.path / "records.bin").read_bytes()
    assert attempt.publish_prefix() is None
    assert attempt.path.exists()
    assert (attempt.path / "records.bin").read_bytes() == raw
    assert not store.pending_attempts("omi_cv1")

    with (attempt.path / "records.bin").open("ab") as stream:
        stream.write(b"torn")
    assert not store.pending_attempts("omi_cv1")


def test_streaming_deduplicates_identical_sealed_bundle(tmp_path: Path) -> None:
    first = _started_streaming_attempt(tmp_path, count=1)
    first.append_record(0, 100, _record(1))
    bundle = first.seal(DoneNotification(0, 101)).bundle_path

    duplicate = _started_streaming_attempt(tmp_path, count=1)
    duplicate.append_record(0, 100, _record(1))
    result = duplicate.seal(DoneNotification(0, 101))

    assert result.bundle_path == bundle
    assert result.deduplicated
    assert duplicate.path.exists()
    StagingStore(tmp_path, _capture_root(tmp_path)).assert_no_pending("omi_cv1")
    store = StagingStore(tmp_path, _capture_root(tmp_path))
    store.assert_no_pending("omi_cv1")
    future = store.prepare_streaming_attempt("omi_cv1", 100, 1)
    assert future.path.exists()
    future.close()


def test_seal_writes_bundle_manifest_and_receipt(tmp_path: Path) -> None:
    attempt = _started_attempt(tmp_path)
    attempt.append_record(0, 100, _record(1))
    attempt.append_record(1, 101, _record(2))

    result = attempt.seal(DoneNotification(0, 102))

    assert not result.deduplicated
    assert result.bundle_path.parent == _capture_root(tmp_path) / "omi_cv1"
    assert (result.bundle_path / "records.bin").read_bytes() == _record(1) + _record(2)
    assert (result.bundle_path / "manifest.json").is_file()
    assert (result.bundle_path / "receipt.json").is_file()
    assert not attempt.path.exists()


def test_full_publication_uses_shared_bundle_directory_mode(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    requested_modes: list[int] = []
    real_mkdir = publication.os.mkdir

    def observed_mkdir(
        path: str | bytes | PathLike[str] | PathLike[bytes],
        mode: int = 0o777,
        *,
        dir_fd: int | None = None,
    ) -> None:
        if dir_fd is not None:
            requested_modes.append(mode)
        real_mkdir(path, mode, dir_fd=dir_fd)

    monkeypatch.setattr(publication.os, "mkdir", observed_mkdir)
    attempt = _started_attempt(tmp_path, count=1)
    attempt.append_record(0, 100, _record(1))

    bundle = attempt.seal(DoneNotification(0, 101)).bundle_path

    assert requested_modes == [0o770]
    mode = S_IMODE(bundle.stat().st_mode)
    assert mode & 0o700 == 0o700
    assert mode & 0o007 == 0


def test_seal_requires_success_complete_count_and_next_sequence(tmp_path: Path) -> None:
    attempt = _started_attempt(tmp_path)
    attempt.append_record(0, 100, _record(1))

    with pytest.raises(AttemptStateError):
        attempt.seal(DoneNotification(1, 102))
    with pytest.raises(AttemptStateError):
        attempt.seal(DoneNotification(0, 101))
    with pytest.raises(AttemptStateError):
        attempt.seal(DoneNotification(0, 102))


def test_exact_bundle_collision_deduplicates_but_other_content_is_preserved(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    first = _started_attempt(tmp_path)
    first.append_record(0, 100, _record(1))
    first.append_record(1, 101, _record(2))
    bundle = first.seal(DoneNotification(0, 102)).bundle_path

    duplicate = _started_attempt(tmp_path)
    duplicate.append_record(0, 100, _record(1))
    duplicate.append_record(1, 101, _record(2))
    deduplicated = duplicate.seal(DoneNotification(0, 102))

    assert deduplicated.bundle_path == bundle
    assert deduplicated.deduplicated
    assert duplicate.path.exists()
    assert loads((bundle / "receipt.json").read_text(encoding="utf-8"))["attempt_id"] != duplicate.attempt_id

    conflict = _started_attempt(tmp_path)
    conflict.append_record(0, 100, _record(9))
    conflict.append_record(1, 101, _record(2))
    monkeypatch.setattr(StagedAttempt, "_bundle_path", lambda _attempt, _hash: bundle)
    with pytest.raises(CollisionError):
        conflict.seal(DoneNotification(0, 102))
    assert conflict.path.exists()


@pytest.mark.parametrize("damage", ["missing", "malformed", "wrong_hash", "wrong_status", "wrong_attempt_id"])
def test_invalid_bundle_receipt_prevents_deduplication(tmp_path: Path, damage: str) -> None:
    first = _started_attempt(tmp_path)
    first.append_record(0, 100, _record(1))
    first.append_record(1, 101, _record(2))
    bundle = first.seal(DoneNotification(0, 102)).bundle_path
    receipt = bundle / "receipt.json"
    if damage == "missing":
        receipt.unlink()
    elif damage == "malformed":
        receipt.write_text("{", encoding="utf-8")
    else:
        value = cast(dict[str, object], loads(receipt.read_text(encoding="utf-8")))
        if damage == "wrong_hash":
            value["raw_sha256"] = "0" * 64
        elif damage == "wrong_status":
            value["status"] = "open"
        else:
            value["attempt_id"] = "not-an-attempt-id"
        receipt.write_text(dumps(value), encoding="utf-8")

    duplicate = _started_attempt(tmp_path)
    duplicate.append_record(0, 100, _record(1))
    duplicate.append_record(1, 101, _record(2))

    with pytest.raises(CollisionError):
        duplicate.seal(DoneNotification(0, 102))
    assert duplicate.path.exists()


def test_preflight_and_fsync_failures_stop_before_read_contract(tmp_path: Path) -> None:
    class TooSmall:
        f_bavail = 1
        f_frsize = 1

    with pytest.raises(DiskSpaceError):
        StagingStore(tmp_path, _capture_root(tmp_path), statvfs_fn=lambda _: TooSmall()).prepare_attempt(
            "omi_cv1", 1, 1
        )

    calls = 0

    def fail_first_sync(_: int) -> None:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise OSError("simulated fsync failure")
        fsync(_)

    with pytest.raises(OSError, match="simulated"):
        StagingStore(tmp_path, _capture_root(tmp_path), fsync_fn=fail_first_sync).prepare_attempt("omi_cv1", 1, 1)


def test_seal_requires_read_begin_and_private_bundle_metadata_requires_it(tmp_path: Path) -> None:
    attempt = StagingStore(tmp_path, _capture_root(tmp_path)).prepare_attempt("omi_cv1", 100, 1)
    with pytest.raises(AttemptStateError, match="READ_BEGIN is missing"):
        attempt.seal(DoneNotification(0, 101))
    with pytest.raises(AttemptStateError, match="READ_BEGIN is missing"):
        attempt._bundle_path("0" * 64)
    with pytest.raises(AttemptStateError, match="READ_BEGIN is missing"):
        attempt._manifest("0" * 64)


def test_collision_detects_different_raw_size_and_invalid_receipt_hash(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    first = _started_attempt(tmp_path, count=1)
    first.append_record(0, 100, _record(1))
    bundle = first.seal(DoneNotification(0, 101)).bundle_path

    duplicate = _started_attempt(tmp_path, count=1)
    duplicate.append_record(0, 100, _record(1))
    monkeypatch.setattr(StagedAttempt, "_bundle_path", lambda _attempt, _hash: bundle)
    (bundle / "records.bin").write_bytes(b"short")
    with pytest.raises(CollisionError):
        duplicate.seal(DoneNotification(0, 101))

    (bundle / "records.bin").write_bytes(_record(9))
    with pytest.raises(CollisionError):
        duplicate.seal(DoneNotification(0, 101))

    (bundle / "records.bin").write_bytes(_record(1))
    receipt = cast(dict[str, object], loads((bundle / "receipt.json").read_text(encoding="utf-8")))
    receipt["raw_sha256"] = "invalid"
    (bundle / "receipt.json").write_text(dumps(receipt), encoding="utf-8")
    with pytest.raises(CollisionError):
        duplicate.seal(DoneNotification(0, 101))
