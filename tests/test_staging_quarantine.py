"""Focused quarantine staging ownership tests."""

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

from omi_collector.capture.adapters import quarantine as quarantine_module
from omi_collector.capture.adapters import staging_filesystem
from omi_collector.capture.adapters.staging_contract import (
    AttemptStateError,
    DeviceAlreadyRunningError,
    PendingAttemptError,
    StagingError,
)
from omi_collector.capture.adapters.staging_filesystem import DeviceLock
from omi_collector.capture.adapters.staging_store import StagingStore
from omi_collector.capture.domain.ring_protocol import RECORD_SIZE, DoneNotification, ReadBeginNotification
from omi_collector.config import CollectorConfig, DurabilityConfig, StagingRetentionConfig

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
    attempt = StagingStore(tmp_path, _capture_root(tmp_path)).prepare_streaming_attempt("omi_cv1", 100, count)
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


def test_device_lock_quarantines_hard_crash_publication_leftover(tmp_path: Path) -> None:
    spool = tmp_path / "spool"
    capture_root = _capture_root(tmp_path)
    publishing = capture_root / "omi_cv1"
    publishing.mkdir(parents=True)
    leftover = publishing / ".100-101-deadbeef.tmp"
    leftover.mkdir()
    payload = _record(1)
    (leftover / "records.bin").write_bytes(payload)

    with StagingStore(spool, capture_root).device_lock("omi_cv1"):
        pass

    assert not leftover.exists()
    quarantined = tuple(
        path for path in (spool / "quarantine" / "omi_cv1").glob("capture-temporary-*") if path.is_dir()
    )
    assert len(quarantined) == 1
    assert (quarantined[0] / "records.bin").read_bytes() == payload
    assert (quarantined[0] / "unprocessable.json").is_file()


def test_device_lock_finalizes_complete_capture_local_publication_temporary(tmp_path: Path) -> None:
    spool = tmp_path / "spool"
    capture_root = _capture_root(tmp_path)
    attempt = StagingStore(spool, capture_root).prepare_streaming_attempt("omi_cv1", 100, 1)
    attempt.record_read_begin(ReadBeginNotification(100, 1))
    attempt.append_record(0, 100, _record(1))
    result = attempt.seal(DoneNotification(0, 101))
    temporary = result.bundle_path.with_name(f".{result.bundle_path.name}.{'a' * 32}.tmp")
    result.bundle_path.replace(temporary)

    with StagingStore(spool, capture_root).device_lock("omi_cv1"):
        pass

    assert result.bundle_path.is_dir()
    assert not temporary.exists()
    assert (result.bundle_path / "records.bin").read_bytes() == _record(1)


def test_sweep_quarantine_deletes_terminal_evidence_after_shared_retention(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    now = 1_000_000_000
    monkeypatch.setattr(quarantine_module, "_wall_clock_ns", lambda: now)
    config = CollectorConfig(staging_retention=StagingRetentionConfig(terminal_retention_seconds=72.0))
    store = StagingStore(tmp_path, _capture_root(tmp_path), config=config)
    root = tmp_path / "quarantine" / "omi_cv1"
    published = root / "published-source"
    unprocessable = root / "unprocessable-source"
    published.mkdir(parents=True)
    unprocessable.mkdir()
    store.mark_quarantine_published("omi_cv1", published)
    store.mark_quarantine_unprocessable("omi_cv1", unprocessable, "strict proof failed")

    now += 72_000_000_000

    assert set(store.sweep_terminal_quarantine("omi_cv1")) == {published, unprocessable}
    assert not published.exists()
    assert not unprocessable.exists()


def test_device_lock_rejects_symlink_publishing_root(tmp_path: Path) -> None:
    spool = tmp_path / "spool"
    capture_root = _capture_root(tmp_path)
    outside = tmp_path / "outside-publishing"
    outside.mkdir()
    (capture_root / "omi_cv1").parent.mkdir(parents=True)
    (capture_root / "omi_cv1").symlink_to(outside, target_is_directory=True)

    with (
        pytest.raises(StagingError, match="capture device root"),
        StagingStore(spool, capture_root).device_lock("omi_cv1"),
    ):
        pass


def test_terminal_retired_marker_ignores_missing_destination_then_expires_only_its_partial(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    now = 1_000_000_000

    def wall_clock_ns() -> int:
        return now

    config = CollectorConfig(staging_retention=StagingRetentionConfig(terminal_retention_seconds=72.0))
    monkeypatch.setattr(quarantine_module, "_wall_clock_ns", wall_clock_ns)
    store = StagingStore(tmp_path, _capture_root(tmp_path), config=config)
    attempt = store.prepare_streaming_attempt("omi_cv1", 100, 2)
    attempt.record_read_begin(ReadBeginNotification(100, 2))
    attempt.append_record(0, 100, _record(1))
    attempt.checkpoint()
    result = attempt.publish_prefix()
    assert result is not None
    attempt.close(durable=True)
    store.terminalize_prefix_attempt("omi_cv1", attempt.attempt_id)

    marker = cast(dict[str, object], loads((attempt.path / "terminal-retired.json").read_text(encoding="utf-8")))
    assert marker == {
        "version": 1,
        "state": "terminal-retired",
        "terminalized_at_unix_ns": now,
    }
    rmtree(result.bundle_path)
    active = store.prepare_streaming_attempt("omi_cv1", 200, 1)
    quarantine = tmp_path / "quarantine" / "omi_cv1"
    quarantine.mkdir(parents=True)
    preserved = quarantine / "preserved"
    preserved.write_text("evidence", encoding="utf-8")

    assert store.pending_attempts("omi_cv1") == (active.descriptor,)
    assert store.sweep_terminal_retired("omi_cv1") == ()
    assert attempt.path.exists()
    now += 72_000_000_000
    assert store.sweep_terminal_retired("omi_cv1") == (attempt.path,)
    assert not attempt.path.exists()
    assert active.path.exists()
    assert preserved.read_text(encoding="utf-8") == "evidence"


def test_terminal_retired_sweep_does_not_rehash_records_before_delete(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    now = 1_000_000_000
    monkeypatch.setattr(quarantine_module, "_wall_clock_ns", lambda: now)
    store = StagingStore(
        tmp_path,
        _capture_root(tmp_path),
        config=CollectorConfig(
            durability=DurabilityConfig(io_chunk_bytes=1024),
            staging_retention=StagingRetentionConfig(terminal_retention_seconds=1.0),
        ),
    )
    attempt = store.prepare_streaming_attempt("omi_cv1", 100, 5)
    attempt.record_read_begin(ReadBeginNotification(100, 5))
    attempt.accept_chunk(100, memoryview(b"x" * (5 * RECORD_SIZE)))
    attempt.checkpoint()
    assert attempt.publish_prefix() is not None
    attempt.close(durable=True)
    store.terminalize_prefix_attempt("omi_cv1", attempt.attempt_id)
    now += 1_000_000_000
    monkeypatch.setattr(
        quarantine_module,
        "_published_prefix",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("retired records were rehashed")),
    )
    assert store.sweep_terminal_retired("omi_cv1") == (attempt.path,)
    assert not attempt.path.exists()


def test_quarantine_attempt_source_preserves_only_existing_attempt_files(tmp_path: Path) -> None:
    store = StagingStore(tmp_path, _capture_root(tmp_path))
    attempt = _started_streaming_attempt(tmp_path, count=2)
    attempt.append_record(0, 100, _record(1))
    attempt.checkpoint()
    attempt_id = attempt.attempt_id
    expected_files = {path.name for path in attempt.path.iterdir()}
    attempt.close(durable=True)

    destination = store.quarantine_attempt_source("omi_cv1", attempt_id)

    assert not attempt.path.exists()
    assert destination.name.startswith(f"{attempt_id}-")
    assert {path.name for path in destination.iterdir()} == expected_files
    assert tuple(destination.parent.iterdir()) == (destination,)
    assert not store.pending_attempts("omi_cv1")


def test_streaming_partial_close_is_preserved_and_blocks_pending(tmp_path: Path) -> None:
    attempt = _started_streaming_attempt(tmp_path, count=2)
    attempt.append_record(0, 100, _record(1))
    attempt.close()

    reopened = StagingStore(tmp_path, _capture_root(tmp_path)).open_attempt(attempt.attempt_id)
    recovery = reopened.recover()
    assert not recovery.clean
    assert recovery.raw_bytes == RECORD_SIZE
    with pytest.raises(AttemptStateError, match="preserved partial evidence"):
        reopened.append_record(1, 101, _record(2))
    with pytest.raises(PendingAttemptError):
        StagingStore(tmp_path, _capture_root(tmp_path)).assert_no_pending("omi_cv1")
    assert attempt.path.exists()


def test_pending_attempts_fail_closed_on_unattributed_malformed_evidence(tmp_path: Path) -> None:
    store = StagingStore(tmp_path, _capture_root(tmp_path))
    matching = store.prepare_streaming_attempt("omi_cv1", 100, 2)

    assert store.pending_attempts("omi_cv1") == (matching.descriptor,)
    with pytest.raises(PendingAttemptError, match="blocks another READ"):
        store.assert_no_pending("omi_cv1")

    malformed = tmp_path / "attempts" / ("f" * 32)
    malformed.mkdir()
    (malformed / "attempt.json").write_text("{", encoding="utf-8")
    with pytest.raises(PendingAttemptError, match="malformed partial attempt evidence"):
        store.pending_attempts("other")
    moved = store.quarantine_pending("other", "unattributed malformed evidence")
    assert len(moved) == 1
    assert not malformed.exists()


def test_pending_attempts_fail_closed_on_invalid_malformed_attribution(tmp_path: Path) -> None:
    store = StagingStore(tmp_path, _capture_root(tmp_path))
    malformed = tmp_path / "attempts" / ("f" * 32)
    malformed.mkdir(parents=True)
    (malformed / "attempt.json").write_text(
        dumps({"attempt_id": malformed.name, "device_slug": "../bad"}), encoding="utf-8"
    )

    with pytest.raises(PendingAttemptError, match="malformed partial attempt evidence"):
        store.pending_attempts("omi_cv1")
    moved = store.quarantine_pending("omi_cv1", "invalid descriptor attribution")
    assert len(moved) == 1
    assert not malformed.exists()
    assert (moved[0].with_name(f"{moved[0].name}.json")).is_file()


def test_attributable_malformed_evidence_is_quarantined_without_read_authorization(tmp_path: Path) -> None:
    store = StagingStore(tmp_path, _capture_root(tmp_path))
    malformed = tmp_path / "attempts" / ("f" * 32)
    malformed.mkdir(parents=True)
    descriptor = dumps({"attempt_id": malformed.name, "device_slug": "omi_cv1"})
    (malformed / "attempt.json").write_text(descriptor, encoding="utf-8")
    (malformed / "records.bin").write_bytes(b"unverified bytes")
    before = {path.name: path.read_bytes() for path in malformed.iterdir()}

    with pytest.raises(PendingAttemptError, match="malformed partial attempt evidence"):
        store.pending_attempts("omi_cv1")
    moved = store.quarantine_pending("omi_cv1", "malformed descriptor")

    assert len(moved) == 1
    assert not malformed.exists()
    assert {path.name: path.read_bytes() for path in moved[0].iterdir() if path.name != "unprocessable.json"} == before
    store.assert_no_pending("omi_cv1")


def test_quarantine_pending_moves_blockers_and_preserves_unrelated_and_retired(tmp_path: Path) -> None:
    store = StagingStore(tmp_path, _capture_root(tmp_path))
    matching = store.prepare_streaming_attempt("omi_cv1", 100, 2)
    unrelated = store.prepare_streaming_attempt("other", 200, 2)
    published = store.prepare_streaming_attempt("omi_cv1", 300, 1)
    assert published.publish_prefix() is None
    published.close(durable=True)
    store.terminalize_prefix_attempt("omi_cv1", published.attempt_id)
    malformed = tmp_path / "attempts" / ("f" * 32)
    malformed.mkdir()
    (malformed / "attempt.json").write_text("{", encoding="utf-8")

    moved = store.quarantine_pending("omi_cv1", "manual recovery")

    assert len(moved) == 2
    assert not matching.path.exists()
    assert not malformed.exists()
    assert unrelated.path.exists()
    assert published.path.exists()
    assert (published.path / "terminal-retired.json").is_file()
    assert store.pending_attempts("omi_cv1") == ()
    assert store.pending_attempts("other") == (unrelated.descriptor,)


def test_direct_quarantine_function_uses_concrete_filesystem(tmp_path: Path) -> None:
    store = StagingStore(tmp_path, _capture_root(tmp_path))
    matching = store.prepare_streaming_attempt("omi_cv1", 100, 1)

    moved = quarantine_module.quarantine_pending(store._filesystem, "omi_cv1", "direct call")

    assert len(moved) == 1
    assert moved[0].parent == tmp_path / "quarantine" / "omi_cv1"
    assert not matching.path.exists()


def test_quarantine_a_does_not_move_unattributed_entry_during_b_lease(tmp_path: Path) -> None:
    store = StagingStore(tmp_path, _capture_root(tmp_path))
    matching = store.prepare_streaming_attempt("omi_cv1", 100, 2)
    malformed = tmp_path / "attempts" / ("f" * 32)
    malformed.mkdir()
    before = malformed / "evidence"
    before.write_bytes(b"unattributed")

    with store.device_lock("other"), pytest.raises(DeviceAlreadyRunningError):
        store.quarantine_pending("omi_cv1", "race recovery")

    assert matching.path.exists()
    assert malformed.is_dir()
    assert before.read_bytes() == b"unattributed"


def test_quarantine_pending_moves_symlink_without_following_it(tmp_path: Path) -> None:
    store = StagingStore(tmp_path, _capture_root(tmp_path))
    (tmp_path / "attempts").mkdir()
    outside = tmp_path / "outside"
    outside.mkdir()
    target = outside / "evidence"
    target.write_bytes(b"evidence")
    link = tmp_path / "attempts" / "link"
    link.symlink_to(target)

    moved = store.quarantine_pending("omi_cv1", "symlink recovery")

    assert len(moved) == 1
    assert moved[0].is_symlink()
    assert moved[0].readlink() == target
    assert not link.is_symlink()
    assert target.read_bytes() == b"evidence"


def test_opaque_quarantine_entries_expire_after_terminal_retention(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    now = 1_000_000_000
    monkeypatch.setattr(quarantine_module, "_wall_clock_ns", lambda: now)
    store = StagingStore(
        tmp_path,
        _capture_root(tmp_path),
        config=CollectorConfig(staging_retention=StagingRetentionConfig(terminal_retention_seconds=72.0)),
    )
    opaque_dir = tmp_path / "attempts" / "opaque-dir"
    opaque_dir.mkdir(parents=True)
    target = tmp_path / "outside"
    target.write_bytes(b"preserve")
    opaque_link = tmp_path / "attempts" / "opaque-link"
    opaque_link.symlink_to(target)

    moved = store.quarantine_pending("omi_cv1", "opaque evidence")
    now += 72_000_000_000

    assert set(store.sweep_terminal_quarantine("omi_cv1")) == set(moved)
    assert not opaque_dir.exists()
    assert not opaque_link.is_symlink()
    assert target.read_bytes() == b"preserve"


def test_quarantine_pending_rejects_active_lease(tmp_path: Path) -> None:
    store = StagingStore(tmp_path, _capture_root(tmp_path))

    with store.device_lock("omi_cv1"), pytest.raises(DeviceAlreadyRunningError):
        store.quarantine_pending("omi_cv1", "manual recovery")


def test_pending_checks_empty_and_rejects_malformed_roots(tmp_path: Path) -> None:
    store = StagingStore(tmp_path, _capture_root(tmp_path))
    store.assert_no_pending("omi_cv1")

    partial = tmp_path / "attempts"
    partial.write_text("not a directory", encoding="utf-8")
    with pytest.raises(PendingAttemptError, match="not a directory"):
        store.pending_attempts("omi_cv1")
    partial.unlink()
    target = tmp_path / "partial-target"
    target.mkdir()
    partial.symlink_to(target, target_is_directory=True)
    with pytest.raises(PendingAttemptError, match="not a directory"):
        store.pending_attempts("omi_cv1")


@pytest.mark.parametrize("root_kind", ["file", "symlink"])
def test_quarantine_pending_moves_unsafe_partial_root_and_recreates_it(tmp_path: Path, root_kind: str) -> None:
    store = StagingStore(tmp_path, _capture_root(tmp_path))
    partial = tmp_path / "attempts"
    target = tmp_path / "outside"
    if root_kind == "file":
        partial.write_bytes(b"unsafe root")
    else:
        target.mkdir()
        (target / "evidence").write_bytes(b"keep me")
        partial.symlink_to(target, target_is_directory=True)

    if root_kind == "symlink":
        with pytest.raises(StagingError, match="attempts root must not be a symlink"):
            store.quarantine_pending("omi_cv1", "root recovery")
        return

    moved = store.quarantine_pending("omi_cv1", "root recovery")

    assert len(moved) == 1
    assert moved[0].name.startswith("attempts-")
    assert moved[0].is_symlink() is (root_kind == "symlink")
    if root_kind == "file":
        assert moved[0].read_bytes() == b"unsafe root"
    else:
        assert moved[0].readlink() == target
        assert (target / "evidence").read_bytes() == b"keep me"
    assert partial.is_dir()
    assert not partial.is_symlink()
    assert tuple(partial.iterdir()) == ()
    assert store.pending_attempts("omi_cv1") == ()


def test_pending_sealed_looking_partial_requires_the_real_destination_bundle(tmp_path: Path) -> None:
    attempt = _started_streaming_attempt(tmp_path, count=1)
    attempt.append_record(0, 100, _record(1))
    attempt.checkpoint()
    raw_hash = sha256(_record(1)).hexdigest()
    (attempt.path / "manifest.json").write_text(dumps(attempt._manifest(raw_hash)), encoding="utf-8")
    (attempt.path / "receipt.json").write_text(dumps(attempt._receipt(raw_hash)), encoding="utf-8")

    with pytest.raises(PendingAttemptError):
        StagingStore(tmp_path, _capture_root(tmp_path)).assert_no_pending("omi_cv1")


def test_pending_partial_with_a_corrupt_destination_bundle_remains_blocking(tmp_path: Path) -> None:
    attempt = _started_streaming_attempt(tmp_path, count=1)
    attempt.append_record(0, 100, _record(1))
    attempt.checkpoint()
    raw_hash = sha256(_record(1)).hexdigest()
    destination = attempt._bundle_path(raw_hash)
    destination.mkdir(parents=True)
    (destination / "records.bin").write_bytes(_record(9))
    (destination / "manifest.json").write_text(dumps(attempt._manifest(raw_hash)), encoding="utf-8")
    (destination / "receipt.json").write_text(dumps(attempt._receipt(raw_hash)), encoding="utf-8")
    (attempt.path / "manifest.json").write_text(dumps(attempt._manifest(raw_hash)), encoding="utf-8")
    (attempt.path / "receipt.json").write_text(dumps(attempt._receipt(raw_hash)), encoding="utf-8")

    with pytest.raises(PendingAttemptError):
        StagingStore(tmp_path, _capture_root(tmp_path)).assert_no_pending("omi_cv1")


def test_device_lock_is_exclusive_and_released_after_an_error(tmp_path: Path) -> None:
    store = StagingStore(tmp_path, _capture_root(tmp_path))
    with store.device_lock("omi_cv1"), pytest.raises(DeviceAlreadyRunningError), store.device_lock("omi_cv1"):
        pass
    with store.device_lock("omi_cv1"):
        pass


def test_device_lock_rejects_forged_cross_store_expired_and_reused_leases(tmp_path: Path) -> None:
    store = StagingStore(tmp_path, _capture_root(tmp_path))
    other = StagingStore(tmp_path / "other", tmp_path / "other-captures")
    forged = DeviceLock(store._filesystem, "omi_cv1")
    cross_store = DeviceLock(other._filesystem, "omi_cv1")

    with pytest.raises(AttemptStateError, match="active spool lock"):
        forged.require_active()
    with store.device_lock("omi_cv1") as active:
        active.require_active()
        with pytest.raises(AttemptStateError, match="active spool lock"):
            forged.require_active()
        with pytest.raises(AttemptStateError, match="active spool lock"):
            cross_store.require_active()
    with pytest.raises(AttemptStateError, match="active spool lock"):
        active.require_active()


def test_pending_ignores_non_directory_entries_and_reports_iterdir_errors(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = StagingStore(tmp_path, _capture_root(tmp_path))
    partial = tmp_path / "attempts"
    partial.mkdir()
    (partial / "evidence.txt").write_text("preserve", encoding="utf-8")
    with pytest.raises(PendingAttemptError, match="malformed partial attempt evidence"):
        store.pending_attempts("omi_cv1")

    entry = partial / "linked-evidence"
    entry_target = tmp_path / "entry-target"
    entry_target.mkdir()
    entry.symlink_to(entry_target, target_is_directory=True)
    with pytest.raises(PendingAttemptError, match="malformed partial attempt evidence"):
        store.pending_attempts("omi_cv1")
    moved = store.quarantine_pending("omi_cv1", "opaque local evidence")
    assert len(moved) == 2
    assert all(path.parent == tmp_path / "quarantine" / "omi_cv1" for path in moved)
    assert all(not path.is_symlink() for path in partial.iterdir())
    assert entry_target.is_dir()

    original_iterdir = Path.iterdir

    def fail_iterdir(path: Path):
        if path == partial:
            raise OSError("simulated directory listing failure")
        return original_iterdir(path)

    monkeypatch.setattr(Path, "iterdir", fail_iterdir)
    with pytest.raises(PendingAttemptError, match="cannot be inspected"):
        store.pending_attempts("omi_cv1")
