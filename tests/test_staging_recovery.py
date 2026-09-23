"""Focused recovery staging ownership tests."""

from __future__ import annotations

from collections.abc import Callable
from errno import EXDEV
from hashlib import sha256
from json import dumps, loads
from os import PathLike, fsync
from pathlib import Path
from shutil import rmtree
from threading import Barrier, Thread
from typing import cast

import pytest

from omi_collector.capture.adapters import quarantine, staging_filesystem
from omi_collector.capture.adapters.attempts import (
    RecordDisposition,
    RecordGapError,
    RecordMismatchError,
    RecordRegressionError,
)
from omi_collector.capture.adapters.bundle_contract import BundleManifest, SealedReceipt
from omi_collector.capture.adapters.clock_corrections import ClockCorrectionStore
from omi_collector.capture.adapters.staging_contract import AttemptStateError, DeviceAlreadyRunningError
from omi_collector.capture.adapters.staging_store import StagingStore
from omi_collector.capture.adapters.timeline_generations import GenerationResult, build_generation
from omi_collector.capture.domain.ring_protocol import RECORD_SIZE, ReadBeginNotification
from omi_collector.config import CollectorConfig, StagingRetentionConfig
from omi_collector.spool_metrics import collect_spool_metrics

_CAPTURE_ROOTS: set[Path] = set()
_ACCEPTANCE_FIRST_BOUNDARY = 7_192_026
_ACCEPTANCE_SECOND_BOUNDARY = 7_717_545
_ACCEPTANCE_FRONTIER = 7_763_451


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


def _one_record_bundle(root: Path, sequence: int, timestamp: int) -> Path:
    raw = timestamp.to_bytes(4, "big") + b"x" * (RECORD_SIZE - 4)
    digest = sha256(raw).hexdigest()
    bundle = root / f"{sequence}-{sequence + 1}-{digest[:16]}"
    bundle.mkdir(parents=True)
    (bundle / "records.bin").write_bytes(raw)
    (bundle / "manifest.json").write_text(
        dumps(BundleManifest(2, sequence, sequence + 1, 1, RECORD_SIZE, digest).as_dict()), encoding="utf-8"
    )
    (bundle / "receipt.json").write_text(dumps(SealedReceipt("a" * 32, digest).as_dict()), encoding="utf-8")
    return bundle


def _acceptance_sequences() -> tuple[int, ...]:
    before = [
        _ACCEPTANCE_FIRST_BOUNDARY + (_ACCEPTANCE_SECOND_BOUNDARY - _ACCEPTANCE_FIRST_BOUNDARY - 1) * index // 42
        for index in range(43)
    ]
    after = [
        _ACCEPTANCE_SECOND_BOUNDARY + (_ACCEPTANCE_FRONTIER - _ACCEPTANCE_SECOND_BOUNDARY) * index // 28
        for index in range(29)
    ]
    return tuple(before + after)


def _bundle_bytes(root: Path) -> dict[str, bytes]:
    return {bundle.name: (bundle / "records.bin").read_bytes() for bundle in root.iterdir() if bundle.is_dir()}


def _seed_acceptance_clock_state(tmp_path: Path) -> ClockCorrectionStore:
    corrections = ClockCorrectionStore(tmp_path / "device.json")
    first_operation = corrections.mark_unresolved(corrections.prepare(43, 72, -29.0, _ACCEPTANCE_FIRST_BOUNDARY))
    corrections.finish(
        first_operation, state="applied", boundary_sequence_max=_ACCEPTANCE_SECOND_BOUNDARY, verified_epoch=72
    )
    second_id = "b" * 32
    (tmp_path / "clock-corrections").mkdir(parents=True, exist_ok=True)
    (tmp_path / "clock-corrections" / f"{second_id}.json").write_text(
        dumps(
            {
                "version": 2,
                "operation_id": second_id,
                "state": "prepared",
                "observed_epoch": 43,
                "target_epoch": 72,
                "drift_seconds": -29.0,
                "boundary_sequence_min": _ACCEPTANCE_SECOND_BOUNDARY,
                "boundary_sequence_max": None,
                "verified_epoch": None,
            }
        ),
        encoding="utf-8",
    )
    second_operation = corrections.mark_unresolved(
        corrections.prepare(43, 72, -29.0, _ACCEPTANCE_SECOND_BOUNDARY, operation_id=second_id)
    )
    initial = corrections.observation_store.native_trusted(
        session_id="restart-session",
        host_boot_id="boot",
        host_realtime_start=43.0,
        host_realtime_end=43.0,
        host_monotonic_start=1.0,
        host_monotonic_end=1.0,
        device_epoch=43,
        info_sequence_min=_ACCEPTANCE_SECOND_BOUNDARY,
        info_sequence_max=_ACCEPTANCE_SECOND_BOUNDARY,
        operation_id=second_operation.operation_id,
        observation_role="initial",
    )
    corrections.observation_store.native_trusted(
        session_id="restart-session",
        host_boot_id="boot",
        host_realtime_start=72.0,
        host_realtime_end=72.0,
        host_monotonic_start=2.0,
        host_monotonic_end=2.0,
        device_epoch=72,
        info_sequence_min=_ACCEPTANCE_SECOND_BOUNDARY,
        info_sequence_max=_ACCEPTANCE_FRONTIER,
        operation_id=second_operation.operation_id,
        effective_boundary_sequence=_ACCEPTANCE_SECOND_BOUNDARY,
        observation_role="later",
        parent_observation_id=initial.observation_id,
    )
    return corrections


def _started_attempt(tmp_path: Path, *, count: int = 2):
    attempt = StagingStore(tmp_path, _capture_root(tmp_path)).prepare_streaming_attempt(100, count)
    attempt.record_read_begin(ReadBeginNotification(100, count))
    return attempt


def _started_streaming_attempt(tmp_path: Path, *, count: int = 2, fsync_fn: Callable[[int], None] = fsync):
    attempt = StagingStore(tmp_path, _capture_root(tmp_path), fsync_fn=fsync_fn).prepare_streaming_attempt(100, count)
    attempt.record_read_begin(ReadBeginNotification(100, count))
    return attempt


def test_startup_reconciles_native_clock_evidence_without_captured_bundles(tmp_path: Path) -> None:
    store = StagingStore.from_paths(
        StagingStore(tmp_path, _capture_root(tmp_path)).paths,
        publication_root=tmp_path / "published",
    )
    correction_store = ClockCorrectionStore(store.device_state_path)
    correction = correction_store.mark_unresolved(correction_store.prepare(1302, 1002, 300.0, 7717545))
    initial = correction_store.observation_store.native_trusted(
        session_id="session",
        host_boot_id="boot",
        host_realtime_start=1000.0,
        host_realtime_end=1000.0,
        host_monotonic_start=1.0,
        host_monotonic_end=1.0,
        device_epoch=1302,
        info_sequence_min=7717545,
        info_sequence_max=7717545,
        operation_id=correction.operation_id,
        observation_role="initial",
    )
    correction_store.observation_store.native_trusted(
        session_id="session",
        host_boot_id="boot",
        host_realtime_start=1002.0,
        host_realtime_end=1002.0,
        host_monotonic_start=2.0,
        host_monotonic_end=2.0,
        device_epoch=1002,
        info_sequence_min=7717545,
        info_sequence_max=7717545,
        operation_id=correction.operation_id,
        effective_boundary_sequence=7717545,
        observation_role="later",
        parent_observation_id=initial.observation_id,
    )

    result = store.recover_and_publish()

    assert result is None
    assert correction_store.records()[0].state == "applied"
    assert tuple(store.capture_root.iterdir()) == ()


def test_restart_hands_43_published_to_72_captured_bundles_without_ble(tmp_path: Path) -> None:
    captured = _capture_root(tmp_path)
    published = tmp_path / "published"
    sequences = _acceptance_sequences()
    for sequence in sequences:
        _one_record_bundle(captured, sequence, 43 if sequence < _ACCEPTANCE_SECOND_BOUNDARY else 72)
    old_generation = build_generation(captured, published, (), max_sequence=sequences[43])
    assert old_generation.bundle_count == 43
    raw_before = _bundle_bytes(captured)
    corrections = _seed_acceptance_clock_state(tmp_path)
    store = StagingStore.from_paths(
        StagingStore(tmp_path, captured).paths,
        publication_root=published,
    )

    result = store.recover_and_publish()

    assert isinstance(result, GenerationResult)
    assert result.bundle_count == 72
    assert result.record_count == 72
    assert (published / "current").resolve() == result.path
    current_bundles = tuple(
        path for path in (published / "current").iterdir() if path.is_dir() and (path / "manifest.json").is_file()
    )
    assert len(current_bundles) == 72
    assert _bundle_bytes(captured) == raw_before
    assert sorted(item.boundary_sequence_min for item in corrections.records()) == [
        _ACCEPTANCE_FIRST_BOUNDARY,
        _ACCEPTANCE_SECOND_BOUNDARY,
    ]
    assert all(item.state == "resolved" for item in corrections.records())
    later = next(item for item in corrections.observation_store.records() if item.observation_role == "later")
    assert later.info_sequence_max == _ACCEPTANCE_FRONTIER
    assert loads((published / "current" / "generation.json").read_text(encoding="utf-8"))["record_count"] == 72
    second_result = store.recover_and_publish()
    assert isinstance(second_result, GenerationResult)
    assert second_result.generation_id == result.generation_id
    assert collect_spool_metrics(published).current_window.bundle_count == 72


def _publication_store(tmp_path: Path) -> StagingStore:
    capture_root = _capture_root(tmp_path)
    _one_record_bundle(capture_root, 100, 1)
    return StagingStore.from_paths(StagingStore(tmp_path, capture_root).paths, publication_root=tmp_path / "published")


def test_background_thread_authority_does_not_borrow_an_unrelated_active_lease(tmp_path: Path) -> None:
    store = _publication_store(tmp_path)
    authority = store.create_publication_authority()
    barrier = Barrier(2)
    observed: dict[str, object] = {}

    with store.device_lock(recover_capture_temporaries=False):

        def contend() -> None:
            barrier.wait()
            try:
                authority.publish()
                observed["publication"] = "borrowed"
            except DeviceAlreadyRunningError:
                observed["publication"] = "rejected"
            try:
                with store.device_lock(recover_capture_temporaries=False):
                    observed["lock"] = "borrowed"
            except DeviceAlreadyRunningError:
                observed["lock"] = "contended"

        thread = Thread(target=contend)
        thread.start()
        barrier.wait()
        thread.join()

    authority.close()
    assert observed == {"publication": "rejected", "lock": "contended"}


def test_authority_issued_during_foreign_lease_does_not_adopt_it(tmp_path: Path) -> None:
    store = _publication_store(tmp_path)
    barrier = Barrier(2)

    def hold_foreign_lease() -> None:
        with store.device_lock(recover_capture_temporaries=False):
            barrier.wait()
            barrier.wait()

    thread = Thread(target=hold_foreign_lease)
    thread.start()
    barrier.wait()
    authority = store.create_publication_authority()
    try:
        with pytest.raises(DeviceAlreadyRunningError):
            authority.publish()
    finally:
        authority.close()
        barrier.wait()
        thread.join()


def test_preissued_authority_rejects_foreign_active_lease(tmp_path: Path) -> None:
    store = _publication_store(tmp_path)
    authority = store.create_publication_authority()
    barrier = Barrier(2)

    def hold_foreign_lease() -> None:
        with store.device_lock(recover_capture_temporaries=False):
            barrier.wait()
            barrier.wait()

    thread = Thread(target=hold_foreign_lease)
    thread.start()
    barrier.wait()
    try:
        with pytest.raises(DeviceAlreadyRunningError):
            authority.publish()
    finally:
        authority.close()
        barrier.wait()
        thread.join()


def test_authority_can_acquire_two_sequential_publication_leases(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = _publication_store(tmp_path)
    authority = store.create_publication_authority()
    results = iter((object(), object()))
    monkeypatch.setattr(store, "_recover_and_publish_unlocked", lambda: next(results))

    first = authority.publish()
    second = authority.publish()

    assert first is not second
    authority.close()


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


def test_terminal_retired_recognition_requires_checkpoint(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    now = 1_000_000_000

    def wall_clock_ns() -> int:
        return now

    monkeypatch.setattr(quarantine, "_wall_clock_ns", wall_clock_ns)
    store = StagingStore(
        tmp_path,
        _capture_root(tmp_path),
        config=CollectorConfig(staging_retention=StagingRetentionConfig(terminal_retention_seconds=1.0)),
    )
    attempt = store.prepare_streaming_attempt(100, 1)
    attempt.record_read_begin(ReadBeginNotification(100, 1))
    attempt.append_record(0, 100, _record(1))
    attempt.checkpoint()
    assert attempt.publish_prefix() is not None
    attempt.close(durable=True)
    store.terminalize_prefix_attempt(attempt.attempt_id)

    (attempt.path / "checkpoint.json").unlink()

    now += 1_000_000_000
    assert store.pending_attempts() == ()
    assert store.sweep_terminal_retired() == (attempt.path,)
    assert not attempt.path.exists()


def test_malformed_terminal_retired_marker_blocks_admission(tmp_path: Path) -> None:
    store = StagingStore(tmp_path, _capture_root(tmp_path))
    attempt = store.prepare_streaming_attempt(100, 1)
    attempt.record_read_begin(ReadBeginNotification(100, 1))
    attempt.append_record(0, 100, _record(1))
    attempt.checkpoint()
    assert attempt.publish_prefix() is not None
    attempt.close(durable=True)
    store.terminalize_prefix_attempt(attempt.attempt_id)

    (attempt.path / "terminal-retired.json").write_text("{}", encoding="utf-8")

    assert store.pending_attempts() == (attempt.descriptor,)


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


def test_recovery_accepts_replayed_durable_record(tmp_path: Path) -> None:
    attempt = _started_streaming_attempt(tmp_path, count=2)
    attempt.append_record(0, 100, _record(1))
    attempt.checkpoint()
    reopened = attempt
    reopened.begin_recovery(100, 2)

    assert reopened.accept_record(100, _record(1)) is RecordDisposition.REPLAYED


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
    with pytest.raises(AttemptStateError, match=r"record aligned|checkpoint hash|preserved"):
        StagingStore(tmp_path, _capture_root(tmp_path)).open_attempt(attempt.attempt_id).append_record(
            0, 100, _record(1)
        )
