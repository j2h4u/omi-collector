from __future__ import annotations

import json
import os
from hashlib import sha256
from pathlib import Path
from stat import S_IMODE
from types import SimpleNamespace

import pytest

from omi_collector.capture.adapters import timeline_generations
from omi_collector.capture.adapters.bundle_contract import BundleManifest, SealedReceipt
from omi_collector.capture.adapters.clock_corrections import ClockCorrectionStore
from omi_collector.capture.adapters.timeline_generations import (
    TimelineGenerationError,
    TimeRepair,
    build_generation,
    publish_from_ledger,
)
from omi_collector.capture.domain.ring_protocol import RECORD_SIZE


def _record(timestamp: int, fill: int) -> bytes:
    return timestamp.to_bytes(4, "big") + bytes((fill,)) * (RECORD_SIZE - 4)


def _bundle(root: Path, start: int, timestamps: tuple[int, ...], attempt: str) -> None:
    raw = b"".join(_record(timestamp, index + 1) for index, timestamp in enumerate(timestamps))
    digest = sha256(raw).hexdigest()
    path = root / f"{start}-{start + len(timestamps)}-{digest[:16]}"
    path.mkdir(parents=True)
    manifest = BundleManifest(2, start, start + len(timestamps), len(timestamps), RECORD_SIZE, digest)
    (path / "records.bin").write_bytes(raw)
    (path / "manifest.json").write_text(json.dumps(manifest.as_dict()))
    (path / "receipt.json").write_text(json.dumps(SealedReceipt(attempt, digest).as_dict()))


def test_generation_repairs_epoch_and_atomically_exposes_ordinary_bundles(tmp_path: Path) -> None:
    captured = tmp_path / "captured"
    published = tmp_path / "source"
    _bundle(captured, 10, (1300, 1301), "a" * 32)
    _bundle(captured, 12, (1002, 1003), "b" * 32)

    result = build_generation(captured, published, (TimeRepair(10, 12, 300, "clock-op"),))

    assert (published / "current").is_symlink()
    assert (published / "current").resolve() == result.path
    bundles = sorted(path for path in (published / "current").iterdir() if path.is_dir())
    timestamps = []
    for bundle in bundles:
        raw = (bundle / "records.bin").read_bytes()
        timestamps.extend(int.from_bytes(raw[index : index + 4], "big") for index in range(0, len(raw), RECORD_SIZE))
    assert timestamps == [1000, 1001, 1002, 1003]


def test_root_recovery_assigns_service_identity_before_service_append(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    captured = tmp_path / "captured"
    published = tmp_path / "source"
    published.mkdir(mode=0o750)
    _bundle(captured, 10, (1000,), "a" * 32)

    service_uid, service_gid = 4242, 4343
    chown_calls: list[tuple[int, int]] = []

    monkeypatch.setattr(timeline_generations.os, "geteuid", lambda: 0)
    monkeypatch.setattr(
        timeline_generations.pwd,
        "getpwnam",
        lambda _name: SimpleNamespace(pw_uid=service_uid, pw_gid=service_gid),
    )
    monkeypatch.setattr(
        timeline_generations.grp,
        "getgrnam",
        lambda _name: SimpleNamespace(gr_gid=service_gid),
    )

    def record_chown(_descriptor: int, uid: int, gid: int) -> None:
        chown_calls.append((uid, gid))

    monkeypatch.setattr(timeline_generations.os, "fchown", record_chown)

    result = build_generation(captured, published, ())

    generations = published / ".generations"
    assert S_IMODE(generations.stat().st_mode) == 0o750
    assert S_IMODE(result.path.stat().st_mode) == 0o750
    assert chown_calls
    assert all((uid, gid) == (service_uid, service_gid) for uid, gid in chown_calls)

    _bundle(captured, 11, (1001,), "b" * 32)
    appended = build_generation(captured, published, ())

    assert appended.path == result.path
    assert appended.bundle_count == 2
    assert appended.record_count == 2
    assert len(chown_calls) >= 5
    assert S_IMODE(result.path.stat().st_mode) == 0o750


def test_root_recovery_tree_is_service_readable_for_later_append(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    captured = tmp_path / "captured"
    published = tmp_path / "source"
    service_uid, service_gid = os.getuid(), os.getgid()
    _bundle(captured, 10, (1000,), "a" * 32)

    monkeypatch.setattr(timeline_generations.os, "geteuid", lambda: 0)
    monkeypatch.setattr(
        timeline_generations.pwd,
        "getpwnam",
        lambda _name: SimpleNamespace(pw_uid=service_uid, pw_gid=service_gid),
    )
    monkeypatch.setattr(
        timeline_generations.grp,
        "getgrnam",
        lambda _name: SimpleNamespace(gr_gid=service_gid),
    )

    result = build_generation(captured, published, ())
    bundle = next(path for path in result.path.iterdir() if path.is_dir())
    assert (result.path.stat().st_uid, result.path.stat().st_gid) == (service_uid, service_gid)
    assert S_IMODE(bundle.stat().st_mode) == 0o750
    assert (bundle.stat().st_uid, bundle.stat().st_gid) == (service_uid, service_gid)
    for path in (*bundle.iterdir(), result.path / "generation.json"):
        assert S_IMODE(path.stat().st_mode) == 0o640
        assert (path.stat().st_uid, path.stat().st_gid) == (service_uid, service_gid)
    assert os.access(result.path / "generation.json", os.R_OK)

    _bundle(captured, 11, (1001,), "b" * 32)
    appended = build_generation(captured, published, ())

    assert appended.path == result.path
    assert appended.bundle_count == 2
    assert appended.record_count == 2
    assert os.access(appended.path / "generation.json", os.R_OK)


def test_existing_generation_recovery_repairs_preexisting_bundle_tree_for_service_append(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    captured = tmp_path / "captured"
    published = tmp_path / "source"
    service_uid, service_gid = os.getuid(), os.getgid()
    _bundle(captured, 10, (1000,), "a" * 32)
    initial = build_generation(captured, published, ())
    initial.path.chmod(0o700)
    (initial.path / "generation.json").chmod(0o600)
    first_bundle = next(path for path in initial.path.iterdir() if path.is_dir())
    first_bundle.chmod(0o700)
    for path in first_bundle.iterdir():
        path.chmod(0o600)

    monkeypatch.setattr(timeline_generations.os, "geteuid", lambda: 0)
    monkeypatch.setattr(
        timeline_generations.pwd,
        "getpwnam",
        lambda _name: SimpleNamespace(pw_uid=service_uid, pw_gid=service_gid),
    )
    monkeypatch.setattr(
        timeline_generations.grp,
        "getgrnam",
        lambda _name: SimpleNamespace(gr_gid=service_gid),
    )
    _bundle(captured, 11, (1001,), "b" * 32)

    appended = build_generation(captured, published, ())

    assert appended.bundle_count == 2
    assert appended.record_count == 2
    assert S_IMODE(appended.path.stat().st_mode) == 0o750
    assert S_IMODE((appended.path / "generation.json").stat().st_mode) == 0o640
    for bundle in appended.path.iterdir():
        if bundle.is_dir():
            assert S_IMODE(bundle.stat().st_mode) == 0o750
            assert os.access(bundle / "records.bin", os.R_OK)
            for artifact in bundle.iterdir():
                assert S_IMODE(artifact.stat().st_mode) == 0o640


def test_generation_repair_rejects_symlinked_generations_without_mutating_target(tmp_path: Path) -> None:
    captured = tmp_path / "captured"
    published = tmp_path / "source"
    sentinel = tmp_path / "sentinel"
    _bundle(captured, 10, (1000,), "a" * 32)
    published.mkdir(mode=0o750)
    sentinel.mkdir(mode=0o711)
    before = sentinel.stat()
    (published / ".generations").symlink_to(sentinel, target_is_directory=True)

    with pytest.raises(TimelineGenerationError, match="service-writable"):
        build_generation(captured, published, ())

    after = sentinel.stat()
    assert S_IMODE(after.st_mode) == S_IMODE(before.st_mode)
    assert (after.st_uid, after.st_gid) == (before.st_uid, before.st_gid)


def test_existing_generation_repair_rejects_symlink_without_mutating_target(tmp_path: Path) -> None:
    captured = tmp_path / "captured"
    published = tmp_path / "source"
    sentinel = tmp_path / "sentinel"
    _bundle(captured, 10, (1000,), "a" * 32)
    initial = build_generation(captured, published, ())
    initial.path.rename(sentinel)
    sentinel.chmod(0o711)
    before = sentinel.stat()
    initial.path.symlink_to(sentinel, target_is_directory=True)

    with pytest.raises(TimelineGenerationError, match="service-writable"):
        build_generation(captured, published, ())

    after = sentinel.stat()
    assert S_IMODE(after.st_mode) == S_IMODE(before.st_mode)
    assert (after.st_uid, after.st_gid) == (before.st_uid, before.st_gid)


def test_post_repair_generations_swap_cannot_escape_to_sentinel(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    captured = tmp_path / "captured"
    published = tmp_path / "source"
    sentinel = tmp_path / "sentinel"
    displaced = tmp_path / "generations-original"
    _bundle(captured, 10, (1000,), "a" * 32)
    sentinel.mkdir(mode=0o711)
    before = sentinel.stat()

    real_prepare = timeline_generations._prepare_generation_directory
    swapped = False

    def swap_after_repair(path: Path, service_uid: int, service_gid: int) -> None:
        nonlocal swapped
        real_prepare(path, service_uid, service_gid)
        if not swapped and path.name.startswith(".") and path.name.endswith(".tmp"):
            swapped = True
            (published / ".generations").rename(displaced)
            (published / ".generations").symlink_to(sentinel, target_is_directory=True)

    monkeypatch.setattr(timeline_generations, "_prepare_generation_directory", swap_after_repair)

    with pytest.raises(TimelineGenerationError, match="was replaced"):
        build_generation(captured, published, ())

    after = sentinel.stat()
    assert swapped
    assert S_IMODE(after.st_mode) == S_IMODE(before.st_mode)
    assert (after.st_uid, after.st_gid) == (before.st_uid, before.st_gid)
    assert not (sentinel / "current").exists()
    assert tuple(sentinel.iterdir()) == ()


def test_inner_bundle_temp_swap_cannot_write_or_publish_to_sentinel(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    captured = tmp_path / "captured"
    published = tmp_path / "source"
    sentinel = tmp_path / "sentinel"
    displaced = tmp_path / "inner-original"
    _bundle(captured, 10, (1000,), "a" * 32)
    sentinel.mkdir(mode=0o711)
    before = sentinel.stat()

    real_open_at = timeline_generations._open_directory_at
    swapped = False

    def swap_after_inner_temp_open(parent: int, name: str) -> int:
        nonlocal swapped
        descriptor = real_open_at(parent, name)
        if not swapped and name.startswith(".") and name.endswith(".tmp"):
            swapped = True
            temporary = timeline_generations._descriptor_path(parent) / name
            temporary.rename(displaced)
            temporary.symlink_to(sentinel, target_is_directory=True)
        return descriptor

    monkeypatch.setattr(timeline_generations, "_open_directory_at", swap_after_inner_temp_open)

    with pytest.raises(TimelineGenerationError, match="bundle temporary directory was replaced"):
        build_generation(captured, published, ())

    after = sentinel.stat()
    assert swapped
    assert S_IMODE(after.st_mode) == S_IMODE(before.st_mode)
    assert (after.st_uid, after.st_gid) == (before.st_uid, before.st_gid)
    assert tuple(sentinel.iterdir()) == ()
    assert not (published / "current").exists()


def test_generation_never_exposes_a_regressing_candidate(tmp_path: Path) -> None:
    captured = tmp_path / "captured"
    published = tmp_path / "source"
    _bundle(captured, 10, (1000, 999), "a" * 32)

    with pytest.raises(TimelineGenerationError, match="regresses"):
        build_generation(captured, published, ())

    assert not (published / "current").exists()


def test_ledger_publishes_only_settled_clock_evidence(tmp_path: Path) -> None:
    captured = tmp_path / "captured"
    published = tmp_path / "source"
    collector = tmp_path / "collector"
    _bundle(captured, 10, (1300, 1301), "a" * 32)
    _bundle(captured, 12, (1002, 1003), "b" * 32)
    collector.mkdir()
    (collector / "timeline-repairs.json").write_text(
        json.dumps(
            {
                "version": 1,
                "repairs": [
                    {
                        "start_sequence": 10,
                        "next_sequence": 12,
                        "offset_seconds": 300,
                        "evidence": "clock-op",
                    }
                ],
            }
        )
    )

    result = publish_from_ledger(captured, published, collector)
    assert result.record_count == 4

    evidence = collector / "clock-corrections/op.json"
    evidence.parent.mkdir(parents=True)
    evidence.write_text(json.dumps({"state": "unresolved"}))
    with pytest.raises(TimelineGenerationError, match="unresolved"):
        publish_from_ledger(captured, published, collector)


@pytest.mark.parametrize(
    "ledger",
    [
        {"repairs": []},
        {"version": 2, "repairs": []},
        {"version": 1.0, "repairs": []},
        {"version": 1, "repairs": [], "extra": True},
    ],
)
def test_timeline_repair_reader_accepts_only_canonical_v1_schema(tmp_path: Path, ledger: dict[str, object]) -> None:
    captured = tmp_path / "captured"
    published = tmp_path / "source"
    collector = tmp_path / "collector"
    _bundle(captured, 10, (1000,), "a" * 32)
    collector.mkdir()
    (collector / "timeline-repairs.json").write_text(json.dumps(ledger))

    with pytest.raises(TimelineGenerationError, match="ledger is invalid"):
        publish_from_ledger(captured, published, collector)


def test_applied_clock_operation_resolves_after_raw_interval_is_monotonic(tmp_path: Path) -> None:
    captured = tmp_path / "captured"
    published = tmp_path / "source"
    collector = tmp_path / "collector"
    _bundle(captured, 10, (1000, 1001, 1302, 1303, 1004), "a" * 32)
    collector.mkdir()
    (collector / "timeline-repairs.json").write_text(
        json.dumps(
            {
                "version": 1,
                "repairs": [
                    {
                        "start_sequence": 12,
                        "next_sequence": 14,
                        "offset_seconds": 300,
                        "evidence": "clock-op",
                    }
                ],
            }
        )
    )
    store = ClockCorrectionStore(collector / "device.json")
    intent = store.mark_unresolved(store.prepare(1302, 1002, 300.0, 12))
    store.finish(intent, state="applied", boundary_sequence_max=14, verified_epoch=1002)
    operation_path = collector / "clock-corrections" / f"{intent.operation_id}.json"

    result = publish_from_ledger(captured, published, collector)

    assert result.record_count == 5
    assert json.loads(operation_path.read_text())["state"] == "resolved"


def test_applied_operation_resolves_across_legitimate_sequence_gaps(tmp_path: Path) -> None:
    captured = tmp_path / "captured"
    published = tmp_path / "source"
    collector = tmp_path / "collector"
    _bundle(captured, 10, (1000, 1001), "a" * 32)
    _bundle(captured, 13, (1302, 1303), "b" * 32)
    _bundle(captured, 15, (1004,), "c" * 32)
    collector.mkdir()
    (collector / "timeline-repairs.json").write_text(
        json.dumps(
            {
                "version": 1,
                "repairs": [
                    {
                        "start_sequence": 13,
                        "next_sequence": 15,
                        "offset_seconds": 300,
                        "evidence": "clock-op",
                    }
                ],
            }
        )
    )
    store = ClockCorrectionStore(collector / "device.json")
    intent = store.mark_unresolved(store.prepare(1302, 1002, 300.0, 12))
    store.finish(intent, state="applied", boundary_sequence_max=15, verified_epoch=1002)
    operation_path = collector / "clock-corrections" / f"{intent.operation_id}.json"

    result = publish_from_ledger(captured, published, collector)

    assert result.record_count == 5
    assert json.loads(operation_path.read_text())["state"] == "resolved"


def test_incident_boundaries_resolve_on_sparse_capture_frontier(tmp_path: Path) -> None:
    captured = tmp_path / "captured"
    published = tmp_path / "source"
    collector = tmp_path / "collector"
    _bundle(captured, 7192018, (1000, 1001), "a" * 32)
    _bundle(captured, 7717546, (1302, 1303), "b" * 32)
    _bundle(captured, 7861464, (1004,), "c" * 32)
    collector.mkdir()
    (collector / "timeline-repairs.json").write_text(
        json.dumps(
            {
                "version": 1,
                "repairs": [
                    {
                        "start_sequence": 7717546,
                        "next_sequence": 7861464,
                        "offset_seconds": 300,
                        "evidence": "clock-op",
                    }
                ],
            }
        )
    )
    store = ClockCorrectionStore(collector / "device.json")
    zero = store.mark_unresolved(store.prepare(1000, 1000, 0.0, 7192026))
    store.finish(zero, state="resolved", boundary_sequence_max=7192026, verified_epoch=1000)
    applied = store.mark_unresolved(store.prepare(1302, 1002, 300.0, 7717545))
    store.finish(applied, state="applied", boundary_sequence_max=7861464, verified_epoch=1002)
    applied_path = collector / "clock-corrections" / f"{applied.operation_id}.json"

    result = publish_from_ledger(captured, published, collector)

    assert result.record_count == 5
    assert json.loads(applied_path.read_text())["state"] == "resolved"
    assert next(correction for correction in store.records() if correction.operation_id == zero.operation_id).state == (
        "resolved"
    )


def test_applied_operation_rejects_regression_across_sequence_gap(tmp_path: Path) -> None:
    captured = tmp_path / "captured"
    published = tmp_path / "source"
    collector = tmp_path / "collector"
    _bundle(captured, 10, (1000, 1001), "a" * 32)
    _bundle(captured, 13, (900,), "b" * 32)
    _bundle(captured, 15, (1003,), "c" * 32)
    collector.mkdir()
    collector.joinpath("timeline-repairs.json").write_text(json.dumps({"version": 1, "repairs": []}))
    store = ClockCorrectionStore(collector / "device.json")
    intent = store.mark_unresolved(store.prepare(900, 900, 300.0, 12))
    store.finish(intent, state="applied", boundary_sequence_max=15, verified_epoch=900)
    operation_path = collector / "clock-corrections" / f"{intent.operation_id}.json"

    with pytest.raises(TimelineGenerationError, match="regresses"):
        publish_from_ledger(captured, published, collector)
    assert json.loads(operation_path.read_text())["state"] == "applied"


def test_existing_generation_is_authenticated_before_reuse(tmp_path: Path) -> None:
    captured = tmp_path / "captured"
    published = tmp_path / "source"
    _bundle(captured, 10, (1000, 1001), "a" * 32)
    result = build_generation(captured, published, ())
    bundle = next(path for path in result.path.iterdir() if path.is_dir())
    (bundle / "records.bin").write_bytes(b"tampered")

    with pytest.raises(TimelineGenerationError, match="invalid"):
        build_generation(captured, published, ())


def test_applied_clock_operation_blocks_until_finite_repair_is_proven(tmp_path: Path) -> None:
    captured = tmp_path / "captured"
    collector = tmp_path / "collector"
    published = tmp_path / "source"
    _bundle(captured, 10, (1000, 1300, 1301, 1002, 1003), "a" * 32)
    operation = collector / "clock-corrections/op.json"
    operation.parent.mkdir(parents=True)
    operation.write_text(
        json.dumps(
            {
                "version": 2,
                "operation_id": "clock-op",
                "state": "applied",
                "observed_epoch": 1300,
                "target_epoch": 1000,
                "drift_seconds": 300.2,
                "boundary_sequence_min": 13,
                "boundary_sequence_max": 14,
                "verified_epoch": 1000,
            }
        )
    )

    with pytest.raises(TimelineGenerationError, match="regresses"):
        publish_from_ledger(captured, published, collector)
    assert json.loads(operation.read_text())["state"] == "applied"


def test_applied_operation_waits_for_successor_then_blocks_later_regression(tmp_path: Path) -> None:
    captured = tmp_path / "captured"
    collector = tmp_path / "collector"
    published = tmp_path / "source"
    _bundle(captured, 10, (1000, 1001, 1302, 1303), "a" * 32)
    collector.mkdir()
    (collector / "timeline-repairs.json").write_text(
        json.dumps(
            {
                "version": 1,
                "repairs": [
                    {
                        "start_sequence": 12,
                        "next_sequence": 14,
                        "offset_seconds": 300,
                        "evidence": "clock-op",
                    }
                ],
            }
        )
    )
    store = ClockCorrectionStore(collector / "device.json")
    intent = store.mark_unresolved(store.prepare(1302, 1002, 300.0, 12))
    store.finish(intent, state="applied", boundary_sequence_max=14, verified_epoch=1002)
    operation = collector / "clock-corrections" / f"{intent.operation_id}.json"

    with pytest.raises(TimelineGenerationError, match="boundaries are incomplete"):
        publish_from_ledger(captured, published, collector)
    assert json.loads(operation.read_text())["state"] == "applied"

    _bundle(captured, 14, (1001,), "b" * 32)
    with pytest.raises(TimelineGenerationError, match="regresses"):
        publish_from_ledger(captured, published, collector)
    assert json.loads(operation.read_text())["state"] == "applied"
    assert not (published / "current").exists()


def test_generation_appends_new_bundles_without_copying_history(tmp_path: Path) -> None:
    captured = tmp_path / "captured"
    published = tmp_path / "source"
    _bundle(captured, 10, (1000, 1001), "a" * 32)
    first = build_generation(captured, published, ())
    original = next(path for path in first.path.iterdir() if path.is_dir()) / "records.bin"
    original_inode = original.stat().st_ino
    _bundle(captured, 12, (1002, 1003), "b" * 32)

    second = build_generation(captured, published, ())

    assert second.generation_id == first.generation_id
    assert second.record_count == 4
    assert original.stat().st_ino == original_inode
