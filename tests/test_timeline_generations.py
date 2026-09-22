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


def _generation_timestamps(root: Path) -> list[int]:
    timestamps: list[int] = []
    for bundle in sorted(path for path in root.iterdir() if path.is_dir()):
        raw = (bundle / "records.bin").read_bytes()
        timestamps.extend(int.from_bytes(raw[index : index + 4], "big") for index in range(0, len(raw), RECORD_SIZE))
    return timestamps


def _tree_bytes(root: Path) -> dict[Path, bytes]:
    return {path.relative_to(root): path.read_bytes() for path in sorted(root.rglob("*")) if path.is_file()}


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
    store = ClockCorrectionStore(collector / "device.json")
    intent = store.mark_unresolved(store.prepare(1300, 1000, 300.0, 10, operation_id="clock-op"))
    store.finish(intent, state="applied", boundary_sequence_max=12, verified_epoch=1000)

    result = publish_from_ledger(captured, published, collector)
    assert result.record_count == 4
    assert json.loads((collector / "timeline-repairs.json").read_text()) == {
        "repairs": [{"evidence": "clock-op", "next_sequence": 12, "offset_seconds": 300, "start_sequence": 10}],
        "version": 1,
    }

    evidence = collector / "clock-corrections/op.json"
    evidence.parent.mkdir(parents=True, exist_ok=True)
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
    store = ClockCorrectionStore(collector / "device.json")
    intent = store.mark_unresolved(store.prepare(1302, 1002, 300.0, 12))
    store.finish(intent, state="applied", boundary_sequence_max=14, verified_epoch=1002)
    operation_path = collector / "clock-corrections" / f"{intent.operation_id}.json"

    result = publish_from_ledger(captured, published, collector)

    assert result.record_count == 5
    assert json.loads(operation_path.read_text())["state"] == "resolved"
    ledger = (collector / "timeline-repairs.json").read_bytes()
    assert json.loads(ledger) == {
        "repairs": [
            {
                "evidence": intent.operation_id,
                "next_sequence": 14,
                "offset_seconds": 300,
                "start_sequence": 12,
            }
        ],
        "version": 1,
    }
    repeated = publish_from_ledger(captured, published, collector)
    assert repeated.generation_id == result.generation_id
    assert (collector / "timeline-repairs.json").read_bytes() == ledger


def test_applied_operation_resolves_across_legitimate_sequence_gaps(tmp_path: Path) -> None:
    captured = tmp_path / "captured"
    published = tmp_path / "source"
    collector = tmp_path / "collector"
    _bundle(captured, 10, (1000, 1001), "a" * 32)
    _bundle(captured, 13, (1302, 1303), "b" * 32)
    _bundle(captured, 15, (1004,), "c" * 32)
    collector.mkdir()
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


def test_failed_applied_validation_preserves_prepared_correction_and_publication_state(tmp_path: Path) -> None:
    captured = tmp_path / "captured"
    published = tmp_path / "source"
    collector = tmp_path / "collector"
    _bundle(captured, 10, (1000, 1001), "a" * 32)
    _bundle(captured, 13, (900,), "b" * 32)
    _bundle(captured, 15, (1003,), "c" * 32)
    collector.mkdir()
    ledger = collector / "timeline-repairs.json"
    ledger.write_text(json.dumps({"version": 1, "repairs": []}))
    published.mkdir()
    current = published / "current"
    current.write_text("unchanged current")
    store = ClockCorrectionStore(collector / "device.json")
    applied = store.mark_unresolved(store.prepare(900, 900, 300.0, 12, operation_id="applied-v2"))
    store.finish(applied, state="applied", boundary_sequence_max=15, verified_epoch=900)
    prepared = collector / "clock-corrections" / "prepared-v2.json"
    prepared.write_text(
        json.dumps(
            {
                "version": 2,
                "operation_id": "prepared-v2",
                "state": "prepared",
                "observed_epoch": 1300,
                "target_epoch": 1000,
                "drift_seconds": 300.0,
                "boundary_sequence_min": 16,
                "boundary_sequence_max": None,
                "verified_epoch": None,
            }
        )
    )
    before_ledger = ledger.read_bytes()
    before_applied = (collector / "clock-corrections" / "applied-v2.json").read_bytes()
    before_prepared = prepared.read_bytes()
    before_current = current.read_bytes()

    with pytest.raises(TimelineGenerationError, match="regresses"):
        publish_from_ledger(captured, published, collector)

    assert ledger.read_bytes() == before_ledger
    assert (collector / "clock-corrections" / "applied-v2.json").read_bytes() == before_applied
    assert prepared.read_bytes() == before_prepared
    assert current.read_bytes() == before_current


def test_invalid_current_does_not_commit_valid_applied_repair(tmp_path: Path) -> None:
    captured = tmp_path / "captured"
    published = tmp_path / "source"
    collector = tmp_path / "collector"
    _bundle(captured, 10, (1000, 1001, 1302, 1303, 1004), "a" * 32)
    collector.mkdir()
    ledger = collector / "timeline-repairs.json"
    ledger.write_text(json.dumps({"version": 1, "repairs": []}))
    published.mkdir()
    current = published / "current"
    current.write_text("not a generation link")
    store = ClockCorrectionStore(collector / "device.json")
    applied = store.mark_unresolved(store.prepare(1302, 1002, 300.0, 12, operation_id="applied-v2"))
    store.finish(applied, state="applied", boundary_sequence_max=14, verified_epoch=1002)
    operation = collector / "clock-corrections" / "applied-v2.json"
    before_ledger = ledger.read_bytes()
    before_operation = operation.read_bytes()
    before_current = current.read_bytes()

    with pytest.raises(TimelineGenerationError, match="generation link"):
        publish_from_ledger(captured, published, collector)

    assert ledger.read_bytes() == before_ledger
    assert operation.read_bytes() == before_operation
    assert current.read_bytes() == before_current


def test_known_operation_id_mismatch_rejects_ledger_without_mutation(tmp_path: Path) -> None:
    captured = tmp_path / "captured"
    published = tmp_path / "source"
    collector = tmp_path / "collector"
    _bundle(captured, 10, (1000, 1001, 1302, 1303, 1004), "a" * 32)
    collector.mkdir()
    store = ClockCorrectionStore(collector / "device.json")
    intent = store.mark_unresolved(store.prepare(1302, 1002, 300.0, 12, operation_id="clock-op"))
    store.finish(intent, state="applied", boundary_sequence_max=14, verified_epoch=1002)
    ledger = collector / "timeline-repairs.json"
    ledger.write_text(
        json.dumps(
            {
                "version": 1,
                "repairs": [{"start_sequence": 12, "next_sequence": 14, "offset_seconds": 299, "evidence": "clock-op"}],
            }
        )
    )
    original_ledger = ledger.read_bytes()

    with pytest.raises(TimelineGenerationError, match="conflicts"):
        publish_from_ledger(captured, published, collector)

    assert store.records()[0].state == "applied"
    assert ledger.read_bytes() == original_ledger


def test_zero_width_applied_clock_operation_resolves_without_a_repair_interval(tmp_path: Path) -> None:
    captured = tmp_path / "captured"
    published = tmp_path / "source"
    collector = tmp_path / "collector"
    _bundle(captured, 10, (1000,), "a" * 32)
    collector.mkdir()
    store = ClockCorrectionStore(collector / "device.json")
    intent = store.mark_unresolved(store.prepare(1300, 1000, 300.0, 10))
    store.finish(intent, state="applied", boundary_sequence_max=10, verified_epoch=1000)

    result = publish_from_ledger(captured, published, collector)

    assert result.record_count == 1
    assert store.records()[0].state == "resolved"
    assert not (collector / "timeline-repairs.json").exists()


def test_legacy_repair_baseline_survives_a_zero_width_resolved_operation(tmp_path: Path) -> None:
    captured = tmp_path / "captured"
    published = tmp_path / "source"
    collector = tmp_path / "collector"
    _bundle(captured, 10, (1300, 1301), "a" * 32)
    _bundle(captured, 12, (1002, 1003), "b" * 32)
    _bundle(captured, 20, (1304, 1305), "c" * 32)
    collector.mkdir()
    ledger = collector / "timeline-repairs.json"
    ledger.write_text(
        json.dumps(
            {
                "version": 1,
                "repairs": [
                    {"start_sequence": 10, "next_sequence": 12, "offset_seconds": 300, "evidence": "legacy-a"},
                    {"start_sequence": 20, "next_sequence": 22, "offset_seconds": 300, "evidence": "legacy-b"},
                ],
            }
        )
    )
    original_ledger = ledger.read_bytes()
    store = ClockCorrectionStore(collector / "device.json")
    correction = store.mark_unresolved(store.prepare(1006, 1006, 0.0, 22, operation_id="point-v2"))
    store.finish(correction, state="resolved", boundary_sequence_max=22, verified_epoch=1006)

    result = publish_from_ledger(captured, published, collector)

    assert result.record_count == 6
    assert _generation_timestamps(result.path) == [1000, 1001, 1002, 1003, 1004, 1005]
    assert ledger.read_bytes() == original_ledger


def test_later_nonzero_applied_operation_appends_after_legacy_baseline(tmp_path: Path) -> None:
    captured = tmp_path / "captured"
    published = tmp_path / "source"
    collector = tmp_path / "collector"
    _bundle(captured, 10, (1300, 1301), "a" * 32)
    _bundle(captured, 12, (1002, 1003), "b" * 32)
    _bundle(captured, 20, (1304, 1305), "c" * 32)
    collector.mkdir()
    ledger = collector / "timeline-repairs.json"
    ledger.write_text(
        json.dumps(
            {
                "version": 1,
                "repairs": [
                    {"start_sequence": 10, "next_sequence": 12, "offset_seconds": 300, "evidence": "legacy-a"},
                    {"start_sequence": 20, "next_sequence": 22, "offset_seconds": 300, "evidence": "legacy-b"},
                ],
            }
        )
    )
    store = ClockCorrectionStore(collector / "device.json")
    point = store.mark_unresolved(store.prepare(1006, 1006, 0.0, 22, operation_id="point-v2"))
    store.finish(point, state="resolved", boundary_sequence_max=22, verified_epoch=1006)
    publish_from_ledger(captured, published, collector)
    _bundle(captured, 22, (1006, 1007), "d" * 32)
    _bundle(captured, 24, (1308, 1309), "e" * 32)
    _bundle(captured, 26, (1010,), "f" * 32)
    applied = store.mark_unresolved(store.prepare(1308, 1008, 300.0, 24, operation_id="later-v2"))
    store.finish(applied, state="applied", boundary_sequence_max=26, verified_epoch=1008)

    result = publish_from_ledger(captured, published, collector)

    assert result.record_count == 11
    assert _generation_timestamps(result.path) == list(range(1000, 1011))
    assert next(item for item in store.records() if item.operation_id == "later-v2").state == "resolved"
    assert json.loads(ledger.read_text())["repairs"][-1] == {
        "start_sequence": 24,
        "next_sequence": 26,
        "offset_seconds": 300,
        "evidence": "later-v2",
    }


def test_overlapping_current_repair_rejects_ledger_without_mutation(tmp_path: Path) -> None:
    captured = tmp_path / "captured"
    published = tmp_path / "source"
    collector = tmp_path / "collector"
    _bundle(captured, 10, (1000, 1001, 1302, 1303, 1004), "a" * 32)
    collector.mkdir()
    ledger = collector / "timeline-repairs.json"
    ledger.write_text(
        json.dumps(
            {
                "version": 1,
                "repairs": [{"start_sequence": 11, "next_sequence": 13, "offset_seconds": 300, "evidence": "legacy-a"}],
            }
        )
    )
    original_ledger = ledger.read_bytes()
    store = ClockCorrectionStore(collector / "device.json")
    correction = store.mark_unresolved(store.prepare(1302, 1002, 300.0, 12, operation_id="current-v2"))
    store.finish(correction, state="applied", boundary_sequence_max=14, verified_epoch=1002)

    with pytest.raises(TimelineGenerationError, match="overlap"):
        publish_from_ledger(captured, published, collector)

    assert store.records()[0].state == "applied"
    assert ledger.read_bytes() == original_ledger
    assert not (published / "current").exists()


def test_resolved_nonzero_operation_requires_an_exact_durable_repair(tmp_path: Path) -> None:
    captured = tmp_path / "captured"
    published = tmp_path / "source"
    collector = tmp_path / "collector"
    _bundle(captured, 10, (1000, 1001, 1302, 1303, 1004), "a" * 32)
    collector.mkdir()
    store = ClockCorrectionStore(collector / "device.json")
    correction = store.mark_unresolved(store.prepare(1302, 1002, 300.0, 12, operation_id="resolved-v2"))
    applied = store.finish(correction, state="applied", boundary_sequence_max=14, verified_epoch=1002)
    store.resolve_applied(applied)

    with pytest.raises(TimelineGenerationError, match="resolved clock correction has no durable timeline repair"):
        publish_from_ledger(captured, published, collector)

    assert not (collector / "timeline-repairs.json").exists()
    assert store.records()[0].state == "resolved"
    assert not (published / "current").exists()


def test_repair_ledger_must_be_durable_before_applied_operation_resolves(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    captured = tmp_path / "captured"
    published = tmp_path / "source"
    collector = tmp_path / "collector"
    _bundle(captured, 10, (1000, 1001, 1302, 1303, 1004), "a" * 32)
    collector.mkdir()
    store = ClockCorrectionStore(collector / "device.json")
    intent = store.mark_unresolved(store.prepare(1302, 1002, 300.0, 12))
    store.finish(intent, state="applied", boundary_sequence_max=14, verified_epoch=1002)

    def fail_ledger_rename(*_args: object, **_kwargs: object) -> None:
        raise OSError("simulated ledger rename failure")

    monkeypatch.setattr(timeline_generations.os, "replace", fail_ledger_rename)

    with pytest.raises(TimelineGenerationError, match="not durable"):
        publish_from_ledger(captured, published, collector)

    assert store.records()[0].state == "applied"
    assert not (collector / "timeline-repairs.json").exists()


def test_commit_failure_does_not_mutate_current_generation_during_preparation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    captured = tmp_path / "captured"
    published = tmp_path / "source"
    collector = tmp_path / "collector"
    _bundle(captured, 10, (1000, 1001), "a" * 32)
    initial = build_generation(captured, published, ())
    _bundle(captured, 12, (1302, 1303), "b" * 32)
    _bundle(captured, 14, (1004,), "c" * 32)
    collector.mkdir()
    store = ClockCorrectionStore(collector / "device.json")
    applied = store.mark_unresolved(store.prepare(1302, 1002, 300.0, 12, operation_id="applied-v2"))
    store.finish(applied, state="applied", boundary_sequence_max=14, verified_epoch=1002)
    before_current = (published / "current").readlink()
    before_tree = _tree_bytes(initial.path)
    real_write_repairs = timeline_generations._write_repairs_atomic

    def fail_commit(*_args: object, **_kwargs: object) -> None:
        raise TimelineGenerationError("simulated repair commit failure")

    monkeypatch.setattr(timeline_generations, "_write_repairs_atomic", fail_commit)
    with pytest.raises(TimelineGenerationError, match="simulated repair commit failure"):
        publish_from_ledger(captured, published, collector)

    assert (published / "current").readlink() == before_current
    assert _tree_bytes(initial.path) == before_tree
    assert not (collector / "timeline-repairs.json").exists()
    assert store.records()[0].state == "applied"
    prepared = tuple(path for path in (published / ".generations").iterdir() if path != initial.path)
    assert len(prepared) == 1

    monkeypatch.setattr(timeline_generations, "_write_repairs_atomic", real_write_repairs)
    result = publish_from_ledger(captured, published, collector)

    assert result.record_count == 5
    assert result.path == prepared[0]
    assert (published / "current").resolve() == result.path
    assert store.records()[0].state == "resolved"


def test_receipt_failure_cleans_isolated_candidate_and_retry_succeeds(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    captured = tmp_path / "captured"
    published = tmp_path / "source"
    collector = tmp_path / "collector"
    _bundle(captured, 10, (1000,), "a" * 32)
    initial = build_generation(captured, published, ())
    _bundle(captured, 11, (1001,), "b" * 32)
    _bundle(captured, 12, (1002,), "c" * 32)
    collector.mkdir()
    before_current = (published / "current").readlink()
    before_tree = _tree_bytes(initial.path)
    generations = published / ".generations"
    before_generations = sorted(path.name for path in generations.iterdir())
    second_receipt = next(path for path in captured.glob("12-13-*") for path in (path / "receipt.json",))
    real_read_text = Path.read_text

    def fail_second_receipt(
        path: Path,
        encoding: str | None = None,
        errors: str | None = None,
        newline: str | None = None,
    ) -> str:
        if path == second_receipt:
            raise OSError("simulated second receipt failure")
        return real_read_text(path, encoding=encoding, errors=errors, newline=newline)

    monkeypatch.setattr(Path, "read_text", fail_second_receipt)
    with pytest.raises(OSError, match="simulated second receipt failure"):
        publish_from_ledger(captured, published, collector)

    assert (published / "current").readlink() == before_current
    assert _tree_bytes(initial.path) == before_tree
    assert sorted(path.name for path in generations.iterdir()) == before_generations

    monkeypatch.setattr(Path, "read_text", real_read_text)
    result = publish_from_ledger(captured, published, collector)

    assert result.record_count == 3
    assert (published / "current").resolve() == result.path


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
    intent = store.mark_unresolved(store.prepare(1302, 1002, 300.0, 12, operation_id="clock-op"))
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


@pytest.mark.parametrize("suffix", ("not-a-digest", "a" * 63, "A" * 64))
def test_current_rejects_malformed_replacement_generation_id(tmp_path: Path, suffix: str) -> None:
    captured = tmp_path / "captured"
    published = tmp_path / "source"
    _bundle(captured, 10, (1000,), "a" * 32)
    initial = build_generation(captured, published, ())
    current = published / "current"
    current.unlink()
    current.symlink_to(f".generations/{initial.generation_id}.{suffix}", target_is_directory=True)

    with pytest.raises(TimelineGenerationError, match="must target a generation"):
        build_generation(captured, published, ())


def test_current_rejects_dangling_replacement_generation(tmp_path: Path) -> None:
    captured = tmp_path / "captured"
    published = tmp_path / "source"
    _bundle(captured, 10, (1000,), "a" * 32)
    initial = build_generation(captured, published, ())
    current = published / "current"
    current.unlink()
    current.symlink_to(f".generations/{initial.generation_id}.{'a' * 64}", target_is_directory=True)

    with pytest.raises(TimelineGenerationError, match="generation is unavailable"):
        build_generation(captured, published, ())


def test_late_salvage_replaces_generation_without_exposing_a_partial_view(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    captured = tmp_path / "captured"
    published = tmp_path / "source"
    _bundle(captured, 102, (1002,), "a" * 32)
    first = build_generation(captured, published, ())
    _bundle(captured, 100, (1000, 1001), "b" * 32)

    real_switch_current = timeline_generations._switch_current
    reader_views: list[list[int]] = []

    def switch_after_reader_can_observe_complete_generation(
        publication_root: Path, destination: Path, *, publication_descriptor: int | None = None
    ) -> None:
        reader_views.append(_generation_timestamps((publication_root / "current").resolve()))
        assert _generation_timestamps(destination) == [1000, 1001, 1002]
        real_switch_current(publication_root, destination, publication_descriptor=publication_descriptor)

    monkeypatch.setattr(timeline_generations, "_switch_current", switch_after_reader_can_observe_complete_generation)

    replacement = build_generation(captured, published, ())
    repeated = build_generation(captured, published, ())

    assert replacement.generation_id != first.generation_id
    assert replacement.path != first.path
    replacement_digest = replacement.generation_id.removeprefix(f"{first.generation_id}.")
    assert len(replacement_digest) == 64
    assert set(replacement_digest) <= set("0123456789abcdef")
    assert _generation_timestamps(first.path) == [1002]
    assert reader_views == [[1002], [1000, 1001, 1002]]
    assert _generation_timestamps((published / "current").resolve()) == [1000, 1001, 1002]
    assert repeated.generation_id == replacement.generation_id
    assert repeated.path == replacement.path
    assert repeated.record_count == 3
    assert {path for path in (published / ".generations").iterdir() if path.is_dir()} == {
        first.path,
        replacement.path,
    }
