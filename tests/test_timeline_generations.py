from __future__ import annotations

import json
from hashlib import sha256
from pathlib import Path

import pytest

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
