from __future__ import annotations

import json
from hashlib import sha256
from pathlib import Path

import pytest

from omi_collector.capture.adapters.bundle_contract import BundleManifest, SealedReceipt
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
    manifest = BundleManifest("omi", start, start + len(timestamps), len(timestamps), RECORD_SIZE, digest)
    (path / "records.bin").write_bytes(raw)
    (path / "manifest.json").write_text(json.dumps(manifest.as_dict()))
    (path / "receipt.json").write_text(json.dumps(SealedReceipt(attempt, digest).as_dict()))


def test_generation_repairs_epoch_and_atomically_exposes_ordinary_bundles(tmp_path: Path) -> None:
    captured = tmp_path / "captured" / "omi"
    published = tmp_path / "source"
    _bundle(captured, 10, (1300, 1301), "a" * 32)
    _bundle(captured, 12, (1002, 1003), "b" * 32)

    result = build_generation(captured.parent, published, "omi", (TimeRepair(10, 12, 300, "clock-op"),))

    assert (published / "omi").is_symlink()
    assert (published / "omi").resolve() == result.path
    bundles = sorted(path for path in (published / "omi").iterdir() if path.is_dir())
    timestamps = []
    for bundle in bundles:
        raw = (bundle / "records.bin").read_bytes()
        timestamps.extend(int.from_bytes(raw[index : index + 4], "big") for index in range(0, len(raw), RECORD_SIZE))
    assert timestamps == [1000, 1001, 1002, 1003]


def test_generation_never_exposes_a_regressing_candidate(tmp_path: Path) -> None:
    captured = tmp_path / "captured" / "omi"
    published = tmp_path / "source"
    _bundle(captured, 10, (1000, 999), "a" * 32)

    with pytest.raises(TimelineGenerationError, match="regresses"):
        build_generation(captured.parent, published, "omi", ())

    assert not (published / "omi").exists()


def test_ledger_publishes_only_settled_clock_evidence(tmp_path: Path) -> None:
    captured = tmp_path / "captured" / "omi"
    published = tmp_path / "source"
    collector = tmp_path / "collector"
    _bundle(captured, 10, (1300, 1301), "a" * 32)
    _bundle(captured, 12, (1002, 1003), "b" * 32)
    collector.mkdir()
    (collector / "timeline-repairs.json").write_text(
        json.dumps(
            {
                "version": 1,
                "devices": {
                    "omi": [
                        {
                            "start_sequence": 10,
                            "next_sequence": 12,
                            "offset_seconds": 300,
                            "evidence": "clock-op",
                        }
                    ]
                },
            }
        )
    )

    result = publish_from_ledger(captured.parent, published, collector, "omi")
    assert result.record_count == 4

    evidence = collector / "clock-corrections/omi/op.json"
    evidence.parent.mkdir(parents=True)
    evidence.write_text(json.dumps({"state": "unresolved"}))
    with pytest.raises(TimelineGenerationError, match="unresolved"):
        publish_from_ledger(captured.parent, published, collector, "omi")


def test_existing_generation_is_authenticated_before_reuse(tmp_path: Path) -> None:
    captured = tmp_path / "captured" / "omi"
    published = tmp_path / "source"
    _bundle(captured, 10, (1000, 1001), "a" * 32)
    result = build_generation(captured.parent, published, "omi", ())
    bundle = next(path for path in result.path.iterdir() if path.is_dir())
    (bundle / "records.bin").write_bytes(b"tampered")

    with pytest.raises(TimelineGenerationError, match="invalid"):
        build_generation(captured.parent, published, "omi", ())


def test_applied_clock_operation_blocks_until_finite_repair_is_proven(tmp_path: Path) -> None:
    captured = tmp_path / "captured" / "omi"
    collector = tmp_path / "collector"
    published = tmp_path / "source"
    _bundle(captured, 10, (1000, 1300, 1301, 1002, 1003), "a" * 32)
    operation = collector / "clock-corrections/omi/op.json"
    operation.parent.mkdir(parents=True)
    operation.write_text(
        json.dumps(
            {
                "state": "applied",
                "operation_id": "clock-op",
                "drift_seconds": 300.2,
                "boundary_sequence_min": 13,
                "boundary_sequence_max": 14,
            }
        )
    )

    with pytest.raises(TimelineGenerationError, match="unresolved"):
        publish_from_ledger(captured.parent, published, collector, "omi")
    assert json.loads(operation.read_text())["state"] == "applied"


def test_generation_appends_new_bundles_without_copying_history(tmp_path: Path) -> None:
    captured = tmp_path / "captured" / "omi"
    published = tmp_path / "source"
    _bundle(captured, 10, (1000, 1001), "a" * 32)
    first = build_generation(captured.parent, published, "omi", ())
    original = next(path for path in first.path.iterdir() if path.is_dir()) / "records.bin"
    original_inode = original.stat().st_ino
    _bundle(captured, 12, (1002, 1003), "b" * 32)

    second = build_generation(captured.parent, published, "omi", ())

    assert second.generation_id == first.generation_id
    assert second.record_count == 4
    assert original.stat().st_ino == original_inode
