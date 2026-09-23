"""Ready finalization keeps draft bytes authoritative until publication is durable."""

from __future__ import annotations

from hashlib import sha256
from json import dumps, loads
from pathlib import Path
from typing import cast

import pytest

from omi_collector.capture.adapters import ready_bundles
from omi_collector.capture.adapters.bundle_contract import BundleManifest, SealedReceipt
from omi_collector.capture.adapters.clock_segments import ClockSegment, ClockSegmentMap
from omi_collector.capture.domain.ring_protocol import RECORD_SIZE


def _record(timestamp: int, marker: int) -> bytes:
    return timestamp.to_bytes(4, "big") + bytes((marker,)) * (RECORD_SIZE - 4)


def _draft(root: Path, timestamps: tuple[int, ...], *, start_sequence: int = 10) -> Path:
    raw = b"".join(_record(timestamp, index) for index, timestamp in enumerate(timestamps, 1))
    digest = sha256(raw).hexdigest()
    path = root / f"{start_sequence}-{start_sequence + len(timestamps)}-{digest[:16]}"
    path.mkdir(parents=True)
    (path / "records.bin").write_bytes(raw)
    manifest = BundleManifest(2, start_sequence, start_sequence + len(timestamps), len(timestamps), RECORD_SIZE, digest)
    (path / "manifest.json").write_text(dumps(manifest.as_dict()), encoding="utf-8")
    (path / "receipt.json").write_text(dumps(SealedReceipt("a" * 32, digest).as_dict()), encoding="utf-8")
    return path


def test_finalization_rewrites_only_confirmed_timestamp_ranges(tmp_path: Path) -> None:
    draft = _draft(tmp_path / "draft", (100, 200, 300))
    original = (draft / "records.bin").read_bytes()
    segments = ClockSegmentMap((ClockSegment("observation", 11, 12, 0.6, 0.1),))

    result = ready_bundles.finalize_drafts(tmp_path / "draft", tmp_path / "ready", tmp_path / "ledger.json", segments)

    assert len(result) == 1
    ready = result[0].path
    records = (ready / "records.bin").read_bytes()
    assert [int.from_bytes(records[index : index + 4], "big") for index in range(0, len(records), RECORD_SIZE)] == [
        100,
        201,
        300,
    ]
    assert records[4:RECORD_SIZE] == original[4:RECORD_SIZE]
    manifest = cast(dict[str, object], loads((ready / "manifest.json").read_text(encoding="utf-8")))
    assert manifest["time_ranges"] == [
        {"start_sequence": 10, "next_sequence": 11, "utc": None},
        {
            "start_sequence": 11,
            "next_sequence": 12,
            "utc": {"observation_id": "observation", "offset_seconds": 0.6, "uncertainty_seconds": 0.1},
        },
        {"start_sequence": 12, "next_sequence": 13, "utc": None},
    ]
    assert not draft.exists()


def test_existing_ready_bundle_completes_crash_replay_before_draft_cleanup(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    draft = _draft(tmp_path / "draft", (100,))
    ledger = tmp_path / "collector" / "ready-publications.json"

    monkeypatch.setattr(ready_bundles, "_record_ready", lambda *_: (_ for _ in ()).throw(OSError("interrupted")))
    with pytest.raises(OSError, match="interrupted"):
        ready_bundles.finalize_drafts(tmp_path / "draft", tmp_path / "ready", ledger, ClockSegmentMap(()))
    assert draft.exists()
    assert len(tuple((tmp_path / "ready").iterdir())) == 1

    monkeypatch.undo()
    result = ready_bundles.finalize_drafts(tmp_path / "draft", tmp_path / "ready", ledger, ClockSegmentMap(()))

    assert len(result) == 1
    assert not draft.exists()
    assert (
        loads(ledger.read_text(encoding="utf-8"))["bundles"][result[0].bundle_id]["records_sha256"]
        == result[0].records_sha256
    )


def test_unknown_time_finalizes_without_changing_device_timestamps(tmp_path: Path) -> None:
    draft = _draft(tmp_path / "draft", (100, 101))
    original = (draft / "records.bin").read_bytes()

    result = ready_bundles.finalize_drafts(
        tmp_path / "draft", tmp_path / "ready", tmp_path / "ledger.json", ClockSegmentMap(())
    )

    assert (result[0].path / "records.bin").read_bytes() == original


def test_timestamp_overflow_keeps_draft_for_retry(tmp_path: Path) -> None:
    draft = _draft(tmp_path / "draft", ((1 << 32) - 1,))

    with pytest.raises(ready_bundles.ReadyBundleError, match="outside uint32"):
        ready_bundles.finalize_drafts(
            tmp_path / "draft",
            tmp_path / "ready",
            tmp_path / "ledger.json",
            ClockSegmentMap((ClockSegment("observation", 10, 11, 1.0, 0.1),)),
        )

    assert draft.exists()
    assert not tuple((tmp_path / "ready").iterdir())
