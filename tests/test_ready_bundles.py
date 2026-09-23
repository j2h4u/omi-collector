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


def _draft(root: Path, timestamps: tuple[int, ...], *, start_sequence: int = 10, marker_start: int = 1) -> Path:
    raw = b"".join(_record(timestamp, marker_start + index) for index, timestamp in enumerate(timestamps))
    digest = sha256(raw).hexdigest()
    path = root / f"{start_sequence}-{start_sequence + len(timestamps)}-{digest[:16]}"
    path.mkdir(parents=True)
    (path / "records.bin").write_bytes(raw)
    manifest = BundleManifest(2, start_sequence, start_sequence + len(timestamps), len(timestamps), RECORD_SIZE, digest)
    (path / "manifest.json").write_text(dumps(manifest.as_dict()), encoding="utf-8")
    (path / "receipt.json").write_text(dumps(SealedReceipt("a" * 32, digest).as_dict()), encoding="utf-8")
    return path


def _checkpoint(
    root: Path,
    identities: list[tuple[str, str]],
    *,
    tail: list[tuple[str, str]] | None = None,
    decisions: list[tuple[str, str]] | None = None,
) -> Path:
    def decision(bundle_id: str, records_sha256: str) -> dict[str, object]:
        return {
            "bundle_id": bundle_id,
            "records_sha256": records_sha256,
            "packet_ranges": [],
            "packet_count": 0,
            "input_id": None,
            "input_sha256": None,
            "receipt_sha256": None,
        }

    path = root / "work" / "omi-ready-checkpoint.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        dumps(
            {
                "analysis_cursor": None,
                "vad_decisions": [decision(*identity) for identity in identities if decisions is None]
                + [decision(*identity) for identity in decisions or []],
                "open_speech_tail": {"entries": [decision(*identity) for identity in tail]} if tail else None,
                "acknowledged": [
                    {"bundle_id": bundle_id, "records_sha256": records_sha256}
                    for bundle_id, records_sha256 in identities
                ],
            }
        ),
        encoding="utf-8",
    )
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


def test_unledgered_ready_reconciles_before_overlap_and_can_later_retire(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    ledger = tmp_path / "collector" / "ready-publications.json"
    ready_root = tmp_path / "ready"
    _draft(tmp_path / "draft", tuple(range(10)), start_sequence=100)
    monkeypatch.setattr(ready_bundles, "_record_ready", lambda *_: (_ for _ in ()).throw(OSError("interrupted")))
    with pytest.raises(OSError, match="interrupted"):
        ready_bundles.finalize_drafts(tmp_path / "draft", ready_root, ledger, ClockSegmentMap(()))
    monkeypatch.undo()
    _draft(tmp_path / "draft", tuple(range(15)), start_sequence=100)

    result = ready_bundles.finalize_drafts(tmp_path / "draft", ready_root, ledger, ClockSegmentMap(()))

    original = next(item for item in result if item.record_count == 10)
    state = cast(dict[str, object], loads(ledger.read_text(encoding="utf-8")))
    bundles = cast(dict[str, dict[str, object]], state["bundles"])
    assert bundles[original.bundle_id]["state"] == "ready"
    checkpoint = _checkpoint(tmp_path, [(original.bundle_id, original.records_sha256)])
    ready_bundles.retire_acknowledged(ready_root, ledger, checkpoint)
    assert not original.path.exists()


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


def test_contained_replay_uses_ready_payloads_without_creating_a_second_bundle(tmp_path: Path) -> None:
    start = 8_942_873
    ready_root = tmp_path / "ready"
    ledger = tmp_path / "collector" / "ready-publications.json"
    _draft(tmp_path / "draft", tuple(range(30)), start_sequence=start)
    ready_bundles.finalize_drafts(tmp_path / "draft", ready_root, ledger, ClockSegmentMap(()))
    duplicate = _draft(tmp_path / "draft", tuple(range(5, 25)), start_sequence=start + 5, marker_start=6)

    result = ready_bundles.finalize_drafts(tmp_path / "draft", ready_root, ledger, ClockSegmentMap(()))

    assert result == ()
    assert not duplicate.exists()
    assert len(tuple(ready_root.iterdir())) == 1


def test_replay_prefix_publishes_only_its_unique_suffix(tmp_path: Path) -> None:
    ready_root = tmp_path / "ready"
    ledger = tmp_path / "collector" / "ready-publications.json"
    _draft(tmp_path / "draft", tuple(range(10)), start_sequence=100)
    ready_bundles.finalize_drafts(tmp_path / "draft", ready_root, ledger, ClockSegmentMap(()))
    _draft(tmp_path / "draft", tuple(range(15)), start_sequence=100)

    result = ready_bundles.finalize_drafts(tmp_path / "draft", ready_root, ledger, ClockSegmentMap(()))

    assert [(item.next_sequence - item.record_count, item.next_sequence) for item in result] == [(110, 115)]
    suffix = result[0].path / "records.bin"
    assert suffix.read_bytes()[4] == 11
    assert tuple(tmp_path.joinpath("draft").iterdir()) == ()


def test_conflicting_ready_overlap_keeps_draft_and_fails_closed(tmp_path: Path) -> None:
    ready_root = tmp_path / "ready"
    ledger = tmp_path / "collector" / "ready-publications.json"
    _draft(tmp_path / "draft", tuple(range(10)), start_sequence=100)
    ready_bundles.finalize_drafts(tmp_path / "draft", ready_root, ledger, ClockSegmentMap(()))
    draft = _draft(tmp_path / "draft", tuple(range(15)), start_sequence=100, marker_start=99)

    with pytest.raises(ready_bundles.ReadyBundleError, match="conflicts with original payload"):
        ready_bundles.finalize_drafts(tmp_path / "draft", ready_root, ledger, ClockSegmentMap(()))

    assert draft.exists()
    assert len(tuple(ready_root.iterdir())) == 1


def test_sequence_reuse_without_stream_epoch_is_never_silently_suppressed(tmp_path: Path) -> None:
    ready_root = tmp_path / "ready"
    ledger = tmp_path / "collector" / "ready-publications.json"
    _draft(tmp_path / "draft", tuple(range(10)), start_sequence=100)
    ready_bundles.finalize_drafts(tmp_path / "draft", ready_root, ledger, ClockSegmentMap(()))
    reset = _draft(tmp_path / "draft", tuple(range(10)), start_sequence=100, marker_start=99)

    with pytest.raises(ready_bundles.ReadyBundleError, match="conflicts with original payload"):
        ready_bundles.finalize_drafts(tmp_path / "draft", ready_root, ledger, ClockSegmentMap(()))

    assert reset.exists()


def test_many_distinct_draft_tails_are_all_finalized(tmp_path: Path) -> None:
    for start in (100, 110, 120, 130):
        _draft(tmp_path / "draft", (start,), start_sequence=start)

    result = ready_bundles.finalize_drafts(
        tmp_path / "draft", tmp_path / "ready", tmp_path / "ledger.json", ClockSegmentMap(())
    )

    assert [item.next_sequence for item in result] == [101, 111, 121, 131]


def test_retired_exact_replay_is_removed_but_partial_retired_overlap_fails_closed(tmp_path: Path) -> None:
    ready_root = tmp_path / "ready"
    ledger = tmp_path / "collector" / "ready-publications.json"
    _draft(tmp_path / "draft", tuple(range(10)), start_sequence=100)
    published = ready_bundles.finalize_drafts(tmp_path / "draft", ready_root, ledger, ClockSegmentMap(()))[0]
    checkpoint = _checkpoint(tmp_path, [(published.bundle_id, published.records_sha256)])
    ready_bundles.retire_acknowledged(ready_root, ledger, checkpoint)
    exact = _draft(tmp_path / "draft", tuple(range(10)), start_sequence=100)

    assert ready_bundles.finalize_drafts(tmp_path / "draft", ready_root, ledger, ClockSegmentMap(())) == ()
    assert not exact.exists()

    partial = _draft(tmp_path / "draft", tuple(range(5)), start_sequence=105, marker_start=6)
    with pytest.raises(ready_bundles.ReadyBundleError, match="retired range"):
        ready_bundles.finalize_drafts(tmp_path / "draft", ready_root, ledger, ClockSegmentMap(()))
    assert partial.exists()


def test_ack_retirement_records_ledger_before_unlink_and_recovers_after_each_boundary(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    ready_root = tmp_path / "ready"
    ledger = tmp_path / "collector" / "ready-publications.json"
    _draft(tmp_path / "draft", (100,), start_sequence=100)
    published = ready_bundles.finalize_drafts(tmp_path / "draft", ready_root, ledger, ClockSegmentMap(()))[0]
    checkpoint = _checkpoint(tmp_path, [(published.bundle_id, published.records_sha256)])
    original_remove = ready_bundles._remove_retired_ready

    monkeypatch.setattr(
        ready_bundles, "_remove_retired_ready", lambda *_: (_ for _ in ()).throw(OSError("before unlink"))
    )
    with pytest.raises(OSError, match="before unlink"):
        ready_bundles.retire_acknowledged(ready_root, ledger, checkpoint)
    assert loads(ledger.read_text(encoding="utf-8"))["bundles"][published.bundle_id]["state"] == "retired"
    assert published.path.exists()

    monkeypatch.setattr(ready_bundles, "_remove_retired_ready", original_remove)
    ready_bundles.retire_acknowledged(ready_root, ledger, checkpoint)
    assert not published.path.exists()

    def unlink_then_interrupt(path: Path) -> None:
        original_remove(path)
        raise OSError("after unlink")

    monkeypatch.setattr(ready_bundles, "_remove_retired_ready", unlink_then_interrupt)
    with pytest.raises(OSError, match="after unlink"):
        ready_bundles.retire_acknowledged(ready_root, ledger, checkpoint)
    monkeypatch.setattr(ready_bundles, "_remove_retired_ready", original_remove)
    assert ready_bundles.retire_acknowledged(ready_root, ledger, checkpoint)[0].bundle_id == published.bundle_id


def test_forged_or_open_tail_ack_never_deletes_ready_bundle(tmp_path: Path) -> None:
    ready_root = tmp_path / "ready"
    ledger = tmp_path / "collector" / "ready-publications.json"
    _draft(tmp_path / "draft", (100,), start_sequence=100)
    published = ready_bundles.finalize_drafts(tmp_path / "draft", ready_root, ledger, ClockSegmentMap(()))[0]
    forged = _checkpoint(tmp_path, [(published.bundle_id, "b" * 64)])

    with pytest.raises(ready_bundles.ReadyBundleError, match="published ready bundle"):
        ready_bundles.retire_acknowledged(ready_root, ledger, forged)
    assert published.path.exists()

    tail = _checkpoint(
        tmp_path,
        [(published.bundle_id, published.records_sha256)],
        tail=[(published.bundle_id, published.records_sha256)],
    )
    with pytest.raises(ready_bundles.ReadyBundleError, match="open speech tail"):
        ready_bundles.retire_acknowledged(ready_root, ledger, tail)
    assert published.path.exists()


def test_ack_without_vad_decision_retires_exact_identity_but_not_mismatch(tmp_path: Path) -> None:
    ready_root = tmp_path / "ready"
    ledger = tmp_path / "collector" / "ready-publications.json"
    _draft(tmp_path / "draft", (100,), start_sequence=100)
    exact = ready_bundles.finalize_drafts(tmp_path / "draft", ready_root, ledger, ClockSegmentMap(()))[0]
    checkpoint = _checkpoint(tmp_path, [(exact.bundle_id, exact.records_sha256)], decisions=[])

    ready_bundles.retire_acknowledged(ready_root, ledger, checkpoint)
    assert not exact.path.exists()

    _draft(tmp_path / "draft", (101,), start_sequence=101)
    mismatch = ready_bundles.finalize_drafts(tmp_path / "draft", ready_root, ledger, ClockSegmentMap(()))[0]
    checkpoint = _checkpoint(tmp_path, [(mismatch.bundle_id, "b" * 64)], decisions=[])

    with pytest.raises(ready_bundles.ReadyBundleError, match="published ready bundle"):
        ready_bundles.retire_acknowledged(ready_root, ledger, checkpoint)
    assert mismatch.path.exists()


def test_mixed_ack_batch_preflights_every_identity_before_retiring_any_bundle(tmp_path: Path) -> None:
    ready_root = tmp_path / "ready"
    ledger = tmp_path / "collector" / "ready-publications.json"
    _draft(tmp_path / "draft", (100,), start_sequence=100)
    _draft(tmp_path / "draft", (200,), start_sequence=101)
    published = ready_bundles.finalize_drafts(tmp_path / "draft", ready_root, ledger, ClockSegmentMap(()))
    valid = (published[0].bundle_id, published[0].records_sha256)
    forged = ("f" * 64, "e" * 64)
    checkpoint = _checkpoint(tmp_path, [valid, forged])
    before = ledger.read_bytes()

    with pytest.raises(ready_bundles.ReadyBundleError, match="published ready bundle"):
        ready_bundles.retire_acknowledged(ready_root, ledger, checkpoint)

    assert ledger.read_bytes() == before
    assert all(item.path.exists() for item in published)


def test_nonfinite_ready_utc_mapping_blocks_ack_retirement_without_mutation(tmp_path: Path) -> None:
    ready_root = tmp_path / "ready"
    ledger = tmp_path / "collector" / "ready-publications.json"
    _draft(tmp_path / "draft", (100,), start_sequence=100)
    published = ready_bundles.finalize_drafts(tmp_path / "draft", ready_root, ledger, ClockSegmentMap(()))[0]
    manifest_path = published.path / "manifest.json"
    manifest = cast(dict[str, object], loads(manifest_path.read_text(encoding="utf-8")))
    ranges = cast(list[dict[str, object]], manifest["time_ranges"])
    ranges[0]["utc"] = {"observation_id": "observation", "offset_seconds": float("nan"), "uncertainty_seconds": 0.5}
    manifest_path.write_text(dumps(manifest), encoding="utf-8")
    checkpoint = _checkpoint(tmp_path, [(published.bundle_id, published.records_sha256)])
    before = ledger.read_bytes()

    with pytest.raises(ready_bundles.ReadyBundleError, match="UTC mapping"):
        ready_bundles.retire_acknowledged(ready_root, ledger, checkpoint)

    assert ledger.read_bytes() == before
    assert published.path.exists()


def test_checkpoint_without_ack_does_not_delete_ready_bundle(tmp_path: Path) -> None:
    ready_root = tmp_path / "ready"
    ledger = tmp_path / "collector" / "ready-publications.json"
    _draft(tmp_path / "draft", (100,), start_sequence=100)
    published = ready_bundles.finalize_drafts(tmp_path / "draft", ready_root, ledger, ClockSegmentMap(()))[0]
    checkpoint = _checkpoint(tmp_path, [])

    assert ready_bundles.retire_acknowledged(ready_root, ledger, checkpoint) == ()
    assert published.path.exists()
