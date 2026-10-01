"""Ready finalization keeps draft bytes authoritative until publication is durable."""

from __future__ import annotations

from dataclasses import replace
from hashlib import sha256
from json import dumps, loads
from os import utime
from pathlib import Path
from stat import S_IMODE, S_ISGID
from typing import cast

import pytest

from omi_collector.capture.adapters import ready_bundles, ready_closures
from omi_collector.capture.adapters.bundle_contract import BundleManifest, SealedReceipt
from omi_collector.capture.adapters.clock_segments import ClockSegment, ClockSegmentMap
from omi_collector.capture.adapters.staging_store import StagingStore
from omi_collector.capture.domain.ring_protocol import RECORD_SIZE
from omi_collector.config import DEFAULT_CONFIG, ReadyConfig

_finalize_impl = ready_bundles.finalize_drafts


def _finalize_drafts(*args: object, **kwargs: object) -> tuple[ready_bundles.ReadyBundleResult, ...]:
    kwargs.setdefault("config", ReadyConfig(target_audio_seconds=0.02))
    if "frontier" not in kwargs and args:
        kwargs["frontier"] = ready_bundles.draft_frontier(args[0])  # type: ignore[arg-type]
    kwargs.setdefault("drained", kwargs["frontier"] is not None)
    return _finalize_impl(*args, **kwargs)  # type: ignore[arg-type]


def _record(timestamp: int, marker: int) -> bytes:
    return timestamp.to_bytes(4, "big") + bytes((2, 8, marker)) + bytes(RECORD_SIZE - 7)


def _draft(root: Path, timestamps: tuple[int, ...], *, start_sequence: int = 10, marker_start: int = 1) -> Path:
    ready = root.parent / "ready"
    ready.mkdir(mode=0o2750, exist_ok=True)
    ready.chmod(0o2750)
    raw = b"".join(_record(timestamp, marker_start + index) for index, timestamp in enumerate(timestamps))
    digest = sha256(raw).hexdigest()
    path = root / f"{start_sequence}-{start_sequence + len(timestamps)}-{digest[:16]}"
    path.mkdir(parents=True)
    (path / "records.bin").write_bytes(raw)
    manifest = BundleManifest(2, start_sequence, start_sequence + len(timestamps), len(timestamps), RECORD_SIZE, digest)
    (path / "manifest.json").write_text(dumps(manifest.as_dict()), encoding="utf-8")
    (path / "receipt.json").write_text(dumps(SealedReceipt("a" * 32, digest).as_dict()), encoding="utf-8")
    return path


def _audio_draft(root: Path, *, sequence: int, timestamp: int = 100) -> Path:
    ready = root.parent / "ready"
    ready.mkdir(mode=0o2750, exist_ok=True)
    ready.chmod(0o2750)
    record = timestamp.to_bytes(4, "big") + bytes((2, 8, 0x55)) + bytes(RECORD_SIZE - 7)
    digest = sha256(record).hexdigest()
    path = root / f"{sequence}-{sequence + 1}-{digest[:16]}"
    path.mkdir(parents=True)
    (path / "records.bin").write_bytes(record)
    manifest = BundleManifest(2, sequence, sequence + 1, 1, RECORD_SIZE, digest)
    (path / "manifest.json").write_text(dumps(manifest.as_dict()), encoding="utf-8")
    (path / "receipt.json").write_text(dumps(SealedReceipt("a" * 32, digest).as_dict()), encoding="utf-8")
    return path


def test_drained_frontier_publishes_all_accumulated_audio_in_one_bundle(tmp_path: Path) -> None:
    draft_root = tmp_path / "draft"
    first = _audio_draft(draft_root, sequence=10)
    second = _audio_draft(draft_root, sequence=11)
    remainder = _audio_draft(draft_root, sequence=12)
    config = ReadyConfig(target_audio_seconds=0.039, max_wait_seconds=86400)

    first_result = _finalize_drafts(
        draft_root, tmp_path / "ready", tmp_path / "ledger.json", ClockSegmentMap(()), config=config
    )

    assert len(first_result) == 1
    manifest = cast(dict[str, object], loads((first_result[0].path / "manifest.json").read_text()))
    assert manifest["start_sequence"] == 10
    assert manifest["next_sequence"] == 13
    assert first.exists() is False
    assert second.exists() is False
    assert remainder.exists() is False


def test_target_without_a_drained_frontier_never_publishes(tmp_path: Path) -> None:
    draft_root = tmp_path / "draft"
    draft = _audio_draft(draft_root, sequence=10)

    result = _finalize_impl(
        draft_root,
        tmp_path / "ready",
        tmp_path / "ledger.json",
        ClockSegmentMap(()),
        config=ReadyConfig(target_audio_seconds=0.02, max_wait_seconds=0.001),
    )

    assert result == ()
    assert draft.exists()


def test_drain_without_a_frontier_is_rejected_before_publication(tmp_path: Path) -> None:
    draft_root = tmp_path / "draft"
    draft = _audio_draft(draft_root, sequence=10)

    with pytest.raises(ready_bundles.ReadyBundleError, match="requires its durable frontier"):
        _finalize_impl(
            draft_root,
            tmp_path / "ready",
            tmp_path / "ledger.json",
            ClockSegmentMap(()),
            config=ReadyConfig(target_audio_seconds=0.02),
            drained=True,
        )

    assert draft.exists()


def test_closed_frontier_publishes_all_eligible_contiguous_drafts_and_keeps_future_drafts(tmp_path: Path) -> None:
    draft_root = tmp_path / "draft"
    first = _audio_draft(draft_root, sequence=10)
    second = _audio_draft(draft_root, sequence=11)
    future = _audio_draft(draft_root, sequence=12)

    result = _finalize_drafts(
        draft_root,
        tmp_path / "ready",
        tmp_path / "ledger.json",
        ClockSegmentMap(()),
        config=ReadyConfig(target_audio_seconds=0.02, max_wait_seconds=0.01),
        frontier=12,
    )

    assert [(item.next_sequence - item.record_count, item.next_sequence) for item in result] == [(10, 12)]
    assert not first.exists() and not second.exists()
    assert future.exists()


def test_closed_frontier_does_not_cleanup_replay_overlap_from_a_future_draft(tmp_path: Path) -> None:
    draft_root = tmp_path / "draft"
    _audio_draft(draft_root, sequence=10)
    ready_root = tmp_path / "ready"
    ledger = tmp_path / "ledger.json"
    _finalize_drafts(draft_root, ready_root, ledger, ClockSegmentMap(()), frontier=11)
    future = _audio_draft(draft_root, sequence=10)

    _finalize_drafts(draft_root, ready_root, ledger, ClockSegmentMap(()), frontier=10)

    assert future.exists()


def test_sequence_gap_rejects_publication_without_consuming_drafts(tmp_path: Path) -> None:
    draft_root = tmp_path / "draft"
    first = _audio_draft(draft_root, sequence=10)
    second = _audio_draft(draft_root, sequence=12)

    with pytest.raises(ready_bundles.ReadyBundleError, match="contains a gap"):
        _finalize_drafts(
            draft_root,
            tmp_path / "ready",
            tmp_path / "ledger.json",
            ClockSegmentMap(()),
            config=ReadyConfig(target_audio_seconds=0.02, max_wait_seconds=0.001),
        )

    assert first.exists() and second.exists()


def test_max_wait_does_not_flush_below_threshold_and_clock_segments_are_preserved(tmp_path: Path) -> None:
    draft_root = tmp_path / "draft"
    first = _audio_draft(draft_root, sequence=10, timestamp=100)
    second = _audio_draft(draft_root, sequence=11, timestamp=200)
    for path in (first, second):
        utime(path / "manifest.json", (1, 1))
    segments = ClockSegmentMap((ClockSegment("clock", 11, 12, 0.6, 0.1),))

    result = _finalize_drafts(
        draft_root,
        tmp_path / "ready",
        tmp_path / "ledger.json",
        segments,
        config=ReadyConfig(target_audio_seconds=60, max_wait_seconds=1),
    )

    assert result == ()
    assert first.exists() and second.exists()


def test_drafts_accumulate_across_drained_visits_and_publish_after_restart(tmp_path: Path) -> None:
    draft_root = tmp_path / "draft"
    ready_root = tmp_path / "ready"
    store = StagingStore.from_paths(
        StagingStore(tmp_path / "collector", draft_root).paths,
        publication_root=ready_root,
        config=replace(DEFAULT_CONFIG, ready=ReadyConfig(target_audio_seconds=0.04, max_wait_seconds=0.001)),
    )
    first = _audio_draft(draft_root, sequence=10)
    store.append_ready_closure(11, "drained")

    assert store.recover_and_publish() is None
    assert first.exists()

    _audio_draft(draft_root, sequence=11)
    store.append_ready_closure(12, "drained")
    restarted = StagingStore.from_paths(
        StagingStore(tmp_path / "collector", draft_root).paths,
        publication_root=ready_root,
        config=replace(DEFAULT_CONFIG, ready=ReadyConfig(target_audio_seconds=0.04, max_wait_seconds=0.001)),
    )
    result = cast(tuple[ready_bundles.ReadyBundleResult, ...], restarted.recover_and_publish())

    assert len(result) == 1
    assert result[0].record_count == 2
    assert not first.exists()
    assert loads(restarted.ready_closures_path.read_text())["closures"] == []


def test_interrupted_new_nondrained_closure_blocks_older_drained_frontier(tmp_path: Path) -> None:
    draft_root = tmp_path / "draft"
    ready_root = tmp_path / "ready"
    store = StagingStore.from_paths(
        StagingStore(tmp_path / "collector", draft_root).paths,
        publication_root=ready_root,
        config=replace(DEFAULT_CONFIG, ready=ReadyConfig(target_audio_seconds=0.02)),
    )
    _audio_draft(draft_root, sequence=10)
    store.append_ready_closure(11, "drained")
    store.append_ready_closure(12, "restart_interrupted")

    assert store.recover_and_publish() is None
    assert not tuple(ready_root.iterdir())
    assert ready_closures.load(store.ready_closures_path) == (ready_closures.ReadyClosure(12, "restart_interrupted"),)


def test_group_recovery_after_ledger_write_finishes_source_cleanup(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    draft_root = tmp_path / "draft"
    first = _audio_draft(draft_root, sequence=10)
    second = _audio_draft(draft_root, sequence=11)
    config = ReadyConfig(target_audio_seconds=0.039, max_wait_seconds=86400)
    remove = ready_bundles._remove_draft
    monkeypatch.setattr(ready_bundles, "_remove_draft", lambda _: (_ for _ in ()).throw(OSError("crash")))

    with pytest.raises(OSError, match="crash"):
        _finalize_drafts(draft_root, tmp_path / "ready", tmp_path / "ledger.json", ClockSegmentMap(()), config=config)
    monkeypatch.setattr(ready_bundles, "_remove_draft", remove)
    result = _finalize_drafts(
        draft_root, tmp_path / "ready", tmp_path / "ledger.json", ClockSegmentMap(()), config=config
    )

    assert result == ()
    assert not first.exists() and not second.exists()
    assert len(tuple((tmp_path / "ready").iterdir())) == 1


def _checkpoint(
    root: Path,
    identities: list[tuple[str, str]],
    *,
    tail: list[tuple[str, str]] | None = None,
    decisions: list[tuple[str, str]] | None = None,
) -> Path:
    def decision(bundle_id: str, records_sha256: str, *, pending: bool = False) -> dict[str, object]:
        return {
            "bundle_id": bundle_id,
            "records_sha256": records_sha256,
            "packet_ranges": [{"packet_start": 0, "packet_next": 1}] if pending else [],
            "no_speech_packet_ranges": [],
            "packet_count": 1 if pending else 0,
            "input_id": "c" * 64 if pending else None,
            "input_sha256": "d" * 64 if pending else None,
            "receipt_sha256": "e" * 64 if pending else None,
        }

    path = root / "work" / "omi-ready-checkpoint.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    decision_entries = [decision(*identity) for identity in identities if decisions is None] + [
        decision(*identity) for identity in decisions or []
    ]
    path.write_text(
        dumps(
            {
                "analysis_cursor": decision_entries[-1] if decision_entries else None,
                "vad_decisions": decision_entries,
                "open_speech_tail": (
                    {"entries": [decision(*identity, pending=True) for identity in tail], "opened_at": 1, "outputs": []}
                    if tail
                    else None
                ),
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

    result = _finalize_drafts(tmp_path / "draft", tmp_path / "ready", tmp_path / "ledger.json", segments)

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


def test_finalization_refuses_ready_root_without_group_traversal(tmp_path: Path) -> None:
    _draft(tmp_path / "draft", (100,))
    (tmp_path / "ready").chmod(0o750)

    with pytest.raises(ready_bundles.ReadyBundleError, match="not group-readable and setgid"):
        _finalize_drafts(tmp_path / "draft", tmp_path / "ready", tmp_path / "ledger.json", ClockSegmentMap(()))


def test_existing_ready_bundle_completes_crash_replay_before_draft_cleanup(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    draft = _draft(tmp_path / "draft", (100,))
    ledger = tmp_path / "collector" / "ready-publications.json"

    monkeypatch.setattr(ready_bundles, "_record_ready", lambda *_: (_ for _ in ()).throw(OSError("interrupted")))
    with pytest.raises(OSError, match="interrupted"):
        _finalize_drafts(tmp_path / "draft", tmp_path / "ready", ledger, ClockSegmentMap(()))
    assert draft.exists()
    assert len(tuple((tmp_path / "ready").iterdir())) == 1

    monkeypatch.undo()
    result = _finalize_drafts(tmp_path / "draft", tmp_path / "ready", ledger, ClockSegmentMap(()))

    assert result == ()
    assert not draft.exists()
    manifest = cast(
        dict[str, object], loads(next((tmp_path / "ready").iterdir()).joinpath("manifest.json").read_text())
    )
    assert (
        loads(ledger.read_text(encoding="utf-8"))["bundles"][manifest["bundle_id"]]["records_sha256"]
        == manifest["records_sha256"]
    )


def test_unledgered_ready_reconciles_before_overlap_and_can_later_retire(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    ledger = tmp_path / "collector" / "ready-publications.json"
    ready_root = tmp_path / "ready"
    _draft(tmp_path / "draft", tuple(range(10)), start_sequence=100)
    monkeypatch.setattr(ready_bundles, "_record_ready", lambda *_: (_ for _ in ()).throw(OSError("interrupted")))
    with pytest.raises(OSError, match="interrupted"):
        _finalize_drafts(tmp_path / "draft", ready_root, ledger, ClockSegmentMap(()))
    monkeypatch.undo()
    _draft(tmp_path / "draft", tuple(range(15)), start_sequence=100)

    _finalize_drafts(tmp_path / "draft", ready_root, ledger, ClockSegmentMap(()))

    original_path = next(
        path for path in ready_root.iterdir() if (path / "records.bin").stat().st_size == 10 * RECORD_SIZE
    )
    original_manifest = cast(dict[str, object], loads((original_path / "manifest.json").read_text()))
    state = cast(dict[str, object], loads(ledger.read_text(encoding="utf-8")))
    bundles = cast(dict[str, dict[str, object]], state["bundles"])
    original_id = cast(str, original_manifest["bundle_id"])
    original_hash = cast(str, original_manifest["records_sha256"])
    assert bundles[original_id]["state"] == "ready"
    checkpoint = _checkpoint(tmp_path, [(original_id, original_hash)])
    ready_bundles.retire_acknowledged(ready_root, ledger, checkpoint)
    assert not original_path.exists()


def test_unknown_time_finalizes_without_changing_device_timestamps(tmp_path: Path) -> None:
    draft = _draft(tmp_path / "draft", (100, 101))
    original = (draft / "records.bin").read_bytes()

    result = _finalize_drafts(tmp_path / "draft", tmp_path / "ready", tmp_path / "ledger.json", ClockSegmentMap(()))

    assert (result[0].path / "records.bin").read_bytes() == original


def test_timestamp_overflow_publishes_unchanged_unknown_range(tmp_path: Path) -> None:
    draft = _draft(tmp_path / "draft", ((1 << 32) - 1, 100))
    original = (draft / "records.bin").read_bytes()

    result = _finalize_drafts(
        tmp_path / "draft",
        tmp_path / "ready",
        tmp_path / "ledger.json",
        ClockSegmentMap((ClockSegment("observation", 10, 12, 1.0, 0.1),)),
    )

    ready = result[0].path
    assert (ready / "records.bin").read_bytes() == original
    manifest = cast(dict[str, object], loads((ready / "manifest.json").read_text(encoding="utf-8")))
    assert manifest["time_ranges"] == [{"start_sequence": 10, "next_sequence": 12, "utc": None}]
    assert not draft.exists()


def test_approximate_clock_range_is_marked_in_ready_manifest(tmp_path: Path) -> None:
    _draft(tmp_path / "draft", (100,))
    segments = ClockSegmentMap((ClockSegment("observation", 10, 11, 0.6, 0.1, "approximate"),))

    result = _finalize_drafts(tmp_path / "draft", tmp_path / "ready", tmp_path / "ledger.json", segments)

    manifest = cast(dict[str, object], loads((result[0].path / "manifest.json").read_text(encoding="utf-8")))
    assert manifest["time_ranges"] == [
        {
            "start_sequence": 10,
            "next_sequence": 11,
            "utc": {
                "observation_id": "observation",
                "offset_seconds": 0.6,
                "uncertainty_seconds": 0.1,
                "confidence": "approximate",
            },
        }
    ]


def test_replayed_published_bundle_keeps_its_original_time_mapping(tmp_path: Path) -> None:
    draft_root = tmp_path / "draft"
    ready_root = tmp_path / "ready"
    ledger = tmp_path / "ledger.json"
    _draft(draft_root, (100,), start_sequence=100)
    first = _finalize_drafts(
        draft_root, ready_root, ledger, ClockSegmentMap((ClockSegment("old", 100, 101, 1.0, 1.1),))
    )[0]
    original_manifest = (first.path / "manifest.json").read_bytes()
    original_records = (first.path / "records.bin").read_bytes()
    _draft(draft_root, (100,), start_sequence=100)

    replay = _finalize_drafts(
        draft_root, ready_root, ledger, ClockSegmentMap((ClockSegment("new", 100, 101, 50.0, 1.1),))
    )

    assert replay == ()
    assert (first.path / "manifest.json").read_bytes() == original_manifest
    assert (first.path / "records.bin").read_bytes() == original_records


def test_contained_replay_uses_ready_payloads_without_creating_a_second_bundle(tmp_path: Path) -> None:
    start = 8_942_873
    ready_root = tmp_path / "ready"
    ledger = tmp_path / "collector" / "ready-publications.json"
    _draft(tmp_path / "draft", tuple(range(30)), start_sequence=start)
    _finalize_drafts(tmp_path / "draft", ready_root, ledger, ClockSegmentMap(()))
    duplicate = _draft(tmp_path / "draft", tuple(range(5, 25)), start_sequence=start + 5, marker_start=6)

    result = _finalize_drafts(tmp_path / "draft", ready_root, ledger, ClockSegmentMap(()))

    assert result == ()
    assert not duplicate.exists()
    assert len(tuple(ready_root.iterdir())) == 1


def test_replay_prefix_publishes_only_its_unique_suffix(tmp_path: Path) -> None:
    ready_root = tmp_path / "ready"
    ledger = tmp_path / "collector" / "ready-publications.json"
    _draft(tmp_path / "draft", tuple(range(10)), start_sequence=100)
    _finalize_drafts(tmp_path / "draft", ready_root, ledger, ClockSegmentMap(()))
    _draft(tmp_path / "draft", tuple(range(15)), start_sequence=100)

    result = _finalize_drafts(tmp_path / "draft", ready_root, ledger, ClockSegmentMap(()))

    assert [(item.next_sequence - item.record_count, item.next_sequence) for item in result] == [(110, 115)]
    suffix = result[0].path / "records.bin"
    assert suffix.read_bytes()[6] == 11
    assert tuple(tmp_path.joinpath("draft").iterdir()) == ()


def test_conflicting_ready_overlap_keeps_draft_and_fails_closed(tmp_path: Path) -> None:
    ready_root = tmp_path / "ready"
    ledger = tmp_path / "collector" / "ready-publications.json"
    _draft(tmp_path / "draft", tuple(range(10)), start_sequence=100)
    _finalize_drafts(tmp_path / "draft", ready_root, ledger, ClockSegmentMap(()))
    draft = _draft(tmp_path / "draft", tuple(range(15)), start_sequence=100, marker_start=99)

    with pytest.raises(ready_bundles.ReadyBundleError, match="conflicts with original payload"):
        _finalize_drafts(tmp_path / "draft", ready_root, ledger, ClockSegmentMap(()))

    assert draft.exists()
    assert len(tuple(ready_root.iterdir())) == 1


def test_sequence_reuse_without_stream_epoch_is_never_silently_suppressed(tmp_path: Path) -> None:
    ready_root = tmp_path / "ready"
    ledger = tmp_path / "collector" / "ready-publications.json"
    _draft(tmp_path / "draft", tuple(range(10)), start_sequence=100)
    _finalize_drafts(tmp_path / "draft", ready_root, ledger, ClockSegmentMap(()))
    reset = _draft(tmp_path / "draft", tuple(range(10)), start_sequence=100, marker_start=99)

    with pytest.raises(ready_bundles.ReadyBundleError, match="conflicts with original payload"):
        _finalize_drafts(tmp_path / "draft", ready_root, ledger, ClockSegmentMap(()))

    assert reset.exists()


def test_sequence_gap_does_not_split_accumulated_audio(tmp_path: Path) -> None:
    for start in (100, 110, 120, 130):
        _draft(tmp_path / "draft", (start,), start_sequence=start)

    with pytest.raises(ready_bundles.ReadyBundleError, match="contains a gap"):
        _finalize_drafts(tmp_path / "draft", tmp_path / "ready", tmp_path / "ledger.json", ClockSegmentMap(()))

    assert len(tuple((tmp_path / "draft").iterdir())) == 4
    assert not tuple((tmp_path / "ready").iterdir())


def test_retired_exact_replay_is_removed_but_partial_retired_overlap_fails_closed(tmp_path: Path) -> None:
    ready_root = tmp_path / "ready"
    ledger = tmp_path / "collector" / "ready-publications.json"
    _draft(tmp_path / "draft", tuple(range(10)), start_sequence=100)
    published = _finalize_drafts(tmp_path / "draft", ready_root, ledger, ClockSegmentMap(()))[0]
    checkpoint = _checkpoint(tmp_path, [(published.bundle_id, published.records_sha256)])
    ready_bundles.retire_acknowledged(ready_root, ledger, checkpoint)
    exact = _draft(tmp_path / "draft", tuple(range(10)), start_sequence=100)

    assert _finalize_drafts(tmp_path / "draft", ready_root, ledger, ClockSegmentMap(())) == ()
    assert not exact.exists()

    partial = _draft(tmp_path / "draft", tuple(range(5)), start_sequence=105, marker_start=6)
    with pytest.raises(ready_bundles.ReadyBundleError, match="retired range"):
        _finalize_drafts(tmp_path / "draft", ready_root, ledger, ClockSegmentMap(()))
    assert partial.exists()


def test_ack_retirement_records_ledger_before_unlink_and_recovers_after_each_boundary(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    ready_root = tmp_path / "ready"
    ledger = tmp_path / "collector" / "ready-publications.json"
    _draft(tmp_path / "draft", (100,), start_sequence=100)
    published = _finalize_drafts(tmp_path / "draft", ready_root, ledger, ClockSegmentMap(()))[0]
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


def test_new_closure_recovers_retirement_committed_before_unlink(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    drafts = tmp_path / "draft"
    ready = tmp_path / "ready"
    ledger = tmp_path / "collector" / "ready-publications.json"
    _draft(drafts, (100,), start_sequence=100)
    old = _finalize_drafts(drafts, ready, ledger, ClockSegmentMap(()))[0]
    checkpoint = _checkpoint(tmp_path, [(old.bundle_id, old.records_sha256)])
    original_remove = ready_bundles._remove_retired_ready

    def interrupt_retirement(_path: Path) -> None:
        raise OSError("after retirement ledger commit")

    monkeypatch.setattr(ready_bundles, "_remove_retired_ready", interrupt_retirement)
    with pytest.raises(OSError, match="after retirement ledger commit"):
        ready_bundles.retire_acknowledged(ready, ledger, checkpoint)
    monkeypatch.setattr(ready_bundles, "_remove_retired_ready", original_remove)
    checkpoint.unlink()
    _draft(drafts, (101,), start_sequence=101)
    open_draft = _draft(drafts, (102,), start_sequence=102)
    store = StagingStore.from_paths(
        StagingStore(tmp_path / "collector", drafts).paths,
        publication_root=ready,
        config=replace(DEFAULT_CONFIG, ready=ReadyConfig(target_audio_seconds=0.02)),
    )
    store.append_ready_closure(102, "drained")

    published = cast(tuple[ready_bundles.ReadyBundleResult, ...], store.recover_and_publish())

    assert [item.next_sequence for item in published] == [102]
    assert not old.path.exists()
    assert open_draft.exists()
    assert loads(ledger.read_text())["bundles"][old.bundle_id]["state"] == "retired"
    assert store.recover_and_publish() is None
    assert open_draft.exists()


def test_forged_or_open_tail_ack_never_deletes_ready_bundle(tmp_path: Path) -> None:
    ready_root = tmp_path / "ready"
    ledger = tmp_path / "collector" / "ready-publications.json"
    _draft(tmp_path / "draft", (100,), start_sequence=100)
    published = _finalize_drafts(tmp_path / "draft", ready_root, ledger, ClockSegmentMap(()))[0]
    forged = _checkpoint(tmp_path, [(published.bundle_id, "b" * 64)])

    with pytest.raises(ready_bundles.ReadyBundleError, match="published ready bundle"):
        ready_bundles.retire_acknowledged(ready_root, ledger, forged)
    assert published.path.exists()

    tail = _checkpoint(
        tmp_path,
        [(published.bundle_id, published.records_sha256)],
        tail=[(published.bundle_id, published.records_sha256)],
    )
    with pytest.raises(ready_bundles.ReadyBundleError, match="pending tail"):
        ready_bundles.retire_acknowledged(ready_root, ledger, tail)
    assert published.path.exists()


def test_ack_identity_retires_exact_bundle_without_reading_decision_contents(tmp_path: Path) -> None:
    ready_root = tmp_path / "ready"
    ledger = tmp_path / "collector" / "ready-publications.json"
    _draft(tmp_path / "draft", (100,), start_sequence=100)
    exact = _finalize_drafts(tmp_path / "draft", ready_root, ledger, ClockSegmentMap(()))[0]
    checkpoint = _checkpoint(tmp_path, [(exact.bundle_id, exact.records_sha256)], decisions=[])

    ready_bundles.retire_acknowledged(ready_root, ledger, checkpoint)
    assert not exact.path.exists()

    _draft(tmp_path / "draft", (101,), start_sequence=101)
    mismatch = _finalize_drafts(tmp_path / "draft", ready_root, ledger, ClockSegmentMap(()))[0]
    checkpoint = _checkpoint(tmp_path, [(mismatch.bundle_id, "b" * 64)], decisions=[])

    with pytest.raises(ready_bundles.ReadyBundleError, match="published ready bundle"):
        ready_bundles.retire_acknowledged(ready_root, ledger, checkpoint)
    assert mismatch.path.exists()


def test_mixed_ack_batch_preflights_every_identity_before_retiring_any_bundle(tmp_path: Path) -> None:
    ready_root = tmp_path / "ready"
    ledger = tmp_path / "collector" / "ready-publications.json"
    _draft(tmp_path / "draft", (100,), start_sequence=100)
    _draft(tmp_path / "draft", (200,), start_sequence=101)
    published = _finalize_drafts(tmp_path / "draft", ready_root, ledger, ClockSegmentMap(()))
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
    published = _finalize_drafts(tmp_path / "draft", ready_root, ledger, ClockSegmentMap(()))[0]
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
    published = _finalize_drafts(tmp_path / "draft", ready_root, ledger, ClockSegmentMap(()))[0]
    checkpoint = _checkpoint(tmp_path, [])

    assert ready_bundles.retire_acknowledged(ready_root, ledger, checkpoint) == ()
    assert published.path.exists()


def test_ready_output_preserves_setgid_parent_without_setting_it_at_runtime(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _draft(tmp_path / "draft", (100,))
    ready_root = tmp_path / "ready"
    ready_root.chmod(0o2750)
    chmod = Path.chmod

    def restricted_chmod(path: Path, mode: int, *args: object, **kwargs: object) -> None:
        if mode & S_ISGID:
            raise PermissionError("runtime cannot set setgid")
        chmod(path, mode, *args, **kwargs)

    monkeypatch.setattr(Path, "chmod", restricted_chmod)

    published = _finalize_drafts(tmp_path / "draft", ready_root, tmp_path / "ledger.json", ClockSegmentMap(()))[0]

    assert S_IMODE(ready_root.stat().st_mode) == 0o2750
    assert S_IMODE(published.path.stat().st_mode) == 0o2750
    assert all(S_IMODE(path.stat().st_mode) == 0o640 for path in published.path.iterdir())
