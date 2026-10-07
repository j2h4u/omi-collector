"""Ready finalization keeps draft bytes authoritative until publication is durable."""

from __future__ import annotations

from dataclasses import replace
from hashlib import sha256
from json import dumps, loads
from os import utime
from pathlib import Path
from shutil import copytree
from stat import S_IMODE, S_ISGID
from typing import cast

import pytest

from omi_collector.capture.adapters import ready_bundles, ready_closures
from omi_collector.capture.adapters.bundle_contract import BundleManifest, SealedReceipt
from omi_collector.capture.adapters.clock_segments import ClockSegment, ClockSegmentMap
from omi_collector.capture.adapters.staging_store import StagingStore
from omi_collector.capture.domain.ring_protocol import RECORD_SIZE
from omi_collector.config import DEFAULT_CONFIG, ReadyConfig
from test_publication_fsm import _count_raw_reads

_finalize_impl = ready_bundles.finalize_drafts


def _finalize_drafts(*args: object, **kwargs: object) -> tuple[ready_bundles.ReadyBundleResult, ...]:
    kwargs.setdefault("config", ReadyConfig(target_audio_seconds=0.02))
    if "frontier" not in kwargs and args:
        kwargs["frontier"] = ready_bundles.draft_frontier(args[0])  # type: ignore[arg-type]
    kwargs.setdefault("drained", kwargs["frontier"] is not None)
    return _finalize_impl(*args, **kwargs).published  # type: ignore[arg-type]


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
    config = ReadyConfig(target_audio_seconds=0.039)

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
        config=ReadyConfig(target_audio_seconds=0.02),
    )

    assert result.state is ready_bundles.ReadyOutcomeState.WAITING
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
        config=ReadyConfig(target_audio_seconds=0.02),
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


def test_sequence_gap_publishes_actual_records_and_canonical_ledger_geometry(tmp_path: Path) -> None:
    draft_root = tmp_path / "draft"
    first = _audio_draft(draft_root, sequence=10)
    second = _audio_draft(draft_root, sequence=12)

    outcome = _finalize_impl(
        draft_root,
        tmp_path / "ready",
        tmp_path / "ledger.json",
        ClockSegmentMap(()),
        config=ReadyConfig(target_audio_seconds=0.02),
        frontier=13,
        drained=True,
    )

    assert outcome.state is ready_bundles.ReadyOutcomeState.PUBLISHED
    (published,) = outcome.published
    manifest = cast(dict[str, object], loads((published.path / "manifest.json").read_text(encoding="utf-8")))
    ranges = cast(list[dict[str, object]], manifest["time_ranges"])
    assert [(item["start_sequence"], item["next_sequence"]) for item in ranges] == [(10, 11), (12, 13)]
    assert published.record_count == 2
    expected_id = sha256(f"10:13:{manifest['draft_raw_sha256']}:11-12".encode()).hexdigest()
    assert published.bundle_id == expected_id
    entries = cast(
        dict[str, dict[str, object]], loads((tmp_path / "ledger.json").read_text(encoding="utf-8"))["bundles"]
    )
    entry = entries[expected_id]
    assert entry["gaps"] == [[11, 12]]
    assert not first.exists() and not second.exists()


def test_below_threshold_waits_and_clock_segments_are_preserved(tmp_path: Path) -> None:
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
        config=ReadyConfig(target_audio_seconds=60),
    )

    assert result == ()
    assert first.exists() and second.exists()


def test_drafts_accumulate_across_drained_visits_and_publish_after_restart(tmp_path: Path) -> None:
    draft_root = tmp_path / "draft"
    ready_root = tmp_path / "ready"
    store = StagingStore.from_paths(
        StagingStore(tmp_path / "collector", draft_root).paths,
        publication_root=ready_root,
        config=replace(DEFAULT_CONFIG, ready=ReadyConfig(target_audio_seconds=0.04)),
    )
    first = _audio_draft(draft_root, sequence=10)
    store.append_ready_closure(11, "drained")

    assert store.recover_and_publish().state is ready_bundles.ReadyOutcomeState.WAITING
    assert first.exists()

    _audio_draft(draft_root, sequence=11)
    store.append_ready_closure(12, "drained")
    restarted = StagingStore.from_paths(
        StagingStore(tmp_path / "collector", draft_root).paths,
        publication_root=ready_root,
        config=replace(DEFAULT_CONFIG, ready=ReadyConfig(target_audio_seconds=0.04)),
    )
    result = restarted.recover_and_publish().published

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

    assert store.recover_and_publish().state is ready_bundles.ReadyOutcomeState.WAITING
    assert not tuple(ready_root.iterdir())
    assert ready_closures.load(store.ready_closures_path) == (ready_closures.ReadyClosure(12, "restart_interrupted"),)


def test_group_recovery_after_ledger_write_finishes_source_cleanup(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    draft_root = tmp_path / "draft"
    first = _audio_draft(draft_root, sequence=10)
    second = _audio_draft(draft_root, sequence=11)
    config = ReadyConfig(target_audio_seconds=0.039)
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


def _published_bundle_with_valid_ack(
    root: Path, *, timestamps: tuple[int, ...] = (100,)
) -> tuple[Path, Path, Path, ready_bundles.ReadyBundleResult]:
    draft_root = root / "draft"
    ready_root = root / "ready"
    ledger = root / "collector" / "ready-publications.json"
    _draft(draft_root, timestamps, start_sequence=100)
    (published,) = _finalize_drafts(draft_root, ready_root, ledger, ClockSegmentMap(()))
    checkpoint = _checkpoint(root, [(published.bundle_id, published.records_sha256)])
    return ready_root, ledger, checkpoint, published


def _retirement_snapshot(published: ready_bundles.ReadyBundleResult, ledger: Path) -> dict[str, bytes]:
    return {
        "records": (published.path / "records.bin").read_bytes(),
        "manifest": (published.path / "manifest.json").read_bytes(),
        "ledger": ledger.read_bytes(),
    }


def _persisted_ready_bundle(
    ready_root: Path,
    manifest: dict[str, object],
    records: bytes,
) -> Path:
    """Write an on-disk ready contract, including cases rejected by the reader."""
    start_sequence = manifest["start_sequence"]
    next_sequence = manifest["next_sequence"]
    draft_raw_sha256 = manifest["draft_raw_sha256"]
    assert isinstance(next_sequence, int)
    assert isinstance(draft_raw_sha256, str)
    bundle_id = sha256(f"{start_sequence}:{next_sequence}:{draft_raw_sha256}".encode()).hexdigest()
    manifest.update(
        {
            "bundle_id": bundle_id,
            "record_size": RECORD_SIZE,
            "records_sha256": sha256(records).hexdigest(),
        }
    )
    path = ready_root / bundle_id
    path.mkdir()
    (path / "records.bin").write_bytes(records)
    (path / "manifest.json").write_text(dumps(manifest), encoding="utf-8")
    return path


def _assert_invalid_utc_mapping_is_not_retired(root: Path, utc: object) -> None:
    ready_root, ledger, checkpoint, published = _published_bundle_with_valid_ack(root)
    manifest_path = published.path / "manifest.json"
    manifest = cast(dict[str, object], loads(manifest_path.read_text(encoding="utf-8")))
    ranges = cast(list[dict[str, object]], manifest["time_ranges"])
    ranges[0]["utc"] = utc
    manifest_path.write_text(dumps(manifest), encoding="utf-8")
    before = _retirement_snapshot(published, ledger)

    with pytest.raises(ready_bundles.ReadyBundleError, match="UTC mapping"):
        ready_bundles.retire_acknowledged(ready_root, ledger, checkpoint)

    assert published.path.exists()
    assert _retirement_snapshot(published, ledger) == before


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


def test_overflow_preflight_keeps_only_the_unrepresentable_record_range_unknown(tmp_path: Path) -> None:
    draft_root = tmp_path / "draft"
    original = _draft(draft_root, (100, 200), start_sequence=10)
    original_records = (original / "records.bin").read_bytes()
    segments = ClockSegmentMap(
        (
            ClockSegment("safe", 10, 11, 0.0, 0.0),
            ClockSegment("underflow", 11, 12, -201.0, 0.0),
        )
    )

    (published,) = _finalize_drafts(draft_root, tmp_path / "ready", tmp_path / "ledger.json", segments)

    manifest = cast(dict[str, object], loads((published.path / "manifest.json").read_text(encoding="utf-8")))
    records = (published.path / "records.bin").read_bytes()
    assert manifest["time_ranges"] == [
        {
            "start_sequence": 10,
            "next_sequence": 11,
            "utc": {"observation_id": "safe", "offset_seconds": 0.0, "uncertainty_seconds": 0.0},
        },
        {"start_sequence": 11, "next_sequence": 12, "utc": None},
    ]
    assert [records[index : index + RECORD_SIZE] for index in range(0, len(records), RECORD_SIZE)] == [
        original_records[:RECORD_SIZE],
        original_records[RECORD_SIZE:],
    ]


@pytest.mark.parametrize(
    ("timestamp", "offset", "normalized", "has_utc"),
    ((100, -100.0, 0, True), (100, 4_294_967_195.0, 4_294_967_295, True), (100, -101.0, -1, False)),
)
def test_finalization_preserves_uint32_endpoints_and_clears_underflow_mapping(
    tmp_path: Path, timestamp: int, offset: float, normalized: int, has_utc: bool
) -> None:
    draft_root = tmp_path / "draft"
    original = _draft(draft_root, (timestamp,), start_sequence=10)
    original_record = (original / "records.bin").read_bytes()
    segments = ClockSegmentMap((ClockSegment("boundary", 10, 11, offset, 0.0),))

    (published,) = _finalize_drafts(draft_root, tmp_path / "ready", tmp_path / "ledger.json", segments)

    manifest = cast(dict[str, object], loads((published.path / "manifest.json").read_text(encoding="utf-8")))
    ready_record = (published.path / "records.bin").read_bytes()
    assert ready_record[:4] == (normalized if has_utc else timestamp).to_bytes(4, "big")
    assert ready_record[4:] == original_record[4:]
    time_range = cast(list[dict[str, object]], manifest["time_ranges"])[0]
    assert (time_range["start_sequence"], time_range["next_sequence"]) == (10, 11)
    assert (time_range["utc"] is not None) is has_utc


def test_time_ranges_ignore_segments_outside_the_draft_interval(tmp_path: Path) -> None:
    draft_root = tmp_path / "draft"
    _draft(draft_root, (100,), start_sequence=10)
    segments = ClockSegmentMap(
        (
            ClockSegment("before", 1, 5, 0.0, 0.0),
            ClockSegment("inside", 10, 11, 0.25, 0.1),
            ClockSegment("after", 20, 30, 0.0, 0.0),
        )
    )

    (published,) = _finalize_drafts(draft_root, tmp_path / "ready", tmp_path / "ledger.json", segments)

    manifest = cast(dict[str, object], loads((published.path / "manifest.json").read_text(encoding="utf-8")))
    assert manifest["time_ranges"] == [
        {
            "start_sequence": 10,
            "next_sequence": 11,
            "utc": {"observation_id": "inside", "offset_seconds": 0.25, "uncertainty_seconds": 0.1},
        }
    ]


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

    outcome = ready_bundles.finalize_drafts(
        tmp_path / "draft",
        tmp_path / "ready",
        tmp_path / "ledger.json",
        ClockSegmentMap(()),
        config=ReadyConfig(target_audio_seconds=0.02),
        frontier=131,
        drained=True,
    )
    assert outcome.state is ready_bundles.ReadyOutcomeState.PUBLISHED
    assert len(outcome.published) == 1
    assert outcome.published[0].record_count == 4
    assert not tuple((tmp_path / "draft").iterdir())


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

    published = store.recover_and_publish().published

    assert [item.next_sequence for item in published] == [102]
    assert not old.path.exists()
    assert open_draft.exists()
    assert loads(ledger.read_text())["bundles"][old.bundle_id]["state"] == "retired"
    assert store.recover_and_publish().state is ready_bundles.ReadyOutcomeState.WAITING
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


@pytest.mark.parametrize(
    "tail",
    [
        {"entries": [], "opened_at": True, "outputs": []},
        {"entries": [], "opened_at": "1", "outputs": []},
        {"entries": [], "opened_at": 1, "outputs": None},
        {"entries": None, "opened_at": 1, "outputs": []},
        {"entries": [None], "opened_at": 1, "outputs": []},
    ],
)
def test_malformed_open_tail_never_mutates_acknowledged_bundle(tmp_path: Path, tail: dict[str, object]) -> None:
    ready_root = tmp_path / "ready"
    ledger = tmp_path / "collector" / "ready-publications.json"
    _draft(tmp_path / "draft", (100,), start_sequence=100)
    published = _finalize_drafts(tmp_path / "draft", ready_root, ledger, ClockSegmentMap(()))[0]
    checkpoint = _checkpoint(tmp_path, [(published.bundle_id, published.records_sha256)])
    data = cast(dict[str, object], loads(checkpoint.read_text(encoding="utf-8")))
    data["open_speech_tail"] = tail
    checkpoint.write_text(dumps(data), encoding="utf-8")
    before = {
        "records": (published.path / "records.bin").read_bytes(),
        "manifest": (published.path / "manifest.json").read_bytes(),
        "ledger": ledger.read_bytes(),
    }

    with pytest.raises(ready_bundles.ReadyBundleError):
        ready_bundles.retire_acknowledged(ready_root, ledger, checkpoint)

    assert published.path.exists()
    assert (published.path / "records.bin").read_bytes() == before["records"]
    assert (published.path / "manifest.json").read_bytes() == before["manifest"]
    assert ledger.read_bytes() == before["ledger"]


def test_valid_empty_open_tail_allows_acknowledged_bundle_retirement(tmp_path: Path) -> None:
    ready_root = tmp_path / "ready"
    ledger = tmp_path / "collector" / "ready-publications.json"
    _draft(tmp_path / "draft", (100,), start_sequence=100)
    published = _finalize_drafts(tmp_path / "draft", ready_root, ledger, ClockSegmentMap(()))[0]
    checkpoint = _checkpoint(tmp_path, [(published.bundle_id, published.records_sha256)])
    data = cast(dict[str, object], loads(checkpoint.read_text(encoding="utf-8")))
    data["open_speech_tail"] = {"entries": [], "opened_at": 1, "outputs": []}
    checkpoint.write_text(dumps(data), encoding="utf-8")

    retired = ready_bundles.retire_acknowledged(ready_root, ledger, checkpoint)

    assert [item.bundle_id for item in retired] == [published.bundle_id]
    assert not published.path.exists()


@pytest.mark.parametrize("extra_fields", [False, True])
def test_matching_open_tail_identity_blocks_ack_even_with_extra_fields(tmp_path: Path, extra_fields: bool) -> None:
    ready_root = tmp_path / "ready"
    ledger = tmp_path / "collector" / "ready-publications.json"
    _draft(tmp_path / "draft", (100,), start_sequence=100)
    published = _finalize_drafts(tmp_path / "draft", ready_root, ledger, ClockSegmentMap(()))[0]
    checkpoint = _checkpoint(tmp_path, [(published.bundle_id, published.records_sha256)])
    data = cast(dict[str, object], loads(checkpoint.read_text(encoding="utf-8")))
    identity: dict[str, object] = {
        "bundle_id": published.bundle_id,
        "records_sha256": published.records_sha256,
    }
    if extra_fields:
        identity["annotation"] = "allowed by the subset contract"
    data["open_speech_tail"] = {"entries": [identity], "opened_at": 1, "outputs": []}
    checkpoint.write_text(dumps(data), encoding="utf-8")
    before = ledger.read_bytes()

    with pytest.raises(ready_bundles.ReadyBundleError):
        ready_bundles.retire_acknowledged(ready_root, ledger, checkpoint)

    assert published.path.exists()
    assert ledger.read_bytes() == before


def test_nonmatching_open_tail_identity_allows_ack_with_extra_fields(tmp_path: Path) -> None:
    ready_root = tmp_path / "ready"
    ledger = tmp_path / "collector" / "ready-publications.json"
    _draft(tmp_path / "draft", (100,), start_sequence=100)
    published = _finalize_drafts(tmp_path / "draft", ready_root, ledger, ClockSegmentMap(()))[0]
    checkpoint = _checkpoint(tmp_path, [(published.bundle_id, published.records_sha256)])
    data = cast(dict[str, object], loads(checkpoint.read_text(encoding="utf-8")))
    data["open_speech_tail"] = {
        "entries": [{"bundle_id": "f" * 64, "records_sha256": "e" * 64, "annotation": "unrelated"}],
        "opened_at": 1,
        "outputs": [],
    }
    checkpoint.write_text(dumps(data), encoding="utf-8")

    retired = ready_bundles.retire_acknowledged(ready_root, ledger, checkpoint)

    assert [item.bundle_id for item in retired] == [published.bundle_id]
    assert not published.path.exists()


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
    _assert_invalid_utc_mapping_is_not_retired(
        tmp_path,
        {"observation_id": "observation", "offset_seconds": float("nan"), "uncertainty_seconds": 0.5},
    )


@pytest.mark.parametrize(
    "utc",
    [
        [],
        {},
        {"observation_id": "", "offset_seconds": 0.0, "uncertainty_seconds": 0.0},
        {"observation_id": 1, "offset_seconds": 0.0, "uncertainty_seconds": 0.0},
        {"observation_id": "observation", "offset_seconds": True, "uncertainty_seconds": 0.0},
        {"observation_id": "observation", "offset_seconds": float("inf"), "uncertainty_seconds": 0.0},
        {"observation_id": "observation", "offset_seconds": 0.0, "uncertainty_seconds": -0.1},
        {"observation_id": "observation", "offset_seconds": 0.0, "uncertainty_seconds": True},
        {
            "observation_id": "observation",
            "offset_seconds": 0.0,
            "uncertainty_seconds": 0.0,
            "confidence": "certain",
        },
    ],
)
def test_invalid_ready_utc_mapping_blocks_ack_retirement_without_mutation(tmp_path: Path, utc: object) -> None:
    _assert_invalid_utc_mapping_is_not_retired(tmp_path, utc)


@pytest.mark.parametrize(
    "damage",
    [
        "frontier-bool",
        "frontier-string",
        "frontier-negative",
        "bundles-nonobject",
        "entry-schema",
        "entry-range",
        "entry-range-conflict",
        "entry-records-digest",
        "entry-draft-digest",
    ],
)
def test_invalid_ready_ledger_blocks_ack_retirement_without_mutation(tmp_path: Path, damage: str) -> None:
    ready_root, ledger, checkpoint, published = _published_bundle_with_valid_ack(tmp_path)
    document = cast(dict[str, object], loads(ledger.read_text(encoding="utf-8")))
    if damage == "frontier-bool":
        document["frontier"] = True
    elif damage == "frontier-string":
        document["frontier"] = "101"
    elif damage == "frontier-negative":
        document["frontier"] = -1
    elif damage == "bundles-nonobject":
        document["bundles"] = []
    else:
        bundles = cast(dict[str, object], document["bundles"])
        entry = cast(dict[str, object], bundles[published.bundle_id])
        if damage == "entry-schema":
            entry["annotation"] = "unexpected"
        elif damage == "entry-range":
            entry["next_sequence"] = entry["start_sequence"]
        elif damage == "entry-range-conflict":
            entry["start_sequence"] = 101
            entry["next_sequence"] = 102
        elif damage == "entry-records-digest":
            entry["records_sha256"] = "z" * 64
        else:
            entry["draft_raw_sha256"] = "z" * 64
    ledger.write_text(dumps(document), encoding="utf-8")
    before = _retirement_snapshot(published, ledger)

    with pytest.raises(ready_bundles.ReadyBundleError):
        ready_bundles.retire_acknowledged(ready_root, ledger, checkpoint)

    assert published.path.exists()
    assert _retirement_snapshot(published, ledger) == before


@pytest.mark.parametrize(
    "time_ranges",
    [
        None,
        [None],
        [{"start_sequence": 100, "next_sequence": 102, "utc": None, "annotation": "unexpected"}],
        [],
        [{"start_sequence": 100, "next_sequence": 100, "utc": None}],
        [{"start_sequence": 101, "next_sequence": 100, "utc": None}],
        [{"start_sequence": 101, "next_sequence": 102, "utc": None}],
        [{"start_sequence": 100, "next_sequence": 101, "utc": None}],
    ],
)
def test_invalid_ready_time_ranges_block_ack_retirement_without_mutation(tmp_path: Path, time_ranges: object) -> None:
    ready_root, ledger, checkpoint, published = _published_bundle_with_valid_ack(tmp_path, timestamps=(100, 101))
    manifest_path = published.path / "manifest.json"
    manifest = cast(dict[str, object], loads(manifest_path.read_text(encoding="utf-8")))
    manifest["time_ranges"] = time_ranges
    manifest_path.write_text(dumps(manifest), encoding="utf-8")
    before = _retirement_snapshot(published, ledger)

    with pytest.raises(ready_bundles.ReadyBundleError):
        ready_bundles.retire_acknowledged(ready_root, ledger, checkpoint)

    assert published.path.exists()
    assert _retirement_snapshot(published, ledger) == before


def test_empty_zero_frontier_ledger_allows_empty_acknowledgement(tmp_path: Path) -> None:
    ready_root = tmp_path / "ready"
    ready_root.mkdir()
    ledger = tmp_path / "collector" / "ready-publications.json"
    ledger.parent.mkdir()
    ledger.write_text(dumps({"bundles": {}, "frontier": 0}), encoding="utf-8")
    checkpoint = _checkpoint(tmp_path, [])

    assert ready_bundles.retire_acknowledged(ready_root, ledger, checkpoint) == ()
    assert loads(ledger.read_text(encoding="utf-8")) == {"bundles": {}, "frontier": 0}


def test_contiguous_utc_ranges_with_zero_values_allow_ack_retirement(tmp_path: Path) -> None:
    draft_root = tmp_path / "draft"
    ready_root = tmp_path / "ready"
    ledger = tmp_path / "collector" / "ready-publications.json"
    _draft(draft_root, (100, 101), start_sequence=100)
    segments = ClockSegmentMap(
        (
            ClockSegment("first", 100, 101, 0.0, 0.0),
            ClockSegment("second", 101, 102, 0.0, 0.0, "approximate"),
        )
    )
    (published,) = _finalize_drafts(draft_root, ready_root, ledger, segments)
    manifest = cast(dict[str, object], loads((published.path / "manifest.json").read_text(encoding="utf-8")))
    checkpoint = _checkpoint(tmp_path, [(published.bundle_id, published.records_sha256)])

    retired = ready_bundles.retire_acknowledged(ready_root, ledger, checkpoint)

    assert [item.bundle_id for item in retired] == [published.bundle_id]
    assert manifest["time_ranges"] == [
        {
            "start_sequence": 100,
            "next_sequence": 101,
            "utc": {"observation_id": "first", "offset_seconds": 0.0, "uncertainty_seconds": 0.0},
        },
        {
            "start_sequence": 101,
            "next_sequence": 102,
            "utc": {
                "observation_id": "second",
                "offset_seconds": 0.0,
                "uncertainty_seconds": 0.0,
                "confidence": "approximate",
            },
        },
    ]
    assert not published.path.exists()


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


def test_finalize_rejects_symlinked_draft_directory_without_touching_external_draft(tmp_path: Path) -> None:
    draft_root = tmp_path / "draft"
    draft = _audio_draft(draft_root, sequence=10)
    external = tmp_path / "external-draft"
    draft.rename(external)
    link = draft_root / draft.name
    link.symlink_to(external, target_is_directory=True)
    original = {path.name: path.read_bytes() for path in external.iterdir()}
    ready_root = tmp_path / "ready"
    ledger = tmp_path / "collector" / "ready-publications.json"

    with pytest.raises(ready_bundles.ReadyBundleError, match="unsafe entry"):
        ready_bundles.finalize_drafts(
            draft_root, ready_root, ledger, ClockSegmentMap(()), config=ReadyConfig(target_audio_seconds=0.02)
        )

    assert link.is_symlink()
    assert {path.name: path.read_bytes() for path in external.iterdir()} == original
    assert not tuple(ready_root.iterdir())
    assert not ledger.exists()


@pytest.mark.parametrize("damage", ("raw-size", "raw-digest"))
def test_finalize_rejects_authenticated_draft_record_mismatch(tmp_path: Path, damage: str) -> None:
    draft_root = tmp_path / "draft"
    draft = _audio_draft(draft_root, sequence=10)
    raw_path = draft / "records.bin"
    raw = raw_path.read_bytes()
    manifest_path = draft / "manifest.json"
    receipt_path = draft / "receipt.json"
    manifest = cast(dict[str, object], loads(manifest_path.read_text(encoding="utf-8")))
    receipt = cast(dict[str, object], loads(receipt_path.read_text(encoding="utf-8")))
    if damage == "raw-size":
        raw += _record(101, 2)
        digest = sha256(raw).hexdigest()
        manifest["raw_sha256"] = digest
        receipt["raw_sha256"] = digest
        manifest_path.write_text(dumps(manifest), encoding="utf-8")
        receipt_path.write_text(dumps(receipt), encoding="utf-8")
    else:
        raw = bytes((raw[0] ^ 1,)) + raw[1:]
    raw_path.write_bytes(raw)
    ready_root = tmp_path / "ready"
    ledger = tmp_path / "collector" / "ready-publications.json"

    with pytest.raises(ready_bundles.ReadyBundleError, match="draft bundle records"):
        ready_bundles.finalize_drafts(
            draft_root, ready_root, ledger, ClockSegmentMap(()), config=ReadyConfig(target_audio_seconds=0.02)
        )

    assert draft.exists()
    assert raw_path.read_bytes() == raw
    assert not tuple(ready_root.iterdir())
    assert not ledger.exists()


def test_zero_start_draft_publishes_and_retires_through_public_contracts(tmp_path: Path) -> None:
    draft_root = tmp_path / "draft"
    draft = _draft(draft_root, (100,), start_sequence=0)
    original = (draft / "records.bin").read_bytes()
    ready_root = tmp_path / "ready"
    ledger = tmp_path / "collector" / "ready-publications.json"

    (published,) = _finalize_drafts(draft_root, ready_root, ledger, ClockSegmentMap(()))
    manifest = cast(dict[str, object], loads((published.path / "manifest.json").read_text(encoding="utf-8")))
    state = cast(dict[str, object], loads(ledger.read_text(encoding="utf-8")))
    checkpoint = _checkpoint(tmp_path, [(published.bundle_id, published.records_sha256)])

    assert (manifest["start_sequence"], manifest["next_sequence"]) == (0, 1)
    assert (published.path / "records.bin").read_bytes() == original
    assert state["frontier"] == 1
    assert ready_bundles.retire_acknowledged(ready_root, ledger, checkpoint) == (published,)
    assert not published.path.exists()


@pytest.mark.parametrize("second_start", (101, 102, 105))
def test_ready_reconciliation_rejects_overlap_and_accepts_adjacent_or_separate_ranges(
    tmp_path: Path, second_start: int
) -> None:
    ready_root = tmp_path / "ready"
    ready_root.mkdir(mode=0o2750)
    ready_root.chmod(0o2750)
    published_paths: list[Path] = []
    for label, start, timestamps, marker_start in (
        ("first", 100, (100, 101), 1),
        ("second", second_start, (200,), 20),
    ):
        draft_root = tmp_path / label / "draft"
        draft_root.parent.mkdir()
        _draft(draft_root, timestamps, start_sequence=start, marker_start=marker_start)
        local_ready = draft_root.parent / "ready"
        (published,) = _finalize_drafts(draft_root, local_ready, draft_root.parent / "ledger.json", ClockSegmentMap(()))
        published_paths.append(published.path)
    for path in published_paths:
        path.rename(ready_root / path.name)
    draft_root = tmp_path / "empty-draft"
    ledger = tmp_path / "common-ledger.json"

    if second_start == 101:
        with pytest.raises(ready_bundles.ReadyBundleError, match="ranges overlap"):
            _finalize_drafts(draft_root, ready_root, ledger, ClockSegmentMap(()))
        assert not ledger.exists()
    else:
        assert _finalize_drafts(draft_root, ready_root, ledger, ClockSegmentMap(())) == ()
        state = cast(dict[str, object], loads(ledger.read_text(encoding="utf-8")))
        assert len(cast(dict[str, object], state["bundles"])) == 2

    assert len(tuple(ready_root.iterdir())) == 2


def test_replay_after_two_ready_prefixes_publishes_only_the_unique_suffix(tmp_path: Path) -> None:
    ready_root = tmp_path / "ready"
    ready_root.mkdir(mode=0o2750)
    ready_root.chmod(0o2750)
    prefix_records: list[tuple[str, bytes]] = []
    for sequence, marker in ((100, 10), (101, 11)):
        draft_root = tmp_path / f"prefix-{sequence}" / "draft"
        draft_root.parent.mkdir()
        _draft(draft_root, (sequence,), start_sequence=sequence, marker_start=marker)
        local_ready = draft_root.parent / "ready"
        (published,) = _finalize_drafts(draft_root, local_ready, draft_root.parent / "ledger.json", ClockSegmentMap(()))
        prefix_records.append((published.bundle_id, (published.path / "records.bin").read_bytes()))
        published.path.rename(ready_root / published.path.name)
    draft_root = tmp_path / "replay-draft"
    replay = _draft(draft_root, (100, 101, 102), start_sequence=100, marker_start=10)
    replay_records = (replay / "records.bin").read_bytes()
    ledger = tmp_path / "ledger.json"

    (suffix,) = _finalize_drafts(draft_root, ready_root, ledger, ClockSegmentMap(()))

    suffix_manifest = cast(dict[str, object], loads((suffix.path / "manifest.json").read_text(encoding="utf-8")))
    suffix_records = (suffix.path / "records.bin").read_bytes()
    assert (suffix.next_sequence - suffix.record_count, suffix.next_sequence) == (102, 103)
    assert suffix_records == replay_records[2 * RECORD_SIZE :]
    assert suffix_manifest["draft_raw_sha256"] == sha256(suffix_records).hexdigest()
    assert all(
        (ready_root / bundle_id / "records.bin").read_bytes() == records for bundle_id, records in prefix_records
    )
    assert len(tuple(ready_root.iterdir())) == 3


@pytest.mark.parametrize(
    ("damage", "message"),
    (
        ("extra-entry", "ready bundle inventory"),
        ("extra-manifest-key", "ready manifest is invalid"),
        ("record-size", "ready manifest range"),
        ("raw-corruption", "ready records do not match manifest"),
    ),
)
def test_retirement_rejects_malformed_ready_inventory_and_manifest(tmp_path: Path, damage: str, message: str) -> None:
    ready_root, ledger, checkpoint, published = _published_bundle_with_valid_ack(tmp_path)
    manifest_path = published.path / "manifest.json"
    if damage == "extra-entry":
        (published.path / "extra").write_bytes(b"unexpected")
    elif damage in ("extra-manifest-key", "record-size"):
        manifest = cast(dict[str, object], loads(manifest_path.read_text(encoding="utf-8")))
        if damage == "extra-manifest-key":
            manifest["annotation"] = "unexpected"
        else:
            manifest["record_size"] = RECORD_SIZE + 1
        manifest_path.write_text(dumps(manifest), encoding="utf-8")
    else:
        raw_path = published.path / "records.bin"
        raw = raw_path.read_bytes()
        raw_path.write_bytes(bytes((raw[0] ^ 1,)) + raw[1:])
    before = _retirement_snapshot(published, ledger)

    with pytest.raises(ready_bundles.ReadyBundleError, match=message):
        ready_bundles.retire_acknowledged(ready_root, ledger, checkpoint)

    assert published.path.exists()
    assert _retirement_snapshot(published, ledger) == before


def test_finalize_rejects_ready_path_that_disagrees_with_manifest_identity(tmp_path: Path) -> None:
    ready_root, ledger, _checkpoint_path, published = _published_bundle_with_valid_ack(tmp_path)
    original_records = (published.path / "records.bin").read_bytes()
    original_manifest = (published.path / "manifest.json").read_bytes()
    original_ledger = ledger.read_bytes()
    moved = ready_root / "renamed-ready-bundle"
    published.path.rename(moved)

    with pytest.raises(ready_bundles.ReadyBundleError, match="manifest identity"):
        _finalize_drafts(tmp_path / "empty-drafts", ready_root, ledger, ClockSegmentMap(()))

    assert moved.is_dir()
    assert (moved / "records.bin").read_bytes() == original_records
    assert (moved / "manifest.json").read_bytes() == original_manifest
    assert ledger.read_bytes() == original_ledger


def test_retirement_rejects_symlinked_manifest_without_touching_target(tmp_path: Path) -> None:
    ready_root, ledger, checkpoint, published = _published_bundle_with_valid_ack(tmp_path)
    manifest_path = published.path / "manifest.json"
    external_manifest = tmp_path / "external-manifest.json"
    manifest_path.rename(external_manifest)
    manifest_bytes = external_manifest.read_bytes()
    manifest_path.symlink_to(external_manifest)
    before = _retirement_snapshot(published, ledger)

    with pytest.raises(ready_bundles.ReadyBundleError, match="metadata is not a regular file"):
        ready_bundles.retire_acknowledged(ready_root, ledger, checkpoint)

    assert manifest_path.is_symlink()
    assert external_manifest.read_bytes() == manifest_bytes
    assert _retirement_snapshot(published, ledger) == before


def test_finalize_rejects_self_consistent_zero_count_ready_bundle(tmp_path: Path) -> None:
    ready_root = tmp_path / "ready"
    ready_root.mkdir(mode=0o2750)
    ready_root.chmod(0o2750)
    empty_digest = sha256(b"").hexdigest()
    bundle = _persisted_ready_bundle(
        ready_root,
        {
            "start_sequence": 100,
            "next_sequence": 100,
            "record_count": 0,
            "draft_raw_sha256": empty_digest,
            "time_ranges": [],
        },
        b"",
    )
    ledger = tmp_path / "collector" / "ready-publications.json"

    with pytest.raises(ready_bundles.ReadyBundleError, match="manifest range"):
        _finalize_drafts(tmp_path / "empty-drafts", ready_root, ledger, ClockSegmentMap(()))

    assert (bundle / "records.bin").read_bytes() == b""
    assert not ledger.exists()


@pytest.mark.parametrize(("start", "next_sequence"), ((-1, 0), (True, 2)))
def test_finalize_rejects_negative_or_boolean_ready_range_start(
    tmp_path: Path, start: int | bool, next_sequence: int
) -> None:
    ready_root = tmp_path / "ready"
    ready_root.mkdir(mode=0o2750)
    ready_root.chmod(0o2750)
    record = _record(100, 1)
    bundle = _persisted_ready_bundle(
        ready_root,
        {
            "start_sequence": start,
            "next_sequence": next_sequence,
            "record_count": 1,
            "draft_raw_sha256": "a" * 64,
            "time_ranges": [{"start_sequence": start, "next_sequence": next_sequence, "utc": None}],
        },
        record,
    )
    ledger = tmp_path / "collector" / "ready-publications.json"

    with pytest.raises(ready_bundles.ReadyBundleError, match="metadata integer"):
        _finalize_drafts(tmp_path / "empty-drafts", ready_root, ledger, ClockSegmentMap(()))

    assert (bundle / "records.bin").read_bytes() == record
    assert not ledger.exists()


@pytest.mark.parametrize("draft_digest", ("short", "g" * 64))
def test_finalize_rejects_malformed_ready_draft_digest_before_recording_ledger(
    tmp_path: Path, draft_digest: str
) -> None:
    ready_root = tmp_path / "ready"
    ready_root.mkdir(mode=0o2750)
    ready_root.chmod(0o2750)
    record = _record(100, 1)
    bundle = _persisted_ready_bundle(
        ready_root,
        {
            "start_sequence": 100,
            "next_sequence": 101,
            "record_count": 1,
            "draft_raw_sha256": draft_digest,
            "time_ranges": [{"start_sequence": 100, "next_sequence": 101, "utc": None}],
        },
        record,
    )
    ledger = tmp_path / "collector" / "ready-publications.json"

    with pytest.raises(ready_bundles.ReadyBundleError, match="metadata digest"):
        _finalize_drafts(tmp_path / "empty-drafts", ready_root, ledger, ClockSegmentMap(()))

    assert (bundle / "manifest.json").is_file()
    assert not ledger.exists()


def test_retirement_rejects_zero_interval_before_valid_full_time_coverage(tmp_path: Path) -> None:
    ready_root, ledger, checkpoint, published = _published_bundle_with_valid_ack(tmp_path, timestamps=(100, 101))
    manifest_path = published.path / "manifest.json"
    manifest = cast(dict[str, object], loads(manifest_path.read_text(encoding="utf-8")))
    manifest["time_ranges"] = [
        {"start_sequence": 100, "next_sequence": 100, "utc": None},
        {"start_sequence": 100, "next_sequence": 102, "utc": None},
    ]
    manifest_path.write_text(dumps(manifest), encoding="utf-8")
    before = _retirement_snapshot(published, ledger)

    with pytest.raises(ready_bundles.ReadyBundleError, match="time ranges"):
        ready_bundles.retire_acknowledged(ready_root, ledger, checkpoint)

    assert published.path.exists()
    assert _retirement_snapshot(published, ledger) == before


def test_finalize_accepts_an_empty_zero_frontier_ledger_without_ack_shortcut(tmp_path: Path) -> None:
    draft_root = tmp_path / "empty-drafts"
    ready_root = tmp_path / "ready"
    ready_root.mkdir(mode=0o2750)
    ready_root.chmod(0o2750)
    ledger = tmp_path / "collector" / "ready-publications.json"
    ledger.parent.mkdir()
    ledger.write_text(dumps({"bundles": {}, "frontier": 0}), encoding="utf-8")
    before = ledger.read_bytes()

    assert _finalize_drafts(draft_root, ready_root, ledger, ClockSegmentMap(())) == ()

    assert ledger.read_bytes() == before
    assert draft_root.is_dir()


@pytest.mark.parametrize(
    ("damage", "expected"),
    (
        ("non-dict", None),
        ("bundle-id", "bad"),
        ("records-digest", "z" * 64),
        ("draft-digest", "z" * 64),
        ("boolean-start", True),
    ),
)
def test_finalize_rejects_unreferenced_malformed_ledger_entry_with_empty_roots(
    tmp_path: Path, damage: str, expected: object
) -> None:
    draft_root = tmp_path / "empty-drafts"
    ready_root = tmp_path / "ready"
    ready_root.mkdir(mode=0o2750)
    ready_root.chmod(0o2750)
    ledger = tmp_path / "collector" / "ready-publications.json"
    ledger.parent.mkdir()
    entry: object = {
        "records_sha256": "b" * 64,
        "state": "ready",
        "start_sequence": 0,
        "next_sequence": 1,
        "draft_raw_sha256": "c" * 64,
    }
    bundle_id = "a" * 64
    if damage == "non-dict":
        entry = []
    elif damage == "bundle-id":
        bundle_id = cast(str, expected)
    elif damage == "records-digest":
        cast(dict[str, object], entry)["records_sha256"] = expected
    elif damage == "draft-digest":
        cast(dict[str, object], entry)["draft_raw_sha256"] = expected
    elif damage == "boolean-start":
        cast(dict[str, object], entry)["start_sequence"] = expected
    ledger.write_text(dumps({"bundles": {bundle_id: entry}, "frontier": 1}), encoding="utf-8")
    before = ledger.read_bytes()

    with pytest.raises(ready_bundles.ReadyBundleError, match="publication ledger"):
        _finalize_drafts(draft_root, ready_root, ledger, ClockSegmentMap(()))

    assert ledger.read_bytes() == before
    assert not tuple(ready_root.iterdir())
    assert draft_root.is_dir()


@pytest.mark.parametrize(("start", "next_sequence", "valid"), ((0, 1, True), (-1, 1, False), (1, 1, False)))
def test_finalize_validates_unreferenced_ledger_ranges_through_public_reader(
    tmp_path: Path, start: int, next_sequence: int, valid: bool
) -> None:
    draft_root = tmp_path / "empty-drafts"
    ready_root = tmp_path / "ready"
    ready_root.mkdir(mode=0o2750)
    ready_root.chmod(0o2750)
    ledger = tmp_path / "ledger.json"
    entry = {
        "records_sha256": "b" * 64,
        "state": "ready",
        "start_sequence": start,
        "next_sequence": next_sequence,
        "draft_raw_sha256": "c" * 64,
    }
    bundle_id = sha256(f"{start}:{next_sequence}:{'c' * 64}".encode()).hexdigest() if valid else "a" * 64
    ledger.write_text(dumps({"bundles": {bundle_id: entry}, "frontier": 1}), encoding="utf-8")
    before = ledger.read_bytes()

    if valid:
        assert _finalize_drafts(draft_root, ready_root, ledger, ClockSegmentMap(())) == ()
    else:
        with pytest.raises(ready_bundles.ReadyBundleError, match="publication ledger"):
            _finalize_drafts(draft_root, ready_root, ledger, ClockSegmentMap(()))

    assert ledger.read_bytes() == before


def test_finalize_rejects_insufficient_legacy_ledger_without_touching_ready(tmp_path: Path) -> None:
    draft_root = tmp_path / "draft"
    draft = _draft(draft_root, (100,), start_sequence=100)
    original_records = (draft / "records.bin").read_bytes()
    ready_root = tmp_path / "ready"
    ledger = tmp_path / "collector" / "ready-publications.json"

    (published,) = _finalize_drafts(draft_root, ready_root, ledger, ClockSegmentMap(()))
    initial = cast(dict[str, object], loads(ledger.read_text(encoding="utf-8")))
    bundles = cast(dict[str, object], initial["bundles"])
    bundles[published.bundle_id] = {"records_sha256": published.records_sha256, "state": "ready"}
    ledger.write_text(dumps(initial), encoding="utf-8")

    before = ledger.read_bytes()
    with pytest.raises(ready_bundles.ReadyBundleError, match="publication ledger"):
        _finalize_drafts(draft_root, ready_root, ledger, ClockSegmentMap(()))

    assert initial["frontier"] == published.next_sequence
    assert ledger.read_bytes() == before
    assert (published.path / "records.bin").read_bytes() == original_records


def test_finalize_creates_missing_storage_roots_and_nested_ledger_parent(tmp_path: Path) -> None:
    draft_root = tmp_path / "nested" / "drafts"
    ready_root = tmp_path / "ready"
    ready_root.mkdir(mode=0o2750)
    ready_root.chmod(0o2750)
    ledger = tmp_path / "ledger-parent" / "nested" / "ready.json"

    assert _finalize_drafts(draft_root, ready_root, ledger, ClockSegmentMap(())) == ()
    assert draft_root.is_dir()

    draft = _draft(draft_root, (100,), start_sequence=10)
    (published,) = _finalize_drafts(draft_root, ready_root, ledger, ClockSegmentMap(()))

    assert ledger.is_file()
    assert published.path.is_dir()
    assert not draft.exists()


def test_finalize_rejects_symlinked_ready_root_without_writing_to_target(tmp_path: Path) -> None:
    source_parent = tmp_path / "source"
    source_parent.mkdir()
    draft_root = source_parent / "draft"
    draft = _draft(draft_root, (100,), start_sequence=10)
    external_ready = tmp_path / "external-ready"
    external_ready.mkdir(mode=0o2750)
    external_ready.chmod(0o2750)
    sentinel = external_ready / ".owned.txt"
    sentinel.write_bytes(b"outside ready root")
    ready_root = tmp_path / "ready-link"
    ready_root.symlink_to(external_ready, target_is_directory=True)
    ledger = tmp_path / "ledger.json"

    with pytest.raises(ready_bundles.ReadyBundleError, match="storage root is unsafe"):
        _finalize_drafts(draft_root, ready_root, ledger, ClockSegmentMap(()))

    assert ready_root.is_symlink()
    assert tuple(external_ready.iterdir()) == (sentinel,)
    assert sentinel.read_bytes() == b"outside ready root"
    assert draft.exists()
    assert not ledger.exists()


def test_publication_propagates_ready_parent_fsync_failure_and_retries(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    draft_root = tmp_path / "draft"
    draft = _draft(draft_root, (100,), start_sequence=10)
    original_records = (draft / "records.bin").read_bytes()
    ready_root = tmp_path / "ready"
    ready_root.chmod(0o2750)
    ledger = tmp_path / "ledger.json"
    sentinel = OSError("ready parent fsync sentinel")
    original_fsync = ready_bundles.os.fsync
    failed = False

    def fail_after_bundle_rename(descriptor: int) -> None:
        nonlocal failed
        path = Path(f"/proc/self/fd/{descriptor}").readlink()
        if (
            path == ready_root
            and not failed
            and any((child / "manifest.json").is_file() for child in ready_root.iterdir())
        ):
            failed = True
            raise sentinel
        original_fsync(descriptor)

    monkeypatch.setattr(ready_bundles.os, "fsync", fail_after_bundle_rename)

    with pytest.raises(OSError) as raised:
        _finalize_drafts(draft_root, ready_root, ledger, ClockSegmentMap(()))

    assert raised.value is sentinel
    assert failed
    assert draft.exists()
    (published_path,) = tuple(ready_root.iterdir())
    assert (published_path / "records.bin").read_bytes() == original_records
    assert not ledger.exists()
    assert _finalize_drafts(draft_root, ready_root, ledger, ClockSegmentMap(())) == ()
    assert not draft.exists()
    assert loads(ledger.read_text(encoding="utf-8"))["bundles"]


@pytest.mark.parametrize("field", ("start_sequence", "next_sequence", "draft_raw_sha256"))
def test_replay_rejects_conflicting_retired_ledger_identity(tmp_path: Path, field: str) -> None:
    draft_root = tmp_path / "draft"
    original = _draft(draft_root, (100, 101), start_sequence=100)
    original_records = (original / "records.bin").read_bytes()
    ready_root = tmp_path / "ready"
    ledger = tmp_path / "collector" / "ready-publications.json"
    (published,) = _finalize_drafts(draft_root, ready_root, ledger, ClockSegmentMap(()))
    checkpoint = _checkpoint(tmp_path, [(published.bundle_id, published.records_sha256)])
    ready_bundles.retire_acknowledged(ready_root, ledger, checkpoint)
    state = cast(dict[str, object], loads(ledger.read_text(encoding="utf-8")))
    bundles = cast(dict[str, object], state["bundles"])
    entry = cast(dict[str, object], bundles[published.bundle_id])
    if field == "start_sequence":
        entry[field] = 101
    elif field == "next_sequence":
        entry[field] = 103
    else:
        entry[field] = "d" * 64
    ledger.write_text(dumps(state), encoding="utf-8")
    before = ledger.read_bytes()
    replay = _draft(draft_root, (100, 101), start_sequence=100)

    with pytest.raises(ready_bundles.ReadyBundleError, match="publication ledger"):
        _finalize_drafts(draft_root, ready_root, ledger, ClockSegmentMap(()))

    assert replay.exists()
    assert (replay / "records.bin").read_bytes() == original_records
    assert ledger.read_bytes() == before
    assert not tuple(ready_root.iterdir())


def test_replay_republishes_ready_ledger_entry_when_its_payload_directory_is_missing(tmp_path: Path) -> None:
    draft_root = tmp_path / "draft"
    _draft(draft_root, (100,), start_sequence=100)
    ready_root = tmp_path / "ready"
    ledger = tmp_path / "ledger.json"
    (published,) = _finalize_drafts(draft_root, ready_root, ledger, ClockSegmentMap(()))
    original_records = (published.path / "records.bin").read_bytes()
    published.path.rename(tmp_path / "parked-ready")
    replay = _draft(draft_root, (100,), start_sequence=100)

    result = _finalize_drafts(draft_root, ready_root, ledger, ClockSegmentMap(()))

    state = cast(dict[str, object], loads(ledger.read_text(encoding="utf-8")))
    assert len(result) == 1
    assert result[0].bundle_id == published.bundle_id
    assert (result[0].path / "records.bin").read_bytes() == original_records
    entry = cast(dict[str, object], cast(dict[str, object], state["bundles"])[published.bundle_id])
    assert entry["state"] == "ready"
    assert not replay.exists()


def test_older_draft_is_ordering_blocked_and_retired_overlap_keeps_sources(tmp_path: Path) -> None:
    ready_root = tmp_path / "ready"
    ledger = tmp_path / "ledger.json"
    _draft(tmp_path / "draft", (101,), start_sequence=101)
    (published,) = _finalize_drafts(tmp_path / "draft", ready_root, ledger, ClockSegmentMap(()))
    ready_bundles.retire_acknowledged(
        ready_root, ledger, _checkpoint(tmp_path, [(published.bundle_id, published.records_sha256)])
    )
    adjacent = _draft(tmp_path / "draft", (100,), start_sequence=100, marker_start=20)

    with pytest.raises(ready_bundles.ReadyBundleError, match="ordering is blocked"):
        _finalize_drafts(tmp_path / "draft", ready_root, ledger, ClockSegmentMap(()))
    assert adjacent.exists()
    overlapping = _draft(tmp_path / "draft", (101, 102), start_sequence=101)
    before_ledger = ledger.read_bytes()
    with pytest.raises(ready_bundles.ReadyBundleError, match="retired range"):
        _finalize_drafts(tmp_path / "draft", ready_root, ledger, ClockSegmentMap(()))

    assert overlapping.exists()
    assert ledger.read_bytes() == before_ledger


@pytest.mark.parametrize("damage", ("corrupt-intact", "partial-cleanup"))
def test_resume_retired_authenticates_intact_bundle_and_finishes_partial_cleanup(tmp_path: Path, damage: str) -> None:
    ready_root, ledger, _checkpoint_path, published = _published_bundle_with_valid_ack(tmp_path)
    state = cast(dict[str, object], loads(ledger.read_text(encoding="utf-8")))
    entry = cast(dict[str, object], cast(dict[str, object], state["bundles"])[published.bundle_id])
    entry["state"] = "retired"
    ledger.write_text(dumps(state), encoding="utf-8")
    records_path = published.path / "records.bin"
    manifest_path = published.path / "manifest.json"
    original_records = records_path.read_bytes()
    original_manifest = manifest_path.read_bytes()
    if damage == "corrupt-intact":
        records_path.write_bytes(bytes((original_records[0] ^ 1,)) + original_records[1:])
        corrupted_records = records_path.read_bytes()
        with pytest.raises(ready_bundles.ReadyBundleError, match="records do not match manifest"):
            ready_bundles.resume_retired(ready_root, ledger)
        assert records_path.read_bytes() == corrupted_records
        assert manifest_path.read_bytes() == original_manifest
        assert published.path.exists()
    else:
        records_path.unlink()
        ready_bundles.resume_retired(ready_root, ledger)
        assert not published.path.exists()
        assert not manifest_path.exists()
    state = cast(dict[str, object], loads(ledger.read_text(encoding="utf-8")))
    entry = cast(dict[str, object], cast(dict[str, object], state["bundles"])[published.bundle_id])
    assert entry["state"] == "retired"


def test_resume_retired_rejects_symlinked_bundle_directory_without_touching_target(tmp_path: Path) -> None:
    ready_root, ledger, _checkpoint_path, published = _published_bundle_with_valid_ack(tmp_path)
    state = cast(dict[str, object], loads(ledger.read_text(encoding="utf-8")))
    entry = cast(dict[str, object], cast(dict[str, object], state["bundles"])[published.bundle_id])
    entry["state"] = "retired"
    ledger.write_text(dumps(state), encoding="utf-8")
    external = tmp_path / "external-ready"
    (published.path / "records.bin").unlink()
    published.path.rename(external)
    target_files = {path.name: path.read_bytes() for path in external.iterdir()}
    published.path.symlink_to(external, target_is_directory=True)

    with pytest.raises(ready_bundles.ReadyBundleError, match="retired ready bundle is unsafe"):
        ready_bundles.resume_retired(ready_root, ledger)

    assert published.path.is_symlink()
    assert {path.name: path.read_bytes() for path in external.iterdir()} == target_files
    assert target_files == {"manifest.json": (external / "manifest.json").read_bytes()}


def test_finalize_refuses_active_ready_directory_marked_retired_in_ledger(tmp_path: Path) -> None:
    ready_root, ledger, _checkpoint_path, published = _published_bundle_with_valid_ack(tmp_path)
    state = cast(dict[str, object], loads(ledger.read_text(encoding="utf-8")))
    entry = cast(dict[str, object], cast(dict[str, object], state["bundles"])[published.bundle_id])
    entry["state"] = "retired"
    ledger.write_text(dumps(state), encoding="utf-8")
    before = _retirement_snapshot(published, ledger)

    with pytest.raises(ready_bundles.ReadyBundleError, match="not active"):
        _finalize_drafts(tmp_path / "draft", ready_root, ledger, ClockSegmentMap(()))

    assert published.path.exists()
    assert _retirement_snapshot(published, ledger) == before


@pytest.mark.parametrize(
    ("damage", "message"),
    (
        ("checkpoint-key", "Windmill ready checkpoint is invalid"),
        ("tail-key", "Windmill pending tail is invalid"),
        ("ack-key", "Windmill acknowledged is invalid"),
    ),
)
def test_retire_rejects_unexpected_checkpoint_tail_and_ack_fields(tmp_path: Path, damage: str, message: str) -> None:
    ready_root, ledger, checkpoint, published = _published_bundle_with_valid_ack(tmp_path)
    data = cast(dict[str, object], loads(checkpoint.read_text(encoding="utf-8")))
    if damage == "checkpoint-key":
        data["annotation"] = "unexpected"
    elif damage == "tail-key":
        data["open_speech_tail"] = {"entries": [], "opened_at": 1, "outputs": [], "annotation": "unexpected"}
    else:
        acknowledged = cast(list[dict[str, object]], data["acknowledged"])
        acknowledged[0]["annotation"] = "unexpected"
    checkpoint.write_text(dumps(data), encoding="utf-8")
    before = _retirement_snapshot(published, ledger)

    with pytest.raises(ready_bundles.ReadyBundleError, match=message):
        ready_bundles.retire_acknowledged(ready_root, ledger, checkpoint)

    assert published.path.exists()
    assert _retirement_snapshot(published, ledger) == before


def test_nonmatching_minimal_open_tail_identity_allows_acknowledged_retirement(tmp_path: Path) -> None:
    ready_root, ledger, checkpoint, published = _published_bundle_with_valid_ack(tmp_path)
    data = cast(dict[str, object], loads(checkpoint.read_text(encoding="utf-8")))
    data["open_speech_tail"] = {
        "entries": [{"bundle_id": "f" * 64, "records_sha256": "e" * 64}],
        "opened_at": 1,
        "outputs": [],
    }
    checkpoint.write_text(dumps(data), encoding="utf-8")

    retired = ready_bundles.retire_acknowledged(ready_root, ledger, checkpoint)

    assert [item.bundle_id for item in retired] == [published.bundle_id]
    assert not published.path.exists()


def test_identical_contained_and_partial_draft_overlaps_publish_each_record_once(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    draft_root = tmp_path / "draft"
    first = _draft(draft_root, (100, 101, 102, 103), start_sequence=100)
    contained = _draft(draft_root, (101, 102), start_sequence=101, marker_start=2)
    partial = _draft(draft_root, (102, 103, 104), start_sequence=102, marker_start=3)
    expected = (first / "records.bin").read_bytes() + _record(104, 5)
    removed: list[Path] = []
    original_remove = ready_bundles._remove_draft

    def track_remove(path: Path) -> None:
        removed.append(path)
        original_remove(path)

    monkeypatch.setattr(ready_bundles, "_remove_draft", track_remove)

    (published,) = _finalize_drafts(draft_root, tmp_path / "ready", tmp_path / "ledger.json", ClockSegmentMap(()))

    assert published.record_count == 5
    assert (published.path / "records.bin").read_bytes() == expected
    assert not any(path.exists() for path in (first, contained, partial))
    assert len(removed) == len(set(removed)) == 3


def test_duplicate_packets_do_not_meet_threshold_and_conflict_preserves_all_sources(tmp_path: Path) -> None:
    draft_root = tmp_path / "draft"
    first = _draft(draft_root, (100, 101, 102), start_sequence=100)
    duplicate = first.with_name("duplicate")
    copytree(first, duplicate)
    ready_root = tmp_path / "ready"
    ledger = tmp_path / "ledger.json"
    config = ReadyConfig(target_audio_seconds=0.08)

    waiting = _finalize_impl(
        draft_root,
        ready_root,
        ledger,
        ClockSegmentMap(()),
        config=config,
        frontier=103,
        drained=True,
    )
    assert waiting.state is ready_bundles.ReadyOutcomeState.WAITING
    assert first.exists() and duplicate.exists()
    assert not tuple(ready_root.iterdir())

    conflict = _draft(draft_root, (101,), start_sequence=101, marker_start=99)
    with pytest.raises(ready_bundles.ConflictingOverlapError, match="conflicting record bytes"):
        _finalize_impl(draft_root, ready_root, ledger, ClockSegmentMap(()), config=config, frontier=103, drained=True)
    assert all(path.exists() for path in (first, duplicate, conflict))
    assert not ledger.exists()


def test_overlap_normalization_compares_record_against_two_retained_slices(tmp_path: Path) -> None:
    draft_root = tmp_path / "draft"
    _draft(draft_root, tuple(range(100, 105)), start_sequence=100)
    _draft(draft_root, tuple(range(101, 110)), start_sequence=101, marker_start=2)
    _draft(draft_root, (104, 105), start_sequence=104, marker_start=5)

    (published,) = _finalize_drafts(draft_root, tmp_path / "ready", tmp_path / "ledger.json", ClockSegmentMap(()))

    assert published.record_count == 10
    assert (published.path / "records.bin").read_bytes() == b"".join(
        _record(sequence, sequence - 99) for sequence in range(100, 110)
    )


def test_sparse_replay_uses_physical_offsets_and_blocks_late_hole_fill(tmp_path: Path) -> None:
    draft_root = tmp_path / "draft"
    ready_root = tmp_path / "ready"
    ledger = tmp_path / "ledger.json"
    _draft(draft_root, (100,), start_sequence=10)
    _draft(draft_root, (102, 103), start_sequence=12, marker_start=2)
    (sparse,) = _finalize_drafts(draft_root, ready_root, ledger, ClockSegmentMap(()))

    replay = _draft(draft_root, (102, 103, 104), start_sequence=12, marker_start=2)
    (suffix,) = _finalize_drafts(draft_root, ready_root, ledger, ClockSegmentMap(()))
    assert suffix.record_count == 1
    assert suffix.next_sequence == 15
    assert (suffix.path / "records.bin").read_bytes() == _record(104, 4)
    assert not replay.exists()

    hole = _draft(draft_root, (101,), start_sequence=11)
    before = ledger.read_bytes()
    with pytest.raises(ready_bundles.ReadyBundleError, match="ordering is blocked"):
        _finalize_drafts(draft_root, ready_root, ledger, ClockSegmentMap(()))
    assert hole.exists() and sparse.path.exists()
    assert ledger.read_bytes() == before


def test_sparse_ack_retirement_keeps_gap_geometry_and_blocks_late_fill(tmp_path: Path) -> None:
    draft_root = tmp_path / "draft"
    ready_root = tmp_path / "ready"
    ledger = tmp_path / "ledger.json"
    _draft(draft_root, (100,), start_sequence=10)
    _draft(draft_root, (102,), start_sequence=12)
    (published,) = _finalize_drafts(draft_root, ready_root, ledger, ClockSegmentMap(()))

    checkpoint = _checkpoint(tmp_path, [(published.bundle_id, published.records_sha256)])
    (retired,) = ready_bundles.retire_acknowledged(ready_root, ledger, checkpoint)
    entries = cast(dict[str, dict[str, object]], loads(ledger.read_text(encoding="utf-8"))["bundles"])
    entry = entries[published.bundle_id]
    assert retired.record_count == 2
    assert entry["state"] == "retired" and entry["gaps"] == [[11, 12]]
    assert not published.path.exists()

    hole = _draft(draft_root, (101,), start_sequence=11)
    with pytest.raises(ready_bundles.ReadyBundleError, match="ordering is blocked"):
        _finalize_drafts(draft_root, ready_root, ledger, ClockSegmentMap(()))
    assert hole.exists()


def test_sparse_ready_destination_reconciles_ledger_before_source_cleanup(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    draft_root = tmp_path / "draft"
    ready_root = tmp_path / "ready"
    ledger = tmp_path / "ledger.json"
    first = _draft(draft_root, (100,), start_sequence=10)
    second = _draft(draft_root, (102,), start_sequence=12)
    original = ready_bundles._record_ready

    def fail_once(*_args: object, **_kwargs: object) -> None:
        raise OSError("ledger interruption")

    monkeypatch.setattr(ready_bundles, "_record_ready", fail_once)
    with pytest.raises(OSError, match="ledger interruption"):
        _finalize_drafts(draft_root, ready_root, ledger, ClockSegmentMap(()))
    assert first.exists() and second.exists()
    assert len(tuple(ready_root.iterdir())) == 1
    monkeypatch.setattr(ready_bundles, "_record_ready", original)

    assert _finalize_drafts(draft_root, ready_root, ledger, ClockSegmentMap(())) == ()
    assert not first.exists() and not second.exists()
    (destination,) = tuple(ready_root.iterdir())
    assert loads(ledger.read_text(encoding="utf-8"))["bundles"][destination.name]["gaps"] == [[11, 12]]


def test_cached_conflicting_overlap_survives_unrelated_ack_without_raw_reread(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    draft_root = tmp_path / "draft"
    ready_root = tmp_path / "ready"
    ledger = tmp_path / "ledger.json"
    _draft(draft_root, (10,), start_sequence=10)
    (older,) = _finalize_drafts(draft_root, ready_root, ledger, ClockSegmentMap(()))
    first = _draft(draft_root, (100, 101), start_sequence=100)
    _draft(draft_root, (100, 101), start_sequence=100, marker_start=99)
    inventory = ready_bundles.authenticated_inventory(draft_root, ready_root, None)
    counts = _count_raw_reads(monkeypatch, first / "records.bin")

    with pytest.raises(ready_bundles.ConflictingOverlapError):
        _finalize_impl(
            draft_root,
            ready_root,
            ledger,
            ClockSegmentMap(()),
            config=ReadyConfig(target_audio_seconds=0.02),
            frontier=102,
            drained=True,
            inventory=inventory,
        )
    first_counts = tuple(counts)
    assert first_counts[0] > 0 and first_counts[1] > 0
    ready_bundles.retire_acknowledged(
        ready_root, ledger, _checkpoint(tmp_path, [(older.bundle_id, older.records_sha256)])
    )

    with pytest.raises(ready_bundles.ConflictingOverlapError):
        _finalize_impl(
            draft_root,
            ready_root,
            ledger,
            ClockSegmentMap(()),
            config=ReadyConfig(target_audio_seconds=0.02),
            frontier=102,
            drained=True,
            inventory=inventory,
        )
    assert tuple(counts) == first_counts


def test_store_keeps_conflict_cache_after_unrelated_ack_and_closure_change(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    draft_root = tmp_path / "draft"
    ready_root = tmp_path / "ready"
    store = StagingStore.from_paths(
        StagingStore(tmp_path / "collector", draft_root).paths,
        publication_root=ready_root,
        config=replace(DEFAULT_CONFIG, ready=ReadyConfig(target_audio_seconds=0.02)),
    )
    _draft(draft_root, (10,), start_sequence=10)
    store.append_ready_closure(11, "drained")
    (older,) = store.recover_and_publish().published
    first = _draft(draft_root, (100, 101), start_sequence=100)
    _draft(draft_root, (100, 101), start_sequence=100, marker_start=99)
    store.append_ready_closure(102, "drained")
    counts = _count_raw_reads(monkeypatch, first / "records.bin")

    blocked = store.recover_and_publish()
    assert blocked.reason == "authenticated draft overlap has conflicting record bytes"
    first_counts = tuple(counts)
    assert first_counts[0] > 0 and first_counts[1] > 0
    _checkpoint(tmp_path, [(older.bundle_id, older.records_sha256)])
    assert store.recover_and_publish().state is ready_bundles.ReadyOutcomeState.BLOCKED
    store.append_ready_closure(102, "drained")
    assert store.recover_and_publish().reason == blocked.reason
    assert tuple(counts) == first_counts


def test_sparse_identity_binds_hole_geometry_but_ignores_utc_splits(tmp_path: Path) -> None:
    def publish(root: Path, gap_start: int, segments: ClockSegmentMap) -> ready_bundles.ReadyBundleResult:
        root.mkdir()
        draft_root = root / "draft"
        if gap_start == 11:
            _draft(draft_root, (100,), start_sequence=10)
            _draft(draft_root, (101, 102), start_sequence=12, marker_start=2)
        else:
            _draft(draft_root, (100, 101), start_sequence=10)
            _draft(draft_root, (102,), start_sequence=13, marker_start=3)
        return _finalize_drafts(draft_root, root / "ready", root / "ledger.json", segments)[0]

    plain = publish(tmp_path / "plain", 11, ClockSegmentMap(()))
    split = publish(tmp_path / "split", 11, ClockSegmentMap((ClockSegment("clock", 13, 14, 0.5, 0.1),)))
    shifted_hole = publish(tmp_path / "other-gap", 12, ClockSegmentMap(()))

    assert plain.bundle_id == split.bundle_id
    assert plain.bundle_id != shifted_hole.bundle_id
    assert (plain.path / "records.bin").read_bytes() == (shifted_hole.path / "records.bin").read_bytes()
    plain_manifest = cast(dict[str, object], loads((plain.path / "manifest.json").read_text(encoding="utf-8")))
    split_manifest = cast(dict[str, object], loads((split.path / "manifest.json").read_text(encoding="utf-8")))
    plain_ranges = cast(list[dict[str, object]], plain_manifest["time_ranges"])
    split_ranges = cast(list[dict[str, object]], split_manifest["time_ranges"])
    assert len(plain_ranges) == 2 and len(split_ranges) == 3


def test_sparse_ledger_gap_tampering_blocks_ack_without_source_mutation(tmp_path: Path) -> None:
    draft_root = tmp_path / "draft"
    ready_root = tmp_path / "ready"
    ledger = tmp_path / "ledger.json"
    _draft(draft_root, (100,), start_sequence=10)
    _draft(draft_root, (102,), start_sequence=12)
    (published,) = _finalize_drafts(draft_root, ready_root, ledger, ClockSegmentMap(()))
    checkpoint = _checkpoint(tmp_path, [(published.bundle_id, published.records_sha256)])
    state = cast(dict[str, object], loads(ledger.read_text(encoding="utf-8")))
    bundles = cast(dict[str, dict[str, object]], state["bundles"])
    bundles[published.bundle_id]["gaps"] = [[11, 13]]
    ledger.write_text(dumps(state), encoding="utf-8")
    before = ledger.read_bytes()

    with pytest.raises(ready_bundles.ReadyBundleError, match="publication ledger"):
        ready_bundles.retire_acknowledged(ready_root, ledger, checkpoint)

    assert published.path.exists()
    assert ledger.read_bytes() == before
