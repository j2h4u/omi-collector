"""Regression coverage for authenticated terminal attempt cleanup."""

from __future__ import annotations

from hashlib import sha256
from json import dumps, loads
from pathlib import Path
from shutil import rmtree
from typing import cast

import pytest

from omi_collector.capture.adapters import attempt_cleanup, staging_store
from omi_collector.capture.adapters.attempts import StagedAttempt
from omi_collector.capture.adapters.bundle_contract import BundleManifest
from omi_collector.capture.adapters.clock_segments import ClockSegment, ClockSegmentMap
from omi_collector.capture.adapters.collector_state_db import StateKind
from omi_collector.capture.adapters.ready_bundles import ReadyBundleError, finalize_drafts
from omi_collector.capture.adapters.staging_filesystem import _ATTEMPT_CLEANUP_INTENT
from omi_collector.capture.domain.ring_protocol import RECORD_SIZE, ReadBeginNotification
from omi_collector.config import ReadyConfig
from staging_test_factory import StagingStore, attempt_payload


def _capture_root(tmp_path: Path) -> Path:
    return tmp_path / "captures"


def _record(marker: int) -> bytes:
    return marker.to_bytes(4, "big") + bytes((marker,)) * (RECORD_SIZE - 4)


def _audio_record(timestamp: int, marker: int) -> bytes:
    return timestamp.to_bytes(4, "big") + bytes((2, 8, marker)) + bytes(RECORD_SIZE - 7)


def _store(tmp_path: Path) -> StagingStore:
    return StagingStore(tmp_path / "spool", _capture_root(tmp_path))


def _terminal_attempt(store: StagingStore, *, records: tuple[bytes, ...] = ()) -> StagedAttempt:
    count = max(1, len(records))
    attempt = store.prepare_streaming_attempt(100, count)
    attempt.record_read_begin(ReadBeginNotification(100, count))
    if records:
        attempt.accept_chunk(100, memoryview(b"".join(records)))
    attempt.checkpoint()
    attempt.publish_prefix()
    attempt.close(durable=True)
    store.terminalize_prefix_attempt(attempt.attempt_id)
    return attempt


def _ready_ack(store: StagingStore, attempt: StagedAttempt) -> None:
    state = attempt_payload(store, attempt.attempt_id)
    assert state is not None
    checkpoint = cast(dict[str, object], state["checkpoint"])
    start = attempt.descriptor.start_sequence
    count = cast(int, checkpoint["record_count"])
    digest = cast(str, checkpoint["raw_sha256"])
    bundle_id = sha256(f"{start}:{start + count}:{digest}".encode()).hexdigest()
    store.state_db.put(
        StateKind.READY_PUBLICATION,
        bundle_id,
        {
            "records_sha256": digest,
            "state": "retired",
            "start_sequence": start,
            "next_sequence": start + count,
            "draft_raw_sha256": digest,
        },
    )


def test_zero_length_terminal_attempt_is_removed_with_state_row(tmp_path: Path) -> None:
    store = _store(tmp_path)
    attempt = _terminal_attempt(store)

    assert store.sweep_terminal_retired() == (attempt.path,)
    assert not attempt.path.exists()
    assert attempt_payload(store, attempt.attempt_id) is None


def test_exact_checkpointed_raw_is_removed_after_authenticated_draft_ack(tmp_path: Path) -> None:
    store = _store(tmp_path)
    attempt = _terminal_attempt(store, records=(_record(1), _record(2)))
    _ready_ack(store, attempt)

    assert store.sweep_terminal_retired() == (attempt.path,)
    assert attempt_payload(store, attempt.attempt_id) is None


def test_uncheckpointed_tail_keeps_terminal_attempt_and_state(tmp_path: Path) -> None:
    store = _store(tmp_path)
    attempt = store.prepare_streaming_attempt(100, 2)
    attempt.record_read_begin(ReadBeginNotification(100, 2))
    attempt.accept_chunk(100, memoryview(_record(1)))
    attempt.checkpoint()
    attempt.accept_chunk(101, memoryview(_record(2)))
    attempt.publish_prefix()
    attempt.close(durable=True)
    store.terminalize_prefix_attempt(attempt.attempt_id)

    assert store.sweep_terminal_retired() == ()
    assert attempt.path.exists()
    assert attempt_payload(store, attempt.attempt_id) is not None


@pytest.mark.parametrize("forged_field", ["draft_raw_sha256", "next_sequence"])
def test_forged_or_mismatched_draft_ack_keeps_terminal_attempt(tmp_path: Path, forged_field: str) -> None:
    store = _store(tmp_path)
    attempt = _terminal_attempt(store, records=(_record(1),))
    state = attempt_payload(store, attempt.attempt_id)
    assert state is not None
    checkpoint = cast(dict[str, object], state["checkpoint"])
    start = attempt.descriptor.start_sequence
    count = cast(int, checkpoint["record_count"])
    digest = cast(str, checkpoint["raw_sha256"])
    bundle_id = sha256(f"{start}:{start + count}:{digest}".encode()).hexdigest()
    ack = {
        "records_sha256": digest,
        "state": "retired",
        "start_sequence": start,
        "next_sequence": start + count,
        "draft_raw_sha256": digest,
    }
    ack[forged_field] = "0" * 64 if forged_field == "draft_raw_sha256" else start + count + 1
    store.state_db.put(StateKind.READY_PUBLICATION, bundle_id, ack)
    for published in _capture_root(tmp_path).iterdir():
        rmtree(published)

    assert store.sweep_terminal_retired() == ()
    assert attempt.path.exists()
    assert attempt_payload(store, attempt.attempt_id) is not None


def test_changed_published_prefix_bytes_block_terminal_cleanup(tmp_path: Path) -> None:
    store = _store(tmp_path)
    attempt = _terminal_attempt(store, records=(_record(1),))
    published = next(path for path in _capture_root(tmp_path).iterdir() if path.is_dir())
    raw = published / "records.bin"
    raw.write_bytes(bytes((raw.read_bytes()[0] ^ 1,)) + raw.read_bytes()[1:])

    assert store.sweep_terminal_retired() == ()
    assert attempt.path.exists()
    assert attempt_payload(store, attempt.attempt_id) is not None


def test_bad_checkpoint_hash_keeps_terminal_attempt_and_state(tmp_path: Path) -> None:
    store = _store(tmp_path)
    attempt = _terminal_attempt(store, records=(_record(1),))
    state = attempt_payload(store, attempt.attempt_id)
    assert state is not None
    checkpoint = cast(dict[str, object], state["checkpoint"])
    checkpoint["raw_sha256"] = "0" * 64
    store.state_db.put(StateKind.ATTEMPT, attempt.attempt_id, state)

    assert store.sweep_terminal_retired() == ()
    assert attempt.path.exists()
    assert attempt_payload(store, attempt.attempt_id) is not None


def test_ready_audio_across_normalization_ranges_authorizes_cleanup(tmp_path: Path) -> None:
    store = _store(tmp_path)
    attempt = _terminal_attempt(store, records=(_audio_record(100, 1), _audio_record(101, 2)))
    ready_root = tmp_path / "ready"
    ready_root.mkdir(mode=0o2750)
    ready_root.chmod(0o2750)
    store._publication_root = ready_root
    outcome = finalize_drafts(
        _capture_root(tmp_path),
        ready_root,
        store.state_db,
        ClockSegmentMap((ClockSegment("clock-a", 100, 101, 0.6, 0.1), ClockSegment("clock-b", 101, 102, 1.6, 0.1))),
        config=ReadyConfig(target_audio_seconds=0.02),
        frontier=102,
        drained=True,
    )
    assert len(outcome.published) == 1
    ready_manifest = cast(
        dict[str, object], loads((outcome.published[0].path / "manifest.json").read_text(encoding="utf-8"))
    )
    time_ranges = ready_manifest["time_ranges"]
    assert isinstance(time_ranges, list)
    assert len(time_ranges) == 2
    assert not any(_capture_root(tmp_path).iterdir())
    assert store.sweep_terminal_retired() == (attempt.path,)
    assert attempt_payload(store, attempt.attempt_id) is None


def test_interrupted_unlink_finalizes_only_persisted_cleanup_intent(tmp_path: Path) -> None:
    store = _store(tmp_path)
    attempt = _terminal_attempt(store, records=(_record(1),))
    store._filesystem._write_attempt_field(attempt.attempt_id, "cleanup_intent", dict(_ATTEMPT_CLEANUP_INTENT))
    (attempt.path / "records.bin").unlink()
    attempt.path.rmdir()

    assert store.sweep_terminal_retired() == ()
    assert attempt_payload(store, attempt.attempt_id) is None


@pytest.mark.parametrize("mode", ["normal", "proof_fails", "late_ack"])
def test_legacy_ready_raw_is_swept_before_ack_removes_its_last_copy(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mode: str
) -> None:
    store = _store(tmp_path)
    attempt = _terminal_attempt(store, records=(_audio_record(100, 1),))
    ready_root = tmp_path / "ready"
    ready_root.mkdir(mode=0o2750)
    ready_root.chmod(0o2750)
    store._publication_root = ready_root
    outcome = finalize_drafts(
        _capture_root(tmp_path),
        ready_root,
        store.state_db,
        ClockSegmentMap(()),
        config=ReadyConfig(target_audio_seconds=0.02),
        frontier=101,
        drained=True,
    )
    (published,) = outcome.published
    row = store.state_db.get(StateKind.READY_PUBLICATION, published.bundle_id)
    assert row is not None
    row.pop("sources")
    store.state_db.put(StateKind.READY_PUBLICATION, published.bundle_id, row)
    checkpoint = tmp_path / "work" / "omi-ready-checkpoint.json"
    checkpoint.parent.mkdir()
    checkpoint_payload = {
        "analysis_cursor": None,
        "vad_decisions": [],
        "open_speech_tail": None,
        "acknowledged": [{"bundle_id": published.bundle_id, "records_sha256": published.records_sha256}],
    }
    original_resume = staging_store.resume_retired
    if mode == "late_ack":
        checkpoint.write_text(dumps({**checkpoint_payload, "acknowledged": []}), encoding="utf-8")

        def add_ack_after_snapshot(*args: object) -> None:
            checkpoint.write_text(dumps(checkpoint_payload), encoding="utf-8")
            original_resume(*args)  # type: ignore[arg-type]

        monkeypatch.setattr(staging_store, "resume_retired", add_ack_after_snapshot)
    else:
        checkpoint.write_text(dumps(checkpoint_payload), encoding="utf-8")

    if mode == "proof_fails":
        monkeypatch.setattr(attempt_cleanup, "_terminal_decision", lambda *_args: (_ for _ in ()).throw(OSError()))
    with store.device_lock() as lease:
        store._filesystem.require_device_lock(lease)
        if mode == "proof_fails":
            with pytest.raises(ReadyBundleError, match="legacy ready retirement still has terminal raw"):
                store._recover_and_publish_unlocked()
        else:
            store._recover_and_publish_unlocked()

    if mode == "late_ack":
        assert attempt.path.exists() and published.path.exists()
        monkeypatch.setattr(staging_store, "resume_retired", original_resume)
        with store.device_lock():
            store._recover_and_publish_unlocked()
    retained = mode == "proof_fails"
    assert attempt.path.exists() is retained
    assert (attempt_payload(store, attempt.attempt_id) is not None) is retained
    assert published.path.exists() is retained


def test_mixed_retired_lineage_and_ready_audio_cover_one_attempt(tmp_path: Path) -> None:
    store = _store(tmp_path)
    records = (_audio_record(100, 1), _audio_record(101, 2))
    attempt = _terminal_attempt(store, records=records)
    draft_root = _capture_root(tmp_path)
    for old_draft in draft_root.iterdir():
        rmtree(old_draft)
    for sequence, record in zip((100, 101), records, strict=True):
        digest = sha256(record).hexdigest()
        draft = draft_root / f"{sequence}-{sequence + 1}-{digest[:16]}"
        draft.mkdir()
        (draft / "records.bin").write_bytes(record)
        manifest = BundleManifest(2, sequence, sequence + 1, 1, RECORD_SIZE, digest)
        (draft / "manifest.json").write_text(dumps(manifest.as_dict()), encoding="utf-8")
    ready_root = tmp_path / "ready"
    ready_root.mkdir(mode=0o2750)
    ready_root.chmod(0o2750)
    store._publication_root = ready_root
    config = ReadyConfig(target_audio_seconds=0.02)
    first = finalize_drafts(
        draft_root, ready_root, store.state_db, ClockSegmentMap(()), config=config, frontier=101, drained=True
    ).published[0]
    finalize_drafts(
        draft_root, ready_root, store.state_db, ClockSegmentMap(()), config=config, frontier=102, drained=True
    )
    first_row = store.state_db.get(StateKind.READY_PUBLICATION, first.bundle_id)
    assert first_row is not None and "sources" in first_row
    first_row["state"] = "retired"
    store.state_db.put(StateKind.READY_PUBLICATION, first.bundle_id, first_row)
    rmtree(first.path)

    assert store.sweep_terminal_retired() == (attempt.path,)
    assert attempt_payload(store, attempt.attempt_id) is None


def test_legacy_retirement_overlap_uses_checkpointed_span_not_reserved_read(tmp_path: Path) -> None:
    store = _store(tmp_path)
    attempt = store.prepare_streaming_attempt(100, 100)
    attempt.record_read_begin(ReadBeginNotification(100, 100))
    attempt.accept_chunk(100, memoryview(_audio_record(100, 1)))
    attempt.checkpoint()
    attempt.publish_prefix()
    attempt.close(durable=True)
    store.terminalize_prefix_attempt(attempt.attempt_id)

    assert attempt_cleanup.terminal_attempts_overlap(store._filesystem, ((100, 101),))
    assert not attempt_cleanup.terminal_attempts_overlap(store._filesystem, ((101, 102),))
