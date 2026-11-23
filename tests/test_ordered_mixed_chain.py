from __future__ import annotations

import json
from contextlib import closing
from pathlib import Path
from typing import cast

import pytest

from omi_collector.capture.adapters import physical_stage, ready_bundles
from omi_collector.capture.adapters.collector_state_db import StateDB, StateKind
from omi_collector.capture.adapters.confirmed_loss import ConfirmedLossLedger
from omi_collector.capture.adapters.internal_audio_store import InternalAudioStore
from omi_collector.capture.adapters.ordered_physical_plan import publish_next_ordered_physical
from omi_collector.capture.domain.clock_state import ClockState
from test_historical_publication_order import _hold_historical_source
from test_historical_release_publication import _classify
from test_public_audio_v2_integration import _checkpoint, _options, _ready_root, _record, _source


def _physical(store: InternalAudioStore, db: StateDB, source_id: str, start: int, values: tuple[int, ...]) -> None:
    source = _source(store, source_id, start, tuple(_record(n) for n in values))
    with db.typed_write_transaction() as connection:
        connection.execute(
            "UPDATE capture_source SET source_type='quarantine',lifecycle='blocked',"
            "quarantine_disposition='published_prefix',certified_prefix_count=committed_count,"
            "coverage_digest=committed_sha256 WHERE source_id=?",
            (source.source_id,),
        )
        connection.execute(
            "INSERT INTO capture_historical_custody SELECT source_id,source_type,lifecycle,committed_count,"
            "committed_sha256,certified_prefix_count,coverage_digest,'pending' FROM capture_source WHERE source_id=?",
            (source.source_id,),
        )


def _ack(root: Path, db: StateDB, tmp_path: Path, result: ready_bundles.ReadyBundleResult) -> None:
    manifest = cast(dict[str, object], json.loads((result.path / "manifest.json").read_bytes()))
    checkpoint_dir = tmp_path / result.bundle_id
    checkpoint_dir.mkdir()
    checkpoint = _checkpoint(
        checkpoint_dir, [{"id": result.bundle_id, "audio_sha256": cast(str, manifest["audio_sha256"])}]
    )
    assert len(ready_bundles.retire_acknowledged_public_audio_v2(root, db, checkpoint)) == 1


def _publish_live_after_physical(
    store: InternalAudioStore, db: StateDB, root: Path, tmp_path: Path, physical: ready_bundles.ReadyBundleResult
) -> None:
    live = _source(store, "e" * 32, 117, (_record(6),))
    frontier = ready_bundles.historical_admission_frontier(db, 118, frozenset({live.source_id}))
    assert frontier == 118
    outcome = ready_bundles.finalize_public_audio_v2(store, (live,), root, _options(ClockState(), frontier))
    assert outcome.published
    manifest = cast(dict[str, object], json.loads((outcome.published[0].path / "manifest.json").read_bytes()))
    assert "source_loss" not in json.dumps(manifest)
    _ack(root, db, tmp_path, physical)
    from omi_collector.capture.adapters.ordered_retirement import retire_one_ordered_physical_page

    for _ in range(5):
        if not retire_one_ordered_physical_page(store, 8):
            break
    with closing(db.open_connection(read_only=True)) as connection:
        assert (
            connection.execute("SELECT count(*) FROM capture_record WHERE source_id=?", ("c" * 32,)).fetchone()[0] == 0
        )
    markers = [
        (key, value)
        for key, value in db.list(StateKind.PHYSICAL_IMPORT)
        if value.get("artifact_id") == physical.bundle_id
    ]
    assert len(markers) == 1
    marker_id, marker = markers[0]
    damaged = cast(dict[str, object], json.loads(json.dumps(marker)))
    ordering = cast(dict[str, object], damaged["ordering"])
    ordering["source_start"] = 1000000
    db.put(StateKind.PHYSICAL_IMPORT, marker_id, damaged)
    with InternalAudioStore(StateDB(db.path)) as restarted:
        live_next = _source(restarted, "f" * 32, 118, (_record(7),))
        with pytest.raises(ready_bundles.ReadyBundleError, match="frontier is unreadable"):
            ready_bundles.historical_admission_frontier(db, 119, frozenset({live_next.source_id}))
        db.put(StateKind.PHYSICAL_IMPORT, marker_id, marker)
        frontier = ready_bundles.historical_admission_frontier(db, 119, frozenset({live_next.source_id}))
        assert frontier == 119
        assert ready_bundles.finalize_public_audio_v2(
            restarted, (live_next,), root, _options(ClockState(), frontier)
        ).published


@pytest.mark.parametrize("later_boundary", [False, True])
def test_blocked_numbered_frontier_releases_next_sealed_in_physical_order(
    tmp_path: Path, later_boundary: bool, monkeypatch: pytest.MonkeyPatch
) -> None:
    db = StateDB(tmp_path / "state.sqlite3")
    root = _ready_root(tmp_path)
    ledger = ConfirmedLossLedger(db)
    ledger.initialize(allow_create=True)
    with InternalAudioStore(db) as store:
        prefix = _source(store, "1" * 32, 100, (_record(1),))
        digest = prefix.source.committed_sha256
        with db.typed_write_transaction() as connection:
            connection.execute(
                "UPDATE capture_source SET source_type='quarantine',lifecycle='blocked',sequence_limit=?,"
                "block_reason='terminal prefix residual',quarantine_disposition='published_prefix',"
                "terminal_time='100',retention_deadline='200',certified_prefix_count=1,coverage_digest=? "
                "WHERE source_id=?",
                ((110).to_bytes(8, "big"), digest, prefix.source_id),
            )
            connection.execute("DELETE FROM capture_read_leg WHERE source_id=?", (prefix.source_id,))
        _hold_historical_source(db, prefix.source_id)
        ledger.record(prefix.source_id, 101, 103, "2026-01-01T00:00:00+00:00")
        _classify(db)
        assert ready_bundles.finalize_public_audio_v2(store, (prefix,), root, _options(ClockState(), 110)).published
        _physical(store, db, "a" * 32, 111, (2,))
        _physical(store, db, "b" * 32, 113, (3,))
        sealed = _source(store, "c" * 32, 115, (_record(4),))
        _hold_historical_source(db, sealed.source_id)
        if later_boundary:
            _physical(store, db, "d" * 32, 120, (5,))
        _classify(db, frozenset({"a" * 32, "b" * 32, "d" * 32}))
        first = publish_next_ordered_physical(store, root)
        second = publish_next_ordered_physical(store, root)
        assert first not in (None, True) and second not in (None, True)
        assert ready_bundles.historical_admission_frontier(db, 116, frozenset({sealed.source_id})) == 103
        concurrent = _source(store, "e" * 32, 115, (_record(8),))
        assert publish_next_ordered_physical(store, root) is None
        with db.typed_write_transaction() as connection:
            connection.execute("DELETE FROM capture_source WHERE source_id=?", (concurrent.source_id,))
        from omi_collector.capture.adapters import ordered_physical_plan

        with monkeypatch.context() as patch:
            decisions = iter((True, False))
            patch.setattr(ordered_physical_plan, "_historical_frontier_blocks_payload", lambda *_: next(decisions))
            with pytest.raises(ready_bundles.ReadyBundleError, match="frontier changed"):
                publish_next_ordered_physical(store, root)
        with monkeypatch.context() as patch:
            patch.setattr(physical_stage, "_commit_physical_stage", lambda *_: (_ for _ in ()).throw(OSError("crash")))
            with pytest.raises(OSError, match="crash"):
                publish_next_ordered_physical(store, root)
        third = physical_stage.resume_physical_stage(store, root)
        assert third not in (None, True) and third.next_sequence is None
        entry = db.get(StateKind.READY_PUBLICATION, third.bundle_id)
        assert entry is not None and entry["source_id"] == sealed.source_id
        manifest = cast(dict[str, object], json.loads((third.path / "manifest.json").read_bytes()))
        assert "source_loss" not in json.dumps(manifest)
        if not later_boundary:
            assert publish_next_ordered_physical(store, root) is None
            _publish_live_after_physical(store, db, root, tmp_path, third)


@pytest.mark.parametrize("overlap", [False, True])
def test_guarded_ordered_chain_survives_ack_and_restart(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, overlap: bool
) -> None:
    db = StateDB(tmp_path / "state.sqlite3")
    root = _ready_root(tmp_path)
    ConfirmedLossLedger(db).initialize(allow_create=True)
    with InternalAudioStore(db) as store:
        first = _source(store, "1" * 32, 100, (_record(1),))
        second = _source(store, "2" * 32, 101, (_record(2),))
        initial = ready_bundles.finalize_public_audio_v2(store, (first, second), root, _options(ClockState(), 102))
        assert len(initial.published) == 1
        _physical(store, db, "a" * 32, 102, (3, 4))
        _physical(store, db, "b" * 32, 104, (4 if overlap else 5, 6))
        successor = _source(store, "3" * 32, 106, (_record(7),))
        _classify(db, frozenset({"a" * 32, "b" * 32}))
        boundary = ready_bundles.historical_admission_frontier(db, 107, frozenset({successor.source_id}))
        assert boundary == 102
        assert not ready_bundles.finalize_public_audio_v2(
            store, (successor,), root, _options(ClockState(), boundary)
        ).published
        _ack(root, db, tmp_path, initial.published[0])
        for _ in range(5):
            ready_bundles.retire_one_covered_typed_source(store, 8)
        assert store.get_source(second.source_id) is not None
        first_physical = publish_next_ordered_physical(store, root)
        assert first_physical is not None and first_physical is not True
        with monkeypatch.context() as patch:
            patch.setattr(physical_stage, "_commit_physical_stage", lambda *_: (_ for _ in ()).throw(OSError("crash")))
            with pytest.raises(OSError, match="crash"):
                publish_next_ordered_physical(store, root)
    with InternalAudioStore(StateDB(db.path)) as store:
        resumed = physical_stage.resume_physical_stage(store, root)
        assert resumed is not None
        assert first_physical.next_sequence is None and resumed.next_sequence is None
        assert publish_next_ordered_physical(store, root) is None
        final_boundary = ready_bundles.historical_admission_frontier(db, 107, frozenset({successor.source_id}))
        assert final_boundary == 107
        final = ready_bundles.finalize_public_audio_v2(
            store, (successor,), root, _options(ClockState(), final_boundary)
        )
        assert final.published
        entries = [(key, value) for key, value in db.list(StateKind.READY_PUBLICATION) if key != "frontier"]
        entries.sort(key=lambda item: cast(int, item[1]["order"]))
        assert [entry["order"] for _, entry in entries] == list(range(len(entries)))
        physical = [entry for _, entry in entries if entry.get("lineage_kind") == "physical_byte_span"]
        assert [entry["source_id"] for entry in physical] == ["a" * 32, "b" * 32]
        assert physical[1]["record_count"] == (1 if overlap else 2)
        assert all("start_sequence" not in entry and "next_sequence" not in entry for entry in physical)
        assert ConfirmedLossLedger(db).read() == ()
        manifests = [
            cast(dict[str, object], json.loads((item.path / "manifest.json").read_bytes()))
            for item in (first_physical, resumed, *final.published)
        ]
        for manifest in manifests:
            assert all(
                "source_loss" not in cast(list[str], boundary["facts"])
                for boundary in cast(list[dict[str, object]], manifest["boundaries"])
            )
        for result in (first_physical, resumed, *final.published):
            _ack(root, db, tmp_path, result)
        for _ in range(10):
            if not ready_bundles.retire_one_covered_typed_source(store, 8):
                break
        assert store.get_source(second.source_id) is None
