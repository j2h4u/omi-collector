from __future__ import annotations

import json
import sqlite3
from contextlib import closing
from dataclasses import replace
from pathlib import Path
from typing import cast

import pytest

from omi_collector.capture.adapters import ordered_boundary, ordered_payloads, ready_bundles
from omi_collector.capture.adapters.collector_state_db import StateDB
from omi_collector.capture.adapters.internal_audio_store import InternalAudioStore
from omi_collector.capture.domain.clock_state import ClockState
from test_public_audio_v2_integration import _options, _ready_root, _record, _source


def test_ordered_ledger_validates_v1_and_v2_before_selecting_v2(tmp_path: Path) -> None:
    from omi_collector.capture.adapters.collector_state_db import StateKind

    db = StateDB(tmp_path / "state.sqlite3")
    facts = ready_bundles._BundleFacts(0, 1, 1, "a" * 64, ())
    legacy_id = ready_bundles._bundle_id(facts)
    legacy = ready_bundles._ledger_entry("b" * 64, "retired", facts)
    db.put(StateKind.READY_PUBLICATION, legacy_id, legacy)
    with closing(db.open_connection(read_only=True)) as connection:
        assert ordered_boundary._ledger(connection) == {}
        assert ordered_boundary.ordered_physical_boundary(connection, "c" * 32, 0) is None
    db.put(StateKind.READY_PUBLICATION, "d" * 64, {"format": "omi-audio-v2", "records_sha256": "e" * 64})
    with (
        closing(db.open_connection(read_only=True)) as connection,
        pytest.raises(ValueError, match="ledger is invalid"),
    ):
        ordered_boundary._ledger(connection)


def test_drained_flush_stops_before_pending_physical_and_preserves_other_batching(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    db = StateDB(tmp_path / "state.sqlite3")
    root = _ready_root(tmp_path)
    monkeypatch.setattr(ordered_payloads, "next_ordered_physical_start", lambda _connection: 20, raising=False)
    monkeypatch.setattr(
        ordered_payloads, "pending_physical_successor", lambda _connection, source_id: source_id == "b" * 32
    )
    with InternalAudioStore(db) as store:
        earlier = _source(store, "a" * 32, 10, (_record(10),))
        predecessor = _source(store, "b" * 32, 11, (_record(11),))
        later = _source(store, "c" * 32, 30, (_record(30),))
        waiting = replace(_options(ClockState(), 40, threshold=3600), frontier=None, drained=False)
        assert not ready_bundles.finalize_public_audio_v2(store, (earlier, predecessor, later), root, waiting).published
        result = ready_bundles.finalize_public_audio_v2(
            store, (earlier, predecessor, later), root, _options(ClockState(), 40, threshold=3600)
        )
        assert result.published and result.published[-1].next_sequence == 12
        retained = store.get_source(later.source_id)
        assert retained is not None and retained.lifecycle == "sealed_waiting"
        for _ in range(10):
            if not ready_bundles.retire_one_covered_typed_source(store, 8):
                break
        assert store.get_source(earlier.source_id) is None
        assert store.get_source(predecessor.source_id) is not None
        monkeypatch.setattr(ordered_payloads, "pending_physical_successor", lambda *_: False)
        for _ in range(10):
            if not ready_bundles.retire_one_covered_typed_source(store, 8):
                break
        assert store.get_source(predecessor.source_id) is None
        monkeypatch.setattr(ordered_payloads, "next_ordered_physical_start", lambda *_: None, raising=False)
        assert not ready_bundles.finalize_public_audio_v2(
            store, (later,), root, _options(ClockState(), 40, threshold=3600)
        ).published


def test_ordered_boundary_fails_closed_for_broken_chain_and_intervening_source(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connection = sqlite3.connect(":memory:")
    connection.executescript(
        "CREATE TABLE state(kind TEXT,id TEXT,payload TEXT); CREATE TABLE capture_source(source_id TEXT,sequence_start BLOB,committed_count INT,lifecycle TEXT);"
    )
    predecessor = "a" * 64
    physical = "b" * 64
    marker_id = "c" * 64
    repeat_id = "d" * 32
    digest = "e" * 64

    def entry(item: str, order: int, physical_entry: bool) -> None:
        connection.execute(
            "INSERT INTO state VALUES (?,?,?)",
            (
                "ready_publication",
                item,
                json.dumps(
                    {
                        "format": "omi-audio-v2",
                        "order": order,
                        "lineage_kind": "physical_byte_span" if physical_entry else "normal",
                    }
                ),
            ),
        )

    entry(predecessor, 1, False)
    entry(physical, 2, True)
    marker = {
        "state": "published",
        "artifact_id": physical,
        "ordering": {
            "predecessor_artifact": predecessor,
            "predecessor_start": 90,
            "predecessor_sha256": digest,
            "predecessor_source_id": "f" * 32,
        },
    }
    connection.execute("INSERT INTO state VALUES (?,?,?)", ("physical_import", marker_id, json.dumps(marker)))
    connection.execute(
        "INSERT INTO capture_source VALUES (?,?,?,?)", (repeat_id, (120).to_bytes(8, "big"), 1, "sealed_waiting")
    )
    monkeypatch.setattr(ready_bundles, "_valid_ledger_entry", lambda *_: True)
    monkeypatch.setattr(ready_bundles, "source_lineage", lambda *_: ((90, 100, digest),))
    monkeypatch.setattr(
        ordered_boundary,
        "ordered_physical_predecessor",
        lambda *_: ordered_payloads.OrderedPredecessor(physical, "1" * 32, digest, 110, 444),
    )
    from omi_collector.capture.adapters import ordered_full_replay

    monkeypatch.setattr(ordered_full_replay, "completed_ordered_replays", lambda *_: frozenset())
    monkeypatch.setattr(ordered_boundary, "published_ordered_sources", lambda *_: frozenset({"1" * 32}))
    assert ordered_boundary.ordered_physical_boundary(connection, repeat_id, 99) == marker_id
    connection.execute(
        "INSERT INTO capture_source VALUES (?,?,?,?)", ("2" * 32, (115).to_bytes(8, "big"), 1, "blocked")
    )
    with pytest.raises(ValueError, match="unresolved earlier source"):
        ordered_boundary.ordered_physical_boundary(connection, repeat_id, 99)
    connection.execute("DELETE FROM capture_source WHERE source_id=?", ("2" * 32,))
    marker["ordering"]["predecessor_sha256"] = "0" * 64
    connection.execute("UPDATE state SET payload=? WHERE id=?", (json.dumps(marker), marker_id))
    with pytest.raises(ValueError, match="predecessor changed"):
        ordered_boundary.ordered_physical_boundary(connection, repeat_id, 99)
    connection.close()


def test_real_physical_ack_then_numbered_audio_has_no_invented_loss(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from omi_collector.capture.adapters.ordered_physical_plan import publish_next_ordered_physical
    from test_ordered_payloads import _retain
    from test_public_audio_v2_integration import _checkpoint

    monkeypatch.setattr(ordered_payloads, "historical_decision", lambda *_: "unknown_held")
    db = StateDB(tmp_path / "state.sqlite3")
    root = _ready_root(tmp_path)
    with InternalAudioStore(db) as store:
        previous = _source(store, "d" * 32, 100, (_record(1),))
        ready_bundles.finalize_public_audio_v2(store, (previous,), root, _options(ClockState(), 101))
        _retain(store, db, "a" * 32, 110)
        physical = publish_next_ordered_physical(store, root)
        assert physical is not None and physical is not True
        manifest = cast(dict[str, str], json.loads((physical.path / "manifest.json").read_bytes()))
        checkpoint = _checkpoint(tmp_path, [{"id": physical.bundle_id, "audio_sha256": manifest["audio_sha256"]}])
        assert len(ready_bundles.retire_acknowledged_public_audio_v2(root, db, checkpoint)) == 1
        from omi_collector.capture.adapters.confirmed_loss import ConfirmedLossLedger
        from test_historical_publication_order import _hold_historical_source
        from test_historical_release_publication import _classify

        successor = _source(store, "b" * 32, 120, (_record(2),))
        successor_digest = successor.source.committed_sha256
        with db.typed_write_transaction() as connection:
            connection.execute(
                "UPDATE capture_source SET source_type='quarantine',lifecycle='blocked',sequence_limit=?,"
                "block_reason='terminal prefix residual',quarantine_disposition='published_prefix',"
                "terminal_time='100',retention_deadline='200',certified_prefix_count=1,coverage_digest=? "
                "WHERE source_id=?",
                ((125).to_bytes(8, "big"), successor_digest, successor.source_id),
            )
            connection.execute("DELETE FROM capture_read_leg WHERE source_id=?", (successor.source_id,))
        _hold_historical_source(db, successor.source_id)
        _retain(store, db, "c" * 32, 130)
        ledger = ConfirmedLossLedger(db)
        ledger.initialize(allow_create=True)
        ledger.record(successor.source_id, 121, 122, "2026-01-01T00:00:00+00:00")
        _classify(db, frozenset({"a" * 32, "c" * 32}))
        successor = ready_bundles.SqlitePublicationSource(store, successor.source_id)
        result = ready_bundles.finalize_public_audio_v2(
            store, (successor,), root, _options(ClockState(), 131, threshold=3600)
        )
        assert result.published
        document = cast(dict[str, object], json.loads((result.published[-1].path / "manifest.json").read_bytes()))
        assert "source_loss" not in json.dumps(document)
        assert "time_incomparable" in json.dumps(document)


def test_replay_receipt_holds_only_referenced_retiring_source(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from omi_collector.capture.adapters import ordered_full_replay

    db = StateDB(tmp_path / "state.sqlite3")
    root = _ready_root(tmp_path)
    monkeypatch.setattr(ordered_full_replay, "has_ordered_replay_reference", lambda _, source: source == "b" * 32)
    with InternalAudioStore(db) as store:
        earlier = _source(store, "a" * 32, 10, (_record(10),))
        referenced = _source(store, "b" * 32, 11, (_record(11),))
        assert ready_bundles.finalize_public_audio_v2(
            store, (earlier, referenced), root, _options(ClockState(), 12)
        ).published
        for _ in range(10):
            if not ready_bundles.retire_one_covered_typed_source(store, 8):
                break
        assert store.get_source(earlier.source_id) is None
        assert store.get_source(referenced.source_id) is not None
        monkeypatch.setattr(ordered_full_replay, "has_ordered_replay_reference", lambda *_: False)
        for _ in range(10):
            if not ready_bundles.retire_one_covered_typed_source(store, 8):
                break
        assert store.get_source(referenced.source_id) is None


def test_full_replay_after_physical_ack_allows_numbered_audio_after_restart(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from typing import cast

    from omi_collector.capture.adapters.collector_state_db import StateKind
    from omi_collector.capture.adapters.ordered_physical_plan import publish_next_ordered_physical
    from test_ordered_payloads import _retain
    from test_public_audio_v2_integration import _checkpoint

    monkeypatch.setattr(ordered_payloads, "historical_decision", lambda *_: "unknown_held")
    db = StateDB(tmp_path / "state.sqlite3")
    root = _ready_root(tmp_path)
    with InternalAudioStore(db) as store:
        previous = _source(store, "d" * 32, 100, (_record(1),))
        ready_bundles.finalize_public_audio_v2(store, (previous,), root, _options(ClockState(), 101))
        _retain(store, db, "a" * 32, 110)
        physical = publish_next_ordered_physical(store, root)
        assert physical is not None and physical is not True
        replay = _source(store, "c" * 32, 130, (_record(110),))
        with db.typed_write_transaction() as connection:
            connection.execute(
                "UPDATE capture_source SET source_type='quarantine',lifecycle='blocked',quarantine_disposition='published_prefix',certified_prefix_count=committed_count,coverage_digest=committed_sha256 WHERE source_id=?",
                (replay.source_id,),
            )
            connection.execute(
                "INSERT INTO capture_historical_custody SELECT source_id,source_type,lifecycle,committed_count,committed_sha256,certified_prefix_count,coverage_digest,'pending' FROM capture_source WHERE source_id=?",
                (replay.source_id,),
            )
        assert publish_next_ordered_physical(store, root) is True
        manifest = cast(dict[str, str], json.loads((physical.path / "manifest.json").read_bytes()))
        checkpoint = _checkpoint(tmp_path, [{"id": physical.bundle_id, "audio_sha256": manifest["audio_sha256"]}])
        assert len(ready_bundles.retire_acknowledged_public_audio_v2(root, db, checkpoint)) == 1
        successor = _source(store, "b" * 32, 140, (_record(2),))
    with InternalAudioStore(StateDB(db.path)) as restarted:
        receipt = db.get(StateKind.ORDERED_FULL_REPLAY, replay.source_id)
        assert receipt is not None
        broken = dict(receipt)
        broken["predecessor_offset"] = -1
        db.put(StateKind.ORDERED_FULL_REPLAY, replay.source_id, broken)
        with pytest.raises(ready_bundles.ReadyBundleError, match="historical publication frontier"):
            ready_bundles.finalize_public_audio_v2(restarted, (successor,), root, _options(ClockState(), 141))
        db.put(StateKind.ORDERED_FULL_REPLAY, replay.source_id, receipt)
        result = ready_bundles.finalize_public_audio_v2(restarted, (successor,), root, _options(ClockState(), 141))
        assert result.published
        document = cast(dict[str, object], json.loads((result.published[-1].path / "manifest.json").read_bytes()))
        assert "source_loss" not in json.dumps(document)
        assert "time_incomparable" in json.dumps(document)


def test_physical_boundary_at_admitted_prefix_preserves_normal_batching(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    db = StateDB(tmp_path / "state.sqlite3")
    root = _ready_root(tmp_path)
    monkeypatch.setattr(ordered_payloads, "next_ordered_physical_start", lambda _: 10)
    with InternalAudioStore(db) as store:
        prefix = _source(store, "a" * 32, 10, (_record(10),))
        later = _source(store, "b" * 32, 11, (_record(11),))
        waiting = ready_bundles.finalize_public_audio_v2(
            store, (prefix, later), root, _options(ClockState(), 12, threshold=3600)
        )
        assert not waiting.published
        released = ready_bundles.finalize_public_audio_v2(store, (prefix, later), root, _options(ClockState(), 12))
        assert released.published[-1].next_sequence == 12
