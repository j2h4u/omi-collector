from __future__ import annotations

import json
from pathlib import Path
from typing import cast

import pytest

from omi_collector.capture.adapters import ordered_payloads, ready_bundles
from omi_collector.capture.adapters.collector_state_db import StateDB
from omi_collector.capture.adapters.internal_audio_store import InternalAudioStore
from omi_collector.capture.adapters.ordered_physical_plan import publish_next_ordered_physical
from omi_collector.capture.adapters.ordered_retirement import retire_one_ordered_physical_page
from omi_collector.capture.domain.clock_state import ClockState
from test_ordered_payloads import _retain
from test_public_audio_v2_integration import _checkpoint, _options, _ready_root, _record, _source


def test_ack_and_successor_are_required_before_retiring_bounded_physical_pages(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(ordered_payloads, "historical_decision", lambda *_: "unknown_held")
    db = StateDB(tmp_path / "state.sqlite3")
    root = _ready_root(tmp_path)
    with InternalAudioStore(db) as store:
        prior = _source(store, "d" * 32, 100, (_record(1),))
        ready_bundles.finalize_public_audio_v2(store, (prior,), root, _options(ClockState(), 101))
        _retain(store, db, "a" * 32, 110)
        _retain(store, db, "b" * 32, 130)
        first = publish_next_ordered_physical(store, root)
        assert first is not None and first is not True
        assert not retire_one_ordered_physical_page(store, 1)
        manifest = cast(dict[str, str], json.loads((first.path / "manifest.json").read_bytes()))
        checkpoint = _checkpoint(tmp_path, [{"id": first.bundle_id, "audio_sha256": manifest["audio_sha256"]}])
        assert len(ready_bundles.retire_acknowledged_public_audio_v2(root, db, checkpoint)) == 1
        assert not retire_one_ordered_physical_page(store, 1)
        second = publish_next_ordered_physical(store, root)
        assert second is not None and second is not True
        with db.typed_write_transaction() as connection:
            connection.execute(
                "INSERT INTO capture_tail(source_id,leg_generation,complete_high_water,received_high_water,byte_offset,bytes) "
                "VALUES (?,0,1,445,444,?)",
                ("a" * 32, b"x"),
            )
        assert not retire_one_ordered_physical_page(store, 1)
        with db.typed_write_transaction() as connection:
            connection.execute("DELETE FROM capture_tail WHERE source_id=?", ("a" * 32,))
            connection.execute(
                "INSERT INTO capture_record(source_id,ordinal,device_timestamp,payload) "
                "SELECT source_id,1,device_timestamp,payload FROM capture_record WHERE source_id=? AND ordinal=0",
                ("a" * 32,),
            )
        with pytest.raises(ValueError, match="extra records"):
            retire_one_ordered_physical_page(store, 1)
        assert len(store.read_record_page("a" * 32, -1, 8)) == 2
        with db.typed_write_transaction() as connection:
            connection.execute("DELETE FROM capture_record WHERE source_id=? AND ordinal=1", ("a" * 32,))
        assert retire_one_ordered_physical_page(store, 1)
        assert not retire_one_ordered_physical_page(store, 1)
        assert store.read_record_page("a" * 32, -1, 8) == ()
        assert len(store.read_record_page("b" * 32, -1, 8)) == 1
        assert store.get_source("a" * 32) is not None
        monkeypatch.setattr(ready_bundles, "_historical_decision", lambda *_: "legacy_custody")
        reader = ready_bundles.SqlitePublicationSource(store, "a" * 32)
        assert ready_bundles._typed_source_admissions(store, (reader,), None) == ()


def test_numbered_successor_receipt_survives_ack_and_allows_physical_cleanup(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(ordered_payloads, "historical_decision", lambda *_: "unknown_held")
    db = StateDB(tmp_path / "state.sqlite3")
    root = _ready_root(tmp_path)
    with InternalAudioStore(db) as store:
        prior = _source(store, "d" * 32, 100, (_record(1),))
        ready_bundles.finalize_public_audio_v2(store, (prior,), root, _options(ClockState(), 101))
        _retain(store, db, "a" * 32, 110)
        first = publish_next_ordered_physical(store, root)
        assert first is not None and first is not True
        manifest = cast(dict[str, str], json.loads((first.path / "manifest.json").read_bytes()))
        checkpoint = _checkpoint(tmp_path, [{"id": first.bundle_id, "audio_sha256": manifest["audio_sha256"]}])
        assert len(ready_bundles.retire_acknowledged_public_audio_v2(root, db, checkpoint)) == 1
        later = _source(store, "b" * 32, 120, (_record(2),))
        result = ready_bundles.finalize_public_audio_v2(store, (later,), root, _options(ClockState(), 121))
        assert result.published
        manifest = cast(dict[str, str], json.loads((result.published[0].path / "manifest.json").read_bytes()))
        next_checkpoint_root = tmp_path / "second_checkpoint"
        next_checkpoint_root.mkdir()
        checkpoint = _checkpoint(
            next_checkpoint_root, [{"id": result.published[0].bundle_id, "audio_sha256": manifest["audio_sha256"]}]
        )
        assert len(ready_bundles.retire_acknowledged_public_audio_v2(root, db, checkpoint)) == 1
        assert retire_one_ordered_physical_page(store, 1)
        assert store.read_record_page("a" * 32, -1, 8) == ()


def test_physical_predecessor_can_retire_while_replay_receipt_remains_valid(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from contextlib import closing

    from omi_collector.capture.adapters.ordered_full_replay import completed_ordered_replays

    monkeypatch.setattr(ordered_payloads, "historical_decision", lambda *_: "unknown_held")
    db = StateDB(tmp_path / "state.sqlite3")
    root = _ready_root(tmp_path)
    with InternalAudioStore(db) as store:
        prior = _source(store, "d" * 32, 100, (_record(1),))
        ready_bundles.finalize_public_audio_v2(store, (prior,), root, _options(ClockState(), 101))
        _retain(store, db, "a" * 32, 110)
        first = publish_next_ordered_physical(store, root)
        assert first is not None and first is not True
        replay = _source(store, "b" * 32, 130, (_record(110),))
        with db.typed_write_transaction() as connection:
            connection.execute(
                "UPDATE capture_source SET source_type='quarantine',lifecycle='blocked',"
                "quarantine_disposition='published_prefix',certified_prefix_count=committed_count,"
                "coverage_digest=committed_sha256 WHERE source_id=?",
                (replay.source_id,),
            )
            connection.execute(
                "INSERT INTO capture_historical_custody SELECT source_id,source_type,lifecycle,committed_count,"
                "committed_sha256,certified_prefix_count,coverage_digest,'pending' FROM capture_source WHERE source_id=?",
                (replay.source_id,),
            )
        assert publish_next_ordered_physical(store, root) is True
        manifest = cast(dict[str, str], json.loads((first.path / "manifest.json").read_bytes()))
        checkpoint = _checkpoint(tmp_path, [{"id": first.bundle_id, "audio_sha256": manifest["audio_sha256"]}])
        assert len(ready_bundles.retire_acknowledged_public_audio_v2(root, db, checkpoint)) == 1
        _retain(store, db, "c" * 32, 150)
        second = publish_next_ordered_physical(store, root)
        assert second is not None and second is not True
        assert retire_one_ordered_physical_page(store, 1)
        assert retire_one_ordered_physical_page(store, 1)
        assert not retire_one_ordered_physical_page(store, 1)
        assert store.read_record_page(replay.source_id, -1, 8) == ()
        with closing(db.open_connection(read_only=True)) as connection:
            connection.execute("BEGIN")
            assert completed_ordered_replays(connection) == frozenset({replay.source_id})


def test_numbered_predecessor_and_full_replay_release_after_physical_successor(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from contextlib import closing

    from omi_collector.capture.adapters.ordered_full_replay import completed_ordered_replays

    monkeypatch.setattr(ordered_payloads, "historical_decision", lambda *_: "unknown_held")
    db = StateDB(tmp_path / "state.sqlite3")
    root = _ready_root(tmp_path)
    with InternalAudioStore(db) as store:
        prior = _source(store, "d" * 32, 100, (_record(110),))
        published = ready_bundles.finalize_public_audio_v2(store, (prior,), root, _options(ClockState(), 101))
        _retain(store, db, "a" * 32, 110)
        assert publish_next_ordered_physical(store, root) is True
        first = published.published[0]
        manifest = cast(dict[str, str], json.loads((first.path / "manifest.json").read_bytes()))
        checkpoint = _checkpoint(tmp_path, [{"id": first.bundle_id, "audio_sha256": manifest["audio_sha256"]}])
        assert len(ready_bundles.retire_acknowledged_public_audio_v2(root, db, checkpoint)) == 1
        _retain(store, db, "b" * 32, 130)
        successor = publish_next_ordered_physical(store, root)
        assert successor is not None and successor is not True
        while ready_bundles.retire_one_covered_typed_source(store, 8):
            pass
        assert store.get_source(prior.source_id) is None
        assert retire_one_ordered_physical_page(store, 8)
        assert store.read_record_page("a" * 32, -1, 8) == ()
        with closing(db.open_connection(read_only=True)) as connection:
            connection.execute("BEGIN")
            assert completed_ordered_replays(connection) == frozenset({"a" * 32})


def test_physical_page_cursor_survives_restart_and_rejects_tampering(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from omi_collector.capture.adapters.collector_state_db import StateKind

    monkeypatch.setattr(ordered_payloads, "historical_decision", lambda *_: "unknown_held")
    db = StateDB(tmp_path / "state.sqlite3")
    root = _ready_root(tmp_path)
    with InternalAudioStore(db) as store:
        prior = _source(store, "d" * 32, 100, (_record(1),))
        ready_bundles.finalize_public_audio_v2(store, (prior,), root, _options(ClockState(), 101))
        source = _source(store, "a" * 32, 110, (_record(110), _record(111), _record(112)))
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
        first = publish_next_ordered_physical(store, root)
        assert first is not None and first is not True
        manifest = cast(dict[str, str], json.loads((first.path / "manifest.json").read_bytes()))
        checkpoint = _checkpoint(tmp_path, [{"id": first.bundle_id, "audio_sha256": manifest["audio_sha256"]}])
        assert len(ready_bundles.retire_acknowledged_public_audio_v2(root, db, checkpoint)) == 1
        _retain(store, db, "b" * 32, 130)
        assert publish_next_ordered_physical(store, root) not in (None, True)
        assert retire_one_ordered_physical_page(store, 1)
    with InternalAudioStore(StateDB(db.path)) as restarted:
        assert retire_one_ordered_physical_page(restarted, 1)
        receipt = db.get(StateKind.ORDERED_RETIREMENT, source.source_id)
        assert receipt is not None and receipt["cursor"] == 2
        receipt["sha256"] = "0" * 64
        db.put(StateKind.ORDERED_RETIREMENT, source.source_id, receipt)
        with pytest.raises(ValueError, match="receipt changed"):
            retire_one_ordered_physical_page(restarted, 1)
        assert len(restarted.read_record_page(source.source_id, -1, 8)) == 1


@pytest.mark.parametrize("crash", [False, True])
def test_numbered_replay_and_predecessor_release_after_numbered_successor(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, crash: bool
) -> None:
    from contextlib import closing

    from omi_collector.capture.adapters.ordered_full_replay import completed_ordered_replays

    monkeypatch.setattr(ordered_payloads, "historical_decision", lambda *_: "unknown_held")
    db = StateDB(tmp_path / "state.sqlite3")
    root = _ready_root(tmp_path)
    with InternalAudioStore(db) as store:
        prior = _source(store, "d" * 32, 100, (_record(110),))
        first = ready_bundles.finalize_public_audio_v2(store, (prior,), root, _options(ClockState(), 101)).published[0]
        _retain(store, db, "a" * 32, 110)
        assert publish_next_ordered_physical(store, root) is True
        manifest = cast(dict[str, str], json.loads((first.path / "manifest.json").read_bytes()))
        checkpoint = _checkpoint(tmp_path, [{"id": first.bundle_id, "audio_sha256": manifest["audio_sha256"]}])
        assert len(ready_bundles.retire_acknowledged_public_audio_v2(root, db, checkpoint)) == 1
        later = _source(store, "b" * 32, 130, (_record(130),))
        if crash:
            with monkeypatch.context() as patch:
                patch.setattr(ready_bundles, "_commit_v2_stage", lambda *_: (_ for _ in ()).throw(OSError("crash")))
                with pytest.raises(OSError, match="crash"):
                    ready_bundles.finalize_public_audio_v2(store, (later,), root, _options(ClockState(), 131))
        result = ready_bundles.finalize_public_audio_v2(store, (later,), root, _options(ClockState(), 131))
        assert result.published
        document = cast(dict[str, object], json.loads((result.published[0].path / "manifest.json").read_bytes()))
        assert "source_loss" not in json.dumps(document)
        assert "time_incomparable" in json.dumps(document)
        while ready_bundles.retire_one_covered_typed_source(store, 8):
            pass
        assert store.get_source(prior.source_id) is None
        assert retire_one_ordered_physical_page(store, 8)
        assert store.read_record_page("a" * 32, -1, 8) == ()
        with closing(db.open_connection(read_only=True)) as connection:
            connection.execute("BEGIN")
            assert completed_ordered_replays(connection) == frozenset({"a" * 32})


def test_full_replay_inside_prior_numbered_range_does_not_imply_loss(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(ordered_payloads, "historical_decision", lambda *_: "unknown_held")
    db = StateDB(tmp_path / "state.sqlite3")
    root = _ready_root(tmp_path)
    with InternalAudioStore(db) as store:
        prior = _source(store, "d" * 32, 100, (_record(100), _record(101), _record(102)))
        ready_bundles.finalize_public_audio_v2(store, (prior,), root, _options(ClockState(), 103))
        _retain(store, db, "a" * 32, 101)
        assert publish_next_ordered_physical(store, root) is True
        later = _source(store, "b" * 32, 130, (_record(130),))
        result = ready_bundles.finalize_public_audio_v2(store, (later,), root, _options(ClockState(), 131))
        assert result.published
        document = cast(dict[str, object], json.loads((result.published[0].path / "manifest.json").read_bytes()))
        assert "source_loss" not in json.dumps(document)
        assert "time_incomparable" in json.dumps(document)
