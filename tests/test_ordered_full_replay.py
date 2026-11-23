from __future__ import annotations

import json
from contextlib import closing
from pathlib import Path
from typing import cast

import pytest

from omi_collector.capture.adapters import ordered_full_replay, ordered_payloads, ready_bundles
from omi_collector.capture.adapters.collector_state_db import StateDB, StateKind
from omi_collector.capture.adapters.internal_audio_store import InternalAudioStore
from omi_collector.capture.domain.clock_state import ClockState
from test_public_audio_v2_integration import _checkpoint, _options, _ready_root, _record, _source


def _seed(
    store: InternalAudioStore, db: StateDB, tmp_path: Path, *, repeated: bool = True
) -> ordered_payloads.OrderedPayload:
    root = _ready_root(tmp_path)
    previous = _source(store, "d" * 32, 100, (_record(1), _record(2)))
    result = ready_bundles.finalize_public_audio_v2(store, (previous,), root, _options(ClockState(), 102))
    result = result.published[0]
    source = _source(store, "a" * 32, 110, (_record(2 if repeated else 3),))
    with db.typed_write_transaction() as connection:
        connection.execute(
            "UPDATE capture_source SET source_type='quarantine',lifecycle='blocked',quarantine_disposition='published_prefix',certified_prefix_count=committed_count,coverage_digest=committed_sha256 WHERE source_id=?",
            (source.source_id,),
        )
        connection.execute(
            "INSERT INTO capture_historical_custody SELECT source_id,source_type,lifecycle,committed_count,committed_sha256,certified_prefix_count,coverage_digest,'pending' FROM capture_source WHERE source_id=?",
            (source.source_id,),
        )
    manifest = cast(dict[str, object], json.loads((result.path / "manifest.json").read_bytes()))
    checkpoint = _checkpoint(tmp_path, [{"id": result.bundle_id, "audio_sha256": cast(str, manifest["audio_sha256"])}])
    assert len(ready_bundles.retire_acknowledged_public_audio_v2(root, db, checkpoint)) == 1
    with closing(db.open_connection(read_only=True)) as connection:
        connection.execute("BEGIN")
        selected = ordered_payloads.select_ordered_payload(connection)
        assert selected is not None
        return selected


def test_complete_replay_survives_ack_restart_and_preserves_audio(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(ordered_payloads, "historical_decision", lambda *_: "unknown_held")
    db = StateDB(tmp_path / "state.sqlite3")
    with InternalAudioStore(db) as store:
        payload = _seed(store, db, tmp_path)
        before = db.list(StateKind.READY_PUBLICATION)
        assert ordered_full_replay.try_record_ordered_full_replay(store, payload)
        assert ordered_full_replay.try_record_ordered_full_replay(store, payload)
        assert db.list(StateKind.READY_PUBLICATION) == before
    with closing(db.open_connection(read_only=True)) as connection:
        connection.execute("BEGIN")
        with monkeypatch.context() as patch:
            patch.setattr(ordered_full_replay, "authenticate_source", lambda *_: pytest.fail("unexpected full scan"))
            assert ordered_full_replay.completed_ordered_replays(connection) == frozenset({payload.source_id})
        assert ordered_full_replay.has_ordered_replay_reference(connection, payload.source_id)
        assert ordered_full_replay.has_ordered_replay_reference(connection, payload.predecessor.source_id)
        assert not ordered_full_replay.has_ordered_replay_reference(connection, "f" * 32)
        assert (
            connection.execute(
                "SELECT COUNT(*) FROM capture_record WHERE source_id=?", (payload.source_id,)
            ).fetchone()[0]
            == 1
        )
        assert (
            connection.execute(
                "SELECT disposition FROM capture_historical_custody WHERE source_id=?", (payload.source_id,)
            ).fetchone()[0]
            == "pending"
        )


def test_replay_receipt_tampering_fails_closed(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(ordered_payloads, "historical_decision", lambda *_: "unknown_held")
    db = StateDB(tmp_path / "state.sqlite3")
    with InternalAudioStore(db) as store:
        payload = _seed(store, db, tmp_path)
        assert ordered_full_replay.try_record_ordered_full_replay(store, payload)
    receipt = db.get(StateKind.ORDERED_FULL_REPLAY, payload.source_id)
    assert receipt is not None
    receipt["predecessor_offset"] = 0
    db.put(StateKind.ORDERED_FULL_REPLAY, payload.source_id, receipt)
    with closing(db.open_connection(read_only=True)) as connection:
        connection.execute("BEGIN")
        with pytest.raises(ValueError, match="receipt"):
            ordered_full_replay.completed_ordered_replays(connection)


def test_changed_source_refuses_to_create_receipt(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(ordered_payloads, "historical_decision", lambda *_: "unknown_held")
    db = StateDB(tmp_path / "state.sqlite3")
    with InternalAudioStore(db) as store:
        payload = _seed(store, db, tmp_path)
        with db.typed_write_transaction() as connection:
            connection.execute("UPDATE capture_record SET payload=? WHERE source_id=?", (bytes(440), payload.source_id))
        with pytest.raises(ValueError):
            ordered_full_replay.try_record_ordered_full_replay(store, payload)
        assert db.get(StateKind.ORDERED_FULL_REPLAY, payload.source_id) is None


def test_nonmatching_source_needs_ordinary_publication(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(ordered_payloads, "historical_decision", lambda *_: "unknown_held")
    db = StateDB(tmp_path / "state.sqlite3")
    with InternalAudioStore(db) as store:
        payload = _seed(store, db, tmp_path, repeated=False)
        assert not ordered_full_replay.try_record_ordered_full_replay(store, payload)
        assert db.get(StateKind.ORDERED_FULL_REPLAY, payload.source_id) is None


def test_replay_scan_allows_concurrent_writer_commit(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    import sqlite3
    from collections.abc import Iterator

    from omi_collector.capture.domain.physical_recovery import PhysicalSpan

    monkeypatch.setattr(ordered_payloads, "historical_decision", lambda *_: "unknown_held")
    db = StateDB(tmp_path / "state.sqlite3")
    original_read = ordered_full_replay.read_physical_span
    writes: list[bool] = []

    def read_with_writer(connection: sqlite3.Connection, span: PhysicalSpan) -> Iterator[bytes]:
        for chunk in original_read(connection, span):
            if not writes:
                with closing(sqlite3.connect(db.path, timeout=1)) as writer:
                    writer.execute(
                        "UPDATE capture_source SET read_begin_count=read_begin_count WHERE source_id=?",
                        (span.source_id,),
                    )
                    writer.commit()
                writes.append(True)
            yield chunk

    with InternalAudioStore(db) as store:
        payload = _seed(store, db, tmp_path)
        monkeypatch.setattr(ordered_full_replay, "read_physical_span", read_with_writer)
        assert ordered_full_replay.try_record_ordered_full_replay(store, payload)
        assert writes == [True]


def test_replay_refuses_custody_change_between_scan_and_commit(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    import sqlite3
    from collections.abc import Iterator

    from omi_collector.capture.domain.physical_recovery import PhysicalSpan

    monkeypatch.setattr(ordered_payloads, "historical_decision", lambda *_: "unknown_held")
    db = StateDB(tmp_path / "state.sqlite3")
    original_read = ordered_full_replay.read_physical_span
    changed: list[bool] = []

    def mutate_during_scan(connection: sqlite3.Connection, span: PhysicalSpan) -> Iterator[bytes]:
        for chunk in original_read(connection, span):
            if not changed:
                with closing(sqlite3.connect(db.path, timeout=1)) as writer:
                    writer.execute(
                        "UPDATE capture_historical_custody SET prior_sha256=? WHERE source_id=?", ("0" * 64, "a" * 32)
                    )
                    writer.commit()
                changed.append(True)
            yield chunk

    with InternalAudioStore(db) as store:
        payload = _seed(store, db, tmp_path)
        monkeypatch.setattr(ordered_full_replay, "read_physical_span", mutate_during_scan)
        with pytest.raises(ValueError, match="custody changed"):
            ordered_full_replay.try_record_ordered_full_replay(store, payload)
        assert db.get(StateKind.ORDERED_FULL_REPLAY, payload.source_id) is None
        assert changed == [True]
