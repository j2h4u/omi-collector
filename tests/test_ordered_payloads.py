from __future__ import annotations

from contextlib import closing
from pathlib import Path
from typing import cast

import pytest

from omi_collector.capture.adapters import ordered_payloads
from omi_collector.capture.adapters.collector_state_db import StateDB
from omi_collector.capture.adapters.internal_audio_store import InternalAudioStore
from omi_collector.capture.adapters.ready_bundles import ReadyBundleResult
from test_public_audio_v2_integration import _record, _source


def _concurrent_scan_reader(db: StateDB):
    import sqlite3
    from collections.abc import Iterator
    from threading import Event, Thread

    from omi_collector.capture.adapters.physical_source import read_physical_span
    from omi_collector.capture.domain.physical_recovery import PhysicalSpan

    checked = Event()

    def read(connection: sqlite3.Connection, span: PhysicalSpan) -> Iterator[bytes]:
        assert not connection.in_transaction
        if not checked.is_set():
            committed = Event()

            def write() -> None:
                with db.typed_write_transaction() as writer:
                    writer.execute("CREATE TABLE IF NOT EXISTS concurrent_writer_progress (id INTEGER)")
                committed.set()

            thread = Thread(target=write, daemon=True)
            thread.start()
            assert committed.wait(3), "same-instance writer blocked during audio scan"
            thread.join()
            checked.set()
        yield from read_physical_span(connection, span)

    return read, checked


def _retain(store: InternalAudioStore, db: StateDB, source_id: str, start: int) -> None:
    source = _source(store, source_id, start, (_record(start),))
    with db.typed_write_transaction() as connection:
        connection.execute(
            "UPDATE capture_source SET source_type='quarantine',lifecycle='blocked',"
            "quarantine_disposition='published_prefix',certified_prefix_count=committed_count,"
            "coverage_digest=committed_sha256 WHERE source_id=?",
            (source.source_id,),
        )
        connection.execute(
            "INSERT INTO capture_historical_custody "
            "SELECT source_id,source_type,lifecycle,committed_count,committed_sha256,"
            "certified_prefix_count,coverage_digest,'pending' FROM capture_source WHERE source_id=?",
            (source.source_id,),
        )


@pytest.fixture
def established_predecessor(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        ordered_payloads,
        "_latest_numbered_predecessor",
        lambda *_: ordered_payloads.OrderedPredecessor("f" * 64, "d" * 32, "e" * 64, 100, 444),
    )
    monkeypatch.setattr(ordered_payloads, "completed_physical_originals", lambda *_: frozenset())
    monkeypatch.setattr(ordered_payloads, "historical_decision", lambda *_: "unknown_held")


@pytest.mark.usefixtures("established_predecessor")
def test_selector_uses_observed_read_order_without_claiming_sequence_extent(tmp_path: Path) -> None:
    db = StateDB(tmp_path / "state.sqlite3")
    with InternalAudioStore(db) as store:
        _retain(store, db, "b" * 32, 130)
        _retain(store, db, "a" * 32, 110)
    with closing(db.open_connection(read_only=True)) as connection:
        connection.execute("BEGIN")
        chosen = ordered_payloads.select_ordered_payload(connection)
    assert chosen is not None
    assert (chosen.source_id, chosen.source_start, chosen.read_begin, chosen.byte_count) == ("a" * 32, 110, 110, 444)
    assert not hasattr(chosen, "next_sequence")


@pytest.mark.parametrize("defect", ["same_order", "earlier_audio", "custody_changed", "no_read_begin"])
@pytest.mark.usefixtures("established_predecessor")
def test_selector_holds_ambiguous_or_unverified_audio(tmp_path: Path, defect: str) -> None:
    db = StateDB(tmp_path / "state.sqlite3")
    with InternalAudioStore(db) as store:
        _retain(store, db, "a" * 32, 110)
        _retain(store, db, "b" * 32, 130)
    with db.typed_write_transaction() as connection:
        if defect == "same_order":
            connection.execute(
                "UPDATE capture_source SET sequence_start=? WHERE source_id=?", ((110).to_bytes(8, "big"), "b" * 32)
            )
        elif defect == "earlier_audio":
            connection.execute(
                "UPDATE capture_source SET sequence_start=? WHERE source_id=?", ((99).to_bytes(8, "big"), "a" * 32)
            )
        elif defect == "custody_changed":
            connection.execute(
                "UPDATE capture_historical_custody SET prior_sha256=? WHERE source_id=?", ("0" * 64, "a" * 32)
            )
        else:
            connection.execute("UPDATE capture_source SET read_begin_start=NULL WHERE source_id=?", ("a" * 32,))
    with closing(db.open_connection(read_only=True)) as connection:
        connection.execute("BEGIN")
        with pytest.raises(ValueError):
            ordered_payloads.select_ordered_payload(connection)


@pytest.mark.usefixtures("established_predecessor")
def test_latest_retry_does_not_reorder_earlier_saved_prefix(tmp_path: Path) -> None:
    db = StateDB(tmp_path / "state.sqlite3")
    with InternalAudioStore(db) as store:
        _retain(store, db, "a" * 32, 110)
        _retain(store, db, "b" * 32, 130)
    with db.typed_write_transaction() as connection:
        connection.execute(
            "UPDATE capture_source SET read_begin_start=? WHERE source_id=?", ((140).to_bytes(8, "big"), "a" * 32)
        )
    with closing(db.open_connection(read_only=True)) as connection:
        connection.execute("BEGIN")
        chosen = ordered_payloads.select_ordered_payload(connection)
    assert chosen is not None and (chosen.source_start, chosen.read_begin) == (110, 140)


@pytest.mark.usefixtures("established_predecessor")
def test_proven_empty_custody_does_not_block_later_audio(tmp_path: Path) -> None:
    db = StateDB(tmp_path / "state.sqlite3")
    with InternalAudioStore(db) as store:
        store.create_source("e" * 32, "attempt", 110, 111)
        _retain(store, db, "a" * 32, 130)
    with db.typed_write_transaction() as connection:
        connection.execute(
            "INSERT INTO capture_historical_custody "
            "SELECT source_id,source_type,lifecycle,committed_count,committed_sha256,"
            "certified_prefix_count,coverage_digest,'pending' FROM capture_source WHERE source_id=?",
            ("e" * 32,),
        )
    with closing(db.open_connection(read_only=True)) as connection:
        connection.execute("BEGIN")
        chosen = ordered_payloads.select_ordered_payload(connection)
    assert chosen is not None and chosen.source_id == "a" * 32
    with db.typed_write_transaction() as connection:
        connection.execute(
            "UPDATE capture_historical_custody SET prior_sha256=? WHERE source_id=?", ("0" * 64, "e" * 32)
        )
    with closing(db.open_connection(read_only=True)) as connection:
        connection.execute("BEGIN")
        with pytest.raises(ValueError, match="empty source"):
            ordered_payloads.select_ordered_payload(connection)


@pytest.mark.usefixtures("established_predecessor")
def test_next_physical_start_respects_pending_source_order(tmp_path: Path) -> None:
    db = StateDB(tmp_path / "state.sqlite3")
    with InternalAudioStore(db) as store:
        _retain(store, db, "b" * 32, 130)
        _retain(store, db, "a" * 32, 110)
    with closing(db.open_connection(read_only=True)) as connection:
        connection.execute("BEGIN")
        assert ordered_payloads.next_ordered_physical_start(connection) == 110
    with db.typed_write_transaction() as connection:
        connection.execute(
            "UPDATE capture_historical_custody SET prior_sha256=? WHERE source_id=?", ("0" * 64, "a" * 32)
        )
    with closing(db.open_connection(read_only=True)) as connection:
        connection.execute("BEGIN")
        assert ordered_payloads.next_ordered_physical_start(connection) is None


def _assert_plan_ack(
    db: StateDB, root: Path, tmp_path: Path, descriptor: dict[str, object], result: ReadyBundleResult
) -> None:
    import json

    from omi_collector.capture.adapters import physical_stage, ready_bundles
    from test_public_audio_v2_integration import _checkpoint

    assert physical_stage.published_physical_artifact(db, descriptor) == result.bundle_id
    manifest = cast(dict[str, object], json.loads((result.path / "manifest.json").read_bytes()))
    checkpoint = _checkpoint(tmp_path, [{"id": result.bundle_id, "audio_sha256": cast(str, manifest["audio_sha256"])}])
    assert len(ready_bundles.retire_acknowledged_public_audio_v2(root, db, checkpoint)) == 1
    assert physical_stage.published_physical_artifact(db, descriptor) == result.bundle_id


def test_ordered_plan_survives_stage_restart_and_ack(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from functools import partial

    from omi_collector.capture.adapters import physical_stage, ready_bundles
    from omi_collector.capture.adapters.ordered_physical_plan import prepare_ordered_physical_plan
    from omi_collector.capture.adapters.physical_proof import prepare_physical_proof
    from omi_collector.capture.adapters.physical_source import authenticate_source, read_physical_span
    from omi_collector.capture.domain.clock_state import ClockState
    from test_public_audio_v2_integration import _options, _ready_root

    monkeypatch.setattr(ordered_payloads, "historical_decision", lambda *_: "unknown_held")
    db = StateDB(tmp_path / "state.sqlite3")
    with InternalAudioStore(db) as store:
        root = _ready_root(tmp_path)
        previous = _source(store, "d" * 32, 100, (_record(1),))
        ready_bundles.finalize_public_audio_v2(store, (previous,), root, _options(ClockState(), 101))
        _retain(store, db, "a" * 32, 110)
        with closing(db.open_connection(read_only=True)) as connection:
            connection.execute("BEGIN")
            payload = ordered_payloads.select_ordered_payload(connection)
            assert payload is not None
            descriptor, prepared = prepare_ordered_physical_plan(connection, payload)
            reader = partial(read_physical_span, connection)
            identity = partial(authenticate_source, connection)
            with prepare_physical_proof(
                descriptor, reader, identity, packet_count=prepared.audio.packet_count
            ) as proof:
                connection.rollback()
                stage = physical_stage.reserve_physical_stage(store, root, descriptor, prepared, proof)
                stage_input = physical_stage.PhysicalStageInput(
                    descriptor, prepared, proof.records_sha256, reader, identity
                )
                with monkeypatch.context() as patch:
                    patch.setattr(
                        physical_stage, "_commit_physical_stage", lambda *_: (_ for _ in ()).throw(OSError("crash"))
                    )
                    with pytest.raises(OSError, match="crash"):
                        physical_stage.publish_physical_stage(store, stage, stage_input)
                resume_read, checked = _concurrent_scan_reader(db)
                with monkeypatch.context() as patch:
                    patch.setattr(physical_stage, "read_physical_span", resume_read)
                    result = physical_stage.resume_physical_stage(store, root)
                assert checked.is_set()
                assert result is not None and result.bundle_id == stage.artifact_id
                _assert_plan_ack(db, root, tmp_path, descriptor, result)


def test_coordinator_publishes_only_after_guarded_historical_decision(tmp_path: Path) -> None:
    from omi_collector.capture.adapters import ready_bundles
    from omi_collector.capture.adapters.confirmed_loss import ConfirmedLossLedger
    from omi_collector.capture.adapters.ordered_physical_plan import publish_next_ordered_physical
    from omi_collector.capture.domain.clock_state import ClockState
    from test_public_audio_v2_integration import _options, _ready_root

    db = StateDB(tmp_path / "state.sqlite3")
    with InternalAudioStore(db) as store:
        root = _ready_root(tmp_path)
        prior = _source(store, "d" * 32, 100, (_record(1),))
        ready_bundles.finalize_public_audio_v2(store, (prior,), root, _options(ClockState(), 101))
        _retain(store, db, "a" * 32, 110)
        ConfirmedLossLedger(db).initialize(allow_create=True)
        assert publish_next_ordered_physical(store, root) is None
        from test_historical_release_publication import _classify

        _classify(db, frozenset({"a" * 32}))
        published = publish_next_ordered_physical(store, root)
        assert published is not None and published is not True and published.record_count == 1
        assert publish_next_ordered_physical(store, root) is None


def test_coordinator_publishes_physical_successors_in_order_once(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from omi_collector.capture.adapters import ready_bundles
    from omi_collector.capture.adapters.ordered_physical_plan import publish_next_ordered_physical
    from omi_collector.capture.domain.clock_state import ClockState
    from test_public_audio_v2_integration import _options, _ready_root

    monkeypatch.setattr(ordered_payloads, "historical_decision", lambda *_: "unknown_held")
    db = StateDB(tmp_path / "state.sqlite3")
    with InternalAudioStore(db) as store:
        root = _ready_root(tmp_path)
        prior = _source(store, "d" * 32, 100, (_record(1),))
        ready_bundles.finalize_public_audio_v2(store, (prior,), root, _options(ClockState(), 101))
        _retain(store, db, "a" * 32, 110)
        _retain(store, db, "b" * 32, 130)
        first = publish_next_ordered_physical(store, root)
        second = publish_next_ordered_physical(store, root)
        assert (
            first is not None
            and first is not True
            and second is not None
            and second is not True
            and first.bundle_id != second.bundle_id
        )
        assert publish_next_ordered_physical(store, root) is None


def test_full_replay_receipt_advances_to_next_source_after_restart_and_ack(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import json

    from omi_collector.capture.adapters import ready_bundles
    from omi_collector.capture.adapters.ordered_physical_plan import publish_next_ordered_physical
    from omi_collector.capture.domain.clock_state import ClockState
    from test_public_audio_v2_integration import _checkpoint, _options, _ready_root

    monkeypatch.setattr(ordered_payloads, "historical_decision", lambda *_: "unknown_held")
    db = StateDB(tmp_path / "state.sqlite3")
    root = _ready_root(tmp_path)
    with InternalAudioStore(db) as store:
        prior = _source(store, "d" * 32, 100, (_record(110),))
        outcome = ready_bundles.finalize_public_audio_v2(store, (prior,), root, _options(ClockState(), 101))
        _retain(store, db, "a" * 32, 110)
        _retain(store, db, "b" * 32, 130)
        generation = db.get_generation()
        assert publish_next_ordered_physical(store, root) is True
        assert db.get_generation() > generation
        first = outcome.published[0]
        manifest = cast(dict[str, object], json.loads((first.path / "manifest.json").read_bytes()))
        checkpoint = _checkpoint(
            tmp_path, [{"id": first.bundle_id, "audio_sha256": cast(str, manifest["audio_sha256"])}]
        )
        assert len(ready_bundles.retire_acknowledged_public_audio_v2(root, db, checkpoint)) == 1
    with InternalAudioStore(StateDB(db.path)) as restarted:
        result = publish_next_ordered_physical(restarted, root)
        assert result is not None and result is not True and result.record_count == 1
        assert publish_next_ordered_physical(restarted, root) is None


def test_coordinator_releases_snapshot_before_audio_scan(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from omi_collector.capture.adapters import ordered_physical_plan, ready_bundles
    from omi_collector.capture.domain.clock_state import ClockState
    from test_public_audio_v2_integration import _options, _ready_root

    monkeypatch.setattr(ordered_payloads, "historical_decision", lambda *_: "unknown_held")
    db = StateDB(tmp_path / "state.sqlite3")
    checked_read, checked = _concurrent_scan_reader(db)

    monkeypatch.setattr(ordered_physical_plan, "read_physical_span", checked_read)
    with InternalAudioStore(db) as store:
        root = _ready_root(tmp_path)
        prior = _source(store, "d" * 32, 100, (_record(1),))
        ready_bundles.finalize_public_audio_v2(store, (prior,), root, _options(ClockState(), 101))
        _retain(store, db, "a" * 32, 110)
        result = ordered_physical_plan.publish_next_ordered_physical(store, root)
        assert checked.is_set() and result is not None and result is not True


@pytest.mark.usefixtures("established_predecessor")
def test_short_sealed_source_is_selected_by_original_source_order(tmp_path: Path) -> None:
    db = StateDB(tmp_path / "state.sqlite3")
    with InternalAudioStore(db) as store:
        source = _source(store, "a" * 32, 110, (_record(110),))
        with db.typed_write_transaction() as connection:
            connection.execute(
                "UPDATE capture_source SET sequence_limit=?,read_begin_start=? WHERE source_id=?",
                ((200).to_bytes(8, "big"), (111).to_bytes(8, "big"), source.source_id),
            )
            connection.execute(
                "INSERT INTO capture_historical_custody "
                "SELECT source_id,source_type,lifecycle,committed_count,committed_sha256,"
                "certified_prefix_count,coverage_digest,'pending' FROM capture_source WHERE source_id=?",
                (source.source_id,),
            )
    with closing(db.open_connection(read_only=True)) as connection:
        connection.execute("BEGIN")
        assert ordered_payloads.next_ordered_physical_start(connection) == 110
        chosen = ordered_payloads.select_ordered_payload(connection)
    assert chosen is not None and (chosen.source_start, chosen.read_begin) == (110, 111)
