from __future__ import annotations

import json
import sqlite3
from contextlib import closing
from dataclasses import replace
from functools import partial
from pathlib import Path
from typing import cast

import pytest

from omi_collector.capture.adapters import physical_stage, ready_bundles
from omi_collector.capture.adapters.collector_state_db import StateDB, StateKind
from omi_collector.capture.adapters.internal_audio_store import InternalAudioStore
from omi_collector.capture.adapters.physical_audio import prepare_physical_audio
from omi_collector.capture.adapters.physical_continuation import physical_continuation
from omi_collector.capture.adapters.physical_proof import prepare_physical_proof
from omi_collector.capture.adapters.physical_source import authenticate_source, read_physical_span
from omi_collector.capture.domain.clock_state import ClockState
from test_physical_stage_e2e import _seed
from test_public_audio_v2_integration import _checkpoint, _options, _ready_root, _record, _source


def _assert_bad_repeat(db: StateDB, marker_id: str, repeated: bytes) -> None:
    with closing(db.open_connection()) as connection:
        connection.execute("UPDATE capture_record SET payload=? WHERE source_id=?", (bytes(440), "b" * 32))
        connection.commit()
    with closing(db.open_connection(read_only=True)) as connection, pytest.raises(ValueError, match="digest"):
        connection.execute("BEGIN")
        physical_continuation(connection, marker_id)
    with closing(db.open_connection()) as connection:
        connection.execute("UPDATE capture_record SET payload=? WHERE source_id=?", (repeated[4:], "b" * 32))
        connection.commit()


def _assert_bad_marker(db: StateDB, marker_id: str) -> None:
    from omi_collector.capture.adapters.physical_continuation import completed_physical_continuation

    marker = db.get(StateKind.PHYSICAL_IMPORT, marker_id)
    assert marker is not None
    db.put(StateKind.PHYSICAL_IMPORT, marker_id, {**marker, "descriptor_sha256": "0" * 64})
    with closing(db.open_connection(read_only=True)) as connection, pytest.raises(ValueError, match="descriptor"):
        connection.execute("BEGIN")
        completed_physical_continuation(connection, marker_id)


def _assert_original_custody_required(db: StateDB, later_id: str) -> None:
    with closing(db.open_connection()) as connection:
        row = cast(
            tuple[str],
            connection.execute(
                "SELECT prior_sha256 FROM capture_historical_custody WHERE source_id=?", ("a" * 32,)
            ).fetchone(),
        )
        old = row[0]
        connection.execute(
            "UPDATE capture_historical_custody SET prior_sha256=? WHERE source_id=?", ("0" * 64, "a" * 32)
        )
        connection.commit()
    with pytest.raises(ready_bundles.ReadyBundleError):
        ready_bundles.historical_admission_frontier(StateDB(db.path), 202, frozenset({later_id}))
    with closing(db.open_connection()) as connection:
        connection.execute("UPDATE capture_historical_custody SET prior_sha256=? WHERE source_id=?", (old, "a" * 32))
        connection.commit()


def _retire_repeat(
    db: StateDB, store: InternalAudioStore, root: Path, checkpoint: Path, artifact: tuple[str, str]
) -> None:
    checkpoint.write_text(
        json.dumps(
            {
                "audio_contract": "omi-audio-v2",
                "analysis_cursor": None,
                "vad_decisions": [],
                "open_speech_tail": None,
                "acknowledged": [{"id": artifact[0], "audio_sha256": artifact[1]}],
                "consumer": {"retained": True},
            }
        ),
        encoding="utf-8",
    )
    ready_bundles.retire_acknowledged_public_audio_v2(root, db, checkpoint)
    for _ in range(10):
        if not ready_bundles.retire_one_covered_typed_source(store, 8):
            break


def _assert_bad_receipt(db: StateDB, marker_id: str, later_id: str) -> None:
    receipt = db.get(StateKind.PHYSICAL_CONTINUATION, marker_id)
    assert receipt is not None
    db.put(StateKind.PHYSICAL_CONTINUATION, marker_id, {**receipt, "repeat_end": 202})
    with pytest.raises(ready_bundles.ReadyBundleError):
        ready_bundles.historical_admission_frontier(StateDB(db.path), 202, frozenset({later_id}))
    db.put(StateKind.PHYSICAL_CONTINUATION, marker_id, receipt)


def _crash_before_repeat_commit(
    context: tuple[StateDB, InternalAudioStore, Path],
    sources: tuple[ready_bundles.SqlitePublicationSource, ...],
    marker_id: str,
    *,
    monkeypatch: pytest.MonkeyPatch,
    crash: bool,
) -> None:
    if not crash:
        return
    db, store, root = context
    with monkeypatch.context() as patch:
        patch.setattr(ready_bundles, "_commit_v2_stage", lambda *_args: (_ for _ in ()).throw(OSError("crash")))
        with pytest.raises(OSError, match="crash"):
            ready_bundles.finalize_public_audio_v2(store, sources, root, _options(ClockState(), 202, threshold=3600))
    assert db.get(StateKind.PHYSICAL_CONTINUATION, marker_id) is None


def _assert_other_incident_holds(
    db: StateDB, store: InternalAudioStore, root: Path, repeat: ready_bundles.SqlitePublicationSource
) -> None:
    obstruction = _source(store, "e" * 32, 150, (_record(9),))
    with closing(db.open_connection()) as connection:
        connection.execute(
            "INSERT INTO capture_framing_incident(incident_id,confirmed_frontier,status,confirmed_damage) "
            "VALUES (?,?,'unresolved',0)",
            (obstruction.source_id, (150).to_bytes(8, "big")),
        )
        connection.commit()
    options = replace(_options(ClockState(), 202), frontier=None, drained=False)
    assert not ready_bundles.finalize_public_audio_v2(store, (repeat,), root, options).published
    with closing(db.open_connection()) as connection:
        connection.execute("DELETE FROM capture_framing_incident WHERE incident_id=?", (obstruction.source_id,))
        connection.commit()


def _publish_initial_physical(
    db: StateDB, store: InternalAudioStore, root: Path
) -> tuple[str, ready_bundles.ReadyBundleResult, bytes]:
    descriptor, _, repeated = _seed(store, db)
    prior = _source(store, "c" * 32, 99, (_record(4),))
    ready_bundles.finalize_public_audio_v2(store, (prior,), root, _options(ClockState(), 100))
    with closing(db.open_connection(read_only=True)) as connection, db.access(connection):
        connection.execute("BEGIN")
        reader = partial(read_physical_span, connection)
        identity = partial(authenticate_source, connection)
        prepared = prepare_physical_audio(descriptor, reader, identity)
        descriptor["record_projection_sha256"] = prepared.record_projection_sha256
        with prepare_physical_proof(descriptor, reader, identity, packet_count=prepared.audio.packet_count) as proof:
            connection.rollback()
            stage = physical_stage.reserve_physical_stage(store, root, descriptor, prepared, proof)
            result = physical_stage.publish_physical_stage(
                store,
                stage,
                physical_stage.PhysicalStageInput(descriptor, prepared, proof.records_sha256, reader, identity),
            )
    return stage.stage_id, result, repeated


def _assert_continuation(db: StateDB, stage_id: str) -> None:
    with closing(db.open_connection(read_only=True)) as connection, db.access(connection):
        connection.execute("BEGIN")
        continuation = physical_continuation(connection, stage_id)
        assert (
            continuation.original_id,
            continuation.repeat_id,
            continuation.repeat_start,
            continuation.repeat_end,
        ) == ("a" * 32, "b" * 32, 200, 201)
        connection.rollback()


@pytest.mark.parametrize("crash", [False, True])
@pytest.mark.parametrize("physical_acked", [False, True])
@pytest.mark.parametrize("drained", [False, True])
def test_continuation_authenticates_published_and_acknowledged_physical_artifact(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, crash: bool, physical_acked: bool, drained: bool
) -> None:
    db = StateDB(tmp_path / "state.sqlite3")
    root = _ready_root(tmp_path)
    with InternalAudioStore(db) as store:
        stage_id, result, repeated = _publish_initial_physical(db, store, root)
        _assert_continuation(db, stage_id)
        manifest = cast(dict[str, object], json.loads((result.path / "manifest.json").read_bytes()))
        checkpoint = _checkpoint(
            tmp_path, [{"id": result.bundle_id, "audio_sha256": cast(str, manifest["audio_sha256"])}]
        )
        if physical_acked:
            ready_bundles.retire_acknowledged_public_audio_v2(root, db, checkpoint)
        _assert_continuation(db, stage_id)
        _assert_bad_repeat(db, stage_id, repeated)
        from test_historical_release_publication import _classify

        _classify(db, frozenset({"a" * 32}))
        repeat = ready_bundles.SqlitePublicationSource(store, "b" * 32)
        _assert_other_incident_holds(db, store, root, repeat)
        later = _source(store, "d" * 32, 201, (_record(7),))
        assert not ready_bundles.finalize_public_audio_v2(store, (later,), root, _options(ClockState(), 202)).published
        _crash_before_repeat_commit((db, store, root), (repeat, later), stage_id, monkeypatch=monkeypatch, crash=crash)
        options = (
            _options(ClockState(), 202, threshold=3600)
            if drained
            else replace(_options(ClockState(), 202, threshold=3600), frontier=None, drained=False)
        )
        result = ready_bundles.finalize_public_audio_v2(store, (repeat, later), root, options)
        assert len(result.published) == 1
        repeat_manifest = cast(dict[str, object], json.loads((result.published[0].path / "manifest.json").read_bytes()))
        assert repeat_manifest["boundaries"] == [{"at_packet": 0, "facts": ["time_incomparable"]}]
        later_snapshot = store.get_source(later.source_id)
        assert later_snapshot is not None and later_snapshot.lifecycle == "sealed_waiting"
        _assert_bad_receipt(db, stage_id, later.source_id)
        _retire_repeat(
            db, store, root, checkpoint, (result.published[0].bundle_id, cast(str, repeat_manifest["audio_sha256"]))
        )
        assert store.get_source(repeat.source_id) is None
        _assert_original_custody_required(db, later.source_id)
        after_restart = ready_bundles.finalize_public_audio_v2(store, (later,), root, _options(ClockState(), 202))
        assert len(after_restart.published) == 1
        _assert_bad_marker(db, stage_id)


def test_staging_publishes_physical_repeat_without_a_drain_closure(tmp_path: Path) -> None:
    from test_historical_release_publication import _classify
    from test_publication_fsm import _store

    staging = _store(tmp_path, threshold=3600)
    db, root = staging.state_db, tmp_path / "ready"
    with InternalAudioStore(db) as store:
        descriptor, _, _ = _seed(store, db)
        prior = _source(store, "c" * 32, 99, (_record(4),))
        ready_bundles.finalize_public_audio_v2(store, (prior,), root, _options(ClockState(), 100))
        with closing(db.open_connection(read_only=True)) as connection, db.access(connection):
            connection.execute("BEGIN")
            reader = partial(read_physical_span, connection)
            identity = partial(authenticate_source, connection)
            prepared = prepare_physical_audio(descriptor, reader, identity)
            descriptor["record_projection_sha256"] = prepared.record_projection_sha256
            with prepare_physical_proof(
                descriptor, reader, identity, packet_count=prepared.audio.packet_count
            ) as proof:
                connection.rollback()
                stage = physical_stage.reserve_physical_stage(store, root, descriptor, prepared, proof)
                physical_stage.publish_physical_stage(
                    store,
                    stage,
                    physical_stage.PhysicalStageInput(descriptor, prepared, proof.records_sha256, reader, identity),
                )
        _classify(db, frozenset({"a" * 32}))
    assert len(staging.recover_and_publish().published) == 1
    assert db.get(StateKind.PHYSICAL_CONTINUATION, stage.stage_id) is not None


def _extend_repeat_as_two_groups(db: StateDB, descriptor: dict[str, object], repeated: bytes) -> None:
    from hashlib import sha256

    first = bytearray(repeated)
    first[442] = 3
    second = _record(4, marker=13)
    digest = sha256(first + second).hexdigest()
    with closing(db.open_connection()) as connection:
        connection.execute(
            "UPDATE capture_record SET payload=? WHERE source_id=? AND ordinal=0", (bytes(first[4:]), "b" * 32)
        )
        connection.execute(
            "INSERT INTO capture_record(source_id,ordinal,device_timestamp,payload) VALUES (?,1,?,?)",
            ("b" * 32, 4, second[4:]),
        )
        connection.execute(
            "UPDATE capture_source SET sequence_limit=?,committed_count=2,committed_sha256=? WHERE source_id=?",
            ((202).to_bytes(8, "big"), digest, "b" * 32),
        )
        connection.execute(
            "UPDATE capture_historical_custody SET prior_count=2,prior_sha256=? WHERE source_id=?", (digest, "b" * 32)
        )
        connection.commit()
    anchor = cast(dict[str, object], descriptor["anchor"])
    anchor["repeat_source_sha256"] = digest


def _publish_multi_group_physical(db: StateDB, store: InternalAudioStore, root: Path) -> str:
    descriptor, _, repeated = _seed(store, db)
    _extend_repeat_as_two_groups(db, descriptor, repeated)
    prior = _source(store, "c" * 32, 99, (_record(4),))
    ready_bundles.finalize_public_audio_v2(store, (prior,), root, _options(ClockState(), 100))
    with closing(db.open_connection(read_only=True)) as connection, db.access(connection):
        connection.execute("BEGIN")
        reader = partial(read_physical_span, connection)
        identity = partial(authenticate_source, connection)
        prepared = prepare_physical_audio(descriptor, reader, identity)
        descriptor["record_projection_sha256"] = prepared.record_projection_sha256
        with prepare_physical_proof(descriptor, reader, identity, packet_count=prepared.audio.packet_count) as proof:
            connection.rollback()
            stage = physical_stage.reserve_physical_stage(store, root, descriptor, prepared, proof)
            physical_stage.publish_physical_stage(
                store,
                stage,
                physical_stage.PhysicalStageInput(descriptor, prepared, proof.records_sha256, reader, identity),
            )
    return stage.stage_id


@pytest.mark.parametrize("sqlite_ambiguous", [False, True])
def test_repeat_receipt_waits_for_last_group_and_survives_lost_commit_response(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, sqlite_ambiguous: bool
) -> None:
    from test_historical_release_publication import _classify

    db = StateDB(tmp_path / "state.sqlite3")
    root = _ready_root(tmp_path)
    with InternalAudioStore(db) as store:
        stage_id = _publish_multi_group_physical(db, store, root)
        _classify(db, frozenset({"a" * 32}))
        repeat = ready_bundles.SqlitePublicationSource(store, "b" * 32)
        original = ready_bundles._commit_v2_stage_once
        original_inspect = ready_bundles._inspect_v2_commit
        calls = 0
        inspected: list[str] = []

        def inspect_after_error(
            audio_store: InternalAudioStore, proposal: ready_bundles._V2Commit, destination: Path
        ) -> str:
            result = original_inspect(audio_store, proposal, destination)
            inspected.append(result)
            return result

        def interrupt_after_commit(audio_store: InternalAudioStore, proposal: ready_bundles._V2Commit) -> None:
            nonlocal calls
            original(audio_store, proposal)
            calls += 1
            if calls == 1:
                assert db.get(StateKind.PHYSICAL_CONTINUATION, stage_id) is None
                raise OSError("first response lost")
            if calls == 2:
                if sqlite_ambiguous:
                    raise sqlite3.OperationalError("last response lost")
                raise OSError("last response lost")

        with monkeypatch.context() as patch:
            patch.setattr(ready_bundles, "_commit_v2_stage_once", interrupt_after_commit)
            patch.setattr(ready_bundles, "_inspect_v2_commit", inspect_after_error)
            with pytest.raises(OSError, match="first response lost"):
                ready_bundles.finalize_public_audio_v2(
                    store, (repeat,), root, _options(ClockState(), 202, threshold=3600)
                )
            assert db.get(StateKind.PHYSICAL_CONTINUATION, stage_id) is None
            if sqlite_ambiguous:
                ready_bundles.finalize_public_audio_v2(
                    store, (repeat,), root, _options(ClockState(), 202, threshold=3600)
                )
                assert inspected == ["proposed"]
                assert calls == 2
            else:
                with pytest.raises(OSError, match="last response lost"):
                    ready_bundles.finalize_public_audio_v2(
                        store, (repeat,), root, _options(ClockState(), 202, threshold=3600)
                    )
        receipt = db.get(StateKind.PHYSICAL_CONTINUATION, stage_id)
        assert receipt is not None
        last_id = cast(str, receipt["last_artifact_id"])
        entry = db.get(StateKind.READY_PUBLICATION, last_id)
        assert entry is not None
        checkpoint = _checkpoint(tmp_path, [{"id": last_id, "audio_sha256": cast(str, entry["audio_sha256"])}])
        ready_bundles.retire_acknowledged_public_audio_v2(root, db, checkpoint)
        ledger_before = db.list(StateKind.READY_PUBLICATION)
        assert not ready_bundles.finalize_public_audio_v2(store, (), root, _options(ClockState(), 202)).published
        assert (
            db.get(StateKind.PHYSICAL_CONTINUATION, stage_id) == receipt
            and len(db.list(StateKind.PHYSICAL_CONTINUATION)) == 1
        )
        assert db.list(StateKind.READY_PUBLICATION) == ledger_before
        assert sum(item_id != "frontier" for item_id, _ in ledger_before) == 4
