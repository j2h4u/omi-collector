"""Durable physical-visit closure queue contracts."""

from __future__ import annotations

import asyncio
from contextlib import closing
from json import dumps, loads
from pathlib import Path
from struct import pack
from typing import cast

import pytest

from omi_collector.capture.adapters.attempts import SqliteStagedAttempt
from omi_collector.capture.adapters.collector_state_db import StateDB, StateDBError, StateKind, StateTransaction
from omi_collector.capture.adapters.framing_incidents import settle_unavailable
from omi_collector.capture.adapters.internal_audio_store import DrainCheckpoint, InternalAudioStore
from omi_collector.capture.adapters.opportunistic_runtime import OpportunisticRuntime
from omi_collector.capture.adapters.ready_closures import (
    ReadyClosure,
    ReadyClosureError,
    append,
    begin_visit,
    coalesce,
    drained_permit,
    legacy_closures,
    load,
    remove,
)
from omi_collector.capture.application.quarantine_maintenance import QuarantineMaintenance
from omi_collector.capture.domain.ring_protocol import RECORD_SIZE, DoneNotification, ReadBeginNotification, RingInfo
from omi_collector.config import CollectorConfig, ReadyConfig
from staging_test_factory import StagingStore, attempt_source, closure_rows


def _record(sequence: int) -> bytes:
    return pack(">I", sequence) + bytes((2, 8, 1)) + bytes(RECORD_SIZE - 7)


def _partial(tmp_path: Path) -> tuple[StagingStore, SqliteStagedAttempt]:
    store = StagingStore(tmp_path / "spool", tmp_path / "capture")
    attempt = store.prepare_streaming_attempt(10, 2)
    attempt.prepare_leg(10, 2)
    attempt.record_read_begin(ReadBeginNotification(10, 2))
    attempt.accept_chunk(10, _record(10))
    attempt.checkpoint()
    attempt.close(durable=True)
    return store, attempt


def _state_db(path: Path) -> StateDB:
    return StateDB(path.with_suffix(".sqlite3"))


def _unresolved_source(db: StateDB, start: int) -> None:
    with InternalAudioStore(db) as store:
        store.create_source("f" * 32, "attempt", start, start + 1)
        store.prepare_leg("f" * 32, start, 1)
        store.record_read_begin("f" * 32, start, 1)
        store.commit_checked_drain("f" * 32, 0, (bytes([255]) * RECORD_SIZE,), DrainCheckpoint(1))


def _sealed_source(store: StagingStore, sequence: int) -> str:
    writer = store.make_staging_writer(sequence, 1)
    writer.prepare()
    writer.prepare_leg(sequence, 1)
    writer.read_begin(ReadBeginNotification(sequence, 1))
    writer.append_chunk(0, memoryview(_record(sequence)))
    assert writer.finish_read(DoneNotification(0, sequence + 1))
    writer.seal(DoneNotification(0, sequence + 1))
    source_id = writer.attempt_id
    writer.close()
    return source_id


def test_closures_coalesce_to_latest_watermark_and_keep_equal_drain(tmp_path: Path) -> None:
    path = _state_db(tmp_path / "ready-closures.json")

    first = append(path, 10, "absence")
    assert append(path, 10, "recovery_exhausted") == first
    second = append(path, 20, "restart_interrupted")
    assert load(path) == (second,)
    drained = append(path, 20, "drained")
    assert drained.reason == "drained"
    assert append(path, 20, "restart_interrupted") == drained
    assert load(path) == (drained,)

    remove(path, drained)
    assert load(path) == ()
    assert path.get(StateKind.READY_CLOSURE, "current") == {"version": 1, "closures": []}


def test_equal_closure_callback_commits_in_same_transaction(tmp_path: Path) -> None:
    db = _state_db(tmp_path / "ready-closures.json")
    closure = append(db, 10, "drained")
    callback_transactions: list[StateTransaction] = []

    def record(transaction: StateTransaction) -> None:
        callback_transactions.append(transaction)
        transaction.put(StateKind.PENDANT_DRAIN, "current", {"marker": "closure"})

    assert append(db, 10, "drained", on_commit=record) == closure
    assert len(callback_transactions) == 1
    assert db.get(StateKind.PENDANT_DRAIN, "current") == {"marker": "closure"}


def test_visit_fence_and_old_drain_permit_commit_together(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    db = _state_db(tmp_path / "ready-closures.json")
    original = append(db, 10, "drained")
    put = StateTransaction.put

    def fail_permit(transaction: StateTransaction, kind: StateKind, item_id: str, payload: dict[str, object]) -> None:
        if kind is StateKind.READY_CLOSURE and item_id == "drained_permit":
            raise StateDBError("injected failure")
        put(transaction, kind, item_id, payload)

    monkeypatch.setattr(StateTransaction, "put", fail_permit)
    with pytest.raises(ReadyClosureError, match="could not be written"):
        begin_visit(db)
    assert load(db) == (original,)
    assert db.get(StateKind.READY_CLOSURE, "drained_permit") is None


def test_new_drain_replaces_old_permit_only_when_callback_commits(tmp_path: Path) -> None:
    db = _state_db(tmp_path / "ready-closures.json")
    old = append(db, 10, "drained")
    collecting = begin_visit(db)

    def fail(_transaction: StateTransaction) -> None:
        raise StateDBError("injected failure")

    with pytest.raises(ReadyClosureError, match="could not be written"):
        append(db, 11, "drained", on_commit=fail)
    assert load(db) == (collecting,)
    assert drained_permit(db) == old
    new = append(db, 11, "drained")
    assert load(db) == (new,)
    assert drained_permit(db) is None


def test_staging_emits_only_verified_armed_drain_and_keeps_repeat_identity(tmp_path: Path) -> None:
    store = StagingStore(tmp_path / "spool", tmp_path / "capture")
    store.append_ready_closure(10, "drained")
    assert store.state_db.get(StateKind.PENDANT_DRAIN, "current") is None

    writer = store.make_staging_writer(10, 1)
    writer.prepare()
    writer.prepare_leg(10, 1)
    writer.read_begin(ReadBeginNotification(10, 1))
    writer.append_chunk(0, memoryview(_record(10)))
    assert writer.finish_read(DoneNotification(0, 11))
    writer.seal(DoneNotification(0, 11))
    source_id = writer.attempt_id
    writer.close()
    store.public_drain.arm_pending(source_id, 11)
    info = RingInfo(11, 11, 100, 0, RECORD_SIZE)
    store.append_ready_closure(
        11, "drained", verified_drain=(info, "2026-10-17T12:00:00+00:00"), completed_at="2026-10-17T12:01:00+00:00"
    )
    state = store.state_db.get(StateKind.PENDANT_DRAIN, "current")
    assert state is not None
    first = state["last_drain"]
    assert state["pending_event_id"] is None
    assert isinstance(first, dict)
    first_event = first["event_id"]
    assert first == {"event_id": first_event, "completed_at": "2026-10-17T12:01:00+00:00"}

    store.append_ready_closure(
        11, "drained", verified_drain=(info, "2026-10-17T12:02:00+00:00"), completed_at="2026-10-17T12:03:00+00:00"
    )
    repeated = store.state_db.get(StateKind.PENDANT_DRAIN, "current")
    assert repeated is not None and repeated["last_drain"] == first


@pytest.mark.parametrize("loss", [None, ("wrong-source", 11, 13), ("same", 11, 12), ("same", 11, 14)])
def test_ahead_drain_needs_one_exact_confirmed_loss(tmp_path: Path, loss: tuple[str, int, int] | None) -> None:
    store = StagingStore(tmp_path / "spool", tmp_path / "capture")
    store.initialize_confirmed_loss_ledger()
    source_id = _sealed_source(store, 10)
    store.public_drain.arm_pending(source_id, 11)
    if loss is not None:
        fact_source = source_id if loss[0] == "same" else "0" * 32
        store.record_confirmed_loss(fact_source, loss[1], loss[2], "2026-10-17T12:00:00+00:00")

    store.append_ready_closure(
        11,
        "drained",
        verified_drain=(RingInfo(13, 13, 100, 0, RECORD_SIZE), "2026-10-17T12:01:00+00:00"),
        completed_at="2026-10-17T12:02:00+00:00",
    )

    state = store.state_db.get(StateKind.PENDANT_DRAIN, "current")
    assert state is not None
    assert state["last_drain"] is None
    assert state["pending_event_id"] is not None


def test_recovered_existing_drain_closure_completes_with_exact_loss(tmp_path: Path) -> None:
    store = StagingStore(tmp_path / "spool", tmp_path / "capture")
    store.initialize_confirmed_loss_ledger()
    source_id = _sealed_source(store, 10)
    store.public_drain.arm_pending(source_id, 11)
    store.record_confirmed_loss(source_id, 11, 13, "2026-10-17T12:00:00+00:00")
    store.append_ready_closure(11, "drained")

    store.close_pending_prefix(
        "drained",
        drain_cursor=13,
        verified_drain=(RingInfo(13, 13, 100, 0, RECORD_SIZE), "2026-10-17T12:01:00+00:00"),
        completed_at="2026-10-17T12:02:00+00:00",
    )

    state = store.state_db.get(StateKind.PENDANT_DRAIN, "current")
    assert state is not None and state["last_drain"] is not None
    assert state["pending_event_id"] is None


def test_new_visit_preserves_old_drain_through_publication_and_restart(tmp_path: Path) -> None:
    base = StagingStore(tmp_path / "spool", tmp_path / "draft")
    ready = tmp_path / "ready"
    ready.mkdir(mode=0o2750)
    ready.chmod(0o2750)
    config = CollectorConfig(ready=ReadyConfig(target_audio_seconds=0.02))
    store = StagingStore.from_paths(base.paths, publication_root=ready, config=config)
    writer = store.make_staging_writer(10, 1)
    writer.prepare()
    writer.prepare_leg(10, 1)
    writer.read_begin(ReadBeginNotification(10, 1))
    writer.append_chunk(0, memoryview(_record(10)))
    assert writer.finish_read(DoneNotification(0, 11))
    writer.seal(DoneNotification(0, 11))
    attempt_id = writer.attempt_id
    writer.close()
    store.append_ready_closure(11, "drained")
    maintenance = QuarantineMaintenance(store, None, OpportunisticRuntime())

    async def visit() -> None:
        await maintenance.enter_capture_priority()
        try:
            assert (await asyncio.to_thread(load, store.state_db))[0].reason == "collecting"
            assert (await asyncio.to_thread(store.publish_ready)).reason == "capture_active"
        finally:
            await maintenance.exit_capture_priority()
            await maintenance.close()

    asyncio.run(visit())
    newer_id = _sealed_source(store, 12)
    _unresolved_source(store.state_db, 11)
    restarted = StagingStore.from_paths(base.paths, publication_root=ready, config=config)
    published = restarted.recover_and_publish()
    source = attempt_source(restarted, attempt_id)
    assert published.state == "published"
    assert source is not None and source.lifecycle == "retiring"
    newer_source = attempt_source(restarted, newer_id)
    assert newer_source is not None and newer_source.lifecycle == "sealed_waiting"
    assert len(tuple(ready.iterdir())) == 1
    assert restarted.recover_and_publish().state == "waiting"
    assert settle_unavailable(store.state_db, 12, 12) == ("f" * 32,)
    restarted.append_ready_closure(13, "drained")
    later = restarted.recover_and_publish()
    assert later.state == "published"
    manifest = cast(dict[str, object], loads((later.published[0].path / "manifest.json").read_text()))
    boundaries = cast(list[dict[str, object]], manifest["boundaries"])
    assert "source_loss" in cast(list[str], boundaries[0]["facts"])
    with closing(store.state_db.open_connection(read_only=True)) as connection:
        assert (
            connection.execute("SELECT bytes FROM capture_observation_chunk").fetchone()[0]
            == bytes([255]) * RECORD_SIZE
        )


def test_visit_begin_reuses_active_writer_lease_across_threads(tmp_path: Path) -> None:
    store = StagingStore(tmp_path / "spool", tmp_path / "draft")
    store.append_ready_closure(11, "drained")

    with store.device_lock() as lease:
        marker = asyncio.run(asyncio.to_thread(store.begin_ready_visit))
        lease.require_active()

    assert marker is not None and marker.reason == "collecting"


def test_preserved_drain_waits_for_threshold_and_batches_at_next_drain(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    base = StagingStore(tmp_path / "spool", tmp_path / "draft")
    ready = tmp_path / "ready"
    ready.mkdir(mode=0o2750)
    ready.chmod(0o2750)
    store = StagingStore.from_paths(
        base.paths, publication_root=ready, config=CollectorConfig(ready=ReadyConfig(target_audio_seconds=0.04))
    )
    for sequence in (10, 11):
        _sealed_source(store, sequence)
        if sequence == 10:
            store.append_ready_closure(11, "drained")
            store.begin_ready_visit()
    restarted = StagingStore.from_paths(base.paths, publication_root=ready, config=store._config)
    monkeypatch.setattr("omi_collector.capture.adapters.framing_incidents.active_framing_frontier", lambda _db: None)
    assert restarted.recover_and_publish().reason == "waiting_for_drain"
    monkeypatch.setattr("omi_collector.capture.adapters.framing_incidents.active_framing_frontier", lambda _db: 11)
    restarted.publication_input_changed()
    assert restarted.recover_and_publish().reason == "waiting_for_threshold"
    assert not tuple(ready.iterdir())
    monkeypatch.setattr("omi_collector.capture.adapters.framing_incidents.active_framing_frontier", lambda _db: None)
    restarted.append_ready_closure(12, "drained")
    assert len(restarted.recover_and_publish().published) == 1


def test_unresolved_frontier_limits_new_drain_and_keeps_its_closure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    base = StagingStore(tmp_path / "spool", tmp_path / "draft")
    ready = tmp_path / "ready"
    ready.mkdir(mode=0o2750)
    ready.chmod(0o2750)
    store = StagingStore.from_paths(
        base.paths, publication_root=ready, config=CollectorConfig(ready=ReadyConfig(target_audio_seconds=0.02))
    )
    for sequence in (10, 11):
        _sealed_source(store, sequence)
    closure = store.append_ready_closure(12, "drained")
    monkeypatch.setattr("omi_collector.capture.adapters.framing_incidents.active_framing_frontier", lambda _db: 11)
    assert len(store.recover_and_publish().published) == 1
    assert load(store.state_db) == (closure,)
    monkeypatch.setattr("omi_collector.capture.adapters.framing_incidents.active_framing_frontier", lambda _db: None)
    store.publication_input_changed()
    assert len(store.recover_and_publish().published) == 1


def test_closures_reject_malformed_state(tmp_path: Path) -> None:
    path = _state_db(tmp_path / "ready-closures.json")
    path.put(
        StateKind.READY_CLOSURE, "current", {"version": 1, "closures": [{"next_sequence": 20, "reason": "absence"}]}
    )

    with pytest.raises(ReadyClosureError, match="regressed"):
        append(path, 10, "absence")

    path.put(
        StateKind.READY_CLOSURE,
        "current",
        {
            "version": 1,
            "closures": [{"next_sequence": 20, "reason": "absence"}, {"next_sequence": 10, "reason": "absence"}],
        },
    )
    with pytest.raises(ReadyClosureError, match="out of order"):
        load(path)

    path.put(StateKind.READY_CLOSURE, "current", {"version": 1, "closures": [{"next_sequence": 20}]})
    with pytest.raises(ReadyClosureError, match="entry schema"):
        load(path)


@pytest.mark.parametrize(
    "state",
    [
        pytest.param({"version": 1}, id="missing-closures"),
        pytest.param({"version": 1, "closures": [], "unexpected": True}, id="extra-field"),
    ],
)
def test_load_rejects_missing_or_extra_top_level_fields(tmp_path: Path, state: dict[str, object]) -> None:
    path = _state_db(tmp_path / "ready-closures.json")
    path.put(StateKind.READY_CLOSURE, "current", state)

    with pytest.raises(ReadyClosureError, match="schema is invalid"):
        load(path)


def test_legacy_closure_reader_validates_and_normalizes_fifo(tmp_path: Path) -> None:
    path = tmp_path / "ready-closures.json"
    path.write_text(dumps({"version": 1, "closures": [{"next_sequence": 4, "reason": "absence"}]}), encoding="utf-8")

    assert legacy_closures(path) == {
        "version": 1,
        "closures": [{"next_sequence": 4, "reason": "absence"}],
    }
    assert legacy_closures(tmp_path / "missing.json") is None


@pytest.mark.parametrize(
    ("document", "message"),
    [
        ({"version": 2, "closures": []}, "schema"),
        ({"version": 1, "closures": [{"next_sequence": True, "reason": "absence"}]}, "frontier"),
        (
            {
                "version": 1,
                "closures": [
                    {"next_sequence": 5, "reason": "absence"},
                    {"next_sequence": 4, "reason": "absence"},
                ],
            },
            "out of order",
        ),
    ],
)
def test_legacy_closure_reader_rejects_invalid_fifo(tmp_path: Path, document: object, message: str) -> None:
    path = tmp_path / "ready-closures.json"
    path.write_text(dumps(document), encoding="utf-8")

    with pytest.raises(ReadyClosureError, match=message):
        legacy_closures(path)


def test_legacy_closure_reader_rejects_symlinks(tmp_path: Path) -> None:
    target = tmp_path / "target.json"
    target.write_text(dumps({"version": 1, "closures": []}), encoding="utf-8")
    link = tmp_path / "ready-closures.json"
    link.symlink_to(target)

    with pytest.raises(ReadyClosureError, match="symlink"):
        legacy_closures(link)


def test_append_creates_missing_nested_parent_directories(tmp_path: Path) -> None:
    path = _state_db(tmp_path / "missing" / "nested" / "ready-closures.json")

    closure = append(path, 10, "absence")

    assert path.path.parent.is_dir()
    assert load(path) == (closure,)


@pytest.mark.parametrize("next_sequence", [-1, True, 1.5, "1"])
def test_append_rejects_invalid_frontier_without_changing_queue(tmp_path: Path, next_sequence: object) -> None:
    path = _state_db(tmp_path / "ready-closures.json")
    original = append(path, 0, "absence")
    original_payload = path.get(StateKind.READY_CLOSURE, "current")

    with pytest.raises(ReadyClosureError, match="frontier is invalid"):
        append(path, next_sequence, "restart_interrupted")  # type: ignore[arg-type]

    assert path.get(StateKind.READY_CLOSURE, "current") == original_payload
    assert load(path) == (original,)


@pytest.mark.parametrize("reason", ["", 1, None])
def test_append_rejects_invalid_reason_without_changing_queue(tmp_path: Path, reason: object) -> None:
    path = _state_db(tmp_path / "ready-closures.json")
    original = append(path, 0, "absence")
    original_payload = path.get(StateKind.READY_CLOSURE, "current")

    with pytest.raises(ReadyClosureError, match="reason is invalid"):
        append(path, 1, reason)  # type: ignore[arg-type]

    assert path.get(StateKind.READY_CLOSURE, "current") == original_payload
    assert load(path) == (original,)


def test_coalesce_keeps_latest_legacy_closure_and_handles_missing_or_singleton(tmp_path: Path) -> None:
    path = _state_db(tmp_path / "ready-closures.json")
    legacy = {
        "version": 1,
        "closures": [
            {"next_sequence": 10, "reason": "absence"},
            {"next_sequence": 20, "reason": "restart_interrupted"},
        ],
    }
    path.put(StateKind.READY_CLOSURE, "current", legacy)
    newest = ReadyClosure(20, "restart_interrupted")

    assert coalesce(path) == (newest,)
    assert load(path) == (newest,)
    persisted = path.get(StateKind.READY_CLOSURE, "current")
    assert persisted is not None
    assert persisted["closures"] == [{"next_sequence": 20, "reason": "restart_interrupted"}]

    missing = _state_db(tmp_path / "missing.json")
    assert coalesce(missing) == ()
    assert missing.get(StateKind.READY_CLOSURE, "current") is None

    singleton = _state_db(tmp_path / "singleton.json")
    only = append(singleton, 30, "absence")
    singleton_payload = singleton.get(StateKind.READY_CLOSURE, "current")
    assert coalesce(singleton) == (only,)
    assert load(singleton) == (only,)
    assert singleton.get(StateKind.READY_CLOSURE, "current") == singleton_payload


def test_coalesce_accepts_legacy_fifo_with_equal_frontiers(tmp_path: Path) -> None:
    path = _state_db(tmp_path / "equal-frontiers.json")
    path.put(
        StateKind.READY_CLOSURE,
        "current",
        {
            "version": 1,
            "closures": [
                {"next_sequence": 30, "reason": "absence"},
                {"next_sequence": 30, "reason": "recovery_exhausted"},
            ],
        },
    )
    first = ReadyClosure(30, "absence")
    newest = ReadyClosure(30, "recovery_exhausted")

    assert load(path) == (first, newest)
    assert coalesce(path) == (newest,)
    assert load(path) == (newest,)


def test_singleton_coalesce_succeeds_when_durable_rewrite_is_unavailable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = _state_db(tmp_path / "ready-closures.json")
    only = append(path, 30, "absence")
    original_payload = path.get(StateKind.READY_CLOSURE, "current")

    def reject_fsync(_fd: int) -> None:
        raise OSError("unexpected rewrite")

    monkeypatch.setattr("omi_collector.capture.adapters.ready_closures._write", lambda *_: reject_fsync(0))

    assert coalesce(path) == (only,)
    assert path.get(StateKind.READY_CLOSURE, "current") == original_payload


def test_begin_visit_returns_existing_non_drained_closure_unchanged(tmp_path: Path) -> None:
    path = _state_db(tmp_path / "ready-closures.json")
    previous = append(path, 30, "absence")
    original_payload = path.get(StateKind.READY_CLOSURE, "current")

    assert begin_visit(path) == previous
    assert path.get(StateKind.READY_CLOSURE, "current") == original_payload


@pytest.mark.parametrize("queue_state", ["empty", "mismatched_head"])
def test_remove_rejects_empty_or_mismatched_queue_without_changing_bytes(tmp_path: Path, queue_state: str) -> None:
    path = _state_db(tmp_path / "ready-closures.json")
    queued = append(path, 30, "absence")
    if queue_state == "empty":
        remove(path, queued)
        rejected = queued
    else:
        rejected = ReadyClosure(31, "absence")
    original_payload = path.get(StateKind.READY_CLOSURE, "current")

    with pytest.raises(ReadyClosureError):
        remove(path, rejected)

    assert path.get(StateKind.READY_CLOSURE, "current") == original_payload


def test_legacy_non_drained_closure_does_not_authorize_prefix_publication(tmp_path: Path) -> None:
    base = StagingStore(tmp_path / "spool", tmp_path / "capture")
    ready = tmp_path / "ready"
    ready.mkdir(mode=0o2750)
    ready.chmod(0o2750)
    store = StagingStore.from_paths(base.paths, publication_root=ready)
    source_id = _sealed_source(store, 10)
    source = attempt_source(store, source_id)
    assert source is not None and source.lifecycle == "sealed_waiting"
    store.append_ready_closure(11, "legacy_prefix_publication")
    assert store.recover_and_publish().state == "waiting"
    assert attempt_source(store, source_id) is not None
    assert not tuple(ready.iterdir())
    assert closure_rows(store) == [{"next_sequence": 11, "reason": "legacy_prefix_publication"}]


def test_interrupted_close_keeps_unconfirmed_pending_prefix(tmp_path: Path) -> None:
    store, attempt = _partial(tmp_path)

    closure = store.close_pending_prefix("absence")

    assert closure is None
    source = attempt_source(store, attempt.attempt_id)
    assert source is not None and source.lifecycle != "sealed_waiting"
    assert store.pending_attempts()
