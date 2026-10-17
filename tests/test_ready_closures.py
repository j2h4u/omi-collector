"""Durable physical-visit closure queue contracts."""

from __future__ import annotations

import asyncio
from json import dumps
from pathlib import Path
from struct import pack

import pytest

from omi_collector.capture.adapters.attempts import SqliteStagedAttempt
from omi_collector.capture.adapters.collector_state_db import StateDB, StateKind, StateTransaction
from omi_collector.capture.adapters.opportunistic_runtime import OpportunisticRuntime
from omi_collector.capture.adapters.ready_closures import (
    ReadyClosure,
    ReadyClosureError,
    append,
    begin_visit,
    coalesce,
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


def test_staging_emits_only_verified_armed_drain_and_keeps_repeat_identity(tmp_path: Path) -> None:
    store = StagingStore(tmp_path / "spool", tmp_path / "capture")
    store.append_ready_closure(10, "drained")
    assert store.state_db.get(StateKind.PENDANT_DRAIN, "current") is None

    writer = store.make_staging_writer(10, 1)
    writer.prepare()
    writer.prepare_leg(10, 1)
    writer.read_begin(ReadBeginNotification(10, 1))
    writer.append_chunk(0, memoryview(_record(10)))
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


def test_new_visit_revokes_old_drain_through_publication_and_restart(tmp_path: Path) -> None:
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
    restarted = StagingStore.from_paths(base.paths, publication_root=ready, config=config)
    assert restarted.recover_and_publish().state == "waiting"
    assert attempt_source(restarted, attempt_id) is not None
    assert not tuple(ready.iterdir())
    restarted.append_ready_closure(11, "drained")
    published = restarted.recover_and_publish()
    source = attempt_source(restarted, attempt_id)
    assert published.state == "published"
    assert published.retirement_pending
    assert source is not None and source.lifecycle == "retiring"
    assert restarted.recover_and_publish().retirement_pending
    assert attempt_source(restarted, attempt_id) is None


def test_visit_begin_reuses_active_writer_lease_across_threads(tmp_path: Path) -> None:
    store = StagingStore(tmp_path / "spool", tmp_path / "draft")
    store.append_ready_closure(11, "drained")

    with store.device_lock() as lease:
        marker = asyncio.run(asyncio.to_thread(store.begin_ready_visit))
        lease.require_active()

    assert marker is not None and marker.reason == "collecting"


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
    base, attempt = _partial(tmp_path)
    ready = tmp_path / "ready"
    ready.mkdir(mode=0o2750)
    ready.chmod(0o2750)
    store = StagingStore.from_paths(base.paths, publication_root=ready)
    store.terminalize_prefix_attempt(attempt.attempt_id)
    source = attempt_source(store, attempt.attempt_id)
    assert source is not None and source.lifecycle == "sealed_waiting"
    store.append_ready_closure(11, "legacy_prefix_publication")
    assert store.recover_and_publish().state == "waiting"
    assert attempt_source(store, attempt.attempt_id) is not None
    assert not tuple(ready.iterdir())
    assert closure_rows(store) == [{"next_sequence": 11, "reason": "legacy_prefix_publication"}]


def test_interrupted_close_publishes_ordinary_pending_prefix(tmp_path: Path) -> None:
    store, attempt = _partial(tmp_path)

    closure = store.close_pending_prefix("absence")

    assert closure is not None and closure.next_sequence == 11
    source = attempt_source(store, attempt.attempt_id)
    assert source is not None and source.lifecycle == "sealed_waiting"
    assert store.pending_attempts() == ()
