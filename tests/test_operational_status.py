from __future__ import annotations

import asyncio
import stat
from itertools import product
from pathlib import Path

import pytest

from omi_collector.capture.adapters.collector_state_db import StateDB, StateDBError, StateKind
from omi_collector.capture.adapters.operational_status import (
    OperationalIdentity,
    OperationalStatusError,
    OperationalStatusStore,
    read_operational_status,
)
from omi_collector.capture.domain.operational_status_machine import (
    OperationalDimension,
    OperationalSignal,
    OperationalState,
    check_transition_completeness,
    transition,
)

BOOT = "00000000-0000-0000-0000-000000000001"
INVOCATION = "00000000000000000000000000000001"


def _db(tmp_path: Path) -> StateDB:
    return StateDB(tmp_path / "collector-state.sqlite3")


def test_every_state_and_typed_signal_has_an_explicit_transition() -> None:
    check_transition_completeness()
    for state, signal in product(OperationalState, OperationalSignal):
        assert isinstance(transition(state, signal), OperationalState)


def test_snapshot_restart_preserves_blocked_and_invalidates_clear(tmp_path: Path) -> None:
    db = _db(tmp_path)
    first_identity = OperationalIdentity(BOOT, INVOCATION)
    first = OperationalStatusStore(db, first_identity)
    first.initialize()
    first.update(OperationalDimension.QUALITY, OperationalSignal.CLEAR)
    first.update(OperationalDimension.PUBLICATION, OperationalSignal.BLOCK)

    next_identity = OperationalIdentity(BOOT, "00000000000000000000000000000002")
    assert read_operational_status(db, next_identity) == {
        "quality": "unknown",
        "publication": "blocked",
        "clock": "unknown",
    }

    first.close()
    restarted = OperationalStatusStore(db, next_identity)
    restarted.initialize()
    assert restarted.as_dict() == {
        "quality": "unknown",
        "publication": "blocked",
        "clock": "unknown",
    }
    restarted.close()


def test_snapshot_writes_only_for_state_or_identity_changes(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    db = _db(tmp_path)
    writes: list[object] = []
    real_put = db.put

    def count_put(kind: StateKind, item_id: str, payload: dict[str, object]) -> None:
        writes.append(payload)
        real_put(kind, item_id, payload)

    monkeypatch.setattr(db, "put", count_put)
    store = OperationalStatusStore(db, OperationalIdentity(BOOT, INVOCATION))
    store.initialize()
    store.initialize()
    store.update(OperationalDimension.QUALITY, OperationalSignal.UNCHANGED)
    assert len(writes) == 1

    store.update(OperationalDimension.QUALITY, OperationalSignal.CLEAR)
    assert len(writes) == 2
    store.close()


def test_status_store_creates_no_independent_lock_file(tmp_path: Path) -> None:
    db = _db(tmp_path)
    store = OperationalStatusStore(db, OperationalIdentity(BOOT, INVOCATION))
    try:
        store.initialize()
        assert stat.S_IMODE(db.path.stat().st_mode) == 0o600
        assert not (db.path.parent / "operational-status.lock").exists()
    finally:
        store.close()


@pytest.mark.parametrize(
    "payload",
    [
        {},
        {"boot_id": BOOT, "invocation_id": INVOCATION, "schema_version": True, "states": {}},
        {
            "boot_id": BOOT,
            "invocation_id": INVOCATION,
            "schema_version": 1,
            "states": {"quality": "clear", "publication": "clear", "clock": "x"},
        },
        {"boot_id": BOOT, "invocation_id": INVOCATION, "schema_version": 1, "states": {}, "extra": 1},
    ],
)
def test_malformed_database_snapshot_fails_closed(tmp_path: Path, payload: dict[str, object]) -> None:
    db = _db(tmp_path)
    db.put(StateKind.OPERATIONAL_STATUS, "current", payload)
    with pytest.raises(OperationalStatusError):
        read_operational_status(db, OperationalIdentity(BOOT, INVOCATION))


def test_oversized_database_snapshot_fails_closed(tmp_path: Path) -> None:
    db = _db(tmp_path)
    db.put(
        StateKind.OPERATIONAL_STATUS,
        "current",
        {"boot_id": BOOT, "invocation_id": INVOCATION, "schema_version": 1, "states": {}, "extra": "x" * 9_000},
    )
    with pytest.raises(OperationalStatusError, match="malformed"):
        read_operational_status(db, OperationalIdentity(BOOT, INVOCATION))


def test_publication_outcomes_have_explicit_state_semantics(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from omi_collector.capture.adapters.opportunistic_runtime import OpportunisticRuntime
    from omi_collector.capture.domain.operational_status_machine import PublicationOutcome

    store = OperationalStatusStore(_db(tmp_path), OperationalIdentity(BOOT, INVOCATION))
    store.initialize()
    monkeypatch.setattr("omi_collector.capture.adapters.opportunistic_runtime.debug_event", lambda *_a, **_k: None)
    runtime = OpportunisticRuntime(store.record_publication_outcome)
    for outcome, expected in (
        (PublicationOutcome.TRANSIENT, "unknown"),
        (PublicationOutcome.BLOCKED, "blocked"),
        (PublicationOutcome.TRANSIENT, "blocked"),
        (PublicationOutcome.PUBLISHED, "clear"),
        (PublicationOutcome.TRANSIENT, "unknown"),
        (PublicationOutcome.WAITING, "clear"),
    ):
        runtime.debug_event(f"ready_publication_{outcome.value}")
        assert store.as_dict()[OperationalDimension.PUBLICATION.value] == expected
    store.close()


def test_write_failure_latches_for_service_supervision_without_raising_from_callback(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    db = _db(tmp_path)
    identity = OperationalIdentity(BOOT, INVOCATION)
    store = OperationalStatusStore(db, identity)
    store.initialize()
    store.update(OperationalDimension.QUALITY, OperationalSignal.CLEAR)

    def fail(_kind: StateKind, _item_id: str, _payload: dict[str, object]) -> None:
        raise StateDBError("write failed")

    monkeypatch.setattr(db, "put", fail)
    store.update(OperationalDimension.QUALITY, OperationalSignal.BLOCK)

    assert store.failure is not None
    assert read_operational_status(db, identity)["quality"] == "clear"
    assert asyncio.run(store.wait_failure()) is store.failure
    store.close()


def test_supervisor_cancels_and_awaits_capture_after_snapshot_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from omi_collector.cli import _supervise_operational_status

    db = _db(tmp_path)
    store = OperationalStatusStore(db, OperationalIdentity(BOOT, INVOCATION))
    store.initialize()
    cleaned: list[bool] = []

    async def capture() -> None:
        try:
            await asyncio.Event().wait()
        finally:
            cleaned.append(True)

    def fail(_kind: StateKind, _item_id: str, _payload: dict[str, object]) -> None:
        raise StateDBError("write failed")

    monkeypatch.setattr(db, "put", fail)

    async def run() -> None:
        operation = asyncio.create_task(_supervise_operational_status(capture(), store))
        await asyncio.sleep(0)
        store.update(OperationalDimension.QUALITY, OperationalSignal.BLOCK)
        with pytest.raises(OperationalStatusError, match="persistence failed"):
            await operation

    asyncio.run(run())
    store.close()
    assert cleaned == [True]


def test_closed_writer_ignores_late_updates(tmp_path: Path) -> None:
    db = _db(tmp_path)
    store = OperationalStatusStore(db, OperationalIdentity(BOOT, INVOCATION))
    store.initialize()
    store.update(OperationalDimension.QUALITY, OperationalSignal.BLOCK)
    after_close = db.get(StateKind.OPERATIONAL_STATUS, "current")
    store.close()
    store.update(OperationalDimension.QUALITY, OperationalSignal.CLEAR)

    assert db.get(StateKind.OPERATIONAL_STATUS, "current") == after_close


@pytest.mark.parametrize(
    ("boot_id", "invocation_id"),
    [("unknown", INVOCATION), (BOOT, "invocation")],
)
def test_identity_rejects_noncanonical_values(boot_id: str, invocation_id: str) -> None:
    with pytest.raises(OperationalStatusError):
        OperationalIdentity(boot_id, invocation_id)
