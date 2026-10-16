from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import cast

import pytest

from omi_collector.capture.adapters.collector_state_db import StateDB, StateKind
from omi_collector.capture.adapters.confirmed_loss import (
    ConfirmedLossError,
    ConfirmedLossLedger,
    read_confirmed_losses,
)
from omi_collector.capture.adapters.staging_store import StagingStore
from omi_collector.capture.domain.ring_protocol import RECORD_SIZE

ATTEMPT = "0123456789abcdef0123456789abcdef"
OCCURRED_AT = "2026-09-08T09:30:00+00:00"


def _db(tmp_path: Path) -> StateDB:
    return StateDB(tmp_path / "collector-state.sqlite3")


def test_ledger_retries_are_idempotent_and_existing_facts_are_retained(tmp_path: Path) -> None:
    db = _db(tmp_path)
    ledger = ConfirmedLossLedger(db)
    ledger.initialize(allow_create=True)
    loss_id = ledger.record(ATTEMPT, 100, 110, OCCURRED_AT)

    assert ledger.record(ATTEMPT, 100, 110, "2026-09-08T09:31:00+00:00") == loss_id
    ledger.initialize(allow_create=False)

    facts = read_confirmed_losses(db)
    assert len(facts) == 1
    assert facts[0].loss_id == loss_id
    assert facts[0].occurred_at == OCCURRED_AT
    assert facts[0].missing_record_count == 10
    assert facts[0].missing_raw_bytes == 10 * RECORD_SIZE


def test_concurrent_duplicate_records_are_atomic_and_idempotent(tmp_path: Path) -> None:
    db = _db(tmp_path)
    ledger = ConfirmedLossLedger(db)
    ledger.initialize(allow_create=True)
    with ThreadPoolExecutor(max_workers=8) as executor:
        ids = tuple(executor.map(lambda _: ledger.record(ATTEMPT, 100, 110, OCCURRED_AT), range(16)))

    assert len(set(ids)) == 1
    assert len(read_confirmed_losses(db)) == 1


def test_missing_ledger_after_initialization_fails_closed(tmp_path: Path) -> None:
    db = _db(tmp_path)
    ledger = ConfirmedLossLedger(db)
    ledger.initialize(allow_create=True)
    db.delete(StateKind.CONFIRMED_LOSS, "ledger")

    with pytest.raises(ConfirmedLossError, match="missing"):
        ledger.initialize(allow_create=False)
    with pytest.raises(ConfirmedLossError, match="missing"):
        ledger.record(ATTEMPT, 100, 110, OCCURRED_AT)


@pytest.mark.parametrize(
    "payload",
    [
        {"schema_version": True, "losses": []},
        {"schema_version": 2, "losses": []},
        {"schema_version": 1, "losses": [{}]},
    ],
)
def test_malformed_database_ledger_fails_closed(tmp_path: Path, payload: dict[str, object]) -> None:
    db = _db(tmp_path)
    db.put(StateKind.CONFIRMED_LOSS, "ledger", payload)

    with pytest.raises(ConfirmedLossError):
        ConfirmedLossLedger(db).initialize(allow_create=True)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("attempt_id", "not-an-attempt"),
        ("loss_id", "not-a-loss-id"),
        ("start_sequence", True),
        ("end_sequence", False),
        ("start_sequence", -1),
        ("end_sequence", 100),
        ("end_sequence", 1 << 64),
        ("missing_record_count", True),
        ("missing_raw_bytes", True),
        ("missing_record_count", 9),
        ("missing_raw_bytes", 9 * RECORD_SIZE),
        ("attempt_id", "1123456789abcdef0123456789abcdef"),
        ("loss_id", "0" * 64),
        ("reason", "operator-confirmed"),
        ("occurred_at", "not-a-timestamp"),
        ("occurred_at", "2026-09-08T09:30:00"),
    ],
)
def test_corrupt_loss_fact_fails_closed_at_public_read_boundary(tmp_path: Path, field: str, value: object) -> None:
    db = _db(tmp_path)
    ledger = ConfirmedLossLedger(db)
    ledger.initialize(allow_create=True)
    ledger.record(ATTEMPT, 100, 110, OCCURRED_AT)
    document = db.get(StateKind.CONFIRMED_LOSS, "ledger")
    assert document is not None
    losses = document["losses"]
    assert isinstance(losses, list)
    fact = losses[0]
    assert isinstance(fact, dict)
    cast(dict[str, object], fact)[field] = value
    db.put(StateKind.CONFIRMED_LOSS, "ledger", document)

    with pytest.raises(ConfirmedLossError, match="malformed"):
        read_confirmed_losses(db)


def test_explicit_first_use_initialization_retains_facts_on_restart(tmp_path: Path) -> None:
    db = _db(tmp_path)
    ConfirmedLossLedger(db).initialize(allow_create=True)
    loss_id = ConfirmedLossLedger(db).record(ATTEMPT, 20, 25, OCCURRED_AT)
    ConfirmedLossLedger(db).initialize(allow_create=False)

    facts = read_confirmed_losses(db)
    assert facts[0].loss_id == loss_id


def test_status_snapshot_prevents_silent_ledger_recreation(tmp_path: Path) -> None:
    spool = tmp_path / "collector"
    spool.mkdir()
    db = StateDB(spool / "collector-state.sqlite3")
    staging = StagingStore(spool, tmp_path / "draft", state_db=db)
    staging.preflight_storage()
    staging.initialize_confirmed_loss_ledger()
    db.put(
        StateKind.OPERATIONAL_STATUS,
        "current",
        {
            "boot_id": "00000000-0000-0000-0000-000000000001",
            "invocation_id": None,
            "schema_version": 1,
            "states": {"clock": "unknown", "publication": "unknown", "quality": "unknown"},
        },
    )
    db.delete(StateKind.CONFIRMED_LOSS, "ledger")

    with pytest.raises(ConfirmedLossError, match="missing after status initialization"):
        staging.initialize_confirmed_loss_ledger()


def test_preflight_rejects_missing_initialized_ledger_without_recreating_it(tmp_path: Path) -> None:
    spool = tmp_path / "collector"
    spool.mkdir()
    db = StateDB(spool / "collector-state.sqlite3")
    staging = StagingStore(spool, tmp_path / "draft", state_db=db)
    staging.preflight_storage()
    staging.initialize_confirmed_loss_ledger()
    db.put(
        StateKind.OPERATIONAL_STATUS,
        "current",
        {
            "boot_id": "00000000-0000-0000-0000-000000000001",
            "invocation_id": None,
            "schema_version": 1,
            "states": {"clock": "unknown", "publication": "unknown", "quality": "unknown"},
        },
    )
    db.delete(StateKind.CONFIRMED_LOSS, "ledger")

    with pytest.raises(ConfirmedLossError, match="missing after status initialization"):
        staging.preflight_storage()

    assert db.get(StateKind.CONFIRMED_LOSS, "ledger") is None
