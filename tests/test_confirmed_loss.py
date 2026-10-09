from __future__ import annotations

import json
import stat
from pathlib import Path

import pytest

from omi_collector.capture.adapters.confirmed_loss import (
    ConfirmedLossError,
    ConfirmedLossLedger,
    read_confirmed_losses,
)
from omi_collector.capture.adapters.staging_contract import StagingError
from omi_collector.capture.adapters.staging_store import StagingStore
from omi_collector.capture.domain.ring_protocol import RECORD_SIZE

ATTEMPT = "0123456789abcdef0123456789abcdef"
OCCURRED_AT = "2026-09-08T09:30:00+00:00"


def test_ledger_retries_are_idempotent_and_existing_facts_are_retained(tmp_path: Path) -> None:
    path = tmp_path / "confirmed-loss.json"
    ledger = ConfirmedLossLedger(path)
    ledger.initialize(allow_create=True)
    loss_id = ledger.record(ATTEMPT, 100, 110, OCCURRED_AT)

    assert ledger.record(ATTEMPT, 100, 110, "2026-09-08T09:31:00+00:00") == loss_id
    ledger.initialize(allow_create=False)

    facts = read_confirmed_losses(path)
    assert len(facts) == 1
    assert facts[0].loss_id == loss_id
    assert facts[0].occurred_at == OCCURRED_AT
    assert facts[0].missing_record_count == 10
    assert facts[0].missing_raw_bytes == 10 * RECORD_SIZE
    assert stat.S_IMODE(path.stat().st_mode) == 0o600


def test_missing_ledger_after_initialization_fails_closed(tmp_path: Path) -> None:
    path = tmp_path / "confirmed-loss.json"
    ledger = ConfirmedLossLedger(path)
    ledger.initialize(allow_create=True)
    path.unlink()

    with pytest.raises(ConfirmedLossError, match="missing"):
        ledger.initialize(allow_create=False)
    with pytest.raises(ConfirmedLossError, match="missing"):
        ledger.record(ATTEMPT, 100, 110, OCCURRED_AT)


@pytest.mark.parametrize(
    "payload",
    [
        b"{",
        b'{"schema_version":true,"losses":[]}',
        b'{"schema_version":1,"losses":[{}]}',
    ],
)
def test_malformed_ledger_fails_closed(tmp_path: Path, payload: bytes) -> None:
    path = tmp_path / "confirmed-loss.json"
    path.write_bytes(payload)

    with pytest.raises(ConfirmedLossError):
        ConfirmedLossLedger(path).initialize(allow_create=True)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        pytest.param("attempt_id", "not-an-attempt", id="attempt-id-shape"),
        pytest.param("loss_id", "not-a-loss-id", id="loss-id-shape"),
        pytest.param("start_sequence", True, id="start-sequence-bool-is-not-int"),
        pytest.param("end_sequence", False, id="end-sequence-bool-is-not-int"),
        pytest.param("start_sequence", -1, id="negative-start"),
        pytest.param("end_sequence", 100, id="empty-interval"),
        pytest.param("end_sequence", 1 << 64, id="end-exceeds-u64"),
        pytest.param("missing_record_count", True, id="record-count-bool-is-not-int"),
        pytest.param("missing_raw_bytes", True, id="raw-bytes-bool-is-not-int"),
        pytest.param("missing_record_count", 9, id="record-count-does-not-match-interval"),
        pytest.param("missing_raw_bytes", 9 * RECORD_SIZE, id="raw-bytes-do-not-match-interval"),
        pytest.param("attempt_id", "1123456789abcdef0123456789abcdef", id="loss-id-binds-attempt"),
        pytest.param("loss_id", "0" * 64, id="loss-id-binds-interval"),
        pytest.param("reason", "operator-confirmed", id="unsupported-reason"),
        pytest.param("occurred_at", "not-a-timestamp", id="invalid-timestamp"),
        pytest.param("occurred_at", "2026-09-08T09:30:00", id="timestamp-needs-offset"),
    ],
)
def test_corrupt_loss_fact_fails_closed_at_public_read_boundary(tmp_path: Path, field: str, value: object) -> None:
    path = tmp_path / "confirmed-loss.json"
    ledger = ConfirmedLossLedger(path)
    ledger.initialize(allow_create=True)
    ledger.record(ATTEMPT, 100, 110, OCCURRED_AT)
    fact = ledger.read()[0].as_dict()
    fact[field] = value
    path.write_text(
        json.dumps({"schema_version": 1, "losses": [fact]}, sort_keys=True),
        encoding="utf-8",
    )

    with pytest.raises(ConfirmedLossError, match="malformed"):
        read_confirmed_losses(path)


def test_symlinked_ledger_fails_closed(tmp_path: Path) -> None:
    target = tmp_path / "target.json"
    target.write_text('{"schema_version":1,"losses":[]}', encoding="utf-8")
    path = tmp_path / "confirmed-loss.json"
    path.symlink_to(target)

    with pytest.raises(ConfirmedLossError):
        read_confirmed_losses(path)


def test_oversized_ledger_fails_closed(tmp_path: Path) -> None:
    path = tmp_path / "confirmed-loss.json"
    path.write_bytes(b" " * (1_048_576 + 1))

    with pytest.raises(ConfirmedLossError, match="oversized"):
        read_confirmed_losses(path)


def test_explicit_first_use_initialization_retains_facts_on_restart(tmp_path: Path) -> None:
    spool = tmp_path / "collector"
    staging = StagingStore(spool, tmp_path / "draft")
    staging.preflight_storage()
    staging.initialize_confirmed_loss_ledger()
    loss_id = staging.record_confirmed_loss(ATTEMPT, 20, 25, OCCURRED_AT)

    restarted = StagingStore(spool, tmp_path / "draft")
    restarted.preflight_storage()
    restarted.initialize_confirmed_loss_ledger()

    facts = read_confirmed_losses(spool / "confirmed-loss.json")
    assert facts[0].loss_id == loss_id


def test_status_snapshot_prevents_silent_ledger_recreation(tmp_path: Path) -> None:
    spool = tmp_path / "collector"
    staging = StagingStore(spool, tmp_path / "draft")
    staging.preflight_storage()
    (spool / "operational-status.json").write_text("snapshot exists", encoding="utf-8")

    with pytest.raises(StagingError, match="missing after status initialization"):
        staging.initialize_confirmed_loss_ledger()

    with staging.device_lock(recover_capture_temporaries=False, operation="test_after_ledger_error") as lease:
        lease.require_active()


def test_preflight_rejects_missing_initialized_ledger_without_recreating_it(tmp_path: Path) -> None:
    spool = tmp_path / "collector"
    staging = StagingStore(spool, tmp_path / "draft")
    staging.preflight_storage()
    staging.initialize_confirmed_loss_ledger()
    (spool / "operational-status.json").write_text("snapshot exists", encoding="utf-8")
    ledger_path = spool / "confirmed-loss.json"
    ledger_path.unlink()

    with pytest.raises(ConfirmedLossError, match="missing after status initialization"):
        staging.preflight_storage()

    assert not ledger_path.exists()
    with staging.device_lock(recover_capture_temporaries=False, operation="test_after_preflight_error") as lease:
        lease.require_active()
