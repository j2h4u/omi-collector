"""Conflict classification and its transactional storage boundaries."""

from contextlib import closing
from pathlib import Path
from typing import cast

import pytest

from omi_collector.capture.adapters.collector_state_db import StateDB
from omi_collector.capture.adapters.internal_audio_store import InternalAudioStore, InternalAudioStoreError
from omi_collector.capture.adapters.staging_contract import StagingError
from omi_collector.capture.domain.internal_audio_machine import QuarantineAction, QuarantineDisposition
from omi_collector.capture.domain.quarantine_disposition import classify_conflict
from staging_test_factory import StagingStore


@pytest.mark.parametrize(
    ("count", "present", "disposition", "action", "reason"),
    [
        (0, False, QuarantineDisposition.UNPROCESSABLE, QuarantineAction.MARK_UNPROCESSABLE, "conflict"),
        (0, True, QuarantineDisposition.UNCLASSIFIED, QuarantineAction.KEEP, "source_authentication_mismatch"),
        (1, False, QuarantineDisposition.UNCLASSIFIED, QuarantineAction.KEEP, "conflict"),
        (1, True, QuarantineDisposition.UNCLASSIFIED, QuarantineAction.KEEP, "conflict"),
    ],
)
def test_conflict_classification(
    count: int, present: bool, disposition: QuarantineDisposition, action: QuarantineAction, reason: str
) -> None:
    result = classify_conflict(count, present, "conflict")
    assert result.classification.next_disposition is disposition
    assert result.classification.action is action
    assert result.block_reason == reason


def test_admission_update_failure_rolls_back(tmp_path: Path) -> None:
    store = StagingStore(tmp_path, tmp_path / "captures")
    attempt = store.prepare_streaming_attempt(100, 2)
    try:
        with closing(store.state_db.open_connection()) as connection:
            connection.execute(
                "CREATE TRIGGER fail_quarantine BEFORE UPDATE ON capture_source "
                "WHEN NEW.source_type = 'quarantine' BEGIN SELECT RAISE(ABORT, 'injected'); END"
            )
            connection.commit()
        with pytest.raises(StagingError, match="typed quarantine source transition failed"):
            store.quarantine_attempt_source(attempt.attempt_id)
        with closing(store.state_db.open_connection(read_only=True)) as connection:
            row = cast(
                tuple[object, ...],
                connection.execute(
                    "SELECT source_type, lifecycle, block_reason FROM capture_source WHERE source_id = ?",
                    (attempt.attempt_id,),
                ).fetchone(),
            )
            assert tuple(row) == ("attempt", "prepared", None)
    finally:
        attempt.close()


def test_replay_update_failure_rolls_back_conflict_evidence(tmp_path: Path) -> None:
    db = StateDB(tmp_path / "state.sqlite3")
    with InternalAudioStore(db) as store:
        source_id = "a" * 32
        store.create_source(source_id, "attempt", 0, 10)
        store.prepare_leg(source_id, 0, 10)
        store.record_read_begin(source_id, 0, 10)
        source = store.get_source(source_id)
        assert source is not None
        with closing(db.open_connection()) as connection:
            connection.execute(
                "CREATE TRIGGER fail_quarantine BEFORE UPDATE ON capture_source "
                "WHEN NEW.source_type = 'quarantine' BEGIN SELECT RAISE(ABORT, 'injected'); END"
            )
            connection.commit()
        with pytest.raises(InternalAudioStoreError, match="replay conflict could not be retained"):
            store._block_replay_conflict(source, 0, b"rejected")
        with closing(db.open_connection(read_only=True)) as connection:
            assert connection.execute("SELECT count(*) FROM capture_conflict").fetchone()[0] == 0
            row = cast(
                tuple[object, ...],
                connection.execute(
                    "SELECT source_type, lifecycle, block_reason FROM capture_source WHERE source_id = ?", (source_id,)
                ).fetchone(),
            )
            assert tuple(row) == ("attempt", "receiving", None)
