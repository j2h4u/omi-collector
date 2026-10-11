"""Legacy-state migration and quarantine crash-replay boundaries."""

from __future__ import annotations

from pathlib import Path

import pytest

from omi_collector.capture.adapters.collector_state_db import StateKind
from omi_collector.capture.adapters.quarantine import (
    _reconcile_committed_move,
    _reconcile_quarantine_move,
    legacy_quarantine_row,
)
from omi_collector.capture.adapters.ready_bundles import (
    ReadyBundleError,
    _bundle_id,
    _BundleFacts,
    legacy_ready_publications,
)
from omi_collector.capture.adapters.staging_contract import StagingError
from staging_test_factory import StagingStore


def _quarantined_attempt(tmp_path: Path) -> tuple[StagingStore, Path, dict[str, object]]:
    store = StagingStore(tmp_path / "spool", tmp_path / "capture")
    attempt = store.prepare_streaming_attempt(10, 1)
    destination = store.quarantine_attempt_source(attempt.attempt_id)
    row = store._filesystem.state_db.get(StateKind.QUARANTINE, destination.name)
    assert row is not None
    return store, destination, row


def test_quarantine_move_replay_completes_same_inode_source_move(tmp_path: Path) -> None:
    store, destination, row = _quarantined_attempt(tmp_path)
    source = store.attempts_root / str(row["source_name"])
    row["move_committed"] = False
    store._filesystem.state_db.put(StateKind.QUARANTINE, destination.name, row)
    destination.replace(source)

    _reconcile_quarantine_move(store._filesystem, destination.name, row)

    assert destination.is_dir()
    assert not source.exists()
    committed = store._filesystem.state_db.get(StateKind.QUARANTINE, destination.name)
    assert committed is not None and committed["move_committed"] is True


def test_quarantine_move_replay_rejects_replaced_destination_inode(tmp_path: Path) -> None:
    store, destination, row = _quarantined_attempt(tmp_path)
    replacement = destination.with_name("replacement")
    original = destination.with_name("original")
    replacement.mkdir()
    destination.rename(original)
    replacement.rename(destination)

    with pytest.raises(StagingError, match="destination identity changed"):
        _reconcile_quarantine_move(store._filesystem, destination.name, row)


def test_quarantine_move_replay_ignores_recreated_source_when_destination_matches(tmp_path: Path) -> None:
    store, destination, row = _quarantined_attempt(tmp_path)
    source = store.attempts_root / str(row["source_name"])
    source.mkdir()
    row["move_committed"] = False
    store._filesystem.state_db.put(StateKind.QUARANTINE, destination.name, row)

    _reconcile_quarantine_move(store._filesystem, destination.name, row)

    assert destination.is_dir()
    assert source.is_dir()
    committed = store._filesystem.state_db.get(StateKind.QUARANTINE, destination.name)
    assert committed is not None and committed["move_committed"] is True


def test_quarantine_move_replay_rejects_changed_source_without_destination(tmp_path: Path) -> None:
    store, destination, row = _quarantined_attempt(tmp_path)
    source = store.attempts_root / str(row["source_name"])
    destination.rename(destination.with_name("retained-evidence"))
    source.mkdir()
    row["move_committed"] = False

    with pytest.raises(StagingError, match="source identity changed"):
        _reconcile_quarantine_move(store._filesystem, destination.name, row)


def test_committed_move_keeps_destination_and_rejects_missing_evidence(tmp_path: Path) -> None:
    store, destination, row = _quarantined_attempt(tmp_path)

    _reconcile_committed_move(store._filesystem, destination.name, row, False, True)
    assert destination.is_dir()
    with pytest.raises(StagingError, match="without durable deletion intent"):
        _reconcile_committed_move(store._filesystem, destination.name, row, False, False)


def test_committed_move_forgets_only_expired_durable_deletion(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    store, destination, row = _quarantined_attempt(tmp_path)
    row.update(
        deletion_pending=True,
        terminal_state="published",
        terminal_at_unix_ns=100,
    )
    store._filesystem.state_db.put(StateKind.QUARANTINE, destination.name, row)
    monkeypatch.setattr("omi_collector.capture.adapters.quarantine._wall_clock_ns", lambda: 101)
    store._filesystem._terminal_retention_seconds = 0.000000001
    attempt_id = row["attempt_id"]

    _reconcile_committed_move(store._filesystem, destination.name, row, False, False)

    assert store._filesystem.state_db.get(StateKind.QUARANTINE, destination.name) is None
    assert store._filesystem.state_db.get(StateKind.ATTEMPT, str(attempt_id)) is None


def test_legacy_ready_ledger_preserves_frontier_and_validated_entries(tmp_path: Path) -> None:
    source = tmp_path / "ready-publications.json"
    bundle_id = _bundle_id(_BundleFacts(1, 2, 1, "c" * 64))
    source.write_text(
        '{"frontier":17,"bundles":{"'
        + bundle_id
        + '":{"state":"retired","start_sequence":1,"next_sequence":2,"records_sha256":"'
        + "b" * 64
        + '","draft_raw_sha256":"'
        + "c" * 64
        + '"}}}',
        encoding="utf-8",
    )

    rows = legacy_ready_publications(source)

    assert rows["frontier"] == {"value": 17}
    assert rows[bundle_id]["state"] == "retired"


def test_legacy_ready_ledger_rejects_invalid_rows_without_partial_conversion(tmp_path: Path) -> None:
    source = tmp_path / "ready-publications.json"
    source.write_text('{"frontier":2,"bundles":{"bad":{}}}', encoding="utf-8")

    with pytest.raises(ReadyBundleError, match="invalid"):
        legacy_ready_publications(source)


def test_legacy_published_directory_marker_maps_terminal_timestamp(tmp_path: Path) -> None:
    entry = tmp_path / "published-attempt"
    entry.mkdir()
    (entry / "records.bin").write_bytes(b"retained evidence")
    (entry / "published.json").write_text(
        '{"version":1,"state":"published","published_at_unix_ns":123}',
        encoding="utf-8",
    )

    row = legacy_quarantine_row(entry)

    assert row["terminal_state"] == "published"
    assert row["terminal_at_unix_ns"] == 123
    assert row["original_name"] == entry.name
