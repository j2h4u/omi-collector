from __future__ import annotations

import hashlib
from pathlib import Path

import pytest
from scripts import check_fallback_ledger_compatibility as compatibility

from omi_collector.capture.adapters import ready_bundles
from omi_collector.capture.adapters.collector_state_db import StateDB, StateKind


def _publication_db(
    path: Path, *, legacy_state: str | None = None, with_dates: bool = False
) -> tuple[str, dict[str, object]]:
    draft_digest = "a" * 64
    bundle_id = hashlib.sha256(f"0:1:{draft_digest}".encode()).hexdigest()
    entry: dict[str, object] = {
        "records_sha256": "b" * 64,
        "state": "ready" if legacy_state is None else legacy_state,
        "start_sequence": 0,
        "next_sequence": 1,
        "draft_raw_sha256": draft_digest,
    }
    if legacy_state is None:
        entry.update(
            format="omi-audio-v2",
            order=0,
            audio_sha256="c" * 64,
            audio_bytes=1,
            manifest_sha256="d" * 64,
            manifest_bytes=1,
            packet_count=1,
            sources=[{"start_sequence": 0, "next_sequence": 1, "raw_sha256": draft_digest}],
        )
    if with_dates and legacy_state is not None:
        entry.update(formed_on_utc="2026-10-17", recorded_on_utc_min=None, recorded_on_utc_max=None)
    database = StateDB(path)
    database.put(StateKind.READY_PUBLICATION, "frontier", {"value": 1})
    database.put(StateKind.READY_PUBLICATION, bundle_id, entry)
    return bundle_id, entry


def test_fallback_check_uses_current_validator_and_does_not_write_database(tmp_path: Path) -> None:
    path = tmp_path / "collector-state.sqlite3"
    bundle_id, entry = _publication_db(path)
    before = path.read_bytes()

    assert ready_bundles._valid_ledger_entry(bundle_id, entry)
    assert compatibility.compatible(path)
    assert path.read_bytes() == before


def test_fallback_check_rejects_date_fields_for_strict_old_reader(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "collector-state.sqlite3"
    _publication_db(path, legacy_state="retired", with_dates=True)
    old_fields = ready_bundles._LEDGER_ENTRY_FIELDS
    monkeypatch.setattr(
        compatibility,
        "_valid_ledger_entry",
        lambda _bundle_id, entry: isinstance(entry, dict) and set(entry) == old_fields,
    )

    assert not compatibility.compatible(path)


def test_fallback_check_rejects_active_legacy_ledger_entry(tmp_path: Path) -> None:
    path = tmp_path / "collector-state.sqlite3"
    bundle_id, entry = _publication_db(path, legacy_state="ready")

    assert not ready_bundles._valid_ledger_entry(bundle_id, entry)
    assert not compatibility.compatible(path)


def test_fallback_check_fails_closed_for_missing_or_unreadable_database(tmp_path: Path) -> None:
    assert not compatibility.compatible(tmp_path / "missing.sqlite3")

    corrupt = tmp_path / "corrupt.sqlite3"
    corrupt.write_text("not sqlite", encoding="utf-8")
    assert not compatibility.compatible(corrupt)
