from __future__ import annotations

import json
from pathlib import Path

import pytest

from omi_collector.capture.adapters.legacy_clock_import import LegacyClockImportError, read_legacy_clock


def _write_observation(
    root: Path,
    overrides: dict[str, object] | None = None,
) -> None:
    directory = root / "clock-observations"
    directory.mkdir(parents=True, exist_ok=True)
    row: dict[str, object] = {
        "version": 1,
        "observation_id": "observation-a",
        "causal_order": 0,
        "evidence_kind": "native_trusted",
        "session_id": "session-a",
        "host_boot_id": "boot-a",
        "host_realtime_start": 1000.0,
        "host_realtime_end": 1000.0,
        "host_monotonic_start": 10.0,
        "host_monotonic_end": 10.0,
        "device_epoch": 900,
        "info_sequence_min": 10,
        "info_sequence_max": 20,
        "operation_id": None,
        "effective_boundary_sequence": None,
        "observation_role": "standalone",
        "parent_observation_id": None,
        "raw_timestamp": None,
        "raw_timestamp_hash": None,
    }
    row.update(overrides or {})
    observation_id = str(row["observation_id"])
    (directory / f"{observation_id}.json").write_text(
        json.dumps(row, ensure_ascii=True, sort_keys=True, separators=(",", ":")) + "\n",
        encoding="utf-8",
    )


def _write_correction(root: Path, *, operation_id: str = "operation-a", state: str = "applied") -> None:
    directory = root / "clock-corrections"
    directory.mkdir(parents=True, exist_ok=True)
    verified = 901 if state in {"applied", "resolved"} else None
    boundary_max = 15 if state in {"applied", "resolved", "unknown"} else None
    row = {
        "version": 2,
        "operation_id": operation_id,
        "state": state,
        "observed_epoch": 900,
        "target_epoch": 901,
        "drift_seconds": 100.0,
        "boundary_sequence_min": 10,
        "boundary_sequence_max": boundary_max,
        "verified_epoch": verified,
    }
    (directory / f"{operation_id}.json").write_text(json.dumps(row), encoding="utf-8")


def _write_membership(
    root: Path,
    *,
    observation_id: str = "observation-a",
    session_id: str = "session-a",
    start: int = 12,
    end: int = 14,
) -> None:
    directory = root / "clock-memberships"
    directory.mkdir(parents=True, exist_ok=True)
    row = {
        "observation_id": observation_id,
        "session_id": session_id,
        "start_sequence": start,
        "next_sequence": end,
    }
    name = f"{start:020d}-{end:020d}-{observation_id}.json"
    (directory / name).write_text(json.dumps(row), encoding="utf-8")


def test_importer_reads_old_clock_ledgers_and_reconstructs_projection(tmp_path: Path) -> None:
    _write_observation(tmp_path)
    _write_correction(tmp_path)
    _write_membership(tmp_path)

    legacy = read_legacy_clock(tmp_path)

    assert len(legacy.observations) == 1
    assert len(legacy.corrections) == 1
    assert len(legacy.memberships) == 1
    assert legacy.segments.utc_for(12, 900) == 1000.0
    assert legacy.segments.utc_for(11, 900) == 1000.0
    assert legacy.segments.utc_for(15, 900) == 1000.0
    assert legacy.segments.utc_for(20, 900) is None


def test_importer_rejects_noncanonical_observation_json(tmp_path: Path) -> None:
    _write_observation(tmp_path)
    path = tmp_path / "clock-observations" / "observation-a.json"
    path.write_text(json.dumps(json.loads(path.read_text(encoding="utf-8")), indent=2) + "\n", encoding="utf-8")

    with pytest.raises(LegacyClockImportError, match="observation ledger is invalid"):
        read_legacy_clock(tmp_path)


@pytest.mark.parametrize(
    "overrides",
    [
        {"version": 2},
        {"evidence_kind": "untrusted"},
        {"causal_order": True},
        {"info_sequence_max": 1 << 64},
        {"raw_timestamp": 1},
        {"raw_timestamp_hash": "0" * 63},
        {"effective_boundary_sequence": True},
    ],
)
def test_importer_rejects_invalid_observation_evidence(tmp_path: Path, overrides: dict[str, object]) -> None:
    _write_observation(tmp_path, overrides)

    with pytest.raises(LegacyClockImportError, match="observation ledger is invalid"):
        read_legacy_clock(tmp_path)


@pytest.mark.parametrize(
    ("state", "boundary_max", "verified_epoch"),
    [("prepared", 15, None), ("not_applied", 15, 901), ("unknown", None, None), ("applied", 15, None)],
)
def test_importer_rejects_invalid_correction_state_fields(
    tmp_path: Path, state: str, boundary_max: int | None, verified_epoch: int | None
) -> None:
    directory = tmp_path / "clock-corrections"
    directory.mkdir()
    row: dict[str, object] = {
        "version": 2,
        "operation_id": "operation-a",
        "state": state,
        "observed_epoch": 900,
        "target_epoch": 901,
        "drift_seconds": 100.0,
        "boundary_sequence_min": 10,
        "boundary_sequence_max": boundary_max,
        "verified_epoch": verified_epoch,
    }
    (directory / "operation-a.json").write_text(json.dumps(row), encoding="utf-8")

    with pytest.raises(LegacyClockImportError, match="correction ledger is invalid"):
        read_legacy_clock(tmp_path)


def test_importer_rejects_causal_gaps_and_orphaned_memberships(tmp_path: Path) -> None:
    _write_observation(tmp_path, {"causal_order": 2})

    with pytest.raises(LegacyClockImportError, match="causal order has a gap"):
        read_legacy_clock(tmp_path)

    observations = tmp_path / "clock-observations"
    for path in observations.iterdir():
        path.unlink()
    observations.rmdir()
    _write_membership(tmp_path, observation_id="missing")

    with pytest.raises(LegacyClockImportError, match="no durable observation"):
        read_legacy_clock(tmp_path)


def test_importer_rejects_invalid_membership_filename(tmp_path: Path) -> None:
    _write_observation(tmp_path)
    _write_membership(tmp_path)
    path = next((tmp_path / "clock-memberships").iterdir())
    path.rename(path.with_name("membership.json"))

    with pytest.raises(LegacyClockImportError, match="membership ledger is invalid"):
        read_legacy_clock(tmp_path)


def test_importer_rejects_overlapping_memberships(tmp_path: Path) -> None:
    _write_observation(tmp_path)
    _write_membership(tmp_path, start=10, end=14)
    _write_membership(tmp_path, start=13, end=15)

    with pytest.raises(LegacyClockImportError, match="memberships overlap"):
        read_legacy_clock(tmp_path)
