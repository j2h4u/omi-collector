"""Retained sparse recovery remains authenticated and clock-projected at cutover."""

from __future__ import annotations

import json
from dataclasses import dataclass, replace
from hashlib import sha256
from pathlib import Path
from typing import cast

import pytest

import omi_collector.state_migration as migration
from omi_collector.capture.adapters.clock_segments import ClockSegmentMap
from omi_collector.capture.adapters.collector_state_db import StateKind
from omi_collector.capture.domain.ring_protocol import RECORD_SIZE
from omi_collector.state_migration import StateMigrationError, migrate_state, open_runtime_state
from omi_collector.storage_layout import StorageLayout, load_operator_config

_RECOVERY_RELATIVE_PATH = Path("recovery/10890392-13085638")
_RECORD_ORDER = "ascending original physical sequence; exact byte-identical overlaps retained once"


@dataclass(frozen=True)
class _RecoveryCase:
    layout: StorageLayout
    observation: Path
    manifest: Path
    records: Path
    status_lock: Path


@pytest.fixture
def recovery_case(tmp_path: Path) -> _RecoveryCase:
    layout = _layout(tmp_path)
    _legacy_device(layout, 20, 21)
    observation = _legacy_clock_observation(layout, 5, 21)
    manifest, records = _write_recovery_package(layout)
    status_lock = layout.collector.root / "operational-status.json.lock"
    status_lock.touch()
    return _RecoveryCase(layout, observation, manifest, records, status_lock)


def _layout(tmp_path: Path) -> StorageLayout:
    config = tmp_path / "config.toml"
    config.write_text(
        '[pendant]\naddress = "AA:BB:CC:DD:EE:FF"\n[ready]\ntarget_audio_seconds = 120\n',
        encoding="utf-8",
    )
    layout = load_operator_config(config).storage
    layout.collector.root.mkdir()
    return layout


def _write_recovery_package(layout: StorageLayout) -> tuple[Path, Path]:
    recovery_dir = layout.root / _RECOVERY_RELATIVE_PATH
    recovery_dir.mkdir(parents=True)
    records = recovery_dir / "records.bin"
    records.write_bytes(b"".join(_record(timestamp) for timestamp in (1000, 1001, 1002)))
    manifest = recovery_dir / "rescue-manifest.json"
    manifest.write_text(
        json.dumps(_recovery_manifest(records.read_bytes()), sort_keys=True, separators=(",", ":")),
        encoding="utf-8",
    )
    return manifest, records


def _record(timestamp: int) -> bytes:
    return timestamp.to_bytes(4, "big") + bytes(RECORD_SIZE - 4)


def _legacy_device(layout: StorageLayout, read_sequence: int, write_sequence: int) -> None:
    path = layout.collector.root / "device.json"
    path.write_text(
        json.dumps(
            {
                "schema_version": 3,
                "latest": {
                    "read_sequence": read_sequence,
                    "write_sequence": write_sequence,
                    "capacity_packets": 64,
                    "dropped_packets": 1,
                    "packet_size": 240,
                },
                "metrics": {"observation_count": 1, "initial": 1, "observed_increase": 0, "regression_count": 0},
            },
            sort_keys=True,
            separators=(",", ":"),
        ),
        encoding="utf-8",
    )


def _legacy_clock_observation(layout: StorageLayout, start: int, end: int) -> Path:
    directory = layout.collector.root / "clock-observations"
    directory.mkdir()
    path = directory / "observation-1.json"
    path.write_text(
        json.dumps(
            {
                "version": 1,
                "observation_id": "observation-1",
                "causal_order": 0,
                "evidence_kind": "native_trusted",
                "session_id": "session-1",
                "host_boot_id": "boot-1",
                "host_realtime_start": 1010.0,
                "host_realtime_end": 1010.0,
                "host_monotonic_start": 1.0,
                "host_monotonic_end": 1.0,
                "device_epoch": 1000,
                "info_sequence_min": start,
                "info_sequence_max": end,
                "operation_id": None,
                "effective_boundary_sequence": None,
                "observation_role": "standalone",
                "parent_observation_id": None,
                "raw_timestamp": None,
                "raw_timestamp_hash": None,
            },
            sort_keys=True,
            separators=(",", ":"),
        )
        + "\n",
        encoding="utf-8",
    )
    return path


def _recovery_manifest(records: bytes) -> dict[str, object]:
    return {
        "format": "omi-recovery-v1",
        "package_kind": "retained_sparse_sequence_recovery",
        "complete": True,
        "records_file": "records.bin",
        "record_size": RECORD_SIZE,
        "record_order": _RECORD_ORDER,
        "records_sha256": sha256(records).hexdigest(),
        "source_window": {"start_sequence": 5, "closure_next_sequence": 9},
        "counts": {"unique_retained_records": 3, "output_bytes": 3 * RECORD_SIZE},
        "output_ranges": [
            {
                "start_sequence": 5,
                "next_sequence": 7,
                "record_count": 2,
                "records_file_offset_bytes": 0,
                "source_sha256": "a" * 64,
                "source_path": "recovery-source-a.bin",
            },
            {
                "start_sequence": 8,
                "next_sequence": 9,
                "record_count": 1,
                "records_file_offset_bytes": 2 * RECORD_SIZE,
                "source_sha256": "b" * 64,
                "source_path": "recovery-source-b.bin",
            },
        ],
        "sequence_gaps": [{"start_sequence": 7, "next_sequence": 8, "record_count": 1, "cause": "unknown"}],
        "missing_sequence_count": 1,
        "publication": {
            "consumer_compatibility_asserted": False,
            "ready_bundle": False,
            "ready_ledger_written": False,
        },
    }


def _retained_files(case: _RecoveryCase) -> tuple[bytes, bytes]:
    return case.manifest.read_bytes(), case.records.read_bytes()


def _assert_rejected_without_deletion(case: _RecoveryCase, message: str) -> None:
    retained_before = _retained_files(case)

    with pytest.raises(StateMigrationError, match=message):
        migrate_state(case.layout, legacy_phy_state_homes=())

    assert case.observation.is_file()
    assert _retained_files(case) == retained_before
    assert not case.layout.collector.state_db.exists()


def test_migration_preserves_clock_only_for_retained_recovery_and_firmware_ranges(
    recovery_case: _RecoveryCase,
) -> None:
    retained_before = _retained_files(recovery_case)

    migrate_state(recovery_case.layout, legacy_phy_state_homes=())

    assert not recovery_case.observation.exists()
    assert _retained_files(recovery_case) == retained_before
    state = open_runtime_state(recovery_case.layout, read_only=True).get(StateKind.CLOCK, "current")
    assert state is not None
    assert state["ranges"] == [
        {
            "start_sequence": 5,
            "next_sequence": 7,
            "offset_seconds": 10.0,
            "uncertainty_seconds": 1.0,
            "confidence": "approximate",
            "source_id": "observation-1",
        },
        {
            "start_sequence": 8,
            "next_sequence": 9,
            "offset_seconds": 10.0,
            "uncertainty_seconds": 1.0,
            "confidence": "approximate",
            "source_id": "observation-1",
        },
        {
            "start_sequence": 20,
            "next_sequence": 21,
            "offset_seconds": 10.0,
            "uncertainty_seconds": 1.0,
            "confidence": "approximate",
            "source_id": "observation-1",
        },
    ]


def test_malformed_retained_recovery_gap_blocks_migration(recovery_case: _RecoveryCase) -> None:
    document = cast(dict[str, object], json.loads(recovery_case.manifest.read_text(encoding="utf-8")))
    gaps = cast(list[dict[str, object]], document["sequence_gaps"])
    gaps[0]["cause"] = "unclassified"
    recovery_case.manifest.write_text(json.dumps(document), encoding="utf-8")

    _assert_rejected_without_deletion(recovery_case, "retained recovery gap inventory is invalid")


def test_retained_recovery_hash_mismatch_blocks_migration(recovery_case: _RecoveryCase) -> None:
    raw = recovery_case.records.read_bytes()
    recovery_case.records.write_bytes(raw[:-1] + bytes((raw[-1] ^ 1,)))

    _assert_rejected_without_deletion(recovery_case, "retained recovery records differ from passport")


def test_out_of_order_retained_ranges_block_migration(recovery_case: _RecoveryCase) -> None:
    document = cast(dict[str, object], json.loads(recovery_case.manifest.read_text(encoding="utf-8")))
    ranges = cast(list[dict[str, object]], document["output_ranges"])
    ranges.reverse()
    ranges[0]["records_file_offset_bytes"] = 0
    ranges[1]["records_file_offset_bytes"] = RECORD_SIZE
    recovery_case.manifest.write_text(json.dumps(document), encoding="utf-8")

    _assert_rejected_without_deletion(recovery_case, "retained recovery raw sequence order is invalid")


def test_changed_clock_projector_blocks_activation_and_legacy_cleanup(
    recovery_case: _RecoveryCase, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(ClockSegmentMap, "from_state", staticmethod(lambda _state: ClockSegmentMap(())))

    _assert_rejected_without_deletion(recovery_case, "clock time_ranges differ after import")


def test_recovery_without_legacy_collector_state_refuses_runtime_and_migration(tmp_path: Path) -> None:
    layout = _layout(tmp_path)
    manifest, records = _write_recovery_package(layout)
    retained_before = manifest.read_bytes(), records.read_bytes()

    with pytest.raises(StateMigrationError, match="requires offline migrate-state"):
        open_runtime_state(layout)
    with pytest.raises(StateMigrationError, match="without recognized control state"):
        migrate_state(layout, legacy_phy_state_homes=())

    assert (manifest.read_bytes(), records.read_bytes()) == retained_before
    assert not layout.collector.state_db.exists()


def test_absent_rescue_is_optional_for_installed_migration_root(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    layout = replace(_layout(tmp_path), root=Path("/srv/pipelines/omi"))
    manifest = tmp_path / "absent-rescue" / "rescue-manifest.json"
    monkeypatch.setattr(migration, "_recovery_manifest_path", lambda _layout: manifest)

    assert migration._load_retained_recovery(layout) is None

    manifest.parent.mkdir()
    with pytest.raises(StateMigrationError, match="retained recovery passport is missing"):
        migration._load_retained_recovery(layout)


def test_completed_migration_allows_retired_rescue_pair(recovery_case: _RecoveryCase) -> None:
    migrate_state(recovery_case.layout, legacy_phy_state_homes=())
    recovery_case.records.unlink()
    recovery_case.manifest.unlink()
    recovery_case.manifest.parent.rmdir()

    assert migrate_state(recovery_case.layout, legacy_phy_state_homes=()) == {"removed_files": 0}
    assert open_runtime_state(recovery_case.layout, read_only=True).get(StateKind.CLOCK, "current") is not None


def _interrupt_after_clock_observation_removal(case: _RecoveryCase, monkeypatch: pytest.MonkeyPatch) -> None:
    original_sync = migration._fsync_directory
    failed = False

    def fail_after_observation_removal(path: Path) -> None:
        nonlocal failed
        original_sync(path)
        if path == case.observation.parent and not case.observation.exists() and not failed:
            failed = True
            raise OSError("simulated cleanup interruption")

    monkeypatch.setattr(migration, "_fsync_directory", fail_after_observation_removal)
    with pytest.raises(OSError, match="simulated cleanup interruption"):
        migrate_state(case.layout, legacy_phy_state_homes=())
    assert failed
    assert not case.observation.exists()
    assert case.status_lock.exists()
    with pytest.raises(StateMigrationError, match="incomplete"):
        open_runtime_state(case.layout)


def test_interrupted_cleanup_retries_with_unchanged_retained_recovery(
    recovery_case: _RecoveryCase, monkeypatch: pytest.MonkeyPatch
) -> None:
    retained_before = _retained_files(recovery_case)
    _interrupt_after_clock_observation_removal(recovery_case, monkeypatch)

    assert migrate_state(recovery_case.layout, legacy_phy_state_homes=()) == {"removed_files": 2}

    assert not recovery_case.status_lock.exists()
    assert _retained_files(recovery_case) == retained_before
    assert open_runtime_state(recovery_case.layout, read_only=True).get(StateKind.CLOCK, "current") is not None


def test_interrupted_cleanup_rejects_changed_recovery_before_deleting_remaining_legacy_file(
    recovery_case: _RecoveryCase, monkeypatch: pytest.MonkeyPatch
) -> None:
    _interrupt_after_clock_observation_removal(recovery_case, monkeypatch)
    retained_records = recovery_case.records.read_bytes()
    document = cast(dict[str, object], json.loads(recovery_case.manifest.read_text(encoding="utf-8")))
    ranges = cast(list[dict[str, object]], document["output_ranges"])
    ranges[0]["source_path"] = "changed-but-still-valid-source.bin"
    recovery_case.manifest.write_text(json.dumps(document), encoding="utf-8")
    changed_manifest = recovery_case.manifest.read_bytes()

    with pytest.raises(StateMigrationError, match="retained recovery changed before migration cleanup"):
        migrate_state(recovery_case.layout, legacy_phy_state_homes=())

    assert recovery_case.status_lock.is_file()
    assert (recovery_case.layout.collector.root / "device.json").is_file()
    assert recovery_case.manifest.read_bytes() == changed_manifest
    assert recovery_case.records.read_bytes() == retained_records
