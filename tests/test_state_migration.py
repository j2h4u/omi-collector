"""Offline cutover keeps the old state until a validated DB is active."""

from __future__ import annotations

import json
from hashlib import sha256
from pathlib import Path

import pytest

import omi_collector.state_migration as migration
from omi_collector.capture.adapters.clock_segments import ClockSegmentMap
from omi_collector.capture.adapters.collector_state_db import StateKind
from omi_collector.capture.adapters.phy_guard import CommandResult
from omi_collector.capture.domain.ring_protocol import RECORD_SIZE
from omi_collector.state_migration import StateMigrationError, migrate_state, open_runtime_state
from omi_collector.storage_layout import StorageLayout, load_operator_config


def _layout(tmp_path: Path) -> StorageLayout:
    config = tmp_path / "config.toml"
    config.write_text(
        '[pendant]\naddress = "AA:BB:CC:DD:EE:FF"\n[ready]\ntarget_audio_seconds = 120\n',
        encoding="utf-8",
    )
    layout = load_operator_config(config).storage
    layout.collector.root.mkdir()
    return layout


def test_fresh_install_without_collector_data_needs_no_offline_import(tmp_path: Path) -> None:
    config = tmp_path / "config.toml"
    config.write_text(
        '[pendant]\naddress = "AA:BB:CC:DD:EE:FF"\n[ready]\ntarget_audio_seconds = 120\n', encoding="utf-8"
    )
    layout = load_operator_config(config).storage

    assert migrate_state(layout, legacy_phy_state_homes=()) == {"removed_files": 0}
    assert not layout.collector.root.exists()


def test_missing_collector_directory_does_not_hide_legacy_phy_state(tmp_path: Path) -> None:
    config = tmp_path / "config.toml"
    config.write_text(
        '[pendant]\naddress = "AA:BB:CC:DD:EE:FF"\n[ready]\ntarget_audio_seconds = 120\n', encoding="utf-8"
    )
    layout = load_operator_config(config).storage
    phy_home = tmp_path / "old-state"
    lock = phy_home / "omi-collector" / "hci0.phy.lock"
    lock.parent.mkdir(parents=True)
    lock.touch()

    with pytest.raises(StateMigrationError, match="legacy PHY state exists"):
        migrate_state(layout, legacy_phy_state_homes=(phy_home,))

    assert lock.is_file()


def _legacy_device(layout: StorageLayout) -> Path:
    path = layout.collector.root / "device.json"
    document = {
        "schema_version": 3,
        "latest": {
            "read_sequence": 7,
            "write_sequence": 9,
            "capacity_packets": 64,
            "dropped_packets": 1,
            "packet_size": 240,
        },
        "metrics": {
            "observation_count": 1,
            "initial": 1,
            "observed_increase": 0,
            "regression_count": 0,
        },
    }
    path.write_text(json.dumps(document, sort_keys=True, separators=(",", ":")), encoding="utf-8")
    return path


def _legacy_clock_observation(layout: StorageLayout) -> Path:
    directory = layout.collector.root / "clock-observations"
    directory.mkdir()
    path = directory / "observation-1.json"
    document = {
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
        "info_sequence_min": 7,
        "info_sequence_max": 9,
        "operation_id": None,
        "effective_boundary_sequence": None,
        "observation_role": "standalone",
        "parent_observation_id": None,
        "raw_timestamp": None,
        "raw_timestamp_hash": None,
    }
    path.write_text(json.dumps(document, sort_keys=True, separators=(",", ":")) + "\n", encoding="utf-8")
    return path


def _legacy_attempt(layout: StorageLayout) -> Path:
    attempt_id = "a" * 32
    path = layout.collector.attempts / attempt_id
    path.mkdir(parents=True)
    raw = (1000).to_bytes(4, "big") + bytes(RECORD_SIZE - 4)
    (path / "records.bin").write_bytes(raw)
    descriptor = {
        "attempt_id": attempt_id,
        "schema_version": 2,
        "start_sequence": 7,
        "packet_count": 1,
        "record_size": RECORD_SIZE,
        "read_begin_start": None,
        "read_begin_count": None,
    }
    checkpoint = {"version": 1, "attempt_id": attempt_id, "record_count": 1, "raw_sha256": sha256(raw).hexdigest()}
    for name, value in (("attempt.json", descriptor), ("checkpoint.json", checkpoint)):
        (path / name).write_text(json.dumps(value, sort_keys=True, separators=(",", ":")), encoding="utf-8")
    return path


def test_migration_activates_parity_checked_db_and_removes_old_control_files(tmp_path: Path) -> None:
    layout = _layout(tmp_path)
    old = _legacy_device(layout)
    status_lock = layout.collector.root / "operational-status.json.lock"
    status_lock.touch()
    phy_home = tmp_path / "phy-home"
    phy_lock = phy_home / "omi-collector" / "hci0.phy.lock"
    phy_lock.parent.mkdir(parents=True)
    phy_lock.touch()

    with pytest.raises(StateMigrationError, match="requires offline migrate-state"):
        open_runtime_state(layout)
    result = migrate_state(layout, legacy_phy_state_homes=(phy_home,))

    assert result == {"removed_files": 3}
    assert not old.exists() and not status_lock.exists() and not phy_lock.exists()
    assert layout.collector.lock.is_file()
    assert layout.collector.lock.stat().st_size == 0
    db = open_runtime_state(layout, read_only=True)
    assert db.get(StateKind.FIRMWARE, "latest") is not None
    assert migrate_state(layout, legacy_phy_state_homes=(phy_home,)) == {"removed_files": 0}


def test_migration_rejects_unknown_attempt_json_without_deleting_it(tmp_path: Path) -> None:
    layout = _layout(tmp_path)
    _legacy_device(layout)
    attempt = _legacy_attempt(layout)
    stray = attempt / "receipt.json"
    stray.write_text('{"operator":"keep"}\n', encoding="utf-8")

    with pytest.raises(StateMigrationError, match="unclassified mutable attempt"):
        migrate_state(layout, legacy_phy_state_homes=())

    assert stray.is_file()
    assert not layout.collector.state_db.exists()


def test_migration_rejects_malformed_draft_receipt_before_cleanup(tmp_path: Path) -> None:
    layout = _layout(tmp_path)
    _legacy_device(layout)
    draft = layout.draft / ("a" * 32)
    draft.mkdir(parents=True)
    receipt = draft / "receipt.json"
    receipt.write_text('{"operator":"keep"}\n', encoding="utf-8")

    with pytest.raises(ValueError, match="sealed receipt schema"):
        migrate_state(layout, legacy_phy_state_homes=())

    assert receipt.is_file()
    assert not layout.collector.state_db.exists()


def test_migration_resumes_direct_cleanup_after_interruption(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    layout = _layout(tmp_path)
    old = _legacy_device(layout)
    original = migration._remove_empty_legacy_directories
    calls = 0

    def interrupt_once(target: StorageLayout) -> None:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise OSError("simulated interruption")
        original(target)

    monkeypatch.setattr(migration, "_remove_empty_legacy_directories", interrupt_once)
    with pytest.raises(OSError, match="simulated interruption"):
        migrate_state(layout, legacy_phy_state_homes=(tmp_path / "phy-home",))

    assert not old.exists()
    with pytest.raises(StateMigrationError, match="incomplete"):
        open_runtime_state(layout)
    assert migrate_state(layout, legacy_phy_state_homes=(tmp_path / "phy-home",)) == {"removed_files": 0}
    assert open_runtime_state(layout, read_only=True).get(StateKind.FIRMWARE, "latest") is not None


def test_migration_rejects_changed_legacy_file_before_cleanup(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    layout = _layout(tmp_path)
    old = _legacy_device(layout)
    original = migration._resume_cleanup

    def interrupt(*args: object, **kwargs: object) -> dict[str, int]:
        del args, kwargs
        raise OSError("simulated activation interruption")

    monkeypatch.setattr(migration, "_resume_cleanup", interrupt)
    with pytest.raises(OSError, match="simulated activation interruption"):
        migrate_state(layout, legacy_phy_state_homes=(tmp_path / "phy-home",))
    monkeypatch.setattr(migration, "_resume_cleanup", original)
    old.write_bytes(old.read_bytes() + b"\n")

    with pytest.raises(StateMigrationError, match="changed"):
        migrate_state(layout, legacy_phy_state_homes=(tmp_path / "phy-home",))
    assert old.exists()
    assert layout.collector.state_db.exists()


def test_retained_immutable_manifest_is_not_legacy_control_state(tmp_path: Path) -> None:
    layout = _layout(tmp_path)
    draft = layout.draft / "bundle"
    draft.mkdir(parents=True)
    (draft / "manifest.json").write_text("{}", encoding="utf-8")

    assert not migration._has_legacy_state(layout)


def test_migration_restores_legacy_phy_before_deleting_marker(tmp_path: Path) -> None:
    layout = _layout(tmp_path)
    _legacy_device(layout)
    phy_home = tmp_path / "phy-home"
    legacy = phy_home / "omi-collector" / "hci0.phy-recovery.json"
    legacy.parent.mkdir(parents=True)
    selected = ["LE1MTX", "LE1MRX", "LE2MTX", "LE2MRX"]
    legacy.write_text(
        json.dumps({"version": 1, "adapter": "hci0", "selected_phys": selected, "owner": {"pid": 42, "start_time": 1}}),
        encoding="utf-8",
    )
    calls: list[tuple[str, ...]] = []

    async def runner(argv: tuple[str, ...]) -> CommandResult:
        calls.append(argv)
        return CommandResult(0, "selected PHYs: " + " ".join(selected) if len(calls) == 2 else "")

    migrate_state(layout, legacy_phy_state_homes=(phy_home,), legacy_phy_runner=runner)

    assert len(calls) == 2
    assert calls[0][-4:] == tuple(selected)
    assert not legacy.exists()
    assert open_runtime_state(layout, read_only=True).get(StateKind.FIRMWARE, "latest") is not None


def test_migration_refuses_other_adapter_phy_state(tmp_path: Path) -> None:
    layout = _layout(tmp_path)
    old = _legacy_device(layout)
    phy_home = tmp_path / "phy-home"
    other = phy_home / "omi-collector" / "hci1.phy-recovery.json"
    other.parent.mkdir(parents=True)
    other.write_text("{}", encoding="utf-8")

    with pytest.raises(StateMigrationError, match="another adapter"):
        migrate_state(layout, legacy_phy_state_homes=(phy_home,))
    assert old.exists() and other.exists()
    assert not layout.collector.state_db.exists()


def test_migration_preserves_legacy_clock_projection(tmp_path: Path) -> None:
    layout = _layout(tmp_path)
    _legacy_device(layout)
    observation = _legacy_clock_observation(layout)
    attempt = _legacy_attempt(layout)

    migrate_state(layout, legacy_phy_state_homes=())

    assert not observation.exists()
    assert not (attempt / "attempt.json").exists()
    assert (attempt / "records.bin").is_file()
    clock = open_runtime_state(layout, read_only=True).get(StateKind.CLOCK, "current")
    assert clock is not None
    assert clock["ranges"] == [
        {
            "start_sequence": 7,
            "next_sequence": 9,
            "offset_seconds": 10.0,
            "uncertainty_seconds": 1.0,
            "confidence": "approximate",
            "source_id": "observation-1",
        }
    ]


def test_migration_blocks_if_new_clock_projector_changes_time_ranges(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    layout = _layout(tmp_path)
    _legacy_device(layout)
    observation = _legacy_clock_observation(layout)
    monkeypatch.setattr(ClockSegmentMap, "from_state", staticmethod(lambda _state: ClockSegmentMap(())))

    with pytest.raises(StateMigrationError, match="clock time_ranges differ"):
        migrate_state(layout, legacy_phy_state_homes=())

    assert observation.exists()
    assert not layout.collector.state_db.exists()


def test_historical_clock_metadata_is_classified_then_removed(tmp_path: Path) -> None:
    layout = _layout(tmp_path)
    _legacy_device(layout)
    repairs = layout.collector.root / "timeline-repairs.json"
    repairs.write_text(
        json.dumps(
            {
                "version": 1,
                "repairs": [
                    {"evidence": "operator repair", "start_sequence": 7, "next_sequence": 9, "offset_seconds": 10.5}
                ],
            }
        ),
        encoding="utf-8",
    )
    imports = layout.collector.root / "clock-imports"
    imports.mkdir()
    receipt = imports / "one.json"
    receipt.write_text(
        json.dumps(
            {
                "version": 1,
                "operation_id": "operator-import",
                "entries": [{"sequence": 7}],
                "entry_hashes": ["a" * 64],
                "boundary_calculation": {"confidence": "operator"},
            }
        ),
        encoding="utf-8",
    )

    migrate_state(layout, legacy_phy_state_homes=())

    assert not repairs.exists() and not receipt.exists()
    assert open_runtime_state(layout, read_only=True).get(StateKind.FIRMWARE, "latest") is not None


@pytest.mark.parametrize(
    "document",
    [
        {"version": 2, "repairs": []},
        {"version": 1, "repairs": [{}]},
        {
            "version": 1,
            "repairs": [{"evidence": "", "start_sequence": 7, "next_sequence": 9, "offset_seconds": 10}],
        },
        {
            "version": 1,
            "repairs": [{"evidence": "operator", "start_sequence": 9, "next_sequence": 7, "offset_seconds": 10}],
        },
        {
            "version": 1,
            "repairs": [{"evidence": "operator", "start_sequence": 7, "next_sequence": 9, "offset_seconds": "10"}],
        },
    ],
)
def test_unsupported_historical_repair_blocks_cutover(tmp_path: Path, document: dict[str, object]) -> None:
    layout = _layout(tmp_path)
    _legacy_device(layout)
    repairs = layout.collector.root / "timeline-repairs.json"
    repairs.write_text(json.dumps(document), encoding="utf-8")

    with pytest.raises(StateMigrationError, match="historical timeline repair"):
        migrate_state(layout, legacy_phy_state_homes=())

    assert repairs.exists() and not layout.collector.state_db.exists()


@pytest.mark.parametrize(
    "replacement",
    [
        {"version": 2},
        {"operation_id": ""},
        {"entries": []},
        {"entries": [{}, {}]},
        {"entry_hashes": ["bad"]},
        {"boundary_calculation": []},
    ],
)
def test_unsupported_clock_import_receipt_blocks_cutover(tmp_path: Path, replacement: dict[str, object]) -> None:
    layout = _layout(tmp_path)
    _legacy_device(layout)
    directory = layout.collector.root / "clock-imports"
    directory.mkdir()
    receipt = directory / "one.json"
    document: dict[str, object] = {
        "version": 1,
        "operation_id": "operator-import",
        "entries": [{}],
        "entry_hashes": ["a" * 64],
        "boundary_calculation": {},
    }
    document.update(replacement)
    receipt.write_text(json.dumps(document), encoding="utf-8")

    with pytest.raises(StateMigrationError, match="historical clock import receipt"):
        migrate_state(layout, legacy_phy_state_homes=())

    assert receipt.exists() and not layout.collector.state_db.exists()


def test_quarantine_opaque_raw_is_retained_with_db_identity(tmp_path: Path) -> None:
    layout = _layout(tmp_path)
    _legacy_device(layout)
    layout.collector.quarantine.mkdir()
    opaque = layout.collector.quarantine / "unreadable-source"
    opaque.mkdir()
    raw = opaque / "records.bin"
    raw.write_bytes(b"do not reinterpret raw custody")
    sidecar = opaque.with_name(f"{opaque.name}.json")
    sidecar.write_text(
        json.dumps(
            {
                "version": 1,
                "state": "unprocessable",
                "classified_at_unix_ns": 1,
                "reason": "legacy classification",
                "original_name": opaque.name,
            }
        ),
        encoding="utf-8",
    )

    migrate_state(layout, legacy_phy_state_homes=())

    assert raw.read_bytes() == b"do not reinterpret raw custody"
    assert not sidecar.exists()
    row = open_runtime_state(layout, read_only=True).get(StateKind.QUARANTINE, opaque.name)
    assert row is not None and row["entry_kind"] == "directory" and row["physical_name"] == opaque.name


def test_nested_quarantine_sidecar_identity_blocks_cutover(tmp_path: Path) -> None:
    layout = _layout(tmp_path)
    _legacy_device(layout)
    layout.collector.quarantine.mkdir()
    for name in ("raw", "raw.json", "raw.json.json"):
        (layout.collector.quarantine / name).write_bytes(b"opaque")

    with pytest.raises(StateMigrationError, match="sidecar identity is ambiguous"):
        migrate_state(layout, legacy_phy_state_homes=())

    assert not layout.collector.state_db.exists()


def test_cleanup_marker_rejects_paths_outside_known_legacy_inventory(tmp_path: Path) -> None:
    layout = _layout(tmp_path)
    allowed = layout.collector.root / "device.json"
    marker: dict[str, object] = {
        "version": 1,
        "phase": "cleanup_pending",
        "files": [[str(allowed), "a" * 64]],
        "state_sha256": "b" * 64,
        "custody": None,
    }
    assert migration._marker_fingerprints(marker, layout, ()) == ((str(allowed), "a" * 64),)

    marker["files"] = [[str(tmp_path / "unrelated.json"), "a" * 64]]
    with pytest.raises(StateMigrationError, match="escapes Collector storage"):
        migration._marker_fingerprints(marker, layout, ())
    marker["files"] = [[str(allowed), "a" * 64], [str(allowed), "a" * 64]]
    with pytest.raises(StateMigrationError, match="duplicated"):
        migration._marker_fingerprints(marker, layout, ())
