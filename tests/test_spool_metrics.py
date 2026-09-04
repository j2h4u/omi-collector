from __future__ import annotations

import hashlib
import json
from pathlib import Path
from shutil import rmtree

import pytest

from omi_collector.capture.adapters.firmware_observations import FirmwareObservationStore
from omi_collector.capture.domain.ring_protocol import RingInfo
from omi_collector.spool_metrics import SpoolMetricsError, collect_spool_metrics

RECORD_SIZE = 444


_CAPTURE_ROOTS: set[Path] = set()


def _capture_root(tmp_path: Path) -> Path:
    root = tmp_path.parent / f"{tmp_path.name}-captures"
    if tmp_path not in _CAPTURE_ROOTS:
        rmtree(root, ignore_errors=True)
        _CAPTURE_ROOTS.add(tmp_path)
    return root


def _firmware_observations(root: Path, counters: tuple[int, ...]) -> None:
    store = FirmwareObservationStore(root / "device.json")
    for counter in counters:
        assert store.record("omi", RingInfo(10, 20, 100, counter, RECORD_SIZE))


def _record(timestamp: int) -> bytes:
    payload = bytearray(440)
    return timestamp.to_bytes(4, "big") + bytes(payload)


def _bundle(
    root: Path,
    name: str,
    records: tuple[bytes, ...],
    *,
    start: int = 10,
) -> Path:
    path = root / name
    path.mkdir(parents=True)
    raw = b"".join(records)
    raw_hash = hashlib.sha256(raw).hexdigest()
    (path / "records.bin").write_bytes(raw)
    (path / "manifest.json").write_text(
        json.dumps(
            {
                "device_slug": "omi",
                "start_sequence": start,
                "next_sequence": start + len(records),
                "record_count": len(records),
                "record_size": RECORD_SIZE,
                "raw_sha256": raw_hash,
            }
        ),
        encoding="utf-8",
    )
    receipt = {"attempt_id": "a" * 32, "raw_sha256": raw_hash, "status": "sealed"}
    (path / "receipt.json").write_text(json.dumps(receipt), encoding="utf-8")
    return path


def test_empty_spool_has_zero_raw_metrics(tmp_path: Path) -> None:
    result = collect_spool_metrics(tmp_path, "omi", observation_root=tmp_path / "device.json")

    assert result.as_dict() == {
        "current_window": {
            "bundle_count": 0,
            "downloaded_records": 0,
            "downloaded_raw_bytes": 0,
            "lost_records": 0,
            "lost_raw_bytes": 0,
            "loss_ratio": 0.0,
        },
        "firmware_lifetime": {
            "observation_count": 0,
            "initial": None,
            "latest": None,
            "observed_increase": 0,
            "regression_count": 0,
            "epoch_count": 0,
        },
    }


def test_empty_capture_device_still_reports_spool_firmware_observations(tmp_path: Path) -> None:
    spool = tmp_path / "spool"
    capture_root = _capture_root(tmp_path)
    spool.mkdir()
    capture_root.mkdir()
    _firmware_observations(spool, (4, 9))

    result = collect_spool_metrics(capture_root, "omi", observation_root=spool / "device.json")

    assert result.current_window.bundle_count == 0
    assert result.firmware_lifetime.observation_count == 2
    assert result.firmware_lifetime.initial == 4
    assert result.firmware_lifetime.latest == 9


def test_firmware_observations_are_reported_separately_from_loss(tmp_path: Path) -> None:
    device = tmp_path / "omi"
    device.mkdir()
    _firmware_observations(tmp_path, (4, 9, 12))

    result = collect_spool_metrics(tmp_path, "omi", observation_root=tmp_path / "device.json")

    assert result.firmware_lifetime.observation_count == 3
    assert result.firmware_lifetime.initial == 4
    assert result.firmware_lifetime.latest == 12
    assert result.firmware_lifetime.observed_increase == 8
    assert result.firmware_lifetime.regression_count == 0
    assert result.firmware_lifetime.epoch_count == 1
    assert result.current_window.lost_records == 0
    assert result.current_window.lost_raw_bytes == 0
    assert result.current_window.loss_ratio == 0.0


def test_firmware_counter_reset_starts_a_new_epoch(tmp_path: Path) -> None:
    device = tmp_path / "omi"
    device.mkdir()
    _firmware_observations(tmp_path, (4, 9, 3, 8, 2))

    result = collect_spool_metrics(tmp_path, "omi", observation_root=tmp_path / "device.json")

    assert result.firmware_lifetime.observation_count == 5
    assert result.firmware_lifetime.initial == 4
    assert result.firmware_lifetime.latest == 2
    assert result.firmware_lifetime.observed_increase == 10
    assert result.firmware_lifetime.regression_count == 2
    assert result.firmware_lifetime.epoch_count == 3
    assert result.current_window.lost_records == 0


def test_malformed_firmware_observation_chain_fails_closed(tmp_path: Path) -> None:
    device = tmp_path / "omi"
    device.mkdir()
    _firmware_observations(tmp_path, (4, 9))
    state = tmp_path / "device.json"
    state.write_text(state.read_text(encoding="utf-8") + "\n", encoding="utf-8")

    with pytest.raises(SpoolMetricsError, match="firmware observations are invalid"):
        collect_spool_metrics(tmp_path, "omi", observation_root=tmp_path / "device.json")


def test_sequence_discontinuity_is_aggregated_between_real_bundles(tmp_path: Path) -> None:
    device = tmp_path / "omi"
    device.mkdir()
    _bundle(device, "first", (_record(1), _record(2)))
    _bundle(device, "second", (_record(4), _record(5)), start=13)

    result = collect_spool_metrics(tmp_path, "omi", observation_root=tmp_path / "device.json")

    assert result.current_window.lost_records == 1
    assert result.current_window.lost_raw_bytes == RECORD_SIZE
    assert result.current_window.loss_ratio == 0.2


def test_conflicting_overlapping_ranges_fail_closed(tmp_path: Path) -> None:
    device = tmp_path / "omi"
    device.mkdir()
    _bundle(device, "first", (_record(1), _record(2)))
    _bundle(device, "overlap", (_record(3), _record(4)), start=11)

    with pytest.raises(SpoolMetricsError, match="sequence ranges overlap"):
        collect_spool_metrics(tmp_path, "omi", observation_root=tmp_path / "device.json")


def test_bundle_validation_streams_records_instead_of_reading_all_bytes(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    device = tmp_path / "omi"
    device.mkdir()
    _bundle(device, "first", (_record(1), _record(2)))
    original_read_bytes = Path.read_bytes

    def guarded_read_bytes(path: Path) -> bytes:
        if path.name == "records.bin":
            raise AssertionError("records.bin must be hashed as a bounded stream")
        return original_read_bytes(path)

    monkeypatch.setattr(Path, "read_bytes", guarded_read_bytes)

    result = collect_spool_metrics(tmp_path, "omi", observation_root=tmp_path / "device.json")

    assert result.current_window.downloaded_records == 2


def test_partial_and_symlink_artifacts_are_not_counted(tmp_path: Path) -> None:
    device = tmp_path / "omi"
    device.mkdir()
    (device / "attempts").mkdir()
    target = device / ".real"
    target.mkdir()
    (device / "symlink").symlink_to(target, target_is_directory=True)

    result = collect_spool_metrics(tmp_path, "omi", observation_root=tmp_path / "device.json")

    assert result.current_window.bundle_count == 0


def test_observation_path_is_optional_when_state_file_is_not_available(tmp_path: Path) -> None:
    result = collect_spool_metrics(tmp_path, "omi")

    assert result.firmware_lifetime.observation_count == 0
    assert result.firmware_lifetime.initial is None
    assert result.firmware_lifetime.latest is None


def test_observation_path_rejects_directory_and_symlink(tmp_path: Path) -> None:
    observation_directory = tmp_path / "collector"
    observation_directory.mkdir()
    with pytest.raises(SpoolMetricsError, match="firmware observations are invalid"):
        collect_spool_metrics(tmp_path, "omi", observation_root=observation_directory)

    target = tmp_path / "state-target.json"
    target.write_text("{}", encoding="utf-8")
    observation_symlink = tmp_path / "device.json"
    observation_symlink.symlink_to(target)
    with pytest.raises(SpoolMetricsError, match="firmware observations are invalid"):
        collect_spool_metrics(tmp_path, "omi", observation_root=observation_symlink)
