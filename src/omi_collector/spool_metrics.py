"""Derive aggregate raw-spool accounting from sealed collector bundles."""

from __future__ import annotations

import stat
from dataclasses import dataclass
from pathlib import Path
from typing import TypeGuard, cast

from omi_collector.capture.adapters import ready_bundles
from omi_collector.capture.adapters.collector_state_db import StateDB, StateDBError
from omi_collector.capture.adapters.firmware_observations import (
    FirmwareObservation,
    FirmwareObservationError,
    read_firmware_observations,
)
from omi_collector.capture.adapters.ready_bundles import ReadyBundleError
from omi_collector.capture.domain.ring_protocol import RECORD_SIZE


class SpoolMetricsError(ValueError):
    """The requested spool contains malformed authoritative evidence."""


@dataclass(frozen=True, slots=True)
class SpoolWindowMetrics:
    """Metrics derived from one explicit set of validated visible bundles."""

    bundle_count: int
    downloaded_records: int
    downloaded_raw_bytes: int
    lost_records: int
    lost_raw_bytes: int
    loss_ratio: float

    def as_dict(self) -> dict[str, object]:
        """Return collection metrics for one named scope."""
        return {
            "bundle_count": self.bundle_count,
            "downloaded_records": self.downloaded_records,
            "downloaded_raw_bytes": self.downloaded_raw_bytes,
            "lost_records": self.lost_records,
            "lost_raw_bytes": self.lost_raw_bytes,
            "loss_ratio": self.loss_ratio,
        }


@dataclass(frozen=True, slots=True)
class FirmwareLifetimeMetrics:
    """Cumulative firmware-counter observations from collector state."""

    observation_count: int
    initial: int | None
    latest: int | None
    observed_increase: int
    regression_count: int
    epoch_count: int

    def as_dict(self) -> dict[str, object]:
        return {
            "observation_count": self.observation_count,
            "initial": self.initial,
            "latest": self.latest,
            "observed_increase": self.observed_increase,
            "regression_count": self.regression_count,
            "epoch_count": self.epoch_count,
        }


@dataclass(frozen=True, slots=True)
class SpoolMetrics:
    """Metrics for raw bundles currently visible to the collector."""

    current_window: SpoolWindowMetrics
    firmware_lifetime: FirmwareLifetimeMetrics

    def as_dict(self) -> dict[str, object]:
        """Return the explicit scoped aggregate JSON contract."""
        return {
            "current_window": self.current_window.as_dict(),
            "firmware_lifetime": self.firmware_lifetime.as_dict(),
        }


@dataclass(frozen=True, slots=True)
class _BundleMeasurement:
    identity: tuple[object, ...]
    records: int
    raw_bytes: int
    ranges: tuple[tuple[int, int], ...]


def collect_spool_metrics(
    publication_root: Path,
    *,
    state_db: StateDB | None = None,
) -> SpoolMetrics:
    """Report visible bundle metadata without reading raw audio payloads."""
    _require_directory(publication_root, "publication root")
    ledger_entries = _ready_ledger_entries(state_db) if state_db is not None else None
    try:
        observations = read_firmware_observations(state_db) if state_db is not None else ()
    except (FirmwareObservationError, StateDBError) as error:
        raise SpoolMetricsError(f"firmware observations are invalid: {error}") from error
    measurements = _current_measurements(publication_root, ledger_entries)
    current = _aggregate_window(measurements)
    return SpoolMetrics(current, FirmwareLifetimeMetrics(*_firmware_metrics(observations)))


def _ready_ledger_entries(state_db: StateDB) -> dict[str, object]:
    try:
        ledger = ready_bundles._read_ledger(state_db)
    except (ReadyBundleError, StateDBError) as error:
        raise SpoolMetricsError(f"ready publication ledger is invalid: {error}") from error
    return cast(dict[str, object], ledger["bundles"])


def _current_measurements(
    publication_root: Path, ledger_entries: dict[str, object] | None
) -> tuple[_BundleMeasurement, ...]:
    return _read_artifacts(publication_root, ledger_entries)


def _read_artifacts(root: Path, ledger_entries: dict[str, object] | None) -> tuple[_BundleMeasurement, ...]:
    measurements: list[_BundleMeasurement] = []
    for entry in _entries(root, "ready publication"):
        if entry.name.startswith("."):
            continue
        ledger_entry = ledger_entries.get(entry.name) if ledger_entries is not None else None
        if _is_v2_ledger_entry(ledger_entry):
            if entry.is_symlink() or not entry.is_dir():
                raise SpoolMetricsError(f"v2 ready artifact is not a real directory: {entry}")
            measurements.append(_read_v2_artifact(entry, ledger_entry))
            continue
        raise SpoolMetricsError(f"visible ready entry has no omi-audio-v2 publication ledger entry: {entry}")
    return tuple(measurements)


def _is_v2_ledger_entry(value: object) -> TypeGuard[dict[str, object]]:
    return isinstance(value, dict) and value.get("format") == "omi-audio-v2"


def _read_v2_artifact(path: Path, entry: dict[str, object]) -> _BundleMeasurement:
    members = _entries(path, "v2 ready artifact")
    if {member.name for member in members} != {"audio.ogg", "manifest.json"}:
        raise SpoolMetricsError(f"v2 ready artifact inventory is invalid: {path}")
    audio_size = _require_regular_file(path / "audio.ogg", "audio.ogg")
    manifest_size = _require_regular_file(path / "manifest.json", "manifest.json")
    if audio_size != entry["audio_bytes"] or manifest_size != entry["manifest_bytes"]:
        raise SpoolMetricsError(f"v2 ready artifact sizes conflict with publication ledger: {path}")
    facts = ready_bundles._entry_facts(entry)
    return _BundleMeasurement(
        ("ready", path.name),
        facts.record_count,
        facts.record_count * RECORD_SIZE,
        ready_bundles._physical_runs(facts),
    )


def _aggregate_window(measurements: tuple[_BundleMeasurement, ...]) -> SpoolWindowMetrics:
    values = _unique_measurements(measurements)
    _reject_overlapping_ranges(values)
    downloaded_records = sum(value.records for value in values)
    downloaded_raw_bytes = sum(value.raw_bytes for value in values)
    unproven_records = _unproven_hole_records(values)
    denominator = downloaded_records + unproven_records
    return SpoolWindowMetrics(
        len(values),
        downloaded_records,
        downloaded_raw_bytes,
        unproven_records,
        unproven_records * RECORD_SIZE,
        unproven_records / denominator if denominator else 0.0,
    )


def _unique_measurements(measurements: tuple[_BundleMeasurement, ...]) -> tuple[_BundleMeasurement, ...]:
    unique: dict[tuple[object, ...], _BundleMeasurement] = {}
    for measurement in measurements:
        unique.setdefault(measurement.identity, measurement)
    return tuple(unique.values())


def _firmware_metrics(
    observations: tuple[FirmwareObservation, ...],
) -> tuple[int, int | None, int | None, int, int, int]:
    if not observations:
        return 0, None, None, 0, 0, 0
    state = observations[0]
    return (
        state.observation_count,
        state.initial,
        state.dropped_packets,
        state.observed_increase,
        state.regression_count,
        state.epoch_count,
    )


def _reject_overlapping_ranges(values: tuple[_BundleMeasurement, ...]) -> None:
    ranges = sorted((start, end, value.identity) for value in values for start, end in value.ranges)
    previous_end = -1
    previous_identity: tuple[object, ...] | None = None
    for start, end, identity in ranges:
        if start < previous_end and identity != previous_identity:
            raise SpoolMetricsError("authoritative artifact sequence ranges overlap")
        if end > previous_end:
            previous_end = end
            previous_identity = identity


def _unproven_hole_records(values: tuple[_BundleMeasurement, ...]) -> int:
    ranges = sorted((start, end) for value in values for start, end in value.ranges)
    if not ranges:
        return 0
    previous_end = ranges[0][1]
    holes = 0
    for start, end in ranges[1:]:
        holes += max(0, start - previous_end)
        previous_end = max(previous_end, end)
    return holes


def _entries(path: Path, label: str) -> tuple[Path, ...]:
    try:
        return tuple(path.iterdir())
    except OSError as error:
        raise SpoolMetricsError(f"cannot inspect {label}: {path}") from error


def _require_directory(path: Path, label: str) -> None:
    try:
        mode = path.lstat().st_mode
    except OSError as error:
        raise SpoolMetricsError(f"{label} is missing or unreadable: {path}") from error
    if stat.S_ISLNK(mode) or not stat.S_ISDIR(mode):
        raise SpoolMetricsError(f"{label} must be a regular directory: {path}")


def _require_regular_file(path: Path, label: str) -> int:
    try:
        info = path.lstat()
    except OSError as error:
        raise SpoolMetricsError(f"{label} is missing or unreadable: {path}") from error
    if stat.S_ISLNK(info.st_mode) or not stat.S_ISREG(info.st_mode):
        raise SpoolMetricsError(f"{label} must be a regular file: {path}")
    return info.st_size
