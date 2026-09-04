"""Derive aggregate raw-spool accounting from sealed collector bundles."""

from __future__ import annotations

import json
import re
import stat
from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path
from typing import cast

from omi_collector.capture.adapters.bundle_contract import BundleManifest, SealedReceipt
from omi_collector.capture.adapters.firmware_observations import (
    FirmwareObservation,
    FirmwareObservationError,
    read_firmware_observations,
)
from omi_collector.capture.domain.ring_protocol import RECORD_SIZE
from omi_collector.config import DEFAULT_CONFIG

_SLUG = re.compile(r"[A-Za-z0-9_-]+")


class SpoolMetricsError(ValueError):
    """The requested spool contains malformed authoritative evidence."""


@dataclass(frozen=True, slots=True)
class SpoolWindowMetrics:
    """Metrics derived from one explicit set of authenticated bundles."""

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
    capture_root: Path,
    device_slug: str,
    *,
    observation_root: Path | None = None,
) -> SpoolMetrics:
    """Report only authenticated raw bundles visible in the capture root."""
    _validate_slug(device_slug)
    _require_directory(capture_root, "capture root")
    try:
        observations = read_firmware_observations(observation_root, device_slug) if observation_root is not None else ()
    except FirmwareObservationError as error:
        raise SpoolMetricsError(f"firmware observations are invalid: {error}") from error
    measurements = _current_measurements(capture_root, device_slug)
    current = _aggregate_window(measurements)
    return SpoolMetrics(current, FirmwareLifetimeMetrics(*_firmware_metrics(observations)))


def _current_measurements(capture_root: Path, device_slug: str) -> tuple[_BundleMeasurement, ...]:
    device_root = capture_root / device_slug
    if not device_root.exists() and not device_root.is_symlink():
        return ()
    _require_directory(device_root, "device spool")
    return _read_artifacts(device_root, device_slug)


def _read_artifacts(device_root: Path, device_slug: str) -> tuple[_BundleMeasurement, ...]:
    measurements: list[_BundleMeasurement] = []
    for entry in _entries(device_root, "device spool"):
        if entry.name.startswith(".") or entry.name == "incidents":
            # Retired gap-only incident directories are intentionally opaque.
            continue
        if entry.is_symlink():
            continue
        if not entry.is_dir():
            raise SpoolMetricsError(f"visible spool entry is not an artifact directory: {entry}")
        measurement = _read_artifact(entry, device_slug)
        if measurement is not None:
            measurements.append(measurement)
    return tuple(measurements)


def _read_artifact(path: Path, device_slug: str) -> _BundleMeasurement | None:
    # A directory is a participant only when it has a real-bundle manifest;
    # gap-only directories therefore disappear without being inspected.
    manifest_path = path / "manifest.json"
    if not _regular_file(manifest_path):
        return None
    return _read_bundle_artifact(path, device_slug)


def _read_bundle_artifact(path: Path, device_slug: str) -> _BundleMeasurement:
    manifest = _read_manifest_and_raw(path, device_slug)
    _validate_receipt(path, manifest.raw_sha256)
    return _BundleMeasurement(
        (
            "bundle",
            manifest.device_slug,
            manifest.start_sequence,
            manifest.next_sequence,
            manifest.raw_sha256,
        ),
        manifest.record_count,
        manifest.record_count * manifest.record_size,
        ((manifest.start_sequence, manifest.next_sequence),),
    )


def _read_manifest_and_raw(path: Path, device_slug: str) -> BundleManifest:
    manifest = _read_json(path / "manifest.json", "manifest.json")
    try:
        parsed = BundleManifest.from_json(manifest)
    except ValueError as error:
        raise SpoolMetricsError(f"manifest is invalid or has the wrong device_slug: {path}") from error
    if parsed.device_slug != device_slug:
        raise SpoolMetricsError(f"manifest is invalid or has the wrong device_slug: {path}")
    raw_path = path / "records.bin"
    _require_regular_file(raw_path, "records.bin")
    raw_size, calculated_hash = _stream_size_and_hash(raw_path)
    if raw_size != parsed.record_count * RECORD_SIZE or calculated_hash != parsed.raw_sha256:
        raise SpoolMetricsError(f"records.bin does not match manifest: {path}")
    return parsed


def _stream_size_and_hash(path: Path) -> tuple[int, str]:
    digest = sha256()
    size = 0
    try:
        with path.open("rb") as raw:
            while chunk := raw.read(DEFAULT_CONFIG.durability.io_chunk_bytes):
                size += len(chunk)
                digest.update(chunk)
    except OSError as error:
        raise SpoolMetricsError(f"records.bin is unreadable: {path.parent}") from error
    return size, digest.hexdigest()


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


def _validate_receipt(path: Path, raw_hash: str) -> None:
    receipt = _read_json(path / "receipt.json", "receipt.json")
    try:
        parsed = SealedReceipt.from_json(receipt)
    except ValueError as error:
        raise SpoolMetricsError(f"sealed receipt fields are invalid: {path}") from error
    if parsed.raw_sha256 != raw_hash:
        raise SpoolMetricsError(f"sealed receipt does not authenticate artifact: {path}")


def _read_json(path: Path, label: str) -> dict[str, object]:
    _require_regular_file(path, label)
    try:
        value = cast(object, json.loads(path.read_text(encoding="utf-8")))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise SpoolMetricsError(f"{label} is invalid JSON: {path}") from error
    if not isinstance(value, dict):
        raise SpoolMetricsError(f"{label} must be a JSON object: {path}")
    return cast(dict[str, object], value)


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


def _require_regular_file(path: Path, label: str) -> None:
    try:
        mode = path.lstat().st_mode
    except OSError as error:
        raise SpoolMetricsError(f"{label} is missing or unreadable: {path}") from error
    if stat.S_ISLNK(mode) or not stat.S_ISREG(mode):
        raise SpoolMetricsError(f"{label} must be a regular file: {path}")


def _regular_file(path: Path) -> bool:
    try:
        mode = path.lstat().st_mode
    except OSError:
        return False
    return stat.S_ISREG(mode)


def _validate_slug(value: str) -> None:
    if _SLUG.fullmatch(value) is None:
        raise SpoolMetricsError("device slug is invalid")
