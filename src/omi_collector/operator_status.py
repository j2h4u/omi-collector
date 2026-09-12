"""Read-only operator summary from bounded collector evidence."""

from __future__ import annotations

import json
import re
import stat
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import cast

from omi_collector.capture.adapters.firmware_observations import FirmwareObservation, read_firmware_observations
from omi_collector.capture.domain.ring_protocol import RECORD_SIZE
from omi_collector.config import DEFAULT_CONFIG
from omi_collector.spool_metrics import collect_spool_metrics
from omi_collector.storage_layout import StorageLayout

_SCHEMA_VERSION = 1
_DEVICE_SLUG = re.compile(r"[A-Za-z0-9_-]+\Z")
_SOURCE_REVISION = re.compile(r"[0-9a-f]{12}\Z")
_TERMINATION_CLASSES = frozenset({"cancelled", "completed", "fatal_error", "retryable_error", "teardown_interrupted"})
_ADVERTISEMENT_FIELDS = frozenset(
    {
        "schema_version",
        "event",
        "recorded_at",
        "session_id",
        "device_slug",
        "advertisement_rssi_dbm",
        "release_version",
        "source_revision",
        "phy_policy",
    }
)
_TRANSFER_FIELDS = frozenset(
    {
        "schema_version",
        "event",
        "completed_at",
        "session_id",
        "device_slug",
        "outcome",
        "termination_class",
        "active_read_elapsed_ms",
        "requested_record_count",
        "record_size_bytes",
        "received_raw_bytes",
        "submitted_raw_bytes",
        "written_raw_bytes",
        "release_version",
        "source_revision",
        "firmware_version",
        "phy_policy",
        "advertisement_rssi_dbm",
    }
)
_LOSS_FIELDS = frozenset(
    {
        "schema_version",
        "event",
        "occurred_at",
        "session_id",
        "device_slug",
        "missing_record_count",
        "missing_raw_bytes",
        "reason",
        "release_version",
        "source_revision",
        "firmware_version",
    }
)


class OperatorStatusError(ValueError):
    """Bounded observability evidence is malformed or unsafe."""


@dataclass(frozen=True, slots=True)
class _QualityWindow:
    advertisements: int = 0
    transfer_sessions: int = 0
    completed_transfers: int = 0
    retryable_transfers: int = 0
    written_raw_bytes: int = 0
    active_read_elapsed_ms: int = 0
    loss_events: int = 0
    missing_records: int = 0
    missing_raw_bytes: int = 0
    last_advertisement_at: str | None = None
    last_advertisement_rssi_dbm: int | None = None
    last_transfer_at: str | None = None
    last_successful_transfer_at: str | None = None
    last_transfer_outcome: str | None = None
    last_transfer_termination_class: str | None = None
    transfer_outcomes: dict[str, int] = field(default_factory=dict)
    transfer_termination_classes: dict[str, int] = field(default_factory=dict)

    def as_dict(self) -> dict[str, object]:
        throughput = (
            self.written_raw_bytes * 1000 / self.active_read_elapsed_ms if self.active_read_elapsed_ms else None
        )
        return {
            "advertisements": self.advertisements,
            "completed_transfers": self.completed_transfers,
            "confirmed_loss_events": self.loss_events,
            "confirmed_lost_raw_bytes": self.missing_raw_bytes,
            "confirmed_lost_records": self.missing_records,
            "last_advertisement_at": self.last_advertisement_at,
            "last_advertisement_rssi_dbm": self.last_advertisement_rssi_dbm,
            "last_successful_transfer_at": self.last_successful_transfer_at,
            "last_transfer_at": self.last_transfer_at,
            "last_transfer_outcome": self.last_transfer_outcome,
            "last_transfer_termination_class": self.last_transfer_termination_class,
            "pooled_written_bytes_per_second": throughput,
            "retryable_transfers": self.retryable_transfers,
            "transfer_outcomes": self.transfer_outcomes,
            "transfer_sessions": self.transfer_sessions,
            "transfer_termination_classes": self.transfer_termination_classes,
            "written_raw_bytes": self.written_raw_bytes,
        }


@dataclass(slots=True)
class _QualityAccumulator:
    advertisements: int = 0
    transfer_sessions: int = 0
    completed_transfers: int = 0
    retryable_transfers: int = 0
    written_raw_bytes: int = 0
    active_read_elapsed_ms: int = 0
    loss_events: int = 0
    missing_records: int = 0
    missing_raw_bytes: int = 0
    last_advertisement: tuple[datetime, int] | None = None
    last_transfer: datetime | None = None
    last_success: datetime | None = None
    last_transfer_outcome: str | None = None
    last_transfer_termination_class: str | None = None
    transfer_outcomes: dict[str, int] = field(default_factory=dict)
    transfer_termination_classes: dict[str, int] = field(default_factory=dict)

    def build(self) -> _QualityWindow:
        return _QualityWindow(
            advertisements=self.advertisements,
            transfer_sessions=self.transfer_sessions,
            completed_transfers=self.completed_transfers,
            retryable_transfers=self.retryable_transfers,
            written_raw_bytes=self.written_raw_bytes,
            active_read_elapsed_ms=self.active_read_elapsed_ms,
            loss_events=self.loss_events,
            missing_records=self.missing_records,
            missing_raw_bytes=self.missing_raw_bytes,
            last_advertisement_at=_iso(self.last_advertisement[0]) if self.last_advertisement else None,
            last_advertisement_rssi_dbm=self.last_advertisement[1] if self.last_advertisement else None,
            last_transfer_at=_iso(self.last_transfer),
            last_successful_transfer_at=_iso(self.last_success),
            last_transfer_outcome=self.last_transfer_outcome,
            last_transfer_termination_class=self.last_transfer_termination_class,
            transfer_outcomes=dict(self.transfer_outcomes),
            transfer_termination_classes=dict(self.transfer_termination_classes),
        )


@dataclass(frozen=True, slots=True)
class _QualityEvent:
    event: str
    timestamp: datetime
    device_slug: str
    advertisement_rssi_dbm: int | None = None
    outcome: str | None = None
    termination_class: str | None = None
    written_raw_bytes: int = 0
    active_read_elapsed_ms: int = 0
    missing_record_count: int = 0
    missing_raw_bytes: int = 0


def collect_operator_status(
    layout: StorageLayout,
    device_slug: str,
    *,
    hours: int,
    now: datetime | None = None,
) -> dict[str, object]:
    """Summarize current device state, publication evidence, and recent quality events."""
    if isinstance(hours, bool) or not isinstance(hours, int) or hours <= 0:
        raise OperatorStatusError("hours must be positive")
    end = now or datetime.now(UTC)
    if end.tzinfo is None:
        raise OperatorStatusError("status clock must be timezone-aware")
    start = end.astimezone(UTC) - timedelta(hours=hours)
    try:
        observations = read_firmware_observations(layout.collector.device_state, device_slug)
        spool = collect_spool_metrics(
            layout.publication.root,
            device_slug,
            observation_root=layout.collector.device_state,
        )
        quality = _quality_window(layout.collector.root, device_slug, start, end.astimezone(UTC))
        runtime = _runtime_status(layout.collector.debug_log, end.astimezone(UTC))
    except ValueError as error:
        raise OperatorStatusError(str(error)) from error
    device = _device_status(observations[0]) if observations else None
    window_status = _window_status(quality)
    return {
        "device": device,
        "generated_at": end.astimezone(UTC).isoformat(timespec="seconds"),
        "publication": spool.current_window.as_dict(),
        "quality_window": quality.as_dict(),
        "runtime": runtime,
        "schema_version": 2,
        "status": window_status,
        "window_hours": hours,
    }


def _runtime_status(path: Path, now: datetime) -> dict[str, object]:
    latest: tuple[datetime, dict[str, object]] | None = None
    observation: tuple[datetime, dict[str, object]] | None = None
    error: tuple[datetime, dict[str, object]] | None = None
    if _path_exists(path, "debug journal"):
        for row in _debug_rows(path):
            decoded = _decode_sync_progress(row)
            if decoded is None:
                continue
            timestamp, progress = decoded
            if latest is None or timestamp > latest[0]:
                latest = decoded
            if progress.get("event") == "pendant_observation" and (observation is None or timestamp > observation[0]):
                observation = decoded
            if progress.get("status") == "session_error" and (error is None or timestamp > error[0]):
                error = decoded
    current = latest[1] if latest else {}
    observed = observation[1] if observation else {}
    state = current.get("status", "unknown")
    active = state == "progress"
    return {
        "battery_percent": observed.get("battery_percent"),
        "battery_observed_at": _iso(observation[0]) if observation else None,
        "firmware": observed.get("firmware"),
        "last_error": _runtime_error(error),
        "state": "transferring" if active else state,
        "updated_at": _iso(latest[0]) if latest else None,
        "updated_age_seconds": max(0, int((now - latest[0]).total_seconds())) if latest else None,
        "transfer": _active_transfer(current) if active else None,
    }


def _runtime_error(error: tuple[datetime, dict[str, object]] | None) -> dict[str, object] | None:
    if error is None:
        return None
    timestamp, progress = error
    return {
        "error_message": progress.get("error_message"),
        "error_type": progress.get("error_type"),
        "occurred_at": _iso(timestamp),
        "phase": progress.get("phase"),
    }


def _debug_rows(path: Path) -> list[dict[str, object]]:
    config = DEFAULT_CONFIG.observability.debug_log
    _require_regular_file(path, max_bytes=config.max_bytes, label="debug journal")
    try:
        payload = path.read_bytes()
    except OSError as error:
        raise OperatorStatusError(f"debug journal is unreadable: {path.name}") from error
    rows: list[dict[str, object]] = []
    for line in payload.splitlines():
        if len(line) > config.max_record_bytes:
            raise OperatorStatusError(f"debug journal record exceeds configured size: {path.name}")
        try:
            value = cast(object, json.loads(line))
        except (json.JSONDecodeError, UnicodeDecodeError) as error:
            raise OperatorStatusError(f"debug journal contains malformed JSON: {path.name}") from error
        if not isinstance(value, dict):
            raise OperatorStatusError(f"debug journal record is not an object: {path.name}")
        rows.append(cast(dict[str, object], value))
    return rows


def _decode_sync_progress(row: dict[str, object]) -> tuple[datetime, dict[str, object]] | None:
    if row.get("event") != "sync_progress":
        return None
    timestamp = _timestamp(row, "timestamp")
    fields = row.get("fields")
    if not isinstance(fields, dict):
        raise OperatorStatusError("debug sync progress has invalid fields")
    progress = fields.get("progress")
    if not isinstance(progress, dict):
        raise OperatorStatusError("debug sync progress has invalid progress")
    return timestamp, cast(dict[str, object], progress)


def _active_transfer(progress: dict[str, object]) -> dict[str, object]:
    completed = progress.get("records_completed")
    total = progress.get("records_total")
    fraction = completed / total if isinstance(completed, int) and isinstance(total, int) and total > 0 else None
    keys = (
        "bytes_per_second",
        "eta_seconds",
        "payload_bytes",
        "records_completed",
        "records_per_second",
        "records_total",
        "remaining_bytes",
        "remaining_packets",
        "total_bytes",
    )
    return {"fraction_complete": fraction, **{key: progress.get(key) for key in keys}}


def _device_status(observation: FirmwareObservation) -> dict[str, object]:
    read_sequence = observation.read_sequence
    write_sequence = observation.write_sequence
    return {
        "capacity_packets": observation.capacity_packets,
        "dropped_packets": observation.dropped_packets,
        "read_sequence": read_sequence,
        "unread_bytes": (write_sequence - read_sequence) * observation.packet_size,
        "unread_packets": write_sequence - read_sequence,
        "write_sequence": write_sequence,
    }


def _window_status(quality: _QualityWindow) -> str:
    if quality.loss_events:
        return "attention"
    if quality.last_transfer_termination_class in {"cancelled", "fatal_error", "teardown_interrupted"}:
        return "attention"
    return "ok" if quality.completed_transfers else "unknown"


def _quality_window(root: Path, device_slug: str, start: datetime, end: datetime) -> _QualityWindow:
    accumulator = _QualityAccumulator()
    for row in _quality_rows(root):
        event = _decode_quality_event(row)
        _accumulate_quality_event(accumulator, event, device_slug, start, end)
    return accumulator.build()


def _accumulate_quality_event(
    accumulator: _QualityAccumulator, event: _QualityEvent, device_slug: str, start: datetime, end: datetime
) -> None:
    if event.device_slug != device_slug or not start <= event.timestamp <= end:
        return
    if event.event == "advertisement_observation":
        _accumulate_advertisement(accumulator, event)
    elif event.event == "transfer_session":
        _accumulate_transfer(accumulator, event)
    else:
        _accumulate_loss(accumulator, event)


def _accumulate_advertisement(accumulator: _QualityAccumulator, event: _QualityEvent) -> None:
    rssi = event.advertisement_rssi_dbm
    if rssi is None:
        raise OperatorStatusError("advertisement event has invalid advertisement_rssi_dbm")
    accumulator.advertisements += 1
    if accumulator.last_advertisement is None or event.timestamp > accumulator.last_advertisement[0]:
        accumulator.last_advertisement = (event.timestamp, rssi)


def _accumulate_transfer(accumulator: _QualityAccumulator, event: _QualityEvent) -> None:
    termination_class = event.termination_class
    outcome = event.outcome
    if termination_class is None or outcome is None:
        raise OperatorStatusError("transfer event is incomplete")
    accumulator.transfer_sessions += 1
    accumulator.written_raw_bytes += event.written_raw_bytes
    accumulator.active_read_elapsed_ms += event.active_read_elapsed_ms
    accumulator.transfer_outcomes[outcome] = accumulator.transfer_outcomes.get(outcome, 0) + 1
    accumulator.transfer_termination_classes[termination_class] = (
        accumulator.transfer_termination_classes.get(termination_class, 0) + 1
    )
    if accumulator.last_transfer is None or event.timestamp > accumulator.last_transfer:
        accumulator.last_transfer = event.timestamp
        accumulator.last_transfer_outcome = outcome
        accumulator.last_transfer_termination_class = termination_class
    if termination_class == "completed":
        accumulator.completed_transfers += 1
        accumulator.last_success = (
            max(event.timestamp, accumulator.last_success) if accumulator.last_success else event.timestamp
        )
    elif termination_class == "retryable_error":
        accumulator.retryable_transfers += 1


def _accumulate_loss(accumulator: _QualityAccumulator, event: _QualityEvent) -> None:
    accumulator.loss_events += 1
    accumulator.missing_records += event.missing_record_count
    accumulator.missing_raw_bytes += event.missing_raw_bytes


def _quality_rows(root: Path) -> list[dict[str, object]]:
    if not _path_exists(root, "collector root"):
        return []
    _require_regular_directory(root)
    config = DEFAULT_CONFIG.observability.quality_metrics
    paths = [root / f"{config.file_name}.{index}" for index in range(config.backup_count, 0, -1)]
    paths.append(root / config.file_name)
    rows: list[dict[str, object]] = []
    for path in paths:
        if not _path_exists(path, "quality journal"):
            continue
        rows.extend(_read_quality_rows(path, config.max_bytes, config.max_record_bytes))
    return rows


def _read_quality_rows(path: Path, max_bytes: int, max_record_bytes: int) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for line in _quality_lines(path, max_bytes):
        if len(line) > max_record_bytes:
            raise OperatorStatusError(f"quality journal record exceeds configured size: {path.name}")
        rows.append(_decode_quality_row(line, path))
    return rows


def _quality_lines(path: Path, max_bytes: int) -> list[bytes]:
    _require_regular_file(path, max_bytes=max_bytes)
    try:
        payload = path.read_bytes()
    except OSError as error:
        raise OperatorStatusError(f"quality journal is unreadable: {path.name}") from error
    if len(payload) > max_bytes:
        raise OperatorStatusError(f"quality journal exceeds configured size: {path.name}")
    return payload.splitlines()


def _decode_quality_row(line: bytes, path: Path) -> dict[str, object]:
    try:
        value = cast(object, json.loads(line))
    except (json.JSONDecodeError, UnicodeDecodeError) as error:
        raise OperatorStatusError(f"quality journal contains malformed JSON: {path.name}") from error
    if not isinstance(value, dict):
        raise OperatorStatusError(f"quality journal record is not an object: {path.name}")
    return cast(dict[str, object], value)


def _path_exists(path: Path, label: str) -> bool:
    try:
        path.lstat()
    except FileNotFoundError:
        return False
    except OSError as error:
        raise OperatorStatusError(f"{label} is unavailable: {path.name}") from error
    return True


def _require_regular_directory(path: Path) -> None:
    try:
        mode = path.lstat().st_mode
    except OSError as error:
        raise OperatorStatusError("collector root is unavailable") from error
    if stat.S_ISLNK(mode) or not stat.S_ISDIR(mode):
        raise OperatorStatusError("collector root must be a regular directory")


def _require_regular_file(path: Path, *, max_bytes: int, label: str = "quality journal") -> None:
    try:
        state = path.lstat()
    except OSError as error:
        raise OperatorStatusError(f"{label} is unavailable: {path.name}") from error
    if not stat.S_ISREG(state.st_mode):
        raise OperatorStatusError(f"{label} must be a regular file: {path.name}")
    if state.st_size > max_bytes:
        raise OperatorStatusError(f"{label} exceeds configured size: {path.name}")


def _decode_quality_event(row: dict[str, object]) -> _QualityEvent:
    event = row.get("event")
    if event == "advertisement_observation":
        return _decode_advertisement(row)
    if event == "transfer_session":
        return _decode_transfer(row)
    if event == "sequence_loss":
        return _decode_loss(row)
    raise OperatorStatusError("quality journal contains unsupported event")


def _decode_advertisement(row: dict[str, object]) -> _QualityEvent:
    timestamp, device_slug = _event_header(row, _ADVERTISEMENT_FIELDS, "recorded_at")
    _require_string(row, "session_id")
    _require_string(row, "release_version")
    _require_string(row, "phy_policy")
    _source_revision(row)
    return _QualityEvent(
        "advertisement_observation",
        timestamp,
        device_slug,
        advertisement_rssi_dbm=_integer(row, "advertisement_rssi_dbm"),
    )


def _decode_transfer(row: dict[str, object]) -> _QualityEvent:
    timestamp, device_slug = _event_header(row, _TRANSFER_FIELDS, "completed_at")
    _require_string(row, "session_id")
    outcome = _require_string(row, "outcome")
    termination_class = _termination_class(row)
    _require_string(row, "release_version")
    _require_string(row, "phy_policy")
    _source_revision(row)
    _nullable_string(row, "firmware_version")
    _optional_integer(row, "advertisement_rssi_dbm")
    _integer(row, "requested_record_count", minimum=0)
    _integer(row, "received_raw_bytes", minimum=0)
    _integer(row, "submitted_raw_bytes", minimum=0)
    if _integer(row, "record_size_bytes", minimum=1) != RECORD_SIZE:
        raise OperatorStatusError("transfer event has invalid record_size_bytes")
    return _QualityEvent(
        "transfer_session",
        timestamp,
        device_slug,
        outcome=outcome,
        termination_class=termination_class,
        written_raw_bytes=_integer(row, "written_raw_bytes", minimum=0),
        active_read_elapsed_ms=_integer(row, "active_read_elapsed_ms", minimum=0),
    )


def _decode_loss(row: dict[str, object]) -> _QualityEvent:
    timestamp, device_slug = _event_header(row, _LOSS_FIELDS, "occurred_at")
    _require_string(row, "session_id")
    _require_string(row, "reason")
    _require_string(row, "release_version")
    _source_revision(row)
    _nullable_string(row, "firmware_version")
    return _QualityEvent(
        "sequence_loss",
        timestamp,
        device_slug,
        missing_record_count=_integer(row, "missing_record_count", minimum=0),
        missing_raw_bytes=_integer(row, "missing_raw_bytes", minimum=0),
    )


def _event_header(row: dict[str, object], fields: frozenset[str], timestamp_key: str) -> tuple[datetime, str]:
    if set(row) != fields:
        raise OperatorStatusError("quality journal event fields are invalid")
    schema_version = row.get("schema_version")
    if isinstance(schema_version, bool) or not isinstance(schema_version, int) or schema_version != _SCHEMA_VERSION:
        raise OperatorStatusError("quality journal event schema version is invalid")
    device_slug = _require_string(row, "device_slug")
    if _DEVICE_SLUG.fullmatch(device_slug) is None:
        raise OperatorStatusError("quality journal event has invalid device_slug")
    return _timestamp(row, timestamp_key), device_slug


def _timestamp(row: dict[str, object], key: str) -> datetime:
    raw = _require_string(row, key)
    try:
        parsed = datetime.fromisoformat(raw)
    except ValueError as error:
        raise OperatorStatusError(f"quality journal event has invalid {key}") from error
    if parsed.tzinfo is None:
        raise OperatorStatusError(f"quality journal event has naive {key}")
    return parsed.astimezone(UTC)


def _require_string(row: dict[str, object], key: str) -> str:
    value = row.get(key)
    if not isinstance(value, str) or not value:
        raise OperatorStatusError(f"quality journal event has invalid {key}")
    return value


def _nullable_string(row: dict[str, object], key: str) -> str | None:
    value = row.get(key)
    if value is None:
        return None
    if not isinstance(value, str) or not value:
        raise OperatorStatusError(f"quality journal event has invalid {key}")
    return value


def _integer(row: dict[str, object], key: str, *, minimum: int | None = None) -> int:
    value = row.get(key)
    if isinstance(value, bool) or not isinstance(value, int) or (minimum is not None and value < minimum):
        raise OperatorStatusError(f"quality journal event has invalid {key}")
    return value


def _optional_integer(row: dict[str, object], key: str) -> int | None:
    value = row.get(key)
    if value is None:
        return None
    return _integer(row, key)


def _source_revision(row: dict[str, object]) -> str | None:
    value = row.get("source_revision")
    if value is None:
        return None
    if not isinstance(value, str) or _SOURCE_REVISION.fullmatch(value) is None:
        raise OperatorStatusError("quality journal event has invalid source_revision")
    return value


def _termination_class(row: dict[str, object]) -> str:
    value = row.get("termination_class")
    if value not in _TERMINATION_CLASSES:
        raise OperatorStatusError("quality journal event has invalid termination_class")
    return cast(str, value)


def _iso(value: datetime | None) -> str | None:
    return value.astimezone(UTC).isoformat(timespec="milliseconds") if value else None
