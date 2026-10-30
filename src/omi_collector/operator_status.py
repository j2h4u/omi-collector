"""Read-only operator summary from bounded collector evidence."""

from __future__ import annotations

import json
import os
import sqlite3
import stat
from collections.abc import Iterable
from contextlib import closing
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import cast

from omi_collector.capture.adapters.ble_scan_health_store import read_ble_scan_incident
from omi_collector.capture.adapters.collector_state_db import (
    StateDB,
    StateDBError,
    StateKind,
    raise_if_sqlite_storage_fault,
)
from omi_collector.capture.adapters.confirmed_loss import ConfirmedLossError, ConfirmedLossFact, read_confirmed_losses
from omi_collector.capture.adapters.debug_logging import read_runtime_status
from omi_collector.capture.adapters.firmware_observations import FirmwareObservation, read_firmware_observations
from omi_collector.capture.adapters.operational_status import (
    OperationalIdentity,
    OperationalStatusError,
    read_operational_status,
)
from omi_collector.capture.adapters.quality_metrics import (
    QualityMetricsError,
    read_quality_events,
    validate_quality_event,
)
from omi_collector.capture.application.operational_telemetry import system_host_boot_id
from omi_collector.capture.domain.internal_audio_machine import QUARANTINE_CUSTODY_HOLD_REASONS
from omi_collector.capture.domain.ring_protocol import RECORD_SIZE
from omi_collector.config import DEFAULT_CONFIG
from omi_collector.spool_metrics import collect_spool_metrics
from omi_collector.storage_layout import StorageLayout

_OPERATIONAL_ATTENTION = {
    "quality": "quality_metrics_unavailable",
    "publication": "ready_publication_blocked",
    "clock": "clock_correction_blocked",
}


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
    clock_corrections: int = 0
    last_clock_correction_at: str | None = None
    last_clock_correction_drift_seconds: float | None = None
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
            "clock_corrections": self.clock_corrections,
            "last_advertisement_at": self.last_advertisement_at,
            "last_advertisement_rssi_dbm": self.last_advertisement_rssi_dbm,
            "last_clock_correction_at": self.last_clock_correction_at,
            "last_clock_correction_drift_seconds": self.last_clock_correction_drift_seconds,
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
    clock_corrections: int = 0
    last_clock_correction: tuple[datetime, float] | None = None
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
            clock_corrections=self.clock_corrections,
            last_clock_correction_at=_iso(self.last_clock_correction[0]) if self.last_clock_correction else None,
            last_clock_correction_drift_seconds=self.last_clock_correction[1] if self.last_clock_correction else None,
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
    advertisement_rssi_dbm: int | None = None
    outcome: str | None = None
    termination_class: str | None = None
    written_raw_bytes: int = 0
    active_read_elapsed_ms: int = 0
    missing_record_count: int = 0
    missing_raw_bytes: int = 0
    reason: str | None = None
    loss_id: str | None = None
    drift_seconds: float | None = None


def collect_operator_status(
    layout: StorageLayout,
    *,
    hours: int,
    now: datetime | None = None,
    identity: OperationalIdentity | None = None,
) -> dict[str, object]:
    """Summarize current device state, publication evidence, and recent quality events."""
    if isinstance(hours, bool) or not isinstance(hours, int) or hours <= 0:
        raise OperatorStatusError("hours must be positive")
    end = now or datetime.now(UTC)
    if end.tzinfo is None:
        raise OperatorStatusError("status clock must be timezone-aware")
    start = end.astimezone(UTC) - timedelta(hours=hours)
    from omi_collector.state_migration import StateMigrationError, open_runtime_state

    try:
        db = open_runtime_state(layout, read_only=True)
        observations = read_firmware_observations(db)
        spool = collect_spool_metrics(
            layout.publication.root,
            state_db=db,
        )
        confirmed_losses = read_confirmed_losses(
            db,
            allow_missing=db.get(StateKind.OPERATIONAL_STATUS, "current") is None,
        )
        quality = _quality_window(
            db,
            start,
            end.astimezone(UTC),
            confirmed_losses=confirmed_losses,
        )
        current_identity = identity or OperationalIdentity(system_host_boot_id(), os.environ.get("INVOCATION_ID"))
        operational_status = read_operational_status(db, current_identity)
        runtime = _runtime_status(db, end.astimezone(UTC), operational_status, current_identity)
        custody_holds = _custody_hold_count(db)
        ble_scan_incident = _ble_scan_incident_summary(db)
    except (
        ConfirmedLossError,
        QualityMetricsError,
        StateDBError,
        ValueError,
        OperationalStatusError,
        StateMigrationError,
    ) as error:
        raise OperatorStatusError(str(error)) from error
    device = _device_status(observations[0]) if observations else None
    window_status = _window_status(quality, operational_status=operational_status)
    if runtime["attention_reasons"]:
        window_status = "attention"
    if ble_scan_incident["active"]:
        window_status = "attention"
    return {
        "device": device,
        "generated_at": end.astimezone(UTC).isoformat(timespec="seconds"),
        "publication": spool.current_window.as_dict(),
        "quality_window": quality.as_dict(),
        "custody_holds": custody_holds,
        "ble_scan_incident": ble_scan_incident,
        "runtime": runtime,
        "schema_version": 2,
        "status": window_status,
        "window_hours": hours,
    }


def _ble_scan_incident_summary(db: StateDB) -> dict[str, object]:
    incident = db.get(StateKind.BLE_SCAN_INCIDENT, "open")
    history = db.get(StateKind.BLE_SCAN_INCIDENT, "history")
    entries = history.get("entries", []) if history is not None else []
    if not isinstance(entries, list):
        raise OperatorStatusError("BLE scan incident history is invalid")
    latest = entries[-1] if entries and isinstance(entries[-1], dict) else {}
    public_incident = read_ble_scan_incident(db)
    if incident is None:
        return {
            "active": False,
            **(public_incident or {}),
            "resolved_count": min(len(entries), 8),
            "last_resolved_at": latest.get("resolved_at"),
        }
    context = incident.get("context")
    if not isinstance(context, dict):
        raise OperatorStatusError("BLE scan incident context is invalid")
    diagnostics = incident.get("diagnostics")
    bluez = diagnostics.get("bluez") if isinstance(diagnostics, dict) else None
    hci = diagnostics.get("hci") if isinstance(diagnostics, dict) else None
    return {
        "active": True,
        **(public_incident or {}),
        "fault_category": "host_ble_receive_unavailable"
        if context.get("reason") == "scan_silence"
        else context.get("reason"),
        "reason": context.get("reason"),
        "active_seconds": context.get("active_seconds"),
        "bleak_callbacks": context.get("bleak_callbacks"),
        "last_callback_age_seconds": context.get("last_callback_age_seconds"),
        "threshold_seconds": context.get("threshold_seconds"),
        "bluez": _incident_diag_summary(bluez, ("powered", "discovering", "devices_count")),
        "hci": _incident_diag_summary(hci, ("status", "failure", "counts")),
        "resolved_count": min(len(entries), 8),
        "last_resolved_at": latest.get("resolved_at"),
    }


def _incident_diag_summary(value: object, keys: tuple[str, ...]) -> dict[str, object]:
    if not isinstance(value, dict):
        return {"available": False}
    result: dict[str, object] = {"available": value.get("available", value.get("status") == "available") is True}
    for key in keys:
        item = value.get(key)
        if key in {"powered", "discovering"}:
            result[key] = item if type(item) is bool else None
        elif key == "devices_count":
            result[key] = item if type(item) is int and item >= 0 else None
        elif key == "counts" and isinstance(item, dict):
            result[key] = {
                name: count if type(count) is int and count >= 0 else None
                for name in ("legacy_advertising", "extended_advertising", "hardware_error", "malformed")
                if (count := item.get(name)) is not None
            }
        elif key in {"status", "failure"}:
            result[key] = item[:64] if isinstance(item, str) else None
    return result


def _custody_hold_count(db: StateDB) -> int:
    """Count SQL audio held indefinitely after unresolved custody conflicts."""
    try:
        with (
            closing(db.open_connection(read_only=True)) as connection,
            db.access(connection),
            closing(
                connection.execute(
                    "SELECT COUNT(*) FROM capture_source WHERE source_type='quarantine' "
                    "AND lifecycle='blocked' AND block_reason IN (?, ?)",
                    tuple(sorted(QUARANTINE_CUSTODY_HOLD_REASONS)),
                )
            ) as cursor,
        ):
            row = cast(sqlite3.Row | None, cursor.fetchone())
    except (OSError, sqlite3.DatabaseError, StateDBError) as error:
        raise_if_sqlite_storage_fault(error)
        raise OperatorStatusError("custody hold inventory is unreadable") from error
    count = None if row is None else cast(object, row[0])
    if type(count) is not int:
        raise OperatorStatusError("custody hold inventory is invalid")
    return cast(int, count)


def _runtime_status(
    db: StateDB, now: datetime, operational_status: dict[str, str], identity: OperationalIdentity
) -> dict[str, object]:
    projection = read_runtime_status(db) or {}
    return _project_runtime_status(projection, now, operational_status, identity)


def _project_runtime_status(
    projection: dict[str, object], now: datetime, operational_status: dict[str, str], identity: OperationalIdentity
) -> dict[str, object]:
    """Derive the operator view from one already-read runtime snapshot."""
    latest = _dated_projection(projection, "progress", "updated_at")
    observation = _dated_projection(projection, "observation", "observation_at")
    battery_observation = _dated_projection(projection, "battery_observation", "battery_observed_at")
    error = _dated_projection(projection, "last_error", "last_error_at")
    current = latest[1] if latest else {}
    observed = observation[1] if observation else {}
    battery = battery_observation[1] if battery_observation else {}
    rssi = projection.get("connection_rssi_dbm")
    rssi_at = projection.get("connection_rssi_observed_at")
    state = current.get("status", "unknown")
    active = (
        state == "progress"
        and identity.invocation_id is not None
        and current.get("invocation_id") == identity.invocation_id
    )
    if state == "progress" and not active:
        state = "unknown"
    return {
        "attention_reasons": sorted(
            _OPERATIONAL_ATTENTION[name] for name, state in operational_status.items() if state == "blocked"
        ),
        "battery_percent": battery.get("battery_percent"),
        "battery_observed_at": _iso(battery_observation[0]) if battery_observation else None,
        "connection_rssi_dbm": rssi,
        "connection_rssi_observed_at": _iso(datetime.fromisoformat(rssi_at)) if isinstance(rssi_at, str) else None,
        "firmware": observed.get("firmware"),
        "last_error": _runtime_error(error),
        "operational_status": operational_status,
        "state": "transferring" if active else state,
        "updated_at": _iso(latest[0]) if latest else None,
        "updated_age_seconds": max(0, int((now - latest[0]).total_seconds())) if latest else None,
        "transfer": _active_transfer(current) if active else None,
    }


def _dated_projection(
    projection: dict[str, object], value_key: str, time_key: str
) -> tuple[datetime, dict[str, object]] | None:
    value = projection.get(value_key)
    timestamp = projection.get(time_key)
    if not isinstance(value, dict) or not isinstance(timestamp, str):
        return None
    return datetime.fromisoformat(timestamp), cast(dict[str, object], value)


def _decode_connection_rssi(row: dict[str, object]) -> tuple[datetime, int] | None:
    if row.get("event") != "ble_link_rssi_observed":
        return None
    fields = row.get("fields")
    if not isinstance(fields, dict):
        raise OperatorStatusError("debug RSSI observation has invalid fields")
    value = fields.get("rssi_dbm")
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int):
        raise OperatorStatusError("debug RSSI observation has invalid rssi_dbm")
    return _timestamp(row, "timestamp"), value


def _newest_connection_rssi(
    current: tuple[datetime, int] | None, candidate: tuple[datetime, int] | None
) -> tuple[datetime, int] | None:
    if candidate is None:
        return current
    if current is None or candidate[0] > current[0]:
        return candidate
    return current


def _runtime_error(error: tuple[datetime, dict[str, object]] | None) -> dict[str, object] | None:
    if error is None:
        return None
    timestamp, progress = error
    return {
        "error_message": progress.get("error_message"),
        "error_type": progress.get("error_type"),
        "lock_context": progress.get("lock_context"),
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
        line = line.lstrip(b"\x00")
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


def _window_status(quality: _QualityWindow, *, operational_status: dict[str, str]) -> str:
    if quality.last_transfer_termination_class in {"cancelled", "fatal_error", "teardown_interrupted"}:
        return "attention"
    if "blocked" in operational_status.values():
        return "attention"
    return (
        "ok"
        if quality.completed_transfers and all(state == "clear" for state in operational_status.values())
        else "unknown"
    )


def _quality_window(
    db: StateDB,
    start: datetime,
    end: datetime,
    *,
    confirmed_losses: tuple[ConfirmedLossFact, ...] = (),
) -> _QualityWindow:
    accumulator = _QualityAccumulator()
    facts_by_id = {fact.loss_id: fact for fact in confirmed_losses}
    _accumulate_confirmed_loss_facts(accumulator, confirmed_losses, start, end)
    for row in read_quality_events(db, start, end):
        event = _decode_quality_event(row)
        if event.loss_id is not None:
            _validate_loss_projection(event, facts_by_id)
            continue
        _accumulate_quality_event(accumulator, event, start, end)
    return accumulator.build()


def _accumulate_confirmed_loss_facts(
    accumulator: _QualityAccumulator,
    facts: tuple[ConfirmedLossFact, ...],
    start: datetime,
    end: datetime,
) -> None:
    relevant = [fact for fact in facts if start <= datetime.fromisoformat(fact.occurred_at).astimezone(UTC) <= end]
    intervals_by_attempt: dict[str, list[tuple[int, int]]] = {}
    for fact in relevant:
        intervals_by_attempt.setdefault(fact.attempt_id, []).append((fact.start_sequence, fact.end_sequence))
    for intervals in intervals_by_attempt.values():
        event_count, union_size = _union_interval_stats(intervals)
        accumulator.loss_events += event_count
        accumulator.missing_records += union_size
        accumulator.missing_raw_bytes += union_size * RECORD_SIZE


def _union_interval_stats(intervals: Iterable[tuple[int, int]]) -> tuple[int, int]:
    ordered = sorted(intervals)
    if not ordered:
        return 0, 0
    components = 1
    total = 0
    current_start, current_end = ordered[0]
    for start, end in ordered[1:]:
        if start > current_end:
            total += current_end - current_start
            current_start, current_end = start, end
            components += 1
        else:
            current_end = max(current_end, end)
    return components, total + current_end - current_start


def _validate_loss_projection(event: _QualityEvent, facts_by_id: dict[str, ConfirmedLossFact]) -> None:
    fact = facts_by_id.get(event.loss_id or "")
    if fact is None:
        raise OperatorStatusError("keyed sequence loss is missing from confirmed-loss ledger")
    if (
        fact.missing_record_count != event.missing_record_count
        or fact.missing_raw_bytes != event.missing_raw_bytes
        or fact.reason != event.reason
    ):
        raise OperatorStatusError("keyed sequence loss does not match confirmed-loss ledger")


def _accumulate_quality_event(
    accumulator: _QualityAccumulator, event: _QualityEvent, start: datetime, end: datetime
) -> None:
    if not start <= event.timestamp <= end:
        return
    if event.event == "advertisement_observation":
        _accumulate_advertisement(accumulator, event)
    elif event.event == "transfer_session":
        _accumulate_transfer(accumulator, event)
    elif event.event == "sequence_loss":
        _accumulate_loss(accumulator, event)
    else:
        _accumulate_clock_correction(accumulator, event)


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


def _accumulate_clock_correction(accumulator: _QualityAccumulator, event: _QualityEvent) -> None:
    drift = event.drift_seconds
    if drift is None:
        raise OperatorStatusError("clock correction event is incomplete")
    accumulator.clock_corrections += 1
    if accumulator.last_clock_correction is None or event.timestamp > accumulator.last_clock_correction[0]:
        accumulator.last_clock_correction = (event.timestamp, drift)


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
    timestamp = validate_quality_event(row)
    event = cast(str, row["event"])
    if event == "advertisement_observation":
        return _decode_advertisement(row, timestamp)
    if event == "transfer_session":
        return _decode_transfer(row, timestamp)
    if event == "sequence_loss":
        return _decode_loss(row, timestamp)
    if event == "clock_correction":
        return _decode_clock_correction(row, timestamp)
    raise AssertionError("validated quality event has an unsupported type")


def _decode_advertisement(row: dict[str, object], timestamp: datetime) -> _QualityEvent:
    return _QualityEvent(
        "advertisement_observation",
        timestamp,
        advertisement_rssi_dbm=cast(int, row["advertisement_rssi_dbm"]),
    )


def _decode_transfer(row: dict[str, object], timestamp: datetime) -> _QualityEvent:
    return _QualityEvent(
        "transfer_session",
        timestamp,
        outcome=cast(str, row["outcome"]),
        termination_class=cast(str, row["termination_class"]),
        written_raw_bytes=cast(int, row["written_raw_bytes"]),
        active_read_elapsed_ms=cast(int, row["active_read_elapsed_ms"]),
    )


def _decode_loss(row: dict[str, object], timestamp: datetime) -> _QualityEvent:
    return _QualityEvent(
        "sequence_loss",
        timestamp,
        missing_record_count=cast(int, row["missing_record_count"]),
        missing_raw_bytes=cast(int, row["missing_raw_bytes"]),
        reason=cast(str, row["reason"]),
        loss_id=cast(str | None, row.get("loss_id")),
    )


def _decode_clock_correction(row: dict[str, object], timestamp: datetime) -> _QualityEvent:
    drift = cast(int | float, row["drift_seconds"])
    return _QualityEvent("clock_correction", timestamp, drift_seconds=float(drift))


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


def _iso(value: datetime | None) -> str | None:
    return value.astimezone(UTC).isoformat(timespec="milliseconds") if value else None
