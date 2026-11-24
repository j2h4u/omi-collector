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
from omi_collector.capture.adapters.confirmed_loss import (
    ConfirmedLossError,
    ConfirmedLossFact,
    read_confirmed_losses,
)
from omi_collector.capture.adapters.debug_logging import read_runtime_status
from omi_collector.capture.adapters.firmware_observations import FirmwareObservation, read_firmware_observations
from omi_collector.capture.adapters.internal_audio_store import decode_uint64
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
from omi_collector.json_type_guards import is_dict as _is_dict
from omi_collector.json_type_guards import is_list as _is_list
from omi_collector.spool_metrics import collect_spool_metrics
from omi_collector.storage_layout import StorageLayout

_OPERATIONAL_ATTENTION = {
    "quality": "quality_metrics_unavailable",
    "publication": "ready_publication_blocked",
    "clock": "clock_correction_blocked",
}
_INCIDENT_SOURCES_CTE = (
    "WITH incident_sources(incident_id,source_id) AS ("
    "SELECT incident_id,incident_id FROM capture_framing_incident UNION "
    "SELECT incident_id,source_id FROM capture_observation UNION "
    "SELECT incident_id,source_id FROM capture_recovery_candidate) "
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


@dataclass(frozen=True, slots=True)
class _FramingRow:
    incident_id: str
    source_id: str
    leg_generation: int
    confirmed_damage: bool
    source_start: int
    read_start: int
    read_count: int
    fault_offset: int | None
    prefix_record_count: int
    tail_range: tuple[int, int] | None
    chunk_range: tuple[int, int] | None


@dataclass(frozen=True, slots=True)
class _FramingSourceRow:
    incident_id: str
    source_id: str
    sequence_start: int
    committed_count: int
    verified_count: int
    confirmed_frontier: int
    verified_conflict: bool
    tail_generation: int | None
    tail_offset: int | None
    tail_size: int | None


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
        custody_holds, runtime["saved_unfinished_records"] = _audio_custody_counts(db)
        audio_integrity = _audio_integrity_summary(db)
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
    if (
        audio_integrity["unresolved_source_count"]
        or audio_integrity["unresolved_observation_count"]
        or audio_integrity["unresolved_conflict_count"]
    ):
        window_status = "attention"
    return {
        "audio_integrity": audio_integrity,
        "device": device,
        "generated_at": end.astimezone(UTC).isoformat(timespec="seconds"),
        "publication": spool.current_window.as_dict(),
        "quality_window": quality.as_dict(),
        "custody_holds": custody_holds,
        "ble_scan_incident": ble_scan_incident,
        "runtime": runtime,
        "schema_version": 4,
        "status": window_status,
        "window_hours": hours,
    }


def _audio_integrity_summary(db: StateDB) -> dict[str, object]:
    """Project only unresolved framing metadata, never its audio bytes."""
    observations: dict[tuple[str, int], dict[str, object]] = {}
    sources = _group_framing_sources(tuple(_decode_framing_source(row) for row in _unresolved_framing_source_rows(db)))
    for raw_row in _unresolved_framing_rows(db):
        _accumulate_framing_row(observations, _decode_framing_row(raw_row))
    conflicts = _unresolved_conflict_rows(db)
    conflict_summary = _finish_conflict_summary(conflicts)
    observation_summary = _finish_framing_summary(observations)
    source_summary = [_project_framing_source(source) for source in sources]
    retained_bytes = sum(cast(int, source["retained_raw_bytes"]) for source in source_summary)
    retained_bytes += sum(cast(int, item["retained_raw_bytes"]) for item in observation_summary)
    retained_bytes += sum(cast(int, item["retained_raw_bytes"]) for item in conflict_summary)
    return {
        "confirmed_malformed_observations": sum(
            cast(bool, item["confirmed_damage"]) and item["fault_offset_read_relative"] is not None
            for item in observation_summary
        ),
        "unresolved_conflict_count": len(conflict_summary),
        "unresolved_retained_storage_bytes": retained_bytes,
        "unresolved_observation_count": len(observation_summary),
        "unresolved_source_count": len(source_summary),
        "unresolved_sources": source_summary,
        "unresolved_observations": observation_summary,
        "unresolved_conflicts": conflict_summary,
    }


def _unresolved_framing_source_rows(db: StateDB) -> list[sqlite3.Row]:
    try:
        with closing(db.open_connection(read_only=True)) as connection, db.access(connection):
            return connection.execute(
                _INCIDENT_SOURCES_CTE
                + "SELECT incident.incident_id, source.source_id, source.sequence_start, source.committed_count, "
                "COALESCE(legs.verified_count,0), incident.confirmed_frontier, incident.verified_conflict, "
                "tail.leg_generation, tail.byte_offset, length(tail.bytes) "
                "FROM capture_framing_incident AS incident "
                "JOIN incident_sources AS related ON related.incident_id=incident.incident_id "
                "JOIN capture_source AS source ON source.source_id=related.source_id "
                "LEFT JOIN (SELECT source_id,MAX(verified_count) AS verified_count "
                "FROM capture_read_leg GROUP BY source_id) AS legs ON legs.source_id=source.source_id "
                "LEFT JOIN capture_tail AS tail ON tail.source_id=source.source_id "
                "WHERE incident.status='unresolved' ORDER BY source.source_id, incident.incident_id"
            ).fetchall()
    except (OSError, sqlite3.DatabaseError, StateDBError) as error:
        raise_if_sqlite_storage_fault(error)
        raise OperatorStatusError("framing source custody is unreadable") from error


def _decode_framing_source(row: sqlite3.Row) -> _FramingSourceRow:
    try:
        (
            incident_id,
            source_id,
            encoded_start,
            committed_count,
            verified_count,
            encoded_frontier,
            verified_conflict,
            tail_generation,
            tail_offset,
            tail_size,
        ) = cast(tuple[object, ...], row)
        sequence_start = decode_uint64(cast(bytes, encoded_start))
        confirmed_frontier = decode_uint64(cast(bytes, encoded_frontier))
    except (StateDBError, TypeError, ValueError) as error:
        raise OperatorStatusError("framing source metadata is invalid") from error
    if (
        not isinstance(incident_id, str)
        or not isinstance(source_id, str)
        or type(committed_count) is not int
        or committed_count < 0
        or type(verified_count) is not int
    ):
        raise OperatorStatusError("framing source metadata is invalid")
    if (
        not 0 <= verified_count <= committed_count
        or type(verified_conflict) is not int
        or verified_conflict not in (0, 1)
    ):
        raise OperatorStatusError("framing source metadata is invalid")
    if tail_generation is not None and (type(tail_generation) is not int or tail_generation < 0):
        raise OperatorStatusError("framing source metadata is invalid")
    if tail_offset is not None and (type(tail_offset) is not int or tail_offset < 0):
        raise OperatorStatusError("framing source metadata is invalid")
    if tail_size is not None and (type(tail_size) is not int or not 1 <= tail_size < RECORD_SIZE):
        raise OperatorStatusError("framing source metadata is invalid")
    if (tail_generation is None) != (tail_offset is None) or (tail_generation is None) != (tail_size is None):
        raise OperatorStatusError("framing source metadata is invalid")
    return _FramingSourceRow(
        incident_id,
        source_id,
        sequence_start,
        committed_count,
        verified_count,
        confirmed_frontier,
        bool(verified_conflict),
        cast(int | None, tail_generation),
        cast(int | None, tail_offset),
        cast(int | None, tail_size),
    )


def _group_framing_sources(rows: tuple[_FramingSourceRow, ...]) -> tuple[tuple[_FramingSourceRow, ...], ...]:
    by_source: dict[str, list[_FramingSourceRow]] = {}
    for row in rows:
        by_source.setdefault(row.source_id, []).append(row)
    return tuple(tuple(by_source[source_id]) for source_id in sorted(by_source))


def _project_framing_source(rows: tuple[_FramingSourceRow, ...]) -> dict[str, object]:
    source = rows[0]
    if any(
        row.sequence_start != source.sequence_start
        or row.committed_count != source.committed_count
        or row.verified_count != source.verified_count
        or (row.tail_generation, row.tail_offset, row.tail_size)
        != (source.tail_generation, source.tail_offset, source.tail_size)
        for row in rows[1:]
    ):
        raise OperatorStatusError("framing source custody rows disagree")
    canonical_starts = [source.verified_count]
    for row in rows:
        if row.verified_conflict:
            conflict_start = max(source.sequence_start, row.confirmed_frontier) - source.sequence_start
            canonical_starts.append(conflict_start)
    canonical_start = min(canonical_starts)
    if canonical_start > source.committed_count:
        raise OperatorStatusError("framing canonical suffix is invalid")
    canonical_end = source.committed_count
    canonical_bytes = (canonical_end - canonical_start) * RECORD_SIZE
    retained_bytes = canonical_bytes
    canonical_range = (
        {"start": canonical_start * RECORD_SIZE, "end": canonical_end * RECORD_SIZE}
        if canonical_start < canonical_end
        else None
    )
    tail: dict[str, object] | None = None
    if source.tail_offset is not None and source.tail_size is not None and source.tail_generation is not None:
        tail = {
            "local_reference": {"source_id": source.source_id, "leg_generation": source.tail_generation},
            "source_relative_byte_range": {
                "start": source.tail_offset,
                "end": source.tail_offset + source.tail_size,
            },
            "retained_raw_bytes": source.tail_size,
        }
        retained_bytes += source.tail_size
    return {
        "local_reference": {"source_id": source.source_id},
        "incident_ids": sorted({row.incident_id for row in rows}),
        "canonical_unverified_records": canonical_end - canonical_start,
        "canonical_unverified_byte_range": canonical_range,
        "retained_raw_bytes": retained_bytes,
        "tail": tail,
    }


def _unresolved_framing_rows(db: StateDB) -> list[sqlite3.Row]:
    try:
        with closing(db.open_connection(read_only=True)) as connection, db.access(connection):
            return connection.execute(
                "SELECT incident.incident_id, observation.source_id, observation.leg_generation, "
                "incident.confirmed_damage, source.sequence_start, observation.read_start, "
                "observation.read_count, observation.fault_offset, observation.prefix_record_count, "
                "observation.received_bytes, length(observation.tail), chunk.byte_offset, length(chunk.bytes) "
                "FROM capture_observation AS observation "
                "JOIN capture_framing_incident AS incident ON incident.incident_id=observation.incident_id "
                "JOIN capture_source AS source ON source.source_id=observation.source_id "
                "LEFT JOIN capture_observation_chunk AS chunk "
                "ON chunk.source_id=observation.source_id AND chunk.leg_generation=observation.leg_generation "
                "WHERE incident.status='unresolved' "
                "ORDER BY observation.source_id, observation.leg_generation, chunk.byte_offset"
            ).fetchall()
    except (OSError, sqlite3.DatabaseError, StateDBError) as error:
        raise_if_sqlite_storage_fault(error)
        raise OperatorStatusError("framing evidence is unreadable") from error


def _unresolved_conflict_rows(db: StateDB) -> list[sqlite3.Row]:
    try:
        with closing(db.open_connection(read_only=True)) as connection, db.access(connection):
            return connection.execute(
                _INCIDENT_SOURCES_CTE
                + "SELECT DISTINCT conflict.source_id, conflict.chunk_ordinal, conflict.leg_generation, "
                "conflict.absolute_byte_offset, length(conflict.bytes), conflict.reason "
                "FROM capture_conflict AS conflict "
                "JOIN incident_sources AS related ON related.source_id=conflict.source_id "
                "JOIN capture_framing_incident AS incident ON incident.incident_id=related.incident_id "
                "WHERE incident.status='unresolved' "
                "ORDER BY conflict.source_id, conflict.absolute_byte_offset, conflict.chunk_ordinal"
            ).fetchall()
    except (OSError, sqlite3.DatabaseError, StateDBError) as error:
        raise_if_sqlite_storage_fault(error)
        raise OperatorStatusError("framing conflict inventory is unreadable") from error


def _decode_framing_row(row: sqlite3.Row) -> _FramingRow:
    try:
        (
            incident_id,
            source_id,
            generation,
            confirmed_damage,
            encoded_source_start,
            encoded_start,
            count,
            fault,
            prefix_count,
            received,
            tail_size,
            chunk_start,
            chunk_size,
        ) = cast(tuple[object, ...], row)
    except ValueError as error:
        raise OperatorStatusError("framing observation row is invalid") from error
    if (
        not isinstance(incident_id, str)
        or not isinstance(source_id, str)
        or type(generation) is not int
        or type(confirmed_damage) is not int
        or confirmed_damage not in (0, 1)
    ):
        raise OperatorStatusError("framing observation metadata is invalid")
    if type(count) is not int or count <= 0 or (type(fault) is not int and fault is not None):
        raise OperatorStatusError("framing observation metadata is invalid")
    if type(prefix_count) is not int or not 0 <= prefix_count <= count:
        raise OperatorStatusError("framing observation metadata is invalid")
    if type(received) is not int or received < 0 or type(tail_size) is not int or not 0 <= tail_size < RECORD_SIZE:
        raise OperatorStatusError("framing observation metadata is invalid")
    try:
        encoded_source_start = decode_uint64(cast(bytes, encoded_source_start))
        encoded_start = decode_uint64(cast(bytes, encoded_start))
    except (StateDBError, TypeError, ValueError) as error:
        raise OperatorStatusError("framing observation READ_BEGIN is invalid") from error
    if received < tail_size or (fault is not None and not 0 <= fault < received):
        raise OperatorStatusError("framing observation byte extent is invalid")
    if prefix_count * RECORD_SIZE > received - tail_size:
        raise OperatorStatusError("framing observation prefix extent exceeds its READ")
    tail_range = (received - tail_size, received) if tail_size else None
    chunk_range = _framing_chunk_range(chunk_start, chunk_size, prefix_count, received, tail_size)
    if encoded_start < encoded_source_start:
        raise OperatorStatusError("framing observation READ_BEGIN precedes its source")
    return _FramingRow(
        incident_id,
        source_id,
        generation,
        bool(confirmed_damage),
        encoded_source_start,
        encoded_start,
        count,
        fault,
        prefix_count,
        tail_range,
        chunk_range,
    )


def _framing_chunk_range(
    chunk_start: object, chunk_size: object, prefix_count: int, received: int, tail_size: int
) -> tuple[int, int] | None:
    if chunk_start is None and chunk_size is None:
        chunk_range = None
    elif type(chunk_start) is int and type(chunk_size) is int and chunk_size > 0:
        chunk_range = (chunk_start, chunk_start + chunk_size)
        if chunk_start < prefix_count * RECORD_SIZE or chunk_range[1] > received - tail_size:
            raise OperatorStatusError("framing observation chunk extent is outside the READ")
    else:
        raise OperatorStatusError("framing observation chunk extent is invalid")
    return chunk_range


def _accumulate_framing_row(observations: dict[tuple[str, int], dict[str, object]], row: _FramingRow) -> None:
    key = (row.source_id, row.leg_generation)
    observation: dict[str, object] | None = observations.get(key)
    read_begin = {"start_sequence": row.read_start, "packet_count": row.read_count}
    if observation is None:
        observation = {
            "incident_id": row.incident_id,
            "confirmed_damage": row.confirmed_damage,
            "local_reference": {"source_id": row.source_id, "leg_generation": row.leg_generation},
            "read_begin": read_begin,
            "fault_offset_read_relative": row.fault_offset,
            "prefix_record_count": row.prefix_record_count,
            "read_relative_byte_ranges": [],
            "_tail_range": row.tail_range,
            "_source_start": row.source_start,
            "_read_start": row.read_start,
        }
        observations[key] = observation
    elif (
        observation["incident_id"] != row.incident_id
        or observation["confirmed_damage"] is not row.confirmed_damage
        or observation["read_begin"] != read_begin
        or any(
            observation[key] != value
            for key, value in (
                ("fault_offset_read_relative", row.fault_offset),
                ("prefix_record_count", row.prefix_record_count),
                ("_tail_range", row.tail_range),
                ("_source_start", row.source_start),
            )
        )
    ):
        raise OperatorStatusError("framing observation rows disagree")
    if row.chunk_range is not None:
        _append_read_range(observation, *row.chunk_range)


def _finish_framing_summary(observations: dict[tuple[str, int], dict[str, object]]) -> list[dict[str, object]]:
    summary: list[dict[str, object]] = []
    for observation in observations.values():
        tail_range = cast(tuple[int, int] | None, observation.pop("_tail_range"))
        source_start = cast(int, observation.pop("_source_start"))
        read_start = cast(int, observation.pop("_read_start"))
        if tail_range is not None:
            ranges = cast(list[dict[str, int]], observation["read_relative_byte_ranges"])
            _append_range_json(ranges, tail_range[0], tail_range[1])
        prefix_count = cast(int, observation["prefix_record_count"])
        prefix_start = (read_start - source_start) * RECORD_SIZE
        observation["prefix_source_relative_byte_range"] = (
            {"start": prefix_start, "end": prefix_start + prefix_count * RECORD_SIZE} if prefix_count else None
        )
        observation["retained_raw_bytes"] = sum(
            byte_range["end"] - byte_range["start"]
            for byte_range in cast(list[dict[str, int]], observation["read_relative_byte_ranges"])
        )
        summary.append(observation)
    summary.sort(
        key=lambda item: (
            cast(dict[str, object], item["local_reference"])["source_id"],
            cast(dict[str, object], item["local_reference"])["leg_generation"],
        )
    )
    return summary


def _append_read_range(observation: dict[str, object], start: int, end: int) -> None:
    ranges = cast(list[dict[str, int]], observation["read_relative_byte_ranges"])
    _append_range_json(ranges, start, end)


def _append_range_json(ranges: list[dict[str, int]], start: int, end: int) -> None:
    if ranges and start < ranges[-1]["end"]:
        raise OperatorStatusError("framing observation byte ranges overlap")
    if ranges and start == ranges[-1]["end"]:
        ranges[-1]["end"] = end
    else:
        ranges.append({"start": start, "end": end})


def _finish_conflict_summary(rows: list[sqlite3.Row]) -> list[dict[str, object]]:
    summary: list[dict[str, object]] = []
    for row in rows:
        source_id, ordinal, generation, offset, size, reason = cast(tuple[object, ...], row)
        if (
            not isinstance(source_id, str)
            or type(ordinal) is not int
            or ordinal < 0
            or type(generation) is not int
            or generation < 0
        ):
            raise OperatorStatusError("framing conflict metadata is invalid")
        if type(offset) is not int or offset < 0 or type(size) is not int or size <= 0 or not isinstance(reason, str):
            raise OperatorStatusError("framing conflict metadata is invalid")
        end = offset + size
        summary.append(
            {
                "local_reference": {"source_id": source_id, "leg_generation": generation},
                "chunk_ordinal": ordinal,
                "source_relative_byte_ranges": [{"start": offset, "end": end}],
                "retained_raw_bytes": size,
                "reason": reason,
            }
        )
    return summary


def _ble_scan_incident_summary(db: StateDB) -> dict[str, object]:
    incident = db.get(StateKind.BLE_SCAN_INCIDENT, "open")
    history = db.get(StateKind.BLE_SCAN_INCIDENT, "history")
    entries: object = history.get("entries", []) if history is not None else []
    if not _is_list(entries):
        raise OperatorStatusError("BLE scan incident history is invalid")
    latest = entries[-1] if entries and _is_dict(entries[-1]) else {}
    public_incident = read_ble_scan_incident(db)
    if incident is None:
        return {
            "active": False,
            **(public_incident or {}),
            "resolved_count": min(len(entries), 8),
            "last_resolved_at": latest.get("resolved_at"),
        }
    context = incident.get("context")
    if not _is_dict(context):
        raise OperatorStatusError("BLE scan incident context is invalid")
    diagnostics = incident.get("diagnostics")
    bluez = diagnostics.get("bluez") if _is_dict(diagnostics) else None
    hci = diagnostics.get("hci") if _is_dict(diagnostics) else None
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
    if not _is_dict(value):
        return {"available": False}
    result: dict[str, object] = {"available": value.get("available", value.get("status") == "available") is True}
    for key in keys:
        item = value.get(key)
        if key in {"powered", "discovering"}:
            result[key] = item if type(item) is bool else None
        elif key == "devices_count":
            result[key] = item if type(item) is int and item >= 0 else None
        elif key == "counts" and _is_dict(item):
            result[key] = {
                name: count if type(count) is int and count >= 0 else None
                for name in ("legacy_advertising", "extended_advertising", "hardware_error", "malformed")
                if (count := item.get(name)) is not None
            }
        elif key in {"status", "failure"}:
            result[key] = item[:64] if isinstance(item, str) else None
    return result


def _audio_custody_counts(db: StateDB) -> tuple[int, int]:
    """Read held and resumable audio counts from one SQL snapshot."""
    try:
        with (
            closing(db.open_connection(read_only=True)) as connection,
            db.access(connection),
            closing(
                connection.execute(
                    "SELECT (SELECT COUNT(*) FROM capture_source WHERE source_type='quarantine' "
                    "AND lifecycle='blocked' AND block_reason IN (?, ?)), COUNT(*), COALESCE(MAX(committed_count), 0) "
                    "FROM capture_source AS source WHERE source_type='attempt' "
                    "AND lifecycle IN ('prepared','leg_prepared','receiving','interrupted') "
                    "AND NOT EXISTS (SELECT 1 FROM capture_historical_custody AS h "
                    "WHERE h.source_id=source.source_id AND h.disposition='pending')",
                    tuple(sorted(QUARANTINE_CUSTODY_HOLD_REASONS)),
                )
            ) as cursor,
        ):
            row = cast(sqlite3.Row | None, cursor.fetchone())
    except (OSError, sqlite3.DatabaseError, StateDBError) as error:
        raise_if_sqlite_storage_fault(error)
        raise OperatorStatusError("custody hold inventory is unreadable") from error
    values = () if row is None else cast(tuple[object, ...], tuple(row))
    if not values or any(type(value) is not int or value < 0 for value in values):
        raise OperatorStatusError("custody hold inventory is invalid")
    holds, attempts, saved = cast(tuple[int, int, int], values)
    if attempts > 1:
        raise OperatorStatusError("multiple unfinished captures block resume")
    return holds, saved


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
    link = projection.get("ble_link_status")
    link = link if _is_dict(link) else {}
    if identity.invocation_id is not None and link.get("invocation_id") != identity.invocation_id:
        link = {}
    history = link.get("last_observed_phy")
    history = dict(history) if _is_dict(history) else None
    observed_at = history.get("observed_at") if history is not None else None
    if history is not None and isinstance(observed_at, str):
        history["age_seconds"] = max(0, int((now - datetime.fromisoformat(observed_at)).total_seconds()))
    link_state = link.get("state")
    if identity.invocation_id is None or not isinstance(link_state, str):
        link_state = "unknown"
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
        "last_link_observation": _last_link_observation(link, now),
        "last_observed_phy": history,
        "link_observation_state": {
            "state": {"connected": "pending", "disconnected": "not_connected"}.get(link_state, link_state),
            "observed_at": link.get("state_at") if identity.invocation_id is not None else None,
            "reason": link.get("reason") if identity.invocation_id is not None else None,
            **(
                {"disconnect_reason_name": link["disconnect_reason_name"]}
                if identity.invocation_id is not None and link.get("disconnect_reason_name") is not None
                else {}
            ),
        },
        "operational_status": operational_status,
        "state": "transferring" if active else state,
        "updated_at": _iso(latest[0]) if latest else None,
        "updated_age_seconds": max(0, int((now - latest[0]).total_seconds())) if latest else None,
        "transfer": _active_transfer(current) if active else None,
    }


def _last_link_observation(link: dict[object, object], now: datetime) -> dict[str, object] | None:
    timestamp = link.get("state_at")
    if not isinstance(timestamp, str):
        return None
    return {
        "state": link.get("state"),
        "reason": link.get("reason"),
        "disconnect_reason_name": link.get("disconnect_reason_name"),
        "observed_at": timestamp,
        "age_seconds": max(0, int((now - datetime.fromisoformat(timestamp)).total_seconds())),
    }


def _dated_projection(
    projection: dict[str, object], value_key: str, time_key: str
) -> tuple[datetime, dict[str, object]] | None:
    value = projection.get(value_key)
    timestamp = projection.get(time_key)
    if not _is_dict(value) or not isinstance(timestamp, str):
        return None
    return datetime.fromisoformat(timestamp), cast(dict[str, object], value)


def _decode_connection_rssi(row: dict[str, object]) -> tuple[datetime, int] | None:
    if row.get("event") != "ble_link_rssi_observed":
        return None
    fields = row.get("fields")
    if not _is_dict(fields):
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
        if not _is_dict(value):
            raise OperatorStatusError(f"debug journal record is not an object: {path.name}")
        rows.append(cast(dict[str, object], value))
    return rows


def _decode_sync_progress(row: dict[str, object]) -> tuple[datetime, dict[str, object]] | None:
    if row.get("event") != "sync_progress":
        return None
    timestamp = _timestamp(row, "timestamp")
    fields = row.get("fields")
    if not _is_dict(fields):
        raise OperatorStatusError("debug sync progress has invalid fields")
    progress = fields.get("progress")
    if not _is_dict(progress):
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
    if not _is_dict(value):
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
