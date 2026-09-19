"""Bounded historical clock-evidence import and restart-safe publication."""

# JSON/journald values are narrowed at the typed import boundary.
# pyright: reportAny=false
# pyright: reportArgumentType=false

from __future__ import annotations

import json
import math
import os
from dataclasses import dataclass, replace
from hashlib import sha256
from itertools import pairwise
from pathlib import Path
from typing import cast
from uuid import uuid4

from ..domain.ring_protocol import RECORD_SIZE
from .clock_corrections import ClockCorrection, ClockCorrectionStore
from .clock_observations import ClockObservation, ClockObservationStore
from .timeline_generations import GenerationResult, publish_from_ledger

_MIN_INCIDENT_ENTRIES = 2
_SHA256_HEX_LENGTH = 64


class HistoricalRecoveryError(ValueError):
    """Historical input cannot establish one safe clock explanation."""


@dataclass(frozen=True, slots=True)
class HistoricalRecoveryDecision:
    operation_id: str
    state: str
    boundary_sequence: int | None
    observation_id: str | None
    reason: str


class HistoricalClockImporter:
    """Import a finite journald JSON export; never tail or interpret log text."""

    def __init__(self, device_state_path: Path, captured_root: Path) -> None:
        self._device_state_path = Path(device_state_path)
        self._captured_root = Path(captured_root)
        self._observations = ClockObservationStore(self._device_state_path)
        self._imports = self._device_state_path.parent / "clock-imports"
        self._corrections = ClockCorrectionStore(self._device_state_path)

    def recover(
        self,
        entries: object,
        *,
        apply: bool = False,
        dry_run: bool = False,
        tolerance_seconds: float = 1.0,
    ) -> tuple[HistoricalRecoveryDecision, ...]:
        """Validate, persist, and optionally apply a bounded evidence export."""
        if dry_run and apply:
            raise HistoricalRecoveryError("dry-run and apply are mutually exclusive")
        if tolerance_seconds <= 0 or not math.isfinite(tolerance_seconds):
            raise HistoricalRecoveryError("mapping tolerance is invalid")
        rows = _rows(entries)
        if not dry_run:
            self._corrections.recover_prepared()
        operations = tuple(item for item in self._corrections.records() if item.state == "unresolved")
        if len(operations) > 1:
            raise HistoricalRecoveryError("multiple unresolved clock corrections are ambiguous")
        if not operations:
            return ()
        operation = operations[0]
        if not rows:
            return self._recover_empty(operation, dry_run, apply, tolerance_seconds)
        normalized = _prepare_incident(tuple(_entry(row) for row in rows), operation, self._captured_root)
        return self._recover_entries(normalized, operation, dry_run, apply, tolerance_seconds)

    def _recover_empty(
        self, operation: ClockCorrection, dry_run: bool, apply: bool, tolerance_seconds: float
    ) -> tuple[HistoricalRecoveryDecision, ...]:
        imported = self._read_import(operation.operation_id)
        if imported is not None:
            normalized = _prepare_incident(imported, operation, self._captured_root)
            return self._recover_entries(normalized, operation, dry_run, apply, tolerance_seconds)
        durable = _later_observation(self._observations.for_operation(operation.operation_id))
        if durable is None:
            return ()
        decision = _decide_native(operation, durable, tolerance_seconds)
        if apply and decision.state == "applied":
            self._apply(operation, decision, durable)
        return (decision,)

    def _recover_entries(
        self,
        normalized: tuple[_Entry, ...],
        operation: ClockCorrection,
        dry_run: bool,
        apply: bool,
        tolerance_seconds: float,
    ) -> tuple[HistoricalRecoveryDecision, ...]:
        if not any(item.role == "anchor" for item in normalized):
            _validate_legacy_incident(normalized, operation)
            observation = self._draft_observation(normalized, operation)
            return (
                HistoricalRecoveryDecision(
                    operation.operation_id,
                    "unresolved",
                    None,
                    observation.observation_id,
                    "systemd start anchor is absent",
                ),
            )
        _validate_incident(normalized, operation)
        _validate_mapping(normalized, tolerance_seconds)
        observation = (
            self._draft_observation(normalized, operation) if dry_run else self._persist_entries(normalized, operation)
        )
        decision = _decide(operation, normalized, observation, self._captured_root, tolerance_seconds)
        if apply and decision.state == "applied":
            self._apply(operation, decision, observation)
        return (decision,)

    def _draft_observation(self, entries: tuple[_Entry, ...], operation: ClockCorrection) -> ClockObservation:
        later = next(item for item in entries if item.role == "later")
        observation_id = sha256(b"".join(item.source_hash.encode() for item in entries)).hexdigest()[:32]
        records = self._observations.records()
        return ClockObservation(
            1,
            observation_id,
            records[-1].causal_order + 1 if records else 0,
            "anchored_monotonic",
            "historical-import",
            later.boot_id,
            entries[0].realtime,
            later.realtime,
            entries[0].monotonic,
            later.monotonic,
            later.device_epoch,
            min(item.sequence for item in entries),
            max(item.sequence for item in entries),
            operation.operation_id,
            None,
            "later",
            None,
            later.raw_timestamp,
            later.raw_timestamp_hash,
        )

    def _persist_entries(self, entries: tuple[_Entry, ...], operation: ClockCorrection) -> ClockObservation:
        initial = next(item for item in entries if item.role == "initial")
        later = next(item for item in entries if item.role == "later")
        observation_id = sha256(b"".join(item.source_hash.encode() for item in entries)).hexdigest()[:32]
        existing = tuple(item for item in self._observations.records() if item.observation_id == observation_id)
        if existing:
            return existing[0]
        source = {
            "version": 1,
            "operation_id": operation.operation_id,
            "entry_hashes": [item.source_hash for item in entries],
            "entries": [_entry_as_dict(item) for item in entries],
            "boundary_calculation": _boundary_calculation(entries),
        }
        self._write_import(source, observation_id)
        self._observations.anchored_monotonic(
            observation_id=sha256((initial.source_hash + ":initial").encode()).hexdigest()[:32],
            session_id="historical-import",
            host_boot_id=initial.boot_id,
            host_realtime_start=initial.realtime,
            host_realtime_end=initial.realtime,
            host_monotonic_start=initial.monotonic,
            host_monotonic_end=initial.monotonic,
            device_epoch=initial.device_epoch,
            info_sequence_min=initial.sequence,
            info_sequence_max=initial.sequence,
            operation_id=operation.operation_id,
            observation_role="initial",
            raw_timestamp=initial.raw_timestamp,
            raw_timestamp_hash=initial.raw_timestamp_hash,
        )
        return self._observations.anchored_monotonic(
            observation_id=observation_id,
            session_id="historical-import",
            host_boot_id=later.boot_id,
            host_realtime_start=later.realtime,
            host_realtime_end=later.realtime,
            host_monotonic_start=later.monotonic,
            host_monotonic_end=later.monotonic,
            device_epoch=later.device_epoch,
            info_sequence_min=later.sequence,
            info_sequence_max=later.sequence,
            operation_id=operation.operation_id,
            observation_role="later",
            parent_observation_id=sha256((initial.source_hash + ":initial").encode()).hexdigest()[:32],
            raw_timestamp=later.raw_timestamp,
            raw_timestamp_hash=later.raw_timestamp_hash,
        )

    def _read_import(self, operation_id: str) -> tuple[_Entry, ...] | None:
        if not self._imports.exists():
            return None
        matches: list[tuple[_Entry, ...]] = []
        for path in sorted(self._imports.glob("*.json")):
            imported = _read_import_file(path, operation_id)
            if imported is not None:
                matches.append(imported)
        if not matches:
            return None
        if any(candidate != matches[0] for candidate in matches[1:]):
            raise HistoricalRecoveryError("historical import sources conflict")
        return matches[0]

    def _write_import(self, value: dict[str, object], source_id: str) -> None:
        self._imports.mkdir(mode=0o750, parents=True, exist_ok=True)
        path = self._imports / f"{source_id}.json"
        if path.exists():
            if path.read_bytes() != _canonical(value):
                raise HistoricalRecoveryError("historical import source conflicts")
            return
        temporary = path.with_name(f".{path.name}.{uuid4().hex}.tmp")
        try:
            with temporary.open("xb") as stream:
                stream.write(_canonical(value))
                stream.flush()
                os.fsync(stream.fileno())
            temporary.replace(path)
            _sync_directory(self._imports)
        except OSError as error:
            temporary.unlink(missing_ok=True)
            raise HistoricalRecoveryError("historical source is not durable") from error

    def _apply(
        self, operation: ClockCorrection, decision: HistoricalRecoveryDecision, observation: ClockObservation
    ) -> None:
        if decision.boundary_sequence is None:
            raise HistoricalRecoveryError("applied recovery has no effective boundary")
        boundary = self._observations.establish_effective_boundary(
            observation, decision.boundary_sequence, operation_id=operation.operation_id
        )
        current = self._corrections.records()
        durable = next((item for item in current if item.operation_id == operation.operation_id), None)
        if durable is None or durable.state != "unresolved":
            return
        self._corrections.finish(
            durable,
            state="applied",
            boundary_sequence_max=boundary.effective_boundary_sequence,
            verified_epoch=observation.device_epoch,
        )


def recover_and_publish(
    device_state_path: Path,
    captured_root: Path,
    publication_root: Path,
    entries: object = (),
    *,
    apply: bool = True,
) -> GenerationResult:
    """Normal idempotent recovery operation used at startup and after capture."""
    importer = HistoricalClockImporter(device_state_path, captured_root)
    importer.recover(entries, apply=apply)
    return publish_from_ledger(captured_root, publication_root, device_state_path.parent)


@dataclass(frozen=True, slots=True)
class _Entry:
    boot_id: str
    realtime: float
    monotonic: float
    device_epoch: int
    sequence: int
    source_hash: str
    role: str
    raw_timestamp: int | None
    raw_timestamp_hash: str | None
    operation_id: str | None = None
    event: str = ""
    invocation_id: str | None = None
    source_realtime: float | None = None


def _rows(entries: object) -> tuple[dict[str, object], ...]:
    entries = _decode_export(entries)
    if isinstance(entries, dict):
        entries = [entries]
    if not isinstance(entries, list | tuple):
        raise HistoricalRecoveryError("historical export must be a finite JSON array or JSONL")
    rows = tuple(_expand_row(item) for item in entries)
    return cast(tuple[dict[str, object], ...], rows)


def _decode_export(entries: object) -> object:
    if isinstance(entries, (str, bytes, bytearray)):
        text = bytes(entries).decode() if not isinstance(entries, str) else entries
        try:
            entries = json.loads(text)
        except UnicodeDecodeError, json.JSONDecodeError:
            parsed: list[object] = []
            for line in text.splitlines():
                if not line.strip():
                    continue
                try:
                    parsed.append(json.loads(line))
                except json.JSONDecodeError as error:
                    raise HistoricalRecoveryError("historical export is invalid JSONL") from error
            entries = parsed
    return entries


def _expand_row(item: object) -> dict[str, object]:
    if not isinstance(item, dict):
        raise HistoricalRecoveryError("historical export entries must be objects")
    message = item.get("MESSAGE")
    if not isinstance(message, str):
        return cast(dict[str, object], item)
    try:
        decoded = json.loads(message)
    except json.JSONDecodeError:
        return cast(dict[str, object], item)
    if not isinstance(decoded, dict):
        return cast(dict[str, object], item)
    return {**cast(dict[str, object], decoded), **cast(dict[str, object], item)}


def _entry(row: dict[str, object]) -> _Entry:
    source = _canonical(row)
    row = _normalize_journald(row)
    event, role = _event_and_role(row)
    boot = row.get("boot_id", row.get("_BOOT_ID"))
    if not isinstance(boot, str) or not boot:
        raise HistoricalRecoveryError("historical boot id is invalid")
    realtime, monotonic = _host_times(row)
    epoch = _integer(row.get("device_epoch", row.get("device_time_epoch", row.get("observed_epoch"))))
    sequence = _integer(row.get("sequence", row.get("write_sequence", row.get("boundary_sequence_min"))))
    if epoch is None and role in {"initial", "anchor"}:
        epoch = 0
    if sequence is None and role == "anchor":
        sequence = 0
    if epoch is None or sequence is None or epoch < 0 or sequence < 0:
        raise HistoricalRecoveryError("historical device values are invalid")
    raw_timestamp = _integer(row.get("raw_timestamp", row.get("record_timestamp")))
    raw_hash = row.get("raw_timestamp_hash", row.get("raw_hash", row.get("record_hash")))
    if raw_timestamp is not None and (not isinstance(raw_hash, str) or not _raw_hash_matches(raw_timestamp, raw_hash)):
        raise HistoricalRecoveryError("historical raw timestamp evidence is corrupt")
    operation_id = row.get("operation_id", row.get("clock_operation_id"))
    if operation_id is not None and (not isinstance(operation_id, str) or not operation_id):
        raise HistoricalRecoveryError("historical operation reference is invalid")
    invocation = row.get("invocation_id", row.get("_SYSTEMD_INVOCATION_ID"))
    if invocation is not None and (not isinstance(invocation, str) or not invocation):
        raise HistoricalRecoveryError("historical invocation reference is invalid")
    source_realtime = _number(
        row.get("_SOURCE_REALTIME_TIMESTAMP", row.get("source_realtime")),
        micros="_SOURCE_REALTIME_TIMESTAMP" in row,
    )
    if role == "anchor" and source_realtime is None:
        raise HistoricalRecoveryError("systemd anchor source realtime is invalid")
    return _Entry(
        boot,
        realtime,
        monotonic,
        cast(int, epoch),
        cast(int, sequence),
        sha256(source).hexdigest(),
        role,
        raw_timestamp,
        raw_hash,
        operation_id,
        event,
        invocation,
        source_realtime,
    )


def _normalize_journald(row: dict[str, object]) -> dict[str, object]:
    journald = {"_BOOT_ID", "__REALTIME_TIMESTAMP", "__MONOTONIC_TIMESTAMP", "device_epoch", "write_sequence"}
    if not journald.issubset(row):
        return row
    return {
        **row,
        "boot_id": row["_BOOT_ID"],
        "realtime": _journal_seconds(row["__REALTIME_TIMESTAMP"]),
        "monotonic": _journal_seconds(row["__MONOTONIC_TIMESTAMP"]),
        "device_epoch": row.get("device_epoch", row.get("device_time_epoch")),
        "sequence": row.get("write_sequence", row.get("sequence")),
    }


def _event_and_role(row: dict[str, object]) -> tuple[str, str]:
    event = row.get("event", row.get("EVENT"))
    role = row.get("observation_role", row.get("phase"))
    if event is None and row.get("_SOURCE_REALTIME_TIMESTAMP") is not None:
        event = "systemd_start"
    elif event is None and role == "initial":
        event = "pendant_clock_sync"
    elif event is None and role == "later":
        event = "pendant_observation"
    if not isinstance(event, str):
        raise HistoricalRecoveryError("historical clock event is missing")
    if role is None:
        role = (
            "anchor"
            if event == "systemd_start"
            else "initial"
            if event == "pendant_clock_sync"
            else "later"
            if event == "pendant_observation"
            else None
        )
    if role not in {"initial", "later", "anchor"}:
        raise HistoricalRecoveryError("historical observation role is invalid")
    return event, cast(str, role)


def _host_times(row: dict[str, object]) -> tuple[float, float]:
    realtime_value = row.get("realtime", row.get("__REALTIME_TIMESTAMP", row.get("_SOURCE_REALTIME_TIMESTAMP")))
    monotonic_value = row.get("monotonic", row.get("__MONOTONIC_TIMESTAMP"))
    numbers = (
        _number(realtime_value, micros="realtime" not in row and "__REALTIME_TIMESTAMP" in row),
        _number(monotonic_value, micros="monotonic" not in row and "__MONOTONIC_TIMESTAMP" in row),
    )
    if any(value is None for value in numbers):
        raise HistoricalRecoveryError("historical host clock values are invalid")
    return float(cast(float, numbers[0])), float(cast(float, numbers[1]))


def _validate_mapping(entries: tuple[_Entry, ...], tolerance: float) -> None:
    boots = {item.boot_id for item in entries}
    if len(boots) != 1:
        raise HistoricalRecoveryError("historical entries span multiple host boots")
    offsets = tuple(item.realtime - item.monotonic for item in entries)
    if max(offsets) - min(offsets) > tolerance:
        raise HistoricalRecoveryError("historical realtime-minus-monotonic mapping is discontinuous")
    if any(b.monotonic <= a.monotonic or b.sequence <= a.sequence for a, b in pairwise(entries)):
        raise HistoricalRecoveryError("historical entries are not ordered")


def _prepare_incident(
    entries: tuple[_Entry, ...], operation: ClockCorrection, captured_root: Path
) -> tuple[_Entry, ...]:
    if not any(item.role == "initial" for item in entries):
        candidates = tuple(item for item in entries if item.sequence == operation.boundary_sequence_min)
        if len(candidates) == 1:
            initial = candidates[0]
            entries = tuple(replace(item, role="initial") if item is initial else item for item in entries)
    initial = next((item for item in entries if item.role == "initial"), None)
    if initial is not None:
        entries = tuple(
            replace(item, sequence=initial.sequence - 1) if item.role == "anchor" and initial.sequence > 0 else item
            for item in entries
        )
    return tuple(_prepare_incident_entry(item, operation, captured_root) for item in entries)


def _prepare_incident_entry(item: _Entry, operation: ClockCorrection, captured_root: Path) -> _Entry:
    if item.role == "anchor":
        if item.event != "systemd_start":
            raise HistoricalRecoveryError("historical anchor event is invalid")
        return replace(item, operation_id=operation.operation_id)
    expected_events = (
        {"pendant_clock_sync", "pendant_observation"} if item.role == "initial" else {"pendant_observation"}
    )
    if item.event not in expected_events:
        raise HistoricalRecoveryError("historical entry is not a clock incident observation")
    if item.operation_id is not None and item.operation_id != operation.operation_id:
        raise HistoricalRecoveryError("historical observation is bound to a competing operation")
    if item.role != "initial":
        return replace(item, operation_id=operation.operation_id)
    if item.sequence != operation.boundary_sequence_min:
        raise HistoricalRecoveryError("incident initial boundary does not match the operation")
    timestamp = _raw_timestamp_at(captured_root, item.sequence)
    if timestamp is None:
        raise HistoricalRecoveryError("initial raw boundary is absent or corrupt")
    initial_epoch = item.device_epoch or operation.observed_epoch
    if item.device_epoch and not math.isclose(
        item.device_epoch - item.realtime,
        operation.drift_seconds,
        rel_tol=0.0,
        abs_tol=1.0,
    ):
        raise HistoricalRecoveryError("initial observation does not match the clock operation scale")
    if item.raw_timestamp is not None and (
        item.raw_timestamp != timestamp
        or item.raw_timestamp_hash is None
        or not _raw_hash_matches(timestamp, item.raw_timestamp_hash)
    ):
        raise HistoricalRecoveryError("initial raw timestamp evidence is corrupt")
    return replace(
        item,
        device_epoch=initial_epoch,
        raw_timestamp=timestamp,
        raw_timestamp_hash=sha256(timestamp.to_bytes(4, "big")).hexdigest(),
        operation_id=operation.operation_id,
    )


def _validate_incident(entries: tuple[_Entry, ...], operation: ClockCorrection) -> None:
    _validate_incident_shape(entries, operation)
    anchor = next(item for item in entries if item.role == "anchor")
    initial = next(item for item in entries if item.role == "initial")
    later = next(item for item in entries if item.role == "later")
    _validate_anchor_lineage(anchor, initial)
    if initial.sequence != operation.boundary_sequence_min or later.sequence <= initial.sequence:
        raise HistoricalRecoveryError("incident observation boundaries do not match the operation")
    if len({item.source_hash for item in entries}) != len(entries):
        raise HistoricalRecoveryError("historical incident contains duplicate evidence")


def _validate_legacy_incident(entries: tuple[_Entry, ...], operation: ClockCorrection) -> None:
    roles = [item.role for item in entries]
    if roles.count("initial") != 1 or roles.count("later") != 1:
        raise HistoricalRecoveryError("incident requires initial and later observations")
    if any(item.operation_id != operation.operation_id for item in entries):
        raise HistoricalRecoveryError("historical observation is bound to a competing operation")


def _validate_incident_shape(entries: tuple[_Entry, ...], operation: ClockCorrection) -> None:
    if len(entries) < _MIN_INCIDENT_ENTRIES + 1:
        raise HistoricalRecoveryError("incident requires anchor, initial, and later observations")
    if any(item.operation_id != operation.operation_id for item in entries):
        raise HistoricalRecoveryError("historical observation is bound to a competing operation")
    roles = [item.role for item in entries]
    if roles.count("anchor") != 1 or roles.count("initial") != 1 or roles.count("later") != 1:
        raise HistoricalRecoveryError("incident must contain one anchor, initial, and later observation")
    if roles != ["anchor", "initial", "later"]:
        raise HistoricalRecoveryError("incident observations are not causally ordered")


def _validate_anchor_lineage(anchor: _Entry, initial: _Entry) -> None:
    if (
        anchor.source_realtime is None
        or anchor.invocation_id is None
        or initial.invocation_id != anchor.invocation_id
        or initial.realtime <= anchor.source_realtime
    ):
        raise HistoricalRecoveryError("incident anchor is not before the initial observation")


def _later_observation(observations: tuple[ClockObservation, ...]) -> ClockObservation | None:
    """Select the latest durable observation that can explain a write."""
    by_id = {item.observation_id: item for item in observations}
    candidates = tuple(
        item for item in observations if item.observation_role == "later" and _has_initial_parent(item, by_id)
    )
    if not candidates:
        return None
    return max(candidates, key=lambda item: item.causal_order)


def _has_initial_parent(item: ClockObservation, by_id: dict[str, ClockObservation]) -> bool:
    if item.parent_observation_id is None:
        return False
    parent = by_id.get(item.parent_observation_id)
    return bool(
        parent is not None
        and parent.operation_id in {None, item.operation_id}
        and parent.observation_role in {"initial", "standalone"}
        and parent.causal_order < item.causal_order
    )


def _decide(
    operation: ClockCorrection,
    entries: tuple[_Entry, ...],
    observation: ClockObservation,
    captured_root: Path,
    tolerance: float,
) -> HistoricalRecoveryDecision:
    initial = next(item for item in entries if item.role == "initial")
    later = next(item for item in entries if item.role == "later")
    anchor = next(item for item in entries if item.role == "anchor")
    old_error = abs(initial.device_epoch - ((initial.realtime + initial.realtime) / 2.0))
    target_error = abs(later.device_epoch - ((later.realtime + later.realtime) / 2.0))
    if target_error > tolerance or old_error <= tolerance:
        return HistoricalRecoveryDecision(
            operation.operation_id, "unresolved", None, observation.observation_id, "clock explanation is ambiguous"
        )
    boundary = _exact_boundary(captured_root, operation.boundary_sequence_min, initial.raw_timestamp)
    if (
        boundary is None
        or initial.raw_timestamp is None
        or initial.raw_timestamp >= initial.device_epoch
        or anchor.source_realtime is None
        or initial.device_epoch - initial.raw_timestamp <= initial.realtime - anchor.source_realtime + 1.0
    ):
        return HistoricalRecoveryDecision(
            operation.operation_id,
            "unresolved",
            None,
            observation.observation_id,
            "raw boundary does not prove a pre-write clock anchor",
        )
    return HistoricalRecoveryDecision(
        operation.operation_id,
        "applied",
        boundary,
        observation.observation_id,
        "target-clock explanation and raw boundary agree",
    )


def _decide_native(
    operation: ClockCorrection, observation: ClockObservation, tolerance: float
) -> HistoricalRecoveryDecision:
    target_error = abs(
        observation.device_epoch - ((observation.host_realtime_start + observation.host_realtime_end) / 2.0)
    )
    if target_error > tolerance:
        return HistoricalRecoveryDecision(
            operation.operation_id,
            "unresolved",
            None,
            observation.observation_id,
            "clock observation is not near host time",
        )
    if observation.effective_boundary_sequence == operation.boundary_sequence_min:
        return HistoricalRecoveryDecision(
            operation.operation_id,
            "applied",
            operation.boundary_sequence_min,
            observation.observation_id,
            "zero-width native observation agrees",
        )
    return HistoricalRecoveryDecision(
        operation.operation_id,
        "unresolved",
        None,
        observation.observation_id,
        "native observation has no exact effective boundary",
    )


def _exact_boundary(
    captured_root: Path,
    boundary: int,
    expected_timestamp: int | None = None,
) -> int | None:
    found: dict[int, int] = {}
    if not captured_root.exists() or not captured_root.is_dir():
        return None
    bundles: list[tuple[int, int, bytes]] = []
    for bundle in (item for item in captured_root.iterdir() if item.is_dir()):
        try:
            manifest = json.loads((bundle / "manifest.json").read_text())
            start = manifest["start_sequence"]
            count = manifest["record_count"]
            raw = (bundle / "records.bin").read_bytes()
        except OSError, KeyError, TypeError, json.JSONDecodeError:
            continue
        digest = manifest.get("raw_sha256")
        if (
            not isinstance(start, int)
            or not isinstance(count, int)
            or not isinstance(digest, str)
            or len(raw) != count * RECORD_SIZE
            or sha256(raw).hexdigest() != digest
        ):
            continue
        bundles.append((start, count, raw))
    for start, count, raw in sorted(bundles):
        for index in range(count):
            sequence = start + index
            timestamp = int.from_bytes(raw[index * RECORD_SIZE : index * RECORD_SIZE + 4], "big")
            if sequence in found and found[sequence] != timestamp:
                return None
            found[sequence] = timestamp
    if boundary not in found or (expected_timestamp is not None and found[boundary] != expected_timestamp):
        return None
    return boundary


def _raw_timestamp_at(captured_root: Path, sequence: int) -> int | None:
    """Read one timestamp from a verified raw bundle boundary."""
    if not captured_root.exists() or not captured_root.is_dir():
        return None
    for bundle in (item for item in captured_root.iterdir() if item.is_dir()):
        try:
            manifest = json.loads((bundle / "manifest.json").read_text())
            start = manifest["start_sequence"]
            count = manifest["record_count"]
            raw = (bundle / "records.bin").read_bytes()
        except OSError, KeyError, TypeError, json.JSONDecodeError:
            continue
        digest = manifest.get("raw_sha256")
        if (
            not isinstance(start, int)
            or not isinstance(count, int)
            or not isinstance(digest, str)
            or len(raw) != count * RECORD_SIZE
            or sha256(raw).hexdigest() != digest
        ):
            continue
        if start <= sequence < start + count:
            position = (sequence - start) * RECORD_SIZE
            return int.from_bytes(raw[position : position + 4], "big")
    return None


def _number(value: object, *, micros: bool = False) -> float | None:
    if isinstance(value, bool):
        return None
    try:
        number = float(value)
    except TypeError, ValueError:
        return None
    if not math.isfinite(number):
        return None
    if micros or number > 10**12:
        number /= 1_000_000.0
    return number


def _journal_seconds(value: object) -> float:
    if isinstance(value, bool):
        raise HistoricalRecoveryError("historical journald timestamp is invalid")
    try:
        number = float(value)
    except (TypeError, ValueError) as error:
        raise HistoricalRecoveryError("historical journald timestamp is invalid") from error
    if not math.isfinite(number):
        raise HistoricalRecoveryError("historical journald timestamp is invalid")
    return number / 1_000_000.0


def _entry_as_dict(item: _Entry) -> dict[str, object]:
    return {
        "boot_id": item.boot_id,
        "realtime": item.realtime,
        "monotonic": item.monotonic,
        "device_epoch": item.device_epoch,
        "sequence": item.sequence,
        "source_hash": item.source_hash,
        "role": item.role,
        "raw_timestamp": item.raw_timestamp,
        "raw_timestamp_hash": item.raw_timestamp_hash,
        "operation_id": item.operation_id,
        "event": item.event,
        "invocation_id": item.invocation_id,
        "source_realtime": item.source_realtime,
    }


def _boundary_calculation(entries: tuple[_Entry, ...]) -> dict[str, float | int | None]:
    anchor = next(item for item in entries if item.role == "anchor")
    initial = next(item for item in entries if item.role == "initial")
    delta_max = initial.realtime - float(anchor.source_realtime or 0.0)
    device_raw_delta = None if initial.raw_timestamp is None else initial.device_epoch - initial.raw_timestamp
    return {
        "anchor_source_realtime": anchor.source_realtime,
        "initial_realtime": initial.realtime,
        "delta_max": delta_max,
        "device_raw_delta": device_raw_delta,
        "timestamp_quantization_seconds": 1,
    }


def _read_import_file(path: Path, operation_id: str) -> tuple[_Entry, ...] | None:
    value = _load_import_document(path)
    if value.get("operation_id") != operation_id:
        return None
    entries, hashes = _import_payload(value)
    parsed = tuple(_entry(cast(dict[str, object], item)) for item in entries)
    return _restore_import_hashes(parsed, hashes)


def _load_import_document(path: Path) -> dict[str, object]:
    try:
        raw = path.read_bytes()
        value = json.loads(raw.decode("utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise HistoricalRecoveryError("historical import source is invalid") from error
    if raw != _canonical(value) or not isinstance(value, dict):
        raise HistoricalRecoveryError("historical import source is non-canonical")
    return cast(dict[str, object], value)


def _import_payload(value: dict[str, object]) -> tuple[list[object], list[str]]:
    entries = value.get("entries")
    hashes = value.get("entry_hashes")
    if not isinstance(entries, list) or not isinstance(hashes, list) or len(entries) != len(hashes):
        raise HistoricalRecoveryError("historical import source entries are invalid")
    if any(not isinstance(item, dict) for item in entries) or any(not isinstance(item, str) for item in hashes):
        raise HistoricalRecoveryError("historical import source entries are invalid")
    if any(len(item) != _SHA256_HEX_LENGTH or any(char not in "0123456789abcdef" for char in item) for item in hashes):
        raise HistoricalRecoveryError("historical import source hashes are invalid")
    return entries, [cast(str, item) for item in hashes]


def _restore_import_hashes(parsed: tuple[_Entry, ...], hashes: list[str]) -> tuple[_Entry, ...]:
    if tuple(item.source_hash for item in parsed) == tuple(hashes):
        return parsed
    return tuple(replace(item, source_hash=source_hash) for item, source_hash in zip(parsed, hashes, strict=True))


def _integer(value: object) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, str) and value.strip().lstrip("+").isdigit():
        return int(value)
    return None


def _raw_hash_matches(timestamp: int, value: str) -> bool:
    if len(value) != _SHA256_HEX_LENGTH or any(char not in "0123456789abcdef" for char in value):
        return False
    candidates = (
        timestamp.to_bytes(4, "big"),
        str(timestamp).encode(),
        timestamp.to_bytes(8, "big"),
    )
    return any(sha256(candidate).hexdigest() == value for candidate in candidates)


def _canonical(value: object) -> bytes:
    return (json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":")) + "\n").encode()


def _sync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
