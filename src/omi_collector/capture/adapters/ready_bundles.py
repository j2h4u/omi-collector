"""Finalize raw draft bundles into immutable, flat ready bundles."""

from __future__ import annotations

import errno
import json
import math
import os
import sqlite3
import stat
from bisect import bisect_right
from collections.abc import Iterator, Mapping
from contextlib import closing
from dataclasses import dataclass, replace
from enum import StrEnum
from hashlib import sha256
from itertools import pairwise
from math import ceil
from pathlib import Path
from typing import NoReturn, Protocol, cast

from ...config import ReadyConfig
from ..application.storage_fault import TransientInternalAudioStorageError
from ..domain.internal_audio_machine import (
    QUARANTINE_CUSTODY_HOLD_REASONS,
    AudioAction,
    AudioPhase,
    AuthenticatedConflict,
    BeginRetirement,
    CohortAction,
    CohortConflict,
    CohortEvent,
    CohortPhase,
    CompleteRetire,
    DurableDestination,
    ExistingReadyCustody,
    OwnedIncompleteRetry,
    QualifiedDrained,
    ReadyCustody,
    cohort_transition,
    transition,
)
from ..domain.internal_audio_machine import (
    RetirementStep as RetirementStepEvent,
)
from ..domain.internal_audio_machine import (
    Stage as StageEvent,
)
from ..domain.opus_duration import inspect_20ms_record
from ..domain.ring_protocol import RECORD_SIZE, TIMESTAMP_SIZE
from .clock_segments import ClockSegmentMap
from .collector_state_db import StateDB, StateDBError, StateKind, raise_if_sqlite_corrupt, raise_if_sqlite_storage_fault
from .internal_audio_store import (
    CaptureRecord,
    CaptureSource,
    InternalAudioStore,
    InternalAudioStoreError,
    decode_uint64,
    encode_uint64,
)
from .staging_contract import FatalInternalAudioStorageError


class ReadyBundleError(RuntimeError):
    """A draft cannot be safely finalized into a ready bundle."""


class ReadyDurabilityFatalError(FatalInternalAudioStorageError):
    """A READY durability failure could not be recorded safely."""


class _ObservedDurabilityIOError(OSError):
    """An EIO was observed from a write, flush, or durability barrier."""


class ConflictingOverlapError(ReadyBundleError):
    """Two authenticated drafts claim different bytes for one sequence."""


class _TypedCustodyConflictError(ReadyBundleError):
    """Typed source custody conflicts with immutable ready history."""


class _TypedAdmissionConflictError(ReadyBundleError):
    def __init__(self, source_id: str, message: str) -> None:
        super().__init__(message)
        self.source_id = source_id


@dataclass(frozen=True, slots=True)
class ReadyBundleResult:
    """One ready bundle made durable during this call."""

    bundle_id: str
    path: Path
    next_sequence: int
    record_count: int
    records_sha256: str


class ReadyOutcomeState(StrEnum):
    WAITING = "waiting"
    BLOCKED = "blocked"
    PUBLISHED = "published"
    TRANSIENT = "transient"


@dataclass(frozen=True, slots=True)
class ReadyOutcome:
    state: ReadyOutcomeState
    published: tuple[ReadyBundleResult, ...] = ()
    reason: str | None = None
    remaining_at_frontier: bool = False
    retry_after_seconds: float | None = None
    stats: ReadyPublicationStats | None = None
    custody_cache: ReadyCustodyCache | None = None
    retirement_pending: bool = False


@dataclass(frozen=True, slots=True)
class ReadyPublicationStats:
    """Bounded facts collected while selecting a typed publication cohort."""

    source_count: int = 0
    record_count: int = 0
    audio_packets: int = 0
    threshold_packets: int = 0
    gap_count: int = 0
    gap_records: int = 0


SourceRevision = tuple[tuple[str, int, int, int, int, int, int], ...]


def source_revision(*roots: Path, extras: tuple[Path, ...] = ()) -> SourceRevision:
    """Return cheap filesystem identity for publication inputs, without opening payloads."""
    entries: list[tuple[str, int, int, int, int, int, int]] = []
    for root in (*roots, *extras):
        paths = (root, *root.iterdir()) if root.is_dir() else (root,)
        for path in paths:
            try:
                info = path.lstat()
            except FileNotFoundError:
                continue
            entries.append(
                (str(path), info.st_dev, info.st_ino, info.st_mode, info.st_size, info.st_mtime_ns, info.st_ctime_ns)
            )
            if path.is_dir() and not path.is_symlink():
                for child in path.iterdir():
                    try:
                        child_info = child.lstat()
                    except FileNotFoundError:
                        continue
                    entries.append(
                        (
                            str(child),
                            child_info.st_dev,
                            child_info.st_ino,
                            child_info.st_mode,
                            child_info.st_size,
                            child_info.st_mtime_ns,
                            child_info.st_ctime_ns,
                        )
                    )
    return tuple(sorted(entries))


@dataclass(frozen=True, slots=True)
class _BundleFacts:
    start_sequence: int
    next_sequence: int
    record_count: int
    raw_sha256: str
    gaps: tuple[tuple[int, int], ...] = ()


SourceLineage = tuple[int, int, str]


@dataclass(frozen=True, slots=True)
class _ReadySource:
    """A validated ready bundle used only to compare replayed payloads."""

    result: ReadyBundleResult
    facts: _BundleFacts
    time_ranges: tuple[Mapping[str, object], ...]


_MANIFEST_NAME = "manifest.json"
_RAW_NAME = "records.bin"
_READY_DIRECTORY_MODE = 0o750
_READY_FILE_MODE = 0o640
_UINT32_MAX = (1 << 32) - 1
_SHA256_HEX_LENGTH = 64
_GAP_ENDPOINT_COUNT = 2
_LEDGER_FIELDS = frozenset({"bundles", "frontier"})
_LEDGER_ENTRY_FIELDS = frozenset({"records_sha256", "state", "start_sequence", "next_sequence", "draft_raw_sha256"})
_LEDGER_SOURCE_FIELDS = frozenset({"start_sequence", "next_sequence", "raw_sha256"})
_WINDMILL_CHECKPOINT_FIELDS = frozenset({"analysis_cursor", "vad_decisions", "open_speech_tail", "acknowledged"})
_WINDMILL_TAIL_FIELDS = frozenset({"entries", "opened_at", "outputs"})
_TYPED_PAGE_MAX = 128
_TYPED_SPACE_HEADROOM = 64 * 1024 * 1024
_TYPED_JOURNAL_MARGIN_PAGES = 16
_TYPED_BTREE_PAGES_PER_LEVEL = 3
# A 15-column range row needs at most 31 record-header bytes, nine-byte
# integer serializations, two eight-byte sequence blobs and short scalar text;
# identifiers and clock text are counted separately below. Round that <160-byte
# fixed part up to one explicit 256-byte per-row bound.
_TYPED_METADATA_ROW_BYTES = 256
_TYPED_STAGE_RANGE_COLUMNS = 15
_TYPED_STAGE_IDENTITY_PARTS = 3
_DURABILITY_IO_FAILURE = "durability_io_error"


def _visible_children(root: Path) -> tuple[Path, ...]:
    return tuple(path for path in root.iterdir() if not path.name.startswith("."))


def _ready_start(source: _ReadySource) -> int:
    return source.facts.start_sequence


def _read_ready_source(path: Path) -> _ReadySource:
    if path.is_symlink() or not path.is_dir() or {item.name for item in path.iterdir()} != {_MANIFEST_NAME, _RAW_NAME}:
        raise ReadyBundleError("ready bundle inventory is invalid")
    value = _read_json(path / _MANIFEST_NAME)
    if not isinstance(value, dict) or set(value) != {
        "bundle_id",
        "start_sequence",
        "next_sequence",
        "record_count",
        "record_size",
        "records_sha256",
        "draft_raw_sha256",
        "time_ranges",
    }:
        raise ReadyBundleError("ready manifest is invalid")
    start = _nonnegative_int(value["start_sequence"])
    next_sequence = _nonnegative_int(value["next_sequence"])
    count = _nonnegative_int(value["record_count"])
    bundle_id = _sha256(value["bundle_id"])
    records_sha256 = _sha256(value["records_sha256"])
    draft_raw_sha256 = _sha256(value["draft_raw_sha256"])
    if next_sequence <= start or count == 0 or value["record_size"] != RECORD_SIZE:
        raise ReadyBundleError("ready manifest range is invalid")
    gaps = _validate_ready_ranges(value["time_ranges"], start, next_sequence, count)
    facts = _BundleFacts(start, next_sequence, count, draft_raw_sha256, gaps)
    if bundle_id != _bundle_id(facts) or path.name != bundle_id:
        raise ReadyBundleError("ready manifest identity is invalid")
    raw = path / _RAW_NAME
    if (
        raw.is_symlink()
        or not raw.is_file()
        or raw.stat().st_size != count * RECORD_SIZE
        or _digest(raw) != records_sha256
    ):
        raise ReadyBundleError("ready records do not match manifest")
    ranges = cast(list[dict[str, object]], value["time_ranges"])
    return _ReadySource(ReadyBundleResult(bundle_id, path, next_sequence, count, records_sha256), facts, tuple(ranges))


def _validate_ready_ranges(value: object, start: int, next_sequence: int, count: int) -> tuple[tuple[int, int], ...]:
    if not isinstance(value, list) or not value:
        raise ReadyBundleError("ready manifest time ranges are invalid")
    cursor = start
    total = 0
    gaps: list[tuple[int, int]] = []
    for item in value:
        if not isinstance(item, dict) or set(item) != {"start_sequence", "next_sequence", "utc"}:
            raise ReadyBundleError("ready manifest time ranges are invalid")
        range_start = _nonnegative_int(item["start_sequence"])
        range_next = _nonnegative_int(item["next_sequence"])
        if range_start < cursor or range_next <= range_start or (total == 0 and range_start != start):
            raise ReadyBundleError("ready manifest time ranges are invalid")
        if range_start > cursor:
            gaps.append((cursor, range_start))
        _validate_utc(item["utc"])
        total += range_next - range_start
        cursor = range_next
    if cursor != next_sequence or total != count:
        raise ReadyBundleError("ready manifest time ranges are invalid")
    return tuple(gaps)


def _validate_utc(value: object) -> None:
    if value is None:
        return
    _validate_utc_mapping(value)


def _validate_utc_mapping(value: object) -> None:
    if not isinstance(value, dict):
        raise ReadyBundleError("ready manifest UTC mapping is invalid")
    if set(value) not in (
        {"observation_id", "offset_seconds", "uncertainty_seconds"},
        {"observation_id", "offset_seconds", "uncertainty_seconds", "confidence"},
    ):
        raise ReadyBundleError("ready manifest UTC mapping is invalid")
    _validate_observation_id(value["observation_id"])
    _finite_number(value["offset_seconds"])
    _validate_nonnegative(_finite_number(value["uncertainty_seconds"]))
    if "confidence" in value and value["confidence"] not in ("approximate", "confirmed"):
        raise ReadyBundleError("ready manifest UTC mapping is invalid")


def _validate_observation_id(value: object) -> None:
    if not isinstance(value, str):
        raise ReadyBundleError("ready manifest UTC mapping is invalid")
    if not value:
        raise ReadyBundleError("ready manifest UTC mapping is invalid")


def _finite_number(value: object) -> float:
    if isinstance(value, bool):
        raise ReadyBundleError("ready manifest UTC mapping is invalid")
    if not isinstance(value, (int, float)):
        raise ReadyBundleError("ready manifest UTC mapping is invalid")
    number = float(value)
    if not math.isfinite(number):
        raise ReadyBundleError("ready manifest UTC mapping is invalid")
    return number


def _validate_nonnegative(value: float) -> None:
    if value < 0:
        raise ReadyBundleError("ready manifest UTC mapping is invalid")


def _nonnegative_int(value: object) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ReadyBundleError("ready metadata integer is invalid")
    return value


def _is_nonnegative_int(value: object) -> bool:
    return type(value) is int and value >= 0


def _sha256(value: object) -> str:
    if (
        not isinstance(value, str)
        or len(value) != _SHA256_HEX_LENGTH
        or any(char not in "0123456789abcdef" for char in value)
    ):
        raise ReadyBundleError("ready metadata digest is invalid")
    return value


def _convert_record(record: bytes, time_range: Mapping[str, object]) -> bytes:
    utc = time_range["utc"]
    if utc is None:
        return record
    assert isinstance(utc, Mapping)
    offset = _finite_number(utc["offset_seconds"])
    timestamp = int.from_bytes(record[:TIMESTAMP_SIZE], "big")
    normalized = math.floor(timestamp + offset + 0.5)
    if not 0 <= normalized <= _UINT32_MAX:
        raise ReadyBundleError("normalized timestamp is outside uint32")
    return normalized.to_bytes(TIMESTAMP_SIZE, "big") + record[TIMESTAMP_SIZE:]


def _time_ranges(facts: _BundleFacts, segments: ClockSegmentMap) -> tuple[dict[str, object], ...]:
    ranges: list[dict[str, object]] = []
    for run_start, run_end in _physical_runs(facts):
        boundaries = {run_start, run_end}
        for segment in segments.segments:
            if run_start < segment.next_sequence and segment.start_sequence < run_end:
                boundaries.add(max(run_start, segment.start_sequence))
                boundaries.add(min(run_end, segment.next_sequence))
        ranges.extend(_time_range(start, end, segments) for start, end in pairwise(sorted(boundaries)))
    if (
        not ranges
        or ranges[0]["start_sequence"] != facts.start_sequence
        or ranges[-1]["next_sequence"] != facts.next_sequence
    ):
        raise ReadyBundleError("ready time ranges do not cover the draft")
    return tuple(ranges)


def _time_range(start_sequence: int, next_sequence: int, segments: ClockSegmentMap) -> dict[str, object]:
    segment = next(
        (item for item in segments.segments if item.start_sequence <= start_sequence < item.next_sequence), None
    )
    if segment is None:
        return {"start_sequence": start_sequence, "next_sequence": next_sequence, "utc": None}
    return {
        "start_sequence": start_sequence,
        "next_sequence": next_sequence,
        "utc": {
            "observation_id": segment.observation_id,
            "offset_seconds": segment.utc_offset_seconds,
            "uncertainty_seconds": segment.uncertainty_seconds,
            **({"confidence": segment.confidence} if segment.confidence == "approximate" else {}),
        },
    }


def _ready_manifest(
    source: _BundleFacts, records_sha256: str, ranges: tuple[dict[str, object], ...]
) -> dict[str, object]:
    return {
        "bundle_id": _bundle_id(source),
        "start_sequence": source.start_sequence,
        "next_sequence": source.next_sequence,
        "record_count": source.record_count,
        "record_size": RECORD_SIZE,
        "records_sha256": records_sha256,
        "draft_raw_sha256": source.raw_sha256,
        "time_ranges": list(ranges),
    }


def _ledger_entry(
    records_sha256: str,
    state: str,
    source: _BundleFacts,
    lineage: tuple[SourceLineage, ...] | None = None,
) -> dict[str, object]:
    return {
        "records_sha256": records_sha256,
        "state": state,
        "start_sequence": source.start_sequence,
        "next_sequence": source.next_sequence,
        "draft_raw_sha256": source.raw_sha256,
        **({"gaps": [list(gap) for gap in source.gaps]} if source.gaps else {}),
        **(
            {
                "sources": [
                    {"start_sequence": start, "next_sequence": end, "raw_sha256": raw_sha256}
                    for start, end, raw_sha256 in lineage
                ]
            }
            if lineage is not None
            else {}
        ),
    }


def _same_ledger_identity(left: Mapping[str, object], right: Mapping[str, object]) -> bool:
    return all(left.get(field) == right.get(field) for field in _LEDGER_ENTRY_FIELDS | {"gaps"})


def resume_retired(ready_root: Path, state_db: StateDB) -> None:
    """Finish already committed retirements before reconciling new publication."""
    _prepare_directory(ready_root)
    bundles = _read_ledger(state_db)["bundles"]
    assert isinstance(bundles, dict)
    retired: list[Path] = []
    for bundle_id, entry in bundles.items():
        if isinstance(entry, dict) and entry.get("state") == "retired":
            destination = ready_root / _sha256(bundle_id)
            _resume_retirement(destination, entry)
            retired.append(destination)
    for destination in retired:
        _remove_retired_ready(destination)


def acknowledged_ready(checkpoint_path: Path) -> tuple[tuple[str, str], ...]:
    """Pin one validated consumer ACK snapshot across cleanup and retirement."""
    return _windmill_acknowledged(checkpoint_path)


def retire_acknowledged(
    ready_root: Path,
    state_db: StateDB,
    checkpoint_path: Path,
    *,
    acknowledged: tuple[tuple[str, str], ...] | None = None,
) -> tuple[ReadyBundleResult, ...]:
    """Retire only Windmill bundles durably ACKed outside an open speech tail."""
    _prepare_directory(ready_root)
    if acknowledged is None:
        acknowledged = acknowledged_ready(checkpoint_path)
    if not acknowledged:
        return ()
    ledger = _read_ledger(state_db)
    for bundle_id, records_sha256 in acknowledged:
        _preflight_retirement(ready_root, ledger, bundle_id, records_sha256)
    retired: list[ReadyBundleResult] = []
    for bundle_id, records_sha256 in acknowledged:
        retired.append(_retire_one(ready_root, state_db, ledger, bundle_id, records_sha256))
    return tuple(retired)


def _preflight_retirement(ready_root: Path, ledger: Mapping[str, object], bundle_id: str, records_sha256: str) -> None:
    bundles = ledger["bundles"]
    assert isinstance(bundles, dict)
    entry = bundles.get(bundle_id)
    if not isinstance(entry, dict) or entry.get("records_sha256") != records_sha256:
        raise ReadyBundleError("Windmill acknowledgement is not a published ready bundle")
    destination = ready_root / bundle_id
    if entry.get("state") == "ready":
        _validate_ready_ledger_entry(entry, _read_ready_source(destination))
        return
    if entry.get("state") == "retired":
        _resume_retirement(destination, entry)
        return
    raise ReadyBundleError("ready publication ledger is invalid")


def _retire_one(
    ready_root: Path, state_db: StateDB, ledger: dict[str, object], bundle_id: str, records_sha256: str
) -> ReadyBundleResult:
    bundles = ledger["bundles"]
    assert isinstance(bundles, dict)
    entry = bundles.get(bundle_id)
    if not isinstance(entry, dict) or entry.get("records_sha256") != records_sha256:
        raise ReadyBundleError("Windmill acknowledgement is not a published ready bundle")
    destination = ready_root / bundle_id
    state = entry.get("state")
    if state == "ready":
        source = _read_ready_source(destination)
        _validate_ready_ledger_entry(entry, source)
        lineage = source_lineage(entry) if "sources" in entry else None
        retired_entry = _ledger_entry(records_sha256, "retired", source.facts, lineage)
        try:
            with state_db.transaction() as transaction:
                transaction.put(StateKind.READY_PUBLICATION, bundle_id, retired_entry)
        except (OSError, ValueError, StateDBError) as error:
            raise_if_sqlite_storage_fault(error)
            raise ReadyBundleError("ready retirement could not be recorded") from error
        bundles[bundle_id] = retired_entry
    elif state == "retired":
        _resume_retirement(destination, entry)
    else:
        raise ReadyBundleError("ready publication ledger is invalid")
    _remove_retired_ready(destination)
    return ReadyBundleResult(
        bundle_id,
        destination,
        _entry_next_sequence(bundles[bundle_id]),
        _entry_count(bundles[bundle_id]),
        records_sha256,
    )


def _resume_retirement(destination: Path, entry: Mapping[str, object]) -> None:
    entries = _retired_ready_entries(destination)
    if entries is None:
        return
    if set(entries) == {_MANIFEST_NAME, _RAW_NAME}:
        _validate_ready_ledger_entry(entry, _read_ready_source(destination))


def _validate_ready_ledger_entry(entry: Mapping[str, object], source: _ReadySource) -> None:
    if entry.get("records_sha256") != source.result.records_sha256:
        raise ReadyBundleError("ready publication ledger digest conflicts with ready bundle")
    if (
        entry.get("start_sequence") != source.facts.start_sequence
        or entry.get("next_sequence") != source.facts.next_sequence
        or entry.get("draft_raw_sha256") != source.facts.raw_sha256
        or _entry_gaps(entry) != source.facts.gaps
    ):
        raise ReadyBundleError("ready publication ledger range conflicts with ready bundle")


def _entry_next_sequence(entry: object) -> int:
    if not isinstance(entry, dict):
        raise ReadyBundleError("ready publication ledger is invalid")
    return _nonnegative_int(entry["next_sequence"])


def _entry_count(entry: object) -> int:
    if not isinstance(entry, dict):
        raise ReadyBundleError("ready publication ledger is invalid")
    return (
        _entry_next_sequence(entry)
        - _nonnegative_int(entry["start_sequence"])
        - sum(end - start for start, end in _entry_gaps(entry))
    )


def _remove_retired_ready(path: Path) -> None:
    entries = _retired_ready_entries(path)
    if entries is None:
        return
    for name in (_RAW_NAME, _MANIFEST_NAME):
        entry = entries.get(name)
        if entry is None:
            continue
        if entry.is_symlink() or not entry.is_file():
            raise ReadyBundleError("retired ready bundle contains an unsafe entry")
        entry.unlink()
    path.rmdir()
    _sync_directory(path.parent)


def _retired_ready_entries(path: Path) -> dict[str, Path] | None:
    if not os.path.lexists(path):
        return None
    if path.is_symlink() or not path.is_dir():
        raise ReadyBundleError("retired ready bundle is unsafe")
    entries = {item.name: item for item in path.iterdir()}
    if not set(entries) <= {_MANIFEST_NAME, _RAW_NAME}:
        raise ReadyBundleError("retired ready bundle inventory is unsafe")
    if any(item.is_symlink() or not item.is_file() for item in entries.values()):
        raise ReadyBundleError("retired ready bundle contains an unsafe entry")
    return entries


def _windmill_acknowledged(checkpoint_path: Path) -> tuple[tuple[str, str], ...]:
    if not checkpoint_path.exists():
        return ()
    value = _read_json(checkpoint_path)
    if not isinstance(value, dict) or set(value) != _WINDMILL_CHECKPOINT_FIELDS:
        raise ReadyBundleError("Windmill ready checkpoint is invalid")
    if not isinstance(value["vad_decisions"], list):
        raise ReadyBundleError("Windmill ready checkpoint is invalid")
    tail = _windmill_tail_identities(value["open_speech_tail"])
    acknowledged = _windmill_identities(value["acknowledged"], "acknowledged")
    if len(acknowledged) != len(set(acknowledged)):
        raise ReadyBundleError("Windmill acknowledgement is duplicated")
    if set(acknowledged) & tail:
        raise ReadyBundleError("Windmill acknowledgement conflicts with pending tail")
    return tuple(acknowledged)


def _windmill_tail_identities(value: object) -> set[tuple[str, str]]:
    if value is None:
        return set()
    if not isinstance(value, dict) or set(value) != _WINDMILL_TAIL_FIELDS:
        raise ReadyBundleError("Windmill pending tail is invalid")
    if (
        isinstance(value["opened_at"], bool)
        or not isinstance(value["opened_at"], int)
        or not isinstance(value["outputs"], list)
    ):
        raise ReadyBundleError("Windmill pending tail is invalid")
    entries = value["entries"]
    if not isinstance(entries, list):
        raise ReadyBundleError("Windmill pending tail is invalid")
    identities: set[tuple[str, str]] = set()
    for entry in entries:
        if not isinstance(entry, dict) or not {"bundle_id", "records_sha256"} <= set(entry):
            raise ReadyBundleError("Windmill pending tail identities are invalid")
        identities.add((_sha256(entry["bundle_id"]), _sha256(entry["records_sha256"])))
    return identities


def _windmill_identities(value: object, label: str) -> list[tuple[str, str]]:
    if not isinstance(value, list):
        raise ReadyBundleError(f"Windmill {label} is invalid")
    identities: list[tuple[str, str]] = []
    for item in value:
        if not isinstance(item, dict) or set(item) != {"bundle_id", "records_sha256"}:
            raise ReadyBundleError(f"Windmill {label} is invalid")
        identities.append((_sha256(item["bundle_id"]), _sha256(item["records_sha256"])))
    return identities


def _read_ledger(state_db: StateDB) -> dict[str, object]:
    try:
        rows = state_db.list(StateKind.READY_PUBLICATION)
    except (OSError, ValueError, StateDBError) as error:
        raise_if_sqlite_storage_fault(error)
        raise ReadyBundleError("ready publication ledger is unreadable") from error
    bundles: dict[str, object] = {}
    frontier = 0
    for bundle_id, entry in rows:
        if bundle_id == "frontier":
            if set(entry) != {"value"}:
                raise ReadyBundleError("ready publication ledger is invalid")
            value = entry["value"]
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise ReadyBundleError("ready publication ledger is invalid")
            frontier = value
            continue
        bundles[bundle_id] = entry
    _validate_ledger_values(bundles, frontier)
    _validate_ledger_entries(bundles)
    return {"bundles": bundles, "frontier": frontier}


def _validate_ledger_values(bundles: object, frontier: object) -> None:
    if not isinstance(bundles, dict):
        raise ReadyBundleError("ready publication ledger is invalid")
    if isinstance(frontier, bool) or not isinstance(frontier, int) or frontier < 0:
        raise ReadyBundleError("ready publication ledger is invalid")


def _validate_ledger_entries(bundles: Mapping[str, object]) -> None:
    for bundle_id, entry in bundles.items():
        if not _valid_ledger_entry(bundle_id, entry):
            raise ReadyBundleError("ready publication ledger is invalid")


def _valid_ledger_entry(bundle_id: object, entry: object) -> bool:
    if not isinstance(bundle_id, str) or not isinstance(entry, dict):
        return False
    if not _is_sha256(bundle_id) or not _is_sha256(entry.get("records_sha256")):
        return False
    start, end = entry.get("start_sequence"), entry.get("next_sequence")
    if (
        set(entry)
        not in (
            _LEDGER_ENTRY_FIELDS,
            _LEDGER_ENTRY_FIELDS | {"gaps"},
            _LEDGER_ENTRY_FIELDS | {"sources"},
            _LEDGER_ENTRY_FIELDS | {"gaps", "sources"},
        )
        or entry.get("state") not in {"ready", "retired"}
        or not _valid_range(start, end)
        or not _is_sha256(entry.get("draft_raw_sha256"))
    ):
        return False
    assert isinstance(start, int) and isinstance(end, int)
    gaps = _parse_ledger_gaps(entry, start, end)
    if gaps is None:
        return False
    digest = entry["draft_raw_sha256"]
    assert isinstance(digest, str)
    facts = _BundleFacts(start, end, end - start - sum(b - a for a, b in gaps), digest, gaps)
    return bundle_id == _bundle_id(facts) and (
        "sources" not in entry or _parse_sources(entry["sources"], facts) is not None
    )


def _parse_ledger_gaps(entry: Mapping[str, object], start: int, end: int) -> tuple[tuple[int, int], ...] | None:
    if "gaps" not in entry:
        return ()
    value = entry["gaps"]
    if not isinstance(value, list) or not value:
        return None
    gaps: list[tuple[int, int]] = []
    cursor = start
    for item in value:
        if not isinstance(item, list) or len(item) != _GAP_ENDPOINT_COUNT:
            return None
        left, right = item
        if not _valid_range(left, right) or left <= cursor or right >= end:
            return None
        assert isinstance(left, int) and isinstance(right, int)
        gaps.append((left, right))
        cursor = right
    return tuple(gaps)


def source_lineage(entry: Mapping[str, object]) -> tuple[SourceLineage, ...]:
    """Read and strictly validate source slices from a ready ledger entry."""
    if "sources" not in entry:
        return ()
    start, end, digest = entry.get("start_sequence"), entry.get("next_sequence"), entry.get("draft_raw_sha256")
    if not _valid_range(start, end) or not _is_sha256(digest):
        raise ReadyBundleError("ready publication ledger source lineage is invalid")
    assert isinstance(start, int) and isinstance(end, int) and isinstance(digest, str)
    gaps = _parse_ledger_gaps(entry, start, end)
    if gaps is None:
        raise ReadyBundleError("ready publication ledger source lineage is invalid")
    facts = _BundleFacts(start, end, end - start - sum(b - a for a, b in gaps), digest, gaps)
    lineage = _parse_sources(entry["sources"], facts)
    if lineage is None:
        raise ReadyBundleError("ready publication ledger source lineage is invalid")
    return lineage


def _parse_sources(value: object, facts: _BundleFacts) -> tuple[SourceLineage, ...] | None:
    if not isinstance(value, list) or not value:
        return None
    sources: list[SourceLineage] = []
    previous_end: int | None = None
    for item in value:
        if not isinstance(item, dict) or set(item) != _LEDGER_SOURCE_FIELDS:
            return None
        start, end, digest = item.get("start_sequence"), item.get("next_sequence"), item.get("raw_sha256")
        if not _valid_range(start, end) or not _is_sha256(digest):
            return None
        assert isinstance(start, int) and isinstance(end, int) and isinstance(digest, str)
        if previous_end is not None and start < previous_end:
            return None
        sources.append((start, end, digest))
        previous_end = end
    if not _sources_cover_runs(tuple(sources), facts):
        return None
    return tuple(sources)


def _sources_cover_runs(sources: tuple[SourceLineage, ...], facts: _BundleFacts) -> bool:
    runs = _physical_runs(facts)
    run_index = 0
    cursor = runs[0][0]
    for start, end, _ in sources:
        while run_index < len(runs) and cursor == runs[run_index][1]:
            run_index += 1
            if run_index < len(runs):
                cursor = runs[run_index][0]
        if run_index == len(runs) or start != cursor or end > runs[run_index][1]:
            return False
        cursor = end
    return run_index == len(runs) - 1 and cursor == runs[-1][1]


def _entry_gaps(entry: Mapping[str, object]) -> tuple[tuple[int, int], ...]:
    start, end = entry["start_sequence"], entry["next_sequence"]
    assert isinstance(start, int) and isinstance(end, int)
    gaps = _parse_ledger_gaps(entry, start, end)
    assert gaps is not None
    return gaps


def _entry_facts(entry: Mapping[str, object]) -> _BundleFacts:
    start, end, digest = entry["start_sequence"], entry["next_sequence"], entry["draft_raw_sha256"]
    assert isinstance(start, int) and isinstance(end, int) and isinstance(digest, str)
    gaps = _entry_gaps(entry)
    return _BundleFacts(start, end, end - start - sum(b - a for a, b in gaps), digest, gaps)


def _is_sha256(value: object) -> bool:
    return (
        isinstance(value, str)
        and len(value) == _SHA256_HEX_LENGTH
        and all(char in "0123456789abcdef" for char in value)
    )


def _valid_range(start: object, next_sequence: object) -> bool:
    return (
        not isinstance(start, bool)
        and isinstance(start, int)
        and start >= 0
        and not isinstance(next_sequence, bool)
        and isinstance(next_sequence, int)
        and next_sequence > start
    )


def _physical_runs(facts: _BundleFacts) -> tuple[tuple[int, int], ...]:
    runs: list[tuple[int, int]] = []
    cursor = facts.start_sequence
    for start, end in facts.gaps:
        runs.append((cursor, start))
        cursor = end
    runs.append((cursor, facts.next_sequence))
    return tuple(runs)


def _bundle_id(facts: _BundleFacts) -> str:
    identity = (
        f"{facts.start_sequence}:{facts.next_sequence}:{facts.raw_sha256}"
        + "".join(f":{start}-{end}" for start, end in facts.gaps)
    ).encode()
    return sha256(identity).hexdigest()


def _prepare_directory(path: Path) -> None:
    path.mkdir(mode=_READY_DIRECTORY_MODE, parents=True, exist_ok=True)
    if path.is_symlink() or not path.is_dir():
        raise ReadyBundleError("ready storage root is unsafe")


def _require_shared_ready_directory(path: Path) -> None:
    mode = path.stat().st_mode
    required = stat.S_ISGID | stat.S_IRGRP | stat.S_IXGRP
    if mode & required != required:
        raise ReadyBundleError("ready directory is not group-readable and setgid")


def _write_file(path: Path, payload: bytes) -> None:
    with path.open("xb") as stream:
        try:
            stream.write(payload)
        except OSError as error:
            _raise_if_observed_durability_eio(error)
        os.fchmod(stream.fileno(), _READY_FILE_MODE)
        try:
            stream.flush()
        except OSError as error:
            _raise_if_observed_durability_eio(error)
    _sync_regular_file(path)


def _read_json(path: Path) -> object:
    if path.is_symlink() or not path.is_file():
        raise ReadyBundleError("ready metadata is not a regular file")
    try:
        return cast(object, json.loads(path.read_text(encoding="utf-8")))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ReadyBundleError("ready metadata is invalid") from error


def _canonical(value: object) -> bytes:
    return json.dumps(value, separators=(",", ":"), sort_keys=True, allow_nan=False).encode()


def _digest(path: Path) -> str:
    if path.is_symlink() or not path.is_file():
        raise ReadyBundleError("ready records are not a regular file")
    digest = sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _sync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC)
    try:
        try:
            os.fsync(descriptor)
        except OSError as error:
            _raise_if_observed_durability_eio(error)
    finally:
        os.close(descriptor)


def _raise_if_observed_durability_eio(error: OSError) -> NoReturn:
    if error.errno == errno.EIO:
        raise _ObservedDurabilityIOError(error.errno, "READY durability I/O error") from error
    raise error


# The SQL projector shares the public ready contract with the legacy projector,
# but all private source custody and crash recovery live in typed SQLite rows.
class TypedPublicationSource(Protocol):
    @property
    def source(self) -> CaptureSource: ...

    def source_page(self, after_ordinal: int = -1, limit: int = 128) -> object: ...


@dataclass(frozen=True, slots=True)
class _TypedPage:
    source: CaptureSource
    after_ordinal: int
    records: tuple[CaptureRecord, ...]


@dataclass(frozen=True, slots=True)
class SqlitePublicationSource:
    store: InternalAudioStore
    source_id: str

    @property
    def source(self) -> CaptureSource:
        source = self.store.get_source(self.source_id)
        if source is None:
            raise ReadyBundleError("typed publication source disappeared")
        return source

    def source_page(self, after_ordinal: int = -1, limit: int = 128) -> _TypedPage:
        source = self.source
        records = self.store.read_record_page(self.source_id, after_ordinal, limit)
        if self.source != source:
            raise ReadyBundleError("typed publication source changed during page read")
        return _TypedPage(source, after_ordinal, records)


@dataclass(frozen=True, slots=True)
class _TypedAdmission:
    reader: TypedPublicationSource
    snapshot: CaptureSource
    admitted_lifecycle: str
    admitted_count: int
    admitted_sha256: str
    cursor: int = 0
    member_ordinal: int | None = None


@dataclass(frozen=True, slots=True)
class _TypedSlice:
    admission: _TypedAdmission
    start_ordinal: int
    record_count: int
    start_sequence: int
    next_sequence: int
    raw_sha256: str
    first_size: int
    final_overflow_size: int | None
    audio_packets: int
    continuity_breaks: tuple[int, ...]


@dataclass(frozen=True, slots=True)
class _TypedGroup:
    slices: tuple[_TypedSlice, ...]
    facts: _BundleFacts
    ranges: tuple[dict[str, object], ...]
    output_sha256: str


@dataclass(frozen=True, slots=True)
class _TypedReadyCustody:
    active: tuple[_ReadySource, ...]
    entries: dict[str, dict[str, object]]


@dataclass(frozen=True, slots=True)
class ReadyCustodyCache:
    """Authenticated ready custody reusable while its complete identity is unchanged."""

    revision: SourceRevision
    custody: _TypedReadyCustody


@dataclass(frozen=True, slots=True)
class _TypedCohort:
    cohort_id: str
    lifecycle: str
    threshold: int
    frontier: int | None
    cursor: int
    config_digest: str
    admissions: tuple[_TypedAdmission, ...]


@dataclass(frozen=True, slots=True)
class _StageRange:
    slice: _TypedSlice
    time_range: dict[str, object]
    time_range_ordinal: int
    device_start: int
    device_end: int
    utc_start: int | None
    utc_end: int | None


@dataclass(frozen=True, slots=True)
class _StageRangeRow:
    ordinal: int
    time_range_ordinal: int
    source_id: str
    start: int
    count: int
    sequence_start: bytes
    next_sequence: bytes
    device_start: object
    device_end: object
    utc_start: object
    utc_end: object
    observation: object
    offset: object
    uncertainty: object
    confidence: object


@dataclass(frozen=True, slots=True)
class _TypedStage:
    stage_id: str
    group_identity: str
    path: Path
    group: _TypedGroup
    rows: tuple[_StageRange, ...]
    owner_token: str


@dataclass(frozen=True, slots=True)
class _MemberProgress:
    member_ordinal: int
    old_cursor: int
    new_cursor: int


@dataclass(frozen=True, slots=True)
class _SourceCoverage:
    source_id: str
    old: tuple[object, ...]
    proposed: tuple[object, ...]


@dataclass(frozen=True, slots=True)
class _ConflictDisposition:
    admission: _TypedAdmission
    cohort: _TypedCohort | None
    proposed_cohort: _TypedCohort | None
    old: tuple[object, ...]
    proposed: tuple[object, ...]
    accepted_time: str | None


@dataclass(frozen=True, slots=True)
class _StageKey:
    stage_id: str
    group_identity: str
    path: Path


@dataclass(frozen=True, slots=True)
class _FinalCommit:
    cohort: _TypedCohort
    stage: _TypedStage
    result: ReadyBundleResult
    entry: dict[str, object]
    progress: tuple[_MemberProgress, ...]
    coverage: tuple[_SourceCoverage, ...]
    all_complete: bool
    ledger: dict[str, object]


@dataclass(frozen=True, slots=True)
class _CustodyCommit:
    cohort: _TypedCohort
    proposed: _TypedCohort
    progress: tuple[_MemberProgress, ...]
    coverage: tuple[_SourceCoverage, ...]
    proofs: dict[str, dict[str, object]]
    all_complete: bool


@dataclass(frozen=True, slots=True)
class _RetirementStep:
    source_id: str
    committed_count: int
    coverage_digest: str
    old_cursor: int
    page_limit: int


@dataclass(frozen=True, slots=True)
class _RetirementResult:
    cursor: int | None
    progressed: bool


@dataclass(frozen=True, slots=True)
class _StageHeader:
    expected_digest: str
    expected_count: int
    frozen_device_start: int
    frozen_device_end: int
    frozen_utc_start: int | None
    frozen_utc_end: int | None
    mapping_revision: str
    owner_token: str
    failure_reason: str | None


def _sql_one(cursor: sqlite3.Cursor) -> tuple[object, ...] | None:
    try:
        return cast(tuple[object, ...] | None, cursor.fetchone())
    finally:
        cursor.close()


def _sql_all(cursor: sqlite3.Cursor) -> tuple[tuple[object, ...], ...]:
    try:
        return tuple(cast(list[tuple[object, ...]], cursor.fetchall()))
    finally:
        cursor.close()


def _require_cohort_decision(
    phase: CohortPhase,
    event: CohortEvent,
    action: CohortAction,
    next_phase: CohortPhase | None,
) -> None:
    decision = cohort_transition(phase, event)
    if decision.action is not action or decision.next_phase is not next_phase:
        raise ReadyBundleError(f"publication cohort FSM refused {type(event).__name__}")


def _require_audio_decision(
    phase: AudioPhase,
    event: ReadyCustody | BeginRetirement | RetirementStepEvent | AuthenticatedConflict,
    action: AudioAction,
    next_phase: AudioPhase | None,
) -> None:
    decision = transition(phase, event)
    if decision.action is not action or decision.next_phase is not next_phase:
        raise ReadyBundleError(f"capture source FSM refused {type(event).__name__}")


def finalize_typed_sources(  # noqa: PLR0913 - publication boundary keeps storage and policy inputs explicit
    audio_store: InternalAudioStore,
    sources: tuple[TypedPublicationSource, ...] | list[TypedPublicationSource],
    ready_root: Path,
    *,
    config: ReadyConfig,
    clock_segments: ClockSegmentMap,
    frontier: int | None = None,
    drained: bool = False,
    page_limit: int = 128,
    custody_cache: ReadyCustodyCache | None = None,
) -> ReadyOutcome:
    """Publish or resume exactly one durable SQL cohort."""
    with audio_store.publication_reader():
        return _finalize_typed_sources(
            audio_store,
            sources,
            ready_root,
            config=config,
            clock_segments=clock_segments,
            frontier=frontier,
            drained=drained,
            page_limit=page_limit,
            custody_cache=custody_cache,
        )


def _finalize_typed_sources(  # noqa: PLR0913 - publication boundary keeps storage and policy inputs explicit
    audio_store: InternalAudioStore,
    sources: tuple[TypedPublicationSource, ...] | list[TypedPublicationSource],
    ready_root: Path,
    *,
    config: ReadyConfig,
    clock_segments: ClockSegmentMap,
    frontier: int | None,
    drained: bool,
    page_limit: int,
    custody_cache: ReadyCustodyCache | None,
) -> ReadyOutcome:
    _validate_typed_request(frontier, drained, page_limit)
    _prepare_directory(ready_root)
    _require_shared_ready_directory(ready_root)
    threshold = ceil(config.target_audio_seconds * 50)
    config_digest = sha256(f"target_audio_seconds={config.target_audio_seconds.hex()}".encode()).hexdigest()
    cohort = _load_active_typed_cohort(audio_store)
    stats: ReadyPublicationStats | None = None
    prepared_slices: tuple[_TypedSlice, ...] | None = None
    if cohort is None:
        custody_cache = _typed_ready_custody(ready_root, audio_store.state_db, custody_cache)
        custody = custody_cache.custody
        admissions = _typed_source_admissions(audio_store, sources, frontier)
        if drained:
            admissions = _cover_typed_full_replays(audio_store, admissions, custody, page_limit)
        try:
            slices = _normalize_typed_admissions(admissions, page_limit, custody)
        except _TypedAdmissionConflictError as error:
            admission = next(item for item in admissions if item.snapshot.source_id == error.source_id)
            _block_typed_custody_conflict(audio_store, admission)
            raise
        _require_typed_admissions_current(audio_store, admissions)
        stats = _typed_stats(admissions, slices, threshold)
        if not drained:
            retirement_pending = _retire_one_typed_source(audio_store, page_limit)
            return ReadyOutcome(
                ReadyOutcomeState.WAITING,
                reason="waiting_for_drain",
                remaining_at_frontier=bool(admissions),
                stats=stats,
                custody_cache=custody_cache,
                retirement_pending=retirement_pending,
            )
        if not slices or stats.audio_packets < threshold:
            retirement_pending = _retire_one_typed_source(audio_store, page_limit)
            return ReadyOutcome(
                ReadyOutcomeState.WAITING,
                reason="waiting_for_threshold",
                remaining_at_frontier=bool(admissions),
                stats=stats,
                custody_cache=custody_cache,
                retirement_pending=retirement_pending,
            )
        cohort = _create_typed_cohort(audio_store, admissions, threshold, frontier, config_digest)
        prepared_slices = _rebind_typed_slices(slices, cohort.admissions)
    published, custody_cache = _resume_typed_cohort(
        audio_store,
        ready_root,
        cohort,
        clock_segments,
        page_limit,
        custody_cache=custody_cache,
        prepared_slices=prepared_slices,
    )
    retirement_pending = _retire_one_typed_source(audio_store, page_limit)
    return ReadyOutcome(
        ReadyOutcomeState.PUBLISHED if published else ReadyOutcomeState.WAITING,
        tuple(published),
        None if published else "already_published",
        bool(cohort.admissions) and not published,
        stats=stats,
        custody_cache=custody_cache,
        retirement_pending=retirement_pending,
    )


publish_typed_sources = finalize_typed_sources
finalize_sql_sources = finalize_typed_sources


def _validate_typed_request(frontier: int | None, drained: bool, page_limit: int) -> None:
    if type(page_limit) is not int or not 0 < page_limit <= _TYPED_PAGE_MAX:
        raise ReadyBundleError("typed publication page limit is invalid")
    if frontier is not None and (type(frontier) is not int or not 0 <= frontier < 2**64):
        raise ReadyBundleError("typed publication frontier is invalid")
    if drained and frontier is None:
        raise ReadyBundleError("a drained typed publication requires its durable frontier")


def _typed_source_admissions(
    audio_store: InternalAudioStore,
    sources: tuple[TypedPublicationSource, ...] | list[TypedPublicationSource],
    frontier: int | None,
) -> tuple[_TypedAdmission, ...]:
    supplied: dict[str, tuple[TypedPublicationSource, CaptureSource]] = {}
    for reader in sources:
        snapshot = reader.source
        if snapshot.source_id in supplied:
            raise ReadyBundleError("typed publication source is duplicated")
        supplied[snapshot.source_id] = (reader, snapshot)
    admissions: list[_TypedAdmission] = []
    try:
        with (
            closing(audio_store.state_db.open_connection(read_only=True)) as connection,
            audio_store.state_db.access(connection),
        ):
            for source_id, (reader, supplied_snapshot) in supplied.items():
                current = audio_store.get_source(source_id)
                if current is None or current != supplied_snapshot:
                    raise ReadyBundleError("typed publication source snapshot changed")
                row = cast(
                    tuple[object, ...] | None,
                    _sql_one(
                        connection.execute(
                            "SELECT certified_prefix_count, coverage_digest, block_reason, "
                            "quarantine_disposition, terminal_time, retention_deadline "
                            "FROM capture_source WHERE source_id=?",
                            (source_id,),
                        )
                    ),
                )
                if row is None:
                    raise ReadyBundleError("typed publication source disappeared")
                admitted = _typed_admission(reader, current, row)
                if admitted is None:
                    continue
                if frontier is not None and current.sequence_start + admitted.admitted_count > frontier:
                    continue
                admissions.append(admitted)
    except (OSError, StateDBError, sqlite3.DatabaseError) as error:
        raise_if_sqlite_storage_fault(error)
        raise ReadyBundleError("typed publication source state is unreadable") from error
    return tuple(sorted(admissions, key=lambda item: (item.snapshot.sequence_start, item.snapshot.source_id)))


def _typed_admission(
    reader: TypedPublicationSource, snapshot: CaptureSource, certification: tuple[object, ...]
) -> _TypedAdmission | None:
    if snapshot.source_type not in {"attempt", "draft", "quarantine"}:
        raise ReadyBundleError("typed publication source type is invalid")
    if snapshot.lifecycle == "sealed_waiting":
        count, digest = snapshot.committed_count, snapshot.committed_sha256
    elif snapshot.lifecycle == "blocked":
        certified = _blocked_typed_certification(snapshot, certification)
        if certified is None:
            return None
        count, digest = certified
    else:
        return None
    if count <= 0 or snapshot.sequence_start + count > snapshot.sequence_limit:
        raise ReadyBundleError("typed publication source checkpoint is invalid")
    return _TypedAdmission(reader, snapshot, snapshot.lifecycle, count, digest)


def _blocked_typed_certification(snapshot: CaptureSource, certification: tuple[object, ...]) -> tuple[int, str] | None:
    count, digest, block_reason, disposition, terminal_time, retention_deadline = certification
    if block_reason in QUARANTINE_CUSTODY_HOLD_REASONS:
        return None
    if snapshot.source_type != "quarantine":
        raise ReadyBundleError("blocked typed publication source lacks quarantine custody")
    if disposition in {None, "unclassified", "unprocessable"}:
        return None
    if disposition != "published_prefix":
        raise ReadyBundleError("blocked typed publication source disposition is invalid")
    if not _valid_quarantine_retention(terminal_time, retention_deadline):
        raise ReadyBundleError("blocked typed publication source retention evidence is incomplete")
    if type(count) is not int or count <= 0 or not _is_sha256(digest):
        return None
    assert isinstance(count, int) and isinstance(digest, str)
    if count > snapshot.committed_count:
        raise ReadyBundleError("typed certified prefix exceeds its source")
    return count, digest


def _valid_quarantine_retention(terminal_time: object, retention_deadline: object) -> bool:
    if not isinstance(terminal_time, str) or not isinstance(retention_deadline, str):
        return False
    try:
        terminal = int(terminal_time)
        deadline = int(retention_deadline)
    except ValueError:
        return False
    return 0 <= terminal <= deadline


def _typed_records(
    admission: _TypedAdmission, start_ordinal: int, count: int, page_limit: int
) -> Iterator[CaptureRecord]:
    if start_ordinal < 0 or count < 0 or start_ordinal + count > admission.admitted_count:
        raise ReadyBundleError("typed source slice is outside its admitted prefix")
    cursor = start_ordinal - 1
    end = start_ordinal + count
    while cursor < end - 1:
        requested = min(page_limit, end - cursor - 1)
        page = admission.reader.source_page(cursor, requested)
        page_source = getattr(page, "source", None)
        page_after = getattr(page, "after_ordinal", None)
        records = getattr(page, "records", None)
        if not isinstance(page_source, CaptureSource) or page_after != cursor or not isinstance(records, tuple):
            raise ReadyBundleError("typed source page is invalid")
        _require_typed_snapshot(page_source, admission)
        if not records or len(records) > requested:
            raise ReadyBundleError("typed source page ended before its frozen slice")
        for record in records:
            if not isinstance(record, CaptureRecord) or record.source_id != admission.snapshot.source_id:
                raise ReadyBundleError("typed source record is invalid")
            if record.ordinal != cursor + 1 or record.ordinal >= end:
                raise ReadyBundleError("typed source page range is invalid")
            yield record
            cursor = record.ordinal


def _typed_ready_custody(
    ready_root: Path,
    state_db: StateDB,
    previous: ReadyCustodyCache | None = None,
) -> ReadyCustodyCache:
    ledger = _read_ledger(state_db)
    bundles = cast(dict[str, object], ledger["bundles"])
    entries = {bundle_id: cast(dict[str, object], entry) for bundle_id, entry in bundles.items()}
    revision = source_revision(ready_root)
    if previous is not None and previous.revision == revision and previous.custody.entries == entries:
        return previous
    active_ids = {bundle_id for bundle_id, entry in entries.items() if entry["state"] == "ready"}
    paths = tuple(sorted(_visible_children(ready_root), key=lambda path: path.name))
    if {path.name for path in paths} != active_ids:
        raise ReadyBundleError("typed ready files do not match the active publication ledger")
    active = tuple(sorted((_read_ready_source(path) for path in paths), key=_ready_start))
    for source in active:
        _validate_ready_ledger_entry(entries[source.result.bundle_id], source)
    if revision != source_revision(ready_root) or entries != cast(dict[str, object], _read_ledger(state_db)["bundles"]):
        raise ReadyBundleError("typed ready custody changed during authentication")
    return ReadyCustodyCache(revision, _TypedReadyCustody(active, entries))


def _typed_ready_overlap(
    admission: _TypedAdmission,
    source: _ReadySource,
    sequence_range: tuple[int, int],
    ready_offset: int,
    page_limit: int,
) -> None:
    start, end = sequence_range
    ordinal = start - admission.snapshot.sequence_start
    range_index = 0
    with (source.result.path / _RAW_NAME).open("rb") as published:
        published.seek(ready_offset * RECORD_SIZE)
        for record in _typed_records(admission, ordinal, end - start, page_limit):
            sequence = admission.snapshot.sequence_start + record.ordinal
            while range_index < len(source.time_ranges) and sequence >= cast(
                int, source.time_ranges[range_index]["next_sequence"]
            ):
                range_index += 1
            if range_index == len(source.time_ranges) or sequence < cast(
                int, source.time_ranges[range_index]["start_sequence"]
            ):
                raise ReadyBundleError("typed ready overlap has no timestamp mapping")
            expected = _convert_record(record.raw_bytes, source.time_ranges[range_index])
            if published.read(RECORD_SIZE) != expected:
                raise ConflictingOverlapError("typed ready overlap conflicts with its source record")


def _typed_active_segments(source: _ReadySource) -> Iterator[tuple[int, int, int]]:
    offset = 0
    for start, end in _physical_runs(source.facts):
        yield start, end, offset
        offset += end - start


def _typed_retired_full_proof(
    admission: _TypedAdmission, custody: _TypedReadyCustody
) -> tuple[str, dict[str, object]] | None:
    start = admission.snapshot.sequence_start
    end = start + admission.admitted_count
    for bundle_id, entry in custody.entries.items():
        if entry["state"] != "retired":
            continue
        lineage = source_lineage(entry)
        if (start, end, admission.admitted_sha256) in lineage:
            return bundle_id, entry
    return None


def _typed_reject_partial_retired_overlap(admission: _TypedAdmission, custody: _TypedReadyCustody) -> None:
    start = admission.snapshot.sequence_start + admission.cursor
    end = admission.snapshot.sequence_start + admission.admitted_count
    if _typed_retired_full_proof(admission, custody) is not None:
        return
    for entry in custody.entries.values():
        if entry["state"] != "retired":
            continue
        if any(start < run_end and run_start < end for run_start, run_end in _physical_runs(_entry_facts(entry))):
            raise _TypedCustodyConflictError("typed source overlaps retired custody without exact full-source lineage")


def _typed_full_ready_proofs(
    admission: _TypedAdmission, custody: _TypedReadyCustody, page_limit: int
) -> tuple[tuple[str, dict[str, object]], ...] | None:
    retired = _typed_retired_full_proof(admission, custody)
    if retired is not None:
        return (retired,)
    _typed_reject_partial_retired_overlap(admission, custody)
    cursor = admission.snapshot.sequence_start
    end = cursor + admission.admitted_count
    proofs: list[tuple[str, dict[str, object]]] = []
    for source in custody.active:
        for run_start, run_end, ready_offset in _typed_active_segments(source):
            if run_end <= cursor:
                continue
            if run_start > cursor:
                return None
            overlap_end = min(run_end, end)
            _typed_ready_overlap(
                admission, source, (cursor, overlap_end), ready_offset + cursor - run_start, page_limit
            )
            proof = (source.result.bundle_id, custody.entries[source.result.bundle_id])
            if not proofs or proofs[-1][0] != proof[0]:
                proofs.append(proof)
            cursor = overlap_end
            if cursor == end:
                return tuple(proofs)
    return None


def _typed_ready_prefix(
    admission: _TypedAdmission, custody: _TypedReadyCustody, page_limit: int
) -> tuple[int, tuple[tuple[str, dict[str, object]], ...]]:
    if admission.cursor == admission.admitted_count:
        return admission.cursor, ()
    retired = _typed_retired_full_proof(admission, custody)
    if retired is not None:
        return admission.admitted_count, (retired,)
    _typed_reject_partial_retired_overlap(admission, custody)
    cursor = admission.snapshot.sequence_start + admission.cursor
    end = admission.snapshot.sequence_start + admission.admitted_count
    proofs: list[tuple[str, dict[str, object]]] = []
    for source in custody.active:
        for run_start, run_end, ready_offset in _typed_active_segments(source):
            if run_end <= cursor:
                continue
            if run_start > cursor:
                return cursor - admission.snapshot.sequence_start, tuple(proofs)
            overlap_end = min(run_end, end)
            _typed_ready_overlap(
                admission, source, (cursor, overlap_end), ready_offset + cursor - run_start, page_limit
            )
            proof = (source.result.bundle_id, custody.entries[source.result.bundle_id])
            if not proofs or proofs[-1][0] != proof[0]:
                proofs.append(proof)
            cursor = overlap_end
            if cursor == end:
                break
        if cursor == end:
            break
    return cursor - admission.snapshot.sequence_start, tuple(proofs)


def _require_typed_snapshot(current: CaptureSource, admission: _TypedAdmission) -> None:
    frozen = admission.snapshot
    current_identity = (
        current.source_id,
        current.source_type,
        current.sequence_start,
        current.sequence_limit,
        current.committed_count,
        current.committed_sha256,
    )
    frozen_identity = (
        frozen.source_id,
        frozen.source_type,
        frozen.sequence_start,
        frozen.sequence_limit,
        frozen.committed_count,
        frozen.committed_sha256,
    )
    if current_identity != frozen_identity or current.lifecycle not in {
        admission.admitted_lifecycle,
        "covered",
        "retiring",
    }:
        raise ReadyBundleError("typed publication source identity changed")


def _require_typed_admissions_current(
    audio_store: InternalAudioStore,
    admissions: tuple[_TypedAdmission, ...],
) -> None:
    for admission in admissions:
        current = audio_store.get_source(admission.snapshot.source_id)
        if current is None:
            raise ReadyBundleError("typed publication source disappeared")
        _require_typed_snapshot(current, admission)


def _rebind_typed_slices(
    slices: tuple[_TypedSlice, ...], admissions: tuple[_TypedAdmission, ...]
) -> tuple[_TypedSlice, ...]:
    by_source = {item.snapshot.source_id: item for item in admissions}
    if len(by_source) != len(admissions):
        raise ReadyBundleError("typed publication source is duplicated")
    try:
        return tuple(replace(item, admission=by_source[item.admission.snapshot.source_id]) for item in slices)
    except KeyError as error:
        raise ReadyBundleError("typed publication slice lost its cohort member") from error


def _typed_slice(admission: _TypedAdmission, start_ordinal: int, count: int, page_limit: int) -> _TypedSlice:
    digest = sha256()
    audio_packets = 0
    first_size = 0
    previous_overflow: int | None = None
    breaks: list[int] = []
    seen = 0
    for record in _typed_records(admission, start_ordinal, count, page_limit):
        raw = record.raw_bytes
        digest.update(raw)
        try:
            layout = inspect_20ms_record(raw)
        except ValueError as error:
            raise ReadyBundleError("typed source contains an invalid 20 ms Opus packet") from error
        if seen == 0:
            first_size = layout.first_size
        elif previous_overflow is not None and layout.first_size != previous_overflow:
            breaks.append(admission.snapshot.sequence_start + record.ordinal)
        seen += 1
        audio_packets += layout.packet_count
        previous_overflow = layout.overflow_size
    if seen != count:
        raise ReadyBundleError("typed source slice ended before its frozen count")
    return _TypedSlice(
        admission,
        start_ordinal,
        count,
        admission.snapshot.sequence_start + start_ordinal,
        admission.snapshot.sequence_start + start_ordinal + count,
        digest.hexdigest(),
        first_size,
        previous_overflow,
        audio_packets,
        tuple(breaks),
    )


def _normalize_typed_admissions(
    admissions: tuple[_TypedAdmission, ...], page_limit: int, custody: _TypedReadyCustody | None = None
) -> tuple[_TypedSlice, ...]:
    normalized: list[_TypedSlice] = []
    ends: list[int] = []
    for admission in admissions:
        remaining = admission.admitted_count - admission.cursor
        if remaining <= 0:
            continue
        candidate = _typed_slice(admission, admission.cursor, remaining, page_limit)
        if admission.cursor == 0 and candidate.raw_sha256 != admission.admitted_sha256:
            raise ReadyBundleError("typed source rows do not match its admitted checkpoint")
        try:
            ready_unique = (
                (candidate,) if custody is None else _unique_against_typed_ready(candidate, custody, page_limit)
            )
        except (ConflictingOverlapError, _TypedCustodyConflictError) as error:
            raise _TypedAdmissionConflictError(admission.snapshot.source_id, str(error)) from error
        for ready_slice in ready_unique:
            index = bisect_right(ends, ready_slice.start_sequence)
            for unique in _unique_against_typed(ready_slice, normalized, index, page_limit):
                position = bisect_right(ends, unique.start_sequence)
                normalized.insert(position, unique)
                ends.insert(position, unique.next_sequence)
    return tuple(normalized)


def _unique_against_typed_ready(
    candidate: _TypedSlice, custody: _TypedReadyCustody, page_limit: int
) -> tuple[_TypedSlice, ...]:
    _typed_reject_partial_retired_overlap(candidate.admission, custody)
    end = candidate.next_sequence
    cursor = candidate.start_sequence
    unique: list[_TypedSlice] = []
    for source in custody.active:
        for run_start, run_end, ready_offset in _typed_active_segments(source):
            if run_end <= cursor:
                continue
            if run_start >= end:
                break
            if cursor < run_start:
                unique.append(_crop_typed_slice(candidate, cursor, min(run_start, end), page_limit))
                cursor = min(run_start, end)
            overlap_end = min(run_end, end)
            if cursor < overlap_end:
                _typed_ready_overlap(
                    candidate.admission,
                    source,
                    (cursor, overlap_end),
                    ready_offset + cursor - run_start,
                    page_limit,
                )
                cursor = overlap_end
            if cursor == end:
                return tuple(unique)
        if cursor == end:
            break
    if cursor < end:
        unique.append(_crop_typed_slice(candidate, cursor, end, page_limit))
    return tuple(unique)


def _unique_against_typed(
    candidate: _TypedSlice, normalized: list[_TypedSlice], index: int, page_limit: int
) -> tuple[_TypedSlice, ...]:
    end = candidate.next_sequence
    cursor = candidate.start_sequence
    unique: list[_TypedSlice] = []
    while index < len(normalized):
        retained = normalized[index]
        if retained.start_sequence >= end:
            break
        if cursor < retained.start_sequence:
            unique.append(_crop_typed_slice(candidate, cursor, min(retained.start_sequence, end), page_limit))
            cursor = min(retained.start_sequence, end)
        overlap_end = min(retained.next_sequence, end)
        if cursor < overlap_end:
            _compare_typed_overlap(candidate, retained, cursor, overlap_end, page_limit)
            cursor = overlap_end
        if cursor == end:
            break
        index += 1
    if cursor < end:
        unique.append(_crop_typed_slice(candidate, cursor, end, page_limit))
    return tuple(unique)


def _compare_typed_overlap(left: _TypedSlice, right: _TypedSlice, start: int, end: int, page_limit: int) -> None:
    left_start = left.start_ordinal + start - left.start_sequence
    right_start = right.start_ordinal + start - right.start_sequence
    left_rows = _typed_records(left.admission, left_start, end - start, page_limit)
    right_rows = _typed_records(right.admission, right_start, end - start, page_limit)
    for left_record, right_record in zip(left_rows, right_rows, strict=True):
        if left_record.raw_bytes != right_record.raw_bytes:
            raise ConflictingOverlapError("authenticated typed source overlap has conflicting record bytes")


def _crop_typed_slice(source: _TypedSlice, start: int, end: int, page_limit: int) -> _TypedSlice:
    if not source.start_sequence <= start < end <= source.next_sequence:
        raise ReadyBundleError("typed source crop is invalid")
    if (start, end) == (source.start_sequence, source.next_sequence):
        return source
    ordinal = source.start_ordinal + start - source.start_sequence
    return _typed_slice(source.admission, ordinal, end - start, page_limit)


def _typed_stats(
    admissions: tuple[_TypedAdmission, ...], slices: tuple[_TypedSlice, ...], threshold: int
) -> ReadyPublicationStats:
    gaps = tuple(
        (left.next_sequence, right.start_sequence)
        for left, right in pairwise(slices)
        if left.next_sequence < right.start_sequence
    )
    return ReadyPublicationStats(
        source_count=len(admissions),
        record_count=sum(item.record_count for item in slices),
        audio_packets=sum(item.audio_packets for item in slices),
        threshold_packets=threshold,
        gap_count=len(gaps),
        gap_records=sum(end - start for start, end in gaps),
    )


def _typed_groups(
    slices: tuple[_TypedSlice, ...], page_limit: int, clock_segments: ClockSegmentMap
) -> tuple[_TypedGroup, ...]:
    physical: list[_TypedSlice] = []
    for item in slices:
        cursor = item.start_sequence
        for boundary in (*item.continuity_breaks, item.next_sequence):
            physical.append(_crop_typed_slice(item, cursor, boundary, page_limit))
            cursor = boundary
    groups: list[list[_TypedSlice]] = []
    previous: _TypedSlice | None = None
    for item in physical:
        split = previous is not None and (
            previous.next_sequence != item.start_sequence
            or (previous.final_overflow_size is not None and previous.final_overflow_size != item.first_size)
        )
        if split or not groups:
            groups.append([])
        groups[-1].append(item)
        previous = item
    return tuple(_make_typed_group(tuple(group), page_limit, clock_segments) for group in groups)


def _make_typed_group(slices: tuple[_TypedSlice, ...], page_limit: int, clock_segments: ClockSegmentMap) -> _TypedGroup:
    if not slices:
        raise ReadyBundleError("cannot publish an empty typed group")
    count = sum(item.record_count for item in slices)
    raw_sha256 = slices[0].raw_sha256 if len(slices) == 1 else _typed_raw_digest(slices, page_limit)
    gaps = tuple(
        (left.next_sequence, right.start_sequence)
        for left, right in pairwise(slices)
        if left.next_sequence < right.start_sequence
    )
    facts = _BundleFacts(slices[0].start_sequence, slices[-1].next_sequence, count, raw_sha256, gaps)
    ranges = _typed_clear_unrepresentable_ranges(slices, _time_ranges(facts, clock_segments), page_limit)
    output_digest = _typed_output_digest(slices, ranges, page_limit)
    return _TypedGroup(slices, facts, ranges, output_digest)


def _typed_clear_unrepresentable_ranges(
    slices: tuple[_TypedSlice, ...], ranges: tuple[dict[str, object], ...], page_limit: int
) -> tuple[dict[str, object], ...]:
    if all(item["utc"] is None for item in ranges):
        return ranges
    invalid: set[int] = set()
    range_index = 0
    next_sequence = cast(int, ranges[0]["next_sequence"])
    for item in slices:
        for record in _typed_records(item.admission, item.start_ordinal, item.record_count, page_limit):
            sequence = item.admission.snapshot.sequence_start + record.ordinal
            while sequence >= next_sequence:
                range_index += 1
                next_sequence = cast(int, ranges[range_index]["next_sequence"])
            utc = ranges[range_index]["utc"]
            if utc is None:
                continue
            assert isinstance(utc, Mapping)
            timestamp = int.from_bytes(record.raw_bytes[:TIMESTAMP_SIZE], "big")
            normalized = math.floor(timestamp + _finite_number(utc["offset_seconds"]) + 0.5)
            if not 0 <= normalized <= _UINT32_MAX:
                invalid.add(range_index)
    return tuple({**item, "utc": None} if index in invalid else item for index, item in enumerate(ranges))


def _typed_raw_digest(slices: tuple[_TypedSlice, ...], page_limit: int) -> str:
    digest = sha256()
    for item in slices:
        for record in _typed_records(item.admission, item.start_ordinal, item.record_count, page_limit):
            digest.update(record.raw_bytes)
    return digest.hexdigest()


def _typed_output_digest(
    slices: tuple[_TypedSlice, ...], ranges: tuple[dict[str, object], ...], page_limit: int
) -> str:
    digest = sha256()
    range_index = 0
    next_sequence = cast(int, ranges[0]["next_sequence"])
    for item in slices:
        for record in _typed_records(item.admission, item.start_ordinal, item.record_count, page_limit):
            sequence = item.admission.snapshot.sequence_start + record.ordinal
            while sequence >= next_sequence:
                range_index += 1
                next_sequence = cast(int, ranges[range_index]["next_sequence"])
            digest.update(_convert_record(record.raw_bytes, ranges[range_index]))
    return digest.hexdigest()


def _typed_cohort_id(
    admissions: tuple[_TypedAdmission, ...], threshold: int, frontier: int | None, config_digest: str
) -> str:
    identity = ":".join(
        f"{item.snapshot.source_id}:{item.snapshot.sequence_start}:{item.admitted_lifecycle}:"
        f"{item.admitted_count}:{item.admitted_sha256}"
        for item in admissions
    )
    return sha256(f"{threshold}:{frontier}:{config_digest}:{identity}".encode()).hexdigest()


def _load_active_typed_cohort(audio_store: InternalAudioStore) -> _TypedCohort | None:
    try:
        with (
            closing(audio_store.state_db.open_connection(read_only=True)) as connection,
            audio_store.state_db.access(connection),
        ):
            rows = _sql_all(
                connection.execute(
                    "SELECT cohort_id,lifecycle,accepted_permit,drained_threshold,closure_frontier,cursor,config_digest "
                    "FROM publication_cohort WHERE lifecycle IN ('accepted','staging','complete') ORDER BY cohort_id"
                )
            )
            if len(rows) > 1:
                raise ReadyBundleError("multiple active typed publication cohorts exist")
            if not rows:
                return None
            row = rows[0]
            cohort_id = cast(str, row[0])
            lifecycle = cast(str, row[1])
            permit = row[2]
            threshold = row[3]
            frontier_raw = row[4]
            cursor = row[5]
            config_digest = row[6]
            if (
                permit != cohort_id
                or not _is_nonnegative_int(threshold)
                or not _is_nonnegative_int(cursor)
                or not _is_sha256(config_digest)
            ):
                raise ReadyBundleError("typed publication cohort permit is invalid")
            assert isinstance(threshold, int) and isinstance(cursor, int)
            frontier = None if frontier_raw is None else decode_uint64(cast(bytes, frontier_raw))
            member_rows = _sql_all(
                connection.execute(
                    "SELECT member_ordinal,source_id,slice_start,slice_count,admitted_lifecycle,"
                    "admitted_count,admitted_sha256,cursor FROM publication_cohort_member "
                    "WHERE cohort_id=? ORDER BY member_ordinal",
                    (cohort_id,),
                )
            )
            certifications = {
                cast(str, member[1]): _sql_one(
                    connection.execute(
                        "SELECT certified_prefix_count,coverage_digest,block_reason,quarantine_disposition,"
                        "terminal_time,retention_deadline FROM capture_source WHERE source_id=?",
                        (member[1],),
                    )
                )
                for member in member_rows
            }
    except ReadyBundleError:
        raise
    except (OSError, StateDBError, sqlite3.DatabaseError, ValueError) as error:
        raise_if_sqlite_storage_fault(error)
        raise ReadyBundleError("typed publication cohort is unreadable") from error
    frozen = tuple(
        _persisted_admission(audio_store, expected, member, certifications)
        for expected, member in enumerate(member_rows)
    )
    if not frozen or _typed_cohort_id(frozen, threshold, frontier, cast(str, config_digest)) != cohort_id:
        raise ReadyBundleError("typed publication cohort identity is invalid")
    return _TypedCohort(
        cohort_id,
        lifecycle,
        threshold,
        frontier,
        cursor,
        cast(str, config_digest),
        frozen,
    )


def _persisted_admission(
    audio_store: InternalAudioStore,
    expected_ordinal: int,
    member: tuple[object, ...],
    certifications: dict[str, tuple[object, ...] | None],
) -> _TypedAdmission:
    member_ordinal, source_id, slice_start, slice_count, lifecycle, admitted_count, digest, progress = member
    valid = (
        member_ordinal == expected_ordinal
        and slice_start == 0
        and type(slice_count) is int
        and type(admitted_count) is int
        and slice_count == admitted_count
        and type(progress) is int
        and 0 <= cast(int, progress) <= cast(int, slice_count)
        and lifecycle in {"sealed_waiting", "blocked"}
        and _is_sha256(digest)
        and isinstance(source_id, str)
    )
    if not valid:
        raise ReadyBundleError("typed publication cohort membership is invalid")
    assert isinstance(member_ordinal, int)
    assert isinstance(source_id, str) and isinstance(admitted_count, int)
    assert isinstance(digest, str) and isinstance(progress, int)
    snapshot = audio_store.get_source(source_id)
    certification = certifications[source_id]
    if snapshot is None or certification is None:
        raise ReadyBundleError("typed publication cohort source evidence disappeared")
    admission = _TypedAdmission(
        SqlitePublicationSource(audio_store, source_id),
        snapshot,
        cast(str, lifecycle),
        admitted_count,
        digest,
        progress,
        member_ordinal,
    )
    _require_persisted_admission(admission, certification)
    return admission


def _require_persisted_admission(admission: _TypedAdmission, certification: tuple[object, ...]) -> None:
    snapshot = admission.snapshot
    if admission.admitted_lifecycle == "sealed_waiting":
        valid = (
            admission.admitted_count == snapshot.committed_count
            and admission.admitted_sha256 == snapshot.committed_sha256
        )
    else:
        valid = _blocked_typed_certification(snapshot, certification) == (
            admission.admitted_count,
            admission.admitted_sha256,
        )
    if not valid or snapshot.lifecycle not in {admission.admitted_lifecycle, "covered", "retiring"}:
        raise ReadyBundleError("typed publication cohort source identity changed")


def _create_typed_cohort(
    audio_store: InternalAudioStore,
    admissions: tuple[_TypedAdmission, ...],
    threshold: int,
    frontier: int | None,
    config_digest: str,
) -> _TypedCohort:
    cohort_id = _typed_cohort_id(admissions, threshold, frontier, config_digest)
    proposed = _TypedCohort(
        cohort_id,
        "accepted",
        threshold,
        frontier,
        0,
        config_digest,
        tuple(replace(item, member_ordinal=index) for index, item in enumerate(admissions)),
    )
    for attempt in range(2):
        try:
            with audio_store.publication_transaction() as (state, connection):
                _require_cohort_decision(
                    CohortPhase.UNQUALIFIED,
                    QualifiedDrained(),
                    CohortAction.ACCEPT,
                    CohortPhase.ACCEPTED,
                )
                active = _sql_all(
                    connection.execute(
                        "SELECT cohort_id FROM publication_cohort WHERE lifecycle IN ('accepted','staging','complete')"
                    )
                )
                if active:
                    raise ReadyBundleError("another typed publication cohort is active")
                connection.execute(
                    "INSERT INTO publication_cohort(cohort_id,lifecycle,accepted_permit,drained_threshold,"
                    "config_digest,closure_frontier,cursor,accepted_time) "
                    "VALUES (?, 'accepted', ?, ?, ?, ?, 0, strftime('%Y-%m-%dT%H:%M:%fZ','now'))",
                    (
                        cohort_id,
                        cohort_id,
                        threshold,
                        config_digest,
                        None if frontier is None else encode_uint64(frontier),
                    ),
                )
                connection.executemany(
                    "INSERT INTO publication_cohort_member(cohort_id,member_ordinal,source_id,slice_start,"
                    "slice_count,admitted_lifecycle,admitted_count,admitted_sha256,cursor) "
                    "VALUES (?, ?, ?, 0, ?, ?, ?, ?, 0)",
                    (
                        (
                            cohort_id,
                            index,
                            item.snapshot.source_id,
                            item.admitted_count,
                            item.admitted_lifecycle,
                            item.admitted_count,
                            item.admitted_sha256,
                        )
                        for index, item in enumerate(admissions)
                    ),
                )
                state.mark_publication_changed()
            return proposed
        except (InternalAudioStoreError, sqlite3.DatabaseError, TransientInternalAudioStorageError) as error:
            raise_if_sqlite_corrupt(error)
            durable = _load_active_for_reconciliation(audio_store)
            if durable is not None and _same_typed_cohort(durable, proposed):
                return durable
            if durable is None and attempt == 0:
                continue
            if durable is None:
                raise_if_sqlite_storage_fault(error)
                raise ReadyBundleError("typed cohort acceptance was rolled back") from error
            _typed_fail_stop(audio_store, "ambiguous typed cohort acceptance has a third state")
    raise AssertionError("unreachable")


def _load_active_for_reconciliation(audio_store: InternalAudioStore) -> _TypedCohort | None:
    try:
        return _load_active_typed_cohort(audio_store)
    except (ReadyBundleError, InternalAudioStoreError, TransientInternalAudioStorageError) as error:
        _typed_fail_stop(audio_store, f"ambiguous typed cohort could not be inspected: {error}")


def _same_typed_cohort(left: _TypedCohort, right: _TypedCohort) -> bool:
    return (
        left.cohort_id,
        left.lifecycle,
        left.threshold,
        left.frontier,
        left.cursor,
        left.config_digest,
        tuple(
            (
                item.snapshot.source_id,
                item.admitted_lifecycle,
                item.admitted_count,
                item.admitted_sha256,
                item.cursor,
                item.member_ordinal,
            )
            for item in left.admissions
        ),
    ) == (
        right.cohort_id,
        right.lifecycle,
        right.threshold,
        right.frontier,
        right.cursor,
        right.config_digest,
        tuple(
            (
                item.snapshot.source_id,
                item.admitted_lifecycle,
                item.admitted_count,
                item.admitted_sha256,
                item.cursor,
                item.member_ordinal,
            )
            for item in right.admissions
        ),
    )


def _resume_typed_cohort(  # noqa: PLR0913 - resume keeps the frozen storage and policy inputs explicit
    audio_store: InternalAudioStore,
    ready_root: Path,
    cohort: _TypedCohort,
    clock_segments: ClockSegmentMap,
    page_limit: int,
    *,
    custody_cache: ReadyCustodyCache | None,
    prepared_slices: tuple[_TypedSlice, ...] | None,
) -> tuple[list[ReadyBundleResult], ReadyCustodyCache | None]:
    published: list[ReadyBundleResult] = []
    current: _TypedCohort | None = cohort
    while current is not None:
        if current.lifecycle == "staging":
            _require_typed_admissions_current(audio_store, current.admissions)
            result = _resume_persisted_typed_stage(audio_store, ready_root, current, page_limit)
        else:
            if current.lifecycle != "accepted":
                raise ReadyBundleError("typed publication cohort lifecycle is invalid")
            custody_cache = _typed_ready_custody(ready_root, audio_store.state_db, custody_cache)
            custody = custody_cache.custody
            try:
                current = _advance_typed_ready_custody(audio_store, current, custody, page_limit)
            except _TypedAdmissionConflictError as error:
                admission = next(item for item in current.admissions if item.snapshot.source_id == error.source_id)
                _block_typed_custody_conflict(audio_store, admission, current)
                raise
            if current is None:
                break
            slices = _accepted_typed_slices(audio_store, current, page_limit, custody, prepared_slices)
            prepared_slices = None
            _require_typed_admissions_current(audio_store, current.admissions)
            if not slices:
                _complete_empty_typed_cohort(audio_store, current)
                break
            group = _typed_groups(slices, page_limit, clock_segments)[0]
            result = _publish_typed_group_with_error_boundary(audio_store, ready_root, current, group, page_limit)
        published.append(result)
        current = _load_active_typed_cohort(audio_store)
        if current is not None and current.cohort_id != cohort.cohort_id:
            _typed_fail_stop(audio_store, "typed publication cohort changed during resume")
    return published, custody_cache


def _accepted_typed_slices(
    audio_store: InternalAudioStore,
    cohort: _TypedCohort,
    page_limit: int,
    custody: _TypedReadyCustody,
    prepared_slices: tuple[_TypedSlice, ...] | None,
) -> tuple[_TypedSlice, ...]:
    if prepared_slices is not None:
        return prepared_slices
    try:
        return _normalize_typed_admissions(cohort.admissions, page_limit, custody)
    except _TypedAdmissionConflictError as error:
        admission = next(item for item in cohort.admissions if item.snapshot.source_id == error.source_id)
        _block_typed_custody_conflict(audio_store, admission, cohort)
        raise


def _advance_typed_ready_custody(
    audio_store: InternalAudioStore,
    cohort: _TypedCohort,
    custody: _TypedReadyCustody,
    page_limit: int,
) -> _TypedCohort | None:
    progress, proofs = _typed_ready_progress(cohort, custody, page_limit)
    _require_typed_admissions_current(audio_store, cohort.admissions)
    if all(item.new_cursor == item.old_cursor for item in progress):
        return cohort
    proposed = replace(
        cohort,
        admissions=tuple(
            replace(admission, cursor=item.new_cursor)
            for item, admission in zip(progress, cohort.admissions, strict=True)
        ),
    )
    commit = _CustodyCommit(
        cohort,
        proposed,
        progress,
        _typed_coverage_proposals(audio_store, cohort, progress),
        proofs,
        all(
            item.new_cursor == admission.admitted_count
            for item, admission in zip(progress, cohort.admissions, strict=True)
        ),
    )
    return _commit_typed_ready_custody(audio_store, commit)


def _typed_ready_progress(
    cohort: _TypedCohort, custody: _TypedReadyCustody, page_limit: int
) -> tuple[tuple[_MemberProgress, ...], dict[str, dict[str, object]]]:
    progress: list[_MemberProgress] = []
    proofs: dict[str, dict[str, object]] = {}
    for admission in cohort.admissions:
        try:
            new_cursor, admission_proofs = _typed_ready_prefix(admission, custody, page_limit)
        except (ConflictingOverlapError, _TypedCustodyConflictError) as error:
            raise _TypedAdmissionConflictError(admission.snapshot.source_id, str(error)) from error
        assert admission.member_ordinal is not None
        progress.append(_MemberProgress(admission.member_ordinal, admission.cursor, new_cursor))
        proofs.update(admission_proofs)
    return tuple(progress), proofs


def _commit_typed_ready_custody(audio_store: InternalAudioStore, commit: _CustodyCommit) -> _TypedCohort | None:
    for attempt in range(2):
        try:
            _commit_typed_ready_custody_once(audio_store, commit)
            return None if commit.all_complete else commit.proposed
        except (
            InternalAudioStoreError,
            StateDBError,
            sqlite3.DatabaseError,
            TransientInternalAudioStorageError,
        ) as error:
            raise_if_sqlite_corrupt(error)
            state = _inspect_typed_ready_custody(audio_store, commit)
            if state == "proposed":
                return None if commit.all_complete else commit.proposed
            if state == "old" and attempt == 0:
                continue
            if state == "old":
                raise_if_sqlite_storage_fault(error)
                raise ReadyBundleError("typed ready custody commit was rolled back") from error
            _typed_fail_stop(audio_store, "ambiguous typed ready custody commit has a third state")
    raise AssertionError("unreachable")


def _commit_typed_ready_custody_once(audio_store: InternalAudioStore, commit: _CustodyCommit) -> None:
    cohort = commit.cohort
    with audio_store.publication_transaction() as (state, connection):
        next_phase = CohortPhase.COMPLETE if commit.all_complete else CohortPhase.ACCEPTED
        _require_cohort_decision(
            CohortPhase.ACCEPTED,
            ExistingReadyCustody(
                proved_progress=any(item.new_cursor > item.old_cursor for item in commit.progress),
                cohort_complete=commit.all_complete,
            ),
            CohortAction.COMMIT,
            next_phase,
        )
        if any(state.get(StateKind.READY_PUBLICATION, key) != entry for key, entry in commit.proofs.items()):
            raise ReadyBundleError("typed ready custody changed before cursor commit")
        row = _sql_one(
            connection.execute("SELECT lifecycle,cursor FROM publication_cohort WHERE cohort_id=?", (cohort.cohort_id,))
        )
        if row is None or tuple(row) != ("accepted", cohort.cursor):
            raise ReadyBundleError("typed cohort changed before custody commit")
        for item in commit.progress:
            if item.new_cursor == item.old_cursor:
                continue
            updated = connection.execute(
                "UPDATE publication_cohort_member SET cursor=? WHERE cohort_id=? AND member_ordinal=? AND cursor=?",
                (item.new_cursor, cohort.cohort_id, item.member_ordinal, item.old_cursor),
            )
            if updated.rowcount != 1:
                raise ReadyBundleError("typed cohort member changed before custody commit")
        _apply_typed_coverage(connection, commit.coverage)
        if commit.all_complete:
            _require_cohort_decision(
                CohortPhase.COMPLETE,
                CompleteRetire(),
                CohortAction.RETIRE,
                None,
            )
            deleted = connection.execute(
                "DELETE FROM publication_cohort WHERE cohort_id=? AND lifecycle='accepted' AND cursor=?",
                (cohort.cohort_id, cohort.cursor),
            )
            if deleted.rowcount != 1:
                raise ReadyBundleError("typed replay cohort completion changed")
        state.mark_publication_changed()


def _inspect_typed_ready_custody(audio_store: InternalAudioStore, commit: _CustodyCommit) -> str:
    durable = _load_active_for_reconciliation(audio_store)
    source_states = tuple(_typed_source_coverage_state(audio_store, item.source_id) for item in commit.coverage)
    proposed_sources = all(state == item.proposed for state, item in zip(source_states, commit.coverage, strict=True))
    proposed_cohort = (
        durable is None if commit.all_complete else durable is not None and _same_typed_cohort(durable, commit.proposed)
    )
    if proposed_cohort and proposed_sources:
        return "proposed"
    old_sources = all(state == item.old for state, item in zip(source_states, commit.coverage, strict=True))
    if durable is not None and _same_typed_cohort(durable, commit.cohort) and old_sources:
        return "old"
    return "third"


def _complete_empty_typed_cohort(audio_store: InternalAudioStore, cohort: _TypedCohort) -> None:
    if any(item.cursor != item.admitted_count for item in cohort.admissions):
        raise ReadyBundleError("typed cohort has no output but incomplete members")
    try:
        with audio_store.publication_transaction() as (state, connection):
            phase = CohortPhase(cohort.lifecycle)
            if phase is CohortPhase.ACCEPTED:
                _require_cohort_decision(
                    phase,
                    ExistingReadyCustody(proved_progress=True, cohort_complete=True),
                    CohortAction.COMMIT,
                    CohortPhase.COMPLETE,
                )
                phase = CohortPhase.COMPLETE
            _require_cohort_decision(phase, CompleteRetire(), CohortAction.RETIRE, None)
            cursor = connection.execute(
                "DELETE FROM publication_cohort WHERE cohort_id=? AND lifecycle IN ('accepted','complete')",
                (cohort.cohort_id,),
            )
            if cursor.rowcount != 1:
                raise ReadyBundleError("typed completed cohort changed")
            state.mark_publication_changed()
    except (InternalAudioStoreError, sqlite3.DatabaseError, TransientInternalAudioStorageError) as error:
        raise_if_sqlite_corrupt(error)
        durable = _load_active_for_reconciliation(audio_store)
        if durable is None:
            return
        if _same_typed_cohort(durable, cohort):
            raise_if_sqlite_storage_fault(error)
            raise ReadyBundleError("typed completed cohort deletion was rolled back") from error
        _typed_fail_stop(audio_store, "ambiguous typed cohort completion has a third state")


def _ensure_typed_stage(
    audio_store: InternalAudioStore,
    ready_root: Path,
    cohort: _TypedCohort,
    group: _TypedGroup,
    page_limit: int,
) -> _TypedStage:
    bundle_id = _bundle_id(group.facts)
    group_identity = f"{cohort.cohort_id}:{cohort.cursor}:{bundle_id}"
    stage_id = sha256(group_identity.encode()).hexdigest()
    path = ready_root / f".{bundle_id}.{stage_id[:16]}.stage"
    key = _StageKey(stage_id, group_identity, path)
    existing = _load_typed_stage(audio_store, cohort, key, page_limit)
    if existing is not None:
        if cohort.lifecycle != "staging":
            _typed_fail_stop(audio_store, "typed stage exists outside STAGING")
        _require_cohort_decision(
            CohortPhase.STAGING,
            OwnedIncompleteRetry(),
            CohortAction.RECONCILE,
            CohortPhase.STAGING,
        )
        _require_typed_stage_space(audio_store, cohort, existing)
        return existing
    if cohort.lifecycle != "accepted":
        raise ReadyBundleError("STAGING cohort lost its typed stage intent")
    if os.path.lexists(path):
        raise ReadyBundleError("unowned typed ready stage blocks publication")
    rows = _stage_ranges(group, page_limit)
    proposed = _TypedStage(stage_id, group_identity, path, group, rows, stage_id)
    _require_typed_stage_space(audio_store, cohort, proposed)
    for attempt in range(2):
        try:
            _insert_typed_stage(audio_store, cohort, proposed)
            return proposed
        except (InternalAudioStoreError, sqlite3.DatabaseError, TransientInternalAudioStorageError) as error:
            raise_if_sqlite_corrupt(error)
            durable_cohort = _load_active_for_reconciliation(audio_store)
            durable = _load_stage_for_reconciliation(audio_store, durable_cohort, key, page_limit)
            if (
                durable_cohort is not None
                and _same_typed_cohort(durable_cohort, replace(cohort, lifecycle="staging"))
                and durable is not None
                and _same_typed_stage(durable, proposed)
            ):
                return durable
            if (
                durable_cohort is not None
                and _same_typed_cohort(durable_cohort, cohort)
                and durable is None
                and attempt == 0
            ):
                continue
            if durable_cohort is not None and _same_typed_cohort(durable_cohort, cohort) and durable is None:
                raise_if_sqlite_storage_fault(error)
                raise ReadyBundleError("typed stage intent was rolled back") from error
            _typed_fail_stop(audio_store, "ambiguous typed stage intent has a third state")
    raise AssertionError("unreachable")


def _load_stage_for_reconciliation(
    audio_store: InternalAudioStore,
    cohort: _TypedCohort | None,
    key: _StageKey,
    page_limit: int,
) -> _TypedStage | None:
    if cohort is None:
        return None
    try:
        return _load_typed_stage(audio_store, cohort, key, page_limit)
    except ReadyBundleError, InternalAudioStoreError, TransientInternalAudioStorageError:
        _typed_fail_stop(audio_store, "ambiguous typed stage intent could not be inspected")


def _insert_typed_stage(audio_store: InternalAudioStore, cohort: _TypedCohort, stage: _TypedStage) -> None:
    ranges_digest = sha256(_canonical(stage.group.ranges)).hexdigest()
    first = stage.rows[0]
    last = stage.rows[-1]
    with audio_store.publication_transaction() as (state, connection):
        _require_cohort_decision(
            CohortPhase.ACCEPTED,
            StageEvent(),
            CohortAction.STAGE,
            CohortPhase.STAGING,
        )
        changed = connection.execute(
            "UPDATE publication_cohort SET lifecycle='staging' WHERE cohort_id=? AND lifecycle='accepted' AND cursor=?",
            (cohort.cohort_id, cohort.cursor),
        )
        if changed.rowcount != 1:
            raise ReadyBundleError("typed cohort changed before stage intent")
        connection.execute(
            "INSERT INTO ready_output_stage(stage_id,group_identity,owned_path,expected_bundle_digest,"
            "expected_record_count,expected_slice_count,progress_records,progress_slice,frozen_device_start,"
            "frozen_device_end,frozen_utc_start,frozen_utc_end,frozen_mapping_revision,owner_token) "
            "VALUES (?, ?, ?, ?, ?, ?, 0, 0, ?, ?, ?, ?, ?, ?)",
            (
                stage.stage_id,
                stage.group_identity,
                str(stage.path),
                stage.group.output_sha256,
                stage.group.facts.record_count,
                len(stage.rows),
                first.device_start,
                last.device_end,
                first.utc_start,
                last.utc_end,
                ranges_digest,
                stage.owner_token,
            ),
        )
        connection.executemany(
            "INSERT INTO ready_stage_range(stage_id,range_ordinal,time_range_ordinal,source_id,start_ordinal,record_count,"
            "sequence_start,next_sequence,device_start,device_end,utc_start,utc_end,utc_observation_id,"
            "utc_offset_seconds,utc_uncertainty_seconds,utc_confidence) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (_stage_range_values(stage.stage_id, index, item) for index, item in enumerate(stage.rows)),
        )
        state.mark_publication_changed()


def _stage_range_values(stage_id: str, index: int, item: _StageRange) -> tuple[object, ...]:
    utc = item.time_range["utc"]
    if utc is None:
        mapping: tuple[object, ...] = (None, None, None, None)
    else:
        assert isinstance(utc, Mapping)
        mapping = (
            utc["observation_id"],
            utc["offset_seconds"],
            utc["uncertainty_seconds"],
            utc.get("confidence", "confirmed"),
        )
    return (
        stage_id,
        index,
        item.time_range_ordinal,
        item.slice.admission.snapshot.source_id,
        item.slice.start_ordinal,
        item.slice.record_count,
        encode_uint64(item.slice.start_sequence),
        encode_uint64(item.slice.next_sequence),
        item.device_start,
        item.device_end,
        item.utc_start,
        item.utc_end,
        *mapping,
    )


def _stage_ranges(group: _TypedGroup, page_limit: int) -> tuple[_StageRange, ...]:
    rows: list[_StageRange] = []
    for item in group.slices:
        for time_range_ordinal, time_range in enumerate(group.ranges):
            start = max(item.start_sequence, cast(int, time_range["start_sequence"]))
            end = min(item.next_sequence, cast(int, time_range["next_sequence"]))
            if start >= end:
                continue
            slice_ = _crop_typed_slice(item, start, end, page_limit)
            device_start, device_end = _typed_endpoint_timestamps(slice_, page_limit)
            utc = time_range["utc"]
            if utc is None:
                utc_start = utc_end = None
            else:
                assert isinstance(utc, Mapping)
                offset = _finite_number(utc["offset_seconds"])
                utc_start = math.floor(device_start + offset + 0.5)
                utc_end = math.floor(device_end + offset + 0.5)
                if not 0 <= utc_start <= _UINT32_MAX or not 0 <= utc_end <= _UINT32_MAX:
                    raise ReadyBundleError("frozen typed UTC endpoint is outside uint32")
            rows.append(
                _StageRange(
                    slice_,
                    {
                        "start_sequence": start,
                        "next_sequence": end,
                        "utc": utc,
                    },
                    time_range_ordinal,
                    device_start,
                    device_end,
                    utc_start,
                    utc_end,
                )
            )
    if not rows or sum(item.slice.record_count for item in rows) != group.facts.record_count:
        raise ReadyBundleError("typed stage ranges do not cover the group")
    return tuple(rows)


def _typed_endpoint_timestamps(item: _TypedSlice, page_limit: int) -> tuple[int, int]:
    first = next(_typed_records(item.admission, item.start_ordinal, 1, page_limit))
    last = next(
        _typed_records(
            item.admission,
            item.start_ordinal + item.record_count - 1,
            1,
            page_limit,
        )
    )
    return first.device_timestamp, last.device_timestamp


def _load_typed_stage(
    audio_store: InternalAudioStore,
    cohort: _TypedCohort,
    key: _StageKey,
    page_limit: int,
) -> _TypedStage | None:
    try:
        with (
            closing(audio_store.state_db.open_connection(read_only=True)) as connection,
            audio_store.state_db.access(connection),
        ):
            row = _sql_one(
                connection.execute(
                    "SELECT group_identity,owned_path,expected_bundle_digest,expected_record_count,"
                    "expected_slice_count,frozen_device_start,frozen_device_end,frozen_utc_start,frozen_utc_end,"
                    "frozen_mapping_revision,owner_token,failure_reason FROM ready_output_stage WHERE stage_id=?",
                    (key.stage_id,),
                )
            )
            if row is None:
                return None
            raw_ranges = _sql_all(
                connection.execute(
                    "SELECT range_ordinal,time_range_ordinal,source_id,start_ordinal,record_count,sequence_start,next_sequence,"
                    "device_start,device_end,utc_start,utc_end,utc_observation_id,utc_offset_seconds,"
                    "utc_uncertainty_seconds,utc_confidence FROM ready_stage_range "
                    "WHERE stage_id=? ORDER BY range_ordinal",
                    (key.stage_id,),
                )
            )
    except (OSError, StateDBError, sqlite3.DatabaseError) as error:
        raise_if_sqlite_storage_fault(error)
        raise ReadyBundleError("typed stage intent is unreadable") from error
    header = _parse_stage_header(row, key, len(raw_ranges))
    by_source = {item.snapshot.source_id: item for item in cohort.admissions}
    stage_rows = tuple(
        _stage_range_from_row(by_source, expected, raw, page_limit) for expected, raw in enumerate(raw_ranges)
    )
    if not stage_rows:
        raise ReadyBundleError("typed stage has no frozen ranges")
    if any(item.slice.start_ordinal < item.slice.admission.cursor for item in stage_rows):
        raise ReadyBundleError("typed stage precedes its cohort member cursor")
    ranges = _rebuild_frozen_ranges(stage_rows)
    slices = tuple(item.slice for item in stage_rows)
    group = _typed_group_from_frozen(slices, ranges, page_limit)
    if key.group_identity.rsplit(":", maxsplit=1)[-1] != _bundle_id(group.facts):
        raise ReadyBundleError("typed stage group identity changed")
    actual_identity = (
        group.output_sha256,
        group.facts.record_count,
        sha256(_canonical(ranges)).hexdigest(),
        stage_rows[0].device_start,
        stage_rows[-1].device_end,
        stage_rows[0].utc_start,
        stage_rows[-1].utc_end,
    )
    expected_identity = (
        header.expected_digest,
        header.expected_count,
        header.mapping_revision,
        header.frozen_device_start,
        header.frozen_device_end,
        header.frozen_utc_start,
        header.frozen_utc_end,
    )
    if actual_identity != expected_identity:
        raise ReadyBundleError("typed stage frozen identity changed")
    return _TypedStage(
        key.stage_id,
        key.group_identity,
        key.path,
        group,
        stage_rows,
        header.owner_token,
    )


def _parse_stage_header(row: tuple[object, ...], key: _StageKey, range_count: int) -> _StageHeader:
    identity, path, digest, count, slices, device_start, device_end, utc_start, utc_end, revision, owner, failure = row
    if failure == _DURABILITY_IO_FAILURE:
        raise ReadyBundleError("typed stage is blocked by a recorded durability I/O failure")
    valid = (
        identity == key.group_identity
        and path == str(key.path)
        and owner == key.stage_id
        and _is_sha256(digest)
        and type(count) is int
        and count > 0
        and slices == range_count
        and _is_sha256(revision)
        and type(device_start) is int
        and type(device_end) is int
        and (utc_start is None or type(utc_start) is int)
        and (utc_end is None or type(utc_end) is int)
        and failure is None
    )
    if not valid:
        raise ReadyBundleError("typed stage ownership is invalid")
    assert isinstance(digest, str) and isinstance(count, int)
    assert isinstance(device_start, int) and isinstance(device_end, int)
    assert isinstance(revision, str) and isinstance(owner, str)
    return _StageHeader(
        digest,
        count,
        device_start,
        device_end,
        cast(int | None, utc_start),
        cast(int | None, utc_end),
        revision,
        owner,
        None,
    )


def _stage_range_from_row(
    by_source: dict[str, _TypedAdmission],
    expected_ordinal: int,
    raw: tuple[object, ...],
    page_limit: int,
) -> _StageRange:
    row = _parse_stage_range_row(raw)
    if row.ordinal != expected_ordinal or row.source_id not in by_source:
        raise ReadyBundleError("typed stage ranges are invalid")
    slice_ = _typed_slice(by_source[row.source_id], row.start, row.count, page_limit)
    sequence = (decode_uint64(row.sequence_start), decode_uint64(row.next_sequence))
    endpoints = _typed_endpoint_timestamps(slice_, page_limit)
    if sequence != (slice_.start_sequence, slice_.next_sequence) or endpoints != (row.device_start, row.device_end):
        raise ReadyBundleError("typed stage slice identity changed")
    utc = _frozen_utc_mapping(
        (row.observation, row.offset, row.uncertainty, row.confidence),
        (row.utc_start, row.utc_end),
    )
    _require_frozen_endpoints(endpoints, row.utc_start, row.utc_end, row.offset, utc)
    return _StageRange(
        slice_,
        {"start_sequence": sequence[0], "next_sequence": sequence[1], "utc": utc},
        row.time_range_ordinal,
        endpoints[0],
        endpoints[1],
        cast(int | None, row.utc_start),
        cast(int | None, row.utc_end),
    )


def _parse_stage_range_row(raw: tuple[object, ...]) -> _StageRangeRow:
    if len(raw) != _TYPED_STAGE_RANGE_COLUMNS:
        raise ReadyBundleError("typed stage ranges are invalid")
    if not _is_nonnegative_int(raw[1]) or not isinstance(raw[2], str) or not _is_nonnegative_int(raw[3]):
        raise ReadyBundleError("typed stage ranges are invalid")
    if not _is_nonnegative_int(raw[0]) or not _is_nonnegative_int(raw[4]) or raw[4] == 0:
        raise ReadyBundleError("typed stage ranges are invalid")
    if not isinstance(raw[5], bytes) or not isinstance(raw[6], bytes):
        raise ReadyBundleError("typed stage ranges are invalid")
    return _StageRangeRow(
        ordinal=cast(int, raw[0]),
        time_range_ordinal=cast(int, raw[1]),
        source_id=raw[2],
        start=cast(int, raw[3]),
        count=cast(int, raw[4]),
        sequence_start=raw[5],
        next_sequence=raw[6],
        device_start=raw[7],
        device_end=raw[8],
        utc_start=raw[9],
        utc_end=raw[10],
        observation=raw[11],
        offset=raw[12],
        uncertainty=raw[13],
        confidence=raw[14],
    )


def _require_frozen_endpoints(
    device: tuple[int, int], utc_start: object, utc_end: object, offset: object, utc: dict[str, object] | None
) -> None:
    if utc is None:
        return
    assert isinstance(offset, (int, float)) and not isinstance(offset, bool)
    calculated = tuple(math.floor(value + offset + 0.5) for value in device)
    if calculated != (utc_start, utc_end):
        raise ReadyBundleError("typed stage UTC endpoints are invalid")


def _frozen_utc_mapping(
    mapping: tuple[object, object, object, object],
    endpoints: tuple[object, object],
) -> dict[str, object] | None:
    observation_id, offset, uncertainty, confidence = mapping
    utc_start, utc_end = endpoints
    values = (observation_id, offset, uncertainty, confidence, utc_start, utc_end)
    if all(value is None for value in values):
        return None
    try:
        _validate_observation_id(observation_id)
        offset_value = _finite_number(offset)
        uncertainty_value = _finite_number(uncertainty)
        _validate_nonnegative(uncertainty_value)
    except ReadyBundleError as error:
        raise ReadyBundleError("typed stage UTC mapping is invalid") from error
    if confidence not in {"confirmed", "approximate"} or type(utc_start) is not int or type(utc_end) is not int:
        raise ReadyBundleError("typed stage UTC mapping is invalid")
    result: dict[str, object] = {
        "observation_id": observation_id,
        "offset_seconds": offset_value,
        "uncertainty_seconds": uncertainty_value,
    }
    if confidence == "approximate":
        result["confidence"] = confidence
    return result


def _rebuild_frozen_ranges(rows: tuple[_StageRange, ...]) -> tuple[dict[str, object], ...]:
    ranges: list[dict[str, object]] = []
    for row in rows:
        item = row.time_range
        if row.time_range_ordinal == len(ranges):
            if ranges and cast(int, ranges[-1]["next_sequence"]) > cast(int, item["start_sequence"]):
                raise ReadyBundleError("typed stage time ranges overlap")
            ranges.append(dict(item))
        elif row.time_range_ordinal == len(ranges) - 1:
            previous = ranges[-1]
            if previous["next_sequence"] != item["start_sequence"] or previous["utc"] != item["utc"]:
                raise ReadyBundleError("typed stage time range continuation changed")
            previous["next_sequence"] = item["next_sequence"]
        else:
            raise ReadyBundleError("typed stage time range ordinals are invalid")
    return tuple(ranges)


def _typed_group_from_frozen(
    slices: tuple[_TypedSlice, ...], ranges: tuple[dict[str, object], ...], page_limit: int
) -> _TypedGroup:
    digest = sha256()
    count = 0
    for item in slices:
        for record in _typed_records(item.admission, item.start_ordinal, item.record_count, page_limit):
            digest.update(record.raw_bytes)
        count += item.record_count
    gaps = tuple(
        (left.next_sequence, right.start_sequence)
        for left, right in pairwise(slices)
        if left.next_sequence < right.start_sequence
    )
    facts = _BundleFacts(slices[0].start_sequence, slices[-1].next_sequence, count, digest.hexdigest(), gaps)
    if ranges[0]["start_sequence"] != facts.start_sequence or ranges[-1]["next_sequence"] != facts.next_sequence:
        raise ReadyBundleError("typed stage time ranges do not cover the group")
    return _TypedGroup(slices, facts, ranges, _typed_output_digest(slices, ranges, page_limit))


def _same_typed_stage(left: _TypedStage, right: _TypedStage) -> bool:
    return (
        left.stage_id,
        left.group_identity,
        left.path,
        left.group.facts,
        left.group.ranges,
        left.group.output_sha256,
        left.owner_token,
    ) == (
        right.stage_id,
        right.group_identity,
        right.path,
        right.group.facts,
        right.group.ranges,
        right.group.output_sha256,
        right.owner_token,
    )


def _publish_typed_group(
    audio_store: InternalAudioStore,
    ready_root: Path,
    cohort: _TypedCohort,
    group: _TypedGroup,
    page_limit: int,
) -> ReadyBundleResult:
    stage = _ensure_typed_stage(audio_store, ready_root, cohort, group, page_limit)
    result = _materialize_typed_stage_with_failure_latch(audio_store, cohort, stage, page_limit)
    _commit_typed_stage(audio_store, cohort, stage, result)
    return result


def _publish_typed_group_with_error_boundary(
    audio_store: InternalAudioStore,
    ready_root: Path,
    cohort: _TypedCohort,
    group: _TypedGroup,
    page_limit: int,
) -> ReadyBundleResult:
    try:
        return _publish_typed_group(audio_store, ready_root, cohort, group, page_limit)
    except InternalAudioStoreError:
        raise
    except (OSError, StateDBError, sqlite3.DatabaseError) as error:
        raise_if_sqlite_storage_fault(error)
        raise ReadyBundleError("typed ready group could not be published") from error


def _resume_persisted_typed_stage(
    audio_store: InternalAudioStore,
    ready_root: Path,
    cohort: _TypedCohort,
    page_limit: int,
) -> ReadyBundleResult:
    try:
        _require_cohort_decision(
            CohortPhase.STAGING,
            OwnedIncompleteRetry(),
            CohortAction.RECONCILE,
            CohortPhase.STAGING,
        )
        key = _persisted_typed_stage_key(audio_store, ready_root, cohort)
        stage = _load_typed_stage(audio_store, cohort, key, page_limit)
        if stage is None:
            raise ReadyBundleError("STAGING cohort lost its typed stage intent")
        _require_typed_stage_space(audio_store, cohort, stage)
        result = _materialize_typed_stage_with_failure_latch(audio_store, cohort, stage, page_limit)
        _commit_typed_stage(audio_store, cohort, stage, result)
        return result
    except InternalAudioStoreError:
        raise
    except (OSError, StateDBError, sqlite3.DatabaseError) as error:
        raise_if_sqlite_storage_fault(error)
        raise ReadyBundleError("typed ready group could not be published") from error


def _persisted_typed_stage_key(
    audio_store: InternalAudioStore,
    ready_root: Path,
    cohort: _TypedCohort,
) -> _StageKey:
    try:
        with (
            closing(audio_store.state_db.open_connection(read_only=True)) as connection,
            audio_store.state_db.access(connection),
        ):
            rows = _sql_all(
                connection.execute(
                    "SELECT stage_id,group_identity,owned_path,owner_token FROM ready_output_stage ORDER BY stage_id"
                )
            )
    except (OSError, StateDBError, sqlite3.DatabaseError) as error:
        raise_if_sqlite_storage_fault(error)
        raise ReadyBundleError("typed stage intent is unreadable") from error
    if len(rows) != 1:
        raise ReadyBundleError("STAGING cohort does not own exactly one typed stage intent")
    stage_id, identity, owned_path, owner_token = rows[0]
    if not all(isinstance(value, str) for value in rows[0]):
        raise ReadyBundleError("typed stage ownership is invalid")
    assert isinstance(stage_id, str) and isinstance(identity, str)
    assert isinstance(owned_path, str) and isinstance(owner_token, str)
    parts = identity.split(":")
    if len(parts) != _TYPED_STAGE_IDENTITY_PARTS:
        raise ReadyBundleError("typed stage group identity is invalid")
    cohort_id, cursor, bundle_id = parts
    expected_stage_id = sha256(identity.encode()).hexdigest()
    expected_path = ready_root / f".{bundle_id}.{expected_stage_id[:16]}.stage"
    if (
        cohort_id != cohort.cohort_id
        or cursor != str(cohort.cursor)
        or not _is_sha256(bundle_id)
        or (stage_id, owner_token, owned_path) != (expected_stage_id, expected_stage_id, str(expected_path))
    ):
        raise ReadyBundleError("typed stage ownership is invalid")
    return _StageKey(stage_id, identity, expected_path)


def _materialize_typed_stage_with_failure_latch(
    audio_store: InternalAudioStore,
    cohort: _TypedCohort,
    stage: _TypedStage,
    page_limit: int,
) -> ReadyBundleResult:
    try:
        return _materialize_typed_stage(stage, page_limit)
    except OSError as error:
        if error.errno != errno.EIO:
            raise
        _latch_typed_durability_failure(audio_store, cohort, stage)
        raise ReadyBundleError("typed stage is blocked by a recorded durability I/O failure") from error


def _latch_typed_durability_failure(
    audio_store: InternalAudioStore,
    cohort: _TypedCohort,
    stage: _TypedStage,
) -> None:
    try:
        with audio_store.publication_transaction() as (state, connection):
            cohort_row = _sql_one(
                connection.execute(
                    "SELECT lifecycle,cursor FROM publication_cohort WHERE cohort_id=?",
                    (cohort.cohort_id,),
                )
            )
            stage_row = _sql_one(
                connection.execute(
                    "SELECT group_identity,owned_path,owner_token,failure_reason "
                    "FROM ready_output_stage WHERE stage_id=?",
                    (stage.stage_id,),
                )
            )
            expected_stage = (stage.group_identity, str(stage.path), stage.owner_token)
            if (
                cohort_row is None
                or tuple(cohort_row) != ("staging", cohort.cursor)
                or stage_row is None
                or tuple(stage_row)[:3] != expected_stage
            ):
                raise ReadyDurabilityFatalError("READY durability failure could not be attached to its stage")
            if stage_row[3] == _DURABILITY_IO_FAILURE:
                return
            if stage_row[3] is not None:
                raise ReadyDurabilityFatalError("READY durability failure encountered an invalid stage latch")
            changed = connection.execute(
                "UPDATE ready_output_stage SET failure_reason=? WHERE stage_id=? AND group_identity=? "
                "AND owned_path=? AND owner_token=? AND failure_reason IS NULL",
                (_DURABILITY_IO_FAILURE, stage.stage_id, *expected_stage),
            )
            if changed.rowcount != 1:
                raise ReadyDurabilityFatalError("READY durability failure latch lost its compare-and-swap")
            state.mark_publication_changed()
    except FatalInternalAudioStorageError:
        raise
    except Exception as error:
        if _typed_durability_failure_is_latched(audio_store, cohort, stage):
            return
        raise ReadyDurabilityFatalError("READY durability failure could not be recorded") from error


def _typed_durability_failure_is_latched(
    audio_store: InternalAudioStore,
    cohort: _TypedCohort,
    stage: _TypedStage,
) -> bool:
    try:
        with (
            closing(audio_store.state_db.open_connection(read_only=True)) as connection,
            audio_store.state_db.access(connection),
        ):
            row = _sql_one(
                connection.execute(
                    "SELECT p.lifecycle,p.cursor,s.group_identity,s.owned_path,s.owner_token,s.failure_reason "
                    "FROM publication_cohort AS p JOIN ready_output_stage AS s ON s.stage_id=? "
                    "WHERE p.cohort_id=?",
                    (stage.stage_id, cohort.cohort_id),
                )
            )
    except (OSError, StateDBError, sqlite3.DatabaseError, TransientInternalAudioStorageError) as error:
        raise_if_sqlite_corrupt(error)
        return False
    return row is not None and tuple(row) == (
        "staging",
        cohort.cursor,
        stage.group_identity,
        str(stage.path),
        stage.owner_token,
        _DURABILITY_IO_FAILURE,
    )


def _materialize_typed_stage(stage: _TypedStage, page_limit: int) -> ReadyBundleResult:
    bundle_id = _bundle_id(stage.group.facts)
    destination = stage.path.parent / bundle_id
    if os.path.lexists(destination):
        _sync_typed_bundle(destination, stage.group, require_bundle_name=True)
        _sync_directory(destination.parent)
        _discard_owned_typed_stage(stage.path)
    else:
        if os.path.lexists(stage.path):
            if _complete_typed_stage_matches(stage.path, stage.group):
                _sync_typed_bundle(stage.path, stage.group, require_bundle_name=False)
                stage.path.rename(destination)
                _sync_directory(destination.parent)
            else:
                _discard_owned_typed_stage(stage.path)
        if not destination.exists():
            _write_typed_stage(stage, page_limit)
            try:
                stage.path.rename(destination)
            except FileExistsError:
                _sync_typed_bundle(destination, stage.group, require_bundle_name=True)
                _discard_owned_typed_stage(stage.path)
            _sync_directory(destination.parent)
    _require_typed_destination(destination, stage.group)
    return ReadyBundleResult(
        bundle_id,
        destination,
        stage.group.facts.next_sequence,
        stage.group.facts.record_count,
        stage.group.output_sha256,
    )


def _write_typed_stage(stage: _TypedStage, page_limit: int) -> None:
    _write_typed_stage_files(stage.path, stage.group, page_limit)


def _require_typed_stage_space(audio_store: InternalAudioStore, cohort: _TypedCohort, stage: _TypedStage) -> None:
    stage_bytes = _typed_output_reserve_bytes(stage)
    database = audio_store.state_db.path
    try:
        with (
            closing(audio_store.state_db.open_connection(read_only=True)) as connection,
            audio_store.state_db.access(connection),
        ):
            page_size_row = _sql_one(connection.execute("PRAGMA page_size"))
            page_count_row = _sql_one(connection.execute("PRAGMA page_count"))
        if page_size_row is None or page_count_row is None:
            raise ReadyBundleError("typed publication SQLite page accounting is unavailable")
        page_size = cast(int, page_size_row[0])
        page_count = cast(int, page_count_row[0])
        if page_size <= 0 or page_count <= 0:
            raise ReadyBundleError("typed publication SQLite page accounting is invalid")
        database_bytes = _typed_database_reserve_bytes(page_size, page_count, cohort, stage)
        ready_stat = stage.path.parent.stat()
        database_stat = database.stat()
        if ready_stat.st_dev == database_stat.st_dev:
            _require_typed_filesystem_space(stage.path.parent, stage_bytes + database_bytes + _TYPED_SPACE_HEADROOM)
        else:
            _require_typed_filesystem_space(stage.path.parent, stage_bytes + _TYPED_SPACE_HEADROOM)
            _require_typed_filesystem_space(database.parent, database_bytes + _TYPED_SPACE_HEADROOM)
    except ReadyBundleError:
        raise
    except (OSError, StateDBError, sqlite3.DatabaseError, TypeError, IndexError) as error:
        raise_if_sqlite_storage_fault(error)
        raise ReadyBundleError("typed publication space reserve could not be inspected") from error


def _typed_output_reserve_bytes(stage: _TypedStage) -> int:
    destination = stage.path.parent / _bundle_id(stage.group.facts)
    if os.path.lexists(destination):
        _require_typed_destination(destination, stage.group)
        return 0
    if os.path.lexists(stage.path) and _complete_typed_stage_matches(stage.path, stage.group):
        return 0
    manifest = _canonical(_ready_manifest(stage.group.facts, stage.group.output_sha256, stage.group.ranges))
    return stage.group.facts.record_count * RECORD_SIZE + len(manifest)


def _typed_database_reserve_bytes(
    page_size: int,
    page_count: int,
    cohort: _TypedCohort,
    stage: _TypedStage,
) -> int:
    range_rows = len(stage.rows)
    member_rows = len(cohort.admissions)
    # The final transaction is the larger case: range deletion, member cursor
    # update, source coverage update, possible member cascade, three state rows,
    # the stage table plus its unique index, and the cohort row.
    btree_mutations = range_rows + 3 * member_rows + 6
    # A binary tree is the least-fanout SQLite b-tree, so bit_length(page_count)
    # bounds its depth. At each level a mutation can dirty the path page, one
    # rebalance sibling and one parent/split page. Each schema index above is
    # already counted as its own mutation.
    btree_depth = max(1, page_count.bit_length())
    btree_pages = _TYPED_BTREE_PAGES_PER_LEVEL * btree_depth * btree_mutations
    payload_bytes = _typed_stage_metadata_bytes(stage, member_rows)
    payload_pages = ceil(payload_bytes / page_size)
    journal_pages = min(page_count, btree_pages) + _TYPED_JOURNAL_MARGIN_PAGES
    growth_pages = btree_pages
    return (journal_pages + growth_pages + payload_pages) * page_size


def _typed_stage_metadata_bytes(stage: _TypedStage, member_rows: int) -> int:
    lineage = tuple((item.start_sequence, item.next_sequence, item.raw_sha256) for item in stage.group.slices)
    entry = _ledger_entry(
        stage.group.output_sha256,
        "ready",
        stage.group.facts,
        lineage,
    )
    text_bytes = sum(
        len(value.encode())
        for value in (
            stage.stage_id,
            stage.group_identity,
            str(stage.path),
            stage.group.output_sha256,
            stage.owner_token,
        )
    )
    row_text_bytes = sum(
        len(item.slice.admission.snapshot.source_id.encode())
        + len(stage.stage_id.encode())
        + (
            len(str(cast(Mapping[str, object], item.time_range["utc"])["observation_id"]).encode())
            if item.time_range["utc"] is not None
            else 0
        )
        for item in stage.rows
    )
    row_count = len(stage.rows) + member_rows + 4
    return (
        text_bytes
        + row_text_bytes
        + len(_canonical(stage.group.ranges))
        + len(_canonical(entry))
        + row_count * _TYPED_METADATA_ROW_BYTES
    )


def _require_typed_filesystem_space(path: Path, required: int) -> None:
    filesystem = os.statvfs(path)
    available = filesystem.f_bavail * filesystem.f_frsize
    if available < required:
        raise ReadyBundleError("insufficient filesystem space for typed ready publication")


def _complete_typed_stage_matches(path: Path, group: _TypedGroup) -> bool:
    if path.is_symlink() or not path.is_dir():
        raise ReadyBundleError("owned typed ready stage is unsafe")
    entries = {item.name for item in path.iterdir()}
    if not entries <= {_RAW_NAME, _MANIFEST_NAME}:
        raise ReadyBundleError("owned typed ready stage contains an unsafe entry")
    if entries != {_RAW_NAME, _MANIFEST_NAME}:
        return False
    return _typed_files_match(path, group, require_bundle_name=False)


def _require_typed_destination(path: Path, group: _TypedGroup) -> None:
    if not _typed_files_match(path, group, require_bundle_name=True):
        raise ReadyBundleError("typed ready destination conflicts with its frozen stage")


def _sync_typed_bundle(path: Path, group: _TypedGroup, *, require_bundle_name: bool) -> None:
    if not _typed_files_match(path, group, require_bundle_name=require_bundle_name):
        raise ReadyBundleError("typed ready output conflicts with its frozen stage")
    path.chmod(_READY_DIRECTORY_MODE)
    _sync_regular_file(path / _RAW_NAME)
    _sync_regular_file(path / _MANIFEST_NAME)
    _sync_directory(path)


def _sync_regular_file(path: Path) -> None:
    flags = os.O_RDONLY | os.O_CLOEXEC
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    descriptor = os.open(path, flags)
    try:
        os.fchmod(descriptor, _READY_FILE_MODE)
        try:
            os.fsync(descriptor)
        except OSError as error:
            _raise_if_observed_durability_eio(error)
    finally:
        os.close(descriptor)


def _typed_files_match(path: Path, group: _TypedGroup, *, require_bundle_name: bool) -> bool:
    if path.is_symlink() or not path.is_dir() or {item.name for item in path.iterdir()} != {_RAW_NAME, _MANIFEST_NAME}:
        return False
    manifest = _read_json(path / _MANIFEST_NAME)
    expected = _ready_manifest(group.facts, group.output_sha256, group.ranges)
    if manifest != expected or (require_bundle_name and path.name != _bundle_id(group.facts)):
        return False
    raw = path / _RAW_NAME
    return (
        not raw.is_symlink()
        and raw.is_file()
        and raw.stat().st_size == group.facts.record_count * RECORD_SIZE
        and _digest(raw) == group.output_sha256
    )


def _discard_owned_typed_stage(path: Path) -> None:
    if not os.path.lexists(path):
        return
    if path.is_symlink() or not path.is_dir():
        raise ReadyBundleError("owned typed ready stage is unsafe")
    entries = tuple(path.iterdir())
    if any(item.is_symlink() or not item.is_file() or item.name not in {_RAW_NAME, _MANIFEST_NAME} for item in entries):
        raise ReadyBundleError("owned typed ready stage contains an unsafe entry")
    for item in entries:
        item.unlink()
    path.rmdir()
    _sync_directory(path.parent)


def _write_typed_stage_files(path: Path, group: _TypedGroup, page_limit: int) -> None:
    if os.path.lexists(path):
        raise ReadyBundleError("typed ready stage already exists")
    path.mkdir(mode=_READY_DIRECTORY_MODE)
    path.chmod(_READY_DIRECTORY_MODE)
    digest = sha256()
    range_index = 0
    next_sequence = cast(int, group.ranges[0]["next_sequence"])
    with (path / _RAW_NAME).open("xb") as output:
        for item in group.slices:
            for record in _typed_records(item.admission, item.start_ordinal, item.record_count, page_limit):
                sequence = item.admission.snapshot.sequence_start + record.ordinal
                while sequence >= next_sequence:
                    range_index += 1
                    next_sequence = cast(int, group.ranges[range_index]["next_sequence"])
                converted = _convert_record(record.raw_bytes, group.ranges[range_index])
                try:
                    output.write(converted)
                except OSError as error:
                    _raise_if_observed_durability_eio(error)
                digest.update(converted)
        os.fchmod(output.fileno(), _READY_FILE_MODE)
        try:
            output.flush()
        except OSError as error:
            _raise_if_observed_durability_eio(error)
    _sync_regular_file(path / _RAW_NAME)
    if digest.hexdigest() != group.output_sha256:
        raise ReadyBundleError("typed ready output changed while staging")
    _write_file(path / _MANIFEST_NAME, _canonical(_ready_manifest(group.facts, group.output_sha256, group.ranges)))
    _sync_directory(path)


def _commit_typed_stage(
    audio_store: InternalAudioStore,
    cohort: _TypedCohort,
    stage: _TypedStage,
    result: ReadyBundleResult,
) -> None:
    progress = _typed_member_progress(cohort, stage.group.facts)
    all_complete = all(
        item.new_cursor == admission.admitted_count for item, admission in zip(progress, cohort.admissions, strict=True)
    )
    entry = _ledger_entry(
        stage.group.output_sha256,
        "ready",
        stage.group.facts,
        tuple((item.start_sequence, item.next_sequence, item.raw_sha256) for item in stage.group.slices),
    )
    ledger = _read_ledger(audio_store.state_db)
    existing = cast(dict[str, object], ledger["bundles"]).get(result.bundle_id)
    if existing is not None and (not isinstance(existing, dict) or not _same_ledger_identity(existing, entry)):
        raise ReadyBundleError("typed ready ledger conflicts with its frozen stage")
    coverage = _typed_coverage_proposals(audio_store, cohort, progress)
    proposal = _FinalCommit(cohort, stage, result, entry, progress, coverage, all_complete, ledger)
    for attempt in range(2):
        try:
            _commit_typed_stage_once(audio_store, proposal)
            return
        except (InternalAudioStoreError, sqlite3.DatabaseError, TransientInternalAudioStorageError) as error:
            raise_if_sqlite_corrupt(error)
            final_state = _inspect_typed_final(audio_store, proposal)
            if final_state == "proposed":
                return
            if final_state == "old" and attempt == 0:
                continue
            if final_state == "old":
                raise_if_sqlite_storage_fault(error)
                raise ReadyBundleError("typed ready final commit was rolled back") from error
            _typed_fail_stop(audio_store, "ambiguous typed ready final commit has a third state")
    raise AssertionError("unreachable")


def _typed_member_progress(cohort: _TypedCohort, facts: _BundleFacts) -> tuple[_MemberProgress, ...]:
    runs = _physical_runs(facts)
    result: list[_MemberProgress] = []
    for admission in cohort.admissions:
        absolute = admission.snapshot.sequence_start + admission.cursor
        limit = admission.snapshot.sequence_start + admission.admitted_count
        for start, end in runs:
            if end <= absolute:
                continue
            if start > absolute:
                break
            absolute = min(limit, max(absolute, end))
            if absolute == limit:
                break
        assert admission.member_ordinal is not None
        result.append(
            _MemberProgress(
                admission.member_ordinal,
                admission.cursor,
                absolute - admission.snapshot.sequence_start,
            )
        )
    return tuple(result)


def _typed_coverage_proposals(
    audio_store: InternalAudioStore,
    cohort: _TypedCohort,
    progress: tuple[_MemberProgress, ...],
) -> tuple[_SourceCoverage, ...]:
    proposals: list[_SourceCoverage] = []
    try:
        with (
            closing(audio_store.state_db.open_connection(read_only=True)) as connection,
            audio_store.state_db.access(connection),
        ):
            for item, admission in zip(progress, cohort.admissions, strict=True):
                if item.old_cursor == admission.admitted_count or item.new_cursor != admission.admitted_count:
                    continue
                proposals.append(_typed_coverage_proposal(connection, admission))
    except (OSError, StateDBError, sqlite3.DatabaseError) as error:
        raise_if_sqlite_storage_fault(error)
        raise ReadyBundleError("typed coverage state is unreadable") from error
    return tuple(proposals)


def _typed_coverage_proposal(connection: sqlite3.Connection, admission: _TypedAdmission) -> _SourceCoverage:
    row = _sql_one(
        connection.execute(
            "SELECT lifecycle,source_type,block_reason,quarantine_disposition,terminal_time,retention_deadline,"
            "committed_count,committed_sha256,closure_frontier,certified_prefix_count,coverage_digest,"
            "retirement_cursor "
            "FROM capture_source WHERE source_id=?",
            (admission.snapshot.source_id,),
        )
    )
    if row is None:
        raise ReadyBundleError("typed source disappeared before coverage commit")
    old = tuple(row)
    _require_typed_coverage_source(old, admission)
    proposed = list(old)
    full = admission.admitted_count == admission.snapshot.committed_count
    if not full and admission.admitted_lifecycle != "blocked":
        raise ReadyBundleError("only a certified blocked prefix may retain uncovered source rows")
    _require_audio_decision(
        AudioPhase(cast(str, old[0])),
        ReadyCustody(
            certified_quarantine_prefix=admission.admitted_lifecycle == "blocked",
        ),
        AudioAction.RETIRE,
        AudioPhase.COVERED,
    )
    proposed[0] = "covered"
    proposed[8] = encode_uint64(admission.snapshot.sequence_start + admission.admitted_count)
    proposed[10] = admission.admitted_sha256
    proposed[11] = None
    if full:
        _require_audio_decision(
            AudioPhase.COVERED,
            BeginRetirement(),
            AudioAction.RETIRE,
            AudioPhase.RETIRING,
        )
        proposed[0] = "retiring"
        proposed[11] = 0
    return _SourceCoverage(admission.snapshot.source_id, old, tuple(proposed))


def _cover_typed_full_replays(
    audio_store: InternalAudioStore,
    admissions: tuple[_TypedAdmission, ...],
    custody: _TypedReadyCustody,
    page_limit: int,
) -> tuple[_TypedAdmission, ...]:
    remaining: list[_TypedAdmission] = []
    for admission in admissions:
        try:
            proofs = _typed_full_ready_proofs(admission, custody, page_limit)
        except ConflictingOverlapError, _TypedCustodyConflictError:
            _block_typed_custody_conflict(audio_store, admission)
            raise
        if proofs is None:
            remaining.append(admission)
            continue
        _commit_typed_replay_coverage(audio_store, admission, proofs)
    return tuple(remaining)


def _commit_typed_replay_coverage(
    audio_store: InternalAudioStore,
    admission: _TypedAdmission,
    proofs: tuple[tuple[str, dict[str, object]], ...],
) -> None:
    try:
        with (
            closing(audio_store.state_db.open_connection(read_only=True)) as connection,
            audio_store.state_db.access(connection),
        ):
            proposal = _typed_coverage_proposal(connection, admission)
    except (OSError, StateDBError, sqlite3.DatabaseError) as error:
        raise_if_sqlite_storage_fault(error)
        raise ReadyBundleError("typed replay coverage state is unreadable") from error
    for attempt in range(2):
        try:
            with audio_store.publication_transaction() as (state, connection):
                if any(state.get(StateKind.READY_PUBLICATION, bundle_id) != entry for bundle_id, entry in proofs):
                    raise ReadyBundleError("typed replay custody changed before coverage commit")
                _apply_typed_coverage(connection, (proposal,))
                state.mark_publication_changed()
            return
        except (InternalAudioStoreError, sqlite3.DatabaseError, TransientInternalAudioStorageError) as error:
            raise_if_sqlite_corrupt(error)
            current = _typed_source_coverage_state(audio_store, proposal.source_id)
            if current == proposal.proposed:
                return
            if current == proposal.old and attempt == 0:
                continue
            if current == proposal.old:
                raise_if_sqlite_storage_fault(error)
                raise ReadyBundleError("typed replay coverage commit was rolled back") from error
            _typed_fail_stop(audio_store, "ambiguous typed replay coverage commit has a third state")
    raise AssertionError("unreachable")


def _typed_source_coverage_state(audio_store: InternalAudioStore, source_id: str) -> tuple[object, ...] | None:
    try:
        with (
            closing(audio_store.state_db.open_connection(read_only=True)) as connection,
            audio_store.state_db.access(connection),
        ):
            row = _sql_one(
                connection.execute(
                    "SELECT lifecycle,source_type,block_reason,quarantine_disposition,terminal_time,retention_deadline,"
                    "committed_count,committed_sha256,closure_frontier,certified_prefix_count,coverage_digest,"
                    "retirement_cursor FROM capture_source WHERE source_id=?",
                    (source_id,),
                )
            )
    except (OSError, StateDBError, sqlite3.DatabaseError, TransientInternalAudioStorageError) as error:
        raise_if_sqlite_corrupt(error)
        _typed_fail_stop(audio_store, f"typed replay coverage could not be inspected: {error}")
    return None if row is None else tuple(row)


def _block_typed_custody_conflict(
    audio_store: InternalAudioStore, admission: _TypedAdmission, cohort: _TypedCohort | None = None
) -> None:
    disposition = _typed_conflict_disposition(audio_store, admission, cohort)
    for attempt in range(2):
        try:
            _block_typed_custody_conflict_once(audio_store, disposition)
            return
        except (
            InternalAudioStoreError,
            StateDBError,
            sqlite3.DatabaseError,
            TransientInternalAudioStorageError,
        ) as error:
            raise_if_sqlite_corrupt(error)
            state = _inspect_typed_conflict_disposition(audio_store, disposition)
            if state == "proposed":
                return
            if state == "old" and attempt == 0:
                continue
            if state == "old":
                raise_if_sqlite_storage_fault(error)
                raise ReadyBundleError("typed conflict disposition was rolled back") from error
            _typed_fail_stop(audio_store, "ambiguous typed conflict disposition has a third state")
    raise AssertionError("unreachable")


def _typed_conflict_disposition(
    audio_store: InternalAudioStore,
    admission: _TypedAdmission,
    cohort: _TypedCohort | None,
) -> _ConflictDisposition:
    try:
        with (
            closing(audio_store.state_db.open_connection(read_only=True)) as connection,
            audio_store.state_db.access(connection),
        ):
            row = _typed_conflict_source_row(connection, admission.snapshot.source_id)
            cohort_row = (
                None
                if cohort is None
                else _sql_one(
                    connection.execute(
                        "SELECT accepted_time FROM publication_cohort WHERE cohort_id=?",
                        (cohort.cohort_id,),
                    )
                )
            )
    except (OSError, StateDBError, sqlite3.DatabaseError) as error:
        raise_if_sqlite_storage_fault(error)
        raise ReadyBundleError("typed conflict source state is unreadable") from error
    if row is None:
        raise ReadyBundleError("typed conflict source disappeared")
    if cohort is not None and (cohort_row is None or not isinstance(cohort_row[0], str)):
        raise ReadyBundleError("typed conflict cohort acceptance time is unavailable")
    old = tuple(row)
    source = admission.snapshot
    expected_closure = None if source.closure_frontier is None else encode_uint64(source.closure_frontier)
    if (
        old[0] != source.source_type
        or old[1] != admission.admitted_lifecycle
        or old[11] != source.committed_count
        or old[12] != source.committed_sha256
        or old[13] != expected_closure
    ):
        raise ReadyBundleError("typed conflict source changed before disposition")
    proposed = (
        "quarantine",
        "blocked",
        "ready_overlap_conflict",
        "unclassified",
        0,
        None,
        None,
        None,
        None,
        None,
        None,
        old[11],
        old[12],
        old[13],
    )
    proposed_cohort = None if cohort is None else _typed_conflict_remainder(cohort, source.source_id)
    return _ConflictDisposition(
        admission, cohort, proposed_cohort, old, proposed, None if cohort_row is None else cast(str, cohort_row[0])
    )


def _typed_conflict_remainder(cohort: _TypedCohort, conflict_source_id: str) -> _TypedCohort | None:
    admissions = tuple(item for item in cohort.admissions if item.snapshot.source_id != conflict_source_id)
    if len(admissions) != len(cohort.admissions) - 1:
        raise ReadyBundleError("typed conflict source is not an exact cohort member")
    if not admissions:
        return None
    renumbered = tuple(replace(item, member_ordinal=index) for index, item in enumerate(admissions))
    return _TypedCohort(
        _typed_cohort_id(renumbered, cohort.threshold, cohort.frontier, cohort.config_digest),
        "accepted",
        cohort.threshold,
        cohort.frontier,
        cohort.cursor,
        cohort.config_digest,
        renumbered,
    )


def _typed_conflict_source_row(connection: sqlite3.Connection, source_id: str) -> tuple[object, ...] | None:
    row = _sql_one(
        connection.execute(
            "SELECT source_type,lifecycle,block_reason,quarantine_disposition,quarantine_opaque,terminal_reason,"
            "terminal_time,retention_deadline,certified_prefix_count,coverage_digest,retirement_cursor,"
            "committed_count,committed_sha256,closure_frontier FROM capture_source WHERE source_id=?",
            (source_id,),
        )
    )
    return None if row is None else tuple(row)


def _block_typed_custody_conflict_once(
    audio_store: InternalAudioStore,
    disposition: _ConflictDisposition,
) -> None:
    source_id = disposition.admission.snapshot.source_id
    accepted_time: str | None = None
    with audio_store.publication_transaction() as (state, connection):
        if _typed_conflict_source_row(connection, source_id) != disposition.old:
            raise ReadyBundleError("typed conflict source changed before disposition")
        _require_audio_decision(
            AudioPhase(cast(str, disposition.old[1])),
            AuthenticatedConflict(),
            AudioAction.BLOCK,
            AudioPhase.BLOCKED,
        )
        if disposition.cohort is not None:
            accepted_time = _require_exact_typed_cohort(connection, disposition.cohort, disposition.accepted_time)
            stage_count = _sql_one(connection.execute("SELECT count(*) FROM ready_output_stage"))
            if stage_count is None or stage_count[0] != 0:
                raise ReadyBundleError("typed conflict cohort has an active stage")
            expected_phase = CohortPhase.ACCEPTED if disposition.proposed_cohort is not None else None
            _require_cohort_decision(
                CohortPhase.ACCEPTED,
                CohortConflict(accepted_remainder=disposition.proposed_cohort is not None),
                CohortAction.BLOCK,
                expected_phase,
            )
        changed = connection.execute(
            "UPDATE capture_source SET source_type=?,lifecycle=?,block_reason=?,quarantine_disposition=?,"
            "quarantine_opaque=?,terminal_reason=?,terminal_time=?,retention_deadline=?,certified_prefix_count=?,"
            "coverage_digest=?,retirement_cursor=? WHERE source_id=? AND source_type IS ? AND lifecycle IS ? "
            "AND block_reason IS ? AND quarantine_disposition IS ? AND quarantine_opaque IS ? "
            "AND terminal_reason IS ? AND terminal_time IS ? AND retention_deadline IS ? "
            "AND certified_prefix_count IS ? AND coverage_digest IS ? AND retirement_cursor IS ? "
            "AND committed_count IS ? AND committed_sha256 IS ? AND closure_frontier IS ?",
            (*disposition.proposed[:11], source_id, *disposition.old),
        )
        if changed.rowcount != 1:
            raise ReadyBundleError("typed conflict source changed before disposition")
        if disposition.cohort is not None:
            deleted = connection.execute(
                "DELETE FROM publication_cohort WHERE cohort_id=? AND lifecycle='accepted' AND cursor=?",
                (disposition.cohort.cohort_id, disposition.cohort.cursor),
            )
            if deleted.rowcount != 1:
                raise ReadyBundleError("typed conflict cohort changed before disposition")
            if disposition.proposed_cohort is not None:
                if accepted_time is None:
                    raise ReadyBundleError("typed conflict cohort acceptance time is unavailable")
                _insert_typed_conflict_remainder(connection, disposition.proposed_cohort, accepted_time)
        state.mark_publication_changed()


def _require_exact_typed_cohort(connection: sqlite3.Connection, cohort: _TypedCohort, accepted_time: str | None) -> str:
    row = _sql_one(
        connection.execute(
            "SELECT cohort_id,lifecycle,accepted_permit,drained_threshold,config_digest,closure_frontier,cursor,"
            "accepted_time "
            "FROM publication_cohort WHERE cohort_id=?",
            (cohort.cohort_id,),
        )
    )
    expected = (
        cohort.cohort_id,
        "accepted",
        cohort.cohort_id,
        cohort.threshold,
        cohort.config_digest,
        None if cohort.frontier is None else encode_uint64(cohort.frontier),
        cohort.cursor,
    )
    members = _sql_all(
        connection.execute(
            "SELECT member_ordinal,source_id,slice_start,slice_count,admitted_lifecycle,admitted_count,"
            "admitted_sha256,cursor FROM publication_cohort_member WHERE cohort_id=? ORDER BY member_ordinal",
            (cohort.cohort_id,),
        )
    )
    expected_members = tuple(
        (
            admission.member_ordinal,
            admission.snapshot.source_id,
            0,
            admission.admitted_count,
            admission.admitted_lifecycle,
            admission.admitted_count,
            admission.admitted_sha256,
            admission.cursor,
        )
        for admission in cohort.admissions
    )
    actual = None if row is None else tuple(row[:7])
    actual_members = tuple(tuple(member) for member in members)
    if actual != expected or actual_members != expected_members or row is None or row[7] != accepted_time:
        raise ReadyBundleError("typed conflict cohort changed before disposition")
    return cast(str, row[7])


def _insert_typed_conflict_remainder(
    connection: sqlite3.Connection,
    cohort: _TypedCohort,
    accepted_time: str,
) -> None:
    connection.execute(
        "INSERT INTO publication_cohort(cohort_id,lifecycle,accepted_permit,drained_threshold,config_digest,"
        "closure_frontier,cursor,accepted_time) VALUES (?, 'accepted', ?, ?, ?, ?, ?, ?)",
        (
            cohort.cohort_id,
            cohort.cohort_id,
            cohort.threshold,
            cohort.config_digest,
            None if cohort.frontier is None else encode_uint64(cohort.frontier),
            cohort.cursor,
            accepted_time,
        ),
    )
    connection.executemany(
        "INSERT INTO publication_cohort_member(cohort_id,member_ordinal,source_id,slice_start,slice_count,"
        "admitted_lifecycle,admitted_count,admitted_sha256,cursor) VALUES (?, ?, ?, 0, ?, ?, ?, ?, ?)",
        (
            (
                cohort.cohort_id,
                admission.member_ordinal,
                admission.snapshot.source_id,
                admission.admitted_count,
                admission.admitted_lifecycle,
                admission.admitted_count,
                admission.admitted_sha256,
                admission.cursor,
            )
            for admission in cohort.admissions
        ),
    )


def _inspect_typed_conflict_disposition(
    audio_store: InternalAudioStore,
    disposition: _ConflictDisposition,
) -> str:
    try:
        with (
            closing(audio_store.state_db.open_connection(read_only=True)) as connection,
            audio_store.state_db.access(connection),
        ):
            source = _typed_conflict_source_row(connection, disposition.admission.snapshot.source_id)
            accepted_row = (
                None
                if disposition.cohort is None
                else _sql_one(
                    connection.execute(
                        "SELECT accepted_time FROM publication_cohort WHERE cohort_id IN (?, ?) LIMIT 1",
                        (
                            disposition.cohort.cohort_id,
                            disposition.proposed_cohort.cohort_id if disposition.proposed_cohort is not None else "",
                        ),
                    )
                )
            )
        active = _load_active_for_reconciliation(audio_store) if disposition.cohort is not None else None
    except (
        OSError,
        StateDBError,
        sqlite3.DatabaseError,
        ReadyBundleError,
        InternalAudioStoreError,
        TransientInternalAudioStorageError,
    ) as error:
        raise_if_sqlite_corrupt(error)
        _typed_fail_stop(audio_store, f"typed conflict disposition could not be inspected: {error}")
    proposed_cohort = (
        disposition.cohort is None
        or (disposition.proposed_cohort is None and active is None)
        or (
            disposition.proposed_cohort is not None
            and active is not None
            and _same_typed_cohort(active, disposition.proposed_cohort)
            and accepted_row is not None
            and tuple(accepted_row) == (disposition.accepted_time,)
        )
    )
    if source == disposition.proposed and proposed_cohort:
        return "proposed"
    old_cohort = disposition.cohort is None or (
        active is not None
        and _same_typed_cohort(active, disposition.cohort)
        and accepted_row is not None
        and tuple(accepted_row) == (disposition.accepted_time,)
    )
    if source == disposition.old and old_cohort:
        return "old"
    return "third"


def _require_typed_coverage_source(state: tuple[object, ...], admission: _TypedAdmission) -> None:
    snapshot = admission.snapshot
    expected_closure = None if snapshot.closure_frontier is None else encode_uint64(snapshot.closure_frontier)
    valid = (
        state[0] == admission.admitted_lifecycle
        and state[1] == snapshot.source_type
        and state[6] == snapshot.committed_count
        and state[7] == snapshot.committed_sha256
        and state[8] == expected_closure
        and state[11] is None
    )
    if admission.admitted_lifecycle == "blocked":
        certification = (state[9], state[10], state[2], state[3], state[4], state[5])
        valid = valid and _blocked_typed_certification(snapshot, certification) == (
            admission.admitted_count,
            admission.admitted_sha256,
        )
    if not valid:
        raise ReadyBundleError("typed source changed before coverage commit")


def _commit_typed_stage_once(audio_store: InternalAudioStore, proposal: _FinalCommit) -> None:
    cohort, stage = proposal.cohort, proposal.stage
    ledger = _typed_proposed_ledger(proposal)
    with audio_store.publication_transaction() as (state, connection):
        next_phase = CohortPhase.COMPLETE if proposal.all_complete else CohortPhase.ACCEPTED
        _require_cohort_decision(
            CohortPhase.STAGING,
            DurableDestination(cohort_complete=proposal.all_complete),
            CohortAction.COMMIT,
            next_phase,
        )
        row = _sql_one(
            connection.execute(
                "SELECT lifecycle,cursor FROM publication_cohort WHERE cohort_id=?",
                (cohort.cohort_id,),
            )
        )
        if row is None or tuple(row) != ("staging", cohort.cursor):
            raise ReadyBundleError("typed cohort changed before final commit")
        stage_row = _sql_one(
            connection.execute(
                "SELECT group_identity,owned_path,expected_bundle_digest,expected_record_count,owner_token,failure_reason "
                "FROM ready_output_stage WHERE stage_id=?",
                (stage.stage_id,),
            )
        )
        if stage_row is None or tuple(stage_row) != (
            stage.group_identity,
            str(stage.path),
            stage.group.output_sha256,
            stage.group.facts.record_count,
            stage.owner_token,
            None,
        ):
            raise ReadyBundleError("typed stage ownership changed before final commit")
        state.put(StateKind.READY_PUBLICATION, proposal.result.bundle_id, proposal.entry)
        state.put(StateKind.READY_PUBLICATION, "frontier", {"value": ledger["frontier"]})
        for item in proposal.progress:
            changed = connection.execute(
                "UPDATE publication_cohort_member SET cursor=? WHERE cohort_id=? AND member_ordinal=? AND cursor=?",
                (item.new_cursor, cohort.cohort_id, item.member_ordinal, item.old_cursor),
            )
            if changed.rowcount != 1:
                raise ReadyBundleError("typed cohort member changed before final commit")
        _apply_typed_coverage(connection, proposal.coverage)
        connection.execute("DELETE FROM ready_output_stage WHERE stage_id=?", (stage.stage_id,))
        if proposal.all_complete:
            _require_cohort_decision(
                CohortPhase.COMPLETE,
                CompleteRetire(),
                CohortAction.RETIRE,
                None,
            )
            deleted = connection.execute(
                "DELETE FROM publication_cohort WHERE cohort_id=? AND lifecycle='staging' AND cursor=?",
                (cohort.cohort_id, cohort.cursor),
            )
            if deleted.rowcount != 1:
                raise ReadyBundleError("typed cohort completion changed before final commit")
        else:
            changed = connection.execute(
                "UPDATE publication_cohort SET lifecycle='accepted',cursor=? "
                "WHERE cohort_id=? AND lifecycle='staging' AND cursor=?",
                (cohort.cursor + 1, cohort.cohort_id, cohort.cursor),
            )
            if changed.rowcount != 1:
                raise ReadyBundleError("typed cohort cursor changed before final commit")
        state.mark_publication_changed()


def _typed_proposed_ledger(proposal: _FinalCommit) -> dict[str, object]:
    bundles = dict(cast(dict[str, object], proposal.ledger["bundles"]))
    bundles[proposal.result.bundle_id] = proposal.entry
    return {
        "bundles": bundles,
        "frontier": max(cast(int, proposal.ledger["frontier"]), proposal.stage.group.facts.next_sequence),
    }


def _apply_typed_coverage(connection: sqlite3.Connection, coverage: tuple[_SourceCoverage, ...]) -> None:
    for item in coverage:
        changed = connection.execute(
            "UPDATE capture_source SET lifecycle=?,closure_frontier=?,coverage_digest=?,retirement_cursor=? "
            "WHERE source_id=? AND lifecycle=? AND source_type=? AND block_reason IS ? "
            "AND quarantine_disposition IS ? AND terminal_time IS ? AND retention_deadline IS ? "
            "AND committed_count=? AND committed_sha256=? "
            "AND closure_frontier IS ? AND certified_prefix_count IS ? AND coverage_digest IS ? "
            "AND retirement_cursor IS ?",
            (
                item.proposed[0],
                item.proposed[8],
                item.proposed[10],
                item.proposed[11],
                item.source_id,
                *item.old,
            ),
        )
        if changed.rowcount != 1:
            raise ReadyBundleError("typed source changed before coverage commit")


def _inspect_typed_final(audio_store: InternalAudioStore, proposal: _FinalCommit) -> str:
    cohort, stage = proposal.cohort, proposal.stage
    try:
        ledger = _read_ledger(audio_store.state_db)
        with (
            closing(audio_store.state_db.open_connection(read_only=True)) as connection,
            audio_store.state_db.access(connection),
        ):
            cohort_row = _sql_one(
                connection.execute(
                    "SELECT lifecycle,cursor FROM publication_cohort WHERE cohort_id=?",
                    (cohort.cohort_id,),
                )
            )
            stage_row = _sql_one(
                connection.execute(
                    "SELECT group_identity,failure_reason FROM ready_output_stage WHERE stage_id=?",
                    (stage.stage_id,),
                )
            )
            cursors = tuple(
                _sql_one(
                    connection.execute(
                        "SELECT cursor FROM publication_cohort_member WHERE cohort_id=? AND member_ordinal=?",
                        (cohort.cohort_id, item.member_ordinal),
                    )
                )
                for item in proposal.progress
            )
            sources = tuple(
                (
                    item.source_id,
                    _sql_one(
                        connection.execute(
                            "SELECT lifecycle,source_type,block_reason,quarantine_disposition,terminal_time,"
                            "retention_deadline,"
                            "committed_count,committed_sha256,closure_frontier,certified_prefix_count,"
                            "coverage_digest,retirement_cursor "
                            "FROM capture_source WHERE source_id=?",
                            (item.source_id,),
                        )
                    ),
                )
                for item in proposal.coverage
            )
    except (
        OSError,
        StateDBError,
        sqlite3.DatabaseError,
        ReadyBundleError,
        TransientInternalAudioStorageError,
    ) as error:
        raise_if_sqlite_corrupt(error)
        _typed_fail_stop(audio_store, f"ambiguous typed final commit could not be inspected: {error}")
    proposed_sources = all(
        row is not None and tuple(row) == item.proposed
        for (_source_id, row), item in zip(sources, proposal.coverage, strict=True)
    )
    old_sources = all(
        row is not None and tuple(row) == item.old
        for (_source_id, row), item in zip(sources, proposal.coverage, strict=True)
    )
    proposed_cohort = (
        cohort_row is None
        if proposal.all_complete
        else cohort_row is not None and tuple(cohort_row) == ("accepted", cohort.cursor + 1)
    )
    proposed_cursors = proposal.all_complete or all(
        row is not None and row[0] == item.new_cursor for row, item in zip(cursors, proposal.progress, strict=True)
    )
    if (
        ledger == _typed_proposed_ledger(proposal)
        and proposed_cohort
        and stage_row is None
        and proposed_cursors
        and proposed_sources
    ):
        return "proposed"
    old_cursors = all(
        row is not None and row[0] == item.old_cursor for row, item in zip(cursors, proposal.progress, strict=True)
    )
    durable_control_state = (
        ledger,
        None if cohort_row is None else tuple(cohort_row),
        None if stage_row is None else tuple(stage_row),
    )
    expected_control_state = (
        proposal.ledger,
        ("staging", cohort.cursor),
        (stage.group_identity, None),
    )
    if durable_control_state == expected_control_state and old_cursors and old_sources:
        return "old"
    return "third"


def _retire_one_typed_source(audio_store: InternalAudioStore, page_limit: int) -> bool:
    try:
        with (
            closing(audio_store.state_db.open_connection(read_only=True)) as connection,
            audio_store.state_db.access(connection),
        ):
            row = _sql_one(
                connection.execute(
                    "SELECT source_id,committed_count,committed_sha256,coverage_digest,retirement_cursor "
                    "FROM capture_source WHERE lifecycle='retiring' AND NOT EXISTS "
                    "(SELECT 1 FROM publication_cohort_member WHERE publication_cohort_member.source_id=capture_source.source_id) "
                    "AND (retirement_cursor<committed_count OR (retirement_cursor=committed_count "
                    "AND NOT EXISTS (SELECT 1 FROM capture_tail WHERE capture_tail.source_id=capture_source.source_id) "
                    "AND NOT EXISTS (SELECT 1 FROM capture_conflict WHERE capture_conflict.source_id=capture_source.source_id) "
                    "AND NOT EXISTS (SELECT 1 FROM capture_evidence WHERE capture_evidence.source_id=capture_source.source_id))) "
                    "ORDER BY retirement_cursor=committed_count,source_id LIMIT 1"
                )
            )
    except (OSError, StateDBError, sqlite3.DatabaseError) as error:
        raise_if_sqlite_storage_fault(error)
        raise ReadyBundleError("typed retirement state is unreadable") from error
    if row is None:
        return False
    source_id, committed_count, committed_digest, coverage_digest, old_cursor = row
    if (
        not isinstance(source_id, str)
        or type(committed_count) is not int
        or coverage_digest != committed_digest
        or type(old_cursor) is not int
        or not 0 <= old_cursor <= committed_count
    ):
        _typed_fail_stop(audio_store, "typed retirement coverage proof is invalid")
    step = _RetirementStep(
        source_id,
        committed_count,
        cast(str, committed_digest),
        old_cursor,
        page_limit,
    )
    return _advance_typed_retirement(audio_store, step)


def _advance_typed_retirement(audio_store: InternalAudioStore, step: _RetirementStep) -> bool:
    old_cursor = step.old_cursor
    committed_count = step.committed_count
    for attempt in range(2):
        proposed = None if old_cursor == committed_count else min(committed_count, old_cursor + step.page_limit)
        try:
            result = _retire_typed_source_once(audio_store, step)
            actual = result.cursor
            if not result.progressed:
                if actual != old_cursor:
                    _typed_fail_stop(audio_store, "typed retirement reported invalid stalled progress")
                return False
            if actual == old_cursor == committed_count:
                return False
            if actual != proposed:
                _typed_fail_stop(audio_store, "typed retirement advanced an unexpected cursor")
            return True
        except (InternalAudioStoreError, sqlite3.DatabaseError, TransientInternalAudioStorageError) as error:
            raise_if_sqlite_corrupt(error)
            state = _inspect_typed_retirement(audio_store, step, proposed)
            if state == "proposed":
                return True
            if state == "old" and attempt == 0:
                continue
            if state == "old":
                raise_if_sqlite_storage_fault(error)
                raise ReadyBundleError("typed retirement step was rolled back") from error
            _typed_fail_stop(audio_store, "ambiguous typed retirement has a third state")
    raise AssertionError("unreachable typed retirement retry state")


def _retire_typed_source_once(audio_store: InternalAudioStore, step: _RetirementStep) -> _RetirementResult:
    proposed: int | None = step.old_cursor
    with audio_store.publication_transaction() as (state, connection):
        source = _sql_one(
            connection.execute(
                "SELECT lifecycle,committed_count,committed_sha256,coverage_digest,retirement_cursor "
                "FROM capture_source WHERE source_id=?",
                (step.source_id,),
            )
        )
        if source is None or tuple(source) != (
            "retiring",
            step.committed_count,
            step.coverage_digest,
            step.coverage_digest,
            step.old_cursor,
        ):
            raise ReadyBundleError("typed retirement source changed")
        ordinals = tuple(
            cast(int, row[0])
            for row in _sql_all(
                connection.execute(
                    "SELECT ordinal FROM capture_record WHERE source_id=? AND ordinal>=? ORDER BY ordinal LIMIT ?",
                    (step.source_id, step.old_cursor, step.page_limit),
                )
            )
        )
        retired_residue = _sql_one(
            connection.execute(
                "SELECT count(*) FROM capture_record WHERE source_id=? AND ordinal<?",
                (step.source_id, step.old_cursor),
            )
        )
        if retired_residue is None or retired_residue[0] != 0:
            raise ReadyBundleError("typed retirement cursor does not match retained rows")
        if ordinals:
            _require_audio_decision(
                AudioPhase.RETIRING,
                RetirementStepEvent(),
                AudioAction.RETIRE,
                AudioPhase.RETIRING,
            )
            if ordinals != tuple(range(step.old_cursor, step.old_cursor + len(ordinals))):
                raise ReadyBundleError("typed retirement rows are not contiguous")
            proposed = step.old_cursor + len(ordinals)
            connection.executemany(
                "DELETE FROM capture_record WHERE source_id=? AND ordinal=?",
                ((step.source_id, ordinal) for ordinal in ordinals),
            )
            changed = connection.execute(
                "UPDATE capture_source SET retirement_cursor=? "
                "WHERE source_id=? AND lifecycle='retiring' AND retirement_cursor=?",
                (proposed, step.source_id, step.old_cursor),
            )
            if changed.rowcount != 1:
                raise ReadyBundleError("typed retirement cursor changed")
        else:
            if step.old_cursor != step.committed_count:
                raise ReadyBundleError("typed retirement rows ended before coverage")
            residue = _sql_one(
                connection.execute(
                    "SELECT "
                    "(SELECT count(*) FROM capture_tail WHERE source_id=?),"
                    "(SELECT count(*) FROM capture_conflict WHERE source_id=?),"
                    "(SELECT count(*) FROM capture_evidence WHERE source_id=?),"
                    "(SELECT count(*) FROM publication_cohort_member WHERE source_id=?)",
                    (step.source_id, step.source_id, step.source_id, step.source_id),
                )
            )
            if residue is None or tuple(residue) != (0, 0, 0, 0):
                return _RetirementResult(step.old_cursor, False)
            _require_audio_decision(
                AudioPhase.RETIRING,
                RetirementStepEvent(final=True),
                AudioAction.RETIRE,
                None,
            )
            deleted = connection.execute(
                "DELETE FROM capture_source WHERE source_id=? AND lifecycle='retiring' "
                "AND retirement_cursor=? AND coverage_digest=?",
                (step.source_id, step.old_cursor, step.coverage_digest),
            )
            if deleted.rowcount != 1:
                raise ReadyBundleError("typed retirement source changed before removal")
            proposed = None
        state.mark_publication_changed()
    return _RetirementResult(proposed, True)


def _inspect_typed_retirement(audio_store: InternalAudioStore, step: _RetirementStep, proposed: int | None) -> str:
    try:
        with (
            closing(audio_store.state_db.open_connection(read_only=True)) as connection,
            audio_store.state_db.access(connection),
        ):
            row = _sql_one(
                connection.execute(
                    "SELECT lifecycle,committed_count,committed_sha256,coverage_digest,retirement_cursor "
                    "FROM capture_source WHERE source_id=?",
                    (step.source_id,),
                )
            )
    except (OSError, StateDBError, sqlite3.DatabaseError, TransientInternalAudioStorageError) as error:
        raise_if_sqlite_corrupt(error)
        _typed_fail_stop(audio_store, f"ambiguous typed retirement could not be inspected: {error}")
    if proposed is None and row is None:
        return "proposed"
    if row is not None and tuple(row[:4]) == (
        "retiring",
        step.committed_count,
        step.coverage_digest,
        step.coverage_digest,
    ):
        if row[4] == proposed:
            return "proposed"
        if row[4] == step.old_cursor:
            return "old"
    return "third"


def _typed_fail_stop(audio_store: InternalAudioStore, message: str) -> NoReturn:
    audio_store._fail_stop(message)
