"""Finalize raw draft bundles into immutable, flat ready bundles."""

from __future__ import annotations

import errno
import hmac
import json
import math
import os
import sqlite3
import stat
from bisect import bisect_right
from collections.abc import Iterable, Iterator, Mapping
from contextlib import closing, contextmanager
from dataclasses import dataclass, replace
from datetime import UTC, date, datetime, timedelta
from enum import StrEnum
from hashlib import sha256
from itertools import pairwise, zip_longest
from math import ceil
from pathlib import Path
from typing import NoReturn, Protocol, cast

from ...config import ReadyConfig
from ..application.storage_fault import TransientInternalAudioStorageError
from ..domain.clock_state import ClockState, ClockStateError
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
from ..domain.opus_duration import RecordAudioLayout, inspect_20ms_record, inspect_mono_20ms_record
from ..domain.public_audio_v2 import (
    BoundaryFact,
    FrozenPublicTime,
    FrozenRecordProjection,
    PublicProjection,
    Quality,
    canonical_manifest,
    derive_artifact_id,
    parse_v2_acknowledged,
    project_public_timeline,
)
from ..domain.publication_contract import ContractEvent, ContractMode, contract_policy, contract_transition
from ..domain.replay_proof import (
    NormalizationMode,
    NormalizationRun,
    ProofPage,
    build_proof_pages,
    normalize_record,
    pack_normalization_runs,
    pack_page_index,
    record_sha256,
    unpack_normalization_runs,
    unpack_page_index,
    validate_proof_page,
)
from ..domain.ring_protocol import RECORD_SIZE, TIMESTAMP_SIZE
from .clock_segments import ClockSegmentMap
from .collector_state_db import (
    StateDB,
    StateDBError,
    StateKind,
    StateTransaction,
    raise_if_sqlite_corrupt,
    raise_if_sqlite_storage_fault,
)
from .internal_audio_store import (
    CaptureRecord,
    CaptureSource,
    InternalAudioStore,
    InternalAudioStoreError,
    decode_uint64,
    encode_uint64,
)
from .public_time_facts import FrozenTimePredecessor, freeze_public_time
from .sqlite_compaction import mark_retirement_compaction_pending
from .staging_contract import FatalInternalAudioStorageError
from .v2_audio import V2AudioDigest, hash_v2_audio, write_v2_audio


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


@dataclass(frozen=True, slots=True)
class PublicationContractState:
    """The durable producer contract and next order reservation."""

    mode: ContractMode
    next_public_order: int | None


@dataclass(frozen=True, slots=True)
class PublicAudioV2Options:
    """Inputs that freeze one v2 publication attempt."""

    config: ReadyConfig
    clock_state: ClockState
    frontier: int | None = None
    drained: bool = False
    page_limit: int = 128
    custody_cache: ReadyCustodyCache | None = None


_PUBLICATION_CONTRACT_ID = "current"
_UINT64_MAX = 2**64 - 1
_UINT64_BYTES = 8


def publication_contract_state(state_db: StateDB) -> PublicationContractState:
    """Read and strictly validate the single durable publication contract."""

    payload = state_db.get(StateKind.PUBLICATION_CONTRACT, _PUBLICATION_CONTRACT_ID)
    return _parse_publication_contract(payload)


def begin_publication_cutover(state_db: StateDB) -> PublicationContractState:
    """Stop accepting new legacy cohorts while preserving resumable legacy work."""

    with state_db.transaction() as state:
        current = _parse_publication_contract(state.get(StateKind.PUBLICATION_CONTRACT, _PUBLICATION_CONTRACT_ID))
        try:
            mode = contract_transition(current.mode, ContractEvent.BEGIN_CUTOVER)
        except ValueError as error:
            raise ReadyBundleError("publication contract cannot begin cutover") from error
        proposed = PublicationContractState(mode, current.next_public_order)
        state.put(StateKind.PUBLICATION_CONTRACT, _PUBLICATION_CONTRACT_ID, _publication_contract_payload(proposed))
    return proposed


def activate_public_audio_v2(
    audio_store: InternalAudioStore, ready_root: Path, checkpoint_path: Path
) -> PublicationContractState:
    """Activate v2 only after legacy custody and the consumer checkpoint are drained."""
    if not checkpoint_path.exists():
        raise ReadyBundleError("public audio v2 checkpoint is missing")
    checkpoint = _read_json(checkpoint_path)
    if (
        not isinstance(checkpoint, dict)
        or checkpoint.get("audio_contract") != "omi-audio-v2"
        or not {"analysis_cursor", "vad_decisions", "open_speech_tail", "acknowledged"} <= checkpoint.keys()
    ):
        raise ReadyBundleError("public audio v2 checkpoint is invalid")
    try:
        acknowledged = parse_v2_acknowledged(checkpoint)
    except ValueError as error:
        raise ReadyBundleError("public audio v2 checkpoint is invalid") from error
    if (
        acknowledged
        or checkpoint["analysis_cursor"] is not None
        or checkpoint["vad_decisions"] != []
        or checkpoint["open_speech_tail"] is not None
    ):
        raise ReadyBundleError("public audio v2 checkpoint is not empty at activation")
    ledger = _read_ledger(audio_store.state_db)
    entries = cast(dict[str, object], ledger["bundles"])
    if any(isinstance(entry, dict) and entry.get("format") == "omi-audio-v2" for entry in entries.values()):
        raise ReadyBundleError("public audio v2 publication state already exists")
    if any(
        isinstance(entry, dict) and entry.get("format") != "omi-audio-v2" and entry.get("state") == "ready"
        for entry in entries.values()
    ):
        raise ReadyBundleError("legacy ready custody must be drained before v2 activation")
    _prepare_directory(ready_root)
    if tuple(ready_root.iterdir()):
        raise ReadyBundleError("ready storage must be empty before v2 activation")
    with audio_store.publication_transaction() as (state, connection):
        current = _parse_publication_contract(state.get(StateKind.PUBLICATION_CONTRACT, _PUBLICATION_CONTRACT_ID))
        try:
            mode = contract_transition(current.mode, ContractEvent.ACTIVATE_V2)
        except ValueError as error:
            raise ReadyBundleError("publication contract cannot activate v2") from error
        active = _sql_one(
            connection.execute(
                "SELECT (SELECT count(*) FROM publication_cohort) + "
                "(SELECT count(*) FROM ready_output_stage) + (SELECT count(*) FROM ready_v2_output_stage) + "
                "(SELECT count(*) FROM ready_replay_proof WHERE format='omi-audio-v2')"
            )
        )
        if active != (0,):
            raise ReadyBundleError("publication work must quiesce before v2 activation")
        proposed = PublicationContractState(mode, 0)
        state.put(StateKind.PUBLICATION_CONTRACT, _PUBLICATION_CONTRACT_ID, _publication_contract_payload(proposed))
    return proposed


def _parse_publication_contract(payload: dict[str, object] | None) -> PublicationContractState:
    if payload is None or set(payload) != {"version", "mode", "next_public_order"} or payload["version"] != 1:
        raise ReadyBundleError("publication contract state is invalid")
    try:
        mode = ContractMode(cast(str, payload["mode"]))
    except (TypeError, ValueError) as error:
        raise ReadyBundleError("publication contract mode is invalid") from error
    order = payload["next_public_order"]
    if order is not None and (type(order) is not int or not 0 <= order <= _UINT64_MAX):
        raise ReadyBundleError("publication order state is invalid")
    if (mode is ContractMode.V2_OPEN) != (order is not None):
        raise ReadyBundleError("publication contract order state is invalid")
    return PublicationContractState(mode, cast(int | None, order))


def _publication_contract_payload(contract: PublicationContractState) -> dict[str, object]:
    return {"version": 1, "mode": contract.mode.value, "next_public_order": contract.next_public_order}


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
_V2_AUDIO_NAME = "audio.ogg"
_READY_DIRECTORY_MODE = 0o2750
_READY_DIRECTORY_CREATE_MODE = _READY_DIRECTORY_MODE & 0o777
_READY_FILE_MODE = 0o640
_UINT32_MAX = (1 << 32) - 1
_SHA256_HEX_LENGTH = 64
_GAP_ENDPOINT_COUNT = 2
_LEDGER_FIELDS = frozenset({"bundles", "frontier"})
_LEDGER_ENTRY_FIELDS = frozenset({"records_sha256", "state", "start_sequence", "next_sequence", "draft_raw_sha256"})
_V2_LEDGER_FIELDS = _LEDGER_ENTRY_FIELDS | frozenset(
    {
        "format",
        "order",
        "audio_sha256",
        "audio_bytes",
        "manifest_sha256",
        "manifest_bytes",
        "packet_count",
        "sources",
    }
)
_PUBLICATION_DATE_FIELDS = frozenset({"formed_on_utc", "recorded_on_utc_min", "recorded_on_utc_max"})
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
_V2_STAGE_OWNERSHIP_COLUMNS = 4
_REPLAY_PROOF_ROOT_COLUMNS = 11
_V2_STAGE_ROW_COLUMNS = 18
_V2_STAGE_RANGE_COLUMNS = 7
_V2_REPLAY_PROOF_COLUMNS = 14
_REPLAY_PROOF_PAGE_COLUMNS = 5
_V2_NONCE_BYTES = 32
_MAX_OPUS_PACKET_BYTES = 160
_PROOF_SCRATCH_CACHE_KIB = 256
_DURABILITY_IO_FAILURE = "durability_io_error"


@dataclass(frozen=True, slots=True)
class _ProofOwnership:
    lifecycle: str
    stage_id: str
    owner_token: str


@dataclass(frozen=True, slots=True)
class _ProofContent:
    bundle_id: str
    records_sha256: str
    manifest_sha256: str
    record_count: int
    page_index: bytes
    normalization_runs: bytes
    immutable_metadata: bytes


@dataclass(frozen=True, slots=True)
class _V2ProofContent:
    artifact_id: str
    order: int
    audio_sha256: str
    manifest_sha256: str
    records_sha256: str
    record_count: int
    packet_count: int
    projection_sha256: str
    page_index: bytes
    normalization_runs: bytes
    descriptor: bytes


@dataclass(frozen=True, slots=True)
class _ReadyOverlap:
    source: _ReadySource
    sequence_range: tuple[int, int]
    ready_offset: int


def _visible_children(root: Path) -> tuple[Path, ...]:
    return tuple(path for path in root.iterdir() if not path.name.startswith("."))


def _ready_start(source: _ReadySource) -> int:
    return source.facts.start_sequence


def _read_ready_source(path: Path, *, require_bundle_name: bool = True) -> _ReadySource:
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
    if bundle_id != _bundle_id(facts) or (require_bundle_name and path.name != bundle_id):
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


def _valid_publication_dates(entry: Mapping[str, object]) -> bool:
    if not _PUBLICATION_DATE_FIELDS.intersection(entry):
        return True
    if not _PUBLICATION_DATE_FIELDS.issubset(entry):
        return False
    formed, lower, upper = (entry[field] for field in ("formed_on_utc", "recorded_on_utc_min", "recorded_on_utc_max"))
    if not _canonical_utc_date(formed):
        return False
    if lower is None or upper is None:
        return lower is None and upper is None
    return _canonical_utc_date(lower) and _canonical_utc_date(upper) and cast(str, lower) <= cast(str, upper)


def _canonical_utc_date(value: object) -> bool:
    if not isinstance(value, str):
        return False
    try:
        return date.fromisoformat(value).isoformat() == value
    except ValueError:
        return False


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
    _typed_ready_custody(ready_root, state_db)
    for bundle_id, records_sha256 in acknowledged:
        _preflight_retirement(ready_root, ledger, bundle_id, records_sha256)
    retired: list[ReadyBundleResult] = []
    for bundle_id, records_sha256 in acknowledged:
        retired.append(_retire_one(ready_root, state_db, ledger, bundle_id, records_sha256))
    return tuple(retired)


def acknowledged_public_audio_v2(checkpoint_path: Path) -> tuple[tuple[str, str], ...]:
    """Read one strict public audio v2 acknowledgement snapshot."""
    if not checkpoint_path.exists():
        return ()
    try:
        return parse_v2_acknowledged(_read_json(checkpoint_path))
    except ValueError as error:
        raise ReadyBundleError("public audio v2 checkpoint is invalid") from error


def resume_retired_public_audio_v2(ready_root: Path, state_db: StateDB) -> None:
    """Finish filesystem removal for v2 retirements already committed in SQLite."""
    _prepare_directory(ready_root)
    entries = cast(dict[str, object], _read_ledger(state_db)["bundles"])
    for artifact_id, entry in entries.items():
        if isinstance(entry, dict) and entry.get("format") == "omi-audio-v2" and entry.get("state") == "retired":
            _remove_retired_v2(ready_root / artifact_id)


def retire_acknowledged_public_audio_v2(
    ready_root: Path,
    state_db: StateDB,
    checkpoint_path: Path,
    *,
    acknowledged: tuple[tuple[str, str], ...] | None = None,
) -> tuple[ReadyBundleResult, ...]:
    """Retire v2 artifacts only after the full ACK batch authenticates."""
    if acknowledged is None:
        acknowledged = acknowledged_public_audio_v2(checkpoint_path)
    if not acknowledged:
        return ()
    resume_retired_public_audio_v2(ready_root, state_db)
    cache = _v2_ready_custody(ready_root, state_db)
    ledger = _read_ledger(state_db)
    entries = cast(dict[str, object], ledger["bundles"])
    _require_v2_acknowledgements(entries, cache, acknowledged)
    retired: list[ReadyBundleResult] = []
    for artifact_id, audio_sha256 in acknowledged:
        entry = cast(dict[str, object], entries[artifact_id])
        if entry["state"] == "ready":
            entry = _record_v2_retirement(state_db, artifact_id, entry)
            entries[artifact_id] = entry
        path = ready_root / artifact_id
        _remove_retired_v2(path)
        retired.append(
            ReadyBundleResult(
                artifact_id,
                path,
                _entry_next_sequence(entry),
                _entry_count(entry),
                cast(str, entry["records_sha256"]),
            )
        )
        if entry["audio_sha256"] != audio_sha256:
            raise ReadyBundleError("public audio v2 acknowledgement changed during retirement")
    return tuple(retired)


def _require_v2_acknowledgements(
    entries: Mapping[str, object],
    cache: ReadyCustodyCache,
    acknowledged: tuple[tuple[str, str], ...],
) -> None:
    for artifact_id, audio_sha256 in acknowledged:
        entry = entries.get(artifact_id)
        if (
            not isinstance(entry, dict)
            or entry.get("format") != "omi-audio-v2"
            or entry.get("audio_sha256") != audio_sha256
            or entry.get("state") not in {"ready", "retired"}
        ):
            raise ReadyBundleError("public audio v2 acknowledgement is not a published artifact")
        if entry["state"] == "ready" and artifact_id not in cache.custody.proofs:
            raise ReadyDurabilityFatalError("public audio v2 acknowledgement has no active replay proof")


def _record_v2_retirement(state_db: StateDB, artifact_id: str, entry: dict[str, object]) -> dict[str, object]:
    updated = dict(entry)
    updated["state"] = "retired"
    try:
        with state_db.transaction() as transaction:
            if transaction.get(StateKind.READY_PUBLICATION, artifact_id) != entry:
                raise ReadyBundleError("public audio v2 custody changed before retirement")
            transaction.put(StateKind.READY_PUBLICATION, artifact_id, updated)
            if not transaction.delete_active_replay_proof(artifact_id):
                raise ReadyDurabilityFatalError("public audio v2 ACK found no active replay proof")
    except (OSError, StateDBError) as error:
        raise_if_sqlite_storage_fault(error)
        raise ReadyBundleError("public audio v2 retirement could not be recorded") from error
    return updated


def _remove_retired_v2(path: Path) -> None:
    if not os.path.lexists(path):
        return
    if path.is_symlink() or not path.is_dir():
        raise ReadyBundleError("retired public audio v2 artifact is unsafe")
    entries = {item.name: item for item in path.iterdir()}
    if set(entries) != {_V2_AUDIO_NAME, _MANIFEST_NAME}:
        raise ReadyBundleError("retired public audio v2 artifact inventory is invalid")
    for item in entries.values():
        if item.is_symlink() or not item.is_file():
            raise ReadyBundleError("retired public audio v2 artifact is unsafe")
        item.unlink()
    path.rmdir()
    _sync_directory(path.parent)


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
        retired_entry = dict(entry)
        retired_entry["state"] = "retired"
        try:
            with state_db.transaction() as transaction:
                transaction.put(StateKind.READY_PUBLICATION, bundle_id, retired_entry)
                if not transaction.delete_active_replay_proof(bundle_id):
                    raise ReadyDurabilityFatalError("ACK found no active private replay proof")
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
    if entry.get("format") == "omi-audio-v2":
        return _valid_v2_ledger_entry(bundle_id, entry, start, end)
    optional = set(entry) - _LEDGER_ENTRY_FIELDS
    if (
        not (optional - _PUBLICATION_DATE_FIELDS <= {"gaps", "sources"})
        or bool(optional & _PUBLICATION_DATE_FIELDS) != _PUBLICATION_DATE_FIELDS.issubset(entry)
        or entry.get("state") not in {"ready", "retired"}
        or not _valid_range(start, end)
        or not _is_sha256(entry.get("draft_raw_sha256"))
    ):
        return False
    assert isinstance(start, int) and isinstance(end, int)
    gaps = _parse_ledger_gaps(entry, start, end)
    if gaps is None or not _valid_publication_dates(entry):
        return False
    digest = entry["draft_raw_sha256"]
    assert isinstance(digest, str)
    facts = _BundleFacts(start, end, end - start - sum(b - a for a, b in gaps), digest, gaps)
    return bundle_id == _bundle_id(facts) and (
        "sources" not in entry or _parse_sources(entry["sources"], facts) is not None
    )


def _valid_v2_ledger_entry(_bundle_id: str, entry: Mapping[str, object], start: object, end: object) -> bool:
    valid = all(
        (
            set(entry) == _V2_LEDGER_FIELDS | ({"gaps"} if "gaps" in entry else set()),
            entry.get("state") in {"ready", "retired"},
            _valid_range(start, end),
            type(entry.get("order")) is int,
            type(entry.get("packet_count")) is int,
            type(entry.get("audio_bytes")) is int,
            type(entry.get("manifest_bytes")) is int,
            _is_sha256(entry.get("audio_sha256")),
            _is_sha256(entry.get("manifest_sha256")),
        )
    )
    if not valid:
        return False
    if not all(
        (
            0 <= cast(int, entry["order"]) <= _UINT64_MAX,
            cast(int, entry["packet_count"]) > 0,
            cast(int, entry["audio_bytes"]) > 0,
            cast(int, entry["manifest_bytes"]) > 0,
        )
    ):
        return False
    assert isinstance(start, int) and isinstance(end, int)
    gaps = _parse_ledger_gaps(entry, start, end)
    digest = entry.get("draft_raw_sha256")
    if gaps is None or not isinstance(digest, str):
        return False
    facts = _BundleFacts(start, end, end - start - sum(right - left for left, right in gaps), digest, gaps)
    return _parse_sources(entry.get("sources"), facts) is not None


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
    path.mkdir(mode=_READY_DIRECTORY_CREATE_MODE, parents=True, exist_ok=True)
    if path.is_symlink() or not path.is_dir():
        raise ReadyBundleError("ready storage root is unsafe")


def _require_shared_ready_directory(path: Path) -> None:
    mode = path.stat().st_mode
    required = stat.S_ISGID | stat.S_IRGRP | stat.S_IXGRP
    if mode & required != required:
        raise ReadyBundleError("ready directory is not group-readable and setgid")


def _require_operator_ready_directory(path: Path) -> None:
    directory = path.lstat()
    parent = path.parent.stat()
    if (
        not stat.S_ISDIR(directory.st_mode)
        or stat.S_IMODE(directory.st_mode) != _READY_DIRECTORY_MODE
        or directory.st_gid != parent.st_gid
    ):
        raise ReadyBundleError("ready bundle directory did not inherit operator access")


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


def _replay_normalization_runs(
    ranges: tuple[Mapping[str, object], ...], record_count: int
) -> tuple[NormalizationRun, ...]:
    runs: list[NormalizationRun] = []
    record_index = 0
    previous_sequence: int | None = None
    for item in ranges:
        start = cast(int, item["start_sequence"])
        end = cast(int, item["next_sequence"])
        utc = item["utc"]
        boundary = 0
        if previous_sequence is not None and start != previous_sequence:
            boundary |= 1
            # Current manifests do not retain the causal facts needed to assert time_incomparable.
        if utc is None:
            mode = NormalizationMode.RAW
            offset = 0.0
        else:
            assert isinstance(utc, Mapping)
            mode = NormalizationMode.MAPPED
            offset = _finite_number(utc["offset_seconds"])
        count = end - start
        runs.append(NormalizationRun(record_index, record_index + count, offset, mode, boundary))
        record_index += count
        previous_sequence = end
    packed = pack_normalization_runs(runs, record_count=record_count)
    return unpack_normalization_runs(packed, record_count=record_count)


def _replay_immutable_metadata(
    source: _ReadySource,
    manifest_sha256: str,
    lineage: tuple[SourceLineage, ...],
) -> bytes:
    return _canonical(
        {
            "bundle_id": source.result.bundle_id,
            "draft_raw_sha256": source.facts.raw_sha256,
            "gaps": [list(item) for item in source.facts.gaps],
            "manifest_sha256": manifest_sha256,
            "next_sequence": source.facts.next_sequence,
            "record_count": source.facts.record_count,
            "records_sha256": source.result.records_sha256,
            "sources": [
                {"next_sequence": end, "raw_sha256": digest, "start_sequence": start} for start, end, digest in lineage
            ],
            "start_sequence": source.facts.start_sequence,
            "time_ranges": [dict(item) for item in source.time_ranges],
        }
    )


def _replay_proof_content_sha256(content: _ProofContent) -> str:
    digest = sha256(b"omi-collector-replay-proof-content-v1\0")
    for value in (
        content.bundle_id.encode(),
        content.records_sha256.encode(),
        content.manifest_sha256.encode(),
        content.record_count.to_bytes(8, "big"),
        content.page_index,
        content.normalization_runs,
        content.immutable_metadata,
    ):
        digest.update(len(value).to_bytes(8, "big"))
        digest.update(value)
    return digest.hexdigest()


def backfill_v2_replay_proofs(ready_root: Path, connection: sqlite3.Connection) -> None:
    """Backfill v2 READY and complete owned stages inside the upgrade transaction."""

    ledger = _migration_ledger(connection)
    bundles = cast(dict[str, object], ledger["bundles"])
    known_visible = set(bundles)
    _backfill_v2_active_proofs(ready_root, connection, bundles)
    known_visible.update(_backfill_v2_stage_proofs(ready_root, connection))
    visible = {path.name for path in _visible_children(ready_root)} if ready_root.is_dir() else set()
    if not visible <= known_visible:
        raise ReadyBundleError("v2 READY inventory contains an unowned publication")


def _backfill_v2_active_proofs(ready_root: Path, connection: sqlite3.Connection, bundles: Mapping[str, object]) -> None:
    for bundle_id, value in bundles.items():
        entry = cast(dict[str, object], value)
        if entry["state"] != "ready":
            continue
        source = _read_ready_source(ready_root / bundle_id)
        _validate_ready_ledger_entry(entry, source)
        _insert_migrated_replay_proof(
            connection,
            source,
            source_lineage(entry),
            _ProofOwnership("active", "migration-v2", "migration-v2"),
        )


def _backfill_v2_stage_proofs(ready_root: Path, connection: sqlite3.Connection) -> set[str]:
    stages = _sql_all(
        connection.execute(
            "SELECT stage_id,group_identity,owned_path,owner_token FROM ready_output_stage ORDER BY stage_id"
        )
    )
    visible: set[str] = set()
    for row in stages:
        bundle_id = _backfill_v2_stage_proof(ready_root, connection, row)
        if bundle_id is not None:
            visible.add(bundle_id)
    return visible


def _backfill_v2_stage_proof(ready_root: Path, connection: sqlite3.Connection, row: tuple[object, ...]) -> str | None:
    if len(row) != _V2_STAGE_OWNERSHIP_COLUMNS or not all(isinstance(value, str) for value in row):
        raise ReadyBundleError("v2 stage ownership is invalid")
    stage_id, identity, owned_path, owner_token = cast(tuple[str, str, str, str], row)
    parts = identity.split(":")
    if len(parts) != _TYPED_STAGE_IDENTITY_PARTS or not _is_sha256(parts[2]):
        raise ReadyBundleError("v2 stage identity is invalid")
    bundle_id = parts[2]
    destination = ready_root / bundle_id
    stage_path = Path(owned_path)
    proof_path = destination if destination.exists() else stage_path
    if not proof_path.exists():
        return None
    entries = {item.name for item in proof_path.iterdir()} if proof_path.is_dir() else set()
    if entries != {_RAW_NAME, _MANIFEST_NAME}:
        if proof_path == stage_path and entries <= {_RAW_NAME, _MANIFEST_NAME}:
            return None
        raise ReadyBundleError("v2 completed stage inventory is invalid")
    source = _read_ready_source(proof_path, require_bundle_name=False)
    if source.result.bundle_id != bundle_id:
        raise ReadyBundleError("v2 completed stage identity is invalid")
    _insert_migrated_replay_proof(
        connection,
        source,
        _migration_stage_lineage(connection, stage_id),
        _ProofOwnership("staging", stage_id, owner_token),
    )
    return bundle_id if proof_path == destination else None


def _migration_ledger(connection: sqlite3.Connection) -> dict[str, object]:
    bundles: dict[str, object] = {}
    frontier = 0
    rows = _sql_all(
        connection.execute(
            "SELECT id,payload FROM state WHERE kind=? ORDER BY id", (StateKind.READY_PUBLICATION.value,)
        )
    )
    try:
        for item_id, payload in rows:
            if not isinstance(item_id, str) or not isinstance(payload, str):
                raise ValueError("ledger row type")
            value = cast(object, json.loads(payload))
            if item_id == "frontier":
                if not isinstance(value, dict) or set(value) != {"value"}:
                    raise ValueError("frontier")
                frontier = value["value"]
            else:
                bundles[item_id] = value
        _validate_ledger_values(bundles, frontier)
        _validate_ledger_entries(bundles)
    except (json.JSONDecodeError, ValueError, TypeError) as error:
        raise ReadyBundleError("v2 ready publication ledger is invalid") from error
    return {"bundles": bundles, "frontier": frontier}


def _insert_migrated_replay_proof(
    connection: sqlite3.Connection,
    source: _ReadySource,
    lineage: tuple[SourceLineage, ...],
    ownership: _ProofOwnership,
) -> None:
    with _proof_page_scratch(
        _ready_normalized_records(source.result.path / _RAW_NAME),
        record_count=source.facts.record_count,
    ) as (page_index, scratch):
        runs = _replay_normalization_runs(source.time_ranges, source.facts.record_count)
        packed_runs = pack_normalization_runs(runs, record_count=source.facts.record_count)
        manifest_sha256 = _digest(source.result.path / _MANIFEST_NAME)
        metadata = _replay_immutable_metadata(source, manifest_sha256, lineage)
        content_sha256 = _replay_proof_content_sha256(
            _ProofContent(
                source.result.bundle_id,
                source.result.records_sha256,
                manifest_sha256,
                source.facts.record_count,
                page_index,
                packed_runs,
                metadata,
            )
        )
        connection.execute(
            "INSERT INTO ready_replay_proof(bundle_id,lifecycle,stage_id,owner_token,record_count,records_sha256,"
            "manifest_sha256,page_index,normalization_runs,immutable_metadata,proof_content_sha256) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                source.result.bundle_id,
                ownership.lifecycle,
                ownership.stage_id,
                ownership.owner_token,
                source.facts.record_count,
                source.result.records_sha256,
                manifest_sha256,
                page_index,
                packed_runs,
                metadata,
                content_sha256,
            ),
        )
        _insert_replay_proof_pages(connection, source.result.bundle_id, scratch)


def _ready_normalized_records(path: Path) -> Iterator[bytes]:
    with path.open("rb") as stream:
        while record := stream.read(RECORD_SIZE):
            if len(record) != RECORD_SIZE:
                raise ReadyBundleError("v2 READY records end in a partial record")
            yield record


@contextmanager
def _proof_page_scratch(records: Iterable[bytes], *, record_count: int) -> Iterator[tuple[bytes, sqlite3.Connection]]:
    try:
        scratch = sqlite3.connect(":memory:")
    except sqlite3.DatabaseError as error:
        raise_if_sqlite_storage_fault(error)
        raise ReadyBundleError("private replay proof scratch could not be opened") from error
    try:
        try:
            scratch.execute("PRAGMA temp_store = FILE")
            mode = _sql_one(scratch.execute("PRAGMA temp_store"))
            if mode != (1,):
                raise ReadyBundleError("private replay proof scratch is not file-backed")
            scratch.execute(f"PRAGMA temp.cache_size = -{_PROOF_SCRATCH_CACHE_KIB}")
            cache_size = _sql_one(scratch.execute("PRAGMA temp.cache_size"))
            if cache_size != (-_PROOF_SCRATCH_CACHE_KIB,):
                raise ReadyBundleError("private replay proof scratch cache is unbounded")
            scratch.execute(
                "CREATE TEMP TABLE replay_proof_page ("
                "page_ordinal INTEGER PRIMARY KEY, first_record_index INTEGER NOT NULL, record_count INTEGER NOT NULL,"
                "hashes BLOB NOT NULL, page_sha256 BLOB NOT NULL) WITHOUT ROWID"
            )

            def prepared_pages() -> Iterator[ProofPage]:
                for ordinal, page in enumerate(build_proof_pages(records)):
                    scratch.execute(
                        "INSERT INTO replay_proof_page VALUES (?, ?, ?, ?, ?)",
                        (ordinal, page.first_record_index, page.record_count, page.hashes, page.page_sha256),
                    )
                    yield page

            page_index = pack_page_index(prepared_pages(), record_count=record_count)
            scratch.commit()
        except sqlite3.DatabaseError as error:
            raise_if_sqlite_storage_fault(error)
            raise ReadyBundleError("private replay proof scratch failed") from error
        yield page_index, scratch
    finally:
        scratch.close()


def _replay_proof_page_values(connection: sqlite3.Connection) -> Iterator[tuple[object, ...]]:
    yield from _stream_sql_rows(
        connection.execute(
            "SELECT page_ordinal,first_record_index,record_count,hashes,page_sha256 "
            "FROM replay_proof_page ORDER BY page_ordinal"
        )
    )


def _stream_sql_rows(cursor: sqlite3.Cursor) -> Iterator[tuple[object, ...]]:
    try:
        while True:
            raw_batch = cast(object, cursor.fetchmany(_TYPED_PAGE_MAX))
            if not isinstance(raw_batch, list):
                raise ReadyBundleError("SQLite row stream returned an invalid batch")
            if not raw_batch:
                return
            for raw_row in raw_batch:
                if not isinstance(raw_row, (tuple, sqlite3.Row)):
                    raise ReadyBundleError("SQLite row stream returned an invalid row")
                yield tuple(cast(Iterable[object], raw_row))
    finally:
        cursor.close()


def _insert_replay_proof_pages(
    connection: sqlite3.Connection,
    bundle_id: str,
    scratch: sqlite3.Connection,
) -> None:
    connection.executemany(
        "INSERT INTO ready_replay_proof_page(bundle_id,page_ordinal,first_record_index,record_count,hashes,"
        "page_sha256) VALUES (?, ?, ?, ?, ?, ?)",
        ((bundle_id, *values) for values in _replay_proof_page_values(scratch)),
    )


def _migration_stage_lineage(connection: sqlite3.Connection, stage_id: str) -> tuple[SourceLineage, ...]:
    rows = _sql_all(
        connection.execute(
            "SELECT source_id,start_ordinal,record_count,sequence_start,next_sequence FROM ready_stage_range "
            "WHERE stage_id=? ORDER BY range_ordinal",
            (stage_id,),
        )
    )
    if not rows:
        raise ReadyBundleError("v2 completed stage has no frozen ranges")
    combined: list[tuple[str, int, int, int, int]] = []
    for source_id, start, count, sequence_start, next_sequence in rows:
        if not (
            isinstance(source_id, str)
            and type(start) is int
            and type(count) is int
            and isinstance(sequence_start, bytes)
            and isinstance(next_sequence, bytes)
        ):
            raise ReadyBundleError("v2 completed stage range is invalid")
        sequence = decode_uint64(sequence_start)
        end_sequence = decode_uint64(next_sequence)
        item = (source_id, start, count, sequence, end_sequence)
        if combined and (
            combined[-1][0] == source_id and combined[-1][1] + combined[-1][2] == start and combined[-1][4] == sequence
        ):
            previous = combined[-1]
            combined[-1] = (source_id, previous[1], previous[2] + count, previous[3], end_sequence)
        else:
            combined.append(item)
    lineage: list[SourceLineage] = []
    for source_id, start, count, sequence, end_sequence in combined:
        digest = sha256()
        records = connection.execute(
            "SELECT ordinal,device_timestamp,payload FROM capture_record WHERE source_id=? AND ordinal>=? "
            "AND ordinal<? ORDER BY ordinal",
            (source_id, start, start + count),
        )
        expected = start
        for ordinal, timestamp, payload in _stream_sql_rows(records):
            if (
                type(ordinal) is not int
                or ordinal != expected
                or type(timestamp) is not int
                or not isinstance(payload, bytes)
            ):
                raise ReadyBundleError("v2 completed stage source row is invalid")
            digest.update(timestamp.to_bytes(TIMESTAMP_SIZE, "big") + payload)
            expected += 1
        if expected != start + count:
            raise ReadyBundleError("v2 completed stage source rows are incomplete")
        lineage.append((sequence, end_sequence, digest.hexdigest()))
    return tuple(lineage)


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
    certified_prefix_count: int | None = None
    coverage_digest: str | None = None


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
    recorded_on_utc_min: str | None
    recorded_on_utc_max: str | None


@dataclass(frozen=True, slots=True)
class _TypedReadyCustody:
    state_db: StateDB
    active: tuple[_ReadySource, ...]
    entries: dict[str, dict[str, object]]
    proofs: dict[str, _ReplayProof]


@dataclass(frozen=True, slots=True)
class _ReplayProof:
    bundle_id: str
    lifecycle: str
    stage_id: str
    owner_token: str
    record_count: int
    records_sha256: str
    manifest_sha256: str
    page_index: bytes
    normalization_runs: bytes
    immutable_metadata: bytes
    proof_content_sha256: str
    index: tuple[tuple[int, int, bytes], ...]
    runs: tuple[NormalizationRun, ...]


@dataclass(frozen=True, slots=True)
class ReadyCustodyCache:
    """Authenticated ready custody reusable while its complete identity is unchanged."""

    revision: SourceRevision
    proof_revision: tuple[tuple[object, ...], ...]
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
class _V2Frontier:
    sequence: int
    time: FrozenPublicTime
    pending_overflow_size: int | None


@dataclass(frozen=True, slots=True)
class _V2PreparedGroup:
    slices: tuple[_TypedSlice, ...]
    facts: _BundleFacts
    projection_sha256: str
    projection: PublicProjection
    audio: V2AudioDigest
    config_sha256: str
    clock_state: ClockState
    initial_frontier: _V2Frontier | None
    final_frontier: _V2Frontier


@dataclass(frozen=True, slots=True)
class _V2Stage:
    stage_id: str
    group_identity: str
    path: Path
    artifact_id: str
    order: int
    nonce: bytes
    descriptor: bytes
    descriptor_sha256: str
    manifest: bytes
    manifest_sha256: str
    group: _V2PreparedGroup
    owner_token: str


@dataclass(frozen=True, slots=True)
class _StoredV2Stage:
    stage_id: str
    identity: str
    owned_path: str
    nonce: bytes
    order: int
    artifact_id: str
    config_sha256: str
    descriptor: bytes
    descriptor_sha256: str
    audio_sha256: str
    manifest_sha256: str
    physical_count: int
    packet_count: int
    page_count: int
    audio_bytes: int
    pending_overflow: int | None


@dataclass(frozen=True, slots=True)
class _V2ActiveProofRow:
    stage_id: str
    owner: str
    record_count: int
    records_sha256: str
    manifest_sha256: str
    page_index: bytes
    packed_runs: bytes
    descriptor: bytes
    content_sha256: str
    order: int
    audio_sha256: str
    packet_count: int


@dataclass(frozen=True, slots=True)
class _V2ResumeContext:
    ready_root: Path
    clock_state: ClockState
    page_limit: int
    custody_cache: ReadyCustodyCache | None
    prepared_slices: tuple[_TypedSlice, ...] | None


@dataclass(frozen=True, slots=True)
class _V2Commit:
    cohort: _TypedCohort
    stage: _V2Stage
    entry: dict[str, object]
    progress: tuple[_MemberProgress, ...]
    coverage: tuple[_SourceCoverage, ...]
    all_complete: bool


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
        row = cast(tuple[object, ...] | sqlite3.Row | None, cursor.fetchone())
        return None if row is None else tuple(row)
    finally:
        cursor.close()


def _sql_all(cursor: sqlite3.Cursor) -> tuple[tuple[object, ...], ...]:
    try:
        rows = cast(list[tuple[object, ...] | sqlite3.Row], cursor.fetchall())
        return tuple(tuple(row) for row in rows)
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
    allow_accept: bool = True,
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
            allow_accept=allow_accept,
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
    allow_accept: bool,
) -> ReadyOutcome:
    _validate_typed_request(frontier, drained, page_limit)
    if not isinstance(allow_accept, bool):
        raise ReadyBundleError("typed publication acceptance policy is invalid")
    _prepare_directory(ready_root)
    _require_shared_ready_directory(ready_root)
    threshold = ceil(config.target_audio_seconds * 50)
    config_digest = sha256(f"target_audio_seconds={config.target_audio_seconds.hex()}".encode()).hexdigest()
    cohort = _load_active_typed_cohort(audio_store)
    stats: ReadyPublicationStats | None = None
    prepared_slices: tuple[_TypedSlice, ...] | None = None
    if cohort is None:
        if not allow_accept:
            return ReadyOutcome(
                ReadyOutcomeState.WAITING,
                reason="publication_contract_quiescing",
                remaining_at_frontier=True,
            )
        custody_cache = _typed_ready_custody(ready_root, audio_store.state_db, custody_cache)
        custody = custody_cache.custody
        admissions = _typed_source_admissions(audio_store, sources, frontier)
        if drained:
            admissions = _cover_typed_full_replays(audio_store, admissions, custody, page_limit)
        _require_typed_admissions_current(audio_store, admissions)
        if not drained:
            retirement_pending = _retire_one_typed_source(audio_store, page_limit)
            return ReadyOutcome(
                ReadyOutcomeState.WAITING,
                reason="waiting_for_drain",
                remaining_at_frontier=bool(admissions),
                custody_cache=custody_cache,
                retirement_pending=retirement_pending,
            )
        summary = _disjoint_packet_summary(audio_store, admissions, custody, page_limit, threshold)
        summary_packets = summary[0] if summary is not None else None
        if summary_packets is not None and summary_packets < threshold:
            _persist_typed_packet_summaries(audio_store, summary[1])
            _require_typed_admissions_current(audio_store, admissions)
            _require_typed_summary_certification_current(audio_store, admissions)
            return ReadyOutcome(
                ReadyOutcomeState.WAITING,
                reason="waiting_for_threshold",
                remaining_at_frontier=bool(admissions),
                stats=summary[2],
                custody_cache=custody_cache,
                retirement_pending=_retire_one_typed_source(audio_store, page_limit),
            )
        try:
            slices = _normalize_typed_admissions(
                admissions, page_limit, custody, () if summary is None else summary[1]
            )
        except _TypedAdmissionConflictError as error:
            admission = next(item for item in admissions if item.snapshot.source_id == error.source_id)
            _block_typed_custody_conflict(audio_store, admission)
            raise
        _require_typed_admissions_current(audio_store, admissions)
        stats = _typed_stats(admissions, slices, threshold)
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


def finalize_public_audio_v2(
    audio_store: InternalAudioStore,
    sources: tuple[TypedPublicationSource, ...] | list[TypedPublicationSource],
    ready_root: Path,
    options: PublicAudioV2Options,
) -> ReadyOutcome:
    """Publish or resume one v2 cohort under the durable v2 contract."""
    with audio_store.publication_reader():
        outcome = _finalize_public_audio_v2(audio_store, sources, ready_root, options)
        retirement_pending = _retire_one_typed_source(audio_store, options.page_limit)
        return replace(outcome, retirement_pending=retirement_pending)


def _finalize_public_audio_v2(
    audio_store: InternalAudioStore,
    sources: tuple[TypedPublicationSource, ...] | list[TypedPublicationSource],
    ready_root: Path,
    options: PublicAudioV2Options,
) -> ReadyOutcome:
    _validate_typed_request(options.frontier, options.drained, options.page_limit)
    contract = publication_contract_state(audio_store.state_db)
    if not contract_policy(contract.mode).allow_v2_accept:
        raise ReadyBundleError("public audio v2 contract is not open")
    _prepare_directory(ready_root)
    _require_shared_ready_directory(ready_root)
    threshold = ceil(options.config.target_audio_seconds * 50)
    cohort = _load_active_typed_cohort(audio_store)
    stats: ReadyPublicationStats | None = None
    prepared_slices: tuple[_TypedSlice, ...] | None = None
    custody_cache = options.custody_cache
    if cohort is None:
        custody_cache = _v2_ready_custody(ready_root, audio_store.state_db, custody_cache)
        custody = custody_cache.custody
        admissions = _typed_source_admissions(audio_store, sources, options.frontier)
        if options.drained:
            admissions = _cover_typed_full_replays(audio_store, admissions, custody, options.page_limit)
        _require_typed_admissions_current(audio_store, admissions)
        if not options.drained:
            return ReadyOutcome(
                ReadyOutcomeState.WAITING,
                reason="waiting_for_drain",
                remaining_at_frontier=bool(admissions),
                custody_cache=custody_cache,
            )
        summary = _disjoint_packet_summary(audio_store, admissions, custody, options.page_limit, threshold)
        summary_packets = summary[0] if summary is not None else None
        if summary_packets is not None and summary_packets < threshold:
            _persist_typed_packet_summaries(audio_store, summary[1])
            _require_typed_admissions_current(audio_store, admissions)
            _require_typed_summary_certification_current(audio_store, admissions)
            return ReadyOutcome(
                ReadyOutcomeState.WAITING,
                reason="waiting_for_threshold",
                remaining_at_frontier=bool(admissions),
                stats=summary[2],
                custody_cache=custody_cache,
            )
        slices = _normalize_typed_admissions(
            admissions, options.page_limit, custody, () if summary is None else summary[1]
        )
        _require_typed_admissions_current(audio_store, admissions)
        stats = _typed_stats(admissions, slices, threshold)
        if not slices or stats.audio_packets < threshold:
            return ReadyOutcome(
                ReadyOutcomeState.WAITING,
                reason="waiting_for_threshold",
                remaining_at_frontier=bool(admissions),
                stats=stats,
                custody_cache=custody_cache,
            )
        cohort = _create_typed_cohort(audio_store, admissions, threshold, options.frontier, _v2_config_sha256())
        prepared_slices = _rebind_typed_slices(slices, cohort.admissions)
    published, custody_cache = _resume_v2_cohort(
        audio_store,
        cohort,
        _V2ResumeContext(
            ready_root,
            options.clock_state,
            options.page_limit,
            custody_cache,
            prepared_slices,
        ),
    )
    return ReadyOutcome(
        ReadyOutcomeState.PUBLISHED if published else ReadyOutcomeState.WAITING,
        tuple(published),
        None if published else "already_published",
        bool(cohort.admissions) and not published,
        stats=stats,
        custody_cache=custody_cache,
    )


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
    certified_count = certification[0] if type(certification[0]) is int else None
    coverage_digest = certification[1] if isinstance(certification[1], str) else None
    return _TypedAdmission(
        reader,
        snapshot,
        snapshot.lifecycle,
        count,
        digest,
        certified_prefix_count=certified_count,
        coverage_digest=coverage_digest,
    )


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


def _replay_proof_rows(state_db: StateDB) -> tuple[tuple[object, ...], ...]:
    try:
        with closing(state_db.open_connection(read_only=True)) as connection, state_db.access(connection):
            return _sql_all(
                connection.execute(
                    "SELECT bundle_id,lifecycle,stage_id,owner_token,record_count,records_sha256,manifest_sha256,"
                    "page_index,normalization_runs,immutable_metadata,proof_content_sha256 "
                    "FROM ready_replay_proof ORDER BY bundle_id"
                )
            )
    except (OSError, StateDBError, sqlite3.DatabaseError) as error:
        raise_if_sqlite_storage_fault(error)
        raise ReadyDurabilityFatalError("private replay proof roots are unreadable") from error


def _active_replay_proofs(
    rows: tuple[tuple[object, ...], ...],
    entries: Mapping[str, dict[str, object]],
    sources: tuple[_ReadySource, ...],
) -> dict[str, _ReplayProof]:
    active_rows = {
        cast(str, row[0]): row for row in rows if len(row) == _REPLAY_PROOF_ROOT_COLUMNS and row[1] == "active"
    }
    active_ids = {bundle_id for bundle_id, entry in entries.items() if entry["state"] == "ready"}
    if set(active_rows) != active_ids:
        raise ReadyDurabilityFatalError("active private replay proofs do not match the READY ledger")
    source_by_id = {source.result.bundle_id: source for source in sources}
    proofs: dict[str, _ReplayProof] = {}
    try:
        for bundle_id in sorted(active_ids):
            proofs[bundle_id] = _validated_active_replay_proof(
                bundle_id,
                active_rows[bundle_id],
                source_by_id[bundle_id],
                entries[bundle_id],
            )
    except (KeyError, IndexError, ReadyBundleError, ValueError) as error:
        raise ReadyDurabilityFatalError("active private replay proof is corrupt") from error
    return proofs


def _validated_active_replay_proof(
    bundle_id: str,
    row: tuple[object, ...],
    source: _ReadySource,
    entry: Mapping[str, object],
) -> _ReplayProof:
    if not (
        isinstance(row[2], str)
        and isinstance(row[3], str)
        and type(row[4]) is int
        and isinstance(row[5], str)
        and isinstance(row[6], str)
        and isinstance(row[7], bytes)
        and isinstance(row[8], bytes)
        and isinstance(row[9], bytes)
        and isinstance(row[10], str)
    ):
        raise ValueError("proof root column type is invalid")
    record_count = cast(int, row[4])
    index = unpack_page_index(cast(bytes, row[7]), record_count=record_count)
    runs = unpack_normalization_runs(cast(bytes, row[8]), record_count=record_count)
    manifest_sha256 = _digest(source.result.path / _MANIFEST_NAME)
    expected_runs = _replay_normalization_runs(source.time_ranges, source.facts.record_count)
    metadata = _replay_immutable_metadata(source, manifest_sha256, source_lineage(entry))
    content_sha256 = _replay_proof_content_sha256(
        _ProofContent(
            bundle_id,
            source.result.records_sha256,
            manifest_sha256,
            source.facts.record_count,
            cast(bytes, row[7]),
            cast(bytes, row[8]),
            metadata,
        )
    )
    stored_identity = (record_count, row[5], row[6], runs, row[9], row[10])
    expected_identity = (
        source.facts.record_count,
        source.result.records_sha256,
        manifest_sha256,
        expected_runs,
        metadata,
        content_sha256,
    )
    if stored_identity != expected_identity:
        raise ValueError("proof root does not match immutable READY metadata")
    return _ReplayProof(
        bundle_id,
        "active",
        cast(str, row[2]),
        cast(str, row[3]),
        record_count,
        cast(str, row[5]),
        manifest_sha256,
        cast(bytes, row[7]),
        cast(bytes, row[8]),
        metadata,
        content_sha256,
        index,
        runs,
    )


def _typed_ready_custody(
    ready_root: Path,
    state_db: StateDB,
    previous: ReadyCustodyCache | None = None,
) -> ReadyCustodyCache:
    ledger = _read_ledger(state_db)
    bundles = cast(dict[str, object], ledger["bundles"])
    entries = {bundle_id: cast(dict[str, object], entry) for bundle_id, entry in bundles.items()}
    revision = source_revision(ready_root)
    proof_rows = _replay_proof_rows(state_db)
    if (
        previous is not None
        and previous.revision == revision
        and previous.proof_revision == proof_rows
        and previous.custody.entries == entries
    ):
        return previous
    active_ids = {bundle_id for bundle_id, entry in entries.items() if entry["state"] == "ready"}
    paths = tuple(sorted(_visible_children(ready_root), key=lambda path: path.name))
    if {path.name for path in paths} != active_ids:
        raise ReadyBundleError("typed ready files do not match the active publication ledger")
    active = tuple(sorted((_read_ready_source(path) for path in paths), key=_ready_start))
    for source in active:
        _validate_ready_ledger_entry(entries[source.result.bundle_id], source)
    proofs = _active_replay_proofs(proof_rows, entries, active)
    if revision != source_revision(ready_root) or entries != cast(dict[str, object], _read_ledger(state_db)["bundles"]):
        raise ReadyBundleError("typed ready custody changed during authentication")
    return ReadyCustodyCache(revision, proof_rows, _TypedReadyCustody(state_db, active, entries, proofs))


def _typed_ready_overlap(
    admission: _TypedAdmission,
    custody: _TypedReadyCustody,
    overlap: _ReadyOverlap,
    page_limit: int,
) -> None:
    start, end = overlap.sequence_range
    ordinal = start - admission.snapshot.sequence_start
    proof = custody.proofs[overlap.source.result.bundle_id]
    expected_hashes = _replay_proof_hashes(custody.state_db, proof, overlap.ready_offset, end - start)
    run_index = 0
    for offset, (record, expected_hash) in enumerate(
        zip(_typed_records(admission, ordinal, end - start, page_limit), expected_hashes, strict=True)
    ):
        publication_index = overlap.ready_offset + offset
        while publication_index >= proof.runs[run_index].next_record_index:
            run_index += 1
        try:
            normalized = normalize_record(record.raw_bytes, proof.runs[run_index])
        except ValueError as error:
            raise ConflictingOverlapError("typed ready overlap cannot be normalized") from error
        if not hmac.compare_digest(record_sha256(normalized), expected_hash):
            raise ConflictingOverlapError("typed ready overlap conflicts with its source record")


def _replay_proof_hashes(
    state_db: StateDB, proof: _ReplayProof, first_record_index: int, record_count: int
) -> tuple[bytes, ...]:
    end = first_record_index + record_count
    if first_record_index < 0 or record_count <= 0 or end > proof.record_count:
        raise ReadyDurabilityFatalError("private replay proof request is outside its root")
    hashes: list[bytes] = []
    try:
        with closing(state_db.open_connection(read_only=True)) as connection, state_db.access(connection):
            connection.execute("BEGIN")
            root = _sql_one(
                connection.execute(
                    "SELECT lifecycle,proof_content_sha256 FROM ready_replay_proof WHERE bundle_id=?",
                    (proof.bundle_id,),
                )
            )
            if root is None or tuple(root) != ("active", proof.proof_content_sha256):
                raise ValueError("proof root changed")
            for page_ordinal, (page_first, page_count, page_sha256) in enumerate(proof.index):
                page_end = page_first + page_count
                if page_end <= first_record_index or page_first >= end:
                    continue
                row = _sql_one(
                    connection.execute(
                        "SELECT first_record_index,record_count,hashes,page_sha256 "
                        "FROM ready_replay_proof_page WHERE bundle_id=? AND page_ordinal=?",
                        (proof.bundle_id, page_ordinal),
                    )
                )
                if row is None or not (
                    type(row[0]) is int
                    and type(row[1]) is int
                    and isinstance(row[2], bytes)
                    and isinstance(row[3], bytes)
                ):
                    raise ValueError("proof page is missing")
                page = ProofPage(cast(int, row[0]), cast(int, row[1]), cast(bytes, row[2]), cast(bytes, row[3]))
                values = validate_proof_page(page, expected_first_record_index=page_first)
                if (page.record_count, page.page_sha256) != (page_count, page_sha256):
                    raise ValueError("proof page does not match index")
                left = max(first_record_index, page_first) - page_first
                right = min(end, page_end) - page_first
                hashes.extend(values[left:right])
    except (OSError, StateDBError, sqlite3.DatabaseError, ValueError) as error:
        raise_if_sqlite_storage_fault(error)
        raise ReadyDurabilityFatalError("private replay proof page is corrupt") from error
    if len(hashes) != record_count:
        raise ReadyDurabilityFatalError("private replay proof pages do not cover the replay")
    return tuple(hashes)


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
                admission,
                custody,
                _ReadyOverlap(source, (cursor, overlap_end), ready_offset + cursor - run_start),
                page_limit,
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
                admission,
                custody,
                _ReadyOverlap(source, (cursor, overlap_end), ready_offset + cursor - run_start),
                page_limit,
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


def _require_typed_summary_certification_current(
    audio_store: InternalAudioStore, admissions: tuple[_TypedAdmission, ...]
) -> None:
    try:
        with (
            closing(audio_store.state_db.open_connection(read_only=True)) as connection,
            audio_store.state_db.access(connection),
        ):
            for admission in admissions:
                row = _sql_one(
                    connection.execute(
                        "SELECT certified_prefix_count,coverage_digest FROM capture_source WHERE source_id=?",
                        (admission.snapshot.source_id,),
                    )
                )
                if row is None or tuple(row) != (admission.certified_prefix_count, admission.coverage_digest):
                    raise ReadyBundleError("typed packet summary certification changed")
    except (OSError, StateDBError, sqlite3.DatabaseError) as error:
        raise_if_sqlite_storage_fault(error)
        raise ReadyBundleError("typed packet summary certification is unreadable") from error


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
    admissions: tuple[_TypedAdmission, ...],
    page_limit: int,
    custody: _TypedReadyCustody | None = None,
    precomputed: tuple[_TypedSlice, ...] = (),
) -> tuple[_TypedSlice, ...]:
    normalized: list[_TypedSlice] = []
    ends: list[int] = []
    prepared = {item.admission.snapshot.source_id: item for item in precomputed}
    for admission in admissions:
        remaining = admission.admitted_count - admission.cursor
        if remaining <= 0:
            continue
        candidate = prepared.pop(admission.snapshot.source_id, None) or _typed_slice(
            admission, admission.cursor, remaining, page_limit
        )
        if candidate.admission != admission or candidate.start_ordinal != admission.cursor or candidate.record_count != remaining:
            raise ReadyBundleError("typed precomputed source slice does not match its admission")
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
                    custody,
                    _ReadyOverlap(source, (cursor, overlap_end), ready_offset + cursor - run_start),
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


def _disjoint_packet_summary(
    audio_store: InternalAudioStore,
    admissions: tuple[_TypedAdmission, ...],
    custody: _TypedReadyCustody,
    page_limit: int,
    threshold: int,
) -> tuple[int, tuple[_TypedSlice, ...], ReadyPublicationStats] | None:
    intervals = tuple(
        (item.snapshot.sequence_start, item.snapshot.sequence_start + item.admitted_count) for item in admissions
    )
    if any(item.cursor != 0 or item.member_ordinal is not None for item in admissions):
        return None
    if any(left[1] > right[0] for left, right in pairwise(intervals)):
        return None
    ready_runs = tuple(
        run
        for entry in custody.entries.values()
        for run in _physical_runs(_entry_facts(entry))
    )
    if any(start < ready_end and ready_start < end for start, end in intervals for ready_start, ready_end in ready_runs):
        return None
    packet_counts: list[int] = []
    fresh_slices: list[_TypedSlice] = []
    for admission in admissions:
        payload = audio_store.state_db.get(StateKind.PACKET_SUMMARY, admission.snapshot.source_id)
        if not _valid_packet_summary(payload, admission):
            slice_ = _typed_slice(admission, 0, admission.admitted_count, page_limit)
            if slice_.raw_sha256 != admission.admitted_sha256:
                raise ReadyBundleError("typed packet summary source hash changed")
            count = slice_.audio_packets
            fresh_slices.append(slice_)
        else:
            count = cast(int, payload["audio_packets"])
        packet_counts.append(count)
    gaps = tuple((left[1], right[0]) for left, right in pairwise(intervals) if left[1] < right[0])
    stats = ReadyPublicationStats(
        source_count=len(admissions),
        record_count=sum(item.admitted_count for item in admissions),
        audio_packets=sum(packet_counts),
        threshold_packets=threshold,
        gap_count=len(gaps),
        gap_records=sum(end - start for start, end in gaps),
    )
    return sum(packet_counts), tuple(fresh_slices), stats


def _typed_packet_fingerprint(admission: _TypedAdmission) -> str:
    return sha256(
        json.dumps(
            (
                admission.snapshot.source_id,
                admission.snapshot.source_type,
                admission.snapshot.lifecycle,
                admission.snapshot.sequence_start,
                admission.snapshot.sequence_limit,
                admission.snapshot.committed_count,
                admission.snapshot.committed_sha256,
                admission.admitted_count,
                admission.admitted_sha256,
                admission.certified_prefix_count,
                admission.coverage_digest,
            ),
            separators=(",", ":"),
        ).encode()
    ).hexdigest()


def _persist_typed_packet_summaries(
    audio_store: InternalAudioStore, fresh_slices: tuple[_TypedSlice, ...]
) -> None:
    if not fresh_slices:
        return
    try:
        with audio_store.publication_transaction() as (state, connection):
            for slice_ in fresh_slices:
                admission = slice_.admission
                row = _sql_one(
                    connection.execute(
                        "SELECT source_type,lifecycle,sequence_start,sequence_limit,committed_count,"
                        "committed_sha256,certified_prefix_count,coverage_digest "
                        "FROM capture_source WHERE source_id=?",
                        (admission.snapshot.source_id,),
                    )
                )
                expected = (
                    admission.snapshot.source_type,
                    admission.admitted_lifecycle,
                    encode_uint64(admission.snapshot.sequence_start),
                    encode_uint64(admission.snapshot.sequence_limit),
                    admission.snapshot.committed_count,
                    admission.snapshot.committed_sha256,
                    admission.certified_prefix_count,
                    admission.coverage_digest,
                )
                if row is None or tuple(row) != expected:
                    raise ReadyBundleError("typed packet summary source changed during scan")
                state.put(
                    StateKind.PACKET_SUMMARY,
                    admission.snapshot.source_id,
                    {
                        "schema_version": 1,
                        "sequence_start": admission.snapshot.sequence_start,
                        "admitted_count": admission.admitted_count,
                        "admitted_sha256": admission.admitted_sha256,
                        "audio_packets": slice_.audio_packets,
                        "source_fingerprint": _typed_packet_fingerprint(admission),
                    },
                )
    except InternalAudioStoreError as error:
        code = error.sqlite_errorcode
        if type(code) is not int or code & 0xFF not in {sqlite3.SQLITE_BUSY, sqlite3.SQLITE_LOCKED}:
            raise
        try:
            for slice_ in fresh_slices:
                admission = slice_.admission
                if audio_store.state_db.get(StateKind.PACKET_SUMMARY, admission.snapshot.source_id) != {
                    "schema_version": 1,
                    "sequence_start": admission.snapshot.sequence_start,
                    "admitted_count": admission.admitted_count,
                    "admitted_sha256": admission.admitted_sha256,
                    "audio_packets": slice_.audio_packets,
                    "source_fingerprint": _typed_packet_fingerprint(admission),
                }:
                    return
        except TransientInternalAudioStorageError:
            return
    except TransientInternalAudioStorageError:
        return


def _valid_packet_summary(payload: dict[str, object] | None, admission: _TypedAdmission) -> bool:
    return (
        payload is not None
        and set(payload)
        == {"schema_version", "sequence_start", "admitted_count", "admitted_sha256", "audio_packets", "source_fingerprint"}
        and type(payload["schema_version"]) is int
        and payload["schema_version"] == 1
        and type(payload["sequence_start"]) is int
        and payload["sequence_start"] == admission.snapshot.sequence_start
        and type(payload["admitted_count"]) is int
        and payload["admitted_count"] == admission.admitted_count
        and isinstance(payload["admitted_sha256"], str)
        and hmac.compare_digest(cast(str, payload["admitted_sha256"]), admission.admitted_sha256)
        and type(payload["audio_packets"]) is int
        and payload["audio_packets"] >= 0
        and payload["source_fingerprint"] == _typed_packet_fingerprint(admission)
    )


def _typed_groups(
    slices: tuple[_TypedSlice, ...], page_limit: int, clock_segments: ClockSegmentMap
) -> tuple[_TypedGroup, ...]:
    return tuple(
        _make_typed_group(group, page_limit, clock_segments) for group in _typed_physical_groups(slices, page_limit)
    )


def _typed_physical_groups(slices: tuple[_TypedSlice, ...], page_limit: int) -> tuple[tuple[_TypedSlice, ...], ...]:
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
    return tuple(tuple(group) for group in groups)


def _v2_physical_groups(slices: tuple[_TypedSlice, ...], page_limit: int) -> tuple[tuple[_TypedSlice, ...], ...]:
    groups: list[tuple[_TypedSlice, ...]] = []
    pending: list[_TypedSlice] = []
    for group in _typed_physical_groups(slices, page_limit):
        if sum(item.audio_packets for item in group) == 0:
            pending.extend(group)
            continue
        groups.append((*pending, *group))
        pending.clear()
    if pending and groups:
        groups[-1] = (*groups[-1], *pending)
    return tuple(groups)


def _v2_config_sha256() -> str:
    return sha256(
        _canonical(
            {
                "format": "omi-audio-v2",
                "packets_per_page": 50,
                "samples_per_packet": 960,
                "channels": 1,
                "pre_skip": 0,
                "input_sample_rate": 16_000,
                "output_gain": 0,
                "mapping_family": 0,
                "stream_serial": 0,
                "vendor": "",
                "comments": [],
            }
        )
    ).hexdigest()


def _v2_frontier(state_db: StateDB) -> _V2Frontier | None:
    payload = state_db.get(StateKind.PUBLICATION_V2_FRONTIER, "current")
    if payload is None:
        return None
    return _v2_frontier_from_payload(payload)


def _v2_frontier_from_payload(payload: Mapping[str, object]) -> _V2Frontier:
    if set(payload) != {"version", "sequence", "time", "pending_overflow_size"} or payload["version"] != 1:
        raise ReadyBundleError("v2 publication frontier is invalid")
    sequence = payload["sequence"]
    time_value = payload["time"]
    pending = payload["pending_overflow_size"]
    valid_time = isinstance(time_value, Mapping) and set(time_value) == {
        "quality",
        "seconds",
        "utc_uncertainty_seconds",
        "causal_membership",
        "comparable_to_previous",
    }
    valid_pending = pending is None or (type(pending) is int and 1 <= pending <= _MAX_OPUS_PACKET_BYTES)
    if type(sequence) is not int or not 0 <= sequence <= _UINT64_MAX or not valid_time or not valid_pending:
        raise ReadyBundleError("v2 publication frontier is invalid")
    time_mapping = cast(Mapping[str, object], time_value)
    try:
        time = FrozenPublicTime(
            cast(Quality, time_mapping["quality"]),
            cast(int, time_mapping["seconds"]),
            cast(float | None, time_mapping["utc_uncertainty_seconds"]),
            cast(str, time_mapping["causal_membership"]),
            cast(bool, time_mapping["comparable_to_previous"]),
        )
        project_public_timeline((FrozenRecordProjection(1, time),))
    except (TypeError, ValueError) as error:
        raise ReadyBundleError("v2 publication frontier is invalid") from error
    return _V2Frontier(sequence, time, cast(int | None, pending))


def _v2_frontier_payload(frontier: _V2Frontier) -> dict[str, object]:
    return {
        "version": 1,
        "sequence": frontier.sequence,
        "time": {
            "quality": frontier.time.quality,
            "seconds": frontier.time.seconds,
            "utc_uncertainty_seconds": frontier.time.utc_uncertainty_seconds,
            "causal_membership": frontier.time.causal_membership,
            "comparable_to_previous": frontier.time.comparable_to_previous,
        },
        "pending_overflow_size": frontier.pending_overflow_size,
    }


def _v2_physical_records(slices: tuple[_TypedSlice, ...], page_limit: int) -> Iterator[tuple[int, CaptureRecord]]:
    for item in slices:
        for record in _typed_records(item.admission, item.start_ordinal, item.record_count, page_limit):
            yield item.admission.snapshot.sequence_start + record.ordinal, record


def _prepare_v2_group(
    slices: tuple[_TypedSlice, ...],
    page_limit: int,
    clock_state: ClockState,
    initial_frontier: _V2Frontier | None,
) -> _V2PreparedGroup:
    if not slices:
        raise ReadyBundleError("cannot publish an empty v2 group")
    raw_digest = sha256()

    def packets() -> Iterator[bytes]:
        for _sequence, record in _v2_physical_records(slices, page_limit):
            raw_digest.update(record.raw_bytes)
            try:
                layout = inspect_mono_20ms_record(record.raw_bytes)
            except ValueError as error:
                raise ReadyBundleError("v2 source contains invalid mono 20 ms Opus") from error
            yield from layout.packets

    try:
        audio = hash_v2_audio(packets())
    except ValueError as error:
        raise ReadyBundleError("v2 audio stream is invalid") from error
    projection, final_frontier, projection_sha256 = _freeze_v2_public_projection(
        slices, page_limit, clock_state, initial_frontier
    )
    count = sum(item.record_count for item in slices)
    gaps = tuple(
        (left.next_sequence, right.start_sequence)
        for left, right in pairwise(slices)
        if left.next_sequence < right.start_sequence
    )
    facts = _BundleFacts(slices[0].start_sequence, slices[-1].next_sequence, count, raw_digest.hexdigest(), gaps)
    return _V2PreparedGroup(
        slices,
        facts,
        projection_sha256,
        projection,
        audio,
        _v2_config_sha256(),
        clock_state,
        initial_frontier,
        final_frontier,
    )


def _freeze_v2_public_projection(
    slices: tuple[_TypedSlice, ...],
    page_limit: int,
    clock_state: ClockState,
    initial_frontier: _V2Frontier | None,
) -> tuple[PublicProjection, _V2Frontier, str]:
    digest = sha256(b"omi-audio-v2-record-projection-v1\0")
    final_frontier = initial_frontier
    seen = False

    def records() -> Iterator[FrozenRecordProjection]:
        nonlocal final_frontier, seen
        for index, (_sequence, _record, _layout, projected, frontier) in enumerate(
            _v2_projected_records(slices, page_limit, clock_state, initial_frontier)
        ):
            value = _canonical(
                {
                    "record_index": index,
                    "packet_count": projected.packet_count,
                    "time": {
                        "quality": projected.time.quality,
                        "seconds": projected.time.seconds,
                        "utc_uncertainty_seconds": projected.time.utc_uncertainty_seconds,
                        "causal_membership": projected.time.causal_membership,
                        "comparable_to_previous": projected.time.comparable_to_previous,
                    },
                    "boundary_before": sorted(projected.boundary_before),
                }
            )
            digest.update(len(value).to_bytes(8, "big"))
            digest.update(value)
            final_frontier = frontier
            seen = True
            yield projected

    projection = project_public_timeline(records())
    if not seen or final_frontier is None:
        raise ReadyBundleError("v2 group has no physical records")
    return projection, final_frontier, digest.hexdigest()


def _v2_projected_records(
    slices: tuple[_TypedSlice, ...],
    page_limit: int,
    clock_state: ClockState,
    initial_frontier: _V2Frontier | None,
) -> Iterator[tuple[int, CaptureRecord, RecordAudioLayout, FrozenRecordProjection, _V2Frontier]]:
    previous = initial_frontier
    for sequence, record in _v2_physical_records(slices, page_limit):
        try:
            layout = inspect_mono_20ms_record(record.raw_bytes)
        except ValueError as error:
            raise ReadyBundleError("v2 source contains invalid mono 20 ms Opus") from error
        adjacent = previous is not None and previous.sequence + 1 == sequence
        predecessor = FrozenTimePredecessor(previous.sequence, previous.time) if adjacent and previous else None
        time = freeze_public_time(
            clock_state,
            sequence=sequence,
            raw_timestamp=record.device_timestamp,
            predecessor=predecessor,
        )
        facts: set[BoundaryFact] = set()
        if previous is not None and not adjacent:
            facts.add("source_loss")
        elif (
            previous is not None
            and previous.pending_overflow_size is not None
            and previous.pending_overflow_size != layout.first_size
        ):
            facts.add("time_incomparable")
        if not time.comparable_to_previous:
            facts.add("time_incomparable")
        projected = FrozenRecordProjection(layout.packet_count, time, frozenset(facts))
        previous = _V2Frontier(sequence, time, layout.overflow_size)
        yield sequence, record, layout, projected, previous


def _v2_descriptor(group: _V2PreparedGroup) -> bytes:
    initial = None if group.initial_frontier is None else _v2_frontier_payload(group.initial_frontier)
    return _canonical(
        {
            "format": "omi-audio-v2-private-descriptor-v1",
            "config_sha256": group.config_sha256,
            "projector": "public-time-facts-v1",
            "clock_state": group.clock_state.to_payload(),
            "physical": {
                "start_sequence": group.facts.start_sequence,
                "next_sequence": group.facts.next_sequence,
                "record_count": group.facts.record_count,
                "raw_sha256": group.facts.raw_sha256,
                "gaps": [list(item) for item in group.facts.gaps],
            },
            "sources": [
                {
                    "source_id": item.admission.snapshot.source_id,
                    "start_ordinal": item.start_ordinal,
                    "record_count": item.record_count,
                    "sequence_start": item.start_sequence,
                    "next_sequence": item.next_sequence,
                    "raw_sha256": item.raw_sha256,
                }
                for item in group.slices
            ],
            "record_projection_sha256": group.projection_sha256,
            "initial_frontier": initial,
            "final_frontier": _v2_frontier_payload(group.final_frontier),
        }
    )


def _v2_normalization_runs(group: _V2PreparedGroup, page_limit: int) -> tuple[NormalizationRun, ...]:
    runs: list[NormalizationRun] = []
    for index, (_sequence, record, _layout, projection, _frontier) in enumerate(
        _v2_projected_records(group.slices, page_limit, group.clock_state, group.initial_frontier)
    ):
        if projection.time.quality == "unknown":
            mode = NormalizationMode.RAW
            offset = 0.0
        else:
            mode = NormalizationMode.MAPPED
            offset = float(projection.time.seconds - record.device_timestamp)
        boundary = int("source_loss" in projection.boundary_before)
        boundary |= 2 * int(
            "time_incomparable" in projection.boundary_before or not projection.time.comparable_to_previous
        )
        if runs and boundary == 0 and runs[-1].mode is mode and runs[-1].offset_seconds == offset:
            runs[-1] = replace(runs[-1], next_record_index=index + 1)
        else:
            runs.append(NormalizationRun(index, index + 1, offset, mode, boundary))
    return tuple(runs)


def _v2_normalized_records(
    group: _V2PreparedGroup,
    page_limit: int,
    runs: tuple[NormalizationRun, ...],
) -> Iterator[bytes]:
    run_index = 0
    for index, (_, record) in enumerate(_v2_physical_records(group.slices, page_limit)):
        while index >= runs[run_index].next_record_index:
            run_index += 1
        yield normalize_record(record.raw_bytes, runs[run_index])


def _v2_packets_from_frozen(group: _V2PreparedGroup, page_limit: int) -> Iterator[bytes]:
    projection, final_frontier, projection_sha256 = _freeze_v2_public_projection(
        group.slices, page_limit, group.clock_state, group.initial_frontier
    )
    if (
        projection != group.projection
        or final_frontier != group.final_frontier
        or projection_sha256 != group.projection_sha256
    ):
        raise ReadyBundleError("v2 frozen record projection changed")
    for _sequence, _record, layout, _projection, _frontier in _v2_projected_records(
        group.slices, page_limit, group.clock_state, group.initial_frontier
    ):
        yield from layout.packets


def _v2_proof_content_sha256(content: _V2ProofContent) -> str:
    digest = sha256(b"omi-collector-replay-proof-v2\0")
    for value in (
        bytes.fromhex(content.artifact_id),
        content.order.to_bytes(8, "big"),
        bytes.fromhex(content.audio_sha256),
        bytes.fromhex(content.manifest_sha256),
        bytes.fromhex(content.records_sha256),
        content.record_count.to_bytes(8, "big"),
        content.packet_count.to_bytes(8, "big"),
        bytes.fromhex(content.projection_sha256),
        content.page_index,
        content.normalization_runs,
        content.descriptor,
    ):
        digest.update(len(value).to_bytes(8, "big"))
        digest.update(value)
    return digest.hexdigest()


def _require_v2_stage_space(audio_store: InternalAudioStore, ready_root: Path, group: _V2PreparedGroup) -> None:
    database = audio_store.state_db.path
    try:
        with (
            closing(audio_store.state_db.open_connection(read_only=True)) as connection,
            audio_store.state_db.access(connection),
        ):
            page_size_row = _sql_one(connection.execute("PRAGMA page_size"))
            page_count_row = _sql_one(connection.execute("PRAGMA page_count"))
        if page_size_row is None or page_count_row is None:
            raise ReadyBundleError("v2 publication SQLite page accounting is unavailable")
        page_size, page_count = cast(int, page_size_row[0]), cast(int, page_count_row[0])
        if page_size <= 0 or page_count <= 0:
            raise ReadyBundleError("v2 publication SQLite page accounting is invalid")
        proof_pages = ceil(group.facts.record_count / 128)
        payload = (
            group.facts.record_count * 64
            + proof_pages * 64
            + len(_v2_descriptor(group))
            + len(group.slices) * _TYPED_METADATA_ROW_BYTES
            + 16 * 1024
        )
        mutations = len(group.slices) + proof_pages + 12
        dirty_pages = _TYPED_BTREE_PAGES_PER_LEVEL * max(1, page_count.bit_length()) * mutations
        database_bytes = (
            min(page_count, dirty_pages) + dirty_pages + ceil(payload / page_size) + _TYPED_JOURNAL_MARGIN_PAGES
        ) * page_size
        output_bytes = group.audio.byte_count + 64 * 1024
        ready_stat = ready_root.stat()
        database_stat = database.stat()
        if ready_stat.st_dev == database_stat.st_dev:
            _require_typed_filesystem_space(ready_root, output_bytes + database_bytes + _TYPED_SPACE_HEADROOM)
        else:
            _require_typed_filesystem_space(ready_root, output_bytes + _TYPED_SPACE_HEADROOM)
            _require_typed_filesystem_space(database.parent, database_bytes + _TYPED_SPACE_HEADROOM)
    except ReadyBundleError:
        raise
    except (OSError, StateDBError, sqlite3.DatabaseError, TypeError, IndexError) as error:
        raise_if_sqlite_storage_fault(error)
        raise ReadyBundleError("v2 publication space reserve could not be inspected") from error


def _create_v2_stage(
    audio_store: InternalAudioStore,
    ready_root: Path,
    cohort: _TypedCohort,
    group: _V2PreparedGroup,
    page_limit: int,
) -> _V2Stage:
    _require_v2_stage_space(audio_store, ready_root, group)
    descriptor = _v2_descriptor(group)
    descriptor_sha256 = sha256(descriptor).hexdigest()
    runs = _v2_normalization_runs(group, page_limit)
    packed_runs = pack_normalization_runs(runs, record_count=group.facts.record_count)
    records_digest = sha256()

    def normalized_records() -> Iterator[bytes]:
        for record in _v2_normalized_records(group, page_limit, runs):
            records_digest.update(record)
            yield record

    with (
        _proof_page_scratch(normalized_records(), record_count=group.facts.record_count) as (page_index, scratch),
        audio_store.publication_transaction() as (state, connection),
    ):
        contract = _parse_publication_contract(state.get(StateKind.PUBLICATION_CONTRACT, _PUBLICATION_CONTRACT_ID))
        if contract.mode is not ContractMode.V2_OPEN or contract.next_public_order is None:
            raise ReadyBundleError("v2 publication contract is not open")
        if contract.next_public_order == _UINT64_MAX:
            raise ReadyBundleError("v2 publication order is exhausted")
        if _sql_one(
            connection.execute(
                "SELECT (SELECT count(*) FROM ready_output_stage) + (SELECT count(*) FROM ready_v2_output_stage)"
            )
        ) != (0,):
            raise ReadyBundleError("another publication stage is active")
        if _sql_one(
            connection.execute("SELECT lifecycle,cursor FROM publication_cohort WHERE cohort_id=?", (cohort.cohort_id,))
        ) != ("accepted", cohort.cursor):
            raise ReadyBundleError("v2 cohort changed before stage intent")
        order = contract.next_public_order
        nonce = os.urandom(_V2_NONCE_BYTES)
        artifact_id = derive_artifact_id(
            stage_nonce=nonce,
            order=order,
            config_sha256=group.config_sha256,
            descriptor_sha256=descriptor_sha256,
        )
        identity = f"{cohort.cohort_id}:{cohort.cursor}:{artifact_id}"
        stage_id = sha256(identity.encode()).hexdigest()
        path = ready_root / f".{artifact_id}.{stage_id[:16]}.stage"
        manifest = canonical_manifest(
            artifact_id=artifact_id,
            order=order,
            audio_sha256=group.audio.sha256,
            projection=group.projection,
        )
        manifest_sha256 = sha256(manifest).hexdigest()
        connection.execute(
            "INSERT INTO ready_v2_output_stage(stage_id,group_identity,owned_path,stage_nonce,public_order,"
            "artifact_id,config_sha256,descriptor,descriptor_sha256,expected_audio_sha256,"
            "expected_manifest_sha256,expected_physical_count,expected_packet_count,expected_page_count,"
            "expected_audio_bytes,pending_overflow_size,owner_token) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                stage_id,
                identity,
                str(path),
                nonce,
                encode_uint64(order),
                artifact_id,
                group.config_sha256,
                descriptor,
                descriptor_sha256,
                group.audio.sha256,
                manifest_sha256,
                group.facts.record_count,
                group.audio.packet_count,
                group.audio.page_count,
                group.audio.byte_count,
                group.final_frontier.pending_overflow_size,
                stage_id,
            ),
        )
        connection.executemany(
            "INSERT INTO ready_v2_stage_range(stage_id,range_ordinal,source_id,start_ordinal,record_count,"
            "sequence_start,next_sequence,raw_sha256) VALUES (?,?,?,?,?,?,?,?)",
            (
                (
                    stage_id,
                    index,
                    item.admission.snapshot.source_id,
                    item.start_ordinal,
                    item.record_count,
                    encode_uint64(item.start_sequence),
                    encode_uint64(item.next_sequence),
                    item.raw_sha256,
                )
                for index, item in enumerate(group.slices)
            ),
        )
        connection.execute(
            "INSERT INTO ready_replay_proof(bundle_id,lifecycle,stage_id,owner_token,record_count,records_sha256,"
            "manifest_sha256,page_index,normalization_runs,immutable_metadata,proof_content_sha256,format,"
            "public_order,audio_sha256,packet_count) "
            "VALUES (?,'staging',?,?,?,?,?,?,?,?,?,'omi-audio-v2',?,?,?)",
            (
                artifact_id,
                stage_id,
                stage_id,
                group.facts.record_count,
                records_digest.hexdigest(),
                manifest_sha256,
                page_index,
                packed_runs,
                descriptor,
                _v2_proof_content_sha256(
                    _V2ProofContent(
                        artifact_id,
                        order,
                        group.audio.sha256,
                        manifest_sha256,
                        records_digest.hexdigest(),
                        group.facts.record_count,
                        group.audio.packet_count,
                        group.projection_sha256,
                        page_index,
                        packed_runs,
                        descriptor,
                    )
                ),
                encode_uint64(order),
                group.audio.sha256,
                group.audio.packet_count,
            ),
        )
        _insert_replay_proof_pages(connection, artifact_id, scratch)
        changed = connection.execute(
            "UPDATE publication_cohort SET lifecycle='staging' WHERE cohort_id=? AND lifecycle='accepted' AND cursor=?",
            (cohort.cohort_id, cohort.cursor),
        )
        if changed.rowcount != 1:
            raise ReadyBundleError("v2 cohort changed before stage intent")
        state.put(
            StateKind.PUBLICATION_CONTRACT,
            _PUBLICATION_CONTRACT_ID,
            _publication_contract_payload(PublicationContractState(ContractMode.V2_OPEN, order + 1)),
        )
        state.mark_publication_changed()
    return _V2Stage(
        stage_id,
        identity,
        path,
        artifact_id,
        order,
        nonce,
        descriptor,
        descriptor_sha256,
        manifest,
        manifest_sha256,
        group,
        stage_id,
    )


def _v2_descriptor_basis(
    descriptor: bytes,
) -> tuple[ClockState, _V2Frontier | None, _V2Frontier, str]:
    try:
        value = cast(object, json.loads(descriptor))
    except (UnicodeDecodeError, json.JSONDecodeError, TypeError) as error:
        raise ReadyBundleError("v2 stage descriptor is invalid") from error
    if not isinstance(value, dict) or _canonical(value) != descriptor:
        raise ReadyBundleError("v2 stage descriptor is invalid")
    if (
        set(value)
        != {
            "format",
            "config_sha256",
            "projector",
            "clock_state",
            "physical",
            "sources",
            "record_projection_sha256",
            "initial_frontier",
            "final_frontier",
        }
        or value.get("format") != "omi-audio-v2-private-descriptor-v1"
        or value.get("projector") != "public-time-facts-v1"
    ):
        raise ReadyBundleError("v2 stage descriptor is invalid")
    initial_value = value.get("initial_frontier")
    final_value = value.get("final_frontier")
    clock_value = value.get("clock_state")
    projection_sha256 = value.get("record_projection_sha256")
    if initial_value is not None and not isinstance(initial_value, Mapping):
        raise ReadyBundleError("v2 stage descriptor frontier is invalid")
    if not isinstance(final_value, Mapping) or not isinstance(clock_value, dict) or not _is_sha256(projection_sha256):
        raise ReadyBundleError("v2 stage descriptor frontier is invalid")
    try:
        clock_state = ClockState.from_payload(clock_value)
    except ClockStateError as error:
        raise ReadyBundleError("v2 stage clock basis is invalid") from error
    initial = None if initial_value is None else _v2_frontier_from_payload(initial_value)
    return clock_state, initial, _v2_frontier_from_payload(final_value), cast(str, projection_sha256)


def _load_v2_stage(
    audio_store: InternalAudioStore,
    ready_root: Path,
    cohort: _TypedCohort,
    page_limit: int,
) -> _V2Stage:
    try:
        with (
            closing(audio_store.state_db.open_connection(read_only=True)) as connection,
            audio_store.state_db.access(connection),
        ):
            rows = _sql_all(
                connection.execute(
                    "SELECT stage_id,group_identity,owned_path,stage_nonce,public_order,artifact_id,config_sha256,"
                    "descriptor,descriptor_sha256,expected_audio_sha256,expected_manifest_sha256,"
                    "expected_physical_count,expected_packet_count,expected_page_count,expected_audio_bytes,"
                    "pending_overflow_size,owner_token,failure_reason FROM ready_v2_output_stage ORDER BY stage_id"
                )
            )
            if len(rows) != 1:
                raise ReadyBundleError("STAGING cohort does not own exactly one v2 stage intent")
            row = rows[0]
            range_rows = _sql_all(
                connection.execute(
                    "SELECT range_ordinal,source_id,start_ordinal,record_count,sequence_start,next_sequence,raw_sha256 "
                    "FROM ready_v2_stage_range WHERE stage_id=? ORDER BY range_ordinal",
                    (row[0],),
                )
            )
    except (OSError, StateDBError, sqlite3.DatabaseError) as error:
        raise_if_sqlite_storage_fault(error)
        raise ReadyBundleError("v2 stage intent is unreadable") from error
    stored = _parse_stored_v2_stage(row, ready_root, cohort)
    slices = _load_v2_stage_slices(cohort, range_rows, page_limit)
    if not slices:
        raise ReadyBundleError("v2 stage has no source ranges")
    group = _rebuild_v2_stage_group(stored, slices, page_limit)
    manifest = canonical_manifest(
        artifact_id=stored.artifact_id,
        order=stored.order,
        audio_sha256=group.audio.sha256,
        projection=group.projection,
    )
    _require_rebuilt_v2_stage(stored, group, manifest)
    stage = _V2Stage(
        stored.stage_id,
        stored.identity,
        Path(stored.owned_path),
        stored.artifact_id,
        stored.order,
        stored.nonce,
        stored.descriptor,
        stored.descriptor_sha256,
        manifest,
        stored.manifest_sha256,
        group,
        stored.stage_id,
    )
    _require_v2_staging_proof(audio_store.state_db, stage, page_limit)
    return stage


def _rebuild_v2_stage_group(
    stored: _StoredV2Stage, slices: tuple[_TypedSlice, ...], page_limit: int
) -> _V2PreparedGroup:
    basis = _v2_descriptor_basis(stored.descriptor)
    gaps = tuple(
        (left.next_sequence, right.start_sequence)
        for left, right in pairwise(slices)
        if left.next_sequence < right.start_sequence
    )
    facts = _BundleFacts(
        slices[0].start_sequence,
        slices[-1].next_sequence,
        sum(item.record_count for item in slices),
        _typed_raw_digest(tuple(slices), page_limit),
        gaps,
    )
    projected = _freeze_v2_public_projection(slices, page_limit, basis[0], basis[1])
    if projected[1] != basis[2] or projected[2] != basis[3]:
        raise ReadyBundleError("v2 stage frozen projection changed")
    provisional = _V2PreparedGroup(
        slices,
        facts,
        basis[3],
        projected[0],
        V2AudioDigest(stored.packet_count, stored.page_count, stored.audio_bytes, stored.audio_sha256),
        stored.config_sha256,
        basis[0],
        basis[1],
        basis[2],
    )
    actual_audio = hash_v2_audio(_v2_packets_from_frozen(provisional, page_limit))
    return replace(provisional, audio=actual_audio)


def _require_rebuilt_v2_stage(stored: _StoredV2Stage, group: _V2PreparedGroup, manifest: bytes) -> None:
    expected = (
        stored.physical_count,
        stored.packet_count,
        stored.page_count,
        stored.audio_bytes,
        stored.pending_overflow,
        stored.config_sha256,
        stored.descriptor,
        stored.audio_sha256,
        stored.manifest_sha256,
    )
    actual = (
        group.facts.record_count,
        group.audio.packet_count,
        group.audio.page_count,
        group.audio.byte_count,
        group.final_frontier.pending_overflow_size,
        _v2_config_sha256(),
        _v2_descriptor(group),
        group.audio.sha256,
        sha256(manifest).hexdigest(),
    )
    if actual != expected:
        raise ReadyBundleError("v2 stage frozen output changed")


def _parse_stored_v2_stage(row: tuple[object, ...], ready_root: Path, cohort: _TypedCohort) -> _StoredV2Stage:
    if len(row) != _V2_STAGE_ROW_COLUMNS or row[17] == _DURABILITY_IO_FAILURE:
        raise ReadyBundleError("v2 stage is blocked by a recorded durability I/O failure")
    if not all(
        (
            isinstance(row[0], str),
            isinstance(row[1], str),
            isinstance(row[2], str),
            isinstance(row[3], bytes),
            len(cast(bytes, row[3])) == _V2_NONCE_BYTES,
            isinstance(row[4], bytes),
            _is_sha256(row[5]),
            _is_sha256(row[6]),
            isinstance(row[7], bytes),
            _is_sha256(row[8]),
            _is_sha256(row[9]),
            _is_sha256(row[10]),
            type(row[11]) is int,
            type(row[12]) is int,
            type(row[13]) is int,
            type(row[14]) is int,
            row[15] is None or type(row[15]) is int,
            row[16] == row[0],
            row[17] is None,
        )
    ):
        raise ReadyBundleError("v2 stage ownership is invalid")
    stored = _StoredV2Stage(
        cast(str, row[0]),
        cast(str, row[1]),
        cast(str, row[2]),
        cast(bytes, row[3]),
        decode_uint64(cast(bytes, row[4])),
        cast(str, row[5]),
        cast(str, row[6]),
        cast(bytes, row[7]),
        cast(str, row[8]),
        cast(str, row[9]),
        cast(str, row[10]),
        cast(int, row[11]),
        cast(int, row[12]),
        cast(int, row[13]),
        cast(int, row[14]),
        cast(int | None, row[15]),
    )
    expected_identity = f"{cohort.cohort_id}:{cohort.cursor}:{stored.artifact_id}"
    expected_path = ready_root / f".{stored.artifact_id}.{stored.stage_id[:16]}.stage"
    derived_artifact = derive_artifact_id(
        stage_nonce=stored.nonce,
        order=stored.order,
        config_sha256=stored.config_sha256,
        descriptor_sha256=stored.descriptor_sha256,
    )
    if not all(
        (
            stored.identity == expected_identity,
            stored.stage_id == sha256(stored.identity.encode()).hexdigest(),
            stored.owned_path == str(expected_path),
            stored.artifact_id == derived_artifact,
            sha256(stored.descriptor).hexdigest() == stored.descriptor_sha256,
        )
    ):
        raise ReadyBundleError("v2 stage immutable identity changed")
    return stored


def _load_v2_stage_slices(
    cohort: _TypedCohort,
    range_rows: tuple[tuple[object, ...], ...],
    page_limit: int,
) -> tuple[_TypedSlice, ...]:
    by_source = {item.snapshot.source_id: item for item in cohort.admissions}
    slices: list[_TypedSlice] = []
    for expected_ordinal, raw in enumerate(range_rows):
        if len(raw) != _V2_STAGE_RANGE_COLUMNS:
            raise ReadyBundleError("v2 stage source ranges are invalid")
        if not all(
            (
                raw[0] == expected_ordinal,
                isinstance(raw[1], str),
                raw[1] in by_source,
                type(raw[2]) is int,
                type(raw[3]) is int,
                isinstance(raw[4], bytes),
                isinstance(raw[5], bytes),
                _is_sha256(raw[6]),
            )
        ):
            raise ReadyBundleError("v2 stage source ranges are invalid")
        item = _typed_slice(by_source[cast(str, raw[1])], cast(int, raw[2]), cast(int, raw[3]), page_limit)
        if (item.start_sequence, item.next_sequence, item.raw_sha256) != (
            decode_uint64(cast(bytes, raw[4])),
            decode_uint64(cast(bytes, raw[5])),
            raw[6],
        ):
            raise ReadyBundleError("v2 stage source range changed")
        slices.append(item)
    return tuple(slices)


def _require_v2_staging_proof(state_db: StateDB, stage: _V2Stage, page_limit: int) -> None:
    runs = _v2_normalization_runs(stage.group, page_limit)
    packed_runs = pack_normalization_runs(runs, record_count=stage.group.facts.record_count)
    normalized_digest = sha256()
    try:
        with closing(state_db.open_connection(read_only=True)) as connection, state_db.access(connection):
            root = _sql_one(
                connection.execute(
                    "SELECT lifecycle,stage_id,owner_token,record_count,records_sha256,manifest_sha256,page_index,"
                    "normalization_runs,immutable_metadata,proof_content_sha256,format,public_order,audio_sha256,"
                    "packet_count FROM ready_replay_proof WHERE bundle_id=?",
                    (stage.artifact_id,),
                )
            )
            pages = _sql_all(
                connection.execute(
                    "SELECT page_ordinal,first_record_index,record_count,hashes,page_sha256 "
                    "FROM ready_replay_proof_page WHERE bundle_id=? ORDER BY page_ordinal",
                    (stage.artifact_id,),
                )
            )
    except (OSError, StateDBError, sqlite3.DatabaseError) as error:
        raise_if_sqlite_storage_fault(error)
        raise ReadyDurabilityFatalError("v2 staging replay proof is unreadable") from error
    if root is None or len(root) != _V2_REPLAY_PROOF_COLUMNS:
        raise ReadyDurabilityFatalError("v2 staging replay proof is missing")
    page_index = root[6]
    records_sha256 = root[4]
    if not isinstance(page_index, bytes) or not isinstance(records_sha256, str):
        raise ReadyDurabilityFatalError("v2 staging replay proof is invalid")
    try:
        index = unpack_page_index(page_index, record_count=stage.group.facts.record_count)
        expected_hashes = _validated_v2_stage_hashes(pages, index)
        normalized = _v2_normalized_records(stage.group, page_limit, runs)
        for record, expected_hash in zip(normalized, expected_hashes, strict=True):
            normalized_digest.update(record)
            if not hmac.compare_digest(record_sha256(record), expected_hash):
                raise ValueError
    except (IndexError, TypeError, ValueError) as error:
        raise ReadyDurabilityFatalError("v2 staging replay proof pages are corrupt") from error
    content_sha256 = _v2_proof_content_sha256(
        _V2ProofContent(
            stage.artifact_id,
            stage.order,
            stage.group.audio.sha256,
            stage.manifest_sha256,
            normalized_digest.hexdigest(),
            stage.group.facts.record_count,
            stage.group.audio.packet_count,
            stage.group.projection_sha256,
            page_index,
            packed_runs,
            stage.descriptor,
        )
    )
    expected = (
        "staging",
        stage.stage_id,
        stage.owner_token,
        stage.group.facts.record_count,
        normalized_digest.hexdigest(),
        stage.manifest_sha256,
        page_index,
        packed_runs,
        stage.descriptor,
        content_sha256,
        "omi-audio-v2",
        encode_uint64(stage.order),
        stage.group.audio.sha256,
        stage.group.audio.packet_count,
    )
    if tuple(root) != expected:
        raise ReadyDurabilityFatalError("v2 staging replay proof root is corrupt")


def _validated_v2_stage_hashes(
    pages: tuple[tuple[object, ...], ...],
    index: tuple[tuple[int, int, bytes], ...],
) -> tuple[bytes, ...]:
    expected_hashes: list[bytes] = []
    for ordinal, raw in enumerate(pages):
        if len(raw) != _REPLAY_PROOF_PAGE_COLUMNS or raw[0] != ordinal:
            raise ValueError
        page = ProofPage(cast(int, raw[1]), cast(int, raw[2]), cast(bytes, raw[3]), cast(bytes, raw[4]))
        hashes = validate_proof_page(page, expected_first_record_index=index[ordinal][0])
        if (page.record_count, page.page_sha256) != (index[ordinal][1], index[ordinal][2]):
            raise ValueError
        expected_hashes.extend(hashes)
    return tuple(expected_hashes)


def _resume_v2_cohort(
    audio_store: InternalAudioStore,
    cohort: _TypedCohort,
    context: _V2ResumeContext,
) -> tuple[list[ReadyBundleResult], ReadyCustodyCache | None]:
    published: list[ReadyBundleResult] = []
    current: _TypedCohort | None = cohort
    custody_cache = context.custody_cache
    prepared_slices = context.prepared_slices
    while current is not None:
        _require_typed_admissions_current(audio_store, current.admissions)
        if current.lifecycle == "staging":
            stage = _load_v2_stage(audio_store, context.ready_root, current, context.page_limit)
        elif current.lifecycle == "accepted":
            custody_cache = _v2_ready_custody(context.ready_root, audio_store.state_db, custody_cache)
            current = _advance_typed_ready_custody(audio_store, current, custody_cache.custody, context.page_limit)
            if current is None:
                break
            slices = _accepted_typed_slices(
                audio_store, current, context.page_limit, custody_cache.custody, prepared_slices
            )
            prepared_slices = None
            if not slices:
                _complete_empty_typed_cohort(audio_store, current)
                break
            groups = _v2_physical_groups(slices, context.page_limit)
            if not groups:
                raise ReadyBundleError("accepted v2 cohort has no audio packets")
            group = _prepare_v2_group(
                groups[0],
                context.page_limit,
                context.clock_state,
                _v2_frontier(audio_store.state_db),
            )
            stage = _ensure_v2_stage(audio_store, context.ready_root, current, group, context.page_limit)
        else:
            raise ReadyBundleError("v2 publication cohort lifecycle is invalid")
        result = _materialize_v2_stage_with_failure_latch(audio_store, current, stage, context.page_limit)
        _commit_v2_stage(audio_store, current, stage, result)
        published.append(result)
        current = _load_active_typed_cohort(audio_store)
        if current is not None and current.cohort_id != cohort.cohort_id:
            _typed_fail_stop(audio_store, "v2 publication cohort changed during resume")
    return published, custody_cache


def _ensure_v2_stage(
    audio_store: InternalAudioStore,
    ready_root: Path,
    cohort: _TypedCohort,
    group: _V2PreparedGroup,
    page_limit: int,
) -> _V2Stage:
    if cohort.lifecycle == "staging":
        return _load_v2_stage(audio_store, ready_root, cohort, page_limit)
    for attempt in range(2):
        try:
            return _create_v2_stage(audio_store, ready_root, cohort, group, page_limit)
        except (InternalAudioStoreError, sqlite3.DatabaseError, TransientInternalAudioStorageError) as error:
            raise_if_sqlite_corrupt(error)
            durable = _load_active_for_reconciliation(audio_store)
            if durable is not None and durable.cohort_id == cohort.cohort_id and durable.lifecycle == "staging":
                return _load_v2_stage(audio_store, ready_root, durable, page_limit)
            if durable is not None and _same_typed_cohort(durable, cohort) and attempt == 0:
                continue
            if durable is not None and _same_typed_cohort(durable, cohort):
                raise_if_sqlite_storage_fault(error)
                raise ReadyBundleError("v2 stage intent was rolled back") from error
            _typed_fail_stop(audio_store, "ambiguous v2 stage intent has a third state")
    raise AssertionError("unreachable")


def _materialize_v2_stage(stage: _V2Stage, page_limit: int) -> ReadyBundleResult:
    destination = stage.path.parent / stage.artifact_id
    if os.path.lexists(destination):
        _require_v2_destination(destination, stage)
        _discard_v2_stage(stage.path)
    else:
        if os.path.lexists(stage.path):
            try:
                _require_v2_destination(stage.path, stage, require_name=False)
            except ReadyBundleError:
                _discard_v2_stage(stage.path)
        if not os.path.lexists(stage.path):
            _write_v2_stage(stage, page_limit)
        _require_v2_destination(stage.path, stage, require_name=False)
        try:
            stage.path.rename(destination)
        except FileExistsError:
            _require_v2_destination(destination, stage)
            _discard_v2_stage(stage.path)
        _sync_directory(destination.parent)
    _require_v2_destination(destination, stage)
    return ReadyBundleResult(
        stage.artifact_id,
        destination,
        stage.group.facts.next_sequence,
        stage.group.facts.record_count,
        _v2_stage_records_sha256(stage),
    )


def _materialize_v2_stage_with_failure_latch(
    audio_store: InternalAudioStore,
    cohort: _TypedCohort,
    stage: _V2Stage,
    page_limit: int,
) -> ReadyBundleResult:
    try:
        result = _materialize_v2_stage(stage, page_limit)
        _require_v2_staging_proof(audio_store.state_db, stage, page_limit)
        return result
    except OSError as error:
        if error.errno != errno.EIO:
            raise
        _latch_v2_durability_failure(audio_store, cohort, stage)
        raise ReadyBundleError("v2 stage is blocked by a recorded durability I/O failure") from error


def _latch_v2_durability_failure(audio_store: InternalAudioStore, cohort: _TypedCohort, stage: _V2Stage) -> None:
    try:
        with audio_store.publication_transaction() as (state, connection):
            cohort_row = _sql_one(
                connection.execute(
                    "SELECT lifecycle,cursor FROM publication_cohort WHERE cohort_id=?", (cohort.cohort_id,)
                )
            )
            stage_row = _sql_one(
                connection.execute(
                    "SELECT group_identity,owned_path,owner_token,failure_reason FROM ready_v2_output_stage "
                    "WHERE stage_id=?",
                    (stage.stage_id,),
                )
            )
            expected = (stage.group_identity, str(stage.path), stage.owner_token)
            if cohort_row != ("staging", cohort.cursor) or stage_row is None or tuple(stage_row)[:3] != expected:
                raise ReadyDurabilityFatalError("v2 durability failure could not be attached to its stage")
            if stage_row[3] == _DURABILITY_IO_FAILURE:
                return
            if stage_row[3] is not None:
                raise ReadyDurabilityFatalError("v2 durability failure encountered an invalid stage latch")
            changed = connection.execute(
                "UPDATE ready_v2_output_stage SET failure_reason=? WHERE stage_id=? AND group_identity=? "
                "AND owned_path=? AND owner_token=? AND failure_reason IS NULL",
                (_DURABILITY_IO_FAILURE, stage.stage_id, *expected),
            )
            if changed.rowcount != 1:
                raise ReadyDurabilityFatalError("v2 durability failure latch lost its compare-and-swap")
            state.mark_publication_changed()
    except FatalInternalAudioStorageError:
        raise
    except Exception as error:
        if _v2_durability_failure_is_latched(audio_store, cohort, stage):
            return
        raise ReadyDurabilityFatalError("v2 durability failure could not be recorded") from error


def _v2_durability_failure_is_latched(audio_store: InternalAudioStore, cohort: _TypedCohort, stage: _V2Stage) -> bool:
    try:
        with (
            closing(audio_store.state_db.open_connection(read_only=True)) as connection,
            audio_store.state_db.access(connection),
        ):
            row = _sql_one(
                connection.execute(
                    "SELECT p.lifecycle,p.cursor,s.group_identity,s.owned_path,s.owner_token,s.failure_reason "
                    "FROM publication_cohort AS p JOIN ready_v2_output_stage AS s ON s.stage_id=? "
                    "WHERE p.cohort_id=?",
                    (stage.stage_id, cohort.cohort_id),
                )
            )
    except (OSError, StateDBError, sqlite3.DatabaseError, TransientInternalAudioStorageError) as error:
        raise_if_sqlite_corrupt(error)
        return False
    return row == (
        "staging",
        cohort.cursor,
        stage.group_identity,
        str(stage.path),
        stage.owner_token,
        _DURABILITY_IO_FAILURE,
    )


def _write_v2_stage(stage: _V2Stage, page_limit: int) -> None:
    stage.path.mkdir(mode=_READY_DIRECTORY_CREATE_MODE)
    _require_operator_ready_directory(stage.path)
    audio_path = stage.path / _V2_AUDIO_NAME
    try:
        write_v2_audio(audio_path, _v2_packets_from_frozen(stage.group, page_limit), stage.group.audio)
    except ValueError as error:
        raise ReadyBundleError("v2 audio changed while staging") from error
    audio_path.chmod(_READY_FILE_MODE)
    _sync_regular_file(audio_path)
    _write_file(stage.path / _MANIFEST_NAME, stage.manifest)
    _sync_directory(stage.path)


def _require_v2_destination(path: Path, stage: _V2Stage, *, require_name: bool = True) -> None:
    if (
        path.is_symlink()
        or not path.is_dir()
        or (require_name and path.name != stage.artifact_id)
        or {item.name for item in path.iterdir()} != {_V2_AUDIO_NAME, _MANIFEST_NAME}
    ):
        raise ReadyBundleError("v2 ready artifact inventory is invalid")
    audio_path = path / _V2_AUDIO_NAME
    manifest_path = path / _MANIFEST_NAME
    if any(item.is_symlink() or not item.is_file() for item in (audio_path, manifest_path)):
        raise ReadyBundleError("v2 ready artifact inventory is unsafe")
    if _digest(audio_path) != stage.group.audio.sha256 or _digest(manifest_path) != stage.manifest_sha256:
        raise ReadyBundleError("v2 ready artifact changed")


def _discard_v2_stage(path: Path) -> None:
    if not os.path.lexists(path):
        return
    if path.is_symlink() or not path.is_dir():
        raise ReadyBundleError("v2 ready stage is unsafe")
    entries = {item.name: item for item in path.iterdir()}
    if not set(entries) <= {_V2_AUDIO_NAME, _MANIFEST_NAME}:
        raise ReadyBundleError("v2 ready stage contains an unsafe entry")
    for item in entries.values():
        if item.is_symlink() or not item.is_file():
            raise ReadyBundleError("v2 ready stage contains an unsafe entry")
        item.unlink()
    path.rmdir()
    _sync_directory(path.parent)


def _v2_stage_records_sha256(stage: _V2Stage) -> str:
    digest = sha256()
    runs = _v2_normalization_runs(stage.group, _TYPED_PAGE_MAX)
    for record in _v2_normalized_records(stage.group, _TYPED_PAGE_MAX, runs):
        digest.update(record)
    return digest.hexdigest()


def _v2_ledger_entry(stage: _V2Stage, records_sha256: str) -> dict[str, object]:
    entry = _ledger_entry(
        records_sha256,
        "ready",
        stage.group.facts,
        tuple((item.start_sequence, item.next_sequence, item.raw_sha256) for item in stage.group.slices),
    )
    entry.update(
        format="omi-audio-v2",
        order=stage.order,
        audio_sha256=stage.group.audio.sha256,
        audio_bytes=stage.group.audio.byte_count,
        manifest_sha256=stage.manifest_sha256,
        manifest_bytes=len(stage.manifest),
        packet_count=stage.group.audio.packet_count,
    )
    return entry


def _commit_v2_stage(
    audio_store: InternalAudioStore, cohort: _TypedCohort, stage: _V2Stage, result: ReadyBundleResult
) -> None:
    _require_v2_staging_proof(audio_store.state_db, stage, _TYPED_PAGE_MAX)
    progress = _typed_member_progress(cohort, stage.group.facts)
    all_complete = all(
        item.new_cursor == admission.admitted_count for item, admission in zip(progress, cohort.admissions, strict=True)
    )
    coverage = _typed_coverage_proposals(audio_store, cohort, progress)
    entry = _v2_ledger_entry(stage, result.records_sha256)
    proposal = _V2Commit(cohort, stage, entry, progress, coverage, all_complete)
    for attempt in range(2):
        try:
            _commit_v2_stage_once(audio_store, proposal)
            return
        except (InternalAudioStoreError, sqlite3.DatabaseError, TransientInternalAudioStorageError) as error:
            raise_if_sqlite_corrupt(error)
            state = _inspect_v2_commit(audio_store, proposal, result.path)
            if state == "proposed":
                return
            if state == "old" and attempt == 0:
                continue
            if state == "old":
                raise_if_sqlite_storage_fault(error)
                raise ReadyBundleError("v2 final commit was rolled back") from error
            _typed_fail_stop(audio_store, "ambiguous v2 final commit has a third state")
    raise AssertionError("unreachable")


def _inspect_v2_commit(audio_store: InternalAudioStore, proposal: _V2Commit, destination: Path) -> str:
    cohort, stage = proposal.cohort, proposal.stage
    try:
        _require_v2_destination(destination, stage)
        entry = audio_store.state_db.get(StateKind.READY_PUBLICATION, stage.artifact_id)
        frontier = audio_store.state_db.get(StateKind.PUBLICATION_V2_FRONTIER, "current")
        with (
            closing(audio_store.state_db.open_connection(read_only=True)) as connection,
            audio_store.state_db.access(connection),
        ):
            cohort_row = _sql_one(
                connection.execute(
                    "SELECT lifecycle,cursor FROM publication_cohort WHERE cohort_id=?", (cohort.cohort_id,)
                )
            )
            stage_row = _sql_one(
                connection.execute(
                    "SELECT group_identity,owned_path,owner_token,failure_reason FROM ready_v2_output_stage "
                    "WHERE stage_id=?",
                    (stage.stage_id,),
                )
            )
            proof = _sql_one(
                connection.execute(
                    "SELECT lifecycle,stage_id,owner_token FROM ready_replay_proof WHERE bundle_id=?",
                    (stage.artifact_id,),
                )
            )
            member_rows = _sql_all(
                connection.execute(
                    "SELECT member_ordinal,cursor FROM publication_cohort_member WHERE cohort_id=? "
                    "ORDER BY member_ordinal",
                    (cohort.cohort_id,),
                )
            )
    except (
        OSError,
        ReadyBundleError,
        StateDBError,
        sqlite3.DatabaseError,
        TransientInternalAudioStorageError,
    ) as error:
        raise_if_sqlite_corrupt(error)
        return "third"
    source_states = tuple(_typed_source_coverage_state(audio_store, item.source_id) for item in proposal.coverage)
    proposed_sources = all(state == item.proposed for state, item in zip(source_states, proposal.coverage, strict=True))
    old_sources = all(state == item.old for state, item in zip(source_states, proposal.coverage, strict=True))
    proposed_cohort = (
        cohort_row is None
        if proposal.all_complete
        else cohort_row == ("accepted", cohort.cursor + 1)
        and member_rows == tuple((item.member_ordinal, item.new_cursor) for item in proposal.progress)
    )
    if all(
        (
            entry == proposal.entry,
            frontier == _v2_frontier_payload(stage.group.final_frontier),
            stage_row is None,
            proof == ("active", stage.stage_id, stage.owner_token),
            proposed_cohort,
            proposed_sources,
        )
    ):
        return "proposed"
    old_members = tuple((item.member_ordinal, item.old_cursor) for item in proposal.progress)
    if all(
        (
            entry is None,
            frontier
            == (None if stage.group.initial_frontier is None else _v2_frontier_payload(stage.group.initial_frontier)),
            cohort_row == ("staging", cohort.cursor),
            stage_row == (stage.group_identity, str(stage.path), stage.owner_token, None),
            proof == ("staging", stage.stage_id, stage.owner_token),
            member_rows == old_members,
            old_sources,
        )
    ):
        return "old"
    return "third"


def _commit_v2_stage_once(audio_store: InternalAudioStore, proposal: _V2Commit) -> None:
    cohort, stage = proposal.cohort, proposal.stage
    with audio_store.publication_transaction() as (state, connection):
        _require_v2_commit_ownership(state, connection, proposal)
        activated = connection.execute(
            "UPDATE ready_replay_proof SET lifecycle='active' WHERE bundle_id=? AND lifecycle='staging' "
            "AND stage_id=? AND owner_token=?",
            (stage.artifact_id, stage.stage_id, stage.owner_token),
        )
        if activated.rowcount != 1:
            raise ReadyDurabilityFatalError("v2 private replay proof activation changed")
        state.put(StateKind.READY_PUBLICATION, stage.artifact_id, proposal.entry)
        frontier_payload = state.get(StateKind.READY_PUBLICATION, "frontier")
        if frontier_payload is None:
            old_frontier = 0
        elif set(frontier_payload) == {"value"} and type(frontier_payload["value"]) is int:
            old_frontier = cast(int, frontier_payload["value"])
        else:
            raise ReadyBundleError("ready publication frontier is invalid")
        state.put(
            StateKind.READY_PUBLICATION,
            "frontier",
            {"value": max(old_frontier, stage.group.facts.next_sequence)},
        )
        state.put(StateKind.PUBLICATION_V2_FRONTIER, "current", _v2_frontier_payload(stage.group.final_frontier))
        for item in proposal.progress:
            changed = connection.execute(
                "UPDATE publication_cohort_member SET cursor=? WHERE cohort_id=? AND member_ordinal=? AND cursor=?",
                (item.new_cursor, cohort.cohort_id, item.member_ordinal, item.old_cursor),
            )
            if changed.rowcount != 1:
                raise ReadyBundleError("v2 cohort member changed before final commit")
        _apply_typed_coverage(connection, proposal.coverage)
        connection.execute("DELETE FROM ready_v2_output_stage WHERE stage_id=?", (stage.stage_id,))
        _advance_v2_commit_cohort(connection, proposal)
        state.mark_publication_changed()


def _require_v2_commit_ownership(
    state: StateTransaction,
    connection: sqlite3.Connection,
    proposal: _V2Commit,
) -> None:
    cohort, stage = proposal.cohort, proposal.stage
    row = _sql_one(
        connection.execute("SELECT lifecycle,cursor FROM publication_cohort WHERE cohort_id=?", (cohort.cohort_id,))
    )
    if row != ("staging", cohort.cursor):
        raise ReadyBundleError("v2 cohort changed before final commit")
    stage_row = _sql_one(
        connection.execute(
            "SELECT group_identity,owned_path,owner_token,failure_reason FROM ready_v2_output_stage WHERE stage_id=?",
            (stage.stage_id,),
        )
    )
    if stage_row != (stage.group_identity, str(stage.path), stage.owner_token, None):
        raise ReadyBundleError("v2 stage ownership changed before final commit")
    proof = _sql_one(
        connection.execute(
            "SELECT lifecycle,stage_id,owner_token FROM ready_replay_proof WHERE bundle_id=?",
            (stage.artifact_id,),
        )
    )
    if proof != ("staging", stage.stage_id, stage.owner_token):
        raise ReadyDurabilityFatalError("v2 stage has no complete private replay proof")
    if state.get(StateKind.READY_PUBLICATION, stage.artifact_id) is not None:
        raise ReadyBundleError("v2 artifact identity already exists in the publication ledger")


def _advance_v2_commit_cohort(connection: sqlite3.Connection, proposal: _V2Commit) -> None:
    cohort = proposal.cohort
    if proposal.all_complete:
        changed = connection.execute(
            "DELETE FROM publication_cohort WHERE cohort_id=? AND lifecycle='staging' AND cursor=?",
            (cohort.cohort_id, cohort.cursor),
        )
        message = "v2 cohort completion changed before final commit"
    else:
        changed = connection.execute(
            "UPDATE publication_cohort SET lifecycle='accepted',cursor=? WHERE cohort_id=? "
            "AND lifecycle='staging' AND cursor=?",
            (cohort.cursor + 1, cohort.cohort_id, cohort.cursor),
        )
        message = "v2 cohort cursor changed before final commit"
    if changed.rowcount != 1:
        raise ReadyBundleError(message)


def _read_v2_ready_source(path: Path, entry: Mapping[str, object]) -> _ReadySource:
    if (
        path.is_symlink()
        or not path.is_dir()
        or {item.name for item in path.iterdir()}
        != {
            _V2_AUDIO_NAME,
            _MANIFEST_NAME,
        }
    ):
        raise ReadyBundleError("v2 ready artifact inventory is invalid")
    audio = path / _V2_AUDIO_NAME
    manifest = path / _MANIFEST_NAME
    if any(item.is_symlink() or not item.is_file() for item in (audio, manifest)):
        raise ReadyBundleError("v2 ready artifact is unsafe")
    if _digest(audio) != entry["audio_sha256"] or _digest(manifest) != entry["manifest_sha256"]:
        raise ReadyBundleError("v2 ready artifact digest conflicts with its ledger")
    manifest_value = _read_json(manifest)
    if not isinstance(manifest_value, dict) or manifest_value.get("format") != "omi-audio-v2":
        raise ReadyBundleError("v2 ready manifest is invalid")
    if (
        manifest_value.get("id") != path.name
        or manifest_value.get("order") != entry["order"]
        or manifest_value.get("audio_sha256") != entry["audio_sha256"]
        or manifest_value.get("packet_count") != entry["packet_count"]
    ):
        raise ReadyBundleError("v2 ready manifest conflicts with its ledger")
    facts = _entry_facts(entry)
    return _ReadySource(
        ReadyBundleResult(
            path.name,
            path,
            facts.next_sequence,
            facts.record_count,
            cast(str, entry["records_sha256"]),
        ),
        facts,
        (),
    )


def _v2_active_proof(row: tuple[object, ...], source: _ReadySource, entry: Mapping[str, object]) -> _ReplayProof:
    proof_row = _parse_v2_active_proof_row(row, source.result.bundle_id)
    projection_sha256 = _v2_descriptor_basis(proof_row.descriptor)[3]
    expected_content = _v2_proof_content_sha256(
        _V2ProofContent(
            source.result.bundle_id,
            proof_row.order,
            proof_row.audio_sha256,
            proof_row.manifest_sha256,
            proof_row.records_sha256,
            proof_row.record_count,
            proof_row.packet_count,
            projection_sha256,
            proof_row.page_index,
            proof_row.packed_runs,
            proof_row.descriptor,
        )
    )
    if (
        proof_row.record_count,
        proof_row.records_sha256,
        proof_row.manifest_sha256,
        proof_row.order,
        proof_row.audio_sha256,
        proof_row.packet_count,
        proof_row.content_sha256,
    ) != (
        source.facts.record_count,
        source.result.records_sha256,
        entry["manifest_sha256"],
        entry["order"],
        entry["audio_sha256"],
        entry["packet_count"],
        expected_content,
    ):
        raise ReadyDurabilityFatalError("active v2 replay proof conflicts with its ledger")
    index = unpack_page_index(proof_row.page_index, record_count=proof_row.record_count)
    runs = unpack_normalization_runs(proof_row.packed_runs, record_count=proof_row.record_count)
    return _ReplayProof(
        source.result.bundle_id,
        "active",
        proof_row.stage_id,
        proof_row.owner,
        proof_row.record_count,
        proof_row.records_sha256,
        proof_row.manifest_sha256,
        proof_row.page_index,
        proof_row.packed_runs,
        proof_row.descriptor,
        proof_row.content_sha256,
        index,
        runs,
    )


def _parse_v2_active_proof_row(row: tuple[object, ...], artifact_id: str) -> _V2ActiveProofRow:
    if len(row) != _V2_REPLAY_PROOF_COLUMNS or row[0] != artifact_id or row[1] != "active":
        raise ReadyDurabilityFatalError("active v2 replay proof is invalid")
    if not all(
        (
            isinstance(row[2], str),
            isinstance(row[3], str),
            type(row[4]) is int,
            isinstance(row[5], str),
            isinstance(row[6], str),
            isinstance(row[7], bytes),
            isinstance(row[8], bytes),
            isinstance(row[9], bytes),
            isinstance(row[10], str),
            isinstance(row[11], bytes) and len(row[11]) == _UINT64_BYTES,
            isinstance(row[12], str),
            type(row[13]) is int,
        )
    ):
        raise ReadyDurabilityFatalError("active v2 replay proof has invalid columns")
    if not 1 <= cast(int, row[4]) <= _UINT32_MAX or not 1 <= cast(int, row[13]) <= _UINT64_MAX:
        raise ReadyDurabilityFatalError("active v2 replay proof has invalid counts")
    return _V2ActiveProofRow(
        cast(str, row[2]),
        cast(str, row[3]),
        cast(int, row[4]),
        cast(str, row[5]),
        cast(str, row[6]),
        cast(bytes, row[7]),
        cast(bytes, row[8]),
        cast(bytes, row[9]),
        cast(str, row[10]),
        decode_uint64(cast(bytes, row[11])),
        cast(str, row[12]),
        cast(int, row[13]),
    )


def _v2_ready_custody(
    ready_root: Path, state_db: StateDB, previous: ReadyCustodyCache | None = None
) -> ReadyCustodyCache:
    ledger = _read_ledger(state_db)
    entries = {
        bundle_id: cast(dict[str, object], entry)
        for bundle_id, entry in cast(dict[str, object], ledger["bundles"]).items()
        if isinstance(entry, dict) and (entry.get("format") == "omi-audio-v2" or entry.get("state") == "retired")
    }
    revision = source_revision(ready_root)
    proof_revision = _replay_proof_rows(state_db)
    if (
        previous is not None
        and previous.revision == revision
        and previous.proof_revision == proof_revision
        and previous.custody.entries == entries
    ):
        return previous
    active_ids = {bundle_id for bundle_id, entry in entries.items() if entry["state"] == "ready"}
    paths = tuple(sorted(_visible_children(ready_root), key=lambda item: item.name))
    if {path.name for path in paths} != active_ids:
        raise ReadyBundleError("v2 ready files do not match the active publication ledger")
    active = tuple(sorted((_read_v2_ready_source(path, entries[path.name]) for path in paths), key=_ready_start))
    source_by_id = {source.result.bundle_id: source for source in active}
    rows = {cast(str, row[0]): row for row in _v2_replay_proof_rows(state_db)}
    if set(rows) != active_ids:
        raise ReadyDurabilityFatalError("active v2 replay proofs do not match the ledger")
    try:
        proofs = {
            bundle_id: _v2_active_proof(rows[bundle_id], source_by_id[bundle_id], entries[bundle_id])
            for bundle_id in active_ids
        }
    except (ReadyBundleError, ValueError, TypeError, IndexError) as error:
        raise ReadyDurabilityFatalError("active v2 replay proof is corrupt") from error
    if revision != source_revision(ready_root):
        raise ReadyBundleError("v2 ready custody changed during authentication")
    return ReadyCustodyCache(revision, proof_revision, _TypedReadyCustody(state_db, active, entries, proofs))


def _v2_replay_proof_rows(state_db: StateDB) -> tuple[tuple[object, ...], ...]:
    try:
        with closing(state_db.open_connection(read_only=True)) as connection, state_db.access(connection):
            return _sql_all(
                connection.execute(
                    "SELECT bundle_id,lifecycle,stage_id,owner_token,record_count,records_sha256,manifest_sha256,"
                    "page_index,normalization_runs,immutable_metadata,proof_content_sha256,public_order,"
                    "audio_sha256,packet_count FROM ready_replay_proof WHERE format='omi-audio-v2' AND lifecycle='active' "
                    "ORDER BY bundle_id"
                )
            )
    except (OSError, StateDBError, sqlite3.DatabaseError) as error:
        raise_if_sqlite_storage_fault(error)
        raise ReadyDurabilityFatalError("active v2 replay proofs are unreadable") from error


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
    output_digest, recorded_min, recorded_max = _typed_output_digest(slices, ranges, page_limit)
    return _TypedGroup(slices, facts, ranges, output_digest, recorded_min, recorded_max)


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
) -> tuple[str, str | None, str | None]:
    digest = sha256()
    lower: int | None = None
    upper: int | None = None
    complete = True
    range_index = 0
    next_sequence = cast(int, ranges[0]["next_sequence"])
    for item in slices:
        for record in _typed_records(item.admission, item.start_ordinal, item.record_count, page_limit):
            sequence = item.admission.snapshot.sequence_start + record.ordinal
            while sequence >= next_sequence:
                range_index += 1
                next_sequence = cast(int, ranges[range_index]["next_sequence"])
            time_range = ranges[range_index]
            converted = _convert_record(record.raw_bytes, time_range)
            digest.update(converted)
            utc = time_range["utc"]
            if utc is None:
                complete = False
                continue
            assert isinstance(utc, Mapping)
            timestamp = int.from_bytes(converted[:TIMESTAMP_SIZE], "big")
            bounds = _utc_record_bounds(timestamp, utc)
            if bounds is None:
                complete = False
                continue
            lower = bounds[0] if lower is None else min(lower, bounds[0])
            upper = bounds[1] if upper is None else max(upper, bounds[1])
    if not complete or lower is None or upper is None:
        return digest.hexdigest(), None, None
    epoch = datetime(1970, 1, 1, tzinfo=UTC)
    try:
        lower_date = (epoch + timedelta(seconds=lower)).date().isoformat()
        upper_date = (epoch + timedelta(seconds=upper)).date().isoformat()
    except OverflowError:
        lower_date = upper_date = None
    return digest.hexdigest(), lower_date, upper_date


def _utc_record_bounds(timestamp: int, utc: Mapping[str, object]) -> tuple[int, int] | None:
    uncertainty = _finite_number(utc["uncertainty_seconds"])
    try:
        return math.floor(timestamp - uncertainty), math.ceil(timestamp + uncertainty)
    except OverflowError, ValueError:
        return None


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
        cast(int | None, certification[0]),
        cast(str | None, certification[1]),
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
    with _stage_replay_proof(proposed, _staged_result(proposed), page_limit) as (proof, scratch):
        for attempt in range(2):
            try:
                _insert_typed_stage(audio_store, cohort, proposed, proof, scratch)
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


def _insert_typed_stage(
    audio_store: InternalAudioStore,
    cohort: _TypedCohort,
    stage: _TypedStage,
    proof: _ReplayProof,
    proof_scratch: sqlite3.Connection,
) -> None:
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
        _insert_staging_replay_proof(connection, proof, proof_scratch)
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
    output_digest, recorded_min, recorded_max = _typed_output_digest(slices, ranges, page_limit)
    return _TypedGroup(slices, facts, ranges, output_digest, recorded_min, recorded_max)


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
    _commit_typed_stage(audio_store, cohort, stage, result, page_limit)
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
        _commit_typed_stage(audio_store, cohort, stage, result, page_limit)
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
        result = _materialize_typed_stage(stage, page_limit)
        _require_staging_replay_proof(audio_store, cohort, stage, result, page_limit)
        return result
    except OSError as error:
        if error.errno != errno.EIO:
            raise
        _latch_typed_durability_failure(audio_store, cohort, stage)
        raise ReadyBundleError("typed stage is blocked by a recorded durability I/O failure") from error


def _typed_normalized_records(group: _TypedGroup, page_limit: int) -> Iterator[bytes]:
    range_index = 0
    next_sequence = cast(int, group.ranges[0]["next_sequence"])
    runs = _replay_normalization_runs(cast(tuple[Mapping[str, object], ...], group.ranges), group.facts.record_count)
    record_index = 0
    for item in group.slices:
        for record in _typed_records(item.admission, item.start_ordinal, item.record_count, page_limit):
            sequence = item.admission.snapshot.sequence_start + record.ordinal
            while sequence >= next_sequence:
                range_index += 1
                next_sequence = cast(int, group.ranges[range_index]["next_sequence"])
            while record_index >= runs[range_index].next_record_index:
                range_index += 1
            yield normalize_record(record.raw_bytes, runs[range_index])
            record_index += 1


@contextmanager
def _stage_replay_proof(
    stage: _TypedStage, result: ReadyBundleResult, page_limit: int
) -> Iterator[tuple[_ReplayProof, sqlite3.Connection]]:
    with _proof_page_scratch(
        _typed_normalized_records(stage.group, page_limit),
        record_count=stage.group.facts.record_count,
    ) as (page_index, scratch):
        runs = _replay_normalization_runs(
            cast(tuple[Mapping[str, object], ...], stage.group.ranges), stage.group.facts.record_count
        )
        packed_runs = pack_normalization_runs(runs, record_count=stage.group.facts.record_count)
        manifest = _canonical(_ready_manifest(stage.group.facts, stage.group.output_sha256, stage.group.ranges))
        manifest_sha256 = sha256(manifest).hexdigest()
        lineage = _typed_stage_lineage(stage, page_limit)
        source = _ReadySource(result, stage.group.facts, cast(tuple[Mapping[str, object], ...], stage.group.ranges))
        metadata = _replay_immutable_metadata(source, manifest_sha256, lineage)
        content_sha256 = _replay_proof_content_sha256(
            _ProofContent(
                result.bundle_id,
                result.records_sha256,
                manifest_sha256,
                stage.group.facts.record_count,
                page_index,
                packed_runs,
                metadata,
            )
        )
        yield (
            _ReplayProof(
                result.bundle_id,
                "staging",
                stage.stage_id,
                stage.owner_token,
                stage.group.facts.record_count,
                result.records_sha256,
                manifest_sha256,
                page_index,
                packed_runs,
                metadata,
                content_sha256,
                unpack_page_index(page_index, record_count=stage.group.facts.record_count),
                runs,
            ),
            scratch,
        )


def _typed_stage_lineage(stage: _TypedStage, page_limit: int) -> tuple[SourceLineage, ...]:
    combined: list[tuple[_TypedAdmission, int, int, int, int]] = []
    for item in stage.group.slices:
        admission = item.admission
        if combined and (
            combined[-1][0].snapshot.source_id == admission.snapshot.source_id
            and combined[-1][1] + combined[-1][2] == item.start_ordinal
            and combined[-1][4] == item.start_sequence
        ):
            previous = combined[-1]
            combined[-1] = (admission, previous[1], previous[2] + item.record_count, previous[3], item.next_sequence)
        else:
            combined.append((admission, item.start_ordinal, item.record_count, item.start_sequence, item.next_sequence))
    return tuple(
        (start_sequence, next_sequence, _typed_slice(admission, start, count, page_limit).raw_sha256)
        for admission, start, count, start_sequence, next_sequence in combined
    )


def _staged_result(stage: _TypedStage) -> ReadyBundleResult:
    bundle_id = _bundle_id(stage.group.facts)
    return ReadyBundleResult(
        bundle_id,
        stage.path.parent / bundle_id,
        stage.group.facts.next_sequence,
        stage.group.facts.record_count,
        stage.group.output_sha256,
    )


def _insert_staging_replay_proof(
    connection: sqlite3.Connection,
    proof: _ReplayProof,
    scratch: sqlite3.Connection,
) -> None:
    connection.execute(
        "INSERT INTO ready_replay_proof(bundle_id,lifecycle,stage_id,owner_token,record_count,records_sha256,"
        "manifest_sha256,page_index,normalization_runs,immutable_metadata,proof_content_sha256) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (proof.bundle_id, *_replay_proof_values(proof)),
    )
    _insert_replay_proof_pages(connection, proof.bundle_id, scratch)


def _require_staging_replay_proof(
    audio_store: InternalAudioStore,
    cohort: _TypedCohort,
    stage: _TypedStage,
    result: ReadyBundleResult,
    page_limit: int,
) -> None:
    with _stage_replay_proof(stage, result, page_limit) as (proof, scratch):
        try:
            with (
                closing(audio_store.state_db.open_connection(read_only=True)) as connection,
                audio_store.state_db.access(connection),
            ):
                stage_row = _sql_one(
                    connection.execute(
                        "SELECT group_identity,owner_token FROM ready_output_stage WHERE stage_id=?",
                        (stage.stage_id,),
                    )
                )
                cohort_row = _sql_one(
                    connection.execute(
                        "SELECT lifecycle,cursor FROM publication_cohort WHERE cohort_id=?", (cohort.cohort_id,)
                    )
                )
                stored = _sql_one(
                    connection.execute(
                        "SELECT lifecycle,stage_id,owner_token,record_count,records_sha256,manifest_sha256,page_index,"
                        "normalization_runs,immutable_metadata,proof_content_sha256 "
                        "FROM ready_replay_proof WHERE bundle_id=?",
                        (proof.bundle_id,),
                    )
                )
                pages_match = _replay_proof_pages_match(connection, proof.bundle_id, scratch)
        except (OSError, StateDBError, sqlite3.DatabaseError) as error:
            raise_if_sqlite_storage_fault(error)
            raise ReadyBundleError("staging private replay proof is unreadable") from error
        if stage_row is None or tuple(stage_row) != (stage.group_identity, stage.owner_token):
            raise ReadyBundleError("typed stage changed before replay proof validation")
        if cohort_row is None or tuple(cohort_row) != ("staging", cohort.cursor):
            raise ReadyBundleError("typed cohort changed before replay proof validation")
        if stored is None or tuple(stored) != _replay_proof_values(proof) or not pages_match:
            raise ReadyDurabilityFatalError("staging private replay proof conflicts with its owner")


def _replay_proof_values(proof: _ReplayProof) -> tuple[object, ...]:
    return (
        proof.lifecycle,
        proof.stage_id,
        proof.owner_token,
        proof.record_count,
        proof.records_sha256,
        proof.manifest_sha256,
        proof.page_index,
        proof.normalization_runs,
        proof.immutable_metadata,
        proof.proof_content_sha256,
    )


def _stored_replay_page_values(connection: sqlite3.Connection, bundle_id: str) -> Iterator[tuple[object, ...]]:
    yield from _stream_sql_rows(
        connection.execute(
            "SELECT page_ordinal,first_record_index,record_count,hashes,page_sha256 "
            "FROM ready_replay_proof_page WHERE bundle_id=? ORDER BY page_ordinal",
            (bundle_id,),
        )
    )


def _replay_proof_pages_match(
    connection: sqlite3.Connection,
    bundle_id: str,
    scratch: sqlite3.Connection,
) -> bool:
    missing = object()
    matches = True
    for stored, expected in zip_longest(
        _stored_replay_page_values(connection, bundle_id),
        _replay_proof_page_values(scratch),
        fillvalue=missing,
    ):
        if stored is missing or expected is missing or stored != expected:
            matches = False
    return matches


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
    proof_pages = ceil(stage.group.facts.record_count / 128)
    # The final transaction is the larger case: range deletion, member cursor
    # update, source coverage update, possible member cascade, three state rows,
    # the stage table plus its unique index, and the cohort row.
    btree_mutations = range_rows + proof_pages + 3 * member_rows + 8
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
    entry.update(
        formed_on_utc="2000-01-01",
        recorded_on_utc_min=stage.group.recorded_on_utc_min,
        recorded_on_utc_max=stage.group.recorded_on_utc_max,
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
    proof_pages = ceil(stage.group.facts.record_count / 128)
    proof_bytes = stage.group.facts.record_count * 64 + proof_pages * 42
    row_count = len(stage.rows) + proof_pages + member_rows + 5
    return (
        text_bytes
        + row_text_bytes
        + len(_canonical(stage.group.ranges))
        + len(_canonical(entry))
        + proof_bytes
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
    _require_operator_ready_directory(path)
    if not _typed_files_match(path, group, require_bundle_name=require_bundle_name):
        raise ReadyBundleError("typed ready output conflicts with its frozen stage")
    directory_gid = path.stat().st_gid
    if any((path / name).stat().st_gid != directory_gid for name in (_RAW_NAME, _MANIFEST_NAME)):
        raise ReadyBundleError("typed ready files do not inherit the operator group")
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
    path.mkdir(mode=_READY_DIRECTORY_CREATE_MODE)
    _require_operator_ready_directory(path)
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
    page_limit: int,
) -> None:
    progress = _typed_member_progress(cohort, stage.group.facts)
    all_complete = all(
        item.new_cursor == admission.admitted_count for item, admission in zip(progress, cohort.admissions, strict=True)
    )
    entry = _ledger_entry(
        stage.group.output_sha256,
        "ready",
        stage.group.facts,
        _typed_stage_lineage(stage, page_limit),
    )
    ledger = _read_ledger(audio_store.state_db)
    existing = cast(dict[str, object], ledger["bundles"]).get(result.bundle_id)
    if existing is not None and (not isinstance(existing, dict) or not _same_ledger_identity(existing, entry)):
        raise ReadyBundleError("typed ready ledger conflicts with its frozen stage")
    if isinstance(existing, dict):
        entry = dict(existing)
    else:
        entry.update(
            formed_on_utc=datetime.now(UTC).date().isoformat(),
            recorded_on_utc_min=stage.group.recorded_on_utc_min,
            recorded_on_utc_max=stage.group.recorded_on_utc_max,
        )
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
        proof = _sql_one(
            connection.execute(
                "SELECT lifecycle,stage_id,owner_token FROM ready_replay_proof WHERE bundle_id=?",
                (proposal.result.bundle_id,),
            )
        )
        if proof is None or tuple(proof) != ("staging", stage.stage_id, stage.owner_token):
            raise ReadyDurabilityFatalError("typed stage has no complete private replay proof")
        activated = connection.execute(
            "UPDATE ready_replay_proof SET lifecycle='active' "
            "WHERE bundle_id=? AND lifecycle='staging' AND stage_id=? AND owner_token=?",
            (proposal.result.bundle_id, stage.stage_id, stage.owner_token),
        )
        if activated.rowcount != 1:
            raise ReadyDurabilityFatalError("typed private replay proof activation changed")
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
            proof_row = _sql_one(
                connection.execute(
                    "SELECT lifecycle,stage_id,owner_token FROM ready_replay_proof WHERE bundle_id=?",
                    (proposal.result.bundle_id,),
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
        (
            ledger,
            proposed_cohort,
            None if stage_row is None else tuple(stage_row),
            None if proof_row is None else tuple(proof_row),
        )
        == (
            _typed_proposed_ledger(proposal),
            True,
            None,
            ("active", stage.stage_id, stage.owner_token),
        )
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
        None if proof_row is None else tuple(proof_row),
    )
    expected_control_state = (
        proposal.ledger,
        ("staging", cohort.cursor),
        (stage.group_identity, None),
        ("staging", stage.stage_id, stage.owner_token),
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
            mark_retirement_compaction_pending(state)
            deleted_records = connection.execute(
                "DELETE FROM capture_record WHERE source_id=? AND ordinal>=? AND ordinal<?",
                (step.source_id, step.old_cursor, proposed),
            )
            if deleted_records.rowcount != len(ordinals):
                raise ReadyBundleError("typed retirement rows changed before removal")
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
            connection.execute(
                "DELETE FROM state WHERE kind=? AND id=?",
                (StateKind.PACKET_SUMMARY.value, step.source_id),
            )
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
            marker = _sql_one(
                connection.execute(
                    "SELECT payload FROM state WHERE kind=? AND id=?", (StateKind.COMPACTION.value, "current")
                )
            )
            marker_payload = json.loads(cast(str, marker[0])) if marker is not None else None
    except (OSError, ValueError, StateDBError, sqlite3.DatabaseError, TransientInternalAudioStorageError) as error:
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
            return "proposed" if isinstance(marker_payload, dict) and marker_payload.get("pending") is True else "third"
        if row[4] == step.old_cursor:
            return "old"
    return "third"


def _typed_fail_stop(audio_store: InternalAudioStore, message: str) -> NoReturn:
    audio_store._fail_stop(message)
