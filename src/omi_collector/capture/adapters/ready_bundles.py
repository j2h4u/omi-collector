"""Finalize raw draft bundles into immutable, flat ready bundles."""

from __future__ import annotations

import json
import math
import os
import shutil
from collections.abc import Mapping
from dataclasses import dataclass
from hashlib import sha256
from itertools import pairwise
from pathlib import Path
from typing import cast
from uuid import uuid4

from ..domain.ring_protocol import RECORD_SIZE, TIMESTAMP_SIZE
from .bundle_contract import BundleManifest, SealedReceipt
from .clock_segments import ClockSegmentMap


class ReadyBundleError(RuntimeError):
    """A draft cannot be safely finalized into a ready bundle."""


@dataclass(frozen=True, slots=True)
class ReadyBundleResult:
    """One ready bundle made durable during this call."""

    bundle_id: str
    path: Path
    next_sequence: int
    record_count: int
    records_sha256: str


@dataclass(frozen=True, slots=True)
class _Draft:
    """An authenticated raw draft, optionally cropped after a replay prefix."""

    path: Path
    manifest: BundleManifest
    byte_offset: int = 0


@dataclass(frozen=True, slots=True)
class _ReadySource:
    """A validated ready bundle used only to compare replayed payloads."""

    result: ReadyBundleResult
    draft_raw_sha256: str


@dataclass(frozen=True, slots=True)
class _Finalization:
    ready_root: Path
    ledger_path: Path
    ledger: dict[str, object]
    clock_segments: ClockSegmentMap


_MANIFEST_NAME = "manifest.json"
_RAW_NAME = "records.bin"
_RECEIPT_NAME = "receipt.json"
_READY_DIRECTORY_MODE = 0o750
_READY_FILE_MODE = 0o640
_UINT32_MAX = (1 << 32) - 1
_SHA256_HEX_LENGTH = 64
_LEDGER_FIELDS = frozenset({"bundles", "frontier"})
_LEDGER_ENTRY_FIELDS = frozenset({"records_sha256", "state", "start_sequence", "next_sequence", "draft_raw_sha256"})
_LEGACY_LEDGER_ENTRY_FIELDS = frozenset({"records_sha256", "state"})
_WINDMILL_CHECKPOINT_FIELDS = frozenset({"analysis_cursor", "vad_decisions", "open_speech_tail", "acknowledged"})
_WINDMILL_DECISION_FIELDS = frozenset(
    {"bundle_id", "records_sha256", "packet_ranges", "packet_count", "input_id", "input_sha256", "receipt_sha256"}
)


def finalize_drafts(
    draft_root: Path,
    ready_root: Path,
    ledger_path: Path,
    clock_segments: ClockSegmentMap,
) -> tuple[ReadyBundleResult, ...]:
    """Publish every authenticated draft once, then remove its raw source.

    A destination is durable before its ledger entry; on restart an existing
    validated destination completes the ledger and source cleanup.
    """
    _prepare_directory(draft_root)
    _prepare_directory(ready_root)
    ledger = _read_ledger(ledger_path)
    results: list[ReadyBundleResult] = []
    finalization = _Finalization(ready_root, ledger_path, ledger, clock_segments)
    _reconcile_ready_ledger(finalization)
    for draft in _drafts(draft_root):
        result = _finalize_one(draft, finalization)
        if result is not None:
            results.append(result)
    return tuple(results)


def _finalize_one(draft: _Draft, finalization: _Finalization) -> ReadyBundleResult | None:
    bundle_id = _bundle_id(draft.manifest)
    retired = _retired_entry(finalization.ledger, bundle_id)
    if retired is not None:
        _validate_retired_duplicate(retired, draft.manifest)
        _remove_draft(draft.path)
        return None
    _reject_retired_overlap(finalization.ledger, draft.manifest)
    destination = finalization.ready_root / bundle_id
    if destination.exists():
        ranges = _time_ranges(draft.manifest, finalization.clock_segments)
        result = _validate_ready(destination, draft.manifest, ranges)
        _record_ready(finalization.ledger_path, finalization.ledger, result, draft.manifest)
        _remove_draft(draft.path)
        return result
    source = _unique_suffix(draft, _ready_sources(finalization.ready_root))
    if source is None:
        _remove_draft(draft.path)
        return None
    bundle_id = _bundle_id(source.manifest)
    destination = finalization.ready_root / bundle_id
    ranges = _time_ranges(source.manifest, finalization.clock_segments)
    if destination.exists():
        result = _validate_ready(destination, source.manifest, ranges)
    else:
        result = _write_ready(draft, source, destination, ranges)
    _record_ready(finalization.ledger_path, finalization.ledger, result, source.manifest)
    _remove_draft(draft.path)
    return result


def _drafts(root: Path) -> tuple[_Draft, ...]:
    drafts: list[_Draft] = []
    for path in root.iterdir():
        if path.name.startswith("."):
            continue
        if path.is_symlink() or not path.is_dir():
            raise ReadyBundleError("draft root contains an unsafe entry")
        try:
            manifest = BundleManifest.from_json(_read_json(path / _MANIFEST_NAME))
            receipt = SealedReceipt.from_json(_read_json(path / _RECEIPT_NAME))
        except (OSError, ValueError) as error:
            raise ReadyBundleError("draft bundle manifest is invalid") from error
        raw = path / _RAW_NAME
        if raw.is_symlink() or not raw.is_file() or raw.stat().st_size != manifest.record_count * RECORD_SIZE:
            raise ReadyBundleError("draft bundle records are invalid")
        if _digest(raw) != manifest.raw_sha256 or receipt.raw_sha256 != manifest.raw_sha256:
            raise ReadyBundleError("draft bundle records do not match its receipt")
        drafts.append(_Draft(path, manifest))
    drafts.sort(key=lambda item: item.manifest.start_sequence)
    return tuple(drafts)


def _unique_suffix(draft: _Draft, ready: tuple[_ReadySource, ...]) -> _Draft | None:
    """Return the unclaimed suffix after byte-identical replay records."""
    source = draft.manifest
    overlaps = [item for item in ready if _overlaps(source, item.result)]
    if not overlaps:
        return draft
    cursor = source.start_sequence
    for item in overlaps:
        ready_start = item.result.next_sequence - item.result.record_count
        if ready_start > cursor:
            raise ReadyBundleError("ready overlap is not a replay prefix")
        overlap_next = min(source.next_sequence, item.result.next_sequence)
        _validate_replayed_payload(draft, item, cursor, overlap_next)
        cursor = overlap_next
        if cursor == source.next_sequence:
            return None
    if any(_overlaps_range(cursor, source.next_sequence, item.result) for item in overlaps):
        raise ReadyBundleError("ready overlap is not a replay prefix")
    return _crop_draft(draft, cursor)


def _ready_sources(root: Path) -> tuple[_ReadySource, ...]:
    sources = tuple(sorted((_read_ready_source(path) for path in _visible_children(root)), key=_ready_start))
    if any(_ready_start(left) + left.result.record_count > _ready_start(right) for left, right in pairwise(sources)):
        raise ReadyBundleError("ready bundle sequence ranges overlap")
    return sources


def _reconcile_ready_ledger(finalization: _Finalization) -> None:
    for source in _ready_sources(finalization.ready_root):
        _reconcile_ready_source(finalization, source)


def _reconcile_ready_source(finalization: _Finalization, source: _ReadySource) -> None:
    bundles = finalization.ledger["bundles"]
    assert isinstance(bundles, dict)
    entry = bundles.get(source.result.bundle_id)
    manifest = _source_manifest(source)
    if entry is None:
        _record_ready(finalization.ledger_path, finalization.ledger, source.result, manifest)
        return
    if not isinstance(entry, dict) or entry.get("state") != "ready":
        raise ReadyBundleError("ready bundle is not active in the publication ledger")
    _validate_ready_ledger_entry(entry, source)
    if set(entry) == _LEGACY_LEDGER_ENTRY_FIELDS:
        _record_ready(finalization.ledger_path, finalization.ledger, source.result, manifest)


def _visible_children(root: Path) -> tuple[Path, ...]:
    return tuple(path for path in root.iterdir() if not path.name.startswith("."))


def _ready_start(source: _ReadySource) -> int:
    return source.result.next_sequence - source.result.record_count


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
    if next_sequence != start + count or count == 0 or value["record_size"] != RECORD_SIZE:
        raise ReadyBundleError("ready manifest range is invalid")
    if (
        bundle_id != _bundle_id(BundleManifest(2, start, next_sequence, count, RECORD_SIZE, draft_raw_sha256))
        or path.name != bundle_id
    ):
        raise ReadyBundleError("ready manifest identity is invalid")
    _validate_ready_ranges(value["time_ranges"], start, next_sequence)
    raw = path / _RAW_NAME
    if (
        raw.is_symlink()
        or not raw.is_file()
        or raw.stat().st_size != count * RECORD_SIZE
        or _digest(raw) != records_sha256
    ):
        raise ReadyBundleError("ready records do not match manifest")
    return _ReadySource(ReadyBundleResult(bundle_id, path, next_sequence, count, records_sha256), draft_raw_sha256)


def _validate_ready_ranges(value: object, start: int, next_sequence: int) -> None:
    if not isinstance(value, list):
        raise ReadyBundleError("ready manifest time ranges are invalid")
    cursor = start
    for item in value:
        if not isinstance(item, dict) or set(item) != {"start_sequence", "next_sequence", "utc"}:
            raise ReadyBundleError("ready manifest time ranges are invalid")
        range_start = _nonnegative_int(item["start_sequence"])
        range_next = _nonnegative_int(item["next_sequence"])
        if range_start != cursor or range_next <= range_start:
            raise ReadyBundleError("ready manifest time ranges are invalid")
        _validate_utc(item["utc"])
        cursor = range_next
    if cursor != next_sequence:
        raise ReadyBundleError("ready manifest time ranges are invalid")


def _validate_utc(value: object) -> None:
    if value is None:
        return
    _validate_utc_mapping(value)


def _validate_utc_mapping(value: object) -> None:
    if not isinstance(value, dict):
        raise ReadyBundleError("ready manifest UTC mapping is invalid")
    if set(value) != {"observation_id", "offset_seconds", "uncertainty_seconds"}:
        raise ReadyBundleError("ready manifest UTC mapping is invalid")
    _validate_observation_id(value["observation_id"])
    _finite_number(value["offset_seconds"])
    _validate_nonnegative(_finite_number(value["uncertainty_seconds"]))


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


def _sha256(value: object) -> str:
    if (
        not isinstance(value, str)
        or len(value) != _SHA256_HEX_LENGTH
        or any(char not in "0123456789abcdef" for char in value)
    ):
        raise ReadyBundleError("ready metadata digest is invalid")
    return value


def _overlaps(source: BundleManifest, ready: ReadyBundleResult) -> bool:
    return _overlaps_range(source.start_sequence, source.next_sequence, ready)


def _overlaps_range(start: int, next_sequence: int, ready: ReadyBundleResult) -> bool:
    return start < ready.next_sequence and _ready_start_from_result(ready) < next_sequence


def _ready_start_from_result(ready: ReadyBundleResult) -> int:
    return ready.next_sequence - ready.record_count


def _validate_replayed_payload(draft: _Draft, ready: _ReadySource, start: int, next_sequence: int) -> None:
    draft_offset = draft.byte_offset + (start - draft.manifest.start_sequence) * RECORD_SIZE
    ready_offset = (start - _ready_start(ready)) * RECORD_SIZE
    with (draft.path / _RAW_NAME).open("rb") as raw, (ready.result.path / _RAW_NAME).open("rb") as published:
        raw.seek(draft_offset)
        published.seek(ready_offset)
        for _ in range(next_sequence - start):
            if raw.read(RECORD_SIZE)[TIMESTAMP_SIZE:] != published.read(RECORD_SIZE)[TIMESTAMP_SIZE:]:
                raise ReadyBundleError("ready overlap conflicts with original payload")


def _crop_draft(draft: _Draft, start_sequence: int) -> _Draft:
    source = draft.manifest
    if start_sequence == source.start_sequence:
        return draft
    count = source.next_sequence - start_sequence
    offset = draft.byte_offset + (start_sequence - source.start_sequence) * RECORD_SIZE
    raw_sha256 = _digest_slice(draft.path / _RAW_NAME, offset, count * RECORD_SIZE)
    manifest = BundleManifest(2, start_sequence, source.next_sequence, count, RECORD_SIZE, raw_sha256)
    return _Draft(draft.path, manifest, offset)


def _write_ready(
    draft: _Draft,
    source: _Draft,
    destination: Path,
    ranges: tuple[dict[str, object], ...],
) -> ReadyBundleResult:
    temporary = destination.parent / f".{destination.name}.{uuid4().hex}.tmp"
    temporary.mkdir(mode=_READY_DIRECTORY_MODE)
    try:
        digest = _write_records(draft.path / _RAW_NAME, temporary / _RAW_NAME, source, ranges)
        manifest = _ready_manifest(source.manifest, digest, ranges)
        _write_file(temporary / _MANIFEST_NAME, _canonical(manifest))
        _sync_directory(temporary)
        temporary.rename(destination)
        _sync_directory(destination.parent)
    except BaseException:
        shutil.rmtree(temporary, ignore_errors=True)
        raise
    manifest = source.manifest
    return ReadyBundleResult(destination.name, destination, manifest.next_sequence, manifest.record_count, digest)


def _write_records(
    source_path: Path,
    destination: Path,
    source: _Draft,
    ranges: tuple[dict[str, object], ...],
) -> str:
    digest = sha256()
    range_index = 0
    with source_path.open("rb") as input_stream, destination.open("xb") as output_stream:
        input_stream.seek(source.byte_offset)
        for index in range(source.manifest.record_count):
            record = input_stream.read(RECORD_SIZE)
            if len(record) != RECORD_SIZE:
                raise ReadyBundleError("draft records ended unexpectedly")
            sequence = source.manifest.start_sequence + index
            next_sequence = ranges[range_index]["next_sequence"]
            assert isinstance(next_sequence, int)
            while sequence >= next_sequence:
                range_index += 1
                next_sequence = ranges[range_index]["next_sequence"]
                assert isinstance(next_sequence, int)
            converted = _convert_record(record, ranges[range_index])
            if converted[TIMESTAMP_SIZE:] != record[TIMESTAMP_SIZE:]:
                raise ReadyBundleError("ready conversion changed an Opus payload")
            output_stream.write(converted)
            digest.update(converted)
        output_stream.flush()
        os.fsync(output_stream.fileno())
    return digest.hexdigest()


def _convert_record(record: bytes, time_range: Mapping[str, object]) -> bytes:
    utc = time_range["utc"]
    if utc is None:
        return record
    assert isinstance(utc, Mapping)
    offset = utc["offset_seconds"]
    assert isinstance(offset, float)
    timestamp = int.from_bytes(record[:TIMESTAMP_SIZE], "big")
    normalized = math.floor(timestamp + offset + 0.5)
    if not 0 <= normalized <= _UINT32_MAX:
        raise ReadyBundleError("normalized timestamp is outside uint32")
    return normalized.to_bytes(TIMESTAMP_SIZE, "big") + record[TIMESTAMP_SIZE:]


def _time_ranges(manifest: BundleManifest, segments: ClockSegmentMap) -> tuple[dict[str, object], ...]:
    boundaries = {manifest.start_sequence, manifest.next_sequence}
    for segment in segments.segments:
        if manifest.start_sequence < segment.next_sequence and segment.start_sequence < manifest.next_sequence:
            boundaries.add(max(manifest.start_sequence, segment.start_sequence))
            boundaries.add(min(manifest.next_sequence, segment.next_sequence))
    ordered = tuple(sorted(boundaries))
    ranges = tuple(_time_range(start, next_sequence, segments) for start, next_sequence in pairwise(ordered))
    if (
        not ranges
        or ranges[0]["start_sequence"] != manifest.start_sequence
        or ranges[-1]["next_sequence"] != manifest.next_sequence
    ):
        raise ReadyBundleError("ready time ranges do not cover the draft")
    return ranges


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
        },
    }


def _ready_manifest(
    source: BundleManifest, records_sha256: str, ranges: tuple[dict[str, object], ...]
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


def _validate_ready(
    destination: Path,
    source: BundleManifest,
    ranges: tuple[dict[str, object], ...],
) -> ReadyBundleResult:
    if destination.is_symlink() or not destination.is_dir():
        raise ReadyBundleError("ready destination is unsafe")
    manifest = _read_json(destination / _MANIFEST_NAME)
    expected = _ready_manifest(source, _digest(destination / _RAW_NAME), ranges)
    raw = destination / _RAW_NAME
    if raw.stat().st_size != source.record_count * RECORD_SIZE or manifest != expected:
        raise ReadyBundleError("ready destination conflicts with its draft")
    digest = expected["records_sha256"]
    assert isinstance(digest, str)
    return ReadyBundleResult(destination.name, destination, source.next_sequence, source.record_count, digest)


def _record_ready(
    ledger_path: Path, ledger: dict[str, object], result: ReadyBundleResult, source: BundleManifest
) -> None:
    bundles = ledger["bundles"]
    assert isinstance(bundles, dict)
    entry = _ledger_entry(result.records_sha256, "ready", source)
    existing = bundles.get(result.bundle_id)
    if existing == entry:
        return
    if existing == {"records_sha256": result.records_sha256, "state": "ready"}:
        bundles[result.bundle_id] = entry
        _write_file_atomic(ledger_path, _canonical(ledger))
        return
    if existing is not None:
        raise ReadyBundleError("ready publication ledger conflicts with bundle")
    bundles[result.bundle_id] = entry
    frontier = ledger["frontier"]
    assert isinstance(frontier, int)
    ledger["frontier"] = max(frontier, result.next_sequence)
    _write_file_atomic(ledger_path, _canonical(ledger))


def _ledger_entry(records_sha256: str, state: str, source: BundleManifest) -> dict[str, object]:
    return {
        "records_sha256": records_sha256,
        "state": state,
        "start_sequence": source.start_sequence,
        "next_sequence": source.next_sequence,
        "draft_raw_sha256": source.raw_sha256,
    }


def _retired_entry(ledger: Mapping[str, object], bundle_id: str) -> dict[str, object] | None:
    bundles = ledger["bundles"]
    assert isinstance(bundles, dict)
    entry = bundles.get(bundle_id)
    if isinstance(entry, dict) and entry.get("state") == "retired":
        return entry
    return None


def _validate_retired_duplicate(entry: Mapping[str, object], source: BundleManifest) -> None:
    if (
        entry.get("start_sequence") != source.start_sequence
        or entry.get("next_sequence") != source.next_sequence
        or entry.get("draft_raw_sha256") != source.raw_sha256
    ):
        raise ReadyBundleError("retired ledger entry conflicts with draft")


def _reject_retired_overlap(ledger: Mapping[str, object], source: BundleManifest) -> None:
    bundles = ledger["bundles"]
    assert isinstance(bundles, dict)
    for entry in bundles.values():
        if not isinstance(entry, dict) or entry.get("state") != "retired":
            continue
        start = entry.get("start_sequence")
        next_sequence = entry.get("next_sequence")
        if (
            isinstance(start, int)
            and isinstance(next_sequence, int)
            and _valid_range(start, next_sequence)
            and source.start_sequence < next_sequence
            and start < source.next_sequence
        ):
            raise ReadyBundleError("draft overlaps a retired range without original payload evidence")


def retire_acknowledged(ready_root: Path, ledger_path: Path, checkpoint_path: Path) -> tuple[ReadyBundleResult, ...]:
    """Retire only Windmill bundles durably ACKed outside an open speech tail."""
    _prepare_directory(ready_root)
    acknowledged = _windmill_acknowledged(checkpoint_path)
    if not acknowledged:
        return ()
    ledger = _read_ledger(ledger_path)
    for bundle_id, records_sha256 in acknowledged:
        _preflight_retirement(ready_root, ledger, bundle_id, records_sha256)
    retired: list[ReadyBundleResult] = []
    for bundle_id, records_sha256 in acknowledged:
        retired.append(_retire_one(ready_root, ledger_path, ledger, bundle_id, records_sha256))
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
    ready_root: Path, ledger_path: Path, ledger: dict[str, object], bundle_id: str, records_sha256: str
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
        bundles[bundle_id] = _ledger_entry(records_sha256, "retired", _source_manifest(source))
        _write_file_atomic(ledger_path, _canonical(ledger))
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


def _source_manifest(source: _ReadySource) -> BundleManifest:
    start = _ready_start(source)
    return BundleManifest(
        2, start, source.result.next_sequence, source.result.record_count, RECORD_SIZE, source.draft_raw_sha256
    )


def _validate_ready_ledger_entry(entry: Mapping[str, object], source: _ReadySource) -> None:
    if entry.get("records_sha256") != source.result.records_sha256:
        raise ReadyBundleError("ready publication ledger digest conflicts with ready bundle")
    if set(entry) == _LEGACY_LEDGER_ENTRY_FIELDS:
        return
    manifest = _source_manifest(source)
    if (
        entry.get("start_sequence") != manifest.start_sequence
        or entry.get("next_sequence") != manifest.next_sequence
        or entry.get("draft_raw_sha256") != manifest.raw_sha256
    ):
        raise ReadyBundleError("ready publication ledger range conflicts with ready bundle")


def _entry_next_sequence(entry: object) -> int:
    if not isinstance(entry, dict):
        raise ReadyBundleError("ready publication ledger is invalid")
    return _nonnegative_int(entry["next_sequence"])


def _entry_count(entry: object) -> int:
    if not isinstance(entry, dict):
        raise ReadyBundleError("ready publication ledger is invalid")
    return _entry_next_sequence(entry) - _nonnegative_int(entry["start_sequence"])


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
    if value["analysis_cursor"] is not None:
        _windmill_decisions([value["analysis_cursor"]])
    decisions = _windmill_decisions(value["vad_decisions"])
    tail = _windmill_tail(value["open_speech_tail"])
    acknowledged = _windmill_identities(value["acknowledged"], "acknowledged")
    if len(acknowledged) != len(set(acknowledged)):
        raise ReadyBundleError("Windmill acknowledgement is duplicated")
    if not set(acknowledged) <= decisions:
        raise ReadyBundleError("Windmill acknowledgement lacks durable VAD evidence")
    if set(acknowledged) & tail:
        raise ReadyBundleError("Windmill acknowledgement conflicts with open speech tail")
    return tuple(acknowledged)


def _windmill_decisions(value: object) -> set[tuple[str, str]]:
    if not isinstance(value, list):
        raise ReadyBundleError("Windmill checkpoint decisions are invalid")
    identities: set[tuple[str, str]] = set()
    for decision in value:
        if not isinstance(decision, dict) or set(decision) != _WINDMILL_DECISION_FIELDS:
            raise ReadyBundleError("Windmill checkpoint decisions are invalid")
        identities.add((_sha256(decision["bundle_id"]), _sha256(decision["records_sha256"])))
    return identities


def _windmill_tail(value: object) -> set[tuple[str, str]]:
    if value is None:
        return set()
    if not isinstance(value, dict) or set(value) != {"entries"}:
        raise ReadyBundleError("Windmill open speech tail is invalid")
    return _windmill_decisions(value["entries"])


def _windmill_identities(value: object, label: str) -> list[tuple[str, str]]:
    if not isinstance(value, list):
        raise ReadyBundleError(f"Windmill {label} is invalid")
    identities: list[tuple[str, str]] = []
    for item in value:
        if not isinstance(item, dict) or set(item) != {"bundle_id", "records_sha256"}:
            raise ReadyBundleError(f"Windmill {label} is invalid")
        identities.append((_sha256(item["bundle_id"]), _sha256(item["records_sha256"])))
    return identities


def _read_ledger(path: Path) -> dict[str, object]:
    if not path.exists():
        return {"bundles": {}, "frontier": 0}
    value = _read_json(path)
    if not isinstance(value, dict):
        raise ReadyBundleError("ready publication ledger is invalid")
    _validate_ledger_header(value)
    bundles = value["bundles"]
    frontier = value["frontier"]
    assert isinstance(bundles, dict)
    assert isinstance(frontier, int)
    _validate_ledger_entries(bundles)
    return value


def _validate_ledger_header(value: dict[str, object]) -> None:
    if set(value) != _LEDGER_FIELDS:
        raise ReadyBundleError("ready publication ledger is invalid")
    _validate_ledger_values(value["bundles"], value["frontier"])


def _validate_ledger_values(bundles: object, frontier: object) -> None:
    if not isinstance(bundles, dict):
        raise ReadyBundleError("ready publication ledger is invalid")
    if isinstance(frontier, bool) or not isinstance(frontier, int) or frontier < 0:
        raise ReadyBundleError("ready publication ledger is invalid")


def _validate_ledger_entries(bundles: dict[object, object]) -> None:
    for bundle_id, entry in bundles.items():
        if not _valid_ledger_entry(bundle_id, entry):
            raise ReadyBundleError("ready publication ledger is invalid")


def _valid_ledger_entry(bundle_id: object, entry: object) -> bool:
    if not isinstance(bundle_id, str) or not isinstance(entry, dict):
        return False
    if not _is_sha256(bundle_id) or not _is_sha256(entry.get("records_sha256")):
        return False
    if set(entry) == _LEGACY_LEDGER_ENTRY_FIELDS:
        return entry.get("state") == "ready"
    return (
        set(entry) == _LEDGER_ENTRY_FIELDS
        and entry.get("state") in {"ready", "retired"}
        and _valid_range(entry.get("start_sequence"), entry.get("next_sequence"))
        and _is_sha256(entry.get("draft_raw_sha256"))
    )


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


def _bundle_id(manifest: BundleManifest) -> str:
    identity = f"{manifest.start_sequence}:{manifest.next_sequence}:{manifest.raw_sha256}".encode()
    return sha256(identity).hexdigest()


def _remove_draft(path: Path) -> None:
    shutil.rmtree(path)
    _sync_directory(path.parent)


def _prepare_directory(path: Path) -> None:
    path.mkdir(mode=_READY_DIRECTORY_MODE, parents=True, exist_ok=True)
    if path.is_symlink() or not path.is_dir():
        raise ReadyBundleError("ready storage root is unsafe")


def _write_file(path: Path, payload: bytes) -> None:
    with path.open("xb") as stream:
        stream.write(payload)
        stream.flush()
        os.fsync(stream.fileno())
    path.chmod(_READY_FILE_MODE)


def _write_file_atomic(path: Path, payload: bytes) -> None:
    path.parent.mkdir(mode=_READY_DIRECTORY_MODE, parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid4().hex}.tmp")
    try:
        _write_file(temporary, payload)
        temporary.replace(path)
        _sync_directory(path.parent)
    finally:
        temporary.unlink(missing_ok=True)


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


def _digest_slice(path: Path, offset: int, length: int) -> str:
    if path.is_symlink() or not path.is_file():
        raise ReadyBundleError("ready records are not a regular file")
    digest = sha256()
    remaining = length
    with path.open("rb") as stream:
        stream.seek(offset)
        while remaining:
            chunk = stream.read(min(1024 * 1024, remaining))
            if not chunk:
                raise ReadyBundleError("draft records ended unexpectedly")
            digest.update(chunk)
            remaining -= len(chunk)
    return digest.hexdigest()


def _sync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
