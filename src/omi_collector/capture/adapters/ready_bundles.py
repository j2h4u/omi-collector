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
    for draft, source in _drafts(draft_root):
        result = _finalize_one(draft, source, finalization)
        results.append(result)
    return tuple(results)


def _finalize_one(
    draft: Path,
    source: BundleManifest,
    finalization: _Finalization,
) -> ReadyBundleResult:
    bundle_id = _bundle_id(source)
    destination = finalization.ready_root / bundle_id
    ranges = _time_ranges(source, finalization.clock_segments)
    if destination.exists():
        result = _validate_ready(destination, source, ranges)
    else:
        result = _write_ready(draft, source, destination, ranges)
    _record_ready(finalization.ledger_path, finalization.ledger, result)
    _remove_draft(draft)
    return result


def _drafts(root: Path) -> tuple[tuple[Path, BundleManifest], ...]:
    drafts: list[tuple[Path, BundleManifest]] = []
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
        drafts.append((path, manifest))
    drafts.sort(key=lambda item: item[1].start_sequence)
    if any(left.next_sequence > right.start_sequence for (_, left), (_, right) in pairwise(drafts)):
        raise ReadyBundleError("draft bundles overlap")
    return tuple(drafts)


def _write_ready(
    draft: Path,
    source: BundleManifest,
    destination: Path,
    ranges: tuple[dict[str, object], ...],
) -> ReadyBundleResult:
    temporary = destination.parent / f".{destination.name}.{uuid4().hex}.tmp"
    temporary.mkdir(mode=_READY_DIRECTORY_MODE)
    try:
        digest = _write_records(draft / _RAW_NAME, temporary / _RAW_NAME, source, ranges)
        manifest = _ready_manifest(source, digest, ranges)
        _write_file(temporary / _MANIFEST_NAME, _canonical(manifest))
        _sync_directory(temporary)
        temporary.rename(destination)
        _sync_directory(destination.parent)
    except BaseException:
        shutil.rmtree(temporary, ignore_errors=True)
        raise
    return ReadyBundleResult(destination.name, destination, source.next_sequence, source.record_count, digest)


def _write_records(
    source: Path,
    destination: Path,
    manifest: BundleManifest,
    ranges: tuple[dict[str, object], ...],
) -> str:
    digest = sha256()
    range_index = 0
    with source.open("rb") as input_stream, destination.open("xb") as output_stream:
        for index in range(manifest.record_count):
            record = input_stream.read(RECORD_SIZE)
            if len(record) != RECORD_SIZE:
                raise ReadyBundleError("draft records ended unexpectedly")
            sequence = manifest.start_sequence + index
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


def _record_ready(ledger_path: Path, ledger: dict[str, object], result: ReadyBundleResult) -> None:
    bundles = ledger["bundles"]
    assert isinstance(bundles, dict)
    entry = {"records_sha256": result.records_sha256, "state": "ready"}
    existing = bundles.get(result.bundle_id)
    if existing == entry:
        return
    if existing is not None:
        raise ReadyBundleError("ready publication ledger conflicts with bundle")
    bundles[result.bundle_id] = entry
    frontier = ledger["frontier"]
    assert isinstance(frontier, int)
    ledger["frontier"] = max(frontier, result.next_sequence)
    _write_file_atomic(ledger_path, _canonical(ledger))


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
    if set(value) != {"bundles", "frontier"}:
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
    return (
        isinstance(bundle_id, str)
        and len(bundle_id) == _SHA256_HEX_LENGTH
        and isinstance(entry, dict)
        and set(entry) == {"records_sha256", "state"}
        and entry.get("state") == "ready"
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


def _sync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
