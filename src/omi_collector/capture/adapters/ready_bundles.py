"""Finalize raw draft bundles into immutable, flat ready bundles."""

from __future__ import annotations

import json
import math
import os
import shutil
import stat
from bisect import bisect_right
from collections.abc import Mapping
from dataclasses import dataclass
from enum import StrEnum
from hashlib import sha256
from itertools import pairwise
from math import ceil
from pathlib import Path
from typing import cast
from uuid import uuid4

from ...config import ReadyConfig
from ..domain.opus_duration import inspect_20ms_record
from ..domain.ready_machine import ReadyCommand, decide_ready
from ..domain.ring_protocol import RECORD_SIZE, TIMESTAMP_SIZE
from .bundle_contract import BundleManifest, SealedReceipt
from .clock_segments import ClockSegmentMap


class ReadyBundleError(RuntimeError):
    """A draft cannot be safely finalized into a ready bundle."""


class ConflictingOverlapError(ReadyBundleError):
    """Two authenticated drafts claim different bytes for one sequence."""


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
    inventory: ReadyInventory | None = None


def source_revision(*roots: Path, extras: tuple[Path, ...] = ()) -> tuple[tuple[str, int, int, int, int, int], ...]:
    """Return cheap filesystem identity for publication inputs, without opening payloads."""
    entries: list[tuple[str, int, int, int, int, int]] = []
    for root in (*roots, *extras):
        paths = (root, *root.iterdir()) if root.is_dir() else (root,)
        for path in paths:
            try:
                info = path.lstat()
            except FileNotFoundError:
                continue
            entries.append((str(path), info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns))
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
                            child_info.st_size,
                            child_info.st_mtime_ns,
                            child_info.st_ctime_ns,
                        )
                    )
    return tuple(sorted(entries))


@dataclass(frozen=True, slots=True)
class _Draft:
    """An authenticated raw draft, optionally cropped after a replay prefix."""

    path: Path
    manifest: BundleManifest
    byte_offset: int = 0


@dataclass(frozen=True, slots=True)
class _DraftGroup:
    drafts: tuple[_Draft, ...]
    facts: _BundleFacts


@dataclass(frozen=True, slots=True)
class _BundleFacts:
    start_sequence: int
    next_sequence: int
    record_count: int
    raw_sha256: str
    gaps: tuple[tuple[int, int], ...] = ()


@dataclass(frozen=True, slots=True)
class _DraftAudio:
    packet_count: int
    first_size: int
    final_overflow_size: int | None
    continuity_breaks: tuple[int, ...]


@dataclass(frozen=True, slots=True)
class _ReadySource:
    """A validated ready bundle used only to compare replayed payloads."""

    result: ReadyBundleResult
    facts: _BundleFacts


@dataclass(slots=True)
class ReadyInventory:
    drafts: tuple[_Draft, ...]
    ready: tuple[_ReadySource, ...]
    identities: dict[str, tuple[tuple[int, int, int, int, int], ...]]
    audio_packets: dict[tuple[str, int, int], _DraftAudio]
    replay_slices: dict[str, tuple[_Draft, ...]] | None = None
    replay_keys: dict[str, tuple[tuple[str, tuple[tuple[int, int, int, int, int], ...]], ...]] | None = None
    normalized_slices: tuple[_Draft, ...] | None = None
    normalization_key: tuple[tuple[str, int, int, tuple[tuple[int, int, int, int, int], ...]], ...] | None = None
    normalization_error: str | None = None


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
_GAP_ENDPOINT_COUNT = 2
_LEDGER_FIELDS = frozenset({"bundles", "frontier"})
_LEDGER_ENTRY_FIELDS = frozenset({"records_sha256", "state", "start_sequence", "next_sequence", "draft_raw_sha256"})
_WINDMILL_CHECKPOINT_FIELDS = frozenset({"analysis_cursor", "vad_decisions", "open_speech_tail", "acknowledged"})
_WINDMILL_TAIL_FIELDS = frozenset({"entries", "opened_at", "outputs"})


def finalize_drafts(  # noqa: PLR0913 - the four storage paths and explicit publication boundary stay visible
    draft_root: Path,
    ready_root: Path,
    ledger_path: Path,
    clock_segments: ClockSegmentMap,
    *,
    config: ReadyConfig,
    frontier: int | None = None,
    drained: bool = False,
    inventory: ReadyInventory | None = None,
) -> ReadyOutcome:
    """Publish every authenticated draft once, then remove its raw source.

    A destination is durable before its ledger entry; on restart an existing
    validated destination completes the ledger and source cleanup.
    """
    if drained and frontier is None:
        raise ReadyBundleError("a drained publication requires its durable frontier")
    _prepare_directory(draft_root)
    _prepare_directory(ready_root)
    _require_shared_ready_directory(ready_root)
    ledger = _read_ledger(ledger_path)
    results: list[ReadyBundleResult] = []
    finalization = _Finalization(ready_root, ledger_path, ledger, clock_segments)
    inventory = authenticated_inventory(draft_root, ready_root, inventory)
    _reconcile_ready_ledger(finalization, inventory.ready)
    eligible, replayed_paths = _eligible_drafts(inventory, frontier)
    eligible = _normalize_drafts(eligible, inventory)
    if not eligible:
        for path in replayed_paths:
            _remove_draft(path)
        return ReadyOutcome(ReadyOutcomeState.WAITING, reason="no_eligible_drafts", inventory=inventory)
    packets = sum(_cached_draft_audio(draft, inventory).packet_count for draft in eligible)
    decision = decide_ready(
        drained=drained,
        has_audio=packets > 0,
        threshold_met=packets >= ceil(config.target_audio_seconds * 50),
    )
    if decision.command is ReadyCommand.PUBLISH:
        source_paths = tuple(
            draft.path for draft in inventory.drafts if frontier is None or draft.manifest.next_sequence <= frontier
        )
        for group in _continuity_groups(eligible, inventory):
            result = _finalize_group(group, finalization, ())
            if result is not None:
                results.append(result)
        for path in source_paths:
            _remove_draft(path)
    else:
        for path in replayed_paths:
            _remove_draft(path)
    return ReadyOutcome(
        ReadyOutcomeState.PUBLISHED if results else ReadyOutcomeState.WAITING,
        tuple(results),
        None if results else decision.state.value,
        bool(eligible) and not results,
        inventory=inventory,
    )


def _eligible_drafts(inventory: ReadyInventory, frontier: int | None) -> tuple[tuple[_Draft, ...], tuple[Path, ...]]:
    eligible: list[_Draft] = []
    replayed_paths: list[Path] = []
    for draft in inventory.drafts:
        if frontier is not None and draft.manifest.next_sequence > frontier:
            continue
        key = str(draft.path)
        slices = inventory.replay_slices
        if slices is not None and key in slices:
            source = slices[key]
        else:
            source = _unique_slices(draft, inventory.ready)
            if slices is not None:
                slices[key] = source
        if not source:
            replayed_paths.append(draft.path)
        else:
            eligible.extend(source)
    return tuple(eligible), tuple(replayed_paths)


def authenticated_inventory(draft_root: Path, ready_root: Path, previous: ReadyInventory | None) -> ReadyInventory:
    previous = previous or ReadyInventory((), (), {}, {})
    prior_drafts = {str(draft.path): draft for draft in previous.drafts}
    prior_ready = {str(source.result.path): source for source in previous.ready}
    identities: dict[str, tuple[tuple[int, int, int, int, int], ...]] = {}
    drafts: list[_Draft] = []
    ready: list[_ReadySource] = []
    audio_packets = dict(previous.audio_packets)
    for path in draft_root.iterdir():
        if path.name.startswith("."):
            continue
        identity = _source_identity(path)
        identities[str(path)] = identity
        draft = prior_drafts.get(str(path))
        if draft is None or previous.identities.get(str(path)) != identity:
            draft = _read_draft(path)
            audio_packets = {key: value for key, value in audio_packets.items() if key[0] != str(path)}
        drafts.append(draft)
    for path in _visible_children(ready_root):
        identity = _source_identity(path)
        identities[str(path)] = identity
        source = prior_ready.get(str(path))
        if source is None or previous.identities.get(str(path)) != identity:
            source = _read_ready_source(path)
        ready.append(source)
    drafts.sort(key=lambda item: item.manifest.start_sequence)
    ready.sort(key=_ready_start)
    if any(left.result.next_sequence > _ready_start(right) for left, right in pairwise(ready)):
        raise ReadyBundleError("ready bundle sequence ranges overlap")
    draft_paths = {str(draft.path) for draft in drafts}
    audio_packets = {key: value for key, value in audio_packets.items() if key[0] in draft_paths}
    replay_keys = {
        str(draft.path): tuple(
            (str(source.result.path), identities[str(source.result.path)])
            for source in ready
            if _overlaps(draft.manifest, source)
        )
        for draft in drafts
    }
    replay_slices = {
        key: value
        for key, value in (previous.replay_slices or {}).items()
        if key in draft_paths
        and previous.identities.get(key) == identities.get(key)
        and (previous.replay_keys or {}).get(key) == replay_keys[key]
    }
    if previous.identities == identities and previous.drafts == tuple(drafts) and previous.ready == tuple(ready):
        return previous
    return ReadyInventory(
        tuple(drafts),
        tuple(ready),
        identities,
        audio_packets,
        replay_slices,
        replay_keys,
        previous.normalized_slices,
        previous.normalization_key,
        previous.normalization_error,
    )


def _source_identity(path: Path) -> tuple[tuple[int, int, int, int, int], ...]:
    if path.is_symlink():
        raise ReadyBundleError("publication source contains an unsafe entry")
    paths = [path]
    if path.is_dir():
        paths.extend(path.iterdir())
    result = []
    for item in paths:
        info = item.lstat()
        result.append((info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns))
    return tuple(result)


def _read_draft(path: Path) -> _Draft:
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
    return _Draft(path, manifest)


def _cached_draft_audio(draft: _Draft, inventory: ReadyInventory) -> _DraftAudio:
    key = (str(draft.path), draft.byte_offset, draft.manifest.record_count)
    if key not in inventory.audio_packets:
        inventory.audio_packets[key] = _draft_audio(draft)
    return inventory.audio_packets[key]


def has_drafts_at_or_below(draft_root: Path, frontier: int) -> bool:
    """Return whether a closed frontier still owns an authenticated draft."""
    if not draft_root.exists():
        return False
    return any(draft.manifest.next_sequence <= frontier for draft in _drafts(draft_root))


def draft_frontier(draft_root: Path) -> int | None:
    """Return the highest authenticated draft boundary, if any."""
    if not draft_root.exists():
        return None
    drafts = _drafts(draft_root)
    return max((draft.manifest.next_sequence for draft in drafts), default=None)


def _make_group(drafts: tuple[_Draft, ...]) -> _DraftGroup:
    if not drafts:
        raise ReadyBundleError("cannot publish an empty draft group")
    digest = sha256()
    count = 0
    for draft in drafts:
        with (draft.path / _RAW_NAME).open("rb") as stream:
            stream.seek(draft.byte_offset)
            remaining = draft.manifest.record_count * RECORD_SIZE
            while remaining:
                block = stream.read(min(remaining, 1024 * 1024))
                if not block:
                    raise ReadyBundleError("draft records ended unexpectedly")
                digest.update(block)
                remaining -= len(block)
        count += draft.manifest.record_count
    first, last = drafts[0].manifest, drafts[-1].manifest
    gaps = tuple(
        (left.manifest.next_sequence, right.manifest.start_sequence)
        for left, right in pairwise(drafts)
        if left.manifest.next_sequence < right.manifest.start_sequence
    )
    return _DraftGroup(drafts, _BundleFacts(first.start_sequence, last.next_sequence, count, digest.hexdigest(), gaps))


def _draft_audio(draft: _Draft) -> _DraftAudio:
    total = 0
    first_size = 0
    previous_overflow: int | None = None
    continuity_breaks: list[int] = []
    with (draft.path / _RAW_NAME).open("rb") as stream:
        stream.seek(draft.byte_offset)
        for index in range(draft.manifest.record_count):
            record = stream.read(RECORD_SIZE)
            if len(record) != RECORD_SIZE:
                raise ReadyBundleError("draft records ended unexpectedly")
            try:
                layout = inspect_20ms_record(record)
            except ValueError as error:
                raise ReadyBundleError("draft contains an invalid 20 ms Opus packet") from error
            if index == 0:
                first_size = layout.first_size
            elif previous_overflow is not None and layout.first_size != previous_overflow:
                continuity_breaks.append(draft.manifest.start_sequence + index)
            total += layout.packet_count
            previous_overflow = layout.overflow_size
    return _DraftAudio(total, first_size, previous_overflow, tuple(continuity_breaks))


def _continuity_groups(drafts: tuple[_Draft, ...], inventory: ReadyInventory) -> tuple[_DraftGroup, ...]:
    """Split physical output where firmware packet continuation is unavailable."""
    groups: list[_DraftGroup] = []
    current: list[_Draft] = []
    previous: _Draft | None = None
    previous_audio: _DraftAudio | None = None
    for draft in drafts:
        audio = _cached_draft_audio(draft, inventory)
        if (
            previous is not None
            and previous_audio is not None
            and previous.manifest.next_sequence == draft.manifest.start_sequence
            and previous_audio.final_overflow_size is not None
            and audio.first_size != previous_audio.final_overflow_size
        ):
            groups.append(_make_group(tuple(current)))
            current.clear()
        cursor = draft.manifest.start_sequence
        for boundary in (*audio.continuity_breaks, draft.manifest.next_sequence):
            current.append(_crop_draft(draft, cursor, boundary))
            if boundary != draft.manifest.next_sequence:
                groups.append(_make_group(tuple(current)))
                current.clear()
            cursor = boundary
        previous, previous_audio = draft, audio
    if current:
        groups.append(_make_group(tuple(current)))
    return tuple(groups)


def _finalize_group(
    group: _DraftGroup, finalization: _Finalization, source_paths: tuple[Path, ...]
) -> ReadyBundleResult | None:
    facts = group.facts
    bundle_id = _bundle_id(facts)
    retired = _retired_entry(finalization.ledger, bundle_id)
    if retired is not None:
        _validate_retired_duplicate(retired, facts)
        for path in source_paths:
            _remove_draft(path)
        return None
    _reject_retired_overlap(finalization.ledger, facts)
    _reject_out_of_order(finalization.ledger, facts, bundle_id)
    destination = finalization.ready_root / bundle_id
    if destination.exists():
        ready = _read_ready_source(destination)
        if ready.facts != facts:
            raise ReadyBundleError("ready destination conflicts with its draft group")
        result = ready.result
        _record_ready(finalization.ledger_path, finalization.ledger, result, facts)
        for path in source_paths:
            _remove_draft(path)
        return result
    ranges = _time_ranges(facts, finalization.clock_segments)
    ranges = _clear_unrepresentable_ranges(group, ranges)
    result = _write_ready_group(group, destination, ranges)
    _record_ready(finalization.ledger_path, finalization.ledger, result, facts)
    for path in source_paths:
        _remove_draft(path)
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


def _unique_slices(draft: _Draft, ready: tuple[_ReadySource, ...]) -> tuple[_Draft, ...]:
    """Remove only authenticated physical ready records from a draft."""
    start, end = draft.manifest.start_sequence, draft.manifest.next_sequence
    cursor = start
    slices: list[_Draft] = []
    for source in ready:
        ready_offset = 0
        for run_start, run_end in _physical_runs(source.facts):
            if run_end <= cursor:
                ready_offset += run_end - run_start
                continue
            if run_start >= end:
                break
            if cursor < run_start:
                slices.append(_crop_draft(draft, cursor, min(run_start, end)))
                cursor = min(run_start, end)
            overlap_end = min(run_end, end)
            if cursor < overlap_end:
                _validate_replayed_payload(draft, source, cursor, overlap_end, ready_offset + cursor - run_start)
                cursor = overlap_end
            ready_offset += run_end - run_start
            if cursor == end:
                return tuple(slices)
    if cursor < end:
        slices.append(_crop_draft(draft, cursor, end))
    return tuple(slices)


def _normalize_drafts(drafts: tuple[_Draft, ...], inventory: ReadyInventory) -> tuple[_Draft, ...]:
    key = tuple(
        (str(draft.path), draft.byte_offset, draft.manifest.record_count, inventory.identities[str(draft.path)])
        for draft in sorted(drafts, key=lambda item: (item.manifest.start_sequence, str(item.path)))
    )
    if inventory.normalization_key == key and inventory.normalized_slices is not None:
        return inventory.normalized_slices
    if inventory.normalization_key == key and inventory.normalization_error is not None:
        raise ConflictingOverlapError(inventory.normalization_error)
    normalized: list[_Draft] = []
    ends: list[int] = []
    try:
        for draft in sorted(drafts, key=lambda item: (item.manifest.start_sequence, str(item.path))):
            for unique in _unique_against_drafts(draft, normalized, bisect_right(ends, draft.manifest.start_sequence)):
                position = bisect_right(ends, unique.manifest.start_sequence)
                normalized.insert(position, unique)
                ends.insert(position, unique.manifest.next_sequence)
    except ConflictingOverlapError as error:
        inventory.normalization_key = key
        inventory.normalized_slices = None
        inventory.normalization_error = str(error)
        raise
    result = tuple(normalized)
    inventory.normalization_key = key
    inventory.normalized_slices = result
    inventory.normalization_error = None
    return result


def _unique_against_drafts(draft: _Draft, normalized: list[_Draft], index: int) -> tuple[_Draft, ...]:
    end = draft.manifest.next_sequence
    cursor = draft.manifest.start_sequence
    unique: list[_Draft] = []
    while index < len(normalized):
        retained = normalized[index]
        left, right = retained.manifest.start_sequence, retained.manifest.next_sequence
        if left >= end:
            break
        if cursor < left:
            unique.append(_crop_draft(draft, cursor, min(left, end)))
            cursor = min(left, end)
        overlap_end = min(right, end)
        if cursor < overlap_end:
            _compare_draft_overlap(draft, retained, cursor, overlap_end)
            cursor = overlap_end
        if cursor == end:
            break
        index += 1
    if cursor < end:
        unique.append(_crop_draft(draft, cursor, end))
    return tuple(unique)


def _compare_draft_overlap(left: _Draft, right: _Draft, start: int, end: int) -> None:
    left_offset = left.byte_offset + (start - left.manifest.start_sequence) * RECORD_SIZE
    right_offset = right.byte_offset + (start - right.manifest.start_sequence) * RECORD_SIZE
    remaining = (end - start) * RECORD_SIZE
    with (left.path / _RAW_NAME).open("rb") as left_stream, (right.path / _RAW_NAME).open("rb") as right_stream:
        left_stream.seek(left_offset)
        right_stream.seek(right_offset)
        while remaining:
            size = min(remaining, RECORD_SIZE * 2048)
            left_bytes, right_bytes = left_stream.read(size), right_stream.read(size)
            if len(left_bytes) != size or left_bytes != right_bytes:
                raise ConflictingOverlapError("authenticated draft overlap has conflicting record bytes")
            remaining -= size


def _reconcile_ready_ledger(finalization: _Finalization, ready: tuple[_ReadySource, ...]) -> None:
    for source in ready:
        _reconcile_ready_source(finalization, source)


def _reconcile_ready_source(finalization: _Finalization, source: _ReadySource) -> None:
    bundles = finalization.ledger["bundles"]
    assert isinstance(bundles, dict)
    entry = bundles.get(source.result.bundle_id)
    if entry is None:
        _record_ready(finalization.ledger_path, finalization.ledger, source.result, source.facts)
        return
    if not isinstance(entry, dict) or entry.get("state") != "ready":
        raise ReadyBundleError("ready bundle is not active in the publication ledger")
    _validate_ready_ledger_entry(entry, source)


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
    return _ReadySource(ReadyBundleResult(bundle_id, path, next_sequence, count, records_sha256), facts)


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


def _sha256(value: object) -> str:
    if (
        not isinstance(value, str)
        or len(value) != _SHA256_HEX_LENGTH
        or any(char not in "0123456789abcdef" for char in value)
    ):
        raise ReadyBundleError("ready metadata digest is invalid")
    return value


def _overlaps(source: BundleManifest, ready: _ReadySource) -> bool:
    return source.start_sequence < ready.facts.next_sequence and ready.facts.start_sequence < source.next_sequence


def _validate_replayed_payload(
    draft: _Draft, ready: _ReadySource, start: int, next_sequence: int, ready_record_offset: int
) -> None:
    draft_offset = draft.byte_offset + (start - draft.manifest.start_sequence) * RECORD_SIZE
    with (draft.path / _RAW_NAME).open("rb") as raw, (ready.result.path / _RAW_NAME).open("rb") as published:
        raw.seek(draft_offset)
        published.seek(ready_record_offset * RECORD_SIZE)
        for _ in range(next_sequence - start):
            original, converted = raw.read(RECORD_SIZE), published.read(RECORD_SIZE)
            if (
                len(original) != RECORD_SIZE
                or len(converted) != RECORD_SIZE
                or original[TIMESTAMP_SIZE:] != converted[TIMESTAMP_SIZE:]
            ):
                raise ReadyBundleError("ready overlap conflicts with original payload")


def _crop_draft(draft: _Draft, start_sequence: int, next_sequence: int) -> _Draft:
    source = draft.manifest
    if start_sequence == source.start_sequence and next_sequence == source.next_sequence:
        return draft
    count = next_sequence - start_sequence
    offset = draft.byte_offset + (start_sequence - source.start_sequence) * RECORD_SIZE
    raw_sha256 = _digest_slice(draft.path / _RAW_NAME, offset, count * RECORD_SIZE)
    manifest = BundleManifest(2, start_sequence, next_sequence, count, RECORD_SIZE, raw_sha256)
    return _Draft(draft.path, manifest, offset)


def _write_ready_group(
    group: _DraftGroup,
    destination: Path,
    ranges: tuple[dict[str, object], ...],
) -> ReadyBundleResult:
    temporary = destination.parent / f".{destination.name}.{uuid4().hex}.tmp"
    temporary.mkdir(mode=_READY_DIRECTORY_MODE)
    try:
        _require_shared_ready_directory(temporary)
        digest = sha256()
        range_index = 0
        with (temporary / _RAW_NAME).open("xb") as output:
            for draft in group.drafts:
                with (draft.path / _RAW_NAME).open("rb") as source:
                    source.seek(draft.byte_offset)
                    for index in range(draft.manifest.record_count):
                        record = source.read(RECORD_SIZE)
                        if len(record) != RECORD_SIZE:
                            raise ReadyBundleError("draft records ended unexpectedly")
                        sequence = draft.manifest.start_sequence + index
                        next_sequence = ranges[range_index]["next_sequence"]
                        assert isinstance(next_sequence, int)
                        while sequence >= next_sequence:
                            range_index += 1
                            next_sequence = ranges[range_index]["next_sequence"]
                            assert isinstance(next_sequence, int)
                        converted = _convert_record(record, ranges[range_index])
                        if converted[TIMESTAMP_SIZE:] != record[TIMESTAMP_SIZE:]:
                            raise ReadyBundleError("ready conversion changed an Opus payload")
                        output.write(converted)
                        digest.update(converted)
            output.flush()
            os.fsync(output.fileno())
        (temporary / _RAW_NAME).chmod(_READY_FILE_MODE)
        _write_file(temporary / _MANIFEST_NAME, _canonical(_ready_manifest(group.facts, digest.hexdigest(), ranges)))
        _sync_directory(temporary)
        temporary.rename(destination)
        _sync_directory(destination.parent)
    except BaseException:
        shutil.rmtree(temporary, ignore_errors=True)
        raise
    return ReadyBundleResult(
        destination.name, destination, group.facts.next_sequence, group.facts.record_count, digest.hexdigest()
    )


def _clear_unrepresentable_ranges(
    group: _DraftGroup, ranges: tuple[dict[str, object], ...]
) -> tuple[dict[str, object], ...]:
    """Keep every record intact when UTC normalization would exceed its uint32 header."""
    invalid: set[int] = set()
    range_index = 0
    next_sequence = ranges[0]["next_sequence"]
    assert isinstance(next_sequence, int)
    for draft in group.drafts:
        with (draft.path / _RAW_NAME).open("rb") as source:
            source.seek(draft.byte_offset)
            for index in range(draft.manifest.record_count):
                record = source.read(RECORD_SIZE)
                if len(record) != RECORD_SIZE:
                    raise ReadyBundleError("draft records ended unexpectedly")
                sequence = draft.manifest.start_sequence + index
                while sequence >= next_sequence:
                    range_index += 1
                    next_sequence = ranges[range_index]["next_sequence"]
                    assert isinstance(next_sequence, int)
                utc = ranges[range_index]["utc"]
                if utc is None:
                    continue
                assert isinstance(utc, Mapping)
                timestamp = int.from_bytes(record[:TIMESTAMP_SIZE], "big")
                offset = utc["offset_seconds"]
                assert isinstance(offset, float)
                normalized = math.floor(timestamp + offset + 0.5)
                if not 0 <= normalized <= _UINT32_MAX:
                    invalid.add(range_index)
    return tuple({**item, "utc": None} if index in invalid else item for index, item in enumerate(ranges))


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


def _record_ready(
    ledger_path: Path, ledger: dict[str, object], result: ReadyBundleResult, source: _BundleFacts
) -> None:
    bundles = ledger["bundles"]
    assert isinstance(bundles, dict)
    entry = _ledger_entry(result.records_sha256, "ready", source)
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


def _ledger_entry(records_sha256: str, state: str, source: _BundleFacts) -> dict[str, object]:
    return {
        "records_sha256": records_sha256,
        "state": state,
        "start_sequence": source.start_sequence,
        "next_sequence": source.next_sequence,
        "draft_raw_sha256": source.raw_sha256,
        **({"gaps": [list(gap) for gap in source.gaps]} if source.gaps else {}),
    }


def _retired_entry(ledger: Mapping[str, object], bundle_id: str) -> dict[str, object] | None:
    bundles = ledger["bundles"]
    assert isinstance(bundles, dict)
    entry = bundles.get(bundle_id)
    if isinstance(entry, dict) and entry.get("state") == "retired":
        return entry
    return None


def _validate_retired_duplicate(entry: Mapping[str, object], source: _BundleFacts) -> None:
    if (
        entry.get("start_sequence") != source.start_sequence
        or entry.get("next_sequence") != source.next_sequence
        or entry.get("draft_raw_sha256") != source.raw_sha256
        or _entry_gaps(entry) != source.gaps
    ):
        raise ReadyBundleError("retired ledger entry conflicts with draft")


def _reject_retired_overlap(ledger: Mapping[str, object], source: _BundleFacts) -> None:
    bundles = ledger["bundles"]
    assert isinstance(bundles, dict)
    for entry in bundles.values():
        if not isinstance(entry, dict) or entry.get("state") != "retired":
            continue
        if any(
            source_start < retired_end and retired_start < source_end
            for source_start, source_end in _physical_runs(source)
            for retired_start, retired_end in _physical_runs(_entry_facts(entry))
        ):
            raise ReadyBundleError("draft overlaps a retired range without original payload evidence")


def _reject_out_of_order(ledger: Mapping[str, object], source: _BundleFacts, bundle_id: str) -> None:
    bundles = ledger["bundles"]
    assert isinstance(bundles, dict)
    if any(
        source.start_sequence < entry["next_sequence"]
        for key, entry in bundles.items()
        if key != bundle_id and isinstance(entry, dict)
    ):
        raise ReadyBundleError("draft ordering is blocked by an already published bundle")


def resume_retired(ready_root: Path, ledger_path: Path) -> None:
    """Finish already committed retirements before reconciling new publication."""
    _prepare_directory(ready_root)
    bundles = _read_ledger(ledger_path)["bundles"]
    assert isinstance(bundles, dict)
    retired: list[Path] = []
    for bundle_id, entry in bundles.items():
        if isinstance(entry, dict) and entry.get("state") == "retired":
            destination = ready_root / _sha256(bundle_id)
            _resume_retirement(destination, entry)
            retired.append(destination)
    for destination in retired:
        _remove_retired_ready(destination)


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
        bundles[bundle_id] = _ledger_entry(records_sha256, "retired", source.facts)
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
    start, end = entry.get("start_sequence"), entry.get("next_sequence")
    if (
        set(entry) not in (_LEDGER_ENTRY_FIELDS, _LEDGER_ENTRY_FIELDS | {"gaps"})
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
    return bundle_id == _bundle_id(_BundleFacts(start, end, end - start - sum(b - a for a, b in gaps), digest, gaps))


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


def _remove_draft(path: Path) -> None:
    shutil.rmtree(path)
    _sync_directory(path.parent)


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
