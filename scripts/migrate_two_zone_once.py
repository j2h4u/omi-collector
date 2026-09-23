"""One-time, fail-closed migration from legacy Omi generations to two zones.

Run without ``--execute`` first.  Execute is deliberately refused while any
producer or consumer that can touch this storage is active.
"""

from __future__ import annotations

import argparse
import fcntl
import json
import os
import shutil
import stat
import subprocess
import sys
from collections.abc import Iterable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from hashlib import sha256
from itertools import pairwise
from pathlib import Path
from typing import cast
from uuid import uuid4

from omi_collector.capture.adapters import ready_bundles
from omi_collector.capture.adapters.bundle_contract import BundleManifest, SealedReceipt
from omi_collector.capture.adapters.clock_segments import ClockSegment, ClockSegmentMap
from omi_collector.capture.domain.ring_protocol import RECORD_SIZE, TIMESTAMP_SIZE, parse_audio_payload


class MigrationError(RuntimeError):
    """The frozen legacy inventory cannot safely be migrated."""


_SHA256_HEX_LENGTH = 64
_SPEECH_PAIR_COUNT = 12
_OUTPUT_DIRECTORY_MODE = 0o750
_READY_DIRECTORY_MODE = 0o2750
_OUTPUT_FILE_MODE = 0o640
_WINDMILL_JOB_LIMIT = 100
_TEMPORARY_NAME_PARTS = 4
_UUID_HEX_LENGTH = 32
_HISTORIC_ACK_COUNT = 81
_WINDMILL_STORAGE_PATHS = (
    "f/omi/speech_archive",
    "f/omi/discover_bundles",
    "f/omi/prepare_vad_chunks",
    "f/omi/authorize_vad_chunk",
    "f/omi/publish_speech_archive",
    "f/omi/commit_archive",
    "f/audio/vad_analyze",
)


@dataclass(frozen=True, slots=True)
class Bundle:
    path: Path
    manifest: BundleManifest
    receipt: SealedReceipt


@dataclass(frozen=True, slots=True)
class ReadyMove:
    legacy: Bundle
    captured: Bundle
    ranges: tuple[dict[str, object], ...]


@dataclass(frozen=True, slots=True)
class DraftMove:
    source: Bundle
    start_sequence: int
    next_sequence: int

    @property
    def complete(self) -> bool:
        return (
            self.start_sequence == self.source.manifest.start_sequence
            and self.next_sequence == self.source.manifest.next_sequence
        )


@dataclass(frozen=True, slots=True)
class Plan:
    generation: Path
    ready: tuple[ReadyMove, ...]
    drafts: tuple[DraftMove, ...]


@dataclass(frozen=True, slots=True)
class Paths:
    root: Path
    current: Path
    captured: Path
    draft: Path
    ready: Path
    ledger: Path
    log: Path
    inventory: Path
    state: Path
    evidence: Path
    generation_evidence: Path
    checkpoint: Path | None
    queue_attestation: Path | None
    speech: Path


def _read_json(path: Path) -> object:
    _regular(path, "JSON")
    try:
        return cast(object, json.loads(path.read_text(encoding="utf-8")))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise MigrationError(f"invalid JSON: {path}") from error


def _regular(path: Path, label: str) -> None:
    if path.is_symlink() or not path.is_file():
        raise MigrationError(f"{label} must be a regular file: {path}")


def _directory(path: Path, label: str) -> None:
    if path.is_symlink() or not path.is_dir():
        raise MigrationError(f"{label} must be a regular directory: {path}")


def _digest(path: Path) -> str:
    _regular(path, "records")
    digest = sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _bundle(path: Path) -> Bundle:
    _directory(path, "bundle")
    if {item.name for item in path.iterdir()} != {"manifest.json", "receipt.json", "records.bin"}:
        raise MigrationError(f"bundle inventory is not exact: {path}")
    try:
        manifest = BundleManifest.from_json(_read_json(path / "manifest.json"))
        receipt = SealedReceipt.from_json(_read_json(path / "receipt.json"))
    except ValueError as error:
        raise MigrationError(f"bundle contract is invalid: {path}") from error
    records = path / "records.bin"
    if records.stat().st_size != manifest.record_count * RECORD_SIZE or _digest(records) != manifest.raw_sha256:
        raise MigrationError(f"bundle records do not match manifest: {path}")
    if receipt.raw_sha256 != manifest.raw_sha256:
        raise MigrationError(f"bundle receipt does not match manifest: {path}")
    return Bundle(path, manifest, receipt)


def _children(root: Path, *, allow_overlap: bool = False) -> tuple[Bundle, ...]:
    _directory(root, "bundle root")
    result = tuple(sorted((_bundle(path) for path in root.iterdir()), key=lambda item: item.manifest.start_sequence))
    if not allow_overlap and any(
        left.manifest.next_sequence > right.manifest.start_sequence for left, right in pairwise(result)
    ):
        raise MigrationError(f"bundle ranges overlap in {root}")
    return result


def _generation(
    current: Path,
) -> tuple[Path, tuple[Bundle, ...], tuple[tuple[int, int, int, str], ...], frozenset[str]]:
    if not current.is_symlink():
        raise MigrationError("legacy current must be its one expected symlink")
    generation = current.resolve(strict=True)
    if not generation.is_relative_to(current.parent / ".generations"):
        raise MigrationError("legacy current escapes .generations")
    _directory(generation, "legacy generation")
    value = _read_json(generation / "generation.json")
    if not isinstance(value, dict) or set(value) != {
        "algorithm",
        "generation_id",
        "record_count",
        "repairs",
        "source_hashes",
    }:
        raise MigrationError("legacy generation metadata is not exact")
    if (
        value["generation_id"] != generation.name
        or isinstance(value["record_count"], bool)
        or not isinstance(value["record_count"], int)
    ):
        raise MigrationError("legacy generation identity is invalid")
    repairs = _repairs(value["repairs"])
    source_hashes = _hashes(value["source_hashes"])
    bundles = _children_except(generation, {"generation.json"})
    if sum(item.manifest.record_count for item in bundles) != value["record_count"]:
        raise MigrationError("legacy generation count is invalid")
    return generation, bundles, repairs, source_hashes


def _children_except(root: Path, files: set[str]) -> tuple[Bundle, ...]:
    entries = tuple(root.iterdir())
    if {item.name for item in entries if item.is_file()} != files or any(item.is_symlink() for item in entries):
        raise MigrationError("legacy generation inventory is not exact")
    result = tuple(
        sorted((_bundle(item) for item in entries if item.is_dir()), key=lambda item: item.manifest.start_sequence)
    )
    if not result or any(
        left.manifest.next_sequence > right.manifest.start_sequence for left, right in pairwise(result)
    ):
        raise MigrationError("legacy generation ranges are invalid")
    return result


def _repairs(value: object) -> tuple[tuple[int, int, int, str], ...]:
    if not isinstance(value, list):
        raise MigrationError("legacy repairs are invalid")
    repairs: list[tuple[int, int, int, str]] = []
    for item in value:
        if not isinstance(item, dict) or set(item) != {"evidence", "start_sequence", "next_sequence", "offset_seconds"}:
            raise MigrationError("legacy repair is invalid")
        start, end, offset, evidence = (
            item["start_sequence"],
            item["next_sequence"],
            item["offset_seconds"],
            item["evidence"],
        )
        if (
            any(isinstance(number, bool) or not isinstance(number, int) for number in (start, end, offset))
            or not isinstance(evidence, str)
            or not evidence
            or end <= start
        ):
            raise MigrationError("legacy repair values are invalid")
        repairs.append((start, end, offset, evidence))
    repairs.sort()
    if any(left[1] > right[0] for left, right in pairwise(repairs)):
        raise MigrationError("legacy repairs overlap")
    return tuple(repairs)


def _hashes(value: object) -> frozenset[str]:
    if (
        not isinstance(value, list)
        or not value
        or any(not isinstance(item, str) or len(item) != _SHA256_HEX_LENGTH for item in value)
    ):
        raise MigrationError("legacy source hashes are invalid")
    result = frozenset(value)
    if len(result) != len(value) or any(
        any(character not in "0123456789abcdef" for character in item) for item in result
    ):
        raise MigrationError("legacy source hashes are invalid")
    return result


def _same_payload(
    left: Bundle, right: Bundle, start: int, end: int, *, corrected: Iterable[tuple[int, int, int, str]] = ()
) -> None:
    left_offset = (start - left.manifest.start_sequence) * RECORD_SIZE
    right_offset = (start - right.manifest.start_sequence) * RECORD_SIZE
    corrections = tuple(corrected)
    with (left.path / "records.bin").open("rb") as left_stream, (right.path / "records.bin").open("rb") as right_stream:
        left_stream.seek(left_offset)
        right_stream.seek(right_offset)
        for sequence in range(start, end):
            raw, normalized = left_stream.read(RECORD_SIZE), right_stream.read(RECORD_SIZE)
            if (
                len(raw) != RECORD_SIZE
                or len(normalized) != RECORD_SIZE
                or raw[TIMESTAMP_SIZE:] != normalized[TIMESTAMP_SIZE:]
            ):
                raise MigrationError("raw and normalized payloads disagree")
            offset = next((item[2] for item in corrections if item[0] <= sequence < item[1]), 0)
            expected = int.from_bytes(raw[:TIMESTAMP_SIZE], "big") - offset
            if not 0 <= expected < 1 << (TIMESTAMP_SIZE * 8) or normalized[:TIMESTAMP_SIZE] != expected.to_bytes(
                TIMESTAMP_SIZE, "big"
            ):
                raise MigrationError("normalized timestamp does not match legacy repair")


def _ranges(manifest: BundleManifest, repairs: tuple[tuple[int, int, int, str], ...]) -> tuple[dict[str, object], ...]:
    segments = tuple(
        ClockSegment(
            evidence, max(start, manifest.start_sequence), min(end, manifest.next_sequence), float(-offset), 0.0
        )
        for start, end, offset, evidence in repairs
        if start < manifest.next_sequence and manifest.start_sequence < end
    )
    return ready_bundles._time_ranges(manifest, ClockSegmentMap(segments))


def _ready_plan(
    legacy: tuple[Bundle, ...], captured: tuple[Bundle, ...], repairs: tuple[tuple[int, int, int, str], ...]
) -> tuple[ReadyMove, ...]:
    by_range: dict[tuple[int, int], list[Bundle]] = {}
    for item in captured:
        by_range.setdefault((item.manifest.start_sequence, item.manifest.next_sequence), []).append(item)
    ready: list[ReadyMove] = []
    for item in legacy:
        matches: list[Bundle] = []
        for candidate in by_range.get((item.manifest.start_sequence, item.manifest.next_sequence), []):
            try:
                _same_payload(
                    candidate,
                    item,
                    candidate.manifest.start_sequence,
                    candidate.manifest.next_sequence,
                    corrected=repairs,
                )
            except MigrationError:
                continue
            matches.append(candidate)
        if len(matches) != 1:
            raise MigrationError(f"legacy normalized bundle has no unique captured source: {item.path.name}")
        source = matches[0]
        ready.append(ReadyMove(item, source, _ranges(source.manifest, repairs)))
    return tuple(ready)


def _assert_owned(root: Path, owner: int) -> None:
    for path in (root, *root.rglob("*")):
        if path.is_symlink() or path.stat().st_uid != owner:
            raise MigrationError(f"storage owner or path type is unsafe: {path}")


def _owned_regular(path: Path, owner: int, label: str) -> None:
    _regular(path, label)
    if path.stat().st_uid != owner:
        raise MigrationError(f"storage owner is unsafe: {path}")


def _inside(path: Path, root: Path, label: str) -> None:
    if not path.is_relative_to(root):
        raise MigrationError(f"{label} escapes its expected root: {path}")


def build_plan(paths: Paths) -> Plan:
    generation, legacy, repairs, expected_hashes = _generation(paths.current)
    captured = _children(paths.captured, allow_overlap=True)
    owner = paths.captured.stat().st_uid
    _assert_owned(paths.captured, owner)
    _assert_owned(generation, owner)
    ready = _ready_plan(legacy, captured, repairs)
    if {item.captured.manifest.raw_sha256 for item in ready} != expected_hashes:
        raise MigrationError("legacy generation source provenance does not match captured inventory")
    seen = [item.captured for item in ready]
    drafts: list[DraftMove] = []
    for source in captured:
        intervals: list[tuple[int, int]] = []
        for prior in seen:
            start, end = (
                max(source.manifest.start_sequence, prior.manifest.start_sequence),
                min(source.manifest.next_sequence, prior.manifest.next_sequence),
            )
            if start < end:
                _same_payload(source, prior, start, end)
                intervals.append((start, end))
        seen.append(source)
        cursor = source.manifest.start_sequence
        for start, end in sorted(intervals):
            if start > cursor:
                drafts.append(DraftMove(source, cursor, start))
            cursor = max(cursor, end)
        if cursor < source.manifest.next_sequence:
            drafts.append(DraftMove(source, cursor, source.manifest.next_sequence))
    return Plan(generation, tuple(ready), tuple(drafts))


def _canonical(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()


def _write(path: Path, payload: bytes) -> None:
    temporary = path.with_name(f".{path.name}.{uuid4().hex}.tmp")
    try:
        with temporary.open("xb") as stream:
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        temporary.replace(path)
        ready_bundles._sync_directory(path.parent)
    finally:
        temporary.unlink(missing_ok=True)


def _prepare_output(path: Path) -> None:
    path.mkdir(mode=_OUTPUT_DIRECTORY_MODE, parents=True, exist_ok=True)
    _directory(path, "migration output")
    if not os.access(path, os.W_OK | os.X_OK):
        raise MigrationError(f"migration output is not writable: {path}")


def _set_runtime_ownership(path: Path, uid: int, gid: int, *, directory_mode: int = _OUTPUT_DIRECTORY_MODE) -> None:
    details = path.lstat()
    if stat.S_ISLNK(details.st_mode):
        raise MigrationError(f"migration ownership target is unsafe: {path}")
    if stat.S_ISDIR(details.st_mode):
        flags, mode = os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC | os.O_NOFOLLOW, directory_mode
    elif stat.S_ISREG(details.st_mode):
        flags, mode = os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW, _OUTPUT_FILE_MODE
    else:
        raise MigrationError(f"migration ownership target is unsafe: {path}")
    try:
        descriptor = os.open(path, flags)
    except OSError as error:
        raise MigrationError(f"migration ownership target is unavailable: {path}") from error
    try:
        os.fchown(descriptor, uid, gid)
        os.fchmod(descriptor, mode)
        os.fsync(descriptor)
    except OSError as error:
        raise MigrationError(f"cannot set migration runtime ownership: {path}") from error
    finally:
        os.close(descriptor)


def _set_runtime_tree_ownership(
    root: Path, uid: int, gid: int, *, directory_mode: int = _OUTPUT_DIRECTORY_MODE
) -> None:
    _directory(root, "migration output")
    paths = (root, *sorted(root.rglob("*")))
    for path in paths:
        _set_runtime_ownership(path, uid, gid, directory_mode=directory_mode)
    ready_bundles._sync_directory(root.parent)


def _exact_output_tree(root: Path, outputs: tuple[Path, ...]) -> None:
    _directory(root, "migration output")
    if any(path.parent != root for path in outputs):
        raise MigrationError("migration output path escapes its root")
    if {path.name for path in root.iterdir()} != {path.name for path in outputs}:
        raise MigrationError(f"migration output inventory is not exact: {root}")


def _runtime_owners(paths: Paths) -> tuple[tuple[int, int], tuple[int, int], tuple[int, int]]:
    captured = paths.captured.stat()
    speech = paths.speech.stat()
    collector = paths.ledger.parent.stat()
    return (captured.st_uid, captured.st_gid), (captured.st_uid, speech.st_gid), (collector.st_uid, collector.st_gid)


def _set_runtime_outputs(paths: Paths, ready: tuple[Path, ...], drafts: tuple[Path, ...]) -> None:
    _exact_output_tree(paths.draft, drafts)
    _exact_output_tree(paths.ready, ready)
    draft_owner, ready_owner, collector_owner = _runtime_owners(paths)
    _set_runtime_tree_ownership(paths.draft, *draft_owner)
    _set_runtime_tree_ownership(paths.ready, *ready_owner, directory_mode=_READY_DIRECTORY_MODE)
    _set_runtime_ownership(paths.ledger, *collector_owner)
    _set_runtime_ownership(paths.state, *collector_owner)


def _rename(source: Path, destination: Path) -> None:
    source_parent, destination_parent = source.parent, destination.parent
    source.rename(destination)
    ready_bundles._sync_directory(destination_parent)
    if source_parent != destination_parent:
        ready_bundles._sync_directory(source_parent)


def _cleanup_temporary(root: Path, owner: int) -> None:
    if not root.exists():
        return
    _directory(root, "migration output")
    for path in root.iterdir():
        if not _migration_temporary(path.name):
            continue
        if path.is_symlink() or path.stat().st_uid != owner:
            raise MigrationError(f"migration temporary is unsafe: {path}")
        if path.is_dir():
            shutil.rmtree(path)
        elif path.is_file():
            path.unlink()
        else:
            raise MigrationError(f"migration temporary is unsafe: {path}")
        ready_bundles._sync_directory(root)


def _migration_temporary(name: str) -> bool:
    pieces = name.split(".")
    return (
        len(pieces) >= _TEMPORARY_NAME_PARTS
        and pieces[0] == ""
        and pieces[-1] == "tmp"
        and len(pieces[-2]) == _UUID_HEX_LENGTH
        and all(character in "0123456789abcdef" for character in pieces[-2])
    )


def _copy_slice(source: DraftMove, draft_root: Path) -> None:
    count = source.next_sequence - source.start_sequence
    offset = (source.start_sequence - source.source.manifest.start_sequence) * RECORD_SIZE
    records = source.source.path / "records.bin"
    digest = sha256()
    manifest_path = draft_root / f"{source.start_sequence}-{source.next_sequence}"
    temporary = draft_root / f".{manifest_path.name}.{uuid4().hex}.tmp"
    temporary.mkdir(mode=0o750)
    try:
        with records.open("rb") as input_stream, (temporary / "records.bin").open("xb") as output_stream:
            input_stream.seek(offset)
            remaining = count * RECORD_SIZE
            while remaining:
                chunk = input_stream.read(min(1024 * 1024, remaining))
                if not chunk:
                    raise MigrationError("captured records ended while cropping draft")
                output_stream.write(chunk)
                digest.update(chunk)
                remaining -= len(chunk)
            output_stream.flush()
            os.fsync(output_stream.fileno())
        manifest = BundleManifest(
            2, source.start_sequence, source.next_sequence, count, RECORD_SIZE, digest.hexdigest()
        )
        destination = draft_root / f"{source.start_sequence}-{source.next_sequence}-{manifest.raw_sha256[:16]}"
        if destination.exists():
            raise MigrationError("cropped draft destination already exists")
        _write(temporary / "manifest.json", _canonical(manifest.as_dict()))
        _write(
            temporary / "receipt.json",
            _canonical(SealedReceipt(source.source.receipt.attempt_id, manifest.raw_sha256).as_dict()),
        )
        ready_bundles._sync_directory(temporary)
        _rename(temporary, destination)
    except BaseException:
        shutil.rmtree(temporary, ignore_errors=True)
        raise


def _bundle_inventory(bundle: Bundle, legacy_source_id: str | None = None) -> dict[str, object]:
    return {
        "path": str(bundle.path),
        "manifest_sha256": _digest(bundle.path / "manifest.json"),
        "receipt_sha256": _digest(bundle.path / "receipt.json"),
        "records_sha256": _digest(bundle.path / "records.bin"),
        "raw_sha256": bundle.manifest.raw_sha256,
        "start_sequence": bundle.manifest.start_sequence,
        "next_sequence": bundle.manifest.next_sequence,
        "legacy_source_id": legacy_source_id,
    }


def _speech_inventory(speech: Path, owner: int) -> tuple[dict[str, str], ...]:
    _directory(speech, "speech root")
    _assert_owned(speech, owner)
    pairs: dict[str, dict[str, Path]] = {}
    for item in speech.iterdir():
        _owned_regular(item, owner, "speech artifact")
        if item.suffix not in {".ogg", ".json"} or not item.stem:
            raise MigrationError(f"speech inventory is not an Ogg/passport pair: {item}")
        pairs.setdefault(item.stem, {})[item.suffix] = item
    if len(pairs) != _SPEECH_PAIR_COUNT or any(set(pair) != {".ogg", ".json"} for pair in pairs.values()):
        raise MigrationError("speech inventory is not exactly twelve Ogg/passport pairs")
    return tuple(
        {
            "ogg_path": str(pair[".ogg"]),
            "ogg_sha256": _digest(pair[".ogg"]),
            "passport_path": str(pair[".json"]),
            "passport_sha256": _digest(pair[".json"]),
        }
        for _, pair in sorted(pairs.items())
    )


def _frozen_before(
    paths: Paths, plan: Plan, evidence: list[dict[str, str]], generation_evidence: dict[str, str]
) -> dict[str, object]:
    if paths.checkpoint is None:
        raise MigrationError("--checkpoint is required for execute")
    if paths.queue_attestation is None:
        raise MigrationError("--windmill-queue-attestation is required for execute")
    checkpoint = paths.checkpoint.resolve(strict=True)
    work = paths.root / "work"
    _inside(checkpoint, work, "checkpoint")
    work_owner = work.stat().st_uid
    _owned_regular(checkpoint, work_owner, "checkpoint")
    _assert_owned(work, work_owner)
    attestation = _queue_attestation(paths.queue_attestation, work, work_owner)
    captured = _children(paths.captured, allow_overlap=True)
    source_ids = {move.captured.path: _source_id(move.legacy) for move in plan.ready}
    return {
        "checkpoint": {"path": str(checkpoint), "sha256": _digest(checkpoint)},
        "windmill_queue_attestation": attestation,
        "legacy_generation": {
            "path": str(plan.generation),
            "generation_sha256": _digest(plan.generation / "generation.json"),
            **generation_evidence,
        },
        "captured": [_bundle_inventory(bundle, source_ids.get(bundle.path)) for bundle in captured],
        "draft": [_bundle_inventory(bundle) for bundle in _children(paths.draft)],
        "speech": list(_speech_inventory(paths.speech, paths.speech.stat().st_uid)),
        "legacy_evidence": evidence,
    }


def _ready_inventory(ready: Path) -> list[dict[str, object]]:
    sources = tuple(
        sorted(
            (ready_bundles._read_ready_source(path).result for path in ready.iterdir()), key=lambda item: item.bundle_id
        )
    )
    return [
        {
            "bundle_id": source.bundle_id,
            "records_sha256": source.records_sha256,
            "start_sequence": source.next_sequence - source.record_count,
            "next_sequence": source.next_sequence,
            "manifest_sha256": _digest(ready / source.bundle_id / "manifest.json"),
        }
        for source in sources
    ]


def _frozen_inventory(paths: Paths, before: dict[str, object], provenance: dict[str, object]) -> dict[str, object]:
    current_checkpoint = cast(dict[str, str], before["checkpoint"])
    if _digest(Path(current_checkpoint["path"])) != current_checkpoint["sha256"]:
        raise MigrationError("checkpoint changed during migration")
    speech_before = cast(list[dict[str, str]], before["speech"])
    if list(_speech_inventory(paths.speech, paths.speech.stat().st_uid)) != speech_before:
        raise MigrationError("speech inventory changed during migration")
    return {
        "schema": "omi-two-zone-frozen-inventory-v1",
        "checkpoint": before["checkpoint"],
        "windmill_queue_attestation": before["windmill_queue_attestation"],
        "legacy_generation": before["legacy_generation"],
        "captured": before["captured"],
        "draft": [_bundle_inventory(bundle) for bundle in _children(paths.draft)],
        "ready": _ready_inventory(paths.ready),
        "speech": before["speech"],
        "mapping_sha256": sha256(_canonical(provenance)).hexdigest(),
        "legacy_evidence": before["legacy_evidence"],
    }


def _legacy_evidence(paths: Paths, plan: Plan) -> list[dict[str, str]]:
    _prepare_output(paths.evidence)
    result: list[dict[str, str]] = []
    for move in plan.ready:
        source_id = _source_id(move.legacy)
        destination = paths.evidence / source_id
        if destination.exists():
            _directory(destination, "legacy evidence")
            if {item.name for item in destination.iterdir()} != {"manifest.json", "receipt.json"}:
                raise MigrationError("legacy evidence inventory is not exact")
        else:
            temporary = paths.evidence / f".{source_id}.{uuid4().hex}.tmp"
            temporary.mkdir(mode=0o750)
            try:
                _write(temporary / "manifest.json", (move.legacy.path / "manifest.json").read_bytes())
                _write(temporary / "receipt.json", (move.legacy.path / "receipt.json").read_bytes())
                ready_bundles._sync_directory(temporary)
                _rename(temporary, destination)
            except BaseException:
                shutil.rmtree(temporary, ignore_errors=True)
                raise
        manifest, receipt = destination / "manifest.json", destination / "receipt.json"
        if _digest(manifest) != _digest(move.legacy.path / "manifest.json") or _digest(receipt) != _digest(
            move.legacy.path / "receipt.json"
        ):
            raise MigrationError("legacy evidence does not match source")
        result.append(
            {
                "source_id": source_id,
                "path": str(destination),
                "manifest_sha256": _digest(manifest),
                "receipt_sha256": _digest(receipt),
            }
        )
    if {path.name for path in paths.evidence.iterdir()} != {item["source_id"] for item in result}:
        raise MigrationError("legacy evidence inventory is not exact")
    return result


def _set_work_evidence_ownership(paths: Paths) -> None:
    work = paths.root / "work"
    _directory(work, "work root")
    owner = work.stat()
    _set_runtime_tree_ownership(paths.evidence, owner.st_uid, owner.st_gid)
    _set_runtime_ownership(paths.generation_evidence, owner.st_uid, owner.st_gid)


def _generation_evidence(paths: Paths, plan: Plan) -> dict[str, str]:
    source = plan.generation / "generation.json"
    if paths.generation_evidence.exists():
        _regular(paths.generation_evidence, "generation evidence")
    else:
        _prepare_output(paths.generation_evidence.parent)
        _write(paths.generation_evidence, source.read_bytes())
    if _digest(paths.generation_evidence) != _digest(source):
        raise MigrationError("generation evidence does not match source")
    return {"evidence_path": str(paths.generation_evidence), "evidence_sha256": _digest(paths.generation_evidence)}


def _queue_attestation(path: Path, work: Path, owner: int) -> dict[str, str]:
    resolved = path.resolve(strict=True)
    _inside(resolved, work, "Windmill queue attestation")
    _owned_regular(resolved, owner, "Windmill queue attestation")
    value = _read_json(resolved)
    if value != {"schema": "omi-windmill-queue-attestation-v1", "queued_omi_jobs": []}:
        raise MigrationError("Windmill queue attestation is invalid")
    return {"path": str(resolved), "sha256": _digest(resolved)}


def _run(command: list[str]) -> subprocess.CompletedProcess[str]:
    try:
        return subprocess.run(command, check=False, text=True, capture_output=True, timeout=30)
    except (OSError, subprocess.TimeoutExpired) as error:
        raise MigrationError(f"quiescence check unavailable: {' '.join(command)}") from error


def _inactive(service: str, *, disabled: bool = False, optional: bool = False) -> None:
    result = _run(
        [
            "systemctl",
            "show",
            service,
            "--property=LoadState",
            "--property=ActiveState",
            "--property=UnitFileState",
        ]
    )
    if result.returncode != 0:
        raise MigrationError(f"service probe failed: {service}")
    properties = dict(line.split("=", 1) for line in result.stdout.splitlines() if "=" in line)
    if set(properties) != {"LoadState", "ActiveState", "UnitFileState"}:
        raise MigrationError(f"service probe is incomplete: {service}")
    if properties["LoadState"] == "not-found":
        if optional and properties["ActiveState"] == "inactive":
            return
        raise MigrationError(f"service is not loaded: {service}")
    if properties["LoadState"] != "loaded" or properties["ActiveState"] != "inactive":
        raise MigrationError(f"service is not inactive: {service}")
    if disabled and properties["UnitFileState"] != "disabled":
        raise MigrationError(f"service is not disabled: {service}")


def _quiescent(paths: Paths) -> None:
    _inactive("omi-collector.service")
    _inactive("omi-speech-archive-jit.service", disabled=True, optional=True)
    _windmill_quiescent()
    for root in (paths.captured, paths.draft, paths.ready, paths.ledger.parent, paths.current.resolve()):
        result = _run(["lsof", "-t", "+D", str(root)])
        if result.returncode not in {0, 1}:
            raise MigrationError("open-file probe failed")
        if result.stdout.strip():
            raise MigrationError(f"storage has open file descriptors: {root}")
    _locks_available(paths)


def _lock_paths(paths: Paths) -> tuple[Path, Path]:
    return paths.root / "collector" / "collector.lock", paths.root / "work" / "speech_archive_jit.lock"


@contextmanager
def _held_producer_locks(paths: Paths) -> Iterator[None]:
    streams = []
    try:
        for lock in _lock_paths(paths):
            _regular(lock, "lock")
            stream = lock.open("r+b")
            try:
                fcntl.flock(stream, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError as error:
                stream.close()
                raise MigrationError(f"storage lock is held: {lock}") from error
            streams.append(stream)
        yield
    finally:
        for stream in streams:
            fcntl.flock(stream, fcntl.LOCK_UN)
            stream.close()


def _locks_available(paths: Paths) -> None:
    with _held_producer_locks(paths):
        pass


def _windmill_quiescent() -> None:
    schedules = _json_command(["wmill", "schedule", "list", "--json"], "Windmill schedule probe")
    if any(_enabled_target_schedule(cast(dict[str, object], item)) for item in schedules):
        raise MigrationError("an Omi Windmill schedule is enabled")
    jobs = _json_command(
        ["wmill", "job", "list", "--running", "--all", "--limit", str(_WINDMILL_JOB_LIMIT), "--json"],
        "Windmill running-job probe",
    )
    if len(jobs) == _WINDMILL_JOB_LIMIT:
        raise MigrationError("Windmill running-job probe reached its exhaustive limit")
    if any(_storage_job(cast(dict[str, object], item)) for item in jobs):
        raise MigrationError("a storage-capable Windmill job is running")
    queue = _run(
        [
            "docker",
            "compose",
            "-f",
            "/opt/docker/windmill/compose.yaml",
            "exec",
            "-T",
            "db",
            "psql",
            "-U",
            "postgres",
            "-d",
            "windmill",
            "-Atc",
            (
                "select count(*) from v2_job_queue q join v2_job j on j.id=q.id and j.workspace_id=q.workspace_id "
                "where j.runnable_path in ('f/omi/speech_archive','f/omi/discover_bundles','f/omi/prepare_vad_chunks',"
                "'f/omi/authorize_vad_chunk','f/omi/publish_speech_archive','f/omi/commit_archive','f/audio/vad_analyze')"
            ),
        ]
    )
    if queue.returncode != 0 or queue.stderr or queue.stdout != "0\n":
        raise MigrationError("Windmill storage queue probe failed or is nonzero")


def _json_command(command: list[str], label: str) -> list[object]:
    result = _run(command)
    if result.returncode != 0:
        raise MigrationError(f"{label} failed")
    try:
        value = cast(object, json.loads(result.stdout))
    except json.JSONDecodeError as error:
        raise MigrationError(f"{label} is not JSON") from error
    if not isinstance(value, list) or any(not isinstance(item, dict) for item in value):
        raise MigrationError(f"{label} has an invalid shape")
    return value


def _enabled_target_schedule(schedule: dict[str, object]) -> bool:
    enabled, path = schedule.get("enabled"), schedule.get("script_path")
    if not isinstance(enabled, bool) or not isinstance(path, str):
        raise MigrationError("Windmill schedule probe has an invalid shape")
    return enabled and path in _WINDMILL_STORAGE_PATHS


def _storage_job(job: dict[str, object]) -> bool:
    path = job.get("script_path")
    if not isinstance(path, str):
        raise MigrationError("Windmill running-job probe has an invalid shape")
    return path in _WINDMILL_STORAGE_PATHS


def _packet_count(bundle: Bundle) -> int:
    with (bundle.path / "records.bin").open("rb") as stream:
        return sum(
            len(parse_audio_payload(stream.read(RECORD_SIZE)[TIMESTAMP_SIZE:]))
            for _ in range(bundle.manifest.record_count)
        )


def _source_id(bundle: Bundle) -> str:
    return _source_id_from_receipt(bundle, bundle.path / "receipt.json")


def _source_id_from_receipt(bundle: Bundle, receipt_path: Path) -> str:
    receipt_hash = sha256(receipt_path.read_bytes()).hexdigest()
    encoded = _canonical({"raw_sha256": bundle.manifest.raw_sha256, "receipt_sha256": receipt_hash}) + b"\n"
    return sha256(encoded).hexdigest()


def _log(paths: Paths, plan: Plan) -> dict[str, object]:
    mappings: list[dict[str, object]] = []
    for move in plan.ready:
        legacy, captured = move.legacy, move.captured
        mappings.append(
            {
                "legacy": {
                    "source_id": _source_id(legacy),
                    "raw_sha256": legacy.manifest.raw_sha256,
                    "start_sequence": legacy.manifest.start_sequence,
                    "next_sequence": legacy.manifest.next_sequence,
                    "path": str(Path("/data/omi") / legacy.path.relative_to(paths.root)),
                },
                "ready": {
                    "bundle_id": ready_bundles._bundle_id(captured.manifest),
                    "records_sha256": legacy.manifest.raw_sha256,
                    "start_sequence": captured.manifest.start_sequence,
                    "next_sequence": captured.manifest.next_sequence,
                    "packet_count": _packet_count(legacy),
                },
            }
        )
    source_ids = [cast(dict[str, str], item["legacy"])["source_id"] for item in mappings]
    if len(set(source_ids)) != len(source_ids):
        raise MigrationError("legacy source identities are not unique")
    return {
        "schema": "omi-two-zone-ready-migration-v1",
        "mappings": mappings,
    }


def _state(plan: Plan, before: dict[str, object]) -> dict[str, object]:
    return {
        "schema": "omi-two-zone-migration-state-v1",
        "phase": "prepared",
        "before": before,
        "generation": str(plan.generation),
        "ready": [
            {
                "source_id": _source_id(move.legacy),
                "legacy_path": str(move.legacy.path),
                "captured_path": str(move.captured.path),
                "ranges": list(move.ranges),
            }
            for move in plan.ready
        ],
        "drafts": [
            {
                "source_path": str(move.source.path),
                "start_sequence": move.start_sequence,
                "next_sequence": move.next_sequence,
            }
            for move in plan.drafts
        ],
    }


def _state_plan(paths: Paths) -> tuple[Plan, dict[str, object]]:
    state = _read_json(paths.state)
    _validate_state_header(state)
    assert isinstance(state, dict)
    before, ready_value, drafts_value = state["before"], state["ready"], state["drafts"]
    assert isinstance(before, dict) and isinstance(ready_value, list) and isinstance(drafts_value, list)
    generation = _state_path(state["generation"], paths.root, "generation")
    repairs = _repairs(cast(dict[str, object], _read_json(generation / "generation.json"))["repairs"])
    return Plan(
        generation, _state_ready(paths, generation, repairs, ready_value), _state_drafts(paths, drafts_value)
    ), before


def _validate_state_header(state: object) -> None:
    if not isinstance(state, dict) or set(state) != {"schema", "phase", "before", "generation", "ready", "drafts"}:
        raise MigrationError("migration state is invalid")
    if state["schema"] != "omi-two-zone-migration-state-v1" or state["phase"] not in {
        "prepared",
        "applying",
        "published",
    }:
        raise MigrationError("migration state is invalid")
    if (
        not isinstance(state["before"], dict)
        or not isinstance(state["ready"], list)
        or not isinstance(state["drafts"], list)
    ):
        raise MigrationError("migration state is invalid")


def _state_ready(
    paths: Paths, generation: Path, repairs: tuple[tuple[int, int, int, str], ...], ready_value: list[object]
) -> tuple[ReadyMove, ...]:
    ready: list[ReadyMove] = []
    for item in ready_value:
        if not isinstance(item, dict) or set(item) != {"source_id", "legacy_path", "captured_path", "ranges"}:
            raise MigrationError("migration state is invalid")
        source_id = item["source_id"]
        if not isinstance(source_id, str) or len(source_id) != _SHA256_HEX_LENGTH:
            raise MigrationError("migration state is invalid")
        legacy_path = _state_path(item["legacy_path"], generation, "legacy bundle")
        captured = _bundle(_state_path(item["captured_path"], paths.captured, "captured bundle"))
        legacy = _bundle(legacy_path) if legacy_path.exists() else _evidence_bundle(paths, legacy_path, source_id)
        ranges = item["ranges"]
        if not isinstance(ranges, list) or tuple(ranges) != _ranges(captured.manifest, repairs):
            raise MigrationError("migration state time ranges are invalid")
        ready.append(ReadyMove(legacy, captured, tuple(cast(dict[str, object], value) for value in ranges)))
    return tuple(ready)


def _state_drafts(paths: Paths, drafts_value: list[object]) -> tuple[DraftMove, ...]:
    drafts: list[DraftMove] = []
    for item in drafts_value:
        if not isinstance(item, dict) or set(item) != {"source_path", "start_sequence", "next_sequence"}:
            raise MigrationError("migration state is invalid")
        start, end = item["start_sequence"], item["next_sequence"]
        if any(isinstance(value, bool) or not isinstance(value, int) for value in (start, end)):
            raise MigrationError("migration state is invalid")
        source_path = _state_path(item["source_path"], paths.captured, "draft source")
        if source_path.exists():
            source = _bundle(source_path)
        else:
            source = _bundle(paths.draft / source_path.name)
            if (source.manifest.start_sequence, source.manifest.next_sequence) != (start, end):
                raise MigrationError("moved draft has an invalid range")
        drafts.append(DraftMove(source, start, end))
    return tuple(drafts)


def _state_path(value: object, root: Path, label: str) -> Path:
    if not isinstance(value, str):
        raise MigrationError("migration state is invalid")
    path = Path(value)
    _inside(path, root, label)
    return path


def _evidence_bundle(paths: Paths, legacy_path: Path, source_id: str) -> Bundle:
    evidence = paths.evidence / source_id
    _directory(evidence, "legacy evidence")
    if {item.name for item in evidence.iterdir()} != {"manifest.json", "receipt.json"}:
        raise MigrationError("legacy evidence inventory is not exact")
    try:
        manifest = BundleManifest.from_json(_read_json(evidence / "manifest.json"))
        receipt = SealedReceipt.from_json(_read_json(evidence / "receipt.json"))
    except ValueError as error:
        raise MigrationError("legacy evidence contract is invalid") from error
    if (manifest.start_sequence, manifest.next_sequence) != _bundle_name_range(legacy_path):
        raise MigrationError("legacy evidence range is invalid")
    source = Bundle(legacy_path, manifest, receipt)
    if _source_id_from_receipt(source, evidence / "receipt.json") != source_id:
        raise MigrationError("legacy evidence source identity is invalid")
    return source


def _bundle_name_range(path: Path) -> tuple[int, int]:
    try:
        start, end, _ = path.name.split("-", 2)
        return int(start), int(end)
    except ValueError as error:
        raise MigrationError("legacy bundle name is invalid") from error


def _validate_before(paths: Paths, before: dict[str, object], plan: Plan) -> None:
    checkpoint = cast(dict[str, str], before["checkpoint"])
    if _digest(Path(checkpoint["path"])) != checkpoint["sha256"]:
        raise MigrationError("checkpoint changed since frozen preflight")
    attestation = cast(dict[str, str], before["windmill_queue_attestation"])
    if _digest(Path(attestation["path"])) != attestation["sha256"]:
        raise MigrationError("Windmill queue attestation changed since frozen preflight")
    captured = cast(list[dict[str, object]], before["captured"])
    source_ids = {Path(cast(str, item["path"])): cast(str | None, item["legacy_source_id"]) for item in captured}
    moved_drafts = {move.source.path.name for move in plan.drafts if move.complete}
    expected = [
        item
        for item in captured
        if Path(cast(str, item["path"])).exists() or Path(cast(str, item["path"])).name not in moved_drafts
    ]
    current = [
        _bundle_inventory(bundle, source_ids.get(bundle.path))
        for bundle in _children(paths.captured, allow_overlap=True)
    ]
    if current != expected:
        raise MigrationError("captured inventory changed since frozen preflight")
    if list(_speech_inventory(paths.speech, paths.speech.stat().st_uid)) != before["speech"]:
        raise MigrationError("speech inventory changed since frozen preflight")


def _prepare_state(paths: Paths) -> tuple[Plan, dict[str, object], dict[str, object]]:
    _prepare_output(paths.draft)
    _prepare_output(paths.ready)
    _prepare_output(paths.ledger.parent)
    if paths.state.exists():
        plan, before = _state_plan(paths)
        if paths.log.exists():
            provenance = cast(dict[str, object], _read_json(paths.log))
        else:
            provenance = _log(paths, plan)
            _write(paths.log, _canonical(provenance))
    else:
        if paths.log.exists() or paths.inventory.exists() or paths.ledger.exists():
            raise MigrationError("migration control files are incomplete")
        plan = build_plan(paths)
        evidence = _legacy_evidence(paths, plan)
        generation_evidence = _generation_evidence(paths, plan)
        _set_work_evidence_ownership(paths)
        before = _frozen_before(paths, plan, evidence, generation_evidence)
        provenance = _log(paths, plan)
        _write(paths.state, _canonical(_state(plan, before)))
        _write(paths.log, _canonical(provenance))
    _validate_before(paths, before, plan)
    _validate_preflight(paths, plan, provenance)
    return plan, before, provenance


def _validate_preflight(paths: Paths, plan: Plan, provenance: dict[str, object]) -> None:
    _validate_provenance(plan, provenance)
    _validate_ready_destinations(paths, plan)
    _validate_draft_destinations(paths, plan)
    _validate_ledger_destinations(paths, plan)


def _validate_provenance(plan: Plan, provenance: dict[str, object]) -> None:
    if provenance.get("schema") != "omi-two-zone-ready-migration-v1":
        raise MigrationError("migration provenance is invalid")
    mappings = provenance.get("mappings")
    if not isinstance(mappings, list) or len(mappings) != len(plan.ready):
        raise MigrationError("migration provenance is invalid")


def _validate_ready_destinations(paths: Paths, plan: Plan) -> None:
    ready_ids = [ready_bundles._bundle_id(move.captured.manifest) for move in plan.ready]
    if len(set(ready_ids)) != len(ready_ids):
        raise MigrationError("ready destinations are not unique")
    if {path.name for path in paths.ready.iterdir()} - set(ready_ids):
        raise MigrationError("ready root contains a non-migration output")
    for move, bundle_id in zip(plan.ready, ready_ids, strict=True):
        destination = paths.ready / bundle_id
        if destination.exists():
            try:
                ready_bundles._validate_ready(destination, move.captured.manifest, move.ranges)
            except ready_bundles.ReadyBundleError as error:
                if not _moved_legacy(destination, move.legacy):
                    raise MigrationError("ready destination conflicts with frozen plan") from error
        elif not move.legacy.path.exists():
            raise MigrationError("legacy source vanished before ready move")


def _cropped_draft_digest(move: DraftMove) -> str:
    digest = sha256()
    offset = (move.start_sequence - move.source.manifest.start_sequence) * RECORD_SIZE
    remaining = (move.next_sequence - move.start_sequence) * RECORD_SIZE
    with (move.source.path / "records.bin").open("rb") as stream:
        stream.seek(offset)
        while remaining:
            chunk = stream.read(min(1024 * 1024, remaining))
            if not chunk:
                raise MigrationError("captured records ended while validating cropped draft")
            digest.update(chunk)
            remaining -= len(chunk)
    return digest.hexdigest()


def _draft_destination(paths: Paths, move: DraftMove) -> Path:
    if move.complete:
        return paths.draft / move.source.path.name
    return paths.draft / f"{move.start_sequence}-{move.next_sequence}-{_cropped_draft_digest(move)[:16]}"


def _validate_draft_destinations(paths: Paths, plan: Plan) -> None:
    destinations = tuple(_draft_destination(paths, move) for move in plan.drafts)
    if len(set(destinations)) != len(destinations):
        raise MigrationError("draft destinations are not unique")
    if {path.name for path in paths.draft.iterdir()} - {path.name for path in destinations}:
        raise MigrationError("draft root contains a non-migration output")
    for move, destination in zip(plan.drafts, destinations, strict=True):
        if not destination.exists():
            continue
        existing = _bundle(destination)
        raw_sha256 = move.source.manifest.raw_sha256 if move.complete else _cropped_draft_digest(move)
        if (
            existing.manifest.start_sequence != move.start_sequence
            or existing.manifest.next_sequence != move.next_sequence
            or existing.manifest.raw_sha256 != raw_sha256
        ):
            raise MigrationError("draft destination conflicts with frozen plan")


def _validate_ledger_destinations(paths: Paths, plan: Plan) -> None:
    ready_ids = [ready_bundles._bundle_id(move.captured.manifest) for move in plan.ready]
    ledger = ready_bundles._read_ledger(paths.ledger)
    if paths.ledger.exists():
        for _move, bundle_id in zip(plan.ready, ready_ids, strict=True):
            if bundle_id in cast(dict[str, object], ledger["bundles"]) and not (paths.ready / bundle_id).exists():
                raise MigrationError("producer ledger leads ready storage")


def _record_state_phase(paths: Paths, phase: str) -> None:
    state = cast(dict[str, object], _read_json(paths.state))
    if state.get("phase") not in {"prepared", "applying", "published", "retiring", "audio_retired"}:
        raise MigrationError("migration state is invalid")
    state["phase"] = phase
    _write(paths.state, _canonical(state))


def _move_ready(paths: Paths, move: ReadyMove, ledger: dict[str, object]) -> Path:
    source = move.captured.manifest
    destination = paths.ready / ready_bundles._bundle_id(source)
    if destination.exists():
        try:
            validated = ready_bundles._validate_ready(destination, source, move.ranges)
        except ready_bundles.ReadyBundleError as error:
            if not _moved_legacy(destination, move.legacy):
                raise MigrationError("ready destination conflicts with frozen plan") from error
            validated = _finish_ready(destination, source, move.ranges)
    else:
        if not move.legacy.path.exists():
            raise MigrationError("legacy source vanished before ready move")
        _rename(move.legacy.path, destination)
        validated = _finish_ready(destination, source, move.ranges)
    ready_bundles._record_ready(paths.ledger, ledger, validated, source)
    return validated.path


def _moved_legacy(destination: Path, legacy: Bundle) -> bool:
    entries = {item.name for item in destination.iterdir()}
    if entries not in ({"manifest.json", "records.bin"}, {"manifest.json", "receipt.json", "records.bin"}):
        return False
    try:
        manifest = BundleManifest.from_json(_read_json(destination / "manifest.json"))
    except ValueError:
        return False
    if manifest != legacy.manifest or _digest(destination / "records.bin") != legacy.manifest.raw_sha256:
        return False
    receipt = destination / "receipt.json"
    if not receipt.exists():
        return True
    try:
        return SealedReceipt.from_json(_read_json(receipt)) == legacy.receipt
    except ValueError:
        return False


def _finish_ready(
    destination: Path, source: BundleManifest, ranges: tuple[dict[str, object], ...]
) -> ready_bundles.ReadyBundleResult:
    (destination / "receipt.json").unlink(missing_ok=True)
    digest = _digest(destination / "records.bin")
    _write(
        destination / "manifest.json",
        ready_bundles._canonical(ready_bundles._ready_manifest(source, digest, ranges)),
    )
    return ready_bundles._read_ready_source(destination).result


def _move_draft(paths: Paths, move: DraftMove) -> Path:
    destination = _draft_destination(paths, move)
    if destination.exists():
        existing = _bundle(destination)
        raw_sha256 = move.source.manifest.raw_sha256 if move.complete else _cropped_draft_digest(move)
        if (
            existing.manifest.start_sequence != move.start_sequence
            or existing.manifest.next_sequence != move.next_sequence
            or existing.manifest.raw_sha256 != raw_sha256
        ):
            raise MigrationError("draft destination conflicts with frozen plan")
        return destination
    if move.complete:
        if not move.source.path.exists():
            raise MigrationError("draft source vanished before move")
        _rename(move.source.path, destination)
        return destination
    _copy_slice(move, paths.draft)
    return destination


def _require_root() -> None:
    if os.geteuid() != 0:
        raise MigrationError("migration execute and finalize require root")


def execute(paths: Paths) -> Plan:
    _require_root()
    _quiescent(paths)
    with _held_producer_locks(paths):
        _inactive("omi-collector.service")
        _inactive("omi-speech-archive-jit.service", disabled=True, optional=True)
        _windmill_quiescent()
        for root in (paths.draft, paths.ready, paths.evidence, paths.ledger.parent):
            if root.exists():
                _cleanup_temporary(root, os.geteuid())
        plan, before, provenance = _prepare_state(paths)
        devices = {
            path.stat().st_dev
            for path in (plan.generation, paths.captured, paths.draft, paths.ready, paths.ledger.parent)
        }
        if len(devices) != 1:
            raise MigrationError("migration roots are not on one filesystem")
        if not paths.ledger.exists():
            _write(paths.ledger, _canonical({"bundles": {}, "frontier": 0}))
        _record_state_phase(paths, "applying")
        ledger = ready_bundles._read_ledger(paths.ledger)
        ready = tuple(_move_ready(paths, move, ledger) for move in plan.ready)
        drafts = tuple(_move_draft(paths, move) for move in plan.drafts)
        inventory = _frozen_inventory(paths, before, provenance)
        _write(paths.inventory, _canonical(inventory))
        _record_state_phase(paths, "published")
        _set_runtime_outputs(paths, ready, drafts)
    return plan


def _retirement_outputs(paths: Paths) -> tuple[tuple[Path, int, int], ...]:
    ready = tuple(
        (source.result.path, source.result.next_sequence - source.result.record_count, source.result.next_sequence)
        for source in (ready_bundles._read_ready_source(path) for path in paths.ready.iterdir())
    )
    drafts = tuple(
        (bundle.path, bundle.manifest.start_sequence, bundle.manifest.next_sequence)
        for bundle in _children(paths.draft)
    )
    return ready + drafts


def _same_audio_records(source: Bundle, target: Path, target_start: int) -> bool:
    source_records = source.path / "records.bin"
    target_records = target / "records.bin"
    with source_records.open("rb") as left, target_records.open("rb") as right:
        right.seek((source.manifest.start_sequence - target_start) * RECORD_SIZE)
        for _ in range(source.manifest.record_count):
            if left.read(RECORD_SIZE)[TIMESTAMP_SIZE:] != right.read(RECORD_SIZE)[TIMESTAMP_SIZE:]:
                return False
    return True


def _retire_captured(paths: Paths) -> None:
    outputs = _retirement_outputs(paths)
    for source in _children(paths.captured, allow_overlap=True):
        matches = [
            (path, start)
            for path, start, end in outputs
            if start <= source.manifest.start_sequence and source.manifest.next_sequence <= end
        ]
        if len(matches) != 1 or not _same_audio_records(source, matches[0][0], matches[0][1]):
            raise MigrationError("captured bundle is not superseded by verified output")
        shutil.rmtree(source.path)
        ready_bundles._sync_directory(paths.captured)


def _retire_generation(paths: Paths, inventory: dict[str, object]) -> None:
    generation = Path(cast(dict[str, str], inventory["legacy_generation"])["path"])
    _inside(generation, paths.root / "source" / ".generations", "legacy generation")
    if paths.current.is_symlink():
        if paths.current.resolve(strict=True) != generation:
            raise MigrationError("legacy current changed before retirement")
        paths.current.unlink()
        ready_bundles._sync_directory(paths.current.parent)
    elif paths.current.exists():
        raise MigrationError("legacy current changed before retirement")
    if generation.exists():
        if {item.name for item in generation.iterdir()} != {"generation.json"}:
            raise MigrationError("legacy generation still contains audio")
        (generation / "generation.json").unlink()
        generation.rmdir()
        ready_bundles._sync_directory(generation.parent)


def _retirement_acknowledged(paths: Paths, inventory: dict[str, object]) -> None:
    if paths.checkpoint is None:
        raise MigrationError("--checkpoint is required for finalize")
    try:
        acknowledged = set(ready_bundles._windmill_acknowledged(paths.checkpoint))
    except ready_bundles.ReadyBundleError as error:
        raise MigrationError("Windmill checkpoint is not a no-clobber acknowledgement") from error
    expected = _historic_ack_identities(inventory, paths.log)
    if set(acknowledged) != expected:
        raise MigrationError("Windmill checkpoint is not the imported historic acknowledgement")


def _historic_ack_identities(inventory: dict[str, object], provenance_path: Path) -> set[tuple[str, str]]:
    checkpoint = cast(dict[str, str], inventory["checkpoint"])
    if _digest(Path(checkpoint["path"])) != checkpoint["sha256"]:
        raise MigrationError("legacy checkpoint evidence changed")
    value = _read_json(Path(checkpoint["path"]))
    if (
        not isinstance(value, dict)
        or not isinstance(value.get("queue"), list)
        or not isinstance(value.get("frontier"), dict)
    ):
        raise MigrationError("legacy checkpoint evidence is invalid")
    source_ids = []
    for item in value["queue"]:
        if not isinstance(item, dict) or not isinstance(item.get("identity"), str):
            raise MigrationError("legacy checkpoint queue is invalid")
        source_ids.append(item["identity"])
    frontier = value["frontier"].get("source_id")
    if not isinstance(frontier, str):
        raise MigrationError("legacy checkpoint frontier is invalid")
    source_ids.append(frontier)
    if len(source_ids) != _HISTORIC_ACK_COUNT or len(set(source_ids)) != _HISTORIC_ACK_COUNT:
        raise MigrationError("legacy checkpoint does not contain the historic acknowledgement set")
    provenance = _read_json(provenance_path)
    if not isinstance(provenance, dict) or not isinstance(provenance.get("mappings"), list):
        raise MigrationError("migration provenance is invalid")
    mapped = {
        cast(dict[str, str], item["legacy"])["source_id"]: (
            cast(dict[str, str], item["ready"])["bundle_id"],
            cast(dict[str, str], item["ready"])["records_sha256"],
        )
        for item in provenance["mappings"]
        if isinstance(item, dict) and isinstance(item.get("legacy"), dict) and isinstance(item.get("ready"), dict)
    }
    if set(source_ids) - set(mapped):
        raise MigrationError("historic checkpoint source is absent from provenance")
    return {mapped[source_id] for source_id in source_ids}


def _retirement_inventory(paths: Paths, phase: str) -> dict[str, object]:
    inventory = cast(dict[str, object], _read_json(paths.inventory))
    expected = {
        "schema",
        "checkpoint",
        "windmill_queue_attestation",
        "legacy_generation",
        "captured",
        "draft",
        "ready",
        "speech",
        "mapping_sha256",
        "legacy_evidence",
    }
    if set(inventory) != expected or inventory["schema"] != "omi-two-zone-frozen-inventory-v1":
        raise MigrationError("frozen inventory is invalid")
    provenance = _read_json(paths.log)
    if sha256(_canonical(provenance)).hexdigest() != inventory["mapping_sha256"]:
        raise MigrationError("frozen inventory mapping changed")
    if _ready_inventory(paths.ready) != inventory["ready"]:
        raise MigrationError("ready inventory changed before retirement")
    if [_bundle_inventory(bundle) for bundle in _children(paths.draft)] != inventory["draft"]:
        raise MigrationError("draft inventory changed before retirement")
    speech = cast(list[dict[str, str]], inventory["speech"])
    if list(_speech_inventory(paths.speech, paths.speech.stat().st_uid)) != speech:
        raise MigrationError("speech inventory changed before retirement")
    _validate_retained_evidence(inventory)
    if phase == "published":
        _validate_retirement_captures(paths, cast(list[dict[str, object]], inventory["captured"]))
    return inventory


def _validate_retained_evidence(inventory: dict[str, object]) -> None:
    generation = cast(dict[str, str], inventory["legacy_generation"])
    evidence = Path(generation["evidence_path"])
    if (
        _digest(evidence) != generation["generation_sha256"]
        or generation["evidence_sha256"] != generation["generation_sha256"]
    ):
        raise MigrationError("generation evidence changed before retirement")
    for item in cast(list[dict[str, str]], inventory["legacy_evidence"]):
        path = Path(item["path"])
        if (
            _digest(path / "manifest.json") != item["manifest_sha256"]
            or _digest(path / "receipt.json") != item["receipt_sha256"]
        ):
            raise MigrationError("legacy evidence changed before retirement")


def _validate_retirement_captures(paths: Paths, frozen: list[dict[str, object]]) -> None:
    source_ids = {Path(cast(str, item["path"])): cast(str | None, item["legacy_source_id"]) for item in frozen}
    expected = [
        item
        for item in frozen
        if Path(cast(str, item["path"])).exists() or not (paths.draft / Path(cast(str, item["path"])).name).is_dir()
    ]
    current = [
        _bundle_inventory(bundle, source_ids.get(bundle.path))
        for bundle in _children(paths.captured, allow_overlap=True)
    ]
    if current != expected:
        raise MigrationError("captured inventory changed before retirement")


def _retire_evidence(paths: Paths) -> None:
    if paths.evidence.exists():
        _assert_owned(paths.evidence, paths.evidence.stat().st_uid)
        shutil.rmtree(paths.evidence)
    if paths.generation_evidence.exists():
        _owned_regular(paths.generation_evidence, paths.generation_evidence.stat().st_uid, "generation evidence")
        paths.generation_evidence.unlink()
    ready_bundles._sync_directory(paths.evidence.parent)


def _retirement_phase(paths: Paths) -> str:
    state = _read_json(paths.state)
    if not isinstance(state, dict) or state.get("schema") != "omi-two-zone-migration-state-v1":
        raise MigrationError("migration state is invalid")
    phase = state.get("phase")
    if phase not in {"published", "retiring", "audio_retired", "retired"}:
        raise MigrationError("migration has not published ready output")
    return phase


def _remove_retirement_controls(paths: Paths) -> None:
    paths.log.unlink(missing_ok=True)
    paths.inventory.unlink(missing_ok=True)
    ready_bundles._sync_directory(paths.log.parent)
    paths.state.unlink(missing_ok=True)
    ready_bundles._sync_directory(paths.state.parent)


def finalize(paths: Paths) -> None:
    _require_root()
    _quiescent(paths)
    with _held_producer_locks(paths):
        _inactive("omi-collector.service")
        _inactive("omi-speech-archive-jit.service", disabled=True, optional=True)
        _windmill_quiescent()
        phase = _retirement_phase(paths)
        if phase == "retired":
            _remove_retirement_controls(paths)
            return
        if phase == "audio_retired":
            _retire_evidence(paths)
            _record_state_phase(paths, "retired")
            _remove_retirement_controls(paths)
            return
        inventory = _retirement_inventory(paths, phase)
        _retirement_acknowledged(paths, inventory)
        _record_state_phase(paths, "retiring")
        _retire_captured(paths)
        _retire_generation(paths, inventory)
        _record_state_phase(paths, "audio_retired")
        _retire_evidence(paths)
        _record_state_phase(paths, "retired")
        _remove_retirement_controls(paths)


def _paths(args: argparse.Namespace) -> Paths:
    root = cast(Path, args.root).resolve(strict=True)
    return Paths(
        root,
        cast(Path, args.current),
        root / "captured",
        root / "draft",
        root / "ready",
        root / "collector" / "ready-publications.json",
        root / "collector" / "two-zone-migration.json",
        root / "collector" / "two-zone-frozen-inventory.json",
        root / "collector" / "two-zone-migration-state.json",
        root / "work" / "two-zone-legacy-evidence",
        root / "work" / "two-zone-generation.json",
        cast(Path | None, args.checkpoint),
        cast(Path | None, args.windmill_queue_attestation),
        root / "speech",
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path("/srv/pipelines/omi"))
    parser.add_argument("--current", type=Path, default=Path("/srv/pipelines/omi/source/current"))
    parser.add_argument("--checkpoint", type=Path)
    parser.add_argument("--windmill-queue-attestation", type=Path)
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--finalize", action="store_true")
    args = parser.parse_args(argv)
    execute_requested = cast(bool, args.execute)
    finalize_requested = cast(bool, args.finalize)
    try:
        if execute_requested and finalize_requested:
            raise MigrationError("--execute and --finalize are mutually exclusive")
        paths = _paths(args)
        if finalize_requested:
            finalize(paths)
            plan = Plan(Path(), (), ())
        else:
            plan = execute(paths) if execute_requested else build_plan(paths)
        sys.stdout.write(
            _canonical(
                {
                    "mode": "finalize" if finalize_requested else "execute" if execute_requested else "dry-run",
                    "ready_bundles": len(plan.ready),
                    "draft_bundles": len(plan.drafts),
                    "snapshot": "retired"
                    if finalize_requested
                    else "frozen"
                    if execute_requested
                    else "live snapshot; not final",
                }
            ).decode()
            + "\n"
        )
        return 0
    except MigrationError as error:
        sys.stderr.write(f"migration refused: {error}\n")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
