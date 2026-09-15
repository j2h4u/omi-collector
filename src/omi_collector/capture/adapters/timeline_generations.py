"""Build and atomically expose immutable generations of normalized audio."""

from __future__ import annotations

import json
import os
import re
import shutil
from dataclasses import asdict, dataclass
from hashlib import sha256
from pathlib import Path
from typing import cast
from uuid import uuid4

from ..domain.ring_protocol import RECORD_SIZE
from .bundle_contract import BundleManifest, SealedReceipt


class TimelineGenerationError(RuntimeError):
    """A complete, monotonic audio generation could not be proven."""


@dataclass(frozen=True, slots=True)
class TimeRepair:
    start_sequence: int
    next_sequence: int
    offset_seconds: int
    evidence: str

    def __post_init__(self) -> None:
        if self.start_sequence < 0 or self.next_sequence <= self.start_sequence:
            raise ValueError("time repair sequence range is invalid")
        if not self.offset_seconds or not self.evidence:
            raise ValueError("time repair requires an offset and evidence")


@dataclass(frozen=True, slots=True)
class GenerationResult:
    path: Path
    bundle_count: int
    record_count: int
    generation_id: str


def publish_from_ledger(
    captured_root: Path, publication_root: Path, collector_root: Path, device_slug: str
) -> GenerationResult:
    """Publish from the durable repair ledger only when clock evidence is settled."""
    _validate_device_slug(device_slug)
    repairs = _read_repairs(collector_root / "timeline-repairs.json", device_slug)
    _require_settled_clock_operations(collector_root / "clock-corrections" / device_slug)
    return build_generation(captured_root, publication_root, device_slug, repairs)


def build_generation(
    captured_root: Path,
    publication_root: Path,
    device_slug: str,
    repairs: tuple[TimeRepair, ...],
) -> GenerationResult:
    """Rebuild every bundle, validate the chain, and switch one symlink."""
    _validate_device_slug(device_slug)
    bundles = _bundles(captured_root / device_slug)
    _validate_repairs(repairs)
    identity = _generation_identity(bundles, repairs)
    generations = publication_root / ".generations" / device_slug
    generations.mkdir(mode=0o750, parents=True, exist_ok=True)
    destination = generations / identity
    if not destination.exists():
        temporary = generations / f".{identity}.{uuid4().hex}.tmp"
        temporary.mkdir(mode=0o750)
        try:
            records, _ = _write_bundles(temporary, bundles, repairs)
            _write_generation_manifest(temporary, identity, bundles, repairs, records)
            _sync_tree(temporary)
            temporary.rename(destination)
            _sync_directory(generations)
        except BaseException:
            shutil.rmtree(temporary, ignore_errors=True)
            raise
    else:
        records = _append_generation(destination, identity, bundles, repairs)
    _switch_current(publication_root, device_slug, destination)
    return GenerationResult(destination, len(bundles), records, identity)


def _validate_device_slug(device_slug: str) -> None:
    if re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]*", device_slug) is None:
        raise TimelineGenerationError("device slug is unsafe")


def _require_settled_clock_operations(root: Path) -> None:
    if not root.exists():
        return
    for path in root.glob("*.json"):
        try:
            value = cast(object, json.loads(path.read_text(encoding="utf-8")))
        except (OSError, json.JSONDecodeError) as error:
            raise TimelineGenerationError("clock correction evidence is invalid") from error
        if not isinstance(value, dict) or value.get("state") not in {
            "prepared",
            "not_written",
            "not_applied",
            "resolved",
        }:
            raise TimelineGenerationError("clock correction evidence is unresolved")


def _read_repairs(path: Path, device_slug: str) -> tuple[TimeRepair, ...]:
    if not path.exists():
        return ()
    try:
        value = cast(object, json.loads(path.read_text(encoding="utf-8")))
        if not isinstance(value, dict) or set(value) != {"version", "devices"} or value["version"] != 1:
            raise ValueError
        devices = value["devices"]
        if not isinstance(devices, dict):
            raise ValueError
        rows = devices.get(device_slug, [])
        if not isinstance(rows, list):
            raise ValueError
        repairs = tuple(_repair(cast(dict[str, object], row)) for row in rows if isinstance(row, dict))
        if len(repairs) != len(rows):
            raise ValueError
        return repairs
    except (OSError, TypeError, ValueError, json.JSONDecodeError) as error:
        raise TimelineGenerationError("timeline repair ledger is invalid") from error


def _repair(value: dict[str, object]) -> TimeRepair:
    if set(value) != {"start_sequence", "next_sequence", "offset_seconds", "evidence"}:
        raise ValueError
    numbers = tuple(value[key] for key in ("start_sequence", "next_sequence", "offset_seconds"))
    evidence = value["evidence"]
    if any(isinstance(item, bool) or not isinstance(item, int) for item in numbers) or not isinstance(evidence, str):
        raise ValueError
    return TimeRepair(cast(int, numbers[0]), cast(int, numbers[1]), cast(int, numbers[2]), evidence)


def _append_generation(
    destination: Path,
    identity: str,
    captured: tuple[tuple[Path, BundleManifest], ...],
    repairs: tuple[TimeRepair, ...],
) -> int:
    existing, records, previous = _validate_existing_generation(destination, identity)
    if len(existing) > len(captured):
        raise TimelineGenerationError("existing generation is ahead of captured source")
    validation_previous: int | None = None
    for index, (output_path, output_manifest) in enumerate(existing):
        source_path, source_manifest = captured[index]
        if (output_manifest.start_sequence, output_manifest.next_sequence) != (
            source_manifest.start_sequence,
            source_manifest.next_sequence,
        ):
            raise TimelineGenerationError("existing generation source order conflicts")
        raw = (source_path / "records.bin").read_bytes()
        normalized, validation_previous = _normalize(raw, source_manifest.start_sequence, repairs, validation_previous)
        if sha256(normalized).hexdigest() != output_manifest.raw_sha256:
            raise TimelineGenerationError("existing generation normalization conflicts")
        if sha256((output_path / "records.bin").read_bytes()).hexdigest() != output_manifest.raw_sha256:
            raise TimelineGenerationError("existing generation bundle is invalid")
    remaining = captured[len(existing) :]
    if remaining:
        added, _ = _write_bundles(destination, remaining, repairs, previous)
        records += added
    _replace_generation_manifest(destination, identity, captured, repairs, records)
    return records


def _validate_existing_generation(
    destination: Path, identity: str
) -> tuple[tuple[tuple[Path, BundleManifest], ...], int, int]:
    try:
        value = cast(object, json.loads((destination / "generation.json").read_text(encoding="utf-8")))
    except (OSError, json.JSONDecodeError) as error:
        raise TimelineGenerationError("existing generation manifest is invalid") from error
    if not isinstance(value, dict) or value.get("generation_id") != identity:
        raise TimelineGenerationError("existing generation identity conflicts")
    bundles = _bundles(destination)
    previous: int | None = None
    records = 0
    for source, manifest in bundles:
        raw = (source / "records.bin").read_bytes()
        if sha256(raw).hexdigest() != manifest.raw_sha256 or len(raw) != manifest.record_count * RECORD_SIZE:
            raise TimelineGenerationError("existing generation bundle is invalid")
        _, previous = _normalize(raw, manifest.start_sequence, (), previous)
        records += manifest.record_count
    assert previous is not None
    return bundles, records, previous


def _replace_generation_manifest(
    root: Path,
    identity: str,
    bundles: tuple[tuple[Path, BundleManifest], ...],
    repairs: tuple[TimeRepair, ...],
    records: int,
) -> None:
    temporary = root / f".generation.{uuid4().hex}.tmp"
    value = _generation_manifest(identity, bundles, repairs, records)
    _write(temporary, _json(value))
    temporary.replace(root / "generation.json")
    _sync_directory(root)


def _bundles(root: Path) -> tuple[tuple[Path, BundleManifest], ...]:
    found: list[tuple[Path, BundleManifest]] = []
    for path in root.iterdir():
        if path.name.startswith(".") or path.is_symlink() or not path.is_dir():
            continue
        try:
            manifest = BundleManifest.from_json(cast(object, json.loads((path / "manifest.json").read_text())))
        except (OSError, ValueError, json.JSONDecodeError) as error:
            raise TimelineGenerationError("captured bundle manifest is invalid") from error
        found.append((path, manifest))
    found.sort(key=lambda item: item[1].start_sequence)
    previous = None
    for _, manifest in found:
        if previous is not None and manifest.start_sequence < previous:
            raise TimelineGenerationError("captured sequence chain overlaps")
        previous = manifest.next_sequence
    if not found:
        raise TimelineGenerationError("captured source is empty")
    return tuple(found)


def _validate_repairs(repairs: tuple[TimeRepair, ...]) -> None:
    previous = -1
    for repair in sorted(repairs, key=lambda item: item.start_sequence):
        if repair.start_sequence < previous:
            raise TimelineGenerationError("time repairs overlap")
        previous = repair.next_sequence


def _generation_identity(bundles: tuple[tuple[Path, BundleManifest], ...], repairs: tuple[TimeRepair, ...]) -> str:
    del bundles
    evidence = {
        "algorithm": 2,
        "repairs": [asdict(repair) for repair in repairs],
    }
    return sha256(json.dumps(evidence, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def _write_bundles(
    destination: Path,
    bundles: tuple[tuple[Path, BundleManifest], ...],
    repairs: tuple[TimeRepair, ...],
    previous_timestamp: int | None = None,
) -> tuple[int, int]:
    total = 0
    for source, manifest in bundles:
        raw = (source / "records.bin").read_bytes()
        if sha256(raw).hexdigest() != manifest.raw_sha256 or len(raw) != manifest.record_count * RECORD_SIZE:
            raise TimelineGenerationError("captured bundle does not match its manifest")
        normalized, previous_timestamp = _normalize(raw, manifest.start_sequence, repairs, previous_timestamp)
        digest = sha256(normalized).hexdigest()
        target = destination / f"{manifest.start_sequence}-{manifest.next_sequence}-{digest[:16]}"
        temporary = destination / f".{target.name}.{uuid4().hex}.tmp"
        temporary.mkdir(mode=0o750)
        output_manifest = BundleManifest(
            manifest.device_slug,
            manifest.start_sequence,
            manifest.next_sequence,
            manifest.record_count,
            RECORD_SIZE,
            digest,
        )
        receipt = SealedReceipt.from_json(cast(object, json.loads((source / "receipt.json").read_text())))
        _write(temporary / "records.bin", normalized)
        _write(temporary / "manifest.json", _json(output_manifest.as_dict()))
        _write(temporary / "receipt.json", _json(SealedReceipt(receipt.attempt_id, digest).as_dict()))
        _sync_directory(temporary)
        temporary.rename(target)
        _sync_directory(destination)
        total += manifest.record_count
    assert previous_timestamp is not None
    return total, previous_timestamp


def _normalize(
    raw: bytes,
    start_sequence: int,
    repairs: tuple[TimeRepair, ...],
    previous_timestamp: int | None,
) -> tuple[bytes, int]:
    output = bytearray(raw)
    for index in range(len(raw) // RECORD_SIZE):
        sequence = start_sequence + index
        offset = next(
            (repair.offset_seconds for repair in repairs if repair.start_sequence <= sequence < repair.next_sequence), 0
        )
        position = index * RECORD_SIZE
        timestamp = int.from_bytes(raw[position : position + 4], "big") - offset
        if not 0 <= timestamp <= 2**32 - 1:
            raise TimelineGenerationError("normalized timestamp is outside uint32")
        if previous_timestamp is not None and timestamp < previous_timestamp:
            raise TimelineGenerationError("normalized timestamp chain regresses")
        output[position : position + 4] = timestamp.to_bytes(4, "big")
        previous_timestamp = timestamp
    assert previous_timestamp is not None
    return bytes(output), previous_timestamp


def _write_generation_manifest(
    root: Path,
    identity: str,
    bundles: tuple[tuple[Path, BundleManifest], ...],
    repairs: tuple[TimeRepair, ...],
    records: int,
) -> None:
    _write(root / "generation.json", _json(_generation_manifest(identity, bundles, repairs, records)))


def _generation_manifest(
    identity: str,
    bundles: tuple[tuple[Path, BundleManifest], ...],
    repairs: tuple[TimeRepair, ...],
    records: int,
) -> dict[str, object]:
    return {
        "algorithm": 2,
        "generation_id": identity,
        "record_count": records,
        "source_hashes": [manifest.raw_sha256 for _, manifest in bundles],
        "repairs": [asdict(repair) for repair in repairs],
    }


def _switch_current(publication_root: Path, device_slug: str, destination: Path) -> None:
    publication_root.mkdir(mode=0o750, parents=True, exist_ok=True)
    current = publication_root / device_slug
    if current.exists() and not current.is_symlink():
        raise TimelineGenerationError("legacy publication must be migrated before generation switch")
    temporary = publication_root / f".{device_slug}.{uuid4().hex}.tmp"
    relative = destination.relative_to(publication_root)
    temporary.symlink_to(relative, target_is_directory=True)
    temporary.replace(current)
    _sync_directory(publication_root)


def _json(value: object) -> bytes:
    return (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode()


def _write(path: Path, payload: bytes) -> None:
    with path.open("xb") as stream:
        stream.write(payload)
        stream.flush()
        os.fsync(stream.fileno())


def _sync_tree(root: Path) -> None:
    for path in root.iterdir():
        if path.is_dir():
            _sync_directory(path)
    _sync_directory(root)


def _sync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
