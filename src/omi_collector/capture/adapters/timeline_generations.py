"""Build and atomically expose immutable generations of normalized audio."""

from __future__ import annotations

import grp
import json
import os
import pwd
import shutil
from contextlib import suppress
from dataclasses import asdict, dataclass
from hashlib import sha256
from pathlib import Path
from stat import S_ISDIR, S_ISLNK, S_ISREG
from typing import cast
from uuid import uuid4

from ..domain.ring_protocol import RECORD_SIZE
from .bundle_contract import BundleManifest, SealedReceipt
from .clock_corrections import ClockCorrection, ClockCorrectionError, ClockCorrectionStore


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
        if not self.evidence:
            raise ValueError("time repair requires evidence")


@dataclass(frozen=True, slots=True)
class GenerationResult:
    path: Path
    bundle_count: int
    record_count: int
    generation_id: str


@dataclass(frozen=True, slots=True)
class _GenerationBuild:
    temporary: Path
    destination: Path
    generations_descriptor: int
    bundles: tuple[tuple[Path, BundleManifest], ...]
    repairs: tuple[TimeRepair, ...]
    identity: str
    service_uid: int
    service_gid: int
    max_sequence: int | None


_PUBLICATION_DIRECTORY_MODE = 0o750
_PUBLICATION_FILE_MODE = 0o640
_SERVICE_ACCOUNT = "omi-collector"
_SERVICE_GROUP = "omi-collector"
_GENERATION_LINK_PARTS = 2
_SHA256_HEX_LENGTH = 64
_LOWERCASE_HEX = frozenset("0123456789abcdef")


def publish_from_ledger(captured_root: Path, publication_root: Path, collector_root: Path) -> GenerationResult:
    """Materialize accepted clock evidence, then publish its normalized timeline."""
    ledger = collector_root / "timeline-repairs.json"
    repairs = _read_repairs(ledger)
    repairs, safe_prefix = _materialize_clock_repairs(
        collector_root / "clock-corrections", captured_root, ledger, repairs
    )
    return build_generation(captured_root, publication_root, repairs, max_sequence=safe_prefix)


def build_generation(
    captured_root: Path,
    publication_root: Path,
    repairs: tuple[TimeRepair, ...],
    *,
    max_sequence: int | None = None,
) -> GenerationResult:
    """Rebuild every bundle, validate the chain, and switch one symlink."""
    bundles = _bundles(captured_root, max_sequence=max_sequence)
    _validate_repairs(repairs)
    identity = _generation_identity(bundles, repairs, max_sequence)
    generations = publication_root / ".generations"
    service_uid, service_gid = _publication_identity()
    _prepare_generation_directory(publication_root, service_uid, service_gid)
    try:
        publication_descriptor = _open_directory(publication_root)
    except OSError as error:
        raise TimelineGenerationError("publication directory is not service-writable") from error
    try:
        generations_view = _descriptor_path(publication_descriptor) / ".generations"
        _prepare_generation_directory(generations_view, service_uid, service_gid)
        try:
            generations_descriptor = _open_directory(generations_view)
        except OSError as error:
            raise TimelineGenerationError("publication generation directory is not service-writable") from error
        try:
            generation_id = _current_generation_id(publication_descriptor, identity) or identity
            destination = generations / generation_id
            destination_view = _descriptor_path(generations_descriptor) / generation_id
            if destination_view.exists():
                _prepare_generation_directory(destination_view, service_uid, service_gid)
                if not _existing_sources_are_prefix(destination_view, generation_id, bundles, max_sequence):
                    generation_id = _replacement_generation_id(identity, bundles, max_sequence)
                    destination = generations / generation_id
                    destination_view = _descriptor_path(generations_descriptor) / generation_id
            temporary_view = _descriptor_path(generations_descriptor) / f".{generation_id}.{uuid4().hex}.tmp"
            if not destination_view.exists():
                records = _create_generation(
                    _GenerationBuild(
                        temporary_view,
                        destination_view,
                        generations_descriptor,
                        bundles,
                        repairs,
                        generation_id,
                        service_uid,
                        service_gid,
                        max_sequence,
                    )
                )
            else:
                _prepare_generation_directory(destination_view, service_uid, service_gid)
                records = _append_generation(
                    destination_view,
                    generation_id,
                    bundles,
                    repairs,
                    service_uid,
                    service_gid,
                    max_sequence=max_sequence,
                )
            try:
                _assert_directory_identity(generations, generations_descriptor)
                _assert_directory_identity(publication_root, publication_descriptor)
            except OSError as error:
                raise TimelineGenerationError("publication directory was replaced") from error
            _switch_current(publication_root, destination, publication_descriptor=publication_descriptor)
            return GenerationResult(destination, len(bundles), records, generation_id)
        finally:
            os.close(generations_descriptor)
    finally:
        os.close(publication_descriptor)


def _create_generation(
    build: _GenerationBuild,
) -> int:
    _prepare_generation_directory(build.temporary, build.service_uid, build.service_gid)
    try:
        records, _ = _write_bundles(
            build.temporary,
            build.bundles,
            build.repairs,
            service_uid=build.service_uid,
            service_gid=build.service_gid,
            max_sequence=build.max_sequence,
        )
        _write_generation_manifest(
            build.temporary,
            build.identity,
            build.bundles,
            build.repairs,
            records,
            build.service_uid,
            build.service_gid,
        )
        _sync_tree(build.temporary)
        temporary_descriptor = _open_directory(build.temporary)
        try:
            _assert_directory_identity(build.temporary, temporary_descriptor)
            os.rename(
                build.temporary.name,
                build.destination.name,
                src_dir_fd=build.generations_descriptor,
                dst_dir_fd=build.generations_descriptor,
            )
        finally:
            os.close(temporary_descriptor)
        _sync_fd(build.generations_descriptor)
        return records
    except BaseException:
        shutil.rmtree(build.temporary, ignore_errors=True)
        raise


def _prepare_generation_directory(path: Path, service_uid: int, service_gid: int) -> None:
    try:
        path.mkdir(mode=_PUBLICATION_DIRECTORY_MODE, parents=True, exist_ok=True)
        _repair_directory_ownership(path, service_uid, service_gid)
    except OSError as error:
        raise TimelineGenerationError("publication generation directory is not service-writable") from error


def _open_directory(path: Path) -> int:
    flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC
    return os.open(path, flags)


def _open_directory_at(parent: int, name: str) -> int:
    flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC
    return os.open(name, flags, dir_fd=parent)


def _descriptor_path(descriptor: int) -> Path:
    return Path(f"/proc/self/fd/{descriptor}")


def _assert_directory_identity(path: Path, descriptor: int) -> None:
    opened = os.fstat(descriptor)
    current = path.stat(follow_symlinks=False)
    if not S_ISDIR(opened.st_mode) or (opened.st_dev, opened.st_ino) != (current.st_dev, current.st_ino):
        raise OSError("publication directory was replaced during generation publication")


def _assert_child_directory_identity(parent: int, name: str, descriptor: int) -> None:
    opened = os.fstat(descriptor)
    current = os.stat(name, dir_fd=parent, follow_symlinks=False)
    if not S_ISDIR(opened.st_mode) or (opened.st_dev, opened.st_ino) != (current.st_dev, current.st_ino):
        raise TimelineGenerationError("bundle temporary directory was replaced during publication")


def _current_generation_id(publication_descriptor: int, identity: str) -> str | None:
    """Return the current generation only when it uses this repair configuration."""
    try:
        current = os.stat("current", dir_fd=publication_descriptor, follow_symlinks=False)
    except FileNotFoundError:
        return None
    if not S_ISLNK(current.st_mode):
        raise TimelineGenerationError("current publication path must be a generation link")
    target = Path(os.readlink("current", dir_fd=publication_descriptor))
    if target.is_absolute() or target.parts[:1] != (".generations",) or len(target.parts) != _GENERATION_LINK_PARTS:
        raise TimelineGenerationError("current publication path must target a generation")
    generation_id = target.name
    if generation_id == identity:
        _require_current_generation_directory(publication_descriptor)
        return generation_id
    replacement_prefix = f"{identity}."
    if generation_id.startswith(replacement_prefix):
        replacement_digest = generation_id.removeprefix(replacement_prefix)
        if len(replacement_digest) != _SHA256_HEX_LENGTH or not set(replacement_digest) <= _LOWERCASE_HEX:
            raise TimelineGenerationError("current publication path must target a generation")
        _require_current_generation_directory(publication_descriptor)
        return generation_id
    return None


def _require_current_generation_directory(publication_descriptor: int) -> None:
    try:
        target = os.stat("current", dir_fd=publication_descriptor)
    except OSError as error:
        raise TimelineGenerationError("current publication generation is unavailable") from error
    if not S_ISDIR(target.st_mode):
        raise TimelineGenerationError("current publication generation is unavailable")


def _remove_child_directory(parent: int, name: str) -> None:
    try:
        current = os.stat(name, dir_fd=parent, follow_symlinks=False)
    except FileNotFoundError:
        return
    if S_ISLNK(current.st_mode):
        os.unlink(name, dir_fd=parent)
        return
    shutil.rmtree(_descriptor_path(parent) / name, ignore_errors=True)


def _repair_directory_ownership(path: Path, service_uid: int, service_gid: int) -> None:
    """Repair one directory through a stable, no-follow descriptor."""
    descriptor = _open_directory(path)
    try:
        _assert_directory_identity(path, descriptor)
        os.fchmod(descriptor, _PUBLICATION_DIRECTORY_MODE)
        os.fchown(descriptor, service_uid, service_gid)
    finally:
        os.close(descriptor)


def _repair_open_directory(descriptor: int, service_uid: int, service_gid: int) -> None:
    metadata = os.fstat(descriptor)
    if not S_ISDIR(metadata.st_mode):
        raise OSError("publication path is not a directory")
    os.fchmod(descriptor, _PUBLICATION_DIRECTORY_MODE)
    os.fchown(descriptor, service_uid, service_gid)


def _publication_identity() -> tuple[int, int]:
    """Return the UID/GID that owns generated source directories.

    The systemd unit and deployment scripts define the service account by
    name. Root-run recovery must resolve that same contract through NSS; a
    missing or mismatched account is an installation error, not a reason to
    recreate an unwritable tree. A non-root process is already constrained to
    its effective service identity in the supported deployment shape.
    """
    if os.geteuid() != 0:
        return os.geteuid(), os.getegid()
    try:
        user = pwd.getpwnam(_SERVICE_ACCOUNT)
        group = grp.getgrnam(_SERVICE_GROUP)
    except KeyError as error:
        raise TimelineGenerationError("collector service account is unavailable") from error
    if user.pw_gid != group.gr_gid:
        raise TimelineGenerationError("collector service account has an unexpected primary group")
    return user.pw_uid, group.gr_gid


def _materialize_clock_repairs(
    root: Path,
    captured_root: Path,
    ledger: Path,
    existing: tuple[TimeRepair, ...],
) -> tuple[tuple[TimeRepair, ...], int | None]:
    """Durably derive repairs before resolving the applied correction evidence."""
    store = ClockCorrectionStore(root.parent / "device-state.json")
    try:
        store.recover_prepared()
        operations = store.records()
    except ClockCorrectionError as error:
        for path in root.glob("*.json"):
            try:
                value = cast(object, json.loads(path.read_text(encoding="utf-8")))
            except (OSError, json.JSONDecodeError):  # fmt: skip
                continue
            if isinstance(value, dict) and value.get("state") in {"prepared", "unresolved"}:
                raise TimelineGenerationError("clock correction evidence is unresolved") from error
        raise TimelineGenerationError("clock correction evidence is invalid") from error
    unresolved = tuple(operation for operation in operations if operation.state in {"unresolved", "applied"})
    pending = tuple(operation for operation in unresolved if operation.state == "unresolved")
    if len(pending) > 1:
        raise TimelineGenerationError("multiple unresolved clock corrections are ambiguous")
    applied = tuple(operation for operation in unresolved if operation.state == "applied")
    confirmed = tuple(operation for operation in operations if operation.state in {"applied", "resolved"})
    repairs = _repairs_from_confirmed_operations(confirmed)
    _validate_resolved_repairs(existing, confirmed)
    safe_prefix = pending[0].boundary_sequence_min if pending else None
    if applied:
        _validate_applied_operations(
            captured_root,
            repairs,
            applied,
            max_sequence=safe_prefix,
        )
    _persist_repairs(ledger, existing, repairs)
    if applied:
        try:
            for operation in applied:
                store.resolve_applied(operation)
        except ClockCorrectionError as error:
            raise TimelineGenerationError("clock correction evidence is not durable") from error
    return repairs, safe_prefix


def _repairs_from_confirmed_operations(operations: tuple[ClockCorrection, ...]) -> tuple[TimeRepair, ...]:
    repairs = tuple(
        TimeRepair(
            operation.boundary_sequence_min,
            operation.boundary_sequence_max,
            operation.observed_epoch - operation.target_epoch,
            operation.operation_id,
        )
        for operation in operations
        if operation.boundary_sequence_max is not None
        and operation.boundary_sequence_max > operation.boundary_sequence_min
    )
    repairs = tuple(sorted(repairs, key=lambda item: (item.start_sequence, item.next_sequence, item.evidence)))
    _validate_repairs(repairs)
    return repairs


def _validate_resolved_repairs(existing: tuple[TimeRepair, ...], operations: tuple[ClockCorrection, ...]) -> None:
    resolved = _repairs_from_confirmed_operations(
        tuple(operation for operation in operations if operation.state == "resolved")
    )
    if any(repair not in existing for repair in resolved):
        raise TimelineGenerationError("resolved clock correction has no durable timeline repair")


def _persist_repairs(ledger: Path, existing: tuple[TimeRepair, ...], repairs: tuple[TimeRepair, ...]) -> None:
    """Replace the ledger only by extending its evidence-derived repair prefix."""
    if existing == repairs:
        return
    if existing != repairs[: len(existing)]:
        raise TimelineGenerationError("timeline repair ledger conflicts with clock correction evidence")
    _write_repairs_atomic(ledger, repairs)


def _write_repairs_atomic(path: Path, repairs: tuple[TimeRepair, ...]) -> None:
    try:
        path.parent.mkdir(mode=_PUBLICATION_DIRECTORY_MODE, parents=True, exist_ok=True)
        descriptor = _open_directory(path.parent)
    except OSError as error:
        raise TimelineGenerationError("timeline repair ledger is not durable") from error
    temporary_name = f".{path.name}.{uuid4().hex}.tmp"
    payload = _json({"version": 1, "repairs": [asdict(repair) for repair in repairs]})
    service_uid, service_gid = _publication_identity()
    try:
        _write_at(descriptor, temporary_name, payload, service_uid, service_gid)
        try:
            current = _read_repairs(path)
        except TimelineGenerationError:
            raise
        if current != repairs[: len(current)]:
            raise TimelineGenerationError("timeline repair ledger conflicts with clock correction evidence")
        os.replace(temporary_name, path.name, src_dir_fd=descriptor, dst_dir_fd=descriptor)
        _sync_fd(descriptor)
    except (OSError, TimelineGenerationError) as error:
        with suppress(OSError):
            os.unlink(temporary_name, dir_fd=descriptor)
        if isinstance(error, TimelineGenerationError):
            raise
        raise TimelineGenerationError("timeline repair ledger is not durable") from error
    finally:
        os.close(descriptor)


def _read_repairs(path: Path) -> tuple[TimeRepair, ...]:
    if not path.exists():
        return ()
    try:
        value = cast(object, json.loads(path.read_text(encoding="utf-8")))
        if (
            not isinstance(value, dict)
            or set(value) != {"version", "repairs"}
            or isinstance(value["version"], bool)
            or not isinstance(value["version"], int)
            or value["version"] != 1
        ):
            raise ValueError
        rows = value["repairs"]
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


def _validate_applied_operations(
    captured_root: Path,
    repairs: tuple[TimeRepair, ...],
    operations: tuple[ClockCorrection, ...],
    *,
    max_sequence: int | None = None,
) -> None:
    """Prove every applied operation against immutable raw records before resolving it."""
    _validate_repairs(repairs)
    timestamps = _normalized_timestamps(captured_root, repairs, max_sequence=max_sequence)
    for operation in operations:
        boundary_max = operation.boundary_sequence_max
        if (
            operation.boundary_sequence_min < 0
            or boundary_max is None
            or boundary_max < operation.boundary_sequence_min
        ):
            raise TimelineGenerationError("clock correction ambiguity boundary is invalid")
        if boundary_max == operation.boundary_sequence_min:
            continue
        ordered_sequences = tuple(sorted(timestamps))
        predecessor = next(
            (sequence for sequence in reversed(ordered_sequences) if sequence < operation.boundary_sequence_min),
            None,
        )
        successor = next((sequence for sequence in ordered_sequences if sequence >= boundary_max), None)
        if successor is None:
            raise TimelineGenerationError("clock correction ambiguity boundaries are incomplete")
        interval_start = predecessor if predecessor is not None else ordered_sequences[0]
        previous: int | None = None
        for sequence in ordered_sequences:
            if sequence < interval_start or sequence > successor:
                continue
            timestamp = timestamps[sequence]
            assert timestamp is not None
            if previous is not None and timestamp < previous:
                raise TimelineGenerationError("clock correction raw interval regresses")
            previous = timestamp


def _normalized_timestamps(
    captured_root: Path, repairs: tuple[TimeRepair, ...], *, max_sequence: int | None = None
) -> dict[int, int]:
    bundles = _bundles(captured_root, max_sequence=max_sequence)
    timestamps: dict[int, int] = {}
    previous: int | None = None
    for source, manifest in bundles:
        raw = (source / "records.bin").read_bytes()
        if sha256(raw).hexdigest() != manifest.raw_sha256 or len(raw) != manifest.record_count * RECORD_SIZE:
            raise TimelineGenerationError("captured bundle does not match its manifest")
        for index in range(manifest.record_count):
            sequence = manifest.start_sequence + index
            if max_sequence is not None and sequence >= max_sequence:
                break
            position = index * RECORD_SIZE
            raw_timestamp = int.from_bytes(raw[position : position + 4], "big")
            timestamp = raw_timestamp - _repair_offset(sequence, repairs)
            if not 0 <= timestamp <= 2**32 - 1:
                raise TimelineGenerationError("normalized timestamp is outside uint32")
            if previous is not None and timestamp < previous:
                raise TimelineGenerationError("normalized timestamp chain regresses")
            timestamps[sequence] = timestamp
            previous = timestamp
    return timestamps


def _repair_offset(sequence: int, repairs: tuple[TimeRepair, ...]) -> int:
    return next(
        (repair.offset_seconds for repair in repairs if repair.start_sequence <= sequence < repair.next_sequence),
        0,
    )


def _append_generation(  # noqa: PLR0913,PLR0917
    destination: Path,
    identity: str,
    captured: tuple[tuple[Path, BundleManifest], ...],
    repairs: tuple[TimeRepair, ...],
    service_uid: int,
    service_gid: int,
    *,
    max_sequence: int | None = None,
) -> int:
    destination_metadata = destination.stat(follow_symlinks=False)
    existing, records, previous, bundle_metadata = _validate_existing_generation(destination, identity)
    _repair_existing_generation(destination, destination_metadata, existing, bundle_metadata, service_uid, service_gid)
    if len(existing) > len(captured):
        raise TimelineGenerationError("existing generation is ahead of captured source")
    validation_previous: int | None = None
    for index, (output_path, output_manifest) in enumerate(existing):
        source_path, source_manifest = captured[index]
        expected_next = _bounded_next(source_manifest, max_sequence)
        if (output_manifest.start_sequence, output_manifest.next_sequence) != (
            source_manifest.start_sequence,
            expected_next,
        ):
            raise TimelineGenerationError("existing generation source order conflicts")
        raw = (source_path / "records.bin").read_bytes()
        count = expected_next - source_manifest.start_sequence
        normalized, validation_previous = _normalize(
            raw[: count * RECORD_SIZE], source_manifest.start_sequence, repairs, validation_previous
        )
        if sha256(normalized).hexdigest() != output_manifest.raw_sha256:
            raise TimelineGenerationError("existing generation normalization conflicts")
        if sha256((output_path / "records.bin").read_bytes()).hexdigest() != output_manifest.raw_sha256:
            raise TimelineGenerationError("existing generation bundle is invalid")
    remaining = captured[len(existing) :]
    if remaining:
        added, _ = _write_bundles(
            destination,
            remaining,
            repairs,
            previous,
            service_uid=service_uid,
            service_gid=service_gid,
            max_sequence=max_sequence,
        )
        records += added
    _replace_generation_manifest(destination, identity, captured, repairs, records, service_uid, service_gid)
    return records


def _repair_existing_generation(  # noqa: PLR0913,PLR0917
    root: Path,
    expected_root: os.stat_result,
    bundles: tuple[tuple[Path, BundleManifest], ...],
    bundle_metadata: tuple[tuple[str, os.stat_result], ...],
    service_uid: int,
    service_gid: int,
) -> None:
    descriptor = _open_directory(root)
    try:
        _assert_open_identity(descriptor, expected_root, "existing generation")
        expected_names = {"generation.json", *(path.name for path, _ in bundles)}
        _reject_unexpected_entries(descriptor, expected_names, "existing generation")
        _repair_regular_file_at(descriptor, "generation.json", service_uid, service_gid)
        metadata_by_name = dict(bundle_metadata)
        for bundle, _ in bundles:
            bundle_descriptor = _open_directory_at(descriptor, bundle.name)
            try:
                _assert_open_identity(bundle_descriptor, metadata_by_name[bundle.name], "existing bundle")
                _repair_open_directory(bundle_descriptor, service_uid, service_gid)
                _reject_unexpected_entries(
                    bundle_descriptor,
                    {"records.bin", "manifest.json", "receipt.json"},
                    "existing bundle",
                )
                for name in ("records.bin", "manifest.json", "receipt.json"):
                    _repair_regular_file_at(bundle_descriptor, name, service_uid, service_gid)
            finally:
                os.close(bundle_descriptor)
    finally:
        os.close(descriptor)


def _assert_open_identity(descriptor: int, expected: os.stat_result, label: str) -> None:
    actual = os.fstat(descriptor)
    if (actual.st_dev, actual.st_ino) != (expected.st_dev, expected.st_ino):
        raise TimelineGenerationError(f"{label} was replaced during ownership repair")


def _reject_unexpected_entries(parent: int, expected: set[str], label: str) -> None:
    for entry in os.scandir(_descriptor_path(parent)):
        if entry.name not in expected:
            raise TimelineGenerationError(f"{label} contains an unexpected artifact")


def _repair_regular_file_at(parent: int, name: str, service_uid: int, service_gid: int) -> None:
    flags = os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC
    try:
        descriptor = os.open(name, flags, dir_fd=parent)
    except OSError as error:
        raise TimelineGenerationError(f"existing publication artifact is not a regular file: {name}") from error
    try:
        metadata = os.fstat(descriptor)
        if not S_ISREG(metadata.st_mode):
            raise TimelineGenerationError(f"existing publication artifact is not a regular file: {name}")
        os.fchmod(descriptor, _PUBLICATION_FILE_MODE)
        os.fchown(descriptor, service_uid, service_gid)
    finally:
        os.close(descriptor)


def _validate_existing_generation(
    destination: Path, identity: str
) -> tuple[tuple[tuple[Path, BundleManifest], ...], int, int, tuple[tuple[str, os.stat_result], ...]]:
    try:
        value = cast(object, json.loads(_read_regular_file(destination / "generation.json")))
    except (OSError, json.JSONDecodeError) as error:
        raise TimelineGenerationError("existing generation manifest is invalid") from error
    if not isinstance(value, dict) or value.get("generation_id") != identity:
        raise TimelineGenerationError("existing generation identity conflicts")
    bundles = _bundles(destination)
    bundle_metadata = tuple((path.name, path.stat(follow_symlinks=False)) for path, _ in bundles)
    previous: int | None = None
    records = 0
    for source, manifest in bundles:
        raw = _read_regular_file(source / "records.bin")
        if sha256(raw).hexdigest() != manifest.raw_sha256 or len(raw) != manifest.record_count * RECORD_SIZE:
            raise TimelineGenerationError("existing generation bundle is invalid")
        _, previous = _normalize(raw, manifest.start_sequence, (), previous)
        records += manifest.record_count
        _read_regular_file(source / "manifest.json")
        _read_regular_file(source / "receipt.json")
    if previous is None:
        raise TimelineGenerationError("existing generation contains no authenticated bundles")
    return bundles, records, previous, bundle_metadata


def _replace_generation_manifest(  # noqa: PLR0913,PLR0917
    root: Path,
    identity: str,
    bundles: tuple[tuple[Path, BundleManifest], ...],
    repairs: tuple[TimeRepair, ...],
    records: int,
    service_uid: int,
    service_gid: int,
) -> None:
    descriptor = _open_directory(root)
    temporary_name = f".generation.{uuid4().hex}.tmp"
    value = _generation_manifest(identity, bundles, repairs, records)
    try:
        _write_at(descriptor, temporary_name, _json(value), service_uid, service_gid)
        os.replace(temporary_name, "generation.json", src_dir_fd=descriptor, dst_dir_fd=descriptor)
        _sync_fd(descriptor)
    except BaseException:
        with suppress(FileNotFoundError):
            os.unlink(temporary_name, dir_fd=descriptor)
        raise
    finally:
        os.close(descriptor)


def _bundles(root: Path, *, max_sequence: int | None = None) -> tuple[tuple[Path, BundleManifest], ...]:
    found: list[tuple[Path, BundleManifest]] = []
    for path in root.iterdir():
        if path.name.startswith(".") or path.is_symlink() or not path.is_dir():
            continue
        if _suffix_is_beyond_frontier(path, max_sequence):
            continue
        try:
            manifest = BundleManifest.from_json(cast(object, json.loads(_read_regular_file(path / "manifest.json"))))
        except (OSError, ValueError, json.JSONDecodeError) as error:
            raise TimelineGenerationError("captured bundle manifest is invalid") from error
        if max_sequence is not None and manifest.start_sequence >= max_sequence:
            continue
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


def _suffix_is_beyond_frontier(path: Path, max_sequence: int | None) -> bool:
    """Skip a wholly unsafe suffix before reading its possibly partial manifest."""
    if max_sequence is None:
        return False
    try:
        return int(path.name.split("-", 1)[0]) >= max_sequence
    except ValueError:
        return False


def _validate_repairs(repairs: tuple[TimeRepair, ...]) -> None:
    previous = -1
    for repair in sorted(repairs, key=lambda item: item.start_sequence):
        if repair.start_sequence < previous:
            raise TimelineGenerationError("time repairs overlap")
        previous = repair.next_sequence


def _generation_identity(
    bundles: tuple[tuple[Path, BundleManifest], ...], repairs: tuple[TimeRepair, ...], max_sequence: int | None = None
) -> str:
    del bundles
    evidence: dict[str, object] = {
        "algorithm": 2,
        "repairs": [asdict(repair) for repair in repairs],
    }
    # Preserve the historical full-generation identity.  A bounded prefix is
    # a different publication and therefore carries its explicit frontier.
    if max_sequence is not None:
        evidence["max_sequence"] = max_sequence
    return sha256(json.dumps(evidence, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def _replacement_generation_id(
    identity: str, bundles: tuple[tuple[Path, BundleManifest], ...], max_sequence: int | None
) -> str:
    """Name a complete replacement from its ordered immutable source list."""
    source = [
        {
            "next_sequence": _bounded_next(manifest, max_sequence),
            "raw_sha256": manifest.raw_sha256,
            "start_sequence": manifest.start_sequence,
        }
        for _, manifest in bundles
    ]
    digest = sha256(json.dumps(source, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    return f"{identity}.{digest}"


def _existing_sources_are_prefix(
    destination: Path,
    identity: str,
    captured: tuple[tuple[Path, BundleManifest], ...],
    max_sequence: int | None,
) -> bool:
    existing, _, _, _ = _validate_existing_generation(destination, identity)
    if len(existing) > len(captured):
        return False
    source_hashes = _generation_source_hashes(destination)
    if len(source_hashes) != len(existing):
        raise TimelineGenerationError("existing generation manifest is invalid")
    for index, (_, output_manifest) in enumerate(existing):
        _, source_manifest = captured[index]
        if (
            source_hashes[index] != source_manifest.raw_sha256
            or output_manifest.start_sequence != source_manifest.start_sequence
            or output_manifest.next_sequence != _bounded_next(source_manifest, max_sequence)
        ):
            return False
    return True


def _generation_source_hashes(destination: Path) -> tuple[str, ...]:
    try:
        value = cast(object, json.loads(_read_regular_file(destination / "generation.json")))
    except (OSError, json.JSONDecodeError) as error:
        raise TimelineGenerationError("existing generation manifest is invalid") from error
    if not isinstance(value, dict) or not isinstance(value.get("source_hashes"), list):
        raise TimelineGenerationError("existing generation manifest is invalid")
    hashes = value["source_hashes"]
    if any(not isinstance(digest, str) for digest in hashes):
        raise TimelineGenerationError("existing generation manifest is invalid")
    return tuple(cast(str, digest) for digest in hashes)


def _write_bundles(  # noqa: PLR0913
    destination: Path,
    bundles: tuple[tuple[Path, BundleManifest], ...],
    repairs: tuple[TimeRepair, ...],
    previous_timestamp: int | None = None,
    *,
    service_uid: int,
    service_gid: int,
    max_sequence: int | None = None,
) -> tuple[int, int]:
    total = 0
    destination_descriptor = _open_directory(destination)
    try:
        for source, manifest in bundles:
            raw = (source / "records.bin").read_bytes()
            if sha256(raw).hexdigest() != manifest.raw_sha256 or len(raw) != manifest.record_count * RECORD_SIZE:
                raise TimelineGenerationError("captured bundle does not match its manifest")
            if max_sequence is not None and manifest.start_sequence >= max_sequence:
                continue
            count = manifest.record_count
            if max_sequence is not None:
                count = min(count, max_sequence - manifest.start_sequence)
                raw = raw[: count * RECORD_SIZE]
            normalized, previous_timestamp = _normalize(raw, manifest.start_sequence, repairs, previous_timestamp)
            digest = sha256(normalized).hexdigest()
            target_name = f"{manifest.start_sequence}-{manifest.start_sequence + count}-{digest[:16]}"
            temporary_name = f".{target_name}.{uuid4().hex}.tmp"
            os.mkdir(temporary_name, mode=0o750, dir_fd=destination_descriptor)
            try:
                temporary_descriptor = _open_directory_at(destination_descriptor, temporary_name)
            except BaseException:
                _remove_child_directory(destination_descriptor, temporary_name)
                raise
            try:
                _repair_open_directory(temporary_descriptor, service_uid, service_gid)
                output_manifest = BundleManifest(
                    2,
                    manifest.start_sequence,
                    manifest.start_sequence + count,
                    count,
                    RECORD_SIZE,
                    digest,
                )
                receipt = SealedReceipt.from_json(cast(object, json.loads((source / "receipt.json").read_text())))
                _write_at(temporary_descriptor, "records.bin", normalized, service_uid, service_gid)
                _write_at(
                    temporary_descriptor,
                    "manifest.json",
                    _json(output_manifest.as_dict()),
                    service_uid,
                    service_gid,
                )
                _write_at(
                    temporary_descriptor,
                    "receipt.json",
                    _json(SealedReceipt(receipt.attempt_id, digest).as_dict()),
                    service_uid,
                    service_gid,
                )
                _sync_fd(temporary_descriptor)
                _assert_child_directory_identity(destination_descriptor, temporary_name, temporary_descriptor)
                os.rename(
                    temporary_name,
                    target_name,
                    src_dir_fd=destination_descriptor,
                    dst_dir_fd=destination_descriptor,
                )
            except BaseException:
                _remove_child_directory(destination_descriptor, temporary_name)
                raise
            finally:
                os.close(temporary_descriptor)
            _sync_fd(destination_descriptor)
            total += count
    finally:
        os.close(destination_descriptor)
    assert previous_timestamp is not None
    return total, previous_timestamp


def _bounded_next(manifest: BundleManifest, max_sequence: int | None) -> int:
    if max_sequence is None:
        return manifest.next_sequence
    return min(manifest.next_sequence, max_sequence)


def _normalize(
    raw: bytes,
    start_sequence: int,
    repairs: tuple[TimeRepair, ...],
    previous_timestamp: int | None,
) -> tuple[bytes, int]:
    output = bytearray(raw)
    for index in range(len(raw) // RECORD_SIZE):
        sequence = start_sequence + index
        offset = _repair_offset(sequence, repairs)
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


def _write_generation_manifest(  # noqa: PLR0913,PLR0917
    root: Path,
    identity: str,
    bundles: tuple[tuple[Path, BundleManifest], ...],
    repairs: tuple[TimeRepair, ...],
    records: int,
    service_uid: int,
    service_gid: int,
) -> None:
    _write(
        root / "generation.json",
        _json(_generation_manifest(identity, bundles, repairs, records)),
        service_uid,
        service_gid,
    )


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


def _switch_current(publication_root: Path, destination: Path, *, publication_descriptor: int | None = None) -> None:
    owns_descriptor = publication_descriptor is None
    if owns_descriptor:
        try:
            publication_descriptor = _open_directory(publication_root)
        except OSError as error:
            raise TimelineGenerationError("publication current link is not service-writable") from error
    assert publication_descriptor is not None
    descriptor = publication_descriptor
    temporary_name = f".current.{uuid4().hex}.tmp"
    try:
        try:
            current = os.stat("current", dir_fd=descriptor, follow_symlinks=False)
        except FileNotFoundError:
            pass
        else:
            if not S_ISLNK(current.st_mode):
                raise TimelineGenerationError("current publication path must be a generation link")
        relative = destination.relative_to(publication_root)
        os.symlink(relative, temporary_name, target_is_directory=True, dir_fd=descriptor)
        try:
            os.replace(temporary_name, "current", src_dir_fd=descriptor, dst_dir_fd=descriptor)
        except BaseException:
            os.unlink(temporary_name, dir_fd=descriptor)
            raise
        os.fsync(descriptor)
    except OSError as error:
        raise TimelineGenerationError("publication current link is not service-writable") from error
    finally:
        if owns_descriptor:
            os.close(descriptor)


def _json(value: object) -> bytes:
    return (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode()


def _write(path: Path, payload: bytes, service_uid: int, service_gid: int) -> None:
    descriptor = _open_directory(path.parent)
    try:
        _write_at(descriptor, path.name, payload, service_uid, service_gid)
    finally:
        os.close(descriptor)


def _read_regular_file(path: Path) -> bytes:
    parent = _open_directory(path.parent)
    flags = os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC
    try:
        descriptor = os.open(path.name, flags, dir_fd=parent)
    except OSError as error:
        os.close(parent)
        raise TimelineGenerationError(f"publication artifact is not a regular file: {path.name}") from error
    try:
        metadata = os.fstat(descriptor)
        if not S_ISREG(metadata.st_mode):
            raise TimelineGenerationError(f"publication artifact is not a regular file: {path.name}")
        chunks: list[bytes] = []
        while chunk := os.read(descriptor, 1024 * 1024):
            chunks.append(chunk)
        return b"".join(chunks)
    finally:
        os.close(descriptor)
        os.close(parent)


def _write_at(parent: int, name: str, payload: bytes, service_uid: int, service_gid: int) -> None:
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC
    descriptor = os.open(name, flags, 0o600, dir_fd=parent)
    try:
        os.fchmod(descriptor, _PUBLICATION_FILE_MODE)
        os.fchown(descriptor, service_uid, service_gid)
        remaining = memoryview(payload)
        while remaining:
            written = os.write(descriptor, remaining)
            remaining = remaining[written:]
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _sync_tree(root: Path) -> None:
    descriptor = _open_directory(root)
    try:
        for entry in os.scandir(_descriptor_path(descriptor)):
            if entry.is_dir(follow_symlinks=False):
                child = _open_directory_at(descriptor, entry.name)
                try:
                    _sync_fd(child)
                finally:
                    os.close(child)
            elif entry.is_symlink():
                raise OSError("generation tree contains a symlink")
        _sync_fd(descriptor)
    finally:
        os.close(descriptor)


def _sync_directory(path: Path) -> None:
    descriptor = _open_directory(path)
    try:
        _sync_fd(descriptor)
    finally:
        os.close(descriptor)


def _sync_fd(descriptor: int) -> None:
    os.fsync(descriptor)
