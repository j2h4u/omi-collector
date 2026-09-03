"""Capture publication identity, receipts, atomic temporary recovery, and deduplication."""

from __future__ import annotations

import os
import stat
from collections.abc import Mapping
from contextlib import suppress
from dataclasses import dataclass
from pathlib import Path
from uuid import uuid4

from ...config import DEFAULT_CONFIG
from ..domain.ring_protocol import RECORD_SIZE
from .recovery import _published_prefix
from .staging_contract import (
    _MANIFEST_NAME,
    _RAW_NAME,
    _RECEIPT_NAME,
    _TERMINAL_RETIRED_STATE,
    _TERMINAL_RETIREMENT_VERSION,
    _UUID_HEX_LENGTH,
    AttemptDescriptor,
    AttemptStateError,
    DurablePrefix,
    StagingError,
    _is_sha256,
    _required_int,
    _required_str,
    _validate_attempt_id,
    _validate_terminalized_at,
)
from .staging_filesystem import (
    _SHARED_BUNDLE_DIRECTORY_MODE,
    StagingFilesystem,
    _copy_synced,
    _file_hash,
    _files_equal,
    _files_equal_bytes,
    _json_bytes,
    _read_json,
    _read_prefix,
    _require_regular_directory,
    _require_regular_file,
    _sync_directory,
    _write_synced,
)


@dataclass(frozen=True, slots=True)
class SealResult:
    """A published bundle, or an already-existing identical bundle."""

    bundle_path: Path
    deduplicated: bool


@dataclass(frozen=True, slots=True)
class PrefixPublicationEvidence:
    """State-only marker; prefix identity is canonical checkpoint state."""

    state: str = "published"

    @classmethod
    def from_json(cls, value: object) -> PrefixPublicationEvidence:
        if not isinstance(value, Mapping):
            raise AttemptStateError("prefix publication marker must be a JSON object")
        if set(value) != {"version", "state"}:
            raise AttemptStateError("prefix publication marker schema is invalid")
        version = value.get("version")
        if isinstance(version, bool) or not isinstance(version, int) or version != 1:
            raise AttemptStateError("prefix publication marker version is invalid")
        if value.get("state") != "published":
            raise AttemptStateError("prefix publication marker state is invalid")
        return cls("published")

    def as_dict(self) -> dict[str, object]:
        return {"version": 1, "state": self.state}


@dataclass(frozen=True, slots=True)
class TerminalRetirementEvidence:
    """State-only terminal marker; timestamp is needed for retention."""

    terminalized_at_unix_ns: int

    @classmethod
    def from_json(cls, value: object) -> TerminalRetirementEvidence:
        if not isinstance(value, Mapping):
            raise AttemptStateError("terminal-retired marker must be a JSON object")
        expected = {"version", "state", "terminalized_at_unix_ns"}
        if set(value) != expected:
            raise AttemptStateError("terminal-retired marker schema is invalid")
        version = value.get("version")
        state = value.get("state")
        if (
            isinstance(version, bool)
            or not isinstance(version, int)
            or version != _TERMINAL_RETIREMENT_VERSION
            or state != _TERMINAL_RETIRED_STATE
        ):
            raise AttemptStateError("terminal-retired marker identity is invalid")
        terminalized_at = value.get("terminalized_at_unix_ns")
        if isinstance(terminalized_at, bool) or not isinstance(terminalized_at, int):
            raise AttemptStateError("terminal-retired marker timestamp is invalid")
        _validate_terminalized_at(terminalized_at)
        return cls(terminalized_at)

    def as_dict(self) -> dict[str, object]:
        return {
            "version": _TERMINAL_RETIREMENT_VERSION,
            "state": _TERMINAL_RETIRED_STATE,
            "terminalized_at_unix_ns": self.terminalized_at_unix_ns,
        }


def publish_prefix_directory(  # noqa: PLR0913
    filesystem: StagingFilesystem,
    *,
    destination: Path,
    device_slug: str,
    prefix_bytes: bytes,
    manifest: dict[str, object],
    receipt: dict[str, object],
) -> None:
    """Write and atomically publish a checkpoint-authenticated prefix bundle."""
    device_fd, temporary_name, temporary = _prepare_capture_temporary(filesystem, destination, device_slug)
    try:
        _write_synced(temporary / _MANIFEST_NAME, _json_bytes(manifest), filesystem._fsync)
        _write_synced(temporary / _RECEIPT_NAME, _json_bytes(receipt), filesystem._fsync)
        _write_synced(temporary / _RAW_NAME, prefix_bytes, filesystem._fsync)
        _sync_directory(temporary, filesystem._fsync)
        os.rename(temporary_name, destination.name, src_dir_fd=device_fd, dst_dir_fd=device_fd)
        filesystem._fsync(device_fd)
    except BaseException:
        _discard_capture_temporary(filesystem, device_fd, temporary)
        raise
    finally:
        os.close(device_fd)


def publish_full_directory(filesystem: StagingFilesystem, source: Path, destination: Path, device_slug: str) -> None:
    """Publish only the completed bundle contract, never attempt-local state."""
    device_fd, temporary_name, temporary = _prepare_capture_temporary(filesystem, destination, device_slug)
    try:
        for name in (_MANIFEST_NAME, _RECEIPT_NAME, _RAW_NAME):
            entry = source / name
            if entry.is_symlink() or not entry.is_file():
                raise StagingError(f"full publication source entry is not a regular file: {entry.name}")
            _copy_synced(entry, temporary / entry.name, filesystem._fsync)
        _sync_directory(temporary, filesystem._fsync)
        os.rename(temporary_name, destination.name, src_dir_fd=device_fd, dst_dir_fd=device_fd)
        filesystem._fsync(device_fd)
    except BaseException:
        _discard_capture_temporary(filesystem, device_fd, temporary)
        raise
    finally:
        os.close(device_fd)


def recover_capture_temporaries(  # noqa: C901
    filesystem: StagingFilesystem, device_slug: str
) -> tuple[tuple[Path, Path, str], ...]:
    """Recover authenticated temporary bundles and return unsafe actions to quarantine."""
    filesystem._prepare_roots()
    device_root = filesystem.capture_root / device_slug
    try:
        mode = device_root.lstat().st_mode
    except FileNotFoundError:
        return ()
    except OSError as error:
        raise StagingError("capture publication leftovers cannot be inspected") from error
    if stat.S_ISLNK(mode) or not stat.S_ISDIR(mode):
        raise StagingError("capture device root must be a real directory")
    try:
        leftovers = tuple(device_root.iterdir())
    except OSError as error:
        raise StagingError("capture publication leftovers cannot be inspected") from error
    unsafe: list[tuple[Path, Path, str]] = []
    for temporary in leftovers:
        if not temporary.name.startswith(".") or not temporary.name.endswith(".tmp"):
            continue
        try:
            mode = temporary.lstat().st_mode
        except OSError as error:
            raise StagingError("capture publication leftover cannot be inspected") from error
        if stat.S_ISLNK(mode) or not stat.S_ISDIR(mode):
            unsafe.append((temporary, device_root, "capture temporary is unsafe"))
            continue
        try:
            destination, manifest, receipt = _authenticated_capture_temporary(filesystem, temporary, device_slug)
        except (OSError, AttemptStateError) as error:
            unsafe.append((temporary, device_root, str(error)))
            continue
        try:
            completed = _finalize_capture_temporary(
                filesystem,
                temporary=temporary,
                destination=destination,
                device_slug=device_slug,
                manifest=manifest,
                receipt=receipt,
            )
        except OSError, StagingError:
            continue
        if not completed:
            unsafe.append((temporary, device_root, "capture temporary conflicts with canonical destination"))
    return tuple(unsafe)


def _prepare_capture_temporary(
    filesystem: StagingFilesystem, destination: Path, device_slug: str
) -> tuple[int, str, Path]:
    device_fd = filesystem.validate_capture_destination(destination, device_slug)
    temporary_name = f".{destination.name}.{uuid4().hex}.tmp"
    try:
        os.mkdir(temporary_name, _SHARED_BUNDLE_DIRECTORY_MODE, dir_fd=device_fd)
        filesystem._fsync(device_fd)
    except BaseException:
        os.close(device_fd)
        raise
    return device_fd, temporary_name, Path(f"/proc/self/fd/{device_fd}") / temporary_name


def _discard_capture_temporary(filesystem: StagingFilesystem, device_fd: int, temporary: Path) -> None:
    try:
        mode = temporary.lstat().st_mode
    except FileNotFoundError:
        return
    if stat.S_ISLNK(mode) or not stat.S_ISDIR(mode):
        return
    with suppress(OSError):
        import shutil

        shutil.rmtree(temporary)
        filesystem._fsync(device_fd)


def _authenticated_capture_temporary(
    filesystem: StagingFilesystem, temporary: Path, device_slug: str
) -> tuple[Path, dict[str, object], dict[str, object]]:
    _require_regular_directory(temporary)
    for entry in temporary.iterdir():
        _require_regular_file(entry, "capture temporary entry")
    raw_path = temporary / _RAW_NAME
    _require_regular_file(raw_path, _RAW_NAME)
    _require_regular_file(temporary / _MANIFEST_NAME, _MANIFEST_NAME)
    _require_regular_file(temporary / _RECEIPT_NAME, _RECEIPT_NAME)
    manifest = _read_json(temporary / _MANIFEST_NAME)
    start, next_sequence, raw_hash = _validate_capture_temporary_manifest(filesystem, manifest, device_slug, raw_path)
    receipt = _read_json(temporary / _RECEIPT_NAME)
    if not _receipt_matches(receipt, manifest):
        raise AttemptStateError("capture temporary receipt is not canonical")
    destination = filesystem.capture_root / device_slug / f"{start}-{next_sequence}-{raw_hash[:16]}"
    nonce = temporary.name.removeprefix(f".{destination.name}.").removesuffix(".tmp")
    if (
        temporary.name != f".{destination.name}.{nonce}.tmp"
        or len(nonce) != _UUID_HEX_LENGTH
        or not all(character in "0123456789abcdef" for character in nonce)
    ):
        raise AttemptStateError("capture temporary name does not bind its canonical destination")
    return destination, manifest, receipt


def _validate_capture_temporary_manifest(
    filesystem: StagingFilesystem, manifest: dict[str, object], device_slug: str, raw_path: Path
) -> tuple[int, int, str]:
    expected_keys = {"device_slug", "start_sequence", "next_sequence", "record_count", "record_size", "raw_sha256"}
    if set(manifest) != expected_keys:
        raise AttemptStateError("capture temporary manifest is not canonical")
    manifest_device = _required_str(manifest, "device_slug")
    start = _required_int(manifest, "start_sequence")
    next_sequence = _required_int(manifest, "next_sequence")
    count = _required_int(manifest, "record_count")
    record_size = _required_int(manifest, "record_size")
    raw_hash = _required_str(manifest, "raw_sha256")
    if manifest_device != device_slug or start < 0 or count <= 0 or next_sequence != start + count:
        raise AttemptStateError("capture temporary manifest or raw bytes are inconsistent")
    if record_size != RECORD_SIZE or not _is_sha256(raw_hash):
        raise AttemptStateError("capture temporary manifest or raw bytes are inconsistent")
    if (
        raw_path.stat().st_size != count * RECORD_SIZE
        or _file_hash(raw_path, chunk_size=filesystem._durability.io_chunk_bytes) != raw_hash
    ):
        raise AttemptStateError("capture temporary manifest or raw bytes are inconsistent")
    return start, next_sequence, raw_hash


def _finalize_capture_temporary(  # noqa: PLR0913
    filesystem: StagingFilesystem,
    *,
    temporary: Path,
    destination: Path,
    device_slug: str,
    manifest: dict[str, object],
    receipt: dict[str, object],
) -> bool:
    device_fd = filesystem.validate_capture_destination(destination, device_slug)
    try:
        if os.path.lexists(destination):
            if _capture_temporary_matches(filesystem, destination, temporary, manifest, receipt):
                _discard_capture_temporary(filesystem, device_fd, temporary)
                return True
            return False
        try:
            os.rename(temporary.name, destination.name, src_dir_fd=device_fd, dst_dir_fd=device_fd)
        except FileExistsError:
            if _capture_temporary_matches(filesystem, destination, temporary, manifest, receipt):
                _discard_capture_temporary(filesystem, device_fd, temporary)
                return True
            return False
        filesystem._fsync(device_fd)
        return True
    finally:
        os.close(device_fd)


def _capture_temporary_matches(
    filesystem: StagingFilesystem,
    destination: Path,
    temporary: Path,
    manifest: dict[str, object],
    receipt: dict[str, object],
) -> bool:
    try:
        _require_regular_directory(destination)
        for name in (_RAW_NAME, _MANIFEST_NAME, _RECEIPT_NAME):
            _require_regular_file(destination / name, f"published {name}")
        if _read_json(destination / _MANIFEST_NAME) != manifest or _read_json(destination / _RECEIPT_NAME) != receipt:
            return False
        return _files_equal(
            destination / _RAW_NAME, temporary / _RAW_NAME, chunk_size=filesystem._durability.io_chunk_bytes
        )
    except (OSError, AttemptStateError) as error:
        raise StagingError("capture destination cannot be compared strictly") from error


def _bundle_matches(
    bundle_path: Path,
    raw_path: Path,
    manifest: dict[str, object],
    *,
    io_chunk_bytes: int = DEFAULT_CONFIG.durability.io_chunk_bytes,
) -> bool:
    try:
        existing = _read_json(bundle_path / _MANIFEST_NAME)
        receipt = _read_json(bundle_path / _RECEIPT_NAME)
    except AttemptStateError:
        return False
    return (
        existing == manifest
        and _receipt_matches(receipt, manifest)
        and _files_equal(bundle_path / _RAW_NAME, raw_path, chunk_size=io_chunk_bytes)
    )


def is_published_attempt(source: Path, descriptor: AttemptDescriptor, filesystem: StagingFilesystem) -> bool:
    """Authenticate a complete source against its canonical capture bundle."""
    manifest_path = source / _MANIFEST_NAME
    receipt_path = source / _RECEIPT_NAME
    raw_path = source / _RAW_NAME
    if not manifest_path.exists() or not receipt_path.exists() or not raw_path.exists():
        return False
    start = descriptor.read_begin_start
    count = descriptor.read_begin_count
    if start is None or count is None:
        return False
    try:
        raw_hash = _file_hash(raw_path, chunk_size=filesystem._durability.io_chunk_bytes)
        manifest = {
            "device_slug": descriptor.device_slug,
            "start_sequence": start,
            "next_sequence": start + count,
            "record_count": count,
            "record_size": RECORD_SIZE,
            "raw_sha256": raw_hash,
        }
        destination = filesystem.capture_root / descriptor.device_slug / f"{start}-{start + count}-{raw_hash[:16]}"
        return destination.exists() and _bundle_matches(
            destination, raw_path, manifest, io_chunk_bytes=filesystem._durability.io_chunk_bytes
        )
    except OSError, StagingError:
        return False


def _prefix_publication_matches(
    source: Path,
    descriptor: AttemptDescriptor,
    marker: PrefixPublicationEvidence,
    *,
    filesystem: StagingFilesystem,
    io_chunk_bytes: int = DEFAULT_CONFIG.durability.io_chunk_bytes,
) -> bool:
    """Check a source marker against its checkpoint and ordinary destination."""
    try:
        prefix = _published_prefix(source, descriptor, filesystem, io_chunk_bytes=io_chunk_bytes)
        if marker.state != "published":
            return False
        if prefix.record_count == 0:
            return True
        destination = _prefix_destination_for(filesystem.capture_root, descriptor.device_slug, prefix)
        return not destination.is_symlink() and _prefix_destination_matches(
            destination,
            _read_prefix(source / _RAW_NAME, prefix.record_count * RECORD_SIZE),
            _manifest_for_descriptor_prefix(descriptor, prefix),
            io_chunk_bytes=io_chunk_bytes,
        )
    except AttemptStateError, OSError, TypeError:
        return False


def _terminal_retired_marker_matches(marker: TerminalRetirementEvidence) -> int | None:
    """Return terminalization time for this exact terminal-only marker schema."""
    return marker.terminalized_at_unix_ns


def _recoverable_prefix_marker_matches(
    marker: PrefixPublicationEvidence,
) -> bool:
    """Validate the local recoverable marker without requiring its destination."""
    return marker.state == "published"


def _prefix_destination_matches(
    destination: Path,
    prefix_bytes: bytes,
    manifest: dict[str, object],
    *,
    io_chunk_bytes: int = DEFAULT_CONFIG.durability.io_chunk_bytes,
) -> bool:
    if destination.is_symlink():
        return False
    try:
        existing_manifest = _read_json(destination / _MANIFEST_NAME)
        receipt = _read_json(destination / _RECEIPT_NAME)
        _require_regular_file(destination / _RAW_NAME, "published raw")
    except AttemptStateError, OSError:
        return False
    return (
        existing_manifest == manifest
        and _receipt_matches(receipt, manifest)
        and _files_equal_bytes(destination / _RAW_NAME, prefix_bytes, chunk_size=io_chunk_bytes)
    )


def _prefix_destination_for(root: Path, device_slug: str, prefix: DurablePrefix) -> Path:
    device_root = root / device_slug
    return device_root / f"{prefix.start_sequence}-{prefix.next_sequence}-{prefix.raw_sha256[:16]}"


def _manifest_for_descriptor_prefix(descriptor: AttemptDescriptor, prefix: DurablePrefix) -> dict[str, object]:
    return {
        "device_slug": descriptor.device_slug,
        "start_sequence": prefix.start_sequence,
        "next_sequence": prefix.next_sequence,
        "record_count": prefix.record_count,
        "record_size": RECORD_SIZE,
        "raw_sha256": prefix.raw_sha256,
    }


def _receipt_matches(receipt: dict[str, object], manifest: dict[str, object]) -> bool:
    """Check a sealed receipt without tying an exact duplicate to this attempt ID."""
    if not set(receipt).issubset({"attempt_id", "raw_sha256", "status"}):
        return False
    try:
        attempt_id = _required_str(receipt, "attempt_id")
        raw_hash = _required_str(receipt, "raw_sha256")
        status = _required_str(receipt, "status")
        _validate_attempt_id(attempt_id)
    except AttemptStateError, ValueError:
        return False
    expected_hash = manifest.get("raw_sha256")
    return status == "sealed" and _is_sha256(raw_hash) and raw_hash == expected_hash
