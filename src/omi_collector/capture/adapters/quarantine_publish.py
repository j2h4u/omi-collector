"""Publish a checkpoint-authenticated prefix from a quarantined attempt."""

from __future__ import annotations

import json
import os
import stat
from collections.abc import Callable
from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path
from typing import Final, cast
from uuid import uuid4

from ...config import DEFAULT_CONFIG
from ..domain.ring_protocol import RECORD_SIZE
from .bundle_contract import BundleManifest, SealedReceipt
from .staging_filesystem import _SHARED_BUNDLE_DIRECTORY_MODE, StagingPaths
from .staging_store import StagingStore

_ATTEMPT_NAME: Final = "attempt.json"
_CHECKPOINT_NAME: Final = "checkpoint.json"
_RAW_NAME: Final = "records.bin"
_MANIFEST_NAME: Final = "manifest.json"
_RECEIPT_NAME: Final = "receipt.json"
_ATTEMPT_KEYS: Final = frozenset(
    {
        "attempt_id",
        "device_slug",
        "start_sequence",
        "packet_count",
        "record_size",
        "read_begin_start",
        "read_begin_count",
    }
)
_CHECKPOINT_KEYS: Final = frozenset({"version", "attempt_id", "record_count", "raw_sha256"})
_HEX = frozenset("0123456789abcdef")
_ATTEMPT_ID_LENGTH = 32
_SHA256_LENGTH = 64


def _never_defer() -> bool:
    return False


class QuarantinePublishError(ValueError):
    """The quarantined attempt cannot authenticate an ordinary bundle."""


class QuarantineOutputCollisionError(RuntimeError):
    """The ordinary bundle identity exists but does not match the prefix."""


class QuarantineSalvageDeferredError(Exception):
    """Cooperative maintenance deferral before an atomic publication commits."""


@dataclass(frozen=True, slots=True)
class QuarantinePublication:
    """The ordinary bundle produced from a quarantined checkpoint prefix."""

    bundle_path: Path
    device_slug: str
    start_sequence: int
    next_sequence: int
    record_count: int
    raw_bytes: int
    raw_sha256: str
    deduplicated: bool

    def as_dict(self) -> dict[str, object]:
        return {
            "bundle_path": str(self.bundle_path),
            "device_slug": self.device_slug,
            "start_sequence": self.start_sequence,
            "next_sequence": self.next_sequence,
            "record_count": self.record_count,
            "raw_bytes": self.raw_bytes,
            "raw_sha256": self.raw_sha256,
            "deduplicated": self.deduplicated,
        }


@dataclass(frozen=True, slots=True)
class _Prefix:
    start_sequence: int
    record_count: int
    raw_sha256: str

    @property
    def next_sequence(self) -> int:
        return self.start_sequence + self.record_count


def publish_quarantined_prefix(
    source: Path,
    paths: StagingPaths,
    device_slug: str,
    *,
    should_defer: Callable[[], bool] = _never_defer,
) -> QuarantinePublication:
    """Publish one checkpoint-bound prefix under the device spool lock."""
    _validate_slug(device_slug)
    with StagingStore.from_paths(paths).device_lock(device_slug, recover_capture_temporaries=False):
        return _publish_quarantined_prefix(source, paths, device_slug, should_defer=should_defer)


def _publish_quarantined_prefix(
    source: Path,
    paths: StagingPaths,
    device_slug: str,
    *,
    should_defer: Callable[[], bool],
) -> QuarantinePublication:
    """Publish one checkpoint-bound prefix while leaving ``source`` untouched."""
    _validate_slug(device_slug)
    source, spool = _validate_quarantine_source(
        source,
        paths.root,
        device_slug,
        quarantine_root=paths.quarantine,
    )
    capture_root = paths.capture_root.absolute()
    _defer_if_requested(should_defer)
    prefix, raw_path, attempt_id = _read_authenticated_prefix(
        source,
        device_slug,
        should_defer=should_defer,
    )

    try:
        _ensure_real_directory(capture_root, "capture root")
        _, spool, capture_root = _validate_source_location(
            source,
            spool,
            capture_root,
            device_slug,
            quarantine_root=paths.quarantine,
        )
        destination = (
            capture_root.absolute()
            / device_slug
            / f"{prefix.start_sequence}-{prefix.next_sequence}-{prefix.raw_sha256[:16]}"
        )
        manifest = BundleManifest(
            device_slug,
            prefix.start_sequence,
            prefix.next_sequence,
            prefix.record_count,
            RECORD_SIZE,
            prefix.raw_sha256,
        ).as_dict()
        receipt = SealedReceipt(attempt_id, prefix.raw_sha256).as_dict()
        if os.path.lexists(destination):
            if _destination_matches(
                destination,
                manifest,
                receipt,
                raw_path,
                prefix.record_count * RECORD_SIZE,
                should_defer=should_defer,
            ):
                return _result(destination, device_slug, prefix, True)
            raise QuarantineOutputCollisionError(f"ordinary bundle collision at {destination}")
        _publish_atomic(
            destination,
            manifest,
            receipt,
            raw_path,
            prefix.record_count * RECORD_SIZE,
            should_defer=should_defer,
        )
    except QuarantineOutputCollisionError:
        raise
    except QuarantinePublishError as error:
        raise OSError("capture publication is temporarily unavailable") from error
    return _result(destination, device_slug, prefix, False)


def _read_authenticated_prefix(
    source: Path,
    device_slug: str,
    *,
    should_defer: Callable[[], bool],
) -> tuple[_Prefix, Path, str]:
    """Read and authenticate the checkpoint-bound prefix from one attempt."""
    attempt = _read_attempt(source / _ATTEMPT_NAME, source.name)
    if attempt["device_slug"] != device_slug:
        raise QuarantinePublishError("attempt device slug does not match --device-slug")
    checkpoint = _read_checkpoint(source / _CHECKPOINT_NAME, cast(str, attempt["attempt_id"]))
    record_count = cast(int, checkpoint["record_count"])
    if record_count <= 0 or record_count > cast(int, attempt["packet_count"]):
        raise QuarantinePublishError("checkpoint record count is outside the attempt range")

    raw_path = source / _RAW_NAME
    _require_regular_file(raw_path, _RAW_NAME)
    actual_hash = _hash_prefix(
        raw_path,
        record_count * RECORD_SIZE,
        should_defer=should_defer,
    )
    expected_hash = cast(str, checkpoint["raw_sha256"])
    if actual_hash != expected_hash:
        raise QuarantinePublishError("records.bin prefix SHA-256 does not match checkpoint")
    prefix = _Prefix(cast(int, attempt["start_sequence"]), record_count, expected_hash)
    return prefix, raw_path, cast(str, attempt["attempt_id"])


def _result(
    destination: Path,
    device_slug: str,
    prefix: _Prefix,
    deduplicated: bool,
) -> QuarantinePublication:
    return QuarantinePublication(
        destination,
        device_slug,
        prefix.start_sequence,
        prefix.next_sequence,
        prefix.record_count,
        prefix.record_count * RECORD_SIZE,
        prefix.raw_sha256,
        deduplicated,
    )


def _validate_source_location(
    source: Path,
    spool: Path,
    capture_root: Path,
    device_slug: str,
    *,
    quarantine_root: Path | None = None,
) -> tuple[Path, Path, Path]:
    capture_root = capture_root.absolute()
    source, spool = _validate_quarantine_source(source, spool, device_slug, quarantine_root=quarantine_root)
    _require_regular_directory(capture_root, "capture root")
    try:
        canonical_capture_root = capture_root.resolve(strict=True)
    except OSError as error:
        raise QuarantinePublishError("spool and capture root cannot be canonicalized") from error
    if (
        spool == canonical_capture_root
        or spool.is_relative_to(canonical_capture_root)
        or canonical_capture_root.is_relative_to(spool)
    ):
        raise QuarantinePublishError("spool and capture root must be distinct, non-nested directories")
    try:
        if spool.stat().st_dev != canonical_capture_root.stat().st_dev:
            raise QuarantinePublishError("spool and capture root must be on the same filesystem")
    except OSError as error:
        raise QuarantinePublishError("spool and capture root filesystems cannot be inspected") from error
    return source, spool, capture_root


def _validate_quarantine_source(
    source: Path, spool: Path, device_slug: str, *, quarantine_root: Path | None = None
) -> tuple[Path, Path]:
    spool = spool.absolute()
    source = source.absolute()
    _require_regular_directory(spool, "spool")
    try:
        spool = spool.resolve(strict=True)
    except OSError as error:
        raise QuarantinePublishError("collector root cannot be canonicalized") from error
    quarantine = Path(quarantine_root).absolute() if quarantine_root is not None else spool / "quarantine"
    if quarantine.parent != spool:
        raise QuarantinePublishError("quarantine root must be directly beneath collector root")
    quarantine_device = quarantine / device_slug
    _require_regular_directory(quarantine, "quarantine root")
    _require_regular_directory(quarantine_device, "quarantine device directory")
    try:
        relative = source.relative_to(quarantine_device)
    except ValueError as error:
        raise QuarantinePublishError("source must be under collector quarantine/device") from error
    if len(relative.parts) != 1:
        raise QuarantinePublishError("source must be one quarantined attempt directory")
    _require_regular_directory(source, "quarantined attempt")
    return source.resolve(strict=True), spool


def _read_attempt(path: Path, source_name: str) -> dict[str, object]:
    raw = _read_object(path, _ATTEMPT_NAME)
    if frozenset(raw) != _ATTEMPT_KEYS:
        raise QuarantinePublishError("attempt.json schema is not exact")
    _validate_attempt_identity(raw, source_name)
    _validate_attempt_range(raw)
    _validate_read_begin(raw)
    return raw


def _validate_attempt_identity(raw: dict[str, object], source_name: str) -> None:
    attempt_id = _string(raw, "attempt_id")
    if len(attempt_id) != _ATTEMPT_ID_LENGTH or any(char not in _HEX for char in attempt_id):
        raise QuarantinePublishError("attempt_id is invalid")
    if source_name != attempt_id and not source_name.startswith(f"{attempt_id}-"):
        raise QuarantinePublishError("quarantined directory does not identify attempt_id")
    _validate_slug(_string(raw, "device_slug"))


def _validate_attempt_range(raw: dict[str, object]) -> None:
    _nonnegative_int(raw, "start_sequence")
    packet_count = _nonnegative_int(raw, "packet_count")
    if packet_count <= 0 or _nonnegative_int(raw, "record_size") != RECORD_SIZE:
        raise QuarantinePublishError("attempt range or record size is invalid")


def _validate_read_begin(raw: dict[str, object]) -> None:
    read_start = raw["read_begin_start"]
    read_count = raw["read_begin_count"]
    if (
        not isinstance(read_start, int)
        or isinstance(read_start, bool)
        or not isinstance(read_count, int)
        or isinstance(read_count, bool)
    ):
        raise QuarantinePublishError("attempt READ_BEGIN is missing")
    if read_start != _nonnegative_int(raw, "start_sequence") or read_count != _nonnegative_int(raw, "packet_count"):
        raise QuarantinePublishError("attempt READ_BEGIN does not match range")


def _read_checkpoint(path: Path, attempt_id: str) -> dict[str, object]:
    _require_regular_file(path, _CHECKPOINT_NAME)
    raw = _read_object(path, _CHECKPOINT_NAME)
    if frozenset(raw) != _CHECKPOINT_KEYS:
        raise QuarantinePublishError("checkpoint.json schema is not exact")
    if _nonnegative_int(raw, "version") != 1 or _string(raw, "attempt_id") != attempt_id:
        raise QuarantinePublishError("checkpoint identity or version is invalid")
    _nonnegative_int(raw, "record_count")
    digest = _string(raw, "raw_sha256")
    if len(digest) != _SHA256_LENGTH or any(char not in _HEX for char in digest):
        raise QuarantinePublishError("checkpoint hash is invalid")
    return raw


def _hash_prefix(
    path: Path,
    size: int,
    *,
    should_defer: Callable[[], bool],
) -> str:
    digest = sha256()
    remaining = size
    descriptor = _open_regular_file(path, _RAW_NAME)
    try:
        while remaining:
            _defer_if_requested(should_defer)
            chunk = os.read(descriptor, min(DEFAULT_CONFIG.durability.io_chunk_bytes, remaining))
            if not chunk:
                raise QuarantinePublishError("records.bin ends before checkpoint prefix")
            digest.update(chunk)
            remaining -= len(chunk)
    finally:
        os.close(descriptor)
    _defer_if_requested(should_defer)
    return digest.hexdigest()


def _destination_matches(  # noqa: PLR0913
    destination: Path,
    manifest: dict[str, object],
    receipt: dict[str, object],
    raw_source: Path,
    prefix_size: int,
    *,
    should_defer: Callable[[], bool],
) -> bool:
    _require_regular_directory(destination, "ordinary bundle destination")
    try:
        names = {entry.name for entry in destination.iterdir()}
    except OSError as error:
        raise OSError("ordinary bundle destination cannot be inspected") from error
    if names != {_RAW_NAME, _MANIFEST_NAME, _RECEIPT_NAME}:
        raise OSError("ordinary bundle destination is not yet canonical")
    existing_manifest = _read_object(destination / _MANIFEST_NAME, _MANIFEST_NAME)
    existing_receipt = _read_object(destination / _RECEIPT_NAME, _RECEIPT_NAME)
    try:
        canonical_manifest = BundleManifest.from_json(existing_manifest).as_dict()
    except ValueError as error:
        raise QuarantineOutputCollisionError(f"ordinary bundle collision at {destination}") from error
    if (
        canonical_manifest != manifest
        or existing_receipt != receipt
        or not _files_equal_prefix(
            destination / _RAW_NAME,
            raw_source,
            prefix_size,
            should_defer=should_defer,
        )
    ):
        raise QuarantineOutputCollisionError(f"ordinary bundle collision at {destination}")
    return True


def _publish_atomic(  # noqa: PLR0913
    destination: Path,
    manifest: dict[str, object],
    receipt: dict[str, object],
    raw_source: Path,
    prefix_size: int,
    *,
    should_defer: Callable[[], bool],
) -> None:
    _defer_if_requested(should_defer)
    capture_root = _canonical_capture_root(destination.parent.parent)
    parent = capture_root / destination.parent.name
    _ensure_real_directory(capture_root, "capture root")
    _ensure_real_directory(parent, "bundle device directory")
    if destination.parent.resolve(strict=True) != parent:
        raise QuarantinePublishError("bundle destination escaped capture root")
    device_fd = _open_directory_fd(parent, "bundle device directory")
    temporary_name = f".{destination.name}.{uuid4().hex}.tmp"
    temporary = Path(f"/proc/self/fd/{device_fd}") / temporary_name
    completed = False
    try:
        os.mkdir(temporary_name, _SHARED_BUNDLE_DIRECTORY_MODE, dir_fd=device_fd)
        _write_prefix_file(
            temporary / _RAW_NAME,
            raw_source,
            prefix_size,
            should_defer=should_defer,
        )
        _write_file(
            temporary / _MANIFEST_NAME,
            _json_bytes(manifest),
            should_defer=should_defer,
        )
        _write_file(
            temporary / _RECEIPT_NAME,
            _json_bytes(receipt),
            should_defer=should_defer,
        )
        _defer_if_requested(should_defer)
        _fsync_directory(temporary)
        _defer_if_requested(should_defer)
        try:
            os.rename(temporary_name, destination.name, src_dir_fd=device_fd, dst_dir_fd=device_fd)
        except OSError as error:
            if not os.path.lexists(destination):
                raise
            if _destination_matches(
                destination,
                manifest,
                receipt,
                raw_source,
                prefix_size,
                should_defer=_never_defer,
            ):
                completed = True
                return
            raise QuarantineOutputCollisionError(f"ordinary bundle collision at {destination}") from error
        completed = True
        _fsync_directory(parent)
    finally:
        if not completed and temporary.exists():
            _remove_temporary(temporary)
        os.close(device_fd)


def _canonical_capture_root(path: Path) -> Path:
    """Resolve the configured capture root without following a swapped symlink."""
    _require_regular_directory(path, "capture root")
    try:
        return path.resolve(strict=True)
    except OSError as error:
        raise QuarantinePublishError("capture root cannot be canonicalized") from error


def _ensure_real_directory(path: Path, label: str) -> None:
    """Create a directory while rejecting symlink and non-directory entries."""
    try:
        mode = path.lstat().st_mode
    except FileNotFoundError:
        try:
            path.mkdir()
        except FileExistsError:
            _ensure_real_directory(path, label)
            return
        return
    except OSError as error:
        raise QuarantinePublishError(f"{label} is missing or unreadable") from error
    if stat.S_ISLNK(mode) or not stat.S_ISDIR(mode):
        raise QuarantinePublishError(f"{label} is not a regular directory")


def _open_directory_fd(path: Path, label: str) -> int:
    try:
        fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    except OSError as error:
        raise QuarantinePublishError(f"{label} is missing or unreadable") from error
    if not stat.S_ISDIR(os.fstat(fd).st_mode):
        os.close(fd)
        raise QuarantinePublishError(f"{label} is not a regular directory")
    return fd


def _read_object(path: Path, label: str) -> dict[str, object]:
    try:
        value = cast(object, json.loads(_read_regular_bytes(path, label).decode("utf-8")))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise QuarantinePublishError(f"{label} is unreadable") from error
    if not isinstance(value, dict):
        raise QuarantinePublishError(f"{label} must contain a JSON object")
    return cast(dict[str, object], value)


def _read_regular_bytes(
    path: Path,
    label: str,
    *,
    should_defer: Callable[[], bool] = _never_defer,
    chunk_size: int = DEFAULT_CONFIG.durability.io_chunk_bytes,
) -> bytes:
    descriptor = _open_regular_file(path, label)
    try:
        chunks: list[bytes] = []
        while True:
            _defer_if_requested(should_defer)
            chunk = os.read(descriptor, chunk_size)
            if not chunk:
                break
            chunks.append(chunk)
    except OSError as error:
        raise QuarantinePublishError(f"{label} is missing or unreadable") from error
    finally:
        os.close(descriptor)
    return b"".join(chunks)


def _open_regular_file(path: Path, label: str) -> int:
    try:
        descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    except OSError as error:
        raise QuarantinePublishError(f"{label} is missing or unreadable") from error
    try:
        if not stat.S_ISREG(os.fstat(descriptor).st_mode):
            raise QuarantinePublishError(f"{label} is not a regular file")
    except BaseException:
        os.close(descriptor)
        raise
    return descriptor


def _require_regular_directory(path: Path, label: str) -> None:
    try:
        mode = path.lstat().st_mode
    except OSError as error:
        raise QuarantinePublishError(f"{label} is missing or unreadable") from error
    if stat.S_ISLNK(mode) or not stat.S_ISDIR(mode):
        raise QuarantinePublishError(f"{label} is not a regular directory")


def _require_regular_file(path: Path, label: str) -> None:
    try:
        mode = path.lstat().st_mode
    except OSError as error:
        raise QuarantinePublishError(f"{label} is missing or unreadable") from error
    if stat.S_ISLNK(mode) or not stat.S_ISREG(mode):
        raise QuarantinePublishError(f"{label} is not a regular file")


def _string(raw: dict[str, object], key: str) -> str:
    value = raw.get(key)
    if not isinstance(value, str):
        raise QuarantinePublishError(f"{key} is invalid")
    return value


def _nonnegative_int(raw: dict[str, object], key: str) -> int:
    value = raw.get(key)
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise QuarantinePublishError(f"{key} is invalid")
    return value


def _validate_slug(value: str) -> None:
    if not value or any(
        char not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-" for char in value
    ):
        raise QuarantinePublishError("device slug is invalid")


def _json_bytes(value: object) -> bytes:
    return (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode()


def _write_file(
    path: Path,
    payload: bytes,
    *,
    should_defer: Callable[[], bool],
) -> None:
    with path.open("xb") as stream:
        offset = 0
        while offset < len(payload):
            _defer_if_requested(should_defer)
            offset += stream.write(payload[offset : offset + DEFAULT_CONFIG.durability.io_chunk_bytes])
        stream.flush()
        os.fsync(stream.fileno())


def _write_prefix_file(
    path: Path,
    source: Path,
    size: int,
    *,
    should_defer: Callable[[], bool],
) -> None:
    """Copy a source prefix with bounded buffers and durable completion."""
    source_fd = _open_regular_file(source, _RAW_NAME)
    try:
        with path.open("xb") as stream:
            remaining = size
            while remaining:
                _defer_if_requested(should_defer)
                chunk = os.read(source_fd, min(DEFAULT_CONFIG.durability.io_chunk_bytes, remaining))
                if not chunk:
                    raise QuarantinePublishError("records.bin ends before checkpoint prefix")
                offset = 0
                while offset < len(chunk):
                    offset += stream.write(chunk[offset:])
                remaining -= len(chunk)
            stream.flush()
            os.fsync(stream.fileno())
    finally:
        os.close(source_fd)


def _files_equal_prefix(
    first: Path,
    second: Path,
    size: int,
    *,
    should_defer: Callable[[], bool],
) -> bool:
    """Compare one bounded prefix without materializing it in memory."""
    try:
        first_fd = _open_regular_file(first, _RAW_NAME)
        second_fd = _open_regular_file(second, _RAW_NAME)
    except QuarantinePublishError:
        return False
    try:
        if os.fstat(first_fd).st_size != size or os.fstat(second_fd).st_size < size:
            return False
        remaining = size
        while remaining:
            _defer_if_requested(should_defer)
            left = os.read(first_fd, min(DEFAULT_CONFIG.durability.io_chunk_bytes, remaining))
            right = os.read(second_fd, len(left))
            if not left or left != right:
                return False
            remaining -= len(left)
        return True
    except OSError as error:
        raise QuarantinePublishError("prefix comparison failed") from error
    finally:
        os.close(first_fd)
        os.close(second_fd)


def _defer_if_requested(should_defer: Callable[[], bool]) -> None:
    if should_defer():
        raise QuarantineSalvageDeferredError


def _fsync_directory(path: Path) -> None:
    fd = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _remove_temporary(path: Path) -> None:
    for child in path.iterdir():
        child.unlink()
    path.rmdir()
