"""Physical staging filesystem boundary: paths, locking, durability, and safe file primitives."""

from __future__ import annotations

import fcntl
import os
import shutil
import stat
from collections.abc import Callable, Iterator
from contextlib import contextmanager, suppress
from dataclasses import asdict, dataclass
from hashlib import sha256
from json import JSONDecodeError, dumps, loads
from os import O_DIRECTORY, O_RDONLY, close, fsync, statvfs
from os import open as os_open
from pathlib import Path
from typing import cast
from uuid import uuid4

from ...config import DEFAULT_CONFIG, CollectorConfig, DurabilityConfig
from ..domain.ring_protocol import RECORD_SIZE
from .staging_contract import (
    _CHECKPOINT_NAME,
    _DESCRIPTOR_NAME,
    AttemptStateError,
    DeviceAlreadyRunningError,
    DiskSpaceError,
    MaintenanceDeferredError,
    StagingError,
    StreamingCheckpoint,
    _descriptor_from_json,
    _read_checkpoint,
    _validate_descriptor,
)

Fsync = Callable[[int], None]
Statvfs = Callable[[str | Path], object]


def _never_defer() -> bool:
    return False


@dataclass(frozen=True, slots=True)
class StagingPaths:
    """Resolved declared paths used by one collector instance."""

    root: Path
    capture_root: Path
    attempts: Path
    quarantine: Path
    lock: Path
    device_state: Path


class StagingFilesystem:
    """Concrete physical boundary for one staging layout."""

    def __init__(
        self,
        spool: Path,
        capture_root: Path,
        *,
        fsync_fn: Fsync = fsync,
        statvfs_fn: Statvfs = statvfs,
        config: CollectorConfig = DEFAULT_CONFIG,
    ) -> None:
        self._spool_input = Path(spool).absolute()
        self._capture_root_input = Path(capture_root).absolute()
        self.spool = self._spool_input
        self.capture_root = self._capture_root_input
        self.attempts_root = self.spool / "attempts"
        self.quarantine_root = self.spool / "quarantine"
        self.lock_path = self.spool / "collector.lock"
        self.device_state_path = self.spool / "device.json"
        self._fsync = fsync_fn
        self._statvfs = statvfs_fn
        self._durability: DurabilityConfig = config.durability
        self._terminal_retention_seconds = config.staging_retention.terminal_retention_seconds
        self._active_leases: dict[str, DeviceLock] = {}
        if self.spool == self.capture_root:
            raise StagingError("spool and capture root must be distinct directories")

    @classmethod
    def from_paths(cls, paths: StagingPaths, *, config: CollectorConfig = DEFAULT_CONFIG) -> StagingFilesystem:
        filesystem = cls(paths.root, paths.capture_root, config=config)
        filesystem._spool_input = Path(paths.root).absolute()
        filesystem._capture_root_input = Path(paths.capture_root).absolute()
        filesystem.attempts_root = Path(paths.attempts).absolute()
        filesystem.quarantine_root = Path(paths.quarantine).absolute()
        filesystem.lock_path = Path(paths.lock).absolute()
        filesystem.device_state_path = Path(paths.device_state).absolute()
        return filesystem

    @property
    def paths(self) -> StagingPaths:
        return StagingPaths(
            self.spool,
            self.capture_root,
            self.attempts_root,
            self.quarantine_root,
            self.lock_path,
            self.device_state_path,
        )

    def preflight_storage(self) -> None:
        """Create and durably probe every directory needed before a device READ."""
        try:
            self._prepare_roots()
            directories = (
                (self.spool, "collector root"),
                (self.attempts_root, "attempts root"),
                (self.quarantine_root, "quarantine root"),
                (self.capture_root, "publication raw root"),
            )
            for path, label in directories:
                self._ensure_real_directory(path, label)
            for path, label in directories:
                self._probe_directory(path, label)
        except StagingError:
            raise
        except OSError as error:
            raise StagingError("storage preflight failed") from error

    def _probe_directory(self, path: Path, label: str) -> None:
        """Prove one directory accepts durable writes without leaving evidence."""
        probe = path / f".storage-preflight-{uuid4().hex}.tmp"
        try:
            _write_synced(probe, b"omi-collector-storage-preflight\n", self._fsync)
            _require_regular_file(probe, "storage preflight probe")
            probe.unlink()
            _sync_directory(path, self._fsync)
        except OSError as error:
            raise StagingError(f"{label} is not writable and durable") from error
        finally:
            with suppress(OSError):
                probe.unlink()

    def _ensure_real_directory(self, path: Path, label: str) -> None:
        try:
            mode = path.lstat().st_mode
        except FileNotFoundError:
            path.mkdir(parents=True)
            _sync_directory(path.parent, self._fsync)
            _sync_directory(path, self._fsync)
            return
        except OSError as error:
            raise StagingError(f"{label} cannot be inspected") from error
        if stat.S_ISLNK(mode) or not stat.S_ISDIR(mode):
            raise StagingError(f"{label} must be a real directory")

    def _ensure_directory(self, path: Path) -> None:
        if path.exists():
            if not path.is_dir():
                raise StagingError(f"{path} is not a directory")
            return
        path.mkdir(parents=True)
        _sync_directory(path.parent, self._fsync)
        _sync_directory(path, self._fsync)

    def _read_descriptor(self, attempt_path: Path):
        _require_regular_directory(attempt_path)
        _require_regular_file(attempt_path / _DESCRIPTOR_NAME, "attempt descriptor")
        raw = _read_json(attempt_path / _DESCRIPTOR_NAME)
        try:
            descriptor = _descriptor_from_json(raw)
        except (TypeError, ValueError) as error:
            raise AttemptStateError("attempt descriptor is malformed") from error
        _validate_descriptor(descriptor, attempt_path.name)
        return descriptor

    def _write_json_atomic(self, path: Path, value: object) -> None:
        temporary = path.with_name(f".{path.name}.{uuid4().hex}.tmp")
        _write_synced(temporary, _json_bytes(value), self._fsync)
        temporary.replace(path)
        _sync_directory(path.parent, self._fsync)

    def _write_checkpoint(
        self, attempt_path: Path, checkpoint: StreamingCheckpoint, *, allow_missing: bool = False
    ) -> None:
        checkpoint_path = attempt_path / _CHECKPOINT_NAME
        if checkpoint_path.exists() or checkpoint_path.is_symlink():
            _require_regular_file(checkpoint_path, "streaming checkpoint")
            _read_checkpoint(checkpoint_path, checkpoint.attempt_id)
        elif not allow_missing:
            raise AttemptStateError("streaming checkpoint is missing")
        self._write_json_atomic(checkpoint_path, asdict(checkpoint))

    def _prepare_roots(self) -> None:
        spool_input = _absolute_root(self._spool_input, "spool")
        capture_input = _absolute_root(self._capture_root_input, "capture root")
        try:
            spool = spool_input.resolve(strict=True)
            capture = capture_input.resolve(strict=True)
        except OSError as error:
            raise StagingError("spool and capture root cannot be canonicalized") from error
        if spool == capture or spool.is_relative_to(capture) or capture.is_relative_to(spool):
            raise StagingError("spool and capture root must be distinct, non-nested directories")
        self.spool = spool
        self.capture_root = capture
        self.attempts_root = _contained_path(self.attempts_root, self.spool, "attempts root")
        self.quarantine_root = _contained_path(self.quarantine_root, self.spool, "quarantine root")
        self.lock_path = _contained_path(self.lock_path, self.spool, "collector lock")
        self.device_state_path = _contained_path(self.device_state_path, self.spool, "device state")
        _require_same_filesystem(self.spool, self.capture_root)

    def _preflight(self, packet_count: int, device_slug: str) -> None:
        required = packet_count * RECORD_SIZE + max(
            self._durability.staging_headroom_bytes,
            int(packet_count * RECORD_SIZE * self._durability.staging_overhead_fraction),
        )
        self._prepare_roots()
        self._ensure_real_directory(self.capture_root / device_slug, "capture device root")
        result = self._statvfs(self.spool)
        available = result.f_bavail * result.f_frsize  # type: ignore[attr-defined]
        if available < required:
            raise DiskSpaceError(f"need {required} bytes of free space; only {available} bytes available")

    @contextmanager
    def device_lock(self, device_slug: str) -> Iterator[DeviceLock]:
        self._prepare_roots()
        self._ensure_directory(self.spool)
        flags = os.O_RDWR | os.O_CREAT | getattr(os, "O_NOFOLLOW", 0)
        fd = os.open(self.lock_path, flags, 0o600)
        lease = DeviceLock(self, device_slug)
        locked = False
        try:
            if not stat.S_ISREG(os.fstat(fd).st_mode):
                raise StagingError("device lock is not a regular file")
            os.fchmod(fd, 0o600)
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError as error:
                raise DeviceAlreadyRunningError(f"device recovery is already active for {device_slug}") from error
            locked = True
            self._active_leases[device_slug] = lease
            lease._activate()
            yield lease
        finally:
            if locked and self._active_leases.get(device_slug) is lease:
                del self._active_leases[device_slug]
            lease._release()
            with suppress(OSError):
                fcntl.flock(fd, fcntl.LOCK_UN)
            os.close(fd)

    def require_device_lock(self, device_slug: str, lease: DeviceLock) -> None:
        if self._active_leases.get(device_slug) is not lease or not lease._matches(self, device_slug):
            raise AttemptStateError("consuming ring operations require this device's active spool lock")

    def validate_capture_destination(self, destination: Path, device_slug: str) -> int:
        self._prepare_roots()
        self._ensure_real_directory(self.capture_root, "capture root")
        device_root = self.capture_root / device_slug
        self._ensure_real_directory(device_root, "capture device root")
        if destination.parent != device_root:
            raise StagingError("bundle destination escaped capture device root")
        if os.path.lexists(destination) and destination.is_symlink():
            raise StagingError("bundle destination must not be a symlink")
        try:
            return os.open(device_root, os.O_RDONLY | O_DIRECTORY | getattr(os, "O_NOFOLLOW", 0))
        except OSError as error:
            raise StagingError("capture publication destination cannot be inspected") from error

    def file_size(self, path: Path) -> int:
        """Read one evidence size through the filesystem boundary."""
        return _file_size(path)


class DeviceLock:
    """Opaque active lease proving one coordinator owns a device spool lock."""

    def __init__(self, filesystem: StagingFilesystem, device_slug: str) -> None:
        self._filesystem = filesystem
        self._device_slug = device_slug
        self._active = False

    def _matches(self, filesystem: StagingFilesystem, device_slug: str) -> bool:
        return self._active and self._filesystem is filesystem and self._device_slug == device_slug

    @property
    def filesystem(self) -> StagingFilesystem:
        """Return the only filesystem this lease may authorize."""
        return self._filesystem

    @property
    def device_slug(self) -> str:
        """Return the only device this lease may authorize."""
        return self._device_slug

    def require_active(self) -> None:
        """Prove the lease is still held before a consuming operation starts."""
        self._filesystem.require_device_lock(self._device_slug, self)

    def _release(self) -> None:
        self._active = False

    def _activate(self) -> None:
        self._active = True


def _json_bytes(value: object, *, newline: bool = False) -> bytes:
    suffix = "\n" if newline else ""
    return (dumps(value, sort_keys=True, separators=(",", ":")) + suffix).encode()


def _read_json(path: Path) -> dict[str, object]:
    try:
        value = cast(object, loads(path.read_text(encoding="utf-8")))
    except (FileNotFoundError, JSONDecodeError, UnicodeDecodeError) as error:
        raise AttemptStateError(f"cannot read {path.name}") from error
    if not isinstance(value, dict):
        raise AttemptStateError(f"{path.name} must contain a JSON object")
    return value


def _write_synced(path: Path, payload: bytes, sync: Fsync) -> None:
    with path.open("wb") as file:
        file.write(payload)
        file.flush()
        sync(file.fileno())


def _copy_synced(source: Path, destination: Path, sync: Fsync) -> None:
    """Copy one regular source file and fsync the destination inode."""
    with source.open("rb") as input_file, destination.open("xb") as output_file:
        shutil.copyfileobj(input_file, output_file)
        output_file.flush()
        sync(output_file.fileno())


def _create_empty_synced(path: Path, sync: Fsync) -> None:
    """Create a regular empty raw file and make its inode durable."""
    with path.open("xb") as file:
        file.flush()
        sync(file.fileno())


def _fsync_path(path: Path, sync: Fsync) -> None:
    with path.open("rb") as file:
        sync(file.fileno())


def _append_synced(path: Path, payload: bytes, sync: Fsync) -> None:
    with path.open("ab") as file:
        file.write(payload)
        file.flush()
        sync(file.fileno())


def _sync_directory(path: Path, sync: Fsync) -> None:
    descriptor = os_open(path, O_RDONLY | O_DIRECTORY)
    try:
        sync(descriptor)
    finally:
        close(descriptor)


def _file_size(path: Path) -> int:
    return path.stat().st_size if path.exists() else 0


def _file_hash(
    path: Path,
    *,
    chunk_size: int = DEFAULT_CONFIG.durability.io_chunk_bytes,
) -> str:
    digest = sha256()
    with path.open("rb") as file:
        while chunk := file.read(chunk_size):
            digest.update(chunk)
    return digest.hexdigest()


def _files_equal(
    first: Path,
    second: Path,
    *,
    chunk_size: int = DEFAULT_CONFIG.durability.io_chunk_bytes,
) -> bool:
    if _file_size(first) != _file_size(second):
        return False
    with first.open("rb") as left, second.open("rb") as right:
        while left_chunk := left.read(chunk_size):
            if left_chunk != right.read(len(left_chunk)):
                return False
    return True


def _files_equal_bytes(
    path: Path,
    expected: bytes,
    *,
    chunk_size: int = DEFAULT_CONFIG.durability.io_chunk_bytes,
) -> bool:
    if _file_size(path) != len(expected):
        return False
    with path.open("rb") as file:
        offset = 0
        while chunk := file.read(chunk_size):
            if chunk != expected[offset : offset + len(chunk)]:
                return False
            offset += len(chunk)
    return True


def _hash_prefix(
    path: Path,
    size: int,
    *,
    chunk_size: int = DEFAULT_CONFIG.durability.io_chunk_bytes,
    should_defer: Callable[[], bool] = _never_defer,
) -> str:
    digest = sha256()
    remaining = size
    with path.open("rb") as file:
        while remaining:
            if should_defer():
                raise MaintenanceDeferredError
            chunk = file.read(min(chunk_size, remaining))
            if not chunk:
                raise AttemptStateError("streaming raw file ended before checkpoint prefix")
            digest.update(chunk)
            remaining -= len(chunk)
    return digest.hexdigest()


def _read_prefix(path: Path, size: int) -> bytes:
    if size < 0:
        raise AttemptStateError("prefix size must be non-negative")
    with path.open("rb") as file:
        value = file.read(size)
    if len(value) != size:
        raise AttemptStateError("streaming raw file ended before prefix")
    return value


def _absolute_root(path: Path, label: str) -> Path:
    """Create and validate one required root without accepting a symlink."""
    root = Path(path).absolute()
    try:
        mode = root.lstat().st_mode
    except FileNotFoundError:
        try:
            root.mkdir(parents=True)
        except FileExistsError:
            mode = root.lstat().st_mode
        else:
            mode = root.lstat().st_mode
    except OSError as error:
        raise StagingError(f"{label} root cannot be inspected") from error
    if stat.S_ISLNK(mode) or not stat.S_ISDIR(mode):
        raise StagingError(f"{label} root must be a real directory")
    return root


def _contained_path(path: Path, root: Path, label: str) -> Path:
    """Resolve a configured child without permitting aliases or escapes."""
    candidate = Path(path).absolute()
    if candidate.parent != root:
        raise StagingError(f"{label} must be directly beneath collector root")
    if os.path.lexists(candidate) and candidate.is_symlink():
        raise StagingError(f"{label} must not be a symlink")
    return candidate


def _require_same_filesystem(spool: Path, capture_root: Path) -> None:
    """Reject split roots on different filesystems before any READ can start."""
    try:
        spool_device = spool.stat().st_dev
        capture_device = capture_root.stat().st_dev
    except OSError as error:
        raise StagingError("spool and capture root filesystems cannot be inspected") from error
    if spool_device != capture_device:
        raise StagingError("spool and capture root must be on the same filesystem")


def _require_regular_directory(path: Path) -> None:
    try:
        mode = path.lstat().st_mode
    except OSError as error:
        raise AttemptStateError("attempt directory is unreadable") from error
    if stat.S_ISLNK(mode) or not stat.S_ISDIR(mode):
        raise AttemptStateError("attempt directory is not a regular directory")


def _require_regular_file(path: Path, label: str) -> None:
    try:
        mode = path.lstat().st_mode
    except OSError as error:
        raise AttemptStateError(f"{label} is missing or unreadable") from error
    if stat.S_ISLNK(mode) or not stat.S_ISREG(mode):
        raise AttemptStateError(f"{label} is not a regular file")
