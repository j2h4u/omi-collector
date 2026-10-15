"""Physical staging filesystem boundary: paths, locking, durability, and safe file primitives."""

from __future__ import annotations

import os
import stat
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager, suppress
from dataclasses import dataclass
from hashlib import sha256
from json import JSONDecodeError, loads
from os import O_DIRECTORY, O_RDONLY, close, fsync, statvfs
from os import open as os_open
from pathlib import Path
from threading import Lock
from typing import Final, cast
from uuid import uuid4

from ...config import DEFAULT_CONFIG, CollectorConfig, DurabilityConfig, ReadyConfig
from ..domain.ring_protocol import RECORD_SIZE
from .collector_state_db import StateDB
from .debug_logging import debug_event
from .staging_contract import (
    _CHECKPOINT_NAME,
    _DESCRIPTOR_NAME,
    _PREFIX_PUBLICATION_NAME,
    _RAW_NAME,
    _TERMINAL_RETIRED_NAME,
    AttemptStateError,
    DeviceAlreadyRunningError,
    MaintenanceDeferredError,
    StagingError,
    _checkpoint_from_json,
    _descriptor_from_json,
    _validate_descriptor,
)

Fsync = Callable[[int], None]
Statvfs = Callable[[str | Path], object]

# Published raw bundles are a shared collector/downstream boundary.  Keep the
# group class rwx so a parent default ACL can retain its named downstream-user
# entry and mask; world access remains disabled.
_SHARED_BUNDLE_DIRECTORY_MODE: Final = 0o770
_UNKNOWN_OPERATION: Final = "unknown"


def legacy_attempt_row(path: Path) -> dict[str, object]:
    """Read and strictly validate one legacy file-backed attempt row."""
    _require_regular_directory(path)
    for filename, label in (
        (_DESCRIPTOR_NAME, "attempt descriptor"),
        (_CHECKPOINT_NAME, "streaming checkpoint"),
        (_RAW_NAME, "attempt raw data"),
    ):
        _require_regular_file(path / filename, label)
    descriptor_value = _read_json(path / _DESCRIPTOR_NAME)
    checkpoint_value = _read_json(path / _CHECKPOINT_NAME)
    if not isinstance(descriptor_value, dict) or not isinstance(checkpoint_value, dict):
        raise AttemptStateError("legacy attempt metadata is malformed")
    try:
        descriptor = _descriptor_from_json(descriptor_value)
        _validate_descriptor(descriptor, descriptor.attempt_id)
        checkpoint = _checkpoint_from_json(checkpoint_value, descriptor.attempt_id)
    except (TypeError, ValueError) as error:
        raise AttemptStateError("legacy attempt metadata is malformed") from error
    checkpoint_bytes = checkpoint.record_count * RECORD_SIZE
    if _file_size(path / _RAW_NAME) < checkpoint_bytes:
        raise AttemptStateError("legacy attempt raw data is shorter than its checkpoint")
    if _hash_prefix(path / _RAW_NAME, checkpoint_bytes) != checkpoint.raw_sha256:
        raise AttemptStateError("legacy attempt raw prefix does not match its checkpoint")
    prefix_path = path / _PREFIX_PUBLICATION_NAME
    terminal_path = path / _TERMINAL_RETIRED_NAME
    prefix: dict[str, object] | None = None
    terminal: dict[str, object] | None = None
    if os.path.lexists(prefix_path):
        _require_regular_file(prefix_path, "prefix publication marker")
        value = _read_json(prefix_path)
        from .publication import PrefixPublicationEvidence

        prefix = PrefixPublicationEvidence.from_json(value).as_dict()
    if os.path.lexists(terminal_path):
        _require_regular_file(terminal_path, "terminal-retired marker")
        value = _read_json(terminal_path)
        from .publication import TerminalRetirementEvidence

        terminal = TerminalRetirementEvidence.from_json(value).as_dict()
        if prefix is None:
            raise AttemptStateError("legacy terminal marker has no published prefix")
    return {
        "descriptor": descriptor_value,
        "checkpoint": checkpoint_value,
        "prefix_publication": prefix,
        "terminal_retirement": terminal,
    }


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
    state_db_path: Path


@dataclass(frozen=True, slots=True)
class StagingIO:
    """Typed testable I/O dependencies and durable staging configuration."""

    fsync_fn: Fsync = fsync
    statvfs_fn: Statvfs = statvfs
    config: CollectorConfig = DEFAULT_CONFIG


DEFAULT_STAGING_IO: Final = StagingIO()


class StagingFilesystem:
    """Concrete physical boundary for one staging layout."""

    def __init__(
        self,
        spool: Path,
        capture_root: Path,
        *,
        state_db: StateDB,
        io: StagingIO = DEFAULT_STAGING_IO,
    ) -> None:
        self._spool_input = Path(spool).absolute()
        self._capture_root_input = Path(capture_root).absolute()
        self.spool = self._spool_input
        self.capture_root = self._capture_root_input
        self.attempts_root = self.spool / "attempts"
        self.quarantine_root = self.spool / "quarantine"
        self.lock_path = self.spool / "collector.lock"
        self.state_db_path = state_db.path
        self._fsync = io.fsync_fn
        self.state_db = state_db
        self._statvfs = io.statvfs_fn
        self._durability: DurabilityConfig = io.config.durability
        self._ready: ReadyConfig = io.config.ready
        self._terminal_retention_seconds = io.config.staging_retention.terminal_retention_seconds
        self._active_lease: DeviceLock | None = None
        self._lease_mutex = Lock()
        if self.spool == self.capture_root:
            raise StagingError("spool and capture root must be distinct directories")

    @classmethod
    def from_paths(
        cls, paths: StagingPaths, *, state_db: StateDB, io: StagingIO = DEFAULT_STAGING_IO
    ) -> StagingFilesystem:
        filesystem = cls(paths.root, paths.capture_root, state_db=state_db, io=io)
        filesystem._spool_input = Path(paths.root).absolute()
        filesystem._capture_root_input = Path(paths.capture_root).absolute()
        filesystem.attempts_root = Path(paths.attempts).absolute()
        filesystem.quarantine_root = Path(paths.quarantine).absolute()
        filesystem.lock_path = Path(paths.lock).absolute()
        filesystem.state_db_path = Path(paths.state_db_path).absolute()
        return filesystem

    @property
    def paths(self) -> StagingPaths:
        return StagingPaths(
            self.spool,
            self.capture_root,
            self.attempts_root,
            self.quarantine_root,
            self.lock_path,
            self.state_db_path,
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

    def _prepare_roots(self) -> None:
        spool_input = _absolute_root(self._spool_input, "spool")
        capture_input = _absolute_root(self._capture_root_input, "capture")
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
        self.state_db_path = _contained_path(self.state_db_path, self.spool, "collector state database")
        _require_same_filesystem(self.spool, self.capture_root)

    @contextmanager
    def device_lock(self, *, operation: str = _UNKNOWN_OPERATION) -> Iterator[DeviceLock]:
        if not self._lease_mutex.acquire(blocking=False):
            raise DeviceAlreadyRunningError("another in-process adapter owns the collector lease")
        lease = DeviceLock(self)
        acquired_at = time.monotonic()
        try:
            self._prepare_roots()
            self._ensure_directory(self.spool)
            self._active_lease = lease
            lease._activate()
            debug_event(
                "device_lock_acquired",
                operation=operation,
                scope="in_process",
                pid=os.getpid(),
                metadata_status="in_process",
            )
            yield lease
        finally:
            if self._active_lease is lease:
                self._active_lease = None
            lease._release()
            debug_event(
                "device_lock_released",
                operation=operation,
                scope="in_process",
                duration_seconds=max(0.0, time.monotonic() - acquired_at),
                metadata_status="in_process",
            )
            self._lease_mutex.release()

    def require_device_lock(self, lease: DeviceLock) -> None:
        if self._active_lease is not lease or not lease._matches(self):
            raise AttemptStateError("consuming ring operations require the active spool lock")


class DeviceLock:
    """Opaque active lease proving one coordinator owns a device spool lock."""

    def __init__(self, filesystem: StagingFilesystem) -> None:
        self._filesystem = filesystem
        self._active = False

    def _matches(self, filesystem: StagingFilesystem) -> bool:
        return self._active and self._filesystem is filesystem

    @property
    def filesystem(self) -> StagingFilesystem:
        """Return the only filesystem this lease may authorize."""
        return self._filesystem

    def require_active(self) -> None:
        """Prove the lease is still held before a consuming operation starts."""
        self._filesystem.require_device_lock(self)

    def _release(self) -> None:
        self._active = False

    def _activate(self) -> None:
        self._active = True


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


def _sync_directory(path: Path, sync: Fsync) -> None:
    descriptor = os_open(path, O_RDONLY | O_DIRECTORY)
    try:
        sync(descriptor)
    finally:
        close(descriptor)


def _file_size(path: Path) -> int:
    return path.stat().st_size if path.exists() else 0


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
