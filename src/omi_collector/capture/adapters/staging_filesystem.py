"""Physical staging filesystem boundary: paths, locking, durability, and safe file primitives."""

from __future__ import annotations

import fcntl
import os
import shutil
import stat
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager, suppress
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from hashlib import sha256
from json import JSONDecodeError, dumps, loads
from os import O_DIRECTORY, O_RDONLY, close, fsync, statvfs
from os import open as os_open
from pathlib import Path
from threading import get_ident
from typing import Final, cast
from uuid import uuid4

from ...config import DEFAULT_CONFIG, CollectorConfig, DurabilityConfig
from ..domain.ring_protocol import RECORD_SIZE
from .debug_logging import debug_event
from .staging_contract import (
    _CHECKPOINT_NAME,
    _DESCRIPTOR_NAME,
    AttemptStateError,
    DeviceAlreadyRunningError,
    DiskSpaceError,
    LockContext,
    MaintenanceDeferredError,
    StagingError,
    StreamingCheckpoint,
    _descriptor_from_json,
    _read_checkpoint,
    _validate_descriptor,
)

Fsync = Callable[[int], None]
Statvfs = Callable[[str | Path], object]

# Published raw bundles are a shared collector/downstream boundary.  Keep the
# group class rwx so a parent default ACL can retain its named downstream-user
# entry and mask; world access remains disabled.
_SHARED_BUNDLE_DIRECTORY_MODE: Final = 0o770
_LOCK_SCOPE: Final = "collector_lock"
_LOCK_METADATA_VERSION: Final = 1
_LOCK_METADATA_MAX_BYTES: Final = 4096
_UNKNOWN_OPERATION: Final = "unknown"


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
        self._active_lease: DeviceLock | None = None
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
        self.device_state_path = _contained_path(self.device_state_path, self.spool, "device state")
        _require_same_filesystem(self.spool, self.capture_root)

    def _preflight(self, packet_count: int, *, staged_bytes: int = 0) -> None:
        captured_bytes = packet_count * RECORD_SIZE
        if staged_bytes < 0 or staged_bytes > captured_bytes:
            raise AttemptStateError("staged raw bytes exceed the prepared range")
        reserve_bytes = max(
            self._durability.staging_headroom_bytes,
            int(captured_bytes * self._durability.staging_overhead_fraction),
        )
        # Full publication copies records.bin into a capture-local temporary
        # directory before atomically renaming it.  The source attempt remains
        # recoverable until that switch succeeds. Existing raw bytes are already
        # allocated, so later READ legs reserve only the missing source suffix,
        # then the full capture copy and configured metadata/durability reserve.
        required = (captured_bytes - staged_bytes) + captured_bytes + reserve_bytes
        self._prepare_roots()
        self._ensure_real_directory(self.capture_root, "capture root")
        result = self._statvfs(self.spool)
        available = result.f_bavail * result.f_frsize  # type: ignore[attr-defined]
        if available < required:
            raise DiskSpaceError(f"need {required} bytes of free space; only {available} bytes available")

    @contextmanager
    def device_lock(self, *, operation: str = _UNKNOWN_OPERATION) -> Iterator[DeviceLock]:
        self._prepare_roots()
        self._ensure_directory(self.spool)
        flags = os.O_RDWR | os.O_CREAT | getattr(os, "O_NOFOLLOW", 0)
        fd = os.open(self.lock_path, flags, 0o600)
        lease = DeviceLock(self)
        locked = False
        acquired_monotonic_ns: int | None = None
        metadata_status = "unavailable"
        try:
            if not stat.S_ISREG(os.fstat(fd).st_mode):
                raise StagingError("device lock is not a regular file")
            os.fchmod(fd, 0o600)
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError as error:
                lock_context = _read_lock_context(fd, operation)
                cast(Callable[..., None], debug_event)("device_lock_busy", **lock_context.as_dict())
                raise DeviceAlreadyRunningError(lock_context=lock_context) from error
            locked = True
            acquired_monotonic_ns = time.monotonic_ns()
            metadata_status = _write_lock_metadata(fd, operation, acquired_monotonic_ns)
            self._active_lease = lease
            lease._activate()
            debug_event(
                "device_lock_acquired",
                operation=operation,
                scope=_LOCK_SCOPE,
                pid=os.getpid(),
                thread_id=get_ident(),
                metadata_status=metadata_status,
            )
            yield lease
        finally:
            if locked and self._active_lease is lease:
                self._active_lease = None
            if locked:
                duration_seconds = None
                if acquired_monotonic_ns is not None:
                    duration_seconds = max(0.0, (time.monotonic_ns() - acquired_monotonic_ns) / 1_000_000_000)
                debug_event(
                    "device_lock_released",
                    operation=operation,
                    scope=_LOCK_SCOPE,
                    duration_seconds=duration_seconds,
                    metadata_status=metadata_status,
                )
                _clear_lock_metadata(fd)
            lease._release()
            with suppress(OSError):
                fcntl.flock(fd, fcntl.LOCK_UN)
            os.close(fd)

    def require_device_lock(self, lease: DeviceLock) -> None:
        if self._active_lease is not lease or not lease._matches(self):
            raise AttemptStateError("consuming ring operations require the active spool lock")

    def validate_capture_destination(self, destination: Path) -> int:
        self._prepare_roots()
        self._ensure_real_directory(self.capture_root, "capture root")
        if destination.parent != self.capture_root:
            raise StagingError("bundle destination escaped capture root")
        if os.path.lexists(destination) and destination.is_symlink():
            raise StagingError("bundle destination must not be a symlink")
        try:
            return os.open(self.capture_root, os.O_RDONLY | O_DIRECTORY | getattr(os, "O_NOFOLLOW", 0))
        except OSError as error:
            raise StagingError("capture publication destination cannot be inspected") from error

    def file_size(self, path: Path) -> int:
        """Read one evidence size through the filesystem boundary."""
        return _file_size(path)


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


def _write_lock_metadata(fd: int, operation: str, acquired_monotonic_ns: int) -> str:
    """Best-effort owner metadata; lock semantics never depend on this write."""
    metadata = {
        "version": _LOCK_METADATA_VERSION,
        "pid": os.getpid(),
        "process_start": _process_start(),
        "thread_id": get_ident(),
        "operation": operation,
        "scope": _LOCK_SCOPE,
        "acquired_at": datetime.now(UTC).isoformat(timespec="milliseconds"),
        "acquired_monotonic_ns": acquired_monotonic_ns,
    }
    payload = _json_bytes(metadata)
    if len(payload) > _LOCK_METADATA_MAX_BYTES:
        return "oversized"
    try:
        os.ftruncate(fd, 0)
        written = os.pwrite(fd, payload, 0)
        if written != len(payload):
            return "write_failed"
        os.ftruncate(fd, len(payload))
    except OSError:
        return "write_failed"
    return "valid"


def _read_lock_context(fd: int, requested_operation: str) -> LockContext:
    metadata: object = None
    metadata_status = "valid"
    try:
        payload = os.pread(fd, _LOCK_METADATA_MAX_BYTES, 0)
    except OSError:
        metadata_status = "unreadable"
    else:
        if not payload:
            metadata_status = "missing"
        else:
            try:
                metadata = cast(object, loads(payload.decode("utf-8")))
            except JSONDecodeError, UnicodeDecodeError, ValueError:
                metadata_status = "invalid"
    if metadata_status != "valid":
        return LockContext(requested_operation, None, None, None, None, "unknown", metadata_status)
    if not isinstance(metadata, dict) or metadata.get("version") != _LOCK_METADATA_VERSION:
        return LockContext(requested_operation, None, None, None, None, "unknown", "invalid")
    operation = metadata.get("operation")
    pid = metadata.get("pid")
    thread_id = metadata.get("thread_id")
    scope = metadata.get("scope")
    process_start = metadata.get("process_start")
    acquired_monotonic_ns = metadata.get("acquired_monotonic_ns")
    if not (
        isinstance(operation, str)
        and isinstance(pid, int)
        and not isinstance(pid, bool)
        and pid > 0
        and isinstance(thread_id, int)
        and not isinstance(thread_id, bool)
        and thread_id > 0
        and isinstance(scope, str)
        and isinstance(acquired_monotonic_ns, int)
        and not isinstance(acquired_monotonic_ns, bool)
        and acquired_monotonic_ns > 0
    ):
        return LockContext(requested_operation, None, None, None, None, "unknown", "invalid")
    age_seconds = max(0.0, (time.monotonic_ns() - acquired_monotonic_ns) / 1_000_000_000)
    holder_scope = _holder_scope(pid, process_start)
    if holder_scope == "unknown":
        return LockContext(requested_operation, None, None, None, None, "unknown", "stale")
    return LockContext(
        requested_operation,
        operation,
        pid,
        thread_id,
        age_seconds,
        holder_scope,
        "valid",
    )


def _holder_scope(holder_pid: int, holder_start: object) -> str:
    if holder_pid != os.getpid():
        current_start = _process_start(holder_pid)
        if not isinstance(holder_start, int) or current_start is None or holder_start != current_start:
            return "unknown"
        return "other_process"
    current_start = _process_start()
    if not isinstance(holder_start, int) or current_start is None:
        return "unknown"
    return "current_process" if holder_start == current_start else "unknown"


def _process_start(pid: int | None = None) -> int | None:
    try:
        stat_path = Path(f"/proc/{pid}/stat") if pid is not None else Path("/proc/self/stat")
        stat_text = stat_path.read_text(encoding="utf-8")
        after_comm = stat_text.rsplit(")", 1)[1].split()
        if pid is not None and after_comm[0] == "Z":
            return None
        return int(after_comm[19])
    except OSError, IndexError, ValueError:
        return None


def _clear_lock_metadata(fd: int) -> None:
    with suppress(OSError):
        os.ftruncate(fd, 0)


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


def _files_equal_prefix(
    first: Path,
    second: Path,
    size: int,
    *,
    chunk_size: int = DEFAULT_CONFIG.durability.io_chunk_bytes,
) -> bool:
    """Compare one bounded prefix without materializing it in memory."""
    if size < 0 or _file_size(first) != size or _file_size(second) < size:
        return False
    with first.open("rb") as left, second.open("rb") as right:
        remaining = size
        while remaining:
            chunk = left.read(min(chunk_size, remaining))
            if not chunk or chunk != right.read(len(chunk)):
                return False
            remaining -= len(chunk)
    return True


def _copy_prefix_synced(
    source: Path,
    destination: Path,
    size: int,
    sync: Fsync,
    *,
    chunk_size: int = DEFAULT_CONFIG.durability.io_chunk_bytes,
) -> None:
    """Copy one bounded prefix using fixed-size buffers and fsync the result."""
    if size < 0:
        raise AttemptStateError("prefix size must be non-negative")
    with source.open("rb") as input_file, destination.open("xb") as output_file:
        remaining = size
        while remaining:
            chunk = input_file.read(min(chunk_size, remaining))
            if not chunk:
                raise AttemptStateError("streaming raw file ended before prefix")
            output_file.write(chunk)
            remaining -= len(chunk)
        output_file.flush()
        sync(output_file.fileno())


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
