"""Quarantine admission, retention, and terminal evidence lifecycle."""

from __future__ import annotations

import os
import shutil
import stat
import time
from collections.abc import Callable
from contextlib import suppress
from pathlib import Path
from uuid import uuid4

from .publication import (
    PrefixPublicationEvidence,
    TerminalRetirementEvidence,
    _prefix_publication_matches,
    _recoverable_prefix_marker_matches,
    _terminal_retired_marker_matches,
    is_published_attempt,
)
from .recovery import _published_prefix
from .staging_contract import (
    _MANIFEST_NAME,
    _PREFIX_PUBLICATION_NAME,
    _PUBLISHED_QUARANTINE_NAME,
    _PUBLISHED_QUARANTINE_STATE,
    _RAW_NAME,
    _RECEIPT_NAME,
    _SALVAGE_PENDING_NAME,
    _TERMINAL_RETIRED_NAME,
    _TERMINAL_RETIREMENT_VERSION,
    _UNPROCESSABLE_QUARANTINE_NAME,
    _UNPROCESSABLE_QUARANTINE_STATE,
    AttemptDescriptor,
    AttemptStateError,
    CollisionError,
    MaintenanceDeferredError,
    PendingAttemptError,
    StagingError,
    _validate_attempt_id,
    _validate_slug,
    _validate_terminalized_at,
)
from .staging_filesystem import (
    StagingFilesystem,
    _never_defer,
    _read_json,
    _require_regular_directory,
    _require_regular_file,
    _sync_directory,
)


def _wall_clock_ns() -> int:
    return time.time_ns()


def _seconds_to_nanoseconds(seconds: float) -> int:
    return int(seconds * 1_000_000_000)


def quarantine_pending(filesystem: StagingFilesystem, device_slug: str, reason: str) -> tuple[Path, ...]:
    """Move blocking partial evidence aside without inspecting its contents further.

    The device lease makes this operation mutually exclusive with a live
    collector.  Only valid, unpublished attempts and safely attributable
    malformed attempts for ``device_slug`` are moved; unattributed, unsafe,
    published, and other-device evidence remains in place.
    """
    _validate_slug(device_slug)
    if not isinstance(reason, str) or not reason.strip():
        raise AttemptStateError("quarantine reason must be a non-empty string")

    with filesystem.device_lock(device_slug):
        root_candidate = _quarantine_attempts_root(filesystem, device_slug, reason)
        if root_candidate is not None:
            return (root_candidate,)
        candidates = _quarantine_candidates(filesystem, device_slug)
        if not candidates:
            return ()
        destination_root = filesystem.quarantine_root / device_slug
        _ensure_quarantine_directory(filesystem, destination_root.parent)
        _ensure_quarantine_directory(filesystem, destination_root)
        return tuple(_move_to_quarantine(filesystem, destination_root, entry, reason) for entry in candidates)


def quarantine_attempt_source(filesystem: StagingFilesystem, device_slug: str, attempt_id: str) -> Path:
    """Move one preserved attempt source without adding diagnostic metadata."""
    _validate_slug(device_slug)
    _validate_attempt_id(attempt_id)
    with filesystem.device_lock(device_slug):
        path = filesystem.attempts_root / attempt_id
        descriptor = filesystem._read_descriptor(path)
        if descriptor.device_slug != device_slug or is_nonblocking_attempt(filesystem, path, descriptor):
            raise AttemptStateError("attempt source is not an active partial")
        destination_root = filesystem.quarantine_root / device_slug
        _ensure_quarantine_directory(filesystem, destination_root.parent)
        _ensure_quarantine_directory(filesystem, destination_root)
        destination = _quarantine_source_path(destination_root, path.name)
        path.replace(destination)
        _sync_directory(destination.parent, filesystem._fsync)
        _sync_directory(filesystem.attempts_root, filesystem._fsync)
        return destination


def terminalize_prefix_attempt(filesystem: StagingFilesystem, device_slug: str, attempt_id: str) -> None:
    """Atomically mark one closed prefix publication as permanently retired.

    The recoverable prefix marker remains in place. The new marker is a
    distinct state-machine stage that wins admission independently of the
    published capture directory.
    """
    _validate_slug(device_slug)
    _validate_attempt_id(attempt_id)
    with filesystem.device_lock(device_slug):
        path = filesystem.attempts_root / attempt_id
        descriptor = filesystem._read_descriptor(path)
        if descriptor.device_slug != device_slug:
            raise AttemptStateError("attempt source belongs to another device")
        if _is_terminal_retired_attempt(filesystem, path, descriptor):
            return
        if _has_terminal_retirement_marker(path):
            raise AttemptStateError("terminal-retired marker is invalid")
        prefix_marker = path / _PREFIX_PUBLICATION_NAME
        _require_regular_file(prefix_marker, "recoverable prefix publication marker")
        marker = PrefixPublicationEvidence.from_json(_read_json(prefix_marker))
        if not _prefix_publication_matches(
            path,
            descriptor,
            marker,
            filesystem=filesystem,
            io_chunk_bytes=filesystem._durability.io_chunk_bytes,
        ):
            raise AttemptStateError("recoverable prefix publication marker is invalid")
        prefix = _published_prefix(path, descriptor, filesystem, io_chunk_bytes=filesystem._durability.io_chunk_bytes)
        terminalized_at_unix_ns = _wall_clock_ns()
        _validate_terminalized_at(terminalized_at_unix_ns)
        terminal_marker = path / _TERMINAL_RETIRED_NAME
        try:
            _sync_directory(path, filesystem._fsync)
            filesystem._write_json_atomic(
                terminal_marker,
                TerminalRetirementEvidence(
                    descriptor.attempt_id,
                    prefix,
                    terminalized_at_unix_ns,
                ).as_dict(),
            )
            _sync_directory(path, filesystem._fsync)
        except BaseException:
            with suppress(OSError):
                terminal_marker.unlink()
            with suppress(OSError):
                _sync_directory(path, filesystem._fsync)
            raise


def sweep_terminal_retired(
    filesystem: StagingFilesystem,
    device_slug: str,
    *,
    should_defer: Callable[[], bool] = _never_defer,
) -> tuple[Path, ...]:
    """Delete only aged terminal-retired partial directories for one device."""
    _validate_slug(device_slug)
    retention_ns = _seconds_to_nanoseconds(filesystem._terminal_retention_seconds)
    now_unix_ns = _wall_clock_ns()
    _validate_terminalized_at(now_unix_ns)
    removed: list[Path] = []
    with filesystem.device_lock(device_slug):
        try:
            if not os.path.lexists(filesystem.attempts_root):
                return ()
            if filesystem.attempts_root.is_symlink() or not filesystem.attempts_root.is_dir():
                raise StagingError("terminal-retired partial root is not a directory")
            entries = tuple(filesystem.attempts_root.iterdir())
        except OSError as error:
            raise StagingError("terminal-retired partials cannot be inspected") from error
        for entry in entries:
            if should_defer():
                break
            try:
                expired = _terminal_retired_expired(
                    filesystem,
                    entry,
                    device_slug,
                    now_unix_ns,
                    retention_ns,
                    should_defer=should_defer,
                )
            except MaintenanceDeferredError:
                break
            except OSError, StagingError:
                continue
            if not expired:
                continue
            if should_defer():
                break
            shutil.rmtree(entry)
            _sync_directory(filesystem.attempts_root, filesystem._fsync)
            removed.append(entry)
    return tuple(removed)


def _terminal_retired_expired(  # noqa: PLR0913
    filesystem: StagingFilesystem,
    entry: Path,
    device_slug: str,
    now_unix_ns: int,
    retention_ns: int,
    *,
    should_defer: Callable[[], bool],
) -> bool:
    mode = entry.lstat().st_mode
    if stat.S_ISLNK(mode) or not stat.S_ISDIR(mode):
        return False
    descriptor = filesystem._read_descriptor(entry)
    terminalized_at = _terminal_retired_at(
        filesystem,
        entry,
        descriptor,
        should_defer=should_defer,
    )
    return (
        descriptor.device_slug == device_slug
        and terminalized_at is not None
        and now_unix_ns - terminalized_at >= retention_ns
    )


def quarantined_attempts(
    filesystem: StagingFilesystem,
    device_slug: str,
    *,
    should_defer: Callable[[], bool] = _never_defer,
) -> tuple[Path, ...]:
    """Return regular quarantined attempt directories without creating roots."""
    _validate_slug(device_slug)
    device_root = filesystem.quarantine_root / device_slug
    try:
        mode = device_root.lstat().st_mode
    except FileNotFoundError:
        return ()
    except OSError as error:
        raise StagingError("quarantine cannot be inspected") from error
    if stat.S_ISLNK(mode) or not stat.S_ISDIR(mode):
        raise StagingError("quarantine device root is not a regular directory")
    try:
        entries = tuple(device_root.iterdir())
    except OSError as error:
        raise StagingError("quarantine cannot be inspected") from error
    result: list[Path] = []
    for entry in entries:
        if should_defer():
            break
        try:
            mode = entry.lstat().st_mode
        except OSError as error:
            raise StagingError("quarantine entry cannot be inspected") from error
        if stat.S_ISLNK(mode) or not stat.S_ISDIR(mode):
            continue
        # A transient publication marker is diagnostic only.  Its strict
        # prefix must be retried automatically on the next lifecycle pass;
        # only an authenticated terminal classification ends salvage.
        if (entry / _PUBLISHED_QUARANTINE_NAME).exists() or (entry / _UNPROCESSABLE_QUARANTINE_NAME).exists():
            continue
        result.append(entry)
    return tuple(result)


def mark_quarantine_published(filesystem: StagingFilesystem, device_slug: str, source: Path) -> None:
    """Mark an automatically published quarantine source for delayed deletion."""
    _mark_quarantine(
        filesystem,
        source,
        device_slug,
        _PUBLISHED_QUARANTINE_NAME,
        {
            "version": _TERMINAL_RETIREMENT_VERSION,
            "state": _PUBLISHED_QUARANTINE_STATE,
            "published_at_unix_ns": _wall_clock_ns(),
        },
    )


def mark_quarantine_unprocessable(filesystem: StagingFilesystem, device_slug: str, source: Path, reason: str) -> None:
    """Terminally classify evidence that cannot authenticate a publishable prefix."""
    if not reason:
        raise AttemptStateError("unprocessable reason must be non-empty")
    _mark_quarantine(
        filesystem,
        source,
        device_slug,
        _UNPROCESSABLE_QUARANTINE_NAME,
        {
            "version": _TERMINAL_RETIREMENT_VERSION,
            "state": _UNPROCESSABLE_QUARANTINE_STATE,
            "classified_at_unix_ns": _wall_clock_ns(),
            "reason": reason,
        },
    )


def mark_quarantine_salvage_pending(filesystem: StagingFilesystem, device_slug: str, source: Path, reason: str) -> None:
    """Classify valid evidence whose publication may succeed on a later pass."""
    if not reason:
        raise AttemptStateError("salvage reason must be non-empty")
    _mark_quarantine(
        filesystem,
        source,
        device_slug,
        _SALVAGE_PENDING_NAME,
        {
            "version": _TERMINAL_RETIREMENT_VERSION,
            "state": "salvage-pending",
            "last_failure_at_unix_ns": _wall_clock_ns(),
            "reason": reason,
        },
    )


def _mark_quarantine(
    filesystem: StagingFilesystem, source: Path, device_slug: str, marker_name: str, payload: dict[str, object]
) -> None:
    _validate_slug(device_slug)
    with filesystem.device_lock(device_slug):
        root = filesystem.quarantine_root / device_slug
        try:
            relative = Path(source).absolute().relative_to(root)
        except ValueError as error:
            raise AttemptStateError("quarantine source escapes device root") from error
        if len(relative.parts) != 1:
            raise AttemptStateError("quarantine source is not an immediate device child")
        _require_regular_directory(source)
        filesystem._write_json_atomic(source / marker_name, payload)


def sweep_terminal_quarantine(
    filesystem: StagingFilesystem,
    device_slug: str,
    *,
    should_defer: Callable[[], bool] = _never_defer,
) -> tuple[Path, ...]:
    """Classify unsafe entries and delete aged terminal quarantine evidence."""
    _validate_slug(device_slug)
    if should_defer():
        return ()
    retention_ns = _seconds_to_nanoseconds(filesystem._terminal_retention_seconds)
    now_unix_ns = _wall_clock_ns()
    removed: list[Path] = []
    with filesystem.device_lock(device_slug):
        root = filesystem.quarantine_root / device_slug
        try:
            mode = root.lstat().st_mode
        except FileNotFoundError:
            return ()
        except OSError as error:
            raise StagingError("terminal quarantine cannot be inspected") from error
        if stat.S_ISLNK(mode) or not stat.S_ISDIR(mode):
            raise StagingError("terminal quarantine root is not a regular directory")
        for entry in tuple(root.iterdir()):
            if should_defer():
                break
            if _is_quarantine_sidecar(root, entry):
                continue
            published_at = _published_quarantine_at(entry)
            if published_at is None:
                _classify_unsafe_quarantine_entry(filesystem, entry, now_unix_ns)
                continue
            if now_unix_ns - published_at < retention_ns:
                continue
            _remove_terminal_quarantine_entry(entry)
            _sync_directory(root, filesystem._fsync)
            removed.append(entry)
    return tuple(removed)


def _remove_terminal_quarantine_entry(entry: Path) -> None:
    mode = entry.lstat().st_mode
    if stat.S_ISDIR(mode) and not stat.S_ISLNK(mode):
        shutil.rmtree(entry)
    else:
        entry.unlink()
        with suppress(FileNotFoundError):
            entry.with_name(f"{entry.name}.json").unlink()


def _is_quarantine_sidecar(root: Path, entry: Path) -> bool:
    """Leave diagnostic sidecars attached to their evidence entry."""
    if not entry.name.endswith(".json"):
        return False
    return os.path.lexists(root / entry.name.removesuffix(".json"))


def _classify_unsafe_quarantine_entry(filesystem: StagingFilesystem, entry: Path, now_unix_ns: int) -> None:
    """Classify an unopenable quarantine entry without following it."""
    try:
        mode = entry.lstat().st_mode
    except OSError:
        return
    if stat.S_ISDIR(mode) and not stat.S_ISLNK(mode):
        return
    marker = entry.with_name(f"{entry.name}.json")
    if os.path.lexists(marker):
        return
    filesystem._write_json_atomic(
        marker,
        {
            "version": _TERMINAL_RETIREMENT_VERSION,
            "state": _UNPROCESSABLE_QUARANTINE_STATE,
            "classified_at_unix_ns": now_unix_ns,
            "reason": "quarantine entry is not a regular directory",
            "original_name": entry.name,
        },
    )


def _published_quarantine_at(entry: Path) -> int | None:
    published_at: int | None = None
    try:
        mode = entry.lstat().st_mode
        if not stat.S_ISLNK(mode) and stat.S_ISDIR(mode):
            marker = _read_json(entry / _PUBLISHED_QUARANTINE_NAME)
            valid_marker = (
                set(marker) == {"version", "state", "published_at_unix_ns"}
                and marker["version"] == _TERMINAL_RETIREMENT_VERSION
                and marker["state"] == _PUBLISHED_QUARANTINE_STATE
            )
            candidate = marker.get("published_at_unix_ns")
            if valid_marker and isinstance(candidate, int) and not isinstance(candidate, bool) and candidate >= 0:
                published_at = candidate
    except OSError, StagingError, TypeError:
        pass
    return published_at


def _terminal_retired_at(
    filesystem: StagingFilesystem,
    path: Path,
    descriptor: AttemptDescriptor,
    *,
    should_defer: Callable[[], bool] = _never_defer,
) -> int | None:
    marker_path = path / _TERMINAL_RETIRED_NAME
    if not marker_path.exists():
        return None
    try:
        _require_regular_file(marker_path, "terminal-retired marker")
        prefix = _published_prefix(
            path,
            descriptor,
            filesystem=filesystem,
            io_chunk_bytes=filesystem._durability.io_chunk_bytes,
            should_defer=should_defer,
        )
        recoverable_marker_path = path / _PREFIX_PUBLICATION_NAME
        _require_regular_file(recoverable_marker_path, "recoverable prefix publication marker")
        recoverable_marker = PrefixPublicationEvidence.from_json(_read_json(recoverable_marker_path))
        if not _recoverable_prefix_marker_matches(
            recoverable_marker,
            descriptor,
            prefix,
            filesystem.capture_root,
        ):
            return None
        return _terminal_retired_marker_matches(
            TerminalRetirementEvidence.from_json(_read_json(marker_path)), descriptor, prefix
        )
    except OSError, StagingError, AttemptStateError:
        return None


def _has_terminal_retirement_marker(path: Path) -> bool:
    return os.path.lexists(path / _TERMINAL_RETIRED_NAME)


def _is_terminal_retired_attempt(filesystem: StagingFilesystem, path: Path, descriptor: AttemptDescriptor) -> bool:
    return _terminal_retired_at(filesystem, path, descriptor) is not None


def is_nonblocking_attempt(filesystem: StagingFilesystem, path: Path, descriptor: AttemptDescriptor) -> bool:
    if _has_terminal_retirement_marker(path):
        return _is_terminal_retired_attempt(filesystem, path, descriptor)
    return _is_published_attempt(filesystem, path, descriptor)


def _is_published_attempt(filesystem: StagingFilesystem, path: Path, descriptor: AttemptDescriptor) -> bool:
    prefix_marker = path / _PREFIX_PUBLICATION_NAME
    if prefix_marker.exists():
        try:
            marker = PrefixPublicationEvidence.from_json(_read_json(prefix_marker))
            return _prefix_publication_matches(
                path,
                descriptor,
                marker,
                filesystem=filesystem,
                io_chunk_bytes=filesystem._durability.io_chunk_bytes,
            )
        except OSError, StagingError, TypeError:
            return False
    manifest_path = path / _MANIFEST_NAME
    receipt_path = path / _RECEIPT_NAME
    raw_path = path / _RAW_NAME
    if not manifest_path.exists() or not receipt_path.exists() or not raw_path.exists():
        return False
    return is_published_attempt(path, descriptor, filesystem)


def assert_no_pending(filesystem: StagingFilesystem, device_slug: str) -> None:
    """Fail closed when preserved partial evidence exists for ``device_slug``."""
    if pending_attempts(filesystem, device_slug):
        raise PendingAttemptError(f"partial staging evidence blocks another READ for {device_slug}")


def _quarantine_candidates(filesystem: StagingFilesystem, device_slug: str) -> tuple[Path, ...]:
    try:
        if not os.path.lexists(filesystem.attempts_root):
            return ()
        if filesystem.attempts_root.is_symlink() or not filesystem.attempts_root.is_dir():
            raise PendingAttemptError("partial staging root is not a directory")
        entries = tuple(filesystem.attempts_root.iterdir())
    except OSError as error:
        raise PendingAttemptError("partial staging cannot be inspected") from error

    candidates: list[Path] = []
    for entry in entries:
        descriptor, malformed = _inspect_pending_entry(filesystem, entry, device_slug)
        if malformed or (
            descriptor is not None
            and descriptor.device_slug == device_slug
            and not is_nonblocking_attempt(filesystem, entry, descriptor)
        ):
            candidates.append(entry)
    return tuple(candidates)


def _inspect_pending_entry(
    filesystem: StagingFilesystem, entry: Path, device_slug: str
) -> tuple[AttemptDescriptor | None, bool]:
    """Inspect one entry, reporting an attributable malformed descriptor."""
    try:
        mode = entry.lstat().st_mode
        if stat.S_ISLNK(mode) or not stat.S_ISDIR(mode):
            raise PendingAttemptError("partial staging evidence is not a regular directory")
        return filesystem._read_descriptor(entry), False
    except PendingAttemptError:
        raise
    except (OSError, StagingError) as error:
        attributed_slug = _descriptor_device_slug(entry)
        if attributed_slug == device_slug:
            return None, True
        if attributed_slug is not None:
            return None, False
        raise PendingAttemptError("partial attempt evidence cannot be attributed safely") from error


def _descriptor_device_slug(entry: Path) -> str | None:
    """Read only enough malformed metadata to establish safe ownership."""
    try:
        _require_regular_file(entry / "attempt.json", "attempt descriptor")
        raw = _read_json(entry / "attempt.json")
    except OSError, StagingError:
        return None
    value = raw.get("device_slug")
    if not isinstance(value, str):
        return None
    try:
        _validate_slug(value)
    except AttemptStateError:
        return None
    return value


def _quarantine_attempts_root(filesystem: StagingFilesystem, device_slug: str, reason: str) -> Path | None:
    """Move an unsafe attempts root itself, then recreate a directory."""
    try:
        mode = filesystem.attempts_root.lstat().st_mode
    except FileNotFoundError:
        return None
    except OSError as error:
        raise PendingAttemptError("partial staging cannot be inspected") from error
    if stat.S_ISDIR(mode) and not stat.S_ISLNK(mode):
        return None

    destination_root = filesystem.quarantine_root / device_slug
    _ensure_quarantine_directory(filesystem, destination_root.parent)
    _ensure_quarantine_directory(filesystem, destination_root)
    destination, sidecar = _quarantine_paths(destination_root, filesystem.attempts_root.name)
    filesystem.attempts_root.replace(destination)
    _sync_directory(destination.parent, filesystem._fsync)
    filesystem._ensure_directory(filesystem.attempts_root)
    filesystem._write_json_atomic(
        sidecar,
        {
            "version": _TERMINAL_RETIREMENT_VERSION,
            "state": _UNPROCESSABLE_QUARANTINE_STATE,
            "classified_at_unix_ns": _wall_clock_ns(),
            "reason": reason,
            "original_name": filesystem.attempts_root.name,
        },
    )
    return destination


def _move_to_quarantine(filesystem: StagingFilesystem, root: Path, entry: Path, reason: str) -> Path:
    destination, sidecar = _quarantine_paths(root, entry.name)
    entry.replace(destination)
    _sync_directory(destination.parent, filesystem._fsync)
    _sync_directory(filesystem.attempts_root, filesystem._fsync)
    filesystem._write_json_atomic(
        sidecar,
        {"version": 1, "reason": reason, "original_name": entry.name},
    )
    return destination


def _quarantine_paths(root: Path, entry_name: str) -> tuple[Path, Path]:
    """Return collision-free evidence and sidecar paths in ``root``."""
    for _ in range(100):
        destination = root / f"{entry_name}-{uuid4().hex}"
        sidecar = destination.with_name(f"{destination.name}.json")
        if not os.path.lexists(destination) and not os.path.lexists(sidecar):
            return destination, sidecar
    raise CollisionError(f"unable to allocate a quarantine destination for {entry_name}")


def _quarantine_source_path(root: Path, entry_name: str) -> Path:
    """Return a collision-free attempt source path without a sidecar."""
    for _ in range(100):
        destination = root / f"{entry_name}-{uuid4().hex}"
        if not os.path.lexists(destination):
            return destination
    raise CollisionError(f"unable to allocate a quarantine destination for {entry_name}")


def _ensure_quarantine_directory(filesystem: StagingFilesystem, path: Path) -> None:
    """Create a quarantine directory without ever accepting a symlink."""
    try:
        mode = path.lstat().st_mode
    except FileNotFoundError:
        try:
            path.mkdir()
        except FileExistsError:
            _ensure_quarantine_directory(filesystem, path)
            return
        _sync_directory(path.parent, filesystem._fsync)
        _sync_directory(path, filesystem._fsync)
        return
    except OSError as error:
        raise StagingError(f"cannot inspect quarantine directory {path}") from error
    if stat.S_ISLNK(mode) or not stat.S_ISDIR(mode):
        raise StagingError(f"quarantine path is not a directory: {path}")


def pending_attempts(filesystem: StagingFilesystem, device_slug: str) -> tuple[AttemptDescriptor, ...]:
    """Return valid, non-published partial attempts attributed to one device."""
    _validate_slug(device_slug)
    try:
        if not os.path.lexists(filesystem.attempts_root):
            return ()
        if filesystem.attempts_root.is_symlink() or not filesystem.attempts_root.is_dir():
            raise PendingAttemptError("partial staging root is not a directory")
        entries = tuple(filesystem.attempts_root.iterdir())
    except OSError as error:
        raise PendingAttemptError("partial staging cannot be inspected") from error
    result: list[AttemptDescriptor] = []
    for entry in entries:
        descriptor, malformed = _inspect_pending_entry(filesystem, entry, device_slug)
        if malformed:
            raise PendingAttemptError("malformed partial attempt evidence blocks resume")
        if (
            descriptor is not None
            and descriptor.device_slug == device_slug
            and not is_nonblocking_attempt(filesystem, entry, descriptor)
        ):
            result.append(descriptor)
    return tuple(result)


def quarantine_capture_temporary(
    filesystem: StagingFilesystem,
    temporary: Path,
    device_root: Path,
    device_slug: str,
    reason: str,
) -> None:
    """Move one unsafe capture temporary into quarantine with retryable evidence."""
    destination_root = filesystem.quarantine_root / device_slug
    _ensure_quarantine_directory(filesystem, destination_root.parent)
    _ensure_quarantine_directory(filesystem, destination_root)
    destination, sidecar = _quarantine_paths(destination_root, f"capture-temporary-{temporary.name}")
    temporary.replace(destination)
    _sync_directory(destination.parent, filesystem._fsync)
    _sync_directory(device_root, filesystem._fsync)
    payload = {
        "version": _TERMINAL_RETIREMENT_VERSION,
        "state": _UNPROCESSABLE_QUARANTINE_STATE,
        "classified_at_unix_ns": _wall_clock_ns(),
        "reason": reason,
    }
    if destination.is_dir() and not destination.is_symlink():
        filesystem._write_json_atomic(destination / _UNPROCESSABLE_QUARANTINE_NAME, payload)
    else:
        filesystem._write_json_atomic(sidecar, payload | {"original_name": temporary.name})
