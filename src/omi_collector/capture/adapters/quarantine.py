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

from ..domain.quarantine_machine import (
    QuarantineAction,
    QuarantineEvent,
    QuarantineState,
)
from ..domain.quarantine_machine import (
    transition as quarantine_transition,
)
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
    _TERMINAL_RETIRED_NAME,
    _TERMINAL_RETIREMENT_VERSION,
    _UNPROCESSABLE_QUARANTINE_NAME,
    _UNPROCESSABLE_QUARANTINE_STATE,
    _UUID_HEX_LENGTH,
    AttemptDescriptor,
    AttemptStateError,
    CollisionError,
    MaintenanceDeferredError,
    PendingAttemptError,
    StagingError,
    _validate_attempt_id,
    _validate_terminalized_at,
)
from .staging_filesystem import (
    DeviceLock,
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


def quarantine_pending(filesystem: StagingFilesystem, reason: str) -> tuple[Path, ...]:
    """Move blocking partial evidence aside without inspecting its contents further.

    The collector lease makes this operation mutually exclusive with a live
    collector. Valid unpublished attempts and opaque entries are moved while
    published evidence remains in place.
    """
    if not isinstance(reason, str) or not reason.strip():
        raise AttemptStateError("quarantine reason must be a non-empty string")

    with filesystem.device_lock(operation="quarantine_pending"):
        root_candidate = _quarantine_attempts_root(filesystem, reason)
        if root_candidate is not None:
            return (root_candidate,)
        candidates = _quarantine_candidates(filesystem)
        if not candidates:
            return ()
        destination_root = filesystem.quarantine_root
        _ensure_quarantine_directory(filesystem, destination_root)
        return tuple(
            _move_to_quarantine(filesystem, destination_root, entry, reason, opaque=opaque)
            for entry, opaque in candidates
        )


def quarantine_attempt_source(filesystem: StagingFilesystem, attempt_id: str) -> Path:
    """Move one preserved attempt source without adding diagnostic metadata."""
    _validate_attempt_id(attempt_id)
    with filesystem.device_lock(operation="quarantine_attempt_source"):
        path = filesystem.attempts_root / attempt_id
        descriptor = filesystem._read_descriptor(path)
        if is_nonblocking_attempt(filesystem, path, descriptor):
            raise AttemptStateError("attempt source is not an active partial")
        destination_root = filesystem.quarantine_root
        _ensure_quarantine_directory(filesystem, destination_root)
        destination = _quarantine_source_path(destination_root, path.name)
        path.replace(destination)
        _sync_directory(destination.parent, filesystem._fsync)
        _sync_directory(filesystem.attempts_root, filesystem._fsync)
        return destination


def terminalize_prefix_attempt(filesystem: StagingFilesystem, attempt_id: str) -> None:
    """Atomically mark one closed prefix publication as permanently retired.

    The recoverable prefix marker remains in place. The new marker is a
    distinct state-machine stage that wins admission independently of the
    published capture directory.
    """
    _validate_attempt_id(attempt_id)
    with filesystem.device_lock(operation="terminalize_prefix_attempt"):
        _terminalize_prefix_attempt_held(filesystem, attempt_id)


def terminalize_prefix_attempt_held(filesystem: StagingFilesystem, attempt_id: str, held_lease: DeviceLock) -> None:
    """Terminalize one prefix while the caller retains the active spool lease."""
    _validate_attempt_id(attempt_id)
    filesystem.require_device_lock(held_lease)
    _terminalize_prefix_attempt_held(filesystem, attempt_id)


def _terminalize_prefix_attempt_held(filesystem: StagingFilesystem, attempt_id: str) -> None:
    path = filesystem.attempts_root / attempt_id
    descriptor = filesystem._read_descriptor(path)
    if _is_terminal_retired_attempt(path):
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
    _published_prefix(path, descriptor, filesystem, io_chunk_bytes=filesystem._durability.io_chunk_bytes)
    terminalized_at_unix_ns = _wall_clock_ns()
    _validate_terminalized_at(terminalized_at_unix_ns)
    terminal_marker = path / _TERMINAL_RETIRED_NAME
    try:
        _sync_directory(path, filesystem._fsync)
        filesystem._write_json_atomic(
            terminal_marker,
            TerminalRetirementEvidence(terminalized_at_unix_ns).as_dict(),
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
    *,
    should_defer: Callable[[], bool] = _never_defer,
) -> tuple[Path, ...]:
    """Delete only aged terminal-retired partial directories."""
    retention_ns = _seconds_to_nanoseconds(filesystem._terminal_retention_seconds)
    now_unix_ns = _wall_clock_ns()
    _validate_terminalized_at(now_unix_ns)
    removed: list[Path] = []
    with filesystem.device_lock(operation="sweep_terminal_retired"):
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
                    now_unix_ns,
                    retention_ns,
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


def _terminal_retired_expired(
    filesystem: StagingFilesystem,
    entry: Path,
    now_unix_ns: int,
    retention_ns: int,
) -> bool:
    mode = entry.lstat().st_mode
    if stat.S_ISLNK(mode) or not stat.S_ISDIR(mode):
        return False
    filesystem._read_descriptor(entry)
    terminalized_at = _terminal_retired_at(entry)
    return terminalized_at is not None and now_unix_ns - terminalized_at >= retention_ns


def quarantined_attempts(
    filesystem: StagingFilesystem,
    *,
    should_defer: Callable[[], bool] = _never_defer,
) -> tuple[Path, ...]:
    """Return regular quarantined attempt directories without creating roots."""
    quarantine_root = filesystem.quarantine_root
    try:
        mode = quarantine_root.lstat().st_mode
    except FileNotFoundError:
        return ()
    except OSError as error:
        raise StagingError("quarantine cannot be inspected") from error
    if stat.S_ISLNK(mode) or not stat.S_ISDIR(mode):
        raise StagingError("quarantine root is not a regular directory")
    try:
        entries = tuple(quarantine_root.iterdir())
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
        action = quarantine_transition(_quarantine_state(entry), QuarantineEvent.INSPECT).action
        if action not in {QuarantineAction.SALVAGE, QuarantineAction.REAUTHENTICATE}:
            continue
        result.append(entry)
    return tuple(result)


def mark_quarantine_published(filesystem: StagingFilesystem, source: Path) -> None:
    """Mark an automatically published quarantine source for delayed deletion."""
    _mark_quarantine(
        filesystem,
        source,
        _PUBLISHED_QUARANTINE_NAME,
        {
            "version": _TERMINAL_RETIREMENT_VERSION,
            "state": _PUBLISHED_QUARANTINE_STATE,
            "published_at_unix_ns": _wall_clock_ns(),
        },
    )


def mark_quarantine_unprocessable(filesystem: StagingFilesystem, source: Path, reason: str) -> None:
    """Terminally classify evidence that cannot authenticate a publishable prefix."""
    if not reason:
        raise AttemptStateError("unprocessable reason must be non-empty")
    _mark_quarantine(
        filesystem,
        source,
        _UNPROCESSABLE_QUARANTINE_NAME,
        {
            "version": _TERMINAL_RETIREMENT_VERSION,
            "state": _UNPROCESSABLE_QUARANTINE_STATE,
            "classified_at_unix_ns": _wall_clock_ns(),
            "reason": reason,
        },
    )


def _mark_quarantine(filesystem: StagingFilesystem, source: Path, marker_name: str, payload: dict[str, object]) -> None:
    with filesystem.device_lock(operation="mark_quarantine"):
        root = filesystem.quarantine_root
        try:
            relative = Path(source).absolute().relative_to(root)
        except ValueError as error:
            raise AttemptStateError("quarantine source escapes quarantine root") from error
        if len(relative.parts) != 1:
            raise AttemptStateError("quarantine source is not an immediate child")
        _require_regular_directory(source)
        if any(
            os.path.lexists(source / existing_name)
            for existing_name in (_PUBLISHED_QUARANTINE_NAME, _UNPROCESSABLE_QUARANTINE_NAME)
        ):
            raise AttemptStateError("quarantine terminal evidence already exists")
        filesystem._write_json_atomic(source / marker_name, payload)


def sweep_terminal_quarantine(
    filesystem: StagingFilesystem,
    *,
    should_defer: Callable[[], bool] = _never_defer,
) -> tuple[Path, ...]:
    """Classify unsafe entries and delete aged terminal quarantine evidence."""
    if should_defer():
        return ()
    retention_ns = _seconds_to_nanoseconds(filesystem._terminal_retention_seconds)
    now_unix_ns = _wall_clock_ns()
    removed: list[Path] = []
    with filesystem.device_lock(operation="sweep_terminal_quarantine"):
        root = filesystem.quarantine_root
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
            if _sweep_terminal_quarantine_entry(filesystem, root, entry, now_unix_ns, retention_ns):
                removed.append(entry)
    return tuple(removed)


def _sweep_terminal_quarantine_entry(
    filesystem: StagingFilesystem,
    root: Path,
    entry: Path,
    now_unix_ns: int,
    retention_ns: int,
) -> bool:
    if _is_quarantine_sidecar(root, entry):
        return False
    state = _quarantine_state(entry)
    if state not in {QuarantineState.PUBLISHED, QuarantineState.UNPROCESSABLE}:
        _classify_unsafe_quarantine_entry(filesystem, entry, now_unix_ns)
        return False
    terminalized_at = _terminal_quarantine_at(entry)
    if terminalized_at is None or now_unix_ns - terminalized_at < retention_ns:
        return False
    if quarantine_transition(state, QuarantineEvent.RETENTION_EXPIRED).action is not QuarantineAction.DELETE:
        return False
    _remove_terminal_quarantine_entry(entry)
    _sync_directory(root, filesystem._fsync)
    return True


def _remove_terminal_quarantine_entry(entry: Path) -> None:
    mode = entry.lstat().st_mode
    if stat.S_ISDIR(mode) and not stat.S_ISLNK(mode):
        shutil.rmtree(entry)
        with suppress(FileNotFoundError):
            entry.with_name(f"{entry.name}.json").unlink()
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


def _terminal_quarantine_at(entry: Path) -> int | None:
    """Return the retention timestamp for published or unprocessable evidence."""
    try:
        mode = entry.lstat().st_mode
    except OSError:
        return None
    if stat.S_ISLNK(mode) or not stat.S_ISDIR(mode):
        marker_paths = (entry.with_name(f"{entry.name}.json"),)
    else:
        marker_paths = (
            entry / _PUBLISHED_QUARANTINE_NAME,
            entry / _UNPROCESSABLE_QUARANTINE_NAME,
            entry.with_name(f"{entry.name}.json"),
        )
    for marker_path in marker_paths:
        if marker_path == entry / _PUBLISHED_QUARANTINE_NAME:
            expected_state = _PUBLISHED_QUARANTINE_STATE
        else:
            expected_state = _UNPROCESSABLE_QUARANTINE_STATE
        timestamp = _terminal_marker_at(
            marker_path,
            expected_state=expected_state,
            sidecar_entry=entry if marker_path == entry.with_name(f"{entry.name}.json") else None,
        )
        if timestamp is not None:
            return timestamp
    return None


def _quarantine_state(entry: Path) -> QuarantineState:
    """Classify marker evidence; malformed or contradictory markers stay live."""
    try:
        mode = entry.lstat().st_mode
    except OSError:
        return QuarantineState.INVALID_EVIDENCE
    if stat.S_ISLNK(mode) or not stat.S_ISDIR(mode):
        return _opaque_quarantine_state(entry)
    return _directory_quarantine_state(entry)


def _opaque_quarantine_state(entry: Path) -> QuarantineState:
    marker = entry.with_name(f"{entry.name}.json")
    valid = _terminal_marker_at(
        marker,
        expected_state=_UNPROCESSABLE_QUARANTINE_STATE,
        sidecar_entry=entry,
    )
    if valid is not None:
        return QuarantineState.UNPROCESSABLE
    return QuarantineState.INVALID_EVIDENCE


def _directory_quarantine_state(entry: Path) -> QuarantineState:
    published_path = entry / _PUBLISHED_QUARANTINE_NAME
    unprocessable_path = entry / _UNPROCESSABLE_QUARANTINE_NAME
    sidecar_path = entry.with_name(f"{entry.name}.json")
    published = _terminal_marker_at(published_path, expected_state=_PUBLISHED_QUARANTINE_STATE)
    unprocessable = _terminal_marker_at(unprocessable_path, expected_state=_UNPROCESSABLE_QUARANTINE_STATE)
    sidecar = _terminal_marker_at(
        sidecar_path,
        expected_state=_UNPROCESSABLE_QUARANTINE_STATE,
        sidecar_entry=entry,
    )
    has_published = os.path.lexists(published_path)
    has_unprocessable = os.path.lexists(unprocessable_path)
    has_sidecar = os.path.lexists(sidecar_path)
    if (has_sidecar and sidecar is None) or (sidecar is not None and (has_published or has_unprocessable)):
        return QuarantineState.INVALID_EVIDENCE
    if sidecar is not None:
        return QuarantineState.UNPROCESSABLE
    if published is not None and not has_unprocessable:
        return QuarantineState.PUBLISHED
    if unprocessable is not None and not has_published:
        return QuarantineState.UNPROCESSABLE
    if has_published or has_unprocessable:
        return QuarantineState.INVALID_EVIDENCE
    return QuarantineState.RETRYABLE


def _terminal_marker_at(
    path: Path,
    *,
    expected_state: str,
    sidecar_entry: Path | None = None,
) -> int | None:
    """Read a terminal marker only when it is a regular, non-symlink file."""
    try:
        _require_regular_file(path, "quarantine terminal marker")
        marker = _read_json(path)
    except OSError, StagingError:
        return None
    version = marker.get("version")
    valid = isinstance(version, int) and not isinstance(version, bool) and version == _TERMINAL_RETIREMENT_VERSION
    state = marker.get("state")
    published_marker = state == expected_state == _PUBLISHED_QUARANTINE_STATE and set(marker) == {
        "version",
        "state",
        "published_at_unix_ns",
    }
    unprocessable_marker = (
        state == expected_state == _UNPROCESSABLE_QUARANTINE_STATE
        and frozenset(marker)
        in {
            frozenset({"version", "state", "classified_at_unix_ns", "reason"}),
            frozenset({"version", "state", "classified_at_unix_ns", "reason", "original_name"}),
        }
        and isinstance(marker.get("reason"), str)
        and bool(marker["reason"])
    )
    if "original_name" in marker and not isinstance(marker["original_name"], str):
        return None
    original_name = marker.get("original_name")
    if original_name is not None and (not isinstance(original_name, str) or not original_name):
        return None
    if sidecar_entry is not None and not _sidecar_name_matches(sidecar_entry.name, original_name):
        return None
    if not valid or not (published_marker or unprocessable_marker):
        return None
    timestamp_key = "published_at_unix_ns" if published_marker else "classified_at_unix_ns"
    candidate = marker.get(timestamp_key)
    return candidate if isinstance(candidate, int) and not isinstance(candidate, bool) and candidate >= 0 else None


def _sidecar_name_matches(entry_name: str, original_name: object) -> bool:
    if not isinstance(original_name, str) or not original_name:
        return False
    if entry_name == original_name:
        return True
    if not entry_name.startswith(f"{original_name}-"):
        return False
    suffix = entry_name[len(original_name) + 1 :]
    return len(suffix) == _UUID_HEX_LENGTH and all(character in "0123456789abcdef" for character in suffix)


def _terminal_retired_at(path: Path) -> int | None:
    marker_path = path / _TERMINAL_RETIRED_NAME
    if not marker_path.exists():
        return None
    try:
        _require_regular_file(marker_path, "terminal-retired marker")
        recoverable_marker_path = path / _PREFIX_PUBLICATION_NAME
        _require_regular_file(recoverable_marker_path, "recoverable prefix publication marker")
        recoverable_marker = PrefixPublicationEvidence.from_json(_read_json(recoverable_marker_path))
        if not _recoverable_prefix_marker_matches(
            recoverable_marker,
        ):
            return None
        return _terminal_retired_marker_matches(TerminalRetirementEvidence.from_json(_read_json(marker_path)))
    except OSError, StagingError, AttemptStateError:
        return None


def _has_terminal_retirement_marker(path: Path) -> bool:
    return os.path.lexists(path / _TERMINAL_RETIRED_NAME)


def _is_terminal_retired_attempt(path: Path) -> bool:
    return _terminal_retired_at(path) is not None


def is_nonblocking_attempt(filesystem: StagingFilesystem, path: Path, descriptor: AttemptDescriptor) -> bool:
    if _has_terminal_retirement_marker(path):
        return _is_terminal_retired_attempt(path)
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


def assert_no_pending(filesystem: StagingFilesystem) -> None:
    """Fail closed when preserved partial evidence exists."""
    if pending_attempts(filesystem):
        raise PendingAttemptError("partial staging evidence blocks another READ")


def _quarantine_candidates(filesystem: StagingFilesystem) -> tuple[tuple[Path, bool], ...]:
    try:
        if not os.path.lexists(filesystem.attempts_root):
            return ()
        if filesystem.attempts_root.is_symlink() or not filesystem.attempts_root.is_dir():
            raise PendingAttemptError("partial staging root is not a directory")
        entries = tuple(filesystem.attempts_root.iterdir())
    except OSError as error:
        raise PendingAttemptError("partial staging cannot be inspected") from error

    candidates: list[tuple[Path, bool]] = []
    for entry in entries:
        descriptor = _inspect_pending_entry(filesystem, entry)
        if descriptor is None or not is_nonblocking_attempt(filesystem, entry, descriptor):
            candidates.append((entry, descriptor is None))
    return tuple(candidates)


def _inspect_pending_entry(filesystem: StagingFilesystem, entry: Path) -> AttemptDescriptor | None:
    """Read one entry without following symlinks; ``None`` means opaque evidence."""
    try:
        mode = entry.lstat().st_mode
        if stat.S_ISLNK(mode) or not stat.S_ISDIR(mode):
            return None
        return filesystem._read_descriptor(entry)
    except OSError, StagingError:
        return None


def _quarantine_attempts_root(filesystem: StagingFilesystem, reason: str) -> Path | None:
    """Move an unsafe attempts root itself, then recreate a directory."""
    try:
        mode = filesystem.attempts_root.lstat().st_mode
    except FileNotFoundError:
        return None
    except OSError as error:
        raise PendingAttemptError("partial staging cannot be inspected") from error
    if stat.S_ISDIR(mode) and not stat.S_ISLNK(mode):
        return None

    destination_root = filesystem.quarantine_root
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


def _move_to_quarantine(filesystem: StagingFilesystem, root: Path, entry: Path, reason: str, *, opaque: bool) -> Path:
    if opaque:
        destination, sidecar = _quarantine_paths(root, entry.name)
    else:
        destination = _quarantine_source_path(root, entry.name)
        sidecar = None
    entry.replace(destination)
    _sync_directory(destination.parent, filesystem._fsync)
    _sync_directory(filesystem.attempts_root, filesystem._fsync)
    if sidecar is not None:
        filesystem._write_json_atomic(
            sidecar,
            {
                "version": _TERMINAL_RETIREMENT_VERSION,
                "state": _UNPROCESSABLE_QUARANTINE_STATE,
                "classified_at_unix_ns": _wall_clock_ns(),
                "reason": reason,
                "original_name": entry.name,
            },
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


def pending_attempts(filesystem: StagingFilesystem) -> tuple[AttemptDescriptor, ...]:
    """Return valid, non-published partial attempts."""
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
        descriptor = _inspect_pending_entry(filesystem, entry)
        if descriptor is None:
            raise PendingAttemptError("malformed partial attempt evidence blocks resume")
        if not is_nonblocking_attempt(filesystem, entry, descriptor):
            result.append(descriptor)
    return tuple(result)


def quarantine_capture_temporary(
    filesystem: StagingFilesystem,
    temporary: Path,
    capture_root: Path,
    reason: str,
) -> None:
    """Move one unsafe capture temporary into quarantine with retryable evidence."""
    destination_root = filesystem.quarantine_root
    _ensure_quarantine_directory(filesystem, destination_root)
    destination, sidecar = _quarantine_paths(destination_root, f"capture-temporary-{temporary.name}")
    temporary.replace(destination)
    _sync_directory(destination.parent, filesystem._fsync)
    _sync_directory(capture_root, filesystem._fsync)
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
