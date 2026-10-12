"""Quarantine admission, retention, and terminal evidence lifecycle."""

from __future__ import annotations

import os
import shutil
import stat
import time
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import cast
from uuid import uuid4

from ..domain.quarantine_machine import (
    QuarantineAction,
    QuarantineEvent,
    QuarantineState,
)
from ..domain.quarantine_machine import (
    transition as quarantine_transition,
)
from .collector_state_db import StateDBError, StateKind
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
    _PUBLISHED_QUARANTINE_NAME,
    _PUBLISHED_QUARANTINE_STATE,
    _RAW_NAME,
    _TERMINAL_RETIREMENT_VERSION,
    _UNPROCESSABLE_QUARANTINE_NAME,
    _UNPROCESSABLE_QUARANTINE_STATE,
    _UUID_HEX_LENGTH,
    AttemptDescriptor,
    AttemptStateError,
    CollisionError,
    PendingAttemptError,
    StagingError,
    _descriptor_from_json,
    _validate_attempt_id,
    _validate_descriptor,
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


@dataclass(frozen=True, slots=True)
class _QuarantineMoveIntent:
    source: Path
    destination: Path
    attempt_id: str | None
    opaque: bool
    reason: str


@dataclass(frozen=True, slots=True)
class _MoveSourceLocation:
    """Observed source details needed to replay one interrupted move."""

    name: str
    root: str
    path: Path
    exists: bool
    destination_matches: bool


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
        _begin_quarantine_move(
            filesystem,
            _QuarantineMoveIntent(path, destination, descriptor.attempt_id, False, "attempt source preserved"),
        )
        path.replace(destination)
        _sync_directory(destination.parent, filesystem._fsync)
        _sync_directory(filesystem.attempts_root, filesystem._fsync)
        _finish_quarantine_move(filesystem, destination)
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
    if _is_terminal_retired_attempt(filesystem, path):
        return
    if _has_terminal_retirement_marker(filesystem, path):
        raise AttemptStateError("terminal-retired marker is invalid")
    state = filesystem._read_attempt_row(attempt_id)
    raw_marker = state["prefix_publication"]
    if not isinstance(raw_marker, dict):
        raise AttemptStateError("recoverable prefix publication marker is missing")
    marker = PrefixPublicationEvidence.from_json(raw_marker)
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
    try:
        _sync_directory(path, filesystem._fsync)
        filesystem._write_attempt_field(
            attempt_id, "terminal_retirement", TerminalRetirementEvidence(terminalized_at_unix_ns).as_dict()
        )
        _sync_directory(path, filesystem._fsync)
    except BaseException:
        raise


def sweep_terminal_retired(
    filesystem: StagingFilesystem,
    *,
    ready_root: Path | None = None,
    should_defer: Callable[[], bool] = _never_defer,
) -> tuple[Path, ...]:
    """Remove terminal raw only after its complete payload is durable elsewhere."""
    from .attempt_cleanup import sweep_terminal_redundant

    return sweep_terminal_redundant(filesystem, ready_root, should_defer=should_defer)


def quarantined_attempts(
    filesystem: StagingFilesystem,
    *,
    should_defer: Callable[[], bool] = _never_defer,
) -> tuple[Path, ...]:
    """Return regular quarantined attempt directories without creating roots."""
    active_lease = filesystem._active_lease
    if active_lease is not None:
        filesystem.require_device_lock(active_lease)
        return _quarantined_attempts_held(filesystem, should_defer)
    with filesystem.device_lock(operation="quarantined_attempts"):
        return _quarantined_attempts_held(filesystem, should_defer)


def _quarantined_attempts_held(
    filesystem: StagingFilesystem,
    should_defer: Callable[[], bool],
) -> tuple[Path, ...]:
    quarantine_root = filesystem.quarantine_root
    _reconcile_quarantine_moves(filesystem)
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
        action = quarantine_transition(_quarantine_state(filesystem, entry), QuarantineEvent.INSPECT).action
        if action not in {QuarantineAction.SALVAGE, QuarantineAction.REAUTHENTICATE}:
            continue
        result.append(entry)
    return tuple(result)


def mark_quarantine_published(filesystem: StagingFilesystem, source: Path) -> None:
    """Mark an automatically published quarantine source for delayed deletion."""
    _mark_quarantine(
        filesystem,
        source,
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
        {
            "version": _TERMINAL_RETIREMENT_VERSION,
            "state": _UNPROCESSABLE_QUARANTINE_STATE,
            "classified_at_unix_ns": _wall_clock_ns(),
            "reason": reason,
        },
    )


def _mark_quarantine(filesystem: StagingFilesystem, source: Path, payload: dict[str, object]) -> None:
    with filesystem.device_lock(operation="mark_quarantine"):
        root = filesystem.quarantine_root
        try:
            relative = Path(source).absolute().relative_to(root)
        except ValueError as error:
            raise AttemptStateError("quarantine source escapes quarantine root") from error
        if len(relative.parts) != 1:
            raise AttemptStateError("quarantine source is not an immediate child")
        _require_regular_directory(source)
        row = _quarantine_row(filesystem, source)
        if row["terminal_state"] is not None:
            raise AttemptStateError("quarantine terminal evidence already exists")
        if payload["state"] == _PUBLISHED_QUARANTINE_STATE:
            row.update(terminal_state="published", terminal_at_unix_ns=payload["published_at_unix_ns"])
        else:
            row.update(
                terminal_state="unprocessable",
                terminal_at_unix_ns=payload["classified_at_unix_ns"],
                terminal_reason=payload["reason"],
            )
        _put_quarantine_row(filesystem, source.name, row)


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
        _reconcile_quarantine_moves(filesystem)
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
    state = _quarantine_state(filesystem, entry)
    if state not in {QuarantineState.PUBLISHED, QuarantineState.UNPROCESSABLE}:
        _classify_unsafe_quarantine_entry(filesystem, entry, now_unix_ns)
        return False
    terminalized_at = _terminal_quarantine_at(filesystem, entry)
    if terminalized_at is None or now_unix_ns - terminalized_at < retention_ns:
        return False
    if quarantine_transition(state, QuarantineEvent.RETENTION_EXPIRED).action is not QuarantineAction.DELETE:
        return False
    row = _quarantine_row(filesystem, entry)
    row["deletion_pending"] = True
    _put_quarantine_row(filesystem, entry.name, row)
    _remove_terminal_quarantine_entry(entry)
    _sync_directory(root, filesystem._fsync)
    with filesystem.state_db.transaction() as transaction:
        transaction.delete(StateKind.QUARANTINE, entry.name)
        attempt_id = row["attempt_id"]
        if isinstance(attempt_id, str):
            transaction.delete(StateKind.ATTEMPT, attempt_id)
    return True


def _remove_terminal_quarantine_entry(entry: Path) -> None:
    mode = entry.lstat().st_mode
    if stat.S_ISDIR(mode) and not stat.S_ISLNK(mode):
        shutil.rmtree(entry)
    else:
        entry.unlink()


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
    try:
        row = _quarantine_row(filesystem, entry)
    except AttemptStateError:
        return
    if row["terminal_state"] is not None:
        return
    row.update(
        terminal_state="unprocessable",
        terminal_at_unix_ns=now_unix_ns,
        terminal_reason="quarantine entry is not a regular directory",
    )
    _put_quarantine_row(filesystem, entry.name, row)


_QUARANTINE_FIELDS = frozenset(
    {
        "physical_name",
        "source_name",
        "source_root",
        "original_name",
        "attempt_id",
        "reason",
        "opaque",
        "entry_kind",
        "move_committed",
        "source_device",
        "source_inode",
        "deletion_pending",
        "terminal_state",
        "terminal_at_unix_ns",
        "terminal_reason",
    }
)


def legacy_quarantine_row(path: Path) -> dict[str, object]:
    """Parse one legacy quarantined path, rejecting ambiguous marker state."""
    mode = path.lstat().st_mode
    is_directory = stat.S_ISDIR(mode) and not stat.S_ISLNK(mode)
    sidecar = path.with_name(f"{path.name}.json")
    sidecar_present = os.path.lexists(sidecar)
    marker_state, marker_value = _legacy_quarantine_terminal(path, is_directory, sidecar, sidecar_present)
    attempt_id = _legacy_quarantine_attempt_id(path) if is_directory else None
    if attempt_id is not None:
        from .staging_filesystem import legacy_attempt_row

        legacy_attempt = legacy_attempt_row(path)
        raw_descriptor = legacy_attempt["descriptor"]
        assert isinstance(raw_descriptor, dict)
        if raw_descriptor.get("attempt_id") != attempt_id:
            raise AttemptStateError("legacy quarantine attempt identity is inconsistent")
    original_name = marker_value.get("original_name") if marker_value is not None else attempt_id
    if not isinstance(original_name, str) or not original_name:
        original_name = path.name
    terminal_at = None
    terminal_reason = None
    if marker_state == _PUBLISHED_QUARANTINE_STATE:
        if marker_value is None:
            raise AttemptStateError("legacy quarantine marker is malformed")
        terminal_state = "published"
        terminal_at = marker_value["published_at_unix_ns"]
    elif marker_state == _UNPROCESSABLE_QUARANTINE_STATE:
        if marker_value is None:
            raise AttemptStateError("legacy quarantine marker is malformed")
        terminal_state = "unprocessable"
        terminal_at = marker_value["classified_at_unix_ns"]
        terminal_reason = marker_value["reason"]
    else:
        terminal_state = None
    return {
        "physical_name": path.name,
        "source_name": original_name,
        "source_root": "attempts",
        "original_name": original_name,
        "attempt_id": attempt_id,
        "reason": terminal_reason or "legacy quarantine entry",
        "opaque": sidecar_present,
        "entry_kind": "symlink" if stat.S_ISLNK(mode) else "directory" if is_directory else "file",
        "move_committed": True,
        "source_device": path.lstat().st_dev,
        "source_inode": path.lstat().st_ino,
        "deletion_pending": False,
        "terminal_state": terminal_state,
        "terminal_at_unix_ns": terminal_at,
        "terminal_reason": terminal_reason,
    }


def _legacy_quarantine_terminal(
    path: Path,
    is_directory: bool,
    sidecar: Path,
    sidecar_present: bool,
) -> tuple[str | None, dict[str, object] | None]:
    markers = []
    if is_directory:
        markers.extend(
            (state, path / filename)
            for state, filename in (
                (_PUBLISHED_QUARANTINE_STATE, _PUBLISHED_QUARANTINE_NAME),
                (_UNPROCESSABLE_QUARANTINE_STATE, _UNPROCESSABLE_QUARANTINE_NAME),
            )
            if os.path.lexists(path / filename)
        )
    if sidecar_present:
        markers.append((_UNPROCESSABLE_QUARANTINE_STATE, sidecar))
    if len(markers) > 1 or (not is_directory and not sidecar_present):
        raise AttemptStateError("legacy quarantine markers are missing or contradictory")
    if not markers:
        return None, None
    state, marker_path = markers[0]
    sidecar_entry = path if marker_path == sidecar else None
    if _terminal_marker_at(marker_path, expected_state=state, sidecar_entry=sidecar_entry) is None:
        raise AttemptStateError("legacy quarantine marker is malformed")
    value = _read_json(marker_path)
    if not isinstance(value, dict):
        raise AttemptStateError("legacy quarantine marker is malformed")
    return state, value


def _legacy_quarantine_attempt_id(path: Path) -> str | None:
    descriptor_path = path / "attempt.json"
    if not os.path.lexists(descriptor_path):
        return None
    _require_regular_file(descriptor_path, "legacy quarantine descriptor")
    raw = _read_json(descriptor_path)
    try:
        if not isinstance(raw, dict):
            raise ValueError("descriptor")
        descriptor = _descriptor_from_json(raw)
        _validate_descriptor(descriptor, descriptor.attempt_id)
        return descriptor.attempt_id
    except (AttemptStateError, TypeError, ValueError) as error:
        raise AttemptStateError("legacy quarantine descriptor is malformed") from error


def _quarantine_row(filesystem: StagingFilesystem, entry: Path) -> dict[str, object]:
    try:
        row = filesystem.state_db.get(StateKind.QUARANTINE, entry.name)
    except (OSError, ValueError, StateDBError) as error:
        raise AttemptStateError("quarantine state is unreadable") from error
    if row is None:
        raise AttemptStateError("quarantine state is missing or malformed")
    return _validate_quarantine_payload(entry.name, row)


def _validate_quarantine_payload(entry_name: str, row: dict[str, object]) -> dict[str, object]:
    if set(row) != _QUARANTINE_FIELDS:
        raise AttemptStateError("quarantine state is missing or malformed")
    if not _valid_quarantine_identity(entry_name, row):
        raise AttemptStateError("quarantine state is malformed")
    _validate_quarantine_terminal(row)
    return row


def _validate_quarantine_terminal(row: dict[str, object]) -> None:
    attempt_id = row["attempt_id"]
    if attempt_id is not None:
        if not isinstance(attempt_id, str):
            raise AttemptStateError("quarantine attempt identity is malformed")
        try:
            _validate_attempt_id(attempt_id)
        except (TypeError, ValueError) as error:
            raise AttemptStateError("quarantine attempt identity is malformed") from error
    terminal_state = row["terminal_state"]
    terminal_at = row["terminal_at_unix_ns"]
    terminal_reason = row["terminal_reason"]
    if terminal_state is None:
        if terminal_at is not None or terminal_reason is not None:
            raise AttemptStateError("quarantine terminal evidence is contradictory")
    elif terminal_state == "published":
        if (
            isinstance(terminal_at, bool)
            or not isinstance(terminal_at, int)
            or terminal_at < 0
            or terminal_reason is not None
        ):
            raise AttemptStateError("quarantine terminal evidence is malformed")
    elif terminal_state == "unprocessable":
        if (
            isinstance(terminal_at, bool)
            or not isinstance(terminal_at, int)
            or terminal_at < 0
            or not isinstance(terminal_reason, str)
            or not terminal_reason
        ):
            raise AttemptStateError("quarantine terminal evidence is malformed")
    else:
        raise AttemptStateError("quarantine terminal evidence is invalid")


def _valid_quarantine_identity(entry_name: str, row: dict[str, object]) -> bool:
    return (
        row["physical_name"] == entry_name
        and isinstance(row["source_name"], str)
        and bool(row["source_name"])
        and row["source_root"] in {"attempts", "spool", "capture"}
        and isinstance(row["original_name"], str)
        and bool(row["original_name"])
        and isinstance(row["reason"], str)
        and bool(row["reason"])
        and isinstance(row["opaque"], bool)
        and row["entry_kind"] in {"directory", "file", "symlink"}
        and isinstance(row["move_committed"], bool)
        and not isinstance(row["source_device"], bool)
        and isinstance(row["source_device"], int)
        and not isinstance(row["source_inode"], bool)
        and isinstance(row["source_inode"], int)
        and isinstance(row["deletion_pending"], bool)
    )


def _put_quarantine_row(filesystem: StagingFilesystem, item_id: str, row: dict[str, object]) -> None:
    if set(row) != _QUARANTINE_FIELDS:
        raise AttemptStateError("quarantine state schema is invalid")
    try:
        with filesystem.state_db.transaction() as transaction:
            transaction.put(StateKind.QUARANTINE, item_id, row)
    except (OSError, ValueError, StateDBError) as error:
        raise AttemptStateError("quarantine state could not be written") from error


def _terminal_quarantine_at(filesystem: StagingFilesystem, entry: Path) -> int | None:
    """Return the retention timestamp for published or unprocessable evidence."""
    row = _quarantine_row(filesystem, entry)
    terminal_at = row["terminal_at_unix_ns"]
    return (
        terminal_at
        if row["terminal_state"] in {"published", "unprocessable"} and isinstance(terminal_at, int)
        else None
    )


def _quarantine_state(filesystem: StagingFilesystem, entry: Path) -> QuarantineState:
    """Classify typed database evidence; missing or contradictory state stays live."""
    result = QuarantineState.INVALID_EVIDENCE
    try:
        mode = entry.lstat().st_mode
        row = _quarantine_row(filesystem, entry)
        entry_kind = "symlink" if stat.S_ISLNK(mode) else "directory" if stat.S_ISDIR(mode) else "file"
        if row["move_committed"] and row["entry_kind"] == entry_kind:
            result = {
                "published": QuarantineState.PUBLISHED,
                "unprocessable": QuarantineState.UNPROCESSABLE,
                None: QuarantineState.RETRYABLE,
            }.get(row["terminal_state"], QuarantineState.INVALID_EVIDENCE)
    except AttemptStateError, OSError:
        pass
    return result


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


def _terminal_retired_at(filesystem: StagingFilesystem, attempt_id: str) -> int | None:
    try:
        row = filesystem._read_attempt_row(attempt_id)
        raw_terminal = row["terminal_retirement"]
        raw_prefix = row["prefix_publication"]
        if not isinstance(raw_terminal, dict) or not isinstance(raw_prefix, dict):
            return None
        prefix = PrefixPublicationEvidence.from_json(raw_prefix)
        terminal = TerminalRetirementEvidence.from_json(raw_terminal)
        if not _recoverable_prefix_marker_matches(prefix):
            return None
        return _terminal_retired_marker_matches(terminal)
    except OSError, StagingError, AttemptStateError, TypeError, ValueError:
        return None


def _has_terminal_retirement_marker(filesystem: StagingFilesystem, path: Path) -> bool:
    row = filesystem._read_attempt_row(path.name)
    return row["terminal_retirement"] is not None


def _is_terminal_retired_attempt(filesystem: StagingFilesystem, path: Path) -> bool:
    descriptor = filesystem._read_descriptor(path)
    return _terminal_retired_at(filesystem, descriptor.attempt_id) is not None


def is_nonblocking_attempt(filesystem: StagingFilesystem, path: Path, descriptor: AttemptDescriptor) -> bool:
    if _has_terminal_retirement_marker(filesystem, path):
        return _is_terminal_retired_attempt(filesystem, path)
    return _is_published_attempt(filesystem, path, descriptor)


def _is_published_attempt(filesystem: StagingFilesystem, path: Path, descriptor: AttemptDescriptor) -> bool:
    state = filesystem._read_attempt_row(descriptor.attempt_id)
    raw_prefix = state["prefix_publication"]
    if raw_prefix is not None:
        try:
            if not isinstance(raw_prefix, dict):
                return False
            marker = PrefixPublicationEvidence.from_json(raw_prefix)
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
    raw_path = path / _RAW_NAME
    if not manifest_path.exists() or not raw_path.exists():
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
    destination = _quarantine_source_path(destination_root, filesystem.attempts_root.name)
    _begin_quarantine_move(
        filesystem,
        _QuarantineMoveIntent(filesystem.attempts_root, destination, None, True, reason),
    )
    filesystem.attempts_root.replace(destination)
    _sync_directory(destination.parent, filesystem._fsync)
    _sync_directory(filesystem.spool, filesystem._fsync)
    _finish_quarantine_move(filesystem, destination)
    filesystem._ensure_directory(filesystem.attempts_root)
    return destination


def _move_to_quarantine(filesystem: StagingFilesystem, root: Path, entry: Path, reason: str, *, opaque: bool) -> Path:
    destination = _quarantine_source_path(root, entry.name)
    attempt_id: str | None = None
    if not opaque:
        attempt_id = filesystem._read_descriptor(entry).attempt_id
    elif root == filesystem.quarantine_root and entry.parent == filesystem.attempts_root:
        try:
            mode = entry.lstat().st_mode
            if stat.S_ISDIR(mode) and not stat.S_ISLNK(mode):
                _validate_attempt_id(entry.name)
                attempt_id = entry.name
        except OSError, AttemptStateError:
            pass
    _begin_quarantine_move(filesystem, _QuarantineMoveIntent(entry, destination, attempt_id, opaque, reason))
    entry.replace(destination)
    _sync_directory(destination.parent, filesystem._fsync)
    _sync_directory(filesystem.attempts_root, filesystem._fsync)
    _finish_quarantine_move(filesystem, destination)
    return destination


def _begin_quarantine_move(
    filesystem: StagingFilesystem,
    intent: _QuarantineMoveIntent,
) -> None:
    source = intent.source
    destination = intent.destination
    attempt_id = intent.attempt_id
    opaque = intent.opaque
    reason = intent.reason
    now = _wall_clock_ns() if opaque else None
    source_mode = source.lstat().st_mode
    entry_kind = "symlink" if stat.S_ISLNK(source_mode) else "directory" if stat.S_ISDIR(source_mode) else "file"
    row: dict[str, object] = {
        "physical_name": destination.name,
        "source_name": source.name,
        "source_root": (
            "spool"
            if source == filesystem.attempts_root
            else "capture"
            if source.parent == filesystem.capture_root
            else "attempts"
        ),
        "original_name": source.name,
        "attempt_id": attempt_id,
        "reason": reason,
        "opaque": opaque,
        "entry_kind": entry_kind,
        "move_committed": False,
        "source_device": source.lstat().st_dev,
        "source_inode": source.lstat().st_ino,
        "deletion_pending": False,
        "terminal_state": "unprocessable" if opaque else None,
        "terminal_at_unix_ns": now,
        "terminal_reason": reason if opaque else None,
    }
    try:
        with filesystem.state_db.transaction() as transaction:
            if transaction.get(StateKind.QUARANTINE, destination.name) is not None:
                raise AttemptStateError("quarantine identity already exists")
            transaction.put(StateKind.QUARANTINE, destination.name, row)
    except (OSError, ValueError, StateDBError) as error:
        raise AttemptStateError("quarantine move intent could not be recorded") from error


def _finish_quarantine_move(filesystem: StagingFilesystem, destination: Path) -> None:
    row = _quarantine_row(filesystem, destination)
    row["move_committed"] = True
    attempt_id = row["attempt_id"]
    try:
        with filesystem.state_db.transaction() as transaction:
            if isinstance(attempt_id, str):
                attempt_state = transaction.get(StateKind.ATTEMPT, attempt_id)
                if attempt_state is not None:
                    try:
                        filesystem._validate_attempt_row(attempt_id, attempt_state)
                    except AttemptStateError, TypeError, ValueError:
                        # Quarantine owns the raw evidence now. Drop positively
                        # identified malformed typed state so it cannot poison
                        # future resume scans or be mistaken for usable data.
                        transaction.delete(StateKind.ATTEMPT, attempt_id)
            transaction.put(StateKind.QUARANTINE, destination.name, row)
    except (OSError, ValueError, StateDBError) as error:
        raise AttemptStateError("quarantine move could not be committed") from error


def _reconcile_quarantine_moves(filesystem: StagingFilesystem) -> None:
    for item_id, row in filesystem.state_db.list(StateKind.QUARANTINE):
        _reconcile_quarantine_move(filesystem, item_id, row)


def _reconcile_quarantine_move(filesystem: StagingFilesystem, item_id: str, row: dict[str, object]) -> None:
    source_name, source_root = _validate_move_identity(item_id, row)
    source_base = {
        "attempts": filesystem.attempts_root,
        "spool": filesystem.spool,
        "capture": filesystem.capture_root,
    }[source_root]
    source = source_base / source_name
    destination = filesystem.quarantine_root / item_id
    source_exists = os.path.lexists(source)
    destination_exists = os.path.lexists(destination)
    destination_matches = destination_exists and _matches_quarantine_identity(row, destination)
    if destination_exists and not destination_matches:
        raise StagingError("quarantine destination identity changed")
    source_exists = _resolve_source_identity(
        filesystem, row, _MoveSourceLocation(source_name, source_root, source, source_exists, destination_matches)
    )
    if row.get("move_committed") is True:
        _reconcile_committed_move(filesystem, item_id, row, source_exists, destination_exists)
        return
    if source_exists == destination_exists:
        raise StagingError("quarantine move location is ambiguous")
    if source_exists:
        source.replace(destination)
        _sync_directory(destination.parent, filesystem._fsync)
        _sync_directory(source.parent, filesystem._fsync)
    _finish_quarantine_move(filesystem, destination)


def _resolve_source_identity(
    filesystem: StagingFilesystem,
    row: dict[str, object],
    location: _MoveSourceLocation,
) -> bool:
    if not location.exists or _matches_quarantine_identity(row, location.path):
        return location.exists
    recreated_root = (
        row.get("move_committed") is True
        and location.root == "spool"
        and location.name == filesystem.attempts_root.name
        and location.path.is_dir()
        and not location.path.is_symlink()
    )
    if recreated_root or (not row.get("move_committed") and location.destination_matches):
        return False
    raise StagingError("quarantine source identity changed")


def _validate_move_identity(item_id: str, row: dict[str, object]) -> tuple[str, str]:
    if not _valid_component(item_id):
        raise StagingError("quarantine move state is malformed")
    try:
        _validate_quarantine_payload(item_id, row)
    except AttemptStateError as error:
        raise StagingError("quarantine move state is malformed") from error
    source_name, source_root = row.get("source_name"), row.get("source_root")
    if not isinstance(source_name, str) or not _valid_component(source_name):
        raise StagingError("quarantine move source identity is malformed")
    if source_root not in {"attempts", "spool", "capture"}:
        raise StagingError("quarantine move source root is malformed")
    return source_name, cast(str, source_root)


def _reconcile_committed_move(
    filesystem: StagingFilesystem,
    item_id: str,
    row: dict[str, object],
    source_exists: bool,
    destination_exists: bool,
) -> None:
    if destination_exists and not source_exists:
        return
    if not destination_exists and not source_exists:
        if _terminal_deletion_expired(filesystem, row):
            _forget_deleted_quarantine(filesystem, item_id, row)
            return
        raise StagingError("quarantine source disappeared without durable deletion intent")
    raise StagingError("quarantine move location is ambiguous")


def _terminal_deletion_expired(filesystem: StagingFilesystem, row: dict[str, object]) -> bool:
    terminal_at = row.get("terminal_at_unix_ns")
    retention_ns = _seconds_to_nanoseconds(filesystem._terminal_retention_seconds)
    return (
        row.get("deletion_pending") is True
        and row.get("terminal_state") in {"published", "unprocessable"}
        and isinstance(terminal_at, int)
        and _wall_clock_ns() - terminal_at >= retention_ns
    )


def _forget_deleted_quarantine(filesystem: StagingFilesystem, item_id: str, row: dict[str, object]) -> None:
    _sync_directory(filesystem.quarantine_root, filesystem._fsync)
    with filesystem.state_db.transaction() as transaction:
        transaction.delete(StateKind.QUARANTINE, item_id)
        if isinstance(row.get("attempt_id"), str):
            transaction.delete(StateKind.ATTEMPT, cast(str, row["attempt_id"]))


def _valid_component(value: str) -> bool:
    return bool(value) and value not in {".", ".."} and "/" not in value and "\0" not in value


def _matches_quarantine_identity(row: dict[str, object], path: Path) -> bool:
    try:
        identity = path.lstat()
    except OSError:
        return False
    kind = "symlink" if stat.S_ISLNK(identity.st_mode) else "directory" if stat.S_ISDIR(identity.st_mode) else "file"
    return (identity.st_dev, identity.st_ino) == (row.get("source_device"), row.get("source_inode")) and row.get(
        "entry_kind"
    ) == kind


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
    destination = _quarantine_source_path(destination_root, f"capture-temporary-{temporary.name}")
    _begin_quarantine_move(filesystem, _QuarantineMoveIntent(temporary, destination, None, True, reason))
    temporary.replace(destination)
    _sync_directory(destination.parent, filesystem._fsync)
    _sync_directory(capture_root, filesystem._fsync)
    _finish_quarantine_move(filesystem, destination)
