"""Durable private pendant-drain state and its repairable public JSON projection."""

from __future__ import annotations

import errno
import json
import os
import re
import stat
import tempfile
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import cast
from uuid import uuid4

from omi_collector.capture.adapters.collector_state_db import StateDB, StateKind, StateTransaction
from omi_collector.capture.domain.ring_protocol import RingInfo

_ROW_ID = "current"
_VERSION = 1
_ROW_FIELDS = frozenset(
    {"version", "ring", "last_drain", "battery", "pending_event_id", "pending_source_id", "pending_frontier"}
)
_PUBLIC_FIELDS = frozenset({"version", "ring", "last_drain", "battery"})
_OBSERVATION_FIELDS = 2
_MAX_BATTERY_PERCENT = 100
_PRIVATE_TEMP_MODES = frozenset({0o600, 0o640})
_PUBLIC_MODE = 0o640
_TEMPORARY_SUFFIX = re.compile(r"[a-z0-9_]{8}\Z")


@dataclass(frozen=True)
class AcceptedDrainCustody:
    """Bind accepted source custody to its frontier and optional confirmed loss endpoint."""

    source_id: str
    frontier: int
    confirmed_loss_end: int | None = None


class PublicDrainProjection:
    """Persist drain evidence in SQLite and atomically project its public subset."""

    def __init__(self, state_db: StateDB, path: Path) -> None:
        self._state_db = state_db
        self.path = Path(path)
        self._wake_callback: Callable[[], None] | None = None
        self._stale_files_scanned = False
        self._needs_sync = False

    def set_wake_callback(self, callback: Callable[[], None] | None) -> None:
        """Register the coordinator event used to coalesce committed changes."""
        self._wake_callback = callback

    def wake(self) -> None:
        """Wake the sole public-file writer after a durable adapter mutation."""
        if self._wake_callback is not None:
            self._wake_callback()

    def publish_snapshot(self) -> None:
        """Atomically publish the latest committed row, or an all-unknown document."""
        if self.path.is_symlink() or self.path.parent.is_symlink():
            raise OSError("public pendant drain path must not contain symlinks")
        if not self.path.parent.is_dir():
            raise OSError("public pendant drain directory is unavailable")
        parent_stat = self.path.parent.stat()
        if not self._stale_files_scanned:
            for candidate in _stale_files(self.path, parent_stat.st_gid):
                self._needs_sync = True
                try:
                    candidate.unlink()
                except FileNotFoundError:
                    continue
            self._stale_files_scanned = True
        row = self._state_db.get(StateKind.PENDANT_DRAIN, _ROW_ID)
        public = _public_row(row)
        encoded = (json.dumps(public, sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8")
        if not self._needs_sync and _matches_public_file(self.path, encoded, parent_stat.st_gid):
            return
        self._needs_sync = True
        descriptor, temporary_name = tempfile.mkstemp(prefix=f".{self.path.name}.", dir=self.path.parent)
        temporary = Path(temporary_name)
        try:
            with os.fdopen(descriptor, "wb") as stream:
                os.fchmod(stream.fileno(), _PUBLIC_MODE)
                stream.write(encoded)
                stream.flush()
                os.fsync(stream.fileno())
            if self.path.is_symlink():
                raise OSError("public pendant drain file must not be a symlink")
            temporary.replace(self.path)
            directory_fd = os.open(self.path.parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
            self._needs_sync = False
        finally:
            if temporary.exists():
                temporary.unlink()

    def update_observations(
        self, *, ring: tuple[int, str] | None = None, battery: tuple[int, str] | None = None
    ) -> None:
        """Commit valid latest observations while retaining other private state."""
        _validate_ring(ring)
        _validate_battery(battery)
        with self._state_db.transaction() as transaction:
            row = _row_or_empty(transaction.get(StateKind.PENDANT_DRAIN, _ROW_ID))
            if ring is not None:
                row["ring"] = {"unread_packets": ring[0], "observed_at": ring[1]}
            if battery is not None:
                row["battery"] = {"percent": battery[0], "observed_at": battery[1]}
            transaction.put(StateKind.PENDANT_DRAIN, _ROW_ID, row)

    def arm_pending(self, source_id: str, durable_next_sequence: int) -> None:
        """Persist a private custody token once, before destructive ring advance."""
        if not isinstance(source_id, str) or not source_id.strip():
            raise ValueError("pendant drain source ID is invalid")
        if type(durable_next_sequence) is not int or durable_next_sequence < 0:
            raise ValueError("pendant drain custody frontier is invalid")
        with self._state_db.transaction() as transaction:
            row = _row_or_empty(transaction.get(StateKind.PENDANT_DRAIN, _ROW_ID))
            if (row["pending_source_id"], row["pending_frontier"]) != (source_id, durable_next_sequence):
                row["pending_event_id"] = str(uuid4())
                row["pending_source_id"] = source_id
                row["pending_frontier"] = durable_next_sequence
                transaction.put(StateKind.PENDANT_DRAIN, _ROW_ID, row)

    def pending_binding(self) -> tuple[str, int] | None:
        """Return the latest private source/frontier binding for lease-held validation."""
        row = _row_or_empty(self._state_db.get(StateKind.PENDANT_DRAIN, _ROW_ID))
        if row["pending_event_id"] is None:
            return None
        return cast(str, row["pending_source_id"]), cast(int, row["pending_frontier"])

    def complete_in_transaction(
        self,
        tx: StateTransaction,
        *,
        verified_drain: tuple[RingInfo, str],
        completed_at: str,
        closure_frontier: int,
        accepted_custody: AcceptedDrainCustody | None,
    ) -> None:
        """Complete an armed token in the same SQLite transaction as its closure."""
        info, observed_at = verified_drain
        _validate_timestamp(observed_at)
        _validate_timestamp(completed_at)
        _validate_frontier(closure_frontier)
        if accepted_custody is not None:
            _validate_accepted_custody(accepted_custody)
        if not isinstance(info, RingInfo):
            raise ValueError("verified drain INFO must report zero unread packets")
        if any(type(sequence) is not int or sequence < 0 for sequence in (info.read_sequence, info.write_sequence)):
            raise ValueError("verified drain INFO sequence is invalid")
        if info.read_sequence != info.write_sequence:
            raise ValueError("verified drain INFO must report zero unread packets")
        row = _row_or_empty(tx.get(StateKind.PENDANT_DRAIN, _ROW_ID))
        row["ring"] = {"unread_packets": 0, "observed_at": observed_at}
        pending = row["pending_event_id"]
        pending_frontier = row["pending_frontier"]
        accepted_binding = None if accepted_custody is None else (accepted_custody.source_id, accepted_custody.frontier)
        pending_binding = (row["pending_source_id"], pending_frontier)
        if (
            pending is not None
            and accepted_custody is not None
            and accepted_binding == pending_binding
            and closure_frontier == pending_frontier
            and (
                info.read_sequence == closure_frontier
                or (info.read_sequence > closure_frontier and info.read_sequence == accepted_custody.confirmed_loss_end)
            )
        ):
            row["last_drain"] = {"event_id": pending, "completed_at": completed_at}
            row["pending_event_id"] = None
            row["pending_source_id"] = None
            row["pending_frontier"] = None
        tx.put(StateKind.PENDANT_DRAIN, _ROW_ID, row)


def _public_row(row: dict[str, object] | None) -> dict[str, object]:
    if row is None:
        return {"version": _VERSION, "ring": None, "last_drain": None, "battery": None}
    validated = _validate_row(row)
    return {key: validated[key] for key in _PUBLIC_FIELDS}


def _row_or_empty(row: dict[str, object] | None) -> dict[str, object]:
    if row is None:
        return {
            "version": _VERSION,
            "ring": None,
            "last_drain": None,
            "battery": None,
            "pending_event_id": None,
            "pending_source_id": None,
            "pending_frontier": None,
        }
    return _validate_row(row)


def _validate_row(row: dict[str, object]) -> dict[str, object]:
    if set(row) != _ROW_FIELDS or type(row.get("version")) is not int or row.get("version") != _VERSION:
        raise ValueError("pendant drain state schema is invalid")
    _validate_public_values(row)
    pending = row["pending_event_id"]
    if pending is not None and (not isinstance(pending, str) or not pending.strip()):
        raise ValueError("pending pendant drain token is invalid")
    source_id = row["pending_source_id"]
    frontier = row["pending_frontier"]
    if pending is None:
        if source_id is not None or frontier is not None:
            raise ValueError("unarmed pendant drain custody metadata is invalid")
    elif not isinstance(source_id, str) or not source_id.strip() or type(frontier) is not int or frontier < 0:
        raise ValueError("pending pendant drain custody metadata is invalid")
    return row


def _validate_public_values(row: dict[str, object]) -> None:
    ring = row["ring"]
    if ring is not None:
        if not isinstance(ring, dict) or set(ring) != {"unread_packets", "observed_at"}:
            raise ValueError("pendant ring observation is invalid")
        unread = ring["unread_packets"]
        if type(unread) is not int or unread < 0:
            raise ValueError("pendant unread packet count is invalid")
        _validate_timestamp(cast(str, ring["observed_at"]))
    drain = row["last_drain"]
    if drain is not None:
        if not isinstance(drain, dict) or set(drain) != {"event_id", "completed_at"}:
            raise ValueError("pendant drain completion is invalid")
        if not isinstance(drain["event_id"], str) or not drain["event_id"].strip():
            raise ValueError("pendant drain event ID is invalid")
        _validate_timestamp(cast(str, drain["completed_at"]))
    battery = row["battery"]
    if battery is not None:
        if not isinstance(battery, dict) or set(battery) != {"percent", "observed_at"}:
            raise ValueError("pendant battery observation is invalid")
        percent = battery["percent"]
        if type(percent) is not int or not 0 <= percent <= _MAX_BATTERY_PERCENT:
            raise ValueError("pendant battery percentage is invalid")
        _validate_timestamp(cast(str, battery["observed_at"]))


def _validate_ring(value: tuple[int, str] | None) -> None:
    if value is not None:
        if (
            not isinstance(value, tuple)
            or len(value) != _OBSERVATION_FIELDS
            or type(value[0]) is not int
            or value[0] < 0
        ):
            raise ValueError("ring observation is invalid")
        _validate_timestamp(value[1])


def _validate_battery(value: tuple[int, str] | None) -> None:
    if value is not None:
        if (
            not isinstance(value, tuple)
            or len(value) != _OBSERVATION_FIELDS
            or type(value[0]) is not int
            or not 0 <= value[0] <= _MAX_BATTERY_PERCENT
        ):
            raise ValueError("battery observation is invalid")
        _validate_timestamp(value[1])


def _validate_frontier(value: int) -> None:
    if type(value) is not int or value < 0:
        raise ValueError("closure frontier is invalid")


def _validate_accepted_custody(value: AcceptedDrainCustody) -> None:
    if not isinstance(value, AcceptedDrainCustody):
        raise ValueError("accepted pendant custody binding is invalid")
    if not isinstance(value.source_id, str) or not value.source_id.strip():
        raise ValueError("accepted pendant custody binding is invalid")
    if type(value.frontier) is not int or value.frontier < 0:
        raise ValueError("accepted pendant custody binding is invalid")
    if value.confirmed_loss_end is not None and (
        type(value.confirmed_loss_end) is not int or value.confirmed_loss_end < 0
    ):
        raise ValueError("accepted pendant custody binding is invalid")


def _safe_stale_file(candidate: os.stat_result, parent_gid: int) -> bool:
    return (
        stat.S_ISREG(candidate.st_mode)
        and candidate.st_uid == os.geteuid()
        and candidate.st_gid == parent_gid
        and candidate.st_nlink == 1
        and stat.S_IMODE(candidate.st_mode) in _PRIVATE_TEMP_MODES
    )


def _stale_files(path: Path, parent_gid: int) -> list[Path]:
    stale_files = []
    prefix = f".{path.name}."
    for candidate in path.parent.iterdir():
        if not candidate.name.startswith(prefix) or not _TEMPORARY_SUFFIX.fullmatch(candidate.name[len(prefix) :]):
            continue
        try:
            candidate_stat = candidate.lstat()
        except FileNotFoundError:
            continue
        if _safe_stale_file(candidate_stat, parent_gid):
            stale_files.append(candidate)
    return stale_files


def _matches_public_file(path: Path, encoded: bytes, parent_gid: int) -> bool:
    try:
        descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    except FileNotFoundError:
        return False
    except OSError as error:
        if error.errno == errno.ELOOP:
            return False
        raise
    with os.fdopen(descriptor, "rb") as stream:
        metadata = os.fstat(stream.fileno())
        return (
            stat.S_ISREG(metadata.st_mode)
            and metadata.st_uid == os.geteuid()
            and metadata.st_gid == parent_gid
            and metadata.st_nlink == 1
            and stat.S_IMODE(metadata.st_mode) == _PUBLIC_MODE
            and metadata.st_size == len(encoded)
            and stream.read() == encoded
        )


def _validate_timestamp(value: str) -> None:
    if not isinstance(value, str):
        raise ValueError("timestamp must be an ISO 8601 string")
    try:
        parsed = datetime.fromisoformat(f"{value[:-1]}+00:00" if value.endswith("Z") else value)
    except ValueError as error:
        raise ValueError("timestamp must be an ISO 8601 string") from error
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("timestamp must include a timezone")
