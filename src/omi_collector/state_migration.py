"""Stopped-service cutover from Collector JSON control files to one SQLite state file."""

from __future__ import annotations

import asyncio
import fcntl
import json
import math
import os
import re
import stat
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from hashlib import sha256
from itertools import pairwise
from pathlib import Path
from typing import cast

from omi_collector.capture.adapters.bundle_contract import SealedReceipt
from omi_collector.capture.adapters.clock_segments import ClockSegmentMap
from omi_collector.capture.adapters.collector_guard import collector_guard
from omi_collector.capture.adapters.collector_state_db import StateDB, StateKind
from omi_collector.capture.adapters.confirmed_loss import parse_legacy_confirmed_loss, read_confirmed_losses
from omi_collector.capture.adapters.debug_logging import (
    import_legacy_runtime_status,
    merge_runtime_status,
    read_runtime_status,
)
from omi_collector.capture.adapters.firmware_observations import parse_legacy_firmware, read_firmware_observations
from omi_collector.capture.adapters.legacy_clock_import import read_legacy_clock
from omi_collector.capture.adapters.operational_status import parse_legacy_operational_status
from omi_collector.capture.adapters.phy_guard import (
    CommandRunner,
    PhyGuardPaths,
    default_phy_guard_paths,
    recover_legacy_phy_marker,
    run_bluetoothctl,
)
from omi_collector.capture.adapters.quality_metrics import import_quality_events, quality_event_id
from omi_collector.capture.adapters.quarantine import legacy_quarantine_row
from omi_collector.capture.adapters.ready_bundles import (
    ReadyBundleError,
    _convert_record,
    _time_range,
    authenticated_inventory,
    legacy_ready_publications,
)
from omi_collector.capture.adapters.ready_closures import legacy_closures
from omi_collector.capture.adapters.staging_filesystem import legacy_attempt_row
from omi_collector.capture.domain.clock_state import ClockAnchor, ClockRange, ClockState
from omi_collector.capture.domain.ring_protocol import RECORD_SIZE
from omi_collector.config import DEFAULT_CONFIG
from omi_collector.operator_status import _debug_rows, _quality_rows
from omi_collector.storage_layout import StorageLayout

_MIGRATION_ID = "current"
_MIGRATION_VERSION = 1
_SHA256_HEX_LENGTH = 64
_FINGERPRINT_PAIR_LENGTH = 2
_CANDIDATE_NAME = ".collector-state.sqlite3.migrating"
_RETAINED_RECOVERY = Path("recovery/10890392-13085638/rescue-manifest.json")
_LEGACY_PHY_NAME = re.compile(r"(hci[0-9]+)\.phy(?:\.lock|-recovery\.json)\Z")
_LEGACY_SINGLETONS = (
    "device.json",
    "operational-status.json",
    "operational-status.lock",
    "operational-status.json.lock",
    "confirmed-loss.json",
    "ready-publications.json",
    "ready-closures.json",
    "timeline-repairs.json",
    "debug.jsonl",
)
_CLOCK_DIRECTORIES = ("clock-observations", "clock-corrections", "clock-memberships", "clock-imports")
_ATTEMPT_STATE_NAMES = (
    "attempt.json",
    "checkpoint.json",
    "prefix-publication.json",
    "terminal-retired.json",
)
_QUARANTINE_STATE_NAMES = (
    *_ATTEMPT_STATE_NAMES,
    "published.json",
    "unprocessable.json",
)


class StateMigrationError(RuntimeError):
    """Legacy state is unsafe, incomplete, or cannot be shown equivalent."""


@dataclass(frozen=True)
class _RetainedRecovery:
    manifest_sha256: str
    records_sha256: str
    intervals: tuple[tuple[int, int], ...]
    sources: tuple[tuple[Path, int, int, int], ...]


def open_runtime_state(layout: StorageLayout, *, read_only: bool = False) -> StateDB:
    """Refuse BLE/runtime use before a verified offline cutover has completed."""
    path = layout.collector.state_db
    if _path_present(layout.collector.root / _CANDIDATE_NAME):
        raise StateMigrationError("unfinished Collector state migration candidate needs inspection")
    if not path.exists():
        if _has_legacy_state(layout) or _has_collector_data(layout):
            raise StateMigrationError("legacy Collector state requires offline migrate-state")
        if read_only:
            raise StateMigrationError("Collector state database is not initialized")
    db = StateDB(path, read_only=read_only)
    marker = db.get(StateKind.MIGRATION, _MIGRATION_ID)
    if marker is None and not read_only and not _has_legacy_state(layout) and not _has_collector_data(layout):
        db.put(StateKind.MIGRATION, _MIGRATION_ID, {"version": _MIGRATION_VERSION, "phase": "complete"})
        return db
    if marker != {"version": _MIGRATION_VERSION, "phase": "complete"}:
        raise StateMigrationError("Collector state migration is incomplete")
    if _has_legacy_state(layout):
        raise StateMigrationError("legacy Collector control files remain after migration")
    return db


def migrate_state(
    layout: StorageLayout,
    *,
    legacy_phy_state_homes: tuple[Path, ...] | None = None,
    legacy_phy_runner: CommandRunner = run_bluetoothctl,
) -> dict[str, int]:
    """Import, verify, activate, and directly remove legacy control files."""
    root = layout.collector.root
    if not root.exists():
        if _path_present(_recovery_manifest_path(layout).parent):
            raise StateMigrationError("retained recovery exists without Collector directory")
        phy_paths = _legacy_phy_paths(layout, DEFAULT_CONFIG.ble.adapter_name, legacy_phy_state_homes)
        if any(_path_present(path.lock_path) or _path_present(path.marker_path) for path in phy_paths):
            raise StateMigrationError("legacy PHY state exists without Collector directory")
        return {"removed_files": 0}
    if not root.is_dir() or root.is_symlink():
        raise StateMigrationError("Collector directory is missing or unsafe")
    with collector_guard(layout.collector.lock):
        _probe_legacy_lock(root / "operational-status.lock")
        _probe_legacy_lock(root / "operational-status.json.lock")
        adapter = DEFAULT_CONFIG.ble.adapter_name
        phy_paths = _legacy_phy_paths(layout, adapter, legacy_phy_state_homes)
        _validate_legacy_phy_inventory(phy_paths, adapter)
        for paths in phy_paths:
            _probe_legacy_lock(paths.lock_path)
            asyncio.run(
                recover_legacy_phy_marker(
                    paths.marker_path,
                    adapter,
                    collector_lock=layout.collector.lock,
                    runner=legacy_phy_runner,
                )
            )
        _probe_clock_locks(layout)
        return _migrate_locked(layout, tuple(paths.lock_path for paths in phy_paths))


def _migrate_locked(layout: StorageLayout, legacy_phy_locks: tuple[Path, ...]) -> dict[str, int]:
    root = layout.collector.root
    canonical = layout.collector.state_db
    candidate = root / _CANDIDATE_NAME
    if canonical.exists():
        return _resume_cleanup(layout, StateDB(canonical), legacy_phy_locks)
    if candidate.exists() or candidate.is_symlink():
        raise StateMigrationError("an unfinished candidate database needs inspection before retry")
    recovery = _load_retained_recovery(layout)
    if not _has_legacy_state(layout) and not any(_path_present(path) for path in legacy_phy_locks):
        if _has_collector_data(layout):
            raise StateMigrationError("Collector data exists without recognized control state")
        return {"removed_files": 0}
    files = _legacy_control_files(layout, legacy_phy_locks)
    _validate_obsolete_clock_metadata(root)
    fingerprints = tuple((str(path), _hash_regular_file(path)) for path in files)
    db: StateDB | None = None
    try:
        db = StateDB(candidate)
        _import_legacy(layout, db, recovery)
        state_sha256 = _state_digest(db)
        db.put(
            StateKind.MIGRATION,
            _MIGRATION_ID,
            {
                "version": _MIGRATION_VERSION,
                "phase": "cleanup_pending",
                "files": [list(item) for item in fingerprints],
                "state_sha256": state_sha256,
                "custody": _recovery_marker(recovery),
            },
        )
        reopened = StateDB(candidate, read_only=True)
        reopened.integrity_check()
        _validate_parity(layout, reopened, recovery=recovery)
        _verify_fingerprints(fingerprints)
        _verify_retained_recovery(layout, recovery)
        candidate.replace(canonical)
        _fsync_directory(root)
    except BaseException:
        if candidate.exists():
            candidate.unlink()
            _fsync_directory(root)
        raise
    assert db is not None
    return _resume_cleanup(layout, StateDB(canonical), legacy_phy_locks)


def _resume_cleanup(layout: StorageLayout, db: StateDB, legacy_phy_locks: tuple[Path, ...]) -> dict[str, int]:
    marker = db.get(StateKind.MIGRATION, _MIGRATION_ID)
    if marker == {"version": _MIGRATION_VERSION, "phase": "complete"}:
        if _has_legacy_state(layout) or any(_path_present(path) for path in legacy_phy_locks):
            raise StateMigrationError("legacy state reappeared after migration")
        return {"removed_files": 0}
    fingerprints = _marker_fingerprints(marker, layout, legacy_phy_locks)
    _verify_recovery_marker(layout, marker)
    assert marker is not None
    if _state_digest(db) != marker["state_sha256"]:
        raise StateMigrationError("Collector state changed before migration cleanup")
    current_files = set(_legacy_control_files(layout, legacy_phy_locks))
    if not current_files <= {Path(path_string) for path_string, _ in fingerprints}:
        raise StateMigrationError("unclassified legacy state appeared during cleanup")
    _validate_parity(layout, db, allow_removed=True)
    _verify_fingerprints(fingerprints, allow_missing=True)
    removed = 0
    for path_string, expected in fingerprints:
        path = Path(path_string)
        if not path.exists() and not path.is_symlink():
            continue
        if _hash_regular_file(path) != expected:
            raise StateMigrationError("legacy file changed during cleanup")
        if path.name.endswith(".lock"):
            _probe_legacy_lock(path)
        path.unlink()
        _fsync_directory(path.parent)
        removed += 1
    _remove_empty_legacy_directories(layout)
    if _has_legacy_state(layout) or any(_path_present(path) for path in legacy_phy_locks):
        raise StateMigrationError("unclassified legacy state remains after cleanup")
    db.put(StateKind.MIGRATION, _MIGRATION_ID, {"version": _MIGRATION_VERSION, "phase": "complete"})
    return {"removed_files": removed}


def _legacy_phy_state_home(layout: StorageLayout) -> Path:
    if layout.root == Path("/srv/pipelines/omi"):
        return Path("/var/lib/omi-collector")
    return Path(os.environ.get("XDG_STATE_HOME", Path.home() / ".local" / "state"))


def _legacy_phy_paths(
    layout: StorageLayout, adapter: str, state_homes: tuple[Path, ...] | None
) -> tuple[PhyGuardPaths, ...]:
    homes = (
        state_homes
        if state_homes is not None
        else (
            _legacy_phy_state_home(layout),
            Path(os.environ.get("XDG_STATE_HOME", Path.home() / ".local" / "state")),
        )
    )
    return tuple(
        {
            paths.lock_path: paths for home in homes if (paths := default_phy_guard_paths(adapter, state_home=home))
        }.values()
    )


def _validate_legacy_phy_inventory(paths: tuple[PhyGuardPaths, ...], adapter: str) -> None:
    for legacy_paths in paths:
        directory = legacy_paths.lock_path.parent
        if not _path_present(directory):
            continue
        _require_directory(directory)
        for item in directory.iterdir():
            match = _LEGACY_PHY_NAME.fullmatch(item.name)
            if match is not None and match.group(1) != adapter:
                raise StateMigrationError("legacy PHY state for another adapter requires inspection")


def _path_present(path: Path) -> bool:
    return path.exists() or path.is_symlink()


def _probe_legacy_lock(path: Path) -> None:
    if not _path_present(path):
        return
    fd = os.open(path, os.O_RDWR | os.O_CLOEXEC | os.O_NOFOLLOW)
    try:
        if not stat.S_ISREG(os.fstat(fd).st_mode):
            raise StateMigrationError("migration lock is not a regular file")
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise StateMigrationError("legacy status or PHY writer is still active") from error
    finally:
        os.close(fd)


def _probe_clock_locks(layout: StorageLayout) -> None:
    for name in _CLOCK_DIRECTORIES:
        _probe_legacy_lock(layout.collector.root / name / ".lock")


def _fsync_directory(path: Path) -> None:
    fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _has_legacy_state(layout: StorageLayout) -> bool:
    root = layout.collector.root
    if any((root / name).exists() or (root / name).is_symlink() for name in _LEGACY_SINGLETONS):
        return True
    if any((root / name).exists() or (root / name).is_symlink() for name in _CLOCK_DIRECTORIES):
        return True
    quality = DEFAULT_CONFIG.observability.quality_metrics
    debug = DEFAULT_CONFIG.observability.debug_log
    rotated = (
        *(f"{quality.file_name}.{i}" for i in range(1, quality.backup_count + 1)),
        *(f"{debug.file_name}.{i}" for i in range(1, debug.backup_count + 1)),
    )
    if any((root / name).exists() or (root / name).is_symlink() for name in (quality.file_name, *rotated)):
        return True
    if layout.publication.root.is_dir() and any(
        (item / "receipt.json").exists() for item in layout.publication.root.iterdir() if item.is_dir()
    ):
        return True
    return any(
        _has_staging_control_files(parent)
        for parent in (layout.collector.attempts, layout.collector.quarantine, layout.draft)
    )


def _has_staging_control_files(parent: Path) -> bool:
    if not parent.is_dir():
        return False
    for entry in parent.iterdir():
        children = (entry,) if entry.is_file() else entry.iterdir() if entry.is_dir() else ()
        for child in children:
            if child.name in _ATTEMPT_STATE_NAMES or (child.suffix == ".json" and child.name != "manifest.json"):
                return True
    return False


def _has_collector_data(layout: StorageLayout) -> bool:
    if _path_present(_recovery_manifest_path(layout).parent):
        return True
    for parent in (layout.collector.attempts, layout.collector.quarantine, layout.draft, layout.publication.root):
        if parent.is_dir() and any(parent.iterdir()):
            return True
    return False


def _legacy_control_files(layout: StorageLayout, legacy_phy_locks: tuple[Path, ...] = ()) -> tuple[Path, ...]:
    files: set[Path] = set()
    _root_control_files(layout.collector.root, files, legacy_phy_locks)
    _clock_control_files(layout.collector.root, files)
    for parent in (layout.collector.attempts, layout.collector.quarantine, layout.draft):
        _staging_control_files(
            parent,
            files,
            quarantine=parent == layout.collector.quarantine,
            draft=parent == layout.draft,
        )
    _ready_receipts(layout.publication.root, files)
    return tuple(sorted(files))


def _root_control_files(root: Path, files: set[Path], legacy_phy_locks: tuple[Path, ...]) -> None:
    for path in (*((root / name) for name in _LEGACY_SINGLETONS), *legacy_phy_locks):
        _add_if_present(files, path)
    quality = DEFAULT_CONFIG.observability.quality_metrics
    for name in (quality.file_name, *(f"{quality.file_name}.{i}" for i in range(1, quality.backup_count + 1))):
        _add_if_present(files, root / name)
    debug = DEFAULT_CONFIG.observability.debug_log
    for name in (debug.file_name, *(f"{debug.file_name}.{i}" for i in range(1, debug.backup_count + 1))):
        _add_if_present(files, root / name)


def _clock_control_files(root: Path, files: set[Path]) -> None:
    for name in _CLOCK_DIRECTORIES:
        directory = root / name
        if not _path_present(directory):
            continue
        _require_directory(directory)
        for item in directory.iterdir():
            if item.name != ".lock" and item.suffix != ".json":
                raise StateMigrationError("unclassified legacy clock state entry")
            _add_if_present(files, item)


def _staging_control_files(parent: Path, files: set[Path], *, quarantine: bool, draft: bool) -> None:
    if not _path_present(parent):
        return
    _require_directory(parent)
    sidecars = _quarantine_sidecars(parent) if quarantine else frozenset()
    for entry in parent.iterdir():
        if entry.name in sidecars:
            _add_if_present(files, entry)
        elif not quarantine or (not entry.is_symlink() and entry.is_dir()):
            names = ("receipt.json",) if draft else _QUARANTINE_STATE_NAMES if quarantine else _ATTEMPT_STATE_NAMES
            _attempt_control_files(entry, files, names=names)


def _attempt_control_files(entry: Path, files: set[Path], *, names: tuple[str, ...]) -> None:
    _require_directory(entry)
    for item in entry.iterdir():
        if item.name in names:
            if item.name == "receipt.json":
                SealedReceipt.from_json(_read_object(item))
            _add_if_present(files, item)
        elif item.suffix == ".json" and item.name != "manifest.json":
            raise StateMigrationError("unclassified mutable attempt or quarantine state")


def _ready_receipts(root: Path, files: set[Path]) -> None:
    if root.is_dir():
        for entry in root.iterdir():
            if entry.is_dir() and not entry.is_symlink():
                if _path_present(entry / "receipt.json"):
                    SealedReceipt.from_json(_read_object(entry / "receipt.json"))
                _add_if_present(files, entry / "receipt.json")


def _quarantine_sidecars(parent: Path) -> frozenset[str]:
    names = {entry.name for entry in parent.iterdir()}
    sidecars = {f"{name}.json" for name in names if f"{name}.json" in names}
    if any(f"{name}.json" in sidecars for name in sidecars):
        raise StateMigrationError("quarantine sidecar identity is ambiguous")
    return frozenset(sidecars)


def _add_if_present(files: set[Path], path: Path) -> None:
    if not path.exists() and not path.is_symlink():
        return
    _hash_regular_file(path)
    files.add(path)


def _require_directory(path: Path) -> None:
    mode = path.lstat().st_mode
    if stat.S_ISLNK(mode) or not stat.S_ISDIR(mode):
        raise StateMigrationError("legacy state directory is unsafe")


def _hash_regular_file(path: Path) -> str:
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC)
    try:
        if not stat.S_ISREG(os.fstat(descriptor).st_mode):
            raise StateMigrationError("legacy state file is not regular")
        digest = sha256()
        while block := os.read(descriptor, 1 << 20):
            digest.update(block)
        return digest.hexdigest()
    finally:
        os.close(descriptor)


def _verify_fingerprints(fingerprints: tuple[tuple[str, str], ...], *, allow_missing: bool = False) -> None:
    for path_string, expected in fingerprints:
        path = Path(path_string)
        if allow_missing and not path.exists() and not path.is_symlink():
            continue
        if _hash_regular_file(path) != expected:
            raise StateMigrationError("legacy state changed during offline migration")


def _marker_fingerprints(
    marker: dict[str, object] | None, layout: StorageLayout, legacy_phy_locks: tuple[Path, ...]
) -> tuple[tuple[str, str], ...]:
    if marker is None or set(marker) != {"version", "phase", "files", "state_sha256", "custody"}:
        raise StateMigrationError("migration cleanup marker is unsupported")
    if marker["version"] != _MIGRATION_VERSION or marker["phase"] != "cleanup_pending":
        raise StateMigrationError("migration cleanup marker is unsupported")
    if not isinstance(marker["files"], list) or not _is_sha256(marker["state_sha256"]):
        raise StateMigrationError("migration cleanup marker is unsupported")
    result: list[tuple[str, str]] = []
    for item in marker["files"]:
        if (
            not isinstance(item, list)
            or len(item) != _FINGERPRINT_PAIR_LENGTH
            or not all(isinstance(value, str) for value in item)
        ):
            raise StateMigrationError("migration cleanup inventory is malformed")
        path_string, expected = item
        path = Path(path_string)
        if not path.is_absolute() or ".." in path.parts or not _is_sha256(expected):
            raise StateMigrationError("migration cleanup inventory escapes Collector storage")
        if not path.is_relative_to(layout.root) and path not in legacy_phy_locks:
            raise StateMigrationError("migration cleanup inventory escapes Collector storage")
        if not _allowed_legacy_path(path, layout, legacy_phy_locks):
            raise StateMigrationError("migration cleanup inventory escapes Collector storage")
        result.append((path_string, expected))
    if len(set(result)) != len(result):
        raise StateMigrationError("migration cleanup inventory is duplicated")
    return tuple(result)


def _recovery_marker(recovery: _RetainedRecovery | None) -> dict[str, str] | None:
    if recovery is None:
        return None
    return {"manifest_sha256": recovery.manifest_sha256, "records_sha256": recovery.records_sha256}


def _verify_recovery_marker(layout: StorageLayout, marker: dict[str, object] | None) -> None:
    assert marker is not None
    recorded = marker.get("custody")
    if recorded is not None and (
        not isinstance(recorded, dict)
        or set(recorded) != {"manifest_sha256", "records_sha256"}
        or not all(_is_sha256(value) for value in recorded.values())
    ):
        raise StateMigrationError("migration custody fingerprint is malformed")
    recovery = _load_retained_recovery(layout)
    if recorded != _recovery_marker(recovery):
        raise StateMigrationError("retained recovery changed before migration cleanup")


def _is_sha256(value: object) -> bool:
    return (
        isinstance(value, str)
        and len(value) == _SHA256_HEX_LENGTH
        and all(char in "0123456789abcdef" for char in value)
    )


def _allowed_legacy_path(path: Path, layout: StorageLayout, legacy_phy_locks: tuple[Path, ...]) -> bool:
    root = layout.collector.root
    quality = DEFAULT_CONFIG.observability.quality_metrics
    debug = DEFAULT_CONFIG.observability.debug_log
    root_names = {
        *_LEGACY_SINGLETONS,
        quality.file_name,
        *(f"{quality.file_name}.{index}" for index in range(1, quality.backup_count + 1)),
        *(f"{debug.file_name}.{index}" for index in range(1, debug.backup_count + 1)),
    }
    return (
        path in legacy_phy_locks
        or (path.parent == root and path.name in root_names)
        or (
            path.parent.parent == root
            and path.parent.name in _CLOCK_DIRECTORIES
            and (path.name == ".lock" or path.suffix == ".json")
        )
        or (path.parent == layout.collector.quarantine and path.suffix == ".json")
        or (path.parent.parent == layout.collector.attempts and path.name in _ATTEMPT_STATE_NAMES)
        or (path.parent.parent == layout.collector.quarantine and path.name in _QUARANTINE_STATE_NAMES)
        or (path.parent.parent in {layout.draft, layout.publication.root} and path.name == "receipt.json")
    )


def _read_object(path: Path) -> dict[str, object]:
    _hash_regular_file(path)
    try:
        value = cast(object, json.loads(path.read_text(encoding="utf-8")))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise StateMigrationError(f"legacy state is unreadable: {path.name}") from error
    if not isinstance(value, dict):
        raise StateMigrationError(f"legacy state is not an object: {path.name}")
    return cast(dict[str, object], value)


def _recovery_manifest_path(layout: StorageLayout) -> Path:
    return layout.root / _RETAINED_RECOVERY


def _load_retained_recovery(layout: StorageLayout) -> _RetainedRecovery | None:
    """Authenticate the known, separately retained sparse rescue package."""
    manifest_path = _recovery_manifest_path(layout)
    if not _path_present(manifest_path):
        if _path_present(manifest_path.parent) or layout.root == Path("/srv/pipelines/omi"):
            raise StateMigrationError("the retained recovery passport is missing")
        return None
    _require_directory(manifest_path.parent.parent)
    _require_directory(manifest_path.parent)
    document = _read_object(manifest_path)
    window = document.get("source_window")
    counts = document.get("counts")
    publication = document.get("publication")
    expected = {
        "format": "omi-recovery-v1",
        "package_kind": "retained_sparse_sequence_recovery",
        "records_file": "records.bin",
        "record_size": RECORD_SIZE,
        "record_order": "ascending original physical sequence; exact byte-identical overlaps retained once",
    }
    if (
        any(document.get(key) != value for key, value in expected.items())
        or document.get("complete") is not True
        or publication
        != {"consumer_compatibility_asserted": False, "ready_bundle": False, "ready_ledger_written": False}
        or not _is_sha256(document.get("records_sha256"))
    ):
        raise StateMigrationError("retained recovery passport is unsupported")
    if not isinstance(window, dict) or not isinstance(counts, dict):
        raise StateMigrationError("retained recovery passport is unsupported")
    ranges = _recovery_ranges(document, window, counts)
    records_path = manifest_path.parent / "records.bin"
    expected_digest = cast(str, document["records_sha256"])
    if (
        records_path.stat(follow_symlinks=False).st_size != counts["output_bytes"]
        or _hash_regular_file(records_path) != expected_digest
    ):
        raise StateMigrationError("retained recovery records differ from passport")
    return _RetainedRecovery(
        _hash_regular_file(manifest_path),
        expected_digest,
        tuple((start, end) for start, end, _ in ranges),
        tuple((records_path, start, end - start, offset) for start, end, offset in ranges),
    )


def _recovery_ranges(
    document: dict[str, object], window: dict[str, object], counts: dict[str, object]
) -> tuple[tuple[int, int, int], ...]:
    raw_ranges, raw_gaps = document.get("output_ranges"), document.get("sequence_gaps")
    start, end = window.get("start_sequence"), window.get("closure_next_sequence")
    expected_count, expected_bytes = counts.get("unique_retained_records"), counts.get("output_bytes")
    if not isinstance(raw_ranges, list) or not raw_ranges or not isinstance(raw_gaps, list):
        raise StateMigrationError("retained recovery range inventory is invalid")
    if not _valid_interval(start, end) or type(expected_count) is not int or type(expected_bytes) is not int:
        raise StateMigrationError("retained recovery range inventory is invalid")
    intervals: list[tuple[int, int, int]] = []
    next_offset = 0
    for row in raw_ranges:
        lower, upper, offset = _parse_recovery_range(row, next_offset)
        if intervals and lower < intervals[-1][1]:
            raise StateMigrationError("retained recovery raw sequence order is invalid")
        intervals.append((lower, upper, offset))
        next_offset += (upper - lower) * RECORD_SIZE
    if next_offset != expected_bytes or sum(upper - lower for lower, upper, _ in intervals) != expected_count:
        raise StateMigrationError("retained recovery record count is invalid")
    _validate_recovery_gaps(intervals, raw_gaps, cast(int, start), cast(int, end), document)
    return tuple(intervals)


def _parse_recovery_range(row: object, expected_offset: int) -> tuple[int, int, int]:
    if not isinstance(row, dict):
        raise StateMigrationError("retained recovery range inventory is invalid")
    lower, upper, count, offset = (
        row.get("start_sequence"),
        row.get("next_sequence"),
        row.get("record_count"),
        row.get("records_file_offset_bytes"),
    )
    if not _valid_interval(lower, upper) or type(count) is not int or count != cast(int, upper) - cast(int, lower):
        raise StateMigrationError("retained recovery range inventory is invalid")
    if (
        type(offset) is not int
        or offset != expected_offset
        or not _is_sha256(row.get("source_sha256"))
        or not isinstance(row.get("source_path"), str)
    ):
        raise StateMigrationError("retained recovery range inventory is invalid")
    return cast(int, lower), cast(int, upper), cast(int, offset)


def _valid_interval(start: object, end: object) -> bool:
    return type(start) is int and type(end) is int and 0 <= start < end


def _validate_recovery_gaps(
    ranges: list[tuple[int, int, int]],
    gaps: list[object],
    start: int,
    end: int,
    document: dict[str, object],
) -> None:
    pieces = [(lower, upper) for lower, upper, _ in ranges]
    missing = 0
    for row in gaps:
        if not isinstance(row, dict):
            raise StateMigrationError("retained recovery gap inventory is invalid")
        lower, upper, count = row.get("start_sequence"), row.get("next_sequence"), row.get("record_count")
        if (
            not _valid_interval(lower, upper)
            or row.get("cause") != "unknown"
            or type(count) is not int
            or count != cast(int, upper) - cast(int, lower)
        ):
            raise StateMigrationError("retained recovery gap inventory is invalid")
        lower, upper = cast(int, lower), cast(int, upper)
        pieces.append((lower, upper))
        missing += count
    pieces.sort()
    if (
        pieces[0][0] != start
        or pieces[-1][1] != end
        or any(left[1] != right[0] for left, right in pairwise(pieces))
        or type(document.get("missing_sequence_count")) is not int
        or document.get("missing_sequence_count") != missing
    ):
        raise StateMigrationError("retained recovery sparse sequence order is invalid")


def _verify_retained_recovery(layout: StorageLayout, recovery: _RetainedRecovery | None) -> None:
    if recovery is None:
        if _path_present(_recovery_manifest_path(layout).parent):
            raise StateMigrationError("retained recovery appeared during migration")
        return
    if (
        _hash_regular_file(_recovery_manifest_path(layout)) != recovery.manifest_sha256
        or _hash_regular_file(recovery.sources[0][0]) != recovery.records_sha256
    ):
        raise StateMigrationError("retained recovery changed during migration")


def _validate_obsolete_clock_metadata(root: Path) -> None:
    """Classify historical repair/import receipts before their direct removal."""
    _validate_timeline_repairs(root / "timeline-repairs.json")
    _validate_clock_import_receipts(root / "clock-imports")


def _validate_timeline_repairs(repairs_path: Path) -> None:
    if _path_present(repairs_path):
        document = _read_object(repairs_path)
        repairs = document.get("repairs")
        if set(document) != {"version", "repairs"} or document["version"] != 1 or not isinstance(repairs, list):
            raise StateMigrationError("historical timeline repair ledger is unsupported")
        for row in repairs:
            if not isinstance(row, dict) or set(row) != {
                "evidence",
                "start_sequence",
                "next_sequence",
                "offset_seconds",
            }:
                raise StateMigrationError("historical timeline repair entry is unsupported")
            start, end, offset = row["start_sequence"], row["next_sequence"], row["offset_seconds"]
            if not isinstance(row["evidence"], str) or not row["evidence"]:
                raise StateMigrationError("historical timeline repair entry is unsupported")
            if type(start) is not int or type(end) is not int or start < 0 or end <= start:
                raise StateMigrationError("historical timeline repair entry is unsupported")
            if type(offset) not in {int, float} or not math.isfinite(offset):
                raise StateMigrationError("historical timeline repair entry is unsupported")


def _validate_clock_import_receipts(import_root: Path) -> None:
    if not import_root.is_dir():
        return
    for path in import_root.glob("*.json"):
        document = _read_object(path)
        entries, hashes, calculation = (
            document.get("entries"),
            document.get("entry_hashes"),
            document.get("boundary_calculation"),
        )
        if set(document) != {"version", "operation_id", "entries", "entry_hashes", "boundary_calculation"}:
            raise StateMigrationError("historical clock import receipt is unsupported")
        if document["version"] != 1 or not isinstance(document["operation_id"], str) or not document["operation_id"]:
            raise StateMigrationError("historical clock import receipt is unsupported")
        if not isinstance(entries, list) or not entries or not isinstance(hashes, list):
            raise StateMigrationError("historical clock import receipt is unsupported")
        if len(entries) != len(hashes) or not all(isinstance(entry, dict) for entry in entries):
            raise StateMigrationError("historical clock import receipt is unsupported")
        if not all(_is_sha256(item) for item in hashes) or not isinstance(calculation, dict):
            raise StateMigrationError("historical clock import receipt is unsupported")


def _legacy_rows(
    layout: StorageLayout, recovery: _RetainedRecovery | None = None
) -> dict[tuple[StateKind, str], dict[str, object]]:
    rows: dict[tuple[StateKind, str], dict[str, object]] = {}
    _snapshot_rows(layout.collector.root, rows)
    _publication_rows(layout.collector.root, rows)
    _attempt_rows(layout.collector.attempts, rows)
    _quarantine_rows(layout.collector.quarantine, rows)
    clock_state = _legacy_clock_state(layout, rows, recovery)
    if clock_state is not None:
        rows[StateKind.CLOCK, "current"] = clock_state.to_payload()
    return rows


def _snapshot_rows(root: Path, rows: dict[tuple[StateKind, str], dict[str, object]]) -> None:
    firmware_path = root / "device.json"
    if firmware_path.exists():
        parse_legacy_firmware(firmware_path)
        rows[StateKind.FIRMWARE, "latest"] = _read_object(firmware_path)
    status_path = root / "operational-status.json"
    if status_path.exists():
        parse_legacy_operational_status(status_path)
        rows[StateKind.OPERATIONAL_STATUS, "current"] = _read_object(status_path)
    loss_path = root / "confirmed-loss.json"
    if loss_path.exists():
        parse_legacy_confirmed_loss(loss_path)
        rows[StateKind.CONFIRMED_LOSS, "ledger"] = _read_object(loss_path)


def _publication_rows(root: Path, rows: dict[tuple[StateKind, str], dict[str, object]]) -> None:
    for item_id, payload in legacy_ready_publications(root / "ready-publications.json").items():
        rows[StateKind.READY_PUBLICATION, item_id] = payload
    closures = legacy_closures(root / "ready-closures.json")
    if closures is not None:
        rows[StateKind.READY_CLOSURE, "current"] = closures


def _attempt_rows(root: Path, rows: dict[tuple[StateKind, str], dict[str, object]]) -> None:
    if root.is_dir():
        for path in sorted(root.iterdir()):
            _require_directory(path)
            row = legacy_attempt_row(path)
            _add_attempt_row(rows, row)


def _quarantine_rows(root: Path, rows: dict[tuple[StateKind, str], dict[str, object]]) -> None:
    if root.is_dir():
        sidecars = _quarantine_sidecars(root)
        for path in sorted(root.iterdir()):
            if path.name in sidecars:
                continue
            row = legacy_quarantine_row(path)
            rows[StateKind.QUARANTINE, path.name] = row
            if path.is_dir() and (path / "attempt.json").exists():
                _add_attempt_row(rows, legacy_attempt_row(path))


def _add_attempt_row(rows: dict[tuple[StateKind, str], dict[str, object]], row: dict[str, object]) -> None:
    descriptor = cast(dict[str, object], row["descriptor"])
    key = (StateKind.ATTEMPT, cast(str, descriptor["attempt_id"]))
    if key in rows:
        raise StateMigrationError("legacy attempt identity is duplicated")
    rows[key] = row


def _legacy_clock_state(
    layout: StorageLayout, rows: dict[tuple[StateKind, str], dict[str, object]], recovery: _RetainedRecovery | None
) -> ClockState | None:
    clock = read_legacy_clock(layout.collector.root)
    observations = clock.observations
    corrections = clock.corrections
    if any(item.state in {"prepared", "unresolved"} for item in corrections):
        raise StateMigrationError("resolve the pending legacy clock write before migration")
    old_map = clock.segments
    intervals = _protected_intervals(layout, rows, recovery)
    state = ClockState()
    for correction in corrections:
        if correction.state != "unknown" or correction.boundary_sequence_max is None:
            continue
        for start, end in _intersections(correction.boundary_sequence_min, correction.boundary_sequence_max, intervals):
            state = state.add_range(ClockRange(start, end, None, None, "unknown", correction.operation_id))
    for segment in old_map.segments:
        for start, end in _intersections(segment.start_sequence, segment.next_sequence, intervals):
            state = state.add_range(
                ClockRange(
                    start,
                    end,
                    segment.utc_offset_seconds,
                    segment.uncertainty_seconds,
                    segment.confidence,
                    segment.observation_id,
                )
            )
    latest = next((item for item in reversed(observations) if item.evidence_kind == "native_trusted"), None)
    if latest is not None:
        midpoint = (latest.host_realtime_start + latest.host_realtime_end) / 2
        state = state.with_anchor(
            ClockAnchor(
                latest.info_sequence_max,
                midpoint - latest.device_epoch,
                (latest.host_realtime_end - latest.host_realtime_start) / 2 + 1,
                "approximate",
                latest.observation_id,
            )
        )
    return state if observations or corrections or state.ranges else None


def _protected_intervals(
    layout: StorageLayout,
    rows: dict[tuple[StateKind, str], dict[str, object]],
    recovery: _RetainedRecovery | None = None,
) -> tuple[tuple[int, int], ...]:
    intervals: list[tuple[int, int]] = []
    firmware = rows.get((StateKind.FIRMWARE, "latest"))
    if firmware is not None:
        latest = cast(dict[str, object], firmware["latest"])
        start, end = latest["read_sequence"], latest["write_sequence"]
        if isinstance(start, int) and isinstance(end, int) and end > start:
            intervals.append((start, end))
    for (kind, _), row in rows.items():
        if kind is not StateKind.ATTEMPT:
            continue
        descriptor = cast(dict[str, object], row["descriptor"])
        start = cast(int, descriptor["start_sequence"])
        intervals.append((start, start + cast(int, descriptor["packet_count"])))
    if layout.draft.is_dir() and layout.publication.root.is_dir():
        inventory = authenticated_inventory(layout.draft, layout.publication.root, None)
        intervals.extend((item.manifest.start_sequence, item.manifest.next_sequence) for item in inventory.drafts)
    if recovery is not None:
        intervals.extend(recovery.intervals)
    return tuple(intervals)


def _intersections(start: int, end: int, intervals: tuple[tuple[int, int], ...]) -> Iterator[tuple[int, int]]:
    for lower, upper in intervals:
        left, right = max(start, lower), min(end, upper)
        if right > left:
            yield left, right


def _state_digest(db: StateDB) -> str:
    records = [
        (kind.value, item_id, payload)
        for kind in StateKind
        if kind is not StateKind.MIGRATION
        for item_id, payload in db.list(kind)
    ]
    data = json.dumps(records, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
    return sha256(data).hexdigest()


def _import_legacy(layout: StorageLayout, db: StateDB, recovery: _RetainedRecovery | None) -> None:
    rows = _legacy_rows(layout, recovery)
    with db.transaction() as transaction:
        for (kind, item_id), payload in rows.items():
            transaction.put(kind, item_id, payload)
    quality = _quality_rows(layout.collector.root)
    quality_config = DEFAULT_CONFIG.observability.quality_metrics
    import_quality_events(
        db,
        ((quality_event_id(row, suffix=str(index)), row) for index, row in enumerate(quality)),
        max_bytes=quality_config.max_bytes * (quality_config.backup_count + 1),
    )
    debug_path = layout.collector.root / DEFAULT_CONFIG.observability.debug_log.file_name
    if debug_path.exists():
        import_legacy_runtime_status(db, _debug_rows(debug_path))


def _validate_parity(
    layout: StorageLayout,
    db: StateDB,
    *,
    allow_removed: bool = False,
    recovery: _RetainedRecovery | None = None,
) -> None:
    db.integrity_check()
    read_firmware_observations(db)
    read_confirmed_losses(db, allow_missing=db.get(StateKind.CONFIRMED_LOSS, "ledger") is None)
    clock_payload = db.get(StateKind.CLOCK, "current")
    if clock_payload is not None:
        ClockState.from_payload(clock_payload)
    if layout.draft.is_dir() and layout.publication.root.is_dir():
        authenticated_inventory(layout.draft, layout.publication.root, None)
    if allow_removed:
        return
    legacy_rows = _legacy_rows(layout, recovery)
    for (kind, item_id), expected in legacy_rows.items():
        if db.get(kind, item_id) != expected:
            raise StateMigrationError(f"legacy {kind.value} state changed during import")
    _validate_clock_projection_parity(layout, db, legacy_rows, recovery)
    quality = _quality_rows(layout.collector.root)
    imported = tuple(payload for _, payload in db.list(StateKind.QUALITY_EVENT))
    if sorted(map(_canonical, imported)) != sorted(map(_canonical, quality)):
        raise StateMigrationError("quality-event history differs after import")
    debug_path = layout.collector.root / DEFAULT_CONFIG.observability.debug_log.file_name
    debug_rows = _debug_rows(debug_path) if debug_path.exists() else ()
    if read_runtime_status(db) != _legacy_runtime_projection(debug_rows):
        raise StateMigrationError("current runtime status differs after import")


def _validate_clock_projection_parity(
    layout: StorageLayout,
    db: StateDB,
    rows: dict[tuple[StateKind, str], dict[str, object]],
    recovery: _RetainedRecovery | None,
) -> None:
    """Compare old and new publication projections over every reachable interval."""
    old_map = read_legacy_clock(layout.collector.root).segments
    new_payload = db.get(StateKind.CLOCK, "current")
    new_map = ClockSegmentMap.from_state(
        ClockState.from_payload(new_payload) if new_payload is not None else ClockState()
    )
    for start, end in _protected_intervals(layout, rows, recovery):
        boundaries = {start, end}
        for segment in (*old_map.segments, *new_map.segments):
            if start < segment.next_sequence and segment.start_sequence < end:
                boundaries.add(max(start, segment.start_sequence))
                boundaries.add(min(end, segment.next_sequence))
        ordered = sorted(boundaries)
        for left, right in pairwise(ordered):
            if _time_range(left, right, old_map) != _time_range(left, right, new_map):
                raise StateMigrationError("clock time_ranges differ after import")
    for source in _reachable_raw_sources(layout):
        _validate_raw_clock_parity(source, old_map, new_map)
    if recovery is not None:
        for source in recovery.sources:
            _validate_raw_clock_parity(source, old_map, new_map)


def _reachable_raw_sources(layout: StorageLayout) -> Iterator[tuple[Path, int, int, int]]:
    for root in (layout.collector.attempts, layout.collector.quarantine):
        if not root.is_dir():
            continue
        for path in sorted(root.iterdir()):
            if not path.is_dir() or not (path / "attempt.json").exists():
                continue
            row = legacy_attempt_row(path)
            descriptor = cast(dict[str, object], row["descriptor"])
            checkpoint = cast(dict[str, object], row["checkpoint"])
            yield (
                path / "records.bin",
                cast(int, descriptor["start_sequence"]),
                cast(int, checkpoint["record_count"]),
                0,
            )
    if layout.draft.is_dir() and layout.publication.root.is_dir():
        inventory = authenticated_inventory(layout.draft, layout.publication.root, None)
        for draft in inventory.drafts:
            yield (
                draft.path / "records.bin",
                draft.manifest.start_sequence,
                draft.manifest.record_count,
                draft.byte_offset,
            )


def _validate_raw_clock_parity(
    source: tuple[Path, int, int, int],
    old_map: ClockSegmentMap,
    new_map: ClockSegmentMap,
) -> None:
    path, start, count, byte_offset = source
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC)
    try:
        if not stat.S_ISREG(os.fstat(descriptor).st_mode):
            raise StateMigrationError("reachable raw audio is not a regular file")
        os.lseek(descriptor, byte_offset, os.SEEK_SET)
        for sequence in range(start, start + count):
            record = os.read(descriptor, RECORD_SIZE)
            if len(record) != RECORD_SIZE:
                raise StateMigrationError("reachable raw audio ended during clock parity check")
            old_range = _time_range(sequence, sequence + 1, old_map)
            new_range = _time_range(sequence, sequence + 1, new_map)
            if _normalized_record(record, old_range) != _normalized_record(record, new_range):
                raise StateMigrationError("normalized raw timestamp differs after clock import")
    finally:
        os.close(descriptor)


def _normalized_record(record: bytes, time_range: dict[str, object]) -> tuple[bytes, bool]:
    try:
        return _convert_record(record, time_range), False
    except ReadyBundleError:
        return record, True


def _legacy_runtime_projection(rows: Iterable[dict[str, object]]) -> dict[str, object] | None:
    projection: dict[str, object] | None = None
    for row in rows:
        event = row.get("event")
        if event not in {"sync_progress", "ble_link_rssi_observed"}:
            continue
        timestamp, fields = row.get("timestamp"), row.get("fields")
        if not isinstance(timestamp, str) or not isinstance(fields, dict):
            raise StateMigrationError("legacy debug status record is invalid")
        projection = merge_runtime_status(projection, cast(str, event), fields, timestamp)
    return projection


def _canonical(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


def _remove_empty_legacy_directories(layout: StorageLayout) -> None:
    for name in _CLOCK_DIRECTORIES:
        path = layout.collector.root / name
        if path.is_dir():
            path.rmdir()
            _fsync_directory(path.parent)
