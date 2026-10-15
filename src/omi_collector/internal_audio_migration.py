"""One-way, stopped-service v1 SQLite to typed internal audio cutover.

The v1 database and private files are never modified after the source receipt
except for the receipt itself.  All conversion happens in a SQLite backup on
the same filesystem.  The selected new release owns every retry after the
candidate is activated.
"""

from __future__ import annotations

import errno
import grp
import hashlib
import json
import os
import pwd
import re
import sqlite3
import stat
import time
from collections.abc import Iterable
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from typing import cast
from urllib.parse import quote
from uuid import uuid4

from omi_collector.capture.adapters.collector_guard import collector_guard
from omi_collector.capture.adapters.collector_state_db import StateDB, StateDBError, StateKind, install_audio_schema
from omi_collector.capture.adapters.legacy_audio_import import (
    LegacyAttempt,
    LegacyAudioImportError,
    LegacyDraft,
    import_legacy_attempt,
    import_legacy_draft,
    inspect_legacy_attempt,
    inspect_legacy_draft,
    verify_imported_attempt,
    verify_imported_draft,
)
from omi_collector.capture.adapters.quarantine import _validate_quarantine_payload
from omi_collector.capture.adapters.sqlite_admission import admit_exclusive
from omi_collector.capture.adapters.staging_contract import AttemptStateError
from omi_collector.config import StagingRetentionConfig
from omi_collector.state_migration import StateMigrationError, _assert_private_audio_gone
from omi_collector.storage_layout import StorageLayout

_RECEIPT_ID = "internal_audio_cutover"
_FORMAT = 1
_TYPED_SCHEMA = 2
_SHA256_LENGTH = 64
_DATABASE_MODE = 0o640
_COLLECTOR_PARENT_MODE = 0o2750
_CANDIDATE_PREFIX = ".collector-state.sqlite3.migrating."
_UUID = re.compile(r"[0-9a-f]{32}\Z")
_REVISION = re.compile(r"[0-9a-f]{40,64}\Z")
_BUFFER_SIZE = 1 << 20
_EVIDENCE_CHUNK_SIZE = 128 * 444
_SPACE_HEADROOM_BYTES = 64 * 1024 * 1024  # SQLite metadata, allocation rounding, and filesystem reserve.
_RETENTION_NS = int(StagingRetentionConfig().terminal_retention_seconds * 1_000_000_000)
_PRIVATE_ROOTS = ("collector/attempts", "collector/quarantine", "draft")


class InternalAudioMigrationError(RuntimeError):
    """Forward cutover stopped before any unverified custody change."""


class _CanonicalJournalRecoveryRequiredError(InternalAudioMigrationError):
    """The canonical database needs SQLite rollback before read-only inspection."""


@dataclass(frozen=True, slots=True)
class _FileIdentity:
    path: str
    kind: str
    device: int
    inode: int
    mode: int
    size: int
    sha256: str | None
    symlink_hex: str | None

    def as_dict(self) -> dict[str, object]:
        return {
            "path": self.path,
            "kind": self.kind,
            "device": self.device,
            "inode": self.inode,
            "mode": self.mode,
            "size": self.size,
            "sha256": self.sha256,
            "symlink_hex": self.symlink_hex,
        }


@dataclass(frozen=True, slots=True)
class _Inventory:
    entries: tuple[_FileIdentity, ...]
    drafts: tuple[LegacyDraft, ...]
    attempts: tuple[LegacyAttempt, ...] = ()
    quarantines: tuple[_Quarantine, ...] = ()


@dataclass(frozen=True, slots=True)
class _Quarantine:
    root: Path
    path: Path | None
    item_id: str
    row: dict[str, object]


@dataclass(frozen=True, slots=True)
class _QuarantineFields:
    disposition: str
    terminal_time: str | None
    deadline: str | None
    reason: str
    terminal_reason: str | None


@dataclass(frozen=True, slots=True)
class _OwnedCandidate:
    path: Path
    identity: tuple[int, int]


def migrate_internal_audio(layout: StorageLayout, *, release_metadata: Path) -> dict[str, int]:
    """Run or resume a single guarded v1→v2 cutover before service startup."""

    revision = _release_revision(release_metadata)
    with collector_guard(layout.collector.lock):
        path = layout.collector.state_db
        _assert_real_canonical(path)
        if not path.exists():
            return _create_empty(layout)
        with closing(_connect(path)) as canonical:
            version = _schema_version(canonical)
            if version == 1:
                return _migrate_v1(layout, canonical, revision)
            if version == _TYPED_SCHEMA:
                return _resume_v2(layout, canonical)
            raise InternalAudioMigrationError(f"unsupported Collector SQLite schema version: {version}")


def inspect_internal_audio_migration(layout: StorageLayout, *, release_metadata: Path) -> dict[str, object]:
    """Check migration compatibility without opening writable state or changing files."""

    try:
        _release_revision(release_metadata)
        path = layout.collector.state_db
        _assert_real_canonical(path)
        if not path.exists():
            _validate_missing_database(layout)
            schema_version = None
        else:
            with closing(_connect_read_only(path)) as canonical:
                schema_version = _schema_version(canonical)
                if schema_version == 1:
                    _validate_v1_preflight(layout, canonical)
                elif schema_version == _TYPED_SCHEMA:
                    _validate_v2_preflight(layout, canonical)
                else:
                    return {
                        "detail": f"unsupported Collector SQLite schema version: {schema_version}",
                        "reason": "unsupported_schema",
                        "status": "migration_refused",
                    }
    except _CanonicalJournalRecoveryRequiredError:
        return {"reason": "canonical_hot_journal", "status": "migration_recovery_required"}
    except InternalAudioMigrationError as error:
        return {"detail": str(error), "reason": "state_validation_failed", "status": "migration_refused"}
    except (
        OSError,
        sqlite3.DatabaseError,
        StateDBError,
        StateMigrationError,
        LegacyAudioImportError,
        ValueError,
    ) as error:
        return {
            "detail": str(error) or type(error).__name__,
            "reason": "inspection_failed",
            "status": "migration_refused",
        }
    return {"schema_version": schema_version, "status": "migration_compatible"}


def recover_canonical_journal(layout: StorageLayout, *, release_metadata: Path) -> dict[str, object]:
    """Perform only SQLite crash recovery under the stopped-service lock."""

    with collector_guard(layout.collector.lock):
        inspection = inspect_internal_audio_migration(layout, release_metadata=release_metadata)
        if inspection.get("status") == "migration_compatible":
            return inspection
        if inspection != {"reason": "canonical_hot_journal", "status": "migration_recovery_required"}:
            raise InternalAudioMigrationError("canonical journal recovery was not approved by read-only inspection")
        path = layout.collector.state_db
        before = _canonical_journal_identity(path)
        with closing(_connect(path)):
            pass
        info = path.lstat()
        if (info.st_dev, info.st_ino, info.st_uid) != before:
            raise InternalAudioMigrationError("canonical SQLite identity changed during journal recovery")
        _assert_no_journal(path)
        inspection = inspect_internal_audio_migration(layout, release_metadata=release_metadata)
        if inspection.get("status") != "migration_compatible":
            raise InternalAudioMigrationError("canonical state remains incompatible after SQLite recovery")
        return inspection


def _validate_missing_database(layout: StorageLayout) -> None:
    inventory = _inventory(layout)
    if inventory.entries or _old_control_present(layout) or _any_ready_entry(layout):
        raise InternalAudioMigrationError("canonical DB is missing while Collector data exists")


def _validate_v1_preflight(layout: StorageLayout, source: sqlite3.Connection) -> None:
    _candidate_parent_identity(layout.collector.root)
    inventory = _inventory(layout, source)
    source_sha = _logical_digest(source)
    _validate_v1_control(source)
    receipt = _read_receipt(source)
    _assert_candidate_namespace(layout, receipt)
    if receipt is None:
        return
    _validate_v1_receipt(receipt, layout, inventory, source_sha)
    candidate = layout.collector.root / cast(str, receipt["candidate"])
    journal = candidate.parent / f"{candidate.name}-journal"
    if candidate.exists() or candidate.is_symlink():
        _check_owned_candidate(candidate, receipt)
    if journal.exists() or journal.is_symlink():
        _check_owned_candidate_journal(journal, receipt)


def _validate_v2_preflight(layout: StorageLayout, canonical: sqlite3.Connection) -> None:
    _require_typed_schema(layout.collector.state_db, "activated database")
    receipt = _read_receipt(canonical)
    if receipt is None:
        _validate_interrupted_fresh(layout, canonical)
        return
    if receipt.get("phase") == "COMPLETE":
        fresh = receipt == {"format": _FORMAT, "phase": "COMPLETE", "fresh": True}
        if not fresh:
            _validate_complete_receipt(receipt)
        _assert_complete_v2_read_only(layout, canonical)
        return
    _validate_resumable_v2(layout, canonical, receipt)


def _assert_complete_v2_read_only(layout: StorageLayout, canonical: sqlite3.Connection) -> None:
    if _remaining_inventory(layout) or _legacy_mutable_rows(canonical):
        raise InternalAudioMigrationError("private legacy audio remains after COMPLETE")
    try:
        _assert_private_audio_gone(layout, canonical)
    except StateMigrationError as error:
        raise InternalAudioMigrationError("private audio or unowned ready scratch remains after COMPLETE") from error


def _validate_resumable_v2(
    layout: StorageLayout, canonical: sqlite3.Connection, receipt: dict[str, object]
) -> tuple[_FileIdentity, ...]:
    _validate_receipt_shape(receipt, layout)
    phase = receipt["phase"]
    if phase not in {"PREPARED", "CLEANUP_PENDING"}:
        raise InternalAudioMigrationError("v2 cutover receipt is incomplete")
    expected = _receipt_entries(receipt)
    present = _remaining_inventory(layout)
    _assert_remaining_inventory(expected, present)
    if phase == "PREPARED" and present != expected:
        raise InternalAudioMigrationError("prepared cutover lost private audio before cleanup began")
    if _legacy_mutable_rows(canonical):
        raise InternalAudioMigrationError("v2 database retained legacy attempt/quarantine rows")
    return expected


def _validate_interrupted_fresh(layout: StorageLayout, canonical: sqlite3.Connection) -> None:
    try:
        StateDB(layout.collector.state_db, read_only=True)
    except StateDBError as error:
        raise InternalAudioMigrationError("typed database without receipt has unsupported schema") from error
    if _old_control_present(layout) or _any_ready_entry(layout) or _remaining_inventory(layout):
        raise InternalAudioMigrationError("typed database without receipt contains Collector data")
    owner = layout.collector.state_db.lstat()
    if owner.st_uid not in {os.geteuid(), layout.collector.root.lstat().st_uid}:
        raise InternalAudioMigrationError("typed database without receipt has unknown owner")
    _assert_empty_fresh_sql(canonical)


def _create_empty(layout: StorageLayout) -> dict[str, int]:
    inventory = _inventory(layout)
    if inventory.entries or _old_control_present(layout) or _any_ready_entry(layout):
        raise InternalAudioMigrationError("canonical DB is missing while Collector data exists")
    database = StateDB(layout.collector.state_db)
    database.put(StateKind.MIGRATION, "current", {"version": 1, "phase": "complete"})
    _handoff_canonical(layout.collector.state_db)
    _verify_handoff_access(layout.collector.state_db)
    database.put(StateKind.MIGRATION, _RECEIPT_ID, {"format": _FORMAT, "phase": "COMPLETE", "fresh": True})
    _fsync_directory(layout.collector.root)
    return {"imported_sources": 0, "removed_files": 0}


def _migrate_v1(layout: StorageLayout, source: sqlite3.Connection, revision: str) -> dict[str, int]:
    parent = _candidate_parent_identity(layout.collector.root)
    inventory = _inventory(layout, source)
    source_sha = _logical_digest(source)
    _validate_v1_control(source)
    receipt = _read_receipt(source)
    resumed_receipt = receipt is not None
    _assert_candidate_namespace(layout, receipt)
    if receipt is None:
        _require_candidate_space(layout, source, inventory)
        receipt = {
            "format": _FORMAT,
            "phase": "BUILDING",
            "uuid": uuid4().hex,
            "source_revision": revision,
            "source_sha256": source_sha,
            "candidate": "",
            "uid": os.geteuid(),
            "gid": os.getegid(),
            "candidate_gid": parent["gid"],
            "parent": parent,
            "files": [entry.as_dict() for entry in inventory.entries],
            "quarantines": [{"id": item.item_id, "row": item.row} for item in inventory.quarantines],
        }
        receipt["candidate"] = _CANDIDATE_PREFIX + cast(str, receipt["uuid"])
        _put_receipt(source, receipt)
    else:
        _validate_v1_receipt(receipt, layout, inventory, source_sha)
    _assert_candidate_parent(layout.collector.root, receipt)
    candidate = layout.collector.root / cast(str, receipt["candidate"])
    journal = candidate.parent / f"{candidate.name}-journal"
    journal_identity = None
    if journal.exists() or journal.is_symlink():
        journal_identity = _check_owned_candidate_journal(journal, receipt)
    if candidate.exists() or candidate.is_symlink():
        candidate_identity = _check_owned_candidate(candidate, receipt)
        if _candidate_is_prepared(candidate, source, receipt, inventory):
            return _activate_candidate(
                layout, source, _OwnedCandidate(candidate, candidate_identity), receipt, inventory
            )
        _remove_owned_candidate(candidate, receipt, candidate_identity, journal_identity)
    else:
        _remove_owned_candidate_journal(candidate, receipt, journal_identity)
    if resumed_receipt:
        _require_candidate_space(layout, source, inventory)
    _assert_candidate_parent(layout.collector.root, receipt)
    _backup_and_convert(source, candidate, receipt, inventory)
    if not _candidate_is_prepared(candidate, source, receipt, inventory):
        raise InternalAudioMigrationError("candidate parity failed after conversion")
    candidate_identity = _check_owned_candidate(candidate, receipt)
    return _activate_candidate(layout, source, _OwnedCandidate(candidate, candidate_identity), receipt, inventory)


def _assert_candidate_namespace(layout: StorageLayout, receipt: dict[str, object] | None) -> None:
    expected = receipt.get("candidate") if receipt is not None else None
    expected_journal = f"{expected}-journal" if isinstance(expected, str) else None
    for entry in layout.collector.root.iterdir():
        if entry.name == ".collector-state.sqlite3.migrating":
            raise InternalAudioMigrationError("unsupported legacy cutover candidate blocks migration")
        if entry.name.startswith(_CANDIDATE_PREFIX) and entry.name not in {expected, expected_journal}:
            raise InternalAudioMigrationError(f"foreign cutover candidate blocks migration: {entry.name}")


def _require_candidate_space(layout: StorageLayout, source: sqlite3.Connection, inventory: _Inventory) -> None:
    """Reserve a full backup, bounded typed growth, a rollback journal, and headroom."""

    page_size = cast(int, source.execute("PRAGMA page_size").fetchone()[0])
    page_count = cast(int, source.execute("PRAGMA page_count").fetchone()[0])
    freelist_count = cast(int, source.execute("PRAGMA freelist_count").fetchone()[0])
    if page_size <= 0 or page_count <= 0 or not 0 <= freelist_count <= page_count:
        raise InternalAudioMigrationError("v1 SQLite page accounting is invalid")
    candidate_base = max(layout.collector.state_db.stat().st_size, page_count * page_size)
    record_count = sum(draft.manifest.record_count for draft in inventory.drafts)
    record_count += sum(attempt.raw_size // 444 for attempt in inventory.attempts)
    evidence = (entry for item in inventory.quarantines for entry in _quarantine_entries(item, inventory.entries))
    evidence_count = 0
    evidence_pages = 0
    for entry in evidence:
        evidence_count += 1
        if entry.kind == "regular":
            chunks = (entry.size + _EVIDENCE_CHUNK_SIZE - 1) // _EVIDENCE_CHUNK_SIZE
            evidence_pages += (entry.size + page_size - 1) // page_size + 2 * chunks
    # Measured 1/128/129/257/4096-record fixture growth stays below 1.32 raw bytes per byte.
    # Two whole SQLite pages per record and four per source/evidence row retain margin for splits.
    typed_pages = 64 + 2 * record_count + 4 * (len(inventory.drafts) + len(inventory.attempts) + evidence_count)
    typed_growth = max(0, typed_pages + evidence_pages - freelist_count) * page_size
    # A backup can journal its whole pre-existing database; chunk imports mutate at most 128 rows.
    journal_bound = max(candidate_base + 16 * page_size, (2 * 128 + 64) * page_size)
    required = candidate_base + typed_growth + journal_bound + _SPACE_HEADROOM_BYTES
    filesystem = os.statvfs(layout.collector.root)
    available = filesystem.f_bavail * filesystem.f_frsize
    if available < required:
        raise InternalAudioMigrationError(
            "insufficient Collector SQLite filesystem space before cutover receipt: "
            f"page_count={page_count} freelist_count={freelist_count} "
            f"candidate_base={candidate_base} typed_growth={typed_growth} "
            f"journal_bound={journal_bound} headroom={_SPACE_HEADROOM_BYTES} "
            f"required={required} available={available}"
        )


def _backup_and_convert(
    source: sqlite3.Connection, candidate: Path, receipt: dict[str, object], inventory: _Inventory
) -> None:
    if candidate.exists() or candidate.is_symlink():
        raise InternalAudioMigrationError("candidate path appeared before backup")
    try:
        with closing(sqlite3.connect(candidate, isolation_level=None)) as target:
            source.backup(target)
            target.execute("PRAGMA foreign_keys = ON")
            install_audio_schema(target)
            for draft in inventory.drafts:
                import_legacy_draft(target, draft)
            for attempt in inventory.attempts:
                authoritative = _v1_payload(source, "attempt", attempt.attempt_id)
                import_legacy_attempt(target, attempt, authoritative_row=authoritative)
                target.execute("DELETE FROM state WHERE kind='attempt' AND id=?", (attempt.attempt_id,))
            for item in inventory.quarantines:
                _import_quarantine(target, item, inventory.entries)
                target.execute("DELETE FROM state WHERE kind='quarantine' AND id=?", (item.item_id,))
            if _legacy_mutable_rows(target):
                raise InternalAudioMigrationError("v1 attempt/quarantine row conversion is not complete")
            _assert_inventory_exact(inventory, _inventory_from_paths(inventory, candidate.parent.parent))
            for draft in inventory.drafts:
                verify_imported_draft(target, draft)
            for attempt in inventory.attempts:
                verify_imported_attempt(target, attempt)
            _verify_quarantines(target, inventory)
            _put_receipt(target, {**receipt, "phase": "PREPARED"})
            _integrity_check(target)
        _fsync_file(candidate)
        _fsync_directory(candidate.parent)
    except (OSError, sqlite3.DatabaseError, LegacyAudioImportError) as error:
        raise InternalAudioMigrationError("candidate backup or audio conversion failed") from error


def _candidate_is_prepared(
    path: Path, source: sqlite3.Connection, receipt: dict[str, object], inventory: _Inventory
) -> bool:
    try:
        with closing(_connect(path)) as candidate:
            if _schema_version(candidate) != _TYPED_SCHEMA:
                return False
            if _read_receipt(candidate) != {**receipt, "phase": "PREPARED"}:
                return False
            _require_typed_schema(path, "prepared candidate")
            if _retained_digest(source) != _retained_digest(candidate):
                return False
            if _legacy_mutable_rows(candidate):
                return False
            for draft in inventory.drafts:
                verify_imported_draft(candidate, draft)
            for attempt in inventory.attempts:
                verify_imported_attempt(candidate, attempt)
            _verify_quarantines(candidate, inventory)
        _assert_no_journal(path)
        return True
    except OSError, sqlite3.DatabaseError, InternalAudioMigrationError, LegacyAudioImportError:
        return False


def _require_typed_schema(path: Path, description: str) -> None:
    try:
        StateDB(path, read_only=True)
    except StateDBError as error:
        raise InternalAudioMigrationError(f"{description} has unsupported typed schema") from error


def _activate_candidate(
    layout: StorageLayout,
    source: sqlite3.Connection,
    candidate: _OwnedCandidate,
    receipt: dict[str, object],
    inventory: _Inventory,
) -> dict[str, int]:
    _assert_candidate_parent(layout.collector.root, receipt)
    _assert_inventory_exact(inventory, _inventory(layout, source))
    if _logical_digest(source) != receipt["source_sha256"]:
        raise InternalAudioMigrationError("v1 source changed before activation")
    source.close()
    _assert_no_journal(layout.collector.state_db)
    _assert_no_journal(candidate.path)
    _assert_candidate_identity(candidate.path, receipt, candidate.identity)
    _require_typed_schema(candidate.path, "prepared candidate")
    _assert_candidate_parent(layout.collector.root, receipt)
    _assert_candidate_identity(candidate.path, receipt, candidate.identity)
    candidate.path.replace(layout.collector.state_db)
    _fsync_directory(layout.collector.root)
    with closing(_connect(layout.collector.state_db)) as canonical:
        return _resume_v2(layout, canonical)


def _resume_v2(layout: StorageLayout, canonical: sqlite3.Connection) -> dict[str, int]:
    if _schema_version(canonical) != _TYPED_SCHEMA:
        raise InternalAudioMigrationError("activated database is not v2")
    _require_typed_schema(layout.collector.state_db, "activated database")
    receipt = _read_receipt(canonical)
    if receipt is None:
        return _recover_interrupted_fresh(layout, canonical)
    if receipt.get("phase") == "COMPLETE":
        return _resume_complete(layout, canonical, receipt)
    expected = _validate_resumable_v2(layout, canonical, receipt)
    _put_receipt(canonical, {**receipt, "phase": "CLEANUP_PENDING"})
    removed = _cleanup_legacy(layout, expected)
    if _remaining_inventory(layout):
        raise InternalAudioMigrationError("private legacy audio remains after cleanup")
    _handoff_canonical(layout.collector.state_db)
    _verify_handoff_access(layout.collector.state_db)
    imported_sources = _imported_source_count(canonical)
    _put_receipt(
        canonical,
        {
            "format": _FORMAT,
            "phase": "COMPLETE",
            "source_revision": receipt["source_revision"],
            "source_sha256": receipt["source_sha256"],
            "imported_sources": imported_sources,
        },
    )
    return {"imported_sources": imported_sources, "removed_files": removed}


def _resume_complete(
    layout: StorageLayout, canonical: sqlite3.Connection, receipt: dict[str, object]
) -> dict[str, int]:
    fresh = receipt == {"format": _FORMAT, "phase": "COMPLETE", "fresh": True}
    if not fresh:
        _validate_complete_receipt(receipt)
    if _remaining_inventory(layout) or _legacy_mutable_rows(canonical):
        raise InternalAudioMigrationError("private legacy audio remains after COMPLETE")
    try:
        _assert_private_audio_gone(layout, canonical)
    except StateMigrationError as error:
        raise InternalAudioMigrationError("private audio or unowned ready scratch remains after COMPLETE") from error
    _handoff_canonical(layout.collector.state_db)
    _verify_handoff_access(layout.collector.state_db)
    return {"imported_sources": 0 if fresh else cast(int, receipt["imported_sources"]), "removed_files": 0}


def _recover_interrupted_fresh(layout: StorageLayout, canonical: sqlite3.Connection) -> dict[str, int]:
    """Finish an empty v2 schema left before the first fresh receipt commit."""

    _validate_interrupted_fresh(layout, canonical)
    _handoff_canonical(layout.collector.state_db)
    _verify_handoff_access(layout.collector.state_db)
    admit_exclusive(canonical)
    try:
        canonical.execute(
            "INSERT INTO state(kind,id,payload) VALUES ('migration','current',?) "
            "ON CONFLICT(kind,id) DO UPDATE SET payload=excluded.payload",
            ('{"phase":"complete","version":1}',),
        )
        canonical.execute(
            "INSERT INTO state(kind,id,payload) VALUES ('migration',?,?)",
            (_RECEIPT_ID, '{"format":1,"fresh":true,"phase":"COMPLETE"}'),
        )
        canonical.commit()
    except BaseException:
        canonical.rollback()
        raise
    return {"imported_sources": 0, "removed_files": 0}


def _assert_empty_fresh_sql(canonical: sqlite3.Connection) -> None:
    for table in (
        "capture_source",
        "capture_record",
        "capture_tail",
        "capture_conflict",
        "capture_evidence",
        "capture_evidence_chunk",
        "publication_cohort",
        "publication_cohort_member",
        "ready_output_stage",
        "ready_stage_range",
    ):
        if canonical.execute(f"SELECT 1 FROM {table} LIMIT 1").fetchone() is not None:
            raise InternalAudioMigrationError("typed database without receipt contains audio state")
    rows = list(cast(Iterable[tuple[str, str, str]], canonical.execute("SELECT kind,id,payload FROM state")))
    if rows and rows != [("migration", "current", '{"phase":"complete","version":1}')]:
        raise InternalAudioMigrationError("typed database without receipt contains state")


def _inventory(layout: StorageLayout, source: sqlite3.Connection | None = None) -> _Inventory:
    entries: list[_FileIdentity] = []
    attempt_rows = _v1_rows(source, "attempt") if source is not None else {}
    quarantine_rows = _v1_rows(source, "quarantine") if source is not None else {}
    quarantines = tuple(_inspect_quarantine(layout, item_id, row) for item_id, row in quarantine_rows.items())
    quarantine_paths = {item.path for item in quarantines if item.path is not None}
    drafts = _inventory_drafts(layout, quarantine_paths, entries)
    attempts, seen_attempts = _inventory_attempts(layout, attempt_rows, quarantine_paths, entries)
    _validate_quarantine_root(layout, quarantine_paths)
    for item in quarantines:
        if item.path is not None:
            _walk_private(layout.root, item.path, entries)
    missing_attempts = sorted(set(attempt_rows) - seen_attempts)
    if missing_attempts:
        raise InternalAudioMigrationError(
            f"v1 attempt row lacks its private directory and cannot be converted: {missing_attempts[0]}"
        )
    _validate_ready_scratch(layout)
    return _Inventory(tuple(sorted(entries, key=lambda item: item.path)), tuple(drafts), tuple(attempts), quarantines)


def _inventory_drafts(
    layout: StorageLayout, blocked: set[Path], entries: list[_FileIdentity]
) -> tuple[LegacyDraft, ...]:
    root = layout.draft
    if not root.exists() and not root.is_symlink():
        return ()
    _real_directory(root)
    if root in blocked:
        return ()
    drafts: list[LegacyDraft] = []
    for child in sorted(root.iterdir()):
        if child in blocked:
            continue
        drafts.append(inspect_legacy_draft(child))
        _walk_private(layout.root, child, entries)
    return tuple(drafts)


def _inventory_attempts(
    layout: StorageLayout,
    rows: dict[str, dict[str, object]],
    blocked: set[Path],
    entries: list[_FileIdentity],
) -> tuple[tuple[LegacyAttempt, ...], set[str]]:
    root = layout.collector.attempts
    if not root.exists() and not root.is_symlink():
        return (), set()
    _real_directory(root)
    if root in blocked:
        return (), set()
    attempts: list[LegacyAttempt] = []
    seen: set[str] = set()
    for child in sorted(root.iterdir()):
        if child in blocked:
            continue
        row = rows.get(child.name)
        if row is None:
            raise InternalAudioMigrationError(f"private attempt entry needs typed conversion: {child}")
        attempt = inspect_legacy_attempt(child, child.name, row)
        if attempt.terminal_retirement is not None:
            raise InternalAudioMigrationError(
                f"v1 terminal-retired attempt requires manual custody resolution before cutover: {child.name}"
            )
        attempts.append(attempt)
        seen.add(child.name)
        _walk_private(layout.root, child, entries)
    return tuple(attempts), seen


def _validate_quarantine_root(layout: StorageLayout, known: set[Path]) -> None:
    root = layout.collector.quarantine
    if not root.exists() and not root.is_symlink():
        return
    _real_directory(root)
    for child in root.iterdir():
        if child not in known:
            raise InternalAudioMigrationError(f"private quarantine entry needs typed conversion: {child}")


def _validate_ready_scratch(layout: StorageLayout) -> None:
    ready = layout.publication.root
    if ready.is_dir():
        for child in ready.iterdir():
            if child.name.startswith("."):
                raise InternalAudioMigrationError(f"hidden ready scratch needs custody classification: {child}")


def _inspect_quarantine(
    layout: StorageLayout,
    item_id: str,
    row: dict[str, object],
) -> _Quarantine:
    try:
        _validate_quarantine_payload(item_id, row)
    except AttemptStateError as error:
        raise InternalAudioMigrationError(f"v1 quarantine row is malformed: {item_id}") from error
    if row["opaque"] is False:
        raise InternalAudioMigrationError(
            f"attributable v1 quarantine requires manual custody resolution before cutover: {item_id}"
        )
    if row["source_root"] == "spool":
        raise InternalAudioMigrationError(
            f"spool-rooted v1 quarantine requires manual custody resolution before cutover: {item_id}"
        )
    source_base = {
        "attempts": layout.collector.attempts,
        "spool": layout.collector.root,
        "capture": layout.draft,
    }[cast(str, row["source_root"])]
    source_path = source_base / cast(str, row["source_name"])
    destination = layout.collector.quarantine / item_id
    source_exists = os.path.lexists(source_path)
    destination_exists = os.path.lexists(destination)
    if source_exists and destination_exists:
        raise InternalAudioMigrationError(f"v1 quarantine has two physical locations: {item_id}")
    path = destination if destination_exists else source_path if source_exists else None
    if path is None:
        deadline = _quarantine_deadline(row)
        if row["deletion_pending"] is not True or deadline is None or deadline > time.time_ns():
            raise InternalAudioMigrationError(f"v1 quarantine row lacks its physical entry: {item_id}")
        return _Quarantine(layout.root, None, item_id, row)
    if row["move_committed"] is True and path != destination:
        raise InternalAudioMigrationError(f"v1 quarantine committed move is missing: {item_id}")
    info = path.lstat()
    actual_kind = "symlink" if stat.S_ISLNK(info.st_mode) else "directory" if stat.S_ISDIR(info.st_mode) else "file"
    if (info.st_dev, info.st_ino, actual_kind) != (row["source_device"], row["source_inode"], row["entry_kind"]):
        raise InternalAudioMigrationError(f"v1 quarantine physical identity changed: {item_id}")
    return _Quarantine(layout.root, path, item_id, row)


def _quarantine_disposition(row: dict[str, object]) -> str:
    state = row["terminal_state"]
    return "unclassified" if state is None else "published_prefix" if state == "published" else "unprocessable"


def _quarantine_deadline(row: dict[str, object]) -> int | None:
    terminal = row["terminal_at_unix_ns"]
    return terminal + _RETENTION_NS if type(terminal) is int else None


def _quarantine_fields(row: dict[str, object]) -> _QuarantineFields:
    terminal = row["terminal_at_unix_ns"]
    deadline = _quarantine_deadline(row)
    return _QuarantineFields(
        disposition=_quarantine_disposition(row),
        terminal_time=str(terminal) if type(terminal) is int else None,
        deadline=str(deadline) if deadline is not None else None,
        reason=cast(str, row["reason"]),
        terminal_reason=cast(str | None, row["terminal_reason"]),
    )


def _quarantine_entries(item: _Quarantine, entries: tuple[_FileIdentity, ...]) -> tuple[_FileIdentity, ...]:
    if item.path is None:
        return ()
    root = item.path.relative_to(item.root).as_posix()
    # Inventory paths are relative to the configured storage root, regardless
    # of whether a move intent left the entry under attempts or capture.
    selected = tuple(entry for entry in entries if entry.path == root or entry.path.startswith(root + "/"))
    if not selected:
        raise InternalAudioMigrationError(f"quarantine inventory is empty: {item.item_id}")
    return selected


def _import_quarantine(
    connection: sqlite3.Connection,
    item: _Quarantine,
    entries: tuple[_FileIdentity, ...],
) -> None:
    fields = _quarantine_fields(item.row)
    if item.path is None:
        return
    for entry in _quarantine_entries(item, entries):
        _import_evidence_entry(connection, entry, item, fields)


def _evidence_id(relative: str) -> str:
    return hashlib.sha256(("v1-quarantine/" + relative).encode("utf-8")).hexdigest()


def _import_evidence_entry(
    connection: sqlite3.Connection,
    entry: _FileIdentity,
    item: _Quarantine,
    fields: _QuarantineFields,
) -> None:
    kind = "regular" if entry.kind == "regular" else entry.kind
    root_entry = item.path is not None and entry.path == item.path.relative_to(item.root).as_posix()
    evidence_id = _evidence_id(entry.path)
    connection.execute(
        "INSERT INTO capture_evidence(evidence_id,source_id,entry_kind,original_relative_path,device,inode,mode,size,sha256,symlink_text,disposition,terminal_time,retention_deadline,reason,terminal_reason) "
        "VALUES (?,NULL,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (
            evidence_id,
            kind,
            entry.path,
            entry.device,
            entry.inode,
            entry.mode,
            entry.size,
            entry.sha256,
            bytes.fromhex(entry.symlink_hex) if entry.symlink_hex is not None else None,
            fields.disposition,
            fields.terminal_time,
            fields.deadline,
            fields.reason if root_entry else None,
            fields.terminal_reason if root_entry else None,
        ),
    )
    if entry.kind != "regular":
        return
    raw = item.root / entry.path
    digest = hashlib.sha256()
    with raw.open("rb", buffering=0) as stream:
        for ordinal, block in enumerate(iter(lambda: stream.read(_EVIDENCE_CHUNK_SIZE), b"")):
            digest.update(block)
            connection.execute(
                "INSERT INTO capture_evidence_chunk(evidence_id,chunk_ordinal,bytes) VALUES (?,?,?)",
                (evidence_id, ordinal, block),
            )
    if digest.hexdigest() != entry.sha256:
        raise InternalAudioMigrationError(f"opaque quarantine content changed during import: {entry.path}")


def _verify_quarantines(connection: sqlite3.Connection, inventory: _Inventory) -> None:
    expected_evidence: set[str] = set()
    for item in inventory.quarantines:
        if item.path is None:
            continue
        fields = _quarantine_fields(item.row)
        for entry in _quarantine_entries(item, inventory.entries):
            expected_evidence.add(_evidence_id(entry.path))
            _verify_evidence_entry(connection, entry, item, fields)
    actual_evidence = {
        row[0] for row in cast(Iterable[tuple[str]], connection.execute("SELECT evidence_id FROM capture_evidence"))
    }
    if actual_evidence != expected_evidence:
        raise InternalAudioMigrationError("typed quarantine evidence set differs")


def _verify_evidence_entry(
    connection: sqlite3.Connection,
    entry: _FileIdentity,
    item: _Quarantine,
    fields: _QuarantineFields,
) -> None:
    evidence_id = _evidence_id(entry.path)
    _verify_evidence_metadata(connection, evidence_id, entry, item, fields)
    _verify_evidence_chunks(connection, evidence_id, entry)


def _verify_evidence_metadata(
    connection: sqlite3.Connection,
    evidence_id: str,
    entry: _FileIdentity,
    item: _Quarantine,
    fields: _QuarantineFields,
) -> None:
    root_entry = item.path is not None and entry.path == item.path.relative_to(item.root).as_posix()
    row = cast(
        tuple[object, ...] | None,
        connection.execute(
            "SELECT entry_kind,original_relative_path,device,inode,mode,size,sha256,symlink_text,disposition,terminal_time,retention_deadline,reason,terminal_reason "
            "FROM capture_evidence WHERE evidence_id=?",
            (evidence_id,),
        ).fetchone(),
    )
    expected = (
        "regular" if entry.kind == "regular" else entry.kind,
        entry.path,
        entry.device,
        entry.inode,
        entry.mode,
        entry.size,
        entry.sha256,
        bytes.fromhex(entry.symlink_hex) if entry.symlink_hex is not None else None,
        fields.disposition,
        fields.terminal_time,
        fields.deadline,
        fields.reason if root_entry else None,
        fields.terminal_reason if root_entry else None,
    )
    if row is None or tuple(row) != expected:
        raise InternalAudioMigrationError(f"typed quarantine evidence metadata differs: {entry.path}")


def _verify_evidence_chunks(connection: sqlite3.Connection, evidence_id: str, entry: _FileIdentity) -> None:
    digest = hashlib.sha256()
    byte_count = 0
    for ordinal, block in cast(
        Iterable[tuple[int, bytes]],
        connection.execute(
            "SELECT chunk_ordinal,bytes FROM capture_evidence_chunk WHERE evidence_id=? ORDER BY chunk_ordinal",
            (evidence_id,),
        ),
    ):
        if (
            ordinal * _EVIDENCE_CHUNK_SIZE != byte_count
            or not isinstance(block, bytes)
            or not 1 <= len(block) <= _EVIDENCE_CHUNK_SIZE
        ):
            raise InternalAudioMigrationError(f"typed quarantine evidence chunks differ: {entry.path}")
        digest.update(block)
        byte_count += len(block)
    if entry.kind == "regular" and (byte_count != entry.size or digest.hexdigest() != entry.sha256):
        raise InternalAudioMigrationError(f"typed quarantine evidence bytes differ: {entry.path}")
    if entry.kind != "regular" and byte_count:
        raise InternalAudioMigrationError(f"typed quarantine nonfile has bytes: {entry.path}")


def _walk_private(root: Path, path: Path, entries: list[_FileIdentity]) -> None:
    info = path.lstat()
    relative = path.relative_to(root).as_posix()
    if stat.S_ISDIR(info.st_mode):
        entries.append(
            _FileIdentity(relative, "directory", info.st_dev, info.st_ino, stat.S_IMODE(info.st_mode), 0, None, None)
        )
        for child in sorted(path.iterdir()):
            _walk_private(root, child, entries)
    elif stat.S_ISREG(info.st_mode):
        entries.append(
            _FileIdentity(
                relative,
                "regular",
                info.st_dev,
                info.st_ino,
                stat.S_IMODE(info.st_mode),
                info.st_size,
                _hash_file(path),
                None,
            )
        )
    elif stat.S_ISLNK(info.st_mode):
        text = os.fsencode(path.readlink())
        entries.append(
            _FileIdentity(
                relative,
                "symlink",
                info.st_dev,
                info.st_ino,
                stat.S_IMODE(info.st_mode),
                info.st_size,
                None,
                text.hex(),
            )
        )
    else:
        raise InternalAudioMigrationError(f"unsupported private entry type: {relative}")


def _inventory_from_paths(inventory: _Inventory, root: Path) -> _Inventory:
    if not inventory.entries:
        return inventory
    # Candidate import can verify physical draft identities without finding a
    # mutable runtime layout or recursively scanning unrelated public output.
    entries: list[_FileIdentity] = []
    drafts: list[LegacyDraft] = []
    attempts: list[LegacyAttempt] = []
    for draft in inventory.drafts:
        drafts.append(inspect_legacy_draft(draft.path))
        _walk_private(root, draft.path, entries)
    for attempt in inventory.attempts:
        attempts.append(inspect_legacy_attempt(attempt.path, attempt.attempt_id))
        _walk_private(root, attempt.path, entries)
    quarantines: list[_Quarantine] = []
    for item in inventory.quarantines:
        if item.path is not None:
            quarantines.append(_Quarantine(item.root, item.path, item.item_id, item.row))
            _walk_private(root, item.path, entries)
        else:
            quarantines.append(item)
    return _Inventory(
        tuple(sorted(entries, key=lambda item: item.path)), tuple(drafts), tuple(attempts), tuple(quarantines)
    )


def _remaining_inventory(layout: StorageLayout) -> tuple[_FileIdentity, ...]:
    """List surviving old entries without requiring already deleted siblings."""

    entries: list[_FileIdentity] = []
    for relative in _PRIVATE_ROOTS:
        root = layout.root / relative
        if not root.exists() and not root.is_symlink():
            continue
        _real_directory(root)
        for child in sorted(root.iterdir()):
            _walk_private(layout.root, child, entries)
    return tuple(sorted(entries, key=lambda item: item.path))


def _cleanup_legacy(layout: StorageLayout, expected: tuple[_FileIdentity, ...]) -> int:
    removed = 0
    for item in sorted(expected, key=lambda value: (value.path.count("/"), value.path), reverse=True):
        path = layout.root / item.path
        if not path.exists() and not path.is_symlink():
            continue
        actual: list[_FileIdentity] = []
        if item.kind == "directory":
            _real_directory(path)
            info = path.lstat()
            current = _FileIdentity(
                item.path, "directory", info.st_dev, info.st_ino, stat.S_IMODE(info.st_mode), 0, None, None
            )
            if current != item or any(path.iterdir()):
                raise InternalAudioMigrationError(f"legacy directory changed before cleanup: {item.path}")
            path.rmdir()
        else:
            _walk_private(layout.root, path, actual)
            if actual != [item]:
                raise InternalAudioMigrationError(f"legacy file changed before cleanup: {item.path}")
            path.unlink()
            removed += 1
        _fsync_directory(path.parent)
    return removed


def _connect(path: Path) -> sqlite3.Connection:
    if path.is_symlink() or not path.is_file():
        raise InternalAudioMigrationError(f"SQLite database path is unsafe: {path}")
    uri = f"file:{quote(path.as_posix(), safe='/')}?mode=rw"
    connection = sqlite3.connect(uri, uri=True, timeout=5, isolation_level=None)
    try:
        connection.execute("PRAGMA busy_timeout = 5000")
        connection.execute("PRAGMA foreign_keys = ON")
        _integrity_check(connection)
    except BaseException:
        connection.close()
        raise
    return connection


def _connect_read_only(path: Path) -> sqlite3.Connection:
    if path.is_symlink() or not path.is_file():
        raise InternalAudioMigrationError(f"SQLite database path is unsafe: {path}")
    uri = f"file:{quote(path.as_posix(), safe='/')}?mode=ro"
    connection: sqlite3.Connection | None = None
    try:
        connection = sqlite3.connect(uri, uri=True, timeout=5, isolation_level=None)
        connection.execute("PRAGMA busy_timeout = 5000")
        connection.execute("PRAGMA foreign_keys = ON")
        _integrity_check(connection)
    except sqlite3.DatabaseError as error:
        if connection is not None:
            connection.close()
        if getattr(error, "sqlite_errorcode", None) == sqlite3.SQLITE_READONLY_ROLLBACK:
            _canonical_journal_identity(path)
            raise _CanonicalJournalRecoveryRequiredError("canonical SQLite needs rollback recovery") from error
        raise
    except BaseException:
        if connection is not None:
            connection.close()
        raise
    return connection


def _schema_version(connection: sqlite3.Connection) -> int:
    row = cast(tuple[int] | None, connection.execute("PRAGMA user_version").fetchone())
    if row is None:
        raise InternalAudioMigrationError("SQLite schema version is unavailable")
    return row[0]


def _integrity_check(connection: sqlite3.Connection) -> None:
    row = cast(tuple[str] | sqlite3.Row | None, connection.execute("PRAGMA integrity_check").fetchone())
    if row is None or row[0] != "ok":
        raise InternalAudioMigrationError("Collector SQLite integrity check failed")


def _read_receipt(connection: sqlite3.Connection) -> dict[str, object] | None:
    row = cast(
        tuple[str] | None,
        connection.execute("SELECT payload FROM state WHERE kind = 'migration' AND id = ?", (_RECEIPT_ID,)).fetchone(),
    )
    if row is None:
        return None
    try:
        value = cast(object, json.loads(row[0]))
    except (TypeError, ValueError, json.JSONDecodeError) as error:
        raise InternalAudioMigrationError("cutover receipt is unreadable") from error
    if not isinstance(value, dict) or any(not isinstance(key, str) for key in value):
        raise InternalAudioMigrationError("cutover receipt is malformed")
    return cast(dict[str, object], value)


def _put_receipt(connection: sqlite3.Connection, receipt: dict[str, object]) -> None:
    payload = json.dumps(receipt, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    try:
        admit_exclusive(connection)
        connection.execute(
            "INSERT INTO state(kind,id,payload) VALUES ('migration',?,?) "
            "ON CONFLICT(kind,id) DO UPDATE SET payload=excluded.payload",
            (_RECEIPT_ID, payload),
        )
        connection.commit()
    except sqlite3.DatabaseError:
        connection.rollback()
        raise


def _logical_digest(connection: sqlite3.Connection) -> str:
    digest = hashlib.sha256()
    for kind, item_id, payload in cast(
        Iterable[tuple[str, str, str]],
        connection.execute(
            "SELECT kind,id,payload FROM state WHERE NOT (kind='migration' AND id=?) ORDER BY kind,id",
            (_RECEIPT_ID,),
        ),
    ):
        for part in (kind, item_id, payload):
            data = part.encode("utf-8")
            digest.update(len(data).to_bytes(8, "big"))
            digest.update(data)
    return digest.hexdigest()


def _retained_digest(connection: sqlite3.Connection) -> tuple[int, str]:
    rows = cast(
        Iterable[tuple[str, str, str] | sqlite3.Row],
        connection.execute(
            "SELECT kind,id,payload FROM state WHERE kind NOT IN ('attempt','quarantine') "
            "AND NOT (kind='migration' AND id=?) ORDER BY kind,id",
            (_RECEIPT_ID,),
        ),
    )
    digest = hashlib.sha256()
    count = 0
    for row in rows:
        for value in row:
            data = str(value).encode("utf-8")
            digest.update(len(data).to_bytes(8, "big"))
            digest.update(data)
        count += 1
    return count, digest.hexdigest()


def _legacy_mutable_rows(connection: sqlite3.Connection) -> bool:
    return (
        connection.execute("SELECT 1 FROM state WHERE kind IN ('attempt','quarantine') LIMIT 1").fetchone() is not None
    )


def _validate_v1_control(connection: sqlite3.Connection) -> None:
    row = cast(
        tuple[str] | None,
        connection.execute("SELECT payload FROM state WHERE kind='migration' AND id='current'").fetchone(),
    )
    if row is None or json.loads(row[0]) != {"phase": "complete", "version": 1}:
        raise InternalAudioMigrationError("v1 control-state migration is incomplete")


def _validate_v1_receipt(
    receipt: dict[str, object], layout: StorageLayout, inventory: _Inventory, source_sha: str
) -> None:
    _validate_receipt_shape(receipt, layout)
    if receipt["phase"] != "BUILDING" or receipt["source_sha256"] != source_sha:
        raise InternalAudioMigrationError("v1 cutover receipt no longer matches source state")
    _assert_inventory_exact(
        _Inventory(_receipt_entries(receipt), inventory.drafts, inventory.attempts, inventory.quarantines), inventory
    )
    if receipt["quarantines"] != [{"id": item.item_id, "row": item.row} for item in inventory.quarantines]:
        raise InternalAudioMigrationError("v1 quarantine rows changed after cutover receipt")
    if receipt["uid"] != os.geteuid() or receipt["gid"] != os.getegid():
        raise InternalAudioMigrationError("v1 cutover owner changed")


def _validate_receipt_shape(receipt: dict[str, object], layout: StorageLayout) -> None:
    required = {
        "format",
        "phase",
        "uuid",
        "source_revision",
        "source_sha256",
        "candidate",
        "uid",
        "gid",
        "candidate_gid",
        "parent",
        "files",
        "quarantines",
    }
    if set(receipt) != required or receipt["format"] != _FORMAT:
        raise InternalAudioMigrationError("cutover receipt format is unsupported")
    uuid = receipt["uuid"]
    revision = receipt["source_revision"]
    source_sha = receipt["source_sha256"]
    candidate = receipt["candidate"]
    if not isinstance(uuid, str) or _UUID.fullmatch(uuid) is None:
        raise InternalAudioMigrationError("cutover receipt identity is invalid")
    if not isinstance(revision, str) or _REVISION.fullmatch(revision) is None:
        raise InternalAudioMigrationError("cutover receipt identity is invalid")
    if not isinstance(source_sha, str) or not _is_sha256(source_sha):
        raise InternalAudioMigrationError("cutover receipt identity is invalid")
    if (
        candidate != _CANDIDATE_PREFIX + uuid
        or type(receipt["uid"]) is not int
        or type(receipt["gid"]) is not int
        or type(receipt["candidate_gid"]) is not int
    ):
        raise InternalAudioMigrationError("cutover receipt identity is invalid")
    path = layout.collector.root / cast(str, candidate)
    if path.parent != layout.collector.root:
        raise InternalAudioMigrationError("cutover candidate escapes Collector root")
    _assert_candidate_parent(layout.collector.root, receipt)
    _receipt_entries(receipt)
    _receipt_quarantines(receipt)


def _validate_complete_receipt(receipt: dict[str, object]) -> None:
    if set(receipt) != {"format", "phase", "source_revision", "source_sha256", "imported_sources"}:
        raise InternalAudioMigrationError("complete cutover receipt is malformed")
    if receipt["format"] != _FORMAT or receipt["phase"] != "COMPLETE":
        raise InternalAudioMigrationError("complete cutover receipt is malformed")
    revision = receipt["source_revision"]
    source_sha = receipt["source_sha256"]
    imported = receipt["imported_sources"]
    if not isinstance(revision, str) or _REVISION.fullmatch(revision) is None:
        raise InternalAudioMigrationError("complete cutover receipt is malformed")
    if not isinstance(source_sha, str) or not _is_sha256(source_sha):
        raise InternalAudioMigrationError("complete cutover receipt is malformed")
    if type(imported) is not int or imported < 0:
        raise InternalAudioMigrationError("complete cutover receipt is malformed")


def _receipt_entries(receipt: dict[str, object]) -> tuple[_FileIdentity, ...]:
    raw = receipt["files"]
    if not isinstance(raw, list):
        raise InternalAudioMigrationError("cutover file inventory is malformed")
    entries = [_parse_file_identity(value) for value in raw]
    if len(entries) != len({entry.path for entry in entries}) or entries != sorted(entries, key=lambda item: item.path):
        raise InternalAudioMigrationError("cutover file inventory order or identity is invalid")
    return tuple(entries)


def _receipt_quarantines(receipt: dict[str, object]) -> tuple[tuple[str, dict[str, object]], ...]:
    raw = receipt["quarantines"]
    if not isinstance(raw, list):
        raise InternalAudioMigrationError("cutover quarantine inventory is malformed")
    result: list[tuple[str, dict[str, object]]] = []
    for value in raw:
        if not isinstance(value, dict) or set(value) != {"id", "row"}:
            raise InternalAudioMigrationError("cutover quarantine row is malformed")
        item_id = value["id"]
        row = value["row"]
        if not isinstance(item_id, str) or not isinstance(row, dict):
            raise InternalAudioMigrationError("cutover quarantine row is malformed")
        try:
            _validate_quarantine_payload(item_id, cast(dict[str, object], row))
        except AttemptStateError as error:
            raise InternalAudioMigrationError("cutover quarantine row is malformed") from error
        result.append((item_id, cast(dict[str, object], row)))
    if [item_id for item_id, _ in result] != sorted({item_id for item_id, _ in result}):
        raise InternalAudioMigrationError("cutover quarantine inventory order is invalid")
    return tuple(result)


def _parse_file_identity(value: object) -> _FileIdentity:
    if not isinstance(value, dict) or set(value) != set(_FileIdentity.__dataclass_fields__):
        raise InternalAudioMigrationError("cutover file identity is malformed")
    data = cast(dict[str, object], value)
    path = _parse_file_path(data["path"])
    kind = _parse_file_kind(data["kind"])
    _validate_file_stat(data)
    sha, link = _parse_file_content(kind, data["sha256"], data["symlink_hex"])
    return _FileIdentity(
        path,
        kind,
        cast(int, data["device"]),
        cast(int, data["inode"]),
        cast(int, data["mode"]),
        cast(int, data["size"]),
        sha,
        link,
    )


def _parse_file_path(value: object) -> str:
    prefixes = ("draft/", "collector/attempts/", "collector/quarantine/")
    if not isinstance(value, str) or not value.startswith(prefixes):
        raise InternalAudioMigrationError("cutover file path is unsupported")
    if any(part in {"", ".", ".."} for part in value.split("/")):
        raise InternalAudioMigrationError("cutover file path escapes storage root")
    return value


def _parse_file_kind(value: object) -> str:
    if not isinstance(value, str) or value not in {"regular", "directory", "symlink"}:
        raise InternalAudioMigrationError("cutover file type is unsupported")
    return value


def _validate_file_stat(value: dict[str, object]) -> None:
    if any(type(value[name]) is not int or cast(int, value[name]) < 0 for name in ("device", "inode", "mode", "size")):
        raise InternalAudioMigrationError("cutover file stat is malformed")


def _parse_file_content(kind: str, sha: object, link: object) -> tuple[str | None, str | None]:
    if kind == "regular":
        if not isinstance(sha, str) or not _is_sha256(sha) or link is not None:
            raise InternalAudioMigrationError("cutover file digest is malformed")
        return sha, None
    if kind == "directory":
        if sha is not None or link is not None:
            raise InternalAudioMigrationError("cutover directory identity is malformed")
        return None, None
    if sha is not None or not isinstance(link, str) or len(link) % 2:
        raise InternalAudioMigrationError("cutover symlink identity is malformed")
    return None, link


def _assert_inventory_exact(expected: _Inventory, current: _Inventory) -> None:
    if (
        expected.entries != current.entries
        or expected.drafts != current.drafts
        or expected.attempts != current.attempts
        or expected.quarantines != current.quarantines
    ):
        raise InternalAudioMigrationError("v1 private audio changed before activation")


def _assert_remaining_inventory(expected: tuple[_FileIdentity, ...], current: tuple[_FileIdentity, ...]) -> None:
    identities = {item.path: item for item in expected}
    for entry in current:
        if identities.get(entry.path) != entry:
            raise InternalAudioMigrationError(f"v2 cleanup found changed or unknown private entry: {entry.path}")


def _draft_entries(entries: tuple[_FileIdentity, ...]) -> set[str]:
    return {item.path.split("/")[1] for item in entries if item.path.startswith("draft/")}


def _imported_source_count(connection: sqlite3.Connection) -> int:
    row = cast(tuple[int] | None, connection.execute("SELECT count(*) FROM capture_source").fetchone())
    if row is None:
        raise InternalAudioMigrationError("typed source count is unavailable")
    return row[0]


def _v1_rows(connection: sqlite3.Connection, kind: str) -> dict[str, dict[str, object]]:
    rows: dict[str, dict[str, object]] = {}
    for item_id, raw in cast(
        Iterable[tuple[str, str]], connection.execute("SELECT id,payload FROM state WHERE kind=? ORDER BY id", (kind,))
    ):
        try:
            value = cast(object, json.loads(raw))
        except (TypeError, ValueError, json.JSONDecodeError) as error:
            raise InternalAudioMigrationError(f"v1 {kind} row is malformed: {item_id}") from error
        if not isinstance(value, dict) or any(not isinstance(key, str) for key in value):
            raise InternalAudioMigrationError(f"v1 {kind} row is malformed: {item_id}")
        rows[item_id] = cast(dict[str, object], value)
    return rows


def _v1_payload(connection: sqlite3.Connection, kind: str, item_id: str) -> dict[str, object]:
    try:
        return _v1_rows(connection, kind)[item_id]
    except KeyError as error:
        raise InternalAudioMigrationError(f"v1 {kind} row disappeared: {item_id}") from error


def _check_owned_candidate(path: Path, receipt: dict[str, object]) -> tuple[int, int]:
    _assert_candidate_parent(path.parent, receipt)
    info = path.lstat()
    if not stat.S_ISREG(info.st_mode) or info.st_uid != receipt["uid"] or info.st_gid != receipt["candidate_gid"]:
        raise InternalAudioMigrationError("candidate is not owned by the cutover migrator")
    return info.st_dev, info.st_ino


def _assert_candidate_identity(path: Path, receipt: dict[str, object], expected: tuple[int, int]) -> None:
    if _check_owned_candidate(path, receipt) != expected:
        raise InternalAudioMigrationError("candidate identity changed during cutover")


def _check_owned_candidate_journal(path: Path, receipt: dict[str, object]) -> tuple[int, int]:
    _assert_candidate_parent(path.parent, receipt)
    info = path.lstat()
    if not stat.S_ISREG(info.st_mode) or info.st_uid != receipt["uid"] or info.st_gid != receipt["candidate_gid"]:
        raise InternalAudioMigrationError("candidate journal is not owned by the cutover migrator")
    return info.st_dev, info.st_ino


def _remove_owned_candidate_journal(path: Path, receipt: dict[str, object], expected: tuple[int, int] | None) -> None:
    _assert_candidate_parent(path.parent, receipt)
    journal = path.parent / f"{path.name}-journal"
    if journal.exists() or journal.is_symlink():
        if expected is None or _check_owned_candidate_journal(journal, receipt) != expected:
            raise InternalAudioMigrationError("candidate journal identity changed during cutover")
        journal.unlink()
        _fsync_directory(path.parent)


def _remove_owned_candidate(
    path: Path, receipt: dict[str, object], expected: tuple[int, int], journal_identity: tuple[int, int] | None
) -> None:
    _assert_candidate_parent(path.parent, receipt)
    _assert_candidate_identity(path, receipt, expected)
    _remove_owned_candidate_journal(path, receipt, journal_identity)
    _assert_candidate_identity(path, receipt, expected)
    path.unlink()
    _fsync_directory(path.parent)


def _release_revision(path: Path) -> str:
    if path.is_symlink() or not path.is_file():
        raise InternalAudioMigrationError("selected release metadata is missing or unsafe")
    try:
        value = cast(object, json.loads(path.read_text(encoding="utf-8")))
    except (OSError, UnicodeError, ValueError, json.JSONDecodeError) as error:
        raise InternalAudioMigrationError("selected release metadata is unreadable") from error
    if not isinstance(value, dict) or set(value) != {"source_revision"}:
        raise InternalAudioMigrationError("selected release metadata is malformed")
    revision = value["source_revision"]
    if not isinstance(revision, str) or _REVISION.fullmatch(revision) is None:
        raise InternalAudioMigrationError("selected release revision is malformed")
    return revision


def _assert_real_canonical(path: Path) -> None:
    if path.is_symlink() or (path.exists() and not path.is_file()):
        raise InternalAudioMigrationError("canonical SQLite path is unsafe")


def _candidate_parent_identity(path: Path) -> dict[str, int]:
    try:
        info = path.lstat()
    except OSError as error:
        raise InternalAudioMigrationError("cutover candidate parent is unavailable") from error
    mode = stat.S_IMODE(info.st_mode)
    if not stat.S_ISDIR(info.st_mode) or mode != _COLLECTOR_PARENT_MODE:
        raise InternalAudioMigrationError("cutover candidate parent is not a real setgid Collector directory")
    return {"dev": info.st_dev, "ino": info.st_ino, "uid": info.st_uid, "gid": info.st_gid, "mode": mode}


def _assert_candidate_parent(path: Path, receipt: dict[str, object]) -> None:
    expected = receipt.get("parent")
    if (
        not isinstance(expected, dict)
        or set(expected) != {"dev", "ino", "uid", "gid", "mode"}
        or any(type(value) is not int for value in expected.values())
        or expected["gid"] != receipt.get("candidate_gid")
    ):
        raise InternalAudioMigrationError("cutover candidate parent receipt is invalid")
    if _candidate_parent_identity(path) != expected:
        raise InternalAudioMigrationError("cutover candidate parent changed")


def _old_control_present(layout: StorageLayout) -> bool:
    for root in (layout.collector.root, layout.draft, layout.publication.root):
        if not root.is_dir():
            continue
        for child in root.iterdir():
            if child in {layout.collector.lock, layout.collector.state_db} or child.name in {"attempts", "quarantine"}:
                continue
            if root == layout.publication.root and not child.name.startswith("."):
                continue
            return True
    return False


def _any_ready_entry(layout: StorageLayout) -> bool:
    ready = layout.publication.root
    if not ready.exists() and not ready.is_symlink():
        return False
    _real_directory(ready)
    return any(ready.iterdir())


def _real_directory(path: Path) -> None:
    try:
        mode = path.lstat().st_mode
    except OSError as error:
        raise InternalAudioMigrationError(f"private root is missing: {path}") from error
    if not stat.S_ISDIR(mode):
        raise InternalAudioMigrationError(f"private root is not a real directory: {path}")


def _hash_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb", buffering=0) as stream:
        for block in iter(lambda: stream.read(_BUFFER_SIZE), b""):
            digest.update(block)
    return digest.hexdigest()


def _is_sha256(value: str) -> bool:
    return len(value) == _SHA256_LENGTH and all(character in "0123456789abcdef" for character in value)


def _assert_no_journal(path: Path) -> None:
    journal = path.parent / f"{path.name}-journal"
    if journal.exists() or journal.is_symlink():
        raise InternalAudioMigrationError(f"SQLite journal remains after recovery: {path.name}")


def _canonical_journal_identity(path: Path) -> tuple[int, int, int]:
    journal = path.parent / f"{path.name}-journal"
    try:
        canonical_info = path.lstat()
        journal_info = journal.lstat()
    except OSError as error:
        raise InternalAudioMigrationError("canonical SQLite rollback journal is unavailable") from error
    if not stat.S_ISREG(canonical_info.st_mode) or not stat.S_ISREG(journal_info.st_mode):
        raise InternalAudioMigrationError("canonical SQLite rollback journal is unsafe")
    if (
        journal_info.st_dev != canonical_info.st_dev
        or journal_info.st_uid != canonical_info.st_uid
        or journal_info.st_gid != canonical_info.st_gid
        or journal_info.st_nlink != 1
    ):
        raise InternalAudioMigrationError("canonical SQLite rollback journal is unsafe")
    return canonical_info.st_dev, canonical_info.st_ino, canonical_info.st_uid


def _fsync_file(path: Path) -> None:
    fd = os.open(path, os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _fsync_directory(path: Path) -> None:
    fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC | os.O_NOFOLLOW)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _handoff_canonical(path: Path) -> None:
    """Hand the activated inode to the provisioned service-owned data root."""

    _real_directory(path.parent)
    owner = path.parent.lstat()
    if path.is_symlink() or not stat.S_ISREG(path.lstat().st_mode):
        raise InternalAudioMigrationError("canonical SQLite inode is unsafe for ownership handoff")
    os.chown(path, owner.st_uid, owner.st_gid, follow_symlinks=False)
    path.chmod(_DATABASE_MODE, follow_symlinks=False)
    info = path.lstat()
    if (info.st_uid, info.st_gid, stat.S_IMODE(info.st_mode)) != (owner.st_uid, owner.st_gid, _DATABASE_MODE):
        raise InternalAudioMigrationError("canonical SQLite ownership handoff failed")
    _fsync_file(path)
    _fsync_directory(path.parent)


def _verify_handoff_access(path: Path) -> None:
    """Verify the service can journal and the operator remains read-only."""

    info = path.lstat()
    try:
        service = pwd.getpwuid(info.st_uid)
        operator_group = grp.getgrgid(info.st_gid)
        operator = pwd.getpwnam(operator_group.gr_name)
    except KeyError as error:
        raise InternalAudioMigrationError("service or operator account is unavailable") from error
    if service.pw_uid == operator.pw_uid:
        raise InternalAudioMigrationError("service and operator custody accounts are not distinct")
    _probe_effective_access(path, service, service.pw_gid, writable=True)
    _probe_effective_access(path, operator, operator.pw_gid, writable=False)


def _probe_effective_access(path: Path, user: pwd.struct_passwd, gid: int, *, writable: bool) -> None:
    probe = path.parent / f".collector-state.access.{uuid4().hex}"
    read_fd, write_fd = os.pipe()
    pid = os.fork()
    if pid == 0:
        _run_access_probe_child((path, probe), user, gid, (read_fd, write_fd), writable=writable)
    os.close(write_fd)
    try:
        result = os.read(read_fd, 16)
    finally:
        os.close(read_fd)
        _, status = os.waitpid(pid, 0)
        _remove_access_probe(probe, user.pw_uid)
    if result != b"ok" or status != 0:
        role = "service" if writable else "operator"
        raise InternalAudioMigrationError(f"{role} account cannot use Collector state with required access")


def _run_access_probe_child(
    paths: tuple[Path, Path],
    user: pwd.struct_passwd,
    gid: int,
    pipe: tuple[int, int],
    *,
    writable: bool,
) -> None:
    os.close(pipe[0])
    try:
        _probe_as_user(paths[0], paths[1], user, gid, writable=writable)
        os.write(pipe[1], b"ok")
    except OSError, InternalAudioMigrationError:
        os.write(pipe[1], b"denied")
    finally:
        os.close(pipe[1])
        os._exit(0)


def _remove_access_probe(probe: Path, expected_uid: int) -> None:
    if not probe.exists() and not probe.is_symlink():
        return
    info = probe.lstat()
    if not stat.S_ISREG(info.st_mode) or info.st_uid != expected_uid:
        raise InternalAudioMigrationError("access probe was replaced before cleanup")
    probe.unlink()
    _fsync_directory(probe.parent)


def _probe_as_user(path: Path, probe: Path, user: pwd.struct_passwd, gid: int, *, writable: bool) -> None:
    _assume_probe_credentials(user, gid)
    access_flags = (os.O_RDWR if writable else os.O_RDONLY) | os.O_NOFOLLOW
    with closing(os.fdopen(os.open(path, access_flags), "rb")):
        pass
    if writable:
        with closing(os.fdopen(os.open(probe, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600), "wb")):
            pass
        probe.unlink()
        return
    _probe_operator_readonly(path, probe)


def _assume_probe_credentials(user: pwd.struct_passwd, gid: int) -> None:
    if os.geteuid() == 0:
        os.initgroups(user.pw_name, gid)
        os.setgid(gid)
        os.setuid(user.pw_uid)
    elif (os.geteuid(), os.getegid()) != (user.pw_uid, gid):
        raise InternalAudioMigrationError("access probe cannot assume account credentials")


def _probe_operator_readonly(path: Path, probe: Path) -> None:
    for target, flags in (
        (path, os.O_WRONLY | os.O_NOFOLLOW),
        (probe, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW),
    ):
        try:
            fd = os.open(target, flags, 0o600)
        except OSError as error:
            if error.errno in {errno.EACCES, errno.EPERM}:
                continue
            raise
        else:
            os.close(fd)
            raise InternalAudioMigrationError("operator account has write access to Collector state")
