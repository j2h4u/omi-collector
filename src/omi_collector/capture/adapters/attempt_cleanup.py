"""Crash-safe removal of terminal raw attempts already durable downstream."""

from __future__ import annotations

import os
import stat
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from hashlib import sha256
from pathlib import Path
from typing import cast

from ..domain.attempt_cleanup_machine import (
    AttemptCleanupAction,
    AttemptLifecycle,
    CopyEvidence,
    RawEvidence,
    decide_attempt_cleanup,
)
from ..domain.ring_protocol import RECORD_SIZE
from .attempts import AttemptDescriptor
from .collector_state_db import StateDBError, StateKind
from .publication import PrefixPublicationEvidence, TerminalRetirementEvidence, _prefix_publication_matches
from .ready_bundles import (
    ReadyBundleError,
    _convert_record,
    _read_ledger,
    _read_ready_source,
    _validate_ready_ledger_entry,
    source_lineage,
)
from .recovery import _published_prefix
from .staging_contract import _RAW_NAME, AttemptStateError, DurablePrefix, StagingError, _validate_attempt_id
from .staging_filesystem import (
    _ATTEMPT_CLEANUP_INTENT,
    StagingFilesystem,
    _read_json,
    _require_regular_directory,
    _sync_directory,
)


@dataclass(slots=True)
class _ReadyEvidence:
    rows: Mapping[str, object]
    sources: dict[str, tuple[dict[str, object], list[dict[str, object]]]] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class _AttemptCopy:
    entry: Path
    descriptor: AttemptDescriptor
    prefix: DurablePrefix
    marker: PrefixPublicationEvidence


def sweep_terminal_redundant(
    filesystem: StagingFilesystem, ready_root: Path | None, *, should_defer: Callable[[], bool]
) -> tuple[Path, ...]:
    """Delete only fully copied terminal raw, then retire its SQLite row."""
    active = filesystem._active_lease
    if active is not None:
        filesystem.require_device_lock(active)
        return _sweep_held(filesystem, ready_root, should_defer)
    with filesystem.device_lock(operation="sweep_terminal_retired"):
        return _sweep_held(filesystem, ready_root, should_defer)


def _sweep_held(
    filesystem: StagingFilesystem, ready_root: Path | None, should_defer: Callable[[], bool]
) -> tuple[Path, ...]:
    removed: list[Path] = []
    _finish_missing_intents(filesystem)
    root = filesystem.attempts_root
    if not os.path.lexists(root):
        return ()
    if root.is_symlink() or not root.is_dir():
        raise StagingError("terminal attempt root is not a directory")
    try:
        entries = tuple(root.iterdir())
    except OSError as error:
        raise StagingError("terminal attempts cannot be inspected") from error
    if not entries:
        return ()
    evidence = _ready_evidence(filesystem)
    for entry in entries:
        if should_defer():
            break
        try:
            if _terminal_decision(filesystem, entry, evidence, ready_root) is AttemptCleanupAction.DELETE:
                _remove_redundant(filesystem, entry)
                removed.append(entry)
        except OSError, StagingError, ReadyBundleError, StateDBError, ValueError:
            continue
    return tuple(removed)


def _finish_missing_intents(filesystem: StagingFilesystem) -> None:
    """A crash after unlink but before DB deletion leaves only a pending row."""
    try:
        rows = filesystem.state_db.list(StateKind.ATTEMPT)
    except (OSError, StateDBError) as error:
        raise StagingError("terminal cleanup intents are unreadable") from error
    for attempt_id, row in rows:
        try:
            _validate_attempt_id(attempt_id)
        except AttemptStateError:
            continue
        if row.get("cleanup_intent") != _ATTEMPT_CLEANUP_INTENT:
            continue
        try:
            filesystem._validate_attempt_row(attempt_id, row)
            raw_terminal = row["terminal_retirement"]
            if not isinstance(raw_terminal, dict):
                continue
            TerminalRetirementEvidence.from_json(raw_terminal)
        except AttemptStateError, ValueError, TypeError:
            continue
        path = filesystem.attempts_root / attempt_id
        if path.is_symlink():
            continue
        if path.is_dir() and not path.is_symlink() and not tuple(path.iterdir()):
            path.rmdir()
            _sync_directory(filesystem.attempts_root, filesystem._fsync)
        action = decide_attempt_cleanup(
            guard_held=filesystem._active_lease is not None,
            lifecycle=AttemptLifecycle.TERMINAL,
            raw=RawEvidence.MISSING,
            copy=CopyEvidence.NONE,
            intent=True,
        )
        if not os.path.lexists(path) and action is AttemptCleanupAction.FINALIZE:
            with filesystem.state_db.transaction() as transaction:
                transaction.delete(StateKind.ATTEMPT, attempt_id)


def _ready_evidence(filesystem: StagingFilesystem) -> _ReadyEvidence:
    try:
        ledger = _read_ledger(filesystem.state_db)
    except OSError, ReadyBundleError, StateDBError, ValueError:
        return _ReadyEvidence({})
    bundles = ledger["bundles"]
    assert isinstance(bundles, dict)
    return _ReadyEvidence(bundles)


def _terminal_decision(
    filesystem: StagingFilesystem, entry: Path, evidence: _ReadyEvidence, ready_root: Path | None
) -> AttemptCleanupAction:
    if entry.is_symlink() or not entry.is_dir():
        return AttemptCleanupAction.REFUSE
    descriptor = filesystem._read_descriptor(entry)
    row = filesystem._read_attempt_row(descriptor.attempt_id)
    raw_prefix = row["prefix_publication"]
    terminal = row["terminal_retirement"]
    if not isinstance(raw_prefix, dict):
        return AttemptCleanupAction.REFUSE
    PrefixPublicationEvidence.from_json(raw_prefix)
    if terminal is None:
        return decide_attempt_cleanup(
            guard_held=filesystem._active_lease is not None,
            lifecycle=AttemptLifecycle.ACTIVE,
            raw=RawEvidence.COMPLETE,
            copy=CopyEvidence.NONE,
            intent=False,
        )
    if not isinstance(terminal, dict):
        return AttemptCleanupAction.REFUSE
    TerminalRetirementEvidence.from_json(terminal)
    prefix = _published_prefix(entry, descriptor, filesystem, io_chunk_bytes=filesystem._durability.io_chunk_bytes)
    raw = entry / _RAW_NAME
    if raw.stat().st_size != prefix.record_count * RECORD_SIZE:
        raw_state = RawEvidence.TAIL
    elif prefix.record_count == 0:
        raw_state = RawEvidence.EMPTY
    else:
        raw_state = RawEvidence.COMPLETE
    copy = _copy_evidence(
        filesystem,
        _AttemptCopy(entry, descriptor, prefix, PrefixPublicationEvidence.from_json(raw_prefix)),
        evidence,
        ready_root,
        raw_state,
    )
    return decide_attempt_cleanup(
        guard_held=filesystem._active_lease is not None,
        lifecycle=AttemptLifecycle.TERMINAL,
        raw=raw_state,
        copy=copy,
        intent=row.get("cleanup_intent") == _ATTEMPT_CLEANUP_INTENT,
    )


def _copy_evidence(
    filesystem: StagingFilesystem,
    source: _AttemptCopy,
    evidence: _ReadyEvidence,
    ready_root: Path | None,
    raw_state: RawEvidence,
) -> CopyEvidence:
    if raw_state is RawEvidence.TAIL or raw_state is RawEvidence.EMPTY:
        return CopyEvidence.NONE
    if _prefix_publication_matches(
        source.entry,
        source.descriptor,
        source.marker,
        filesystem=filesystem,
        io_chunk_bytes=filesystem._durability.io_chunk_bytes,
    ):
        return CopyEvidence.DRAFT
    if _downstream_copy_matches(source.entry, source.prefix, evidence, ready_root):
        return CopyEvidence.READY
    return CopyEvidence.NONE


def _downstream_copy_matches(
    entry: Path, prefix: DurablePrefix, evidence: _ReadyEvidence, ready_root: Path | None
) -> bool:
    cursor = prefix.start_sequence
    retired = _retired_slices(evidence, prefix)
    while cursor < prefix.next_sequence:
        next_cursor = _ready_advance(entry, prefix, evidence, ready_root, cursor)
        if next_cursor == cursor:
            next_cursor = _retired_advance(entry, prefix, retired, cursor)
        if next_cursor == cursor:
            return False
        cursor = next_cursor
    return True


def _ready_advance(
    entry: Path,
    prefix: DurablePrefix,
    evidence: _ReadyEvidence,
    ready_root: Path | None,
    cursor: int,
) -> int:
    if ready_root is None:
        return cursor
    for bundle_id, row in sorted(evidence.rows.items(), key=lambda item: _row_start(item[1])):
        if not isinstance(row, dict) or row.get("state") != "ready":
            continue
        start, next_sequence = row["start_sequence"], row["next_sequence"]
        if not isinstance(start, int) or not isinstance(next_sequence, int) or not start <= cursor < next_sequence:
            continue
        ranges = _authenticated_ready_ranges(ready_root, bundle_id, row, evidence)
        matched = _match_ready_ranges(entry / _RAW_NAME, prefix, cursor, ranges, ready_root / bundle_id / _RAW_NAME)
        if matched > cursor:
            return matched
    return cursor


def _row_start(row: object) -> int:
    if not isinstance(row, dict):
        return 2**63
    value = row.get("start_sequence")
    return value if type(value) is int else 2**63


def _authenticated_ready_ranges(
    ready_root: Path, bundle_id: str, row: Mapping[str, object], evidence: _ReadyEvidence
) -> list[dict[str, object]]:
    cached = evidence.sources.get(bundle_id)
    if cached is not None:
        return cached[1]
    destination = ready_root / bundle_id
    source = _read_ready_source(destination)
    _validate_ready_ledger_entry(row, source)
    manifest = _read_json(destination / "manifest.json")
    assert isinstance(manifest, dict)
    raw_ranges = manifest["time_ranges"]
    assert isinstance(raw_ranges, list)
    ranges = cast(list[dict[str, object]], raw_ranges)
    evidence.sources[bundle_id] = (dict(row), ranges)
    return ranges


def _match_ready_ranges(
    source: Path, prefix: DurablePrefix, cursor: int, ranges: list[dict[str, object]], ready: Path
) -> int:
    offset_records = 0
    for item in ranges:
        if cursor >= prefix.next_sequence:
            break
        start, next_sequence = item["start_sequence"], item["next_sequence"]
        assert isinstance(start, int) and isinstance(next_sequence, int)
        if start <= cursor < next_sequence:
            until = min(next_sequence, prefix.next_sequence)
            with source.open("rb") as original, ready.open("rb") as converted:
                original.seek((cursor - prefix.start_sequence) * RECORD_SIZE)
                converted.seek((offset_records + cursor - start) * RECORD_SIZE)
                for _ in range(until - cursor):
                    raw_record, ready_record = original.read(RECORD_SIZE), converted.read(RECORD_SIZE)
                    if len(raw_record) != RECORD_SIZE or _convert_record(raw_record, item) != ready_record:
                        return cursor
            cursor = until
        offset_records += next_sequence - start
    return cursor


def _retired_slices(evidence: _ReadyEvidence, prefix: DurablePrefix) -> tuple[tuple[int, int, str], ...]:
    slices: list[tuple[int, int, str]] = []
    for row in evidence.rows.values():
        if not isinstance(row, dict) or row.get("state") != "retired":
            continue
        lineage = source_lineage(row) if "sources" in row else _legacy_exact_lineage(row, prefix)
        slices.extend(
            (start, end, digest)
            for start, end, digest in lineage
            if prefix.start_sequence <= start and end <= prefix.next_sequence
        )
    return tuple(sorted(slices))


def _retired_advance(entry: Path, prefix: DurablePrefix, slices: tuple[tuple[int, int, str], ...], cursor: int) -> int:
    with (entry / _RAW_NAME).open("rb") as raw:
        for start, end, digest in slices:
            if not start <= cursor < end:
                continue
            raw.seek((start - prefix.start_sequence) * RECORD_SIZE)
            if sha256(raw.read((end - start) * RECORD_SIZE)).hexdigest() == digest:
                return end
    return cursor


def _legacy_exact_lineage(row: Mapping[str, object], prefix: DurablePrefix) -> tuple[tuple[int, int, str], ...]:
    """Old retired entries can prove only their complete original draft, never a slice."""
    if (
        row.get("start_sequence") == prefix.start_sequence
        and row.get("next_sequence") == prefix.next_sequence
        and row.get("draft_raw_sha256") == prefix.raw_sha256
        and "gaps" not in row
    ):
        return ((prefix.start_sequence, prefix.next_sequence, prefix.raw_sha256),)
    return ()


def terminal_attempts_overlap(filesystem: StagingFilesystem, ranges: tuple[tuple[int, int], ...]) -> bool:
    """Block legacy ACK when a surviving terminal raw could still need its ready copy."""
    try:
        rows = filesystem.state_db.list(StateKind.ATTEMPT)
    except OSError, StateDBError:
        return True
    for attempt_id, row in rows:
        path = filesystem.attempts_root / attempt_id
        if not os.path.lexists(path):
            continue
        try:
            _validate_attempt_id(attempt_id)
            filesystem._validate_attempt_row(attempt_id, row)
            terminal = row["terminal_retirement"]
            if terminal is None:
                continue
            if not isinstance(terminal, dict):
                return True
            TerminalRetirementEvidence.from_json(terminal)
            descriptor = cast(dict[str, object], row["descriptor"])
            checkpoint = cast(dict[str, object], row["checkpoint"])
            start = cast(int, descriptor["start_sequence"])
            end = start + cast(int, checkpoint["record_count"])
        except AttemptStateError, TypeError, ValueError, KeyError:
            return True
        if any(start < range_end and range_start < end for range_start, range_end in ranges):
            return True
    return False


def _remove_redundant(filesystem: StagingFilesystem, entry: Path) -> None:
    attempt_id = entry.name
    filesystem._write_attempt_field(attempt_id, "cleanup_intent", dict(_ATTEMPT_CLEANUP_INTENT))
    _require_regular_directory(entry)
    if {child.name for child in entry.iterdir()} != {_RAW_NAME}:
        raise AttemptStateError("terminal attempt inventory changed during cleanup")
    if not stat.S_ISREG((entry / _RAW_NAME).lstat().st_mode):
        raise AttemptStateError("terminal raw is not a regular file")
    (entry / _RAW_NAME).unlink()
    _sync_directory(entry, filesystem._fsync)
    entry.rmdir()
    _sync_directory(filesystem.attempts_root, filesystem._fsync)
    with filesystem.state_db.transaction() as transaction:
        transaction.delete(StateKind.ATTEMPT, attempt_id)
