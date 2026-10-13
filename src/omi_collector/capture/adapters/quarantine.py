"""Quarantine admission, retention, and terminal evidence lifecycle."""

from __future__ import annotations

import sqlite3
import time
from collections.abc import Callable, Iterator
from contextlib import closing, contextmanager
from dataclasses import dataclass
from typing import cast

from ..domain.internal_audio_machine import (
    AudioAction,
    AudioPhase,
    AuthenticatedConflict,
)
from ..domain.internal_audio_machine import (
    transition as audio_transition,
)
from ..domain.quarantine_machine import (
    QuarantineState,
)
from .collector_state_db import StateDBError, StateTransaction
from .internal_audio_store import (
    DEFAULT_CHUNK_LIMIT,
    MAX_CHUNK_LIMIT,
    InternalAudioStoreError,
    authenticate_capture_source,
)
from .staging_contract import (
    AttemptDescriptor,
    AttemptStateError,
    PendingAttemptError,
    StagingError,
    _validate_attempt_id,
)
from .staging_filesystem import (
    DeviceLock,
    StagingFilesystem,
    _never_defer,
)


class QuarantinePublishError(ValueError):
    """Typed quarantine evidence cannot authenticate a scheduler prefix."""


class QuarantineSalvageDeferredError(Exception):
    """Cooperative maintenance deferred before SQL admission committed."""


def _wall_clock_ns() -> int:
    return time.time_ns()


def _seconds_to_nanoseconds(seconds: float) -> int:
    return int(seconds * 1_000_000_000)


def _typed_salvage_source_ids(filesystem: StagingFilesystem) -> tuple[str, ...]:
    """Return only parseable typed quarantine sources; opaque evidence is terminal."""
    try:
        with closing(filesystem.state_db.open_connection(read_only=True)) as connection:
            rows = connection.execute(
                "SELECT source_id FROM capture_source "
                "WHERE source_type = 'quarantine' AND COALESCE(quarantine_opaque, 0) = 0 "
                "ORDER BY source_id"
            ).fetchall()
    except (OSError, sqlite3.DatabaseError, StateDBError) as error:
        raise StagingError("typed quarantine inventory is unreadable") from error
    typed_rows = cast(list[tuple[object]], rows)
    return tuple(str(row[0]) for row in typed_rows)


@contextmanager
def _typed_transaction(filesystem: StagingFilesystem) -> Iterator[sqlite3.Connection]:
    """Run one typed quarantine mutation with explicit rollback on failure."""
    with closing(filesystem.state_db.open_connection()) as connection:
        try:
            connection.execute("BEGIN IMMEDIATE")
            yield connection
            connection.commit()
        except BaseException:
            connection.rollback()
            raise


def _typed_quarantine_pending(filesystem: StagingFilesystem, reason: str) -> tuple[str, ...]:
    try:
        with _typed_transaction(filesystem) as connection:
            rows = cast(
                list[tuple[object, object]],
                connection.execute(
                    "SELECT source_id, lifecycle FROM capture_source "
                    "WHERE source_type = 'attempt' AND lifecycle IN "
                    "('prepared','leg_prepared','receiving','interrupted') ORDER BY source_id"
                ).fetchall(),
            )
            source_ids = tuple(str(row[0]) for row in rows)
            for row in rows:
                _typed_quarantine_row(
                    connection,
                    str(row[0]),
                    str(row[1]),
                    reason,
                    filesystem._terminal_retention_seconds,
                )
    except (OSError, sqlite3.DatabaseError, StateDBError) as error:
        raise StagingError("typed quarantine admission failed") from error
    return source_ids


def _typed_quarantine_row(
    connection: sqlite3.Connection,
    source_id: str,
    lifecycle: str,
    reason: str,
    retention_seconds: float,
) -> None:
    """Block one exact active attempt snapshot without rewriting terminal rows."""
    try:
        phase = AudioPhase(lifecycle)
    except ValueError as error:
        raise AttemptStateError("typed attempt lifecycle is unsupported") from error
    decision = audio_transition(phase, AuthenticatedConflict())
    if decision.action is not AudioAction.BLOCK or decision.next_phase is not AudioPhase.BLOCKED:
        raise AttemptStateError(decision.reason)
    now = _wall_clock_ns()
    deadline = now + _seconds_to_nanoseconds(retention_seconds)
    cursor = connection.execute(
        "UPDATE capture_source SET source_type = 'quarantine', lifecycle = ?, "
        "block_reason = ?, quarantine_disposition = CASE WHEN committed_count = 0 "
        "THEN 'unprocessable' ELSE 'unclassified' END, quarantine_opaque = 0, "
        "terminal_reason = CASE WHEN committed_count = 0 "
        "THEN 'quarantine contains no committed records' ELSE NULL END, "
        "terminal_time = ?, retention_deadline = ?, "
        "certified_prefix_count = CASE WHEN committed_count = 0 THEN NULL ELSE committed_count END, "
        "coverage_digest = CASE WHEN committed_count = 0 THEN NULL ELSE committed_sha256 END "
        "WHERE source_id = ? AND source_type = 'attempt' AND lifecycle = ?",
        (decision.next_phase.value, reason, str(now), str(deadline), source_id, lifecycle),
    )
    if cursor.rowcount != 1:
        raise AttemptStateError("typed attempt changed during quarantine admission")


@dataclass(frozen=True, slots=True)
class QuarantinePrefixAdmission:
    """A certified SQL prefix admitted to the normal publication scheduler."""

    source_id: str
    start_sequence: int
    next_sequence: int
    record_count: int
    raw_sha256: str
    lifecycle: str


@dataclass(frozen=True, slots=True)
class _TypedAdmissionSnapshot:
    lifecycle: str
    start_sequence: int
    record_count: int
    raw_sha256: str
    disposition: object
    terminal_time: object
    retention_deadline: object


def _typed_admission_snapshot(row: tuple[object, ...]) -> _TypedAdmissionSnapshot:
    lifecycle = str(row[0])
    count, digest = row[2], row[3]
    if row[6] == 1:
        raise QuarantinePublishError("opaque typed quarantine evidence cannot be admitted")
    if lifecycle not in {"blocked", "covered", "retiring"}:
        raise QuarantinePublishError("typed quarantine source is not eligible for prefix admission")
    if type(count) is not int or count <= 0 or not isinstance(digest, str):
        raise QuarantinePublishError("typed quarantine source has no committed prefix")
    if row[4] not in {None, count} or row[5] not in {None, digest}:
        raise QuarantinePublishError("typed quarantine prefix certification is inconsistent")
    if row[7] not in {None, "unclassified", "published_prefix"}:
        raise QuarantinePublishError("typed quarantine source is already terminally unprocessable")
    return _TypedAdmissionSnapshot(
        lifecycle,
        int.from_bytes(cast(bytes, row[1]), "big"),
        count,
        digest,
        row[7],
        row[8],
        row[9],
    )


def _persist_typed_admission(
    connection: sqlite3.Connection,
    source_id: str,
    snapshot: _TypedAdmissionSnapshot,
    terminal_time: str,
    deadline: str,
) -> None:
    if snapshot.disposition == "published_prefix":
        if snapshot.terminal_time is None or snapshot.retention_deadline is None:
            raise QuarantinePublishError("typed quarantine retention evidence is incomplete")
        return
    retained_terminal_time, retained_deadline = _retention_clock_or_default(
        snapshot.terminal_time,
        snapshot.retention_deadline,
        terminal_time,
        deadline,
    )
    changed = connection.execute(
        "UPDATE capture_source SET certified_prefix_count = ?, coverage_digest = ?, "
        "quarantine_disposition = 'published_prefix', terminal_reason = NULL, "
        "terminal_time = ?, retention_deadline = ? "
        "WHERE source_id = ? AND source_type = 'quarantine' AND lifecycle = ? "
        "AND quarantine_disposition IN ('unclassified')",
        (
            snapshot.record_count,
            snapshot.raw_sha256,
            retained_terminal_time,
            retained_deadline,
            source_id,
            snapshot.lifecycle,
        ),
    )
    if changed.rowcount != 1:
        raise AttemptStateError("typed quarantine source changed during prefix admission")


def _retention_clock_or_default(
    terminal_time: object,
    retention_deadline: object,
    default_terminal_time: str,
    default_deadline: str,
) -> tuple[str, str]:
    if isinstance(terminal_time, str) and isinstance(retention_deadline, str):
        try:
            if 0 <= int(terminal_time) <= int(retention_deadline):
                return terminal_time, retention_deadline
        except ValueError:
            pass
    return default_terminal_time, default_deadline


def _read_typed_admission_snapshot(connection: sqlite3.Connection, source_id: str) -> _TypedAdmissionSnapshot:
    row = cast(
        tuple[object, ...] | None,
        connection.execute(
            "SELECT lifecycle, sequence_start, committed_count, committed_sha256, "
            "certified_prefix_count, coverage_digest, quarantine_opaque, quarantine_disposition, "
            "terminal_time, retention_deadline FROM capture_source "
            "WHERE source_id = ? AND source_type = 'quarantine'",
            (source_id,),
        ).fetchone(),
    )
    if row is None:
        raise QuarantinePublishError("typed quarantine source does not exist")
    return _typed_admission_snapshot(row)


def _quarantine_admission(source_id: str, snapshot: _TypedAdmissionSnapshot) -> QuarantinePrefixAdmission:
    return QuarantinePrefixAdmission(
        source_id,
        snapshot.start_sequence,
        snapshot.start_sequence + snapshot.record_count,
        snapshot.record_count,
        snapshot.raw_sha256,
        snapshot.lifecycle,
    )


def _require_authenticated_admission(
    source_id: str, snapshot: _TypedAdmissionSnapshot, filesystem: StagingFilesystem, page_limit: int
) -> None:
    try:
        source = authenticate_capture_source(filesystem.state_db, source_id, page_limit=page_limit)
    except InternalAudioStoreError as error:
        raise QuarantinePublishError("typed quarantine prefix authentication failed") from error
    if (
        source.source_type != "quarantine"
        or source.lifecycle != snapshot.lifecycle
        or source.sequence_start != snapshot.start_sequence
        or source.committed_count != snapshot.record_count
        or source.committed_sha256 != snapshot.raw_sha256
    ):
        raise QuarantinePublishError("typed quarantine source changed during prefix authentication")


def admit_quarantined_prefix(
    filesystem: StagingFilesystem,
    source_id: str,
    *,
    page_limit: int = DEFAULT_CHUNK_LIMIT,
    should_defer: Callable[[], bool] = _never_defer,
) -> QuarantinePrefixAdmission:
    """Certify one parseable prefix and start retention for its residual evidence."""
    _validate_attempt_id(source_id)
    if should_defer():
        raise QuarantineSalvageDeferredError("quarantine admission deferred")
    try:
        with closing(filesystem.state_db.open_connection(read_only=True)) as connection:
            inspected = _read_typed_admission_snapshot(connection, source_id)
        if inspected.disposition == "published_prefix":
            if inspected.terminal_time is None or inspected.retention_deadline is None:
                raise QuarantinePublishError("typed quarantine retention evidence is incomplete")
            return _quarantine_admission(source_id, inspected)
        _require_authenticated_admission(source_id, inspected, filesystem, page_limit)
        if should_defer():
            raise QuarantineSalvageDeferredError("quarantine admission deferred")
        now_ns = _wall_clock_ns()
        terminal_time = str(now_ns)
        deadline = str(now_ns + _seconds_to_nanoseconds(filesystem._terminal_retention_seconds))
        with _typed_transaction(filesystem) as connection:
            snapshot = _read_typed_admission_snapshot(connection, source_id)
            if snapshot.disposition != "published_prefix" and snapshot != inspected:
                raise AttemptStateError("typed quarantine source changed during prefix admission")
            _persist_typed_admission(connection, source_id, snapshot, terminal_time, deadline)
            return _quarantine_admission(source_id, snapshot)
    except AttemptStateError, QuarantinePublishError, QuarantineSalvageDeferredError:
        raise
    except (OSError, sqlite3.DatabaseError, StateDBError, TypeError, ValueError) as error:
        raise StagingError("typed quarantine prefix admission failed") from error


def quarantine_pending(filesystem: StagingFilesystem, reason: str) -> tuple[str, ...]:
    """Move active typed attempts into durable quarantine custody."""
    if not isinstance(reason, str) or not reason.strip():
        raise AttemptStateError("quarantine reason must be a non-empty string")
    return _typed_quarantine_pending(filesystem, reason)


def quarantine_attempt_source(filesystem: StagingFilesystem, attempt_id: str) -> str:
    """Move one preserved typed attempt into quarantine custody."""
    _validate_attempt_id(attempt_id)
    try:
        with _typed_transaction(filesystem) as connection:
            row = cast(
                tuple[object, object] | None,
                connection.execute(
                    "SELECT source_type, lifecycle FROM capture_source WHERE source_id = ?", (attempt_id,)
                ).fetchone(),
            )
            if row is None:
                raise AttemptStateError("typed quarantine source does not exist")
            source_type, lifecycle = str(row[0]), str(row[1])
            if source_type == "quarantine" and lifecycle == "blocked":
                return attempt_id
            if source_type != "attempt":
                raise AttemptStateError("typed quarantine source is not an attempt")
            if lifecycle not in {"prepared", "leg_prepared", "receiving", "interrupted"}:
                raise AttemptStateError("attempt source is not an active partial")
            _typed_quarantine_row(
                connection,
                attempt_id,
                lifecycle,
                "attempt source preserved",
                filesystem._terminal_retention_seconds,
            )
    except AttemptStateError:
        raise
    except (OSError, sqlite3.DatabaseError, StateDBError) as error:
        raise StagingError("typed quarantine source transition failed") from error
    return attempt_id


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
    _typed_terminalize_prefix(filesystem, attempt_id)


def _typed_terminalize_prefix(filesystem: StagingFilesystem, attempt_id: str) -> None:
    try:
        with _typed_transaction(filesystem) as connection:
            row = cast(
                tuple[object] | None,
                connection.execute(
                    "SELECT lifecycle FROM capture_source WHERE source_id = ? AND source_type = 'attempt'",
                    (attempt_id,),
                ).fetchone(),
            )
            if row is None:
                raise AttemptStateError("typed prefix source does not exist")
            lifecycle = str(row[0])
            if lifecycle == "sealed_waiting":
                return
            if lifecycle not in {"prepared", "leg_prepared", "receiving", "interrupted"}:
                raise AttemptStateError("attempt source is not an active partial")
            cursor = connection.execute(
                "UPDATE capture_source SET lifecycle = 'sealed_waiting', terminal_time = ? "
                "WHERE source_id = ? AND source_type = 'attempt' AND lifecycle = ?",
                (str(_wall_clock_ns()), attempt_id, lifecycle),
            )
            if cursor.rowcount != 1:
                raise AttemptStateError("typed prefix source changed during terminalization")
    except AttemptStateError:
        raise
    except (OSError, sqlite3.DatabaseError, StateDBError) as error:
        raise StagingError("typed prefix terminalization failed") from error


def quarantined_attempts(
    filesystem: StagingFilesystem,
    *,
    should_defer: Callable[[], bool] = _never_defer,
) -> tuple[str, ...]:
    """Return parseable typed quarantine sources available for admission."""
    return () if should_defer() else _typed_salvage_source_ids(filesystem)


def mark_quarantine_unprocessable(filesystem: StagingFilesystem, source: str, reason: str) -> None:
    """Terminally classify evidence that cannot authenticate a publishable prefix."""
    if not reason:
        raise AttemptStateError("unprocessable reason must be non-empty")
    _typed_mark_unprocessable(filesystem, source, reason)


def _typed_mark_unprocessable(filesystem: StagingFilesystem, source_id: str, reason: str) -> None:
    now_ns = _wall_clock_ns()
    now = str(now_ns)
    deadline = str(now_ns + _seconds_to_nanoseconds(filesystem._terminal_retention_seconds))
    try:
        with _typed_transaction(filesystem) as connection:
            current = cast(
                tuple[object, object, object, object] | None,
                connection.execute(
                    "SELECT lifecycle, quarantine_disposition, terminal_reason, retention_deadline "
                    "FROM capture_source WHERE source_id = ? AND source_type = 'quarantine'",
                    (source_id,),
                ).fetchone(),
            )
            if current is None:
                raise AttemptStateError("typed quarantine source does not exist")
            if current[1] == "unprocessable":
                return
            if current[1] not in {None, "unclassified"} or current[0] not in {"blocked", "covered", "retiring"}:
                raise AttemptStateError("typed quarantine source is not ready for terminal classification")
            changed = connection.execute(
                "UPDATE capture_source SET quarantine_disposition = ?, "
                "terminal_reason = ?, terminal_time = ?, retention_deadline = ?, "
                "certified_prefix_count = NULL, coverage_digest = NULL "
                "WHERE source_id = ? AND source_type = 'quarantine' AND lifecycle = ? "
                "AND quarantine_disposition IN ('unclassified')",
                ("unprocessable", reason, now, deadline, source_id, current[0]),
            )
            if changed.rowcount != 1:
                raise AttemptStateError("typed quarantine source changed during terminal classification")
    except AttemptStateError:
        raise
    except (OSError, sqlite3.DatabaseError, StateDBError) as error:
        raise StagingError("typed quarantine terminal state failed") from error


def sweep_terminal_quarantine(
    filesystem: StagingFilesystem,
    *,
    should_defer: Callable[[], bool] = _never_defer,
    chunk_limit: int = DEFAULT_CHUNK_LIMIT,
) -> tuple[str, ...]:
    """Classify and expire terminal typed quarantine evidence."""
    if type(chunk_limit) is not int or not 0 < chunk_limit <= MAX_CHUNK_LIMIT:
        raise ValueError("chunk_limit must be between 1 and MAX_CHUNK_LIMIT")
    if should_defer():
        return ()
    return _typed_sweep_terminal_quarantine(filesystem, should_defer, chunk_limit)


def _typed_sweep_terminal_quarantine(
    filesystem: StagingFilesystem, should_defer: Callable[[], bool], chunk_limit: int
) -> tuple[str, ...]:
    now = _wall_clock_ns()
    removed: list[str] = []
    try:
        with _typed_transaction(filesystem) as connection:
            changes_before = connection.total_changes
            _classify_standalone_typed_evidence(filesystem, connection, now)
            _classify_one_typed_opaque(filesystem, connection, now)
            remaining = chunk_limit
            if not should_defer():
                source = _next_expired_typed_source(connection, now)
                if source is not None:
                    source_removed, spent = _sweep_one_typed_source(connection, source, now, remaining)
                    remaining -= spent
                    if source_removed:
                        removed.append(source)
            if remaining and not should_defer():
                _sweep_one_expired_standalone(connection, now, remaining)
            if connection.total_changes != changes_before:
                state = StateTransaction(connection)
                state.mark_publication_changed()
                state.finish()
    except (OSError, sqlite3.DatabaseError, StateDBError) as error:
        raise StagingError("typed quarantine retention sweep failed") from error
    return tuple(removed)


def _classify_one_typed_opaque(filesystem: StagingFilesystem, connection: sqlite3.Connection, now: int) -> None:
    row = cast(
        tuple[object] | None,
        connection.execute(
            "SELECT source_id FROM capture_source WHERE source_type = 'quarantine' "
            "AND quarantine_opaque = 1 AND quarantine_disposition = 'unclassified' ORDER BY source_id LIMIT 1"
        ).fetchone(),
    )
    if row is not None:
        _classify_typed_opaque(filesystem, connection, str(row[0]), (None, "blocked", "unclassified", 1), now)


def _expired_typed_source_sql() -> str:
    return (
        "source_type = 'quarantine' AND lifecycle IN ('blocked','covered','retiring') "
        "AND quarantine_disposition IN ('unclassified','unprocessable','published_prefix') "
        "AND retention_deadline != '' AND retention_deadline NOT GLOB '*[^0-9]*' "
        "AND CAST(retention_deadline AS INTEGER) <= ?"
    )


def _canonical_delete_eligible_sql() -> str:
    return (
        _expired_typed_source_sql() + " AND lifecycle = 'blocked' AND quarantine_disposition = 'unprocessable' "
        "AND NOT EXISTS (SELECT 1 FROM publication_cohort_member m WHERE m.source_id = capture_source.source_id) "
        "AND NOT EXISTS (SELECT 1 FROM ready_stage_range r WHERE r.source_id = capture_source.source_id)"
    )


def _next_expired_typed_source(connection: sqlite3.Connection, now: int) -> str | None:
    row = cast(
        tuple[object] | None,
        connection.execute(
            "SELECT source_id FROM capture_source WHERE " + _expired_typed_source_sql() + " AND ("
            "EXISTS (SELECT 1 FROM capture_tail WHERE source_id = capture_source.source_id) OR "
            "EXISTS (SELECT 1 FROM capture_conflict WHERE source_id = capture_source.source_id) OR "
            "EXISTS (SELECT 1 FROM capture_evidence WHERE source_id = capture_source.source_id) OR "
            "(" + _canonical_delete_eligible_sql() + ")) ORDER BY source_id LIMIT 1",
            (now, now),
        ).fetchone(),
    )
    return None if row is None else str(row[0])


def _sweep_one_typed_source(
    connection: sqlite3.Connection, source_id: str, now: int, chunk_limit: int
) -> tuple[bool, int]:
    row = cast(
        tuple[object, object, object, object] | None,
        connection.execute(
            "SELECT retention_deadline, lifecycle, quarantine_disposition, quarantine_opaque "
            "FROM capture_source WHERE source_id = ?",
            (source_id,),
        ).fetchone(),
    )
    if row is None or not _typed_retention_expired(row, now):
        return False, 0
    remaining = chunk_limit
    if row[1] == "blocked" and row[2] == "unprocessable" and _canonical_unreferenced(connection, source_id):
        remaining -= _delete_typed_children(connection, "capture_record", source_id, now, remaining)
    if remaining:
        remaining -= _delete_typed_evidence_chunks(connection, source_id, now, remaining)
    if remaining:
        remaining -= _delete_typed_children(connection, "capture_tail", source_id, now, remaining)
    if remaining:
        remaining -= _delete_typed_children(connection, "capture_conflict", source_id, now, remaining)
    if remaining:
        remaining -= _delete_typed_evidence_parent(connection, source_id, now, remaining)
    removed = _delete_expired_typed_source(connection, source_id, now) if row[2] == "unprocessable" else False
    return removed, chunk_limit - remaining


def _canonical_unreferenced(connection: sqlite3.Connection, source_id: str) -> bool:
    return (
        connection.execute(
            "SELECT 1 WHERE NOT EXISTS (SELECT 1 FROM publication_cohort_member WHERE source_id = ?) "
            "AND NOT EXISTS (SELECT 1 FROM ready_stage_range WHERE source_id = ?)",
            (source_id, source_id),
        ).fetchone()
        is not None
    )


def _delete_typed_children(connection: sqlite3.Connection, table: str, source_id: str, now: int, limit: int) -> int:
    if limit <= 0:
        return 0
    key = "ordinal" if table == "capture_record" else "chunk_ordinal" if table == "capture_conflict" else "source_id"
    eligibility = _canonical_delete_eligible_sql() if table == "capture_record" else _expired_typed_source_sql()
    deleted = connection.execute(
        f"DELETE FROM {table} WHERE source_id = ? AND {key} IN ("
        f"SELECT {key} FROM {table} WHERE source_id = ? ORDER BY {key} LIMIT ?) "
        "AND EXISTS (SELECT 1 FROM capture_source WHERE source_id = ? AND " + eligibility + ")",
        (source_id, source_id, limit, source_id, now),
    )
    return deleted.rowcount


def _delete_typed_evidence_chunks(connection: sqlite3.Connection, source_id: str, now: int, limit: int) -> int:
    if limit <= 0:
        return 0
    deleted = connection.execute(
        "DELETE FROM capture_evidence_chunk WHERE (evidence_id, chunk_ordinal) IN ("
        "SELECT c.evidence_id, c.chunk_ordinal FROM capture_evidence_chunk c "
        "JOIN capture_evidence e ON e.evidence_id = c.evidence_id "
        "WHERE e.source_id = ? ORDER BY c.evidence_id, c.chunk_ordinal LIMIT ?) "
        "AND EXISTS (SELECT 1 FROM capture_source WHERE source_id = ? AND " + _expired_typed_source_sql() + ")",
        (source_id, limit, source_id, now),
    )
    return deleted.rowcount


def _delete_typed_evidence_parent(connection: sqlite3.Connection, source_id: str, now: int, limit: int) -> int:
    deleted = connection.execute(
        "DELETE FROM capture_evidence WHERE evidence_id IN ("
        "SELECT evidence_id FROM capture_evidence WHERE source_id = ? "
        "AND NOT EXISTS (SELECT 1 FROM capture_evidence_chunk c WHERE c.evidence_id = capture_evidence.evidence_id) "
        "ORDER BY evidence_id LIMIT ?) "
        "AND EXISTS (SELECT 1 FROM capture_source WHERE source_id = ? AND " + _expired_typed_source_sql() + ")",
        (source_id, limit, source_id, now),
    )
    return deleted.rowcount


def _classify_typed_opaque(
    filesystem: StagingFilesystem,
    connection: sqlite3.Connection,
    source_id: str,
    row: tuple[object, object, object, object] | None,
    now: int,
) -> bool:
    if row is None or row[3] != 1 or row[2] not in {None, "unclassified"}:
        return False
    terminal_time = str(now)
    deadline = str(now + _seconds_to_nanoseconds(filesystem._terminal_retention_seconds))
    connection.execute(
        "UPDATE capture_source SET quarantine_disposition = 'unprocessable', "
        "terminal_reason = 'opaque typed quarantine evidence', terminal_time = ?, "
        "retention_deadline = ?, certified_prefix_count = NULL, coverage_digest = NULL "
        "WHERE source_id = ? AND source_type = 'quarantine' "
        "AND quarantine_disposition IN ('unclassified')",
        (terminal_time, deadline, source_id),
    )
    return True


def _typed_retention_expired(row: tuple[object, object, object, object], now: int) -> bool:
    if (
        row[0] is None
        or row[1] not in {"blocked", "covered", "retiring"}
        or row[2] not in {"unclassified", "published_prefix", "unprocessable"}
    ):
        return False
    try:
        return now >= int(cast(str, row[0]))
    except TypeError, ValueError:
        return False


def _classify_standalone_typed_evidence(
    filesystem: StagingFilesystem,
    connection: sqlite3.Connection,
    now: int,
) -> None:
    """Give unlinked evidence its own terminal lifecycle and retention clock."""
    terminal_time = str(now)
    deadline = str(now + _seconds_to_nanoseconds(filesystem._terminal_retention_seconds))
    connection.execute(
        "UPDATE capture_evidence SET disposition = 'unprocessable', "
        "terminal_reason = 'standalone typed quarantine evidence', terminal_time = ?, retention_deadline = ? "
        "WHERE evidence_id = (SELECT evidence_id FROM capture_evidence "
        "WHERE source_id IS NULL AND disposition = 'unclassified' ORDER BY evidence_id LIMIT 1)",
        (terminal_time, deadline),
    )


def _sweep_one_expired_standalone(connection: sqlite3.Connection, now: int, limit: int) -> int:
    row = cast(
        tuple[object] | None,
        connection.execute(
            "SELECT evidence_id FROM capture_evidence WHERE source_id IS NULL "
            "AND disposition IN ('published_prefix','unprocessable') "
            "AND retention_deadline != '' AND retention_deadline NOT GLOB '*[^0-9]*' "
            "AND CAST(retention_deadline AS INTEGER) <= ? ORDER BY evidence_id LIMIT 1",
            (now,),
        ).fetchone(),
    )
    if row is None:
        return 0
    evidence_id = str(row[0])
    spent = connection.execute(
        "DELETE FROM capture_evidence_chunk WHERE evidence_id = ? AND chunk_ordinal IN ("
        "SELECT chunk_ordinal FROM capture_evidence_chunk WHERE evidence_id = ? ORDER BY chunk_ordinal LIMIT ?) "
        "AND EXISTS (SELECT 1 FROM capture_evidence WHERE evidence_id = ? AND source_id IS NULL "
        "AND disposition IN ('published_prefix','unprocessable') AND retention_deadline != '' "
        "AND retention_deadline NOT GLOB '*[^0-9]*' AND CAST(retention_deadline AS INTEGER) <= ?)",
        (evidence_id, evidence_id, limit, evidence_id, now),
    ).rowcount
    if spent < limit:
        spent += connection.execute(
            "DELETE FROM capture_evidence WHERE evidence_id = ? "
            "AND source_id IS NULL AND disposition IN ('published_prefix','unprocessable') "
            "AND retention_deadline != '' AND retention_deadline NOT GLOB '*[^0-9]*' "
            "AND CAST(retention_deadline AS INTEGER) <= ? "
            "AND NOT EXISTS (SELECT 1 FROM capture_evidence_chunk WHERE evidence_id = ?)",
            (evidence_id, now, evidence_id),
        ).rowcount
    return spent


def _delete_expired_typed_source(
    connection: sqlite3.Connection,
    source_id: str,
    now: int,
) -> bool:
    deleted = connection.execute(
        "DELETE FROM capture_source WHERE source_id = ? AND "
        + _canonical_delete_eligible_sql()
        + " AND NOT EXISTS (SELECT 1 FROM capture_record WHERE source_id = capture_source.source_id) "
        "AND NOT EXISTS (SELECT 1 FROM capture_tail WHERE source_id = capture_source.source_id) "
        "AND NOT EXISTS (SELECT 1 FROM capture_conflict WHERE source_id = capture_source.source_id) "
        "AND NOT EXISTS (SELECT 1 FROM capture_evidence WHERE source_id = ?)",
        (source_id, now, source_id),
    )
    return deleted.rowcount == 1


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


def _validate_quarantine_payload(entry_name: str, row: dict[str, object]) -> dict[str, object]:
    if set(row) != _QUARANTINE_FIELDS:
        raise AttemptStateError("quarantine state is missing or malformed")
    if not _valid_quarantine_identity(entry_name, row):
        raise AttemptStateError("quarantine state is malformed")
    _validate_quarantine_terminal(row)
    return row


def _validate_quarantine_terminal(row: dict[str, object]) -> None:
    _validate_quarantine_attempt(row["attempt_id"])
    _validate_terminal_evidence(row["terminal_state"], row["terminal_at_unix_ns"], row["terminal_reason"])


def _validate_quarantine_attempt(attempt_id: object) -> None:
    if attempt_id is not None:
        if not isinstance(attempt_id, str):
            raise AttemptStateError("quarantine attempt identity is malformed")
        try:
            _validate_attempt_id(attempt_id)
        except (TypeError, ValueError) as error:
            raise AttemptStateError("quarantine attempt identity is malformed") from error


def _valid_terminal_timestamp(value: object) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and value >= 0


def _validate_terminal_evidence(terminal_state: object, terminal_at: object, terminal_reason: object) -> None:
    if terminal_state is None:
        if terminal_at is not None or terminal_reason is not None:
            raise AttemptStateError("quarantine terminal evidence is contradictory")
        return
    if terminal_state == "published":
        if not _valid_terminal_timestamp(terminal_at) or terminal_reason is not None:
            raise AttemptStateError("quarantine terminal evidence is malformed")
        return
    if terminal_state == "unprocessable":
        if not _valid_terminal_timestamp(terminal_at) or not isinstance(terminal_reason, str) or not terminal_reason:
            raise AttemptStateError("quarantine terminal evidence is malformed")
        return
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


def _quarantine_state(filesystem: StagingFilesystem, source_id: str) -> QuarantineState:
    """Classify one typed quarantine source without mutating its evidence."""
    try:
        with closing(filesystem.state_db.open_connection(read_only=True)) as connection:
            row = cast(
                tuple[object, object] | None,
                connection.execute(
                    "SELECT quarantine_disposition, quarantine_opaque FROM capture_source "
                    "WHERE source_id = ? AND source_type = 'quarantine'",
                    (source_id,),
                ).fetchone(),
            )
        if row is None or row[1] == 1:
            return QuarantineState.INVALID_EVIDENCE
        if row[0] in {None, "unclassified"}:
            return QuarantineState.RETRYABLE
        return QuarantineState.PUBLISHED if row[0] == "published_prefix" else QuarantineState.UNPROCESSABLE
    except OSError, sqlite3.DatabaseError, StateDBError:
        return QuarantineState.INVALID_EVIDENCE


def assert_no_pending(filesystem: StagingFilesystem) -> None:
    """Fail closed when preserved partial evidence exists."""
    if pending_attempts(filesystem):
        raise PendingAttemptError("partial staging evidence blocks another READ")


def pending_attempts(filesystem: StagingFilesystem) -> tuple[AttemptDescriptor, ...]:
    """Return active partial attempts from typed SQL custody."""
    try:
        with closing(filesystem.state_db.open_connection(read_only=True)) as connection:
            rows = cast(
                list[tuple[object, object, object]],
                connection.execute(
                    "SELECT source_id, sequence_start, sequence_limit FROM capture_source "
                    "WHERE source_type = 'attempt' AND lifecycle IN "
                    "('prepared','leg_prepared','receiving','interrupted') ORDER BY source_id"
                ).fetchall(),
            )
    except (OSError, sqlite3.DatabaseError, StateDBError) as error:
        raise PendingAttemptError("typed partial staging cannot be inspected") from error
    from .internal_audio_store import decode_uint64

    return tuple(
        AttemptDescriptor(
            str(row[0]),
            2,
            decode_uint64(cast(bytes, row[1])),
            decode_uint64(cast(bytes, row[2])) - decode_uint64(cast(bytes, row[1])),
        )
        for row in rows
    )
