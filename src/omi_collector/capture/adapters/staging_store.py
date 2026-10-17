"""Sequencing facade for durable staging attempts."""

from __future__ import annotations

import errno
import os
import sqlite3
from collections.abc import Callable, Iterator
from contextlib import closing, contextmanager
from functools import partial
from logging import getLogger
from pathlib import Path
from threading import Event, Lock
from time import monotonic
from typing import cast
from uuid import uuid4

from ..application.ports import StagingWriterTargetPort, StorageLeasePort
from ..application.storage_fault import TransientInternalAudioStorageError
from ..domain.quarantine_machine import QuarantineState
from ..domain.ready_machine import (
    CaptureBegin,
    CaptureEnd,
    Compacting,
    CompactionDue,
    CompactionFinished,
    CompactionProbeFailed,
    Finished,
    InputChanged,
    PublicationAction,
    PublicationCommand,
    PublicationEvent,
    PublicationMode,
    PublicationResult,
    PublicationState,
    Quiesced,
    RetryWait,
    Running,
    Settled,
    Shutdown,
    TimerFired,
    Wake,
    publication_transition,
    recovered_closure,
)
from ..domain.ring_protocol import RingInfo
from . import quarantine, ready_closures
from .attempts import SqliteAttemptInspection, SqliteStagedAttempt
from .clock_segments import ClockSegmentError, ClockSegmentMap
from .collector_state_db import StateDB, StateDBError, StateKind, StateTransaction, raise_if_sqlite_storage_fault
from .confirmed_loss import ConfirmedLossError, ConfirmedLossLedger
from .debug_logging import debug_event, debug_exception
from .internal_audio_store import (
    AUDIO_RECORD_SIZE,
    CaptureSource,
    InternalAudioStore,
    InternalAudioStoreError,
    authenticate_capture_source,
    capture_space_requirement,
    read_capture_source,
    refresh_utc_projection,
)
from .public_drain import AcceptedDrainCustody, PublicDrainProjection
from .ready_bundles import (
    ReadyBundleError,
    ReadyCustodyCache,
    ReadyOutcome,
    ReadyOutcomeState,
    SqlitePublicationSource,
    _typed_source_admissions,
    acknowledged_ready,
    finalize_typed_sources,
    resume_retired,
    retire_acknowledged,
    source_revision,
)
from .sqlite_compaction import compaction_failure_outcome, pending_compaction_generation, vacuum_pending
from .staging_contract import (
    AttemptDescriptor,
    AttemptStateError,
    DeviceAlreadyRunningError,
    DiskSpaceError,
    FatalInternalAudioStorageError,
    PendingAttemptError,
    StagingError,
    _validate_attempt_id,
    _validate_count,
    _validate_int,
)
from .staging_filesystem import (
    DEFAULT_STAGING_IO,
    DeviceLock,
    StagingFilesystem,
    StagingIO,
    StagingPaths,
    _never_defer,
)

_LOGGER = getLogger(__name__)
_UNKNOWN_REVISION = object()
_PUBLICATION_ERROR_PATH_LIMIT = 256


def _debug_publication_failure(
    error: BaseException,
    *,
    phase: str,
    path: Path | None = None,
) -> None:
    filename: object = cast(object, error.filename) if isinstance(error, OSError) else None
    if filename is None and path is not None:
        filename = os.fspath(path)
    if isinstance(filename, bytes):
        filename = os.fsdecode(filename)
    diagnostic_path = filename[:_PUBLICATION_ERROR_PATH_LIMIT] if isinstance(filename, str) else None
    debug_event(
        "ready_publication_failure",
        phase=phase,
        error_type=type(error).__name__,
        errno=error.errno if isinstance(error, OSError) else None,
        path=diagnostic_path,
    )


def _publication_io_reason(error: OSError | TransientInternalAudioStorageError) -> str:
    if isinstance(error, TransientInternalAudioStorageError):
        return "publication_sqlite_busy"
    if error.errno in {errno.EACCES, errno.EPERM}:
        return "publication_access_denied"
    return "publication_io_error"


def _db_one(cursor: sqlite3.Cursor) -> sqlite3.Row | tuple[object, ...] | None:
    try:
        return cast(sqlite3.Row | tuple[object, ...] | None, cursor.fetchone())
    finally:
        cursor.close()


def _db_all(cursor: sqlite3.Cursor) -> list[sqlite3.Row] | list[tuple[object, ...]]:
    try:
        return cursor.fetchall()
    finally:
        cursor.close()


class StagingStore:
    """Stage transport state in spool and publish bundles in capture_root."""

    def __init__(
        self,
        spool: Path,
        capture_root: Path,
        *,
        state_db: StateDB,
        io: StagingIO = DEFAULT_STAGING_IO,
    ) -> None:
        self._config = io.config
        self._state_db = state_db
        self._public_drain = PublicDrainProjection(state_db, state_db.path.parent / "public" / "pendant-drain.json")
        self._filesystem = StagingFilesystem(
            spool,
            capture_root,
            state_db=state_db,
            io=io,
        )
        self._publication_root: Path | None = None
        self._publication_lock = Lock()
        self._publication_state = PublicationState()
        self._ready_custody_cache: ReadyCustodyCache | None = None
        self._compaction_interrupt = Event()

    @classmethod
    def from_paths(
        cls,
        paths: StagingPaths,
        *,
        state_db: StateDB,
        io: StagingIO = DEFAULT_STAGING_IO,
        publication_root: Path | None = None,
    ) -> StagingStore:
        """Build a store from the external layout authority."""
        store = cls.__new__(cls)
        store._config = io.config
        store._state_db = state_db
        store._public_drain = PublicDrainProjection(state_db, state_db.path.parent / "public" / "pendant-drain.json")
        store._filesystem = StagingFilesystem.from_paths(paths, state_db=state_db, io=io)
        store._publication_root = publication_root
        store._publication_lock = Lock()
        store._publication_state = PublicationState()
        store._ready_custody_cache = None
        store._compaction_interrupt = Event()
        return store

    @property
    def public_drain(self) -> PublicDrainProjection:
        """Expose the private drain recorder to the coordinator composition."""
        return self._public_drain

    def publish_ready(self, held_lease: DeviceLock | None = None) -> ReadyOutcome:
        """Publish a complete normalized view when this store has an external boundary."""
        if self._publication_root is None:
            return ReadyOutcome(ReadyOutcomeState.WAITING, reason="publication_unconfigured")
        command = self._publication_event(Wake(None, monotonic()))
        return self._execute_publication_command(command, held_lease)

    def _publication_event(self, event: PublicationEvent) -> PublicationCommand:
        with self._publication_lock:
            self._publication_state, command = publication_transition(self._publication_state, event)
            return command

    def _publication_outcome(self, command: PublicationCommand) -> ReadyOutcome:
        with self._publication_lock:
            state = self._publication_state
        if command.action is PublicationAction.ARM and command.deadline is not None:
            reason = (
                state.work.outcome.reason
                if isinstance(state.work, RetryWait) and isinstance(state.work.outcome, ReadyOutcome)
                else "publication_io_error"
            )
            return ReadyOutcome(
                ReadyOutcomeState.TRANSIENT, reason=reason, retry_after_seconds=max(0.0, command.deadline - monotonic())
            )
        if state.mode is PublicationMode.CAPTURE:
            return ReadyOutcome(ReadyOutcomeState.WAITING, reason="capture_active")
        if state.mode is PublicationMode.CLOSED:
            return ReadyOutcome(ReadyOutcomeState.WAITING, reason="publication_closed")
        if isinstance(state.work, Settled) and isinstance(state.work.outcome, ReadyOutcome):
            if state.work.outcome.state is ReadyOutcomeState.PUBLISHED:
                return ReadyOutcome(ReadyOutcomeState.WAITING, reason="settled_input")
            return state.work.outcome
        return ReadyOutcome(ReadyOutcomeState.WAITING, reason="publication_active")

    def _execute_publication_command(self, command: PublicationCommand, held_lease: DeviceLock | None) -> ReadyOutcome:
        while command.action is PublicationAction.CHECK_INPUT:
            probe_error: OSError | TransientInternalAudioStorageError | None = None
            try:
                revision: object = self._publication_revision()
            except (OSError, TransientInternalAudioStorageError) as error:
                _debug_publication_failure(error, phase="input_revision")
                revision = _UNKNOWN_REVISION
                probe_error = error
            command = self._publication_event(Wake(revision, monotonic()))
            if probe_error is not None and command.action is PublicationAction.RUN:
                return self._finish_publication(
                    command,
                    revision,
                    ReadyOutcome(ReadyOutcomeState.TRANSIENT, reason=_publication_io_reason(probe_error)),
                )
        if command.action is PublicationAction.RUN:
            assert command.token is not None
            outcome, revision = self._run_publication_effect(command.token, held_lease)
            return self._finish_publication(command, revision, outcome, held_lease)
        if command.action is PublicationAction.COMPACT:
            return self._run_compaction(command, held_lease)
        return self._publication_outcome(command)

    def _run_compaction(self, command: PublicationCommand, held_lease: DeviceLock | None) -> ReadyOutcome:
        assert command.token is not None
        with self._publication_lock:
            work = self._publication_state.work
            admitted = (
                self._publication_state.mode is PublicationMode.AVAILABLE
                and isinstance(work, Compacting)
                and work.token == command.token
            )
        if not admitted or not isinstance(work, Compacting):
            return ReadyOutcome(ReadyOutcomeState.WAITING, reason="capture_active")
        try:
            if held_lease is None:
                with self.device_lock(operation="ready_compaction"):
                    result = vacuum_pending(self._state_db, work.debt_generation, self._compaction_interrupt)
            else:
                self._filesystem.require_device_lock(held_lease)
                result = vacuum_pending(self._state_db, work.debt_generation, self._compaction_interrupt)
        except FatalInternalAudioStorageError:
            raise
        except DeviceAlreadyRunningError as error:
            _debug_publication_failure(error, phase="ready_compaction")
            result = "deferred"
        except (OSError, sqlite3.DatabaseError, StateDBError, TransientInternalAudioStorageError) as error:
            _debug_publication_failure(error, phase="ready_compaction")
            result = compaction_failure_outcome(error, self._compaction_interrupt)
        outcome = ReadyOutcome(
            ReadyOutcomeState.TRANSIENT if result == "deferred" else ReadyOutcomeState.WAITING,
            reason=f"compaction_{result}",
        )
        next_command = self._publication_event(
            CompactionFinished(
                work.token,
                work.debt_generation,
                work.revision,
                PublicationResult.TRANSIENT if result == "deferred" else PublicationResult.SETTLED,
                monotonic(),
                self._config.retry.rapid_backoff,
            )
        )
        if next_command.action is PublicationAction.ARM:
            return self._publication_outcome(next_command)
        if next_command.action is PublicationAction.CHECK_INPUT:
            return self._execute_publication_command(next_command, held_lease)
        return outcome

    def _run_publication_effect(self, token: int, held_lease: DeviceLock | None) -> tuple[ReadyOutcome, object]:
        with self._publication_lock:
            running = self._publication_state.work
            allowed = (
                self._publication_state.mode is PublicationMode.AVAILABLE
                and isinstance(running, Running)
                and running.token == token
            )
        if not allowed or not isinstance(running, Running):
            return ReadyOutcome(ReadyOutcomeState.WAITING, reason="capture_active"), _UNKNOWN_REVISION
        phase = "publication_effect"
        try:
            if held_lease is None:
                with self.device_lock(operation="ready_publication"):
                    outcome = self._recover_and_publish_unlocked()
                    phase = "post_effect_revision"
                    revision = self._publication_revision()
            else:
                self._filesystem.require_device_lock(held_lease)
                outcome = self._recover_and_publish_unlocked()
                phase = "post_effect_revision"
                revision = self._publication_revision()
            return outcome, revision
        except FatalInternalAudioStorageError:
            raise
        except DeviceAlreadyRunningError as error:
            _debug_publication_failure(error, phase="device_lock", path=self._filesystem.lock_path)
            return ReadyOutcome(ReadyOutcomeState.TRANSIENT, reason="publication_lock_busy"), running.revision
        except (OSError, TransientInternalAudioStorageError) as error:
            _debug_publication_failure(error, phase=phase)
            return ReadyOutcome(ReadyOutcomeState.TRANSIENT, reason=_publication_io_reason(error)), running.revision
        except Exception as error:
            if not isinstance(error, ReadyBundleError):
                debug_exception(
                    "ready_publication_failure",
                    error,
                    phase=phase,
                )
                _LOGGER.exception("ready publication failed unexpectedly", exc_info=Exception("details redacted"))
            try:
                revision = self._publication_revision()
            except (OSError, TransientInternalAudioStorageError) as revision_error:
                if isinstance(error, ReadyBundleError):
                    _debug_publication_failure(error, phase="publication_effect")
                _debug_publication_failure(revision_error, phase="post_effect_revision")
                return (
                    ReadyOutcome(ReadyOutcomeState.TRANSIENT, reason=_publication_io_reason(revision_error)),
                    _UNKNOWN_REVISION,
                )
            reason = str(error) if isinstance(error, ReadyBundleError) else type(error).__name__
            return ReadyOutcome(ReadyOutcomeState.BLOCKED, reason=reason), revision

    def _finish_publication(
        self,
        command: PublicationCommand,
        revision: object,
        outcome: ReadyOutcome,
        held_lease: DeviceLock | None = None,
    ) -> ReadyOutcome:
        assert command.token is not None
        result = (
            PublicationResult.TRANSIENT if outcome.state is ReadyOutcomeState.TRANSIENT else PublicationResult.SETTLED
        )
        next_command = self._publication_event(
            Finished(command.token, revision, outcome, result, monotonic(), self._config.retry.rapid_backoff)
        )
        if next_command.action is PublicationAction.CHECK_INPUT:
            if outcome.retirement_pending:
                return outcome
            checked = self._execute_publication_command(next_command, held_lease)
            if outcome.state is ReadyOutcomeState.PUBLISHED and checked.state is ReadyOutcomeState.WAITING:
                return outcome
            return checked
        if next_command.action is PublicationAction.ARM:
            return self._publication_outcome(next_command)
        if (
            next_command.action is PublicationAction.NONE
            and not outcome.retirement_pending
            and outcome.state not in {ReadyOutcomeState.BLOCKED, ReadyOutcomeState.TRANSIENT}
            and not self._compaction_interrupt.is_set()
        ):
            compacted = self._compact_after_publication(revision, held_lease)
            if compacted is not None:
                return compacted
        return outcome

    def _compact_after_publication(self, revision: object, held_lease: DeviceLock | None) -> ReadyOutcome | None:
        try:
            generation = pending_compaction_generation(self._state_db)
        except FatalInternalAudioStorageError:
            raise
        except (OSError, ValueError, sqlite3.DatabaseError, StateDBError, TransientInternalAudioStorageError) as error:
            _debug_publication_failure(error, phase="compaction_probe")
            result = compaction_failure_outcome(error, self._compaction_interrupt)
            if result == "interrupted":
                return None
            self._publication_event(CompactionProbeFailed(revision, monotonic(), self._config.retry.rapid_backoff))
            return None
        if generation is None or self._compaction_interrupt.is_set():
            return None
        compact = self._publication_event(CompactionDue(revision, generation))
        if compact.action is PublicationAction.COMPACT:
            result = self._execute_publication_command(compact, held_lease)
            with self._publication_lock:
                work = self._publication_state.work
            # Surface a nested publication failure without replacing an already
            # published result with the separate maintenance outcome.
            if (
                result.state in {ReadyOutcomeState.BLOCKED, ReadyOutcomeState.TRANSIENT}
                and isinstance(work, (Settled, RetryWait))
                and isinstance(work.outcome, ReadyOutcome)
            ):
                return result
        return None

    def _publication_revision(self) -> object:
        if self._publication_root is None:
            return ()
        checkpoint_path = self._publication_root.parent / "work" / "omi-ready-checkpoint.json"
        return (
            self._state_db.get_generation(),
            source_revision(self._publication_root, extras=(checkpoint_path,)),
        )

    def publication_retry_schedule(self) -> tuple[int, float] | None:
        """Project the current reducer-owned deadline for the async timer handle."""
        with self._publication_lock:
            state = self._publication_state
            if state.mode is PublicationMode.AVAILABLE and isinstance(state.work, RetryWait):
                return state.generation, state.work.deadline
        return None

    def publication_followup_due(self) -> bool:
        """Report a reducer-issued input check that has not been consumed."""
        if self._publication_root is None:
            return False
        with self._publication_lock:
            state = self._publication_state
            return state.mode is PublicationMode.AVAILABLE and state.needs_check

    def publication_wake_admitted(self) -> bool:
        with self._publication_lock:
            return self._publication_root is not None and self._publication_state.mode is PublicationMode.AVAILABLE

    def publication_timer_fired(self, generation: int, deadline: float) -> bool:
        command = self._publication_event(TimerFired(generation, deadline, monotonic()))
        return command.action is PublicationAction.CHECK_INPUT

    def publication_capture_begin(self) -> None:
        self._compaction_interrupt.set()
        self._publication_event(CaptureBegin())

    def publication_capture_intent(self) -> None:
        """Interrupt compaction before readiness or a pending presence join."""
        self._compaction_interrupt.set()

    def publication_quiesced(self) -> None:
        self._publication_event(Quiesced())

    def publication_capture_end(self) -> bool:
        command = self._publication_event(CaptureEnd())
        self._compaction_interrupt = Event()
        return command.action is PublicationAction.CHECK_INPUT

    def publication_shutdown(self) -> None:
        self._compaction_interrupt.set()
        self._publication_event(Shutdown())

    def publication_input_changed(self) -> bool:
        command = self._publication_event(InputChanged())
        return command.action is PublicationAction.CHECK_INPUT

    def recover_and_publish(self) -> ReadyOutcome:
        """Replay native durable clock evidence and publish without a BLE connection."""
        if self._publication_root is None:
            return ReadyOutcome(ReadyOutcomeState.WAITING, reason="publication_unconfigured")
        return self.publish_ready()

    def inspect_recovery(self) -> tuple[bool, bool]:
        """Report sealed SQL prefixes and orphan-draft evidence under the lease."""
        with self.device_lock(operation="inspect_recovery"):
            closures = ready_closures.coalesce(self._state_db)
            closed_frontier = closures[-1].next_sequence if closures else None
            recoverable_prefix = any(
                source.lifecycle == "sealed_waiting"
                and (closed_frontier is None or source.sequence_start + source.committed_count > closed_frontier)
                for source in self._typed_attempt_sources(include_sealed=True)
            )
            frontier = self._cached_draft_frontier()
            unclosed_drafts = frontier is not None and (not closures or frontier > closures[-1].next_sequence)
            return recoverable_prefix, unclosed_drafts

    def append_ready_closure(
        self,
        next_sequence: int,
        reason: str,
        *,
        verified_drain: tuple[RingInfo, str] | None = None,
        completed_at: str | None = None,
    ) -> ready_closures.ReadyClosure:
        """Durably close one physical visit before allowing ready publication."""
        with self.device_lock(operation="ready_closure") as lease:
            return self._append_ready_closure_unlocked(
                lease, next_sequence, reason, verified_drain=verified_drain, completed_at=completed_at
            )

    def begin_ready_visit(self) -> ready_closures.ReadyClosure | None:
        """Fence an older drain permit before a new physical visit can run."""
        with self._publication_lock:
            needs_fence = self._publication_state.mode is PublicationMode.AVAILABLE
        if needs_fence:
            self.publication_capture_begin()
        active = self._filesystem._active_lease
        if active is not None:
            self._filesystem.require_device_lock(active)
            return ready_closures.begin_visit(self._state_db)
        with self.device_lock(operation="ready_visit_begin"):
            return ready_closures.begin_visit(self._state_db)

    def close_orphaned_drafts(
        self,
        reason: str,
        *,
        drain_cursor: int | None = None,
        verified_drain: tuple[RingInfo, str] | None = None,
        completed_at: str | None = None,
    ) -> ready_closures.ReadyClosure | None:
        """Create a restart closure for authenticated drafts without a pending attempt."""
        with self.device_lock(operation="close_orphaned_drafts") as lease:
            frontier = self._cached_draft_frontier()
            drain_completion = (
                (verified_drain, completed_at) if verified_drain is not None and completed_at is not None else None
            )
            return self._merge_recovered_closure_unlocked(lease, frontier, reason, drain_cursor, drain_completion)

    def _cached_draft_frontier(self) -> int | None:
        try:
            with (
                closing(self._state_db.open_connection(read_only=True)) as connection,
                self._state_db.access(connection),
            ):
                rows = cast(
                    list[tuple[object, object]],
                    _db_all(
                        connection.execute(
                            "SELECT sequence_start, committed_count FROM capture_source "
                            "WHERE source_type = 'draft' AND committed_count > 0"
                        )
                    ),
                )
        except (OSError, sqlite3.DatabaseError, StateDBError) as error:
            raise_if_sqlite_storage_fault(error)
            raise StagingError("typed draft frontier is unreadable") from error
        return max((int.from_bytes(cast(bytes, row[0]), "big") + cast(int, row[1]) for row in rows), default=None)

    def close_pending_prefix(
        self,
        reason: str,
        *,
        include_unpublished: bool = True,
        drain_cursor: int | None = None,
        verified_drain: tuple[RingInfo, str] | None = None,
        completed_at: str | None = None,
    ) -> ready_closures.ReadyClosure | None:
        """Seal the durable SQL prefix and append its recovered closure."""
        with self.device_lock(operation="close_pending_prefix") as lease:
            self._validate_recovery_cursor_unlocked(lease, reason, drain_cursor)
            sources = tuple(
                source
                for source in self._typed_attempt_sources(include_sealed=True)
                if include_unpublished or source.lifecycle == "sealed_waiting"
            )
            if not sources:
                return None
            orphan_frontier = self._cached_draft_frontier() if reason == "drained" else None
            frontiers = [source.sequence_start + source.committed_count for source in sources]
            if orphan_frontier is not None:
                frontiers.append(orphan_frontier)
            frontier = max(frontiers) if frontiers else None
            assert frontier is not None
            decision, existing = self._decide_recovered_closure_unlocked(lease, frontier, reason, drain_cursor)
            for source in sources:
                if source.lifecycle != "sealed_waiting":
                    self.terminalize_prefix_attempt_held(source.source_id, lease)
            if sources:
                self.publication_input_changed()
            if decision is None:
                if existing is not None:
                    self._complete_recovered_drain_unlocked(existing, verified_drain, completed_at)
                return existing
            return self._append_ready_closure_unlocked(
                lease, *decision, verified_drain=verified_drain, completed_at=completed_at
            )

    def _validate_recovery_cursor_unlocked(self, lease: DeviceLock, reason: str, drain_cursor: int | None) -> None:
        self._filesystem.require_device_lock(lease)
        closures = ready_closures.load(self._state_db)
        existing = closures[-1] if closures else None
        try:
            recovered_closure(
                existing=None if existing is None else (existing.next_sequence, existing.reason),
                recovered_frontier=None,
                reason=reason,
                drain_cursor=drain_cursor,
            )
        except ValueError as error:
            raise ready_closures.ReadyClosureError(str(error)) from error

    def _decide_recovered_closure_unlocked(
        self, lease: DeviceLock, frontier: int | None, reason: str, drain_cursor: int | None
    ) -> tuple[tuple[int, str] | None, ready_closures.ReadyClosure | None]:
        self._filesystem.require_device_lock(lease)
        closures = ready_closures.load(self._state_db)
        existing = closures[-1] if closures else None
        try:
            decision = recovered_closure(
                existing=None if existing is None else (existing.next_sequence, existing.reason),
                recovered_frontier=frontier,
                reason=reason,
                drain_cursor=drain_cursor,
            )
        except ValueError as error:
            raise ready_closures.ReadyClosureError(str(error)) from error
        closures = ready_closures.coalesce(self._state_db)
        existing = closures[-1] if closures else None
        return decision, existing

    def _merge_recovered_closure_unlocked(
        self,
        lease: DeviceLock,
        frontier: int | None,
        reason: str,
        drain_cursor: int | None,
        drain_completion: tuple[tuple[RingInfo, str], str] | None,
    ) -> ready_closures.ReadyClosure | None:
        verified_drain = None if drain_completion is None else drain_completion[0]
        completed_at = None if drain_completion is None else drain_completion[1]
        decision, existing = self._decide_recovered_closure_unlocked(lease, frontier, reason, drain_cursor)
        if decision is None:
            if existing is not None:
                self._complete_recovered_drain_unlocked(existing, verified_drain, completed_at)
            return existing if frontier is not None else None
        next_sequence, closure_reason = decision
        return self._append_ready_closure_unlocked(
            lease,
            next_sequence,
            closure_reason,
            verified_drain=verified_drain,
            completed_at=completed_at,
        )

    def _append_ready_closure_unlocked(
        self,
        lease: DeviceLock,
        next_sequence: int,
        reason: str,
        *,
        verified_drain: tuple[RingInfo, str] | None = None,
        completed_at: str | None = None,
    ) -> ready_closures.ReadyClosure:
        self._filesystem.require_device_lock(lease)
        callback: Callable[[StateTransaction], None] | None = None
        if reason == "drained" and verified_drain is not None and completed_at is not None:
            callback = partial(
                self._public_drain.complete_in_transaction,
                verified_drain=verified_drain,
                completed_at=completed_at,
                closure_frontier=next_sequence,
                accepted_custody=self._accepted_public_custody(verified_drain, next_sequence),
            )
        closure = ready_closures.append(self._state_db, next_sequence, reason, on_commit=callback)
        self._public_drain.wake()
        self._publication_event(InputChanged())
        return closure

    def _complete_recovered_drain_unlocked(
        self,
        closure: ready_closures.ReadyClosure,
        verified_drain: tuple[RingInfo, str] | None,
        completed_at: str | None,
    ) -> None:
        if closure.reason != "drained" or verified_drain is None or completed_at is None:
            return
        accepted_custody = self._accepted_public_custody(verified_drain, closure.next_sequence)
        with self._state_db.transaction() as transaction:
            self._public_drain.complete_in_transaction(
                transaction,
                verified_drain=verified_drain,
                completed_at=completed_at,
                closure_frontier=closure.next_sequence,
                accepted_custody=accepted_custody,
            )
        self._public_drain.wake()

    def _accepted_public_custody(
        self,
        verified_drain: tuple[RingInfo, str],
        closure_frontier: int,
    ) -> AcceptedDrainCustody | None:
        accepted_custody = self._accepted_drain_binding(closure_frontier)
        info, _observed_at = verified_drain
        if (
            accepted_custody is None
            or accepted_custody != self._public_drain.pending_binding()
            or accepted_custody[1] != closure_frontier
            or info.read_sequence != info.write_sequence
            or info.read_sequence <= closure_frontier
        ):
            return None if accepted_custody is None else AcceptedDrainCustody(*accepted_custody)
        matches = tuple(
            fact
            for fact in ConfirmedLossLedger(self._state_db).read()
            if fact.attempt_id == accepted_custody[0]
            and fact.start_sequence == closure_frontier
            and fact.end_sequence == info.read_sequence
        )
        confirmed_loss_end = info.read_sequence if len(matches) == 1 else None
        return AcceptedDrainCustody(*accepted_custody, confirmed_loss_end=confirmed_loss_end)

    def _accepted_drain_binding(self, closure_frontier: int) -> tuple[str, int] | None:
        binding = self._public_drain.pending_binding()
        if binding is None:
            return None
        source_id, durable_frontier = binding
        try:
            with InternalAudioStore(
                self._state_db,
                chunk_limit=self._config.writer.chunk_records,
            ) as audio_store:
                reader = SqlitePublicationSource(audio_store, source_id)
                admissions = _typed_source_admissions(audio_store, (reader,), closure_frontier)
        except InternalAudioStoreError, ReadyBundleError, StateDBError:
            return None
        if len(admissions) != 1:
            return None
        admitted = admissions[0]
        admitted_frontier = admitted.snapshot.sequence_start + admitted.admitted_count
        if admitted.snapshot.source_id != source_id or admitted_frontier != durable_frontier:
            return None
        return source_id, admitted_frontier

    def _typed_publication_source_ids(self) -> tuple[str, ...]:
        """List SQL sources that the canonical publisher can admit or resume."""
        try:
            with (
                closing(self._state_db.open_connection(read_only=True)) as connection,
                self._state_db.access(connection),
            ):
                rows = cast(
                    list[tuple[object]],
                    _db_all(
                        connection.execute(
                            "SELECT source_id FROM capture_source "
                            "WHERE lifecycle IN ('sealed_waiting','blocked') "
                            "ORDER BY sequence_start, source_id"
                        )
                    ),
                )
        except (OSError, sqlite3.DatabaseError, StateDBError) as error:
            raise_if_sqlite_storage_fault(error)
            raise ReadyBundleError("typed publication source inventory is unreadable") from error
        return tuple(str(row[0]) for row in rows)

    def _recover_and_publish_unlocked(self) -> ReadyOutcome:
        publication_root = self._publication_root
        if publication_root is None:
            return ReadyOutcome(ReadyOutcomeState.WAITING, reason="publication_unconfigured")
        checkpoint_path = publication_root.parent / "work" / "omi-ready-checkpoint.json"
        acknowledged = acknowledged_ready(checkpoint_path)
        try:
            from .clock_state_store import ClockStateStore

            segments = ClockStateStore(self._state_db).segment_map()
        except (ClockSegmentError, ValueError) as error:
            _LOGGER.warning("clock metadata unavailable; publishing ready audio without UTC normalization: %s", error)
            segments = ClockSegmentMap(())
        resume_retired(publication_root, self._state_db)
        closures = ready_closures.coalesce(self._state_db)
        closure = closures[-1] if closures else None
        frontier = None if closure is None else closure.next_sequence
        drained = closure is not None and closure.reason == "drained"
        try:
            with InternalAudioStore(
                self._state_db,
                chunk_limit=self._config.writer.chunk_records,
            ) as audio_store:
                sources = tuple(
                    SqlitePublicationSource(audio_store, source_id)
                    for source_id in self._typed_publication_source_ids()
                )
                outcome = finalize_typed_sources(
                    audio_store,
                    sources,
                    publication_root,
                    config=self._filesystem._ready,
                    clock_segments=segments,
                    frontier=frontier,
                    drained=drained,
                    page_limit=self._config.writer.chunk_records,
                    custody_cache=self._ready_custody_cache,
                )
                if outcome.custody_cache is not None:
                    self._ready_custody_cache = outcome.custody_cache
        except ReadyBundleError as error:
            outcome = ReadyOutcome(ReadyOutcomeState.BLOCKED, reason=str(error), remaining_at_frontier=True)
        if closure is not None and not outcome.remaining_at_frontier:
            ready_closures.remove(self._state_db, closure)
        retired = retire_acknowledged(publication_root, self._state_db, checkpoint_path, acknowledged=acknowledged)
        if retired and outcome.state is ReadyOutcomeState.WAITING:
            outcome = ReadyOutcome(ReadyOutcomeState.WAITING, reason="ack_retired")
        return outcome

    @property
    def capture_root(self) -> Path:
        return self._filesystem.capture_root

    @property
    def attempts_root(self) -> Path:
        return self._filesystem.attempts_root

    @property
    def state_db(self) -> StateDB:
        return self._state_db

    @property
    def paths(self) -> StagingPaths:
        """Return the currently validated storage authority."""
        return self._filesystem.paths

    def preflight_storage(self) -> None:
        """Validate and durably probe storage before any device operation starts."""
        self._filesystem.preflight_storage()
        if self._state_db.get(StateKind.CONFIRMED_LOSS, "ledger") is not None:
            ConfirmedLossLedger(self._state_db).read()
        elif self._state_db.get(StateKind.OPERATIONAL_STATUS, "current") is not None:
            raise ConfirmedLossError("confirmed-loss ledger is missing after status initialization")

    def require_capture_space(self, record_count: int) -> None:
        """Refresh the bounded SQLite growth and journal reserve before READ."""
        required, available = capture_space_requirement(
            self._state_db,
            record_count,
            self._config.writer.chunk_records,
        )
        raw_bytes = record_count * AUDIO_RECORD_SIZE
        safety = max(
            self._config.durability.staging_headroom_bytes,
            int(raw_bytes * self._config.durability.staging_overhead_fraction),
        )
        required += safety
        if available < required:
            raise DiskSpaceError(f"need {required} bytes of capture space; only {available} bytes available")

    def initialize_confirmed_loss_ledger(self) -> None:
        """Initialize only before the first operational snapshot exists."""
        with self.clock_mutation_lease():
            ConfirmedLossLedger(self._state_db).initialize(
                allow_create=self._state_db.get(StateKind.OPERATIONAL_STATUS, "current") is None
            )

    def record_confirmed_loss(
        self,
        attempt_id: str,
        start_sequence: int,
        end_sequence: int,
        occurred_at: str,
    ) -> str:
        """Durably record one confirmed half-open interval under the device lease."""
        with self.clock_mutation_lease():
            return ConfirmedLossLedger(self._state_db).record(
                attempt_id,
                start_sequence,
                end_sequence,
                occurred_at,
            )

    def quarantine_pending(self, reason: str) -> tuple[str, ...]:
        return quarantine.quarantine_pending(self._filesystem, reason)

    def quarantine_attempt_source(self, attempt_id: str) -> str:
        return quarantine.quarantine_attempt_source(
            self._filesystem,
            attempt_id,
        )

    def terminalize_prefix_attempt(self, attempt_id: str) -> None:
        quarantine.terminalize_prefix_attempt(
            self._filesystem,
            attempt_id,
        )

    def terminalize_prefix_attempt_held(self, attempt_id: str, lease: DeviceLock) -> None:
        """Terminalize a prefix under the writer's already-held device lease."""
        quarantine.terminalize_prefix_attempt_held(self._filesystem, attempt_id, lease)

    def quarantined_attempts(self, *, should_defer: Callable[[], bool] | None = None) -> tuple[str, ...]:
        return quarantine.quarantined_attempts(
            self._filesystem,
            should_defer=should_defer or _never_defer,
        )

    def quarantine_state(self, source: str) -> QuarantineState:
        return quarantine._quarantine_state(self._filesystem, source)

    def mark_quarantine_unprocessable(self, source: str, reason: str) -> None:
        quarantine.mark_quarantine_unprocessable(
            self._filesystem,
            source,
            reason,
        )

    def admit_quarantined_prefix(
        self, source: str, *, should_defer: Callable[[], bool] = _never_defer
    ) -> quarantine.QuarantinePrefixAdmission:
        """Admit a certified SQL prefix to the normal publication scheduler."""
        active_lease = self._filesystem._active_lease
        if active_lease is None:
            with self.device_lock(operation="admit_quarantined_prefix"):
                admitted = quarantine.admit_quarantined_prefix(
                    self._filesystem,
                    source,
                    should_defer=should_defer,
                    page_limit=self._config.writer.chunk_records,
                )
        else:
            self._filesystem.require_device_lock(active_lease)
            admitted = quarantine.admit_quarantined_prefix(
                self._filesystem,
                source,
                should_defer=should_defer,
                page_limit=self._config.writer.chunk_records,
            )
        self.publication_input_changed()
        return admitted

    def sweep_terminal_quarantine(self, *, should_defer: Callable[[], bool] | None = None) -> quarantine.SweepResult:
        generation = self._state_db.get_generation()
        result = quarantine.sweep_terminal_quarantine(
            self._filesystem,
            should_defer=should_defer or _never_defer,
            chunk_limit=self._config.writer.chunk_records,
        )
        if self._state_db.get_generation() != generation:
            self.publication_input_changed()
        return result

    def refresh_utc_projection(self) -> int:
        """Refresh one bounded chunk of queryable UTC metadata during idle maintenance."""

        return refresh_utc_projection(self._state_db, chunk_limit=self._config.writer.chunk_records)

    def assert_no_pending(self) -> None:
        quarantine.assert_no_pending(self._filesystem)

    def _typed_attempt_sources(self, *, include_sealed: bool = False) -> tuple[CaptureSource, ...]:
        """Read typed attempt snapshots without creating a persistent writer."""
        lifecycles = "('prepared','leg_prepared','receiving','interrupted'"
        if include_sealed:
            lifecycles += ",'sealed_waiting'"
        lifecycles += ")"
        try:
            with (
                closing(self._state_db.open_connection(read_only=True)) as connection,
                self._state_db.access(connection),
            ):
                rows = cast(
                    list[tuple[object]],
                    _db_all(
                        connection.execute(
                            "SELECT source_id FROM capture_source "
                            f"WHERE source_type = 'attempt' AND lifecycle IN {lifecycles} ORDER BY source_id"
                        )
                    ),
                )
        except (OSError, sqlite3.DatabaseError, StateDBError) as error:
            raise_if_sqlite_storage_fault(error)
            raise AttemptStateError("typed attempt inventory is unreadable") from error
        sources: list[CaptureSource] = []
        try:
            for row in rows:
                source = read_capture_source(self._state_db, str(row[0]))
                if source is not None:
                    sources.append(source)
        except InternalAudioStoreError as error:
            raise AttemptStateError("typed attempt inventory is unreadable") from error
        return tuple(sources)

    @staticmethod
    def _typed_descriptor(source: CaptureSource) -> AttemptDescriptor:
        return AttemptDescriptor(
            source.source_id,
            2,
            source.sequence_start,
            source.sequence_limit - source.sequence_start,
            read_begin_start=source.read_begin_start,
            read_begin_count=source.read_begin_count,
        )

    def prepare_streaming_attempt(self, start_sequence: int, packet_count: int) -> SqliteStagedAttempt:
        """Prepare a restart-safe typed source before issuing READ."""
        # Mirrors the app's buffered/full-read transfer model:
        # https://github.com/BasedHardware/omi/blob/6f7c57ac1545c1931c806a01605646405d398198/app/lib/services/wals/ring_storage_sync.dart#L545-L608
        _validate_int(start_sequence, "start_sequence")
        _validate_count(packet_count)
        source_id = uuid4().hex
        try:
            audio_store = InternalAudioStore(
                self._state_db,
                chunk_limit=self._config.writer.chunk_records,
                terminal_retention_seconds=self._config.staging_retention.terminal_retention_seconds,
            )
        except InternalAudioStoreError as error:
            raise AttemptStateError(str(error)) from error
        try:
            source = audio_store.create_source(source_id, "attempt", start_sequence, start_sequence + packet_count)
        except InternalAudioStoreError as error:
            audio_store.close()
            raise AttemptStateError(str(error)) from error
        return SqliteStagedAttempt(audio_store, source, self._typed_descriptor(source), close_store=True)

    def make_staging_writer(self, start: int, count: int) -> StagingWriterTargetPort:
        """Construct the sole writer target without exposing this concrete store to runtime composition."""
        from .staging_writer import StagingWriter

        return StagingWriter(self, start, count)

    def open_attempt(self, attempt_id: str) -> SqliteAttemptInspection:
        """Open one immutable SQL snapshot without creating a persistent writer."""
        _validate_attempt_id(attempt_id)
        try:
            typed = authenticate_capture_source(
                self._state_db,
                attempt_id,
                page_limit=self._config.writer.chunk_records,
            )
        except InternalAudioStoreError as error:
            raise AttemptStateError(str(error)) from error
        if typed is not None and typed.source_type == "attempt":
            return SqliteAttemptInspection(typed, self._typed_descriptor(typed))
        raise AttemptStateError("typed audio source does not exist")

    def resume_streaming_attempt(self, lease: StorageLeasePort) -> SqliteStagedAttempt | None:
        """Validate and reopen the unique streaming partial under an active lease.

        ``open_attempt`` is deliberately inspection-only.  This consuming seam
        requires the same process's device lease and is the only path that can
        promote complete raw tail records into the durable checkpoint.
        """
        if not isinstance(lease, DeviceLock):
            raise AttemptStateError("resume requires an active device lease")
        lease.require_active()
        if lease.filesystem is not self._filesystem:
            raise AttemptStateError("resume requires the active spool lock")
        typed_candidates = self._typed_attempt_sources()
        if len(typed_candidates) > 1:
            raise PendingAttemptError("multiple partial typed attempts block resume")
        if typed_candidates:
            source = typed_candidates[0]
            try:
                audio_store = InternalAudioStore(
                    self._state_db,
                    chunk_limit=self._config.writer.chunk_records,
                    terminal_retention_seconds=self._config.staging_retention.terminal_retention_seconds,
                )
            except InternalAudioStoreError as error:
                raise AttemptStateError(str(error)) from error
            try:
                if source.lifecycle == "receiving":
                    source = audio_store.interrupt(source.source_id)
            except InternalAudioStoreError as error:
                audio_store.close()
                raise AttemptStateError(str(error)) from error
            attempt = SqliteStagedAttempt(
                audio_store,
                source,
                self._typed_descriptor(source),
                close_store=True,
            )
            try:
                attempt.activate_for_resume(lease)
            except BaseException:
                attempt.close()
                raise
            return attempt
        return None

    def pending_attempts(self) -> tuple[AttemptDescriptor, ...]:
        typed = self._typed_attempt_sources()
        if typed:
            return tuple(self._typed_descriptor(source) for source in typed)
        return quarantine.pending_attempts(self._filesystem)

    @contextmanager
    def device_lock(
        self,
        *,
        operation: str = "unknown",
    ) -> Iterator[DeviceLock]:
        """Acquire the filesystem lease for one typed storage mutation."""
        with self._filesystem.device_lock(operation=operation) as lease:
            yield lease

    @contextmanager
    def clock_mutation_lease(self) -> Iterator[DeviceLock]:
        """Protect clock files with the active writer lease or a newly acquired store lease."""
        active = self._filesystem._active_lease
        if active is not None:
            self._filesystem.require_device_lock(active)
            yield active
            return
        with self.device_lock(operation="clock_mutation") as lease:
            yield lease

    def require_device_lock(self, lease: DeviceLock) -> None:
        """Reject consuming operations that are not protected by this active lease."""
        self._filesystem.require_device_lock(lease)
