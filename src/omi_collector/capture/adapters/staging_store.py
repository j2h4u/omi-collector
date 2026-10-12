"""Sequencing facade for durable staging attempts."""

from __future__ import annotations

import errno
import os
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import asdict, dataclass, replace
from hashlib import sha256
from logging import getLogger
from pathlib import Path
from threading import Lock
from time import monotonic
from typing import TYPE_CHECKING, cast
from uuid import uuid4

from ..application.ports import StagingWriterTargetPort, StorageLeasePort
from ..domain.quarantine_machine import QuarantineState
from ..domain.ready_machine import (
    CaptureBegin,
    CaptureEnd,
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
from ..domain.ring_protocol import RECORD_SIZE
from . import publication, quarantine, ready_closures
from .attempts import StagedAttempt
from .clock_segments import ClockSegmentError, ClockSegmentMap
from .collector_state_db import StateDB, StateKind
from .confirmed_loss import ConfirmedLossError, ConfirmedLossLedger
from .debug_logging import debug_event
from .ready_bundles import (
    ReadyBundleError,
    ReadyInventory,
    ReadyOutcome,
    ReadyOutcomeState,
    authenticated_inventory,
    draft_frontier,
    finalize_drafts,
    resume_retired,
    retire_acknowledged,
    source_revision,
)
from .staging_contract import (
    _RAW_NAME,
    AttemptDescriptor,
    AttemptStateError,
    DeviceAlreadyRunningError,
    PendingAttemptError,
    StreamingCheckpoint,
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
    _create_empty_synced,
    _never_defer,
    _require_regular_directory,
    _sync_directory,
)

if TYPE_CHECKING:
    from .quarantine_publish import QuarantinePublication

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


def _publication_io_reason(error: OSError) -> str:
    if error.errno in {errno.EACCES, errno.EPERM}:
        return "publication_access_denied"
    return "publication_io_error"


@dataclass(frozen=True, slots=True)
class _FailedReadyInspection:
    revision: tuple[tuple[str, int, int, int, int, int], ...]
    error: ReadyBundleError


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
        self._filesystem = StagingFilesystem(
            spool,
            capture_root,
            state_db=state_db,
            io=io,
        )
        self._validated_attempts: dict[str, StagedAttempt] = {}
        self._publication_root: Path | None = None
        self._publication_lock = Lock()
        self._publication_state = PublicationState()
        self._ready_inspection: ReadyInventory | _FailedReadyInspection | None = None

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
        store._filesystem = StagingFilesystem.from_paths(paths, state_db=state_db, io=io)
        store._validated_attempts = {}
        store._publication_root = publication_root
        store._publication_lock = Lock()
        store._publication_state = PublicationState()
        store._ready_inspection = None
        return store

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
            probe_error: OSError | None = None
            try:
                revision: object = self._publication_revision()
            except OSError as error:
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
        return self._publication_outcome(command)

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
                with self.device_lock(recover_capture_temporaries=False, operation="ready_publication"):
                    outcome = self._recover_and_publish_unlocked()
                    phase = "post_effect_revision"
                    revision = self._publication_revision()
            else:
                self._filesystem.require_device_lock(held_lease)
                outcome = self._recover_and_publish_unlocked()
                phase = "post_effect_revision"
                revision = self._publication_revision()
            return outcome, revision
        except DeviceAlreadyRunningError as error:
            _debug_publication_failure(error, phase="device_lock", path=self._filesystem.lock_path)
            return ReadyOutcome(ReadyOutcomeState.TRANSIENT, reason="publication_lock_busy"), running.revision
        except OSError as error:
            _debug_publication_failure(error, phase=phase)
            return ReadyOutcome(ReadyOutcomeState.TRANSIENT, reason=_publication_io_reason(error)), running.revision
        except Exception as error:
            if not isinstance(error, ReadyBundleError):
                _LOGGER.exception("ready publication failed unexpectedly")
            try:
                revision = self._publication_revision()
            except OSError as revision_error:
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
            checked = self._execute_publication_command(next_command, held_lease)
            if outcome.state is ReadyOutcomeState.PUBLISHED and checked.state is ReadyOutcomeState.WAITING:
                return outcome
            return checked
        if next_command.action is PublicationAction.ARM:
            return self._publication_outcome(next_command)
        return outcome

    def _publication_revision(self) -> object:
        if self._publication_root is None:
            return ()
        checkpoint_path = self._publication_root.parent / "work" / "omi-ready-checkpoint.json"
        return (
            self._state_db.get_generation(),
            source_revision(self.capture_root, self._publication_root, extras=(checkpoint_path,)),
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
        with self._publication_lock:
            state = self._publication_state
            return state.mode is PublicationMode.AVAILABLE and state.needs_check

    def publication_wake_admitted(self) -> bool:
        with self._publication_lock:
            return self._publication_state.mode is PublicationMode.AVAILABLE

    def publication_timer_fired(self, generation: int, deadline: float) -> bool:
        command = self._publication_event(TimerFired(generation, deadline, monotonic()))
        return command.action is PublicationAction.CHECK_INPUT

    def publication_capture_begin(self) -> None:
        self._publication_event(CaptureBegin())

    def publication_quiesced(self) -> None:
        self._publication_event(Quiesced())

    def publication_capture_end(self) -> bool:
        command = self._publication_event(CaptureEnd())
        return command.action is PublicationAction.CHECK_INPUT

    def publication_shutdown(self) -> None:
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
        """Report prefix-marker and orphan-draft evidence under the device lease."""
        with self.device_lock(recover_capture_temporaries=False, operation="inspect_recovery"):
            recoverable_prefix = False
            if self.attempts_root.exists():
                _require_regular_directory(self.attempts_root)
                for path in self.attempts_root.iterdir():
                    if path.is_symlink() or not path.is_dir():
                        raise AttemptStateError("partial staging root contains a non-directory entry")
                    row = self._filesystem._read_attempt_row_if_valid_schema(path.name)
                    if row is None:
                        # Let pending-attempt inspection classify attributable
                        # corruption so startup can move it to quarantine while
                        # retaining the raw evidence.
                        continue
                    if row["terminal_retirement"] is not None:
                        continue
                    if row["prefix_publication"] is not None:
                        recoverable_prefix = True
                        break
            frontier = self._cached_draft_frontier()
            closures = ready_closures.coalesce(self._state_db)
            unclosed_drafts = frontier is not None and (not closures or frontier > closures[-1].next_sequence)
            return recoverable_prefix, unclosed_drafts

    def append_ready_closure(self, next_sequence: int, reason: str) -> ready_closures.ReadyClosure:
        """Durably close one physical visit before allowing ready publication."""
        with self.device_lock(recover_capture_temporaries=False, operation="ready_closure") as lease:
            return self._append_ready_closure_unlocked(lease, next_sequence, reason)

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
        with self.device_lock(recover_capture_temporaries=False, operation="ready_visit_begin"):
            return ready_closures.begin_visit(self._state_db)

    def close_orphaned_drafts(
        self, reason: str, *, drain_cursor: int | None = None
    ) -> ready_closures.ReadyClosure | None:
        """Create a restart closure for authenticated drafts without a pending attempt."""
        with self.device_lock(recover_capture_temporaries=False, operation="close_orphaned_drafts") as lease:
            frontier = self._cached_draft_frontier()
            return self._merge_recovered_closure_unlocked(lease, frontier, reason, drain_cursor)

    def _cached_draft_frontier(self) -> int | None:
        publication_root = self._publication_root
        if publication_root is None or not publication_root.is_dir():
            return draft_frontier(self.capture_root)
        inventory = self._authenticated_ready_inventory(publication_root)
        return max((draft.manifest.next_sequence for draft in inventory.drafts), default=None)

    def _authenticated_ready_inventory(self, publication_root: Path) -> ReadyInventory:
        """Reuse an authenticated view or the unchanged source-validation failure."""
        revision = source_revision(self.capture_root, publication_root)
        previous = self._ready_inspection
        if isinstance(previous, _FailedReadyInspection) and previous.revision == revision:
            raise previous.error.with_traceback(None)
        prior_inventory = previous if isinstance(previous, ReadyInventory) else None
        try:
            inventory = authenticated_inventory(self.capture_root, publication_root, prior_inventory)
        except ReadyBundleError as error:
            self._ready_inspection = _FailedReadyInspection(revision, error)
            raise
        self._ready_inspection = inventory
        return inventory

    def close_pending_prefix(
        self, reason: str, *, include_unpublished: bool = True, drain_cursor: int | None = None
    ) -> ready_closures.ReadyClosure | None:
        """Replay and close every published or resumable prefix under one lease."""
        with self.device_lock(recover_capture_temporaries=False, operation="close_pending_prefix") as lease:
            self._validate_recovery_cursor_unlocked(lease, reason, drain_cursor)
            candidates = self._pending_prefix_candidates(include_unpublished)
            drained_plan: tuple[tuple[int, str] | None, ready_closures.ReadyClosure | None] | None = None
            orphan_frontier = self._cached_draft_frontier() if reason == "drained" else None
            preflight_all = reason == "drained" and (len(candidates) > 1 or orphan_frontier is not None)
            if preflight_all:
                frontiers = [self._preflight_prefix_frontier(item[2]) for item in candidates]
                if orphan_frontier is not None:
                    frontiers.append(orphan_frontier)
                frontier = max(frontiers) if frontiers else None
                drained_plan = self._decide_recovered_closure_unlocked(lease, frontier, reason, drain_cursor)
            if not candidates:
                return None
            last_closure: ready_closures.ReadyClosure | None = None
            for _start_sequence, _path, descriptor in sorted(candidates, key=lambda item: item[0]):
                last_closure = self._close_prefix_candidate(
                    lease, descriptor, reason, drain_cursor, defer_drain_commit=preflight_all
                )
            if drained_plan is not None:
                decision, existing = drained_plan
                if decision is None:
                    return existing
                return self._append_ready_closure_unlocked(lease, *decision)
            return last_closure

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

    def _pending_prefix_candidates(self, include_unpublished: bool) -> list[tuple[int, Path, AttemptDescriptor]]:
        if not self.attempts_root.exists():
            return []
        _require_regular_directory(self.attempts_root)
        pending_ids = (
            {descriptor.attempt_id for descriptor in self.pending_attempts()} if include_unpublished else set()
        )
        candidates: list[tuple[int, Path, AttemptDescriptor]] = []
        for path in tuple(self.attempts_root.iterdir()):
            if path.is_symlink() or not path.is_dir():
                raise AttemptStateError("partial staging root contains a non-directory entry")
            row = self._filesystem._read_attempt_row(path.name)
            if row["terminal_retirement"] is not None:
                continue
            marker_present = row["prefix_publication"] is not None
            if not marker_present and path.name not in pending_ids:
                continue
            descriptor = self._filesystem._read_descriptor(path)
            candidates.append((descriptor.start_sequence, path, descriptor))
        return candidates

    def _close_prefix_candidate(
        self,
        lease: DeviceLock,
        descriptor: AttemptDescriptor,
        reason: str,
        drain_cursor: int | None,
        *,
        defer_drain_commit: bool,
    ) -> ready_closures.ReadyClosure | None:
        attempt = self.open_attempt(descriptor.attempt_id)
        try:
            attempt.activate_for_resume(lease)
            prefix = attempt.durable_prefix
            decision, existing = (None, None)
            if reason != "drained" or not defer_drain_commit:
                decision, existing = self._decide_recovered_closure_unlocked(
                    lease, prefix.next_sequence, reason, drain_cursor
                )
            attempt.publish_prefix()
            attempt.close(durable=True)
            self.terminalize_prefix_attempt_held(attempt.attempt_id, lease)
            if reason == "drained" and defer_drain_commit:
                return None
            if decision is None:
                return existing
            return self._append_ready_closure_unlocked(lease, *decision)
        finally:
            attempt.close(durable=True)

    def _preflight_prefix_frontier(self, descriptor: AttemptDescriptor) -> int:
        attempt = self.open_attempt(descriptor.attempt_id)
        try:
            recovery = attempt.recover()
            recovered_records = max(recovery.valid_records, recovery.raw_bytes // RECORD_SIZE)
            return descriptor.start_sequence + recovered_records
        finally:
            attempt.close()

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
        self, lease: DeviceLock, frontier: int | None, reason: str, drain_cursor: int | None
    ) -> ready_closures.ReadyClosure | None:
        decision, existing = self._decide_recovered_closure_unlocked(lease, frontier, reason, drain_cursor)
        if decision is None:
            return existing if frontier is not None else None
        next_sequence, closure_reason = decision
        return self._append_ready_closure_unlocked(lease, next_sequence, closure_reason)

    def _append_ready_closure_unlocked(
        self, lease: DeviceLock, next_sequence: int, reason: str
    ) -> ready_closures.ReadyClosure:
        self._filesystem.require_device_lock(lease)
        closure = ready_closures.append(self._state_db, next_sequence, reason)
        self._publication_event(InputChanged())
        return closure

    def _recover_and_publish_unlocked(self) -> ReadyOutcome:
        publication_root = self._publication_root
        if publication_root is None:
            return ReadyOutcome(ReadyOutcomeState.WAITING, reason="publication_unconfigured")
        checkpoint_path = publication_root.parent / "work" / "omi-ready-checkpoint.json"
        try:
            from .clock_state_store import ClockStateStore

            segments = ClockStateStore(self._state_db).segment_map()
        except (ClockSegmentError, ValueError) as error:
            _LOGGER.warning("clock metadata unavailable; publishing ready audio without UTC normalization: %s", error)
            segments = ClockSegmentMap(())
        resume_retired(publication_root, self._state_db)
        closures = ready_closures.coalesce(self._state_db)
        outcome = ReadyOutcome(ReadyOutcomeState.WAITING, reason="no_closed_frontier")
        if not closures:
            self._authenticated_ready_inventory(publication_root)
        else:
            closure = closures[-1]
            try:
                inventory = self._authenticated_ready_inventory(publication_root)
                outcome = finalize_drafts(
                    self.capture_root,
                    publication_root,
                    self._state_db,
                    segments,
                    config=self._filesystem._ready,
                    frontier=closure.next_sequence,
                    drained=closure.reason == "drained",
                    inventory=inventory,
                )
                self._ready_inspection = outcome.inventory
            except ReadyBundleError as error:
                outcome = ReadyOutcome(ReadyOutcomeState.BLOCKED, reason=str(error), remaining_at_frontier=True)
            if not outcome.remaining_at_frontier:
                inventory = self._authenticated_ready_inventory(publication_root)
                pending = any(draft.manifest.next_sequence <= closure.next_sequence for draft in inventory.drafts)
                if pending:
                    outcome = replace(outcome, remaining_at_frontier=True, inventory=inventory)
                else:
                    ready_closures.remove(self._state_db, closure)
        retired = retire_acknowledged(publication_root, self._state_db, checkpoint_path)
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

    def quarantine_pending(self, reason: str) -> tuple[Path, ...]:
        moved = quarantine.quarantine_pending(self._filesystem, reason)
        for attempt_id, attempt in tuple(self._validated_attempts.items()):
            if not attempt.path.exists():
                self._validated_attempts.pop(attempt_id, None)
                attempt.close()
        return moved

    def quarantine_attempt_source(self, attempt_id: str) -> Path:
        cached = self._validated_attempts.pop(attempt_id, None)
        if cached is not None:
            cached.close()
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

    def sweep_terminal_retired(self, *, should_defer: Callable[[], bool] | None = None) -> tuple[Path, ...]:
        return quarantine.sweep_terminal_retired(
            self._filesystem,
            should_defer=should_defer or _never_defer,
        )

    def quarantined_attempts(self, *, should_defer: Callable[[], bool] | None = None) -> tuple[Path, ...]:
        return quarantine.quarantined_attempts(
            self._filesystem,
            should_defer=should_defer or _never_defer,
        )

    def quarantine_state(self, source: Path) -> QuarantineState:
        return quarantine._quarantine_state(self._filesystem, source)

    def mark_quarantine_published(self, source: Path) -> None:
        quarantine.mark_quarantine_published(
            self._filesystem,
            source,
        )

    def mark_quarantine_unprocessable(self, source: Path, reason: str) -> None:
        quarantine.mark_quarantine_unprocessable(
            self._filesystem,
            source,
            reason,
        )

    def publish_quarantined_prefix(
        self, source: Path, *, should_defer: Callable[[], bool] = _never_defer
    ) -> QuarantinePublication:
        """Salvage one quarantined prefix through this store's explicit paths capability."""
        from .quarantine_publish import publish_quarantined_prefix

        active_lease = self._filesystem._active_lease
        if active_lease is None:
            with self.device_lock(recover_capture_temporaries=False, operation="publish_quarantined_prefix") as lease:
                published = publish_quarantined_prefix(source, self._filesystem, lease, should_defer=should_defer)
        else:
            published = publish_quarantined_prefix(source, self._filesystem, active_lease, should_defer=should_defer)
        self.publication_input_changed()
        return published

    def sweep_terminal_quarantine(self, *, should_defer: Callable[[], bool] | None = None) -> tuple[Path, ...]:
        return quarantine.sweep_terminal_quarantine(
            self._filesystem,
            should_defer=should_defer or _never_defer,
        )

    def assert_no_pending(self) -> None:
        quarantine.assert_no_pending(self._filesystem)

    def prepare_streaming_attempt(self, start_sequence: int, packet_count: int) -> StagedAttempt:
        """Prepare a restart-safe streaming attempt and its empty checkpoint."""
        # Mirrors the app's buffered/full-read transfer model:
        # https://github.com/BasedHardware/omi/blob/6f7c57ac1545c1931c806a01605646405d398198/app/lib/services/wals/ring_storage_sync.dart#L545-L608
        _validate_int(start_sequence, "start_sequence")
        _validate_count(packet_count)
        self._filesystem._preflight(packet_count)
        self._filesystem._ensure_directory(self.attempts_root)
        descriptor = AttemptDescriptor(
            uuid4().hex,
            2,
            start_sequence,
            packet_count,
        )
        attempt_path = self.attempts_root / descriptor.attempt_id
        self._filesystem._ensure_directory(attempt_path)
        _create_empty_synced(attempt_path / _RAW_NAME, self._filesystem._fsync)
        _sync_directory(attempt_path, self._filesystem._fsync)
        self._filesystem._create_attempt_row(
            descriptor.attempt_id,
            asdict(descriptor),
            asdict(StreamingCheckpoint(1, descriptor.attempt_id, 0, sha256(b"").hexdigest())),
        )
        return StagedAttempt(self._filesystem, attempt_path, descriptor)

    def make_staging_writer(self, start: int, count: int) -> StagingWriterTargetPort:
        """Construct the sole writer target without exposing this concrete store to runtime composition."""
        from .staging_writer import StagingWriter

        return StagingWriter(self, start, count)

    def open_attempt(self, attempt_id: str) -> StagedAttempt:
        """Open a valid persisted attempt without changing it."""
        _validate_attempt_id(attempt_id)
        attempt_path = self.attempts_root / attempt_id
        descriptor = self._filesystem._read_descriptor(attempt_path)
        _require_regular_directory(attempt_path)
        return StagedAttempt(self._filesystem, attempt_path, descriptor, live=False)

    def retain_validated_attempt(self, attempt_id: str, attempt: object) -> None:
        """Transfer ownership of a startup-hydrated attempt to the next lease."""
        if not isinstance(attempt, StagedAttempt):
            raise TypeError("validated attempt must be a StagedAttempt")
        previous = self._validated_attempts.pop(attempt_id, None)
        if previous is not None and previous is not attempt:
            previous.close()
        self._validated_attempts[attempt_id] = attempt

    def resume_streaming_attempt(self, lease: StorageLeasePort) -> StagedAttempt | None:
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
        candidates = self.pending_attempts()
        if len(candidates) > 1:
            raise PendingAttemptError("multiple partial attempts block resume")
        if not candidates:
            return None
        descriptor = candidates[0]
        path = self.attempts_root / descriptor.attempt_id
        _require_regular_directory(path)
        attempt = self._validated_attempts.pop(descriptor.attempt_id, None)
        if attempt is None:
            attempt = StagedAttempt(self._filesystem, path, descriptor, live=False)
        attempt.activate_for_resume(lease)
        return attempt

    def pending_attempts(self) -> tuple[AttemptDescriptor, ...]:
        return quarantine.pending_attempts(self._filesystem)

    @contextmanager
    def device_lock(
        self,
        *,
        recover_capture_temporaries: bool = True,
        operation: str = "unknown",
    ) -> Iterator[DeviceLock]:
        """Acquire the filesystem lease, then sequence publication recovery and quarantine."""
        with self._filesystem.device_lock(operation=operation) as lease:
            if recover_capture_temporaries:
                unsafe = publication.recover_capture_temporaries(self._filesystem)
                for temporary, capture_root, reason in unsafe:
                    quarantine.quarantine_capture_temporary(
                        self._filesystem,
                        temporary,
                        capture_root,
                        reason,
                    )
            yield lease

    @contextmanager
    def clock_mutation_lease(self) -> Iterator[DeviceLock]:
        """Protect clock files with the active writer lease or a newly acquired store lease."""
        active = self._filesystem._active_lease
        if active is not None:
            self._filesystem.require_device_lock(active)
            yield active
            return
        with self.device_lock(recover_capture_temporaries=False, operation="clock_mutation") as lease:
            yield lease

    def require_device_lock(self, lease: DeviceLock) -> None:
        """Reject consuming operations that are not protected by this active lease."""
        self._filesystem.require_device_lock(lease)
