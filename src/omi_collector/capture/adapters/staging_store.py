"""Sequencing facade for durable staging attempts."""

from __future__ import annotations

from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import asdict
from hashlib import sha256
from os import fsync, statvfs
from pathlib import Path
from threading import Lock
from typing import TYPE_CHECKING
from uuid import uuid4

from ...config import DEFAULT_CONFIG, CollectorConfig
from ..application.ports import StagingWriterTargetPort, StorageLeasePort
from . import publication, quarantine
from .attempts import StagedAttempt
from .clock_corrections import ClockCorrectionStore
from .clock_memberships import ClockMembershipStore
from .ready_bundles import finalize_drafts, retire_acknowledged
from .staging_contract import (
    _DESCRIPTOR_NAME,
    _RAW_NAME,
    AttemptDescriptor,
    AttemptStateError,
    PendingAttemptError,
    StreamingCheckpoint,
    _validate_attempt_id,
    _validate_count,
    _validate_int,
)
from .staging_filesystem import (
    DeviceLock,
    Fsync,
    StagingFilesystem,
    StagingPaths,
    Statvfs,
    _create_empty_synced,
    _never_defer,
    _require_regular_directory,
)

if TYPE_CHECKING:
    from .quarantine_publish import QuarantinePublication


class _PublicationAuthority:
    """Store-issued, lifecycle-bound publication capability.

    The store accepts only the exact instance it issued.  The capability may
    move between asyncio tasks, but cannot outlive the collector run that
    closes it.
    """

    __slots__ = ("_on_failure", "_store")

    def __init__(self, store: StagingStore, on_failure: Callable[[], None] | None) -> None:
        self._store = store
        self._on_failure = on_failure

    def publish(self) -> object | None:
        return self._store._publish_with_authority(self)

    def close(self) -> None:
        self._store._revoke_publication_authority(self)

    def schedule_retry(self) -> None:
        if self._on_failure is not None:
            self._on_failure()


class StagingStore:
    """Stage transport state in spool and publish bundles in capture_root."""

    def __init__(
        self,
        spool: Path,
        capture_root: Path,
        *,
        fsync_fn: Fsync = fsync,
        statvfs_fn: Statvfs = statvfs,
        config: CollectorConfig = DEFAULT_CONFIG,
    ) -> None:
        self._filesystem = StagingFilesystem(
            spool,
            capture_root,
            fsync_fn=fsync_fn,
            statvfs_fn=statvfs_fn,
            config=config,
        )
        self._validated_attempts: dict[str, StagedAttempt] = {}
        self._publication_root: Path | None = None
        self._publication_authority: _PublicationAuthority | None = None
        self._publication_authority_lease: DeviceLock | None = None
        self._publication_lock = Lock()

    @classmethod
    def from_paths(
        cls,
        paths: StagingPaths,
        *,
        config: CollectorConfig = DEFAULT_CONFIG,
        publication_root: Path | None = None,
    ) -> StagingStore:
        """Build a store from the external layout authority."""
        store = cls.__new__(cls)
        store._filesystem = StagingFilesystem.from_paths(paths, config=config)
        store._validated_attempts = {}
        store._publication_root = publication_root
        store._publication_authority = None
        store._publication_authority_lease = None
        store._publication_lock = Lock()
        return store

    def publish_ready(self, held_lease: DeviceLock | None = None) -> object | None:
        """Publish a complete normalized view when this store has an external boundary."""
        if not self._publication_lock.acquire(blocking=False):
            raise AttemptStateError("ready publication is already active")
        try:
            return self._publish_ready(held_lease)
        finally:
            self._publication_lock.release()

    def _publish_ready(self, held_lease: DeviceLock | None) -> object | None:
        if self._publication_root is None:
            return None
        if held_lease is None:
            with self.device_lock(recover_capture_temporaries=False, operation="ready_publication"):
                return self._recover_and_publish_unlocked()
        self._filesystem.require_device_lock(held_lease)
        return self._recover_and_publish_unlocked()

    def create_publication_authority(self, on_failure: Callable[[], None] | None = None) -> _PublicationAuthority:
        """Issue the sole capability allowed to publish during one collector run."""
        with self._publication_lock:
            if self._publication_authority is not None:
                raise AttemptStateError("publication authority is already active")
            authority = _PublicationAuthority(self, on_failure)
            self._publication_authority = authority
            self._publication_authority_lease = None
            return authority

    def _publish_with_authority(self, authority: _PublicationAuthority) -> object | None:
        """Publish under an issued capability, never under task-local identity."""
        if not self._publication_lock.acquire(blocking=False):
            raise AttemptStateError("ready publication is already active")
        try:
            if authority is not self._publication_authority:
                raise AttemptStateError("publication authority is revoked or was not issued by this store")
            try:
                lease = self._publication_authority_lease
                if lease is not None and lease._matches(self._filesystem):
                    return self._publish_ready(lease)
                return self._publish_ready(None)
            except Exception:
                authority.schedule_retry()
                raise
        finally:
            self._publication_lock.release()

    def notify_publication_failure(self) -> None:
        """Schedule the run's existing local retry after writer-side projection fails."""
        with self._publication_lock:
            authority = self._publication_authority
        if authority is not None:
            authority.schedule_retry()

    def _revoke_publication_authority(self, authority: _PublicationAuthority) -> None:
        with self._publication_lock:
            if authority is not self._publication_authority:
                raise AttemptStateError("publication authority is revoked or was not issued by this store")
            self._publication_authority = None
            self._publication_authority_lease = None

    def recover_and_publish(self) -> object | None:
        """Replay native durable clock evidence and publish without a BLE connection."""
        if self._publication_root is None:
            return None
        with self.device_lock(recover_capture_temporaries=False, operation="ready_publication"):
            return self._recover_and_publish_unlocked()

    def _recover_and_publish_unlocked(self) -> object | None:
        publication_root = self._publication_root
        if publication_root is None:
            return None
        corrections = ClockCorrectionStore(self.device_state_path)
        corrections.recover_prepared()
        corrections.reconcile_recovered_observations(
            near_zero_threshold=DEFAULT_CONFIG.telemetry.clock_drift_threshold_seconds
        )
        ledger_path = self.device_state_path.parent / "ready-publications.json"
        retire_acknowledged(
            publication_root,
            ledger_path,
            publication_root.parent / "work" / "omi-ready-checkpoint.json",
        )
        if not self._has_draft_bundles():
            return None
        segments = ClockMembershipStore(self.device_state_path).segments(corrections.observation_store.records())
        return finalize_drafts(
            self.capture_root,
            publication_root,
            ledger_path,
            segments,
        )

    def _has_draft_bundles(self) -> bool:
        try:
            return any(self.capture_root.iterdir())
        except FileNotFoundError:
            return False

    @property
    def capture_root(self) -> Path:
        return self._filesystem.capture_root

    def transfer_publication_authority(self, lease: DeviceLock) -> None:
        """Transfer this active writer lease to the run's publication capability."""
        self._filesystem.require_device_lock(lease)
        with self._publication_lock:
            bound = self._publication_authority_lease
            if self._publication_authority is None:
                return
            if bound is lease:
                raise AttemptStateError("publication authority was already transferred to this writer")
            if bound is not None and bound._matches(self._filesystem):
                raise AttemptStateError("publication authority is already bound to another active writer")
            self._publication_authority_lease = lease

    @property
    def attempts_root(self) -> Path:
        return self._filesystem.attempts_root

    @property
    def device_state_path(self) -> Path:
        return self._filesystem.device_state_path

    @property
    def clock_membership_store(self) -> ClockMembershipStore:
        return ClockMembershipStore(self.device_state_path)

    @property
    def paths(self) -> StagingPaths:
        """Return the currently validated storage authority."""
        return self._filesystem.paths

    def preflight_storage(self) -> None:
        """Validate and durably probe storage before any device operation starts."""
        self._filesystem.preflight_storage()

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

    def publish_quarantined_prefix(self, source: Path, *, should_defer: Callable[[], bool]) -> QuarantinePublication:
        """Salvage one quarantined prefix through this store's explicit paths capability."""
        from .quarantine_publish import publish_quarantined_prefix

        return publish_quarantined_prefix(source, self.paths, should_defer=should_defer)

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
        self._filesystem._write_json_atomic(attempt_path / _DESCRIPTOR_NAME, asdict(descriptor))
        _create_empty_synced(attempt_path / _RAW_NAME, self._filesystem._fsync)
        self._filesystem._write_checkpoint(
            attempt_path,
            StreamingCheckpoint(1, descriptor.attempt_id, 0, sha256(b"").hexdigest()),
            allow_missing=True,
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
