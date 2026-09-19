"""Sequencing facade for durable staging attempts."""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import asdict
from hashlib import sha256
from os import fsync, statvfs
from pathlib import Path
from threading import get_ident
from uuid import uuid4

from ...config import DEFAULT_CONFIG, CollectorConfig
from . import publication, quarantine
from .attempts import StagedAttempt
from .recovery import Recovery
from .staging_contract import (
    _DESCRIPTOR_NAME,
    _RAW_NAME,
    AttemptDescriptor,
    AttemptStateError,
    PendingAttemptError,
    StagingError,
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
    _file_size,
    _never_defer,
    _require_regular_directory,
)


def _lease_owner_token() -> tuple[int, int | None]:
    try:
        task = asyncio.current_task()
    except RuntimeError:
        task = None
    return get_ident(), id(task) if task is not None else None


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
        self._lease_owner: tuple[int, int | None] | None = None
        self._lease_handoffs: dict[object, tuple[tuple[int, int | None], DeviceLock | None, bool]] = {}

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
        store._lease_owner = None
        store._lease_handoffs = {}
        return store

    def publish_timeline(self, held_lease: DeviceLock | None = None) -> object | None:
        """Publish a complete normalized view when this store has an external boundary."""
        if self._publication_root is None:
            return None
        if not self._has_captured_bundles():
            return None
        if held_lease is None:
            with self.device_lock(recover_capture_temporaries=False):
                return self._recover_and_publish_unlocked()
        self._filesystem.require_device_lock(held_lease)
        return self._recover_and_publish_unlocked()

    def publish_timeline_with_handoff(self, token: object) -> object | None:
        """Publish through an explicit operation-scoped lease handoff."""
        if self._publication_root is None or not self._has_captured_bundles():
            return None
        with self.handoff_device_lease(token):
            return self._recover_and_publish_unlocked()

    def recover_and_publish(self, entries: object = (), *, apply: bool = True) -> object | None:
        """Replay durable clock evidence and publish without a BLE connection."""
        if self._publication_root is None:
            return None
        with self.device_lock(recover_capture_temporaries=False):
            return self._recover_and_publish_unlocked(entries, apply=apply)

    def _recover_and_publish_unlocked(self, entries: object = (), *, apply: bool = True) -> object:
        from .clock_recovery import HistoricalClockImporter

        publication_root = self._publication_root
        if publication_root is None:
            return None
        decisions = HistoricalClockImporter(self.device_state_path, self.capture_root).recover(
            entries,
            apply=apply,
            dry_run=not apply,
        )
        if not self._has_captured_bundles():
            return decisions
        from .timeline_generations import publish_from_ledger

        return publish_from_ledger(self.capture_root, publication_root, self.device_state_path.parent)

    def _has_captured_bundles(self) -> bool:
        try:
            return any(self.capture_root.iterdir())
        except FileNotFoundError:
            return False

    def recover_clock(self, entries: object = (), *, apply: bool = False) -> tuple[object, ...]:
        """Validate/apply clock evidence while holding the collector lease."""
        if self._publication_root is None:
            return ()
        publication_root = self._publication_root
        with self.device_lock(recover_capture_temporaries=False):
            from .clock_recovery import HistoricalClockImporter

            importer = HistoricalClockImporter(self.device_state_path, self.capture_root)
            decisions = importer.recover(entries, apply=apply, dry_run=not apply)
            if apply and self._has_captured_bundles():
                from .timeline_generations import publish_from_ledger

                publish_from_ledger(self.capture_root, publication_root, self.device_state_path.parent)
            return decisions

    @property
    def capture_root(self) -> Path:
        return self._filesystem.capture_root

    @property
    def active_device_lease(self) -> DeviceLock | None:
        """Return a lease only to the task/thread that acquired it."""
        lease = self._filesystem._active_lease
        if lease is None or not lease._matches(self._filesystem) or self._lease_owner != _lease_owner_token():
            return None
        return lease

    @property
    def held_device_lease(self) -> DeviceLock | None:
        """Return only a lease owned by this task/thread."""
        return self.active_device_lease

    def create_lease_handoff(self) -> object:
        """Issue an operation-scoped capability for an explicit lease handoff."""
        token = object()
        owner = _lease_owner_token()
        active_lease = self._filesystem._active_lease
        lease = self.active_device_lease
        self._lease_handoffs[token] = (owner, lease, active_lease is None)
        return token

    def _bind_pending_handoff(self) -> None:
        """Bind the sole unbound operation handoff to this writer's active lease."""
        active_lease = self._filesystem._active_lease
        if active_lease is None:
            return
        candidates = tuple(
            token for token, (_owner, lease, can_rebind) in self._lease_handoffs.items() if lease is None and can_rebind
        )
        if len(candidates) != 1:
            return
        token = candidates[0]
        owner, _lease, _can_rebind = self._lease_handoffs[token]
        self._lease_handoffs[token] = (owner, active_lease, False)

    def release_lease_handoff(self, token: object) -> None:
        """Revoke an operation-scoped lease handoff capability."""
        self._lease_handoffs.pop(token, None)

    @contextmanager
    def handoff_device_lease(self, token: object) -> Iterator[DeviceLock]:
        """Use an explicitly issued operation-scoped lease handoff."""
        owner = _lease_owner_token()
        handoff = self._lease_handoffs.get(token)
        if handoff is None or handoff[0] != owner:
            raise AttemptStateError("lease handoff belongs to another operation owner")
        bound_lease = handoff[1]
        active_lease = self._filesystem._active_lease
        if bound_lease is not None and active_lease is not bound_lease:
            self._lease_handoffs[token] = (owner, None, True)
            bound_lease = None
        if bound_lease is not None and active_lease is bound_lease and bound_lease._matches(self._filesystem):
            self._filesystem.require_device_lock(bound_lease)
            yield bound_lease
            return
        with self.device_lock(recover_capture_temporaries=False) as acquired:
            self._lease_handoffs[token] = (owner, acquired, False)
            try:
                yield acquired
            finally:
                if self._lease_handoffs.get(token) == (owner, acquired, False):
                    self._lease_handoffs[token] = (owner, None, True)

    @contextmanager
    def held_device_lock(self, lease: DeviceLock) -> Iterator[DeviceLock]:
        """Use an explicitly supplied lease without taking the lock again."""
        self._filesystem.require_device_lock(lease)
        yield lease

    @property
    def attempts_root(self) -> Path:
        return self._filesystem.attempts_root

    @property
    def device_state_path(self) -> Path:
        return self._filesystem.device_state_path

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
        self._bind_pending_handoff()
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

    def open_attempt(self, attempt_id: str) -> StagedAttempt:
        """Open a valid persisted attempt without changing it."""
        _validate_attempt_id(attempt_id)
        attempt_path = self.attempts_root / attempt_id
        descriptor = self._filesystem._read_descriptor(attempt_path)
        _require_regular_directory(attempt_path)
        return StagedAttempt(self._filesystem, attempt_path, descriptor, live=False)

    def open_attempt_for_resume(self, attempt_id: str) -> StagedAttempt:
        """Hydrate a pending attempt once for startup validation and reuse."""
        _validate_attempt_id(attempt_id)
        attempt_path = self.attempts_root / attempt_id
        descriptor = self._filesystem._read_descriptor(attempt_path)
        _require_regular_directory(attempt_path)
        # This is still inspection.  Promotion is performed only by
        # ``activate_for_resume`` after the device lease is held.
        return StagedAttempt(self._filesystem, attempt_path, descriptor, live=False)

    def retain_validated_attempt(self, attempt_id: str, attempt: object) -> None:
        """Transfer ownership of a startup-hydrated attempt to the next lease."""
        if not isinstance(attempt, StagedAttempt):
            raise TypeError("validated attempt must be a StagedAttempt")
        previous = self._validated_attempts.pop(attempt_id, None)
        if previous is not None and previous is not attempt:
            previous.close()
        self._validated_attempts[attempt_id] = attempt

    def resume_streaming_attempt(self, lease: DeviceLock) -> StagedAttempt | None:
        """Validate and reopen the unique streaming partial under an active lease.

        ``open_attempt`` is deliberately inspection-only.  This consuming seam
        requires the same process's device lease and is the only path that can
        promote complete raw tail records into the durable checkpoint.
        """
        lease.require_active()
        if lease.filesystem is not self._filesystem:
            raise AttemptStateError("resume requires the active spool lock")
        self._bind_pending_handoff()
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

    def recover_attempt(self, attempt_id: str) -> Recovery:
        """Inspect an attempt without deleting or truncating any evidence."""
        _validate_attempt_id(attempt_id)
        attempt_path = self.attempts_root / attempt_id
        try:
            return self.open_attempt(attempt_id).recover()
        except StagingError as error:
            return Recovery(attempt_id, 0, _file_size(attempt_path / _RAW_NAME), False, str(error))

    def pending_attempts(self) -> tuple[AttemptDescriptor, ...]:
        return quarantine.pending_attempts(self._filesystem)

    @contextmanager
    def device_lock(
        self,
        *,
        recover_capture_temporaries: bool = True,
    ) -> Iterator[DeviceLock]:
        """Acquire the filesystem lease, then sequence publication recovery and quarantine."""
        with self._filesystem.device_lock() as lease:
            self._lease_owner = _lease_owner_token()
            try:
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
            finally:
                self._lease_owner = None

    def require_device_lock(self, lease: DeviceLock) -> None:
        """Reject consuming operations that are not protected by this active lease."""
        self._filesystem.require_device_lock(lease)
