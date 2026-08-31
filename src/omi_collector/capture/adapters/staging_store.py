"""Sequencing facade for durable staging attempts."""

from __future__ import annotations

from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import asdict
from hashlib import sha256
from os import fsync, statvfs
from pathlib import Path
from uuid import uuid4

from ...config import DEFAULT_CONFIG, CollectorConfig
from . import publication, quarantine
from .attempts import StagedAttempt
from .recovery import Recovery
from .staging_contract import (
    _COMMITS_NAME,
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
    _validate_slug,
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

    @classmethod
    def from_paths(cls, paths: StagingPaths, *, config: CollectorConfig = DEFAULT_CONFIG) -> StagingStore:
        """Build a store from the external layout authority."""
        store = cls.__new__(cls)
        store._filesystem = StagingFilesystem.from_paths(paths, config=config)
        return store

    @property
    def capture_root(self) -> Path:
        return self._filesystem.capture_root

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

    def quarantine_pending(self, device_slug: str, reason: str) -> tuple[Path, ...]:
        return quarantine.quarantine_pending(
            self._filesystem,
            device_slug,
            reason,
        )

    def quarantine_attempt_source(self, device_slug: str, attempt_id: str) -> Path:
        return quarantine.quarantine_attempt_source(
            self._filesystem,
            device_slug,
            attempt_id,
        )

    def terminalize_prefix_attempt(self, device_slug: str, attempt_id: str) -> None:
        quarantine.terminalize_prefix_attempt(
            self._filesystem,
            device_slug,
            attempt_id,
        )

    def sweep_terminal_retired(
        self, device_slug: str, *, should_defer: Callable[[], bool] | None = None
    ) -> tuple[Path, ...]:
        return quarantine.sweep_terminal_retired(
            self._filesystem,
            device_slug,
            should_defer=should_defer or _never_defer,
        )

    def quarantined_attempts(
        self, device_slug: str, *, should_defer: Callable[[], bool] | None = None
    ) -> tuple[Path, ...]:
        return quarantine.quarantined_attempts(
            self._filesystem,
            device_slug,
            should_defer=should_defer or _never_defer,
        )

    def mark_quarantine_published(self, device_slug: str, source: Path) -> None:
        quarantine.mark_quarantine_published(
            self._filesystem,
            device_slug,
            source,
        )

    def mark_quarantine_unprocessable(self, device_slug: str, source: Path, reason: str) -> None:
        quarantine.mark_quarantine_unprocessable(
            self._filesystem,
            device_slug,
            source,
            reason,
        )

    def mark_quarantine_salvage_pending(self, device_slug: str, source: Path, reason: str) -> None:
        quarantine.mark_quarantine_salvage_pending(
            self._filesystem,
            device_slug,
            source,
            reason,
        )

    def sweep_terminal_quarantine(
        self, device_slug: str, *, should_defer: Callable[[], bool] | None = None
    ) -> tuple[Path, ...]:
        return quarantine.sweep_terminal_quarantine(
            self._filesystem,
            device_slug,
            should_defer=should_defer or _never_defer,
        )

    def assert_no_pending(self, device_slug: str) -> None:
        quarantine.assert_no_pending(
            self._filesystem,
            device_slug,
        )

    def prepare_attempt(self, device_slug: str, start_sequence: int, packet_count: int) -> StagedAttempt:
        """Persist an empty attempt and return only after its descriptor is durable."""
        _validate_int(start_sequence, "start_sequence")
        _validate_count(packet_count)
        _validate_slug(device_slug)
        self._filesystem._preflight(packet_count, device_slug)
        self._filesystem._ensure_directory(self.attempts_root)
        descriptor = AttemptDescriptor(uuid4().hex, device_slug, start_sequence, packet_count)
        attempt_path = self.attempts_root / descriptor.attempt_id
        self._filesystem._ensure_directory(attempt_path)
        self._filesystem._write_json_atomic(attempt_path / _DESCRIPTOR_NAME, asdict(descriptor))
        return StagedAttempt(self._filesystem, attempt_path, descriptor)

    def prepare_streaming_attempt(self, device_slug: str, start_sequence: int, packet_count: int) -> StagedAttempt:
        """Prepare a restart-safe streaming attempt and its empty checkpoint."""
        # Mirrors the app's buffered/full-read transfer model:
        # https://github.com/BasedHardware/omi/blob/6f7c57ac1545c1931c806a01605646405d398198/app/lib/services/wals/ring_storage_sync.dart#L545-L608
        _validate_int(start_sequence, "start_sequence")
        _validate_count(packet_count)
        _validate_slug(device_slug)
        self._filesystem._preflight(packet_count, device_slug)
        self._filesystem._ensure_directory(self.attempts_root)
        descriptor = AttemptDescriptor(
            uuid4().hex,
            device_slug,
            start_sequence,
            packet_count,
            mode="streaming",
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

    def resume_streaming_attempt(self, device_slug: str, lease: DeviceLock) -> StagedAttempt | None:
        """Validate and reopen the unique streaming partial under an active lease.

        ``open_attempt`` is deliberately inspection-only.  This consuming seam
        requires the same process's device lease and is the only path that can
        promote complete raw tail records into the durable checkpoint.
        """
        lease.require_active()
        if lease.filesystem is not self._filesystem or lease.device_slug != device_slug:
            raise AttemptStateError("resume requires this device's active spool lock")
        candidates = self.pending_attempts(device_slug)
        if len(candidates) > 1:
            raise PendingAttemptError(f"multiple partial attempts block resume for {device_slug}")
        if not candidates:
            return None
        descriptor = candidates[0]
        if descriptor.mode != "streaming":
            raise PendingAttemptError("non-streaming partial evidence cannot be resumed")
        path = self.attempts_root / descriptor.attempt_id
        _require_regular_directory(path)
        attempt = StagedAttempt(self._filesystem, path, descriptor, live=True, resume=True)
        attempt._resume_lease = lease
        return attempt

    def recover_attempt(self, attempt_id: str) -> Recovery:
        """Inspect an attempt without deleting or truncating any evidence."""
        _validate_attempt_id(attempt_id)
        attempt_path = self.attempts_root / attempt_id
        try:
            return self.open_attempt(attempt_id).recover()
        except StagingError as error:
            return Recovery(
                attempt_id,
                0,
                _file_size(attempt_path / _RAW_NAME),
                _file_size(attempt_path / _COMMITS_NAME),
                False,
                str(error),
            )

    def pending_attempts(self, device_slug: str) -> tuple[AttemptDescriptor, ...]:
        return quarantine.pending_attempts(self._filesystem, device_slug)

    @contextmanager
    def device_lock(
        self,
        device_slug: str,
        *,
        recover_capture_temporaries: bool = True,
    ) -> Iterator[DeviceLock]:
        """Acquire the filesystem lease, then sequence publication recovery and quarantine."""
        _validate_slug(device_slug)
        with self._filesystem.device_lock(device_slug) as lease:
            if recover_capture_temporaries:
                unsafe = publication.recover_capture_temporaries(self._filesystem, device_slug)
                for temporary, device_root, reason in unsafe:
                    quarantine.quarantine_capture_temporary(
                        self._filesystem,
                        temporary,
                        device_root,
                        device_slug,
                        reason,
                    )
            yield lease

    def require_device_lock(self, device_slug: str, lease: DeviceLock) -> None:
        """Reject consuming operations that are not protected by this active lease."""
        self._filesystem.require_device_lock(device_slug, lease)
