"""Single-threaded target adapter for one streaming staging attempt.

``AttemptWriter`` owns a worker thread, but intentionally knows nothing about
the on-disk staging protocol.  This module is the narrow synchronous target
between the two: it acquires one device lease and then keeps the
``StagingStore`` and ``StagedAttempt`` behind methods that are called only by
that worker thread.

Construction is side-effect free.  The lease, partial attempt, and any
durability work are created by :meth:`prepare` (or :meth:`prepare_leg`) on the
owning thread.  The adapter does not introduce another persistence format or
copy the staging validation rules.
"""

from __future__ import annotations

import sys
from contextlib import AbstractContextManager
from pathlib import Path
from threading import get_ident
from typing import Protocol

from ..domain.ring_protocol import RECORD_SIZE, DoneNotification, ReadBeginNotification
from .attempts import RecordDisposition, StagedAttempt
from .publication import SealResult
from .staging_contract import AttemptDescriptor, DurablePrefix
from .staging_filesystem import DeviceLock
from .staging_store import StagingStore


class StagingWriterTarget(Protocol):
    """Synchronous target surface suitable for :class:`AttemptWriter`."""

    def prepare(self) -> AttemptDescriptor:
        """Acquire the device lease and prepare or resume the attempt."""
        ...

    def prepare_leg(self, start_sequence: int, packet_count: int) -> DurablePrefix:
        """Persist the initial or recovery-leg intent."""
        ...

    def read_begin(self, notice: ReadBeginNotification) -> None:
        """Persist the device's READ_BEGIN metadata."""
        ...

    def append_chunk(self, offset: int, chunk: memoryview) -> tuple[RecordDisposition, ...]:
        """Accept one aligned chunk from the shared transfer arena."""
        ...

    def checkpoint(self) -> DurablePrefix:
        """Return the staging layer's durable prefix."""
        ...

    def seal(self, done: DoneNotification) -> SealResult:
        """Validate and publish a completed attempt."""
        ...

    def publish_prefix(self) -> SealResult | None:
        """Publish the checkpoint-authenticated prefix as an ordinary bundle."""
        ...

    def close(self) -> None:
        """Flush and release the attempt and device lease."""
        ...


class StagingWriterError(RuntimeError):
    """Base error for an incorrectly used staging writer target."""


class ThreadAffinityError(StagingWriterError):
    """A target operation was called from a thread other than its owner."""


class StagingWriterStateError(StagingWriterError):
    """A target operation does not fit the adapter's lifecycle."""


class StagingWriter:
    """Own one streaming attempt and its device lease on one thread.

    The constructor only stores immutable transfer intent and constructs a
    ``StagingStore`` object.  It does not create directories, locks, or partial
    attempts.  ``prepare`` first resumes the unique streaming partial, if one
    exists, and otherwise creates a fresh streaming attempt.
    """

    def __init__(  # noqa: PLR0913
        self,
        root: Path | str | StagingStore,
        device_slug: str,
        start_sequence: int,
        packet_count: int,
        *,
        capture_root: Path | str | None = None,
        store: StagingStore | None = None,
    ) -> None:
        if store is not None:
            self._store = store
        elif isinstance(root, StagingStore):
            self._store = root
        else:
            if capture_root is None:
                raise TypeError("capture_root is required when constructing StagingWriter from a path")
            self._store = StagingStore(Path(root), Path(capture_root))
        self._device_slug = device_slug
        self._start_sequence = start_sequence
        self._packet_count = packet_count
        self._lease_context: AbstractContextManager[DeviceLock] | None = None
        self._lease: DeviceLock | None = None
        self._attempt: StagedAttempt | None = None
        self._owner_thread: int | None = None
        self._prepared = False
        self._read_started = False
        self._sealed = False
        self._closed = False
        self._active_start = start_sequence
        self._active_count = packet_count

    def prepare(self) -> AttemptDescriptor:
        """Acquire the lease and prepare or resume the streaming attempt."""
        self._enter("prepare")
        if self._closed:
            raise StagingWriterStateError("staging writer is closed")
        if self._prepared:
            assert self._attempt is not None
            return self._attempt.descriptor

        context = self._store.device_lock(self._device_slug)
        lease: DeviceLock | None = None
        try:
            lease = context.__enter__()
            attempt = self._store.resume_streaming_attempt(self._device_slug, lease)
            if attempt is None:
                attempt = self._store.prepare_streaming_attempt(
                    self._device_slug, self._start_sequence, self._packet_count
                )
        except BaseException:
            if lease is not None:
                context.__exit__(*sys.exc_info())
            raise
        self._lease_context = context
        self._lease = lease
        self._attempt = attempt
        self._prepared = True
        self._active_start = attempt.descriptor.start_sequence
        self._active_count = attempt.descriptor.packet_count
        return attempt.descriptor

    @property
    def attempt_id(self) -> str:
        """Return the prepared attempt identity for source quarantine."""
        self._require_prepared()
        assert self._attempt is not None
        return self._attempt.attempt_id

    def prepare_leg(self, start_sequence: int, packet_count: int) -> DurablePrefix:
        """Prepare an initial or in-process recovery range before READ."""
        self._enter("prepare_leg")
        if not self._prepared:
            self.prepare()
        self._require_prepared()
        self._validate_leg(start_sequence, packet_count)
        assert self._attempt is not None
        prefix = self._attempt.prepare_leg(start_sequence, packet_count)
        self._active_start = start_sequence
        self._active_count = packet_count
        self._read_started = False
        return prefix

    def begin_recovery(self, start_sequence: int, packet_count: int) -> DurablePrefix:
        """Persist recovery intent before accepting a recovery READ."""
        self._enter("begin_recovery")
        self._require_prepared()
        self._validate_leg(start_sequence, packet_count)
        assert self._attempt is not None
        prefix = self._attempt.begin_recovery(start_sequence, packet_count)
        self._active_start = start_sequence
        self._active_count = packet_count
        self._read_started = False
        return prefix

    def read_begin(self, notice: ReadBeginNotification) -> None:
        """Persist READ_BEGIN using the notification's existing typed fields."""
        self._enter("read_begin")
        self._require_ready_for_read()
        if not isinstance(notice, ReadBeginNotification):
            raise TypeError("notice must be a ReadBeginNotification")
        assert self._attempt is not None
        self._require_lease()
        notice_range = (notice.transfer_start_sequence, notice.packet_count)
        descriptor = self._attempt.descriptor
        if self._read_started and notice_range == (
            descriptor.read_begin_start,
            descriptor.read_begin_count,
        ):
            self._attempt.record_read_begin(notice)
            return
        if (notice.transfer_start_sequence, notice.packet_count) != (
            self._active_start,
            self._active_count,
        ):
            raise StagingWriterStateError("READ_BEGIN does not match the prepared leg")
        self._attempt.record_read_begin(notice)
        self._read_started = True

    def append_chunk(self, offset: int, chunk: memoryview) -> tuple[RecordDisposition, ...]:
        """Map an arena-relative byte offset to the exact ring sequence."""
        self._enter("append_chunk")
        self._require_ready_for_data()
        if isinstance(offset, bool) or not isinstance(offset, int):
            raise TypeError("offset must be an integer")
        if offset < 0 or offset % RECORD_SIZE:
            raise ValueError(f"offset must be a non-negative multiple of {RECORD_SIZE}")
        if not isinstance(chunk, memoryview):
            raise TypeError("chunk must be a memoryview")
        if not chunk.c_contiguous:
            raise ValueError("chunk must be C-contiguous")
        byte_count = chunk.nbytes
        if byte_count <= 0 or byte_count % RECORD_SIZE:
            raise ValueError(f"chunk must be a positive multiple of {RECORD_SIZE} bytes")
        if offset + byte_count > self._active_count * RECORD_SIZE:
            raise ValueError("chunk exceeds the prepared READ leg")
        assert self._attempt is not None
        self._require_lease()
        sequence = self._active_start + offset // RECORD_SIZE
        return self._attempt.accept_chunk(sequence, chunk)

    def accept_record(self, sequence: int, record: bytes) -> RecordDisposition:
        """Compatibility seam for the existing record-oriented read helper."""
        self._enter("accept_record")
        self._require_ready_for_data()
        if sequence < self._active_start or sequence >= self._active_start + self._active_count:
            raise ValueError("record sequence is outside the prepared READ leg")
        if len(record) != RECORD_SIZE:
            raise ValueError(f"record must be exactly {RECORD_SIZE} bytes")
        assert self._attempt is not None
        self._require_lease()
        return self._attempt.accept_record(sequence, record)

    def checkpoint(self) -> DurablePrefix:
        """Checkpoint through the existing staging API."""
        self._enter("checkpoint")
        self._require_ready_for_data()
        assert self._attempt is not None
        self._require_lease()
        return self._attempt.checkpoint()

    def seal(self, done: DoneNotification) -> SealResult:
        """Validate and publish through the existing staging API."""
        self._enter("seal")
        self._require_ready_for_data()
        if not isinstance(done, DoneNotification):
            raise TypeError("done must be a DoneNotification")
        if self._sealed:
            raise StagingWriterStateError("staging writer is already sealed")
        assert self._attempt is not None
        self._require_lease()
        result = self._attempt.seal(done)
        self._sealed = True
        return result

    def publish_prefix(self) -> SealResult | None:
        """Publish the checkpoint-authenticated prefix through staging."""
        self._enter("publish_prefix")
        self._require_ready_for_data()
        assert self._attempt is not None
        self._require_lease()
        result = self._attempt.publish_prefix()
        self._sealed = True
        return result

    def close(self) -> None:
        """Durably close the attempt, then release its device lease."""
        self._enter("close")
        if self._closed:
            return
        error: BaseException | None = None
        try:
            if self._attempt is not None:
                self._require_lease()
                self._attempt.close(durable=True)
        except BaseException as caught:  # noqa: BLE001 - release must run for every failure
            error = caught
        finally:
            context = self._lease_context
            self._lease_context = None
            self._lease = None
            self._closed = True
            if context is not None:
                try:
                    if error is None:
                        context.__exit__(None, None, None)
                    else:
                        context.__exit__(type(error), error, error.__traceback__)
                except BaseException as release_error:  # noqa: BLE001 - preserve the close failure
                    if error is None:
                        error = release_error
        if error is not None:
            raise error

    def _enter(self, operation: str) -> None:
        del operation
        current = get_ident()
        if self._owner_thread is None:
            self._owner_thread = current
        elif self._owner_thread != current:
            raise ThreadAffinityError("staging writer target is owned by another thread")

    def _require_prepared(self) -> None:
        if self._closed:
            raise StagingWriterStateError("staging writer is closed")
        if not self._prepared or self._attempt is None or self._lease is None:
            raise StagingWriterStateError("staging writer has not been prepared")

    def _require_ready_for_read(self) -> None:
        self._require_prepared()
        if self._sealed:
            raise StagingWriterStateError("staging writer is sealed")

    def _require_ready_for_data(self) -> None:
        self._require_ready_for_read()
        if not self._read_started:
            raise StagingWriterStateError("READ_BEGIN has not been recorded")

    def _require_lease(self) -> None:
        if self._lease is None:
            raise StagingWriterStateError("staging writer has no active device lease")
        self._lease.require_active()

    def _validate_leg(self, start_sequence: int, packet_count: int) -> None:
        if isinstance(start_sequence, bool) or not isinstance(start_sequence, int) or start_sequence < 0:
            raise ValueError("start_sequence must be a non-negative integer")
        if isinstance(packet_count, bool) or not isinstance(packet_count, int) or packet_count <= 0:
            raise ValueError("packet_count must be a positive integer")
