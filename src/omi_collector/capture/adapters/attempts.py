"""Typed SQL attempt sequencing and read-only startup inspection."""

from __future__ import annotations

from dataclasses import dataclass, replace

from ..domain.ring_protocol import DoneNotification, ReadBeginNotification
from .internal_audio_store import (
    AUDIO_RECORD_SIZE,
    CaptureRecord,
    CaptureSource,
    CaptureTail,
    DrainCheckpoint,
    InternalAudioStore,
    InternalAudioStoreError,
)
from .recovery import Recovery
from .staging_contract import AttemptDescriptor, AttemptStateError, DurablePrefix
from .staging_filesystem import DeviceLock


class RecordAcceptanceError(AttemptStateError):
    """A recovery record cannot be reconciled with the durable prefix."""


class RecordMismatchError(RecordAcceptanceError):
    """A replayed sequence contains bytes different from the durable prefix."""


class RecordGapError(RecordAcceptanceError):
    """A recovery record skips over the next sequence that can be appended."""


class RecordRegressionError(RecordAcceptanceError):
    """A recovery record precedes the original snapshot or requested range."""


@dataclass(frozen=True, slots=True)
class AudioSourcePage:
    """One bounded page paired with the source snapshot it belongs to."""

    source: CaptureSource
    after_ordinal: int
    records: tuple[CaptureRecord, ...]


@dataclass(frozen=True, slots=True)
class DurableSqliteSeal:
    """Exact internal custody established before any device ADVANCE."""

    source_id: str
    start_sequence: int
    next_sequence: int
    record_count: int
    raw_sha256: str


class SqliteAttemptInspection:
    """Read-only startup view that never creates a persistent SQL writer."""

    def __init__(self, source: CaptureSource, descriptor: AttemptDescriptor) -> None:
        self._source = source
        self.descriptor = descriptor

    @property
    def attempt_id(self) -> str:
        return self._source.source_id

    @property
    def durable_prefix(self) -> DurablePrefix:
        source = self._source
        return DurablePrefix(
            source.sequence_start,
            source.sequence_start + source.committed_count,
            source.committed_count,
            source.committed_sha256,
        )

    def recover(self) -> Recovery:
        source = self._source
        count = source.committed_count
        return Recovery(source.source_id, count, count * AUDIO_RECORD_SIZE, True, None)

    def close(self) -> None:
        """A read-only snapshot owns no persistent resource."""


class SqliteStagedAttempt:
    """Runtime attempt target backed solely by :class:`InternalAudioStore`."""

    def __init__(
        self,
        audio_store: InternalAudioStore,
        source: CaptureSource,
        descriptor: AttemptDescriptor,
        *,
        close_store: bool = False,
    ) -> None:
        self._audio_store = audio_store
        self._source = source
        self.descriptor = descriptor
        self._close_store = close_store
        self._closed = False
        self._resume_lease: DeviceLock | None = None

    @property
    def attempt_id(self) -> str:
        return self.descriptor.attempt_id

    @property
    def source(self) -> CaptureSource:
        """Return the latest immutable SQL source snapshot."""
        try:
            source = self._audio_store.get_source(self.attempt_id)
        except InternalAudioStoreError as error:
            raise AttemptStateError(str(error)) from error
        if source is None:
            raise AttemptStateError("typed audio source no longer exists")
        self._source = source
        return source

    @property
    def path(self) -> None:
        """Typed attempts intentionally have no filesystem path."""
        return None

    @property
    def durable_prefix(self) -> DurablePrefix:
        source = self.source
        return DurablePrefix(
            source.sequence_start,
            source.sequence_start + source.committed_count,
            source.committed_count,
            source.committed_sha256,
        )

    def source_page(self, after_ordinal: int = -1, limit: int = 128) -> AudioSourcePage:
        """Read one bounded, short-lived page for the publication adapter."""
        source = self.source
        try:
            records = self._audio_store.read_record_page(self.attempt_id, after_ordinal, limit)
        except InternalAudioStoreError as error:
            raise AttemptStateError(str(error)) from error
        return AudioSourcePage(source, after_ordinal, records)

    def record_read_begin(self, notice: ReadBeginNotification) -> None:
        self._require_open()
        try:
            self._audio_store.record_read_begin(
                self.attempt_id,
                notice.transfer_start_sequence,
                notice.packet_count,
            )
        except InternalAudioStoreError as error:
            raise AttemptStateError(str(error)) from error
        self._refresh_descriptor()

    def prepare_leg(self, start: int, count: int) -> DurablePrefix:
        self._require_open()
        try:
            self._audio_store.prepare_leg(self.attempt_id, start, count)
        except InternalAudioStoreError as error:
            raise AttemptStateError(str(error)) from error
        self._refresh_descriptor()
        return self.durable_prefix

    def begin_recovery(self, start_sequence: int, packet_count: int) -> DurablePrefix:
        return self.prepare_leg(start_sequence, packet_count)

    def accept_chunk(
        self,
        start_sequence: int,
        records: bytes | bytearray | memoryview,
        tail: bytes | bytearray | memoryview = b"",
    ) -> None:
        """Commit exact complete records and their bounded following tail."""
        self._require_open()
        try:
            payload = bytes(records)
            partial = bytes(tail)
        except (TypeError, ValueError) as error:
            raise AttemptStateError("ring drain must be bytes-like") from error
        if len(payload) % AUDIO_RECORD_SIZE:
            raise AttemptStateError(f"ring chunk must be a multiple of {AUDIO_RECORD_SIZE} bytes")
        if len(partial) >= AUDIO_RECORD_SIZE:
            raise AttemptStateError(f"ring tail must be shorter than {AUDIO_RECORD_SIZE} bytes")
        if not payload and not partial:
            raise AttemptStateError("ring drain must contain records or a partial tail")
        offset = start_sequence - self.descriptor.start_sequence
        if offset < 0:
            raise RecordRegressionError("record precedes the source range")
        positions = range(0, len(payload), self._audio_store.chunk_limit * AUDIO_RECORD_SIZE)
        for position in positions:
            chunk = payload[position : position + self._audio_store.chunk_limit * AUDIO_RECORD_SIZE]
            expected = offset + position // AUDIO_RECORD_SIZE
            chunk_end = position + len(chunk)
            following = partial if chunk_end == len(payload) else b""
            try:
                source = self.source
                record_count = len(chunk) // AUDIO_RECORD_SIZE
                checkpoint_tail = (
                    CaptureTail(
                        source.current_leg_generation,
                        expected + record_count,
                        (expected + record_count) * AUDIO_RECORD_SIZE + len(following),
                        (expected + record_count) * AUDIO_RECORD_SIZE,
                        following,
                    )
                    if following
                    else None
                )
                self._audio_store.commit_drain(
                    self.attempt_id,
                    expected,
                    (chunk[index : index + AUDIO_RECORD_SIZE] for index in range(0, len(chunk), AUDIO_RECORD_SIZE)),
                    DrainCheckpoint(source.current_leg_generation, checkpoint_tail),
                )
            except InternalAudioStoreError as error:
                raise RecordAcceptanceError(str(error)) from error
        if not payload:
            try:
                source = self.source
                checkpoint_tail = CaptureTail(
                    source.current_leg_generation,
                    offset,
                    offset * AUDIO_RECORD_SIZE + len(partial),
                    offset * AUDIO_RECORD_SIZE,
                    partial,
                )
                self._audio_store.commit_tail(
                    self.attempt_id,
                    offset,
                    DrainCheckpoint(source.current_leg_generation, checkpoint_tail),
                )
            except InternalAudioStoreError as error:
                raise RecordAcceptanceError(str(error)) from error
        self._refresh_descriptor()

    def checkpoint(self) -> DurablePrefix:
        self._require_open()
        return self.durable_prefix

    def recover(self) -> Recovery:
        source = self.source
        count = source.committed_count
        return Recovery(self.attempt_id, count, count * AUDIO_RECORD_SIZE, True, None)

    def activate_for_resume(self, lease: DeviceLock) -> None:
        lease.require_active()
        self._resume_lease = lease

    def interrupt(self) -> None:
        self._require_open()
        try:
            self._audio_store.interrupt(self.attempt_id)
        except InternalAudioStoreError as error:
            raise AttemptStateError(str(error)) from error
        self._refresh_descriptor()

    def seal(self, done: DoneNotification) -> DurableSqliteSeal:
        """Validate a complete source before the publication adapter takes it."""
        self._require_open()
        source = self.source
        if not done.is_ok:
            raise AttemptStateError("DONE status does not permit sealing")
        if source.committed_count != self.descriptor.packet_count:
            raise AttemptStateError("typed source is incomplete")
        if done.next_sequence != self.descriptor.start_sequence + self.descriptor.packet_count:
            raise AttemptStateError("DONE count does not match the typed source")
        try:
            sealed = self._audio_store.seal_source(
                self.attempt_id,
                leg_generation=source.current_leg_generation,
                expected_count=source.committed_count,
                expected_sha256=source.committed_sha256,
            )
        except InternalAudioStoreError as error:
            raise AttemptStateError(str(error)) from error
        self._refresh_descriptor()
        return DurableSqliteSeal(
            sealed.source_id,
            sealed.sequence_start,
            sealed.sequence_start + sealed.committed_count,
            sealed.committed_count,
            sealed.committed_sha256,
        )

    def publish_prefix(self) -> DurableSqliteSeal:
        """Typed sources are published from SQL rows by the publication worker."""
        self._require_open()
        source = self.source
        return DurableSqliteSeal(
            source.source_id,
            source.sequence_start,
            source.sequence_start + source.committed_count,
            source.committed_count,
            source.committed_sha256,
        )

    def close(self, *, durable: bool = False) -> None:
        del durable
        if self._closed:
            return
        self._closed = True
        if self._close_store:
            self._audio_store.close()

    def _refresh_descriptor(self) -> None:
        source = self.source
        self.descriptor = replace(
            self.descriptor,
            read_begin_start=source.read_begin_start,
            read_begin_count=source.read_begin_count,
        )

    def _require_open(self) -> None:
        if self._closed:
            raise AttemptStateError("typed attempt is closed")
