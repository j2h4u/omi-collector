"""Attempt descriptors and the durable attempt sequencing facade."""

from __future__ import annotations

import shutil
from contextlib import suppress
from dataclasses import asdict
from enum import Enum
from hashlib import sha256
from pathlib import Path
from typing import BinaryIO, Protocol, cast

from ..domain.ring_protocol import RECORD_SIZE, DoneNotification, ReadBeginNotification
from .publication import (
    PrefixPublicationEvidence,
    SealResult,
    _bundle_matches,
    _prefix_destination_for,
    _prefix_destination_matches,
    publish_full_directory,
    publish_prefix_directory,
)
from .recovery import Recovery
from .staging_contract import (
    _CHECKPOINT_NAME,
    _DESCRIPTOR_NAME,
    _MANIFEST_NAME,
    _PREFIX_PUBLICATION_NAME,
    _RAW_NAME,
    _RECEIPT_NAME,
    AttemptDescriptor,
    AttemptStateError,
    CollisionError,
    DurablePrefix,
    StreamingCheckpoint,
    _read_checkpoint,
    _validate_count,
    _validate_int,
)
from .staging_filesystem import (
    DeviceLock,
    StagingFilesystem,
    _file_size,
    _fsync_path,
    _read_prefix,
    _require_regular_file,
    _sync_directory,
)


class RecordAcceptanceError(AttemptStateError):
    """A recovery record cannot be reconciled with the durable prefix."""


class RecordMismatchError(RecordAcceptanceError):
    """A replayed sequence contains bytes different from the durable prefix."""


class RecordGapError(RecordAcceptanceError):
    """A recovery record skips over the next sequence that can be appended."""


class RecordRegressionError(RecordAcceptanceError):
    """A recovery record precedes the original snapshot or requested range."""


class RecordDisposition(Enum):
    """The idempotent result of accepting one recovery record."""

    REPLAYED = "replayed"
    APPENDED = "appended"


class _HashDigest(Protocol):
    def update(self, data: bytes, /) -> None: ...

    def hexdigest(self) -> str: ...


class StagedAttempt:
    """One on-disk attempt; instances never perform Bluetooth operations."""

    def __init__(
        self,
        filesystem: StagingFilesystem,
        path: Path,
        descriptor: AttemptDescriptor,
        *,
        live: bool = True,
    ) -> None:
        self._filesystem = filesystem
        self.path = path
        self.descriptor = descriptor
        self._stream_raw: BinaryIO | None = None
        self._stream_hash: _HashDigest = sha256()
        self._stream_durable_hash = self._stream_hash.hexdigest()
        self._stream_next_index = 0
        self._stream_durable_next_index = 0
        self._stream_written_bytes = 0
        self._stream_failed = False
        self._live_recovery = live
        self._resume_lease: DeviceLock | None = None
        self._stream_requires_recovery = False
        self._recovery_start: int | None = None
        self._recovery_count: int | None = None
        self._hydrate_streaming_prefix()
        self._stream_raw = cast(BinaryIO, (path / _RAW_NAME).open("ab", buffering=0)) if live else None

    @property
    def attempt_id(self) -> str:
        return self.descriptor.attempt_id

    def record_read_begin(self, notice: ReadBeginNotification) -> None:
        """Durably bind this attempt to the device's matching READ_BEGIN."""
        self._require_resume_lease()
        if self.descriptor.read_begin_start is not None:
            if (notice.transfer_start_sequence, notice.packet_count) == (
                self.descriptor.read_begin_start,
                self.descriptor.read_begin_count,
            ):
                return
            if (notice.transfer_start_sequence, notice.packet_count) == (
                self._recovery_start,
                self._recovery_count,
            ):
                return
            raise AttemptStateError("READ_BEGIN is already persisted")
        if (notice.transfer_start_sequence, notice.packet_count) != self._requested_range():
            raise AttemptStateError("READ_BEGIN does not match the prepared READ range")
        descriptor = AttemptDescriptor(
            self.descriptor.attempt_id,
            self.descriptor.device_slug,
            self.descriptor.start_sequence,
            self.descriptor.packet_count,
            read_begin_start=notice.transfer_start_sequence,
            read_begin_count=notice.packet_count,
        )
        self._filesystem._write_json_atomic(self.path / _DESCRIPTOR_NAME, asdict(descriptor))
        self.descriptor = descriptor

    def prepare_leg(self, start: int, count: int) -> DurablePrefix:
        """Persist only the current live recovery intent before a READ."""
        if self.descriptor.read_begin_start is None:
            if (start, count) != self._requested_range():
                raise AttemptStateError("initial READ does not match the prepared range")
            return self.checkpoint()
        return self.begin_recovery(start, count)

    def append_record(self, index: int, sequence: int, record: bytes) -> None:
        """Append and fsync one 444-byte record through the streaming writer."""
        self._require_resume_lease()
        self._append_streaming_record(index, sequence, record)

    @property
    def durable_prefix(self) -> DurablePrefix:
        """Return the verified streaming prefix without changing on-disk bytes."""
        if not self._live_recovery:
            raise AttemptStateError("reopened streaming attempts have no trusted live durable prefix")
        return DurablePrefix(
            self.descriptor.start_sequence,
            self.descriptor.start_sequence + self._stream_durable_next_index,
            self._stream_durable_next_index,
            self._stream_durable_hash,
        )

    def begin_recovery(self, start_sequence: int, packet_count: int) -> DurablePrefix:
        """Durably record a continuation range before accepting any DATA bytes."""
        self._require_resume_lease()
        if not self._live_recovery:
            raise AttemptStateError("reopened partial attempts are not recoverable")
        _validate_int(start_sequence, "recovery start_sequence")
        _validate_count(packet_count)
        if self.descriptor.read_begin_start is None or self.descriptor.read_begin_count is None:
            raise AttemptStateError("READ_BEGIN must be persisted before recovery")
        original_end = self.descriptor.start_sequence + self.descriptor.packet_count
        if start_sequence < self.descriptor.start_sequence or start_sequence + packet_count > original_end:
            raise RecordRegressionError("recovery leg is outside the original snapshot range")
        prefix = self.durable_prefix
        if start_sequence > prefix.next_sequence:
            raise RecordGapError("recovery leg starts after the next durable sequence")
        # Recovery intent is ephemeral. The checkpoint is the sole durable
        # recovery frontier; after a restart the next READ begins at it again.
        self._recovery_start = start_sequence
        self._recovery_count = packet_count
        return prefix

    def accept_record(self, sequence: int, record: bytes) -> RecordDisposition:
        """Replay or append one record while preserving the durable prefix."""
        dispositions = self.accept_chunk(sequence, record)
        return dispositions[0]

    def accept_chunk(
        self, start_sequence: int, records: bytes | bytearray | memoryview
    ) -> tuple[RecordDisposition, ...]:
        """Replay or append one contiguous block of complete ring records.

        The raw stream is a synchronous disk target; callers own transport
        buffering and must hand us an immutable snapshot of the block.  A
        replay prefix is compared before one write appends its new suffix.
        """
        self._require_resume_lease()
        _validate_int(start_sequence, "start_sequence")
        try:
            payload = bytes(records)
        except (TypeError, ValueError) as error:
            raise AttemptStateError("ring chunk must be bytes-like") from error
        if not payload or len(payload) % RECORD_SIZE:
            raise AttemptStateError(f"ring chunk must be a positive multiple of {RECORD_SIZE} bytes")
        record_count = len(payload) // RECORD_SIZE
        return self._accept_streaming_chunk(start_sequence, payload, record_count)

    def _accept_streaming_chunk(
        self, start_sequence: int, payload: bytes, record_count: int
    ) -> tuple[RecordDisposition, ...]:

        if self._recovery_start is None or self._recovery_count is None:
            expected = self.descriptor.start_sequence + self._stream_next_index
            if start_sequence != expected:
                raise AttemptStateError("initial record is not the next expected sequence")
            self._append_streaming_chunk(self._stream_next_index, payload)
            return (RecordDisposition.APPENDED,) * record_count
        return self._accept_recovery_chunk(start_sequence, payload, record_count)

    def _accept_recovery_chunk(
        self, start_sequence: int, payload: bytes, record_count: int
    ) -> tuple[RecordDisposition, ...]:
        end_sequence = start_sequence + record_count
        recovery_start = self._recovery_start
        recovery_count = self._recovery_count
        assert recovery_start is not None and recovery_count is not None
        if start_sequence < recovery_start:
            raise RecordRegressionError("recovery sequence regresses before the recovery leg")
        if end_sequence > recovery_start + recovery_count:
            raise RecordGapError("recovery sequence exceeds the requested range")

        next_sequence = self.descriptor.start_sequence + self._stream_next_index
        if start_sequence > next_sequence:
            raise RecordGapError("recovery sequence leaves a gap after the durable prefix")
        replay_count = max(0, min(end_sequence, next_sequence) - start_sequence)
        if replay_count:
            offset = (start_sequence - self.descriptor.start_sequence) * RECORD_SIZE
            replay_bytes = replay_count * RECORD_SIZE
            with (self.path / _RAW_NAME).open("rb") as raw:
                raw.seek(offset)
                existing = raw.read(replay_bytes)
            if existing != payload[:replay_bytes]:
                raise RecordMismatchError("replayed record bytes do not match the durable prefix")
        appended_count = record_count - replay_count
        if appended_count:
            self._append_streaming_chunk(self._stream_next_index, payload[replay_count * RECORD_SIZE :])
        return (RecordDisposition.REPLAYED,) * replay_count + (RecordDisposition.APPENDED,) * appended_count

    def recover(self) -> Recovery:
        """Return the checkpoint-authenticated prefix and raw evidence status."""
        return self._recover_streaming()

    def seal(self, done: DoneNotification) -> SealResult:
        """Publish a fully validated attempt using an fsync-and-rename boundary."""
        self._require_resume_lease()
        raw_path = self.path / _RAW_NAME
        self._require_streaming_sealable(done)
        self._flush_stream_raw(durable=True)
        if _file_size(raw_path) != self._stream_written_bytes:
            raise AttemptStateError("streaming raw file changed during transfer")
        # ``_stream_hash`` is updated from every byte accepted by the single
        # writer thread.  The lease and the size check above establish that no
        # other writer can have changed the file, so reuse this digest instead
        # of reading the completed raw stream a second time just to hash it.
        raw_hash = self._stream_hash.hexdigest()
        self.close()
        bundle_path = self._bundle_path(raw_hash)
        manifest = self._manifest(raw_hash)
        if bundle_path.exists():
            if _bundle_matches(
                bundle_path,
                raw_path,
                manifest,
                io_chunk_bytes=self._filesystem._durability.io_chunk_bytes,
            ):
                self._filesystem._write_json_atomic(self.path / _MANIFEST_NAME, manifest)
                self._filesystem._write_json_atomic(self.path / _RECEIPT_NAME, self._receipt(raw_hash))
                return SealResult(bundle_path, True)
            raise CollisionError(f"bundle collision preserved at {self.path}; destination is {bundle_path}")
        self._filesystem._write_json_atomic(self.path / _MANIFEST_NAME, manifest)
        self._filesystem._write_json_atomic(self.path / _RECEIPT_NAME, self._receipt(raw_hash))
        publish_full_directory(self._filesystem, self.path, bundle_path, self.descriptor.device_slug)
        shutil.rmtree(self.path)
        _sync_directory(self._filesystem.attempts_root, self._filesystem._fsync)
        return SealResult(bundle_path, False)

    def publish_prefix(self) -> SealResult | None:
        """Publish only the checkpoint-authenticated prefix as an ordinary bundle.

        The source partial remains in place so uncheckpointed or torn raw bytes
        remain available for inspection. A zero-length prefix has no audio
        artifact; its source reaches terminal-retired only after the writer's
        successful close.
        """
        self._require_resume_lease()
        prefix = self.durable_prefix
        if self._stream_raw is not None:
            self._flush_stream_raw(durable=True)
        prefix_bytes = _read_prefix(self.path / _RAW_NAME, prefix.record_count * RECORD_SIZE)
        if sha256(prefix_bytes).hexdigest() != prefix.raw_sha256:
            raise AttemptStateError("streaming prefix hash does not match raw bytes")
        if not prefix.record_count:
            self._filesystem._write_json_atomic(
                self.path / _PREFIX_PUBLICATION_NAME,
                PrefixPublicationEvidence().as_dict(),
            )
            return None

        destination = _prefix_destination_for(self._filesystem.capture_root, self.descriptor.device_slug, prefix)
        manifest = self._manifest_for_prefix(prefix)
        if destination.exists():
            if _prefix_destination_matches(
                destination,
                prefix_bytes,
                manifest,
                io_chunk_bytes=self._filesystem._durability.io_chunk_bytes,
            ):
                self._filesystem._write_json_atomic(
                    self.path / _PREFIX_PUBLICATION_NAME,
                    PrefixPublicationEvidence().as_dict(),
                )
                return SealResult(destination, True)
            raise CollisionError(f"prefix collision preserved at {self.path}; destination is {destination}")
        publish_prefix_directory(
            self._filesystem,
            destination=destination,
            device_slug=self.descriptor.device_slug,
            prefix_bytes=prefix_bytes,
            manifest=manifest,
            receipt=self._receipt(prefix.raw_sha256),
        )
        self._filesystem._write_json_atomic(
            self.path / _PREFIX_PUBLICATION_NAME,
            PrefixPublicationEvidence().as_dict(),
        )
        return SealResult(destination, False)

    def close(self, *, durable: bool = False, _durable: bool | None = None) -> None:
        """Close a streaming handle while retaining partial evidence.

        ``durable`` is opt-in for callers that are about to lose the process;
        ``_durable`` remains accepted for the collector's protocol spelling.
        """
        stream = self._stream_raw
        if stream is None:
            return
        self._stream_raw = None
        should_durable = durable if _durable is None else _durable
        try:
            stream.flush()
            if should_durable:
                self._filesystem._fsync(stream.fileno())
                self._stream_durable_next_index = self._stream_next_index
                self._stream_durable_hash = self._stream_hash.hexdigest()
        finally:
            stream.close()

    def activate_for_resume(self, lease: DeviceLock) -> None:
        """Bind a startup-hydrated inspection to its consuming device lease."""
        lease.require_active()
        if self._live_recovery or self._stream_raw is not None:
            raise AttemptStateError("streaming attempt is already active")
        self._resume_lease = lease
        try:
            raw_count = self._stream_written_bytes // RECORD_SIZE
            if raw_count > self._stream_durable_next_index:
                # Hydration already authenticated the checkpoint prefix and
                # computed the complete raw digest in one streaming pass.
                # Only this lease-bound consuming seam may advance the
                # durable frontier over the complete aligned tail.
                raw_path = self.path / _RAW_NAME
                _fsync_path(raw_path, self._filesystem._fsync)
                self._stream_durable_next_index = raw_count
                self._stream_durable_hash = self._stream_hash.hexdigest()
                self._filesystem._write_checkpoint(
                    self.path,
                    StreamingCheckpoint(1, self.attempt_id, raw_count, self._stream_durable_hash),
                )
            self._stream_next_index = raw_count
            self._live_recovery = True
            self._stream_requires_recovery = False
            self._stream_raw = cast(BinaryIO, (self.path / _RAW_NAME).open("ab", buffering=0))
        except BaseException:
            self._resume_lease = None
            raise

    def checkpoint(self) -> DurablePrefix:
        """Fsync raw bytes, then atomically publish their checkpoint proof."""
        self._require_resume_lease()
        # The upstream DATA path flushes full chunks during transfer:
        # https://github.com/BasedHardware/omi/blob/6f7c57ac1545c1931c806a01605646405d398198/app/lib/services/wals/ring_storage_sync.dart#L504-L523
        _require_regular_file(self.path / _RAW_NAME, "streaming raw file")
        self._checkpoint_stream_prefix(self._stream_next_index, self._stream_hash.hexdigest())
        return self.durable_prefix

    def __del__(self) -> None:
        with suppress(Exception):
            self.close()

    def _require_resume_lease(self) -> None:
        if self._resume_lease is not None:
            self._resume_lease.require_active()

    def _requested_range(self) -> tuple[int, int]:
        return self.descriptor.start_sequence, self.descriptor.packet_count

    def _hydrate_streaming_prefix(self) -> None:
        """Authenticate the checkpoint prefix while hashing raw evidence once."""
        raw_path = self.path / _RAW_NAME
        checkpoint_path = self.path / _CHECKPOINT_NAME
        _require_regular_file(raw_path, "streaming raw file")
        _require_regular_file(checkpoint_path, "streaming checkpoint")
        checkpoint = _read_checkpoint(checkpoint_path, self.attempt_id)
        raw_size = raw_path.stat().st_size
        if raw_size % RECORD_SIZE:
            raise AttemptStateError("streaming raw file is not record aligned")
        raw_count = raw_size // RECORD_SIZE
        limit = self.descriptor.packet_count
        if self.descriptor.read_begin_count is not None:
            limit = min(limit, self.descriptor.read_begin_count)
        if raw_count > limit:
            raise AttemptStateError("streaming raw file exceeds the prepared range")
        if checkpoint.record_count > limit:
            raise AttemptStateError("streaming checkpoint exceeds the prepared range")
        if raw_count < checkpoint.record_count:
            raise AttemptStateError("streaming raw file is shorter than its checkpoint")
        checkpoint_bytes = checkpoint.record_count * RECORD_SIZE
        bytes_seen, digest, prefix_hash = self._hash_raw_stream(raw_path, checkpoint_bytes)
        if bytes_seen != raw_size:
            raise AttemptStateError("streaming raw file changed during hydration")
        if prefix_hash != checkpoint.raw_sha256:
            raise AttemptStateError("streaming checkpoint hash does not match raw bytes")
        # Keep the complete digest state from the streaming pass.  Hashlib's
        # state cannot be reconstructed from a digest, so replaying the chunks
        # would violate the one-pass hydration contract.
        self._stream_hash = digest
        self._stream_durable_hash = checkpoint.raw_sha256
        self._stream_durable_next_index = checkpoint.record_count
        self._stream_written_bytes = raw_size
        # Keep the uncheckpointed tail in memory only as a digest state. It is
        # not consumable until activate_for_resume binds an active device lease.
        self._stream_next_index = checkpoint.record_count
        self._stream_requires_recovery = not self._live_recovery

    def _hash_raw_stream(self, path: Path, checkpoint_bytes: int) -> tuple[int, _HashDigest, str | None]:
        """Hash raw evidence once and return byte count, full hash, and prefix hash."""
        digest = sha256()
        prefix_remaining = checkpoint_bytes
        prefix_hash = sha256(b"").hexdigest() if not prefix_remaining else None
        bytes_seen = 0
        with path.open("rb") as raw:
            while chunk := raw.read(self._filesystem._durability.io_chunk_bytes):
                prefix_remaining, prefix_hash = self._hash_raw_chunk(
                    digest,
                    chunk,
                    prefix_remaining,
                    prefix_hash,
                )
                bytes_seen += len(chunk)
        return bytes_seen, digest, prefix_hash

    @staticmethod
    def _hash_raw_chunk(
        digest: _HashDigest,
        chunk: bytes,
        prefix_remaining: int,
        prefix_hash: str | None,
    ) -> tuple[int, str | None]:
        """Update one raw chunk while capturing the checkpoint boundary digest."""
        if prefix_remaining:
            prefix_bytes = min(len(chunk), prefix_remaining)
            digest.update(chunk[:prefix_bytes])
            prefix_remaining -= prefix_bytes
            if not prefix_remaining:
                prefix_hash = digest.hexdigest()
            digest.update(chunk[prefix_bytes:])
        else:
            digest.update(chunk)
        return prefix_remaining, prefix_hash

    def _append_streaming_record(self, index: int, sequence: int, record: bytes) -> None:
        _validate_int(index, "index")
        _validate_int(sequence, "sequence")
        if len(record) != RECORD_SIZE:
            raise AttemptStateError(f"ring record must be exactly {RECORD_SIZE} bytes")
        if sequence != self.descriptor.start_sequence + index:
            raise AttemptStateError("record index or sequence is not the next expected value")
        self._append_streaming_chunk(index, record)

    def _append_streaming_chunk(self, index: int, payload: bytes) -> None:
        """Write one contiguous suffix and advance its in-memory proofs."""
        _validate_int(index, "index")
        if not payload or len(payload) % RECORD_SIZE:
            raise AttemptStateError(f"ring chunk must be a positive multiple of {RECORD_SIZE} bytes")
        record_count = len(payload) // RECORD_SIZE
        self._validate_streaming_chunk_append(index, record_count)
        self._write_streaming_chunk(payload)
        old_index = self._stream_next_index
        self._stream_next_index += record_count
        self._stream_written_bytes += len(payload)
        checkpoint_hashes = self._hash_streaming_chunk(old_index, payload, record_count)
        self._checkpoint_streaming_boundaries(checkpoint_hashes)

    def _validate_streaming_chunk_append(self, index: int, record_count: int) -> None:
        if self._stream_failed:
            raise AttemptStateError("streaming attempt has preserved partial evidence")
        if self._stream_requires_recovery:
            raise AttemptStateError("attempt has preserved partial evidence; restart is blocked")
        if self._stream_raw is None:
            raise AttemptStateError("streaming attempt is closed")
        if self.descriptor.read_begin_start is None or self.descriptor.read_begin_count is None:
            raise AttemptStateError("READ_BEGIN must be persisted before records")
        if index != self._stream_next_index:
            raise AttemptStateError("record index is not the next expected value")
        if index + record_count > self.descriptor.read_begin_count:
            raise AttemptStateError("record count exceeds READ_BEGIN")

    def _write_streaming_chunk(self, payload: bytes) -> None:
        stream = self._stream_raw
        if stream is None:
            raise AttemptStateError("streaming attempt is closed")
        try:
            written = stream.write(payload)
        except OSError:
            self._stream_failed = True
            raise
        if written != len(payload):
            self._stream_failed = True
            raise OSError("streaming raw write was short")

    def _hash_streaming_chunk(self, old_index: int, payload: bytes, record_count: int) -> list[tuple[int, str]]:
        checkpoint_hashes: list[tuple[int, str]] = []
        checkpoint_records = self._filesystem._durability.checkpoint_records
        offset = 0
        hash_index = old_index
        while offset < len(payload):
            next_boundary = ((hash_index // checkpoint_records) + 1) * checkpoint_records
            segment_records = min(next_boundary - hash_index, record_count - offset // RECORD_SIZE)
            segment_bytes = segment_records * RECORD_SIZE
            self._stream_hash.update(payload[offset : offset + segment_bytes])
            offset += segment_bytes
            hash_index += segment_records
            if hash_index % checkpoint_records == 0:
                checkpoint_hashes.append((hash_index, self._stream_hash.hexdigest()))
        return checkpoint_hashes

    def _checkpoint_streaming_boundaries(self, checkpoint_hashes: list[tuple[int, str]]) -> None:
        try:
            for checkpoint_count, checkpoint_hash in checkpoint_hashes:
                self._checkpoint_stream_prefix(checkpoint_count, checkpoint_hash)
        except OSError:
            self._stream_failed = True
            raise

    def _checkpoint_stream_prefix(self, record_count: int, raw_hash: str) -> None:
        """Persist a checkpoint boundary without changing the raw stream."""
        self._flush_stream_raw(durable=True)
        self._stream_durable_next_index = record_count
        self._stream_durable_hash = raw_hash
        self._filesystem._write_checkpoint(
            self.path,
            StreamingCheckpoint(1, self.attempt_id, record_count, raw_hash),
        )

    def _checkpoint_if_due(self) -> None:
        if self._stream_next_index % self._filesystem._durability.checkpoint_records != 0:
            return
        try:
            self.checkpoint()
        except OSError:
            self._stream_failed = True
            raise

    def _recover_streaming(self) -> Recovery:
        if self._stream_failed:
            return Recovery(
                self.attempt_id,
                0,
                max(_file_size(self.path / _RAW_NAME), self._stream_written_bytes),
                False,
                "streaming attempt has preserved partial evidence",
            )
        # Construction already authenticated the checkpoint-backed prefix.
        # Reuse that in-memory result here instead of hashing the same prefix
        # a second time during startup validation.
        raw_size = self._stream_written_bytes
        if self.descriptor.read_begin_start is None or self.descriptor.read_begin_count is None:
            return Recovery(
                self.attempt_id,
                self._stream_durable_next_index,
                raw_size,
                raw_size == 0,
                None if raw_size == 0 else "READ_BEGIN is not persisted",
            )
        raw_count, remainder = divmod(raw_size, RECORD_SIZE)
        limit = min(self.descriptor.packet_count, self.descriptor.read_begin_count)
        issue: str | None = None
        if remainder:
            issue = "streaming raw file is not record aligned"
        elif raw_count > limit:
            issue = "streaming raw file exceeds the prepared range"
        elif self._stream_durable_next_index > limit:
            issue = "streaming checkpoint exceeds the prepared range"
        elif raw_count < self._stream_durable_next_index:
            issue = "streaming raw file is shorter than its checkpoint"
        elif raw_count != self.descriptor.read_begin_count:
            issue = "streaming attempt is incomplete"
        return Recovery(self.attempt_id, self._stream_durable_next_index, raw_size, issue is None, issue)

    def _require_streaming_sealable(self, done: DoneNotification) -> None:
        if not done.is_ok:
            raise AttemptStateError("DONE did not report success")
        if self._stream_failed:
            raise AttemptStateError("streaming attempt has preserved partial evidence")
        if self.descriptor.read_begin_start is None or self.descriptor.read_begin_count is None:
            raise AttemptStateError("READ_BEGIN is missing")
        if done.next_sequence != self.descriptor.read_begin_start + self.descriptor.read_begin_count:
            raise AttemptStateError("DONE next sequence does not match READ_BEGIN")
        if self._stream_next_index != self.descriptor.read_begin_count:
            raise AttemptStateError("attempt does not contain a complete record count")
        if self._stream_written_bytes != self.descriptor.read_begin_count * RECORD_SIZE:
            raise AttemptStateError("streaming raw file size does not match record count")

    def _flush_stream_raw(self, *, durable: bool) -> None:
        if self._stream_raw is None:
            raise AttemptStateError("streaming attempt is closed")
        self._stream_raw.flush()
        if durable:
            self._filesystem._fsync(self._stream_raw.fileno())

    def _bundle_path(self, raw_hash: str) -> Path:
        start = self.descriptor.read_begin_start
        count = self.descriptor.read_begin_count
        if start is None or count is None:
            raise AttemptStateError("READ_BEGIN is missing")
        return self._filesystem.capture_root / self.descriptor.device_slug / f"{start}-{start + count}-{raw_hash[:16]}"

    def _manifest(self, raw_hash: str) -> dict[str, object]:
        start = self.descriptor.read_begin_start
        count = self.descriptor.read_begin_count
        if start is None or count is None:
            raise AttemptStateError("READ_BEGIN is missing")
        return {
            "device_slug": self.descriptor.device_slug,
            "start_sequence": start,
            "next_sequence": start + count,
            "record_count": count,
            "record_size": RECORD_SIZE,
            "raw_sha256": raw_hash,
        }

    def _manifest_for_prefix(self, prefix: DurablePrefix) -> dict[str, object]:
        return {
            "device_slug": self.descriptor.device_slug,
            "start_sequence": prefix.start_sequence,
            "next_sequence": prefix.next_sequence,
            "record_count": prefix.record_count,
            "record_size": RECORD_SIZE,
            "raw_sha256": prefix.raw_sha256,
        }

    def _receipt(self, raw_hash: str) -> dict[str, object]:
        receipt: dict[str, object] = {"attempt_id": self.attempt_id, "raw_sha256": raw_hash, "status": "sealed"}
        return receipt
