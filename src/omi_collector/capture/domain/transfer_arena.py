"""Bounded RAM-only assembly for one Omi ring transfer snapshot.

The arena is deliberately narrower than the durable staging layer.  A BLE
notification callback may call :meth:`TransferArena.append` to copy bytes into
the preallocated buffer, but this module never performs I/O, waits, or retains
notification objects.  A writer receives complete records through a read-only
submitted-prefix seam; only an explicit successful checkpoint acknowledgement
advances the durable watermark.
"""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass

from .ring_protocol import RECORD_SIZE


class TransferArenaError(ValueError):
    """Base error for invalid arena or leg metadata."""


class ArenaCapacityError(MemoryError):
    """The requested snapshot exceeds the explicit RAM admission limit."""


class ArenaLegError(TransferArenaError):
    """A leg cannot be admitted into the current snapshot."""


class ArenaOverrunError(ArenaLegError):
    """A fragment would exceed the selected leg or snapshot."""


class ArenaSequenceError(ArenaLegError):
    """A leg is out of order or does not overlap the known prefix."""


class ArenaDataMismatchError(ArenaLegError):
    """A replayed overlap differs from bytes already received."""


class ArenaPublicationError(TransferArenaError):
    """A writer or checkpoint requested an invalid boundary."""


@dataclass(frozen=True, slots=True)
class TransferSnapshot:
    """Immutable bounds for one transfer.

    ``record_size`` is retained in the snapshot so a writer can carry all
    framing metadata without depending on module globals.  Omi ring records
    are intentionally fixed at 444 bytes; accepting another size would make
    sequence validation ambiguous.
    """

    start_sequence: int
    total_records: int
    record_size: int = RECORD_SIZE

    def __post_init__(self) -> None:
        _require_uint(self.start_sequence, "start_sequence")
        _require_uint(self.total_records, "total_records")
        if self.record_size != RECORD_SIZE:
            raise TransferArenaError(f"record_size must be exactly {RECORD_SIZE}")

    @property
    def total_bytes(self) -> int:
        """Return the exact backing-buffer size."""
        return self.total_records * self.record_size

    @property
    def end_sequence(self) -> int:
        """Return the first sequence after the snapshot."""
        return self.start_sequence + self.total_records


@dataclass(frozen=True, slots=True)
class WriterCheckpoint:
    """Successful writer result authorizing a durable arena watermark."""

    next_sequence: int

    def __post_init__(self) -> None:
        _require_uint(self.next_sequence, "next_sequence")


Buffer = bytes | bytearray | memoryview


class TransferArena:
    """Assemble one bounded snapshot without retaining notification objects.

    The constructor performs all admission checks before allocating the exact
    ``total_records * 444`` byte buffer.  The initial leg is the snapshot's
    complete requested range, which permits a callback to append immediately.
    Reconnects must call :meth:`begin_leg` explicitly.
    """

    def __init__(
        self,
        start_sequence: int,
        total_records: int,
        *,
        max_bytes: int,
        record_size: int = RECORD_SIZE,
    ) -> None:
        snapshot = TransferSnapshot(start_sequence, total_records, record_size)
        _require_uint(max_bytes, "max_bytes")
        if snapshot.total_bytes > max_bytes:
            raise ArenaCapacityError(f"snapshot requires {snapshot.total_bytes} bytes; max_bytes is {max_bytes}")

        # Keep this as the only allocation owned by the callback-side arena.
        self._buffer = bytearray(snapshot.total_bytes)
        self._snapshot = snapshot
        self._received_bytes = 0
        self._submitted_bytes = 0
        self._durable_bytes = 0
        self._leg_start = snapshot.start_sequence
        self._leg_bytes = 0
        self._leg_total_bytes = snapshot.total_bytes

    @property
    def snapshot(self) -> TransferSnapshot:
        """Return immutable transfer bounds."""
        return self._snapshot

    @property
    def start_sequence(self) -> int:
        """Return the first sequence in the snapshot."""
        return self._snapshot.start_sequence

    @property
    def record_size(self) -> int:
        """Return the fixed Omi record size."""
        return self._snapshot.record_size

    @property
    def total_records(self) -> int:
        """Return the snapshot's record count."""
        return self._snapshot.total_records

    @property
    def total_bytes(self) -> int:
        """Return the snapshot's exact byte count."""
        return self._snapshot.total_bytes

    @property
    def received_bytes(self) -> int:
        """Return the O(1) byte watermark copied into the arena."""
        return self._received_bytes

    @property
    def complete_records(self) -> int:
        """Return the O(1) record-aligned receive watermark."""
        return self._received_bytes // self.record_size

    @property
    def received_records(self) -> int:
        """Alias for :attr:`complete_records`."""
        return self.complete_records

    @property
    def leg_received_bytes(self) -> int:
        """Return bytes received for the currently selected leg."""
        return self._leg_bytes

    @property
    def leg_complete_records(self) -> int:
        """Return complete records received for the current leg."""
        return self._leg_bytes // self.record_size

    @property
    def next_sequence(self) -> int:
        """Return the first sequence after the complete received prefix."""
        return self.start_sequence + self.complete_records

    @property
    def submitted_bytes(self) -> int:
        """Return the non-durable prefix handed to the writer."""
        return self._submitted_bytes

    @property
    def submitted_records(self) -> int:
        """Return the complete records handed to the writer."""
        return self._submitted_bytes // self.record_size

    @property
    def durable_bytes(self) -> int:
        """Return the prefix confirmed by an explicit writer checkpoint."""
        return self._durable_bytes

    @property
    def durable_records(self) -> int:
        """Return the complete records confirmed by the writer checkpoint."""
        return self._durable_bytes // self.record_size

    @property
    def durable_next_sequence(self) -> int:
        """Return the first sequence after the durable prefix."""
        return self.start_sequence + self.durable_records

    def begin_leg(self, start_sequence: int, record_count: int) -> None:
        """Validate and select a reconnect leg.

        A leg may continue at the complete receive watermark, or replay any
        still-resident received bytes, regardless of durability.  A gap, a
        regression before the snapshot, and a leg extending beyond the
        immutable snapshot are all rejected before any data is copied.
        """
        _require_uint(start_sequence, "start_sequence")
        _require_uint(record_count, "record_count")
        if record_count == 0:
            raise ArenaLegError("record_count must be positive")
        leg_end = start_sequence + record_count
        if start_sequence < self.start_sequence:
            raise ArenaSequenceError("leg starts before the snapshot")
        if leg_end > self._snapshot.end_sequence:
            raise ArenaOverrunError("leg extends beyond the snapshot")

        if start_sequence > self.next_sequence:
            raise ArenaSequenceError("leg starts after the next expected sequence")

        self._leg_start = start_sequence
        self._leg_bytes = 0
        self._leg_total_bytes = record_count * self.record_size

    def append(self, fragment: Buffer) -> None:
        """Copy one arbitrary notification fragment into the selected leg.

        The fragment may split records at any byte boundary.  Replay bytes are
        compared against the existing prefix and never written again; only the
        previously unseen suffix advances the receive watermark.
        """
        view = memoryview(fragment)
        length = len(view)
        remaining = self._leg_total_bytes - self._leg_bytes
        if length > remaining:
            raise ArenaOverrunError("fragment exceeds the selected leg")

        absolute_start = (self._leg_start - self.start_sequence) * self.record_size + self._leg_bytes
        absolute_end = absolute_start + length
        if absolute_end > self.total_bytes:
            raise ArenaOverrunError("fragment exceeds the snapshot")

        overlap_end = min(absolute_end, self._received_bytes)
        if absolute_start < overlap_end:
            existing = memoryview(self._buffer)[absolute_start:overlap_end]
            incoming = view[: overlap_end - absolute_start]
            if existing != incoming:
                raise ArenaDataMismatchError("replayed bytes differ from the received prefix")

        write_start = max(absolute_start, self._received_bytes)
        if write_start < absolute_end:
            source_start = write_start - absolute_start
            self._buffer[write_start:absolute_end] = view[source_start:]
            self._received_bytes = absolute_end
        self._leg_bytes += length

    def submit_prefix(self, record_count: int | None = None) -> memoryview:
        """Hand a complete-record prefix to a writer as a read-only view.

        Submission is not a durability acknowledgement.  The coordinator must
        call :meth:`acknowledge_durable` after a successful writer checkpoint.
        ``record_count`` is optional; when omitted, all currently complete
        records are submitted.  Submission never moves backward.
        """
        available = self.complete_records
        target_records = available if record_count is None else record_count
        if not 0 <= target_records <= available:
            raise ArenaPublicationError("submitted record count is outside the received prefix")
        target_bytes = target_records * self.record_size
        if target_bytes < self._submitted_bytes:
            raise ArenaPublicationError("submitted prefix cannot move backward")
        self._submitted_bytes = target_bytes
        return self.submitted_prefix()

    def submitted_prefix(self) -> memoryview:
        """Return the submitted source prefix without changing any cursor."""
        return memoryview(self._buffer)[: self._submitted_bytes].toreadonly()

    def readonly_source(self) -> memoryview:
        """Return a live read-only view over the full preallocated source."""
        return memoryview(self._buffer).toreadonly()

    def acknowledge_durable(self, checkpoint: WriterCheckpoint) -> memoryview:
        """Advance durability only from an explicit successful checkpoint."""
        if not isinstance(checkpoint, WriterCheckpoint):
            raise ArenaPublicationError("durability requires a WriterCheckpoint result")
        next_sequence = checkpoint.next_sequence
        if not self.start_sequence <= next_sequence <= self.start_sequence + self.submitted_records:
            raise ArenaPublicationError("checkpoint exceeds the submitted prefix")
        target_bytes = (next_sequence - self.start_sequence) * self.record_size
        if target_bytes < self._durable_bytes:
            raise ArenaPublicationError("durable prefix cannot move backward")
        self._durable_bytes = target_bytes
        return self.durable_prefix()

    def durable_prefix(self) -> memoryview:
        """Return the explicitly acknowledged durable prefix."""
        return memoryview(self._buffer)[: self._durable_bytes].toreadonly()

    def submitted_chunks(self, chunk_bytes: int) -> Iterator[memoryview]:
        """Yield bounded read-only chunks from the submitted source prefix."""
        return self._chunks(self._submitted_bytes, chunk_bytes)

    def durable_chunks(self, chunk_bytes: int) -> Iterator[memoryview]:
        """Yield bounded read-only chunks from the durable prefix."""
        return self._chunks(self._durable_bytes, chunk_bytes)

    def _chunks(self, limit: int, chunk_bytes: int) -> Iterator[memoryview]:
        """Yield views without copying the arena's backing buffer."""
        _require_uint(chunk_bytes, "chunk_bytes")
        if chunk_bytes == 0:
            raise ValueError("chunk_bytes must be positive")
        for offset in range(0, limit, chunk_bytes):
            yield memoryview(self._buffer)[offset : min(offset + chunk_bytes, limit)].toreadonly()


def _require_uint(value: int, label: str) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise TransferArenaError(f"{label} must be a non-negative integer")
