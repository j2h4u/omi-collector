"""Typed internal audio storage backed by the Collector state database."""

from __future__ import annotations

import hashlib
import sqlite3
import threading
from collections.abc import Iterable, Iterator
from contextlib import closing, contextmanager, suppress
from dataclasses import dataclass, replace
from typing import Final, NoReturn, Protocol, cast

from ..domain.internal_audio_machine import (
    AudioAction,
    AudioEvent,
    AudioPhase,
    Create,
    Data,
    Interruption,
    PrepareLeg,
    ReadBegin,
    transition,
)
from .collector_state_db import StateDB, StateDBError, StateTransaction, install_audio_schema

__all__ = [
    "AUDIO_PAYLOAD_SIZE",
    "AUDIO_RECORD_SIZE",
    "DEFAULT_CHUNK_LIMIT",
    "MAX_CHUNK_LIMIT",
    "CaptureRecord",
    "CaptureSource",
    "CaptureTail",
    "DrainCheckpoint",
    "InternalAudioStore",
    "InternalAudioStoreError",
    "decode_sequence",
    "decode_uint64",
    "encode_sequence",
    "encode_uint64",
    "install_audio_schema",
    "validate_source_stream",
]

AUDIO_RECORD_SIZE: Final = 444
AUDIO_PAYLOAD_SIZE: Final = 440
DEFAULT_CHUNK_LIMIT: Final = 128
MAX_CHUNK_LIMIT: Final = DEFAULT_CHUNK_LIMIT
MAX_PROTOCOL_UINT32: Final = 2**32 - 1
UINT64_BYTES: Final = 8
TAIL_MAX_BYTES: Final = 443
SOURCE_ID_LENGTH: Final = 32
SHA256_HEX_LENGTH: Final = 64

_SOURCE_TYPES = frozenset({"attempt", "draft", "quarantine"})
_EMPTY_SHA256 = hashlib.sha256(b"").hexdigest()


class InternalAudioStoreError(StateDBError):
    """A typed audio operation was refused or could not be made durable."""


@dataclass(frozen=True, slots=True)
class CaptureSource:
    source_id: str
    source_type: str
    lifecycle: str
    sequence_start: int
    sequence_limit: int
    prepared_start: int | None
    prepared_count: int | None
    read_begin_start: int | None
    read_begin_count: int | None
    current_leg_generation: int
    current_leg_start: int
    current_leg_count: int
    committed_count: int
    committed_sha256: str
    closure_frontier: int | None


@dataclass(frozen=True, slots=True)
class CaptureRecord:
    source_id: str
    ordinal: int
    device_timestamp: int
    payload: bytes
    utc_timestamp: int | None = None
    mapping_revision: str | None = None

    @property
    def raw_bytes(self) -> bytes:
        return self.device_timestamp.to_bytes(4, "big") + self.payload


@dataclass(frozen=True, slots=True)
class CaptureTail:
    leg_generation: int
    complete_high_water: int
    received_high_water: int
    byte_offset: int
    data: bytes


@dataclass(frozen=True, slots=True)
class DrainCheckpoint:
    leg_generation: int
    tail: CaptureTail | None = None
    expected_sha256: str | None = None


@dataclass(frozen=True, slots=True)
class _ReplayRequest:
    source: CaptureSource
    expected: int
    records: tuple[bytes, ...]
    expected_sha256: str | None
    tail: CaptureTail | None
    overlap_count: int


@dataclass(frozen=True, slots=True)
class _CreateRequest:
    source_id: str
    source_type: str
    start: int
    limit: int
    generation: int


class _HashState(Protocol):
    def update(self, data: bytes, /) -> None: ...

    def hexdigest(self) -> str: ...

    def copy(self) -> _HashState: ...


def encode_uint64(value: int) -> bytes:
    """Encode an unsigned sequence value as fixed-width big-endian bytes."""

    if type(value) is not int or not 0 <= value <= 2**64 - 1:
        raise InternalAudioStoreError("uint64 value is invalid")
    return value.to_bytes(8, "big")


def decode_uint64(value: bytes | bytearray | memoryview) -> int:
    """Decode one fixed-width unsigned sequence value."""

    if not isinstance(value, (bytes, bytearray, memoryview)) or len(value) != UINT64_BYTES:
        raise InternalAudioStoreError("uint64 bytes are invalid")
    return int.from_bytes(value, "big")


encode_sequence = encode_uint64
decode_sequence = decode_uint64


def validate_source_stream(
    records: Iterable[bytes], *, expected_count: int | None = None, expected_sha256: str | None = None
) -> tuple[int, str]:
    """Hash exact wire records without materializing the stream."""

    count = 0
    digest = hashlib.sha256()
    for record in records:
        raw = _record_bytes(record)
        count += 1
        if count > MAX_PROTOCOL_UINT32:
            raise InternalAudioStoreError("source record count exceeds uint32")
        digest.update(raw)
    result = (count, digest.hexdigest())
    if expected_count is not None and count != _uint32(expected_count, "expected count"):
        raise InternalAudioStoreError("source stream count does not match checkpoint")
    if expected_sha256 is not None and result[1] != _sha256(expected_sha256):
        raise InternalAudioStoreError("source stream digest does not match checkpoint")
    return result


class InternalAudioStore:
    """A thread-owned persistent writer plus short-lived read connections."""

    def __init__(
        self, state_db: StateDB, *, chunk_limit: int = DEFAULT_CHUNK_LIMIT, busy_deadline: float = 5.0
    ) -> None:
        self.state_db = state_db
        self.chunk_limit = _chunk_limit(chunk_limit)
        if isinstance(busy_deadline, bool) or not isinstance(busy_deadline, (int, float)) or busy_deadline <= 0:
            raise InternalAudioStoreError("busy deadline is invalid")
        self.busy_deadline = float(busy_deadline)
        self._owner_thread = threading.get_ident()
        self._failed = False
        self._busy_timeout_ms = max(1, min(5_000, int(self.busy_deadline * 1_000)))
        self._digest_cache: dict[str, tuple[int, str, _HashState]] = {}
        self._connection: sqlite3.Connection | None = None
        try:
            self._connection = state_db.open_connection()
            self._connection.execute(f"PRAGMA busy_timeout = {self._busy_timeout_ms}")
        except sqlite3.DatabaseError as error:
            if self._connection is not None:
                self._connection.close()
            raise InternalAudioStoreError("audio writer connection setup failed") from error

    def close(self) -> None:
        self._check_thread()
        if self._connection is not None:
            self._connection.close()
            self._connection = None

    @contextmanager
    def publication_transaction(self) -> Iterator[tuple[StateTransaction, sqlite3.Connection]]:
        """Commit READY_PUBLICATION and typed cohort/retirement rows together.

        The caller uses the yielded transaction for state rows and the same
        connection for typed rows. On a commit error, it must compare the
        durable stage, cohort, and ledger before reporting success.
        """

        self._check_writer()
        connection = self._require_connection()
        try:
            connection.execute("BEGIN IMMEDIATE")
            state = StateTransaction(connection)
            yield state, connection
            state.finish()
            self._commit()
        except sqlite3.DatabaseError as error:
            self._rollback()
            self._reopen()
            raise InternalAudioStoreError("publication commit requires durable-state inspection") from error
        except BaseException:
            self._rollback()
            raise

    def __enter__(self) -> InternalAudioStore:
        return self

    def __exit__(self, exc_type: object, exc_value: object, traceback: object) -> None:
        self.close()

    def create_source(
        self, source_id: str, source_type: str, sequence_start: int, sequence_limit: int
    ) -> CaptureSource:
        self._check_writer()
        source_id = _source_id(source_id)
        if source_type not in _SOURCE_TYPES:
            raise InternalAudioStoreError("source type is invalid")
        lifecycle = _phase_after(None, Create(), AudioAction.PERSIST).value
        start = _uint64(sequence_start, "sequence start")
        limit = _uint64(sequence_limit, "sequence limit")
        if start > limit or limit - start > MAX_PROTOCOL_UINT32:
            raise InternalAudioStoreError("source sequence range is invalid")
        generation = 0
        connection = self._require_connection()
        try:
            connection.execute("BEGIN IMMEDIATE")
            connection.execute(
                "INSERT INTO capture_source(source_id, source_type, lifecycle, sequence_start, sequence_limit, "
                "current_leg_generation, current_leg_start, current_leg_count, committed_sha256) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, 0, ?)",
                (
                    source_id,
                    source_type,
                    lifecycle,
                    encode_uint64(start),
                    encode_uint64(limit),
                    generation,
                    encode_uint64(start),
                    _EMPTY_SHA256,
                ),
            )
            self._commit()
        except InternalAudioStoreError:
            self._rollback()
            raise
        except sqlite3.IntegrityError as error:
            self._rollback()
            raise InternalAudioStoreError("source already exists or is invalid") from error
        except sqlite3.DatabaseError as error:
            self._rollback()
            return self._recover_create(_CreateRequest(source_id, source_type, start, limit, generation), error)
        result = self.get_source(source_id)
        if result is None:
            raise InternalAudioStoreError("source disappeared after creation")
        return result

    def prepare_leg(self, source_id: str, start: int, count: int) -> CaptureSource:
        """Durably prepare one bounded initial or recovery READ before Bluetooth I/O."""

        self._check_writer()
        source = self._require_source(source_id)
        start = _uint64(start, "leg start")
        count = _uint32(count, "leg count")
        if count == 0 or start < source.sequence_start or start + count > source.sequence_limit:
            raise InternalAudioStoreError("leg is outside the immutable source range")
        if start > source.sequence_start + source.committed_count:
            raise InternalAudioStoreError("leg starts after the durable frontier")
        if source.lifecycle == AudioPhase.PREPARED.value and (
            start != source.sequence_start or start + count != source.sequence_limit
        ):
            raise InternalAudioStoreError("initial leg must cover the prepared source range")
        exact = (start, count) == (source.prepared_start, source.prepared_count)
        next_phase = _phase_after(
            _phase(source),
            PrepareLeg(exact_identity=exact, barrier_committed=False),
            AudioAction.IDEMPOTENT if source.lifecycle == AudioPhase.LEG_PREPARED.value and exact else None,
        )
        if source.lifecycle == AudioPhase.LEG_PREPARED.value and exact:
            return source
        generation = source.current_leg_generation + 1
        if generation > MAX_PROTOCOL_UINT32:
            raise InternalAudioStoreError("leg generation is exhausted")
        connection = self._require_connection()
        try:
            connection.execute("BEGIN IMMEDIATE")
            self._check_source_snapshot(connection, source)
            connection.execute(
                "UPDATE capture_source SET lifecycle=?, prepared_start=?, prepared_count=?, "
                "read_begin_start=NULL, read_begin_count=NULL, current_leg_generation=?, "
                "current_leg_start=?, current_leg_count=? WHERE source_id=?",
                (
                    next_phase.value,
                    encode_uint64(start),
                    count,
                    generation,
                    encode_uint64(start),
                    count,
                    source.source_id,
                ),
            )
            self._commit()
        except (sqlite3.DatabaseError, InternalAudioStoreError) as error:
            self._rollback()
            raise InternalAudioStoreError("leg preparation was not confirmed durable") from error
        return self._require_source(source.source_id)

    def record_read_begin(self, source_id: str, start: int, count: int) -> CaptureSource:
        """Bind the exact prepared leg to its matching device READ_BEGIN."""

        self._check_writer()
        source = self._require_source(source_id)
        start = _uint64(start, "READ_BEGIN start")
        count = _uint32(count, "READ_BEGIN count")
        exact = (start, count) == (source.prepared_start, source.prepared_count)
        if not exact or (start, count) != (source.current_leg_start, source.current_leg_count):
            raise InternalAudioStoreError("READ_BEGIN does not match the durable prepared leg")
        duplicate = source.lifecycle == AudioPhase.RECEIVING.value and (start, count) == (
            source.read_begin_start,
            source.read_begin_count,
        )
        next_phase = _phase_after(
            _phase(source),
            ReadBegin(matching=exact, duplicate=duplicate),
            AudioAction.IDEMPOTENT if duplicate else AudioAction.PERSIST,
        )
        if duplicate:
            return source
        connection = self._require_connection()
        try:
            connection.execute("BEGIN IMMEDIATE")
            self._check_source_snapshot(connection, source)
            connection.execute(
                "UPDATE capture_source SET lifecycle=?, read_begin_start=?, read_begin_count=? WHERE source_id=?",
                (next_phase.value, encode_uint64(start), count, source.source_id),
            )
            self._commit()
        except (sqlite3.DatabaseError, InternalAudioStoreError) as error:
            self._rollback()
            raise InternalAudioStoreError("READ_BEGIN was not confirmed durable") from error
        return self._require_source(source.source_id)

    def interrupt(self, source_id: str) -> CaptureSource:
        """Record an interrupted leg without discarding the durable prefix or tail."""

        self._check_writer()
        source = self._require_source(source_id)
        phase = _phase(source)
        decision = transition(phase, Interruption())
        if decision.action in {AudioAction.IDEMPOTENT, AudioAction.INSPECT_ONLY}:
            return source
        if decision.action is not AudioAction.PERSIST or decision.next_phase is None:
            raise InternalAudioStoreError("interruption is invalid for this source")
        connection = self._require_connection()
        try:
            connection.execute("BEGIN IMMEDIATE")
            self._check_source_snapshot(connection, source)
            connection.execute(
                "UPDATE capture_source SET lifecycle=? WHERE source_id=?",
                (decision.next_phase.value, source.source_id),
            )
            self._commit()
        except (sqlite3.DatabaseError, InternalAudioStoreError) as error:
            self._rollback()
            raise InternalAudioStoreError("interruption was not confirmed durable") from error
        return self._require_source(source.source_id)

    def _require_source(self, source_id: str) -> CaptureSource:
        source = self.get_source(source_id)
        if source is None:
            raise InternalAudioStoreError("source does not exist")
        return source

    def get_source(self, source_id: str) -> CaptureSource | None:
        source_id = _source_id(source_id)
        try:
            with closing(self.state_db.open_connection(read_only=True)) as connection:
                row = _fetchone(connection.execute("SELECT * FROM capture_source WHERE source_id = ?", (source_id,)))
        except sqlite3.DatabaseError as error:
            raise InternalAudioStoreError("source read failed") from error
        return _source_from_row(row) if row is not None else None

    def read_record_page(
        self, source_id: str, after_ordinal: int = -1, limit: int = DEFAULT_CHUNK_LIMIT
    ) -> tuple[CaptureRecord, ...]:
        source_id = _source_id(source_id)
        if type(after_ordinal) is not int or after_ordinal < -1 or after_ordinal > MAX_PROTOCOL_UINT32:
            raise InternalAudioStoreError("record page cursor is invalid")
        limit = _page_limit(limit, self.chunk_limit)
        try:
            with closing(self.state_db.open_connection(read_only=True)) as connection:
                rows = _fetchall(
                    connection.execute(
                        "SELECT source_id, ordinal, device_timestamp, payload, utc_timestamp, mapping_revision "
                        "FROM capture_record WHERE source_id = ? AND ordinal > ? ORDER BY ordinal LIMIT ?",
                        (source_id, after_ordinal, limit),
                    )
                )
        except sqlite3.DatabaseError as error:
            raise InternalAudioStoreError("record page read failed") from error
        return tuple(_record_from_row(row) for row in rows)

    def commit_drain(
        self,
        source_id: str,
        expected_ordinal: int,
        records: Iterable[bytes],
        checkpoint: DrainCheckpoint,
    ) -> int:
        self._check_writer()
        source_id = _source_id(source_id)
        expected = _uint32(expected_ordinal, "expected ordinal")
        raw_records = _bounded_records(records, self.chunk_limit)
        if not 0 < len(raw_records) <= self.chunk_limit:
            raise InternalAudioStoreError("drain size is outside the configured bound")
        expected_sha256 = _sha256(checkpoint.expected_sha256) if checkpoint.expected_sha256 is not None else None
        source = self._require_source(source_id)
        _validate_receiving_leg(source, expected, len(raw_records), checkpoint.leg_generation)
        overlap_count = max(0, min(len(raw_records), source.committed_count - expected))
        if expected < source.committed_count:
            return self._replay_or_append(
                _ReplayRequest(source, expected, raw_records, expected_sha256, checkpoint.tail, overlap_count)
            )
        if expected != source.committed_count:
            raise InternalAudioStoreError("drain ordinal is not the committed frontier")
        return self._append_drain(source, expected, raw_records, expected_sha256, checkpoint.tail)

    def _append_drain(
        self,
        source: CaptureSource,
        expected: int,
        raw_records: tuple[bytes, ...],
        expected_sha256: str | None,
        tail: CaptureTail | None,
    ) -> int:
        new_count = expected + len(raw_records)
        if new_count > MAX_PROTOCOL_UINT32 or source.sequence_start + new_count > source.sequence_limit:
            raise InternalAudioStoreError("drain exceeds source range")
        digest_state = self._digest_state(source)
        parsed = tuple(_parse_record(source.source_id, expected + index, raw) for index, raw in enumerate(raw_records))
        proposed_digest = digest_state.copy()
        for record in parsed:
            proposed_digest.update(record.raw_bytes)
        digest = proposed_digest.hexdigest()
        if expected_sha256 is not None and digest != expected_sha256:
            raise InternalAudioStoreError("drain digest does not match exact source rows")
        previous_tail = self._read_tail(source.source_id)
        desired_tail = self._resolve_tail(source, previous_tail, tail, new_count, parsed)
        connection = self._require_connection()
        try:
            connection.execute("BEGIN IMMEDIATE")
            self._recheck_source(connection, source)
            self._recheck_tail(connection, source.source_id, previous_tail)
            connection.executemany(
                "INSERT INTO capture_record(source_id, ordinal, device_timestamp, payload) VALUES (?, ?, ?, ?)",
                ((record.source_id, record.ordinal, record.device_timestamp, record.payload) for record in parsed),
            )
            connection.execute(
                "UPDATE capture_source SET committed_count = ?, committed_sha256 = ? WHERE source_id = ?",
                (new_count, digest, source.source_id),
            )
            self._write_tail(connection, source.source_id, desired_tail, previous_tail)
            self._commit()
            self._publish_digest(source.source_id, new_count, digest, proposed_digest)
        except InternalAudioStoreError:
            self._rollback()
            raise
        except sqlite3.IntegrityError as error:
            self._rollback()
            raise InternalAudioStoreError("drain violates source constraints") from error
        except sqlite3.DatabaseError as error:
            self._rollback()
            return self._recover_ambiguous(
                source, expected, parsed, new_count, digest, desired_tail, previous_tail, error
            )
        return new_count

    def validate_source_stream(
        self, records: Iterable[bytes], *, expected_count: int | None = None, expected_sha256: str | None = None
    ) -> tuple[int, str]:
        self._check_thread()
        return validate_source_stream(records, expected_count=expected_count, expected_sha256=expected_sha256)

    def _replay_or_append(self, request: _ReplayRequest) -> int:
        source = request.source
        expected = request.expected
        records = request.records
        expected_sha256 = request.expected_sha256
        tail = request.tail
        overlap_count = request.overlap_count
        overlap = self.read_record_page(source.source_id, expected - 1, overlap_count)
        if len(overlap) != overlap_count or any(
            row.raw_bytes != raw for row, raw in zip(overlap, records[:overlap_count], strict=True)
        ):
            self._block_replay_conflict(source, expected * AUDIO_RECORD_SIZE, b"".join(records))
            raise InternalAudioStoreError("drain replay conflicts with accepted records")
        suffix = records[overlap_count:]
        if not suffix:
            checkpoint_count = expected + len(records)
            if expected_sha256 is not None:
                checkpoint_digest = (
                    source.committed_sha256
                    if checkpoint_count == source.committed_count
                    else self._prefix_digest(source.source_id, checkpoint_count)
                )
                if expected_sha256 != checkpoint_digest:
                    raise InternalAudioStoreError("drain replay checkpoint conflicts with accepted source")
            previous_tail = self._read_tail(source.source_id)
            desired_tail = self._resolve_tail(source, previous_tail, tail, source.committed_count, ())
            if desired_tail != previous_tail:
                self._commit_replay_tail(source, previous_tail, desired_tail)
            return source.committed_count
        return self._append_replay(source, suffix, expected_sha256, tail)

    def _prefix_digest(self, source_id: str, count: int) -> str:
        digest = hashlib.sha256()
        ordinal = 0
        while ordinal < count:
            page = self.read_record_page(source_id, ordinal - 1, min(self.chunk_limit, count - ordinal))
            if not page or any(row.ordinal != ordinal + index for index, row in enumerate(page)):
                raise InternalAudioStoreError("replay prefix is not contiguous")
            for record in page:
                digest.update(record.raw_bytes)
            ordinal += len(page)
        return digest.hexdigest()

    def _commit_replay_tail(
        self, source: CaptureSource, previous: CaptureTail | None, desired: CaptureTail | None
    ) -> None:
        connection = self._require_connection()
        try:
            connection.execute("BEGIN IMMEDIATE")
            self._recheck_source(connection, source)
            self._recheck_tail(connection, source.source_id, previous)
            self._write_tail(connection, source.source_id, desired, previous)
            self._commit()
        except InternalAudioStoreError:
            self._rollback()
            raise
        except sqlite3.DatabaseError as error:
            self._rollback()
            self._recover_ambiguous(
                source,
                source.committed_count,
                (),
                source.committed_count,
                source.committed_sha256,
                desired,
                previous,
                error,
            )

    def _append_replay(
        self,
        source: CaptureSource,
        suffix: tuple[bytes, ...],
        expected_sha256: str | None,
        tail: CaptureTail | None,
    ) -> int:
        new_count = source.committed_count + len(suffix)
        if new_count > MAX_PROTOCOL_UINT32 or source.sequence_start + new_count > source.sequence_limit:
            raise InternalAudioStoreError("drain exceeds source range")
        parsed = tuple(
            _parse_record(source.source_id, source.committed_count + index, raw) for index, raw in enumerate(suffix)
        )
        digest_state = self._digest_state(source)
        proposed_digest = digest_state.copy()
        for record in parsed:
            proposed_digest.update(record.raw_bytes)
        digest = proposed_digest.hexdigest()
        if expected_sha256 is not None and digest != expected_sha256:
            raise InternalAudioStoreError("drain digest does not match exact source rows")
        previous_tail = self._read_tail(source.source_id)
        desired_tail = self._resolve_tail(source, previous_tail, tail, new_count, parsed)
        connection = self._require_connection()
        try:
            connection.execute("BEGIN IMMEDIATE")
            self._recheck_source(connection, source)
            self._recheck_tail(connection, source.source_id, previous_tail)
            connection.executemany(
                "INSERT INTO capture_record(source_id, ordinal, device_timestamp, payload) VALUES (?, ?, ?, ?)",
                ((record.source_id, record.ordinal, record.device_timestamp, record.payload) for record in parsed),
            )
            connection.execute(
                "UPDATE capture_source SET committed_count = ?, committed_sha256 = ? WHERE source_id = ?",
                (new_count, digest, source.source_id),
            )
            self._write_tail(connection, source.source_id, desired_tail, previous_tail)
            self._commit()
            self._publish_digest(source.source_id, new_count, digest, proposed_digest)
        except InternalAudioStoreError:
            self._rollback()
            raise
        except sqlite3.IntegrityError as error:
            self._rollback()
            raise InternalAudioStoreError("drain violates source constraints") from error
        except sqlite3.DatabaseError as error:
            self._rollback()
            return self._recover_ambiguous(
                source, source.committed_count, parsed, new_count, digest, desired_tail, previous_tail, error
            )
        return new_count

    def _digest_state(self, source: CaptureSource) -> _HashState:
        key = (source.source_id, source.committed_count, source.committed_sha256)
        cached = self._digest_cache.get(source.source_id)
        if cached is not None and cached[:2] == key[1:]:
            return cached[2].copy()
        digest: _HashState = hashlib.sha256()
        count = 0
        try:
            with closing(self.state_db.open_connection(read_only=True)) as connection:
                while True:
                    with closing(
                        connection.execute(
                            "SELECT ordinal, device_timestamp, payload FROM capture_record "
                            "WHERE source_id = ? AND ordinal >= ? ORDER BY ordinal LIMIT ?",
                            (source.source_id, count, self.chunk_limit),
                        )
                    ) as cursor:
                        rows = _fetchall(cursor)
                    if not rows:
                        break
                    for row in rows:
                        ordinal = _int_value(_value(row, 0))
                        timestamp = _uint32(_int_value(_value(row, 1)), "stored device timestamp")
                        payload = _bytes(_value(row, 2))
                        if ordinal != count or len(payload) != AUDIO_PAYLOAD_SIZE or count >= source.committed_count:
                            raise InternalAudioStoreError("source rows do not match committed frontier")
                        digest.update(timestamp.to_bytes(4, "big") + payload)
                        count += 1
        except sqlite3.DatabaseError as error:
            raise InternalAudioStoreError("source digest read failed") from error
        if count != source.committed_count:
            raise InternalAudioStoreError("source rows do not match committed count")
        if digest.hexdigest() != source.committed_sha256:
            raise InternalAudioStoreError("stored source digest does not match exact rows")
        self._digest_cache.clear()
        self._digest_cache[source.source_id] = (source.committed_count, source.committed_sha256, digest.copy())
        return digest

    def _publish_digest(self, source_id: str, count: int, digest: str, state: _HashState) -> None:
        self._digest_cache.clear()
        self._digest_cache[source_id] = (count, digest, state.copy())

    def _discard_digest(self, source_id: str) -> None:
        self._digest_cache.pop(source_id, None)

    def _recheck_source(self, connection: sqlite3.Connection, expected: CaptureSource) -> None:
        self._check_source_snapshot(connection, expected)
        _validate_receiving_leg(
            expected, expected.current_leg_start - expected.sequence_start, 1, expected.current_leg_generation
        )

    def _check_source_snapshot(self, connection: sqlite3.Connection, expected: CaptureSource) -> None:
        row = _fetchone(connection.execute("SELECT * FROM capture_source WHERE source_id = ?", (expected.source_id,)))
        if row is None or _source_from_row(row) != expected:
            raise InternalAudioStoreError("source changed during drain")

    def _block_replay_conflict(
        self, source: CaptureSource, byte_offset: int, rejected: bytes, *, reason: str = "replay_mismatch"
    ) -> None:
        if not rejected or len(rejected) > self.chunk_limit * AUDIO_RECORD_SIZE:
            raise InternalAudioStoreError("rejected evidence is outside the bounded chunk")
        connection = self._require_connection()
        try:
            connection.execute("BEGIN IMMEDIATE")
            self._recheck_source(connection, source)
            row = _fetchone(
                connection.execute(
                    "SELECT COALESCE(MAX(chunk_ordinal), -1) FROM capture_conflict WHERE source_id = ?",
                    (source.source_id,),
                )
            )
            if row is None:
                raise InternalAudioStoreError("conflict evidence ordinal is unavailable")
            ordinal = _int_value(_value(row, 0)) + 1
            if ordinal > MAX_PROTOCOL_UINT32:
                raise InternalAudioStoreError("conflict evidence ordinal is exhausted")
            connection.execute(
                "INSERT INTO capture_conflict(source_id, chunk_ordinal, leg_generation, "
                "absolute_byte_offset, reason, bytes) VALUES (?, ?, ?, ?, ?, ?)",
                (
                    source.source_id,
                    ordinal,
                    source.current_leg_generation,
                    byte_offset,
                    reason,
                    rejected,
                ),
            )
            connection.execute(
                "UPDATE capture_source SET lifecycle = 'blocked', block_reason = ? WHERE source_id = ?",
                (reason, source.source_id),
            )
            self._commit()
        except InternalAudioStoreError:
            self._rollback()
            raise
        except sqlite3.DatabaseError as error:
            self._rollback()
            raise InternalAudioStoreError("replay conflict could not be retained") from error

    def _resolve_tail(
        self,
        source: CaptureSource,
        previous: CaptureTail | None,
        incoming: CaptureTail | None,
        new_count: int,
        appended: tuple[CaptureRecord, ...],
    ) -> CaptureTail | None:
        _validate_tail(incoming, source, new_count)
        self._check_previous_tail(source, previous, new_count, appended)
        if incoming is not None and incoming.complete_high_water < new_count:
            ordinal = incoming.complete_high_water
            if ordinal < source.committed_count:
                page = self.read_record_page(source.source_id, ordinal - 1, 1)
                if len(page) != 1 or page[0].ordinal != ordinal:
                    raise InternalAudioStoreError("accepted row for replay tail is missing")
                full_record = page[0].raw_bytes
            else:
                full_record = appended[ordinal - source.committed_count].raw_bytes
            self._assert_tail_prefix(source, incoming, full_record)
            incoming = None
        if incoming is None:
            return previous if new_count == source.committed_count else None
        if previous is None or incoming.byte_offset > previous.byte_offset:
            return incoming
        if incoming.byte_offset < previous.byte_offset:
            return previous
        if previous.data[: len(incoming.data)] != incoming.data[: len(previous.data)]:
            self._block_replay_conflict(source, incoming.byte_offset, incoming.data, reason="tail_mismatch")
            raise InternalAudioStoreError("replayed tail conflicts with accepted bytes")
        return incoming if len(incoming.data) > len(previous.data) else previous

    def _check_previous_tail(
        self,
        source: CaptureSource,
        previous: CaptureTail | None,
        new_count: int,
        appended: tuple[CaptureRecord, ...],
    ) -> None:
        if previous is None:
            return
        if previous.byte_offset != source.committed_count * AUDIO_RECORD_SIZE:
            raise InternalAudioStoreError("stored tail is not at the committed frontier")
        if new_count > source.committed_count:
            self._assert_tail_prefix(source, previous, appended[0].raw_bytes)

    def _assert_tail_prefix(self, source: CaptureSource, tail: CaptureTail, full_record: bytes) -> None:
        if full_record.startswith(tail.data):
            return
        self._block_replay_conflict(source, tail.byte_offset, full_record, reason="tail_mismatch")
        raise InternalAudioStoreError("completed record conflicts with accepted tail")

    def _recheck_tail(self, connection: sqlite3.Connection, source_id: str, previous: CaptureTail | None) -> None:
        if self._tail_from_connection(connection, source_id) != previous:
            raise InternalAudioStoreError("tail changed during drain")

    def _write_tail(
        self,
        connection: sqlite3.Connection,
        source_id: str,
        desired: CaptureTail | None,
        previous_tail: CaptureTail | None,
    ) -> None:
        if desired == previous_tail:
            return
        if desired is not None:
            connection.execute(
                "INSERT INTO capture_tail(source_id, leg_generation, complete_high_water, received_high_water, byte_offset, bytes) "
                "VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT(source_id) DO UPDATE SET leg_generation=excluded.leg_generation, "
                "complete_high_water=excluded.complete_high_water, received_high_water=excluded.received_high_water, "
                "byte_offset=excluded.byte_offset, bytes=excluded.bytes",
                (
                    source_id,
                    desired.leg_generation,
                    desired.complete_high_water,
                    desired.received_high_water,
                    desired.byte_offset,
                    desired.data,
                ),
            )
        else:
            connection.execute("DELETE FROM capture_tail WHERE source_id = ?", (source_id,))

    def _recover_ambiguous(  # noqa: PLR0913, PLR0917
        self,
        source: CaptureSource,
        expected: int,
        records: tuple[CaptureRecord, ...],
        new_count: int,
        digest: str,
        tail: CaptureTail | None,
        previous_tail: CaptureTail | None,
        error: sqlite3.DatabaseError,
    ) -> int:
        self._discard_digest(source.source_id)
        try:
            self._reopen()
            current = self.get_source(source.source_id)
            actual_tail = self._read_tail(source.source_id)
            proposed_rows = self.read_record_page(source.source_id, expected - 1, len(records)) if records else ()
            authenticated = self._digest_state(current) if current is not None else None
        except (sqlite3.DatabaseError, StateDBError) as inspection_error:
            self._fail_stop(f"ambiguous drain could not be inspected: {inspection_error}")
        if current is None or authenticated is None:
            self._fail_stop("source disappeared after ambiguous commit")
        exact_rows = len(proposed_rows) == len(records) and all(
            row.raw_bytes == value.raw_bytes for row, value in zip(proposed_rows, records, strict=True)
        )
        proposed = replace(source, committed_count=new_count, committed_sha256=digest)
        if current == proposed and exact_rows and actual_tail == tail:
            self._publish_digest(source.source_id, new_count, digest, authenticated)
            return new_count
        if current == source and not proposed_rows and actual_tail == previous_tail:
            self._publish_digest(source.source_id, source.committed_count, source.committed_sha256, authenticated)
            raise InternalAudioStoreError("ambiguous drain was rolled back") from error
        self._fail_stop("ambiguous drain commit could not be reconciled")
        raise AssertionError("fail-stop must raise")

    def _recover_create(self, request: _CreateRequest, error: sqlite3.DatabaseError) -> CaptureSource:
        try:
            self._reopen()
            source = self.get_source(request.source_id)
        except (sqlite3.DatabaseError, StateDBError) as inspection_error:
            self._fail_stop(f"ambiguous source creation could not be inspected: {inspection_error}")
        expected = CaptureSource(
            request.source_id,
            request.source_type,
            "prepared",
            request.start,
            request.limit,
            None,
            None,
            None,
            None,
            request.generation,
            request.start,
            0,
            0,
            _EMPTY_SHA256,
            None,
        )
        if source is not None and source == expected:
            return source
        if source is None:
            raise InternalAudioStoreError("ambiguous source creation was rolled back") from error
        self._fail_stop("ambiguous source creation could not be reconciled")
        raise AssertionError("fail-stop must raise")

    def _read_tail(self, source_id: str) -> CaptureTail | None:
        try:
            with closing(self.state_db.open_connection(read_only=True)) as connection:
                return self._tail_from_connection(connection, source_id)
        except sqlite3.DatabaseError as error:
            raise InternalAudioStoreError("tail read failed") from error

    def _tail_from_connection(self, connection: sqlite3.Connection, source_id: str) -> CaptureTail | None:
        row = _fetchone(
            connection.execute(
                "SELECT leg_generation, complete_high_water, received_high_water, byte_offset, bytes "
                "FROM capture_tail WHERE source_id = ?",
                (source_id,),
            )
        )
        if row is None:
            return None
        return CaptureTail(
            _int_value(_value(row, 0)),
            _int_value(_value(row, 1)),
            _int_value(_value(row, 2)),
            _int_value(_value(row, 3)),
            _bytes(_value(row, 4)),
        )

    def _commit(self) -> None:
        self._require_connection().commit()

    def _rollback(self) -> None:
        with suppress(sqlite3.DatabaseError):
            self._require_connection().rollback()

    def _reopen(self) -> None:
        if self._connection is not None:
            self._connection.close()
        self._connection = self.state_db.open_connection()
        self._connection.execute(f"PRAGMA busy_timeout = {self._busy_timeout_ms}")

    def _check_thread(self) -> None:
        if threading.get_ident() != self._owner_thread:
            raise InternalAudioStoreError("audio store is thread-affine")

    def _check_writer(self) -> None:
        self._check_thread()
        if self._failed:
            raise InternalAudioStoreError("audio store is fail-stopped")

    def _fail_stop(self, message: str) -> NoReturn:
        self._failed = True
        raise InternalAudioStoreError(message)

    def _require_connection(self) -> sqlite3.Connection:
        if self._connection is None:
            raise InternalAudioStoreError("audio store is closed")
        return self._connection


def _phase(source: CaptureSource) -> AudioPhase:
    try:
        return AudioPhase(source.lifecycle)
    except ValueError as error:
        raise InternalAudioStoreError("source lifecycle is unsupported") from error


def _phase_after(phase: AudioPhase | None, event: AudioEvent, required: AudioAction | None) -> AudioPhase:
    decision = transition(phase, event)
    if required is not None and decision.action is not required:
        raise InternalAudioStoreError(decision.reason)
    if decision.action not in {AudioAction.PERSIST, AudioAction.COMMIT_BARRIER, AudioAction.IDEMPOTENT}:
        raise InternalAudioStoreError(decision.reason)
    if decision.next_phase is None:
        raise InternalAudioStoreError("source lifecycle transition has no next phase")
    return decision.next_phase


def _validate_receiving_leg(source: CaptureSource, expected: int, count: int, leg_generation: int) -> None:
    if type(leg_generation) is not int or leg_generation != source.current_leg_generation:
        raise InternalAudioStoreError("drain leg generation is stale")
    decision = transition(_phase(source), Data())
    if decision.action is not AudioAction.PERSIST:
        raise InternalAudioStoreError("drain requires a receiving source")
    if (
        source.prepared_start != source.current_leg_start
        or source.prepared_count != source.current_leg_count
        or source.read_begin_start != source.current_leg_start
        or source.read_begin_count != source.current_leg_count
        or source.current_leg_count <= 0
    ):
        raise InternalAudioStoreError("receiving source has no matching durable leg")
    sequence_start = source.sequence_start + expected
    if (
        count <= 0
        or sequence_start < source.current_leg_start
        or sequence_start + count > source.current_leg_start + source.current_leg_count
    ):
        raise InternalAudioStoreError("drain is outside the bound READ leg")


def _source_from_row(row: sqlite3.Row) -> CaptureSource:
    return CaptureSource(
        source_id=str(_value(row, "source_id")),
        source_type=str(_value(row, "source_type")),
        lifecycle=str(_value(row, "lifecycle")),
        sequence_start=decode_uint64(_bytes(_value(row, "sequence_start"))),
        sequence_limit=decode_uint64(_bytes(_value(row, "sequence_limit"))),
        prepared_start=_optional_uint64(_value(row, "prepared_start")),
        prepared_count=_optional_uint32(_value(row, "prepared_count")),
        read_begin_start=_optional_uint64(_value(row, "read_begin_start")),
        read_begin_count=_optional_uint32(_value(row, "read_begin_count")),
        current_leg_generation=_int_value(_value(row, "current_leg_generation")),
        current_leg_start=decode_uint64(_bytes(_value(row, "current_leg_start"))),
        current_leg_count=_int_value(_value(row, "current_leg_count")),
        committed_count=_int_value(_value(row, "committed_count")),
        committed_sha256=str(_value(row, "committed_sha256")),
        closure_frontier=_optional_uint64(_value(row, "closure_frontier")),
    )


def _record_from_row(row: sqlite3.Row) -> CaptureRecord:
    return CaptureRecord(
        str(_value(row, 0)),
        _int_value(_value(row, 1)),
        _int_value(_value(row, 2)),
        _bytes(_value(row, 3)),
        _optional_uint32(_value(row, 4)),
        _optional_sha(_value(row, 5)),
    )


def _parse_record(source_id: str, ordinal: int, raw: bytes) -> CaptureRecord:
    return CaptureRecord(source_id, ordinal, int.from_bytes(raw[:4], "big"), raw[4:])


def _record_bytes(value: bytes) -> bytes:
    if not isinstance(value, bytes) or len(value) != AUDIO_RECORD_SIZE:
        raise InternalAudioStoreError("audio record must be exactly 444 bytes")
    return value


def _bounded_records(records: Iterable[bytes], limit: int) -> tuple[bytes, ...]:
    materialized: list[bytes] = []
    try:
        iterator = iter(records)
        for _ in range(limit + 1):
            try:
                materialized.append(_record_bytes(next(iterator)))
            except StopIteration:
                break
    except TypeError as error:
        raise InternalAudioStoreError("drain records are not iterable") from error
    if not 0 < len(materialized) <= limit:
        raise InternalAudioStoreError("drain size is outside the configured bound")
    return tuple(materialized)


def _fetchone(cursor: sqlite3.Cursor) -> sqlite3.Row | None:
    result = cast(object, cursor.fetchone())
    if result is not None and not isinstance(result, sqlite3.Row):
        raise InternalAudioStoreError("audio database returned an invalid row")
    return result


def _fetchall(cursor: sqlite3.Cursor) -> list[sqlite3.Row]:
    result = cast(object, cursor.fetchall())
    if not isinstance(result, list) or any(not isinstance(row, sqlite3.Row) for row in result):
        raise InternalAudioStoreError("audio database returned invalid rows")
    return result


def _value(row: sqlite3.Row, column: int | str) -> object:
    return cast(object, row[column])


def _bytes(value: object) -> bytes:
    if not isinstance(value, bytes):
        raise InternalAudioStoreError("audio database blob is invalid")
    return value


def _int_value(value: object) -> int:
    if type(value) is not int:
        raise InternalAudioStoreError("audio database integer is invalid")
    return value


def _optional_sha(value: object) -> str | None:
    if value is not None and not isinstance(value, str):
        raise InternalAudioStoreError("audio database mapping revision is invalid")
    return value


def _validate_tail(tail: CaptureTail | None, source: CaptureSource, new_count: int) -> None:
    if tail is None:
        return
    if type(tail.leg_generation) is not int or tail.leg_generation != source.current_leg_generation:
        raise InternalAudioStoreError("tail generation does not match source")
    if (
        type(tail.complete_high_water) is not int
        or tail.complete_high_water < source.current_leg_start - source.sequence_start
        or tail.complete_high_water > new_count
    ):
        raise InternalAudioStoreError("tail complete frontier does not match drain")
    if type(tail.received_high_water) is not int or tail.received_high_water < 0:
        raise InternalAudioStoreError("tail received frontier is invalid")
    if type(tail.byte_offset) is not int or tail.byte_offset != tail.complete_high_water * AUDIO_RECORD_SIZE:
        raise InternalAudioStoreError("tail byte offset is invalid")
    if not isinstance(tail.data, bytes) or not 1 <= len(tail.data) <= TAIL_MAX_BYTES:
        raise InternalAudioStoreError("tail bytes are invalid")
    if tail.received_high_water != tail.byte_offset + len(tail.data):
        raise InternalAudioStoreError("tail received frontier does not match bytes")


def _source_id(value: str) -> str:
    if (
        not isinstance(value, str)
        or len(value) != SOURCE_ID_LENGTH
        or value != value.lower()
        or any(char not in "0123456789abcdef" for char in value)
    ):
        raise InternalAudioStoreError("source identity is invalid")
    return value


def _sha256(value: str) -> str:
    if (
        not isinstance(value, str)
        or len(value) != SHA256_HEX_LENGTH
        or value != value.lower()
        or any(char not in "0123456789abcdef" for char in value)
    ):
        raise InternalAudioStoreError("SHA-256 digest is invalid")
    return value


def _uint32(value: int, label: str) -> int:
    if type(value) is not int or not 0 <= value <= MAX_PROTOCOL_UINT32:
        raise InternalAudioStoreError(f"{label} is invalid")
    return value


def _uint64(value: int, label: str) -> int:
    if type(value) is not int or not 0 <= value <= 2**64 - 1:
        raise InternalAudioStoreError(f"{label} is invalid")
    return value


def _optional_uint64(value: object) -> int | None:
    return None if value is None else decode_uint64(_bytes(value))


def _optional_uint32(value: object) -> int | None:
    return None if value is None else _uint32(_int_value(value), "stored uint32")


def _chunk_limit(value: int) -> int:
    if type(value) is not int or not 1 <= value <= MAX_CHUNK_LIMIT:
        raise InternalAudioStoreError("chunk limit is invalid")
    return value


def _page_limit(value: int, maximum: int) -> int:
    if type(value) is not int or not 1 <= value <= maximum:
        raise InternalAudioStoreError("record page limit is invalid")
    return value
