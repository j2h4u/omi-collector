"""Typed internal audio storage backed by the Collector state database."""

from __future__ import annotations

import hashlib
import math
import os
import sqlite3
import sys
import threading
import time
from collections.abc import Iterable, Iterator
from contextlib import AbstractContextManager, closing, contextmanager, suppress
from dataclasses import dataclass, replace
from typing import Final, NoReturn, Protocol, cast

from ...config import MAX_WRITER_CHUNK_RECORDS
from ..application.storage_fault import FatalInternalAudioStorageError, TransientInternalAudioStorageError
from ..domain.clock_state import ClockState
from ..domain.internal_audio_machine import (
    AudioAction,
    AudioEvent,
    AudioPhase,
    Create,
    Data,
    Done,
    Interruption,
    PrepareLeg,
    QuarantineAction,
    QuarantineDisposition,
    QuarantineOpened,
    ReadBegin,
    quarantine_transition,
    transition,
)
from .clock_segments import ClockSegmentMap
from .collector_state_db import (
    StateDB,
    StateDBError,
    StateTransaction,
    _decode,
    _encode,
    install_audio_schema,
    raise_if_sqlite_corrupt,
    raise_if_sqlite_storage_fault,
)

__all__ = [
    "AUDIO_PAYLOAD_SIZE",
    "AUDIO_RECORD_SIZE",
    "DEFAULT_CHUNK_LIMIT",
    "MAX_CHUNK_LIMIT",
    "CaptureRecord",
    "CaptureSource",
    "CaptureSourceIntegrityError",
    "CaptureSourceReadError",
    "CaptureTail",
    "DrainCheckpoint",
    "InternalAudioStore",
    "InternalAudioStoreError",
    "authenticate_capture_source",
    "capture_space_requirement",
    "clock_mapping_revision",
    "decode_sequence",
    "decode_uint64",
    "encode_sequence",
    "encode_uint64",
    "install_audio_schema",
    "refresh_utc_projection",
    "validate_source_stream",
]

AUDIO_RECORD_SIZE: Final = 444
AUDIO_PAYLOAD_SIZE: Final = 440
DEFAULT_CHUNK_LIMIT: Final = MAX_WRITER_CHUNK_RECORDS
MAX_CHUNK_LIMIT: Final = MAX_WRITER_CHUNK_RECORDS
MAX_PROTOCOL_UINT32: Final = 2**32 - 1
UINT64_BYTES: Final = 8
TAIL_MAX_BYTES: Final = 443
SOURCE_ID_LENGTH: Final = 32
SHA256_HEX_LENGTH: Final = 64
# The retained 684,816-record replay measured 636.6 database bytes per
# 444-byte record and a 127,736-byte journal high-water for K=128. Round both
# upward; the configured safety headroom is added by the staging facade.
_CAPTURE_DB_BYTES_PER_RECORD: Final = 640
_CAPTURE_JOURNAL_BYTES_PER_RECORD: Final = 1024
_CAPTURE_JOURNAL_MARGIN_PAGES: Final = 16
_DEFAULT_TERMINAL_RETENTION_SECONDS: Final = 72.0 * 60.0 * 60.0

_SOURCE_TYPES = frozenset({"attempt", "draft", "quarantine"})
_EMPTY_SHA256 = hashlib.sha256(b"").hexdigest()
_DEFAULT_CLOCK_PAYLOAD = _encode(ClockState().to_payload())


class InternalAudioStoreError(StateDBError):
    """A typed audio operation was refused or could not be made durable."""


class CaptureSourceIntegrityError(InternalAudioStoreError):
    """Canonical source rows do not match their durable authentication checkpoint."""


class CaptureSourceReadError(InternalAudioStoreError):
    """Canonical source authentication failed while reading SQLite state."""

    def __init__(self, message: str, *, sqlite_errorcode: int | None) -> None:
        super().__init__(message)
        self.sqlite_errorcode = sqlite_errorcode


def capture_space_requirement(state_db: StateDB, record_count: int, chunk_records: int) -> tuple[int, int]:
    """Return bounded capture demand and currently available database-filesystem bytes."""
    if type(record_count) is not int or record_count <= 0:
        raise ValueError("record count must be a positive integer")
    if type(chunk_records) is not int or chunk_records <= 0 or chunk_records > MAX_CHUNK_LIMIT:
        raise ValueError("chunk records must be within the typed writer bound")
    try:
        with closing(state_db.open_connection(read_only=True)) as connection, state_db.access(connection):
            page_size = _pragma_positive_int(connection, "page_size")
            freelist_count = _pragma_nonnegative_int(connection, "freelist_count")
        reusable_bytes = freelist_count * page_size
        database_growth = max(0, record_count * _CAPTURE_DB_BYTES_PER_RECORD - reusable_bytes)
        transaction_records = min(record_count, chunk_records)
        journal_bytes = (
            transaction_records * _CAPTURE_JOURNAL_BYTES_PER_RECORD + _CAPTURE_JOURNAL_MARGIN_PAGES * page_size
        )
        filesystem = os.statvfs(state_db.path.parent)
        available = filesystem.f_bavail * filesystem.f_frsize
    except (OSError, sqlite3.DatabaseError, StateDBError, TypeError, AttributeError) as error:
        raise_if_sqlite_storage_fault(error)
        raise InternalAudioStoreError("capture space reserve could not be inspected") from error
    return database_growth + journal_bytes, available


def _pragma_positive_int(connection: sqlite3.Connection, name: str) -> int:
    with closing(connection.execute(f"PRAGMA {name}")) as cursor:
        row = cast(tuple[object] | None, cursor.fetchone())
    if row is None or type(row[0]) is not int or row[0] <= 0:
        raise InternalAudioStoreError(f"SQLite {name} is invalid")
    return cast(int, row[0])


def _pragma_nonnegative_int(connection: sqlite3.Connection, name: str) -> int:
    with closing(connection.execute(f"PRAGMA {name}")) as cursor:
        row = cast(tuple[object] | None, cursor.fetchone())
    if row is None or type(row[0]) is not int or row[0] < 0:
        raise InternalAudioStoreError(f"SQLite {name} is invalid")
    return cast(int, row[0])


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


def clock_mapping_revision(state_db: StateDB) -> str:
    """Hash the exact authoritative clock payload bytes used by SQL clients."""

    try:
        with closing(state_db.open_connection(read_only=True)) as connection, state_db.access(connection):
            payload = _clock_payload_text(connection)
        return hashlib.sha256((payload if payload is not None else _DEFAULT_CLOCK_PAYLOAD).encode("utf-8")).hexdigest()
    except (OSError, sqlite3.DatabaseError, StateDBError, UnicodeError) as error:
        raise_if_sqlite_storage_fault(error)
        raise InternalAudioStoreError("clock mapping revision could not be read") from error


def refresh_utc_projection(state_db: StateDB, *, chunk_limit: int = DEFAULT_CHUNK_LIMIT) -> int:
    """Refresh at most one indexed chunk of stale unpublished UTC projections."""

    limit = _chunk_limit(chunk_limit)
    payload, revision, segments = _clock_projection_snapshot(state_db)
    try:
        with state_db.access():
            connection = state_db.open_connection()
            try:
                state_db.begin_exclusive(connection)
                if _clock_payload_text(connection) != payload:
                    connection.commit()
                    return 0
                rows = _stale_projection_rows(connection, revision, limit)
                for source_id, ordinal, sequence_start, device_timestamp, old_revision in rows:
                    utc_timestamp = _project_utc_timestamp(
                        segments,
                        decode_sequence(sequence_start) + ordinal,
                        device_timestamp,
                    )
                    changed = connection.execute(
                        "UPDATE capture_record SET utc_timestamp = ?, mapping_revision = ? "
                        "WHERE source_id = ? AND ordinal = ? AND mapping_revision IS ? "
                        "AND EXISTS (SELECT 1 FROM capture_source "
                        "WHERE capture_source.source_id = capture_record.source_id "
                        "AND capture_source.lifecycle NOT IN ('covered','retiring'))",
                        (utc_timestamp, revision, source_id, ordinal, old_revision),
                    )
                    try:
                        if changed.rowcount != 1:
                            raise InternalAudioStoreError("UTC projection row changed during refresh")
                    finally:
                        changed.close()
                connection.commit()
                return len(rows)
            except BaseException as error:
                state_db._rollback_managed_write(connection, error)
                raise
            finally:
                state_db._close_managed_write(connection)
    except InternalAudioStoreError:
        raise
    except (OSError, sqlite3.DatabaseError, StateDBError, UnicodeError, TypeError, ValueError) as error:
        raise_if_sqlite_storage_fault(error)
        raise InternalAudioStoreError("UTC projection refresh failed") from error


def _clock_projection_snapshot(state_db: StateDB) -> tuple[str | None, str, ClockSegmentMap]:
    try:
        with closing(state_db.open_connection(read_only=True)) as connection, state_db.access(connection):
            payload = _clock_payload_text(connection)
        authoritative = payload if payload is not None else _DEFAULT_CLOCK_PAYLOAD
        state = ClockState.from_payload(_decode(authoritative))
        return payload, hashlib.sha256(authoritative.encode("utf-8")).hexdigest(), ClockSegmentMap.from_state(state)
    except (OSError, sqlite3.DatabaseError, StateDBError, UnicodeError, TypeError, ValueError) as error:
        raise_if_sqlite_storage_fault(error)
        raise InternalAudioStoreError("clock mapping snapshot is invalid") from error


def _clock_payload_text(connection: sqlite3.Connection) -> str | None:
    row = _fetchone(connection.execute("SELECT payload FROM state WHERE kind = 'clock' AND id = 'current'"))
    if row is None:
        return None
    payload = cast(object, row[0])
    if not isinstance(payload, str):
        raise InternalAudioStoreError("clock mapping payload is invalid")
    return payload


def _stale_projection_rows(
    connection: sqlite3.Connection, revision: str, limit: int
) -> tuple[tuple[str, int, bytes, int, str | None], ...]:
    rows: list[tuple[str, int, bytes, int, str | None]] = []
    predicates: tuple[tuple[str, tuple[object, ...]], ...] = (
        ("record.mapping_revision IS NULL", ()),
        ("record.mapping_revision < ?", (revision,)),
        ("record.mapping_revision > ?", (revision,)),
    )
    for predicate, parameters in predicates:
        remaining = limit - len(rows)
        if remaining == 0:
            break
        selected = _fetchall(
            connection.execute(
                "SELECT record.source_id, record.ordinal, source.sequence_start, "
                "record.device_timestamp, record.mapping_revision "
                "FROM capture_record AS record JOIN capture_source AS source USING (source_id) "
                f"WHERE {predicate} AND source.lifecycle NOT IN ('covered','retiring') "
                "ORDER BY record.mapping_revision, record.source_id, record.ordinal LIMIT ?",
                (*parameters, remaining),
            )
        )
        rows.extend(
            (
                cast(str, row[0]),
                cast(int, row[1]),
                cast(bytes, row[2]),
                cast(int, row[3]),
                cast(str | None, row[4]),
            )
            for row in selected
        )
    return tuple(rows)


def _project_utc_timestamp(segments: ClockSegmentMap, sequence: int, device_timestamp: int) -> int | None:
    projected = segments.utc_for(sequence, device_timestamp)
    if projected is None:
        return None
    normalized = math.floor(projected + 0.5)
    return normalized if 0 <= normalized <= MAX_PROTOCOL_UINT32 else None


def read_capture_source(state_db: StateDB, source_id: str) -> CaptureSource | None:
    """Read one immutable source snapshot without creating a persistent writer."""

    source_id = _source_id(source_id)
    try:
        with closing(state_db.open_connection(read_only=True)) as connection, state_db.access(connection):
            return _read_capture_source(connection, source_id)
    except InternalAudioStoreError:
        raise
    except (sqlite3.DatabaseError, StateDBError) as error:
        raise_if_sqlite_storage_fault(error)
        raise InternalAudioStoreError("source read failed") from error


def authenticate_capture_source(
    state_db: StateDB,
    source_id: str,
    *,
    page_limit: int = DEFAULT_CHUNK_LIMIT,
) -> CaptureSource:
    """Authenticate canonical records against one stable source checkpoint."""

    source_id = _source_id(source_id)
    limit = _chunk_limit(page_limit)
    try:
        with closing(state_db.open_connection(read_only=True)) as connection, state_db.access(connection):
            source = _read_capture_source(connection, source_id)
            if source is None:
                raise InternalAudioStoreError("source does not exist")
            count, digest, integrity_error = _scan_authenticated_records(connection, source_id, limit)
            current = _read_capture_source(connection, source_id)
    except InternalAudioStoreError:
        raise
    except (sqlite3.DatabaseError, StateDBError) as error:
        raise_if_sqlite_storage_fault(error)
        raise _capture_source_read_error(error) from error
    except OSError as error:
        raise InternalAudioStoreError("source record authentication failed") from error
    if current != source:
        raise InternalAudioStoreError("source changed during record authentication")
    if integrity_error is not None:
        raise integrity_error
    if count != source.committed_count:
        raise CaptureSourceIntegrityError("source record count does not match checkpoint")
    if digest != source.committed_sha256:
        raise CaptureSourceIntegrityError("source record digest does not match checkpoint")
    return source


def _capture_source_read_error(error: sqlite3.DatabaseError | StateDBError) -> CaptureSourceReadError:
    code = getattr(error, "sqlite_errorcode", None)
    return CaptureSourceReadError(
        "source record authentication failed",
        sqlite_errorcode=code if type(code) is int else None,
    )


def _scan_authenticated_records(
    connection: sqlite3.Connection, source_id: str, limit: int
) -> tuple[int, str, CaptureSourceIntegrityError | None]:
    count = 0
    digest = hashlib.sha256()
    while True:
        with closing(
            connection.execute(
                "SELECT ordinal, device_timestamp, payload FROM capture_record "
                "WHERE source_id = ? AND ordinal > ? ORDER BY ordinal LIMIT ?",
                (source_id, count - 1, limit),
            )
        ) as cursor:
            rows = cast(list[tuple[object, object, object]], cursor.fetchall())
        if not rows:
            return count, digest.hexdigest(), None
        for ordinal_value, timestamp_value, payload_value in rows:
            try:
                digest.update(_authenticated_record_bytes(ordinal_value, timestamp_value, payload_value, count))
            except CaptureSourceIntegrityError as error:
                return count, digest.hexdigest(), error
            count += 1


def _authenticated_record_bytes(
    ordinal_value: object, timestamp_value: object, payload_value: object, expected: int
) -> bytes:
    if type(ordinal_value) is not int or not 0 <= ordinal_value <= MAX_PROTOCOL_UINT32:
        raise CaptureSourceIntegrityError("source record ordinal is invalid")
    ordinal = ordinal_value
    if ordinal != expected:
        raise CaptureSourceIntegrityError("source record ordinals are not contiguous")
    if type(timestamp_value) is not int or not 0 <= timestamp_value <= MAX_PROTOCOL_UINT32:
        raise CaptureSourceIntegrityError("source record timestamp is invalid")
    if not isinstance(payload_value, bytes) or len(payload_value) != AUDIO_PAYLOAD_SIZE:
        raise CaptureSourceIntegrityError("source record payload layout is invalid")
    return timestamp_value.to_bytes(4, "big") + payload_value


class InternalAudioStore:
    """A thread-owned persistent writer plus short-lived read connections."""

    def __init__(
        self,
        state_db: StateDB,
        *,
        chunk_limit: int = DEFAULT_CHUNK_LIMIT,
        busy_deadline: float = 5.0,
        terminal_retention_seconds: float = _DEFAULT_TERMINAL_RETENTION_SECONDS,
    ) -> None:
        self.state_db = state_db
        self.chunk_limit = _chunk_limit(chunk_limit)
        if isinstance(busy_deadline, bool) or not isinstance(busy_deadline, (int, float)) or busy_deadline <= 0:
            raise InternalAudioStoreError("busy deadline is invalid")
        self.busy_deadline = float(busy_deadline)
        if (
            isinstance(terminal_retention_seconds, bool)
            or not isinstance(terminal_retention_seconds, (int, float))
            or terminal_retention_seconds <= 0
        ):
            raise InternalAudioStoreError("terminal retention is invalid")
        self._terminal_retention_ns = int(float(terminal_retention_seconds) * 1_000_000_000)
        self._owner_thread = threading.get_ident()
        self._failed = False
        self._busy_timeout_ms = max(1, min(5_000, int(self.busy_deadline * 1_000)))
        self._digest_cache: dict[str, tuple[int, str, _HashState]] = {}
        self._connection: sqlite3.Connection | None = None
        self._publication_reader: sqlite3.Connection | None = None
        self._write_access: AbstractContextManager[None] | None = None
        try:
            self._connection = state_db.open_connection()
            with state_db.access(self._connection):
                self._connection.execute(f"PRAGMA busy_timeout = {self._busy_timeout_ms}").close()
        except sqlite3.DatabaseError as error:
            if self._connection is not None:
                with suppress(sqlite3.DatabaseError):
                    self._connection.close()
                self._connection = None
            raise_if_sqlite_storage_fault(error)
            raise InternalAudioStoreError("audio writer connection setup failed") from error

    def close(self) -> None:
        self._check_thread()
        if self._write_access is not None:
            self._rollback()
        if self._publication_reader is not None:
            self._publication_reader.close()
            self._publication_reader = None
        if self._connection is not None:
            with self.state_db.access(self._connection):
                self._connection.close()
            self._connection = None

    @contextmanager
    def publication_reader(self) -> Iterator[None]:
        """Reuse one autocommit reader across a bounded publication call."""

        self._check_writer()
        if self._publication_reader is not None:
            raise InternalAudioStoreError("publication reader is already active")
        try:
            self._publication_reader = self.state_db.open_connection(read_only=True)
            yield
        except InternalAudioStoreError:
            raise
        except (OSError, sqlite3.DatabaseError, StateDBError) as error:
            raise_if_sqlite_storage_fault(error)
            raise InternalAudioStoreError("publication reader failed") from error
        finally:
            if self._publication_reader is not None:
                self._publication_reader.close()
                self._publication_reader = None

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
            self._start_write(connection)
            state = StateTransaction(connection)
            yield state, connection
            state.finish()
            self._commit()
        except sqlite3.DatabaseError as error:
            self._rollback_after_error(error)
            try:
                self._reopen()
            except (sqlite3.DatabaseError, StateDBError, TransientInternalAudioStorageError) as inspection_error:
                raise_if_sqlite_corrupt(inspection_error)
                self._fail_stop("publication commit could not be inspected")
            raise InternalAudioStoreError(
                "publication commit requires durable-state inspection",
                sqlite_errorcode=getattr(error, "sqlite_errorcode", None),
            ) from error
        except BaseException:
            self._rollback()
            raise

    def __enter__(self) -> InternalAudioStore:
        return self

    def __exit__(self, _exc_type: object, _exc_value: object, _traceback: object) -> None:
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
            self._start_write(connection)
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
            self._rollback_after_error(error)
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
            self._start_write(connection)
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
            self._rollback_after_error(error)
            raise_if_sqlite_storage_fault(error)
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
            self._start_write(connection)
            self._check_source_snapshot(connection, source)
            connection.execute(
                "UPDATE capture_source SET lifecycle=?, read_begin_start=?, read_begin_count=? WHERE source_id=?",
                (next_phase.value, encode_uint64(start), count, source.source_id),
            )
            self._commit()
        except (sqlite3.DatabaseError, InternalAudioStoreError) as error:
            self._rollback_after_error(error)
            raise_if_sqlite_storage_fault(error)
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
            self._start_write(connection)
            self._check_source_snapshot(connection, source)
            connection.execute(
                "UPDATE capture_source SET lifecycle=? WHERE source_id=?",
                (decision.next_phase.value, source.source_id),
            )
            self._commit()
        except (sqlite3.DatabaseError, InternalAudioStoreError) as error:
            self._rollback_after_error(error)
            raise_if_sqlite_storage_fault(error)
            raise InternalAudioStoreError("interruption was not confirmed durable") from error
        return self._require_source(source.source_id)

    def seal_source(
        self,
        source_id: str,
        *,
        leg_generation: int,
        expected_count: int,
        expected_sha256: str,
    ) -> CaptureSource:
        """Durably seal one exact complete source before device advancement."""

        self._check_writer()
        source = self._require_source(source_id)
        count = _uint32(expected_count, "seal count")
        digest = _sha256(expected_sha256)
        if type(leg_generation) is not int or leg_generation != source.current_leg_generation:
            raise InternalAudioStoreError("seal leg generation is stale")
        if (
            count != source.committed_count
            or digest != source.committed_sha256
            or source.sequence_start + count != source.sequence_limit
        ):
            raise InternalAudioStoreError("seal does not match the complete durable source")
        next_phase = _phase_after(_phase(source), Done(), AudioAction.PERSIST)
        if self._read_tail(source.source_id) is not None:
            raise InternalAudioStoreError("complete source still has a partial tail")
        authenticate_capture_source(self.state_db, source.source_id, page_limit=self.chunk_limit)
        proposed = replace(source, lifecycle=next_phase.value)
        connection = self._require_connection()
        try:
            self._start_write(connection)
            self._check_source_snapshot(connection, source)
            if self._tail_from_connection(connection, source.source_id) is not None:
                raise InternalAudioStoreError("complete source still has a partial tail")
            connection.execute(
                "UPDATE capture_source SET lifecycle = ? WHERE source_id = ?",
                (next_phase.value, source.source_id),
            )
            self._commit()
        except InternalAudioStoreError:
            self._rollback()
            raise
        except sqlite3.DatabaseError as error:
            self._rollback_after_error(error)
            return self._recover_seal(source, proposed, error)
        return proposed

    def _require_source(self, source_id: str) -> CaptureSource:
        source = self.get_source(source_id)
        if source is None:
            raise InternalAudioStoreError("source does not exist")
        return source

    def get_source(self, source_id: str) -> CaptureSource | None:
        source_id = _source_id(source_id)
        if self._publication_reader is not None:
            try:
                with self.state_db.access(self._publication_reader):
                    return _read_capture_source(self._publication_reader, source_id)
            except sqlite3.DatabaseError as error:
                raise_if_sqlite_storage_fault(error)
                raise InternalAudioStoreError("source read failed") from error
        return read_capture_source(self.state_db, source_id)

    def read_record_page(
        self, source_id: str, after_ordinal: int = -1, limit: int = DEFAULT_CHUNK_LIMIT
    ) -> tuple[CaptureRecord, ...]:
        source_id = _source_id(source_id)
        if type(after_ordinal) is not int or after_ordinal < -1 or after_ordinal > MAX_PROTOCOL_UINT32:
            raise InternalAudioStoreError("record page cursor is invalid")
        limit = _page_limit(limit, self.chunk_limit)
        try:
            if self._publication_reader is not None:
                with self.state_db.access(self._publication_reader):
                    return _read_record_page(self._publication_reader, source_id, after_ordinal, limit)
            with closing(self.state_db.open_connection(read_only=True)) as connection, self.state_db.access(connection):
                return _read_record_page(connection, source_id, after_ordinal, limit)
        except InternalAudioStoreError:
            raise
        except (sqlite3.DatabaseError, StateDBError) as error:
            raise_if_sqlite_storage_fault(error)
            raise InternalAudioStoreError("record page read failed") from error

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

    def commit_tail(self, source_id: str, expected_ordinal: int, checkpoint: DrainCheckpoint) -> int:
        """Commit a bounded partial tail at the exact durable record frontier."""

        self._check_writer()
        source = self._require_source(_source_id(source_id))
        expected = _uint32(expected_ordinal, "expected ordinal")
        if expected != source.committed_count:
            raise InternalAudioStoreError("tail ordinal is not the committed frontier")
        _validate_receiving_checkpoint(source, expected, checkpoint.leg_generation)
        if checkpoint.tail is None:
            raise InternalAudioStoreError("tail checkpoint is missing")
        previous = self._read_tail(source.source_id)
        desired = self._resolve_tail(source, previous, checkpoint.tail, source.committed_count, ())
        if desired == previous:
            return source.committed_count
        self._commit_replay_tail(source, previous, desired)
        return source.committed_count

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
            self._start_write(connection)
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
            self._rollback_after_error(error)
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
        source_id = _source_id(source_id)
        digest = hashlib.sha256()
        after_ordinal = -1
        hashed_count = 0
        try:
            with closing(self.state_db.open_connection(read_only=True)) as connection, self.state_db.access(connection):
                while hashed_count < count:
                    page = _read_record_page(
                        connection,
                        source_id,
                        after_ordinal,
                        min(self.chunk_limit, count - hashed_count),
                    )
                    if not page or any(record.ordinal != hashed_count + index for index, record in enumerate(page)):
                        raise InternalAudioStoreError("replay prefix is not contiguous")
                    for record in page:
                        digest.update(record.raw_bytes)
                    hashed_count += len(page)
                    after_ordinal = page[-1].ordinal
        except InternalAudioStoreError:
            raise
        except (sqlite3.DatabaseError, StateDBError) as error:
            raise_if_sqlite_storage_fault(error)
            raise InternalAudioStoreError("record page read failed") from error
        return digest.hexdigest()

    def _commit_replay_tail(
        self, source: CaptureSource, previous: CaptureTail | None, desired: CaptureTail | None
    ) -> None:
        connection = self._require_connection()
        try:
            self._start_write(connection)
            self._recheck_source(connection, source)
            self._recheck_tail(connection, source.source_id, previous)
            self._write_tail(connection, source.source_id, desired, previous)
            self._commit()
        except InternalAudioStoreError:
            self._rollback()
            raise
        except sqlite3.DatabaseError as error:
            self._rollback_after_error(error)
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
            self._start_write(connection)
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
            self._rollback_after_error(error)
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
            with closing(self.state_db.open_connection(read_only=True)) as connection, self.state_db.access(connection):
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
            raise_if_sqlite_storage_fault(error)
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
            self._start_write(connection)
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
            canonical_row = _fetchone(
                connection.execute(
                    "SELECT EXISTS(SELECT 1 FROM capture_record WHERE source_id = ?)",
                    (source.source_id,),
                )
            )
            if canonical_row is None:
                raise InternalAudioStoreError("canonical record presence is unavailable")
            canonical_value = _int_value(_value(canonical_row, 0))
            if canonical_value not in {0, 1}:
                raise InternalAudioStoreError("canonical record presence is invalid")
            has_canonical_records = bool(canonical_value)
            now = time.time_ns()
            classification = quarantine_transition(
                QuarantineDisposition.UNCLASSIFIED,
                QuarantineOpened(source.committed_count, has_canonical_records),
            )
            if classification.action not in {QuarantineAction.KEEP, QuarantineAction.MARK_UNPROCESSABLE}:
                raise InternalAudioStoreError(classification.reason)
            assert classification.next_disposition is not None
            unprocessable = classification.next_disposition is QuarantineDisposition.UNPROCESSABLE
            block_reason = (
                "source_authentication_mismatch" if source.committed_count == 0 and has_canonical_records else reason
            )
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
                "UPDATE capture_source SET source_type = 'quarantine', lifecycle = 'blocked', block_reason = ?, "
                "quarantine_disposition = ?, quarantine_opaque = 0, "
                "terminal_reason = ?, "
                "terminal_time = ?, retention_deadline = ?, "
                "certified_prefix_count = NULL, coverage_digest = NULL "
                "WHERE source_id = ?",
                (
                    block_reason,
                    classification.next_disposition.value,
                    "quarantine contains no committed records" if unprocessable else None,
                    str(now) if unprocessable else None,
                    str(now + self._terminal_retention_ns) if unprocessable else None,
                    source.source_id,
                ),
            )
            self._commit()
        except InternalAudioStoreError:
            self._rollback()
            raise
        except sqlite3.DatabaseError as error:
            self._rollback_after_error(error)
            raise_if_sqlite_storage_fault(error)
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
        except (
            sqlite3.DatabaseError,
            StateDBError,
            InternalAudioStoreError,
            TransientInternalAudioStorageError,
        ) as inspection_error:
            raise_if_sqlite_corrupt(inspection_error)
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
            raise_if_sqlite_storage_fault(error)
            raise InternalAudioStoreError("ambiguous drain was rolled back") from error
        self._fail_stop("ambiguous drain commit could not be reconciled")
        raise AssertionError("fail-stop must raise")

    def _recover_create(self, request: _CreateRequest, error: sqlite3.DatabaseError) -> CaptureSource:
        try:
            self._reopen()
            source = self.get_source(request.source_id)
        except (
            sqlite3.DatabaseError,
            StateDBError,
            InternalAudioStoreError,
            TransientInternalAudioStorageError,
        ) as inspection_error:
            raise_if_sqlite_corrupt(inspection_error)
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
            raise_if_sqlite_storage_fault(error)
            raise InternalAudioStoreError("ambiguous source creation was rolled back") from error
        self._fail_stop("ambiguous source creation could not be reconciled")
        raise AssertionError("fail-stop must raise")

    def _recover_seal(
        self, source: CaptureSource, proposed: CaptureSource, error: sqlite3.DatabaseError
    ) -> CaptureSource:
        try:
            self._reopen()
            current = self.get_source(source.source_id)
            tail = self._read_tail(source.source_id)
        except (
            sqlite3.DatabaseError,
            StateDBError,
            InternalAudioStoreError,
            TransientInternalAudioStorageError,
        ) as inspection_error:
            raise_if_sqlite_corrupt(inspection_error)
            self._fail_stop(f"ambiguous seal could not be inspected: {inspection_error}")
        if current == proposed and tail is None:
            return proposed
        if current == source and tail is None:
            raise_if_sqlite_storage_fault(error)
            raise InternalAudioStoreError("ambiguous seal was rolled back") from error
        self._fail_stop("ambiguous seal commit could not be reconciled")
        raise AssertionError("fail-stop must raise")

    def _read_tail(self, source_id: str) -> CaptureTail | None:
        try:
            with closing(self.state_db.open_connection(read_only=True)) as connection, self.state_db.access(connection):
                return self._tail_from_connection(connection, source_id)
        except sqlite3.DatabaseError as error:
            raise_if_sqlite_storage_fault(error)
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
        self._finish_write()

    def _rollback(self) -> None:
        if self._write_access is None:
            return
        connection = self._require_connection()
        with suppress(sqlite3.DatabaseError):
            connection.rollback()
        if connection.in_transaction:
            self._abandon_unsettled_write(connection)
        self._finish_write()

    def _abandon_unsettled_write(self, connection: sqlite3.Connection) -> NoReturn:
        """Close a failed writer and release its instance guard on the owner thread."""

        scope = self._write_access
        if scope is None:
            raise InternalAudioStoreError("audio writer transaction is not active")
        pending = sys.exception()
        self._failed = True
        try:
            self.state_db._close_managed_write(connection)
        finally:
            self._connection = None
            self._write_access = None
            scope.__exit__(None, None, None)
        if isinstance(pending, FatalInternalAudioStorageError):
            raise pending
        raise FatalInternalAudioStorageError("audio writer rollback could not be confirmed") from pending

    def _rollback_after_error(self, error: BaseException) -> None:
        """Settle the transaction without losing a direct corruption fatal."""

        corruption: FatalInternalAudioStorageError | None = None
        try:
            raise_if_sqlite_corrupt(error)
        except FatalInternalAudioStorageError as classified:
            corruption = classified
        try:
            self._rollback()
        except BaseException:
            if corruption is not None:
                raise corruption from error
            raise
        if corruption is not None:
            raise corruption from error

    def _start_write(self, connection: sqlite3.Connection) -> None:
        if self._write_access is not None:
            raise InternalAudioStoreError("audio writer transaction is already active")
        scope = self.state_db.access(connection)
        scope.__enter__()
        try:
            self.state_db.begin_exclusive(connection)
        except BaseException:
            scope.__exit__(None, None, None)
            raise
        self._write_access = scope

    def _finish_write(self) -> None:
        scope = self._write_access
        if scope is None:
            raise InternalAudioStoreError("audio writer transaction is not active")
        self.state_db.end_write(self._require_connection())
        self._write_access = None
        scope.__exit__(None, None, None)

    def _reopen(self) -> None:
        if self._connection is not None:
            with self.state_db.access(self._connection):
                self._connection.close()
        self._connection = self.state_db.open_connection()
        try:
            with self.state_db.access(self._connection):
                self._connection.execute(f"PRAGMA busy_timeout = {self._busy_timeout_ms}").close()
        except sqlite3.DatabaseError as error:
            raise_if_sqlite_storage_fault(error)
            raise

    def _check_thread(self) -> None:
        if threading.get_ident() != self._owner_thread:
            raise InternalAudioStoreError("audio store is thread-affine")

    def _check_writer(self) -> None:
        self._check_thread()
        if self._failed:
            raise InternalAudioStoreError("audio store is fail-stopped")

    def _fail_stop(self, message: str) -> NoReturn:
        self._failed = True
        raise FatalInternalAudioStorageError(message)

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


def _validate_receiving_checkpoint(source: CaptureSource, expected: int, leg_generation: int) -> None:
    if type(leg_generation) is not int or leg_generation != source.current_leg_generation:
        raise InternalAudioStoreError("drain leg generation is stale")
    decision = transition(_phase(source), Data())
    if decision.action is not AudioAction.PERSIST:
        raise InternalAudioStoreError("tail requires a receiving source")
    active_leg = (source.current_leg_start, source.current_leg_count)
    durable_leg = (source.prepared_start, source.prepared_count), (source.read_begin_start, source.read_begin_count)
    if durable_leg != (active_leg, active_leg):
        raise InternalAudioStoreError("tail is outside the bound READ leg")
    leg_floor = source.current_leg_start - source.sequence_start
    leg_ceiling = min(source.committed_count, leg_floor + source.current_leg_count - 1)
    if not leg_floor <= expected <= leg_ceiling:
        raise InternalAudioStoreError("tail is outside the bound READ leg")


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


def _read_capture_source(connection: sqlite3.Connection, source_id: str) -> CaptureSource | None:
    row = _fetchone(connection.execute("SELECT * FROM capture_source WHERE source_id = ?", (source_id,)))
    return _source_from_row(row) if row is not None else None


def _read_record_page(
    connection: sqlite3.Connection,
    source_id: str,
    after_ordinal: int,
    limit: int,
) -> tuple[CaptureRecord, ...]:
    rows = _fetchall(
        connection.execute(
            "SELECT source_id, ordinal, device_timestamp, payload, utc_timestamp, mapping_revision "
            "FROM capture_record WHERE source_id = ? AND ordinal > ? ORDER BY ordinal LIMIT ?",
            (source_id, after_ordinal, limit),
        )
    )
    return tuple(_record_from_row(row) for row in rows)


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
    try:
        result = cast(object, cursor.fetchone())
    finally:
        cursor.close()
    if result is not None and not isinstance(result, sqlite3.Row):
        raise InternalAudioStoreError("audio database returned an invalid row")
    return result


def _fetchall(cursor: sqlite3.Cursor) -> list[sqlite3.Row]:
    try:
        result = cast(object, cursor.fetchall())
    finally:
        cursor.close()
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
    leg_byte_limit = (source.current_leg_start - source.sequence_start + source.current_leg_count) * AUDIO_RECORD_SIZE
    if (
        type(tail.received_high_water) is not int
        or tail.received_high_water < 0
        or tail.received_high_water > leg_byte_limit
    ):
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
