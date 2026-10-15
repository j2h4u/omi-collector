"""Bounded, candidate-only conversion of authenticated v1 private audio."""

from __future__ import annotations

import hashlib
import json
import sqlite3
import stat
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import cast

from .bundle_contract import BundleManifest
from .internal_audio_store import AUDIO_PAYLOAD_SIZE, AUDIO_RECORD_SIZE, DEFAULT_CHUNK_LIMIT, encode_uint64
from .publication import PrefixPublicationEvidence, TerminalRetirementEvidence
from .staging_contract import (
    AttemptDescriptor,
    AttemptStateError,
    StreamingCheckpoint,
    _checkpoint_from_json,
    _descriptor_from_json,
    _validate_attempt_id,
    _validate_descriptor,
)
from .staging_filesystem import _hash_prefix, legacy_attempt_row


class LegacyAudioImportError(RuntimeError):
    """A v1 private source cannot be imported without losing custody."""


@dataclass(frozen=True, slots=True)
class LegacyDraft:
    path: Path
    source_id: str
    manifest: BundleManifest


@dataclass(frozen=True, slots=True)
class LegacyAttempt:
    path: Path
    attempt_id: str
    descriptor: AttemptDescriptor
    checkpoint: StreamingCheckpoint
    prefix_publication: dict[str, object] | None
    terminal_retirement: dict[str, object] | None
    raw_size: int


@dataclass(frozen=True, slots=True)
class _LegacySourceState:
    lifecycle: str
    generation: int
    read_begin_start: int | None
    has_read_begin: bool


def inspect_legacy_attempt(
    path: Path, attempt_id: str, authoritative_row: Mapping[str, object] | None = None
) -> LegacyAttempt:
    """Authenticate one v1 attempt directory under its explicit source identity."""

    try:
        _validate_attempt_id(attempt_id)
    except AttemptStateError as error:
        raise LegacyAudioImportError("attempt identity is invalid") from error
    _real_directory(path)
    allowed = {"attempt.json", "checkpoint.json", "records.bin", "prefix-publication.json", "terminal-retired.json"}
    try:
        entries = {entry.name for entry in path.iterdir()}
    except OSError as error:
        raise LegacyAudioImportError(f"attempt directory cannot be inspected: {path}") from error
    if not entries <= allowed:
        raise LegacyAudioImportError(f"attempt has unsupported entries: {path}")
    row, descriptor, checkpoint, raw_size = _attempt_metadata(path, attempt_id, authoritative_row, entries)
    _require_read_begin_for_private_bytes(descriptor, raw_size)
    return LegacyAttempt(
        path,
        attempt_id,
        descriptor,
        checkpoint,
        cast(dict[str, object] | None, row["prefix_publication"]),
        cast(dict[str, object] | None, row["terminal_retirement"]),
        raw_size,
    )


def _attempt_metadata(
    path: Path, attempt_id: str, authoritative_row: Mapping[str, object] | None, entries: set[str]
) -> tuple[dict[str, object], AttemptDescriptor, StreamingCheckpoint, int]:
    try:
        if entries == {"records.bin"} and authoritative_row is not None:
            row = _sql_only_attempt_row(authoritative_row)
        else:
            row = legacy_attempt_row(path)
        descriptor_raw = cast(dict[str, object], row["descriptor"])
        checkpoint_raw = cast(dict[str, object], row["checkpoint"])
        descriptor = _descriptor_from_json(descriptor_raw)
        _validate_descriptor(descriptor, attempt_id)
        checkpoint = _checkpoint_from_json(checkpoint_raw, attempt_id)
        if descriptor.attempt_id != attempt_id:
            raise LegacyAudioImportError("attempt descriptor identity differs from expected attempt")
        raw_path = path / "records.bin"
        _regular_file(raw_path)
        raw_size = raw_path.stat(follow_symlinks=False).st_size
        checkpoint_bytes = checkpoint.record_count * AUDIO_RECORD_SIZE
        if raw_size < checkpoint_bytes or _hash_prefix(raw_path, checkpoint_bytes) != checkpoint.raw_sha256:
            raise LegacyAudioImportError("attempt raw prefix differs from checkpoint")
        if raw_size > descriptor.packet_count * AUDIO_RECORD_SIZE:
            raise LegacyAudioImportError("attempt raw data exceeds descriptor packet count")
        _check_authoritative_row(row, authoritative_row)
        return row, descriptor, checkpoint, raw_size
    except LegacyAudioImportError:
        raise
    except (AttemptStateError, OSError, KeyError, TypeError, ValueError) as error:
        raise LegacyAudioImportError(f"attempt metadata is invalid: {path}") from error


def _sql_only_attempt_row(authoritative_row: Mapping[str, object]) -> dict[str, object]:
    row = dict(authoritative_row)
    cleanup = row.pop("cleanup_intent", None)
    if cleanup is not None and cleanup != {"version": 1, "state": "pending"}:
        raise LegacyAudioImportError("attempt cleanup intent is invalid")
    if set(row) != {"descriptor", "checkpoint", "prefix_publication", "terminal_retirement"}:
        raise LegacyAudioImportError("attempt authoritative row has invalid fields")
    if not isinstance(row["descriptor"], dict) or not isinstance(row["checkpoint"], dict):
        raise LegacyAudioImportError("attempt authoritative metadata is malformed")
    prefix = row["prefix_publication"]
    terminal = row["terminal_retirement"]
    if prefix is not None:
        row["prefix_publication"] = PrefixPublicationEvidence.from_json(prefix).as_dict()
    if terminal is not None:
        if prefix is None:
            raise LegacyAudioImportError("attempt terminal marker has no published prefix")
        row["terminal_retirement"] = TerminalRetirementEvidence.from_json(terminal).as_dict()
    return row


def _check_authoritative_row(row: dict[str, object], authoritative_row: Mapping[str, object] | None) -> None:
    if authoritative_row is None:
        return
    authoritative = dict(authoritative_row)
    cleanup = authoritative.pop("cleanup_intent", None)
    if cleanup is not None and cleanup != {"version": 1, "state": "pending"}:
        raise LegacyAudioImportError("attempt cleanup intent is invalid")
    if authoritative != row:
        raise LegacyAudioImportError("attempt metadata differs from authoritative state row")


def _require_read_begin_for_private_bytes(descriptor: AttemptDescriptor, raw_size: int) -> None:
    if raw_size and descriptor.read_begin_start is None:
        raise LegacyAudioImportError(
            "v1 attempt has private bytes without READ_BEGIN; manual custody resolution required"
        )


def import_legacy_attempt(
    connection: sqlite3.Connection,
    attempt: LegacyAttempt,
    *,
    authoritative_row: Mapping[str, object] | None = None,
    chunk_limit: int = DEFAULT_CHUNK_LIMIT,
) -> None:
    """Import complete authenticated attempt records and residual tail bytes."""

    if type(chunk_limit) is not int or not 1 <= chunk_limit <= DEFAULT_CHUNK_LIMIT:
        raise LegacyAudioImportError("migration chunk size is invalid")
    _require_read_begin_for_private_bytes(attempt.descriptor, attempt.raw_size)
    expected = inspect_legacy_attempt(attempt.path, attempt.attempt_id, authoritative_row)
    if expected != attempt:
        raise LegacyAudioImportError(f"attempt changed before import: {attempt.path}")
    complete_count, residual = divmod(attempt.raw_size, AUDIO_RECORD_SIZE)
    read_begin_start = attempt.descriptor.read_begin_start
    has_read_begin = read_begin_start is not None
    lifecycle = "interrupted" if has_read_begin else "prepared"
    generation = 0
    try:
        connection.execute("BEGIN IMMEDIATE")
        _insert_legacy_source(
            connection, attempt, _LegacySourceState(lifecycle, generation, read_begin_start, has_read_begin)
        )
        connection.commit()
        tail = _import_attempt_records(connection, attempt, complete_count, residual, chunk_limit)
        if tail:
            connection.execute("BEGIN IMMEDIATE")
            connection.execute(
                "INSERT INTO capture_tail(source_id, leg_generation, complete_high_water, received_high_water, byte_offset, bytes) VALUES (?, ?, ?, ?, ?, ?)",
                (
                    attempt.attempt_id,
                    generation,
                    complete_count,
                    attempt.raw_size,
                    complete_count * AUDIO_RECORD_SIZE,
                    tail,
                ),
            )
            connection.commit()
    except LegacyAudioImportError:
        connection.rollback()
        raise
    except (OSError, sqlite3.DatabaseError) as error:
        connection.rollback()
        raise LegacyAudioImportError(f"attempt import failed: {attempt.path}") from error


def _insert_legacy_source(
    connection: sqlite3.Connection,
    attempt: LegacyAttempt,
    state: _LegacySourceState,
) -> None:
    connection.execute(
        "INSERT INTO capture_source(source_id, source_type, lifecycle, sequence_start, sequence_limit, "
        "prepared_start, prepared_count, read_begin_start, read_begin_count, current_leg_generation, "
        "current_leg_start, current_leg_count, committed_count, committed_sha256, certified_prefix_count, "
        "coverage_digest, terminal_time, terminal_reason) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?, ?, ?, ?, ?)",
        (
            attempt.attempt_id,
            "attempt",
            state.lifecycle,
            encode_uint64(attempt.descriptor.start_sequence),
            encode_uint64(attempt.descriptor.start_sequence + attempt.descriptor.packet_count),
            encode_uint64(attempt.descriptor.start_sequence) if state.has_read_begin else None,
            attempt.descriptor.packet_count if state.has_read_begin else None,
            encode_uint64(state.read_begin_start) if state.read_begin_start is not None else None,
            attempt.descriptor.read_begin_count if state.has_read_begin else None,
            state.generation,
            encode_uint64(attempt.descriptor.start_sequence),
            attempt.descriptor.packet_count if state.has_read_begin else 0,
            hashlib.sha256(b"").hexdigest(),
            attempt.checkpoint.record_count,
            attempt.checkpoint.raw_sha256,
            str(attempt.terminal_retirement["terminalized_at_unix_ns"])
            if attempt.terminal_retirement is not None
            else None,
            "terminal-retired" if attempt.terminal_retirement is not None else None,
        ),
    )


def _import_attempt_records(
    connection: sqlite3.Connection,
    attempt: LegacyAttempt,
    complete_count: int,
    residual: int,
    chunk_limit: int,
) -> bytes:
    digest = hashlib.sha256()
    raw_path = attempt.path / "records.bin"
    with raw_path.open("rb", buffering=0) as raw:
        ordinal = 0
        while ordinal < complete_count:
            count = min(chunk_limit, complete_count - ordinal)
            payload = raw.read(count * AUDIO_RECORD_SIZE)
            if len(payload) != count * AUDIO_RECORD_SIZE:
                raise LegacyAudioImportError(f"attempt changed during import: {raw_path}")
            rows = []
            for index in range(count):
                record = payload[index * AUDIO_RECORD_SIZE : (index + 1) * AUDIO_RECORD_SIZE]
                rows.append((attempt.attempt_id, ordinal + index, int.from_bytes(record[:4], "big"), record[4:]))
            digest.update(payload)
            connection.execute("BEGIN IMMEDIATE")
            connection.executemany(
                "INSERT INTO capture_record(source_id, ordinal, device_timestamp, payload) VALUES (?, ?, ?, ?)",
                rows,
            )
            ordinal += count
            connection.execute(
                "UPDATE capture_source SET committed_count=?, committed_sha256=? WHERE source_id=?",
                (ordinal, digest.hexdigest(), attempt.attempt_id),
            )
            connection.commit()
        tail = raw.read(residual) if residual else b""
        if raw.read(1):
            raise LegacyAudioImportError(f"attempt grew during import: {raw_path}")
    return tail


def verify_imported_attempt(connection: sqlite3.Connection, attempt: LegacyAttempt) -> None:
    """Verify typed complete rows, derived SHA, and residual tail against the source."""

    row = cast(
        tuple[object, ...] | sqlite3.Row | None,
        cast(
            object,
            connection.execute(
                "SELECT committed_count, committed_sha256, certified_prefix_count, coverage_digest "
                "FROM capture_source WHERE source_id = ?",
                (attempt.attempt_id,),
            ).fetchone(),
        ),
    )
    if row is None:
        raise LegacyAudioImportError(f"attempt source is missing: {attempt.path}")
    complete_count, residual = divmod(attempt.raw_size, AUDIO_RECORD_SIZE)
    digest, expected_tail = _verify_attempt_stream(connection, attempt, complete_count, residual)
    if tuple(row) != (
        complete_count,
        digest,
        attempt.checkpoint.record_count,
        attempt.checkpoint.raw_sha256,
    ):
        raise LegacyAudioImportError(f"attempt typed digest differs: {attempt.path}")
    tail = cast(
        tuple[object, object, object, object] | None,
        cast(
            object,
            connection.execute(
                "SELECT complete_high_water, received_high_water, byte_offset, bytes FROM capture_tail WHERE source_id = ?",
                (attempt.attempt_id,),
            ).fetchone(),
        ),
    )
    if residual:
        if tail is None or tail[:3] != (complete_count, attempt.raw_size, complete_count * AUDIO_RECORD_SIZE):
            raise LegacyAudioImportError(f"attempt tail differs: {attempt.path}")
        tail_bytes = tail[3]
        if not isinstance(tail_bytes, bytes) or tail_bytes != expected_tail:
            raise LegacyAudioImportError(f"attempt tail bytes differ: {attempt.path}")
    elif tail is not None:
        raise LegacyAudioImportError(f"unexpected attempt tail: {attempt.path}")


def _verify_attempt_stream(
    connection: sqlite3.Connection, attempt: LegacyAttempt, complete_count: int, residual: int
) -> tuple[str, bytes]:
    digest = hashlib.sha256()
    rows = cast(
        Iterable[object],
        cast(
            object,
            connection.execute(
                "SELECT ordinal, device_timestamp, payload FROM capture_record WHERE source_id = ? ORDER BY ordinal",
                (attempt.attempt_id,),
            ),
        ),
    )
    raw_path = attempt.path / "records.bin"
    with raw_path.open("rb", buffering=0) as raw:
        count = 0
        for count, raw_row in enumerate(rows, start=1):
            ordinal, timestamp, payload = cast(tuple[object, object, object], tuple(cast(Iterable[object], raw_row)))
            if (
                type(ordinal) is not int
                or type(timestamp) is not int
                or ordinal != count - 1
                or not isinstance(payload, bytes)
                or len(payload) != AUDIO_PAYLOAD_SIZE
            ):
                raise LegacyAudioImportError(f"attempt typed rows are malformed: {attempt.path}")
            record = timestamp.to_bytes(4, "big") + payload
            if raw.read(AUDIO_RECORD_SIZE) != record:
                raise LegacyAudioImportError(f"attempt typed bytes differ from source: {attempt.path}")
            digest.update(record)
        if count != complete_count:
            raise LegacyAudioImportError(f"attempt typed row count differs: {attempt.path}")
        expected_tail = raw.read(residual)
        if len(expected_tail) != residual or raw.read(1):
            raise LegacyAudioImportError(f"attempt source bytes changed: {attempt.path}")
    return digest.hexdigest(), expected_tail


def inspect_legacy_draft(path: Path) -> LegacyDraft:
    """Authenticate one complete v1 draft without following private symlinks."""

    _real_directory(path)
    entries = {entry.name for entry in path.iterdir()}
    if entries != {"records.bin", "manifest.json"}:
        raise LegacyAudioImportError(f"draft has unsupported entries: {path}")
    manifest_path = path / "manifest.json"
    raw_path = path / "records.bin"
    _regular_file(manifest_path)
    _regular_file(raw_path)
    try:
        manifest = BundleManifest.from_json(cast(object, json.loads(manifest_path.read_text(encoding="utf-8"))))
    except (OSError, UnicodeError, ValueError, json.JSONDecodeError) as error:
        raise LegacyAudioImportError(f"draft manifest is invalid: {manifest_path}") from error
    expected_name = f"{manifest.start_sequence}-{manifest.next_sequence}-{manifest.raw_sha256[:16]}"
    if path.name != expected_name:
        raise LegacyAudioImportError(f"draft identity disagrees with manifest: {path}")
    if raw_path.stat(follow_symlinks=False).st_size != manifest.record_count * AUDIO_RECORD_SIZE:
        raise LegacyAudioImportError(f"draft byte count differs from manifest: {raw_path}")
    source_id = hashlib.sha256(f"draft/{path.name}".encode("ascii")).hexdigest()[:32]
    return LegacyDraft(path, source_id, manifest)


def import_legacy_draft(
    connection: sqlite3.Connection, draft: LegacyDraft, *, chunk_limit: int = DEFAULT_CHUNK_LIMIT
) -> None:
    """Import exact v1 draft bytes into a *candidate* v2 database only."""

    if type(chunk_limit) is not int or not 1 <= chunk_limit <= DEFAULT_CHUNK_LIMIT:
        raise LegacyAudioImportError("migration chunk size is invalid")
    expected = inspect_legacy_draft(draft.path)
    if expected != draft:
        raise LegacyAudioImportError(f"draft changed before import: {draft.path}")
    manifest = draft.manifest
    try:
        connection.execute("BEGIN IMMEDIATE")
        connection.execute(
            "INSERT INTO capture_source(source_id, source_type, lifecycle, sequence_start, sequence_limit, "
            "read_begin_start, read_begin_count, current_leg_generation, current_leg_start, current_leg_count, "
            "committed_count, committed_sha256) VALUES (?, 'draft', 'sealed_waiting', ?, ?, ?, ?, 0, ?, ?, 0, ?)",
            (
                draft.source_id,
                encode_uint64(manifest.start_sequence),
                encode_uint64(manifest.next_sequence),
                encode_uint64(manifest.start_sequence),
                manifest.record_count,
                encode_uint64(manifest.start_sequence),
                manifest.record_count,
                hashlib.sha256(b"").hexdigest(),
            ),
        )
        connection.commit()
        digest = hashlib.sha256()
        ordinal = 0
        raw_path = draft.path / "records.bin"
        with raw_path.open("rb", buffering=0) as raw:
            while ordinal < manifest.record_count:
                count = min(chunk_limit, manifest.record_count - ordinal)
                payload = raw.read(count * AUDIO_RECORD_SIZE)
                if len(payload) != count * AUDIO_RECORD_SIZE:
                    raise LegacyAudioImportError(f"draft changed during import: {raw_path}")
                rows = []
                for index in range(count):
                    record = payload[index * AUDIO_RECORD_SIZE : (index + 1) * AUDIO_RECORD_SIZE]
                    rows.append((draft.source_id, ordinal + index, int.from_bytes(record[:4], "big"), record[4:]))
                digest.update(payload)
                connection.execute("BEGIN IMMEDIATE")
                connection.executemany(
                    "INSERT INTO capture_record(source_id, ordinal, device_timestamp, payload) VALUES (?, ?, ?, ?)",
                    rows,
                )
                ordinal += count
                connection.execute(
                    "UPDATE capture_source SET committed_count = ?, committed_sha256 = ? WHERE source_id = ?",
                    (ordinal, digest.hexdigest(), draft.source_id),
                )
                connection.commit()
            if raw.read(1):
                raise LegacyAudioImportError(f"draft grew during import: {raw_path}")
        if digest.hexdigest() != manifest.raw_sha256:
            raise LegacyAudioImportError(f"draft hash differs from manifest: {raw_path}")
    except (OSError, sqlite3.DatabaseError) as error:
        connection.rollback()
        raise LegacyAudioImportError(f"draft import failed: {draft.path}") from error


def verify_imported_draft(connection: sqlite3.Connection, draft: LegacyDraft) -> None:
    """Reconstruct the candidate stream and compare it with the v1 manifest."""

    row = cast(
        tuple[int, str] | sqlite3.Row | None,
        connection.execute(
            "SELECT committed_count, committed_sha256 FROM capture_source WHERE source_id = ?",
            (draft.source_id,),
        ).fetchone(),
    )
    if row is None or tuple(row) != (draft.manifest.record_count, draft.manifest.raw_sha256):
        raise LegacyAudioImportError(f"draft checkpoint differs after import: {draft.path}")
    digest = hashlib.sha256()
    count = 0
    cursor = connection.execute(
        "SELECT ordinal, device_timestamp, payload FROM capture_record WHERE source_id = ? ORDER BY ordinal",
        (draft.source_id,),
    )
    for ordinal, device_timestamp, payload in cast(Iterable[tuple[int, int, bytes]], cursor):
        if ordinal != count or not isinstance(payload, bytes) or len(payload) != AUDIO_PAYLOAD_SIZE:
            raise LegacyAudioImportError(f"draft ordinal or payload differs after import: {draft.path}")
        digest.update(int(device_timestamp).to_bytes(4, "big") + payload)
        count += 1
    if count != draft.manifest.record_count or digest.hexdigest() != draft.manifest.raw_sha256:
        raise LegacyAudioImportError(f"draft stream differs after import: {draft.path}")


def _real_directory(path: Path) -> None:
    try:
        mode = path.lstat().st_mode
    except OSError as error:
        raise LegacyAudioImportError(f"draft path cannot be inspected: {path}") from error
    if not stat.S_ISDIR(mode):
        raise LegacyAudioImportError(f"draft path is not a real directory: {path}")


def _regular_file(path: Path) -> None:
    try:
        mode = path.lstat().st_mode
    except OSError as error:
        raise LegacyAudioImportError(f"draft file cannot be inspected: {path}") from error
    if not stat.S_ISREG(mode):
        raise LegacyAudioImportError(f"draft file is not regular: {path}")
