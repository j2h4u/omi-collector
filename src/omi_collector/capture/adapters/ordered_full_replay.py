"""Durable evidence that an ordered source repeats already published physical bytes."""

from __future__ import annotations

import json
import sqlite3
from contextlib import closing
from functools import partial
from hashlib import sha256
from typing import cast

from ..domain.physical_overlap import _prefix_borders
from ..domain.physical_recovery import RECORD_SIZE, PhysicalSegment, PhysicalSpan, iter_physical_records
from . import ready_bundles as ready
from .collector_state_db import StateKind
from .internal_audio_store import InternalAudioStore
from .ordered_payloads import OrderedPayload, _historical_rows, _require_payload_custody, select_ordered_payload
from .physical_source import authenticate_source, read_physical_span, source_identity
from .ready_ledger import read_ready_ledger

_KIND = "ordered_full_replay"


def _records(connection: sqlite3.Connection, source_id: str, offset: int, size: int):
    span = PhysicalSpan(source_id, offset, size)
    return iter_physical_records(PhysicalSegment((span,)), partial(read_physical_span, connection))


def _published_window(connection: sqlite3.Connection, payload: OrderedPayload, offset: int) -> None:
    try:
        entry = read_ready_ledger(connection, payload.predecessor.artifact_id)
    except ValueError as error:
        raise ValueError("replay predecessor ledger is invalid") from error
    if entry is None:
        raise ValueError("replay predecessor ledger is missing")
    if entry.get("lineage_kind") == "physical_byte_span":
        # The shared verifier validates marker identity, descriptor and projection.
        from .ordered_payloads import ordered_physical_predecessor

        if ordered_physical_predecessor(connection, payload.predecessor.artifact_id) != payload.predecessor:
            raise ValueError("replay predecessor changed")
        spans = cast(list[dict[str, object]], entry["spans"])
        if not any(
            cast(int, span["byte_start"]) <= offset and offset + payload.byte_count <= cast(int, span["byte_end"])
            for span in spans
        ):
            raise ValueError("replayed bytes are outside published predecessor spans")
    else:
        lineage = ready.source_lineage(entry)
        expected = (
            payload.predecessor.source_start,
            payload.predecessor.source_start + payload.predecessor.byte_count // RECORD_SIZE,
            payload.predecessor.source_sha256,
        )
        if not lineage or lineage[-1] != expected:
            raise ValueError("replay predecessor lineage changed")


def _find_offset(connection: sqlite3.Connection, payload: OrderedPayload) -> int | None:
    predecessor = payload.predecessor
    pattern = [sha256(record).digest() for record in _records(connection, payload.source_id, 0, payload.byte_count)]
    if not pattern:
        raise ValueError("empty source cannot establish replay")
    borders = _prefix_borders(pattern)
    matched = 0
    found: list[int] = []
    for index, record in enumerate(_records(connection, predecessor.source_id, 0, predecessor.byte_count)):
        digest = sha256(record).digest()
        while matched and pattern[matched] != digest:
            matched = borders[matched - 1]
        if pattern[matched] == digest:
            matched += 1
        if matched == len(pattern):
            found.append((index + 1 - matched) * RECORD_SIZE)
            matched = borders[matched - 1]
    if len(found) > 1:
        raise ValueError("replay occurrence is ambiguous")
    if not found:
        return None
    return found[0]


def _matching_offset(connection: sqlite3.Connection, payload: OrderedPayload) -> int | None:
    predecessor = payload.predecessor
    if authenticate_source(connection, predecessor.source_id) != (predecessor.byte_count, predecessor.source_sha256):
        raise ValueError("replay predecessor custody changed")
    if authenticate_source(connection, payload.source_id) != (payload.byte_count, payload.source_sha256):
        raise ValueError("replay source custody changed")
    offset = _find_offset(connection, payload)
    if offset is None:
        return None
    left = _records(connection, predecessor.source_id, offset, payload.byte_count)
    right = _records(connection, payload.source_id, 0, payload.byte_count)
    if any(a != b for a, b in zip(left, right, strict=True)):
        raise ValueError("replay bytes differ")
    _published_window(connection, payload, offset)
    return offset


def _receipt(payload: OrderedPayload, offset: int) -> dict[str, object]:
    p = payload.predecessor
    receipt: dict[str, object] = {
        "source_id": payload.source_id,
        "source_sha256": payload.source_sha256,
        "byte_count": payload.byte_count,
        "source_start": payload.source_start,
        "read_begin": payload.read_begin,
        "read_count": payload.read_count,
        "predecessor_artifact": p.artifact_id,
        "predecessor_source": p.source_id,
        "predecessor_sha256": p.source_sha256,
        "predecessor_start": p.source_start,
        "predecessor_bytes": p.byte_count,
        "predecessor_offset": offset,
    }
    receipt["receipt_sha256"] = sha256(ready._canonical(receipt)).hexdigest()
    return receipt


def _check_custody(connection: sqlite3.Connection, payload: OrderedPayload) -> None:
    rows = [row for row in _historical_rows(connection) if row[0] == payload.source_id]
    if len(rows) != 1:
        raise ValueError("replay historical source missing")
    _require_payload_custody(connection, rows[0])
    from .internal_audio_store import decode_uint64

    row = rows[0]
    if (
        row[6],
        cast(int, row[5]) * RECORD_SIZE,
        decode_uint64(cast(bytes, row[16])),
        decode_uint64(cast(bytes, row[3])),
        row[4],
    ) != (
        payload.source_sha256,
        payload.byte_count,
        payload.source_start,
        payload.read_begin,
        payload.read_count,
    ):
        raise ValueError("replay historical source changed")


def _require_source_identities(connection: sqlite3.Connection, payload: OrderedPayload) -> None:
    predecessor = payload.predecessor
    if source_identity(connection, payload.source_id) != (payload.byte_count, payload.source_sha256):
        raise ValueError("replay source custody changed")
    if source_identity(connection, predecessor.source_id) != (predecessor.byte_count, predecessor.source_sha256):
        raise ValueError("replay predecessor custody changed")


def try_record_ordered_full_replay(store: InternalAudioStore, payload: OrderedPayload) -> bool:
    """Record a complete, unique published replay without allocating a READY artifact."""
    with closing(store.state_db.open_connection(read_only=True)) as connection:
        connection.execute("BEGIN")
        existing = cast(
            tuple[str] | None,
            connection.execute(
                "SELECT payload FROM state WHERE kind=? AND id=?", (_KIND, payload.source_id)
            ).fetchone(),
        )
        if existing is None and select_ordered_payload(connection) != payload:
            raise ValueError("replay source is not the next ordered payload")
        _check_custody(connection, payload)
        # Retained source records are immutable; close the SQLite snapshot before
        # streaming pages so a DELETE-journal reader does not block intake commits.
        connection.rollback()
        offset = _matching_offset(connection, payload)
        _require_source_identities(connection, payload)
        if offset is None:
            if existing is not None:
                raise ValueError("replay receipt no longer matches source")
            return False
        receipt = _receipt(payload, offset)
    with store.state_db.transaction() as state:
        connection = state._connection
        current = state.get(StateKind(_KIND), payload.source_id)
        if current is None and select_ordered_payload(connection) != payload:
            raise ValueError("replay source is not the next ordered payload")
        _check_custody(connection, payload)
        _require_source_identities(connection, payload)
        _published_window(connection, payload, offset)
        if current is not None and current != receipt:
            raise ValueError("replay receipt conflicts with custody")
        if current is None:
            state.put(StateKind(_KIND), payload.source_id, receipt)
        return True


def read_ordered_replay_receipt(connection: sqlite3.Connection, source_id: str) -> dict[str, object]:
    """Read an existing receipt after the caller validates the completed set."""
    row = cast(
        tuple[str] | None,
        connection.execute(
            "SELECT payload FROM state WHERE kind=? AND id=?", (StateKind.ORDERED_FULL_REPLAY.value, source_id)
        ).fetchone(),
    )
    if row is None:
        raise ValueError("ordered replay receipt disappeared")
    value = cast(object, json.loads(row[0]))
    if not isinstance(value, dict):
        raise ValueError("ordered replay receipt is invalid")
    return cast(dict[str, object], value)


def completed_ordered_replays(connection: sqlite3.Connection) -> frozenset[str]:
    """Reauthenticate stored receipts before allowing selection to advance."""
    from .ordered_payloads import OrderedPredecessor

    if not connection.in_transaction:
        raise ValueError("replay lookup requires a stable snapshot")
    result: set[str] = set()
    rows = cast(
        list[tuple[str, str]], connection.execute("SELECT id,payload FROM state WHERE kind=?", (_KIND,)).fetchall()
    )
    for source_id, raw in rows:
        data = cast(dict[str, object], json.loads(raw))
        try:
            predecessor = OrderedPredecessor(
                cast(str, data["predecessor_artifact"]),
                cast(str, data["predecessor_source"]),
                cast(str, data["predecessor_sha256"]),
                cast(int, data["predecessor_start"]),
                cast(int, data["predecessor_bytes"]),
            )
            payload = OrderedPayload(
                cast(str, data["source_id"]),
                cast(str, data["source_sha256"]),
                cast(int, data["byte_count"]),
                cast(int, data["source_start"]),
                cast(int, data["read_begin"]),
                cast(int, data["read_count"]),
                predecessor,
            )
            _check_custody(connection, payload)
            offset = data["predecessor_offset"]
            if (
                type(offset) is not int
                or offset < 0
                or offset % RECORD_SIZE
                or offset + payload.byte_count > predecessor.byte_count
            ):
                raise ValueError("replay receipt range is invalid")
            if source_id != payload.source_id or data != _receipt(payload, offset):
                raise ValueError("replay receipt changed")
            if source_identity(connection, payload.source_id) != (payload.byte_count, payload.source_sha256):
                raise ValueError("replay source custody changed")
            predecessor_row = cast(
                tuple[object, object] | None,
                connection.execute(
                    "SELECT committed_count,committed_sha256 FROM capture_source WHERE source_id=?",
                    (predecessor.source_id,),
                ).fetchone(),
            )
            if predecessor_row is None:
                from .ordered_retirement import ordered_replay_handed_off

                if not ordered_replay_handed_off(connection, data):
                    raise ValueError("replay predecessor custody disappeared")
            elif tuple(predecessor_row) != (predecessor.byte_count // RECORD_SIZE, predecessor.source_sha256):
                raise ValueError("replay predecessor custody changed")
            _published_window(connection, payload, offset)
        except (KeyError, TypeError, IndexError) as error:
            raise ValueError("replay receipt is invalid") from error
        result.add(cast(str, source_id))
    return frozenset(result)


def has_ordered_replay_reference(connection: sqlite3.Connection, source_id: str) -> bool:
    """Protect both source and predecessor raw bytes while a receipt exists."""
    completed_ordered_replays(connection)
    rows = cast(
        list[tuple[str, str]], connection.execute("SELECT id,payload FROM state WHERE kind=?", (_KIND,)).fetchall()
    )
    for key, raw in rows:
        value = cast(object, json.loads(raw))
        if not isinstance(value, dict):
            raise ValueError("replay receipt is invalid")
        receipt = cast(dict[str, object], value)
        if key == source_id:
            return True
        if receipt.get("predecessor_source") == source_id:
            from .ordered_retirement import ordered_replay_handed_off

            if not ordered_replay_handed_off(connection, receipt):
                return True
    return False
