"""Retire acknowledged ordered physical records without device sequence claims."""

from __future__ import annotations

import json
import sqlite3
from contextlib import closing
from dataclasses import dataclass
from typing import cast

from . import ready_bundles as ready
from .collector_state_db import StateDB, StateKind
from .internal_audio_store import InternalAudioStore
from .ordered_boundary import _ledger, _markers
from .ordered_full_replay import completed_ordered_replays, read_ordered_replay_receipt
from .ordered_payloads import ordered_physical_predecessor
from .physical_source import authenticate_source
from .sqlite_compaction import mark_retirement_compaction_pending


@dataclass(frozen=True, slots=True)
class _PhysicalRetirement:
    source_id: str
    marker_id: str
    artifact_id: str
    successor_id: str
    count: int
    sha256: str
    cursor: int


def _successor(
    connection: sqlite3.Connection,
    artifact: str,
    marker_id: str,
    entries: dict[str, dict[str, object]],
    markers: dict[str, tuple[str, dict[str, object]]],
) -> str | None:
    entry = entries[artifact]
    order = cast(int, entry["order"])
    if sum(value.get("order") == order for value in entries.values()) != 1:
        return None
    successors = [key for key, value in entries.items() if value.get("order") == order + 1]
    if len(successors) != 1:
        return None
    successor = successors[0]
    if successor in markers:
        ordering = markers[successor][1].get("ordering")
        if isinstance(ordering, dict):
            fields = cast(dict[str, object], ordering)
            predecessor = ordered_physical_predecessor(connection, artifact)
            if (
                fields.get("predecessor_artifact"),
                fields.get("predecessor_source_id"),
                fields.get("predecessor_sha256"),
                fields.get("predecessor_start"),
            ) == (artifact, predecessor.source_id, predecessor.source_sha256, predecessor.source_start):
                ordered_physical_predecessor(connection, successor)
                return successor
        return None
    row = cast(
        tuple[str] | None,
        connection.execute(
            "SELECT payload FROM state WHERE kind=? AND id=?", (StateKind.ORDERED_SUCCESSOR.value, marker_id)
        ).fetchone(),
    )
    if row is None:
        return None
    receipt = cast(object, json.loads(row[0]))
    return successor if receipt == {"physical_artifact": artifact, "successor_artifact": successor} else None


def _candidate(
    connection: sqlite3.Connection,
    artifact: str,
    marked: tuple[str, dict[str, object]],
    entries: dict[str, dict[str, object]],
    markers: dict[str, tuple[str, dict[str, object]]],
) -> _PhysicalRetirement | None:
    marker_id, marker = marked
    if "ordering" not in marker or entries[artifact].get("state") != "retired":
        return None
    predecessor = ordered_physical_predecessor(connection, artifact)
    successor = _successor(connection, artifact, marker_id, entries, markers)
    if successor is None:
        return None
    completed_ordered_replays(connection)
    entry = entries[artifact]
    if entry.get("seams") != []:
        return None
    basis = _PhysicalRetirement(
        predecessor.source_id,
        marker_id,
        artifact,
        successor,
        predecessor.byte_count // 444,
        predecessor.source_sha256,
        0,
    )
    return _retirement_for_source(connection, basis)


def _retirement_for_source(
    connection: sqlite3.Connection,
    basis: _PhysicalRetirement,
) -> _PhysicalRetirement | None:
    source_id, marker_id, artifact, successor, count, source_sha256, _ = (
        basis.source_id,
        basis.marker_id,
        basis.artifact_id,
        basis.successor_id,
        basis.count,
        basis.sha256,
        basis.cursor,
    )
    residue = cast(
        tuple[int] | None,
        connection.execute(
            "SELECT EXISTS(SELECT 1 FROM capture_tail WHERE source_id=?) OR "
            "EXISTS(SELECT 1 FROM capture_conflict WHERE source_id=?) OR "
            "EXISTS(SELECT 1 FROM capture_evidence WHERE source_id=?) OR "
            "EXISTS(SELECT 1 FROM capture_observation WHERE source_id=?) OR "
            "EXISTS(SELECT 1 FROM capture_framing_incident WHERE incident_id=?) OR "
            "EXISTS(SELECT 1 FROM capture_recovery_candidate WHERE source_id=?)",
            (source_id,) * 6,
        ).fetchone(),
    )
    if residue is None or residue[0] != 0:
        return None
    row = cast(
        tuple[str] | None,
        connection.execute(
            "SELECT payload FROM state WHERE kind=? AND id=?", (StateKind.ORDERED_RETIREMENT.value, source_id)
        ).fetchone(),
    )
    cursor = 0
    if row is not None:
        value = cast(object, json.loads(row[0]))
        if not isinstance(value, dict):
            raise ValueError("ordered retirement receipt is invalid")
        data = cast(dict[str, object], value)
        cursor = cast(int, data.get("cursor"))
        if data != {
            "artifact_id": artifact,
            "successor_id": successor,
            "count": count,
            "sha256": source_sha256,
            "cursor": cursor,
        }:
            raise ValueError("ordered retirement receipt changed")
    if type(cursor) is not int or not 0 <= cursor <= count:
        raise ValueError("ordered retirement cursor is invalid")
    return _PhysicalRetirement(source_id, marker_id, artifact, successor, count, source_sha256, cursor)


def _numbered_replay_successor(
    connection: sqlite3.Connection, data: dict[str, object], artifact: str, successor: str, entry: dict[str, object]
) -> bool:
    row = cast(
        tuple[str] | None,
        connection.execute(
            "SELECT payload FROM state WHERE kind=? AND id=?",
            (StateKind.ORDERED_REPLAY_SUCCESSOR.value, data["source_id"]),
        ).fetchone(),
    )
    if row is None or json.loads(row[0]) != {"predecessor_artifact": artifact, "successor_artifact": successor}:
        return False
    lineage = ready.source_lineage(entry)
    return bool(lineage) and lineage[0][0] > cast(int, data["source_start"])


def ordered_replay_handed_off(connection: sqlite3.Connection, data: dict[str, object]) -> bool:
    """Prove that a replay's predecessor and a later physical READY survive ACK."""
    entries, markers = _ledger(connection), _markers(connection)
    artifact = cast(str, data["predecessor_artifact"])
    entry = entries.get(artifact)
    if entry is None or entry.get("state") != "retired" or type(entry.get("order")) is not int:
        return False
    order = cast(int, entry["order"])
    following = [key for key, value in entries.items() if value.get("order") == order + 1]
    if sum(value.get("order") == order for value in entries.values()) != 1 or len(following) != 1:
        return False
    successor = following[0]
    if successor not in markers:
        return _numbered_replay_successor(connection, data, artifact, successor, entries[successor])
    ordering = markers[successor][1].get("ordering")
    if not isinstance(ordering, dict):
        return False
    fields = cast(dict[str, object], ordering)
    next_start = fields.get("source_start")
    identity = (
        fields.get("predecessor_artifact"),
        fields.get("predecessor_source_id"),
        fields.get("predecessor_sha256"),
        fields.get("predecessor_start"),
    )
    expected = (artifact, data["predecessor_source"], data["predecessor_sha256"], data["predecessor_start"])
    if identity != expected or type(next_start) is not int or next_start <= cast(int, data["source_start"]):
        return False
    ordered_physical_predecessor(connection, successor)
    return True


def _replay_candidate(
    connection: sqlite3.Connection,
    source_id: str,
    entries: dict[str, dict[str, object]],
    markers: dict[str, tuple[str, dict[str, object]]],
) -> _PhysicalRetirement | None:
    data = read_ordered_replay_receipt(connection, source_id)
    artifact = cast(str, data["predecessor_artifact"])
    if artifact not in entries or entries[artifact].get("state") != "retired":
        return None
    if artifact not in markers:
        if not ordered_replay_handed_off(connection, data):
            return None
        order = cast(int, entries[artifact]["order"])
        successor = next(key for key, value in entries.items() if value.get("order") == order + 1)
        marker_id = markers[successor][0] if successor in markers else ""
    else:
        marker_id, marker = markers[artifact]
        successor = _successor(connection, artifact, marker_id, entries, markers) if "ordering" in marker else None
        if successor is None:
            return None
    if successor in markers:
        ordering = cast(dict[str, object], markers[successor][1]["ordering"])
        next_start = ordering["source_start"]
    else:
        lineage = ready.source_lineage(entries[successor])
        if not lineage:
            return None
        next_start = lineage[0][0]
    if type(next_start) is not int or next_start <= cast(int, data["source_start"]):
        return None
    basis = _PhysicalRetirement(
        source_id,
        marker_id,
        artifact,
        successor,
        cast(int, data["byte_count"]) // 444,
        cast(str, data["source_sha256"]),
        0,
    )
    return _retirement_for_source(connection, basis)


def _next_candidate(connection: sqlite3.Connection) -> _PhysicalRetirement | None:
    entries, markers = _ledger(connection), _markers(connection)
    for artifact, (marker_id, marker) in markers.items():
        candidate = _candidate(connection, artifact, (marker_id, marker), entries, markers)
        if candidate is not None and candidate.cursor < candidate.count:
            return candidate
    for source_id in sorted(completed_ordered_replays(connection)):
        candidate = _replay_candidate(connection, source_id, entries, markers)
        if candidate is not None and candidate.cursor < candidate.count:
            return candidate
    return None


def retire_one_ordered_physical_page(store: InternalAudioStore, page_limit: int) -> bool:
    """Delete one authenticated physical page after ACK and an ordered successor."""
    if type(page_limit) is not int or not 0 < page_limit <= ready._TYPED_PAGE_MAX:
        raise ValueError("ordered retirement page limit is invalid")
    db = StateDB(store.state_db.path, read_only=True)
    with closing(db.open_connection(read_only=True)) as connection, db.access(connection):
        connection.execute("BEGIN")
        candidate = _next_candidate(connection)
        connection.rollback()
        if candidate is None:
            return False
        if candidate.cursor == 0 and authenticate_source(connection, candidate.source_id) != (
            candidate.count * 444,
            candidate.sha256,
        ):
            raise ValueError("ordered retirement source bytes changed")
    with store.publication_transaction() as (state, connection):
        if _next_candidate(connection) != candidate:
            raise ValueError("ordered retirement custody changed")
        rows = cast(
            list[tuple[int]],
            connection.execute(
                "SELECT ordinal FROM capture_record WHERE source_id=? ORDER BY ordinal LIMIT ?",
                (candidate.source_id, page_limit),
            ).fetchall(),
        )
        count = min(page_limit, candidate.count - candidate.cursor)
        if tuple(row[0] for row in rows) != tuple(range(candidate.cursor, candidate.cursor + count)):
            raise ValueError("ordered retirement source pages changed")
        if candidate.cursor + count == candidate.count:
            extra = cast(
                tuple[int] | None,
                connection.execute(
                    "SELECT 1 FROM capture_record WHERE source_id=? AND ordinal>=? LIMIT 1",
                    (candidate.source_id, candidate.count),
                ).fetchone(),
            )
            if extra is not None:
                raise ValueError("ordered retirement source has extra records")
        mark_retirement_compaction_pending(state)
        deleted = connection.execute(
            "DELETE FROM capture_record WHERE source_id=? AND ordinal>=? AND ordinal<?",
            (candidate.source_id, candidate.cursor, candidate.cursor + count),
        )
        if deleted.rowcount != count:
            raise ValueError("ordered retirement page changed")
        state.put(
            StateKind.ORDERED_RETIREMENT,
            candidate.source_id,
            {
                "artifact_id": candidate.artifact_id,
                "successor_id": candidate.successor_id,
                "count": candidate.count,
                "sha256": candidate.sha256,
                "cursor": candidate.cursor + count,
            },
        )
    return True
