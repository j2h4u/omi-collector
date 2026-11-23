"""Select retained legacy physical audio in observed READ order."""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from typing import cast

from .historical_reconciliation import historical_decision
from .internal_audio_store import decode_uint64
from .physical_continuation import completed_physical_originals
from .ready_ledger import read_ready_ledger


@dataclass(frozen=True, slots=True)
class OrderedPredecessor:
    artifact_id: str
    source_id: str
    source_sha256: str
    source_start: int
    byte_count: int


@dataclass(frozen=True, slots=True)
class OrderedPayload:
    """Physical source custody and its ordering fact, never a numbered audio range."""

    source_id: str
    source_sha256: str
    byte_count: int
    source_start: int
    read_begin: int
    read_count: int
    predecessor: OrderedPredecessor


def select_ordered_payload(connection: sqlite3.Connection) -> OrderedPayload | None:
    """Return the next retained physical READ after an authenticated published repeat.

    A caller owns the stable snapshot. Earlier unaccounted audio and ambiguous READ order
    stop the selection; immutable source start orders observations, not their bytes.
    """
    if not connection.in_transaction:
        raise ValueError("ordered physical selection requires a stable snapshot")
    predecessor = _latest_numbered_predecessor(connection)
    previous_start = predecessor.source_start
    from .ordered_full_replay import completed_ordered_replays

    published = (
        completed_physical_originals(connection)
        | published_ordered_sources(connection)
        | completed_ordered_replays(connection)
    )
    rows = _historical_rows(connection)
    candidates = _ordered_candidates(connection, rows, previous_start, published)
    if not candidates:
        return None
    candidates.sort(key=lambda item: item[0])
    if len(candidates) > 1 and candidates[0][0] == candidates[1][0]:
        raise ValueError("historical audio READ order is ambiguous")
    start, row = candidates[0]
    _require_payload_custody(connection, row)
    return OrderedPayload(
        cast(str, row[0]),
        cast(str, row[6]),
        cast(int, row[5]) * 444,
        start,
        decode_uint64(cast(bytes, row[3])),
        cast(int, row[4]),
        predecessor,
    )


def _historical_rows(connection: sqlite3.Connection) -> list[tuple[object, ...]]:
    return cast(
        list[tuple[object, ...]],
        connection.execute(
            "SELECT s.source_id,s.source_type,s.lifecycle,s.read_begin_start,s.read_begin_count,"
            "s.committed_count,s.committed_sha256,s.certified_prefix_count,s.coverage_digest,"
            "h.prior_source_type,h.prior_lifecycle,h.prior_count,h.prior_sha256,h.prior_certified_count,"
            "h.prior_coverage_digest,"
            "EXISTS(SELECT 1 FROM capture_tail WHERE source_id=s.source_id) OR "
            "EXISTS(SELECT 1 FROM capture_conflict WHERE source_id=s.source_id) OR "
            "EXISTS(SELECT 1 FROM capture_evidence WHERE source_id=s.source_id) OR "
            "EXISTS(SELECT 1 FROM capture_observation WHERE source_id=s.source_id),s.sequence_start,s.sequence_limit "
            "FROM capture_historical_custody h JOIN capture_source s ON s.source_id=h.source_id "
            "WHERE h.disposition='pending' ORDER BY s.sequence_start,s.source_id"
        ).fetchall(),
    )


def _pending_physical_start(connection: sqlite3.Connection, row: tuple[object, ...]) -> int | None:
    if not isinstance(row[16], bytes):
        raise ValueError("historical audio lacks source order")
    start = decode_uint64(row[16])
    if (row[1], row[2]) == ("quarantine", "blocked"):
        _require_payload_custody(connection, row)
        return start
    if (row[1], row[2]) != ("attempt", "sealed_waiting") or not isinstance(row[17], bytes):
        raise ValueError("historical source custody is unresolved")
    if start + cast(int, row[5]) < decode_uint64(row[17]):
        _require_payload_custody(connection, row)
        return start
    if historical_decision(connection, cast(str, row[0])) != "legacy_custody":
        raise ValueError("historical source decision is unresolved")
    return None


def next_ordered_physical_start(connection: sqlite3.Connection) -> int | None:
    """Bound a normal flush by the first pending physical payload in source order."""
    if not connection.in_transaction:
        raise ValueError("ordered physical selection requires a stable snapshot")
    from .ordered_full_replay import completed_ordered_replays

    published = (
        completed_physical_originals(connection)
        | published_ordered_sources(connection)
        | completed_ordered_replays(connection)
    )
    try:
        for row in _historical_rows(connection):
            if row[0] in published:
                continue
            if row[5] == 0:
                if not _empty_custody_is_proven(connection, row):
                    raise ValueError("historical empty source custody is unresolved")
                continue
            start = _pending_physical_start(connection, row)
            if start is not None:
                return start
    except ValueError:
        return None
    return None


def _latest_numbered_predecessor(connection: sqlite3.Connection) -> OrderedPredecessor:
    import json

    from . import ready_bundles as ready
    from .collector_state_db import StateKind
    from .physical_source import source_identity

    rows = cast(
        list[tuple[str, str]],
        connection.execute(
            "SELECT id,payload FROM state WHERE kind=? AND id!='frontier'", (StateKind.READY_PUBLICATION.value,)
        ).fetchall(),
    )
    entries: list[tuple[str, dict[str, object]]] = []
    for artifact_id, payload in rows:
        value = cast(object, json.loads(payload))
        if not isinstance(value, dict) or not ready._valid_ledger_entry(artifact_id, cast(dict[str, object], value)):
            raise ValueError("ordered physical predecessor ledger is invalid")
        entry = cast(dict[str, object], value)
        if type(entry.get("order")) is int:
            entries.append((artifact_id, entry))
    if not entries:
        raise ValueError("ordered physical predecessor is absent")
    artifact_id, latest = max(entries, key=lambda item: cast(int, item[1]["order"]))
    if latest.get("lineage_kind") == "physical_byte_span":
        return _physical_predecessor(connection, artifact_id, latest)
    lineage = ready.source_lineage(latest)
    if not lineage:
        raise ValueError("ordered physical predecessor lineage is absent")
    start, end, digest = lineage[-1]
    candidates = cast(
        list[tuple[object, ...]],
        connection.execute(
            "SELECT source_id,committed_count FROM capture_source WHERE sequence_start=? AND committed_sha256=? "
            "AND lifecycle IN ('sealed_waiting','covered','retiring') ORDER BY source_id",
            (start.to_bytes(8, "big"), digest),
        ).fetchall(),
    )
    matches = [
        (cast(str, source_id), count) for source_id, count in candidates if type(count) is int and start + count == end
    ]
    if not matches:
        raise ValueError("ordered physical predecessor raw custody is unavailable")
    source_id, count = matches[0]
    if source_identity(connection, source_id) != (cast(int, count) * 444, digest):
        raise ValueError("ordered physical predecessor custody changed")
    return OrderedPredecessor(artifact_id, source_id, digest, start, cast(int, count) * 444)


def _physical_predecessor(
    connection: sqlite3.Connection, artifact_id: str, entry: dict[str, object]
) -> OrderedPredecessor:
    import json
    from hashlib import sha256

    from ..domain.physical_publication import validate_physical_descriptor
    from . import ready_bundles as ready
    from .collector_state_db import StateKind

    rows = cast(
        list[tuple[str, str]],
        connection.execute("SELECT id,payload FROM state WHERE kind=?", (StateKind.PHYSICAL_IMPORT.value,)).fetchall(),
    )
    matches: list[tuple[str, dict[str, object]]] = []
    for marker_id, raw in rows:
        value = cast(object, json.loads(raw))
        if isinstance(value, dict):
            marker = cast(dict[str, object], value)
            if marker.get("state") == "published" and marker.get("artifact_id") == artifact_id:
                matches.append((marker_id, marker))
    if len(matches) != 1:
        raise ValueError("ordered physical predecessor marker is missing or ambiguous")
    marker_id, marker = matches[0]
    if not isinstance(marker.get("ordering"), dict):
        raise ValueError("ordered physical predecessor lacks ordering proof")
    descriptor = validate_physical_descriptor(
        {
            "format": "omi-audio-v2-ordered-physical-v1",
            **{
                key: entry[key]
                for key in ("source_id", "source_sha256", "source_bytes", "spans", "seams", "record_count")
            },
            "ordering": marker["ordering"],
            "overlap": marker["overlap"],
            "record_projection_sha256": marker["record_projection_sha256"],
        }
    )
    digest = sha256(ready._canonical(descriptor)).hexdigest()
    if (
        marker.get("source_id") != descriptor["source_id"]
        or marker.get("descriptor_sha256") != digest
        or marker_id != sha256(f"{digest}:0:{artifact_id}".encode()).hexdigest()
    ):
        raise ValueError("ordered physical predecessor proof changed")
    ready._v2_byte_span_projection(ready._canonical(descriptor), entry)
    source_id = cast(str, descriptor["source_id"])
    custody = cast(
        tuple[object, ...] | None,
        connection.execute(
            "SELECT s.committed_count,s.committed_sha256,h.prior_sha256,h.disposition "
            "FROM capture_source s JOIN capture_historical_custody h ON s.source_id=h.source_id WHERE s.source_id=?",
            (source_id,),
        ).fetchone(),
    )
    if custody is None or tuple(custody) != (
        cast(int, descriptor["source_bytes"]) // 444,
        descriptor["source_sha256"],
        descriptor["source_sha256"],
        "pending",
    ):
        raise ValueError("ordered physical predecessor custody changed")
    ordering = cast(dict[str, object], descriptor["ordering"])
    return OrderedPredecessor(
        artifact_id,
        source_id,
        cast(str, descriptor["source_sha256"]),
        cast(int, ordering["source_start"]),
        cast(int, descriptor["source_bytes"]),
    )


def published_ordered_sources(connection: sqlite3.Connection) -> frozenset[str]:
    import json

    from .collector_state_db import StateKind

    result: set[str] = set()
    rows = cast(
        list[tuple[str]],
        connection.execute("SELECT payload FROM state WHERE kind=?", (StateKind.PHYSICAL_IMPORT.value,)).fetchall(),
    )
    for (raw,) in rows:
        value = cast(object, json.loads(raw))
        if not isinstance(value, dict):
            raise ValueError("ordered physical marker is invalid")
        marker = cast(dict[str, object], value)
        if marker.get("state") != "published" or "ordering" not in marker:
            continue
        artifact_id = marker.get("artifact_id")
        if not isinstance(artifact_id, str):
            raise ValueError("ordered physical ledger is absent")
        try:
            entry = read_ready_ledger(connection, artifact_id)
        except ValueError as error:
            raise ValueError("ordered physical ledger is invalid") from error
        if entry is None:
            raise ValueError("ordered physical ledger is absent")
        result.add(_physical_predecessor(connection, artifact_id, entry).source_id)
    return frozenset(result)


def _ordered_candidates(
    connection: sqlite3.Connection, rows: list[tuple[object, ...]], previous_start: int, published: frozenset[str]
) -> list[tuple[int, tuple[object, ...]]]:
    candidates: list[tuple[int, tuple[object, ...]]] = []
    for row in rows:
        if row[0] in published:
            continue
        if row[5] == 0:
            if _empty_custody_is_proven(connection, row):
                continue
            raise ValueError("historical empty source has unresolved custody")
        if not isinstance(row[3], bytes):
            raise ValueError("historical audio has no observed READ order")
        if not isinstance(row[16], bytes):
            raise ValueError("historical audio has no initial source order")
        start = decode_uint64(row[16])
        if start <= previous_start:
            raise ValueError("historical audio precedes its established predecessor")
        candidates.append((start, row))
    return candidates


def _empty_custody_is_proven(connection: sqlite3.Connection, row: tuple[object, ...]) -> bool:
    from hashlib import sha256

    empty_digest = sha256().hexdigest()
    if row[11] != 0 or row[6] != empty_digest or row[12] != empty_digest or row[15]:
        return False
    source_id = row[0]
    missing = cast(
        tuple[int] | None,
        connection.execute(
            "SELECT EXISTS(SELECT 1 FROM capture_record WHERE source_id=?) OR "
            "EXISTS(SELECT 1 FROM capture_read_leg WHERE source_id=?) OR "
            "EXISTS(SELECT 1 FROM capture_recovery_candidate WHERE source_id=?) OR "
            "EXISTS(SELECT 1 FROM capture_framing_incident WHERE incident_id=?) OR "
            "EXISTS(SELECT 1 FROM publication_cohort_member WHERE source_id=?) OR "
            "EXISTS(SELECT 1 FROM ready_stage_range WHERE source_id=?) OR "
            "EXISTS(SELECT 1 FROM ready_v2_stage_range WHERE source_id=?)",
            (source_id,) * 7,
        ).fetchone(),
    )
    return missing is not None and missing[0] == 0


def _require_payload_custody(connection: sqlite3.Connection, row: tuple[object, ...]) -> None:
    if (row[1], row[2]) not in {("quarantine", "blocked"), ("attempt", "sealed_waiting")}:
        raise ValueError("historical payload requires separate custody review")
    if (row[1], row[2], row[5], row[6], row[7], row[8]) != tuple(row[9:15]):
        raise ValueError("historical payload custody changed")
    if type(row[4]) is not int or row[4] <= 0 or type(row[5]) is not int or row[5] <= 0:
        raise ValueError("historical payload READ evidence is invalid")
    if (row[1], row[2]) == ("quarantine", "blocked") and (row[7] != row[5] or row[8] != row[6]):
        raise ValueError("historical payload lacks a durable certified prefix")
    if row[15] and not _has_isolated_tail(connection, row):
        raise ValueError("historical payload requires separate custody review")
    if historical_decision(connection, cast(str, row[0])) is None:
        raise ValueError("historical payload lacks a durable custody decision")


def _has_isolated_tail(connection: sqlite3.Connection, row: tuple[object, ...]) -> bool:
    """Allow full certified records while retaining a separate, bounded partial tail."""
    if (row[1], row[2]) != ("quarantine", "blocked"):
        return False
    source_id = row[0]
    count = cast(int, row[5])
    result = cast(
        tuple[int] | None,
        connection.execute(
            "SELECT EXISTS(SELECT 1 FROM capture_tail WHERE source_id=? AND complete_high_water=? "
            "AND byte_offset=? AND typeof(bytes)='blob' AND length(bytes) BETWEEN 1 AND 443 "
            "AND received_high_water=byte_offset+length(bytes)) "
            "AND NOT EXISTS(SELECT 1 FROM capture_conflict WHERE source_id=?) "
            "AND NOT EXISTS(SELECT 1 FROM capture_evidence WHERE source_id=?) "
            "AND NOT EXISTS(SELECT 1 FROM capture_observation WHERE source_id=?) "
            "AND NOT EXISTS(SELECT 1 FROM capture_framing_incident WHERE incident_id=?) "
            "AND NOT EXISTS(SELECT 1 FROM capture_recovery_candidate WHERE source_id=? OR incident_id=?)",
            (source_id, count, count * 444, source_id, source_id, source_id, source_id, source_id, source_id),
        ).fetchone(),
    )
    return result is not None and result[0] == 1


def pending_physical_successor(connection: sqlite3.Connection, source_id: str) -> bool:
    """Keep only the latest numbered source while later historical audio is pending."""
    import json

    from . import ready_bundles as ready
    from .collector_state_db import StateKind

    source = cast(
        tuple[object, ...] | None,
        connection.execute(
            "SELECT sequence_start,committed_count,committed_sha256 FROM capture_source WHERE source_id=?", (source_id,)
        ).fetchone(),
    )
    if source is None:
        return False
    rows = cast(
        list[tuple[str, str]],
        connection.execute(
            "SELECT id,payload FROM state WHERE kind=? AND id!='frontier'", (StateKind.READY_PUBLICATION.value,)
        ).fetchall(),
    )
    entries: list[tuple[str, dict[str, object]]] = []
    for artifact_id, raw in rows:
        value = cast(object, json.loads(raw))
        if isinstance(value, dict):
            entry = cast(dict[str, object], value)
            if isinstance(entry.get("order"), int):
                entries.append((artifact_id, entry))
    if not entries:
        return False
    artifact_id, latest = max(entries, key=lambda pair: cast(int, pair[1]["order"]))
    if not ready._valid_ledger_entry(artifact_id, latest) or latest.get("lineage_kind") == "physical_byte_span":
        return False
    lineage = ready.source_lineage(latest)
    if not lineage:
        return False
    start, end, digest = lineage[-1]
    if (source[0], source[1], source[2]) != (start.to_bytes(8, "big"), end - start, digest):
        return False
    return (
        connection.execute(
            "SELECT 1 FROM capture_historical_custody h JOIN capture_source s ON s.source_id=h.source_id "
            "WHERE h.disposition='pending' AND s.committed_count>0 AND s.sequence_start>? LIMIT 1",
            (source[0],),
        ).fetchone()
        is not None
    )


def ordered_physical_predecessor(connection: sqlite3.Connection, artifact_id: str) -> OrderedPredecessor:
    """Authenticate a published ordered physical artifact and its retained source."""
    try:
        entry = read_ready_ledger(connection, artifact_id)
    except ValueError as error:
        raise ValueError("ordered physical ledger is invalid") from error
    if entry is None:
        raise ValueError("ordered physical ledger is absent")
    return _physical_predecessor(connection, artifact_id, entry)
