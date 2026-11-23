"""Authenticate the boundary between ordered physical READY and later numbered audio."""

from __future__ import annotations

import json
import sqlite3
from typing import cast

from .collector_state_db import StateKind
from .internal_audio_store import decode_uint64
from .ordered_payloads import ordered_physical_predecessor, published_ordered_sources


def _ledger(connection: sqlite3.Connection) -> dict[str, dict[str, object]]:
    from . import ready_bundles as ready

    result: dict[str, dict[str, object]] = {}
    rows = cast(
        list[tuple[str, str]],
        connection.execute(
            "SELECT id,payload FROM state WHERE kind=? AND id!='frontier'", (StateKind.READY_PUBLICATION.value,)
        ).fetchall(),
    )
    for artifact_id, raw in rows:
        entry = cast(object, json.loads(raw))
        if not isinstance(entry, dict) or not ready._valid_ledger_entry(artifact_id, cast(dict[str, object], entry)):
            raise ValueError("ordered boundary ledger is invalid")
        validated = cast(dict[str, object], entry)
        if validated.get("format") == "omi-audio-v2":
            result[artifact_id] = validated
    return result


def _markers(connection: sqlite3.Connection) -> dict[str, tuple[str, dict[str, object]]]:
    result: dict[str, tuple[str, dict[str, object]]] = {}
    rows = cast(
        list[tuple[str, str]],
        connection.execute("SELECT id,payload FROM state WHERE kind=?", (StateKind.PHYSICAL_IMPORT.value,)).fetchall(),
    )
    for marker_id, raw in rows:
        marker = cast(object, json.loads(raw))
        if not isinstance(marker, dict):
            raise ValueError("ordered boundary marker is invalid")
        marker = cast(dict[str, object], marker)
        if marker.get("state") != "published":
            continue
        artifact = marker.get("artifact_id")
        if not isinstance(artifact, str) or artifact in result:
            raise ValueError("ordered boundary marker is ambiguous")
        result[artifact] = (marker_id, marker)
    return result


def _intervening_source(connection: sqlite3.Connection, source_id: str, start: int, end: int, frontier: int) -> bool:
    from .ordered_full_replay import completed_ordered_replays

    published = published_ordered_sources(connection) | completed_ordered_replays(connection)
    rows = cast(
        list[tuple[str, bytes]],
        connection.execute(
            "SELECT source_id,sequence_start FROM capture_source WHERE source_id!=? AND committed_count>0 "
            "AND lifecycle IN ('prepared','leg_prepared','receiving','interrupted','sealed_waiting','blocked') "
            "AND sequence_start>? AND sequence_start<=?",
            (source_id, start.to_bytes(8, "big"), end.to_bytes(8, "big")),
        ).fetchall(),
    )
    return any(other_id not in published and decode_uint64(raw_start) > frontier for other_id, raw_start in rows)


def _chain_is_valid(
    connection: sqlite3.Connection,
    entries: dict[str, dict[str, object]],
    markers: dict[str, tuple[str, dict[str, object]]],
    current_id: str,
    frontier: int,
) -> bool:
    from . import ready_bundles as ready

    while current_id in markers:
        _, marker = markers[current_id]
        ordered_physical_predecessor(connection, current_id)
        ordering = cast(dict[str, object], marker["ordering"])
        parent = ordering.get("predecessor_artifact")
        if not isinstance(parent, str) or parent not in entries:
            raise ValueError("ordered boundary predecessor is missing")
        if cast(int, entries[parent]["order"]) + 1 != entries[current_id]["order"]:
            raise ValueError("ordered boundary order is not contiguous")
        if parent in markers:
            if not isinstance(markers[parent][1].get("ordering"), dict):
                raise ValueError("ordered boundary physical predecessor lacks ordering")
            prior = ordered_physical_predecessor(connection, parent)
            if (
                ordering.get("predecessor_source_id"),
                ordering.get("predecessor_sha256"),
                ordering.get("predecessor_start"),
            ) != (prior.source_id, prior.source_sha256, prior.source_start):
                raise ValueError("ordered boundary physical predecessor changed")
            current_id = parent
            continue
        if entries[parent].get("lineage_kind") == "physical_byte_span":
            raise ValueError("ordered boundary numbered predecessor is invalid")
        lineage = ready.source_lineage(entries[parent])
        if not lineage:
            raise ValueError("ordered boundary numbered lineage is missing")
        start, end, digest = lineage[-1]
        if (start, end, digest) != (
            ordering.get("predecessor_start"),
            frontier + 1,
            ordering.get("predecessor_sha256"),
        ):
            raise ValueError("ordered boundary numbered predecessor changed")
        return True
    return False


def ordered_physical_boundary(connection: sqlite3.Connection, source_id: str, frontier: int) -> str | None:
    """Return the final physical marker only for a proven contiguous public chain."""
    entries = _ledger(connection)
    if not entries:
        return None
    latest = max(entries, key=lambda key: cast(int, entries[key]["order"]))
    if entries[latest].get("lineage_kind") != "physical_byte_span":
        return None
    markers = _markers(connection)
    if latest not in markers:
        raise ValueError("physical boundary marker is missing")
    latest_marker = markers[latest][1]
    if "anchor" in latest_marker and "ordering" not in latest_marker:
        return None
    if not isinstance(latest_marker.get("ordering"), dict):
        raise ValueError("ordered boundary marker is invalid")
    last_physical = ordered_physical_predecessor(connection, latest)
    candidate = cast(
        tuple[bytes] | None,
        connection.execute("SELECT sequence_start FROM capture_source WHERE source_id=?", (source_id,)).fetchone(),
    )
    if candidate is None:
        raise ValueError("ordered boundary source disappeared")
    next_start = decode_uint64(candidate[0])
    if next_start <= max(frontier, last_physical.source_start) or _intervening_source(
        connection, source_id, last_physical.source_start, next_start, frontier
    ):
        raise ValueError("ordered boundary has an unresolved earlier source")
    if not _chain_is_valid(connection, entries, markers, latest, frontier):
        raise ValueError("ordered boundary chain is incomplete")
    return markers[latest][0]


def ordered_source_held(connection: sqlite3.Connection, source_id: str) -> bool:
    """Keep source bytes required by pending publication or durable replay proof."""
    from .ordered_full_replay import has_ordered_replay_reference
    from .ordered_payloads import pending_physical_successor

    return pending_physical_successor(connection, source_id) or has_ordered_replay_reference(connection, source_id)


def _matching_replay_markers(
    connection: sqlite3.Connection, completed: frozenset[str], latest: str, minimum_start: int, next_start: int
) -> list[tuple[int, str]]:
    from .ordered_full_replay import read_ordered_replay_receipt

    candidates: list[tuple[int, str]] = []
    for replay_id in completed:
        receipt = read_ordered_replay_receipt(connection, replay_id)
        start = cast(int, receipt["source_start"])
        if receipt["predecessor_artifact"] == latest and minimum_start < start < next_start:
            candidates.append((start, cast(str, receipt["receipt_sha256"])))
    return candidates


def ordered_replay_boundary(connection: sqlite3.Connection, source_id: str, frontier: int) -> str | None:
    """Bind a numbered successor to a proven full replay without guessing a loss."""
    from . import ready_bundles as ready
    from .ordered_full_replay import completed_ordered_replays

    completed = completed_ordered_replays(connection)
    if not completed:
        return None
    entries = _ledger(connection)
    if not entries:
        return None
    latest = max(entries, key=lambda key: cast(int, entries[key]["order"]))
    entry = entries[latest]
    if entry.get("lineage_kind") == "physical_byte_span":
        return None
    lineage = ready.source_lineage(entry)
    if not lineage or lineage[-1][1] != frontier + 1:
        return None
    row = cast(
        tuple[bytes] | None,
        connection.execute("SELECT sequence_start FROM capture_source WHERE source_id=?", (source_id,)).fetchone(),
    )
    if row is None:
        raise ValueError("ordered replay successor disappeared")
    next_start = decode_uint64(row[0])
    candidates = _matching_replay_markers(connection, completed, latest, lineage[-1][0], next_start)
    if not candidates:
        return None
    candidates.sort()
    if len(candidates) > 1 and candidates[-1][0] == candidates[-2][0]:
        raise ValueError("ordered replay boundary is ambiguous")
    if _intervening_source(connection, source_id, frontier, next_start, frontier):
        raise ValueError("ordered replay boundary has an unresolved earlier source")
    return candidates[-1][1]
