"""Prepare a bounded, authenticated physical recovery plan from ordered custody."""

from __future__ import annotations

import hashlib
import sqlite3
from functools import partial
from pathlib import Path
from typing import TYPE_CHECKING, Literal, cast

from ..domain.physical_overlap import unique_suffix_prefix_overlap
from ..domain.physical_recovery import PhysicalSegment, PhysicalSpan, iter_physical_records
from .ordered_payloads import OrderedPayload
from .physical_audio import PreparedPhysicalAudio, prepare_physical_audio
from .physical_source import authenticate_source, read_physical_span

if TYPE_CHECKING:
    from .internal_audio_store import InternalAudioStore
    from .ready_bundles import ReadyBundleResult


def prepare_ordered_physical_plan(
    connection: sqlite3.Connection, payload: OrderedPayload
) -> tuple[dict[str, object], PreparedPhysicalAudio]:
    """Freeze source spans and their packet projection without assigning device sequences."""
    read = partial(read_physical_span, connection)
    identity = partial(authenticate_source, connection)
    predecessor = payload.predecessor
    if identity(predecessor.source_id) != (predecessor.byte_count, predecessor.source_sha256):
        raise ValueError("ordered physical predecessor custody changed")
    if identity(payload.source_id) != (payload.byte_count, payload.source_sha256):
        raise ValueError("ordered physical source custody changed")
    previous = PhysicalSpan(predecessor.source_id, 0, predecessor.byte_count)
    current = PhysicalSpan(payload.source_id, 0, payload.byte_count)
    count = unique_suffix_prefix_overlap(
        iter_physical_records(PhysicalSegment((previous,)), read),
        iter_physical_records(PhysicalSegment((current,)), read),
    )
    offset = count * 444
    if offset == payload.byte_count:
        raise ValueError("ordered physical source is wholly overlapping")

    def digest(start: int, length: int) -> str:
        result = hashlib.sha256()
        if length:
            for chunk in read(PhysicalSpan(payload.source_id, start, length)):
                result.update(chunk)
        return result.hexdigest()

    descriptor: dict[str, object] = {
        "format": "omi-audio-v2-ordered-physical-v1",
        "source_id": payload.source_id,
        "source_sha256": payload.source_sha256,
        "source_bytes": payload.byte_count,
        "spans": [
            {
                "byte_start": offset,
                "byte_end": payload.byte_count,
                "sha256": digest(offset, payload.byte_count - offset),
            }
        ],
        "record_count": (payload.byte_count - offset) // 444,
        "seams": [],
        "ordering": {
            "predecessor_artifact": predecessor.artifact_id,
            "predecessor_source_id": predecessor.source_id,
            "predecessor_sha256": predecessor.source_sha256,
            "predecessor_start": predecessor.source_start,
            "source_start": payload.source_start,
            "read_begin": payload.read_begin,
            "read_count": payload.read_count,
        },
        "overlap": {"record_count": count, "sha256": digest(0, offset)},
        "record_projection_sha256": "0" * 64,
    }
    prepared = prepare_physical_audio(descriptor, read, identity)
    descriptor["record_projection_sha256"] = prepared.record_projection_sha256
    return descriptor, prepared


def _historical_frontier_blocks_payload(connection: sqlite3.Connection, payload: OrderedPayload) -> bool:
    from . import ready_bundles as ready
    from .historical_reconciliation import historical_decision
    from .ordered_boundary import _ledger

    predecessor = _ledger(connection).get(payload.predecessor.artifact_id)
    if predecessor is None or predecessor.get("lineage_kind") != "physical_byte_span":
        return False
    if historical_decision(connection, payload.source_id) != "legacy_custody":
        return False
    source = cast(
        tuple[str, str] | None,
        connection.execute(
            "SELECT source_type,lifecycle FROM capture_source WHERE source_id=?", (payload.source_id,)
        ).fetchone(),
    )
    if source is None or tuple(source) != ("attempt", "sealed_waiting"):
        return False
    source_end = payload.source_start + payload.byte_count // 444
    if ready._historical_proven_frontier(connection, source_end, frozenset({payload.source_id})) >= source_end:
        return False
    blocked = cast(
        tuple[int] | None,
        connection.execute(
            "SELECT EXISTS(SELECT 1 FROM capture_framing_incident WHERE status='unresolved' "
            "AND confirmed_frontier<?) OR EXISTS(SELECT 1 FROM capture_source s "
            "WHERE s.source_id!=? AND s.sequence_start>? AND s.sequence_start<=? "
            "AND s.lifecycle IN ('prepared','leg_prepared','receiving','interrupted','sealed_waiting','blocked') "
            "AND NOT EXISTS(SELECT 1 FROM capture_historical_custody h WHERE h.source_id=s.source_id))",
            (
                source_end.to_bytes(8, "big"),
                payload.source_id,
                payload.predecessor.source_start.to_bytes(8, "big"),
                payload.source_start.to_bytes(8, "big"),
            ),
        ).fetchone(),
    )
    return blocked is not None and blocked[0] == 0


def publish_next_ordered_physical(store: InternalAudioStore, root: Path) -> ReadyBundleResult | Literal[True] | None:
    """Publish the next authorized historical payload after normal READY ordering settles."""
    from contextlib import closing

    from .collector_state_db import StateDB
    from .ordered_payloads import next_ordered_physical_start, select_ordered_payload
    from .physical_proof import prepare_physical_proof
    from .physical_source import source_identity
    from .physical_stage import PhysicalStageInput, publish_physical_stage, reserve_physical_stage

    db = StateDB(store.state_db.path, read_only=True)
    with closing(db.open_connection(read_only=True)) as connection, db.access(connection):
        connection.execute("BEGIN")
        boundary = next_ordered_physical_start(connection)
        try:
            payload = select_ordered_payload(connection)
        except ValueError:
            return None
        if payload is None or (
            payload.source_start != boundary and not _historical_frontier_blocks_payload(connection, payload)
        ):
            return None
        connection.rollback()
        try:
            descriptor, prepared = prepare_ordered_physical_plan(connection, payload)
        except ValueError as error:
            if str(error) not in {
                "physical source is a complete replay within its predecessor",
                "ordered physical source is wholly overlapping",
            }:
                raise
            from .ordered_full_replay import try_record_ordered_full_replay

            if not try_record_ordered_full_replay(store, payload):
                raise ValueError("full replay could not be authenticated") from error
            return True
        read = partial(read_physical_span, connection)
        identity = partial(source_identity, connection)
        with prepare_physical_proof(descriptor, read, identity, packet_count=prepared.audio.packet_count) as proof:
            stage = reserve_physical_stage(store, root, descriptor, prepared, proof)
            source = PhysicalStageInput(descriptor, prepared, proof.records_sha256, read, identity)
            return publish_physical_stage(store, stage, source)
