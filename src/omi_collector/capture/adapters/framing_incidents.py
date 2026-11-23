"""Read and update durable framing boundaries used by publication."""

from __future__ import annotations

from contextlib import closing
from typing import cast

from .collector_state_db import StateDB
from .damage_alerts import record_damage_outcome
from .internal_audio_store import decode_uint64, encode_uint64


def active_framing_frontier(state_db: StateDB) -> int | None:
    """Return the earliest unresolved confirmed frontier, if one exists."""
    with closing(state_db.open_connection(read_only=True)) as connection, state_db.access(connection):
        row = cast(
            tuple[object] | None,
            connection.execute(
                "SELECT MIN(confirmed_frontier) FROM capture_framing_incident WHERE status='unresolved'"
            ).fetchone(),
        )
    if row is None or row[0] is None:
        return None
    if not isinstance(row[0], bytes):
        raise ValueError("framing frontier is invalid")
    return decode_uint64(row[0])


def settle_unavailable(state_db: StateDB, read_head: int, write_head: int) -> tuple[str, ...]:
    """Settle gaps whose unresolved frontier is behind a fresh device head."""
    if type(read_head) is not int or type(write_head) is not int or not 0 <= read_head <= write_head:
        raise ValueError("device range is invalid")
    encoded = encode_uint64(read_head)
    with state_db.typed_write_transaction() as connection:
        rows = cast(
            list[tuple[str]],
            connection.execute(
                "SELECT incident_id FROM capture_framing_incident "
                "WHERE status='unresolved' AND confirmed_frontier < ? ORDER BY confirmed_frontier",
                (encoded,),
            ).fetchall(),
        )
        for (incident_id,) in rows:
            record_damage_outcome(connection, incident_id, "quarantined")
        connection.execute(
            "UPDATE capture_framing_incident SET status='settled',settled_head=?, "
            "settlement_reason='device_head_passed_unresolved_frontier' "
            "WHERE status='unresolved' AND confirmed_frontier < ?",
            (encoded, encoded),
        )
    return tuple(row[0] for row in rows)


def settle_recovered(state_db: StateDB, source_id: str | None) -> tuple[str, ...]:
    """Resolve incidents fully covered by a later independent completed source."""
    with state_db.typed_write_transaction() as connection:
        rows = cast(
            list[tuple[str, bytes]],
            connection.execute(
                "SELECT incident.incident_id, MIN(leg.done_next) FROM capture_framing_incident incident "
                "JOIN capture_source original ON original.source_id=incident.incident_id "
                "JOIN capture_recovery_candidate candidate ON candidate.incident_id=incident.incident_id "
                "JOIN capture_source recovery ON recovery.source_id=candidate.source_id AND recovery.lifecycle='sealed_waiting' "
                "JOIN capture_read_leg leg ON leg.source_id=candidate.source_id AND leg.leg_generation=candidate.leg_generation "
                "WHERE incident.status='unresolved' AND incident.verified_conflict=0 "
                "AND (? IS NULL OR candidate.source_id = ?) AND leg.verified_count IS NOT NULL "
                "AND leg.read_start = incident.confirmed_frontier AND leg.done_next >= original.sequence_limit "
                "GROUP BY incident.incident_id",
                (source_id, source_id),
            ).fetchall(),
        )
        for incident_id, end in rows:
            record_damage_outcome(connection, incident_id, "recovered")
            connection.execute(
                "UPDATE capture_framing_incident SET status='settled',settled_head=?, "
                "settlement_reason='independent_complete_read' WHERE incident_id=? AND status='unresolved'",
                (end, incident_id),
            )
    return tuple(row[0] for row in rows)


def has_framing_uncertainty(state_db: StateDB, start: int, end: int) -> bool:
    """Check whether a device interval overlaps a retained unknown gap."""
    if type(start) is not int or type(end) is not int or not 0 <= start < end:
        raise ValueError("device interval is invalid")
    with closing(state_db.open_connection(read_only=True)) as connection, state_db.access(connection):
        row = cast(
            tuple[object] | None,
            connection.execute(
                "SELECT 1 FROM capture_framing_incident WHERE confirmed_frontier < ? AND "
                "(status='unresolved' OR (settlement_reason='device_head_passed_unresolved_frontier' "
                "AND settled_head >= ?)) LIMIT 1",
                (encode_uint64(end), encode_uint64(start)),
            ).fetchone(),
        )
    return row is not None


def extend_uncertain_framing(state_db: StateDB, start: int, read_head: int, write_head: int) -> tuple[str, ...]:
    """Extend an existing unknown gap across a later device-head advance."""
    if (
        type(start) is not int
        or type(read_head) is not int
        or type(write_head) is not int
        or not 0 <= start < read_head <= write_head
    ):
        raise ValueError("device range is invalid")
    lower, upper = encode_uint64(start), encode_uint64(read_head)
    with state_db.typed_write_transaction() as connection:
        rows = cast(
            list[tuple[str]],
            connection.execute(
                "SELECT incident_id FROM capture_framing_incident "
                "WHERE settlement_reason='device_head_passed_unresolved_frontier' "
                "AND confirmed_frontier <= ? AND settled_head >= ? AND settled_head < ?",
                (lower, lower, upper),
            ).fetchall(),
        )
        for (incident_id,) in rows:
            connection.execute(
                "UPDATE capture_framing_incident SET settled_head=? WHERE incident_id=?", (upper, incident_id)
            )
    return tuple(row[0] for row in rows)
