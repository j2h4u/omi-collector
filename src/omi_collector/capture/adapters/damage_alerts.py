"""Persist the first recovery result alongside the transition that established it."""

from __future__ import annotations

import sqlite3
from typing import Literal, cast

Outcome = Literal["recovered", "quarantined"]


def enroll_new_damage_incident(connection: sqlite3.Connection, incident_id: str) -> None:
    """Mark only incidents created after notification support was activated."""
    connection.execute(
        "INSERT INTO state(kind,id,payload) "
        "SELECT 'audio_damage_eligible',incident_id,'{}' FROM capture_framing_incident incident "
        "WHERE incident_id=? AND NOT EXISTS "
        "(SELECT 1 FROM capture_historical_custody WHERE source_id=incident.incident_id)",
        (incident_id,),
    )


def record_damage_outcome(connection: sqlite3.Connection, incident_id: str, outcome: Outcome) -> None:
    """Keep one immutable result for confirmed damage; never backfill old incidents."""
    connection.execute(
        "INSERT INTO state(kind,id,payload) "
        "SELECT 'audio_damage_alert',incident_id,? FROM capture_framing_incident "
        "WHERE incident_id=? AND confirmed_damage=1 "
        "AND EXISTS (SELECT 1 FROM state WHERE kind='audio_damage_eligible' AND id=incident_id) "
        "AND NOT EXISTS (SELECT 1 FROM capture_historical_custody WHERE source_id=incident_id) "
        "ON CONFLICT(kind,id) DO NOTHING",
        ('{"outcome":"' + outcome + '"}', incident_id),
    )


def record_interrupted_recovery(connection: sqlite3.Connection, source_id: str, generation: int) -> None:
    """Record an unsuccessful started retry without altering incident settlement."""
    rows = cast(
        list[tuple[str]],
        connection.execute(
            "SELECT incident.incident_id FROM capture_recovery_candidate candidate "
            "JOIN capture_framing_incident incident ON incident.incident_id=candidate.incident_id "
            "JOIN capture_read_leg leg ON leg.source_id=candidate.source_id AND leg.leg_generation=candidate.leg_generation "
            "WHERE candidate.source_id=? AND candidate.leg_generation=? AND incident.status='unresolved' "
            "AND incident.confirmed_frontier=leg.read_start AND leg.done_next IS NULL",
            (source_id, generation),
        ).fetchall(),
    )
    for (incident_id,) in rows:
        record_damage_outcome(connection, incident_id, "quarantined")
