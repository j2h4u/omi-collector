"""Prepare legacy custody decisions without inventing completed READ evidence."""

from __future__ import annotations

import json
import re
import sqlite3
from dataclasses import dataclass
from typing import cast

from .collector_state_db import StateKind
from .confirmed_loss import (
    ConfirmedLossFact,
    ConfirmedLossLedger,
)

_HEX_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_IDENTITY_FIELDS = 10


@dataclass(frozen=True, slots=True)
class HistoricalDecision:
    source_id: str
    decision: str
    source_identity: tuple[object, ...]
    prior_identity: tuple[object, ...]
    loss_evidence: tuple[object, ...] | None

    def payload(self) -> dict[str, object]:
        return {
            "version": 1,
            "decision": self.decision,
            "source_identity": list(self.source_identity),
            "prior_identity": list(self.prior_identity),
            "loss_evidence": None if self.loss_evidence is None else list(self.loss_evidence),
        }


def _rows(connection: sqlite3.Connection, source_id: str | None = None) -> list[tuple[object, ...]]:
    query = (
        "SELECT s.source_id,s.source_type,s.lifecycle,s.sequence_start,s.sequence_limit,s.committed_count,"
        "s.committed_sha256,s.certified_prefix_count,s.coverage_digest,s.block_reason,s.quarantine_disposition,"
        "s.terminal_time,s.retention_deadline,h.prior_source_type,h.prior_lifecycle,h.prior_count,h.prior_sha256,"
        "h.prior_certified_count,h.prior_coverage_digest,h.disposition,s.read_begin_start,s.read_begin_count "
        "FROM capture_historical_custody h JOIN capture_source s ON s.source_id=h.source_id "
        "WHERE h.disposition IN ('pending','admitted') "
    )
    parameters: tuple[str, ...] = ()
    if source_id is not None:
        query += "AND h.source_id=? "
        parameters = (source_id,)
    cursor = connection.execute(query + "ORDER BY s.sequence_start,s.source_id", parameters)
    return cast(list[tuple[object, ...]], cursor.fetchall())


def _valid_hash(value: object) -> bool:
    return isinstance(value, str) and _HEX_SHA256.fullmatch(value) is not None


def _valid_retention(start: object, end: object) -> bool:
    try:
        return isinstance(start, str) and isinstance(end, str) and int(end) > int(start) >= 0
    except ValueError:
        return False


def _candidate(
    row: tuple[object, ...], known_ambiguous: frozenset[str], facts: tuple[ConfirmedLossFact, ...]
) -> HistoricalDecision:
    source_id = cast(str, row[0])
    start, end = (int.from_bytes(cast(bytes, value), "big") for value in row[3:5])
    begin = None if row[20] is None else int.from_bytes(cast(bytes, row[20]), "big")
    source_identity = (row[1], row[2], start, end, *row[5:9], begin, row[21])
    prior_identity = tuple(row[13:19])
    matching = (row[1], row[2], row[5], row[6], row[7], row[8]) == prior_identity
    count = row[5]
    sealed = (
        row[1] in {"attempt", "draft", "quarantine"}
        and row[2] == "sealed_waiting"
        and type(count) is int
        and count > 0
        and count == end - start
        and _valid_hash(row[6])
    )
    prefix_count = row[7]
    prefix = (
        row[1] == "quarantine"
        and row[2] == "blocked"
        and row[10] == "published_prefix"
        and row[9] == "terminal prefix residual"
        and type(count) is int
        and type(prefix_count) is int
        and 0 < prefix_count <= count
        and start + prefix_count <= end
        and _valid_hash(row[8])
        and _valid_retention(row[11], row[12])
    )
    matching_losses = (
        tuple(
            fact
            for fact in facts
            if fact.attempt_id == source_id and fact.start_sequence == start + cast(int, prefix_count)
        )
        if prefix
        else ()
    )
    loss = matching_losses[0] if len(matching_losses) == 1 else None
    decision = (
        "legacy_custody"
        if matching
        and source_id not in known_ambiguous
        and (sealed or (prefix and prefix_count == count and loss is not None))
        else "unknown_held"
    )
    evidence = (loss.loss_id, loss.start_sequence, loss.end_sequence) if decision == "legacy_custody" and loss else None
    return HistoricalDecision(source_id, decision, source_identity, prior_identity, evidence)


def _loss_facts(connection: sqlite3.Connection) -> tuple[ConfirmedLossFact, ...]:
    row = cast(
        tuple[object, ...] | None,
        connection.execute("SELECT payload FROM state WHERE kind='confirmed_loss' AND id='ledger'").fetchone(),
    )
    if row is None:
        raise ValueError("confirmed loss ledger is missing")
    value = cast(object, json.loads(cast(str, row[0])))
    if not isinstance(value, dict):
        raise ValueError("confirmed loss ledger is invalid")
    return ConfirmedLossLedger._decode_ledger(cast(dict[str, object], value))


def historical_decision(connection: sqlite3.Connection, source_id: str) -> str | None:
    """Return an unchanged durable decision, refusing stale or malformed provenance."""
    raw = cast(
        tuple[object, ...] | None,
        connection.execute(
            "SELECT payload FROM state WHERE kind=? AND id=?", (StateKind.HISTORICAL_RESOLUTION.value, source_id)
        ).fetchone(),
    )
    if raw is None:
        return None
    value = cast(object, json.loads(cast(str, raw[0])))
    if not isinstance(value, dict):
        raise ValueError("historical decision is invalid")
    payload = cast(dict[str, object], value)
    if (
        set(payload) != {"version", "decision", "source_identity", "prior_identity", "loss_evidence"}
        or type(payload["version"]) is not int
        or payload["version"] != 1
    ):
        raise ValueError("historical decision is invalid")
    decision = payload["decision"]
    if decision not in {"legacy_custody", "unknown_held"}:
        raise ValueError("historical decision is invalid")
    current = next(iter(_rows(connection, source_id)), None)
    if current is None:
        raise ValueError("historical source is no longer pending")
    if current[19] == "admitted":
        if not _admitted_matches(current, payload, _loss_facts(connection)):
            raise ValueError("historical decision no longer matches custody")
        return cast(str, decision)
    expected = _candidate(
        current, frozenset({source_id}) if decision == "unknown_held" else frozenset(), _loss_facts(connection)
    )
    if expected.payload() != payload:
        raise ValueError("historical decision no longer matches custody")
    return cast(str, decision)


def _admitted_matches(
    row: tuple[object, ...], payload: dict[str, object], facts: tuple[ConfirmedLossFact, ...]
) -> bool:
    identity = payload.get("source_identity")
    if not isinstance(identity, list):
        return False
    identity = cast(list[object], identity)
    if len(identity) != _IDENTITY_FIELDS or payload.get("decision") != "legacy_custody":
        return False
    start, end = (int.from_bytes(cast(bytes, value), "big") for value in row[3:5])
    begin = None if row[20] is None else int.from_bytes(cast(bytes, row[20]), "big")
    immutable: list[object] = [row[1], identity[1], start, end, row[5], row[6], row[7], identity[7], begin, row[21]]
    if (
        identity != immutable
        or row[2] not in {identity[1], "covered", "retiring"}
        or payload["prior_identity"] != list(row[13:19])
        or [identity[0], identity[1], identity[4], identity[5], identity[6], identity[7]] != list(row[13:19])
    ):
        return False
    evidence = payload["loss_evidence"]
    if evidence is None:
        return identity[1] == "sealed_waiting"
    return identity[1] == "blocked" and any(
        evidence == [fact.loss_id, fact.start_sequence, fact.end_sequence]
        and fact.attempt_id == row[0]
        and fact.start_sequence == start + cast(int, row[7])
        for fact in facts
    )


def historical_loss_end(connection: sqlite3.Connection, source_id: str) -> int | None:
    """Return the end of a validated legacy prefix loss fact, if present."""
    if historical_decision(connection, source_id) != "legacy_custody":
        return None
    row = cast(
        tuple[object, ...],
        connection.execute(
            "SELECT payload FROM state WHERE kind=? AND id=?", (StateKind.HISTORICAL_RESOLUTION.value, source_id)
        ).fetchone(),
    )
    payload = cast(dict[str, object], json.loads(cast(str, row[0])))
    evidence = payload["loss_evidence"]
    return cast(int, evidence[2]) if isinstance(evidence, list) else None
