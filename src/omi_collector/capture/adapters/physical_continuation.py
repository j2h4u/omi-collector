"""Authenticate the private evidence linking physical recovery to its numbered repeat."""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from hashlib import sha256
from typing import cast

from ..domain.physical_publication import validate_physical_descriptor
from .collector_state_db import StateKind
from .internal_audio_store import decode_uint64
from .physical_source import authenticate_source


@dataclass(frozen=True, slots=True)
class PhysicalContinuation:
    marker_id: str
    artifact_id: str
    original_id: str
    repeat_id: str
    repeat_sha256: str
    repeat_start: int
    repeat_end: int


def _load(connection: sqlite3.Connection, kind: StateKind, item_id: str) -> dict[str, object] | None:
    row = cast(
        tuple[str] | None,
        connection.execute("SELECT payload FROM state WHERE kind=? AND id=?", (kind.value, item_id)).fetchone(),
    )
    if row is None:
        return None
    value = cast(object, json.loads(row[0]))
    if not isinstance(value, dict):
        raise ValueError("physical continuation evidence is invalid")
    return cast(dict[str, object], value)


def physical_continuation(connection: sqlite3.Connection, marker_id: str) -> PhysicalContinuation:
    """Verify published physical proof and independently authenticate the numbered repeat.

    The caller owns one stable read snapshot; no raw data or inferred positions are emitted.
    """
    from . import ready_bundles as ready

    if not connection.in_transaction:
        raise ValueError("physical continuation requires a stable read snapshot")
    marker = _load(connection, StateKind.PHYSICAL_IMPORT, marker_id)
    fields = {"state", "artifact_id", "source_id", "anchor", "record_projection_sha256", "descriptor_sha256"}
    if marker is None or set(marker) != fields or marker["state"] != "published":
        raise ValueError("physical continuation marker is invalid")
    artifact_id = marker["artifact_id"]
    if not isinstance(artifact_id, str):
        raise ValueError("physical continuation artifact is invalid")
    entry = _load(connection, StateKind.READY_PUBLICATION, artifact_id)
    if (
        entry is None
        or not ready._valid_ledger_entry(artifact_id, entry)
        or entry.get("lineage_kind") != "physical_byte_span"
    ):
        raise ValueError("physical continuation has no valid READY custody")
    descriptor = validate_physical_descriptor(
        {
            "format": "omi-audio-v2-physical-byte-span-v1",
            **{
                key: entry[key]
                for key in ("source_id", "source_sha256", "source_bytes", "spans", "seams", "record_count")
            },
            "record_projection_sha256": marker["record_projection_sha256"],
            "anchor": marker["anchor"],
        }
    )
    encoded = ready._canonical(descriptor)
    digest = sha256(encoded).hexdigest()
    if (
        descriptor["source_id"] != marker["source_id"]
        or digest != marker["descriptor_sha256"]
        or sha256(f"{digest}:0:{artifact_id}".encode()).hexdigest() != marker_id
    ):
        raise ValueError("physical continuation descriptor does not match its published marker")
    _require_original_custody(connection, descriptor)
    anchor = cast(dict[str, object], descriptor["anchor"])
    repeat_id = cast(str, anchor["repeat_source_id"])
    row = cast(
        tuple[object, ...] | None,
        connection.execute(
            "SELECT s.sequence_start,s.sequence_limit,s.committed_count,s.committed_sha256,h.prior_sha256,"
            "h.prior_lifecycle,h.disposition,s.source_type,s.lifecycle,h.prior_source_type,h.prior_count "
            "FROM capture_source s JOIN capture_historical_custody h "
            "ON h.source_id=s.source_id WHERE s.source_id=?",
            (repeat_id,),
        ).fetchone(),
    )
    if row is None or row[3] != anchor["repeat_source_sha256"] or row[4] != row[3] or row[5] != "sealed_waiting":
        raise ValueError("physical continuation repeat lacks sealed custody")
    if (
        row[6] not in {"pending", "admitted"}
        or type(row[2]) is not int
        or anchor["repeat_offset"] != 0
        or (row[7], row[8], row[10]) != (row[9], "sealed_waiting", row[2])
    ):
        raise ValueError("physical continuation repeat range is invalid")
    start, end = decode_uint64(cast(bytes, row[0])), decode_uint64(cast(bytes, row[1]))
    if start + cast(int, row[2]) != end or authenticate_source(connection, repeat_id) != (
        cast(int, row[2]) * 444,
        row[3],
    ):
        raise ValueError("physical continuation repeat bytes do not match sealed custody")
    return PhysicalContinuation(
        marker_id, artifact_id, cast(str, descriptor["source_id"]), repeat_id, cast(str, row[3]), start, end
    )


def _require_original_custody(connection: sqlite3.Connection, descriptor: dict[str, object]) -> None:
    original = cast(
        tuple[object, ...] | None,
        connection.execute(
            "SELECT s.committed_count,s.committed_sha256,h.prior_sha256,h.disposition "
            "FROM capture_source s JOIN capture_historical_custody h ON h.source_id=s.source_id "
            "WHERE s.source_id=?",
            (descriptor["source_id"],),
        ).fetchone(),
    )
    if (
        original is None
        or type(original[0]) is not int
        or original[0] * 444 != descriptor["source_bytes"]
        or original[1] != descriptor["source_sha256"]
        or original[2:] != (original[1], "pending")
    ):
        raise ValueError("physical continuation original custody changed")


def completed_physical_continuation(connection: sqlite3.Connection, marker_id: str) -> dict[str, object] | None:
    """Validate the durable proof that the numbered repeat reached READY completely."""
    receipt = _load(connection, StateKind.PHYSICAL_CONTINUATION, marker_id)
    if receipt is None:
        return None
    fields = {
        "marker_id",
        "physical_artifact_id",
        "repeat_id",
        "repeat_sha256",
        "repeat_start",
        "repeat_end",
        "last_artifact_id",
        "receipt_sha256",
    }
    digest = receipt.get("receipt_sha256")
    content = {key: value for key, value in receipt.items() if key != "receipt_sha256"}
    encoded = json.dumps(content, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
    if set(receipt) != fields or receipt["marker_id"] != marker_id or digest != sha256(encoded).hexdigest():
        raise ValueError("physical continuation receipt is invalid")
    marker = _load(connection, StateKind.PHYSICAL_IMPORT, marker_id)
    if (
        marker is None
        or set(marker)
        != {"state", "artifact_id", "source_id", "anchor", "record_projection_sha256", "descriptor_sha256"}
        or marker.get("state") != "published"
        or marker.get("artifact_id") != receipt["physical_artifact_id"]
    ):
        raise ValueError("physical continuation receipt lost its marker")
    anchor = marker.get("anchor")
    if not isinstance(anchor, dict):
        raise ValueError("physical continuation receipt anchor is invalid")
    anchor = cast(dict[str, object], anchor)
    if (
        anchor.get("repeat_source_id") != receipt["repeat_id"]
        or anchor.get("repeat_source_sha256") != receipt["repeat_sha256"]
    ):
        raise ValueError("physical continuation receipt does not match its anchor")
    _verify_completed_ledger(connection, marker_id, marker, receipt, anchor)
    return receipt


def _verify_completed_ledger(
    connection: sqlite3.Connection,
    marker_id: str,
    marker: dict[str, object],
    receipt: dict[str, object],
    anchor: dict[str, object],
) -> None:
    from . import ready_bundles as ready

    physical_id, last_id = receipt["physical_artifact_id"], receipt["last_artifact_id"]
    if not isinstance(physical_id, str) or not isinstance(last_id, str):
        raise ValueError("physical continuation receipt artifact is invalid")
    physical = _load(connection, StateKind.READY_PUBLICATION, physical_id)
    last = _load(connection, StateKind.READY_PUBLICATION, last_id)
    if (
        physical is None
        or last is None
        or not ready._valid_ledger_entry(physical_id, physical)
        or not ready._valid_ledger_entry(last_id, last)
    ):
        raise ValueError("physical continuation receipt ledger is invalid")
    if (
        physical.get("lineage_kind") != "physical_byte_span"
        or cast(int, last["order"]) <= cast(int, physical["order"])
        or last.get("next_sequence") != receipt["repeat_end"]
    ):
        raise ValueError("physical continuation receipt order is invalid")
    descriptor = validate_physical_descriptor(
        {
            "format": "omi-audio-v2-physical-byte-span-v1",
            **{
                key: physical[key]
                for key in ("source_id", "source_sha256", "source_bytes", "spans", "seams", "record_count")
            },
            "record_projection_sha256": marker.get("record_projection_sha256"),
            "anchor": anchor,
        }
    )
    _require_original_custody(connection, descriptor)
    digest = sha256(ready._canonical(descriptor)).hexdigest()
    if (
        descriptor["source_id"] != marker.get("source_id")
        or digest != marker.get("descriptor_sha256")
        or sha256(f"{digest}:0:{physical_id}".encode()).hexdigest() != marker_id
    ):
        raise ValueError("physical continuation receipt descriptor is invalid")
    if (
        type(receipt["repeat_start"]) is not int
        or type(receipt["repeat_end"]) is not int
        or receipt["repeat_start"] >= receipt["repeat_end"]
    ):
        raise ValueError("physical continuation receipt range is invalid")


def find_physical_continuation(
    connection: sqlite3.Connection, source_ids: frozenset[str]
) -> PhysicalContinuation | None:
    """Find the one published physical proof whose repeat is included in this cohort."""
    if not connection.in_transaction:
        raise ValueError("physical continuation requires a stable read snapshot")
    rows = cast(
        list[tuple[str, str]],
        connection.execute(
            "SELECT id,payload FROM state WHERE kind=? ORDER BY id", (StateKind.PHYSICAL_IMPORT.value,)
        ).fetchall(),
    )
    found: list[PhysicalContinuation] = []
    for marker_id, raw in rows:
        marker = cast(object, json.loads(raw))
        if not isinstance(marker, dict):
            raise ValueError("physical continuation marker is invalid")
        marker = cast(dict[str, object], marker)
        anchor = marker.get("anchor")
        if marker.get("state") != "published" or not isinstance(anchor, dict):
            continue
        if (
            cast(dict[str, object], anchor).get("repeat_source_id") in source_ids
            and completed_physical_continuation(connection, marker_id) is None
        ):
            found.append(physical_continuation(connection, marker_id))
    if len(found) > 1:
        raise ValueError("ambiguous physical continuation")
    return found[0] if found else None


def completed_physical_originals(connection: sqlite3.Connection) -> frozenset[str]:
    """Return original sources whose independent repeat reached durable READY."""
    rows = cast(
        list[tuple[str]],
        connection.execute("SELECT id FROM state WHERE kind=?", (StateKind.PHYSICAL_CONTINUATION.value,)).fetchall(),
    )
    originals: set[str] = set()
    for (marker_id,) in rows:
        completed_physical_continuation(connection, marker_id)
        marker = _load(connection, StateKind.PHYSICAL_IMPORT, marker_id)
        if marker is None or not isinstance(marker.get("source_id"), str):
            raise ValueError("physical continuation source is invalid")
        originals.add(cast(str, marker["source_id"]))
    return frozenset(originals)
