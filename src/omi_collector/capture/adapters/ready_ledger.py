"""Read and validate one durable READY ledger entry."""

from __future__ import annotations

import json
import sqlite3
from typing import cast

from .collector_state_db import StateKind


def read_ready_ledger(connection: sqlite3.Connection, artifact_id: str) -> dict[str, object] | None:
    """Return a validated ledger entry, or None when no such artifact exists."""
    from . import ready_bundles as ready

    row = cast(
        tuple[str] | None,
        connection.execute(
            "SELECT payload FROM state WHERE kind=? AND id=?", (StateKind.READY_PUBLICATION.value, artifact_id)
        ).fetchone(),
    )
    if row is None:
        return None
    value = cast(object, json.loads(row[0]))
    if not isinstance(value, dict) or not ready._valid_ledger_entry(artifact_id, cast(dict[str, object], value)):
        raise ValueError("ready publication ledger is invalid")
    return cast(dict[str, object], value)
