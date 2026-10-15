"""Narrow admission for a canonical SQLite write transaction."""

from __future__ import annotations

import sqlite3


def admit_exclusive(connection: sqlite3.Connection) -> None:
    """Wait for an ordinary external reader before executing any transaction body.

    SQLite's configured busy handler bounds each individual admission attempt.
    Only the direct, primary BUSY from BEGIN may be retried; a failed COMMIT or
    DML is never replayed here.
    """

    if connection.in_transaction:
        raise RuntimeError("SQLite write admission requires an idle connection")
    while True:
        try:
            connection.execute("BEGIN EXCLUSIVE").close()
            return
        except sqlite3.OperationalError as error:
            if getattr(error, "sqlite_errorcode", None) != sqlite3.SQLITE_BUSY or connection.in_transaction:
                raise
