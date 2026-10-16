"""Narrow admission for a canonical SQLite write transaction."""

from __future__ import annotations

import sqlite3
from collections.abc import Callable
from typing import Literal


def _admit(connection: sqlite3.Connection, statement: Literal["BEGIN EXCLUSIVE", "BEGIN IMMEDIATE"]) -> None:
    """Retry direct busy admission before executing any transaction body."""

    if connection.in_transaction:
        raise RuntimeError("SQLite write admission requires an idle connection")
    while True:
        try:
            connection.execute(statement).close()
            return
        except sqlite3.OperationalError as error:
            if getattr(error, "sqlite_errorcode", None) != sqlite3.SQLITE_BUSY or connection.in_transaction:
                raise


def admit_exclusive(connection: sqlite3.Connection) -> None:
    """Wait for ordinary readers during schema initialization and recovery."""

    _admit(connection, "BEGIN EXCLUSIVE")


def admit_immediate(connection: sqlite3.Connection) -> None:
    """Reserve the writer slot while allowing readers during runtime writes."""

    _admit(connection, "BEGIN IMMEDIATE")


def commit_with_busy_retry(connection: sqlite3.Connection, commit: Callable[[], None]) -> None:
    """Retry COMMIT only while a direct primary BUSY leaves it active."""

    while True:
        try:
            commit()
            return
        except sqlite3.OperationalError as error:
            if getattr(error, "sqlite_errorcode", None) != sqlite3.SQLITE_BUSY or not connection.in_transaction:
                raise
