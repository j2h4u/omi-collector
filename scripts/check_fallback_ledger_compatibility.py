#!/usr/bin/env python3
"""Check whether this release can read the canonical ready publication ledger."""

from __future__ import annotations

import sqlite3
import sys
from contextlib import closing
from pathlib import Path
from typing import cast
from urllib.parse import quote

from omi_collector.capture.adapters.collector_state_db import StateDBError, _decode
from omi_collector.capture.adapters.ready_bundles import _valid_ledger_entry

_ARGUMENT_COUNT = 2
_LEDGER_ROW_COLUMN_COUNT = 2


def _valid_ledger_rows(cursor: sqlite3.Cursor) -> bool:
    while True:
        row = cast(object, cursor.fetchone())
        if row is None:
            return True
        if not isinstance(row, tuple) or len(row) != _LEDGER_ROW_COLUMN_COUNT:
            return False
        bundle_id, raw_payload = row
        if not isinstance(bundle_id, str) or not isinstance(raw_payload, str):
            return False
        entry = _decode(raw_payload)
        if bundle_id == "frontier":
            value = entry.get("value")
            if set(entry) != {"value"} or type(value) is not int or value < 0:
                return False
        elif not _valid_ledger_entry(bundle_id, entry):
            return False


def compatible(database_path: Path) -> bool:
    if database_path.is_symlink() or not database_path.is_file():
        return False
    uri = f"file:{quote(database_path.absolute().as_posix(), safe='/')}?mode=ro"
    try:
        with closing(sqlite3.connect(uri, uri=True, timeout=60)) as connection:
            connection.execute("PRAGMA query_only = ON")
            return _valid_ledger_rows(
                connection.execute("SELECT id, payload FROM state WHERE kind = ? ORDER BY id", ("ready_publication",))
            )
    except OSError, sqlite3.Error, StateDBError, TypeError, ValueError:
        return False
    return True


def main() -> int:
    if len(sys.argv) != _ARGUMENT_COUNT or not compatible(Path(sys.argv[1])):
        print(
            "fallback cannot read the ready publication ledger; keep the service stopped and deploy a forward-compatible release",
            file=sys.stderr,
        )
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
