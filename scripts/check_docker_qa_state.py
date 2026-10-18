"""Validate the state left by the Docker QA initialization bootstrap."""

from __future__ import annotations

import json
import os
import re
import sqlite3
import stat
from pathlib import Path
from typing import cast

_DATABASE = Path("/data/omi/collector/collector-state.sqlite3")
_COLLECTOR_LOCK = Path("/data/omi/collector/collector.lock")
_READY = Path("/data/omi/ready")
_RELEASE_METADATA = Path("/opt/venv/share/omi-collector/release.json")
_REVISION = re.compile(r"[0-9a-f]{40,64}\Z")
_APP_UID = 1000
_OPERATOR_GID = 1001
_DATABASE_MODE = 0o640
_SCHEMA_VERSION = 3
_LOCK_MODE = 0o600
_READY_MODE = 0o2750


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise RuntimeError(message)


def _check_pid_one() -> None:
    fields: dict[str, list[str]] = {}
    for line in Path("/proc/1/status").read_text(encoding="utf-8").splitlines():
        name, separator, values = line.partition(":")
        if separator and name in {"Uid", "Gid"}:
            fields[name] = values.split()
    for name in ("Uid", "Gid"):
        _require(fields.get(name) == ["1000"] * 4, f"/proc/1/status {name} is not all 1000")


def _check_release_metadata() -> None:
    revision = os.environ.get("OMI_COLLECTOR_SOURCE_REVISION")
    _require(revision is not None and _REVISION.fullmatch(revision) is not None, "source revision env is invalid")
    metadata = cast(dict[str, object], json.loads(_RELEASE_METADATA.read_text(encoding="utf-8")))
    _require(metadata.get("source_revision") == revision, "release metadata source_revision does not match env")


def _check_database() -> None:
    ready_stat = _READY.stat()
    _require(ready_stat.st_uid == _APP_UID, f"ready uid is {ready_stat.st_uid}, expected {_APP_UID}")
    _require(ready_stat.st_gid == _OPERATOR_GID, f"ready gid is {ready_stat.st_gid}, expected {_OPERATOR_GID}")
    _require(stat.S_IMODE(ready_stat.st_mode) == _READY_MODE, "ready mode is not 2750")
    _require(os.access(_READY, os.R_OK | os.X_OK), "ready directory is not readable/traversable by app")
    lock_stat = _COLLECTOR_LOCK.stat()
    _require(lock_stat.st_uid == _APP_UID, f"collector lock uid is {lock_stat.st_uid}, expected {_APP_UID}")
    _require(lock_stat.st_gid == _APP_UID, f"collector lock gid is {lock_stat.st_gid}, expected {_APP_UID}")
    _require(stat.S_IMODE(lock_stat.st_mode) == _LOCK_MODE, "collector lock mode is not 0600")
    database_stat = _DATABASE.stat()
    _require(database_stat.st_uid == _APP_UID, f"database uid is {database_stat.st_uid}, expected {_APP_UID}")
    _require(
        database_stat.st_gid == _OPERATOR_GID,
        f"database gid is {database_stat.st_gid}, expected {_OPERATOR_GID}",
    )
    _require(stat.S_IMODE(database_stat.st_mode) == _DATABASE_MODE, "database mode is not 0640")
    with sqlite3.connect(f"file:{_DATABASE}?mode=ro", uri=True) as connection:
        version = cast(tuple[int] | None, connection.execute("PRAGMA user_version").fetchone())
        _require(
            version is not None and version[0] == _SCHEMA_VERSION,
            f"SQLite user_version is {version}, expected {_SCHEMA_VERSION}",
        )
        _require(
            connection.execute("SELECT 1 FROM state WHERE kind IN ('attempt','quarantine') LIMIT 1").fetchone() is None,
            "legacy capture rows remain",
        )


def main() -> int:
    try:
        _check_pid_one()
        _check_release_metadata()
        _check_database()
    except (OSError, RuntimeError, sqlite3.Error, json.JSONDecodeError) as error:
        print(f"Docker QA state check failed: {error}")
        return 1
    print("Docker QA state is healthy")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
