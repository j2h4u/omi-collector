"""Runtime gate for current Collector SQLite state."""

from __future__ import annotations

import errno
import json
import os
import re
import sqlite3
import stat
from contextlib import closing
from hashlib import sha256
from pathlib import Path
from typing import cast
from urllib.parse import quote

from omi_collector.capture.adapters.collector_state_db import (
    StateDB,
    StateDBError,
    StateKind,
    _validate_audio_schema,
    _validate_schema,
    raise_if_sqlite_storage_fault,
)
from omi_collector.capture.adapters.ready_bundles import (
    ReadyBundleError,
    _is_sha256,
    _parse_publication_contract,
    _v2_config_sha256,
)
from omi_collector.capture.domain.publication_contract import ContractMode
from omi_collector.storage_layout import StorageLayout


class StateMigrationError(RuntimeError):
    """The installed Collector state is unavailable or unsafe to run."""


class InternalAudioCutoverRequiredError(StateMigrationError):
    """A known v1 or unsafe stopped-service state needs operator repair."""


def _validate_canonical(layout: StorageLayout, *, read_only: bool) -> None:
    path = layout.collector.state_db
    try:
        mode = path.lstat().st_mode
        if not stat.S_ISREG(mode):
            raise StateMigrationError("Collector SQLite path is not a regular file")
        _check_canonical_access(path, read_only=read_only)
        # A writable startup must let SQLite roll back an accessible hot
        # journal before the logical v2 checks inspect state rows.
        mode = "ro" if read_only else "rw"
        uri = f"file:{quote(path.absolute().as_posix(), safe='/')}?mode={mode}"
        with closing(sqlite3.connect(uri, uri=True, timeout=30 if read_only else 5)) as connection:
            _validate_current(connection)
            _assert_private_audio_gone(layout, connection)
    except FileNotFoundError as error:
        raise StateMigrationError("Collector SQLite database is missing; run stopped-service init-state") from error
    except PermissionError as error:
        raise InternalAudioCutoverRequiredError(
            "Collector SQLite or journal access needs stopped-service repair"
        ) from error
    except (OSError, sqlite3.DatabaseError, UnicodeError, ValueError, TypeError) as error:
        raise_if_sqlite_storage_fault(error)
        raise StateMigrationError("Collector SQLite state cannot be validated") from error


_TYPED_SCHEMA = 4
_PREVIOUS_TYPED_SCHEMA = 3
_LEGACY_TYPED_SCHEMA = 2


def _validate_current(connection: sqlite3.Connection) -> None:
    connection.row_factory = sqlite3.Row
    version_row = cast(tuple[int] | None, connection.execute("PRAGMA user_version").fetchone())
    if version_row is None:
        raise StateMigrationError("Collector SQLite schema version is unavailable")
    if version_row[0] == 1:
        raise InternalAudioCutoverRequiredError("Collector v1 state is unsupported by this release")
    if version_row[0] in {_LEGACY_TYPED_SCHEMA, _PREVIOUS_TYPED_SCHEMA}:
        raise InternalAudioCutoverRequiredError(
            f"Collector v{version_row[0]} state requires stopped-service init-state migration"
        )
    if version_row[0] != _TYPED_SCHEMA:
        raise StateMigrationError("Collector SQLite schema version is unsupported")
    try:
        _validate_schema(connection)
        _validate_audio_schema(connection)
    except StateDBError as error:
        raise StateMigrationError("Collector SQLite typed schema is invalid") from error
    if connection.execute("SELECT 1 FROM state WHERE kind IN ('attempt','quarantine') LIMIT 1").fetchone():
        raise InternalAudioCutoverRequiredError("legacy capture rows remain in typed state")
    _validate_v2_only_publication(connection)


def _validate_v2_only_publication(connection: sqlite3.Connection) -> None:
    contract_row = cast(
        tuple[str] | None,
        connection.execute(
            "SELECT payload FROM state WHERE kind=? AND id='current'",
            (StateKind.PUBLICATION_CONTRACT.value,),
        ).fetchone(),
    )
    try:
        contract = json.loads(cast(str, contract_row[0])) if contract_row is not None else None
    except (json.JSONDecodeError, TypeError) as error:
        raise StateMigrationError("Collector publication contract is invalid") from error
    try:
        parsed_contract = _parse_publication_contract(contract if isinstance(contract, dict) else None)
    except ReadyBundleError as error:
        raise InternalAudioCutoverRequiredError(
            "Collector publication contract must be v2 before this release"
        ) from error
    if parsed_contract.mode is not ContractMode.V2_OPEN:
        raise InternalAudioCutoverRequiredError("Collector publication contract must be v2 before this release")
    legacy_custody = cast(
        tuple[int, int, int, int, int, int] | None,
        connection.execute(
            "SELECT (SELECT count(*) FROM ready_output_stage), (SELECT count(*) FROM ready_stage_range), "
            "(SELECT count(*) FROM ready_replay_proof WHERE format!='omi-audio-v2'), "
            "(SELECT count(*) FROM ready_replay_proof_page AS page LEFT JOIN ready_replay_proof AS proof "
            "ON proof.bundle_id=page.bundle_id AND proof.format='omi-audio-v2' WHERE proof.bundle_id IS NULL), "
            "(SELECT count(*) FROM publication_cohort_member AS member LEFT JOIN publication_cohort AS cohort "
            "ON cohort.cohort_id=member.cohort_id LEFT JOIN capture_source AS member_source "
            "ON member_source.source_id=member.source_id "
            "WHERE cohort.cohort_id IS NULL OR member_source.source_id IS NULL), "
            "(SELECT count(*) FROM ready_v2_stage_range AS stage_range LEFT JOIN ready_v2_output_stage AS stage "
            "ON stage.stage_id=stage_range.stage_id LEFT JOIN capture_source AS range_source "
            "ON range_source.source_id=stage_range.source_id "
            "WHERE stage.stage_id IS NULL OR range_source.source_id IS NULL)"
        ).fetchone(),
    )
    if legacy_custody is None or tuple(legacy_custody) != (0, 0, 0, 0, 0, 0):
        raise InternalAudioCutoverRequiredError("unresolved publication custody must be repaired before this release")
    _validate_v2_cohorts(connection)
    rows = cast(
        list[tuple[str]],
        connection.execute(
            "SELECT payload FROM state WHERE kind=? AND id!='frontier'",
            (StateKind.READY_PUBLICATION.value,),
        ).fetchall(),
    )
    try:
        entries = tuple(cast(object, json.loads(row[0])) for row in rows)
    except (json.JSONDecodeError, TypeError) as error:
        raise StateMigrationError("Collector publication ledger is invalid") from error
    if any(
        not isinstance(entry, dict) or (entry.get("format") != "omi-audio-v2" and entry.get("state") != "retired")
        for entry in entries
    ):
        raise InternalAudioCutoverRequiredError("active legacy publication must retire before this release")


def _validate_v2_cohorts(connection: sqlite3.Connection) -> None:
    rows = cast(
        list[tuple[str, str, str | None]],
        connection.execute(
            "SELECT cohort_id,lifecycle,config_digest FROM publication_cohort ORDER BY cohort_id"
        ).fetchall(),
    )
    active = _require_v2_cohort_inventory(rows)
    if len(active) > 1:
        raise InternalAudioCutoverRequiredError("multiple active publication cohorts require stopped-service repair")
    for cohort_id, _, _ in active:
        _validate_active_v2_cohort(connection, cohort_id)


def _require_v2_cohort_inventory(
    rows: list[tuple[str, str, str | None]],
) -> tuple[tuple[str, str, str | None], ...]:
    active = tuple(row for row in rows if row[1] in {"accepted", "staging"})
    if any(row[1] not in {"accepted", "staging"} for row in rows):
        raise InternalAudioCutoverRequiredError(
            "unsupported publication cohort lifecycle requires stopped-service repair"
        )
    if any(row[2] != _v2_config_sha256() for row in rows):
        raise InternalAudioCutoverRequiredError(
            "legacy or unqualified publication cohort must retire before this release"
        )
    return active


def _validate_active_v2_cohort(connection: sqlite3.Connection, cohort_id: str) -> None:
    cohort = cast(
        tuple[str | None, int, int] | None,
        connection.execute(
            "SELECT accepted_permit,drained_threshold,cursor FROM publication_cohort WHERE cohort_id=?",
            (cohort_id,),
        ).fetchone(),
    )
    if cohort is None or cohort[0] != cohort_id or type(cohort[1]) is not int or type(cohort[2]) is not int:
        raise InternalAudioCutoverRequiredError("active publication cohort permit is invalid")
    members = cast(
        list[tuple[int, str, int, int, str, int, str, int]],
        connection.execute(
            "SELECT member.member_ordinal,member.source_id,member.slice_start,member.slice_count,"
            "member.admitted_lifecycle,member.admitted_count,member.admitted_sha256,member.cursor "
            "FROM publication_cohort_member AS member LEFT JOIN capture_source AS source "
            "ON source.source_id=member.source_id WHERE member.cohort_id=? ORDER BY member.member_ordinal",
            (cohort_id,),
        ).fetchall(),
    )
    if not members:
        raise InternalAudioCutoverRequiredError("active publication cohort members are invalid")
    for expected, member in enumerate(members):
        _validate_v2_cohort_member(connection, expected, member)


def _validate_v2_cohort_member(
    connection: sqlite3.Connection,
    expected_ordinal: int,
    member: tuple[int, str, int, int, str, int, str, int],
) -> None:
    ordinal, source_id, start, count, admitted_lifecycle, admitted_count, digest, cursor = member
    if (
        ordinal != expected_ordinal
        or start != 0
        or count != admitted_count
        or admitted_lifecycle not in {"sealed_waiting", "blocked"}
        or not _is_sha256(digest)
    ):
        raise InternalAudioCutoverRequiredError("active publication cohort member is invalid")
    if not 0 <= cursor <= count:
        raise InternalAudioCutoverRequiredError("active publication cohort member cursor is invalid")
    source = cast(
        tuple[object, ...] | None,
        connection.execute(
            "SELECT source_type,lifecycle,committed_count,committed_sha256,certified_prefix_count,coverage_digest "
            "FROM capture_source WHERE source_id=?",
            (source_id,),
        ).fetchone(),
    )
    if source is None or source[1] not in {admitted_lifecycle, "covered", "retiring"}:
        raise InternalAudioCutoverRequiredError("active publication cohort source snapshot is invalid")
    if admitted_lifecycle == "sealed_waiting" and tuple(source[2:4]) != (count, digest):
        raise InternalAudioCutoverRequiredError("active publication cohort source snapshot is invalid")
    if admitted_lifecycle == "blocked" and (source[0] != "quarantine" or tuple(source[4:6]) != (count, digest)):
        raise InternalAudioCutoverRequiredError("active publication cohort source snapshot is invalid")


def _validate_v2(connection: sqlite3.Connection) -> None:
    """Validate the sole stopped-service source schema for the v3 migration."""

    connection.row_factory = sqlite3.Row
    version_row = cast(tuple[int] | None, connection.execute("PRAGMA user_version").fetchone())
    if version_row is None or version_row[0] != _LEGACY_TYPED_SCHEMA:
        raise StateMigrationError("Collector SQLite schema is not v2")
    try:
        _validate_schema(connection)
        _validate_audio_schema(connection, version=_LEGACY_TYPED_SCHEMA)
    except StateDBError as error:
        raise StateMigrationError("Collector SQLite v2 typed schema is invalid") from error
    if connection.execute("SELECT 1 FROM state WHERE kind IN ('attempt','quarantine') LIMIT 1").fetchone():
        raise InternalAudioCutoverRequiredError("legacy capture rows remain in v2 state")


def _validate_v3(connection: sqlite3.Connection) -> None:
    """Validate the stopped-service source schema for the v4 migration."""

    connection.row_factory = sqlite3.Row
    version_row = cast(tuple[int] | None, connection.execute("PRAGMA user_version").fetchone())
    if version_row is None or version_row[0] != _PREVIOUS_TYPED_SCHEMA:
        raise StateMigrationError("Collector SQLite schema is not v3")
    try:
        _validate_schema(connection)
        _validate_audio_schema(connection, version=_PREVIOUS_TYPED_SCHEMA)
    except StateDBError as error:
        raise StateMigrationError("Collector SQLite v3 typed schema is invalid") from error
    if connection.execute("SELECT 1 FROM state WHERE kind IN ('attempt','quarantine') LIMIT 1").fetchone():
        raise InternalAudioCutoverRequiredError("legacy capture rows remain in v3 state")


def open_runtime_state(layout: StorageLayout, *, read_only: bool = False) -> StateDB:
    """Open only a complete current database; runtime never creates or upgrades it."""

    _validate_canonical(layout, read_only=read_only)
    try:
        return StateDB(layout.collector.state_db, read_only=read_only)
    except StateDBError as error:
        try:
            _check_canonical_access(layout.collector.state_db, read_only=read_only)
        except PermissionError as denied:
            raise InternalAudioCutoverRequiredError(
                "Collector SQLite or journal access needs stopped-service repair"
            ) from denied
        raise StateMigrationError("Collector SQLite state cannot be opened") from error


def _check_canonical_access(path: Path, *, read_only: bool) -> None:
    flags = (os.O_RDONLY if read_only else os.O_RDWR) | os.O_NOFOLLOW | os.O_CLOEXEC
    descriptor = os.open(path, flags)
    os.close(descriptor)
    journal = path.parent / f"{path.name}-journal"
    try:
        descriptor = os.open(journal, flags | os.O_NONBLOCK)
    except FileNotFoundError:
        pass
    except OSError as error:
        if error.errno in {errno.ELOOP, errno.EISDIR}:
            raise InternalAudioCutoverRequiredError(
                "Collector SQLite journal is unsafe; stopped-service repair required"
            ) from error
        raise
    else:
        try:
            if not stat.S_ISREG(os.fstat(descriptor).st_mode):
                raise InternalAudioCutoverRequiredError(
                    "Collector SQLite journal is unsafe; stopped-service repair required"
                )
        finally:
            os.close(descriptor)
    if not read_only and not os.access(path.parent, os.W_OK | os.X_OK, effective_ids=True):
        raise PermissionError("Collector SQLite journal directory is not writable")


def _assert_private_audio_gone(layout: StorageLayout, connection: sqlite3.Connection) -> None:
    for root in (layout.collector.attempts, layout.collector.quarantine, layout.draft):
        try:
            mode = root.lstat().st_mode
        except FileNotFoundError:
            continue
        if not stat.S_ISDIR(mode):
            raise StateMigrationError(f"private audio root is unsafe: {root}")
        if any(root.iterdir()):
            raise InternalAudioCutoverRequiredError(f"private audio remains outside typed SQLite state: {root}")
    ready = layout.publication.root
    if not ready.exists() and not ready.is_symlink():
        return
    if ready.is_symlink() or not ready.is_dir():
        raise StateMigrationError("ready publication root is unsafe")
    for entry in ready.iterdir():
        if entry.name.startswith(".") and not _owned_typed_ready_stage(entry, connection):
            raise InternalAudioCutoverRequiredError("unowned ready scratch remains after cutover")


def _owned_typed_ready_stage(entry: Path, connection: sqlite3.Connection) -> bool:
    match = re.fullmatch(r"\.([0-9a-f]{64})\.([0-9a-f]{16})\.stage", entry.name)
    if match is None or not stat.S_ISDIR(entry.lstat().st_mode):
        return False
    return _owned_v2_ready_stage(entry, match, connection)


def _owned_v2_ready_stage(entry: Path, match: re.Match[str], connection: sqlite3.Connection) -> bool:
    rows = cast(
        list[tuple[str, str, str, str]],
        connection.execute(
            "SELECT stage_id,group_identity,artifact_id,owner_token FROM ready_v2_output_stage WHERE owned_path=?",
            (str(entry),),
        ).fetchall(),
    )
    if len(rows) != 1:
        return False
    stage_id, identity, artifact_id, owner_token = rows[0]
    parts = re.fullmatch(r"([0-9a-f]{64}):([0-9]{1,19}):([0-9a-f]{64})", identity)
    if (
        parts is None
        or stage_id != sha256(identity.encode()).hexdigest()
        or owner_token != stage_id
        or artifact_id != parts[3]
        or match.groups() != (artifact_id, stage_id[:16])
    ):
        return False
    if not _owned_stage_cohort(connection, parts):
        return False
    return all(
        child.name in {"audio.ogg", "manifest.json"} and stat.S_ISREG(child.lstat().st_mode)
        for child in entry.iterdir()
    )


def _owned_stage_cohort(connection: sqlite3.Connection, parts: re.Match[str]) -> bool:
    cohort = cast(
        tuple[str, int] | None,
        connection.execute("SELECT lifecycle,cursor FROM publication_cohort WHERE cohort_id=?", (parts[1],)).fetchone(),
    )
    return cohort is not None and (cohort[0], cohort[1]) == ("staging", int(parts[2])) and str(cohort[1]) == parts[2]
