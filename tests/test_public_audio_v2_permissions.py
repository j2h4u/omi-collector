from __future__ import annotations

import os
import sqlite3
from collections.abc import Iterator
from contextlib import closing
from pathlib import Path
from struct import pack
from typing import cast

import pytest

from omi_collector.capture.adapters import ready_bundles
from omi_collector.capture.adapters.collector_state_db import StateDB, StateKind
from omi_collector.capture.adapters.internal_audio_store import AUDIO_RECORD_SIZE, DrainCheckpoint, InternalAudioStore
from omi_collector.capture.adapters.ready_bundles import (
    PublicAudioV2Options,
    ReadyBundleError,
    SqlitePublicationSource,
    finalize_public_audio_v2,
)
from omi_collector.capture.domain.clock_state import ClockState
from omi_collector.config import ReadyConfig


@pytest.fixture
def audio_store(tmp_path: Path) -> Iterator[tuple[StateDB, InternalAudioStore]]:
    database = StateDB(tmp_path / "collector-state.sqlite3")
    store = InternalAudioStore(database)
    try:
        yield database, store
    finally:
        store.close()


def _ready_root(root: Path) -> Path:
    ready = root / "ready"
    ready.mkdir(mode=0o2750)
    ready.chmod(0o2750)
    return ready


def _source(store: InternalAudioStore) -> SqlitePublicationSource:
    source_id = "a" * 32
    record = pack(">I", 100) + bytes((2, 8, 1)) + bytes(AUDIO_RECORD_SIZE - 7)
    store.create_source(source_id, "attempt", 10, 11)
    source = store.prepare_leg(source_id, 10, 1)
    source = store.record_read_begin(source_id, 10, 1)
    store.commit_drain(source_id, 0, (record,), DrainCheckpoint(source.current_leg_generation))
    source = store.get_source(source_id)
    assert source is not None
    store.seal_source(
        source_id,
        leg_generation=source.current_leg_generation,
        expected_count=source.committed_count,
        expected_sha256=source.committed_sha256,
    )
    return SqlitePublicationSource(store, source_id)


def _options() -> PublicAudioV2Options:
    return PublicAudioV2Options(ReadyConfig(target_audio_seconds=0.02), ClockState(), 11, True, 128)


def _durable_state(database: StateDB) -> tuple[object, ...]:
    with closing(database.open_connection(read_only=True)) as connection:
        private_tables = (
            "capture_source",
            "capture_record",
            "publication_cohort",
            "publication_cohort_member",
            "ready_v2_output_stage",
        )
        private = tuple(_query_rows(connection, f"SELECT * FROM {table}") for table in private_tables)
        proofs = _query_rows(connection, "SELECT * FROM ready_replay_proof")
        proof_pages = _query_rows(connection, "SELECT * FROM ready_replay_proof_page")
    return database.list(StateKind.READY_PUBLICATION), private, proofs, proof_pages


def _query_rows(connection: sqlite3.Connection, query: str) -> tuple[tuple[object, ...], ...]:
    cursor = cast(sqlite3.Cursor, connection.execute(query))
    rows = cast(list[sqlite3.Row], cursor.fetchall())
    return tuple(sorted((tuple(row) for row in rows), key=repr))


def _artifact_bytes(path: Path) -> tuple[tuple[str, ...], tuple[bytes, ...]]:
    names = tuple(sorted(item.name for item in path.iterdir()))
    contents = tuple((path / name).read_bytes() for name in names)
    return names, contents


@pytest.mark.parametrize("name", ("audio.ogg", "manifest.json"))
@pytest.mark.parametrize("damage", ("mode", "gid"), ids=("wrong-mode", "wrong-gid"))
def test_existing_v2_artifact_permission_damage_fails_closed_before_hashing(
    tmp_path: Path,
    audio_store: tuple[StateDB, InternalAudioStore],
    monkeypatch: pytest.MonkeyPatch,
    name: str,
    damage: str,
) -> None:
    database, store = audio_store
    ready = _ready_root(tmp_path)
    artifact = finalize_public_audio_v2(store, (_source(store),), ready, _options()).published[0]
    target = artifact.path / name
    if damage == "mode":
        target.chmod(0o600)
    else:
        original_lstat = Path.lstat
        wrong_gid = 0 if target.lstat().st_gid != 0 else 1

        def lstat_with_wrong_gid(path: Path) -> os.stat_result:
            result = original_lstat(path)
            if path != target:
                return result
            values = (
                result.st_mode,
                result.st_ino,
                result.st_dev,
                result.st_nlink,
                result.st_uid,
                wrong_gid,
                result.st_size,
                result.st_atime,
                result.st_mtime,
                result.st_ctime,
            )
            return os.stat_result(values)

        monkeypatch.setattr(Path, "lstat", lstat_with_wrong_gid)

    expected_bytes = _artifact_bytes(artifact.path)
    expected_state = _durable_state(database)
    digest_calls: list[Path] = []
    original_digest = ready_bundles._digest

    def observed_digest(path: Path) -> str:
        digest_calls.append(path)
        return original_digest(path)

    monkeypatch.setattr(ready_bundles, "_digest", observed_digest)
    with pytest.raises(ReadyBundleError, match="operator access"):
        finalize_public_audio_v2(store, (), ready, _options())

    assert digest_calls == []
    assert artifact.path.is_dir()
    assert _artifact_bytes(artifact.path) == expected_bytes
    assert _durable_state(database) == expected_state


@pytest.mark.parametrize(("damage", "name"), (("mode", "audio.ogg"), ("gid", "manifest.json")))
def test_interrupted_destination_permission_damage_fails_closed_on_restart(
    tmp_path: Path,
    audio_store: tuple[StateDB, InternalAudioStore],
    monkeypatch: pytest.MonkeyPatch,
    damage: str,
    name: str,
) -> None:
    database, store = audio_store
    ready = _ready_root(tmp_path)
    source = _source(store)
    original_rename = Path.rename
    fired = False

    def rename_then_interrupt(path: Path, target: str | os.PathLike[str]) -> Path:
        nonlocal fired
        result = original_rename(path, target)
        if path.parent == ready and path.name.endswith(".stage") and not fired:
            fired = True
            raise RuntimeError("simulated interruption after public rename")
        return result

    monkeypatch.setattr(Path, "rename", rename_then_interrupt)
    with pytest.raises(RuntimeError, match="after public rename"):
        finalize_public_audio_v2(store, (source,), ready, _options())
    assert fired
    monkeypatch.setattr(Path, "rename", original_rename)

    with closing(database.open_connection(read_only=True)) as connection:
        row = cast(
            tuple[object, ...] | None,
            connection.execute("SELECT artifact_id FROM ready_v2_output_stage").fetchone(),
        )
    assert row is not None
    destination = ready / cast(str, row[0])
    target = destination / name
    if damage == "mode":
        target.chmod(0o600)
    else:
        original_lstat = Path.lstat
        wrong_gid = 0 if target.lstat().st_gid != 0 else 1

        def lstat_with_wrong_gid(path: Path) -> os.stat_result:
            result = original_lstat(path)
            if path != target:
                return result
            return os.stat_result(
                (
                    result.st_mode,
                    result.st_ino,
                    result.st_dev,
                    result.st_nlink,
                    result.st_uid,
                    wrong_gid,
                    result.st_size,
                    result.st_atime,
                    result.st_mtime,
                    result.st_ctime,
                )
            )

        monkeypatch.setattr(Path, "lstat", lstat_with_wrong_gid)

    expected_bytes = _artifact_bytes(destination)
    expected_state = _durable_state(database)
    digest_calls: list[Path] = []
    original_digest = ready_bundles._digest

    def observed_digest(path: Path) -> str:
        digest_calls.append(path)
        return original_digest(path)

    monkeypatch.setattr(ready_bundles, "_digest", observed_digest)
    with (
        InternalAudioStore(StateDB(database.path)) as restarted,
        pytest.raises(ReadyBundleError, match="operator access"),
    ):
        finalize_public_audio_v2(
            restarted,
            (SqlitePublicationSource(restarted, source.source_id),),
            ready,
            _options(),
        )

    assert digest_calls == []
    assert destination.is_dir()
    assert _artifact_bytes(destination) == expected_bytes
    assert _durable_state(database) == expected_state
