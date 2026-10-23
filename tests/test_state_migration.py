"""Runtime accepts only a safe typed Collector database."""

from __future__ import annotations

import errno
import os
import sqlite3
from contextlib import closing
from hashlib import sha256
from pathlib import Path
from typing import cast

import pytest

from omi_collector import state_migration
from omi_collector.capture.adapters import ready_bundles
from omi_collector.capture.adapters.collector_state_db import StateDB, StateKind
from omi_collector.capture.adapters.internal_audio_store import AUDIO_RECORD_SIZE, DrainCheckpoint, InternalAudioStore
from omi_collector.capture.adapters.ready_bundles import (
    PublicAudioV2Options,
    ReadyBundleError,
    finalize_public_audio_v2,
)
from omi_collector.capture.domain.clock_state import ClockState
from omi_collector.capture.domain.publication_contract import ContractMode
from omi_collector.config import ReadyConfig
from omi_collector.state_migration import (
    InternalAudioCutoverRequiredError,
    StateMigrationError,
    _validate_current,
    open_runtime_state,
)
from omi_collector.storage_layout import StorageLayout, load_operator_config


def _layout(tmp_path: Path) -> StorageLayout:
    config = tmp_path / "config.toml"
    config.write_text('pendant_address = "AA:BB:CC:DD:EE:FF"\nbundle_min_audio_seconds = 120\n', encoding="utf-8")
    return load_operator_config(config).storage


def test_runtime_never_creates_missing_database(tmp_path: Path) -> None:
    layout = _layout(tmp_path)
    with pytest.raises(StateMigrationError, match="missing"):
        open_runtime_state(layout)
    assert not layout.collector.state_db.exists()


def test_known_v1_requires_stopped_cutover(tmp_path: Path) -> None:
    layout = _layout(tmp_path)
    layout.collector.root.mkdir()
    with closing(sqlite3.connect(layout.collector.state_db)) as connection:
        connection.execute("CREATE TABLE state(kind TEXT, id TEXT, payload TEXT)")
        connection.execute("PRAGMA user_version=1")
        connection.commit()
    with pytest.raises(InternalAudioCutoverRequiredError, match="v1"):
        open_runtime_state(layout)


def test_current_receipt_is_inert_and_private_audio_must_be_gone(tmp_path: Path) -> None:
    layout = _layout(tmp_path)
    db = StateDB(layout.collector.state_db)
    db.put(StateKind.MIGRATION, "internal_audio_cutover", {"format": 1, "phase": "CLEANUP_PENDING"})
    assert open_runtime_state(layout, read_only=True).path == db.path
    db.put(StateKind.MIGRATION, "internal_audio_cutover", {"format": 1, "phase": "COMPLETE", "fresh": True})
    assert open_runtime_state(layout, read_only=True).get(StateKind.MIGRATION, "internal_audio_cutover") is not None
    layout.draft.mkdir()
    (layout.draft / "leftover").write_bytes(b"raw")
    with pytest.raises(InternalAudioCutoverRequiredError, match="private audio"):
        open_runtime_state(layout)


def test_current_rejects_legacy_mutable_rows(tmp_path: Path) -> None:
    layout = _layout(tmp_path)
    db = StateDB(layout.collector.state_db)
    db.put(StateKind.MIGRATION, "internal_audio_cutover", {"format": 1, "phase": "COMPLETE", "fresh": True})
    db.put(StateKind.ATTEMPT, "a" * 32, {"descriptor": {"attempt_id": "a" * 32}})
    with pytest.raises(InternalAudioCutoverRequiredError, match="legacy capture rows"):
        open_runtime_state(layout)


@pytest.mark.parametrize("mode", ["legacy_open", "quiescing"])
def test_current_rejects_legacy_publication_contract(tmp_path: Path, mode: str) -> None:
    layout = _layout(tmp_path)
    db = StateDB(layout.collector.state_db)
    db.put(StateKind.PUBLICATION_CONTRACT, "current", {"version": 1, "mode": mode, "next_public_order": 0})

    with pytest.raises(InternalAudioCutoverRequiredError, match="publication contract must be v2"):
        open_runtime_state(layout)


def test_current_rejects_active_legacy_publication_and_accepts_retired_lineage(tmp_path: Path) -> None:
    layout = _layout(tmp_path)
    db = StateDB(layout.collector.state_db)
    digest = "a" * 64
    bundle_id = sha256(f"0:1:{digest}".encode()).hexdigest()
    entry = {
        "records_sha256": "b" * 64,
        "state": "ready",
        "start_sequence": 0,
        "next_sequence": 1,
        "formed_on_utc": "2024-01-02",
        "recorded_on_utc_min": "2023-12-31",
        "recorded_on_utc_max": "2024-01-02",
        "draft_raw_sha256": digest,
        "sources": [{"start_sequence": 0, "next_sequence": 1, "raw_sha256": digest}],
    }
    db.put(StateKind.READY_PUBLICATION, bundle_id, entry)

    with pytest.raises(InternalAudioCutoverRequiredError, match="active legacy publication"):
        open_runtime_state(layout)

    db.put(StateKind.READY_PUBLICATION, bundle_id, {**entry, "state": "retired"})
    assert open_runtime_state(layout, read_only=True).path == db.path
    assert db.get(StateKind.READY_PUBLICATION, bundle_id) == {**entry, "state": "retired"}


@pytest.mark.parametrize("with_member", [False, True], ids=["no-members", "with-member"])
@pytest.mark.parametrize("config_digest", [None, "a" * 64], ids=["null-digest", "legacy-digest"])
def test_runtime_rejects_unqualified_cohort_without_stage(
    tmp_path: Path, with_member: bool, config_digest: str | None
) -> None:
    layout = _layout(tmp_path)
    db = StateDB(layout.collector.state_db)
    db.put(
        StateKind.PUBLICATION_CONTRACT,
        "current",
        {"version": 1, "mode": ContractMode.V2_OPEN.value, "next_public_order": 0},
    )
    cohort_id = "c" * 64
    source_id = "b" * 32
    source: tuple[int, str] | None = None
    if with_member:
        store = InternalAudioStore(db)
        record = bytes(AUDIO_RECORD_SIZE)
        store.create_source(source_id, "attempt", 10, 11)
        current = store.prepare_leg(source_id, 10, 1)
        current = store.record_read_begin(source_id, 10, 1)
        store.commit_drain(source_id, 0, (record,), DrainCheckpoint(current.current_leg_generation))
        current = store.get_source(source_id)
        assert current is not None
        store.seal_source(
            source_id,
            leg_generation=current.current_leg_generation,
            expected_count=current.committed_count,
            expected_sha256=current.committed_sha256,
        )
        source = (current.committed_count, current.committed_sha256)
        store.close()

    with closing(db.open_connection()) as connection:
        connection.execute(
            "INSERT INTO publication_cohort(cohort_id,lifecycle,config_digest) VALUES (?,'unqualified',?)",
            (cohort_id, config_digest),
        )
        if source is not None:
            connection.execute(
                "INSERT INTO publication_cohort_member(cohort_id,member_ordinal,source_id,slice_start,slice_count,"
                "admitted_lifecycle,admitted_count,admitted_sha256) VALUES (?,0,?,0,1,'sealed_waiting',?,?)",
                (cohort_id, source_id, source[0], source[1]),
            )
        connection.commit()

    with pytest.raises(InternalAudioCutoverRequiredError, match="legacy or unqualified publication cohort"):
        open_runtime_state(layout)
    with closing(db.open_connection(read_only=True)) as connection:
        assert connection.execute("SELECT count(*) FROM publication_cohort").fetchone()[0] == 1
        assert connection.execute("SELECT count(*) FROM ready_v2_output_stage").fetchone()[0] == 0


def test_runtime_accepts_completed_v2_cohort_after_member_cleanup(tmp_path: Path) -> None:
    layout = _layout(tmp_path)
    db = StateDB(layout.collector.state_db)
    db.put(
        StateKind.PUBLICATION_CONTRACT,
        "current",
        {"version": 1, "mode": ContractMode.V2_OPEN.value, "next_public_order": 0},
    )
    cohort_id = "d" * 64
    with closing(db.open_connection()) as connection:
        connection.execute(
            "INSERT INTO publication_cohort(cohort_id,lifecycle,accepted_permit,drained_threshold,cursor,config_digest) "
            "VALUES (?,'complete',?,1,1,?)",
            (cohort_id, cohort_id, ready_bundles._v2_config_sha256()),
        )
        connection.commit()

    assert open_runtime_state(layout, read_only=True).path == db.path


def test_current_runtime_rejects_legacy_replay_proof_custody(tmp_path: Path) -> None:
    layout = _layout(tmp_path)
    db = StateDB(layout.collector.state_db)
    with closing(db.open_connection()) as connection:
        connection.execute(
            "INSERT INTO ready_replay_proof VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                "a" * 64,
                "active",
                "legacy-stage",
                "legacy-owner",
                1,
                "b" * 64,
                "c" * 64,
                b"index",
                b"runs",
                b"metadata",
                "d" * 64,
                "legacy-records-v1",
                None,
                None,
                None,
            ),
        )
        connection.commit()

    with pytest.raises(InternalAudioCutoverRequiredError, match="publication custody"):
        open_runtime_state(layout)


@pytest.mark.parametrize(
    "custody",
    (
        "ready_replay_proof_page",
        "publication_cohort_member",
        "ready_stage_range",
        "ready_v2_stage_range",
        "cohort_member_missing_source",
        "v2_stage_range_missing_source",
    ),
)
def test_current_runtime_rejects_orphan_publication_custody(tmp_path: Path, custody: str) -> None:
    layout = _layout(tmp_path)
    db = StateDB(layout.collector.state_db)
    statements: dict[str, tuple[str, tuple[object, ...]]] = {
        "ready_replay_proof_page": (
            "INSERT INTO ready_replay_proof_page VALUES (?,?,?,?,?,?)",
            ("a" * 64, 0, 0, 1, b"b" * 32, b"c" * 32),
        ),
        "publication_cohort_member": (
            "INSERT INTO publication_cohort_member VALUES (?,?,?,?,?,?,?,?,?)",
            ("orphan", 0, "a" * 32, 0, 0, "sealed_waiting", 0, "b" * 64, 0),
        ),
        "ready_stage_range": (
            "INSERT INTO ready_stage_range VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            ("orphan", 0, 0, "a" * 32, 0, 1, bytes(8), (1).to_bytes(8), 0, 0, None, None, None, None, None, None),
        ),
        "ready_v2_stage_range": (
            "INSERT INTO ready_v2_stage_range VALUES (?,?,?,?,?,?,?,?)",
            ("a" * 64, 0, "b" * 32, 0, 1, bytes(8), (1).to_bytes(8), "c" * 64),
        ),
    }
    with closing(sqlite3.connect(db.path)) as connection:
        statement_key = custody
        if custody == "cohort_member_missing_source":
            connection.execute(
                "INSERT INTO publication_cohort(cohort_id,lifecycle,cursor) VALUES ('orphan','accepted',0)"
            )
            statement_key = "publication_cohort_member"
        elif custody == "v2_stage_range_missing_source":
            connection.execute(
                "INSERT INTO ready_v2_output_stage VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    "a" * 64,
                    "group",
                    "/owned/stage",
                    bytes(32),
                    bytes(8),
                    "b" * 64,
                    "c" * 64,
                    b"descriptor",
                    "d" * 64,
                    "e" * 64,
                    "f" * 64,
                    1,
                    1,
                    1,
                    1,
                    None,
                    "owner",
                    None,
                ),
            )
            statement_key = "ready_v2_stage_range"
        connection.execute(*statements[statement_key])
        connection.commit()

    with pytest.raises(InternalAudioCutoverRequiredError, match="publication custody"):
        open_runtime_state(layout)


def test_current_runtime_preserves_v2_replay_proof_pages(tmp_path: Path) -> None:
    layout = _layout(tmp_path)
    db = StateDB(layout.collector.state_db)
    bundle_id = "a" * 64
    with closing(db.open_connection()) as connection:
        connection.execute(
            "INSERT INTO ready_replay_proof VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                bundle_id,
                "active",
                "v2-stage",
                "v2-owner",
                1,
                "b" * 64,
                "c" * 64,
                b"index",
                b"runs",
                b"metadata",
                "d" * 64,
                "omi-audio-v2",
                bytes(8),
                "e" * 64,
                1,
            ),
        )
        connection.execute(
            "INSERT INTO ready_replay_proof_page VALUES (?,?,?,?,?,?)",
            (bundle_id, 0, 0, 1, b"f" * 32, b"g" * 32),
        )
        connection.commit()
    before = layout.collector.state_db.read_bytes()

    opened = open_runtime_state(layout, read_only=True)

    assert opened.path == db.path
    assert layout.collector.state_db.read_bytes() == before
    with closing(opened.open_connection(read_only=True)) as connection:
        proof = cast(tuple[object, ...] | None, connection.execute("SELECT format FROM ready_replay_proof").fetchone())
        page = cast(
            tuple[object, ...] | None,
            connection.execute("SELECT hashes,page_sha256 FROM ready_replay_proof_page").fetchone(),
        )
        assert proof is not None and tuple(proof) == ("omi-audio-v2",)
        assert page is not None and tuple(page) == (b"f" * 32, b"g" * 32)


def test_runtime_schema_validation_does_not_scan_retained_audio(tmp_path: Path) -> None:
    layout = _layout(tmp_path)
    db = StateDB(layout.collector.state_db)
    db.put(StateKind.MIGRATION, "internal_audio_cutover", {"format": 1, "phase": "COMPLETE", "fresh": True})
    statements: list[str] = []
    with closing(sqlite3.connect(db.path)) as connection:
        connection.set_trace_callback(statements.append)
        _validate_current(connection)
    assert statements
    assert all("quick_check" not in statement.lower() for statement in statements)
    assert all(
        "capture_record" not in statement.lower() or "sqlite_master" in statement.lower() for statement in statements
    )


@pytest.mark.parametrize(("read_only", "expected_timeout"), [(True, 30), (False, 5)])
def test_canonical_validation_uses_bounded_mode_specific_timeout(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, read_only: bool, expected_timeout: int
) -> None:
    layout = _layout(tmp_path)
    StateDB(layout.collector.state_db)
    timeouts: list[float] = []
    connect = sqlite3.connect

    def track_timeout(database: str, *, uri: bool = False, timeout: float = 5.0) -> sqlite3.Connection:
        timeouts.append(timeout)
        return connect(database, uri=uri, timeout=timeout)

    monkeypatch.setattr(state_migration, "_validate_current", lambda _connection: None)
    monkeypatch.setattr(state_migration, "_assert_private_audio_gone", lambda _layout, _connection: None)
    monkeypatch.setattr(sqlite3, "connect", track_timeout)

    state_migration._validate_canonical(layout, read_only=read_only)

    assert timeouts == [expected_timeout]


def test_current_runtime_rejects_legacy_ready_stage_scratch(tmp_path: Path) -> None:
    layout = _layout(tmp_path)
    db = StateDB(layout.collector.state_db)
    db.put(StateKind.MIGRATION, "internal_audio_cutover", {"format": 1, "phase": "COMPLETE", "fresh": True})
    cohort_id, bundle_id = "a" * 64, "b" * 64
    identity = f"{cohort_id}:0:{bundle_id}"
    stage_id = sha256(identity.encode()).hexdigest()
    stage = layout.publication.root / f".{bundle_id}.{stage_id[:16]}.stage"
    stage.mkdir(parents=True)
    stage.joinpath("records.bin").write_bytes(b"partial")
    with closing(db.open_connection()) as connection:
        connection.execute(
            "INSERT INTO publication_cohort(cohort_id,lifecycle,cursor) VALUES (?,'staging',0)", (cohort_id,)
        )
        connection.execute(
            "INSERT INTO ready_output_stage(stage_id,group_identity,owned_path,expected_bundle_digest,"
            "expected_record_count,expected_slice_count,owner_token) VALUES (?,?,?,?,1,1,?)",
            (stage_id, identity, str(stage), "c" * 64, stage_id),
        )
        connection.commit()
    with pytest.raises(InternalAudioCutoverRequiredError, match="publication custody"):
        open_runtime_state(layout)


def test_current_runtime_rejects_unowned_stage_lookalike(tmp_path: Path) -> None:
    layout = _layout(tmp_path)
    db = StateDB(layout.collector.state_db)
    db.put(StateKind.MIGRATION, "internal_audio_cutover", {"format": 1, "phase": "COMPLETE", "fresh": True})
    stage = layout.publication.root / f".{('a' * 64)}.{('b' * 16)}.stage"
    stage.mkdir(parents=True)
    with pytest.raises(InternalAudioCutoverRequiredError, match="unowned ready scratch"):
        open_runtime_state(layout)


def test_current_runtime_accepts_owned_v2_stage_inventory_and_rejects_unsafe_entries(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    layout = _layout(tmp_path)
    db = StateDB(layout.collector.state_db)
    store = InternalAudioStore(db)
    record = (100).to_bytes(4, "big") + bytes((2, 8, 1)) + bytes(AUDIO_RECORD_SIZE - 7)
    source_id = "e" * 32
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
    layout.publication.root.mkdir(mode=0o2750)
    layout.publication.root.chmod(0o2750)
    original_materialize = ready_bundles._materialize_v2_stage_with_failure_latch

    def interrupt(*_args: object, **_kwargs: object) -> object:
        raise ReadyBundleError("simulated loss after stage intent")

    monkeypatch.setattr(ready_bundles, "_materialize_v2_stage_with_failure_latch", interrupt)
    with pytest.raises(ReadyBundleError, match="simulated loss"):
        finalize_public_audio_v2(
            store,
            (ready_bundles.SqlitePublicationSource(store, source_id),),
            layout.publication.root,
            PublicAudioV2Options(ReadyConfig(target_audio_seconds=0.02), ClockState(), 11, True),
        )
    monkeypatch.setattr(ready_bundles, "_materialize_v2_stage_with_failure_latch", original_materialize)
    with closing(db.open_connection(read_only=True)) as connection:
        stage = ready_bundles._sql_one(connection.execute("SELECT owned_path FROM ready_v2_output_stage"))
        assert stage is not None and isinstance(stage[0], str)
        stage_path = Path(stage[0])
    stage_path.mkdir()
    audio = stage_path / "audio.ogg"
    manifest = stage_path / "manifest.json"
    audio.write_bytes(b"audio")
    manifest.write_bytes(b"manifest")
    store.close()

    assert open_runtime_state(layout, read_only=True).path == db.path

    foreign = stage_path / "foreign-marker"
    foreign.write_bytes(b"foreign")
    with pytest.raises(InternalAudioCutoverRequiredError, match="unowned ready scratch"):
        open_runtime_state(layout)
    foreign.unlink()

    manifest.unlink()
    manifest.symlink_to(audio)
    with pytest.raises(InternalAudioCutoverRequiredError, match="unowned ready scratch"):
        open_runtime_state(layout)


def test_historical_migration_payload_is_inert(tmp_path: Path) -> None:
    layout = _layout(tmp_path)
    db = StateDB(layout.collector.state_db)
    db.put(
        StateKind.MIGRATION,
        "internal_audio_cutover",
        {
            "format": 1,
            "phase": "COMPLETE",
            "fresh": True,
            "files": ["old-raw-path"],
        },
    )
    assert open_runtime_state(layout, read_only=True).path == db.path


def test_identified_canonical_access_denial_requires_stopped_repair(tmp_path: Path) -> None:
    layout = _layout(tmp_path)
    db = StateDB(layout.collector.state_db)
    db.put(StateKind.MIGRATION, "internal_audio_cutover", {"format": 1, "phase": "COMPLETE", "fresh": True})
    layout.collector.state_db.chmod(0)
    try:
        with pytest.raises(InternalAudioCutoverRequiredError, match="access needs stopped-service repair"):
            open_runtime_state(layout)
    finally:
        layout.collector.state_db.chmod(0o640)


def test_optional_journal_disappearance_does_not_mask_canonical_access(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    database = tmp_path / "collector.sqlite3"
    database.write_bytes(b"database")
    journal = tmp_path / "collector.sqlite3-journal"
    real_open = os.open
    seen: list[Path] = []

    def open_with_disappearing_journal(path: str | os.PathLike[str], flags: int, mode: int = 0o777) -> int:
        if Path(path) == journal:
            seen.append(journal)
            raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), str(journal))
        return real_open(path, flags, mode)

    monkeypatch.setattr(state_migration.os, "open", open_with_disappearing_journal)
    state_migration._check_canonical_access(database, read_only=True)
    assert seen == [journal]


@pytest.mark.parametrize("error_number", [errno.EACCES, errno.EIO])
def test_optional_journal_access_error_is_not_ignored(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, error_number: int
) -> None:
    database = tmp_path / "collector.sqlite3"
    database.write_bytes(b"database")
    journal = tmp_path / "collector.sqlite3-journal"
    real_open = os.open

    def open_with_journal_error(path: str | os.PathLike[str], flags: int, mode: int = 0o777) -> int:
        if Path(path) == journal:
            raise OSError(error_number, os.strerror(error_number), str(journal))
        return real_open(path, flags, mode)

    monkeypatch.setattr(state_migration.os, "open", open_with_journal_error)
    with pytest.raises(OSError) as failure:
        state_migration._check_canonical_access(database, read_only=True)
    assert failure.value.errno == error_number


def test_optional_journal_refuses_symlink_and_fifo(tmp_path: Path) -> None:
    database = tmp_path / "collector.sqlite3"
    database.write_bytes(b"database")
    journal = tmp_path / "collector.sqlite3-journal"
    journal.symlink_to(database)
    with pytest.raises(InternalAudioCutoverRequiredError, match="journal is unsafe"):
        state_migration._check_canonical_access(database, read_only=True)
    journal.unlink()
    os.mkfifo(journal)
    with pytest.raises(InternalAudioCutoverRequiredError, match="journal is unsafe"):
        state_migration._check_canonical_access(database, read_only=True)


def test_unsupported_database_does_not_auto_upgrade(tmp_path: Path) -> None:
    layout = _layout(tmp_path)
    layout.collector.root.mkdir()
    with closing(sqlite3.connect(layout.collector.state_db)) as connection:
        connection.execute("PRAGMA user_version=99")
        connection.execute("CREATE TABLE state(kind TEXT, id TEXT, payload TEXT)")
        connection.commit()
    with pytest.raises(StateMigrationError, match="unsupported"):
        open_runtime_state(layout)
    with closing(sqlite3.connect(layout.collector.state_db)) as connection:
        assert connection.execute("PRAGMA user_version").fetchone() == (99,)
