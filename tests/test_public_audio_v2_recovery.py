from __future__ import annotations

import errno
import hashlib
import json
import sqlite3
import stat
from collections.abc import Callable, Iterator
from contextlib import closing
from io import BytesIO
from pathlib import Path
from struct import pack
from typing import Literal, cast

import pytest

from omi_collector.capture.adapters import ready_bundles
from omi_collector.capture.adapters.collector_state_db import StateDB, StateKind
from omi_collector.capture.adapters.internal_audio_store import AUDIO_RECORD_SIZE, DrainCheckpoint, InternalAudioStore
from omi_collector.capture.adapters.ready_bundles import (
    PublicAudioV2Options,
    ReadyBundleError,
    ReadyBundleResult,
    ReadyCustodyCache,
    ReadyDurabilityFatalError,
    ReadyOutcomeState,
    SqlitePublicationSource,
    _TypedCohort,
    _V2Commit,
    _V2Stage,
    acknowledged_public_audio_v2_needs_retirement,
    finalize_public_audio_v2,
    publication_contract_state,
    resume_retired_public_audio_v2,
    retire_acknowledged_public_audio_v2,
    retire_one_covered_typed_source,
)
from omi_collector.capture.domain.clock_state import ClockRange, ClockState
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


def _record(timestamp: int) -> bytes:
    return pack(">I", timestamp) + bytes((2, 8, 1)) + bytes(AUDIO_RECORD_SIZE - 7)


def _source(store: InternalAudioStore, source_id: str, start: int, count: int = 1) -> SqlitePublicationSource:
    records = tuple(_record(100 + offset) for offset in range(count))
    store.create_source(source_id, "attempt", start, start + count)
    source = store.prepare_leg(source_id, start, count)
    source = store.record_read_begin(source_id, start, count)
    store.commit_drain(source_id, 0, records, DrainCheckpoint(source.current_leg_generation))
    source = store.get_source(source_id)
    assert source is not None
    store.seal_source(
        source_id,
        leg_generation=source.current_leg_generation,
        expected_count=source.committed_count,
        expected_sha256=source.committed_sha256,
    )
    return SqlitePublicationSource(store, source_id)


def _checkpoint(root: Path) -> Path:
    path = root / "work" / "omi-ready-checkpoint.json"
    path.parent.mkdir()
    path.write_text(
        json.dumps(
            {
                "audio_contract": "omi-audio-v2",
                "analysis_cursor": None,
                "vad_decisions": [],
                "open_speech_tail": None,
                "acknowledged": [],
            }
        ),
        encoding="utf-8",
    )
    return path


def _set_acknowledged(checkpoint: Path, acknowledged: tuple[tuple[str, str], ...]) -> None:
    payload = cast(dict[str, object], json.loads(checkpoint.read_text(encoding="utf-8")))
    payload["acknowledged"] = [{"id": artifact_id, "audio_sha256": digest} for artifact_id, digest in acknowledged]
    checkpoint.write_text(json.dumps(payload), encoding="utf-8")


def _retire_without_removing(
    database: StateDB,
    store: InternalAudioStore,
    ready: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> ReadyBundleResult:
    artifact = finalize_public_audio_v2(store, (_source(store, "a" * 32, 10),), ready, _options()).published[0]
    manifest = cast(dict[str, object], json.loads((artifact.path / "manifest.json").read_bytes()))
    checkpoint = _checkpoint(ready.parent)
    _set_acknowledged(checkpoint, ((artifact.bundle_id, cast(str, manifest["audio_sha256"])),))
    original_remove = ready_bundles._remove_v2_artifact

    def preserve_retired_artifact(
        path: Path, label: Literal["retired public audio v2 artifact", "v2 ready stage"]
    ) -> None:
        if label == "retired public audio v2 artifact":
            return
        original_remove(path, label)

    with monkeypatch.context() as retirement:
        retirement.setattr(ready_bundles, "_remove_v2_artifact", preserve_retired_artifact)
        retire_acknowledged_public_audio_v2(
            ready,
            database,
            checkpoint,
        )
    return artifact


def _options(clock: ClockState | None = None, frontier: int = 11) -> PublicAudioV2Options:
    return PublicAudioV2Options(ReadyConfig(target_audio_seconds=0.02), clock or ClockState(), frontier, True, 128)


def _counts(database: StateDB) -> tuple[int, int, int, int]:
    with closing(database.open_connection(read_only=True)) as connection:
        rows = (
            cast(tuple[object, ...], connection.execute("SELECT count(*) FROM publication_cohort").fetchone()),
            cast(tuple[object, ...], connection.execute("SELECT count(*) FROM ready_v2_output_stage").fetchone()),
            cast(tuple[object, ...], connection.execute("SELECT count(*) FROM ready_replay_proof").fetchone()),
            cast(
                tuple[object, ...],
                connection.execute(
                    "SELECT count(*) FROM state WHERE kind=? AND id!='frontier'", (StateKind.READY_PUBLICATION.value,)
                ).fetchone(),
            ),
        )
    return cast(tuple[int, int, int, int], tuple(cast(int, row[0]) for row in rows))


def _assert_publication_operator_access(path: Path, ready: Path) -> None:
    assert stat.S_IMODE(path.stat().st_mode) == 0o2750
    assert path.stat().st_gid == ready.stat().st_gid
    assert {item.name for item in path.iterdir()} == {"audio.ogg", "manifest.json"}
    for name in ("audio.ogg", "manifest.json"):
        item = path / name
        assert stat.S_IMODE(item.stat().st_mode) == 0o640
        assert item.stat().st_gid == ready.stat().st_gid
        assert item.stat().st_mode & stat.S_IRGRP


def _install_materialization_failure(
    monkeypatch: pytest.MonkeyPatch, ready: Path, boundary: str, after: bool
) -> Callable[[], bool]:
    originals: dict[str, Callable[..., object]] = {
        "write": cast(Callable[..., object], ready_bundles.write_v2_audio),
        "file_sync": cast(Callable[..., object], ready_bundles._sync_regular_file),
        "manifest": cast(Callable[..., object], ready_bundles._write_file),
        "directory_sync": cast(Callable[..., object], ready_bundles._sync_directory),
        "rename": cast(Callable[..., object], Path.rename),
    }
    fired = [False]
    failure = RuntimeError("simulated non-durable process interruption")

    def make_hook(name: str, original: Callable[..., object]) -> Callable[..., object]:
        def hook(*args: object, **kwargs: object) -> object:
            stage_path = args[0] if args and isinstance(args[0], Path) else None
            applies = name not in {"directory_sync", "rename"} or (
                stage_path is not None and stage_path.parent == ready and stage_path.name.endswith(".stage")
            )
            if boundary == name and applies and not after and not fired[0]:
                fired[0] = True
                raise failure
            result = original(*args, **kwargs)
            if boundary == name and applies and after and not fired[0]:
                fired[0] = True
                raise failure
            return result

        return hook

    for name, original in originals.items():
        attribute = {
            "write": "write_v2_audio",
            "file_sync": "_sync_regular_file",
            "manifest": "_write_file",
            "directory_sync": "_sync_directory",
            "rename": "rename",
        }[name]
        owner = Path if name == "rename" else ready_bundles
        monkeypatch.setattr(owner, attribute, make_hook(name, original))
    return lambda: fired[0]


@pytest.mark.parametrize("after_commit", (False, True), ids=("rollback", "ambiguous-commit"))
def test_v2_stage_intent_commit_ambiguity_resumes_one_reservation(
    tmp_path: Path,
    audio_store: tuple[StateDB, InternalAudioStore],
    monkeypatch: pytest.MonkeyPatch,
    after_commit: bool,
) -> None:
    database, store = audio_store
    ready = _ready_root(tmp_path)
    source = _source(store, "a" * 32, 10)
    original = store._commit
    injected = False
    reserved_ids: list[str] = []

    def interrupt() -> None:
        nonlocal injected
        connection = store._require_connection()
        reserved_row = cast(
            tuple[object, ...], connection.execute("SELECT count(*) FROM ready_v2_output_stage").fetchone()
        )
        reserved = reserved_row[0] == 1
        if reserved and not injected:
            injected = True
            reserved_row = cast(
                tuple[object, ...], connection.execute("SELECT artifact_id FROM ready_v2_output_stage").fetchone()
            )
            reserved_ids.append(cast(str, reserved_row[0]))
            if after_commit:
                original()
            raise sqlite3.OperationalError("simulated stage-intent process loss")
        original()

    monkeypatch.setattr(store, "_commit", interrupt)
    outcome = finalize_public_audio_v2(store, (source,), ready, _options())
    monkeypatch.setattr(store, "_commit", original)

    assert injected
    assert len(outcome.published) == 1
    if after_commit:
        assert outcome.published[0].bundle_id == reserved_ids[0]
    else:
        assert outcome.published[0].bundle_id != reserved_ids[0]
    with closing(database.open_connection(read_only=True)) as connection:
        assert connection.execute("SELECT count(*) FROM ready_v2_output_stage").fetchone()[0] == 0
        assert connection.execute("SELECT count(*) FROM ready_replay_proof").fetchone()[0] == 1
        assert (
            connection.execute(
                "SELECT count(*) FROM state WHERE kind=? AND id!='frontier'", (StateKind.READY_PUBLICATION.value,)
            ).fetchone()[0]
            == 1
        )
    assert publication_contract_state(database).next_public_order == 1
    assert len(tuple(ready.glob("*/manifest.json"))) == 1


def test_v2_stage_intent_freezes_time_basis_across_restart(
    tmp_path: Path,
    audio_store: tuple[StateDB, InternalAudioStore],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database, store = audio_store
    ready = _ready_root(tmp_path)
    source = _source(store, "a" * 32, 10)
    original = ready_bundles._materialize_v2_stage_with_failure_latch

    def interrupt(*_args: object, **_kwargs: object) -> object:
        raise ReadyBundleError("simulated process stop after durable stage intent")

    first_clock = ClockState((ClockRange(10, 11, 1.25, 0.5, "confirmed", "before-restart"),))
    monkeypatch.setattr(ready_bundles, "_materialize_v2_stage_with_failure_latch", interrupt)
    with pytest.raises(ReadyBundleError, match="process stop"):
        finalize_public_audio_v2(store, (source,), ready, _options(first_clock))
    with closing(database.open_connection(read_only=True)) as connection:
        reserved = cast(
            tuple[object, ...] | None,
            connection.execute("SELECT artifact_id,expected_manifest_sha256 FROM ready_v2_output_stage").fetchone(),
        )
    assert reserved is not None

    monkeypatch.setattr(ready_bundles, "_materialize_v2_stage_with_failure_latch", original)
    later_clock = ClockState((ClockRange(10, 11, 99.0, 8.0, "confirmed", "after-restart"),))
    outcome = finalize_public_audio_v2(store, (source,), ready, _options(later_clock))
    assert outcome.published[0].bundle_id == cast(str, reserved[0])
    final_manifest = (outcome.published[0].path / "manifest.json").read_bytes()
    assert hashlib.sha256(final_manifest).hexdigest() == reserved[1]
    assert json.loads(final_manifest)["id"] == reserved[0]
    assert publication_contract_state(database).next_public_order == 1


@pytest.mark.parametrize(
    ("boundary", "after"),
    (
        ("write", False),
        ("write", True),
        ("file_sync", False),
        ("file_sync", True),
        ("manifest", False),
        ("manifest", True),
        ("directory_sync", False),
        ("directory_sync", True),
        ("rename", False),
        ("rename", True),
    ),
    ids=(
        "write-before",
        "write-after",
        "file-sync-before",
        "file-sync-after",
        "manifest-before",
        "manifest-after",
        "stage-sync-before",
        "stage-sync-after",
        "rename-before",
        "rename-after",
    ),
)
def test_v2_materialization_process_loss_converges_to_reserved_artifact(
    tmp_path: Path,
    audio_store: tuple[StateDB, InternalAudioStore],
    monkeypatch: pytest.MonkeyPatch,
    boundary: str,
    after: bool,
) -> None:
    database, store = audio_store
    ready = _ready_root(tmp_path)
    source = _source(store, "a" * 32, 10)
    with monkeypatch.context() as patcher:
        fired = _install_materialization_failure(patcher, ready, boundary, after)
        with pytest.raises(RuntimeError, match="simulated non-durable"):
            finalize_public_audio_v2(store, (source,), ready, _options())
        assert fired()
        if boundary == "directory_sync" and after:
            stages = tuple(ready.glob(".*.stage"))
            assert len(stages) == 1
            _assert_publication_operator_access(stages[0], ready)
    with closing(database.open_connection(read_only=True)) as connection:
        reserved = cast(
            tuple[object, ...] | None, connection.execute("SELECT artifact_id FROM ready_v2_output_stage").fetchone()
        )
    assert reserved is not None

    with InternalAudioStore(StateDB(database.path)) as restarted:
        restarted_source = SqlitePublicationSource(restarted, source.source_id)
        outcome = finalize_public_audio_v2(restarted, (restarted_source,), ready, _options())
    assert outcome.published[0].bundle_id == cast(str, reserved[0])
    _assert_publication_operator_access(outcome.published[0].path, ready)
    assert publication_contract_state(database).next_public_order == 1
    assert _counts(database)[1:3] == (0, 1)


@pytest.mark.parametrize("after_commit", (False, True), ids=("rollback", "ambiguous-commit"))
def test_v2_ready_commit_ambiguity_converges_without_duplicate_rows(
    tmp_path: Path,
    audio_store: tuple[StateDB, InternalAudioStore],
    monkeypatch: pytest.MonkeyPatch,
    after_commit: bool,
) -> None:
    database, store = audio_store
    ready = _ready_root(tmp_path)
    source = _source(store, "a" * 32, 10)
    original = ready_bundles._commit_v2_stage_once
    calls = 0

    def fail_once(audio_store_arg: InternalAudioStore, proposal: _V2Commit) -> None:
        nonlocal calls
        calls += 1
        if calls == 1:
            if after_commit:
                original(audio_store_arg, proposal)
            raise sqlite3.OperationalError("simulated final commit loss")
        original(audio_store_arg, proposal)

    monkeypatch.setattr(ready_bundles, "_commit_v2_stage_once", fail_once)
    outcome = finalize_public_audio_v2(store, (source,), ready, _options())

    assert calls >= 1
    assert len(outcome.published) == 1
    assert _counts(database)[1:3] == (0, 1)
    assert publication_contract_state(database).next_public_order == 1
    assert len(tuple(ready.glob("*/manifest.json"))) == 1


def test_v2_eio_durability_failure_latches_and_fails_closed_after_restart(
    tmp_path: Path,
    audio_store: tuple[StateDB, InternalAudioStore],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database, store = audio_store
    ready = _ready_root(tmp_path)
    source = _source(store, "a" * 32, 10)
    original = ready_bundles._write_file

    def fail_manifest(_path: Path, _payload: bytes) -> None:
        raise OSError(errno.EIO, "simulated durable I/O failure")

    monkeypatch.setattr(ready_bundles, "_write_file", fail_manifest)
    with pytest.raises(ReadyBundleError, match="durability I/O failure"):
        finalize_public_audio_v2(store, (source,), ready, _options())
    with closing(database.open_connection(read_only=True)) as connection:
        assert (
            connection.execute("SELECT failure_reason FROM ready_v2_output_stage").fetchone()[0]
            == "durability_io_error"
        )
        assert (
            connection.execute("SELECT count(*) FROM ready_replay_proof WHERE lifecycle='staging'").fetchone()[0] == 1
        )
    monkeypatch.setattr(ready_bundles, "_write_file", original)
    with pytest.raises(ReadyBundleError, match="recorded durability I/O failure"):
        finalize_public_audio_v2(store, (source,), ready, _options())
    assert _counts(database)[1:3] == (1, 1)


@pytest.mark.parametrize("damage", ("remove-root", "remove-page", "tamper-page", "tamper-root"))
def test_v2_stage_proof_damage_fail_closes_before_ready_commit(
    tmp_path: Path,
    audio_store: tuple[StateDB, InternalAudioStore],
    monkeypatch: pytest.MonkeyPatch,
    damage: str,
) -> None:
    database, store = audio_store
    ready = _ready_root(tmp_path)
    source = _source(store, "a" * 32, 10)
    original = ready_bundles._materialize_v2_stage_with_failure_latch

    def damage_proof(
        current_store: InternalAudioStore, cohort: _TypedCohort, stage: _V2Stage, page_limit: int
    ) -> object:
        result = original(current_store, cohort, stage, page_limit)
        with closing(database.open_connection()) as connection:
            if damage == "remove-root":
                connection.execute("DELETE FROM ready_replay_proof")
            elif damage == "tamper-root":
                connection.execute("UPDATE ready_replay_proof SET proof_content_sha256=?", ("0" * 64,))
            else:
                if damage == "remove-page":
                    connection.execute("DELETE FROM ready_replay_proof_page")
                else:
                    connection.execute("UPDATE ready_replay_proof_page SET page_sha256=?", (bytes(32),))
                    connection.execute("UPDATE ready_replay_proof_page SET hashes=?", (bytes(32),))
            connection.commit()
        return result

    monkeypatch.setattr(ready_bundles, "_materialize_v2_stage_with_failure_latch", damage_proof)
    with pytest.raises(ReadyDurabilityFatalError):
        finalize_public_audio_v2(store, (source,), ready, _options())
    assert database.list(StateKind.READY_PUBLICATION) == ()
    with closing(database.open_connection(read_only=True)) as connection:
        assert connection.execute("SELECT count(*) FROM ready_v2_output_stage").fetchone()[0] == 1
        proof = cast(
            tuple[object, ...] | None, connection.execute("SELECT lifecycle FROM ready_replay_proof").fetchone()
        )
        if damage == "remove-root":
            assert proof is None
        else:
            assert proof is not None
            assert proof[0] == "staging"
    assert publication_contract_state(database).next_public_order == 1


@pytest.mark.parametrize("damage", ("descriptor", "page-index", "public-order", "packet-count"))
def test_v2_active_proof_structural_corruption_is_durability_fatal(
    tmp_path: Path,
    audio_store: tuple[StateDB, InternalAudioStore],
    damage: str,
) -> None:
    database, store = audio_store
    ready = _ready_root(tmp_path)
    source = _source(store, "a" * 32, 10)
    outcome = finalize_public_audio_v2(store, (source,), ready, _options())
    assert outcome.state is ReadyOutcomeState.PUBLISHED

    with closing(database.open_connection()) as connection:
        if damage == "descriptor":
            connection.execute("UPDATE ready_replay_proof SET immutable_metadata=?", (b"{",))
        elif damage == "public-order":
            connection.execute("PRAGMA ignore_check_constraints = ON")
            connection.execute("UPDATE ready_replay_proof SET public_order=?", (b"\0",))
        elif damage == "packet-count":
            connection.execute("PRAGMA ignore_check_constraints = ON")
            connection.execute("UPDATE ready_replay_proof SET packet_count=-1")
        else:
            row = cast(
                tuple[object, ...],
                connection.execute(
                    "SELECT bundle_id,public_order,audio_sha256,manifest_sha256,records_sha256,record_count,"
                    "packet_count,normalization_runs,immutable_metadata FROM ready_replay_proof"
                ).fetchone(),
            )
            descriptor = cast(bytes, row[8])
            corrupt_index = b"\0"
            content_sha256 = ready_bundles._v2_proof_content_sha256(
                ready_bundles._V2ProofContent(
                    cast(str, row[0]),
                    int.from_bytes(cast(bytes, row[1]), "big"),
                    cast(str, row[2]),
                    cast(str, row[3]),
                    cast(str, row[4]),
                    cast(int, row[5]),
                    cast(int, row[6]),
                    ready_bundles._v2_descriptor_basis(descriptor)[3],
                    corrupt_index,
                    cast(bytes, row[7]),
                    descriptor,
                )
            )
            connection.execute(
                "UPDATE ready_replay_proof SET page_index=?,proof_content_sha256=?",
                (corrupt_index, content_sha256),
            )
        connection.commit()

    with pytest.raises(ReadyDurabilityFatalError, match="active v2 replay proof"):
        finalize_public_audio_v2(store, (), ready, _options(frontier=11))


@pytest.mark.parametrize("bad_second_ack", ("unknown", "wrong-hash"))
def test_v2_ack_batch_preflights_both_and_retires_idempotently_after_source_gc(
    tmp_path: Path,
    audio_store: tuple[StateDB, InternalAudioStore],
    bad_second_ack: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database, store = audio_store
    ready = _ready_root(tmp_path)
    checkpoint = _checkpoint(tmp_path)
    clock = ClockState((ClockRange(10, 13, 0.0, 0.0, "confirmed", "test"),))
    first_source = _source(store, "a" * 32, 10)
    second_source = _source(store, "b" * 32, 12)
    first_outcome = finalize_public_audio_v2(store, (first_source,), ready, _options(clock, 11))
    second_outcome = finalize_public_audio_v2(store, (second_source,), ready, _options(clock, 13))
    artifacts = first_outcome.published + second_outcome.published
    assert len(artifacts) == 2
    hashes = tuple(
        cast(str, json.loads((item.path / "manifest.json").read_bytes())["audio_sha256"]) for item in artifacts
    )
    valid = tuple((item.bundle_id, digest) for item, digest in zip(artifacts, hashes, strict=True))
    with closing(database.open_connection(read_only=True)) as connection:
        assert connection.execute("SELECT count(*) FROM capture_record").fetchone()[0] == 2
    generation_before_retirement = database.get_generation()
    assert acknowledged_public_audio_v2_needs_retirement(database, valid)
    stale_second = ("f" * 64, "0" * 64) if bad_second_ack == "unknown" else (valid[1][0], "0" * 64)
    _set_acknowledged(checkpoint, (valid[0], stale_second))
    with pytest.raises(ReadyBundleError, match="not a published artifact"):
        retire_acknowledged_public_audio_v2(ready, database, checkpoint)
    assert all(item.path.is_dir() for item in artifacts)
    assert _counts(database)[2] == 2
    assert database.get_generation() == generation_before_retirement

    _set_acknowledged(checkpoint, valid)
    assert len(retire_acknowledged_public_audio_v2(ready, database, checkpoint)) == 2
    assert all(not item.path.exists() for item in artifacts)
    assert _counts(database)[2] == 0
    assert not acknowledged_public_audio_v2_needs_retirement(database, valid)
    assert database.get_generation() == generation_before_retirement
    debt = database.get(StateKind.COMPACTION, "current")
    assert debt is not None and debt["pending"] is True
    with monkeypatch.context() as no_op:
        no_op.setattr(ready_bundles, "_digest", lambda _path: pytest.fail("retired ACK hashed audio"))
        no_op.setattr(
            ready_bundles, "resume_retired_public_audio_v2", lambda *_args: pytest.fail("retired ACK touched files")
        )
        assert retire_acknowledged_public_audio_v2(ready, database, checkpoint) == ()
        _set_acknowledged(checkpoint, (valid[0], stale_second))
        with pytest.raises(ReadyBundleError, match="not a published artifact"):
            retire_acknowledged_public_audio_v2(ready, database, checkpoint)
    assert _counts(database)[2] == 0


def test_v2_ack_yields_between_completed_retirement_groups(
    tmp_path: Path,
    audio_store: tuple[StateDB, InternalAudioStore],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database, store = audio_store
    ready = _ready_root(tmp_path)
    checkpoint = _checkpoint(tmp_path)
    clock = ClockState((ClockRange(10, 13, 0.0, 0.0, "confirmed", "test"),))
    first = finalize_public_audio_v2(store, (_source(store, "a" * 32, 10),), ready, _options(clock, 11)).published[0]
    second = finalize_public_audio_v2(store, (_source(store, "b" * 32, 12),), ready, _options(clock, 13)).published[0]
    acknowledged = tuple(
        (artifact.bundle_id, cast(str, json.loads((artifact.path / "manifest.json").read_bytes())["audio_sha256"]))
        for artifact in (first, second)
    )
    _set_acknowledged(checkpoint, acknowledged)
    deferred = False
    original_remove = ready_bundles._remove_v2_artifact

    def remove_then_defer(path: Path, label: Literal["retired public audio v2 artifact", "v2 ready stage"]) -> None:
        nonlocal deferred
        original_remove(path, label)
        if label == "retired public audio v2 artifact":
            deferred = True

    with monkeypatch.context() as interruption:
        interruption.setattr(ready_bundles, "_remove_v2_artifact", remove_then_defer)
        retired = retire_acknowledged_public_audio_v2(ready, database, checkpoint, should_defer=lambda: deferred)
    assert tuple(result.bundle_id for result in retired) == (first.bundle_id,)
    assert not first.path.exists() and second.path.is_dir()
    assert acknowledged_public_audio_v2_needs_retirement(database, acknowledged)
    assert tuple(result.bundle_id for result in retire_acknowledged_public_audio_v2(ready, database, checkpoint)) == (
        second.bundle_id,
    )
    assert not acknowledged_public_audio_v2_needs_retirement(database, acknowledged)


@pytest.mark.parametrize("mutated", ("none", "audio", "manifest", "proof", "ledger"))
def test_v2_ack_retirement_reuses_only_current_authenticated_custody(
    tmp_path: Path,
    audio_store: tuple[StateDB, InternalAudioStore],
    monkeypatch: pytest.MonkeyPatch,
    mutated: str,
) -> None:
    database, store = audio_store
    ready = _ready_root(tmp_path)
    checkpoint = _checkpoint(tmp_path)
    artifact = finalize_public_audio_v2(store, (_source(store, "a" * 32, 10),), ready, _options()).published[0]
    cached = finalize_public_audio_v2(store, (), ready, _options()).custody_cache
    assert cached is not None
    digest = cast(str, json.loads((artifact.path / "manifest.json").read_bytes())["audio_sha256"])
    acknowledged = ((artifact.bundle_id, digest),)
    _set_acknowledged(checkpoint, acknowledged)
    audio = artifact.path / "audio.ogg"
    if mutated == "audio":
        content = audio.read_bytes()
        audio.write_bytes(bytes((content[0] ^ 1,)) + content[1:])
    elif mutated == "manifest":
        manifest = artifact.path / "manifest.json"
        manifest.write_bytes(manifest.read_bytes() + b" ")
    elif mutated == "proof":
        with closing(database.open_connection()) as connection:
            connection.execute(
                "UPDATE ready_replay_proof SET proof_content_sha256=? WHERE bundle_id=?",
                ("0" * 64, artifact.bundle_id),
            )
            connection.commit()
    elif mutated == "ledger":
        entry = database.get(StateKind.READY_PUBLICATION, artifact.bundle_id)
        assert entry is not None
        database.put(StateKind.READY_PUBLICATION, artifact.bundle_id, {**entry, "audio_sha256": "0" * 64})
    if mutated != "none":
        with pytest.raises((ReadyBundleError, ReadyDurabilityFatalError)):
            retire_acknowledged_public_audio_v2(ready, database, checkpoint, custody_cache=cached)
        entry = database.get(StateKind.READY_PUBLICATION, artifact.bundle_id)
        assert entry is not None and entry["state"] == "ready"
        assert artifact.path.exists()
    else:
        monkeypatch.setattr(ready_bundles, "_digest", lambda _path: pytest.fail("current custody was rehashed"))
        assert len(retire_acknowledged_public_audio_v2(ready, database, checkpoint, custody_cache=cached)) == 1
        assert not artifact.path.exists()


def test_v2_ack_change_during_custody_check_blocks_first_marker(
    tmp_path: Path,
    audio_store: tuple[StateDB, InternalAudioStore],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database, store = audio_store
    ready = _ready_root(tmp_path)
    checkpoint = _checkpoint(tmp_path)
    artifact = finalize_public_audio_v2(store, (_source(store, "a" * 32, 10),), ready, _options()).published[0]
    digest = cast(str, json.loads((artifact.path / "manifest.json").read_bytes())["audio_sha256"])
    acknowledged = ((artifact.bundle_id, digest),)
    _set_acknowledged(checkpoint, acknowledged)
    original = ready_bundles._v2_ready_custody

    def change_checkpoint(
        ready_root: Path,
        state_db: StateDB,
        previous: ReadyCustodyCache | None = None,
        should_defer: Callable[[], bool] | None = None,
    ) -> ReadyCustodyCache:
        custody = original(ready_root, state_db, previous, should_defer)
        _set_acknowledged(checkpoint, ())
        return custody

    monkeypatch.setattr(ready_bundles, "_v2_ready_custody", change_checkpoint)
    with pytest.raises(ReadyBundleError, match="changed before retirement"):
        retire_acknowledged_public_audio_v2(ready, database, checkpoint)
    entry = database.get(StateKind.READY_PUBLICATION, artifact.bundle_id)
    assert entry is not None and entry["state"] == "ready"
    assert artifact.path.exists()


def test_v2_ack_retirement_yields_before_first_audio_read(
    tmp_path: Path, audio_store: tuple[StateDB, InternalAudioStore]
) -> None:
    database, store = audio_store
    ready = _ready_root(tmp_path)
    checkpoint = _checkpoint(tmp_path)
    artifact = finalize_public_audio_v2(store, (_source(store, "a" * 32, 10),), ready, _options()).published[0]
    digest = cast(str, json.loads((artifact.path / "manifest.json").read_bytes())["audio_sha256"])
    _set_acknowledged(checkpoint, ((artifact.bundle_id, digest),))
    checks = 0

    def should_defer() -> bool:
        nonlocal checks
        checks += 1
        return checks > 1

    assert retire_acknowledged_public_audio_v2(ready, database, checkpoint, should_defer=should_defer) == ()
    entry = database.get(StateKind.READY_PUBLICATION, artifact.bundle_id)
    assert entry is not None and entry["state"] == "ready"
    assert artifact.path.exists()


def test_v2_digest_yields_after_first_megabyte() -> None:
    class CountedStream(BytesIO):
        bytes_read = 0

        def read(self, size: int | None = -1) -> bytes:
            chunk = super().read(size)
            self.bytes_read += len(chunk)
            return chunk

    class AudioPath:
        def __init__(self) -> None:
            self.stream = CountedStream(b"a" * (2 * 1024 * 1024))

        def is_symlink(self) -> bool:
            return False

        def is_file(self) -> bool:
            return True

        def open(self, _mode: str) -> CountedStream:
            return self.stream

    path = AudioPath()
    checks = 0

    def should_defer() -> bool:
        nonlocal checks
        checks += 1
        return checks > 1

    with pytest.raises(ready_bundles._ReadyMaintenanceDeferredError):
        ready_bundles._digest(cast(Path, path), should_defer)
    assert path.stream.bytes_read == 1024 * 1024


def test_v2_retirement_resumes_after_process_loss_between_file_unlinks(
    tmp_path: Path,
    audio_store: tuple[StateDB, InternalAudioStore],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database, store = audio_store
    ready = _ready_root(tmp_path)
    checkpoint = _checkpoint(tmp_path)
    artifact = finalize_public_audio_v2(store, (_source(store, "a" * 32, 10),), ready, _options()).published[0]
    manifest = cast(dict[str, object], json.loads((artifact.path / "manifest.json").read_bytes()))
    _set_acknowledged(checkpoint, ((artifact.bundle_id, cast(str, manifest["audio_sha256"])),))
    original_unlink = Path.unlink
    interrupted = False

    def interrupt_after_unlink(path: Path, *, missing_ok: bool = False) -> None:
        nonlocal interrupted
        original_unlink(path, missing_ok=missing_ok)
        if path.parent == artifact.path and not interrupted:
            interrupted = True
            raise RuntimeError("simulated process loss after first retired file unlink")

    with monkeypatch.context() as interruption:
        interruption.setattr(Path, "unlink", interrupt_after_unlink)
        with pytest.raises(RuntimeError, match="process loss"):
            retire_acknowledged_public_audio_v2(
                ready,
                database,
                checkpoint,
            )

    assert interrupted
    assert {item.name for item in artifact.path.iterdir()} in ({"audio.ogg"}, {"manifest.json"})
    retired = database.get(StateKind.READY_PUBLICATION, artifact.bundle_id)
    assert retired is not None and retired["state"] == "retired"
    assert _counts(database)[2] == 0

    resume_retired_public_audio_v2(ready, database)

    assert not artifact.path.exists()


@pytest.mark.parametrize(
    "remaining",
    (None, (), ("audio.ogg",), ("manifest.json",), ("audio.ogg", "manifest.json")),
    ids=("absent", "empty", "audio-only", "manifest-only", "full-pair"),
)
def test_v2_retired_resume_accepts_every_known_file_subset(
    tmp_path: Path,
    audio_store: tuple[StateDB, InternalAudioStore],
    monkeypatch: pytest.MonkeyPatch,
    remaining: tuple[str, ...] | None,
) -> None:
    database, store = audio_store
    ready = _ready_root(tmp_path)
    artifact = _retire_without_removing(database, store, ready, monkeypatch)
    retained = set(remaining or ())
    for item in artifact.path.iterdir():
        if item.name not in retained:
            item.unlink()
    if remaining is None:
        artifact.path.rmdir()

    resume_retired_public_audio_v2(ready, database)

    assert not artifact.path.exists()


@pytest.mark.parametrize("unsafe", ("foreign", "directory", "symlink"))
def test_v2_retired_resume_rejects_foreign_and_unsafe_entries(
    tmp_path: Path,
    audio_store: tuple[StateDB, InternalAudioStore],
    monkeypatch: pytest.MonkeyPatch,
    unsafe: str,
) -> None:
    database, store = audio_store
    ready = _ready_root(tmp_path)
    artifact = _retire_without_removing(database, store, ready, monkeypatch)
    if unsafe == "foreign":
        (artifact.path / "unexpected").write_bytes(b"unsafe")
    else:
        audio = artifact.path / "audio.ogg"
        audio.unlink()
        if unsafe == "directory":
            audio.mkdir()
        else:
            audio.symlink_to("manifest.json")

    with pytest.raises(ReadyBundleError, match=r"inventory is invalid|artifact is unsafe"):
        resume_retired_public_audio_v2(ready, database)

    assert artifact.path.exists()


def test_covered_source_retirement_reports_final_deletion_without_waking_publication(
    tmp_path: Path, audio_store: tuple[StateDB, InternalAudioStore]
) -> None:
    database, store = audio_store
    ready = _ready_root(tmp_path)
    source = _source(store, "a" * 32, 10)
    assert finalize_public_audio_v2(store, (source,), ready, _options()).published
    generation = database.get_generation()

    assert not retire_one_covered_typed_source(store, 1, should_defer=lambda: True)
    assert retire_one_covered_typed_source(store, 1)
    assert database.get_generation() == generation
    assert store.get_source(source.source_id) is not None
    assert retire_one_covered_typed_source(store, 1)
    assert database.get_generation() == generation
    assert store.get_source(source.source_id) is None
    assert not retire_one_covered_typed_source(store, 1)


def test_v2_ready_retirement_preflight_still_requires_the_full_pair(
    tmp_path: Path,
    audio_store: tuple[StateDB, InternalAudioStore],
) -> None:
    database, store = audio_store
    ready = _ready_root(tmp_path)
    artifact = finalize_public_audio_v2(store, (_source(store, "a" * 32, 10),), ready, _options()).published[0]
    manifest = cast(dict[str, object], json.loads((artifact.path / "manifest.json").read_bytes()))
    (artifact.path / "manifest.json").unlink()
    checkpoint = _checkpoint(tmp_path)
    _set_acknowledged(checkpoint, ((artifact.bundle_id, cast(str, manifest["audio_sha256"])),))

    with pytest.raises(ReadyBundleError):
        retire_acknowledged_public_audio_v2(
            ready,
            database,
            checkpoint,
        )

    entry = database.get(StateKind.READY_PUBLICATION, artifact.bundle_id)
    assert entry is not None and entry["state"] == "ready"
    assert _counts(database)[2] == 1


def test_v2_waiting_preserves_covered_source_until_bounded_maintenance_after_ack(tmp_path: Path) -> None:
    database = StateDB(tmp_path / "collector-state.sqlite3")
    ready = _ready_root(tmp_path)
    checkpoint = _checkpoint(tmp_path)
    with InternalAudioStore(database) as store:
        published_source = _source(store, "a" * 32, 10, count=3)
        clock = ClockState((ClockRange(10, 21, 0.0, 0.0, "confirmed", "test"),))
        options = PublicAudioV2Options(ReadyConfig(target_audio_seconds=0.02), clock, 13, True, 1)
        artifact = finalize_public_audio_v2(store, (published_source,), ready, options).published[0]
        manifest = cast(dict[str, object], json.loads((artifact.path / "manifest.json").read_bytes()))
        _set_acknowledged(checkpoint, ((artifact.bundle_id, cast(str, manifest["audio_sha256"])),))
        retire_acknowledged_public_audio_v2(
            ready,
            database,
            checkpoint,
        )
        zero = bytearray(AUDIO_RECORD_SIZE)
        zero[:4] = (200).to_bytes(4, "big")
        store.create_source("b" * 32, "attempt", 20, 21)
        snapshot = store.prepare_leg("b" * 32, 20, 1)
        snapshot = store.record_read_begin("b" * 32, 20, 1)
        store.commit_drain("b" * 32, 0, (bytes(zero),), DrainCheckpoint(snapshot.current_leg_generation))
        snapshot = store.get_source("b" * 32)
        assert snapshot is not None
        store.seal_source(
            "b" * 32,
            leg_generation=snapshot.current_leg_generation,
            expected_count=snapshot.committed_count,
            expected_sha256=snapshot.committed_sha256,
        )
        zero_source = SqlitePublicationSource(store, "b" * 32)
        waiting_options = PublicAudioV2Options(ReadyConfig(target_audio_seconds=0.02), clock, 21, True, 1)

        waiting = finalize_public_audio_v2(store, (zero_source,), ready, waiting_options)
        assert store.get_source("a" * 32) is not None
        generation = database.get_generation()
        for _attempt in range(5):
            if store.get_source("a" * 32) is None:
                break
            assert retire_one_covered_typed_source(store, 1)

        assert waiting.state is ReadyOutcomeState.WAITING
        assert store.get_source("a" * 32) is None
        assert database.get_generation() == generation
        preserved = store.get_source("b" * 32)
        assert preserved is not None and preserved.lifecycle == "sealed_waiting"
        assert len(store.read_record_page("b" * 32, -1, 2)) == 1
        assert not artifact.path.exists()
        with closing(database.open_connection(read_only=True)) as connection:
            assert connection.execute("SELECT count(*) FROM ready_replay_proof").fetchone()[0] == 0
