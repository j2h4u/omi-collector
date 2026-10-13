from __future__ import annotations

import hashlib
import json
import multiprocessing
import os
import select
import signal
import socket
import sqlite3
import stat
import subprocess
from collections.abc import Iterable, Sequence
from contextlib import closing
from multiprocessing.connection import Connection
from pathlib import Path
from typing import cast

import pytest
from typer.testing import CliRunner

import omi_collector.cli as collector_cli
import omi_collector.internal_audio_migration as migration
from omi_collector.capture.adapters.legacy_audio_import import DEFAULT_CHUNK_LIMIT, LegacyDraft
from omi_collector.cli import app
from omi_collector.internal_audio_migration import migrate_internal_audio
from omi_collector.state_migration import open_runtime_state
from omi_collector.storage_layout import CollectorLayout, PublicationLayout, StorageLayout, load_operator_config
from test_internal_audio_migration import _draft, _layout, _release, _v1
from test_systemd_unit import (
    _deployment_harness,
    _DeploymentHarness,
    _DeploymentScenario,
    _previous_release,
    _run_deployer,
)


@pytest.fixture(autouse=True)
def _local_handoff_probe(monkeypatch: pytest.MonkeyPatch) -> None:
    def verify(path: Path) -> None:
        info = path.lstat()
        assert stat.S_ISREG(info.st_mode)
        assert (info.st_uid, info.st_gid) == (path.parent.stat().st_uid, path.parent.stat().st_gid)
        assert stat.S_IMODE(info.st_mode) == 0o640
        descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
        os.close(descriptor)

    monkeypatch.setattr(migration, "_verify_handoff_access", verify)


def _barrier(connection: Connection, detail: object = None) -> None:
    connection.send(("ready", detail))
    connection.recv()


def _assert_runtime_exit_78(config_path: Path) -> None:
    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(collector_cli, "_require_host_singleton", lambda _loaded: None)
        result = CliRunner().invoke(app, ["service", "--config", str(config_path)])
    assert result.exit_code == 78, result.output


def _fetch_row(connection: sqlite3.Connection, query: str) -> tuple[object, ...]:
    row = cast(tuple[object, ...] | None, connection.execute(query).fetchone())
    assert row is not None
    return cast(tuple[object, ...], row)


def _hook_receipt(checkpoint: str, connection: Connection) -> None:
    original_put = migration._put_receipt

    def put_receipt(database: sqlite3.Connection, receipt: dict[str, object]) -> None:
        original_put(database, receipt)
        if checkpoint in {"building", "cleanup_pending", "complete"}:
            expected = {
                "building": "BUILDING",
                "cleanup_pending": "CLEANUP_PENDING",
                "complete": "COMPLETE",
            }[checkpoint]
            if receipt.get("phase") == expected and (checkpoint != "complete" or "source_revision" in receipt):
                _barrier(connection, receipt.copy())

    migration._put_receipt = put_receipt


def _hook_partial_candidate(connection: Connection) -> None:
    original_import = migration.import_legacy_draft

    class ConnectionProxy:
        def __init__(self, target: sqlite3.Connection) -> None:
            self.target = target

        def execute(self, sql: str, parameters: Sequence[object] = ()) -> sqlite3.Cursor:
            return self.target.execute(sql, parameters)

        def executemany(self, sql: str, rows: Iterable[Sequence[object]]) -> sqlite3.Cursor:
            cursor = self.target.executemany(sql, rows)
            database_list = cast(tuple[int, str, str] | None, self.target.execute("PRAGMA database_list").fetchone())
            assert database_list is not None
            journal = Path(database_list[2] + "-journal")
            if journal.exists():
                _barrier(connection, str(journal))
            return cursor

        def commit(self) -> None:
            self.target.commit()

        def rollback(self) -> None:
            self.target.rollback()

    def import_draft(target: sqlite3.Connection, draft: LegacyDraft, *, chunk_limit: int = DEFAULT_CHUNK_LIMIT) -> None:
        original_import(cast(sqlite3.Connection, ConnectionProxy(target)), draft, chunk_limit=chunk_limit)

    migration.import_legacy_draft = import_draft


def _hook_prepared_candidate(connection: Connection) -> None:
    original = migration._backup_and_convert

    def backup_and_convert(*args: object) -> None:
        original(*args)  # type: ignore[arg-type]
        _barrier(connection)

    migration._backup_and_convert = backup_and_convert


def _hook_activated_prepared(connection: Connection) -> None:
    original = migration._resume_v2
    first = True

    def resume_v2(layout_arg: object, canonical: sqlite3.Connection) -> dict[str, int]:
        nonlocal first
        receipt = migration._read_receipt(canonical)
        if first and receipt is not None and receipt.get("phase") == "PREPARED":
            first = False
            _barrier(connection)
        return original(layout_arg, canonical)  # type: ignore[arg-type]

    migration._resume_v2 = resume_v2


def _hook_deletion(checkpoint: str, connection: Connection) -> None:
    wanted = int(checkpoint.removeprefix("delete_"))
    original_unlink, original_rmdir = Path.unlink, Path.rmdir
    original_fsync = migration._fsync_directory
    pending: set[Path] = set()
    synchronized = 0

    def unlink(path: Path, missing_ok: bool = False) -> None:
        original_unlink(path, missing_ok=missing_ok)
        pending.add(path)

    def rmdir(path: Path) -> None:
        original_rmdir(path)
        pending.add(path)

    def fsync_directory(path: Path) -> None:
        nonlocal synchronized
        original_fsync(path)
        durable = {candidate for candidate in pending if not candidate.exists() and candidate.parent == path}
        synchronized += len(durable)
        pending.difference_update(durable)
        if synchronized == wanted:
            _barrier(connection, synchronized)

    patch = pytest.MonkeyPatch()
    patch.setattr(Path, "unlink", unlink)
    patch.setattr(Path, "rmdir", rmdir)
    migration._fsync_directory = fsync_directory


def _hook_handoff(connection: Connection) -> None:
    def verify(path: Path) -> None:
        info = path.lstat()
        assert stat.S_ISREG(info.st_mode)
        assert stat.S_IMODE(info.st_mode) == 0o640
        descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
        os.close(descriptor)
        # The separate root-only handoff test exercises distinct account credentials.
        _barrier(connection)

    migration._verify_handoff_access = verify


def _crash_child(root: str, release: str, checkpoint: str, connection: Connection) -> None:
    """Run a real migration and stop only after the named durable boundary."""

    layout = _layout_from_root(Path(root))
    hooks = {
        "building": lambda: _hook_receipt(checkpoint, connection),
        "cleanup_pending": lambda: _hook_receipt(checkpoint, connection),
        "complete": lambda: _hook_receipt(checkpoint, connection),
        "partial_candidate": lambda: _hook_partial_candidate(connection),
        "prepared_candidate": lambda: _hook_prepared_candidate(connection),
        "activated_prepared": lambda: _hook_activated_prepared(connection),
        "handoff": lambda: _hook_handoff(connection),
    }
    if checkpoint.startswith("delete_"):
        _hook_deletion(checkpoint, connection)
    else:
        hooks[checkpoint]()

    try:
        migrate_internal_audio(layout, release_metadata=Path(release))
    except BaseException as error:
        connection.send(("error", repr(error)))
        raise


def _layout_from_root(root: Path) -> StorageLayout:
    return load_operator_config(root / "config.toml").storage


def _attempt(layout: StorageLayout) -> tuple[Path, bytes]:
    attempt_id = "a" * 32
    layout.collector.attempts.mkdir(exist_ok=True)
    path = layout.collector.attempts / attempt_id
    path.mkdir()
    complete = b"".join(index.to_bytes(4, "big") + bytes([index]) * 440 for index in range(3))
    raw = complete + b"partial"
    descriptor = {
        "attempt_id": attempt_id,
        "schema_version": 2,
        "start_sequence": 100,
        "packet_count": 4,
        "record_size": 444,
        "read_begin_start": 100,
        "read_begin_count": 4,
    }
    checkpoint = {
        "version": 1,
        "attempt_id": attempt_id,
        "record_count": 2,
        "raw_sha256": hashlib.sha256(complete[:888]).hexdigest(),
    }
    (path / "attempt.json").write_text(json.dumps(descriptor), encoding="utf-8")
    (path / "checkpoint.json").write_text(json.dumps(checkpoint), encoding="utf-8")
    (path / "records.bin").write_bytes(raw)
    with closing(sqlite3.connect(layout.collector.state_db)) as connection:
        connection.execute(
            "INSERT INTO state VALUES ('attempt',?,?)",
            (
                attempt_id,
                json.dumps(
                    {
                        "descriptor": descriptor,
                        "checkpoint": checkpoint,
                        "prefix_publication": None,
                        "terminal_retirement": None,
                    }
                ),
            ),
        )
        connection.commit()
    return path, raw


def _assert_final(storage: StorageLayout, raw: bytes, count: int, sentinel: Path) -> None:
    with closing(sqlite3.connect(storage.collector.state_db)) as connection:
        assert connection.execute("PRAGMA integrity_check").fetchone() == ("ok",)
        assert connection.execute("PRAGMA user_version").fetchone() == (2,)
        rows = cast(
            list[tuple[int, bytes]],
            connection.execute("SELECT device_timestamp,payload FROM capture_record ORDER BY ordinal").fetchall(),
        )
        reconstructed = b"".join(timestamp.to_bytes(4, "big") + payload for timestamp, payload in rows)
        assert reconstructed == raw
        assert hashlib.sha256(reconstructed).hexdigest() == hashlib.sha256(raw).hexdigest()
        source = _fetch_row(
            connection,
            "SELECT sequence_start,sequence_limit,committed_count,committed_sha256 FROM capture_source LIMIT 1",
        )
        assert source == (
            (700).to_bytes(8, "big"),
            (700 + count).to_bytes(8, "big"),
            count,
            hashlib.sha256(raw).hexdigest(),
        )
        assert json.loads(
            cast(str, _fetch_row(connection, "SELECT payload FROM state WHERE kind='runtime_status'")[0])
        ) == {"last_error": "тест"}
        receipt = cast(
            dict[str, object],
            json.loads(
                cast(
                    str,
                    _fetch_row(
                        connection,
                        "SELECT payload FROM state WHERE kind='migration' AND id='internal_audio_cutover'",
                    )[0],
                )
            ),
        )
        assert receipt["phase"] == "COMPLETE"
        assert set(receipt) == {"format", "phase", "source_revision", "source_sha256", "imported_sources"}
    assert not tuple(
        path
        for root in (storage.collector.attempts, storage.collector.quarantine, storage.draft)
        if root.exists()
        for path in root.rglob("*")
    )
    assert sentinel.read_bytes() == b"unowned"


def _assert_attempt_final(storage: StorageLayout, raw: bytes, sentinel: Path) -> None:
    complete = raw[:-7]
    with closing(sqlite3.connect(storage.collector.state_db)) as connection:
        rows = cast(
            list[tuple[int, bytes]],
            connection.execute("SELECT device_timestamp,payload FROM capture_record ORDER BY ordinal").fetchall(),
        )
        reconstructed = b"".join(timestamp.to_bytes(4, "big") + payload for timestamp, payload in rows)
        assert reconstructed == complete
        assert _fetch_row(
            connection,
            "SELECT sequence_start,sequence_limit,committed_count,committed_sha256,certified_prefix_count "
            "FROM capture_source WHERE source_type='attempt'",
        ) == (
            (100).to_bytes(8, "big"),
            (104).to_bytes(8, "big"),
            3,
            hashlib.sha256(complete).hexdigest(),
            2,
        )
        assert connection.execute("PRAGMA integrity_check").fetchone() == ("ok",)
        assert connection.execute("PRAGMA user_version").fetchone() == (2,)
        receipt = cast(
            dict[str, object],
            json.loads(
                cast(
                    str,
                    _fetch_row(
                        connection,
                        "SELECT payload FROM state WHERE kind='migration' AND id='internal_audio_cutover'",
                    )[0],
                )
            ),
        )
        assert receipt["phase"] == "COMPLETE"
        assert json.loads(
            cast(str, _fetch_row(connection, "SELECT payload FROM state WHERE kind='runtime_status'")[0])
        ) == {"last_error": "тест"}
    assert not tuple(storage.collector.attempts.rglob("*"))
    assert sentinel.read_bytes() == b"unowned"


def _run_to_migration_killpoint(
    storage: StorageLayout, release: Path, checkpoint: str, expected_phase: str
) -> tuple[object, dict[str, object] | None]:
    context = multiprocessing.get_context("fork")
    parent, child = context.Pipe(duplex=True)
    process = context.Process(target=_crash_child, args=(str(storage.root), str(release), checkpoint, child))
    process.start()
    child.close()
    building_receipt: dict[str, object] | None = None
    try:
        assert parent.poll(30), f"child did not reach durable checkpoint {checkpoint}; exit={process.exitcode}"
        state, detail = cast(tuple[str, object], parent.recv())
        assert state == "ready", detail
        if checkpoint == "building":
            building_receipt = cast(dict[str, object], detail)
            assert not (storage.collector.root / cast(str, building_receipt["candidate"])).exists()
        if checkpoint == "partial_candidate":
            assert isinstance(detail, str)
            assert Path(detail).exists()
        with closing(sqlite3.connect(storage.collector.state_db)) as connection:
            expected_version = 1 if expected_phase == "BUILDING" else 2
            assert connection.execute("PRAGMA user_version").fetchone() == (expected_version,)
            receipt = cast(
                dict[str, object],
                json.loads(
                    cast(
                        str,
                        _fetch_row(
                            connection,
                            "SELECT payload FROM state WHERE id='internal_audio_cutover'",
                        )[0],
                    )
                ),
            )
            assert receipt["phase"] == expected_phase
            if building_receipt is not None:
                assert receipt["uuid"] == building_receipt["uuid"]
        pid = process.pid
        assert pid is not None
        os.kill(pid, signal.SIGKILL)
        process.join(10)
        assert process.exitcode == -signal.SIGKILL
    finally:
        if process.is_alive():
            pid = process.pid
            assert pid is not None
            os.kill(pid, signal.SIGKILL)
            process.join(10)
        parent.close()
    if expected_phase != "COMPLETE":
        _assert_runtime_exit_78(storage.root / "config.toml")
    else:
        open_runtime_state(storage, read_only=True)
    return detail, building_receipt


def _seed_deployer_storage(harness: _DeploymentHarness) -> tuple[StorageLayout, bytes]:
    state_file = Path(harness.environment["DEPLOY_STATE_FILE"])
    storage = StorageLayout(
        root=state_file.parent.parent,
        collector=CollectorLayout(
            root=state_file.parent,
            attempts=state_file.parent / "attempts",
            quarantine=state_file.parent / "quarantine",
            lock=state_file.parent / "collector.lock",
            state_db=state_file,
            debug_log=state_file.parent / "debug.jsonl",
        ),
        draft=state_file.parent.parent / "draft",
        publication=PublicationLayout(state_file.parent.parent / "ready"),
    )
    storage.collector.root.mkdir(parents=True, exist_ok=True)
    storage.collector.attempts.mkdir(exist_ok=True)
    storage.collector.quarantine.mkdir(exist_ok=True)
    storage.draft.mkdir(exist_ok=True)
    storage.publication.root.mkdir(exist_ok=True)
    (storage.root / "config.toml").write_text(harness.config_file.read_text(encoding="utf-8"), encoding="utf-8")
    _v1(storage)
    _draft(storage)
    raw = b"".join(index.to_bytes(4, "big") + bytes([index % 251]) * 440 for index in range(257))
    return storage, raw


def _install_selector_barrier(harness: _DeploymentHarness, *, after_publish: bool) -> None:
    script = harness.deployer.read_text(encoding="utf-8")
    barrier = (
        '/usr/bin/python3 -c \'import os, socket; sock=socket.socket(fileno=int(os.environ["CUTOVER_BARRIER_FD"])); '
    )
    barrier += 'sock.sendall(b"ready"); sock.recv(1)\'\n'
    if after_publish:
        anchor = 'publish_current_target "$current_link" "releases/${release_name}" "$temporary_link"\n'
    else:
        anchor = 'mv -T -- "$temporary_link" "$current_link" || die \'could not publish current deployment selector\'\n'
    assert anchor in script
    if after_publish:
        script = script.replace(anchor, anchor + barrier, 1)
    else:
        script = script.replace(anchor, barrier + anchor, 1)
    harness.deployer.write_text(script, encoding="utf-8")


def _kill_deployer_at_selector_barrier(harness: _DeploymentHarness, tmp_path: Path) -> None:
    parent_socket, child_socket = socket.socketpair()
    environment = {**harness.environment, "CUTOVER_BARRIER_FD": str(child_socket.fileno())}
    process = subprocess.Popen(
        [harness.deployer],
        env=environment,
        cwd=tmp_path,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        pass_fds=(child_socket.fileno(),),
        start_new_session=True,
    )
    child_socket.close()
    try:
        ready, _, _ = select.select([parent_socket], [], [], 60)
        assert ready, "deployer did not reach selector killpoint"
        assert parent_socket.recv(5) == b"ready"
        assert process.poll() is None
        os.killpg(process.pid, signal.SIGKILL)
        _stdout, stderr = process.communicate(timeout=10)
        assert process.returncode == -signal.SIGKILL, stderr.decode(errors="replace")
    finally:
        parent_socket.close()
        if process.poll() is None:
            os.killpg(process.pid, signal.SIGKILL)
            process.wait(timeout=10)


def _kill_deployer_at_selector(
    tmp_path: Path, *, after_publish: bool
) -> tuple[_DeploymentHarness, Path, StorageLayout, bytes]:
    harness = _deployment_harness(tmp_path, _DeploymentScenario(migration_activates=True))
    previous = _previous_release(harness)
    storage, raw = _seed_deployer_storage(harness)
    _install_selector_barrier(harness, after_publish=after_publish)
    _kill_deployer_at_selector_barrier(harness, tmp_path)
    return harness, previous, storage, raw


@pytest.mark.parametrize("after_publish", [False, True], ids=["before-selector-mv", "after-selector-fsync"])
def test_deployer_sigkill_at_selector_boundary_preserves_or_keeps_selected_release(
    tmp_path: Path, after_publish: bool
) -> None:
    harness, previous, storage, raw = _kill_deployer_at_selector(tmp_path, after_publish=after_publish)
    if after_publish:
        assert harness.current_link.resolve(strict=True) != previous
        assert "migration" not in harness.log.read_text(encoding="utf-8").lower()
    else:
        assert harness.current_link.resolve(strict=True) == previous
    with closing(sqlite3.connect(storage.collector.state_db)) as connection:
        assert connection.execute("PRAGMA user_version").fetchone() == (1,)
        assert connection.execute("SELECT 1 FROM state WHERE id='internal_audio_cutover'").fetchone() is None
    assert next(storage.draft.iterdir()).joinpath("records.bin").read_bytes() == raw
    _assert_runtime_exit_78(storage.root / "config.toml")
    resumed = _run_deployer(harness)
    assert resumed.returncode == 0, resumed.stderr
    assert harness.current_link.resolve(strict=True) != previous


@pytest.mark.parametrize(
    ("checkpoint", "expected_phase"),
    [
        ("building", "BUILDING"),
        ("partial_candidate", "BUILDING"),
        ("prepared_candidate", "BUILDING"),
        ("activated_prepared", "PREPARED"),
        ("cleanup_pending", "CLEANUP_PENDING"),
        ("delete_1", "CLEANUP_PENDING"),
        ("delete_2", "CLEANUP_PENDING"),
        ("delete_3", "CLEANUP_PENDING"),
        ("handoff", "CLEANUP_PENDING"),
        ("complete", "COMPLETE"),
    ],
)
def test_sigkill_crash_matrix_resumes_without_losing_audio(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, checkpoint: str, expected_phase: str
) -> None:
    layout = _layout(tmp_path)
    release = _release(tmp_path)
    _v1(layout)
    draft, raw = _draft(layout, count=4096 if checkpoint == "partial_candidate" else 257)
    sentinel = tmp_path / "unowned"
    sentinel.write_bytes(b"unowned")
    detail, receipt = _run_to_migration_killpoint(layout, release, checkpoint, expected_phase)
    if checkpoint == "partial_candidate":
        assert isinstance(detail, str)
        assert Path(detail).exists() and layout.collector.state_db.exists() and draft.exists()
    if checkpoint == "prepared_candidate":
        assert tuple(layout.collector.root.glob(".collector-state.sqlite3.migrating.*")) and draft.exists()
    if checkpoint == "activated_prepared":
        assert draft.exists()
    if checkpoint == "handoff":
        assert not tuple(
            path
            for root in (layout.collector.attempts, layout.collector.quarantine, layout.draft)
            for path in root.rglob("*")
        )
    if checkpoint.startswith("delete_"):
        deleted = int(checkpoint.removeprefix("delete_"))
        assert (draft / "records.bin").exists() is (deleted < 1)
        assert (draft / "manifest.json").exists() is (deleted < 2)
        assert draft.exists() is (deleted < 3)
    rebuilt: list[tuple[str, str]] = []
    if receipt is not None:
        original = migration._backup_and_convert

        def record_candidate(
            source: sqlite3.Connection, candidate: Path, cutover_receipt: dict[str, object], inventory: object
        ) -> None:
            rebuilt.append((candidate.name, str(cutover_receipt["uuid"])))
            original(source, candidate, cutover_receipt, inventory)  # type: ignore[arg-type]

        monkeypatch.setattr(migration, "_backup_and_convert", record_candidate)
    if expected_phase != "COMPLETE":
        migrate_internal_audio(layout, release_metadata=release)
    if receipt is not None:
        assert rebuilt == [(str(receipt["candidate"]), str(receipt["uuid"]))]
    assert not draft.exists()
    _assert_final(layout, raw, 4096 if checkpoint == "partial_candidate" else 257, sentinel)
    open_runtime_state(layout, read_only=True)
    migrate_internal_audio(layout, release_metadata=release)


@pytest.mark.parametrize("deleted", [1, 2, 3, 4])
def test_sigkill_after_each_attempt_source_deletion_resumes_exact_remaining_subset(
    tmp_path: Path, deleted: int
) -> None:
    layout = _layout(tmp_path)
    release = _release(tmp_path)
    _v1(layout)
    attempt, raw = _attempt(layout)
    sentinel = tmp_path / "unowned"
    sentinel.write_bytes(b"unowned")
    detail, _receipt = _run_to_migration_killpoint(layout, release, f"delete_{deleted}", "CLEANUP_PENDING")
    assert detail == deleted
    assert (attempt / "records.bin").exists() is (deleted < 1)
    assert (attempt / "checkpoint.json").exists() is (deleted < 2)
    assert (attempt / "attempt.json").exists() is (deleted < 3)
    assert attempt.exists() is (deleted < 4)
    result = migrate_internal_audio(layout, release_metadata=release)
    assert result == {"imported_sources": 1, "removed_files": max(0, 3 - deleted)}
    _assert_attempt_final(layout, raw, sentinel)
    open_runtime_state(layout, read_only=True)
