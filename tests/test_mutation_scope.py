from __future__ import annotations

import errno
import fcntl
import json
import os
import signal
import socket
import sqlite3
import subprocess
import sys
import threading
import time
from collections.abc import Generator
from contextlib import closing, suppress
from pathlib import Path
from typing import cast

import pytest
from scripts import mutation_campaign, mutation_scope


@pytest.fixture
def control_job(tmp_path: Path) -> Generator[tuple[Path, mutation_scope.OwnerControlServer]]:
    job = tmp_path / "job"
    job.mkdir()
    token = "test-run-token"
    socket_path = mutation_scope.control_socket_path(job)
    server = mutation_scope.OwnerControlServer(
        socket_path,
        token,
        lambda: {"state": "running", "run_token": token},
        lambda: True,
    )
    server.start()
    (job / mutation_scope.OWNER_FILE).write_text(
        json.dumps({"schema": 1, "run_token": token, "state": "running", "control_socket": str(socket_path)}),
        encoding="utf-8",
    )
    try:
        yield job, server
    finally:
        server.stop()


def _select_test_job(monkeypatch: pytest.MonkeyPatch, job: Path) -> None:
    monkeypatch.setattr(mutation_scope, "select_job", lambda **_: job)


def _discover_test_job(monkeypatch: pytest.MonkeyPatch, job: Path) -> None:
    monkeypatch.setattr(mutation_scope, "discover_jobs", lambda _audit_root: [job])


def _read_json_object(path: Path) -> dict[str, object]:
    value = cast(object, json.loads(path.read_text(encoding="utf-8")))
    if not isinstance(value, dict):
        raise AssertionError(f"expected JSON object in {path}")
    return cast(dict[str, object], value)


def _read_json_object_from_text(text: str) -> dict[str, object]:
    value = cast(object, json.loads(text))
    if not isinstance(value, dict):
        raise AssertionError("expected JSON object in command output")
    return cast(dict[str, object], value)


def _read_pid_map(path: Path) -> dict[str, int]:
    value = cast(object, json.loads(path.read_text(encoding="utf-8")))
    if not isinstance(value, dict) or any(
        not isinstance(key, str) or not isinstance(pid, int) or isinstance(pid, bool) for key, pid in value.items()
    ):
        raise AssertionError(f"expected string to integer PID map in {path}")
    return cast(dict[str, int], value)


def _without_outer_job_token() -> dict[str, str]:
    environment = os.environ.copy()
    environment.pop(mutation_campaign.JOB_TOKEN_ENV, None)
    return environment


class _RequestBytes:
    def __init__(self, payload: bytes, chunk_sizes: tuple[int, ...] = ()) -> None:
        self.payload = payload
        self.chunk_sizes = iter(chunk_sizes)
        self.consumed = 0

    def settimeout(self, _timeout: float | None) -> None:
        pass

    def recv(self, size: int) -> bytes:
        chunk_size = min(size, next(self.chunk_sizes, size))
        chunk = self.payload[:chunk_size]
        self.payload = self.payload[len(chunk) :]
        self.consumed += len(chunk)
        return chunk


def test_status_uses_the_owner_socket_and_returns_live_state(
    control_job: tuple[Path, mutation_scope.OwnerControlServer],
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    job, _server = control_job
    _discover_test_job(monkeypatch, job)

    assert mutation_scope.main(["status"]) == 0

    response = cast(dict[str, object], json.loads(capsys.readouterr().out))
    assert response["ok"] is True
    assert response["state"] == "running"
    owner_pid = response.get("owner_pid")
    assert isinstance(owner_pid, int)
    assert owner_pid > 0
    assert mutation_scope.control_socket_path(job).stat().st_mode & 0o777 == 0o600


def test_receive_request_rejects_a_newline_at_the_size_limit() -> None:
    request = b'{"action":"status"}'
    payload = request + b" " * (mutation_scope.MAX_CONTROL_BYTES - len(request) - 1) + b"\n"

    with pytest.raises(mutation_scope.ScopeError, match="bounded newline terminator"):
        mutation_scope._receive_request(_RequestBytes(payload), time.monotonic() + 1)  # type: ignore[arg-type]


def test_receive_request_does_not_overread_the_size_limit() -> None:
    payload = b"x" * (mutation_scope.MAX_CONTROL_BYTES + 1024)
    connection = _RequestBytes(payload, (1024, 1024, 1024, 1023, 1024))

    with pytest.raises(mutation_scope.ScopeError, match="bounded newline terminator"):
        mutation_scope._receive_request(connection, time.monotonic() + 1)  # type: ignore[arg-type]

    assert connection.consumed == mutation_scope.MAX_CONTROL_BYTES


def test_remove_stale_socket_refuses_to_unlink_a_replaced_endpoint(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    socket_directory = tmp_path / "control"
    socket_directory.mkdir(mode=0o700)
    socket_directory.chmod(0o700)
    monkeypatch.setattr(mutation_scope, "SOCKET_DIRECTORY", socket_directory)
    socket_path = socket_directory / "stale.sock"
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as stale_socket:
        stale_socket.bind(str(socket_path))
    socket_path.chmod(0o600)
    identity = mutation_scope._socket_identity(socket_path)
    assert identity is not None

    calls = 0

    def changed_identity(_path: Path) -> tuple[int, int]:
        nonlocal calls
        calls += 1
        return identity if calls == 1 else (identity[0], identity[1] + 1)

    monkeypatch.setattr(mutation_scope, "_socket_identity", changed_identity)

    with pytest.raises(mutation_scope.ScopeError, match="changed during stale-endpoint check"):
        mutation_scope.remove_stale_socket(socket_path)

    assert socket_path.exists()


def test_pause_reports_paused_only_after_owner_acknowledges_checkpoint_stop(
    control_job: tuple[Path, mutation_scope.OwnerControlServer],
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    job, server = control_job
    _select_test_job(monkeypatch, job)
    server.pause_requester = lambda: True

    assert mutation_scope.main(["pause"]) == 0

    response = cast(dict[str, object], json.loads(capsys.readouterr().out))
    assert response["state"] == "paused"


def test_pause_fails_closed_when_owner_cannot_confirm_cleanup(
    control_job: tuple[Path, mutation_scope.OwnerControlServer],
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _job, server = control_job
    server.pause_requester = lambda: False
    _select_test_job(monkeypatch, _job)

    assert mutation_scope.main(["pause"]) == 1

    assert "could not verify a safe checkpoint stop" in capsys.readouterr().err


def test_owner_control_shutdown_closes_silent_client_and_joins_handler(
    control_job: tuple[Path, mutation_scope.OwnerControlServer],
) -> None:
    _job, server = control_job
    client = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    client.connect(str(server.socket_path))
    with server._changed:
        assert server._changed.wait_for(lambda: "reading" in server._connections.values(), timeout=2)

    server.stop()

    assert client.recv(1) == b""
    assert not server._connections
    assert all(not handler.is_alive() for handler in server._handlers.values())
    client.close()


def test_owner_control_shutdown_joins_handlers_when_listener_close_raises(
    control_job: tuple[Path, mutation_scope.OwnerControlServer],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _job, server = control_job
    client = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    client.connect(str(server.socket_path))
    with server._changed:
        assert server._changed.wait_for(lambda: "reading" in server._connections.values(), timeout=2)
    assert server.listener is not None
    listener = server.listener

    class ListenerCloseFailure:
        def accept(self) -> tuple[socket.socket, str]:
            return listener.accept()

        def close(self) -> None:
            listener.close()
            raise OSError("injected listener close failure")

    monkeypatch.setattr(server, "listener", cast(socket.socket, ListenerCloseFailure()))
    with pytest.raises(mutation_scope.ScopeError, match="could not close mutation control listener"):
        server.stop()

    assert client.recv(1) == b""
    assert not server._connections
    assert all(not handler.is_alive() for handler in server._handlers.values())
    monkeypatch.setattr(server, "listener", listener)
    client.close()


def test_owner_control_shutdown_reports_handler_past_shared_deadline(
    control_job: tuple[Path, mutation_scope.OwnerControlServer],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _job, server = control_job
    monkeypatch.setattr(mutation_scope, "PAUSE_ACK_SECONDS", 1.0)
    callback_entered = threading.Event()
    release_callback = threading.Event()

    def blocked_pause() -> bool:
        callback_entered.set()
        release_callback.wait()
        return True

    server.pause_requester = blocked_pause
    client = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    client.connect(str(server.socket_path))
    client.sendall(b'{"action":"pause","token":"test-run-token"}\n')
    assert callback_entered.wait(timeout=2)

    try:
        with pytest.raises(mutation_scope.ScopeError, match="did not stop before deadline"):
            server.stop()
        assert not server.socket_path.exists()
        assert any(handler.is_alive() for handler in server._handlers.values())
    finally:
        release_callback.set()
        for handler in server._handlers.values():
            handler.join(timeout=2)
        client.close()


def test_owner_control_shutdown_allows_dispatching_pause_to_acknowledge(
    control_job: tuple[Path, mutation_scope.OwnerControlServer],
) -> None:
    _job, server = control_job
    callback_entered = threading.Event()
    release_callback = threading.Event()
    stop_entered = threading.Event()
    stop_errors: list[Exception] = []

    def blocked_pause() -> bool:
        callback_entered.set()
        release_callback.wait()
        return True

    def stop_server() -> None:
        stop_entered.set()
        try:
            server.stop()
        except mutation_scope.ScopeError as exc:
            stop_errors.append(exc)

    server.pause_requester = blocked_pause
    client = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    client.settimeout(2)
    client.connect(str(server.socket_path))
    client.sendall(b'{"action":"pause","token":"test-run-token"}\n')
    assert callback_entered.wait(timeout=2)
    stopper = threading.Thread(target=stop_server)
    stopper.start()
    assert stop_entered.wait(timeout=2)
    with server._changed:
        assert server._changed.wait_for(lambda: server._lifecycle == "stopping", timeout=2)
    release_callback.set()
    response = _read_json_object_from_text(client.recv(mutation_scope.MAX_CONTROL_BYTES).decode())
    stopper.join(timeout=2)
    client.close()

    assert response["ok"] is True
    assert response["state"] == "paused"
    assert not stop_errors
    assert not stopper.is_alive()


def test_owner_control_read_deadline_is_absolute_during_slow_drip(
    control_job: tuple[Path, mutation_scope.OwnerControlServer],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _job, server = control_job
    pause_calls: list[bool] = []
    server.pause_requester = lambda: pause_calls.append(True) or True
    monkeypatch.setattr(mutation_scope, "CONTROL_READ_SECONDS", 0.3)
    client = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    client.settimeout(2)
    client.connect(str(server.socket_path))
    client.sendall(b'{"token":')

    def drip() -> None:
        threading.Event().wait(0.2)
        with suppress(OSError):
            client.sendall(b'"test-run-token",')
        threading.Event().wait(0.2)
        with suppress(OSError):
            client.sendall(b'"action":"pause"}\n')

    dripper = threading.Thread(target=drip)
    dripper.start()
    response = client.recv(mutation_scope.MAX_CONTROL_BYTES)
    dripper.join(timeout=2)

    assert b'"ok": false' in response
    assert pause_calls == []
    client.close()


def test_owner_control_caps_concurrent_silent_clients(
    control_job: tuple[Path, mutation_scope.OwnerControlServer],
) -> None:
    _job, server = control_job
    clients: list[socket.socket] = []
    try:
        for _ in range(mutation_scope.MAX_CONTROL_HANDLERS + 3):
            client = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            client.settimeout(2)
            client.connect(str(server.socket_path))
            clients.append(client)
        with server._changed:
            assert server._changed.wait_for(
                lambda: len(server._connections) == mutation_scope.MAX_CONTROL_HANDLERS,
                timeout=2,
            )
        assert len(server._handlers) <= mutation_scope.MAX_CONTROL_HANDLERS
        assert all(client.recv(1) == b"" for client in clients[mutation_scope.MAX_CONTROL_HANDLERS :])
    finally:
        server.stop()
        for client in clients:
            client.close()


def test_stale_token_cannot_signal_an_unrelated_live_process(
    control_job: tuple[Path, mutation_scope.OwnerControlServer],
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    job, server = control_job
    pause_calls: list[bool] = []
    server.pause_requester = lambda: pause_calls.append(True) or True
    owner = mutation_scope._read_owner(job)
    owner["run_token"] = "stale-token"
    (job / mutation_scope.OWNER_FILE).write_text(json.dumps(owner), encoding="utf-8")
    _select_test_job(monkeypatch, job)
    sentinel = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(30)"])
    try:
        assert mutation_scope.main(["pause"]) == 1
        assert "stale or invalid" in capsys.readouterr().err
        assert sentinel.poll() is None
        assert pause_calls == []
    finally:
        sentinel.terminate()
        sentinel.wait(timeout=5)


def test_missing_owner_socket_never_signals_a_pid_from_the_receipt(
    control_job: tuple[Path, mutation_scope.OwnerControlServer],
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    job, server = control_job
    server.stop()
    owner = mutation_scope._read_owner(job)
    owner["pid"] = os.getpid()
    (job / mutation_scope.OWNER_FILE).write_text(json.dumps(owner), encoding="utf-8")
    _select_test_job(monkeypatch, job)
    sentinel = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(30)"])
    try:
        assert mutation_scope.main(["pause"]) == 1
        assert "socket is absent" in capsys.readouterr().err
        assert sentinel.poll() is None
    finally:
        sentinel.terminate()
        sentinel.wait(timeout=5)


def test_status_falls_back_to_latest_terminal_receipt_and_exposes_cache(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    audit_root = tmp_path / "audit"
    jobs_root = audit_root / "jobs" / "omi-collector"
    older_job = jobs_root / "20261001T120000Z-old"
    latest_job = jobs_root / "20261002T120000Z-latest"
    older_job.mkdir(parents=True)
    latest_job.mkdir()
    (older_job / mutation_scope.OWNER_FILE).write_text(
        json.dumps(
            {
                "schema": 1,
                "run_token": "old-terminal-token",
                "state": "preparing",
                "created_at": "2026-10-01T12:00:00+00:00",
                "exit_status": 0,
            }
        ),
        encoding="utf-8",
    )
    campaign = {
        "state": "complete_unresolved",
        "counts": {"survived": 2},
        "log": "mutation.log",
        "exit_status": 3,
    }
    cache_receipt = {"results_db": "checkout/.gremlins_cache/results.db", "quick_check": "ok"}
    (latest_job / mutation_scope.OWNER_FILE).write_text(
        json.dumps(
            {
                "schema": 1,
                "run_token": "latest-terminal-token",
                "state": "complete_unresolved",
                "commit": "abc123",
                "created_at": "2026-10-02T12:00:00+00:00",
                "started_at": "2026-10-02T12:01:00+00:00",
                "ended_at": "2026-10-02T12:03:00+00:00",
                "exit_status": 3,
                "cleanup_verified": True,
                "checkpoint_verified": False,
                "campaign": campaign,
                "cache": cache_receipt,
                "error": "native unresolved outcomes remain",
                "postflight_error": None,
            }
        ),
        encoding="utf-8",
    )
    cache_path = latest_job / "checkout" / ".gremlins_cache" / "results.db"
    cache_path.parent.mkdir(parents=True)
    with closing(sqlite3.connect(cache_path)) as database, database:
        database.execute("CREATE TABLE results (cache_key TEXT PRIMARY KEY, result_json TEXT NOT NULL)")
        database.executemany(
            "INSERT INTO results VALUES (?, ?)",
            [("g-1", '{"status":"SURVIVED"}'), ("g-2", '{"status":"ZAPPED"}')],
        )
    monkeypatch.setattr(mutation_scope, "AUDIT_ROOT", audit_root)

    assert mutation_scope.main(["status"]) == 0

    response = _read_json_object_from_text(capsys.readouterr().out)
    assert response["ok"] is True
    assert response["state"] == "complete_unresolved"
    assert response["job_root"] == str(latest_job)
    assert response["commit"] == "abc123"
    assert response["started_at"] == "2026-10-02T12:01:00+00:00"
    assert response["ended_at"] == "2026-10-02T12:03:00+00:00"
    assert response["exit_status"] == 3
    assert response["cleanup_verified"] is True
    assert response["checkpoint_verified"] is False
    assert response["campaign_state"] == campaign["state"]
    assert response["campaign_exit_status"] == 3
    assert response["log"] == str(latest_job / "checkout" / "mutation.log")
    assert response["cache"] == cache_receipt
    assert response["error"] == "native unresolved outcomes remain"
    assert response["postflight_error"] is None
    assert response["cached_result_rows"] == 2


def test_status_without_jobs_uses_an_isolated_audit_root(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    audit_root = tmp_path / "empty-audit"
    audit_root.mkdir()
    monkeypatch.setattr(mutation_scope, "AUDIT_ROOT", audit_root)

    assert mutation_scope.main(["status"]) == 1

    captured = capsys.readouterr()
    assert captured.out == ""
    assert "no active or terminal mutation job receipt found" in captured.err


def test_status_refuses_fallback_when_multiple_jobs_are_active(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    audit_root = tmp_path / "audit"
    jobs_root = audit_root / "jobs" / "omi-collector"
    first_job = jobs_root / "20261001T120000Z-first"
    second_job = jobs_root / "20261002T120000Z-second"
    first_job.mkdir(parents=True)
    second_job.mkdir()
    for job, token in ((first_job, "first-active"), (second_job, "second-active")):
        (job / mutation_scope.OWNER_FILE).write_text(
            json.dumps({"schema": 1, "run_token": token, "state": "running"}),
            encoding="utf-8",
        )
    monkeypatch.setattr(mutation_scope, "AUDIT_ROOT", audit_root)
    monkeypatch.setattr(
        mutation_scope,
        "send_control",
        lambda *_args, **_kwargs: pytest.fail("status must refuse before contacting either owner"),
    )

    assert mutation_scope.main(["status"]) == 1

    captured = capsys.readouterr()
    assert captured.out == ""
    assert captured.err


def test_resume_delegates_to_the_foreground_cache_backed_job(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[list[str], dict[str, object]]] = []

    def run(args: list[str], **kwargs: object) -> subprocess.CompletedProcess[str]:
        calls.append((args, kwargs))
        return subprocess.CompletedProcess(args, 17)

    monkeypatch.setattr(mutation_scope.subprocess, "run", run)

    assert mutation_scope.main(["resume"]) == 17
    assert calls == [
        (["just", "mutation", "resume"], {"cwd": Path.cwd(), "check": False}),
    ]


def _process_tree_runner(pids_path: Path) -> str:
    return f"""
import json, os, pathlib, signal, sqlite3, subprocess, sys, time
pathlib.Path('.gremlins_cache/campaign.json').write_text(json.dumps({{'state': 'running'}}))
pathlib.Path(os.environ['TMPDIR'], 'pytest_gremlins_sources.py').write_text('temporary')
db = sqlite3.connect('.gremlins_cache/results.db')
db.execute('CREATE TABLE results (cache_key TEXT PRIMARY KEY, result_json TEXT NOT NULL)')
db.execute("INSERT INTO results VALUES (?, ?)", ('completed', json.dumps({{'status': 'ZAPPED'}})))
db.commit()
db.close()
def cache_worker_exit(signum, frame):
    db = sqlite3.connect('.gremlins_cache/results.db')
    db.execute("INSERT OR REPLACE INTO results VALUES (?, ?)", ('in-flight', json.dumps({{'status': 'ERROR'}})))
    db.commit()
    db.close()
signal.signal(signal.SIGCHLD, cache_worker_exit)
child = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(60)'], start_new_session=True)
pids_path = pathlib.Path({str(pids_path)!r})
temporary_pids_path = pids_path.with_suffix('.tmp')
temporary_pids_path.write_text(json.dumps({{"parent": os.getpid(), 'child': child.pid}}), encoding='utf-8')
temporary_pids_path.replace(pids_path)
time.sleep(60)
"""


def _process_stopped(pid: int) -> bool:
    deadline = time.monotonic() + 2
    while time.monotonic() < deadline:
        try:
            stat = Path(f"/proc/{pid}/stat").read_text(encoding="ascii")
        except FileNotFoundError:
            return True
        if stat.rsplit(")", 1)[-1].split()[0] == "Z":
            return True
        time.sleep(0.01)
    return False


def test_pidfd_zombie_race_is_safe_only_for_the_same_process_identity(monkeypatch: pytest.MonkeyPatch) -> None:
    pid = 12345
    ticks = "42"
    monkeypatch.setattr(mutation_campaign, "_proc_identity", lambda _pid: (1, ticks, "Z"))

    with pytest.raises(ProcessLookupError) as zombie:
        mutation_campaign._open_pidfd(pid, ticks)
    assert zombie.value.errno == errno.ESRCH
    assert mutation_campaign._verified_process_exit_race(zombie.value, pid, ticks)

    monkeypatch.setattr(mutation_campaign, "_proc_identity", lambda _pid: (1, "43", "S"))
    with pytest.raises(ProcessLookupError) as reused:
        mutation_campaign._open_pidfd(pid, ticks)
    assert reused.value.errno is None
    assert not mutation_campaign._verified_process_exit_race(zombie.value, pid, ticks)

    monkeypatch.setattr(mutation_campaign, "_proc_identity", lambda _pid: (1, ticks, "S"))
    assert not mutation_campaign._verified_process_exit_race(zombie.value, pid, ticks)


def _ensure_pause_owner_stopped(owner: threading.Thread, job: Path, token: str) -> None:
    if not owner.is_alive():
        return
    with suppress(mutation_scope.ScopeError):
        mutation_scope.send_control(job, "pause")
    owner.join(timeout=5)
    if owner.is_alive():
        mutation_campaign._cleanup_token_processes(token)
        owner.join(timeout=5)
    assert not owner.is_alive(), "owned process-tree controller survived bounded cleanup"


def test_pause_stops_new_session_descendant_but_preserves_unrelated_process(
    tmp_path: Path,
) -> None:
    job = tmp_path / "job"
    job.mkdir()
    checkout = job / "checkout"
    (checkout / ".gremlins_cache").mkdir(parents=True)
    (job / mutation_scope.OWNER_FILE).write_text(
        json.dumps({"schema": 1, "run_token": "process-tree-run", "state": "preparing"}),
        encoding="utf-8",
    )
    pids_path = tmp_path / "pids.json"
    token = "process-tree-run"
    owner_result: list[int | Exception] = []

    def enter_owner() -> None:
        try:
            owner_result.append(
                mutation_campaign._enter_owner(
                    job,
                    "resume",
                    token,
                    command=[sys.executable, "-c", _process_tree_runner(pids_path), "--gremlins"],
                    environment={**_without_outer_job_token(), "OMI_MUTATION_PID_FILE": str(pids_path)},
                )
            )
        except (OSError, ValueError, subprocess.SubprocessError) as exc:
            owner_result.append(exc)

    owner = threading.Thread(
        target=enter_owner,
        daemon=True,
    )
    sentinel = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)"])
    owner.start()
    try:
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline and not pids_path.exists():
            time.sleep(0.01)
        assert pids_path.is_file()
        _pids = _read_pid_map(pids_path)
        assert (checkout / ".gremlins_cache" / "results.db").is_file()

        response = mutation_scope.send_control(job, "pause")

        assert response["state"] == "paused"
        assert response.get("checkpoint_verified") is True
        owner.join(timeout=5)
        assert not owner.is_alive()
        assert owner_result == [130]
        assert sentinel.poll() is None
        receipt = _read_json_object(job / mutation_scope.OWNER_FILE)
        assert receipt["state"] == "paused"
        assert receipt["checkpoint_verified"] is True
        assert not (job / "tmp").exists()
        with closing(sqlite3.connect(checkout / ".gremlins_cache" / "results.db")) as cache:
            assert cache.execute("PRAGMA quick_check").fetchone() == ("ok",)
            assert cache.execute("SELECT * FROM results ORDER BY cache_key").fetchall() == [
                ("completed", '{"status": "ZAPPED"}'),
            ]
        for pid in _pids.values():
            assert _process_stopped(pid), f"owned process {pid} survived checkpoint stop"
    finally:
        try:
            _ensure_pause_owner_stopped(owner, job, token)
        finally:
            sentinel.terminate()
            sentinel.wait(timeout=5)


def _owner_lock_available(job: Path) -> bool:
    with (job / mutation_campaign.OWNER_LOCK).open("a+b") as lock:
        try:
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return False
        fcntl.flock(lock.fileno(), fcntl.LOCK_UN)
        return True


def _start_signal_owner(job: Path, pids_path: Path, token: str) -> subprocess.Popen[str]:
    checkout = job / "checkout"
    (checkout / ".gremlins_cache").mkdir(parents=True)
    (job / mutation_scope.OWNER_FILE).write_text(
        json.dumps(
            {
                "schema": 1,
                "run_token": token,
                "state": "preparing",
                "control_socket": str(mutation_scope.control_socket_path(job)),
            }
        ),
        encoding="utf-8",
    )
    owner_code = f"""
import os, sys
from pathlib import Path
from scripts import mutation_campaign
job = Path({str(job)!r})
environment = {{**os.environ, 'OMI_MUTATION_PID_FILE': {str(pids_path)!r}}}
status = mutation_campaign._enter_owner(
    job,
    'resume',
    {token!r},
    command=[sys.executable, '-c', {_process_tree_runner(pids_path)!r}, '--gremlins'],
    environment=environment,
)
print(status, flush=True)
raise SystemExit(status)
"""
    return subprocess.Popen(
        [sys.executable, "-c", owner_code],
        cwd=Path.cwd(),
        env=_without_outer_job_token(),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        start_new_session=True,
    )


def _cleanup_signal_owner(owner: subprocess.Popen[str], sentinel: subprocess.Popen[bytes], token: str) -> None:
    if owner.poll() is None:
        owner.send_signal(signal.SIGTERM)
    try:
        owner.communicate(timeout=10)
    except subprocess.TimeoutExpired:
        owner.kill()
        owner.communicate(timeout=5)
        with suppress(OSError, ValueError):
            mutation_campaign._cleanup_token_processes(token)
    finally:
        if owner.stdout is not None:
            owner.stdout.close()
        if owner.stderr is not None:
            owner.stderr.close()
    sentinel.terminate()
    sentinel.wait(timeout=5)


def test_owner_sigterm_stops_process_tree_before_releasing_lock(tmp_path: Path) -> None:
    job = tmp_path / "signal-job"
    checkout = job / "checkout"
    token = "owner-sigterm-run"
    pids_path = tmp_path / "signal-pids.json"
    owner = _start_signal_owner(job, pids_path, token)
    sentinel = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)"])
    try:
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline and not pids_path.is_file() and owner.poll() is None:
            time.sleep(0.01)
        if not pids_path.is_file():
            owner_stderr = owner.stderr.read() if owner.stderr is not None else ""
            pytest.fail(owner_stderr or "owner child did not start")
        pids = _read_pid_map(pids_path)
        assert set(pids) == {"parent", "child"}
        assert not _owner_lock_available(job)
        assert (checkout / ".gremlins_cache" / "results.db").is_file()

        owner.send_signal(signal.SIGTERM)
        stdout, stderr = owner.communicate(timeout=15)

        assert owner.returncode == 143, f"stdout={stdout!r}, stderr={stderr!r}"
        assert stdout.strip() == "143"
        receipt = _read_json_object(job / mutation_scope.OWNER_FILE)
        assert receipt["state"] == "interrupted"
        assert receipt["exit_status"] == 143
        assert receipt["cleanup_verified"] is True
        assert receipt["checkpoint_verified"] is True
        assert not (job / "tmp").exists()
        campaign = _read_json_object(checkout / ".gremlins_cache" / "campaign.json")
        assert campaign["state"] == "interrupted"
        with closing(sqlite3.connect(checkout / ".gremlins_cache" / "results.db")) as cache:
            assert cache.execute("PRAGMA quick_check").fetchone() == ("ok",)
            assert cache.execute("SELECT * FROM results ORDER BY cache_key").fetchall() == [
                ("completed", '{"status": "ZAPPED"}'),
            ]
        for pid in pids.values():
            assert _process_stopped(pid), f"owned process {pid} survived SIGTERM cleanup"
        assert sentinel.poll() is None
        assert _owner_lock_available(job)
    finally:
        _cleanup_signal_owner(owner, sentinel, token)
