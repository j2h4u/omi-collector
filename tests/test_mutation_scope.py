from __future__ import annotations

import fcntl
import json
import os
import signal
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


def _read_json_object(path: Path) -> dict[str, object]:
    value = cast(object, json.loads(path.read_text(encoding="utf-8")))
    if not isinstance(value, dict):
        raise AssertionError(f"expected JSON object in {path}")
    return cast(dict[str, object], value)


def _read_pid_map(path: Path) -> dict[str, int]:
    value = cast(object, json.loads(path.read_text(encoding="utf-8")))
    if not isinstance(value, dict) or any(
        not isinstance(key, str) or not isinstance(pid, int) or isinstance(pid, bool) for key, pid in value.items()
    ):
        raise AssertionError(f"expected string to integer PID map in {path}")
    return cast(dict[str, int], value)


def test_status_uses_the_owner_socket_and_returns_live_state(
    control_job: tuple[Path, mutation_scope.OwnerControlServer],
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    job, _server = control_job
    _select_test_job(monkeypatch, job)

    assert mutation_scope.main(["status"]) == 0

    response = cast(dict[str, object], json.loads(capsys.readouterr().out))
    assert response["ok"] is True
    assert response["state"] == "running"
    owner_pid = response.get("owner_pid")
    assert isinstance(owner_pid, int)
    assert owner_pid > 0
    assert mutation_scope.control_socket_path(job).stat().st_mode & 0o777 == 0o600


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
db = sqlite3.connect('.gremlins_cache/results.db')
db.execute('CREATE TABLE outcomes (gremlin_id TEXT PRIMARY KEY, status TEXT NOT NULL)')
db.execute("INSERT INTO outcomes VALUES ('completed', 'ZAPPED')")
db.commit()
db.close()
def cache_worker_exit(signum, frame):
    db = sqlite3.connect('.gremlins_cache/results.db')
    db.execute("INSERT OR REPLACE INTO outcomes VALUES ('in-flight', 'ERROR')")
    db.commit()
    db.close()
signal.signal(signal.SIGCHLD, cache_worker_exit)
child = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(60)'], start_new_session=True)
pathlib.Path({str(pids_path)!r}).write_text(json.dumps({{"parent": os.getpid(), 'child': child.pid}}))
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


def test_pause_stops_new_session_descendant_but_preserves_unrelated_process(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
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
                    environment={**os.environ, "OMI_MUTATION_PID_FILE": str(pids_path)},
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
        monkeypatch.setattr(mutation_scope, "PAUSE_ACK_SECONDS", 5.0)

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
        with closing(sqlite3.connect(checkout / ".gremlins_cache" / "results.db")) as cache:
            assert cache.execute("PRAGMA quick_check").fetchone() == ("ok",)
            assert cache.execute("SELECT * FROM outcomes ORDER BY gremlin_id").fetchall() == [
                ("completed", "ZAPPED"),
            ]
        for pid in _pids.values():
            assert _process_stopped(pid), f"owned process {pid} survived checkpoint stop"
    finally:
        if owner.is_alive():
            with suppress(mutation_scope.ScopeError):
                mutation_scope.send_control(job, "pause")
            owner.join(timeout=5)
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


def test_owner_sigterm_stops_process_tree_before_releasing_lock(tmp_path: Path) -> None:
    job = tmp_path / "signal-job"
    checkout = job / "checkout"
    (checkout / ".gremlins_cache").mkdir(parents=True)
    token = "owner-sigterm-run"
    pids_path = tmp_path / "signal-pids.json"
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
    owner = subprocess.Popen(
        [sys.executable, "-c", owner_code],
        cwd=Path.cwd(),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        start_new_session=True,
    )
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
        campaign = _read_json_object(checkout / ".gremlins_cache" / "campaign.json")
        assert campaign["state"] == "interrupted"
        with closing(sqlite3.connect(checkout / ".gremlins_cache" / "results.db")) as cache:
            assert cache.execute("PRAGMA quick_check").fetchone() == ("ok",)
            assert cache.execute("SELECT * FROM outcomes ORDER BY gremlin_id").fetchall() == [
                ("completed", "ZAPPED"),
            ]
        for pid in pids.values():
            assert _process_stopped(pid), f"owned process {pid} survived SIGTERM cleanup"
        assert sentinel.poll() is None
        assert _owner_lock_available(job)
    finally:
        if owner.poll() is None:
            owner.send_signal(signal.SIGTERM)
            try:
                owner.wait(timeout=10)
            except subprocess.TimeoutExpired:
                owner.kill()
                owner.wait(timeout=5)
            with suppress(OSError, ValueError):
                mutation_campaign._cleanup_token_processes(token)
        sentinel.terminate()
        sentinel.wait(timeout=5)
