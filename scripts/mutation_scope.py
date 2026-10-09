"""Control the one durable Omi mutation audit job over a local socket."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import socket
import sqlite3
import stat
import struct
import subprocess
import sys
import threading
import time
from collections.abc import Callable, Sequence
from contextlib import suppress
from pathlib import Path
from typing import cast

AUDIT_ROOT = Path(os.environ.get("OMI_MUTATION_AUDIT_ROOT", "/srv/omi-collector-mutation-audit"))
JOBS_ROOT = AUDIT_ROOT / "jobs" / "omi-collector"
OWNER_FILE = "owner.json"
MAX_CONTROL_BYTES = 4096
PEERCRED_SIZE = struct.calcsize("3i")
PAUSE_ACK_SECONDS = 180.0
CONTROL_READ_SECONDS = 5.0
MAX_CONTROL_HANDLERS = 4
PRIVATE_DIRECTORY_MODE = 0o700
PRIVATE_SOCKET_MODE = 0o600
SOCKET_DIRECTORY = Path(f"/tmp/omi-mutation-control-{os.getuid()}")


class ScopeError(RuntimeError):
    """A safe, operator-facing audit control failure."""


def control_socket_path(job_root: Path) -> Path:
    """Return the short, deterministic local endpoint recorded in the job receipt."""
    digest = hashlib.sha256(str(job_root.resolve()).encode()).hexdigest()[:16]
    return SOCKET_DIRECTORY / f"{digest}.sock"


def _validate_socket_directory() -> None:
    SOCKET_DIRECTORY.mkdir(mode=PRIVATE_DIRECTORY_MODE, parents=True, exist_ok=True)
    details = SOCKET_DIRECTORY.lstat()
    if (
        not stat.S_ISDIR(details.st_mode)
        or details.st_uid != os.getuid()
        or stat.S_IMODE(details.st_mode) != PRIVATE_DIRECTORY_MODE
    ):
        raise ScopeError("mutation control directory is not private to the current user")


def _socket_identity(path: Path) -> tuple[int, int] | None:
    try:
        details = path.lstat()
    except FileNotFoundError:
        return None
    if (
        not stat.S_ISSOCK(details.st_mode)
        or details.st_uid != os.getuid()
        or stat.S_IMODE(details.st_mode) != PRIVATE_SOCKET_MODE
    ):
        raise ScopeError("mutation control endpoint is not a private socket owned by this user")
    return details.st_dev, details.st_ino


def remove_stale_socket(path: Path) -> None:
    """Remove only a same-user stale socket, after proving it refuses connections."""
    _validate_socket_directory()
    identity = _socket_identity(path)
    if identity is None:
        return
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as probe:
        probe.settimeout(1.0)
        try:
            probe.connect(str(path))
        except ConnectionRefusedError:
            pass
        except OSError as exc:
            raise ScopeError(f"cannot prove mutation socket is stale: {exc}") from exc
        else:
            raise ScopeError("mutation control socket already has a live owner")
    if _socket_identity(path) != identity:
        raise ScopeError("mutation control socket changed during stale-endpoint check")
    path.unlink()


def _read_owner(job_root: Path) -> dict[str, object]:
    owner_path = job_root / OWNER_FILE
    if not owner_path.is_file():
        raise ScopeError(f"missing mutation job receipt: {owner_path}")
    value = cast(object, json.loads(owner_path.read_text(encoding="utf-8")))
    if not isinstance(value, dict) or value.get("schema") != 1:
        raise ScopeError("mutation job receipt has an unsupported shape")
    return cast(dict[str, object], value)


def discover_jobs(audit_root: Path = AUDIT_ROOT) -> list[Path]:
    jobs_root = audit_root / "jobs" / "omi-collector"
    if not jobs_root.is_dir():
        return []
    return sorted(path.parent for path in jobs_root.glob(f"*/{OWNER_FILE}") if path.is_file())


def select_job(audit_root: Path = AUDIT_ROOT, *, states: set[str] | None = None) -> Path:
    candidates = discover_jobs(audit_root)
    if states is not None:
        candidates = [path for path in candidates if _read_owner(path).get("state") in states]
    if len(candidates) != 1:
        state_text = ", ".join(sorted(states)) if states else "any"
        raise ScopeError(f"expected one {state_text} mutation job; found {len(candidates)}")
    return candidates[0]


class OwnerControlServer:
    """Serve status and checkpoint-stop requests inside the owner process."""

    def __init__(
        self,
        socket_path: Path,
        run_token: str,
        state_reader: Callable[[], dict[str, object]],
        pause_requester: Callable[[], bool],
    ) -> None:
        self.socket_path = socket_path
        self.run_token = run_token
        self.state_reader = state_reader
        self.pause_requester = pause_requester
        self.stop_event = threading.Event()
        self.listener: socket.socket | None = None
        self.thread: threading.Thread | None = None
        self.socket_identity: tuple[int, int] | None = None
        self._lock = threading.Lock()
        self._changed = threading.Condition(self._lock)
        self._connections: dict[socket.socket, str] = {}
        self._handlers: dict[socket.socket, threading.Thread] = {}
        self._handler_slots = threading.BoundedSemaphore(MAX_CONTROL_HANDLERS)
        self._lifecycle = "stopped"

    def start(self) -> None:
        _validate_socket_directory()
        if self.socket_path.exists():
            raise ScopeError("mutation control socket already exists; refusing to replace another owner's endpoint")
        self.listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.listener.bind(str(self.socket_path))
        self.socket_path.chmod(PRIVATE_SOCKET_MODE)
        details = self.socket_path.lstat()
        self.socket_identity = (details.st_dev, details.st_ino)
        self.listener.listen(8)
        self.listener.settimeout(0.2)
        self._lifecycle = "accepting"
        self.thread = threading.Thread(target=self._serve, name="mutation-control", daemon=True)
        self.thread.start()

    def stop(self) -> None:
        deadline = time.monotonic() + PAUSE_ACK_SECONDS
        self.stop_event.set()
        with self._changed:
            self._lifecycle = "stopping"
            for connection, state in self._connections.items():
                if state == "reading":
                    self._connections[connection] = "stopping"
            self._changed.notify_all()
        try:
            close_error: OSError | None = None
            if self.listener is not None:
                try:
                    self.listener.close()
                except OSError as exc:
                    close_error = exc
            try:
                self._join_control_workers(deadline)
            except ScopeError:
                if close_error is None:
                    raise
            if close_error is not None:
                raise ScopeError(f"could not close mutation control listener: {close_error}") from close_error
        finally:
            if self.socket_identity is not None and _socket_identity(self.socket_path) == self.socket_identity:
                self.socket_path.unlink()

    def _join_control_workers(self, deadline: float) -> None:
        if self.thread is not None:
            self.thread.join(timeout=max(0.0, deadline - time.monotonic()))
        with self._lock:
            connections = tuple(self._connections.items())
            handlers = tuple(self._handlers.values())
        for connection, state in connections:
            if state == "stopping":
                self._shutdown_connection(connection)
        workers = ([self.thread] if self.thread is not None else []) + list(handlers)
        for worker in workers:
            worker.join(timeout=max(0.0, deadline - time.monotonic()))
        survivors = [worker.name for worker in workers if worker.is_alive()]
        if survivors:
            for connection, _state in connections:
                self._shutdown_connection(connection)
            raise ScopeError(f"mutation control threads did not stop before deadline: {', '.join(survivors)}")

    @staticmethod
    def _shutdown_connection(connection: socket.socket) -> None:
        with suppress(OSError):
            connection.shutdown(socket.SHUT_RDWR)
        connection.close()

    def _serve(self) -> None:
        while not self.stop_event.is_set():
            if self.listener is None:
                return
            try:
                connection, _address = cast(tuple[socket.socket, str], self.listener.accept())
            except TimeoutError:
                continue
            except OSError:
                if self.stop_event.is_set():
                    return
                continue
            with self._changed:
                if self._lifecycle != "accepting" or not self._handler_slots.acquire(blocking=False):
                    connection.close()
                    continue
                for accepted, previous in tuple(self._handlers.items()):
                    if not previous.is_alive():
                        previous.join()
                        self._handlers.pop(accepted, None)
                self._connections[connection] = "reading"
                handler = threading.Thread(
                    target=self._handle,
                    args=(connection,),
                    name="mutation-control-handler",
                    daemon=True,
                )
                self._handlers[connection] = handler
                self._changed.notify_all()
                handler.start()

    def _handle(self, connection: socket.socket) -> None:
        try:
            peer_pid, peer_uid, _peer_gid = cast(
                tuple[int, int, int],
                struct.unpack("3i", connection.getsockopt(socket.SOL_SOCKET, socket.SO_PEERCRED, PEERCRED_SIZE)),
            )
            if peer_uid != os.getuid():
                raise ScopeError("control peer UID does not match the audit owner")
            request = _receive_request(connection, time.monotonic() + CONTROL_READ_SECONDS)
            if request.get("token") != self.run_token:
                raise ScopeError("control run token is stale or invalid")
            action = request.get("action")
            with self._changed:
                if self._lifecycle == "stopping":
                    raise ScopeError("mutation control server is stopping")
                self._connections[connection] = "dispatching"
                self._changed.notify_all()
            if action == "status":
                state = self.state_reader()
                response = {
                    "ok": True,
                    "state": state.get("state"),
                    "commit": state.get("commit"),
                    "started_at": state.get("started_at"),
                    "owner_pid": os.getpid(),
                }
            elif action == "pause":
                acknowledged = self.pause_requester()
                if not acknowledged:
                    raise ScopeError("owner could not verify a safe checkpoint stop")
                response = {
                    "ok": True,
                    "state": "paused",
                    "checkpoint_verified": True,
                    "owner_pid": os.getpid(),
                }
            else:
                raise ScopeError("control action must be status or pause")
            response["peer_pid"] = peer_pid
        except TimeoutError as exc:
            response = {"ok": False, "error": f"control request read timed out: {exc}"}
        except (ScopeError, OSError, ValueError, json.JSONDecodeError, struct.error) as exc:
            response = {"ok": False, "error": str(exc)}
        try:
            with suppress(OSError):
                connection.sendall((json.dumps(response, sort_keys=True) + "\n").encode("utf-8"))
        finally:
            connection.close()
            with self._changed:
                self._connections.pop(connection, None)
                self._handler_slots.release()
                self._changed.notify_all()


def _receive_request(connection: socket.socket, deadline: float) -> dict[str, object]:
    data = bytearray()
    while b"\n" not in data and len(data) < MAX_CONTROL_BYTES:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError("control request deadline expired")
        connection.settimeout(remaining)
        chunk = connection.recv(min(1024, MAX_CONTROL_BYTES - len(data)))
        if not chunk:
            break
        data.extend(chunk)
    if len(data) >= MAX_CONTROL_BYTES or b"\n" not in data:
        raise ScopeError("control request is missing a bounded newline terminator")
    value = cast(object, json.loads(bytes(data).splitlines()[0]))
    if not isinstance(value, dict):
        raise ScopeError("control request must be a JSON object")
    return cast(dict[str, object], value)


def send_control(job_root: Path, action: str) -> dict[str, object]:
    owner = _read_owner(job_root)
    token, state = owner.get("run_token"), owner.get("state")
    if not isinstance(token, str) or not token:
        raise ScopeError("mutation job receipt has no run token")
    if state not in {"running", "pausing"}:
        if action == "status":
            return {"ok": True, "state": state, "job_root": str(job_root)}
        raise ScopeError(f"cannot {action} mutation job in state {state}")
    expected_path = control_socket_path(job_root)
    recorded_path = owner.get("control_socket")
    if recorded_path != str(expected_path):
        raise ScopeError("mutation owner receipt has an invalid control endpoint")
    socket_path = expected_path
    if _socket_identity(socket_path) is None:
        raise ScopeError("mutation owner socket is absent; refusing unverified control")
    request = {"action": action, "token": token}
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
        client.settimeout(PAUSE_ACK_SECONDS if action == "pause" else 5.0)
        try:
            client.connect(str(socket_path))
            client.sendall((json.dumps(request, sort_keys=True) + "\n").encode("utf-8"))
            with client.makefile("rb") as response_stream:
                value = cast(object, json.loads(response_stream.readline(MAX_CONTROL_BYTES)))
        except (ConnectionError, OSError, TimeoutError, json.JSONDecodeError) as exc:
            raise ScopeError(f"mutation owner control request failed: {exc}") from exc
    if not isinstance(value, dict):
        raise ScopeError("mutation owner returned an invalid control response")
    response = cast(dict[str, object], value)
    if response.get("ok") is not True:
        raise ScopeError(str(response.get("error", "mutation owner rejected the request")))
    return response


def _resume() -> int:
    current = Path.cwd()
    result = subprocess.run(["just", "mutation", "resume"], cwd=current, check=False)
    return result.returncode


def _terminal_status(job_root: Path, owner: dict[str, object]) -> dict[str, object]:
    campaign = owner.get("campaign")
    campaign_state = campaign.get("state") if isinstance(campaign, dict) else None
    campaign_exit_status = campaign.get("exit_status") if isinstance(campaign, dict) else None
    campaign_log = campaign.get("log") if isinstance(campaign, dict) else None
    log = str(job_root / "checkout" / campaign_log) if isinstance(campaign_log, str) else None
    results_db = job_root / "checkout" / ".gremlins_cache" / "results.db"
    cached_result_rows: int | None = None
    if results_db.is_file():
        connection = sqlite3.connect(results_db.resolve().as_uri() + "?mode=ro", uri=True, timeout=5.0)
        try:
            row = cast(tuple[object, ...] | None, connection.execute("SELECT COUNT(*) FROM results").fetchone())
        except sqlite3.Error as exc:
            raise ScopeError(f"cannot read mutation cache row count: {exc}") from exc
        finally:
            connection.close()
        if row is None or not isinstance(row[0], int) or isinstance(row[0], bool):
            raise ScopeError("mutation cache row count query returned no integer result")
        cached_result_rows = row[0]
    return {
        "ok": True,
        "state": owner.get("state"),
        "job_root": str(job_root),
        "commit": owner.get("commit"),
        "started_at": owner.get("started_at"),
        "ended_at": owner.get("ended_at"),
        "exit_status": owner.get("exit_status"),
        "cleanup_verified": owner.get("cleanup_verified"),
        "checkpoint_verified": owner.get("checkpoint_verified"),
        "error": owner.get("error"),
        "postflight_error": owner.get("postflight_error"),
        "log": log,
        "campaign_state": campaign_state,
        "campaign_exit_status": campaign_exit_status,
        "cache": owner.get("cache"),
        "cached_result_rows": cached_result_rows,
    }


def _status(audit_root: Path | None = None) -> dict[str, object]:
    root = AUDIT_ROOT if audit_root is None else audit_root
    owners = [(job_root, _read_owner(job_root)) for job_root in discover_jobs(root)]
    active = [(job_root, owner) for job_root, owner in owners if owner.get("state") in {"running", "pausing"}]
    if len(active) > 1:
        raise ScopeError(f"expected at most one active mutation job; found {len(active)}")
    if active:
        return send_control(active[0][0], "status")
    terminal = [(job_root, owner) for job_root, owner in owners if owner.get("state") not in {"running", "pausing"}]
    if not terminal:
        raise ScopeError("no active or terminal mutation job receipt found")
    job_root, owner = max(
        terminal,
        key=lambda entry: (
            entry[1].get("created_at") if isinstance(entry[1].get("created_at"), str) else "",
            entry[0].name,
        ),
    )
    if owner.get("state") == "preparing":
        raise ScopeError("latest mutation job is preparing; no terminal receipt is available yet")
    return _terminal_status(job_root, owner)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Control the durable Omi mutation audit owner.")
    parser.add_argument("action", choices=("status", "pause", "resume"))
    args = parser.parse_args(argv)
    action = cast(str, args.action)
    try:
        if action == "resume":
            return _resume()
        if action == "status":
            response = _status()
        else:
            job_root = select_job(states={"running", "pausing"})
            response = send_control(job_root, action)
        print(json.dumps(response, sort_keys=True))
    except (ScopeError, OSError, sqlite3.Error, json.JSONDecodeError, subprocess.SubprocessError) as exc:
        print(f"mutation control: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
