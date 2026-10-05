"""Control the one durable Omi mutation audit job over a local socket."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import socket
import stat
import struct
import subprocess
import sys
import threading
from collections.abc import Callable, Sequence
from pathlib import Path
from typing import cast

AUDIT_ROOT = Path(os.environ.get("OMI_MUTATION_AUDIT_ROOT", "/srv/omi-collector-mutation-audit"))
JOBS_ROOT = AUDIT_ROOT / "jobs" / "omi-collector"
OWNER_FILE = "owner.json"
MAX_CONTROL_BYTES = 4096
PEERCRED_SIZE = struct.calcsize("3i")
PAUSE_ACK_SECONDS = 180.0
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
        self.thread = threading.Thread(target=self._serve, name="mutation-control", daemon=True)
        self.thread.start()

    def stop(self) -> None:
        self.stop_event.set()
        if self.listener is not None:
            self.listener.close()
        if self.thread is not None:
            self.thread.join(timeout=2)
        if self.socket_identity is not None and _socket_identity(self.socket_path) == self.socket_identity:
            self.socket_path.unlink()

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
            threading.Thread(target=self._handle, args=(connection,), daemon=True).start()

    def _handle(self, connection: socket.socket) -> None:
        with connection:
            try:
                peer_pid, peer_uid, _peer_gid = cast(
                    tuple[int, int, int],
                    struct.unpack("3i", connection.getsockopt(socket.SOL_SOCKET, socket.SO_PEERCRED, PEERCRED_SIZE)),
                )
                if peer_uid != os.getuid():
                    raise ScopeError("control peer UID does not match the audit owner")
                request = _receive_request(connection)
                if request.get("token") != self.run_token:
                    raise ScopeError("control run token is stale or invalid")
                action = request.get("action")
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
            except (ScopeError, OSError, ValueError, json.JSONDecodeError, struct.error) as exc:
                response = {"ok": False, "error": str(exc)}
            connection.sendall((json.dumps(response, sort_keys=True) + "\n").encode("utf-8"))


def _receive_request(connection: socket.socket) -> dict[str, object]:
    data = bytearray()
    while b"\n" not in data and len(data) < MAX_CONTROL_BYTES:
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


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Control the durable Omi mutation audit owner.")
    parser.add_argument("action", choices=("status", "pause", "resume"))
    args = parser.parse_args(argv)
    action = cast(str, args.action)
    try:
        if action == "resume":
            return _resume()
        job_root = select_job(states={"running", "pausing"})
        response = send_control(job_root, action)
        print(json.dumps(response, sort_keys=True))
    except (ScopeError, OSError, json.JSONDecodeError, subprocess.SubprocessError) as exc:
        print(f"mutation control: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
