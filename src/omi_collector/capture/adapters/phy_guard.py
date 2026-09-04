"""Crash-safe, adapter-scoped guard for temporarily disabling LE 2M PHYs."""

from __future__ import annotations

import asyncio
import fcntl
import json
import os
import re
import stat
import tempfile
from collections.abc import Awaitable, Callable
from contextlib import AbstractAsyncContextManager, suppress
from dataclasses import dataclass
from pathlib import Path
from typing import cast

from ...config import DEFAULT_CONFIG

_ADAPTER = re.compile(r"hci[0-9]+\Z")
_PHY_TOKEN = re.compile(r"[A-Z0-9]+\Z")
_SELECTED_PHYS = re.compile(r"^\s*selected\s+phys?\s*:\s*(?P<tokens>.+?)\s*$", re.IGNORECASE | re.MULTILINE)
_REMOVED_PHYS = frozenset(("LE2MTX", "LE2MRX"))
_REQUIRED_PHYS = frozenset(("LE1MTX", "LE1MRX", "LE2MTX", "LE2MRX"))
_MARKER_VERSION = 1
_PHY_CONFIG = DEFAULT_CONFIG.phy
_BLUETOOTHCTL_TIMEOUT_SECONDS = _PHY_CONFIG.bluetoothctl_timeout_seconds
_REAP_TIMEOUT_SECONDS = _PHY_CONFIG.reap_timeout_seconds
_BLUETOOTHCTL = (
    "sudo",
    "-n",
    "/usr/bin/bluetoothctl",
    "--timeout",
    f"{_BLUETOOTHCTL_TIMEOUT_SECONDS:g}",
    "mgmt.phy",
)


class PhyGuardError(RuntimeError):
    """Base error for an unsafe or unsuccessful PHY guard operation."""


class PhyGuardBusyError(PhyGuardError):
    """Raised when another process owns the adapter's PHY guard lock."""


class PhyCommandError(PhyGuardError):
    """Raised when bluetoothctl reports a command failure."""


class PhyStateError(PhyGuardError):
    """Raised when the controller state cannot be safely established."""


@dataclass(frozen=True, slots=True)
class CommandResult:
    """The minimal command result contract required by :class:`ScopedPhyGuard`."""

    returncode: int
    stdout: str
    stderr: str = ""


CommandRunner = Callable[[tuple[str, ...]], Awaitable[CommandResult]]


@dataclass(frozen=True, slots=True)
class PhyGuardPaths:
    """Explicit storage paths for one adapter's lock and recovery marker."""

    lock_path: Path
    marker_path: Path

    def __post_init__(self) -> None:
        _validate_storage_path(self.lock_path, "lock")
        _validate_storage_path(self.marker_path, "marker")
        if self.lock_path == self.marker_path:
            raise ValueError("PHY guard lock and marker paths must differ")


def default_phy_guard_paths(adapter: str, *, state_home: Path | None = None) -> PhyGuardPaths:
    """Return user-state paths for *adapter* without touching the filesystem."""
    _validate_adapter(adapter)
    base = state_home or Path(os.environ.get("XDG_STATE_HOME", Path.home() / ".local" / "state"))
    directory = base / "omi-collector"
    return PhyGuardPaths(
        lock_path=directory / f"{adapter}.phy.lock",
        marker_path=directory / f"{adapter}.phy-recovery.json",
    )


async def run_bluetoothctl(argv: tuple[str, ...]) -> CommandResult:
    """Run an exact prebuilt bluetoothctl argv; callers normally inject a fake."""
    process = await asyncio.create_subprocess_exec(
        *argv,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        stdout, stderr = await process.communicate()
    except BaseException:
        await _terminate_and_reap(process)
        raise
    return CommandResult(process.returncode or 0, stdout.decode(), stderr.decode())


async def _terminate_and_reap(process: asyncio.subprocess.Process) -> None:
    if process.returncode is not None:
        return
    _terminate(process)
    try:
        await asyncio.wait_for(process.wait(), timeout=_REAP_TIMEOUT_SECONDS)
    except TimeoutError:
        if process.returncode is None:
            _kill(process)
        await process.wait()


def _terminate(process: asyncio.subprocess.Process) -> None:
    with suppress(ProcessLookupError):
        process.terminate()


def _kill(process: asyncio.subprocess.Process) -> None:
    with suppress(ProcessLookupError):
        process.kill()


class ScopedPhyGuard(AbstractAsyncContextManager["ScopedPhyGuard"]):
    """Temporarily remove LE 2M PHYs and restore the exact prior selected set.

    This is controller-global while active.  The file lock serializes only
    cooperating users of this guard; it cannot isolate unrelated BlueZ clients.
    """

    def __init__(
        self,
        adapter: str,
        *,
        runner: CommandRunner = run_bluetoothctl,
        paths: PhyGuardPaths | None = None,
    ) -> None:
        _validate_adapter(adapter)
        self.adapter = adapter
        self._runner = runner
        self._paths = paths or default_phy_guard_paths(adapter)
        self._lock_fd: int | None = None
        self._snapshot: tuple[str, ...] | None = None
        self._owner: dict[str, int] | None = None

    async def recover(self) -> None:
        """Restore a stale marker, if any, while exclusively holding the lock."""
        self._acquire_lock()
        try:
            await self._recover_stale_marker()
        finally:
            self._release_lock()

    async def __aenter__(self) -> ScopedPhyGuard:
        self._acquire_lock()
        try:
            await self._recover_stale_marker()
            snapshot = await self._selected_phys()
            temporary = _temporary_phys(snapshot)
            self._snapshot = snapshot
            self._owner = _marker_owner()
            self._write_marker(snapshot, self._owner)
            await self._set_selected_phys(temporary)
        except BaseException:
            try:
                await self._restore_after_failed_enter()
            finally:
                self._release_lock()
            raise
        return self

    async def __aexit__(self, exc_type: object, exc: object, traceback: object) -> bool:
        del exc_type, exc, traceback
        try:
            await self._restore_and_clear_marker()
        finally:
            self._release_lock()
        return False

    def _acquire_lock(self) -> None:
        if self._lock_fd is not None:
            raise PhyStateError("PHY guard lock is already held by this instance")
        _prepare_storage_parent(self._paths.lock_path)
        fd = _open_regular_file(self._paths.lock_path, os.O_RDWR | os.O_CREAT)
        try:
            os.fchmod(fd, 0o600)
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            os.close(fd)
            raise PhyGuardBusyError(f"PHY guard for {self.adapter} is already active") from exc
        except BaseException:
            os.close(fd)
            raise
        self._lock_fd = fd

    def _release_lock(self) -> None:
        if self._lock_fd is None:
            return
        fcntl.flock(self._lock_fd, fcntl.LOCK_UN)
        os.close(self._lock_fd)
        self._lock_fd = None

    async def _recover_stale_marker(self) -> None:
        marker = self._read_marker()
        if marker is None:
            return
        snapshot, owner = _parse_marker(marker, self.adapter)
        if _owner_is_live(owner):
            raise PhyStateError("PHY recovery marker belongs to a live process")
        await self._set_selected_phys(snapshot)
        if await self._selected_phys() != snapshot:
            raise PhyStateError("PHY recovery restore verification failed")
        self._delete_marker(snapshot, owner)

    async def _restore_after_failed_enter(self) -> None:
        if self._snapshot is None or self._owner is None:
            return
        await self._restore_and_clear_marker()

    async def _restore_and_clear_marker(self) -> None:
        if self._snapshot is None or self._owner is None:
            raise PhyStateError("PHY guard has no recoverable snapshot")
        await self._set_selected_phys(self._snapshot)
        if await self._selected_phys() != self._snapshot:
            raise PhyStateError("PHY restore verification failed")
        self._delete_marker(self._snapshot, self._owner)

    async def _selected_phys(self) -> tuple[str, ...]:
        result = await self._run(_BLUETOOTHCTL)
        return parse_selected_phys(result.stdout)

    async def _set_selected_phys(self, tokens: tuple[str, ...]) -> None:
        if not tokens:
            raise PhyStateError("refusing to set an empty PHY selection")
        await self._run((*_BLUETOOTHCTL, *tokens))

    async def _run(self, argv: tuple[str, ...]) -> CommandResult:
        result = await self._runner(argv)
        if result.returncode != 0:
            raise PhyCommandError(f"bluetoothctl exited with status {result.returncode}")
        return result

    def _write_marker(self, snapshot: tuple[str, ...], owner: dict[str, int]) -> None:
        _prepare_storage_parent(self._paths.marker_path)
        if self._paths.marker_path.is_symlink():
            raise PhyStateError("PHY recovery marker is a symlink")
        payload = {
            "adapter": self.adapter,
            "owner": owner,
            "selected_phys": list(snapshot),
            "version": _MARKER_VERSION,
        }
        _atomic_json_write(self._paths.marker_path, payload)

    def _read_marker(self) -> dict[str, object] | None:
        path = self._paths.marker_path
        _prepare_storage_parent(path)
        try:
            fd = _open_regular_file(path, os.O_RDONLY)
        except FileNotFoundError:
            return None
        try:
            with os.fdopen(fd, encoding="utf-8") as stream:
                raw = cast(object, json.load(stream))
        except json.JSONDecodeError as exc:
            raise PhyStateError("PHY recovery marker is malformed") from exc
        if not isinstance(raw, dict):
            raise PhyStateError("PHY recovery marker is malformed")
        return raw

    def _delete_marker(self, snapshot: tuple[str, ...], owner: dict[str, int]) -> None:
        marker = self._read_marker()
        if marker is None:
            raise PhyStateError("PHY recovery marker disappeared before cleanup")
        if _parse_marker(marker, self.adapter) != (snapshot, owner):
            raise PhyStateError("PHY recovery marker changed before cleanup")
        self._paths.marker_path.unlink()
        _fsync_directory(self._paths.marker_path.parent)


def parse_selected_phys(output: str) -> tuple[str, ...]:
    """Extract ordered selected PHY tokens from one ``bluetoothctl mgmt.phy`` output."""
    lines = tuple(match.group("tokens") for match in _SELECTED_PHYS.finditer(output))
    if len(lines) != 1:
        raise PhyStateError("bluetoothctl output has no unambiguous selected PHYs line")
    tokens = tuple(lines[0].split())
    _validate_phys(tokens)
    return tokens


def _temporary_phys(snapshot: tuple[str, ...]) -> tuple[str, ...]:
    _validate_phys(snapshot)
    missing = _REQUIRED_PHYS.difference(snapshot)
    if missing:
        raise PhyStateError("refusing PHY guard without LE 1M and LE 2M TX/RX selections")
    temporary = tuple(token for token in snapshot if token not in _REMOVED_PHYS)
    if set(snapshot).difference(temporary) != _REMOVED_PHYS:
        raise PhyStateError("temporary PHY selection would change more than LE 2M")
    return temporary


def _validate_adapter(adapter: str) -> None:
    if not _ADAPTER.fullmatch(adapter):
        raise ValueError("adapter must be an hci<number> name")


def _validate_storage_path(path: Path, label: str) -> None:
    if not path.is_absolute() or ".." in path.parts or path.name in {"", ".", ".."}:
        raise ValueError(f"PHY guard {label} path must be an absolute concrete file path")


def _validate_phys(tokens: tuple[str, ...]) -> None:
    if not tokens or any(_PHY_TOKEN.fullmatch(token) is None for token in tokens) or len(set(tokens)) != len(tokens):
        raise PhyStateError("selected PHYs are malformed")


def _prepare_storage_parent(path: Path) -> None:
    _validate_storage_path(path, "storage")
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    for ancestor in path.parents:
        if ancestor.is_symlink():
            raise PhyStateError("PHY guard storage parent is a symlink")


def _open_regular_file(path: Path, flags: int) -> int:
    if path.is_symlink():
        raise PhyStateError("PHY guard storage file is a symlink")
    fd = os.open(path, flags | os.O_CLOEXEC | os.O_NOFOLLOW, 0o600)
    if not stat.S_ISREG(os.fstat(fd).st_mode):
        os.close(fd)
        raise PhyStateError("PHY guard storage file is not regular")
    return fd


def _atomic_json_write(path: Path, payload: dict[str, object]) -> None:
    fd, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    temporary = Path(temporary_name)
    try:
        os.fchmod(fd, 0o600)
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            json.dump(payload, stream, separators=(",", ":"), sort_keys=True)
            stream.flush()
            os.fsync(stream.fileno())
        temporary.replace(path)
        _fsync_directory(path.parent)
    finally:
        if temporary.exists():
            temporary.unlink()


def _fsync_directory(directory: Path) -> None:
    fd = os.open(directory, os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _marker_owner() -> dict[str, int]:
    pid = os.getpid()
    start_time = _process_start_time(pid)
    if start_time is None:
        raise PhyStateError("cannot determine current process start time")
    return {"pid": pid, "start_time": start_time}


def _owner_is_live(owner: dict[str, int]) -> bool:
    return _process_start_time(owner["pid"]) == owner["start_time"]


def _process_start_time(pid: int) -> int | None:
    try:
        stat_line = Path(f"/proc/{pid}/stat").read_text(encoding="utf-8")
    except FileNotFoundError:
        return None
    try:
        return int(stat_line.rsplit(") ", maxsplit=1)[1].split()[19])
    except IndexError, ValueError:
        return None


def _parse_marker(marker: dict[str, object], adapter: str) -> tuple[tuple[str, ...], dict[str, int]]:
    selected = marker.get("selected_phys")
    owner = marker.get("owner")
    if (
        marker.get("version") != _MARKER_VERSION
        or marker.get("adapter") != adapter
        or not isinstance(selected, list)
        or not all(isinstance(token, str) for token in selected)
        or not isinstance(owner, dict)
    ):
        raise PhyStateError("PHY recovery marker is malformed")
    snapshot = tuple(selected)
    _validate_phys(snapshot)
    pid = owner.get("pid")
    start_time = owner.get("start_time")
    if not isinstance(pid, int) or pid <= 0 or not isinstance(start_time, int) or start_time < 0:
        raise PhyStateError("PHY recovery marker is malformed")
    return snapshot, {"pid": pid, "start_time": start_time}
