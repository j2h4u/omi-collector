"""Crash-safe, adapter-scoped guard for temporarily disabling LE 2M PHYs."""

from __future__ import annotations

import asyncio
import json
import os
import re
import stat
from collections.abc import Awaitable, Callable
from contextlib import AbstractAsyncContextManager, suppress
from dataclasses import dataclass
from pathlib import Path
from typing import cast

from ...config import DEFAULT_CONFIG
from .collector_guard import guard_is_held
from .collector_state_db import StateDB, StateKind

_ADAPTER = re.compile(r"hci[0-9]+\Z")
_PHY_TOKEN = re.compile(r"[A-Z0-9]+\Z")
_SELECTED_PHYS = re.compile(r"^\s*selected\s+phys?\s*:\s*(?P<tokens>.+?)\s*$", re.IGNORECASE | re.MULTILINE)
_REMOVED_PHYS = frozenset(("LE2MTX", "LE2MRX"))
_REQUIRED_PHYS = frozenset(("LE1MTX", "LE1MRX", "LE2MTX", "LE2MRX"))
_MARKER_VERSION = 2
_LEGACY_MARKER_VERSION = 1
_MAX_LEGACY_MARKER_BYTES = 4096
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


async def recover_legacy_phy_marker(
    path: Path,
    adapter: str,
    *,
    collector_lock: Path,
    runner: CommandRunner = run_bluetoothctl,
) -> bool:
    """Restore a pre-cutover PHY snapshot before deleting its old file marker."""
    if not guard_is_held(collector_lock):
        raise PhyGuardBusyError("Collector process guard is required for legacy PHY recovery")
    marker = read_legacy_phy_marker(path, adapter)
    if marker is None:
        return False
    snapshot, _owner = _parse_legacy_marker(marker, adapter)
    changed = await runner((*_BLUETOOTHCTL, *snapshot))
    if changed.returncode != 0:
        raise PhyCommandError(f"bluetoothctl exited with status {changed.returncode}")
    observed = await runner(_BLUETOOTHCTL)
    if observed.returncode != 0 or parse_selected_phys(observed.stdout) != snapshot:
        raise PhyStateError("legacy PHY recovery verification failed")
    if read_legacy_phy_marker(path, adapter) != marker:
        raise PhyStateError("legacy PHY recovery marker changed before cleanup")
    path.unlink()
    descriptor = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    return True


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

    The outer Collector process guard serializes all cooperating PHY users.
    """

    def __init__(
        self,
        adapter: str,
        *,
        state_db: StateDB,
        runner: CommandRunner = run_bluetoothctl,
    ) -> None:
        _validate_adapter(adapter)
        self.adapter = adapter
        self._runner = runner
        self._db = state_db
        self._snapshot: tuple[str, ...] | None = None

    async def recover(self) -> None:
        """Restore a stale marker while the outer Collector guard is held."""
        self._require_guard()
        await self._recover_stale_marker()

    async def __aenter__(self) -> ScopedPhyGuard:
        self._require_guard()
        try:
            await self._recover_stale_marker()
            snapshot = await self._selected_phys()
            temporary = _temporary_phys(snapshot)
            self._snapshot = snapshot
            self._write_marker(snapshot)
            await self._set_selected_phys(temporary)
        except BaseException:
            await self._restore_after_failed_enter()
            raise
        return self

    async def __aexit__(self, exc_type: object, exc: object, traceback: object) -> bool:
        del exc_type, exc, traceback
        await self._restore_and_clear_marker()
        return False

    def _require_guard(self) -> None:
        if not guard_is_held(self._db.path.parent / "collector.lock"):
            raise PhyGuardBusyError("Collector process guard is required for PHY changes")

    async def _recover_stale_marker(self) -> None:
        marker = self._read_marker()
        if marker is None:
            return
        snapshot = _parse_db_marker(marker, self.adapter)
        await self._set_selected_phys(snapshot)
        if await self._selected_phys() != snapshot:
            raise PhyStateError("PHY recovery restore verification failed")
        self._delete_marker(snapshot)

    async def _restore_after_failed_enter(self) -> None:
        if self._snapshot is None:
            return
        await self._restore_and_clear_marker()

    async def _restore_and_clear_marker(self) -> None:
        if self._snapshot is None:
            raise PhyStateError("PHY guard has no recoverable snapshot")
        await self._set_selected_phys(self._snapshot)
        if await self._selected_phys() != self._snapshot:
            raise PhyStateError("PHY restore verification failed")
        self._delete_marker(self._snapshot)

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

    def _write_marker(self, snapshot: tuple[str, ...]) -> None:
        payload = {
            "adapter": self.adapter,
            "selected_phys": list(snapshot),
            "version": _MARKER_VERSION,
        }
        if self._db.get(StateKind.PHY_RESTORATION, self.adapter) is not None:
            raise PhyStateError("PHY recovery marker already exists")
        self._db.put(StateKind.PHY_RESTORATION, self.adapter, payload)

    def _read_marker(self) -> dict[str, object] | None:
        return self._db.get(StateKind.PHY_RESTORATION, self.adapter)

    def _delete_marker(self, snapshot: tuple[str, ...]) -> None:
        marker = self._read_marker()
        if marker is None:
            raise PhyStateError("PHY recovery marker disappeared before cleanup")
        if _parse_db_marker(marker, self.adapter) != snapshot:
            raise PhyStateError("PHY recovery marker changed before cleanup")
        with self._db.transaction() as transaction:
            current = transaction.get(StateKind.PHY_RESTORATION, self.adapter)
            if current != marker:
                raise PhyStateError("PHY recovery marker changed before cleanup")
            transaction.delete(StateKind.PHY_RESTORATION, self.adapter)


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


def read_legacy_phy_marker(path: Path, adapter: str) -> dict[str, object] | None:
    """Inspect the pre-cutover PHY marker without creating legacy state."""
    try:
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC)
    except FileNotFoundError:
        return None
    try:
        file_state = os.fstat(fd)
        if not stat.S_ISREG(file_state.st_mode) or file_state.st_size > _MAX_LEGACY_MARKER_BYTES:
            raise PhyStateError("legacy PHY recovery marker is unsafe")
        with os.fdopen(fd, "r", encoding="utf-8", closefd=False) as stream:
            value = cast(object, json.load(stream))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise PhyStateError("legacy PHY recovery marker is unreadable") from error
    finally:
        os.close(fd)
    if not isinstance(value, dict):
        raise PhyStateError("legacy PHY recovery marker is malformed")
    _parse_legacy_marker(value, adapter)
    return cast(dict[str, object], value)


def _parse_db_marker(marker: dict[str, object], adapter: str) -> tuple[str, ...]:
    if set(marker) != {"adapter", "selected_phys", "version"} or marker.get("version") != _MARKER_VERSION:
        raise PhyStateError("PHY recovery marker is malformed")
    selected = marker.get("selected_phys")
    if marker.get("adapter") != adapter or not isinstance(selected, list):
        raise PhyStateError("PHY recovery marker is malformed")
    if not all(isinstance(token, str) for token in selected):
        raise PhyStateError("PHY recovery marker is malformed")
    snapshot = tuple(cast(list[str], selected))
    _validate_phys(snapshot)
    return snapshot


def _parse_legacy_marker(marker: dict[str, object], adapter: str) -> tuple[tuple[str, ...], dict[str, int]]:
    selected = marker.get("selected_phys")
    owner = marker.get("owner")
    if (
        marker.get("version") != _LEGACY_MARKER_VERSION
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
