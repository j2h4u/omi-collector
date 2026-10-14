"""Crash-safe, adapter-scoped guard for temporarily disabling LE 2M PHYs."""

from __future__ import annotations

import asyncio
import re
from collections.abc import Awaitable, Callable
from contextlib import AbstractAsyncContextManager, suppress
from dataclasses import dataclass
from typing import cast

from ...config import DEFAULT_CONFIG
from ..application.owned_work import join_owned, joined_to_thread
from ..application.storage_fault import FatalInternalAudioStorageError
from .collector_guard import guard_is_held
from .collector_state_db import StateDB, StateKind

_ADAPTER = re.compile(r"hci[0-9]+\Z")
_PHY_TOKEN = re.compile(r"[A-Z0-9]+\Z")
_SELECTED_PHYS = re.compile(r"^\s*selected\s+phys?\s*:\s*(?P<tokens>.+?)\s*$", re.IGNORECASE | re.MULTILINE)
_REMOVED_PHYS = frozenset(("LE2MTX", "LE2MRX"))
_REQUIRED_PHYS = frozenset(("LE1MTX", "LE1MRX", "LE2MTX", "LE2MRX"))
_MARKER_VERSION = 2
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
        await join_owned(self._recover_stale_marker())

    async def __aenter__(self) -> ScopedPhyGuard:
        self._require_guard()
        try:
            await join_owned(self._recover_stale_marker())
            snapshot = await self._selected_phys()
            temporary = _temporary_phys(snapshot)
            self._snapshot = snapshot
            await joined_to_thread(self._write_marker, snapshot)
            await self._set_selected_phys(temporary)
        except BaseException as primary:
            try:
                await join_owned(self._restore_after_failed_enter())
            except BaseException:
                if isinstance(primary, FatalInternalAudioStorageError):
                    raise primary from primary.__cause__
                raise
            raise
        return self

    async def __aexit__(self, exc_type: object, exc: object, traceback: object) -> bool:
        del exc_type, traceback
        try:
            await join_owned(self._restore_and_clear_marker())
        except BaseException:
            if isinstance(exc, FatalInternalAudioStorageError):
                raise exc from exc.__cause__
            raise
        return False

    def _require_guard(self) -> None:
        if not guard_is_held(self._db.path.parent / "collector.lock"):
            raise PhyGuardBusyError("Collector process guard is required for PHY changes")

    async def _recover_stale_marker(self) -> None:
        marker = await joined_to_thread(self._read_marker)
        if marker is None:
            return
        snapshot = _parse_db_marker(marker, self.adapter)
        await self._set_selected_phys(snapshot)
        if await self._selected_phys() != snapshot:
            raise PhyStateError("PHY recovery restore verification failed")
        await joined_to_thread(self._delete_marker, snapshot)

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
        await joined_to_thread(self._delete_marker, self._snapshot)

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


def _validate_phys(tokens: tuple[str, ...]) -> None:
    if not tokens or any(_PHY_TOKEN.fullmatch(token) is None for token in tokens) or len(set(tokens)) != len(tokens):
        raise PhyStateError("selected PHYs are malformed")


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
