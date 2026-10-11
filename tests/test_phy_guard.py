"""PHY selection is recoverable through the single Collector state database."""

from __future__ import annotations

import asyncio
import sys
from collections.abc import Awaitable, Callable
from functools import wraps
from pathlib import Path
from typing import cast

import pytest

import omi_collector.capture.adapters.phy_guard as phy_guard
from omi_collector.capture.adapters.collector_guard import collector_guard
from omi_collector.capture.adapters.collector_state_db import StateDB, StateKind
from omi_collector.capture.adapters.phy_guard import (
    CommandResult,
    PhyCommandError,
    PhyGuardBusyError,
    PhyStateError,
    ScopedPhyGuard,
    parse_selected_phys,
    run_bluetoothctl,
)
from omi_collector.config import DEFAULT_CONFIG

ORIGINAL = ("BR1M1SLOT", "LE1MTX", "LE1MRX", "LE2MTX", "LE2MRX")
TEMPORARY = ORIGINAL[:-2]


def _async_test[**P](function: Callable[P, Awaitable[None]]) -> Callable[P, None]:
    @wraps(function)
    def wrapped(*args: P.args, **kwargs: P.kwargs) -> None:
        asyncio.run(function(*args, **kwargs))

    return wrapped


class FakeRunner:
    def __init__(self, selected: tuple[str, ...] = ORIGINAL, failures: tuple[int, ...] = ()) -> None:
        self.selected = selected
        self.calls: list[tuple[str, ...]] = []
        self.failures = set(failures)

    async def __call__(self, argv: tuple[str, ...]) -> CommandResult:
        self.calls.append(argv)
        if len(self.calls) in self.failures:
            return CommandResult(1, "", "failed")
        if len(argv) == 6:
            return CommandResult(0, f"selected PHYs: {' '.join(self.selected)}\n")
        self.selected = argv[6:]
        return CommandResult(0, "")


class FakeProcess:
    def __init__(self, *, timeout_on_wait: bool = False) -> None:
        self.returncode: int | None = None
        self.communicate_started = asyncio.Event()
        self.timeout_on_wait = timeout_on_wait
        self.terminated = False
        self.killed = False
        self.waits = 0

    async def communicate(self) -> tuple[bytes, bytes]:
        self.communicate_started.set()
        await asyncio.Event().wait()
        return b"", b""

    def terminate(self) -> None:
        self.terminated = True

    def kill(self) -> None:
        self.killed = True

    async def wait(self) -> int:
        self.waits += 1
        if self.timeout_on_wait and self.waits == 1:
            raise TimeoutError
        self.returncode = -9 if self.killed else -15
        return self.returncode


def _db(tmp_path: Path) -> StateDB:
    return StateDB(tmp_path / "collector" / "collector-state.sqlite3")


def _marker(db: StateDB) -> dict[str, object] | None:
    return db.get(StateKind.PHY_RESTORATION, "hci0")


def test_phy_deadlines_project_from_runtime_config() -> None:
    configured = DEFAULT_CONFIG.phy
    assert configured.bluetoothctl_timeout_seconds == phy_guard._BLUETOOTHCTL_TIMEOUT_SECONDS
    assert configured.bluetoothctl_timeout_seconds == float(phy_guard._BLUETOOTHCTL[4])
    assert configured.reap_timeout_seconds == phy_guard._REAP_TIMEOUT_SECONDS


@_async_test
async def test_run_bluetoothctl_preserves_nonzero_exit_status() -> None:
    result = await run_bluetoothctl((sys.executable, "-c", "raise SystemExit(17)"))
    assert result == CommandResult(17, "", "")


@_async_test
async def test_run_bluetoothctl_cancellation_terminates_and_reaps(monkeypatch: pytest.MonkeyPatch) -> None:
    process = FakeProcess()

    async def create_process(*argv: str, **kwargs: object) -> FakeProcess:
        del argv, kwargs
        return process

    monkeypatch.setattr(phy_guard.asyncio, "create_subprocess_exec", create_process)
    task = asyncio.create_task(run_bluetoothctl(("fake-bluetoothctl",)))
    await process.communicate_started.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert process.terminated and not process.killed and process.waits == 1


@_async_test
async def test_run_bluetoothctl_reap_timeout_kills_process(monkeypatch: pytest.MonkeyPatch) -> None:
    process = FakeProcess(timeout_on_wait=True)

    async def create_process(*argv: str, **kwargs: object) -> FakeProcess:
        del argv, kwargs
        return process

    monkeypatch.setattr(phy_guard.asyncio, "create_subprocess_exec", create_process)
    task = asyncio.create_task(run_bluetoothctl(("fake-bluetoothctl",)))
    await process.communicate_started.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert process.terminated and process.killed and process.waits == 2


@_async_test
async def test_guard_sets_only_1m_and_restores_exact_snapshot(tmp_path: Path) -> None:
    db, runner = _db(tmp_path), FakeRunner()
    with collector_guard(db.path.parent / "collector.lock"):
        async with ScopedPhyGuard("hci0", state_db=db, runner=runner):
            assert runner.selected == TEMPORARY
            assert _marker(db) == {"adapter": "hci0", "selected_phys": list(ORIGINAL), "version": 2}
        assert runner.selected == ORIGINAL
        assert _marker(db) is None
    assert [call[6:] for call in runner.calls if len(call) > 6] == [TEMPORARY, ORIGINAL]
    assert not list(db.path.parent.glob("*.phy*"))


@_async_test
async def test_phy_requires_outer_collector_guard(tmp_path: Path) -> None:
    db, runner = _db(tmp_path), FakeRunner()
    with pytest.raises(PhyGuardBusyError, match="Collector process guard"):
        async with ScopedPhyGuard("hci0", state_db=db, runner=runner):
            pytest.fail("body must not run")
    assert runner.calls == []
    assert _marker(db) is None


@_async_test
async def test_set_failure_restores_and_clears_marker(tmp_path: Path) -> None:
    db, runner = _db(tmp_path), FakeRunner(failures=(2,))
    with collector_guard(db.path.parent / "collector.lock"):
        with pytest.raises(PhyCommandError):
            async with ScopedPhyGuard("hci0", state_db=db, runner=runner):
                pytest.fail("body must not run")
        assert runner.selected == ORIGINAL
        assert _marker(db) is None


@_async_test
async def test_body_error_restores_before_reraising(tmp_path: Path) -> None:
    db, runner = _db(tmp_path), FakeRunner()
    with collector_guard(db.path.parent / "collector.lock"):
        with pytest.raises(ValueError, match="body failed"):
            async with ScopedPhyGuard("hci0", state_db=db, runner=runner):
                raise ValueError("body failed")
        assert runner.selected == ORIGINAL
        assert _marker(db) is None


@_async_test
async def test_restore_failure_leaves_db_marker_for_next_run(tmp_path: Path) -> None:
    db, runner = _db(tmp_path), FakeRunner(failures=(4,))
    with collector_guard(db.path.parent / "collector.lock"):
        with pytest.raises(PhyCommandError):
            async with ScopedPhyGuard("hci0", state_db=db, runner=runner):
                assert runner.selected == TEMPORARY
        assert _marker(db) is not None
    runner.failures.clear()
    with collector_guard(db.path.parent / "collector.lock"):
        await ScopedPhyGuard("hci0", state_db=db, runner=runner).recover()
        assert runner.selected == ORIGINAL
        assert _marker(db) is None


@_async_test
async def test_stale_marker_recovers_before_new_temporary_selection(tmp_path: Path) -> None:
    db, runner = _db(tmp_path), FakeRunner(selected=TEMPORARY)
    db.put(StateKind.PHY_RESTORATION, "hci0", {"adapter": "hci0", "selected_phys": list(ORIGINAL), "version": 2})
    with collector_guard(db.path.parent / "collector.lock"):
        async with ScopedPhyGuard("hci0", state_db=db, runner=runner):
            assert runner.selected == TEMPORARY
        assert _marker(db) is None
    assert [call[6:] for call in runner.calls if len(call) > 6] == [ORIGINAL, TEMPORARY, ORIGINAL]


@_async_test
async def test_malformed_marker_fails_closed_before_controller_changes(tmp_path: Path) -> None:
    db, runner = _db(tmp_path), FakeRunner()
    bad_marker = {"adapter": "hci0", "selected_phys": [], "version": 2}
    db.put(StateKind.PHY_RESTORATION, "hci0", bad_marker)
    with collector_guard(db.path.parent / "collector.lock"):
        with pytest.raises(PhyStateError):
            await ScopedPhyGuard("hci0", state_db=db, runner=runner).recover()
        assert runner.calls == []
        assert _marker(db) == bad_marker


@_async_test
async def test_marker_mutation_before_cleanup_is_detected(tmp_path: Path) -> None:
    db, runner = _db(tmp_path), FakeRunner()
    with collector_guard(db.path.parent / "collector.lock"):
        with pytest.raises(PhyStateError, match="marker changed"):
            async with ScopedPhyGuard("hci0", state_db=db, runner=runner):
                db.put(
                    StateKind.PHY_RESTORATION,
                    "hci0",
                    {"adapter": "hci0", "selected_phys": list(TEMPORARY), "version": 2},
                )
        assert _marker(db) is not None


@pytest.mark.parametrize("selected", [("LE1MTX", "LE1MRX"), ("LE2MTX", "LE2MRX")])
@_async_test
async def test_guard_refuses_missing_required_phys(tmp_path: Path, selected: tuple[str, ...]) -> None:
    db, runner = _db(tmp_path), FakeRunner(selected=selected)
    with collector_guard(db.path.parent / "collector.lock"):
        with pytest.raises(PhyStateError):
            async with ScopedPhyGuard("hci0", state_db=db, runner=runner):
                pytest.fail("body must not run")
        assert _marker(db) is None


def test_parse_selected_phys_preserves_order_and_rejects_ambiguity() -> None:
    assert parse_selected_phys(f"selected PHYs: {' '.join(ORIGINAL)}") == ORIGINAL
    with pytest.raises(PhyStateError):
        parse_selected_phys("selected PHYs: LE1MTX LE1MRX\nselected PHYs: LE2MTX LE2MRX")
    with pytest.raises(PhyStateError):
        parse_selected_phys("selected PHYs: LE1MTX LE1MTX")


def test_invalid_adapter_rejected(tmp_path: Path) -> None:
    with pytest.raises(ValueError):
        ScopedPhyGuard("hci0;rm", state_db=_db(tmp_path), runner=FakeRunner())


@_async_test
async def test_restore_without_snapshot_is_rejected(tmp_path: Path) -> None:
    db = _db(tmp_path)
    with pytest.raises(PhyStateError, match="no recoverable snapshot"):
        await ScopedPhyGuard("hci0", state_db=db, runner=FakeRunner()).__aexit__(None, None, None)


def test_reap_returns_after_process_exited() -> None:
    process = FakeProcess()
    process.returncode = 0
    asyncio.run(phy_guard._terminate_and_reap(cast(asyncio.subprocess.Process, process)))
    assert not process.terminated and not process.killed
