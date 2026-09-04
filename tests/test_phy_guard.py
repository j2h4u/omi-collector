from __future__ import annotations

import asyncio
import json
import os
from collections.abc import Awaitable, Callable
from functools import wraps
from pathlib import Path
from typing import cast

import pytest

import omi_collector.capture.adapters.phy_guard as phy_guard
from omi_collector.capture.adapters.phy_guard import (
    CommandResult,
    PhyCommandError,
    PhyGuardBusyError,
    PhyGuardPaths,
    PhyStateError,
    ScopedPhyGuard,
    default_phy_guard_paths,
    parse_selected_phys,
    run_bluetoothctl,
)
from omi_collector.config import DEFAULT_CONFIG

ORIGINAL = (
    "BR1M1SLOT",
    "BR1M3SLOT",
    "BR1M5SLOT",
    "EDR2M1SLOT",
    "EDR2M3SLOT",
    "EDR2M5SLOT",
    "EDR3M1SLOT",
    "EDR3M3SLOT",
    "EDR3M5SLOT",
    "LE1MTX",
    "LE1MRX",
    "LE2MTX",
    "LE2MRX",
)
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
        self._failures = set(failures)

    async def __call__(self, argv: tuple[str, ...]) -> CommandResult:
        self.calls.append(argv)
        if len(self.calls) in self._failures:
            return CommandResult(1, "", "failed")
        if len(argv) == 6:
            return CommandResult(0, _phy_output(self.selected))
        self.selected = argv[6:]
        return CommandResult(0, "")


class FakeProcess:
    def __init__(self, *, cancel_communicate: bool = False, terminate_timeout: bool = False) -> None:
        self.returncode: int | None = None
        self.cancel_communicate = cancel_communicate
        self.terminate_timeout = terminate_timeout
        self.communicate_started = asyncio.Event()
        self.terminated = False
        self.killed = False
        self.waits = 0

    async def communicate(self) -> tuple[bytes, bytes]:
        if self.cancel_communicate:
            self.communicate_started.set()
            await asyncio.Event().wait()
        self.returncode = 0
        return b"selected PHYs: LE1MTX LE1MRX LE2MTX LE2MRX\n", b""

    def terminate(self) -> None:
        self.terminated = True

    def kill(self) -> None:
        self.killed = True

    async def wait(self) -> int:
        self.waits += 1
        if self.terminate_timeout and self.waits == 1:
            raise TimeoutError
        self.returncode = -9 if self.killed else -15
        return self.returncode


def _patch_process_factory(monkeypatch: pytest.MonkeyPatch, process: FakeProcess) -> None:
    async def fake_create_subprocess_exec(*argv: str, **kwargs: object) -> FakeProcess:
        del argv, kwargs
        return process

    monkeypatch.setattr(phy_guard.asyncio, "create_subprocess_exec", fake_create_subprocess_exec)


def _paths(tmp_path: Path) -> PhyGuardPaths:
    directory = tmp_path / "guard-state"
    return PhyGuardPaths(directory / "hci0.lock", directory / "hci0.marker")


def _phy_output(tokens: tuple[str, ...]) -> str:
    return f"hci0: Primary controller\nselected PHYs: {' '.join(tokens)}\n"


def _live_owner() -> dict[str, int]:
    stat_line = Path("/proc/self/stat").read_text(encoding="utf-8")
    return {"pid": os.getpid(), "start_time": int(stat_line.rsplit(") ", maxsplit=1)[1].split()[19])}


def _guard(tmp_path: Path, runner: FakeRunner) -> ScopedPhyGuard:
    return ScopedPhyGuard("hci0", paths=_paths(tmp_path), runner=runner)


def test_phy_deadlines_project_from_runtime_config() -> None:
    configured = DEFAULT_CONFIG.phy

    assert configured.bluetoothctl_timeout_seconds == phy_guard._BLUETOOTHCTL_TIMEOUT_SECONDS
    assert configured.bluetoothctl_timeout_seconds == float(phy_guard._BLUETOOTHCTL[4])
    assert configured.reap_timeout_seconds == phy_guard._REAP_TIMEOUT_SECONDS


@_async_test
async def test_run_bluetoothctl_returns_normal_result(monkeypatch: pytest.MonkeyPatch) -> None:
    process = FakeProcess()
    _patch_process_factory(monkeypatch, process)

    result = await run_bluetoothctl(("fake-bluetoothctl",))

    assert result == CommandResult(0, "selected PHYs: LE1MTX LE1MRX LE2MTX LE2MRX\n", "")
    assert not process.terminated
    assert not process.killed


@_async_test
async def test_run_bluetoothctl_cancellation_terminates_and_reaps(monkeypatch: pytest.MonkeyPatch) -> None:
    process = FakeProcess(cancel_communicate=True)
    _patch_process_factory(monkeypatch, process)
    task = asyncio.create_task(run_bluetoothctl(("fake-bluetoothctl",)))
    await process.communicate_started.wait()
    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task

    assert process.terminated
    assert not process.killed
    assert process.waits == 1
    assert process.returncode == -15


@_async_test
async def test_run_bluetoothctl_termination_timeout_kills_and_reaps(monkeypatch: pytest.MonkeyPatch) -> None:
    process = FakeProcess(cancel_communicate=True, terminate_timeout=True)
    _patch_process_factory(monkeypatch, process)
    task = asyncio.create_task(run_bluetoothctl(("fake-bluetoothctl",)))
    await process.communicate_started.wait()
    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task

    assert process.terminated
    assert process.killed
    assert process.waits == 2
    assert process.returncode == -9


@_async_test
async def test_run_bluetoothctl_timeout_does_not_kill_process_that_exited() -> None:
    class ExitingProcess(FakeProcess):
        async def wait(self) -> int:
            self.waits += 1
            if self.waits == 1:
                self.returncode = -15
                raise TimeoutError
            assert self.returncode is not None
            return self.returncode

    process = ExitingProcess()
    await phy_guard._terminate_and_reap(cast(asyncio.subprocess.Process, process))
    assert process.terminated
    assert not process.killed
    assert process.waits == 2


@_async_test
async def test_happy_path_sets_only_1m_and_restores_exact_snapshot(tmp_path: Path) -> None:
    runner = FakeRunner()
    guard = _guard(tmp_path, runner)

    async with guard:
        assert runner.selected == TEMPORARY

    assert runner.selected == ORIGINAL
    assert not _paths(tmp_path).marker_path.exists()
    assert [call[6:] for call in runner.calls if len(call) > 6] == [TEMPORARY, ORIGINAL]


@_async_test
async def test_set_failure_restores_snapshot_and_clears_verified_marker(tmp_path: Path) -> None:
    runner = FakeRunner(failures=(2,))

    with pytest.raises(PhyCommandError):
        async with _guard(tmp_path, runner):
            pytest.fail("context body must not run")

    assert runner.selected == ORIGINAL
    assert not _paths(tmp_path).marker_path.exists()


@_async_test
async def test_body_failure_restores_before_reraising(tmp_path: Path) -> None:
    runner = FakeRunner()

    with pytest.raises(ValueError, match="body failed"):
        async with _guard(tmp_path, runner):
            raise ValueError("body failed")

    assert runner.selected == ORIGINAL
    assert not _paths(tmp_path).marker_path.exists()


@_async_test
async def test_restore_failure_retains_marker_for_recovery(tmp_path: Path) -> None:
    runner = FakeRunner(failures=(3,))
    paths = _paths(tmp_path)

    with pytest.raises(PhyCommandError):
        async with ScopedPhyGuard("hci0", paths=paths, runner=runner):
            pass

    assert paths.marker_path.exists()
    assert runner.selected == TEMPORARY


@_async_test
async def test_failed_enter_releases_lock_when_restore_also_fails(tmp_path: Path) -> None:
    runner = FakeRunner(failures=(2, 3))
    paths = _paths(tmp_path)
    guard = ScopedPhyGuard("hci0", paths=paths, runner=runner)

    with pytest.raises(PhyCommandError):
        await guard.__aenter__()

    assert guard._lock_fd is None
    assert paths.marker_path.exists()
    marker_before = paths.marker_path.read_bytes()

    fresh_guard = ScopedPhyGuard("hci0", paths=paths, runner=FakeRunner())
    fresh_guard._acquire_lock()
    try:
        assert fresh_guard._lock_fd is not None
        assert paths.marker_path.read_bytes() == marker_before
    finally:
        fresh_guard._release_lock()


@_async_test
async def test_cancellation_restores_snapshot(tmp_path: Path) -> None:
    runner = FakeRunner()

    with pytest.raises(asyncio.CancelledError):
        async with _guard(tmp_path, runner):
            raise asyncio.CancelledError

    assert runner.selected == ORIGINAL
    assert not _paths(tmp_path).marker_path.exists()


@_async_test
async def test_recover_restores_stale_marker_and_removes_it(tmp_path: Path) -> None:
    runner = FakeRunner(TEMPORARY)
    paths = _paths(tmp_path)
    paths.marker_path.parent.mkdir(mode=0o700)
    paths.marker_path.write_text(
        json.dumps(
            {
                "version": 1,
                "adapter": "hci0",
                "selected_phys": list(ORIGINAL),
                "owner": {"pid": 999999, "start_time": 1},
            }
        ),
        encoding="utf-8",
    )

    await ScopedPhyGuard("hci0", paths=paths, runner=runner).recover()

    assert runner.selected == ORIGINAL
    assert not paths.marker_path.exists()


@_async_test
async def test_stale_recovery_failure_retains_marker(tmp_path: Path) -> None:
    runner = FakeRunner(TEMPORARY, failures=(1,))
    paths = _paths(tmp_path)
    paths.marker_path.parent.mkdir(mode=0o700)
    paths.marker_path.write_text(
        json.dumps(
            {
                "version": 1,
                "adapter": "hci0",
                "selected_phys": list(ORIGINAL),
                "owner": {"pid": 999999, "start_time": 1},
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(PhyCommandError):
        await ScopedPhyGuard("hci0", paths=paths, runner=runner).recover()

    assert paths.marker_path.exists()
    assert runner.selected == TEMPORARY


@_async_test
async def test_live_marker_owner_fails_closed_without_running_commands(tmp_path: Path) -> None:
    runner = FakeRunner(TEMPORARY)
    paths = _paths(tmp_path)
    paths.marker_path.parent.mkdir(mode=0o700)
    paths.marker_path.write_text(
        json.dumps(
            {
                "version": 1,
                "adapter": "hci0",
                "selected_phys": list(ORIGINAL),
                "owner": _live_owner(),
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(PhyStateError, match="live process"):
        await ScopedPhyGuard("hci0", paths=paths, runner=runner).recover()

    assert runner.calls == []
    assert paths.marker_path.exists()


@_async_test
async def test_malformed_marker_fails_closed_without_running_commands(tmp_path: Path) -> None:
    runner = FakeRunner(TEMPORARY)
    paths = _paths(tmp_path)
    paths.marker_path.parent.mkdir(mode=0o700)
    paths.marker_path.write_text("not json", encoding="utf-8")

    with pytest.raises(PhyStateError, match="malformed"):
        await ScopedPhyGuard("hci0", paths=paths, runner=runner).recover()

    assert runner.calls == []
    assert paths.marker_path.exists()


@_async_test
async def test_lock_contention_is_refused(tmp_path: Path) -> None:
    runner = FakeRunner()
    first = _guard(tmp_path, runner)
    second = _guard(tmp_path, runner)

    async with first:
        with pytest.raises(PhyGuardBusyError):
            async with second:
                pytest.fail("second guard must not enter")


@pytest.mark.parametrize(
    "selected",
    (
        ORIGINAL[:-2],
        tuple(token for token in ORIGINAL if token != "LE1MRX"),
    ),
)
@_async_test
async def test_refuses_missing_2m_or_1m_selection(tmp_path: Path, selected: tuple[str, ...]) -> None:
    runner = FakeRunner(selected)
    paths = _paths(tmp_path)

    with pytest.raises(PhyStateError, match="LE 1M and LE 2M"):
        async with ScopedPhyGuard("hci0", paths=paths, runner=runner):
            pytest.fail("context body must not run")

    assert runner.selected == selected
    assert not paths.marker_path.exists()


def test_parse_selected_phys_preserves_order_and_rejects_ambiguity() -> None:
    assert parse_selected_phys(_phy_output(ORIGINAL)) == ORIGINAL

    with pytest.raises(PhyStateError, match="unambiguous"):
        parse_selected_phys("selected PHYs: LE1MTX LE1MRX\nselected PHYs: LE2MTX LE2MRX\n")


def test_path_and_adapter_validation(tmp_path: Path) -> None:
    same = tmp_path / "same"
    with pytest.raises(ValueError, match="must differ"):
        PhyGuardPaths(same, same)
    with pytest.raises(ValueError, match="absolute concrete"):
        PhyGuardPaths(Path("relative"), tmp_path / "marker")
    with pytest.raises(ValueError, match="absolute concrete"):
        PhyGuardPaths(Path("/"), tmp_path / "marker")
    with pytest.raises(ValueError, match="absolute concrete"):
        PhyGuardPaths(tmp_path / ".." / "lock", tmp_path / "marker")
    with pytest.raises(ValueError, match="hci<number>"):
        ScopedPhyGuard("hci-nope", paths=_paths(tmp_path), runner=FakeRunner())


def test_default_phy_guard_paths_uses_state_home(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("XDG_STATE_HOME", str(tmp_path / "xdg-state"))
    paths = default_phy_guard_paths("hci2")
    assert paths.lock_path == tmp_path / "xdg-state" / "omi-collector" / "hci2.phy.lock"
    assert paths.marker_path == tmp_path / "xdg-state" / "omi-collector" / "hci2.phy-recovery.json"


def test_default_phy_guard_paths_rejects_invalid_adapter(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="hci<number>"):
        default_phy_guard_paths("hci")
    paths = default_phy_guard_paths("hci0", state_home=tmp_path)
    assert paths.lock_path.parent == tmp_path / "omi-collector"


@_async_test
async def test_run_bluetoothctl_reap_returns_when_process_already_exited() -> None:
    process = FakeProcess()
    process.returncode = 0
    await phy_guard._terminate_and_reap(cast(asyncio.subprocess.Process, process))
    assert process.waits == 0


@_async_test
async def test_set_selected_phys_rejects_empty_selection(tmp_path: Path) -> None:
    guard = _guard(tmp_path, FakeRunner())
    with pytest.raises(PhyStateError, match="empty PHY"):
        await guard._set_selected_phys(())


def test_release_lock_is_noop_when_not_held(tmp_path: Path) -> None:
    guard = _guard(tmp_path, FakeRunner())
    guard._release_lock()
    assert guard._lock_fd is None


def test_acquire_lock_rejects_reentrant_instance(tmp_path: Path) -> None:
    guard = _guard(tmp_path, FakeRunner())
    guard._lock_fd = 123
    with pytest.raises(PhyStateError, match="already held by this instance"):
        guard._acquire_lock()
    guard._lock_fd = None


def test_acquire_lock_closes_descriptor_on_unexpected_flock_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    guard = _guard(tmp_path, FakeRunner())

    def fail_flock(*args: object) -> None:
        del args
        raise OSError("flock failed")

    monkeypatch.setattr(phy_guard.fcntl, "flock", fail_flock)
    with pytest.raises(OSError, match="flock failed"):
        guard._acquire_lock()
    assert guard._lock_fd is None


class MismatchingRunner(FakeRunner):
    def __init__(
        self,
        first: tuple[str, ...] = ORIGINAL,
        later: tuple[str, ...] = TEMPORARY,
        reads_before_later: int = 1,
    ) -> None:
        super().__init__(first)
        self._read_count = 0
        self._later = later
        self._reads_before_later = reads_before_later

    async def __call__(self, argv: tuple[str, ...]) -> CommandResult:
        if len(argv) == 6:
            self.calls.append(argv)
            self._read_count += 1
            selected = self.selected if self._read_count <= self._reads_before_later else self._later
            return CommandResult(0, _phy_output(selected))
        self.calls.append(argv)
        self.selected = argv[6:]
        return CommandResult(0, "")


def _write_stale_marker(paths: PhyGuardPaths) -> None:
    paths.marker_path.parent.mkdir(mode=0o700, parents=True)
    paths.marker_path.write_text(
        json.dumps(
            {
                "version": 1,
                "adapter": "hci0",
                "selected_phys": list(ORIGINAL),
                "owner": {"pid": 999999, "start_time": 1},
            }
        ),
        encoding="utf-8",
    )


@_async_test
async def test_recovery_restore_verification_fails_closed(tmp_path: Path) -> None:
    paths = _paths(tmp_path)
    _write_stale_marker(paths)
    runner = MismatchingRunner(TEMPORARY, TEMPORARY, reads_before_later=0)
    with pytest.raises(PhyStateError, match="restore verification"):
        await ScopedPhyGuard("hci0", paths=paths, runner=runner).recover()
    assert paths.marker_path.exists()


@_async_test
async def test_restore_without_snapshot_is_rejected(tmp_path: Path) -> None:
    guard = _guard(tmp_path, FakeRunner())
    with pytest.raises(PhyStateError, match="no recoverable snapshot"):
        await guard._restore_and_clear_marker()


@_async_test
async def test_context_restore_verification_fails_closed(tmp_path: Path) -> None:
    runner = MismatchingRunner()
    guard = ScopedPhyGuard("hci0", paths=_paths(tmp_path), runner=runner)
    await guard.__aenter__()
    with pytest.raises(PhyStateError, match="PHY restore verification failed"):
        await guard.__aexit__(None, None, None)
    assert _paths(tmp_path).marker_path.exists()


@_async_test
async def test_marker_symlink_is_rejected(tmp_path: Path) -> None:
    paths = _paths(tmp_path)
    paths.marker_path.parent.mkdir(mode=0o700)
    target = paths.marker_path.parent / "target"
    target.write_text("safe", encoding="utf-8")
    paths.marker_path.symlink_to(target)
    guard = ScopedPhyGuard("hci0", paths=paths, runner=FakeRunner())
    with pytest.raises(PhyStateError, match="marker is a symlink"):
        guard._write_marker(ORIGINAL, {"pid": 1, "start_time": 1})


def test_non_mapping_marker_is_rejected(tmp_path: Path) -> None:
    paths = _paths(tmp_path)
    paths.marker_path.parent.mkdir(mode=0o700)
    paths.marker_path.write_text("[]", encoding="utf-8")
    with pytest.raises(PhyStateError, match="malformed"):
        ScopedPhyGuard("hci0", paths=paths, runner=FakeRunner())._read_marker()


@_async_test
async def test_marker_disappearance_before_cleanup_is_detected(tmp_path: Path) -> None:
    paths = _paths(tmp_path)
    runner = FakeRunner()
    with pytest.raises(PhyStateError, match="disappeared"):
        async with ScopedPhyGuard("hci0", paths=paths, runner=runner):
            paths.marker_path.unlink()


@_async_test
async def test_marker_mutation_before_cleanup_is_detected(tmp_path: Path) -> None:
    paths = _paths(tmp_path)
    runner = FakeRunner()
    with pytest.raises(PhyStateError, match="changed"):
        async with ScopedPhyGuard("hci0", paths=paths, runner=runner):
            marker = cast(dict[str, object], json.loads(paths.marker_path.read_text(encoding="utf-8")))
            marker["selected_phys"] = [*TEMPORARY, "LE2MTX", "LE2MRX"]
            owner = cast(dict[str, int], marker["owner"])
            owner["start_time"] += 1
            paths.marker_path.write_text(json.dumps(marker), encoding="utf-8")


def test_temporary_selection_rejects_unexpected_removed_set(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(phy_guard, "_REMOVED_PHYS", frozenset({"LE2MTX", "NOT_SELECTED"}))
    with pytest.raises(PhyStateError, match="change more than LE 2M"):
        phy_guard._temporary_phys(ORIGINAL)


def test_selected_output_and_tokens_are_validated() -> None:
    with pytest.raises(PhyStateError, match="unambiguous"):
        parse_selected_phys("controller output without selected line")
    with pytest.raises(PhyStateError, match="malformed"):
        parse_selected_phys("selected PHYs: LE1MTX LE1MTX")
    with pytest.raises(PhyStateError, match="malformed"):
        parse_selected_phys("selected PHYs: le1mtx")


def test_storage_parent_symlink_is_rejected(tmp_path: Path) -> None:
    real = tmp_path / "real"
    real.mkdir()
    link = tmp_path / "link"
    link.symlink_to(real, target_is_directory=True)
    with pytest.raises(PhyStateError, match="parent is a symlink"):
        phy_guard._prepare_storage_parent(link / "state")


def test_storage_symlink_and_non_regular_file_are_rejected(tmp_path: Path) -> None:
    target = tmp_path / "target"
    target.write_text("safe", encoding="utf-8")
    link = tmp_path / "link"
    link.symlink_to(target)
    with pytest.raises(PhyStateError, match="file is a symlink"):
        phy_guard._open_regular_file(link, os.O_RDONLY)

    directory = tmp_path / "directory"
    directory.mkdir()
    with pytest.raises(PhyStateError, match="not regular"):
        phy_guard._open_regular_file(directory, os.O_RDONLY)


def test_atomic_marker_write_removes_temporary_file_on_replace_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    marker = tmp_path / "marker.json"

    def fail_replace(self: Path, target: Path) -> Path:
        del self, target
        raise OSError("replace failed")

    monkeypatch.setattr(Path, "replace", fail_replace)
    with pytest.raises(OSError, match="replace failed"):
        phy_guard._atomic_json_write(marker, {"ok": True})
    assert tuple(tmp_path.iterdir()) == ()


def test_marker_owner_requires_process_start_time(monkeypatch: pytest.MonkeyPatch) -> None:
    def missing_start_time(pid: int) -> None:
        del pid

    monkeypatch.setattr(phy_guard, "_process_start_time", missing_start_time)
    with pytest.raises(PhyStateError, match="start time"):
        phy_guard._marker_owner()


@pytest.mark.parametrize("stat_line", ("malformed", "comm) " + " ".join(["0"] * 19 + ["x"])))
def test_process_start_time_rejects_malformed_proc_stat(monkeypatch: pytest.MonkeyPatch, stat_line: str) -> None:
    def fake_read_text(path: Path, **kwargs: object) -> str:
        del path, kwargs
        return stat_line

    monkeypatch.setattr(Path, "read_text", fake_read_text)
    assert phy_guard._process_start_time(123) is None


@pytest.mark.parametrize(
    "marker",
    (
        {},
        {"version": 2, "adapter": "hci0", "selected_phys": list(ORIGINAL), "owner": {"pid": 1, "start_time": 1}},
        {"version": 1, "adapter": "other", "selected_phys": list(ORIGINAL), "owner": {"pid": 1, "start_time": 1}},
        {"version": 1, "adapter": "hci0", "selected_phys": "bad", "owner": {"pid": 1, "start_time": 1}},
        {"version": 1, "adapter": "hci0", "selected_phys": [1], "owner": {"pid": 1, "start_time": 1}},
        {"version": 1, "adapter": "hci0", "selected_phys": list(ORIGINAL), "owner": []},
    ),
)
def test_marker_shape_is_validated(marker: dict[str, object]) -> None:
    with pytest.raises(PhyStateError, match="malformed"):
        phy_guard._parse_marker(marker, "hci0")


@pytest.mark.parametrize(
    "owner",
    (
        {"pid": 0, "start_time": 1},
        {"pid": "1", "start_time": 1},
        {"pid": 1, "start_time": -1},
        {"pid": 1, "start_time": "1"},
    ),
)
def test_marker_owner_fields_are_validated(owner: dict[str, object]) -> None:
    marker = {"version": 1, "adapter": "hci0", "selected_phys": list(ORIGINAL), "owner": owner}
    with pytest.raises(PhyStateError, match="malformed"):
        phy_guard._parse_marker(marker, "hci0")


@_async_test
async def test_marker_is_private_after_enter(tmp_path: Path) -> None:
    runner = FakeRunner()
    paths = _paths(tmp_path)

    async with ScopedPhyGuard("hci0", paths=paths, runner=runner):
        assert paths.marker_path.stat().st_mode & 0o777 == 0o600
