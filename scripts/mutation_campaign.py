"""Guard and reconcile the one native full-project mutation campaign."""

from __future__ import annotations

import argparse
import ctypes
import errno
import hashlib
import json
import os
import secrets
import shutil
import signal
import sqlite3
import stat
import subprocess
import sys
import threading
import time
from collections.abc import Callable
from contextlib import closing
from dataclasses import dataclass, field
from datetime import UTC, datetime
from importlib.metadata import PackageNotFoundError, distribution
from pathlib import Path
from types import FrameType
from typing import BinaryIO, cast

from scripts.mutation_scope import AUDIT_ROOT, OwnerControlServer, ScopeError, control_socket_path, remove_stale_socket

CACHE = Path(".gremlins_cache")
RECEIPT = CACHE / "campaign.json"
REPORT = Path("coverage/gremlins/gremlins.json")
NATIVE_CACHE = (CACHE / "results.db", CACHE / "coverage.json", CACHE / "coverage.sqlite")
OWNER_FILE = "owner.json"
OWNER_LOCK = ".owner.lock"
JOB_TOKEN_ENV = "OMI_MUTATION_JOB_TOKEN"
OWNER_CHECK_SECONDS = 600.0
PRIVATE_TMP_MODE = 0o700
PROCESS_SETTLE_SECONDS = 0.1
STABLE_PROCESS_SCANS = 2
UNRESOLVED_EXIT_STATUS = 3
FIXED_ENV = ("COVERAGE_CORE", "COVERAGE_FILE", "PYTEST_ADDOPTS", "UV_LINK_MODE")
PROC_STAT_START_TICKS = 19
RELEVANT_ENV = (
    *FIXED_ENV,
    "COVERAGE_PROCESS_START",
    "COVERAGE_RCFILE",
    "LC_ALL",
    "PATH",
    "PYTHONHASHSEED",
    "PYTHONHOME",
    "PYTHONPATH",
    "PYTEST_DISABLE_PLUGIN_AUTOLOAD",
    "PYTEST_PLUGINS",
    "PYTEST_TIMEOUT",
    "TZ",
    "TMPDIR",
    "UV_CACHE_DIR",
    "UV_PROJECT_ENVIRONMENT",
    "VIRTUAL_ENV",
)
RELEASE_WRAPPER = "scripts/omi-collector-deploy-release"
RELEASE_SUDOERS = "scripts/omi-collector-deploy-release.sudoers"
RELEASE_WRAPPER_MODE = 0o755
RELEASE_SUDOERS_MODE = 0o644


@dataclass
class OwnerRun:
    job_root: Path
    mode: str
    token: str
    checkout: Path
    run_env: dict[str, str]
    receipt: dict[str, object]
    process: subprocess.Popen[bytes] | None = None
    paused: threading.Event = field(default_factory=threading.Event)
    invalidated: threading.Event = field(default_factory=threading.Event)
    state_lock: threading.Lock = field(default_factory=threading.Lock)
    requested_signal: list[int] = field(default_factory=list)


def _git(*args: str) -> str:
    result = subprocess.run(["git", *args], check=True, capture_output=True, text=True)
    return result.stdout.strip()


def _identity() -> dict[str, object]:
    dirty = _git("status", "--porcelain", "--untracked-files=all")
    if dirty:
        raise ValueError("campaign inputs must be clean and committed")
    env = {key: os.environ.get(key, "") for key in RELEVANT_ENV}
    expected = {"COVERAGE_CORE": "ctrace", "COVERAGE_FILE": "", "PYTEST_ADDOPTS": "", "UV_LINK_MODE": "hardlink"}
    invalid = [key for key, value in expected.items() if env[key] != value]
    if invalid:
        raise ValueError("campaign environment has invalid values for: " + ", ".join(invalid))
    try:
        gremlins = distribution("pytest-gremlins")
        gremlins_version = gremlins.version
        direct_url = gremlins.read_text("direct_url.json") or ""
    except PackageNotFoundError as exc:
        raise ValueError("pytest-gremlins is not installed in this uv environment") from exc
    uv_path = shutil.which("uv")
    uv_version = subprocess.run(
        [uv_path or "uv", "--version"], check=True, capture_output=True, text=True
    ).stdout.strip()
    lock = Path("uv.lock").read_bytes()
    project = Path("pyproject.toml").read_bytes()
    return {
        "commit": _git("rev-parse", "HEAD"),
        "uv_lock_sha256": hashlib.sha256(lock).hexdigest(),
        "pyproject_sha256": hashlib.sha256(project).hexdigest(),
        "python": {"executable": sys.executable, "version": sys.version},
        "uv": {"path": uv_path, "version": uv_version},
        "pytest_gremlins": {"version": gremlins_version, "direct_url": direct_url},
        "environment": {key: env[key] for key in FIXED_ENV},
        "environment_sha256": hashlib.sha256(json.dumps(env, sort_keys=True).encode()).hexdigest(),
    }


def _read_receipt() -> dict[str, object] | None:
    if not RECEIPT.is_file():
        return None
    value = cast(object, json.loads(RECEIPT.read_text(encoding="utf-8")))
    if not isinstance(value, dict):
        raise ValueError("campaign receipt is not an object")
    return cast(dict[str, object], value)


def _write_receipt(receipt: dict[str, object]) -> None:
    CACHE.mkdir(exist_ok=True)
    temporary = RECEIPT.with_suffix(".tmp")
    temporary.write_text(json.dumps(receipt, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    temporary.replace(RECEIPT)


def _prepare(fresh: bool, launcher_pid: int) -> str:
    identity = _identity()
    old = _read_receipt()
    has_native_cache = any(path.exists() and path.stat().st_size for path in NATIVE_CACHE)
    if fresh:
        mode = "fresh"
    elif old is None:
        if has_native_cache:
            raise ValueError("native cache has no campaign identity; preserve it and use explicit fresh mode")
        mode = "new"
    else:
        if old.get("identity") != identity:
            raise ValueError("campaign identity changed; preserve old evidence and use explicit fresh mode")
        state = old.get("state")
        if state == "complete_unresolved":
            raise ValueError(
                "completed report contains unresolved outcomes; adjudicate it before starting another audit"
            )
        if state == "complete":
            raise ValueError("campaign is already complete; preserve its report before explicit fresh mode")
        _assert_no_live_campaign_processes(
            int(cast(int, old["started_at_ns"])), cast(dict[str, object], old.get("launcher", {}))
        )
        mode = "resume"
    now = time.time_ns()
    token = datetime.fromtimestamp(now / 1_000_000_000, UTC).strftime("%Y%m%dT%H%M%S") + f".{now % 1_000_000_000:09d}Z"
    previous_report = REPORT.stat().st_mtime_ns if REPORT.exists() else None
    receipt = {
        "identity": identity,
        "state": "running",
        "mode": mode,
        "started_at": datetime.fromtimestamp(now / 1_000_000_000, UTC).isoformat(),
        "started_at_ns": now,
        "launcher": {"pid": launcher_pid, "start_ticks": _proc_start_ticks(launcher_pid)},
        "report_mtime_before_ns": previous_report,
        "log": f"{CACHE}/mutation-{token}.log",
        "exit_status": None,
        "report": None,
    }
    _write_receipt(receipt)
    return token


def _proc_start_ticks(pid: int) -> str | None:
    try:
        stat = Path(f"/proc/{pid}/stat").read_text(encoding="ascii")
    except OSError:
        return None
    fields = stat.rsplit(")", 1)[-1].split()
    return fields[PROC_STAT_START_TICKS] if len(fields) > PROC_STAT_START_TICKS else None


def _proc_parent_pid(pid: int) -> int | None:
    try:
        fields = Path(f"/proc/{pid}/stat").read_text(encoding="ascii").rsplit(")", 1)[-1].split()
        return int(fields[1])
    except OSError, IndexError, ValueError:
        return None


def _assert_no_live_campaign_processes(started_at_ns: int, launcher: dict[str, object]) -> None:
    try:
        boot_time = int(
            next(line.split()[1] for line in Path("/proc/stat").read_text().splitlines() if line.startswith("btime "))
        )
    except (OSError, StopIteration, ValueError) as exc:
        raise ValueError("cannot verify Linux process ownership before campaign resume") from exc
    ticks_per_second = os.sysconf("SC_CLK_TCK")
    first_tick = max(0, int((started_at_ns / 1_000_000_000 - boot_time) * ticks_per_second))
    active: list[str] = []
    launcher_pid = launcher.get("pid")
    launcher_ticks = launcher.get("start_ticks")
    if (
        isinstance(launcher_pid, int)
        and isinstance(launcher_ticks, str)
        and _proc_start_ticks(launcher_pid) == launcher_ticks
    ):
        active.append(f"{launcher_pid}:{launcher_ticks}:recorded campaign launcher")
    own_chain: set[int] = set()
    current_pid: int | None = os.getpid()
    while current_pid and current_pid not in own_chain:
        own_chain.add(current_pid)
        current_pid = _proc_parent_pid(current_pid)
    root = Path.cwd().resolve()
    for proc in Path("/proc").iterdir():
        if not proc.name.isdecimal() or int(proc.name) in own_chain:
            continue
        try:
            ticks = int(cast(str, _proc_start_ticks(int(proc.name))))
            command = proc.joinpath("cmdline").read_bytes().replace(b"\0", b" ").decode(errors="replace")
            cwd = proc.joinpath("cwd").resolve()
        except OSError, TypeError, ValueError:
            continue
        if (
            ticks >= first_tick
            and cwd == root
            and any(token in command for token in ("pytest", "forkserver", "spawn_main"))
        ):
            active.append(f"{proc.name}:{ticks}:{command[:100]}")
    if active:
        raise ValueError("campaign-owned processes may remain; inspect PID/start-time receipts: " + "; ".join(active))


def _finish(status: int) -> int:
    receipt = _read_receipt()
    if receipt is None or receipt.get("state") != "running":
        raise ValueError("there is no active campaign receipt to finish")
    receipt["ended_at"] = datetime.now(UTC).isoformat()
    receipt["exit_status"] = status
    if status != 0:
        log_path = Path(str(receipt.get("log", "")))
        log_text = log_path.read_text(encoding="utf-8", errors="replace") if log_path.is_file() else ""
        if status in (124, 130, 137, 143):
            receipt["state"] = "interrupted"
        elif "No gremlins tested" in log_text or "baseline failed" in log_text.lower():
            receipt["state"] = "baseline_failed"
        else:
            receipt["state"] = "controller_failed"
        _write_receipt(receipt)
        return 0
    try:
        result = _postflight(receipt)
        receipt["state"] = result["state"]
        receipt["report"] = result["report"]
        print(
            f"Native campaign report reconciled: {result['mutant_count']} mutants across {result['source_file_count']} source files."
        )
        if result["state"] == "complete_unresolved":
            report = cast(dict[str, object], result["report"])
            counts = cast(dict[str, int], report["status_counts"])
            print(
                f"Unresolved native outcomes remain: timeout={counts['timeout']} error={counts['error']}; review required.",
                file=sys.stderr,
            )
    except (OSError, sqlite3.Error, subprocess.SubprocessError, ValueError, json.JSONDecodeError) as exc:
        receipt["state"] = "failed"
        receipt["postflight_error"] = str(exc)
        _write_receipt(receipt)
        raise
    _write_receipt(receipt)
    return 3 if receipt["state"] == "complete_unresolved" else 0


def _postflight(receipt: dict[str, object]) -> dict[str, object]:
    if receipt.get("identity") != _identity():
        raise ValueError("campaign inputs changed while it was running")
    report = _load_report(int(cast(int, receipt["started_at_ns"])))
    source_paths, ids, results, summary = _report_scope(report)
    counts = _result_counts(results)
    _validate_report_counts(summary, ids, results, counts)
    _validate_report_files(report, source_paths, results)
    state = "complete" if counts["error"] == counts["timeout"] == 0 else "complete_unresolved"
    report_receipt = {
        "sha256": hashlib.sha256(REPORT.read_bytes()).hexdigest(),
        "source_file_count": len(source_paths),
        "mutant_count": len(ids),
        "status_counts": counts,
    }
    return {"state": state, "report": report_receipt, "mutant_count": len(ids), "source_file_count": len(source_paths)}


def _report_scope(report: dict[str, object]) -> tuple[list[str], list[str], list[object], dict[str, object]]:
    scope, results, summary = report.get("scope"), report.get("results"), report.get("summary")
    if not isinstance(scope, dict) or not isinstance(results, list) or not isinstance(summary, dict):
        raise ValueError("native report scope, results, or summary has an invalid shape")
    if not isinstance(scope.get("generation_errors"), list) or scope["generation_errors"]:
        raise ValueError("native Gremlins failed to transform one or more source files")
    source_files, mutant_ids = scope.get("source_files"), scope.get("gremlin_ids")
    if not isinstance(source_files, list) or not isinstance(mutant_ids, list):
        raise ValueError("native report scope is missing source files or generated mutant IDs")
    source_paths, ids = [str(path) for path in source_files], [str(item) for item in mutant_ids]
    if not source_paths or len(source_paths) != len(set(source_paths)) or source_paths != sorted(source_paths):
        raise ValueError("native report source-file scope is empty, duplicate, or unsorted")
    _validate_source_scope(source_paths)
    if not ids or len(ids) != len(set(ids)) or ids != sorted(ids):
        raise ValueError("native generated mutant IDs are empty, duplicate, or unsorted")
    return source_paths, ids, results, summary


def _result_counts(results: list[object]) -> dict[str, int]:
    statuses = [str(entry.get("status")) for entry in results if isinstance(entry, dict)]
    known = {"zapped", "survived", "timeout", "error", "pardoned"}
    if len(statuses) != len(results) or not set(statuses) <= known:
        raise ValueError("native report contains an invalid or unresolved status")
    return {name: statuses.count(name) for name in sorted(known)}


def _validate_report_counts(
    summary: dict[str, object], ids: list[str], results: list[object], counts: dict[str, int]
) -> None:
    result_ids = [str(entry.get("gremlin_id")) for entry in results if isinstance(entry, dict)]
    if len(result_ids) != len(results) or len(result_ids) != len(set(result_ids)) or set(result_ids) != set(ids):
        raise ValueError("native result IDs are duplicate, missing, or foreign to generated scope")
    if summary.get("total") != len(ids) or any(summary.get(name, 0) != count for name, count in counts.items()):
        raise ValueError("native summary counts do not match generated scope and results")


def _load_report(started_at_ns: int) -> dict[str, object]:
    receipt = _read_receipt()
    previous_mtime = receipt.get("report_mtime_before_ns") if receipt is not None else None
    if not REPORT.is_file() or REPORT.stat().st_mtime_ns < started_at_ns or REPORT.stat().st_mtime_ns == previous_mtime:
        raise ValueError("no fresh native JSON report was written by this attempt")
    value = cast(object, json.loads(REPORT.read_text(encoding="utf-8")))
    if not isinstance(value, dict):
        raise ValueError("native JSON report is not an object")
    return cast(dict[str, object], value)


def _validate_report_files(report: dict[str, object], source_paths: list[str], results: list[object]) -> None:
    files = report.get("files")
    if not isinstance(files, dict):
        raise ValueError("native JSON report file breakdown is invalid")
    result_counts: dict[str, int] = {}
    for item in results:
        if isinstance(item, dict):
            path = _relative_report_path(str(item.get("file_path")))
            result_counts[path] = result_counts.get(path, 0) + 1
    file_counts = {
        _relative_report_path(str(path)): value.get("total") for path, value in files.items() if isinstance(value, dict)
    }
    if len(file_counts) != len(files) or file_counts != result_counts:
        raise ValueError("native per-file results do not match unique report entries")
    if not set(file_counts) <= set(source_paths):
        raise ValueError("native report file breakdown includes a path outside generated source scope")


def _relative_report_path(raw_path: str) -> str:
    path = Path(raw_path)
    if path.is_absolute():
        try:
            path = path.relative_to(Path.cwd())
        except ValueError as exc:
            raise ValueError(f"native report contains a foreign path: {raw_path}") from exc
    if ".." in path.parts:
        raise ValueError(f"native report contains an invalid path: {raw_path}")
    return path.as_posix()


def _validate_source_scope(source_paths: list[str]) -> None:
    for source in source_paths:
        path = Path(source)
        if path.is_absolute() or ".." in path.parts or path.suffix != ".py" or not path.is_file():
            raise ValueError(f"native report contains invalid source path: {source}")
        if not source.startswith(("src/omi_collector/", "scripts/")):
            raise ValueError(f"native report contains foreign source path: {source}")
    tracked = set(_git("ls-files", "--cached", "--", "src/omi_collector", "scripts").splitlines())
    expected = {
        source
        for source in tracked
        if source.endswith(".py")
        and not Path(source).name.startswith("test_")
        and not Path(source).name.endswith("_test.py")
        and Path(source).name != "conftest.py"
    }
    if set(source_paths) != expected:
        raise ValueError("native discovered source-file scope does not match committed QA targets")


def _write_owner(job_root: Path, receipt: dict[str, object]) -> None:
    path = job_root / OWNER_FILE
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(receipt, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    temporary.chmod(0o600)
    temporary.replace(path)


def _proc_identity(pid: int) -> tuple[int, str, str]:
    proc = Path(f"/proc/{pid}")
    fields = proc.joinpath("stat").read_text(encoding="ascii").rsplit(")", 1)[-1].split()
    if len(fields) <= PROC_STAT_START_TICKS:
        raise ValueError(f"cannot parse process identity for PID {pid}")
    return int(fields[1]), fields[PROC_STAT_START_TICKS], fields[0]


def _process_table(root_pid: int | None) -> dict[int, tuple[int, str, str, str]]:
    processes: dict[int, tuple[int, str, str, str]] = {}
    for proc in Path("/proc").iterdir():
        if not proc.name.isdecimal():
            continue
        pid = int(proc.name)
        try:
            if proc.stat().st_uid != os.getuid():
                continue
            parent, start_ticks, state = _proc_identity(pid)
        except FileNotFoundError:
            continue
        except PermissionError as exc:
            if pid == root_pid:
                raise ValueError(f"cannot verify the owned root PID {pid}") from exc
            continue
        try:
            cmdline = proc.joinpath("cmdline").read_bytes().replace(b"\0", b" ").decode("utf-8", errors="replace")
        except FileNotFoundError:
            continue
        except PermissionError:
            cmdline = ""
        processes[pid] = (parent, start_ticks, state, cmdline)
    return processes


def _descendants(processes: dict[int, tuple[int, str, str, str]], root_pid: int, root_ticks: str) -> set[int]:
    root = processes.get(root_pid)
    if root is not None and root[1] != root_ticks:
        raise ValueError("the owned root PID was reused")
    if root is None:
        return set()
    descendants = {root_pid}
    while True:
        additions = {pid for pid, (parent, _ticks, _state, _cmdline) in processes.items() if parent in descendants}
        if additions <= descendants:
            return descendants
        descendants.update(additions)


def _has_job_token(pid: int, token: str, descendants: set[int]) -> bool:
    try:
        environ = Path(f"/proc/{pid}/environ").read_bytes()
    except FileNotFoundError:
        return False
    except PermissionError:
        return pid in descendants
    return JOB_TOKEN_ENV.encode() + b"=" + token.encode() in environ.split(b"\0")


def _token_pids(token: str, root_pid: int | None = None, root_ticks: str | None = None) -> dict[int, tuple[str, str]]:
    processes = _process_table(root_pid)
    descendants: set[int] = set()
    if root_pid is not None and root_ticks is not None:
        descendants = _descendants(processes, root_pid, root_ticks)
    matches: dict[int, tuple[str, str]] = {}
    for pid, (_parent, start_ticks, state, cmdline) in processes.items():
        if state == "Z":
            continue
        if _has_job_token(pid, token, descendants) or pid in descendants:
            matches[pid] = (start_ticks, cmdline)
    return matches


def _open_pidfd(pid: int, expected_ticks: str) -> int:
    _parent, current_ticks, state = _proc_identity(pid)
    if current_ticks != expected_ticks or state == "Z":
        raise ProcessLookupError(pid)
    libc = ctypes.CDLL(None, use_errno=True)
    try:
        pidfd_open = libc.pidfd_open
    except AttributeError as exc:
        raise ValueError("this host does not expose pidfd_open") from exc
    pidfd_open.argtypes = (ctypes.c_int, ctypes.c_uint)
    pidfd_open.restype = ctypes.c_int
    raw_fd = cast(int, pidfd_open(pid, 0))
    if raw_fd < 0:
        error_number = ctypes.get_errno()
        raise OSError(error_number, os.strerror(error_number))
    pidfd = int(raw_fd)
    try:
        _parent, current_ticks, state = _proc_identity(pid)
        if current_ticks != expected_ticks or state == "Z":
            raise ProcessLookupError(pid)
    except BaseException:
        os.close(pidfd)
        raise
    return pidfd


def _verified_process_exit_race(error: OSError, pid: int, expected_ticks: str) -> bool:
    if error.errno not in {errno.ENOENT, errno.ESRCH}:
        return False
    try:
        _parent, current_ticks, state = _proc_identity(pid)
    except FileNotFoundError:
        return True
    return current_ticks != expected_ticks or state == "Z"


def _pidfd_signal(pidfd: int, sig: signal.Signals) -> None:
    libc = ctypes.CDLL(None, use_errno=True)
    try:
        pidfd_send_signal = libc.pidfd_send_signal
    except AttributeError as exc:
        raise ValueError("this host does not expose pidfd_send_signal") from exc
    pidfd_send_signal.argtypes = (ctypes.c_int, ctypes.c_int, ctypes.c_void_p, ctypes.c_uint)
    pidfd_send_signal.restype = ctypes.c_int
    result = cast(int, pidfd_send_signal(pidfd, int(sig), None, 0))
    if result < 0:
        error_number = ctypes.get_errno()
        raise OSError(error_number, os.strerror(error_number))


def _wait_pidfds(pids: dict[int, tuple[str, str]], timeout: float) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        remaining = []
        for pid, (ticks, _cmdline) in pids.items():
            try:
                _parent, current_ticks, state = _proc_identity(pid)
            except FileNotFoundError:
                continue
            if current_ticks == ticks and state != "Z":
                remaining.append(pid)
        if not remaining:
            return True
        time.sleep(0.05)
    return False


def _terminate_owned_process(pid: int, ticks: str, pidfds: dict[int, int]) -> None:
    try:
        pidfd = _open_pidfd(pid, ticks)
    except OSError as exc:
        if _verified_process_exit_race(exc, pid, ticks):
            return
        raise
    pidfds[pid] = pidfd
    try:
        _pidfd_signal(pidfd, signal.SIGTERM)
    except OSError as exc:
        if not _verified_process_exit_race(exc, pid, ticks):
            raise
        os.close(pidfds.pop(pid))


def _cleanup_token_processes(token: str) -> None:
    owned = _token_pids(token)
    pidfds: dict[int, int] = {}
    try:
        for pid, (ticks, _cmdline) in owned.items():
            _terminate_owned_process(pid, ticks, pidfds)
        if not _wait_pidfds(owned, 2.0):
            for pidfd in pidfds.values():
                with suppress_process_gone():
                    _pidfd_signal(pidfd, signal.SIGKILL)
            if not _wait_pidfds(owned, 5.0):
                raise ValueError("a token-owned mutation process survived bounded cleanup")
        remaining = _token_pids(token)
        if remaining:
            raise ValueError("token-owned mutation processes remain after cleanup: " + ", ".join(map(str, remaining)))
    finally:
        for pidfd in pidfds.values():
            os.close(pidfd)


def _tmp_identity(path: Path) -> tuple[int, int]:
    details = path.lstat()
    if (
        not stat.S_ISDIR(details.st_mode)
        or details.st_uid != os.getuid()
        or stat.S_IMODE(details.st_mode) != PRIVATE_TMP_MODE
    ):
        raise ValueError("mutation scratch directory is not a private directory owned by this user")
    return details.st_dev, details.st_ino


def _tmp_exists(path: Path) -> bool:
    try:
        path.lstat()
    except FileNotFoundError:
        return False
    return True


def _prepare_job_tmp(job_root: Path, receipt: dict[str, object]) -> Path:
    path = job_root / "tmp"
    recorded = receipt.get("tmp_identity")
    if _tmp_exists(path):
        if recorded is None:
            raise ValueError("mutation scratch directory has no recorded owner identity")
    else:
        path.mkdir(mode=PRIVATE_TMP_MODE)
    identity = _tmp_identity(path)
    if recorded is not None and recorded != {"device": identity[0], "inode": identity[1]}:
        raise ValueError("mutation scratch directory identity changed")
    receipt["tmp_identity"] = {"device": identity[0], "inode": identity[1]}
    return path


def _remove_job_tmp(job_root: Path, receipt: dict[str, object]) -> None:
    path = job_root / "tmp"
    if not _tmp_exists(path):
        return
    identity = _tmp_identity(path)
    if receipt.get("tmp_identity") != {"device": identity[0], "inode": identity[1]}:
        raise ValueError("mutation scratch directory identity changed")
    shutil.rmtree(path)
    if _tmp_exists(path):
        raise ValueError("mutation scratch directory remains after cleanup")


def _record_tmp_cleanup_eligible(run: OwnerRun) -> None:
    run.receipt.update(
        {
            "cleanup_verified": True,
            "cleanup_verified_token": run.token,
            "cleanup_verified_identity": run.receipt.get("identity"),
        }
    )
    _write_owner(run.job_root, run.receipt)


def _tmp_cleanup_proved(receipt: dict[str, object], token: str) -> bool:
    return (
        receipt.get("cleanup_verified") is True
        and receipt.get("cleanup_verified_token") == token
        and receipt.get("cleanup_verified_identity") == receipt.get("identity")
    )


def _cleanup_verified_job_tmp(run: OwnerRun) -> None:
    _record_tmp_cleanup_eligible(run)
    _remove_job_tmp(run.job_root, run.receipt)


def _validate_private_tmp_owner_path(
    job_root: Path, jobs_root: Path, root_details: os.stat_result, owner_details: os.stat_result
) -> None:
    if (
        not stat.S_ISDIR(root_details.st_mode)
        or root_details.st_uid != os.getuid()
        or stat.S_IMODE(root_details.st_mode) != PRIVATE_TMP_MODE
        or job_root.parent.resolve(strict=True) != jobs_root.resolve(strict=True)
    ):
        raise ValueError("mutation scratch owner path is not a private direct job; preserving it")
    if not stat.S_ISREG(owner_details.st_mode) or owner_details.st_uid != os.getuid():
        raise ValueError("mutation scratch owner path is not a private direct job; preserving it")


def _recover_abandoned_job_tmp(job_root: Path) -> bool:
    """Remove only scratch whose exact owner receipt proves its process tree was reaped."""
    import fcntl

    jobs_root = AUDIT_ROOT / "jobs" / "omi-collector"
    try:
        root_details = job_root.lstat()
        owner_details = (job_root / OWNER_FILE).lstat()
    except OSError as exc:
        raise ValueError("mutation scratch owner path is unavailable; preserving it") from exc
    _validate_private_tmp_owner_path(job_root, jobs_root, root_details, owner_details)
    path = job_root / "tmp"
    if not _tmp_exists(path):
        return False
    lock = (job_root / OWNER_LOCK).open("a+b")
    try:
        try:
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise ValueError("mutation owner is still active; preserving its scratch directory") from exc
        receipt = _read_receipt_from(job_root / OWNER_FILE)
        if receipt is None:
            raise ValueError("mutation scratch has no owner receipt; preserving it")
        token = receipt.get("run_token")
        identity = receipt.get("identity")
        if (
            receipt.get("cleanup_verified") is not True
            or receipt.get("cleanup_verified_token") != token
            or receipt.get("cleanup_verified_identity") != identity
            or not isinstance(token, str)
        ):
            raise ValueError("previous mutation owner did not verify process cleanup; preserving scratch")
        _remove_job_tmp(job_root, receipt)
        return True
    finally:
        fcntl.flock(lock.fileno(), fcntl.LOCK_UN)
        lock.close()


def _mark_campaign_interrupted(checkout: Path, reason: str) -> None:
    campaign_path = checkout / RECEIPT
    if not campaign_path.is_file():
        return
    campaign = cast(dict[str, object], json.loads(campaign_path.read_text(encoding="utf-8")))
    if campaign.get("state") == "running":
        campaign.update(
            {
                "state": "interrupted",
                "exit_status": 130,
                "ended_at": datetime.now(UTC).isoformat(),
                "interruption_reason": reason,
            }
        )
        temporary = campaign_path.with_suffix(".tmp")
        temporary.write_text(json.dumps(campaign, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        temporary.replace(campaign_path)


def _stop_direct_launcher(
    process: subprocess.Popen[bytes], owned: dict[int, tuple[str, str]], pidfds: dict[int, int]
) -> None:
    if process.poll() is not None:
        return
    try:
        _parent, ticks, state = _proc_identity(process.pid)
    except FileNotFoundError:
        if process.poll() is not None:
            return
        raise
    if state == "Z":
        return
    try:
        pidfd = _open_pidfd(process.pid, ticks)
    except OSError as exc:
        if _verified_process_exit_race(exc, process.pid, ticks):
            return
        raise
    pidfds[process.pid] = pidfd
    owned[process.pid] = (ticks, "direct mutation launcher")
    try:
        _pidfd_signal(pidfd, signal.SIGSTOP)
    except OSError as exc:
        if not _verified_process_exit_race(exc, process.pid, ticks):
            raise
        os.close(pidfds.pop(process.pid))
        owned.pop(process.pid)


def _owned_tree_stopped(owned: dict[int, tuple[str, str]]) -> bool:
    for pid, (ticks, _cmdline) in owned.items():
        try:
            _parent, current_ticks, state = _proc_identity(pid)
        except FileNotFoundError:
            continue
        if current_ticks != ticks or state not in {"T", "t", "Z"}:
            return False
    return True


def _freeze_owned_tree(
    process: subprocess.Popen[bytes], token: str, owned: dict[int, tuple[str, str]], pidfds: dict[int, int]
) -> None:
    previous: set[int] = set()
    stable = 0
    for _ in range(20):
        root = owned.get(process.pid)
        current = _token_pids(token, process.pid, root[0]) if root is not None else _token_pids(token)
        stable = stable + 1 if current.keys() == previous else 0
        for pid, (ticks, cmdline) in current.items():
            if pid not in pidfds:
                try:
                    pidfd = _open_pidfd(pid, ticks)
                except OSError as exc:
                    if _verified_process_exit_race(exc, pid, ticks):
                        continue
                    raise
                pidfds[pid] = pidfd
                owned[pid] = (ticks, cmdline)
                try:
                    _pidfd_signal(pidfd, signal.SIGSTOP)
                except OSError as exc:
                    if not _verified_process_exit_race(exc, pid, ticks):
                        raise
                    os.close(pidfds.pop(pid))
                    owned.pop(pid)
        if stable >= STABLE_PROCESS_SCANS and _owned_tree_stopped(owned):
            return
        previous = set(current)
        time.sleep(PROCESS_SETTLE_SECONDS)
    raise ValueError("owned mutation process tree did not reach a stable stop")


def _kill_verify_reap(
    process: subprocess.Popen[bytes],
    token: str,
    checkout: Path,
    owned: dict[int, tuple[str, str]],
    pidfds: dict[int, int],
) -> dict[str, object]:
    ordered = sorted(owned.items(), key=lambda item: "--gremlins" not in item[1][1])
    for pid, _details in ordered:
        if pid in pidfds:
            with suppress_process_gone():
                _pidfd_signal(pidfds[pid], signal.SIGKILL)
    if not _wait_pidfds(owned, 5.0):
        raise ValueError("a token-owned mutation process survived checkpoint cleanup")
    try:
        process.wait(timeout=5)
    except subprocess.TimeoutExpired as exc:
        raise ValueError("direct mutation child did not reap after checkpoint cleanup") from exc
    remaining = _token_pids(token)
    if remaining:
        raise ValueError("token-owned processes remain after checkpoint cleanup: " + ", ".join(map(str, remaining)))
    return {
        "processes_stopped": sorted(owned),
        "cache": _verify_native_cache(checkout),
        "verified_at": datetime.now(UTC).isoformat(),
    }


def _signal_pidfds(pidfds: dict[int, int], sig: signal.Signals) -> None:
    for pidfd in pidfds.values():
        with suppress_process_gone():
            _pidfd_signal(pidfd, sig)


def _stop_owned_processes(process: subprocess.Popen[bytes], token: str, checkout: Path) -> dict[str, object]:
    """Stop the direct job launcher, then kill only verified token holders."""
    owned: dict[int, tuple[str, str]] = {}
    pidfds: dict[int, int] = {}
    killed = False
    try:
        _stop_direct_launcher(process, owned, pidfds)
        _freeze_owned_tree(process, token, owned, pidfds)
        killed = True
        return _kill_verify_reap(process, token, checkout, owned, pidfds)
    except (OSError, ValueError, ProcessLookupError) as exc:
        _signal_pidfds(pidfds, signal.SIGKILL if killed else signal.SIGCONT)
        if killed:
            _wait_pidfds(owned, 5.0)
        raise ValueError(f"could not verify safe mutation checkpoint: {exc}") from exc
    finally:
        for pidfd in pidfds.values():
            os.close(pidfd)


class suppress_process_gone:
    def __enter__(self) -> None:
        return None

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> bool:
        return isinstance(exc, OSError) and exc.errno == errno.ESRCH


def _verify_native_cache(checkout: Path) -> dict[str, object]:
    results = checkout / CACHE / "results.db"
    if not results.is_file():
        return {"results_db": "absent", "quick_check": "not-applicable"}
    with closing(sqlite3.connect(f"file:{results}?mode=ro", uri=True, timeout=5)) as connection:
        row = cast(tuple[str] | None, connection.execute("PRAGMA quick_check").fetchone())
    if row != ("ok",):
        raise ValueError(f"Gremlins results.db failed integrity check: {row!r}")
    return {"results_db": str(results), "quick_check": "ok"}


def _preflight_process_control(job_root: Path) -> None:
    token = secrets.token_hex(24)
    env = {**os.environ, JOB_TOKEN_ENV: token}
    probe = subprocess.Popen(
        [sys.executable, "-c", "import time; time.sleep(30)"],
        cwd=job_root / "checkout",
        env=env,
        start_new_session=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    pidfd: int | None = None
    try:
        deadline = time.monotonic() + 3
        while time.monotonic() < deadline:
            _parent, root_ticks, _state = _proc_identity(probe.pid)
            observed = _token_pids(token, probe.pid, root_ticks)
            if probe.pid in observed:
                pidfd = _open_pidfd(probe.pid, observed[probe.pid][0])
                break
            if probe.poll() is not None:
                break
            time.sleep(0.02)
        if pidfd is None:
            raise ValueError("separate-session process ownership is not visible through /proc/pidfd")
        _pidfd_signal(pidfd, signal.SIGKILL)
        probe.wait(timeout=3)
    finally:
        if pidfd is not None:
            os.close(pidfd)
        if probe.poll() is None:
            probe.kill()
            probe.wait(timeout=3)


def _check_snapshot_seal(run: OwnerRun) -> None:
    process = run.process
    assert process is not None
    try:
        if _snapshot_identity(run.checkout, run.run_env) == run.receipt.get("identity"):
            return
        reason = "snapshot-seal-changed"
        checkpoint = _stop_owned_processes(process, run.token, run.checkout)
        _cleanup_verified_job_tmp(run)
        _mark_campaign_interrupted(run.checkout, reason)
        run.receipt.update(
            {
                "state": "source_invalidated",
                "cleanup_verified": True,
                "checkpoint_verified": True,
                "checkpoint": checkpoint,
                "ended_at": datetime.now(UTC).isoformat(),
            }
        )
    except (OSError, ValueError, sqlite3.Error, json.JSONDecodeError) as exc:
        if _tmp_cleanup_proved(run.receipt, run.token):
            run.receipt.update({"state": "cleanup_failed", "error": str(exc), "cleanup_verified": True})
            _write_owner(run.job_root, run.receipt)
            run.invalidated.set()
            return
        try:
            checkpoint = _stop_owned_processes(process, run.token, run.checkout)
        except (OSError, ValueError, sqlite3.Error, json.JSONDecodeError) as cleanup_exc:
            run.receipt.update(
                {
                    "state": "cleanup_failed",
                    "cleanup_verified": False,
                    "error": f"seal check failed ({exc}); cleanup failed ({cleanup_exc})",
                }
            )
        else:
            _cleanup_verified_job_tmp(run)
            _mark_campaign_interrupted(run.checkout, "snapshot-seal-unreadable")
            run.receipt.update(
                {
                    "state": "source_invalidated",
                    "cleanup_verified": True,
                    "checkpoint_verified": True,
                    "checkpoint": checkpoint,
                    "error": str(exc),
                }
            )
    _write_owner(run.job_root, run.receipt)
    run.invalidated.set()


def _monitor_owner_child(run: OwnerRun) -> None:
    if OWNER_CHECK_SECONDS <= 0:
        raise ValueError("owner seal check interval must be positive")
    next_check = time.monotonic() + OWNER_CHECK_SECONDS
    assert run.process is not None
    while run.process.poll() is None and not run.requested_signal and not run.invalidated.is_set():
        if time.monotonic() >= next_check:
            with run.state_lock:
                _check_snapshot_seal(run)
            next_check = time.monotonic() + OWNER_CHECK_SECONDS
        time.sleep(0.2)


def _record_child_failure(job_root: Path, receipt: dict[str, object], error: Exception, *, state: str) -> int:
    receipt.update({"state": state, "error": str(error), "ended_at": datetime.now(UTC).isoformat()})
    receipt.setdefault("cleanup_verified", False)
    _write_owner(job_root, receipt)
    return 1


def _finish_signalled_child(run: OwnerRun, signum: int) -> int:
    assert run.process is not None
    try:
        checkpoint = _stop_owned_processes(run.process, run.token, run.checkout)
        _cleanup_verified_job_tmp(run)
        _mark_campaign_interrupted(run.checkout, "owner-signal")
    except (OSError, ValueError, sqlite3.Error, json.JSONDecodeError) as exc:
        return _record_child_failure(run.job_root, run.receipt, exc, state="cleanup_failed")
    run.receipt.update(
        {
            "state": "interrupted",
            "exit_status": 128 + signum,
            "cleanup_verified": True,
            "checkpoint_verified": True,
            "checkpoint": checkpoint,
            "ended_at": datetime.now(UTC).isoformat(),
        }
    )
    _write_owner(run.job_root, run.receipt)
    return 128 + signum


def _finish_completed_child(run: OwnerRun) -> int:
    assert run.process is not None
    try:
        status = run.process.wait(timeout=5)
        _cleanup_token_processes(run.token)
        _cleanup_verified_job_tmp(run)
        cache_check = _verify_native_cache(run.checkout)
        if (
            run.receipt.get("identity") is not None
            and _snapshot_identity(run.checkout, run.run_env) != run.receipt["identity"]
        ):
            raise ValueError("mutation snapshot seal changed before finalization")
        campaign = _read_receipt_from(run.checkout / RECEIPT)
    except (OSError, ValueError, sqlite3.Error, json.JSONDecodeError, subprocess.TimeoutExpired) as exc:
        run.receipt.update(
            {
                "state": "cleanup_failed",
                "checkpoint_verified": False,
                "exit_status": run.process.returncode,
                "error": str(exc),
                "ended_at": datetime.now(UTC).isoformat(),
            }
        )
        if not _tmp_cleanup_proved(run.receipt, run.token):
            run.receipt["cleanup_verified"] = False
        _write_owner(run.job_root, run.receipt)
        return 1
    if status == 0 and campaign is None:
        return _record_child_failure(
            run.job_root,
            run.receipt,
            ValueError("child exited successfully without a campaign receipt"),
            state="cleanup_failed",
        )
    if status == 0 and campaign and campaign.get("state") == "complete":
        final_state = "complete"
    elif status == UNRESOLVED_EXIT_STATUS and campaign and campaign.get("state") == "complete_unresolved":
        final_state = "complete_unresolved"
    elif campaign and campaign.get("state") in {"baseline_failed", "controller_failed", "interrupted"}:
        final_state = cast(str, campaign["state"])
    else:
        final_state = "failed"
    run.receipt.update(
        {
            "state": final_state,
            "exit_status": status,
            "ended_at": datetime.now(UTC).isoformat(),
            "checkpoint_verified": True,
            "cleanup_verified": True,
            "cache": cache_check,
            "campaign": campaign,
        }
    )
    _write_owner(run.job_root, run.receipt)
    return status


def _finish_owner_child(run: OwnerRun) -> int:
    assert run.process is not None
    with run.state_lock:
        if run.paused.is_set():
            return 130
        if run.invalidated.is_set() or run.receipt.get("state") == "control_failed":
            return 1
        if run.requested_signal:
            return _finish_signalled_child(run, run.requested_signal[0])
        return _finish_completed_child(run)


def _request_pause(run: OwnerRun) -> bool:
    with run.state_lock:
        process = run.process
        if run.paused.is_set() or process is None or process.poll() is not None:
            return run.paused.is_set()
        run.receipt.update({"state": "pausing", "pause_requested_at": datetime.now(UTC).isoformat()})
        _write_owner(run.job_root, run.receipt)
        try:
            checkpoint = _stop_owned_processes(process, run.token, run.checkout)
            _cleanup_verified_job_tmp(run)
            _mark_campaign_interrupted(run.checkout, "checkpoint-stop")
        except (OSError, ValueError, sqlite3.Error, json.JSONDecodeError) as exc:
            run.receipt.update({"state": "control_failed", "checkpoint_verified": False, "error": str(exc)})
            _write_owner(run.job_root, run.receipt)
            return False
        run.receipt.update(
            {
                "state": "paused",
                "checkpoint_verified": True,
                "checkpoint": checkpoint,
                "ended_at": datetime.now(UTC).isoformat(),
            }
        )
        _write_owner(run.job_root, run.receipt)
        run.paused.set()
        return True


def _prepare_owner(job_root: Path, run_token: str) -> tuple[BinaryIO, dict[str, object], Path]:
    import fcntl

    job_root.mkdir(parents=True, exist_ok=True, mode=0o700)
    job_root.chmod(0o700)
    lock_path = job_root / OWNER_LOCK
    lock = lock_path.open("a+b")
    fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    receipt_path = job_root / OWNER_FILE
    try:
        receipt = cast(dict[str, object], json.loads(receipt_path.read_text(encoding="utf-8")))
    except (OSError, json.JSONDecodeError) as exc:
        lock.close()
        raise ValueError("mutation job has no valid owner receipt") from exc
    if receipt.get("schema") != 1 or receipt.get("run_token") != run_token:
        lock.close()
        raise ValueError("mutation owner receipt identity does not match launch")
    socket_path = control_socket_path(job_root)
    try:
        remove_stale_socket(socket_path)
    except OSError, ScopeError:
        fcntl.flock(lock.fileno(), fcntl.LOCK_UN)
        lock.close()
        raise
    receipt["control_socket"] = str(socket_path)
    try:
        _preflight_process_control(job_root)
    except (OSError, ValueError, subprocess.SubprocessError, TimeoutError) as exc:
        receipt.update(
            {
                "state": "controller_failed",
                "cleanup_verified": True,
                "error": f"process-control preflight failed: {exc}",
                "ended_at": datetime.now(UTC).isoformat(),
            }
        )
        _write_owner(job_root, receipt)
        fcntl.flock(lock.fileno(), fcntl.LOCK_UN)
        lock.close()
        raise
    return lock, receipt, socket_path


def _enter_owner(
    job_root: Path,
    mode: str,
    run_token: str,
    *,
    command: list[str] | None = None,
    environment: dict[str, str] | None = None,
) -> int:
    import fcntl

    run_env = _owner_run_environment(job_root, environment)
    lock, receipt, socket_path = _prepare_owner(job_root, run_token)
    checkout = job_root / "checkout"
    token = run_token
    child_env = {**run_env, JOB_TOKEN_ENV: token}
    actual_command = command or ["uv", "run", "--frozen", "--no-sync", "just", "mutation-internal", mode]
    run = OwnerRun(job_root, mode, token, checkout, run_env, receipt)
    previous_handlers: dict[int, signal.Handlers | int | Callable[[int, FrameType | None], object] | None] = {}

    server = OwnerControlServer(socket_path, run_token, lambda: run.receipt.copy(), lambda: _request_pause(run))
    try:
        _prepare_owner_tmp(run, mode)
        if threading.current_thread() is threading.main_thread():

            def request_stop(signum: int, _frame: object) -> None:
                run.requested_signal.append(signum)

            for signum in (signal.SIGINT, signal.SIGTERM):
                previous_handlers[signum] = signal.getsignal(signum)
                signal.signal(signum, request_stop)
        server.start()
        run.process = subprocess.Popen(
            actual_command,
            cwd=checkout,
            env=child_env,
            start_new_session=True,
            stdout=None,
            stderr=None,
        )
        receipt.update({"child_pid": run.process.pid, "child_start_ticks": _proc_identity(run.process.pid)[1]})
        _write_owner(job_root, receipt)
        _monitor_owner_child(run)
        return _finish_owner_child(run)
    except (
        OSError,
        ValueError,
        sqlite3.Error,
        json.JSONDecodeError,
        subprocess.SubprocessError,
        TimeoutError,
        ScopeError,
    ) as exc:
        cleanup_error: str | None = None
        try:
            if run.process is not None and run.process.poll() is None:
                _stop_owned_processes(run.process, token, checkout)
                _cleanup_verified_job_tmp(run)
                _mark_campaign_interrupted(checkout, "owner-exception")
            else:
                _cleanup_token_processes(token)
                _cleanup_verified_job_tmp(run)
        except (OSError, ValueError, sqlite3.Error, json.JSONDecodeError) as cleanup_exc:
            cleanup_error = str(cleanup_exc)
        receipt.update(
            {
                "state": "cleanup_failed" if cleanup_error else "controller_failed",
                "cleanup_verified": _tmp_cleanup_proved(receipt, token) or cleanup_error is None,
                "error": str(exc) if cleanup_error is None else f"{exc}; cleanup failed: {cleanup_error}",
                "ended_at": datetime.now(UTC).isoformat(),
            }
        )
        _write_owner(job_root, receipt)
        return 1
    finally:
        server.stop()
        for signum, previous in previous_handlers.items():
            signal.signal(signum, previous)
        if lock and not run.paused.is_set():
            fcntl.flock(lock.fileno(), fcntl.LOCK_UN)
        lock.close()


def _owner_run_environment(job_root: Path, environment: dict[str, str] | None) -> dict[str, str]:
    run_env = dict(environment or os.environ)
    if JOB_TOKEN_ENV in run_env:
        raise ValueError("owner environment must not contain the private child token")
    run_env.update(
        {
            "UV_PROJECT_ENVIRONMENT": str(job_root / ".venv"),
            "UV_CACHE_DIR": str(job_root / "uv-cache"),
            "UV_LINK_MODE": "hardlink",
            "UV_NO_SYNC": "1",
            "TMPDIR": str(job_root / "tmp"),
        }
    )
    return run_env


def _prepare_owner_tmp(run: OwnerRun, mode: str) -> None:
    run.receipt.update(
        {
            "state": "running",
            "mode": mode,
            "owner_pid": os.getpid(),
            "started_at": datetime.now(UTC).isoformat(),
            "cleanup_verified": False,
        }
    )
    run.receipt.pop("cleanup_verified_token", None)
    run.receipt.pop("cleanup_verified_identity", None)
    _prepare_job_tmp(run.job_root, run.receipt)
    _write_owner(run.job_root, run.receipt)


def _read_receipt_from(path: Path) -> dict[str, object] | None:
    if not path.is_file():
        return None
    value = cast(object, json.loads(path.read_text(encoding="utf-8")))
    if not isinstance(value, dict):
        raise ValueError("campaign receipt is not an object")
    return cast(dict[str, object], value)


def _canonicalize_snapshot_modes(checkout: Path) -> None:
    for relative, expected_mode in (
        (RELEASE_WRAPPER, RELEASE_WRAPPER_MODE),
        (RELEASE_SUDOERS, RELEASE_SUDOERS_MODE),
    ):
        path = checkout / relative
        if not path.is_file() or path.is_symlink():
            raise ValueError(f"mutation snapshot is missing a regular tracked path: {relative}")
        path.chmod(expected_mode)


def _snapshot_identity(checkout: Path, environment: dict[str, str]) -> dict[str, object]:
    dirty = subprocess.run(
        ["git", "status", "--porcelain", "--untracked-files=all"],
        cwd=checkout,
        check=True,
        capture_output=True,
        text=True,
    ).stdout
    if dirty:
        raise ValueError("the frozen mutation snapshot has tracked or untracked changes")
    env = {key: environment.get(key, "") for key in RELEVANT_ENV}
    expected = {"COVERAGE_CORE": "ctrace", "COVERAGE_FILE": "", "PYTEST_ADDOPTS": "", "UV_LINK_MODE": "hardlink"}
    invalid = [key for key, value in expected.items() if env[key] != value]
    if invalid:
        raise ValueError("campaign environment has invalid values for: " + ", ".join(invalid))
    digest = _tracked_inputs_digest(checkout)
    lock = (checkout / "uv.lock").read_bytes()
    project = (checkout / "pyproject.toml").read_bytes()
    return {
        "commit": subprocess.run(
            ["git", "rev-parse", "HEAD"], cwd=checkout, check=True, capture_output=True, text=True
        ).stdout.strip(),
        "tree": subprocess.run(
            ["git", "rev-parse", "HEAD^{tree}"], cwd=checkout, check=True, capture_output=True, text=True
        ).stdout.strip(),
        "uv_lock_sha256": hashlib.sha256(lock).hexdigest(),
        "pyproject_sha256": hashlib.sha256(project).hexdigest(),
        "tracked_inputs_sha256": digest,
        "environment": env,
        "environment_sha256": hashlib.sha256(json.dumps(env, sort_keys=True).encode()).hexdigest(),
    }


def _tracked_inputs_digest(checkout: Path) -> str:
    tracked = subprocess.run(
        [
            "git",
            "ls-files",
            "-s",
        ],
        cwd=checkout,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.splitlines()
    digest = hashlib.sha256()
    for entry in tracked:
        metadata, relative = entry.split("\t", 1)
        mode = metadata.split()[0]
        path = checkout / relative
        if mode == "160000":
            raise ValueError(f"mutation inputs cannot include a submodule: {relative}")
        actual_stat = path.lstat()
        actual_mode = stat.S_IMODE(actual_stat.st_mode)
        if mode == "120000":
            if not path.is_symlink():
                raise ValueError(f"tracked symlink input changed type: {relative}")
            content = os.fsencode(path.readlink())
        elif mode in {"100644", "100755"}:
            if not path.is_file() or path.is_symlink():
                raise ValueError(f"tracked regular input changed type: {relative}")
            content = path.read_bytes()
        else:
            raise ValueError(f"mutation input has an unsupported Git mode: {relative}")
        if relative == RELEASE_WRAPPER and actual_mode != RELEASE_WRAPPER_MODE:
            raise ValueError(f"release wrapper mode changed: {actual_mode:o}")
        if relative == RELEASE_SUDOERS and actual_mode != RELEASE_SUDOERS_MODE:
            raise ValueError(f"release sudoers mode changed: {actual_mode:o}")
        digest.update(mode.encode())
        digest.update(f"{actual_mode:o}".encode())
        digest.update(b"\0")
        digest.update(relative.encode())
        digest.update(b"\0")
        digest.update(hashlib.sha256(content).digest())
    return digest.hexdigest()


def _job_environment(job_root: Path) -> dict[str, str]:
    env = dict(os.environ)
    for key in (
        "COVERAGE_PROCESS_START",
        "COVERAGE_RCFILE",
        "PYTHONHOME",
        "PYTHONPATH",
        "PYTEST_DISABLE_PLUGIN_AUTOLOAD",
        "PYTEST_PLUGINS",
        "PYTEST_TIMEOUT",
        "VIRTUAL_ENV",
        JOB_TOKEN_ENV,
    ):
        env.pop(key, None)
    env.update(
        {
            "COVERAGE_CORE": "ctrace",
            "COVERAGE_FILE": "",
            "PYTEST_ADDOPTS": "",
            "LC_ALL": "C.UTF-8",
            "TZ": "UTC",
            "TMPDIR": str(job_root / "tmp"),
            "UV_CACHE_DIR": str(job_root / "uv-cache"),
            "UV_PROJECT_ENVIRONMENT": str(job_root / ".venv"),
            "UV_LINK_MODE": "hardlink",
            "UV_NO_SYNC": "1",
        }
    )
    return env


def _sync_job_environment(job_root: Path, environment: dict[str, str] | None = None) -> None:
    checkout = job_root / "checkout"
    env = dict(environment or _job_environment(job_root))
    env["UV_PROJECT_ENVIRONMENT"] = str(job_root / ".venv")
    env["UV_CACHE_DIR"] = str(job_root / "uv-cache")
    env["UV_LINK_MODE"] = "hardlink"
    env.pop("UV_NO_SYNC", None)
    env.pop("TMPDIR", None)
    Path(env["UV_CACHE_DIR"]).mkdir(parents=True, exist_ok=True)
    subprocess.run(
        ["uv", "sync", "--frozen", "--project", str(checkout)],
        cwd=checkout,
        env=env,
        check=True,
    )


def _new_job(repo: Path) -> Path:
    dirty = subprocess.run(
        ["git", "status", "--porcelain", "--untracked-files=all"],
        cwd=repo,
        check=True,
        capture_output=True,
        text=True,
    ).stdout
    if dirty:
        raise ValueError("refusing to audit a snapshot while the invoking checkout has uncommitted changes")
    commit = subprocess.run(
        ["git", "rev-parse", "HEAD"], cwd=repo, check=True, capture_output=True, text=True
    ).stdout.strip()
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    job_root = AUDIT_ROOT / "jobs" / "omi-collector" / f"{stamp}-{commit[:12]}-{secrets.token_hex(3)}"
    job_root.mkdir(parents=True, mode=0o700)
    checkout = job_root / "checkout"
    subprocess.run(["git", "worktree", "add", "--detach", str(checkout), commit], cwd=repo, check=True)
    _canonicalize_snapshot_modes(checkout)
    status = subprocess.run(
        ["git", "status", "--porcelain", "--untracked-files=all"],
        cwd=checkout,
        check=True,
        capture_output=True,
        text=True,
    ).stdout
    if status:
        raise ValueError("canonical mutation snapshot is not clean: " + status.strip())
    _sync_job_environment(job_root)
    return job_root


def _validate_resume(job_root: Path, environment: dict[str, str]) -> None:
    owner = _read_receipt_from(job_root / OWNER_FILE)
    if owner is None or owner.get("state") not in {"paused", "interrupted"}:
        raise ValueError("resume requires one verified paused or interrupted job")
    if owner.get("state") == "paused" and owner.get("checkpoint_verified") is not True:
        raise ValueError("paused job has no verified checkpoint")
    if owner.get("state") == "interrupted" and owner.get("cleanup_verified") is not True:
        raise ValueError("interrupted job has no verified child cleanup")
    identity = owner.get("identity")
    current = _snapshot_identity(job_root / "checkout", environment)
    if not isinstance(identity, dict):
        raise ValueError("resume identity receipt is invalid")
    previous = cast(dict[str, object], identity)
    if previous != current:
        differing = sorted(key for key in previous.keys() | current.keys() if previous.get(key) != current.get(key))
        prior_env_value = previous.get("environment", {})
        current_env = current.get("environment", {})
        if not isinstance(prior_env_value, dict) or not isinstance(current_env, dict):
            raise ValueError("resume identity environment is invalid")
        prior_env = cast(dict[str, str], prior_env_value)
        current_env = cast(dict[str, str], current_env)
        env_keys = sorted(
            key for key in set(prior_env) | set(current_env) if prior_env.get(key) != current_env.get(key)
        )
        detail = ", ".join(differing)
        if env_keys:
            detail += "; environment keys: " + ", ".join(env_keys)
        raise ValueError("resume identity does not match the frozen snapshot and runtime environment: " + detail)
    _verify_native_cache(job_root / "checkout")


def _launch_locked(mode: str) -> int:
    jobs_root = AUDIT_ROOT / "jobs" / "omi-collector"
    for owner_path in jobs_root.glob("*/" + OWNER_FILE):
        if owner_path.is_file():
            _recover_abandoned_job_tmp(owner_path.parent)
    if mode == "fresh":
        active_states = {"running", "pausing", "control_failed", "cleanup_failed", "source_invalidated"}
        active = [
            owner
            for owner in jobs_root.glob("*/" + OWNER_FILE)
            if owner.is_file()
            and (receipt := _read_receipt_from(owner)) is not None
            and receipt.get("state") in active_states
        ]
        if active:
            raise ValueError("an existing mutation job must be resolved before starting a fresh audit")
        job_root = _new_job(Path.cwd().resolve())
    else:
        jobs = [
            path
            for path in sorted(jobs_root.glob("*/" + OWNER_FILE))
            if path.is_file()
            and _read_receipt_from(path) is not None
            and cast(dict[str, object], _read_receipt_from(path)).get("state") in {"paused", "interrupted"}
        ]
        if len(jobs) != 1:
            raise ValueError(f"resume requires exactly one paused mutation job; found {len(jobs)}")
        job_root = jobs[0].parent
        _validate_resume(job_root, _job_environment(job_root))
    run_token = secrets.token_hex(32)
    _write_owner(
        job_root,
        {
            "schema": 1,
            "run_token": run_token,
            "state": "preparing",
            "mode": mode,
            "commit": subprocess.run(
                ["git", "rev-parse", "HEAD"], cwd=job_root / "checkout", check=True, capture_output=True, text=True
            ).stdout.strip(),
            "tree": subprocess.run(
                ["git", "rev-parse", "HEAD^{tree}"],
                cwd=job_root / "checkout",
                check=True,
                capture_output=True,
                text=True,
            ).stdout.strip(),
            "identity": _snapshot_identity(job_root / "checkout", _job_environment(job_root)),
            "control_socket": str(control_socket_path(job_root)),
            "created_at": datetime.now(UTC).isoformat(),
        },
    )
    return _enter_owner(job_root, mode, run_token, environment=_job_environment(job_root))


def _launch(mode: str) -> int:
    import fcntl

    jobs_root = AUDIT_ROOT / "jobs" / "omi-collector"
    jobs_root.mkdir(parents=True, exist_ok=True, mode=0o700)
    lock_path = jobs_root / ".launch.lock"
    with lock_path.open("a+b") as launcher_lock:
        try:
            fcntl.flock(launcher_lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise ValueError("another full-project mutation audit is already owned") from exc
        return _launch_locked(mode)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    prepare = commands.add_parser("prepare")
    prepare.add_argument("--fresh", action="store_true")
    prepare.add_argument("--launcher-pid", type=int, required=True)
    finish = commands.add_parser("finish")
    finish.add_argument("--status", type=int, required=True)
    launch = commands.add_parser("launch")
    launch.add_argument("--mode", choices=("fresh", "resume"), required=True)
    commands.add_parser("identity")
    args = parser.parse_args()
    command = cast(str, args.command)
    fresh = cast(bool, getattr(args, "fresh", False))
    launcher_pid = cast(int, getattr(args, "launcher_pid", 0))
    status = cast(int, getattr(args, "status", 0))
    try:
        if command == "prepare":
            print(_prepare(fresh, launcher_pid))
        elif command == "finish":
            return _finish(status)
        elif command == "identity":
            print(json.dumps(_identity(), sort_keys=True))
        else:
            return _launch(cast(str, args.mode))
    except (OSError, subprocess.SubprocessError, ValueError, json.JSONDecodeError, ScopeError) as exc:
        print(f"mutation campaign: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
