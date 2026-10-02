"""Guard and reconcile the one native full-project mutation campaign."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import subprocess
import sys
import time
from datetime import UTC, datetime
from importlib.metadata import PackageNotFoundError, distribution
from pathlib import Path
from typing import cast

CACHE = Path(".gremlins_cache")
RECEIPT = CACHE / "campaign.json"
REPORT = Path("coverage/gremlins/gremlins.json")
NATIVE_CACHE = (CACHE / "results.db", CACHE / "coverage.json", CACHE / "coverage.sqlite")
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
    "UV_CACHE_DIR",
    "UV_PROJECT_ENVIRONMENT",
    "VIRTUAL_ENV",
)


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
    uv_version = subprocess.run([uv_path or "uv", "--version"], check=True, capture_output=True, text=True).stdout.strip()
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
            raise ValueError("completed report contains unresolved outcomes; adjudicate it before starting another audit")
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
    except (OSError, IndexError, ValueError):
        return None


def _assert_no_live_campaign_processes(started_at_ns: int, launcher: dict[str, object]) -> None:
    try:
        boot_time = int(next(line.split()[1] for line in Path("/proc/stat").read_text().splitlines() if line.startswith("btime ")))
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
        except (OSError, TypeError, ValueError):
            continue
        if ticks >= first_tick and cwd == root and any(token in command for token in ("pytest", "forkserver", "spawn_main")):
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
        receipt["state"] = "interrupted" if status in (124, 137) else "failed"
        _write_receipt(receipt)
        return 0
    try:
        result = _postflight(receipt)
        receipt["state"] = result["state"]
        receipt["report"] = result["report"]
        print(f"Native campaign report reconciled: {result['mutant_count']} mutants across {result['source_file_count']} source files.")
        if result["state"] == "complete_unresolved":
            report = cast(dict[str, object], result["report"])
            counts = cast(dict[str, int], report["status_counts"])
            print(f"Unresolved native outcomes remain: timeout={counts['timeout']} error={counts['error']}; review required.", file=sys.stderr)
    except (OSError, subprocess.SubprocessError, ValueError, json.JSONDecodeError) as exc:
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
    if (
        not REPORT.is_file()
        or REPORT.stat().st_mtime_ns < started_at_ns
        or REPORT.stat().st_mtime_ns == previous_mtime
    ):
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
    file_counts = {_relative_report_path(str(path)): value.get("total") for path, value in files.items() if isinstance(value, dict)}
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
        if source.endswith(".py") and not Path(source).name.startswith("test_") and not Path(source).name.endswith("_test.py") and Path(source).name != "conftest.py"
    }
    if set(source_paths) != expected:
        raise ValueError("native discovered source-file scope does not match committed QA targets")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    prepare = commands.add_parser("prepare")
    prepare.add_argument("--fresh", action="store_true")
    prepare.add_argument("--launcher-pid", type=int, required=True)
    finish = commands.add_parser("finish")
    finish.add_argument("--status", type=int, required=True)
    args = parser.parse_args()
    command = cast(str, args.command)
    fresh = cast(bool, getattr(args, "fresh", False))
    launcher_pid = cast(int, getattr(args, "launcher_pid", 0))
    status = cast(int, getattr(args, "status", 0))
    try:
        if command == "prepare":
            print(_prepare(fresh, launcher_pid))
        else:
            return _finish(status)
    except (OSError, subprocess.SubprocessError, ValueError, json.JSONDecodeError) as exc:
        print(f"mutation campaign: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
