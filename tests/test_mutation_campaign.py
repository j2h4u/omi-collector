from __future__ import annotations

import json
import os
import subprocess
import sys
import time
from contextlib import redirect_stderr, redirect_stdout
from io import StringIO
from pathlib import Path
from subprocess import CompletedProcess
from typing import cast

import pytest
from scripts import mutation_campaign

SCRIPT = Path(__file__).parents[1] / "scripts" / "mutation_campaign.py"


@pytest.fixture
def campaign_repo(tmp_path: Path) -> tuple[Path, dict[str, str]]:
    (tmp_path / ".gitignore").write_text(".gremlins_cache/\ncoverage/gremlins/\n", encoding="utf-8")
    (tmp_path / "uv.lock").write_text("version = 1\n", encoding="utf-8")
    (tmp_path / "pyproject.toml").write_text(
        "[project]\nname = 'campaign-fixture'\nversion = '0.1.0'\n", encoding="utf-8"
    )
    (tmp_path / "src" / "omi_collector").mkdir(parents=True)
    (tmp_path / "src" / "omi_collector" / "demo.py").write_text("value = 1\n", encoding="utf-8")
    (tmp_path / "scripts").mkdir()
    (tmp_path / "scripts" / "demo.py").write_text("value = 2\n", encoding="utf-8")
    subprocess.run(["git", "init", "--quiet"], cwd=tmp_path, check=True)
    subprocess.run(["git", "config", "user.name", "Campaign test"], cwd=tmp_path, check=True)
    subprocess.run(["git", "config", "user.email", "campaign-test@example.invalid"], cwd=tmp_path, check=True)
    subprocess.run(["git", "add", "."], cwd=tmp_path, check=True)
    subprocess.run(["git", "commit", "--quiet", "-m", "campaign fixture"], cwd=tmp_path, check=True)
    env = os.environ.copy()
    env.update(COVERAGE_CORE="ctrace", PYTEST_ADDOPTS="", UV_LINK_MODE="hardlink")
    return tmp_path, env


def _campaign(repo: Path, env: dict[str, str], *args: str) -> CompletedProcess[str]:
    if args and args[0] == "prepare" and "--launcher-pid" not in args:
        args = (*args, "--launcher-pid", "2147483647")
    stdout, stderr = StringIO(), StringIO()
    with pytest.MonkeyPatch.context() as patch:
        patch.chdir(repo)
        for key in mutation_campaign.RELEVANT_ENV:
            if key in env:
                patch.setenv(key, env[key])
            else:
                patch.delenv(key, raising=False)
        patch.setattr(sys, "argv", [str(SCRIPT), *args])
        with redirect_stdout(stdout), redirect_stderr(stderr):
            status = mutation_campaign.main()
    return CompletedProcess([str(SCRIPT), *args], status, stdout.getvalue(), stderr.getvalue())


def _write_report(repo: Path, *, duplicate: bool = False) -> None:
    report = repo / "coverage" / "gremlins" / "gremlins.json"
    report.parent.mkdir(parents=True)
    ids = ["g-001"]
    results = [{"gremlin_id": "g-001", "file_path": str(repo / "src/omi_collector/demo.py"), "status": "zapped"}]
    files = {str(repo / "src/omi_collector/demo.py"): {"total": 1, "zapped": 1, "survived": 0, "percentage": 100.0}}
    if duplicate:
        ids.append("g-001")
        results.append(results[0])
        files[str(repo / "src/omi_collector/demo.py")]["total"] = 2
    report.write_text(
        json.dumps(
            {
                "scope": {
                    "source_files": ["scripts/demo.py", "src/omi_collector/demo.py"],
                    "gremlin_ids": ids,
                    "generation_errors": [],
                },
                "summary": {
                    "total": len(ids),
                    "zapped": len(ids),
                    "survived": 0,
                    "timeout": 0,
                    "error": 0,
                    "pardoned": 0,
                },
                "files": files,
                "results": results,
            }
        ),
        encoding="utf-8",
    )
    future = time.time_ns() + 1_000_000
    os.utime(report, ns=(future, future))


def test_timeout_resume_keeps_campaign_and_native_cache(campaign_repo: tuple[Path, dict[str, str]]) -> None:
    repo, env = campaign_repo
    (repo / ".gremlins_cache").mkdir()
    (repo / ".gremlins_cache" / "results.db").write_text("persisted verdict", encoding="utf-8")
    # First create a campaign receipt before there is a native cache, as the real recipe does.
    (repo / ".gremlins_cache" / "results.db").unlink()
    assert _campaign(repo, env, "prepare").returncode == 0
    (repo / ".gremlins_cache" / "results.db").write_text("persisted verdict", encoding="utf-8")
    assert _campaign(repo, env, "finish", "--status", "124").returncode == 0
    resumed = _campaign(repo, env, "prepare")
    assert resumed.returncode == 0, resumed.stderr
    assert (repo / ".gremlins_cache" / "results.db").read_text(encoding="utf-8") == "persisted verdict"
    receipt = cast(
        dict[str, object],
        json.loads((repo / ".gremlins_cache" / "campaign.json").read_text(encoding="utf-8")),
    )
    assert receipt["mode"] == "resume"


def test_fresh_is_explicit_and_does_not_delete_cache_itself(campaign_repo: tuple[Path, dict[str, str]]) -> None:
    repo, env = campaign_repo
    (repo / ".gremlins_cache").mkdir()
    (repo / ".gremlins_cache" / "results.db").write_text("old", encoding="utf-8")
    assert _campaign(repo, env, "prepare").returncode != 0
    fresh = _campaign(repo, env, "prepare", "--fresh")
    assert fresh.returncode == 0, fresh.stderr
    assert (repo / ".gremlins_cache" / "results.db").read_text(encoding="utf-8") == "old"


def test_inherited_coverage_file_is_rejected(campaign_repo: tuple[Path, dict[str, str]]) -> None:
    repo, env = campaign_repo
    env["COVERAGE_FILE"] = "/tmp/unrelated-coverage"
    result = _campaign(repo, env, "prepare")
    assert result.returncode != 0
    assert "COVERAGE_FILE" in result.stderr


def test_changed_committed_identity_refuses_resume(campaign_repo: tuple[Path, dict[str, str]]) -> None:
    repo, env = campaign_repo
    assert _campaign(repo, env, "prepare").returncode == 0
    (repo / "uv.lock").write_text("changed\n", encoding="utf-8")
    result = _campaign(repo, env, "prepare")
    assert result.returncode != 0
    assert "clean and committed" in result.stderr


@pytest.mark.parametrize("duplicate", [False, True])
def test_postflight_rejects_stale_or_duplicate_native_report(
    campaign_repo: tuple[Path, dict[str, str]], duplicate: bool
) -> None:
    repo, env = campaign_repo
    assert _campaign(repo, env, "prepare").returncode == 0
    if duplicate:
        _write_report(repo, duplicate=True)
        result = _campaign(repo, env, "finish", "--status", "0")
        assert result.returncode != 0
        assert "duplicate" in result.stderr or "do not match" in result.stderr
    else:
        result = _campaign(repo, env, "finish", "--status", "0")
        assert result.returncode != 0
        assert "fresh native JSON report" in result.stderr


def test_postflight_rejects_old_report_even_when_its_timestamp_is_future(
    campaign_repo: tuple[Path, dict[str, str]],
) -> None:
    repo, env = campaign_repo
    _write_report(repo)
    report = repo / "coverage" / "gremlins" / "gremlins.json"
    os.utime(report, ns=(4_000_000_000_000_000_000, 4_000_000_000_000_000_000))
    assert _campaign(repo, env, "prepare").returncode == 0
    result = _campaign(repo, env, "finish", "--status", "0")
    assert result.returncode != 0
    assert "fresh native JSON report" in result.stderr
    receipt = cast(
        dict[str, object],
        json.loads((repo / ".gremlins_cache" / "campaign.json").read_text(encoding="utf-8")),
    )
    assert receipt["state"] == "failed"
    assert "ended_at" in receipt


def test_status_137_is_recorded_and_only_resumes_on_a_new_invocation(
    campaign_repo: tuple[Path, dict[str, str]],
) -> None:
    repo, env = campaign_repo
    assert _campaign(repo, env, "prepare").returncode == 0
    assert _campaign(repo, env, "finish", "--status", "137").returncode == 0
    result = _campaign(repo, env, "prepare")
    assert result.returncode == 0, result.stderr
    receipt = cast(
        dict[str, object],
        json.loads((repo / ".gremlins_cache" / "campaign.json").read_text(encoding="utf-8")),
    )
    assert receipt["mode"] == "resume"
    assert receipt["exit_status"] is None


def test_resume_refuses_live_recorded_launcher(campaign_repo: tuple[Path, dict[str, str]]) -> None:
    repo, env = campaign_repo
    assert _campaign(repo, env, "prepare", "--launcher-pid", str(os.getpid())).returncode == 0
    assert _campaign(repo, env, "finish", "--status", "124").returncode == 0
    result = _campaign(repo, env, "prepare")
    assert result.returncode != 0
    assert "recorded campaign launcher" in result.stderr


def test_postflight_subprocess_failure_records_terminal_receipt(
    campaign_repo: tuple[Path, dict[str, str]], monkeypatch: pytest.MonkeyPatch
) -> None:
    repo, env = campaign_repo
    assert _campaign(repo, env, "prepare").returncode == 0

    def fail_identity() -> dict[str, object]:
        raise subprocess.CalledProcessError(1, ["git", "status"])

    monkeypatch.setattr(mutation_campaign, "_identity", fail_identity)
    result = _campaign(repo, env, "finish", "--status", "0")
    assert result.returncode != 0
    receipt = cast(
        dict[str, object],
        json.loads((repo / ".gremlins_cache" / "campaign.json").read_text(encoding="utf-8")),
    )
    assert receipt["state"] == "failed"
    assert "returned non-zero exit status 1" in cast(str, receipt["postflight_error"])


def test_native_errors_and_timeouts_remain_unresolved(campaign_repo: tuple[Path, dict[str, str]]) -> None:
    repo, env = campaign_repo
    assert _campaign(repo, env, "prepare").returncode == 0
    _write_report(repo)
    report = repo / "coverage" / "gremlins" / "gremlins.json"
    data = cast(dict[str, object], json.loads(report.read_text(encoding="utf-8")))
    results = cast(list[dict[str, object]], data["results"])
    summary = cast(dict[str, object], data["summary"])
    results[0]["status"] = "timeout"
    summary["zapped"] = 0
    summary["timeout"] = 1
    report.write_text(json.dumps(data), encoding="utf-8")
    future = time.time_ns() + 1_000_000
    os.utime(report, ns=(future, future))
    assert _campaign(repo, env, "finish", "--status", "0").returncode == 3
    receipt = cast(
        dict[str, object],
        json.loads((repo / ".gremlins_cache" / "campaign.json").read_text(encoding="utf-8")),
    )
    assert receipt["state"] == "complete_unresolved"
    report_data = cast(dict[str, object], receipt["report"])
    status_counts = cast(dict[str, int], report_data["status_counts"])
    assert status_counts["timeout"] == 1
