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


def _write_status_report(repo: Path, statuses: list[str]) -> None:
    _write_report(repo)
    report = repo / "coverage" / "gremlins" / "gremlins.json"
    data = cast(dict[str, object], json.loads(report.read_text(encoding="utf-8")))
    ids = [f"g-{index:03}" for index in range(len(statuses))]
    source = str(repo / "src/omi_collector/demo.py")
    data["scope"] = {
        "source_files": ["scripts/demo.py", "src/omi_collector/demo.py"],
        "gremlin_ids": ids,
        "generation_errors": [],
    }
    data["results"] = [
        {"gremlin_id": gremlin_id, "file_path": source, "status": status}
        for gremlin_id, status in zip(ids, statuses, strict=True)
    ]
    data["summary"] = {
        "total": len(ids),
        **{status: statuses.count(status) for status in ("zapped", "survived", "timeout", "error", "pardoned")},
    }
    data["files"] = {source: {"total": len(ids)}}
    report.write_text(json.dumps(data), encoding="utf-8")
    future = time.time_ns() + 1_000_000
    os.utime(report, ns=(future, future))


def _corrupt_report(report: dict[str, object], corruption: str, repo: Path) -> None:
    scope = cast(dict[str, object], report["scope"])
    results = cast(list[dict[str, object]], report["results"])
    source = str(repo / "src/omi_collector/demo.py")
    files = cast(dict[str, dict[str, object]], report["files"])
    summary = cast(dict[str, object], report["summary"])
    targets = {
        "scope": (report, "scope", []),
        "results": (report, "results", {}),
        "summary": (report, "summary", []),
        "generation-errors-shape": (scope, "generation_errors", "none"),
        "generation-errors-present": (scope, "generation_errors", ["demo.py"]),
        "source-files-shape": (scope, "source_files", "src/omi_collector/demo.py"),
        "ids-shape": (scope, "gremlin_ids", "g-001"),
        "empty-source": (scope, "source_files", []),
        "duplicate-source": (scope, "source_files", ["scripts/demo.py", "scripts/demo.py"]),
        "unsorted-source": (scope, "source_files", ["src/omi_collector/demo.py", "scripts/demo.py"]),
        "empty-ids": (scope, "gremlin_ids", []),
        "duplicate-ids": (scope, "gremlin_ids", ["g-001", "g-001"]),
        "unsorted-ids": (scope, "gremlin_ids", ["g-002", "g-001"]),
        "unknown-status": (results[0], "status", "cancelled"),
        "foreign-result-id": (results[0], "gremlin_id", "g-foreign"),
        "summary-count": (summary, "zapped", 0),
        "file-count": (files[source], "total", 2),
        "outside-file-scope": (report, "files", {"src/omi_collector/unscoped.py": {"total": 1}}),
        "foreign-file-path": (results[0], "file_path", "/foreign/outside.py"),
        "invalid-source-path": (scope, "source_files", ["../demo.py"]),
    }
    if corruption == "generation-errors-missing":
        scope.pop("generation_errors")
    elif corruption == "missing-result":
        report["results"] = []
    elif corruption == "duplicate-result-id":
        results.append(results[0].copy())
    else:
        target, key, value = targets[corruption]
        target[key] = value
    if corruption == "outside-file-scope":
        results[0]["file_path"] = str(repo / "src/omi_collector/unscoped.py")


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


def test_prepare_captures_pinned_tool_and_commit_identity(campaign_repo: tuple[Path, dict[str, str]]) -> None:
    repo, env = campaign_repo

    assert _campaign(repo, env, "prepare").returncode == 0
    receipt = cast(dict[str, object], json.loads((repo / ".gremlins_cache" / "campaign.json").read_text()))
    identity = cast(dict[str, object], receipt["identity"])
    uv = cast(dict[str, object], identity["uv"])
    gremlins = cast(dict[str, object], identity["pytest_gremlins"])

    assert identity["commit"]
    assert uv["path"]
    assert uv["version"]
    assert gremlins["version"]
    assert (
        json.loads(cast(str, gremlins["direct_url"]))["vcs_info"]["commit_id"]
        == "073a5e8d4e0239f3c3b468946a0d8469a510c69b"
    )


def test_prepare_tokens_remain_unique_within_the_same_second(
    campaign_repo: tuple[Path, dict[str, str]], monkeypatch: pytest.MonkeyPatch
) -> None:
    repo, env = campaign_repo
    instants = iter((1_800_000_000_123_000_000, 1_800_000_000_456_000_000))
    monkeypatch.setattr(mutation_campaign.time, "time_ns", lambda: next(instants))

    first = _campaign(repo, env, "prepare")
    second = _campaign(repo, env, "prepare", "--fresh")

    assert first.returncode == second.returncode == 0
    first_token, second_token = first.stdout.strip(), second.stdout.strip()
    assert first_token != second_token
    assert first_token[:15] == second_token[:15]
    receipt = cast(dict[str, object], json.loads((repo / ".gremlins_cache" / "campaign.json").read_text()))
    assert receipt["log"] == f".gremlins_cache/mutation-{second_token}.log"


def test_prepare_records_external_uv_failure(
    campaign_repo: tuple[Path, dict[str, str]], monkeypatch: pytest.MonkeyPatch
) -> None:
    repo, env = campaign_repo
    original_run = subprocess.run

    def fail_uv(
        args: list[str], *, check: bool, capture_output: bool, text: bool, cwd: Path | None = None
    ) -> CompletedProcess[str]:
        if args[:2] == ["/controlled/uv", "--version"]:
            assert check is True
            raise subprocess.CalledProcessError(1, args, stderr="uv unavailable")
        return cast(
            CompletedProcess[str], original_run(args, check=check, capture_output=capture_output, text=text, cwd=cwd)
        )

    monkeypatch.setattr(mutation_campaign.shutil, "which", lambda name: "/controlled/uv" if name == "uv" else None)
    monkeypatch.setattr(mutation_campaign.subprocess, "run", fail_uv)

    result = _campaign(repo, env, "prepare")

    assert result.returncode == 1
    assert "returned non-zero exit status 1" in result.stderr


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


def test_finish_records_nonzero_child_status_as_failed(campaign_repo: tuple[Path, dict[str, str]]) -> None:
    repo, env = campaign_repo
    assert _campaign(repo, env, "prepare").returncode == 0

    assert _campaign(repo, env, "finish", "--status", "127").returncode == 0
    receipt = cast(dict[str, object], json.loads((repo / ".gremlins_cache" / "campaign.json").read_text()))
    assert receipt["state"] == "failed"
    assert receipt["exit_status"] == 127


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


def test_finish_reports_exact_unresolved_counts_on_stderr(campaign_repo: tuple[Path, dict[str, str]]) -> None:
    repo, env = campaign_repo
    assert _campaign(repo, env, "prepare").returncode == 0
    _write_status_report(repo, ["zapped", "timeout", "error"])

    result = _campaign(repo, env, "finish", "--status", "0")

    assert result.returncode == 3
    assert result.stderr == "Unresolved native outcomes remain: timeout=1 error=1; review required.\n"


def test_finish_accepts_report_written_at_start_boundary(
    campaign_repo: tuple[Path, dict[str, str]], monkeypatch: pytest.MonkeyPatch
) -> None:
    repo, env = campaign_repo
    _write_report(repo)
    report_path = repo / "coverage" / "gremlins" / "gremlins.json"
    started_at_ns = 1_800_000_000_000_000_000
    previous_mtime_ns = started_at_ns - 1
    os.utime(report_path, ns=(previous_mtime_ns, previous_mtime_ns))
    monkeypatch.setattr(mutation_campaign.time, "time_ns", lambda: started_at_ns)

    prepared = _campaign(repo, env, "prepare")

    assert prepared.returncode == 0, prepared.stderr
    receipt_path = repo / ".gremlins_cache" / "campaign.json"
    receipt = cast(dict[str, object], json.loads(receipt_path.read_text(encoding="utf-8")))
    assert receipt["started_at_ns"] == started_at_ns
    assert receipt["report_mtime_before_ns"] == previous_mtime_ns
    assert previous_mtime_ns != started_at_ns
    os.utime(report_path, ns=(started_at_ns, started_at_ns))

    result = _campaign(repo, env, "finish", "--status", "0")

    assert result.returncode == 0, result.stderr
    finished = cast(dict[str, object], json.loads(receipt_path.read_text(encoding="utf-8")))
    assert finished["state"] == "complete"


def test_finish_accepts_all_zapped_results(campaign_repo: tuple[Path, dict[str, str]]) -> None:
    repo, env = campaign_repo
    assert _campaign(repo, env, "prepare").returncode == 0
    _write_status_report(repo, ["zapped", "zapped"])

    result = _campaign(repo, env, "finish", "--status", "0")

    assert result.returncode == 0, result.stderr
    receipt = cast(dict[str, object], json.loads((repo / ".gremlins_cache" / "campaign.json").read_text()))
    assert receipt["state"] == "complete"
    report = cast(dict[str, object], receipt["report"])
    assert report["mutant_count"] == 2
    assert report["source_file_count"] == 2


def test_file_breakdown_can_be_a_strict_subset_of_source_scope(campaign_repo: tuple[Path, dict[str, str]]) -> None:
    repo, env = campaign_repo
    assert _campaign(repo, env, "prepare").returncode == 0
    _write_status_report(repo, ["zapped"])

    result = _campaign(repo, env, "finish", "--status", "0")

    assert result.returncode == 0, result.stderr
    receipt = cast(dict[str, object], json.loads((repo / ".gremlins_cache" / "campaign.json").read_text()))
    assert cast(dict[str, object], receipt["report"])["source_file_count"] == 2


def test_file_breakdown_can_equal_the_generated_source_scope(campaign_repo: tuple[Path, dict[str, str]]) -> None:
    repo, env = campaign_repo
    assert _campaign(repo, env, "prepare").returncode == 0
    _write_report(repo)
    report_path = repo / "coverage" / "gremlins" / "gremlins.json"
    data = cast(dict[str, object], json.loads(report_path.read_text(encoding="utf-8")))
    scope = cast(dict[str, object], data["scope"])
    scope["gremlin_ids"] = ["g-001", "g-002"]
    results = cast(list[dict[str, object]], data["results"])
    results.append({"gremlin_id": "g-002", "file_path": str(repo / "scripts/demo.py"), "status": "zapped"})
    data["summary"] = {"total": 2, "zapped": 2, "survived": 0, "timeout": 0, "error": 0, "pardoned": 0}
    data["files"] = {
        str(repo / "scripts/demo.py"): {"total": 1},
        str(repo / "src/omi_collector/demo.py"): {"total": 1},
    }
    report_path.write_text(json.dumps(data), encoding="utf-8")
    future = time.time_ns() + 1_000_000
    os.utime(report_path, ns=(future, future))

    result = _campaign(repo, env, "finish", "--status", "0")

    assert result.returncode == 0, result.stderr
    receipt = cast(dict[str, object], json.loads((repo / ".gremlins_cache" / "campaign.json").read_text()))
    assert cast(dict[str, object], receipt["report"])["mutant_count"] == 2


def test_finish_scope_excludes_tracked_non_python_inputs(campaign_repo: tuple[Path, dict[str, str]]) -> None:
    repo, env = campaign_repo
    (repo / "scripts" / "helper.sh").write_text("#!/bin/sh\n", encoding="utf-8")
    (repo / "scripts" / "example.sudoers").write_text("root ALL=(ALL) NOPASSWD: ALL\n", encoding="utf-8")
    (repo / "src" / "omi_collector" / "py.typed").write_text("", encoding="utf-8")
    subprocess.run(["git", "add", "."], cwd=repo, check=True)
    subprocess.run(["git", "commit", "--quiet", "-m", "add non-Python scope fixtures"], cwd=repo, check=True)

    assert _campaign(repo, env, "prepare").returncode == 0
    _write_status_report(repo, ["zapped"])
    result = _campaign(repo, env, "finish", "--status", "0")

    assert result.returncode == 0, result.stderr
    receipt = cast(dict[str, object], json.loads((repo / ".gremlins_cache" / "campaign.json").read_text()))
    assert cast(dict[str, object], receipt["report"])["source_file_count"] == 2


def test_finish_reconciles_every_status_and_marks_unresolved(campaign_repo: tuple[Path, dict[str, str]]) -> None:
    repo, env = campaign_repo
    assert _campaign(repo, env, "prepare").returncode == 0
    _write_status_report(repo, ["zapped", "survived", "timeout", "error", "pardoned"])

    result = _campaign(repo, env, "finish", "--status", "0")

    assert result.returncode == 3
    receipt = cast(dict[str, object], json.loads((repo / ".gremlins_cache" / "campaign.json").read_text()))
    assert receipt["state"] == "complete_unresolved"
    report = cast(dict[str, object], receipt["report"])
    assert report["status_counts"] == {"error": 1, "pardoned": 1, "survived": 1, "timeout": 1, "zapped": 1}


def test_finish_requires_running_receipt(campaign_repo: tuple[Path, dict[str, str]]) -> None:
    repo, env = campaign_repo

    result = _campaign(repo, env, "finish", "--status", "0")

    assert result.returncode == 1
    assert "no active campaign receipt" in result.stderr


def test_campaign_external_command_failures_are_reported(
    campaign_repo: tuple[Path, dict[str, str]], monkeypatch: pytest.MonkeyPatch
) -> None:
    repo, env = campaign_repo
    original_run = subprocess.run

    def fail_git(
        args: list[str], *, check: bool, capture_output: bool, text: bool, cwd: Path | None = None
    ) -> CompletedProcess[str]:
        if args[:2] == ["git", "status"]:
            assert check is True
            raise subprocess.CalledProcessError(1, args, stderr="git unavailable")
        return cast(
            CompletedProcess[str], original_run(args, check=check, capture_output=capture_output, text=text, cwd=cwd)
        )

    monkeypatch.setattr(mutation_campaign.subprocess, "run", fail_git)

    result = _campaign(repo, env, "prepare")

    assert result.returncode == 1
    assert "returned non-zero exit status 1" in result.stderr


@pytest.mark.parametrize("arguments", [[], ["prepare"], ["finish"]])
def test_main_requires_action_and_command_specific_arguments(
    campaign_repo: tuple[Path, dict[str, str]], monkeypatch: pytest.MonkeyPatch, arguments: list[str]
) -> None:
    repo, env = campaign_repo
    with monkeypatch.context() as patch:
        patch.chdir(repo)
        for key in mutation_campaign.RELEVANT_ENV:
            if key in env:
                patch.setenv(key, env[key])
            else:
                patch.delenv(key, raising=False)
        patch.setattr(sys, "argv", [str(SCRIPT), *arguments])
        with pytest.raises(SystemExit, match="2"):
            mutation_campaign.main()

    assert not (repo / ".gremlins_cache").exists()


@pytest.mark.parametrize(
    "scenario",
    [
        pytest.param(("orphan", 10_001, True, True), id="orphan"),
        pytest.param(("equal-start-tick", 10_000, True, True), id="equal-start-tick"),
        pytest.param(("older", 9_999, True, False), id="older"),
        pytest.param(("other-cwd", 10_001, False, False), id="other-cwd"),
        pytest.param(("own-ancestor", 10_001, True, False), id="own-ancestor"),
    ],
)
def test_resume_scans_proc_snapshot_for_campaign_owned_processes(
    campaign_repo: tuple[Path, dict[str, str]],
    monkeypatch: pytest.MonkeyPatch,
    scenario: tuple[str, int, bool, bool],
) -> None:
    candidate, ticks, cwd_matches, blocked = scenario
    repo, env = campaign_repo
    started_at = 100_100 * 1_000_000_000
    monkeypatch.setattr(mutation_campaign.time, "time_ns", lambda: started_at)
    assert _campaign(repo, env, "prepare").returncode == 0
    assert _campaign(repo, env, "finish", "--status", "124").returncode == 0
    real_path = Path
    parent_pid = os.getpid()
    candidate_pid = 1 if candidate == "own-ancestor" else 300_000
    parent_map = {parent_pid: 1, 1: 0, candidate_pid: 0}
    ticks_map = {parent_pid: 10_001, 1: 10_001, candidate_pid: ticks}
    cwd_map = {parent_pid: repo, 1: repo, candidate_pid: repo if cwd_matches else repo.parent}

    class SnapshotPath:
        def __init__(self, raw: str) -> None:
            self.raw = raw
            self.name = raw.rsplit("/", 1)[-1]

        def read_text(self, encoding: str | None = None) -> str:
            assert encoding in {None, "ascii"}
            if self.raw == "/proc/stat":
                return "btime 100000\n"
            pid = int(self.raw.split("/")[2])
            if pid not in parent_map:
                raise FileNotFoundError(self.raw)
            tail = ["S", str(parent_map[pid]), *("0" for _ in range(17)), str(ticks_map[pid])]
            return f"{pid} (pytest fixture) {' '.join(tail)}"

        def read_bytes(self) -> bytes:
            return b"python -m pytest\0" if self.raw.endswith("/cmdline") else b""

        def resolve(self) -> Path:
            pid = int(self.raw.split("/")[2])
            return real_path(cwd_map[pid])

        def iterdir(self) -> list[SnapshotPath]:
            return [SnapshotPath(f"/proc/{pid}") for pid in set(parent_map) | {candidate_pid}]

        def joinpath(self, part: str) -> SnapshotPath:
            return SnapshotPath(f"{self.raw}/{part}")

    def snapshot_path(raw: str | Path) -> SnapshotPath | Path:
        value = str(raw)
        return SnapshotPath(value) if value == "/proc" or value.startswith("/proc/") else real_path(raw)

    snapshot_path.cwd = real_path.cwd  # type: ignore[attr-defined]
    monkeypatch.setattr(mutation_campaign, "Path", snapshot_path)

    result = _campaign(repo, env, "prepare")

    assert result.returncode == int(blocked), result.stderr
    assert ("campaign-owned processes may remain" in result.stderr) is blocked


@pytest.mark.parametrize(
    ("corruption", "diagnostic"),
    [
        ("scope", "scope, results, or summary"),
        ("results", "scope, results, or summary"),
        ("summary", "scope, results, or summary"),
        ("generation-errors-missing", "failed to transform"),
        ("generation-errors-shape", "failed to transform"),
        ("generation-errors-present", "failed to transform"),
        ("source-files-shape", "missing source files"),
        ("ids-shape", "missing source files"),
        ("empty-source", "source-file scope is empty"),
        ("duplicate-source", "source-file scope is empty"),
        ("unsorted-source", "source-file scope is empty"),
        ("empty-ids", "generated mutant IDs are empty"),
        ("duplicate-ids", "generated mutant IDs are empty"),
        ("unsorted-ids", "generated mutant IDs are empty"),
        ("unknown-status", "invalid or unresolved status"),
        ("foreign-result-id", "duplicate, missing, or foreign"),
        ("missing-result", "duplicate, missing, or foreign"),
        ("duplicate-result-id", "duplicate, missing, or foreign"),
        ("summary-count", "summary counts do not match"),
        ("file-count", "per-file results do not match"),
        ("outside-file-scope", "outside generated source scope"),
        ("foreign-file-path", "foreign path"),
        ("invalid-source-path", "invalid source path"),
    ],
)
def test_finish_rejects_malformed_native_report(
    campaign_repo: tuple[Path, dict[str, str]], corruption: str, diagnostic: str
) -> None:
    repo, env = campaign_repo
    assert _campaign(repo, env, "prepare").returncode == 0
    _write_report(repo)
    report_path = repo / "coverage" / "gremlins" / "gremlins.json"
    report = cast(dict[str, object], json.loads(report_path.read_text(encoding="utf-8")))
    _corrupt_report(report, corruption, repo)
    report_path.write_text(json.dumps(report), encoding="utf-8")
    future = time.time_ns() + 1_000_000
    os.utime(report_path, ns=(future, future))

    result = _campaign(repo, env, "finish", "--status", "0")

    assert result.returncode == 1
    assert diagnostic in result.stderr
