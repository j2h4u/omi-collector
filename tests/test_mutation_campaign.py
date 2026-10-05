from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import subprocess
import sys
import threading
import time
from collections import Counter
from collections.abc import Callable
from contextlib import closing, redirect_stderr, redirect_stdout
from io import StringIO
from pathlib import Path
from typing import cast

import pytest
from scripts import mutation_campaign


@pytest.fixture
def campaign_project(tmp_path: Path) -> tuple[Path, dict[str, str]]:
    repo = tmp_path / "project"
    repo.mkdir()
    (repo / ".gitignore").write_text(".gremlins_cache/\n.coveragerc.gremlins\ncoverage/\n", encoding="utf-8")
    (repo / "uv.lock").write_text("version = 1\n", encoding="utf-8")
    (repo / "pyproject.toml").write_text("[project]\nname = 'campaign-fixture'\nversion = '0.1.0'\n", encoding="utf-8")
    (repo / "src" / "omi_collector").mkdir(parents=True)
    (repo / "src" / "omi_collector" / "demo.py").write_text("value = 'before'\n", encoding="utf-8")
    (repo / "scripts").mkdir()
    (repo / "scripts" / "demo.py").write_text("script_value = 2\n", encoding="utf-8")
    wrapper = repo / "scripts" / "omi-collector-deploy-release"
    wrapper.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    sudoers = repo / "scripts" / "omi-collector-deploy-release.sudoers"
    sudoers.write_text("operator ALL=(root) NOPASSWD: /usr/bin/true\n", encoding="utf-8")
    wrapper.chmod(0o755)
    sudoers.chmod(0o644)
    subprocess.run(["git", "init", "--quiet"], cwd=repo, check=True)
    subprocess.run(["git", "config", "user.name", "Campaign test"], cwd=repo, check=True)
    subprocess.run(["git", "config", "user.email", "campaign-test@example.invalid"], cwd=repo, check=True)
    subprocess.run(["git", "add", "."], cwd=repo, check=True)
    subprocess.run(["git", "commit", "--quiet", "-m", "campaign fixture"], cwd=repo, check=True)
    # Simulate the shared-filesystem mode drift from the failed host bootstrap.
    wrapper.chmod(0o775)
    sudoers.chmod(0o664)
    env = os.environ.copy()
    env.update(COVERAGE_CORE="ctrace", COVERAGE_FILE="", PYTEST_ADDOPTS="", UV_LINK_MODE="hardlink")
    return repo, env


def _launch(
    repo: Path,
    env: dict[str, str],
    monkeypatch: pytest.MonkeyPatch,
    *args: str,
) -> tuple[int, str, str]:
    stdout, stderr = StringIO(), StringIO()
    with monkeypatch.context() as patch:
        patch.chdir(repo)
        for key in mutation_campaign.RELEVANT_ENV:
            if key in env:
                patch.setenv(key, env[key])
            else:
                patch.delenv(key, raising=False)
        patch.setattr(sys, "argv", ["mutation_campaign", *args])
        with redirect_stdout(stdout), redirect_stderr(stderr):
            status = mutation_campaign.main()
    return status, stdout.getvalue(), stderr.getvalue()


def _without_runner_defaults(environment: dict[str, str]) -> dict[str, str]:
    caller_environment = environment.copy()
    for key in mutation_campaign.FIXED_ENV:
        caller_environment.pop(key, None)
    return caller_environment


def _without_outer_job_token(environment: dict[str, str] | None = None) -> dict[str, str]:
    child_environment = (os.environ if environment is None else environment).copy()
    child_environment.pop(mutation_campaign.JOB_TOKEN_ENV, None)
    return child_environment


def test_job_environment_removes_outer_token_and_nested_owner_guard_stays_strict(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    job = tmp_path / "guard-job"
    (job / "checkout" / ".gremlins_cache").mkdir(parents=True)
    token = "nested-owner-run"
    outer_token = "outer-audit-run"
    monkeypatch.setenv(mutation_campaign.JOB_TOKEN_ENV, outer_token)
    monkeypatch.setenv("ACTIVE_GREMLIN", "preserve-this-runner-setting")
    (job / "owner.json").write_text(
        json.dumps({"schema": 1, "run_token": token, "state": "preparing"}), encoding="utf-8"
    )

    job_environment = mutation_campaign._job_environment(job)

    assert mutation_campaign.JOB_TOKEN_ENV not in job_environment
    assert job_environment["ACTIVE_GREMLIN"] == "preserve-this-runner-setting"
    with pytest.raises(ValueError, match="must not contain the private child token"):
        mutation_campaign._enter_owner(
            job,
            "resume",
            token,
            command=[sys.executable, "-c", "raise SystemExit(0)"],
            environment={**job_environment, mutation_campaign.JOB_TOKEN_ENV: outer_token},
        )


def test_stop_owned_processes_accepts_pidfd_open_race_after_process_exit(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    token = "pidfd-exit-race"
    environment = {**_without_outer_job_token(), mutation_campaign.JOB_TOKEN_ENV: token}
    owner = subprocess.Popen(
        [sys.executable, "-c", "import time; time.sleep(60)"],
        env=environment,
        start_new_session=True,
    )
    exiting = subprocess.Popen(
        [sys.executable, "-c", "import time; time.sleep(60)"],
        env=environment,
        start_new_session=True,
    )
    try:
        _parent, exiting_ticks, _state = mutation_campaign._proc_identity(exiting.pid)
        real_open_pidfd = mutation_campaign._open_pidfd

        def scan(
            _token: str, root_pid: int | None = None, _root_ticks: str | None = None
        ) -> dict[int, tuple[str, str]]:
            return {exiting.pid: (exiting_ticks, "exiting token process")} if root_pid is not None else {}

        def open_pidfd(pid: int, ticks: str) -> int:
            if pid == exiting.pid:
                exiting.terminate()
                exiting.wait(timeout=5)
                raise ProcessLookupError(3, "No such process")
            return real_open_pidfd(pid, ticks)

        monkeypatch.setattr(mutation_campaign, "_token_pids", scan)
        monkeypatch.setattr(mutation_campaign, "_open_pidfd", open_pidfd)

        checkpoint = mutation_campaign._stop_owned_processes(owner, token, tmp_path)

        assert owner.poll() is not None
        assert checkpoint["processes_stopped"] == [owner.pid]
        assert checkpoint["cache"] == {"results_db": "absent", "quick_check": "not-applicable"}
    finally:
        for process in (owner, exiting):
            if process.poll() is None:
                process.terminate()
                process.wait(timeout=5)


def test_stop_owned_processes_rejects_esrch_when_pid_is_still_live(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    token = "pidfd-live-race"
    environment = {**_without_outer_job_token(), mutation_campaign.JOB_TOKEN_ENV: token}
    owner = subprocess.Popen(
        [sys.executable, "-c", "import time; time.sleep(60)"],
        env=environment,
        start_new_session=True,
    )
    live = subprocess.Popen(
        [sys.executable, "-c", "import time; time.sleep(60)"],
        env=environment,
        start_new_session=True,
    )
    try:
        _parent, live_ticks, _state = mutation_campaign._proc_identity(live.pid)
        real_open_pidfd = mutation_campaign._open_pidfd

        def scan(
            _token: str, root_pid: int | None = None, _root_ticks: str | None = None
        ) -> dict[int, tuple[str, str]]:
            return {live.pid: (live_ticks, "still-live token process")} if root_pid is not None else {}

        def open_pidfd(pid: int, ticks: str) -> int:
            if pid == live.pid:
                raise ProcessLookupError(3, "No such process")
            return real_open_pidfd(pid, ticks)

        monkeypatch.setattr(mutation_campaign, "_token_pids", scan)
        monkeypatch.setattr(mutation_campaign, "_open_pidfd", open_pidfd)

        with pytest.raises(ValueError, match="could not verify safe mutation checkpoint"):
            mutation_campaign._stop_owned_processes(owner, token, tmp_path)

        assert owner.poll() is None
        assert live.poll() is None
        _parent, current_ticks, state = mutation_campaign._proc_identity(live.pid)
        assert current_ticks == live_ticks
        assert state not in {"T", "t", "Z"}
        with monkeypatch.context() as patch:
            patch.setattr(mutation_campaign, "_proc_identity", lambda _pid: (_ for _ in ()).throw(PermissionError()))
            with pytest.raises(PermissionError):
                mutation_campaign._verified_process_exit_race(
                    ProcessLookupError(3, "No such process"), live.pid, live_ticks
                )
    finally:
        for process in (owner, live):
            if process.poll() is None:
                process.terminate()
                process.wait(timeout=5)


def _job_directories(audit_root: Path) -> list[Path]:
    return sorted(path.parent for path in audit_root.glob("jobs/omi-collector/*/owner.json"))


def _read_json_object(path: Path) -> dict[str, object]:
    value = cast(object, json.loads(path.read_text(encoding="utf-8")))
    if not isinstance(value, dict):
        raise AssertionError(f"expected JSON object in {path}")
    return cast(dict[str, object], value)


def _process_stopped(pid: int) -> bool:
    deadline = time.monotonic() + 2
    while time.monotonic() < deadline:
        try:
            stat = Path(f"/proc/{pid}/stat").read_text(encoding="ascii")
        except FileNotFoundError:
            return True
        if stat.rsplit(")", 1)[-1].split()[0] == "Z":
            return True
        time.sleep(0.01)
    return False


def _write_cache(path: Path, rows: list[tuple[str, str]]) -> bytes:
    path.parent.mkdir(parents=True, exist_ok=True)
    with closing(sqlite3.connect(path)) as cache, cache:
        cache.execute("CREATE TABLE results (cache_key TEXT PRIMARY KEY, result_json TEXT NOT NULL)")
        cache.executemany(
            "INSERT INTO results VALUES (?, ?)",
            [(key, json.dumps({"status": status})) for key, status in rows],
        )
    return path.read_bytes()


def test_fresh_public_launch_snapshots_and_normalizes_without_mutating_the_project(
    campaign_project: tuple[Path, dict[str, str]], monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    repo, env = campaign_project
    audit_root = tmp_path / "audit"
    monkeypatch.setattr(mutation_campaign, "AUDIT_ROOT", audit_root)
    sync_calls: list[Path] = []
    owner_calls: list[tuple[Path, str, str]] = []

    def sync(job_root: Path, _environment: dict[str, str] | None = None) -> None:
        sync_calls.append(job_root)

    def owner(job_root: Path, mode: str, token: str, **kwargs: object) -> int:
        runner_environment = cast(dict[str, str], kwargs["environment"])
        assert mutation_campaign.JOB_TOKEN_ENV not in runner_environment
        assert runner_environment["COVERAGE_CORE"] == "ctrace"
        assert runner_environment["COVERAGE_FILE"] == ""
        assert runner_environment["PYTEST_ADDOPTS"] == ""
        assert runner_environment["UV_LINK_MODE"] == "hardlink"
        owner_calls.append((job_root, mode, token))
        checkout = job_root / "checkout"
        assert (checkout / "scripts/omi-collector-deploy-release").stat().st_mode & 0o777 == 0o755
        assert (checkout / "scripts/omi-collector-deploy-release.sudoers").stat().st_mode & 0o777 == 0o644
        assert (checkout / "src/omi_collector/demo.py").read_text(encoding="utf-8") == "value = 'before'\n"
        (repo / "src" / "omi_collector" / "demo.py").write_text("value = 'edited while running'\n", encoding="utf-8")
        assert (checkout / "src/omi_collector/demo.py").read_text(encoding="utf-8") == "value = 'before'\n"
        return 0

    monkeypatch.setattr(mutation_campaign, "_sync_job_environment", sync)
    monkeypatch.setattr(mutation_campaign, "_enter_owner", owner)

    status, _stdout, stderr = _launch(
        repo,
        _without_runner_defaults(env),
        monkeypatch,
        "launch",
        "--mode",
        "fresh",
    )

    jobs = _job_directories(audit_root)
    assert status == 0, stderr
    assert len(jobs) == 1
    assert len(sync_calls) == 1
    assert owner_calls == [(jobs[0], "fresh", json.loads((jobs[0] / "owner.json").read_text())["run_token"])]
    assert (repo / "scripts/omi-collector-deploy-release").stat().st_mode & 0o777 == 0o775
    assert (repo / "scripts/omi-collector-deploy-release.sudoers").stat().st_mode & 0o777 == 0o664


def test_generated_gremlins_coverage_config_stays_out_of_snapshot_identity(
    campaign_project: tuple[Path, dict[str, str]],
) -> None:
    repo, environment = campaign_project
    mutation_campaign._canonicalize_snapshot_modes(repo)
    generated = repo / ".coveragerc.gremlins"
    generated.write_text("[run]\nbranch = True\n", encoding="utf-8")

    mutation_campaign._snapshot_identity(repo, environment)

    (repo / "unexpected-run-output.tmp").write_text("not ignored\n", encoding="utf-8")
    with pytest.raises(ValueError, match="tracked or untracked changes"):
        mutation_campaign._snapshot_identity(repo, environment)


def test_resume_reuses_exact_job_and_preserves_completed_native_cache(
    campaign_project: tuple[Path, dict[str, str]], monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    repo, env = campaign_project
    audit_root = tmp_path / "audit"
    monkeypatch.setattr(mutation_campaign, "AUDIT_ROOT", audit_root)
    monkeypatch.setattr(mutation_campaign, "_sync_job_environment", lambda *_: None)
    monkeypatch.setattr(mutation_campaign, "_enter_owner", lambda *_args, **_kwargs: 0)
    status, _stdout, stderr = _launch(repo, env, monkeypatch, "launch", "--mode", "fresh")
    assert status == 0, stderr
    job = _job_directories(audit_root)[0]
    owner_receipt = _read_json_object(job / "owner.json")
    with monkeypatch.context() as patch:
        patch.chdir(repo)
        for key in mutation_campaign.RELEVANT_ENV:
            if key in env:
                patch.setenv(key, env[key])
            else:
                patch.delenv(key, raising=False)
        runtime_env = mutation_campaign._job_environment(job)
        current_identity = mutation_campaign._snapshot_identity(job / "checkout", runtime_env)
    expected_identity = owner_receipt.get("identity")
    assert isinstance(expected_identity, dict)
    differences = {
        key: (expected_identity.get(key), current_identity.get(key))
        for key in expected_identity.keys() | current_identity.keys()
        if expected_identity.get(key) != current_identity.get(key)
    }
    assert not differences, json.dumps(differences, indent=2)
    owner_receipt["state"] = "paused"
    owner_receipt["checkpoint_verified"] = True
    (job / "owner.json").write_text(json.dumps(owner_receipt), encoding="utf-8")
    cache_path = job / "checkout" / ".gremlins_cache" / "results.db"
    before = _write_cache(cache_path, [("completed", "ZAPPED")])
    calls: list[tuple[Path, str]] = []

    def resume_owner(job_root: Path, mode: str, *_args: object, **kwargs: object) -> int:
        runner_environment = cast(dict[str, str], kwargs["environment"])
        assert runner_environment["COVERAGE_CORE"] == "ctrace"
        assert runner_environment["COVERAGE_FILE"] == ""
        assert runner_environment["PYTEST_ADDOPTS"] == ""
        assert runner_environment["UV_LINK_MODE"] == "hardlink"
        calls.append((job_root, mode))
        assert cache_path.read_bytes() == before
        return 0

    monkeypatch.setattr(mutation_campaign, "_enter_owner", resume_owner)

    status, _stdout, stderr = _launch(
        repo,
        _without_runner_defaults(env),
        monkeypatch,
        "launch",
        "--mode",
        "resume",
    )

    assert status == 0, stderr
    assert calls == [(job, "resume")]
    assert cache_path.read_bytes() == before
    with closing(sqlite3.connect(cache_path)) as cache, cache:
        assert cache.execute("PRAGMA quick_check").fetchone() == ("ok",)
        assert cache.execute("SELECT * FROM results").fetchall() == [
            ("completed", '{"status": "ZAPPED"}'),
        ]


def test_incompatible_resume_refuses_before_mutating_cache(
    campaign_project: tuple[Path, dict[str, str]], monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    repo, env = campaign_project
    audit_root = tmp_path / "audit"
    monkeypatch.setattr(mutation_campaign, "AUDIT_ROOT", audit_root)
    monkeypatch.setattr(mutation_campaign, "_sync_job_environment", lambda *_: None)
    monkeypatch.setattr(mutation_campaign, "_enter_owner", lambda *_args, **_kwargs: 0)
    assert _launch(repo, env, monkeypatch, "launch", "--mode", "fresh")[0] == 0
    job = _job_directories(audit_root)[0]
    owner_receipt = _read_json_object(job / "owner.json")
    owner_receipt["state"] = "paused"
    owner_receipt["checkpoint_verified"] = True
    cast(dict[str, object], owner_receipt["identity"])["tree"] = "not-the-recorded-tree"
    (job / "owner.json").write_text(json.dumps(owner_receipt), encoding="utf-8")
    cache_path = job / "checkout" / ".gremlins_cache" / "results.db"
    before = _write_cache(cache_path, [("completed", "ZAPPED")])
    owner_calls: list[bool] = []
    monkeypatch.setattr(mutation_campaign, "_enter_owner", lambda *_a, **_k: owner_calls.append(True) or 0)

    status, _stdout, stderr = _launch(repo, env, monkeypatch, "launch", "--mode", "resume")

    assert status != 0
    assert stderr
    assert owner_calls == []
    assert hashlib.sha256(cache_path.read_bytes()).digest() == hashlib.sha256(before).digest()


def test_resume_rejects_changed_snapshot_content_and_preserves_native_cache(
    campaign_project: tuple[Path, dict[str, str]], monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    repo, env = campaign_project
    audit_root = tmp_path / "audit"
    monkeypatch.setattr(mutation_campaign, "AUDIT_ROOT", audit_root)
    monkeypatch.setattr(mutation_campaign, "_sync_job_environment", lambda *_: None)
    monkeypatch.setattr(mutation_campaign, "_enter_owner", lambda *_args, **_kwargs: 0)
    assert _launch(repo, env, monkeypatch, "launch", "--mode", "fresh")[0] == 0
    job = _job_directories(audit_root)[0]
    owner_receipt = _read_json_object(job / "owner.json")
    owner_receipt["state"] = "paused"
    owner_receipt["checkpoint_verified"] = True
    (job / "owner.json").write_text(json.dumps(owner_receipt), encoding="utf-8")
    snapshot_source = job / "checkout" / "src/omi_collector/demo.py"
    snapshot_source.write_text("value = 'tampered snapshot'\n", encoding="utf-8")
    cache_path = job / "checkout" / ".gremlins_cache" / "results.db"
    before = _write_cache(cache_path, [("completed", "ZAPPED")])
    owner_calls: list[bool] = []
    monkeypatch.setattr(mutation_campaign, "_enter_owner", lambda *_a, **_k: owner_calls.append(True) or 0)

    status, _stdout, stderr = _launch(repo, env, monkeypatch, "launch", "--mode", "resume")

    assert status != 0
    assert stderr
    assert owner_calls == []
    assert cache_path.read_bytes() == before


def test_live_snapshot_drift_stops_owner_and_invalidates_the_job(
    campaign_project: tuple[Path, dict[str, str]],
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    repo, _env = campaign_project
    monkeypatch.setattr(mutation_campaign, "AUDIT_ROOT", tmp_path / "audit")
    monkeypatch.setattr(mutation_campaign, "_sync_job_environment", lambda *_: None)
    monkeypatch.setenv("COVERAGE_CORE", "ctrace")
    monkeypatch.setenv("COVERAGE_FILE", "")
    monkeypatch.setenv("PYTEST_ADDOPTS", "")
    monkeypatch.setenv("UV_LINK_MODE", "hardlink")
    job = mutation_campaign._new_job(repo)
    checkout = job / "checkout"
    pid_path = tmp_path / "child.pid"
    token = "source-drift-run"
    environment = mutation_campaign._job_environment(job)
    with monkeypatch.context() as patch:
        patch.chdir(checkout)
        identity = mutation_campaign._snapshot_identity(checkout, environment)
    (job / "owner.json").write_text(
        json.dumps(
            {
                "schema": 1,
                "run_token": token,
                "state": "preparing",
                "identity": identity,
                "control_socket": str(mutation_campaign.control_socket_path(job)),
            }
        ),
        encoding="utf-8",
    )
    runner = f"""
import os, pathlib, time
source = pathlib.Path('src/omi_collector/demo.py')
source.write_text("value = 'tampered while running'\\n")
pathlib.Path({str(pid_path)!r}).write_text(str(os.getpid()))
time.sleep(60)
"""
    monkeypatch.setattr(mutation_campaign, "OWNER_CHECK_SECONDS", 0.05)
    owner_result: list[int] = []

    def enter_owner() -> None:
        owner_result.append(
            mutation_campaign._enter_owner(
                job,
                "fresh",
                token,
                command=[sys.executable, "-c", runner],
                environment=environment,
            )
        )

    with monkeypatch.context() as patch:
        patch.chdir(checkout)
        owner = threading.Thread(target=enter_owner, daemon=True)
        owner.start()
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline and not pid_path.exists() and owner.is_alive():
            time.sleep(0.01)
        if not pid_path.is_file():
            owner.join(timeout=5)
            receipt = _read_json_object(job / "owner.json")
            pytest.fail(
                f"owner exited before child PID was recorded: {owner_result!r}; "
                f"alive={owner.is_alive()}, receipt={receipt!r}"
            )
        child_pid = int(pid_path.read_text(encoding="ascii"))
        owner.join(timeout=5)

    assert not owner.is_alive()
    assert owner_result == [1]
    receipt = _read_json_object(job / "owner.json")
    assert receipt["state"] == "source_invalidated"
    assert receipt["cleanup_verified"] is True
    assert mutation_campaign._token_pids(token) == {}
    assert _process_stopped(child_pid)


def test_controller_failure_and_native_unresolved_report_have_distinct_receipts(tmp_path: Path) -> None:
    native_job = tmp_path / "native-unresolved"
    (native_job / "checkout" / ".gremlins_cache").mkdir(parents=True)
    native_token = "native-unresolved-token"
    (native_job / "owner.json").write_text(
        json.dumps({"schema": 1, "run_token": native_token, "state": "preparing"}), encoding="utf-8"
    )
    native_report = "import json, pathlib; pathlib.Path('.gremlins_cache/campaign.json').write_text(json.dumps({'state': 'complete_unresolved'})); raise SystemExit(3)"

    native_status = mutation_campaign._enter_owner(
        native_job,
        "resume",
        native_token,
        command=[sys.executable, "-c", native_report],
        environment=_without_outer_job_token(),
    )

    native_receipt = _read_json_object(native_job / "owner.json")
    assert native_status == 3
    assert native_receipt["state"] == "complete_unresolved"
    native_campaign = cast(dict[str, object], native_receipt["campaign"])
    assert native_campaign["state"] == "complete_unresolved"

    failed_job = tmp_path / "controller-failed"
    (failed_job / "checkout" / ".gremlins_cache").mkdir(parents=True)
    failed_token = "controller-failed-token"
    (failed_job / "owner.json").write_text(
        json.dumps({"schema": 1, "run_token": failed_token, "state": "preparing"}), encoding="utf-8"
    )

    failed_status = mutation_campaign._enter_owner(
        failed_job,
        "resume",
        failed_token,
        command=[sys.executable, "-c", "raise SystemExit(1)"],
        environment=_without_outer_job_token(),
    )

    failed_receipt = _read_json_object(failed_job / "owner.json")
    assert failed_status == 1
    assert failed_receipt["state"] == "failed"
    assert failed_receipt["campaign"] is None


def test_finish_reconciles_native_outcome_counts_without_crediting_error_or_timeout() -> None:
    counts = mutation_campaign._result_counts(
        [
            {"gremlin_id": "done", "status": "zapped"},
            {"gremlin_id": "survived", "status": "survived"},
            {"gremlin_id": "error", "status": "error"},
            {"gremlin_id": "timeout", "status": "timeout"},
        ]
    )

    assert counts == {"error": 1, "pardoned": 0, "survived": 1, "timeout": 1, "zapped": 1}


def test_invalid_native_status_is_not_classified_as_controller_success() -> None:
    with pytest.raises(ValueError, match="invalid or unresolved status"):
        mutation_campaign._result_counts([{"gremlin_id": "cancelled", "status": "cancelled"}])


def _native_report(repo: Path, statuses: list[str]) -> dict[str, object]:
    ids = [f"g-{index:03d}" for index in range(len(statuses))]
    source = "src/omi_collector/demo.py"
    source_paths = ["scripts/demo.py", source]
    source_abs = str(repo / source)
    status_counts = Counter(statuses)
    return {
        "scope": {"source_files": source_paths, "gremlin_ids": ids, "generation_errors": []},
        "summary": {
            "total": len(ids),
            **{name: status_counts[name] for name in ("zapped", "survived", "timeout", "error", "pardoned")},
        },
        "files": {source_abs: {"total": len(ids)}},
        "results": [
            {"gremlin_id": gremlin_id, "file_path": source_abs, "status": status}
            for gremlin_id, status in zip(ids, statuses, strict=True)
        ],
    }


def _postflight(
    repo: Path,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    report: dict[str, object],
) -> dict[str, object]:
    report_path = tmp_path / "gremlins.json"
    report_path.write_text(json.dumps(report), encoding="utf-8")
    identity = {"commit": "test-commit"}
    monkeypatch.setattr(mutation_campaign, "REPORT", report_path)
    monkeypatch.setattr(mutation_campaign, "_identity", lambda: identity)
    monkeypatch.setattr(mutation_campaign, "_load_report", lambda _started: report)
    with monkeypatch.context() as patch:
        patch.chdir(repo)
        return mutation_campaign._postflight({"identity": identity, "started_at_ns": 1})


def test_postflight_keeps_survivors_errors_and_timeouts_unresolved(
    campaign_project: tuple[Path, dict[str, str]], monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    repo, _env = campaign_project

    result = _postflight(repo, tmp_path, monkeypatch, _native_report(repo, ["zapped", "survived", "error", "timeout"]))

    assert result["state"] == "complete_unresolved"
    assert result["mutant_count"] == 4
    assert result["source_file_count"] == 2
    report = cast(dict[str, object], result["report"])
    assert report["status_counts"] == {
        "error": 1,
        "pardoned": 0,
        "survived": 1,
        "timeout": 1,
        "zapped": 1,
    }


def test_postflight_accepts_file_breakdown_covering_the_full_source_scope(
    campaign_project: tuple[Path, dict[str, str]], monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    repo, _env = campaign_project
    report = _native_report(repo, ["zapped", "zapped"])
    results = cast(list[dict[str, object]], report["results"])
    results[1]["file_path"] = str(repo / "scripts/demo.py")
    report["files"] = {
        str(repo / "src/omi_collector/demo.py"): {"total": 1},
        str(repo / "scripts/demo.py"): {"total": 1},
    }

    result = _postflight(repo, tmp_path, monkeypatch, report)

    assert result["state"] == "complete"
    assert result["source_file_count"] == 2
    assert result["mutant_count"] == 2


@pytest.mark.parametrize(
    ("corruption", "diagnostic"),
    [
        ("scope-shape", "scope, results, or summary"),
        ("results-shape", "scope, results, or summary"),
        ("summary-shape", "scope, results, or summary"),
        ("generation-errors-missing", "failed to transform"),
        ("generation-errors-shape", "failed to transform"),
        ("generation-errors", "failed to transform"),
        ("source-files-shape", "missing source files"),
        ("ids-shape", "missing source files"),
        ("empty-source", "source-file scope is empty"),
        ("duplicate-source", "source-file scope is empty"),
        ("unsorted-source", "source-file scope is empty"),
        ("invalid-source", "invalid source path"),
        ("empty-ids", "generated mutant IDs are empty"),
        ("duplicate-ids", "generated mutant IDs are empty"),
        ("unsorted-ids", "generated mutant IDs are empty"),
        ("invalid-result-shape", "invalid or unresolved status"),
        ("foreign-result", "duplicate, missing, or foreign"),
        ("missing-result", "duplicate, missing, or foreign"),
        ("duplicate-result", "duplicate, missing, or foreign"),
        ("summary-count", "summary counts do not match"),
        ("invalid-status", "invalid or unresolved status"),
        ("file-count", "per-file results do not match"),
        ("files-shape", "file breakdown is invalid"),
        ("outside-source-scope", "outside generated source scope"),
        ("foreign-file", "foreign path"),
    ],
)
def test_postflight_rejects_inconsistent_native_report_evidence(
    campaign_project: tuple[Path, dict[str, str]],
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    corruption: str,
    diagnostic: str,
) -> None:
    repo, _env = campaign_project
    report = _native_report(repo, ["zapped"])
    scope = cast(dict[str, object], report["scope"])
    results = cast(list[dict[str, object]], report["results"])
    summary = cast(dict[str, object], report["summary"])
    files = cast(dict[str, dict[str, object]], report["files"])
    source_abs = str(repo / "src/omi_collector/demo.py")
    other_abs = str(repo / "src/omi_collector/other.py")

    def omit_generation_errors() -> None:
        scope.pop("generation_errors")

    def mark_outside_source_scope() -> None:
        results[0].update(file_path=other_abs)
        report.update(files={other_abs: {"total": 1}})

    def mark_foreign_file() -> None:
        results[0].update(file_path="/outside/foreign.py")
        report.update(files={"/outside/foreign.py": {"total": 1}})

    corruptions: dict[str, Callable[[], None]] = {
        "scope-shape": lambda: report.update(scope=[]),
        "results-shape": lambda: report.update(results={}),
        "summary-shape": lambda: report.update(summary=[]),
        "generation-errors-missing": omit_generation_errors,
        "generation-errors-shape": lambda: scope.update(generation_errors="none"),
        "generation-errors": lambda: scope.update(generation_errors=["demo.py"]),
        "source-files-shape": lambda: scope.update(source_files="src/omi_collector/demo.py"),
        "ids-shape": lambda: scope.update(gremlin_ids="g-000"),
        "empty-source": lambda: scope.update(source_files=[]),
        "duplicate-source": lambda: scope.update(source_files=["src/omi_collector/demo.py"] * 2),
        "unsorted-source": lambda: scope.update(source_files=["src/z_demo.py", "src/omi_collector/demo.py"]),
        "invalid-source": lambda: scope.update(source_files=["../demo.py"]),
        "empty-ids": lambda: scope.update(gremlin_ids=[]),
        "duplicate-ids": lambda: scope.update(gremlin_ids=["g-000", "g-000"]),
        "unsorted-ids": lambda: scope.update(gremlin_ids=["g-002", "g-001"]),
        "invalid-result-shape": lambda: report.update(results=["not-a-result"]),
        "foreign-result": lambda: results[0].update(gremlin_id="g-foreign"),
        "missing-result": lambda: report.update(results=[]),
        "duplicate-result": lambda: results.append(results[0].copy()),
        "summary-count": lambda: summary.update(zapped=0),
        "invalid-status": lambda: results[0].update(status="controller_failed"),
        "file-count": lambda: files[source_abs].update(total=2),
        "files-shape": lambda: report.update(files=[]),
        "outside-source-scope": mark_outside_source_scope,
        "foreign-file": mark_foreign_file,
    }
    corruptions[corruption]()

    with pytest.raises(ValueError, match=diagnostic):
        _postflight(repo, tmp_path, monkeypatch, report)
