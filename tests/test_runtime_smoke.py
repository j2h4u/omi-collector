import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

import pytest


@pytest.fixture
def fake_docker(tmp_path: Path) -> tuple[Path, Path]:
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    log = tmp_path / "docker.jsonl"
    docker = bin_dir / "docker"
    docker.write_text(
        f"#!{sys.executable}\n"
        "import json, os, sys, time\n"
        f"with open({str(log)!r}, 'a', encoding='utf-8') as stream:\n"
        "    stream.write(json.dumps(sys.argv[1:]) + '\\n')\n"
        "args = sys.argv[1:]\n"
        "command = next((item for item in ('up', 'exec', 'down') if item in args), '')\n"
        "if command == 'exec' and os.environ.get('FAKE_DOCKER_EXEC_DELAY'):\n"
        "    time.sleep(float(os.environ['FAKE_DOCKER_EXEC_DELAY']))\n"
        "if args[0] == 'inspect':\n"
        "    print(os.environ.get('FAKE_DOCKER_INSPECT_STATE', 'true healthy'))\n"
        "elif args[0] == 'compose' and 'ps' in args:\n"
        "    print('fake-container-id')\n"
        "elif command == 'exec':\n"
        "    print('ok')\n"
        "raise SystemExit(int(os.environ.get(f'FAKE_DOCKER_{command.upper()}_STATUS', '0')))\n",
        encoding="utf-8",
    )
    docker.chmod(0o755)
    return bin_dir, log


def _environment(bin_dir: Path, **values: str) -> dict[str, str]:
    return {**os.environ, "PATH": f"{bin_dir}:{os.environ['PATH']}", **values}


def _calls(log: Path) -> list[list[str]]:
    if not log.exists():
        return []
    return [json.loads(line) for line in log.read_text(encoding="utf-8").splitlines()]


def _run_smoke(bin_dir: Path, **values: str) -> subprocess.CompletedProcess[str]:
    process = subprocess.Popen(
        ["just", "runtime-smoke"],
        env=_environment(bin_dir, **values),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        start_new_session=True,
    )
    try:
        stdout, stderr = process.communicate(timeout=15)
    except subprocess.TimeoutExpired:
        os.killpg(process.pid, signal.SIGKILL)
        stdout, stderr = process.communicate()
        raise
    return subprocess.CompletedProcess(process.args, process.returncode, stdout, stderr)


def test_runtime_smoke_cleans_up_after_success(fake_docker: tuple[Path, Path]) -> None:
    bin_dir, log = fake_docker

    result = _run_smoke(bin_dir)

    assert result.returncode == 0
    calls = _calls(log)
    assert any("down" in call for call in calls)
    inspect_calls = [call for call in calls if call[0] == "inspect"]
    assert len(inspect_calls) == 1
    assert ".State.Running" in " ".join(inspect_calls[0])
    assert ".State.Health.Status" in " ".join(inspect_calls[0])


def test_runtime_smoke_rejects_healthy_but_stopped_container(fake_docker: tuple[Path, Path]) -> None:
    bin_dir, log = fake_docker

    result = _run_smoke(bin_dir, FAKE_DOCKER_INSPECT_STATE="false healthy")

    assert result.returncode != 0
    assert "not running and healthy: false healthy" in result.stderr
    assert any("down" in call for call in _calls(log))


def test_runtime_smoke_reports_cleanup_failure_after_success(fake_docker: tuple[Path, Path]) -> None:
    bin_dir, log = fake_docker

    result = _run_smoke(bin_dir, FAKE_DOCKER_DOWN_STATUS="19")

    assert result.returncode != 0
    assert "Failed to clean up Docker project omi-collector-qa-" in result.stderr
    assert any("down" in call for call in _calls(log))


def test_runtime_smoke_preserves_start_failure_when_cleanup_fails(fake_docker: tuple[Path, Path]) -> None:
    bin_dir, log = fake_docker

    result = _run_smoke(bin_dir, FAKE_DOCKER_UP_STATUS="23", FAKE_DOCKER_DOWN_STATUS="19")

    assert result.returncode == 23
    assert "Failed to clean up Docker project omi-collector-qa-" in result.stderr
    assert any("down" in call for call in _calls(log))


def test_runtime_smoke_cleans_up_after_term(fake_docker: tuple[Path, Path]) -> None:
    bin_dir, log = fake_docker
    shell_pid = log.parent / "recipe-shell.pid"
    bash_env = log.parent / "bash-env"
    bash_env.write_text('printf \'%s\\n\' "$$" > "$RECIPE_PID_FILE"\n', encoding="utf-8")
    process = subprocess.Popen(
        ["just", "runtime-smoke"],
        env=_environment(
            bin_dir,
            BASH_ENV=str(bash_env),
            FAKE_DOCKER_EXEC_DELAY="2",
            RECIPE_PID_FILE=str(shell_pid),
        ),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        start_new_session=True,
    )
    try:
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline and not any("exec" in call for call in _calls(log)):
            time.sleep(0.01)
        assert any("exec" in call for call in _calls(log))
        os.kill(int(shell_pid.read_text(encoding="utf-8")), signal.SIGTERM)
        _, stderr = process.communicate(timeout=10)
    finally:
        if process.poll() is None:
            os.killpg(process.pid, signal.SIGKILL)
            process.communicate(timeout=5)

    assert process.returncode != 0
    assert "down" in " ".join(" ".join(call) for call in _calls(log))
    assert "Failed to clean up" not in stderr
