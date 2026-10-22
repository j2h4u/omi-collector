from __future__ import annotations

from pathlib import Path

import test_systemd_unit as deploy_tests

_INVOCATION = "a" * 32
_READY = f"active:4242:0:{_INVOCATION}:0"
_STARTING = f"activating:4242:0:{_INVOCATION}:0"


def _readiness_polls(log: Path) -> int:
    return log.read_text(encoding="utf-8").count(
        "systemctl show omi-collector.service --property=ActiveState --property=MainPID"
    )


def test_deployer_waits_for_readiness_beyond_five_elapsed_polls(tmp_path: Path) -> None:
    deployer, environment = deploy_tests._fake_deployer(
        tmp_path, readiness_sequence=(*(_STARTING for _ in range(7)), _READY)
    )

    result = deploy_tests._run_fake_deployer(deployer, environment)

    assert result.returncode == 0, result.stderr
    # Eight readiness polls are followed by one final stability snapshot.
    assert _readiness_polls(Path(environment["TEST_LOG"])) == 9
    assert "Deployed and verified" in result.stdout


def test_deployer_keeps_live_invocation_selected_when_readiness_times_out(tmp_path: Path) -> None:
    deployer, environment = deploy_tests._fake_deployer(tmp_path, readiness_sequence=(_STARTING,))
    content = deployer.read_text(encoding="utf-8").replace(
        "readiness_timeout_seconds=900", "readiness_timeout_seconds=0"
    )
    deployer.write_text(content, encoding="utf-8")

    result = deploy_tests._run_fake_deployer(deployer, environment)

    runtime = Path(environment["TEST_HOME"]) / ".local/share/omi-collector"
    assert result.returncode != 0
    assert "deployment remains unverified and in progress" in result.stderr
    assert (runtime / "current").is_symlink()
    assert Path(tmp_path / "service.active").exists()
    assert Path(environment["TEST_LOG"]).read_text(encoding="utf-8").count("systemctl stop omi-collector.service") == 1


def test_deployer_keeps_live_invocation_selected_when_interrupted(tmp_path: Path) -> None:
    deployer, environment = deploy_tests._fake_deployer(tmp_path, readiness_sequence=(_STARTING,))
    sleep = Path(environment["PATH"].split(":")[0]) / "sleep"
    sleep.write_text('#!/usr/bin/env bash\nkill -TERM "$PPID"\nexit 0\n', encoding="utf-8")
    sleep.chmod(0o755)

    result = deploy_tests._run_fake_deployer(deployer, environment)

    runtime = Path(environment["TEST_HOME"]) / ".local/share/omi-collector"
    assert result.returncode == 143
    assert "deployment remains unverified and in progress" in result.stderr
    assert (runtime / "current").is_symlink()
    assert Path(tmp_path / "service.active").exists()
    assert Path(environment["TEST_LOG"]).read_text(encoding="utf-8").count("systemctl stop omi-collector.service") == 1


def test_deployer_stops_same_pid_with_new_invocation_during_stability(tmp_path: Path) -> None:
    changed_invocation = f"active:4242:1:{'b' * 32}:0"
    deployer, environment = deploy_tests._fake_deployer(tmp_path, readiness_sequence=(_READY, changed_invocation))

    result = deploy_tests._run_fake_deployer(deployer, environment)

    assert result.returncode != 0
    assert "service restarted during stability interval" in result.stderr
    assert "systemctl stop omi-collector.service" in Path(environment["TEST_LOG"]).read_text(encoding="utf-8")


def test_unverified_release_can_be_checked_read_only_afterward(tmp_path: Path) -> None:
    deployer, environment = deploy_tests._fake_deployer(tmp_path, readiness_sequence=(_STARTING,))
    content = deployer.read_text(encoding="utf-8").replace(
        "readiness_timeout_seconds=900", "readiness_timeout_seconds=0"
    )
    deployer.write_text(content, encoding="utf-8")
    result = deploy_tests._run_fake_deployer(deployer, environment)
    assert result.returncode != 0

    runtime = Path(environment["TEST_HOME"]) / ".local/share/omi-collector"
    selected = (runtime / "current").resolve(strict=True)
    before = Path(environment["TEST_LOG"]).read_text(encoding="utf-8")
    assert selected.is_dir()
    snapshot = deploy_tests.subprocess.run(
        [
            str(Path(environment["PATH"].split(":")[0]) / "systemctl"),
            "show",
            "omi-collector.service",
            "--property=ActiveState",
            "--property=MainPID",
            "--property=NRestarts",
            "--property=InvocationID",
            "--property=Job",
        ],
        check=True,
        capture_output=True,
        text=True,
        env=environment,
    )

    assert (runtime / "current").resolve(strict=True) == selected
    assert "ActiveState=activating" in snapshot.stdout
    after = Path(environment["TEST_LOG"]).read_text(encoding="utf-8")
    assert "systemctl restart --no-block omi-collector.service" not in after[len(before) :]
    assert "collector init-state" not in after[len(before) :]
