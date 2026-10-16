from __future__ import annotations

import shlex
from pathlib import Path

import test_systemd_unit as deploy_tests


def _write_journal(harness: deploy_tests._DeploymentHarness, body: str, *, fail: bool = False) -> None:
    journal = Path(harness.environment["PATH"].split(":")[0]) / "journalctl"
    journal.write_text(
        "#!/usr/bin/env bash\n"
        f'printf "journalctl args=%s\\n" "$*" >> {shlex.quote(str(harness.log))}\n'
        + ("exit 1\n" if fail else f"printf '%s\\n' {shlex.quote(body)}\n"),
        encoding="utf-8",
    )
    journal.chmod(0o755)


def test_deployer_waits_for_readiness_beyond_five_elapsed_polls(tmp_path: Path) -> None:
    harness = deploy_tests._deployment_harness(tmp_path)
    _write_journal(
        harness,
        '{"config":"' + str(harness.config_file) + '","status":"deployment_ready"}',
    )
    journal = Path(harness.environment["PATH"].split(":")[0]) / "journalctl"
    journal.write_text(
        "#!/usr/bin/env bash\n"
        f"count_file={shlex.quote(str(tmp_path / 'journal.count'))}\n"
        'count=0; [[ ! -f "$count_file" ]] || read -r count < "$count_file"\n'
        'count=$((count + 1)); printf "%s\\n" "$count" > "$count_file"\n'
        "if (( count < 8 )); then exit 0; fi\n"
        f"printf '%s\\n' {shlex.quote('{"config":"' + str(harness.config_file) + '","status":"deployment_ready"}')}\n",
        encoding="utf-8",
    )
    journal.chmod(0o755)

    result = deploy_tests._run_deployer(harness)

    assert result.returncode == 0, result.stderr
    assert int((tmp_path / "journal.count").read_text(encoding="utf-8")) == 8
    assert "Deployed and verified" in result.stdout


def test_deployer_preserves_same_live_invocation_when_journal_observation_fails(tmp_path: Path) -> None:
    harness = deploy_tests._deployment_harness(tmp_path)
    previous = deploy_tests._previous_release(harness)
    _write_journal(harness, "", fail=True)

    result = deploy_tests._run_deployer(harness)

    assert result.returncode != 0
    assert "selected release is unverified, running in the same invocation" in result.stderr
    assert "invocation=" + "a" * 32 in result.stderr
    assert harness.current_link.resolve(strict=True) != previous
    commands = harness.log.read_text(encoding="utf-8")
    assert commands.count("systemctl args=stop omi-collector.service") == 1
    assert "systemctl args=restart omi-collector.service" in commands
    assert "systemctl args=stop omi-collector.service" in commands
    assert set((harness.deployment_root / "releases").glob("release-*")) == {
        previous,
        harness.current_link.resolve(strict=True),
    }


def test_deployer_preserves_same_live_invocation_when_interrupted(tmp_path: Path) -> None:
    harness = deploy_tests._deployment_harness(tmp_path)
    sleep = Path(harness.environment["PATH"].split(":")[0]) / "sleep"
    _write_journal(harness, "")
    sleep.write_text('#!/usr/bin/env bash\nkill -TERM "$PPID"\nexit 0\n', encoding="utf-8")
    sleep.chmod(0o755)

    result = deploy_tests._run_deployer(harness)

    assert result.returncode == 143
    assert "selected release is unverified, running in the same invocation" in result.stderr
    assert "invocation=" + "a" * 32 in result.stderr
    assert harness.log.read_text(encoding="utf-8").count("systemctl args=stop omi-collector.service") == 1


def test_deployer_stops_same_pid_with_new_invocation_during_stability(tmp_path: Path) -> None:
    harness = deploy_tests._deployment_harness(tmp_path, deploy_tests._DeploymentScenario(crash_loop=True))
    systemctl = Path(harness.environment["PATH"].split(":")[0]) / "systemctl"
    systemctl.write_text(
        systemctl.read_text(encoding="utf-8").replace("pid=5252; restarts=1;", "pid=4242; restarts=1;"),
        encoding="utf-8",
    )

    result = deploy_tests._run_deployer(harness)

    assert result.returncode != 0
    assert "restarted during the stability interval" in result.stderr
    assert "requires forward Collector recovery" in result.stderr
    commands = harness.log.read_text(encoding="utf-8")
    assert commands.count("systemctl args=stop omi-collector.service") == 2


def test_unverified_release_can_be_checked_read_only_afterward(tmp_path: Path) -> None:
    harness = deploy_tests._deployment_harness(tmp_path)
    _write_journal(harness, "", fail=True)
    result = deploy_tests._run_deployer(harness)
    assert result.returncode != 0

    selected = harness.current_link.resolve(strict=True)
    before = harness.log.read_text(encoding="utf-8")
    assert selected.is_dir()
    assert "systemctl args=restart omi-collector.service" in before
    _write_journal(
        harness,
        '{"config":"' + str(harness.config_file) + '","status":"deployment_ready"}',
    )
    bin_dir = harness.environment["PATH"].split(":")[0]
    snapshot = deploy_tests.subprocess.run(
        [str(Path(bin_dir) / "systemctl"), "show", "omi-collector.service"],
        check=True,
        capture_output=True,
        text=True,
        env=harness.environment,
    )
    deploy_tests.subprocess.run(
        [str(Path(bin_dir) / "sleep"), "6"],
        check=True,
        capture_output=True,
        text=True,
        env=harness.environment,
    )
    stable_snapshot = deploy_tests.subprocess.run(
        [str(Path(bin_dir) / "systemctl"), "show", "omi-collector.service"],
        check=True,
        capture_output=True,
        text=True,
        env=harness.environment,
    )
    journal = deploy_tests.subprocess.run(
        [
            str(Path(bin_dir) / "journalctl"),
            "--unit",
            "omi-collector.service",
            "_SYSTEMD_INVOCATION_ID=" + "a" * 32,
            "--output",
            "cat",
            "--no-pager",
        ],
        check=True,
        capture_output=True,
        text=True,
        env=harness.environment,
    )

    assert harness.current_link.resolve(strict=True) == selected
    assert "MainPID=4242" in snapshot.stdout
    assert snapshot.stdout == stable_snapshot.stdout
    assert '{"config":"' + str(harness.config_file) + '","status":"deployment_ready"}' in journal.stdout
    after = harness.log.read_text(encoding="utf-8")
    assert "systemctl args=restart omi-collector.service" not in after[len(before) :]
    assert "candidate args=init-state" not in after[len(before) :]
