from __future__ import annotations

import fcntl
import json
import os
import pwd
import shlex
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

_ROOT = Path(__file__).parents[1]
_UNIT = _ROOT / "systemd" / "omi-collector.service"
_INSTALLER = _ROOT / "scripts" / "install-systemd-unit.sh"
_HELPER = _ROOT / "scripts" / "omi-collector-bluetooth-ready"
_DEPLOYER = _ROOT / "scripts" / "deploy-systemd-service.sh"
_RELEASE_DEPLOYER = _ROOT / "scripts" / "dev-deploy-release.sh"
_STATUS = _ROOT / "scripts" / "omi-collector-status"
_SOURCE_PACKAGE = _ROOT / "src" / "omi_collector"


def test_unit_runs_as_rendered_owner_with_read_only_selected_code() -> None:
    unit = _UNIT.read_text(encoding="utf-8")

    assert "User=@OMI_COLLECTOR_USER@" in unit
    assert "Group=@OMI_COLLECTOR_GROUP@" in unit
    assert "ExecStart=@OMI_COLLECTOR_HOME@/.local/share/omi-collector/current/bin/omi-collector" in unit
    assert "ExecStartPre=+/" not in unit
    assert "ExecStartPre=/usr/local/sbin/omi-collector-bluetooth-ready" in unit
    assert "Requires=bluetooth.service" in unit and "After=bluetooth.service" in unit
    assert "AmbientCapabilities=CAP_NET_RAW" in unit
    assert "ProtectHome=read-only" in unit
    assert ".local/share/omi-collector" not in unit.split("ReadWritePaths=", 1)[1]
    assert "StateDirectory=" not in unit
    assert "/var/lib/omi-collector" not in unit


def test_installer_revokes_old_root_entrypoints_before_installing_the_unit() -> None:
    installer = _INSTALLER.read_text(encoding="utf-8")

    revoke = installer.index("rm -f -- /usr/local/sbin/omi-collector-deploy-release")
    install = installer.index('install -o root -g root -m 0644 -- "$staged_unit" "$target_unit"')
    assert revoke < install
    assert "@OMI_COLLECTOR_HOME@" in installer
    assert "systemctl start --no-block omi-collector.service" in installer
    assert "systemctl stop omi-collector.service" in installer
    assert "systemctl restart omi-collector.service" in installer
    assert "useradd" not in installer and "groupadd" not in installer
    assert 'chown root:root -- "$staged_unit"' in installer


def test_status_and_release_helpers_are_unprivileged() -> None:
    status = _STATUS.read_text(encoding="utf-8")
    deploy = _RELEASE_DEPLOYER.read_text(encoding="utf-8")

    assert "EUID != 0" in status and "sudo " not in status
    assert "current/bin/omi-collector" in status
    assert "EUID != 0" in deploy and "sudo " not in deploy
    assert "worktree add --detach" in deploy
    assert "worktree remove --force" in deploy
    assert not (_ROOT / "scripts" / "omi-collector-deploy-release").exists()
    assert not (_ROOT / "scripts" / "omi-collector-deploy-release.sudoers").exists()
    assert not (_ROOT / "scripts" / "omi-collector-status.sudoers").exists()
    assert not (_ROOT / "scripts" / "seal_deployment_tree.py").exists()
    assert not (_ROOT / "tests" / "test_deployment_sealer.py").exists()


@pytest.mark.parametrize(
    "script",
    (_INSTALLER, _DEPLOYER, _RELEASE_DEPLOYER, _STATUS, _HELPER),
)
def test_shell_scripts_are_syntactically_valid(script: Path) -> None:
    subprocess.run(("bash", "-n", str(script)), check=True)


def test_bluetooth_ready_powers_controller_with_bounded_polling(tmp_path: Path) -> None:
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    bluetoothctl = fake_bin / "bluetoothctl"
    bluetoothctl.write_text(
        "#!/usr/bin/env bash\n"
        'if [[ "$*" == *" show" ]]; then printf "Controller AA:BB:CC:DD:EE:FF\n Powered: yes\n"; fi\n'
        'printf "%s\\n" "$*" >> "$BLUETOOTH_LOG"\n',
        encoding="utf-8",
    )
    bluetoothctl.chmod(0o755)
    sleep = fake_bin / "sleep"
    sleep.write_text("#!/usr/bin/env bash\nexit 0\n", encoding="utf-8")
    sleep.chmod(0o755)
    log = tmp_path / "bluetooth.log"
    helper = tmp_path / "omi-collector-bluetooth-ready"
    helper.write_text(
        _HELPER.read_text(encoding="utf-8").replace("/usr/bin/bluetoothctl", str(bluetoothctl)), encoding="utf-8"
    )
    helper.chmod(0o755)
    result = subprocess.run(
        (str(helper),),
        env={**os.environ, "PATH": f"{fake_bin}:{os.environ['PATH']}", "BLUETOOTH_LOG": str(log)},
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    assert "show" in log.read_text(encoding="utf-8")
    assert "--timeout 10 power on" in log.read_text(encoding="utf-8")


def _fake_deployer(
    tmp_path: Path,
    *,
    stopped_refusal: bool = False,
    readiness_sequence: tuple[str, ...] = ("active:4242:0:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa:0",),
    hot_journal: bool = False,
    recovery_fail: bool = False,
) -> tuple[Path, dict[str, str]]:
    repo = tmp_path / "repo"
    scripts = repo / "scripts"
    source = repo / "src" / "omi_collector"
    scripts.mkdir(parents=True)
    source.parent.mkdir(parents=True)
    shutil.copytree(_SOURCE_PACKAGE, source)
    shutil.copy2(_DEPLOYER, scripts / _DEPLOYER.name)
    shutil.copy2(
        _ROOT / "scripts" / "check_fallback_ledger_compatibility.py", scripts / "check_fallback_ledger_compatibility.py"
    )
    subprocess.run(("git", "init", "--quiet", str(repo)), check=True)
    subprocess.run(
        ("git", "-C", str(repo), "-c", "user.name=Test", "-c", "user.email=test@example.invalid", "add", "."),
        check=True,
    )
    subprocess.run(
        (
            "git",
            "-C",
            str(repo),
            "-c",
            "user.name=Test",
            "-c",
            "user.email=test@example.invalid",
            "commit",
            "--quiet",
            "-m",
            "fixture",
        ),
        check=True,
    )
    deployer = scripts / _DEPLOYER.name
    content = deployer.read_text(encoding="utf-8").replace(
        "(( EUID != 0 )) || die 'run as the service owner, without sudo'", "true"
    )
    config = tmp_path / "config.toml"
    state_file = tmp_path / "collector-state.sqlite3"
    content = content.replace(
        "declare config_file='/srv/pipelines/omi/config.toml' state_file='/srv/pipelines/omi/collector/collector-state.sqlite3'",
        f"declare config_file={shlex.quote(str(config))} state_file={shlex.quote(str(state_file))}",
    )
    deployer.write_text(content, encoding="utf-8")
    deployer.chmod(0o755)
    assert f"declare config_file={shlex.quote(str(config))}" in deployer.read_text(encoding="utf-8")
    assert "/srv/pipelines/omi/config.toml" not in deployer.read_text(encoding="utf-8")

    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    home = tmp_path / "home"
    home.mkdir()
    user = pwd.getpwuid(os.getuid())
    config.write_text('pendant_address = "12:34:56:78:9A:BC"\nbundle_min_audio_seconds = 3600.0\n', encoding="utf-8")
    active = tmp_path / "service.active"
    active.touch()
    log = tmp_path / "commands.log"
    call_count = tmp_path / "state-checks"
    (fake_bin / "getent").write_text(
        "#!/usr/bin/env bash\n"
        f'if [[ "$1 $2" == "passwd {user.pw_name}" ]]; then printf \'%s\\n\' '
        f"{shlex.quote(f'{user.pw_name}:x:{user.pw_uid}:{user.pw_gid}:test:{home}:/bin/bash')}; exit 0; fi\n"
        'exec /usr/bin/getent "$@"\n',
        encoding="utf-8",
    )
    (fake_bin / "getent").chmod(0o755)
    (fake_bin / "git").write_text(
        '#!/usr/bin/env bash\ncase "$3" in rev-parse|archive) exec /usr/bin/git "$@" ;; status) [[ "${FAKE_GIT_ERROR:-0}" == 1 ]] && exit 3; [[ "${FAKE_GIT_DIRTY:-0}" == 1 ]] && printf \' M source.py\\n\'; exit 0 ;; *) exit 2 ;; esac\n',
        encoding="utf-8",
    )
    (fake_bin / "git").chmod(0o755)
    (fake_bin / "uv").write_text(
        "#!/usr/bin/env bash\n"
        f'{shlex.quote(sys.executable)} -m venv --without-pip --copies "$UV_PROJECT_ENVIRONMENT"\n'
        "project=${3}\n"
        'purelib=$("$UV_PROJECT_ENVIRONMENT/bin/python" -I -B -c \'import sysconfig; print(sysconfig.get_path("purelib"))\')\n'
        'cp -a "$project/src/omi_collector" "$purelib/omi_collector"\n'
        f'[[ "${{FAKE_EDIT_CHECKOUT:-0}}" == 1 ]] && printf "# concurrent edit\\n" >> {shlex.quote(str(source / "__init__.py"))}\n'
        "cat > \"$UV_PROJECT_ENVIRONMENT/bin/omi-collector\" <<'CLI'\n"
        "#!/usr/bin/env bash\n"
        f"printf 'collector %s\\n' \"$*\" >> {shlex.quote(str(log))}\n"
        'case "$1" in\n'
        'config) if [[ "${FAKE_CONFIG_INVALID:-0}" == 1 ]]; then printf \'{"config":"wrong","status":"config_valid"}\\n\'; else printf \'{"config":"%s","status":"config_valid"}\\n\' "$4"; fi ;;\n'
        "check-state)\n"
        f"  count=0; [[ ! -f {shlex.quote(str(call_count))} ]] || read -r count < {shlex.quote(str(call_count))}\n"
        f"  count=$((count+1)); printf '%s\\n' \"$count\" > {shlex.quote(str(call_count))}\n"
        f'  if [[ "$*" == *--stopped* && {int(stopped_refusal)} == 1 ]]; then printf \'{{"detail":"refused","reason":"state_validation_failed","status":"state_refused"}}\\n\'; exit 1; fi\n'
        f'  printf \'hot=%s args=%s\\n\' "$FAKE_HOT_JOURNAL" "$*" >> {shlex.quote(str(log))}\n'
        f'  if [[ {int(hot_journal)} == 1 && ! -f {shlex.quote(str(call_count))}.recovered ]]; then printf \'{{"reason":"canonical_hot_journal","status":"recovery_required"}}\\n\'; exit 1; fi\n'
        '  printf \'{"schema_version":4,"status":"state_compatible"}\\n\' ;;\n'
        f"recover-state-journal) printf 'collector recover-state-journal\\n' >> {shlex.quote(str(log))}; [[ {int(recovery_fail)} == 1 ]] && exit 1; touch {shlex.quote(str(call_count))}.recovered; printf '{{\"status\":\"recovered\"}}\\n' ;;\n"
        'init-state) [[ "${FAKE_INIT_FAIL:-0}" == 1 ]] && exit 1; printf \'{"schema_version":4}\\n\' ;;\n'
        "*) exit 2 ;;\n"
        "esac\n"
        'CLI\nchmod 0755 "$UV_PROJECT_ENVIRONMENT/bin/omi-collector"\n',
        encoding="utf-8",
    )
    (fake_bin / "uv").chmod(0o755)
    fake_systemctl = fake_bin / "systemctl"
    fake_systemctl.write_text(
        "#!/usr/bin/env bash\n"
        f"printf 'systemctl %s\\n' \"$*\" >> {shlex.quote(str(log))}\n"
        'case "$1" in\n'
        f"show) if [[ \"$*\" != *MainPID* ]]; then [[ -e {shlex.quote(str(active))} ]] && printf 'ActiveState=active\\n' || printf 'ActiveState=inactive\\n'; exit 0; fi\n"
        "  IFS=';' read -r -a states <<< \"$READINESS_SEQUENCE\"\n"
        f"  index=0; [[ ! -f {shlex.quote(str(call_count))}.systemctl ]] || read -r index < {shlex.quote(str(call_count))}.systemctl\n"
        f"  printf '%s\\n' $((index+1)) > {shlex.quote(str(call_count))}.systemctl\n"
        '  (( index < ${#states[@]} )) || index=$((${#states[@]}-1)); IFS=: read -r active_state main_pid restarts invocation job <<< "${states[index]}"\n'
        '  printf \'ActiveState=%s\\nMainPID=%s\\nNRestarts=%s\\nInvocationID=%s\\nJob=%s\\n\' "$active_state" "$main_pid" "$restarts" "$invocation" "$job" ;;\n'
        f"stop) rm -f {shlex.quote(str(active))} ;;\n"
        f"start|restart) touch {shlex.quote(str(active))} ;;\n"
        "*) exit 2 ;;\nesac\n",
        encoding="utf-8",
    )
    fake_systemctl.chmod(0o755)
    (fake_bin / "sudo").write_text(
        '#!/usr/bin/env bash\n[[ "$1" == -n ]] && shift\n'
        f'if [[ "$1" == /usr/bin/systemctl ]]; then shift; exec {shlex.quote(str(fake_systemctl))} "$@"; fi\n'
        'exec "$@"\n',
        encoding="utf-8",
    )
    (fake_bin / "sudo").chmod(0o755)
    (fake_bin / "sleep").write_text("#!/usr/bin/env bash\nexit 0\n", encoding="utf-8")
    (fake_bin / "sleep").chmod(0o755)
    env = {
        **os.environ,
        "PATH": f"{fake_bin}:{os.environ['PATH']}",
        "TEST_HOME": str(home),
        "TEST_CONFIG": str(config),
        "FAKE_GIT_DIRTY": "0",
        "FAKE_GIT_ERROR": "0",
        "FAKE_CONFIG_INVALID": "0",
        "FAKE_INIT_FAIL": "0",
        "TEST_LOG": str(log),
        "READINESS_SEQUENCE": ";".join(readiness_sequence),
    }
    return deployer, env


def test_deployer_builds_frozen_commit_when_checkout_changes_during_sync(tmp_path: Path) -> None:
    deployer, env = _fake_deployer(tmp_path)
    env["FAKE_EDIT_CHECKOUT"] = "1"

    result = _run_fake_deployer(deployer, env)

    runtime = Path(env["TEST_HOME"]) / ".local/share/omi-collector"
    assert result.returncode == 0, result.stderr
    selected = (runtime / "current").resolve(strict=True)
    revision = subprocess.check_output(
        ("git", "-C", str(deployer.parent.parent), "rev-parse", "HEAD"), text=True
    ).strip()
    assert json.loads((selected / "share/omi-collector/release.json").read_text(encoding="utf-8")) == {
        "source_revision": revision
    }
    installed_source = next(selected.glob("lib/python*/site-packages/omi_collector/__init__.py"))
    assert "concurrent edit" not in installed_source.read_text(encoding="utf-8")
    assert "concurrent edit" in (deployer.parent.parent / "src/omi_collector/__init__.py").read_text(encoding="utf-8")


def test_deployer_does_not_report_success_when_snapshot_cleanup_fails(tmp_path: Path) -> None:
    deployer, env = _fake_deployer(tmp_path)
    fake_rm = Path(env["PATH"].split(":", 1)[0]) / "rm"
    fake_rm.write_text(
        '#!/usr/bin/env bash\n/usr/bin/rm "$@" || exit $?\n[[ "$*" == *omi-collector-source.* ]] && exit 1\nexit 0\n',
        encoding="utf-8",
    )
    fake_rm.chmod(0o755)

    result = _run_fake_deployer(deployer, env)

    assert result.returncode != 0
    assert "could not remove source snapshot" in result.stderr
    assert "Deployed and verified" not in result.stdout


def test_release_helper_fails_when_successful_deploy_cannot_clean_worktree(tmp_path: Path) -> None:
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    temp = tmp_path / "temp"
    temp.mkdir()
    (fake_bin / "git").write_text(
        "#!/usr/bin/env bash\n"
        'case "$3" in\n'
        "status|fetch|show-ref|merge-base) exit 0 ;;\n"
        "rev-list) printf '%040d\\n' 0 ;;\n"
        'worktree) if [[ "$4" == add ]]; then mkdir -p "$6/scripts"; printf \'#!/usr/bin/env bash\\necho deployed\\n\' > "$6/scripts/deploy-systemd-service.sh"; chmod +x "$6/scripts/deploy-systemd-service.sh"; else exit 1; fi ;;\n'
        "*) exit 2 ;;\nesac\n",
        encoding="utf-8",
    )
    (fake_bin / "git").chmod(0o755)
    result = subprocess.run(
        (str(_RELEASE_DEPLOYER), "v1.2.3"),
        cwd=_ROOT,
        env={**os.environ, "PATH": f"{fake_bin}:{os.environ['PATH']}", "TMPDIR": str(temp)},
        capture_output=True,
        text=True,
        check=False,
    )

    leftovers = list(temp.iterdir())
    assert result.returncode != 0
    assert "could not remove release worktree" in result.stderr
    assert "Deployed v1.2.3" not in result.stdout
    assert leftovers
    shutil.rmtree(leftovers[0])


def _run_fake_deployer(deployer: Path, env: dict[str, str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        (str(deployer),), env=env, cwd=deployer.parent.parent, text=True, capture_output=True, check=False
    )


def test_deployer_builds_and_selects_candidate_then_removes_previous_release(tmp_path: Path) -> None:
    deployer, env = _fake_deployer(tmp_path)
    runtime = Path(env["TEST_HOME"]) / ".local/share/omi-collector"
    previous = runtime / "releases" / ("release-" + "1" * 40 + "-1-1")
    previous.mkdir(parents=True)
    fake_python = previous / "bin/python"
    fake_python.parent.mkdir()
    fake_python.write_text("#!/usr/bin/env bash\nexit 0\n", encoding="utf-8")
    fake_python.chmod(0o755)
    (runtime / "current").symlink_to(f"releases/{previous.name}")

    result = _run_fake_deployer(deployer, env)

    assert result.returncode == 0, result.stderr
    selected = (runtime / "current").resolve(strict=True)
    assert selected.parent == runtime / "releases"
    assert not previous.exists()
    revision = subprocess.check_output(
        ("git", "-C", str(deployer.parent.parent), "rev-parse", "HEAD"), text=True
    ).strip()
    assert json.loads((selected / "share/omi-collector/release.json").read_text(encoding="utf-8")) == {
        "source_revision": revision
    }
    commands = Path(env["TEST_LOG"]).read_text(encoding="utf-8")
    assert commands.index("systemctl stop") < commands.index("--stopped") if "--stopped" in commands else True
    assert "UV_LINK_MODE=hardlink" in deployer.read_text(encoding="utf-8")
    assert commands.index("systemctl restart --no-block") < commands.index(
        "systemctl show omi-collector.service --property=ActiveState --property=MainPID"
    )


def test_deployer_restores_compatible_previous_release_on_stopped_state_refusal(tmp_path: Path) -> None:
    deployer, env = _fake_deployer(tmp_path, stopped_refusal=True)
    runtime = Path(env["TEST_HOME"]) / ".local/share/omi-collector"
    previous = runtime / "releases" / ("release-" + "1" * 40 + "-1-1")
    (previous / "bin").mkdir(parents=True)
    python = previous / "bin/python"
    python.write_text("#!/usr/bin/env bash\nexit 0\n", encoding="utf-8")
    python.chmod(0o755)
    (runtime / "current").symlink_to(f"releases/{previous.name}")

    result = _run_fake_deployer(deployer, env)

    assert result.returncode != 0
    assert (runtime / "current").resolve(strict=True) == previous
    assert Path(tmp_path / "service.active").exists()
    assert "systemctl start --no-block omi-collector.service" in Path(env["TEST_LOG"]).read_text(encoding="utf-8")


def test_deployer_leaves_unverified_selected_release_stopped(tmp_path: Path) -> None:
    deployer, env = _fake_deployer(tmp_path, readiness_sequence=("failed:0:0:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa:0",))

    result = _run_fake_deployer(deployer, env)

    runtime = Path(env["TEST_HOME"]) / ".local/share/omi-collector"
    assert result.returncode != 0
    assert (runtime / "current").is_symlink()
    assert not Path(tmp_path / "service.active").exists()
    assert "forward recovery" in result.stderr


def test_deployer_rejects_dirty_or_uninspectable_source_before_stop(tmp_path: Path) -> None:
    for variable, value in (("FAKE_GIT_DIRTY", "1"), ("FAKE_GIT_ERROR", "1")):
        case = tmp_path / variable
        deployer, env = _fake_deployer(case)
        env[variable] = value
        result = _run_fake_deployer(deployer, env)
        commands = Path(env["TEST_LOG"]).read_text(encoding="utf-8") if Path(env["TEST_LOG"]).exists() else ""
        assert result.returncode != 0
        assert "systemctl stop" not in commands


def test_deployer_rejects_invalid_config_before_stop(tmp_path: Path) -> None:
    deployer, env = _fake_deployer(tmp_path)
    env["FAKE_CONFIG_INVALID"] = "1"

    result = _run_fake_deployer(deployer, env)

    assert result.returncode != 0
    assert "systemctl stop" not in Path(env["TEST_LOG"]).read_text(encoding="utf-8")


def test_deployer_recovers_hot_journal_only_under_stopped_guard(tmp_path: Path) -> None:
    deployer, env = _fake_deployer(tmp_path, hot_journal=True)

    result = _run_fake_deployer(deployer, env)

    assert result.returncode == 0, result.stderr
    commands = Path(env["TEST_LOG"]).read_text(encoding="utf-8")
    assert commands.index("systemctl stop") < commands.index("collector recover-state-journal")
    assert commands.count("collector check-state") == 3
    assert commands.index("collector recover-state-journal") < commands.index("collector init-state")


def test_deployer_recovery_failure_does_not_publish_selector(tmp_path: Path) -> None:
    deployer, env = _fake_deployer(tmp_path, hot_journal=True, recovery_fail=True)

    result = _run_fake_deployer(deployer, env)

    runtime = Path(env["TEST_HOME"]) / ".local/share/omi-collector"
    assert result.returncode != 0
    assert not (runtime / "current").exists()


def test_deployer_init_failure_leaves_selected_release_stopped(tmp_path: Path) -> None:
    deployer, env = _fake_deployer(tmp_path)
    env["FAKE_INIT_FAIL"] = "1"

    result = _run_fake_deployer(deployer, env)

    runtime = Path(env["TEST_HOME"]) / ".local/share/omi-collector"
    assert result.returncode != 0
    assert (runtime / "current").is_symlink()
    assert not (tmp_path / "service.active").exists()


@pytest.mark.parametrize(
    "sequence",
    (
        (
            "activating:4242:0:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa:0",
            "active:4242:0:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa:0",
        ),
        (
            "inactive:0:3:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb:77 /start",
            "activating:4242:0:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa:0",
            "active:4242:0:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa:0",
        ),
    ),
)
def test_deployer_accepts_same_invocation_after_queued_job(tmp_path: Path, sequence: tuple[str, ...]) -> None:
    deployer, env = _fake_deployer(tmp_path, readiness_sequence=sequence)

    result = _run_fake_deployer(deployer, env)

    assert result.returncode == 0, result.stderr
    assert "Deployed and verified" in result.stdout


@pytest.mark.parametrize(
    "sequence",
    (
        ("activating:4242:0:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa:0", "active:4242:1:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa:0"),
        ("activating:4242:0:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa:0", "active:4243:0:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa:0"),
        ("activating:4242:0:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa:0", "active:4242:0:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb:0"),
        ("inactive:0:0:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa:0",),
    ),
)
def test_deployer_fails_closed_on_changed_or_missing_invocation(tmp_path: Path, sequence: tuple[str, ...]) -> None:
    deployer, env = _fake_deployer(tmp_path, readiness_sequence=sequence)

    result = _run_fake_deployer(deployer, env)

    runtime = Path(env["TEST_HOME"]) / ".local/share/omi-collector"
    assert result.returncode != 0
    assert (runtime / "current").is_symlink()
    assert not (tmp_path / "service.active").exists()


def test_deployer_preserves_confirmed_invocation_when_readiness_is_pending(tmp_path: Path) -> None:
    deployer, env = _fake_deployer(
        tmp_path, readiness_sequence=("activating:4242:0:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa:0",)
    )
    content = deployer.read_text(encoding="utf-8").replace(
        "readiness_timeout_seconds=900", "readiness_timeout_seconds=0"
    )
    deployer.write_text(content, encoding="utf-8")

    result = _run_fake_deployer(deployer, env)

    runtime = Path(env["TEST_HOME"]) / ".local/share/omi-collector"
    assert result.returncode != 0
    assert "unverified and in progress" in result.stderr
    assert (runtime / "current").is_symlink()
    assert (tmp_path / "service.active").exists()


def test_deployer_refuses_a_concurrent_transaction(tmp_path: Path) -> None:
    deployer, env = _fake_deployer(tmp_path)
    runtime = Path(env["TEST_HOME"]) / ".local/share/omi-collector"
    runtime.mkdir(parents=True)
    lock = (runtime / ".deployment.lock").open("w")
    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)

    result = _run_fake_deployer(deployer, env)

    assert result.returncode != 0
    assert "another deployment is already in progress" in result.stderr
    lock.close()
