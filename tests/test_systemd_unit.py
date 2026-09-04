import fcntl
import json
import os
import shlex
import shutil
import subprocess
import venv
from dataclasses import dataclass
from pathlib import Path

import pytest
from click import unstyle
from typer.testing import CliRunner

from omi_collector.cli import app

_ROOT = Path(__file__).parents[1]
_UNIT = _ROOT / "systemd" / "omi-collector.service"
_EXEC = _ROOT / "systemd" / "omi-collector-exec"
_INSTALLER = _ROOT / "scripts" / "install-systemd-unit.sh"
_DEPLOYER = _ROOT / "scripts" / "deploy-systemd-service.sh"
_LEGACY_INSPECTOR = _ROOT / "scripts" / "inspect-legacy-device-state.py"
_FEATURE = _ROOT / "features" / "opportunistic_collection.feature"
_SOURCE_PACKAGE = _ROOT / "src" / "omi_collector"


@dataclass(frozen=True, slots=True)
class _DeploymentScenario:
    readiness_layout: str | None = None
    crash_loop: bool = False
    git_status: str = ""
    signal_during_readiness: bool = False
    prune_failure: bool = False
    replacement_race: bool = False
    rollback_failure: bool = False
    candidate_writes_current_state: bool = False
    postcommit_signal: bool = False
    postcommit_failure: bool = False
    build_failure: bool = False
    staging_failure: bool = False
    staging_mktemp_failure: bool = False
    release_move_signal: bool = False
    release_move_failure: bool = False
    rollback_stop_failure: bool = False
    readiness_signal: str = "TERM"


_DEFAULT_DEPLOYMENT_SCENARIO = _DeploymentScenario()


def _unit_sections() -> dict[str, dict[str, str]]:
    sections: dict[str, dict[str, str]] = {}
    section: dict[str, str] | None = None
    for raw_line in _UNIT.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("[") and line.endswith("]"):
            section = {}
            sections[line[1:-1]] = section
            continue
        assert section is not None, f"directive outside a section: {line}"
        key, separator, value = line.partition("=")
        assert separator, f"invalid unit directive: {line}"
        section[key] = f"{section[key]}\n{value}" if key in section else value
    return sections


def test_production_unit_uses_the_dedicated_account_and_static_pair() -> None:
    service = _unit_sections()["Service"]

    assert service["User"] == "omi-collector"
    assert service["Group"] == "omi-collector"
    assert service["EnvironmentFile"].splitlines() == [
        "/etc/omi-collector/omi-collector.env",
        "-/var/lib/omi-collector-deployments/deployment.env",
    ]
    assert service["ExecStart"] == "/usr/local/libexec/omi-collector/omi-collector-exec"
    assert service["ReadWritePaths"] == "/var/lib/omi-collector"
    assert service["Environment"] == (
        "PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1 XDG_STATE_HOME=/var/lib/omi-collector"
    )
    assert service["StateDirectory"] == "omi-collector"
    assert service["AmbientCapabilities"] == "CAP_NET_RAW"
    assert "WorkingDirectory" not in service


def test_installer_and_deployer_keep_production_targets_non_overridable() -> None:
    installer = _INSTALLER.read_text(encoding="utf-8")
    deployer = _DEPLOYER.read_text(encoding="utf-8")

    for script in (installer, deployer):
        assert "OMI_COLLECTOR_ENV_FILE" not in script
        assert "OMI_COLLECTOR_SYSTEMD_UNIT_TARGET" not in script
        assert "OMI_COLLECTOR_SYSTEMD_EXEC_TARGET" not in script
    assert "environment_file='/etc/omi-collector/omi-collector.env'" in installer
    assert "unit_target='/etc/systemd/system/omi-collector.service'" in installer
    assert "exec_target='/usr/local/libexec/omi-collector/omi-collector-exec'" in installer
    assert "ensure_service_account" in installer
    assert "groupadd --system" in installer
    assert "useradd --system" in installer
    assert 'install -d -o "$account_user" -g "$account_group" -m 0750' in installer
    assert "validate_layout_file" in installer
    assert 'chown root:"$account_group" -- "$layout_file"' in installer
    assert 'chmod 0640 -- "$layout_file"' in installer
    assert 'stage_file "$source_exec" "$exec_target" 0755' in installer
    assert 'stage_file "$source_unit" "$unit_target" 0644' in installer
    assert "validate_staged_pair" in installer
    assert 'validation_unit="${validation_root}/${service_name}"' in installer
    assert "ExecStart=${staged_exec} --check" in installer
    assert 'systemd-analyze verify "$validation_unit"' in installer
    assert "--root=" not in installer
    assert "--offline" not in deployer
    assert "rollback_pair" in installer
    assert '[[ "$layout_file" == /* && -f "$layout_file" && ! -L "$layout_file" ]]' in installer
    assert "must name a regular absolute file" in installer
    assert "environment_file='/etc/omi-collector/omi-collector.env'" in deployer
    assert "deployment_environment_file='/var/lib/omi-collector-deployments/deployment.env'" in deployer
    assert "deployment_root='/var/lib/omi-collector-deployments'" in deployer
    assert "deployments_dir='/var/lib/omi-collector-deployments/releases'" in deployer
    assert "staging_dir='/var/lib/omi-collector-deployments/staging'" in deployer
    assert "installed_unit='/etc/systemd/system/omi-collector.service'" in deployer
    assert "installed_exec='/usr/local/libexec/omi-collector/omi-collector-exec'" in deployer
    assert "(( EUID == 0 ))" in deployer
    assert '"$runuser_bin" --user "$account_user" -- env' in deployer
    assert "sudo --non-interactive" not in deployer
    assert '[[ "${layout_path:-}" == /* && -f "$layout_path" && ! -L "$layout_path" ]]' in deployer
    assert "must name a regular absolute file" in deployer
    assert 'chown -R root:root -- "$environment"' in deployer
    assert 'install -d -o root -g root -m 0755 -- "$deployment_root" "$deployments_dir" "$staging_dir"' in deployer
    assert "deployment_committed=1" in deployer
    assert "legacy_restore_required=0" in deployer
    assert 'chown root:root -- "$legacy_state_backup"' in deployer
    assert 'chmod 0600 -- "$legacy_state_backup"' in deployer
    assert "release_moved=0" in deployer
    assert 'staged_deployment_file=$(mktemp --tmpdir="$deployment_root"' in deployer
    assert 'release_moved=1\nmv -- "$staged_environment" "$release_path"' in deployer
    assert 'flock -n "$deployment_lock_fd"' in deployer


def test_wrapper_and_example_require_explicit_operator_values() -> None:
    example = (_ROOT / "config" / "omi-collector.env.example").read_text(encoding="utf-8")
    wrapper = _EXEC.read_text(encoding="utf-8")

    assert _EXEC.stat().st_mode & 0o111
    assert _INSTALLER.stat().st_mode & 0o111
    assert _DEPLOYER.stat().st_mode & 0o111
    assert "eval" not in wrapper
    assert "OMI_COLLECTOR_DEVICE_ADDRESS=AA:BB:CC:DD:EE:FF" in example
    assert "OMI_COLLECTOR_LAYOUT_PATH=/var/lib/omi-collector/collector.toml" in example
    assert "OMI_COLLECTOR_UV_PROJECT_ENVIRONMENT=/var/lib/omi-collector-deployments/uninitialized" in example


def test_legacy_state_inspector_only_allows_exact_disposable_document(tmp_path: Path) -> None:
    layout = tmp_path / "collector.toml"
    layout.write_text(
        'version = 2\n\n[collector]\nroot = "collector"\nattempts = "attempts"\n'
        'quarantine = "quarantine"\nlock = "collector.lock"\ndevice_state = "device.json"\n'
        'debug_log = "debug.jsonl"\n\n[publication]\nroot = "source"\n',
        encoding="utf-8",
    )
    collector = tmp_path / "collector"
    collector.mkdir()
    state = collector / "device.json"

    state.write_text('{"device_slug":"omi","observations":[],"schema_version":1}', encoding="utf-8")
    allowed = subprocess.run(
        ["python3", str(_LEGACY_INSPECTOR), str(layout), "omi"], check=False, capture_output=True, text=True
    )
    assert allowed.returncode == 0
    fields = allowed.stdout.strip().split("\t")
    assert fields[:2] == ["LEGACY", str(state)]
    assert fields[2:] == [str(state.stat().st_dev), str(state.stat().st_ino)]

    current = {
        "schema_version": 2,
        "device_slug": "omi",
        "latest": {
            "read_sequence": 1,
            "write_sequence": 2,
            "capacity_packets": 64,
            "dropped_packets": 3,
            "packet_size": 20,
        },
        "metrics": {
            "observation_count": 1,
            "initial": 3,
            "observed_increase": 0,
            "regression_count": 0,
        },
    }
    state.write_text(json.dumps(current, sort_keys=True, separators=(",", ":")), encoding="utf-8")
    recognized = subprocess.run(
        ["python3", str(_LEGACY_INSPECTOR), str(layout), "omi"], check=False, capture_output=True, text=True
    )
    assert recognized.returncode == 0
    assert recognized.stdout.strip() == "CURRENT"
    assert state.is_file()

    for payload in ("{", '{"device_slug":"omi","observations":[],"schema_version":99}'):
        state.write_text(payload, encoding="utf-8")
        rejected = subprocess.run(
            ["python3", str(_LEGACY_INSPECTOR), str(layout), "omi"], check=False, capture_output=True, text=True
        )
        assert rejected.returncode != 0
    state.unlink()
    state.symlink_to(tmp_path / "missing")
    rejected = subprocess.run(
        ["python3", str(_LEGACY_INSPECTOR), str(layout), "omi"], check=False, capture_output=True, text=True
    )
    assert rejected.returncode != 0


def test_wrapper_check_allows_an_external_layout_but_exec_requires_environment(tmp_path: Path) -> None:
    state_root = tmp_path / "state"
    state_root.mkdir()
    external_root = tmp_path / "external"
    external_root.mkdir()
    layout = external_root / "collector.toml"
    layout.write_text("[collector]\nroot = 'collector'\n", encoding="utf-8")
    project = tmp_path / "project"
    project.mkdir()
    uv = tmp_path / "uv"
    uv.write_text("#!/usr/bin/env bash\nexit 0\n", encoding="utf-8")
    uv.chmod(0o755)
    environment = {
        **os.environ,
        "OMI_COLLECTOR_DEVICE_ADDRESS": "12:34:56:78:9A:BC",
        "OMI_COLLECTOR_DEVICE_SLUG": "test-pendant",
        "OMI_COLLECTOR_LAYOUT_PATH": str(layout),
        "OMI_COLLECTOR_PROJECT_DIR": str(project),
        "OMI_COLLECTOR_UV_BIN": str(uv),
        "OMI_COLLECTOR_UV_PROJECT_ENVIRONMENT": str(state_root / "venv"),
    }

    check = subprocess.run([str(_EXEC), "--check"], check=False, capture_output=True, text=True, env=environment)
    execute = subprocess.run([str(_EXEC)], check=False, capture_output=True, text=True, env=environment)

    assert check.returncode == 0, check.stderr
    assert execute.returncode != 0
    assert "must name a regular absolute directory" in execute.stderr

    prepared_environment = state_root / "venv"
    prepared_environment.mkdir()
    uv_environment = tmp_path / "uv-environment"
    uv.write_text(
        f'#!/usr/bin/env bash\nprintf "%s\\n" "$UV_PROJECT_ENVIRONMENT" > {shlex.quote(str(uv_environment))}\n',
        encoding="utf-8",
    )
    prepared_execute = subprocess.run([str(_EXEC)], check=False, capture_output=True, text=True, env=environment)

    assert prepared_execute.returncode == 0, prepared_execute.stderr
    assert uv_environment.read_text(encoding="utf-8") == f"{prepared_environment}\n"


def test_feature_rules_do_not_contain_steps_outside_a_scenario() -> None:
    step_context = "feature"
    for raw_line in _FEATURE.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if line.startswith("Background:"):
            step_context = "background"
        elif line.startswith("Rule:"):
            step_context = "rule"
        elif line.startswith("Scenario:"):
            step_context = "scenario"
        elif line.startswith(("Given ", "When ", "Then ", "And ", "But ")):
            assert step_context in {"background", "scenario"}, f"step outside Scenario: {line}"


def test_cli_requires_an_explicit_layout_for_storage_commands() -> None:
    result = CliRunner().invoke(app, ["device", "metrics", "--device-slug", "omi"])

    assert result.exit_code == 2
    assert "Missing option '--layout'" in unstyle(result.output)


def _write_commands(
    fake_bin: Path, log: Path, scenario: _DeploymentScenario, layout_path: Path, source_package: Path
) -> None:
    quoted_log = shlex.quote(str(log))
    (fake_bin / "uv").write_text(
        "#!/usr/bin/env bash\n"
        f'printf "uv args=%s\\n" "$*" >> {quoted_log}\n'
        f'printf "uv env=%s|%s|%s\\n" "$UV_PROJECT_ENVIRONMENT" "$UV_LINK_MODE" "$UV_CACHE_DIR" >> {quoted_log}\n'
        '[[ "${DEPLOY_BUILD_FAIL:-}" != 1 ]] || exit 1\n'
        f'python3 -m venv --clear "$UV_PROJECT_ENVIRONMENT"\n'
        f'purelib=$("$UV_PROJECT_ENVIRONMENT/bin/python" -I -B -c \'import sysconfig; print(sysconfig.get_path("purelib"))\')\n'
        f'cp -a {shlex.quote(str(source_package))} "$purelib/omi_collector"\n',
        encoding="utf-8",
    )
    (fake_bin / "uv").chmod(0o755)
    (fake_bin / "git").write_text(
        "#!/usr/bin/env bash\n"
        '[[ "${1:-}" == -C ]] || exit 2\n'
        'case "${3:-}" in\n'
        "rev-parse) printf '%040d\\n' 0 ;;\n"
        f"status) printf '%s' {shlex.quote(scenario.git_status)} ;;\n"
        "*) exit 2 ;;\n"
        "esac\n",
        encoding="utf-8",
    )
    (fake_bin / "git").chmod(0o755)
    initial_invocation = "a" * 32
    restarted_invocation = "b" * 32
    readiness_layout = scenario.readiness_layout or str(layout_path)
    crash_clause = (
        f"if (( count > 1 )); then pid=5252; restarts=1; invocation={restarted_invocation}; fi\n"
        if scenario.crash_loop
        else ""
    )
    (fake_bin / "runuser").write_text(
        "#!/usr/bin/env bash\n"
        f'printf "runuser args=%s\\n" "$*" >> {quoted_log}\n'
        '[[ "${1:-}" == --user && "${2:-}" == root && "${3:-}" == -- ]] || exit 2\n'
        "shift 3\n"
        'exec "$@"\n',
        encoding="utf-8",
    )
    (fake_bin / "runuser").chmod(0o755)
    (fake_bin / "chown").write_text(
        "#!/usr/bin/env bash\n"
        f'printf "chown args=%s\\n" "$*" >> {quoted_log}\n'
        '[[ "${DEPLOY_STAGING_FAIL:-}" == 1 && "$*" == *".deployment.env.tmp."* ]] && exit 1\n'
        'exec /usr/bin/chown "$@"\n',
        encoding="utf-8",
    )
    (fake_bin / "chown").chmod(0o755)
    if scenario.staging_mktemp_failure:
        (fake_bin / "mktemp").write_text(
            "#!/usr/bin/env bash\n"
            'result=$(/usr/bin/mktemp "$@") || exit 2\n'
            'printf "%s\\n" "$result"\n'
            '[[ "${1:-}" == --tmpdir=* && "${2:-}" == .deployment.env.tmp.* ]] || exit 0\n'
            "exit 1\n",
            encoding="utf-8",
        )
        (fake_bin / "mktemp").chmod(0o755)
    (fake_bin / "systemctl").write_text(
        "#!/usr/bin/env bash\n"
        f'printf "systemctl args=%s\\n" "$*" >> {quoted_log}\n'
        'case "${1:-}" in\n'
        "daemon-reload) exit 0 ;;\n"
        "stop)\n"
        f"stop_count_file={shlex.quote(str(fake_bin / 'stop-count'))}\n"
        'stop_count=0; [[ -f "$stop_count_file" ]] && read -r stop_count < "$stop_count_file"\n'
        '(( stop_count++ )); printf "%s\\n" "$stop_count" > "$stop_count_file"\n'
        'if [[ "${DEPLOY_ROLLBACK_STOP_FAIL:-}" == 1 && "$stop_count" -gt 1 ]]; then exit 1; fi\n'
        "exit 0 ;;\n"
        "start) exit 0 ;;\n"
        "restart)\n"
        f"restart_count_file={shlex.quote(str(fake_bin / 'restart-count'))}\n"
        'restart_count=0; [[ -f "$restart_count_file" ]] && read -r restart_count < "$restart_count_file"\n'
        '(( restart_count++ )); printf "%s\\n" "$restart_count" > "$restart_count_file"\n'
        'if [[ "${DEPLOY_ROLLBACK_FAIL:-}" == 1 && "$restart_count" -gt 1 ]]; then exit 1; fi\n'
        "exit 0 ;;\n"
        "show)\n"
        f"count_file={shlex.quote(str(fake_bin / 'show-count'))}\n"
        'count=0; [[ -f "$count_file" ]] && read -r count < "$count_file"\n'
        '(( count++ )); printf "%s\\n" "$count" > "$count_file"\n'
        f"pid=4242; restarts=0; invocation={initial_invocation}\n"
        f"{crash_clause}"
        'printf "ActiveState=active\\nMainPID=%s\\nNRestarts=%s\\nInvocationID=%s\\n" "$pid" "$restarts" "$invocation"\n'
        ";;\n"
        "*) exit 2 ;;\n"
        "esac\n",
        encoding="utf-8",
    )
    (fake_bin / "systemctl").chmod(0o755)
    if scenario.release_move_signal or scenario.release_move_failure:
        (fake_bin / "mv").write_text(
            "#!/usr/bin/env bash\n"
            'if [[ "${3:-}" == */release-* && "${DEPLOY_RELEASE_MOVE_FAIL:-}" == 1 ]]; then exit 1; fi\n'
            'if [[ "${3:-}" == */release-* && "${DEPLOY_RELEASE_MOVE_SIGNAL:-}" == 1 ]]; then\n'
            '    /usr/bin/mv "$@" || exit $?\n'
            '    kill -TERM "$PPID"\n'
            "    exit 0\n"
            "fi\n"
            'exec /usr/bin/mv "$@"\n',
            encoding="utf-8",
        )
        (fake_bin / "mv").chmod(0o755)
    (fake_bin / "journalctl").write_text(
        "#!/usr/bin/env bash\n"
        f'printf "journalctl args=%s\\n" "$*" >> {quoted_log}\n'
        '[[ "${1:-}" == --unit ]] || exit 2\n'
        + (
            f'if [[ "${{DEPLOY_CANDIDATE_WRITES_STATE:-}}" == 1 && ! -e {shlex.quote(str(layout_path.parent / "collector" / "device.json") + ".candidate-written")} ]]; then '
            f"mkdir -p {shlex.quote(str(layout_path.parent / 'collector'))}\n"
            f'printf \'%s\' \'{{"device_slug":"test-pendant","latest":{{"capacity_packets":64,"dropped_packets":3,"packet_size":20,"read_sequence":1,"write_sequence":2}},"metrics":{{"initial":3,"observation_count":1,"observed_increase":0,"regression_count":0}},"schema_version":2}}\' > {shlex.quote(str(layout_path.parent / "collector" / "device.json"))}\n'
            f"touch {shlex.quote(str(layout_path.parent / 'collector' / 'device.json') + '.candidate-written')}\n"
            "fi\n"
            if scenario.candidate_writes_current_state
            else ""
        )
        + f'printf \'%s\\n\' \'{{"layout":"{readiness_layout}","status":"deployment_ready"}}\'\n',
        encoding="utf-8",
    )
    (fake_bin / "journalctl").chmod(0o755)
    (fake_bin / "sleep").write_text(
        "#!/usr/bin/env bash\n"
        f'printf "sleep args=%s\\n" "$*" >> {quoted_log}\n'
        'if [[ -n "${DEPLOY_SIGNAL:-}" && "${1:-}" == 1 ]]; then kill -"$DEPLOY_SIGNAL" "$PPID"; fi\n',
        encoding="utf-8",
    )
    (fake_bin / "sleep").chmod(0o755)
    if scenario.prune_failure or scenario.postcommit_signal or scenario.postcommit_failure:
        (fake_bin / "rm").write_text(
            "#!/usr/bin/env bash\n"
            'for argument in "$@"; do\n'
            '    [[ "$argument" == *release-2222222222222222222222222222222222222222-* && "${DEPLOY_PRUNE_FAIL:-}" == 1 ]] && exit 1\n'
            '    [[ "$argument" == *.deployment.env.backup.* && "${DEPLOY_POSTCOMMIT_SIGNAL:-}" == 1 ]] && kill -TERM "$PPID"\n'
            '    [[ "$argument" == *.device-state.backup.* && "${DEPLOY_POSTCOMMIT_FAIL:-}" == 1 ]] && exit 1\n'
            "done\n"
            'exec /usr/bin/rm "$@"\n',
            encoding="utf-8",
        )
        (fake_bin / "rm").chmod(0o755)
    if scenario.replacement_race:
        state_path = layout_path.parent / "collector" / "device.json"
        (fake_bin / "stat").write_text(
            "#!/usr/bin/env bash\n"
            f'if [[ "${{1:-}}" == -c && "${{4:-}}" == {shlex.quote(str(state_path))} && ! -e {shlex.quote(str(state_path) + ".raced")} ]]; then '
            f"mv -- {shlex.quote(str(state_path))} {shlex.quote(str(state_path) + '.raced')}\n"
            f"printf '{{}}' > {shlex.quote(str(state_path))}\n"
            "fi\n"
            'exec /usr/bin/stat "$@"\n',
            encoding="utf-8",
        )
        (fake_bin / "stat").chmod(0o755)


def _deployment_harness(
    tmp_path: Path, scenario: _DeploymentScenario = _DEFAULT_DEPLOYMENT_SCENARIO
) -> tuple[Path, Path, Path, dict[str, str], Path]:
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    state_root = tmp_path / "state"
    state_root.mkdir()
    (tmp_path / "deployments").mkdir()
    external_root = tmp_path / "external"
    external_root.mkdir()
    layout_path = external_root / "collector.toml"
    layout_path.write_text(
        'version = 2\n\n[collector]\nroot = "collector"\nattempts = "attempts"\nquarantine = "quarantine"\n'
        'lock = "collector.lock"\ndevice_state = "device.json"\ndebug_log = "debug.jsonl"\n'
        '\n[publication]\nroot = "source"\n',
        encoding="utf-8",
    )
    environment = state_root / "venv"
    venv.EnvBuilder(with_pip=False).create(environment)
    purelib = Path(
        subprocess.check_output(
            [
                environment / "bin" / "python",
                "-I",
                "-B",
                "-c",
                'import sysconfig; print(sysconfig.get_path("purelib"))',
            ],
            text=True,
        ).strip()
    )
    staged_repo = tmp_path / "repo"
    shutil.copytree(_SOURCE_PACKAGE, staged_repo / "src" / "omi_collector")
    shutil.copytree(_ROOT / "systemd", staged_repo / "systemd")
    (staged_repo / "scripts").mkdir()
    shutil.copy2(_ROOT / "scripts" / "inspect-legacy-device-state.py", staged_repo / "scripts")
    installed_unit = tmp_path / "installed" / "omi-collector.service"
    installed_exec = tmp_path / "installed" / "omi-collector-exec"
    installed_unit.parent.mkdir()
    shutil.copy2(staged_repo / "systemd" / "omi-collector.service", installed_unit)
    shutil.copy2(staged_repo / "systemd" / "omi-collector-exec", installed_exec)
    environment_file = tmp_path / "omi-collector.env"
    environment_file.write_text(
        "\n".join(
            (
                "OMI_COLLECTOR_DEVICE_ADDRESS=12:34:56:78:9A:BC",
                "OMI_COLLECTOR_DEVICE_SLUG=test-pendant",
                f"OMI_COLLECTOR_LAYOUT_PATH={layout_path}",
                f"OMI_COLLECTOR_PROJECT_DIR={staged_repo}",
                f"OMI_COLLECTOR_UV_BIN={fake_bin / 'uv'}",
                f"OMI_COLLECTOR_UV_PROJECT_ENVIRONMENT={environment}",
                "",
            )
        ),
        encoding="utf-8",
    )
    deployer = _DEPLOYER.read_text(encoding="utf-8")
    replacements = {
        "/etc/omi-collector/omi-collector.env": str(environment_file),
        "/etc/systemd/system/omi-collector.service": str(installed_unit),
        "/usr/local/libexec/omi-collector/omi-collector-exec": str(installed_exec),
        "/var/lib/omi-collector-deployments": str(tmp_path / "deployments"),
        "/var/lib/omi-collector": str(state_root),
        "account_user='omi-collector'": "account_user='root'",
        "account_group='omi-collector'": "account_group='root'",
    }
    for production_path, harness_path in replacements.items():
        assert production_path in deployer
        deployer = deployer.replace(production_path, harness_path)
    harness_deployer = staged_repo / "scripts" / "deploy-systemd-service.sh"
    harness_deployer.write_text(deployer, encoding="utf-8")
    harness_deployer.chmod(0o755)
    shutil.copytree(staged_repo / "src" / "omi_collector", purelib / "omi_collector")
    log = tmp_path / "commands.log"
    _write_commands(fake_bin, log, scenario, layout_path, staged_repo / "src" / "omi_collector")
    return (
        harness_deployer,
        environment_file,
        layout_path,
        {
            **os.environ,
            "PATH": f"{fake_bin}{os.pathsep}{os.environ['PATH']}",
            **({"DEPLOY_SIGNAL": "TERM"} if scenario.signal_during_readiness else {}),
            **({"DEPLOY_PRUNE_FAIL": "1"} if scenario.prune_failure else {}),
            **({"DEPLOY_REPLACE_STATE": "1"} if scenario.replacement_race else {}),
            **({"DEPLOY_ROLLBACK_FAIL": "1"} if scenario.rollback_failure else {}),
            **({"DEPLOY_CANDIDATE_WRITES_STATE": "1"} if scenario.candidate_writes_current_state else {}),
            **({"DEPLOY_POSTCOMMIT_SIGNAL": "1"} if scenario.postcommit_signal else {}),
            **({"DEPLOY_POSTCOMMIT_FAIL": "1"} if scenario.postcommit_failure else {}),
            **({"DEPLOY_BUILD_FAIL": "1"} if scenario.build_failure else {}),
            **({"DEPLOY_STAGING_FAIL": "1"} if scenario.staging_failure else {}),
            **({"DEPLOY_STAGING_MKTEMP_FAIL": "1"} if scenario.staging_mktemp_failure else {}),
            **({"DEPLOY_RELEASE_MOVE_SIGNAL": "1"} if scenario.release_move_signal else {}),
            **({"DEPLOY_RELEASE_MOVE_FAIL": "1"} if scenario.release_move_failure else {}),
            **({"DEPLOY_ROLLBACK_STOP_FAIL": "1"} if scenario.rollback_stop_failure else {}),
            **({"DEPLOY_SIGNAL": scenario.readiness_signal} if scenario.signal_during_readiness else {}),
        },
        log,
    )


def _root_prefix() -> list[str]:
    if os.geteuid() == 0:
        return []
    unshare = shutil.which("unshare")
    if unshare is None:
        pytest.skip("root deploy harness requires unshare")
    probe = subprocess.run([unshare, "-Ur", "true"], check=False)
    if probe.returncode != 0:
        pytest.skip("root deploy harness requires usable user namespaces")
    return [unshare, "-Ur"]


def test_deployer_requires_root_before_side_effects(tmp_path: Path) -> None:
    if os.geteuid() == 0:
        pytest.skip("test runner is already root")
    deployer, _, _, environment, log = _deployment_harness(tmp_path)

    result = subprocess.run([str(deployer)], check=False, capture_output=True, text=True, env=environment)

    assert result.returncode != 0
    assert "must run as root" in result.stderr
    assert not log.exists()


def test_deployer_harness_requires_readiness_and_stability(tmp_path: Path) -> None:
    deployer, _, layout_path, environment, log = _deployment_harness(tmp_path)

    result = subprocess.run(
        [*_root_prefix(), str(deployer)], check=False, capture_output=True, text=True, env=environment
    )

    assert result.returncode == 0, result.stderr
    assert f"layout={layout_path}" in result.stdout
    commands = log.read_text(encoding="utf-8")
    assert "runuser args=--user root -- env" in commands
    assert "uv env=" in commands
    assert "|copy|" in commands
    assert f"chown args=-R root:root -- {tmp_path / 'state' / 'uv-cache'}" in commands
    assert (tmp_path / "deployments" / "releases").is_dir()
    assert "systemctl args=restart omi-collector.service" in commands
    assert "sleep args=6" in commands
    assert commands.index("systemctl args=stop omi-collector.service") < commands.index("uv env=")
    assert (tmp_path / "deployments" / "deployment.env").read_text(encoding="utf-8") == (
        "OMI_COLLECTOR_SOURCE_REVISION=0000000000000000000000000000000000000000\n"
        f"OMI_COLLECTOR_UV_PROJECT_ENVIRONMENT={next((tmp_path / 'deployments' / 'releases').glob('release-*'))}\n"
    )


def test_deployer_restores_service_after_build_failure(tmp_path: Path) -> None:
    deployer, _, _, environment, log = _deployment_harness(tmp_path, _DeploymentScenario(build_failure=True))

    result = subprocess.run(
        [*_root_prefix(), str(deployer)], check=False, capture_output=True, text=True, env=environment
    )

    assert result.returncode != 0
    commands = log.read_text(encoding="utf-8")
    assert commands.index("systemctl args=stop omi-collector.service") < commands.index("uv env=")
    assert "systemctl args=start omi-collector.service" in commands
    assert not tuple((tmp_path / "state" / "staging").glob(".staging.*"))
    assert not tuple((tmp_path / "deployments" / "releases").glob("release-*"))


def test_deployer_removes_release_moved_before_selector_publication(tmp_path: Path) -> None:
    deployer, _, _, environment, log = _deployment_harness(tmp_path, _DeploymentScenario(staging_failure=True))

    result = subprocess.run(
        [*_root_prefix(), str(deployer)], check=False, capture_output=True, text=True, env=environment
    )

    assert result.returncode != 0
    assert "could not set deployment provenance environment group" in result.stderr
    assert not tuple((tmp_path / "deployments" / "releases").glob("release-*"))
    assert not tuple((tmp_path / "state" / "staging").glob(".staging.*"))
    assert "systemctl args=start omi-collector.service" in log.read_text(encoding="utf-8")


def test_deployer_removes_release_when_signal_interrupts_move_bookkeeping(tmp_path: Path) -> None:
    deployer, _, _, environment, log = _deployment_harness(tmp_path, _DeploymentScenario(release_move_signal=True))

    result = subprocess.run(
        [*_root_prefix(), str(deployer)], check=False, capture_output=True, text=True, env=environment
    )

    assert result.returncode == 143
    assert "interrupted by SIGTERM" in result.stderr
    assert not tuple((tmp_path / "state" / "staging").glob(".staging.*"))
    assert not tuple((tmp_path / "deployments" / "releases").glob("release-*"))
    assert not tuple((tmp_path / "deployments").glob(".deployment.env.tmp.*"))
    assert "systemctl args=start omi-collector.service" in log.read_text(encoding="utf-8")


def test_deployer_removes_staging_when_release_move_fails(tmp_path: Path) -> None:
    deployer, _, _, environment, log = _deployment_harness(tmp_path, _DeploymentScenario(release_move_failure=True))

    result = subprocess.run(
        [*_root_prefix(), str(deployer)], check=False, capture_output=True, text=True, env=environment
    )

    assert result.returncode != 0
    assert "could not publish staged deployment environment" in result.stderr
    assert not tuple((tmp_path / "state" / "staging").glob(".staging.*"))
    assert not tuple((tmp_path / "deployments" / "releases").glob("release-*"))
    assert "systemctl args=start omi-collector.service" in log.read_text(encoding="utf-8")


def test_deployer_removes_provenance_temp_when_mktemp_fails_after_creation(tmp_path: Path) -> None:
    deployer, _, _, environment, log = _deployment_harness(tmp_path, _DeploymentScenario(staging_mktemp_failure=True))

    result = subprocess.run(
        [*_root_prefix(), str(deployer)], check=False, capture_output=True, text=True, env=environment
    )

    assert result.returncode != 0
    assert "could not stage deployment provenance environment" in result.stderr
    assert not tuple((tmp_path / "state" / "staging").glob(".staging.*"))
    assert not tuple((tmp_path / "deployments" / "releases").glob("release-*"))
    assert not tuple((tmp_path / "deployments").glob(".deployment.env.tmp.*"))
    assert "systemctl args=start omi-collector.service" in log.read_text(encoding="utf-8")


def test_deployer_starts_service_when_rollback_stop_fails_after_quiescing(tmp_path: Path) -> None:
    deployer, _, _, environment, log = _deployment_harness(
        tmp_path, _DeploymentScenario(readiness_layout="/wrong/collector.toml", rollback_stop_failure=True)
    )

    result = subprocess.run(
        [*_root_prefix(), str(deployer)], check=False, capture_output=True, text=True, env=environment
    )

    assert result.returncode != 0
    commands = log.read_text(encoding="utf-8")
    assert commands.count("systemctl args=stop omi-collector.service") == 2
    assert "systemctl args=start omi-collector.service" in commands
    assert "recovery artifacts were retained" in result.stderr
    assert tuple((tmp_path / "deployments" / "releases").glob("release-*"))


def test_deployer_rejects_a_concurrent_transaction(tmp_path: Path) -> None:
    deployer, _, _, environment, log = _deployment_harness(tmp_path)
    lock_path = tmp_path / "deployments" / ".deployment.lock"
    lock_path.touch()

    with lock_path.open("a+b") as lock_handle:
        fcntl.flock(lock_handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        result = subprocess.run(
            [*_root_prefix(), str(deployer)], check=False, capture_output=True, text=True, env=environment
        )

    assert result.returncode != 0
    assert "another deployment transaction is already in progress" in result.stderr
    assert "systemctl args=" not in log.read_text(encoding="utf-8")


def test_deployer_seals_published_release_against_service_writes(tmp_path: Path) -> None:
    deployer, _, _, environment, _ = _deployment_harness(tmp_path)

    result = subprocess.run(
        [*_root_prefix(), str(deployer)], check=False, capture_output=True, text=True, env=environment
    )

    assert result.returncode == 0, result.stderr
    deployment_root = tmp_path / "deployments"
    release = next((deployment_root / "releases").glob("release-*"))
    assert deployment_root.stat().st_mode & 0o22 == 0
    assert (deployment_root / "releases").stat().st_mode & 0o22 == 0
    assert (deployment_root / "staging").stat().st_mode & 0o22 == 0
    for path in release.rglob("*"):
        assert path.lstat().st_uid == os.getuid(), path
        if not path.is_symlink():
            assert path.lstat().st_mode & 0o22 == 0, path


def test_deployer_does_not_restore_schema_one_after_postcommit_signal(tmp_path: Path) -> None:
    deployer, _, layout_path, environment, _ = _deployment_harness(
        tmp_path, _DeploymentScenario(postcommit_signal=True)
    )
    state = layout_path.parent / "collector" / "device.json"
    state.parent.mkdir()
    state.write_bytes(b'{"device_slug":"test-pendant","observations":[],"schema_version":1}')
    previous_release = tmp_path / "deployments" / "releases" / ("release-" + "1" * 40 + "-1-1")
    previous_release.mkdir(parents=True)
    deployment_file = tmp_path / "deployments" / "deployment.env"
    deployment_file.write_text(
        "OMI_COLLECTOR_SOURCE_REVISION=" + "1" * 40 + f"\nOMI_COLLECTOR_UV_PROJECT_ENVIRONMENT={previous_release}\n",
        encoding="utf-8",
    )

    result = subprocess.run(
        [*_root_prefix(), str(deployer)], check=False, capture_output=True, text=True, env=environment
    )

    assert result.returncode == 143
    assert not state.exists()
    assert "SOURCE_REVISION=0000000000000000000000000000000000000000" in deployment_file.read_text(encoding="utf-8")


def test_deployer_does_not_restore_schema_one_after_postcommit_cleanup_failure(tmp_path: Path) -> None:
    deployer, _, layout_path, environment, _ = _deployment_harness(
        tmp_path, _DeploymentScenario(postcommit_failure=True)
    )
    state = layout_path.parent / "collector" / "device.json"
    state.parent.mkdir()
    state.write_bytes(b'{"device_slug":"test-pendant","observations":[],"schema_version":1}')
    previous_release = tmp_path / "deployments" / "releases" / ("release-" + "1" * 40 + "-1-1")
    previous_release.mkdir(parents=True)
    deployment_file = tmp_path / "deployments" / "deployment.env"
    deployment_file.write_text(
        "OMI_COLLECTOR_SOURCE_REVISION=" + "1" * 40 + f"\nOMI_COLLECTOR_UV_PROJECT_ENVIRONMENT={previous_release}\n",
        encoding="utf-8",
    )

    result = subprocess.run(
        [*_root_prefix(), str(deployer)], check=False, capture_output=True, text=True, env=environment
    )

    assert result.returncode == 0, result.stderr
    assert "could not remove committed legacy device state artifact" in result.stderr
    assert not state.exists()
    assert "SOURCE_REVISION=0000000000000000000000000000000000000000" in deployment_file.read_text(encoding="utf-8")


def test_deployer_harness_rejects_wrong_readiness(tmp_path: Path) -> None:
    deployer, _, layout_path, environment, log = _deployment_harness(
        tmp_path, _DeploymentScenario(readiness_layout="/wrong/collector.toml")
    )

    result = subprocess.run(
        [*_root_prefix(), str(deployer)], check=False, capture_output=True, text=True, env=environment
    )

    assert result.returncode != 0
    assert f"did not announce readiness for layout {layout_path}" in result.stderr
    assert log.read_text(encoding="utf-8").count("sleep args=1") == 4
    assert not (tmp_path / "deployments" / "deployment.env").exists()
    assert not tuple((tmp_path / "deployments" / "releases").glob("release-*"))


def test_deployer_restores_previous_release_and_provenance_on_failure(tmp_path: Path) -> None:
    deployer, _, _, environment, _ = _deployment_harness(
        tmp_path, _DeploymentScenario(readiness_layout="/wrong/collector.toml")
    )
    previous_release = tmp_path / "deployments" / "releases" / ("release-" + "1" * 40 + "-1-1")
    previous_release.parent.mkdir()
    previous_release.mkdir()
    deployment_file = tmp_path / "deployments" / "deployment.env"
    deployment_file.write_text(
        "OMI_COLLECTOR_SOURCE_REVISION=" + "1" * 40 + f"\nOMI_COLLECTOR_UV_PROJECT_ENVIRONMENT={previous_release}\n",
        encoding="utf-8",
    )

    result = subprocess.run(
        [*_root_prefix(), str(deployer)], check=False, capture_output=True, text=True, env=environment
    )

    assert result.returncode != 0
    assert "did not announce readiness" in result.stderr
    assert deployment_file.read_text(encoding="utf-8") == (
        "OMI_COLLECTOR_SOURCE_REVISION=" + "1" * 40 + f"\nOMI_COLLECTOR_UV_PROJECT_ENVIRONMENT={previous_release}\n"
    )
    assert previous_release.is_dir()
    assert len(tuple((tmp_path / "deployments" / "releases").glob("release-*"))) == 1


def test_deployer_prunes_only_known_obsolete_releases_after_success(tmp_path: Path) -> None:
    deployer, _, _, environment, _ = _deployment_harness(tmp_path)
    releases = tmp_path / "deployments" / "releases"
    obsolete = releases / ("release-" + "2" * 40 + "-1-1")
    obsolete.mkdir(parents=True)
    unrelated = releases / "keep-me"
    unrelated.mkdir()

    result = subprocess.run(
        [*_root_prefix(), str(deployer)], check=False, capture_output=True, text=True, env=environment
    )

    assert result.returncode == 0, result.stderr
    assert not obsolete.exists()
    assert unrelated.is_dir()
    assert len(tuple(releases.glob("release-*"))) == 1


def test_deployer_harness_rejects_restart_during_stability(tmp_path: Path) -> None:
    deployer, _, _, environment, _ = _deployment_harness(tmp_path, _DeploymentScenario(crash_loop=True))

    result = subprocess.run(
        [*_root_prefix(), str(deployer)], check=False, capture_output=True, text=True, env=environment
    )

    assert result.returncode != 0
    assert "restarted during the stability interval" in result.stderr


def test_deployer_preserves_current_schema_two_state_across_consecutive_deployments(tmp_path: Path) -> None:
    deployer, _, layout_path, environment, _ = _deployment_harness(tmp_path)
    command = [*_root_prefix(), str(deployer)]

    first = subprocess.run(command, check=False, capture_output=True, text=True, env=environment)
    assert first.returncode == 0, first.stderr
    state = layout_path.parent / "collector" / "device.json"
    state.parent.mkdir()
    current = {
        "schema_version": 2,
        "device_slug": "test-pendant",
        "latest": {
            "read_sequence": 1,
            "write_sequence": 2,
            "capacity_packets": 64,
            "dropped_packets": 3,
            "packet_size": 20,
        },
        "metrics": {
            "observation_count": 1,
            "initial": 3,
            "observed_increase": 0,
            "regression_count": 0,
        },
    }
    state.write_bytes(json.dumps(current, sort_keys=True, separators=(",", ":")).encode())
    preserved = state.read_bytes()

    second = subprocess.run(command, check=False, capture_output=True, text=True, env=environment)
    assert second.returncode == 0, second.stderr
    assert state.read_bytes() == preserved


def test_deployer_rolls_back_on_signal_during_readiness(tmp_path: Path) -> None:
    deployer, _, _, environment, log = _deployment_harness(
        tmp_path, _DeploymentScenario(readiness_layout="/wrong/collector.toml", signal_during_readiness=True)
    )

    result = subprocess.run(
        [*_root_prefix(), str(deployer)], check=False, capture_output=True, text=True, env=environment
    )

    assert result.returncode == 143
    assert "interrupted by SIGTERM" in result.stderr
    assert result.stderr.count("Restored the previous verified deployment.") == 1
    assert "systemctl args=stop omi-collector.service" in log.read_text(encoding="utf-8")


def test_deployer_finalizer_handles_signal_without_an_explicit_signal_trap_gap(tmp_path: Path) -> None:
    deployer, _, _, environment, _ = _deployment_harness(
        tmp_path,
        _DeploymentScenario(
            readiness_layout="/wrong/collector.toml", signal_during_readiness=True, readiness_signal="QUIT"
        ),
    )

    result = subprocess.run(
        [*_root_prefix(), str(deployer)], check=False, capture_output=True, text=True, env=environment
    )

    assert result.returncode == 131
    assert "interrupted by SIGQUIT" in result.stderr
    assert not (tmp_path / "deployments" / "deployment.env").exists()
    assert not tuple((tmp_path / "deployments" / "releases").glob("release-*"))


def test_deployer_retains_recovery_artifacts_when_rollback_fails(tmp_path: Path) -> None:
    deployer, _, _, environment, _ = _deployment_harness(
        tmp_path, _DeploymentScenario(readiness_layout="/wrong/collector.toml", rollback_failure=True)
    )
    previous_release = tmp_path / "deployments" / "releases" / ("release-" + "1" * 40 + "-1-1")
    previous_release.mkdir(parents=True)
    deployment_file = tmp_path / "deployments" / "deployment.env"
    deployment_file.write_text(
        "OMI_COLLECTOR_SOURCE_REVISION=" + "1" * 40 + f"\nOMI_COLLECTOR_UV_PROJECT_ENVIRONMENT={previous_release}\n",
        encoding="utf-8",
    )

    result = subprocess.run(
        [*_root_prefix(), str(deployer)], check=False, capture_output=True, text=True, env=environment
    )

    assert result.returncode != 0
    assert "recovery artifacts were retained" in result.stderr
    assert tuple((tmp_path / "deployments").glob(".deployment.env.backup.*"))
    assert tuple((tmp_path / "deployments" / "releases").glob("release-0000000000000000000000000000000000000000-*"))


def test_deployer_restores_retired_schema_one_after_candidate_state_and_readiness_failure(tmp_path: Path) -> None:
    deployer, _, layout_path, environment, _ = _deployment_harness(
        tmp_path,
        _DeploymentScenario(readiness_layout="/wrong/collector.toml", candidate_writes_current_state=True),
    )
    state = layout_path.parent / "collector" / "device.json"
    state.parent.mkdir()
    legacy_bytes = b'{"device_slug":"test-pendant","observations":[],"schema_version":1}'
    state.write_bytes(legacy_bytes)

    result = subprocess.run(
        [*_root_prefix(), str(deployer)], check=False, capture_output=True, text=True, env=environment
    )

    assert result.returncode != 0
    assert "did not announce readiness" in result.stderr
    assert state.read_bytes() == legacy_bytes
    assert not tuple((tmp_path / "deployments").glob(".device-state.backup.*"))


def test_deployer_starts_service_after_precommit_legacy_validation_failure(tmp_path: Path) -> None:
    deployer, _, layout_path, environment, log = _deployment_harness(tmp_path)
    state = layout_path.parent / "collector" / "device.json"
    state.parent.mkdir()
    state.write_text("{", encoding="utf-8")

    result = subprocess.run(
        [*_root_prefix(), str(deployer)], check=False, capture_output=True, text=True, env=environment
    )

    assert result.returncode != 0
    commands = log.read_text(encoding="utf-8")
    assert "systemctl args=stop omi-collector.service" in commands
    assert "systemctl args=start omi-collector.service" in commands
    assert state.read_text(encoding="utf-8") == "{"


def test_deployer_keeps_new_selection_when_postcommit_pruning_fails(tmp_path: Path) -> None:
    deployer, _, layout_path, environment, _ = _deployment_harness(tmp_path, _DeploymentScenario(prune_failure=True))
    releases = tmp_path / "deployments" / "releases"
    obsolete = releases / ("release-" + "2" * 40 + "-1-1")
    obsolete.mkdir(parents=True)
    state = layout_path.parent / "collector" / "device.json"
    state.parent.mkdir()
    state.write_bytes(b'{"device_slug":"test-pendant","observations":[],"schema_version":1}')

    result = subprocess.run(
        [*_root_prefix(), str(deployer)], check=False, capture_output=True, text=True, env=environment
    )

    assert result.returncode != 0
    assert "could not prune obsolete deployment" in result.stderr
    deployment_file = tmp_path / "deployments" / "deployment.env"
    assert "SOURCE_REVISION=0000000000000000000000000000000000000000" in deployment_file.read_text()
    assert not state.exists()
    assert obsolete.is_dir()
    assert len(tuple(releases.glob("release-" + "0" * 40 + "-*"))) == 1


def test_deployer_rejects_legacy_state_replacement_before_unlink(tmp_path: Path) -> None:
    deployer, _, layout_path, environment, _ = _deployment_harness(tmp_path, _DeploymentScenario(replacement_race=True))
    state = layout_path.parent / "collector" / "device.json"
    state.parent.mkdir()
    state.write_text('{"device_slug":"test-pendant","observations":[],"schema_version":1}', encoding="utf-8")

    result = subprocess.run(
        [*_root_prefix(), str(deployer)], check=False, capture_output=True, text=True, env=environment
    )

    assert result.returncode != 0
    assert "changed during validation" in result.stderr
    assert state.exists()
    assert (state.parent / "device.json.raced").exists()


@pytest.mark.parametrize("git_status", (" M src/omi_collector/cli.py\n", "?? capture.tmp\n"))
def test_deployer_harness_refuses_dirty_source_before_building(tmp_path: Path, git_status: str) -> None:
    deployer, _, _, environment, log = _deployment_harness(tmp_path, _DeploymentScenario(git_status=git_status))

    result = subprocess.run(
        [*_root_prefix(), str(deployer)], check=False, capture_output=True, text=True, env=environment
    )

    assert result.returncode != 0
    assert "refusing deployment from a dirty source tree" in result.stderr
    assert not log.exists()
