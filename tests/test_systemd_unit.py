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
_FEATURE = _ROOT / "features" / "opportunistic_collection.feature"
_SOURCE_PACKAGE = _ROOT / "src" / "omi_collector"


@dataclass(frozen=True, slots=True)
class _DeploymentScenario:
    readiness_layout: str | None = None
    crash_loop: bool = False


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
        section[key] = value
    return sections


def test_production_unit_uses_the_dedicated_account_and_static_pair() -> None:
    service = _unit_sections()["Service"]

    assert service["User"] == "omi-collector"
    assert service["Group"] == "omi-collector"
    assert service["EnvironmentFile"] == "/etc/omi-collector/omi-collector.env"
    assert service["ExecStart"] == "/usr/local/libexec/omi-collector/omi-collector-exec"
    assert service["ReadWritePaths"] == "/var/lib/omi-collector"
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
    assert "rollback_pair" in installer
    assert '[[ "$layout_file" == /* && -f "$layout_file" && ! -L "$layout_file" ]]' in installer
    assert "must name a regular absolute file" in installer
    assert "environment_file='/etc/omi-collector/omi-collector.env'" in deployer
    assert "installed_unit='/etc/systemd/system/omi-collector.service'" in deployer
    assert "installed_exec='/usr/local/libexec/omi-collector/omi-collector-exec'" in deployer
    assert "(( EUID == 0 ))" in deployer
    assert '"$runuser_bin" --user "$account_user" -- env' in deployer
    assert "sudo --non-interactive" not in deployer
    assert '[[ "${layout_path:-}" == /* && -f "$layout_path" && ! -L "$layout_path" ]]' in deployer
    assert "must name a regular absolute file" in deployer


def test_wrapper_and_example_require_explicit_operator_values() -> None:
    example = (_ROOT / "config" / "omi-collector.env.example").read_text(encoding="utf-8")
    wrapper = _EXEC.read_text(encoding="utf-8")

    assert _EXEC.stat().st_mode & 0o111
    assert _INSTALLER.stat().st_mode & 0o111
    assert _DEPLOYER.stat().st_mode & 0o111
    assert "eval" not in wrapper
    assert "OMI_COLLECTOR_DEVICE_ADDRESS=AA:BB:CC:DD:EE:FF" in example
    assert "OMI_COLLECTOR_LAYOUT_PATH=/var/lib/omi-collector/collector.toml" in example
    assert "OMI_COLLECTOR_UV_PROJECT_ENVIRONMENT=/var/lib/omi-collector/venv" in example


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


def _write_commands(fake_bin: Path, log: Path, scenario: _DeploymentScenario, layout_path: Path) -> None:
    quoted_log = shlex.quote(str(log))
    (fake_bin / "uv").write_text(
        "#!/usr/bin/env bash\n"
        f'printf "uv args=%s\\n" "$*" >> {quoted_log}\n'
        f'printf "uv env=%s|%s|%s\\n" "$UV_PROJECT_ENVIRONMENT" "$UV_LINK_MODE" "$UV_CACHE_DIR" >> {quoted_log}\n',
        encoding="utf-8",
    )
    (fake_bin / "uv").chmod(0o755)
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
    (fake_bin / "systemctl").write_text(
        "#!/usr/bin/env bash\n"
        f'printf "systemctl args=%s\\n" "$*" >> {quoted_log}\n'
        'case "${1:-}" in\n'
        "restart) exit 0 ;;\n"
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
    (fake_bin / "journalctl").write_text(
        "#!/usr/bin/env bash\n"
        f'printf "journalctl args=%s\\n" "$*" >> {quoted_log}\n'
        '[[ "${1:-}" == --unit ]] || exit 2\n'
        f'printf \'%s\\n\' \'{{"layout":"{readiness_layout}","status":"deployment_ready"}}\'\n',
        encoding="utf-8",
    )
    (fake_bin / "journalctl").chmod(0o755)
    (fake_bin / "sleep").write_text(
        f'#!/usr/bin/env bash\nprintf "sleep args=%s\\n" "$*" >> {quoted_log}\n', encoding="utf-8"
    )
    (fake_bin / "sleep").chmod(0o755)


def _deployment_harness(
    tmp_path: Path, scenario: _DeploymentScenario = _DEFAULT_DEPLOYMENT_SCENARIO
) -> tuple[Path, Path, Path, dict[str, str], Path]:
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    state_root = tmp_path / "state"
    state_root.mkdir()
    external_root = tmp_path / "external"
    external_root.mkdir()
    layout_path = external_root / "collector.toml"
    layout_path.write_text('[collector]\nroot = "collector"\n', encoding="utf-8")
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
    _write_commands(fake_bin, log, scenario, layout_path)
    return (
        harness_deployer,
        environment_file,
        layout_path,
        {**os.environ, "PATH": f"{fake_bin}{os.pathsep}{os.environ['PATH']}"},
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
    assert "systemctl args=restart omi-collector.service" in commands
    assert "sleep args=6" in commands


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


def test_deployer_harness_rejects_restart_during_stability(tmp_path: Path) -> None:
    deployer, _, _, environment, _ = _deployment_harness(tmp_path, _DeploymentScenario(crash_loop=True))

    result = subprocess.run(
        [*_root_prefix(), str(deployer)], check=False, capture_output=True, text=True, env=environment
    )

    assert result.returncode != 0
    assert "restarted during the stability interval" in result.stderr
