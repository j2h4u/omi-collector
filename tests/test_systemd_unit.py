from __future__ import annotations

import fcntl
import getpass
import grp
import json
import os
import pwd
import shlex
import shutil
import stat
import subprocess
from dataclasses import dataclass
from pathlib import Path

import pytest

_ROOT = Path(__file__).parents[1]
_UNIT = _ROOT / "systemd" / "omi-collector.service"
_INSTALLER = _ROOT / "scripts" / "install-systemd-unit.sh"
_DEPLOYER = _ROOT / "scripts" / "deploy-systemd-service.sh"
_SEALER = _ROOT / "scripts" / "seal_deployment_tree.py"
_DEV_DEPLOYER = _ROOT / "scripts" / "dev-deploy-release.sh"
_STATUS_COMMAND = _ROOT / "scripts" / "omi-collector-status"
_STATUS_SUDOERS = _ROOT / "scripts" / "omi-collector-status.sudoers"
_RELEASE_COMMAND = _ROOT / "scripts" / "omi-collector-deploy-release"
_RELEASE_SUDOERS = _ROOT / "scripts" / "omi-collector-deploy-release.sudoers"
_FEATURE = _ROOT / "features" / "opportunistic_collection.feature"
_SOURCE_PACKAGE = _ROOT / "src" / "omi_collector"


@dataclass(frozen=True, slots=True)
class _DeploymentScenario:
    readiness_config: str | None = None
    config_check_failure: bool = False
    crash_loop: bool = False
    git_status: str = ""
    metadata_symlink: bool = False
    same_uid: bool = False
    same_primary_gid: bool = False
    service_group_membership: bool = False
    busybox_tools: bool = False


_DEFAULT_SCENARIO = _DeploymentScenario()


@dataclass(frozen=True, slots=True)
class _FakeCommandContext:
    fake_bin: Path
    log: Path
    config_file: Path
    source_package: Path
    build_state: Path
    uv_cache: Path
    python_install: Path
    deployment_root: Path
    deployments_dir: Path
    account_user: str
    account_group: str
    build_user: str
    build_group: str


@dataclass(frozen=True, slots=True)
class _DeploymentHarness:
    deployer: Path
    deployment_root: Path
    current_link: Path
    config_file: Path
    log: Path
    environment: dict[str, str]


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


def test_production_unit_uses_one_config_and_the_selected_release() -> None:
    service = _unit_sections()["Service"]

    assert service["User"] == "omi-collector"
    assert service["Group"] == "omi-collector"
    assert service["ExecStartPre"] == "+/usr/bin/bluetoothctl --timeout 10 power on"
    assert "EnvironmentFile" not in service
    assert service["ExecStart"] == (
        "/var/lib/omi-collector-deployments/current/bin/omi-collector service --config /srv/pipelines/omi/config.toml"
    )
    assert service["ReadWritePaths"].split() == [
        "/var/lib/omi-collector",
        "/srv/pipelines/omi",
    ]
    assert service["Environment"] == (
        "PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1 XDG_STATE_HOME=/var/lib/omi-collector"
    )
    assert service["StateDirectory"] == "omi-collector"
    assert service["AmbientCapabilities"] == "CAP_NET_RAW"
    assert "WorkingDirectory" not in service
    unit = _unit_sections()["Unit"]
    assert unit["Requires"] == "bluetooth.service"


def test_systemd_material_uses_only_the_fixed_unit_and_release_selector() -> None:
    installer = _INSTALLER.read_text(encoding="utf-8")
    deployer = _DEPLOYER.read_text(encoding="utf-8")
    unit = _UNIT.read_text(encoding="utf-8")

    assert {path.name for path in (_ROOT / "systemd").iterdir()} == {_UNIT.name}
    assert "EnvironmentFile" not in unit
    assert "config_file='/srv/pipelines/omi/config.toml'" in installer
    assert "config_file='/srv/pipelines/omi/config.toml'" in deployer
    assert "current_link='/var/lib/omi-collector-deployments/current'" in deployer
    assert 'config check --config "$config_file"' in deployer
    assert "share/omi-collector" in deployer
    assert '{"source_revision":"%s"}' in deployer
    assert "build_user='omi-collector-build'" in deployer
    assert "build_group='omi-collector-build'" in deployer
    assert "uv_cache_dir='/var/lib/omi-collector-build/uv-cache'" in deployer
    assert "python_install_dir='/var/lib/omi-collector-deployments/python'" in deployer


def test_deployer_runs_candidate_code_only_as_the_dedicated_build_identity() -> None:
    deployer = _DEPLOYER.read_text(encoding="utf-8")

    assert "build_user='omi-collector-build'" in deployer
    assert "account_user='omi-collector'" in deployer
    assert '[[ "$build_user" != "$service_user" && "$build_group" != "$service_group" ]]' in deployer
    assert 'build_uid=$(id -u "$build_user")' in deployer
    assert 'service_uid=$(id -u "$service_user")' in deployer
    assert 'supplementary_output=$(id -G "$build_user")' in deployer
    assert '"$group_id" != "$service_gid"' in deployer
    assert 'verify_installed_package "$runuser_bin" "$build_user"' in deployer
    assert 'validate_candidate_config "$runuser_bin" "$build_user"' in deployer
    assert '"$runuser_bin" --user "$build_user" --group "$build_group" -- env HOME="$build_home"' in deployer
    python_command = deployer.index('"${environment}/bin/python" -I -B -c')
    assert deployer.rfind('"$runuser_bin" --user "$build_user" --group "$build_group"', 0, python_command) >= 0
    assert deployer.index('validate_candidate_config "$runuser_bin" "$build_user"') < deployer.index(
        'seal_deployment_environment "$staged_environment"'
    )


def test_deployer_seals_candidate_paths_before_root_metadata_writes() -> None:
    deployer = _DEPLOYER.read_text(encoding="utf-8")

    assert "seal_tree_with_descriptors" in deployer
    assert "sealer_python='/usr/bin/python3'" in deployer
    assert "'-I' '-B' '-S'" in deployer
    assert "chown -hRP root:root" not in deployer
    assert "find -P" not in deployer
    assert '[[ ! -e "${environment}/share" && ! -L "${environment}/share" ]]' in deployer
    assert deployer.index('seal_deployment_environment "$staged_environment"') < deployer.index(
        'write_release_metadata "$staged_environment"'
    )


def test_installer_keeps_the_unit_and_config_targets_fixed() -> None:
    installer = _INSTALLER.read_text(encoding="utf-8")

    assert "unit_target='/etc/systemd/system/omi-collector.service'" in installer
    assert "ensure_service_account" in installer
    assert "groupadd --system" in installer
    assert "useradd --system" in installer
    assert 'chmod 0644 -- "$config_file"' in installer
    assert 'stage_file "$source_unit" "$unit_target" 0644 staged_unit' in installer
    assert "ExecStart=/usr/bin/true" in installer
    assert "systemd-analyze verify" in installer
    assert "rollback" in installer.lower()
    assert "--root=" not in installer
    assert "/usr/local/sbin/omi-collector-status" in installer
    assert "/etc/sudoers.d/omi-collector-status" in installer
    assert "/usr/sbin/visudo -cf" in installer


def test_installer_stages_root_owned_release_deploy_material_with_rollback() -> None:
    installer = _INSTALLER.read_text(encoding="utf-8")

    assert 'source_deploy_release="${repo_root}/scripts/omi-collector-deploy-release"' in installer
    assert 'source_deploy_release_sudoers="${repo_root}/scripts/omi-collector-deploy-release.sudoers"' in installer
    assert "deploy_release_target='/usr/local/sbin/omi-collector-deploy-release'" in installer
    assert "deploy_release_sudoers_target='/etc/sudoers.d/omi-collector-deploy-release'" in installer
    assert '/usr/sbin/visudo -cf "$source_deploy_release_sudoers"' in installer
    assert 'stage_file "$source_deploy_release" "$deploy_release_target" 0755 staged_deploy_release' in installer
    assert (
        'stage_file "$source_deploy_release_sudoers" "$deploy_release_sudoers_target" 0440 '
        "staged_deploy_release_sudoers"
    ) in installer
    assert 'backup_target "$deploy_release_target" deploy_release_backup' in installer
    assert 'backup_target "$deploy_release_sudoers_target" deploy_release_sudoers_backup' in installer
    assert 'restore_target "$deploy_release_target" "$deploy_release_backup"' in installer
    assert 'restore_target "$deploy_release_sudoers_target" "$deploy_release_sudoers_backup"' in installer


def test_release_deploy_wrapper_has_fixed_privileged_contract() -> None:
    command = _RELEASE_COMMAND.read_text(encoding="utf-8")
    policy = _RELEASE_SUDOERS.read_text(encoding="utf-8")

    assert command.startswith("#!/usr/bin/env bash\nset -euo pipefail\n")
    assert "declare -r deployer='/opt/omi-collector/scripts/dev-deploy-release.sh'" in command
    assert "[[ $# -eq 1 ]]" in command
    assert '[[ "$1" =~ ^v[0-9]+\\.[0-9]+\\.[0-9]+$ ]]' in command
    assert 'exec "$deployer" "$1"' in command
    assert "stat -c '%U:%G:%a' \"$deployer\"" in command
    assert "root:root:755" in command
    assert policy == "%sudo ALL=(root) NOPASSWD: /usr/local/sbin/omi-collector-deploy-release\n"
    assert (_RELEASE_COMMAND.stat().st_mode & 0o777) == 0o755
    assert (_RELEASE_SUDOERS.stat().st_mode & 0o777) == 0o644


def test_release_deploy_wrapper_rejects_invalid_arguments_without_delegating() -> None:
    command = _RELEASE_COMMAND.read_text(encoding="utf-8")

    assert "printf 'Usage: sudo -n omi-collector-deploy-release vMAJOR.MINOR.PATCH\\n'" in command
    assert "printf 'ERROR: invalid release tag: %s\\n' \"$1\"" in command
    assert "exit 2" in command
    assert command.index("[[ $# -eq 1 ]]") < command.index('[[ "$1" =~')
    assert command.index('[[ "$1" =~') < command.index('exec "$deployer" "$1"')
    assert "clock-recover" not in command


def test_operator_status_command_has_no_arguments_or_extra_privilege() -> None:
    command = _STATUS_COMMAND.read_text(encoding="utf-8")
    policy = _STATUS_SUDOERS.read_text(encoding="utf-8")

    assert "(( $# != 0 ))" in command
    assert "runuser --user omi-collector" in command
    assert "device status --config /srv/pipelines/omi/config.toml --hours 24" in command.replace("\\\n", "")
    assert policy == '%sudo ALL=(root) NOPASSWD: /usr/local/sbin/omi-collector-status ""\n'


def test_shell_scripts_are_syntactically_clean() -> None:
    scripts = (_INSTALLER, _DEPLOYER, _DEV_DEPLOYER, _STATUS_COMMAND, _RELEASE_COMMAND)

    subprocess.run(("bash", "-n", *(str(script) for script in scripts)), check=True)
    subprocess.run(("shellcheck", *(str(script) for script in scripts)), check=True)


def test_checked_in_unit_passes_systemd_validation(tmp_path: Path) -> None:
    validation_unit = tmp_path / "omi-collector.service"
    bluetooth_unit = tmp_path / "bluetooth.service"
    validation_unit.write_text(
        _UNIT.read_text(encoding="utf-8")
        .replace(
            "ExecStart=/var/lib/omi-collector-deployments/current/bin/omi-collector "
            "service --config /srv/pipelines/omi/config.toml",
            "ExecStart=/usr/bin/true",
        )
        .replace("ExecStartPre=+/usr/bin/bluetoothctl --timeout 10 power on", "ExecStartPre=/usr/bin/true"),
        encoding="utf-8",
    )
    bluetooth_unit.write_text("[Service]\nExecStart=/usr/bin/true\n", encoding="utf-8")

    subprocess.run(("systemd-analyze", "verify", str(bluetooth_unit), str(validation_unit)), check=True)


def test_dev_release_deployer_accepts_https_with_readonly_caller_variable() -> None:
    function_prelude = _DEV_DEPLOYER.read_text(encoding="utf-8").split("declare -r PROJECT_DIR", maxsplit=1)[0]
    command = "\n".join(
        (
            function_prelude,
            "declare -r origin_url='https://github.com/j2h4u/omi-collector.git'",
            'is_expected_origin "$origin_url"',
        )
    )

    subprocess.run(("bash", "-c", command), check=True)


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


def _write_build_fakes(
    context: _FakeCommandContext,
    scenario: _DeploymentScenario,
) -> None:
    fake_bin = context.fake_bin
    config_file = context.config_file
    quoted_log = shlex.quote(str(context.log))
    quoted_source = shlex.quote(str(context.source_package))
    quoted_user = shlex.quote(context.build_user)

    (fake_bin / "uv").write_text(
        "#!/usr/bin/env bash\n"
        "set -uo pipefail\n"
        f'printf "uv cwd=%s args=%s\\n" "$PWD" "$*" >> {quoted_log}\n'
        '[[ "$DEPLOY_BUILD_FAIL" != 1 ]] || exit 1\n'
        'if [[ "$1" == python && "$2" == install ]]; then\n'
        '    mkdir --parents "$UV_PYTHON_INSTALL_DIR/cpython-3.14/bin"\n'
        "    exit 0\n"
        "fi\n"
        'python3 -m venv --copies --clear "$UV_PROJECT_ENVIRONMENT"\n'
        'purelib=$("$UV_PROJECT_ENVIRONMENT/bin/python" -I -B -c '
        "'import sysconfig; print(sysconfig.get_path(\"purelib\"))')\n"
        f'cp -a {quoted_source} "$purelib/omi_collector"\n'
        'entrypoint="$UV_PROJECT_ENVIRONMENT/bin/omi-collector"\n'
        "printf '%s\\n' '#!/usr/bin/env bash' > \"$entrypoint\"\n"
        "printf '%s\\n' 'if [[ \"${DEPLOY_CONFIG_FAIL:-0}\" == 1 ]]; then exit 2; fi' >> \"$entrypoint\"\n"
        f'printf \'%s\\n\' \'printf \'"\'"\'{{"config":"{config_file}","status":"config_valid"}}\\n\'"\'"\'\' >> "$entrypoint"\n'
        'chmod 0755 "$entrypoint"\n'
        'if [[ "${DEPLOY_METADATA_SYMLINK:-0}" == 1 ]]; then\n'
        '    ln --symbolic -- "$DEPLOY_SENTINEL" "$UV_PROJECT_ENVIRONMENT/share"\n'
        "fi\n",
        encoding="utf-8",
    )
    (fake_bin / "uv").chmod(0o755)

    (fake_bin / "git").write_text(
        "#!/usr/bin/env bash\n"
        '[[ "$1" == -C ]] || exit 2\n'
        'case "$3" in\n'
        "rev-parse) printf '%040d\\n' 0 ;;\n"
        f"status) printf '%s' {shlex.quote(scenario.git_status)} ;;\n"
        "*) exit 2 ;;\n"
        "esac\n",
        encoding="utf-8",
    )
    (fake_bin / "git").chmod(0o755)

    (fake_bin / "runuser").write_text(
        "#!/usr/bin/env bash\n"
        f'printf "runuser args=%s\\n" "$*" >> {quoted_log}\n'
        f'[[ "$1" == --user && "$2" == {quoted_user} && "$3" == --group && "$4" == {shlex.quote(context.build_group)} && "$5" == -- ]] || exit 2\n'
        "shift 5\n"
        'exec "$@"\n',
        encoding="utf-8",
    )
    (fake_bin / "runuser").chmod(0o755)


def _write_filesystem_fakes(context: _FakeCommandContext, scenario: _DeploymentScenario) -> None:
    fake_bin = context.fake_bin
    quoted_log = shlex.quote(str(context.log))
    quoted_config = shlex.quote(str(context.config_file))
    quoted_build_state = shlex.quote(str(context.build_state))
    quoted_uv_cache = shlex.quote(str(context.uv_cache))
    quoted_python_install = shlex.quote(str(context.python_install))
    quoted_deployment_root = shlex.quote(str(context.deployment_root))
    quoted_deployments_dir = shlex.quote(str(context.deployments_dir))

    busybox_check = 'case "$*" in *--no-dereference*|*--recursive*) exit 64 ;; esac\n' if scenario.busybox_tools else ""
    (fake_bin / "chown").write_text(
        f'#!/usr/bin/env bash\n{busybox_check}printf "chown args=%s\\n" "$*" >> {quoted_log}\nexit 0\n',
        encoding="utf-8",
    )
    (fake_bin / "chown").chmod(0o755)

    if scenario.busybox_tools:
        busybox = shutil.which("busybox")
        assert busybox is not None
        quoted_busybox = shlex.quote(busybox)
        for tool_name in ("chmod", "find"):
            (fake_bin / tool_name).write_text(
                f'#!/usr/bin/env bash\nexec {quoted_busybox} {tool_name} "$@"\n',
                encoding="utf-8",
            )
            (fake_bin / tool_name).chmod(0o755)

    (fake_bin / "install").write_text(
        "#!/usr/bin/env bash\n"
        "set -uo pipefail\n"
        "args=()\n"
        "while (( $# > 0 )); do\n"
        '    case "$1" in\n'
        "        -o|-g) shift 2 ;;\n"
        '        *) args+=("$1"); shift ;;\n'
        "    esac\n"
        "done\n"
        'exec /usr/bin/install "${args[@]}"\n',
        encoding="utf-8",
    )
    (fake_bin / "install").chmod(0o755)

    (fake_bin / "stat").write_text(
        "#!/usr/bin/env bash\n"
        'path="${@: -1}"\n'
        f"if [[ \"$path\" == {quoted_config} ]]; then printf '%s\\n' 'root:root:644'; exit 0; fi\n"
        f"if [[ \"$path\" == {quoted_build_state} ]]; then printf '%s\\n' 'root:root:755'; exit 0; fi\n"
        f"if [[ \"$path\" == {quoted_uv_cache} ]]; then printf '%s\\n' "
        f"'{context.build_user}:{context.build_group}:750'; exit 0; fi\n"
        f'if [[ "$path" == {quoted_deployment_root} || "$path" == {quoted_deployments_dir} '
        f'|| "$path" == {quoted_python_install} || "$path" == {quoted_deployments_dir}/release-* '
        "|| \"$path\" == */share/omi-collector ]]; then printf '%s\\n' 'root:root:755'; exit 0; fi\n"
        'exec /usr/bin/stat "$@"\n',
        encoding="utf-8",
    )
    (fake_bin / "stat").chmod(0o755)


def _write_service_fakes(context: _FakeCommandContext, scenario: _DeploymentScenario) -> None:
    fake_bin = context.fake_bin
    quoted_log = shlex.quote(str(context.log))

    initial_invocation = "a" * 32
    restarted_invocation = "b" * 32
    crash_clause = (
        f"if (( count > 1 )); then pid=5252; restarts=1; invocation={restarted_invocation}; fi\n"
        if scenario.crash_loop
        else ""
    )
    (fake_bin / "systemctl").write_text(
        "#!/usr/bin/env bash\n"
        f'printf "systemctl args=%s\\n" "$*" >> {quoted_log}\n'
        'case "$1" in\n'
        "stop|start|restart) exit 0 ;;\n"
        "show)\n"
        f"count_file={shlex.quote(str(fake_bin / 'show-count'))}\n"
        'count=0; [[ -f "$count_file" ]] && read -r count < "$count_file"\n'
        '(( count++ )); printf "%s\\n" "$count" > "$count_file"\n'
        f"pid=4242; restarts=0; invocation={initial_invocation}\n"
        f"{crash_clause}"
        'printf "ActiveState=active\\nMainPID=%s\\nNRestarts=%s\\nInvocationID=%s\\n" '
        '"$pid" "$restarts" "$invocation"\n'
        ";;\n"
        "*) exit 2 ;;\n"
        "esac\n",
        encoding="utf-8",
    )
    (fake_bin / "systemctl").chmod(0o755)

    readiness_config = scenario.readiness_config or str(context.config_file)
    (fake_bin / "journalctl").write_text(
        "#!/usr/bin/env bash\n"
        f'printf "journalctl args=%s\\n" "$*" >> {quoted_log}\n'
        f'printf \'%s\\n\' \'{{"config":"{readiness_config}","status":"deployment_ready"}}\'\n',
        encoding="utf-8",
    )
    (fake_bin / "journalctl").chmod(0o755)
    (fake_bin / "sleep").write_text(
        f'#!/usr/bin/env bash\nprintf "sleep args=%s\\n" "$*" >> {quoted_log}\n',
        encoding="utf-8",
    )
    (fake_bin / "sleep").chmod(0o755)


def _write_fake_commands(context: _FakeCommandContext, scenario: _DeploymentScenario) -> None:
    _write_build_fakes(context, scenario)
    _write_filesystem_fakes(context, scenario)
    _write_service_fakes(context, scenario)
    _write_identity_fakes(context, scenario)


def _write_identity_fakes(context: _FakeCommandContext, scenario: _DeploymentScenario) -> None:
    fake_bin = context.fake_bin
    quoted_build_user = shlex.quote(context.build_user)
    quoted_build_group = shlex.quote(context.build_group)
    quoted_service_user = shlex.quote(context.account_user)
    build_uid = "1000" if scenario.same_uid else "1001"
    build_gid = "2000" if scenario.same_primary_gid else "2001"
    build_groups = f"{build_gid} 2000" if scenario.service_group_membership else build_gid
    quoted_build_groups = shlex.quote(build_groups)

    (fake_bin / "getent").write_text(
        "#!/usr/bin/env bash\n"
        f'if [[ "$1" == group && "$2" == {quoted_build_group} ]]; then exit 0; fi\n'
        f'if [[ "$1" == passwd && "$2" == {quoted_build_user} ]]; then exit 0; fi\n'
        'exec /usr/bin/getent "$@"\n',
        encoding="utf-8",
    )
    (fake_bin / "getent").chmod(0o755)
    (fake_bin / "id").write_text(
        "#!/usr/bin/env bash\n"
        f'if [[ "$1" == -gn && "$2" == {quoted_build_user} ]]; then printf \'%s\\n\' {quoted_build_group}; exit 0; fi\n'
        f'if [[ "$1" == -u && "$2" == {quoted_build_user} ]]; then printf \'%s\\n\' {build_uid}; exit 0; fi\n'
        f'if [[ "$1" == -g && "$2" == {quoted_build_user} ]]; then printf \'%s\\n\' {build_gid}; exit 0; fi\n'
        f'if [[ "$1" == -u && "$2" == {quoted_service_user} ]]; then printf \'%s\\n\' 1000; exit 0; fi\n'
        f'if [[ "$1" == -g && "$2" == {quoted_service_user} ]]; then printf \'%s\\n\' 2000; exit 0; fi\n'
        f'if [[ "$1" == -G && "$2" == {quoted_build_user} ]]; then printf \'%s\\n\' {quoted_build_groups}; exit 0; fi\n'
        'exec /usr/bin/id "$@"\n',
        encoding="utf-8",
    )
    (fake_bin / "id").chmod(0o755)


def _stage_harness_repository(tmp_path: Path) -> tuple[Path, Path, Path]:
    staged_repo = tmp_path / "repo"
    scripts_dir = staged_repo / "scripts"
    systemd_dir = staged_repo / "systemd"
    source_root = staged_repo / "src"
    source_package = source_root / "omi_collector"
    scripts_dir.mkdir(parents=True)
    systemd_dir.mkdir()
    source_root.mkdir()
    shutil.copytree(_SOURCE_PACKAGE, source_package)
    shutil.copy2(_SEALER, scripts_dir / _SEALER.name)
    shutil.copy2(_UNIT, systemd_dir / _UNIT.name)
    installed_unit = tmp_path / "installed" / "omi-collector.service"
    installed_unit.parent.mkdir()
    shutil.copy2(systemd_dir / _UNIT.name, installed_unit)
    return scripts_dir, source_package, installed_unit


def _fake_command_context(tmp_path: Path, source_package: Path) -> _FakeCommandContext:
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    log = tmp_path / "commands.log"
    log.touch()
    build_state = tmp_path / "build-state"
    uv_cache = build_state / "uv-cache"
    deployment_root = tmp_path / "deployments"
    deployments_dir = deployment_root / "releases"
    python_install = deployment_root / "python"
    config_file = tmp_path / "config.toml"
    config_file.write_text(
        '[pendant]\naddress = "12:34:56:78:9A:BC"\n',
        encoding="utf-8",
    )
    config_file.chmod(0o644)
    account_user = getpass.getuser()
    account_group = grp.getgrgid(pwd.getpwnam(account_user).pw_gid).gr_name
    build_user = "omi-collector-build-test"
    build_group = "omi-collector-build-test"
    return _FakeCommandContext(
        fake_bin,
        log,
        config_file,
        source_package,
        build_state,
        uv_cache,
        python_install,
        deployment_root,
        deployments_dir,
        account_user,
        account_group,
        build_user,
        build_group,
    )


def _deployment_harness(tmp_path: Path, scenario: _DeploymentScenario = _DEFAULT_SCENARIO) -> _DeploymentHarness:
    scripts_dir, source_package, installed_unit = _stage_harness_repository(tmp_path)
    context = _fake_command_context(tmp_path, source_package)
    _write_fake_commands(
        context,
        scenario,
    )

    current_link = context.deployment_root / "current"
    deployer = _DEPLOYER.read_text(encoding="utf-8")
    replacements = (
        ("/etc/systemd/system/omi-collector.service", str(installed_unit)),
        ("/srv/pipelines/omi/config.toml", str(context.config_file)),
        ("/usr/local/bin/uv", str(context.fake_bin / "uv")),
        ("/var/lib/omi-collector-deployments/releases", str(context.deployments_dir)),
        (
            "/var/lib/omi-collector-deployments/.deployment.lock",
            str(context.deployment_root / ".deployment.lock"),
        ),
        ("/var/lib/omi-collector-deployments/current", str(current_link)),
        ("/var/lib/omi-collector-deployments", str(context.deployment_root)),
        ("/var/lib/omi-collector-build/uv-cache", str(context.uv_cache)),
        ("/var/lib/omi-collector-build", str(context.build_state)),
        ("account_user='omi-collector'", f"account_user={shlex.quote(context.account_user)}"),
        ("account_group='omi-collector'", f"account_group={shlex.quote(context.account_group)}"),
        ("build_user='omi-collector-build'", f"build_user={shlex.quote(context.build_user)}"),
        ("build_group='omi-collector-build'", f"build_group={shlex.quote(context.build_group)}"),
        ("sealer_owner='0:0'", f"sealer_owner='{os.getuid()}:{os.getgid()}'"),
        ("(( EUID == 0 ))", "true"),
    )
    for production_value, harness_value in replacements:
        assert production_value in deployer
        deployer = deployer.replace(production_value, harness_value)
    harness_deployer = scripts_dir / _DEPLOYER.name
    harness_deployer.write_text(deployer, encoding="utf-8")
    harness_deployer.chmod(0o755)

    environment = {
        **os.environ,
        "PATH": f"{context.fake_bin}{os.pathsep}{os.environ['PATH']}",
        "DEPLOY_BUILD_FAIL": "0",
        "DEPLOY_CONFIG_FAIL": "1" if scenario.config_check_failure else "0",
        "DEPLOY_METADATA_SYMLINK": "1" if scenario.metadata_symlink else "0",
        "DEPLOY_SENTINEL": str(tmp_path / "metadata-sentinel"),
    }
    return _DeploymentHarness(
        harness_deployer,
        context.deployment_root,
        current_link,
        context.config_file,
        context.log,
        environment,
    )


def _previous_release(harness: _DeploymentHarness) -> Path:
    release = harness.deployment_root / "releases" / ("release-" + "1" * 40 + "-1-1")
    release.mkdir(parents=True)
    harness.current_link.symlink_to(f"releases/{release.name}")
    return release


def _run_deployer(harness: _DeploymentHarness, caller_cwd: Path | None = None) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [harness.deployer],
        check=False,
        capture_output=True,
        text=True,
        env=harness.environment,
        cwd=caller_cwd,
    )


def test_deployer_builds_validates_selects_and_seals_one_release(tmp_path: Path) -> None:
    harness = _deployment_harness(tmp_path)
    obsolete = harness.deployment_root / "releases" / ("release-" + "2" * 40 + "-1-1")
    obsolete.mkdir(parents=True)
    unrelated = harness.deployment_root / "releases" / "keep-me"
    unrelated.mkdir()

    result = _run_deployer(harness)

    assert result.returncode == 0, result.stderr
    assert harness.current_link.is_symlink()
    selected = harness.current_link.resolve(strict=True)
    assert selected.name.startswith("release-" + "0" * 40)
    release_metadata = selected / "share" / "omi-collector" / "release.json"
    assert release_metadata.read_bytes() == b'{"source_revision":"0000000000000000000000000000000000000000"}\n'
    assert json.loads(release_metadata.read_text(encoding="utf-8")) == {"source_revision": "0" * 40}
    assert not obsolete.exists()
    assert unrelated.is_dir()
    assert tuple(path for path in (harness.deployment_root / "releases").glob("release-*")) == (selected,)
    commands = harness.log.read_text(encoding="utf-8")
    assert "uv cwd=" in commands
    assert "args=sync --project" in commands
    assert "config check --config" in commands
    assert "systemctl args=stop omi-collector.service" in commands
    assert "systemctl args=restart omi-collector.service" in commands
    assert f"config={harness.config_file}" in result.stdout


def test_deployer_verifies_candidate_as_builder_before_root_seals_or_restarts(tmp_path: Path) -> None:
    harness = _deployment_harness(tmp_path)

    result = _run_deployer(harness)

    assert result.returncode == 0, result.stderr
    commands = harness.log.read_text(encoding="utf-8")
    builder_runs = [line for line in commands.splitlines() if line.startswith("runuser args=")]
    assert len(builder_runs) == 4
    assert all("--user omi-collector-build-test --group omi-collector-build-test --" in line for line in builder_runs)
    assert "python install 3.14" in builder_runs[0]
    assert "sync --project" in builder_runs[1]
    assert "/bin/python -I -B -c" in builder_runs[2]
    assert "config check --config" in builder_runs[3]
    config_check = commands.index("config check --config")
    restart = commands.index("systemctl args=stop omi-collector.service")
    assert config_check < restart
    deployer = harness.deployer.read_text(encoding="utf-8")
    assert deployer.index('validate_candidate_config "$runuser_bin" "$build_user"') < deployer.index(
        'seal_deployment_environment "$staged_environment"'
    )


def test_deployer_build_user_runs_uv_from_the_staged_repository_not_the_caller_directory(tmp_path: Path) -> None:
    harness = _deployment_harness(tmp_path)
    caller_cwd = tmp_path / "caller"
    caller_cwd.mkdir()

    result = _run_deployer(harness, caller_cwd)

    assert result.returncode == 0, result.stderr
    commands = harness.log.read_text(encoding="utf-8")
    expected_cwd = harness.deployer.parents[1]
    uv_runs = [line for line in commands.splitlines() if line.startswith("uv cwd=")]
    assert len(uv_runs) == 2
    assert all(f"uv cwd={expected_cwd} " in line for line in uv_runs)
    assert all(f"uv cwd={caller_cwd} " not in line for line in uv_runs)
    assert "args=python install 3.14" in uv_runs[0]
    assert "args=sync --project" in uv_runs[1]


def test_deployer_keeps_the_service_untouched_when_the_build_user_uv_command_fails(tmp_path: Path) -> None:
    harness = _deployment_harness(tmp_path)
    previous = _previous_release(harness)
    harness.environment["DEPLOY_BUILD_FAIL"] = "1"

    result = _run_deployer(harness)

    assert result.returncode != 0
    assert "could not install the managed build Python" in result.stderr
    assert harness.current_link.resolve(strict=True) == previous
    assert tuple((harness.deployment_root / "releases").glob("release-*")) == (previous,)
    assert "systemctl args=" not in harness.log.read_text(encoding="utf-8")


def test_deployer_rejects_candidate_metadata_symlink_before_service_restart(tmp_path: Path) -> None:
    harness = _deployment_harness(tmp_path, _DeploymentScenario(metadata_symlink=True))
    sentinel = Path(harness.environment["DEPLOY_SENTINEL"])
    sentinel.write_text("preserve me\n", encoding="utf-8")

    result = _run_deployer(harness)

    assert result.returncode != 0
    assert "refusing external symlink target" in result.stderr
    assert sentinel.read_text(encoding="utf-8") == "preserve me\n"
    assert "systemctl args=" not in harness.log.read_text(encoding="utf-8")


def test_deployer_sealing_is_compatible_with_busybox_paths(tmp_path: Path) -> None:
    harness = _deployment_harness(tmp_path, _DeploymentScenario(busybox_tools=True))

    result = _run_deployer(harness)

    assert result.returncode == 0, result.stderr
    commands = harness.log.read_text(encoding="utf-8")
    assert "chown args=" in commands
    assert "--no-dereference" not in commands


def _run_tree_sealer(root: Path, *allowed_external: Path) -> subprocess.CompletedProcess[str]:
    command = [
        "/usr/bin/python3",
        "-I",
        "-B",
        "-S",
        str(_SEALER),
        "--root",
        str(root),
        "--owner",
        f"{os.getuid()}:{os.getgid()}",
    ]
    for allowed_path in allowed_external:
        command.extend(("--allow-external", str(allowed_path)))
    return subprocess.run(command, check=False, capture_output=True, text=True)


def test_descriptor_sealer_rejects_swapped_candidate_symlink(tmp_path: Path) -> None:
    release = tmp_path / "release"
    release.mkdir()
    sentinel = tmp_path / "sentinel"
    sentinel.write_text("preserve me\n", encoding="utf-8")
    candidate = release / "candidate.py"
    candidate.write_text("candidate\n", encoding="utf-8")
    candidate.unlink()
    candidate.symlink_to(sentinel)
    original_mode = sentinel.stat().st_mode

    result = _run_tree_sealer(release)

    assert result.returncode != 0
    assert "refusing external symlink target" in result.stderr
    assert sentinel.read_text(encoding="utf-8") == "preserve me\n"
    assert sentinel.stat().st_mode == original_mode


def test_descriptor_sealer_seals_regular_entries_without_following_safe_links(tmp_path: Path) -> None:
    release = tmp_path / "release"
    binary_dir = release / "bin"
    binary_dir.mkdir(parents=True)
    executable = binary_dir / "omi-collector"
    executable.write_text("#!/bin/sh\n", encoding="utf-8")
    executable.chmod(0o700)
    data_file = release / "data.txt"
    data_file.write_text("data\n", encoding="utf-8")
    data_file.chmod(0o600)
    (release / "data-link").symlink_to("data.txt")

    result = _run_tree_sealer(release)

    assert result.returncode == 0, result.stderr
    assert stat.S_IMODE(release.stat().st_mode) == 0o755
    assert stat.S_IMODE(binary_dir.stat().st_mode) == 0o755
    assert stat.S_IMODE(executable.stat().st_mode) == 0o755
    assert stat.S_IMODE(data_file.stat().st_mode) == 0o644


def test_descriptor_sealer_contract_uses_fd_identity_and_no_follow_operations() -> None:
    sealer = _SEALER.read_text(encoding="utf-8")
    deployer = _DEPLOYER.read_text(encoding="utf-8")

    assert "os.O_NOFOLLOW" in sealer
    assert "os.O_DIRECTORY" in sealer
    assert "os.fstat" in sealer
    assert "st_dev != context.root_device" in sealer
    assert "st_ino" in sealer
    assert "os.fchown" in sealer
    assert "os.fchmod" in sealer
    assert "sealer_python='/usr/bin/python3'" in deployer
    assert "'-I' '-B' '-S'" in deployer


@pytest.mark.parametrize(
    ("scenario", "message"),
    (
        (_DeploymentScenario(same_uid=True), "build UID must differ"),
        (_DeploymentScenario(same_primary_gid=True), "build primary GID must differ"),
        (_DeploymentScenario(service_group_membership=True), "must not belong to the live service group"),
    ),
)
def test_deployer_rejects_build_identity_that_overlaps_the_service(
    tmp_path: Path, scenario: _DeploymentScenario, message: str
) -> None:
    harness = _deployment_harness(tmp_path, scenario)

    result = _run_deployer(harness)

    assert result.returncode != 0
    assert message in result.stderr
    commands = harness.log.read_text(encoding="utf-8")
    assert "runuser args=" not in commands
    assert "systemctl args=" not in commands


def test_deployer_rejects_config_before_stopping_the_previous_service(tmp_path: Path) -> None:
    harness = _deployment_harness(tmp_path, _DeploymentScenario(config_check_failure=True))
    previous = _previous_release(harness)

    result = _run_deployer(harness)

    assert result.returncode != 0
    assert "candidate rejected the operator configuration" in result.stderr
    assert harness.current_link.resolve(strict=True) == previous
    assert tuple((harness.deployment_root / "releases").glob("release-*")) == (previous,)
    assert "systemctl args=" not in harness.log.read_text(encoding="utf-8")


def test_deployer_restores_previous_release_when_readiness_fails(tmp_path: Path) -> None:
    harness = _deployment_harness(tmp_path, _DeploymentScenario(readiness_config="/wrong/config.toml"))
    previous = _previous_release(harness)

    result = _run_deployer(harness)

    assert result.returncode != 0
    assert "did not announce readiness" in result.stderr
    assert "Restored the prior deployment state." in result.stderr
    assert harness.current_link.resolve(strict=True) == previous
    assert tuple((harness.deployment_root / "releases").glob("release-*")) == (previous,)
    commands = harness.log.read_text(encoding="utf-8")
    assert commands.count("systemctl args=stop omi-collector.service") == 2
    assert commands.count("systemctl args=restart omi-collector.service") == 2


def test_failed_first_deployment_leaves_no_selector_or_release(tmp_path: Path) -> None:
    harness = _deployment_harness(tmp_path, _DeploymentScenario(readiness_config="/wrong/config.toml"))

    result = _run_deployer(harness)

    assert result.returncode != 0
    assert not harness.current_link.exists()
    assert not tuple((harness.deployment_root / "releases").glob("release-*"))
    commands = harness.log.read_text(encoding="utf-8")
    assert commands.count("systemctl args=restart omi-collector.service") == 1


def test_deployer_restores_previous_release_after_restart_during_stability(tmp_path: Path) -> None:
    harness = _deployment_harness(tmp_path, _DeploymentScenario(crash_loop=True))
    previous = _previous_release(harness)

    result = _run_deployer(harness)

    assert result.returncode != 0
    assert "restarted during the stability interval" in result.stderr
    assert harness.current_link.resolve(strict=True) == previous
    assert tuple((harness.deployment_root / "releases").glob("release-*")) == (previous,)


def test_deployer_rejects_a_dirty_source_before_creating_a_release(tmp_path: Path) -> None:
    harness = _deployment_harness(tmp_path, _DeploymentScenario(git_status=" M source.py\n"))

    result = _run_deployer(harness)

    assert result.returncode != 0
    assert "dirty source tree" in result.stderr
    assert not harness.deployment_root.exists()


def test_deployer_rejects_a_concurrent_transaction(tmp_path: Path) -> None:
    harness = _deployment_harness(tmp_path)
    harness.deployment_root.mkdir()
    lock_path = harness.deployment_root / ".deployment.lock"

    with lock_path.open("w", encoding="utf-8") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        result = _run_deployer(harness)

    assert result.returncode != 0
    assert "another deployment transaction is already in progress" in result.stderr
    assert not tuple((harness.deployment_root / "releases").glob("release-*"))


def test_deployer_rejects_an_unmanaged_current_selector(tmp_path: Path) -> None:
    harness = _deployment_harness(tmp_path)
    harness.deployment_root.mkdir()
    harness.current_link.symlink_to("/tmp/unmanaged-release")

    result = _run_deployer(harness)

    assert result.returncode != 0
    assert "unsupported target" in result.stderr
    assert not tuple((harness.deployment_root / "releases").glob("release-*"))
