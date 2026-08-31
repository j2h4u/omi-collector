#!/usr/bin/env bash

set -uo pipefail

function die {
    printf 'ERROR: %s\n' "${1:-operation failed}" >&2
    exit "${2:-1}"
}

function read_service_snapshot {
    local -r unit_name="$1"
    local active_state process_id restart_count invocation_id output line key value

    output=$(systemctl show "$unit_name" --property=ActiveState --property=MainPID --property=NRestarts --property=InvocationID) \
        || return 1
    active_state=''
    process_id=''
    restart_count=''
    invocation_id=''
    while IFS= read -r line || [[ -n "$line" ]]; do
        key=${line%%=*}
        value=${line#*=}
        case "$key" in
            ActiveState) active_state=$value ;;
            MainPID) process_id=$value ;;
            NRestarts) restart_count=$value ;;
            InvocationID) invocation_id=$value ;;
        esac
    done <<< "$output"
    [[ "$active_state" == active && "$process_id" =~ ^[1-9][0-9]*$ && "$restart_count" =~ ^[0-9]+$ && "$invocation_id" =~ ^[[:xdigit:]]{32}$ ]] \
        || return 1
    printf '%s|%s|%s\n' "$process_id" "$restart_count" "$invocation_id"
}

function load_environment_file {
    local raw_line key value

    [[ -r "$environment_file" && -f "$environment_file" && ! -L "$environment_file" ]] \
        || die "environment file is missing or unreadable: ${environment_file}"
    while IFS= read -r raw_line || [[ -n "$raw_line" ]]; do
        [[ -z "$raw_line" || "$raw_line" == \#* ]] && continue
        [[ "$raw_line" =~ ^([A-Z_][A-Z0-9_]*)=([^[:space:]#]+)$ ]] \
            || die "environment file has an unsupported line: ${raw_line}"
        key=${BASH_REMATCH[1]}
        value=${BASH_REMATCH[2]}
        case "$key" in
            OMI_COLLECTOR_DEVICE_ADDRESS) [[ -z "${device_address:-}" ]] || die "environment file repeats ${key}"; device_address=$value ;;
            OMI_COLLECTOR_DEVICE_SLUG) [[ -z "${device_slug:-}" ]] || die "environment file repeats ${key}"; device_slug=$value ;;
            OMI_COLLECTOR_LAYOUT_PATH) [[ -z "${layout_path:-}" ]] || die "environment file repeats ${key}"; layout_path=$value ;;
            OMI_COLLECTOR_PROJECT_DIR) [[ -z "${project_dir:-}" ]] || die "environment file repeats ${key}"; project_dir=$value ;;
            OMI_COLLECTOR_UV_BIN) [[ -z "${uv_bin:-}" ]] || die "environment file repeats ${key}"; uv_bin=$value ;;
            OMI_COLLECTOR_UV_PROJECT_ENVIRONMENT) [[ -z "${project_environment:-}" ]] || die "environment file repeats ${key}"; project_environment=$value ;;
            *) die "environment file has unsupported variable: ${key}" ;;
        esac
    done < "$environment_file"
}

function validate_environment {
    [[ -n "${device_address:-}" && "$device_address" =~ ^([[:xdigit:]]{2}:){5}[[:xdigit:]]{2}$ ]] \
        || die 'OMI_COLLECTOR_DEVICE_ADDRESS must be a Bluetooth address'
    [[ -n "${device_slug:-}" && "$device_slug" =~ ^[a-z0-9][a-z0-9-]*$ ]] \
        || die 'OMI_COLLECTOR_DEVICE_SLUG must contain lowercase letters, digits, and hyphens'
    [[ "${layout_path:-}" == /* && -f "$layout_path" && ! -L "$layout_path" ]] \
        || die 'OMI_COLLECTOR_LAYOUT_PATH must name a regular absolute file'
    [[ "${project_dir:-}" == /* && -d "$project_dir" && ! -L "$project_dir" ]] \
        || die 'OMI_COLLECTOR_PROJECT_DIR must be a regular absolute directory'
    [[ "${uv_bin:-}" == /* && -x "$uv_bin" && ! -L "$uv_bin" ]] \
        || die 'OMI_COLLECTOR_UV_BIN must name an executable absolute file'
    [[ "${project_environment:-}" == /* && ! -L "$project_environment" ]] \
        || die 'OMI_COLLECTOR_UV_PROJECT_ENVIRONMENT must be an absolute non-symlink path'
}

function prepare_service_environment {
    [[ "$uv_cache_dir" == "${state_dir}"/* && ! -L "$uv_cache_dir" ]] \
        || die "UV cache directory must be below ${state_dir}"
    [[ "$project_environment" == "${state_dir}"/* && ! -L "$project_environment" ]] \
        || die "OMI_COLLECTOR_UV_PROJECT_ENVIRONMENT must be below ${state_dir}"
    install -d -o "$account_user" -g "$account_group" -m 0750 -- "$uv_cache_dir" "$project_environment" \
        || die 'could not prepare service UV state directories'
    chown -R "$account_user:$account_group" -- "$uv_cache_dir" \
        || die "could not make UV cache writable by ${account_user}"
    chown -R "$account_user:$account_group" -- "$project_environment" \
        || die "could not make project environment writable by ${account_user}"
    [[ $(stat -c '%U:%G:%a' -- "$uv_cache_dir") == "${account_user}:${account_group}:750" ]] \
        || die "UV cache directory must be ${account_user}:${account_group} 0750: ${uv_cache_dir}"
    [[ $(stat -c '%U:%G:%a' -- "$project_environment") == "${account_user}:${account_group}:750" ]] \
        || die "project environment must be ${account_user}:${account_group} 0750: ${project_environment}"
}

declare script_dir repo_root source_package source_unit source_exec environment_file installed_unit installed_exec
declare service_name uv_cache_dir state_dir account_user account_group runuser_bin resolved_project_dir resolved_environment resolved_purelib package_dir compare_status resolver_output
declare initial_snapshot initial_invocation final_snapshot final_pid final_restarts expected_readiness journal_output
declare device_address device_slug layout_path project_dir uv_bin project_environment
declare -i attempt readiness_seen=0
declare -a resolver_paths
declare -ri readiness_poll_attempts=5 readiness_poll_interval_seconds=1 stability_interval_seconds=6

script_dir=$(builtin cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P) || die 'cannot resolve script directory'
repo_root=$(builtin cd -- "${script_dir}/.." && pwd -P) || die 'cannot resolve repository root'
source_unit="${repo_root}/systemd/omi-collector.service"
source_exec="${repo_root}/systemd/omi-collector-exec"
environment_file='/etc/omi-collector/omi-collector.env'
installed_unit='/etc/systemd/system/omi-collector.service'
installed_exec='/usr/local/libexec/omi-collector/omi-collector-exec'
service_name='omi-collector.service'
uv_cache_dir='/var/lib/omi-collector/uv-cache'
state_dir='/var/lib/omi-collector'
account_user='omi-collector'
account_group='omi-collector'

(( $# == 0 )) || die 'this command does not accept arguments'
(( EUID == 0 )) || die 'must run as root (use sudo)'
[[ -f "$source_unit" && -f "$source_exec" ]] || die 'checked-in systemd files are missing'
runuser_bin=$(command -v runuser || true)
[[ -n "$runuser_bin" || -x /usr/sbin/runuser ]] || die 'runuser is required to synchronize the service environment'
[[ -n "$runuser_bin" ]] || runuser_bin='/usr/sbin/runuser'
load_environment_file
validate_environment
resolved_project_dir=$(builtin cd -- "$project_dir" && pwd -P) || die "cannot resolve OMI_COLLECTOR_PROJECT_DIR: ${project_dir}"
[[ "$resolved_project_dir" == "$repo_root" ]] \
    || die 'OMI_COLLECTOR_PROJECT_DIR must match this checked-out repository for deployment'
source_package="${resolved_project_dir}/src/omi_collector"
[[ -d "$source_package" ]] || die "source package not found: ${source_package}"
prepare_service_environment

cd -- "$resolved_project_dir" || die "could not enter OMI_COLLECTOR_PROJECT_DIR: ${resolved_project_dir}"
if ! "$runuser_bin" --user "$account_user" -- env \
    UV_PROJECT_ENVIRONMENT="$project_environment" \
    UV_LINK_MODE=hardlink \
    UV_CACHE_DIR="$uv_cache_dir" \
    "$uv_bin" sync --locked --no-dev --no-editable --reinstall-package omi-collector --no-python-downloads --offline; then
    die 'uv sync failed; systemd was not touched'
fi

resolved_environment=$(builtin cd -- "$project_environment" && pwd -P) \
    || die "cannot resolve OMI_COLLECTOR_UV_PROJECT_ENVIRONMENT: ${project_environment}"
[[ -x "${resolved_environment}/bin/python" ]] || die "project environment Python not found: ${resolved_environment}/bin/python"
resolver_output=$("${resolved_environment}/bin/python" -I -B -c '
import importlib.util
from pathlib import Path
import sysconfig

purelib = Path(sysconfig.get_path("purelib")).resolve()
spec = importlib.util.find_spec("omi_collector")
if spec is None or spec.submodule_search_locations is None:
    raise SystemExit("omi_collector package is not importable")
print(purelib)
print(Path(next(iter(spec.submodule_search_locations))).resolve())
') || die 'could not resolve the project environment Python paths'
mapfile -t resolver_paths <<< "$resolver_output"
(( ${#resolver_paths[@]} == 2 )) || die 'project environment Python returned an invalid path resolution'
resolved_purelib=${resolver_paths[0]}
package_dir=${resolver_paths[1]}
[[ -d "$resolved_purelib" && "$resolved_purelib" == "${resolved_environment}"/* ]] \
    || die "resolved Python purelib is outside project environment: ${resolved_purelib}"
[[ -d "$package_dir" && "$package_dir" == "${resolved_purelib}"/* ]] \
    || die "resolved omi_collector package is outside Python purelib: ${package_dir}"
if diff --recursive --brief --exclude __pycache__ --exclude '*.pyc' --exclude '*.pyo' -- "$source_package" "$package_dir"; then
    :
else
    compare_status=$?
    (( compare_status == 1 )) && die 'installed omi_collector package is stale; systemd was not touched'
    die "could not compare omi_collector package trees (status ${compare_status})"
fi

[[ -r "$installed_unit" ]] || die 'installed systemd unit is missing or unreadable; run sudo scripts/install-systemd-unit.sh once'
cmp --silent -- "$source_unit" "$installed_unit" || die 'installed systemd unit differs; run sudo scripts/install-systemd-unit.sh once'
[[ -r "$installed_exec" ]] || die 'installed systemd wrapper is missing or unreadable; run sudo scripts/install-systemd-unit.sh once'
cmp --silent -- "$source_exec" "$installed_exec" || die 'installed systemd wrapper differs; run sudo scripts/install-systemd-unit.sh once'

systemctl restart "$service_name" || die "could not restart ${service_name}"
initial_snapshot=$(read_service_snapshot "$service_name") || die "${service_name} did not provide a valid active-process snapshot after deployment"
IFS='|' read -r _ _ initial_invocation <<< "$initial_snapshot"
expected_readiness=$(printf '{"layout":"%s","status":"deployment_ready"}' "$layout_path")
for (( attempt = 1; attempt <= readiness_poll_attempts; attempt++ )); do
    journal_output=$(journalctl --unit "$service_name" "_SYSTEMD_INVOCATION_ID=${initial_invocation}" --output cat --no-pager) \
        || die "could not read ${service_name} journal for readiness"
    if grep --fixed-strings --line-regexp -- "$expected_readiness" <<< "$journal_output" &> /dev/null; then
        readiness_seen=1
        break
    fi
    if (( attempt < readiness_poll_attempts )); then
        sleep "$readiness_poll_interval_seconds" || die 'could not wait for application readiness'
    fi
done
(( readiness_seen )) || die "${service_name} did not announce readiness for layout ${layout_path}"
sleep "$stability_interval_seconds" || die 'could not wait for the deployment stability interval'
final_snapshot=$(read_service_snapshot "$service_name") || die "${service_name} did not remain active during the stability interval"
[[ "$final_snapshot" == "$initial_snapshot" ]] || die "${service_name} restarted during the stability interval"
IFS='|' read -r final_pid final_restarts _ <<< "$final_snapshot"
printf 'Deployed and verified %s: pid=%s restarts=%s layout=%s.\n' "$service_name" "$final_pid" "$final_restarts" "$layout_path"
