#!/usr/bin/env bash

set -uo pipefail

function die {
    local -r message="${1:-operation failed}"
    printf 'ERROR: %s\n' "$message" >&2
    exit "${2:-1}"
}

function handle_signal {
    local -r signal_name="$1"
    local -ri exit_status="$2"
    die "deployment interrupted by ${signal_name}" "$exit_status"
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
    [[ "$deployment_root" == /* && "$deployment_root" != "${state_dir}"/* && ! -L "$deployment_root" ]] \
        || die 'deployment root must be a dedicated absolute state path'
    [[ "$deployments_dir" == "${deployment_root}"/* && ! -L "$deployments_dir" ]] \
        || die "deployment directory must be below ${deployment_root}"
    install -d -o "$account_user" -g "$account_group" -m 0750 -- "$uv_cache_dir" \
        || die 'could not prepare service UV cache'
    install -d -o root -g root -m 0755 -- "$deployment_root" "$deployments_dir" \
        || die 'could not prepare root-owned deployment directories'
    chown -R "$account_user:$account_group" -- "$uv_cache_dir" \
        || die "could not make UV cache writable by ${account_user}"
    [[ $(stat -c '%U:%G:%a' -- "$uv_cache_dir") == "${account_user}:${account_group}:750" ]] \
        || die "UV cache directory must be ${account_user}:${account_group} 0750: ${uv_cache_dir}"
    [[ $(stat -c '%U:%G:%a' -- "$deployment_root") == 'root:root:755' ]] \
        || die "deployment root must be root:root 0755: ${deployment_root}"
    [[ $(stat -c '%U:%G:%a' -- "$deployments_dir") == 'root:root:755' ]] \
        || die "deployment directory must be root:root 0755: ${deployments_dir}"
}

function seal_deployment_environment {
    local -r environment="$1"

    chown -R root:root -- "$environment" \
        || die 'could not make completed deployment root-owned'
    find -P "$environment" -type d -exec chmod 0755 -- {} + \
        || die 'could not set completed deployment directory modes'
    find -P "$environment" -type f -exec chmod 0644 -- {} + \
        || die 'could not set completed deployment file modes'
    find -P "$environment/bin" -type f -exec chmod 0755 -- {} + \
        || die 'could not restore completed deployment executable modes'
    [[ $(stat -c '%U:%G:%a' -- "$environment") == 'root:root:755' ]] \
        || die "completed deployment must be root:root 0755: ${environment}"
}

function stage_deployment_environment {
    local metadata

    [[ "$source_revision" =~ ^[[:xdigit:]]{40,64}$ ]] \
        || die "source revision is not a full Git object ID: ${source_revision}"
    staged_deployment_file=$(mktemp --tmpdir="$deployment_root" .deployment.env.tmp.XXXXXX) \
        || die 'could not stage deployment provenance environment'
    printf 'OMI_COLLECTOR_SOURCE_REVISION=%s\nOMI_COLLECTOR_UV_PROJECT_ENVIRONMENT=%s\n' \
        "$source_revision" "$staged_environment" > "$staged_deployment_file" \
        || die 'could not write deployment provenance environment'
    chown root:"$account_group" -- "$staged_deployment_file" \
        || die 'could not set deployment provenance environment group'
    chmod 0640 -- "$staged_deployment_file" \
        || die 'could not set deployment provenance environment mode'
    metadata=$(stat -c '%U:%G:%a' -- "$staged_deployment_file") \
        || die 'could not inspect deployment provenance environment'
    [[ "$metadata" == "root:${account_group}:640" && -f "$staged_deployment_file" && ! -L "$staged_deployment_file" ]] \
        || die 'deployment provenance environment must be a regular root-owned 0640 file'
}

function backup_deployment_environment {
    if [[ ! -e "$deployment_environment_file" && ! -L "$deployment_environment_file" ]]; then
        previous_deployment_file=''
        return
    fi
    [[ -f "$deployment_environment_file" && ! -L "$deployment_environment_file" ]] \
        || die "deployment provenance is not a regular file: ${deployment_environment_file}"
    previous_deployment_file=$(mktemp --tmpdir="$deployment_root" .deployment.env.backup.XXXXXX) \
        || die 'could not back up deployment provenance environment'
    cp --preserve=mode,ownership,timestamps -- "$deployment_environment_file" "$previous_deployment_file" \
        || die 'could not back up deployment provenance environment'
}

function publish_deployment_environment {
    mv -f -- "$staged_deployment_file" "$deployment_environment_file" \
        || die 'could not publish deployment provenance environment'
    staged_deployment_file=''
    selection_published=1
}

function rollback_deployment {
    local -r failed_release="$staged_environment"
    local restored_environment

    if (( candidate_started )); then
        service_quiesced=1
        systemctl stop "$service_name" || return 1
        candidate_started=0
    fi
    if [[ -n "$previous_deployment_file" ]]; then
        restored_environment=$(mktemp --tmpdir="$deployment_root" .deployment.env.restore.XXXXXX) || return 1
        cp --preserve=mode,ownership,timestamps -- "$previous_deployment_file" "$restored_environment" || return 1
        mv -f -- "$restored_environment" "$deployment_environment_file" || return 1
    else
        rm -f -- "$deployment_environment_file" || return 1
    fi
    systemctl daemon-reload || return 1
    restore_legacy_device_state || return 1
    systemctl restart "$service_name" || return 1
    read_service_snapshot "$service_name" >/dev/null || return 1
    service_quiesced=0
    if [[ "$failed_release" == "${deployments_dir}"/release-* && -d "$failed_release" && ! -L "$failed_release" ]]; then
        rm -rf -- "$failed_release" || return 1
    fi
    printf 'Restored the previous verified deployment.\n' >&2
}

function cleanup_staged_environment {
    local -ri preserve_recovery="${1:-0}"

    if (( !preserve_recovery && !deployment_committed && !selection_published && release_created )); then
        if [[ "$release_path" == "$deployments_dir"/release-* && -d "$release_path" && ! -L "$release_path" ]]; then
            rm -rf -- "$release_path" || printf 'ERROR: could not remove unpublished deployment: %s\n' "$release_path" >&2
        fi
    fi
    if (( !preserve_recovery )); then
        [[ -n "$staged_deployment_file" && -f "$staged_deployment_file" ]] && rm -f -- "$staged_deployment_file"
        [[ -n "$previous_deployment_file" && -f "$previous_deployment_file" ]] && rm -f -- "$previous_deployment_file"
        [[ -n "$legacy_state_backup" && -f "$legacy_state_backup" ]] && rm -f -- "$legacy_state_backup"
    fi
    if (( service_quiesced )); then
        if systemctl start "$service_name"; then
            service_quiesced=0
        else
            printf 'ERROR: could not start %s after deployment failure\n' "$service_name" >&2
        fi
    fi
}

function finalize {
    local -i final_status=$?
    local -i rollback_failed=0

    trap '' INT TERM HUP QUIT USR1 USR2
    if (( deployment_committed )); then
        selection_published=0
        legacy_restore_required=0
    elif (( selection_published )); then
        if rollback_deployment; then
            selection_published=0
            legacy_restore_required=0
        else
            rollback_failed=1
            printf 'ERROR: could not restore the previous deployment; recovery artifacts were retained\n' >&2
        fi
    else
        if (( legacy_restore_required == 0 )); then
            :
        elif restore_legacy_device_state; then
            legacy_restore_required=0
        else
            rollback_failed=1
            printf 'ERROR: could not restore retired device state; recovery artifacts were retained\n' >&2
        fi
    fi
    cleanup_staged_environment "$rollback_failed"
    trap - EXIT
    exit "$final_status"
}

trap finalize EXIT
trap 'handle_signal SIGINT 130' INT
trap 'handle_signal SIGTERM 143' TERM
trap 'handle_signal SIGHUP 129' HUP
trap 'handle_signal SIGQUIT 131' QUIT
trap 'handle_signal SIGUSR1 138' USR1
trap 'handle_signal SIGUSR2 140' USR2

function retire_legacy_device_state {
    local result state_path state_device state_inode current_identity

    result=$(/usr/bin/python3 "$repo_root/scripts/inspect-legacy-device-state.py" "$layout_path" "$device_slug") \
        || die 'refusing deployment: device state is malformed, unknown, or unsafe'
    case "$result" in
        ABSENT) ;;
        CURRENT) ;;
        LEGACY$'\t'*)
            IFS=$'\t' read -r _ state_path state_device state_inode <<< "$result"
            [[ -n "$state_path" && "$state_device" =~ ^[0-9]+$ && "$state_inode" =~ ^[0-9]+$ ]] \
                || die 'refusing deployment: legacy device state identity is invalid'
            current_identity=$(stat -c '%d:%i' -- "$state_path") \
                || die "could not inspect legacy device state: ${state_path}"
            [[ "$current_identity" == "${state_device}:${state_inode}" ]] \
                || die "refusing deployment: legacy device state changed during validation: ${state_path}"
            legacy_state_device=$state_device
            legacy_state_inode=$state_inode
            legacy_state_path=$state_path
            legacy_state_backup=$(mktemp --tmpdir="$deployment_root" .device-state.backup.XXXXXX) \
                || die 'could not stage legacy device state rollback artifact'
            cp --preserve=timestamps -- "$state_path" "$legacy_state_backup" \
                || die 'could not preserve legacy device state rollback artifact'
            chown root:root -- "$legacy_state_backup" || die 'could not own legacy rollback artifact'
            chmod 0600 -- "$legacy_state_backup" || die 'could not protect legacy rollback artifact'
            current_identity=$(stat -c '%d:%i' -- "$state_path") \
                || die "could not recheck legacy device state: ${state_path}"
            [[ "$current_identity" == "${state_device}:${state_inode}" ]] \
                || die "refusing deployment: legacy device state changed during preservation: ${state_path}"
            legacy_restore_required=1
            rm -f -- "$state_path" || die "could not retire legacy device state: ${state_path}"
            printf 'Retired exact schema-1 device state: %s\n' "$state_path"
            ;;
        *) die 'refusing deployment: device state retirement returned an unknown result' ;;
    esac
}

function restore_legacy_device_state {
    local current_identity restored_state

    [[ -n "$legacy_state_backup" ]] || return 0
    [[ -n "$legacy_state_path" ]] || return 1
    if [[ -e "$legacy_state_path" || -L "$legacy_state_path" ]]; then
        current_identity=$(stat -c '%d:%i' -- "$legacy_state_path") || return 1
        [[ "$current_identity" != "${legacy_state_device}:${legacy_state_inode}" ]] || return 1
        restored_state=$(/usr/bin/python3 "$repo_root/scripts/inspect-legacy-device-state.py" "$layout_path" "$device_slug") \
            || return 1
        [[ "$restored_state" == CURRENT ]] || return 1
        rm -f -- "$legacy_state_path" || return 1
    fi
    cp --preserve=timestamps -- "$legacy_state_backup" "$legacy_state_path" || return 1
    chown "$account_user:$account_group" -- "$legacy_state_path" || return 1
    chmod 0640 -- "$legacy_state_path" || return 1
}

function prune_obsolete_deployments {
    local candidate name

    while IFS= read -r -d '' candidate; do
        name=$(basename -- "$candidate")
        [[ "$candidate" == "$staged_environment" ]] && continue
        [[ "$name" =~ ^release-[[:xdigit:]]{40,64}-[0-9]+-[0-9]+$ ]] || continue
        [[ -d "$candidate" && ! -L "$candidate" ]] || die "known deployment is not a regular directory: ${candidate}"
        rm -rf -- "$candidate" || die "could not prune obsolete deployment: ${candidate}"
    done < <(find "$deployments_dir" -mindepth 1 -maxdepth 1 -type d -print0)
}

function require_clean_source_tree {
    local source_tree_status

    source_tree_status=$(git -C "$resolved_project_dir" status --porcelain=v1 --untracked-files=all) \
        || die 'could not inspect the checked-out source tree'
    [[ -z "$source_tree_status" ]] \
        || die 'refusing deployment from a dirty source tree; commit, stash, or remove every tracked and untracked change first'
}

declare script_dir repo_root source_package source_unit source_exec environment_file deployment_environment_file installed_unit installed_exec
declare service_name uv_cache_dir state_dir deployment_root deployments_dir deployment_lock_file staged_environment staged_deployment_file previous_deployment_file release_path account_user account_group runuser_bin resolved_project_dir resolved_environment resolved_purelib package_dir compare_status resolver_output source_revision legacy_state_path legacy_state_backup legacy_state_device legacy_state_inode
declare initial_snapshot initial_invocation final_snapshot final_pid final_restarts expected_readiness journal_output
declare device_address device_slug layout_path project_dir uv_bin project_environment
declare -i attempt readiness_seen=0 selection_published=0 deployment_committed=0 legacy_restore_required=0 service_quiesced=0 candidate_started=0 release_created=0 deployment_lock_fd=-1
declare -a resolver_paths
declare -ri readiness_poll_attempts=5 readiness_poll_interval_seconds=1 stability_interval_seconds=6

script_dir=$(builtin cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P) || die 'cannot resolve script directory'
repo_root=$(builtin cd -- "${script_dir}/.." && pwd -P) || die 'cannot resolve repository root'
source_unit="${repo_root}/systemd/omi-collector.service"
source_exec="${repo_root}/systemd/omi-collector-exec"
environment_file='/etc/omi-collector/omi-collector.env'
deployment_environment_file='/var/lib/omi-collector-deployments/deployment.env'
installed_unit='/etc/systemd/system/omi-collector.service'
installed_exec='/usr/local/libexec/omi-collector/omi-collector-exec'
service_name='omi-collector.service'
uv_cache_dir='/var/lib/omi-collector/uv-cache'
state_dir='/var/lib/omi-collector'
deployment_root='/var/lib/omi-collector-deployments'
deployments_dir='/var/lib/omi-collector-deployments/releases'
deployment_lock_file='/var/lib/omi-collector-deployments/.deployment.lock'
account_user='omi-collector'
account_group='omi-collector'
staged_environment=''
staged_deployment_file=''
previous_deployment_file=''
legacy_state_path=''
legacy_state_backup=''
legacy_state_device=''
legacy_state_inode=''

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
source_revision=$(git -C "$resolved_project_dir" rev-parse --verify 'HEAD^{commit}') \
    || die 'could not resolve the checked-out source revision'
require_clean_source_tree
source_package="${resolved_project_dir}/src/omi_collector"
[[ -d "$source_package" ]] || die "source package not found: ${source_package}"

prepare_service_environment
exec {deployment_lock_fd}>>"$deployment_lock_file" \
    || die 'could not open the deployment transaction lock'
flock -n "$deployment_lock_fd" \
    || die 'another deployment transaction is already in progress'

service_quiesced=1
systemctl stop "$service_name" || die "could not stop ${service_name} before preparing deployment"

release_path="${deployments_dir}/release-${source_revision}-$(date +%s)-$$"
# Arm cleanup before creation so interruption cannot strand an unselected release.
release_created=1
mkdir --mode=0750 -- "$release_path" || die 'could not create deployment environment'
staged_environment="$release_path"
chown "$account_user:$account_group" -- "$staged_environment" \
    || die 'could not set staged deployment ownership'
chmod 0750 -- "$staged_environment" || die 'could not set staged deployment mode'

cd -- "$resolved_project_dir" || die "could not enter OMI_COLLECTOR_PROJECT_DIR: ${resolved_project_dir}"
if ! "$runuser_bin" --user "$account_user" -- env \
    UV_PROJECT_ENVIRONMENT="$staged_environment" \
    UV_LINK_MODE=copy \
    UV_CACHE_DIR="$uv_cache_dir" \
    "$uv_bin" sync --locked --no-dev --no-editable --reinstall-package omi-collector --no-python-downloads; then
    die 'uv sync failed; systemd was not touched'
fi
seal_deployment_environment "$staged_environment"

resolved_environment=$(builtin cd -- "$staged_environment" && pwd -P) \
    || die "cannot resolve OMI_COLLECTOR_UV_PROJECT_ENVIRONMENT: ${project_environment}"
[[ -x "${resolved_environment}/bin/python" ]] || die "staged project environment Python not found: ${resolved_environment}/bin/python"
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
') || die 'could not resolve the staged project environment Python paths'
mapfile -t resolver_paths <<< "$resolver_output"
(( ${#resolver_paths[@]} == 2 )) || die 'staged project environment Python returned an invalid path resolution'
resolved_purelib=${resolver_paths[0]}
package_dir=${resolver_paths[1]}
[[ -d "$resolved_purelib" && "$resolved_purelib" == "${resolved_environment}"/* ]] \
    || die "resolved Python purelib is outside staged project environment: ${resolved_purelib}"
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

retire_legacy_device_state
stage_deployment_environment
backup_deployment_environment
# Arm rollback before the rename so a signal cannot strand a newly selected target.
selection_published=1
publish_deployment_environment
systemctl daemon-reload || die 'could not reload systemd after selecting deployment'
candidate_started=1
systemctl restart "$service_name" || die "could not restart ${service_name}"
initial_snapshot=$(read_service_snapshot "$service_name") || die "${service_name} did not provide a valid active-process snapshot after deployment"
service_quiesced=0
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
# The new release is verified; clear rollback state before any fallible post-commit cleanup.
deployment_committed=1
selection_published=0
legacy_restore_required=0
rm -f -- "$previous_deployment_file" || die 'could not remove deployment rollback backup'
previous_deployment_file=''
if [[ -n "$legacy_state_backup" ]]; then
    rm -f -- "$legacy_state_backup" || printf 'ERROR: could not remove committed legacy device state artifact: %s\n' "$legacy_state_backup" >&2
    legacy_state_backup=''
fi
prune_obsolete_deployments
printf 'Deployed and verified %s: pid=%s restarts=%s layout=%s.\n' "$service_name" "$final_pid" "$final_restarts" "$layout_path"
