#!/usr/bin/env bash

set -uo pipefail

function die {
    printf 'ERROR: %s\n' "${1:-operation failed}" >&2
    exit "${2:-1}"
}

function service_snapshot {
    local output
    output=$(systemctl show omi-collector.service --property=ActiveState --property=MainPID \
        --property=NRestarts --property=InvocationID) || return 1
    local active_state='' process_id='' restarts='' invocation_id='' line key value
    while IFS= read -r line || [[ -n "$line" ]]; do
        key=${line%%=*}
        value=${line#*=}
        case "$key" in
            ActiveState) active_state=$value ;;
            MainPID) process_id=$value ;;
            NRestarts) restarts=$value ;;
            InvocationID) invocation_id=$value ;;
        esac
    done <<< "$output"
    [[ "$active_state" == active && "$process_id" =~ ^[1-9][0-9]*$ && "$restarts" =~ ^[0-9]+$ \
        && "$invocation_id" =~ ^[[:xdigit:]]{32}$ ]] || return 1
    printf '%s|%s|%s\n' "$process_id" "$restarts" "$invocation_id"
}

function candidate_is_live {
    local output state='' process_id='' restarts='' invocation='' line key value
    output=$(systemctl show omi-collector.service --property=ActiveState --property=MainPID \
        --property=NRestarts --property=InvocationID) || return 1
    while IFS= read -r line || [[ -n "$line" ]]; do
        key=${line%%=*}; value=${line#*=}
        case "$key" in
            ActiveState) state=$value ;;
            MainPID) process_id=$value ;;
            NRestarts) restarts=$value ;;
            InvocationID) invocation=$value ;;
        esac
    done <<< "$output"
    [[ "$state" == activating || "$state" == active ]] || return 1
    [[ "$process_id" == "$observed_pid" && "$invocation" == "$observed_invocation" \
        && "$process_id" =~ ^[1-9][0-9]*$ && "$invocation" =~ ^[[:xdigit:]]{32}$ && "$restarts" == 0 ]]
}

function state_check {
    local -r environment="$1" stopped="$2"
    local -a args=()
    local output status
    [[ "$stopped" == true ]] && args+=(--stopped)
    if output=$("$environment/bin/omi-collector" check-state --config "$config_file" "${args[@]}"); then
        case "$output" in
            '{"schema_version":null,"status":"state_compatible"}'|\
            '{"schema_version":0,"status":"state_compatible"}'|\
            '{"schema_version":2,"status":"state_compatible"}'|\
            '{"schema_version":3,"status":"state_compatible"}'|\
            '{"schema_version":4,"status":"state_compatible"}') printf 'compatible\n'; return ;;
            *) die 'candidate returned an unexpected Collector state check record' ;;
        esac
    else
        status=$?
    fi
    if (( status == 1 )) && [[ "$output" == '{"reason":"canonical_hot_journal","status":"recovery_required"}' ]]; then
        printf 'recovery_required\n'
        return
    fi
    if [[ "$stopped" == false && $status == 1 \
        && "$output" == '{"detail":"canonical audio database is busy","fault":"transient","reason":"inspection_failed","status":"state_refused"}' ]]; then
        printf 'online_busy\n'
        return
    fi
    [[ -z "$output" ]] || printf 'Collector state check response (exit %s): %s\n' "$status" "$output" >&2
    die 'candidate refused the Collector state check'
}

function publish_selector {
    local -r target="$1" temporary="${current_link%/*}/.current.${BASHPID}.${RANDOM}"
    [[ ! -e "$temporary" && ! -L "$temporary" ]] || die 'temporary selector already exists'
    ln -s -- "$target" "$temporary" || die 'could not stage deployment selector'
    if ! mv -Tf -- "$temporary" "$current_link"; then
        rm -f -- "$temporary"
        die 'could not publish deployment selector'
    fi
    python3 -I -S - "$runtime_root" <<'PY' || die 'could not persist deployment selector'
import os, sys
fd = os.open(sys.argv[1], os.O_RDONLY | os.O_DIRECTORY)
try:
    os.fsync(fd)
finally:
    os.close(fd)
PY
}

function previous_is_compatible {
    local -r previous="$1"
    [[ -x "$previous/bin/python" ]] \
        && "$previous/bin/python" -I -B "${script_dir}/check_fallback_ledger_compatibility.py" "$state_file"
}

function finalize {
    local -i status=$? recovery_failed=0
    local selected=''
    trap - EXIT INT TERM HUP QUIT USR1 USR2
    if [[ -L "$current_link" ]]; then
        selected=$(readlink -- "$current_link") || selected='ambiguous'
    elif [[ -e "$current_link" ]]; then
        selected='ambiguous'
    fi
    if (( deployment_committed )); then
        :
    elif (( readiness_pending )) && [[ "$selected" == "releases/${release_name}" ]] && candidate_is_live; then
        printf 'ERROR: selected service is still starting; deployment remains unverified and in progress\n' >&2
        recovery_failed=1
    elif [[ "$selected" == "releases/${release_name}" || "$selected" == ambiguous ]]; then
        sudo -n /usr/bin/systemctl stop omi-collector.service || recovery_failed=1
        printf 'ERROR: selected release needs forward recovery; service remains stopped\n' >&2
        recovery_failed=1
    elif (( service_stopped && service_was_active )) && [[ -n "$previous_target" ]]; then
        if previous_is_compatible "${runtime_root}/${previous_target}"; then
            sudo -n /usr/bin/systemctl start --no-block omi-collector.service || recovery_failed=1
        else
            printf 'ERROR: previous release is not compatible with current state; service remains stopped\n' >&2
            recovery_failed=1
        fi
    fi
    if (( release_created && ! selector_published && ! deployment_committed && ! recovery_failed )); then
        rm -rf -- "$release_path" || printf 'ERROR: could not remove unpublished release: %s\n' "$release_path" >&2
    fi
    if [[ -n "$source_snapshot" ]]; then
        rm -rf -- "$source_snapshot" || { printf 'ERROR: could not remove source snapshot: %s\n' "$source_snapshot" >&2; (( status != 0 )) || status=1; }
    fi
    if (( status == 0 )); then
        printf 'Deployed and verified omi-collector.service: pid=%s restarts=%s config=%s.\n' \
            "$main_pid" "$restart_count" "$config_file"
    fi
    exit "$status"
}

function handle_signal {
    if (( restart_requested )) && [[ -n "$observed_pid" && -n "$observed_invocation" ]]; then
        readiness_pending=1
    fi
    die "deployment interrupted by $1" "$2"
}

declare script_dir='' repo_root='' runtime_root='' releases_dir='' current_link='' lock_file='' source_snapshot=''
declare config_file='/srv/pipelines/omi/config.toml' state_file='/srv/pipelines/omi/collector/collector-state.sqlite3'
declare source_revision='' release_name='' release_path='' previous_target=''
declare initial_snapshot='' final_snapshot='' snapshot=''
declare readiness_started_at='' active_state='' main_pid='' restart_count='' current_invocation='' job='' line='' key='' value=''
declare initial_state='' state_preflight='' candidate_config='' resolved_package='' operator_user='' operator_home=''
declare candidate='' deployment_epoch=''
declare -i service_was_active=0 service_stopped=0 selector_published=0 deployment_committed=0 release_created=0 readiness_pending=0 restart_requested=0
declare -i lock_fd=-1
declare observed_pid='' observed_invocation=''
declare -ri readiness_timeout_seconds=900 stability_seconds=6

(( EUID != 0 )) || die 'run as the service owner, without sudo'
(( $# == 0 )) || die 'this command does not accept arguments'
trap finalize EXIT
trap 'handle_signal SIGINT 130' INT
trap 'handle_signal SIGTERM 143' TERM
trap 'handle_signal SIGHUP 129' HUP
trap 'handle_signal SIGQUIT 131' QUIT
trap 'handle_signal SIGUSR1 138' USR1
trap 'handle_signal SIGUSR2 140' USR2
script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P) || die 'cannot resolve script directory'
repo_root=$(cd -- "${script_dir}/.." && pwd -P) || die 'cannot resolve repository root'
cd -- "$repo_root" || die 'cannot enter repository root'
source_revision=$(git -C "$repo_root" rev-parse --verify 'HEAD^{commit}') || die 'could not resolve source revision'
[[ "$source_revision" =~ ^[0-9a-f]{40,64}$ ]] || die 'source revision is not a full Git object ID'
source_status=$(git -C "$repo_root" status --porcelain=v1 --untracked-files=all) \
    || die 'could not inspect source checkout state'
[[ -z "$source_status" ]] || die 'refusing deployment from a dirty source tree'
source_snapshot=$(mktemp -d "${TMPDIR:-/tmp}/omi-collector-source.XXXXXX") || die 'could not create source snapshot directory'
git -C "$repo_root" archive --format=tar "$source_revision" | tar -xf - -C "$source_snapshot" \
    || die 'could not prepare frozen source snapshot'

operator_user=$(id --user --name) || die 'cannot identify service owner'
operator_home=$(getent passwd "$operator_user" | cut -d: -f6) || die 'cannot resolve service owner home'
[[ -n "$operator_home" && "$operator_home" == /* && -d "$operator_home" ]] || die 'service owner home is missing'
runtime_root="${operator_home}/.local/share/omi-collector"
releases_dir="${runtime_root}/releases"
current_link="${runtime_root}/current"
lock_file="${runtime_root}/.deployment.lock"
[[ -r "$config_file" && -f "$config_file" && ! -L "$config_file" ]] || die 'operator configuration is missing or unsafe'
[[ ! -L "${operator_home}/.local" && ! -L "${operator_home}/.local/share" && ! -L "$runtime_root" ]] \
    || die 'runtime path must not contain symlinks'
mkdir -p -- "$releases_dir" || die 'could not create operator-owned release directory'
[[ -d "$runtime_root" && -d "$releases_dir" && ! -L "$releases_dir" ]] || die 'runtime directories are unsafe'
exec {lock_fd}>>"$lock_file" || die 'could not open deployment lock'
flock --nonblock "$lock_fd" || die 'another deployment is already in progress'

previous_target=''
if [[ -e "$current_link" || -L "$current_link" ]]; then
    [[ -L "$current_link" ]] || die 'current deployment selector is not a symlink'
    previous_target=$(readlink -- "$current_link") || die 'could not read current selector'
    [[ "$previous_target" =~ ^releases/release-[0-9a-f]{40,64}-[0-9]+-[0-9]+$ \
        && -d "${runtime_root}/${previous_target}" && ! -L "${runtime_root}/${previous_target}" ]] \
        || die 'current deployment selector is unsafe'
fi
printf -v deployment_epoch '%(%s)T' -1
release_name="release-${source_revision}-${deployment_epoch}-$$"
release_path="${releases_dir}/${release_name}"
mkdir -m 0755 -- "$release_path" || die 'could not create candidate release directory'
release_created=1
UV_PROJECT_ENVIRONMENT="$release_path" UV_LINK_MODE=hardlink \
    uv sync --project "$source_snapshot" --locked --no-dev --no-editable --reinstall-package omi-collector \
    || die 'uv sync failed; the running service was not touched'
[[ -x "$release_path/bin/python" && -x "$release_path/bin/omi-collector" ]] || die 'candidate environment is incomplete'
resolved_package=$("$release_path/bin/python" -I -B -c 'import importlib.util; from pathlib import Path; spec=importlib.util.find_spec("omi_collector"); print(Path(next(iter(spec.submodule_search_locations))).resolve() if spec and spec.submodule_search_locations else "")') \
    || die 'candidate package could not be imported'
[[ "$resolved_package" == "$release_path"/* ]] || die 'candidate imports package outside its release environment'
diff --recursive --brief --exclude __pycache__ --exclude '*.pyc' --exclude '*.pyo' \
    "$source_snapshot/src/omi_collector" "$resolved_package" >/dev/null \
    || die 'installed candidate package differs from the frozen source revision'
mkdir -p -- "$release_path/share/omi-collector" || die 'could not create release metadata directory'
python3 -I -S - "$release_path/share/omi-collector/release.json" "$source_revision" <<'PYMETA' \
    || die 'could not write release source metadata'
import json, os, sys, tempfile
path, revision = sys.argv[1:]
fd, temporary = tempfile.mkstemp(dir=os.path.dirname(path), prefix=".release.")
try:
    with os.fdopen(fd, "w", encoding="utf-8") as stream:
        json.dump({"source_revision": revision}, stream, separators=(",", ":"))
        stream.write("\n")
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, path)
    directory = os.open(os.path.dirname(path), os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)
finally:
    if os.path.exists(temporary):
        os.unlink(temporary)
PYMETA
candidate_config=$("$release_path/bin/omi-collector" config check --config "$config_file") \
    || die 'candidate rejected production configuration'
[[ "$candidate_config" == "$(printf '{\"config\":\"%s\",\"status\":\"config_valid\"}' "$config_file")" ]] \
    || die 'candidate returned an unexpected configuration validation record'
state_preflight=$(state_check "$release_path" false) || die 'candidate refused Collector state'
if [[ "$state_preflight" == online_busy ]]; then
    : # A live capture may hold SQLite; repeat under the stopped-service guard.
fi
initial_state=$(systemctl show omi-collector.service --property=ActiveState) || die 'could not inspect service state'
case "$initial_state" in
    ActiveState=active) service_was_active=1 ;;
    ActiveState=inactive|ActiveState=failed) ;;
    *) die "ambiguous service state: ${initial_state}" ;;
esac
service_stopped=1
sudo -n /usr/bin/systemctl stop omi-collector.service || die 'could not stop service before state migration'
[[ ! -L "$state_file" ]] || die 'Collector database must not be a symlink'
state_preflight=$(state_check "$release_path" true) || die 'stopped Collector state check failed'
if [[ "$state_preflight" == recovery_required ]]; then
    "$release_path/bin/omi-collector" recover-state-journal --config "$config_file" \
        || die 'guarded SQLite journal recovery failed'
    [[ $(state_check "$release_path" true) == compatible ]] || die 'Collector state remains incompatible after journal recovery'
fi
publish_selector "releases/${release_name}"
selector_published=1
"${current_link}/bin/omi-collector" init-state --config "$config_file" || die 'state migration failed; service remains stopped'
sudo -n /usr/bin/systemctl restart --no-block omi-collector.service || die 'could not start selected service'
restart_requested=1
readiness_started_at=$SECONDS
while :; do
    snapshot=$(systemctl show omi-collector.service --property=ActiveState --property=MainPID \
        --property=NRestarts --property=InvocationID --property=Job) || die 'could not observe service readiness'
    active_state='' main_pid='' restart_count='' current_invocation='' job=''
    while IFS= read -r line || [[ -n "$line" ]]; do
        key=${line%%=*}; value=${line#*=}
        case "$key" in
            ActiveState) active_state=$value ;;
            MainPID) main_pid=$value ;;
            NRestarts) restart_count=$value ;;
            InvocationID) current_invocation=$value ;;
            Job) job=$value ;;
        esac
    done <<< "$snapshot"
    if [[ "$active_state" == activating || "$active_state" == active ]]; then
        [[ "$restart_count" == 0 ]] || die 'service restarted before readiness'
    fi
    if [[ "$main_pid" =~ ^[1-9][0-9]*$ ]]; then
        if [[ -n "$observed_pid" && "$main_pid" != "$observed_pid" ]]; then
            die 'service changed process before readiness'
        fi
        observed_pid=$main_pid
        if [[ "$current_invocation" =~ ^[[:xdigit:]]{32}$ ]]; then
            if [[ -n "$observed_invocation" && "$current_invocation" != "$observed_invocation" ]]; then
                die 'service changed invocation before readiness'
            fi
            observed_invocation=$current_invocation
        fi
    fi
    if [[ "$active_state" == active ]]; then
        [[ "$main_pid" =~ ^[1-9][0-9]*$ && "$restart_count" == 0 \
            && "$current_invocation" =~ ^[[:xdigit:]]{32}$ \
            && "$main_pid" == "$observed_pid" && "$current_invocation" == "$observed_invocation" ]] \
            || die 'active service has an invalid process snapshot'
        initial_snapshot="${main_pid}|${restart_count}|${current_invocation}"
        break
    fi
    if [[ "$active_state" == inactive || "$active_state" == failed ]]; then
        [[ -n "$job" && "$job" != 0 && -z "$observed_pid" ]] \
            || die "service became ${active_state} without a queued start job"
    elif [[ "$active_state" != activating ]]; then
        die "service entered ${active_state} before readiness"
    fi
    if (( SECONDS - readiness_started_at >= readiness_timeout_seconds )); then
        if [[ -n "$observed_pid" && "$observed_invocation" =~ ^[[:xdigit:]]{32}$ ]] && candidate_is_live; then
            readiness_pending=1
            die 'service is still activating; deployment remains unverified and in progress'
        fi
        die 'service readiness timed out without a confirmed live invocation'
    fi
    sleep 1 || die 'readiness poll wait failed'
done
sleep "$stability_seconds" || die 'service stability wait failed'
final_snapshot=$(service_snapshot) || die 'service did not remain active'
[[ "$final_snapshot" == "$initial_snapshot" ]] || die 'service restarted during stability interval'
IFS='|' read -r main_pid restart_count _ <<< "$final_snapshot"
deployment_committed=1
selector_published=0
for candidate in "$releases_dir"/release-*; do
    [[ -d "$candidate" && ! -L "$candidate" ]] || continue
    [[ "${candidate##*/}" =~ ^release-[0-9a-f]{40,64}-[0-9]+-[0-9]+$ ]] || continue
    [[ "$candidate" == "$release_path" ]] || rm -rf -- "$candidate" || die "could not remove previous release: $candidate"
done
