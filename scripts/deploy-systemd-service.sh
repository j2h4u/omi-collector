#!/usr/bin/env bash

set -uo pipefail

function die {
    local -r message="${1:-operation failed}"
    local -ri exit_status="${2:-1}"

    printf 'ERROR: %s\n' "$message"
    exit "$exit_status"
} 1>&2

function handle_signal {
    # args
    local -r signal_name="$1"
    local -ri exit_status="$2"

    # code
    die "deployment interrupted by ${signal_name}" "$exit_status"
}

function read_service_snapshot {
    # args
    local -r unit_name="$1"

    # vars
    local active_state process_id restart_count invocation_id output line key value

    # code
    output=$(systemctl show "$unit_name" --property=ActiveState --property=MainPID --property=NRestarts \
        --property=InvocationID) || return 1
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

    # result: one valid active-process snapshot
    [[ "$active_state" == active && "$process_id" =~ ^[1-9][0-9]*$ && "$restart_count" =~ ^[0-9]+$ \
        && "$invocation_id" =~ ^[[:xdigit:]]{32}$ ]] || return 1
    printf '%s|%s|%s\n' "$process_id" "$restart_count" "$invocation_id"
}

function validate_operator_config_file {
    # args
    local -r config_file="$1" account_group="$2"

    # vars
    local metadata

    # code
    # assert: deployment reads the same regular configuration as systemd
    [[ -r "$config_file" && -f "$config_file" && ! -L "$config_file" ]] \
        || die "operator configuration is missing or unreadable: ${config_file}"
    metadata=$(stat -c '%U:%G:%a' -- "$config_file") \
        || die "could not inspect operator configuration: ${config_file}"
    # assert: the system collector and user-owned pipeline can both read it
    [[ "$metadata" == "root:root:644" ]] \
        || die "operator configuration must be root:root 0644: ${config_file}"
}

function ensure_build_account {
    # args
    local -r build_user="$1" build_group="$2" build_state_dir="$3" service_user="$4" service_group="$5"

    # vars
    local primary_group build_uid build_gid service_uid service_gid supplementary_output group_id
    local -a build_groups

    # code
    # assert: candidate code has a dedicated unprivileged identity
    [[ "$build_user" != "$service_user" && "$build_group" != "$service_group" ]] \
        || die 'build identity names must differ from the live service identity'
    [[ ! -L "$build_state_dir" ]] || die "build state directory must not be a symlink: ${build_state_dir}"
    if ! getent group "$build_group" &> /dev/null; then
        groupadd --system "$build_group" || die "could not create build group ${build_group}"
    fi
    if ! getent passwd "$build_user" &> /dev/null; then
        useradd --system --gid "$build_group" --home-dir "$build_state_dir" --shell /usr/sbin/nologin --no-create-home \
            "$build_user" || die "could not create build user ${build_user}"
    fi
    primary_group=$(id -gn "$build_user") || die "could not determine primary group for ${build_user}"
    build_uid=$(id -u "$build_user") || die "could not determine UID for ${build_user}"
    build_gid=$(id -g "$build_user") || die "could not determine primary GID for ${build_user}"
    service_uid=$(id -u "$service_user") || die "could not determine UID for ${service_user}"
    service_gid=$(id -g "$service_user") || die "could not determine primary GID for ${service_user}"
    supplementary_output=$(id -G "$build_user") || die "could not determine supplementary groups for ${build_user}"
    read -r -a build_groups <<< "$supplementary_output"
    # assert: the build account cannot inherit the service account group
    [[ "$primary_group" == "$build_group" ]] \
        || die "build user ${build_user} must have primary group ${build_group}"
    [[ "$build_uid" =~ ^[0-9]+$ && "$build_gid" =~ ^[0-9]+$ && "$service_uid" =~ ^[0-9]+$ && "$service_gid" =~ ^[0-9]+$ ]] \
        || die 'build and service account identifiers must be numeric'
    [[ "$build_uid" != "$service_uid" ]] \
        || die 'build UID must differ from the live service UID'
    [[ "$build_gid" != "$service_gid" ]] \
        || die 'build primary GID must differ from the live service GID'
    (( ${#build_groups[@]} > 0 )) || die "build user ${build_user} has no initialized groups"
    for group_id in "${build_groups[@]}"; do
        [[ "$group_id" =~ ^[0-9]+$ ]] || die "build user ${build_user} has an invalid group identifier"
        [[ "$group_id" != "$service_gid" ]] \
            || die 'build user must not belong to the live service group'
    done
}

function prepare_deployment_directories {
    # args
    local -r build_state_dir="$1" uv_cache_dir="$2" deployment_root="$3" deployments_dir="$4"
    local -r python_install_dir="$5" build_user="$6" build_group="$7" output_name="$8"

    # code
    # assert: candidate cache and release storage have fixed, non-overlapping roles
    [[ "$build_state_dir" == /* && ! -L "$build_state_dir" ]] \
        || die 'build state directory must be a dedicated absolute state path'
    [[ "$uv_cache_dir" == "${build_state_dir}"/* && ! -L "$uv_cache_dir" ]] \
        || die "UV cache directory must be below ${build_state_dir}"
    [[ "$deployment_root" == /* && "$deployment_root" != "${build_state_dir}"/* && ! -L "$deployment_root" ]] \
        || die 'deployment root must be a dedicated absolute state path'
    [[ "$deployments_dir" == "${deployment_root}"/* && ! -L "$deployments_dir" ]] \
        || die "deployment directory must be below ${deployment_root}"
    [[ "$python_install_dir" == "${deployment_root}"/* && ! -L "$python_install_dir" ]] \
        || die "managed Python directory must be below ${deployment_root}"
    install -d -o root -g root -m 0755 -- "$build_state_dir" "$deployment_root" "$deployments_dir" \
        || die 'could not prepare root-owned deployment directories'
    if [[ ! -e "$uv_cache_dir" && ! -L "$uv_cache_dir" ]]; then
        install -d -o "$build_user" -g "$build_group" -m 0750 -- "$uv_cache_dir" \
            || die 'could not prepare build UV cache'
    fi
    if [[ ! -e "$python_install_dir" && ! -L "$python_install_dir" ]]; then
        install -d -o "$build_user" -g "$build_group" -m 0755 -- "$python_install_dir" \
            || die 'could not prepare managed Python installation directory'
        printf -v "$output_name" '%s' '1'
    else
        printf -v "$output_name" '%s' '0'
    fi
    # assert: the service cannot write the builder cache or release selectors
    [[ $(stat -c '%U:%G:%a' -- "$build_state_dir") == 'root:root:755' && ! -L "$build_state_dir" ]] \
        || die "build state directory must be root:root 0755: ${build_state_dir}"
    [[ $(stat -c '%U:%G:%a' -- "$uv_cache_dir") == "${build_user}:${build_group}:750" && ! -L "$uv_cache_dir" ]] \
        || die "UV cache directory must be ${build_user}:${build_group} 0750: ${uv_cache_dir}"
    [[ $(stat -c '%U:%G:%a' -- "$deployment_root") == 'root:root:755' ]] \
        || die "deployment root must be root:root 0755: ${deployment_root}"
    [[ $(stat -c '%U:%G:%a' -- "$deployments_dir") == 'root:root:755' ]] \
        || die "deployment directory must be root:root 0755: ${deployments_dir}"
}

function write_release_metadata {
    # args
    local -r environment="$1" deployments_dir="$2" source_revision="$3"

    # vars
    local metadata_dir metadata_file

    # code
    # assert: provenance is one full lowercase Git object ID
    [[ "$source_revision" =~ ^[0-9a-f]{40,64}$ ]] \
        || die "source revision is not a full lowercase Git object ID: ${source_revision}"
    # assert: root metadata is added only after a release is sealed at its fixed location
    [[ "$environment" == "${deployments_dir}"/release-* && -d "$environment" && ! -L "$environment" ]] \
        || die "completed deployment is missing or unsafe: ${environment}"
    [[ $(stat -c '%U:%G:%a' -- "$environment") == 'root:root:755' ]] \
        || die "completed deployment must be root:root 0755: ${environment}"
    metadata_dir="${environment}/share/omi-collector"
    metadata_file="${metadata_dir}/release.json"
    # assert: no build-controlled path can redirect root metadata writes
    [[ ! -e "${environment}/share" && ! -L "${environment}/share" ]] \
        || die 'candidate created an unsafe release metadata path'
    install -d -o root -g root -m 0755 -- "$metadata_dir" || die 'could not create release metadata directory'
    [[ -d "$metadata_dir" && ! -L "$metadata_dir" && $(stat -c '%U:%G:%a' -- "$metadata_dir") == 'root:root:755' ]] \
        || die 'release metadata directory is unsafe after creation'
    printf '{"source_revision":"%s"}\n' "$source_revision" > "$metadata_file" \
        || die 'could not write release provenance'
    # assert: the just-created metadata file cannot redirect the root mode update
    [[ -f "$metadata_file" && ! -L "$metadata_file" ]] \
        || die 'release metadata file is unsafe after creation'
    chmod 0644 "$metadata_file" || die 'could not set release provenance mode'
}

function seal_tree_with_descriptors {
    # args
    local -r sealer_python="$1" sealer_script="$2" tree_root="$3" owner="$4" allowed_external="$5"

    # vars
    local -a command

    # code
    # assert: the fixed interpreter cannot import a builder-controlled site package
    command=("$sealer_python" '-I' '-B' '-S' "$sealer_script" '--root' "$tree_root" '--owner' "$owner")
    if [[ -n "$allowed_external" ]]; then
        command+=('--allow-external' "$allowed_external")
    fi
    "${command[@]}" || die "could not seal completed deployment tree: ${tree_root}"
}

function seal_deployment_environment {
    # args
    local -r environment="$1" deployments_dir="$2" sealer_python="$3" sealer_script="$4" sealer_owner="$5"
    local -r python_install_dir="$6"

    # code
    # assert: root seals exactly one builder-created release, without following its links
    [[ "$environment" == "${deployments_dir}"/release-* && -d "$environment" && ! -L "$environment" ]] \
        || die "staged deployment is missing or unsafe: ${environment}"
    seal_tree_with_descriptors "$sealer_python" "$sealer_script" "$environment" "$sealer_owner" "$python_install_dir"
    # assert: selected releases cannot be modified by the service account
    [[ $(stat -c '%U:%G:%a' -- "$environment") == 'root:root:755' ]] \
        || die "completed deployment must be root:root 0755: ${environment}"
}

function seal_managed_python {
    # args
    local -r python_install_dir="$1" deployment_root="$2" sealer_python="$3" sealer_script="$4" sealer_owner="$5"

    # code
    # assert: root seals only the fixed build-owned Python installation location
    [[ "$python_install_dir" == "${deployment_root}"/* && -d "$python_install_dir" && ! -L "$python_install_dir" ]] \
        || die "managed Python installation is missing or unsafe: ${python_install_dir}"
    seal_tree_with_descriptors "$sealer_python" "$sealer_script" "$python_install_dir" "$sealer_owner" ''
    # assert: later candidates can read but cannot replace the runtime interpreter
    [[ $(stat -c '%U:%G:%a' -- "$python_install_dir") == 'root:root:755' ]] \
        || die "managed Python installation must be root:root 0755: ${python_install_dir}"
}

function verify_installed_package {
    # args
    local -r runuser_bin="$1" build_user="$2" build_group="$3" build_home="$4" environment="$5" source_package="$6"

    # vars
    local resolver_output resolved_purelib package_dir compare_status
    local -a resolver_paths

    # code
    # assert: the staged environment has its own interpreter and entry point
    [[ -x "${environment}/bin/python" && -x "${environment}/bin/omi-collector" ]] \
        || die "staged environment is incomplete: ${environment}"
    resolver_output=$("$runuser_bin" --user "$build_user" --group "$build_group" -- env HOME="$build_home" \
        "${environment}/bin/python" -I -B -c '
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
    # assert: imports resolve exclusively inside the staged release
    (( ${#resolver_paths[@]} == 2 )) || die 'staged project environment returned invalid path resolution'
    resolved_purelib=${resolver_paths[0]}
    package_dir=${resolver_paths[1]}
    [[ -d "$resolved_purelib" && "$resolved_purelib" == "${environment}"/* ]] \
        || die "resolved Python purelib is outside staged environment: ${resolved_purelib}"
    [[ -d "$package_dir" && "$package_dir" == "${resolved_purelib}"/* ]] \
        || die "resolved omi_collector package is outside Python purelib: ${package_dir}"
    if diff --recursive --brief --exclude __pycache__ --exclude '*.pyc' --exclude '*.pyo' -- \
        "$source_package" "$package_dir"; then
        return
    fi
    compare_status=$?
    (( compare_status == 1 )) && die 'installed omi_collector package is stale; systemd was not touched'
    die "could not compare omi_collector package trees (status ${compare_status})"
}

function validate_candidate_config {
    # args
    local -r runuser_bin="$1" build_user="$2" build_group="$3" build_home="$4" environment="$5" config_file="$6"

    # vars
    local output expected

    # code
    output=$("$runuser_bin" --user "$build_user" --group "$build_group" -- env HOME="$build_home" "${environment}/bin/omi-collector" \
        config check --config "$config_file") || die 'candidate rejected the operator configuration'
    expected=$(printf '{"config":"%s","status":"config_valid"}' "$config_file")
    # assert: the candidate validated the exact production configuration
    [[ "$output" == "$expected" ]] || die 'candidate returned an unexpected configuration validation record'
}

function read_current_target {
    # args
    local -r current_link="$1" deployments_dir="$2" output_name="$3"

    # vars
    local target resolved

    # code
    if [[ ! -e "$current_link" && ! -L "$current_link" ]]; then
        printf -v "$output_name" '%s' ''
        return
    fi
    # assert: release selection is one relative symlink managed by this deployer
    [[ -L "$current_link" ]] || die "current deployment selector is not a symlink: ${current_link}"
    target=$(readlink -- "$current_link") || die 'could not read current deployment selector'
    [[ "$target" =~ ^releases/release-[0-9a-f]{40,64}-[0-9]+-[0-9]+$ ]] \
        || die "current deployment selector has an unsupported target: ${target}"
    resolved="$(dirname -- "$current_link")/${target}"
    [[ -d "$resolved" && ! -L "$resolved" && "$resolved" == "${deployments_dir}"/release-* ]] \
        || die "current deployment target is missing or unsafe: ${resolved}"
    printf -v "$output_name" '%s' "$target"
}

function publish_current_target {
    # args
    local -r current_link="$1" target="$2" temporary_link="$3"

    # code
    # assert: a deployment transaction owns an unused temporary selector
    [[ ! -e "$temporary_link" && ! -L "$temporary_link" ]] \
        || die "temporary deployment selector already exists: ${temporary_link}"
    ln --symbolic -- "$target" "$temporary_link" || die 'could not stage current deployment selector'
    mv -T -- "$temporary_link" "$current_link" || die 'could not publish current deployment selector'
}

function rollback_deployment {
    # args
    local -r service_name="$1" current_link="$2" previous_target="$3" temporary_link="$4"
    local -r failed_release="$5" deployments_dir="$6"

    # code
    systemctl stop "$service_name" || return 1
    if [[ -n "$previous_target" ]]; then
        publish_current_target "$current_link" "$previous_target" "$temporary_link" || return 1
        systemctl restart "$service_name" || return 1
        read_service_snapshot "$service_name" &> /dev/null || return 1
    else
        rm -f -- "$current_link" || return 1
    fi
    service_quiesced=0
    selection_published=0
    if [[ "$failed_release" == "${deployments_dir}"/release-* && -d "$failed_release" && ! -L "$failed_release" ]]; then
        rm -rf -- "$failed_release" || return 1
        release_created=0
    fi
    printf 'Restored the prior deployment state.\n' >&2
}

function prune_obsolete_deployments {
    # args
    local -r deployments_dir="$1" selected_environment="$2"

    # vars
    local candidate name

    # code
    while IFS= read -r -d '' candidate; do
        name=$(basename -- "$candidate")
        [[ "$candidate" == "$selected_environment" ]] && continue
        [[ "$name" =~ ^release-[0-9a-f]{40,64}-[0-9]+-[0-9]+$ ]] || continue
        # assert: only known regular release directories are pruned
        [[ -d "$candidate" && ! -L "$candidate" ]] \
            || die "known deployment is not a regular directory: ${candidate}"
        rm -rf -- "$candidate" || die "could not prune obsolete deployment: ${candidate}"
    done < <(find "$deployments_dir" -mindepth 1 -maxdepth 1 -type d -print0)
}

function require_clean_source_tree {
    # args
    local -r project_dir="$1"

    # vars
    local source_tree_status

    # code
    source_tree_status=$(git -C "$project_dir" status --porcelain=v1 --untracked-files=all) \
        || die 'could not inspect the checked-out source tree'
    # assert: deployment exactly matches one committed source revision
    [[ -z "$source_tree_status" ]] \
        || die 'refusing deployment from a dirty source tree; commit, stash, or remove every change first'
}

declare script_dir repo_root source_package source_unit installed_unit config_file service_name
declare uv_bin runuser_bin sealer_python sealer_script sealer_owner account_user account_group build_user build_group
declare build_state_dir uv_cache_dir python_install_dir
declare deployment_root deployments_dir deployment_lock_file current_link temporary_link
declare source_revision release_name release_path staged_environment previous_target deployment_epoch
declare initial_snapshot initial_invocation final_snapshot final_pid final_restarts expected_readiness journal_output
declare -i attempt readiness_seen=0 selection_published=0 deployment_committed=0 python_install_pending=0
declare -i service_quiesced=0 release_created=0 deployment_lock_fd=-1
declare -ri readiness_poll_attempts=5 readiness_poll_interval_seconds=1 stability_interval_seconds=6

script_dir=$(builtin cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P) || die 'cannot resolve script directory'
repo_root=$(builtin cd -- "${script_dir}/.." && pwd -P) || die 'cannot resolve repository root'
source_package="${repo_root}/src/omi_collector"
source_unit="${repo_root}/systemd/omi-collector.service"
installed_unit='/etc/systemd/system/omi-collector.service'
config_file='/srv/pipelines/omi/config.toml'
service_name='omi-collector.service'
uv_bin='/usr/local/bin/uv'
sealer_python='/usr/bin/python3'
sealer_script="${script_dir}/seal_deployment_tree.py"
sealer_owner='0:0'
account_user='omi-collector'
account_group='omi-collector'
build_user='omi-collector-build'
build_group='omi-collector-build'
build_state_dir='/var/lib/omi-collector-build'
uv_cache_dir='/var/lib/omi-collector-build/uv-cache'
deployment_root='/var/lib/omi-collector-deployments'
deployments_dir='/var/lib/omi-collector-deployments/releases'
python_install_dir='/var/lib/omi-collector-deployments/python'
deployment_lock_file='/var/lib/omi-collector-deployments/.deployment.lock'
current_link='/var/lib/omi-collector-deployments/current'
temporary_link="${deployment_root}/.current.${BASHPID}.${RANDOM}"
release_path=''
staged_environment=''
previous_target=''

function cleanup {
    # args
    local -ri preserve_recovery="${1:-0}"

    # code
    if (( ! preserve_recovery )); then
        [[ -e "$temporary_link" || -L "$temporary_link" ]] && rm -f -- "$temporary_link"
        if (( release_created && ! deployment_committed && ! selection_published )); then
            if [[ "$release_path" == "${deployments_dir}"/release-* && -d "$release_path" && ! -L "$release_path" ]]; then
                rm -rf -- "$release_path" || printf 'ERROR: could not remove unpublished deployment: %s\n' "$release_path" >&2
            fi
        fi
    fi
}

function finalize {
    # vars
    local -i final_status=$? rollback_failed=0

    # code
    trap '' INT TERM HUP QUIT USR1 USR2
    if (( deployment_committed )); then
        selection_published=0
    elif (( selection_published )); then
        if ! rollback_deployment "$service_name" "$current_link" "$previous_target" "$temporary_link" \
            "$staged_environment" "$deployments_dir"; then
            rollback_failed=1
            printf 'ERROR: could not restore the previous deployment; recovery artifacts were retained\n' >&2
        fi
    elif (( service_quiesced )) && [[ -n "$previous_target" ]]; then
        if systemctl start "$service_name"; then
            service_quiesced=0
        else
            rollback_failed=1
            printf 'ERROR: could not restart the previous deployment; recovery artifacts were retained\n' >&2
        fi
    fi
    cleanup "$rollback_failed"
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

# assert: deployment has fixed privileged inputs and no operator overrides
(( $# == 0 )) || die 'this command does not accept arguments'
(( EUID == 0 )) || die 'must run as root (use sudo)'
[[ -f "$source_unit" && -d "$source_package" ]] || die 'checked-in deployment sources are missing'
[[ -f "$uv_bin" && -x "$uv_bin" && ! -L "$uv_bin" ]] \
    || die "uv must be installed as an executable regular file: ${uv_bin}"
[[ -x "$sealer_python" && -f "$sealer_script" && ! -L "$sealer_script" ]] \
    || die 'trusted descriptor-based deployment sealer is missing or unsafe'
[[ -r "$installed_unit" && -f "$installed_unit" && ! -L "$installed_unit" ]] \
    || die 'installed systemd unit is missing or unsafe; run sudo scripts/install-systemd-unit.sh once'
cmp --silent -- "$source_unit" "$installed_unit" \
    || die 'installed systemd unit differs; run sudo scripts/install-systemd-unit.sh once'
runuser_bin=$(command -v runuser || true)
[[ -n "$runuser_bin" || -x /usr/sbin/runuser ]] || die 'runuser is required to synchronize the service environment'
[[ -n "$runuser_bin" ]] || runuser_bin='/usr/sbin/runuser'

validate_operator_config_file "$config_file" "$account_group"
source_revision=$(git -C "$repo_root" rev-parse --verify 'HEAD^{commit}') \
    || die 'could not resolve the checked-out source revision'
[[ "$source_revision" =~ ^[0-9a-f]{40,64}$ ]] \
    || die "source revision is not a full lowercase Git object ID: ${source_revision}"
require_clean_source_tree "$repo_root"
ensure_build_account "$build_user" "$build_group" "$build_state_dir" "$account_user" "$account_group"
prepare_deployment_directories "$build_state_dir" "$uv_cache_dir" "$deployment_root" "$deployments_dir" \
    "$python_install_dir" "$build_user" "$build_group" python_install_pending
exec {deployment_lock_fd}>>"$deployment_lock_file" || die 'could not open the deployment transaction lock'
flock --nonblock "$deployment_lock_fd" || die 'another deployment transaction is already in progress'
read_current_target "$current_link" "$deployments_dir" previous_target

printf -v deployment_epoch '%(%s)T' -1
release_name="release-${source_revision}-${deployment_epoch}-$$"
release_path="${deployments_dir}/${release_name}"
mkdir --mode=0750 -- "$release_path" || die 'could not create deployment environment'
release_created=1
staged_environment="$release_path"
chown "$build_user:$build_group" "$staged_environment" \
    || die 'could not set staged deployment ownership'
chmod 0750 "$staged_environment" || die 'could not set staged deployment mode'

if (( python_install_pending )); then
    if ! "$runuser_bin" --user "$build_user" --group "$build_group" -- env HOME="$uv_cache_dir" \
        UV_CACHE_DIR="$uv_cache_dir" \
        UV_PYTHON_INSTALL_DIR="$python_install_dir" \
        "$uv_bin" python install 3.14; then
        die 'could not install the managed build Python'
    fi
fi
if ! "$runuser_bin" --user "$build_user" --group "$build_group" -- env HOME="$uv_cache_dir" \
    UV_PROJECT_ENVIRONMENT="$staged_environment" \
    UV_LINK_MODE=copy \
    UV_CACHE_DIR="$uv_cache_dir" \
    UV_PYTHON_INSTALL_DIR="$python_install_dir" \
    "$uv_bin" sync --project "$repo_root" --locked --no-dev --no-editable --reinstall-package omi-collector \
        --no-python-downloads; then
    die 'uv sync failed; systemd was not touched'
fi
verify_installed_package "$runuser_bin" "$build_user" "$build_group" "$uv_cache_dir" "$staged_environment" "$source_package"
validate_candidate_config "$runuser_bin" "$build_user" "$build_group" "$uv_cache_dir" "$staged_environment" "$config_file"
seal_managed_python "$python_install_dir" "$deployment_root" "$sealer_python" "$sealer_script" "$sealer_owner"
seal_deployment_environment "$staged_environment" "$deployments_dir" "$sealer_python" "$sealer_script" "$sealer_owner" \
    "$python_install_dir"
write_release_metadata "$staged_environment" "$deployments_dir" "$source_revision"

service_quiesced=1
systemctl stop "$service_name" || die "could not stop ${service_name} before selecting deployment"
selection_published=1
publish_current_target "$current_link" "releases/${release_name}" "$temporary_link"
systemctl restart "$service_name" || die "could not restart ${service_name}"
initial_snapshot=$(read_service_snapshot "$service_name") \
    || die "${service_name} did not provide a valid active-process snapshot after deployment"
service_quiesced=0
IFS='|' read -r _ _ initial_invocation <<< "$initial_snapshot"
expected_readiness=$(printf '{"config":"%s","status":"deployment_ready"}' "$config_file")
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
(( readiness_seen )) || die "${service_name} did not announce readiness for ${config_file}"
sleep "$stability_interval_seconds" || die 'could not wait for the deployment stability interval'
final_snapshot=$(read_service_snapshot "$service_name") \
    || die "${service_name} did not remain active during the stability interval"
[[ "$final_snapshot" == "$initial_snapshot" ]] || die "${service_name} restarted during the stability interval"
IFS='|' read -r final_pid final_restarts _ <<< "$final_snapshot"

# The new release is verified; failures from this point must not roll it back.
deployment_committed=1
selection_published=0
prune_obsolete_deployments "$deployments_dir" "$staged_environment"
printf 'Deployed and verified %s: pid=%s restarts=%s config=%s.\n' \
    "$service_name" "$final_pid" "$final_restarts" "$config_file"
