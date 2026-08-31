#!/usr/bin/env bash

set -uo pipefail

function die {
    printf 'ERROR: %s\n' "${1:-operation failed}" >&2
    exit "${2:-1}"
}

function usage {
    printf 'Usage: %s [--restart]\n' "$(basename -- "$0")"
    printf 'Install and enable omi-collector.service; restart only with --restart.\n'
}

function validate_environment_file {
    local raw_line key value
    declare -gA environment_values=()

    [[ -r "$environment_file" && -f "$environment_file" && ! -L "$environment_file" ]] \
        || die "environment file is missing or unsafe: ${environment_file}"
    while IFS= read -r raw_line || [[ -n "$raw_line" ]]; do
        [[ -z "$raw_line" || "$raw_line" == \#* ]] && continue
        [[ "$raw_line" =~ ^([A-Z_][A-Z0-9_]*)=([^[:space:]#]+)$ ]] \
            || die "environment file has an unsupported line: ${raw_line}"
        key=${BASH_REMATCH[1]}
        value=${BASH_REMATCH[2]}
        case "$key" in
            OMI_COLLECTOR_DEVICE_ADDRESS|OMI_COLLECTOR_DEVICE_SLUG|OMI_COLLECTOR_LAYOUT_PATH|OMI_COLLECTOR_PROJECT_DIR|OMI_COLLECTOR_UV_BIN|OMI_COLLECTOR_UV_PROJECT_ENVIRONMENT)
                [[ -z "${environment_values[$key]:-}" ]] || die "environment file repeats ${key}"
                environment_values["$key"]=$value
                ;;
            *) die "environment file has unsupported variable: ${key}" ;;
        esac
    done < "$environment_file"
    for key in OMI_COLLECTOR_DEVICE_ADDRESS OMI_COLLECTOR_DEVICE_SLUG OMI_COLLECTOR_LAYOUT_PATH OMI_COLLECTOR_PROJECT_DIR OMI_COLLECTOR_UV_BIN OMI_COLLECTOR_UV_PROJECT_ENVIRONMENT; do
        [[ -n "${environment_values[$key]:-}" ]] || die "environment file is missing ${key}"
    done
    [[ "${environment_values[OMI_COLLECTOR_DEVICE_ADDRESS]}" != 'AA:BB:CC:DD:EE:FF' ]] \
        || die 'environment file has the example device address'
    [[ "${environment_values[OMI_COLLECTOR_DEVICE_SLUG]}" != 'replace-me' ]] \
        || die 'environment file has the example device slug'
}

function ensure_service_account {
    local primary_group

    [[ ! -L "$state_dir" ]] || die "state directory must not be a symlink: ${state_dir}"
    if ! getent group "$account_group" >/dev/null; then
        groupadd --system "$account_group" || die "could not create system group ${account_group}"
    fi
    if ! getent passwd "$account_user" >/dev/null; then
        useradd --system --gid "$account_group" --home-dir "$state_dir" --shell /usr/sbin/nologin --no-create-home "$account_user" \
            || die "could not create system user ${account_user}"
    fi
    primary_group=$(id -gn "$account_user") || die "could not determine primary group for ${account_user}"
    [[ "$primary_group" == "$account_group" ]] \
        || die "system user ${account_user} must have primary group ${account_group}"
    install -d -o "$account_user" -g "$account_group" -m 0750 -- "$state_dir" \
        || die "could not provision state directory ${state_dir}"
    [[ $(stat -c '%U:%G:%a' -- "$state_dir") == "${account_user}:${account_group}:750" && ! -L "$state_dir" ]] \
        || die "state directory must be ${account_user}:${account_group} 0750: ${state_dir}"
}

function validate_layout_file {
    local -r layout_file="${environment_values[OMI_COLLECTOR_LAYOUT_PATH]}"

    [[ "$layout_file" == /* && -f "$layout_file" && ! -L "$layout_file" ]] \
        || die 'OMI_COLLECTOR_LAYOUT_PATH must name a regular absolute file'
    chown root:"$account_group" -- "$layout_file" || die "could not set layout file group: ${layout_file}"
    chmod 0640 -- "$layout_file" || die "could not set layout file mode: ${layout_file}"
    [[ $(stat -c '%U:%G:%a' -- "$layout_file") == "root:${account_group}:640" ]] \
        || die "layout file must be root:${account_group} 0640: ${layout_file}"
}

function stage_file {
    local -r source="$1"
    local -r target="$2"
    local -r mode="$3"
    local target_dir metadata

    target_dir=$(dirname -- "$target") || die "could not resolve target directory for ${target}"
    install -d -o root -g root -m 0755 -- "$target_dir" || die "could not create ${target_dir}"
    staged_file=$(mktemp --tmpdir="$target_dir" ".$(basename -- "$target").tmp.XXXXXX") \
        || die "could not create temporary file beside ${target}"
    if ! install -o root -g root -m "$mode" -- "$source" "$staged_file"; then
        rm -f -- "$staged_file" || true
        die "could not stage ${target}"
    fi
    metadata=$(stat -c '%u:%g:%a' -- "$staged_file") || die "could not inspect staged file: ${staged_file}"
    [[ "$metadata" == "0:0:${mode#0}" && -f "$staged_file" && ! -L "$staged_file" ]] \
        || die "staged file metadata is not root:root ${mode} regular file: ${staged_file}"
}

function backup_target {
    local -r target="$1"
    local -r variable_name="$2"
    local target_dir backup

    if [[ ! -e "$target" && ! -L "$target" ]]; then
        printf -v "$variable_name" '%s' ''
        return
    fi
    [[ -f "$target" && ! -L "$target" ]] || die "existing target is not a regular file: ${target}"
    target_dir=$(dirname -- "$target") || die "could not resolve target directory for ${target}"
    backup=$(mktemp --tmpdir="$target_dir" ".$(basename -- "$target").backup.XXXXXX") \
        || die "could not create backup beside ${target}"
    cp --preserve=mode,ownership,timestamps -- "$target" "$backup" || die "could not back up ${target}"
    printf -v "$variable_name" '%s' "$backup"
}

function restore_target {
    local -r target="$1"
    local -r backup="$2"

    if [[ -n "$backup" ]]; then
        mv -f -- "$backup" "$target" || die "could not restore ${target}"
    else
        rm -f -- "$target" || die "could not remove incomplete ${target}"
    fi
}

function rollback_pair {
    restore_target "$unit_target" "$unit_backup"
    restore_target "$exec_target" "$exec_backup"
    systemctl daemon-reload || die 'systemd daemon-reload failed while rolling back the pair'
}

function validate_staged_pair {
    validation_root=$(mktemp -d) || die 'could not create staged validation root'
    install -d -o root -g root -m 0755 -- \
        "${validation_root}/etc/systemd/system" \
        "${validation_root}/usr/local/libexec/omi-collector" \
        "${validation_root}/etc/omi-collector" || die 'could not prepare staged validation root'
    install -o root -g root -m 0644 -- "$staged_unit" "${validation_root}${unit_target}" \
        || die 'could not stage the unit for validation'
    install -o root -g root -m 0755 -- "$staged_exec" "${validation_root}${exec_target}" \
        || die 'could not stage the wrapper for validation'
    install -o root -g root -m 0640 -- "$environment_file" "${validation_root}${environment_file}" \
        || die 'could not stage the environment for validation'
    systemd-analyze verify --root="$validation_root" "$unit_target" \
        || die "systemd unit validation failed: ${source_unit}"
}

declare script_dir repo_root source_unit source_exec staged_file staged_unit staged_exec validation_root
declare unit_backup exec_backup account_user account_group state_dir environment_file unit_target exec_target service_name
declare -A environment_values
script_dir=$(builtin cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P) || die 'cannot resolve installer directory'
repo_root=$(builtin cd -- "${script_dir}/.." && pwd -P) || die 'cannot resolve repository root'
source_unit="${repo_root}/systemd/omi-collector.service"
source_exec="${repo_root}/systemd/omi-collector-exec"
environment_file='/etc/omi-collector/omi-collector.env'
unit_target='/etc/systemd/system/omi-collector.service'
exec_target='/usr/local/libexec/omi-collector/omi-collector-exec'
account_user='omi-collector'
account_group='omi-collector'
state_dir='/var/lib/omi-collector'
service_name='omi-collector.service'
staged_file=''
staged_unit=''
staged_exec=''
unit_backup=''
exec_backup=''
validation_root=''

function cleanup {
    for staged_file in "$staged_unit" "$staged_exec" "$unit_backup" "$exec_backup"; do
        [[ -n "$staged_file" && ( -e "$staged_file" || -L "$staged_file" ) ]] && rm -f -- "$staged_file" || true
    done
    [[ -n "$validation_root" && -d "$validation_root" ]] && rm -rf -- "$validation_root" || true
}
trap cleanup EXIT

declare -i restart_requested=0
while (( $# > 0 )); do
    case "$1" in
        --restart) restart_requested=1 ;;
        --help|-h) usage; exit 0 ;;
        *) die "unknown argument: $1" ;;
    esac
    shift
done

(( EUID == 0 )) || die 'must run as root (use sudo)'
[[ -f "$source_unit" && -f "$source_exec" ]] || die 'checked-in systemd files are missing'
command -v systemd-analyze >/dev/null 2>&1 || die 'systemd-analyze is required'
command -v systemctl >/dev/null 2>&1 || die 'systemctl is required'
ensure_service_account
validate_environment_file
install -d -o root -g root -m 0755 -- "$(dirname -- "$environment_file")" \
    || die "could not provision environment directory $(dirname -- "$environment_file")"
chown root:"$account_group" -- "$environment_file" || die "could not set environment file group: ${environment_file}"
chmod 0640 -- "$environment_file" || die "could not set environment file mode: ${environment_file}"
[[ $(stat -c '%U:%G:%a' -- "$environment_file") == "root:${account_group}:640" ]] \
    || die "environment file must be root:${account_group} 0640: ${environment_file}"
validate_layout_file

stage_file "$source_exec" "$exec_target" 0755
staged_exec=$staged_file
stage_file "$source_unit" "$unit_target" 0644
staged_unit=$staged_file
validate_staged_pair
env \
    OMI_COLLECTOR_DEVICE_ADDRESS="${environment_values[OMI_COLLECTOR_DEVICE_ADDRESS]}" \
    OMI_COLLECTOR_DEVICE_SLUG="${environment_values[OMI_COLLECTOR_DEVICE_SLUG]}" \
    OMI_COLLECTOR_LAYOUT_PATH="${environment_values[OMI_COLLECTOR_LAYOUT_PATH]}" \
    OMI_COLLECTOR_PROJECT_DIR="${environment_values[OMI_COLLECTOR_PROJECT_DIR]}" \
    OMI_COLLECTOR_UV_BIN="${environment_values[OMI_COLLECTOR_UV_BIN]}" \
    OMI_COLLECTOR_UV_PROJECT_ENVIRONMENT="${environment_values[OMI_COLLECTOR_UV_PROJECT_ENVIRONMENT]}" \
    "$staged_exec" --check || die 'staged systemd wrapper validation failed'

backup_target "$exec_target" exec_backup
backup_target "$unit_target" unit_backup
if ! mv -f -- "$staged_exec" "$exec_target"; then
    rollback_pair
    die "could not install ${exec_target}"
fi
staged_exec=''
if ! mv -f -- "$staged_unit" "$unit_target"; then
    rollback_pair
    die "could not install ${unit_target}"
fi
staged_unit=''
if ! systemctl daemon-reload; then
    rollback_pair
    die 'systemd daemon-reload failed; restored previous pair'
fi
if ! systemctl enable "$service_name"; then
    rollback_pair
    die "could not enable ${service_name}; restored previous pair"
fi
rm -f -- "$unit_backup" "$exec_backup"
unit_backup=''
exec_backup=''

printf 'Installed and enabled %s.\n' "$service_name"
if (( restart_requested )); then
    systemctl restart "$service_name" || die "could not restart ${service_name}"
    printf 'Restarted %s.\n' "$service_name"
else
    printf 'Service was not started; use --restart to restart it explicitly.\n'
fi
