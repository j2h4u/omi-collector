#!/usr/bin/env bash

set -uo pipefail

function die {
    local -r message="${1:-operation failed}"
    local -ri exit_status="${2:-1}"

    printf 'ERROR: %s\n' "$message"
    exit "$exit_status"
} 1>&2

function usage {
    printf 'Usage: %s [--restart]\n' "$(basename -- "$0")"
    printf 'Install and enable omi-collector.service; restart only with --restart.\n'
}

function ensure_service_account {
    # args
    local -r account_user="$1" account_group="$2" state_dir="$3"

    # vars
    local primary_group

    # code
    # assert: the service state path is not a symlink
    [[ ! -L "$state_dir" ]] || die "state directory must not be a symlink: ${state_dir}"
    if ! getent group "$account_group" &> /dev/null; then
        groupadd --system "$account_group" || die "could not create system group ${account_group}"
    fi
    if ! getent passwd "$account_user" &> /dev/null; then
        useradd --system --gid "$account_group" --home-dir "$state_dir" --shell /usr/sbin/nologin --no-create-home \
            "$account_user" || die "could not create system user ${account_user}"
    fi
    primary_group=$(id -gn "$account_user") || die "could not determine primary group for ${account_user}"
    # assert: the existing account uses the dedicated group
    [[ "$primary_group" == "$account_group" ]] \
        || die "system user ${account_user} must have primary group ${account_group}"
    install -d -o "$account_user" -g "$account_group" -m 0750 -- "$state_dir" \
        || die "could not provision state directory ${state_dir}"
    # assert: systemd and the deployer see the expected service-state ownership
    [[ $(stat -c '%U:%G:%a' -- "$state_dir") == "${account_user}:${account_group}:750" && ! -L "$state_dir" ]] \
        || die "state directory must be ${account_user}:${account_group} 0750: ${state_dir}"
}

function validate_operator_config_file {
    # args
    local -r config_file="$1" account_group="$2"

    # code
    # assert: the one operator configuration is a regular local file
    [[ -f "$config_file" && ! -L "$config_file" ]] \
        || die "operator configuration is missing or unsafe: ${config_file}"
    chown root:root -- "$config_file" \
        || die "could not set operator configuration ownership: ${config_file}"
    chmod 0644 -- "$config_file" \
        || die "could not set operator configuration mode: ${config_file}"
    # assert: both the system collector and user-owned pipeline can read it
    [[ $(stat -c '%U:%G:%a' -- "$config_file") == "root:root:644" ]] \
        || die "operator configuration must be root:root 0644: ${config_file}"
}

function stage_file {
    # args
    local -r source="$1" target="$2" mode="$3" output_name="$4"

    # vars
    local target_dir temporary metadata

    # code
    target_dir=$(dirname -- "$target") || die "could not resolve target directory for ${target}"
    install -d -o root -g root -m 0755 -- "$target_dir" || die "could not create ${target_dir}"
    temporary=$(mktemp --tmpdir="$target_dir" ".$(basename -- "$target").tmp.XXXXXX") \
        || die "could not create temporary file beside ${target}"
    printf -v "$output_name" '%s' "$temporary"
    if ! install -o root -g root -m "$mode" -- "$source" "$temporary"; then
        rm -f -- "$temporary" || true
        printf -v "$output_name" '%s' ''
        die "could not stage ${target}"
    fi
    metadata=$(stat -c '%u:%g:%a' -- "$temporary") || die "could not inspect staged file: ${temporary}"
    # assert: staged system material is a root-owned regular file
    [[ "$metadata" == "0:0:${mode#0}" && -f "$temporary" && ! -L "$temporary" ]] \
        || die "staged file metadata is not root:root ${mode}: ${temporary}"
}

function backup_target {
    # args
    local -r target="$1" output_name="$2"

    # vars
    local target_dir backup

    # code
    if [[ ! -e "$target" && ! -L "$target" ]]; then
        printf -v "$output_name" '%s' ''
        return
    fi
    # assert: an existing installed unit is replaceable without following links
    [[ -f "$target" && ! -L "$target" ]] || die "existing target is not a regular file: ${target}"
    target_dir=$(dirname -- "$target") || die "could not resolve target directory for ${target}"
    backup=$(mktemp --tmpdir="$target_dir" ".$(basename -- "$target").backup.XXXXXX") \
        || die "could not create backup beside ${target}"
    printf -v "$output_name" '%s' "$backup"
    cp --preserve=mode,ownership,timestamps -- "$target" "$backup" || die "could not back up ${target}"
}

function restore_target {
    # args
    local -r target="$1" backup="$2"

    # code
    if [[ -n "$backup" ]]; then
        mv -f -- "$backup" "$target" || die "could not restore ${target}"
    else
        rm -f -- "$target" || die "could not remove incomplete ${target}"
    fi
}

function validate_staged_unit {
    # args
    local -r staged_unit="$1" service_name="$2"

    # vars
    local validation_root validation_unit

    # code
    validation_root=$(mktemp -d) || die 'could not create staged validation root'
    validation_unit="${validation_root}/${service_name}"
    if ! sed 's|^ExecStart=.*|ExecStart=/usr/bin/true|' "$staged_unit" > "$validation_unit"; then
        rm -rf -- "$validation_root" || true
        die 'could not prepare staged unit for validation'
    fi
    if ! chown root:root -- "$validation_unit" || ! chmod 0644 -- "$validation_unit"; then
        rm -rf -- "$validation_root" || true
        die 'could not protect staged validation unit'
    fi
    if ! systemd-analyze verify "$validation_unit"; then
        rm -rf -- "$validation_root" || true
        die 'systemd unit validation failed'
    fi
    rm -rf -- "$validation_root" || die 'could not remove staged validation root'
}

declare script_dir repo_root source_unit source_status source_status_sudoers config_file storage_root unit_target service_name
declare account_user account_group state_dir staged_unit unit_backup
declare -i restart_requested=0

script_dir=$(builtin cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P) || die 'cannot resolve installer directory'
repo_root=$(builtin cd -- "${script_dir}/.." && pwd -P) || die 'cannot resolve repository root'
source_unit="${repo_root}/systemd/omi-collector.service"
source_status="${repo_root}/scripts/omi-collector-status"
source_status_sudoers="${repo_root}/scripts/omi-collector-status.sudoers"
config_file='/srv/pipelines/omi/config.toml'
storage_root=$(dirname -- "$config_file") || die 'cannot resolve storage root'
unit_target='/etc/systemd/system/omi-collector.service'
service_name='omi-collector.service'
account_user='omi-collector'
account_group='omi-collector'
state_dir='/var/lib/omi-collector'
staged_unit=''
unit_backup=''

function cleanup {
    if [[ -n "$staged_unit" && ( -e "$staged_unit" || -L "$staged_unit" ) ]]; then
        rm -f -- "$staged_unit" || true
    fi
    if [[ -n "$unit_backup" && ( -e "$unit_backup" || -L "$unit_backup" ) ]]; then
        rm -f -- "$unit_backup" || true
    fi
}
trap cleanup EXIT

while (( $# > 0 )); do
    case "$1" in
        --restart) restart_requested=1 ;;
        --help|-h) usage; exit 0 ;;
        *) die "unknown argument: $1" ;;
    esac
    shift
done

# assert: installation has the authority and tools needed to change systemd
(( EUID == 0 )) || die 'must run as root (use sudo)'
[[ -f "$source_unit" ]] || die "checked-in systemd unit is missing: ${source_unit}"
command -v systemd-analyze &> /dev/null || die 'systemd-analyze is required'
command -v systemctl &> /dev/null || die 'systemctl is required'
[[ -x /usr/sbin/visudo ]] || die 'visudo is required'

ensure_service_account "$account_user" "$account_group" "$state_dir"
[[ -d "$storage_root" && ! -L "$storage_root" ]] \
    || die "storage root is missing or unsafe: ${storage_root}"
validate_operator_config_file "$config_file" "$account_group"
/usr/sbin/visudo -cf "$source_status_sudoers" || die 'status sudo policy is invalid'
install -o root -g root -m 0755 -- "$source_status" /usr/local/sbin/omi-collector-status \
    || die 'could not install operator status command'
install -o root -g root -m 0440 -- "$source_status_sudoers" /etc/sudoers.d/omi-collector-status \
    || die 'could not install operator status sudo policy'
stage_file "$source_unit" "$unit_target" 0644 staged_unit
validate_staged_unit "$staged_unit" "$service_name"
backup_target "$unit_target" unit_backup

if ! mv -f -- "$staged_unit" "$unit_target"; then
    restore_target "$unit_target" "$unit_backup"
    die "could not install ${unit_target}"
fi
staged_unit=''
if ! systemctl daemon-reload; then
    restore_target "$unit_target" "$unit_backup"
    systemctl daemon-reload || die 'daemon-reload failed while restoring the previous unit'
    die 'systemd daemon-reload failed; restored previous unit'
fi
if ! systemctl enable "$service_name"; then
    restore_target "$unit_target" "$unit_backup"
    systemctl daemon-reload || die 'daemon-reload failed while restoring the previous unit'
    die "could not enable ${service_name}; restored previous unit"
fi
rm -f -- "$unit_backup" || die 'could not remove installed-unit rollback backup'
unit_backup=''

printf 'Installed and enabled %s.\n' "$service_name"
if (( restart_requested )); then
    systemctl restart "$service_name" || die "could not restart ${service_name}"
    printf 'Restarted %s.\n' "$service_name"
else
    printf 'Service was not started; deploy a release before starting it.\n'
fi
