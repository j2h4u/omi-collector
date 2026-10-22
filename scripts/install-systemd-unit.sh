#!/usr/bin/env bash

set -uo pipefail

function die {
    printf 'ERROR: %s\n' "${1:-operation failed}" >&2
    exit "${2:-1}"
}

function cleanup {
    [[ -z "$staged_unit" || ! -e "$staged_unit" ]] || rm -f -- "$staged_unit"
    [[ -z "$staged_policy" || ! -e "$staged_policy" ]] || rm -f -- "$staged_policy"
    [[ -z "$validation_root" || ! -d "$validation_root" ]] || rm -rf -- "$validation_root"
}

function validate_runtime_directory {
    local -r path="$1"
    local owner_group_mode
    if [[ -L "$path" ]]; then
        die "Collector data path must not be a symlink: ${path}"
    fi
    if [[ -e "$path" ]]; then
        [[ -d "$path" ]] || die "Collector data path is not a directory: ${path}"
        owner_group_mode=$(stat -c '%u:%g:%a' -- "$path") || die "cannot inspect Collector data path: ${path}"
        [[ "$owner_group_mode" == "${operator_uid}:${operator_gid}:2750" ]] \
            || die "Collector data path must be owned by ${operator_user}:${operator_group} with mode 2750: ${path}"
    fi
}

declare script_dir='' repo_root='' source_unit='' source_helper='' target_unit='' target_helper=''
declare target_policy='' staged_unit='' staged_policy='' validation_root=''
declare operator_user='' operator_group='' operator_uid='' operator_gid='' operator_home=''
declare source='' owner='' mode='' data_directory='' data_root='/srv/pipelines/omi' config_owner=''
declare directory=''
declare -i restart_requested=0

if (( EUID != 0 )); then
    printf 'Usage: sudo scripts/install-systemd-unit.sh [--restart]\n' >&2
    die 'this one-time installer requires root'
fi
if [[ "${1:-}" == '--restart' && $# == 1 ]]; then
    restart_requested=1
elif (( $# != 0 )); then
    die 'usage: install-systemd-unit.sh [--restart]' 2
fi
[[ -n "${SUDO_USER:-}" && "$SUDO_USER" != root ]] || die 'run through sudo from the nonroot service owner'
operator_user=$SUDO_USER
passwd_record=$(getent passwd "$operator_user") || die 'could not resolve invoking user'
IFS=: read -r _ _ operator_uid operator_gid _ operator_home _ <<< "$passwd_record"
operator_group=$(getent group "$operator_gid" | cut -d: -f1) || die 'could not resolve invoking user primary group'
[[ "$operator_uid" =~ ^[1-9][0-9]*$ && "$operator_home" =~ ^/[A-Za-z0-9._/-]+$ && -d "$operator_home" ]] \
    || die 'invoking user must have a normal account and home directory'
[[ "$operator_user" =~ ^[a-z_][a-z0-9_-]*[$]?$ && "$operator_group" =~ ^[a-z_][a-z0-9_-]*[$]?$ ]] \
    || die 'invoking user or primary group cannot be safely rendered into the systemd unit'

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P) || die 'cannot resolve installer directory'
repo_root=$(cd -- "${script_dir}/.." && pwd -P) || die 'cannot resolve repository root'
source_unit="${repo_root}/systemd/omi-collector.service"
source_helper="${repo_root}/scripts/omi-collector-bluetooth-ready"
target_unit='/etc/systemd/system/omi-collector.service'
target_helper='/usr/local/sbin/omi-collector-bluetooth-ready'
target_policy='/etc/sudoers.d/omi-collector-service-control'

# assert: root only installs from the frozen root-owned source used for the one-time cutover
for source in "${BASH_SOURCE[0]}" "$source_unit" "$source_helper"; do
    [[ -f "$source" && ! -L "$source" ]] || die "trusted install source is missing or unsafe: ${source}"
    read -r owner mode <<< "$(stat -c '%u %a' -- "$source")" || die "cannot inspect install source: ${source}"
    if [[ "$owner" != 0 ]] || (( (8#$mode & 022) != 0 )); then
        die "install source must be root-owned and not group/world writable: ${source}"
    fi
done
directory="$repo_root"
while [[ "$directory" != / ]]; do
    read -r owner mode <<< "$(stat -c '%u %a' -- "$directory")" || die "cannot inspect source directory: ${directory}"
    if [[ "$owner" != 0 ]] || (( (8#$mode & 022) != 0 )); then
        die "frozen install source path must be root-owned and not group/world writable: ${directory}"
    fi
    directory=$(dirname -- "$directory") || die 'cannot inspect frozen source path'
done

# The runtime root is operator-owned, without changing any foreign child tree.
[[ -d "$data_root" && ! -L "$data_root" ]] || die 'Collector data root must be a real directory'
chown "$operator_user:$operator_group" -- "$data_root" || die 'could not assign Collector data root to the operator'
chmod 0755 -- "$data_root" || die 'could not set Collector data root mode'
[[ -f /srv/pipelines/omi/config.toml && ! -L /srv/pipelines/omi/config.toml ]] \
    || die 'operator configuration must be a regular non-symlink file'
config_owner=$(stat -c '%u' -- /srv/pipelines/omi/config.toml) || die 'cannot inspect operator configuration owner'
[[ "$config_owner" == "$operator_uid" ]] || die 'operator configuration must be owned by the service user'

# Existing data dirs must already have the ownership needed by the rendered service identity.
for data_directory in /srv/pipelines/omi/collector /srv/pipelines/omi/ready; do
    validate_runtime_directory "$data_directory"
done

# Revoke old root entrypoints before this unit can point at an operator-owned environment.
rm -f -- /usr/local/sbin/omi-collector-deploy-release /etc/sudoers.d/omi-collector-deploy-release \
    /usr/local/sbin/omi-collector-status /etc/sudoers.d/omi-collector-status \
    || die 'could not revoke obsolete privileged entrypoints'

trap cleanup EXIT
install -d -o root -g root -m 0755 -- "$(dirname -- "$target_unit")" "$(dirname -- "$target_helper")" \
    "$(dirname -- "$target_policy")" || die 'could not prepare system configuration directories'
staged_unit=$(mktemp --tmpdir="$(dirname -- "$target_unit")" .omi-collector.service.XXXXXX) \
    || die 'could not stage systemd unit'
sed -e "s/@OMI_COLLECTOR_USER@/${operator_user}/g" \
    -e "s/@OMI_COLLECTOR_GROUP@/${operator_group}/g" \
    -e "s|@OMI_COLLECTOR_HOME@|${operator_home}|g" "$source_unit" > "$staged_unit" \
    || die 'could not render systemd unit'
chown root:root -- "$staged_unit" || die 'could not set staged unit ownership'
chmod 0644 -- "$staged_unit" || die 'could not protect staged unit'
validation_root=$(mktemp -d) || die 'could not stage systemd validation unit'
sed -e 's|^ExecStartPre=.*|ExecStartPre=/usr/bin/true|' \
    -e 's|^ExecStart=.*|ExecStart=/usr/bin/true|' "$staged_unit" > "${validation_root}/omi-collector.service" \
    || die 'could not prepare systemd validation unit'
printf '[Unit]\nDescription=Bluetooth validation dependency\n\n[Service]\nExecStart=/usr/bin/true\n' \
    > "${validation_root}/bluetooth.service" || die 'could not prepare validation dependency'
systemd-analyze verify "${validation_root}/bluetooth.service" "${validation_root}/omi-collector.service" \
    || die 'systemd unit validation failed'

staged_policy=$(mktemp --tmpdir="$(dirname -- "$target_policy")" .omi-collector-service-control.XXXXXX) \
    || die 'could not stage service-control policy'
printf '%s ALL=(root) NOPASSWD: /usr/bin/systemctl start omi-collector.service, /usr/bin/systemctl start --no-block omi-collector.service, /usr/bin/systemctl stop omi-collector.service, /usr/bin/systemctl restart omi-collector.service, /usr/bin/systemctl restart --no-block omi-collector.service\n' \
    "$operator_user" > "$staged_policy" || die 'could not write service-control policy'
chown root:root -- "$staged_policy" || die 'could not set service-control policy ownership'
chmod 0440 -- "$staged_policy" || die 'could not protect service-control policy'
visudo --check --file "$staged_policy" || die 'service-control policy validation failed'
for data_directory in /srv/pipelines/omi/collector /srv/pipelines/omi/ready; do
    if [[ ! -e "$data_directory" ]]; then
        install -d -o "$operator_user" -g "$operator_group" -m 2750 -- "$data_directory" \
            || die "could not create Collector data directory: ${data_directory}"
    fi
done

install -o root -g root -m 0755 -- "$source_helper" "$target_helper" || die 'could not install Bluetooth readiness helper'
install -o root -g root -m 0644 -- "$staged_unit" "$target_unit" || die 'could not install systemd unit'
install -o root -g root -m 0440 -- "$staged_policy" "$target_policy" || die 'could not install service-control policy'
systemctl daemon-reload || die 'systemd daemon-reload failed'
systemctl enable omi-collector.service || die 'could not enable omi-collector.service'
if (( restart_requested )); then
    systemctl restart --no-block omi-collector.service || die 'could not enqueue omi-collector.service restart'
    printf 'Restart queued for omi-collector.service; verify service readiness with scripts/omi-collector-status.\n'
fi
printf 'Installed the system unit for %s.\n' "$operator_user"
