#!/usr/bin/env bash

set -uo pipefail

function die {
    printf 'ERROR: %s\n' "${1:-operation failed}" >&2
    exit "${2:-1}"
}

function cleanup {
    if [[ -n "$checkout" && -d "$checkout" ]]; then
        git -C "$repo_root" worktree remove --force "$checkout" || printf 'ERROR: could not remove release worktree %s\n' "$checkout" >&2
    fi
    [[ -z "$temporary_root" || ! -d "$temporary_root" ]] || rmdir -- "$temporary_root" 2>/dev/null || true
}

declare script_dir='' repo_root='' temporary_root='' checkout='' release_tag='' release_commit=''
(( EUID != 0 )) || die 'run as the service owner, without sudo'
if [[ "${1:-}" == --help || "${1:-}" == -h ]]; then
    printf 'Usage: scripts/dev-deploy-release.sh vMAJOR.MINOR.PATCH\n'
    exit 0
fi
(( $# == 1 )) || die 'expected exactly one release tag' 2
release_tag=$1
[[ "$release_tag" =~ ^v[0-9]+\.[0-9]+\.[0-9]+$ ]] || die "invalid release tag: ${release_tag}" 2
script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P) || die 'cannot resolve script directory'
repo_root=$(cd -- "${script_dir}/.." && pwd -P) || die 'cannot resolve repository root'
source_status=$(git -C "$repo_root" status --porcelain=v1 --untracked-files=all) \
    || die 'could not inspect source checkout state'
[[ -z "$source_status" ]] || die 'refusing release deployment from a dirty source checkout'
git -C "$repo_root" fetch origin --prune --tags || die 'could not fetch release metadata'
git -C "$repo_root" show-ref --verify --quiet "refs/tags/${release_tag}" \
    || die "release tag does not exist: ${release_tag}"
release_commit=$(git -C "$repo_root" rev-list --max-count=1 "${release_tag}^{commit}") \
    || die "could not resolve release tag: ${release_tag}"
git -C "$repo_root" merge-base --is-ancestor "$release_commit" origin/main \
    || die "release tag is not reachable from origin/main: ${release_tag}"
temporary_root=$(mktemp -d "${TMPDIR:-/tmp}/omi-collector-release.XXXXXX") \
    || die 'could not create temporary release worktree directory'
checkout="${temporary_root}/checkout"
trap cleanup EXIT
git -C "$repo_root" worktree add --detach "$checkout" "$release_tag" \
    || die "could not prepare release worktree for ${release_tag}"
"${checkout}/scripts/deploy-systemd-service.sh" || die "deployment failed for ${release_tag}"
printf 'Deployed %s at %s.\n' "$release_tag" "$release_commit"
