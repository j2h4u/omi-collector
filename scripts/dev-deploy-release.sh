#!/usr/bin/env bash

set -uo pipefail

function die {
    printf 'ERROR: %s\n' "${1:-operation failed}" >&2
    exit "${2:-1}"
}

function cleanup {
    local -i status=$? cleanup_failed=0
    trap - EXIT
    if [[ -n "$checkout" && -d "$checkout" ]]; then
        git -C "$repo_root" worktree remove --force "$checkout" || { printf 'ERROR: could not remove release worktree %s\n' "$checkout" >&2; cleanup_failed=1; }
    fi
    if [[ -n "$temporary_root" && -d "$temporary_root" ]]; then
        rmdir -- "$temporary_root" || { printf 'ERROR: could not remove release work directory %s\n' "$temporary_root" >&2; cleanup_failed=1; }
    fi
    (( status != 0 || ! cleanup_failed )) || status=1
    if (( status == 0 )); then
        printf 'Deployed %s at %s.\n' "$release_tag" "$release_commit"
    fi
    exit "$status"
}

declare script_dir='' repo_root='' temporary_root='' checkout='' release_tag='' release_commit=''
declare remote_tag='' remote_ref='' advertised='' fetched_tag='' local_tag=''
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
remote_ref="refs/tags/${release_tag}"
advertised=$(git -C "$repo_root" ls-remote --refs origin "$remote_ref") || die 'could not inspect origin tag'
[[ -n "$advertised" && "$advertised" != *$'\n'* ]] || die "release tag is absent or ambiguous on origin: ${release_tag}"
IFS=$'\t' read -r remote_tag local_tag <<< "$advertised"
[[ "$remote_tag" =~ ^[0-9a-f]{40,64}$ && "$local_tag" == "$remote_ref" ]] || die 'invalid origin tag record'
if git -C "$repo_root" show-ref --verify --quiet "$remote_ref"; then
    local_tag=$(git -C "$repo_root" rev-parse --verify "$remote_ref") || die 'could not inspect local tag'
    [[ "$local_tag" == "$remote_tag" ]] || die "local tag conflicts with origin: ${release_tag}"
fi
git -C "$repo_root" fetch --no-tags origin "$remote_ref" || die 'could not fetch selected origin tag'
fetched_tag=$(git -C "$repo_root" rev-parse --verify FETCH_HEAD) || die 'could not inspect fetched tag'
[[ "$fetched_tag" == "$remote_tag" ]] || die "origin tag changed during fetch: ${release_tag}"
release_commit=$(git -C "$repo_root" rev-parse --verify 'FETCH_HEAD^{commit}') \
    || die "could not resolve release tag to a commit: ${release_tag}"
git -C "$repo_root" fetch --no-tags origin '+refs/heads/main:refs/remotes/origin/main' \
    || die 'could not fetch origin main'
git -C "$repo_root" merge-base --is-ancestor "$release_commit" origin/main \
    || die "release tag is not reachable from origin/main: ${release_tag}"
temporary_root=$(mktemp -d "${TMPDIR:-/tmp}/omi-collector-release.XXXXXX") \
    || die 'could not create temporary release worktree directory'
checkout="${temporary_root}/checkout"
trap cleanup EXIT
git -C "$repo_root" worktree add --detach "$checkout" "$release_commit" \
    || die "could not prepare release worktree for ${release_tag}"
"${checkout}/scripts/deploy-systemd-service.sh" || die "deployment failed for ${release_tag}"
