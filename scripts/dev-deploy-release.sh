#!/usr/bin/env bash

# Dev Script: deploys a published release to the maintainer's fixed server
# checkout. It is not a portable installer for third-party deployments.

set -uo pipefail

function die {
    local -r message="${1:-operation failed}"

    printf 'ERROR: %s\n' "$message" >&2
    exit "${2:-1}"
}

function usage {
    printf 'Usage: sudo scripts/dev-deploy-release.sh vMAJOR.MINOR.PATCH\n'
}

function is_expected_origin {
    # args
    local -r origin_url="$1"

    # result: true for either canonical GitHub transport
    [[ "$origin_url" == 'git@github.com:j2h4u/omi-collector.git' \
        || "$origin_url" == 'https://github.com/j2h4u/omi-collector.git' ]]
}

declare -r PROJECT_DIR='/opt/omi-collector'
declare -r SERVICE_NAME='omi-collector.service'

if [[ "${1:-}" == '--help' || "${1:-}" == '-h' ]]; then
    usage
    exit 0
fi

# assert: exactly one release tag was supplied
[[ $# -eq 1 ]] || {
    usage >&2
    die 'expected exactly one release tag'
}

declare -r RELEASE_TAG="$1"

# assert: the requested revision is a normal release tag
[[ "$RELEASE_TAG" =~ ^v[0-9]+\.[0-9]+\.[0-9]+$ ]] \
    || die "invalid release tag: ${RELEASE_TAG}"

# assert: privileged checkout and service operations are available
(( EUID == 0 )) || die 'run this script with sudo'

# assert: the documented production checkout exists
[[ -d "$PROJECT_DIR/.git" && ! -L "$PROJECT_DIR" ]] \
    || die "production checkout is missing or unsafe: ${PROJECT_DIR}"

origin_url=$(git -C "$PROJECT_DIR" remote get-url origin) \
    || die 'could not read the production checkout origin'
declare -r origin_url
is_expected_origin "$origin_url" || die "unexpected origin: ${origin_url}"

# assert: deployment will not discard local production changes
working_tree=$(git -C "$PROJECT_DIR" status --porcelain) \
    || die 'could not inspect the production checkout'
declare -r working_tree
[[ -z "$working_tree" ]] || die 'production checkout is dirty'

git -C "$PROJECT_DIR" fetch origin --prune --tags \
    || die 'could not fetch release metadata'

git -C "$PROJECT_DIR" show-ref --verify --quiet "refs/tags/${RELEASE_TAG}" \
    || die "release tag does not exist: ${RELEASE_TAG}"

release_commit=$(git -C "$PROJECT_DIR" rev-list --max-count=1 "${RELEASE_TAG}^{commit}") \
    || die "could not resolve release tag: ${RELEASE_TAG}"
declare -r release_commit

git -C "$PROJECT_DIR" merge-base --is-ancestor "$release_commit" origin/main \
    || die "release tag is not reachable from origin/main: ${RELEASE_TAG}"

git -C "$PROJECT_DIR" checkout --detach "$RELEASE_TAG" \
    || die "could not select release tag: ${RELEASE_TAG}"

"$PROJECT_DIR/scripts/deploy-systemd-service.sh" \
    || die "deployment failed for ${RELEASE_TAG}"

systemctl is-active --quiet "$SERVICE_NAME" \
    || die "service is not active after deployment: ${SERVICE_NAME}"

printf 'Deployed %s at %s.\n' "$RELEASE_TAG" "$release_commit"
