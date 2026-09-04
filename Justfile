set shell := ["bash", "-uc"]
export UV_LINK_MODE := "hardlink"

# Show available repo commands.
default:
    @just --list

# Compile Python sources for syntax errors.
compile:
    uv run python -m compileall -q src scripts tests

# Verify uv.lock is synchronized with pyproject.toml.
lock-check:
    uv lock --check

# Lint with ruff across the whole repo.
lint:
    uv run ruff check --preview src scripts tests

# Check preview-only complexity/refactor rules explicitly.
preview-complexity-lint:
    uv run ruff check --preview --select PLR0914,PLR0916,PLR0917 src scripts tests

# Check production code for accidental debug prints.
print-lint:
    uv run ruff check --preview --select T20 src/omi_collector

# Check formatting without writing.
fmt-check:
    uv run ruff format --no-preview --check src scripts tests

# Check import-layer architecture contracts.
import-contracts:
    uv run lint-imports

# Validate GitHub Actions workflow syntax and expressions.
actionlint:
    uv run actionlint

# Guard obvious supply-chain drift in workflows and container image references.
supply-chain-pins:
    uv run python scripts/check_supply_chain_pins.py

# Check declared Python dependencies against imports.
deptry:
    uv run deptry src scripts tests --per-rule-ignores DEP004=radon

# Run the canonical static type checker on production code.
typecheck:
    uv run basedpyright src/omi_collector scripts

# Type-check tests separately so production and fixture issues stay easy to read.
typecheck-tests:
    uv run basedpyright tests --warnings

# Scan for dead code with vulture.
dead-code:
    uv run vulture

# Build and install the wheel in an isolated environment, then smoke the CLI.
package-smoke:
    uv run python scripts/check_packaging_smoke.py

# Auto-fix Ruff findings with safe fixes only, then format.
fix:
    uv run ruff check --preview --fix src scripts tests
    uv run ruff format --no-preview src scripts tests

# Static quality gate.
check: fmt-check lint preview-complexity-lint print-lint lock-check typecheck typecheck-tests import-contracts actionlint supply-chain-pins deptry compile dead-code package-smoke

# Unit tests.
unit:
    uv run pytest -q -n auto -m "not slow"

# Test coverage report.
coverage:
    uv run pytest --cov=src/omi_collector --cov-report=term-missing

# Human CRAP report over the full suite.
crap:
    uv run pytest --cov=src/omi_collector --cov-report=term-missing --crap --crap-threshold=30 --crap-top-n=30

# Hard CRAP gate: every function must stay at or below CRAP 30.
crap-check:
    coverage_file="$(mktemp /tmp/omi-collector-crap-coverage.XXXXXX.json)"; \
    trap 'rm -f "$coverage_file"' EXIT; \
    uv run pytest --cov=src/omi_collector --cov-report=json:"$coverage_file"; pytest_status=$?; \
    if (( pytest_status != 0 )); then exit "$pytest_status"; fi; \
    uv run python -m scripts.crap_gate --coverage "$coverage_file" --src src/omi_collector --threshold 30

# Validate the collector package image and Compose file without running a service.
docker-check:
    docker compose config --quiet
    docker build --check .

# Build the collector package image.
docker-build: docker-check
    docker build -t omi-collector:local .

# Recreate the local Docker service.
docker-up:
    docker compose up -d --force-recreate --remove-orphans --wait --wait-timeout 90

# CI-safe runtime smoke: build, start, wait for health, exercise the installed CLI, and clean up.
runtime-smoke:
    #!/usr/bin/env bash
    set -euo pipefail
    project="omi-collector-qa-$$-$RANDOM"
    cleanup() {
        status="$1"
        if [ "$status" -ne 0 ]; then
            docker compose -p "$project" ps || true
            docker compose -p "$project" logs --no-color --timestamps --tail=200 || true
        fi
        docker compose -p "$project" down -v --remove-orphans || true
    }
    trap 'cleanup "$?"' EXIT
    docker compose -p "$project" up -d --build --force-recreate --remove-orphans --wait --wait-timeout 90
    docker compose -p "$project" exec -T omi-collector-qa omi-collector health

# Full local gate for agents before claiming completion.
verify: check crap-check unit docker-build runtime-smoke

# Everything the PR and release metadata owe release-please before CI has to say no.
release-check title="" body="":
    #!/usr/bin/env bash
    set -euo pipefail
    base="$(git merge-base origin/main HEAD)"
    count="$(git rev-list --no-merges --count "${base}..HEAD")"
    title="{{title}}"
    if [ -z "${title}" ]; then
        title="$(git log -1 --format=%s HEAD)"
    fi
    uv run python scripts/validate_release_config.py
    uv run python scripts/validate_pr_title.py --title "${title}"
    uv run python -m scripts.validate_pr_commits --base-sha "${base}" --head-sha "$(git rev-parse HEAD)"
    if [ -n "{{body}}" ]; then
        uv run python -m scripts.validate_release_notes --body-file "{{body}}" --commit-count "${count}"
    elif [ "${count}" -gt 1 ]; then
        printf 'note: %s commits will squash into one.\n' "${count}" >&2
        printf 'The PR body needs a BEGIN_COMMIT_OVERRIDE block; re-run with body=<file> to check it.\n' >&2
        exit 1
    fi
