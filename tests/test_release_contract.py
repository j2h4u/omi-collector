from __future__ import annotations

import re
from pathlib import Path

from scripts.validate_pr_commits import validate_commit_messages
from scripts.validate_pr_title import validate_pr_title
from scripts.validate_release_config import validate_release_config
from scripts.validate_release_notes import _split_messages, validate_release_notes

_ROOT = Path(__file__).parents[1]
_CI_WORKFLOW = (_ROOT / ".github" / "workflows" / "ci.yml").read_text(encoding="utf-8")
_RELEASE_WORKFLOW = (_ROOT / ".github" / "workflows" / "release.yml").read_text(encoding="utf-8")

OVERRIDE = """
BEGIN_COMMIT_OVERRIDE
fix(capture): persist sealed collector artifacts

feat(cli): add a bounded inspection command
END_COMMIT_OVERRIDE
"""


def test_current_release_configuration_is_consistent() -> None:
    assert validate_release_config(_ROOT) == []


def test_releasable_pr_title_is_accepted() -> None:
    assert validate_pr_title("fix(capture): preserve sealed artifacts")[0]


def test_multi_commit_pr_without_an_override_is_rejected() -> None:
    ok, messages = validate_release_notes("Just a description.", commit_count=2, require_above=1)

    assert not ok
    assert any("squashes 2 commits" in message for message in messages)


def test_override_block_splits_into_one_entry_per_message() -> None:
    ok, messages = validate_release_notes(OVERRIDE, commit_count=2, require_above=1)

    assert ok
    assert "2 changelog entr" in messages[0]


def test_github_default_squash_body_shape_is_rejected() -> None:
    body = """
BEGIN_COMMIT_OVERRIDE
* fix(capture): persist sealed collector artifacts
* feat(cli): add a bounded inspection command
END_COMMIT_OVERRIDE
"""

    block = body.split("BEGIN_COMMIT_OVERRIDE")[1].split("END_COMMIT_OVERRIDE")[0]
    assert len(_split_messages(block)) == 1

    ok, messages = validate_release_notes(body, commit_count=2, require_above=1)

    assert not ok
    assert any("not a Conventional Commit subject" in message for message in messages)


def test_blank_line_after_breaking_change_is_rejected() -> None:
    body = """
BEGIN_COMMIT_OVERRIDE
refactor(cli)!: drop a duplicate command

BREAKING CHANGE: the command surface changed.

- Use the replacement command.
END_COMMIT_OVERRIDE
"""

    ok, messages = validate_release_notes(body, commit_count=2, require_above=1)

    assert not ok
    assert any("blank line directly after" in message for message in messages)


def test_column_zero_bullet_in_a_commit_body_is_rejected() -> None:
    ok, messages = validate_commit_messages(["ci: add release validation\n\n- validate it"])

    assert not ok
    assert any("Markdown bullet at column 0" in message for message in messages)


def test_release_pr_contract_requires_the_real_release_please_identity() -> None:
    assert "RELEASE_BRANCH: release-please--branches--main--components--omi-collector" in _CI_WORKFLOW
    assert "github.event.pull_request.head.repo.full_name || inputs.head_repo" in _CI_WORKFLOW
    assert "github.event.pull_request.user.login || inputs.pr_author" in _CI_WORKFLOW
    assert '"${HEAD_REPO}" == "${GITHUB_REPOSITORY}"' in _CI_WORKFLOW
    assert '"${PR_AUTHOR}" == "github-actions[bot]"' in _CI_WORKFLOW
    assert '| jq -r --arg repository "${REPOSITORY}" --arg release_branch "${RELEASE_BRANCH}"' in _RELEASE_WORKFLOW
    assert (
        'select(.head.repo.full_name == $repository and .head.ref == $release_branch and .user.login == "github-actions[bot]")'
        in _RELEASE_WORKFLOW
    )


def test_release_merge_is_bound_to_repository_and_observed_head() -> None:
    assert 'gh pr merge "${pr_number}" --auto --squash \\' in _RELEASE_WORKFLOW
    assert '--repo "${REPOSITORY}"' in _RELEASE_WORKFLOW
    assert '--match-head-commit "${head_sha}"' in _RELEASE_WORKFLOW


def test_release_attestation_requires_exact_release_pr_workflow_runs() -> None:
    assert "statuses: write" in _RELEASE_WORKFLOW
    assert 'contexts=(ci "Analyze Python" dependency-review)' in _RELEASE_WORKFLOW
    assert '"repos/${REPOSITORY}/statuses/${head_sha}"' in _RELEASE_WORKFLOW
    assert "event=workflow_dispatch&branch=${head_ref}" in _RELEASE_WORKFLOW
    assert "--jq --arg" not in _RELEASE_WORKFLOW
    assert "| jq -r --arg workflow_name" in _RELEASE_WORKFLOW
    assert '.event == "workflow_dispatch"' in _RELEASE_WORKFLOW
    assert ".head_branch == $head_ref" in _RELEASE_WORKFLOW
    assert ".head_sha == $head_sha" in _RELEASE_WORKFLOW
    assert ".created_at >= $started_at" in _RELEASE_WORKFLOW
    assert "| select($existing | index($id) | not)\n                   | $id]" in _RELEASE_WORKFLOW
    assert "wait_for_workflow ci.yml CI" in _RELEASE_WORKFLOW
    assert "wait_for_workflow codeql.yml CodeQL" in _RELEASE_WORKFLOW
    assert 'wait_for_workflow dependency-review.yml "Dependency review"' in _RELEASE_WORKFLOW


def test_release_attestation_fails_before_auto_merge_and_only_then_succeeds() -> None:
    merge_at = _RELEASE_WORKFLOW.index('gh pr merge "${pr_number}" --auto --squash')
    assert _RELEASE_WORKFLOW.index('publish_status ci failure "release PR CI attestation failed"') < merge_at
    assert (
        _RELEASE_WORKFLOW.index('publish_status "Analyze Python" failure "release PR CodeQL attestation failed"')
        < merge_at
    )
    assert (
        _RELEASE_WORKFLOW.index(
            'publish_status dependency-review failure "release PR dependency review attestation failed"'
        )
        < merge_at
    )
    assert (
        _RELEASE_WORKFLOW.index('publish_status "${context}" success "release PR check attestation succeeded"')
        < merge_at
    )


def test_attested_ci_dispatch_replays_the_release_pr_contract_on_exact_head() -> None:
    assert "release_attestation:" in _CI_WORKFLOW
    for input_name in ("pr_title:", "pr_body:", "base_sha:", "head_sha:", "head_ref:", "head_repo:", "pr_author:"):
        assert input_name in _CI_WORKFLOW
    assert "Verify release-attestation metadata" in _CI_WORKFLOW
    assert '"${HEAD_SHA}" = "${GITHUB_SHA}"' in _CI_WORKFLOW
    assert '"${HEAD_REF}" = "${GITHUB_REF_NAME}"' in _CI_WORKFLOW
    assert (
        "ref: ${{ github.event_name == 'pull_request' && github.event.pull_request.head.sha || inputs.head_sha }}"
        in _CI_WORKFLOW
    )
    assert "Bind checkout to attested PR commits" in _CI_WORKFLOW
    assert 'git merge-base --is-ancestor "${BASE_SHA}" "${HEAD_SHA}"' in _CI_WORKFLOW
    assert "github.event_name == 'pull_request' || inputs.release_attestation" in _CI_WORKFLOW


def test_token_merge_dispatches_checks_and_release_for_exact_main_commit() -> None:
    assert '(.merge_commit_sha // "")' in _RELEASE_WORKFLOW
    assert 'main_sha="$(gh api "repos/${REPOSITORY}/git/ref/heads/main" --jq \'.object.sha\')"' in _RELEASE_WORKFLOW
    assert "actions/workflows/ci.yml/dispatches" in _RELEASE_WORKFLOW
    assert "actions/workflows/release.yml/dispatches" in _RELEASE_WORKFLOW
    assert '-f "inputs[commit-sha]=${merge_sha}"' in _RELEASE_WORKFLOW
    assert "requested release commit" in _RELEASE_WORKFLOW
    assert "Verify requested release commit is still main tip" in _RELEASE_WORKFLOW
    assert "-f ref=main" in _RELEASE_WORKFLOW
    assert "main advanced from merge commit" in _RELEASE_WORKFLOW


def test_release_job_timeout_exceeds_its_merge_poll_window() -> None:
    release_job = re.search(r"(?ms)^  release-please:\n(?P<body>.*)\Z", _RELEASE_WORKFLOW)

    assert release_job is not None
    timeout = re.search(r"^    timeout-minutes: (\d+)$", release_job.group("body"), re.MULTILINE)
    poll_window = re.search(r"deadline=.*\+ (\d+) \)\)", release_job.group("body"))

    assert timeout is not None
    assert poll_window is not None
    assert int(timeout.group(1)) * 60 > int(poll_window.group(1))


def test_release_prs_keep_the_complete_ci_gate() -> None:
    for job in ("quality:", "test:", "crap:", "docker-build:", "runtime-smoke:"):
        assert f"  {job}" in _CI_WORKFLOW
    assert "needs: [pr-release-contract, quality, test, crap, docker-build, runtime-smoke]" in _CI_WORKFLOW


def test_security_document_does_not_claim_unavailable_validity_checks() -> None:
    security = (_ROOT / "docs" / "SECURITY.md").read_text(encoding="utf-8")

    assert "validity checks are unavailable on the current repository plan" in security
    assert "does not claim that validity checks are enabled" in security
