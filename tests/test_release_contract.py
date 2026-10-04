from __future__ import annotations

import json
import re
from io import StringIO
from pathlib import Path

import pytest
from scripts.validate_pr_commits import validate_commit_messages
from scripts.validate_pr_title import main as validate_title_main
from scripts.validate_pr_title import validate_pr_title
from scripts.validate_release_config import main as validate_config_main
from scripts.validate_release_config import validate_release_config
from scripts.validate_release_notes import _split_messages, validate_release_notes
from scripts.validate_release_notes import main as validate_notes_main

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


def test_invalid_pr_titles_have_stable_errors() -> None:
    assert validate_pr_title("   ") == (False, "PR title is empty.")
    ok, message = validate_pr_title("not conventional")
    assert not ok
    assert message.startswith("PR title must look like")
    ok, message = validate_pr_title("wip: draft")
    assert not ok
    assert message.startswith("Unsupported Conventional Commit type 'wip'.")


def test_pr_title_main_routes_messages_to_expected_streams(capsys: pytest.CaptureFixture[str]) -> None:
    assert validate_title_main(["--title", "fix: repair capture"]) == 0
    captured = capsys.readouterr()
    assert captured.out == "PR title is releasable.\n"
    assert captured.err == ""

    assert validate_title_main(["--title", "wip: draft"]) == 1
    captured = capsys.readouterr()
    assert captured.out == ""
    assert captured.err.startswith("Unsupported Conventional Commit type 'wip'.")


def test_pr_title_main_requires_title() -> None:
    with pytest.raises(SystemExit) as error:
        validate_title_main([])

    assert error.value.code == 2


def _write_release_fixture(root: Path, *, package: dict[str, object] | None = None) -> None:
    root.mkdir(parents=True, exist_ok=True)
    release_package = (
        package
        if package is not None
        else {
            "release-type": "python",
            "package-name": "omi-collector",
            "include-component-in-tag": False,
            "extra-files": [
                {"type": "toml", "path": "uv.lock", "jsonpath": "$.package[?(@.name.value=='omi-collector')].version"}
            ],
        }
    )
    (root / "release-please-config.json").write_text(json.dumps({"packages": {".": release_package}}), encoding="utf-8")
    (root / ".release-please-manifest.json").write_text(json.dumps({".": "1.2.3"}), encoding="utf-8")
    (root / "pyproject.toml").write_text('[project]\nname = "omi-collector"\nversion = "1.2.3"\n', encoding="utf-8")
    (root / "uv.lock").write_text('[[package]]\nname = "omi-collector"\nversion = "1.2.3"\n', encoding="utf-8")


def test_release_config_aggregates_package_invariants(tmp_path: Path) -> None:
    _write_release_fixture(
        tmp_path,
        package={
            "release-type": "node",
            "package-name": "wrong",
            "include-component-in-tag": True,
            "extra-files": [],
        },
    )

    assert validate_release_config(tmp_path) == [
        "release-please must use the python strategy so it updates pyproject.toml",
        "release-please package-name must be 'omi-collector'",
        "release-please root tags must not include a component prefix",
        "release-please must update the omi-collector version in uv.lock",
    ]


def test_release_config_checks_manifest_project_and_lock_coherence(tmp_path: Path) -> None:
    _write_release_fixture(tmp_path)
    assert validate_release_config(tmp_path) == []
    (tmp_path / ".release-please-manifest.json").write_text('{".": "9.9.9"}', encoding="utf-8")
    (tmp_path / "uv.lock").write_text('[[package]]\nname = "omi-collector"\nversion = "8.8.8"\n', encoding="utf-8")
    assert validate_release_config(tmp_path) == [
        "release-please manifest version must match pyproject.toml",
        "uv.lock project version must match pyproject.toml",
    ]


@pytest.mark.parametrize(
    "lock_text",
    [
        '[[package]]\nname = "omi-collector"\nversion = 123\n',
        (
            '[[package]]\nname = "omi-collector"\nversion = "1.2.3"\n'
            '[[package]]\nname = "omi-collector"\nversion = "1.2.3"\n'
        ),
    ],
    ids=("non-string-version", "duplicate-root-package"),
)
def test_release_config_requires_one_string_root_package_version(tmp_path: Path, lock_text: str) -> None:
    _write_release_fixture(tmp_path)
    assert validate_release_config(tmp_path) == []
    (tmp_path / "uv.lock").write_text(lock_text, encoding="utf-8")

    assert validate_release_config(tmp_path) == ["uv.lock must contain exactly one 'omi-collector' package version"]


def test_release_config_requires_root_package_configuration(tmp_path: Path) -> None:
    _write_release_fixture(tmp_path)
    (tmp_path / "release-please-config.json").write_text(json.dumps({"packages": {"other": {}}}), encoding="utf-8")

    assert validate_release_config(tmp_path) == ["release-please-config.json must configure the root package"]


def test_release_config_reports_missing_project_version_and_lock_package_list(tmp_path: Path) -> None:
    _write_release_fixture(tmp_path)
    (tmp_path / "pyproject.toml").write_text('[project]\nname = "omi-collector"\n', encoding="utf-8")
    (tmp_path / "uv.lock").write_text("version = 1\n", encoding="utf-8")
    assert validate_release_config(tmp_path) == [
        "pyproject.toml must declare [project].version",
        "release-please manifest version must match pyproject.toml",
        "uv.lock must contain a package list",
    ]


def test_release_config_main_reports_valid_and_invalid_root(capsys: pytest.CaptureFixture[str], tmp_path: Path) -> None:
    assert validate_config_main(["--root", str(_ROOT)]) == 0
    captured = capsys.readouterr()
    assert captured.out == "release-please configuration matches pyproject.toml and uv.lock\n"
    assert captured.err == ""

    _write_release_fixture(tmp_path, package={})
    assert validate_config_main(["--root", str(tmp_path)]) == 1
    captured = capsys.readouterr()
    assert (
        captured.out
        == "release configuration error: release-please must use the python strategy so it updates pyproject.toml\nrelease configuration error: release-please package-name must be 'omi-collector'\nrelease configuration error: release-please root tags must not include a component prefix\nrelease configuration error: release-please must update the omi-collector version in uv.lock\n"
    )
    assert captured.err == ""


def test_multi_commit_pr_without_an_override_is_rejected() -> None:
    ok, messages = validate_release_notes("Just a description.", commit_count=2, require_above=1)

    assert not ok
    assert any("squashes 2 commits" in message for message in messages)


def test_release_notes_override_threshold_and_empty_override() -> None:
    assert validate_release_notes("body", commit_count=1, require_above=1)[0]
    assert validate_release_notes("body", commit_count=0, require_above=0)[0]
    assert validate_release_notes("BEGIN_COMMIT_OVERRIDE\n \nEND_COMMIT_OVERRIDE", 2, 1) == (
        False,
        ["The BEGIN_COMMIT_OVERRIDE block is empty."],
    )


def test_release_notes_reject_malformed_and_unsupported_subjects() -> None:
    for subject, expected in (
        ("plain text", "not a Conventional Commit subject"),
        ("wip: draft", "uses unsupported type 'wip'"),
    ):
        ok, messages = validate_release_notes(f"BEGIN_COMMIT_OVERRIDE\n{subject}\nEND_COMMIT_OVERRIDE", 1, 1)
        assert not ok
        assert any(expected in message for message in messages)


def test_breaking_change_bullets_immediately_follow_note() -> None:
    body = """BEGIN_COMMIT_OVERRIDE
refactor(cli)!: replace command syntax

BREAKING CHANGE: old invocations no longer work.
- Use the new command syntax.
END_COMMIT_OVERRIDE"""

    assert validate_release_notes(body, 2, 1)[0]


def test_release_notes_main_file_stdin_and_failure_streams(
    capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    body_file = tmp_path / "body.md"
    body_file.write_text("fix: repair capture", encoding="utf-8")
    assert validate_notes_main(["--body-file", str(body_file), "--commit-count", "1"]) == 0
    captured = capsys.readouterr()
    assert captured.out == "No override block, and none owed for 1 commit(s).\n"
    assert captured.err == ""

    monkeypatch.setattr("sys.stdin", StringIO("body"))
    assert validate_notes_main(["--body-file", "-", "--commit-count", "2"]) == 1
    captured = capsys.readouterr()
    assert captured.out == ""
    assert "squashes 2 commits" in captured.err
    assert "Add a BEGIN_COMMIT_OVERRIDE / END_COMMIT_OVERRIDE block" in captured.err


def test_release_notes_main_requires_body_and_commit_count_before_input_io(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    class UnreadableInput:
        def read(self) -> str:
            raise AssertionError("stdin must not be read before argument validation")

    def unreadable_file(_path: Path, *_args: object, **_kwargs: object) -> str:
        raise AssertionError("body file must not be read before argument validation")

    monkeypatch.setattr("sys.stdin", UnreadableInput())
    with pytest.raises(SystemExit) as missing_body:
        validate_notes_main(["--commit-count", "1"])
    assert missing_body.value.code == 2

    monkeypatch.setattr(Path, "read_text", unreadable_file)
    with pytest.raises(SystemExit) as missing_count:
        validate_notes_main(["--body-file", str(tmp_path / "body.md")])
    assert missing_count.value.code == 2


def test_override_block_splits_into_one_entry_per_message() -> None:
    ok, messages = validate_release_notes(OVERRIDE, commit_count=2, require_above=1)

    assert ok
    assert "2 changelog entr" in messages[0]


def test_first_override_entry_at_block_start_accepts_multiple_well_formed_messages() -> None:
    body = """PR context before release notes.
BEGIN_COMMIT_OVERRIDE
fix(capture): retain the first record

Keep the first record attached to the attempt.

feat(cli): expose current status

Show the current collector state.
END_COMMIT_OVERRIDE"""

    assert validate_release_notes(body, commit_count=2, require_above=1) == (
        True,
        ["Override block parses into 2 changelog entr(ies)."],
    )


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
