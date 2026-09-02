from __future__ import annotations

from pathlib import Path

from scripts.validate_pr_commits import validate_commit_messages
from scripts.validate_pr_title import validate_pr_title
from scripts.validate_release_config import validate_release_config
from scripts.validate_release_notes import _split_messages, validate_release_notes

_ROOT = Path(__file__).parents[1]

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
