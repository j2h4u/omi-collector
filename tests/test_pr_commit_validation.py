from __future__ import annotations

import subprocess
from pathlib import Path
from typing import Never

import pytest
from pytest import CaptureFixture, MonkeyPatch
from scripts.validate_pr_commits import (
    commit_messages,
    editable_message,
    main,
    validate_commit_messages,
)


def test_validate_commit_messages_empty_and_multiple_valid_messages() -> None:
    assert validate_commit_messages([]) == (True, ["No non-merge commits to validate."])
    assert validate_commit_messages(["fix(api): handle retries", "feat(cli): add status"])[0] is True
    assert validate_commit_messages(["fix(api): handle retries", "feat(cli): add status"])[1] == [
        "All 2 commit message(s) are releasable."
    ]


def test_validate_commit_messages_aggregates_subject_and_type_problems() -> None:
    ok, problems = validate_commit_messages(["broken subject", "wip: add release task"])

    assert not ok
    assert any("not a Conventional Commit subject" in problem for problem in problems)
    assert any("uses unsupported type 'wip'" in problem for problem in problems)


def test_validate_commit_messages_accepts_indented_bullets_and_rejects_other_column_zero_markers() -> None:
    assert validate_commit_messages(["docs: explain setup\n\n  - step one\n  * step two\n  + step three"])[0]
    for marker in ("*", "+"):
        ok, problems = validate_commit_messages([f"docs: explain setup\n\n{marker} step one"])
        assert not ok
        assert any("Markdown bullet at column 0" in problem for problem in problems)


def test_title_only_squash_skips_body_bullets_but_keeps_subject_validation() -> None:
    message = "fix(audio): retain recording metadata\n\n- Keep metadata attached."
    assert not validate_commit_messages([message])[0]
    assert validate_commit_messages([message], title_only_squash=True)[0]
    assert not validate_commit_messages(["wip: unsafe type\n\n- body"], title_only_squash=True)[0]


def test_editable_message_discards_comments_and_scissors_but_keeps_body() -> None:
    raw = (
        "fix(audio): preserve capture notes\n\n"
        "Keep the note with the captured audio.\n"
        "  - retain indented list items\n"
        "# editor comment\n"
        "# ------------------------ >8 ------------------------\n"
        "ignored template text\n"
    )

    assert editable_message(raw) == (
        "fix(audio): preserve capture notes\n\nKeep the note with the captured audio.\n  - retain indented list items"
    )


def _git(repo: Path, *args: str) -> str:
    return subprocess.run(["git", *args], cwd=repo, check=True, capture_output=True, text=True).stdout.strip()


def test_commit_messages_reads_non_merge_commits_newest_first(tmp_path: Path, monkeypatch: MonkeyPatch) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    _git(repo, "init", "-q")
    _git(repo, "config", "user.name", "Test User")
    _git(repo, "config", "user.email", "test@example.invalid")
    (repo / "file.txt").write_text("base\n", encoding="utf-8")
    _git(repo, "add", "file.txt")
    _git(repo, "commit", "-q", "-m", "chore: establish fixture")
    base_sha = _git(repo, "rev-parse", "HEAD")
    (repo / "file.txt").write_text("first\n", encoding="utf-8")
    _git(repo, "commit", "-qam", "fix(audio): retain recording metadata", "-m", "Keep metadata attached.")
    (repo / "file.txt").write_text("second\n", encoding="utf-8")
    _git(repo, "commit", "-qam", "feat(cli): show capture status", "-m", "Expose the current state.")
    head_sha = _git(repo, "rev-parse", "HEAD")
    monkeypatch.chdir(repo)

    assert commit_messages(base_sha, head_sha) == [
        "feat(cli): show capture status\n\nExpose the current state.",
        "fix(audio): retain recording metadata\n\nKeep metadata attached.",
    ]


def test_main_git_mode_reports_valid_range_and_invalid_ref_fails(
    tmp_path: Path, monkeypatch: MonkeyPatch, capsys: CaptureFixture[str]
) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    _git(repo, "init", "-q")
    _git(repo, "config", "user.name", "Test User")
    _git(repo, "config", "user.email", "test@example.invalid")
    (repo / "file.txt").write_text("base\n", encoding="utf-8")
    _git(repo, "add", "file.txt")
    _git(repo, "commit", "-q", "-m", "chore: establish fixture")
    base_sha = _git(repo, "rev-parse", "HEAD")
    (repo / "file.txt").write_text("feature\n", encoding="utf-8")
    _git(repo, "commit", "-qam", "fix(audio): retain recording metadata")
    head_sha = _git(repo, "rev-parse", "HEAD")
    monkeypatch.chdir(repo)

    assert main(["--base-sha", base_sha, "--head-sha", head_sha]) == 0
    captured = capsys.readouterr()
    assert "All 1 commit message(s) are releasable." in captured.out
    assert captured.err == ""

    with pytest.raises(subprocess.CalledProcessError):
        commit_messages(base_sha, "missing-head-for-test")


def test_main_message_file_routes_success_and_failure_to_expected_streams(
    tmp_path: Path, capsys: CaptureFixture[str]
) -> None:
    valid = tmp_path / "valid.txt"
    valid.write_text("fix(audio): retain capture\n\nKeep the sample.", encoding="utf-8")
    assert main(["--message-file", str(valid)]) == 0
    captured = capsys.readouterr()
    assert "All 1 commit message(s) are releasable." in captured.out
    assert captured.err == ""

    invalid = tmp_path / "invalid.txt"
    invalid.write_text("bad subject", encoding="utf-8")
    assert main(["--message-file", str(invalid)]) == 1
    captured = capsys.readouterr()
    assert captured.out == ""
    assert "not a Conventional Commit subject" in captured.err


def test_main_requires_a_commit_source_before_file_or_git_io(
    monkeypatch: MonkeyPatch, capsys: CaptureFixture[str]
) -> None:
    def unexpected_io(*_args: object, **_kwargs: object) -> Never:
        raise AssertionError("missing CLI arguments must fail before input IO")

    monkeypatch.setattr(Path, "read_text", unexpected_io)
    monkeypatch.setattr(subprocess, "run", unexpected_io)

    with pytest.raises(SystemExit) as missing_source:
        main([])
    assert missing_source.value.code == 2
    assert "one of the arguments --base-sha --message-file is required" in capsys.readouterr().err

    with pytest.raises(SystemExit) as missing_head:
        main(["--base-sha", "base"])
    assert missing_head.value.code == 2
    assert "--head-sha is required with --base-sha" in capsys.readouterr().err

    with pytest.raises(SystemExit) as missing_base:
        main(["--head-sha", "HEAD"])
    assert missing_base.value.code == 2
    assert "one of the arguments --base-sha --message-file is required" in capsys.readouterr().err


@pytest.mark.parametrize("contents", ["", "# editor comment\n"])
def test_blank_message_is_rejected_by_validator_and_message_file(
    contents: str, tmp_path: Path, capsys: CaptureFixture[str]
) -> None:
    ok, problems = validate_commit_messages([""])
    assert not ok
    assert any("not a Conventional Commit subject" in problem for problem in problems)

    message_file = tmp_path / "blank-message.txt"
    message_file.write_text(contents, encoding="utf-8")
    assert main(["--message-file", str(message_file)]) == 1
    captured = capsys.readouterr()
    assert captured.out == ""
    assert "not a Conventional Commit subject" in captured.err
