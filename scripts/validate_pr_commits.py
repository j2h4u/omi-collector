"""Validate that PR commits are parseable release-please input."""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path
from typing import cast

from scripts.validate_pr_title import RELEASABLE_TYPES, TITLE_PATTERN

BULLET_MARKERS = ("- ", "* ", "+ ")
RECORD_SEPARATOR_FORMAT = "%x00"
RECORD_SEPARATOR = "\x00"
SCISSORS = "# ------------------------ >8 ------------------------"


def commit_messages(base_sha: str, head_sha: str) -> list[str]:
    result = subprocess.run(
        ["/usr/bin/git", "log", "--no-merges", f"--format=%B{RECORD_SEPARATOR_FORMAT}", f"{base_sha}..{head_sha}"],
        capture_output=True,
        check=True,
        text=True,
    )
    return [message.strip("\n") for message in result.stdout.split(RECORD_SEPARATOR) if message.strip()]


def _validate_message(message: str) -> list[str]:
    problems: list[str] = []
    lines = message.splitlines()
    subject = lines[0].strip()

    match = TITLE_PATTERN.fullmatch(subject)
    if match is None:
        problems.append(f"'{subject}' is not a Conventional Commit subject.")
    else:
        commit_type = match.group("type")
        if commit_type not in RELEASABLE_TYPES:
            allowed = ", ".join(sorted(RELEASABLE_TYPES))
            problems.append(f"'{subject}' uses unsupported type '{commit_type}'. Allowed types: {allowed}.")

    bullets = [line for line in lines[1:] if line.startswith(BULLET_MARKERS)]
    if bullets:
        problems.append(
            f"'{subject}' has a Markdown bullet at column 0 ({bullets[0]!r}). "
            "Release-please reads the marker as a commit type and can drop the commit from the changelog. "
            "Indent body bullets by two spaces."
        )

    return problems


def validate_commit_messages(messages: list[str]) -> tuple[bool, list[str]]:
    if not messages:
        return True, ["No non-merge commits to validate."]

    problems = [problem for message in messages for problem in _validate_message(message)]
    if problems:
        return False, problems
    return True, [f"All {len(messages)} commit message(s) are releasable."]


def editable_message(raw: str) -> str:
    lines: list[str] = []
    for line in raw.split("\n"):
        if line.rstrip() == SCISSORS:
            break
        if not line.startswith("#"):
            lines.append(line)
    return "\n".join(lines).strip("\n")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--base-sha", help="Base commit the PR branches from.")
    source.add_argument("--message-file", help="File holding one commit message.")
    parser.add_argument("--head-sha", help="Head commit of the PR. Required with --base-sha.")
    args = parser.parse_args(argv)

    message_file = cast("str | None", args.message_file)
    head_sha = cast("str | None", args.head_sha)
    if message_file is not None:
        messages = [editable_message(Path(message_file).read_text(encoding="utf-8"))]
    else:
        if head_sha is None:
            parser.error("--head-sha is required with --base-sha")
        messages = commit_messages(cast("str", args.base_sha), head_sha)

    ok, reported = validate_commit_messages(messages)
    stream = sys.stdout if ok else sys.stderr
    for message in reported:
        print(message, file=stream)
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
