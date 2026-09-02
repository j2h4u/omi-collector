"""Validate the release-note contract a pull request owes release-please."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import cast

from scripts.validate_pr_title import RELEASABLE_TYPES, TITLE_PATTERN

BEGIN_MARKER = "BEGIN_COMMIT_OVERRIDE"
END_MARKER = "END_COMMIT_OVERRIDE"
BREAKING_PREFIX = "BREAKING CHANGE:"


def _extract_override(body: str) -> str | None:
    if BEGIN_MARKER not in body:
        return None
    after_begin = body.split(BEGIN_MARKER, 1)[1]
    if END_MARKER not in after_begin:
        return None
    return after_begin.split(END_MARKER, 1)[0].strip("\n")


def _split_messages(block: str) -> list[str]:
    messages: list[str] = []
    current: list[str] = []
    previous_blank = True

    for line in block.splitlines():
        starts_message = (
            previous_blank
            and TITLE_PATTERN.fullmatch(line.strip()) is not None
            and not line.startswith((" ", "\t", "*", "-"))
        )
        if starts_message and current:
            messages.append("\n".join(current).strip("\n"))
            current = []
        current.append(line)
        previous_blank = not line.strip()

    if current:
        messages.append("\n".join(current).strip("\n"))
    return [message for message in messages if message.strip()]


def _validate_message(message: str) -> list[str]:
    problems: list[str] = []
    lines = message.splitlines()
    subject = lines[0].strip()

    match = TITLE_PATTERN.fullmatch(subject)
    if match is None:
        return [f"'{subject}' is not a Conventional Commit subject."]

    commit_type = match.group("type")
    if commit_type not in RELEASABLE_TYPES:
        allowed = ", ".join(sorted(RELEASABLE_TYPES))
        problems.append(f"'{subject}' uses unsupported type '{commit_type}'. Allowed types: {allowed}.")

    for index, line in enumerate(lines):
        if not line.startswith(BREAKING_PREFIX):
            continue
        remainder = lines[index + 1 :]
        if remainder and not remainder[0].strip() and any(item.strip() for item in remainder):
            problems.append(
                f"'{subject}' has a blank line directly after '{BREAKING_PREFIX}'. "
                "Release-please ends the breaking-change note there, dropping everything below it. "
                "Put the bullets on the very next line."
            )

    return problems


def validate_release_notes(body: str, commit_count: int, require_above: int) -> tuple[bool, list[str]]:
    block = _extract_override(body)

    if block is None:
        if commit_count > require_above:
            return False, [
                (
                    f"This PR squashes {commit_count} commits into one, so its title cannot describe all of them "
                    "and the changelog would render a single line."
                ),
                (
                    f"Add a {BEGIN_MARKER} / {END_MARKER} block to the PR description listing what shipped, "
                    "one Conventional Commit message per entry, separated by blank lines."
                ),
            ]
        return True, [f"No override block, and none owed for {commit_count} commit(s)."]

    messages = _split_messages(block)
    if not messages:
        return False, [f"The {BEGIN_MARKER} block is empty."]

    problems = [problem for message in messages for problem in _validate_message(message)]
    if problems:
        return False, problems

    return True, [f"Override block parses into {len(messages)} changelog entr(ies)."]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--body-file", required=True, help="File holding the PR description ('-' for stdin).")
    parser.add_argument("--commit-count", required=True, type=int, help="Commits the merge will squash.")
    parser.add_argument(
        "--require-above",
        default=1,
        type=int,
        help="Demand an override block when the PR has more commits than this.",
    )
    args = parser.parse_args(argv)

    body_file = cast("str", args.body_file)
    body = sys.stdin.read() if body_file == "-" else Path(body_file).read_text(encoding="utf-8")
    ok, messages = validate_release_notes(body, cast("int", args.commit_count), cast("int", args.require_above))
    stream = sys.stdout if ok else sys.stderr
    for message in messages:
        print(message, file=stream)
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
