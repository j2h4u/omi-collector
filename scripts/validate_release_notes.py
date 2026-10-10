"""Validate the release-note contract a pull request owes release-please."""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path
from typing import cast

from scripts.validate_pr_commits import commit_messages
from scripts.validate_pr_title import RELEASABLE_TYPES, TITLE_PATTERN

BEGIN_MARKER = "BEGIN_COMMIT_OVERRIDE"
END_MARKER = "END_COMMIT_OVERRIDE"
BREAKING_MARKER = re.compile(r"BREAKING(?: |-)CHANGE:")
RELEASE_AS = re.compile(r"^Release-As:", re.IGNORECASE)


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
        marker = BREAKING_MARKER.search(line)
        if marker is None:
            continue
        remainder = lines[index + 1 :]
        if remainder and not remainder[0].strip() and any(item.strip() for item in remainder):
            problems.append(
                f"'{subject}' has a blank line directly after '{marker.group()}'. "
                "Release-please ends the breaking-change note there, dropping everything below it. "
                "Put the bullets on the very next line."
            )

    return problems


def _requires_override(messages: list[str]) -> bool:
    for message in messages:
        lines = message.splitlines()
        match = TITLE_PATTERN.fullmatch(lines[0].strip()) if lines else None
        has_body = any(line.strip() for line in lines[1:])
        if (match is not None and match.group("breaking") and has_body) or any(
            BREAKING_MARKER.search(line) or RELEASE_AS.match(line) for line in lines[1:]
        ):
            return True
    return False


def _validate_preserved_metadata(source: list[str], override: list[str]) -> list[str]:
    source_text = "\n".join(source)
    override_text = "\n".join(override)
    problems: list[str] = []

    for line in source_text.splitlines():
        if BREAKING_MARKER.search(line) and line not in override_text.splitlines():
            problems.append(f"The override must preserve the breaking-change note {line!r}.")
        match = RELEASE_AS.match(line)
        if match is not None and line not in override_text.splitlines():
            problems.append(f"The override must preserve {line!r}.")

    for message in source:
        lines = message.splitlines()
        match = TITLE_PATTERN.fullmatch(lines[0].strip()) if lines else None
        if match is None or not match.group("breaking"):
            continue
        candidates = [entry for entry in override if _is_breaking_entry(entry)]
        if not candidates:
            problems.append(
                "The override must keep a Conventional Commit breaking marker for a breaking source commit."
            )
        if any(line.strip() for line in lines[1:]) and not any(
            any(line.strip() for line in entry.splitlines()[1:]) for entry in candidates
        ):
            problems.append("The override must include the explanatory body of a breaking source commit.")

    return problems


def _is_breaking_entry(message: str) -> bool:
    lines = message.splitlines()
    match = TITLE_PATTERN.fullmatch(lines[0].strip()) if lines else None
    return (match is not None and bool(match.group("breaking"))) or any(
        BREAKING_MARKER.search(line) for line in lines[1:]
    )


def validate_release_notes(
    body: str, commit_count: int, require_above: int, messages: list[str] | None = None
) -> tuple[bool, list[str]]:
    block = _extract_override(body)
    source_messages = messages
    override_required = commit_count > require_above or (messages is not None and _requires_override(messages))

    if block is None:
        if override_required:
            return False, [
                (
                    "This squash would lose release information from its commit bodies."
                    if messages is not None and _requires_override(messages)
                    else f"This PR squashes {commit_count} commits into one, so its title cannot describe all of them "
                    "and the changelog would render a single line."
                ),
                (
                    f"Add a {BEGIN_MARKER} / {END_MARKER} block to the PR description listing what shipped, "
                    "one Conventional Commit message per entry, separated by blank lines."
                ),
            ]
        return True, [f"No override block, and none owed for {commit_count} commit(s)."]

    override_entries = _split_messages(block)
    if not override_entries:
        return False, [f"The {BEGIN_MARKER} block is empty."]

    problems = [problem for message in override_entries for problem in _validate_message(message)]
    if source_messages is not None:
        problems.extend(_validate_preserved_metadata(source_messages, override_entries))
    if problems:
        return False, problems

    return True, [f"Override block parses into {len(override_entries)} changelog entr(ies)."]


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
    parser.add_argument("--base-sha", help="Base commit for reading release-relevant commit bodies.")
    parser.add_argument("--head-sha", help="Head commit for reading release-relevant commit bodies.")
    args = parser.parse_args(argv)

    base_sha = cast("str | None", args.base_sha)
    head_sha = cast("str | None", args.head_sha)
    if (base_sha is None) != (head_sha is None):
        parser.error("--base-sha and --head-sha must be supplied together")

    body_file = cast("str", args.body_file)
    body = sys.stdin.read() if body_file == "-" else Path(body_file).read_text(encoding="utf-8")
    commit_texts = None if base_sha is None else commit_messages(base_sha, cast("str", head_sha))
    ok, reported = validate_release_notes(
        body, cast("int", args.commit_count), cast("int", args.require_above), commit_texts
    )
    stream = sys.stdout if ok else sys.stderr
    for message in reported:
        print(message, file=stream)
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
