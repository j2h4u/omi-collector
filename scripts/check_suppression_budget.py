"""Fail when inline suppression counts exceed their initial historical ceilings."""

from __future__ import annotations

import re
import sys
import tomllib
from dataclasses import dataclass
from pathlib import Path
from typing import cast

CONFIG_TABLE = ("tool", "omi_collector", "suppressions")
DEFAULT_SCAN_PATHS = ("src", "scripts", "tests", "pyproject.toml", "Justfile", ".github/workflows")
SUPPRESSION_PATTERNS = {
    "noqa": re.compile(r"#\s*noqa(?::|\b)", re.IGNORECASE),
    "type_ignore": re.compile(r"#\s*type:\s*ignore(?:\[|\b)"),
    "pyright": re.compile(r"#\s*pyright:\s*ignore(?:\[|\b)"),
    "pylint": re.compile(r"#\s*pylint:\s*disable="),
    "ruff": re.compile(r"#\s*ruff:\s*noqa(?::|\b)", re.IGNORECASE),
}
TEXT_SUFFIXES = {".py", ".toml", ".yml", ".yaml"}


@dataclass(frozen=True, slots=True)
class Finding:
    category: str
    path: Path
    line_number: int
    line: str


def _table(value: object, context: str) -> dict[str, object]:
    if not isinstance(value, dict):
        raise TypeError(context)
    return cast(dict[str, object], value)


def _config(root: Path) -> dict[str, object]:
    pyproject = _table(
        tomllib.loads((root / "pyproject.toml").read_text(encoding="utf-8")), "pyproject must be a table"
    )
    current: object = pyproject
    for key in CONFIG_TABLE:
        if not isinstance(current, dict) or key not in current:
            raise ValueError("missing [tool.omi_collector.suppressions] configuration")
        current = current[key]
    return _table(current, "[tool.omi_collector.suppressions] must be a table")


def _scan_paths(root: Path, config: dict[str, object]) -> list[Path]:
    raw_paths = config.get("paths", list(DEFAULT_SCAN_PATHS))
    if not isinstance(raw_paths, list) or not all(isinstance(path, str) and path for path in raw_paths):
        raise TypeError("[tool.omi_collector.suppressions].paths must be a list of nonempty strings")
    paths = [root / path for path in cast(list[str], raw_paths)]
    if any(not path.resolve().is_relative_to(root.resolve()) for path in paths):
        raise ValueError("suppression scan paths must stay inside the repository")
    if any(not path.exists() for path in paths):
        raise ValueError("a configured suppression scan path does not exist")
    return paths


def _baseline(config: dict[str, object]) -> dict[str, int]:
    raw_baseline = config.get("baseline")
    if not isinstance(raw_baseline, dict) or set(raw_baseline) != set(SUPPRESSION_PATTERNS):
        raise TypeError("suppression baseline must define every category exactly once")
    baseline: dict[str, int] = {}
    for category in SUPPRESSION_PATTERNS:
        value = raw_baseline[category]
        if type(value) is not int or value < 0:
            raise TypeError(f"suppression baseline for {category} must be a non-negative integer")
        baseline[category] = value
    return baseline


def _is_text_candidate(path: Path) -> bool:
    return path.name == "Justfile" or path.suffix in TEXT_SUFFIXES


def _iter_files(path: Path) -> list[Path]:
    if path.is_file():
        return [path] if _is_text_candidate(path) else []
    if not path.exists():
        return []
    return sorted(item for item in path.rglob("*") if item.is_file() and _is_text_candidate(item))


def _scan_file(root: Path, path: Path) -> list[Finding]:
    findings: list[Finding] = []
    for line_number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        for category, pattern in SUPPRESSION_PATTERNS.items():
            if pattern.search(line):
                findings.append(Finding(category, path.relative_to(root), line_number, line.strip()))
    return findings


def scan(root: Path, paths: list[Path]) -> list[Finding]:
    root = root.resolve()
    findings: list[Finding] = []
    for path in paths:
        for candidate in _iter_files(path):
            if not candidate.resolve().is_relative_to(root):
                raise ValueError(f"suppression scan file escapes repository: {candidate}")
            findings.extend(_scan_file(root, candidate))
    return findings


def check(root: Path) -> int:
    config = _config(root)
    baseline = _baseline(config)
    findings = scan(root, _scan_paths(root, config))
    counts = dict.fromkeys(SUPPRESSION_PATTERNS, 0)
    for finding in findings:
        counts[finding.category] += 1

    offenders = [category for category, count in counts.items() if count > baseline[category]]
    if offenders:
        print("Suppression budget failed:", file=sys.stderr)
        for category in offenders:
            print(f"  {category}: {counts[category]} found, budget {baseline[category]}", file=sys.stderr)
        for finding in findings:
            if finding.category in offenders:
                print(f"  {finding.path}:{finding.line_number}: {finding.category}: {finding.line}", file=sys.stderr)
        return 1

    summary = ", ".join(f"{category}={counts[category]}/{baseline[category]}" for category in sorted(counts))
    print(f"Suppression budget passed: {summary}")
    return 0


def main() -> int:
    root = Path(__file__).resolve().parents[1]
    try:
        return check(root)
    except (OSError, TypeError, ValueError, tomllib.TOMLDecodeError) as exc:
        print(f"Suppression budget could not be checked: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
