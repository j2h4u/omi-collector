"""Require explicit, current review dispositions for both clone detectors."""

import argparse
import json
import sys
from collections import Counter
from dataclasses import asdict
from pathlib import Path
from typing import cast

from scripts.clone_tools import POLICY, CloneFinding, analyze

SOURCE_ROOTS = ("src", "scripts")
LEDGER_NAME = ".clone-acceptances.json"
SCHEMA_VERSION = 1
MAX_DISPLAY_FINDINGS = 20


def contract() -> dict[str, object]:
    return {"identity": "raw-pair-v1", "roots": list(SOURCE_ROOTS), "tools": POLICY}


def _table(value: object) -> dict[str, object]:
    if not isinstance(value, dict):
        raise ValueError("clone acceptance data must be an object")
    return cast(dict[str, object], value)


def _record(value: object) -> tuple[tuple[str, str], int]:
    data = _table(value)
    if set(data) != {"detector", "identity", "count", "reason"}:
        raise ValueError("each acceptance needs exactly detector, identity, count, reason")
    detector, identity = data["detector"], data["identity"]
    if detector not in ("pyscn", "jscpd") or not isinstance(identity, str) or not identity:
        raise ValueError("acceptance has invalid detector or identity")
    count = data["count"]
    if type(count) is not int or count <= 0:
        raise ValueError("acceptance count must be a positive integer")
    reason = data["reason"]
    if not isinstance(reason, str) or not reason.strip():
        raise ValueError("acceptance requires a nonempty review reason")
    return (str(detector), identity), count


def _accepted(path: Path) -> Counter[tuple[str, str]]:
    data = _table(cast(object, json.loads(path.read_text(encoding="utf-8"))))
    if set(data) != {"schema_version", "contract", "accepted"}:
        raise ValueError("invalid acceptance file fields")
    if type(data["schema_version"]) is not int or data["schema_version"] != SCHEMA_VERSION:
        raise ValueError("unsupported acceptance schema")
    if data["contract"] != contract():
        raise ValueError("clone policy changed: review and explicitly migrate the acceptance contract")
    records = data["accepted"]
    if not isinstance(records, list):
        raise ValueError("accepted must be a list")
    result: Counter[tuple[str, str]] = Counter()
    for value in records:
        key, count = _record(value)
        if key in result:
            raise ValueError("duplicate acceptance record")
        result[key] = count
    return result


def validate_acceptances(path: Path, findings: list[CloneFinding]) -> list[str]:
    accepted = _accepted(path)
    current = Counter((finding.detector, finding.identity) for finding in findings)
    errors: list[str] = []
    for key in sorted(current.keys() | accepted.keys()):
        if current[key] != accepted[key]:
            status = "unreviewed" if current[key] > accepted[key] else "stale acceptance"
            errors.append(f"{status}: {key[0]} {key[1]} current={current[key]} accepted={accepted[key]}")
    return errors


def _write_review(output: Path, findings: list[CloneFinding]) -> None:
    counts = Counter((finding.detector, finding.identity) for finding in findings)
    records = [
        {"detector": detector, "identity": identity, "count": count, "reason": ""}
        for (detector, identity), count in sorted(counts.items())
    ]
    report = {
        "schema_version": SCHEMA_VERSION,
        "contract": contract(),
        "findings": [asdict(finding) for finding in findings],
        "review_candidates": records,
    }
    output.mkdir(parents=True, exist_ok=True)
    (output / "review.json").write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")


def check(root: Path, output: Path) -> int:
    findings = analyze(root, SOURCE_ROOTS, output)
    _write_review(output, findings)
    errors = validate_acceptances(root / LEDGER_NAME, findings)
    if errors:
        for error in errors:
            print(error, file=sys.stderr)
        for finding in findings[:MAX_DISPLAY_FINDINGS]:
            print(f"  {finding.detector}: {finding.locations[0]} ~ {finding.locations[1]}", file=sys.stderr)
        print(f"Inspect {output / 'review.json'}; fix duplication or record a reason for each accepted pair.")
        return 1
    print(f"Clone gate passed: {len(findings)} reviewed findings across both detectors")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=Path(".clone-reports"))
    args = parser.parse_args(argv)
    root = Path(__file__).resolve().parents[1]
    try:
        return check(root, root / cast(Path, args.output))
    except (OSError, ValueError, RuntimeError) as exc:
        print(f"Clone gate failed: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
