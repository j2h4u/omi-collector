"""Run pinned offline clone detectors against an isolated, explicit inventory."""

import ast
import hashlib
import io
import json
import re
import shutil
import subprocess
import tempfile
import tokenize
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import cast

MAX_PAIRS = 10_000
PYSCN_SCHEMA_VERSION = 2
MAX_SOURCE_BYTES = 1_048_576
TIMEOUT_SECONDS = 360
POLICY: dict[str, object] = {
    "pyscn": {
        "version": "1.33.0",
        "min_lines": 8,
        "min_nodes": 20,
        "max_edit_distance": 50,
        "ignore_literals": False,
        "ignore_identifiers": False,
        "cost_model_type": "python",
        "type1_threshold": 0.85,
        "type2_threshold": 0.75,
        "type3_threshold": 0.7,
        "type4_threshold": 0.65,
        "similarity_threshold": 0.65,
        "enable_dfa": True,
        "enabled_clone_types": ["type1", "type2", "type3", "type4"],
        "min_similarity": 0.0,
        "max_similarity": 1.0,
        "max_results": 0,
        "grouping_mode": "complete_linkage",
        "grouping_threshold": 0.65,
        "k_core_k": 2,
        "lsh_enabled": "false",
        "max_memory_mb": 0,
        "enable_batching": False,
        "max_goroutines": 1,
        "timeout_seconds": 300,
        "exclude_patterns": ["**/*.pyc"],
    },
    "jscpd": {
        "version": "5.4.1",
        "minTokens": 50,
        "minLines": 5,
        "maxLines": 1_000_000,
        "mode": "weak",
        "ignoreIdentifiers": True,
        "ignoreLiterals": True,
        "maxGapLines": 2,
        "similarity": 0.8,
        "minNodes": 20,
        "format": ["python"],
        "gitignore": False,
        "absolute": True,
        "maxSize": "1mb",
        "reporters": ["json"],
    },
}
IGNORE_COMMENT = re.compile(
    r"(?:jscpd\s*:\s*ignore|cpd\s*[-_: ]\s*(?:off|on|disable|enable)|pyscn\s*:\s*ignore)", re.IGNORECASE
)
IGNORE_RAW = re.compile(r"jscpd\s*:\s*ignore", re.IGNORECASE)


@dataclass(frozen=True)
class CloneFinding:
    detector: str
    identity: str
    locations: tuple[str, str]


def _object(value: object) -> dict[str, object]:
    if not isinstance(value, dict) or not all(isinstance(key, str) for key in value):
        raise ValueError("Expected a JSON object")
    return cast("dict[str, object]", value)


def _array(value: object) -> list[object]:
    if not isinstance(value, list):
        raise ValueError("Expected a JSON array")
    return cast("list[object]", value)


def _integer(value: object) -> int:
    if type(value) is not int or value < 0:
        raise ValueError("Expected a nonnegative integer")
    return value


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise ValueError(message)


def inventory(root: Path, roots: tuple[str, ...]) -> dict[str, str]:
    """Parse all scoped files independently, before either native detector."""
    paths: set[Path] = set()
    for name in roots:
        directory = root / name
        _require(directory.is_dir(), f"Missing source root: {name}")
        _require(not directory.is_symlink(), f"Source directory symlink: {name}")
        _require(directory.resolve().is_relative_to(root.resolve()), f"Source root escapes repository: {name}")
        for entry in directory.rglob("*"):
            _require(not (entry.is_symlink() and entry.is_dir()), f"Source directory symlink: {entry}")
            if entry.suffix == ".py":
                paths.add(entry)
    sources: dict[str, str] = {}
    for path in sorted(paths):
        _require(path.resolve().is_relative_to(root.resolve()), f"Source symlink escapes repository: {path}")
        raw = path.read_bytes()
        _require(len(raw) <= MAX_SOURCE_BYTES, f"Source exceeds detector size limit: {path}")
        text = raw.decode("utf-8")
        ast.parse(text, filename=str(path))
        # jscpd matches raw source markers even inside Python string literals.
        _require(IGNORE_RAW.search(text) is None, f"Raw clone suppression: {path}")
        comments = (
            token.string
            for token in tokenize.generate_tokens(io.StringIO(text).readline)
            if token.type == tokenize.COMMENT
        )
        _require(not any(IGNORE_COMMENT.search(comment) for comment in comments), f"Inline clone suppression: {path}")
        sources[path.relative_to(root).as_posix()] = text
    return sources


def _endpoint(location: dict[str, object], staged: Path, sources: dict[str, str]) -> tuple[str, str]:
    filename = location["file_path"]
    _require(isinstance(filename, str), "Invalid endpoint filename")
    path = Path(cast("str", filename))
    absolute = path if path.is_absolute() else staged / path
    _require(absolute.resolve().is_relative_to(staged.resolve()), "Endpoint escapes staged inventory")
    relative = absolute.resolve().relative_to(staged.resolve()).as_posix()
    _require(relative in sources, f"Unknown endpoint: {relative}")
    start, end = _integer(location["start_line"]), _integer(location["end_line"])
    lines = sources[relative].splitlines(keepends=True)
    _require(1 <= start <= end <= len(lines), f"Invalid endpoint span: {relative}:{start}-{end}")
    digest = hashlib.sha256("".join(lines[start - 1 : end]).encode("utf-8")).hexdigest()
    return digest, f"{relative}:{start}-{end}"


def _finding(
    detector: str, endpoints: tuple[dict[str, object], dict[str, object]], staged: Path, sources: dict[str, str]
) -> CloneFinding:
    first, second = (_endpoint(endpoint, staged, sources) for endpoint in endpoints)
    _require(first[1] != second[1], "Clone pair repeats the same endpoint")
    payload = json.dumps([detector, *sorted((first[0], second[0]))], separators=(",", ":"))
    identity = hashlib.sha256(payload.encode("utf-8")).hexdigest()
    locations = sorted((first[1], second[1]))
    return CloneFinding(detector, identity, (locations[0], locations[1]))


def _no_errors(report: dict[str, object]) -> None:
    for key in ("errors", "diagnostics", "parseErrors", "skippedFiles"):
        if key in report and report[key] is not None:
            _require(not _array(report[key]), f"Detector reported {key}: {report[key]}")
    _require(not report.get("error"), f"Detector error: {report.get('error')}")


def parse_pyscn(report: dict[str, object], staged: Path, sources: dict[str, str]) -> list[CloneFinding]:
    _require(
        report["schema_version"] == PYSCN_SCHEMA_VERSION and report["version"] == "1.33.0",
        "Unsupported PySCN report version",
    )
    _no_errors(report)
    clone, summary = _object(report["clone"]), _object(report["summary"])
    _no_errors(clone)
    _require(clone["success"] is True, "PySCN clone analysis failed")
    stats = _object(clone["statistics"])
    for key in ("total_files", "analyzed_files"):
        _require(_integer(summary[key]) == len(sources), f"PySCN {key} does not match inventory")
    _require(_integer(summary["skipped_files"]) == 0, "PySCN skipped input")
    _require(_integer(stats["files_analyzed"]) == len(sources), "PySCN clone file count mismatch")
    pairs = [] if clone["clone_pairs"] is None else _array(clone["clone_pairs"])
    _require(len(pairs) < MAX_PAIRS, "PySCN pair capacity reached; report may be truncated")
    _require(_integer(stats["total_clone_pairs"]) == len(pairs), "PySCN pair count mismatch")
    _require(_integer(summary["clone_pairs"]) == len(pairs), "PySCN summary pair count mismatch")
    # Authorization binds source fragments, not unused scoring or grouping metadata.
    return [
        _finding(
            "pyscn",
            (_object(_object(pair["clone1"])["location"]), _object(_object(pair["clone2"])["location"])),
            staged,
            sources,
        )
        for pair in map(_object, pairs)
    ]


def parse_jscpd(report: dict[str, object], staged: Path, sources: dict[str, str]) -> list[CloneFinding]:
    """Validate native output; thresholds can intentionally exclude tiny files."""
    _no_errors(report)
    duplicates = _array(report["duplicates"])
    total = _object(_object(report["statistics"])["total"])
    _require(_integer(total["clones"]) == len(duplicates), "jscpd clone count mismatch")
    _require(_integer(total["sources"]) <= len(sources), "jscpd source count exceeds inventory")
    return [_jscpd_pair(_object(pair), staged, sources) for pair in duplicates]


def _jscpd_pair(pair: dict[str, object], staged: Path, sources: dict[str, str]) -> CloneFinding:
    _require(pair["format"] == "python", "Unexpected clone language")
    locations = [_object(pair[key]) for key in ("firstFile", "secondFile")]
    endpoints = tuple(
        {"file_path": item["name"], "start_line": item["start"], "end_line": item["end"]} for item in locations
    )
    return _finding("jscpd", (endpoints[0], endpoints[1]), staged, sources)


def _run(arguments: list[str], cwd: Path) -> str:
    result = subprocess.run(arguments, cwd=cwd, capture_output=True, text=True, timeout=TIMEOUT_SECONDS, check=False)
    if result.returncode:
        raise RuntimeError(f"{arguments[0]} failed ({result.returncode}): {result.stderr}\n{result.stdout}")
    return result.stdout


def _binaries(staged: Path) -> dict[str, str]:
    binaries: dict[str, str] = {}
    for name, expected in (("pyscn", "pyscn version 1.33.0"), ("jscpd", "jscpd 5.4.1")):
        binary = shutil.which(name)
        _require(binary is not None, f"Missing locked tool: {name}")
        binaries[name] = cast("str", binary)
        _require(_run([binaries[name], "--version"], staged).strip() == expected, f"Unexpected {name} version")
    return binaries


def _pyscn_config() -> str:
    settings = _object(POLICY["pyscn"])
    # An empty exclusion list restores PySCN defaults, which silently omit test_*.py.
    lines = ["[analysis]", 'exclude_patterns = ["**/*.pyc"]', "[clones]"]
    lines.extend(f"{key} = {json.dumps(value)}" for key, value in settings.items() if key != "version")
    return "\n".join(lines) + "\n"


def _scan(staged: Path, sources: dict[str, str], output: Path) -> list[CloneFinding]:
    binaries = _binaries(staged)
    pyscn_config = staged / "pyscn.toml"
    pyscn_config.write_text(_pyscn_config(), encoding="utf-8")
    jscpd_config = staged / "jscpd.json"
    settings = {key: value for key, value in _object(POLICY["jscpd"]).items() if key != "version"}
    jscpd_config.write_text(json.dumps(settings), encoding="utf-8")
    files = list(sources)
    _run(
        [
            binaries["pyscn"],
            "analyze",
            "--select",
            "clones",
            "--json",
            "--config",
            str(pyscn_config),
            "--output",
            str(output / "pyscn.json"),
            *files,
        ],
        staged,
    )
    _run(
        [
            binaries["jscpd"],
            "--config",
            str(jscpd_config),
            "--no-gitignore",
            "--absolute",
            "--output",
            str(output),
            *files,
        ],
        staged,
    )
    pyscn = _object(cast("object", json.loads((output / "pyscn.json").read_text(encoding="utf-8"))))
    jscpd = _object(cast("object", json.loads((output / "jscpd-report.json").read_text(encoding="utf-8"))))
    return [*parse_pyscn(pyscn, staged, sources), *parse_jscpd(jscpd, staged, sources)]


def analyze(root: Path, roots: tuple[str, ...], output: Path) -> list[CloneFinding]:
    output = output.resolve()
    output.mkdir(parents=True, exist_ok=True)
    # Fresh reports cannot be mistaken for a previous successful analysis after failure.
    for name in ("pyscn.json", "jscpd-report.json", "findings.json"):
        (output / name).unlink(missing_ok=True)
    sources = inventory(root, roots)
    with tempfile.TemporaryDirectory(prefix="clone-scan-") as temporary:
        staged = Path(temporary)
        for name, text in sources.items():
            path = staged / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(text.encode("utf-8"))
        findings = _scan(staged, sources, output) if sources else []
    normalized = {"input_files": len(sources), "policy": POLICY, "findings": [asdict(finding) for finding in findings]}
    (output / "findings.json").write_text(json.dumps(normalized, indent=2) + "\n", encoding="utf-8")
    return findings
