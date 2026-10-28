"""Fail-closed report parsing and real pinned-binary scenarios."""

import json
import subprocess
from collections import Counter
from pathlib import Path
from typing import cast

import pytest
from scripts import clone_tools

SOURCES = {"src/a.py": "alpha = 1\nbeta = 2\n", "src/b.py": "gamma = 3\ndelta = 4\n"}
FUNCTION = """def calculate(values):
    total = 0
    count = 0
    for value in values:
        if value > 0:
            total += value * 2
            count += 1
        else:
            total -= value
    return total / max(count, 1)
"""


def jscpd_report() -> dict[str, object]:
    return {
        "duplicates": [
            {
                "firstFile": {"name": "src/a.py", "start": 1, "end": 2},
                "secondFile": {"name": "src/b.py", "start": 1, "end": 2},
                "format": "python",
                "lines": 2,
                "tokens": 10,
            }
        ],
        "statistics": {"total": {"sources": 2, "clones": 1}},
    }


def pyscn_report() -> dict[str, object]:
    return {
        "schema_version": 2,
        "version": "1.33.0",
        "clone": {
            "success": True,
            "clones": [],
            "clone_pairs": None,
            "clone_groups": None,
            "statistics": {"files_analyzed": 2, "total_clones": 0, "total_clone_pairs": 0, "total_clone_groups": 0},
        },
        "summary": {
            "total_files": 2,
            "analyzed_files": 2,
            "skipped_files": 0,
            "clone_pairs": 0,
            "total_clones": 0,
            "clone_groups": 0,
        },
    }


def test_identity_survives_movement_reversal_and_preceding_lines(tmp_path: Path) -> None:
    original = clone_tools.parse_jscpd(jscpd_report(), tmp_path, SOURCES)[0]
    report = jscpd_report()
    pair = cast("list[dict[str, object]]", report["duplicates"])[0]
    pair["firstFile"], pair["secondFile"] = pair["secondFile"], pair["firstFile"]
    assert clone_tools.parse_jscpd(report, tmp_path, SOURCES)[0].identity == original.identity
    pair["firstFile"] = {"name": "moved.py", "start": 2, "end": 3}
    moved = {"src/a.py": SOURCES["src/a.py"], "moved.py": "\n" + SOURCES["src/b.py"]}
    assert clone_tools.parse_jscpd(report, tmp_path, moved)[0].identity == original.identity


@pytest.mark.parametrize("name", ["src/a.py", "src/b.py"])
def test_raw_endpoint_edit_requires_new_identity(tmp_path: Path, name: str) -> None:
    original = clone_tools.parse_jscpd(jscpd_report(), tmp_path, SOURCES)[0]
    changed = SOURCES | {name: SOURCES[name].replace(" = ", "  = ")}
    assert clone_tools.parse_jscpd(jscpd_report(), tmp_path, changed)[0].identity != original.identity


@pytest.mark.parametrize(
    "field,value",
    [("name", "../escape.py"), ("name", "src/unknown.py"), ("start", 0), ("start", True), ("end", 3), ("end", "2")],
)
def test_invalid_endpoints_fail(tmp_path: Path, field: str, value: object) -> None:
    report = jscpd_report()
    pair = cast("list[dict[str, object]]", report["duplicates"])[0]
    endpoint = cast("dict[str, object]", pair["firstFile"])
    endpoint[field] = value
    with pytest.raises(ValueError):
        clone_tools.parse_jscpd(report, tmp_path, SOURCES)


@pytest.mark.parametrize(
    "field,value", [("duplicates", None), ("duplicates", {}), ("errors", ["bad parse"]), ("error", "failed")]
)
def test_malformed_jscpd_report_fails(tmp_path: Path, field: str, value: object) -> None:
    report = jscpd_report() | {field: value}
    with pytest.raises(ValueError):
        clone_tools.parse_jscpd(report, tmp_path, SOURCES)


def test_missing_native_arrays_and_counts_fail(tmp_path: Path) -> None:
    report = jscpd_report()
    del report["duplicates"]
    with pytest.raises(KeyError):
        clone_tools.parse_jscpd(report, tmp_path, SOURCES)
    malformed = pyscn_report()
    cast("dict[str, object]", malformed["clone"])["clone_pairs"] = {}
    with pytest.raises(ValueError):
        clone_tools.parse_pyscn(malformed, tmp_path, SOURCES)


@pytest.mark.parametrize(
    "field,value", [("schema_version", 1), ("version", "1.32.4"), ("diagnostics", ["parse failed"])]
)
def test_pyscn_schema_and_diagnostics_fail(tmp_path: Path, field: str, value: object) -> None:
    with pytest.raises(ValueError):
        clone_tools.parse_pyscn(pyscn_report() | {field: value}, tmp_path, SOURCES)


@pytest.mark.parametrize("field,value", [("analyzed_files", 1), ("total_files", 3), ("skipped_files", 1)])
def test_pyscn_accounting_fails(tmp_path: Path, field: str, value: int) -> None:
    report = pyscn_report()
    cast("dict[str, object]", report["summary"])[field] = value
    with pytest.raises(ValueError):
        clone_tools.parse_pyscn(report, tmp_path, SOURCES)


def test_saturation_fails_before_pair_filtering(tmp_path: Path) -> None:
    report = pyscn_report()
    cast("dict[str, object]", report["clone"])["clone_pairs"] = [{}] * clone_tools.MAX_PAIRS
    with pytest.raises(ValueError, match="capacity"):
        clone_tools.parse_pyscn(report, tmp_path, SOURCES)


def test_nullable_empty_pyscn_pairs_are_valid(tmp_path: Path) -> None:
    assert clone_tools.parse_pyscn(pyscn_report(), tmp_path, SOURCES) == []


def test_pyscn_identity_requires_only_endpoint_sources(tmp_path: Path) -> None:
    pair: dict[str, object] = {
        "clone1": {"location": {"file_path": "src/a.py", "start_line": 1, "end_line": 2}},
        "clone2": {"location": {"file_path": "src/b.py", "start_line": 1, "end_line": 2}},
    }
    report = pyscn_report()
    clone = cast("dict[str, object]", report["clone"])
    clone["clone_pairs"] = [pair]
    cast("dict[str, object]", clone["statistics"])["total_clone_pairs"] = 1
    cast("dict[str, object]", report["summary"])["clone_pairs"] = 1
    assert clone_tools.parse_pyscn(report, tmp_path, SOURCES)[0].detector == "pyscn"


@pytest.mark.parametrize("comment", ["CPD-OFF", "CPD-ON", "cpd-disable", "pyscn:ignore"])
def test_suppression_comment_rejected_not_string(tmp_path: Path, comment: str) -> None:
    source = tmp_path / "src"
    source.mkdir()
    path = source / "sample.py"
    path.write_text(f"example = {comment!r}\n", encoding="utf-8")
    assert clone_tools.inventory(tmp_path, ("src",))
    path.write_text(f"# {comment}\n", encoding="utf-8")
    with pytest.raises(ValueError, match="suppression"):
        clone_tools.inventory(tmp_path, ("src",))


@pytest.mark.parametrize("suffix", ["start", "end"])
def test_native_marker_rejected_in_comments_and_strings(tmp_path: Path, suffix: str) -> None:
    source = tmp_path / "src"
    source.mkdir()
    path = source / "sample.py"
    marker = "jscpd" + ":ignore-" + suffix
    for text in (f"# {marker}\n", f"example = {marker!r}\n"):
        path.write_text(text, encoding="utf-8")
        with pytest.raises(ValueError, match="suppression"):
            clone_tools.inventory(tmp_path, ("src",))


@pytest.mark.parametrize("external", [False, True])
def test_directory_symlinks_fail_instead_of_silently_excluding_source(tmp_path: Path, external: bool) -> None:
    source = tmp_path / "src"
    source.mkdir()
    target = tmp_path / "outside" if external else source / "real"
    target.mkdir()
    (target / "sample.py").write_text("value = 1\n", encoding="utf-8")
    (source / "linked").symlink_to(target, target_is_directory=True)
    with pytest.raises(ValueError, match="directory symlink"):
        clone_tools.inventory(tmp_path, ("src",))


def test_missing_root_parse_error_and_size_fail(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="Missing"):
        clone_tools.inventory(tmp_path, ("src",))
    source = tmp_path / "src"
    source.mkdir()
    path = source / "bad.py"
    path.write_text("def broken(:\n", encoding="utf-8")
    with pytest.raises(SyntaxError):
        clone_tools.inventory(tmp_path, ("src",))
    path.write_bytes(b" " * (clone_tools.MAX_SOURCE_BYTES + 1))
    with pytest.raises(ValueError, match="size limit"):
        clone_tools.inventory(tmp_path, ("src",))


def test_empty_scope_writes_empty_report(tmp_path: Path) -> None:
    (tmp_path / "src").mkdir()
    output = tmp_path / "report"
    assert clone_tools.analyze(tmp_path, ("src",), output) == []
    assert '"input_files": 0' in (output / "findings.json").read_text(encoding="utf-8")


@pytest.mark.integration
def test_native_empty_tiny_hidden_and_ignored_inputs(tmp_path: Path) -> None:
    source = tmp_path / "src"
    source.mkdir()
    (source / "empty.py").write_text("", encoding="utf-8")
    (source / "test_tiny.py").write_text("value = 1\n", encoding="utf-8")
    (source / ".hidden.py").write_text("answer = 42\n", encoding="utf-8")
    (tmp_path / ".gitignore").write_text("src/\n", encoding="utf-8")
    (tmp_path / ".jscpd.json").write_text('{"ignore":["**/*"]}', encoding="utf-8")
    output = tmp_path / "report"
    assert clone_tools.analyze(tmp_path, ("src",), output) == []
    report = cast("dict[str, object]", json.loads((output / "pyscn.json").read_text(encoding="utf-8")))
    assert cast("dict[str, object]", report["summary"])["analyzed_files"] == 3


@pytest.mark.integration
def test_native_third_fourth_copy_and_repeat_stability(tmp_path: Path) -> None:
    source = tmp_path / "src"
    source.mkdir()
    counts: list[int] = []
    findings: list[clone_tools.CloneFinding] = []
    for count in range(2, 5):
        for number in range(count):
            (source / f"copy{number}.py").write_text(FUNCTION, encoding="utf-8")
        findings = clone_tools.analyze(tmp_path, ("src",), tmp_path / "report")
        assert {finding.detector for finding in findings} == {"pyscn", "jscpd"}
        counts.append(len(findings))
    assert counts[0] < counts[1] < counts[2]
    repeated = clone_tools.analyze(tmp_path, ("src",), tmp_path / "report")
    assert Counter(repeated) == Counter(findings)


@pytest.mark.integration
def test_native_identity_movement_and_normalized_edit(tmp_path: Path) -> None:
    source = tmp_path / "src"
    source.mkdir()
    (source / "first.py").write_text(FUNCTION, encoding="utf-8")
    second = source / "second.py"
    second.write_text(FUNCTION, encoding="utf-8")
    original = clone_tools.analyze(tmp_path, ("src",), tmp_path / "report")
    second.unlink()
    moved = source / "moved.py"
    moved.write_text("\n\n" + FUNCTION, encoding="utf-8")
    relocated = clone_tools.analyze(tmp_path, ("src",), tmp_path / "report")
    assert Counter(item.identity for item in original) == Counter(item.identity for item in relocated)
    moved.write_text("\n\n" + FUNCTION.replace("calculate", "evaluate"), encoding="utf-8")
    changed = clone_tools.analyze(tmp_path, ("src",), tmp_path / "report")
    assert {item.detector for item in changed} == {"pyscn", "jscpd"}
    assert {item.identity for item in original}.isdisjoint(item.identity for item in changed)


@pytest.mark.integration
def test_raw_marker_bypass_fails_and_removes_stale_report(tmp_path: Path) -> None:
    source = tmp_path / "src"
    source.mkdir()
    (source / "first.py").write_text(FUNCTION, encoding="utf-8")
    second = source / "second.py"
    second.write_text(FUNCTION, encoding="utf-8")
    output = tmp_path / "report"
    assert any(item.detector == "jscpd" for item in clone_tools.analyze(tmp_path, ("src",), output))
    start, end = ("# jscpd" + ":ignore-" + suffix for suffix in ("start", "end"))
    second.write_text(f"marker = {start!r}\n{FUNCTION}\nmarker_end = {end!r}\n", encoding="utf-8")
    with pytest.raises(ValueError, match="Raw clone suppression"):
        clone_tools.analyze(tmp_path, ("src",), output)
    assert not (output / "findings.json").exists()


def test_subprocess_errors_and_timeout_never_succeed(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    def fail(*_args: object, **_kwargs: object) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess(["pyscn"], 1, "", "parse error")

    monkeypatch.setattr(subprocess, "run", fail)
    with pytest.raises(RuntimeError, match="parse error"):
        clone_tools._run(["pyscn"], tmp_path)

    def timeout(*_args: object, **_kwargs: object) -> subprocess.CompletedProcess[str]:
        raise subprocess.TimeoutExpired("pyscn", 1)

    monkeypatch.setattr(subprocess, "run", timeout)
    with pytest.raises(subprocess.TimeoutExpired):
        clone_tools._run(["pyscn"], tmp_path)
