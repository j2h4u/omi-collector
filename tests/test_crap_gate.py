from __future__ import annotations

import json
from pathlib import Path

import pytest
from scripts.crap_gate import _function_metrics_from_report, _load_coverage_report, main


def _write_source(root: Path, relative_path: str, source: str) -> Path:
    path = root / relative_path
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(source, encoding="utf-8")
    return path


def _write_report(path: Path, files: dict[Path, dict[str, tuple[int, int]]]) -> Path:
    path.write_text(
        json.dumps(
            {
                "files": {
                    str(file_path): {
                        "functions": {
                            name: {"summary": {"covered_lines": covered, "num_statements": statements}}
                            for name, (covered, statements) in functions.items()
                        }
                    }
                    for file_path, functions in files.items()
                }
            }
        ),
        encoding="utf-8",
    )
    return path


def test_function_metrics_round_coverage_and_crap_values(tmp_path: Path) -> None:
    source_root = tmp_path / "src"
    source = _write_source(source_root, "example.py", "def measured():\n    return 1\n")
    coverage = _load_coverage_report(_write_report(tmp_path / "coverage.json", {source: {"measured": (2, 3)}}))

    [metric] = _function_metrics_from_report(coverage, source_root)

    assert metric.key == "example.py::measured"
    assert metric.coverage_fraction == 0.666667
    assert metric.crap == 1.037037


def test_main_passes_at_threshold_and_fails_above_threshold(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    source_root = tmp_path / "src"
    source = _write_source(source_root, "example.py", "def simple():\n    return 1\n")
    report = _write_report(tmp_path / "coverage.json", {source: {"simple": (1, 1)}})
    arguments = ["--coverage", str(report), "--src", str(source_root)]

    assert main([*arguments, "--threshold", "1"]) == 0
    assert capsys.readouterr().out == "CRAP gate passed: 1 function(s), threshold 1.00\n"

    assert main([*arguments, "--threshold", "0.99"]) == 1
    assert capsys.readouterr().out == (
        "CRAP gate failed: 1 function(s) exceed 0.99\n  example.py::simple:1 CRAP 1.00, complexity 1, coverage 100.0%\n"
    )


def test_main_requires_coverage_report(capsys: pytest.CaptureFixture[str]) -> None:
    with pytest.raises(SystemExit, match="2"):
        main([])

    assert "--coverage" in capsys.readouterr().err


def test_main_maps_nested_class_and_closure_and_filters_outside_source(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    source_root = tmp_path / "src"
    source = _write_source(
        source_root,
        "nested.py",
        "def outer():\n"
        "    def inner():\n"
        "        return 1\n"
        "    return inner()\n"
        "class Outer:\n"
        "    class Inner:\n"
        "        def method(self):\n"
        "            return 1\n",
    )
    outside = _write_source(tmp_path / "other", "ignored.py", "def ignored():\n    return 1\n")
    report = _write_report(
        tmp_path / "coverage.json",
        {
            source: {
                "outer": (2, 2),
                "outer.inner": (1, 1),
                "Outer.Inner.method": (1, 1),
            },
            outside: {"ignored": (0, 1)},
        },
    )

    assert main(["--coverage", str(report), "--src", str(source_root)]) == 0
    assert capsys.readouterr().out == "CRAP gate passed: 3 function(s), threshold 30.00\n"


def test_zero_statement_function_is_treated_as_fully_covered(tmp_path: Path) -> None:
    source_root = tmp_path / "src"
    source = _write_source(source_root, "empty.py", "def empty():\n    pass\n")
    coverage = _load_coverage_report(_write_report(tmp_path / "coverage.json", {source: {"empty": (0, 0)}}))

    [metric] = _function_metrics_from_report(coverage, source_root)

    assert metric.coverage_fraction == 1.0
    assert metric.crap == 1.0


def test_main_rejects_uncovered_simple_function_at_threshold_below_its_crap_score(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    source_root = tmp_path / "src"
    source = _write_source(source_root, "example.py", "def simple():\n    return 1\n")
    report = _write_report(tmp_path / "coverage.json", {source: {"simple": (0, 1)}})

    assert main(["--coverage", str(report), "--src", str(source_root), "--threshold", "1.5"]) == 1
    assert capsys.readouterr().out == (
        "CRAP gate failed: 1 function(s) exceed 1.50\n  example.py::simple:1 CRAP 2.00, complexity 1, coverage 0.0%\n"
    )
