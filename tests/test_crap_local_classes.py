import json
import re
from pathlib import Path
from textwrap import dedent
from typing import cast

import pytest
from coverage import Coverage
from scripts.crap_gate import _function_metrics_from_report, _load_coverage_report
from scripts.crap_gate import main as crap_gate_main


def _real_coverage(tmp_path: Path, source: str) -> tuple[Path, Path]:
    source_root = tmp_path / "src"
    source_root.mkdir()
    module = source_root / "worker.py"
    module.write_text(dedent(source), encoding="utf-8")
    measurement = Coverage(data_file=None, config_file=False, include=[str(module)])
    measurement.start()
    try:
        exec(compile(module.read_text(encoding="utf-8"), str(module), "exec"), {})
    finally:
        measurement.stop()
    report = tmp_path / "coverage.json"
    measurement.json_report(outfile=str(report))
    measurement.get_data().close(force=True)
    return source_root, report


def test_local_method_exceeding_crap_threshold_fails(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    source_root, report = _real_coverage(
        tmp_path,
        """
        def factory():
            class Worker:
                def run(self, value):
                    if value == 0:
                        return 0
                    if value == 1:
                        return 1
                    if value == 2:
                        return 2
                    if value == 3:
                        return 3
                    if value == 4:
                        return 4
                    return value
            return Worker
        factory()
        """,
    )

    assert crap_gate_main(["--coverage", str(report), "--src", str(source_root)]) == 1
    assert "factory.Worker.run" in capsys.readouterr().out
    metrics = _function_metrics_from_report(_load_coverage_report(report), source_root)
    method = next(metric for metric in metrics if metric.key.endswith("::factory.Worker.run"))
    assert method.complexity == 6
    assert method.coverage_fraction == 0
    assert method.crap == 42


_SCOPED_SOURCE = """
class Worker:
    def run(self):
        return 1

def factory():
    class Worker:
        async def run(self):
            def finish():
                return 2
            return finish()
        class Nested:
            def run(self):
                return 3
    def closure():
        class Worker:
            def run(self):
                class Deep:
                    def run(self):
                        return 4
                return Deep
        return Worker
    return Worker, closure

def other():
    class Worker:
        def run(self):
            return 5
    return Worker

factory()
other()
"""

_SCOPED_NAMES = {
    "Worker.run",
    "factory",
    "factory.Worker.run",
    "factory.Worker.run.finish",
    "factory.Worker.Nested.run",
    "factory.closure",
    "factory.closure.Worker.run",
    "factory.closure.Worker.run.Deep.run",
    "other",
    "other.Worker.run",
}


def test_local_methods_and_closures_match_real_coverage(tmp_path: Path) -> None:
    source_root, report = _real_coverage(tmp_path, _SCOPED_SOURCE)
    coverage_report = _load_coverage_report(report)
    functions = next(iter(coverage_report["files"].values()))["functions"]

    assert set(functions) - {""} == _SCOPED_NAMES
    metrics = _function_metrics_from_report(coverage_report, source_root)
    assert {metric.key.removeprefix("worker.py::") for metric in metrics} == _SCOPED_NAMES
    assert all(metric.complexity == 1 for metric in metrics)
    assert crap_gate_main(["--coverage", str(report), "--src", str(source_root)]) == 0


@pytest.mark.parametrize("missing", sorted(_SCOPED_NAMES))
def test_missing_scoped_function_coverage_fails(tmp_path: Path, missing: str) -> None:
    source_root, report = _real_coverage(tmp_path, _SCOPED_SOURCE)
    coverage_report = _load_coverage_report(report)
    functions = next(iter(coverage_report["files"].values()))["functions"]
    del functions[missing]
    report.write_text(json.dumps(coverage_report), encoding="utf-8")

    with pytest.raises(ValueError, match=re.escape(missing)):
        _function_metrics_from_report(coverage_report, source_root)
    assert crap_gate_main(["--coverage", str(report), "--src", str(source_root)]) == 1


def test_unmatched_function_coverage_fails(tmp_path: Path) -> None:
    source_root, report = _real_coverage(tmp_path, _SCOPED_SOURCE)
    coverage_report = _load_coverage_report(report)
    functions = next(iter(coverage_report["files"].values()))["functions"]
    functions["stale.run"] = functions["factory.Worker.run"]
    report.write_text(json.dumps(coverage_report), encoding="utf-8")

    with pytest.raises(ValueError, match=r"stale\.run"):
        _function_metrics_from_report(coverage_report, source_root)
    assert crap_gate_main(["--coverage", str(report), "--src", str(source_root)]) == 1


def test_duplicate_qualified_names_use_separate_line_coverage(tmp_path: Path) -> None:
    source_root, report = _real_coverage(
        tmp_path,
        """
        def work():
            return 1
        work()
        def work():
            return 2
        """,
    )
    metrics = _function_metrics_from_report(_load_coverage_report(report), source_root)
    assert [(metric.start_line, metric.coverage_fraction) for metric in metrics] == [(2, 1.0), (5, 0.0)]


def test_excluded_function_remains_in_inventory(tmp_path: Path) -> None:
    source_root, report = _real_coverage(
        tmp_path,
        """
        def excluded():  # pragma: no cover
            return 1
        def included():
            return 2
        included()
        """,
    )
    metrics = _function_metrics_from_report(_load_coverage_report(report), source_root)
    assert {metric.key for metric in metrics} == {"worker.py::excluded", "worker.py::included"}


def test_coverage_omitting_function_in_else_fails_closed(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    source_root, report = _real_coverage(
        tmp_path,
        """
        if False:
            pass
        else:
            def conditional():
                return 1
        """,
    )
    assert crap_gate_main(["--coverage", str(report), "--src", str(source_root)]) == 1
    assert "missing function metrics: worker.py::conditional" in capsys.readouterr().out


def test_stale_report_cannot_hide_duplicate_function(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    source_root, report = _real_coverage(tmp_path, "def work():\n    return 1\nwork()\n")
    with (source_root / "worker.py").open("a", encoding="utf-8") as source:
        source.write(
            "def work(value):\n"
            "    if value == 0: return 0\n"
            "    if value == 1: return 1\n"
            "    if value == 2: return 2\n"
            "    if value == 3: return 3\n"
            "    if value == 4: return 4\n"
            "    return value\n"
        )
    assert crap_gate_main(["--coverage", str(report), "--src", str(source_root)]) == 1
    assert "missing source lines" in capsys.readouterr().out


def test_duplicate_empty_and_excluded_functions_are_valid(tmp_path: Path) -> None:
    source_root, report = _real_coverage(
        tmp_path,
        '''
        def work():
            """Documentation only."""
        def work():  # pragma: no cover
            return 1
        ''',
    )
    metrics = _function_metrics_from_report(_load_coverage_report(report), source_root)
    assert len(metrics) == 2
    assert all(metric.coverage_fraction == 1.0 for metric in metrics)


@pytest.mark.parametrize("field,lines", [("executed_lines", [True]), ("missing_lines", [0]), ("missing_lines", [2])])
def test_invalid_duplicate_coverage_lines_fail(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], field: str, lines: list[int]
) -> None:
    source_root, report = _real_coverage(tmp_path, "def work():\n    return 1\nwork()\ndef work():\n    return 2\n")
    data = _load_coverage_report(report)
    file_data = cast(dict[str, object], next(iter(data["files"].values())))
    file_data[field] = lines
    report.write_text(json.dumps(data), encoding="utf-8")
    assert crap_gate_main(["--coverage", str(report), "--src", str(source_root)]) == 1
    assert "CRAP gate failed:" in capsys.readouterr().out
