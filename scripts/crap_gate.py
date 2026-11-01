import argparse
import ast
import json
from collections import Counter
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol, TypedDict, cast

from coverage import Coverage, CoverageException
from radon.complexity import cc_visit_ast

DEFAULT_THRESHOLD = 30.0
DEFAULT_SOURCE_ROOT = Path("src/omi_collector")


class _FunctionSummary(TypedDict):
    covered_lines: int
    num_statements: int


class _CoverageFunctionEntry(TypedDict):
    summary: _FunctionSummary


class _CoverageFileEntry(TypedDict):
    functions: dict[str, _CoverageFunctionEntry]


class _CoverageReport(TypedDict):
    files: dict[str, _CoverageFileEntry]


class _RadonBlock(Protocol):
    name: str
    lineno: int
    endline: int
    complexity: int


class _GateArgs(Protocol):
    coverage: Path
    src: Path
    threshold: float


@dataclass(frozen=True, slots=True)
class FunctionMetric:
    key: str
    start_line: int
    complexity: int
    coverage_fraction: float
    crap: float


RadonVisit = Callable[[ast.AST], Sequence[_RadonBlock]]
_CC_VISIT: RadonVisit = cast(RadonVisit, cc_visit_ast)


def _round_metric(value: float) -> float:
    return round(value, 6)


def _expect_dict(value: object, context: str) -> dict[str, object]:
    if not isinstance(value, dict):
        raise ValueError(context)
    return cast(dict[str, object], value)


def _expect_str(value: object, context: str) -> str:
    if not isinstance(value, str):
        raise ValueError(context)
    return value


def _expect_int(value: object, context: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(context)
    return value


type _FunctionNode = ast.FunctionDef | ast.AsyncFunctionDef


def _function_blocks(node: ast.AST, prefix: tuple[str, ...] = ()) -> list[tuple[str, _FunctionNode, _RadonBlock]]:
    items: list[tuple[str, _FunctionNode, _RadonBlock]] = []
    if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
        prefix = (*prefix, node.name)
        if not isinstance(node, ast.ClassDef):
            # Radon's enclosing-function visitor drops local classes; visit each function directly.
            items.append((".".join(prefix), node, _CC_VISIT(node)[0]))
    for child in ast.iter_child_nodes(node):
        items.extend(_function_blocks(child, prefix))
    return items


def _coverage_lines(file_data: dict[str, object], field: str) -> set[int]:
    value = file_data.get(field)
    if not isinstance(value, list):
        raise ValueError(f"coverage data has no {field} array for colliding function names")
    lines = {_expect_int(line, f"coverage {field} contains an invalid line") for line in value}
    if any(line <= 0 for line in lines):
        raise ValueError(f"coverage {field} contains an invalid line")
    return lines


def _body_lines(node: _FunctionNode) -> set[int]:
    end_line = node.body[-1].end_lineno
    if end_line is None:
        raise ValueError("function body has no end line")
    return set(range(node.body[0].lineno, end_line + 1))


def _positional_coverage(
    node: _FunctionNode, file_data: dict[str, object], source_statements: set[int]
) -> _CoverageFunctionEntry:
    lines = _body_lines(node)
    # Nested bodies belong to their own functions, including methods of local classes.
    for child in ast.walk(node):
        if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef)) and child is not node:
            lines -= _body_lines(child)
    excluded = _coverage_lines(file_data, "excluded_lines")
    executed = _coverage_lines(file_data, "executed_lines")
    missing = _coverage_lines(file_data, "missing_lines")
    if executed & missing:
        raise ValueError("coverage executed and missing lines overlap")
    if source_statements & lines - (excluded | executed | missing):
        raise ValueError(f"coverage report is missing source lines for function at line {node.lineno}")
    lines -= excluded
    covered = lines & executed
    statements = covered | (lines & missing)
    return {"summary": {"covered_lines": len(covered), "num_statements": len(statements)}}


def _load_coverage_report(path: Path) -> _CoverageReport:
    raw = cast(object, json.loads(path.read_text(encoding="utf-8")))
    raw_report = _expect_dict(raw, "coverage report is not an object")
    return cast(
        _CoverageReport,
        {"files": _expect_dict(raw_report.get("files"), "coverage report does not contain an object at 'files'")},
    )


def _function_metrics_from_report(coverage_report: _CoverageReport, source_root: Path) -> list[FunctionMetric]:
    source_root = source_root.resolve()
    metrics: list[FunctionMetric] = []
    coverage_files = {
        Path(_expect_str(raw_path, "coverage report file path is invalid")).resolve(): file_data
        for raw_path, file_data in coverage_report["files"].items()
    }
    for file_path in _source_files(source_root):
        file_data = coverage_files.get(file_path)
        if file_data is None:
            raise ValueError(f"coverage report is missing source file {file_path.relative_to(source_root)}")
        file_metrics = _function_metrics_from_file(file_path, file_data, source_root)
        metrics.extend(file_metrics)
    if not metrics:
        raise ValueError(f"no function metrics found under {source_root}")
    return metrics


def _source_files(source_root: Path) -> Iterable[Path]:
    if source_root.is_file():
        yield source_root.resolve()
        return
    for path in sorted(source_root.rglob("*.py")):
        yield path.resolve()


def _function_metrics_from_file(
    file_path: Path,
    file_data: _CoverageFileEntry,
    source_root: Path,
) -> list[FunctionMetric]:
    relative_path = file_path.name if file_path == source_root else file_path.relative_to(source_root).as_posix()
    source_text = file_path.read_text(encoding="utf-8")
    file_object = _expect_dict(file_data, f"coverage data for {relative_path} is invalid")
    functions = _expect_dict(file_object.get("functions"), f"coverage data for {relative_path} has no functions object")
    metrics: list[FunctionMetric] = []
    missing: list[str] = []
    blocks = _function_blocks(ast.parse(source_text))
    counts = Counter(qualname for qualname, _, _ in blocks)
    source_statements: set[int] = set()
    if any(count > 1 for count in counts.values()):
        analysis = Coverage(data_file=None, config_file=False)
        try:
            source_statements = set(analysis.analysis2(str(file_path))[1])
        finally:
            analysis.get_data().close(force=True)
    for qualname, node, block in blocks:
        coverage_entry = functions.get(qualname)
        if coverage_entry is None:
            missing.append(f"{relative_path}::{qualname}")
            continue
        metric = _metric_for_function(relative_path, qualname, block, coverage_entry)
        if counts[qualname] > 1:
            metric = _metric_for_function(
                relative_path, qualname, block, _positional_coverage(node, file_object, source_statements)
            )
        metrics.append(metric)
    if missing:
        raise ValueError("coverage report is missing function metrics: " + ", ".join(missing))
    unmatched = functions.keys() - counts.keys() - {""}
    if unmatched:
        raise ValueError(
            f"coverage report has unmatched function metrics for {relative_path}: {', '.join(sorted(unmatched))}"
        )
    return metrics


def _metric_for_function(
    relative_path: str,
    qualname: str,
    block: _RadonBlock,
    function_data: object,
) -> FunctionMetric:
    summary = _expect_dict(
        _expect_dict(function_data, f"coverage data for {relative_path}::{qualname} is invalid").get("summary"),
        f"coverage data for {relative_path}::{qualname} has no summary object",
    )
    num_statements = _expect_int(
        summary.get("num_statements"),
        f"coverage summary for {relative_path}::{qualname} has invalid num_statements",
    )
    covered_lines = _expect_int(
        summary.get("covered_lines"),
        f"coverage summary for {relative_path}::{qualname} has invalid covered_lines",
    )
    if not 0 <= covered_lines <= num_statements:
        raise ValueError(f"coverage summary for {relative_path}::{qualname} has invalid line counts")
    coverage_fraction = 1.0 if num_statements == 0 else covered_lines / num_statements
    crap = (block.complexity**2) * ((1 - coverage_fraction) ** 3) + block.complexity
    return FunctionMetric(
        key=f"{relative_path}::{qualname}",
        start_line=block.lineno,
        complexity=block.complexity,
        coverage_fraction=_round_metric(coverage_fraction),
        crap=_round_metric(crap),
    )


def _format_offender(metric: FunctionMetric) -> str:
    return (
        f"{metric.key}:{metric.start_line} "
        f"CRAP {metric.crap:.2f}, complexity {metric.complexity}, coverage {metric.coverage_fraction:.1%}"
    )


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Fail if any function exceeds the configured CRAP threshold.")
    parser.add_argument("--coverage", type=Path, required=True, help="pytest-cov JSON report")
    parser.add_argument("--src", type=Path, default=DEFAULT_SOURCE_ROOT, help="source root to scan")
    parser.add_argument("--threshold", type=float, default=DEFAULT_THRESHOLD, help="maximum allowed CRAP per function")
    return parser


def _parse_args(argv: list[str] | None) -> _GateArgs:
    parser = _build_parser()
    return cast(_GateArgs, parser.parse_args(argv))


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    try:
        metrics = _function_metrics_from_report(_load_coverage_report(args.coverage), args.src)
    except (CoverageException, OSError, ValueError) as exc:
        print(f"CRAP gate failed: {exc}")
        return 1
    offenders = [metric for metric in metrics if metric.crap > args.threshold]
    if offenders:
        print(f"CRAP gate failed: {len(offenders)} function(s) exceed {args.threshold:.2f}")
        for metric in sorted(offenders, key=lambda item: (-item.crap, item.key))[:20]:
            print(f"  {_format_offender(metric)}")
        return 1

    print(f"CRAP gate passed: {len(metrics)} function(s), threshold {args.threshold:.2f}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
