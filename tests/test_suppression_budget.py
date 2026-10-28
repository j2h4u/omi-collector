from pathlib import Path

from pytest import CaptureFixture
from scripts.check_suppression_budget import check


def _configure(root: Path, *, baseline: int) -> None:
    (root / "pyproject.toml").write_text(
        f"""
[tool.omi_collector.suppressions]
paths = ["src"]

[tool.omi_collector.suppressions.baseline]
noqa = {baseline}
type_ignore = 0
pyright = 0
pylint = 0
ruff = 0
""",
        encoding="utf-8",
    )


def test_suppression_budget_accepts_reviewed_existing_count(tmp_path: Path, capsys: CaptureFixture[str]) -> None:
    source = tmp_path / "src" / "module.py"
    source.parent.mkdir()
    source.write_text("value = 1  # " + "noqa: F401\n", encoding="utf-8")
    _configure(tmp_path, baseline=1)

    assert check(tmp_path) == 0
    assert "noqa=1/1" in capsys.readouterr().out


def test_suppression_budget_rejects_new_suppression(tmp_path: Path, capsys: CaptureFixture[str]) -> None:
    source = tmp_path / "src" / "module.py"
    source.parent.mkdir()
    source.write_text("value = 1  # " + "noqa: F401\n", encoding="utf-8")
    _configure(tmp_path, baseline=0)

    assert check(tmp_path) == 1
    assert "noqa: 1 found, budget 0" in capsys.readouterr().err
