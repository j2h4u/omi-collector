from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest
from scripts import check_packaging_smoke


def test_run_raises_for_failed_command() -> None:
    with pytest.raises(subprocess.CalledProcessError) as error:
        check_packaging_smoke._run([sys.executable, "-c", "raise SystemExit(7)"])

    assert error.value.returncode == 7


def test_main_rejects_project_without_string_version(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    project_root = tmp_path / "project"
    scripts_dir = project_root / "scripts"
    scripts_dir.mkdir(parents=True)
    (project_root / "pyproject.toml").write_text("[project]\nname = 'missing-version'\n", encoding="utf-8")
    monkeypatch.setattr(check_packaging_smoke, "__file__", str(scripts_dir / "check_packaging_smoke.py"))

    def fail_if_build_starts(command: list[str], *, cwd: Path | None = None, env: dict[str, str] | None = None) -> None:
        del command, cwd, env
        raise AssertionError("invalid metadata must be rejected before running a command")

    monkeypatch.setattr(check_packaging_smoke, "_run", fail_if_build_starts)

    with pytest.raises(RuntimeError, match=r"pyproject.toml must declare \[project\]\.version"):
        check_packaging_smoke.main()


def test_main_builds_and_runs_installed_cli() -> None:
    assert check_packaging_smoke.main() == 0
