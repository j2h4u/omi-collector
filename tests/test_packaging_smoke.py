from __future__ import annotations

import subprocess
import sys
import tomllib
from pathlib import Path
from typing import TypedDict, Unpack, cast

import pytest
from scripts import check_packaging_smoke


class _SubprocessRunOptions(TypedDict, total=False):
    capture_output: bool
    check: bool
    cwd: Path | None
    env: dict[str, str] | None
    text: bool | None


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


def test_main_rejects_version_command_that_exits_nonzero(monkeypatch: pytest.MonkeyPatch) -> None:
    repo_root = Path(check_packaging_smoke.__file__).resolve().parents[1]
    metadata = cast(dict[str, object], tomllib.loads((repo_root / "pyproject.toml").read_text(encoding="utf-8")))
    project = metadata.get("project")
    assert isinstance(project, dict)
    raw_version: object = cast(dict[str, object], project).get("version")
    assert isinstance(raw_version, str)
    expected_version = raw_version
    real_run = subprocess.run

    def run_with_failed_version(
        command: list[str], **kwargs: Unpack[_SubprocessRunOptions]
    ) -> subprocess.CompletedProcess[str] | subprocess.CompletedProcess[bytes]:
        if command[-1] == "--version":
            command = [sys.executable, "-c", f"print({expected_version!r}); raise SystemExit(7)"]
        return real_run(command, **kwargs)

    monkeypatch.setattr(subprocess, "run", run_with_failed_version)

    with pytest.raises(subprocess.CalledProcessError) as error:
        check_packaging_smoke.main()

    assert error.value.returncode == 7
    stdout = cast(str | bytes | None, error.value.stdout)
    assert isinstance(stdout, str)
    assert stdout.strip() == expected_version
