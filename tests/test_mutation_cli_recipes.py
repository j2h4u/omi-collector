from __future__ import annotations

import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

PROJECT = Path(__file__).resolve().parents[1]


def _justfile_command(
    just: str,
    recipe: str,
    *arguments: str,
    working_directory: Path = PROJECT,
) -> list[str]:
    return [
        just,
        "--justfile",
        str(PROJECT / "Justfile"),
        "--working-directory",
        str(working_directory),
        recipe,
        *arguments,
    ]


def _uv_shim(
    directory: Path,
    capture: Path,
    expected_arguments: tuple[str, ...],
    module_arguments: tuple[str, ...],
) -> None:
    directory.mkdir()
    script = "\n".join(
        (
            f"#!{sys.executable}",
            "import os, pathlib, sys",
            f"expected = {expected_arguments!r}",
            f"pathlib.Path({str(capture)!r}).write_text('\\n'.join(sys.argv[1:]), encoding='utf-8')",
            "if tuple(sys.argv[1:]) != expected:",
            "    raise SystemExit(f'unexpected uv argv: {sys.argv[1:]!r}')",
            f"os.execv(sys.executable, [sys.executable, '-m', *{module_arguments!r}])",
            "",
        )
    )
    executable = directory / "uv"
    executable.write_text(script, encoding="utf-8")
    executable.chmod(0o755)


def _env_with_path(directory: Path) -> dict[str, str]:
    environment = os.environ.copy()
    environment["PATH"] = os.pathsep.join((str(directory), environment.get("PATH", "")))
    runtime = directory.parent / "runtime"
    runtime.mkdir(exist_ok=True)
    environment["XDG_RUNTIME_DIR"] = str(runtime)
    return environment


def test_public_fresh_recipe_invokes_campaign_module_without_dispatch(tmp_path: Path) -> None:
    just = shutil.which("just")
    assert just is not None
    capture = tmp_path / "uv-argv.txt"
    _uv_shim(
        tmp_path / "bin",
        capture,
        (
            "run",
            "--frozen",
            "--no-sync",
            "python",
            "-m",
            "scripts.mutation_campaign",
            "launch",
            "--mode",
            "fresh",
        ),
        ("scripts.mutation_campaign", "launch", "--help"),
    )

    environment = _env_with_path(tmp_path / "bin")
    for key in ("COVERAGE_CORE", "COVERAGE_FILE", "PYTEST_ADDOPTS"):
        environment.pop(key, None)
    result = subprocess.run(
        _justfile_command(just, "mutation", "fresh"),
        cwd=PROJECT,
        env=environment,
        capture_output=True,
        text=True,
        timeout=15,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    assert capture.read_text(encoding="utf-8").splitlines() == [
        "run",
        "--frozen",
        "--no-sync",
        "python",
        "-m",
        "scripts.mutation_campaign",
        "launch",
        "--mode",
        "fresh",
    ]
    assert "usage:" in result.stdout
    assert "ModuleNotFoundError" not in result.stderr


@pytest.mark.parametrize(
    ("recipe", "arguments", "message"),
    (
        ("mutation", ("invalid",), "mutation mode must be resume or fresh"),
        ("mutation-internal", ("invalid",), "mutation mode must be resume or fresh"),
    ),
)
def test_mutation_recipes_reject_invalid_modes_before_launch(
    tmp_path: Path, recipe: str, arguments: tuple[str, ...], message: str
) -> None:
    just = shutil.which("just")
    assert just is not None

    result = subprocess.run(
        _justfile_command(just, recipe, *arguments, working_directory=tmp_path),
        cwd=tmp_path,
        env=_env_with_path(tmp_path / "bin"),
        capture_output=True,
        text=True,
        timeout=15,
        check=False,
    )

    assert result.returncode == 2
    assert message in result.stderr
    assert "ModuleNotFoundError" not in result.stderr


def test_internal_recipe_wires_prepare_and_finish_through_campaign_module(tmp_path: Path) -> None:
    just = shutil.which("just")
    assert just is not None
    runtime = tmp_path / "runtime"
    runtime.mkdir()
    environment = os.environ.copy()
    environment["XDG_RUNTIME_DIR"] = str(runtime)

    result = subprocess.run(
        [
            just,
            "--dry-run",
            "--justfile",
            str(PROJECT / "Justfile"),
            "--working-directory",
            str(tmp_path),
            "mutation-internal",
            "fresh",
        ],
        cwd=tmp_path,
        env=environment,
        capture_output=True,
        text=True,
        timeout=15,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    output = result.stdout + result.stderr
    assert "python -m scripts.mutation_campaign prepare" in output
    assert "python -m scripts.mutation_campaign finish" in output
    assert "scripts/mutation_campaign.py" not in output


def test_status_recipe_runs_scope_module_and_handles_no_job(tmp_path: Path) -> None:
    just = shutil.which("just")
    assert just is not None
    capture = tmp_path / "uv-argv.txt"
    _uv_shim(
        tmp_path / "bin",
        capture,
        ("run", "--frozen", "--no-sync", "python", "-m", "scripts.mutation_scope", "status"),
        ("scripts.mutation_scope", "status"),
    )
    environment = _env_with_path(tmp_path / "bin")
    environment["OMI_MUTATION_AUDIT_ROOT"] = str(tmp_path / "empty-audit")

    result = subprocess.run(
        _justfile_command(just, "mutation-status"),
        cwd=PROJECT,
        env=environment,
        capture_output=True,
        text=True,
        timeout=15,
        check=False,
    )

    assert result.returncode == 1
    assert "no active or terminal mutation job receipt found" in result.stderr
    assert "ModuleNotFoundError" not in result.stderr
    assert capture.read_text(encoding="utf-8").splitlines() == [
        "run",
        "--frozen",
        "--no-sync",
        "python",
        "-m",
        "scripts.mutation_scope",
        "status",
    ]
