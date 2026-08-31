from __future__ import annotations

import os
import subprocess
import tempfile
import zipfile
from pathlib import Path
from venv import EnvBuilder


def _run(command: list[str], *, cwd: Path | None = None, env: dict[str, str] | None = None) -> None:
    subprocess.run(command, cwd=cwd, env=env, check=True)


def main() -> int:
    repo_root = Path(__file__).resolve().parents[1]

    with tempfile.TemporaryDirectory(prefix="omi-collector-packaging-") as tmp:
        workdir = Path(tmp)
        dist_dir = workdir / "dist"
        venv_dir = workdir / "venv"

        _run(["uv", "build", "--wheel", "--out-dir", str(dist_dir), "--no-build-logs", str(repo_root)])

        wheel_files = sorted(dist_dir.glob("*.whl"))
        if len(wheel_files) != 1:
            raise RuntimeError(f"expected exactly one wheel, found {len(wheel_files)} in {dist_dir}")

        with zipfile.ZipFile(wheel_files[0]) as wheel:
            payload = [name for name in wheel.namelist() if ".dist-info/" not in name]
        unexpected_payload = [name for name in payload if not name.startswith("omi_collector/")]
        if unexpected_payload:
            raise RuntimeError(f"wheel contains non-omi_collector payload: {unexpected_payload}")
        EnvBuilder(with_pip=False, clear=True).create(venv_dir)
        executable = venv_dir / "bin" / "omi-collector"
        venv_python = venv_dir / "bin" / "python"
        if not venv_python.is_file():
            raise RuntimeError(f"venv python not found: {venv_python}")

        install_env = {**os.environ, "UV_LINK_MODE": "copy"}
        _run(["uv", "pip", "install", "--python", str(venv_python), str(wheel_files[0])], env=install_env)
        _run(
            [
                str(venv_python),
                "-c",
                ("import importlib.util; assert importlib.util.find_spec('omi_collector') is not None"),
            ]
        )
        _run([str(executable), "--help"])
        _run([str(executable), "health"])

    print("packaging smoke passed: wheel built, installed, CLI help and health ran")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
