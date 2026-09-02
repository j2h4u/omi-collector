"""Validate the release-please files against the packaged project metadata."""

from __future__ import annotations

import argparse
import json
import tomllib
from pathlib import Path
from typing import cast

PACKAGE_NAME = "omi-collector"
PACKAGE_PATH = "."
LOCK_VERSION_PATH = "$.package[?(@.name.value=='omi-collector')].version"


def _project_version(pyproject: dict[str, object]) -> str:
    project = pyproject.get("project")
    if not isinstance(project, dict) or not isinstance(project.get("version"), str):
        raise ValueError("pyproject.toml must declare [project].version")
    return project["version"]


def _lock_version(lock: dict[str, object]) -> str:
    packages = lock.get("package")
    if not isinstance(packages, list):
        raise ValueError("uv.lock must contain a package list")
    versions = [item.get("version") for item in packages if isinstance(item, dict) and item.get("name") == PACKAGE_NAME]
    if len(versions) != 1 or not isinstance(versions[0], str):
        raise ValueError(f"uv.lock must contain exactly one {PACKAGE_NAME!r} package version")
    return versions[0]


def validate_release_config(root: Path) -> list[str]:
    config = cast("dict[str, object]", json.loads((root / "release-please-config.json").read_text(encoding="utf-8")))
    manifest = cast(
        "dict[str, object]", json.loads((root / ".release-please-manifest.json").read_text(encoding="utf-8"))
    )
    pyproject = tomllib.loads((root / "pyproject.toml").read_text(encoding="utf-8"))
    lock = tomllib.loads((root / "uv.lock").read_text(encoding="utf-8"))
    errors: list[str] = []

    packages = config.get("packages")
    package = packages.get(PACKAGE_PATH) if isinstance(packages, dict) else None
    if not isinstance(package, dict):
        return ["release-please-config.json must configure the root package"]
    if package.get("release-type") != "python":
        errors.append("release-please must use the python strategy so it updates pyproject.toml")
    if package.get("package-name") != PACKAGE_NAME:
        errors.append(f"release-please package-name must be {PACKAGE_NAME!r}")
    if package.get("include-component-in-tag") is not False:
        errors.append("release-please root tags must not include a component prefix")

    expected_lock_updater = {"type": "toml", "path": "uv.lock", "jsonpath": LOCK_VERSION_PATH}
    extra_files = package.get("extra-files")
    if not isinstance(extra_files, list) or expected_lock_updater not in extra_files:
        errors.append("release-please must update the omi-collector version in uv.lock")

    try:
        version = _project_version(cast("dict[str, object]", pyproject))
    except ValueError as error:
        errors.append(str(error))
        version = ""
    if manifest.get(PACKAGE_PATH) != version:
        errors.append("release-please manifest version must match pyproject.toml")
    try:
        if _lock_version(cast("dict[str, object]", lock)) != version:
            errors.append("uv.lock project version must match pyproject.toml")
    except ValueError as error:
        errors.append(str(error))

    return errors


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[1])
    args = parser.parse_args(argv)

    errors = validate_release_config(cast("Path", args.root))
    if errors:
        for error in errors:
            print(f"release configuration error: {error}")
        return 1
    print("release-please configuration matches pyproject.toml and uv.lock")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
