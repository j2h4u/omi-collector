"""Exercise the upstream TOML updater that the release action runs."""

from __future__ import annotations

import json
import shutil
import subprocess
import tempfile
import tomllib
from pathlib import Path
from typing import cast

import pytest
from scripts.validate_release_config import LOCK_VERSION_PATH, PACKAGE_NAME

_ROOT = Path(__file__).parents[1]
_RELEASE_PLEASE_VERSION = "17.6.0"
_BUMP_VERSION = "99.98.97"
_NODE_UPDATER = """
const {readFileSync} = require('node:fs');
const {join} = require('node:path');
const releasePleaseRoot = process.argv[1];
const lockPath = process.argv[2];
const jsonpath = process.argv[3];
const nextVersion = process.argv[4];
const {GenericToml} = require(join(releasePleaseRoot, 'build/src/updaters/generic-toml.js'));
const {Version} = require(join(releasePleaseRoot, 'build/src/version.js'));
process.stdout.write(new GenericToml(jsonpath, Version.parse(`v${nextVersion}`)).updateContent(readFileSync(lockPath, 'utf8')));
"""


def _package_versions(lock: str) -> dict[str, str]:
    document = cast("dict[str, object]", tomllib.loads(lock))
    packages = document.get("package")
    assert isinstance(packages, list)
    return {
        item["name"]: item["version"]
        for item in packages
        if isinstance(item, dict) and isinstance(item.get("name"), str) and isinstance(item.get("version"), str)
    }


@pytest.mark.integration
def test_release_please_v17_6_toml_updater_bumps_only_the_project_lock_version() -> None:
    npm = shutil.which("npm")
    node = shutil.which("node")
    if npm is None or node is None:
        pytest.skip("the release-please updater check needs npm and node")

    config = cast("dict[str, object]", json.loads((_ROOT / "release-please-config.json").read_text(encoding="utf-8")))
    packages = config.get("packages")
    assert isinstance(packages, dict)
    package = packages.get(".")
    assert isinstance(package, dict)
    extra_files = package.get("extra-files")
    assert isinstance(extra_files, list)
    updater = next(item for item in extra_files if isinstance(item, dict) and item.get("path") == "uv.lock")
    assert updater.get("jsonpath") == LOCK_VERSION_PATH

    original_lock = (_ROOT / "uv.lock").read_text(encoding="utf-8")
    with tempfile.TemporaryDirectory(prefix="omi-collector-release-please-") as temporary_directory:
        temporary_root = Path(temporary_directory)
        subprocess.run(
            [
                npm,
                "install",
                "--prefix",
                str(temporary_root),
                "--no-save",
                "--no-package-lock",
                "--ignore-scripts",
                f"release-please@{_RELEASE_PLEASE_VERSION}",
            ],
            check=True,
            capture_output=True,
            text=True,
        )
        result = subprocess.run(
            [
                node,
                "-e",
                _NODE_UPDATER,
                str(temporary_root / "node_modules" / "release-please"),
                str(_ROOT / "uv.lock"),
                LOCK_VERSION_PATH,
                _BUMP_VERSION,
            ],
            check=True,
            capture_output=True,
            text=True,
        )

    before = _package_versions(original_lock)
    after = _package_versions(result.stdout)
    changed = {name: version for name, version in after.items() if before.get(name) != version}

    assert after[PACKAGE_NAME] == _BUMP_VERSION
    assert changed == {PACKAGE_NAME: _BUMP_VERSION}
