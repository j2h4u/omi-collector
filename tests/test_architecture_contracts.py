from __future__ import annotations

import subprocess
import tomllib
from pathlib import Path
from typing import TypedDict, cast

import pytest


class Module(TypedDict):
    path: str
    depends_on: list[str]


@pytest.fixture
def architecture_project(tmp_path: Path) -> Path:
    config_path = Path(__file__).resolve().parents[1] / "tach.toml"
    config_text = config_path.read_text()
    config = tomllib.loads(config_text)
    (tmp_path / "tach.toml").write_text(config_text)
    package = tmp_path / "src" / "omi_collector"
    package.mkdir(parents=True)
    (package / "__init__.py").write_text("")
    (package / "capture").mkdir()
    (package / "capture" / "__init__.py").write_text("")
    for module in cast("list[Module]", config["modules"]):
        path = tmp_path / "src" / Path(*module["path"].split("."))
        path.mkdir(parents=True, exist_ok=True)
        imports = "".join(f"import {dependency}\n" for dependency in module["depends_on"])
        (path / "__init__.py").write_text(imports)
    return tmp_path


def run_check(project: Path, *args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["tach", "check", *args],
        cwd=project,
        capture_output=True,
        text=True,
        check=False,
        timeout=90,
    )


def test_allowed_dependencies_pass(architecture_project: Path) -> None:
    result = run_check(architecture_project)
    assert result.returncode == 0, result.stdout + result.stderr


def test_adapter_can_import_application_ports(architecture_project: Path) -> None:
    application = architecture_project / "src/omi_collector/capture/application"
    (application / "ports.py").write_text("class Port: pass\n")
    adapter = architecture_project / "src/omi_collector/capture/adapters/__init__.py"
    adapter.write_text(adapter.read_text() + "from omi_collector.capture.application.ports import Port\n")
    result = run_check(architecture_project)
    assert result.returncode == 0, result.stdout + result.stderr


@pytest.mark.parametrize(
    "source",
    [
        "import omi_collector.capture.adapters\n",
        "def use():\n    import omi_collector.capture.adapters\n",
        "from typing import TYPE_CHECKING\nif TYPE_CHECKING:\n    import omi_collector.capture.adapters\n",
    ],
    ids=["module", "local", "type-checking"],
)
def test_domain_cannot_depend_on_adapters(architecture_project: Path, source: str) -> None:
    path = architecture_project / "src/omi_collector/capture/domain/__init__.py"
    path.write_text(source)
    result = run_check(architecture_project)
    assert result.returncode == 1
    assert "Cannot use 'omi_collector.capture.adapters'" in result.stderr
    assert "Module 'omi_collector.capture.domain' cannot depend on 'omi_collector.capture.adapters'" in result.stderr


def test_unused_declared_dependency_fails(architecture_project: Path) -> None:
    path = architecture_project / "src/omi_collector/capture/application/__init__.py"
    path.write_text("import omi_collector.config\n")
    result = run_check(architecture_project)
    assert result.returncode == 1
    assert "Unused Dependencies" in result.stderr
    assert "'omi_collector.capture.application' does not depend on" in result.stderr
    assert "'omi_collector.capture.domain'" in result.stderr


def test_dependency_on_unassigned_module_fails(architecture_project: Path) -> None:
    (architecture_project / "src/omi_collector/rogue.py").write_text("value = 1\n")
    path = architecture_project / "src/omi_collector/capture/domain/__init__.py"
    path.write_text("import omi_collector.rogue\n")
    result = run_check(architecture_project)
    assert result.returncode == 1
    assert "Cannot use 'omi_collector.rogue'" in result.stderr
    assert "Module 'omi_collector.capture.domain' cannot depend on '<root>'" in result.stderr


def test_cycle_in_declared_dependencies_fails(architecture_project: Path) -> None:
    config_path = architecture_project / "tach.toml"
    config_path.write_text(
        config_path.read_text().replace(
            'path = "omi_collector.capture.domain"\ndepends_on = []',
            'path = "omi_collector.capture.domain"\ndepends_on = ["omi_collector.capture.application"]',
        )
    )
    path = architecture_project / "src/omi_collector/capture/domain/__init__.py"
    path.write_text("import omi_collector.capture.application\n")
    result = run_check(architecture_project)
    assert result.returncode == 1
    assert "Circular dependency detected for module 'omi_collector.capture.application'" in result.stderr
    assert "Circular dependency detected for module 'omi_collector.capture.domain'" in result.stderr
