"""Regression probes for the named import-linter contracts."""

from __future__ import annotations

import os
import subprocess
import tomllib
from pathlib import Path
from typing import TypedDict, cast

import pytest


class Contract(TypedDict):
    id: str
    name: str
    source_modules: list[str]
    forbidden_modules: list[str]


CONFIG = Path(__file__).resolve().parents[1] / "pyproject.toml"


def configured_contracts() -> list[Contract]:
    return cast(
        "list[Contract]",
        tomllib.loads(CONFIG.read_text())["tool"]["importlinter"]["contracts"],
    )


CONTRACT_CASES = [
    (contract["id"], forbidden, mode)
    for contract in configured_contracts()
    for forbidden in contract["forbidden_modules"]
    for mode in ("direct", "indirect")
]
POLICY_WITNESSES = (
    ("core-independent-from-cli", "omi_collector.core", ("omi_collector.cli",)),
    (
        "capture-domain-independent-from-outer-layers",
        "omi_collector.capture.domain",
        (
            "omi_collector.capture.application",
            "omi_collector.capture.adapters",
            "omi_collector.capture.cli",
        ),
    ),
    (
        "capture-application-uses-ports",
        "omi_collector.capture.application",
        ("omi_collector.capture.adapters", "bleak", "dbus_fast"),
    ),
)


@pytest.fixture
def import_project(tmp_path: Path) -> Path:
    """Build only the configured packages needed by the real contracts."""
    contracts = configured_contracts()
    modules = {
        module
        for contract in contracts
        for module in (*contract["source_modules"], *contract["forbidden_modules"])
        if module.startswith("omi_collector.")
    }
    external_modules = {
        module
        for contract in contracts
        for module in contract["forbidden_modules"]
        if not module.startswith("omi_collector.")
    }
    for module in modules | external_modules:
        package = tmp_path
        for part in module.split("."):
            package /= part
            package.mkdir(exist_ok=True)
            (package / "__init__.py").touch()
    return tmp_path


def run_contract(
    project: Path,
    contract_id: str | None = None,
) -> subprocess.CompletedProcess[str]:
    env = os.environ.copy()
    env["PYTHONPATH"] = os.pathsep.join(part for part in (str(project), env.get("PYTHONPATH", "")) if part)
    command = ["lint-imports", "--config", str(CONFIG)]
    if contract_id is not None:
        command.extend(("--contract", contract_id))
    command.extend(("--no-cache", "--no-logo"))
    return subprocess.run(
        command,
        cwd=project,
        env=env,
        capture_output=True,
        text=True,
        check=False,
        timeout=90,
    )


def test_empty_configured_packages_pass(import_project: Path) -> None:
    contracts = configured_contracts()
    result = run_contract(import_project)

    assert result.returncode == 0, result.stdout + result.stderr
    assert all(contract["name"] in result.stdout for contract in contracts)


@pytest.mark.parametrize(
    ("contract_id", "source", "forbidden"),
    POLICY_WITNESSES,
    ids=[contract_id for contract_id, _, _ in POLICY_WITNESSES],
)
def test_required_import_policy_witnesses_remain_configured(
    contract_id: str,
    source: str,
    forbidden: tuple[str, ...],
) -> None:
    contract = next(
        (contract for contract in configured_contracts() if contract["id"] == contract_id),
        None,
    )

    assert contract is not None, f"missing import contract {contract_id}"
    assert source in contract["source_modules"]
    assert set(forbidden) <= set(contract["forbidden_modules"])


def test_configured_contracts_accept_lawful_adapter_port_import(import_project: Path) -> None:
    contract = next(
        contract for contract in configured_contracts() if contract["id"] == "capture-application-uses-ports"
    )
    source = contract["source_modules"][0]
    adapter = next(module for module in contract["forbidden_modules"] if module.endswith(".adapters"))
    ports = f"{source}.ports"
    port_file = import_project.joinpath(*ports.split("."), "__init__.py")
    port_file.parent.mkdir(parents=True, exist_ok=True)
    port_file.write_text("class Port: ...\n")
    adapter_file = import_project.joinpath(*adapter.split("."), "__init__.py")
    adapter_file.write_text(f"from {ports} import Port\n")

    result = run_contract(import_project, contract["id"])

    assert result.returncode == 0, result.stdout + result.stderr
    assert contract["name"] in result.stdout


@pytest.mark.parametrize(
    ("contract_id", "forbidden", "mode"),
    CONTRACT_CASES,
    ids=[f"{contract_id}-{forbidden}-{mode}" for contract_id, forbidden, mode in CONTRACT_CASES],
)
def test_forbidden_import_breaks_its_named_contract(
    import_project: Path,
    contract_id: str,
    forbidden: str,
    mode: str,
) -> None:
    contract = next(contract for contract in configured_contracts() if contract["id"] == contract_id)
    assert forbidden in contract["forbidden_modules"]
    source = contract["source_modules"][0]
    source_file = import_project.joinpath(*source.split("."), "__init__.py")
    if mode == "direct":
        source_file.write_text(f"import {forbidden}\n")
        dependency_path = f"{source} -> {forbidden}"
    else:
        bridge = "omi_collector.contract_bridge"
        bridge_file = import_project.joinpath(*bridge.split("."), "__init__.py")
        bridge_file.parent.mkdir(parents=True, exist_ok=True)
        bridge_file.write_text(f"import {forbidden}\n")
        source_file.write_text(f"import {bridge}\n")
        dependency_path = f"{bridge} -> {forbidden}"

    result = run_contract(import_project, contract_id)
    output = result.stdout + result.stderr
    normalized_output = " ".join(output.split())

    assert result.returncode == 1, output
    assert contract["name"] in output, output
    assert "BROKEN" in output, output
    assert f"{source} is not allowed to import {forbidden}" in normalized_output, output
    assert dependency_path in normalized_output, output
