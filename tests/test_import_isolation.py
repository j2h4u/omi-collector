"""Guard command-group imports against crossing bounded-context boundaries."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

from omi_collector.capture.adapters.collector_state_db import StateDB, StateKind
from omi_collector.storage_layout import load_operator_config

_CONFIG = '[pendant]\naddress = "AA:BB:CC:DD:EE:FF"\n[ready]\ntarget_audio_seconds = 3600.0\n'


def test_product_namespace_is_importable() -> None:
    script = "import importlib.util; assert importlib.util.find_spec('omi_collector') is not None"
    subprocess.run([sys.executable, "-c", script], check=True)


def _imports_after(statement: str) -> set[str]:
    script = (
        "import sys; "
        f"{statement}; "
        "print('\\n'.join(sorted(name for name in sys.modules if name.startswith('omi_collector.'))))"
    )
    completed = subprocess.run(
        [sys.executable, "-c", script],
        check=True,
        capture_output=True,
        text=True,
    )
    return set(completed.stdout.splitlines())


def test_root_cli_does_not_import_unselected_capture() -> None:
    imported = _imports_after("import omi_collector.cli")
    assert not any(name.startswith("omi_collector.capture") for name in imported)


def test_device_sync_executes_without_importing_downstream_code(tmp_path: Path) -> None:
    config = tmp_path / "config.toml"
    config.write_text(_CONFIG, encoding="utf-8")
    storage = load_operator_config(config).storage
    state_db = StateDB(storage.collector.state_db)
    state_db.put(StateKind.MIGRATION, "internal_audio_cutover", {"format": 1, "phase": "COMPLETE", "fresh": True})
    fake_sync_source = """
async def fake_sync(*args: object, **kwargs: object) -> object:
    del args, kwargs
    from omi_collector.capture.application.collector import NoDataResult
    from omi_collector.capture.domain.ring_protocol import RECORD_SIZE, RingInfo

    return NoDataResult(RingInfo(10, 10, 100, 0, RECORD_SIZE))
"""
    _imports_after(
        "from typer.testing import CliRunner; "
        "from pathlib import Path; "
        "from omi_collector import cli; "
        f"cli.DEFAULT_CONFIG_PATH = Path({str(tmp_path / 'absent-installed-config.toml')!r}); "
        "from omi_collector.capture import cli as capture_cli; "
        f"exec({fake_sync_source!r}); "
        "capture_cli.sync = fake_sync; "
        f"result = CliRunner().invoke(cli.app, ['device', 'sync', '--config', {str(config)!r}, '--confirm-sync']); "
        "assert result.exit_code == 0, result.output"
    )
