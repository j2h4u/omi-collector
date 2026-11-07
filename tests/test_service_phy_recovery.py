from __future__ import annotations

import signal
from pathlib import Path

import pytest
from typer.testing import CliRunner

from omi_collector import cli
from omi_collector.capture import cli as capture_cli
from omi_collector.capture.adapters.collector_guard import guard_is_held
from omi_collector.capture.adapters.collector_state_db import StateDB, StateKind
from omi_collector.capture.adapters.phy_guard import CommandResult, ScopedPhyGuard
from omi_collector.storage_layout import load_operator_config

SNAPSHOT = ("BR1M1SLOT", "LE1MTX", "LE1MRX", "LE2MTX", "LE2MRX")


@pytest.mark.parametrize("scenario", ["no_marker", "restore", "failure", "cancel"])
def test_service_recovers_phy_before_readiness(scenario: str, monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    config = tmp_path / "config.toml"
    config.write_text('pendant_address = "AA:BB:CC:DD:EE:FF"\n', encoding="utf-8")
    layout = load_operator_config(config).storage
    layout.collector.root.mkdir(parents=True, exist_ok=True)
    layout.collector.root.chmod(0o2750)
    db = StateDB(layout.collector.state_db)
    marker: dict[str, object] = {"adapter": "hci0", "selected_phys": list(SNAPSHOT), "version": 2}
    if scenario != "no_marker":
        db.put(StateKind.PHY_RESTORATION, "hci0", marker)
    events: list[str] = []

    async def runner(argv: tuple[str, ...]) -> CommandResult:
        assert guard_is_held(layout.collector.lock)
        events.append("set" if len(argv) > 6 else "verify")
        if scenario == "cancel":
            signal.raise_signal(signal.SIGTERM)
        if scenario in {"failure", "cancel"}:
            return CommandResult(1, "")
        return CommandResult(0, f"selected PHYs: {' '.join(SNAPSHOT)}")

    make_guard = capture_cli.make_guard

    def guard(adapter: str) -> ScopedPhyGuard:
        original = make_guard(adapter)
        return ScopedPhyGuard(adapter, state_db=original._db, runner=runner)

    monkeypatch.setattr(cli, "DEFAULT_CONFIG_PATH", tmp_path / "installed" / "config.toml")
    monkeypatch.setattr(capture_cli, "make_guard", guard)
    monkeypatch.setattr(cli, "_notify_systemd_ready", lambda: events.append("ready"))
    monkeypatch.setattr(cli, "_sync", lambda *_args, **_kwargs: events.append("sync"))
    monkeypatch.setattr(
        "omi_collector.capture.application.operational_telemetry.system_host_boot_id",
        lambda: "00000000-0000-0000-0000-000000000001",
    )

    result = CliRunner().invoke(cli.app, ["service", "--config", str(config)])

    if scenario == "no_marker":
        assert result.exit_code == 0
        assert events == ["ready", "sync"]
    elif scenario == "restore":
        assert result.exit_code == 0
        assert events == ["set", "verify", "ready", "sync"]
        assert db.get(StateKind.PHY_RESTORATION, "hci0") is None
    else:
        assert result.exit_code != 0
        assert events == ["set"]
        assert db.get(StateKind.PHY_RESTORATION, "hci0") == marker
