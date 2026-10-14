from __future__ import annotations

import asyncio
import json
import logging
import sqlite3
import subprocess
import sys
from collections.abc import Coroutine
from contextlib import closing, contextmanager
from pathlib import Path
from shutil import rmtree
from typing import cast

import pytest
from typer.testing import CliRunner

from omi_collector import cli
from omi_collector.capture import cli as device_cli
from omi_collector.capture.adapters.clock_state_store import ClockStateStore
from omi_collector.capture.adapters.collector_state_db import StateDB, StateDBError, StateKind
from omi_collector.capture.adapters.debug_logging import close_debug_logging, configure_debug_logging
from omi_collector.capture.adapters.operational_status import OperationalIdentity, OperationalStatusStore
from omi_collector.capture.adapters.publication import SealResult
from omi_collector.capture.adapters.staging_contract import FatalInternalAudioStorageError, StagingError
from omi_collector.capture.application.collector import CollectionResult
from omi_collector.capture.application.session_lifecycle import ActivityEvent
from omi_collector.capture.domain.clock_state import ClockIntent
from omi_collector.capture.domain.ring_protocol import RECORD_SIZE, RingInfo
from omi_collector.cli import app
from omi_collector.config import DebugLogConfig
from omi_collector.core import package_version
from omi_collector.operator_status import OperatorStatusError
from omi_collector.spool_metrics import SpoolMetricsError
from omi_collector.state_migration import StateMigrationError
from omi_collector.storage_layout import load_operator_config

_CAPTURE_ROOTS: set[Path] = set()


@pytest.fixture(autouse=True)
def _isolate_installed_host_authority(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setattr(cli, "DEFAULT_CONFIG_PATH", tmp_path / "absent-installed-config.toml")


def _capture_root(tmp_path: Path) -> Path:
    root = tmp_path.parent / f"{tmp_path.name}-captures"
    if tmp_path not in _CAPTURE_ROOTS:
        rmtree(root, ignore_errors=True)
        _CAPTURE_ROOTS.add(tmp_path)
    return root


def _layout(tmp_path: Path) -> Path:
    path = tmp_path / "config.toml"
    path.write_text(
        '[pendant]\naddress = "AA:BB:CC:DD:EE:FF"\n[ready]\ntarget_audio_seconds = 3600.0\n',
        encoding="utf-8",
    )
    layout = load_operator_config(path).storage
    if not layout.collector.state_db.exists():
        db = StateDB(layout.collector.state_db)
        db.put(StateKind.MIGRATION, "current", {"version": 1, "phase": "complete"})
        db.put(StateKind.MIGRATION, "internal_audio_cutover", {"format": 1, "phase": "COMPLETE", "fresh": True})
    return path


def test_fatal_internal_audio_storage_exits_without_service_restart(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    loaded = load_operator_config(_layout(tmp_path))
    with pytest.raises(cli.typer.Exit) as state_exit, cli._command_state(loaded):
        raise FatalInternalAudioStorageError("READY latch failed")
    assert state_exit.value.exit_code == 78

    class FatalCapture:
        OperationInterruptedError = RuntimeError

        @staticmethod
        def run(operation: Coroutine[object, object, object]) -> object:
            operation.close()
            raise FatalInternalAudioStorageError("READY latch failed")

    monkeypatch.setattr(cli, "_capture_cli", FatalCapture)

    async def operation() -> object:
        return None

    with pytest.raises(cli.typer.Exit) as operation_exit:
        cli._run_device_operation(operation())
    assert operation_exit.value.exit_code == 78


def test_mutating_cli_rejects_alternate_root_for_same_adapter_with_different_pendant(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    installed = _layout(tmp_path)
    alternate = tmp_path / "alternate" / "config.toml"
    alternate.parent.mkdir()
    alternate.write_text(
        '[pendant]\naddress = "11:22:33:44:55:66"\n[ready]\ntarget_audio_seconds = 3600.0\n',
        encoding="utf-8",
    )
    monkeypatch.setattr(cli, "DEFAULT_CONFIG_PATH", installed)

    with pytest.raises(StateMigrationError, match="Bluetooth adapter is bound"):
        cli._require_host_singleton(load_operator_config(alternate))


def test_migrate_state_cli_requires_stopped_installed_service(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    config_path = _layout(tmp_path)
    monkeypatch.setattr(cli, "DEFAULT_CONFIG_PATH", config_path)
    calls: list[object] = []
    monkeypatch.setattr(
        "omi_collector.internal_audio_migration.migrate_internal_audio", lambda *_args, **_kwargs: calls.append(True)
    )
    monkeypatch.setattr(
        cli,
        "_systemd_service_status",
        lambda: {"available": True, "active_state": "active"},
    )

    result = CliRunner().invoke(app, ["migrate-state", "--config", str(config_path)])

    assert result.exit_code == 1
    assert "confirm the Collector service is stopped" in result.output
    assert calls == []


def test_migrate_state_cli_uses_the_only_cutover_path(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    config_path = _layout(tmp_path)
    monkeypatch.setattr(cli, "DEFAULT_CONFIG_PATH", config_path)
    monkeypatch.setattr(cli, "_systemd_service_status", lambda: {"available": True, "active_state": "inactive"})
    captured: list[tuple[object, object]] = []

    def fake_migrate(layout: object, *, release_metadata: Path) -> dict[str, int]:
        captured.append((layout, release_metadata))
        return {"removed_files": 3}

    monkeypatch.setattr("omi_collector.internal_audio_migration.migrate_internal_audio", fake_migrate)

    result = CliRunner().invoke(app, ["migrate-state", "--config", str(config_path)])

    assert result.exit_code == 0
    assert json.loads(result.output) == {"removed_files": 3}
    assert captured[0][1] == Path(cli.sys.prefix) / "share/omi-collector/release.json"


def test_migrate_state_cli_uses_selected_release_metadata(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    config_path = _layout(tmp_path)
    monkeypatch.setattr(cli, "DEFAULT_CONFIG_PATH", config_path)
    monkeypatch.setattr(cli, "_systemd_service_status", lambda: {"available": True, "active_state": "inactive"})
    captured: list[tuple[object, Path]] = []

    def fake_internal(layout: object, *, release_metadata: Path) -> dict[str, int]:
        captured.append((layout, release_metadata))
        return {"imported_sources": 2, "removed_files": 4}

    monkeypatch.setattr("omi_collector.internal_audio_migration.migrate_internal_audio", fake_internal)
    result = CliRunner().invoke(app, ["migrate-state", "--config", str(config_path)])

    assert result.exit_code == 0
    assert json.loads(result.output) == {"imported_sources": 2, "removed_files": 4}
    assert captured[0][1] == Path(cli.sys.prefix) / "share/omi-collector/release.json"


def test_mutating_cli_distinguishes_required_cutover_from_sqlite_busy(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    config_path = _layout(tmp_path)
    layout = load_operator_config(config_path).storage
    with closing(StateDB(layout.collector.state_db).open_connection()) as connection:
        connection.execute("PRAGMA user_version=1")
        connection.commit()

    monkeypatch.setattr(cli, "_service_locked", lambda *_args: None)
    cutover = CliRunner().invoke(app, ["service", "--config", str(config_path)])
    assert cutover.exit_code == 78
    assert "requires stopped-service internal audio cutover" in cutover.output

    def busy(_layout: object) -> StateDB:
        raise StateDBError("database is locked")

    monkeypatch.setattr("omi_collector.state_migration.open_runtime_state", busy)
    locked = CliRunner().invoke(app, ["service", "--config", str(config_path)])
    assert locked.exit_code == 1
    assert "database is locked" in locked.output


def test_mutating_cli_stops_restart_loop_for_inaccessible_hot_journal(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    config_path = _layout(tmp_path)
    monkeypatch.setattr(cli, "_service_locked", lambda *_args: None)
    state_file = load_operator_config(config_path).storage.collector.state_db
    subprocess.run(
        (
            sys.executable,
            "-c",
            """
import os
import sqlite3
import sys
connection = sqlite3.connect(sys.argv[1], isolation_level=None)
connection.execute("BEGIN IMMEDIATE")
connection.execute("UPDATE state SET payload='transient' WHERE kind='migration' AND id='current'")
os._exit(0)
""",
            str(state_file),
        ),
        check=True,
        timeout=10,
    )
    journal = state_file.parent / f"{state_file.name}-journal"
    assert journal.is_file()
    journal.chmod(0)
    try:
        result = CliRunner().invoke(app, ["service", "--config", str(config_path)])
        assert result.exit_code == 78
        assert "journal access needs stopped-service repair" in result.output
    finally:
        journal.chmod(0o600)


def test_mutating_cli_recovers_accessible_hot_journal(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    config_path = _layout(tmp_path)
    monkeypatch.setattr(cli, "_service_locked", lambda *_args: None)
    state_file = load_operator_config(config_path).storage.collector.state_db
    subprocess.run(
        (
            sys.executable,
            "-c",
            """
import os
import sqlite3
import sys
connection = sqlite3.connect(sys.argv[1], isolation_level=None)
connection.execute("BEGIN IMMEDIATE")
connection.execute("UPDATE state SET payload='transient' WHERE kind='migration' AND id='current'")
os._exit(0)
""",
            str(state_file),
        ),
        check=True,
        timeout=10,
    )
    journal = state_file.parent / f"{state_file.name}-journal"
    assert journal.is_file()

    result = CliRunner().invoke(app, ["service", "--config", str(config_path)])

    assert result.exit_code == 0, result.output
    assert StateDB(state_file).get(StateKind.MIGRATION, "current") == {"version": 1, "phase": "complete"}


@pytest.mark.parametrize("journal_kind", ["directory", "symlink"])
def test_mutating_cli_stops_for_nonregular_journal(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, journal_kind: str
) -> None:
    config_path = _layout(tmp_path)
    monkeypatch.setattr(cli, "_service_locked", lambda *_args: None)
    state_file = load_operator_config(config_path).storage.collector.state_db
    journal = state_file.parent / f"{state_file.name}-journal"
    if journal_kind == "directory":
        journal.mkdir()
    else:
        journal.symlink_to(tmp_path / "absent")

    result = CliRunner().invoke(app, ["service", "--config", str(config_path)])

    assert result.exit_code == 78, result.output
    assert "journal is unsafe" in result.output


@pytest.mark.parametrize("malformed_draft", [False, True])
def test_migrate_state_reports_invalid_v1_inputs_without_traceback(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, malformed_draft: bool
) -> None:
    config_path = _layout(tmp_path)
    layout = load_operator_config(config_path).storage
    layout.collector.state_db.unlink()
    if malformed_draft:
        with closing(sqlite3.connect(layout.collector.state_db)) as connection:
            connection.execute(
                "CREATE TABLE state(kind TEXT NOT NULL,id TEXT NOT NULL,payload TEXT NOT NULL,"
                "PRIMARY KEY(kind,id)) WITHOUT ROWID"
            )
            connection.execute("PRAGMA user_version=1")
            connection.execute(
                "INSERT INTO state VALUES ('migration','current','{\"phase\":\"complete\",\"version\":1}')"
            )
            connection.commit()
        layout.draft.mkdir()
        (layout.draft / "invalid-draft").write_bytes(b"unclassified")
    else:
        layout.collector.state_db.write_bytes(b"not a SQLite database")
    monkeypatch.setattr("omi_collector.internal_audio_migration._release_revision", lambda _path: "a" * 40)

    result = CliRunner().invoke(app, ["migrate-state", "--config", str(config_path)])

    assert result.exit_code == 1
    assert "Traceback" not in result.output
    if malformed_draft:
        assert "draft" in result.output
    else:
        assert "database" in result.output.lower()


def test_service_runs_under_one_guarded_state_scope(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    config_path = _layout(tmp_path)
    calls: list[str] = []

    @contextmanager
    def fake_command_state(_loaded: object):
        calls.append("guard-enter")
        yield object()
        calls.append("guard-exit")

    monkeypatch.setattr(cli, "_command_state", fake_command_state)
    monkeypatch.setattr(cli, "_service_locked", lambda *_args: calls.append("service"))

    result = CliRunner().invoke(app, ["service", "--config", str(config_path)])

    assert result.exit_code == 0
    assert calls == ["guard-enter", "service", "guard-exit"]


def test_version_flag_reports_package_version() -> None:
    result = CliRunner().invoke(app, ["--version"])

    assert result.exit_code == 0
    assert result.output.strip() == package_version()


def test_health_command_reports_ok() -> None:
    result = CliRunner().invoke(app, ["health"])

    assert result.exit_code == 0
    assert result.output.strip() == "ok"


def test_staging_keeps_raw_drafts_inside_collector_state(tmp_path: Path) -> None:
    config_path = _layout(tmp_path)
    config_path.write_text(
        '[pendant]\naddress = "AA:BB:CC:DD:EE:FF"\n[ready]\ntarget_audio_seconds = 123.0\n',
        encoding="utf-8",
    )
    store = cli._staging(load_operator_config(config_path))

    assert store.paths.capture_root == tmp_path / "draft"
    assert store._filesystem._ready.target_audio_seconds == 123.0


def test_config_check_reports_canonical_path(tmp_path: Path) -> None:
    path = _layout(tmp_path)
    result = CliRunner().invoke(app, ["config", "check", "--config", str(path)])

    assert result.exit_code == 0
    assert json.loads(result.output) == {"config": str(path), "status": "config_valid"}


def test_service_announces_readiness_after_preflight(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    path = _layout(tmp_path)
    sync_options: list[dict[str, object]] = []
    monkeypatch.delenv("INVOCATION_ID", raising=False)
    monkeypatch.setattr(
        "omi_collector.capture.application.operational_telemetry.system_host_boot_id",
        lambda: "00000000-0000-0000-0000-000000000001",
    )

    def fake_sync(*_args: object, **kwargs: object) -> None:
        sync_options.append(kwargs)

    monkeypatch.setattr(cli, "_sync", fake_sync)

    result = CliRunner().invoke(app, ["service", "--config", str(path)])

    assert result.exit_code == 0
    assert result.stdout == ""
    assert json.loads(result.stderr) == {"config": str(path), "status": "deployment_ready"}
    assert len(sync_options) == 1
    assert sync_options[0]["force_1m"] is False
    assert sync_options[0]["log_level"] is cli.SyncLogLevel.INFO
    from omi_collector.capture.adapters.operational_status import OperationalStatusStore

    status = cast(OperationalStatusStore, sync_options[0]["operational_status"])
    assert status.identity.boot_id == "00000000-0000-0000-0000-000000000001"
    assert isinstance(status.identity.invocation_id, str) and len(status.identity.invocation_id) == 32
    assert status._closed


def test_service_does_not_announce_readiness_for_invalid_config(tmp_path: Path) -> None:
    path = tmp_path / "config.toml"
    path.write_text("not = [valid", encoding="utf-8")

    result = CliRunner().invoke(app, ["service", "--config", str(path)])

    assert result.exit_code == 2
    assert result.stdout == ""
    assert "deployment_ready" not in result.stderr
    assert "config TOML is unreadable or malformed" in result.stderr


def test_device_status_only_flags_a_confirmed_inactive_service(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(cli, "collect_operator_status", lambda *_args, **_kwargs: {"status": "ok"})
    service_statuses = (
        ({"available": False, "error": "OSError", "unit": "omi-collector.service"}, "ok"),
        ({"available": True, "active_state": "inactive", "unit": "omi-collector.service"}, "attention"),
        ({"available": True, "active_state": "active", "unit": "omi-collector.service"}, "ok"),
    )

    for service_status, expected_status in service_statuses:
        monkeypatch.setattr(cli, "_systemd_service_status", lambda service_status=service_status: service_status)
        result = CliRunner().invoke(app, ["device", "status", "--config", str(_layout(tmp_path))])

        assert result.exit_code == 0
        payload = cast(dict[str, object], json.loads(result.output))
        assert payload["status"] == expected_status
        assert payload["service"] == service_status


def test_device_status_fetches_systemd_invocation_before_collecting_snapshot(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    events: list[str] = []
    observed: list[OperationalIdentity] = []

    def service_status() -> dict[str, object]:
        events.append("systemd")
        return {
            "available": True,
            "active_state": "active",
            "invocation_id": "00000000000000000000000000000003",
        }

    def collect(*_args: object, **kwargs: object) -> dict[str, object]:
        events.append("collect")
        identity = kwargs["identity"]
        assert isinstance(identity, OperationalIdentity)
        observed.append(identity)
        return {"status": "unknown"}

    monkeypatch.setattr(cli, "_systemd_service_status", service_status)
    monkeypatch.setattr(cli, "collect_operator_status", collect)

    result = CliRunner().invoke(app, ["device", "status", "--config", str(_layout(tmp_path))])

    assert result.exit_code == 0
    assert events == ["systemd", "collect"]
    assert observed[0].invocation_id == "00000000000000000000000000000003"


def test_ble_link_record_is_redacted_before_journal_logging(tmp_path: Path, caplog: pytest.LogCaptureFixture) -> None:
    info_lines: list[str] = []
    debug_lines: list[str] = []
    debug_reporter = cli.SyncProgressReporter(cli.SyncLogLevel.DEBUG, emit=debug_lines.append)

    record = {"event": "ble_link_session", "address": "AA:BB", "handle": 7}
    logger = configure_debug_logging(tmp_path, DebugLogConfig(logger_name="tests.debug.ble_link_report"))
    try:
        with caplog.at_level(logging.DEBUG, logger="tests.debug.ble_link_report"):
            info_reporter = cli.SyncProgressReporter(emit=info_lines.append, debug_logger=logger)
            info_reporter.report_ble_link(record)
    finally:
        close_debug_logging(logger)

    assert info_lines == []
    assert 'fields={"record":{"event":"ble_link_session","handle":7}}' in caplog.text
    assert "AA:BB" not in caplog.text

    debug_reporter.report_ble_link(record)
    assert [json.loads(line) for line in debug_lines] == [{"event": "ble_link_session", "handle": 7}]


def test_systemd_service_status_reports_supervisor_fields(monkeypatch: pytest.MonkeyPatch) -> None:
    output = """ActiveState=active
SubState=running
MainPID=42
NRestarts=0
ExecMainStatus=0
ActiveEnterTimestamp=Sat 2026-09-12 10:00:00 +05"""
    completed = cli.subprocess.CompletedProcess([], 0, stdout=output, stderr="")
    monkeypatch.setattr(cli.subprocess, "run", lambda *_args, **_kwargs: completed)

    assert cli._systemd_service_status() == {
        "active_enter_timestamp": "Sat 2026-09-12 10:00:00 +05",
        "active_state": "active",
        "available": True,
        "exec_main_status": 0,
        "invocation_id": None,
        "main_pid": 42,
        "restart_count": 0,
        "sub_state": "running",
        "unit": "omi-collector.service",
    }


def test_systemd_service_status_reports_command_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    completed = cli.subprocess.CompletedProcess([], 1, stdout="", stderr="unit unavailable\n")
    monkeypatch.setattr(cli.subprocess, "run", lambda *_args, **_kwargs: completed)

    assert cli._systemd_service_status() == {
        "available": False,
        "error": "unit unavailable",
        "unit": "omi-collector.service",
    }


@pytest.mark.parametrize("error", [OSError(), cli.subprocess.TimeoutExpired(["systemctl"], 3)])
def test_systemd_service_status_reports_execution_failure(
    monkeypatch: pytest.MonkeyPatch, error: OSError | cli.subprocess.TimeoutExpired
) -> None:
    def fail(*_args: object, **_kwargs: object) -> None:
        raise error

    monkeypatch.setattr(cli.subprocess, "run", fail)

    assert cli._systemd_service_status() == {
        "available": False,
        "error": type(error).__name__,
        "unit": "omi-collector.service",
    }


def test_systemd_service_status_parses_text_and_handles_nonzero_exit(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    systemctl = fake_bin / "systemctl"
    systemctl.write_text(
        "#!/bin/sh\n"
        'if [ "$2" = "failing.service" ]; then\n'
        "  printf 'unit unavailable\\n' >&2\n"
        "  exit 1\n"
        "fi\n"
        "printf 'ActiveState=active\\nSubState=running\\nMainPID=42\\nNRestarts=0\\n'\n"
        "printf 'ExecMainStatus=0\\nActiveEnterTimestamp=Sat 2026-09-12 10:00:00 +05\\n'\n",
        encoding="utf-8",
    )
    systemctl.chmod(0o755)
    monkeypatch.setenv("PATH", str(fake_bin))

    assert cli._systemd_service_status("healthy.service") == {
        "active_enter_timestamp": "Sat 2026-09-12 10:00:00 +05",
        "active_state": "active",
        "available": True,
        "exec_main_status": 0,
        "invocation_id": None,
        "main_pid": 42,
        "restart_count": 0,
        "sub_state": "running",
        "unit": "healthy.service",
    }
    assert cli._systemd_service_status("failing.service") == {
        "available": False,
        "error": "unit unavailable",
        "unit": "failing.service",
    }


def test_phy_check_and_recover_commands_report_success(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    config_path = _layout(tmp_path)
    events: list[tuple[str, str]] = []

    async def fake_phy_check(adapter: str) -> None:
        events.append(("check", adapter))

    async def fake_recover_phy(adapter: str) -> None:
        events.append(("recover", adapter))

    monkeypatch.setattr(device_cli, "phy_check", fake_phy_check)
    monkeypatch.setattr(device_cli, "recover_phy", fake_recover_phy)
    runner = CliRunner()

    phy_result = runner.invoke(app, ["device", "phy-check", "--config", str(config_path), "--confirm-host-change"])
    recover_result = runner.invoke(app, ["device", "recover-phy", "--config", str(config_path), "--adapter", "hci1"])

    assert phy_result.exit_code == 0
    assert json.loads(phy_result.output) == {"status": "phy_restored"}
    assert recover_result.exit_code == 0
    assert json.loads(recover_result.output) == {"status": "phy_recovered"}
    assert events == [("check", "hci0"), ("recover", "hci1")]


def test_device_operation_interruption_is_reported(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    config_path = _layout(tmp_path)
    created_operations: list[Coroutine[object, object, None]] = []

    async def fake_phy_check(_adapter: str) -> None:
        return

    def track_phy_check(_adapter: str) -> Coroutine[object, object, None]:
        operation = fake_phy_check(_adapter)
        created_operations.append(operation)
        return operation

    def interrupted(operation: Coroutine[object, object, object]) -> object:
        operation.close()
        raise device_cli.OperationInterruptedError("interrupted safely")

    monkeypatch.setattr(device_cli, "phy_check", track_phy_check)
    monkeypatch.setattr(device_cli, "run", interrupted)

    try:
        result = CliRunner().invoke(app, ["device", "phy-check", "--config", str(config_path), "--confirm-host-change"])
    finally:
        for operation in created_operations:
            operation.close()

    assert result.exit_code == 0
    assert result.stdout == ""
    assert result.stderr.strip() == "interrupted safely"


def test_device_operation_failure_is_reported_on_stderr(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    config_path = _layout(tmp_path)

    async def fake_phy_check(_adapter: str) -> None:
        return

    def failed(operation: Coroutine[object, object, object]) -> object:
        operation.close()
        raise RuntimeError("private detail")

    monkeypatch.setattr(device_cli, "phy_check", fake_phy_check)
    monkeypatch.setattr(device_cli, "run", failed)

    result = CliRunner().invoke(app, ["device", "phy-check", "--config", str(config_path), "--confirm-host-change"])

    assert result.exit_code == 1
    assert result.stdout == ""
    assert result.stderr.strip() == "device operation failed: RuntimeError"


def test_device_host_change_and_read_routes_refuse_without_confirmation(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    config_path = _layout(tmp_path)
    calls: list[str] = []

    async def fake_operation(*_args: object) -> None:
        return

    def must_not_run(operation: Coroutine[object, object, object]) -> object:
        operation.close()
        calls.append("device operation")
        raise RuntimeError("confirmation guard was bypassed")

    monkeypatch.setattr(device_cli, "phy_check", fake_operation)
    monkeypatch.setattr(device_cli, "probe", fake_operation)
    monkeypatch.setattr(device_cli, "info", fake_operation)
    monkeypatch.setattr(device_cli, "collect", fake_operation)
    monkeypatch.setattr(device_cli, "run", must_not_run)
    monkeypatch.setattr(cli, "_staging", lambda _loaded, **_kwargs: object())
    monkeypatch.setattr(cli, "_preflight_storage", lambda _staging: None)
    commands = (
        (["device", "phy-check"], "--confirm-host-change"),
        (["device", "probe", "--config", str(config_path)], "--confirm-host-change"),
        (["device", "info", "--config", str(config_path)], "--confirm-host-change"),
        (["device", "collect", "--config", str(config_path)], "--confirm-read"),
    )

    for arguments, required_flag in commands:
        result = CliRunner().invoke(app, arguments)

        assert result.exit_code == 2
        assert result.stdout == ""
        assert result.stderr.strip() == f"refusing operation without {required_flag}"
        assert calls == []


def test_sync_requires_explicit_read_advance_confirmation(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    async def fail_sync(*_args: object, **_kwargs: object) -> object:
        raise AssertionError("sync must not start without confirmation")

    monkeypatch.setattr(device_cli, "sync", fail_sync)
    result = CliRunner().invoke(
        app,
        ["device", "sync", "--config", str(_layout(tmp_path))],
    )

    assert result.exit_code == 2
    assert result.stdout == ""
    assert "--confirm-sync" in result.stderr


def test_sync_reports_progress_on_stderr_and_metrics_on_stdout(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    now = [100.0]
    monkeypatch.setattr(cli.time, "monotonic", lambda: now[0])
    bundle = tmp_path / "omi" / "10-11-deadbeef"
    bundle.mkdir(parents=True)
    (bundle / "manifest.json").write_text('{"raw_sha256":"abc123"}', encoding="utf-8")
    expected = CollectionResult(
        RingInfo(10, 11, 100, 0, RECORD_SIZE),
        1,
        SealResult(bundle, False),
        next_sequence=11,
        advance_confirmed=True,
    )

    async def fake_sync(
        _address: str,
        adapter: str,
        _staging: object,
        *args: object,
        **kwargs: object,
    ) -> CollectionResult:
        progress = cast(device_cli.ProgressReporter, args[0])
        force_1m = cast(bool, kwargs["force_1m"])
        presence = kwargs["presence"]
        link_terminal_callback = kwargs["link_terminal_callback"]
        debug_logger = kwargs["debug_logger"]
        del _staging
        assert adapter == "hci0"
        assert force_1m is False
        assert isinstance(presence, device_cli.PresenceScheduler)
        assert callable(link_terminal_callback)
        assert debug_logger is not None
        progress(device_cli.DownloadProgress(0.0, RECORD_SIZE, 0.0, 0.0, 0, 0.0, 1, 1))
        now[0] = 102.0
        return expected

    monkeypatch.setattr(device_cli, "sync", fake_sync)
    result = CliRunner().invoke(
        app,
        [
            "device",
            "sync",
            "--config",
            str(_layout(tmp_path)),
            "--confirm-sync",
        ],
    )

    assert result.exit_code == 0
    assert result.stderr
    lines = result.stderr.splitlines()
    assert any(json.loads(line)["status"] == "progress" for line in lines)
    final = cast(dict[str, object], json.loads(result.stdout))
    assert final["status"] == "synced"
    assert final["advance_confirmed"] is True
    assert final["payload_bytes"] == RECORD_SIZE
    assert final["remaining_packets"] == 0
    assert final["elapsed_seconds"] == 2.0
    assert cast(float, final["bytes_per_second"]) >= 0


def test_collect_reports_elapsed_time_in_final_metrics(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    now = [100.0]
    monkeypatch.setattr(cli.time, "monotonic", lambda: now[0])

    async def fake_collect(*_args: object) -> device_cli.collector.NoDataResult:
        now[0] = 102.0
        return device_cli.collector.NoDataResult(RingInfo(10, 10, 100, 0, RECORD_SIZE))

    monkeypatch.setattr(device_cli, "collect", fake_collect)
    result = CliRunner().invoke(
        app,
        ["device", "collect", "--config", str(_layout(tmp_path)), "--confirm-read"],
    )

    assert result.exit_code == 0
    payload = cast(dict[str, object], json.loads(result.stdout))
    assert payload["status"] == "no_data"
    assert payload["elapsed_seconds"] == 2.0


def test_collect_preflight_failure_is_reported_on_stderr(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    class FailingStaging:
        def preflight_storage(self) -> None:
            raise StagingError("read-only filesystem")

    async def must_not_collect(*_args: object) -> device_cli.collector.NoDataResult:
        raise AssertionError("collect must not start after preflight failure")

    monkeypatch.setattr(cli, "_staging", lambda _loaded, **_kwargs: FailingStaging())
    monkeypatch.setattr(device_cli, "collect", must_not_collect)
    result = CliRunner().invoke(
        app,
        ["device", "collect", "--config", str(_layout(tmp_path)), "--confirm-read"],
    )

    assert result.exit_code == 1
    assert result.stdout == ""
    assert result.stderr.strip() == "storage preflight failed: read-only filesystem"


def test_device_metrics_failure_is_reported_on_stderr(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    config_path = _layout(tmp_path)
    cli._staging(load_operator_config(config_path))

    def failed(*_args: object, **_kwargs: object) -> object:
        raise SpoolMetricsError("manifest is malformed")

    monkeypatch.setattr(cli, "collect_spool_metrics", failed)
    result = CliRunner().invoke(app, ["device", "metrics", "--config", str(config_path)])

    assert result.exit_code == 1
    assert result.stdout == ""
    assert result.stderr.strip() == "manifest is malformed"


def test_device_status_failure_is_reported_on_stderr(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    def failed(*_args: object, **_kwargs: object) -> dict[str, object]:
        raise OperatorStatusError("status evidence is malformed")

    monkeypatch.setattr(cli, "collect_operator_status", failed)
    result = CliRunner().invoke(app, ["device", "status", "--config", str(_layout(tmp_path))])

    assert result.exit_code == 1
    assert result.stdout == ""
    assert result.stderr.strip() == "status evidence is malformed"


def _sync_progress(  # noqa: PLR0913
    *,
    state: str = "progress",
    eta_seconds: float | None = 2.0,
    operational_event: dict[str, object] | None = None,
    phase: str | None = None,
    error_type: str | None = None,
    error_message: str | None = None,
) -> device_cli.DownloadProgress:
    return device_cli.DownloadProgress(
        1.0,
        RECORD_SIZE,
        12.5,
        1.0,
        2,
        eta_seconds,
        1,
        3,
        state=state,
        operational_event=operational_event,
        phase=phase,
        error_type=error_type,
        error_message=error_message,
    )


def test_sync_reporter_info_suppresses_routine_polling() -> None:
    lines: list[str] = []
    reporter = cli.SyncProgressReporter(emit=lines.append)

    reporter(_sync_progress(state="away"))
    reporter(_sync_progress(state="drained"))
    reporter(_sync_progress(state="connecting"))
    reporter(_sync_progress(state="storage_wait"))
    reporter(
        _sync_progress(
            state="operational",
            operational_event={"event": "pendant_observation", "unread_packets": 0},
        )
    )
    reporter(
        _sync_progress(
            state="operational",
            operational_event={"event": "pendant_clock_sync", "outcome": "within_threshold"},
        )
    )

    assert len(lines) == 1
    assert json.loads(lines[0])["event"] == "pendant_observation"


def test_sync_reporter_info_emits_verified_clock_correction_to_system_journal() -> None:
    lines: list[str] = []
    reporter = cli.SyncProgressReporter(emit=lines.append)
    correction = {
        "event": "pendant_clock_sync",
        "outcome": "verified",
        "drift_seconds": 2359.68,
        "target_epoch": 1789128032,
        "boundary_sequence_min": 5898589,
        "boundary_sequence_max": 5898591,
    }

    reporter(_sync_progress(state="operational", operational_event=correction))

    assert len(lines) == 1
    journal_event = cast(dict[str, object], json.loads(lines[0]))
    assert all(journal_event[key] == value for key, value in correction.items())


@pytest.mark.parametrize(
    ("event", "expected"),
    [
        ({"outcome": "verified"}, True),
        ({"outcome": "verified", "boundary_ambiguous": True}, False),
        ({"outcome": "verified", "reconciliation": "pending"}, False),
        ({"outcome": "verified", "reconciled_operations": 0}, True),
        ({"outcome": "verified", "reconciled_operations": False}, False),
        ({"outcome": "verified", "reconciled_operations": -1}, False),
    ],
)
def test_clock_reconciliation_only_accepts_conclusive_event(event: dict[str, object], expected: bool) -> None:
    assert cli._clock_reconciliation_is_conclusive(event) is expected


def test_clock_status_clears_only_after_conclusive_reconciliation_without_pending_intent(tmp_path: Path) -> None:
    staging = cli._staging(load_operator_config(_layout(tmp_path)))
    status = OperationalStatusStore(
        staging.state_db,
        OperationalIdentity("00000000-0000-0000-0000-000000000001", "00000000000000000000000000000001"),
    )
    status.initialize()
    reporter = cli.SyncProgressReporter(operational_status=status, staging=staging, emit=lambda _line: None)

    reporter(
        _sync_progress(
            state="operational", operational_event={"event": "pendant_clock_sync", "outcome": "intent_persist_failed"}
        )
    )
    assert status.as_dict()["clock"] == "blocked"
    reporter(
        _sync_progress(
            state="operational",
            operational_event={"event": "pendant_clock_sync", "outcome": "verified", "boundary_ambiguous": True},
        )
    )
    assert status.as_dict()["clock"] == "blocked"

    clock = ClockStateStore(staging.state_db)
    clock.prepare(ClockIntent("pending", "prepared", 100, 200, 150.0, 7))
    reporter(
        _sync_progress(state="operational", operational_event={"event": "pendant_clock_sync", "outcome": "verified"})
    )
    assert status.as_dict()["clock"] == "blocked"
    clock.cancel_prepared("pending")
    reporter(
        _sync_progress(state="operational", operational_event={"event": "pendant_clock_sync", "outcome": "verified"})
    )
    assert status.as_dict() == {"quality": "unknown", "publication": "unknown", "clock": "clear"}
    status.close()


def test_sync_reporter_logs_filtered_debug_callback(tmp_path: Path, caplog: pytest.LogCaptureFixture) -> None:
    lines: list[str] = []
    logger = configure_debug_logging(tmp_path, DebugLogConfig(logger_name="tests.debug.progress"))
    try:
        with caplog.at_level(logging.DEBUG, logger="tests.debug.progress"):
            reporter = cli.SyncProgressReporter(emit=lines.append, debug_logger=logger)
            reporter(_sync_progress(state="away"))
    finally:
        close_debug_logging(logger)

    assert lines == []
    assert any('"status":"away"' in record.message for record in caplog.records)
    assert not (tmp_path / "debug.jsonl").exists()


def test_sync_reporter_info_emits_clean_drain_cooldown_event() -> None:
    lines: list[str] = []
    reporter = cli.SyncProgressReporter(emit=lines.append)
    duration = 7.5
    next_attempt_in_seconds = 4.25
    callback = device_cli._activity_callback(reporter)
    assert callback is not None

    asyncio.run(
        cast(
            Coroutine[object, object, object],
            callback(
                ActivityEvent(
                    "cooldown_started",
                    reason="clean_drain",
                    duration_seconds=duration,
                    next_attempt_in_seconds=next_attempt_in_seconds,
                )
            ),
        )
    )

    assert len(lines) == 1
    assert json.loads(lines[0]) == {
        "bytes_per_second": 0,
        "duration_seconds": duration,
        "elapsed_seconds": 0,
        "eta_seconds": None,
        "next_attempt_in_seconds": next_attempt_in_seconds,
        "payload_bytes": 0,
        "reason": "clean_drain",
        "records_completed": 0,
        "records_per_second": 0,
        "records_total": 0,
        "remaining_bytes": 0,
        "remaining_packets": 0,
        "status": "cooldown_started",
        "total_bytes": 0,
    }


def test_sync_reporter_debug_exposes_routine_polling() -> None:
    lines: list[str] = []
    reporter = cli.SyncProgressReporter(cli.SyncLogLevel.DEBUG, emit=lines.append)

    reporter(_sync_progress(state="away"))
    reporter(_sync_progress(state="storage_wait"))
    reporter(
        _sync_progress(
            state="operational",
            operational_event={"event": "pendant_observation", "unread_packets": 0},
        )
    )

    assert [cast(dict[str, object], json.loads(line))["status"] for line in lines] == [
        "away",
        "storage_wait",
        "operational",
    ]


def test_sync_reporter_debug_keeps_repeated_session_errors() -> None:
    lines: list[str] = []
    reporter = cli.SyncProgressReporter(cli.SyncLogLevel.DEBUG, emit=lines.append)
    error = _sync_progress(
        state="session_error",
        error_type="UnexpectedError",
        error_message="unexpected failure",
        phase="connect",
    )

    reporter(error)
    reporter(error)

    assert [json.loads(line)["status"] for line in lines] == ["session_error", "session_error"]


def test_sync_reporter_deduplicates_expected_errors_until_recovery() -> None:
    lines: list[str] = []
    reporter = cli.SyncProgressReporter(emit=lines.append)
    error = _sync_progress(
        state="session_error",
        error_type="RingTransportUnavailableError",
        error_message="BLE transport unavailable",
        phase="connect",
    )

    reporter(error)
    reporter(error)
    reporter(
        _sync_progress(
            state="operational",
            operational_event={"event": "pendant_observation", "unread_packets": 1},
        )
    )
    reporter(
        _sync_progress(
            state="operational",
            operational_event={"event": "pendant_observation", "unread_packets": 1},
        )
    )

    assert [cast(dict[str, object], json.loads(line))["status"] for line in lines] == [
        "session_error",
        "recovered",
        "operational",
    ]


def test_sync_reporter_deduplicates_notification_overflow_until_recovery() -> None:
    lines: list[str] = []
    reporter = cli.SyncProgressReporter(emit=lines.append)
    error = _sync_progress(
        state="session_error",
        error_type="NotificationOverflowError",
        error_message="notification queue overflow",
        phase="read/reconcile",
    )

    reporter(error)
    reporter(error)
    reporter(_sync_progress(state="progress"))
    reporter(error)

    assert [cast(dict[str, object], json.loads(line))["status"] for line in lines] == [
        "session_error",
        "recovered",
        "progress",
        "session_error",
    ]


def test_sync_reporter_deduplicates_unexpected_errors_and_distinct_phases() -> None:
    lines: list[str] = []
    reporter = cli.SyncProgressReporter(emit=lines.append)

    for phase in ("connect", "connect", "read", "read"):
        reporter(
            _sync_progress(
                state="session_error",
                error_type="UnexpectedError",
                error_message="unexpected failure",
                phase=phase,
            )
        )

    assert [json.loads(line)["phase"] for line in lines] == ["connect", "read"]


def test_sync_reporter_deduplicates_error_keys_until_recovery() -> None:
    lines: list[str] = []
    reporter = cli.SyncProgressReporter(emit=lines.append)

    for error_message in ("first", "second", "first"):
        reporter(
            _sync_progress(
                state="session_error",
                error_type="UnexpectedError",
                error_message=error_message,
                phase="connect",
            )
        )

    assert [json.loads(line)["error_message"] for line in lines] == ["first", "second"]


def test_sync_reporter_only_recovers_on_healthy_activity() -> None:
    lines: list[str] = []
    reporter = cli.SyncProgressReporter(emit=lines.append)
    reporter(
        _sync_progress(
            state="session_error",
            error_type="UnexpectedError",
            error_message="unexpected failure",
            phase="connect",
        )
    )
    reporter(_sync_progress(state="operational", operational_event={"event": "firmware_observation_error"}))
    reporter(_sync_progress(state="operational", operational_event={"event": "other_error"}))
    assert [json.loads(line)["status"] for line in lines] == ["session_error", "operational", "operational"]

    reporter(_sync_progress(state="reading"))
    assert [json.loads(line)["status"] for line in lines] == ["session_error", "operational", "operational"]
    reporter(_sync_progress(state="progress"))
    assert [json.loads(line)["status"] for line in lines] == [
        "session_error",
        "operational",
        "operational",
        "recovered",
        "progress",
    ]


def test_sync_reporter_recovers_only_after_verified_clock_sync() -> None:
    lines: list[str] = []
    reporter = cli.SyncProgressReporter(emit=lines.append)
    reporter(_sync_progress(state="session_error", error_type="UnexpectedError", error_message="failed"))
    reporter(
        _sync_progress(
            state="operational",
            operational_event={"event": "pendant_clock_sync", "outcome": "verification_failed"},
        )
    )

    assert [json.loads(line)["status"] for line in lines] == ["session_error", "operational"]

    reporter(
        _sync_progress(
            state="operational",
            operational_event={"event": "pendant_clock_sync", "outcome": "verified"},
        )
    )

    assert [json.loads(line)["status"] for line in lines] == [
        "session_error",
        "operational",
        "recovered",
        "operational",
    ]


def test_sync_reporter_uses_unknown_error_type_in_recovery_notice() -> None:
    lines: list[str] = []
    reporter = cli.SyncProgressReporter(emit=lines.append)
    reporter(_sync_progress(state="session_error", error_message="missing type"))
    reporter(_sync_progress(state="progress"))

    payloads = [cast(dict[str, object], json.loads(line)) for line in lines]
    recovery = next(payload for payload in payloads if payload["status"] == "recovered")
    assert recovery == {"recovered_from": "unknown", "status": "recovered"}


def test_sync_reporter_info_emits_first_and_changed_observation_health() -> None:
    lines: list[str] = []
    reporter = cli.SyncProgressReporter(emit=lines.append)
    base = {
        "event": "pendant_observation",
        "battery_percent": 90,
        "rtc_valid": True,
        "model": "Omi CV 1",
        "firmware": "3.0.21",
        "hardware": "5.0",
        "manufacturer": "Based Hardware",
        "capacity_packets": 10,
        "packet_size": 444,
        "dropped_packets": 0,
        "optional_outcomes": {"battery": "ok"},
        "read_sequence": 1,
        "write_sequence": 2,
        "unread_packets": 1,
        "used_bytes": 444,
        "free_bytes": 100,
        "device_time_epoch": 123,
    }
    reporter(_sync_progress(state="operational", operational_event=base))
    changed_dynamic = {**base, "read_sequence": 2, "unread_packets": 0, "device_time_epoch": 124}
    reporter(_sync_progress(state="operational", operational_event=changed_dynamic))
    reporter(_sync_progress(state="operational", operational_event={**changed_dynamic, "battery_percent": 89}))

    assert len(lines) == 2
    assert [json.loads(line)["battery_percent"] for line in lines] == [90, 89]


def test_sync_reporter_keeps_one_aggregate_progress_line_at_info() -> None:
    lines: list[str] = []
    reporter = cli.SyncProgressReporter(emit=lines.append)

    reporter(_sync_progress(state="progress", eta_seconds=2.0))
    reporter(_sync_progress(state="progress", eta_seconds=1.0))

    assert len(lines) == 1
    payload = cast(dict[str, object], json.loads(lines[0]))
    assert payload["status"] == "progress"
    assert payload["bytes_per_second"] == 12.5
    assert payload["eta_seconds"] == 2.0

    reporter(_sync_progress(state="reading"))
    reporter(_sync_progress(state="progress", eta_seconds=1.0))

    assert json.loads(lines[-1])["status"] == "progress"
    assert [json.loads(line)["status"] for line in lines] == ["progress", "progress"]


def test_sync_reporter_keeps_fatal_events_at_info() -> None:
    lines: list[str] = []
    reporter = cli.SyncProgressReporter(emit=lines.append)

    reporter(_sync_progress(state="fatal"))

    assert json.loads(lines[0])["status"] == "fatal"


def test_sync_accepts_debug_log_level(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    async def fake_sync(
        _address: str,
        _adapter: str,
        _staging: object,
        *args: object,
        **kwargs: object,
    ) -> device_cli.collector.NoDataResult:
        progress = cast(device_cli.ProgressReporter, args[0])
        force_1m = cast(bool, kwargs["force_1m"])
        presence = kwargs["presence"]
        link_terminal_callback = kwargs["link_terminal_callback"]
        debug_logger = kwargs["debug_logger"]
        del _staging, force_1m, presence, link_terminal_callback, debug_logger
        progress(_sync_progress(state="away"))
        return device_cli.collector.NoDataResult(device_cli.RingInfo(10, 10, 100, 0, RECORD_SIZE))

    monkeypatch.setattr(device_cli, "sync", fake_sync)
    result = CliRunner().invoke(
        app,
        [
            "device",
            "sync",
            "--config",
            str(_layout(tmp_path)),
            "--confirm-sync",
            "--log-level",
            "DEBUG",
        ],
    )

    assert result.exit_code == 0
    assert any(json.loads(line)["status"] == "away" for line in result.output.splitlines())


def test_sync_force_1m_flag_is_forwarded(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    captured: list[bool] = []

    async def fake_sync(
        _address: str,
        _adapter: str,
        _staging: object,
        *_args: object,
        **kwargs: object,
    ) -> object:
        force_1m = cast(bool, kwargs["force_1m"])
        presence = kwargs["presence"]
        link_terminal_callback = kwargs["link_terminal_callback"]
        debug_logger = kwargs["debug_logger"]
        del _staging, link_terminal_callback, debug_logger
        assert isinstance(presence, device_cli.PresenceScheduler)
        captured.append(force_1m)
        return device_cli.collector.NoDataResult(device_cli.RingInfo(10, 10, 100, 0, RECORD_SIZE))

    monkeypatch.setattr(device_cli, "sync", fake_sync)
    result = CliRunner().invoke(
        app,
        [
            "device",
            "sync",
            "--config",
            str(_layout(tmp_path)),
            "--confirm-sync",
            "--force-1m",
        ],
    )

    assert result.exit_code == 0
    assert captured == [True]


def test_sync_constructs_production_presence_scheduler_once(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    sentinel = object()
    factory_calls: list[tuple[str, str]] = []
    captured: list[object] = []

    def make_presence(address: str, adapter: str, **kwargs: object) -> object:
        assert kwargs["config"] == load_operator_config(_layout(tmp_path)).config
        factory_calls.append((address, adapter))
        return sentinel

    async def fake_sync(
        _address: str,
        _adapter: str,
        _staging: object,
        *_args: object,
        **kwargs: object,
    ) -> object:
        force_1m = cast(bool, kwargs["force_1m"])
        presence = kwargs["presence"]
        link_terminal_callback = kwargs["link_terminal_callback"]
        debug_logger = kwargs["debug_logger"]
        del _staging, force_1m, link_terminal_callback, debug_logger
        captured.append(presence)
        return device_cli.collector.NoDataResult(device_cli.RingInfo(10, 10, 100, 0, RECORD_SIZE))

    monkeypatch.setattr(device_cli, "make_presence_scheduler", make_presence)
    monkeypatch.setattr(device_cli, "sync", fake_sync)
    result = CliRunner().invoke(
        app,
        [
            "device",
            "sync",
            "--config",
            str(_layout(tmp_path)),
            "--confirm-sync",
        ],
    )

    assert result.exit_code == 0
    assert factory_calls == [("AA:BB:CC:DD:EE:FF", "hci0")]
    assert captured == [sentinel]
