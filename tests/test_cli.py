from __future__ import annotations

import asyncio
import json
from collections.abc import Coroutine
from pathlib import Path
from shutil import rmtree
from typing import cast

import pytest
from typer.testing import CliRunner

from omi_collector import cli
from omi_collector.capture import cli as device_cli
from omi_collector.capture.adapters.debug_logging import close_debug_logging, configure_debug_logging
from omi_collector.capture.adapters.publication import SealResult
from omi_collector.capture.adapters.staging_contract import StagingError
from omi_collector.capture.adapters.staging_store import StagingStore
from omi_collector.capture.application.collector import CollectionResult
from omi_collector.capture.application.session_lifecycle import ActivityEvent
from omi_collector.capture.domain.ring_protocol import RECORD_SIZE, RingInfo
from omi_collector.cli import app
from omi_collector.config import DebugLogConfig
from omi_collector.spool_metrics import (
    FirmwareLifetimeMetrics,
    SpoolMetrics,
    SpoolMetricsError,
    SpoolWindowMetrics,
)

_CAPTURE_ROOTS: set[Path] = set()


def _capture_root(tmp_path: Path) -> Path:
    root = tmp_path.parent / f"{tmp_path.name}-captures"
    if tmp_path not in _CAPTURE_ROOTS:
        rmtree(root, ignore_errors=True)
        _CAPTURE_ROOTS.add(tmp_path)
    return root


def _layout(tmp_path: Path) -> Path:
    path = tmp_path / "layout.toml"
    path.write_text(
        """version = 1

[collector]
root = "collector"
attempts = "attempts"
quarantine = "quarantine"
lock = "collector.lock"
device_state = "device.json"
debug_log = "debug.jsonl"

[publication]
root = "pipeline"
raw = "raw"
""",
        encoding="utf-8",
    )
    return path


def test_version_flag_reports_package_version() -> None:
    result = CliRunner().invoke(app, ["--version"])

    assert result.exit_code == 0
    assert result.output.strip() == "0.1.0"


def test_health_command_reports_ok() -> None:
    result = CliRunner().invoke(app, ["health"])

    assert result.exit_code == 0
    assert result.output.strip() == "ok"


def test_sync_announces_loaded_layout_before_absent_pendant_wait(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    async def fake_sync(*args: object, **kwargs: object) -> object:
        del kwargs
        progress = cast(cli.SyncProgressReporter, args[4])
        progress(_sync_progress(state="away"))
        return device_cli.collector.NoDataResult(device_cli.RingInfo(10, 10, 100, 0, RECORD_SIZE))

    layout_path = _layout(tmp_path)
    monkeypatch.setattr(device_cli, "sync", fake_sync)
    result = CliRunner().invoke(
        app,
        [
            "device",
            "sync",
            "--address",
            "AA:BB",
            "--device-slug",
            "omi",
            "--layout",
            str(layout_path),
            "--confirm-sync",
            "--announce-readiness",
        ],
    )

    assert result.exit_code == 0, result.output
    readiness = cast(dict[str, object], json.loads(result.output.splitlines()[0]))
    assert readiness == {
        "layout": str(layout_path),
        "status": "deployment_ready",
    }


def test_sync_does_not_announce_readiness_for_bad_layout(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    bad_layout = tmp_path / "layout.toml"
    bad_layout.write_text("not = [valid", encoding="utf-8")

    async def unexpected_sync(*args: object, **kwargs: object) -> object:
        del args, kwargs
        raise AssertionError("sync must not start")

    monkeypatch.setattr(device_cli, "sync", unexpected_sync)
    result = CliRunner().invoke(
        app,
        [
            "device",
            "sync",
            "--address",
            "AA:BB",
            "--device-slug",
            "omi",
            "--layout",
            str(bad_layout),
            "--confirm-sync",
            "--announce-readiness",
        ],
    )

    assert result.exit_code == 2
    assert "deployment_ready" not in result.output


def test_sync_does_not_announce_readiness_when_storage_preflight_fails(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    def fail_preflight(_store: StagingStore) -> None:
        raise StagingError("simulated durable-write denial")

    async def unexpected_sync(*args: object, **kwargs: object) -> object:
        del args, kwargs
        raise AssertionError("sync must not start")

    monkeypatch.setattr(StagingStore, "preflight_storage", fail_preflight)
    monkeypatch.setattr(device_cli, "sync", unexpected_sync)
    result = CliRunner().invoke(
        app,
        [
            "device",
            "sync",
            "--address",
            "AA:BB",
            "--device-slug",
            "omi",
            "--layout",
            str(_layout(tmp_path)),
            "--confirm-sync",
            "--announce-readiness",
        ],
    )

    assert result.exit_code == 1
    assert "deployment_ready" not in result.output
    assert "simulated durable-write denial" in result.output


def test_device_metrics_reports_one_stable_json_object(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    expected = SpoolMetrics(
        SpoolWindowMetrics(1, 3, 1332, 4, 1776, 4 / 7),
        FirmwareLifetimeMetrics(3, 1, 6, 5, 0, 1),
    )
    captured: list[tuple[Path, str, Path]] = []

    def fake_metrics(capture_root: Path, device_slug: str, *, observation_root: Path) -> SpoolMetrics:
        captured.append((capture_root, device_slug, observation_root))
        return expected

    monkeypatch.setattr(cli, "collect_spool_metrics", fake_metrics)
    result = CliRunner().invoke(
        app,
        [
            "device",
            "metrics",
            "--layout",
            str(_layout(tmp_path)),
            "--device-slug",
            "omi",
        ],
    )

    assert result.exit_code == 0
    assert result.output == json.dumps(expected.as_dict(), sort_keys=True, separators=(",", ":")) + "\n"
    assert captured == [(tmp_path / "pipeline" / "raw", "omi", tmp_path / "collector" / "device.json")]


def test_device_metrics_reports_malformed_authority_as_nonzero(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    def fail_metrics(*_args: object, **_kwargs: object) -> SpoolMetrics:
        raise SpoolMetricsError("bad artifact")

    monkeypatch.setattr(cli, "collect_spool_metrics", fail_metrics)

    result = CliRunner().invoke(
        app,
        [
            "device",
            "metrics",
            "--layout",
            str(_layout(tmp_path)),
            "--device-slug",
            "omi",
        ],
    )

    assert result.exit_code == 1
    assert result.output.strip() == "bad artifact"


def test_serve_passes_interval_to_sleep_until_stopped(monkeypatch: pytest.MonkeyPatch) -> None:
    class StopServingError(RuntimeError):
        pass

    intervals: list[int] = []

    def stop_after_sleep(interval: int) -> None:
        intervals.append(interval)
        raise StopServingError

    monkeypatch.setattr(cli.time, "sleep", stop_after_sleep)

    result = CliRunner().invoke(app, ["serve", "--interval-seconds", "7"])

    assert isinstance(result.exception, StopServingError)
    assert intervals == [7]


def test_phy_check_and_recover_commands_report_success(monkeypatch: pytest.MonkeyPatch) -> None:
    events: list[tuple[str, str]] = []

    async def fake_phy_check(adapter: str) -> None:
        events.append(("check", adapter))

    async def fake_recover_phy(adapter: str) -> None:
        events.append(("recover", adapter))

    monkeypatch.setattr(device_cli, "phy_check", fake_phy_check)
    monkeypatch.setattr(device_cli, "recover_phy", fake_recover_phy)
    runner = CliRunner()

    phy_result = runner.invoke(app, ["device", "phy-check", "--confirm-host-change"])
    recover_result = runner.invoke(app, ["device", "recover-phy", "--adapter", "hci1"])

    assert phy_result.exit_code == 0
    assert json.loads(phy_result.output) == {"status": "phy_restored"}
    assert recover_result.exit_code == 0
    assert json.loads(recover_result.output) == {"status": "phy_recovered"}
    assert events == [("check", "hci0"), ("recover", "hci1")]


def test_device_operation_interruption_is_reported(monkeypatch: pytest.MonkeyPatch) -> None:
    async def fake_phy_check(_adapter: str) -> None:
        return

    def interrupted(operation: Coroutine[object, object, object]) -> object:
        operation.close()
        raise device_cli.OperationInterruptedError("interrupted safely")

    monkeypatch.setattr(device_cli, "phy_check", fake_phy_check)
    monkeypatch.setattr(device_cli, "run", interrupted)

    result = CliRunner().invoke(app, ["device", "phy-check", "--confirm-host-change"])

    assert result.exit_code == 0
    assert result.output.strip() == "interrupted safely"


def test_sync_requires_explicit_read_advance_confirmation(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    async def fail_sync(*_args: object, **_kwargs: object) -> object:
        raise AssertionError("sync must not start without confirmation")

    monkeypatch.setattr(device_cli, "sync", fail_sync)
    result = CliRunner().invoke(
        app,
        ["device", "sync", "--address", "AA:BB", "--device-slug", "omi", "--layout", str(_layout(tmp_path))],
    )

    assert result.exit_code == 2
    assert "--confirm-sync" in result.output


def test_sync_reports_progress_on_stderr_and_metrics_on_stdout(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
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
        _slug: str,
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
        return expected

    monkeypatch.setattr(device_cli, "sync", fake_sync)
    result = CliRunner().invoke(
        app,
        [
            "device",
            "sync",
            "--address",
            "AA:BB",
            "--device-slug",
            "omi",
            "--layout",
            str(_layout(tmp_path)),
            "--confirm-sync",
        ],
    )

    assert result.exit_code == 0
    lines = result.output.splitlines()
    assert any(json.loads(line)["status"] == "progress" for line in lines)
    final = cast(dict[str, object], json.loads(lines[-1]))
    assert final["status"] == "synced"
    assert final["advance_confirmed"] is True
    assert final["payload_bytes"] == RECORD_SIZE
    assert final["remaining_packets"] == 0
    assert cast(float, final["bytes_per_second"]) >= 0


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


def test_sync_reporter_persists_filtered_debug_callback(tmp_path: Path) -> None:
    lines: list[str] = []
    logger = configure_debug_logging(tmp_path, DebugLogConfig(logger_name="tests.debug.progress"))
    try:
        reporter = cli.SyncProgressReporter(emit=lines.append, debug_logger=logger)
        reporter(_sync_progress(state="away"))
    finally:
        close_debug_logging(logger)

    entry = cast(dict[str, object], json.loads((tmp_path / "debug.jsonl").read_text()))
    fields = cast(dict[str, object], entry["fields"])
    progress = cast(dict[str, object], fields["progress"])

    assert lines == []
    assert entry["event"] == "sync_progress"
    assert progress["status"] == "away"


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
        "remaining_packets": 0,
        "status": "cooldown_started",
    }


def test_sync_reporter_debug_exposes_routine_polling() -> None:
    lines: list[str] = []
    reporter = cli.SyncProgressReporter(cli.SyncLogLevel.DEBUG, emit=lines.append)

    reporter(_sync_progress(state="away"))
    reporter(
        _sync_progress(
            state="operational",
            operational_event={"event": "pendant_observation", "unread_packets": 0},
        )
    )

    assert [cast(dict[str, object], json.loads(line))["status"] for line in lines] == ["away", "operational"]


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
        _slug: str,
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
            "--address",
            "AA:BB",
            "--device-slug",
            "omi",
            "--layout",
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
        _slug: str,
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
            "--address",
            "AA:BB",
            "--device-slug",
            "omi",
            "--layout",
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

    def make_presence(address: str, adapter: str) -> object:
        factory_calls.append((address, adapter))
        return sentinel

    async def fake_sync(
        _address: str,
        _adapter: str,
        _slug: str,
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
            "--address",
            "AA:BB",
            "--device-slug",
            "omi",
            "--layout",
            str(_layout(tmp_path)),
            "--confirm-sync",
        ],
    )

    assert result.exit_code == 0
    assert factory_calls == [("AA:BB", "hci0")]
    assert captured == [sentinel]
