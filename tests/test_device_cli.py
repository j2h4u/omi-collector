from __future__ import annotations

import asyncio
import json
import os
import signal
import subprocess
import sys
from collections.abc import AsyncIterator, Callable, Coroutine
from contextlib import AbstractAsyncContextManager
from pathlib import Path
from shutil import rmtree
from struct import pack
from typing import Protocol, cast

import pytest
from bleak.backends.device import BLEDevice
from typer.testing import CliRunner

from fakes import ScriptedRingSession, WriteStep
from omi_collector.capture import cli as device_cli
from omi_collector.capture.adapters.publication import SealResult
from omi_collector.capture.adapters.staging_contract import DeviceAlreadyRunningError
from omi_collector.capture.adapters.staging_store import StagingStore
from omi_collector.capture.application.collector import CollectionResult, ProgressEvent
from omi_collector.capture.application.presence import PresenceScheduler
from omi_collector.capture.application.ring_transport import (
    RingSession,
    RingTransportDisconnectedError,
    RingTransportUnavailableError,
)
from omi_collector.capture.application.session_lifecycle import (
    ActivityEvent,
    OpportunisticOptions,
)
from omi_collector.capture.application.session_lifecycle import RetryPolicy as DefaultRetryPolicy
from omi_collector.capture.application.session_lifecycle import (
    sanitize_error_message as _sanitize_error_message,
)
from omi_collector.capture.domain.ring_protocol import (
    RECORD_SIZE,
    RingInfo,
    RingStatus,
    encode_advance_command,
    encode_read_command,
    encode_stop_command,
)
from omi_collector.cli import app
from omi_collector.config import BleConfig, CollectorConfig, PresenceConfig, RetryConfig, TransferConfig


class FakeGuard:
    def __init__(self, events: list[str]) -> None:
        self.events = events

    async def recover(self) -> None:
        self.events.append("recover")

    async def __aenter__(self) -> object:
        self.events.append("guard-enter")
        return self

    async def __aexit__(self, exc_type: object, exc: object, traceback: object) -> bool:
        del exc_type, exc, traceback
        self.events.append("guard-exit")
        return False


_CAPTURE_ROOTS: set[Path] = set()


def _capture_root(tmp_path: Path) -> Path:
    root = tmp_path.parent / f"{tmp_path.name}-captures"
    if tmp_path not in _CAPTURE_ROOTS:
        rmtree(root, ignore_errors=True)
        _CAPTURE_ROOTS.add(tmp_path)
    return root


def _store(tmp_path: Path) -> StagingStore:
    return StagingStore(tmp_path, _capture_root(tmp_path))


def _layout(tmp_path: Path) -> Path:
    path = tmp_path / "layout.toml"
    path.write_text(
        """version = 2

[collector]
root = "collector"
attempts = "attempts"
quarantine = "quarantine"
lock = "collector.lock"
device_state = "device.json"
debug_log = "debug.jsonl"

[publication]
root = "source"
""",
        encoding="utf-8",
    )
    return path


class FakeSession:
    def __init__(self, status: RingStatus, notifications: tuple[bytes, ...] = ()) -> None:
        self._status = status
        self._notifications = notifications
        self.writes: list[bytes] = []

    async def read_status(self) -> RingStatus:
        return self._status

    def notifications(self) -> AsyncIterator[bytes]:
        return self._notification_stream()

    async def write_control(self, payload: bytes) -> None:
        self.writes.append(payload)

    async def close(self) -> None:
        return

    async def _notification_stream(self) -> AsyncIterator[bytes]:
        for payload in self._notifications:
            yield payload


class TransportContext(Protocol):
    async def __aenter__(self) -> RingSession: ...

    async def __aexit__(self, exc_type: object, exc: object, traceback: object) -> None: ...


class FakeTransport:
    def __init__(self, address: str, session: RingSession, events: list[str]) -> None:
        self.address = address
        self._session = session
        self._events = events

    async def __aenter__(self) -> RingSession:
        self._events.append(f"connect:{self.address}")
        return self._session

    async def __aexit__(self, exc_type: object, exc: object, traceback: object) -> None:
        del exc_type, exc, traceback
        self._events.append("disconnect")


def _install_fakes(monkeypatch: pytest.MonkeyPatch, session: RingSession, events: list[str]) -> None:
    monkeypatch.setattr(device_cli, "make_guard", lambda _adapter: FakeGuard(events))
    monkeypatch.setattr(
        device_cli, "make_transport", lambda address, **_kwargs: FakeTransport(address, session, events)
    )


def _status() -> RingStatus:
    return RingStatus(100, 2, 900, 1)


def _info(write_sequence: int = 11) -> bytes:
    return pack(">BQQIQH", 0x02, 10, write_sequence, 100, 0, RECORD_SIZE)


def test_production_factories_construct_monkeypatched_dependencies(monkeypatch: pytest.MonkeyPatch) -> None:
    class Guard:
        def __init__(self, adapter: str) -> None:
            self.adapter = adapter

    class Transport:
        def __init__(self, address: str, **kwargs: object) -> None:
            self.address = address
            self.kwargs = kwargs

    monkeypatch.setattr(device_cli, "ScopedPhyGuard", Guard)
    monkeypatch.setattr(device_cli, "BleakRingTransport", Transport)

    guard = device_cli.make_guard("hci0")
    transport = device_cli.make_transport("AA:BB")

    assert isinstance(guard, Guard)
    assert guard.adapter == "hci0"
    assert isinstance(transport, Transport)
    assert transport.address == "AA:BB"
    assert (
        transport.kwargs["att_mtu_query_timeout_seconds"] == device_cli.DEFAULT_CONFIG.ble.att_mtu_query_timeout_seconds
    )


def test_phy_check_and_recover_use_fake_guard_only(monkeypatch: pytest.MonkeyPatch) -> None:
    events: list[str] = []
    _install_fakes(monkeypatch, FakeSession(_status()), events)

    asyncio.run(device_cli.phy_check("hci0"))
    asyncio.run(device_cli.recover_phy("hci0"))

    assert events == ["guard-enter", "guard-exit", "recover"]


def test_probe_and_info_errors_propagate_through_fake_session(monkeypatch: pytest.MonkeyPatch) -> None:
    events: list[str] = []
    _install_fakes(monkeypatch, FakeSession(_status()), events)

    async def fail_probe(_session: FakeSession) -> RingStatus:
        raise OSError("probe transport failed")

    async def fail_info(_session: FakeSession, *, timeout: float) -> RingInfo:
        del timeout
        raise OSError("info transport failed")

    monkeypatch.setattr(device_cli.collector, "probe", fail_probe)
    with pytest.raises(OSError, match="probe transport failed"):
        asyncio.run(device_cli.probe("AA:BB", "hci0"))

    monkeypatch.setattr(device_cli.collector, "ring_info", fail_info)
    with pytest.raises(OSError, match="info transport failed"):
        asyncio.run(device_cli.info("AA:BB", "hci0"))

    assert events == [
        "guard-enter",
        "connect:AA:BB",
        "disconnect",
        "guard-exit",
        "guard-enter",
        "connect:AA:BB",
        "disconnect",
        "guard-exit",
    ]


def test_confirmation_gates_do_not_construct_host_operations(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    def no_host_operation(*_args: object, **_kwargs: object) -> object:
        raise AssertionError("host operation must not be constructed")

    monkeypatch.setattr(device_cli, "phy_check", no_host_operation)
    monkeypatch.setattr(device_cli, "probe", no_host_operation)
    monkeypatch.setattr(device_cli, "info", no_host_operation)
    monkeypatch.setattr(device_cli, "collect", no_host_operation)
    runner = CliRunner()

    phy_result = runner.invoke(app, ["device", "phy-check"])
    probe_result = runner.invoke(app, ["device", "probe", "--address", "AA:BB"])
    info_result = runner.invoke(app, ["device", "info", "--address", "AA:BB"])
    collect_result = runner.invoke(
        app,
        ["device", "collect", "--address", "AA:BB", "--device-slug", "omi", "--layout", str(_layout(tmp_path))],
    )

    assert phy_result.exit_code == 2
    assert "--confirm-host-change" in phy_result.output
    assert probe_result.exit_code == 2
    assert "--confirm-host-change" in probe_result.output
    assert info_result.exit_code == 2
    assert "--confirm-host-change" in info_result.output
    assert collect_result.exit_code == 2
    assert "--confirm-read" in collect_result.output


def test_non_hci0_adapter_fails_before_host_operation(monkeypatch: pytest.MonkeyPatch) -> None:
    def no_host_operation(*_args: object, **_kwargs: object) -> object:
        raise AssertionError("host operation must not be constructed")

    monkeypatch.setattr(device_cli, "make_guard", no_host_operation)

    result = CliRunner().invoke(
        app,
        ["device", "probe", "--address", "AA:BB", "--adapter", "hci7", "--confirm-host-change"],
    )

    assert result.exit_code == 1
    assert "ValueError" in result.output


def test_unexpected_transport_error_is_redacted(monkeypatch: pytest.MonkeyPatch) -> None:
    async def failing_probe(_address: str, _adapter: str) -> RingStatus:
        raise Exception("raw transport detail must not reach the terminal")

    monkeypatch.setattr(device_cli, "probe", failing_probe)

    result = CliRunner().invoke(app, ["device", "probe", "--address", "AA:BB", "--confirm-host-change"])

    assert result.exit_code == 1
    assert result.output.strip() == "device operation failed: Exception"


def test_probe_uses_injected_fakes_and_renders_safe_status(monkeypatch: pytest.MonkeyPatch) -> None:
    events: list[str] = []
    _install_fakes(monkeypatch, FakeSession(_status()), events)

    result = CliRunner().invoke(
        app,
        ["device", "probe", "--address", "AA:BB", "--adapter", "hci0", "--confirm-host-change"],
    )

    assert result.exit_code == 0
    assert json.loads(result.output) == {
        "free_bytes": 900,
        "rtc_valid": True,
        "status": "ok",
        "unread_packets": 2,
        "used_bytes": 100,
    }
    assert events == ["guard-enter", "connect:AA:BB", "disconnect", "guard-exit"]


def test_info_renders_sequences_without_audio(monkeypatch: pytest.MonkeyPatch) -> None:
    events: list[str] = []
    session = FakeSession(_status(), (b"\x01\x00", _info()))
    _install_fakes(monkeypatch, session, events)

    result = CliRunner().invoke(app, ["device", "info", "--address", "AA:BB", "--confirm-host-change"])

    assert result.exit_code == 0
    assert json.loads(result.output) == {
        "capacity_packets": 100,
        "dropped_packets": 0,
        "packet_size": 444,
        "read_sequence": 10,
        "status": "ok",
        "unread_packets": 1,
        "write_sequence": 11,
    }
    assert session.writes == [b"\x10"]


def test_collect_rejects_values_above_conservative_bound(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    _install_fakes(monkeypatch, FakeSession(_status()), [])

    result = CliRunner().invoke(
        app,
        [
            "device",
            "collect",
            "--address",
            "AA:BB",
            "--device-slug",
            "omi",
            "--layout",
            str(_layout(tmp_path)),
            "--max-records",
            "257",
            "--confirm-read",
        ],
    )

    assert result.exit_code == 2
    assert "256" in result.output


def test_sync_uses_one_guard_and_transport_and_forwards_end_to_end_progress(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    events: list[str] = []
    session = FakeSession(_status())
    _install_fakes(monkeypatch, session, events)

    def forbidden_guard(_adapter: str) -> object:
        raise AssertionError("normal sync must preserve the selected adapter PHY")

    monkeypatch.setattr(device_cli, "make_guard", forbidden_guard)
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
    updates: list[device_cli.DownloadProgress] = []

    async def fake_run_opportunistic_collector(
        provider: Callable[[object | None], AbstractAsyncContextManager[RingSession]],
        _staging: object,
        _slug: str,
        options: OpportunisticOptions,
        *,
        runtime: object,
    ) -> CollectionResult:
        del runtime
        assert options.timeouts == device_cli.collector.TransferTimeouts(
            info=device_cli.DEFAULT_CONFIG.transfer.info_timeout_seconds,
            transfer=device_cli.DEFAULT_CONFIG.transfer.sync_timeout_seconds,
        )
        async with provider(None) as connected:
            assert connected is session
        progress = options.progress
        await cast(Callable[[ProgressEvent], Coroutine[object, object, object]], progress)(
            ProgressEvent(1, 1, RECORD_SIZE, RECORD_SIZE, 0.0, 0.0, 0.0, None)
        )
        return expected

    monkeypatch.setattr(device_cli, "run_opportunistic_collector", fake_run_opportunistic_collector)
    result = asyncio.run(device_cli.sync("AA:BB", "hci0", "omi", _store(tmp_path), updates.append))

    assert result == expected
    assert events == ["connect:AA:BB", "disconnect"]
    assert updates[0].payload_bytes == RECORD_SIZE
    assert updates[0].remaining_packets == 0
    assert updates[0].eta_seconds is None


def test_sync_forwards_configured_att_mtu_timeout_to_all_transport_paths(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    events: list[str] = []
    session = FakeSession(_status())
    timeout_seconds = 0.123
    config = CollectorConfig(ble=BleConfig(att_mtu_query_timeout_seconds=timeout_seconds))
    captured: list[float] = []

    monkeypatch.setattr(device_cli, "make_guard", lambda _adapter: FakeGuard(events))

    def capture_transport(address: str, **kwargs: object) -> FakeTransport:
        captured.append(cast(float, kwargs["att_mtu_query_timeout_seconds"]))
        return FakeTransport(address, session, events)

    monkeypatch.setattr(device_cli, "make_transport", capture_transport)

    async def fake_run(
        provider: Callable[[object | None], AbstractAsyncContextManager[RingSession]],
        _staging: object,
        _slug: str,
        _options: OpportunisticOptions,
        *,
        runtime: object,
    ) -> object:
        del runtime
        async with provider(None) as connected:
            assert connected is session
        async with provider(BLEDevice("AA:BB", "omi", object())) as connected:
            assert connected is session
        return device_cli.collector.NoDataResult(RingInfo(10, 10, 100, 0, RECORD_SIZE))

    monkeypatch.setattr(device_cli, "run_opportunistic_collector", fake_run)
    result = asyncio.run(device_cli.sync("AA:BB", "hci0", "omi", _store(tmp_path), config=config))

    assert isinstance(result, device_cli.collector.NoDataResult)
    assert captured == [timeout_seconds, timeout_seconds]

    captured.clear()

    async def fake_run_force(
        provider: Callable[[object | None], AbstractAsyncContextManager[RingSession]],
        _staging: object,
        _slug: str,
        _options: OpportunisticOptions,
        *,
        runtime: object,
    ) -> object:
        del runtime
        async with provider(None) as connected:
            assert connected is session
        return device_cli.collector.NoDataResult(RingInfo(10, 10, 100, 0, RECORD_SIZE))

    monkeypatch.setattr(device_cli, "run_opportunistic_collector", fake_run_force)
    result = asyncio.run(device_cli.sync("AA:BB", "hci0", "omi", _store(tmp_path), force_1m=True, config=config))

    assert isinstance(result, device_cli.collector.NoDataResult)
    assert captured == [timeout_seconds]


def test_sync_force_1m_enters_and_restores_phy_guard(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    events: list[str] = []
    session = FakeSession(_status())
    _install_fakes(monkeypatch, session, events)

    async def fake_run_opportunistic_collector(
        provider: Callable[[object | None], AbstractAsyncContextManager[RingSession]],
        _staging: object,
        _slug: str,
        options: OpportunisticOptions,
        *,
        runtime: object,
    ) -> object:
        del runtime
        assert options.timeouts == device_cli.collector.TransferTimeouts(
            info=device_cli.DEFAULT_CONFIG.transfer.info_timeout_seconds,
            transfer=device_cli.DEFAULT_CONFIG.transfer.sync_timeout_seconds,
        )
        async with provider(None) as connected:
            assert connected is session
        return device_cli.collector.NoDataResult(RingInfo(10, 10, 100, 0, RECORD_SIZE))

    monkeypatch.setattr(device_cli, "run_opportunistic_collector", fake_run_opportunistic_collector)
    result = asyncio.run(device_cli.sync("AA:BB", "hci0", "omi", _store(tmp_path), force_1m=True))

    assert isinstance(result, device_cli.collector.NoDataResult)
    assert events == ["guard-enter", "connect:AA:BB", "disconnect", "guard-exit"]


def _record(value: int) -> bytes:
    return value.to_bytes(4, "big") + bytes((value,)) * (RECORD_SIZE - 4)


def _info_notification(read_sequence: int, write_sequence: int) -> bytes:
    return pack(">BQQIQH", 0x02, read_sequence, write_sequence, 100, 0, RECORD_SIZE)


def _read_begin(start: int, count: int) -> bytes:
    return b"\x05" + start.to_bytes(8, "big") + count.to_bytes(4, "big")


def _data(record: bytes) -> bytes:
    return b"\x03" + record


def _done(next_sequence: int) -> bytes:
    return b"\x04\x00" + next_sequence.to_bytes(8, "big")


def test_sync_reconnects_with_overlap_and_enters_one_outer_guard(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    first = ScriptedRingSession(
        RingStatus(0, 0, 0, 1),
        (
            WriteStep(b"\x10", (_info_notification(10, 12),)),
            WriteStep(
                encode_read_command(10, 2),
                (_read_begin(10, 2), _data(_record(10)), RingTransportDisconnectedError("gone")),
            ),
            WriteStep(encode_stop_command()),
        ),
    )
    second = ScriptedRingSession(
        RingStatus(0, 0, 0, 1),
        (
            WriteStep(b"\x10", (_info_notification(11, 12),)),
            WriteStep(encode_read_command(11, 1), (_read_begin(11, 1), _data(_record(11)), _done(12))),
            WriteStep(b"\x10", (_info_notification(11, 12),)),
            WriteStep(encode_advance_command(12), (b"\x01\x00",)),
            WriteStep(b"\x10", (_info_notification(12, 12),)),
        ),
    )
    sessions = [first, second]
    events: list[str] = []
    monkeypatch.setattr(device_cli, "make_guard", lambda _adapter: FakeGuard(events))
    monkeypatch.setattr(
        device_cli,
        "make_transport",
        lambda address, **_kwargs: FakeTransport(address, sessions.pop(0), events),
    )
    monkeypatch.setattr(
        device_cli, "RetryPolicy", lambda **_kwargs: DefaultRetryPolicy(backoff=(0.001,), stop_after_drained=True)
    )

    updates: list[device_cli.DownloadProgress] = []
    result = asyncio.run(device_cli.sync("AA:BB", "hci0", "omi", _store(tmp_path), updates.append, force_1m=True))

    assert isinstance(result, CollectionResult)
    assert result.next_sequence == 12
    assert result.advance_confirmed
    assert len(updates) >= 1
    session_errors = [update for update in updates if update.state == "session_error"]
    assert len(session_errors) == 1
    assert session_errors[0].as_dict()["phase"] == "read/reconcile"
    assert session_errors[0].as_dict()["error_type"] == "RingTransportDisconnectedError"
    assert session_errors[0].as_dict()["error_message"] == "RingTransportDisconnectedError: gone"
    assert events.count("connect:AA:BB") == 2
    assert events.count("disconnect") == 2
    assert events.count("guard-enter") == 2
    assert events.count("guard-exit") == 2


def test_sync_uses_injected_presence_scheduler_without_reconstruction(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    class Observer:
        async def start(self, callback: Callable[[object], object]) -> None:
            del callback

        async def stop(self) -> None:
            return

    injected = PresenceScheduler(Observer())
    captured: list[OpportunisticOptions] = []

    async def fake_run(
        _provider: Callable[[object | None], AbstractAsyncContextManager[RingSession]],
        _staging: object,
        _slug: str,
        options: OpportunisticOptions,
        *,
        runtime: object,
    ) -> object:
        del runtime
        captured.append(options)
        return device_cli.collector.NoDataResult(RingInfo(10, 10, 100, 0, RECORD_SIZE))

    monkeypatch.setattr(device_cli, "run_opportunistic_collector", fake_run)
    result = asyncio.run(device_cli.sync("AA:BB", "hci0", "omi", _store(tmp_path), presence=injected))

    assert isinstance(result, device_cli.collector.NoDataResult)
    assert len(captured) == 1
    assert captured[0].presence is injected
    assert captured[0].policy.backoff == injected.policy.rapid_backoff
    assert captured[0].policy.drain_cooldown_seconds == injected.policy.drained_fallback_seconds


def test_presence_and_retry_policies_share_one_config_instance(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    config = CollectorConfig(
        presence=PresenceConfig(
            fallback_seconds=301.0,
            max_fallback_seconds=301.0,
            drained_fallback_seconds=901.0,
            max_drained_fallback_seconds=901.0,
            scan_cancel_grace_min_seconds=0.02,
            scan_cancel_grace_max_seconds=0.03,
            scan_cancel_grace_fraction=0.5,
        ),
        retry=RetryConfig(rapid_backoff=(0.25, 0.5)),
        transfer=TransferConfig(info_timeout_seconds=3.0, sync_timeout_seconds=4.0),
    )

    presence = device_cli.make_presence_scheduler("AA:BB", "hci0", config=config)
    captured: list[OpportunisticOptions] = []

    async def fake_run(
        _provider: Callable[[object | None], AbstractAsyncContextManager[RingSession]],
        _staging: object,
        _slug: str,
        options: OpportunisticOptions,
        *,
        runtime: object,
    ) -> object:
        del runtime
        captured.append(options)
        return device_cli.collector.NoDataResult(RingInfo(10, 10, 100, 0, RECORD_SIZE))

    monkeypatch.setattr(device_cli, "run_opportunistic_collector", fake_run)
    result = asyncio.run(device_cli.sync("AA:BB", "hci0", "omi", _store(tmp_path), presence=presence, config=config))

    assert isinstance(result, device_cli.collector.NoDataResult)
    assert presence.policy.rapid_backoff == config.retry.rapid_backoff
    assert presence.policy.fallback_seconds == config.presence.fallback_seconds
    assert presence.policy.scan_cancel_grace_min_seconds == config.presence.scan_cancel_grace_min_seconds
    assert presence.policy.scan_cancel_grace_max_seconds == config.presence.scan_cancel_grace_max_seconds
    assert presence.policy.scan_cancel_grace_fraction == config.presence.scan_cancel_grace_fraction
    assert captured[0].policy.backoff == config.retry.rapid_backoff
    assert captured[0].policy.drain_cooldown_seconds == config.presence.drained_fallback_seconds
    assert captured[0].config is config
    assert captured[0].timeouts == device_cli.collector.TransferTimeouts(3.0, 4.0)


def test_sync_cancellation_exits_transport_and_restores_one_outer_guard(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    hanging = ScriptedRingSession(RingStatus(0, 0, 0, 1), (WriteStep(b"\x10"),))
    events: list[str] = []
    monkeypatch.setattr(device_cli, "make_guard", lambda _adapter: FakeGuard(events))
    monkeypatch.setattr(
        device_cli, "make_transport", lambda _address, **_kwargs: FakeTransport("AA:BB", hanging, events)
    )

    async def cancel() -> None:
        task = asyncio.create_task(device_cli.sync("AA:BB", "hci0", "omi", _store(tmp_path), force_1m=True))
        while len(events) < 2:
            await asyncio.sleep(0)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    asyncio.run(cancel())
    assert events == ["guard-enter", "connect:AA:BB", "disconnect", "guard-exit"]


def test_collect_does_not_read_while_sync_writer_holds_the_device_lock(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    entered = asyncio.Event()
    calls = 0
    guards: list[str] = []

    class BlockingReadSession(ScriptedRingSession):
        async def write_control(self, payload: bytes) -> None:
            await super().write_control(payload)
            if payload == encode_read_command(10, 1):
                entered.set()

    session = BlockingReadSession(
        RingStatus(0, 0, 0, 1),
        (
            WriteStep(b"\x10", (_info_notification(10, 11),)),
            WriteStep(encode_read_command(10, 1), (_read_begin(10, 1),)),
        ),
    )
    contender = ScriptedRingSession(RingStatus(0, 0, 0, 1), (WriteStep(b"\x10", (_info_notification(10, 11),)),))
    sessions = [session, contender]

    class BlockingTransport:
        async def __aenter__(self) -> RingSession:
            return sessions.pop(0)

        async def __aexit__(self, _type: object, _value: object, _traceback: object) -> None:
            return None

    def make_blocking_transport(_address: str, **_kwargs: object) -> BlockingTransport:
        nonlocal calls
        calls += 1
        return BlockingTransport()

    def tracked_guard(_adapter: str) -> FakeGuard:
        return FakeGuard(guards)

    monkeypatch.setattr(device_cli, "make_transport", make_blocking_transport)
    monkeypatch.setattr(device_cli, "make_guard", tracked_guard)

    async def contend() -> None:
        syncing = asyncio.create_task(device_cli.sync("AA:BB", "hci0", "omi", _store(tmp_path)))
        await entered.wait()
        with pytest.raises(DeviceAlreadyRunningError):
            await device_cli.collect("AA:BB", "hci0", "omi", _store(tmp_path), 1)
        syncing.cancel()
        with pytest.raises(asyncio.CancelledError):
            await syncing

    asyncio.run(contend())
    assert calls == 2
    assert guards == ["guard-enter", "guard-exit"]
    assert contender.writes == [b"\x10"]
    assert session.writes == [b"\x10", encode_read_command(10, 1), encode_stop_command()]


def test_sync_retries_pendant_absence_with_a_fresh_transport(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    class UnavailableTransport:
        async def __aenter__(self) -> RingSession:
            raise RingTransportUnavailableError("not in range")

        async def __aexit__(self, exc_type: object, exc: object, traceback: object) -> None:
            del exc_type, exc, traceback

    recovered = ScriptedRingSession(
        RingStatus(0, 0, 0, 1),
        (WriteStep(b"\x10", (_info_notification(10, 10),)),),
    )
    transports: list[TransportContext] = [UnavailableTransport(), FakeTransport("AA:BB", recovered, [])]
    calls: list[str] = []
    monkeypatch.setattr(
        device_cli,
        "make_transport",
        lambda address, **_kwargs: (calls.append(address), transports.pop(0))[1],
    )
    monkeypatch.setattr(
        device_cli, "RetryPolicy", lambda **_kwargs: DefaultRetryPolicy(backoff=(0.001,), stop_after_drained=True)
    )

    result = asyncio.run(device_cli.sync("AA:BB", "hci0", "omi", _store(tmp_path)))

    assert isinstance(result, device_cli.collector.NoDataResult)
    assert calls == ["AA:BB", "AA:BB"]


def test_download_metrics_are_finite_for_no_data_and_zero_duration() -> None:
    info = RingInfo(10, 10, 100, 0, RECORD_SIZE)
    metrics = device_cli.download_metrics(device_cli.collector.NoDataResult(info), 0.0)

    assert metrics.as_dict() == {
        "bytes_per_second": 0.0,
        "elapsed_seconds": 0.0,
        "eta_seconds": 0.0,
        "payload_bytes": 0,
        "records_per_second": 0.0,
        "remaining_packets": 0,
    }


def test_download_metric_serialization_rounds_float_fields_only() -> None:
    metrics = device_cli.DownloadMetrics(1.234, 444, 123.456, 7.891, 2, 0.126)
    progress = device_cli.DownloadProgress(2.345, 222, 98.765, 4.321, 3, 1.239, 1, 4)

    assert metrics.as_dict() == {
        "bytes_per_second": 123.46,
        "elapsed_seconds": 1.23,
        "eta_seconds": 0.13,
        "payload_bytes": 444,
        "records_per_second": 7.89,
        "remaining_packets": 2,
    }
    assert progress.as_dict() == {
        "bytes_per_second": 98.77,
        "elapsed_seconds": 2.35,
        "eta_seconds": 1.24,
        "payload_bytes": 222,
        "records_completed": 1,
        "records_per_second": 4.32,
        "records_total": 4,
        "remaining_packets": 3,
        "status": "progress",
    }


def test_activity_error_serialization_exposes_phase_and_sanitizes_payload() -> None:
    updates: list[device_cli.DownloadProgress] = []
    callback = device_cli._activity_callback(updates.append)
    assert callback is not None

    asyncio.run(
        cast(
            Coroutine[object, object, object],
            callback(
                ActivityEvent(
                    "session_error",
                    phase="read/reconcile",
                    error_type="ValueError",
                    error_message=_sanitize_error_message(ValueError("bad notification b'\\x03secret-audio'")),
                )
            ),
        )
    )

    assert updates[0].as_dict()["phase"] == "read/reconcile"
    assert updates[0].as_dict()["error_type"] == "ValueError"
    assert updates[0].as_dict()["error_message"] == "session operation failed"


def test_collect_no_data_result_is_safe_json(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    events: list[str] = []
    session = FakeSession(_status(), (_info(10),))
    _install_fakes(monkeypatch, session, events)

    result = CliRunner().invoke(
        app,
        [
            "device",
            "collect",
            "--address",
            "AA:BB",
            "--device-slug",
            "omi",
            "--layout",
            str(_layout(tmp_path)),
            "--max-records",
            "1",
            "--confirm-read",
        ],
    )

    assert result.exit_code == 0
    output = cast(dict[str, object], json.loads(result.output))
    assert output == {
        "bytes_per_second": 0.0,
        "elapsed_seconds": output["elapsed_seconds"],
        "eta_seconds": 0.0,
        "payload_bytes": 0,
        "read_sequence": 10,
        "records_per_second": 0.0,
        "remaining_packets": 0,
        "status": "no_data",
        "unread_packets": 0,
        "write_sequence": 10,
    }
    assert session.writes == [b"\x10"]


def test_collect_seals_one_bounded_read_without_advance(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    events: list[str] = []
    session = ScriptedRingSession(
        RingStatus(0, 0, 0, 1),
        (
            WriteStep(b"\x10", (_info_notification(10, 11),)),
            WriteStep(encode_read_command(10, 1), (_read_begin(10, 1), _data(_record(10)), _done(11))),
        ),
    )
    _install_fakes(monkeypatch, session, events)

    result = asyncio.run(device_cli.collect("AA:BB", "hci0", "omi", _store(tmp_path), 1))

    assert isinstance(result, CollectionResult)
    assert not result.advance_confirmed
    assert session.writes == [b"\x10", encode_read_command(10, 1)]
    assert all(command[:1] != b"\x12" for command in session.writes)
    assert events == ["guard-enter", "connect:AA:BB", "disconnect", "guard-exit"]


def test_collect_sealed_command_reports_bundle_metadata_without_audio(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    bundle = tmp_path / "omi" / "10-11-deadbeef"
    bundle.mkdir(parents=True)
    (bundle / "manifest.json").write_text('{"raw_sha256":"abc123"}', encoding="utf-8")
    collection = CollectionResult(
        RingInfo(10, 11, 100, 0, RECORD_SIZE),
        1,
        SealResult(bundle, True),
    )

    async def fake_collect(
        _address: str,
        _adapter: str,
        _device_slug: str,
        _staging: object,
        _max_records: int,
    ) -> CollectionResult:
        del _staging
        return collection

    monkeypatch.setattr(device_cli, "collect", fake_collect)
    result = CliRunner().invoke(
        app,
        [
            "device",
            "collect",
            "--address",
            "AA:BB",
            "--device-slug",
            "omi",
            "--layout",
            str(_layout(tmp_path)),
            "--confirm-read",
        ],
    )

    assert result.exit_code == 0
    assert json.loads(result.output)["status"] == "sealed"
    assert json.loads(result.output)["deduplicated"] is True
    assert "records.bin" not in result.output


def test_collect_command_redacts_operational_errors(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    async def fail_collect(
        _address: str,
        _adapter: str,
        _device_slug: str,
        _staging: object,
        _max_records: int,
    ) -> CollectionResult:
        del _staging
        raise OSError("raw ring detail")

    monkeypatch.setattr(device_cli, "collect", fail_collect)
    result = CliRunner().invoke(
        app,
        [
            "device",
            "collect",
            "--address",
            "AA:BB",
            "--device-slug",
            "omi",
            "--layout",
            str(_layout(tmp_path)),
            "--confirm-read",
        ],
    )

    assert result.exit_code == 1
    assert result.output.strip() == "device operation failed: OSError"
    assert "raw ring detail" not in result.output


@pytest.mark.parametrize(
    "manifest_text",
    ["not-json", "[]", '{"raw_sha256": 123}'],
)
def test_render_collect_rejects_malformed_manifest(tmp_path: Path, manifest_text: str) -> None:
    bundle = tmp_path / "bundle"
    bundle.mkdir()
    (bundle / "manifest.json").write_text(manifest_text, encoding="utf-8")
    result = CollectionResult(
        RingInfo(10, 11, 100, 0, RECORD_SIZE),
        1,
        SealResult(bundle, False),
    )

    with pytest.raises(RuntimeError, match="sealed bundle metadata"):
        device_cli.render_collect(result)


def test_render_collect_rejects_missing_manifest(tmp_path: Path) -> None:
    bundle = tmp_path / "bundle"
    bundle.mkdir()
    result = CollectionResult(
        RingInfo(10, 11, 100, 0, RECORD_SIZE),
        1,
        SealResult(bundle, False),
    )

    with pytest.raises(RuntimeError, match="could not be read"):
        device_cli.render_collect(result)


@pytest.mark.parametrize("max_records", [0, device_cli.MAX_RECORDS + 1])
def test_collect_rejects_max_records_outside_bound(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, max_records: int
) -> None:
    _install_fakes(monkeypatch, FakeSession(_status()), [])

    with pytest.raises(ValueError, match="max_records must be between"):
        asyncio.run(device_cli.collect("AA:BB", "hci0", "omi", _store(tmp_path), max_records))


def test_run_uses_windows_asyncio_path(monkeypatch: pytest.MonkeyPatch) -> None:
    async def operation() -> int:
        return 17

    def fake_run(coroutine: Coroutine[object, object, object]) -> int:
        coroutine.close()
        return 17

    monkeypatch.setattr(device_cli.sys, "platform", "win32")
    monkeypatch.setattr(device_cli.asyncio, "run", fake_run)

    assert device_cli.run(operation()) == 17


def test_posix_runner_converts_fake_sigterm_cancellation(monkeypatch: pytest.MonkeyPatch) -> None:
    class FakeLoop:
        def add_signal_handler(self, _signal: signal.Signals, callback: Callable[[], object]) -> None:
            callback()

        def remove_signal_handler(self, _signal: signal.Signals) -> bool:
            return True

    monkeypatch.setattr(device_cli.asyncio, "get_running_loop", lambda: FakeLoop())

    async def operation() -> None:
        return

    with pytest.raises(device_cli.OperationInterruptedError, match="after cleanup"):
        asyncio.run(device_cli._run_posix_operation(operation()))


def test_posix_runner_reraises_unrelated_cancellation(monkeypatch: pytest.MonkeyPatch) -> None:
    class FakeLoop:
        def add_signal_handler(self, _signal: signal.Signals, _callback: object) -> None:
            return

        def remove_signal_handler(self, _signal: signal.Signals) -> bool:
            return True

    monkeypatch.setattr(device_cli.asyncio, "get_running_loop", lambda: FakeLoop())

    async def operation() -> None:
        raise asyncio.CancelledError

    with pytest.raises(asyncio.CancelledError):
        asyncio.run(device_cli._run_posix_operation(operation()))


def test_sealed_collect_output_contains_only_bundle_metadata(tmp_path: Path) -> None:
    bundle = tmp_path / "omi" / "10-11-deadbeef"
    bundle.mkdir(parents=True)
    (bundle / "manifest.json").write_text('{"raw_sha256":"abc123"}', encoding="utf-8")
    result = CollectionResult(
        RingInfo(10, 11, 100, 0, RECORD_SIZE),
        1,
        SealResult(bundle, False),
    )

    rendered = device_cli.render_collect(result)

    assert json.loads(rendered) == {
        "bundle_path": str(bundle),
        "deduplicated": False,
        "next_sequence": 11,
        "raw_sha256": "abc123",
        "record_count": 1,
        "start_sequence": 10,
        "status": "sealed",
    }
    assert "records.bin" not in rendered


def test_help_has_no_manual_destructive_ring_commands() -> None:
    result = CliRunner().invoke(app, ["device", "--help"])

    assert result.exit_code == 0
    assert "sync" in result.output
    assert "CLEAR" not in result.output


@pytest.mark.skipif(sys.platform == "win32", reason="SIGTERM loop handlers are POSIX-only")
def test_sigterm_cancels_operation_after_fake_context_cleanup() -> None:
    script = """
import asyncio
import os
import signal

from omi_collector.capture import cli as device_cli

cleaned_up = False

class FakeGuard:
    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, traceback):
        global cleaned_up
        cleaned_up = True

async def operation():
    async with FakeGuard():
        asyncio.get_running_loop().call_soon(os.kill, os.getpid(), signal.SIGTERM)
        await asyncio.Future()

try:
    device_cli.run(operation())
except device_cli.OperationInterruptedError:
    if not cleaned_up:
        raise AssertionError("fake context cleanup did not run")
else:
    raise AssertionError("SIGTERM did not interrupt the operation")
"""
    environment = os.environ | {"PYTHONPATH": str(Path(__file__).parents[1] / "src")}

    result = subprocess.run(
        [sys.executable, "-c", script],
        check=False,
        capture_output=True,
        env=environment,
        text=True,
    )

    assert result.returncode == 0, result.stderr
