import asyncio
import logging
from datetime import UTC, datetime
from pathlib import Path
from typing import cast

import pytest

from omi_collector.capture.adapters.ble_link_observability import BleLinkObserver
from omi_collector.capture.adapters.collector_state_db import StateDB, StateKind
from omi_collector.capture.adapters.debug_logging import (
    read_runtime_status,
    set_runtime_status_sink,
    write_runtime_status_event,
)
from omi_collector.capture.adapters.operational_status import OperationalIdentity
from omi_collector.operator_status import _project_runtime_status

INVOCATION = "1" * 32
IDENTITY = OperationalIdentity("00000000-0000-0000-0000-000000000001", INVOCATION)


def _packet(event: int, payload: bytes) -> bytes:
    return bytes((4, event, len(payload))) + payload


def _connect() -> bytes:
    body = bytearray(18)
    body[1:3] = (0x42).to_bytes(2, "little")
    body[5:11] = b"\x06\x05\x04\x03\x02\x01"
    return _packet(0x3E, b"\x01" + body)


def _view(db: StateDB) -> dict[str, object]:
    return _project_runtime_status(read_runtime_status(db) or {}, datetime.now(UTC), {}, IDENTITY)


@pytest.mark.parametrize(
    "case",
    [(8, "0x08", "supervision_timeout"), (19, "0x13", "remote_user_terminated"), (255, "0xff", "unknown")],
)
def test_disconnect_reason_reaches_log_and_stored_operator_status(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    case: tuple[int, str, str],
) -> None:
    reason, hex_reason, name = case
    monkeypatch.setenv("INVOCATION_ID", INVOCATION)
    db = StateDB(tmp_path / "state.sqlite3")
    logger = logging.getLogger("tests.disconnect.reporting")

    def persist(event: str, fields: dict[str, object], timestamp: str) -> bool:
        write_runtime_status_event(db, event, fields, timestamp)
        return True

    set_runtime_status_sink(logger, persist)
    observer = BleLinkObserver("01:02:03:04:05:06", debug_logger=logger)
    try:
        with caplog.at_level(logging.INFO, logger="omi_collector.ble_link_status"):
            observer.handle_packet(_connect())
            observer.handle_packet(_packet(0x05, b"\x00\x42\x00" + bytes((reason,))))
        state = cast(dict[str, object], _view(StateDB(tmp_path / "state.sqlite3"))["link_observation_state"])
        assert (state["state"], state["reason"], state["disconnect_reason_name"]) == ("not_connected", hex_reason, name)
        assert hex_reason in caplog.text and name in caplog.text
        assert "01:02:03:04:05:06" not in caplog.text
        assert "0x42" not in caplog.text
    finally:
        asyncio.run(observer.close())
        set_runtime_status_sink(logger, None)


def test_stopping_observer_does_not_claim_disconnect(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("INVOCATION_ID", INVOCATION)
    db = StateDB(tmp_path / "state.sqlite3")
    logger = logging.getLogger("tests.disconnect.stop")

    def persist(event: str, fields: dict[str, object], timestamp: str) -> bool:
        write_runtime_status_event(db, event, fields, timestamp)
        return True

    set_runtime_status_sink(logger, persist)
    observer = BleLinkObserver("01:02:03:04:05:06", debug_logger=logger)
    try:
        observer.handle_packet(_connect())
        asyncio.run(observer.close())
        state = cast(dict[str, object], _view(db)["link_observation_state"])
        assert state["state"] == "unavailable"
        assert state["reason"] == "observer_stopped"
        assert state.get("disconnect_reason_name") is None
    finally:
        set_runtime_status_sink(logger, None)


@pytest.mark.parametrize("bad_name", [[], {}])
def test_invalid_persisted_disconnect_name_is_rejected(tmp_path: Path, bad_name: object) -> None:
    db = StateDB(tmp_path / "state.sqlite3")
    fields = {
        "invocation_id": INVOCATION,
        "link_id": "2" * 32,
        "state": "disconnected",
        "phy_policy": "auto",
        "tx_phy": None,
        "rx_phy": None,
        "source": None,
        "reason": "0x08",
    }
    write_runtime_status_event(
        db, "ble_link_status", {**fields, "state": "connected", "reason": None}, "2026-01-01T00:00:00+00:00"
    )
    write_runtime_status_event(db, "ble_link_status", fields, "2026-01-01T00:00:01+00:00")
    projection = read_runtime_status(db)
    assert projection is not None
    cast(dict[str, object], projection["ble_link_status"])["disconnect_reason_name"] = bad_name
    db.put(StateKind.RUNTIME_STATUS, "current", projection)
    with pytest.raises(ValueError, match="invalid"):
        read_runtime_status(db)


@pytest.mark.parametrize(
    "case",
    [
        (8, "disconnected", "0x08", "supervision_timeout"),
        (19, "disconnected", "0x13", "remote_user_terminated"),
        (None, "unavailable", "observer_stopped", None),
    ],
)
def test_operator_status_without_invocation_shows_last_link_event(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    case: tuple[int | None, str, str, str | None],
) -> None:
    from omi_collector.capture.adapters.confirmed_loss import ConfirmedLossLedger
    from omi_collector.capture.adapters.operational_status import OperationalStatusStore
    from omi_collector.operator_status import collect_operator_status
    from omi_collector.state_migration import open_runtime_state
    from omi_collector.storage_layout import load_operator_config

    disconnect_reason, expected_state, expected_reason, expected_name = case
    monkeypatch.setenv("INVOCATION_ID", INVOCATION)
    monkeypatch.setattr("omi_collector.operator_status.system_host_boot_id", lambda: IDENTITY.boot_id)
    config = tmp_path / "config.toml"
    config.write_text('pendant_address = "AA:BB:CC:DD:EE:FF"\nbundle_min_audio_seconds = 3600.0\n', encoding="utf-8")
    (tmp_path / "collector").mkdir()
    layout = load_operator_config(config).storage
    layout.publication.root.mkdir()
    db = StateDB(layout.collector.state_db)
    db.put(StateKind.MIGRATION, "internal_audio_cutover", {"format": 1, "phase": "COMPLETE", "fresh": True})
    db = open_runtime_state(layout)
    ConfirmedLossLedger(db).initialize(allow_create=True)
    OperationalStatusStore(db, IDENTITY).initialize()
    logger = logging.getLogger("tests.disconnect.operator")

    def persist(event: str, fields: dict[str, object], _timestamp: str) -> bool:
        write_runtime_status_event(db, event, fields, "2026-01-01T00:00:02+00:00")
        return True

    set_runtime_status_sink(logger, persist)
    observer = BleLinkObserver("01:02:03:04:05:06", debug_logger=logger)
    try:
        observer.handle_packet(_connect())
        if disconnect_reason is not None:
            observer.handle_packet(_packet(0x05, b"\x00\x42\x00" + bytes((disconnect_reason,))))
        asyncio.run(observer.close())
    finally:
        set_runtime_status_sink(logger, None)
    monkeypatch.delenv("INVOCATION_ID")
    result = collect_operator_status(layout, hours=24, now=datetime(2026, 1, 1, 0, 0, 30, tzinfo=UTC))
    runtime = cast(dict[str, object], result["runtime"])
    historical = cast(dict[str, object], runtime["last_link_observation"])
    assert (historical["state"], historical["reason"], historical["disconnect_reason_name"]) == (
        expected_state,
        expected_reason,
        expected_name,
    )
    assert datetime.fromisoformat(cast(str, historical["observed_at"])) == datetime(2026, 1, 1, 0, 0, 2, tzinfo=UTC)
    assert historical["age_seconds"] == 28
    assert cast(dict[str, object], runtime["link_observation_state"])["state"] == "unknown"
