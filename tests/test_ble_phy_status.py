from datetime import UTC, datetime
from pathlib import Path
from typing import cast

import pytest

from omi_collector.capture.adapters.collector_state_db import StateDB
from omi_collector.capture.adapters.debug_logging import read_runtime_status, write_runtime_status_event
from omi_collector.capture.adapters.operational_status import OperationalIdentity
from omi_collector.operator_status import _project_runtime_status

INVOCATION = "00000000000000000000000000000001"
OTHER_INVOCATION = "00000000000000000000000000000002"
IDENTITY = OperationalIdentity("00000000-0000-0000-0000-000000000001", INVOCATION)


def _event(link: str, state: str, *, invocation: str = INVOCATION, **values: object) -> dict[str, object]:
    return {
        "invocation_id": invocation,
        "link_id": link,
        "state": state,
        "phy_policy": "auto",
        "tx_phy": None,
        "rx_phy": None,
        "source": None,
        "reason": None,
        **values,
    }


def _view(db: StateDB, identity: OperationalIdentity = IDENTITY) -> dict[str, object]:
    return _project_runtime_status(
        read_runtime_status(db) or {}, datetime(2026, 1, 1, 0, 0, 30, tzinfo=UTC), {}, identity
    )


def test_persisted_phy_observations_remain_historical_across_link_events(tmp_path: Path) -> None:
    db = StateDB(tmp_path / "state.sqlite3")
    first, second = "1" * 32, "2" * 32
    write_runtime_status_event(db, "ble_link_status", _event(first, "connected"), "2026-01-01T00:00:01+00:00")
    write_runtime_status_event(
        db,
        "ble_link_status",
        _event(first, "observed", tx_phy="1M", rx_phy="2M", source="read_phy"),
        "2026-01-01T00:00:02+00:00",
    )
    view = _view(StateDB(tmp_path / "state.sqlite3"))
    phy = view["last_observed_phy"]
    assert isinstance(phy, dict)
    assert (phy["tx_phy"], phy["rx_phy"], phy["phy_policy"], phy["source"], phy["age_seconds"]) == (
        "1M",
        "2M",
        "auto",
        "read_phy",
        28,
    )
    assert "current_phy" not in view
    write_runtime_status_event(db, "ble_link_status", _event(second, "connected"), "2026-01-01T00:00:03+00:00")
    write_runtime_status_event(
        db,
        "ble_link_status",
        _event(second, "observed", tx_phy="2M", rx_phy="1M", source="phy_update"),
        "2026-01-01T00:00:04+00:00",
    )
    write_runtime_status_event(db, "ble_link_status", _event(first, "disconnected"), "2026-01-01T00:00:05+00:00")
    view = _view(db)
    assert cast(dict[str, object], view["last_observed_phy"])["tx_phy"] == "2M"
    assert cast(dict[str, object], view["last_observed_phy"])["rx_phy"] == "1M"
    assert cast(dict[str, object], view["link_observation_state"])["state"] == "observed"
    write_runtime_status_event(db, "ble_link_status", _event(second, "disconnected"), "2026-01-01T00:00:06+00:00")
    assert cast(dict[str, object], _view(db)["link_observation_state"])["state"] == "not_connected"
    other = OperationalIdentity(IDENTITY.boot_id, OTHER_INVOCATION)
    assert cast(dict[str, object], _view(db, other)["link_observation_state"])["state"] != "observed"


def test_failed_read_marks_observation_unavailable(tmp_path: Path) -> None:
    db = StateDB(tmp_path / "state.sqlite3")
    link = "1" * 32
    write_runtime_status_event(db, "ble_link_status", _event(link, "connected"), "2026-01-01T00:00:01+00:00")
    assert cast(dict[str, object], _view(db)["link_observation_state"])["state"] == "pending"
    write_runtime_status_event(
        db, "ble_link_status", _event(link, "unavailable", reason="0x1a"), "2026-01-01T00:00:02+00:00"
    )
    view = _view(db)
    assert view["last_observed_phy"] is None
    assert cast(dict[str, object], view["link_observation_state"])["state"] == "unavailable"
    assert cast(dict[str, object], view["link_observation_state"])["reason"] == "0x1a"


def test_observer_degradation_and_rssi_keep_distinct_observation_times(tmp_path: Path) -> None:
    db = StateDB(tmp_path / "state.sqlite3")
    link = "1" * 32
    write_runtime_status_event(db, "ble_link_rssi_observed", {"rssi_dbm": -47}, "2026-01-01T00:00:01+00:00")
    write_runtime_status_event(db, "ble_link_status", _event(link, "connected"), "2026-01-01T00:00:02+00:00")
    write_runtime_status_event(
        db,
        "ble_link_status",
        _event(link, "observed", tx_phy="1M", rx_phy="1M", source="read_phy"),
        "2026-01-01T00:00:03+00:00",
    )
    write_runtime_status_event(
        db,
        "ble_link_status",
        _event(link, "unavailable", reason="observer_degraded"),
        "2026-01-01T00:00:04+00:00",
    )
    view = _view(db)
    assert view["connection_rssi_dbm"] == -47
    assert datetime.fromisoformat(cast(str, view["connection_rssi_observed_at"])) == datetime(
        2026, 1, 1, 0, 0, 1, tzinfo=UTC
    )
    assert datetime.fromisoformat(
        cast(str, cast(dict[str, object], view["last_observed_phy"])["observed_at"])
    ) == datetime(2026, 1, 1, 0, 0, 3, tzinfo=UTC)
    assert cast(dict[str, object], view["link_observation_state"])["state"] == "unavailable"
    assert cast(dict[str, object], view["link_observation_state"])["reason"] == "observer_degraded"


def test_phy_status_is_persisted_by_real_quality_worker(tmp_path: Path) -> None:
    from omi_collector.capture.adapters.quality_metrics import SQLiteQualityMetrics

    db = StateDB(tmp_path / "state.sqlite3")
    writer = SQLiteQualityMetrics(db, release_version="1.2.3")
    assert writer.record_runtime_status("ble_link_status", _event("1" * 32, "connected"), "2026-01-01T00:00:01+00:00")
    assert writer.record_runtime_status(
        "ble_link_status",
        _event("1" * 32, "observed", tx_phy="1M", rx_phy="2M", source="read_phy"),
        "2026-01-01T00:00:02+00:00",
    )
    assert writer.close(timeout_seconds=2)
    view = _view(StateDB(tmp_path / "state.sqlite3"))
    assert cast(dict[str, object], view["last_observed_phy"])["tx_phy"] == "1M"
    assert cast(dict[str, object], view["last_observed_phy"])["rx_phy"] == "2M"


def test_operator_status_outside_systemd_shows_historical_phy(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from omi_collector.capture.adapters.collector_state_db import StateKind
    from omi_collector.capture.adapters.confirmed_loss import ConfirmedLossLedger
    from omi_collector.capture.adapters.operational_status import OperationalStatusStore
    from omi_collector.operator_status import collect_operator_status
    from omi_collector.state_migration import open_runtime_state
    from omi_collector.storage_layout import load_operator_config

    monkeypatch.delenv("INVOCATION_ID", raising=False)
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
    write_runtime_status_event(db, "ble_link_status", _event("1" * 32, "connected"), "2026-01-01T00:00:01+00:00")
    write_runtime_status_event(
        db,
        "ble_link_status",
        _event("1" * 32, "observed", tx_phy="1M", rx_phy="2M", source="read_phy"),
        "2026-01-01T00:00:02+00:00",
    )
    result = collect_operator_status(layout, hours=24, now=datetime(2026, 1, 1, 0, 0, 30, tzinfo=UTC))
    runtime = cast(dict[str, object], result["runtime"])
    assert cast(dict[str, object], runtime["last_observed_phy"])["tx_phy"] == "1M"
    assert cast(dict[str, object], runtime["last_observed_phy"])["rx_phy"] == "2M"
    assert cast(dict[str, object], runtime["last_observed_phy"])["age_seconds"] == 28
    assert cast(dict[str, object], runtime["link_observation_state"])["state"] == "unknown"


@pytest.mark.parametrize("field", ["state", "phy_policy", "tx_phy", "rx_phy", "source"])
@pytest.mark.parametrize("bad_value", [[], {}])
def test_corrupt_nested_phy_projection_is_rejected(tmp_path: Path, field: str, bad_value: object) -> None:

    from omi_collector.capture.adapters.collector_state_db import StateKind

    db = StateDB(tmp_path / "state.sqlite3")
    write_runtime_status_event(db, "ble_link_status", _event("1" * 32, "connected"), "2026-01-01T00:00:01+00:00")
    projection = read_runtime_status(db)
    assert projection is not None
    cast(dict[str, object], projection["ble_link_status"])[field] = bad_value
    db.put(StateKind.RUNTIME_STATUS, "current", projection)
    with pytest.raises(ValueError, match="invalid"):
        read_runtime_status(db)
