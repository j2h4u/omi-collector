from __future__ import annotations

import json
import math
from pathlib import Path

import pytest

from omi_collector.capture.adapters.clock_observations import ClockObservationError, ClockObservationStore

# Dynamic malformed-schema fixtures intentionally cross the JSON boundary.
# pyright: reportAny=false, reportArgumentType=false


def _values() -> dict[str, object]:
    return {
        "session_id": "session-a",
        "host_boot_id": "boot-a",
        "host_realtime_start": 1789749500.0,
        "host_realtime_end": 1789749500.2,
        "host_monotonic_start": 42.0,
        "host_monotonic_end": 42.2,
        "device_epoch": 1789749500,
        "info_sequence_min": 10,
        "info_sequence_max": 12,
    }


def test_observation_store_is_causal_and_immutable(tmp_path: Path) -> None:
    store = ClockObservationStore(tmp_path / "device.json")
    first = store.append(evidence_kind="native_trusted", **_values())
    second = store.append(
        evidence_kind="native_trusted",
        **{**_values(), "device_epoch": 1789749501, "info_sequence_min": 12, "info_sequence_max": 14},
    )
    established = store.append(
        evidence_kind="native_trusted",
        operation_id="op-a",
        effective_boundary_sequence=11,
        **_values(),
    )

    assert [item.causal_order for item in store.records()] == [0, 1, 2]
    assert established.effective_boundary_sequence == 11
    assert established.causal_order == 2
    assert established.parent_observation_id == first.observation_id
    assert established.observation_role == "initial"
    assert store.records()[0].effective_boundary_sequence is None
    assert second.causal_order == 1


@pytest.mark.parametrize(
    "field,value",
    [
        ("host_realtime_start", math.nan),
        ("host_monotonic_end", math.inf),
        ("info_sequence_max", 9),
        ("effective_boundary_sequence", 99),
        ("evidence_kind", "within_threshold"),
    ],
)
def test_store_rejects_untrusted_or_invalid_evidence(tmp_path: Path, field: str, value: object) -> None:
    values = _values()
    values[field] = value
    with pytest.raises(ClockObservationError):
        kind = values.pop("evidence_kind", "native_trusted")
        ClockObservationStore(tmp_path / "device.json").append(evidence_kind=kind, **values)


def test_store_rejects_noncanonical_or_gapped_ledger(tmp_path: Path) -> None:
    store = ClockObservationStore(tmp_path / "device.json")
    item = store.append(evidence_kind="native_trusted", **_values())
    path = tmp_path / "clock-observations" / f"{item.observation_id}.json"
    document = json.loads(path.read_text())
    document["causal_order"] = 2
    path.write_text(json.dumps(document, sort_keys=True, separators=(",", ":")) + "\n")
    with pytest.raises(ClockObservationError):
        store.records()
