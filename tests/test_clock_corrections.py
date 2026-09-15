from __future__ import annotations

import json
from pathlib import Path
from typing import cast

import pytest

from omi_collector.capture.adapters.clock_corrections import (
    ClockCorrectionError,
    ClockCorrectionStore,
)


def test_intent_is_durable_before_confirmation(tmp_path: Path) -> None:
    state = tmp_path / "device.json"
    store = ClockCorrectionStore(state, tmp_path / "attempts")
    intent = store.prepare("omi", 1360, 1000, 360.2, 20)

    prepared = cast(
        dict[str, object],
        json.loads((tmp_path / "clock-corrections" / "omi" / f"{intent.operation_id}.json").read_text()),
    )
    assert prepared["state"] == "prepared"

    intent = store.mark_unresolved(intent)
    store.finish(intent, state="applied", boundary_sequence_max=22, verified_epoch=1001)
    assert store.confirmed("omi")[0].state == "applied"


def test_active_attempt_prevents_clock_write_but_retired_attempt_does_not(tmp_path: Path) -> None:
    attempts = tmp_path / "attempts"
    active = attempts / "active"
    active.mkdir(parents=True)
    store = ClockCorrectionStore(tmp_path / "device.json", attempts)

    with pytest.raises(ClockCorrectionError, match="active audio attempt"):
        store.prepare("omi", 1100, 1000, 100.0, 20)

    (active / "terminal-retired.json").write_text("{}", encoding="utf-8")
    assert store.prepare("omi", 1100, 1000, 100.0, 20).state == "prepared"


def test_unresolved_clock_write_is_not_confirmed(tmp_path: Path) -> None:
    store = ClockCorrectionStore(tmp_path / "device.json", tmp_path / "attempts")
    intent = store.prepare("omi", 1100, 1000, 100.0, 20)
    intent = store.mark_unresolved(intent)
    store.finish(intent, state="unresolved", boundary_sequence_max=None, verified_epoch=None)

    assert store.confirmed("omi") == ()
