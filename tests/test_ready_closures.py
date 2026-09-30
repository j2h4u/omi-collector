"""Durable physical-visit closure queue contracts."""

from __future__ import annotations

from json import loads
from pathlib import Path

import pytest

from omi_collector.capture.adapters.ready_closures import ReadyClosureError, append, load, remove


def test_closures_are_fifo_and_replay_safe(tmp_path: Path) -> None:
    path = tmp_path / "ready-closures.json"

    first = append(path, 10, "absence")
    assert append(path, 10, "recovery_exhausted") == first
    second = append(path, 20, "restart_interrupted")

    assert load(path) == (first, second)
    remove(path, first)
    assert load(path) == (second,)
    assert loads(path.read_text(encoding="utf-8"))["version"] == 1


def test_closures_reject_malformed_or_regressing_state(tmp_path: Path) -> None:
    path = tmp_path / "ready-closures.json"
    path.write_text('{"closures":[{"next_sequence":20,"reason":"absence"}],"version":1}', encoding="utf-8")

    with pytest.raises(ReadyClosureError, match="frontier regressed"):
        append(path, 10, "absence")

    path.write_text('{"closures":[{"next_sequence":20}],"version":1}', encoding="utf-8")
    with pytest.raises(ReadyClosureError, match="entry schema"):
        load(path)
