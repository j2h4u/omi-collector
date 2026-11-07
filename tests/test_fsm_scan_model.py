"""Executable bounded scan-health contract and generated runtime comparison.

Run: uv run --no-sync pytest -q tests/test_fsm_scan_model.py --hypothesis-show-statistics
Model state: incident open/closed and latest failure time (initially closed, none).
Events: failure, current/stale-generation callback, flush; a callback can resolve
only when its observation is strictly later than the latest failure. Failure and
flush represent successful durable sink effects. A stale generation is ignored.
The bounded checker enumerates all length-four sequences over failure and three
callback classes, checks reachable edges and safety. It has no terminal state:
flush is always enabled. Persistence retry, cancellation and concurrent callbacks
are outside this model and must be checked separately. The generated runtime test
resets the real monitor and oracle per example, and Hypothesis shrinks failures.
This bounded check is not a proof of runtime equivalence or eventual progress;
progress assumes a current-generation later callback and successful storage.
"""

from __future__ import annotations

import asyncio
from itertools import product

from hypothesis import event, given, settings
from hypothesis import strategies as st

from omi_collector.capture.application.ble_scan_health import ScanHealthMonitor, ScanIncidentContext


class Sink:
    def __init__(self) -> None:
        self.is_active = False
        self.resolutions = 0

    async def active(self) -> bool:
        return self.is_active

    async def open(self, context: ScanIncidentContext) -> None:
        del context
        self.is_active = True

    async def resolve(self, generation: int, reason: str) -> None:
        del generation, reason
        self.is_active = False
        self.resolutions += 1


class Diagnostics:
    async def prepare(self, generation: int) -> None:
        pass

    async def close(self, generation: int) -> None:
        pass

    async def snapshot(self, generation: int) -> dict[str, object]:
        del generation
        return {}

    def healthy(self, generation: int) -> None:
        pass


ACTIONS = ("failure", "stop_failure", "old", "equal", "new", "foreign")


def test_bounded_contract_reaches_all_callback_classes() -> None:
    edges: set[tuple[bool, str, bool]] = set()
    for sequence in product(ACTIONS, repeat=4):
        active = False
        failure = -1
        for now, action in enumerate(sequence):
            before = active
            if action in {"failure", "stop_failure"}:
                failure = now
                active = True
            elif action == "new" and failure >= 0:
                active = False
            if action in {"old", "equal", "foreign"}:
                assert active == before
            edges.add((before, action, active))
    assert {(True, action, True) for action in ("old", "equal", "foreign")} <= edges
    assert (True, "new", False) in edges
    assert (False, "failure", True) in edges
    assert (True, "failure", True) in edges


@settings(max_examples=40, deadline=None)
@given(st.lists(st.sampled_from(ACTIONS), min_size=1, max_size=15))
def test_generated_scan_failure_chronology(actions: list[str]) -> None:
    async def scenario() -> None:
        now = [0.0]
        sink = Sink()
        monitor = ScanHealthMonitor(sink, Diagnostics(), 1000.0, clock=lambda: now[0])
        await monitor.prepare(1)
        active = False
        failure = -1.0
        for index, action in enumerate(actions):
            now[0] = float(index * 10 + 10)
            before = active
            if action in {"failure", "stop_failure"}:
                if action == "failure":
                    await monitor.scan_start_failed(1, OSError("failure"))
                else:
                    await monitor.scan_stop_failed(1, OSError("failure"))
                active = True
                failure = now[0]
            else:
                observation = {"old": failure - 1, "equal": failure, "new": failure + 1, "foreign": now[0]}[action]
                monitor.radio_activity(2 if action == "foreign" else 1, observation)
                await monitor.flush_recovery()
                if action == "new" and failure >= 0:
                    active = False
            event(f"{before}:{action}:{active}")
            assert sink.is_active == active

    asyncio.run(scenario())
