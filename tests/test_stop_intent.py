from __future__ import annotations

import asyncio
import signal
from typing import cast

from omi_collector.capture.application.stop_intent import StopIntent, StopRequestedError


def test_signal_before_attach_schedules_one_cancellation_and_keeps_first_signal() -> None:
    scheduled: list[object] = []

    class FakeLoop:
        def call_soon_threadsafe(self, callback: object) -> None:
            scheduled.append(callback)

    class FakeTask:
        def cancel(self) -> None:
            pass

    intent = StopIntent()
    intent.request(signal.SIGTERM)
    intent.attach(cast(asyncio.AbstractEventLoop, FakeLoop()), cast(asyncio.Task[object], FakeTask()))
    intent.request(signal.SIGINT)

    assert intent.requested
    assert intent.signal_number == signal.SIGTERM
    assert intent.exit_code == 143
    assert len(scheduled) == 1
    try:
        intent.check()
    except StopRequestedError:
        pass
    else:
        raise AssertionError("stop admission was allowed")


def test_signal_after_attach_schedules_one_cancellation() -> None:
    async def scenario() -> None:
        async def wait_forever() -> None:
            await asyncio.Future()

        intent = StopIntent()
        task = asyncio.create_task(wait_forever())
        intent.attach(asyncio.get_running_loop(), task)
        intent.request(signal.SIGINT)
        intent.request(signal.SIGTERM)
        try:
            await task
        except asyncio.CancelledError:
            pass
        else:
            raise AssertionError("attached task was not cancelled")
        assert intent.exit_code == 130
        intent.detach(task)

    asyncio.run(scenario())
