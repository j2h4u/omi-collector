from __future__ import annotations

import asyncio
import threading

from omi_collector.capture.application.owned_work import invoke_owned


def test_cancelled_callback_owns_thread_invocation_and_returned_awaitable() -> None:
    async def run() -> None:
        started = threading.Event()
        release = threading.Event()
        awaited = threading.Event()
        main_thread = threading.get_ident()
        invocation_threads: list[int] = []

        def callback() -> object:
            invocation_threads.append(threading.get_ident())
            started.set()
            assert release.wait(2)

            async def returned() -> None:
                awaited.set()

            return returned()

        task = asyncio.create_task(invoke_owned(callback))
        assert await asyncio.to_thread(started.wait, 2)
        assert invocation_threads != [main_thread]
        task.cancel()
        release.set()
        try:
            await task
        except asyncio.CancelledError:
            pass
        else:
            raise AssertionError("cancellation must be re-raised after the callback settles")
        assert awaited.is_set()

    asyncio.run(run())
