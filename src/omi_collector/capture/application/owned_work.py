"""Cancellation-safe ownership of asynchronous work and blocking calls."""

from __future__ import annotations

import asyncio
import inspect
from collections.abc import Awaitable, Callable
from typing import cast


async def join_owned[T](awaitable: Awaitable[T]) -> T:
    """Join through repeated cancellation and report the settled worker result."""

    task = asyncio.ensure_future(awaitable)
    cancellation: asyncio.CancelledError | None = None
    while True:
        try:
            result = await asyncio.shield(task)
        except asyncio.CancelledError as error:
            cancellation = error
            if task.done():
                break
        else:
            if cancellation is not None:
                raise cancellation
            return result
    result = task.result()  # The original worker error outranks cancellation.
    if cancellation is not None:
        raise cancellation
    return result


async def joined_to_thread[T](function: Callable[..., T], *args: object, **kwargs: object) -> T:
    """Run a blocking call in a worker and join its actual outcome."""

    return await join_owned(asyncio.to_thread(function, *args, **kwargs))


async def invoke_owned[T](function: Callable[..., T | Awaitable[T]], *args: object, **kwargs: object) -> T:
    """Own a sync callback invocation and the awaitable it may return as one task."""

    async def _invoke() -> T:
        result = await asyncio.to_thread(function, *args, **kwargs)
        if inspect.isawaitable(result):
            return await cast(Awaitable[T], result)
        return cast(T, result)

    return await join_owned(_invoke())
