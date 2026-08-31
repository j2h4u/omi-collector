"""Scripted in-memory ring session for collector state-machine tests."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Iterable
from dataclasses import dataclass
from typing import cast

from omi_collector.capture.application.ring_transport import RingTransportDisconnectedError
from omi_collector.capture.domain.ring_protocol import RingStatus


@dataclass(frozen=True, slots=True)
class DelayedNotification:
    """One notification delivered after a deterministic event-loop delay."""

    delay: float
    payload: bytes


ScriptItem = bytes | BaseException | DelayedNotification


@dataclass(frozen=True, slots=True)
class WriteStep:
    """One expected write and the notifications it makes observable."""

    expected: bytes
    notifications: tuple[ScriptItem, ...] = ()
    error: BaseException | None = None


class ScriptedRingSession:
    """A connected session whose scripted events are released by control writes."""

    _END = object()

    def __init__(self, status: RingStatus, steps: Iterable[WriteStep] = ()) -> None:
        self._status = status
        self._steps = list(steps)
        self._queue: asyncio.Queue[object] = asyncio.Queue()
        self.writes: list[bytes] = []
        self.status_reads = 0
        self.closed = False

    async def read_status(self) -> RingStatus:
        self.status_reads += 1
        return self._status

    def notifications(self) -> AsyncIterator[bytes]:
        return self._iter_notifications()

    async def write_control(self, payload: bytes) -> None:
        written = bytes(payload)
        self.writes.append(written)
        if not self._steps:
            raise AssertionError("unexpected control write")
        step = self._steps.pop(0)
        if written != step.expected:
            raise AssertionError("control write did not match the script")
        for item in step.notifications:
            self._queue.put_nowait(item)
        if step.error is not None:
            raise step.error

    async def close(self) -> None:
        self.closed = True
        self._queue.put_nowait(self._END)

    def emit(self, payload: bytes) -> None:
        self._queue.put_nowait(bytes(payload))

    def fail(self, error: BaseException) -> None:
        self._queue.put_nowait(error)

    def disconnect(self) -> None:
        self.fail(RingTransportDisconnectedError("scripted ring session disconnected"))

    async def _iter_notifications(self) -> AsyncIterator[bytes]:
        while True:
            item = await self._queue.get()
            if item is self._END:
                return
            if isinstance(item, BaseException):
                raise item
            if isinstance(item, DelayedNotification):
                await asyncio.sleep(item.delay)
                yield item.payload
                continue
            yield cast(bytes, item)
