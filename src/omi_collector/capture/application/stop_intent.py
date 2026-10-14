"""One command-lifetime termination intent across startup and async cleanup."""

from __future__ import annotations

import asyncio
import signal
import threading
from collections.abc import Iterator
from contextlib import contextmanager
from types import FrameType


class StopRequestedError(asyncio.CancelledError):
    """The command has been asked to stop admitting new device work."""


class StopIntent:
    """Latch the first TERM/INT and cancel an attached root task at most once."""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._signal_number: int | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._root_task: asyncio.Task[object] | None = None
        self._cancel_scheduled = False

    @property
    def requested(self) -> bool:
        with self._lock:
            return self._signal_number is not None

    @property
    def signal_number(self) -> int | None:
        with self._lock:
            return self._signal_number

    @property
    def exit_code(self) -> int:
        return 128 + (self.signal_number or signal.SIGTERM)

    def check(self) -> None:
        if self.requested:
            raise StopRequestedError("command stop requested")

    def request(self, signum: int) -> None:
        with self._lock:
            if self._signal_number is not None:
                return
            self._signal_number = signum
            self._schedule_cancel()

    def attach(self, loop: asyncio.AbstractEventLoop, root_task: asyncio.Task[object]) -> None:
        with self._lock:
            if self._root_task is not None:
                raise RuntimeError("stop intent already has a root task")
            self._loop = loop
            self._root_task = root_task
            self._schedule_cancel()

    def detach(self, root_task: asyncio.Task[object]) -> None:
        with self._lock:
            if self._root_task is root_task:
                self._root_task = None
                self._loop = None

    def _schedule_cancel(self) -> None:
        if self._signal_number is None or self._loop is None or self._root_task is None or self._cancel_scheduled:
            return
        self._cancel_scheduled = True
        self._loop.call_soon_threadsafe(self._root_task.cancel)

    def _handle_signal(self, signum: int, _frame: FrameType | None) -> None:
        self.request(signum)

    @contextmanager
    def installed(self) -> Iterator[None]:
        """Keep the same Python handlers installed through the whole command."""

        old_term = signal.signal(signal.SIGTERM, self._handle_signal)
        try:
            old_int = signal.signal(signal.SIGINT, self._handle_signal)
        except BaseException:
            signal.signal(signal.SIGTERM, old_term)
            raise
        try:
            yield
        finally:
            signal.signal(signal.SIGINT, old_int)
            signal.signal(signal.SIGTERM, old_term)
