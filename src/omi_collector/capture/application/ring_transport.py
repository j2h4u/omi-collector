"""Side-effect-free interfaces and notification plumbing for the Omi ring.

The protocol opcodes and their wire encodings live in :mod:`ring_protocol`.
This module only defines the asynchronous boundary used by a collector and by
scripted transports in tests.  In particular, it has no Bluetooth dependency.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from typing import Protocol, Self

from ...config import DEFAULT_CONFIG
from ..domain.ring_protocol import RingStatus

RING_SERVICE_UUID = "30295780-4301-eabd-2904-2849adfeae43"
CONTROL_CHARACTERISTIC_UUID = "30295781-4301-eabd-2904-2849adfeae43"
STATUS_CHARACTERISTIC_UUID = "30295782-4301-eabd-2904-2849adfeae43"


# The upstream firmware's minimum ATT notification value is 20 bytes,
# including the one-byte DATA opcode and leaving 19 audio bytes per
# notification. Sources are pinned to the inspected upstream commit:
# firmware https://github.com/BasedHardware/omi/blob/6f7c57ac1545c1931c806a01605646405d398198/omi/firmware/omi/src/lib/core/storage.c#L45-L50
# app https://github.com/BasedHardware/omi/blob/6f7c57ac1545c1931c806a01605646405d398198/app/lib/services/wals/ring_storage_sync.dart#L545-L608
# This queue is only a bounded transport/control cushion.  Transfer data is
# copied into the preallocated TransferArena admitted from one fresh INFO
# snapshot (512 MiB by default); the BLE event-loop callback performs no disk
# I/O, and a dedicated writer thread may stall independently.  Do not size or
# describe this queue as the reservoir for a complete snapshot or for disk
# backpressure.
class RingTransportError(RuntimeError):
    """Base class for failures at the BLE transport boundary."""


class RingTransportDisconnectedError(RingTransportError, ConnectionError):
    """The peer disconnected while a ring session was active."""


class RingTransportUnavailableError(RingTransportError, ConnectionError):
    """The pendant could not be connected or a connection setup failed."""


class CandidateUnavailableError(RingTransportUnavailableError):
    """A previously observed candidate can no longer be used for connection."""


class NotificationOverflowError(RingTransportError):
    """The bounded control-notification queue could not accept another item."""


class NotificationProtocolError(RingTransportError):
    """A control-notification callback delivered an invalid payload."""


RingTransportDisconnected = RingTransportDisconnectedError


class ControlNotificationStream(AsyncIterator[bytes]):
    """A bounded transport/control cushion fed by a BLE callback."""

    _END = object()

    def __init__(self, buffer_bytes: int = DEFAULT_CONFIG.memory.notification_buffer_bytes) -> None:
        if buffer_bytes < 1:
            raise ValueError("notification buffer byte budget must be positive")
        self._buffer_bytes = buffer_bytes
        self._buffered_bytes = 0
        # The queue itself is unbounded; the explicit byte budget below is the
        # actual bound because each notification may have a different length.
        self._queue: asyncio.Queue[bytes] = asyncio.Queue()
        self._closed = False
        self._terminal: BaseException | object | None = None
        self._terminal_event = asyncio.Event()

    def __aiter__(self) -> Self:
        return self

    async def __anext__(self) -> bytes:
        while True:
            try:
                payload = self._queue.get_nowait()
                self._buffered_bytes -= len(payload)
                return payload
            except asyncio.QueueEmpty:
                pass

            terminal = self._terminal
            if terminal is not None:
                if terminal is self._END:
                    raise StopAsyncIteration
                if isinstance(terminal, BaseException):
                    raise terminal
                raise RuntimeError("invalid notification stream terminal state")

            queue_get = asyncio.create_task(self._queue.get())
            terminal_wait = asyncio.create_task(self._terminal_event.wait())
            try:
                done, _pending = await asyncio.wait((queue_get, terminal_wait), return_when=asyncio.FIRST_COMPLETED)
            except BaseException:
                queue_get.cancel()
                terminal_wait.cancel()
                await asyncio.gather(queue_get, terminal_wait, return_exceptions=True)
                raise
            if queue_get in done:
                terminal_wait.cancel()
                await asyncio.gather(terminal_wait, return_exceptions=True)
                payload = queue_get.result()
                self._buffered_bytes -= len(payload)
                return payload
            queue_get.cancel()
            await asyncio.gather(queue_get, return_exceptions=True)

    def feed(self, payload: bytes | bytearray | memoryview) -> None:
        """Copy and enqueue one callback payload.

        Bleak reuses mutable ``bytearray`` callback values in some backends, so
        copying here is part of the transport contract. The first payload that
        would exceed the byte budget is rejected. Payloads already accepted
        remain available in FIFO order before the terminal overflow is raised.
        """
        if self._closed:
            return
        copied = bytes(payload)
        if not copied:
            self._fail(NotificationProtocolError("control notification payload must not be empty"))
            return
        if self._buffered_bytes + len(copied) > self._buffer_bytes:
            self._fail(NotificationOverflowError("control notification buffer overflowed"))
            return
        self._queue.put_nowait(copied)
        self._buffered_bytes += len(copied)

    def fail_disconnected(self) -> None:
        """Wake the consumer with a cleanly typed peer-disconnect failure."""
        if not self._closed:
            self._closed = True
            self._terminal = RingTransportDisconnectedError("Omi disconnected during ring session")
            self._terminal_event.set()

    def close(self) -> None:
        """Close the stream and discard queued values during session teardown."""
        if not self._closed:
            self._closed = True
            self._terminal = self._END
            self._terminal_event.set()
        self._discard_pending()

    def _fail(self, error: BaseException) -> None:
        self._closed = True
        self._terminal = error
        self._terminal_event.set()

    def _discard_pending(self) -> None:
        while True:
            try:
                payload = self._queue.get_nowait()
            except asyncio.QueueEmpty:
                break
            self._buffered_bytes -= len(payload)

    @property
    def buffered_bytes(self) -> int:
        """Return the number of accepted bytes awaiting consumption."""
        return self._buffered_bytes


class RingSession(Protocol):
    """Minimal session contract consumed by the collector."""

    async def read_status(self) -> RingStatus:
        """Read fresh status from the cached-status characteristic."""
        ...

    def notifications(self) -> AsyncIterator[bytes]:
        """Return the control/data notification stream."""
        ...

    async def write_control(self, payload: bytes) -> None:
        """Write an already encoded safe ring command with response requested."""
        ...

    async def close(self) -> None:
        """Stop notifications and disconnect the session."""
        ...


class RingTransport(Protocol):
    """Async connection lifecycle expected from a ring transport."""

    async def connect(self) -> RingSession:
        """Connect and subscribe before returning a usable session."""
        ...

    async def disconnect(self) -> None:
        """Idempotently tear down the active session."""
        ...

    async def __aenter__(self) -> RingSession:
        """Connect the transport for an async context."""
        ...

    async def __aexit__(self, exc_type: object, _exc_value: object, traceback: object) -> None:
        """Disconnect the transport when leaving an async context."""
        ...
