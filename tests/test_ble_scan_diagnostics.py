import asyncio
import errno
import os
import struct
from collections import deque
from contextlib import suppress

from omi_collector.capture.adapters.ble_scan_diagnostics import ScanDiagnostics


def _event(code: int, payload: bytes) -> bytes:
    return bytes((4, code, len(payload))) + payload


def _legacy_report(data: bytes = b"") -> bytes:
    return b"\x01" + b"\x00" * 8 + bytes((len(data),)) + data + b"\x00"


def _extended_report(data: bytes = b"") -> bytes:
    return b"\x01" + b"\x00" * 23 + bytes((len(data),)) + data


class _Socket:
    def __init__(self, read_fd: int, write_fd: int, *, le_meta_enabled: bool = True) -> None:
        self.read_fd = read_fd
        self.write_fd = write_fd
        self.packets: deque[bytes] = deque()
        self.closed = False
        self.options: list[tuple[int, int, bytes]] = []
        high_mask = 1 << 30 if le_meta_enabled else 0
        self.readback = struct.pack("<IIIH2x", 0, 0, high_mask, 0)

    def fileno(self) -> int:
        return self.read_fd

    def setblocking(self, flag: bool) -> None:
        assert flag is False

    def setsockopt(self, level: int, option: int, value: bytes) -> None:
        self.options.append((level, option, value))

    def getsockopt(self, _level: int, _option: int, _size: int) -> bytes:
        return self.readback

    def recv(self, _size: int) -> bytes:
        if self.packets:
            os.read(self.read_fd, 1)
            return self.packets.popleft()
        with suppress(BlockingIOError):
            os.read(self.read_fd, 1)
        raise BlockingIOError

    def feed(self, packet: bytes) -> None:
        self.packets.append(packet)
        os.write(self.write_fd, b".")

    def close(self) -> None:
        self.closed = True
        os.close(self.read_fd)
        os.close(self.write_fd)


def test_observes_advertising_scan_commands_and_hardware_error_without_retaining_packets() -> None:
    now = iter((1.25, 2.5, 3.75))
    observer = ScanDiagnostics("hci0", clock=lambda: next(now))
    observer._observe(_event(0x3E, b"\x02" + _legacy_report(b"private legacy bytes")))
    observer._observe(_event(0x3E, b"\x0d" + _extended_report(b"private extended bytes")))
    observer._observe(_event(0x0E, b"\x01\x0b\x20\x00"))
    observer._observe(_event(0x0F, b"\x0c\x01\x42\x20"))
    observer._observe(_event(0x10, b"\x01"))

    snapshot = observer.snapshot()
    assert snapshot["counts"] == {
        "legacy_advertising": 1,
        "extended_advertising": 1,
        "hardware_error": 1,
        "malformed": 0,
    }
    assert snapshot["last_monotonic"]["hardware_error"] == 3.75  # type: ignore[index]
    assert snapshot["scan_commands"]["0x200b"] == {"count": 1, "last_status": 0}  # type: ignore[index]
    assert snapshot["scan_commands"]["0x2042"] == {"count": 1, "last_status": 12}  # type: ignore[index]
    assert "private legacy bytes" not in repr(snapshot)
    assert "private extended bytes" not in repr(snapshot)


def test_truncated_packets_are_counted_without_payload_retention() -> None:
    observer = ScanDiagnostics("hci0")
    observer._observe(b"\x04\x3e\x05\x02")
    observer._observe(b"\x02\x00\x00")
    observer._observe(_event(0x3E, b""))
    observer._observe(_event(0x10, b""))
    observer._observe(_event(0x3E, b"\x02\x00"))
    observer._observe(_event(0x3E, b"\x02\x01" + b"\x00" * 8))
    observer._observe(_event(0x3E, b"\x0d\x01" + b"\x00" * 23))
    counts = observer.snapshot()["counts"]
    assert counts["legacy_advertising"] == 0  # type: ignore[index]
    assert counts["extended_advertising"] == 0  # type: ignore[index]
    assert counts["malformed"] == 7  # type: ignore[index]


def test_start_reads_effective_event_filter_and_stop_closes_socket() -> None:
    async def run() -> None:
        read_fd, write_fd = os.pipe()
        os.set_blocking(read_fd, False)
        sock = _Socket(read_fd, write_fd)
        sockets = [sock]
        binds: list[tuple[int, int, int, int]] = []
        observer = ScanDiagnostics(
            "hci3",
            clock=lambda: 8.0,
            socket_factory=lambda *_args: sockets.pop(0),
            native_bind=lambda *args: binds.append(args),
        )
        await observer.start()
        sock.feed(_event(0x3E, b"\x02" + _legacy_report()))
        await asyncio.sleep(0)
        await asyncio.sleep(0)

        assert binds == [(read_fd, 31, 3, 0)]
        assert observer.snapshot()["filter"]["le_meta_enabled"] is True  # type: ignore[index]
        assert observer.snapshot()["counts"]["legacy_advertising"] == 1  # type: ignore[index]
        assert len(sock.options) == 1
        assert sock.options[0][2] == struct.pack("<IIIH2x", 1 << 4, (1 << 14) | (1 << 15) | (1 << 16), 1 << 30, 0)

        await observer.stop()
        assert sock.closed
        assert observer.snapshot()["status"] == "stopped"
        assert not asyncio.get_running_loop().remove_reader(read_fd)

        next_read_fd, next_write_fd = os.pipe()
        os.set_blocking(next_read_fd, False)
        next_socket = _Socket(next_read_fd, next_write_fd)
        sockets.append(next_socket)
        await observer.start()
        assert observer.snapshot()["generation"] == 3
        assert observer.snapshot()["counts"]["legacy_advertising"] == 0  # type: ignore[index]
        assert observer.snapshot()["started_monotonic"] == 8.0
        await observer.stop()

    asyncio.run(run())


def test_kernel_stripped_le_meta_filter_is_reported() -> None:
    async def run() -> None:
        read_fd, write_fd = os.pipe()
        os.set_blocking(read_fd, False)
        sock = _Socket(read_fd, write_fd, le_meta_enabled=False)
        observer = ScanDiagnostics("hci0", socket_factory=lambda *_args: sock, native_bind=lambda *_args: None)
        await observer.start()
        assert observer.snapshot()["status"] == "available"
        assert observer.snapshot()["filter"] == {"available": True, "le_meta_enabled": False}
        await observer.stop()

    asyncio.run(run())


def test_missing_cap_net_raw_degrades_without_raising() -> None:
    async def run() -> None:
        observer = ScanDiagnostics(
            "hci0",
            socket_factory=lambda *_args: (_ for _ in ()).throw(PermissionError(errno.EPERM, "denied")),
        )
        await observer.start()
        assert observer.snapshot()["status"] == "degraded"
        assert observer.snapshot()["failure"] == "cap_net_raw_unavailable"
        await observer.stop()

    asyncio.run(run())
