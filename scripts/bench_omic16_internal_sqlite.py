#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import resource
import shutil
import sqlite3
import statistics
import struct
import tempfile
import threading
import time
from dataclasses import dataclass
from pathlib import Path

from omi_collector.capture.adapters.attempt_writer import AttemptWriter
from omi_collector.capture.domain.ring_protocol import RECORD_SIZE, ReadBeginNotification
from omi_collector.capture.domain.transfer_arena import TransferArena

TOTAL_RECORDS = 684_816
CHUNK_RECORDS = 128
RATE_BYTES = 1_048_576
MIN_RESOURCE_BYTES = 3 * 1024**3
MAX_RECORDS = 2**32 - 1
SYNCHRONOUS_EXTRA = 3
BUSY_TIMEOUT_MS = 5_000


@dataclass(frozen=True)
class Ack:
    next_sequence: int
    record_count: int


class SQLiteTarget:
    def __init__(self, db: Path, total: int) -> None:
        self.db = db
        self.total = total
        self.conn: sqlite3.Connection | None = None
        self.owner: int | None = None
        self.count = 0
        self.digest = hashlib.sha256()
        self.tx_ms: list[float] = []
        self.journal_peak = 0

    def _assert(self) -> sqlite3.Connection:
        if self.owner != threading.get_ident() or self.conn is None:
            raise RuntimeError("SQLite target thread affinity violated")
        journal = str(self.conn.execute("PRAGMA journal_mode").fetchone()[0]).lower()
        sync = int(self.conn.execute("PRAGMA synchronous").fetchone()[0])
        busy = int(self.conn.execute("PRAGMA busy_timeout").fetchone()[0])
        if journal != "delete" or sync != SYNCHRONOUS_EXTRA or busy != BUSY_TIMEOUT_MS:
            raise RuntimeError(f"pragma mismatch journal={journal} sync={sync} busy={busy}")
        return self.conn

    def prepare(self) -> object:
        self.owner = threading.get_ident()
        self.conn = sqlite3.connect(self.db, isolation_level=None)
        self.conn.execute("PRAGMA journal_mode=DELETE")
        self.conn.execute("PRAGMA synchronous=EXTRA")
        self.conn.execute("PRAGMA busy_timeout=5000")
        c = self._assert()
        c.executescript(
            """
            CREATE TABLE capture_source (
                source_id TEXT PRIMARY KEY,
                committed_count INTEGER NOT NULL DEFAULT 0,
                committed_sha256 TEXT NOT NULL DEFAULT ''
            ) WITHOUT ROWID;
            CREATE TABLE capture_record (
                source_id TEXT NOT NULL,
                ordinal INTEGER NOT NULL,
                device_timestamp INTEGER NOT NULL CHECK(device_timestamp BETWEEN 0 AND 4294967295),
                payload BLOB NOT NULL CHECK(length(payload)=440),
                utc_timestamp INTEGER,
                mapping_revision BLOB,
                PRIMARY KEY(source_id, ordinal),
                FOREIGN KEY(source_id) REFERENCES capture_source(source_id)
            ) WITHOUT ROWID;
            CREATE INDEX capture_record_device_timestamp ON capture_record(device_timestamp);
            CREATE INDEX capture_record_utc_timestamp ON capture_record(utc_timestamp);
            """
        )
        c.execute("INSERT INTO capture_source(source_id) VALUES ('synthetic-1')")
        return None

    def prepare_leg(self, start_sequence: int, record_count: int) -> object:
        if start_sequence != 0 or record_count != self.total:
            raise RuntimeError("unexpected synthetic leg")
        return Ack(0, 0)

    def read_begin(self, _notice: object) -> object:
        self._assert()
        return None

    def append_chunk(self, offset: int, chunk: memoryview) -> object:
        c = self._assert()
        if offset != self.count * RECORD_SIZE or chunk.nbytes % RECORD_SIZE or chunk.nbytes > CHUNK_RECORDS * RECORD_SIZE:
            raise RuntimeError("invalid writer drain geometry")
        started = time.perf_counter()
        c.execute("BEGIN IMMEDIATE")
        try:
            for local in range(chunk.nbytes // RECORD_SIZE):
                record = chunk[local * RECORD_SIZE : (local + 1) * RECORD_SIZE]
                ts = int.from_bytes(record[:4], "big")
                payload = bytes(record[4:])
                c.execute(
                    "INSERT INTO capture_record(source_id,ordinal,device_timestamp,payload,utc_timestamp,mapping_revision) VALUES (?,?,?,?,NULL,NULL)",
                    ("synthetic-1", self.count, ts, payload),
                )
                self.digest.update(record)
                self.count += 1
            c.execute(
                "UPDATE capture_source SET committed_count=?, committed_sha256=? WHERE source_id='synthetic-1'",
                (self.count, self.digest.hexdigest()),
            )
            journal = self.db.with_name(self.db.name + "-journal")
            if journal.exists():
                self.journal_peak = max(self.journal_peak, journal.stat().st_size)
            c.commit()
        except BaseException:
            c.rollback()
            raise
        self.tx_ms.append((time.perf_counter() - started) * 1000)
        return None

    def checkpoint(self) -> Ack:
        self._assert()
        return Ack(self.count, self.count)

    def seal(self, _notice: object) -> object:
        return self.checkpoint()

    def publish_prefix(self) -> object:
        return self.checkpoint()

    def close(self) -> object:
        c = self._assert()
        c.close()
        self.conn = None
        return None


def make_chunk(start: int, count: int) -> bytearray:
    chunk = bytearray(count * RECORD_SIZE)
    for i in range(count):
        pos = i * RECORD_SIZE
        struct.pack_into(">I", chunk, pos, start + i)
        chunk[pos + 4 : pos + RECORD_SIZE] = bytes(((start + i + j) & 255) for j in range(RECORD_SIZE - 4))
    return chunk


def available_memory() -> int:
    for line in Path("/proc/meminfo").read_text(encoding="ascii").splitlines():
        if line.startswith("MemAvailable:"):
            return int(line.split()[1]) * 1024
    raise RuntimeError("MemAvailable is unavailable")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Synthetic OMIC-16 internal SQLite writer benchmark")
    parser.add_argument("--records", type=int, default=TOTAL_RECORDS)
    parser.add_argument("--rate-bytes", type=int, default=RATE_BYTES)
    parser.add_argument("--max-seconds", type=float, default=600.0)
    args = parser.parse_args()
    if args.records <= 0 or args.records > MAX_RECORDS or args.rate_bytes <= 0 or args.max_seconds <= 0:
        parser.error(f"records must be 1..{MAX_RECORDS}; rate-bytes and max-seconds must be positive")
    return args


async def replay(
    writer: AttemptWriter,
    arena: TransferArena,
    args: argparse.Namespace,
    expected: object,
) -> tuple[float, float, int]:
    started = time.perf_counter()
    q_peak = 0
    await writer.start()
    await writer.prepare_leg(0, args.records)
    await writer.read_begin(ReadBeginNotification(0, args.records))
    for start in range(0, args.records, CHUNK_RECORDS):
        if time.perf_counter() - started > args.max_seconds:
            raise TimeoutError("synthetic replay deadline exceeded")
        count = min(CHUNK_RECORDS, args.records - start)
        chunk = make_chunk(start, count)
        expected.update(chunk)
        arena.append(chunk)
        arena.submit_prefix(start + count)
        writer.publish(arena.submitted_bytes)
        q_peak = max(q_peak, writer.submitted_high_water - writer.written_high_water)
        delay = arena.submitted_bytes / args.rate_bytes - (time.perf_counter() - started)
        if delay > 0:
            await asyncio.sleep(delay)
    barrier_started = time.perf_counter()
    ack = await writer.checkpoint()
    barrier_ms = (time.perf_counter() - barrier_started) * 1000
    if ack.record_count != args.records or writer.written_high_water != arena.total_bytes:
        raise RuntimeError("final writer parity failed")
    return time.perf_counter() - started, barrier_ms, q_peak


def verify(db: Path, expected: object, records: int) -> tuple[int, int, int]:
    check = sqlite3.connect(db)
    try:
        row = check.execute("SELECT committed_count, committed_sha256 FROM capture_source").fetchone()
        rows = check.execute("SELECT count(*) FROM capture_record").fetchone()[0]
        pages = check.execute("PRAGMA page_count").fetchone()[0]
        freelist = check.execute("PRAGMA freelist_count").fetchone()[0]
    finally:
        check.close()
    if row != (records, expected.hexdigest()) or rows != records:
        raise RuntimeError(f"database parity failed row={row} rows={rows}")
    return pages, freelist, rows


async def run(args: argparse.Namespace) -> None:
    memory_before = available_memory()
    free_before = shutil.disk_usage("/var/tmp").free
    arena_bytes = args.records * RECORD_SIZE
    required_bytes = arena_bytes + MIN_RESOURCE_BYTES
    if memory_before < required_bytes:
        raise RuntimeError(f"MemAvailable preflight failed: {memory_before} < required {required_bytes}")
    if free_before < required_bytes:
        raise RuntimeError(f"free-space preflight failed: {free_before} < required {required_bytes}")
    root = Path(tempfile.mkdtemp(prefix="omic16-replay-", dir="/var/tmp"))
    writer: AttemptWriter | None = None
    try:
        if root.stat().st_dev != Path("/var/tmp").stat().st_dev:
            raise RuntimeError("fixture is not on the same filesystem as /var/tmp")
        db = root / "collector-state.sqlite3"
        target = SQLiteTarget(db, args.records)
        arena = TransferArena(0, args.records, max_bytes=arena_bytes)
        expected = hashlib.sha256()
        writer = AttemptWriter(target, arena.readonly_source())
        async with asyncio.timeout(args.max_seconds):
            elapsed_s, barrier_ms, q_peak = await replay(writer, arena, args, expected)
        await writer.close(timeout=30)
        pages, freelist, _rows = verify(db, expected, args.records)
        check = sqlite3.connect(db)
        try:
            page_size = check.execute("PRAGMA page_size").fetchone()[0]
        finally:
            check.close()
        print(json.dumps({
            "root": str(root), "records": args.records, "bytes": arena.total_bytes,
            "total_elapsed_s": elapsed_s,
            "tx_samples": len(target.tx_ms), "tx_p50_ms": statistics.median(target.tx_ms),
            "tx_p95_ms": sorted(target.tx_ms)[max(0, int(len(target.tx_ms) * .95) - 1)],
            "tx_max_ms": max(target.tx_ms), "barrier_ms": barrier_ms,
            "q_peak_bytes": q_peak, "q_peak_records": q_peak // RECORD_SIZE,
            "db_bytes": db.stat().st_size, "page_count": pages, "page_size": page_size,
            "freelist_count": freelist, "journal_peak_bytes": target.journal_peak,
            "free_before": free_before, "free_after": shutil.disk_usage("/var/tmp").free,
            "memory_before": memory_before, "memory_after": available_memory(),
            "rss_max_kb": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
            "loadavg": os.getloadavg(), "parity": True,
            "rate_label": "synthetic producer pacing; not BLE capacity",
            "index_label": "provisional device_timestamp and utc_timestamp lookup indexes",
        }, sort_keys=True))
    finally:
        retained = False
        if writer is not None and writer.thread.is_alive():
            try:
                await writer.close(timeout=30)
            except Exception as error:  # noqa: BLE001 - preserve active fixture on close timeout
                if writer.thread.is_alive():
                    retained = True
                    print(json.dumps({"retained": str(root), "close_error": str(error)}))
        if not retained:
            shutil.rmtree(root)
            print(json.dumps({"cleaned": str(root), "exists_after": root.exists()}))


def main() -> None:
    args = parse_args()
    asyncio.run(run(args))


if __name__ == "__main__":
    main()
