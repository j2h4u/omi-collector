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
import subprocess
import tempfile
import time
from collections import deque
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Protocol, cast
from unittest.mock import patch

from omi_collector.capture.adapters.attempt_writer import AttemptWriter, WriterTarget
from omi_collector.capture.adapters.attempts import DurableSqliteSeal
from omi_collector.capture.adapters.collector_state_db import StateDB
from omi_collector.capture.adapters.internal_audio_store import (
    DEFAULT_CHUNK_LIMIT,
    CaptureRecord,
    InternalAudioStore,
    read_capture_source,
)
from omi_collector.capture.adapters.ready_bundles import ReadyOutcome, ReadyOutcomeState
from omi_collector.capture.adapters.staging_contract import AttemptDescriptor, DurablePrefix
from omi_collector.capture.adapters.staging_filesystem import StagingIO
from omi_collector.capture.adapters.staging_store import StagingStore
from omi_collector.capture.adapters.staging_writer import StagingWriter
from omi_collector.capture.domain.ring_protocol import RECORD_SIZE, DoneNotification, ReadBeginNotification
from omi_collector.capture.domain.transfer_arena import TransferArena
from omi_collector.config import DEFAULT_CONFIG, MAX_WRITER_CHUNK_RECORDS, CollectorConfig

TOTAL_RECORDS = 684_816
CHUNK_RECORDS = DEFAULT_CONFIG.writer.chunk_records
RATE_BYTES = 1_048_576
MIN_RESOURCE_BYTES = 3 * 1024**3
MAX_ARENA_BYTES = 512 * 1024**2
DB_RESERVE_FACTOR = 2
DB_FIXED_RESERVE = 512 * 1024**2
MAX_RECORDS = 2**32 - 1
EXPECTED_JOURNAL = "delete"
EXPECTED_SYNCHRONOUS = 3
EXPECTED_BUSY_TIMEOUT_MS = 5_000
BARRIER_TIMEOUT_SECONDS = 60.0


class Digest(Protocol):
    def update(self, data: bytes | bytearray | memoryview, /) -> None: ...

    def hexdigest(self) -> str: ...


@dataclass
class BenchConfig:
    records: int = TOTAL_RECORDS
    rate_bytes: int = RATE_BYTES
    max_seconds: float = 600.0
    publication: bool = False


@dataclass
class PublicationContext:
    staging: StagingStore
    state_db: StateDB
    root: Path
    args: BenchConfig
    expected: Digest
    config: CollectorConfig
    deadline: float


@dataclass
class BenchmarkContext:
    args: BenchConfig
    started: float
    deadline: float
    config: CollectorConfig
    arena_bytes: int
    memory_before: int
    free_before: int
    root: Path
    db: Path
    state_db: StateDB
    staging: StagingStore
    target: ObservedStagingWriter
    arena: TransferArena
    expected: Digest
    rss_before_arena_kb: int
    writer: AttemptWriter | None = None


@dataclass
class CaptureResult:
    elapsed_s: float
    barrier_ms: float
    q_peak: int
    queue_latency: list[float]
    seal_ms: float
    count: int
    digest: str
    pages: int
    page_size: int
    freelist: int
    db_bytes: int


class ObservedStagingWriter(StagingWriter):
    """Read the actual persistent connection settings on its owner thread."""

    actual_pragmas: tuple[str, int, int] | None = None

    def __init__(self, store: StagingStore, start_sequence: int, packet_count: int) -> None:
        super().__init__(store, start_sequence, packet_count)
        self.commit_ms_by_record_count: dict[int, list[float]] = {1: [], CHUNK_RECORDS: []}

    def append_chunk(self, offset: int, chunk: memoryview, tail: bytes = b"") -> None:
        record_count = chunk.nbytes // RECORD_SIZE
        started = time.perf_counter()
        try:
            super().append_chunk(offset, chunk, tail)
        finally:
            if record_count in self.commit_ms_by_record_count:
                self.commit_ms_by_record_count[record_count].append((time.perf_counter() - started) * 1000)

    def prepare(self) -> AttemptDescriptor:
        descriptor = super().prepare()
        assert self._attempt is not None
        connection = self._attempt._audio_store._connection
        assert connection is not None
        journal = cast(str, connection.execute("PRAGMA journal_mode").fetchone()[0]).lower()
        synchronous = cast(int, connection.execute("PRAGMA synchronous").fetchone()[0])
        busy_timeout = cast(int, connection.execute("PRAGMA busy_timeout").fetchone()[0])
        self.actual_pragmas = (journal, synchronous, busy_timeout)
        if self.actual_pragmas != (
            EXPECTED_JOURNAL,
            EXPECTED_SYNCHRONOUS,
            EXPECTED_BUSY_TIMEOUT_MS,
        ):
            raise RuntimeError(f"production writer PRAGMA mismatch: {self.actual_pragmas}")
        return descriptor


def make_chunk(start: int, count: int, *, publication: bool = False) -> bytearray:
    chunk = bytearray(count * RECORD_SIZE)
    for local in range(count):
        sequence = start + local
        offset = local * RECORD_SIZE
        struct.pack_into(">I", chunk, offset, sequence)
        if publication:
            chunk[offset + 4 : offset + RECORD_SIZE] = bytes((2, 8, sequence & 255)) + bytes(RECORD_SIZE - 7)
        else:
            chunk[offset + 4 : offset + RECORD_SIZE] = bytes(
                ((sequence + index) & 255) for index in range(RECORD_SIZE - 4)
            )
    return chunk


def available_memory() -> int:
    for line in Path("/proc/meminfo").read_text(encoding="ascii").splitlines():
        if line.startswith("MemAvailable:"):
            return int(line.split()[1]) * 1024
    raise RuntimeError("MemAvailable is unavailable")


def resident_kb() -> int:
    for line in Path("/proc/self/status").read_text(encoding="ascii").splitlines():
        if line.startswith("VmRSS:"):
            return int(line.split()[1])
    raise RuntimeError("process RSS is unavailable")


def parse_args() -> BenchConfig:
    parser = argparse.ArgumentParser(
        description="Synthetic OMIC-16 production SQLite writer and optional publication capacity harness"
    )
    parser.add_argument("--records", type=int)
    parser.add_argument("--rate-bytes", type=int)
    parser.add_argument("--max-seconds", type=float)
    parser.add_argument("--publication", action="store_true", help="include one ready publication and retirement step")
    args = parser.parse_args(namespace=BenchConfig())
    arena_bytes = args.records * RECORD_SIZE
    if args.records <= 0 or args.records > MAX_RECORDS or args.rate_bytes <= 0 or args.max_seconds <= 0:
        parser.error(f"records must be 1..{MAX_RECORDS}; rate-bytes and max-seconds must be positive")
    if arena_bytes > MAX_ARENA_BYTES:
        parser.error(f"records exceed the production 512 MiB arena: {arena_bytes} bytes")
    if CHUNK_RECORDS > MAX_WRITER_CHUNK_RECORDS:
        parser.error(f"production writer chunk exceeds the {MAX_WRITER_CHUNK_RECORDS}-record maximum: {CHUNK_RECORDS}")
    return args


def drain_observations(writer: AttemptWriter, outstanding: deque[tuple[int, float]], latency_ms: list[float]) -> None:
    written = writer.written_high_water
    now = time.perf_counter()
    while outstanding and outstanding[0][0] <= written:
        _, submitted_at = outstanding.popleft()
        latency_ms.append((now - submitted_at) * 1000)


def publish(
    writer: AttemptWriter,
    arena: TransferArena,
    outstanding: deque[tuple[int, float]],
    q_peak: int,
    tail: bytes = b"",
) -> int:
    arena.submit_prefix()
    high_water = arena.submitted_bytes
    writer.publish(high_water, tail)
    outstanding.append((high_water, time.perf_counter()))
    return max(q_peak, writer.submitted_high_water - writer.written_high_water)


async def pace_until(
    deadline: float,
    writer: AttemptWriter,
    outstanding: deque[tuple[int, float]],
    latency_ms: list[float],
) -> None:
    while True:
        drain_observations(writer, outstanding, latency_ms)
        delay = deadline - time.perf_counter()
        if delay <= 0:
            return
        await asyncio.sleep(min(delay, 0.01))


async def replay(
    writer: AttemptWriter,
    arena: TransferArena,
    args: BenchConfig,
    expected: Digest,
    deadline: float,
) -> tuple[float, float, int, list[float]]:
    started = time.perf_counter()
    outstanding: deque[tuple[int, float]] = deque()
    latency_ms: list[float] = []
    q_peak = 0
    await writer.start()
    await writer.prepare_leg(0, args.records)
    await writer.read_begin(ReadBeginNotification(0, args.records))

    # Exercise persisted partial-tail recovery, then complete the same record.
    first_count = min(CHUNK_RECORDS, args.records)
    first = make_chunk(0, first_count, publication=args.publication)
    expected.update(first)
    if first_count > 1:
        tail_end = RECORD_SIZE + 17
        arena.append(first[:tail_end])
        tail = bytes(first[RECORD_SIZE:tail_end])
        q_peak = publish(writer, arena, outstanding, q_peak, tail)
        await pace_until(started + arena.received_bytes / args.rate_bytes, writer, outstanding, latency_ms)
        arena.append(first[tail_end:])
        q_peak = publish(writer, arena, outstanding, q_peak)
        await pace_until(started + arena.submitted_bytes / args.rate_bytes, writer, outstanding, latency_ms)
    else:
        arena.append(first)
        q_peak = publish(writer, arena, outstanding, q_peak)
        await pace_until(started + arena.submitted_bytes / args.rate_bytes, writer, outstanding, latency_ms)

    for start in range(first_count, args.records, CHUNK_RECORDS):
        if time.monotonic() >= deadline:
            raise TimeoutError("synthetic replay deadline exceeded")
        count = min(CHUNK_RECORDS, args.records - start)
        chunk = make_chunk(start, count, publication=args.publication)
        expected.update(chunk)
        arena.append(chunk)
        q_peak = publish(writer, arena, outstanding, q_peak)
        await pace_until(started + arena.submitted_bytes / args.rate_bytes, writer, outstanding, latency_ms)

    barrier_started = time.perf_counter()
    ack = cast(DurablePrefix, await asyncio.wait_for(writer.barrier(), timeout=BARRIER_TIMEOUT_SECONDS))
    barrier_ms = (time.perf_counter() - barrier_started) * 1000
    drain_observations(writer, outstanding, latency_ms)
    if outstanding:
        raise RuntimeError("writer queue did not drain at the durable barrier")
    if ack.record_count != args.records or writer.written_high_water != arena.total_bytes:
        raise RuntimeError("final writer parity failed")
    return time.perf_counter() - started, barrier_ms, q_peak, latency_ms


def verify(
    db: Path,
    source_id: str,
    expected: Digest,
    records: int,
    deadline: float,
) -> tuple[int, str, int, int, int, int]:
    state_db = StateDB(db, read_only=True)
    source = read_capture_source(state_db, source_id)
    if source is None:
        raise RuntimeError("sealed production source is missing")
    digest = hashlib.sha256()
    count = 0
    with sqlite3.connect(db) as check:
        rows = check.execute(
            "SELECT device_timestamp, payload FROM capture_record WHERE source_id=? ORDER BY ordinal",
            (source_id,),
        )
        row = cast(tuple[int, bytes] | None, rows.fetchone())
        while row is not None:
            if count % CHUNK_RECORDS == 0 and time.monotonic() >= deadline:
                raise TimeoutError("durable source verification deadline exceeded")
            timestamp, payload = row
            digest.update(int(timestamp).to_bytes(4, "big") + bytes(payload))
            count += 1
            row = cast(tuple[int, bytes] | None, rows.fetchone())
        page_count = cast(int, check.execute("PRAGMA page_count").fetchone()[0])
        page_size = cast(int, check.execute("PRAGMA page_size").fetchone()[0])
        journal_mode = cast(str, check.execute("PRAGMA journal_mode").fetchone()[0]).lower()
        freelist_count = cast(int, check.execute("PRAGMA freelist_count").fetchone()[0])
    actual_digest = digest.hexdigest()
    actual = (
        count,
        source.committed_count,
        source.lifecycle,
        source.committed_sha256,
        actual_digest,
        journal_mode,
    )
    wanted = (records, records, "sealed_waiting", expected.hexdigest(), expected.hexdigest(), EXPECTED_JOURNAL)
    if actual != wanted:
        raise RuntimeError(
            f"durable seal or reconstructed parity failed: count={count}, lifecycle={source.lifecycle}, "
            f"digest={actual_digest}, journal={journal_mode}"
        )
    return count, actual_digest, page_count, page_size, freelist_count, db.stat().st_size


def measure_publication(publication_store: StagingStore) -> tuple[ReadyOutcome, dict[str, object]]:
    page_calls = 0
    rows_read = 0
    connection_opens = 0
    original_read_page = InternalAudioStore.read_record_page
    original_open_connection = StateDB.open_connection

    def counted_read_page(
        store: InternalAudioStore,
        source_id: str,
        after_ordinal: int = -1,
        limit: int = DEFAULT_CHUNK_LIMIT,
    ) -> tuple[CaptureRecord, ...]:
        nonlocal page_calls, rows_read
        records = original_read_page(store, source_id, after_ordinal, limit)
        page_calls += 1
        rows_read += len(records)
        return records

    def counted_open_connection(store: StateDB, *, read_only: bool = False) -> sqlite3.Connection:
        nonlocal connection_opens
        connection = original_open_connection(store, read_only=read_only)
        connection_opens += 1
        return connection

    with (
        patch.object(InternalAudioStore, "read_record_page", counted_read_page),
        patch.object(StateDB, "open_connection", counted_open_connection),
    ):
        started = time.perf_counter()
        outcome = publication_store.recover_and_publish()
        elapsed_s = time.perf_counter() - started
    return outcome, {
        "publication_elapsed_s": elapsed_s,
        "publication_page_calls": page_calls,
        "publication_rows_read": rows_read,
        "publication_connection_opens": connection_opens,
    }


def verify_ready_bundle(outcome: ReadyOutcome, context: PublicationContext) -> dict[str, object]:
    if outcome.state is not ReadyOutcomeState.PUBLISHED or len(outcome.published) != 1:
        raise RuntimeError(
            f"ready publication failed: state={outcome.state}, reason={outcome.reason}, "
            f"bundles={len(outcome.published)}"
        )

    (bundle,) = outcome.published
    manifest = cast(dict[str, object], json.loads((bundle.path / "manifest.json").read_text(encoding="utf-8")))
    records = context.args.records
    expected_digest = context.expected.hexdigest()
    actual = (
        bundle.next_sequence,
        bundle.record_count,
        bundle.records_sha256,
        manifest.get("start_sequence"),
        manifest.get("next_sequence"),
        manifest.get("record_count"),
        manifest.get("records_sha256"),
    )
    wanted = (records, records, expected_digest, 0, records, records, expected_digest)
    if actual != wanted:
        raise RuntimeError("ready bundle does not cover the full published record range")

    output = bundle.path / "records.bin"
    output_bytes = output.stat().st_size
    if output_bytes != context.args.records * RECORD_SIZE:
        raise RuntimeError(f"ready output size mismatch: {output_bytes} bytes")
    digest = hashlib.sha256()
    streamed_bytes = 0
    with output.open("rb") as stream:
        while chunk := stream.read(1024 * 1024):
            if time.monotonic() >= context.deadline:
                raise TimeoutError("ready output verification exceeded the benchmark deadline")
            digest.update(chunk)
            streamed_bytes += len(chunk)
    actual_digest = digest.hexdigest()
    if streamed_bytes != output_bytes or actual_digest != context.expected.hexdigest():
        raise RuntimeError("streamed ready output does not match the writer digest")
    return {
        "publication_output_bytes": output_bytes,
        "publication_sha256": actual_digest,
        "publication_bundle_count": len(outcome.published),
        "publication_retirement_pending": outcome.retirement_pending,
    }


def publish_ready(context: PublicationContext) -> dict[str, object]:
    ready_root = context.root / "ready"
    ready_root.mkdir()
    ready_root.chmod(0o2750)
    publication_store = StagingStore.from_paths(
        context.staging.paths,
        state_db=context.state_db,
        io=StagingIO(config=context.config),
        publication_root=ready_root,
    )
    publication_store.append_ready_closure(context.args.records, "drained")
    if time.monotonic() >= context.deadline:
        raise TimeoutError("publication deadline exceeded before recovery")
    outcome, metrics = measure_publication(publication_store)
    if time.monotonic() >= context.deadline:
        raise TimeoutError("publication exceeded the benchmark deadline")
    metrics.update(verify_ready_bundle(outcome, context))
    with sqlite3.connect(context.state_db.path) as check:
        metrics["publication_page_count"] = cast(int, check.execute("PRAGMA page_count").fetchone()[0])
        metrics["publication_page_size"] = cast(int, check.execute("PRAGMA page_size").fetchone()[0])
        metrics["publication_db_bytes"] = context.state_db.path.stat().st_size
    if time.monotonic() >= context.deadline:
        raise TimeoutError("publication metrics exceeded the benchmark deadline")
    return metrics


def resource_preflight(args: BenchConfig) -> tuple[int, int, int]:
    arena_bytes = args.records * RECORD_SIZE
    db_reserve = arena_bytes * DB_RESERVE_FACTOR + DB_FIXED_RESERVE
    required_memory = MIN_RESOURCE_BYTES + arena_bytes
    publication_reserve = arena_bytes if args.publication else 0
    required_space = MIN_RESOURCE_BYTES + arena_bytes + db_reserve + publication_reserve
    memory_before = available_memory()
    free_before = shutil.disk_usage("/var/tmp").free
    if memory_before < required_memory:
        raise RuntimeError(f"MemAvailable preflight failed: {memory_before} < required {required_memory}")
    if free_before < required_space:
        raise RuntimeError(f"free-space preflight failed: {free_before} < required {required_space}")
    return arena_bytes, memory_before, free_before


def make_context(args: BenchConfig, started: float, deadline: float, config: CollectorConfig) -> BenchmarkContext:
    arena_bytes, memory_before, free_before = resource_preflight(args)
    root = Path(tempfile.mkdtemp(prefix="omic16-replay-", dir="/var/tmp"))
    try:
        if root.stat().st_dev != Path("/var/tmp").stat().st_dev:
            raise RuntimeError("fixture is not on the same filesystem as /var/tmp")
        db = root / "spool" / "collector-state.sqlite3"
        state_db = StateDB(db)
        staging = StagingStore(root / "spool", root / "captures", state_db=state_db, io=StagingIO(config=config))
        target = ObservedStagingWriter(staging, 0, args.records)
        arena = TransferArena(0, args.records, max_bytes=MAX_ARENA_BYTES)
        return BenchmarkContext(
            args,
            started,
            deadline,
            config,
            arena_bytes,
            memory_before,
            free_before,
            root,
            db,
            state_db,
            staging,
            target,
            arena,
            hashlib.sha256(),
            resident_kb(),
        )
    except BaseException:
        shutil.rmtree(root)
        raise


async def run_writer(context: BenchmarkContext) -> CaptureResult:
    context.writer = AttemptWriter(
        cast(WriterTarget, context.target),
        context.arena.readonly_source(),
        config=context.config.writer,
    )
    async with asyncio.timeout(max(0.001, context.deadline - time.monotonic())):
        elapsed, barrier_ms, q_peak, queue_latency = await replay(
            context.writer, context.arena, context.args, context.expected, context.deadline
        )
        seal_started = time.perf_counter()
        seal_result = await context.writer.seal(DoneNotification(0, context.args.records))
        retained_seal = await context.writer.await_seal_result()
        if not isinstance(seal_result, DurableSqliteSeal) or retained_seal != seal_result:
            raise RuntimeError("writer did not retain the durable seal result")
        seal_ms = (time.perf_counter() - seal_started) * 1000
    await context.writer.close()
    count, digest, pages, page_size, freelist, db_bytes = verify(
        context.db,
        seal_result.source_id,
        context.expected,
        context.args.records,
        context.deadline,
    )
    return CaptureResult(
        elapsed, barrier_ms, q_peak, queue_latency, seal_ms, count, digest, pages, page_size, freelist, db_bytes
    )


def publication_metrics(context: BenchmarkContext) -> dict[str, object]:
    empty_metrics: dict[str, object] = {
        "publication_enabled": context.args.publication,
        "publication_elapsed_s": None,
        "publication_page_calls": None,
        "publication_rows_read": None,
        "publication_connection_opens": None,
        "publication_output_bytes": None,
        "publication_sha256": None,
        "publication_bundle_count": None,
        "publication_retirement_pending": None,
    }
    if context.args.publication:
        empty_metrics.update(
            publish_ready(
                PublicationContext(
                    context.staging,
                    context.state_db,
                    context.root,
                    context.args,
                    context.expected,
                    context.config,
                    context.deadline,
                )
            )
        )
    return empty_metrics


def benchmark_metrics(context: BenchmarkContext, capture: CaptureResult) -> dict[str, object]:
    queue_latency = capture.queue_latency
    full_k_commits = sorted(context.target.commit_ms_by_record_count[CHUNK_RECORDS])
    one_record_commits = sorted(context.target.commit_ms_by_record_count[1])
    return {
        "root": str(context.root),
        "records": capture.count,
        "bytes": context.arena_bytes,
        "total_elapsed_s": capture.elapsed_s,
        "seal_ms": capture.seal_ms,
        "queue_latency_samples": len(queue_latency),
        "queue_latency_p50_ms": statistics.median(queue_latency),
        "queue_latency_p95_ms": sorted(queue_latency)[max(0, int(len(queue_latency) * 0.95) - 1)],
        "queue_latency_max_ms": max(queue_latency),
        "full_K_commit_samples": len(full_k_commits),
        "full_K_commit_p95_ms": (
            full_k_commits[max(0, int(len(full_k_commits) * 0.95) - 1)] if full_k_commits else None
        ),
        "full_K_commit_max_ms": max(full_k_commits) if full_k_commits else None,
        "one_record_commit_samples": len(one_record_commits),
        "one_record_commit_p95_ms": (
            one_record_commits[max(0, int(len(one_record_commits) * 0.95) - 1)] if one_record_commits else None
        ),
        "one_record_commit_max_ms": max(one_record_commits) if one_record_commits else None,
        "barrier_ms": capture.barrier_ms,
        "q_peak_bytes": capture.q_peak,
        "q_peak_records": capture.q_peak // RECORD_SIZE,
        "db_bytes": capture.db_bytes,
        "page_count": capture.pages,
        "page_size": capture.page_size,
        "freelist_count": capture.freelist,
        "free_before": context.free_before,
        "free_after": shutil.disk_usage("/var/tmp").free,
        "memory_before": context.memory_before,
        "memory_after": available_memory(),
        "rss_before_arena_kb": context.rss_before_arena_kb,
        "rss_after_kb": resident_kb(),
        "rss_max_kb": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
        "run_elapsed_s": time.perf_counter() - context.started,
        "loadavg": os.getloadavg(),
        "sha256": capture.digest,
        "sealed": True,
        "actual_writer_pragmas": context.target.actual_pragmas,
        "rate_label": "synthetic producer pacing; not measured BLE peak ingress",
    }


async def cleanup(context: BenchmarkContext) -> None:
    retained = False
    if context.writer is not None and context.writer.thread.is_alive():
        try:
            await context.writer.close()
        except Exception as error:  # noqa: BLE001 - preserve fixture if writer cannot safely close
            if context.writer.thread.is_alive():
                retained = True
                print(json.dumps({"retained": str(context.root), "close_error": str(error)}))
    if not retained:
        shutil.rmtree(context.root)
        print(json.dumps({"cleaned": str(context.root), "exists_after": context.root.exists()}))


async def run(args: BenchConfig) -> None:
    started = time.perf_counter()
    deadline = time.monotonic() + args.max_seconds
    config = (
        replace(DEFAULT_CONFIG, ready=replace(DEFAULT_CONFIG.ready, target_audio_seconds=args.records * 0.02))
        if args.publication
        else DEFAULT_CONFIG
    )
    context = make_context(args, started, deadline, config)
    try:
        capture = await run_writer(context)
        metrics = benchmark_metrics(context, capture)
        metrics.update(publication_metrics(context))
        print(json.dumps(metrics, sort_keys=True))
    finally:
        await cleanup(context)


def main() -> None:
    os.nice(19)
    ionice = shutil.which("ionice")
    if ionice is None:
        raise RuntimeError("ionice is required to run the capacity replay at idle I/O priority")
    subprocess.run((ionice, "-c", "3", "-p", str(os.getpid())), check=True, capture_output=True)
    args = parse_args()
    asyncio.run(run(args))


if __name__ == "__main__":
    main()
