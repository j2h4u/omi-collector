"""Real store and async driver checks for publication admission and raw I/O."""

from __future__ import annotations

import asyncio
from collections.abc import Iterator
from contextlib import contextmanager
from hashlib import sha256
from json import dumps
from pathlib import Path
from struct import pack
from threading import Event
from types import TracebackType
from typing import IO

import pytest

from omi_collector.capture.adapters.bundle_contract import BundleManifest, SealedReceipt
from omi_collector.capture.adapters.opportunistic_runtime import OpportunisticRuntime
from omi_collector.capture.adapters.ready_bundles import ReadyBundleError, ReadyOutcome, ReadyOutcomeState
from omi_collector.capture.adapters.staging_contract import DeviceAlreadyRunningError
from omi_collector.capture.adapters.staging_filesystem import DeviceLock
from omi_collector.capture.adapters.staging_store import StagingStore
from omi_collector.capture.application.quarantine_maintenance import QuarantineMaintenance
from omi_collector.capture.domain.ring_protocol import RECORD_SIZE
from omi_collector.config import CollectorConfig, ReadyConfig, RetryConfig


def _store(tmp_path: Path, *, threshold: float = 0.02, backoff: float = 0.05) -> StagingStore:
    draft = tmp_path / "draft"
    draft.mkdir()
    bootstrap = StagingStore(tmp_path / "spool", draft)
    ready = tmp_path / "ready"
    ready.mkdir(mode=0o2750)
    ready.chmod(0o2750)
    config = CollectorConfig(
        ready=ReadyConfig(target_audio_seconds=threshold), retry=RetryConfig(rapid_backoff=(backoff,))
    )
    return StagingStore.from_paths(bootstrap.paths, publication_root=ready, config=config)


def _record(sequence: int) -> bytes:
    return pack(">I", sequence) + bytes((2, 8, sequence % 256)) + bytes(RECORD_SIZE - 7)


def _draft(store: StagingStore, name: str, start: int, count: int = 1) -> Path:
    raw = b"".join(_record(sequence) for sequence in range(start, start + count))
    digest = sha256(raw).hexdigest()
    path = store.capture_root / name
    path.mkdir()
    (path / "records.bin").write_bytes(raw)
    manifest = BundleManifest(2, start, start + count, count, RECORD_SIZE, digest)
    (path / "manifest.json").write_text(dumps(manifest.as_dict()), encoding="utf-8")
    (path / "receipt.json").write_text(dumps(SealedReceipt("a" * 32, digest).as_dict()), encoding="utf-8")
    return path


class _CountedReader:
    def __init__(self, stream: IO[bytes], counts: list[int]) -> None:
        self._stream = stream
        self._counts = counts

    def __enter__(self) -> _CountedReader:
        return self

    def __exit__(
        self, kind: type[BaseException] | None, error: BaseException | None, traceback: TracebackType | None
    ) -> bool | None:
        return self._stream.__exit__(kind, error, traceback)

    def seek(self, offset: int, whence: int = 0) -> int:
        return self._stream.seek(offset, whence)

    def read(self, size: int = -1) -> bytes:
        block = self._stream.read(size)
        self._counts[1] += len(block)
        return block


def _count_raw_reads(monkeypatch: pytest.MonkeyPatch, watch: Path | None = None) -> list[int]:
    counts = [0, 0]
    original = Path.open

    def counted(
        path: Path,
        mode: str = "r",
        *,
        encoding: str | None = None,
        errors: str | None = None,
        newline: str | None = None,
    ) -> object:
        stream = original(path, mode, encoding=encoding, errors=errors, newline=newline)
        if path.name != "records.bin" or "r" not in mode or (watch is not None and path != watch):
            return stream
        counts[0] += 1
        return _CountedReader(stream, counts)

    monkeypatch.setattr(Path, "open", counted)
    return counts


def test_conflicting_overlap_stays_flat_across_real_driver_wakes_and_new_input(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = _store(tmp_path)
    _draft(store, "first", 100)
    conflict = _draft(store, "conflict", 100)
    raw = bytearray((conflict / "records.bin").read_bytes())
    raw[-1] ^= 1
    changed = bytes(raw)
    digest = sha256(changed).hexdigest()
    (conflict / "records.bin").write_bytes(changed)
    (conflict / "manifest.json").write_text(
        dumps(BundleManifest(2, 100, 101, 1, RECORD_SIZE, digest).as_dict()), encoding="utf-8"
    )
    (conflict / "receipt.json").write_text(dumps(SealedReceipt("a" * 32, digest).as_dict()), encoding="utf-8")
    store.append_ready_closure(101, "drained")
    counts = _count_raw_reads(monkeypatch)
    maintenance = QuarantineMaintenance(store, None, OpportunisticRuntime())

    async def scenario() -> None:
        await maintenance.ensure_publication_ready()
        first = tuple(counts)
        assert first[0] > 0 and first[1] > 0
        for _ in range(3):
            assert "conflicting record bytes" in (store.recover_and_publish().reason or "")
            assert "conflicting record bytes" in (store.publish_ready().reason or "")
            await maintenance.ensure_publication_ready()
            store.inspect_recovery()
        assert tuple(counts) == first
        repaired = _record(100)
        repaired_digest = sha256(repaired).hexdigest()
        (conflict / "records.bin").write_bytes(repaired)
        (conflict / "manifest.json").write_text(
            dumps(BundleManifest(2, 100, 101, 1, RECORD_SIZE, repaired_digest).as_dict()), encoding="utf-8"
        )
        (conflict / "receipt.json").write_text(
            dumps(SealedReceipt("a" * 32, repaired_digest).as_dict()), encoding="utf-8"
        )
        assert store.recover_and_publish().state is ReadyOutcomeState.PUBLISHED
        assert counts[1] > first[1]
        after_change = tuple(counts)
        await maintenance.ensure_publication_ready()
        assert tuple(counts) == after_change
        await maintenance.close()

    asyncio.run(scenario())


def test_corrupt_raw_verdict_is_reused_until_same_path_is_repaired(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = _store(tmp_path)
    corrupt = _draft(store, "only", 100)
    raw = bytearray((corrupt / "records.bin").read_bytes())
    raw[-1] ^= 1
    (corrupt / "records.bin").write_bytes(raw)
    store.append_ready_closure(101, "drained")
    counts = _count_raw_reads(monkeypatch)

    blocked = store.recover_and_publish()
    assert blocked.state is ReadyOutcomeState.BLOCKED
    assert blocked.reason == "draft bundle records do not match its receipt"
    initial = tuple(counts)
    assert initial[0] > 0 and initial[1] > 0
    for _ in range(3):
        assert store.recover_and_publish().reason == blocked.reason
        with pytest.raises(ReadyBundleError, match=blocked.reason):
            store.inspect_recovery()
        with pytest.raises(ReadyBundleError, match=blocked.reason):
            store.close_orphaned_drafts("restart_interrupted")
    assert tuple(counts) == initial

    replacement = _draft(store, "replacement", 100)
    for name in ("records.bin", "manifest.json", "receipt.json"):
        (replacement / name).replace(corrupt / name)
    replacement.rmdir()
    published = store.recover_and_publish()
    assert published.state is ReadyOutcomeState.PUBLISHED
    assert len(published.published) == 1
    assert counts[1] > initial[1]
    after_repair = tuple(counts)
    assert store.recover_and_publish().state is ReadyOutcomeState.WAITING
    assert tuple(counts) == after_repair


def test_success_reports_once_and_repeated_wakes_do_not_reopen_audio(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = _store(tmp_path)
    _draft(store, "only", 100)
    store.append_ready_closure(101, "drained")
    counts = _count_raw_reads(monkeypatch)

    first = store.recover_and_publish()
    assert first.state is ReadyOutcomeState.PUBLISHED
    assert len(first.published) == 1
    assert first.published[0].path.is_dir()
    initial = tuple(counts)
    assert initial[1] > 0
    for _ in range(3):
        assert store.recover_and_publish().state is ReadyOutcomeState.WAITING
    assert tuple(counts) == initial


def test_partial_replay_does_not_reopen_unchanged_raw_after_unrelated_ack(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = _store(tmp_path, threshold=0.04)
    _draft(store, "older", 90, 2)
    store.append_ready_closure(92, "drained")
    older = store.recover_and_publish().published[0]
    _draft(store, "prefix", 100, 2)
    store.append_ready_closure(102, "drained")
    assert store.recover_and_publish().state is ReadyOutcomeState.PUBLISHED
    replay = _draft(store, "replay", 100, 3)
    store.append_ready_closure(103, "drained")
    counts = _count_raw_reads(monkeypatch, replay / "records.bin")

    assert store.recover_and_publish().state is ReadyOutcomeState.WAITING
    first = tuple(counts)
    assert first[0] > 0 and first[1] > 0
    for _ in range(3):
        assert store.recover_and_publish().state is ReadyOutcomeState.WAITING
    store.append_ready_closure(103, "drained")
    assert store.recover_and_publish().state is ReadyOutcomeState.WAITING
    checkpoint = tmp_path / "work" / "omi-ready-checkpoint.json"
    checkpoint.parent.mkdir()
    checkpoint.write_text(
        dumps(
            {
                "analysis_cursor": None,
                "vad_decisions": [],
                "open_speech_tail": None,
                "acknowledged": [{"bundle_id": older.bundle_id, "records_sha256": older.records_sha256}],
            }
        ),
        encoding="utf-8",
    )
    assert store.recover_and_publish().state is ReadyOutcomeState.WAITING
    assert tuple(counts) == first

    _draft(store, "suffix", 103)
    store.append_ready_closure(104, "drained")
    published = store.recover_and_publish()
    assert published.state is ReadyOutcomeState.PUBLISHED
    assert published.published[0].record_count == 2
    assert counts[1] > first[1]


def test_transient_deadline_is_one_timer_and_no_immediate_retry(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = _store(tmp_path, threshold=0.04, backoff=0.08)
    _draft(store, "only", 100)
    store.append_ready_closure(101, "drained")
    original_lock = store.device_lock
    attempts = 0

    @contextmanager
    def busy_once(*, recover_capture_temporaries: bool = True, operation: str = "unknown") -> Iterator[DeviceLock]:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise DeviceAlreadyRunningError
        with original_lock(recover_capture_temporaries=recover_capture_temporaries, operation=operation) as lease:
            yield lease

    monkeypatch.setattr(store, "device_lock", busy_once)
    first = store.recover_and_publish()
    assert first.state is ReadyOutcomeState.TRANSIENT
    scheduled = store.publication_retry_schedule()
    assert scheduled is not None
    for _ in range(10):
        assert store.recover_and_publish().state is ReadyOutcomeState.TRANSIENT
    assert attempts == 1
    assert store.publication_retry_schedule() == scheduled

    maintenance = QuarantineMaintenance(store, None, OpportunisticRuntime())

    async def scenario() -> None:
        await maintenance.ensure_publication_ready()
        assert attempts == 1
        await asyncio.sleep(0.12)
        assert attempts == 2
        assert not tuple((tmp_path / "ready").iterdir())
        await maintenance.close()

    asyncio.run(scenario())


def test_capture_begin_fences_foreground_publication_before_durable_visit(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = _store(tmp_path)
    _draft(store, "only", 100)
    store.append_ready_closure(101, "drained")
    maintenance = QuarantineMaintenance(store, None, OpportunisticRuntime())
    entered = Event()
    release = Event()

    def slow_effect() -> ReadyOutcome:
        entered.set()
        assert release.wait(2)
        return ReadyOutcome(ReadyOutcomeState.WAITING, reason="test_effect")

    monkeypatch.setattr(store, "_recover_and_publish_unlocked", slow_effect)

    async def scenario() -> None:
        publication = asyncio.create_task(maintenance.ensure_publication_ready())
        assert await asyncio.to_thread(entered.wait, 2)
        capture = asyncio.create_task(maintenance.enter_capture_priority())
        await asyncio.sleep(0)
        assert store.recover_and_publish().reason == "capture_active"
        assert not capture.done()
        release.set()
        await capture
        assert store.recover_and_publish().reason == "capture_active"
        maintenance.exit_capture_priority()
        await maintenance.close()
        await asyncio.gather(publication, return_exceptions=True)

    try:
        asyncio.run(scenario())
    finally:
        release.set()
