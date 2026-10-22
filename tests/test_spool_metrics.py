from __future__ import annotations

import errno
import hashlib
import json
import os
import subprocess
import sys
import threading
import time
from collections.abc import Callable, Iterator
from itertools import pairwise
from multiprocessing import get_context
from multiprocessing.connection import Connection
from pathlib import Path
from shutil import rmtree
from typing import cast

import pytest

from omi_collector.capture.adapters import ready_bundles
from omi_collector.capture.adapters.clock_state_store import ClockStateStore
from omi_collector.capture.adapters.collector_state_db import StateDB, StateKind
from omi_collector.capture.adapters.firmware_observations import FirmwareObservationStore
from omi_collector.capture.adapters.internal_audio_store import (
    AUDIO_RECORD_SIZE,
    DrainCheckpoint,
    InternalAudioStore,
)
from omi_collector.capture.adapters.quality_metrics import SQLiteQualityMetrics
from omi_collector.capture.adapters.ready_bundles import (
    PublicAudioV2Options,
    SqlitePublicationSource,
    activate_public_audio_v2,
    begin_publication_cutover,
    finalize_public_audio_v2,
)
from omi_collector.capture.application.quality_metrics import SequenceLossMetric
from omi_collector.capture.domain.clock_state import ClockRange, ClockState
from omi_collector.capture.domain.ring_protocol import RingInfo
from omi_collector.config import ReadyConfig
from omi_collector.spool_metrics import SpoolMetricsError, collect_spool_metrics

RECORD_SIZE = 444


_CAPTURE_ROOTS: set[Path] = set()


def _capture_root(tmp_path: Path) -> Path:
    root = tmp_path.parent / f"{tmp_path.name}-captures"
    if tmp_path not in _CAPTURE_ROOTS:
        rmtree(root, ignore_errors=True)
        _CAPTURE_ROOTS.add(tmp_path)
    return root


def _state_db_path(root: Path) -> Path:
    return root.parent / f"{root.name}-collector-state.sqlite3"


def _state_db(root: Path, *, read_only: bool = False) -> StateDB:
    return StateDB(_state_db_path(root), read_only=read_only)


def _v2_record(timestamp: int, *, audio: bool = True) -> bytes:
    payload = bytes((2, 8, 1)) + bytes(AUDIO_RECORD_SIZE - 7) if audio else bytes(AUDIO_RECORD_SIZE - 4)
    return timestamp.to_bytes(4, "big") + payload


def _v2_source(
    store: InternalAudioStore, source_id: str, start: int, records: tuple[bytes, ...]
) -> SqlitePublicationSource:
    store.create_source(source_id, "attempt", start, start + len(records))
    source = store.prepare_leg(source_id, start, len(records))
    source = store.record_read_begin(source_id, start, len(records))
    store.commit_drain(source_id, 0, records, DrainCheckpoint(source.current_leg_generation))
    source = store.get_source(source_id)
    assert source is not None
    store.seal_source(
        source_id,
        leg_generation=source.current_leg_generation,
        expected_count=source.committed_count,
        expected_sha256=source.committed_sha256,
    )
    return SqlitePublicationSource(store, source_id)


@pytest.fixture
def v2_spool(tmp_path: Path) -> Iterator[tuple[StateDB, Path, tuple[Path, ...]]]:
    database = StateDB(tmp_path / "collector-state.sqlite3")
    store = InternalAudioStore(database)
    ready = tmp_path / "ready"
    ready.mkdir()
    ready.chmod(0o2750)
    checkpoint = tmp_path / "work" / "omi-ready-checkpoint.json"
    checkpoint.parent.mkdir()
    checkpoint.write_text(
        json.dumps(
            {
                "audio_contract": "omi-audio-v2",
                "analysis_cursor": None,
                "vad_decisions": [],
                "open_speech_tail": None,
                "acknowledged": [],
            }
        ),
        encoding="utf-8",
    )
    begin_publication_cutover(database)
    activate_public_audio_v2(store, ready, checkpoint)
    clock = ClockState((ClockRange(10, 17, 1.0, 0.25, "confirmed", "test"),))
    options = PublicAudioV2Options(ReadyConfig(target_audio_seconds=0.02), clock, 17, True, 128)
    first = _v2_source(
        store,
        "a" * 32,
        10,
        tuple(_v2_record(timestamp, audio=timestamp in {10, 12}) for timestamp in range(10, 15)),
    )
    second = _v2_source(store, "b" * 32, 16, (_v2_record(16),))
    first_result = finalize_public_audio_v2(store, (first,), ready, options)
    second_result = finalize_public_audio_v2(store, (second,), ready, options)
    artifacts = tuple(item.path for result in (first_result, second_result) for item in result.published)
    try:
        yield database, ready, artifacts
    finally:
        store.close()


def _firmware_observations(root: Path, counters: tuple[int, ...]) -> None:
    store = FirmwareObservationStore(_state_db(root))
    for counter in counters:
        assert store.record(RingInfo(10, 20, 100, counter, RECORD_SIZE))


def _record(timestamp: int) -> bytes:
    payload = bytearray(440)
    return timestamp.to_bytes(4, "big") + bytes(payload)


def _bundle(
    root: Path,
    name: str,
    records: tuple[bytes, ...],
    *,
    start: int = 10,
    time_ranges: tuple[tuple[int, int], ...] | None = None,
) -> Path:
    path = root / name
    path.mkdir(parents=True)
    raw = b"".join(records)
    raw_hash = hashlib.sha256(raw).hexdigest()
    (path / "records.bin").write_bytes(raw)
    ranges = time_ranges or ((start, start + len(records)),)
    end = ranges[-1][1]
    gaps = "".join(f":{left[1]}-{right[0]}" for left, right in pairwise(ranges) if left[1] < right[0])
    bundle_id = hashlib.sha256(f"{start}:{end}:{raw_hash}{gaps}".encode()).hexdigest()
    (path / "manifest.json").write_text(
        json.dumps(
            {
                "bundle_id": bundle_id,
                "start_sequence": start,
                "next_sequence": end,
                "record_count": len(records),
                "record_size": RECORD_SIZE,
                "records_sha256": raw_hash,
                "draft_raw_sha256": raw_hash,
                "time_ranges": [
                    {"start_sequence": range_start, "next_sequence": range_end, "utc": None}
                    for range_start, range_end in ranges
                ],
            }
        ),
        encoding="utf-8",
    )
    return path


def _rewrite_manifest(bundle: Path, **updates: object) -> None:
    manifest_path = bundle / "manifest.json"
    manifest = cast(dict[str, object], json.loads(manifest_path.read_text(encoding="utf-8")))
    manifest.update(updates)
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")


def _collect_spool_metrics_in_child(root: str, result: Connection) -> None:
    try:
        collect_spool_metrics(Path(root))
    except SpoolMetricsError:
        result.send("rejected")
    else:
        result.send("accepted")
    finally:
        result.close()


def test_empty_spool_has_zero_raw_metrics(tmp_path: Path) -> None:
    result = collect_spool_metrics(tmp_path)

    assert result.as_dict() == {
        "current_window": {
            "bundle_count": 0,
            "downloaded_records": 0,
            "downloaded_raw_bytes": 0,
            "lost_records": 0,
            "lost_raw_bytes": 0,
            "loss_ratio": 0.0,
        },
        "firmware_lifetime": {
            "observation_count": 0,
            "initial": None,
            "latest": None,
            "observed_increase": 0,
            "regression_count": 0,
            "epoch_count": 0,
        },
    }


def test_v2_metrics_count_physical_records_and_gaps_without_opening_public_files(
    v2_spool: tuple[StateDB, Path, tuple[Path, ...]], monkeypatch: pytest.MonkeyPatch
) -> None:
    database, ready, artifacts = v2_spool
    assert len(artifacts) == 2
    packet_count = 0
    for artifact in artifacts:
        entry = database.get(StateKind.READY_PUBLICATION, artifact.name)
        assert entry is not None
        packet_count += cast(int, entry["packet_count"])
    assert packet_count == 3
    forbidden = {path / name for path in artifacts for name in ("audio.ogg", "manifest.json")}
    original_open = Path.open
    original_read_bytes = Path.read_bytes
    original_read_text = Path.read_text
    original_read_ledger = ready_bundles._read_ledger
    ledger_reads = 0

    def count_ledger_reads(state_db: StateDB) -> dict[str, object]:
        nonlocal ledger_reads
        ledger_reads += 1
        return original_read_ledger(state_db)

    def guarded_open(path: Path, *args: object, **kwargs: object) -> object:
        if path in forbidden:
            raise AssertionError(f"public v2 artifact was opened: {path}")
        return cast(Callable[..., object], original_open)(path, *args, **kwargs)

    def guarded_read_bytes(path: Path) -> bytes:
        if path in forbidden:
            raise AssertionError(f"public v2 artifact was read: {path}")
        return original_read_bytes(path)

    def guarded_read_text(
        path: Path, *, encoding: str | None = None, errors: str | None = None, newline: str | None = None
    ) -> str:
        if path in forbidden:
            raise AssertionError(f"public v2 artifact was read: {path}")
        return original_read_text(path, encoding=encoding, errors=errors, newline=newline)

    monkeypatch.setattr(Path, "open", guarded_open)
    monkeypatch.setattr(Path, "read_bytes", guarded_read_bytes)
    monkeypatch.setattr(Path, "read_text", guarded_read_text)
    monkeypatch.setattr(ready_bundles, "_read_ledger", count_ledger_reads)

    metrics = collect_spool_metrics(ready, state_db=database).current_window

    assert metrics.bundle_count == 2
    assert metrics.downloaded_records == 6
    assert metrics.downloaded_raw_bytes == 6 * RECORD_SIZE
    assert metrics.lost_records == 1
    assert metrics.lost_raw_bytes == RECORD_SIZE
    assert ledger_reads == 1


@pytest.mark.parametrize("corruption", ["size", "extra", "audio_symlink", "manifest_symlink"])
def test_v2_metrics_reject_invalid_public_artifact_inventory(
    v2_spool: tuple[StateDB, Path, tuple[Path, ...]], tmp_path: Path, corruption: str
) -> None:
    database, ready, artifacts = v2_spool
    artifact = artifacts[0]
    audio = artifact / "audio.ogg"
    manifest = artifact / "manifest.json"
    if corruption == "size":
        with audio.open("ab") as stream:
            stream.write(b"x")
    elif corruption == "extra":
        (artifact / "unexpected").touch()
    else:
        source = audio if corruption == "audio_symlink" else manifest
        target = tmp_path / f"retained-{source.name}"
        target.write_bytes(source.read_bytes())
        source.unlink()
        source.symlink_to(target)

    with pytest.raises(SpoolMetricsError):
        collect_spool_metrics(ready, state_db=database)


def test_v2_metrics_require_the_publication_ledger(
    v2_spool: tuple[StateDB, Path, tuple[Path, ...]], tmp_path: Path
) -> None:
    _database, ready, _artifacts = v2_spool
    with pytest.raises(SpoolMetricsError, match="ready manifest is invalid"):
        collect_spool_metrics(ready)
    empty_database = StateDB(tmp_path / "empty-state.sqlite3")
    with pytest.raises(SpoolMetricsError, match="ready manifest is invalid"):
        collect_spool_metrics(ready, state_db=empty_database)


def test_v2_metrics_include_retired_ledger_entries_while_artifacts_remain(
    v2_spool: tuple[StateDB, Path, tuple[Path, ...]],
) -> None:
    database, ready, artifacts = v2_spool
    bundle_id = artifacts[0].name
    entry = database.get(StateKind.READY_PUBLICATION, bundle_id)
    assert entry is not None
    database.put(StateKind.READY_PUBLICATION, bundle_id, {**entry, "state": "retired"})

    metrics = collect_spool_metrics(ready, state_db=database).current_window

    assert metrics.bundle_count == 2
    assert metrics.downloaded_records == 6


def test_legacy_metrics_keep_the_manifest_path_when_database_has_no_v2_entry(tmp_path: Path) -> None:
    bundle = _bundle(tmp_path, "10-11", (_record(1),))
    database = StateDB(tmp_path.with_name(f"{tmp_path.name}-collector-state.sqlite3"))

    metrics = collect_spool_metrics(tmp_path, state_db=database).current_window

    assert metrics.bundle_count == 1
    assert metrics.downloaded_records == 1
    assert metrics.downloaded_raw_bytes == RECORD_SIZE
    assert bundle.is_dir()


def test_unreadable_manifest_is_reported_as_read_error(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    bundle = _bundle(tmp_path, "unreadable", (_record(1),))
    original_read_text = Path.read_text

    def deny_manifest_read(
        path: Path, *, encoding: str | None = None, errors: str | None = None, newline: str | None = None
    ) -> str:
        if path == bundle / "manifest.json":
            raise PermissionError(errno.EACCES, "permission denied", str(path))
        return original_read_text(path, encoding=encoding, errors=errors, newline=newline)

    monkeypatch.setattr(Path, "read_text", deny_manifest_read)

    with pytest.raises(SpoolMetricsError, match=r"cannot read manifest\.json"):
        collect_spool_metrics(tmp_path)


def test_malformed_manifest_is_reported_as_invalid_json(tmp_path: Path) -> None:
    bundle = _bundle(tmp_path, "malformed", (_record(1),))
    (bundle / "manifest.json").write_text("{", encoding="utf-8")

    with pytest.raises(SpoolMetricsError, match=r"manifest\.json is invalid JSON"):
        collect_spool_metrics(tmp_path)


@pytest.mark.parametrize("kind", ["unexpected_field", "boolean_record_count"])
def test_metrics_rejects_noncanonical_bundle_evidence(tmp_path: Path, kind: str) -> None:
    device_root = tmp_path
    bundle = _bundle(device_root, "100-101", (_record(1),))
    manifest = cast(dict[str, object], json.loads((bundle / "manifest.json").read_text(encoding="utf-8")))
    if kind == "unexpected_field":
        manifest["obsolete"] = True
    else:
        manifest["record_count"] = True
    (bundle / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")

    with pytest.raises(SpoolMetricsError):
        collect_spool_metrics(tmp_path)


def test_metrics_rejects_fifo_records_file_without_blocking(tmp_path: Path) -> None:
    bundle = _bundle(tmp_path, "10-11", (_record(1),))
    records_path = bundle / "records.bin"
    records_path.unlink()
    os.mkfifo(records_path)
    writer_stopped = threading.Event()
    writer_errors: list[OSError] = []

    def write_if_collector_opens_fifo() -> None:
        deadline = time.monotonic() + 12
        while not writer_stopped.is_set() and time.monotonic() < deadline:
            try:
                descriptor = os.open(records_path, os.O_WRONLY | os.O_NONBLOCK)
            except OSError as error:
                if error.errno != errno.ENXIO:
                    writer_errors.append(error)
                    return
                writer_stopped.wait(0.01)
            else:
                try:
                    os.write(descriptor, _record(1))
                except OSError as error:
                    writer_errors.append(error)
                finally:
                    os.close(descriptor)
                return

    writer = threading.Thread(target=write_if_collector_opens_fifo)
    writer.start()
    script = """\
from pathlib import Path
import sys

from omi_collector.spool_metrics import SpoolMetricsError, collect_spool_metrics

try:
    collect_spool_metrics(Path(sys.argv[1]))
except SpoolMetricsError:
    print("rejected")
else:
    print("accepted")
    """
    try:
        # Keep a mutant that blocks opening a FIFO killable by the test.
        result = subprocess.run(
            [sys.executable, "-c", script, str(tmp_path)],
            capture_output=True,
            check=False,
            cwd=Path.cwd(),
            text=True,
            timeout=10,
        )
    finally:
        writer_stopped.set()
        writer.join(timeout=1)

    assert not writer.is_alive()
    assert not writer_errors
    assert result.returncode == 0
    assert result.stdout.strip() == "rejected"


def test_metrics_rejects_fifo_records_file_without_a_writer(tmp_path: Path) -> None:
    bundle = _bundle(tmp_path, "10-11", (_record(1),))
    records_path = bundle / "records.bin"
    records_path.unlink()
    os.mkfifo(records_path)
    receive, send = get_context("fork").Pipe(duplex=False)
    process = get_context("fork").Process(
        target=_collect_spool_metrics_in_child,
        args=(str(tmp_path), send),
    )
    process_started = False
    try:
        process.start()
        process_started = True
        send.close()
        assert receive.poll(2.0), "spool metrics collection blocked opening a FIFO records file"
        assert receive.recv() == "rejected"
        process.join(timeout=2.0)
        assert process.exitcode == 0
    finally:
        send.close()
        receive.close()
        if process_started and process.is_alive():
            process.terminate()
            process.join(timeout=2.0)
        if process_started and process.is_alive():
            process.kill()
            process.join(timeout=2.0)
        if process_started:
            process.close()


def test_empty_capture_device_still_reports_spool_firmware_observations(tmp_path: Path) -> None:
    spool = tmp_path / "spool"
    capture_root = _capture_root(tmp_path)
    spool.mkdir()
    capture_root.mkdir()
    _firmware_observations(spool, (4, 9))

    result = collect_spool_metrics(capture_root, state_db=_state_db(spool, read_only=True))

    assert result.current_window.bundle_count == 0
    assert result.firmware_lifetime.observation_count == 2
    assert result.firmware_lifetime.initial == 4
    assert result.firmware_lifetime.latest == 9


def test_ready_bundle_is_a_valid_device_spool(tmp_path: Path) -> None:
    _bundle(tmp_path, "0-2-a", (_record(1000), _record(1001)), start=0)

    result = collect_spool_metrics(tmp_path)

    assert result.current_window.bundle_count == 1
    assert result.current_window.downloaded_records == 2
    assert result.current_window.downloaded_raw_bytes == 2 * RECORD_SIZE


def test_sparse_ready_bundle_uses_manifest_ranges_for_loss_accounting(tmp_path: Path) -> None:
    _bundle(tmp_path, "sparse", (_record(1000), _record(1001)), start=10, time_ranges=((10, 11), (12, 13)))

    result = collect_spool_metrics(tmp_path)

    assert result.current_window.downloaded_records == 2
    assert result.current_window.lost_records == 1
    assert result.current_window.lost_raw_bytes == RECORD_SIZE


@pytest.mark.parametrize(
    "time_ranges",
    [
        [{"start_sequence": 10, "next_sequence": 10, "utc": None}],
        [{"start_sequence": 11, "next_sequence": 12, "utc": None}],
        [{"start_sequence": 10, "next_sequence": 12, "utc": None}],
        [
            {"start_sequence": 10, "next_sequence": 11, "utc": None},
            {"start_sequence": 11, "next_sequence": 13, "utc": None},
        ],
    ],
)
def test_ready_bundle_rejects_malformed_time_ranges(tmp_path: Path, time_ranges: object) -> None:
    bundle = _bundle(tmp_path, "invalid", (_record(1), _record(2)), start=10, time_ranges=((10, 11), (12, 13)))
    _rewrite_manifest(bundle, time_ranges=time_ranges)

    with pytest.raises(SpoolMetricsError, match="ready manifest is invalid"):
        collect_spool_metrics(tmp_path)


@pytest.mark.parametrize(
    ("start", "records", "record_size"),
    [(-1, (_record(1),), RECORD_SIZE), (10, (), RECORD_SIZE), (10, (_record(1),), RECORD_SIZE + 1)],
)
def test_ready_bundle_rejects_invalid_dimensions(
    tmp_path: Path, start: int, records: tuple[bytes, ...], record_size: int
) -> None:
    bundle = _bundle(tmp_path, "invalid", records, start=start)
    _rewrite_manifest(bundle, record_size=record_size)

    with pytest.raises(SpoolMetricsError, match="ready manifest is invalid"):
        collect_spool_metrics(tmp_path)


def test_ready_bundle_rejects_malformed_draft_hash_even_when_identity_matches(tmp_path: Path) -> None:
    bundle = _bundle(tmp_path, "invalid", (_record(1),))
    draft_hash = "not-a-sha256"
    bundle_id = hashlib.sha256(f"10:11:{draft_hash}".encode()).hexdigest()
    _rewrite_manifest(bundle, draft_raw_sha256=draft_hash, bundle_id=bundle_id)

    with pytest.raises(SpoolMetricsError, match="ready manifest is invalid"):
        collect_spool_metrics(tmp_path)


def test_ordinary_metrics_rejects_wrong_raw_size_without_opening_payload(tmp_path: Path) -> None:
    bundle = _bundle(tmp_path, "invalid", (_record(1),))
    (bundle / "records.bin").write_bytes(b"short")

    with pytest.raises(SpoolMetricsError, match=r"records\.bin does not match manifest"):
        collect_spool_metrics(tmp_path)


def test_empty_ready_time_ranges_are_rejected(tmp_path: Path) -> None:
    bundle = _bundle(tmp_path, "invalid", (_record(1),))
    _rewrite_manifest(bundle, time_ranges=[])

    with pytest.raises(SpoolMetricsError, match="ready manifest is invalid"):
        collect_spool_metrics(tmp_path)


def test_ready_bundle_symlink_is_not_counted(tmp_path: Path) -> None:
    outside = tmp_path / "outside"
    outside.mkdir()
    (tmp_path / "bundle").symlink_to(outside, target_is_directory=True)

    assert collect_spool_metrics(tmp_path).current_window.bundle_count == 0


def test_ready_bundle_rejects_symlinked_records_file_even_when_bytes_match(tmp_path: Path) -> None:
    file_name = "records.bin"
    bundle = _bundle(tmp_path, "bundle", (_record(10),))
    artifact = bundle / file_name
    target = tmp_path.parent / f"{tmp_path.name}-outside-{file_name}"
    target.write_bytes(artifact.read_bytes())
    artifact.unlink()
    artifact.symlink_to(target)

    try:
        with pytest.raises(SpoolMetricsError, match="must be a regular file"):
            collect_spool_metrics(tmp_path)
    finally:
        target.unlink()


def test_firmware_observations_are_reported_separately_from_loss(tmp_path: Path) -> None:
    _firmware_observations(tmp_path, (4, 9, 12))
    ready_root = tmp_path / "ready"
    ready_root.mkdir()
    state_db = _state_db(tmp_path)
    quality_writer = SQLiteQualityMetrics(state_db, release_version="1.2.3")
    quality_writer.record_sequence_loss(
        SequenceLossMetric("2026-10-11T01:02:03+00:00", "session", 1, RECORD_SIZE, "test", "1.2.3", None, None)
    )
    assert quality_writer.close()
    ClockStateStore(state_db).observe(
        RingInfo(10, 20, 100, 12, RECORD_SIZE),
        offset_seconds=0.0,
        uncertainty_seconds=0.01,
        source_id="test",
        tolerance_seconds=0.1,
    )
    reader = _state_db(tmp_path, read_only=True)
    state_kinds = (
        StateKind.FIRMWARE,
        StateKind.CLOCK,
        StateKind.QUALITY_EVENT,
        StateKind.QUALITY_EVENT_BUDGET,
    )
    before = {kind: reader.list(kind) for kind in state_kinds}

    result = collect_spool_metrics(ready_root, state_db=reader)
    after = {kind: reader.list(kind) for kind in state_kinds}

    assert result.firmware_lifetime.observation_count == 3
    assert result.firmware_lifetime.initial == 4
    assert result.firmware_lifetime.latest == 12
    assert result.firmware_lifetime.observed_increase == 8
    assert result.firmware_lifetime.regression_count == 0
    assert result.firmware_lifetime.epoch_count == 1
    assert result.current_window.lost_records == 0
    assert result.current_window.lost_raw_bytes == 0
    assert result.current_window.loss_ratio == 0.0
    assert before == after
    assert before[StateKind.CLOCK]
    assert before[StateKind.QUALITY_EVENT]
    assert before[StateKind.QUALITY_EVENT_BUDGET]


def test_firmware_counter_reset_starts_a_new_epoch(tmp_path: Path) -> None:
    _firmware_observations(tmp_path, (4, 9, 3, 8, 2))
    ready_root = tmp_path / "ready"
    ready_root.mkdir()

    result = collect_spool_metrics(ready_root, state_db=_state_db(tmp_path, read_only=True))

    assert result.firmware_lifetime.observation_count == 5
    assert result.firmware_lifetime.initial == 4
    assert result.firmware_lifetime.latest == 2
    assert result.firmware_lifetime.observed_increase == 10
    assert result.firmware_lifetime.regression_count == 2
    assert result.firmware_lifetime.epoch_count == 3
    assert result.current_window.lost_records == 0


def test_malformed_firmware_observation_chain_fails_closed(tmp_path: Path) -> None:
    _firmware_observations(tmp_path, (4, 9))
    state_db = _state_db(tmp_path)
    state_db.put(StateKind.FIRMWARE, "latest", {"unexpected": True})

    with pytest.raises(SpoolMetricsError, match="firmware observations are invalid"):
        collect_spool_metrics(tmp_path, state_db=_state_db(tmp_path, read_only=True))


def test_sequence_discontinuity_is_aggregated_between_real_bundles(tmp_path: Path) -> None:
    device = tmp_path
    _bundle(device, "first", (_record(1), _record(2)))
    _bundle(device, "second", (_record(4), _record(5)), start=13)

    result = collect_spool_metrics(tmp_path)

    assert result.current_window.lost_records == 1
    assert result.current_window.lost_raw_bytes == RECORD_SIZE
    assert result.current_window.loss_ratio == 0.2


def test_adjacent_half_open_ranges_are_valid_and_have_no_loss(tmp_path: Path) -> None:
    _bundle(tmp_path, "first", (_record(1), _record(2)), start=10)
    _bundle(tmp_path, "next", (_record(3),), start=12)

    current = collect_spool_metrics(tmp_path).current_window

    assert current.bundle_count == 2
    assert current.downloaded_records == 3
    assert current.lost_records == 0
    assert current.loss_ratio == 0.0


def test_conflicting_overlapping_ranges_fail_closed(tmp_path: Path) -> None:
    device = tmp_path
    _bundle(device, "first", (_record(1), _record(2)))
    _bundle(device, "overlap", (_record(3), _record(4)), start=11)

    with pytest.raises(SpoolMetricsError, match="sequence ranges overlap"):
        collect_spool_metrics(tmp_path)


def test_ordinary_metrics_does_not_open_any_ready_payload(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _bundle(tmp_path, "first", (_record(1), _record(2)))
    _bundle(tmp_path, "second", (_record(3),), start=12)
    original_open = Path.open

    def guarded_open(path: Path, *args: object, **kwargs: object) -> object:
        if path.name == "records.bin":
            raise AssertionError("ordinary metrics must not open records.bin")
        return cast(Callable[..., object], original_open)(path, *args, **kwargs)

    monkeypatch.setattr(Path, "open", guarded_open)

    result = collect_spool_metrics(tmp_path)

    assert result.current_window.bundle_count == 2
    assert result.current_window.downloaded_records == 3
    assert result.current_window.downloaded_raw_bytes == 3 * RECORD_SIZE


def test_partial_and_symlink_artifacts_are_not_counted(tmp_path: Path) -> None:
    device = tmp_path
    (device / "attempts").mkdir()
    target = device / ".real"
    target.mkdir()
    (device / "symlink").symlink_to(target, target_is_directory=True)

    result = collect_spool_metrics(tmp_path)

    assert result.current_window.bundle_count == 0


def test_firmware_observations_are_optional_without_a_state_database(tmp_path: Path) -> None:
    result = collect_spool_metrics(tmp_path)

    assert result.firmware_lifetime.observation_count == 0
    assert result.firmware_lifetime.initial is None
    assert result.firmware_lifetime.latest is None


def test_spool_metrics_rejects_a_state_database_path_replaced_by_directory_or_symlink(tmp_path: Path) -> None:
    database = _state_db(tmp_path)
    reader = _state_db(tmp_path, read_only=True)
    database.path.unlink()
    database.path.mkdir()
    with pytest.raises(SpoolMetricsError, match="ready publication ledger is invalid"):
        collect_spool_metrics(tmp_path, state_db=reader)
    database.path.rmdir()
    target = StateDB(tmp_path.parent / f"{tmp_path.name}-state-target.sqlite3")
    database.path.symlink_to(target.path)
    with pytest.raises(SpoolMetricsError, match="ready publication ledger is invalid"):
        collect_spool_metrics(tmp_path, state_db=reader)
