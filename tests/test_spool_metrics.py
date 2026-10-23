from __future__ import annotations

import os
from collections.abc import Callable, Iterator
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
    with pytest.raises(SpoolMetricsError, match="no omi-audio-v2 publication ledger entry"):
        collect_spool_metrics(ready)
    empty_database = StateDB(tmp_path / "empty-state.sqlite3")
    with pytest.raises(SpoolMetricsError, match="no omi-audio-v2 publication ledger entry"):
        collect_spool_metrics(ready, state_db=empty_database)


@pytest.mark.parametrize("kind", ["directory", "symlink"])
def test_unledgered_visible_entries_fail_closed(tmp_path: Path, kind: str) -> None:
    if kind == "directory":
        (tmp_path / "unknown").mkdir()
    else:
        target = tmp_path / "target"
        target.mkdir()
        (tmp_path / "unknown").symlink_to(target, target_is_directory=True)

    with pytest.raises(SpoolMetricsError, match="no omi-audio-v2 publication ledger entry"):
        collect_spool_metrics(tmp_path)


def test_hidden_staging_entries_are_ignored_without_a_ledger(tmp_path: Path) -> None:
    (tmp_path / ".staging").mkdir()

    assert collect_spool_metrics(tmp_path).current_window.bundle_count == 0


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


def test_v2_metrics_reject_fifo_audio_file_without_blocking(
    v2_spool: tuple[StateDB, Path, tuple[Path, ...]],
) -> None:
    database, ready, artifacts = v2_spool
    audio = artifacts[0] / "audio.ogg"
    audio.unlink()
    os.mkfifo(audio)

    with pytest.raises(SpoolMetricsError, match=r"audio\.ogg must be a regular file"):
        collect_spool_metrics(ready, state_db=database)


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
