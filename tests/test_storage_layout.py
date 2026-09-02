from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

from omi_collector.capture.adapters.firmware_observations import FirmwareObservationStore, read_firmware_observations
from omi_collector.capture.adapters.opportunistic_runtime import OpportunisticRuntime
from omi_collector.capture.adapters.staging_filesystem import StagingPaths
from omi_collector.capture.adapters.staging_store import StagingStore
from omi_collector.capture.application.quarantine_maintenance import QuarantineMaintenance
from omi_collector.capture.domain.ring_protocol import RECORD_SIZE, ReadBeginNotification, RingInfo
from omi_collector.storage_layout import StorageLayoutError, load_storage_layout


def _layout(path: Path) -> Path:
    path.write_text(
        """version = 2

[collector]
root = "collector"
attempts = "attempts"
quarantine = "quarantine"
lock = "collector.lock"
device_state = "device.json"
debug_log = "debug.jsonl"

[publication]
root = "source"
""",
        encoding="utf-8",
    )
    return path


def test_layout_parent_is_the_only_root_and_loading_creates_nothing(tmp_path: Path) -> None:
    layout = load_storage_layout(_layout(tmp_path / "layout.toml"))

    assert layout.collector.root == tmp_path / "collector"
    assert layout.publication.root == tmp_path / "source"
    assert not layout.collector.root.exists()
    assert not layout.publication.root.exists()


def test_layout_rejects_a_symlinked_declared_target(tmp_path: Path) -> None:
    _layout(tmp_path / "layout.toml")
    outside = tmp_path / "outside"
    outside.mkdir()
    (tmp_path / "collector").symlink_to(outside, target_is_directory=True)

    with pytest.raises(StorageLayoutError, match="symlink"):
        load_storage_layout(tmp_path / "layout.toml")


def test_layout_rejects_unsupported_version(tmp_path: Path) -> None:
    path = tmp_path / "layout.toml"
    path.write_text(
        _layout(path).read_text(encoding="utf-8").replace("version = 2", "version = 999"),
        encoding="utf-8",
    )

    with pytest.raises(StorageLayoutError, match="version is unsupported"):
        load_storage_layout(path)


def test_layout_rejects_unknown_publication_key(tmp_path: Path) -> None:
    path = tmp_path / "layout.toml"
    path.write_text(
        _layout(path).read_text(encoding="utf-8").replace('root = "source"', 'root = "source"\nextra = "x"'),
        encoding="utf-8",
    )

    with pytest.raises(StorageLayoutError, match="publication"):
        load_storage_layout(path)


def test_quarantine_lifecycle_publishes_authenticated_prefix_and_retains_bad_source(tmp_path: Path) -> None:
    collector = tmp_path / "collector"
    paths = StagingPaths(
        collector,
        tmp_path / "source",
        collector / "attempts",
        collector / "quarantine",
        collector / "collector.lock",
        collector / "device.json",
    )
    store = StagingStore.from_paths(paths)
    attempt = store.prepare_streaming_attempt("omi", 100, 1)
    attempt.record_read_begin(ReadBeginNotification(100, 1))
    record = (1).to_bytes(4, "big") + bytes(RECORD_SIZE - 4)
    attempt.append_record(0, 100, record)
    attempt.checkpoint()
    attempt.close(durable=True)
    source = store.quarantine_attempt_source("omi", attempt.attempt_id)
    bad = paths.quarantine / "omi" / "bad"
    bad.mkdir()

    asyncio.run(QuarantineMaintenance(store, "omi", None, OpportunisticRuntime()).run_once(lambda: False))

    assert (paths.capture_root / "omi").is_dir()
    assert (source / "published.json").is_file()
    assert (bad / "unprocessable.json").is_file()


def test_firmware_observations_stay_in_one_declared_device_file(tmp_path: Path) -> None:
    state = tmp_path / "collector" / "device.json"
    store = FirmwareObservationStore(state)
    assert store.record("omi", RingInfo(10, 20, 100, 1, RECORD_SIZE))
    assert store.record("omi", RingInfo(10, 21, 100, 2, RECORD_SIZE))

    assert [item.dropped_packets for item in read_firmware_observations(state, "omi")] == [1, 2]
    assert {path.name for path in state.parent.iterdir()} == {"device.json"}
