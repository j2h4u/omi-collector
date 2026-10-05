from __future__ import annotations

import errno
import hashlib
import json
import os
import subprocess
import sys
import threading
import time
from multiprocessing import get_context
from multiprocessing.connection import Connection
from pathlib import Path
from shutil import rmtree
from typing import cast

import pytest

from omi_collector.capture.adapters.firmware_observations import FirmwareObservationStore
from omi_collector.capture.domain.ring_protocol import RingInfo
from omi_collector.spool_metrics import SpoolMetricsError, collect_spool_metrics

RECORD_SIZE = 444


_CAPTURE_ROOTS: set[Path] = set()


def _capture_root(tmp_path: Path) -> Path:
    root = tmp_path.parent / f"{tmp_path.name}-captures"
    if tmp_path not in _CAPTURE_ROOTS:
        rmtree(root, ignore_errors=True)
        _CAPTURE_ROOTS.add(tmp_path)
    return root


def _firmware_observations(root: Path, counters: tuple[int, ...]) -> None:
    store = FirmwareObservationStore(root / "device.json")
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
) -> Path:
    path = root / name
    path.mkdir(parents=True)
    raw = b"".join(records)
    raw_hash = hashlib.sha256(raw).hexdigest()
    (path / "records.bin").write_bytes(raw)
    end = start + len(records)
    bundle_id = hashlib.sha256(f"{start}:{end}:{raw_hash}".encode()).hexdigest()
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
                "time_ranges": [{"start_sequence": start, "next_sequence": end, "utc": None}],
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
    result = collect_spool_metrics(tmp_path, observation_root=tmp_path / "device.json")

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

    result = collect_spool_metrics(capture_root, observation_root=spool / "device.json")

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


@pytest.mark.parametrize("mismatch", ["size", "hash"])
def test_ready_bundle_rejects_each_records_manifest_mismatch(tmp_path: Path, mismatch: str) -> None:
    bundle = _bundle(tmp_path, "invalid", (_record(1),))
    if mismatch == "size":
        (bundle / "records.bin").write_bytes(b"short")
        actual_hash = hashlib.sha256(b"short").hexdigest()
        _rewrite_manifest(bundle, records_sha256=actual_hash)
    else:
        (bundle / "records.bin").write_bytes(_record(2))

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

    result = collect_spool_metrics(ready_root, observation_root=tmp_path / "device.json")

    assert result.firmware_lifetime.observation_count == 3
    assert result.firmware_lifetime.initial == 4
    assert result.firmware_lifetime.latest == 12
    assert result.firmware_lifetime.observed_increase == 8
    assert result.firmware_lifetime.regression_count == 0
    assert result.firmware_lifetime.epoch_count == 1
    assert result.current_window.lost_records == 0
    assert result.current_window.lost_raw_bytes == 0
    assert result.current_window.loss_ratio == 0.0


def test_firmware_counter_reset_starts_a_new_epoch(tmp_path: Path) -> None:
    _firmware_observations(tmp_path, (4, 9, 3, 8, 2))
    ready_root = tmp_path / "ready"
    ready_root.mkdir()

    result = collect_spool_metrics(ready_root, observation_root=tmp_path / "device.json")

    assert result.firmware_lifetime.observation_count == 5
    assert result.firmware_lifetime.initial == 4
    assert result.firmware_lifetime.latest == 2
    assert result.firmware_lifetime.observed_increase == 10
    assert result.firmware_lifetime.regression_count == 2
    assert result.firmware_lifetime.epoch_count == 3
    assert result.current_window.lost_records == 0


def test_malformed_firmware_observation_chain_fails_closed(tmp_path: Path) -> None:
    _firmware_observations(tmp_path, (4, 9))
    state = tmp_path / "device.json"
    state.write_text(state.read_text(encoding="utf-8") + "\n", encoding="utf-8")

    with pytest.raises(SpoolMetricsError, match="firmware observations are invalid"):
        collect_spool_metrics(tmp_path, observation_root=tmp_path / "device.json")


def test_sequence_discontinuity_is_aggregated_between_real_bundles(tmp_path: Path) -> None:
    device = tmp_path
    _bundle(device, "first", (_record(1), _record(2)))
    _bundle(device, "second", (_record(4), _record(5)), start=13)

    result = collect_spool_metrics(tmp_path, observation_root=tmp_path / "device.json")

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
        collect_spool_metrics(tmp_path, observation_root=tmp_path / "device.json")


def test_bundle_validation_streams_records_instead_of_reading_all_bytes(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    device = tmp_path
    _bundle(device, "first", (_record(1), _record(2)))
    original_read_bytes = Path.read_bytes

    def guarded_read_bytes(path: Path) -> bytes:
        if path.name == "records.bin":
            raise AssertionError("records.bin must be hashed as a bounded stream")
        return original_read_bytes(path)

    monkeypatch.setattr(Path, "read_bytes", guarded_read_bytes)

    result = collect_spool_metrics(tmp_path, observation_root=tmp_path / "device.json")

    assert result.current_window.downloaded_records == 2


def test_partial_and_symlink_artifacts_are_not_counted(tmp_path: Path) -> None:
    device = tmp_path
    (device / "attempts").mkdir()
    target = device / ".real"
    target.mkdir()
    (device / "symlink").symlink_to(target, target_is_directory=True)

    result = collect_spool_metrics(tmp_path, observation_root=tmp_path / "device.json")

    assert result.current_window.bundle_count == 0


def test_observation_path_is_optional_when_state_file_is_not_available(tmp_path: Path) -> None:
    result = collect_spool_metrics(tmp_path)

    assert result.firmware_lifetime.observation_count == 0
    assert result.firmware_lifetime.initial is None
    assert result.firmware_lifetime.latest is None


def test_observation_path_rejects_directory_and_symlink(tmp_path: Path) -> None:
    observation_directory = tmp_path / "collector"
    observation_directory.mkdir()
    with pytest.raises(SpoolMetricsError, match="firmware observations are invalid"):
        collect_spool_metrics(tmp_path, observation_root=observation_directory)

    target = tmp_path / "state-target.json"
    target.write_text("{}", encoding="utf-8")
    observation_symlink = tmp_path / "device.json"
    observation_symlink.symlink_to(target)
    with pytest.raises(SpoolMetricsError, match="firmware observations are invalid"):
        collect_spool_metrics(tmp_path, observation_root=observation_symlink)
