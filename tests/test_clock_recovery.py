from __future__ import annotations

import json
import shutil
from hashlib import sha256
from pathlib import Path
from typing import cast

import pytest

from omi_collector.capture.adapters.bundle_contract import BundleManifest, SealedReceipt
from omi_collector.capture.adapters.clock_corrections import ClockCorrectionStore
from omi_collector.capture.adapters.clock_recovery import (
    HistoricalClockImporter,
    HistoricalRecoveryError,
    recover_and_publish,
)
from omi_collector.capture.domain.ring_protocol import RECORD_SIZE


def _bundle(root: Path, start: int, timestamps: tuple[int, ...]) -> None:
    raw = b"".join(
        timestamp.to_bytes(4, "big") + bytes((index + 1,)) * (RECORD_SIZE - 4)
        for index, timestamp in enumerate(timestamps)
    )
    digest = sha256(raw).hexdigest()
    path = root / f"{start}-{start + len(timestamps)}-{digest[:16]}"
    path.mkdir(parents=True)
    (path / "records.bin").write_bytes(raw)
    manifest = BundleManifest(2, start, start + len(timestamps), len(timestamps), RECORD_SIZE, digest)
    (path / "manifest.json").write_text(json.dumps(manifest.as_dict()))
    (path / "receipt.json").write_text(json.dumps(SealedReceipt("a" * 32, digest).as_dict()))


def test_historical_recovery_rejects_info_boundary_before_time_sample(tmp_path: Path) -> None:
    captured = tmp_path / "captured"
    captured.mkdir()
    _bundle(captured, 10, (1300, 1301))
    _bundle(captured, 12, (1002,))
    collector = tmp_path / "collector"
    collector.mkdir()
    (collector / "timeline-repairs.json").write_text(
        json.dumps(
            {
                "version": 1,
                "repairs": [{"start_sequence": 10, "next_sequence": 12, "offset_seconds": 300, "evidence": "op"}],
            }
        )
    )
    store = ClockCorrectionStore(collector / "device.json")
    operation = store.mark_unresolved(store.prepare(1301, 1000, 300.0, 10))
    entries = [
        {
            "boot_id": "boot",
            "invocation_id": "invocation-a",
            "realtime": 999.0,
            "monotonic": 9.0,
            "event": "systemd_start",
            "_SOURCE_REALTIME_TIMESTAMP": 999_000_000,
        },
        {
            "boot_id": "boot",
            "invocation_id": "invocation-a",
            "realtime": 1001.0,
            "monotonic": 11.0,
            "device_time_epoch": 1301,
            "event": "pendant_clock_sync",
            "boundary_sequence_min": 10,
        },
        {
            "boot_id": "boot",
            "realtime": 1002.0,
            "monotonic": 12.0,
            "device_epoch": 1002,
            "event": "pendant_observation",
            "write_sequence": 12,
        },
    ]

    decision = HistoricalClockImporter(collector / "device.json", captured).recover(entries, apply=True)[0]
    assert decision.state == "unresolved"
    assert decision.boundary_sequence is None
    assert next(item for item in store.records() if item.operation_id == operation.operation_id).state == "unresolved"


def test_historical_recovery_without_systemd_anchor_stays_unresolved(tmp_path: Path) -> None:
    captured = tmp_path / "captured"
    captured.mkdir()
    _bundle(captured, 10, (1300, 1301))
    collector = tmp_path / "collector"
    collector.mkdir()
    store = ClockCorrectionStore(collector / "device.json")
    operation = store.mark_unresolved(store.prepare(1301, 1000, 300.0, 10))

    decision = HistoricalClockImporter(collector / "device.json", captured).recover(
        [
            {
                "boot_id": "boot",
                "realtime": 1000.0,
                "monotonic": 10.0,
                "device_epoch": 1301,
                "event": "pendant_clock_sync",
                "sequence": 10,
            },
            {
                "boot_id": "boot",
                "realtime": 1001.0,
                "monotonic": 11.0,
                "device_epoch": 1001,
                "event": "pendant_observation",
                "sequence": 11,
            },
        ],
        dry_run=True,
    )[0]

    assert decision.operation_id == operation.operation_id
    assert decision.state == "unresolved"
    assert decision.boundary_sequence is None
    assert "anchor" in decision.reason


def test_historical_recovery_rejects_boot_mismatch_and_gap(tmp_path: Path) -> None:
    collector, captured = tmp_path / "collector", tmp_path / "captured"
    collector.mkdir()
    captured.mkdir()
    store = ClockCorrectionStore(collector / "device.json")
    store.mark_unresolved(store.prepare(1300, 1000, 300.0, 10))
    importer = HistoricalClockImporter(collector / "device.json", captured)
    try:
        importer.recover(
            [
                {"boot_id": "a", "realtime": 1000.0, "monotonic": 10.0, "device_epoch": 1000, "sequence": 100},
                {"boot_id": "b", "realtime": 1000.0, "monotonic": 11.0, "device_epoch": 1000, "sequence": 101},
            ]
        )
    except ValueError:
        pass
    else:
        raise AssertionError("boot mismatch must be rejected")


def test_historical_rejects_arbitrary_journald_json_without_incident_evidence(tmp_path: Path) -> None:
    collector, captured = tmp_path / "collector", tmp_path / "captured"
    collector.mkdir()
    captured.mkdir()
    store = ClockCorrectionStore(collector / "device.json")
    store.mark_unresolved(store.prepare(1300, 1000, 300.0, 10))
    importer = HistoricalClockImporter(collector / "device.json", captured)
    entries = [
        {
            "_BOOT_ID": "boot",
            "__REALTIME_TIMESTAMP": 1_000_000_000,
            "__MONOTONIC_TIMESTAMP": 10_000_000,
            "device_epoch": 1000,
            "write_sequence": 100,
            "MESSAGE": json.dumps({"source": "typed"}),
        }
    ]
    with pytest.raises(HistoricalRecoveryError, match="clock event"):
        importer.recover(json.dumps(entries).encode(), dry_run=True)
    assert not (collector / "clock-observations").exists()


def test_historical_recovery_accepts_real_jsonl_message_and_string_microseconds(tmp_path: Path) -> None:
    captured = tmp_path / "captured"
    captured.mkdir()
    _bundle(captured, 10, (1298, 1301))
    collector = tmp_path / "collector"
    collector.mkdir()
    store = ClockCorrectionStore(collector / "device.json")
    store.mark_unresolved(store.prepare(1300, 1000, 300.0, 10))
    rows = [
        {
            "_BOOT_ID": "boot",
            "_SYSTEMD_INVOCATION_ID": "invocation-a",
            "__REALTIME_TIMESTAMP": "999500000",
            "__MONOTONIC_TIMESTAMP": "9500000",
            "_SOURCE_REALTIME_TIMESTAMP": "999500000",
            "MESSAGE": json.dumps({"event": "systemd_start"}),
        },
        {
            "_BOOT_ID": "boot",
            "_SYSTEMD_INVOCATION_ID": "invocation-a",
            "__REALTIME_TIMESTAMP": "1000000000",
            "__MONOTONIC_TIMESTAMP": "10000000",
            "MESSAGE": json.dumps({"event": "pendant_clock_sync", "boundary_sequence_min": 10}),
        },
        {
            "_BOOT_ID": "boot",
            "__REALTIME_TIMESTAMP": "1002000000",
            "__MONOTONIC_TIMESTAMP": "12000000",
            "MESSAGE": json.dumps({"event": "pendant_observation", "device_time_epoch": 1002, "write_sequence": 12}),
        },
    ]
    decision = HistoricalClockImporter(collector / "device.json", captured).recover(
        "\n".join(json.dumps(row) for row in rows), dry_run=True
    )[0]
    assert decision.state == "applied"


def test_historical_dry_run_preserves_device_epoch_and_raw_scale_shift(tmp_path: Path) -> None:
    captured = tmp_path / "captured"
    captured.mkdir()
    _bundle(captured, 7_717_545, (1_789_749_898, 1_789_749_900))
    collector = tmp_path / "collector"
    collector.mkdir()
    store = ClockCorrectionStore(collector / "device.json")
    operation = store.mark_unresolved(store.prepare(1_789_749_943, 1_789_749_900, 45.0, 7_717_545))
    rows = [
        {
            "boot_id": "boot",
            "invocation_id": "invocation-a",
            "realtime": 1_789_749_897.5,
            "monotonic": 99.5,
            "event": "systemd_start",
            "_SOURCE_REALTIME_TIMESTAMP": 1_789_749_897_500_000,
        },
        {
            "boot_id": "boot",
            "invocation_id": "invocation-a",
            "realtime": 1_789_749_898.0,
            "monotonic": 100.0,
            "device_time_epoch": 1_789_749_943,
            "event": "pendant_observation",
            "write_sequence": 7_717_545,
        },
        {
            "boot_id": "boot",
            "realtime": 1_789_749_900.0,
            "monotonic": 102.0,
            "device_time_epoch": 1_789_749_900,
            "event": "pendant_observation",
            "write_sequence": 7_717_546,
        },
    ]

    decision = HistoricalClockImporter(collector / "device.json", captured).recover(rows, dry_run=True)[0]

    assert decision.operation_id == operation.operation_id
    assert decision.state == "applied"
    assert not (collector / "clock-observations").exists()


def test_real_incident_applies_boundary_without_late_raw_frontier(tmp_path: Path) -> None:
    captured = tmp_path / "captured"
    captured.mkdir()
    _bundle(captured, 7_717_545, (1_789_749_898,))
    _bundle(captured, 7_763_450, (1_789_749_899,))
    collector = tmp_path / "collector"
    collector.mkdir()
    store = ClockCorrectionStore(collector / "device.json")
    operation = store.mark_unresolved(store.prepare(1_789_749_943, 1_789_749_504, 438.890716, 7_717_545))
    rows = [
        {
            "boot_id": "real-boot",
            "invocation_id": "f89",
            "realtime": 1_789_749_487.155169,
            "monotonic": 92_261.988504,
            "event": "systemd_start",
            "_SOURCE_REALTIME_TIMESTAMP": 1_789_749_487_155_169,
        },
        {
            "boot_id": "real-boot",
            "invocation_id": "f89",
            "realtime": 1_789_749_504.123796,
            "monotonic": 92_278.957131,
            "device_time_epoch": 1_789_749_943,
            "event": "pendant_observation",
            "write_sequence": 7_717_545,
        },
        {
            "boot_id": "real-boot",
            "invocation_id": "ff45",
            "realtime": 1_789_760_526.239924,
            "monotonic": 103_301.073259,
            "device_time_epoch": 1_789_760_526,
            "event": "pendant_observation",
            "write_sequence": 7_829_831,
        },
    ]

    decision = HistoricalClockImporter(collector / "device.json", captured).recover(rows, apply=True)[0]

    assert decision.operation_id == operation.operation_id
    assert decision.state == "applied"
    assert decision.boundary_sequence == 7_717_545
    assert store.records()[0].verified_epoch == 1_789_760_526
    source = next((collector / "clock-imports").glob("*.json"))
    calculation = cast(dict[str, object], json.loads(source.read_text()))["boundary_calculation"]
    calculation = cast(dict[str, object], calculation)
    assert calculation["delta_max"] == pytest.approx(16.968626976)
    assert calculation["device_raw_delta"] == 45


@pytest.mark.parametrize("anchor_invocation", [None, "wrong-invocation"])
def test_real_incident_rejects_missing_or_wrong_anchor_invocation(
    tmp_path: Path, anchor_invocation: str | None
) -> None:
    captured = tmp_path / "captured"
    captured.mkdir()
    _bundle(captured, 7_717_545, (1_789_749_898,))
    collector = tmp_path / "collector"
    collector.mkdir()
    store = ClockCorrectionStore(collector / "device.json")
    store.mark_unresolved(store.prepare(1_789_749_943, 1_789_749_504, 438.890716, 7_717_545))
    anchor = {
        "boot_id": "real-boot",
        "realtime": 1_789_749_487.155169,
        "monotonic": 92_261.988504,
        "event": "systemd_start",
        "_SOURCE_REALTIME_TIMESTAMP": 1_789_749_487_155_169,
    }
    if anchor_invocation is not None:
        anchor["invocation_id"] = anchor_invocation
    with pytest.raises(HistoricalRecoveryError, match="anchor"):
        HistoricalClockImporter(collector / "device.json", captured).recover(
            [
                anchor,
                {
                    "boot_id": "real-boot",
                    "invocation_id": "f89",
                    "realtime": 1_789_749_504.123796,
                    "monotonic": 92_278.957131,
                    "device_time_epoch": 1_789_749_943,
                    "event": "pendant_observation",
                    "write_sequence": 7_717_545,
                },
                {
                    "boot_id": "real-boot",
                    "invocation_id": "ff45",
                    "realtime": 1_789_760_526.239924,
                    "monotonic": 103_301.073259,
                    "device_time_epoch": 1_789_760_526,
                    "event": "pendant_observation",
                    "write_sequence": 7_829_831,
                },
            ],
            dry_run=True,
        )
    assert store.records()[0].state == "unresolved"


def test_unresolved_later_operation_publishes_prefix_of_crossing_bundle(tmp_path: Path) -> None:
    captured, published, collector = tmp_path / "captured", tmp_path / "source", tmp_path / "collector"
    captured.mkdir()
    collector.mkdir()
    _bundle(captured, 8, (1000, 1001, 1002, 1003))
    (collector / "timeline-repairs.json").write_text(json.dumps({"version": 1, "repairs": []}))
    store = ClockCorrectionStore(collector / "device.json")
    store.mark_unresolved(store.prepare(1300, 1000, 300.0, 10))
    result = recover_and_publish(collector / "device.json", captured, published)
    assert result.record_count == 2
    result_again = recover_and_publish(collector / "device.json", captured, published)
    assert result_again.record_count == 2


def test_empty_startup_replays_durable_import_after_observation_crash(tmp_path: Path) -> None:
    captured, collector = tmp_path / "captured", tmp_path / "collector"
    captured.mkdir()
    collector.mkdir()
    _bundle(captured, 10, (1298, 1301))
    store = ClockCorrectionStore(collector / "device.json")
    operation = store.mark_unresolved(store.prepare(1300, 1000, 300.0, 10))
    importer = HistoricalClockImporter(collector / "device.json", captured)
    rows = [
        {
            "boot_id": "boot",
            "invocation_id": "invocation-a",
            "realtime": 999.5,
            "monotonic": 9.5,
            "event": "systemd_start",
            "_SOURCE_REALTIME_TIMESTAMP": 999_500_000,
        },
        {
            "boot_id": "boot",
            "invocation_id": "invocation-a",
            "realtime": 1000.0,
            "monotonic": 10.0,
            "event": "pendant_clock_sync",
            "sequence": 10,
        },
        {
            "boot_id": "boot",
            "realtime": 1001.0,
            "monotonic": 11.0,
            "device_epoch": 1001,
            "event": "pendant_observation",
            "sequence": 11,
        },
    ]

    importer.recover(rows)
    shutil.rmtree(collector / "clock-observations")

    decision = importer.recover((), apply=True)[0]

    assert decision.state == "applied"
    assert next(item for item in store.records() if item.operation_id == operation.operation_id).state == "applied"


def test_startup_replays_zero_width_native_observation_without_future_raw(tmp_path: Path) -> None:
    collector, captured = tmp_path / "collector", tmp_path / "captured"
    collector.mkdir()
    captured.mkdir()
    store = ClockCorrectionStore(collector / "device.json")
    operation = store.mark_unresolved(store.prepare(1300, 1000, 300.0, 10))
    store.observation_store.native_trusted(
        observation_id="a" * 32,
        session_id="native",
        host_boot_id="boot",
        host_realtime_start=1000.0,
        host_realtime_end=1000.0,
        host_monotonic_start=10.0,
        host_monotonic_end=10.0,
        device_epoch=1300,
        info_sequence_min=10,
        info_sequence_max=10,
        operation_id=operation.operation_id,
        observation_role="initial",
    )
    store.observation_store.native_trusted(
        observation_id="b" * 32,
        session_id="native",
        host_boot_id="boot",
        host_realtime_start=1000.0,
        host_realtime_end=1000.0,
        host_monotonic_start=10.0,
        host_monotonic_end=10.0,
        device_epoch=1000,
        info_sequence_min=10,
        info_sequence_max=10,
        operation_id=operation.operation_id,
        effective_boundary_sequence=10,
        observation_role="later",
    )

    decision = HistoricalClockImporter(collector / "device.json", captured).recover((), apply=True)[0]
    assert decision.state == "applied"
    assert store.records()[0].verified_epoch == 1000


def test_startup_does_not_promote_unparented_native_later_observation(tmp_path: Path) -> None:
    collector, captured = tmp_path / "collector", tmp_path / "captured"
    collector.mkdir()
    captured.mkdir()
    store = ClockCorrectionStore(collector / "device.json")
    operation = store.mark_unresolved(store.prepare(1300, 1000, 300.0, 10))
    store.observation_store.native_trusted(
        observation_id="e" * 32,
        session_id="native",
        host_boot_id="boot",
        host_realtime_start=1000.0,
        host_realtime_end=1000.0,
        host_monotonic_start=10.0,
        host_monotonic_end=10.0,
        device_epoch=1000,
        info_sequence_min=10,
        info_sequence_max=10,
        operation_id=operation.operation_id,
        effective_boundary_sequence=10,
        observation_role="later",
    )

    assert HistoricalClockImporter(collector / "device.json", captured).recover((), apply=True) == ()
    assert store.records()[0].state == "unresolved"
