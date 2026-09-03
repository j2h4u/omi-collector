from dataclasses import FrozenInstanceError

import pytest

from omi_collector.config import (
    DEFAULT_CONFIG,
    BleConfig,
    CollectorConfig,
    DebugLogConfig,
    DurabilityConfig,
    FirmwareObservationConfig,
    MemoryConfig,
    ObservabilityConfig,
    PhyConfig,
    PresenceConfig,
    QualityMetricsConfig,
    RetryConfig,
    ServiceConfig,
    StagingRetentionConfig,
    TransferConfig,
    WriterConfig,
)


def test_default_config_is_hierarchical_and_immutable() -> None:
    assert isinstance(DEFAULT_CONFIG, CollectorConfig)
    assert isinstance(DEFAULT_CONFIG.presence, PresenceConfig)
    assert isinstance(DEFAULT_CONFIG.retry, RetryConfig)
    assert DEFAULT_CONFIG.presence.absence_seconds == 60.0
    assert DEFAULT_CONFIG.presence.fallback_seconds == 300.0
    assert DEFAULT_CONFIG.presence.drained_fallback_seconds == 900.0
    assert DEFAULT_CONFIG.presence.scan_transition_seconds == 2.0
    assert DEFAULT_CONFIG.retry.rapid_backoff == (1.0, 2.0, 4.0, 8.0, 16.0, 30.0)
    assert DEFAULT_CONFIG.retry.storage_not_ready_backoff == (1.0, 2.0, 5.0)
    assert DEFAULT_CONFIG.transfer.collect_timeout_seconds == 600.0
    assert DEFAULT_CONFIG.memory.arena_max_bytes == 512 * 1024 * 1024
    assert DEFAULT_CONFIG.durability.checkpoint_records == 1024
    assert DEFAULT_CONFIG.ble.preflight_settle_seconds == 0.5
    assert DEFAULT_CONFIG.ble.observer_max_connection_parameter_requests == 8
    assert DEFAULT_CONFIG.ble.observer_max_connection_parameter_updates == 8
    assert DEFAULT_CONFIG.ble.observer_max_phy_update_outcomes == 8
    assert DEFAULT_CONFIG.ble.observer_max_data_length_transitions == 8
    assert DEFAULT_CONFIG.writer.chunk_records == 128
    assert DEFAULT_CONFIG.writer.max_control_commands == 32
    assert DEFAULT_CONFIG.writer.state_poll_seconds == 0.001
    assert DEFAULT_CONFIG.writer.close_timeout_seconds == 5.0
    assert DEFAULT_CONFIG.writer.join_poll_seconds == 0.005
    assert isinstance(DEFAULT_CONFIG.firmware_observations, FirmwareObservationConfig)
    assert DEFAULT_CONFIG.firmware_observations.retry_backoff_seconds == (0.05, 0.25, 1.0, 5.0)
    assert DEFAULT_CONFIG.firmware_observations.close_timeout_seconds == 0.5
    assert DEFAULT_CONFIG.telemetry.clock_drift_threshold_seconds == 5.0
    assert isinstance(DEFAULT_CONFIG.observability, ObservabilityConfig)
    assert DEFAULT_CONFIG.observability.max_error_chain_entries == 8
    assert DEFAULT_CONFIG.observability.max_error_entry_chars == 1024
    assert isinstance(DEFAULT_CONFIG.observability.debug_log, DebugLogConfig)
    assert DEFAULT_CONFIG.observability.debug_log.file_name == "debug.jsonl"
    assert isinstance(DEFAULT_CONFIG.observability.quality_metrics, QualityMetricsConfig)
    assert DEFAULT_CONFIG.observability.quality_metrics.file_name == "quality.jsonl"
    assert DEFAULT_CONFIG.observability.quality_metrics.source_revision_env == "OMI_COLLECTOR_SOURCE_REVISION"
    assert DEFAULT_CONFIG.phy.reap_timeout_seconds == 5.0
    assert DEFAULT_CONFIG.service.max_records == 256
    with pytest.raises(FrozenInstanceError):
        DEFAULT_CONFIG.presence = PresenceConfig()  # type: ignore[reportAttributeAccessIssue]
    with pytest.raises(FrozenInstanceError):
        DEFAULT_CONFIG.retry.rapid_backoff += (60.0,)  # type: ignore[operator]


def test_ble_config_att_mtu_query_timeout_default() -> None:
    assert DEFAULT_CONFIG.ble.att_mtu_query_timeout_seconds == 0.5


def test_staging_retention_default_is_terminal_lifecycle_window() -> None:
    assert isinstance(DEFAULT_CONFIG.staging_retention, StagingRetentionConfig)
    assert DEFAULT_CONFIG.staging_retention.terminal_retention_seconds == 72.0 * 60.0 * 60.0


@pytest.mark.parametrize(
    ("factory", "field", "value"),
    [
        (PresenceConfig, "absence_seconds", 0.0),
        (RetryConfig, "max_storage_not_ready_responses", 0),
        (TransferConfig, "info_timeout_seconds", 0.0),
        (MemoryConfig, "arena_max_bytes", 0),
        (DurabilityConfig, "checkpoint_records", 0),
        (StagingRetentionConfig, "terminal_retention_seconds", 0.0),
        (BleConfig, "preflight_settle_seconds", 0.0),
        (BleConfig, "att_mtu_query_timeout_seconds", 0.0),
        (WriterConfig, "chunk_records", 0),
        (FirmwareObservationConfig, "close_timeout_seconds", 0.0),
        (ObservabilityConfig, "max_error_chain_entries", 0),
        (ObservabilityConfig, "max_error_entry_chars", 0),
        (DebugLogConfig, "max_bytes", 0),
        (PhyConfig, "reap_timeout_seconds", 0.0),
        (ServiceConfig, "interval_seconds", 0),
    ],
)
def test_runtime_limits_must_be_positive(factory: type[object], field: str, value: object) -> None:
    with pytest.raises(ValueError, match="positive"):
        factory(**{field: value})  # type: ignore[operator]


def test_runtime_ranges_are_coherent() -> None:
    with pytest.raises(ValueError, match="max_fallback"):
        PresenceConfig(fallback_seconds=31.0, max_fallback_seconds=30.0)
    with pytest.raises(ValueError, match="overhead"):
        DurabilityConfig(staging_overhead_fraction=1.1)
    with pytest.raises(ValueError, match="default_collect_records"):
        ServiceConfig(max_records=1, default_collect_records=2)
    with pytest.raises(ValueError, match="scan cancellation"):
        PresenceConfig(scan_cancel_grace_min_seconds=0.2, scan_cancel_grace_max_seconds=0.1)


def test_config_defaults_use_tuples_for_schedules() -> None:
    assert isinstance(DEFAULT_CONFIG.retry.rapid_backoff, tuple)
    assert isinstance(DEFAULT_CONFIG.retry.storage_not_ready_backoff, tuple)


@pytest.mark.parametrize("field", ["rapid_backoff", "storage_not_ready_backoff"])
def test_retry_schedules_reject_mutable_lists(field: str) -> None:
    schedule = [1.0, 2.0]
    with pytest.raises(ValueError, match="tuple"):
        RetryConfig(**{field: schedule})  # type: ignore[arg-type]
    schedule.append(3.0)


@pytest.mark.parametrize("value", [float("inf"), float("nan")])
def test_float_limits_must_be_finite(value: float) -> None:
    with pytest.raises(ValueError):
        TransferConfig(info_timeout_seconds=value)
    with pytest.raises(ValueError):
        RetryConfig(rapid_backoff=(value,))
    with pytest.raises(ValueError):
        DurabilityConfig(staging_overhead_fraction=value)


def test_integer_limits_reject_bool() -> None:
    with pytest.raises(ValueError):
        MemoryConfig(arena_max_bytes=True)  # type: ignore[arg-type]
    with pytest.raises(ValueError):
        WriterConfig(chunk_records=False)  # type: ignore[arg-type]


def test_debug_log_config_rejects_unsafe_names_and_unknown_encoding() -> None:
    with pytest.raises(ValueError, match="relative"):
        DebugLogConfig(file_name="../debug.jsonl")
    with pytest.raises(ValueError, match="encoding"):
        DebugLogConfig(encoding="not-an-encoding")
    with pytest.raises(ValueError, match="positive"):
        DebugLogConfig(backup_count=0)
    with pytest.raises(ValueError, match="exceed"):
        DebugLogConfig(max_bytes=512, max_record_bytes=1024)
    with pytest.raises(ValueError, match="relative"):
        QualityMetricsConfig(file_name="../quality.jsonl")
