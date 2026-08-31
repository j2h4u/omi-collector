"""Dependency-free immutable runtime configuration for the collector."""

from __future__ import annotations

import codecs
from dataclasses import dataclass
from math import isfinite
from pathlib import PurePath


def _require_positive_float(value: float, name: str) -> None:
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not isfinite(value) or value <= 0:
        raise ValueError(f"{name} must be positive")


def _require_positive_int(value: int, name: str) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError(f"{name} must be a positive integer")


def _require_path_component(value: str, name: str) -> None:
    if not isinstance(value, str) or not value or value != value.strip():
        raise ValueError(f"{name} must be a non-empty path component")
    path = PurePath(value)
    if path.is_absolute() or len(path.parts) != 1 or path.name != value:
        raise ValueError(f"{name} must be one relative path component")


def _require_encoding(value: str, name: str) -> None:
    if not isinstance(value, str) or not value or value != value.strip():
        raise ValueError(f"{name} must be a non-empty encoding")
    try:
        codecs.lookup(value)
    except LookupError as error:
        raise ValueError(f"{name} must name a supported encoding") from error


def _require_logger_name(value: str, name: str) -> None:
    if not isinstance(value, str) or not value or value != value.strip():
        raise ValueError(f"{name} must be a non-empty logger name")
    if any(not component.isidentifier() for component in value.split(".")):
        raise ValueError(f"{name} must contain dot-separated identifier components")


def _require_non_empty_positive(values: tuple[float, ...], name: str) -> None:
    if (
        not isinstance(values, tuple)
        or not values
        or any(
            isinstance(value, bool) or not isinstance(value, (int, float)) or not isfinite(value) or value <= 0
            for value in values
        )
    ):
        raise ValueError(f"{name} must be a tuple containing positive values")


def _require_fraction(value: float, name: str) -> None:
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not isfinite(value) or not 0 <= value <= 1:
        raise ValueError(f"{name} must be finite and between zero and one")


_RAPID_BACKOFF = (1.0, 2.0, 4.0, 8.0, 16.0, 30.0)


@dataclass(frozen=True, slots=True)
class PresenceConfig:
    """Presence hysteresis and fallback timings, in seconds."""

    absence_seconds: float = 60.0
    fallback_seconds: float = 30.0
    drained_fallback_seconds: float = 900.0
    scan_transition_seconds: float = 2.0
    max_fallback_seconds: float = 300.0
    max_drained_fallback_seconds: float = 900.0
    scan_cancel_grace_min_seconds: float = 0.01
    scan_cancel_grace_max_seconds: float = 0.1
    scan_cancel_grace_fraction: float = 0.1

    def __post_init__(self) -> None:
        for name, value in (
            ("absence_seconds", self.absence_seconds),
            ("fallback_seconds", self.fallback_seconds),
            ("drained_fallback_seconds", self.drained_fallback_seconds),
            ("scan_transition_seconds", self.scan_transition_seconds),
            ("max_fallback_seconds", self.max_fallback_seconds),
            ("max_drained_fallback_seconds", self.max_drained_fallback_seconds),
        ):
            _require_positive_float(value, name)
        if self.fallback_seconds > self.max_fallback_seconds:
            raise ValueError("fallback_seconds must not exceed max_fallback_seconds")
        if self.drained_fallback_seconds > self.max_drained_fallback_seconds:
            raise ValueError("drained_fallback_seconds must not exceed its maximum")
        _require_positive_float(self.scan_cancel_grace_min_seconds, "scan_cancel_grace_min_seconds")
        _require_positive_float(self.scan_cancel_grace_max_seconds, "scan_cancel_grace_max_seconds")
        if self.scan_cancel_grace_min_seconds > self.scan_cancel_grace_max_seconds:
            raise ValueError("scan cancellation minimum must not exceed its maximum")
        _require_fraction(self.scan_cancel_grace_fraction, "scan_cancel_grace_fraction")


@dataclass(frozen=True, slots=True)
class RetryConfig:
    """Retry and bounded recovery schedules."""

    rapid_backoff: tuple[float, ...] = _RAPID_BACKOFF
    storage_not_ready_backoff: tuple[float, ...] = (1.0, 2.0, 5.0)
    max_storage_not_ready_responses: int = 3
    presence_preflight_budget_seconds: float = 1.0

    def __post_init__(self) -> None:
        _require_non_empty_positive(self.rapid_backoff, "rapid_backoff")
        _require_non_empty_positive(self.storage_not_ready_backoff, "storage_not_ready_backoff")
        _require_positive_int(self.max_storage_not_ready_responses, "max_storage_not_ready_responses")
        _require_positive_float(self.presence_preflight_budget_seconds, "presence_preflight_budget_seconds")


@dataclass(frozen=True, slots=True)
class TransferConfig:
    """BLE operation deadlines and progress reporting cadence."""

    info_timeout_seconds: float = 30.0
    collect_timeout_seconds: float = 600.0
    sync_timeout_seconds: float = 60.0
    progress_interval_seconds: float = 2.0

    def __post_init__(self) -> None:
        for name, value in (
            ("info_timeout_seconds", self.info_timeout_seconds),
            ("collect_timeout_seconds", self.collect_timeout_seconds),
            ("sync_timeout_seconds", self.sync_timeout_seconds),
            ("progress_interval_seconds", self.progress_interval_seconds),
        ):
            _require_positive_float(value, name)


@dataclass(frozen=True, slots=True)
class MemoryConfig:
    """In-memory arena and notification admission limits."""

    arena_max_bytes: int = 512 * 1024 * 1024
    notification_buffer_bytes: int = 2 * 1024 * 1024

    def __post_init__(self) -> None:
        for name, value in (
            ("arena_max_bytes", self.arena_max_bytes),
            ("notification_buffer_bytes", self.notification_buffer_bytes),
        ):
            _require_positive_int(value, name)


@dataclass(frozen=True, slots=True)
class DurabilityConfig:
    """Disk admission, checkpoint, and file-hash batching limits."""

    staging_headroom_bytes: int = 16 * 1024 * 1024
    staging_overhead_fraction: float = 0.10
    checkpoint_records: int = 1024
    io_chunk_bytes: int = 1024 * 1024

    def __post_init__(self) -> None:
        _require_positive_int(self.staging_headroom_bytes, "staging_headroom_bytes")
        _require_fraction(self.staging_overhead_fraction, "staging_overhead_fraction")
        _require_positive_int(self.checkpoint_records, "checkpoint_records")
        _require_positive_int(self.io_chunk_bytes, "io_chunk_bytes")


@dataclass(frozen=True, slots=True)
class StagingRetentionConfig:
    """Retention for terminal staging evidence that can never resume."""

    terminal_retention_seconds: float = 72.0 * 60.0 * 60.0

    def __post_init__(self) -> None:
        _require_positive_float(self.terminal_retention_seconds, "terminal_retention_seconds")


@dataclass(frozen=True, slots=True)
class BleConfig:
    """Transport policy and bounded observer limits.

    HCI packet layout values belong to the raw observer adapter.  Keeping them
    out of the runtime configuration prevents protocol implementation details
    from becoming an operator-facing policy surface.
    """

    preflight_settle_seconds: float = 0.5
    cancelled_disconnect_timeout_seconds: float = 1.0
    att_mtu_query_timeout_seconds: float = 0.5
    adapter_name: str = "hci0"
    observer_queue_max_packets: int = 128
    observer_receive_bytes: int = 260
    observer_poll_seconds: float = 0.02
    observer_join_timeout_seconds: float = 0.5
    observer_max_phy_transitions: int = 8
    observer_max_phy_update_outcomes: int = 8
    observer_max_data_length_transitions: int = 8
    observer_max_connection_parameter_requests: int = 8
    observer_max_connection_parameter_updates: int = 8
    observer_max_local_name_chars: int = 64

    def __post_init__(self) -> None:
        for name, value in (
            ("preflight_settle_seconds", self.preflight_settle_seconds),
            ("cancelled_disconnect_timeout_seconds", self.cancelled_disconnect_timeout_seconds),
            ("att_mtu_query_timeout_seconds", self.att_mtu_query_timeout_seconds),
            ("observer_poll_seconds", self.observer_poll_seconds),
            ("observer_join_timeout_seconds", self.observer_join_timeout_seconds),
        ):
            _require_positive_float(value, name)
        _require_path_component(self.adapter_name, "adapter_name")
        for name, value in (
            ("observer_queue_max_packets", self.observer_queue_max_packets),
            ("observer_receive_bytes", self.observer_receive_bytes),
            ("observer_max_phy_transitions", self.observer_max_phy_transitions),
            ("observer_max_phy_update_outcomes", self.observer_max_phy_update_outcomes),
            ("observer_max_data_length_transitions", self.observer_max_data_length_transitions),
            ("observer_max_connection_parameter_requests", self.observer_max_connection_parameter_requests),
            ("observer_max_connection_parameter_updates", self.observer_max_connection_parameter_updates),
            ("observer_max_local_name_chars", self.observer_max_local_name_chars),
        ):
            _require_positive_int(value, name)


@dataclass(frozen=True, slots=True)
class WriterConfig:
    """Dedicated writer queue, batching, and shutdown controls."""

    chunk_records: int = 128
    max_control_commands: int = 32
    state_poll_seconds: float = 0.001
    close_timeout_seconds: float = 5.0
    join_poll_seconds: float = 0.005

    def __post_init__(self) -> None:
        _require_positive_int(self.chunk_records, "chunk_records")
        _require_positive_int(self.max_control_commands, "max_control_commands")
        _require_positive_float(self.state_poll_seconds, "state_poll_seconds")
        _require_positive_float(self.close_timeout_seconds, "close_timeout_seconds")
        _require_positive_float(self.join_poll_seconds, "join_poll_seconds")


@dataclass(frozen=True, slots=True)
class FirmwareObservationConfig:
    """Retry and bounded shutdown controls for firmware observations."""

    retry_backoff_seconds: tuple[float, ...] = (0.05, 0.25, 1.0, 5.0)
    close_timeout_seconds: float = 0.5

    def __post_init__(self) -> None:
        _require_non_empty_positive(self.retry_backoff_seconds, "retry_backoff_seconds")
        _require_positive_float(self.close_timeout_seconds, "close_timeout_seconds")


@dataclass(frozen=True, slots=True)
class TelemetryConfig:
    """Optional characteristic and clock-observation limits."""

    clock_drift_threshold_seconds: float = 5.0
    optional_operation_timeout_seconds: float = 0.5
    host_clock_probe_timeout_seconds: float = 1.0

    def __post_init__(self) -> None:
        _require_positive_float(self.clock_drift_threshold_seconds, "clock_drift_threshold_seconds")
        _require_positive_float(self.optional_operation_timeout_seconds, "optional_operation_timeout_seconds")
        _require_positive_float(self.host_clock_probe_timeout_seconds, "host_clock_probe_timeout_seconds")


@dataclass(frozen=True, slots=True)
class DebugLogConfig:
    """Persistent, bounded DEBUG diagnostic ring beneath the collector root."""

    file_name: str = "debug.jsonl"
    max_bytes: int = 8 * 1024 * 1024
    backup_count: int = 3
    max_record_bytes: int = 1 * 1024 * 1024
    queue_max_records: int = 256
    shutdown_join_seconds: float = 1.0
    encoding: str = "utf-8"
    logger_name: str = "omi_collector.debug"

    def __post_init__(self) -> None:
        _require_path_component(self.file_name, "file_name")
        _require_positive_int(self.max_bytes, "max_bytes")
        _require_positive_int(self.backup_count, "backup_count")
        _require_positive_int(self.max_record_bytes, "max_record_bytes")
        if self.max_record_bytes > self.max_bytes:
            raise ValueError("max_record_bytes must not exceed max_bytes")
        _require_positive_int(self.queue_max_records, "queue_max_records")
        _require_positive_float(self.shutdown_join_seconds, "shutdown_join_seconds")
        _require_encoding(self.encoding, "encoding")
        _require_logger_name(self.logger_name, "logger_name")


@dataclass(frozen=True, slots=True)
class ObservabilityConfig:
    """Bounded diagnostics retained separately from the operational journal."""

    max_error_chain_entries: int = 8
    max_error_entry_chars: int = 1024
    debug_log: DebugLogConfig = DebugLogConfig()

    def __post_init__(self) -> None:
        _require_positive_int(self.max_error_chain_entries, "max_error_chain_entries")
        _require_positive_int(self.max_error_entry_chars, "max_error_entry_chars")


@dataclass(frozen=True, slots=True)
class PhyConfig:
    """Temporary controller PHY guard deadlines."""

    bluetoothctl_timeout_seconds: float = 5.0
    reap_timeout_seconds: float = 5.0

    def __post_init__(self) -> None:
        _require_positive_float(self.bluetoothctl_timeout_seconds, "bluetoothctl_timeout_seconds")
        _require_positive_float(self.reap_timeout_seconds, "reap_timeout_seconds")


@dataclass(frozen=True, slots=True)
class ServiceConfig:
    """CLI service cadence and bounded one-shot collection defaults."""

    interval_seconds: int = 60
    max_records: int = 256
    default_collect_records: int = 1

    def __post_init__(self) -> None:
        _require_positive_int(self.interval_seconds, "interval_seconds")
        _require_positive_int(self.max_records, "max_records")
        _require_positive_int(self.default_collect_records, "default_collect_records")
        if self.default_collect_records > self.max_records:
            raise ValueError("default_collect_records must not exceed max_records")


@dataclass(frozen=True, slots=True)
class CollectorConfig:
    """Complete immutable runtime configuration tree."""

    presence: PresenceConfig = PresenceConfig()
    retry: RetryConfig = RetryConfig()
    transfer: TransferConfig = TransferConfig()
    memory: MemoryConfig = MemoryConfig()
    durability: DurabilityConfig = DurabilityConfig()
    staging_retention: StagingRetentionConfig = StagingRetentionConfig()
    ble: BleConfig = BleConfig()
    writer: WriterConfig = WriterConfig()
    firmware_observations: FirmwareObservationConfig = FirmwareObservationConfig()
    telemetry: TelemetryConfig = TelemetryConfig()
    observability: ObservabilityConfig = ObservabilityConfig()
    phy: PhyConfig = PhyConfig()
    service: ServiceConfig = ServiceConfig()


DEFAULT_CONFIG = CollectorConfig()
