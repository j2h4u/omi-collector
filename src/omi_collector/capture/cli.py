"""Safe, human-facing operations for one Omi pendant.

Host-facing dependencies sit behind module-level factories. Tests replace those
factories with fakes, so CLI tests never need Bluetooth or sudo access.
"""

from __future__ import annotations

import asyncio
import inspect
import json
import logging
import signal
import sys
from collections.abc import AsyncIterator, Callable, Coroutine, Mapping
from contextlib import asynccontextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import cast

from bleak.backends.device import BLEDevice

from ..config import DEFAULT_CONFIG, CollectorConfig
from ..core import package_version
from .adapters.bleak_transport import BleakPresenceObserver, BleakRingTransport, Device
from .adapters.opportunistic_runtime import OpportunisticRuntime
from .adapters.phy_guard import ScopedPhyGuard
from .adapters.publication import SealResult
from .adapters.quality_metrics import JsonlQualityMetrics, source_revision_from_environment
from .adapters.staging_store import StagingStore
from .application import collector
from .application.opportunistic_sync import (
    CollectionPreservedCancelledError,
    run_opportunistic_collector,
)
from .application.presence import PresencePolicy, PresenceScheduler
from .application.ring_transport import RingSession
from .application.session_lifecycle import ActivityEvent, OpportunisticOptions, RetryPolicy
from .domain.ring_protocol import RingInfo, RingStatus

MAX_RECORDS = DEFAULT_CONFIG.service.max_records
SUPPORTED_ADAPTER = DEFAULT_CONFIG.ble.adapter_name
TRANSFER_TIMEOUTS = collector.TransferTimeouts(
    info=DEFAULT_CONFIG.transfer.info_timeout_seconds,
    transfer=DEFAULT_CONFIG.transfer.collect_timeout_seconds,
)


@dataclass(frozen=True, slots=True)
class DownloadMetrics:
    """End-to-end transfer accounting emitted by the command layer."""

    elapsed_seconds: float
    payload_bytes: int
    bytes_per_second: float
    records_per_second: float
    remaining_packets: int
    eta_seconds: float | None

    def as_dict(self) -> dict[str, object]:
        """Return stable JSON field names, including finite zero values."""
        return {
            "bytes_per_second": round(self.bytes_per_second, 2),
            "elapsed_seconds": round(self.elapsed_seconds, 2),
            "eta_seconds": round(self.eta_seconds, 2) if self.eta_seconds is not None else None,
            "payload_bytes": self.payload_bytes,
            "records_per_second": round(self.records_per_second, 2),
            "remaining_packets": self.remaining_packets,
        }


@dataclass(frozen=True, slots=True)
class DownloadProgress:
    """A normalized progress line measured from before PHY guard entry."""

    elapsed_seconds: float
    payload_bytes: int
    bytes_per_second: float
    records_per_second: float
    remaining_packets: int
    eta_seconds: float | None
    records_completed: int
    records_total: int
    state: str = "progress"
    retry_seconds: float | None = None
    operational_event: Mapping[str, object] | None = None
    phase: str | None = None
    error_type: str | None = None
    error_message: str | None = None
    reason: str | None = None
    duration_seconds: float | None = None
    next_attempt_in_seconds: float | None = None

    def as_dict(self) -> dict[str, object]:
        """Return progress fields suitable for one JSON line on stderr."""
        result: dict[str, object] = {
            "bytes_per_second": round(self.bytes_per_second, 2),
            "elapsed_seconds": round(self.elapsed_seconds, 2),
            "eta_seconds": round(self.eta_seconds, 2) if self.eta_seconds is not None else None,
            "payload_bytes": self.payload_bytes,
            "records_completed": self.records_completed,
            "records_per_second": round(self.records_per_second, 2),
            "records_total": self.records_total,
            "remaining_packets": self.remaining_packets,
            "status": self.state,
        }
        if self.retry_seconds is not None:
            result["retry_seconds"] = round(self.retry_seconds, 2)
        if self.operational_event is not None:
            result.update(self.operational_event)
        if self.phase is not None:
            result["phase"] = self.phase
        if self.error_type is not None:
            result["error_type"] = self.error_type
        if self.error_message is not None:
            result["error_message"] = self.error_message
        if self.reason is not None:
            result["reason"] = self.reason
        if self.duration_seconds is not None:
            result["duration_seconds"] = round(self.duration_seconds, 2)
        if self.next_attempt_in_seconds is not None:
            result["next_attempt_in_seconds"] = round(self.next_attempt_in_seconds, 2)
        return result


type ProgressReporter = Callable[[DownloadProgress], object]
type OperationalProgressReporter = Callable[[Mapping[str, object]], object]


class OperationInterruptedError(RuntimeError):
    """Raised after a SIGTERM-cancelled operation has completed its cleanup."""


def make_guard(adapter: str) -> ScopedPhyGuard:
    """Build the production scoped-PHY guard; tests monkeypatch this seam."""
    return ScopedPhyGuard(adapter)


def make_transport(  # noqa: PLR0913
    address: str,
    *,
    device_selector: Device | None = None,
    att_mtu_query_timeout_seconds: float = DEFAULT_CONFIG.ble.att_mtu_query_timeout_seconds,
    adapter: str = DEFAULT_CONFIG.ble.adapter_name,
    phy_policy: str = "auto",
    local_name: str | None = None,
    link_terminal_callback: Callable[[dict[str, object]], object] | None = None,
    debug_logger: logging.Logger | None = None,
) -> BleakRingTransport:
    """Build a transport, preferring the exact scanner device when available."""
    transport_options: dict[str, object] = {
        "att_mtu_query_timeout_seconds": att_mtu_query_timeout_seconds,
    }
    if adapter != DEFAULT_CONFIG.ble.adapter_name:
        transport_options["adapter"] = adapter
    if phy_policy != "auto":
        transport_options["phy_policy"] = phy_policy
    if local_name is not None:
        transport_options["local_name"] = local_name
    if link_terminal_callback is not None:
        transport_options["link_terminal_callback"] = link_terminal_callback
    if debug_logger is not None:
        transport_options["debug_logger"] = debug_logger
    if device_selector is None:
        return BleakRingTransport(address, **transport_options)  # type: ignore[reportArgumentType]
    return BleakRingTransport(address, device_selector=device_selector, **transport_options)  # type: ignore[reportArgumentType]


def make_presence_scheduler(
    address: str,
    adapter: str,
    *,
    policy: PresencePolicy | None = None,
    config: CollectorConfig = DEFAULT_CONFIG,
) -> PresenceScheduler:
    """Build the production exact-address observer and monotonic scheduler."""
    if policy is None:
        policy = PresencePolicy(
            absence_seconds=config.presence.absence_seconds,
            fallback_seconds=config.presence.fallback_seconds,
            drained_fallback_seconds=config.presence.drained_fallback_seconds,
            rapid_backoff=config.retry.rapid_backoff,
            scan_transition_seconds=config.presence.scan_transition_seconds,
        )
    return PresenceScheduler(BleakPresenceObserver(address, adapter), policy=policy)


async def phy_check(adapter: str) -> None:
    """Enter and exit the scoped guard, proving its restore path runs."""
    _require_supported_adapter(adapter)
    async with make_guard(adapter):
        pass


async def recover_phy(adapter: str) -> None:
    """Recover an orphaned PHY marker without contacting a pendant."""
    _require_supported_adapter(adapter)
    await make_guard(adapter).recover()


async def probe(address: str, adapter: str) -> RingStatus:
    """Read cached safe ring status while scoped to LE 1M."""
    _require_supported_adapter(adapter)
    async with make_guard(adapter), make_transport(address) as session:
        return await collector.probe(session)
    raise RuntimeError("device session ended without a probe result")


async def info(address: str, adapter: str) -> RingInfo:
    """Request safe ring sequence metadata while scoped to LE 1M."""
    _require_supported_adapter(adapter)
    async with make_guard(adapter), make_transport(address) as session:
        return await collector.ring_info(session, timeout=TRANSFER_TIMEOUTS.info)
    raise RuntimeError("device session ended without an INFO result")


async def collect(
    address: str,
    adapter: str,
    device_slug: str,
    staging: StagingStore,
    max_records: int,
) -> collector.CollectResult:
    """Persist one deliberately bounded READ batch through the coordinator."""
    _require_supported_adapter(adapter)
    _validate_max_records(max_records)

    def provider(candidate: object | None):
        return _guarded_transport(adapter, address, device_selector=cast(BLEDevice | None, candidate))

    options = OpportunisticOptions(
        timeouts=TRANSFER_TIMEOUTS,
        policy=RetryPolicy(
            batch_records=max_records,
            arena_max_bytes=max_records * collector.RECORD_SIZE,
            advance_enabled=False,
            stop_after_drained=True,
        ),
        quality_metrics=_quality_metrics(staging, DEFAULT_CONFIG),
        phy_policy="force_1m",
    )
    return await run_opportunistic_collector(provider, staging, device_slug, options, runtime=OpportunisticRuntime())


# The public seam keeps address, adapter, and progress arguments compatible.
async def sync(  # noqa: PLR0913
    address: str,
    adapter: str,
    device_slug: str,
    staging: StagingStore,
    progress: ProgressReporter | None = None,
    *,
    force_1m: bool = False,
    presence: PresenceScheduler | None = None,
    link_terminal_callback: Callable[[dict[str, object]], object] | None = None,
    debug_logger: logging.Logger | None = None,
    config: CollectorConfig = DEFAULT_CONFIG,
) -> collector.CollectResult:
    """Run until cancelled, draining RAM-admitted batches while the pendant is near.

    A fresh transport is created for each presence session. ``force_1m`` scopes
    the controller-wide guard to that single connection, never the away period.
    """
    _require_supported_adapter(adapter)

    def provider(candidate: object | None):
        # Reusing a disconnected Bleak client can retain stale subscriptions.
        if force_1m:
            return _guarded_transport(
                adapter,
                address,
                device_selector=cast(BLEDevice | None, candidate),
                phy_policy="force_1m",
                local_name=device_slug,
                link_terminal_callback=link_terminal_callback,
                debug_logger=debug_logger,
                att_mtu_query_timeout_seconds=config.ble.att_mtu_query_timeout_seconds,
            )
        if candidate is not None:
            return make_transport(
                address,
                device_selector=cast(BLEDevice | None, candidate),
                adapter=adapter,
                phy_policy="auto",
                local_name=device_slug,
                link_terminal_callback=link_terminal_callback,
                debug_logger=debug_logger,
                att_mtu_query_timeout_seconds=config.ble.att_mtu_query_timeout_seconds,
            )
        # The bounded fallback may run without a scanner candidate.  Keep the
        # explicit address path as a safety net; the active scanner restarts
        # after this attempt so it does not become the normal discovery path.
        return make_transport(
            address,
            adapter=adapter,
            phy_policy="auto",
            local_name=device_slug,
            link_terminal_callback=link_terminal_callback,
            debug_logger=debug_logger,
            att_mtu_query_timeout_seconds=config.ble.att_mtu_query_timeout_seconds,
        )

    retry_policy = (
        RetryPolicy(
            backoff=presence.policy.rapid_backoff,
            drain_cooldown_seconds=presence.policy.drained_fallback_seconds,
        )
        if presence is not None
        else RetryPolicy(backoff=config.retry.rapid_backoff, drain_cooldown_seconds=config.presence.fallback_seconds)
    )
    options = OpportunisticOptions(
        timeouts=collector.TransferTimeouts(
            info=config.transfer.info_timeout_seconds,
            transfer=config.transfer.sync_timeout_seconds,
        ),
        policy=retry_policy,
        progress=_progress_callback(progress),
        activity=_activity_callback(progress),
        operational=_operational_callback(progress),
        presence=presence,
        quality_metrics=_quality_metrics(staging, config, debug_logger),
        phy_policy="force_1m" if force_1m else "auto",
        config=config,
    )
    return await run_opportunistic_collector(provider, staging, device_slug, options, runtime=OpportunisticRuntime())


def _quality_metrics(
    staging: StagingStore, config: CollectorConfig, debug_logger: logging.Logger | None = None
) -> JsonlQualityMetrics | None:
    """Build auxiliary evidence storage without making capture depend on it."""
    from .adapters.debug_logging import debug_exception

    try:
        return JsonlQualityMetrics(
            staging.paths.root,
            release_version=package_version(),
            source_revision=source_revision_from_environment(config=config.observability.quality_metrics),
            config=config.observability.quality_metrics,
            diagnostic_logger=debug_logger,
        )
    except Exception as error:  # noqa: BLE001 - provenance/journal setup is auxiliary
        debug_exception("quality_metrics_configuration_error", error, logger=debug_logger)
        return None


def _progress_callback(progress: ProgressReporter | None) -> collector.ProgressCallback | None:
    if progress is None:
        return None

    async def report(event: collector.ProgressEvent) -> None:
        elapsed = max(0.0, event.elapsed)
        records = max(0, event.records_completed)
        total = max(records, event.records_total)
        update = DownloadProgress(
            elapsed,
            max(0, event.bytes_completed),
            max(0.0, event.bytes_per_second),
            max(0.0, event.records_per_second),
            max(0, total - records),
            event.eta,
            records,
            total,
        )
        result = progress(update)
        if inspect.isawaitable(result):
            await result

    return report


def _activity_callback(progress: ProgressReporter | None) -> Callable[[ActivityEvent], object] | None:
    if progress is None:
        return None

    async def report(event: ActivityEvent) -> None:
        result = progress(
            DownloadProgress(
                0.0,
                0,
                0.0,
                0.0,
                0,
                None,
                0,
                0,
                state=event.state,
                retry_seconds=event.retry_seconds,
                phase=event.phase,
                error_type=event.error_type,
                error_message=event.error_message,
                reason=event.reason,
                duration_seconds=event.duration_seconds,
                next_attempt_in_seconds=event.next_attempt_in_seconds,
            )
        )
        if inspect.isawaitable(result):
            await result

    return report


def _operational_callback(progress: ProgressReporter | None) -> OperationalProgressReporter | None:
    if progress is None:
        return None

    async def report(event: Mapping[str, object]) -> None:
        result = progress(
            DownloadProgress(0.0, 0, 0.0, 0.0, 0, None, 0, 0, state="operational", operational_event=event)
        )
        if inspect.isawaitable(result):
            await result

    return report


@asynccontextmanager
async def _guarded_transport(  # noqa: PLR0913
    adapter: str,
    address: str,
    *,
    device_selector: BLEDevice | None = None,
    att_mtu_query_timeout_seconds: float = DEFAULT_CONFIG.ble.att_mtu_query_timeout_seconds,
    phy_policy: str = "auto",
    local_name: str | None = None,
    link_terminal_callback: Callable[[dict[str, object]], object] | None = None,
    debug_logger: logging.Logger | None = None,
) -> AsyncIterator[RingSession]:
    """Apply weak-RF PHY fallback only while one BLE session is connected."""
    if device_selector is None:
        transport = make_transport(
            address,
            adapter=adapter,
            att_mtu_query_timeout_seconds=att_mtu_query_timeout_seconds,
            phy_policy=phy_policy,
            local_name=local_name,
            link_terminal_callback=link_terminal_callback,
            debug_logger=debug_logger,
        )
    else:
        transport = make_transport(
            address,
            device_selector=device_selector,
            adapter=adapter,
            att_mtu_query_timeout_seconds=att_mtu_query_timeout_seconds,
            phy_policy=phy_policy,
            local_name=local_name,
            link_terminal_callback=link_terminal_callback,
            debug_logger=debug_logger,
        )
    async with make_guard(adapter), transport as session:
        yield session


def download_metrics(result: collector.CollectResult, elapsed_seconds: float) -> DownloadMetrics:
    """Compute finite end-to-end metrics for a completed download."""
    elapsed = max(0.0, elapsed_seconds)
    if isinstance(result, collector.NoDataResult):
        records = 0
        remaining = 0
    else:
        records = max(0, result.packet_count)
        remaining = max(0, result.info.unread_packets - records)
    payload_bytes = records * collector.RECORD_SIZE
    records_per_second = records / elapsed if elapsed > 0 else 0.0
    bytes_per_second = payload_bytes / elapsed if elapsed > 0 else 0.0
    eta = remaining / records_per_second if records_per_second > 0 else 0.0
    return DownloadMetrics(elapsed, payload_bytes, bytes_per_second, records_per_second, remaining, eta)


def run[T](operation: Coroutine[object, object, T]) -> T:
    """Run an operational coroutine and restore scoped state when SIGTERM arrives."""
    if sys.platform == "win32":
        return asyncio.run(operation)
    return asyncio.run(_run_posix_operation(operation))


async def _run_posix_operation[T](operation: Coroutine[object, object, T]) -> T:
    """Cancel the active task on SIGTERM so async context managers can exit."""
    terminated = False
    task = asyncio.create_task(operation)

    def cancel_for_sigterm() -> None:
        nonlocal terminated
        terminated = True
        task.cancel()

    loop = asyncio.get_running_loop()
    loop.add_signal_handler(signal.SIGTERM, cancel_for_sigterm)
    try:
        return await task
    except asyncio.CancelledError as error:
        if terminated:
            detail = f"; {error}" if isinstance(error, CollectionPreservedCancelledError) else ""
            raise OperationInterruptedError(f"operation interrupted by SIGTERM after cleanup{detail}") from error
        raise
    finally:
        loop.remove_signal_handler(signal.SIGTERM)


def render_status(status: RingStatus) -> str:
    """Render only non-audio cached status fields."""
    return _json(
        {
            "free_bytes": status.free_bytes,
            "rtc_valid": status.has_valid_rtc,
            "status": "ok",
            "unread_packets": status.unread_packets,
            "used_bytes": status.used_bytes,
        }
    )


def render_info(info_result: RingInfo) -> str:
    """Render safe ring sequence and capacity metadata."""
    return _json(
        {
            "capacity_packets": info_result.capacity_packets,
            "dropped_packets": info_result.dropped_packets,
            "packet_size": info_result.packet_size,
            "read_sequence": info_result.read_sequence,
            "status": "ok",
            "unread_packets": info_result.unread_packets,
            "write_sequence": info_result.write_sequence,
        }
    )


def render_collect(result: collector.CollectResult, metrics: DownloadMetrics | None = None) -> str:
    """Render sealed-bundle metadata, never ring-record or audio bytes."""
    if isinstance(result, collector.NoDataResult):
        payload: dict[str, object] = {
            "read_sequence": result.info.read_sequence,
            "status": "no_data",
            "unread_packets": result.info.unread_packets,
            "write_sequence": result.info.write_sequence,
        }
        if metrics is not None:
            payload.update(metrics.as_dict())
        return _json(payload)
    seal = _require_seal(result)
    manifest = _bundle_manifest(seal.bundle_path)
    payload = {
        "bundle_path": str(seal.bundle_path),
        "deduplicated": seal.deduplicated,
        "next_sequence": result.info.read_sequence + result.packet_count,
        "raw_sha256": manifest.get("raw_sha256"),
        "record_count": result.packet_count,
        "start_sequence": result.info.read_sequence,
        "status": "sealed",
    }
    if metrics is not None:
        payload.update(metrics.as_dict())
    return _json(payload)


def render_sync(result: collector.CollectResult, metrics: DownloadMetrics) -> str:
    """Render sync receipt metadata and finite end-to-end transfer metrics."""
    if isinstance(result, collector.NoDataResult):
        payload: dict[str, object] = {
            "read_sequence": result.info.read_sequence,
            "status": "no_data",
            "unread_packets": result.info.unread_packets,
            "write_sequence": result.info.write_sequence,
        }
    else:
        seal = _require_seal(result)
        manifest = _bundle_manifest(seal.bundle_path)
        payload = {
            "advance_confirmed": result.advance_confirmed,
            "bundle_path": str(seal.bundle_path),
            "deduplicated": seal.deduplicated,
            "next_sequence": result.next_sequence,
            "raw_sha256": manifest.get("raw_sha256"),
            "record_count": result.packet_count,
            "start_sequence": result.info.read_sequence,
            "status": "synced",
        }
    payload.update(metrics.as_dict())
    return _json(payload)


def _validate_max_records(max_records: int) -> None:
    if not 1 <= max_records <= MAX_RECORDS:
        raise ValueError(f"max_records must be between 1 and {MAX_RECORDS}")


def _require_seal(result: collector.CollectionResult) -> SealResult:
    seal = result.seal
    if not isinstance(seal, SealResult):
        raise RuntimeError("sealed bundle metadata is unavailable")
    return seal


def _require_supported_adapter(adapter: str) -> None:
    if adapter != SUPPORTED_ADAPTER:
        raise ValueError("only --adapter hci0 is supported until adapter selection is implemented")


def _bundle_manifest(bundle_path: Path) -> dict[str, object]:
    try:
        loaded = cast(object, json.loads((bundle_path / "manifest.json").read_text(encoding="utf-8")))
    except (OSError, json.JSONDecodeError) as error:
        raise RuntimeError("sealed bundle metadata could not be read") from error
    if not isinstance(loaded, dict) or not isinstance(loaded.get("raw_sha256"), str):
        raise RuntimeError("sealed bundle metadata is malformed")
    return loaded


def _json(payload: dict[str, object]) -> str:
    return json.dumps(payload, sort_keys=True)
