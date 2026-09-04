from __future__ import annotations

import json
import logging
import time
from collections.abc import Callable, Coroutine, Mapping
from enum import StrEnum
from pathlib import Path
from typing import TYPE_CHECKING, Annotated, ClassVar, Protocol, cast

import typer

from omi_collector.config import DEFAULT_CONFIG
from omi_collector.core import package_version

if TYPE_CHECKING:
    from omi_collector.capture.adapters.staging_store import StagingStore
    from omi_collector.capture.cli import DownloadProgress
    from omi_collector.storage_layout import StorageLayout


class _CaptureCli(Protocol):
    """Runtime capture command surface loaded only for a device operation."""

    MAX_RECORDS: int
    SUPPORTED_ADAPTER: str
    OperationInterruptedError: type[BaseException]

    def run(self, operation: Coroutine[object, object, object]) -> object: ...

    def phy_check(self, adapter: str) -> Coroutine[object, object, object]: ...

    def recover_phy(self, adapter: str) -> Coroutine[object, object, object]: ...

    def probe(self, address: str, adapter: str) -> Coroutine[object, object, object]: ...

    def info(self, address: str, adapter: str) -> Coroutine[object, object, object]: ...

    def collect(self, *args: object) -> Coroutine[object, object, object]: ...

    def sync(self, *args: object, **kwargs: object) -> Coroutine[object, object, object]: ...

    def make_presence_scheduler(self, address: str, adapter: str) -> object: ...

    def download_metrics(self, result: object, elapsed: float) -> object: ...

    def render_status(self, result: object) -> str: ...

    def render_info(self, result: object) -> str: ...

    def render_collect(self, result: object, metrics: object) -> str: ...

    def render_sync(self, result: object, metrics: object) -> str: ...


class _JsonResult(Protocol):
    def as_dict(self) -> dict[str, object]: ...


app = typer.Typer(help="Bounded pendant collection tools.")
device = typer.Typer(help="Safely inspect and collect bounded Omi ring batches.")
app.add_typer(device, name="device")

# Test seams remain unloaded until their command is selected.  A production
# import of this module therefore does not pull in Bluetooth code.
collect_spool_metrics: object | None = None


def _capture_cli() -> _CaptureCli:
    """Load capture composition only when a device command is selected."""
    from omi_collector.capture.entrypoint import cli

    return cast(_CaptureCli, cli)


def _load_layout(path: Path) -> StorageLayout:
    from omi_collector.storage_layout import StorageLayoutError, load_storage_layout

    try:
        return load_storage_layout(path)
    except StorageLayoutError as error:
        typer.echo(str(error), err=True)
        raise typer.Exit(code=2) from error


def _staging(layout: StorageLayout) -> StagingStore:
    from omi_collector.capture.adapters.staging_filesystem import StagingPaths
    from omi_collector.capture.adapters.staging_store import StagingStore

    # Layout is validated by ``_load_layout`` before this helper is called.
    return StagingStore.from_paths(
        StagingPaths(
            layout.collector.root,
            layout.publication.root,
            layout.collector.attempts,
            layout.collector.quarantine,
            layout.collector.lock,
            layout.collector.device_state,
        )
    )


class SyncLogLevel(StrEnum):
    """The small, command-local verbosity surface for long-lived sync."""

    INFO = "INFO"
    DEBUG = "DEBUG"


class SyncProgressReporter:
    """Classify sync callbacks before they reach the system journal.

    ``device_cli`` deliberately reports lifecycle callbacks rather than
    logging itself.  Keeping this state at the CLI boundary means the hot
    DATA path stays untouched and the machine-readable lines remain backward
    compatible.  A READ callback is already one aggregate leg, so INFO emits
    only the first such callback; DEBUG can still inspect later legs.
    """

    _DEBUG_STATES: ClassVar[frozenset[str]] = frozenset({"away", "connecting", "drained"})
    _HEALTHY_CLOCK_OUTCOMES: ClassVar[frozenset[str]] = frozenset({"within_threshold", "verified"})
    _OBSERVATION_SIGNATURE_FIELDS: ClassVar[tuple[str, ...]] = (
        "battery_percent",
        "rtc_valid",
        "model",
        "firmware",
        "hardware",
        "manufacturer",
        "capacity_packets",
        "packet_size",
        "dropped_packets",
        "optional_outcomes",
    )

    def __init__(
        self,
        level: SyncLogLevel = SyncLogLevel.INFO,
        *,
        emit: Callable[[str], object] | None = None,
        debug_logger: logging.Logger | None = None,
    ) -> None:
        self.level = level
        self._emit = emit or (lambda line: typer.echo(line, err=True))
        self._debug_logger = debug_logger
        self._session_error_keys: set[tuple[str, str, str | None]] = set()
        self._session_error_key: tuple[str, str, str | None] | None = None
        self._progress_emitted = False
        self._observation_signature: tuple[str, ...] | None = None

    def __call__(self, progress: DownloadProgress) -> None:
        from omi_collector.capture.adapters.debug_logging import debug_event

        """Persist every aggregate callback before journal visibility filtering."""
        debug_event("sync_progress", logger=self._debug_logger, progress=progress.as_dict())
        state = progress.state
        if state == "session_error":
            self._report_session_error(progress)
            return

        if state == "reading":
            self._progress_emitted = False

        if self._is_successful_read_or_operation(progress):
            self._report_recovery()

        if not self._visible(progress):
            return
        self._emit_json(progress.as_dict())

    def report_ble_link(self, record: dict[str, object]) -> None:
        """Emit the observer's single terminal record at the selected sink."""
        public_record = dict(record)
        public_record.pop("address", None)
        self._emit_json(public_record)

    def _report_session_error(self, progress: DownloadProgress) -> None:
        error_type = progress.error_type or "unknown"
        error_message = progress.error_message or "session operation failed"
        key = (error_type, error_message, progress.phase)
        self._session_error_key = key
        if self.level is not SyncLogLevel.DEBUG and key in self._session_error_keys:
            return
        self._session_error_keys.add(key)
        self._emit_json(progress.as_dict())

    def _report_recovery(self) -> None:
        if self._session_error_key is None:
            return
        error_type, _, _ = self._session_error_key
        self._emit_json({"recovered_from": error_type, "status": "recovered"})
        self._session_error_keys.clear()
        self._session_error_key = None

    def _is_successful_read_or_operation(self, progress: DownloadProgress) -> bool:
        if progress.state in {"drained", "progress", "batch_complete"}:
            return True
        if progress.state != "operational" or progress.operational_event is None:
            return False
        event = progress.operational_event
        event_name = event.get("event")
        if event_name == "pendant_observation":
            return True
        return event_name == "pendant_clock_sync" and event.get("outcome") in self._HEALTHY_CLOCK_OUTCOMES

    def _visible(self, progress: DownloadProgress) -> bool:
        if self.level is SyncLogLevel.DEBUG:
            return True
        if progress.state == "fatal":
            return True
        if progress.state == "reading":
            return False
        if progress.state == "progress":
            return self._first_progress_visible()
        if progress.state == "operational":
            return self._operational_visible(progress)
        return progress.state not in self._DEBUG_STATES

    def _first_progress_visible(self) -> bool:
        if self._progress_emitted:
            return False
        self._progress_emitted = True
        return True

    def _operational_visible(self, progress: DownloadProgress) -> bool:
        event = progress.operational_event or {}
        event_name = event.get("event")
        if event_name == "pendant_observation":
            signature = self._observation_health_signature(event)
            if signature == self._observation_signature:
                return False
            self._observation_signature = signature
            return True
        return not (event_name == "pendant_clock_sync" and event.get("outcome") == "within_threshold")

    def _observation_health_signature(self, event: Mapping[str, object]) -> tuple[str, ...]:
        return tuple(
            json.dumps(event.get(field), sort_keys=True, separators=(",", ":"))
            for field in self._OBSERVATION_SIGNATURE_FIELDS
        )

    def _emit_json(self, payload: dict[str, object]) -> None:
        self._emit(json.dumps(payload, sort_keys=True, separators=(",", ":")))


@app.callback(invoke_without_command=True)
def main(
    version: Annotated[bool, typer.Option("--version", help="Show the installed version and exit.")] = False,
) -> None:
    if version:
        typer.echo(package_version())
        raise typer.Exit


@app.command()
def health() -> None:
    typer.echo("ok")


@app.command()
def serve(
    interval_seconds: Annotated[
        int, typer.Option("--interval-seconds", min=1)
    ] = DEFAULT_CONFIG.service.interval_seconds,
) -> None:
    while True:
        time.sleep(interval_seconds)


@device.command("phy-check")
def device_phy_check(
    adapter: Annotated[
        str, typer.Option("--adapter", help="Bluetooth adapter to scope temporarily.")
    ] = DEFAULT_CONFIG.ble.adapter_name,
    confirm_host_change: Annotated[
        bool,
        typer.Option("--confirm-host-change", help="Acknowledge the temporary controller-wide PHY change."),
    ] = False,
) -> None:
    """Enter and restore the PHY guard without connecting a pendant."""
    _require_confirmation(confirm_host_change, "--confirm-host-change")
    _run_device_operation(_capture_cli().phy_check(adapter))
    typer.echo('{"status": "phy_restored"}')


@device.command("recover-phy")
def device_recover_phy(
    adapter: Annotated[
        str, typer.Option("--adapter", help="Bluetooth adapter with a stale PHY marker.")
    ] = DEFAULT_CONFIG.ble.adapter_name,
) -> None:
    """Restore a stale PHY snapshot from an interrupted Omi operation."""
    _run_device_operation(_capture_cli().recover_phy(adapter))
    typer.echo('{"status": "phy_recovered"}')


@device.command("probe")
def device_probe(
    address: Annotated[str, typer.Option("--address", help="Omi BLE address to connect to.")],
    adapter: Annotated[
        str, typer.Option("--adapter", help="Bluetooth adapter to scope temporarily.")
    ] = DEFAULT_CONFIG.ble.adapter_name,
    confirm_host_change: Annotated[
        bool,
        typer.Option("--confirm-host-change", help="Acknowledge the temporary controller-wide PHY change."),
    ] = False,
) -> None:
    """Read cached ring status without issuing a ring control command."""
    _require_confirmation(confirm_host_change, "--confirm-host-change")
    result = _run_device_operation(_capture_cli().probe(address, adapter))
    typer.echo(_capture_cli().render_status(cast(object, result)))


@device.command("info")
def device_info(
    address: Annotated[str, typer.Option("--address", help="Omi BLE address to connect to.")],
    adapter: Annotated[
        str, typer.Option("--adapter", help="Bluetooth adapter to scope temporarily.")
    ] = DEFAULT_CONFIG.ble.adapter_name,
    confirm_host_change: Annotated[
        bool,
        typer.Option("--confirm-host-change", help="Acknowledge the temporary controller-wide PHY change."),
    ] = False,
) -> None:
    """Read safe ring sequence metadata without requesting audio records."""
    _require_confirmation(confirm_host_change, "--confirm-host-change")
    result = _run_device_operation(_capture_cli().info(address, adapter))
    typer.echo(_capture_cli().render_info(cast(object, result)))


@device.command("collect")
def device_collect(
    *,
    address: Annotated[str, typer.Option("--address", help="Omi BLE address to connect to.")],
    device_slug: Annotated[
        str, typer.Option("--device-slug", help="Stable local directory component for this pendant.")
    ],
    layout_path: Annotated[Path, typer.Option("--layout", help="Storage-layout TOML authority.")],
    max_records: Annotated[
        int,
        typer.Option("--max-records", min=1, max=DEFAULT_CONFIG.service.max_records, help="Bounded READ size."),
    ] = DEFAULT_CONFIG.service.default_collect_records,
    confirm_read: Annotated[
        bool,
        typer.Option(
            "--confirm-read",
            help="Acknowledge a consuming READ and the temporary controller-wide PHY change.",
        ),
    ] = False,
) -> None:
    """Durably stage a bounded READ; output contains bundle metadata only."""
    _require_confirmation(confirm_read, "--confirm-read")
    layout = _load_layout(layout_path)
    staging = _staging(layout)
    _preflight_storage(staging)
    started = time.monotonic()
    result = _run_device_operation(
        _capture_cli().collect(
            address,
            _capture_cli().SUPPORTED_ADAPTER,
            device_slug,
            staging,
            max_records,
        )
    )
    metrics = _capture_cli().download_metrics(cast(object, result), max(0.0, time.monotonic() - started))
    typer.echo(_capture_cli().render_collect(cast(object, result), metrics))


@device.command("sync")
def device_sync(  # noqa: PLR0913
    *,
    address: Annotated[str, typer.Option("--address", help="Omi BLE address to connect to.")],
    device_slug: Annotated[
        str, typer.Option("--device-slug", help="Stable local directory component for this pendant.")
    ],
    layout_path: Annotated[Path, typer.Option("--layout", help="Storage-layout TOML authority.")],
    confirm_sync: Annotated[
        bool,
        typer.Option(
            "--confirm-sync",
            "--confirm-read-advance",
            help="Acknowledge consuming READ+ADVANCE; --force-1m additionally changes controller PHY policy.",
        ),
    ] = False,
    force_1m: Annotated[
        bool,
        typer.Option(
            "--force-1m",
            help="Fallback: temporarily force controller-wide LE 1M for weak RF, then restore it.",
        ),
    ] = False,
    log_level: Annotated[
        SyncLogLevel,
        typer.Option(
            "--log-level",
            help="Progress verbosity for the long-lived sync: INFO or DEBUG.",
        ),
    ] = SyncLogLevel.INFO,
    announce_readiness: Annotated[
        bool,
        typer.Option(
            "--announce-readiness",
            help="Emit one startup record after the storage layout is loaded.",
        ),
    ] = False,
) -> None:
    """Sync using normal PHY negotiation, or explicit temporary LE 1M fallback."""
    _require_confirmation(confirm_sync, "--confirm-sync")
    layout = _load_layout(layout_path)
    staging = _staging(layout)
    _preflight_storage(staging)
    if announce_readiness:
        typer.echo(
            json.dumps(
                {"layout": str(layout.path), "status": "deployment_ready"},
                sort_keys=True,
                separators=(",", ":"),
            ),
            err=True,
        )
    started = time.monotonic()
    from omi_collector.capture.adapters.debug_logging import (
        close_debug_logging,
        configure_debug_logging,
        debug_exception,
    )

    debug_logger = configure_debug_logging(layout.collector.root, file_name=layout.collector.debug_log.name)
    try:
        report_progress = SyncProgressReporter(log_level, debug_logger=debug_logger)
        presence = _capture_cli().make_presence_scheduler(address, _capture_cli().SUPPORTED_ADAPTER)
        result = _run_device_operation(
            _capture_cli().sync(
                address,
                _capture_cli().SUPPORTED_ADAPTER,
                device_slug,
                staging,
                report_progress,
                force_1m=force_1m,
                presence=presence,
                link_terminal_callback=report_progress.report_ble_link,
                debug_logger=debug_logger,
            )
        )
        metrics = _capture_cli().download_metrics(cast(object, result), max(0.0, time.monotonic() - started))
        typer.echo(_capture_cli().render_sync(cast(object, result), metrics))
    except Exception as error:
        debug_exception("sync_command_failed", error, logger=debug_logger, device_slug=device_slug)
        raise
    finally:
        close_debug_logging(debug_logger)


@device.command("metrics")
def device_metrics(
    *,
    layout_path: Annotated[Path, typer.Option("--layout", help="Storage-layout TOML authority.")],
    device_slug: Annotated[
        str, typer.Option("--device-slug", help="Stable local directory component for this pendant.")
    ],
) -> None:
    """Report metrics for the source bundles currently visible to the collector."""
    from omi_collector.spool_metrics import SpoolMetricsError

    try:
        layout = _load_layout(layout_path)
        metrics = collect_spool_metrics
        if metrics is None:
            from omi_collector.spool_metrics import collect_spool_metrics as metrics
        result = cast(Callable[..., object], metrics)(
            layout.publication.root,
            device_slug,
            observation_root=layout.collector.device_state,
        )
    except SpoolMetricsError as error:
        typer.echo(str(error), err=True)
        raise typer.Exit(code=1) from error
    typer.echo(json.dumps(cast(_JsonResult, result).as_dict(), sort_keys=True, separators=(",", ":")))


def _require_confirmation(confirmed: bool, flag: str) -> None:
    if not confirmed:
        typer.echo(f"refusing operation without {flag}", err=True)
        raise typer.Exit(code=2)


def _preflight_storage(staging: StagingStore) -> None:
    """Fail before BLE composition when local storage cannot safely accept a READ."""
    from omi_collector.capture.adapters.staging_contract import StagingError

    try:
        staging.preflight_storage()
    except StagingError as error:
        typer.echo(f"storage preflight failed: {error}", err=True)
        raise typer.Exit(code=1) from error


def _run_device_operation(operation: Coroutine[object, object, object]) -> object:
    capture_cli = _capture_cli()
    try:
        return capture_cli.run(operation)
    except capture_cli.OperationInterruptedError as error:
        typer.echo(str(error), err=True)
        raise typer.Exit(code=0) from error
    except Exception as error:  # User-facing boundary; BaseException remains unsuppressed.
        typer.echo(f"device operation failed: {type(error).__name__}", err=True)
        raise typer.Exit(code=1) from error
