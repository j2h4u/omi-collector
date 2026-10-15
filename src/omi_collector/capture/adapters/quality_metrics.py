"""Best-effort bounded quality events stored in the Collector state database."""

from __future__ import annotations

import json
import logging
import queue
import re
import sys
from collections.abc import Callable, Iterable, Mapping
from datetime import UTC, datetime
from pathlib import Path
from threading import Lock, Thread
from typing import cast
from uuid import uuid4

from ...config import DEFAULT_CONFIG, QualityMetricsConfig
from ..application.quality_metrics import (
    AdvertisementMetric,
    ClockCorrectionMetric,
    QualityMetricsPort,
    SequenceLossMetric,
    TransferSessionMetric,
)
from ..application.storage_fault import FatalInternalAudioStorageError
from ..domain.operational_status_machine import OperationalSignal
from ..domain.ring_protocol import RECORD_SIZE
from .collector_state_db import StateDB, StateDBError, StateKind, StateTransaction

_SCHEMA_VERSION = 2
_REVISION = re.compile(r"[0-9a-f]{40,64}")
_SOURCE_REVISION = re.compile(r"[0-9a-f]{12}\Z")
_LOSS_ID = re.compile(r"[0-9a-f]{64}\Z")
_RELEASE_METADATA = Path("share/omi-collector/release.json")
_TERMINATION_CLASSES = frozenset({"cancelled", "completed", "fatal_error", "retryable_error", "teardown_interrupted"})
_ADVERTISEMENT_FIELDS = frozenset(
    {
        "schema_version",
        "event",
        "recorded_at",
        "session_id",
        "advertisement_rssi_dbm",
        "release_version",
        "source_revision",
        "phy_policy",
    }
)
_TRANSFER_FIELDS = frozenset(
    {
        "schema_version",
        "event",
        "completed_at",
        "session_id",
        "outcome",
        "termination_class",
        "active_read_elapsed_ms",
        "requested_record_count",
        "record_size_bytes",
        "received_raw_bytes",
        "submitted_raw_bytes",
        "written_raw_bytes",
        "release_version",
        "source_revision",
        "firmware_version",
        "phy_policy",
        "advertisement_rssi_dbm",
    }
)
_LOSS_FIELDS = frozenset(
    {
        "schema_version",
        "event",
        "occurred_at",
        "session_id",
        "missing_record_count",
        "missing_raw_bytes",
        "reason",
        "release_version",
        "source_revision",
        "firmware_version",
    }
)
_CLOCK_FIELDS = frozenset(
    {
        "schema_version",
        "event",
        "occurred_at",
        "session_id",
        "drift_seconds",
        "target_epoch",
        "boundary_sequence_min",
        "boundary_sequence_max",
        "release_version",
        "source_revision",
        "firmware_version",
    }
)


class QualityMetricsError(RuntimeError):
    """The evidence database could not accept a complete validated event."""


class SQLiteQualityMetrics(QualityMetricsPort):
    """Queue complete events while capture proceeds independently of SQLite."""

    def __init__(
        self,
        state_db: StateDB,
        *,
        release_version: str,
        source_revision: str | None = None,
        config: QualityMetricsConfig = DEFAULT_CONFIG.observability.quality_metrics,
        diagnostic_logger: logging.Logger | None = None,
    ) -> None:
        self._db = state_db
        self._max_bytes = config.max_bytes * (config.backup_count + 1)
        self._max_record_bytes = config.max_record_bytes
        self._encoding = config.encoding
        self._queue: queue.Queue[tuple[str, dict[str, object], str] | None] = queue.Queue(
            maxsize=config.queue_max_records
        )
        self._state_lock = Lock()
        self._closed = False
        self._stop_enqueued = False
        self._configured_pending = False
        self._block_pending = False
        self._dropped_records = 0
        self._shutdown_timeout = config.shutdown_join_seconds
        self._logger = diagnostic_logger or logging.getLogger(__name__)
        self._operational_signal: Callable[[OperationalSignal], None] | None = None
        self._fatal_callback: Callable[[FatalInternalAudioStorageError], None] | None = None
        self._fatal_error: FatalInternalAudioStorageError | None = None
        self.release_version = _require_release_version(release_version)
        self.source_revision = normalize_source_revision(source_revision)
        self._thread = Thread(target=self._run, name="omi-quality-metrics", daemon=True)
        self._thread.start()

    def set_operational_signal(self, callback: Callable[[OperationalSignal], None]) -> None:
        with self._state_lock:
            self._operational_signal = callback
            self._configured_pending = True

    def set_fatal_callback(self, callback: Callable[[FatalInternalAudioStorageError], None]) -> None:
        with self._state_lock:
            self._fatal_callback = callback
            fatal = self._fatal_error
        if fatal is not None:
            callback(fatal)

    def record_transfer_session(self, metric: TransferSessionMetric) -> None:
        self._enqueue(metric.as_dict())

    def record_advertisement(self, metric: AdvertisementMetric) -> None:
        self._enqueue(metric.as_dict())

    def record_sequence_loss(self, metric: SequenceLossMetric) -> None:
        self._enqueue(metric.as_dict())

    def record_clock_correction(self, metric: ClockCorrectionMetric) -> None:
        self._enqueue(metric.as_dict())

    def record_runtime_status(self, event: str, fields: dict[str, object], timestamp: str) -> bool:
        """Queue one current-status update without delaying its BLE callback."""
        if event not in {"sync_progress", "ble_link_rssi_observed"}:
            return False
        value: dict[str, object] = {"event": event, "fields": fields, "timestamp": timestamp}
        try:
            from .debug_logging import _validate_runtime_event

            _validate_runtime_event(value)
        except ValueError:
            return False
        with self._state_lock:
            if self._closed:
                return False
            try:
                self._queue.put_nowait(("runtime", value, timestamp))
            except queue.Full:
                return False
        return True

    def close(self, timeout_seconds: float | None = None) -> bool:
        """Drain and join the writer before its runtime lock can be released.

        The timeout is a warning threshold. Returning with the worker alive could
        let it mutate StateDB after the collector releases its invocation lock.
        """
        drop_reason: str | None = None
        with self._state_lock:
            self._closed = True
            timeout = self._shutdown_timeout if timeout_seconds is None else timeout_seconds
            if not self._stop_enqueued:
                while True:
                    try:
                        self._queue.put_nowait(None)
                        self._stop_enqueued = True
                        break
                    except queue.Full:
                        try:
                            self._queue.get_nowait()
                        except queue.Empty:
                            continue
                        self._block_pending = True
                        drop_reason = "shutdown_queue_overflow"
        if drop_reason is not None:
            self._record_drop(drop_reason)
        self._thread.join(timeout)
        if self._thread.is_alive():
            self._diagnose("shutdown_timeout")
            self._thread.join()
        if self._fatal_error is not None:
            raise self._fatal_error
        return not self._thread.is_alive()

    @property
    def dropped_records(self) -> int:
        with self._state_lock:
            return self._dropped_records

    def _enqueue(self, value: dict[str, object]) -> None:
        try:
            validate_quality_event(value)
            encoded = (
                json.dumps(value, ensure_ascii=False, separators=(",", ":"), allow_nan=False).encode(self._encoding)
                + b"\n"
            )
            if len(encoded) > self._max_record_bytes:
                raise QualityMetricsError("quality metric record exceeds configured limit")
            item_id = quality_event_id(value)
        except QualityMetricsError:
            raise
        except (TypeError, ValueError, UnicodeError) as error:
            raise QualityMetricsError("cannot encode or validate quality metrics") from error
        drop_reason: str | None = None
        with self._state_lock:
            if self._closed:
                drop_reason = "writer_closed"
            else:
                try:
                    self._queue.put_nowait(("quality", value, item_id))
                except queue.Full:
                    drop_reason = "queue_full"
        if drop_reason is not None:
            self._record_drop(drop_reason)

    def _run(self) -> None:
        while True:
            self._deliver_pending_signals()
            queued = self._queue.get()
            self._deliver_pending_signals()
            if queued is None:
                return
            try:
                if queued[0] == "quality":
                    self._put_quality_event(queued[2], queued[1])
                    self._signal(OperationalSignal.CLEAR)
                else:
                    from .debug_logging import write_runtime_status_event

                    event = cast(dict[str, object], queued[1])
                    write_runtime_status_event(
                        self._db,
                        cast(str, event["event"]),
                        cast(dict[str, object], event["fields"]),
                        cast(str, event["timestamp"]),
                    )
            except FatalInternalAudioStorageError as error:
                self._record_fatal(error)
                return
            except Exception as error:  # noqa: BLE001 - auxiliary storage must not kill collection
                if queued[0] == "quality":
                    self._signal(OperationalSignal.BLOCK)
                    self._diagnose("write_failed", error)
                else:
                    self._diagnose("runtime_status_write_failed", error)

    def _record_fatal(self, error: FatalInternalAudioStorageError) -> None:
        with self._state_lock:
            self._fatal_error = error
            callback = self._fatal_callback
        if callback is not None:
            callback(error)

    def _record_drop(self, reason: str) -> None:
        with self._state_lock:
            self._dropped_records += 1
        self._diagnose(reason)
        self._signal(OperationalSignal.BLOCK)

    def _signal(self, signal: OperationalSignal) -> None:
        if signal is OperationalSignal.BLOCK:
            with self._state_lock:
                self._block_pending = True
            return
        if signal is OperationalSignal.CLEAR and self._operational_signal is not None:
            self._deliver_pending_signals()
            try:
                self._operational_signal(signal)
            except Exception:  # noqa: BLE001 - status reporting must not affect collection
                return

    def _deliver_pending_signals(self) -> None:
        with self._state_lock:
            callback = self._operational_signal
            configured, blocked = self._configured_pending, self._block_pending
            self._configured_pending = self._block_pending = False
        if callback is None:
            return
        for signal, pending in (
            (OperationalSignal.CONFIGURED, configured),
            (OperationalSignal.BLOCK, blocked),
        ):
            if pending:
                try:
                    callback(signal)
                except Exception:  # noqa: BLE001 - status reporting must not affect collection
                    continue

    def _diagnose(self, event: str, error: BaseException | None = None) -> None:
        try:
            if error is None:
                self._logger.warning("quality metrics %s", event)
            else:
                self._logger.warning("quality metrics %s: %s", event, error)
        except Exception:  # noqa: BLE001 - diagnostics must not affect collection
            return

    def _put_quality_event(self, item_id: str, payload: dict[str, object]) -> None:
        with self._db.transaction() as transaction:
            existing = transaction.get(StateKind.QUALITY_EVENT, item_id)
            if existing is not None and existing != payload:
                raise QualityMetricsError("quality event ID conflicts with retained history")
            history_bytes = _history_budget(transaction)
            if existing is None:
                transaction.put(StateKind.QUALITY_EVENT, item_id, payload)
                history_bytes += _event_size(payload)
            history_bytes = _prune_quality_history(transaction, history_bytes, self._max_bytes)
            transaction.put(StateKind.QUALITY_EVENT_BUDGET, "current", {"value": history_bytes})


def read_quality_events(state_db: StateDB, start: datetime, end: datetime) -> tuple[dict[str, object], ...]:
    """Return validated retained events inside an inclusive UTC window."""
    if start.tzinfo is None or end.tzinfo is None:
        raise QualityMetricsError("quality window bounds must be timezone-aware")
    try:
        events = [
            (item_id, payload, validate_quality_event(payload))
            for item_id, payload in state_db.list(StateKind.QUALITY_EVENT)
        ]
    except (StateDBError, ValueError, TypeError) as error:
        raise QualityMetricsError("quality event history is invalid or unavailable") from error
    lower, upper = start.astimezone(UTC), end.astimezone(UTC)
    return tuple(
        payload
        for _, payload, timestamp in sorted(events, key=lambda item: (item[2], item[0]))
        if lower <= timestamp <= upper
    )


def import_quality_event(
    state_db: StateDB,
    row: Mapping[str, object],
    *,
    item_id: str,
    max_bytes: int = 32 * 1024 * 1024,
) -> None:
    """Import one legacy event with its caller-assigned stable, sortable identity."""
    payload = dict(row)
    timestamp = validate_quality_event(payload)
    if not item_id.startswith(_quality_timestamp_key(timestamp) + "|"):
        raise QualityMetricsError("quality event ID must sort by its UTC timestamp")
    import_quality_events(state_db, ((item_id, payload),), max_bytes=max_bytes)


def import_quality_events(
    state_db: StateDB,
    rows: Iterable[tuple[str, Mapping[str, object]]],
    *,
    max_bytes: int = 32 * 1024 * 1024,
) -> None:
    """Import validated legacy events in one transaction, then prune once."""
    prepared = _prepare_quality_import_rows(rows)
    if type(max_bytes) is not int or max_bytes < 0:
        raise QualityMetricsError("quality history budget is invalid")
    with state_db.transaction() as transaction:
        history_bytes = _history_budget(transaction)
        history_bytes = _insert_quality_import_rows(transaction, prepared, history_bytes)
        history_bytes = _prune_quality_history(transaction, history_bytes, max_bytes)
        transaction.put(StateKind.QUALITY_EVENT_BUDGET, "current", {"value": history_bytes})


def _prepare_quality_import_rows(
    rows: Iterable[tuple[str, Mapping[str, object]]],
) -> list[tuple[str, dict[str, object]]]:
    prepared: list[tuple[str, dict[str, object]]] = []
    for item_id, row in rows:
        payload = dict(row)
        timestamp = validate_quality_event(payload)
        if not isinstance(item_id, str) or not item_id.startswith(_quality_timestamp_key(timestamp) + "|"):
            raise QualityMetricsError("quality event ID must sort by its UTC timestamp")
        prepared.append((item_id, payload))
    return prepared


def _history_budget(transaction: StateTransaction) -> int:
    budget_row = transaction.get(StateKind.QUALITY_EVENT_BUDGET, "current")
    if budget_row is None:
        return transaction.payload_bytes(StateKind.QUALITY_EVENT, row_overhead=1)
    return _quality_budget_value(budget_row)


def _insert_quality_import_rows(
    transaction: StateTransaction,
    prepared: Iterable[tuple[str, dict[str, object]]],
    history_bytes: int,
) -> int:
    for item_id, payload in prepared:
        existing = transaction.get(StateKind.QUALITY_EVENT, item_id)
        if existing is not None and existing != payload:
            raise QualityMetricsError("quality event ID conflicts with retained history")
        if existing is None:
            transaction.put(StateKind.QUALITY_EVENT, item_id, payload)
            history_bytes += _event_size(payload)
    return history_bytes


def _prune_quality_history(transaction: StateTransaction, history_bytes: int, max_bytes: int) -> int:
    while history_bytes > max_bytes:
        oldest = transaction.list(StateKind.QUALITY_EVENT, limit=1)
        if not oldest:
            break
        oldest_id, oldest_payload = oldest[0]
        if not transaction.delete(StateKind.QUALITY_EVENT, oldest_id):
            break
        history_bytes -= _event_size(oldest_payload)
    return history_bytes


def quality_event_id(row: Mapping[str, object], suffix: str | None = None) -> str:
    """Return a sortable unique ID whose prefix is the validated event timestamp."""
    timestamp = validate_quality_event(row)
    return f"{_quality_timestamp_key(timestamp)}|{suffix or uuid4().hex}"


def validate_quality_event(row: Mapping[str, object]) -> datetime:
    """Validate the existing v2 quality-event contract and return its UTC time."""
    event = row.get("event")
    fields, timestamp_key = _event_schema(row, event)
    if set(row) != fields:
        raise ValueError("quality event fields are invalid")
    version = row.get("schema_version")
    if isinstance(version, bool) or not isinstance(version, int) or version != _SCHEMA_VERSION:
        raise ValueError("quality event schema version is invalid")
    timestamp = _require_timestamp(row, timestamp_key)
    _require_string(row, "session_id")
    _require_string(row, "release_version")
    _require_source_revision(row)
    if event == "advertisement_observation":
        _validate_advertisement(row)
    elif event == "transfer_session":
        _validate_transfer(row)
    elif event == "sequence_loss":
        _validate_loss(row)
    else:
        _validate_clock(row)
    return timestamp


def _event_schema(row: Mapping[str, object], event: object) -> tuple[frozenset[str], str]:
    if event == "advertisement_observation":
        return _ADVERTISEMENT_FIELDS, "recorded_at"
    if event == "transfer_session":
        return _TRANSFER_FIELDS, "completed_at"
    if event == "sequence_loss":
        return _LOSS_FIELDS | ({"loss_id"} if "loss_id" in row else set()), "occurred_at"
    if event == "clock_correction":
        return _CLOCK_FIELDS, "occurred_at"
    raise ValueError("quality event type is unsupported")


def _validate_advertisement(row: Mapping[str, object]) -> None:
    _require_string(row, "phy_policy")
    _integer(row, "advertisement_rssi_dbm")


def _validate_transfer(row: Mapping[str, object]) -> None:
    _require_string(row, "outcome")
    if _require_string(row, "termination_class") not in _TERMINATION_CLASSES:
        raise ValueError("transfer event has invalid termination_class")
    _require_string(row, "phy_policy")
    _nullable_string(row, "firmware_version")
    _optional_integer(row, "advertisement_rssi_dbm")
    for name in (
        "active_read_elapsed_ms",
        "requested_record_count",
        "received_raw_bytes",
        "submitted_raw_bytes",
        "written_raw_bytes",
    ):
        _integer(row, name, minimum=0)
    if _integer(row, "record_size_bytes", minimum=1) != RECORD_SIZE:
        raise ValueError("transfer event has invalid record_size_bytes")


def _validate_loss(row: Mapping[str, object]) -> None:
    if "loss_id" in row and _LOSS_ID.fullmatch(_require_string(row, "loss_id")) is None:
        raise ValueError("sequence loss has invalid loss_id")
    _require_string(row, "reason")
    _nullable_string(row, "firmware_version")
    _integer(row, "missing_record_count", minimum=0)
    _integer(row, "missing_raw_bytes", minimum=0)


def _validate_clock(row: Mapping[str, object]) -> None:
    _nullable_string(row, "firmware_version")
    _integer(row, "target_epoch", minimum=0)
    boundary_min = _integer(row, "boundary_sequence_min", minimum=0)
    boundary_max = _integer(row, "boundary_sequence_max", minimum=0)
    if boundary_max < boundary_min:
        raise ValueError("clock correction has invalid sequence boundary")
    drift = row.get("drift_seconds")
    if isinstance(drift, bool) or not isinstance(drift, int | float):
        raise ValueError("clock correction has invalid drift_seconds")


def source_revision_from_release_metadata(path: Path | None = None) -> str | None:
    """Read immutable deployment provenance from the installed release."""
    metadata_path = path or Path(sys.prefix) / _RELEASE_METADATA
    try:
        value = cast(object, json.loads(metadata_path.read_text(encoding="utf-8")))
    except FileNotFoundError:
        return None
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError("release metadata is unreadable or malformed") from error
    if not isinstance(value, dict) or set(value) != {"source_revision"}:
        raise ValueError("release metadata schema is invalid")
    revision = value["source_revision"]
    if not isinstance(revision, str):
        raise ValueError("release source revision is invalid")
    _validate_source_revision(revision)
    return revision


def normalize_source_revision(value: str | None) -> str | None:
    if value is None or value == "":
        return None
    _validate_source_revision(value)
    return value[:12]


def _event_size(payload: Mapping[str, object]) -> int:
    return (
        len(
            json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False).encode(
                "utf-8"
            )
        )
        + 1
    )


def _quality_budget_value(row: Mapping[str, object]) -> int:
    value = row.get("value")
    if type(value) is not int or value < 0:
        raise QualityMetricsError("quality history budget counter is invalid")
    return value


def _quality_timestamp_key(timestamp: datetime) -> str:
    return timestamp.astimezone(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")


def _require_timestamp(row: Mapping[str, object], key: str) -> datetime:
    raw = _require_string(row, key)
    try:
        timestamp = datetime.fromisoformat(raw)
    except ValueError as error:
        raise ValueError(f"quality event has invalid {key}") from error
    if timestamp.tzinfo is None:
        raise ValueError(f"quality event has naive {key}")
    return timestamp.astimezone(UTC)


def _require_string(row: Mapping[str, object], key: str) -> str:
    value = row.get(key)
    if not isinstance(value, str) or not value:
        raise ValueError(f"quality event has invalid {key}")
    return value


def _require_source_revision(row: Mapping[str, object]) -> None:
    value = row.get("source_revision")
    if value is not None and (not isinstance(value, str) or _SOURCE_REVISION.fullmatch(value) is None):
        raise ValueError("quality event has invalid source_revision")


def _nullable_string(row: Mapping[str, object], key: str) -> None:
    value = row.get(key)
    if value is not None and not isinstance(value, str):
        raise ValueError(f"quality event has invalid {key}")


def _integer(row: Mapping[str, object], key: str, *, minimum: int | None = None) -> int:
    value = row.get(key)
    if isinstance(value, bool) or not isinstance(value, int) or (minimum is not None and value < minimum):
        raise ValueError(f"quality event has invalid {key}")
    return value


def _optional_integer(row: Mapping[str, object], key: str) -> None:
    value = row.get(key)
    if value is not None:
        _integer(row, key)


def _validate_source_revision(value: str) -> None:
    if _REVISION.fullmatch(value) is None:
        raise ValueError("source revision must be 40 to 64 lowercase hexadecimal characters")


def _require_release_version(value: str) -> str:
    if not isinstance(value, str) or not value:
        raise ValueError("release version must be a non-empty string")
    return value
