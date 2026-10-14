"""Bounded firmware counter state used by metrics and current diagnostics."""

from __future__ import annotations

import json
import os
import stat
import threading
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Final, cast

from ...config import DEFAULT_CONFIG, CollectorConfig, FirmwareObservationConfig
from ..application.storage_fault import FatalInternalAudioStorageError
from ..domain.ring_protocol import RingInfo
from .collector_state_db import StateDB, StateDBError, StateKind

_SCHEMA_VERSION: Final = 3
_MAX_LEGACY_STATE_BYTES: Final = 65_536
_U16_MAX: Final = (1 << 16) - 1
_U32_MAX: Final = (1 << 32) - 1
_U64_MAX: Final = (1 << 64) - 1
_ErrorCallback = Callable[[Exception], None]


class FirmwareObservationError(ValueError):
    """The declared device observation file is malformed or unsafe."""


@dataclass(frozen=True, slots=True)
class FirmwareObservation:
    """The latest firmware snapshot plus bounded lifetime counter aggregates."""

    info: RingInfo
    observation_count: int
    initial: int
    observed_increase: int
    regression_count: int

    @property
    def read_sequence(self) -> int:
        return self.info.read_sequence

    @property
    def write_sequence(self) -> int:
        return self.info.write_sequence

    @property
    def capacity_packets(self) -> int:
        return self.info.capacity_packets

    @property
    def dropped_packets(self) -> int:
        return self.info.dropped_packets

    @property
    def packet_size(self) -> int:
        return self.info.packet_size

    @property
    def latest(self) -> int:
        return self.dropped_packets

    @property
    def epoch_count(self) -> int:
        return self.regression_count + 1


def _validate_ring_info(info: RingInfo) -> None:
    if not isinstance(info, RingInfo):
        raise TypeError("info must be a RingInfo")
    for name, value, maximum in (
        ("read_sequence", info.read_sequence, _U64_MAX),
        ("write_sequence", info.write_sequence, _U64_MAX),
        ("capacity_packets", info.capacity_packets, _U32_MAX),
        ("dropped_packets", info.dropped_packets, _U64_MAX),
        ("packet_size", info.packet_size, _U16_MAX),
    ):
        if isinstance(value, bool) or not isinstance(value, int) or not 0 <= value <= maximum:
            raise ValueError(f"{name} must be an unsigned integer within firmware bounds")


def _integer(data: Mapping[str, object], key: str, maximum: int | None = None) -> int:
    value = data.get(key)
    if isinstance(value, bool) or not isinstance(value, int) or value < 0 or (maximum is not None and value > maximum):
        raise FirmwareObservationError(f"{key} is outside firmware bounds")
    return value


def _decode(document: Mapping[str, object]) -> FirmwareObservation:
    if (
        set(document) != {"schema_version", "latest", "metrics"}
        or type(document.get("schema_version")) is not int
        or document.get("schema_version") != _SCHEMA_VERSION
    ):
        raise FirmwareObservationError("unexpected device state fields")
    latest = document.get("latest")
    metrics = document.get("metrics")
    if not isinstance(latest, dict) or not isinstance(metrics, dict):
        raise FirmwareObservationError("device state sections are invalid")
    if set(latest) != {"read_sequence", "write_sequence", "capacity_packets", "dropped_packets", "packet_size"}:
        raise FirmwareObservationError("latest firmware state is invalid")
    if set(metrics) != {"observation_count", "initial", "observed_increase", "regression_count"}:
        raise FirmwareObservationError("firmware metrics state is invalid")
    info = RingInfo(
        _integer(latest, "read_sequence", _U64_MAX),
        _integer(latest, "write_sequence", _U64_MAX),
        _integer(latest, "capacity_packets", _U32_MAX),
        _integer(latest, "dropped_packets", _U64_MAX),
        _integer(latest, "packet_size", _U16_MAX),
    )
    count = _integer(metrics, "observation_count")
    initial = _integer(metrics, "initial", _U64_MAX)
    increase = _integer(metrics, "observed_increase", _U64_MAX)
    regressions = _integer(metrics, "regression_count")
    if count <= 0:
        raise FirmwareObservationError("firmware metrics state is inconsistent")
    return FirmwareObservation(info, count, initial, increase, regressions)


def read_firmware_observations(db: StateDB) -> tuple[FirmwareObservation, ...]:
    """Read the latest bounded state record; absent state is an empty result."""
    try:
        document = db.get(StateKind.FIRMWARE, "latest")
    except StateDBError as error:
        raise FirmwareObservationError("firmware observation database read failed") from error
    if document is None:
        return ()
    return (_decode(document),)


def parse_legacy_firmware(path: Path) -> tuple[FirmwareObservation, ...]:
    """Strictly decode the old device snapshot for the offline state importer."""
    try:
        descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
        try:
            info = os.fstat(descriptor)
            if not stat.S_ISREG(info.st_mode) or info.st_size > _MAX_LEGACY_STATE_BYTES:
                raise ValueError("legacy firmware state is not a regular file")
            with os.fdopen(descriptor, "rb", closefd=False) as stream:
                raw = stream.read(_MAX_LEGACY_STATE_BYTES + 1)
        finally:
            os.close(descriptor)
        if len(raw) > _MAX_LEGACY_STATE_BYTES or raw != json.dumps(
            cast(object, json.loads(raw)), ensure_ascii=True, sort_keys=True, separators=(",", ":")
        ).encode("utf-8"):
            raise ValueError("legacy firmware JSON is not canonical or is oversized")
        document = cast(object, json.loads(raw))
        if not isinstance(document, dict):
            raise ValueError("legacy firmware state must be an object")
        return (_decode(cast(dict[str, object], document)),)
    except (OSError, UnicodeDecodeError, json.JSONDecodeError, TypeError, ValueError) as error:
        raise FirmwareObservationError("legacy firmware state is malformed or unsafe") from error


class FirmwareObservationStore:
    """Atomically persist one latest snapshot and bounded metric aggregates."""

    def __init__(self, db: StateDB) -> None:
        self._db = db

    def record(self, info: RingInfo) -> bool:
        _validate_ring_info(info)
        try:
            with self._db.transaction() as transaction:
                document = transaction.get(StateKind.FIRMWARE, "latest")
                persisted = (_decode(document),) if document is not None else ()
                previous = persisted[0] if persisted else None
                if previous is not None and previous.info == info:
                    return False
                if previous is None:
                    state = FirmwareObservation(info, 1, info.dropped_packets, 0, 0)
                elif previous.dropped_packets == info.dropped_packets:
                    state = FirmwareObservation(
                        info,
                        previous.observation_count,
                        previous.initial,
                        previous.observed_increase,
                        previous.regression_count,
                    )
                else:
                    increase = previous.observed_increase + max(0, info.dropped_packets - previous.dropped_packets)
                    regressions = previous.regression_count + (info.dropped_packets < previous.dropped_packets)
                    state = FirmwareObservation(
                        info,
                        previous.observation_count + 1,
                        previous.initial,
                        increase,
                        regressions,
                    )
                document = {
                    "schema_version": _SCHEMA_VERSION,
                    "latest": {
                        "read_sequence": state.read_sequence,
                        "write_sequence": state.write_sequence,
                        "capacity_packets": state.capacity_packets,
                        "dropped_packets": state.dropped_packets,
                        "packet_size": state.packet_size,
                    },
                    "metrics": {
                        "observation_count": state.observation_count,
                        "initial": state.initial,
                        "observed_increase": state.observed_increase,
                        "regression_count": state.regression_count,
                    },
                }
                transaction.put(StateKind.FIRMWARE, "latest", document)
                return True
        except StateDBError as error:
            raise FirmwareObservationError("firmware observation database write failed") from error


class FirmwareObservationWriter:
    """A non-blocking latest-value mailbox backed by one retrying daemon."""

    def __init__(
        self,
        store: FirmwareObservationStore,
        config: FirmwareObservationConfig | CollectorConfig = DEFAULT_CONFIG.firmware_observations,
        on_error: _ErrorCallback | None = None,
    ) -> None:
        self._store = store
        self._config = config.firmware_observations if isinstance(config, CollectorConfig) else config
        self._on_error = on_error
        self._condition = threading.Condition()
        self._pending: RingInfo | None = None
        self._latest_observation: RingInfo | None = None
        self._failure_active = False
        self._stopping = False
        self._thread = threading.Thread(target=self._run, name="firmware-observations", daemon=True)
        self._thread.start()

    def observe(self, info: RingInfo) -> None:
        _validate_ring_info(info)
        with self._condition:
            if self._latest_observation == info:
                return
            self._latest_observation = info
            self._pending = info
            self._condition.notify()

    def close(self) -> None:
        """Stop accepting updates and wait until the final bounded DB write exits."""
        with self._condition:
            self._stopping = True
            self._condition.notify()
        self._thread.join()

    def _report_error(self, error: Exception) -> None:
        if self._on_error is None:
            return
        try:
            self._on_error(error)
        except Exception:  # noqa: BLE001
            return

    def _run(self) -> None:
        retry_index = 0
        while True:
            with self._condition:
                while self._pending is None and not self._stopping:
                    self._condition.wait()
                if self._pending is None:
                    return
                item = self._pending
                self._pending = None
            try:
                self._store.record(item)
            except FatalInternalAudioStorageError as error:
                self._report_error(error)
                return
            except Exception as error:  # noqa: BLE001
                if not self._failure_active:
                    self._failure_active = True
                    self._report_error(error)
                with self._condition:
                    if self._pending is None:
                        self._pending = item
                    if self._stopping:
                        return
                    delay = self._config.retry_backoff_seconds[
                        min(retry_index, len(self._config.retry_backoff_seconds) - 1)
                    ]
                    retry_index += 1
                    self._condition.wait(delay)
                continue
            self._failure_active = False
            retry_index = 0
