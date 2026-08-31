"""Firmware counter history in the declared single ``device.json`` file."""

from __future__ import annotations

import json
import os
import re
import stat
import threading
from collections.abc import Callable, Mapping
from contextlib import suppress
from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path
from typing import Final, cast
from uuid import uuid4

from ...config import DEFAULT_CONFIG, CollectorConfig, FirmwareObservationConfig
from ..domain.ring_protocol import RingInfo

_SCHEMA_VERSION: Final = 1
_SHA256_LENGTH = 64
_U16_MAX: Final = (1 << 16) - 1
_U32_MAX: Final = (1 << 32) - 1
_U64_MAX: Final = (1 << 64) - 1
_SLUG = re.compile(r"[A-Za-z0-9_-]+\Z")
_LOCKS: dict[str, threading.RLock] = {}
_LOCKS_GUARD = threading.Lock()

_ErrorCallback = Callable[[Exception], None]


class FirmwareObservationError(ValueError):
    """The declared device observation file is malformed or unsafe."""


@dataclass(frozen=True, slots=True)
class FirmwareObservation(Mapping[str, object]):
    """One hash-linked historical firmware counter snapshot."""

    observation_sequence: int
    previous_sha256: str | None
    device_slug: str
    info: RingInfo
    sha256: str

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

    def as_dict(self) -> dict[str, object]:
        return {
            "schema_version": _SCHEMA_VERSION,
            "observation_sequence": self.observation_sequence,
            "previous_sha256": self.previous_sha256,
            "device_slug": self.device_slug,
            "read_sequence": self.read_sequence,
            "write_sequence": self.write_sequence,
            "capacity_packets": self.capacity_packets,
            "dropped_packets": self.dropped_packets,
            "packet_size": self.packet_size,
            "sha256": self.sha256,
        }

    def __getitem__(self, key: str) -> object:
        return self.as_dict()[key]

    def __iter__(self):  # type: ignore[no-untyped-def]
        return iter(self.as_dict())

    def __len__(self) -> int:
        return len(self.as_dict())


def _canonical_json(value: object) -> bytes:
    return json.dumps(value, ensure_ascii=True, sort_keys=True, separators=(",", ":")).encode("utf-8")


def _validate_slug(device_slug: str) -> None:
    if not isinstance(device_slug, str) or _SLUG.fullmatch(device_slug) is None:
        raise FirmwareObservationError("device_slug must contain only ASCII letters, digits, '-' and '_'")


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


def _lock_for(path: Path) -> threading.RLock:
    key = str(path)
    with _LOCKS_GUARD:
        lock = _LOCKS.get(key)
        if lock is None:
            lock = threading.RLock()
            _LOCKS[key] = lock
        return lock


def _parse_object(raw: bytes, path: Path) -> dict[str, object]:
    def reject_constant(value: str) -> None:
        raise FirmwareObservationError(f"non-finite JSON value in {path.name}: {value}")

    def reject_duplicate(pairs: list[tuple[str, object]]) -> dict[str, object]:
        result: dict[str, object] = {}
        for key, value in pairs:
            if key in result:
                raise FirmwareObservationError(f"duplicate JSON key in {path.name}: {key}")
            result[key] = value
        return result

    try:
        value = cast(
            object, json.loads(raw.decode("utf-8"), object_pairs_hook=reject_duplicate, parse_constant=reject_constant)
        )
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise FirmwareObservationError(f"malformed observation JSON: {path.name}") from error
    if not isinstance(value, dict):
        raise FirmwareObservationError(f"observation file must be a JSON object: {path.name}")
    return cast(dict[str, object], value)


def _integer(data: Mapping[str, object], key: str, path: Path, maximum: int | None = None) -> int:
    value = data.get(key)
    if isinstance(value, bool) or not isinstance(value, int) or value < 0 or (maximum is not None and value > maximum):
        raise FirmwareObservationError(f"{key} is outside firmware bounds: {path.name}")
    return value


def _hash(data: Mapping[str, object], key: str, path: Path, *, nullable: bool = False) -> str | None:
    value = data.get(key)
    if nullable and value is None:
        return None
    if not isinstance(value, str) or len(value) != _SHA256_LENGTH or re.fullmatch(r"[0-9a-f]{64}", value) is None:
        raise FirmwareObservationError(f"{key} must be a lowercase SHA-256 hex string: {path.name}")
    return value


def _decode_observation(data: Mapping[str, object], path: Path, sequence: int, device_slug: str) -> FirmwareObservation:
    required = {
        "schema_version",
        "observation_sequence",
        "previous_sha256",
        "device_slug",
        "read_sequence",
        "write_sequence",
        "capacity_packets",
        "dropped_packets",
        "packet_size",
        "sha256",
    }
    if set(data) != required or data.get("schema_version") != _SCHEMA_VERSION:
        raise FirmwareObservationError(f"unexpected observation fields: {path.name}")
    if _integer(data, "observation_sequence", path) != sequence or data.get("device_slug") != device_slug:
        raise FirmwareObservationError(f"observation identity is invalid: {path.name}")
    digest = _hash(data, "sha256", path)
    assert digest is not None
    unsigned = {key: value for key, value in data.items() if key != "sha256"}
    if sha256(_canonical_json(unsigned)).hexdigest() != digest:
        raise FirmwareObservationError(f"observation hash mismatch: {path.name}")
    return FirmwareObservation(
        sequence,
        _hash(data, "previous_sha256", path, nullable=True),
        device_slug,
        RingInfo(
            _integer(data, "read_sequence", path, _U64_MAX),
            _integer(data, "write_sequence", path, _U64_MAX),
            _integer(data, "capacity_packets", path, _U32_MAX),
            _integer(data, "dropped_packets", path, _U64_MAX),
            _integer(data, "packet_size", path, _U16_MAX),
        ),
        digest,
    )


def read_firmware_observations(device_state_path: Path, device_slug: str) -> tuple[FirmwareObservation, ...]:
    """Read the complete history from the declared state file."""
    _validate_slug(device_slug)
    path = Path(device_state_path)
    if not path.exists() and not path.is_symlink():
        return ()
    _require_regular_file(path)
    try:
        raw = path.read_bytes()
    except OSError as error:
        raise FirmwareObservationError(f"observation file is unreadable: {path.name}") from error
    document = _parse_object(raw, path)
    if (
        set(document) != {"schema_version", "device_slug", "observations"}
        or document.get("schema_version") != _SCHEMA_VERSION
    ):
        raise FirmwareObservationError(f"unexpected device state fields: {path.name}")
    if document.get("device_slug") != device_slug or not isinstance(document.get("observations"), list):
        raise FirmwareObservationError(f"device state identity is invalid: {path.name}")
    if raw != _canonical_json(document):
        raise FirmwareObservationError(f"device state JSON is not canonical: {path.name}")
    result: list[FirmwareObservation] = []
    previous: str | None = None
    for sequence, item in enumerate(cast(list[object], document["observations"]), 1):
        if not isinstance(item, dict):
            raise FirmwareObservationError(f"observation is not an object: {path.name}")
        observation = _decode_observation(cast(dict[str, object], item), path, sequence, device_slug)
        if observation.previous_sha256 != previous:
            raise FirmwareObservationError(f"broken observation chain: {path.name}")
        result.append(observation)
        previous = observation.sha256
    return tuple(result)


class FirmwareObservationStore:
    """Atomically persist the declared single-file observation chain."""

    def __init__(self, device_state_path: Path) -> None:
        self._path = Path(device_state_path)

    def record(self, device_slug: str, info: RingInfo) -> bool:
        _validate_slug(device_slug)
        _validate_ring_info(info)
        with _lock_for(self._path):
            _ensure_regular_parent(self._path)
            persisted = read_firmware_observations(self._path, device_slug)
            if persisted and persisted[-1].dropped_packets == info.dropped_packets:
                return False
            data: dict[str, object] = {
                "schema_version": _SCHEMA_VERSION,
                "observation_sequence": len(persisted) + 1,
                "previous_sha256": persisted[-1].sha256 if persisted else None,
                "device_slug": device_slug,
                "read_sequence": info.read_sequence,
                "write_sequence": info.write_sequence,
                "capacity_packets": info.capacity_packets,
                "dropped_packets": info.dropped_packets,
                "packet_size": info.packet_size,
            }
            data["sha256"] = sha256(_canonical_json(data)).hexdigest()
            document = {
                "schema_version": _SCHEMA_VERSION,
                "device_slug": device_slug,
                "observations": [item.as_dict() for item in persisted] + [data],
            }
            _write_atomic(self._path, _canonical_json(document))
            return True


def _require_regular_file(path: Path) -> None:
    try:
        mode = path.lstat().st_mode
    except OSError as error:
        raise FirmwareObservationError(f"observation file is missing or unreadable: {path.name}") from error
    if stat.S_ISLNK(mode) or not stat.S_ISREG(mode):
        raise FirmwareObservationError("observation file must be a regular non-symlink file")


def _ensure_regular_parent(path: Path) -> None:
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        mode = path.parent.lstat().st_mode
    except OSError as error:
        raise FirmwareObservationError("observation parent is unavailable") from error
    if stat.S_ISLNK(mode) or not stat.S_ISDIR(mode):
        raise FirmwareObservationError("observation parent must be a regular non-symlink directory")
    if path.exists() or path.is_symlink():
        _require_regular_file(path)


def _write_atomic(path: Path, payload: bytes) -> None:
    temporary = path.with_name(f".{path.name}.{uuid4().hex}.tmp")
    try:
        with temporary.open("xb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        temporary.replace(path)
        _fsync_directory(path.parent)
    except OSError as error:
        with suppress(FileNotFoundError):
            temporary.unlink()
        raise FirmwareObservationError(f"cannot publish firmware observation: {path.name}") from error


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


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
        self._pending: dict[str, RingInfo] = {}
        self._latest_counters: dict[str, int] = {}
        self._failure_active = False
        self._stopping = False
        self._thread = threading.Thread(target=self._run, name="firmware-observations", daemon=True)
        self._thread.start()

    def observe(self, device_slug: str, info: RingInfo) -> None:
        _validate_slug(device_slug)
        _validate_ring_info(info)
        with self._condition:
            if self._latest_counters.get(device_slug) == info.dropped_packets:
                return
            self._latest_counters[device_slug] = info.dropped_packets
            self._pending[device_slug] = info
            self._condition.notify()

    def close(self) -> None:
        with self._condition:
            self._stopping = True
            self._condition.notify()
        self._thread.join(self._config.close_timeout_seconds)

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
                while not self._pending and not self._stopping:
                    self._condition.wait()
                if not self._pending:
                    return
                device_slug = next(iter(self._pending))
                item = (device_slug, self._pending.pop(device_slug))
            try:
                self._store.record(*item)
            except Exception as error:  # noqa: BLE001
                if not self._failure_active:
                    self._failure_active = True
                    self._report_error(error)
                with self._condition:
                    self._pending.setdefault(item[0], item[1])
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
