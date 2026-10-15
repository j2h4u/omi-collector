"""Strict operator configuration and fixed single-pendant storage layout."""

from __future__ import annotations

import os
import re
import stat
import tomllib
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import cast

from .config import CollectorConfig, PresenceConfig, ReadyConfig

DEFAULT_CONFIG_PATH = Path("/srv/pipelines/omi/config.toml")
_ADDRESS = re.compile(r"(?:[0-9A-F]{2}:){5}[0-9A-F]{2}\Z")


class StorageLayoutError(ValueError):
    """The collector configuration is unsafe or malformed."""


@dataclass(frozen=True, slots=True)
class PendantConfig:
    """The one pendant selected by this collector installation."""

    address: str


@dataclass(frozen=True, slots=True)
class CollectorLayout:
    """Private paths owned by the collector transport boundary."""

    root: Path
    attempts: Path
    quarantine: Path
    lock: Path
    state_db: Path
    debug_log: Path


@dataclass(frozen=True, slots=True)
class PublicationLayout:
    """Immutable ready bundles exposed to downstream processing."""

    root: Path


@dataclass(frozen=True, slots=True)
class StorageLayout:
    """Fixed paths derived from one operator-owned storage root."""

    root: Path
    collector: CollectorLayout
    draft: Path
    publication: PublicationLayout


@dataclass(frozen=True, slots=True)
class OperatorConfig:
    """The complete operator-owned collector configuration."""

    path: Path
    pendant: PendantConfig
    storage: StorageLayout
    config: CollectorConfig = field(default_factory=CollectorConfig)


def load_operator_config(path: Path = DEFAULT_CONFIG_PATH) -> OperatorConfig:
    """Load the single-pendant TOML authority and derive storage beside it."""
    config_path = Path(path)
    _require_regular_file(config_path, "config file")
    try:
        document = cast(dict[str, object], tomllib.loads(config_path.read_text(encoding="utf-8")))
    except (OSError, UnicodeDecodeError, tomllib.TOMLDecodeError) as error:
        raise StorageLayoutError("config TOML is unreadable or malformed") from error
    address, runtime_config = _parse_settings(document)
    root = config_path.absolute().parent
    if os.path.lexists(root) and root.is_symlink():
        raise StorageLayoutError("config parent must not be a symlink")
    collector_root = root / "collector"
    publication_root = root / "ready"
    layout = StorageLayout(
        root=root,
        collector=CollectorLayout(
            root=collector_root,
            attempts=collector_root / "attempts",
            quarantine=collector_root / "quarantine",
            lock=collector_root / "collector.lock",
            state_db=collector_root / "collector-state.sqlite3",
            debug_log=collector_root / "debug.jsonl",
        ),
        draft=root / "draft",
        publication=PublicationLayout(root=publication_root),
    )
    return OperatorConfig(config_path.absolute(), PendantConfig(address), layout, runtime_config)


def _parse_settings(document: dict[str, object]) -> tuple[str, CollectorConfig]:
    allowed = {
        "pendant_address",
        "bundle_min_audio_seconds",
        "presence_arrival_stability_seconds",
        "presence_arrival_max_gap_seconds",
    }
    required = {"pendant_address", "bundle_min_audio_seconds"}
    if not isinstance(document, dict):
        raise StorageLayoutError("config must be a TOML table")
    missing = required - set(document)
    if missing:
        raise StorageLayoutError(f"config is missing required keys: {', '.join(sorted(missing))}")
    unknown = set(document) - allowed
    if unknown:
        raise StorageLayoutError(f"config contains unsupported keys: {', '.join(sorted(unknown))}")
    address = document["pendant_address"]
    if not isinstance(address, str) or not address or address != address.strip():
        raise StorageLayoutError("pendant_address must be a non-empty string")
    if _ADDRESS.fullmatch(address) is None:
        raise StorageLayoutError("pendant_address must be an uppercase Bluetooth address")
    runtime_config = CollectorConfig()
    presence_keys = {"presence_arrival_stability_seconds", "presence_arrival_max_gap_seconds"}
    if set(document) & presence_keys:
        if not presence_keys <= set(document):
            raise StorageLayoutError(
                "presence tuning requires both presence_arrival_stability_seconds and "
                "presence_arrival_max_gap_seconds"
            )
        presence = _number_fields(document, presence_keys)
        try:
            runtime_config = replace(
                runtime_config,
                presence=PresenceConfig(
                    arrival_stability_seconds=presence["presence_arrival_stability_seconds"],
                    arrival_max_gap_seconds=presence["presence_arrival_max_gap_seconds"],
                ),
            )
        except ValueError as error:
            raise StorageLayoutError(f"presence settings are invalid: {error}") from error
    ready = _number_fields(document, {"bundle_min_audio_seconds"})
    try:
        runtime_config = replace(
            runtime_config,
            ready=ReadyConfig(target_audio_seconds=ready["bundle_min_audio_seconds"]),
        )
    except ValueError as error:
        raise StorageLayoutError(f"bundle settings are invalid: {error}") from error
    return address, runtime_config


def _number_fields(document: dict[str, object], keys: set[str]) -> dict[str, float]:
    result: dict[str, float] = {}
    for key in keys:
        item = document[key]
        if isinstance(item, bool) or not isinstance(item, (int, float)):
            raise StorageLayoutError(f"{key} must be a number")
        result[key] = float(item)
    return result


def _require_regular_file(path: Path, label: str) -> None:
    try:
        mode = path.lstat().st_mode
    except OSError as error:
        raise StorageLayoutError(f"{label} is missing or unreadable") from error
    if stat.S_ISLNK(mode) or not stat.S_ISREG(mode):
        raise StorageLayoutError(f"{label} must be a regular non-symlink file")
