"""Strict operator configuration and fixed single-pendant storage layout."""

from __future__ import annotations

import os
import re
import stat
import tomllib
from dataclasses import dataclass
from pathlib import Path
from typing import cast

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
    device_state: Path
    debug_log: Path


@dataclass(frozen=True, slots=True)
class PublicationLayout:
    """Normalized audio generations exposed to downstream processing."""

    root: Path
    generations: Path
    current: Path


@dataclass(frozen=True, slots=True)
class StorageLayout:
    """Fixed paths derived from one operator-owned storage root."""

    root: Path
    collector: CollectorLayout
    captured: Path
    publication: PublicationLayout


@dataclass(frozen=True, slots=True)
class OperatorConfig:
    """The complete operator-owned collector configuration."""

    path: Path
    pendant: PendantConfig
    storage: StorageLayout


def load_operator_config(path: Path = DEFAULT_CONFIG_PATH) -> OperatorConfig:
    """Load the single-pendant TOML authority and derive storage beside it."""
    config_path = Path(path)
    _require_regular_file(config_path, "config file")
    try:
        document = cast(dict[str, object], tomllib.loads(config_path.read_text(encoding="utf-8")))
    except (OSError, UnicodeDecodeError, tomllib.TOMLDecodeError) as error:
        raise StorageLayoutError("config TOML is unreadable or malformed") from error
    if not isinstance(document, dict) or set(document) != {"pendant"}:
        raise StorageLayoutError("config must contain exactly [pendant]")
    pendant = _section(document["pendant"], {"address"}, "pendant")
    address = pendant["address"]
    if _ADDRESS.fullmatch(address) is None:
        raise StorageLayoutError("pendant.address must be an uppercase Bluetooth address")
    root = config_path.absolute().parent
    if os.path.lexists(root) and root.is_symlink():
        raise StorageLayoutError("config parent must not be a symlink")
    collector_root = root / "collector"
    publication_root = root / "source"
    layout = StorageLayout(
        root=root,
        collector=CollectorLayout(
            root=collector_root,
            attempts=collector_root / "attempts",
            quarantine=collector_root / "quarantine",
            lock=collector_root / "collector.lock",
            device_state=collector_root / "device.json",
            debug_log=collector_root / "debug.jsonl",
        ),
        captured=root / "captured",
        publication=PublicationLayout(
            root=publication_root,
            generations=publication_root / ".generations",
            current=publication_root / "current",
        ),
    )
    return OperatorConfig(config_path.absolute(), PendantConfig(address), layout)


def _section(value: object, keys: set[str], name: str) -> dict[str, str]:
    if not isinstance(value, dict) or set(value) != keys:
        raise StorageLayoutError(f"[{name}] must contain exactly {', '.join(sorted(keys))}")
    result: dict[str, str] = {}
    for key, item in value.items():
        if not isinstance(item, str) or not item or item != item.strip():
            raise StorageLayoutError(f"{name}.{key} must be a non-empty string")
        result[key] = item
    return result


def _require_regular_file(path: Path, label: str) -> None:
    try:
        mode = path.lstat().st_mode
    except OSError as error:
        raise StorageLayoutError(f"{label} is missing or unreadable") from error
    if stat.S_ISLNK(mode) or not stat.S_ISREG(mode):
        raise StorageLayoutError(f"{label} must be a regular non-symlink file")
