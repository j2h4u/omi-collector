"""Strict, single-source storage layout for collector runtime state."""

from __future__ import annotations

import os
import stat
import tomllib
from dataclasses import dataclass
from pathlib import Path, PurePath
from typing import Final, cast

_VERSION: Final = 1
_COLLECTOR_KEYS: Final = frozenset({"root", "attempts", "quarantine", "lock", "device_state", "debug_log"})
_PUBLICATION_KEYS: Final = frozenset({"root", "raw"})


class StorageLayoutError(ValueError):
    """The external storage-layout authority is unsafe or malformed."""


@dataclass(frozen=True, slots=True)
class CollectorLayout:
    """Resolved paths owned by the collector transport boundary."""

    root: Path
    attempts: Path
    quarantine: Path
    lock: Path
    device_state: Path
    debug_log: Path


@dataclass(frozen=True, slots=True)
class PublicationLayout:
    """Resolved paths owned by the downstream publication boundary."""

    root: Path
    raw: Path


@dataclass(frozen=True, slots=True)
class StorageLayout:
    """The complete resolved version-one collector/publication contract."""

    path: Path
    collector: CollectorLayout
    publication: PublicationLayout


def load_storage_layout(path: Path) -> StorageLayout:
    """Load one regular TOML authority without accepting aliases or symlinks."""
    layout_path = Path(path)
    _require_regular_file(layout_path, "layout file")
    try:
        root = layout_path.parent.resolve(strict=True)
    except OSError as error:
        raise StorageLayoutError("layout parent cannot be resolved") from error
    _require_regular_directory(root, "layout parent")
    try:
        document = cast(dict[str, object], tomllib.loads(layout_path.read_text(encoding="utf-8")))
    except (OSError, UnicodeDecodeError, tomllib.TOMLDecodeError) as error:
        raise StorageLayoutError("layout TOML is unreadable or malformed") from error
    if not isinstance(document, dict) or set(document) != {"version", "collector", "publication"}:
        raise StorageLayoutError("layout must contain exactly version, collector, and publication")
    if document["version"] != _VERSION:
        raise StorageLayoutError("layout version is unsupported")
    collector_values = _section(document["collector"], _COLLECTOR_KEYS, "collector")
    publication_values = _section(document["publication"], _PUBLICATION_KEYS, "publication")
    collector_root = _component(root, collector_values["root"], "collector.root")
    publication_root = _component(root, publication_values["root"], "publication.root")
    collector = CollectorLayout(
        root=collector_root,
        attempts=_component(collector_root, collector_values["attempts"], "collector.attempts"),
        quarantine=_component(collector_root, collector_values["quarantine"], "collector.quarantine"),
        lock=_component(collector_root, collector_values["lock"], "collector.lock"),
        device_state=_component(collector_root, collector_values["device_state"], "collector.device_state"),
        debug_log=_component(collector_root, collector_values["debug_log"], "collector.debug_log"),
    )
    publication = PublicationLayout(
        root=publication_root,
        raw=_component(publication_root, publication_values["raw"], "publication.raw"),
    )
    _validate_unique_paths(
        (
            collector.root,
            collector.attempts,
            collector.quarantine,
            collector.lock,
            collector.device_state,
            collector.debug_log,
            publication.root,
            publication.raw,
        )
    )
    for candidate in (
        collector.root,
        collector.attempts,
        collector.quarantine,
        collector.lock,
        collector.device_state,
        collector.debug_log,
        publication.root,
        publication.raw,
    ):
        if os.path.lexists(candidate) and candidate.is_symlink():
            raise StorageLayoutError(f"layout target must not be a symlink: {candidate.name}")
    return StorageLayout(layout_path.absolute(), collector, publication)


def _section(value: object, keys: frozenset[str], name: str) -> dict[str, str]:
    if not isinstance(value, dict) or set(value) != keys:
        raise StorageLayoutError(f"[{name}] must contain exactly its supported keys")
    result: dict[str, str] = {}
    for key, component in value.items():
        if not isinstance(component, str):
            raise StorageLayoutError(f"{name}.{key} must be a string path component")
        _validate_component(component, f"{name}.{key}")
        result[key] = component
    return result


def _validate_component(value: str, name: str) -> None:
    path = PurePath(value)
    if value in {".", ".."} or path.is_absolute():
        raise StorageLayoutError(f"{name} must be one relative path component")
    if len(path.parts) != 1 or path.name != value:
        raise StorageLayoutError(f"{name} must be one relative path component")
    if "/" in value or "\\" in value:
        raise StorageLayoutError(f"{name} must be one relative path component")


def _component(parent: Path, value: str, name: str) -> Path:
    _validate_component(value, name)
    return parent / value


def _validate_unique_paths(paths: tuple[Path, ...]) -> None:
    seen: set[Path] = set()
    for path in paths:
        if path in seen:
            raise StorageLayoutError(f"layout paths collide at {path.name}")
        seen.add(path)


def _require_regular_file(path: Path, label: str) -> None:
    try:
        mode = path.lstat().st_mode
    except OSError as error:
        raise StorageLayoutError(f"{label} is missing or unreadable") from error
    if stat.S_ISLNK(mode) or not stat.S_ISREG(mode):
        raise StorageLayoutError(f"{label} must be a regular non-symlink file")


def _require_regular_directory(path: Path, label: str) -> None:
    try:
        mode = path.lstat().st_mode
    except OSError as error:
        raise StorageLayoutError(f"{label} is missing or unreadable") from error
    if stat.S_ISLNK(mode) or not stat.S_ISDIR(mode):
        raise StorageLayoutError(f"{label} must be a regular non-symlink directory")
