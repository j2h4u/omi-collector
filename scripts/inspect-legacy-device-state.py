#!/usr/bin/env python3
"""Validate the disposable schema-one device state during deployment."""

from __future__ import annotations

import hashlib
import json
import os
import stat
import sys
import tomllib
from pathlib import Path
from typing import cast

_LAYOUT_VERSION = 2
_LEGACY_SCHEMA_VERSION = 1
_CURRENT_SCHEMA_VERSION = 2
_HASH_LENGTH = 64
_ARGUMENT_COUNT = 3


def _fail(message: str) -> int:
    print(message, file=sys.stderr)
    return 2


def _json_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate JSON key: {key}")
        result[key] = value
    return result


def _reject_constant(value: str) -> None:
    raise ValueError(f"non-finite JSON value: {value}")


def _state_path(layout_path: Path) -> Path:
    if layout_path.is_symlink() or not stat.S_ISREG(layout_path.lstat().st_mode):
        raise ValueError("layout file must be a regular non-symlink file")
    document = cast(dict[str, object], tomllib.loads(layout_path.read_text(encoding="utf-8")))
    if set(document) != {"version", "collector", "publication"} or document["version"] != _LAYOUT_VERSION:
        raise ValueError("layout is not an exact version-two document")
    collector = document["collector"]
    publication = document["publication"]
    keys = {"root", "attempts", "quarantine", "lock", "device_state", "debug_log"}
    if not isinstance(collector, dict) or set(collector) != keys:
        raise ValueError("layout collector section is invalid")
    if not isinstance(publication, dict) or set(publication) != {"root"}:
        raise ValueError("layout publication section is invalid")
    components = [collector[key] for key in keys] + [publication["root"]]
    if any(
        not isinstance(value, str) or value in {".", ".."} or "/" in value or "\\" in value or Path(value).is_absolute()
        for value in components
    ):
        raise ValueError("layout contains an unsafe path component")
    return layout_path.parent / collector["root"] / collector["device_state"]


def _validate_observation(observation: object, sequence: int, device_slug: str, previous: str | None) -> str:
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
    if not isinstance(observation, dict) or set(observation) != required:
        raise ValueError("device state is not an exact schema-1 document")
    if observation["schema_version"] != _LEGACY_SCHEMA_VERSION or observation["observation_sequence"] != sequence:
        raise ValueError("device state is not an exact schema-1 document")
    if observation["device_slug"] != device_slug or observation["previous_sha256"] != previous:
        raise ValueError("device state is not an exact schema-1 document")
    limits = {
        "read_sequence": 2**64 - 1,
        "write_sequence": 2**64 - 1,
        "capacity_packets": 2**32 - 1,
        "dropped_packets": 2**64 - 1,
        "packet_size": 2**16 - 1,
    }
    for key, maximum in limits.items():
        value = observation[key]
        if isinstance(value, bool) or not isinstance(value, int) or not 0 <= value <= maximum:
            raise ValueError("device state firmware fields are invalid")
    digest = observation["sha256"]
    if not isinstance(digest, str) or len(digest) != _HASH_LENGTH or any(c not in "0123456789abcdef" for c in digest):
        raise ValueError("device state hash is invalid")
    unsigned = {key: value for key, value in observation.items() if key != "sha256"}
    encoded = json.dumps(unsigned, ensure_ascii=True, sort_keys=True, separators=(",", ":")).encode("utf-8")
    if hashlib.sha256(encoded).hexdigest() != digest:
        raise ValueError("device state hash is invalid")
    return digest


def _validate_schema_one(path: Path, device_slug: str) -> None:
    mode = path.lstat().st_mode
    if stat.S_ISLNK(mode) or not stat.S_ISREG(mode):
        raise ValueError("device state must be a regular non-symlink file")
    raw = path.read_bytes()
    document = cast(
        object,
        json.loads(raw.decode("utf-8"), object_pairs_hook=_json_object, parse_constant=_reject_constant),
    )
    canonical = json.dumps(document, ensure_ascii=True, sort_keys=True, separators=(",", ":")).encode("utf-8")
    if raw != canonical or not isinstance(document, dict):
        raise ValueError("device state is not an exact schema-1 document")
    if set(document) != {"schema_version", "device_slug", "observations"}:
        raise ValueError("device state is not an exact schema-1 document")
    if document["schema_version"] != _LEGACY_SCHEMA_VERSION or document["device_slug"] != device_slug:
        raise ValueError("device state is not an exact schema-1 document")
    observations = document["observations"]
    if not isinstance(observations, list):
        raise ValueError("device state is not an exact schema-1 document")
    previous: str | None = None
    for sequence, observation in enumerate(observations, 1):
        previous = _validate_observation(observation, sequence, device_slug, previous)


def _integer(data: dict[str, object], key: str, maximum: int | None = None) -> int:
    value = data.get(key)
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError("current device state contains an invalid integer")
    if maximum is not None and value > maximum:
        raise ValueError("current device state contains an out-of-range integer")
    return value


def _validate_schema_two(path: Path, device_slug: str) -> None:
    mode = path.lstat().st_mode
    if stat.S_ISLNK(mode) or not stat.S_ISREG(mode):
        raise ValueError("device state must be a regular non-symlink file")
    raw = path.read_bytes()
    document = cast(
        object,
        json.loads(raw.decode("utf-8"), object_pairs_hook=_json_object, parse_constant=_reject_constant),
    )
    canonical = json.dumps(document, ensure_ascii=True, sort_keys=True, separators=(",", ":")).encode("utf-8")
    if raw != canonical or not isinstance(document, dict):
        raise ValueError("device state is not an exact schema-2 document")
    if set(document) != {"schema_version", "device_slug", "latest", "metrics"}:
        raise ValueError("device state is not an exact schema-2 document")
    if document["schema_version"] != _CURRENT_SCHEMA_VERSION or document["device_slug"] != device_slug:
        raise ValueError("device state is not an exact schema-2 document")
    latest = document["latest"]
    metrics = document["metrics"]
    if not isinstance(latest, dict) or not isinstance(metrics, dict):
        raise ValueError("current device state sections are invalid")
    if set(latest) != {"read_sequence", "write_sequence", "capacity_packets", "dropped_packets", "packet_size"}:
        raise ValueError("current device state latest section is invalid")
    if set(metrics) != {"observation_count", "initial", "observed_increase", "regression_count"}:
        raise ValueError("current device state metrics section is invalid")
    _integer(latest, "read_sequence", 2**64 - 1)
    _integer(latest, "write_sequence", 2**64 - 1)
    _integer(latest, "capacity_packets", 2**32 - 1)
    _integer(latest, "dropped_packets", 2**64 - 1)
    _integer(latest, "packet_size", 2**16 - 1)
    if _integer(metrics, "observation_count") <= 0:
        raise ValueError("current device state observation count is invalid")
    _integer(metrics, "initial", 2**64 - 1)
    _integer(metrics, "observed_increase", 2**64 - 1)
    _integer(metrics, "regression_count")


def main() -> int:
    if len(sys.argv) != _ARGUMENT_COUNT:
        return _fail("usage: inspect-legacy-device-state.py LAYOUT DEVICE_SLUG")
    try:
        state_path = _state_path(Path(sys.argv[1]))
        if not os.path.lexists(state_path):
            print("ABSENT")
            return 0
        mode = state_path.lstat().st_mode
        if stat.S_ISLNK(mode) or not stat.S_ISREG(mode):
            raise ValueError("device state must be a regular non-symlink file")
        raw = state_path.read_bytes()
        document = cast(
            object,
            json.loads(raw.decode("utf-8"), object_pairs_hook=_json_object, parse_constant=_reject_constant),
        )
        if not isinstance(document, dict):
            raise ValueError("device state is not a JSON object")
        schema_version = document.get("schema_version")
        if schema_version == _CURRENT_SCHEMA_VERSION:
            _validate_schema_two(state_path, sys.argv[2])
            print("CURRENT")
            return 0
        if schema_version != _LEGACY_SCHEMA_VERSION:
            raise ValueError("device state schema version is unknown")
        _validate_schema_one(state_path, sys.argv[2])
        identity = state_path.stat()
        print(f"LEGACY\t{state_path}\t{identity.st_dev}\t{identity.st_ino}")
        return 0
    except (OSError, UnicodeError, ValueError, tomllib.TOMLDecodeError, json.JSONDecodeError) as error:
        return _fail(str(error))


if __name__ == "__main__":
    raise SystemExit(main())
