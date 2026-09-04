"""Canonical manifest and sealed-receipt values for published bundles."""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass

from ..domain.ring_protocol import RECORD_SIZE

_SHA256 = re.compile(r"[0-9a-f]{64}")
_ATTEMPT_ID = re.compile(r"[0-9a-f]{32}")


def _int(value: object, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{name} must be an integer")
    return value


def _string(value: object, name: str) -> str:
    if not isinstance(value, str):
        raise ValueError(f"{name} must be a string")
    return value


@dataclass(frozen=True, slots=True)
class BundleManifest:
    """The exact six-field manifest written beside records.bin."""

    device_slug: str
    start_sequence: int
    next_sequence: int
    record_count: int
    record_size: int
    raw_sha256: str

    @classmethod
    def from_json(cls, value: object) -> BundleManifest:
        if not isinstance(value, Mapping):
            raise ValueError("manifest must be a JSON object")
        expected = {
            "device_slug",
            "start_sequence",
            "next_sequence",
            "record_count",
            "record_size",
            "raw_sha256",
        }
        if set(value) != expected:
            raise ValueError("manifest schema is not canonical")
        manifest = cls(
            _string(value.get("device_slug"), "device_slug"),
            _int(value.get("start_sequence"), "start_sequence"),
            _int(value.get("next_sequence"), "next_sequence"),
            _int(value.get("record_count"), "record_count"),
            _int(value.get("record_size"), "record_size"),
            _string(value.get("raw_sha256"), "raw_sha256"),
        )
        if manifest.start_sequence < 0:
            raise ValueError("start_sequence must be non-negative")
        if manifest.record_count <= 0:
            raise ValueError("record_count must be positive")
        if manifest.next_sequence != manifest.start_sequence + manifest.record_count:
            raise ValueError("manifest sequence range is invalid")
        if manifest.record_size != RECORD_SIZE:
            raise ValueError("record_size is invalid")
        if _SHA256.fullmatch(manifest.raw_sha256) is None:
            raise ValueError("raw_sha256 is invalid")
        return manifest

    def as_dict(self) -> dict[str, object]:
        return {
            "device_slug": self.device_slug,
            "start_sequence": self.start_sequence,
            "next_sequence": self.next_sequence,
            "record_count": self.record_count,
            "record_size": self.record_size,
            "raw_sha256": self.raw_sha256,
        }


@dataclass(frozen=True, slots=True)
class SealedReceipt:
    """The exact three-field receipt authenticating a sealed bundle."""

    attempt_id: str
    raw_sha256: str
    status: str = "sealed"

    @classmethod
    def from_json(cls, value: object) -> SealedReceipt:
        if not isinstance(value, Mapping) or set(value) != {"attempt_id", "raw_sha256", "status"}:
            raise ValueError("sealed receipt schema is not canonical")
        receipt = cls(
            _string(value.get("attempt_id"), "attempt_id"),
            _string(value.get("raw_sha256"), "raw_sha256"),
            _string(value.get("status"), "status"),
        )
        if _ATTEMPT_ID.fullmatch(receipt.attempt_id) is None:
            raise ValueError("attempt_id is invalid")
        if _SHA256.fullmatch(receipt.raw_sha256) is None:
            raise ValueError("raw_sha256 is invalid")
        if receipt.status != "sealed":
            raise ValueError("receipt status is invalid")
        return receipt

    def as_dict(self) -> dict[str, object]:
        return {"attempt_id": self.attempt_id, "raw_sha256": self.raw_sha256, "status": self.status}
