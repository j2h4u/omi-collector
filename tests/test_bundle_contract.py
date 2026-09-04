"""Tests for the canonical published-bundle contract."""

import pytest

from omi_collector.capture.adapters.bundle_contract import BundleManifest, SealedReceipt
from omi_collector.capture.domain.ring_protocol import RECORD_SIZE

MANIFEST = {
    "device_slug": "omi-cv1",
    "start_sequence": 10,
    "next_sequence": 12,
    "record_count": 2,
    "record_size": RECORD_SIZE,
    "raw_sha256": "a" * 64,
}
RECEIPT = {"attempt_id": "b" * 32, "raw_sha256": "a" * 64, "status": "sealed"}


def test_canonical_values_round_trip() -> None:
    manifest = BundleManifest.from_json(MANIFEST)
    receipt = SealedReceipt.from_json(RECEIPT)

    assert manifest.as_dict() == MANIFEST
    assert receipt.as_dict() == RECEIPT


@pytest.mark.parametrize(
    "field,value",
    [
        ("start_sequence", True),
        ("record_count", 0),
        ("next_sequence", 11),
        ("record_size", RECORD_SIZE + 1),
        ("raw_sha256", "A" * 64),
    ],
)
def test_manifest_rejects_invalid_values(field: str, value: object) -> None:
    malformed = {**MANIFEST, field: value}

    with pytest.raises(ValueError):
        BundleManifest.from_json(malformed)


@pytest.mark.parametrize("extra", ["completion", "unexpected"])
def test_manifest_and_receipt_reject_extra_fields(extra: str) -> None:
    with pytest.raises(ValueError):
        BundleManifest.from_json({**MANIFEST, extra: 1})
    with pytest.raises(ValueError):
        SealedReceipt.from_json({**RECEIPT, extra: 1})


@pytest.mark.parametrize("field,value", [("attempt_id", "A" * 32), ("raw_sha256", "g" * 64), ("status", "open")])
def test_receipt_rejects_invalid_values(field: str, value: object) -> None:
    with pytest.raises(ValueError):
        SealedReceipt.from_json({**RECEIPT, field: value})
