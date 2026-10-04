"""Tests for the canonical published-bundle contract."""

import pytest

from omi_collector.capture.adapters.bundle_contract import BundleManifest, SealedReceipt
from omi_collector.capture.domain.ring_protocol import RECORD_SIZE

MANIFEST = {
    "schema_version": 2,
    "start_sequence": 0,
    "next_sequence": 1,
    "record_count": 1,
    "record_size": RECORD_SIZE,
    "raw_sha256": "a" * 64,
}
RECEIPT = {"attempt_id": "b" * 32, "raw_sha256": "a" * 64, "status": "sealed"}


def test_canonical_values_round_trip() -> None:
    manifest = BundleManifest.from_json(MANIFEST)
    receipt = SealedReceipt.from_json(RECEIPT)

    assert manifest.as_dict() == MANIFEST
    assert receipt.as_dict() == RECEIPT


@pytest.mark.parametrize("field", ["schema_version", "start_sequence", "next_sequence", "record_count", "record_size"])
@pytest.mark.parametrize("value", [True, 1.5])
def test_manifest_rejects_non_integer_numeric_fields(field: str, value: object) -> None:
    malformed = {**MANIFEST, field: value}

    with pytest.raises(ValueError):
        BundleManifest.from_json(malformed)


@pytest.mark.parametrize(
    "field,value",
    [
        ("next_sequence", 2),
        ("record_size", RECORD_SIZE + 1),
        ("raw_sha256", "A" * 64),
    ],
)
def test_manifest_rejects_inconsistent_or_invalid_values(field: str, value: object) -> None:
    malformed = {**MANIFEST, field: value}

    with pytest.raises(ValueError):
        BundleManifest.from_json(malformed)


def test_manifest_rejects_zero_records_with_consistent_empty_range() -> None:
    malformed = {**MANIFEST, "next_sequence": 0, "record_count": 0}

    with pytest.raises(ValueError, match="record_count must be positive"):
        BundleManifest.from_json(malformed)


def test_manifest_rejects_negative_start_with_consistent_range() -> None:
    malformed = {**MANIFEST, "start_sequence": -1, "next_sequence": 0}

    with pytest.raises(ValueError, match="start_sequence must be non-negative"):
        BundleManifest.from_json(malformed)


@pytest.mark.parametrize("schema_version", [1, 3])
def test_manifest_rejects_unsupported_schema_version(schema_version: int) -> None:
    with pytest.raises(ValueError, match="schema_version is invalid"):
        BundleManifest.from_json({**MANIFEST, "schema_version": schema_version})


@pytest.mark.parametrize("payload", [None, [], "manifest", 1])
def test_parsers_reject_non_object_payloads(payload: object) -> None:
    with pytest.raises(ValueError):
        BundleManifest.from_json(payload)
    with pytest.raises(ValueError):
        SealedReceipt.from_json(payload)


def test_parsers_reject_each_missing_canonical_field() -> None:
    for field in MANIFEST:
        with pytest.raises(ValueError):
            BundleManifest.from_json({key: value for key, value in MANIFEST.items() if key != field})
    for field in RECEIPT:
        with pytest.raises(ValueError):
            SealedReceipt.from_json({key: value for key, value in RECEIPT.items() if key != field})


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


@pytest.mark.parametrize("field", ["attempt_id", "raw_sha256", "status"])
def test_receipt_rejects_non_string_fields(field: str) -> None:
    with pytest.raises(ValueError):
        SealedReceipt.from_json({**RECEIPT, field: 1})
