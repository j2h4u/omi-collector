from __future__ import annotations

from copy import deepcopy
from typing import cast

import pytest

from omi_collector.capture.domain.physical_publication import validate_physical_descriptor


def _descriptor() -> dict[str, object]:
    return {
        "format": "omi-audio-v2-physical-byte-span-v1",
        "source_id": "a" * 32,
        "source_sha256": "b" * 64,
        "source_bytes": 893,
        "spans": [
            {"byte_start": 0, "byte_end": 444, "sha256": "c" * 64},
            {"byte_start": 446, "byte_end": 890, "sha256": "d" * 64},
        ],
        "record_count": 2,
        "seams": [
            {
                "byte_start": 444,
                "byte_end": 446,
                "sha256": "e" * 64,
                "status": "unresolved",
                "decoded_prefix_bytes": 1,
                "decoded_packet_count": 1,
                "decoded_packets_sha256": "4" * 64,
            }
        ],
        "anchor": {
            "repeat_source_id": "f" * 32,
            "repeat_source_sha256": "1" * 64,
            "source_offset": 890,
            "repeat_offset": 0,
            "length": 3,
            "sha256": "2" * 64,
        },
        "record_projection_sha256": "3" * 64,
    }


def test_descriptor_covers_every_source_byte_without_device_sequence() -> None:
    descriptor = _descriptor()
    assert validate_physical_descriptor(descriptor) == descriptor
    assert "start_sequence" not in descriptor


@pytest.mark.parametrize("change", ["gap", "overlap", "wrong_count", "same_anchor", "seam_status", "extra_field"])
def test_descriptor_rejects_unaccounted_or_ambiguous_custody(change: str) -> None:
    value = deepcopy(_descriptor())
    anchor = cast(dict[str, object], value["anchor"])
    seams = cast(list[dict[str, object]], value["seams"])
    if change == "gap":
        seams[0]["byte_start"] = 445
    elif change == "overlap":
        seams[0]["byte_start"] = 443
    elif change == "wrong_count":
        value["record_count"] = 3
    elif change == "same_anchor":
        anchor["repeat_source_id"] = value["source_id"]
    elif change == "seam_status":
        seams[0]["status"] = "discarded"
    else:
        value["start_sequence"] = 1
    with pytest.raises(ValueError):
        validate_physical_descriptor(value)


def test_ordered_descriptor_covers_source_without_a_fabricated_anchor() -> None:
    descriptor = _descriptor()
    descriptor["format"] = "omi-audio-v2-ordered-physical-v1"
    descriptor["source_bytes"] = 888
    descriptor["spans"] = [{"byte_start": 0, "byte_end": 888, "sha256": "c" * 64}]
    descriptor["seams"] = []
    del descriptor["anchor"]
    descriptor["ordering"] = {
        "predecessor_artifact": "a" * 64,
        "predecessor_source_id": "f" * 32,
        "predecessor_sha256": "b" * 64,
        "predecessor_start": 10,
        "source_start": 20,
        "read_begin": 21,
        "read_count": 10,
    }
    descriptor["overlap"] = {"record_count": 0, "sha256": "e" * 64}
    assert validate_physical_descriptor(descriptor) == descriptor
    ordering = cast(dict[str, object], descriptor["ordering"])
    ordering["source_start"] = 10
    with pytest.raises(ValueError, match="order"):
        validate_physical_descriptor(descriptor)
