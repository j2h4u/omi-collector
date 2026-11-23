from __future__ import annotations

import hashlib
from collections.abc import Iterable

import pytest

from omi_collector.capture.adapters.physical_audio import iter_physical_audio, prepare_physical_audio
from omi_collector.capture.adapters.v2_audio import hash_v2_audio
from omi_collector.capture.domain.physical_recovery import PhysicalSpan
from omi_collector.capture.domain.public_audio_v2 import canonical_manifest
from test_public_audio_v2_integration import _record


def _sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _fixture() -> tuple[dict[str, object], dict[str, bytes]]:
    one, two = _record(1, marker=10), _record(2, marker=11)
    packet = bytes((8, 42))
    seam = bytes(4) + bytes((2,)) + packet + bytes((255,)) + bytes(524)
    anchor = b"unique independent overlap"
    original = one + seam + two + anchor
    repeated = anchor + b"tail"
    seam_packets = hashlib.sha256(
        b"omi-physical-seam-packets-v1\0" + len(packet).to_bytes(4, "big") + packet
    ).hexdigest()
    first_end = len(one)
    second_start = first_end + len(seam)
    anchor_start = second_start + len(two)
    descriptor: dict[str, object] = {
        "format": "omi-audio-v2-physical-byte-span-v1",
        "source_id": "a" * 32,
        "source_sha256": _sha(original),
        "source_bytes": len(original),
        "spans": [
            {"byte_start": 0, "byte_end": first_end, "sha256": _sha(one)},
            {"byte_start": second_start, "byte_end": anchor_start, "sha256": _sha(two)},
        ],
        "record_count": 2,
        "seams": [
            {
                "byte_start": first_end,
                "byte_end": second_start,
                "sha256": _sha(seam),
                "status": "unresolved",
                "decoded_prefix_bytes": 7,
                "decoded_packet_count": 1,
                "decoded_packets_sha256": seam_packets,
            }
        ],
        "anchor": {
            "repeat_source_id": "b" * 32,
            "repeat_source_sha256": _sha(repeated),
            "source_offset": anchor_start,
            "repeat_offset": 0,
            "length": len(anchor),
            "sha256": _sha(anchor),
        },
        "record_projection_sha256": "0" * 64,
    }
    return descriptor, {"a" * 32: original, "b" * 32: repeated}


def _read(sources: dict[str, bytes], span: PhysicalSpan) -> Iterable[bytes]:
    value = sources[span.source_id][span.offset : span.offset + span.length]
    for index in range(0, len(value), 17):
        yield value[index : index + 17]


def test_exact_physical_stream_includes_partial_seam_once_without_sequence() -> None:
    descriptor, sources = _fixture()
    records = tuple(
        iter_physical_audio(
            descriptor, lambda span: _read(sources, span), lambda key: (len(sources[key]), _sha(sources[key]))
        )
    )
    assert [len(item.packets) for item in records] == [1, 1, 1]
    assert [item.boundary_before for item in records] == [True, False, True]
    assert hash_v2_audio(packet for item in records for packet in item.packets).packet_count == 3

    prepared = prepare_physical_audio(
        descriptor, lambda span: _read(sources, span), lambda key: (len(sources[key]), _sha(sources[key]))
    )
    assert prepared.audio.packet_count == prepared.projection.packet_count == 3
    manifest = canonical_manifest(
        artifact_id="a" * 64, order=78, audio_sha256=prepared.audio.sha256, projection=prepared.projection
    )
    assert b"start_sequence" not in manifest and b"source_id" not in manifest


def test_source_or_anchor_change_fails_before_stream_can_be_committed() -> None:
    descriptor, sources = _fixture()
    original = sources["a" * 32]
    sources["a" * 32] = original[:-1] + b"!"
    with pytest.raises(ValueError, match="custody changed"):
        tuple(
            iter_physical_audio(
                descriptor, lambda span: _read(sources, span), lambda key: (len(sources[key]), _sha(sources[key]))
            )
        )


def test_stream_rechecks_bytes_even_if_metadata_is_unchanged() -> None:
    descriptor, sources = _fixture()
    frozen = {key: (len(value), _sha(value)) for key, value in sources.items()}
    original = sources["a" * 32]
    sources["a" * 32] = original[:5] + bytes((original[5] ^ 1,)) + original[6:]
    with pytest.raises(ValueError):
        tuple(iter_physical_audio(descriptor, lambda span: _read(sources, span), frozen.__getitem__))


@pytest.mark.parametrize("overlap", [False, True])
def test_ordered_stream_preserves_packets_and_only_removes_proven_overlap(overlap: bool) -> None:
    first, second, third = (_record(n, marker=10 + n) for n in range(1, 4))
    previous = first + second
    current = second + third if overlap else third
    skipped = second if overlap else b""
    emitted = third
    source_id, predecessor_id = "a" * 32, "b" * 32
    descriptor: dict[str, object] = {
        "format": "omi-audio-v2-ordered-physical-v1",
        "source_id": source_id,
        "source_sha256": _sha(current),
        "source_bytes": len(current),
        "spans": [{"byte_start": len(skipped), "byte_end": len(current), "sha256": _sha(emitted)}],
        "seams": [],
        "record_count": 1,
        "ordering": {
            "predecessor_artifact": "c" * 64,
            "predecessor_source_id": predecessor_id,
            "predecessor_sha256": _sha(previous),
            "predecessor_start": 10,
            "source_start": 20,
            "read_begin": 20,
            "read_count": 2,
        },
        "overlap": {"record_count": int(overlap), "sha256": _sha(skipped)},
        "record_projection_sha256": "0" * 64,
    }
    sources = {source_id: current, predecessor_id: previous}
    frozen = {key: (len(value), _sha(value)) for key, value in sources.items()}
    records = tuple(iter_physical_audio(descriptor, lambda span: _read(sources, span), frozen.__getitem__))
    assert len(records) == 1 and records[0].raw_record == third
    assert records[0].boundary_before
    sources[predecessor_id] = previous[:-1] + bytes((previous[-1] ^ 1,))
    with pytest.raises(ValueError, match="predecessor bytes changed"):
        tuple(iter_physical_audio(descriptor, lambda span: _read(sources, span), frozen.__getitem__))
