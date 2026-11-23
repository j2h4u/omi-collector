from __future__ import annotations

import hashlib
from functools import partial

import pytest

from omi_collector.capture.adapters.physical_audio import prepare_physical_audio
from omi_collector.capture.adapters.physical_proof import prepare_physical_proof
from omi_collector.capture.adapters.ready_bundles import _v2_proof_content_sha256, _V2ProofContent
from omi_collector.capture.domain.replay_proof import unpack_normalization_runs, unpack_page_index
from test_physical_audio import _fixture, _read, _sha
from test_public_audio_v2_integration import _record


def test_physical_proof_binds_records_and_seam_without_synthetic_record() -> None:
    descriptor, sources = _fixture()
    reader = partial(_read, sources)
    identity = {key: (len(value), _sha(value)) for key, value in sources.items()}.__getitem__
    audio = prepare_physical_audio(descriptor, reader, identity)
    descriptor["record_projection_sha256"] = audio.record_projection_sha256
    with prepare_physical_proof(descriptor, reader, identity, packet_count=3) as proof:
        assert proof.records_sha256 == hashlib.sha256(_record(1, marker=10) + _record(2, marker=11)).hexdigest()
        assert unpack_page_index(proof.page_index, record_count=2)[0][1] == 2
        runs = unpack_normalization_runs(proof.packed_runs, record_count=2)
        assert [(run.first_record_index, run.next_record_index, run.boundary_before) for run in runs] == [
            (0, 1, 2),
            (1, 2, 2),
        ]
        content = _V2ProofContent(
            "a" * 64,
            1,
            audio.audio.sha256,
            "b" * 64,
            proof.records_sha256,
            2,
            3,
            audio.record_projection_sha256,
            proof.page_index,
            proof.packed_runs,
            proof.descriptor,
        )
        baseline = _v2_proof_content_sha256(content)
        assert baseline != _v2_proof_content_sha256(
            _V2ProofContent(
                "a" * 64,
                1,
                audio.audio.sha256,
                "b" * 64,
                proof.records_sha256,
                2,
                2,
                audio.record_projection_sha256,
                proof.page_index,
                proof.packed_runs,
                proof.descriptor,
            )
        )


def test_physical_proof_pages_are_bounded_to_128_records() -> None:
    records = b"".join(_record(index) for index in range(130))
    anchor = b"unique bounded proof anchor"
    sources = {"a" * 32: records + anchor, "b" * 32: anchor}
    descriptor: dict[str, object] = {
        "format": "omi-audio-v2-physical-byte-span-v1",
        "source_id": "a" * 32,
        "source_sha256": _sha(sources["a" * 32]),
        "source_bytes": len(sources["a" * 32]),
        "spans": [{"byte_start": 0, "byte_end": len(records), "sha256": _sha(records)}],
        "record_count": 130,
        "seams": [],
        "anchor": {
            "repeat_source_id": "b" * 32,
            "repeat_source_sha256": _sha(anchor),
            "source_offset": len(records),
            "repeat_offset": 0,
            "length": len(anchor),
            "sha256": _sha(anchor),
        },
        "record_projection_sha256": "0" * 64,
    }
    reader = partial(_read, sources)
    identity = {key: (len(value), _sha(value)) for key, value in sources.items()}.__getitem__
    audio = prepare_physical_audio(descriptor, reader, identity)
    descriptor["record_projection_sha256"] = audio.record_projection_sha256
    with prepare_physical_proof(descriptor, reader, identity, packet_count=130) as proof:
        assert [row[1] for row in unpack_page_index(proof.page_index, record_count=130)] == [128, 2]


@pytest.mark.parametrize("failure", ["projection", "packet_count", "changed_bytes"])
def test_physical_proof_never_yields_unverified_pages(failure: str) -> None:
    descriptor, sources = _fixture()
    identity = {key: (len(value), _sha(value)) for key, value in sources.items()}
    reader = partial(_read, sources)
    prepared = prepare_physical_audio(descriptor, reader, identity.__getitem__)
    descriptor["record_projection_sha256"] = prepared.record_projection_sha256
    count = 3
    if failure == "projection":
        descriptor["record_projection_sha256"] = "0" * 64
    elif failure == "packet_count":
        count = 2
    else:
        original = sources["a" * 32]
        sources["a" * 32] = original[:-1] + b"!"
    with (
        pytest.raises(ValueError),
        prepare_physical_proof(descriptor, reader, identity.__getitem__, packet_count=count),
    ):
        pytest.fail("proof escaped before verification")
