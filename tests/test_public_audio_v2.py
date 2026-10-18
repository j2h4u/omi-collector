from __future__ import annotations

import hashlib
from typing import cast

import pytest

from omi_collector.capture.domain.public_audio_v2 import (
    FrozenPublicTime,
    FrozenRecordProjection,
    PublicBoundary,
    PublicProjection,
    Quality,
    TimelineSpan,
    canonical_manifest,
    derive_artifact_id,
    parse_v2_acknowledged,
    project_public_timeline,
)


def _time(
    seconds: int,
    *,
    causal: str = "clock-a",
    comparable: bool = True,
    quality: str = "known",
) -> FrozenPublicTime:
    public_quality = cast(Quality, quality)
    return FrozenPublicTime(public_quality, seconds, 1.0 if public_quality != "unknown" else None, causal, comparable)


def test_projection_keeps_jumps_and_reversals_as_record_labels() -> None:
    projection = project_public_timeline(
        (
            FrozenRecordProjection(2, _time(100)),
            FrozenRecordProjection(1, _time(500)),
            FrozenRecordProjection(3, _time(90)),
        )
    )

    assert [(span.packet_start, span.packet_next, span.seconds) for span in projection.timeline] == [
        (0, 2, 100),
        (2, 3, 500),
        (3, 6, 90),
    ]


def test_zero_packet_boundaries_accumulate_at_the_current_packet_position() -> None:
    projection = project_public_timeline(
        (
            FrozenRecordProjection(1, _time(100)),
            FrozenRecordProjection(0, _time(100), frozenset(("source_loss",))),
            FrozenRecordProjection(0, _time(100, comparable=False)),
            FrozenRecordProjection(1, _time(100)),
        )
    )

    assert projection.boundaries == (PublicBoundary(1, ("source_loss", "time_incomparable")),)
    assert [(span.packet_start, span.packet_next) for span in projection.timeline] == [(0, 1), (1, 2)]


def test_equal_public_labels_coalesce_only_with_matching_causal_facts() -> None:
    merged = project_public_timeline((FrozenRecordProjection(2, _time(100)), FrozenRecordProjection(3, _time(100))))
    split = project_public_timeline(
        (FrozenRecordProjection(2, _time(100)), FrozenRecordProjection(3, _time(100, causal="clock-b")))
    )

    assert len(merged.timeline) == 1
    assert len(split.timeline) == 2


def test_unknown_labels_remain_source_seconds_and_equivalent_mappings_coalesce() -> None:
    unknown = project_public_timeline((FrozenRecordProjection(2, _time(17, quality="unknown")),))
    equivalent = project_public_timeline(
        (
            FrozenRecordProjection(1, _time(100, causal="same-public-mapping")),
            FrozenRecordProjection(2, _time(100, causal="same-public-mapping")),
        )
    )

    assert unknown.timeline[0].quality == "unknown"
    assert unknown.timeline[0].seconds == 17
    assert len(equivalent.timeline) == 1


@pytest.mark.parametrize("packet_count", (-1, True))
def test_projection_rejects_negative_or_boolean_record_packet_counts(packet_count: int) -> None:
    with pytest.raises(ValueError, match="packet_count"):
        project_public_timeline((FrozenRecordProjection(packet_count, _time(100)),))


def test_projection_rejects_packet_sum_that_exceeds_unsigned_64_bit_range() -> None:
    with pytest.raises(ValueError, match="unsigned 64-bit"):
        project_public_timeline(
            (
                FrozenRecordProjection(0xFFFFFFFFFFFFFFFF, _time(100)),
                FrozenRecordProjection(1, _time(100)),
            )
        )


def test_all_zero_packet_input_is_not_a_publication() -> None:
    with pytest.raises(ValueError, match="packets"):
        project_public_timeline((FrozenRecordProjection(0, _time(100)),))


def test_identity_matches_domain_separated_frozen_inputs() -> None:
    nonce = bytes(range(32))
    config, descriptor = "a" * 64, "b" * 64
    expected = hashlib.sha256(
        b"omi-audio-v2-id\0" + nonce + (9).to_bytes(8, "big") + bytes.fromhex(config + descriptor)
    ).hexdigest()

    assert (
        derive_artifact_id(stage_nonce=nonce, order=9, config_sha256=config, descriptor_sha256=descriptor) == expected
    )


def test_manifest_is_exact_canonical_schema_and_hides_private_clock_membership() -> None:
    projection = PublicProjection(
        2,
        (TimelineSpan(0, 2, "unknown", 57, None),),
        (PublicBoundary(0, ("time_incomparable",)),),
    )
    manifest = canonical_manifest(artifact_id="a" * 64, order=1, audio_sha256="b" * 64, projection=projection)

    assert manifest.endswith(b"\n")
    assert manifest == (
        b'{"audio_sha256":"'
        + b"b" * 64
        + b'","boundaries":[{"at_packet":0,"facts":["time_incomparable"]}],'
        + b'"format":"omi-audio-v2","id":"'
        + b"a" * 64
        + b'","order":1,"packet_count":2,"timeline":[{"packets":[0,2],'
        + b'"quality":"unknown","source_seconds":57}]}\n'
    )
    assert b"clock" not in manifest and b"causal" not in manifest


@pytest.mark.parametrize(
    "projection",
    (
        PublicProjection(2, (TimelineSpan(0, 1, "known", 1, 0.0),), ()),
        PublicProjection(1, (TimelineSpan(0, 1, "known", True, 0.0),), ()),
        PublicProjection(1, (TimelineSpan(0, 1, "known", 1, float("nan")),), ()),
        PublicProjection(1, (TimelineSpan(0, 1, "unknown", 1, 0.0),), ()),
        PublicProjection(1, (TimelineSpan(0, 1, "known", 1, 0.0),), (PublicBoundary(1, ()),)),
    ),
)
def test_manifest_rejects_invalid_ranges_times_and_boundaries(projection: PublicProjection) -> None:
    with pytest.raises(ValueError):
        canonical_manifest(artifact_id="a" * 64, order=1, audio_sha256="b" * 64, projection=projection)


def test_ack_parser_ignores_other_root_fields_and_returns_pairs() -> None:
    acknowledged = [{"id": "a" * 64, "audio_sha256": "b" * 64}]

    assert parse_v2_acknowledged({"acknowledged": acknowledged, "consumer": {"cursor": 3}}) == (("a" * 64, "b" * 64),)


def test_ack_parser_rejects_duplicate_or_extra_entry_fields() -> None:
    item = {"id": "a" * 64, "audio_sha256": "b" * 64}

    with pytest.raises(ValueError, match="duplicate"):
        parse_v2_acknowledged({"acknowledged": [item, item]})
    with pytest.raises(ValueError, match="exactly"):
        parse_v2_acknowledged({"acknowledged": [{**item, "order": 1}]})
