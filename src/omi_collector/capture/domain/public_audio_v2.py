"""Frozen public packet timeline projection for v2 audio publications."""

from __future__ import annotations

import json
import math
import re
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from hashlib import sha256
from typing import Final, Literal

_HEX_256: Final = re.compile(r"[0-9a-f]{64}\Z")
_UINT32_MAX: Final = 0xFFFFFFFF
_UINT64_MAX: Final = 0xFFFFFFFFFFFFFFFF
_STAGE_NONCE_BYTES: Final = 32
_BOUNDARY_FACTS: Final = frozenset(("source_loss", "time_incomparable"))

BoundaryFact = Literal["source_loss", "time_incomparable"]
Quality = Literal["known", "approximate", "unknown"]


@dataclass(frozen=True, slots=True)
class FrozenPublicTime:
    quality: Quality
    seconds: int
    utc_uncertainty_seconds: float | None
    causal_membership: str
    comparable_to_previous: bool


@dataclass(frozen=True, slots=True)
class FrozenRecordProjection:
    packet_count: int
    time: FrozenPublicTime
    boundary_before: frozenset[BoundaryFact] = frozenset()


@dataclass(frozen=True, slots=True)
class TimelineSpan:
    packet_start: int
    packet_next: int
    quality: Quality
    seconds: int
    utc_uncertainty_seconds: float | None


@dataclass(frozen=True, slots=True)
class PublicBoundary:
    at_packet: int
    facts: tuple[BoundaryFact, ...]


@dataclass(frozen=True, slots=True)
class PublicProjection:
    packet_count: int
    timeline: tuple[TimelineSpan, ...]
    boundaries: tuple[PublicBoundary, ...]


def project_public_timeline(records: Iterable[FrozenRecordProjection]) -> PublicProjection:
    """Project frozen record labels and explicit causal facts to packet coordinates."""
    spans: list[TimelineSpan] = []
    boundary_facts: dict[int, set[BoundaryFact]] = {}
    packet = 0
    previous_key: tuple[object, ...] | None = None
    pending_boundary = False
    for record in records:
        time, facts = _validated_record_projection(record)
        if facts:
            boundary_facts.setdefault(packet, set()).update(facts)
            pending_boundary = True
        if record.packet_count == 0:
            continue
        key = (
            time.quality,
            time.seconds,
            time.utc_uncertainty_seconds,
            time.causal_membership,
            time.comparable_to_previous,
        )
        if record.packet_count > _UINT64_MAX - packet:
            raise ValueError("projected packet_count exceeds unsigned 64-bit range")
        next_packet = packet + record.packet_count
        if spans and key == previous_key and not pending_boundary:
            prior = spans[-1]
            spans[-1] = TimelineSpan(
                prior.packet_start,
                next_packet,
                prior.quality,
                prior.seconds,
                prior.utc_uncertainty_seconds,
            )
        else:
            spans.append(TimelineSpan(packet, next_packet, time.quality, time.seconds, time.utc_uncertainty_seconds))
        packet = next_packet
        previous_key = key
        pending_boundary = False
    if packet == 0:
        raise ValueError("public projection must contain packets")
    boundaries = tuple(
        PublicBoundary(position, tuple(sorted(facts))) for position, facts in sorted(boundary_facts.items())
    )
    return PublicProjection(packet, tuple(spans), boundaries)


def _validated_record_projection(
    record: FrozenRecordProjection,
) -> tuple[FrozenPublicTime, frozenset[BoundaryFact]]:
    _uint(record.packet_count, _UINT64_MAX, "record packet_count")
    time = record.time
    if time.quality not in ("known", "approximate", "unknown"):
        raise ValueError("invalid public time quality")
    _uint(time.seconds, _UINT32_MAX, "record seconds")
    if not isinstance(time.causal_membership, str) or not time.causal_membership:
        raise ValueError("causal_membership must be nonempty")
    if not isinstance(time.comparable_to_previous, bool):
        raise ValueError("comparable_to_previous must be a boolean")
    uncertainty = time.utc_uncertainty_seconds
    if time.quality == "unknown":
        if uncertainty is not None:
            raise ValueError("unknown public times cannot have UTC uncertainty")
    elif (
        isinstance(uncertainty, bool)
        or not isinstance(uncertainty, (int, float))
        or not math.isfinite(uncertainty)
        or uncertainty < 0
    ):
        raise ValueError("public time uncertainty must be finite and nonnegative")
    facts = set(record.boundary_before)
    if any(fact not in _BOUNDARY_FACTS for fact in facts):
        raise ValueError("invalid public boundary fact")
    if not time.comparable_to_previous:
        facts.add("time_incomparable")
    return time, frozenset(facts)


def derive_artifact_id(*, stage_nonce: bytes, order: int, config_sha256: str, descriptor_sha256: str) -> str:
    """Derive a stable, domain-separated opaque publication identifier."""
    if not isinstance(stage_nonce, bytes) or len(stage_nonce) != _STAGE_NONCE_BYTES:
        raise ValueError(f"stage_nonce must contain {_STAGE_NONCE_BYTES} bytes")
    _uint(order, _UINT64_MAX, "order")
    _digest(config_sha256, "config_sha256")
    _digest(descriptor_sha256, "descriptor_sha256")
    return sha256(
        b"omi-audio-v2-id\0"
        + stage_nonce
        + order.to_bytes(8, "big")
        + bytes.fromhex(config_sha256)
        + bytes.fromhex(descriptor_sha256)
    ).hexdigest()


def canonical_manifest(*, artifact_id: str, order: int, audio_sha256: str, projection: PublicProjection) -> bytes:
    """Serialize the exact seven-field public manifest as canonical UTF-8 JSON."""
    _digest(artifact_id, "artifact_id")
    _digest(audio_sha256, "audio_sha256")
    _uint(order, _UINT64_MAX, "order")
    _uint(projection.packet_count, _UINT64_MAX, "packet_count", positive=True)
    timeline = _manifest_timeline(projection)
    boundaries = _manifest_boundaries(projection)
    result = {
        "format": "omi-audio-v2",
        "id": artifact_id,
        "order": order,
        "audio_sha256": audio_sha256,
        "packet_count": projection.packet_count,
        "timeline": timeline,
        "boundaries": boundaries,
    }
    return (json.dumps(result, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8")


def _manifest_timeline(projection: PublicProjection) -> list[dict[str, object]]:
    timeline: list[dict[str, object]] = []
    expected_start = 0
    for span in projection.timeline:
        _uint(span.packet_start, _UINT64_MAX, "packet_start")
        _uint(span.packet_next, _UINT64_MAX, "packet_next", positive=True)
        if span.packet_start != expected_start or span.packet_next <= span.packet_start:
            raise ValueError("timeline must cover positive contiguous packet ranges")
        if span.quality not in ("known", "approximate", "unknown"):
            raise ValueError("invalid timeline quality")
        _uint(span.seconds, _UINT32_MAX, "seconds")
        if span.quality == "unknown":
            if span.utc_uncertainty_seconds is not None:
                raise ValueError("unknown timeline spans cannot have UTC uncertainty")
            timeline.append(
                {"packets": [span.packet_start, span.packet_next], "quality": "unknown", "source_seconds": span.seconds}
            )
        else:
            uncertainty = span.utc_uncertainty_seconds
            if (
                isinstance(uncertainty, bool)
                or not isinstance(uncertainty, (int, float))
                or not math.isfinite(uncertainty)
                or uncertainty < 0
            ):
                raise ValueError("known timeline uncertainty must be finite and nonnegative")
            timeline.append(
                {
                    "packets": [span.packet_start, span.packet_next],
                    "quality": span.quality,
                    "unix_seconds": span.seconds,
                    "utc_uncertainty_seconds": uncertainty,
                }
            )
        expected_start = span.packet_next
    if expected_start != projection.packet_count:
        raise ValueError("timeline must exhaustively cover packet_count")
    return timeline


def _manifest_boundaries(projection: PublicProjection) -> list[dict[str, object]]:
    boundaries: list[dict[str, object]] = []
    prior_position = -1
    for boundary in projection.boundaries:
        _uint(boundary.at_packet, projection.packet_count, "at_packet")
        if boundary.at_packet <= prior_position:
            raise ValueError("boundary positions must be unique and sorted")
        if not boundary.facts or len(set(boundary.facts)) != len(boundary.facts):
            raise ValueError("boundary facts must be nonempty and unique")
        if tuple(sorted(boundary.facts)) != boundary.facts or any(
            fact not in _BOUNDARY_FACTS for fact in boundary.facts
        ):
            raise ValueError("boundary facts must be known and sorted")
        boundaries.append({"at_packet": boundary.at_packet, "facts": list(boundary.facts)})
        prior_position = boundary.at_packet
    return boundaries


def parse_v2_acknowledged(value: object) -> tuple[tuple[str, str], ...]:
    """Parse acknowledgement entries while ignoring consumer-owned root fields."""
    if not isinstance(value, Mapping):
        raise ValueError("checkpoint must be a mapping")
    acknowledged = value.get("acknowledged")
    if not isinstance(acknowledged, list):
        raise ValueError("acknowledged must be a list")
    result: list[tuple[str, str]] = []
    seen: set[str] = set()
    for entry in acknowledged:
        if not isinstance(entry, Mapping) or set(entry) != {"id", "audio_sha256"}:
            raise ValueError("acknowledgement entries must contain exactly id and audio_sha256")
        artifact_id = _digest(entry["id"], "id")
        audio_digest = _digest(entry["audio_sha256"], "audio_sha256")
        if artifact_id in seen:
            raise ValueError("duplicate acknowledgement id")
        seen.add(artifact_id)
        result.append((artifact_id, audio_digest))
    return tuple(result)


def _digest(value: object, name: str) -> str:
    if not isinstance(value, str) or _HEX_256.fullmatch(value) is None:
        raise ValueError(f"{name} must be lowercase SHA-256 hex")
    return value


def _uint(value: object, maximum: int, name: str, *, positive: bool = False) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value < int(positive) or value > maximum:
        qualifier = "positive " if positive else ""
        raise ValueError(f"{name} must be a {qualifier}unsigned integer")
