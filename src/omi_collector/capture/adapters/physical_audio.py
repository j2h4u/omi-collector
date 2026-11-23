"""Validate and read ordered physical audio without device sequence identities."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable, Iterable, Iterator
from dataclasses import dataclass
from functools import partial
from typing import Literal, Protocol, cast

from omi_collector.capture.domain.opus_duration import inspect_mono_20ms_record
from omi_collector.capture.domain.physical_overlap import unique_suffix_prefix_overlap
from omi_collector.capture.domain.physical_publication import validate_physical_descriptor
from omi_collector.capture.domain.physical_recovery import (
    PhysicalSegment,
    PhysicalSpan,
    inspect_partial_seam,
    iter_physical_records,
)
from omi_collector.capture.domain.public_audio_v2 import (
    FrozenPublicTime,
    FrozenRecordProjection,
    PublicProjection,
    PublicTimelineBuilder,
)

from .v2_audio import V2AudioDigest, hash_v2_audio

_MAX_BOUNDARY_BYTES = 65536
_SEAM_DOMAIN = b"omi-physical-seam-packets-v1\0"


@dataclass(frozen=True, slots=True)
class PhysicalAudioRecord:
    timestamp: int
    packets: tuple[bytes, ...]
    boundary_before: bool
    raw_record: bytes | None


@dataclass(frozen=True, slots=True)
class PreparedPhysicalAudio:
    audio: V2AudioDigest
    projection: PublicProjection
    record_projection_sha256: str


class _SourceVerifier:
    def __init__(self, needle: bytes | None) -> None:
        self.digest = hashlib.sha256()
        self.needle = needle
        self.tail = b""
        self.offset = 0
        self.matches: list[int] = []

    def feed(self, chunk: bytes) -> None:
        self.digest.update(chunk)
        if self.needle is None:
            self.offset += len(chunk)
            return
        combined = self.tail + chunk
        cursor = 0
        while (found := combined.find(self.needle, cursor)) != -1:
            self.matches.append(self.offset - len(self.tail) + found)
            if len(self.matches) > 1:
                raise ValueError("physical anchor is not unique")
            cursor = found + 1
        self.offset += len(chunk)
        self.tail = combined[-(len(self.needle) - 1) :] if len(self.needle) > 1 else b""


def _small_span(read_span: Callable[[PhysicalSpan], Iterable[bytes]], span: PhysicalSpan) -> bytes:
    if span.length > _MAX_BOUNDARY_BYTES:
        raise ValueError("physical boundary exceeds the bounded reader")
    result = bytearray()
    for chunk in read_span(span):
        if not isinstance(chunk, bytes) or len(result) + len(chunk) > span.length:
            raise ValueError("physical boundary reader exceeded its span")
        result.extend(chunk)
    if len(result) != span.length:
        raise ValueError("physical boundary reader ended early")
    return bytes(result)


def _packet_digest(packets: tuple[bytes, ...]) -> str:
    digest = hashlib.sha256(_SEAM_DOMAIN)
    for packet in packets:
        digest.update(len(packet).to_bytes(4, "big"))
        digest.update(packet)
    return digest.hexdigest()


def _parts(data: dict[str, object]) -> list[tuple[int, str, dict[str, object]]]:
    spans = cast(list[dict[str, object]], data["spans"])
    seams = cast(list[dict[str, object]], data["seams"])
    parts = [(cast(int, item["byte_start"]), "span", item) for item in spans]
    parts += [(cast(int, item["byte_start"]), "seam", item) for item in seams]
    if "anchor" in data:
        anchor = cast(dict[str, object], data["anchor"])
        parts.append((cast(int, anchor["source_offset"]), "anchor", anchor))
    else:
        overlap = cast(dict[str, object], data["overlap"])
        length = cast(int, overlap["record_count"]) * 444
        if length:
            parts.append((0, "overlap", {"byte_start": 0, "byte_end": length, "sha256": overlap["sha256"]}))
    return sorted(parts, key=lambda item: item[0])


class _Digest(Protocol):
    def update(self, data: bytes, /) -> None: ...


def update_physical_projection_digest(digest: _Digest, index: int, record: PhysicalAudioRecord) -> None:
    """Bind a physical record or decoded seam to its public packet projection."""
    encoded = json.dumps(
        {
            "index": index,
            "timestamp": record.timestamp,
            "packets": len(record.packets),
            "boundary": record.boundary_before,
        },
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    digest.update(len(encoded).to_bytes(8, "big"))
    digest.update(encoded)


class _PhysicalAudioReader:
    def __init__(
        self,
        descriptor: object,
        read_span: Callable[[PhysicalSpan], Iterable[bytes]],
        source_identity: Callable[[str], tuple[int, str]],
    ) -> None:
        self.data = validate_physical_descriptor(descriptor)
        self.read_span = read_span
        self.source_id = cast(str, self.data["source_id"])
        if source_identity(self.source_id) != (self.data["source_bytes"], self.data["source_sha256"]):
            raise ValueError("physical source custody changed")
        self.anchor = cast(dict[str, object] | None, self.data.get("anchor"))
        self.needle: bytes | None = None
        if self.anchor is None:
            self._verify_ordered_overlap(source_identity)
        else:
            repeat_id = cast(str, self.anchor["repeat_source_id"])
            repeat_size, repeat_sha = source_identity(repeat_id)
            repeat_offset = cast(int, self.anchor["repeat_offset"])
            anchor_length = cast(int, self.anchor["length"])
            if repeat_sha != self.anchor["repeat_source_sha256"] or repeat_offset + anchor_length > repeat_size:
                raise ValueError("physical anchor source custody changed")
            self.needle = _small_span(read_span, PhysicalSpan(repeat_id, repeat_offset, anchor_length))
            if hashlib.sha256(self.needle).hexdigest() != self.anchor["sha256"]:
                raise ValueError("physical anchor digest changed")
        self.verifier = _SourceVerifier(self.needle)
        self.previous_overflow: int | None = None
        self.boundary = True

    def _verify_ordered_overlap(self, source_identity: Callable[[str], tuple[int, str]]) -> None:
        ordering = cast(dict[str, object], self.data["ordering"])
        overlap = cast(dict[str, object], self.data["overlap"])
        predecessor = cast(str, ordering["predecessor_source_id"])
        size, digest = source_identity(predecessor)
        if digest != ordering["predecessor_sha256"] or size <= 0 or size % 444:
            raise ValueError("ordered physical predecessor custody changed")
        hasher = hashlib.sha256()
        span = PhysicalSpan(predecessor, 0, size)

        def previous() -> Iterator[bytes]:
            for record in iter_physical_records(PhysicalSegment((span,)), self.read_span):
                hasher.update(record)
                yield record

        current = PhysicalSpan(self.source_id, 0, cast(int, self.data["source_bytes"]))
        following = iter_physical_records(PhysicalSegment((current,)), self.read_span)
        count = unique_suffix_prefix_overlap(previous(), following)
        if hasher.hexdigest() != digest:
            raise ValueError("ordered physical predecessor bytes changed")
        if count != overlap["record_count"]:
            raise ValueError("ordered physical overlap changed")
        length = count * 444
        if length:
            self._require_equal_overlap(predecessor, size, length)
        checksum = hashlib.sha256()
        if length:
            for chunk in self.read_span(PhysicalSpan(self.source_id, 0, length)):
                checksum.update(chunk)
        if checksum.hexdigest() != overlap["sha256"]:
            raise ValueError("ordered physical overlap digest changed")

    def _require_equal_overlap(self, predecessor: str, size: int, length: int) -> None:
        previous_overlap = PhysicalSpan(predecessor, size - length, length)
        current_overlap = PhysicalSpan(self.source_id, 0, length)
        previous_records = iter_physical_records(PhysicalSegment((previous_overlap,)), self.read_span)
        current_records = iter_physical_records(PhysicalSegment((current_overlap,)), self.read_span)
        if any(left != right for left, right in zip(previous_records, current_records, strict=True)):
            raise ValueError("ordered physical overlap bytes differ")

    def _checked_chunks(self, span: PhysicalSpan, digest: _Digest) -> Iterator[bytes]:
        for chunk in self.read_span(span):
            if not isinstance(chunk, bytes):
                raise ValueError("physical source reader returned non-bytes")
            digest.update(chunk)
            self.verifier.feed(chunk)
            yield chunk

    def _records(self, span: PhysicalSpan, digest: _Digest) -> Iterator[PhysicalAudioRecord]:
        for record in iter_physical_records(PhysicalSegment((span,)), lambda _: self._checked_chunks(span, digest)):
            layout = inspect_mono_20ms_record(record)
            if self.previous_overflow is not None and self.previous_overflow != layout.first_size:
                self.boundary = True
            yield PhysicalAudioRecord(int.from_bytes(record[:4], "big"), layout.packets, self.boundary, record)
            self.previous_overflow = layout.overflow_size
            self.boundary = False

    def _seam(self, raw: bytes, item: dict[str, object]) -> PhysicalAudioRecord | None:
        prefix = inspect_partial_seam(raw, self.previous_overflow)
        packets = tuple(raw[item.offset : item.offset + item.length] for item in prefix.packets)
        if (
            prefix.consumed_prefix_len != item["decoded_prefix_bytes"]
            or len(packets) != item["decoded_packet_count"]
            or _packet_digest(packets) != item["decoded_packets_sha256"]
        ):
            raise ValueError("physical seam packet evidence changed")
        self.previous_overflow = None
        result = PhysicalAudioRecord(int.from_bytes(raw[:4], "big"), packets, self.boundary, None) if packets else None
        self.boundary = True
        return result

    def _consume_overlap(self, span: PhysicalSpan, digest: _Digest) -> None:
        chunks = partial(self._checked_chunks, span, digest)
        for _ in iter_physical_records(PhysicalSegment((span,)), lambda _: chunks()):
            pass

    def records(self) -> Iterator[PhysicalAudioRecord]:
        for start, kind, item in _parts(self.data):
            length = cast(int, item["length"]) if kind == "anchor" else cast(int, item["byte_end"]) - start
            span = PhysicalSpan(self.source_id, start, length)
            digest = hashlib.sha256()
            if kind == "span":
                yield from self._records(span, digest)
            elif kind == "overlap":
                self._consume_overlap(span, digest)
            else:
                raw = _small_span(partial(self._checked_chunks, digest=digest), span)
                if kind == "anchor" and raw != self.needle:
                    raise ValueError("physical anchor bytes differ")
                if kind == "seam":
                    seam = self._seam(raw, item)
                    if seam is not None:
                        yield seam
            if digest.hexdigest() != item["sha256"]:
                raise ValueError("physical source span digest changed")
        if (
            self.verifier.offset != self.data["source_bytes"]
            or self.verifier.digest.hexdigest() != self.data["source_sha256"]
        ):
            raise ValueError("physical source bytes differ from custody")
        if self.anchor is not None and self.verifier.matches != [self.anchor["source_offset"]]:
            raise ValueError("physical anchor is not unique")


def iter_physical_audio(
    descriptor: object,
    read_span: Callable[[PhysicalSpan], Iterable[bytes]],
    source_identity: Callable[[str], tuple[int, str]],
) -> Iterator[PhysicalAudioRecord]:
    """Yield usable packets in physical order; exhaust before committing output."""
    return _PhysicalAudioReader(descriptor, read_span, source_identity).records()


def prepare_physical_audio(
    descriptor: object,
    read_span: Callable[[PhysicalSpan], Iterable[bytes]],
    source_identity: Callable[[str], tuple[int, str]],
) -> PreparedPhysicalAudio:
    """Hash Ogg and freeze the public timeline in the same authenticated pass."""
    timeline = PublicTimelineBuilder()
    digest = hashlib.sha256(b"omi-physical-record-projection-v1\0")

    def packets() -> Iterator[bytes]:
        for index, record in enumerate(iter_physical_audio(descriptor, read_span, source_identity)):
            facts: frozenset[Literal["source_loss", "time_incomparable"]] = (
                frozenset({"time_incomparable"}) if record.boundary_before else frozenset()
            )
            time = FrozenPublicTime("unknown", record.timestamp, None, "physical-source", not record.boundary_before)
            timeline.add(FrozenRecordProjection(len(record.packets), time, facts))
            update_physical_projection_digest(digest, index, record)
            yield from record.packets

    audio = hash_v2_audio(packets())
    return PreparedPhysicalAudio(audio, timeline.finish(), digest.hexdigest())
