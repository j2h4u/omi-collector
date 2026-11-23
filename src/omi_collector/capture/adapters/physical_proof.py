"""Build bounded READY replay proof for an ordered physical-byte artifact."""

from __future__ import annotations

from collections.abc import Callable, Iterable, Iterator
from contextlib import contextmanager
from dataclasses import replace
from hashlib import sha256
from typing import cast

from ..domain.physical_publication import validate_physical_descriptor
from ..domain.physical_recovery import PhysicalSpan
from ..domain.replay_proof import NormalizationMode, NormalizationRun, pack_normalization_runs
from .physical_audio import iter_physical_audio, update_physical_projection_digest
from .ready_bundles import _canonical, _proof_page_scratch, _V2StageProof


@contextmanager
def prepare_physical_proof(
    descriptor: object,
    read_span: Callable[[PhysicalSpan], Iterable[bytes]],
    source_identity: Callable[[str], tuple[int, str]],
    *,
    packet_count: int,
) -> Iterator[_V2StageProof]:
    """Exhaust and verify physical evidence before exposing scratch proof pages."""
    data = validate_physical_descriptor(descriptor)
    record_count = cast(int, data["record_count"])
    if type(packet_count) is not int or packet_count <= 0:
        raise ValueError("physical proof packet count is invalid")
    projection = sha256(b"omi-physical-record-projection-v1\0")
    records_digest = sha256()
    runs: list[NormalizationRun] = []
    packets = 0

    def normalized_records() -> Iterator[bytes]:
        nonlocal packets
        for index, record in enumerate(iter_physical_audio(data, read_span, source_identity)):
            update_physical_projection_digest(projection, index, record)
            packets += len(record.packets)
            raw = record.raw_record
            if raw is None:
                continue
            position = runs[-1].next_record_index if runs else 0
            boundary = 2 if record.boundary_before else 0
            if runs and not boundary:
                runs[-1] = replace(runs[-1], next_record_index=position + 1)
            else:
                runs.append(NormalizationRun(position, position + 1, 0.0, NormalizationMode.RAW, boundary))
            records_digest.update(raw)
            yield raw

    encoded = _canonical(data)
    with _proof_page_scratch(normalized_records(), record_count=record_count) as (page_index, scratch):
        if projection.hexdigest() != data["record_projection_sha256"] or packets != packet_count:
            raise ValueError("physical proof projection changed")
        packed_runs = pack_normalization_runs(runs, record_count=record_count)
        yield _V2StageProof(
            encoded, sha256(encoded).hexdigest(), packed_runs, records_digest.hexdigest(), page_index, scratch
        )
