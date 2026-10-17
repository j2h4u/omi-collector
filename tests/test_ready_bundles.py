"""SQL ready publication keeps the public bundle and ACK contracts stable."""

from __future__ import annotations

from collections.abc import Callable, Iterator
from hashlib import sha256
from json import dumps, loads
from pathlib import Path
from struct import pack
from typing import cast

import pytest

from omi_collector.capture.adapters import ready_bundles
from omi_collector.capture.adapters.clock_segments import ClockSegment, ClockSegmentMap
from omi_collector.capture.adapters.collector_state_db import StateDB, StateKind
from omi_collector.capture.adapters.internal_audio_store import (
    AUDIO_RECORD_SIZE,
    CaptureRecord,
    DrainCheckpoint,
    InternalAudioStore,
)
from omi_collector.capture.adapters.ready_bundles import (
    ConflictingOverlapError,
    ReadyBundleError,
    ReadyOutcome,
    ReadyOutcomeState,
    SqlitePublicationSource,
    finalize_typed_sources,
)
from omi_collector.config import ReadyConfig


def _record(timestamp: int, marker: int) -> bytes:
    return pack(">I", timestamp) + bytes((2, 8, marker)) + bytes(AUDIO_RECORD_SIZE - 7)


def _overflow_record(timestamp: int, size: int, marker: int) -> bytes:
    record = bytearray(_record(timestamp, marker))
    record[4 + 438] = size
    return bytes(record)


def _sized_record(timestamp: int, size: int, marker: int) -> bytes:
    record = bytearray(AUDIO_RECORD_SIZE)
    record[:4] = timestamp.to_bytes(4, "big")
    record[4] = size
    record[5] = 8
    record[6 : 5 + size] = bytes((marker,)) * (size - 1)
    return bytes(record)


def _state_db(root: Path) -> StateDB:
    root.mkdir(parents=True, exist_ok=True)
    return StateDB(root / "collector-state.sqlite3")


@pytest.fixture
def audio_store(tmp_path: Path) -> Iterator[tuple[StateDB, InternalAudioStore]]:
    state_db = _state_db(tmp_path)
    store = InternalAudioStore(state_db)
    try:
        yield state_db, store
    finally:
        store.close()


def _ready_root(root: Path) -> Path:
    ready = root / "ready"
    ready.mkdir(parents=True, exist_ok=True, mode=0o2750)
    ready.chmod(0o2750)
    return ready


def _source(
    store: InternalAudioStore,
    source_id: str,
    start: int,
    records: tuple[bytes, ...],
) -> SqlitePublicationSource:
    store.create_source(source_id, "attempt", start, start + len(records))
    source = store.prepare_leg(source_id, start, len(records))
    source = store.record_read_begin(source_id, start, len(records))
    store.commit_drain(source_id, 0, records, DrainCheckpoint(source.current_leg_generation))
    source = store.get_source(source_id)
    assert source is not None
    store.seal_source(
        source_id,
        leg_generation=source.current_leg_generation,
        expected_count=source.committed_count,
        expected_sha256=source.committed_sha256,
    )
    return SqlitePublicationSource(store, source_id)


def _publish(
    store: InternalAudioStore,
    sources: tuple[SqlitePublicationSource, ...],
    ready: Path,
) -> ReadyOutcome:
    frontier = max(source.source.sequence_start + source.source.committed_count for source in sources)
    return finalize_typed_sources(
        store,
        sources,
        ready,
        config=ReadyConfig(target_audio_seconds=0.02),
        clock_segments=ClockSegmentMap(()),
        frontier=frontier,
        drained=True,
    )


def _entry(state_db: StateDB, bundle_id: str) -> dict[str, object]:
    value = state_db.get(StateKind.READY_PUBLICATION, bundle_id)
    assert value is not None
    return value


def _checkpoint(
    root: Path,
    identities: list[tuple[str, str]],
    *,
    decisions: list[tuple[str, str]] | None = None,
    tail: list[tuple[str, str]] | None = None,
) -> Path:
    path = root / "work" / "omi-ready-checkpoint.json"
    path.parent.mkdir(parents=True, exist_ok=True)

    def decision(bundle_id: str, records_sha256: str, *, pending: bool = False) -> dict[str, object]:
        return {
            "bundle_id": bundle_id,
            "records_sha256": records_sha256,
            "packet_ranges": [{"packet_start": 0, "packet_next": 1}] if pending else [],
            "no_speech_packet_ranges": [],
            "packet_count": 1 if pending else 0,
            "input_id": "c" * 64 if pending else None,
            "input_sha256": "d" * 64 if pending else None,
            "receipt_sha256": "e" * 64 if pending else None,
        }

    entries = [decision(*item) for item in (identities if decisions is None else decisions)]
    path.write_text(
        dumps(
            {
                "analysis_cursor": entries[-1] if entries else None,
                "vad_decisions": entries,
                "open_speech_tail": (
                    {"entries": [decision(*item, pending=True) for item in tail], "opened_at": 1, "outputs": []}
                    if tail
                    else None
                ),
                "acknowledged": [
                    {"bundle_id": bundle_id, "records_sha256": records_sha256}
                    for bundle_id, records_sha256 in identities
                ],
            }
        ),
        encoding="utf-8",
    )
    return path


def test_threshold_requires_both_enough_audio_and_a_completed_drain(
    tmp_path: Path, audio_store: tuple[StateDB, InternalAudioStore]
) -> None:
    _state_db, store = audio_store
    ready = _ready_root(tmp_path)
    source = _source(store, "a" * 32, 10, (_record(100, 1),))

    waiting_for_drain = finalize_typed_sources(
        store,
        (source,),
        ready,
        config=ReadyConfig(target_audio_seconds=0.02),
        clock_segments=ClockSegmentMap(()),
        frontier=11,
        drained=False,
    )
    waiting_for_audio = finalize_typed_sources(
        store,
        (source,),
        ready,
        config=ReadyConfig(target_audio_seconds=0.04),
        clock_segments=ClockSegmentMap(()),
        frontier=11,
        drained=True,
    )
    published = _publish(store, (source,), ready)

    assert waiting_for_drain.reason == "waiting_for_drain"
    assert waiting_for_audio.reason == "waiting_for_threshold"
    assert published.state is ReadyOutcomeState.PUBLISHED
    assert published.published[0].record_count == 1


def test_exact_contained_and_partial_overlaps_publish_each_record_once(
    tmp_path: Path, audio_store: tuple[StateDB, InternalAudioStore]
) -> None:
    _state_db, store = audio_store
    ready = _ready_root(tmp_path)
    first_records = tuple(_record(sequence, sequence - 99) for sequence in range(100, 104))
    sources = (
        _source(store, "a" * 32, 100, first_records),
        _source(store, "b" * 32, 101, first_records[1:3]),
        _source(store, "c" * 32, 102, (first_records[2], first_records[3], _record(104, 5))),
    )

    outcome = _publish(store, sources, ready)

    assert outcome.state is ReadyOutcomeState.PUBLISHED
    assert len(outcome.published) == 1
    published = outcome.published[0]
    assert published.record_count == 5
    assert (published.path / "records.bin").read_bytes() == b"".join((*first_records, _record(104, 5)))


def test_conflicting_overlap_fails_closed_without_ready_output(
    tmp_path: Path, audio_store: tuple[StateDB, InternalAudioStore]
) -> None:
    state_db, store = audio_store
    ready = _ready_root(tmp_path)
    left = _source(store, "a" * 32, 100, (_record(100, 1), _record(101, 2)))
    right = _source(store, "b" * 32, 101, (_record(101, 99), _record(102, 3)))

    with pytest.raises(ConflictingOverlapError, match="conflicting record bytes"):
        _publish(store, (left, right), ready)

    assert not tuple(ready.iterdir())
    assert state_db.list(StateKind.READY_PUBLICATION) == ()


def test_sparse_sequences_keep_physical_ranges_and_order(
    tmp_path: Path, audio_store: tuple[StateDB, InternalAudioStore]
) -> None:
    _state_db, store = audio_store
    ready = _ready_root(tmp_path)
    sources = (
        _source(store, "a" * 32, 10, (_record(100, 1),)),
        _source(store, "b" * 32, 12, (_record(102, 2), _record(103, 3))),
    )

    outcome = _publish(store, sources, ready)

    assert outcome.state is ReadyOutcomeState.PUBLISHED
    assert [(item.path.name, item.record_count, item.next_sequence) for item in outcome.published] == [
        (outcome.published[0].bundle_id, 1, 11),
        (outcome.published[1].bundle_id, 2, 14),
    ]
    manifests = [
        cast(dict[str, object], loads((item.path / "manifest.json").read_text())) for item in outcome.published
    ]
    assert [(item["start_sequence"], item["next_sequence"]) for item in manifests] == [(10, 11), (12, 14)]
    assert b"".join((item.path / "records.bin").read_bytes() for item in outcome.published) == (
        _record(100, 1) + _record(102, 2) + _record(103, 3)
    )


def test_opus_continuity_breaks_split_public_bundles_byte_exactly(
    tmp_path: Path, audio_store: tuple[StateDB, InternalAudioStore]
) -> None:
    _state_db, store = audio_store
    ready = _ready_root(tmp_path)
    records = (
        _record(100, 1),
        _overflow_record(101, 75, 2),
        _sized_record(3_543, 88, 3),
        _overflow_record(3_544, 97, 4),
        _sized_record(3_554, 130, 5),
    )
    source = _source(store, "a" * 32, 10, records)

    outcome = _publish(store, (source,), ready)

    assert [(item.next_sequence - item.record_count, item.next_sequence) for item in outcome.published] == [
        (10, 12),
        (12, 14),
        (14, 15),
    ]
    assert b"".join((item.path / "records.bin").read_bytes() for item in outcome.published) == b"".join(records)


def test_confirmed_clock_ranges_change_only_published_timestamps(
    tmp_path: Path, audio_store: tuple[StateDB, InternalAudioStore]
) -> None:
    _state_db, store = audio_store
    ready = _ready_root(tmp_path)
    records = (_record(100, 1), _record(200, 2), _record(300, 3))
    source = _source(store, "a" * 32, 10, records)
    segments = ClockSegmentMap((ClockSegment("observation", 11, 12, 0.6, 0.1),))

    (published,) = finalize_typed_sources(
        store,
        (source,),
        ready,
        config=ReadyConfig(target_audio_seconds=0.02),
        clock_segments=segments,
        frontier=13,
        drained=True,
    ).published

    output = (published.path / "records.bin").read_bytes()
    assert output[:4] == records[0][:4]
    assert output[AUDIO_RECORD_SIZE : AUDIO_RECORD_SIZE + 4] == (201).to_bytes(4, "big")
    assert output[2 * AUDIO_RECORD_SIZE : 2 * AUDIO_RECORD_SIZE + 4] == records[2][:4]
    manifest = cast(dict[str, object], loads((published.path / "manifest.json").read_text()))
    ranges = cast(list[dict[str, object]], manifest["time_ranges"])
    assert [item["utc"] is not None for item in ranges] == [False, True, False]


def test_publication_ledger_records_whole_bundle_utc_date_envelope(
    tmp_path: Path, audio_store: tuple[StateDB, InternalAudioStore]
) -> None:
    state_db, store = audio_store
    ready = _ready_root(tmp_path)
    records = (_record(172800, 1), _record(1, 2))
    source = _source(store, "a" * 32, 100, records)
    segments = ClockSegmentMap((ClockSegment("mapped", 100, 102, 0.0, 2.0),))

    (published,) = finalize_typed_sources(
        store,
        (source,),
        ready,
        config=ReadyConfig(target_audio_seconds=0.02),
        clock_segments=segments,
        frontier=102,
        drained=True,
    ).published

    entry = _entry(state_db, published.bundle_id)
    assert (entry["recorded_on_utc_min"], entry["recorded_on_utc_max"]) == ("1969-12-31", "1970-01-03")
    assert ready_bundles._valid_publication_dates(entry)


def test_fractional_clock_offset_is_applied_once_before_date_uncertainty(
    tmp_path: Path, audio_store: tuple[StateDB, InternalAudioStore]
) -> None:
    state_db, store = audio_store
    ready = _ready_root(tmp_path)
    source = _source(store, "a" * 32, 100, (_record(86399, 1),))
    segments = ClockSegmentMap((ClockSegment("fractional", 100, 101, 0.6, 0.0),))

    (published,) = finalize_typed_sources(
        store,
        (source,),
        ready,
        config=ReadyConfig(target_audio_seconds=0.02),
        clock_segments=segments,
        frontier=101,
        drained=True,
    ).published

    entry = _entry(state_db, published.bundle_id)
    assert (entry["recorded_on_utc_min"], entry["recorded_on_utc_max"]) == ("1970-01-02", "1970-01-02")


@pytest.mark.parametrize("uncertainty", [1e300, 0.0])
def test_publication_date_envelope_is_unknown_for_unrepresentable_or_unmapped_records(
    tmp_path: Path, audio_store: tuple[StateDB, InternalAudioStore], uncertainty: float
) -> None:
    state_db, store = audio_store
    ready = _ready_root(tmp_path)
    source = _source(store, "a" * 32, 100, (_record(100, 1), _record(101, 2)))
    segments = (
        ClockSegmentMap((ClockSegment("wide", 100, 102 if uncertainty else 101, 0.0, uncertainty),))
        if uncertainty
        else ClockSegmentMap((ClockSegment("partial", 100, 101, 0.0, uncertainty),))
    )

    (published,) = finalize_typed_sources(
        store,
        (source,),
        ready,
        config=ReadyConfig(target_audio_seconds=0.02),
        clock_segments=segments,
        frontier=102,
        drained=True,
    ).published

    entry = _entry(state_db, published.bundle_id)
    assert entry["recorded_on_utc_min"] is None
    assert entry["recorded_on_utc_max"] is None
    assert published.path.is_dir()


def test_matching_ledger_entry_keeps_its_original_formation_date(
    tmp_path: Path, audio_store: tuple[StateDB, InternalAudioStore]
) -> None:
    state_db, store = audio_store
    ready = _ready_root(tmp_path)
    source = _source(store, "a" * 32, 100, (_record(100, 1),))
    (published,) = _publish(store, (source,), ready).published
    entry = _entry(state_db, published.bundle_id)
    entry["formed_on_utc"] = "2001-02-03"
    state_db.put(StateKind.READY_PUBLICATION, published.bundle_id, entry)

    _publish(store, (source,), ready)

    assert _entry(state_db, published.bundle_id)["formed_on_utc"] == "2001-02-03"


def test_legacy_date_less_entry_stays_date_less_through_reuse_and_ack(
    tmp_path: Path, audio_store: tuple[StateDB, InternalAudioStore]
) -> None:
    state_db, store = audio_store
    ready = _ready_root(tmp_path)
    source = _source(store, "a" * 32, 100, (_record(100, 1),))
    (published,) = _publish(store, (source,), ready).published
    entry = _entry(state_db, published.bundle_id)
    for field in ("formed_on_utc", "recorded_on_utc_min", "recorded_on_utc_max"):
        entry.pop(field)
    state_db.put(StateKind.READY_PUBLICATION, published.bundle_id, entry)

    _publish(store, (source,), ready)
    assert (
        not {"formed_on_utc", "recorded_on_utc_min", "recorded_on_utc_max"}
        & _entry(state_db, published.bundle_id).keys()
    )
    ready_bundles.retire_acknowledged(
        ready,
        state_db,
        _checkpoint(tmp_path, [(published.bundle_id, published.records_sha256)]),
    )

    retired = _entry(state_db, published.bundle_id)
    assert retired["state"] == "retired"
    assert not {"formed_on_utc", "recorded_on_utc_min", "recorded_on_utc_max"} & retired.keys()


def test_output_digest_collects_date_envelope_in_its_existing_record_pass(
    tmp_path: Path,
    audio_store: tuple[StateDB, InternalAudioStore],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _state_db, store = audio_store
    ready = _ready_root(tmp_path)
    records = (_record(100, 1), _record(101, 2))
    source = _source(store, "a" * 32, 100, records)
    original_digest = cast(Callable[..., tuple[str, str | None, str | None]], ready_bundles._typed_output_digest)
    original_read = cast(Callable[..., tuple[CaptureRecord, ...]], store.read_record_page)
    digest_running = False
    digest_calls = 0
    digest_records = 0

    def tracked_digest(*args: object, **kwargs: object) -> tuple[str, str | None, str | None]:
        nonlocal digest_calls, digest_running
        digest_calls += 1
        digest_running = True
        try:
            return original_digest(*args, **kwargs)
        finally:
            digest_running = False

    def tracked_read(source_id: str, after_ordinal: int = -1, limit: int = 128) -> tuple[CaptureRecord, ...]:
        nonlocal digest_records
        page = original_read(source_id, after_ordinal, limit)
        if digest_running:
            digest_records += len(page)
        return page

    monkeypatch.setattr(ready_bundles, "_typed_output_digest", tracked_digest)
    monkeypatch.setattr(store, "read_record_page", tracked_read)

    finalize_typed_sources(
        store,
        (source,),
        ready,
        config=ReadyConfig(target_audio_seconds=0.02),
        clock_segments=ClockSegmentMap((ClockSegment("mapped", 100, 102, 0.0, 1.0),)),
        frontier=102,
        drained=True,
    )

    assert digest_calls > 0
    assert digest_records == len(records) * digest_calls


@pytest.mark.parametrize(
    "changes",
    [
        {"formed_on_utc": "2026-2-03"},
        {"formed_on_utc": "2026-02-03", "recorded_on_utc_min": None},
        {"formed_on_utc": "2026-02-03", "recorded_on_utc_min": "2026-02-04", "recorded_on_utc_max": "2026-02-03"},
    ],
)
def test_ledger_rejects_malformed_or_partial_publication_dates(changes: dict[str, object]) -> None:
    assert not ready_bundles._valid_publication_dates(changes)


def test_clock_overflow_preserves_every_timestamp_in_the_affected_range(
    tmp_path: Path, audio_store: tuple[StateDB, InternalAudioStore]
) -> None:
    _state_db, store = audio_store
    ready = _ready_root(tmp_path)
    maximum = (1 << 32) - 1
    records = (_record(0, 1), _record(maximum, 2), _record(maximum - 1, 3))
    source = _source(store, "a" * 32, 10, records)
    segments = ClockSegmentMap((ClockSegment("overflow", 10, 12, 1.0, 0.0), ClockSegment("boundary", 12, 13, 1.0, 0.0)))

    (published,) = finalize_typed_sources(
        store,
        (source,),
        ready,
        config=ReadyConfig(target_audio_seconds=0.02),
        clock_segments=segments,
        frontier=13,
        drained=True,
    ).published

    output = (published.path / "records.bin").read_bytes()
    assert output == records[0] + records[1] + _record(maximum, 3)
    manifest = cast(dict[str, object], loads((published.path / "manifest.json").read_text()))
    ranges = cast(list[dict[str, object]], manifest["time_ranges"])
    assert [(item["start_sequence"], item["next_sequence"], item["utc"] is not None) for item in ranges] == [
        (10, 12, False),
        (12, 13, True),
    ]


def test_sql_bundle_lineage_survives_ack_retirement(
    tmp_path: Path, audio_store: tuple[StateDB, InternalAudioStore]
) -> None:
    state_db, store = audio_store
    ready = _ready_root(tmp_path)
    first, second = _record(100, 1), _record(101, 2)
    sources = (_source(store, "a" * 32, 100, (first,)), _source(store, "b" * 32, 101, (second,)))
    (published,) = _publish(store, sources, ready).published
    expected_lineage = ((100, 101, sha256(first).hexdigest()), (101, 102, sha256(second).hexdigest()))
    published_entry = _entry(state_db, published.bundle_id)
    publication_dates = tuple(
        published_entry[field] for field in ("formed_on_utc", "recorded_on_utc_min", "recorded_on_utc_max")
    )
    checkpoint = _checkpoint(tmp_path, [(published.bundle_id, published.records_sha256)])

    ready_bundles.retire_acknowledged(ready, state_db, checkpoint)

    assert not published.path.exists()
    entry = _entry(state_db, published.bundle_id)
    assert entry["state"] == "retired"
    assert ready_bundles.source_lineage(entry) == expected_lineage
    assert (
        tuple(entry[field] for field in ("formed_on_utc", "recorded_on_utc_min", "recorded_on_utc_max"))
        == publication_dates
    )


@pytest.mark.parametrize(
    "lineage",
    [
        [],
        [{"start_sequence": 100, "next_sequence": 102, "raw_sha256": "z" * 64}],
        [{"start_sequence": 101, "next_sequence": 102, "raw_sha256": "a" * 64}],
    ],
)
def test_sql_publication_lineage_rejects_missing_or_mismatched_source_slices(
    tmp_path: Path, audio_store: tuple[StateDB, InternalAudioStore], lineage: list[dict[str, object]]
) -> None:
    state_db, store = audio_store
    ready = _ready_root(tmp_path)
    source = _source(store, "a" * 32, 100, (_record(100, 1),))
    (published,) = _publish(store, (source,), ready).published
    entry = _entry(state_db, published.bundle_id)
    entry["sources"] = lineage
    state_db.put(StateKind.READY_PUBLICATION, published.bundle_id, entry)

    with pytest.raises(ReadyBundleError, match="publication ledger"):
        ready_bundles._read_ledger(state_db)


def test_retired_replay_requires_exact_full_source_lineage(
    tmp_path: Path, audio_store: tuple[StateDB, InternalAudioStore]
) -> None:
    state_db, store = audio_store
    ready = _ready_root(tmp_path)
    records = (_record(100, 1), _record(101, 2))
    (published,) = _publish(store, (_source(store, "a" * 32, 100, records),), ready).published
    checkpoint = _checkpoint(tmp_path, [(published.bundle_id, published.records_sha256)])
    ready_bundles.retire_acknowledged(ready, state_db, checkpoint)

    exact = _source(store, "b" * 32, 100, records)
    assert _publish(store, (exact,), ready).published == ()
    partial = _source(store, "c" * 32, 101, (records[1],))
    with pytest.raises(ReadyBundleError, match=r"retired custody|retired range"):
        _publish(store, (partial,), ready)


def test_multiple_ready_prefixes_replay_only_the_unique_suffix(
    tmp_path: Path, audio_store: tuple[StateDB, InternalAudioStore]
) -> None:
    _state_db, store = audio_store
    ready = _ready_root(tmp_path)
    first_record, second_record, suffix_record = _record(100, 1), _record(101, 2), _record(102, 3)
    first = _source(store, "a" * 32, 100, (first_record,))
    second = _source(store, "b" * 32, 101, (second_record,))
    prefix_one = _publish(store, (first,), ready).published[0]
    prefix_two = _publish(store, (second,), ready).published[0]
    extension = _source(store, "c" * 32, 100, (first_record, second_record, suffix_record))

    suffix = _publish(store, (extension,), ready)

    assert len(suffix.published) == 1
    assert suffix.published[0].next_sequence == 103
    assert suffix.published[0].record_count == 1
    assert (suffix.published[0].path / "records.bin").read_bytes() == suffix_record
    assert prefix_one.path.exists() and prefix_two.path.exists()


def test_ready_output_tampering_blocks_replay_without_changing_sql_ledger(
    tmp_path: Path, audio_store: tuple[StateDB, InternalAudioStore]
) -> None:
    state_db, store = audio_store
    ready = _ready_root(tmp_path)
    records = (_record(100, 1),)
    first = _source(store, "a" * 32, 100, records)
    (published,) = _publish(store, (first,), ready).published
    before = state_db.list(StateKind.READY_PUBLICATION)
    raw_path = published.path / "records.bin"
    raw_path.write_bytes(_record(101, 1))
    replay = _source(store, "b" * 32, 100, records)

    with pytest.raises(ReadyBundleError):
        _publish(store, (replay,), ready)

    assert raw_path.exists()
    assert state_db.list(StateKind.READY_PUBLICATION) == before


def test_ack_preflights_every_identity_before_retiring_any_bundle(
    tmp_path: Path, audio_store: tuple[StateDB, InternalAudioStore]
) -> None:
    state_db, store = audio_store
    ready = _ready_root(tmp_path)
    sources = (
        _source(store, "a" * 32, 100, (_record(100, 1),)),
        _source(store, "b" * 32, 101, (_record(101, 2),)),
    )
    published = _publish(store, sources, ready).published
    checkpoint = _checkpoint(
        tmp_path,
        [(item.bundle_id, item.records_sha256) for item in published] + [("f" * 64, "e" * 64)],
    )
    before = state_db.list(StateKind.READY_PUBLICATION)

    with pytest.raises(ReadyBundleError, match="published ready bundle"):
        ready_bundles.retire_acknowledged(ready, state_db, checkpoint)

    assert all(item.path.exists() for item in published)
    assert state_db.list(StateKind.READY_PUBLICATION) == before


def test_ready_bundle_remains_until_acknowledged(
    tmp_path: Path, audio_store: tuple[StateDB, InternalAudioStore]
) -> None:
    state_db, store = audio_store
    ready = _ready_root(tmp_path)
    source = _source(store, "a" * 32, 100, (_record(100, 1),))
    (published,) = _publish(store, (source,), ready).published
    checkpoint = _checkpoint(tmp_path, [])

    assert ready_bundles.retire_acknowledged(ready, state_db, checkpoint) == ()
    assert published.path.exists()
    assert _entry(state_db, published.bundle_id)["state"] == "ready"


@pytest.mark.parametrize("pending_matches", [False, True])
def test_open_speech_tail_blocks_only_matching_ack_identity(
    tmp_path: Path, audio_store: tuple[StateDB, InternalAudioStore], pending_matches: bool
) -> None:
    state_db, store = audio_store
    ready = _ready_root(tmp_path)
    source = _source(store, "a" * 32, 100, (_record(100, 1),))
    (published,) = _publish(store, (source,), ready).published
    identity = (published.bundle_id, published.records_sha256)
    other = ("f" * 64, "e" * 64)
    checkpoint = _checkpoint(
        tmp_path,
        [identity],
        tail=[identity if pending_matches else other],
    )

    if pending_matches:
        with pytest.raises(ReadyBundleError, match="pending tail"):
            ready_bundles.retire_acknowledged(ready, state_db, checkpoint)
        assert published.path.exists()
    else:
        assert ready_bundles.retire_acknowledged(ready, state_db, checkpoint)[0].bundle_id == published.bundle_id
        assert not published.path.exists()


@pytest.mark.parametrize("damage", ["wrong_hash", "pending_tail", "invalid_manifest"])
def test_public_ack_validation_fails_closed_without_mutation(
    tmp_path: Path, audio_store: tuple[StateDB, InternalAudioStore], damage: str
) -> None:
    state_db, store = audio_store
    ready = _ready_root(tmp_path)
    source = _source(store, "a" * 32, 100, (_record(100, 1),))
    (published,) = _publish(store, (source,), ready).published
    identity = (published.bundle_id, published.records_sha256)
    checkpoint = _checkpoint(
        tmp_path,
        [(published.bundle_id, "b" * 64) if damage == "wrong_hash" else identity],
        tail=[identity] if damage == "pending_tail" else None,
    )
    before = state_db.list(StateKind.READY_PUBLICATION)
    if damage == "invalid_manifest":
        manifest_path = published.path / "manifest.json"
        manifest = cast(dict[str, object], loads(manifest_path.read_text()))
        manifest["time_ranges"] = [
            {"start_sequence": 100, "next_sequence": 101, "utc": {"offset_seconds": float("nan")}}
        ]
        manifest_path.write_text(dumps(manifest), encoding="utf-8")

    with pytest.raises(ReadyBundleError):
        ready_bundles.retire_acknowledged(ready, state_db, checkpoint)

    assert published.path.exists()
    assert state_db.list(StateKind.READY_PUBLICATION) == before


def test_symlinked_ready_root_is_rejected_without_writing_to_target(
    tmp_path: Path, audio_store: tuple[StateDB, InternalAudioStore]
) -> None:
    _state_db, store = audio_store
    target = _ready_root(tmp_path)
    link = tmp_path / "linked-ready"
    link.symlink_to(target, target_is_directory=True)
    source = _source(store, "a" * 32, 100, (_record(100, 1),))

    with pytest.raises(ReadyBundleError):
        _publish(store, (source,), link)

    assert not tuple(target.iterdir())
