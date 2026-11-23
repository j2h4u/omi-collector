from __future__ import annotations

from framing_recovery_fixtures import (
    NOV_6_BYTE_DEFICIT,
    NOV_6_BYTE_SHIFT,
    NOV_6_OVERLAP_BYTES,
    framing_recovery_corpus,
)
from omi_collector.capture.domain.opus_duration import inspect_mono_20ms_record
from omi_collector.capture.domain.ring_protocol import (
    RECORD_SIZE,
    parse_data_notification,
    parse_done_notification,
    parse_read_begin_notification,
)


def test_nov_6_shift_fixture_carries_the_recorded_byte_shape() -> None:
    corpus = framing_recovery_corpus()
    read = corpus.shifted_read
    begin = parse_read_begin_notification(read.notifications[0])
    done = parse_done_notification(read.notifications[-1])
    data = b"".join(parse_data_notification(frame) for frame in read.notifications[1:-1])

    assert (begin.transfer_start_sequence, begin.packet_count) == (700, read.packet_count)
    assert done.is_ok and done.next_sequence == read.done_sequence
    assert data == read.raw_bytes
    assert data[NOV_6_BYTE_SHIFT : NOV_6_BYTE_SHIFT + NOV_6_OVERLAP_BYTES] == corpus.verified[:NOV_6_OVERLAP_BYTES]
    assert len(data) + NOV_6_BYTE_DEFICIT == read.packet_count * RECORD_SIZE


def test_exact_overlap_fixture_preserves_partial_final_record_bytes() -> None:
    corpus = framing_recovery_corpus()

    assert corpus.exact_overlap_read.raw_bytes == corpus.verified[:NOV_6_OVERLAP_BYTES]
    assert len(corpus.exact_overlap_read.raw_bytes) == 6 * RECORD_SIZE + 356
    assert tuple(map(len, corpus.exact_overlap_read.data_chunks)) == (443, 1, 1_777, 799)


def test_whole_block_loss_fixture_has_identical_observed_bytes_and_both_edges() -> None:
    corpus = framing_recovery_corpus()
    raw = corpus.ambiguous_read.raw_bytes
    edge_bytes = 3 * RECORD_SIZE

    assert raw == corpus.adjacent_history
    assert corpus.whole_block_loss_history[:edge_bytes] == raw[:edge_bytes]
    assert corpus.whole_block_loss_history[-edge_bytes:] == raw[-edge_bytes:]
    assert len(corpus.whole_block_loss_history) - len(raw) == 2 * RECORD_SIZE


def test_reference_records_are_structurally_valid_synthetic_opus() -> None:
    corpus = framing_recovery_corpus()
    references = (corpus.verified, corpus.adjacent_history, corpus.whole_block_loss_history)

    assert all(
        inspect_mono_20ms_record(raw[offset : offset + RECORD_SIZE]).packet_count == 1
        for raw in references
        for offset in range(0, len(raw), RECORD_SIZE)
    )
