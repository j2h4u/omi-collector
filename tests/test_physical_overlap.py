from __future__ import annotations

import pytest

from omi_collector.capture.domain.physical_overlap import unique_suffix_prefix_overlap


def _record(value: int) -> bytes:
    return value.to_bytes(4, "big") + bytes((value,)) * 440


def test_independent_reads_without_matching_prefix_preserve_all_audio() -> None:
    assert unique_suffix_prefix_overlap(map(_record, (1, 2, 3)), map(_record, (4, 5))) == 0


def test_unique_exact_suffix_prefix_is_the_only_deduplicated_audio() -> None:
    assert unique_suffix_prefix_overlap(map(_record, (1, 2, 3, 4)), map(_record, (3, 4, 5))) == 2
    assert unique_suffix_prefix_overlap(map(_record, (1, 2, 3, 4)), map(_record, (3, 7))) == 0


def test_repeated_anchor_requires_review_instead_of_guessing() -> None:
    with pytest.raises(ValueError, match="multiple valid"):
        unique_suffix_prefix_overlap(map(_record, (1, 1)), map(_record, (1, 1, 3)))


def test_overlap_never_accepts_short_or_non_record_data() -> None:
    with pytest.raises(ValueError, match="invalid"):
        unique_suffix_prefix_overlap((bytes(444), b"short"), (bytes(444),))


def test_repeated_first_record_with_one_complete_suffix_is_unambiguous() -> None:
    x, y, z = (_record(n) for n in range(1, 4))
    assert unique_suffix_prefix_overlap([x, y, x, z], [x, z]) == 2


def test_whole_replay_inside_earlier_source_is_not_published_again() -> None:
    a, b, c, d = (_record(n) for n in range(1, 5))
    with pytest.raises(ValueError, match="complete replay"):
        unique_suffix_prefix_overlap([a, b, c, d], [b, c])
