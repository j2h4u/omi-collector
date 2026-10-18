from __future__ import annotations

from dataclasses import replace
from pathlib import Path

import pytest

from omi_collector.capture.adapters.v2_audio import V2AudioDigest, hash_v2_audio, write_v2_audio


def _packets():
    return (bytes((8, 0x55)), bytes((8, 0x66)))


def test_fixed_profile_hash_and_writer_agree_for_one_shot_iterators(tmp_path: Path) -> None:
    digest = hash_v2_audio(iter(_packets()))
    written = write_v2_audio(tmp_path / "audio.ogg", iter(_packets()), digest)

    assert written == digest
    assert written.packet_count == 2
    assert written.page_count > 0
    assert written.byte_count == (tmp_path / "audio.ogg").stat().st_size


def test_writer_rejects_mismatch_in_any_frozen_digest_field(tmp_path: Path) -> None:
    digest = hash_v2_audio(_packets())

    with pytest.raises(ValueError, match="frozen digest"):
        write_v2_audio(tmp_path / "audio.ogg", iter(_packets()), replace(digest, page_count=digest.page_count + 1))


def test_empty_stream_is_rejected() -> None:
    with pytest.raises(ValueError, match="empty"):
        hash_v2_audio(iter(()))


def test_digest_field_order_is_packet_page_byte_hash() -> None:
    assert tuple(V2AudioDigest.__dataclass_fields__) == ("packet_count", "page_count", "byte_count", "sha256")
