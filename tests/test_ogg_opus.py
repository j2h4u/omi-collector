from __future__ import annotations

import ast
import shutil
import subprocess
import sys
from hashlib import sha256
from pathlib import Path
from typing import cast

import pytest

from omi_collector.capture.adapters import ogg_opus


def _pages(data: bytes) -> tuple[tuple[int, int, int, int], ...]:
    pages: list[tuple[int, int, int, int]] = []
    offset = 0
    while offset < len(data):
        segment_count = data[offset + 26]
        body_size = sum(data[offset + 27 : offset + 27 + segment_count])
        page_end = offset + 27 + segment_count + body_size
        pages.append(
            (
                data[offset + 5],
                int.from_bytes(data[offset + 6 : offset + 14], "little"),
                int.from_bytes(data[offset + 14 : offset + 18], "little"),
                int.from_bytes(data[offset + 18 : offset + 22], "little"),
            )
        )
        offset = page_end
    return tuple(pages)


def _packets(data: bytes) -> tuple[bytes, ...]:
    packets: list[bytes] = []
    pending = bytearray()
    offset = 0
    while offset < len(data):
        assert data[offset : offset + 4] == b"OggS"
        segment_count = data[offset + 26]
        lacing = data[offset + 27 : offset + 27 + segment_count]
        body_start = offset + 27 + segment_count
        body_end = body_start + sum(lacing)
        body_cursor = body_start
        for size in lacing:
            pending.extend(data[body_cursor : body_cursor + size])
            body_cursor += size
            if size < 255:
                packets.append(bytes(pending))
                pending.clear()
        offset = body_end
    assert not pending
    return tuple(packets)


def _assert_page_crcs(data: bytes) -> None:
    offset = 0
    while offset < len(data):
        segment_count = data[offset + 26]
        body_start = offset + 27 + segment_count
        body_end = body_start + sum(data[offset + 27 : body_start])
        page = bytearray(data[offset:body_end])
        stored_crc = int.from_bytes(page[22:26], "little")
        page[22:26] = bytes(4)
        assert ogg_opus._ogg_crc(page) == stored_crc
        offset = body_end


def test_mux_preserves_packet_order_and_reports_file_digest(tmp_path: Path) -> None:
    packets = (b"first", b"second", b"third")
    output = tmp_path / "audio.ogg"

    result = ogg_opus.write_ogg_opus(output, packets, 0)
    data = output.read_bytes()

    assert _packets(data)[2:] == packets
    assert result.page_count == len(_pages(data)) == 3
    assert result.bytes == len(data)
    assert result.sha256 == sha256(data).hexdigest()
    assert _pages(data)[0][0] & 0x02
    assert _pages(data)[-1][0] & 0x04
    _assert_page_crcs(data)


def test_mux_output_matches_a_fixed_golden_stream_across_batches(tmp_path: Path) -> None:
    packets = tuple(bytes([65 + index]) * (index + 1) for index in range(7))
    config = ogg_opus.OggOpusConfig(packets_per_flush=3, packet_samples=321)
    output = tmp_path / "golden.ogg"

    ogg_opus.write_ogg_opus(output, packets, 0x12345678, config=config)

    assert output.read_bytes() == bytes.fromhex(
        "4f676753000200000000000000007856341200000000eee0af9401134f7075734865616401010000803e00000000004f67675300000000000000000000785634120100000021a4a83401104f7075735461677300000000000000004f6767530000c30300000000000078563412020000002079de41030102034142424343434f67675300008607000000000000785634120300000091871838030405064444444445454545454646464646464f6767530004c7080000000000007856341204000000593d7289010747474747474747"
    )


def test_hash_and_write_match_with_a_one_shot_iterator(tmp_path: Path) -> None:
    class OneShotPackets:
        def __init__(self, values: tuple[bytes, ...]) -> None:
            self._values = values
            self._index = 0
            self.iter_calls = 0

        def __iter__(self) -> OneShotPackets:
            self.iter_calls += 1
            if self.iter_calls > 1:
                raise AssertionError("packet iterator was requested more than once")
            return self

        def __next__(self) -> bytes:
            if self._index == len(self._values):
                raise StopIteration
            value = self._values[self._index]
            self._index += 1
            return value

    packets = (b"alpha", b"beta", b"gamma")
    hashed_input = OneShotPackets(packets)
    written_input = OneShotPackets(packets)

    hashed = ogg_opus.hash_ogg_opus(hashed_input, 17)
    written = ogg_opus.write_ogg_opus(tmp_path / "audio.ogg", written_input, 17)

    assert hashed == written
    assert hashed_input.iter_calls == written_input.iter_calls == 1


def test_large_packets_round_trip_across_continued_pages(tmp_path: Path) -> None:
    packets = (b"a" * 20_000, b"bc" * 5_000, b"last")
    output = tmp_path / "long.ogg"

    written = ogg_opus.write_ogg_opus(output, packets, 17)
    hashed = ogg_opus.hash_ogg_opus(iter(packets), 17)
    data = output.read_bytes()

    assert _packets(data)[2:] == packets
    assert any(page[0] & 0x01 for page in _pages(data))
    assert hashed == written
    _assert_page_crcs(data)


def test_empty_stream_is_rejected_by_hash_and_writer(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="packets must not be empty"):
        ogg_opus.hash_ogg_opus((), 0)
    with pytest.raises(ValueError, match="packets must not be empty"):
        ogg_opus.write_ogg_opus(tmp_path / "empty.ogg", (), 0)


def test_invalid_packet_types_and_serials_keep_clear_errors() -> None:
    invalid_packet = cast(bytes, object())
    with pytest.raises(TypeError, match="packet 0 must be bytes"):
        ogg_opus.hash_ogg_opus((invalid_packet,), 0)
    with pytest.raises(ValueError, match="unsigned 32-bit"):
        ogg_opus.hash_ogg_opus((b"packet",), -1)


def test_muxer_imports_only_standard_library_modules() -> None:
    module_path = Path(__file__).parents[1] / "src/omi_collector/capture/adapters/ogg_opus.py"
    tree = ast.parse(module_path.read_text(encoding="utf-8"))
    imported: set[str] = set()
    relative_import = False
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported.update(alias.name.partition(".")[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            relative_import |= node.level > 0
            if node.module:
                imported.add(node.module.partition(".")[0])

    assert not relative_import
    assert imported <= sys.stdlib_module_names | {"__future__"}


def test_real_opus_packets_survive_rewrap_and_page_checksums(tmp_path: Path) -> None:
    ffmpeg = shutil.which("ffmpeg")
    if ffmpeg is None:
        pytest.skip("ffmpeg is not installed")
    encoders = subprocess.run([ffmpeg, "-hide_banner", "-encoders"], check=True, capture_output=True, text=True)
    if "libopus" not in encoders.stdout + encoders.stderr:
        pytest.skip("ffmpeg has no libopus encoder")
    source = tmp_path / "source.ogg"
    output = tmp_path / "rewrapped.ogg"
    subprocess.run(
        [
            ffmpeg,
            "-v",
            "error",
            "-f",
            "lavfi",
            "-i",
            "sine=frequency=1000:sample_rate=16000:duration=0.02",
            "-c:a",
            "libopus",
            "-frame_duration",
            "20",
            "-application",
            "audio",
            "-y",
            str(source),
        ],
        check=True,
        capture_output=True,
    )
    packets = _packets(source.read_bytes())[2:]
    assert packets

    result = ogg_opus.write_ogg_opus(
        output,
        packets,
        0,
        config=ogg_opus.OggOpusConfig(packets_per_flush=50, packet_samples=960),
    )
    data = output.read_bytes()
    pages = _pages(data)

    assert _packets(data)[2:] == packets
    assert all(page[2] == 0 for page in pages)
    assert [page[3] for page in pages] == list(range(len(pages)))
    assert pages[-1][1] == len(packets) * 960
    assert pages[-1][0] & 0x04
    assert result.sha256 == sha256(data).hexdigest()
    _assert_page_crcs(data)
    subprocess.run([ffmpeg, "-v", "error", "-i", str(output), "-f", "null", "-"], check=True, capture_output=True)
