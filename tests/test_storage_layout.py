from __future__ import annotations

import os
from pathlib import Path

import pytest

from omi_collector.storage_layout import StorageLayoutError, load_operator_config


def _config(path: Path, address: str = "AA:BB:CC:DD:EE:FF") -> Path:
    path.write_text(
        f'pendant_address = "{address}"\nbundle_min_audio_seconds = 3600.0\n',
        encoding="utf-8",
    )
    return path


def test_config_parent_is_storage_root_and_loading_creates_nothing(tmp_path: Path) -> None:
    loaded = load_operator_config(_config(tmp_path / "config.toml"))

    assert loaded.pendant.address == "AA:BB:CC:DD:EE:FF"
    assert loaded.storage.root == tmp_path
    assert loaded.storage.collector.root == tmp_path / "collector"
    assert loaded.storage.draft == tmp_path / "draft"
    assert loaded.storage.publication.root == tmp_path / "ready"
    assert not loaded.storage.collector.root.exists()
    assert loaded.config.presence.arrival_stability_seconds == 30.0
    assert loaded.config.presence.arrival_max_gap_seconds == 10.0


def test_config_accepts_optional_presence_pair(tmp_path: Path) -> None:
    path = tmp_path / "config.toml"
    path.write_text(
        'pendant_address = "AA:BB:CC:DD:EE:FF"\n'
        "bundle_min_audio_seconds = 3600.0\n"
        "presence_arrival_stability_seconds = 12.5\n"
        "presence_arrival_max_gap_seconds = 4.0\n",
        encoding="utf-8",
    )

    loaded = load_operator_config(path)

    assert loaded.config.presence.arrival_stability_seconds == 12.5
    assert loaded.config.presence.arrival_max_gap_seconds == 4.0


def test_config_wires_bundle_minimum(tmp_path: Path) -> None:
    path = tmp_path / "config.toml"
    path.write_text(
        'pendant_address = "AA:BB:CC:DD:EE:FF"\nbundle_min_audio_seconds = 1200.0\n',
        encoding="utf-8",
    )

    loaded = load_operator_config(path)

    assert loaded.config.ready.target_audio_seconds == 1200.0


def test_config_accepts_optional_scan_silence_threshold(tmp_path: Path) -> None:
    path = tmp_path / "config.toml"
    path.write_text(
        'pendant_address = "AA:BB:CC:DD:EE:FF"\nbundle_min_audio_seconds = 3600.0\nscan_silence_seconds = 240.0\n',
        encoding="utf-8",
    )

    loaded = load_operator_config(path)

    assert loaded.config.presence.scan_silence_seconds == 240.0


@pytest.mark.parametrize("seconds", ["0.0", "nan", "inf", "1e308"])
def test_config_rejects_invalid_bundle_minimum(tmp_path: Path, seconds: str) -> None:
    path = tmp_path / "config.toml"
    path.write_text(
        f'pendant_address = "AA:BB:CC:DD:EE:FF"\nbundle_min_audio_seconds = {seconds}\n',
        encoding="utf-8",
    )

    with pytest.raises(StorageLayoutError):
        load_operator_config(path)


def test_config_requires_bundle_minimum(tmp_path: Path) -> None:
    path = tmp_path / "config.toml"
    path.write_text('pendant_address = "AA:BB:CC:DD:EE:FF"\n', encoding="utf-8")

    with pytest.raises(StorageLayoutError, match="bundle_min_audio_seconds"):
        load_operator_config(path)


def test_config_rejects_incomplete_presence_pair(tmp_path: Path) -> None:
    path = tmp_path / "config.toml"
    path.write_text(
        'pendant_address = "AA:BB:CC:DD:EE:FF"\n'
        "bundle_min_audio_seconds = 3600.0\n"
        "presence_arrival_stability_seconds = 12.5\n",
        encoding="utf-8",
    )

    with pytest.raises(StorageLayoutError):
        load_operator_config(path)


@pytest.mark.parametrize(
    "contents",
    [
        'pendant_address = "aa:bb:cc:dd:ee:ff"\nbundle_min_audio_seconds = 3600.0\n',
        'pendant_address = "AA:BB:CC:DD:EE:FF"\nextra = "x"\n',
        'pendant_address = "AA:BB:CC:DD:EE:FF"\nbundle_min_audio_seconds = 3600.0\nextra = "x"\n',
        'pendant_address = "AA:BB:CC:DD:EE:FF"\nbundle_min_audio_seconds = 3600.0\n[storage]\nroot = "/tmp"\n',
        'version = 1\npendant_address = "AA:BB:CC:DD:EE:FF"\nbundle_min_audio_seconds = 3600.0\n',
        '[pendant]\naddress = "AA:BB:CC:DD:EE:FF"\n[ready]\ntarget_audio_seconds = 3600.0\n',
    ],
)
def test_config_rejects_noncanonical_schema(tmp_path: Path, contents: str) -> None:
    path = tmp_path / "config.toml"
    path.write_text(contents, encoding="utf-8")

    with pytest.raises(StorageLayoutError):
        load_operator_config(path)


@pytest.mark.parametrize(
    "contents",
    [
        "pendant_address = 123\nbundle_min_audio_seconds = 3600.0\n",
        'pendant_address = "AA:BB:CC:DD:EE:FF"\nbundle_min_audio_seconds = "3600"\n',
        'pendant_address = "AA:BB:CC:DD:EE:FF"\nbundle_min_audio_seconds = true\n',
        (
            'pendant_address = "AA:BB:CC:DD:EE:FF"\n'
            "bundle_min_audio_seconds = 3600.0\n"
            "presence_arrival_stability_seconds = true\n"
            "presence_arrival_max_gap_seconds = 4.0\n"
        ),
    ],
)
def test_config_rejects_wrong_field_types(tmp_path: Path, contents: str) -> None:
    path = tmp_path / "config.toml"
    path.write_text(contents, encoding="utf-8")

    with pytest.raises(StorageLayoutError):
        load_operator_config(path)


def test_config_rejects_symlink(tmp_path: Path) -> None:
    target = _config(tmp_path / "target.toml")
    path = tmp_path / "config.toml"
    path.symlink_to(target)

    with pytest.raises(StorageLayoutError, match="non-symlink"):
        load_operator_config(path)


def test_config_rejects_symlinked_parent(tmp_path: Path) -> None:
    target = tmp_path / "target"
    target.mkdir()
    _config(target / "config.toml")
    parent = tmp_path / "linked"
    parent.symlink_to(target, target_is_directory=True)

    with pytest.raises(StorageLayoutError, match="parent must not be a symlink"):
        load_operator_config(parent / "config.toml")


def test_config_rejects_fifo_before_reading(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    path = tmp_path / "config.toml"
    os.mkfifo(path)

    def fail_if_read(self: Path, encoding: str | None = None, errors: str | None = None) -> str:
        del self, encoding, errors
        raise AssertionError("FIFO must be rejected before reading")

    monkeypatch.setattr(Path, "read_text", fail_if_read)

    with pytest.raises(StorageLayoutError, match="regular non-symlink"):
        load_operator_config(path)
