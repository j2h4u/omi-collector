from __future__ import annotations

from pathlib import Path

import pytest

from omi_collector.storage_layout import StorageLayoutError, load_operator_config


def _config(path: Path, address: str = "AA:BB:CC:DD:EE:FF") -> Path:
    path.write_text(
        f'[pendant]\naddress = "{address}"\n[ready]\ntarget_audio_seconds = 3600.0\nmax_wait_seconds = 86400.0\n',
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


def test_config_accepts_strict_optional_presence_section(tmp_path: Path) -> None:
    path = tmp_path / "config.toml"
    path.write_text(
        '[pendant]\naddress = "AA:BB:CC:DD:EE:FF"\n'
        "[presence]\narrival_stability_seconds = 12.5\narrival_max_gap_seconds = 4.0\n"
        "[ready]\ntarget_audio_seconds = 3600.0\nmax_wait_seconds = 86400.0\n",
        encoding="utf-8",
    )

    loaded = load_operator_config(path)

    assert loaded.config.presence.arrival_stability_seconds == 12.5
    assert loaded.config.presence.arrival_max_gap_seconds == 4.0


def test_config_wires_strict_ready_section(tmp_path: Path) -> None:
    path = tmp_path / "config.toml"
    path.write_text(
        '[pendant]\naddress = "AA:BB:CC:DD:EE:FF"\n'
        "[ready]\ntarget_audio_seconds = 1200.0\nmax_wait_seconds = 43200.0\n",
        encoding="utf-8",
    )

    loaded = load_operator_config(path)

    assert loaded.config.ready.target_audio_seconds == 1200.0
    assert loaded.config.ready.max_wait_seconds == 43200.0


@pytest.mark.parametrize(
    "section",
    [
        "target_audio_seconds = 1200.0\n",
        "target_audio_seconds = 1200.0\nmax_wait_seconds = 43200.0\nextra = 1\n",
        "target_audio_seconds = 0.0\nmax_wait_seconds = 43200.0\n",
    ],
)
def test_config_rejects_invalid_ready_section(tmp_path: Path, section: str) -> None:
    path = tmp_path / "config.toml"
    path.write_text(
        '[pendant]\naddress = "AA:BB:CC:DD:EE:FF"\n[ready]\n' + section,
        encoding="utf-8",
    )

    with pytest.raises(StorageLayoutError):
        load_operator_config(path)


def test_config_requires_ready_section(tmp_path: Path) -> None:
    path = tmp_path / "config.toml"
    path.write_text('[pendant]\naddress = "AA:BB:CC:DD:EE:FF"\n', encoding="utf-8")

    with pytest.raises(StorageLayoutError, match=r"\[ready\]"):
        load_operator_config(path)


@pytest.mark.parametrize(
    "presence",
    [
        "arrival_stability_seconds = 12.5\n",
        "arrival_stability_seconds = 12.5\narrival_max_gap_seconds = 4.0\nextra = 1\n",
    ],
)
def test_config_rejects_incomplete_or_extended_presence_section(tmp_path: Path, presence: str) -> None:
    path = tmp_path / "config.toml"
    path.write_text(
        '[pendant]\naddress = "AA:BB:CC:DD:EE:FF"\n[presence]\n'
        + presence
        + "[ready]\ntarget_audio_seconds = 3600.0\nmax_wait_seconds = 86400.0\n",
        encoding="utf-8",
    )

    with pytest.raises(StorageLayoutError):
        load_operator_config(path)


@pytest.mark.parametrize(
    "contents",
    [
        '[pendant]\naddress = "aa:bb:cc:dd:ee:ff"\n',
        '[pendant]\naddress = "AA:BB:CC:DD:EE:FF"\nextra = "x"\n',
        '[pendant]\naddress = "AA:BB:CC:DD:EE:FF"\n[storage]\nroot = "/tmp"\n',
        'version = 1\n[pendant]\naddress = "AA:BB:CC:DD:EE:FF"\n',
    ],
)
def test_config_rejects_noncanonical_schema(tmp_path: Path, contents: str) -> None:
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
