from __future__ import annotations

from pathlib import Path

import pytest

from omi_collector.storage_layout import StorageLayoutError, load_operator_config


def _config(path: Path, address: str = "AA:BB:CC:DD:EE:FF") -> Path:
    path.write_text(f'[pendant]\naddress = "{address}"\n', encoding="utf-8")
    return path


def test_config_parent_is_storage_root_and_loading_creates_nothing(tmp_path: Path) -> None:
    loaded = load_operator_config(_config(tmp_path / "config.toml"))

    assert loaded.pendant.address == "AA:BB:CC:DD:EE:FF"
    assert loaded.storage.root == tmp_path
    assert loaded.storage.collector.root == tmp_path / "collector"
    assert loaded.storage.captured == tmp_path / "captured"
    assert loaded.storage.publication.root == tmp_path / "source"
    assert loaded.storage.publication.current == tmp_path / "source" / "current"
    assert not loaded.storage.collector.root.exists()


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
