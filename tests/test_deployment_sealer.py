from __future__ import annotations

import os
from pathlib import Path

import pytest
from scripts.seal_deployment_tree import (
    _parse_owner,
    _relative_target_stays_within,
    _seal_tree,
    _validate_link,
)


@pytest.mark.parametrize(("owner", "expected"), [("0:0", (0, 0)), ("1000:100", (1000, 100))])
def test_parse_owner_accepts_numeric_uid_gid_pairs(owner: str, expected: tuple[int, int]) -> None:
    assert _parse_owner(owner) == expected


@pytest.mark.parametrize("owner", ["1000", ":100", "1000:", "-1:100", "1000:gid", "1:2:3"])
def test_parse_owner_rejects_malformed_pairs(owner: str) -> None:
    with pytest.raises(ValueError):
        _parse_owner(owner)


@pytest.mark.parametrize(
    "target",
    ["/srv/releases", "/srv/releases/../releases/bin/omi-collector"],
)
def test_validate_link_allows_targets_at_or_normalized_within_tree(target: str) -> None:
    _validate_link(target, "/srv/releases", (), ())


def test_relative_target_allows_normalized_path_within_tree() -> None:
    assert _relative_target_stays_within(("one", "two"), "../three/./file")


def test_relative_target_rejects_nested_parent_escape() -> None:
    assert not _relative_target_stays_within(("one", "two"), "../../../outside")


def test_validate_link_rejects_sibling_with_shared_path_prefix() -> None:
    with pytest.raises(RuntimeError, match="external symlink target"):
        _validate_link("/srv/releases-old/file", "/srv/releases", (), ())


def test_validate_link_allows_target_within_configured_external_root() -> None:
    _validate_link("/opt/shared/lib/tool", "/srv/releases", (), ("/opt/shared",))


def test_seal_tree_sets_nested_modes_and_owner(tmp_path: Path) -> None:
    root = tmp_path / "release"
    executable = root / "bin" / "omi-collector"
    library = root / "lib" / "nested" / "module.py"
    executable.parent.mkdir(parents=True)
    library.parent.mkdir(parents=True)
    executable.write_text("run", encoding="utf-8")
    library.write_text("module", encoding="utf-8")
    executable.chmod(0o600)
    library.chmod(0o600)
    executable.parent.chmod(0o700)
    library.parent.chmod(0o700)
    (root / "lib").chmod(0o700)
    root.chmod(0o700)

    owner = os.getuid(), os.getgid()
    _seal_tree(str(root), owner, ())

    assert root.stat().st_mode & 0o777 == 0o755
    assert executable.parent.stat().st_mode & 0o777 == 0o755
    assert (root / "lib").stat().st_mode & 0o777 == 0o755
    assert library.parent.stat().st_mode & 0o777 == 0o755
    assert executable.stat().st_mode & 0o777 == 0o755
    assert library.stat().st_mode & 0o777 == 0o644
    assert (executable.stat().st_uid, executable.stat().st_gid) == owner


def test_seal_tree_preserves_allowed_external_symlink_and_target(tmp_path: Path) -> None:
    root = tmp_path / "release"
    root.mkdir()
    target = tmp_path / "shared-data"
    target.write_bytes(b"outside release")
    target.chmod(0o666)
    link = root / "shared-data"
    link.symlink_to(target)

    target_state = target.stat()
    _seal_tree(str(root), (os.getuid(), os.getgid()), (str(tmp_path),))

    assert link.is_symlink()
    assert link.readlink() == target
    assert target.read_bytes() == b"outside release"
    assert target.stat().st_mode & 0o777 == 0o666
    assert (target.stat().st_uid, target.stat().st_gid) == (target_state.st_uid, target_state.st_gid)


@pytest.mark.parametrize(
    ("target", "message"),
    [
        ("../../outside", "escapes the sealed tree"),
        ("/opt/outside/file", "external symlink target"),
    ],
)
def test_seal_tree_rejects_symlink_escapes(tmp_path: Path, target: str, message: str) -> None:
    root = tmp_path / "release"
    branch = root / "nested"
    branch.mkdir(parents=True)
    (branch / "link").symlink_to(target)

    with pytest.raises(RuntimeError, match=message):
        _seal_tree(str(root), (os.getuid(), os.getgid()), ())


def test_seal_tree_rejects_fifo_without_opening_it(tmp_path: Path) -> None:
    root = tmp_path / "release"
    root.mkdir()
    os.mkfifo(root / "pipe")

    with pytest.raises(RuntimeError, match="unsupported candidate filesystem entry"):
        _seal_tree(str(root), (os.getuid(), os.getgid()), ())
