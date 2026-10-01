from __future__ import annotations

import os
import stat
from pathlib import Path

import pytest
from scripts.seal_deployment_tree import (
    _parse_owner,
    _relative_target_stays_within,
    _seal_directory,
    _seal_file,
    _seal_tree,
    _SealContext,
    _validate_link,
    main,
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


@pytest.mark.parametrize("case", ["non_regular_fd", "different_device", "different_inode"])
def test_seal_file_rejects_descriptor_mismatches_before_changing_metadata(tmp_path: Path, case: str) -> None:
    target = tmp_path / ("directory" if case == "non_regular_fd" else "file")
    if case == "non_regular_fd":
        target.mkdir()
        target.chmod(0o700)
    else:
        target.write_text("file", encoding="utf-8")
        target.chmod(0o600)
    other = tmp_path / "other"
    other.write_text("other", encoding="utf-8")
    original = target.stat()
    opened_stat = target.stat()
    if case == "different_inode":
        opened_stat = other.stat()
    root_device = original.st_dev + 1 if case == "different_device" else original.st_dev
    flags = os.O_RDONLY | (os.O_DIRECTORY if case == "non_regular_fd" else 0)
    descriptor = os.open(target, flags)
    context = _SealContext((os.getuid(), os.getgid()), root_device, str(tmp_path), ())

    try:
        with pytest.raises(RuntimeError):
            _seal_file(descriptor, opened_stat, context, executable=False)
    finally:
        os.fchmod(descriptor, stat.S_IMODE(original.st_mode))
        os.close(descriptor)

    actual = target.stat()
    assert stat.S_IMODE(actual.st_mode) == stat.S_IMODE(original.st_mode)
    assert (actual.st_uid, actual.st_gid) == (original.st_uid, original.st_gid)


@pytest.mark.parametrize("case", ["non_directory_fd", "different_device", "different_inode"])
def test_seal_directory_rejects_descriptor_mismatches_before_changing_metadata(tmp_path: Path, case: str) -> None:
    target = tmp_path / ("file" if case == "non_directory_fd" else "directory")
    if case == "non_directory_fd":
        target.write_text("file", encoding="utf-8")
        target.chmod(0o600)
    else:
        target.mkdir()
        target.chmod(0o700)
    other = tmp_path / "other-directory"
    other.mkdir()
    other.chmod(0o700)
    original = target.stat()
    expected_stat = other.stat() if case == "different_inode" else None
    root_device = original.st_dev + 1 if case == "different_device" else original.st_dev
    flags = os.O_RDONLY | (os.O_DIRECTORY if case != "non_directory_fd" else 0)
    descriptor = os.open(target, flags)
    context = _SealContext((os.getuid(), os.getgid()), root_device, str(tmp_path), ())

    try:
        with pytest.raises(RuntimeError):
            _seal_directory(descriptor, context, (), expected_stat)
    finally:
        os.fchmod(descriptor, stat.S_IMODE(original.st_mode))
        os.close(descriptor)

    actual = target.stat()
    assert stat.S_IMODE(actual.st_mode) == stat.S_IMODE(original.st_mode)
    assert (actual.st_uid, actual.st_gid) == (original.st_uid, original.st_gid)


def test_main_seals_absolute_tree_with_absolute_external_allowance(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = tmp_path / "release"
    root.mkdir()
    file = root / "data"
    file.write_text("release", encoding="utf-8")
    file.chmod(0o600)
    external = tmp_path / "external"
    external.mkdir()
    monkeypatch.setattr(
        "sys.argv",
        [
            "seal_deployment_tree.py",
            "--root",
            str(root),
            "--owner",
            f"{os.getuid()}:{os.getgid()}",
            "--allow-external",
            str(external),
        ],
    )

    main()

    assert file.stat().st_mode & 0o777 == 0o644
    assert root.stat().st_mode & 0o777 == 0o755


@pytest.mark.parametrize(
    ("invalid_option", "invalid_value", "message"),
    [
        ("--root", "release", "root must be absolute"),
        ("--allow-external", "external", "allowed external paths must be absolute"),
    ],
)
def test_main_rejects_relative_paths_before_sealing(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    invalid_option: str,
    invalid_value: str,
    message: str,
) -> None:
    root = tmp_path / "release"
    root.mkdir()
    file = root / "data"
    file.write_text("release", encoding="utf-8")
    file.chmod(0o600)
    root.chmod(0o700)
    arguments = ["seal_deployment_tree.py", "--root", str(root), "--owner", f"{os.getuid()}:{os.getgid()}"]
    if invalid_option == "--root":
        arguments[arguments.index("--root") + 1] = invalid_value
        monkeypatch.chdir(tmp_path)
    else:
        arguments.extend((invalid_option, invalid_value))
    monkeypatch.setattr("sys.argv", arguments)

    try:
        with pytest.raises(ValueError, match=message):
            main()
    finally:
        file.chmod(0o600)
        root.chmod(0o700)

    assert file.stat().st_mode & 0o777 == 0o600
    assert root.stat().st_mode & 0o777 == 0o700


@pytest.mark.parametrize("missing_option", ["--root", "--owner"])
def test_main_requires_root_and_owner_arguments(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, missing_option: str
) -> None:
    root = tmp_path / "release"
    root.mkdir()
    original_mode = stat.S_IMODE(root.stat().st_mode)
    arguments = ["seal_deployment_tree.py"]
    if missing_option == "--root":
        arguments.extend(("--owner", f"{os.getuid()}:{os.getgid()}"))
    else:
        arguments.extend(("--root", str(root)))
    monkeypatch.setattr("sys.argv", arguments)

    with pytest.raises(SystemExit) as error:
        main()

    assert error.value.code == 2
    assert stat.S_IMODE(root.stat().st_mode) == original_mode
