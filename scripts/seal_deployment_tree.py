#!/usr/bin/env python3
"""Seal one deployment tree without following candidate-controlled paths."""

import argparse
import os
import stat
import sys
from dataclasses import dataclass
from pathlib import PurePosixPath
from typing import cast


def _parse_owner(owner: str) -> tuple[int, int]:
    user_id, separator, group_id = owner.partition(":")
    if not separator or not user_id.isdecimal() or not group_id.isdecimal():
        raise ValueError("owner must be a numeric UID:GID pair")
    return int(user_id), int(group_id)


def _path_is_within(target: str, root: str) -> bool:
    try:
        return os.path.commonpath((target, root)) == root
    except ValueError:
        return False


def _relative_target_stays_within(parent_parts: tuple[str, ...], target: str) -> bool:
    parts = list(parent_parts)
    for part in PurePosixPath(target).parts:
        if part in ("", "."):
            continue
        if part == "..":
            if not parts:
                return False
            parts.pop()
            continue
        parts.append(part)
    return True


@dataclass(frozen=True, slots=True)
class _SealContext:
    owner: tuple[int, int]
    root_device: int
    allowed_external: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class _SealerArguments:
    root: str
    owner: str
    allow_external: list[str]


class _ArgparseArguments(argparse.Namespace):
    root: str
    owner: str
    allow_external: list[str]


def _validate_link(target: str, parent_parts: tuple[str, ...], allowed_external: tuple[str, ...]) -> None:
    if PurePosixPath(target).is_absolute():
        if any(_path_is_within(target, root) for root in allowed_external):
            return
        raise RuntimeError(f"refusing external symlink target: {target}")
    if not _relative_target_stays_within(parent_parts, target):
        raise RuntimeError(f"refusing symlink that escapes the sealed tree: {target}")


def _open_child(directory_fd: int, name: str, flags: int) -> int:
    return os.open(name, flags | os.O_NOFOLLOW | os.O_CLOEXEC, dir_fd=directory_fd)


def _seal_file(file_fd: int, entry_stat: os.stat_result, context: _SealContext, executable: bool) -> None:
    opened_stat = os.fstat(file_fd)
    if not stat.S_ISREG(opened_stat.st_mode) or opened_stat.st_dev != context.root_device:
        raise RuntimeError("refusing non-regular file or mount escape")
    if (opened_stat.st_dev, opened_stat.st_ino) != (entry_stat.st_dev, entry_stat.st_ino):
        raise RuntimeError("refusing a file that changed during sealing")
    os.fchown(file_fd, *context.owner)
    os.fchmod(file_fd, 0o755 if executable else 0o644)


def _seal_directory(
    directory_fd: int,
    context: _SealContext,
    parent_parts: tuple[str, ...],
    expected_stat: os.stat_result | None = None,
) -> None:
    directory_stat = os.fstat(directory_fd)
    if not stat.S_ISDIR(directory_stat.st_mode) or directory_stat.st_dev != context.root_device:
        raise RuntimeError("refusing non-directory or mount escape")
    if expected_stat is not None and (directory_stat.st_dev, directory_stat.st_ino) != (
        expected_stat.st_dev,
        expected_stat.st_ino,
    ):
        raise RuntimeError("refusing a directory that changed during sealing")
    os.fchown(directory_fd, *context.owner)
    os.fchmod(directory_fd, 0o755)
    with os.scandir(directory_fd) as entries:
        for entry in entries:
            entry_stat = os.stat(entry.name, dir_fd=directory_fd, follow_symlinks=False)
            if stat.S_ISLNK(entry_stat.st_mode):
                _validate_link(os.readlink(entry.name, dir_fd=directory_fd), parent_parts, context.allowed_external)
                continue
            if stat.S_ISDIR(entry_stat.st_mode):
                child_fd = _open_child(directory_fd, entry.name, os.O_RDONLY | os.O_DIRECTORY)
                try:
                    _seal_directory(
                        child_fd,
                        context,
                        (*parent_parts, entry.name),
                        entry_stat,
                    )
                finally:
                    os.close(child_fd)
                continue
            if stat.S_ISREG(entry_stat.st_mode):
                file_fd = _open_child(directory_fd, entry.name, os.O_RDONLY)
                try:
                    _seal_file(
                        file_fd,
                        entry_stat,
                        context,
                        parent_parts[-1:] == ("bin",),
                    )
                finally:
                    os.close(file_fd)
                continue
            raise RuntimeError(f"refusing unsupported candidate filesystem entry: {entry.name}")


def _seal_tree(root: str, owner: tuple[int, int], allowed_external: tuple[str, ...]) -> None:
    root_fd = os.open(root, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC)
    try:
        context = _SealContext(owner, os.fstat(root_fd).st_dev, allowed_external)
        _seal_directory(root_fd, context, ())
    finally:
        os.close(root_fd)


def _parse_arguments() -> _SealerArguments:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", required=True)
    parser.add_argument("--owner", required=True)
    parser.add_argument("--allow-external", action="append", default=[])
    arguments = cast(_ArgparseArguments, parser.parse_args())
    if not isinstance(arguments.root, str) or not isinstance(arguments.owner, str):
        raise ValueError("root and owner must be strings")
    if not isinstance(arguments.allow_external, list) or not all(
        isinstance(path, str) for path in arguments.allow_external
    ):
        raise ValueError("allowed external paths must be strings")
    return _SealerArguments(arguments.root, arguments.owner, arguments.allow_external)


def main() -> int:
    arguments = _parse_arguments()
    if not PurePosixPath(arguments.root).is_absolute():
        raise ValueError("root must be absolute")
    allowed_external = tuple(str(PurePosixPath(path)) for path in arguments.allow_external)
    if not all(PurePosixPath(path).is_absolute() for path in allowed_external):
        raise ValueError("allowed external paths must be absolute")
    _seal_tree(arguments.root, _parse_owner(arguments.owner), allowed_external)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (OSError, RuntimeError, ValueError) as error:
        print(f"ERROR: {error}", file=sys.stderr)
        raise SystemExit(1) from error
