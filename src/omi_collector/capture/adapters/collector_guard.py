"""One stable process guard for Collector-owned mutable work."""

from __future__ import annotations

import fcntl
import os
import stat
from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from pathlib import Path


class CollectorGuardError(RuntimeError):
    """The Collector process guard cannot be acquired safely."""


@dataclass(slots=True)
class _GuardScope:
    path: Path
    active: bool = True


_CURRENT: ContextVar[_GuardScope | None] = ContextVar("omi_collector_guard", default=None)


def guard_is_held(path: Path) -> bool:
    """Whether the current invocation owns the Collector guard."""
    scope = _CURRENT.get()
    return scope is not None and scope.active and scope.path == Path(path).absolute()


@contextmanager
def collector_guard(path: Path) -> Iterator[None]:
    """Hold one stable flock inode for an entire writable invocation."""
    target = Path(path).absolute()
    if _CURRENT.get() is not None:
        raise CollectorGuardError("a Collector guard is already active in this invocation")
    _prepare_parent(target.parent)
    flags = os.O_RDWR | os.O_CREAT | os.O_CLOEXEC | os.O_NOFOLLOW
    try:
        fd = os.open(target, flags, 0o600)
    except OSError as error:
        raise CollectorGuardError("Collector guard cannot be opened") from error
    try:
        if not stat.S_ISREG(os.fstat(fd).st_mode):
            raise CollectorGuardError("Collector guard must be a regular file")
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise CollectorGuardError("Collector service or command is already active") from error
        if os.fstat(fd).st_ino != target.stat(follow_symlinks=False).st_ino:
            raise CollectorGuardError("Collector guard inode changed during acquisition")
        os.fchmod(fd, 0o600)
        os.ftruncate(fd, 0)
        scope = _GuardScope(target)
        token = _CURRENT.set(scope)
        try:
            yield
        finally:
            scope.active = False
            _CURRENT.reset(token)
    finally:
        os.close(fd)


def _prepare_parent(parent: Path) -> None:
    try:
        parent.mkdir(parents=True, exist_ok=True)
        mode = parent.lstat().st_mode
    except OSError as error:
        raise CollectorGuardError("Collector guard directory is unavailable") from error
    if stat.S_ISLNK(mode) or not stat.S_ISDIR(mode):
        raise CollectorGuardError("Collector guard directory is unsafe")
