"""Narrow dynamically decoded containers without changing their contents."""

from collections.abc import Mapping, Sequence
from typing import TypeGuard


def is_dict(value: object) -> TypeGuard[dict[object, object]]:
    return isinstance(value, dict)


def is_list(value: object) -> TypeGuard[list[object]]:
    return isinstance(value, list)


def is_tuple(value: object) -> TypeGuard[tuple[object, ...]]:
    return isinstance(value, tuple)


def is_mapping(value: object) -> TypeGuard[Mapping[object, object]]:
    return isinstance(value, Mapping)


def is_sequence(value: object) -> TypeGuard[Sequence[object]]:
    return isinstance(value, Sequence)
