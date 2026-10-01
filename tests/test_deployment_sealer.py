from __future__ import annotations

import pytest
from scripts.seal_deployment_tree import (
    _parse_owner,
    _relative_target_stays_within,
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
