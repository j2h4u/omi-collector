from __future__ import annotations

import json
import re
import sqlite3
from contextlib import closing
from hashlib import sha256
from pathlib import Path

from omi_collector.state_migration import _owned_v2_ready_stage


def test_physical_stage_requires_matching_durable_owner(tmp_path: Path) -> None:
    artifact = "a" * 64
    identity = f"{'b' * 64}:0:{artifact}"
    stage_id = sha256(identity.encode()).hexdigest()
    path = tmp_path / f".{artifact}.{stage_id[:16]}.stage"
    path.mkdir()
    match = re.fullmatch(r"\.([0-9a-f]{64})\.([0-9a-f]{16})\.stage", path.name)
    assert match is not None
    with closing(sqlite3.connect(":memory:")) as connection:
        connection.execute(
            "CREATE TABLE ready_v2_output_stage(stage_id, group_identity, artifact_id, owner_token, owned_path)"
        )
        connection.execute("CREATE TABLE publication_cohort(cohort_id, lifecycle, cursor)")
        connection.execute("CREATE TABLE state(kind, id, payload)")
        connection.execute(
            "INSERT INTO ready_v2_output_stage VALUES (?, ?, ?, ?, ?)",
            (stage_id, identity, artifact, stage_id, str(path)),
        )
        assert not _owned_v2_ready_stage(path, match, connection)
        connection.execute(
            "INSERT INTO state VALUES ('physical_import', ?, ?)",
            (stage_id, json.dumps({"state": "staging", "stage_identity": identity})),
        )
        assert _owned_v2_ready_stage(path, match, connection)
        (path / "unexpected").touch()
        assert not _owned_v2_ready_stage(path, match, connection)
