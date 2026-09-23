"""One-time two-zone migration only accepts authenticated, non-duplicated input."""

from __future__ import annotations

from hashlib import sha256
from json import dumps, loads
from pathlib import Path
from typing import cast

import pytest
from scripts import migrate_two_zone_once as migration

from omi_collector.capture.adapters.bundle_contract import BundleManifest, SealedReceipt
from omi_collector.capture.domain.ring_protocol import RECORD_SIZE


def _records(values: range) -> bytes:
    return b"".join(value.to_bytes(4, "big") + bytes((value,)) * (RECORD_SIZE - 4) for value in values)


def _bundle(root: Path, values: range) -> tuple[Path, str]:
    records = _records(values)
    digest = sha256(records).hexdigest()
    path = root / f"{values.start}-{values.stop}-{digest[:16]}"
    path.mkdir(parents=True)
    (path / "records.bin").write_bytes(records)
    manifest = BundleManifest(2, values.start, values.stop, len(values), RECORD_SIZE, digest)
    (path / "manifest.json").write_text(dumps(manifest.as_dict()), encoding="utf-8")
    (path / "receipt.json").write_text(dumps(SealedReceipt("a" * 32, digest).as_dict()), encoding="utf-8")
    return path, digest


def _controls(root: Path) -> tuple[Path, Path, Path]:
    work = root / "work"
    work.mkdir()
    checkpoint = work / "e4a4.json"
    checkpoint.write_text("{}", encoding="utf-8")
    attestation = work / "windmill-queue-attestation.json"
    attestation.write_text(
        dumps({"schema": "omi-windmill-queue-attestation-v1", "queued_omi_jobs": []}), encoding="utf-8"
    )
    (work / "speech_archive_jit.lock").write_bytes(b"")
    collector = root / "collector"
    collector.mkdir()
    (collector / "collector.lock").write_bytes(b"")
    speech = root / "speech"
    speech.mkdir()
    for index in range(12):
        (speech / f"pair-{index}.ogg").write_bytes(b"ogg")
        (speech / f"pair-{index}.json").write_text("{}", encoding="utf-8")
    return checkpoint, attestation, speech


def _scenario(root: Path) -> tuple[migration.Paths, Path, Path, Path, Path]:
    captured = root / "captured"
    legacy_source, legacy_hash = _bundle(captured, range(10, 12))
    tail, _ = _bundle(captured, range(20, 23))
    _bundle(captured, range(21, 22))
    generation = root / "source" / ".generations" / "generation"
    legacy = generation / legacy_source.name
    legacy.parent.mkdir(parents=True)
    legacy_source.rename(legacy)
    _bundle(captured, range(10, 12))
    (generation / "generation.json").write_text(
        dumps(
            {
                "algorithm": 2,
                "generation_id": generation.name,
                "record_count": 2,
                "repairs": [],
                "source_hashes": [legacy_hash],
            }
        ),
        encoding="utf-8",
    )
    checkpoint, attestation, speech = _controls(root)
    current = root / "source" / "current"
    current.symlink_to(Path(".generations") / generation.name)
    return (
        migration.Paths(
            root,
            current,
            captured,
            root / "draft",
            root / "ready",
            root / "collector" / "ready-publications.json",
            root / "collector" / "two-zone-migration.json",
            root / "collector" / "two-zone-frozen-inventory.json",
            root / "collector" / "two-zone-migration-state.json",
            root / "work" / "two-zone-legacy-evidence",
            root / "work" / "two-zone-generation.json",
            checkpoint,
            attestation,
            speech,
        ),
        legacy_source,
        tail,
        legacy,
        checkpoint,
    )


def test_plan_keeps_one_contained_duplicate_out_of_draft(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    root = tmp_path / "omi"
    paths, legacy_source, tail, legacy, checkpoint = _scenario(root)

    plan = migration.build_plan(paths)

    assert [item.captured.path.name for item in plan.ready] == [legacy_source.name]
    assert [(item.start_sequence, item.next_sequence) for item in plan.drafts] == [(20, 23)]
    assert tail.exists()
    assert legacy.exists()

    monkeypatch.setattr(migration, "_quiescent", lambda _paths: None)
    monkeypatch.setattr(migration, "_inactive", lambda _service, **_kwargs: None)
    monkeypatch.setattr(migration, "_windmill_quiescent", lambda: None)
    migration.execute(paths)

    ready = next(paths.ready.iterdir())
    ledger = cast(dict[str, object], loads(paths.ledger.read_text(encoding="utf-8")))
    provenance = cast(dict[str, object], loads(paths.log.read_text(encoding="utf-8")))
    inventory = cast(dict[str, object], loads(paths.inventory.read_text(encoding="utf-8")))
    bundles = cast(dict[str, dict[str, str]], ledger["bundles"])
    mappings = cast(list[dict[str, dict[str, str]]], provenance["mappings"])
    assert (paths.draft / tail.name).is_dir()
    assert not legacy.exists()
    assert not tail.exists()
    assert bundles[ready.name]["state"] == "ready"
    assert provenance["schema"] == "omi-two-zone-ready-migration-v1"
    assert mappings[0]["ready"]["bundle_id"] == ready.name
    assert inventory["schema"] == "omi-two-zone-frozen-inventory-v1"
    assert len(cast(list[object], inventory["legacy_evidence"])) == 1
    migration.execute(paths)
    checkpoint.write_text(
        dumps(
            {
                "analysis_cursor": None,
                "vad_decisions": [],
                "open_speech_tail": None,
                "acknowledged": [{"bundle_id": ready.name, "records_sha256": bundles[ready.name]["records_sha256"]}],
            }
        ),
        encoding="utf-8",
    )
    with pytest.raises(migration.MigrationError, match="legacy checkpoint evidence"):
        migration.finalize(paths)


def test_historic_ack_identity_set_rejects_one_later_ready_bundle(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source_ids = [f"{index:064x}" for index in range(81)]
    evidence = tmp_path / "legacy-checkpoint.json"
    evidence.write_text(
        dumps(
            {
                "queue": [{"identity": source_id} for source_id in source_ids[:80]],
                "frontier": {"source_id": source_ids[80]},
            }
        ),
        encoding="utf-8",
    )
    mappings = [
        {
            "legacy": {"source_id": source_id},
            "ready": {"bundle_id": f"{index + 100:064x}", "records_sha256": f"{index + 200:064x}"},
        }
        for index, source_id in enumerate(source_ids)
    ]
    provenance = tmp_path / "migration.json"
    provenance.write_text(dumps({"mappings": mappings}), encoding="utf-8")
    inventory: dict[str, object] = {
        "checkpoint": {"path": str(evidence), "sha256": sha256(evidence.read_bytes()).hexdigest()}
    }

    expected = migration._historic_ack_identities(inventory, provenance)
    wrong = set(expected)
    wrong.remove(next(iter(expected)))
    wrong.add(("f" * 64, "e" * 64))

    assert len(expected) == len(wrong) == 81
    assert wrong != expected
    paths = migration.Paths(
        tmp_path,
        tmp_path / "current",
        tmp_path / "captured",
        tmp_path / "draft",
        tmp_path / "ready",
        tmp_path / "ledger",
        provenance,
        tmp_path / "inventory",
        tmp_path / "state",
        tmp_path / "evidence",
        tmp_path / "generation-evidence",
        tmp_path / "new-checkpoint",
        None,
        tmp_path / "speech",
    )
    monkeypatch.setattr(migration.ready_bundles, "_windmill_acknowledged", lambda _path: tuple(wrong))

    with pytest.raises(migration.MigrationError, match="historic acknowledgement"):
        migration._retirement_acknowledged(paths, inventory)
