"""One-time two-zone migration only accepts authenticated, non-duplicated input."""

from __future__ import annotations

from hashlib import sha256
from json import dumps, loads
from pathlib import Path
from stat import S_IMODE
from subprocess import CompletedProcess
from types import SimpleNamespace
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
    monkeypatch.setattr(migration.os, "geteuid", lambda: 0)
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
    assert paths.ready.stat().st_gid == paths.speech.stat().st_gid
    assert S_IMODE(paths.ready.stat().st_mode) == 0o2750
    assert S_IMODE(ready.stat().st_mode) == 0o2750
    assert all(S_IMODE(path.stat().st_mode) == 0o640 for path in ready.iterdir())
    assert paths.ledger.stat().st_uid == paths.ledger.parent.stat().st_uid
    assert paths.state.stat().st_gid == paths.state.parent.stat().st_gid
    assert paths.evidence.stat().st_uid == (root / "work").stat().st_uid
    assert paths.generation_evidence.stat().st_gid == (root / "work").stat().st_gid
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


def _systemctl_result(load: str, active: str, unit_file: str) -> CompletedProcess[str]:
    return CompletedProcess(
        ["systemctl"],
        0,
        f"LoadState={load}\nActiveState={active}\nUnitFileState={unit_file}\n",
        "",
    )


def test_system_jit_may_be_absent_but_collector_must_be_loaded_and_inactive(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(migration, "_run", lambda _command: _systemctl_result("not-found", "inactive", ""))

    migration._inactive("omi-speech-archive-jit.service", disabled=True, optional=True)
    with pytest.raises(migration.MigrationError, match="not loaded"):
        migration._inactive("omi-collector.service")

    monkeypatch.setattr(migration, "_run", lambda _command: _systemctl_result("loaded", "inactive", "enabled"))
    with pytest.raises(migration.MigrationError, match="not disabled"):
        migration._inactive("omi-speech-archive-jit.service", disabled=True, optional=True)

    monkeypatch.setattr(migration, "_run", lambda _command: _systemctl_result("loaded", "inactive", "disabled"))
    migration._inactive("omi-collector.service")


def test_root_runner_rejects_unplanned_output_before_moving_legacy(tmp_path: Path) -> None:
    root = tmp_path / "omi"
    paths, _legacy_source, _tail, legacy, _checkpoint = _scenario(root)
    paths.ready.mkdir()
    (paths.ready / "unplanned").mkdir()

    plan = migration.build_plan(paths)

    with pytest.raises(migration.MigrationError, match="non-migration output"):
        migration._validate_ready_destinations(paths, plan)
    assert legacy.exists()


def test_root_runner_sets_only_expected_runtime_ownership(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    root = tmp_path / "omi"
    paths, _legacy_source, _tail, _legacy, _checkpoint = _scenario(root)
    paths.draft.mkdir()
    paths.ready.mkdir()
    ready = paths.ready / "ready"
    draft = paths.draft / "draft"
    ready.mkdir()
    draft.mkdir()
    (ready / "records.bin").write_bytes(b"ready")
    (draft / "records.bin").write_bytes(b"draft")
    paths.ledger.write_text("{}", encoding="utf-8")
    paths.state.write_text("{}", encoding="utf-8")
    calls: list[tuple[Path, int, int, int]] = []

    def record(path: Path, uid: int, gid: int, *, directory_mode: int = 0o750) -> None:
        calls.append((path, uid, gid, directory_mode))

    owner_modes = {
        paths.captured: SimpleNamespace(st_uid=996, st_gid=1000),
        paths.root / "work": SimpleNamespace(st_uid=1000, st_gid=981),
        paths.speech: SimpleNamespace(st_uid=1000, st_gid=1000),
        paths.ledger.parent: SimpleNamespace(st_uid=996, st_gid=981),
    }
    observed_stats: list[Path] = []
    original_stat = Path.stat

    def runtime_stat(path: Path, *args: object, **kwargs: object) -> object:
        observed_stats.append(path)
        return owner_modes.get(path, original_stat(path, *args, **kwargs))

    monkeypatch.setattr(Path, "stat", runtime_stat)
    monkeypatch.setattr(migration, "_set_runtime_ownership", record)

    migration._set_runtime_outputs(paths, (ready,), (draft,))

    assert paths.root / "work" not in observed_stats
    assert calls == [
        (paths.draft, 996, 1000, 0o750),
        (draft, 996, 1000, 0o750),
        (draft / "records.bin", 996, 1000, 0o750),
        (paths.ready, 996, 1000, 0o2750),
        (ready, 996, 1000, 0o2750),
        (ready / "records.bin", 996, 1000, 0o2750),
        (paths.ledger, 996, 981, 0o750),
        (paths.state, 996, 981, 0o750),
    ]


def test_root_runner_rejects_symlink_ownership_target(tmp_path: Path) -> None:
    target = tmp_path / "target"
    target.write_bytes(b"audio")
    link = tmp_path / "link"
    link.symlink_to(target)

    with pytest.raises(migration.MigrationError, match="unsafe"):
        migration._set_runtime_ownership(link, 996, 1000)
