"""Reserve an ordered READY stage for authenticated physical audio."""

from __future__ import annotations

import json
import os
import sqlite3
from collections.abc import Callable, Iterable, Iterator
from contextlib import closing
from dataclasses import dataclass
from functools import partial
from hashlib import sha256
from pathlib import Path
from typing import cast

from ..domain.physical_recovery import PhysicalSpan
from ..domain.public_audio_v2 import canonical_manifest, derive_artifact_id
from ..domain.publication_contract import ContractMode
from . import ready_bundles as ready
from .collector_state_db import StateDB, StateKind, StateTransaction
from .internal_audio_store import InternalAudioStore, decode_uint64, encode_uint64
from .physical_audio import PreparedPhysicalAudio, iter_physical_audio, prepare_physical_audio
from .physical_source import authenticate_source, read_physical_span, source_identity
from .v2_audio import write_v2_audio


@dataclass(frozen=True, slots=True)
class PhysicalStage:
    stage_id: str
    artifact_id: str
    order: int
    path: Path
    manifest: bytes


@dataclass(frozen=True, slots=True)
class PhysicalStageInput:
    descriptor: dict[str, object]
    prepared: PreparedPhysicalAudio
    records_sha: str
    read_span: Callable[[PhysicalSpan], Iterable[bytes]]
    source_identity: Callable[[str], tuple[int, str]]


def published_physical_artifact(db: StateDB, descriptor: dict[str, object]) -> str | None:
    """Return the durable result of this exact physical publication, including after ACK."""
    encoded = ready._canonical(descriptor)
    digest = sha256(encoded).hexdigest()
    matches: list[str] = []
    for _stage_id, marker in db.list(StateKind.PHYSICAL_IMPORT):
        if marker.get("state") != "published" or marker.get("source_id") != descriptor.get("source_id"):
            continue
        if marker.get("descriptor_sha256") != digest:
            raise ready.ReadyBundleError("physical source already has a different publication")
        artifact_id = marker.get("artifact_id")
        if not isinstance(artifact_id, str):
            raise ready.ReadyDurabilityFatalError("physical publication marker is invalid")
        entry = db.get(StateKind.READY_PUBLICATION, artifact_id)
        if (
            entry is None
            or not ready._valid_ledger_entry(artifact_id, entry)
            or any(
                marker.get(key) != descriptor.get(key)
                for key in ("anchor", "ordering", "overlap", "record_projection_sha256")
            )
        ):
            raise ready.ReadyDurabilityFatalError("physical publication marker conflicts with its ledger")
        ready._v2_byte_span_projection(encoded, entry)
        matches.append(artifact_id)
    if len(matches) > 1:
        raise ready.ReadyDurabilityFatalError("physical source has multiple published results")
    return matches[0] if matches else None


def resume_physical_stage(
    store: InternalAudioStore, root: Path, expected_descriptor: dict[str, object] | None = None
) -> ready.ReadyBundleResult | None:
    """Resume a physical stage after a restart, reauthenticating both sources."""
    read_db = StateDB(store.state_db.path, read_only=True)
    with closing(read_db.open_connection(read_only=True)) as connection, read_db.access(connection):
        orphaned = ready._sql_one(
            connection.execute(
                "SELECT (SELECT count(*) FROM ready_v2_output_stage AS s WHERE "
                "NOT EXISTS (SELECT 1 FROM state AS m WHERE m.kind=? AND m.id=s.stage_id "
                "AND json_extract(m.payload,'$.state')='staging') AND NOT EXISTS "
                "(SELECT 1 FROM publication_cohort AS c WHERE c.lifecycle='staging' "
                "AND s.group_identity LIKE c.cohort_id || ':%')) + "
                "(SELECT count(*) FROM state AS m WHERE m.kind=? AND json_extract(m.payload,'$.state')='staging' "
                "AND NOT EXISTS (SELECT 1 FROM ready_v2_output_stage AS s WHERE s.stage_id=m.id))",
                (StateKind.PHYSICAL_IMPORT.value, StateKind.PHYSICAL_IMPORT.value),
            )
        )
        if orphaned != (0,):
            raise ready.ReadyDurabilityFatalError("publication stage has no durable owner")
        rows = ready._sql_all(
            connection.execute(
                "SELECT s.descriptor FROM ready_v2_output_stage AS s JOIN state AS m "
                "ON m.kind=? AND m.id=s.stage_id WHERE json_extract(m.payload,'$.state')='staging'",
                (StateKind.PHYSICAL_IMPORT.value,),
            )
        )
        if not rows:
            return None
        if len(rows) != 1 or not isinstance(rows[0][0], bytes):
            raise ready.ReadyBundleError("physical stage custody is ambiguous")
        descriptor = cast(dict[str, object], json.loads(rows[0][0]))
        if expected_descriptor is not None and descriptor != expected_descriptor:
            raise ready.ReadyBundleError("physical stage differs from reviewed recovery plan")
        reference = cast(dict[str, object], descriptor.get("anchor", descriptor.get("ordering")))
        reference_id = reference.get("repeat_source_id", reference.get("predecessor_source_id"))
        authenticate_source(connection, cast(str, reference_id))
        read = partial(read_physical_span, connection)
        identity = partial(source_identity, connection)
        prepared = prepare_physical_audio(descriptor, read, identity)
        stage, records_sha = load_physical_stage(connection, root, descriptor, prepared)
    with closing(read_db.open_connection(read_only=True)) as connection, read_db.access(connection):
        source = PhysicalStageInput(
            descriptor,
            prepared,
            records_sha,
            partial(read_physical_span, connection),
            partial(source_identity, connection),
        )
        return publish_physical_stage(store, stage, source)


def load_physical_stage(
    connection: sqlite3.Connection, root: Path, descriptor: dict[str, object], prepared: PreparedPhysicalAudio
) -> tuple[PhysicalStage, str]:
    """Reconstruct a stage from durable evidence after independently rechecking its input."""
    rows = ready._sql_all(
        connection.execute(
            "SELECT s.stage_id,s.group_identity,s.owned_path,s.public_order,s.artifact_id,s.owner_token,"
            "s.descriptor,s.expected_audio_sha256,s.expected_manifest_sha256,p.records_sha256,p.lifecycle "
            "FROM ready_v2_output_stage AS s JOIN ready_replay_proof AS p ON p.stage_id=s.stage_id "
            "AND p.bundle_id=s.artifact_id JOIN state AS m ON m.kind=? AND m.id=s.stage_id",
            (StateKind.PHYSICAL_IMPORT.value,),
        )
    )
    if len(rows) != 1:
        raise ready.ReadyBundleError("physical stage custody is missing or ambiguous")
    row = cast(tuple[str, str, str, bytes, str, str, bytes, str, str, str, str], rows[0])
    stage_id, identity, artifact_id = row[0], row[1], row[4]
    order = decode_uint64(row[3])
    path = root / f".{artifact_id}.{stage_id[:16]}.stage"
    manifest = canonical_manifest(
        artifact_id=artifact_id, order=order, audio_sha256=prepared.audio.sha256, projection=prepared.projection
    )
    marker = ready._sql_one(
        connection.execute(
            "SELECT payload FROM state WHERE kind=? AND id=?", (StateKind.PHYSICAL_IMPORT.value, stage_id)
        )
    )
    if (
        stage_id != sha256(identity.encode()).hexdigest()
        or row[5] != stage_id
        or row[2] != str(path)
        or row[6] != ready._canonical(descriptor)
        or prepared.record_projection_sha256 != descriptor.get("record_projection_sha256")
    ):
        raise ready.ReadyBundleError("physical stage evidence changed")
    if (
        row[7] != prepared.audio.sha256
        or row[8] != sha256(manifest).hexdigest()
        or row[10] != "staging"
        or marker is None
        or json.loads(cast(str, marker[0])) != {"state": "staging", "stage_identity": identity}
    ):
        raise ready.ReadyBundleError("physical stage evidence changed")
    return PhysicalStage(stage_id, artifact_id, order, path, manifest), row[9]


def _require_physical_admission(
    state: StateTransaction, connection: sqlite3.Connection, descriptor: dict[str, object]
) -> None:
    source = ready._sql_one(
        connection.execute(
            "SELECT s.committed_count,s.committed_sha256,h.disposition,s.sequence_start FROM capture_source AS s "
            "JOIN capture_historical_custody AS h ON h.source_id=s.source_id WHERE s.source_id=?",
            (descriptor["source_id"],),
        )
    )
    if source is None or source[:3] != (
        cast(int, descriptor["source_bytes"]) // 444,
        descriptor["source_sha256"],
        "pending",
    ):
        raise ready.ReadyBundleError("physical source no longer has pending historical custody")
    if "ordering" in descriptor:
        from .ordered_payloads import next_ordered_physical_start, select_ordered_payload
        from .ordered_physical_plan import _historical_frontier_blocks_payload

        selected = select_ordered_payload(connection)
        if (
            selected is not None
            and selected.source_start != next_ordered_physical_start(connection)
            and not _historical_frontier_blocks_payload(connection, selected)
        ):
            raise ready.ReadyBundleError("ordered physical frontier changed before reservation")
        ordering = cast(dict[str, object], descriptor["ordering"])
        if selected is None or (
            selected.source_id,
            selected.source_sha256,
            selected.byte_count,
            selected.source_start,
            selected.read_begin,
            selected.read_count,
            selected.predecessor.artifact_id,
            selected.predecessor.source_id,
            selected.predecessor.source_sha256,
            selected.predecessor.source_start,
        ) != (
            descriptor["source_id"],
            descriptor["source_sha256"],
            descriptor["source_bytes"],
            ordering["source_start"],
            ordering["read_begin"],
            ordering["read_count"],
            ordering["predecessor_artifact"],
            ordering["predecessor_source_id"],
            ordering["predecessor_sha256"],
            ordering["predecessor_start"],
        ):
            raise ready.ReadyBundleError("ordered physical source is not the next eligible payload")
    else:
        anchor = cast(dict[str, object], descriptor["anchor"])
        repeat = ready._sql_one(
            connection.execute(
                "SELECT s.committed_sha256,h.prior_sha256,h.prior_lifecycle,s.committed_count,h.prior_count,"
                "s.lifecycle,h.disposition "
                "FROM capture_source AS s "
                "JOIN capture_historical_custody AS h ON h.source_id=s.source_id WHERE s.source_id=?",
                (anchor["repeat_source_id"],),
            )
        )
        if (
            repeat is None
            or repeat[:3] != (anchor["repeat_source_sha256"], anchor["repeat_source_sha256"], "sealed_waiting")
            or repeat[3] != repeat[4]
            or repeat[5:] != ("sealed_waiting", "pending")
        ):
            raise ready.ReadyBundleError("physical anchor source lacks its sealed prior custody")
        frontier = state.get(StateKind.PUBLICATION_V2_FRONTIER, "current")
        if frontier is None or ready._v2_frontier_from_payload(frontier).sequence + 1 != decode_uint64(
            cast(bytes, source[3])
        ):
            raise ready.ReadyBundleError("earlier audio must be published before physical recovery")
    active = ready._sql_one(
        connection.execute(
            "SELECT (SELECT count(*) FROM ready_output_stage)+(SELECT count(*) FROM ready_v2_output_stage)"
            "+(SELECT count(*) FROM publication_cohort WHERE lifecycle IN ('accepted','staging'))"
        )
    )
    if active != (0,):
        raise ready.ReadyBundleError("another publication stage or cohort is active")


def reserve_physical_stage(
    store: InternalAudioStore,
    root: Path,
    descriptor: dict[str, object],
    prepared: PreparedPhysicalAudio,
    proof: ready._V2StageProof,
) -> PhysicalStage:
    """Atomically reserve an order, evidence pages, and stage ownership."""
    if proof.descriptor != ready._canonical(descriptor) or prepared.record_projection_sha256 != descriptor.get(
        "record_projection_sha256"
    ):
        raise ready.ReadyBundleError("physical publication proof differs from its descriptor")
    ready._require_v2_space(
        store, root, (cast(int, descriptor["record_count"]), len(proof.descriptor), 1, prepared.audio.byte_count)
    )
    config_sha = ready._v2_config_sha256()
    with store.publication_transaction() as (state, connection):
        _require_physical_admission(state, connection, descriptor)
        contract = ready._parse_publication_contract(state.get(StateKind.PUBLICATION_CONTRACT, "current"))
        if contract.next_public_order == ready._UINT64_MAX:
            raise ready.ReadyBundleError("v2 publication order is exhausted")
        order = contract.next_public_order
        nonce = os.urandom(32)
        artifact_id = derive_artifact_id(
            stage_nonce=nonce, order=order, config_sha256=config_sha, descriptor_sha256=proof.descriptor_sha256
        )
        identity = f"{proof.descriptor_sha256}:0:{artifact_id}"
        stage_id = sha256(identity.encode()).hexdigest()
        path = root / f".{artifact_id}.{stage_id[:16]}.stage"
        manifest = canonical_manifest(
            artifact_id=artifact_id, order=order, audio_sha256=prepared.audio.sha256, projection=prepared.projection
        )
        manifest_sha = sha256(manifest).hexdigest()
        rows = ready._sql_all(
            connection.execute(
                "SELECT id,payload FROM state WHERE kind=? AND id!='frontier'", (StateKind.READY_PUBLICATION.value,)
            )
        )
        entries = {cast(str, item_id): cast(object, json.loads(cast(str, payload))) for item_id, payload in rows}
        entry = _ledger_entry(descriptor, prepared, proof.records_sha256, order, manifest)
        ready._validate_ledger_entries({**entries, artifact_id: entry})
        connection.execute(
            "INSERT INTO ready_v2_output_stage(stage_id,group_identity,owned_path,stage_nonce,public_order,"
            "artifact_id,config_sha256,descriptor,descriptor_sha256,expected_audio_sha256,expected_manifest_sha256,"
            "expected_physical_count,expected_packet_count,expected_page_count,expected_audio_bytes,pending_overflow_size,owner_token) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                stage_id,
                identity,
                str(path),
                nonce,
                encode_uint64(order),
                artifact_id,
                config_sha,
                proof.descriptor,
                proof.descriptor_sha256,
                prepared.audio.sha256,
                manifest_sha,
                descriptor["record_count"],
                prepared.audio.packet_count,
                prepared.audio.page_count,
                prepared.audio.byte_count,
                None,
                stage_id,
            ),
        )
        content = ready._V2ProofContent(
            artifact_id,
            order,
            prepared.audio.sha256,
            manifest_sha,
            proof.records_sha256,
            cast(int, descriptor["record_count"]),
            prepared.audio.packet_count,
            prepared.record_projection_sha256,
            proof.page_index,
            proof.packed_runs,
            proof.descriptor,
        )
        connection.execute(
            "INSERT INTO ready_replay_proof(bundle_id,lifecycle,stage_id,owner_token,record_count,records_sha256,"
            "manifest_sha256,page_index,normalization_runs,immutable_metadata,proof_content_sha256,format,public_order,"
            "audio_sha256,packet_count) VALUES (?,'staging',?,?,?,?,?,?,?,?,?,'omi-audio-v2',?,?,?)",
            (
                artifact_id,
                stage_id,
                stage_id,
                descriptor["record_count"],
                proof.records_sha256,
                manifest_sha,
                proof.page_index,
                proof.packed_runs,
                proof.descriptor,
                ready._v2_proof_content_sha256(content),
                encode_uint64(order),
                prepared.audio.sha256,
                prepared.audio.packet_count,
            ),
        )
        ready._insert_replay_proof_pages(connection, artifact_id, proof.scratch)
        state.put(StateKind.PHYSICAL_IMPORT, stage_id, {"state": "staging", "stage_identity": identity})
        state.put(
            StateKind.PUBLICATION_CONTRACT,
            "current",
            ready._publication_contract_payload(ready.PublicationContractState(ContractMode.V2_OPEN, order + 1)),
        )
        state.mark_publication_changed()
    return PhysicalStage(stage_id, artifact_id, order, path, manifest)


def _ledger_entry(
    descriptor: dict[str, object], prepared: PreparedPhysicalAudio, records_sha: str, order: int, manifest: bytes
) -> dict[str, object]:
    return {
        "format": "omi-audio-v2",
        "state": "ready",
        "order": order,
        "audio_sha256": prepared.audio.sha256,
        "audio_bytes": prepared.audio.byte_count,
        "manifest_sha256": sha256(manifest).hexdigest(),
        "manifest_bytes": len(manifest),
        "packet_count": prepared.audio.packet_count,
        "records_sha256": records_sha,
        "lineage_kind": "physical_byte_span",
        **{
            key: descriptor[key]
            for key in ("source_id", "source_sha256", "source_bytes", "spans", "seams", "record_count")
        },
    }


def publish_physical_stage(
    store: InternalAudioStore,
    stage: PhysicalStage,
    source: PhysicalStageInput,
) -> ready.ReadyBundleResult:
    """Write and activate a previously reserved, authenticated physical stage."""
    entry = _ledger_entry(source.descriptor, source.prepared, source.records_sha, stage.order, stage.manifest)
    if not ready._valid_ledger_entry(stage.artifact_id, entry):
        raise ready.ReadyBundleError("physical READY ledger entry is invalid")
    _require_physical_staging_proof(store, stage, source)
    destination = ready._materialize_v2_files(
        stage.path,
        stage.artifact_id,
        lambda path, require_name: _validate_files(path, require_name, stage, source.prepared),
        lambda: _write_files(stage, source),
    )
    _commit_physical_stage(store, stage, source, entry)
    return ready.ReadyBundleResult(
        stage.artifact_id, destination, None, cast(int, source.descriptor["record_count"]), source.records_sha
    )


def _require_physical_staging_proof(
    store: InternalAudioStore, stage: PhysicalStage, source: PhysicalStageInput
) -> None:
    with closing(store.state_db.open_connection(read_only=True)) as connection, store.state_db.access(connection):
        row = ready._sql_one(
            connection.execute(
                "SELECT records_sha256,page_index,immutable_metadata,normalization_runs,proof_content_sha256,"
                "record_count,packet_count,manifest_sha256,audio_sha256,public_order FROM ready_replay_proof "
                "WHERE bundle_id=? AND lifecycle='staging' AND stage_id=? AND owner_token=?",
                (stage.artifact_id, stage.stage_id, stage.stage_id),
            )
        )
        pages = ready._sql_all(
            connection.execute(
                "SELECT page_ordinal,first_record_index,record_count,hashes,page_sha256 FROM ready_replay_proof_page "
                "WHERE bundle_id=? ORDER BY page_ordinal",
                (stage.artifact_id,),
            )
        )
    if row is None or row[0] != source.records_sha or row[2] != ready._canonical(source.descriptor):
        raise ready.ReadyDurabilityFatalError("physical staging proof differs from retained audio")
    try:
        if row[5:] != (
            source.descriptor["record_count"],
            source.prepared.audio.packet_count,
            sha256(stage.manifest).hexdigest(),
            source.prepared.audio.sha256,
            encode_uint64(stage.order),
        ):
            raise ValueError("physical proof metadata changed")
        content = ready._V2ProofContent(
            stage.artifact_id,
            stage.order,
            source.prepared.audio.sha256,
            sha256(stage.manifest).hexdigest(),
            source.records_sha,
            cast(int, row[5]),
            source.prepared.audio.packet_count,
            source.prepared.record_projection_sha256,
            cast(bytes, row[1]),
            cast(bytes, row[3]),
            cast(bytes, row[2]),
        )
        if row[4] != ready._v2_proof_content_sha256(content):
            raise ValueError("physical proof root changed")
        index = ready.unpack_page_index(cast(bytes, row[1]), record_count=cast(int, source.descriptor["record_count"]))
        ready.unpack_normalization_runs(cast(bytes, row[3]), record_count=cast(int, row[5]))
        if len(pages) != len(index):
            raise ValueError("physical proof page count changed")
        ready._validated_v2_stage_hashes(pages, index)
    except (IndexError, TypeError, ValueError) as error:
        raise ready.ReadyDurabilityFatalError("physical staging proof pages are corrupt") from error


def _validate_files(path: Path, require_name: bool, stage: PhysicalStage, prepared: PreparedPhysicalAudio) -> None:
    if (require_name and path.name != stage.artifact_id) or path.is_symlink() or not path.is_dir():
        raise ready.ReadyBundleError("physical READY directory is invalid")
    if {item.name for item in path.iterdir()} != {"audio.ogg", "manifest.json"}:
        raise ready.ReadyBundleError("physical READY inventory is invalid")
    if any(item.is_symlink() or not item.is_file() for item in (path / "audio.ogg", path / "manifest.json")):
        raise ready.ReadyBundleError("physical READY artifact is unsafe")
    ready._require_v2_operator_access(path)
    if (
        ready._digest(path / "audio.ogg") != prepared.audio.sha256
        or ready._digest(path / "manifest.json") != sha256(stage.manifest).hexdigest()
    ):
        raise ready.ReadyBundleError("physical READY files changed")


def _write_files(stage: PhysicalStage, source: PhysicalStageInput) -> None:
    def packets() -> Iterator[bytes]:
        for record in iter_physical_audio(source.descriptor, source.read_span, source.source_identity):
            yield from record.packets

    stage.path.mkdir(mode=ready._READY_DIRECTORY_CREATE_MODE)
    ready._require_operator_ready_directory(stage.path)
    audio = stage.path / "audio.ogg"
    write_v2_audio(audio, packets(), source.prepared.audio)
    audio.chmod(ready._READY_FILE_MODE)
    ready._sync_regular_file(audio)
    ready._write_file(stage.path / "manifest.json", stage.manifest)
    ready._sync_directory(stage.path)


def _commit_physical_stage(
    store: InternalAudioStore, stage: PhysicalStage, source_input: PhysicalStageInput, entry: dict[str, object]
) -> None:
    descriptor, prepared = source_input.descriptor, source_input.prepared
    with store.publication_transaction() as (state, connection):
        marker = state.get(StateKind.PHYSICAL_IMPORT, stage.stage_id)
        stage_row = ready._sql_one(
            connection.execute(
                "SELECT group_identity,artifact_id,owner_token,descriptor,expected_audio_sha256,expected_manifest_sha256 "
                "FROM ready_v2_output_stage WHERE stage_id=?",
                (stage.stage_id,),
            )
        )
        if marker is None or stage_row is None or marker != {"state": "staging", "stage_identity": stage_row[0]}:
            raise ready.ReadyBundleError("physical stage ownership changed")
        if tuple(stage_row)[1:] != (
            stage.artifact_id,
            stage.stage_id,
            ready._canonical(descriptor),
            prepared.audio.sha256,
            sha256(stage.manifest).hexdigest(),
        ):
            raise ready.ReadyBundleError("physical stage owner conflicts")
        source = ready._sql_one(
            connection.execute(
                "SELECT s.committed_count,s.committed_sha256,h.disposition FROM capture_source AS s "
                "JOIN capture_historical_custody AS h ON h.source_id=s.source_id WHERE s.source_id=?",
                (descriptor["source_id"],),
            )
        )
        if source is None or tuple(source) != (
            cast(int, descriptor["source_bytes"]) // 444,
            descriptor["source_sha256"],
            "pending",
        ):
            raise ready.ReadyBundleError("physical source custody changed before publication")
        rows = ready._sql_all(
            connection.execute(
                "SELECT id,payload FROM state WHERE kind=? AND id!='frontier'", (StateKind.READY_PUBLICATION.value,)
            )
        )
        entries = {cast(str, item_id): cast(object, json.loads(cast(str, payload))) for item_id, payload in rows}
        ready._validate_ledger_entries({**entries, stage.artifact_id: entry})
        proof_row = ready._sql_one(
            connection.execute(
                "SELECT records_sha256,manifest_sha256,immutable_metadata,audio_sha256,packet_count "
                "FROM ready_replay_proof WHERE bundle_id=? AND lifecycle='staging' AND stage_id=? AND owner_token=?",
                (stage.artifact_id, stage.stage_id, stage.stage_id),
            )
        )
        if proof_row is None or tuple(proof_row) != (
            source_input.records_sha,
            sha256(stage.manifest).hexdigest(),
            ready._canonical(descriptor),
            prepared.audio.sha256,
            prepared.audio.packet_count,
        ):
            raise ready.ReadyBundleError("physical replay proof differs from stage")
        changed = connection.execute(
            "UPDATE ready_replay_proof SET lifecycle='active' WHERE bundle_id=? AND lifecycle='staging' "
            "AND stage_id=? AND owner_token=?",
            (stage.artifact_id, stage.stage_id, stage.stage_id),
        )
        if changed.rowcount != 1:
            raise ready.ReadyBundleError("physical replay proof ownership changed")
        active_proof = ready._sql_one(
            connection.execute(
                "SELECT bundle_id,lifecycle,stage_id,owner_token,record_count,records_sha256,manifest_sha256,"
                "page_index,normalization_runs,immutable_metadata,proof_content_sha256,public_order,audio_sha256,"
                "packet_count FROM ready_replay_proof WHERE bundle_id=?",
                (stage.artifact_id,),
            )
        )
        if active_proof is None:
            raise ready.ReadyBundleError("physical replay proof disappeared")
        ready._v2_active_proof(tuple(active_proof), stage.artifact_id, entry)
        state.put(StateKind.READY_PUBLICATION, stage.artifact_id, entry)
        linkage = (
            {"anchor": descriptor["anchor"]}
            if "anchor" in descriptor
            else {"ordering": descriptor["ordering"], "overlap": descriptor["overlap"]}
        )
        state.put(
            StateKind.PHYSICAL_IMPORT,
            stage.stage_id,
            {
                "state": "published",
                "artifact_id": stage.artifact_id,
                "source_id": descriptor["source_id"],
                **linkage,
                "record_projection_sha256": descriptor["record_projection_sha256"],
                "descriptor_sha256": sha256(ready._canonical(descriptor)).hexdigest(),
            },
        )
        connection.execute("DELETE FROM ready_v2_output_stage WHERE stage_id=?", (stage.stage_id,))
        state.mark_publication_changed()
