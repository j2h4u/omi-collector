from __future__ import annotations

import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from typing import cast

import pytest

from omi_collector.capture.adapters import ready_bundles
from omi_collector.capture.adapters.collector_state_db import StateDBError, StateKind
from omi_collector.capture.adapters.internal_audio_store import InternalAudioStore
from omi_collector.capture.application.storage_fault import FatalInternalAudioStorageError
from omi_collector.capture.domain.publication_recovery import (
    CohortFact,
    CommitEvidence,
    CommitExpectation,
    MemberCursor,
    ProofFact,
    SourceExpectation,
    StageCustody,
    classify_commit,
)


def _expectation(*, complete: bool = False, old_frontier: object = None) -> CommitExpectation:
    return CommitExpectation(
        entry={"artifact": "a"},
        old_frontier=old_frontier,
        proposed_frontier={"next": 20},
        old_cohort_cursor=3,
        complete=complete,
        old_members=(MemberCursor(0, 2),),
        proposed_members=(MemberCursor(0, 3),),
        stage_id="stage",
        owner="owner",
        sources=(SourceExpectation(("sealed", 2), ("covered", 2)),),
    )


def _proposed(*, complete: bool = False) -> CommitEvidence:
    return CommitEvidence(
        entry={"artifact": "a"},
        frontier={"next": 20},
        cohort=None if complete else CohortFact("accepted", 4),
        members=(MemberCursor(0, 99),) if complete else (MemberCursor(0, 3),),
        proof=ProofFact("active", "stage", "owner"),
        stage=StageCustody.ABSENT,
        sources=(("covered", 2),),
    )


def _old(frontier: object = None) -> CommitEvidence:
    return CommitEvidence(
        entry=None,
        frontier=frontier,
        cohort=CohortFact("staging", 3),
        members=(MemberCursor(0, 2),),
        proof=ProofFact("staging", "stage", "owner"),
        stage=StageCustody.OWNED,
        sources=(("sealed", 2),),
    )


def test_commit_classifier_accepts_complete_and_continuing_proposals() -> None:
    assert classify_commit(_proposed(complete=True), _expectation(complete=True)) == "proposed"
    assert classify_commit(_proposed(), _expectation()) == "proposed"


@pytest.mark.parametrize("frontier", [None, {"next": 9}])
def test_commit_classifier_accepts_exact_old_frontier(frontier: object) -> None:
    assert classify_commit(_old(frontier), _expectation(old_frontier=frontier)) == "old"


@pytest.mark.parametrize(
    "changes",
    [
        {"entry": None},
        {"frontier": {"next": 19}},
        {"cohort": CohortFact("accepted", 3)},
        {"members": (MemberCursor(0, 2),)},
        {"proof": ProofFact("staging", "stage", "owner")},
        {"stage": StageCustody.CONFLICTING},
        {"sources": (("sealed", 2),)},
    ],
    ids=("entry", "frontier", "cohort", "members", "proof", "foreign-stage", "source"),
)
def test_commit_classifier_rejects_each_mismatched_proposed_witness(changes: dict[str, object]) -> None:
    assert classify_commit(replace(_proposed(), **changes), _expectation()) == "third"


@pytest.mark.parametrize(
    "changes",
    [
        {"frontier": {"next": 8}},
        {"cohort": CohortFact("accepted", 3)},
        {"members": (MemberCursor(0, 1),)},
        {"proof": ProofFact("active", "stage", "owner")},
        {"stage": StageCustody.CONFLICTING},
        {"sources": (("covered", 2),)},
    ],
    ids=("frontier", "cohort", "members", "proof", "foreign-stage", "source"),
)
def test_commit_classifier_rejects_each_mismatched_old_witness(changes: dict[str, object]) -> None:
    assert classify_commit(replace(_old(), **changes), _expectation()) == "third"


def test_commit_classifier_preserves_empty_source_vacuity() -> None:
    assert classify_commit(replace(_proposed(), sources=()), replace(_expectation(), sources=())) == "proposed"
    assert classify_commit(replace(_old(), sources=()), replace(_expectation(), sources=())) == "old"


def test_commit_classifier_requires_equal_source_counts() -> None:
    with pytest.raises(ValueError):
        classify_commit(_proposed(), replace(_expectation(), sources=()))


class _Cursor:
    def __init__(self, row: tuple[object, ...] | None = None, rows: tuple[tuple[object, ...], ...] = ()) -> None:
        self._row = row
        self._rows = rows

    def fetchone(self) -> tuple[object, ...] | None:
        return self._row

    def fetchall(self) -> tuple[tuple[object, ...], ...]:
        return self._rows

    def close(self) -> None:
        pass


class _Connection:
    def __init__(self, calls: list[str], responses: tuple[object, ...]) -> None:
        self.calls = calls
        self.responses = iter(responses)

    def execute(self, sql: str, _parameters: object = ()) -> _Cursor:
        self.calls.append(sql.split(" FROM ")[0].removeprefix("SELECT "))
        response = next(self.responses)
        if isinstance(response, BaseException):
            raise response
        if sql.startswith("SELECT member_ordinal"):
            return _Cursor(rows=cast(tuple[tuple[object, ...], ...], response))
        return _Cursor(cast(tuple[object, ...] | None, response))

    def close(self) -> None:
        pass


class _StateDB:
    def __init__(self, calls: list[str], responses: tuple[object, ...], failure: BaseException | None = None) -> None:
        self.calls = calls
        self.responses = responses
        self.failure = failure

    def get(self, kind: StateKind, _key: str) -> object:
        self.calls.append(f"get:{kind.value}")
        if self.failure is not None:
            raise self.failure
        return {"artifact": "a"} if kind is StateKind.READY_PUBLICATION else "final"

    def open_connection(self, *, read_only: bool) -> _Connection:
        assert read_only
        self.calls.append("open")
        return _Connection(self.calls, self.responses)

    @contextmanager
    def access(self, _connection: object) -> Iterator[None]:
        yield


def _adapter_case(
    monkeypatch: pytest.MonkeyPatch,
    *,
    responses: tuple[object, ...] = (
        ("accepted", 4),
        None,
        ("active", "stage", "owner"),
        ((0, 3),),
    ),
    database_failure: BaseException | None = None,
    source_failure: BaseException | None = None,
) -> tuple[list[str], object]:
    calls: list[str] = []
    state_db = _StateDB(calls, responses, database_failure)
    store = SimpleNamespace(state_db=state_db)
    frontier = object()
    initial_frontier = object()
    group = SimpleNamespace(final_frontier=frontier, initial_frontier=initial_frontier)
    stage = SimpleNamespace(
        artifact_id="artifact",
        stage_id="stage",
        owner_token="owner",
        group=group,
        group_identity="identity",
        path=Path("/ready/.artifact.stage"),
    )
    progress = (SimpleNamespace(member_ordinal=0, old_cursor=2, new_cursor=3),)
    coverage = (SimpleNamespace(source_id="source", old=("old",), proposed=("new",)),)
    proposal = SimpleNamespace(
        cohort=SimpleNamespace(cohort_id="cohort", cursor=3),
        stage=stage,
        entry={"artifact": "a"},
        progress=progress,
        coverage=coverage,
        all_complete=False,
    )
    monkeypatch.setattr(ready_bundles, "_require_v2_destination", lambda *_args: calls.append("destination"))
    monkeypatch.setattr(ready_bundles, "_v2_frontier_payload", lambda item: "final" if item is frontier else "old")

    def inspect_source(_audio_store: object, _source_id: str) -> tuple[str]:
        calls.append("source")
        if source_failure is not None:
            raise source_failure
        return ("new",)

    monkeypatch.setattr(ready_bundles, "_typed_source_coverage_state", inspect_source)
    return calls, ready_bundles._inspect_v2_commit(
        cast(InternalAudioStore, store), cast(ready_bundles._V2Commit, proposal), Path("/ready/artifact")
    )


def test_adapter_inspects_destination_frontiers_rows_then_sources(monkeypatch: pytest.MonkeyPatch) -> None:
    calls, state = _adapter_case(monkeypatch)
    assert state == "proposed"
    assert calls == [
        "destination",
        f"get:{StateKind.READY_PUBLICATION.value}",
        f"get:{StateKind.PUBLICATION_V2_FRONTIER.value}",
        "open",
        "lifecycle,cursor",
        "group_identity,owned_path,owner_token,failure_reason",
        "lifecycle,stage_id,owner_token",
        "member_ordinal,cursor",
        "source",
    ]


def test_adapter_caught_inspection_failure_classifies_as_third(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[str] = []
    store = SimpleNamespace(state_db=_StateDB(calls, (), StateDBError("injected")))
    monkeypatch.setattr(ready_bundles, "_require_v2_destination", lambda *_args: None)
    proposal = SimpleNamespace(cohort=None, stage=SimpleNamespace(artifact_id="a"))
    assert (
        ready_bundles._inspect_v2_commit(
            cast(InternalAudioStore, store), cast(ready_bundles._V2Commit, proposal), Path("/ready/a")
        )
        == "third"
    )


def test_adapter_keeps_corrupt_database_failure_fatal(monkeypatch: pytest.MonkeyPatch) -> None:
    failure = sqlite3.DatabaseError("malformed")
    failure.sqlite_errorcode = sqlite3.SQLITE_CORRUPT
    calls: list[str] = []
    store = SimpleNamespace(state_db=_StateDB(calls, (), failure))
    monkeypatch.setattr(ready_bundles, "_require_v2_destination", lambda *_args: None)
    proposal = SimpleNamespace(cohort=None, stage=SimpleNamespace(artifact_id="a"))
    with pytest.raises(FatalInternalAudioStorageError, match="database is corrupt"):
        ready_bundles._inspect_v2_commit(
            cast(InternalAudioStore, store), cast(ready_bundles._V2Commit, proposal), Path("/ready/a")
        )


def test_source_coverage_inspection_failure_remains_outside_catch(monkeypatch: pytest.MonkeyPatch) -> None:
    failure = StateDBError("source query failed")
    with pytest.raises(StateDBError, match="source query failed"):
        _adapter_case(monkeypatch, source_failure=failure)
