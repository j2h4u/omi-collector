"""Generated retry contract against the publication reducer.

Run: uv run --no-sync pytest -q tests/test_fsm_publication_model.py --hypothesis-show-statistics
State is running or retry-wait, beginning with a fresh wake. A transient result
records its post-effect revision; before the deadline the same revision cannot
start again. At the deadline retry is permitted and repeated failures use the
next delay, capped at the final configured delay. A successful finish settles.
Each generated example resets the reducer, and a fair suffix supplies eventual
success. This checks bounded reducer behavior, not I/O, concurrent revision
changes, custody, crash recovery, cancellation, or equivalence to the model.
"""

from hypothesis import event, given, settings
from hypothesis import strategies as st

from omi_collector.capture.domain.ready_machine import (
    Finished,
    PublicationAction,
    PublicationResult,
    PublicationState,
    Wake,
    publication_transition,
)


@settings(max_examples=20, deadline=None)
@given(failures=st.integers(min_value=1, max_value=8))
def test_generated_self_revision_retry_backoff_and_eventual_settlement(failures: int) -> None:
    state = PublicationState()
    now = 0.0
    revision = 0
    state, command = publication_transition(state, Wake(revision, now))
    for index in range(failures):
        assert command.action is PublicationAction.RUN
        assert command.token is not None
        revision += 1
        state, command = publication_transition(
            state, Finished(command.token, revision, None, PublicationResult.TRANSIENT, now, (1.0, 2.0, 4.0))
        )
        delay = (1.0, 2.0, 4.0)[min(index, 2)]
        assert command.action is PublicationAction.ARM
        assert command.deadline == now + delay
        state, early = publication_transition(state, Wake(revision, now))
        assert early.action is PublicationAction.ARM
        assert early.deadline == now + delay
        now += delay
        state, command = publication_transition(state, Wake(revision, now))
        event(f"failure-count={min(index + 1, 3)}")
    assert command.token is not None
    state, command = publication_transition(
        state, Finished(command.token, revision, None, PublicationResult.SETTLED, now)
    )
    state, command = publication_transition(state, Wake(revision, now))
    assert command.action is PublicationAction.NONE


@settings(max_examples=60, deadline=None)
@given(
    st.lists(
        st.sampled_from(("wake", "change", "capture", "release", "success", "fail", "stale")), min_size=1, max_size=20
    )
)
def test_generated_publication_ownership_and_fair_suffix(actions: list[str]) -> None:
    from omi_collector.capture.domain.ready_machine import (
        CaptureBegin,
        CaptureEnd,
        InputChanged,
        PublicationMode,
        Quiesced,
        Running,
    )

    state = PublicationState()
    revision = 0
    capturing = False
    for index, action in enumerate(actions):
        now = float(index * 10)
        before = state
        if action == "wake":
            state, command = publication_transition(state, Wake(revision, now))
            if before.mode is PublicationMode.CAPTURE:
                assert command.action is not PublicationAction.RUN
        elif action == "change":
            revision += 1
            state, _ = publication_transition(state, InputChanged())
        elif action == "capture" and not capturing:
            state, command = publication_transition(state, CaptureBegin())
            capturing = True
            assert command.action is PublicationAction.CANCEL_AND_JOIN
        elif action == "release" and capturing:
            state, _ = publication_transition(state, Quiesced())
            state, _ = publication_transition(state, CaptureEnd())
            capturing = False
        elif action in {"success", "fail"} and isinstance(state.work, Running):
            work = state.work
            result = PublicationResult.SETTLED if action == "success" else PublicationResult.TRANSIENT
            state, _ = publication_transition(state, Finished(work.token, revision, None, result, now, (1.0, 2.0)))
        elif action == "stale":
            state, command = publication_transition(state, Finished(-1, revision, None, PublicationResult.SETTLED, now))
            assert state == before
            assert command.action is PublicationAction.NONE
        assert (state.mode is PublicationMode.CAPTURE) == capturing
        event(f"{before.mode.value}:{type(before.work).__name__}:{action}:{type(state.work).__name__}")
    # Release capture, invalidate stale work and deliver a successful effect.
    if state.mode is PublicationMode.CAPTURE:
        state, _ = publication_transition(state, Quiesced())
        state, _ = publication_transition(state, CaptureEnd())
    state, _ = publication_transition(state, Quiesced())
    state, _ = publication_transition(state, InputChanged())
    state, command = publication_transition(state, Wake(revision, 1000.0))
    assert command.action is PublicationAction.RUN
    assert command.token is not None
    state, _ = publication_transition(state, Finished(command.token, revision, None, PublicationResult.SETTLED, 1000.0))
    _, command = publication_transition(state, Wake(revision, 1000.0))
    assert command.action is PublicationAction.NONE


@settings(max_examples=10, deadline=None)
@given(gap=st.integers(min_value=2, max_value=5), late_offset=st.integers(min_value=1, max_value=4))
def test_generated_frontier_preserves_late_custody(gap: int, late_offset: int) -> None:
    """A unique late source remains sealed; exact old replay consumes no order."""
    from pathlib import Path
    from tempfile import TemporaryDirectory

    import pytest

    from omi_collector.capture.adapters.collector_state_db import StateDB
    from omi_collector.capture.adapters.internal_audio_store import InternalAudioStore
    from omi_collector.capture.adapters.ready_bundles import (
        ReadyBundleError,
        finalize_public_audio_v2,
        publication_contract_state,
    )
    from omi_collector.capture.domain.clock_state import ClockState
    from test_public_audio_v2_integration import _options, _ready_root, _record, _source

    late_offset = min(late_offset, gap - 1)
    with TemporaryDirectory(prefix="omi-frontier-model-") as temporary:
        root = Path(temporary)
        database = StateDB(root / "state.sqlite3")
        store = InternalAudioStore(database)
        try:
            ready = _ready_root(root)
            first, last = 10, 10 + gap
            first_record = _record(100)
            finalize_public_audio_v2(
                store, (_source(store, "a" * 32, first, (first_record,)),), ready, _options(ClockState(), first + 1)
            )
            finalize_public_audio_v2(
                store, (_source(store, "b" * 32, last, (_record(110),)),), ready, _options(ClockState(), last + 1)
            )
            before = publication_contract_state(database).next_public_order
            replay = _source(store, "c" * 32, first, (first_record,))
            assert not finalize_public_audio_v2(store, (replay,), ready, _options(ClockState(), last + 1)).published
            late = _source(store, "d" * 32, first + late_offset, (_record(105),))
            with pytest.raises(ReadyBundleError, match="committed v2 frontier"):
                finalize_public_audio_v2(store, (late,), ready, _options(ClockState(), last + 1))
            assert publication_contract_state(database).next_public_order == before
            source = store.get_source("d" * 32)
            assert source is not None and source.lifecycle == "sealed_waiting"
            event(f"gap={gap},offset={late_offset}")
        finally:
            store.close()


def test_publication_terminal_and_capture_guards() -> None:
    from typing import cast

    import pytest

    from omi_collector.capture.domain.ready_machine import (
        CaptureBegin,
        CaptureEnd,
        PublicationEvent,
        PublicationMode,
        Shutdown,
    )

    state, _ = publication_transition(PublicationState(), CaptureBegin())
    with pytest.raises(ValueError):
        publication_transition(state, CaptureBegin())
    state, _ = publication_transition(state, Shutdown())
    assert state.mode is PublicationMode.CLOSED
    for attempted in (Wake("revision", 100.0), CaptureEnd()):
        state, command = publication_transition(state, attempted)
        assert state.mode is PublicationMode.CLOSED
        assert command.action is not PublicationAction.RUN
    with pytest.raises(ValueError):
        publication_transition(state, CaptureBegin())
    with pytest.raises(ValueError):
        publication_transition(state, cast(PublicationEvent, object()))
