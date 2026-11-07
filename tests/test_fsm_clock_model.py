"""Generated clock-write contract against completion and serialized recovery.

Run: uv run --no-sync pytest -q tests/test_fsm_clock_model.py --hypothesis-show-statistics
State is a prepared, uncertain, or completed write with integer boundaries b,e;
initially prepared. Events mark uncertain, finish, and reload. A completed write
must preserve the causal boundary, including b=e. Each edge entering, leaving,
or inside [b,e) is incomparable regardless of raw clock labels. The independent
oracle is this interval rule, not the runtime transition table. Examples reset
all state. Bounds: b=1..5, length=0..3 and labels=0..10; serialization models
restart after committed completion, not crashes during writes or old descriptors.
Progress requires successful persistence and a known end boundary. No concurrency
or equivalence proof is claimed. Invalid transitions are covered by clock tests.
"""

from hypothesis import event, given, settings
from hypothesis import strategies as st

from omi_collector.capture.adapters.public_time_facts import FrozenTimePredecessor, PublicTimeProjection
from omi_collector.capture.domain.clock_state import ClockIntent, ClockRange, ClockState


@settings(max_examples=40, deadline=None)
@given(
    start=st.integers(min_value=1, max_value=5),
    length=st.integers(min_value=0, max_value=3),
    labels=st.lists(st.integers(min_value=0, max_value=10), min_size=6, max_size=6),
)
def test_generated_completed_clock_boundary_survives_reload(start: int, length: int, labels: list[int]) -> None:
    end = start + length
    intent = ClockIntent("write", "prepared", 0, 0, 0.0, start)
    state = ClockState().begin_intent(intent).mark_uncertain("write", write_sequence_after=end)
    ranges = (ClockRange(start, end, None, None, "unknown", "write"),) if length else ()
    state = ClockState.from_payload(state.finish_intent("write", ranges).to_payload())
    projection = PublicTimeProjection.from_state(state)
    previous = projection.freeze(sequence=start - 1, raw_timestamp=labels[0])
    for sequence in range(start, end + 2):
        current = projection.freeze(
            sequence=sequence,
            raw_timestamp=labels[sequence - start + 1],
            predecessor=FrozenTimePredecessor(sequence - 1, previous),
        )
        if sequence <= end:
            assert current.comparable_to_previous is False
        event(f"length={length},edge={'boundary' if sequence <= end else 'outside'}")
        previous = current


@settings(max_examples=50, deadline=None)
@given(
    actions=st.lists(st.sampled_from(("reload", "mark", "finish", "wrong_id")), min_size=1, max_size=15),
    length=st.integers(min_value=0, max_value=3),
)
def test_generated_clock_transitions_and_positive_comparability(actions: list[str], length: int) -> None:
    import pytest

    from omi_collector.capture.domain.clock_state import ClockStateError

    start, end = 3, 3 + length
    ordinary = ClockRange(0, 12, None, None, "unknown", "ordinary")
    intent = ClockIntent("write", "prepared", 0, 0, 0.0, start)
    state = ClockState((ordinary,)).begin_intent(intent)
    phase = "prepared"
    ranges = (ClockRange(start, end, None, None, "unknown", "write"),) if length else ()
    for action in actions:
        previous_phase = phase
        if action == "reload":
            state = ClockState.from_payload(state.to_payload())
        elif action == "wrong_id":
            with pytest.raises(ClockStateError):
                state.mark_uncertain("other", write_sequence_after=end)
        elif action == "mark":
            if phase == "completed":
                with pytest.raises(ClockStateError):
                    state.mark_uncertain("write", write_sequence_after=end)
            else:
                state = state.mark_uncertain("write", write_sequence_after=end)
                phase = "uncertain"
        elif phase == "uncertain":
            state = state.finish_intent("write", ranges)
            phase = "completed"
        else:
            with pytest.raises(ClockStateError):
                state.finish_intent("write", ranges)
        event(f"{previous_phase}:{action}:{phase}")
    if phase == "prepared":
        state = state.mark_uncertain("write", write_sequence_after=end)
        phase = "uncertain"
    if phase == "uncertain":
        state = state.finish_intent("write", ranges)
    state = ClockState.from_payload(state.to_payload())
    projection = PublicTimeProjection.from_state(state)
    for sequence in range(1, 10):
        before = projection.freeze(sequence=sequence - 1, raw_timestamp=100)
        after = projection.freeze(
            sequence=sequence, raw_timestamp=100, predecessor=FrozenTimePredecessor(sequence - 1, before)
        )
        assert after.comparable_to_previous is (not (start <= sequence <= end))
