# Writer State Machine Plan

## Decision

Replace `AttemptWriter`'s interacting lifecycle flags with one small immutable
state machine. Keep its queue, memory arena, writer thread, target calls,
timeouts, and public API in the existing driver. Reuse the shape and vocabulary
of the presence machine, but add no generic state-machine framework or library.

The purpose is narrow: make contradictory writer states unrepresentable and
make completion after cancellation reflect what the writer thread actually did.
This change must not alter BLE, cursor, persistence, recovery, or publication
semantics.

## Boundary

The machine owns only `AttemptWriter` lifecycle decisions. `StagingWriter` and
`StagedAttempt` remain synchronous, thread-affine effect targets. Locks,
futures, commands, byte counters, wakeups, deadlines, and I/O remain outside
the pure model.

## State algebra

Every state is a frozen, slotted dataclass in `attempt_writer_machine.py`.

| State | Meaning |
| --- | --- |
| `Constructed` | no prepare command has been admitted |
| `Preparing` | prepare is admitted and its result is pending |
| `Prepared` | target exists; no successful `READ_BEGIN` is active |
| `Reading` | data publication, checkpoint, and finalization are permitted |
| `Finalizing` | seal or prefix publication is admitted and exclusive |
| `Finalized` | seal or prefix publication completed successfully |
| `Failed` | the first target failure is latched; only close remains legal |
| `Closing` | one shared close is admitted; retains failure and whether close must drain |
| `Closed` | terminal state; retains the first failure for diagnostics |

Seal and durable-prefix publication deliberately share bare `Finalizing` and
`Finalized` states because no lifecycle decision depends on which one ran.
`Closing` retains the current failure and a drain decision so close queued
behind a successful `READ_BEGIN` cannot silently drop already accepted bytes.

## Events and directives

Requests are start, prepare leg, read begin, checkpoint, finalize, and close;
data publication has a pure admission rule over the same state.
Worker results are prepare success, leg success, read-begin success, checkpoint
success, finalize success, command failure, close success, and close failure.

One pure transition function returns a new state and one directive: enqueue an
existing writer command, reject with an existing public error category, reuse
the pending close, or do nothing. It never mutates input, touches I/O, reads a
clock, or manages a future.

## Required transitions

- Start admits exactly one prepare and is idempotent after admission.
- Prepare success enters `Prepared`; prepare failure enters `Failed`.
- Preparing a leg is legal only before finalization and returns to `Prepared`,
  so data cannot drain until a subsequent successful `READ_BEGIN`. This is an
  intentional tightening: today the outer writer can drain while its target
  has already closed that gate, turning an early chunk into a latched failure.
- `READ_BEGIN` success enters `Reading`; checkpoint and finalization require
  `Reading`.
- Finalization becomes exclusive before its command is queued. Competing data,
  controls, and finalization are rejected while it is pending or complete.
- Queue-capacity rejection leaves the state unchanged.
- The first target failure is retained, queued non-close commands fail, and one
  close remains executable.
- Close may begin from every nonterminal state, is idempotent while pending,
  and ends in `Closed` even when target close reports a failure.
- Events after `Closed` cannot reopen the writer.
- Data publication remains legal before start and before `READ_BEGIN`; it only
  advances the in-memory high-water mark and cannot drain until `Reading`.
- Controls admitted while finalization is pending or complete are rejected
  before they can poison an already sealed target. Failed-state rejection keeps
  existing error chaining and a failed-state `READ_BEGIN` returns an
  asynchronously failed future. Pre-existing closed/queue-full callback escapes
  remain outside this lifecycle slice.

## Effect and cancellation ordering

Request validation, queue-capacity validation, state transition, and queue
insertion occur atomically under the existing lock. Capacity is checked before
the transition is committed, so rejection needs no rollback. Worker-result
events are applied only after the real target call returns. FIFO controls,
coalesced byte high-water publication, chunking, and draining to the command's
captured high-water mark remain unchanged.

When close is admitted, its state records whether data was already drainable;
a successful earlier `READ_BEGIN` queued ahead of close enables that drain.
Failure disables it. Cancellation never rolls back an admitted operation. The caller may stop
waiting, while the worker thread later records the target's real success or
failure. Close keeps its existing bounded, shielded wait and thread-join
contract. A failed writer skips further data drain but still closes its target.

## Final review resolution

The final review findings within this slice are resolved by an explicit
`CheckpointSucceeded` result, total writer-command result mapping, and
first-failure-preserving failure transitions in every machine state. The driver
also turns an impossible unlatchable failure result into a latched failure so a
worker future is completed rather than abandoned. Closed or queue-full
`READ_BEGIN` callback escapes predate this slice and are documented here as
outside its scope, not claimed as resolved.

## Delivery checklist

- [x] Add characterization tests for current public ordering, errors, start
  idempotence after admission, pre-admission close rejection, queue bounds,
  snapshots, failure, and close behavior.
- [x] Add the immutable state/event/directive model and exhaustive pure
  transition tests.
- [x] Replace lifecycle flags in `AttemptWriter` with one machine state without
  changing queue, data, thread, or target code.
- [x] Drive state changes from actual worker results and make cancellation
  converge on those results.
- [x] Cover close queued behind `READ_BEGIN`, pre-start data publication,
  asynchronous failed `READ_BEGIN`, and the intentional early rejections around
  finalization.
- [x] Verify staging, reconciliation, byte ordering, durability, and existing
  public behavior with focused tests.
- [x] Run `just check`, `just crap-check`, `just unit`, and `just docker-build`.
- [x] Run an independent adversarial review against this plan and implementation
  and resolve material findings.

## Acceptance and stopping rule

The slice is complete when lifecycle state is exactly one tagged immutable
value, invalid combinations cannot be constructed through the public driver,
an admitted operation always converges to its real worker result, and all
repository gates pass without externally observable capture changes.

Stop there. Do not refactor `StagingWriter`, `StagedAttempt`, batch recovery,
BLE, persistence formats, telemetry, or other lifecycle candidates. Do not add
a reusable FSM framework, migrations, compatibility layers, or dependencies.
