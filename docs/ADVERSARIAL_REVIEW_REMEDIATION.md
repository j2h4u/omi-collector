# Adversarial review remediation plan

This document is the ordered implementation backlog produced from the full
adversarial review of the collector. It is intentionally a checklist: assign a
small coherent group to an implementation agent, have a different agent review
the result, and check the two boxes independently.

The ordering is significant. Later waves assume the ownership, recovery, and
publication contracts established by earlier waves. Do not combine unrelated
waves merely to reduce the number of pull requests.

## Completion rules

For every item:

- `[ ] Implemented` means the production change and focused tests are complete.
- `[ ] Verified` means an independent review confirmed the stated acceptance
  criteria and the required project gates passed.

Before checking `Verified` for a behavior change, run focused tests while
iterating, then `just check`, `just crap-check`, and `just unit` on the release
candidate. Also run `just docker-build`; run `just runtime-smoke` when Docker
runtime behavior changed. Deployment changes require a staged deployment smoke
and a live service verification.

## Shared ownership contract

Complete this design checkpoint before items 2, 8, 10, or 11 are implemented.

- [x] Implemented
- [x] Verified

Use one run-level session admission lease acquired before opening the BLE
transport. Transfer its storage authority explicitly to the writer for staging
mutations. Delegate a narrowly scoped publication capability to the single
publication worker; it may publish under the active session authority or under
its own exclusive storage acquisition after the session releases it. Ownership
must be represented by typed, non-forgeable values and must not depend on which
`asyncio` task happens to execute a callback.

The completion artifact is a short ownership section in the relevant port
module plus executable ownership tests. It must name the owner before transport
entry, during writer operation, during publication, and after shutdown. Tests
must reject concurrent mutation, stale capabilities, double transfer, and use
after release. Items 2, 8, 10, and 11 must consume this contract rather than
inventing separate lease types.

## Wave 1 — Restore reliable publication and recovery

### 1. Publish a sealed capture under the lease already owned by the writer

- [x] Implemented
- [x] Verified

`_StagingWriterAdapter.seal()` currently asks `StagingStore` to reacquire its
own non-reentrant device lock. The failure is reduced to a diagnostic, leaving
the capture durable but absent from `source/current`.

Change the publication contract so the writer passes its active device lease
explicitly. Publication must not reacquire the same lock, and failures must
remain visible and retryable.

Acceptance: sealing a real staged record creates or advances `source/current`
before the writer closes; a focused test uses the real store and lock; a
publication failure leaves the captured bundle intact and reports the pending
publication state.

### 2. Make clock-triggered publication use an explicitly authorized owner

- [x] Implemented
- [x] Verified

The handoff token is owned by the coordinator task, while bounded telemetry
runs in child tasks. A child therefore cannot use the token to publish after a
clock correction.

Represent lease ownership or ownership transfer in a typed interface and run
publication in the authorized context. Do not weaken owner checks. Ensure a
failed attempt is retried independently of another BLE visit.

Acceptance: a confirmed clock correction can publish from the real telemetry
task topology; the owner mismatch remains rejected for unauthorized callers;
publication retry does not require reconnecting the pendant.

### 3. Bind startup recovery state exactly once

- [x] Implemented
- [x] Verified

Quarantine maintenance currently rebinds the cached startup descriptor on
every presence attempt. After successful reconciliation clears that descriptor,
the next visit restores the completed attempt.

Make startup binding a one-shot transition. Later maintenance runs must retain
the reconciler's current state.

Acceptance: reproduce a partial range, recover and complete it, then start a
second pendant visit at a later cursor. The completed descriptor must remain
absent and the new visit must proceed normally.

### 4. Admit an aligned recovered tail at its promoted durable frontier

- [x] Implemented
- [x] Verified

Resume activation correctly promotes complete raw records written after the
last checkpoint, but batch admission still compares against the older startup
frontier and aborts before `READ`.

Establish the lease-authorized frontier before constructing and binding the
arena. Do not merely remove the consistency check.

Acceptance: a descriptor whose checkpoint ends at sequence 101 and whose
aligned raw tail ends at 102 resumes on the first service start, with correct
arena offsets and without a fatal restart.

### 5. Rebuild publication when late salvage changes source ordering

- [x] Implemented
- [x] Verified

The fast generation path assumes every new source is appended after the
currently published sources. A valid older bundle recovered from quarantine
violates that assumption and permanently blocks future generations.

Keep the append path when the old source list is a prefix. Otherwise build a
complete replacement generation and atomically switch `source/current`.

Acceptance: publish sequence 102, later salvage sequence 100, and obtain a
valid ordered generation containing both. Repeated publication is idempotent,
and concurrent readers see either the old or the complete replacement.

## Wave 2 — Complete the clock-correction contract

### 6. Convert confirmed corrections into durable timeline repair intervals

- [x] Implemented
- [x] Verified

Clock corrections are recorded and confirmed, but production has no path that
writes the repair ledger consumed by timeline normalization. A verified backward
RTC correction can therefore block publication indefinitely.

Define the exact interval derived from accepted correction evidence, bind it to
the correct sequence/source boundary, persist it durably, and have publication
consume it automatically. Never infer a repair from an arbitrary timestamp
regression.

Acceptance: captures spanning a confirmed backward correction publish with a
monotonic external timeline and accurate source-time mapping; an unexplained
regression remains blocked; restart produces the same repair result.

### 7. Consolidate restart clock reconciliation around the production path

- [x] Implemented
- [x] Verified

`ClockCorrectionStore.replay_observations` and its selector are test-only and
duplicate the causal-selection policy used by `HistoricalClockImporter`.

Move necessary tests to the real restart path, remove the unused replay API,
and preserve live `reconcile_causal_observation` behavior. Reject legacy
incidents without anchors directly instead of maintaining a special unresolved
compatibility path.

Acceptance: native observations and retained historical evidence still recover;
missing-anchor legacy input is rejected as unresolved evidence, creates no
repair interval, and cannot alter publication; only one causal-selection policy
remains.

## Wave 3 — Fix exclusive ownership and resource admission

### 8. Acquire exclusive session admission before any pendant-side mutation

- [ ] Implemented
- [ ] Verified

Transport preflight sends `STOP` before the writer acquires the device lease.
A second collector can therefore interrupt the active collector before being
rejected.

Acquire a run/session admission lease before entering the transport and
transfer it explicitly to the writer, or introduce one narrow run-level lock
covering the complete BLE session.

Acceptance: two concurrent collection attempts cannot both reach a pendant
write; the rejected attempt sends no `STOP`; normal recovery and publication
can still use their documented lease paths.

### 9. Reserve the actual peak disk footprint

- [x] Implemented
- [ ] Verified

Disk preflight accounts for the staged batch and a small reserve, while sealing
temporarily retains the attempt and copies the full batch into captured storage.
Timeline normalization may add another copy.

Retain the current copy-and-atomic-switch publication semantics for this fix and
calculate its real worst-case peak. Include both simultaneous full copies and
normalization buffers in the memory/disk budget. A later optimization may
replace copying only with separate crash-consistency analysis.

Acceptance: a boundary-space test either rejects the batch before `READ` or
seals and publishes it successfully; ENOSPC preserves recoverable evidence;
normalization stays within the configured resource budget.

### 10. Make required storage capabilities explicit typed ports

- [x] Implemented
- [x] Verified

Lease handoff, recovery, publication, and clock operations are mandatory in
production but are omitted from application ports and discovered with
`getattr`, casts, and silent fallbacks.

Introduce narrow typed lease/publication/clock capabilities and direct calls.
Update fakes rather than preserving optional compatibility branches. Do not add
another orchestration layer.

Acceptance: a production composition missing a required capability fails type
checking or construction; recovery/publication cannot silently become no-ops;
the concrete runtime no longer casts a generic staging port back to its adapter.

### 11. Isolate publication work from BLE callbacks

- [ ] Implemented
- [ ] Verified

Telemetry invokes synchronous publication before it can apply an async
deadline. Historical validation and whole-file normalization can block BLE
callbacks and shutdown.

Use one owned publication worker with explicit queued, running, completed, and
deferred outcomes. Stream normalization in bounded chunks.

Acceptance: slow publication does not block BLE notification handling or
session shutdown; only one publisher mutates generations; restart safely
continues queued work.

## Wave 4 — Make operational status truthful

### 12. Report one reconciled captured-to-published outcome

- [ ] Implemented
- [ ] Verified

Routine status does not inventory captured bundles or state how much remains
unpublished. Publication errors are reconstructed from only the current rotating
debug log.

Report captured records/bytes, published records/bytes, held records/bytes, and
the durable reason publication is blocked. Treat debug events as explanation,
not status authority.

Acceptance: every retained capture belongs to exactly one reported publication
state and totals reconcile; a blocked capture remains visible after log rotation
and service restart.

### 13. Separate current liveness, visit completion, and historical quality

- [ ] Implemented
- [ ] Verified

An old successful transfer can keep status `ok` during a later failed or
unfinished visit. `away` and `drained` can report `ok` without a completed
transfer, and device backlog lacks a useful observation age.

Add a heartbeat tied to the running invocation, timestamp every device/backlog
observation, expose the last verified drain, and detect sustained failure
separately from ordinary short absences and retries. Put the freshness and
failure-window thresholds in the validated immutable configuration, choose and
document production defaults in the same change, and test the exact boundary
values. Use the product wording “service healthy, pendant out of range” for a
healthy absent device.

Acceptance: stale observations cannot certify the current visit; a previous
success cannot hide sustained failures; absence alone is not an error; the
operator can determine whether the latest visit finished.

### 14. Persist component health outside rotating diagnostics

- [ ] Implemented
- [ ] Verified

Unresolved publication, configuration, and quality-journal failures disappear
when their debug events rotate away.

Persist current component health and clear a degraded state only after the
corresponding component successfully recovers.

Acceptance: an unresolved failure survives all debug rotations and restart;
the precise recovery action clears it; status does not rely on scanning backup
logs.

### 15. Surface quality-journal write failures

- [ ] Implemented
- [ ] Verified

Journal append failures are ordinary warnings. Status can continue to report no
confirmed loss even though evidence is no longer being recorded.

Emit structured writer-health state, count failed/dropped writes, and retain a
degraded condition until a successful durable append.

Acceptance: permission, disk, and queue-saturation failures produce degraded
status and accurate counters; the initial “ready” state is emitted only after a
successful write.

### 16. Use distinct names for sequence gaps and confirmed loss

- [ ] Implemented
- [ ] Verified

Publication holes are currently labeled `lost_records`, while quality evidence
uses the same language for confirmed cursor loss.

Rename publication-only values to `missing_sequence_records` and
`sequence_gap_ratio`, or remove them from routine status. Reserve “confirmed
loss” for explicit loss evidence.

Acceptance: ranges `[0,10)` and `[20,30)` without loss evidence report a
sequence gap but zero confirmed loss; downstream JSON and documentation use the
same meanings.

### 17. Make routine status scale with retained history

- [ ] Implemented
- [ ] Verified

Every status call currently rereads and hashes all visible audio. Published
audio has no retention limit, so cost grows indefinitely.

Maintain validated incremental inventory summaries for routine status and keep
full content verification as an explicit audit command. Routine status may read
metadata for the complete inventory, but it must read/hash audio bytes only for
bundles added or changed since the last valid summary.

Acceptance: routine status work is proportional to new/changeable state rather
than total history; the explicit audit still detects historical corruption;
summary recovery after interruption is deterministic.

### 18. Correct reconnect quality counters

- [x] Implemented
- [x] Verified

Each physical reconnect reports arena and writer lifetime byte totals, which
are then added again to session metrics.

Capture per-leg baselines and report byte deltas. Keep lifetime watermarks only
for recovery decisions.

Acceptance: a two-record transfer interrupted after one record reports 444
bytes for each leg and 888 only for the overall batch; completion and throughput
metrics reconcile.

## Wave 5 — Harden deployment and readiness

### 19. Never execute a mutable candidate interpreter as root

- [x] Implemented
- [x] Verified

Deployment builds a candidate environment and cache as the service account, then
executes the candidate Python as root before ownership is sealed.

Run candidate-code verification as an unprivileged dedicated build identity,
separate build/cache ownership from the live service identity, and ensure root
metadata operations cannot follow attacker-controlled paths.

Acceptance: no root step imports or executes candidate Python or code from a
service-writable cache; ownership and symlink checks cover every root mutation;
the staged deployment smoke still validates the installed package.

### 20. Move readiness after local recovery and scanner initialization

- [ ] Implemented
- [ ] Verified

The process announces deployment readiness before synchronization, scheduler
construction, BLE observation, and recovery. Deployment then removes all prior
releases after a short wait.

Emit readiness after configuration, local recovery, and scanner initialization.
Keep readiness independent of pendant presence. Deployment must fail unless the
current systemd invocation emits that readiness event within the script's
declared timeout. Retain one previous known-good release until the new release
passes the same unprivileged package/import verification and the service remains
active with the expected invocation after the readiness event.

Acceptance: delayed initialization failures reject deployment; a missing
pendant does not; rollback remains available after acceptance and its cleanup is
explicitly bounded.

### 21. Define `health` honestly

- [ ] Implemented
- [ ] Verified

The command always prints `ok`, yet its name and documentation imply service
health and Docker uses it as a health check.

Rename it to an import/package smoke and use it only for packaging checks.
Direct operators and service monitoring to the real status command for
collection state. Do not add a second shallow runtime-health implementation.

Acceptance: command name, output, README, and Docker packaging check describe
the same import/package contract; systemd monitoring uses status/liveness;
unconditional `ok` is not presented as production health.

## Wave 6 — Make configuration and operator commands coherent

### 22. Map immutable configuration once at composition

- [ ] Implemented
- [ ] Verified

Arena size, notification-buffer size, and clock thresholds do not consistently
flow from the configuration object; one CLI staging path ignores the configured
path. Tests also use constructor overrides that compete with configuration.

Create one explicit composition mapping and remove duplicate override routes.

Acceptance: every retained setting has one source and reaches its production
consumer; configuration-path selection is honored by every command; tests
construct explicit configurations instead of hidden compatibility overrides.

### 23. Stop changing controller-wide PHY state during ordinary inspection

- [ ] Implemented
- [ ] Verified

`probe` and `info` enter the controller mutation guard even though automatic
collection uses normal negotiation.

Use normal negotiation by default and retain forced 1M PHY as an explicit
diagnostic option with restoration and recovery.

Acceptance: ordinary inspection needs no host-change confirmation and does not
mutate controller policy; the diagnostic fallback remains testable and restores
the previous state.

## Wave 7 — Make quality gates fail closed and deterministic

### 24. Bound every asynchronous and threaded test wait

- [ ] Implemented
- [ ] Verified

Presence, lease, and cancellation tests contain unbounded spin loops, barriers,
executor waits, and thread joins.

Use bounded event/barrier waits, unconditional cleanup signals in `finally`,
bounded joins, and a final process-level watchdog.

Acceptance: intentionally broken startup, lease, and cancellation paths fail
with a local assertion instead of hanging the suite or consuming a core.

### 25. Replace scheduler-latency assertions with synchronization assertions

- [ ] Implemented
- [ ] Verified

Several tests require operations to complete or remain pending within 30–200
milliseconds under xdist and coverage.

Control workers with events, verify event-loop progress before release, and use
generous watchdogs only for deadlock detection. Test deadline calculations with
a controlled clock.

Acceptance: tests verify nonblocking behavior and teardown ordering without
depending on host scheduling latency; repeated loaded runs remain stable.

### 26. Make the CRAP gate enumerate source and reject incomplete reports

- [ ] Implemented
- [ ] Verified

The gate currently discovers functions from the coverage report and accepts an
empty report as “0 functions passed.”

Enumerate source functions independently, require a complete match, and fail on
empty, omitted, malformed, or unmatched coverage input.

Acceptance: self-tests cover empty reports, missing files/functions, bad counts,
unmatched qualified names, and a known offender; the current complete report
still maps all source functions.

### 27. Require successful scripted protocols to consume their expectations

- [ ] Implemented
- [ ] Verified

The scripted ring rejects unexpected writes but permits close with expected
steps left unconsumed.

Add `assert_consumed()` to successful scenarios. Mark interruption and
cancellation scenarios explicitly when unfinished scripts are intentional.

Acceptance: omitting terminal INFO or ADVANCE verification fails the relevant
successful coordinator test.

### 28. Add an abrupt-process recovery matrix

- [ ] Implemented
- [ ] Verified

Existing recovery tests generally allow Python cleanup and do not cover process
termination at filesystem commit boundaries.

Use subprocesses that exit without cleanup between raw write/checkpoint update,
destination rename/directory fsync, and publication/retirement markers, then
recover in a fresh process.

Acceptance: each boundary has a deterministic recovered state with no accepted
byte corruption, duplicate ADVANCE, or invisible durable capture. Describe this
as process-crash coverage, not physical power-loss simulation.

### 29. Correct state-machine coverage claims and CI aggregation

- [ ] Implemented
- [ ] Verified

One test checks transition totality and result types but is described as
semantic coverage of every transition. Presence tests omit `Searching` and
`CoolingDown` despite claiming every waiting state. CI aggregation accepts
skipped prerequisite jobs.

Add independent semantic invariants for terminal/failed states, cover all
waiting states, and correct misleading test names. The aggregate CI job must
derive its mandatory set from every job listed in its `needs` dependency and
require every result to equal `success`; cancellation, failure, and skip all
fail aggregation.

Acceptance: coverage claims match what assertions establish; skipped mandatory
jobs cannot produce a successful aggregate result.

## Wave 8 — Remove obsolete surfaces and duplication

### 30. Delete the superseded record-oriented staging API

- [x] Implemented
- [x] Verified

Remove unused `StagingWriter.accept_record`. Migrate tests from
`StagedAttempt.append_record` and its private helper to the production chunk
path, then remove those APIs.

Acceptance: production and tests use one chunk write contract while preserving
chunk validation, replay comparison, checkpoints, and crash scenarios.

### 31. Remove durability state from `TransferArena`

- [x] Implemented
- [x] Verified

Arena durability checkpoints and durable/submitted chunk iterators have no
production consumer. Actual durability belongs to the staging writer and batch
checkpoint.

Acceptance: arena retains bounded receive/replay storage only; the writer
checkpoint remains the single durable authority; recovery tests retain their
behavior.

### 32. Remove the unused record decoder/assembler island

- [x] Implemented
- [x] Verified

Remove `RingRecord`, `parse_audio_payload`, and `RingRecordAssembler` with their
isolated tests. Retain wire notification parsing, record-size constants, arena
ingestion, and publication-time timestamp handling.

Acceptance: no production behavior or publication decoding depends on the
removed island; protocol and arena suites remain complete.

### 33. Collapse compatibility constructors and duplicate staging openers

- [ ] Implemented
- [ ] Verified

Use only `WriterConfig` for `AttemptWriter`, one store-based `StagingWriter`
constructor, and one staging inspection opener. Keep resume activation as the
separate mutating operation.

Acceptance: production and tests use the same constructor shapes; no override
knobs or identically implemented `open_attempt` variants remain.

### 34. Remove confirmed zero-consumer production surfaces

- [ ] Implemented
- [ ] Verified

Remove, after a fresh reference check:

Completed in this pass:

- the unused staging-contract conversion/hash helpers and resulting imports;
- unused filesystem `file_size` and append helper;
- unused held-lease aliases;
- the duplicate `StagingWriterTarget` protocol;
- the unused `RingTransport` and `OperationalSession` protocols;
- unused battery-first collection helper;
- the unread publication retry counter;
- test-only convenience entrypoints whose tests can use production paths.

Still pending:

- unused observation-boundary helper;
- unpopulated replay/append result fields and unread progress total.

Do not remove framework-dispatched logging overrides, Typer commands, pytest
fixtures, consumed protocol parameters, `RingSession`, `WriterTarget`, lease
handoff, checkpoints, publication markers, HCI event variants, or useful lazy
imports.

Acceptance: the review produces a committed production-only inventory. Every
remaining exception names the symbol, its framework/runtime caller, and why
static discovery cannot see it; ordinary vulture and all static gates pass.

### 35. Remove mutation-only CLI test hooks and the forwarding entrypoint

- [ ] Implemented
- [ ] Verified

Patch defining reporting modules in tests instead of retaining production
globals used only for monkeypatching. Import the real capture CLI lazily instead
of routing through a module that only reexports it.

Acceptance: health/help import isolation remains intact and production contains
no test-only dispatch globals.

### 36. Consolidate exact low-level duplicates and fixture helpers

- [ ] Implemented
- [ ] Verified

Share the duplicated lock wrapper, directory-fsync operation, and operational
callback invocation without creating a generic storage framework. Consolidate
repeated capture-root, symlink-replacement, and guarded-rename test helpers into
explicit fixtures/factories.

Acceptance: duplicated bodies have one maintained implementation; syscall fault
injection and filesystem identity checks remain visible in tests.

### 37. Remove unused directive fields and misleading generation terminology

- [ ] Implemented
- [ ] Verified

Remove the unread `Observe.until` field or make it the sole deadline authority.
Document generations according to their actual append/rebuild behavior; do not
call a mutating generation an immutable snapshot.

Acceptance: presence has one deadline authority and generation terminology
matches the implementation introduced in item 5.

### 38. Strengthen the production-surface dead-code audit

- [ ] Implemented
- [ ] Verified

The current vulture confidence threshold and combined source/test scan allow
test-only APIs to appear live.

Add a narrow reviewed production-surface check with a committed allowlist. Each
allowlist entry must name one symbol, its framework/runtime caller, and the
reason static discovery cannot see it. Do not blindly reject every
low-confidence framework hook.

Acceptance: a synthetic test-only production API is reported, while framework
entrypoints remain intentionally allowed.

### 39. Reduce the ordinary CLI to product-level operations

- [ ] Implemented
- [ ] Verified

The command surface presents `probe`, bounded `collect`, PHY manipulation, and
`device metrics` alongside normal unattended operation even though they are
specialist diagnostics, and device metrics substantially overlaps status.

Classify diagnostic commands explicitly, remove a command when its useful
output is fully represented in reconciled status, and keep low-level controls
out of the normal operating path. Preserve capabilities that are still needed
for hardware diagnosis rather than deleting them solely to shrink command
count.

Acceptance: README and CLI help expose one obvious ordinary workflow; every
remaining diagnostic command has a distinct stated purpose; redundant device
metrics have either been removed or have a documented outcome unavailable from
status.

## Wave 9 — Align documentation and product language

### 40. Correct the public boundary specification

- [ ] Implemented
- [ ] Verified

The acceptance feature promises original bytes downstream, while the actual
public timeline intentionally normalizes timestamps. Rewrite scenarios around
the two boundaries: captured bytes are preserved; `source/current` is the usable
normalized timeline.

Acceptance: feature text, README, best practices, and timeline tests state the
same contract, including the behavior when normalization is blocked.

### 41. Describe Windmill as an optional future integration

- [ ] Implemented
- [ ] Verified

There is currently no external consumer, but README presents a private Windmill
speech/VAD pipeline as operating production behavior.

End the collector's current product flow at `source/current`. Mention Windmill
briefly as the intended private downstream integration without claiming it is
connected.

Acceptance: readers can distinguish current collector behavior from planned or
external processing, and no undocumented retention promise is implied.

### 42. Document current visit, cached observation, gap, and loss semantics

- [ ] Implemented
- [ ] Verified

Explain that service health, pendant presence, current visit completion,
historical transfer quality, cached device state, sequence gaps, and confirmed
loss are separate facts.

Acceptance: operator guidance tells the reader exactly when a visit has finished
and what can safely be concluded from every status state; it does not claim that
a sequence gap is confirmed audio loss.

### 43. Establish ownership of retained raw and published storage

- [ ] Implemented
- [ ] Verified

Start this item by recording one explicit policy decision in README: the exact
configured retention for raw captured data, permanent retention for published
artifacts while no downstream consumer exists, and deletion authority. Then
make implementation, configuration, status, and documentation agree with that
decision. Temporary processing material may exist only for the lifetime of the
operation that consumes it.

Acceptance: README contains the decision and owner; configured cleanup matches
the stated raw retention; published artifacts are not automatically deleted;
temporary material is cleaned on success and failure; routine status exposes
the inventory and capacity needed to operate the policy.

## Final release review

- [ ] Every item above has separate implementation and verification evidence.
- [ ] No compatibility fallback, migration helper, backup, worktree, temporary
  file, container, database, or cache created for the remediation remains unless
  it is an intentional production resource documented here.
- [ ] `just check`, `just crap-check`, `just unit`, and `just docker-build` pass
  from a clean checkout; applicable runtime and deployment smokes pass.
- [ ] An independent adversarial review finds no unresolved P0/P1 issue and no
  regression in raw-byte preservation, replay comparison, fresh INFO before
  ADVANCE, quarantine, or recovery.
- [ ] Production runs the reviewed release and live status reconciles captured,
  published, and held data without relying on rotating debug logs.
