# Cross-AI Result: deepseek-v4-1-flash

Execution attempts:

- shared: /home/j2h4u/.opencode/bin/opencode run --model deepseek/deepseek-flash --agent plan --format json --variant max --attach http://127.0.0.1:34515 --dir /home/j2h4u/.compressed/repos/j2h4u/omi-collector -f /home/j2h4u/.compressed/repos/j2h4u/omi-collector/.planning/reviews/omic-16/20261013T141411Z/inputs/omic-16-implementation-plan.md --title cross-ai-deepseek-v4-1-flash -- 'Goal / decision to support: Second adversarial review of the REVISED attached OMIC-16 IMPLEMENTATION PLAN, not implemented code. Read its First standard review disposition table and validate each correction against current source. Focus on remaining actionable plan gaps and regressions introduced by the revision: per-append-chunk COMMIT preserving current process-kill recovery; immutable loop-thread partial-tail snapshots and shorter replay; total leg/source/quarantine/retirement FSMs; exact-byte typed schema and unsigned sequence domain; query-only SQL UTC versus unchanged range-level public output; accepted cohort across partial publication; one deterministic output stage; SQLite-aware hot-journal backup and logical parity; legacy JSON conversion and opaque evidence classification; durable new-release selector BEFORE schema activation; backward-readable v1 cutover receipt and true pre/post activation rollback; bounded space, contention, revision wakeups and cleanup. Standard output ready audio/passports and external ACK remain unchanged, all internal audio lives in the existing SQLite, no archives/fallback files/second backend. Do not repeat unsupported earlier claims that private audio waits for consumer ACK or that capture-root temporary recovery covers ready-root scratch. No implementation/tests/production commands. Measurements are explicitly a prerequisite stage of the executable plan, not claimed completed; flag an underspecified measurement or fallback, not mere absence of runtime results at plan-review time. Return only concrete actionable Critical/High/Medium/Low findings with failure trace, plan/source references, smallest PLAN correction and acceptance check, then unconditional GO or NO-GO with remaining blockers. If no actionable gaps remain, say so; do not invent findings.

Adversarially review the attached context. Focus on correctness, design fit, security/privacy, operational risk, and whether this is ready to ship. Return Critical/High/Medium/Low findings first, ordered by severity, with file/line references where possible. End explicitly with GO or NO-GO, a brief rationale, and any remaining actionable blockers. Avoid nitpicks. Do not edit files.

This is a headless, non-interactive session in a trusted workspace. The tool restrictions are a behavioral and resource boundary, not a hostile-code security sandbox. Never request permission or approval because an unanswered prompt deadlocks the run. Do not delegate to nested agents, invoke task, or use @explore. Your complete working boundary is /home/j2h4u/.compressed/repos/j2h4u/omi-collector. Freely read and search files beneath that exact directory. Never inspect sibling directories, parent directories, or any other absolute path. If evidence is unavailable inside the boundary, state that limitation and continue the review. The process already runs from the repository root, so invoke git diff, git status, git show, or git log without git -C. If any tool call is denied, do not search for workarounds: state the denied command and limitation in your final response immediately. Do not edit files.'

Command:

```bash
/home/j2h4u/.opencode/bin/opencode run --model deepseek/deepseek-flash --agent plan --format json --variant max --attach http://127.0.0.1:34515 --dir /home/j2h4u/.compressed/repos/j2h4u/omi-collector -f /home/j2h4u/.compressed/repos/j2h4u/omi-collector/.planning/reviews/omic-16/20261013T141411Z/inputs/omic-16-implementation-plan.md --title cross-ai-deepseek-v4-1-flash -- 'Goal / decision to support: Second adversarial review of the REVISED attached OMIC-16 IMPLEMENTATION PLAN, not implemented code. Read its First standard review disposition table and validate each correction against current source. Focus on remaining actionable plan gaps and regressions introduced by the revision: per-append-chunk COMMIT preserving current process-kill recovery; immutable loop-thread partial-tail snapshots and shorter replay; total leg/source/quarantine/retirement FSMs; exact-byte typed schema and unsigned sequence domain; query-only SQL UTC versus unchanged range-level public output; accepted cohort across partial publication; one deterministic output stage; SQLite-aware hot-journal backup and logical parity; legacy JSON conversion and opaque evidence classification; durable new-release selector BEFORE schema activation; backward-readable v1 cutover receipt and true pre/post activation rollback; bounded space, contention, revision wakeups and cleanup. Standard output ready audio/passports and external ACK remain unchanged, all internal audio lives in the existing SQLite, no archives/fallback files/second backend. Do not repeat unsupported earlier claims that private audio waits for consumer ACK or that capture-root temporary recovery covers ready-root scratch. No implementation/tests/production commands. Measurements are explicitly a prerequisite stage of the executable plan, not claimed completed; flag an underspecified measurement or fallback, not mere absence of runtime results at plan-review time. Return only concrete actionable Critical/High/Medium/Low findings with failure trace, plan/source references, smallest PLAN correction and acceptance check, then unconditional GO or NO-GO with remaining blockers. If no actionable gaps remain, say so; do not invent findings.

Adversarially review the attached context. Focus on correctness, design fit, security/privacy, operational risk, and whether this is ready to ship. Return Critical/High/Medium/Low findings first, ordered by severity, with file/line references where possible. End explicitly with GO or NO-GO, a brief rationale, and any remaining actionable blockers. Avoid nitpicks. Do not edit files.

This is a headless, non-interactive session in a trusted workspace. The tool restrictions are a behavioral and resource boundary, not a hostile-code security sandbox. Never request permission or approval because an unanswered prompt deadlocks the run. Do not delegate to nested agents, invoke task, or use @explore. Your complete working boundary is /home/j2h4u/.compressed/repos/j2h4u/omi-collector. Freely read and search files beneath that exact directory. Never inspect sibling directories, parent directories, or any other absolute path. If evidence is unavailable inside the boundary, state that limitation and continue the review. The process already runs from the repository root, so invoke git diff, git status, git show, or git log without git -C. If any tool call is denied, do not search for workarounds: state the denied command and limitation in your final response immediately. Do not edit files.'
```

Output:

# Adversarial review — revised OMIC-16 plan vs. source at HEAD `c4b7863` (v0.12.2 + #244)

Scope: plan `docs/OMIC-16-IMPLEMENTATION-PLAN.md` (line refs below) checked against current `src/omi_collector` and `scripts/deploy-systemd-service.sh`. No edits made. The three mandated challenges were verified against source (see "Verification notes"); they do not produce findings. Findings below are limited to gaps/regressions introduced or left by the revision.

---

## High

### H1. The eager per-row UTC projection refresh is an unbounded SQL write that contradicts the plan's own bounded-transaction, preflight, and contention rules

Plan references: line 21 — "Refresh derived UTC for unpublished rows from the existing clock mapper **when its revision changes** and again before freezing an output-stage mapping"; line 61 — "Preflight … the worst **bounded** transaction's DELETE rollback journal. Keep insertion and retirement transactions bounded (at most one writer drain …)"; line 27 — clock/quality writes must complete against the writer's bounded retry deadline.

Source references: `collector_state_db.py:165-187,246-311` (per-call connection, `BEGIN IMMEDIATE`, `journal_mode=DELETE`, `synchronous=EXTRA`), `collector_state_db.py:334-341` (`CLOCK` is a publication-input kind), `clock_state_store.py:38-67` (clock observation is a synchronous write on the calling thread — during sync that is the event loop), `config.py:147` + `session_lifecycle.py:147-151` (largest configured batch is the whole unread range up to 512 MiB ≈ 1.2 M records).

Failure trace: one sub-threshold unpublished source can hold ~1.2 M rows (~500 MB payload). Any accepted clock observation changes the clock revision. Implemented as written, the refresh is a single `UPDATE capture_record SET utc_timestamp=…, mapping_revision=… WHERE published=0`: DELETE-journal mode must journal every modified leaf page (the entire payload table), `synchronous=EXTRA` fsyncs it, the event-loop clock call blocks for the duration, the writer thread's finite busy-retry deadline can expire mid-capture (which the plan itself says must prevent `ADVANCE`), and the preflight reserved only the bounded insert/retire journal — disk-full mid-UPDATE rolls back needing that same journal. Worse, the projection is explicitly query-only and is **not** used to write public output (line 47), so this risk buys nothing.

Smallest plan correction: replace the eager refresh with a bounded, resumable projection refresh: fixed row quantum per transaction with a persisted cursor (or pure query-time derivation), executed off the event loop and never inside a BLE-receiving window; add the refresh quantum to the space preflight; state explicitly that a stale/`NULL` projection is acceptable because public output derives ranges from `ClockSegmentMap`.

Acceptance check: fixture with a full-batch unpublished source; change the clock revision; assert peak `page_count`/journal/freelist delta and writer drain frontier/latency are bounded by the configured quantum, and that the event-loop write completes within its own deadline.

### H2. "Selector before activation" is not executable with the current `migrate-state` contract; the phase boundary, receipt visibility and trap inputs are unspecified

Plan references: line 63 — "durably publish/fsync the **new** release selector only after the candidate is completely built, receipted and parity-verified, but **before** `candidate.replace(canonical)`"; "finalize trap must re-read the canonical schema and cutover receipt on **every** invocation"; step 5, line 73.

Source references: `cli.py:414-440` — one `migrate-state` invocation calls `migrate_state()`; `state_migration.py:159-204` — candidate build, parity, `candidate.replace(canonical)` (line 196), legacy deletion and completion all happen inside that call; `deploy-systemd-service.sh:710-719` — stop → `migrate-state` → publish selector → restart; `deploy-systemd-service.sh:597-638` — `finalize` currently decides `forward_only` from `service_quiesced || selection_published` and `state_file exists`, with no schema/receipt read.

Failure trace: there is no point at which the wrapper can publish the selector "after parity but before `replace`"; an implementer must either keep activation inside the same call (selector published after the irreversible v2 boundary — the exact hazard the revision claims to remove) or invent an unspecified phase split. The trap then has no defined inputs: after a crash between selector publication and activation the selector already points to the new release, canonical is v1, a verified candidate exists, and the current `finalize`/`rollback_deployment` (`:480-501`) neither reads schema/receipt nor removes the candidate. This specifically breaks pre-activation rollback: 0.12.2 `open_runtime_state()` raises `"unfinished Collector state migration candidate needs inspection"` whenever `.collector-state.sqlite3.migrating` exists (`state_migration.py:103-104`, unchanged at tag `232fc74`), so restarting the old binary with the candidate present produces a crash loop. The plan's "safe candidate disposition" phrase never states this behavior or the required removal.

Smallest plan correction: define a two-phase CLI contract, e.g. `migrate-state --prepare` (validates v1 source, writes source receipt, builds/parity-verifies candidate, writes `phase=prepared` candidate receipt, exits success **without** `replace`), wrapper fsync-publishes selector, `migrate-state --activate` (idempotent `replace` + fsync parent + fingerprint-verified deletion, resuming from the receipt); name the exact receipt fields, paths and exit codes, and state that the pre-activation trap disposes of the verified candidate (remove/relocate) before starting the old binary, deciding solely on canonical schema + receipt, never on process-local flags.

Acceptance check: kill -9 immediately before/after selector publication and immediately before/after activation; assert selector/canonical-schema consistency, candidate disposition, and old-release readiness (`deploy_ready`) on pre-activation rollback in each case.

---

## Medium

### M1. Tail row vs. committed complete-record frontier is not pinned for split ≤128-record drains

Plan references: line 23 — every delivered drain (including a shorter drain) commits "rows, count/SHA checkpoint, and the current exact tail"; line 25 — the tail snapshot is copied on the loop thread before await, "the writer drains only the captured complete high-water and commits only the copied snapshot", and "A committed tail never advances the complete-record sequence frontier"; `capture_tail` is "1–443 bytes".

Source references: `attempt_writer.py:651-667` caps each `append_chunk` at `chunk_records` = 128 (`config.py:233`); a captured snapshot's complete frontier can exceed one chunk whenever the writer lagged or a burst of `publish()` calls coalesced (`collector.py:463-470`).

Failure trace: the copied tail is anchored at the snapshot frontier, but an intermediate 128-record chunk commits at a lower frontier. Committing the snapshot tail with that chunk stores a tail offset ahead of the committed frontier (violating the plan's own invariant and changing replay comparison semantics), while reading arena bytes at the intermediate frontier on the writer thread violates "never read a mutable live tail". Either resolution is currently unspecified, so a kill between split chunk commits can leave tail evidence that does not correspond to the durable frontier and can cause a valid replay to be classified as a conflict.

Smallest plan correction: state that each ≤128-record transaction commits the tail **whose offset equals that transaction's committed complete frontier**, taken from bytes below the captured received watermark (which the arena already guarantees immutable), or explicitly capture a snapshot per drain boundary; explicitly forbid any committed tail offset ≠ committed frontier.

Acceptance check: force a split drain of one snapshot, kill between chunk commits, then replay shorter/equal/longer prefixes; assert the stored tail offset equals the committed count and resolution matches the matrix without a false conflict.

---

## Low

### L1. Step-1 per-drain latency criterion has no numeric pass/fail

Plan references: lines 23 and 69 — "cannot keep up with the largest real batch", "without unbounded writer lag".

Failure trace: "unbounded writer lag" is not measurable as written; the transfer's DONE barrier (`batch_reconciliation.py:537-542` with `sync_timeout_seconds`=60, `config.py:130`) is what actually fails, so an implementer cannot decide GO/no-GO from the measurement. Smallest correction: define p95 per-drain commit latency, max writer lag (records/bytes) over the largest measured batch, and that the final barrier must complete within the configured transfer timeout; otherwise the design revision gate of line 23 triggers. Acceptance check: record the numbers next to the step-1 baseline and re-check in step 6.

---

## Verification notes on the three mandated challenges

- **0.12.2 source-receipt backward compatibility — verified.** At tag `232fc74` and at HEAD, `open_runtime_state()` reads only `state(kind='migration', id='current')` and `_state_digest()` skips the entire `MIGRATION` kind (`state_migration.py:100-119,900-908`), so an added `internal_audio_cutover` row is invisible to the old binary. The old binary does reject schema v2 at `collector_state_db.py:209-210`, matching the plan's irreversible-boundary claim. The only residual hazard is H2 (candidate presence), not the receipt.
- **Legacy opaque evidence — representable byte-exactly, no finding.** `quarantine.py:421-471` already classifies `symlink`/`file`/`directory` kinds with `lstat` identity; symlink text via `readlink` without following, regular sidecar/opaque files in bounded chunks, unknown directory shapes refused — consistent with the plan's `capture_evidence` design.
- **Per-drain latency — correctly deferred to the measurement gate; only the criterion is under-specified (L1).** The plan's claim that the old raw writer survived each delivered chunk on process kill is accurate (`attempts.py:339-366` promotes the aligned raw tail on restart), and the per-drain commit is a testable replacement.

Other dispositions in the first-review table were checked and hold against source (draft removal after durable ready+ledger, `ready_bundles.py:213-222,465-475`; `recover_capture_temporaries` covers only the capture root, `publication.py:161-204`; UUID ready scratch, `ready_bundles.py:804`; `_publication_revision` composition, `staging_store.py:296-303`; writer drain 128/`checkpoint_records` 1024, `attempt_writer.py:651-667`, `attempts.py:515-529`).

---

## Verdict

**NO-GO.** Blockers: **H1** (unbounded, unnecessary eager SQL refresh that can stall capture, exhaust disk, and violate the plan's own bounded-transaction and contention guarantees) and **H2** (the central irreversible-boundary ordering has no executable phase contract, and the pre-activation rollback path omits the old binary's candidate-refusal behavior). Both are narrow plan amendments, not architectural reversals: the storage model, FSMs, UTC/public-output split, cohort permit, deterministic stage, hot-journal migration approach, and the first-review dispositions otherwise validate against current source. M1 and L1 should be folded into the same edit; only H1/H2 need re-review.

(No denied commands; all evidence was read from within the workspace. Tool restrictions limited shell use to `git log/status/show`, which was sufficient.)
