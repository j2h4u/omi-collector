# Service audit tracker

This tracker covers the eight original service findings and later QA and
operational follow-ups. A checked item means its fix is implemented and its
targeted evidence has been reviewed and accepted; an open item remains
unresolved.

## Findings

1. [x] **Older orphan draft crashes recovery.** An older orphan draft below an
  existing closure reaches `staging_store.close_orphaned_drafts` before
  `inspect_recovery` can classify it, raising `ReadyClosureError` and stopping
  the service. Preserve the recovery invariant that an eligible older orphan
  is closed only after a typed `DrainConfirmed(cursor)` event chain reaches its
  confirmed cursor; recovery must apply an exhaustive, deterministic merge
  policy across all drafts and closures. The FSM must cover every state/event
  pair explicitly. Do not weaken authorization or monotonic sequence checks,
  or swallow exceptions to bypass the FSM.
  Acceptance: test an older orphan below a later closure, each drain-confirmed
  cursor transition, merge-order permutations, and invalid or unauthorized
  state; every affected FSM model must reach its specified outcome without
  losing evidence. Coverage enumerates declared state/event/work/result
  variants and inventories, without claiming guarantees for arbitrary payloads
  or future undeclared variants. Sol accepted exact 3x5 operational coverage,
  all 5x6x6 batch cells with inventories, and 352 representative visit cells
  with union inventories. The publication transition now explicitly handles
  `InputChanged` and `SETTLED`, rejects unsupported variants, and pins test
  fixtures to declared unions and enums. Sol accepted final review; the earlier
  closure cluster passed 355 focused tests, and the publication-machine cluster
  passed 9 targeted tests.
  The older-orphan implementation itself was accepted after 355 focused tests.
2. [x] **Status can report false `ok`.** `operator_status.window` may return
  `ok` while the service is away or drained and the window has zero completed
  transfers. Model current health in a durable FSM with explicit `unknown`,
  `clear`, and `blocked` states, independent of rotating debug logs. One
  producer owns the snapshot under its exclusive writer lease; persist only
  low-rate state changes, with no heartbeat or TTL. The producer identity is
  the host boot ID plus systemd invocation ID, or a generated UUID when no
  invocation ID is supplied (for foreground or Docker runs). A reader uses a
  systemd invocation ID only when systemd reports the service active; without
  one, old `clear` state is `unknown` and old `blocked` state remains attention.
  Acceptance: test each affected status FSM model through startup/unknown,
  available/clear, and blocked transitions; away or drained with no completed
  transfer cannot report `ok`; test competing writers, lease lifetime, both
  identity paths and reader fallback, and log rotation. Initial 119-test run
  had two stale-fixture failures; those fixtures are fixed and the latest
  four-module rerun passed: 125 tests in 4.01 seconds. Sol accepted the
  read-only review of lease, fallback, quality, and supervision behavior.
3. [x] **Confirmed-loss attention can be cleared by unrelated telemetry.**
  A confirmed `SequenceLossMetric` can be dropped when the bounded quality
  queue overflows; a later successful quality write can then clear `BLOCK`,
  losing the only durable loss fact and producing false `ok`. The status
  snapshot alone does not fix this. Accepted architecture: write and fsync an
  authoritative confirmed-loss ledger before prefix publication; status reads
  that ledger, while quality telemetry remains a projection. Use automatic
  idempotency and count deduplication, preserve retained legacy loss facts, and
  require no manual reset. Acceptance: test crash/order boundaries around
  fsync and publication; queue overflow followed by success must retain loss
  and `attention`; repeated projections must not double-count; retained legacy
  facts remain visible; no reset is needed. Sol accepted the loss-ledger/status
  seams and the ordered storage call with drained-loss and empty-quarantine
  assertions. The seven-module targeted cluster passed 279 tests in 8.49
  seconds; targeted implementation and review are accepted.
4. [x] **Failed mutation receipt leaks job resources.**
  `mutation_campaign._new_job` creates a checkout and environment before the
  owner receipt is safely recorded. Write a discoverable `preparing` receipt
  before bootstrap; every bootstrap outcome must leave a durable owned state
  or clean up all resources it created. Acceptance: inject failures before,
  during, and after bootstrap and verify a discoverable `preparing` or failed
  receipt, correct ownership, and no leaked unowned checkout or environment.
  Root accepted the fix in the 102-test focused group (15.52 seconds).
5. [x] **Owner control server can hang on a client.**
  `OwnerControlServer` silently receives unbounded data and its thread can
  outlive the test's timeout, masking the actual failure. Give the complete
  connection handling an absolute deadline, bound each handler, and make stop
  own and join cleanup. Acceptance: test oversized and stalled input against
  one deadline, and verify stop reliably closes clients and joins the server
  thread even after handler failure. Root accepted the fix in the 102-test
  focused group (15.52 seconds).
6. [x] **Runtime smoke suppresses cleanup failure.** The Justfile runtime-smoke
  cleanup uses `down || true`, hiding failed teardown. Handle TERM and INT with
  cleanup while preserving the original smoke exit status; report cleanup
  failure when the smoke otherwise succeeds. Acceptance: test success and
  failure of smoke and cleanup in combination, including TERM/INT, and verify
  cleanup always runs without replacing the original failure status. Root
  accepted the fix in the 102-test focused group (15.52 seconds).
7. [x] **Supply-chain check skips `FROM --platform`.** The Dockerfile pin check
  misses image references with a platform qualifier. Parse `FROM --platform`
  stages and apply the same immutable-image requirement to their image token.
  Acceptance: test pinned and unpinned references with platform qualifiers,
  including the supported option order and stage syntax. Root accepted the fix
  in the 102-test focused group (15.52 seconds).
8. [x] **README clock behavior is incomplete.** Clock mapping can include both
  confirmed intervals and approximate intervals derived from trusted native
  reads. Both mapped intervals rewrite timestamps; approximate mappings are
  marked with `confidence: approximate`, and UTC stays unknown only where no
  estimate exists. Audio ordering follows record sequence, not wall-clock
  timestamps. Acceptance: README accurately describes those rules and its
  examples agree with manifest behavior. The clock source and documentation
  diff were checked and accepted by root.
9. [x] **Collector checkpoint access is restored.** The retained revision in
  `/srv/pipelines/omi/work/omi-ready-checkpoint.json` is a real ACK dependency.
  The original work parent (`j2h4u:j2h4u`, mode `2770`) lacked collector
  traversal, and the checkpoint (UID 10001/GID 1000, mode `0660`) lacked
  collector read access. The live ACL repair preserves ownership and grants
  only the required parent traversal and checkpoint read permissions. No
  syscall trace was captured.

  Producer PR #33 (main `2d0ebc3`) grants the trusted collector UID a numeric
  read ACL on the temporary checkpoint before fsync and replace; if ACL
  application fails, the old checkpoint stays intact. Consumer PRs #217/#218
  add an execute-only ACL for parent traversal without widening masks or
  changing ownership, and preflight the service identity before stopping the
  old service; symlinks are rejected and an absent checkpoint is allowed.
  Runtime diagnostics distinguish sanitized access, storage I/O, and lock
  contention while publication retries and capture continues. Producer tests
  (33 focused, 265 full), consumer tests (148 with 2 skips), and shared-image
  tests (318) passed with their static, lock-freshness, and CI checks.

  Shared-image PR #42 uses source
  `58fecbe3282683bb459aa405a4f953cdb2ac7445`. The control-only image
  `windmill-universal-worker:1.816.0-acl-58fecbe` was rebuilt and rolled to
  `worker-transcripts-control-1`; its digest is
  `sha256:5c834c0900e3f9573ab4d93d793284975831158b6f0b80197e97e3c6b6f1d125`
  (2,224,597,518 bytes), with the image revision label matching the full
  source SHA. The build-time and live-container ACL checks passed. The control
  container is running as UID/GID 1000:1000 with all capabilities dropped and
  zero restarts; the other worker containers kept their prior image IDs.

  The `f/omi/flat_ready` reader was published and its live source hash matches
  `3400388a9c8e3b1a38257a7dcbd3b4e6cfde35011c5f6b6fa1350d9dc5caceec`.
  Two producer-created scratch checkpoint replacements in the real work
  parent were read and checked by the actual collector identity (UID 996,
  GID 981, no supplementary groups); the scratch file was removed. The
  existing checkpoint was also opened and read by that identity. This confirms
  checkpoint access and state visibility; it does not confirm that any
  pre-existing publication backlog or archived audio was processed. The
  speech archive schedule remains disabled and its queue was empty during the
  checks.

## Release status

PR #215 merged at `25ee0169096774ccb7d9422b30ba29cbdbbebf2d`; PR and main CI
and CodeQL passed. Release PR #216 published `v0.11.16` at revision
`0d7b7ed90b1dc6d3a84d25c7b36e788401d3d3ef`. The deployment was verified with
zero restarts and readiness; private state files were mode `0600`.

Consumer PRs #217/#218 merged with green CI on main
`54b1254701b28272ccca10bada848f4d84a66bbc`, published as `v0.11.17`.
Release PR #220 published `v0.11.18` at
`ce8ac25ee6d52439b007ff2fd1e5f3570a0184f4`; the service is running that
release with PID 3767599 and zero restarts. Producer PR #33 and shared-image
PR #42 also merged with fresh CI. Candidate full unit (2,566 passed, 2
skipped), CRAP (2,566 passed, 2 skipped; 1,585 functions at or below 30),
static checks, Docker build, and runtime smoke passed. Finding 9's checkpoint
access contract is verified live. No claim is made that archived audio or a
pre-existing publication backlog has been processed.

## QA follow-up

The initial CRAP run's failure was in two source-mode assertions in
`tests/test_systemd_unit.py`, which compared worktree `stat()` permissions
with exact `0755` and `0644`. Git records only executable status (`100755` or
`100644`); a checkout can show source mode `0775` because of umask and still
match its tracked mode. The assertions now check Git index modes. Installer
safety checks remain exact: the wrapper is staged `root:root 0755` and sudoers
as `0440`. The source wrapper is restored to `0775`; the installed root-owned
wrapper remains `0755`.

- [x] **Source-mode gate correction.** The targeted correction passed 49 tests
  with 2 skips and Sol review accepted it; no installer permission assertion
  was weakened.
- [x] **Transient publication-retry test race.** The old test asserted the
  private retry-task reference immediately after an event that only marked the
  next attempt's entry; the consumer could legitimately clear the reference
  after completion. The test now waits for observable `ready_publication_waiting`
  before asserting two attempts and a cleared schedule, without sleep or an
  extra follow-up sequence. Sol accepted the test-only fix; the full quarantine
  module passed 60 tests in 1.30 seconds and the exact test passed in 0.47
  seconds. Production sources were unchanged.
- [x] **Operational state files must be private to the service account.**
  CodeQL reported a high-severity group-readable lock file at
  `src/omi_collector/capture/adapters/operational_status.py:303`. The snapshot,
  writer lock, and confirmed-loss ledger are private to the service UID at mode
  `0600`. The writer verifies regular-file ownership and obtains the exclusive
  lock before applying `fchmod` to the held descriptor. The writer and reader
  run as the same service UID. The focused permission and lifecycle tests
  passed (128 tests in 1.16 seconds). No directory or audio/ready permissions
  changed and no query was suppressed. PR #215 CodeQL passed after the fix.

## Operational context

The live service showed 16 crash-stack events and 17 restarts. Separately, 21
records were lost after BLE disconnect; the cause in stock firmware is
unconfirmed. This is retained as an unresolved device-side investigation, not
a ninth host-fix item or an established firmware cause.
