# Service audit tracker

This tracker covers the eight host-side findings from the service audit. Keep
every checkbox open until the root agent accepts the root-cause fix and its
targeted tests. A changed file alone is not completion.

## Findings

- [ ] **Older orphan draft crashes recovery.** An older orphan draft below an
  existing closure reaches `staging_store.close_orphaned_drafts` before
  `inspect_recovery` can classify it, raising `ReadyClosureError` and stopping
  the service. Preserve the recovery invariant that every eligible older
  orphan is classified and safely closed or quarantined before bundle
  finalization, without hiding corruption. Acceptance: recovery handles an
  older orphan beneath a later closure without crashing and retains explicit
  failure behavior for invalid state. Test the mixed-sequence regression and
  invalid-state case.
- [ ] **Status can report false `ok`.** `operator_status.window` may return
  `ok` while the service is away or drained and the window has zero completed
  transfers. Status must represent whether the window has evidence to assess
  and whether the collector is currently available. Acceptance: zero
  completions cannot produce `ok`; test away, drained, and successful-window
  cases.
- [ ] **Persistent attention loses reconstructed history.** Attention is
  reconstructed only from the current debug ring, so rotation can erase the
  evidence while the underlying condition remains. Persist the minimum durable
  attention state and clear it only through the established recovery
  condition. Acceptance: ring rotation preserves attention, and the defined
  recovery clears it; test both transitions.
- [ ] **Failed mutation receipt leaks job resources.**
  `mutation_campaign._new_job` creates a checkout and environment before the
  owner receipt is safely recorded. Make ownership durable before creating
  resources, or clean up every resource if receipt creation fails. Acceptance:
  injected receipt failure leaves no unowned checkout or environment and does
  not misreport job ownership; test the failure path.
- [ ] **Owner control server can hang on a client.**
  `OwnerControlServer` silently receives unbounded data and its thread can
  outlive the test's timeout, masking the actual failure. Bound and validate
  client input, make shutdown own and join the server thread, and ensure test
  timeouts cover the server lifecycle. Acceptance: oversized or stalled input
  is bounded, and teardown reliably joins the thread; test both cases.
- [ ] **Runtime smoke suppresses cleanup failure.** The Justfile runtime-smoke
  cleanup uses `down || true`, hiding failed teardown. Preserve the original
  smoke result while surfacing cleanup failure when no earlier failure exists.
  Acceptance: successful smoke plus failed cleanup fails; an earlier smoke
  failure remains visible. Test both command outcomes.
- [ ] **Supply-chain check skips `FROM --platform`.** The Dockerfile pin check
  misses image references with a platform qualifier. Parse the platform form
  and apply the same immutable-image requirement. Acceptance: pinned qualified
  images pass and unpinned qualified images fail; test both forms.
- [ ] **README rewrites confirmed time as approximate UTC.** The clock
  description reports confirmed timestamps as approximate. Keep confirmed
  clock ranges distinct from unconfirmed device time and describe each
  accurately. Acceptance: documentation matches the timestamp semantics and
  does not label confirmed UTC as approximate.

## Operational context

The live service showed 16 crash-stack events and 17 restarts. Separately, 21
records were lost after BLE disconnect; the cause in stock firmware is
unconfirmed. This is retained as an unresolved device-side investigation, not
a ninth host-fix item or an established firmware cause.
