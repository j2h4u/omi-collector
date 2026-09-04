# Final audit

This checklist tracks the accepted findings from the final repository audit.
An item is complete only when implementation, focused tests, and durable
documentation agree.

- [x] Make release-please PRs trigger the same required gates and merge without
  a manual reopen.
- [x] Make a successful CodeQL check prove that analysis was uploaded.
- [x] Build deployments in a staged environment and restore the previous
  working release after any failed readiness check.
- [x] Process large durable prefixes with bounded buffers instead of loading
  complete recordings into memory.
- [x] Authenticate terminal evidence once; do not rehash retired recordings
  while admitting new pendant reads.
- [x] Bound quality-metrics storage and keep its durable writes off the BLE
  event loop.
- [x] Retire only the exact disposable schema-1 firmware metrics document
  during deployment and fail closed for every other unexpected file.
- [x] Make the documented default installation writable under the hardened
  systemd unit without an omitted host-specific step.
- [x] Pin remote GitHub Actions and container images to immutable revisions and
  make the repository gate reject mutable references.
- [x] Enable the public repository's available secret scanning, push protection,
  and private vulnerability reporting. Validity checks are unavailable on the
  current GitHub plan.
- [x] Remove unused compatibility surfaces and private downstream/storage
  conventions from the public collector documentation.
- [x] Pass focused checks for every slice and the complete repository gates on
  the release candidate.
- [x] Retry soft scanner failures while a drained, absent pendant is waiting to
  return; never leave the service alive but unable to discover it.
- [x] Keep the pendant BLE address out of the system journal.
- [x] Hash operator metrics input as a bounded stream instead of loading a
  near-full pendant recording into memory.
- [x] Remove the duplicate network-dependent release-please updater test; keep
  the deterministic local release contract gate.
- [x] Make the documented first installation select a root-owned sealed release
  before the service can start.
- [x] Redact BLE addresses from nested transport errors and local mismatch
  diagnostics without hiding the useful low-level error chain.
- [x] Retry interrupted READ timeouts without adding a second seal-timeout
  policy that differs only in tests.
- [x] Remove the dead integration marker and unused checkpoint helper.
- [x] Redact both human-readable and BlueZ object-path forms of BLE addresses.
- [x] Keep soft scan-start cleanup failures inside the bounded presence retry
  loop.
- [x] Remove inert runtime environment and mount directives from the systemd
  unit, and document the required first successful deployment.
- [x] Keep deployment staging under a root-owned parent so the service account
  cannot replace a sealed candidate before selection.
- [x] Preserve a due rapid retry while restarting a failed presence scan.
- [x] Run a whole-service adversarial review through the repository cross-AI
  script with `--all`; correct every confirmed issue.
- [x] Give this checklist and the resulting diff to fresh context-free
  auditor; correct every unsupported checkbox and confirmed issue.

The audit explicitly rejected queued journald reporting, a new shutdown
subsystem without an observed timeout, BLE-in-Docker end-to-end testing, a
ports-and-adapters rewrite, automatic checkout selection, and enterprise
governance machinery. Their cost is not justified by current product evidence.
The final whole-service panel also rejected shrinking the in-memory transfer
arena, automatic cleanup after `SIGKILL`, and optimizing operator-initiated
deployment downtime: available RAM, explicit scope, and the pendant's durable
buffer already make those changes unnecessary.

## Next slice

- [ ] Replace the presence scheduler's interacting flags and mutating checks
  with one immutable state and pure event transitions. Keep the BLE session and
  download protocol unchanged.
