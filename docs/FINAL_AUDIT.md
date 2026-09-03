# Final audit

This checklist tracks the final cleanup of the collector. An item is complete
only when the implementation, focused tests, and relevant documentation agree.

## Repository hygiene

- [x] Link the quality-metrics documentation from an operator-facing document.
- [x] Describe the Gherkin scenarios accurately as acceptance specifications,
  unless they become executable tests.
- [x] Remove unused compatibility aliases and their obsolete tests and exports:
  `ingest_read`, `read_snapshot`, `StagingWriterAdapter`,
  `StreamingStagingWriter`, `RingTransportDisconnected`, and
  `WriterShutdownTimeout`.
- [x] Remove disposable local QA caches after the work; retain the project
  virtual environment.

## Persisted state

- [x] Remove the unused durable-attempt branch and `commits.jsonl`; keep the
  streaming checkpoint as the single recovery model.
- [x] Remove the write-only `salvage-pending.json` marker.
- [x] Replace the unbounded, hash-chained firmware observation history with the
  smallest bounded state needed for operational metrics.
- [x] Remove the unused `recovery_leg` receipt field.
- [x] Minimize publication and retirement markers so they contain state only;
  do not duplicate coordinates or hashes available from canonical state.

## Runtime efficiency

- [x] Eliminate the second full read of `records.bin` during successful
  sealing and publication.
- [x] Hydrate and validate a resumed partial only once before the first pendant
  read.
- [x] Bound maintenance frequency so presence and retry loops do not repeatedly
  scan the complete staging and quarantine trees.
- [x] Prevent retryable quarantine publication failures from repeatedly reading
  the same large prefix without a cooldown.

## Final verification

- [x] Run focused tests after each completed slice.
- [ ] Reset the disposable schema-1 firmware metrics file during deployment;
  do not add a compatibility reader or migration for it.
- [x] Pass the complete repository gates on the release candidate.
- [ ] Give this checklist and the resulting diff to a fresh, context-free
  auditor; correct every unsupported checkbox and remaining confirmed issue.
