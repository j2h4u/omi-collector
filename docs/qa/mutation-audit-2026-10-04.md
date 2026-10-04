# Mutation audit record — 2026-10-04

The original native campaign completed with exit status 0 against commit
`545bbe71aaf19e38edcfc17d266747f275740b6f`. It generated 5,707 mutants across
66 source files (60 mutated): 3,786 were zapped, 1,632 survived, 91 timed out,
and 198 errored. The campaign is `complete_unresolved`; exit status 0 does not
resolve its errors or timeouts and does not certify the full audit.

Follow-up native runs targeted selected files and their designated tests. Their
receipts reconcile 133 original survivors killed in those scopes. Sixteen
corrections kills used a pre-final fixture identity and are retained with that
caveat. These scoped results do not establish a new full-project survivor
total; the reconciliation check, for example, matched all 67 original
survivors for one source and killed 3 in its two-test scope.

The source map covers all 66 source files, with 60 mutated and 55 selected test
files. The evidence bundle is stored at
`/srv/omi-collector-mutation-audit/20261004/mutation-audit-20261004.tar.gz`;
its manifest records the original archive checksum and hashes for the compact
receipts, source map, and runtime-control evidence. The original native cache
and report are preserved in the bundle. This record does not claim a clean
full-project mutation audit.

On integrated commit `e05e9967a2d74a0aefd3925d893bb9734c0daa8e`, `just check`
passed, `just unit` passed with 1,519 tests and 2 skips, and `just crap-check`
passed for 1,483 functions at threshold 30. The Docker build passed earlier on
unchanged production sources; it was not repeated for this test-only
integration.

## Independent full validation and focused controls

A later full native run completed with exit status 0 against commit
`f6a525308b3cdb7492384d5bb89c38036934b03e`. It generated 5,738 mutants
across 67 source files: 4,009 were zapped, 1,506 survived, 27 reached the
Gremlins 150-second mutant budget, and 196 ended in pytest or collection
errors. The campaign state is `complete_unresolved`; these results are a
separate run, not an additive update to the earlier 5,707-mutant campaign.

The unresolved inventory has 223 records: 71 pytest timeouts without JUnit
reports, 115 pytest errors without completed test outcomes, 10 pytest
`INTERNALERROR`s without completed outcomes, and 27 Gremlins budget timeouts.
The saved subprocess output is truncated by the pinned runner, so this
inventory does not assign unobserved causes. The final run archive is
`/srv/omi-collector-mutation-audit/20261004/mutation-audit-final-validation-20261004.tar.gz`
(SHA-256 `205b391e4e204512b8e5d916bd282653eed3024bf9566f0081f922d3d6e2dde4`);
it preserves the raw report, campaign receipt and log, databases, runtime
readback, exact source/test snapshot, and unresolved inventory.

Four accepted focused controls changed selected outcomes: BLE `g173` and
`g174` and quality adapter `g005` had timed out, then completed naturally as
`SURVIVED`; batch `g003` changed from `SURVIVED` to `ZAPPED` with its original
pytest configuration and all 52 selected tests. These are narrow test-control
results, not a new full-project total and not arithmetic reductions from the
223 unresolved records. Their receipts and review plan are preserved in the
supplementary remediation archive below.

One additional selected replay addressed `collector_5849_g028`, an original
pytest-timeout/no-JUnit outcome. After adding TaskGroup ownership to the
progress-pump tests, the exact five-selector control completed: clean source
`SURVIVED`, exact `return`→`None` mutant `ZAPPED`, with five completed test
failures and no JUnit errors. This is one mutant outcome transition
(`ERROR`→`ZAPPED`), not five kills. The run used the immutable Gremlins pin
`073a5e8d4e0239f3c3b468946a0d8469a510c69b` and retained the original
120-second/thread pytest timeout and 150-second mutant subprocess budget.
The archived full campaign report and its 223-row unresolved inventory remain
unchanged; this scoped replay is recorded separately. The exact BLE
notification-wakeup replay for `bleak_transport_c276_g057` is accepted:
the clean source `SURVIVED` and the mutant `ZAPPED` under the same seven
selectors, with four completed test failures and no JUnit errors. This is one
additional `ERROR`→`ZAPPED` transition. The old no-JUnit error remains
preserved separately. Including the accepted batch control above, seven
selected outcome transitions are accepted: three `TIMEOUT`→`SURVIVED`, one
`SURVIVED`→`ZAPPED`, and three `ERROR`→`ZAPPED`.

The accepted bounded-readiness changes for `presence_40d1_g059` passed the
exact 30-selector clean control. Two exploratory mutant runs invoked pytest
without Gremlins' existing `-x` flag and timed out at 150 seconds; those
all-selected runs are retained as noncanonical diagnostics. The final control
used the installed Gremlins `_build_test_command` with its native fail-fast
flag, unchanged strict pytest configuration, 120-second/thread timeout,
150-second mutant budget, source import guard, and the same 30 selectors. Its
clean source `SURVIVED` in 2.480 seconds; the exact mutant was `ZAPPED` in
6.419 seconds with completed JUnit (one failure, no errors). This is one
`ERROR`→`ZAPPED` mutant outcome; the failed test is not counted as an
additional kill. The immutable full-campaign report and its 223-row
unresolved inventory remain unchanged. The supplementary remediation archive
is `/srv/omi-collector-mutation-audit/20261004/mutation-audit-remediation-20261004.tar.gz`
(5,011,164 bytes; SHA-256
`44a5a73b6e112e4801af72650d5bf2364affe12c0ada06cc350c7b8dd4ecb954`). Its
gzip integrity and all 409 internal SHA-256 entries were verified.

On the final integrated test tree, `just check` passed, `just unit` passed with
1,519 tests and 2 skips, and `just crap-check` passed with 1,519 tests, 2
skips, and 1,483 functions under the threshold 30. `just docker-build` passed
after using a task-specific Docker configuration directory because the
default Docker configuration path is read-only in this environment.
