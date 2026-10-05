# Finite mutation queue reconciliation — 2026-10-05

The immutable full campaign remains `complete_unresolved`: 5,738 generated
mutants, with 4,009 zapped, 1,506 survived, 27 timed out, and 196 errored.
This follow-up reconciles only its finite 1,729-row review queue (the 1,506
survivors, 196 errors, and 27 timeouts). The authoritative queue ledger
partitions those rows into 799 strict native kills, 30 observed behavioral
failures, 801 source-equivalent cases, and 99 nonviable cases, with no pending
or unclassified rows. Observed failures are not kills. This queue accounting
does not revise the immutable campaign report or produce a new full-project
mutation score.

Native kills were credited only when the original source and stable mutation
identity matched, a clean run used the same ordered selector set, and the
strict pytest/JUnit outcome was a named behavioral failure. Equivalent and
nonviable cases remain separate dispositions. The final focused controls used
the pinned Gremlins runner with `lightweight_runner = false`, at most two
workers, and current public tests before historical selectors.

The candidate passed `just check`, `just unit` (2,307 passed, 2 skipped),
`just crap-check` (2,307 passed, 2 skipped; 1,483 functions under CRAP 30),
and `just docker-build`. The durable evidence bundle is
`/srv/omi-collector-mutation-audit/20261005/mutation-queue-20261005.tar.gz`
(49,413,657 bytes; SHA-256
`7582466baf367b43ffa3dae942c99356796054a65d5be56bf360d30f3c56c859`). It
contains the immutable inputs, source/selector plans, strict per-ID receipts,
and authoritative ledger. The ledger SHA-256 is
`abf68cdd5eac2c4ec707d845b26c4a54e4c7b8087bb7686d43b387182fc68cfa`.
