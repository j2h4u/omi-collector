# SQLite compaction plan

**Status: the implementation candidate at `2c5cbf1d3d915b600e8a50ab3c8384c580cf46ca`
passed live compaction and follow-on capture acceptance on 2026-10-15. The
final flat-configuration source candidate at `1a8e72aef1701d47bd86477a955321b1a43748fc`
passed all five local gates and was installed successfully at 15:12 UTC on
2026-10-15. Remote PR, CI, and release gates remain pending.**
The pre-compaction production snapshot measured 177.39 MiB, with 174.59 MiB on
SQLite's freelist and 2.8 MiB used.

The candidate Docker image is
`sha256:e1b498cc3e21e63744abd94d6ed0056f94f40bcd89de83371aa3141be9ae56f7`.
The retained local QA receipt is `/var/tmp/omi19-full-qa.y3MjI5gZ/ram-final-qa.log`.

The final source candidate requires `pendant_address` and
`bundle_min_audio_seconds` in the flat TOML config, preserves the optional flat
presence pair and its defaults, and rejects derived audio-threshold overflow.
`just verify` passed at `1a8e72aef1701d47bd86477a955321b1a43748fc`: `just check`,
`just crap-check`, `just unit`, `just docker-build`, and `just runtime-smoke`.
The CRAP and unit runs reported 2,463 passed and 5 skipped; CRAP passed for
1,968 functions. Their durations were 288.03 and 77.68 seconds. The built
image digest is
`sha256:002c1ceec5fc7de96b1a15e58af1e8efc2b3c02e9b0ea8da03c912a6f40b2fa1`;
the receipt is `/var/tmp/omi19-full-qa.y3MjI5gZ/flat-corrected-qa.log`. The
live receipt below records the first compaction on `2c5cbf1`; the final
flat-configuration candidate was installed and revalidated separately below.

## Live candidate receipt

The committed candidate was installed at 2026-10-15 14:51:58 UTC. The service
was running as PID 3603844 with zero restarts. Automatic state-machine
compaction completed within 52 seconds. The database changed from
186,011,648 bytes to 5,148,672 bytes (about 177.39 MiB to 4.91 MiB).
`PRAGMA quick_check` returned `ok`; the previously absent compaction marker
bootstrapped to generation 0 with `pending=false`.

All 4,812 `capture_record` payloads and device timestamps matched exactly
before and after; the single `capture_tail` also matched exactly. Inode, size,
and mtime remained unchanged for the 130 observed ready files. The existing
2.5 GiB ready payload was not read or hashed, so its content was not
independently compared. The captured receipts are
`/var/tmp/omic19-candidate.3Q32lFX7/before.json`, `after.json`, `deploy.log`,
and `verification.log`.

At 2026-10-15 14:53:22.673 UTC, the same process (PID 3603844, zero restarts)
completed the next pendant transfer: 1,429 records, 634,476 bytes, and
`unread=0`; sequence read/write was 1546/1429 and completed transfers reached
8. Service status remained healthy and all clear. This confirms capture after
completed VACUUM; it does not exercise capture arrival during VACUUM or
establish physical slow-disk timing.

## Final flat-configuration candidate receipt

The final source candidate was installed at 2026-10-15 15:12 UTC. The service
is active and running as PID 3734089 with zero restarts. The installed flat
`/srv/pipelines/omi/config.toml` contains the required `pendant_address` and
`bundle_min_audio_seconds = 3600` keys, with comments; the optional flat
presence pair retains its defaults.

Before/after state and the deployment log are captured in
`/var/tmp/omic20-candidate.H8yCtLs1/{before.json,after.json,deploy.log}`. Strict
snapshots matched all 8,146 `capture_record` timestamps and payload hashes, the
single 200-byte `capture_tail`, and inode/size/mtime for all 130 ready files.
The database is now 7,499,776 bytes; the increase from the first post-VACUUM
size reflects new small captures since that compaction.

## Decision

Run one full `VACUUM` after each actionable covered-record retirement cycle
has exhausted its eligible records, typically once or twice per day, with no
rate cap. Determine exhaustion from the authoritative database predicate.
`retirement_pending = false` alone is insufficient: it can mean the terminal source disappeared
while other sources remain. Do not require all sources to reach zero, wait for
an ACK, or trigger on a database-size threshold. Record capture intent before
readiness or joining maintenance; it makes VACUUM ineligible and interrupts an
already-running VACUUM. Actual GATT capture waits for the worker to settle. A
retained 200-byte tail and newly captured audio below the normal publication
threshold remain valid and must be preserved.

Do not enable `auto_vacuum=FULL` or `INCREMENTAL`, or add a dedicated
scheduler thread, settings, archives, manual CLI, or production priority
changes. Use the existing owned worker. SQLite-managed temporary space and
rollback journal may require up to about twice the original
database size in additional free space (roughly 355 MiB at the current size).

## State and ownership

Add explicit `Compacting` and `Quiescing` variants to the existing publication
state machine. `QuarantineMaintenance` remains the owner. Both presence-wake
branches must record capture intent before `ensure_publication_ready()` or
joining maintenance. Readiness must never admit a vacuum. Use the existing
device lease and `StateDB.access` with a worker-owned connection in autocommit
mode, outside a SQL transaction. The existing state RLock cannot be acquired
by a cancellation signal handler.

Persist one pending-compaction key in existing state. Set it atomically with
the actual `capture_record` DELETE and retirement-cursor commit. Include this
write in ambiguous-commit inspection. Keep it false after successful VACUUM;
bootstrap a missing key once from `PRAGMA freelist_count`. Clear debt only for
the compaction generation that completed: stale completion cannot clear newer
debt, while success that wins a cancellation race may clear its own debt.

## Lifecycle and event expectations

The existing owner mode axis is `AVAILABLE`, `CAPTURE`, or `CLOSED`. Compaction
and quiescing belong on the work-phase axis; they are not owner modes. The
cases below are illustrative, not the complete Cartesian transition table.
Implementation and tests must compare that complete mode × phase × event
table against an independently maintained expected matrix. Unknown events
fail closed.

| Owner mode | Work phase | Capture arrival | Shutdown or repeated cancel | Worker completion / storage event |
| --- | --- | --- | --- | --- |
| AVAILABLE | Idle / eligible | Record intent before readiness/join; make VACUUM ineligible | Enter CLOSED | Authoritative predicate may admit compaction only without pending capture intent |
| AVAILABLE | Compacting | Record intent, interrupt/join VACUUM, then begin GATT capture | Enter CLOSED; interrupt/join worker | Success clears matching debt; BUSY/LOCKED/FULL retains debt and uses existing backoff |
| AVAILABLE | Quiescing | Keep intent; wait for settlement before GATT capture | Enter CLOSED; join owned worker | Rollback/close or confirmed success settles worker |
| CAPTURE | Idle | Reject duplicate arrival | Enter CLOSED | Capture remains the active owner |
| CAPTURE | Compacting | Existing VACUUM must be interrupted and joined before GATT begins | Enter CLOSED; interrupt/join worker | Success clears matching debt; stale completion cannot clear newer debt |
| CAPTURE | Quiescing | Keep intent; wait for settlement before GATT capture | Idempotent; join owned worker | Rollback/close or confirmed success settles worker |
| CLOSED | Any | Reject | Idempotent | Ignore stale notification; never reopen work |

An arrival or shutdown request uses a `threading.Event`, SQLite progress
handler, and a pre-execute cancellation check. Cancellation interrupts and
joins the real worker; the worker rolls back if needed and closes its
connection before GATT capture is admitted. Repeated cancellation is safe. Full
VACUUM cannot guarantee prompt preemption: final copy, fsync, or rollback can
block. Do not impose an overall
short elapsed timeout or promise immediate ingest on this single-database
service.

## Failure handling

| Outcome | Required behavior |
| --- | --- |
| `SQLITE_BUSY`, `SQLITE_LOCKED`, or `SQLITE_FULL` | Retain debt, release ownership, and use existing backoff; never hot-loop. |
| Pending-marker write fails after VACUUM succeeds | Keep debt so a later pass can retry safely. |
| `SQLITE_IOERR` | Run existing fatal cleanup/reopen/validation path; stop if state is uncertain. |
| `SQLITE_CORRUPT` or `SQLITE_NOTADB` | Fatal. |
| Ambiguous retirement DELETE commit | Inspect the committed row and cursor plus marker atomically before deciding whether debt exists. |

## Implementation and acceptance milestones

- [x] Add the durable pending key and atomic retirement commit integration.
- [x] Add explicit states, owner lifecycle, admission rules, cancellation,
  worker join, and restart/bootstrap behavior.
- [x] Add independent complete transition matrix and guards for unknown events.
- [x] Test real SQLite file-size reduction and preservation of live content,
  including retained tail and below-threshold new audio.
- [x] Cover cancellation and admission/success races; BUSY/FULL/IOERR/corruption;
  pending-key clear failure; and SIGKILL/restart before and after VACUUM.
  `tests/test_publication_ownership.py::test_capture_interrupts_and_joins_compaction_before_visit`
  blocks the worker and verifies repeated cancellation and no GATT visit before
  join.
- [x] Exercise ambiguous retirement-DELETE commit reconciliation with
  failure injection. `tests/test_ready_sql_fault_recovery.py` covers exact
  cursor states (rolled back, committed, and third state) and final-source
  deletion followed by a commit error; the current cases assert marker
  generation 2 remains pending.
- [x] Reject an absent or `pending=false` marker after the proposed retirement
  cursor committed. The `marker_missing` and `marker_cleared` cases in
  `tests/test_ready_sql_fault_recovery.py` assert a fatal third state and a
  failed store without advancing publication.
- [x] Run the five local gates on the frozen candidate commit above:
  `just check`, `just crap-check`, `just unit`, `just docker-build`, and
  `just runtime-smoke`. The two pytest runs each reported 2,459 passed and 5
  skipped; CRAP checked 1,968 functions. Docker build and runtime smoke passed.
- [x] Install the committed branch candidate after local gates; verify service
  health, automatic compaction, database integrity, preserved capture data,
  and the complete subsequent pendant transfer.
- [ ] After live proof, open/update the PR and pass remote CI/release gates.

Physical slow-disk timing remains an optional follow-up, not a release gate.
The tested logical guard interrupts and joins the owner before GATT visitation;
the live follow-on transfer verifies normal capture after VACUUM, not arrival
while VACUUM is active.

## Source and SQLite references

Implementation touchpoints: `capture/adapters/ready_bundles.py` (retirement,
around line 4059 onward), `capture/adapters/staging_store.py` (around line
238 onward), both presence-wake branches in
`capture/application/quarantine_maintenance.py`,
`capture/application/owned_work.py`, and
`capture/domain/ready_machine.py`.

- [SQLite VACUUM](https://www.sqlite.org/lang_vacuum.html)
- [SQLite progress handler](https://www.sqlite.org/c3ref/progress_handler.html)
- [SQLite interrupt](https://www.sqlite.org/c3ref/interrupt.html)
