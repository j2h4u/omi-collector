# SQLite compaction plan

**Status: design reviewed; implementation and deployment have not begun.** The
production database is healthy. The current file is 177.39 MiB, with 174.59
MiB on SQLite's freelist and 2.8 MiB used. Production has not run `VACUUM`.

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

- [ ] Add the durable pending key and atomic retirement commit integration.
- [ ] Add explicit states, owner lifecycle, admission rules, cancellation,
  worker join, and restart/bootstrap behavior.
- [ ] Add independent complete transition matrix and guards for unknown events.
- [ ] Test real SQLite file-size reduction and preservation of live content,
  including retained tail and below-threshold new audio.
- [ ] Cover repeated cancellation; admission, arrival, and success races;
  slow disk; BUSY/FULL/IOERR/corruption; ambiguous DELETE commit; pending-key
  clear failure; and SIGKILL/restart before and after VACUUM.
- [ ] Run applicable project gates and runtime smoke on a frozen candidate.
- [ ] Install the committed branch candidate only after applicable local gates
  pass; confirm live health and content preservation.
- [ ] After live proof, open/update the PR and pass remote CI/release gates.

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
