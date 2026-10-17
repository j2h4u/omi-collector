# Best practices

Keep this project a small capture boundary. The collector owns presence, BLE
transfer, recovery/quarantine, clock normalization, and atomic audio
publication. Consumer-specific processing and external service integration are
outside its boundary.

## Quality gates

Use `uv` and keep `uv.lock` synchronized. The required gates are:

- `just check` — static, type, import, dependency, workflow, compile, and
  packaging checks;
- `just crap-check` — the authoritative radon-backed CRAP threshold for every
  function;
- `just unit` — behavior tests;
- `just docker-build` — Dockerfile, Compose, and image validation;
- `just runtime-smoke` — a bounded installed-CLI container smoke;
- `just verify` — the complete local contract when a local full run is needed.

Do not weaken or locally suppress a gate. Use targeted checks while iterating.
Before a local candidate install, all applicable local tests and linters must
pass. Complete the five gates above or reuse retained results only when their
relevant inputs are byte-identical and targeted checks cover an isolated later
change. Freeze and commit the exact candidate. A reviewed transactional entry
point may install it for live feedback while preserving production stop,
migration, rollback, and storage-integrity safeguards. Record it as a host
candidate, not a released version. Remote CI, PR, security, merge, and release
are not prerequisites for this local installation. After live feedback, push
the accepted change and pass the existing remote gates before protected-main
merge; the GitHub release can follow in the background. See [edge.md](../edge.md)
for the short workflow sequence.

Runtime QA must run serially at low CPU and idle I/O priority. The canonical
`just unit` command is bounded to 600 seconds; set
`PYTEST_XDIST_AUTO_NUM_WORKERS=1` so its pytest-xdist invocation uses one
worker. Keep the tests and assertions intact.

For full QA on a slow disk, put only temporary test scratch in a short,
task-owned `TMPDIR` on tmpfs; keep logs and receipts on persistent storage.
pytest-xdist's Unix socket path, including its generated suffix, must fit the
Linux 108-byte `AF_UNIX` path limit. A prior 117-byte path caused the harness
to time out at 600 seconds; using a shorter `TMPDIR` let the unchanged gates
pass. Remove the run's owned scratch directory after completion. Keep diagnostic
logs only while investigating failures or recording acceptance, then delete
them once the useful result is recorded in WorkGraph. Preserve reusable mutation
campaign results; they are not disposable test scratch.

Git tracks only whether a source file is executable (`100644` or `100755`),
not its full worktree permission bits. Tests of source-file executability must
read the Git index mode instead of comparing `stat()` with exact `0755` or
`0644` permissions that umask can change. Keep installer safety checks exact:
the installed wrapper must be `root:root 0755` and sudoers files `0440`.
Correcting a mistaken source-mode assertion must not weaken these installed
permission checks.

## Mutation testing

Run `just mutation` for a separate full-project behavioral audit: mutate
`src/omi_collector` and `scripts`, and collect the complete `tests` suite,
including slow tests. It uses two pytest-gremlins workers and keeps strict
pytest checks. The dependency is pinned to a reviewed fork commit containing
upstream PR 522 plus configurable timeouts, durable partial caching, coverage
failure diagnostics, native module/package metadata preservation and POSIX process-group cleanup. Timed-out test processes
and descendants in their process group must stop before the next mutation runs.
`lightweight_runner = false` is required. A successful native coverage pre-scan
is saved with an input fingerprint so later timed windows can reuse the exact
coverage map; the full-suite prescan has a fixed 600-second budget independent
of the per-mutant timeout. A failing baseline stops before coverage collection
or mutation dispatch, preserving the original pytest failure. Start a new
audit with `just mutation fresh` after preserving prior evidence; ordinary
`just mutation` resumes only a campaign with matching clean committed inputs.
The released runner timeout uses a canonical cache-key form across integer TOML values and
equivalent integral CLI floats, while fractional timeouts retain exact values.
lightweight runner can falsely kill mutations when fixtures or parametrization
are involved. A regression canary checks that unrelated mutations survive. Mutation workers
must run serial pytest: xdist workers do not inherit the import hook. The
command clears `PYTEST_ADDOPTS` to prevent implicit parallelism or test filters.
Before trusting a new runner revision, run the complete instrumented suite with
no active mutation; module metadata and imports must behave like the original
code. A behavioral mutation must be killed and an equivalent mutation must
survive. Console, HTML and JSON reports describe surviving mutations.
Review them for missing observable behavior, not just a higher score. Equivalent
mutations, cosmetic messages and implementation-only changes are not reasons
to add brittle assertions. Mutation testing does not replace complete
state/event matrices, effect-boundary scenarios or existing release gates.
Baseline coverage failures retain their complete logs on disk while console
output stays bounded.

The command sets `COVERAGE_CORE=ctrace`: Gremlins uses dynamic test contexts,
which require this coverage backend on our Python 3.14 stack. The pinned fork
also selects a compatible tracer for its private coverage subprocess, so direct
Gremlins invocations retain distinct test contexts. Defaults, annotations, decorators
and module/class initialization require the complete test suite because their
effects outlive the test recorded by line coverage. Ordinary callable bodies
retain coverage-guided selection. Parametrized node IDs must remain intact;
an unmapped coverage name falls back to the complete suite instead of silently
dropping a test. Coverage collection remains separate
from the CRAP coverage gate. A fresh audit clears the cache only when the
operator explicitly chooses `just mutation fresh` after preserving prior
evidence. Default `just mutation` resumes only a matching incomplete campaign
after its recorded process IDs and start times are absent. Completed mutants
are saved immediately. A timeout or untested mutant needs investigation; it is
not confirmation that tests caught the intended behavior. Errors are reported
separately from killed mutants. AST operators do not replace enum
references in transition tables; independent complete state/event expectations
remain necessary for those decisions.

For a finite follow-up, freeze the original campaign report and account for its
selected IDs in a separate ledger. Join by source identity and stable AST
mutation identity, pair each mutant with a clean run using the same ordered
selectors, and credit only strict pytest/JUnit behavioral failures. Keep
observed failures, timeouts, equivalents, and nonviable mutations distinct; a
completed ledger is not a new full-project score.

Unit, coverage, CRAP and mutation commands use CPU niceness 19 and Linux
`ionice` idle class; child test processes inherit both priorities. Ordinary
pytest commands remain bounded to 600 seconds. The canonical full-project
mutation campaign uses one finite 24-hour GNU timeout with TERM and a
five-second KILL grace; this is an operating budget, not a completion promise.
Targeted mutants retain their 150-second budget and the independent coverage
pre-scan retains 600 seconds. Declaration-time mutants and any mutant whose
actual selection is the complete suite use a separate 600-second budget. A
retained clean baseline collected 2,298 tests (2,296
passed, 2 skipped) in 287.25 seconds at nice 19/idle I/O; doubling that measured
duration for two workers leaves about 25 seconds of margin. This diagnostic
baseline is not a native mutation result. Earlier serial g082 benchmarks took
103.67–104.88 seconds with different diagnostic flags, and the cause of the
timing difference is unconfirmed. A native two-worker attempt timed out at
150.57 seconds after pytest had run 148.85 seconds. Selecting `tests/` instead
of all 2,294 IDs saved only 1.21 seconds in that older run. The full-suite
budget applies when behavior is evaluated outside a callable body or coverage
selection falls back to all collected tests. The recipe retains the complete timeout and runner
log and records a small identity/start/end/report receipt, then reconciles native generated IDs
against the fresh native JSON report. Missing or malformed report metadata,
identity changes, nonzero exits, duplicate/missing/foreign IDs, and unresolved
errors or timeouts cannot certify a clean audit. Status 137 is fail-stopped and
is never retried automatically. There is no mutation score gate, and mutation
testing remains separate from `just verify`.

The audit runs from one detached snapshot with a per-job frozen uv environment
and cache under `/srv/omi-collector-mutation-audit/jobs`. Its control socket is
in a per-user mode-0700 directory under `/tmp`; the exact endpoint is recorded
in the private job receipt.
`just mutation fresh` creates a new job without deleting prior evidence;
`just mutation` resumes only one compatible, verified paused or interrupted
job. Use `just mutation-status` to inspect it and `just mutation-pause` to
request a checkpoint. The owner stops and reaps the controller and its
token-owned test processes, verifies the SQLite cache, records an interrupted
checkpoint, and exits; no stopped test process remains. Resume starts a new
controller so any in-flight mutant is retried. A failed baseline with zero
mutants dispatched is recorded as a failed bootstrap, never a completed audit.
When several paused jobs are preserved, select one explicitly with
`just mutation resume --job <job-id>`; the default remains fail-closed when
more than one resumable job exists.
The owner and all children run at SCHED_IDLE CPU policy, ionice idle class, and
nice 19. The 24-hour GNU timeout bounds the inner native test run, not the
owner, and a timeout or signal is an interrupted audit rather than a clean
mutation result. Each job retains its snapshot and evidence for later review.

## Capture safety

Keep runtime SQLite writers on `BEGIN IMMEDIATE` so status and other readers can
see committed data during audio transfers; reserve `BEGIN EXCLUSIVE` for schema
initialization and recovery. Retry only a direct `SQLITE_BUSY` at admission or
commit, and never replay DML after a commit error.

Keep physical-download policy in the pure `visit_machine`: it alone authorizes
drained, interrupted, restart, or operator-limit closure. Session, presence, and
storage code supply facts or execute its commands; they do not choose a second
closure policy. Test event traces independently from BLE and filesystem tests.
Only a fresh final zero-unread INFO with settled reconciliation, successful
teardown, and checkpoint can produce `DrainConfirmed`. Publication uses a
separate durable FIFO; its retries do not change the current visit state.

Model lifecycle decisions as finite typed states and events, with explicit
rejection of invalid pairs. Keep effects outside pure transitions and advance
only after their results are acknowledged. Reconstruct durable state from
authenticated filesystem evidence after restart; an in-memory phase is not a
durability receipt. Preserve audio when clock metadata or optional telemetry
fails.

For each lifecycle model, enumerate all representative state/event classes,
check allowed and invalid transitions, reachability, and paths to completion.
Also test the real driver at effect boundaries: failure, timeout, cancellation,
and restart. Graph reachability alone does not guarantee progress: retry loops
depend on explicit timer, availability, and fairness assumptions. No FSM
library replaces these contracts; prefer the existing pure reducers unless a
dependency removes concrete complexity.

Pin each model's declared event, work, and result inventories alongside its
transition expectations. Cover every declared state/event cell, or state the
bounded representative coverage and its declared unions explicitly; assert
unsupported variants are rejected rather than silently taking a default
branch. This guarantee covers the declared finite variants, not arbitrary
payloads or future undeclared cases. Keep the inventory fixtures tied to the
model's enums and unions so new variants force a test update.

Foreground attempt and closure effects own priority over background publication
retries. Join any running retry before admission and defer new retries until
the foreground scope exits. Inline clock publication uses the explicitly
transferred in-process clock lease; it must not acquire another filesystem
lock. The stable outer `collector.lock` remains held for the entire invocation.

`INFO` is the source of truth for unread state. Automatic service and sync
admission requires a current exact-address scanner candidate that has remained
visible for the configured stable-arrival span; the default is 30 seconds with
no 10-second gap. Timers and remembered addresses never authorize GATT work.
Serialize attempts per pendant and stop this collector's scanner before GATT
work. Explicit probe, info, and confirmed collect commands remain direct
operator paths.

Keep the connected-session preflight budget long enough for the bounded host
clock trust probe, clock ledger reconciliation, and optional BLE observations.
The default is 10 seconds; an explicitly shorter budget still takes priority.

Before `READ`, admit enough disk space for the bounded batch and ensure staging
metadata is durable. Write record bytes and checkpoints with `fsync`; publish a
sealed bundle with an atomic rename. On restart, accept only one authenticated
partial, verify replayed overlap byte-for-byte, and quarantine malformed,
conflicting, or ambiguous evidence.

Never expose `CLEAR` or issue a blind `ADVANCE`. A firmware `READ` can advance
the pendant's cursor while notifications are acknowledged, so a physical read
is potentially consuming. If history is unavailable or cannot be proven, keep
only aggregate diagnostics and never create a fake audio identity.

The temporary weak-RF PHY guard is controller-global. Snapshot and restore the
exact selected set on every exit path, including cancellation and recovery.

## Runtime operations

systemd is the production supervisor. The public unit runs as the dedicated
`omi-collector` system user. Keep the checkout root-owned and readable; keep
`/srv/pipelines/omi/config.toml` root-owned and readable by both the system
collector and the user-owned pipeline, mode `0644`; and keep the service-local state directory
`omi-collector:omi-collector` mode `0750`.

Initial setup is deliberately small. Keep the checkout root-owned under
`/opt`, copy and edit `config/config.toml.example` at
`/srv/pipelines/omi/config.toml`, then run
`sudo scripts/install-systemd-unit.sh`. The installer creates or validates the
system account and state ownership, protects the configuration, validates the
staged unit, and restores the prior unit if installation cannot be reloaded or
enabled. It never starts the service without `--restart`.
It also provisions the fixed Collector data roots as
`omi-collector:j2h4u` mode `2750`. The setgid bit keeps new ready bundles and
service-created files in the operator's read group.
The operator group must also have a same-named operator account: stopped
migration checks that account's effective read-only access before completion.
Each deployment reapplies these root modes and sets the SQLite state database
to `omi-collector:j2h4u` mode `0640`; the singleton lock remains private
`omi-collector:omi-collector` mode `0600`. This operator access is read-only:
service control and deployment use their separate bounded commands. Older
nested data and retained root-owned recovery packages require a one-time,
audited group-read repair; do not recursively chown unrelated pipeline state.
The notification boundary is the single atomic
`collector/public/pendant-drain.json` projection. Provision `collector/public`
with the same service owner, operator read group and mode `2750`; its JSON
file is `0640`. A downstream container mounts only this directory read-only,
without the private database or other Collector roots. On this host the public
directory owner/group are numeric `996:1000`; application code derives paths
and inherits the provisioned group rather than hardcoding these identities.
The completion receipt lives in SQLite and is exported again after restart.
Its ID denotes a verified empty ring after accepted durable custody, including
the existing quality-loss classification. Repeated empty observations do not
create new completions; later unread data retains the previous receipt.
Notification delivery, consumer deduplication and mounting belong to the
downstream service.
The installed sudo policy permits `j2h4u` to start, stop, or restart only
`omi-collector.service`, alongside the existing fixed status and release
wrappers.

The installer also installs the root-owned
`/usr/local/sbin/omi-collector-deploy-release` wrapper and its fixed sudo
policy. The wrapper accepts exactly one `vMAJOR.MINOR.PATCH` tag and delegates
to the checked-in transactional `/opt/omi-collector/scripts/dev-deploy-release.sh`;
it does not perform clock recovery or accept arbitrary commands. Run a
reviewed release as:

```bash
sudo -n /usr/local/sbin/omi-collector-deploy-release v0.3.0
```

The configuration is strict and contains `[pendant] address`, plus optional
`[presence]` and `[ready]` sections. `[ready]` accepts `target_audio_seconds`
(3600 seconds by default). Only a confirmed complete
pendant drain permits packaging, and only when the cumulative captured Opus
audio reaches that minimum. All accumulated eligible drafts then form one
bundle; crossing the minimum during downloading never splits the visit.
Smaller downloads accumulate across visits and restarts indefinitely. Calendar
gaps do not count toward audio duration; no age-based or manual flush exists.
Interrupted closures preserve data but do not authorize packaging.
Admission of a new capture visit durably revokes the previous drain permit
before BLE work. Clock publication and restart recovery cannot reuse that stale
permit; a new confirmed drain grants permission again at the retained frontier.
Its fixed parent `/srv/pipelines/omi` is the storage root; `collector`,
`draft`, and `ready` are derived beneath it. The base unit grants the
service write access to that root. Use filesystem ownership or ACLs on the
derived directories when a downstream account also needs access.

### State ownership and state initialization

All mutable structured Collector state lives in
`collector/collector-state.sqlite3`, using SQLite's DELETE rollback journal,
`synchronous=EXTRA`, and a bounded busy timeout. Private attempt, draft, and
quarantine audio and its recovery evidence live in typed tables in that same
database. Public ready bundles remain immutable filesystem artifacts containing
only `records.bin` and `manifest.json`. Keep the separate, zero-content
`collector/collector.lock` stable: the service holds its flock for the full
invocation, and every state-mutating manual command and stopped-service
initialization takes that same guard before opening writable state. Read-only
`device status` and `device metrics` may read the database while the service is
active.

The deployer checks state before stopping the service and repeats the check
under the stable guard after stop. A verified hot rollback journal is recovered
by SQLite under that guard, then a read-only check must accept the recovered
state. The selected release's `omi-collector init-state --config PATH` creates
v2 only for an empty installation and validates existing v2 without rewriting
it. Missing databases alongside READY, private audio, control data, or unknown
scratch fail closed. Existing v1 databases are refused unchanged; this command
never imports or deletes old data. Runtime also refuses v1 and never creates a
replacement database. Keep the service stopped while repairing an incompatible
state, and preserve the stable lock inode through deploy and recovery.

Before restarting the previous release after an interrupted deployment, its
own interpreter must validate the current publication ledger read-only. Older
readers reject the bundle-date fields introduced after they were built. If
compatibility cannot be established, leave the service stopped for forward
recovery; never strip dates or restore an older state database.

A mismatched canonical record or checkpoint stays in blocked, unclassified
quarantine custody without an expiry deadline. Ordinary salvage and terminal
classification must leave it untouched until an explicit custody resolution;
only terminal residual evidence may expire. Canonical records remain pinned by
their independent publication references. READY materialization that observes
`EIO` must durably latch its stage, preserve source rows, and refuse later
publication retries. If that refusal cannot be recorded, the collector exits
with status 78 so systemd does not restart it into another capture attempt.
Confirmed SQLite `CORRUPT` or `NOTADB` is likewise fatal; transient read faults
remain retryable.

The retained ready-bundle ACK checkpoint under `work` is a real publication
dependency. The producer must grant the trusted collector UID a numeric read
ACL on each temporary checkpoint before fsync and replace, preserving the old
checkpoint if that step fails. The consumer needs traverse permission on the
parent; provision only the required execute ACL with the established helper.
Do not grant blanket read or default ACLs and do not change ownership. Before
stopping the old service, preflight the service identity's checkpoint access;
reject symlinks and allow an absent checkpoint. Surface sanitized access,
storage-I/O, and lock-contention failures separately. An unavailable
checkpoint blocks publication and retries, while collection continues.
After deployment, verify parent traversal and checkpoint reads as the actual
consumer identity, including its supplementary groups. Keep dependent
processing disabled until those checks pass; checkpoint readability alone
does not show that an existing publication backlog was processed.

After installing the unit, run `sudo scripts/deploy-systemd-service.sh`. It
checks the installed unit and fixed operator configuration, records the full
source revision, builds the environment as the dedicated build account, and
copies dependencies into it. It then seals the selected release as root-owned under
`/var/lib/omi-collector-deployments`. The first successful deployment creates
the `current` selector used by the fixed unit command.

Enable BlueZ and verify its normal user-level access with
`sudo -u omi-collector bluetoothctl show`. The default long-running sync keeps
`--force-1m` off. If an explicit weak-RF fallback is required, allow only the
exact `/usr/bin/bluetoothctl --timeout 5 mgmt.phy` forms needed by the service
through a dedicated `visudo -f /etc/sudoers.d/omi-collector-bluetoothctl` file.
The base allowlist is the no-argument query plus `LE1MTX LE1MRX` and
`LE1MTX LE1MRX LE2MTX LE2MRX`; add a different sequence only after observing it.
Do not grant general `bluetoothctl`, shell, or unrestricted sudo access.

Docker is for packaging and runtime QA only; it is not a production Bluetooth
supervisor. Keep production state and publication roots configured explicitly,
and restrict their permissions. Do not put raw audio, credentials, BLE
addresses, or detailed live device observations in documentation or public
logs. Keep the private diagnostic ring access-restricted.

## Observability

### Pendant missing while nearby

When the operator confirms the pendant is nearby and switched on, check host
Bluetooth before asking them to power-cycle the pendant. First check
`systemctl is-active bluetooth.service` and `bluetoothctl --timeout 5 show`:
the service must be active and the controller powered. Start an inactive
service with `sudo -n /usr/bin/systemctl start bluetooth.service`; enable a
disabled controller with `bluetoothctl --timeout 5 power on`. Those flags, `Discovering: yes`,
and cached devices do not confirm that fresh BLE advertisements arrive.

Read `sudo -n /usr/local/sbin/omi-collector-status` first. If it shows fresh
transfer progress, leave collection running; do not scan or restart Bluetooth.
For an idle discovery investigation, stop the collector gracefully and count
fresh BLE devices in a bounded independent scan from the maintainer checkout:

```bash
sudo -n /usr/bin/systemctl stop omi-collector.service
timeout --signal=TERM --kill-after=5s 20s uv run --frozen python -c \
  'import asyncio; from bleak import BleakScanner; print(len(asyncio.run(BleakScanner.discover(timeout=10))))'
```

If known nearby BLE devices should be advertising but the count is zero, or
the probe errors or times out, investigate host Bluetooth. If other devices
appear, investigate the pendant's power, current address and other connection
instead. Restore the collector after the investigation in either case.
After a successful stop, always run
`sudo -n /usr/bin/systemctl start omi-collector.service` before leaving,
including after probe or recovery failure, or if unrelated connections prevent
recovery. Leave those unrelated connections intact.

For a stalled discovery stack, check `bluetoothctl --timeout 5 devices Connected` for
unrelated connections before restarting Bluetooth; the restart disconnects
them. With no unrelated connections, recover the service and repeat the
bounded scan before restarting the collector:

```bash
sudo -n /usr/bin/systemctl restart bluetooth.service
bluetoothctl --timeout 5 power on
# Repeat the bounded scan above, then restore normal collection.
sudo -n /usr/bin/systemctl start omi-collector.service
```

Verify fresh discoveries and then new collector advertisements or transfer
progress. A successful restart alone does not confirm recovery. If discovery
still fails, inspect the Bluetooth and kernel journals before attributing the
failure to the pendant. Bound reads with
`sudo -n journalctl -u bluetooth.service -b -n 40 --no-pager` and
`sudo -n journalctl -k -b -n 80 --no-pager`.

### Status and logs

Use the system journal for the small operational stream. The default `INFO`
level shows readiness, transfer outcomes, failures, and recovery; it suppresses
repeated `storage_wait` polling and detailed `ble_link_session` records. Use
`--log-level DEBUG` for a diagnostic foreground sync when link negotiation or
polling detail is required. Detailed events go to the system journal; there is
no rotating debug JSONL state file. Current runtime progress and the latest
firmware observation are projected into the shared StateDB.

The read-only `device status` command joins systemd health, the latest runtime
progress and battery observation, firmware state, published-bundle metrics,
and the retained recent quality-event window in the shared StateDB:

```bash
sudo -n /usr/local/sbin/omi-collector-status
```

Routine status and `device metrics` validate READY manifest structure, identity,
file type, and exact payload size without reading `records.bin`; their counts
describe visible metadata. Publication, reuse, and retirement automatically
verify payload hashes at their custody boundaries.

The operational snapshot stores low-rate `unknown`, `clear`, or `blocked`
states for quality, publication, and clock health in the shared StateDB. It is
scoped to the host boot ID and systemd invocation ID: a new identity degrades
old `clear` state to `unknown`, while old
`blocked` state remains attention until the service records recovery. State
changes, not arbitrary heartbeats or TTLs, update the snapshot. A failed
snapshot write is supervised: collection is cancelled through its finalizer,
which closes or preserves active audio evidence, and the service exits with an
error instead of claiming healthy status.

Confirmed loss has its own authoritative StateDB rows, committed before the
prefix publication that exposes the gap, and `quality_window` reports those
totals independently of retained telemetry. Quality events are a bounded,
best-effort projection in the same database; a full queue or failed event write
can lose a telemetry event, but cannot erase the confirmed-loss rows. Migration
deduplicates retained legacy loss facts. Loss totals are quality statistics and
do not change top-level status by themselves.

Interpret `ok` as requiring a completed transfer in the quality window and
`clear` operational states, and no latest terminal transfer failure. Without a
completed transfer, status is `unknown` unless a blocked operational state or
a known stopped service requires `attention`; absence alone is not healthy.
Confirmed-loss totals remain visible in the quality window but do not change
status by themselves. The quality window includes outcome and
termination-class counts. A missing `device` object means no firmware
observation has been recorded yet.
Treat status as an operational summary: inspect the journal and sealed bundles
before diagnosing a specific transfer.

## Documentation hygiene

Keep the root README short and link only to maintained documents. Protocol
details belong in [DEVICE_PROTOCOL.md](DEVICE_PROTOCOL.md), security claims in
[SECURITY.md](SECURITY.md), transfer-quality interpretation in
[QUALITY_METRICS.md](QUALITY_METRICS.md), and acceptance behavior in the
Gherkin acceptance specification at
`features/opportunistic_collection.feature`. The feature file is a reviewed
specification, not an executable test; executable checks remain in pytest.
Remove obsolete documents and host-specific observations rather than expanding
this documentation set.
# Audio time ownership

Clock correction owns its complete lifecycle; publication must not finish it.
The operational assumption is that uncontrolled clock changes happen only
across pendant power-off/power-on. Our own clock writes have known sequence
boundaries. Calendar timestamps are approximate; sequence, Opus packets and
sample counts determine audio order and duration.

| State | Meaning | Recovery/admission |
|---|---|---|
| `prepared` | Durable intent, no possible clock write yet | Restart finishes it as `not_written` |
| `unresolved` | Clock write may have happened | Close the old BLE execution window, observe fresh RTC, then recover or supersede |
| `applied` | Readback confirmed the write | Terminal, even if the old sequence boundary is approximate |
| `not_applied`, `not_written`, `resolved`, `unknown` | Completed history | Does not block another correction; `unknown` preserves the old uncertainty |

Persist intent and its own initial observation before each clock write. Never
write without durable intent or while an interrupted audio attempt is pending.
After interruption, do not replay an old target; use a fresh observation and
the same transition policy as normal completion.

Use the RTC-read midpoint to estimate the timestamp offset for backlog, and
readback for records after our correction. Mark estimates as approximate;
previous power cycles can make old calendar dates less accurate. Do not
rewrite already published ready bundles. A clock-only fault or unrepresentable
estimate must leave valid audio publishable with its original timestamp and
unknown mapping. It must never change audio payloads, order, packet count,
playback duration, speech boundaries, or ACK eligibility. Keep actual packet
loss in operational quality evidence.
