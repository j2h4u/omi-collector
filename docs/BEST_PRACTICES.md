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
- `just verify` — the complete local contract.

Do not weaken or locally suppress a gate. Use targeted checks while iterating,
then run the full contract before release or handoff.

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
Individual mutants retain their 150-second budget and the independent coverage
pre-scan retains 600 seconds. The recipe retains the complete timeout and runner
log and records a small identity/start/end/report receipt, then reconciles native generated IDs
against the fresh native JSON report. Missing or malformed report metadata,
identity changes, nonzero exits, duplicate/missing/foreign IDs, and unresolved
errors or timeouts cannot certify a clean audit. Status 137 is fail-stopped and
is never retried automatically. There is no mutation score gate, and mutation
testing remains separate from `just verify`.

For a controllable audit, run `just mutation-start` or
`just mutation-fresh-start` in a persistent terminal such as tmux. Inspect it
with `just mutation-status`, then use `just mutation-pause` and
`just mutation-resume` from another shell to freeze and thaw its dedicated user
scope. The scope runs at SCHED_IDLE CPU policy, ionice idle class, and nice 19.
Freezing pauses processes, not their monotonic deadlines: GNU's 24-hour audit
budget and the runner's 120-second test and 150-second mutant budgets continue
to elapse. An in-flight test or mutant can time out immediately after resume.
Keep the canonical `just mutation` recipe for foreground use.

## Capture safety

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

Foreground attempt and closure effects own priority over background publication
retries. Join any running retry before admission and defer new retries until
the foreground scope exits. Inline clock publication uses the explicitly
transferred clock lease; it must not reacquire the same filesystem lock.

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
`[presence]` and backwards-compatible `[ready]` sections. `[ready]` accepts
`target_audio_seconds` (3600 seconds by default). Only a confirmed complete
pendant drain permits packaging, and only when the cumulative captured Opus
audio reaches that minimum. All accumulated eligible drafts then form one
bundle; crossing the minimum during downloading never splits the visit.
Smaller downloads accumulate across visits and restarts indefinitely. Calendar
gaps do not count toward audio duration. The legacy `max_wait_seconds` key is
accepted for existing configurations but has no effect; no age-based or manual
flush exists. Interrupted closures preserve data but do not authorize packaging.
Admission of a new capture visit durably revokes the previous drain permit
before BLE work. Clock publication and restart recovery cannot reuse that stale
permit; a new confirmed drain grants permission again at the retained frontier.
Its fixed parent `/srv/pipelines/omi` is the storage root; `collector`,
`draft`, and `ready` are derived beneath it. The base unit grants the
service write access to that root. Use filesystem ownership or ACLs on the
derived directories when a downstream account also needs access.

After installing the unit, run `sudo scripts/deploy-systemd-service.sh`. It
builds the environment as the dedicated build account, copies dependencies into it,
then seals the selected release as root-owned under
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

Run `sudo scripts/deploy-systemd-service.sh` from the reviewed production
checkout. It checks the installed unit against the checked-in unit, validates
the fixed operator configuration with the staged application, records the full
source revision inside the sealed environment, then atomically selects the
release. Readiness or stability failure restores the previous verified
environment.

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
polling detail is required. The bounded `debug.jsonl` ring receives sync
callbacks and link diagnostics separately from the journal; keep its private
permissions and inspect it only for an active investigation.

The read-only `device status` command joins systemd health, the latest runtime
progress and battery observation, firmware state, published-bundle metrics,
and the selected recent window from `quality.jsonl`:

```bash
sudo -n /usr/local/sbin/omi-collector-status
```

Interpret `ok` as a completed transfer with no confirmed loss or terminal
failure in the window. `attention` is the degraded state for confirmed loss or
a latest fatal, cancelled, or teardown-interrupted transfer; `unknown` means no
completed transfer in the window. The quality window includes both outcome and
termination-class counts. A missing `device` object means no firmware
observation has been recorded yet. Treat the status as an operational summary:
inspect the journal, debug ring, and sealed bundles before diagnosing a
specific transfer.

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
