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

## Capture safety

Keep physical-download policy in the pure `visit_machine`: it alone authorizes
drained, interrupted, restart, or operator-limit closure. Session, presence, and
storage code supply facts or execute its commands; they do not choose a second
closure policy. Test event traces independently from BLE and filesystem tests.
Only a fresh final zero-unread INFO with settled reconciliation, successful
teardown, and checkpoint can produce `DrainConfirmed`. Publication uses a
separate durable FIFO; its retries do not change the current visit state.

`INFO` is the source of truth for unread state. Automatic service and sync
admission requires a current exact-address scanner candidate that has remained
visible for the configured stable-arrival span; the default is 30 seconds with
no 10-second gap. Timers and remembered addresses never authorize GATT work.
Serialize attempts per pendant and stop this collector's scanner before GATT
work. Explicit probe, info, and confirmed collect commands remain direct
operator paths.

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
exactly `target_audio_seconds` and `max_wait_seconds`; those legacy values do
not trigger publication. Ready bundles are published after a durable physical
visit closure, including closures recovered after restart.
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
