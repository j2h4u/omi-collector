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

`INFO` is the source of truth for unread state. Advertisements are advisory and
only wake a bounded attempt. Serialize attempts per pendant and stop this
collector's scanner before GATT work.

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

The configuration is strict and contains only `[pendant] address`. Its fixed
parent `/srv/pipelines/omi` is the storage root; `collector`, `captured`, and
`source` are derived beneath it. The base unit grants the service write access
to that root. Use filesystem ownership or ACLs on the derived directories when
a downstream account also needs access.

After installing the unit, run `sudo scripts/deploy-systemd-service.sh`. It
builds the environment as the service account, copies dependencies into it,
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

Persist a clock-correction intent before writing the pendant clock, then record whether the write was confirmed or remains uncertain. Never write the clock when that intent cannot be made durable or while an interrupted audio attempt is pending. Apply only confirmed corrections to record timestamps before bundle publication so consumers receive one ordinary audio timeline. Keep unrecoverable packet loss in operational quality evidence; consumers cannot act on missing audio.
