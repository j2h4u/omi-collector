# Best practices

Keep this project a small raw-capture boundary. The collector owns presence,
BLE transfer, recovery/quarantine, and atomic raw publication. Consumer-specific
processing and external service integration are outside its boundary.

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
`omi-collector` system user. Keep the checkout root-owned/readable at the
configured absolute project path; keep the environment file root-owned,
group-readable by `omi-collector`, mode `0640`; and keep the service-local
state directory `omi-collector:omi-collector` mode `0750`.

Initial setup is deliberately small. For this production host, copy
`config/layout.toml` to `/srv/pipelines/omi/collector.toml`, copy and edit
`config/omi-collector.env.example` at `/etc/omi-collector/omi-collector.env`,
then run `sudo scripts/install-systemd-unit.sh`. The installer creates or
validates the system account and state ownership, validates the staged wrapper
against the environment, validates the staged unit, and rolls both files back
if the composite install cannot be reloaded or enabled. It never starts the
service without `--restart`.

The layout schema is strict version two: `collector` is the private collector
root and `source` is the direct publication root. `OMI_COLLECTOR_LAYOUT_PATH`
may name any regular non-symlink absolute layout file; roots resolve relative to
its parent. This host uses `/srv/pipelines/omi/collector.toml`. Create the two
roots with ownership and permissions appropriate to the service account, then
add this host-only drop-in at
`/etc/systemd/system/omi-collector.service.d/storage.conf`:

```ini
[Service]
ReadWritePaths=/var/lib/omi-collector /srv/pipelines/omi/collector /srv/pipelines/omi/source
```

Keep host-specific paths out of the checked-in unit. The layout file's parent
directories must permit `omi-collector` traversal and reading; both declared
roots must permit that account to write. Set ownership or narrowly scoped
filesystem ACLs deliberately, then validate the drop-in and reload systemd. The
installer still enforces `root:omi-collector` mode `0640` on every accepted
layout file.

Prepare the dedicated noneditable UV environment as the service account:

```bash
sudo -u omi-collector env UV_PROJECT_ENVIRONMENT=/var/lib/omi-collector/venv \
  UV_LINK_MODE=hardlink /usr/local/bin/uv sync --locked --no-dev --no-editable
```

Enable BlueZ and verify its normal user-level access with
`sudo -u omi-collector bluetoothctl show`. The default long-running sync keeps
`--force-1m` off. If an explicit weak-RF fallback is required, allow only the
exact `/usr/bin/bluetoothctl --timeout 5 mgmt.phy` forms needed by the service
through a dedicated `visudo -f /etc/sudoers.d/omi-collector-bluetoothctl` file.
The base allowlist is the no-argument query plus `LE1MTX LE1MRX` and
`LE1MTX LE1MRX LE2MTX LE2MRX`; add a different sequence only after observing it.
Do not grant general `bluetoothctl`, shell, or unrestricted sudo access.

Run `sudo scripts/deploy-systemd-service.sh` only from the configured checkout.
It synchronizes the UV environment as `omi-collector`, checks the installed unit
and wrapper as the exact checked-in pair, then accepts the root-controlled
restart only after readiness plus a bounded stable-process interval.

Docker is for packaging and runtime QA only; it is not a production Bluetooth
supervisor. Keep production state and publication roots configured explicitly,
and restrict their permissions. Do not put raw audio, credentials, BLE
addresses, or detailed live device observations in documentation or logs.

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
