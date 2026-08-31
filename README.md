# Omi Collector

A self-hosted, raw-only BLE collector for an Omi CV1 pendant running stock
firmware. It detects presence, drains bounded batches from the pendant's ring,
recovers interrupted attempts, and publishes atomic sealed bundles containing
the original records.

Only raw capture and publication are in scope; consumers of the sealed bundles
are outside this repository.

## Requirements

- Python 3.14
- [`uv`](https://docs.astral.sh/uv/)
- [`just`](https://github.com/casey/just)
- Linux with BlueZ for physical BLE operation
- systemd for the production service
- Docker for packaging and runtime QA

## Capture invariants

- `INFO` is authoritative for the unread cursor; advertisements only wake an
  opportunistic attempt.
- Reads are bounded and serialized per pendant. `CLEAR` is not exposed.
- Stock firmware may checkpoint while sending a `READ`, so `READ` can consume
  data even without an explicit `ADVANCE`.
- A record is sealed only after its bytes and checkpoint are durably written.
  A sealed bundle is renamed into publication atomically.
- Restart recovery accepts one authenticated partial, verifies any replayed
  overlap byte-for-byte, and quarantines malformed or ambiguous evidence.
  The collector never sends a blind `ADVANCE` or invents an identity for
  unavailable audio.
- The weak-RF LE 1M guard is temporary and controller-global. It restores the
  exact prior PHY selection after normal completion, failure, cancellation, or
  recovery.

## Use

Inspect the available commands with:

```bash
uv run omi-collector --help
uv run omi-collector device --help
```

The production supervisor is systemd. Install and enable the checked-in unit
[`systemd/omi-collector.service`](systemd/omi-collector.service), configure its
environment from [`config/omi-collector.env.example`](config/omi-collector.env.example),
and keep the configured collector and publication roots on the host. Do not use
the QA Compose service as a production Bluetooth supervisor.

## Production installation

Use a root-owned checkout that the service account can read, and keep all
mutable collector state under `/var/lib/omi-collector`:

```bash
sudo git clone https://github.com/j2h4u/omi-collector.git /opt/omi-collector
cd /opt/omi-collector
sudo install -d -o root -g root -m 0755 /etc/omi-collector /var/lib/omi-collector
sudo install -o root -g root -m 0640 config/layout.toml /var/lib/omi-collector/collector.toml
sudo install -o root -g root -m 0600 config/omi-collector.env.example /etc/omi-collector/omi-collector.env
sudoedit /etc/omi-collector/omi-collector.env
sudo scripts/install-systemd-unit.sh
sudo -u omi-collector env UV_PROJECT_ENVIRONMENT=/var/lib/omi-collector/venv \
  UV_LINK_MODE=hardlink /usr/local/bin/uv sync --locked --no-dev --no-editable
sudo systemctl restart omi-collector.service
```

Set `OMI_COLLECTOR_PROJECT_DIR=/opt/omi-collector`, a real pendant address and
slug, `/var/lib/omi-collector/collector.toml`, `/usr/local/bin/uv`, and
`/var/lib/omi-collector/venv` in the environment file. The installer provisions
the `omi-collector` system user/group and corrects the state directory to
`omi-collector:omi-collector` mode `0750`; it stages and validates both the unit
and wrapper before replacing either one. It does not start the service unless
`--restart` is explicit.

BlueZ must be running and the account must be able to use the host adapter:

```bash
sudo systemctl enable --now bluetooth.service
sudo -u omi-collector bluetoothctl show
```

Normal production sync uses ordinary PHY negotiation and needs no passwordless
sudo. `--force-1m` is disabled by default; if an operator explicitly enables
that controller-wide fallback, install a dedicated, reviewed sudoers rule for
only the required `bluetoothctl mgmt.phy` invocations. For the standard
five-second commands, edit `/etc/sudoers.d/omi-collector-bluetoothctl` with
`visudo -f` and use this exact allowlist:

```sudoers
Cmnd_Alias OMI_COLLECTOR_PHY = /usr/bin/bluetoothctl --timeout 5 mgmt.phy, \
  /usr/bin/bluetoothctl --timeout 5 mgmt.phy LE1MTX LE1MRX, \
  /usr/bin/bluetoothctl --timeout 5 mgmt.phy LE1MTX LE1MRX LE2MTX LE2MRX
omi-collector ALL=(root) NOPASSWD: OMI_COLLECTOR_PHY
```

Validate the file with `visudo -cf /etc/sudoers.d/omi-collector-bluetoothctl`.
If the adapter requires another exact PHY token sequence, add only that observed
sequence; never grant a blanket `bluetoothctl`, `sudo`, or shell rule.

For updates, run `sudo scripts/deploy-systemd-service.sh` from the configured
checkout. It synchronizes the UV environment as `omi-collector`, verifies the
installed unit and wrapper pair, then requires readiness and a bounded stable
process after restarting the real service.

## Verification

Run the complete local contract with:

```bash
just verify
```

During iteration, the relevant gates are `just check`, `just crap-check`,
`just unit`, `just docker-build`, and `just runtime-smoke`. Do not weaken a
gate to make a change pass.

## Documentation

- [Device protocol](docs/DEVICE_PROTOCOL.md): GATT UUIDs, ring framing,
  commands, notifications, and the firmware consumption warning.
- [Security](docs/SECURITY.md): the qualified stock-BLE privacy finding and
  handling expectations.
- [Best practices](docs/BEST_PRACTICES.md): concise QA, deployment, and data
  handling guidance.
- [Capture acceptance scenarios](features/opportunistic_collection.feature):
  presence, transfer, recovery, and publication behavior.

The upstream protocol reference is
[`BasedHardware/omi`](https://github.com/BasedHardware/omi/tree/6f7c57ac1545c1931c806a01605646405d398198)
at pinned revision `6f7c57ac1545c1931c806a01605646405d398198`.

## License

PolyForm Noncommercial License 1.0.0. Noncommercial use is permitted;
commercial use requires a separate license or prior written permission from
Max Brashenko.
