# Omi Collector

[![CI](https://github.com/j2h4u/omi-collector/actions/workflows/ci.yml/badge.svg)](https://github.com/j2h4u/omi-collector/actions/workflows/ci.yml)
[![CodeQL](https://github.com/j2h4u/omi-collector/actions/workflows/codeql.yml/badge.svg)](https://github.com/j2h4u/omi-collector/actions/workflows/codeql.yml)
[![Python 3.14+](https://img.shields.io/badge/python-3.14%2B-blue)](https://www.python.org/)
[![License: PolyForm Noncommercial](https://img.shields.io/badge/license-PolyForm%20Noncommercial-blue)](LICENSE)

**Continuously drain offline audio from an Omi CV1 pendant to your own Linux server.**

Omi Collector discovers a nearby pendant over Bluetooth Low Energy, downloads
its buffered records, survives interrupted transfers, and publishes durable raw
bundles for whatever audio pipeline you want to build next. It works with the
stock pendant firmware and runs unattended as a systemd service.

This is an independent community project. It is not an official Omi or Based
Hardware product. The protocol implementation is grounded in the
[official Omi repository](https://github.com/BasedHardware/omi).

The pendant carries sensitive audio and stock BLE access is not hardened
against every nearby client. Use it only in a trusted radio environment,
restrict access to published bundles, and read the [security notes](docs/SECURITY.md).

## Why

The official mobile app is not the only useful home for pendant audio. A local
collector makes the Omi hardware usable in self-hosted workflows while keeping
capture separate from transcription, diarization, search, and long-term
storage.

Omi Collector provides the narrow first stage:

- detects when the pendant enters Bluetooth range;
- drains the on-device ring as quickly as the link permits;
- resumes after ordinary disconnects and process restarts;
- verifies replayed overlap instead of silently skipping records;
- atomically publishes sealed bundles containing the original bytes;
- records operational metrics and recent debug context in local state.

It deliberately does **not** transcode, run VAD, transcribe, call the Omi cloud,
or delete published bundles. Those are downstream responsibilities.

## What comes next

In my downstream pipeline, each raw bundle passes through voice activity
detection (VAD). The pipeline keeps only speech audio for long-term storage and
publishes a compact passport alongside it that maps the retained speech back to
the original timeline. This preserves when speech and removed gaps occurred
without retaining the much larger stream of silence and non-speech audio.

That processing intentionally lives outside Omi Collector. This repository
ends at durable publication of the original pendant data, so other users can
attach a different VAD, transcription, or archival pipeline.

## Requirements

- Omi CV1 pendant running stock firmware
- Linux with BlueZ and a working Bluetooth adapter
- Python 3.14 or newer
- [`uv`](https://docs.astral.sh/uv/)
- systemd for unattended operation

[`just`](https://github.com/casey/just) and Docker are needed only for local QA,
not for the running collector.

## Try the CLI

```bash
git clone https://github.com/j2h4u/omi-collector.git
cd omi-collector
uv sync --locked
uv run omi-collector --help
uv run omi-collector device --help
uv run omi-collector health
```

Commands that initiate a PHY guard or consume ring data require explicit
confirmation; emergency PHY recovery intentionally does not. Read each device
command's `--help` output first. This matters because stock firmware may advance
its persisted ring checkpoint while serving a read, even when the client never
sends an explicit `ADVANCE` command.

## Production installation

The supported production shape is a root-owned checkout, a dedicated
`omi-collector` service account, and mutable state under
`/var/lib/omi-collector`.

```bash
sudo git clone https://github.com/j2h4u/omi-collector.git /opt/omi-collector
cd /opt/omi-collector
UV_BIN=$(command -v uv)
[[ "$UV_BIN" == /usr/local/bin/uv ]] || \
  sudo install -o root -g root -m 0755 "$UV_BIN" /usr/local/bin/uv
sudo install -d -o root -g root -m 0755 /etc/omi-collector /var/lib/omi-collector
sudo install -o root -g root -m 0640 config/layout.toml /var/lib/omi-collector/collector.toml
sudo install -o root -g root -m 0600 config/omi-collector.env.example /etc/omi-collector/omi-collector.env
sudoedit /etc/omi-collector/omi-collector.env
sudo scripts/install-systemd-unit.sh
sudo -u omi-collector env HOME=/var/lib/omi-collector \
  UV_CACHE_DIR=/var/lib/omi-collector/uv-cache \
  UV_PROJECT_ENVIRONMENT=/var/lib/omi-collector/venv UV_LINK_MODE=hardlink \
  /usr/local/bin/uv sync --locked --no-dev --no-editable
sudo systemctl enable --now bluetooth.service
sudo -u omi-collector bluetoothctl show
sudo systemctl restart omi-collector.service
```

Replace every placeholder in the environment file. At minimum, configure the
pendant Bluetooth address, a lowercase device slug, the layout file, checkout,
`uv`, and virtual-environment paths. The installer enables the service but does
not start it unless `--restart` is explicit.

Follow the service with:

```bash
systemctl status omi-collector.service
journalctl -u omi-collector.service -f
```

For subsequent updates, pull a reviewed revision and run:

```bash
sudo scripts/deploy-systemd-service.sh
```

The deployment command refreshes the dedicated environment, restarts the
service, and requires both application readiness and a stable process.

## Storage

The default layout keeps private collector state and published bundles below
`/var/lib/omi-collector`. Paths are configured in a TOML layout file rather
than hard-coded in the application.

An external layout may use any absolute regular, non-symlink file. External
data roots also need a host-specific systemd drop-in, for example:

```ini
# /etc/systemd/system/omi-collector.service.d/storage.conf
[Service]
ReadWritePaths=/var/lib/omi-collector /srv/omi/collector /srv/omi/pipeline/raw
```

Do not add host-specific paths to the checked-in base unit. Grant the service
account only the traversal and write permissions it needs.

Published bundles are a shared boundary. If another local account consumes
them, configure either a shared Unix group or a default ACL on the raw root.
For a named downstream account, the ACL shape is:

```bash
sudo setfacl -m u:omi-collector:rwx,u:DOWNSTREAM:rwx /path/to/raw
sudo setfacl -m d:u:omi-collector:rwx,d:u:DOWNSTREAM:rwx /path/to/raw
```

Replace `DOWNSTREAM` and the path, and ensure every parent directory is
traversable by both accounts. Verify access as the downstream account after the
first bundle is published.

## Safety model

The collector treats missing audio as worse than duplicate audio:

- `INFO` is authoritative for the unread cursor; advertisements only trigger a
  collection attempt.
- Reads are bounded and serialized per pendant. Destructive `CLEAR` is not
  exposed.
- Bytes and checkpoints become durable before a record can be sealed.
- Interrupted attempts recover from one authenticated partial and compare any
  replayed overlap byte-for-byte.
- Malformed or ambiguous evidence is quarantined instead of being silently
  accepted or discarded.
- Publication uses an atomic rename, so downstream consumers see either a
  complete bundle or nothing.

The optional `--force-1m` weak-RF workaround changes controller-wide PHY state.
It is disabled by default and restores the prior selection after completion,
failure, cancellation, or recovery. See the
[device protocol](docs/DEVICE_PROTOCOL.md) before enabling it.

## Verification

The release gate is:

```bash
just verify
```

It runs static and architecture checks, the behavioral test suite, the CRAP
complexity gate, packaging validation, a Docker build, and a runtime smoke.

## Documentation

- [Device protocol](docs/DEVICE_PROTOCOL.md) — GATT services, ring framing,
  commands, notifications, and firmware behavior.
- [Security](docs/SECURITY.md) — BLE privacy observations and data-handling
  expectations.
- [Best practices](docs/BEST_PRACTICES.md) — concise operational and QA
  guidance.
- [Acceptance scenarios](features/opportunistic_collection.feature) — presence,
  transfer, interruption, recovery, and publication behavior.

The protocol reference is pinned to revision
[`6f7c57a`](https://github.com/BasedHardware/omi/tree/6f7c57ac1545c1931c806a01605646405d398198)
of the official Omi repository. Source comments link back to the corresponding
official firmware and app behavior where it matters.

## Project status

The current release is intentionally narrow: one pendant per service, stock
firmware, Linux/BlueZ, and raw local publication. Reports from other adapters,
distributions, and pendant revisions are welcome.

## License

[PolyForm Noncommercial License 1.0.0](LICENSE). Noncommercial use is
permitted; this is source-available software, not OSI open source. Commercial
use requires a separate license or prior written permission from Max Brashenko.
Omi and Based Hardware are names of their respective owners.
