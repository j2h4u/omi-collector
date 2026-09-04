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

A useful downstream pipeline can pass each raw bundle through voice activity
detection (VAD), keep only speech for long-term storage, and publish a compact
passport that maps retained speech back to the original timeline. This keeps
the result small without losing when speech and removed gaps occurred.

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

The supported generic production shape is a root-owned checkout and a
dedicated `omi-collector` service account with all mutable state under
`/var/lib/omi-collector`. This is the checked-in default and works with the
hardened service unit without a host-specific drop-in.

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
sudo systemctl enable --now bluetooth.service
sudo -u omi-collector bluetoothctl show
sudo scripts/deploy-systemd-service.sh
```

Replace every placeholder in the environment file. At minimum, configure the
pendant Bluetooth address, a lowercase device slug, the layout file, checkout,
`uv`, and virtual-environment paths. The installer enables the service but does
not start it unless `--restart` is explicit. Complete the first successful
deployment before rebooting or leaving the host unattended: until then the
environment deliberately points at an unusable placeholder.

Follow the service with:

```bash
systemctl status omi-collector.service
journalctl -u omi-collector.service -f
```

For subsequent updates, pull a reviewed revision and run:

```bash
sudo scripts/deploy-systemd-service.sh
```

The deployment command builds a versioned, root-owned environment under
`/var/lib/omi-collector-deployments`, atomically selects it with its
source-revision provenance, starts the service, and
requires both application readiness and a stable process. If either check
fails, it restores the previous environment, provenance, and service. Known
obsolete release directories are pruned only after a successful deployment.
During deployment, the exact canonical schema-1 `device.json` format from
older releases is treated as disposable and removed. Malformed, unknown,
symlinked, or non-regular device state stops deployment without deletion.

## Storage

The recommended generic layout file is `/var/lib/omi-collector/collector.toml`.
Its parent is the Omi root, with `collector` and `source` as direct sibling
roots. The collector keeps private state under the former; each device slug
creates its own bundle directory directly below the latter, such as
`/var/lib/omi-collector/source/omi-cv1`. The application accepts any absolute,
regular, non-symlink layout file; all declared roots resolve relative to its
parent.

Custom storage roots require a host-specific `ReadWritePaths` systemd drop-in.
Do not add those paths to the checked-in base unit. Grant the service account
only the traversal and write permissions it needs.

Published bundles are a shared boundary. If another local account consumes
them, configure either a shared Unix group or a default ACL on the source root.
For a named downstream account, the ACL shape is:

```bash
sudo setfacl -m u:omi-collector:rwx,u:DOWNSTREAM:rwx /var/lib/omi-collector/source
sudo setfacl -m d:u:omi-collector:rwx,d:u:DOWNSTREAM:rwx /var/lib/omi-collector/source
```

Replace `DOWNSTREAM`, and ensure every parent directory is traversable by both
accounts. Verify access as the downstream account after the first bundle is
published.

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

### Stock firmware can lose a short tail

The stock pendant firmware advances its persisted read checkpoint while data
is being transmitted. If the pendant leaves radio range during a transfer, a
small amount of audio can therefore become unavailable before the collector
has received it. The collector detects and measures these gaps, but cannot
recover bytes that the stock firmware has already discarded.

The practical mitigation is to avoid repeatedly carrying the pendant through
the edge of Bluetooth range: place it near the server and leave it there until
the current download has drained. There is no ready-made firmware alternative
that eliminates this failure mode. Doing so means forking the stock firmware,
implementing less aggressive checkpointing or a replay window, building it,
and flashing the pendant yourself.

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

## Releases

Release-please opens a release PR from merged Conventional Commits. Use `feat:`
for a minor release, `fix:` for a patch, and `!` for a breaking release. Run
`just release-check` before opening a releasable PR; multi-commit squash PRs
need a `BEGIN_COMMIT_OVERRIDE` / `END_COMMIT_OVERRIDE` block in their body.
Release-please owns `CHANGELOG.md`, `pyproject.toml`, and `uv.lock` version
updates; review and merge its release PR to create the tag and GitHub release.
Published changes are summarized in the [changelog](CHANGELOG.md).

## Documentation

- [Device protocol](docs/DEVICE_PROTOCOL.md) — GATT services, ring framing,
  commands, notifications, and firmware behavior.
- [Security](docs/SECURITY.md) — BLE privacy observations and data-handling
  expectations.
- [Best practices](docs/BEST_PRACTICES.md) — concise operational and QA
  guidance.
- [Transfer-quality evidence](docs/QUALITY_METRICS.md) — the durable metric
  journal and how operators interpret its records.
- [Acceptance specification (Gherkin)](features/opportunistic_collection.feature)
  — presence, transfer, interruption, recovery, and publication behavior.
  The specification is reviewed alongside the executable pytest suite; the
  feature file itself is not an executable test.

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
