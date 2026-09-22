# Omi Collector

[![CI](https://github.com/j2h4u/omi-collector/actions/workflows/ci.yml/badge.svg)](https://github.com/j2h4u/omi-collector/actions/workflows/ci.yml)
[![CodeQL](https://github.com/j2h4u/omi-collector/actions/workflows/codeql.yml/badge.svg)](https://github.com/j2h4u/omi-collector/actions/workflows/codeql.yml)
[![Python 3.14+](https://img.shields.io/badge/python-3.14%2B-blue)](https://www.python.org/)
[![License: PolyForm Noncommercial](https://img.shields.io/badge/license-PolyForm%20Noncommercial-blue)](LICENSE)

**Continuously drain offline audio from an Omi CV1 pendant to your own Linux server.**

Omi Collector discovers a nearby pendant over Bluetooth Low Energy, downloads
its buffered records, survives interrupted transfers, corrects known clock
shifts, and publishes validated source bundles for the audio pipeline that
follows. It works with the stock pendant firmware and runs unattended as a
systemd service.

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
- corrects timestamps around recorded pendant clock changes;
- atomically publishes sealed bundles with one consistent timeline;
- records operational metrics and recent debug context in local state.

It deliberately does **not** transcode, run VAD, transcribe, call the Omi cloud,
or delete published bundles. Those are downstream responsibilities.

## What comes next

Our production installation passes the published source bundles to a private
Windmill flow. That downstream implementation is not published in this
repository. Windmill prepares temporary audio for voice activity detection
(VAD), keeps only speech for long-term storage, and publishes one ordinary Ogg
Opus file with a compact JSON passport. The passport maps retained speech back
to the real timeline and records removed or missing intervals.

That processing intentionally lives outside Omi Collector. This repository
ends at durable publication of validated pendant records, so Windmill is not a
runtime dependency and other users can attach a different processing pipeline.

### Pipeline boundary

```text
Omi pendant -> Omi Collector -> source/current -> Windmill -> speech/*.ogg + speech/*.json
```

Omi Collector owns Bluetooth transfer, interruption recovery, source-loss
reporting, clock correction, and publication of a consistent record timeline.
Its public handoff is the generation exposed atomically at `source/current`.
Each generation contains `generation.json` plus sequence-range directories;
each range contains `records.bin`, `manifest.json`, and `receipt.json`.

`records.bin` stores consecutive 444-byte pendant records: a normalized Unix
timestamp followed by the packed Opus payload. The manifest binds the sequence
range, record count, size, and content hash; the receipt marks the bundle as
sealed. Windmill discovers only these complete published ranges. The
`captured` and `collector` directories remain internal to Omi Collector,
`work` is temporary Windmill state, and `speech` contains Windmill's completed
audio and passport artifacts.

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
`omi-collector` service account, and one shared pipeline root at
`/srv/pipelines/omi`. The operator configuration lives at
`/srv/pipelines/omi/config.toml`; its parent is also the fixed storage root.

```bash
sudo git clone https://github.com/j2h4u/omi-collector.git /opt/omi-collector
cd /opt/omi-collector
UV_BIN=$(command -v uv)
[[ "$UV_BIN" == /usr/local/bin/uv ]] || \
  sudo install -o root -g root -m 0755 "$UV_BIN" /usr/local/bin/uv
sudo install -d -o root -g root -m 0755 /srv/pipelines/omi
sudo install -o root -g root -m 0644 config/config.toml.example /srv/pipelines/omi/config.toml
sudoedit /srv/pipelines/omi/config.toml
sudo scripts/install-systemd-unit.sh
sudo install -d -o omi-collector -g omi-collector -m 0750 \
  /srv/pipelines/omi/collector /srv/pipelines/omi/captured /srv/pipelines/omi/source
sudo systemctl enable --now bluetooth.service
sudo -u omi-collector bluetoothctl show
sudo scripts/deploy-systemd-service.sh
```

Replace the placeholder with the pendant Bluetooth address. The optional
`[presence]` section controls automatic scanner admission: the default requires
30 seconds of observations with no gap of 10 seconds or more before GATT is
opened. A timer, a remembered address, or a stale scanner event cannot create
an automatic connection permit. The fixed configuration location determines
storage.
The installer creates the service account, keeps the configuration root-owned
and readable by both services as mode `0644`, and enables the service. It does not start the
service unless `--restart` is explicit. Complete the first successful
deployment before rebooting or leaving the host unattended because the unit
has no selected runtime before then.

The installer also installs the root-owned release wrapper at
`/usr/local/sbin/omi-collector-deploy-release` and its fixed sudo policy. The
wrapper accepts exactly one `vMAJOR.MINOR.PATCH` tag and delegates to the
checked-in transactional `scripts/dev-deploy-release.sh` in `/opt/omi-collector`:

```bash
sudo -n /usr/local/sbin/omi-collector-deploy-release v0.3.0
```

It does not perform clock recovery or accept arbitrary commands.

Follow the service with:

```bash
systemctl status omi-collector.service
journalctl -u omi-collector.service -f
```

### Observability

The service uses `INFO` journal output for durable operator signals: readiness,
transfer progress and completion, failures, and recovery. Routine
`storage_wait` polling and detailed `ble_link_session` records stay out of the
INFO journal. A diagnostic sync can use `--log-level DEBUG` when those records
are needed. Every sync callback and BLE link session is also retained in the
bounded `debug.jsonl` ring under the collector root, so protect that state as
you would the rest of the private collector data.

For one machine-readable operational snapshot of systemd health, current
transfer progress, last observed battery and firmware, device backlog,
publication, and recent transfer/loss evidence,
run:

```bash
sudo -n /usr/local/sbin/omi-collector-status
```

Interpret `status=ok` as at least one completed transfer in the window with no
confirmed loss or terminal failure. `status=attention` is the degraded state:
confirmed loss or a latest fatal, cancelled, or teardown-interrupted transfer
was recorded;
`status=unknown` means the window has no completed transfer to assess. The
`publication` object describes currently visible bundles, while
`quality_window` contains bounded advertisements, transfer throughput,
outcome and termination-class breakdowns, and loss totals. Use `device metrics`
for the publication inventory alone and `journalctl -u omi-collector.service`
or `debug.jsonl` when the summary needs more context.

For subsequent updates, select the reviewed revision in the production
checkout and run:

```bash
sudo scripts/deploy-systemd-service.sh
```

The deployment command validates the operator configuration, builds a
versioned root-owned environment under `/var/lib/omi-collector-deployments`,
writes its source revision into the release, atomically selects it, and
starts the service. It
requires both application readiness and a stable process. If either check
fails, it restores the previous selected environment and service. Known
obsolete release directories are pruned only after a successful deployment.

### Maintainer Dev Script

`scripts/dev-deploy-release.sh` is a host-specific convenience tool for the
project maintainer. It is intended only for deployments whose live checkout is
fixed under an operator-owned `/opt` tree, following the same convention as
Docker services under `/opt/docker/<service>`. This systemd deployment uses
`/opt/omi-collector` instead. The script fetches and validates a published
release tag in that exact checkout, refuses local changes or an unexpected
origin, and then invokes the transactional deployer above.

It is not part of the portable installation contract. Other operators should
select their reviewed revision in the production checkout and run
`deploy-systemd-service.sh` directly.

On the maintainer host, run it from the source checkout with an explicit tag:

```bash
sudo scripts/dev-deploy-release.sh v0.3.0
```

## Storage

`/srv/pipelines/omi/config.toml` is the single operator-facing configuration.
Its parent is the storage root. The collector uses `collector` for private
state, `captured` for captured inputs, and `source` for the current published
history. The checked-in systemd unit grants write access to this one fixed root.

Published bundles are a shared boundary. If another local account consumes
them, configure either a shared Unix group or a default ACL on the source root.
For a named downstream account, the ACL shape is:

```bash
sudo setfacl -m u:omi-collector:rwx,u:DOWNSTREAM:rwx /srv/pipelines/omi/source
sudo setfacl -m d:u:omi-collector:rwx,d:u:DOWNSTREAM:rwx /srv/pipelines/omi/source
```

Replace `DOWNSTREAM`, and ensure every parent directory is traversable by both
accounts. Verify access as the downstream account after the first bundle is
published.

## Safety model

The collector treats missing audio as worse than duplicate audio:

- `INFO` is authoritative for the unread cursor; automatic collection requires
  a current exact-address scanner candidate after stable repeated visibility.
- Reads are bounded and serialized per pendant. Destructive `CLEAR` is not
  exposed.
- Bytes and checkpoints become durable before a record can be sealed.
- Interrupted attempts recover from one authenticated partial and compare any
  replayed overlap byte-for-byte.
- Malformed or ambiguous evidence is quarantined instead of being silently
  accepted or discarded.
- Publication uses an atomic rename, so downstream consumers see either a
  complete bundle or nothing.

### Stock pendant gotchas

These are observed limitations of the stock CV1 firmware that materially
affect capture. They are not properties of the external audio format.

#### Recommended operating routine

Keep the pendant powered on whenever possible, including while charging.
Powering it off and back on can leave its clock substantially wrong, which in
turn complicates the recorded timeline. If a shutdown is unavoidable, switch
the pendant on near the collector and leave it there while the collector
automatically corrects the clock and downloads all buffered audio.

Treat each visit to the collector as one complete synchronization session.
Place the pendant close to the Bluetooth adapter, wait until the download has
fully drained, and only then carry it away. Avoid several short approaches and
departures: every interrupted transfer creates another opportunity for the
stock firmware to discard a few records before the collector has stored them.
No manual clock or download command is required; the collector performs both
steps automatically. Its device status should report no unread packets and a
cleanly completed transfer before the pendant is removed.

#### A transmitted packet may already be gone

The stock pendant firmware advances its persisted read checkpoint while data
is being transmitted. Bluetooth transmission completion does not prove that
the collector received a complete record and stored it durably. If the pendant
leaves radio range between those events, reconnecting can start after a small
amount of audio that the collector never received. This is a timing-dependent
race, not a loss on every disconnect; a weak or intermittent link makes it more
likely. The collector detects and measures the resulting cursor gap, but cannot
recover bytes that the stock firmware has already discarded. The confirmed
upstream report is
[BasedHardware/omi#13100](https://github.com/BasedHardware/omi/issues/13100).

The practical mitigation is to avoid repeatedly carrying the pendant through
the edge of Bluetooth range: place it near the server and leave it there until
the current download has drained. There is no ready-made firmware alternative
that eliminates this failure mode. Doing so means forking the stock firmware,
implementing less aggressive checkpointing or a replay window, building it,
and flashing the pendant yourself.

#### The pendant clock can be substantially wrong

Each stored record contains a pendant-generated Unix timestamp. The CV1 clock
has been observed ahead of trusted host time by many minutes. The corrections
are too large to assume ordinary crystal drift, but current evidence does not
distinguish among RTC restoration, sleep-time accounting, reset behavior, or
another firmware defect.

Correcting the clock can make later raw records appear earlier than records
captured immediately before the write. The collector therefore persists its
intent before changing the clock, reads the value back afterward, and records
the exact sequence interval in which a verified reset occurred. It applies
only this evidence-backed correction when publishing the audio timeline and
also records the event in the operational journal. A bundle boundary alone is
not proof of a clock correction.

This gives consumers one monotonic recording timeline while preserving the
device evidence. It does not explain or fix the underlying RTC behavior. Treat
`sequence_loss` as confirmed unrecoverable source loss, and treat a raw
timestamp regression as authorized only when it falls within a verified clock
correction boundary. Recheck both behaviors after every firmware upgrade; a
new version number alone is not proof that either one was fixed.

Clock observations are retained in a separate immutable evidence ledger. A
restart replays that ledger and can publish a safe captured prefix without a
new BLE visit. For an older incident, use `device clock-recover` with a finite
machine-readable journald JSON export; the importer accepts only one host boot,
numerically stable realtime/monotonic mapping, valid raw bundle hashes, and an
unambiguous first record at the correction boundary. `--apply` performs the
bounded recovery and publication; without it the command is validation-only.

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
