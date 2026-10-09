# Transfer-quality evidence

`quality.jsonl` is a bounded, rotating JSONL journal directly beneath the
collector root. A daemon writer fsyncs complete lines without delaying BLE
collection; a full queue or failed write is reported to the separate debug
ring and never prevents audio staging. Rotation keeps the configured number of
bounded backups.

`advertisement_observation` is written when a stable exact-address scanner
encounter releases a connection attempt. It records the scanner-provided RSSI and shares a
`session_id` with a later `transfer_session` when that attempt reaches `READ`.
Every automatic collection attempt starts from such a scanner encounter.

`transfer_session` appears once when a physical session that issued `READ` terminates. Its `active_read_elapsed_ms` covers READ work only, so per-session mean/median and pooled throughput should use `written_raw_bytes / active_read_elapsed_ms` (or the analogous received counter) from these terminal records. `written_raw_bytes` is writer-thread output, not a claim that the bytes were fsynced; sealed staging artifacts remain the durability authority. Do not average precomputed speeds: none are stored. `advertisement_rssi_dbm` is the latest scanner advertisement sample that woke the session, not connected-link or HCI RSSI. `source_revision` is the first 12 characters of the validated full revision in the selected release's `share/omi-collector/release.json`, or null outside a deployed release.

`sequence_loss` appears only after a confirmed cursor-ahead gap. Sum `missing_record_count` and `missing_raw_bytes` to aggregate confirmed loss. It deliberately omits sequence ranges, record identities, and loss seconds: ring record size and Opus packing cannot establish audio duration. Both event types carry release and firmware context when known.

`clock_correction` appears only after the collector wrote an NTP-trusted time
and read the same value back from the pendant. `boundary_sequence_min` is the
next sequence observed before the RTC write and `boundary_sequence_max` is the
next sequence observed after verification. A raw timestamp reset caused by the
write must begin within that inclusive interval. Downstream may treat a reset
as authorized only when the raw boundary falls in such an interval; a bundle
or terminal-source boundary alone is not clock-correction evidence.

## Operator summary

The read-only status command combines the current firmware observation, visible
published bundles, and a bounded quality window:

```bash
uv run omi-collector device status
```

Its top-level `status` is `ok` only when the quality window contains a
completed transfer, operational health is `clear`, and there is no confirmed
loss or terminal failure. A blocked health state or confirmed loss produces
`attention`; no completed transfer by itself is `unknown`, never evidence of
healthy operation. `publication` reports the currently visible bundle
inventory. `quality_window` reports recent advertisements, transfer sessions,
pooled bytes per second, outcome counts, termination-class counts, and
confirmed loss totals. A null `device` means no firmware observation has been
recorded for that device.

Confirmed loss must remain visible independently of this bounded telemetry
window. The accepted contract is an authoritative loss ledger, fsynced before
prefix publication; status reads that ledger, while `quality.jsonl` remains a
projection. Queue overflow may drop a metric record, but cannot erase the loss
fact or permit a later successful transfer to clear attention. Repeated
observations are deduplicated automatically, and retained legacy loss facts
remain part of the status. The service implements this contract; see the
[Service audit](SERVICE_AUDIT.md) for targeted proof and release status.

Current operational health is held separately from rotating debug logs as
low-rate `unknown`, `clear`, or `blocked` states for quality, publication, and
clock processing. The snapshot is tied to the host boot ID and systemd
invocation ID. On identity change, prior `clear` becomes `unknown`; prior
`blocked` remains attention until recovery is recorded. These states change on
events, not a periodic heartbeat or TTL. If snapshot persistence fails, the
service supervises the writer failure, cancels collection through its audio
finalizer, and exits nonzero rather than reporting a false healthy result. See
[Best practices](BEST_PRACTICES.md#status-and-logs) for operational guidance.

Use `journalctl -u omi-collector.service` for the operational timeline. The
service's INFO stream omits routine `storage_wait` polling and detailed
`ble_link_session` records; run a diagnostic sync with `--log-level DEBUG` when
those records are needed. The rotating `debug.jsonl` ring contains the
lower-level sync and BLE diagnostics separately from the journal.
