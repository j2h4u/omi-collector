# Transfer-quality evidence

Recent transfer-quality events are stored as bounded `quality_event` rows in
`collector/collector-state.sqlite3`, not in a separate JSONL journal. A single
asynchronous best-effort writer keeps BLE collection independent of telemetry
storage. The retained event history follows a byte budget; queue overflow or a
failed event write may drop telemetry but cannot change audio durability or the
authoritative confirmed-loss state. Read-only status queries select the
requested recent window from those retained rows.

`advertisement_observation` is written when a stable exact-address scanner
encounter releases a connection attempt. It records scanner-provided RSSI and
shares a `session_id` with a later `transfer_session` if that attempt reaches
`READ`. Every automatic collection attempt starts from such a scanner
encounter.

`transfer_session` appears once when a physical session that issued `READ`
terminates. Its `active_read_elapsed_ms` covers READ work only, so per-session
mean/median and pooled throughput should use
`written_raw_bytes / active_read_elapsed_ms` (or the analogous received
counter) from terminal records. `written_raw_bytes` is writer-thread output,
not a claim that the bytes were fsynced; sealed staging artifacts remain the
durability authority. Do not average precomputed speeds: none are stored.
`advertisement_rssi_dbm` is the latest scanner advertisement sample that woke
the session, not connected-link or HCI RSSI. `source_revision` is the first 12
characters of the validated full revision in the selected release's
`share/omi-collector/release.json`, or null outside a deployed release.

`sequence_loss` appears only after a confirmed cursor-ahead gap. Sum
`missing_record_count` and `missing_raw_bytes` to aggregate confirmed loss. It
deliberately omits sequence ranges, record identities, and loss seconds: ring
record size and Opus packing cannot establish audio duration. Both event types
carry release and firmware context when known.

`clock_correction` appears only after the collector wrote an NTP-trusted time
and read the same value back from the pendant. `boundary_sequence_min` is the
next sequence observed before the RTC write and `boundary_sequence_max` is the
next sequence observed after verification. A raw timestamp reset caused by the
write must begin within that inclusive interval. Downstream may treat a reset
as authorized only when the raw boundary falls in such an interval; a bundle
or terminal-source boundary alone is not clock-correction evidence.

## Bundle date history

The `ready_publication` rows in the same SQLite database retain each bundle's
identity, sequence range and three calendar dates, including after downstream
acknowledgement removes the bundle files. This durable publication metadata
does not use the best-effort quality-event queue or its byte budget.

`formed_on_utc` is the UTC date sampled for the final publication commit
proposal. Retries and reuse preserve that value; it describes publication,
not when the audio was recorded or an exact transaction commit instant.
`recorded_on_utc_min` and `recorded_on_utc_max` are inclusive estimated bounds
on the UTC recording dates. They cover every converted record and expand
outward for clock uncertainty. Both are null if any record has no UTC mapping
or the dates cannot be represented safely. A date envelope does not claim
continuous recording between its endpoints. Sequence ranges remain the basis
for ordering; overlapping ranges do not establish a strict archive order.

Old entries without these fields have unknown dates. Do not fill them from file
modification dates, the latest clock correction, or the number of records.
Reading this history does not require opening audio files. For example, use
the following query through a read-only SQLite connection:

```sql
SELECT id AS bundle_id,
       json_extract(payload, '$.formed_on_utc') AS formed_on_utc,
       json_extract(payload, '$.recorded_on_utc_min') AS recorded_on_utc_min,
       json_extract(payload, '$.recorded_on_utc_max') AS recorded_on_utc_max
FROM state
WHERE kind = 'ready_publication' AND id != 'frontier';
```

## Operator summary

The read-only status command combines the current firmware observation,
visible published bundles, and a bounded quality window:

```bash
uv run omi-collector device status
```

Its top-level `status` is `ok` only when the quality window contains a
completed transfer, the service is active with operational health `clear`, and
there is no latest terminal transfer failure. A blocked health state, inactive
service, or terminal failure produces `attention`; no completed transfer by
itself is `unknown`, never evidence of healthy operation. Confirmed-loss
totals remain visible in `quality_window` but do not change the top-level
status by themselves. `publication` reports the currently visible bundle
inventory. Its `loss_ratio` estimates unproven sequence holes among those
visible bundles; it is not the fraction of historically confirmed loss.
`quality_window` reports recent advertisements, transfer sessions, pooled
bytes per second, outcome counts, termination-class counts, and confirmed-loss
totals as `confirmed_loss_events`, `confirmed_lost_records`, and
`confirmed_lost_raw_bytes`. A null `device` means no firmware observation has
been recorded for that device.

Confirmed loss is kept in its own authoritative StateDB rows and committed
before the prefix publication that exposes a gap. The quality event history is
only a projection and may lose telemetry during queue overflow; it cannot
erase confirmed-loss rows or their status totals. Migration imports and
deduplicates retained legacy loss facts. Loss totals are quality statistics and
do not change top-level status by themselves.

Current operational health is kept as low-rate `unknown`, `clear`, or
`blocked` values in the shared StateDB for quality, publication, and clock
processing. The snapshot is tied to the host boot ID and systemd invocation ID.
On identity change, prior `clear` becomes `unknown`; prior `blocked` remains
attention until recovery is recorded. These states change on events, not a
periodic heartbeat or TTL. If snapshot persistence fails, the service
supervises the writer failure, cancels collection through its audio finalizer,
and exits nonzero rather than reporting a false healthy result. See [Best
practices](BEST_PRACTICES.md#status-and-logs) for operational guidance.

Use `journalctl -u omi-collector.service` for the operational timeline. The
service's INFO stream omits routine `storage_wait` polling and detailed
`ble_link_session` records; run a diagnostic sync with `--log-level DEBUG` when
those records are needed. Detailed diagnostics go to the system journal; there
is no rotating debug JSONL state file.
