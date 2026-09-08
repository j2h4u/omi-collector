# Transfer-quality evidence

`quality.jsonl` is a bounded, rotating JSONL journal directly beneath the
collector root. A daemon writer fsyncs complete lines without delaying BLE
collection; a full queue or failed write is reported to the separate debug
ring and never prevents audio staging. Rotation keeps the configured number of
bounded backups.

`advertisement_observation` is written as soon as a fresh scanner advertisement
wakes a connection attempt. It records the scanner-provided RSSI and shares a
`session_id` with a later `transfer_session` when that attempt reaches `READ`.
Fallback attempts without a fresh scanner sample do not emit this event.

`transfer_session` appears once when a physical session that issued `READ` terminates. Its `active_read_elapsed_ms` covers READ work only, so per-session mean/median and pooled throughput should use `written_raw_bytes / active_read_elapsed_ms` (or the analogous received counter) from these terminal records. `written_raw_bytes` is writer-thread output, not a claim that the bytes were fsynced; sealed staging artifacts remain the durability authority. Do not average precomputed speeds: none are stored. `advertisement_rssi_dbm` is the latest scanner advertisement sample that woke the session, not connected-link or HCI RSSI, and may be null. `source_revision` is the first 12 characters of validated `OMI_COLLECTOR_SOURCE_REVISION`, or null when deployment did not supply it.

`sequence_loss` appears only after a confirmed cursor-ahead gap. Sum `missing_record_count` and `missing_raw_bytes` to aggregate confirmed loss. It deliberately omits sequence ranges, record identities, and loss seconds: ring record size and Opus packing cannot establish audio duration. Both event types carry release and firmware context when known.

## Operator summary

The read-only status command combines the current firmware observation, visible
published bundles, and a bounded quality window:

```bash
uv run omi-collector device status \
  --layout /var/lib/omi-collector/collector.toml \
  --device-slug omi-cv1 --hours 24
```

Its top-level `status` is `ok` when the window contains a completed transfer
and no confirmed loss or terminal failure, `attention` as the degraded state
when confirmed loss or a latest fatal, cancelled, or teardown-interrupted
transfer is present, and
`unknown` when there is no completed transfer to assess. `publication` reports
the currently visible bundle inventory. `quality_window` reports recent
advertisements, transfer sessions, pooled bytes per second, outcome counts,
termination-class counts, and confirmed loss totals. A null `device` means no
firmware observation has been recorded for that device.

Use `journalctl -u omi-collector.service` for the operational timeline. The
service's INFO stream omits routine `storage_wait` polling and detailed
`ble_link_session` records; run a diagnostic sync with `--log-level DEBUG` when
those records are needed. The rotating `debug.jsonl` ring contains the
lower-level sync and BLE diagnostics separately from the journal.
