# OMIC-16 pre-code measurements (2026-10-13)

These are pre-code capacity measurements for the [accepted internal SQLite plan](../OMIC-16-IMPLEMENTATION-PLAN.md). The observed session rates are not instantaneous BLE peaks, and the local benchmarks are not a pass for the final writer-lag and barrier gates. No production files or services were changed for these measurements.

## Current configuration and retained observations

The active v0.12.2 service inherits `writer.chunk_records=128`, `transfer.sync_timeout_seconds=60`, `memory.arena_max_bytes=512 MiB`, and a 2 MiB notification-buffer limit. The arena admits at most 1,209,168 complete 444-byte records (536,870,592 bytes). The configured 256-record one-shot CLI cap is not the service batch cap; service batches follow the device's current INFO/write-sequence snapshot.

Read-only SQLite quality events cover 442 sessions from 2026-09-02 through 2026-10-13. Across all releases, received bytes divided by accumulated active READ time had median 38.3 kB/s and p90 75.8 kB/s. The v0.12.2 subset (15 sessions) had median 59.7 kB/s and p90 77.0 kB/s. Its largest observed session requested 294,823 records and received 130,901,412 bytes over 2,242,358 ms of active READ (~58.4 kB/s). The largest retained historical session requested 684,816 records and received 304,058,304 bytes over 5,259,722 ms (v0.9.0). These are session-average rates, not a measured sustained ingress peak. The retained events contain no per-drain latency, writer lag, or final-barrier latency.

`/var/tmp`, the collector SQLite file, and `ready/` share ext4 `/dev/sda2`; roughly 33.3 GB was available at measurement time. This is a free-space snapshot, not a migration or steady-state space pass. The main DB and ready output compete for that same capacity.

## Disposable same-filesystem comparison

A serial `nice 19`/idle-I/O fixture on `/var/tmp` used SQLite 3.53.1, 4096-byte pages, `journal_mode=DELETE`, `synchronous=EXTRA` (readback 3), and `busy_timeout=5000`. Each timed transaction inserted exact 444-byte records as a queryable uint32 timestamp and a 440-byte BLOB, then updated its source's count and streaming SHA-256 checkpoint atomically. The fixture had source and record `WITHOUT ROWID` primary-key trees, without secondary indexes. Every actual connection and in-transaction check asserted the pragmas in the corrected 1/128-record run. Timings include connection setup and close in per-call mode and begin/commit in persistent mode. Each cell had 31 samples; the host had concurrent load near 8 and warm filesystem cache.

| Drain | Connection | p50 | p95 | Final DB growth | Observed journal high-water |
| --- | --- | ---: | ---: | ---: | ---: |
| 1 record | per call | 26.23 ms | 32.23 ms | 16,384 B | 25,136 B |
| 1 record | persistent | 27.15 ms | 92.32 ms | 16,384 B | 25,136 B |
| 128 records | per call | 27.26 ms | 42.87 ms | 2,318,336 B | 49,760 B |
| 128 records | persistent | 36.45 ms | 110.98 ms | 2,318,336 B | 49,760 B |

The 1,024-record diagnostic from the first run had 12 samples per mode: p95 108.38 ms per call and 104.82 ms persistent. It is not the proposed commit cadence. That first run set EXTRA on timed connections but verified the pragma on a new connection, which reported its own FULL default; the later 1/128 run corrected the verification. Journal sizes were sampled just before commit, so they are observed values, not proven maxima. The noisy samples do not establish that either connection strategy is faster. Peak RSS change was below the fixture's measurement resolution, and there were no secondary timestamp indexes or full v2 source columns.

For comparison, the existing file-backed `StagedAttempt.accept_chunk` wrote 1/128 records with p95 0.077/28.90 ms (31 samples; no fsync). Its explicit `checkpoint()` barrier had p95 60.19/46.78 ms and includes raw fsync plus metadata persistence. The new per-drain SQL commit is a stronger and more frequent durable boundary than the old unfsynced append. All benchmark databases and generated raw files were removed after measurement.

## Bounded writer replay

An owned disposable harness on the same ext4 filesystem drove the existing `TransferArena` and `AttemptWriter` thread into a writer-thread-owned persistent SQLite target. It generated each 128-record chunk as it went, without a second whole-batch payload copy, and replayed the largest retained historical batch shape: 684,816 records / 304,058,304 bytes. The producer was paced at a **synthetic 1 MiB/s**, roughly 13 times the v0.12.2 session-average p90; this was a stress margin, not a measured BLE rate. The run used `nice 19`/idle I/O and finished in 303.87 seconds under host load near 9–11. Every ≤128-record transaction asserted DELETE/EXTRA/5,000-ms busy timeout and committed exact record rows together with source count/SHA. The provisional schema included both timestamp lookup indexes as a conservative index-cost assumption, not finalized v2 DDL.

The writer committed 5,351 drains; transaction p50/p95/max was 36.26/141.12/1,227.85 ms. Peak submitted-minus-written lag was **45,977,088 bytes** (103,552 records), below the conservative 510 MiB comparison limit. The final checkpoint barrier took **13.65 seconds**, below the configured 60 seconds, and final row count/SHA matched the independently generated stream. The synthetic producer temporarily outpaced the SQL target, but its backlog drained within the barrier. These observations say nothing definitive about instantaneous BLE peaks.

The final fixture DB occupied 435,978,240 bytes (106,440 4096-byte pages, zero freelist pages), versus 304,058,304 bytes of original record data; the observed rollback-journal high-water was 127,736 bytes. Filesystem free bytes fell from 33,329,524,736 to 32,891,998,208 before fixture cleanup. Process maximum RSS was 329,232 kB; that includes the 304 MB arena and is not an isolated incremental memory delta. The original harness omitted a timestamped pre-allocation `MemAvailable` check; the separate pre-run inventory saw about 7.9 GB available and a post-run check saw about 7.6 GB, but neither establishes the replay's exact preflight value. All generated SQLite/raw fixture data was removed. The retained [replay harness](../../scripts/bench_omic16_internal_sqlite.py) now has explicit memory/space preflight and finite cleanup; a 128-record self-check passed without repeating the large replay. Cold-cache, real RF, power-loss, full v2 source geometry, and a maximum 512 MiB admitted batch remain unmeasured.

## Capacity gate status

The accepted gate compares p95 full-drain commit time with `(128 × 444) / measured_peak_sustained_ingress_bytes_per_second`, checks peak submitted-minus-written bytes below the 512 MiB arena limit less the largest admitted notification, and requires the final checkpoint/seal barrier within 60 seconds. The numerator is 56,832 bytes. Current session-average data cannot provide the required peak sustained ingress rate. The configured notification-buffer budget provides a conservative 2 MiB upper bound on one admitted notification, leaving 510 MiB for the lag comparison; actual BLE notification size was not observed here.

The assertion-backed SQLite comparison and replay are provisional capacity signals only. At the synthetic rate, the 128-record arrival interval is about 54 ms and the replay's p95 commit time was 141 ms; the measured backlog and final barrier describe the resulting stress behavior. The accepted p95 gate requires a **measured peak sustained BLE ingress rate**, which the retained data lacks. Executable writer/scenario tests and live telemetry must close the real peak, `Q_peak`, peak memory, and final-barrier acceptance checks before release; the larger configured arena geometry also needs a release-candidate scenario.
