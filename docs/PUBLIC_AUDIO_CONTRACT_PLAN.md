# Public Audio Contract Plan

Status: **Collector implementation complete; coordinated cutover pending**
Parent: OMIC-49  
Implementation: Collector v2 implemented; activation is tracked by OMIC-55
Premium adversarial review: **GO**

## Decision

Omi Collector will replace its hardware-record public handoff with one standard `audio.ogg` file and one compact
`manifest.json` passport per publication. Collector will publish all retained audio, validate and unpack pendant
records, normalize usable timestamp labels, and mux the encoded Opus packets without transcoding. VAD, speech
selection, dialogue policy, and long-term speech artifacts remain downstream responsibilities.

This coordinated cutover is not a compatible extension of `records.bin`. Old artifacts drain under the old contract;
new artifacts start only after both producer and consumer implementations are ready.

## Public artifact

Each visible `ready/<id>/` directory contains exactly `audio.ogg` and `manifest.json`.

Both files are immutable after the directory becomes visible. The manifest is canonical UTF-8 JSON with sorted keys,
compact separators, and one trailing newline. Unknown fields are rejected.

An illustrative complete manifest is:

```json
{
  "audio_sha256": "7c2f...64 lowercase hexadecimal characters...",
  "boundaries": [
    {
      "at_packet": 100,
      "facts": ["source_loss", "time_incomparable"]
    }
  ],
  "format": "omi-audio-v2",
  "id": "06d4...64 lowercase hexadecimal characters...",
  "order": 74,
  "packet_count": 150,
  "timeline": [
    {
      "packets": [0, 100],
      "quality": "known",
      "unix_seconds": 1792300000,
      "utc_uncertainty_seconds": 2.0
    },
    {
      "packets": [100, 150],
      "quality": "unknown",
      "source_seconds": 412
    }
  ]
}
```

### Audio profile

`omi-audio-v2` fixes these byte-producing choices:

- mono Opus packets already validated as 20 ms;
- no transcoding, packet synthesis, or inserted silence;
- 960 Ogg granule samples per audio packet;
- pre-skip zero, output gain zero, and mapping family zero;
- input sample-rate metadata 16,000 Hz;
- empty vendor and comment lists;
- 50 audio packets per writer flush;
- Ogg logical-stream serial zero because each file has one stream.

Packet coordinates count Opus audio packets only; headers, pages, granules, seconds, and pendant records are not
alternative public coordinates. `[10, 35)` is exactly 25 retained packets and therefore 500 ms of encoded audio.

Collector MUST preserve validated packet bytes and source order. It validates that every emitted packet is mono and
20 ms. Current records never split an Opus packet between records: a record-end overflow is an opaque tail sentinel,
and the next contiguous record carries the complete packet. When the sentinel size matches that packet, Collector
discards the tail and emits the complete packet with the next record's timestamp label. At a cohort cut, the expected
size remains on the existing private SQLite publication frontier until checked against the first packet of the next
contiguous cohort. A mismatch splits publications at a conservative `time_incomparable` boundary; it does not prove
missing source coverage or block valid audio. A finally closed unmatched tail is ignored without inventing a boundary;
a separately confirmed source gap causes `source_loss`. A malformed complete packet still blocks the relevant source.
OMIC-53 tests cross-cohort matching and mismatch. Package and Collector acceptance exercises real extraction and EOS.

### Identity and order

`id` is opaque and immutable. At Prepare, Collector persists a random stage nonce, reserved order, format/config digest,
and private source-slice identities. It derives the ID as a domain-separated hash of those frozen private inputs, not
from the eventual audio or manifest. Resume revalidates and reuses the same inputs and ID. The ledger later binds the
ID to exact audio and canonical-manifest hashes. An intentional restage gets a new nonce and ID; nondeterministic output
for the same stage blocks. Consumers MUST NOT reproduce the derivation.

`order` is an unsigned monotonic feed position, reserved with the `next_public_order` increment in one SQLite
transaction when the stage is created. A recovered stage reuses it; an abandoned reservation leaves a gap and is never
reused. The first value is initialized only when v2 activation confirms no v2 stage or publication exists.

Order reflects accepted source order beyond the committed private publication frontier. Exact replay is deduplicated.
Material arriving behind that frontier must authenticate against active or retired custody or remain blocked; it is
never inserted later with a misleading order. Directory, ID, and wall-clock order are not substitutes.

Order adjacency does not assert uninterrupted recording. An order gap means only that a reserved value was not published.

### Timeline

`timeline` MUST exhaustively cover `[0, packet_count)` with positive integer half-open spans in ascending order. Spans
meet exactly and never overlap. `packet_count` is positive and agrees with the packets parsed from `audio.ogg`.

Each span has exactly one of these shapes:

| Quality | Required time fields | Meaning |
|---|---|---|
| `known` | `unix_seconds`, `utc_uncertainty_seconds` | Calendar mapping is established with ordinary confidence. |
| `approximate` | `unix_seconds`, `utc_uncertainty_seconds` | Calendar mapping is usable but explicitly approximate. |
| `unknown` | `source_seconds` | An unanchored source-clock label, never Unix time. |

UTC uncertainty covers calendar mapping, not acoustic capture time. Every emitted packet inherits the exact normalized
public label of the physical record containing that complete packet; Collector never interpolates across records. The
SD worker labels records, and one partially packed record can span a microphone pause. The manifest claims neither an
exact capture instant nor silence duration.

At Prepare, the descriptor freezes each record's public label and uncertainty, causal clock-segment membership, and
comparability with its predecessor. This includes unknown-clock membership and uncertainty boundaries that current
clock projection can otherwise drop. These facts are frozen through a versioned canonical ClockState snapshot, the
initial predecessor frontier, and authenticated immutable source slices, rather than a second per-record copy. Prepare
and resume stream that basis to reproduce the projection and compare its domain-separated digest, exact audio hash,
and canonical manifest hash. The current clock state never changes an already prepared artifact. A span is maximal
only while exact public label, quality, uncertainty, causal
membership, and predecessor comparability all match and no boundary intervenes. A private offset or observation change
may coalesce only when those public and causal facts remain identical. Equal labels on opposite sides of uncertain
causality remain separate and carry `time_incomparable`.

Unknown labels never create an epoch, boot identity, or global clock. Labels may be subtracted across a span or bundle
boundary only for consecutive packet/order positions without `time_incomparable`. The producer MUST put that fact at
bundle position zero unless its private clock segment establishes comparability with the immediately preceding order.
An order gap is incomparable; quality alone never grants comparability. Positive and negative jumps retain source order.

### Boundaries

`boundaries` is sorted by `at_packet`. Entries are unique by packet position
and contain a non-empty, sorted set drawn from:

- `source_loss`: retained source coverage has a confirmed gap;
- `time_incomparable`: labels on opposite sides cannot safely be subtracted.

Positions from zero through `packet_count`, inclusive, are valid. Multiple facts at one position share one entry.
Absence of `source_loss` does not prove that the microphone recorded continuously.

Zero-packet records create no empty timeline spans and contribute no duration. They remain as ordinary subthreshold
SQLite capture rows until a future mixed cohort qualifies; an all-zero draft is never retired through an empty artifact
or tombstone. Proof custody includes every physical normalized record, including zero-packet records. Boundary facts
carried by such a record stay at the current packet position and appear on the next emitted packet/artifact.

## Field decisions

| Current public concept | v2 decision |
|---|---|
| Bundle identity | Keep as opaque `id`; bind privately to audio and descriptor. |
| `records_sha256` | Replace with `audio_sha256`. |
| Absolute pendant sequences | Private only; replace public use with `order` and boundaries. |
| Record count and record size | Remove; expose `packet_count`. |
| Raw draft SHA-256 | Private custody only. |
| Offset and observation ID | Private normalization evidence only. |
| UTC confidence and uncertainty | Keep as result semantics in timeline spans. |
| Unmapped device time | Keep as `unknown`/`source_seconds`, never Unix time. |
| Hardware gaps | Replace with packet-position `source_loss` facts. |
| VAD decisions and open speech tail | Downstream only; absent from producer ACK parsing. |

## Private replay custody

The current guarantee compares every complete normalized 444-byte record. V2 preserves it after `records.bin` and
capture rows are gone. Retired custody stays unchanged: exact full-source lineage alone is accepted; partial overlap is
rejected. Existing SQLite gains one active proof root and bounded pages, with no sidecar, raw archive, or history table.

The proof row stores mutable stage ownership and `staging`/`active` lifecycle outside the authenticated content. Its
immutable `proof_content_sha256` excludes those two fields and binds:

- artifact ID;
- audio SHA-256 and canonical manifest SHA-256;
- normalized-record count and page metadata index;
- private physical runs/gaps and admitted source lineage;
- packed normalization/boundary runs;
- all other immutable proof metadata and ordered page hashes.

The staging-to-active transaction changes ownership/lifecycle without changing authenticated content. SQL constraints
and the READY ledger enforce the allowed transition; any immutable field change requires a newly verified content hash.

`publication_record_index` is zero-based physical normalized-record custody order and includes zero-packet records. It
is independent of public packet coordinates and private source ordinals. Private physical runs map hardware sequence
to this record index; a separate packet projection maps records to emitted packets.

Each proof page has at most 128 ordered hashes of complete normalized records plus first index, count, and page SHA-256.
The root carries the ordered page-hash index. Once validated and cached against the ledger revision, a partial replay
loads only touched pages and checks their indexed hashes; it neither rereads all proof BLOBs nor adds a Merkle framework.

A valid proof with a mismatching candidate is a device replay conflict and follows existing conflict quarantine. A
missing, malformed, or root/page-mismatched proof is instead a private storage-integrity failure: retain incoming data,
enter the existing blocked/fail-closed storage path, and never fall back to rereading Ogg/READY or repeatedly rebuilding
proofs. SQLite busy and ordinary I/O faults retain their existing bounded transient classification.

Normalization runs use an exact 32-byte big-endian record:

```text
first_publication_record_index:u64
next_publication_record_index:u64
offset_seconds:f64       # +0.0 for raw, finite IEEE-754 binary64 for mapped
mode:u8                  # 0 = raw/UTC-null, 1 = mapped
boundary_before:u8       # bit 0 source_loss, bit 1 time_incomparable
reserved:6 bytes         # all zero
```

Runs are positive, sorted, non-overlapping, and exhaustive. `raw` preserves the four-byte device timestamp; `mapped`
uses exactly:

```text
floor(device_timestamp + offset_seconds + 0.5)
```

The result fits uint32 and the other 440 bytes remain unchanged. Before commit, streamed public boundaries must match
the frozen run flags and source gaps; manifest and proof hashes bind that projection after source retirement.

The hash/run subtotal is `32R` hash bytes plus at most `32R` normalization-run bytes: normally about 7.2% and at worst
about 14.5% of 444-byte raw records. Capacity and acceptance measurements also include page indices, source lineage,
physical runs, the frozen clock basis and projection digest, staging overlap, SQLite pages, WAL/journal growth, and
transaction margin. No per-record packet-projection table or duplicate raw audio is retained. The
existing host-space reserve is extended to cover those v2 costs before Prepare; insufficient reserve enters the
existing storage-blocked path without persisting stage intent. No new option, metric, or physical database ratio is
needed or promised. Retaining capture rows until all dependent ACKs would delay reclamation and require a
many-publication guard; proof pages preserve early retirement.

## Publication and crash outcomes

| State/event | Required outcome |
|---|---|
| Before Prepare | Refresh the existing host-filesystem reserve for Ogg, proof/index/lineage, staging overlap, SQLite/WAL/journal growth, and headroom; insufficient space stays storage-blocked with no stage intent. |
| Prepare | Freeze stage nonce, source slices, order, format/config digest, descriptor/causal inputs, pending sentinel expectation, ownership, and proof expectations in SQLite. No public file exists. |
| Stream write | Write Ogg, manifest inputs, proof pages, timeline, and boundaries in bounded chunks. Do CPU work and file fsync outside long SQLite write leases. |
| Crash during write | Resume only an exactly owned stage after validating frozen progress; foreign or conflicting scratch blocks. |
| Files complete | Validate packet count, audio hash, descriptor hash, proof root, and boundary projection; fsync both files and the stage directory. |
| Rename | Atomically rename the complete directory to `ready/<id>` and fsync its parent. |
| Crash after rename, before commit | Reconcile visible bytes against the owned stage and proof root, then commit exactly once; do not admit another cohort. |
| Commit | In one SQLite transaction, write the READY ledger binding, flip proof `staging` to `active`, advance coverage, and remove stage rows. |
| Ambiguous commit | Inspect exact ledger, stage, proof, and files; accept only the pre-state or complete post-state. |
| Source retirement | First commit durable source-coverage authorization containing the validated coverage digest; then delete capture records in bounded pages. |
| Matching active replay | Apply the frozen run, hash the complete normalized record, compare the relevant vector entry, and suppress the duplicate. |
| Mismatching active replay | Preserve the existing blocking conflict evidence and fail closed. |
| ACK | Validate every `(id, audio_sha256)` against the active or retired private ledger before mutating any entry. |
| ACK transaction | Atomically change READY to the existing retired ledger and delete its active proof root/pages. Durable source-coverage authorization remains until bounded capture deletion completes. Then remove public files restartably and fsync the parent. |
| Crash or ACK racing capture GC | Resume capture deletion from its coverage digest even if ACK already deleted active proof, then resume public-file removal. Retired exact-lineage replay remains idempotent; partial overlap is rejected. |

Proof staging and READY commit cannot leave an active ledger without proofs or reusable proofs without a matching
ledger. Cleanup removes only exact task-owned proof rows and files.

## ACK projection

The checkpoint path is `publication_root.parent/work/omi-ready-checkpoint.json` (relative
`work/omi-ready-checkpoint.json`). During normal v2 operation, a missing file means an empty ACK set and authorizes no
deletion. Collector extracts `acknowledged`; other top-level fields remain consumer-owned and are ignored. The public
projection is:

```json
{
  "acknowledged": [
    {"audio_sha256": "<64 lowercase hex>", "id": "<64 lowercase hex>"}
  ]
}
```

Every acknowledged entry has exactly `id` and `audio_sha256`. Duplicates, unknown IDs, stale hashes, malformed entries,
or a present checkpoint with missing/non-list `acknowledged` fail closed. ACK irrevocably asserts durable results and
tail state, with no open tail, retry, cache, or future computation needing the artifact. Collector may then immediately
retire ledger/proof and public bytes. A matching retired `(id, audio_sha256)` is an idempotent no-op without files or
active proof; the same retired ID with a stale hash is rejected. Consumer crash tests cover results, tail, checkpoint,
and ACK.

## Isolated Collector muxer

Collector owns a small standard-library-only Ogg muxer module. It receives Opus packets and writes or hashes an Ogg
stream; it imports no Collector state, Bluetooth, clock, publication, or pipeline code. An architecture test enforces
this boundary. The existing v2 adapter fixes the public audio profile and checks the expected result.

The implementation reuses the vetted muxer source with provenance recorded, but has no build-time or runtime
relationship with Windmill or the private `audio-primitives` repository. Collector requires no external wheel,
repository credentials, or shared deployment. This replaces the initial shared-package decision at the operator's
request; the separately completed package is not a Collector dependency.

Acceptance includes deterministic retry bytes, packet byte/order round-trip, Ogg CRC and page behavior, real
ffmpeg/ffprobe decode, granule duration, and the fixed serial-zero profile. Decoder tools are validation tools, not
runtime dependencies. Downstream chooses its own implementation against the public artifact contract.

## Durable cutover

One live cutover follows private proof, the embedded muxer, Collector v2, and downstream v2 gates. The proof cluster may land first on the
branch, but is not a separate production activation by default. The old production binary cannot enter the new mode,
so installation begins with a transition-capable Collector release operating in `legacy_open`, plus a verified v2
consumer candidate installed but paused. That Collector temporarily supports the legacy writer before activation and
the v2 writer afterward; it never permits both formats to publish concurrently.

Collector adds a durable publication contract mode in existing SQLite:

```text
legacy_open -> quiescing -> v2_open
```

`BeginContractCutover` moves to `quiescing` under the device/publication lock. Collection and sealing continue; new
material may join an unqualified cohort, but none becomes accepted. Existing accepted/staging legacy cohorts finish.

Activation follows this restartable checklist:

1. Install and restart the transition-capable Collector in `legacy_open`; install the v2 consumer candidate paused.
2. Enter durable `quiescing` and verify Collector restart preserves forward recovery.
3. Finish every legacy accepted/staging cohort and owned output stage.
4. Have the legacy consumer drain every visible READY input and durably settle all results, open tail, and ACKs.
5. Run legacy ACK retirement; require zero legacy READY entries/directories and zero accepted/staging cohort or stage,
   while preserving every unqualified/subthreshold SQLite source unchanged.
6. Stop and freeze the legacy checkpoint writer. The consumer proves legacy cursor, VAD decisions, and open tail are
   drained, then atomically writes their v2-empty values plus `acknowledged: []`, preserving unrelated fields and never
   reinterpreting legacy IDs. A missing or invalid v2 checkpoint cannot activate; an interruption remains `quiescing`
   and is restartable.
7. Initialize the v2 order counter and change `quiescing` to `v2_open` in one checked SQLite transaction, then start
   the paused v2 consumer. No mode transition occurs before compatible code and checkpoint are present.
8. Verify one real v2 artifact, processing result, ACK, proof GC, completed capture GC, and unchanged subthreshold custody.
9. After that live v2 proof, remove legacy producer, consumer, and checkpoint-migration paths in coordinated releases
   that pass each repository's protected release gates.

If any prerequisite is unsettled, mode stays quiesced on the old side. Retired history is not reconstructed, public
bytes are never rewritten, old READY is not backfilled, and historical restore is out of scope.

## WorkGraph decomposition

| Child | Owner and files | Dependencies | Acceptance |
|---|---|---|---|
| OMIC-50 Contract and review | Collector owner; this plan and README link | None | Final premium review returns GO with every finding resolved before implementation. |
| OMIC-51 Current-format private proof | Collector owner; proof codec/staging, current `records.bin` replay/ACK behavior, and focused tests | OMIC-50 | Current-format proof commit, partial replay, source/capture GC authorization, ACK GC, and extended existing host-space reserve pass. This does not claim the full v2 crash matrix or a separate production activation. |
| OMIC-52 Initial muxer package experiment | Completed independently; no Collector dependency | OMIC-50 | Isolated wheel and real decode were verified. The operator superseded shared packaging with an embedded, architecturally isolated Collector module under OMIC-53. |
| OMIC-53 Collector v2 writer | Collector owner; v2 writer, manifest, ACK, contract mode, migration, metrics, docs, and tests | OMIC-50, OMIC-51 | Embedded muxer isolation, packet parity, schema, causal timeline, cross-cohort sentinel, zero-packet custody, identity/recovery, ACK race, retired `audio_sha256`, reserve, and the full v2 crash matrix pass. |
| OMIC-54 Consumer and checkpoint | Dagster/pipeline owner; discovery/parser, VAD input, passport/checkpoint, results/tail crash tests | OMIC-50 | Reads Ogg directly, validates invariants, proves/reset drained legacy cursor/VAD/tail, preserves result semantics, and emits ACK only after irrevocable release. No Collector edits. |
| OMIC-55 Joint cutover and removal | Collector owner plus Dagster operator | OMIC-53, OMIC-54 | Transition release, quiesce/checkpoint migration, real v2 publish/process/ACK/GC, and subthreshold survival succeed before legacy removal; both repositories pass release gates. |

Tests belong to the behavior-changing ticket; there is no QA-only ticket. Dagster runtime and pipeline code remain
outside Collector ownership and require the explicit OMIC-54 handoff.

## Source evidence

- `ready_bundles.py`: `_convert_record`, active/retired overlap, mismatch feed splitting, stage commit, ACK, and retirement invariants.
- `collector_state_db.py` and `internal_audio_store.py`: retained typed data and the atomic publication boundary.
- `opus_duration.py`: producer inspection marks record-end overflow but does not assemble a split packet.
- `tests/test_ready_conflict_quarantine.py`, `tests/test_staging_attempts.py`,
  `tests/test_staging_publication.py`, and `tests/test_staging_recovery.py`:
  current overlap, conflict, schema, and restart expectations.
- `audio-primitives/.../ogg_opus.py`: vetted source provenance for the embedded standard-library muxer; not a dependency.
- `tests/test_ready_bundles.py`: current mismatch behavior expects three feed groups rather than publication failure.
- `tests/test_ready_sql_publication.py`: equal clock mappings separated by uncertain causality stay distinct.
- `ready_bundles.py`: the existing pre-stage host-space check accounts for output, SQLite growth, journal, and headroom.
- `omi-pipeline` `flat_ready.py` and `bundle_audio.py`: current parser discards an overflow tail, validates the next
  complete packet's size, and defines Ogg, timeline, tail, and ACK coupling.

## Premium review

Status: **GO for implementation** (2026-10-18).

Round 1 returned NO-GO from both reviewers. The plan resolved zero-packet custody, physical versus packet indexing,
overflow semantics, causal timeline membership, pre-output identity, proof completeness and space bounds, ACK/GC
races, checkpoint migration, transition bootstrap, and ticket ownership. Source adjudication rejected split-packet
assembly: firmware carries only a size sentinel at record end and the next record carries the one complete packet.

Round 2 returned GO from Codex Astra at high effort and Claude Opus at low effort, with no Critical or High findings.
The accepted Opus clarifications now cover cross-cohort sentinel state, the existing host-space reserve, drained legacy
checkpoint fields, retired-ledger audio hashes, and the serial-zero wrapper test. Its claim that no host-space check
existed was rejected: current `ready_bundles.py` already checks output, SQLite growth, journal, and headroom before stage
intent; OMIC-51/53 extend that mechanism for v2 costs. This receipt approves the design and decomposition. Runtime
behavior remains unverified until the implementation tickets and their gates pass.
