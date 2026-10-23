# Public Audio Contract

## Scope

Collector publishes validated Opus packets in immutable Ogg Opus files with
one compact JSON manifest per artifact. It does not transcode audio. VAD,
speech selection, dialogue policy, and long-term speech artifacts belong to a
downstream consumer. The current Collector publication contract is v2 only;
legacy raw record bundles are not a public output format. Retired legacy
lineage remains private in SQLite where needed for replay and overlap rejection.

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

### Identity and order

`id` is opaque and immutable. At Prepare, Collector persists a random stage nonce, reserved order, format/config digest,
and private source-slice identities. It derives the ID as a domain-separated hash of those frozen private inputs, not
from the eventual audio or manifest. Resume revalidates and reuses the same inputs and ID. The ledger later binds the
ID to exact audio and canonical-manifest hashes. An intentional restage gets a new nonce and ID; nondeterministic output
for the same stage blocks. Consumers MUST NOT reproduce the derivation.

`order` is an unsigned monotonic feed position, reserved with the `next_public_order` increment in one SQLite
transaction when the stage is created. A recovered stage reuses it; an abandoned reservation leaves a gap and is never
reused. New databases and drained stopped-service upgrades initialize the first value to zero; an existing v4 database
must already contain a valid v2 order.

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

## Public and private fields

| Data | Public or private representation |
|---|---|
| Artifact identity | Public opaque `id`, privately bound to audio and descriptor. |
| Audio integrity | Public `audio_sha256`; raw-record hashes remain private. |
| Packet position | Public `order`, `packet_count`, timeline spans, and boundaries. |
| Pendant sequence and source records | Private custody; confirmed gaps project to `source_loss`. |
| Clock offsets and observation identifiers | Private normalization evidence. |
| Unmapped device time | Public `unknown` quality with `source_seconds`, never Unix time. |
| VAD decisions and open speech tail | Downstream state, never producer ACK fields. |

## Private replay custody

Collector keeps source records, clock/date history, and replay evidence in
private SQLite according to their custody and retention rules. Public ACK
allows covered source rows to be garbage-collected after durable authorization;
it does not mean all private history has vanished. Retired lineage metadata
remains available for exact full-source replay and overlap rejection. None of
these private values appears in the public manifest.

The private replay proof authenticates each complete normalized 444-byte source
record by hash. Proof pages contain hashes, not raw audio. A proof root binds
the artifact ID, audio and manifest hashes, private source lineage,
normalization and boundary runs, and ordered proof-page hashes.

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
| ACK transaction | Atomically change READY to the retired ledger, delete its active proof root/pages, and retain durable source-coverage authorization. Remove public files restartably and fsync the parent; bounded capture GC uses the recorded authorization. |
| Crash or ACK racing capture GC | Resume public-file removal and capture deletion from durable state even if ACK already deleted the active proof. Retired exact-lineage replay remains idempotent; partial overlap is rejected. |

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
