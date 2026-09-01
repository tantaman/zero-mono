# Backup Archive Mode: Implementation Plan

|                  |                                                                                                         |
| ---------------- | ------------------------------------------------------------------------------------------------------- |
| **Status**       | Proposed                                                                                                |
| **Companions**   | Logical Change Archive and SQLite Base Backups; Initial Sync and Base Bootstrap                         |
| **Origin**       | [Replace Litestream Backups discussion](https://chatgpt.com/share/6a96150b-0a24-83e9-bc5d-842305ccd958) |
| **Last updated** | September 1, 2026                                                                                       |

## Purpose

The companion documents specify a backup architecture in which the committed
logical change stream is archived to S3 and SQLite bases are produced by an
independent builder, replacing Litestream as the canonical backup mechanism.

This document does two things. It fixes the **end state**: the
replication-manager becomes a **gateway** that owns the replication slot and
the committed stream but holds **no replica** — every SQLite replica in the
fleet is materialized by the backup producer (the base builder) or restored
from one of its published bases. And it maps it onto the existing `zero-cache`
codebase as a **separate, whole-world mode**: all existing code stays in place
and remains the default, and a single config value flips a stack between the
two worlds. There is **no dual-write mode** — a stack is in the litestream
world or the archive world, never both. Rollback is the config flipped back
plus a resync. Nothing changes for stacks that do not flip.

The short version: the codebase already has every seam this design needs. The
upstream ACK already supports gating on backup durability via a `min()` over
multiple tracked stores (`UpstreamAcker`), restore is already behind a small
result contract that a second implementation can slot into, and resync — the
rollback mechanism — is an existing, exercised code path. The new system is
~90% additive code in a new directory, touching existing code at six bounded
seams.

## Goals

This replaces the existing replication-manager + replica setup outright. The
end system is:

1. **S3 logical logs.** The committed change stream, archived as sealed,
   checksummed segments; the durable artifact of record.
2. **Daily bases.** The backup producer publishes a SQLite base roughly once
   a day; everything between bases is served by the log. The tail-replay
   budget can trigger a base early, but the steady-state cadence is daily.
3. **Log GC.** Retention is bases-plus-covering-logs with a PITR window;
   segments at or below the oldest retained base's cursor are collected.
4. **Speedy recovery.** A view-syncer that needs a replica receives a fresh
   base concurrently copied from the producer's live file (the accelerated
   live-base restore) rather than replaying up to a day of tail — restore
   latency is bounded by copy bandwidth, not by base age.
5. **Initial sync through S3.** Postgres is table-copied once per lineage —
   by the producer — and every other node syncs from S3. No fleet-sized
   fan-in on the upstream database, ever.

And the rollout model:

6. **A per-stack config flip, no dual writes.** A stack is flipped into the
   archive world by one value in its Flux-managed configuration; forward is
   a resync into S3 (producer genesis), rollback is the value flipped back
   and a resync into the litestream world. The safety story is cheap,
   symmetric rollback — not a dual-run. See "Rollout: one value per stack,
   reconciled by Flux".

Two cross-cutting constraints shape every mechanism below:

- **Streaming, low resident memory.** No component may hold a transaction —
  or any unbounded section of the stream — in memory. Bounded buffers, disk
  spools, and streaming codecs everywhere. See "Streaming and memory
  discipline".
- **A path to pgoutput protocol v2.** Nothing may assume a transaction
  arrives contiguously, or fits anywhere but disk before its commit: v2
  streams large in-progress transactions in interleaved chunks. See "Toward
  logical replication protocol v2".

## The end state: a gateway replication-manager

The replication-manager's replica exists today because Litestream needs a
physical SQLite file to back up: the backup-replicator worker applies the
stream to it, and litestream ships its pages. Make the backup logical and that
file loses its reason to live on the RM. The end state removes it entirely.

**The replication-manager is a gateway.** It owns the replication slot,
archives the committed stream (the archive writer), persists the stream for
catchup (the PG and SQLite change logs), serves subscriptions, and coordinates
snapshots and purging. It holds no replica: no replica file, no
backup-replicator worker, no litestream process, and no restore step at
startup. Its durable state is the change DB, the change logs, and the archive.

**The backup producer is the only component that materializes SQLite.** At
lineage genesis it performs initial sync and publishes the result as the first
base (per the Initial Sync and Base Bootstrap companion); thereafter it tails
the archive through the real apply path and publishes bases on the daily
cadence (with the tail-replay budget as an early trigger). Its own recovery is
a restore of its own newest base (discard-and-rebuild), so even the producer's
working file is producer-derived.

**Every other replica is a restore of a producer base.** View-syncers restore
base + tail and then catch up live over their subscription, exactly as they
restore from litestream today — only the format changes. A single-node
deployment composes gateway and view-syncer in one process; the serving
replica it holds is a consumer-restored replica, not a gateway artifact.

What makes the replica removable:

- **Resume.** The stream's resume point never actually came from the replica:
  the change-streamer resumes from the change-log head
  (`ChangeLogInitializer` / the storer's start-stream parameters), and the
  replica's subscription state supplies generation identity plus a
  cross-check. That identity (`replicaVersion`, publications) is already
  duplicated in the change DB (`ensureReplicationConfig` reconciles them) and
  the upstream `replicas` table. The gateway reads identity and resume from
  those, with the durable archive cursor — S3 authoritative, change-DB copy
  as a cache — as the floor that ACK gating (`trackArchive`) makes safe:
  nothing above it is ever acknowledged, so Postgres re-sends whatever the
  archive lacks.
- **Backup production.** The replica's real job — being the thing that gets
  backed up — moves wholesale to the producer, which _is_ the applier reading
  the archived envelopes. Determinism improves: there is exactly one
  materialization path, and its output is checksummed and published.
- **Genesis.** An empty archive no longer falls through to an RM-resident
  initial sync. The gateway records the new lineage (a fresh
  `replicaVersion`) and begins archiving from the slot's consistent point;
  the producer performs the table copy and publishes the first base.
  Readiness gating keys off the first durable segment plus the first complete
  base, so nothing serves from a lineage that cannot yet be restored.
- **The SQLite change log stands alone.** It is named after and seeded beside
  the replica file today purely as a convention; its identity (generation,
  replicaID) comes from subscription state, which the gateway carries in the
  change DB.

What this deletes from the RM: the backup-replicator worker, the litestream
subprocesses, restore-before-serving at startup (faster failover, near-zero
local state), and the per-change SQLite write amplification of applying the
stream locally. What it deletes from the system: the divergence class between
"the replica that gets backed up" and "the replica that gets restored" — they
are the same artifact by construction.

Mode `archive` **is** this world. There is no intermediate mode in which the
RM keeps its replica with litestream as a safety net: the safety net is that
flipping the value back and resyncing returns the stack to the litestream
world wholesale. Confidence comes from restore drills and from flipping
low-stakes stacks first, not from a dual run.

## Streaming and memory discipline

The rule: resident memory is O(bounded buffer) — never O(transaction),
O(segment), or O(base). A bulk `UPDATE` touching every row of the largest
table must flow through the gateway, the archive, the producer, and a
restoring consumer without any of them growing their heap. Per component:

**Archive writer: spool, don't buffer.** The in-stream writer appends each
message's JSON to a local disk spool through a streaming zstd encoder as the
message arrives, and tracks the spool offset of the last committed
transaction. A rollback or interrupted stream truncates the spool back to
that offset; memory holds the compression window and offset bookkeeping,
nothing proportional to the transaction. Sealing finalizes the spool file
(checksum computed over it in a streaming pass) and hands it to the uploader,
which streams it to the store — multipart above the part threshold — without
loading it. The durable cursor still advances only at fully-uploaded commits,
so ACK semantics are unchanged. _The current `archive-writer.ts` buffers the
open segment and the upload queue in memory, bounded only by back-pressure;
replacing that with the spool is required work, tracked in the delivery
sequence._

**Transactions larger than a segment: parts.** A segment normally seals at a
commit boundary, but a transaction larger than the target size must not force
an unbounded segment. A transaction may therefore span segments: interior
parts end mid-transaction, the part chain is part of the continuity check,
and only the part carrying the `commit` advances the durable cursor — so a
crash mid-chain re-sends the whole transaction, exactly as an unsealed
segment would today. (Exact part naming is an open question below.)

**Archive reader and applier: stream the replay.** Tail replay and the
producer's `ArchiveChangeSource` stream a segment body (streaming/ranged GET)
through a streaming zstd decoder, parse one message line at a time, and feed
`ChangeProcessor` — which already applies message-by-message inside a single
SQLite transaction. At no point is a whole segment or transaction resident.
_The current `decodeSegment` buffers and fully parses a segment; it remains
the right tool for verification tooling, but the replay path needs a
streaming counterpart._

**Bases: already streaming.** The publisher reads fixed-size chunks through
one reusable buffer and hashes incrementally; restore downloads chunks with
bounded concurrency to fixed offsets. Memory is O(chunkBytes × concurrency)
on both sides, independent of base size.

**Object store: grow streaming reads and writes.** The minimal interface
gains `getStream`/`putStream` (S3: streaming body / multipart upload; fs:
file streams) for segment upload and replay; whole-object `get`/`put` remain
for manifests and pointers, which are small by construction.

**The gateway itself is already streaming.** The change-streamer forwards
message-by-message with flow control and the storer batches boundedly;
nothing in this design may regress that by inserting a transaction-sized
buffer into its loop.

## The mode flag

A new `backup` option group in `packages/zero-cache/src/config/zero-config.ts`
(hidden while experimental, cross-flag validation in `assertNormalized`):

```ts
backup: {
  // hidden: true while experimental
  mode: v.literalUnion('litestream', 'archive').default('litestream'),

  // s3://... or file://... - deliberately distinct from litestream.backupURL.
  archiveURL: v.string().optional(),

  segmentTargetBytes,          // default 16 MiB
  segmentSealIntervalSeconds,  // the time-based seal; bounds archive RPO

  base: {
    maxIntervalHours,          // primary cadence: one base per day (default 24)
    maxReplaySeconds,          // early trigger when tail replay would exceed budget
    chunkBytes,                // default 64 MiB; deliberately NOT segmentTargetBytes
    integrityCheck: v.literalUnion('full', 'quick'),
  },

  gc: {
    enabled,                   // default true; an emergency off switch.
                               // Only read in mode 'archive'.
    retainBases,               // >= 2
    pitrHours,
  },
}
```

The two object sizes are independent knobs on purpose, because they are
sized by different forces. **Log segments** (`segmentTargetBytes`, 16 MiB)
are sized by latency: the durable cursor — and with it PG feedback — advances
only when a segment uploads, so the seal size bounds archive lag, ACK lag,
and WAL retention, and segments are also the streaming-replay read unit.
**Base chunks** (`base.chunkBytes`, 64 MiB) sit on a path where no cursor
waits on any single object: they are sized for bulk-transfer throughput on
files that can reach hundreds of GiB — large enough to amortize per-request
overhead and keep object counts sane (a 100 GiB base is ~1,600 chunks at
64 MiB), small enough for retry granularity, pipelined download-during-upload
on the accelerated path, and the O(chunkBytes × concurrency) memory bound on
both sides. Tuning one must never drag the other along.

### Mode semantics

Two worlds, one value:

| Mode         | World                                                                                                                                                                             |
| ------------ | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `litestream` | Today's system, unchanged: RM + backup-replicator + litestream, litestream restores, RM initial sync.                                                                             |
| `archive`    | The gateway world, whole: archive writer (fail-stall), ACKs gated on the durable archive cursor, base producer (genesis + daily bases + accelerated restore), S3 restores, GC on. |

### Flipping forward, flipping back

**Forward** (`litestream` → `archive`): the stack starts a **new lineage**.
The gateway takes the slot at a fresh generation and the producer performs
initial sync through S3 (genesis → first base); view-syncers restore from S3
once readiness (first durable segment + first complete base) clears. The cost
is one resync — the same cost as any resync today — which is what makes the
flip safe to do per stack, low-stakes stacks first. Nothing migrates: the old
litestream backups and replicas are simply no longer consulted. (If forward
resync cost ever bites for very large stacks, seeding the first base from the
last litestream replica is a possible accelerator — extra machinery,
deliberately not on the critical path.)

**Rollback** (`archive` → `litestream`): flip the value back and the existing
code does the rest. The RM boots in the litestream world, attempts a
litestream restore, and finds either nothing or a backup from a **stale
generation** — which the existing `replicaVersion` validation rejects
(`invalid_replica`) — so it falls through to the existing auto-reset → initial
sync path: a fresh generation, a fresh slot, a fresh litestream backup
lineage. No manual database surgery, no new rollback code; rollback correct-
ness rests on machinery that runs in production today. The abandoned archive
lineage is inert (nothing reads it) and is reclaimed out of band.

Because both directions are resyncs into the target world, there is no state
that must survive the flip, no window in which both systems write, and no
compatibility matrix between worlds.

Validation and role handling follow existing house style:

- Cross-flag asserts in `packages/zero-cache/src/config/normalize.ts`
  (`assertNormalized`): `mode === 'archive'` requires `archiveURL`;
  `retainBases >= 2`. `gc.enabled` defaults on and is simply unread outside
  mode `archive`.
- Flags reaching a node that is not the replication-manager warn-and-continue
  rather than fail (the pattern documented in `server/main.ts`), because a
  multi-node fleet is configured from one shared environment.
- The flag stays `hidden: true` during development so the `--help` snapshot
  test (`config/zero-config.test.ts`) does not churn.

### Why view-syncers do not need the flag

View-syncers learn where and how to restore from the replication-manager over
the existing snapshot-reservation protocol
(`services/change-streamer/snapshot.ts`), whose status message already carries
`{backupURL, replicaVersion, minWatermark}`. We add one optional,
backward-compatible field:

```ts
backupFormat?: 'litestream' | 'archive'
```

The view-syncer restores in whatever format the RM advertises. This is what
makes the per-stack flip clean: flip the value on a stack and every
view-syncer in that stack follows the replication-manager automatically. No
cross-node flag coordination, and mixed pods during a Flux reconcile remain
correct. Protocol version negotiation already exists
(`change-streamer-http.ts`) if a breaking change is ever needed; this one is
not breaking.

## Component-to-seam mapping

### 1. Archive writer → the in-stream writer slot in `ChangeStreamerImpl`

The change-streamer already fans one committed stream out to the `Storer`
(durable PG change log), the `Forwarder` (live subscribers), and an optional
in-stream `SQLiteChangeLogWriter`
(`services/change-streamer/change-streamer-service.ts`). The `ArchiveWriter` is
a fourth consumer wired in the same constructor: it appends transaction
envelopes to the open segment, seals on size or time, uploads asynchronously,
and exposes the highest **contiguous** durable cursor. Per the streaming
discipline, the open segment lives in a local disk spool behind a streaming
compressor — never in memory — and a transaction larger than the segment
target spans segment parts.

The envelope to archive is exactly the existing `ChangeStreamData` protocol
(`services/change-source/protocol/current/downstream.ts`) — the
`['begin'|'data'|'commit', ...]` tuples with watermarks — which is already what
the SQLite change log persists per transaction
(`services/replicator/change-log-db.ts`). The segment format is essentially
"sealed, compressed, checksummed ranges of the stream we already persist,"
which satisfies the companion document's requirement to archive the exact
envelopes the applier consumes, by construction.

For timestamp-based PITR, segment headers should additionally record the
first and last upstream commit timestamps they cover (the commit envelopes
carry `commitTimeMs`), so a wall-clock restore target can be mapped to a
cursor without decoding segments. The format's version field makes this an
additive change; it is not in format v1.

One deliberate difference from the SQLite change-log writer: that writer is
fail-soft (disables itself on error, replication continues), because it is a
cache. The archive writer is authoritative and therefore **fail-stall**,
always: on upload failure the durable cursor stops advancing, PG feedback
stops advancing, and WAL-retention alerts fire, as the companion document
requires. With no dual mode there is no fail-soft variant to maintain.

Object names are deterministic from stream identity and cursor interval
(`log/v1/<stream-id>/<start>-<end>.zst` under the archive prefix), so upload
retries are idempotent or fail loudly.

### 2. PG feedback gating → `UpstreamAcker`

`services/change-streamer/upstream-acker.ts` ACKs upstream at the `min()` of
the stores it is told to track, currently
`{trackPgChangeLog: true, trackBackup: litestreamVersion === 'v5'}` (wired in
`ChangeStreamerImpl`). The companion document's acknowledgement rule —
"feedback advances only through the durable archive cursor" — is a third
tracked watermark:

- Add `trackArchive: boolean` and a `trackArchive(watermark)` method, driven by
  the archive writer's contiguous-durable callback.
- Mode `archive` sets `trackArchive: true`, unconditionally — the archive is
  authoritative from the moment the stack flips. The cursor's lag behind the
  last committed watermark is exported as the primary archive-health metric.
- Mode `litestream` is unchanged (`trackArchive: false`; the v5 backup gate
  where configured).

Note that the litestream-v5 path already established the precedent of gating
the replication slot on backup durability, so mode `archive` introduces a new
participant in an existing mechanism, not a new mechanism.

### 3. Purge floor and snapshot confirmation → the backed-up-watermark plumbing

Change-log cleanup and view-syncer snapshot confirmation are keyed off the
backed-up watermark today (`ChangeStreamerImpl.trackBackupWatermark`, the
cleanup floor of `min(backupWatermark, subscriber acks, reservations)`). Two
changes:

- `createBackupCleanupMonitor()`
  (`services/change-streamer/backup-cleanup-monitor-factory.ts`) is already a
  config-selected three-way switch producing one `Source<BackedUpWatermark>`
  (replica poller / v3 Prometheus poller / v5 VFS poller). Mode `archive` adds
  a fourth producer whose watermark is the **cursor of the newest complete
  base** — that is the point from which a restored node needs catchup, so it is
  the correct floor for the PG/SQLite change logs.
- S3-archive GC (retain >= 2 verified bases, retain log segments covering the
  older retained base through the current cursor, PITR window) is a separate
  new service, active only in mode `archive` with `gc.enabled`, operating
  purely in cursor space per the companion document. Where cross-region
  bucket replication is configured, its completion is an additional GC
  precondition: a segment must not be collected in the primary region before
  the base that supersedes it is durable in the replica region.

### 4. Restore → the `RestoreResult` contract at both call sites

`tryRestore` (`services/litestream/commands.ts`) returns
`'success' | 'no_backup' | 'invalid_replica' | 'error'`, and `restoreReplica`
(`services/change-source/common/replica-restore.ts`) falls through to initial
sync on `no_backup`. The new `archiveRestore()` implements the same contract:

1. Select the newest complete base manifest (`complete.json`); no manifest
   means `no_backup`.
2. Download chunks in parallel to fixed offsets; verify per-chunk and
   whole-file checksums.
3. Open and validate: embedded cursor equals the manifest cursor, schema and
   applier versions compatible (`getSubscriptionState` validation exists).
4. Replay the logical tail from the archive to the target cursor.
5. Set serving SQLite configuration and promote atomically.

In the archive world the contract's callers are consumers only: the
view-syncer (`server/replicator.ts`, driven by the advertised `backupFormat`
rather than local config) and the producer's own discard-and-rebuild. The
replication-manager's litestream restore call site
(`selectAndRestoreReplica` in `services/change-source/pg/change-source.ts`)
belongs to the litestream world and is simply not exercised in mode `archive`
— the gateway holds no replica to restore. The companion document's "start
the durable path concurrently with the accelerated path and promote whichever
verifies first" fits inside `archiveRestore()` without the callers knowing.

Two existing behaviors carry over for free:

- An empty archive is handled by producer genesis (initial sync → first
  base), with view-syncer readiness gated until the first base is complete —
  the same shape as today's `waitForBackupBeforeServing`.
- The post-restore `prepare()` step (`workers/replicator.ts`) already forces
  `journal_mode = delete` and then the target journal mode on restored files,
  which handles the "base built with `journal_mode = OFF` carries file format
  1" promotion requirement from the companion document.

### 5. Base producer → a new worker that reuses the apply path wholesale

Write an `ArchiveChangeSource` that reads sealed segments from S3 and presents
the same `Downstream` stream the change-streamer's subscribe API produces. The
base producer is then `IncrementalSyncer` + write-worker + `ChangeProcessor`
**unchanged** — the determinism requirement (bases contain exactly what the
applier would produce) is satisfied because it _is_ the applier. In the end
state this worker is the **only** thing in the system that materializes a
SQLite replica from Postgres data; everything else restores its output.

- Spawn it as a new optional worker on the `server/shadow-syncer.ts` template
  (register in `server/worker-urls.ts`, gate in `server/main.ts` on
  `runsChangeStreamer(config) && backup.mode !== 'litestream'`). This puts it
  in the RM's process tree for the canary while leaving a clean path to
  deploying it as a separate node later — it only talks to S3, so
  failure-domain separation is a deployment decision, not a code change.
- Throughput pragmas have an existing seam: `getPragmaConfig(mode)` in
  `workers/replicator.ts` already varies pragmas by replica file mode. Add a
  `'base-builder'` mode (`journal_mode = OFF`, `synchronous = OFF`,
  `locking_mode = EXCLUSIVE`, large cache), benchmarked individually per the
  companion document.
- The freeze / verify / chunk-upload / manifest-last / reopen cycle, the
  clean-shutdown marker, and the discard-on-unclean-start rule are new code in
  the builder worker. Discard-and-rebuild is just "delete file, run
  `archiveRestore` from the latest complete base, resume tailing" — its own
  restore path, as the companion document requires. Pieces are never uploaded
  from a file that remains mutable; the alternative to pausing the applier
  for the freeze is SQLite's Online Backup API, which yields a consistent
  snapshot file while the applier keeps running — worth adopting if the
  pause ever shows up in apply-lag (at the origin discussion's observed
  60-80k rows/sec apply rate, the daily pause is expected to be negligible).
- The manifest should also carry the database page size and the schema /
  applier / log-format versions (the origin discussion's manifest field
  list); the implemented v1 manifest carries cursor, sizes, and hashes, and
  its version field makes the rest additive when the producer lands.
- At lineage genesis the producer owns initial sync: the table copy lands in
  the producer's working file and is published as the first base. This is
  also the forward-flip path — flipping a stack into the archive world _is_ a
  genesis. (Seeding the first base from a frozen litestream-era replica —
  "promote a live replica" — remains a possible accelerator for very large
  stacks, but is deliberately not built until forward-resync cost proves to
  need it.)
- The accelerated live-base restore is a request/response between the restore
  coordinator and this worker (publish `intent.json`, upload chunks, publish
  `complete.json`), reusing the same publication code as the periodic base.
  Chunks being **separately addressable objects** — rather than parts of one
  S3 multipart upload, which materializes only at `CompleteMultipartUpload` —
  is what lets the restorer download chunks while the producer is still
  uploading them, which is the whole point of the accelerated path:
  `T_restore ≈ T_freeze + max(T_upload, T_download) + T_tail-replay`. The
  restored file is still not opened until every chunk has arrived and the
  complete manifest has verified.

### 6. Object store → a new abstraction, needed regardless

There is no S3 client in `zero-cache` today; all object-store I/O goes through
the litestream subprocess. `apps/zbugs` already depends on
`@aws-sdk/client-s3`, so the dependency is precedented in the monorepo. Define
a minimal interface (immutable put-if-absent, get, list, head — plus
`getStream`/`putStream` for the segment upload and replay paths, per the
streaming discipline) with S3 and filesystem backends. The `file://` backend
is what makes full restore-drill integration tests and local development
possible, matching litestream's own file-URL support.

### 7. Gateway initialization → identity and resume without a replica

`initializePostgresChangeSource` currently restores (or initial-syncs) the
replica to obtain `subscriptionState` before the streamer starts. Mode
`archive` replaces that read: generation identity and publications come from
the change DB (`ensureReplicationConfig` already reconciles them) and the
upstream `replicas` table; the resume watermark comes from the change-log head
as it does today; and the durable archive cursor — S3 authoritative, change-DB
copy as a cache — is the floor below which nothing needs re-sending and above
which ACK gating guarantees Postgres still has everything. This is the one
seam that does not exist yet as a seam, and with no intermediate mode it is on
the critical path: an archive-world RM never has a replica to read.

## New code layout

```
packages/zero-cache/src/services/backup/
  object-store/
    store.ts            // interface: immutable put, get, list, head
    s3.ts
    fs.ts
  archive/
    segment-format.ts   // versioned envelope framing, compression, checksums
    archive-writer.ts   // Service: consumes committed stream, seals, uploads,
                        // exposes contiguous durable cursor
    archive-reader.ts   // continuity verification, transaction iteration
    archive-change-source.ts  // presents Downstream stream from segments
  base/
    manifest.ts         // intent.json / complete.json schemas
    base-publisher.ts   // freeze, integrity check, chunk upload, manifest-last
  restore/
    archive-restore.ts  // implements the RestoreResult contract
  gc.ts
packages/zero-cache/src/server/base-builder.ts   // worker entry point
```

Existing files touched (the seams plus config):

- `config/zero-config.ts`, `config/normalize.ts` — the flag and validation.
- `services/change-streamer/change-streamer-service.ts` — wire `ArchiveWriter`,
  add `trackArchive` to the acker wiring.
- `services/change-streamer/upstream-acker.ts` — third tracked watermark.
- `services/change-streamer/backup-cleanup-monitor-factory.ts` — fourth
  watermark producer.
- `services/change-streamer/snapshot.ts` — optional `backupFormat` field.
- `services/change-source/pg/change-source.ts`, `server/replicator.ts` —
  restore-path selection (transitional), then gateway initialization: identity
  and resume without a replica.
- `server/main.ts`, `server/worker-urls.ts` — the producer worker.
- `server/change-streamer.ts` — archive store construction and health
  gauges; in mode `archive`, no backup-replicator and no litestream in the
  process tree.

Everything under `services/litestream/` is untouched; it is simply not
exercised by a stack in the archive world.

## Delivery sequence

With no dual mode, every step lands **dark** behind `mode: 'archive'` —
`litestream` mode stays green throughout, and nothing activates until a stack
flips. The sequence orders the build; the flip of the first real stack waits
for all of it.

1. **Flag + object store + segment format.** Config declaration and
   validation, the object-store abstraction with both backends, the versioned
   segment format with round-trip and corruption tests. No behavior change.
2. **Archive writer.** Wired into the change-streamer as a fail-stall
   consumer, with the health metrics: contiguous durable archive cursor,
   cursor lag, buffered/spooled bytes.
3. **Archive reader + restore + drill tooling.** `ArchiveChangeSource`,
   `archiveRestore`, and a drill/compare tool that restores into a scratch
   path and diffs logical content against a reference replica — mirror the
   `sqlite-change-log-comparator` pattern.
4. **Streaming retrofit.** Replace the writer's in-memory open segment and
   upload queue with the disk spool (truncate-on-rollback, streaming
   checksum, streaming/multipart upload), add `getStream`/`putStream` to the
   object store, add the streaming replay decoder, and define the part
   scheme for transactions larger than a segment. Gated by the memory
   ceiling test below; must land before any production stack flips.
5. **Base producer worker.** Genesis initial sync (the forward-flip path),
   publication on the daily cadence with the replay-budget early trigger,
   clean-shutdown marker, discard-and-rebuild, GC, and the accelerated
   live-base restore.
6. **Gateway initialization + consumer restore path.** Identity and resume
   without a replica (seam 7), the snapshot-protocol `backupFormat`
   advertising, the view-syncer restore selection, purge-floor rekeying, and
   the archive-mode process tree (no backup-replicator, no litestream, plus
   the producer worker). This completes mode `archive` as a whole world.
7. **Flux machinery + flip runbook.** The one-value overlay, pre-provisioned
   bucket/IAM, mode-aware alerts, and the forward/rollback runbooks,
   exercised end to end on a scratch stack (flip forward, flip back) before
   any real stack flips.
8. **Convergence metrics** (`zero_apply_*`). Orthogonal to the mode flag and
   valuable on their own; the replicator already threads
   `upstreamCommitTimeMs` through version-ready notifications for lag
   computation. Can ship at any point.

## Codebase findings that feed back into the companion documents

These came out of the code survey and either confirm or sharpen assumptions in
the two companion documents.

**Applier idempotency (the bootstrap document's blocking prerequisite):
mostly yes, with two caveats.** Row operations are idempotent —
`ChangeProcessor` applies inserts as `INSERT OR REPLACE`, updates fall back to
an upsert when zero rows change, and deletes tolerate missing rows. But
`ChangeProcessor` has **no watermark guard**; replay suppression lives upstream
in `subscriber.ts`, so the `ArchiveChangeSource` must do watermark filtering —
which is consistent with where dedup lives today. And **DDL changes
(`create-table`, `create-index`, ...) are not idempotent**. This mostly does
not matter for tail replay, because every SQLite transaction commits the
watermark atomically with the data (`updateReplicationWatermark` runs inside
the same transaction), so a journaled replay always resumes from the embedded
cursor exactly-once, and the journal-disabled builder is covered by
discard-and-rebuild. It **does** matter for the bootstrap document's per-table
filtered catchup: DDL interleaved with the catchup window needs an explicit
answer there.

**The "promote a live replica" precondition already holds in production.**
The path requires the atomic applied-cursor invariant; that is how the
replicator works today (data and `_zero.replicationState` commit in one
transaction). With the flip-and-resync rollout it is no longer on any
critical path — forward flips are geneses — but it remains the known-safe
accelerator if forward-resync cost on very large stacks ever demands one.

**Gating the slot ACK on backup durability is not new.** The litestream-v5
path already does it (`UpstreamAcker` with `trackBackup`), including the
operational consequence that a stalled backup stalls `confirmed_flush_lsn`.
Mode `archive` changes which store gates, not whether one does.

**Archive lineage must be namespaced by `replicaVersion`.** A resync starts a
new generation; the upstream `replicas` table's per-generation `backupPath`
scheme is the existing pattern to reuse for archive prefixes, and the change-DB
truncate-and-reseed logic (`ensureReplicationConfig`) is the analog for
starting a new archive lineage.

**The wedged-backup and initial-backup-deadline behaviors need archive
equivalents.** The v3 poller shuts the RM down after a sustained backup stall
and after a missed initial-backup deadline. Mode `archive` should carry
equivalent guardrails: a stalled archive beyond a grace period is a hard
failure, and readiness gating (`waitForBackupBeforeServing` →
`firstBackupReceived`) should key off the first durable segment plus a
complete base.

## Toward logical replication protocol v2

pgoutput protocol v2 streams large in-progress transactions: chunks of
multiple transactions arrive interleaved between `Stream Start`/`Stream Stop`
markers, and a streamed transaction can end in an abort. Today zero-cache
speaks v1, where every transaction arrives whole and in commit order — which
is also the only reason the current pipeline can get away with
transaction-sized buffering anywhere.

The archive keeps its canonical contract regardless of ingest protocol:
segments contain **committed transactions in commit order**, exactly the
envelopes the applier consumes. v2 ingestion then becomes a gateway-local
concern: interleaved in-progress chunks are spooled to disk per-xid (the same
spool mechanism the writer uses, one spool per open transaction), and a
transaction enters the committed stream — and therefore the archive, the
change logs, and subscribers — only at its commit, in commit order; an abort
deletes its spool. Because the streaming discipline already forbids holding a
transaction in memory, v2's arbitrarily large in-progress transactions cost
disk, not resident memory — the discipline is what makes v2 adoptable at all.

This is deliberately _not_ "archive the raw v2 stream": keeping the archive
commit-ordered means the applier, the base producer, catchup, restore, and
every consumer are untouched by the protocol migration. The one component
that learns v2 is the PG change source; everything downstream of the
committed stream is already correct.

## Testing

- Unit: segment format round-trip, corruption/gap/overlap rejection, manifest
  discipline, GC cutoff computation.
- Integration (fs object store): full loop — apply-and-archive a stream
  through the real change processor → freeze/publish → delete replica →
  `archiveRestore` → logical diff against the source. Crash injection at
  every publication boundary (segment upload, intent, chunk, manifest,
  pointer), unclean builder shutdown, and restore fallback with the live
  producer failed mid-upload, per the exit criteria.
- Restore drills on a schedule in the archive world: the drill tool from
  step 3 restoring into a scratch path and diffing against a live consumer —
  this replaces the confidence a dual run would have provided.
- Gateway boot: cold-start the RM with no replica file against a populated
  archive and change DB, and against an empty bucket (producer genesis),
  asserting identical streaming behavior in both.
- Flip drills: on a scratch stack, flip forward (genesis through S3) and
  back (forced resync into the litestream world), asserting convergence and
  bounded unavailability in both directions — the rollback path is only a
  safety net if it is exercised.
- Memory ceiling: archive, replay, and restore a synthetic multi-GB
  transaction (and a multi-GB base) under a fixed `--max-old-space-size` far
  below the payload size. This is the acceptance test for the streaming
  discipline, and the regression guard that keeps a transaction-sized buffer
  from ever creeping back in.
- The existing `.pg.test.ts` multi-config vitest setup covers the
  PG-integration variants; no new test infrastructure is needed.

## Rollout: one value per stack, reconciled by Flux

The fleet is GitOps-managed by Flux, and the flip is more than an env var:
the two worlds have different infrastructure shapes. The machinery exists to
collapse that difference back into **one value in the stack's values file**
(`backupMode: litestream | archive`), from which everything else derives, so
that a flip — in either direction — is a single git commit that Flux
reconciles.

### The Kubernetes resource model

What each world is, as resources (the fleet repo owns the manifests; this is
their spec):

| Workload            | `litestream` world                                                               | `archive` world                                                                                                                       |
| ------------------- | -------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------- |
| replication-manager | today's shape: zero-cache + backup-replicator child + litestream, replica volume | **gateway**: same image, `ZERO_BACKUP_MODE=archive` + `ZERO_BACKUP_ARCHIVE_URL`; a spool `emptyDir`; no replica volume, no litestream |
| backup-producer     | —                                                                                | `StatefulSet`, `replicas: 1`, working replica on a `volumeClaimTemplates` PVC                                                         |
| view-syncers        | Deployment, restores from litestream                                             | same Deployment, restores from S3 (follows the RM's advertised `backupFormat` — no manifest change beyond env)                        |
| GC                  | —                                                                                | inside the producer; no CronJob                                                                                                       |

The load-bearing choices:

- **The producer is a single-replica StatefulSet, not a Deployment.** A
  StatefulSet gives at-most-one pod semantics across reschedules (base
  publication is `putIfAbsent`-safe against a race, but two producers are
  waste and noise), and its PVC makes a reschedule a resume instead of a
  rebuild. Set `persistentVolumeClaimRetentionPolicy: {whenDeleted: Delete}`:
  the working file is re-derivable from the producer's own bases, and a
  rollback that prunes the StatefulSet must not orphan PVCs across repeated
  flips.
- **The gateway carries no PVC.** Its spool is an `emptyDir` (or generic
  ephemeral volume) with a `sizeLimit` per the streaming discipline's disk
  budget — the spool is re-derivable from the slot, so losing the node loses
  nothing. This is the visible payoff of the no-replica design: the RM
  becomes a stateless-disk workload.
- **Readiness is the flip's completion signal.** The gateway's existing
  readiness endpoint gates on the first durable segment + first complete
  base, so during a forward flip the rollout simply reports not-ready until
  the new world can actually serve — Kubernetes and Flux health checks see
  the truth without any bespoke orchestration.

**IAM (IRSA), split by role and pre-provisioned.** Three ServiceAccounts,
three policies on the archive bucket: the gateway writes and lists the log
prefix (`PutObject` conditional writes, `ListBucket`, `GetObject`); the
producer has full CRUD (it publishes bases and runs GC's deletes); the
view-syncers are read-only. All three exist **in both worlds**, ahead of any
flip: an idle role and bucket cost nothing, and keeping provisioning off the
flip's critical path is what makes the flip a config change rather than an
infrastructure project. Likewise the litestream bucket and roles remain
provisioned while a stack is in the archive world; rollback does not wait on
Terraform.

### Expressing the flip in Flux

Flux's `postBuild` variable substitution alone cannot express this flip: the
two worlds differ in which **resources exist** (the producer StatefulSet,
litestream sidecars), not just in env values. So the flip is a resource-set
selection — either idiom works, and the fleet's existing convention wins:

- **Kustomize components**: the stack's `kustomization.yaml` includes exactly
  one of `components/backup-litestream` or `components/backup-archive`; each
  component carries its workload patches, extra resources, and its
  `PrometheusRule`. The flip is a one-line change:

  ```yaml
  # stacks/acme-prod/kustomization.yaml
  components:
    - ../../components/backup-archive # was: ../../components/backup-litestream
  ```

- **Helm**: a single `backupMode` value in the `HelmRelease`, with the chart
  templating the conditional resources off that one value.

Either way, one value/line — never two knobs that can disagree — and the
Flux `Kustomization` that applies it needs `prune: true` (so a rollback
actually removes the producer and its resources, rather than leaving both
worlds' workloads standing) and `wait: true` with `healthChecks` on the
gateway and producer, so the flip commit reports reconciled-and-healthy only
when the new world is serving.

**Reconcile-window tolerance.** Flux applies manifests in no useful order,
and pods of both shapes can briefly coexist during a flip. The application
absorbs this rather than the pipeline sequencing it: an archive-mode gateway
boots without the producer present (readiness gates on the first base), a
litestream-mode process ignores archive resources entirely, and
`assertNormalized` rejects internally inconsistent env combinations so a
half-templated overlay fails loudly at pod start instead of running in a
mixed state. Only one process can hold the replication slot, so the two
worlds cannot both stream regardless of pod overlap. No `dependsOn`
choreography between app Kustomizations is needed — the one hard dependency
(bucket + IAM before first archive write) is satisfied by pre-provisioning,
not by ordering.

**Forward runbook.** Commit `backupMode: archive` for the stack → Flux
reconciles → gateway starts a new lineage and the producer performs genesis →
readiness clears on the first durable segment + first complete base →
view-syncers restore from S3. Watch: archive cursor lag, base cadence and
early-trigger rate, gateway resident memory under bulk-write load, drill
results. Flip dev and staging stacks first; the blast radius of any flip is
one stack's resync.

**Rollback runbook.** Revert the commit → Flux restores the litestream shape
→ the RM rejects the stale-generation state and resyncs through the existing
auto-reset path (see "Flipping forward, flipping back"). The abandoned
archive lineage is left in place and reclaimed out of band once the stack is
confirmed healthy — never deleted as part of the rollback itself.

**Mode-aware observability.** Alerts follow the value because each world's
`PrometheusRule` ships inside its component/conditional: litestream stall
alerts in one world; archive cursor-lag, base-staleness, gap-count, and
GC-failure alerts in the other. The flip commit therefore flips the alert
set atomically with the workloads — a stack must never sit in a world whose
backup alerts are watching the other one.

## Open implementation questions

1. Does the archive writer consume the stream synchronously in the
   change-streamer loop (like the SQLite change-log writer) with async sealing,
   or fully async with its own queue? Resolved by the streaming discipline:
   synchronous framing into the disk spool with async streaming upload, and
   `readyForMore`-style back-pressure keyed to spool-plus-queue bytes on
   disk rather than heap.
2. Where does the producer's live tail come from — polling sealed segments
   (simple, adds seal-interval latency to base freshness) or a
   change-streamer subscription with archive catchup (fresher, more wiring)?
   Proposed: sealed-segment polling first; the freshness bound equals the seal
   interval, which is acceptable for daily base production.
3. Do we store archive continuity metadata (highest contiguous cursor) only in
   S3 pointer objects, or also in the change DB next to `replicationState`?
   Proposed: S3 is authoritative (survives RM loss); the change DB copy is a
   cache. This is load-bearing, not an optimization: the archive cursor is
   part of how a replica-free RM initializes.
4. Chunk size and parallelism defaults for base upload/download, to be set
   from drill measurements.
5. Cleanup of abandoned lineages: a rollback (and any resync) strands a
   lineage in the bucket. Proposed: a bucket lifecycle rule or an out-of-band
   sweep that deletes lineages other than the current generation after a
   retention delay — never as part of the flip itself.
6. Genesis coordination between the gateway and the producer: the initial
   table copy must happen at the slot's consistent snapshot, which today is
   visible only to the session that created the slot. Options: the gateway
   exports the snapshot (`pg_export_snapshot`) and holds the creating
   transaction open while the producer copies; or a one-shot bootstrap job
   performs slot creation + copy and hands the slot to the gateway. The
   bootstrap companion owns this; the gateway cutover depends on the answer.
7. Single-node and dev topology: the combined process needs a serving replica
   and is a consumer of it, but with no separate producer node, does dev mode
   run the producer in-process against a `file://` archive, or keep a local
   direct-apply path as a permanent exception? Proposed: producer in-process —
   one materialization path everywhere is most of the point.
8. The part-naming scheme for transactions that span segments (interior parts
   have no commit watermark to name an `end` by), and the spool's disk
   budget: the spool directory needs sizing guidance and a disk-full posture
   (fail-stall in mode `archive`, like an upload failure), since the
   streaming discipline trades heap for disk.
