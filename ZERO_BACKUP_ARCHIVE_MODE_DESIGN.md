# Backup Archive Mode: Implementation Plan

|                   |                                                          |
| ----------------- | -------------------------------------------------------- |
| **Status**        | Proposed                                                 |
| **Companions**    | Logical Change Archive and SQLite Base Backups; Initial Sync and Base Bootstrap |
| **Last updated**  | August 31, 2026                                          |

## Purpose

The companion documents specify a backup architecture in which the committed
logical change stream is archived to S3 and SQLite bases are produced by an
independent builder, replacing Litestream as the canonical backup mechanism.

This document maps that architecture onto the existing `zero-cache` codebase as
a **separate, opt-in mode**: all existing code stays in place and continues to
be the default, a single hidden config flag selects the new behavior, and a
canary deployment opts in by setting one environment variable on its
replication-manager. Nothing changes for deployments that do not set the flag.

The short version: the codebase already has every seam this design needs. The
team has run a staged, config-gated dual-write rollout of exactly this shape
once before (`--change-streamer-sqlite-change-log-mode`), the upstream ACK
already supports gating on backup durability via a `min()` over multiple
tracked stores (`UpstreamAcker`), and restore is already behind a small result
contract that a second implementation can slot into. The new system is ~90%
additive code in a new directory, touching existing code at five bounded seams.

## The mode flag

A new `backup` option group in `packages/zero-cache/src/config/zero-config.ts`,
modeled on `changeStreamer.sqliteChangeLogMode` (hidden, enum-valued, cumulative
semantics, cross-flag validation in `assertNormalized`):

```ts
backup: {
  // hidden: true while experimental
  mode: v.literalUnion('litestream', 'archive-dual', 'archive')
        .default('litestream'),

  // s3://... or file://... - deliberately distinct from litestream.backupURL
  // so dual-run writes to a separate prefix/bucket.
  archiveURL: v.string().optional(),

  segmentTargetBytes,          // default 16 MiB
  segmentSealIntervalSeconds,  // the time-based seal; bounds archive RPO

  base: {
    maxReplaySeconds,          // primary trigger: tail-replay budget
    maxIntervalHours,          // fallback trigger when telemetry is unhealthy
    chunkBytes,
    integrityCheck: v.literalUnion('full', 'quick'),
  },

  gc: {
    enabled,                   // only honored in mode 'archive'
    retainBases,               // >= 2
    pitrHours,
  },
}
```

### Mode semantics

| Mode           | Archive writer | Base builder | PG ACK gated on | Restore source | GC  | Litestream |
| -------------- | -------------- | ------------ | --------------- | -------------- | --- | ---------- |
| `litestream`   | off            | off          | today's rules   | litestream     | n/a | authoritative (unchanged) |
| `archive-dual` | on             | on           | today's rules (archive cursor exported as metric only) | litestream | off | authoritative |
| `archive`      | on             | on           | archive durable cursor | archive (base + tail) | on  | optional safety net while `litestream.backupURL` remains set |

Mapping to the companion document's rollout plan: `archive-dual` is Phases 1-3
(dual-write, base production, restore drills via tooling), `archive` is Phase 4
(change of authority), and unsetting `litestream.backupURL` under `archive` is
Phase 5 (Litestream removal). The exit criteria in the companion document gate
each transition; the flag makes each transition a config change, not a deploy.

Validation and role handling follow existing house style:

- Cross-flag asserts in `packages/zero-cache/src/config/normalize.ts`
  (`assertNormalized`), next to the litestream-v5 dependency asserts:
  `mode !== 'litestream'` requires `archiveURL`; `gc.enabled` requires
  `mode === 'archive'`; `retainBases >= 2`.
- Flags reaching a node that is not the replication-manager warn-and-continue
  rather than fail (the pattern documented in `server/main.ts`), because a
  multi-node fleet is configured from one shared environment.
- The flag stays `hidden: true` during rollout so the `--help` snapshot test
  (`config/zero-config.test.ts`) does not churn.

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
makes the canary story clean: flip the flag on one deployment's
replication-manager and every view-syncer in that deployment follows
automatically. No cross-node flag coordination, and mixed fleets during deploys
remain correct. Protocol version negotiation already exists
(`change-streamer-http.ts`) if a breaking change is ever needed; this one is
not breaking.

## Component-to-seam mapping

### 1. Archive writer → the in-stream writer slot in `ChangeStreamerImpl`

The change-streamer already fans one committed stream out to the `Storer`
(durable PG change log), the `Forwarder` (live subscribers), and an optional
in-stream `SQLiteChangeLogWriter`
(`services/change-streamer/change-streamer-service.ts`). The `ArchiveWriter` is
a fourth consumer wired in the same constructor: it buffers transaction
envelopes into segments, seals on size or time, uploads asynchronously, and
exposes the highest **contiguous** durable cursor.

The envelope to archive is exactly the existing `ChangeStreamData` protocol
(`services/change-source/protocol/current/downstream.ts`) — the
`['begin'|'data'|'commit', ...]` tuples with watermarks — which is already what
the SQLite change log persists per transaction
(`services/replicator/change-log-db.ts`). The segment format is essentially
"sealed, compressed, checksummed ranges of the stream we already persist,"
which satisfies the companion document's requirement to archive the exact
envelopes the applier consumes, by construction.

One deliberate difference from the SQLite change-log writer: that writer is
fail-soft (disables itself on error, replication continues). The archive writer
in mode `archive` must be **fail-stall**: on upload failure the durable cursor
stops advancing, PG feedback stops advancing, and WAL-retention alerts fire, as
the companion document requires. In `archive-dual` a stalled archive only
degrades the dual-run metrics.

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
- `archive-dual`: `trackArchive: false`. Export the would-be archive cursor and
  its lag behind the actual ACK as metrics — this is the dual-run validation
  signal for Phase 1.
- `archive`: `trackArchive: true`. During the soak period the litestream-v5
  gate can stay on as well; `min()` over all tracked stores is strictly safe.

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
  purely in cursor space per the companion document.

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

Mode selects the implementation at the two existing call sites: the
replication-manager (`selectAndRestoreReplica` in
`services/change-source/pg/change-source.ts`) and the view-syncer
(`server/replicator.ts`, driven by the advertised `backupFormat` rather than
local config). The companion document's "start the durable path concurrently
with the accelerated path and promote whichever verifies first" fits inside
this one function without the callers knowing.

Two existing behaviors carry over for free:

- `no_backup` on the RM falls through to initial sync, which gives the archive
  mode sane empty-bucket behavior with no new code.
- The post-restore `prepare()` step (`workers/replicator.ts`) already forces
  `journal_mode = delete` and then the target journal mode on restored files,
  which handles the "base built with `journal_mode = OFF` carries file format
  1" promotion requirement from the companion document.

### 5. Base builder → a new worker that reuses the apply path wholesale

Write an `ArchiveChangeSource` that reads sealed segments from S3 and presents
the same `Downstream` stream the change-streamer's subscribe API produces. The
base builder is then `IncrementalSyncer` + write-worker + `ChangeProcessor`
**unchanged** — the determinism requirement (bases contain exactly what the
applier would produce) is satisfied because it *is* the applier.

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
  restore path, as the companion document requires.
- The accelerated live-base restore is a request/response between the restore
  coordinator and this worker (publish `intent.json`, upload chunks, publish
  `complete.json`), reusing the same publication code as the periodic base.

### 6. Object store → a new abstraction, needed regardless

There is no S3 client in `zero-cache` today; all object-store I/O goes through
the litestream subprocess. `apps/zbugs` already depends on
`@aws-sdk/client-s3`, so the dependency is precedented in the monorepo. Define
a minimal interface (immutable put-if-absent, get, list, head) with S3 and
filesystem backends. The `file://` backend is what makes full restore-drill
integration tests and local development possible, matching litestream's own
file-URL support.

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

Existing files touched (the five seams plus config):

- `config/zero-config.ts`, `config/normalize.ts` — the flag and validation.
- `services/change-streamer/change-streamer-service.ts` — wire `ArchiveWriter`,
  add `trackArchive` to the acker wiring.
- `services/change-streamer/upstream-acker.ts` — third tracked watermark.
- `services/change-streamer/backup-cleanup-monitor-factory.ts` — fourth
  watermark producer.
- `services/change-streamer/snapshot.ts` — optional `backupFormat` field.
- `services/change-source/pg/change-source.ts`, `server/replicator.ts` —
  restore-path selection.
- `server/main.ts`, `server/worker-urls.ts` — the builder worker.

Everything under `services/litestream/` is untouched.

## Delivery sequence

Each step is independently shippable and `litestream` mode stays green
throughout.

1. **Flag + object store + segment format.** Config declaration and
   validation, the object-store abstraction with both backends, the versioned
   segment format with round-trip and corruption tests. No behavior change.
2. **Archive writer under `archive-dual`.** Wired into the change-streamer,
   with metrics: contiguous durable archive cursor, archive lag, and the
   ack-vs-archive-cursor delta. This is companion Phase 1's dual-write.
3. **Archive reader + restore + drill tooling.** `ArchiveChangeSource`,
   `archiveRestore`, and a drill/compare tool that restores into a scratch
   path and diffs logical content against the live replica — mirror the
   `sqlite-change-log-comparator` pattern. Companion Phase 2.
4. **Base builder worker.** Publication protocol, clean-shutdown marker,
   discard-and-rebuild, and first-base bootstrap via the "promote a live
   replica" cutover (freeze the backup-replicator's file, verify, publish).
   Companion Phases 2-3.
5. **Mode `archive`.** `trackArchive` in the acker, the snapshot-protocol
   field, purge-floor rekeying, GC, accelerated live-base restore. Companion
   Phase 4.
6. **Convergence metrics** (`zero_apply_*`). Orthogonal to the mode flag and
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

**The Litestream-cutover precondition already holds in production.** The
"promote a live replica" path requires the atomic applied-cursor invariant;
that is how the replicator works today (data and `_zero.replicationState`
commit in one transaction), so the cheap first-base path is safe and should be
the default cutover.

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

## Testing

- Unit: segment format round-trip, corruption/gap/overlap rejection, manifest
  discipline, GC cutoff computation.
- Integration (fs object store): full loop — initial sync → dual-write →
  freeze/publish → delete replica → `archiveRestore` → logical diff against
  the source. Crash injection at every publication boundary (segment upload,
  intent, chunk, manifest, pointer), unclean builder shutdown, and restore
  fallback with the live producer failed mid-upload, per the exit criteria.
- Dual-run comparison in `archive-dual`: the drill tool from step 3 run on a
  schedule, plus the ack-vs-archive-cursor metric.
- The existing `.pg.test.ts` multi-config vitest setup covers the
  PG-integration variants; no new test infrastructure is needed.

## Canary procedure

1. Deploy a stack with `ZERO_BACKUP_MODE=archive-dual` and
   `ZERO_BACKUP_ARCHIVE_URL` pointing at a prefix distinct from the litestream
   bucket path. Litestream remains authoritative; blast radius is extra S3
   writes and one extra worker.
2. Watch the dual-run signals: contiguous-cursor gaps (should be zero),
   archive lag vs ACK, base publication cadence vs the replay budget, drill
   results.
3. Promote the canary to `ZERO_BACKUP_MODE=archive` once the companion
   document's Phase 4 criteria hold. Litestream keeps running as the safety
   net until `litestream.backupURL` is unset.
4. Roll back at any point by reverting the env var; `litestream` mode is
   untouched by any of this code.

## Open implementation questions

1. Does the archive writer consume the stream synchronously in the
   change-streamer loop (like the SQLite change-log writer) with async sealing,
   or fully async with its own queue? Synchronous framing with async upload is
   the proposed default; the storer's back-pressure mechanism
   (`readyForMore`) is the model if segment buffering needs flow control.
2. Where does the builder's live tail come from in `archive-dual` — polling
   sealed segments (simple, adds seal-interval latency to base freshness) or a
   change-streamer subscription with archive catchup (fresher, more wiring)?
   Proposed: sealed-segment polling first; the freshness bound equals the seal
   interval, which is acceptable for base production.
3. Do we store archive continuity metadata (highest contiguous cursor) only in
   S3 pointer objects, or also in the change DB next to `replicationState`?
   Proposed: S3 is authoritative (survives RM loss); the change DB copy is a
   cache.
4. Chunk size and parallelism defaults for base upload/download, to be set
   from drill measurements.
5. Whether `archive-dual` should also exercise view-syncer restores from the
   archive on an opt-in sub-flag (a `compare`-style read percentage, as
   `sqliteChangeLogMode` did) before `archive` flips the default.
