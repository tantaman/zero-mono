# Backup Archive Mode: Work Plan

|                  |                                                                   |
| ---------------- | ----------------------------------------------------------------- |
| **Design**       | ZERO_BACKUP_ARCHIVE_MODE_DESIGN.md (the what and why)             |
| **This doc**     | The PR-level breakdown of what remains, with proposed resolutions |
| **Last updated** | September 1, 2026                                                 |

Milestones are ordered by dependency, not calendar. Every PR lands dark
behind `--backup-mode` (default `litestream`); nothing activates until a
stack flips, and no production stack flips before M1–M4 are complete.

## Milestone 0 — landed (this branch)

For orientation, what already exists under
`packages/zero-cache/src/services/backup/` and its seams, all tested:

- Config: `--backup-*` group (`litestream | archive`), validation.
- Object store: interface + fs and S3 backends (conditional writes),
  lazy-loaded SDK.
- Archive: lineage/key layout, versioned segment format (zstd + SHA-256,
  decode-time rejection of corruption and malformed sequences), fail-stall
  `ArchiveWriter` wired as the change-streamer's fourth consumer with
  replay filtering, gap detection, back-pressure, and ACK gating
  (`UpstreamAcker.trackArchive`); reader with continuity verification.
- Bases + restore: manifest-last `publishBase`, `archiveRestore`
  (`RestoreResult` contract, damaged-base fallback, tail replay through
  `ChangeProcessor`), GC (retention plan + manifest-first executor).
- Protocol: optional `backupFormat` on the snapshot status message.
- Oracle layer 1 (cursor-aligned determinism at every transaction
  boundary) as vitest; health gauges.

Known deltas from the design, tracked below: the manifest lacks page size
and schema/applier versions (M2). The M1 deltas (in-memory writer,
whole-segment decode, missing commit timestamps) are resolved — M1 landed
on this branch.

## Milestone 1 — Streaming retrofit (landed, this branch)

Gate: no production flip until this lands. Everything else can proceed in
parallel, but M2's `ArchiveChangeSource` should consume the streaming
decoder from day one.

All six items below are landed. Notable resolutions beyond the plan as
written:

- **Part naming** (item 5, resolving open question 8) embeds the spanning
  transaction's commit watermark: interior parts are
  `<start>-.<watermark>.NNNNNNNN.seg` and the final part is the ordinary
  `<start>-<watermark>.seg`. The sort property is as proposed (`.` sorts
  before every watermark character), and the embedded watermark keeps
  retries idempotent — an abandoned chain's debris can never collide with
  a different transaction's chain. The writer's reconcile additionally
  deletes incomplete chains outright, so a re-sent transaction re-seals
  cleanly even if `--backup-segment-target-bytes` changed across restarts.
- The chain trigger is a distinct `partTargetBytes` writer option
  defaulting to `segmentTargetBytes` (the design's single-knob behavior);
  tests pin it independently.
- Format version 2 covers both format changes in one bump; v1 (which
  never left the lab) is not decodable.
- The memory-ceiling drill (item 6) measured ~16 MB of live heap
  archiving + replaying + restoring a 1 GiB transaction under a 192 MB
  old-space cap, in ~22 s.

1. **Object-store streaming** (S).
   `getStream(key): ReadableStream` and
   `putStreamIfAbsent(key, source, sizeHint)` on the interface; fs via file
   streams; S3 via streaming body, switching to multipart above the part
   threshold — S3 conditional writes apply to `CompleteMultipartUpload`
   too, so put-if-absent semantics survive multipart. Contract tests on
   fs; command-mapping tests on S3.
2. **Segment spool** (M). New `archive/segment-spool.ts`: append message
   lines to an **uncompressed** local spool file, tracking the byte offset
   of the last committed transaction; rollback/abort = `ftruncate` to that
   offset. Compression moves to seal time as a streaming pass
   (spool → `createZstdCompress` → sealed temp file, hashing the
   compressed bytes as they pass, then patch the header checksum in the
   local file before upload). Rationale: truncating a mid-stream
   compressed spool requires flush-per-commit framing; the uncompressed
   spool is trivially truncatable and the seal pass stays O(buffer). The
   sealed file is the upload/retry unit. Disk cost is spool + sealed file,
   both bounded and re-derivable from the slot.
3. **Writer rework** (M). Replace `ArchiveWriter`'s in-memory
   `#pending`/`#currentTx`/byte-array queue with the spool and a queue of
   sealed file paths; back-pressure keyed to on-disk bytes; delete sealed
   files once durable. The seam (`write/abort/reconcile/readyForMore/
close`, `onDurable`) is unchanged, so the change-streamer wiring and
   most tests carry over.
4. **Streaming decode for replay** (M). Replay downloads a segment to a
   local temp file via `getStream`, verifies the checksum in a streaming
   pass, then decodes line-at-a-time (`createZstdDecompress` + line
   splitter) feeding `ChangeProcessor`. Verify-then-use is preserved
   without O(segment) memory — the temp file is the buffer, and it is
   bounded by segment/part size. `decodeSegment` stays for tooling and
   small-segment tests.
5. **Parts for oversized transactions** (M). Format + layout change,
   bundled with adding first/last commit timestamps to the header (one
   format bump). Proposed naming, resolving open question 8: interior
   parts `<start>-.NNNNNNNN.seg`, final part `<start>-<end>.seg` — `.`
   sorts before every watermark character, so a chain lists as
   `05-.00000001.seg, 05-.00000002.seg, 05-0g.seg`, and only the final
   part advances the durable cursor. Continuity: consecutive part numbers
   plus a final part; a chain with no final is re-sent work, not a gap.
   Reader, writer, GC (treat a chain atomically), and corruption tests
   updated together.
6. **Memory-ceiling test** (S). A spawned-subprocess test: archive, replay,
   and restore a synthetic multi-GB transaction under
   `--max-old-space-size` far below the payload. This is M1's acceptance
   test and the permanent regression guard.

## Milestone 2 — Base producer worker

1. **`ArchiveChangeSource`** (M). Presents the **change-streamer subscribe
   surface** (`Source<Downstream>`) from sealed segments — catchup from a
   cursor, then sealed-segment polling for the live tail (freshness bound =
   seal interval; fine for daily bases). Watermark filtering lives here,
   consistent with where dedup lives today. Consumes the M1 streaming
   decoder. `IncrementalSyncer` + `ChangeProcessor` are then used
   unchanged.
2. **Worker + pragmas** (S). `server/base-producer.ts` on the
   shadow-syncer template; register in `worker-urls.ts`; gate in `main.ts`
   on mode `archive`; add a `'base-builder'` mode to `getPragmaConfig`
   (`journal_mode = OFF`, `synchronous = OFF`, exclusive locking, large
   cache), benchmarked.
3. **Publication loop** (M). Daily cadence + replay-budget early trigger
   (estimate from measured apply rate vs. archived-tail size);
   pause-at-boundary freeze → `publishBase` (exists) → resume;
   clean-shutdown marker; discard-and-rebuild on unclean start (delete
   working file, `archiveRestore` own newest base, resume tailing); invoke
   `runArchiveGC` (exists) after each successful publication. Extend the
   manifest with page size and schema/applier/log-format versions here
   (additive on the manifest's version field).
4. **Genesis** (L, and the open design decision). Producer-performed
   initial sync at the slot's consistent snapshot. Proposed resolution of
   open question 6: the gateway creates the slot and exports the snapshot
   (`pg_export_snapshot`), holding the creating transaction open while the
   producer copies; the copy lands in the producer's working file and
   publishes as the first base. The bootstrap companion owns the final
   call; build the producer-side copy against the existing initial-sync
   code (`tableCopyWorkers` etc.) so only the snapshot handoff is new.
5. **Accelerated live-base restore** (M). Decoupled request/response
   through the store itself: a restorer writes a request marker
   (`base/requests/<taskID>.json`), the producer polls, freezes, and
   publishes intent → chunks → complete at the current cursor; the
   restorer downloads chunks as they appear (separately addressable
   objects are what make this possible) and falls back to the newest
   complete base on timeout — the fallback loop in `archiveRestore`
   already exists.
6. **Producer metrics** (S): base age, publication duration, apply lag,
   replay-budget estimate, GC results.

Exit criteria: the full drill (genesis → tail → daily base → kill producer
at every publication boundary → restore) green on the fs store in CI, and
oracle layer 1 extended over producer-built bases.

## Milestone 3 — The gateway world, complete

Depends on M2 (readiness and restores need bases to exist).

1. **Gateway initialization** (L). In mode `archive`,
   `initializePostgresChangeSource` neither restores nor initial-syncs:
   identity and publications from the change DB
   (`ensureReplicationConfig`) + upstream `replicas` table; resume from
   the change-log head; outstanding backfills from change-DB cookie
   state; empty change DB + empty archive → genesis (M2.4). No
   backup-replicator, no litestream in the archive-mode process tree
   (`server/change-streamer.ts`).
2. **Purge floor + readiness** (M). Fourth producer in
   `createBackupCleanupMonitor`: poll the base listing for the newest
   complete base's cursor → `trackBackupWatermark` (purge floor, snapshot
   confirmation); readiness = first durable segment + first complete
   base; archive-stall and initial-base-deadline guardrails mirroring the
   v3 poller's wedged-backup behavior.
3. **Consumer restore selection** (M). Advertise
   `{backupURL: archiveURL, backupFormat: 'archive'}` in the snapshot
   status; `server/replicator.ts` selects `archiveRestore` on the
   advertised format; the existing post-restore `prepare()` handles
   journal-mode promotion.
4. **Resumable backfills** (M, separable PR). `ORDER BY pk COLLATE "C"`
   on the backfill copy query; last-emitted-PK progress mark in the
   cookie state; `backfillRequestsFrom` resumes from the mark. Mark
   cadence per open question 9 (start per-batch; relax if write
   amplification shows).
5. **Single-node/dev** (S). Producer in-process against a `file://`
   archive for `zero-cache-dev`.

Exit criteria: the gateway-boot tests from the design's Testing section
(cold start against populated archive; against empty bucket) green in the
`.pg.test.ts` suites; a scratch stack serves end-to-end in mode `archive`.

## Milestone 4 — Verification at scale + rollout machinery

1. **Drill tool** (M): a zero-cache subcommand that restores into a
   scratch path and runs the oracle layer-3 comparator against a live
   replica (mirror `sqlite-change-log-comparator`); scheduled in the
   archive world.
2. **Ledger workload** (M): oracle layer 2 in `apps/zero-throughput` —
   per-table count + order-independent hash maintained inside each
   Postgres transaction; a self-consistency checker runnable against any
   replica at any watermark.
3. **Chaos harness** (L): fault-injecting `ObjectStore` wrapper
   (productionize the test `beforePut` hook into error/latency/outage
   schedules), process-kill orchestration for the design's kill matrix on
   a scratch stack, PG restart/slot-churn injection; the oracle is the
   only judge.
4. **Flux machinery** (fleet repo): the mode component/values conditional,
   pre-provisioned bucket + three-way IRSA, PrometheusRules per world,
   `prune`/`wait`/healthChecks, and the forward/rollback runbooks.
5. **Flip drills**: scratch stack forward and back under chaos, then the
   first low-stakes real stack.

## Milestone 5 — Convergence metrics

`zero_apply_*` lag metrics; independent of everything above, ship whenever.

## Suggested PR sequence

M1.1 → M1.2+M1.3 → M1.4 → M1.5 → M1.6, then M2.1 → M2.2+M2.3 → M2.5 →
M2.4, then M3.1 → M3.2+M3.3 → M3.4 → M3.5, with M4.1/M4.2 startable any
time after M0 and M4.3–M4.5 after M3. Each PR keeps `litestream` mode
byte-identical and lands with its tests; the memory-ceiling and oracle
tests are ratchets — once green, they stay in CI.
