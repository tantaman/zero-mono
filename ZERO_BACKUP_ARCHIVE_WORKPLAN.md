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

## Milestone 2 — Base producer worker (landed, this branch)

All six items below are landed. Notable resolutions beyond the plan as
written:

- The freeze is pause-at-boundary end to end: `ArchiveChangeSource` gained
  `holdAtBoundary()`, which parks the stream at the next transaction
  boundary with everything before it consumed (per-message back-pressure
  makes "consumed" mean "committed by the applier"); a hold that resolves
  "stream died" publishes nothing and the session fails into
  discard-and-rebuild. This is what makes `journal_mode = OFF` safe on the
  working file.
- Genesis (item 4) resolves open question 6 along the proposed line, with
  the handoff as a request/response through the store (like item 5):
  `genesis.ts` carries the offer/heartbeat protocol, and `initialSync`
  gained a `providedSnapshot` mode (real replica, no upstream mutations —
  the gateway owns the slot and the `replicas` record). The gateway side
  (`awaitGenesisBase`, holding the snapshot transaction open) is consumed
  by M3.1.
- Item 5's chunk overlap is a prefetch cache: `requestLiveBase` downloads
  the in-flight publication's chunks as they appear, and `archiveRestore`
  consults the cache with unchanged verification (a stale entry costs a
  re-download, never correctness).
- The replay-budget estimate is a declared heuristic (compressed tail ×
  assumed expansion / measured apply rate) for the M4 drills to calibrate.

Exit-criteria status: the genesis → tail → base → unclean-kill → restore
drill is green at the service level (in-memory store, in-process worker);
the full fs-store subprocess drill and extending oracle layer 1 over
producer-built bases remain, tracked with M4's kill matrix. The
`providedSnapshot` initial-sync mode needs a `.pg.test.ts` exercising it
against real Postgres (CI has the container infrastructure; this
environment does not).

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

Status: items 1–3 are landed on this branch. Item 4 landed its ordered
emission and resume plumbing: backfill COPYs are always `ORDER BY` the
row key (text-family columns `COLLATE "C"` so the order is bytewise and
stable across locale/ICU changes), `backfillRequestSchema` gained an
optional `resumeAfter` key, and `streamBackfill` restricts a resumed copy
to rows strictly after that key (reported totals reflect the remaining
rows; unsupported key values fall back to a full restart, which is always
correct since backfills are idempotent). Two consequences worth knowing:
backfilled rows now reach the replica in row-key order rather than
upstream heap order (which is what makes the last applied row a valid
resumption point), and a table with no row key at all — no primary key
and no replica identity index, so it is not synced anyway — gets no
cursor and keeps the unordered download.

The durable progress mark then landed too, which is what makes item 4
actually resume rather than merely be able to. The `cookieOps` fold gained
one op, `advance-backfill`, folded from the `backfill` batch itself — the
first _data_ change the cookie jar folds, and it is there because the mark
has to survive a restart and only the change log can hold it. All three
stores carry it beside the backfilling column they already held
(`cdc.backfilling`, `_zero.changeLogBackfilling`, `_zero.backfilling`,
bumping the change-log DB to v5 and the replica schema to v18), and
`backfillRequestsFrom` turns it into the request's `resumeAfter`. Points
worth keeping in mind:

- A table resumes only when _every_ backfilling column agrees on the mark.
  A column whose backfill started later has no rows before the others'
  mark, so the table restarts — which is the only thing that populates it.
- The mark is stored as text in Postgres rather than `jsonb`, unlike every
  other cookie document: an `int8` key past 2^53 does not survive a round
  trip through jsonb's JS representation, and a rounded mark is a resume on
  the wrong row. For the same reason `keyValueLiteral` renders bigints.
- The mark is a hint, not a fact the stores must agree on, so the
  change-log initialization comparison ignores it. Losing one costs a
  restart of something idempotent; losing a `backfilling` row costs a
  column that is never populated.
- Every change source must now emit backfill rows in row-key order across
  the whole backfill, which `backfill.rowValues` documents. The Postgres
  source does, by item 4; a source that did not would have rows before the
  mark silently skipped.

Item 5 landed as three small changes
rather than new machinery: in development mode (`zero-cache-dev` sets
`NODE_ENV=development`) an unset `--backup-archive-url` defaults to a
`file://` store next to the replica file; a serving replicator in mode
`archive` restores from the archive even when the change-streamer is
local (the local gateway owns no replica file — the in-process base
producer publishes the first base and the restore picks it up); and the
dispatcher never selects `serving-copy` in mode `archive` (there is no
local replica to copy, whatever `--litestream-backup-url` says). The
single-node process tree was already wired: `main.ts` loads the base
producer whenever the task runs the change-streamer in mode `archive`.

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

   _Landed on this branch_ as `zero-archive-drill`. Alignment is
   pin-then-restore: a read transaction freezes the live replica at its
   watermark W, the drill briefly waits for the archive's durable head to
   reach W (bounded; a stall reports `archive-behind`), then restores a
   scratch replica point-in-time via a new `upTo` option on
   `archiveRestore` (base selection ≤ W, tail replay bounded at W —
   mid-segment works because replay filters by commit watermark) and
   diffs the two logically: per-table column signatures plus an
   order-independent 128-bit sum of per-row content hashes (physical row
   order and rowids may differ between materializations; logical content
   may not). Runtime-history bookkeeping (`_zero.changeLog*`,
   `_zero.runtimeEvents`, wall-clock'd config/state rows) is excluded by
   default; identity and watermark are checked exactly instead. Exit code
   0 only on `match`, so scheduling it is a cron job.

2. **Ledger workload** (M): oracle layer 2 in `apps/zero-throughput` —
   per-table count + order-independent hash maintained inside each
   Postgres transaction; a self-consistency checker runnable against any
   replica at any watermark.

   _Landed on this branch_ behind `--ledger` (off by default: the ledger
   row serializes each table's writers, so it is for chaos/correctness
   runs, not throughput measurement). `ledger.ts` is the single spec both
   sides derive from: per-table triggers maintain
   `zero_throughput_ledger` (count + sum of 56-bit md5 row hashes mod
   2^64, stored as text so it replicates byte-identically) inside every
   transaction, hashing exactly the columns whose pg→lite mapping is
   canonical (text/int/bool — every logical write bumps `seq`, so stale
   row versions still show); `ledger-check.ts` (`pnpm run ledger-check`,
   plain `node:sqlite`) recomputes the aggregates over any replica and
   exits non-zero on divergence. The JS spec and checker are validated
   end to end (match, stale row, missing row, absent table), and the
   generated trigger SQL has had its first run against a real Postgres
   16: the trigger-computed count and hash match the JS encoding exactly
   over multibyte text, NULLs, booleans, negative and above-2^53
   integers, and update and delete deltas.

3. **Chaos harness** (L): fault-injecting `ObjectStore` wrapper
   (productionize the test `beforePut` hook into error/latency/outage
   schedules), process-kill orchestration for the design's kill matrix on
   a scratch stack, PG restart/slot-churn injection; the oracle is the
   only judge.

   _The wrapper is landed on this branch_: `ChaosObjectStore` applies a
   `ChaosPolicy` (seeded-random error/lost-response/latency rates,
   scripted outage windows, composable) to every operation, with an
   `error-after` decision for the "upload landed, response lost" case the
   deterministic-name retry discipline exists for, and per-run stats. A
   ratchet test drives the archive writer to a contiguous durable head
   through 30% upload errors + 20% lost responses. The process-kill
   orchestration and PG restart injection on a scratch stack remain (they
   need a running stack, which this environment cannot host).

4. **Flux machinery** (fleet repo): the mode component/values conditional,
   pre-provisioned bucket + three-way IRSA, PrometheusRules per world,
   `prune`/`wait`/healthChecks, and the forward/rollback runbooks.
5. **Flip drills**: scratch stack forward and back under chaos, then the
   first low-stakes real stack.

## Milestone 5 — Convergence metrics (landed, this branch)

`zero_apply_*` lag metrics; independent of everything above, ship whenever.

Landed as two instruments in `IncrementalSyncer` (so every consumer of the
change stream reports them — serving replicas and the base producer alike,
distinguished by a `mode` attribute): `zero.apply.commits` counts
transactions applied to the local replica, and `zero.apply.lag` is a
latency histogram of `now - upstreamCommitTimeMs` at apply time (the same
clock-domain crossing as `zero.replication.total_lag`).

## Suggested PR sequence

M1.1 → M1.2+M1.3 → M1.4 → M1.5 → M1.6, then M2.1 → M2.2+M2.3 → M2.5 →
M2.4, then M3.1 → M3.2+M3.3 → M3.4 → M3.5, with M4.1/M4.2 startable any
time after M0 and M4.3–M4.5 after M3. Each PR keeps `litestream` mode
byte-identical and lands with its tests; the memory-ceiling and oracle
tests are ratchets — once green, they stay in CI.

## Branch status

Everything this repository can carry is landed on this branch: M1, M2,
M3, and M5 in full (with the deltas noted in each milestone's status
block), plus M4's in-repo pieces — the drill tool (M4.1), the ledger
workload and checker (M4.2), and the chaos `ObjectStore` wrapper
(M4.3's wrapper).

The suites have been run against a real Postgres 16 (`wal_level=logical`,
`TEST_PG_16=...`, which is the same escape hatch the dev container uses):
`zero-cache/pg-16` and `zero-cache/no-pg` pass apart from tests that need
IPv6, which the sandbox they ran in does not have. That run is what caught
the two backfill regressions fixed above (the empty `ORDER BY` for a table
with no row key, and the emission-order expectation in
`change-source.backfill.pg.test.ts`), so ordered emission and resume are
now validated against Postgres rather than only in unit tests.

### End-to-end against zbugs (single-node, `file://` archive)

The single-node path has now been run end to end against `apps/zbugs` on a
real Postgres 16 — `--backup-mode=archive`, no replica file, no litestream,
no object-store configuration (M3.5's dev default puts the archive next to
the replica path). It works, and getting there took three fixes, each of
which only ever failed outside the unit tests:

- **Cold-boot deadlock.** `main.ts` started the base producer only after
  awaiting change-streamer readiness, but genesis blocks the change-streamer
  until the producer publishes the first base. Nothing ever answered the
  offer; genesis abandoned it after the heartbeat timeout and the process
  exited 255. The producer now starts alongside the change-streamer.
- **Genesis left the `replicas` row unusable.** The producer's initial sync
  deliberately leaves that record to the gateway, and the gateway never
  wrote it, so the read-back that ends `performGenesis` always returned null
  ("genesis created no replica at version X") — after the copy and the base
  publication had both succeeded. The gateway now records `initialSchema`
  (read at the offered snapshot) and `initialSyncContext`.
- **Tail replay could not decode a real segment.** Both segment decoders
  parsed change-stream messages strictly, while the wire parses them in
  `passthrough` mode; the Postgres source's `begin`/`commit` carry
  `commitLsn`, `commitTime` and `xid`, so every archived transaction was
  rejected on replay. The synthetic transactions the tests build carry only
  the schema-declared fields, which is why this was invisible until a real
  upstream produced a segment.

What the run then demonstrated: genesis (2,089 rows, 13 tables) performed by
the producer at the gateway's exported snapshot; the first base published and
the serving replica restored from it with the gateway holding no replica file
of its own; live inserts, updates and deletes flowing to sealed log segments;
a restart resuming the existing lineage rather than re-running genesis; and
`zero-archive-drill` reporting `match` — a replica restored purely from the
archive is identical to the live one, before and after the restart.

Two environment limits are worth recording, since neither is a defect in
this work: `http-service.ts` binds `::`, so every test and every worker that
listens fails on a host without IPv6; and `view-syncer/inspect-handler.ts`
uses a `using` declaration, which needs the Node 24 that CI runs (`.nvmrc`
and `engines` both say 22).

### Still outstanding

What remains needs infrastructure this environment does not have: the
scratch-stack chaos orchestration, the Flux machinery (fleet repo, M4.4),
the flip drills (M4.5), and the follow-ups called out inline (the fs-store
CI drill, oracle layer 1 over producer bases). The zbugs run above is
single-node and hand-driven; it is not a substitute for the CI drill.
