# Logical Log Replay: Benchmark Results

**Question.** Can backup/restore for the replication manager be "base snapshot
in S3 + logical log segments in S3" instead of litestream? We already have a CDC
stream and a component that applies it to SQLite. The number that decides it is:
how fast can a stored logical log be replayed into a SQLite replica?

**Status.** Measured. The approach is viable; the ceiling is roughly 14 MB/s of
uncompressed log against a 1 GB replica, which is ~1:20 to catch up on 1 GB of
log. Whether that is good enough is a product decision about snapshot frequency,
not an engineering one — see [Open questions](#open-questions).

---

## 1. What was built

| File | Purpose |
| --- | --- |
| `packages/zero-cache/src/services/replicator/logical-log-fixture.ts` | Deterministic CDC stream generator + table/index specs. Shared, so every benchmark replays byte-identical input. |
| `.../logical-log-apply.bench.ts` | What the **current** apply path costs. Sweeps workload × coalescing × pragma profile × replica size. |
| `.../logical-log-applier-headroom.bench.ts` | How much of that cost is **recoverable**. Compares the current path against a prototype applier and a binary log format. |

Log entries are the canonical downstream wire form — exactly what
`serializeChangeStreamData()` produces and what the change-streamer already
stores and fans out:

```
["begin",{"tag":"begin"},{"commitWatermark":"3a2"}]
["data",{"tag":"insert","relation":{...},"new":{...}}]
["commit",{"tag":"commit"},{"watermark":"3a2"}]
```

"MB of log" throughout means those bytes, uncompressed.

### Two guards, because both failure modes look like good news

1. **Row-key lookups must be indexed.** `create-table` alone produces a replica
   table with no primary key index — the PK arrives as a separate
   `create-index` message (`<table>_pkey`). Without it every update and delete
   is a full table scan; the benchmark read 4 MB/s and got *slower as the
   replica grew*, which is exactly the shape legitimate cache pressure makes.
   Setup now runs `EXPLAIN QUERY PLAN` and throws unless the lookup uses an
   index.
2. **Every replay is verified.** `ChangeProcessor` reports failures through its
   `failService` callback rather than throwing from `processMessage`, and a
   processor that has failed silently drops every subsequent message — which
   registers as excellent throughput. Each run is checked to have reached the
   log's final watermark with the expected row counts, and the headroom
   benchmark checksums every variant against the baseline's resulting replica.

---

## 2. Environment and caveats

- 4 vCPU Intel Xeon @ 2.80 GHz, 15 GB RAM, Node 22.22.2, `@rocicorp/zero-sqlite3`
  1.1.4 (SQLite 3.54.0).
- **A shared cloud VM. Treat absolute MB/s as a floor; the ratios are what
  travel.** Real replication-manager hardware has better I/O and more cache.
- **Not disk-bound.** Moving the replica to tmpfs buys ~10%, so these are CPU
  numbers.
- **Not measured:** S3 download, decompression, restoring the base snapshot,
  concurrent read load during replay, and replicas well past 1 GB.

---

## 3. Headline results

Against a **977 MB replica** (2M rows), mixed workload, replay pragmas:

| | value |
| --- | --- |
| Replay throughput, current path | **9.2 MB/s** |
| Replay throughput, prototype applier | **14.3 MB/s** |
| 1 GB of log, current path | 2:06 |
| 1 GB of log, prototype applier | ~1:20 |
| 1 GB of log, stored in S3 | **72 MB** (gzip -1, 14.4×) |

Storage and transfer are not the constraint. CPU is.

### Throughput falls with replica size

| Replica | Inherited pragmas | Replay pragmas |
| --- | --- | --- |
| 9 MB | 19.1 MB/s | 18.1 MB/s |
| 254 MB | 8.7 MB/s | 10.2 MB/s |
| 977 MB | 4.0 MB/s | 8.1 MB/s |

This is SQLite, not the benchmark. It reproduces with no Zero code in the path —
a bare table, a prepared `UPDATE … WHERE id = ?`, 30k random keys:

| Table | File | PK + secondary index | PK index only |
| --- | --- | --- | --- |
| 50k rows | 35 MB | 82,974 upd/s | 109,626 upd/s |
| 500k rows | 348 MB | 36,650 upd/s | 40,018 upd/s |
| 2M rows | 1394 MB | 10,043 upd/s | 13,296 upd/s |

The same 8× appears with no secondary index, so it is not index churn. Ruled
out: page-cache sizing (flat from 64 MB to 4 GB), disk (file is in OS cache
throughout), and within-commit write locality (sorting a batch by primary key
recovers ~6%). What is left is the B-tree — deeper descents and more distinct
4 KB pages dirtied per commit as rows spread over a larger file.

---

## 4. Levers that worked

### 4.1 Pragmas — 2.2×, the single biggest win

Every pragma the replication manager sets exists to protect a reader or a crash.
Replay has neither. At a 977 MB replica:

| Profile | Settings | MB/s | WAL left |
| --- | --- | --- | --- |
| `rep-manager` | WAL, `synchronous=NORMAL`, `wal_autocheckpoint=0` | 3.7 | 368 MB |
| `wal-ckpt` | WAL, `synchronous=OFF`, checkpointing on | 8.4 | 31 MB |
| `no-journal` | `journal_mode=OFF`, `synchronous=OFF` | 7.8 | none |
| `excl` | + `locking_mode=EXCLUSIVE` | **8.4** | none |
| `excl+cache` | + `cache_size=512MB` | 9.4 | none |

- **Nearly the whole win is not inheriting `wal_autocheckpoint = 0`**, which
  exists so litestream can own checkpointing. Everything after is ~10%.
- `journal_mode=OFF` is **not** faster than a checkpointing WAL — it is slightly
  slower until `locking_mode=EXCLUSIVE` brings it level. What it buys is the WAL
  file ceasing to exist: no second file, no checkpoint step, no disk headroom.
- `cache_size` does nothing at moderate batch sizes (64 MB → 4 GB measures
  flat); one pass over a log touches each page about once, leaving no reuse for
  a cache to capture. It matters *only* as the counterpart to very large
  transactions.

### 4.2 Coalescing — real, but saturates fast

Logical transactions per SQLite commit, 977 MB replica, mixed:

| Txns/commit | Commits | Inherited | WAL left | Replay pragmas |
| --- | --- | --- | --- | --- |
| 1 | 3,831 | 3.4 MB/s | 522 MB | 7.2 MB/s |
| 16 | 240 | 4.1 | 445 MB | **8.2** |
| 256 | 15 | 6.2 | 368 MB | **8.3** |
| 1,024 | 4 | — | — | 8.1 |
| whole chunk | 1 | 7.6 | 170 MB | 7.9 |

- Under the old pragmas coalescing looks like a 2.2× lever, but that is page
  rewrites into a never-checkpointing WAL going away — it is standing in for
  the pragma fix. Once the pragmas are right it is worth ~15%.
- **Whole-chunk-in-one-transaction regresses** at the default page cache
  (update-heavy: 7.5 MB/s at 256, 5.9 at whole chunk): the dirty set outgrows
  the cache and SQLite spills mid-transaction, rewriting pages touched again.
  With a 4 GB cache the same case is the fastest measured (8.2 MB/s).
- **Recommendation: batch 16–256 logical transactions.** Within ~5% of the best
  with no memory tuning and no all-or-nothing progress.
- Not free: every row in a group takes that group's final `_0_version`, so a
  ViewSyncer restoring the replica later re-sends rows it did not need to.
  Versions only move forward so nothing is lost, but the window is the batch
  size — another reason to keep it modest.

### 4.3 A custom applier — 1.56×

| Variant | 977 MB replica | 12 MB replica |
| --- | --- | --- |
| baseline (`ChangeProcessor` + `BigIntJSON.parse`) | 9.2 MB/s (1.00×) | 16.8 MB/s (1.00×) |
| prototype applier + native `JSON.parse` | **14.3 MB/s (1.56×)** | 37.4 MB/s (2.23×) |
| prototype applier + binary format | 12.5 MB/s (1.36×) | 45.4 MB/s (2.71×) |

The prototype caches a prepared statement per (op, table, column-set) and binds
positionally, instead of rebuilding the SQL text from `Object.keys(row)` per row
and re-normalizing its whitespace on every cache lookup. It handles only
insert/update/delete/begin/commit — no schema changes, backfill, truncate, JSON
columns or oversized integers. Those omissions are what make it a clean upper
bound, and why the real thing lands somewhat below 1.56×.

**A binary log format lost.** It is *slower* than V8's native `JSON.parse` at
realistic replica size, and its storage win is 1.12× after gzip rather than the
1.49× raw — a compact encoding and a general-purpose compressor chase the same
redundancy, and the encoding gets there first (10.8× compression vs 14.4×). It
only leads on a 12 MB replica, the regime that matters least.

---

## 5. Where the time goes

CPU profile of the apply loop alone (generation and base restore excluded),
977 MB replica, ~68 µs/change:

| Share | Where |
| --- | --- |
| ~41% | Native SQLite — B-tree descent, page updates, index maintenance, commit |
| ~27% | `BigIntJSON.parse` |
| ~20% | Per-change JS on the way to SQLite |
| rest | GC, profiler overhead |

Only the SQLite share grows with the replica; the other two are fixed per change,
so they dominate on a small replica and fade on a large one.

`BigIntJSON` hand-rolls a parser so `int8` values past 2^53 survive. Measured
over identical entries: native `JSON.parse` 196 MB/s, `BigIntJSON.parse`
67 MB/s (2.9× slower), + valita validation 52 MB/s.

**The ceiling is bounded.** SQLite is ~41% of the baseline's per-change cost at
this replica size, so even a free decode lands near 2.4×.

---

## 6. Levers that were measured and failed

Recorded so they are not re-tried blind. All at 977 MB, mixed, replay pragmas.

| Lever | Result |
| --- | --- |
| `page_size` 8K / 16K | **Worse**: 5.6 → 4.7 → 4.0 MB/s. Random single-row updates pay for the whole page they touch; a shallower B-tree does not make it back. |
| `journal_mode = wal2` | **Slower** than wal (6.5 vs 7.4). wal2 keeps readers from starving a checkpoint; replay has no readers. |
| `mmap_size = 2 GB` | ~15%, inside this machine's run-to-run noise. |
| `BEGIN CONCURRENT`, 2–4 writers | **Worse.** See below. |

### Concurrency, in detail

The build *does* support `BEGIN CONCURRENT` (SQLite 3.54.0, begin-concurrent
branch), so this was testable rather than impossible.

| Partitioning | 1 writer | 2 writers | 4 writers | Retries |
| --- | --- | --- | --- | --- |
| by PK hash | 13.6 MB/s | 8.7 | 8.6 | 76 → 356 |
| **by table** | 13.2 MB/s | **9.3** | — | **0** |

Partitioned by table, two writers share no B-tree at all and get **zero**
retries — and it is still 30% slower than one writer. That rules out conflicts.
What remains is shared regardless of what writers touch: the WAL append
serializes, each `COMMIT` validates its read-set against concurrent commits, and
each connection keeps its own page cache so the effective cache fragments.

**SQLite writes do not parallelize within one database file.** They would across
separate files — separate files mean separate write locks — so a per-table
sharded replica is the version of this idea that could work, at the cost of
every cross-table read.

> **Open:** this conclusion assumes the contention is SQLite's file-level
> machinery. It has not yet been separated from *process-global* contention —
> see [Open questions](#open-questions).

---

## 7. Open questions

1. **Compile flags.** `PRAGMA compile_options` shows `THREADSAFE=2` and
   `SYSTEM_MALLOC`, with `SQLITE_DEFAULT_MEMSTATUS` **not** disabled and
   `ENABLE_STMT_SCANSTATUS` **on**. Under `THREADSAFE=2` the core allocator
   mutex is still active, and with memstatus enabled every `malloc`/`free`
   takes a global mutex to update statistics. That is a plausible explanation
   for concurrency failing to scale, and it is separable: two writers against
   two *separate database files* should scale if the contention is per-file, and
   should not if it is process-global.
2. **Snapshot frequency.** The real knob. At 14 MB/s, a 60-second recovery
   budget means keeping the log under ~800 MB.
3. **Log compaction** — last write per key before replaying. On a 240 MB log:
   1.06× insert-heavy, 1.28× mixed, **1.46× update-heavy**, and the ratio grows
   with log size. Heavily workload-dependent; measure on a real log.
4. **Replicas past 1 GB**, where the trend says the answer gets materially worse.

---

## 8. Running it

```bash
pnpm --filter zero-cache run bench logical-log-apply
pnpm --filter zero-cache run bench logical-log-applier-headroom
```

Knobs (all optional) are documented in each file's header. The most useful:

```bash
LOGICAL_LOG_TARGET_MB=24        # log size per sample
LOGICAL_LOG_BASE_ROWS=2000000   # base replica size
LOGICAL_LOG_PROFILES=excl       # pragma profile
LOGICAL_LOG_COALESCE=1,16,256   # logical txns per SQLite commit
```

A 1 GB log run needs heap headroom:

```bash
NODE_OPTIONS=--max-old-space-size=8192 LOGICAL_LOG_TARGET_MB=1024 \
  pnpm --filter zero-cache run bench logical-log-apply
```
