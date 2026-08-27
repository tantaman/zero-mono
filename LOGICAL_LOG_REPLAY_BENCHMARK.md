# Logical Log Replay: Benchmark Results

**Question.** Can backup/restore for the replication manager be "base snapshot
in S3 + logical log segments in S3" instead of litestream? We already have a CDC
stream and a component that applies it to SQLite. The number that decides it is:
how fast can a stored logical log be replayed into a SQLite replica?

**Status.** Measured, including the absolute ceiling. Against a 1 GB replica the
current path does 9.1 MB/s; a prototype TypeScript applier does 14.3; a C
harness with a zero-parse log and permanently prepared statements does **30.9**
(1 GB of log in ~33 s). **We are 3.4× off the machine, not 100×** — and 2.1× of
that gap is the JS↔native boundary. See [§6](#6-the-absolute-ceiling).

---

## 1. What was built

| File | Purpose |
| --- | --- |
| `packages/zero-cache/src/services/replicator/logical-log-fixture.ts` | Deterministic CDC stream generator + table/index specs. Shared, so every benchmark replays byte-identical input. |
| `.../logical-log-apply.bench.ts` | What the **current** apply path costs. Sweeps workload × coalescing × pragma profile × replica size. |
| `.../logical-log-applier-headroom.bench.ts` | How much of that cost is **recoverable**. Compares the current path against a prototype applier and a binary log format. |
| `packages/zero-cache/bench/logical-log-ceiling/` | The **absolute ceiling**: a C harness with a zero-parse `mmap`'d log and permanently prepared statements. See [§6](#6-the-absolute-ceiling). |

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
| Replay throughput, prototype applier + binary format | 15.4 MB/s |
| Replay throughput, **C harness** (absolute ceiling) | **30.9 MB/s** |
| 1 GB of log, current path | 2:06 |
| 1 GB of log, prototype applier | **~1:12** |
| 1 GB of log, C harness | **~0:33** |
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

### 4.3 A custom applier — 1.57×

All non-baseline variants run the replay pragmas (`journal_mode=OFF`,
`synchronous=OFF`, `locking_mode=EXCLUSIVE`) and hold prepared statements.

| Variant | 977 MB replica | 12 MB replica |
| --- | --- | --- |
| baseline (`ChangeProcessor` + `BigIntJSON.parse`) | 9.1 MB/s (1.00×) | 17.2 MB/s (1.00×) |
| `FastApplier` + native `JSON.parse` | 13.8 (1.51×) | 38.5 (2.23×) |
| **`DirectApplier` + native `JSON.parse`** | **14.3 (1.57×)** | 40.3 (2.34×) |
| `DirectApplier` + binary format | **15.4 (1.69×)** | 51.4 (2.98×) |

`FastApplier` caches a prepared statement per (op, table, column-set), keyed by
a string it builds per row, and spreads a values array into `run()`.
`DirectApplier` removes both: it keeps resolved statements on a per-table shape
list, recognises the steady-state shape by comparing column names in place, and
binds from a caller-owned array reused across rows. In steady state a row costs
a length check, a few string compares, and the bind.

That last refinement is worth only **~4%** (13.8 → 14.3). Most of the applier
win is simply *having* cached statements and native `JSON.parse` — the residual
per-row string building was ~1.5 µs of a ~41 µs budget, matching the
micro-benchmark in §5.

Both appliers handle only insert/update/delete/begin/commit — no schema
changes, backfill, truncate, JSON columns or oversized integers. Those omissions
are what make this a clean upper bound, and why the real thing lands somewhat
below 1.57×.

#### Correction: the binary format does win on throughput

An earlier run of this benchmark reported the binary format as *slower* than
native `JSON.parse`. That comparison was noise — the pair flipped order between
runs at 3 samples. At 8 samples, with both formats driving the **same**
applier so only the encoding differs, binary is consistently ahead: **+8% at
977 MB, +28% at 12 MB.**

The storage conclusion is unchanged, and is still the reason not to reach for
it first: binary is 1.49× smaller raw but only **1.12×** smaller after gzip -1,
and it compresses worse (10.8× vs 14.4×) because a compact encoding and a
general-purpose compressor chase the same redundancy. So the case for a binary
format rests on ~8% of replay throughput at realistic replica sizes, against
the cost of owning a schema-versioned wire format. The applier is the clear win;
the format is a judgement call.

---

## 5. Where the time goes

### Are we using prepared statements? Yes — but the path to them costs 3×

`ChangeProcessor` → `StatementRunner.run(sql, args)` → `StatementCache.use(sql)`,
which prepares once and reuses the `Statement` thereafter. The statement is
genuinely cached. What is *not* cached is everything in front of it: the SQL text
is rebuilt from `Object.keys(row)` for every row, and the cache then
re-normalizes that text with a `replaceAll(/\s+/g, ' ')` and churns four Map
operations to check it out and back in.

Measured against a small table, so SQLite's own work is near-constant and what
varies is the JS in front of it:

| Path | µs/row |
| --- | --- |
| `StatementRunner`, SQL rebuilt per row (what runs today) | **9.05** |
| `StatementRunner`, SQL string hoisted | 4.99 |
| Prepared `Statement` held directly | **2.97** |

Rebuilding the SQL costs 4.1 µs/row; the cache lookup costs another 2.0. We
spend **6.1 µs getting to a statement that takes 3.0 µs to run.** That is the
~20% JS share below, and it is what the prototype applier removes by keying a
held `Statement` on (op, table, column-set).



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

## 6. The absolute ceiling

`packages/zero-cache/bench/logical-log-ceiling/` is a C harness built to bound
the optimization work. Everything removable is removed: the log is `mmap`'d and
walked by pointer arithmetic over fixed-stride records (no parsing), strings are
bound `SQLITE_STATIC` straight out of the mapping (no copying), nothing is
allocated per change, and every statement is prepared once and only reset and
rebound. It compiles against the same amalgamation and the same defines as
`@rocicorp/zero-sqlite3`, so it is not measuring a different SQLite.

Both harnesses print the same checksum over the resulting replica and **they
match exactly**, so the two paths provably did the same work.

Applying a 24 MB log (42,890 changes, mixed, 256 txns/commit) with
`journal_mode=OFF`, `synchronous=OFF`, `locking_mode=EXCLUSIVE`:

| Replica | C harness | Best TypeScript applier | Current path |
| --- | --- | --- | --- |
| 12 MB | **6.2 µs/change** (90 MB/s) | 13.3 µs (43 MB/s) | 32.4 µs (17.2 MB/s) |
| 977 MB | **18.1 µs/change** (31 MB/s) | 38.0 µs (14.7 MB/s) | 61.4 µs (9.1 MB/s) |

Three things fall out of that:

- **The gap from today is 3.4×, not 100×.** 1 GB of log lands in ~33 s rather
  than ~2:06. The approach is not fighting an order of magnitude.
- **~2.1× of the gap is the JS↔native boundary**, and it is remarkably stable —
  2.15× at 12 MB, 2.10× at 977 MB. No amount of TypeScript tuning reaches it;
  the prototype applier already captured what is reachable from that side.
- **SQLite's size-dependent work is ~12 µs/change** (18.1 at 977 MB minus the
  6.2 µs fixed cost at 12 MB). That is the irreducible part, and it is why
  replica size dominates every other lever.

### Correction: the profile over-attributed cost to SQLite

§5 reports a CPU profile splitting the baseline ~41% "native SQLite". The C
harness does the *entire* job in 18.1 µs where that profile implied SQLite alone
was ~25 µs of the 61.4. The profile's `run` frame was charging
better-sqlite3's napi marshalling — converting and copying every bound value
across the JS/C++ boundary — to SQLite. Real SQLite work is ≤18.1 µs; the rest
of that frame is boundary cost, which is exactly what the C harness removes.

---

## 7. Why it slows down: key scheme and table structure

Two candidate explanations for the size-dependence, both about how rows are
*keyed* rather than how much data there is. `bench/logical-log-ceiling/keys.c`
tests them crossed, in C, with the same row payload throughout.

**Inserts** (rows/s, into a table growing to N):

| N | random (uuid4) | time-ordered (v7) | seqnum |
| --- | --- | --- | --- |
| 250k | 197,637 | **475,610** | 446,676 |
| 1M | 101,801 | **420,887** | 326,934 |
| 2M | 84,095 | **310,416** | 290,739 |
| 4M | 77,637 | **240,507** | 234,634 |

**Random point updates** (rows/s, 50k by key):

| N | random (uuid4) | time-ordered (v7) | seqnum |
| --- | --- | --- | --- |
| 250k | 122,052 | 121,027 | 120,019 |
| 1M | 104,578 | 100,957 | 104,274 |
| 2M | 93,276 | 89,140 | 92,904 |
| 4M | 86,359 | 76,964 | 75,086 |

### Is it the missing primary key? No — a real one is worse

The replica ends up as a plain rowid table with the key in a separate UNIQUE
index, so a lookup by id is two B-tree descents. Making `id` a real
`PRIMARY KEY` in a `WITHOUT ROWID` table collapses that to one — and measured
**slower on both axes**, at 2M rows:

| Structure | insert rows/s | update rows/s |
| --- | --- | --- |
| rowid + unique index (today) | **84,095** | **93,276** |
| `WITHOUT ROWID`, `PRIMARY KEY(id)` | 41,797 | 69,827 |

`WITHOUT ROWID` stores the whole row inside the key-ordered B-tree. Our rows are
~600 bytes, so interior pages hold far fewer keys, the tree is deeper, and every
split moves more bytes. SQLite's own guidance is that `WITHOUT ROWID` suits
small rows; these are not small rows. **Leave the schema as it is.**

### Is it random keys? Yes — but only for inserts

Time-ordered keys are **2.4×–4.1× faster to insert** at every table size, and
the advantage does not fade: 310k vs 84k rows/s at 2M. Random keys scatter each
insert to an unpredictable index page, dirtying a fresh page nearly every time;
an ordered key appends at the right edge, where the page is already hot.

For **updates the key scheme makes no difference at all** — every scheme lands
within noise of the others at every size. A random point update is a random
descent no matter how the tree was built.

### Neither flattens the curve

Random inserts degrade 2.5× from 250k to 4M rows; ordered inserts degrade 2.0×;
updates degrade ~1.4×. Ordered keys shift the whole curve *up*, they do not make
it flat. The size-dependence in §3 is real B-tree behaviour and survives both
changes.

> **Scope.** The key scheme is the *application's* choice — zbugs and friends
> mint nanoid-style random ids — not something the replicator picks. So this is
> schema guidance for app authors, and it is worth the most where the workload
> is insert-dominated: initial sync, backfills, and append-heavy tables. It does
> nothing for an update-heavy replay.
>
> These are controlled comparisons of key scheme and table structure with a
> deliberately simplified row (fixed-length text, four updated columns), so the
> absolute rates run well above the replay numbers elsewhere in this document.
> Read the ratios, not the magnitudes.

---

## 8. Levers that were measured and failed

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

### Is it a global lock? No — measured

The obvious suspect was process-global contention — a global allocator mutex
serializing every `malloc`/`free`.

> **Correction.** An earlier version of this document claimed
> `SQLITE_DEFAULT_MEMSTATUS` was left enabled. It is not: `defines.gypi` sets
> `SQLITE_DEFAULT_MEMSTATUS=0`, and `PRAGMA compile_options` does report
> `DEFAULT_MEMSTATUS=0` at runtime. The earlier reading came from a truncated
> dump that cut off the alphabetically-first options. So the global-mutex
> concern never applied — which the experiment below independently confirms.

That is separable. Give each writer its **own database file** and nothing is
shared at the SQLite level; anything still limiting throughput has to be
process-global.

| Writers | One shared file (`BEGIN CONCURRENT`) | Separate files (`BEGIN IMMEDIATE`) |
| --- | --- | --- |
| 1 | 10.7 MB/s | 12.3 MB/s |
| 2 | 6.4 MB/s (0.60×) | **22.6 MB/s (1.84×)** |
| 4 | 8.6 MB/s | **30.6 MB/s (2.49×)** |

Separate files scale near-linearly to 2 writers and 2.49× on 4 (on 4 vCPUs, with
the main thread also live). A global allocator mutex would throttle those
equally. **It is not a global lock — it is per-database-file contention:** the
WAL append, the write lock, and `BEGIN CONCURRENT`'s commit-time snapshot
validation.

`BEGIN CONCURRENT` also costs ~6% even single-threaded (13.3 vs 12.5 MB/s), so
it is a pessimization unless it is actually buying parallelism.

---

## 9. Open questions

1. **`SQLITE_ENABLE_STMT_SCANSTATUS` is on**, adding per-VDBE-step accounting to
   every row. It is the one compile flag still worth questioning —
   `THREADSAFE=2` is right, `SQLITE_DEFAULT_MEMSTATUS=0` is already set, and
   `SQLITE_ENABLE_MEMORY_MANAGEMENT` is already off. Untested, since the C
   harness inherits the same defines by design; measuring it means building both
   ways.
2. **A native applier.** The ceiling says 2.1× sits on the far side of the
   JS↔native boundary. Whether that is worth a native component — and whether it
   could reuse the C harness's zero-parse format — is the real open design
   question.
3. **Snapshot frequency.** The real knob. At 14 MB/s, a 60-second recovery
   budget means keeping the log under ~800 MB.
4. **Log compaction** — last write per key before replaying. On a 240 MB log:
   1.06× insert-heavy, 1.28× mixed, **1.46× update-heavy**, and the ratio grows
   with log size. Heavily workload-dependent; measure on a real log.
5. **Replicas past 1 GB**, where the trend says the answer gets materially worse.

---

## 10. Running it

```bash
pnpm --filter zero-cache run bench logical-log-apply
pnpm --filter zero-cache run bench logical-log-applier-headroom
```

For the C ceiling harness, see its own
[README](packages/zero-cache/bench/logical-log-ceiling/README.md).

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
