# Backup Archive Mode: End-to-End Replication Throughput

|                 |                                                                         |
| --------------- | ----------------------------------------------------------------------- |
| **Status**      | Measured                                                                |
| **Companions**  | Backup Archive Mode: Implementation Plan; Backup Archive Mode: Workplan |
| **Measured on** | September 1, 2026, branch `claude/zero-backup-archive-mode-1d9q13`      |
| **Harness**     | `apps/zero-throughput`, `pnpm --filter zero-throughput run ceiling`     |

## What was measured

End-to-end throughput from Postgres to a **view-syncer running no pipelines**,
in backup mode `archive`:

```
Postgres WAL -> replication-manager (gateway: change source, PG change log,
archive writer) -> WebSocket -> view-syncer replicator -> SQLite replica
```

No client connects, so the view-syncer holds no query, runs no IVM pipeline
and sends no poke. The only thing that moves is the replica file, which is
what makes this a measurement of the replication path rather than of the
serving path. The measurement seam is the replica itself: a second,
read-only connection samples `max(seq)` and the newest applied row's
`clock_timestamp()`, twice a second, which gives both the applied rate and
the end-to-end latency of the newest row the view-syncer holds.

Both worlds were run under identical load: `archive` (gateway with no
replica, base producer in its process tree, view-syncer restored from the
archive) and `litestream` (today's default, here without a backup URL, so
the manager keeps the replica it synced and the view-syncer starts from a
copy of it).

## The environment, and what it means for the numbers

Everything ran on **one 4-vCPU box** (Intel Xeon @ 2.10GHz, 15 GiB RAM):
Postgres 16.13 (`wal_level=logical`, `synchronous_commit=off`,
`shared_buffers=2GB`), the replication-manager, the base producer, the
view-syncer, the `file://` archive and the load generator all share those
four cores and one disk. Upstream, CVR and change DB are the same Postgres
instance. The SQLite change log is `off` (its default), so the gateway
writes the PG change log and the archive, and nothing else.

Absolute rates on a box like this are not fleet numbers. What transfers is
**CPU per applied row per worker** and the **shape** of the ceiling: which
worker runs out first, what the archive adds, and how the path responds to
transaction size. Those are reported alongside every rate below.

## Headline: the rate ladder

40s of writes at each target rate, 20 rows per transaction, 256B
incompressible payload, 4 writer connections. "Held" means the backlog was
flat and the replica caught up after the writers stopped.

| mode       | target rows/s | commit rows/s | apply rows/s | lag p50 | lag p95 | backlog slope | WAL retained max | held |
| ---------- | ------------- | ------------- | ------------ | ------- | ------- | ------------- | ---------------- | ---- |
| archive    | 2000          | 2000          | 1999         | 183ms   | 247ms   | 0/s           | 4 MiB            | yes  |
| archive    | 4000          | 4000          | 3997         | 187ms   | 231ms   | 3/s           | 7 MiB            | yes  |
| archive    | 6000          | 6000          | 4265         | 7.1s    | 11.0s   | 1735/s        | 83 MiB           | no   |
| archive    | 8000          | 8001          | 4176         | 12.0s   | 18.6s   | 3825/s        | 162 MiB          | no   |
| litestream | 2000          | 1999          | 2000         | 170ms   | 244ms   | -1/s          | 1 MiB            | yes  |
| litestream | 4000          | 4001          | 4000         | 160ms   | 201ms   | 1/s           | 1 MiB            | yes  |
| litestream | 6000          | 6001          | 5603         | 1.9s    | 3.0s    | 398/s         | 26 MiB           | no   |
| litestream | 8000          | 8000          | 5476         | 8.6s    | 12.9s   | 2524/s        | 126 MiB          | no   |

**On this box, at 20 rows per transaction, the path sustains 4000 rows/s in
`archive` and plateaus at ~4.3k when overdriven; the `litestream` baseline
plateaus at ~5.6k.** Most of that difference is _not_ the archive: it is the
base producer, which runs in the manager's process tree here and takes a
core that the intended deployment gives it on its own node. With the
producer taken out of the contention (see "Isolating the producer"), the
archive world plateaus at 5600 rows/s -- within noise of the litestream
baseline's 5603 -- and **sustains 5000 rows/s at a p50 end-to-end lag of
164ms and a p95 of 417ms**.

Below the ceiling the path is not merely keeping up, it is prompt: at 4000
rows/s co-located, p50 lag is 187ms and p95 231ms, with 7 MiB of WAL
retained by ACK gating on the durable archive cursor. Above the ceiling the
backlog grows linearly and lag with it; nothing breaks, and the replica
always caught up once the writers stopped (the drain runs 10-25% faster than
the loaded rate, which is the load generator and Postgres giving their cores
back).

### Isolating the producer

The same runs with the base producer `SIGSTOP`ed for the measurement window
-- the gateway still archives, the view-syncer still restores from and
follows the archive-world manager, but the second applier is not competing
for cores, as it would not be in the intended deployment:

| shape       | archive, producer co-located | archive, producer frozen | litestream  |
| ----------- | ---------------------------- | ------------------------ | ----------- |
| 20 rows/tx  | 4265 rows/s                  | 5600 rows/s              | 5603 rows/s |
| 500 rows/tx | 5645 rows/s                  | 7717 rows/s              | 7347 rows/s |

**The archive path costs the view-syncer nothing measurable.** What it costs
is a node to run the producer on, and ACK latency (below).

## What the ceiling is made of

Steady-state CPU, in ms of CPU per applied row (equivalently, cores per
1000 rows/s):

| mode       | rm/change-streamer | rm/base-producer | vs-0/replicator | postgres | harness |
| ---------- | ------------------ | ---------------- | --------------- | -------- | ------- |
| archive    | 0.14               | 0.15             | 0.18            | 0.07     | 0.03    |
| litestream | 0.11               | -                | 0.16            | 0.06     | 0.03    |

Two things stand out.

**Once the producer is out of the way, the view-syncer's replicator is the
ceiling -- and it is a single-core ceiling.** In the frozen-producer runs it
sits at 0.86 cores while sustaining 5000 rows/s and at 0.99 cores while
plateauing at 7717 rows/s: it saturates, and no amount of box helps, because
one view-syncer applies its stream in one replicator process. In the
co-located runs nothing saturates (at the 6000 target the change-streamer is
at 0.59 cores, the producer at 0.64, the replicator at 0.76, on a 4-core
box); there the three of them plus Postgres and the generator interleave,
and each waits more than it works. Both regimes are worth keeping in mind:
the first is the real ceiling, the second is what an under-provisioned node
looks like.

**The per-change cost splits into a per-row term and a per-transaction
term,** and which worker runs out first depends on which term dominates.
Three transaction shapes, each driven ~1.5x past its ceiling:

| rows/tx | archive rows/s | litestream rows/s | apply tx/s | ms/tx | busiest worker              |
| ------- | -------------- | ----------------- | ---------- | ----- | --------------------------- |
| 1       | 1271           | 1231              | 1271       | 0.79  | rm/change-streamer (0.72)   |
| 20      | 4265           | 5603              | 213        | 4.69  | vs-0/replicator (0.76-0.90) |
| 500     | 5645           | 7347              | 11.3       | 88.6  | vs-0/replicator (0.82-0.98) |

Fitting `ms/tx = a + b*rows` gives **b ~= 0.18-0.21 ms per row and a ~= 0.6-1.2
ms per transaction**. The per-row term dominates past about 20 rows per
transaction, which is why widening transactions further stops helping.

Note the first row: at one row per transaction the two backup worlds are
**identical** (1271 vs 1231), and the busiest worker is the gateway, not the
view-syncer. Transaction-bound load is bound by the manager's per-transaction
work -- and the archive adds nothing to it. Row-bound load is bound by the
view-syncer's replicator, which reaches 0.98-0.99 cores and is the one place
in these runs where a worker genuinely saturates.

## Where the time actually goes

CPU profiles (`--profile-rm --profile-vs`) at the saturation point, with
each sample's self time attributed to the nearest application frame:

**Gateway change-streamer** (the RM's hot loop):

| share | frame                                                           |
| ----- | --------------------------------------------------------------- |
| 13.2% | `types/streams.ts:251 streamOutInternal` (forwarding)           |
| 12.5% | socket `writeBuffer` (unattributed native)                      |
| 10.0% | `types/pg.ts:37 timestampToFpMillis`                            |
| 7.7%  | `shared/bigint-json.ts:73 stringify`                            |
| 7.7%  | `backup/archive/segment-spool.ts:143 append` (the archive)      |
| 2.0%  | `change-streamer/change-log-codec.ts:22 extractChangeSubstring` |

**View-syncer replicator, main thread:**

| share | frame                                                               |
| ----- | ------------------------------------------------------------------- |
| 31.3% | `replicator/write-worker-client.ts:201 #call` (postMessage + await) |
| 24.8% | `types/streams.ts:357 consumed` (per-message consumption)           |
| 11.8% | `shared/bigint-json.ts:53 parse`                                    |
| 4.8%  | `shared/valita.ts:215 test` (per-change schema validation)          |

**View-syncer write-worker thread:**

| share | frame                                                       |
| ----- | ----------------------------------------------------------- |
| 45.7% | `zqlite/db.ts:206 run` (the actual SQLite work)             |
| 24.9% | `replicator/write-worker.ts:137` (message handling + reply) |
| 5.3%  | `replicator/change-processor.ts:489 #upsert`                |

The view-syncer's two threads each sit at ~0.42 cores and ping-pong:
`processMessage` posts **one message per change-stream message** and asserts
that only one is in flight (`assert(this.#pending === null)`), so a 20-row
transaction costs 22 round trips through structured clone. Only about 46%
of one of those two threads is SQLite; the rest is the handoff and the
stream plumbing. That is the single biggest lever on this path, and it is
not archive-specific -- it is what the view-syncer costs in either world.

## What the archive actually costs

- **In throughput to a view-syncer: nothing measurable.** The gateway's
  per-row CPU does rise from 0.11 to 0.14 ms (+27%), of which
  `segment-spool.append` is 7.7% of that worker -- but at the row-bound
  shapes the gateway is not the limiter, and with the producer off the box
  the archive world reaches the same 5.6k rows/s as the baseline. At the
  transaction-bound shape, where the gateway _is_ the busiest worker, the two
  worlds are also equal (1271 vs 1231 rows/s). The spool append is real work
  in the gateway's serial loop; it is not, at these rates, the thing that
  runs out.
- **In bytes: 0.8 MiB/s of compressed segments for 1.34 MiB/s of committed
  rows** (~1.6x compression on incompressible payloads with JSON envelopes),
  and 37-38 sealed segments per 40s run at a 1s seal interval.
- **In ACK latency: 7 MiB of retained WAL at 4000 rows/s**, against 1 MiB in
  the litestream world -- the visible price of gating ACKs on the durable
  archive cursor rather than on the PG change log alone. It scales with the
  seal interval, which was turned down to 1s here; at the 30s production
  default expect proportionally more. This is the number to watch on a
  flipped stack, because it is what turns an S3 outage into WAL pressure.
- **A whole second applier: 0.15 ms/row, as much as the view-syncer's own
  replicator.** In the intended deployment the producer is its own
  StatefulSet, so this does not come out of the view-syncer's budget -- but
  it is a node, and it must keep up with the stream, so it inherits the same
  ~5-7k rows/s per-core ceiling measured here. The litestream world runs an
  applier of its own on the manager (the backup-replicator whose file
  litestream ships), which this baseline does **not** include, because there
  is no litestream binary in the sandbox. The like-for-like reading is
  therefore: `archive` gateway + producer ~= today's gateway +
  backup-replicator, plus the spool append in the gateway loop, minus
  whatever litestream's page shipping costs.

## Reproducing

```bash
# Postgres 16 with wal_level=logical on :6436, then:
pnpm --filter zero-throughput run ceiling -- \
  --backup-mode archive --topology distributed --users 0 \
  --pg-url postgresql://user:password@127.0.0.1:6436/postgres \
  --write-rate 4000 --rows-per-statement 20 --statements-per-tx 1 \
  --write-concurrency 4 --duration-ms 40000 --warmup-ms 10000

pnpm --filter zero-throughput run ceiling:report -- results/ceiling
```

`--profile-rm --profile-vs` adds V8 CPU profiles for the two process trees.
See `apps/zero-throughput/README.md` for the full option list.

## What would raise the ceiling

In the order the profiles rank them:

1. **Batch the replicator's write-worker calls.** One postMessage round trip
   per change, strictly serialized, is ~31% of the replicator thread and
   ~25% of the write-worker thread -- and the replicator is the worker that
   actually saturates on row-bound load. Passing a transaction's changes as
   one message would take most of that back. Not archive-specific: it is the
   ceiling of the view-syncer in either world.
2. **The gateway's per-transaction cost** (~0.6-1.2 ms) is what bounds
   transaction-bound workloads at ~1250 tx/s, with the change-streamer the
   busiest worker at 0.72 cores -- i.e. bound by serialized round trips, not
   by compute.
3. **`timestampToFpMillis` at 10% of the gateway loop** is a surprising
   amount for timestamp conversion (it runs through `@google-cloud/precise-date`).
4. **Per-change valita validation on the view-syncer** (4.8%) is paid on a
   stream the gateway already validated.
5. **The segment append (7.7% of the gateway loop)** is the only
   archive-specific item on this list, and it is last for a reason.

## Caveats

- One box, four cores, one disk, and Postgres sharing them. Rates are not
  fleet numbers; the per-row CPU costs and the profile shares are.
- The `file://` object store is local disk. A real S3 raises segment upload
  latency, which moves ACK latency and retained WAL, not the apply rate --
  the archive writer's fail-stall keeps uploads off the forwarding path
  until the queue saturates.
- The litestream baseline has no litestream process and no backup-replicator
  (no binary in the sandbox), so it is a _floor_ for that world's RM cost,
  not a like-for-like production comparison. See "What the archive actually
  costs".
- The runs surfaced a small defect, fixed in the same commit range: in
  archive mode the gateway's `replica.backup_lag` gauge tried to read a
  replica file that a gateway does not have, once per metric collection,
  logging a `SQLITE_ERROR` each time. The numbers above were taken with it
  present, at a cost of roughly 0.4% of one worker.
- The producer-frozen runs use `SIGSTOP`, which is a stand-in for "the
  producer is on another node", not for "there is no producer": the archive
  still accumulates unread segments, and a frozen producer publishes no
  bases. It is a clean isolation of _contention_, which is what it is for.
