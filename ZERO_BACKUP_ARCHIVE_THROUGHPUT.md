# Backup Archive Mode: End-to-End Replication Throughput

|                 |                                                                         |
| --------------- | ----------------------------------------------------------------------- |
| **Status**      | Measured                                                                |
| **Companions**  | Backup Archive Mode: Implementation Plan; Backup Archive Mode: Workplan |
| **Measured on** | September 1, 2026, branch `claude/zero-backup-archive-mode-1d9q13`      |
| **Harness**     | `apps/zero-throughput`, `pnpm --filter zero-throughput run ceiling`     |

## What was measured

End-to-end throughput from Postgres to a **view-syncer running no pipelines**:

```
Postgres WAL -> replication-manager -> WebSocket -> view-syncer replicator
-> SQLite replica
```

No client connects, so the view-syncer holds no query, runs no IVM pipeline
and sends no poke. The only thing that moves is the replica file, which is
what makes this a measurement of the replication path rather than of the
serving path. The measurement seam is the replica itself: a second,
read-only connection samples `max(seq)` and the newest applied row's
`clock_timestamp()` twice a second, giving both the applied rate and the
end-to-end latency of the newest row the view-syncer holds.

## Headline: the gateway is faster, and the reason is subscriber count

The replication-manager's shape, not its per-change CPU, is what moves this
number. (These are post-batching numbers; the write-worker batching described
under "Batching the write-worker hop" lifted every shape here, and the
sections between are labelled where they predate it.) Two shapes under an identical 8000 rows/s offered load (20 rows per
transaction, 256B incompressible payloads, one serving view-syncer whose
replica is what gets measured):

| replication-manager shape                             | live subscribers | vs-0 applied    | vs-0 replicator | lag p50 |
| ----------------------------------------------------- | ---------------- | --------------- | --------------- | ------- |
| **archive gateway**, applier off the live stream      | 1                | **7813 rows/s** | 0.79 cores      | 1.5s    |
| litestream-shaped: serving VS **+ backup-replicator** | 2                | 6593 rows/s     | 0.63 cores      | 4.1s    |

Before the write-worker batching described below, the same three shapes ran
at 5290-5349, 3247-3446, and (the control: an _archive_ gateway with a
second live subscriber) 3362 rows/s. Batching lifted every shape and
narrowed the gap between them, because the per-hop cost the second
subscriber used to double is now amortized across a batch.

And at loads each shape can actually hold:

| shape                                      | target | vs-0 applied    | lag p50 | lag p95 | verdict   |
| ------------------------------------------ | ------ | --------------- | ------- | ------- | --------- |
| archive gateway, applier off-stream        | 7000/s | **6997 rows/s** | 93ms    | 156ms   | SUSTAINED |
| litestream-shaped (VS + backup-replicator) | 6000/s | 5968 rows/s     | 156ms   | 538ms   | SUSTAINED |

**+18% at the plateau and +17% on the sustainable rate.** Before batching
the same comparison was +55%, and the control that explained it was an
_archive_ gateway with a second live subscriber, which landed on the
litestream number (3362 vs 3247): the variable was never the backup world,
it was **how many subscribers gate the live stream.** That is still the
mechanism; batching just made each gating event carry ~20 changes instead of
one, so the same structural advantage is worth proportionally less.

### Why: flow control waits for a majority of subscribers

The change-streamer pulls upstream no faster than its subscribers process
(`Broadcast` in `change-streamer/broadcast.ts`), waiting for
`majority = floor(n/2) + 1` of them to ACK each batch before continuing, and
only then arming a proportional timeout for the stragglers.

- In the **litestream world** the backup-replicator -- the applier whose
  file litestream ships -- is a subscriber like any other: same
  `ChangeStreamerHttpClient`, same `ReplicatorService`, counted in the same
  `Forwarder`. With one view-syncer that is n=2, majority=2: **every batch
  waits for both**, so the serving view-syncer advances in lockstep with the
  backup applier. The CPU numbers show it: both replicators sit at ~0.65
  cores on a box that is only 2.5/4 busy, i.e. each waits more than it
  works.
- In the **archive world** the applier is the base producer, and it consumes
  **sealed segments from the object store**, not the live subscription
  (`ArchiveChangeSource`). It cannot gate the stream, however slow it is.
  The serving view-syncer is then the only subscriber, gets its own core
  (0.89-0.99), and runs at its own speed.

That is the throughput argument for the gateway, and it is structural rather
than incidental: the archive world removes an applier from the critical path
instead of making it cheaper.

**It is not hardware contention.** At the same 8000 rows/s offered load, with
the box instrumented for disk as well as CPU:

| shape                     | vs-0 applied | replicator CPU | disk written           | device util | box CPU used |
| ------------------------- | ------------ | -------------- | ---------------------- | ----------- | ------------ |
| archive gateway, 1 sub    | 5349 rows/s  | 0.89 cores     | 30.5 MiB/s (1 replica) | 11%         | ~2.2 of 4    |
| litestream-shaped, 2 subs | 3446 rows/s  | 0.68 cores     | 19.1 + 19.1 MiB/s      | 10%         | ~2.5 of 4    |

The two-subscriber case pushes _more_ total bytes to disk and still advances
the serving replica 36% slower, with ~1.5 idle cores and a disk at one tenth
of its ticks. The replicators are not short of anything; each is waiting on
the other's ACK, which is what dropping from 0.89 cores to 0.68 apiece looks
like.

**Nuance for larger fleets.** The penalty is worst at n=2 -- one view-syncer
plus the backup-replicator, which is a very common small deployment. With
four view-syncers plus a backup-replicator (n=5, majority=3) the backup
applier is a tolerated minority and can straggle within the consensus
timeout, so the litestream world's disadvantage shrinks as the fleet grows.
It never reverses: a subscriber that is not there cannot be waited for.

## The environment, and what it means for the numbers

Everything ran on **one 4-vCPU box** (Intel Xeon @ 2.10GHz, 15 GiB RAM):
Postgres 16.13 (`wal_level=logical`, `synchronous_commit=off`,
`shared_buffers=2GB`), the replication-manager, the base producer, the
view-syncer(s), the `file://` archive and the load generator all share those
four cores and one disk. Upstream, CVR and change DB are the same Postgres
instance. The SQLite change log is `off` (its default), so the manager
writes the PG change log and, in `archive`, the archive.

Absolute rates on a box like this are not fleet numbers. What transfers is
**CPU per applied row per worker**, the **shape** of the ceiling, and the
subscriber-count effect above. Two substitutions were necessary and are
called out wherever they matter:

- **The backup-replicator stand-in.** There is no litestream binary in this
  sandbox (it must be built from the `rocicorp/litestream` fork), so the
  litestream world's RM is reproduced as a second view-syncer: the same
  replicator code, the same subscription client, applying the same stream to
  its own replica. It differs only in replica pragmas
  (`backup` mode sets `wal_autocheckpoint=0`) and in _not_ having litestream
  shipping pages off the same file -- I/O the real thing also pays. The
  litestream-shaped numbers above are therefore a **ceiling** for that world,
  not a handicap.
- **The producer's node.** In the intended deployment the base producer is
  its own StatefulSet. Here it would share the four cores, so the runs that
  isolate the gateway `SIGSTOP` it for the measurement window (verified: its
  CPU slope is 0.00). Runs where it is co-located are labelled as such.

## The rate ladder

**(Pre-batching numbers; see "Batching the write-worker hop" for what these
became.)** 40s of writes per point, 20 rows per transaction, one view-syncer. "Held"
means the backlog was flat and the replica caught up after the writers
stopped. The third mode here is a **no-backup control** -- backup mode
`litestream` with no backup URL, so the manager runs _neither_ an archive
writer _nor_ a backup-replicator. It is not a world anyone deploys; it is
the floor that isolates what each backup mechanism adds.

| RM shape                          | target rows/s | apply rows/s | lag p50 | lag p95 | WAL retained max | held |
| --------------------------------- | ------------- | ------------ | ------- | ------- | ---------------- | ---- |
| archive, producer co-located      | 2000          | 1999         | 183ms   | 247ms   | 4 MiB            | yes  |
| archive, producer co-located      | 4000          | 3997         | 187ms   | 231ms   | 7 MiB            | yes  |
| archive, producer co-located      | 6000          | 4265         | 7.1s    | 11.0s   | 83 MiB           | no   |
| archive, producer co-located      | 8000          | 4176         | 12.0s   | 18.6s   | 162 MiB          | no   |
| archive, producer off-stream      | 5000          | 5043         | 164ms   | 417ms   | 10 MiB           | yes  |
| archive, producer off-stream      | 6000          | 5600         | 2.8s    | 3.8s    | 36 MiB           | no   |
| archive, producer off-stream      | 8000          | 5290         | 8.7s    | 13.4s   | 133 MiB          | no   |
| no-backup control                 | 4000          | 4000         | 160ms   | 201ms   | 1 MiB            | yes  |
| no-backup control                 | 6000          | 5603         | 1.9s    | 3.0s    | 26 MiB           | no   |
| no-backup control                 | 8000          | 5476         | 8.6s    | 12.9s   | 126 MiB          | no   |
| litestream-shaped (2 subscribers) | 5000          | 3515         | 8.6s    | 12.5s   | 78 MiB           | no   |
| litestream-shaped (2 subscribers) | 8000          | 3247         | 15.3s   | 23.3s   | 187 MiB          | no   |

Two readings:

- **Against the no-backup control, the archive writer itself is free on the
  serving path**: 5600 rows/s with the archive running vs 5603 without, at
  the same offered load. The gateway does pay for it in CPU (below); it does
  not pay for it in throughput, because the gateway is not the limiter at
  this shape.
- **Against the litestream shape, the archive world wins by the whole cost
  of a second subscriber.** The no-backup control sits between them, exactly
  where it should.

## What the ceiling is made of

**(Pre-batching.)** Steady-state CPU, in ms of CPU per applied row
(equivalently, cores per 1000 rows/s), one view-syncer:

| RM shape          | rm/change-streamer | rm/base-producer | vs-0/replicator | postgres |
| ----------------- | ------------------ | ---------------- | --------------- | -------- |
| archive           | 0.14               | 0.15             | 0.18            | 0.07     |
| no-backup control | 0.11               | -                | 0.16            | 0.06     |

**When it is the only subscriber, the view-syncer's replicator is the
ceiling, and it is a single-core ceiling**: 0.86 cores while sustaining 5000
rows/s, 0.89-0.99 while plateauing. One view-syncer applies its stream in
one replicator process; more cores do not help it.

**The per-change cost splits into a per-row term and a per-transaction
term,** and which worker runs out first depends on which dominates. Three
transaction shapes, each driven ~1.5x past its ceiling (archive with the
producer co-located, and the no-backup control, for the same box):

| rows/tx | archive rows/s | control rows/s | apply tx/s | ms/tx | busiest worker              |
| ------- | -------------- | -------------- | ---------- | ----- | --------------------------- |
| 1       | 1271           | 1231           | 1271       | 0.79  | rm/change-streamer (0.72)   |
| 20      | 4265           | 5603           | 213        | 4.69  | vs-0/replicator (0.76-0.90) |
| 500     | 5645           | 7347           | 11.3       | 88.6  | vs-0/replicator (0.82-0.98) |

Fitting `ms/tx = a + b*rows` gives **b ~= 0.18-0.21 ms per row and a ~= 0.6-1.2
ms per transaction**. The per-row term dominates past about 20 rows per
transaction, which is why widening transactions further stops helping.

Note the first row: at one row per transaction the two are **identical**
(1271 vs 1231) and the busiest worker is the manager, not the view-syncer.
Transaction-bound load is bound by the manager's per-transaction work, and
the archive adds nothing measurable to it.

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
only one is in flight (`assert(this.#pending === null)`), so a 20-row
transaction costs 22 round trips through structured clone. Only ~46% of one
of those two threads is SQLite; the rest is the handoff and the stream
plumbing. Because the two threads alternate, their per-row costs **add** to
wall-clock time rather than overlapping -- which is why the process tops out
at 0.89 cores with neither thread above ~0.45.

### The per-row budget

**(The pre-batching analysis that motivated the change below.)** 5290-5349
rows/s to one view-syncer is **187-189us per row**. Where it goes,
from the profile shares above converted to us/row and cross-checked against
direct micro-benchmarks (`pnpm --filter zero-throughput run pipeline-cost`):

| per row                                            | us    | share | measured how                                              |
| -------------------------------------------------- | ----- | ----- | --------------------------------------------------------- |
| write-worker postMessage round trip                | 45-49 | ~25%  | direct probe; profile agrees (26 main + 21 worker)        |
| SQLite statement execution                         | ~38   | ~20%  | 45.7% of the write-worker thread                          |
| WS frame in + `consumed()` ack out, **per change** | 13-21 | ~9%   | direct probe (21.6us both ends); profile `consumed` 24.8% |
| `BigIntJSON.parse` + valita validation             | ~14   | ~7%   | profile, 11.8% + 4.8% of the main thread                  |
| change-processor bookkeeping, subscription, GC     | ~50   | ~27%  | remainder                                                 |

And the floor underneath all of it: prepared inserts of the same rows into a
table shaped like a replicated one, with `getPragmaConfig('serving')`
pragmas, run at **10.5us per row -- 95,000 rows/s**. The applier's own SQLite
work is 38us/row, 3.5x that, because it runs more statements per row than a
bare insert (upsert plus the replica's change log), which is legitimate. But
the pipeline as a whole delivers **1/18th of what the storage can absorb**,
and only a fifth of its budget is spent in SQLite.

**Flow control is not what is costing here.** With one subscriber the
change-streamer's majority is one, and it only awaits subscriber progress
every `getDefaultHighWaterMark(false)` = 64 KiB of change JSON -- about every
190 changes, ~28 times a second at this rate. Everything in between is
pipelined. Flow control was the story in the two-subscriber comparison, where
the majority is two; at one subscriber the path simply runs at the
consumer's speed, and the consumer's speed is the table above.

**The unit of work is one change, everywhere.** One WebSocket frame down and
one ack frame back per change (`streams.ts`), one structured-clone round trip
to the write worker per change, one parse and one schema validation per
change. A 20-row transaction pays all of that 22 times. That is what a 189us
row was made of, and it was the same in either backup world.

### Batching the write-worker hop

The largest of those terms is now gone. `WriteWorkerClient.processMessages`
takes a batch, and `IncrementalSyncer` accumulates changes into one, flushing
when it hits **64 KiB of change JSON or 500 messages, whichever comes first**
and -- the common case below saturation -- as soon as nothing else has
arrived, so batching never buys throughput with latency it does not need.

The caps are the point: a batch is resident memory that `postMessage`
transiently doubles, so **a transaction is never held in it**. A transaction
larger than the cap is flushed and applied in pieces, mid-transaction, which
is safe because the boundary messages that make it atomic are just more
messages in the stream -- SQLite's transaction spans the pieces, and the
change processor's existing failure latch drops the rest of a batch after an
error. The byte cap is deliberately the same 64 KiB the change-streamer
pipelines before waiting on subscriber flow control, so neither side runs far
ahead of the other.

What it bought, all with the producer off the live stream:

| shape                      | before             | after                  | change         |
| -------------------------- | ------------------ | ---------------------- | -------------- |
| 20 rows/tx, highest held   | 5000/s @ p50 164ms | **7000/s @ p50 93ms**  | +40%, lag -43% |
| 20 rows/tx, plateau        | 5290-5349/s        | **7813/s**             | +47%           |
| 1 row/tx, highest held     | ~1250/s            | **3000/s @ p50 198ms** | +2.4x          |
| 1 row/tx, plateau          | 1271/s             | **3201/s**             | +2.5x          |
| 500 rows/tx, plateau       | 7717/s             | **11760/s**            | +52%           |
| litestream-shaped, plateau | 3446/s             | **6593/s**             | +91%           |

The transaction-bound shape gains most because batching crosses transaction
boundaries: at one row per transaction the old path paid three hops per row,
and the new one pays a fraction of one. And the ceiling has **moved to the
replication-manager**: in every 20-rows/tx run above, the gateway's
change-streamer now sits at 1.00-1.16 cores -- saturated -- while the
view-syncer's replicator has dropped to 0.67-0.79. The next lever is on the
manager's side of the wire, where the unit of work is still one change.

## What the archive buys and costs

**Buys:**

- **An applier off the live stream.** Worth +17% at a sustainable load and
  +18% at the plateau in a one-view-syncer deployment, for the reason in the
  headline -- and it was +43%/+63% before the write-worker batching, which is
  the honest way to read it: the advantage is a per-gating-event cost, so it
  shrinks as each event carries more work, and as the fleet grows. It never
  reverses.
- **A manager with no replica**: no replica volume, no restore at startup,
  no litestream process, no backup-replicator worker.

**Costs:**

- **A node for the producer.** 0.15 ms/row -- as much as the view-syncer's
  own replicator -- and it inherits the same per-core ceiling measured here,
  so it must be sized like a view-syncer, not like a sidecar.
- **ACK latency, and the WAL it retains.** 7-10 MiB retained at 4000-5000
  rows/s against 1 MiB for the control, because ACKs gate on the durable
  archive cursor. That is the number to watch on a flipped stack: it is what
  turns an S3 stall into WAL pressure. It scales with the seal interval,
  turned down to 1s here from the 30s default.
- **~8% of the gateway's hot loop** for `segment-spool.append`, and +27% on
  the change-streamer's per-row CPU (0.11 -> 0.14 ms). Not visible in
  throughput at these shapes because the gateway is not the limiter; it
  would be at transaction-bound loads if the archive were on that path,
  which it is not (1 row/tx: 1271 vs 1231).
- **Bytes**: 0.8 MiB/s of compressed segments per 1.34 MiB/s of committed
  rows (~1.6x on incompressible payloads with JSON envelopes), ~38 sealed
  segments per 40s at a 1s seal interval.

## Reproducing

```bash
# Postgres 16 with wal_level=logical on :6436, then:
pnpm --filter zero-throughput run ceiling -- \
  --backup-mode archive --topology distributed --users 0 \
  --pg-url postgresql://user:password@127.0.0.1:6436/postgres \
  --write-rate 5000 --rows-per-statement 20 --statements-per-tx 1 \
  --write-concurrency 4 --duration-ms 40000 --warmup-ms 10000

# The litestream-shaped RM: a second view-syncer stands in for the
# backup-replicator, so two subscribers gate the stream.
pnpm --filter zero-throughput run ceiling -- \
  --backup-mode litestream --num-view-syncers 2 --write-rate 5000 ...

pnpm --filter zero-throughput run ceiling:report -- results/ceiling
```

`--profile-rm --profile-vs` adds V8 CPU profiles for the two process trees.
See `apps/zero-throughput/README.md` for the full option list.

## What would raise the ceiling further

In the order the profiles rank them:

1. ~~**Batch the replicator's write-worker calls.**~~ Done, above. The probe
   predicted ~6.8k rows/s from 5.3k; the real change delivered 7.8k, and 2.5x
   on the transaction-bound shape. The ceiling moved to the manager.
2. **The manager's per-transaction cost** (~0.6-1.2 ms) bounds
   transaction-bound workloads at ~1250 tx/s, with the change-streamer the
   busiest worker at 0.72 cores -- bound by serialized round trips, not
   compute.
3. **`timestampToFpMillis` at 10% of the gateway loop** is a lot for
   timestamp conversion (it runs through `@google-cloud/precise-date`).
4. **Per-change valita validation on the view-syncer** (4.8%) is paid on a
   stream the gateway already validated.
5. **The manager is now the limiter**, saturating one core at ~7.8k rows/s
   (~128us/row), and its profile is the same story a level up: framing,
   stringifying, ack-parsing and timestamp-converting one change at a time.
   The wire unit wants to become a transaction the way the thread hop just
   did -- one frame carrying a batch, one ack for it.
6. **The segment append (7.7% of the gateway loop)** is the only
   archive-specific item on this list, and it is last for a reason.

## Caveats

- One box, four cores, one disk, Postgres sharing them, and upstream/CVR/
  change DB all the same instance. Rates are not fleet numbers; the per-row
  CPU costs, the profile shares and the subscriber-count effect are.
- The litestream world is reproduced with a stand-in and without litestream
  itself, which makes those numbers optimistic for that world. See "The
  environment".
- `SIGSTOP` on the producer stands in for "the producer is on another node",
  not for "there is no producer": the archive still accumulates unread
  segments and a frozen producer publishes no bases. It isolates
  _contention_, which is what it is for.
- The `file://` object store is local disk. Real S3 raises segment upload
  latency, which moves ACK latency and retained WAL rather than the apply
  rate -- fail-stall keeps uploads off the forwarding path until the queue
  saturates.
- The runs surfaced a small defect, fixed in this branch: in archive mode
  the gateway's `replica.backup_lag` gauge tried to read a replica file a
  gateway does not have, once per metric collection. The numbers were taken
  with it present, at ~0.4% of one worker.
