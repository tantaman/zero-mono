# Replication Flow Control: What It Is, What Is Wrong With It, and the Shape It Should Have

|                |                                                                                                                      |
| -------------- | -------------------------------------------------------------------------------------------------------------------- |
| **Status**     | Proposal                                                                                                             |
| **Companions** | Backup Archive Mode: Implementation Plan; Backup Archive Mode: Workplan; Backup Archive Mode: Replication Throughput |
| **Branch**     | `claude/zero-backup-archive-mode-1d9q13`, as of the write-worker batching commit                                     |

## The question

The throughput document ends with the ceiling having moved to the
replication-manager, and names the next lever: the wire unit between the
manager and its subscribers is still one change -- one frame, one ack, one
`stringify`, one `timestampToFpMillis` -- and it wants the treatment the
write-worker hop just got.

That is not a separate piece of work from flow control. It _is_ flow control.
The per-frame ack is the flow-control signal; the 64 KiB checkpoint in the
change-streamer's loop is defined in terms of it; the majority/consensus
machinery in `Broadcast` counts it; the purge floors read the watermark it
carries. Batching the wire without first deciding what an ack means and who
is allowed to wait for it would bake the current shape into a new protocol
version. So: decide the shape first, then build the wire unit to it.

This document inventories every place the replication path waits today,
says what is structurally wrong, proposes the shape, and lists the decisions
that shape needs. It does not propose code beyond what is needed to make the
shape concrete.

## Inventory: every place the pipeline waits

From Postgres to a view-syncer's replica, with the code that implements each
wait and what it protects. "Unit" is what the bound is counted in.

| #   | hop                                        | mechanism                                                                                                                                                                                                                                             | unit                               | bound                                      | waits on                                      | protects                                    |
| --- | ------------------------------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------- | ------------------------------------------ | --------------------------------------------- | ------------------------------------------- |
| 1   | walsender -> change-source                 | `pipe({bufferMessages: 5})` in `logical-replication/stream.ts`, then Node stream back-pressure, then the TCP window                                                                                                                                   | pgoutput messages                  | 5 + socket buffers                         | the change-source loop dequeuing              | manager heap; WAL retained behind           |
| 2   | change-source -> change-streamer           | `await changes.push(change)` in `change-source.ts` (`ChangeStreamMultiplexer`)                                                                                                                                                                        | messages                           | 1 in flight                                | the change-streamer loop iterating            | nothing; it is just serial                  |
| 3   | change-streamer -> PG change log           | `Storer.readyForMore()`: heap-proportional threshold (`backPressureLimitHeapProportion`, 4% of free heap at startup), released at 80%                                                                                                                 | estimated heap bytes               | ~4% of free heap                           | batched `INSERT`s committing                  | manager heap                                |
| 4   | change-streamer -> SQLite change log       | synchronous `write()`, fail-soft                                                                                                                                                                                                                      | --                                 | --                                         | nothing                                       | --                                          |
| 5   | change-streamer -> archive                 | `ArchiveWriter.readyForMore()`: on-disk bytes (spool + sealed, 128 MiB default), released at half; fail-stall                                                                                                                                         | disk bytes                         | 128 MiB                                    | S3 uploads                                    | manager disk; WAL retained behind           |
| 6   | change-streamer -> subscribers (fan-out)   | `Forwarder.forward()` fire-and-forget until `unflushedBytes >= getDefaultHighWaterMark(false)` (64 KiB), then `forwardWithFlowControl()` -> `Broadcast`: majority ack of _that one message_, then a timeout of 2x the majority's elapsed for the rest | JSON bytes, then one message's ack | 64 KiB pipelined per checkpoint            | remote subscribers acking one frame           | manager heap and socket buffers             |
| 6b  | same, the laggard rule                     | `Forwarder.checkSubscriberProgress()` every 1s: a subscriber that missed the last timeout _and_ is slower than the slowest on-time subscriber for 30s is closed with `StreamTooFarBehind`                                                             | time                               | 30s                                        | --                                            | the other subscribers' throughput           |
| 7   | change-streamer -> replicator, on the wire | `streamOutStringified`: one WS frame per message, `{ack: id}` per frame back; `ws.bufferedAmount` is never consulted                                                                                                                                  | frames                             | unbounded between checkpoints (so: 64 KiB) | the replicator consuming                      | nothing directly                            |
| 7b  | same, during catchup                       | `Subscriber` backlog: live changes buffered in a `RingBuffer` while the log is read; `send()` stops resolving at 16 MiB (released at 80%); SQLite catchup abandons on overflow and `fail()`s the subscriber                                           | JSON bytes                         | 16 MiB per subscriber                      | the catchup read plus the subscriber draining | manager heap                                |
| 8   | replicator main thread -> write worker     | batch of 64 KiB or 500 messages or "nothing else queued"; `assert(#pending === null)` -- one batch in flight                                                                                                                                          | JSON bytes, messages               | 64 KiB / 500                               | SQLite applying                               | replicator heap (`postMessage` clones)      |
| 9   | replicator -> view-syncers                 | `Notifier`: coalesced version notifications                                                                                                                                                                                                           | --                                 | never blocks                               | --                                            | (correct: serving never stalls replication) |
| 10  | change-streamer -> Postgres (the ACK)      | `UpstreamAcker`: `min(PG change log committed, litestream-v5 backup watermark, archive durable cursor)`                                                                                                                                               | watermark                          | --                                         | durability of the manager's own stores        | what Postgres may discard                   |
| 11  | archive -> base producer                   | `ArchiveChangeSource`: sealed-segment polling; `await sub.push().result` per message                                                                                                                                                                  | messages                           | 1 in flight                                | the producer applying                         | producer heap                               |

Three things fall out of reading the table as a whole.

**Six different units.** Messages (1, 2, 11), heap bytes by estimate (3),
disk bytes (5), JSON bytes (6, 7b, 8), frames (7), and time (6, 6b). Each
was sized on its own; none was sized against a neighbor. The 64 KiB
checkpoint in (6) is Node's default stream high-water mark -- a
socket-buffer number -- and the replicator's batch cap in (8) was set equal
to it "so neither side runs far ahead of the other", but (6) waits for the
ack of a single frame and which replicator batch that frame lands in is
accidental.

**Two purposes in one mechanism.** (6) exists, per its own documentation, for
memory safety: "without doing so, I/O buffers fill up and cause the system
to spend most of its time in GC." But it is implemented as pacing -- the
source does not advance until a quorum of remote processes report progress.
Memory safety is a local property of one process; it does not need a remote
party's opinion, let alone a majority of them.

**One clean separation, worth keeping.** (10) is the only mechanism that
decides what may be _discarded_, and it is gated purely on durability, never
on subscriber progress. A slow view-syncer does not retain WAL. This is
right and the proposal below leaves it alone.

## What is wrong

### 1. The live stream waits for subscribers, and subscribers wait for each other

`Broadcast` makes the source's progress at each checkpoint a function of the
slowest member of the majority. With one view-syncer and the
backup-replicator -- the common small deployment -- n=2, majority=2, and
there is no minority for the consensus timeout to release. Every checkpoint
waits for both.

The throughput document measured exactly this, pre-batching, at the same
8000 rows/s offered load: one subscriber applied 5349 rows/s at 0.89 cores;
two subscribers applied 3446 rows/s each at 0.68 cores apiece, on a box with
~1.5 idle cores and a disk at 10% of its ticks. Nobody was short of
anything. Post-batching the gap narrowed to 7813 vs 6593, because each
checkpoint now carries ~20 changes instead of one -- which is to say the
mechanism's cost is per checkpoint, and batching bought fewer checkpoints,
not a better mechanism.

The checkpoint is also stop-and-wait with a window of one. Acks are strictly
ordered (`streamOut` throws on an out-of-order ack), so when the ack for the
checkpoint frame arrives, the subscriber has dequeued everything the manager
sent; the manager then bursts the next 64 KiB into an empty queue. With one
subscriber that is cheap: the consumer is the bottleneck and keeps one batch
in hand across the gap. With two balanced subscribers each checkpoint costs
`max(t1, t2)` and the jitter compounds; that is what 36% looks like.

### 2. "Ack" means two things, and on this branch it just changed meaning

The per-frame ack is read by three consumers with three different needs:

- `Broadcast`, for pacing (what is the subscriber's _intake_?)
- `Subscriber.acked`, via `Forwarder.getAcks()`, for the PG change-log purge
  floor (`#getCleanupFloor`) and the SQLite purge scheduler (what has the
  subscriber _durably applied_, so the log may forget it?)
- the "consumed means committed by the applier" assumption that
  `ArchiveChangeSource.holdAtBoundary()` and the workplan's freeze design
  rely on

Before the write-worker batching, `IncrementalSyncer` called `next()` only
after `await processMessage`, so all three got the same answer: an ack meant
applied and, at a `commit`, committed to SQLite. Batching changed the loop to
call `next()` to fill the batch, and `Subscription`'s consumed-on-iterate
semantics (`[Symbol.asyncIterator]().next()` calls `prevConsumed()` before
awaiting the next entry) now fire the ack for message N when N+1 is
dequeued -- before N is applied -- for every message except the last in a
batch.

The archive source is unaffected: it awaits each push, so the producer's
batch is always one message and consumed still means applied.
`holdAtBoundary()` remains correct. The live WebSocket path is where the
queue can be deep, and there:

- Pacing now measures intake. That is arguably what memory safety wants,
  and the overlap it buys (the manager refills while the replicator applies)
  is part of the measured gain.
- `Subscriber.acked` is optimistic by up to one batch. A replicator that acks
  commit W and dies before its batch applies restarts from W' < W. If a purge
  cycle runs in that window and the backup watermark and every other ack
  are past W', the replica finds its watermark below the log and must
  restore from backup instead of catching up. The floor is `min(backup,
acks)`, so this is never data loss; it is a forced restore with a window
  of one batch times the purge cadence. Rare, bounded, and a semantic
  regression this branch introduced. It is listed under decisions below
  rather than fixed silently, because the fix that keeps the throughput is
  the protocol change in this proposal, and the fix that is available today
  gives some of the throughput back.

### 3. The escape hatch is a restore

When a subscriber cannot keep up, the sequence is: it misses the consensus
timeout; the progress monitor compares its rate to the slowest on-time
subscriber; after 30s of "lagging" it is closed with `StreamTooFarBehind`;
`IncrementalSyncer` treats any `['error', ...]` as terminal, deletes the
replica file, and stops so the process restarts and restores from backup.

For a view-syncer that is a serving outage of the restore's duration. In
litestream-v5 mode, for the backup-replicator, it is the ACK gate stalling
until the restore completes, which is WAL retention. Slowness is converted
into an outage rather than into lag. The comment on `Subscriber.fail()`
already makes this distinction for _transient_ failures -- they are routed
as clean closes precisely so that a reconnect, not a fleet-wide restore, is
the response -- and the laggard path is the one that still sends the error.

### 4. In the archive world the outer buffer is Postgres WAL, and nothing says so

The archive world removes the applier from the live stream, so subscriber
pacing is not its question. Its question is what happens when S3 is slow:
fail-stall keeps the durable cursor from advancing; the spool and sealed
segments fill 128 MiB of manager disk; `readyForMore()` blocks the loop;
the multiplexer stops consuming; the socket fills; walsender blocks; WAL
retention grows; `max_slot_wal_keep_size` is reached; the slot is
invalidated; the stack resyncs from scratch.

Every step of that is the correct posture -- never ACK what is not durable,
and never lose data to avoid retaining it -- until the last one, which is a
Postgres setting the manager does not read and the operator may not have
set. The throughput runs retained 7-10 MiB at 4-5k rows/s with the seal
interval turned down to 1s; at the 30s default that is ~30x more at the same
rate, before any S3 trouble. The gauges exist (`slot_retained_wal_bytes`,
`slot_safe_wal_bytes`). The policy does not.

### 5. Everything else is a consequence of (1)

The consensus timeout, the proportion knob, the laggard grace period, the
rate comparison against the slowest on-time subscriber, the "catching-up vs
lagging" classification -- all of it exists to soften the blow of (1) in the
scenarios `Broadcast`'s documentation lists: a new subscriber with a
backlog, a broken TCP connection, a zombie task. Each is a heuristic for
deciding _how long the fleet should wait for one subscriber_. If the fleet
never waits for one subscriber, none of them has a job.

## The shape it should have

The model to hold in mind: **the manager is a log with a tail buffer;
subscribers are independent cursors on that log.** Every subscriber is
always catching up; "live" is the optimization where the read is served
from the in-memory tail instead of from the log. That is the shape of every
log-structured broker for the same reason it should be the shape here: the
producer's rate and each consumer's rate are decoupled by the log, and the
only global back-pressure is _log retention_, which is a durability and disk
decision, not a speed decision.

Seven principles, then what they mean in the code.

**P1. Three concerns, three mechanisms, never shared.**

- _Memory and disk safety_ is local and byte-keyed: a buffer that is full
  stops its own producer from reading more input. It never waits for a
  remote party. (3) and (5) are already this.
- _Durability_ -- what Postgres may discard -- is `UpstreamAcker`, unchanged.
- _Subscriber pacing_ is a per-subscriber window. A subscriber that exhausts
  it stops being served from the tail and is served from the log. It never
  slows ingestion and never slows another subscriber.

**P2. The live stream never waits for a subscriber.** `Broadcast`, the
majority, the consensus timeout, the laggard rule and both `flowControl*`
knobs go away. Their scenarios all reduce to one local condition -- this
subscriber's window is exhausted -- with one local response: it is no longer
live. A new subscriber with a backlog is not live yet; a broken TCP
connection stops acking and falls out of the window; a zombie is a broken
connection that takes longer.

**P3. Ingestion is gated only by the manager's own durable stores.** The
storer's heap threshold, the archive's disk threshold, and behind both of
them Postgres WAL. That is the current loop minus `forwardWithFlowControl`.
Note what this means for the change-streamer's per-change CPU: the manager
runs at the speed of its stores, and the profile's `streamOutInternal` share
becomes a fan-out cost that batching can amortize, not a wait.

**P4. Catchup is a steady-state mode, not a startup phase.** A live
subscriber that falls behind is demoted to catchup -- the SQLite change log
first, PG as the fallback, exactly the routing `subscribe()` does today --
and promoted back when its read reaches the tail. All of the machinery
exists: `Storer.catchup()`, `SQLiteChangeLogCatchup`, the subscriber backlog
and `setCaughtUp()`'s bounded drain. What is new is _re-entering_ it. If the
backlog fills while catching up, the backlog is dropped and catchup restarts
from the subscriber's current watermark -- the log has everything, so
nothing is lost -- and the loop converges exactly when the subscriber's
catchup rate exceeds the live rate, which is the same eventual-catchup
argument `Broadcast`'s documentation makes. If it never converges, the
subscriber lags. Lag is the right outcome. Retention is the one global knob:
`min(durable floor, retention window)`, and a subscriber whose watermark
falls below retention restores from backup, which becomes the _only_ reason
a subscriber ever restores.

**P5. Batch on the wire, and ack cumulatively with the applied watermark.**
One frame per batch, capped in bytes and in latency the way the replicator's
worker batch already is. One ack per SQLite commit (or per applied batch,
whichever is less frequent): "everything through frame N is applied", where
the frame at a `commit` boundary carries the commit's watermark. The frame
format need not change -- `{ack: id}` stays -- only its semantics: cumulative,
ids may be skipped, and `streamOut` releases every frame `<= id` instead of
throwing on `!== id`. This single change does three things at once:

- it removes the per-message frame, `stringify`, and ack-parse cost the
  throughput document identified as the manager's next ceiling;
- it restores "ack means applied" for the purge floors and for anything
  else that reads `Subscriber.acked`, fixing (2);
- it gives the manager a byte-accurate per-subscriber window
  (`sent - acked`, which `Subscriber` already tracks as `#pendingBytes`)
  to implement P1's pacing without any remote wait.

Pacing and durability then read different things from the same ack: the
window reads bytes released; the floor reads the watermark. Intake and apply
are no longer conflated because there is no intake signal at all -- the
replicator's own bounded batch is its intake bound.

**P6. Everything in bytes, with a time bound.** Per-subscriber window in
bytes (1 MiB is the right order: enough to cover a WS round trip plus one
worker batch at the measured rates, small enough that N subscribers' windows
are not a memory story). Wire batch caps in bytes and milliseconds, and the
"flush when nothing else is queued" rule from the worker batch, so batching
never buys throughput with latency it does not need. Backlog high-water as
the demotion threshold (the 16 MiB that exists). Retention in time and in
bytes. No limit counted in messages, no limit counted in frames, and no
limit whose value is a socket-buffer default.

**P7. WAL is the outer buffer; give it a policy.** The manager reads
`max_slot_wal_keep_size` at startup and on reconnect. Unset (`-1`) is
logged as "unbounded: an archive or change-DB stall will retain WAL until
the disk fills"; set, it is exported as the denominator of an
`archive_wal_budget_used` ratio next to `slot_retained_wal_bytes`, and a
warning fires when retained WAL crosses a configurable fraction of it. The
documentation for backup mode `archive` says, in these words, that
fail-stall trades WAL for durability and that the budget is this setting,
sized to N minutes of peak write rate.

### What this looks like in the code

By module, in the order the data flows.

- **`change-streamer-service.ts`, the loop.** Delete `unflushedBytes`,
  `flushBytesThreshold`, and the `forwardWithFlowControl` branch;
  `forwarder.forward(entry)` is the only path. The storer and archive
  `readyForMore()` waits stay. The loop is now paced by its stores, and by
  nothing downstream.
- **`forwarder.ts` and `broadcast.ts`.** `Broadcast` is deleted.
  `Forwarder` keeps the active/queued transaction-boundary bookkeeping
  (which `Storer.catchup()` depends on), the stats gauges, and gains a
  demotion counter. `startProgressMonitor()` becomes a stats sampler with no
  side effects.
- **`subscriber.ts`.** Live `send()` enqueues into a bounded window
  (bytes in flight against the cumulative ack); when the window is full,
  changes go to the existing backlog; when the backlog crosses its
  high-water mark the subscriber is demoted. `acked` advances only from
  cumulative acks, which now mean applied. In phase 1 demotion is
  `fail()` -- a clean close, which `IncrementalSyncer` already answers with
  backoff and a re-subscribe from its replica's watermark, landing in the
  same `subscribe()` catchup routing a new subscriber gets. In phase 2 it is
  in-place: `#backlog` re-created, the catchup coordinator re-registered from
  `acked`, the boundary alignment `Forwarder.add()` already handles. Phase 1
  is a few dozen lines and exercises paths that exist; phase 2 removes a WS
  handshake per demotion and should wait for evidence that demotions are
  frequent enough to matter.
- **`streams.ts`.** `streamOutStringified` sends one frame per batch, sized
  by the same caps as the worker batch and flushed on "nothing else queued";
  `id` is per batch. Acks are cumulative. `streamInStringified` hands the
  consumer the batch and an explicit `consumed()` -- the `pipeline` iterable,
  not consumed-on-iterate -- so the replicator acks after apply, not after
  dequeue.
- **`incremental-sync.ts`.** Keeps its batching; a wire batch is already a
  batch. Calls `consumed()` after `flush()` resolves, or sends the
  cumulative ack itself after each result that carries a commit.
- **`change-streamer-http.ts` and protocol version.** This is v7:
  batch frames and cumulative acks. `MIN_SUPPORTED_PROTOCOL_VERSION` is 4,
  so a v7 manager serves v4-v6 subscribers during rollout by framing per
  message for them, selected by the subscriber's `protocolVersion` the way
  `supportsMessage()` already selects message types. The window and
  demotion logic applies to both framings, so old subscribers get P2's
  behavior immediately and P5's cost savings when they upgrade.
- **Config.** `flowControlConsensusTimeoutProportion` and
  `flowControlSlowSubscriberGracePeriodSeconds` are accepted and ignored for
  one release, with a deprecation notice, then removed. New:
  `flowControlSubscriberWindowBytes` (default 1 MiB), wire batch caps
  (bytes and ms), and a WAL-budget warning fraction. The heap-proportion and
  archive-disk knobs are unchanged.
- **Metrics.** `flow_control.waits` and `wait_duration` stop being emitted
  (there are no waits). New: per-subscriber window bytes, `demotions`
  labeled by cause (window, backlog, connection), a gauge of subscribers in
  catchup, and the WAL-budget ratio.

### What it changes in the measured picture

Be explicit about this, because the throughput document's headline depends
on it. That document attributes the archive gateway's advantage over the
litestream shape to subscriber count, and says the advantage "never
reverses: a subscriber that is not there cannot be waited for." Under this
proposal a subscriber that _is_ there is not waited for either. The
litestream-shaped manager's serving view-syncer should land on the
no-backup control's number (5603 vs 5600 pre-batching; both shapes should
converge to whatever the single-subscriber ceiling is), and the archive
world's throughput advantage on the serving path goes to zero.

What the archive world keeps is everything else it was built for: a manager
with no replica volume, no restore at startup, no litestream process, no
backup-replicator worker, and an applier that can be sized and placed
independently. Those were always the reasons; the throughput advantage was
a measurement of the current flow control's cost, and the honest reading is
that it measured a defect the archive world happened to route around.

The manager's ceiling moves up as well: the fan-out cost becomes one
`stringify` and N socket writes per batch instead of per change, and the
loop never idles waiting for a remote ack.

## Decisions

1. **The ack regression on this branch (2).** Options: (A) carry it until
   v7 -- the window is one batch, the floor is the backup watermark, the
   consequence is a forced restore, not loss; (B) fix now by switching
   `IncrementalSyncer` to the `pipeline` iterable and calling `consumed()`
   after `flush()` -- about fifteen lines, exact semantics, and it costs the
   refill overlap, so the ladder should be re-run; (C) B now and v7 after.
   Recommendation: B. It is the same code shape v7 needs on the replicator
   side, and the throughput document's numbers should stand on the semantics
   the purge floors assume. The re-measurement is one harness run.
2. **Demotion: phase 1 (reconnect) or phase 2 (in-place) first.**
   Recommendation: phase 1, with the demotion counter, and phase 2 only if
   the counter shows demotions are routine rather than exceptional.
3. **The window default and whether it is per subscriber.** Recommendation:
   per subscriber, 1 MiB, configurable. A global window reintroduces coupling
   between subscribers through a shared budget.
4. **One protocol bump or two.** Batch framing and cumulative acks could ship
   as separate versions. Recommendation: one, v7. Shipping batch frames with
   per-frame acks first would mean designing the batch around a signal this
   proposal removes.
5. **WAL budget: warn or refuse.** Whether an archive-mode manager should
   refuse to start with `max_slot_wal_keep_size` unset. Recommendation: warn
   and export the ratio. Unbounded is the correct default for durability;
   the failure it risks is a full disk, which is visible, rather than a lost
   slot, which is a resync.
6. **Retention as the one knob.** `sqliteChangeLogRetentionMs` defaults to
   60s and the PG change log purges to the backup watermark. Under P4 a
   subscriber's tolerable lag is bounded by retention, so the default needs
   to be chosen as a product number ("a view-syncer may fall N minutes
   behind before it must restore"), not a rollout-staging number.

## Measurement before and after

- **`flowControlWaitDurationMs` is null in every ceiling result.** The
  harness records the wait _count_ (3231 at 8000 rows/s in both shapes --
  identical by construction, since a checkpoint is every 64 KiB of the same
  offered load) but not the histogram. The wait _time_ by release mode is
  the number that quantifies (1) directly, and it should be captured from
  the current code before any of this changes, so the before/after is a
  measurement and not an inference from CPU idleness.
- **A deliberately slow second subscriber.** `SIGSTOP` one of two
  view-syncers for five seconds mid-run. Today this should show the
  consensus timeout releasing, the serving view-syncer's rate dropping in
  proportion to the timeout, and -- if the stop exceeds 30s -- an eviction
  and a restore. After: the serving view-syncer's rate unchanged, one
  demotion, one catchup, one promotion. That run is the acceptance test for
  P2.
- **The ladder, re-run.** The litestream-shaped manager should land on the
  no-backup control at every rung.
- **A memory ceiling with many subscribers.** N idle subscribers times the
  window plus the backlog high-water is the manager's fan-out budget; the
  existing memory-ceiling drill should be pointed at it with N = 20 to set
  the window default with evidence rather than the order-of-magnitude guess
  above.

## What this does not change

- `UpstreamAcker` and the durability gating of ACKs, including the archive
  cursor and the litestream-v5 backup watermark.
- The storer's heap threshold and the archive writer's disk threshold and
  fail-stall posture.
- The replicator's worker batching, its caps, and the never-hold-a-
  transaction bound.
- The `Notifier`'s never-block relationship between replicator and
  view-syncers. Serving lag is the client's to absorb; that is what CVR
  diffs are for.
- The producer's `ArchiveChangeSource` and `holdAtBoundary()`; its
  one-in-flight push keeps consumed meaning applied regardless of (2).
