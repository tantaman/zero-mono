# logical-log-compression

Measures how well change-streamer logical-log chunks compress, so we can size
the cost and restore time of shipping fixed-size chunks of CDC stream to S3
instead of running litestream.

## What it measures

The bytes under test are the real thing, not an approximation. The harness
starts an actual Postgres logical replication stream through
`initializePostgresChangeSource`, drives real mutations, and serializes each
data-plane message with `serializeChangeStreamData` -- the same call the
`Storer` makes. Messages are framed as newline-delimited JSON and cut into
fixed-size chunks at message boundaries.

A captured line looks like:

```
["begin",{"tag":"begin","commitLsn":"0/6870C10","commitTime":...,"xid":2330,"json":"s"},{"commitWatermark":"51t79cg"}]
["data",{"tag":"insert","relation":{"tag":"relation","relationOid":19572,"schema":"public","name":"label","rowKey":{"columns":["id"],"type":"default"},"keyColumns":["id"],"replicaIdentity":"default"},"new":{...}}]
["commit",{"tag":"commit","flags":0,"commitLsn":"0/6870C10","commitEndLsn":"0/6870C40","commitTime":...,"commitTimeMs":...},{"watermark":"51t79cg"}]
```

## Datasets

Real data, loaded into a local Postgres:

| dataset | source                            | character                                                        |
| ------- | --------------------------------- | ---------------------------------------------------------------- |
| chinook | `lerocha/chinook-database` v1.4.5 | short strings, integer FKs, a 2-column join table                |
| pagila  | `devrimgunduz/pagila`             | timestamps, numerics, enums, arrays, tsvector, partitioned table |
| zbugs   | `apps/zbugs/db/seed-data/github`  | real GitHub issue/comment markdown                               |

## Methodology notes

Two things would otherwise bias the result upward, and are deliberately
avoided:

- **Row duplication.** Generated rows recombine sampled column values at
  co-prime strides rather than copying whole real rows, so value distributions
  are preserved without repeating entire rows.
- **Text repetition.** Cycling zbugs' 924 real comment bodies to fill a 16MiB
  chunk would make it look far more compressible than production. Text columns
  are filled from `corpus.ts` (13.4MB of distinct developer prose, markdown and
  code from this repo) at the _real_ length distribution sampled from the
  seeded tables. `insert-comment-highentropy` fills the same fields with
  incompressible bytes to bracket the pessimistic end.

## What it found

A row change is one insert, update or delete -- not one message, since a
single-row transaction emits three (`begin`, the change, `commit`).

- **Ratios span 4.4x to 132x** with `zstd-3`, so the ratio is not a useful
  planning number. **Stored bytes per row change** is: 2 B for a batched
  delete, ~7 B for a narrow join-table insert, ~46 B for a pagila row, ~106 B
  for zbugs' mixed OLTP traffic.
- **Transaction size is the biggest lever.** `begin`/`commit` carry an LSN, an
  end LSN, an xid, a microsecond commit time and a watermark -- all unique per
  transaction, so they survive compression nearly intact. Measured floor:
  38-65 stored bytes per transaction. Committing one row at a time makes a
  chinook insert 8.9x more expensive on S3 than committing a hundred.
- **A single random `uuid` column is 52% of pagila's compressed chunk.**
  Dropping it takes `insert-rental-batch100` from 9.3x to 17.1x. Random
  identifiers are the most expensive thing per byte a row can carry.
- **zstd's default window is smaller than the chunk** (2 MiB at level 3,
  4 MiB at level 9). `ZSTD_c_windowLog = 24` costs no measurable throughput
  and is worth up to +73% on the chunks where long-range matching matters.
- **Compression throughput is not the constraint.** 100k row changes/s is
  ~40 MiB/s of logical log; `zstd-9-win24` does 163 MiB/s. The level is a
  CPU-budget decision, not a keep-up decision.
- **Restore is not download-bound.** At 16 MiB chunks, a 100 GiB replay
  downloads in seconds at modest GET concurrency but takes 30-115 s to
  decompress on one core -- and applying the changes is slower still.
- **`REPLICA IDENTITY FULL` costs 3.7x** the stored bytes per row change on an
  otherwise identical update.

## Running it

Needs a Postgres with `wal_level = logical`:

```bash
# 1. capture chunks (Postgres-bound; datasets can run concurrently)
node src/main.ts --datasets chinook,zbugs,pagila --chunk-mib 16 \
  --skip-bench --save-chunks ./chunks --out capture.json

# 2. benchmark codecs serially on an idle machine
node src/bench-chunks.ts --chunks ./chunks --captures capture.json --out results.json

# 3. optionally merge in the large-window zstd variants
node src/bench-extra.ts --chunks ./chunks --results results.json

# 4. attribute a chunk's compressed size to individual columns
node src/field-cost.ts --chunks ./chunks --only pagila--insert-rental-batch100

# 5. cost and restore model, and the HTML report
node src/model.ts --results results.json --codec zstd-9-win24 --restore-gib 100
node src/report.ts --results results.json --chunks ./chunks --out report.html
```

`--pg-url` defaults to `postgres://postgres@127.0.0.1:54329`.

## Committed results

`results.json` and `field-cost.json` are the measured output of the run
described above, so the report can be regenerated without a Postgres:

```bash
node src/report.ts --results results.json --field-cost field-cost.json --out report.html
node src/model.ts --results results.json --codec zstd-9-win24
```

## Files

- `capture.ts` -- drives the real change source, accumulates serialized bytes
- `corpus.ts` -- non-repeating real text at a target length distribution
- `workloads.ts` / `datasets.ts` -- the insert/update/delete schemes
- `codecs.ts` -- zstd / gzip / brotli with ratio and throughput
- `bench-chunks.ts` / `bench-extra.ts` -- codec benchmarks over saved chunks
- `field-cost.ts` -- marginal compressed cost of each row column
- `intern.ts` -- re-encodes a chunk with `relation` blocks interned, to
  separate "the format is redundant" from "the compressor already fixed it"
- `model.ts` -- S3 cost, GET concurrency and NIC saturation
- `report.ts` + `report/` -- the self-contained HTML report
