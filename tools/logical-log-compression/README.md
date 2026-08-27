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

## Running it

Needs a Postgres with `wal_level = logical`:

```bash
# capture chunks (Postgres-bound; datasets can run concurrently)
node src/main.ts --datasets chinook,zbugs,pagila --chunk-mib 16 \
  --skip-bench --save-chunks ./chunks --out capture.json

# benchmark codecs serially on an idle machine
node src/bench-chunks.ts --chunks ./chunks --captures capture.json --out results.json

# cost and restore model
node src/model.ts --results results.json --restore-gib 100
```

`--pg-url` defaults to `postgres://postgres@127.0.0.1:54329`.

## Files

- `capture.ts` -- drives the real change source, accumulates serialized bytes
- `corpus.ts` -- non-repeating real text at a target length distribution
- `workloads.ts` / `datasets.ts` -- the insert/update/delete schemes
- `codecs.ts` -- zstd / gzip / brotli with ratio and throughput
- `intern.ts` -- re-encodes a chunk with `relation` blocks interned, to
  separate "the format is redundant" from "the compressor already fixed it"
- `model.ts` -- S3 cost, GET concurrency and NIC saturation
