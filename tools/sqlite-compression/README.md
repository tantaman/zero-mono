# SQLite compression analysis

Measures how far a zero-cache SQLite replica compresses, so a base-image
upload to S3 can be sized before it is minted.

The databases here are generated, not sampled, because the question is about
100GB replicas and we do not have one to hand. They are shaped to match what
`zero-cache` actually writes:

* no declared `PRIMARY KEY` — `pg-to-lite.ts` drops it and relies on UNIQUE
  indexes instead, so the PK costs a full auxiliary B-tree
* a `_0_version` TEXT column on every table
* lite type strings (`varchar|NOT_NULL`) as the declared column types
* every index from the upstream schema recreated

Two schemas, chosen because they sit at opposite ends of the key-shape axis:

| | keys | text |
|---|---|---|
| `zbugs` | 21-char nanoid TEXT | issue/comment prose |
| `chinook` | INTEGER | short repeated labels |

## Scripts

| script | what it answers |
|---|---|
| `gen_db.py` | builds a replica-shaped DB at a target size |
| `bench.sh` | ratio + throughput across codecs and levels |
| `run_matrix.sh` | the size x schema x codec sweep (build → bench → delete) |
| `window.sh` | does the ratio drift with size, and does `--long` fix it |
| `strategies.sh` | page image vs. VACUUM vs. logical dump; page size; restore cost |
| `sensitivity.sh` | how much `_0_version` cardinality and text entropy move the ratio |
| `makeup.sh` | supplementary points: real-lz4 throughput, raw disk read rate |
| `model.py` | projects to 100GB and prices the S3 upload |
| `status.sh` | progress of a running sweep |

## Running it

```bash
# one database, one codec sweep
python3 gen_db.py --schema zbugs --gb 1 --out /tmp/replica.db
bash bench.sh /tmp/replica.db zbugs 1GB full

# the whole sweep (hours; builds and deletes one DB at a time)
bash run_matrix.sh

# project and price
python3 model.py --results matrix.jsonl --target-gb 100
```

`gen_db.py` knobs that matter:

* `--version-mode constant|mixed|random` — `_0_version` cardinality.
  `constant` models a replica straight out of initial-sync; `mixed` a replica
  that has been live a while; `random` is the pessimistic bound.
* `--text-entropy template|high` — `template` replays the gigabugs seed corpus
  verbatim, which is far more repetitive than real issue prose. `high` adds
  per-row unique traces and ids. Real data sits between them.

## Results

`results/` holds the raw measurements from the sweep this harness was written
for: 28 databases across two schemas at 1/2/4/10GB, 11 codec configurations,
on 4 vCPU. `report.txt` is `model.py` run over `matrix.jsonl`.

Headline: a 100GB replica lands at 35-45GB uploaded with zstd-3 -- roughly
2.3-2.8x, not the 5-10x usually assumed. 41-44% of the file is index B-tree,
and in a nanoid-keyed schema those pages are near-random bytes.

Three findings worth knowing before you tune anything:

* **The ratio tracks entity count, not file size.** Holding the file at exactly
  1GB and varying only how many users the issues point at moves zstd-3 from
  36.42% (549 users) to 41.81% (53,651 users). That accounts for nearly all of
  the +2.44pp-per-decade drift seen when the file itself grows. Integer-keyed
  chinook is immune -- SQLite varint-encodes its foreign keys, so they grow in
  the raw file at the same rate as their entropy.
* **A logical dump is half the size of a page image** (20.3% vs 39.2% for
  zbugs) because it carries no index data, but restore goes from 7.4s to 44.4s
  at 2GB and litestream cannot consume it.
* **VACUUM buys nothing** (39.20% -> 39.19%) and **raising the page size hurts**
  (39.20% at 4K, 40.63% at 64K). `--long` does not help either; it costs 30% of
  throughput for a fractionally worse ratio.

## Caveat

Generated data cannot settle the ratio for a real deployment — text entropy is
the largest single input and it is a property of the customer's data. Use these
numbers for the shape of the curve and the codec trade-off; measure the real
ratio by running `zstd -3` over an actual replica before committing to a
retention policy.
