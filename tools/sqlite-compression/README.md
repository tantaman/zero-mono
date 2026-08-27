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

## Caveat

Generated data cannot settle the ratio for a real deployment — text entropy is
the largest single input and it is a property of the customer's data. Use these
numbers for the shape of the curve and the codec trade-off; measure the real
ratio by running `zstd -3` over an actual replica before committing to a
retention policy.
