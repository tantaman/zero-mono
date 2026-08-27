# Logical log replay: the absolute ceiling

A C harness that answers one question: with *everything* removable removed, how
fast can a logical log be applied to a SQLite replica? It exists to bound the
optimization work — to say whether the TypeScript apply path is 2× or 100× off
what the machine can do.

Everything that could be taken out, has been:

- **No parsing.** The log is `mmap`'d and walked by pointer arithmetic over
  fixed-stride records. No tokenizing, no length prefixes to chase.
- **No copying.** Strings are bound with `SQLITE_STATIC` straight out of the
  mapping, so their bytes never move on the way into SQLite.
- **No allocation** per change.
- **Permanently prepared statements**, reset and rebound rather than looked up.

What is left is SQLite's own work plus the `bind`/`step` calls. That is the
number.

It is built against the same amalgamation and the same compile-time defines as
`@rocicorp/zero-sqlite3`, so it is not measuring a different SQLite.

## Running

```bash
# 1. Build the base replica and export a zero-parse binary log.
#    ROWS controls replica size; LOG_MB the amount of log to replay.
ROWS=2000000 LOG_MB=24 node export.ts

# 2. Build and run the C harness against a copy of the base.
make
cp "$TMPDIR/zero-logical-log-ceiling/base.db" work.db
./apply work.db "$TMPDIR/zero-logical-log-ceiling/log.bin"

# 3. Apply the *same* log through the best TypeScript applier, for comparison.
node jsref.ts
```

Both harnesses print the same checksum over the resulting replica — row counts,
id and timestamp length sums, and distinct `_0_version` values per table, plus
the final watermark. **They must match.** If they do not, the two paths did
different work and the timings are not comparable.

## Results on a 4 vCPU Xeon @ 2.80 GHz

Applying a 24 MB log (42,890 changes, `mixed` workload, 256 logical
transactions per commit) with `journal_mode=OFF`, `synchronous=OFF`,
`locking_mode=EXCLUSIVE`:

| Replica | C harness | Best TypeScript applier | Current path |
| --- | --- | --- | --- |
| 12 MB | **6.2 µs/change** (90 MB/s) | 13.3 µs (43 MB/s) | 32.4 µs |
| 977 MB | **18.1 µs/change** (31 MB/s) | 38.0 µs (14.7 MB/s) | 61.4 µs |

Read from that:

- **The ceiling is ~3.4× above today's path**, not 100×. 1 GB of log lands in
  ~33 s rather than ~2:06.
- **~2.1× of that gap is the JS↔native boundary**, and it holds at both replica
  sizes. That is the part no amount of TypeScript tuning reaches.
- **SQLite's size-dependent work is ~12 µs/change** (18.1 at 977 MB minus the
  6.2 fixed cost at 12 MB) — the irreducible part, and the reason replica size
  dominates everything else.

See `LOGICAL_LOG_REPLAY_BENCHMARK.md` at the repo root for the full picture.
