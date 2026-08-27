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

## Running it

From the repo root, after `pnpm install`:

```bash
pnpm --filter zero-cache run bench:ceiling             # 2M-row replica, 24 MB log
pnpm --filter zero-cache run bench:ceiling 200000 12   # smaller, for a first look
```

That builds both harnesses, generates a base replica and a log, and runs every
variant against the same input: the C ceiling, deferred index rebuild, secondary
indexes dropped, and the best TypeScript applier — followed by the key-scheme
comparison. Roughly 5 minutes and ~3 GB of disk at the default size; well under
a minute at the smaller one.

Requirements: a C compiler, node, and `pnpm install` already run. Linux and
macOS both work.

Knobs:

```bash
ROWS=4000000 LOG_MB=96 pnpm --filter zero-cache run bench:ceiling
WORKLOADS="mixed" pnpm --filter zero-cache run bench:ceiling
CEILING_DIR=/dev/shm/zllc pnpm --filter zero-cache run bench:ceiling   # see below
```

To drive the pieces individually, see `run.sh` — it is short, and each step is
`node export.ts`, `./apply`, `node jsref.ts`, `./keys`.

**Every variant prints a checksum over the resulting replica and the runner
fails loudly if they disagree.** That guard is not decoration: it has already
caught the reference applier silently replaying a *different* log than the C
harness because it regenerated one from its own defaults.

## Is it just a slow disk?

Worth ruling out, and it is ruled out here — but check it on your own hardware,
because the answer depends on the machine:

- `/proc/self/io` over the measured region (Linux; the harness prints it)
  reports **0 bytes read** from storage and ~17 MB written for a 24 MB log.
  The replica is entirely in the OS page cache and there is no block-level
  write amplification.
- Putting the replica on tmpfs is worth **~10%**.
- `PRAGMA mmap_size`, which removes the read syscalls entirely, is worth ~8%.
  `PRAGMA cache_size` from 64 MB to 4 GB is flat.

To check directly, point the harness at a RAM disk and compare:

```bash
CEILING_DIR=/dev/shm/zllc pnpm --filter zero-cache run bench:ceiling      # Linux
# macOS: diskutil erasevolume HFS+ RAM $(hdiutil attach -nomount ram://8388608)
CEILING_DIR=/Volumes/RAM/zllc pnpm --filter zero-cache run bench:ceiling
```

If the numbers barely move, the workload is CPU-bound on your machine too. What
*will* differ is single-core speed — these numbers come from a shared 2.80 GHz
cloud vCPU, and a modern laptop or desktop core should be materially faster.
The ratios between variants are what travel.

## Reference results (4 vCPU Xeon @ 2.80 GHz)

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

## `keys.c` — why replay slows down as the replica grows

A second harness in this directory, crossing key scheme (random uuid4 /
time-ordered uuidv7 / decimal suffix) against table structure (rowid + unique
index, as the replica has today, vs `WITHOUT ROWID` with a real primary key).

```bash
make keys
./keys 2000000 50000     # rows, then random updates
```

Headline: time-ordered keys insert **2.4×–4.1× faster** at every table size and
make **no difference to updates**; a real `WITHOUT ROWID` primary key is
*slower* on both, because ~600-byte rows make for a deep key-ordered tree.
Neither flattens the size curve.

See `LOGICAL_LOG_REPLAY_BENCHMARK.md` at the repo root for the full picture.
