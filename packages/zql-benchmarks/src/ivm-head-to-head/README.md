# Zero vs rindle — IVM engine head-to-head (Zero side)

This directory is **Zero's half** of a cross-engine benchmark. The other half is
[rindle](https://github.com/tantaman/rindle)'s Rust IVM engine — a port of this
engine — and the point of the exercise is to run *the same query shapes over the
same data through both* and see where the two diverge.

Nothing here runs in CI or in `pnpm bench`; these are standalone scripts, driven
from the rindle side by `tools/ivm-head-to-head.mjs` and
`tools/cluster-vs-zero-cache.mjs`.

## The two benchmarks

| file | what it measures | rindle counterpart |
|---|---|---|
| `main.ts` | the **core engine**: hydration, per-write maintenance and held state for one query, on the `MemorySource` leaf and the `zqlite` `TableSource` leaf, in view mode (`ArrayView`) and sink mode | `rust/rindle-triangle-bench` |
| `zero-cache-derive.ts` | the **derivation plane**: zero-cache's `PipelineDriver` keeping V registered queries fresh per replicated write | `rust/rindle-replica/examples/bench_cluster_derive.rs` |

## Running it

```sh
curl -fsSL -o /tmp/Chinook_Sqlite.sql \
  https://github.com/lerocha/chinook-database/releases/download/v1.4.5/Chinook_Sqlite.sql

# core engine — one cell, or the whole matrix
node --experimental-transform-types --expose-gc \
  packages/zql-benchmarks/src/ivm-head-to-head/main.ts --cell zero_mem exists 1
node --experimental-transform-types --expose-gc \
  packages/zql-benchmarks/src/ivm-head-to-head/main.ts

# derivation plane
node --experimental-transform-types --expose-gc \
  packages/zql-benchmarks/src/ivm-head-to-head/zero-cache-derive.ts
```

`--expose-gc` is required for the `state_bytes` metric (a forced major GC on
either side of building the pipeline); without it the driver warns and the
number is noise. `--experimental-transform-types` is what lets Node run the
workspace's TypeScript sources directly — there is no build step.

Env knobs, shared with the rindle side so a filter narrows both matrices
identically: `CHINOOK_SQL`, `TRIANGLE_SCALES`, `TRIANGLE_W`,
`TRIANGLE_PATTERNS`, `TRIANGLE_CONTESTANTS`, and for the derivation bench
`DERIVE_SCALES`, `DERIVE_VIEWS`, `DERIVE_W`, `DERIVE_SHAPES`.

## What makes it a fair comparison

The loader, the scaling, the SQLite schema and indexes, the timing estimator
(~150 ms warmup, then the min ns/op over rounds), the write stream and the
labelled push cases are all ports of the rindle harness — the two are meant to
be diffed line for line. Divergences that are *deliberate* are called out in the
file that owns them:

- **`count` is absent.** Zero's ZQL has no aggregation operator, so rindle's
  group-by/count has nothing to race here.
- **`state_bytes` is not the same instrument on both sides.** rindle counts
  bytes from a counting global allocator; V8 can only be asked for `heapUsed`
  after a forced GC, which still includes slack. Same order, not same precision.
- **Operator state lives on the JS heap by default** (`MemoryStorage`, the
  client's configuration). `zero_tab_dbsto` is the same cell with
  `DatabaseStorage`, which is what zero-cache runs.

The harness also asserts that no pipeline is left un-torn-down between
measurements (`assertNoLeak`). That check exists because its absence silently
corrupted a whole run: `ArrayView.destroy()` only calls its `onDestroy`
callback, so a harness that builds views directly and forgets to supply one
leaks a source connection per build — and since `Source.push` fans out to every
live connection, the later push measurements were timing pushes into hundreds of
abandoned pipelines (`top50` on the SQLite leaf read 801 ms/write instead of
~1 ms).
