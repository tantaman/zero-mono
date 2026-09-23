# IVM regression harness

Measures query **hydration** and **query maintenance** (pushes) across commits
for three query shapes: simple queries with `limit`, queries with `related`,
and queries with `related` + `whereExists`.

```bash
# From the repo root, after `pnpm install`:
PARALLEL=2 packages/zql-benchmarks/src/ivm-regression/compare-commits.sh \
  /tmp/ivm base=1d9e880 head=HEAD -- memory zqlite driver
node packages/zql-benchmarks/src/ivm-regression/analyze.ts /tmp/ivm \
  --base=base --labels=base,head --summary=head
```

`compare-commits.sh` makes a worktree per commit, links it to this checkout's
`node_modules`, and copies `ivm-regression.ts` into it, so every commit runs
the same workload against its own `zql` / `zqlite` / `zero-cache` sources.
Pass several result directories (rounds) to `analyze.ts` to combine them.

## What is measured

A zbugs-like data set: 20k issues, ~100k comments, ~40k issue labels, 200
users, 30 labels. It is deterministic and fully indexed.

| mode     | sources / storage                                         | stands for            |
| -------- | --------------------------------------------------------- | --------------------- |
| `memory` | `MemorySource`, `MemoryStorage`, `ArrayView`              | zero-client           |
| `zqlite` | `TableSource`, file-backed `DatabaseStorage`, `ArrayView` | zero-cache IVM        |
| `driver` | `PipelineDriver.addQuery()` / `advance()` over a replica  | zero-cache end to end |

Queries: `L1` limit, `L2` where + limit, `R1`–`R3` limit + related (`R3` is
the zbugs issue list: owner, labels, comments(10) with author), `R4` related
with no limit, `E1`–`E6` exists + related (non-flipped, flipped, `or(cmp,
exists)`, and a permission-style exists without a limit).

Workloads are forward/inverse transactions timed separately. They include
top-of-window add/remove, edits inside and outside the window, child
add/remove, a user rename that fans out to 89 issues, adding and removing a
label (the issue enters or leaves the exists), bulk removal of the top 10, and
renaming label-00 (every exists row leaves, then returns).

Each result is a median time. From a separate untimed pass it also reports
operator-storage calls and SQLite statements per operation; those counts are
deterministic, so they attribute costs without timing noise.

## Results: 1d9e880 (before #6617) vs 2860c57 (HEAD), 2026-09-23

Three rounds, median of per-round medians, 4 vCPU cloud container, node 22.
Single-run noise is about ±10%, so treat changes under ~15% as noise.

### Summary (geomean change, HEAD vs 1d9e880)

| mode   | group            | hydration | pushes ≥0.1 ms | worst push                        |
| ------ | ---------------- | --------- | -------------- | --------------------------------- |
| memory | simple + limit   | +5%       | —              |                                   |
| memory | related          | **+30%**  | −27%           | +37% R3 bulk remove               |
| memory | related + exists | **+42%**  | **+18%**       | +91% E6 label rename              |
| zqlite | simple + limit   | −2%       | −8%            |                                   |
| zqlite | related          | +12%      | −4%            | +39% R4 user edit                 |
| zqlite | related + exists | **+33%**  | **+19%**       | +92% E2 user edit (+4.6 ms)       |
| driver | simple + limit   | +1%       | +2%            |                                   |
| driver | related          | +9%       | −7%            | +83% R3 issueLabel add (+0.13 ms) |
| driver | related + exists | **+31%**  | +14%           | +91% E2 user edit (+5.6 ms)       |

### Hydration (ms at 1d9e880, change at HEAD)

| query                                  | driver        | zqlite        | memory        | storage writes  |
| -------------------------------------- | ------------- | ------------- | ------------- | --------------- |
| L1 limit(100)                          | 0.88 −1%      | 0.38 −2%      | 0.04 +2%      | 2 → 1           |
| R1 limit(100).related(owner)           | 2.79 +10%     | 1.69 +14%     | 0.22 +34%     | 2 → 101         |
| R2 limit(100).related(comments(10))    | 9.08 −6%      | 5.75 +2%      | 0.96 +10%     | 105 → 201       |
| R3 zbugs list                          | 31.7 **+18%** | 22.7 **+15%** | 3.38 **+38%** | 105 → 1,086     |
| R4 related, no limit (4k rows)         | 113 **+18%**  | 70.9 **+17%** | 15.4 **+40%** | 0 → 4,013       |
| E1 exists limit(100) + related         | 98.7 **+24%** | 74.1 **+30%** | 22.2 **+55%** | 1,643 → 6,860   |
| E2 flipped exists limit(100) + related | 23.4 **+45%** | 16.5 **+40%** | 10.5 **+21%** | 2 → 1,905       |
| E3 exists(comments) limit(50)          | 131 +8%       | 107 +1%       | 29.8 +6%      | 2,360 → 4,713   |
| E4 or(cmp, exists) limit(100)          | 84.3 **+38%** | 64.8 **+33%** | 19.9 **+64%** | 1,530 → 6,049   |
| E5 or(cmp, flipped exists) limit(100)  | 14.7 **+37%** | 9.01 **+62%** | 8.94 **+34%** | 2 → 1,506       |
| E6 exists, no limit (2.6k rows)        | 1063 **+37%** | 769 **+39%**  | 176 **+86%**  | 20,030 → 79,965 |

Client heap retained per hydrated query (memory mode) also grows: E1 0.3 →
1.0 MiB, E4 0.25 → 0.83 MiB, E6 4.1 → 11.0 MiB, R3 91 → 231 KiB, R4 0.7 → 1.2
MiB.

### Pushes that moved the most (driver, ms at 1d9e880, change at HEAD)

| query | workload                               | base (fwd / inv) | HEAD (fwd / inv) | storage ops (fwd)             |
| ----- | -------------------------------------- | ---------------- | ---------------- | ----------------------------- |
| E2    | user edit (fans out to 89 issues)      | 6.1 / 6.2        | +84% / +91%      | 1 → 1,321                     |
| E5    | user edit                              | 7.2 / 7.4        | +54% / +51%      | 1 → 1,320                     |
| E5    | issue add / remove at top              | 9.7 / 18.9       | +47% / +54%      | 3 → 1,316                     |
| E2    | issueLabel add / remove                | 10.0 / 19.0      | +41% / +42%      | 4 → 1,338                     |
| E3    | comment add / remove, parent in window | 2.1 / 2.7        | +29% / +23%      | 110 → 212                     |
| E1    | issueLabel add / remove                | 1.7 / 2.2        | +24% / +25%      | 54 → 114                      |
| E6    | user edit                              | 1.5 / 1.5        | +27% / +29%      | 99 → 199                      |
| E1–E5 | label rename: rows leave               | 0.7–3.7 s        | **−29% to −70%** | fewer (E1, E4), more (E2, E5) |
| E1–E5 | label rename: rows return              | 0.4–1.8 s        | +17% to +32%     | up to 30x more                |
| R1    | user edit (fans out to 89 issues)      | 0.36 / 0.35      | **−78% / −77%**  | 1 → 1                         |
| R3    | user edit (fans out to 89 issues)      | 3.6 / 3.6        | **−73% / −73%**  | 503 → 27                      |
| L1/L2 | every workload                         |                  | within ±9%       |                               |

With the label rename, HEAD also emits far fewer row changes (E2: 990 vs
6,046), because refills no longer churn rows in and out one at a time.

### Attribution by commit (zqlite, deterministic op counts)

| commit                     | effect                                                                                                                                  |
| -------------------------- | --------------------------------------------------------------------------------------------------------------------------------------- |
| #6617 TakeGate             | One `storage.get` per gated fetch; E1 hydration +1.6k gets. Large win for fan-out pushes (R3 user edit −78%).                           |
| #6642 partitions → storage | R3 hydration +685 sets and +685 gets (nested `related` partition tracking). Superseded by #6645.                                        |
| **#6645 JoinIndex**        | **Most of the regression.** One storage write per parent row per Join/FlippedJoin fetch. E6 20k → 80k writes, R4 0 → 4k, E2 296 → 1.9k. |
| #6620 two-phase push       | Mass invalidation 60–70% faster, far fewer output changes; small fixed cost otherwise.                                                  |
| #6636 UnionFanIn witnesses | No measurable cost. E5 hydration returns 390 rows instead of 388 (the fix).                                                             |
| #6660 join overlay         | Replaces a per-parent `getBound()` (one storage get per row) with one read per push, which removes the #6617 get overhead.              |

## Why

1. **`Join.fetch` and `FlippedJoin.fetch` write to the join index for every
   parent they yield.** On the server that is one SQLite `INSERT … ON
CONFLICT` in `DatabaseStorage` per row; on the client it is a
   `compareUTF8` BTree insert. Hydration pays it for every parent, including
   parents that a downstream `Exists` or `Take` rejects. A non-flipped exists
   indexes every scanned parent, and a join with no limit indexes the whole
   result.
2. **Fetches during a push re-write the index.** When a push makes the outer
   flipped join re-fetch its child side, the inner flipped join of the
   junction (`issueLabel` → `label`) re-indexes all ~1,320 junction rows.
   That is 1,320 SQLite writes for a push that used to run 11 SQLite
   statements in total.
3. **A parent `EDIT` does `del` + `set` of the same key.** Sources split edits
   that change a join or partition key, and zero-cache sends an edit only when
   the primary key is unchanged, so the key never changes. That is 2 wasted
   writes per join per parent edit, even for rows outside the window when the
   join sits below the `Take` (every exists).
4. Operator storage of a removed query is not deleted until
   `PipelineDriver.destroy()` (not on `removeQuery()` or `reset()`). That was
   already true for Take/Exists state, but the join index multiplies the row
   count (E6: ~80k rows per hydration instead of ~20k).

## Possible fixes

- Skip the re-index on `EDIT` when the primary and partition keys are
  unchanged. A prototype of this passes the zql join/exists/take tests and
  brings edit pushes back to baseline (e.g. zqlite E4 in-window edit +47% →
  −3%, driver R3 in-window edit +25% → 0%). It does not help hydration.
- Only maintain the index where it can prune: joins below a `Take`
  (`boundProvider`). For joins with no limit (R4, E6) the index holds every
  parent, so it costs a write per row and prunes nothing a constrained
  parent fetch would not.
- Avoid re-writing rows that are already indexed during fetches made inside
  a push (flipped exists over a junction table is the worst case), or keep the
  index for the inner, non-bounded side of a flipped join off entirely.
- Delete a pipeline's operator storage when its query is removed.
