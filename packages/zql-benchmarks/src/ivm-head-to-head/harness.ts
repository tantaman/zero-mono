/**
 * Timing + memory harness for the Zero-vs-rindle IVM head-to-head.
 *
 * `benchNs` is the SAME estimator as rindle's `bench_ns`
 * (`rust/rindle-triangle-bench/src/main.rs`): ~150 ms warmup, then for each
 * round double the iteration count until the batch takes >= 60 ms and keep the
 * minimum ns/op across rounds. Min is the most noise-robust estimator for
 * microbenchmarks — it reports the run the scheduler left alone.
 */

/** Keeps the optimizer from eliminating the measured work. */
let sink: unknown;
export function blackBox<T>(v: T): T {
  sink = v;
  return v;
}
export function sinkValue(): unknown {
  return sink;
}

const WARMUP_MS = 150;
const MIN_BATCH_NS = 60_000_000;

export function benchNs(rounds: number, body: () => void): number {
  const warmEnd = process.hrtime.bigint() + BigInt(WARMUP_MS) * 1_000_000n;
  while (process.hrtime.bigint() < warmEnd) {
    body();
  }
  let best = Infinity;
  for (let round = 0; round < rounds; round++) {
    let iters = 1;
    for (;;) {
      const start = process.hrtime.bigint();
      for (let j = 0; j < iters; j++) {
        body();
      }
      const dt = Number(process.hrtime.bigint() - start);
      if (dt >= MIN_BATCH_NS) {
        best = Math.min(best, dt / iters);
        break;
      }
      iters *= 2;
    }
  }
  return best;
}

/**
 * Live JS heap in bytes, after a full GC.
 *
 * NOT the same instrument as the rindle side, and the difference is reported
 * rather than papered over: rindle counts bytes handed out by a counting global
 * allocator (exact, no slack), while V8 can only be asked for `heapUsed` after
 * a forced major GC — which still includes allocation slack and any objects the
 * collector chose not to reclaim. Treat `state_bytes` as same-order, not
 * same-precision. Both sides exclude SQLite's C-side page cache, so the
 * table-source comparison is at least symmetric in what it leaves out.
 *
 * Requires `--expose-gc`; `runCell` refuses to report state without it.
 */
export function heapBytes(): number {
  const gc = (globalThis as {gc?: () => void}).gc;
  if (gc) {
    // Two passes: the first can resurrect via finalizers / weak refs.
    gc();
    gc();
  }
  return process.memoryUsage().heapUsed;
}

export function haveGc(): boolean {
  return typeof (globalThis as {gc?: () => void}).gc === 'function';
}

export function fmtNs(ns: number): string {
  if (ns >= 1e9) return `${(ns / 1e9).toFixed(2)} s`;
  if (ns >= 1e6) return `${(ns / 1e6).toFixed(2)} ms`;
  if (ns >= 1e3) return `${(ns / 1e3).toFixed(1)} µs`;
  return `${ns.toFixed(0)} ns`;
}

export function fmtBytes(b: number): string {
  if (b >= 1e9) return `${(b / 1e9).toFixed(2)} GB`;
  if (b >= 1e6) return `${(b / 1e6).toFixed(2)} MB`;
  if (b >= 1e3) return `${(b / 1e3).toFixed(1)} KB`;
  return `${b.toFixed(0)} B`;
}
