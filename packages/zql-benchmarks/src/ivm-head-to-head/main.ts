/**
 * Zero's half of the Zero-vs-rindle IVM head-to-head.
 *
 * Runs the SAME cells as rindle's `rust/rindle-triangle-bench` over the SAME
 * chinook data at the SAME scales, and prints the SAME
 * `res|contestant|pattern|scaleN|metric|value` lines, so the two outputs can be
 * joined directly. `rindle/tools/ivm-head-to-head.mjs` drives both sides and
 * renders the comparison.
 *
 * Contestants:
 *   zero_mem        MemorySource      + ArrayView      (the browser/client leaf)
 *   zero_mem_sink   MemorySource      + change sink
 *   zero_tab        zqlite TableSource + ArrayView     (the server leaf)
 *   zero_tab_sink   zqlite TableSource + change sink
 *   zero_tab_dbsto  zqlite TableSource + ArrayView, with operator state in
 *                   SQLite (`DatabaseStorage`) — what zero-cache actually runs
 *
 * Metrics per cell: rows, state_bytes, hydrate_ns, push_common_ns,
 * push_worst_ns, stream_ns_per_write.
 *
 * Usage:
 *   node --experimental-transform-types --expose-gc \
 *        packages/zql-benchmarks/src/ivm-head-to-head/main.ts
 *   # one cell (what the driver spawns; one process per cell = clean memory)
 *   node ... main.ts --cell zero_mem exists 1
 *
 * Env: CHINOOK_SQL, TRIANGLE_SCALES=1,10,30, TRIANGLE_W=200,
 *      TRIANGLE_CONTESTANTS, TRIANGLE_PATTERNS.
 */
import {spawnSync} from 'node:child_process';
import {testLogConfig} from '../../../otel/src/test-log-config.ts';
import {createSilentLogContext} from '../../../shared/src/logging-test-utils.ts';
import type {Source} from '../../../zql/src/ivm/source.ts';
import {loadScaled} from './data.ts';
import {
  benchNs,
  blackBox,
  fmtBytes,
  fmtNs,
  haveGc,
  heapBytes,
} from './harness.ts';
import {
  ALL_PATTERNS,
  needs,
  pushCases,
  streamRows,
  WRITE_TABLE,
  type PatternLabel,
} from './patterns.ts';
import {
  assertNoLeak,
  buildSink,
  buildSources,
  buildView,
  pushAdd,
  pushRemove,
  type Leaf,
  type Mode,
  type StorageKind,
  type Sources,
} from './pipeline.ts';

const lc = createSilentLogContext();
const logConfig = testLogConfig;

type ContestantSpec = {
  readonly leaf: Leaf;
  readonly mode: Mode;
  readonly storage: StorageKind;
};

const CONTESTANTS: Record<string, ContestantSpec> = {
  zero_mem: {leaf: 'memory', mode: 'view', storage: 'memory'},
  zero_mem_sink: {leaf: 'memory', mode: 'sink', storage: 'memory'},
  zero_tab: {leaf: 'table', mode: 'view', storage: 'memory'},
  zero_tab_sink: {leaf: 'table', mode: 'sink', storage: 'memory'},
  zero_tab_dbsto: {leaf: 'table', mode: 'view', storage: 'sqlite'},
};

const DEFAULT_CONTESTANTS = [
  'zero_mem',
  'zero_mem_sink',
  'zero_tab',
  'zero_tab_sink',
];

const METRICS = [
  'rows',
  'state_bytes',
  'hydrate_ns',
  'push_common_ns',
  'push_worst_ns',
  'stream_ns_per_write',
] as const;

type Emit = (metric: string, value: number) => void;

// ---------------------------------------------------------------------------
// View mode
// ---------------------------------------------------------------------------

function writeSource(sources: Record<string, Source>): Source {
  return sources[WRITE_TABLE];
}

/** The live view's row count — rindle charges the same `view_data(..).len()`. */
function viewLen(built: {view: {data: unknown}}): number {
  const d = built.view.data;
  return Array.isArray(d) ? d.length : 0;
}

function runView(
  pattern: PatternLabel,
  srcs: Sources,
  data: ReturnType<typeof loadScaled>,
  w: number,
  emit: Emit,
): void {
  // state: heap delta of "register + hydrate, hold live"
  const before = heapBytes();
  let held = buildView(pattern, srcs);
  const state = Math.max(0, heapBytes() - before);
  emit('rows', held.rows);
  emit('state_bytes', state);
  held.destroy();
  held = undefined as unknown as typeof held;
  assertNoLeak(srcs, 'view/state');

  emit(
    'hydrate_ns',
    benchNs(5, () => {
      const b = buildView(pattern, srcs);
      blackBox(b.rows);
      b.destroy();
    }),
  );
  assertNoLeak(srcs, 'view/hydrate');

  for (const [label, row] of pushCases(pattern, data)) {
    const built = buildView(pattern, srcs);
    const src = writeSource(built.sources);
    emit(
      `push_${label}_ns`,
      benchNs(5, () => {
        pushAdd(src, row);
        built.view.flush();
        pushRemove(src, row);
        built.view.flush();
        blackBox(viewLen(built));
      }),
    );
    built.destroy();
    assertNoLeak(srcs, `view/push_${label}`);
  }

  const writes = streamRows(data, w);
  const built = buildView(pattern, srcs);
  const src = writeSource(built.sources);
  const ns = benchNs(3, () => {
    for (const r of writes) {
      pushAdd(src, r);
      built.view.flush();
    }
    for (const r of writes) {
      pushRemove(src, r);
      built.view.flush();
    }
    blackBox(viewLen(built));
  });
  built.destroy();
  assertNoLeak(srcs, 'view/stream');
  emit('stream_ns_per_write', ns / (2 * w));
}

// ---------------------------------------------------------------------------
// Sink mode
// ---------------------------------------------------------------------------

function runSink(
  pattern: PatternLabel,
  srcs: Sources,
  data: ReturnType<typeof loadScaled>,
  w: number,
  emit: Emit,
): void {
  const before = heapBytes();
  let held = buildSink(pattern, srcs);
  const rows = held.sink.hydrate();
  held.sink.take(); // the initial set is the consumer's, not engine state
  const state = Math.max(0, heapBytes() - before);
  emit('rows', rows);
  emit('state_bytes', state);
  held.destroy();
  held = undefined as unknown as typeof held;
  assertNoLeak(srcs, 'sink/state');

  emit(
    'hydrate_ns',
    benchNs(5, () => {
      const b = buildSink(pattern, srcs);
      blackBox(b.sink.hydrate());
      b.destroy();
    }),
  );
  assertNoLeak(srcs, 'sink/hydrate');

  for (const [label, row] of pushCases(pattern, data)) {
    const built = buildSink(pattern, srcs);
    built.sink.hydrate();
    built.sink.take();
    const src = writeSource(built.sources);
    emit(
      `push_${label}_ns`,
      benchNs(5, () => {
        pushAdd(src, row);
        blackBox(built.sink.take());
        pushRemove(src, row);
        blackBox(built.sink.take());
      }),
    );
    built.destroy();
    assertNoLeak(srcs, `sink/push_${label}`);
  }

  const writes = streamRows(data, w);
  const built = buildSink(pattern, srcs);
  built.sink.hydrate();
  built.sink.take();
  const src = writeSource(built.sources);
  const ns = benchNs(3, () => {
    for (const r of writes) {
      pushAdd(src, r);
      blackBox(built.sink.take());
    }
    for (const r of writes) {
      pushRemove(src, r);
      blackBox(built.sink.take());
    }
  });
  built.destroy();
  assertNoLeak(srcs, 'sink/stream');
  emit('stream_ns_per_write', ns / (2 * w));
}

// ---------------------------------------------------------------------------
// Cell
// ---------------------------------------------------------------------------

function runCell(contestant: string, pattern: PatternLabel, scale: number) {
  const spec = CONTESTANTS[contestant];
  if (!spec) {
    throw new Error(`unknown contestant ${contestant}`);
  }
  const w = Number(process.env.TRIANGLE_W ?? 200);
  const emit: Emit = (metric, value) => {
    process.stdout.write(
      `res|${contestant}|${pattern}|scale${scale}|${metric}|${value.toFixed(1)}\n`,
    );
  };

  const data = loadScaled(lc, scale, needs(pattern));
  const srcs = buildSources(lc, logConfig, data, spec.leaf, spec.storage);
  try {
    if (spec.mode === 'view') {
      runView(pattern, srcs, data, w, emit);
    } else {
      runSink(pattern, srcs, data, w, emit);
    }
  } finally {
    srcs.close();
  }
}

// ---------------------------------------------------------------------------
// Driver
// ---------------------------------------------------------------------------

function listFromEnv<T extends string>(
  name: string,
  all: readonly T[],
): readonly T[] {
  const raw = process.env[name];
  if (!raw) {
    return all;
  }
  const keep = new Set(raw.split(',').map(s => s.trim()));
  return all.filter(x => keep.has(x));
}

function scales(): number[] {
  return (process.env.TRIANGLE_SCALES ?? '1,10,30')
    .split(',')
    .map(s => Number(s.trim()));
}

type Key = `${string}|${string}|${number}|${string}`;

function drive() {
  if (!haveGc()) {
    process.stderr.write(
      '# WARNING: run with --expose-gc; state_bytes will be noise without it\n',
    );
  }
  const contestants = listFromEnv('TRIANGLE_CONTESTANTS', DEFAULT_CONTESTANTS);
  const patterns = listFromEnv<PatternLabel>('TRIANGLE_PATTERNS', ALL_PATTERNS);
  const results = new Map<Key, number>();

  for (const scale of scales()) {
    for (const contestant of contestants) {
      for (const pattern of patterns) {
        process.stderr.write(`# cell ${contestant}:${pattern}:scale${scale}\n`);
        const out = spawnSync(
          process.execPath,
          [
            ...process.execArgv,
            process.argv[1],
            '--cell',
            contestant,
            pattern,
            String(scale),
          ],
          {encoding: 'utf8', env: process.env},
        );
        if (out.status !== 0) {
          process.stderr.write(
            `# CELL FAILED ${contestant}:${pattern}:scale${scale}\n${out.stderr}\n`,
          );
          continue;
        }
        for (const line of out.stdout.split('\n')) {
          if (!line.startsWith('res|')) {
            continue;
          }
          process.stdout.write(line + '\n');
          const [, c, p, s, metric, value] = line.split('|');
          results.set(
            `${c}|${p}|${Number(s.slice('scale'.length))}|${metric}`,
            Number(value),
          );
        }
      }
    }
  }

  summarize(results, contestants, patterns);
}

function summarize(
  results: Map<Key, number>,
  contestants: readonly string[],
  patterns: readonly PatternLabel[],
) {
  const out: string[] = [];
  for (const scale of scales()) {
    out.push(`\n### scale ×${scale}\n`);
    out.push(`| metric | pattern | ${contestants.join(' | ')} |`);
    out.push(`|---|---|${contestants.map(() => '--:').join('|')}|`);
    for (const metric of METRICS) {
      for (const pattern of patterns) {
        const cells = contestants.map(c => {
          const v = results.get(`${c}|${pattern}|${scale}|${metric}`);
          if (v === undefined) return '—';
          if (metric === 'rows') return String(Math.round(v));
          if (metric === 'state_bytes') return fmtBytes(v);
          return fmtNs(v);
        });
        if (cells.every(c => c === '—')) {
          continue;
        }
        out.push(`| ${metric} | ${pattern} | ${cells.join(' | ')} |`);
      }
    }
  }
  process.stdout.write(out.join('\n') + '\n');
}

// ---------------------------------------------------------------------------

const argv = process.argv.slice(2);
if (argv[0] === '--cell') {
  runCell(argv[1], argv[2] as PatternLabel, Number(argv[3]));
} else {
  drive();
}
