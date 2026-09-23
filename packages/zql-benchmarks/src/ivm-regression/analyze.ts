/**
 * Summarizes ivm-regression.ts output as markdown tables.
 *
 *   node analyze.ts <dir>... [--base=A] [--labels=A,B,H] [--modes=zqlite]
 *                            [--summary=H]
 *
 * Several directories (rounds) are combined by taking the median of each
 * label's per-round medians. Cells show the change relative to `--base`.
 */

import {readdirSync, readFileSync} from 'node:fs';
import {join} from 'node:path';

type Counts = Record<string, number>;
type Result = {
  label: string;
  mode: string;
  kind: 'hydrate' | 'push' | 'error';
  query: string;
  workload?: string;
  dir?: string;
  median: number;
  rows?: number;
  out?: number;
  counts?: Counts;
  error?: string;
};

const args = process.argv.slice(2);
const dirs = args.filter(a => !a.startsWith('--'));
const opts = Object.fromEntries(
  args
    .filter(a => a.startsWith('--'))
    .map(a => a.slice(2).split('=', 2) as [string, string]),
);
const labelsOpt = opts.labels as string | undefined;
const base: string = opts.base ?? 'A';
const modes = (opts.modes ?? 'memory,zqlite,driver').split(',');

const medians = new Map<string, Map<string, number[]>>();
const counts = new Map<string, Map<string, Counts>>();
const sizes = new Map<string, Map<string, number | undefined>>();
const labelsSeen = new Set<string>();
const keys = new Map<string, Result>();
const errors: Result[] = [];

function getOrCreate<V>(m: Map<string, V>, k: string, init: () => V): V {
  let v = m.get(k);
  if (v === undefined) {
    v = init();
    m.set(k, v);
  }
  return v;
}

for (const d of dirs) {
  for (const f of readdirSync(d).filter(f => f.endsWith('.jsonl'))) {
    for (const line of readFileSync(join(d, f), 'utf8').split('\n')) {
      if (!line) {
        continue;
      }
      const r = JSON.parse(line) as Result;
      if (r.kind === 'error') {
        errors.push(r);
        continue;
      }
      const key = [r.mode, r.kind, r.query, r.workload ?? '', r.dir ?? ''].join(
        '|',
      );
      keys.set(key, r);
      labelsSeen.add(r.label);
      getOrCreate(
        getOrCreate(medians, key, () => new Map()),
        r.label,
        () => [],
      ).push(r.median);
      if (r.counts) {
        getOrCreate(counts, key, () => new Map()).set(r.label, r.counts);
      }
      getOrCreate(sizes, key, () => new Map()).set(r.label, r.rows ?? r.out);
    }
  }
}

const labels = labelsOpt ? labelsOpt.split(',') : [...labelsSeen].toSorted();

/** The median over rounds of each round's median, so one noisy round does not skew it. */
function value(key: string, label: string): number | undefined {
  const v = medians
    .get(key)
    ?.get(label)
    ?.toSorted((a, b) => a - b);
  if (!v?.length) {
    return undefined;
  }
  const mid = v.length >> 1;
  return v.length % 2 ? v[mid] : (v[mid - 1] + v[mid]) / 2;
}

function fmtMs(v: number | undefined): string {
  if (v === undefined) {
    return '—';
  }
  if (v >= 100) {
    return v.toFixed(0);
  }
  if (v >= 10) {
    return v.toFixed(1);
  }
  if (v >= 1) {
    return v.toFixed(2);
  }
  return `${(v * 1000).toFixed(0)}µs`;
}

function fmtChange(r: number | undefined): string {
  if (r === undefined) {
    return '—';
  }
  const pct = (r - 1) * 100;
  const s = `${pct >= 0 ? '+' : ''}${pct.toFixed(0)}%`;
  return pct >= 15 ? `**${s}**` : s;
}

function storageOps(c: Counts | undefined): number {
  return c ? c.sget + c.sset + c.sdel + c.sscan : 0;
}

const out: string[] = [];
const others = labels.filter(l => l !== base);
for (const mode of modes) {
  for (const kind of ['hydrate', 'push'] as const) {
    const ks = [...keys.keys()]
      .filter(k => {
        const r = keys.get(k)!;
        return r.mode === mode && r.kind === kind;
      })
      .sort();
    if (!ks.length) {
      continue;
    }
    out.push(`\n### ${mode}: ${kind}\n`);
    const hdr = [
      'query',
      ...(kind === 'push' ? ['workload', 'dir'] : []),
      `${base} (ms)`,
      ...others,
      `storage ops ${base}→${others.at(-1)}`,
      `sqlite stmts ${base}→${others.at(-1)}`,
    ];
    out.push(`| ${hdr.join(' | ')} |`, `|${'---|'.repeat(hdr.length)}`);
    for (const k of ks) {
      const r = keys.get(k)!;
      const b = value(k, base);
      const cb = counts.get(k)?.get(base);
      const cl = counts.get(k)?.get(others.at(-1) ?? base);
      const cells = [
        r.query,
        ...(kind === 'push' ? [r.workload ?? '', r.dir ?? ''] : []),
        fmtMs(b),
        ...others.map(l => {
          const v = value(k, l);
          return fmtChange(v !== undefined && b ? v / b : undefined);
        }),
        cb && cl
          ? `${storageOps(cb).toFixed(0)}→${storageOps(cl).toFixed(0)}`
          : '—',
        cb && cl ? `${cb.stmt.toFixed(0)}→${cl.stmt.toFixed(0)}` : '—',
      ];
      out.push(`| ${cells.join(' | ')} |`);
    }
  }
}

if (opts.summary) {
  const target: string = opts.summary;
  const geomean = (xs: number[]) =>
    xs.length
      ? Math.exp(xs.reduce((a, x) => a + Math.log(x), 0) / xs.length)
      : undefined;
  out.push(`\n## Summary: ${target} vs ${base} (geomean of median ratios)`);
  for (const mode of modes) {
    out.push(
      `\n### ${mode}\n`,
      '| group | hydration | pushes ≥100µs (n) | pushes <100µs (n) | worst push ≥100µs |',
      '|---|---|---|---|---|',
    );
    for (const [g, name] of [
      ['L', 'simple + limit'],
      ['R', 'related'],
      ['E', 'related + exists'],
    ]) {
      const hyd: number[] = [];
      const big: number[] = [];
      const small: number[] = [];
      let worst: [number, string] = [0, ''];
      for (const [k, r] of keys) {
        if (r.mode !== mode || !r.query.startsWith(g)) {
          continue;
        }
        const b = value(k, base);
        const t = value(k, target);
        if (b === undefined || t === undefined) {
          continue;
        }
        const ratio = t / b;
        if (r.kind === 'hydrate') {
          hyd.push(ratio);
        } else if (b >= 0.1) {
          big.push(ratio);
          if (ratio > worst[0]) {
            worst = [ratio, `${r.query.slice(0, 2)} ${r.workload} (${r.dir})`];
          }
        } else {
          small.push(ratio);
        }
      }
      out.push(
        `| ${name} | ${fmtChange(geomean(hyd))} | ${fmtChange(geomean(big))} (${big.length}) | ${fmtChange(geomean(small))} (${small.length}) | ${worst[1] ? `${fmtChange(worst[0])} ${worst[1]}` : '—'} |`,
      );
    }
  }
}

const mismatches: string[] = [];
for (const [k, byLabel] of sizes) {
  if (new Set(byLabel.values()).size > 1) {
    mismatches.push(`  ${k}: ${JSON.stringify(Object.fromEntries(byLabel))}`);
  }
}
if (mismatches.length) {
  out.push(
    '\nResult sizes (hydration rows / pushed output changes) that differ by commit:',
    ...mismatches,
  );
}
for (const e of errors) {
  out.push(
    `ERROR ${e.label} ${e.mode} ${e.query} ${e.workload ?? ''}: ${e.error}`,
  );
}

process.stdout.write(out.join('\n') + '\n');
