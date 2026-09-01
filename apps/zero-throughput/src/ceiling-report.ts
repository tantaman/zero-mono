/**
 * Prints a comparison table over replication-ceiling result files.
 *
 *   pnpm --filter zero-throughput run ceiling:report -- results/ceiling
 *
 * One run answers "did this rate hold"; the ceiling is the shape of the
 * ladder, so the table is the actual deliverable of a sweep.
 */
import {readdirSync, readFileSync} from 'node:fs';
import {join} from 'node:path';
import type {CeilingResult} from './ceiling-results.ts';
import {appPath} from './config.ts';
import {log} from './util.ts';

const dir = appPath(process.argv[2] ?? 'results/ceiling');
const rows: {file: string; result: CeilingResult}[] = [];
for (const file of readdirSync(dir)) {
  if (!file.endsWith('.json')) {
    continue;
  }
  rows.push({
    file,
    result: JSON.parse(readFileSync(join(dir, file), 'utf-8')) as CeilingResult,
  });
}

rows.sort((a, b) => {
  const mode = a.result.summary.backupMode.localeCompare(
    b.result.summary.backupMode,
  );
  return mode !== 0
    ? mode
    : a.result.summary.targetWriteRate - b.result.summary.targetWriteRate;
});

const header = [
  'mode',
  'rows/tx',
  'target',
  'commit rows/s',
  'apply rows/s',
  'MiB/s in',
  'archive MiB/s',
  'lag p50',
  'lag p95',
  'backlog slope',
  'WAL max MiB',
  'cs cpu',
  'prod cpu',
  'repl cpu',
  'held',
];
const table = rows.map(({result}) => {
  const s = result.summary;
  // Results written before per-worker attribution existed still summarize.
  const w = s.workerCpuCores ?? {};
  return [
    s.backupMode,
    String(s.rowsPerTx),
    s.targetWriteRate === 0 ? 'max' : String(s.targetWriteRate),
    s.commitRowsPerSec.toFixed(0),
    s.applyRowsPerSec.toFixed(0),
    s.commitMiBPerSec.toFixed(2),
    s.archiveBytesPerSec === undefined
      ? '-'
      : (s.archiveBytesPerSec / (1024 * 1024)).toFixed(2),
    `${s.lagP50Ms.toFixed(0)}ms`,
    `${s.lagP95Ms.toFixed(0)}ms`,
    `${s.backlogSlopeRowsPerSec.toFixed(0)}/s`,
    s.retainedWalMaxMiB?.toFixed(0) ?? '-',
    (w['rm/change-streamer'] ?? 0).toFixed(2),
    (w['rm/base-producer'] ?? 0).toFixed(2),
    (w['vs-0/replicator'] ?? 0).toFixed(2),
    s.sustained ? 'yes' : 'NO',
  ];
});

printTable(header, table);

// The rate a given box sustains is a property of the box. CPU per applied
// row is not: it is what says which worker runs out of core first, and what
// a bigger box would buy.
log('');
log('CPU cost per applied row (ms of CPU, i.e. cores per 1000 rows/s):');
const costHeader = [
  'mode',
  'rows/tx',
  'target',
  'apply rows/s',
  'rm/change-streamer',
  'rm/base-producer',
  'vs-0/replicator',
  'postgres',
  'harness',
];
const costTable = rows.map(({result}) => {
  const s = result.summary;
  const perKRow = (cores: number | undefined) =>
    s.applyRowsPerSec > 0
      ? (((cores ?? 0) * 1000) / s.applyRowsPerSec).toFixed(2)
      : '-';
  return [
    s.backupMode,
    String(s.rowsPerTx),
    s.targetWriteRate === 0 ? 'max' : String(s.targetWriteRate),
    s.applyRowsPerSec.toFixed(0),
    perKRow((s.workerCpuCores ?? {})['rm/change-streamer']),
    perKRow((s.workerCpuCores ?? {})['rm/base-producer']),
    perKRow((s.workerCpuCores ?? {})['vs-0/replicator']),
    perKRow(s.cpuCores.postgres),
    perKRow(s.cpuCores.harness),
  ];
});
printTable(costHeader, costTable);

function printTable(head: readonly string[], body: readonly string[][]): void {
  const widths = head.map((h, i) =>
    Math.max(h.length, ...body.map(r => r[i].length)),
  );
  const line = (cells: readonly string[]) =>
    `| ${cells.map((c, i) => c.padEnd(widths[i])).join(' | ')} |`;
  log(line(head));
  log(`|${widths.map(w => '-'.repeat(w + 2)).join('|')}|`);
  for (const row of body) {
    log(line(row));
  }
}
