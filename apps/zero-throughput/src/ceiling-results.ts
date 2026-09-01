import {execFileSync} from 'node:child_process';
import {mkdir, writeFile} from 'node:fs/promises';
import {cpus} from 'node:os';
import {dirname} from 'node:path';
import type {BulkWriterStats} from './bulk-writer.ts';
import {appPath, appRoot, type BenchmarkConfig} from './config.ts';
import type {MetricSummary, OTelMetricsCollector} from './metrics.ts';
import type {ProcessCommand} from './processes.ts';
import {percentile} from './util.ts';

export type CeilingSample = {
  readonly atMs: number;
  readonly elapsedMs: number;
  readonly committedRows: number;
  readonly committedSeq: number;
  readonly appliedSeq: number;
  readonly appliedWrittenAtMs: number;
  readonly appliedStateVersion: string;
  readonly probeMs: number;
  readonly cpuSeconds: Readonly<Record<string, number>>;
  readonly workerCpuSeconds: Readonly<Record<string, number>>;
  readonly rssMiB: Readonly<Record<string, number>>;
  readonly writeBytes: Readonly<Record<string, number>>;
  readonly diskBusyMs: number;
  readonly retainedWalBytes?: number | undefined;
  readonly archiveBytes?: number | undefined;
  readonly archiveSegments?: number | undefined;
};

export type CeilingResult = {
  readonly gitCommit: string | undefined;
  readonly config: BenchmarkConfig;
  readonly processes: readonly Omit<ProcessCommand, 'cwd'>[];
  readonly environment: {
    readonly node: string;
    readonly platform: string;
    readonly arch: string;
    readonly cpus: number;
  };
  readonly samples: readonly CeilingSample[];
  readonly metrics: MetricSummary;
  readonly summary: {
    readonly backupMode: string;
    readonly rowsPerTx: number;
    readonly targetWriteRate: number;
    readonly committedRows: number;
    readonly committedTransactions: number;
    readonly writeErrors: number;
    readonly commitRowsPerSec: number;
    readonly commitTxPerSec: number;
    readonly commitMiBPerSec: number;
    readonly applyRowsPerSec: number;
    readonly lagP50Ms: number;
    readonly lagP95Ms: number;
    readonly lagMaxMs: number;
    readonly backlogP50: number;
    readonly backlogMax: number;
    readonly backlogSlopeRowsPerSec: number;
    readonly drainMs: number | undefined;
    readonly drainRowsPerSec: number;
    readonly txLatencyP50Ms: number;
    readonly txLatencyP95Ms: number;
    readonly retainedWalP50MiB: number | undefined;
    readonly retainedWalMaxMiB: number | undefined;
    readonly archiveBytesPerSec: number | undefined;
    readonly archiveSegments: number | undefined;
    readonly cpuCores: Readonly<Record<string, number>>;
    readonly workerCpuCores: Readonly<Record<string, number>>;
    readonly writeMiBPerSec: Readonly<Record<string, number>>;
    /** Device busy time per second of wall clock: the classic %util, as 0-1. */
    readonly diskUtil: number;
    readonly rssPeakMiB: Readonly<Record<string, number>>;
    readonly probeP95Ms: number;
    readonly sustained: boolean;
  };
};

/**
 * The steady-state window is everything after the warmup and before the
 * writers stop. Rates are least-squares slopes over that window rather than
 * endpoint differences, so one slow sample (a checkpoint, a segment upload)
 * does not set the headline number.
 */
export function summarize(args: {
  config: BenchmarkConfig;
  writerStats: BulkWriterStats;
  samples: readonly CeilingSample[];
  startedAtMs: number;
  writesStoppedAtMs: number;
  drainedAtMs: number | undefined;
  metrics: OTelMetricsCollector;
  processes: readonly ProcessCommand[];
}): CeilingResult {
  const {config, writerStats, samples, writesStoppedAtMs, drainedAtMs} = args;
  const windowStart = writerStats.startedAtMs + config.warmupMs;
  const steady = samples.filter(
    s => s.atMs >= windowStart && s.atMs <= writesStoppedAtMs,
  );
  const window = steady.length >= 2 ? steady : samples;

  const commitRowsPerSec = slope(window, s => s.committedRows);
  const applyRowsPerSec = slope(window, s => s.appliedSeq);
  const rowsPerTx = config.rowsPerStatement * config.statementsPerTx;
  const backlogs = window.map(s => s.committedSeq - s.appliedSeq);
  // Lag is only meaningful once rows have been applied; a sample taken
  // before the first apply has no newest-row timestamp to subtract.
  const lags = window
    .filter(s => s.appliedWrittenAtMs > 0)
    .map(s => s.atMs - s.appliedWrittenAtMs);

  const cpuCores = ratesPerRole(window, s => s.cpuSeconds);
  const workerCpuCores = ratesPerRole(window, s => s.workerCpuSeconds);
  const writeBytesPerSec = ratesPerRole(window, s => s.writeBytes);
  const writeMiBPerSec: Record<string, number> = {};
  for (const [role, bytes] of Object.entries(writeBytesPerSec)) {
    writeMiBPerSec[role] = bytes / (1024 * 1024);
  }
  const rssPeakMiB: Record<string, number> = {};
  for (const s of window) {
    for (const [role, mib] of Object.entries(s.rssMiB)) {
      rssPeakMiB[role] = Math.max(rssPeakMiB[role] ?? 0, mib);
    }
  }

  const retainedWal = window
    .map(s => s.retainedWalBytes)
    .filter((b): b is number => b !== undefined)
    .map(b => b / (1024 * 1024));
  const archiveBytesPerSec =
    config.backupMode === 'archive'
      ? slope(
          window.filter(s => s.archiveBytes !== undefined),
          s => s.archiveBytes ?? 0,
        )
      : undefined;

  const drainMs =
    drainedAtMs === undefined ? undefined : drainedAtMs - writesStoppedAtMs;
  const lastWriting = samples.findLast(s => s.atMs <= writesStoppedAtMs);
  const backlogAtStop =
    lastWriting === undefined
      ? 0
      : lastWriting.committedSeq - lastWriting.appliedSeq;

  // Sustained means the replica kept up: the backlog is not trending up and
  // the applier matched the commit rate to within 2%.
  const backlogSlopeRowsPerSec = slope(
    window,
    s => s.committedSeq - s.appliedSeq,
  );
  const sustained =
    backlogSlopeRowsPerSec < Math.max(commitRowsPerSec * 0.02, 5) &&
    applyRowsPerSec >= commitRowsPerSec * 0.98 &&
    drainedAtMs !== undefined;

  return {
    gitCommit: gitCommit(),
    config,
    processes: args.processes.map(({name, command, logPath}) => ({
      name,
      command,
      logPath,
    })),
    environment: {
      node: process.version,
      platform: process.platform,
      arch: process.arch,
      cpus: cpus().length,
    },
    samples,
    metrics: args.metrics.getSummary(),
    summary: {
      backupMode: config.backupMode,
      rowsPerTx,
      targetWriteRate: config.writeRate,
      committedRows: writerStats.committedRows,
      committedTransactions: writerStats.committedTransactions,
      writeErrors: writerStats.errors,
      commitRowsPerSec,
      commitTxPerSec: commitRowsPerSec / rowsPerTx,
      commitMiBPerSec:
        (commitRowsPerSec * writerStats.approxRowBytes) / (1024 * 1024),
      applyRowsPerSec,
      lagP50Ms: percentile(lags, 50),
      lagP95Ms: percentile(lags, 95),
      lagMaxMs: lags.length === 0 ? 0 : Math.max(...lags),
      backlogP50: percentile(backlogs, 50),
      backlogMax: backlogs.length === 0 ? 0 : Math.max(...backlogs),
      backlogSlopeRowsPerSec,
      drainMs,
      drainRowsPerSec:
        drainMs === undefined || drainMs <= 0
          ? 0
          : backlogAtStop / (drainMs / 1000),
      txLatencyP50Ms: percentile(writerStats.txLatencyMs, 50),
      txLatencyP95Ms: percentile(writerStats.txLatencyMs, 95),
      retainedWalP50MiB:
        retainedWal.length === 0 ? undefined : percentile(retainedWal, 50),
      retainedWalMaxMiB:
        retainedWal.length === 0 ? undefined : Math.max(...retainedWal),
      archiveBytesPerSec,
      archiveSegments: window.at(-1)?.archiveSegments,
      cpuCores,
      workerCpuCores,
      writeMiBPerSec,
      diskUtil: slope(window, s => s.diskBusyMs) / 1000,
      rssPeakMiB,
      probeP95Ms: percentile(
        window.map(s => s.probeMs),
        95,
      ),
      sustained,
    },
  };
}

/** Least-squares slope of `value` against time, in units per second. */
function slope(
  samples: readonly CeilingSample[],
  value: (s: CeilingSample) => number,
): number {
  if (samples.length < 2) {
    return 0;
  }
  const t0 = samples[0].atMs;
  let sumX = 0;
  let sumY = 0;
  let sumXY = 0;
  let sumXX = 0;
  for (const s of samples) {
    const x = (s.atMs - t0) / 1000;
    const y = value(s);
    sumX += x;
    sumY += y;
    sumXY += x * y;
    sumXX += x * x;
  }
  const n = samples.length;
  const denominator = n * sumXX - sumX * sumX;
  if (denominator === 0) {
    return 0;
  }
  return (n * sumXY - sumX * sumY) / denominator;
}

function ratesPerRole(
  samples: readonly CeilingSample[],
  value: (s: CeilingSample) => Readonly<Record<string, number>>,
): Record<string, number> {
  const roles = new Set<string>();
  for (const s of samples) {
    for (const role of Object.keys(value(s))) {
      roles.add(role);
    }
  }
  const out: Record<string, number> = {};
  for (const role of roles) {
    out[role] = slope(samples, s => value(s)[role] ?? 0);
  }
  return out;
}

export async function writeCeilingResult(
  config: BenchmarkConfig,
  result: CeilingResult,
): Promise<string> {
  const path = appPath(
    config.outputPath === 'results/latest.json'
      ? `results/ceiling-${config.backupMode}-${config.runID}.json`
      : config.outputPath,
  );
  await mkdir(dirname(path), {recursive: true});
  await writeFile(path, `${JSON.stringify(result, null, 2)}\n`, 'utf-8');
  return path;
}

function gitCommit(): string | undefined {
  try {
    return execFileSync('git', ['rev-parse', 'HEAD'], {
      cwd: appRoot,
      encoding: 'utf-8',
    }).trim();
  } catch {
    return undefined;
  }
}
