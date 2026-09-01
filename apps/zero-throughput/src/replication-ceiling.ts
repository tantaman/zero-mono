/**
 * Replication-ceiling runner: end-to-end throughput from Postgres to a
 * view-syncer that is running no pipelines.
 *
 * The regular benchmark (`main.ts`) measures what a client sees: writes go
 * in, synthetic clients hold live queries, and the number that comes out is
 * client-visible lag, which folds the IVM pipelines and the poke path into
 * every measurement. This runner deliberately removes that half. No client
 * connects, so the view-syncer runs no pipelines and the only thing left
 * moving is the replication path itself:
 *
 *   Postgres WAL -> replication-manager (change source, change log,
 *   and in backup mode `archive` the archive writer) -> WebSocket ->
 *   view-syncer replicator -> SQLite replica
 *
 * What it reports is that path's ceiling: the rate at which the replica can
 * be advanced, the end-to-end latency of the newest applied row at that
 * rate, whether the backlog is flat (sustained) or growing, and where the
 * CPU went -- plus, in `archive` mode, the archive's own signals (segment
 * bytes, and the retained WAL that ACK gating on the durable archive cursor
 * produces).
 *
 *   pnpm --filter zero-throughput run ceiling -- --backup-mode archive
 */
import {statSync} from 'node:fs';
import {readdir, rm} from 'node:fs/promises';
import {join} from 'node:path';
import {BulkWriter, CEILING_TABLE} from './bulk-writer.ts';
import {
  writeCeilingResult,
  summarize,
  type CeilingSample,
} from './ceiling-results.ts';
import {appPath, loadConfig, type BenchmarkConfig} from './config.ts';
import {
  connectBenchmarkDB,
  resetBenchmarkDatabase,
  waitForPostgres,
  type BenchmarkDB,
} from './db.ts';
import {OTelMetricsCollector} from './metrics.ts';
import {ProcSampler, type ProcSample} from './proc-sampler.ts';
import {
  archiveDirPath,
  removeReplicaFiles,
  startZeroTopology,
  waitForZeroCache,
  type ManagedProcess,
} from './processes.ts';
import {ReplicaProbe, type ReplicaSample} from './replica-probe.ts';
import {formatDuration, log, sleep, warn} from './util.ts';

async function main(): Promise<void> {
  const config = loadConfig();
  const cleanup: (() => Promise<void>)[] = [];
  const runCleanup = async () => {
    for (const fn of cleanup.splice(0).reverse()) {
      try {
        await fn();
      } catch (e) {
        warn(`cleanup failed: ${String(e)}`);
      }
    }
  };
  const onSigint = () => {
    warn('Interrupted. Cleaning up...');
    void runCleanup().finally(() => process.exit(130));
  };
  process.once('SIGINT', onSigint);

  try {
    log(
      `replication ceiling: backup-mode=${config.backupMode} ` +
        `topology=${config.topology} run ${config.runID}`,
    );

    const metricsCollector = new OTelMetricsCollector();
    await metricsCollector.start();
    cleanup.push(() => metricsCollector.stop());

    log('Waiting for PostgreSQL...');
    await waitForPostgres(config.pg.url, config.pg.readyTimeoutMs);
    const sql = connectBenchmarkDB(
      config.pg.url,
      Math.max(20, config.writeConcurrency * 2),
    );
    cleanup.push(() => sql.end());

    if (config.reset) {
      log('Resetting benchmark database, replicas and archive...');
      await resetBenchmarkDatabase(sql, config);
      await removeReplicaFiles(config.zero.replicaFile);
      await clearArchive(config);
    }

    log(
      `Starting topology (${config.topology}, backup-mode ${config.backupMode})...`,
    );
    const topology = await startZeroTopology(config, metricsCollector.endpoint);
    cleanup.push(() => topology.stop());
    for (const p of topology.processes) {
      if (p.logPath !== undefined) {
        log(`${p.name} logs: ${p.logPath}`);
      }
    }
    for (const url of topology.readyURLs) {
      await waitForZeroCache(url, config.zero.readyTimeoutMs, undefined);
    }
    log(
      'Topology ready. No clients are started: the view-syncer runs no pipelines.',
    );

    const replicaFile =
      config.topology === 'distributed'
        ? `${config.zero.replicaFile}-vs0`
        : config.zero.replicaFile;
    const probe = new ReplicaProbe(replicaFile, CEILING_TABLE);
    cleanup.push(() => {
      probe.close();
      return Promise.resolve();
    });

    const procSampler = new ProcSampler([
      ...topology.processes
        .filter((p): p is ManagedProcess => p.child.pid !== undefined)
        .map(p => ({name: p.name, pid: p.child.pid as number})),
      // The load generator competes for the same cores as everything it is
      // measuring, so it has to be in the accounting: a run where the
      // harness is the biggest consumer is a run about the harness.
      {name: 'harness', pid: process.pid},
    ]);

    const slotName = await findSlot(sql, config);
    log(`Replication slot: ${slotName ?? '(none found)'}`);

    const samples: CeilingSample[] = [];
    const writer = new BulkWriter(sql, config);
    const startedAtMs = Date.now();
    let slotState: SlotState | undefined;
    let archiveState: ArchiveState | undefined;
    let lastProc: ProcSample | undefined;
    let lastReplica: ReplicaSample | undefined;

    // The slot and archive queries are an order of magnitude more expensive
    // than the replica probe, so they run on their own slower beat and each
    // sample carries the most recent value of each.
    const slowTimer = setInterval(() => {
      void (async () => {
        slotState = slotName ? await readSlot(sql, slotName) : undefined;
        archiveState = await readArchive(config);
      })();
    }, 2000);

    const sampler = setInterval(() => {
      const replica = (lastReplica = probe.sample());
      lastProc = procSampler.sample();
      samples.push({
        atMs: Date.now(),
        elapsedMs: Date.now() - startedAtMs,
        committedRows: writer.committedRows,
        committedSeq: writer.highestCommittedSeq,
        appliedSeq: replica.maxSeq,
        appliedWrittenAtMs: replica.writtenAtMs,
        appliedStateVersion: replica.stateVersion,
        probeMs: replica.probeMs,
        cpuSeconds: lastProc.cpuSeconds,
        workerCpuSeconds: lastProc.workerCpuSeconds,
        rssMiB: lastProc.rssMiB,
        retainedWalBytes: slotState?.retainedBytes,
        archiveBytes: archiveState?.bytes,
        archiveSegments: archiveState?.segments,
      });
    }, config.sampleIntervalMs);

    log(
      `Writing for ${formatDuration(config.durationMs)}: ` +
        `${config.writeRate === 0 ? 'unthrottled' : `${config.writeRate} rows/s target`}, ` +
        `${config.rowsPerStatement} rows/statement x ${config.statementsPerTx} statements/tx, ` +
        `concurrency ${config.writeConcurrency}, payload ${config.payloadBytes}B`,
    );
    const progress = setInterval(
      () => {
        const s = samples.at(-1);
        if (s) {
          log(
            `  t=${formatDuration(s.elapsedMs)} committed=${s.committedSeq} ` +
              `applied=${s.appliedSeq} backlog=${s.committedSeq - s.appliedSeq}` +
              (s.retainedWalBytes === undefined
                ? ''
                : ` retainedWAL=${(s.retainedWalBytes / (1024 * 1024)).toFixed(1)}MiB`),
          );
        }
      },
      Math.max(config.progressIntervalMs, 1000),
    );

    const writerStats = await writer.run(config.durationMs);
    const writesStoppedAtMs = Date.now();
    clearInterval(progress);

    log(
      `Writes done: ${writerStats.committedRows} rows in ` +
        `${writerStats.committedTransactions} transactions. Draining ` +
        `(waiting up to ${formatDuration(config.drainMs)} for the replica to reach seq ` +
        `${writerStats.highestCommittedSeq})...`,
    );
    const drainDeadline = Date.now() + config.drainMs;
    let drainedAtMs: number | undefined;
    while (Date.now() < drainDeadline) {
      if ((lastReplica?.maxSeq ?? 0) >= writerStats.highestCommittedSeq) {
        drainedAtMs = Date.now();
        break;
      }
      await sleep(100);
    }
    // One last sample after the drain, so the tail is in the series.
    await sleep(config.sampleIntervalMs);
    clearInterval(sampler);
    clearInterval(slowTimer);

    if (drainedAtMs === undefined) {
      warn(
        `The replica did not catch up within ${formatDuration(config.drainMs)}: ` +
          `applied ${lastReplica?.maxSeq ?? 0} of ${writerStats.highestCommittedSeq}.`,
      );
    }

    const result = summarize({
      config,
      writerStats,
      samples,
      startedAtMs,
      writesStoppedAtMs,
      drainedAtMs,
      metrics: metricsCollector,
      processes: topology.processes,
    });
    const outputPath = await writeCeilingResult(config, result);
    await runCleanup();
    printSummary(result, outputPath);
  } catch (error) {
    await runCleanup();
    throw error;
  } finally {
    process.off('SIGINT', onSigint);
  }
}

type SlotState = {readonly retainedBytes: number};

async function findSlot(
  sql: BenchmarkDB,
  config: BenchmarkConfig,
): Promise<string | undefined> {
  const rows = await sql<{slotName: string}[]>`
    SELECT slot_name AS "slotName" FROM pg_replication_slots
      WHERE slot_name LIKE ${`${config.zero.appID}%`}`;
  return rows[0]?.slotName;
}

async function readSlot(
  sql: BenchmarkDB,
  slotName: string,
): Promise<SlotState | undefined> {
  try {
    const rows = await sql<{retainedBytes: string | null}[]>`
      SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)::text
               AS "retainedBytes"
        FROM pg_replication_slots WHERE slot_name = ${slotName}`;
    const retained = rows[0]?.retainedBytes;
    return retained === null || retained === undefined
      ? undefined
      : {retainedBytes: Number(retained)};
  } catch {
    return undefined;
  }
}

type ArchiveState = {readonly bytes: number; readonly segments: number};

async function readArchive(
  config: BenchmarkConfig,
): Promise<ArchiveState | undefined> {
  if (config.backupMode !== 'archive') {
    return undefined;
  }
  try {
    return await walkArchive(archiveDirPath(config));
  } catch {
    return undefined;
  }
}

async function walkArchive(dir: string): Promise<ArchiveState> {
  let bytes = 0;
  let segments = 0;
  const entries = await readdir(dir, {withFileTypes: true, recursive: true});
  for (const entry of entries) {
    if (!entry.isFile()) {
      continue;
    }
    const path = join(entry.parentPath, entry.name);
    try {
      bytes += statSync(path).size;
    } catch {
      continue; // collected between listing and stat
    }
    if (entry.parentPath.includes('/log')) {
      segments++;
    }
  }
  return {bytes, segments};
}

async function clearArchive(config: BenchmarkConfig): Promise<void> {
  if (config.backupMode !== 'archive') {
    return;
  }
  await rm(appPath(config.archiveDir), {recursive: true, force: true});
}

function printSummary(
  result: ReturnType<typeof summarize>,
  outputPath: string,
): void {
  const s = result.summary;
  log('');
  log(`Backup mode:            ${result.config.backupMode}`);
  log(
    `Write shape:            ${s.rowsPerTx} rows/tx, ${result.config.payloadBytes}B payload`,
  );
  log(
    `Committed:              ${s.committedRows} rows, ${s.committedTransactions} tx`,
  );
  log(
    `Commit rate (steady):   ${s.commitRowsPerSec.toFixed(0)} rows/s, ` +
      `${s.commitTxPerSec.toFixed(1)} tx/s, ${s.commitMiBPerSec.toFixed(2)} MiB/s`,
  );
  log(
    `Applied rate (steady):  ${s.applyRowsPerSec.toFixed(0)} rows/s ` +
      `(${((s.applyRowsPerSec / Math.max(s.commitRowsPerSec, 1)) * 100).toFixed(1)}% of commits)`,
  );
  log(
    `E2E lag (newest row):   p50=${s.lagP50Ms.toFixed(0)}ms ` +
      `p95=${s.lagP95Ms.toFixed(0)}ms max=${s.lagMaxMs.toFixed(0)}ms`,
  );
  log(
    `Backlog:                p50=${s.backlogP50.toFixed(0)} rows, ` +
      `max=${s.backlogMax} rows, slope=${s.backlogSlopeRowsPerSec.toFixed(1)} rows/s`,
  );
  log(
    `Drain after writes:     ${
      s.drainMs === undefined
        ? 'did not catch up'
        : `${formatDuration(s.drainMs)} (${s.drainRowsPerSec.toFixed(0)} rows/s)`
    }`,
  );
  if (s.retainedWalMaxMiB !== undefined) {
    log(
      `Retained WAL (slot):    p50=${s.retainedWalP50MiB?.toFixed(1)}MiB ` +
        `max=${s.retainedWalMaxMiB.toFixed(1)}MiB`,
    );
  }
  if (s.archiveBytesPerSec !== undefined) {
    log(
      `Archive growth:         ${(s.archiveBytesPerSec / (1024 * 1024)).toFixed(2)} MiB/s, ` +
        `${s.archiveSegments ?? 0} segments`,
    );
  }
  log(`CPU (cores, steady):    ${formatCpu(s.cpuCores)}`);
  log(`  by worker:            ${formatCpu(s.workerCpuCores)}`);
  log(`Verdict:                ${s.sustained ? 'SUSTAINED' : 'FELL BEHIND'}`);
  log(`details: ${outputPath}`);
}

function formatCpu(cpu: Readonly<Record<string, number>>): string {
  return Object.entries(cpu)
    .sort((a, b) => b[1] - a[1])
    .map(([role, cores]) => `${role}=${cores.toFixed(2)}`)
    .join(' ');
}

await main();
