import {consoleLogSink, LogContext} from '@rocicorp/logger';
import {must} from '../../../shared/src/must.ts';
import {getServerContext} from '../config/server-context.ts';
import {getNormalizedZeroConfig} from '../config/zero-config.ts';
import {initEventSink} from '../observability/events.ts';
import {getOrCreateGauge} from '../observability/metrics.ts';
import {
  BaseProducerService,
  type BaseProducerState,
} from '../services/backup/base-producer.ts';
import {createObjectStore} from '../services/backup/object-store/create-object-store.ts';
import {initReplica} from '../services/change-source/common/replica-schema.ts';
import {initialSync} from '../services/change-source/pg/initial-sync.ts';
import {exitAfter, runUntilKilled} from '../services/life-cycle.ts';
import {
  parentWorker,
  singleProcessMode,
  type Worker,
} from '../types/processes.ts';
import {getShardConfig} from '../types/shards.ts';
import {createLogContext} from './logging.ts';
import {startOtelAuto} from './otel-start.ts';

const MS_PER_HOUR = 1000 * 60 * 60;

// Default LogContext, overridden in runWorker
let lc = new LogContext('info', {}, consoleLogSink);

export default async function runWorker(
  parent: Worker,
  env: NodeJS.ProcessEnv,
  ...argv: string[]
): Promise<void> {
  const config = getNormalizedZeroConfig({env, argv});

  startOtelAuto(
    createLogContext(config, 'base-producer', 0, false),
    'base-producer',
    0,
  );
  lc = createLogContext(config, 'base-producer');
  initEventSink(lc, config);

  const {
    taskID,
    backup,
    litestream,
    replica,
    upstream,
    initialSync: syncConfig,
  } = config;
  const archiveURL = must(
    backup.archiveURL,
    '--backup-archive-url is required when --backup-mode is not litestream',
  );
  const shard = getShardConfig(config);
  const store = await createObjectStore(archiveURL, {
    // The archive shares the litestream bucket's S3 configuration; only
    // the URL (bucket/prefix) is deliberately distinct.
    endpoint: litestream.endpoint,
    region: litestream.region,
  });

  const service = new BaseProducerService(lc, {
    taskID,
    store,
    // The producer's working file is its own; the configured replica file
    // belongs to the gateway world.
    replicaFile: `${replica.file}-base-producer`,
    // The freshness of the archived tail is bounded by the seal interval,
    // so polling faster buys nothing.
    pollIntervalMs: backup.segmentSealIntervalSeconds * 1000,
    baseMaxIntervalMs: backup.baseMaxIntervalHours * MS_PER_HOUR,
    baseMaxReplayMs: backup.baseMaxReplaySeconds * 1000,
    chunkBytes: backup.baseChunkBytes,
    integrityCheck: backup.baseIntegrityCheck,
    gc: backup.gcEnabled
      ? {retainBases: backup.gcRetainBases, pitrHours: backup.gcPitrHours}
      : null,
    logConfig: config.log,
    // Lineage genesis: the real initial-sync code fed the gateway's
    // exported snapshot; only the snapshot handoff is new.
    genesisCopier: (glc, targetFile, offer) =>
      upstream.type === 'pg'
        ? initReplica(glc, 'base-producer-genesis', targetFile, (log, tx) =>
            initialSync(
              log,
              shard,
              tx,
              upstream.db,
              {
                tableCopyWorkers: syncConfig.tableCopyWorkers,
                textCopy: syncConfig.textCopy,
                providedSnapshot: {
                  snapshotID: offer.snapshotID,
                  lsn: offer.lsn,
                },
              },
              getServerContext(config),
            ).then(() => {}),
          )
        : Promise.reject(
            new Error(`genesis is not supported for upstream ${upstream.type}`),
          ),
  });
  registerProducerGauges(() => service.state());

  parent.send(['ready', {ready: true}]);

  return runUntilKilled(lc, parent, service);
}

/**
 * The health gauges of base production: base age (the signal that
 * publication has stalled), the replay-budget estimate, apply lag behind
 * upstream commits, and the last publication's duration.
 */
function registerProducerGauges(state: () => BaseProducerState) {
  getOrCreateGauge('replica', 'backup_base.age_ms', {
    description:
      'Age of the newest complete base. Climbing past the publication ' +
      'cadence means base production has stalled.',
    unit: 'ms',
  }).addCallback(o => {
    const {lastBaseCompletedAt} = state();
    if (lastBaseCompletedAt !== undefined) {
      o.observe(Date.now() - lastBaseCompletedAt);
    }
  });
  getOrCreateGauge('replica', 'backup_base.replay_estimate_ms', {
    description:
      'Estimated time to replay the archived tail above the newest base; ' +
      'the replay-budget publication trigger fires on this.',
    unit: 'ms',
  }).addCallback(o => {
    const {replayEstimateMs} = state();
    if (replayEstimateMs !== undefined) {
      o.observe(replayEstimateMs);
    }
  });
  getOrCreateGauge('replica', 'backup_producer.apply_lag_ms', {
    description:
      "How far the producer's applied state trails upstream commits, from " +
      'the commit timestamps carried in the archived stream.',
    unit: 'ms',
  }).addCallback(o => {
    const {lastAppliedCommitTimeMs} = state();
    if (lastAppliedCommitTimeMs !== undefined) {
      o.observe(Date.now() - lastAppliedCommitTimeMs);
    }
  });
  getOrCreateGauge('replica', 'backup_base.publication_duration_ms', {
    description: 'Duration of the last base publication (freeze to manifest).',
    unit: 'ms',
  }).addCallback(o => {
    const {lastPublicationDurationMs} = state();
    if (lastPublicationDurationMs !== undefined) {
      o.observe(lastPublicationDurationMs);
    }
  });
}

// fork()
if (!singleProcessMode()) {
  void exitAfter(
    () => lc,
    () => runWorker(must(parentWorker), process.env, ...process.argv.slice(2)),
  );
}
