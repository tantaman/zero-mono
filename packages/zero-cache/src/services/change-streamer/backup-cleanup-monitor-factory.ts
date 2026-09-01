import type {LogContext} from '@rocicorp/logger';
import {must} from '../../../../shared/src/must.ts';
import type {NormalizedZeroConfig} from '../../config/normalize.ts';
import type {Source} from '../../types/streams.ts';
import type {ObjectStore} from '../backup/object-store/object-store.ts';
import {getLastBackupTime} from '../litestream/commands.ts';
import {ArchiveWatermarkPoller} from './archive-watermark-poller.ts';
import {type BackedUpWatermark, BackupMonitor} from './backup-monitor.ts';
import type {ChangeStreamerService} from './change-streamer.ts';
import {
  type BackupStateVerifier,
  Litestream3PrometheusPoller,
} from './litestream3-prometheus-poller.ts';
import {ReplicaPoller} from './replica-poller.ts';
import {VfsWatermarkPoller} from './vfs-watermark-poller.ts';

export type BackupCleanupMonitorFactoryOptions = {
  lc: LogContext;
  config: NormalizedZeroConfig;
  replicaFile: string;
  changeStreamer: ChangeStreamerService;
  verifyBackupState?: BackupStateVerifier | undefined;
  /**
   * Supplied in backup mode `archive`: the purge floor and snapshot
   * confirmation come from the archive's complete-base listing rather than
   * any litestream signal.
   */
  archive?: {store: ObjectStore; replicaVersion: string} | undefined;
};

export function createBackupCleanupMonitor({
  lc,
  config,
  replicaFile,
  changeStreamer,
  verifyBackupState,
  archive,
}: BackupCleanupMonitorFactoryOptions): BackupMonitor {
  const {log, litestream, replica} = config;
  const {backupURL} = litestream;

  let stream: Source<BackedUpWatermark>;

  if (archive) {
    stream = new ArchiveWatermarkPoller(lc, {
      store: archive.store,
      replicaVersion: archive.replicaVersion,
      // Stale = several missed publication cadences; the trigger includes a
      // replay-budget path that publishes far more often under load.
      staleBaseGraceMs: config.backup.baseMaxIntervalHours * 3 * 3600 * 1000,
    }).start();
  } else if (!backupURL) {
    stream = new ReplicaPoller(lc, replicaFile).start();
  } else if (config.litestream.backupUsingV5) {
    const {
      logLevel,
      endpoint,
      region,
      vfsQueryExecutable,
      vfsPollIntervalMs: remotePollIntervalMs,
    } = litestream;
    stream = new VfsWatermarkPoller(lc, replicaFile, {
      executable: must(
        vfsQueryExecutable,
        `litestream-vfs-query-executable must be defined`,
      ),
      remotePollIntervalMs,
      backupURL,
      region,
      endpoint,
      logLevel,
      logFormat: log.format,
    }).start();
  } else {
    const {port: metricsPort} = litestream;
    stream = new Litestream3PrometheusPoller(
      lc,
      replicaFile,
      backupURL,
      `http://localhost:${metricsPort}/metrics`,
      verifyBackupState ??
        (() => getLastBackupTime(lc, litestream, replica.file)),
    ).start();
  }

  // The archive world's gateway holds no replica, so there is no file to
  // derive the backup-lag gauge from.
  return new BackupMonitor(
    lc,
    stream,
    changeStreamer,
    archive ? null : replicaFile,
  );
}
