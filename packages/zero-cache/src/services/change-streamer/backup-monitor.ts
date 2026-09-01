import type {LogContext} from '@rocicorp/logger';
import {resolver} from '@rocicorp/resolver';
import {promiseVoid} from '../../../../shared/src/resolved-promises.ts';
import {Database} from '../../../../zqlite/src/db.ts';
import {getOrCreateGauge} from '../../observability/metrics.ts';
import type {Source} from '../../types/streams.ts';
import type {SingletonService} from '../service.ts';
import type {ChangeStreamerService} from './change-streamer.ts';

export type BackedUpWatermark = {
  watermark: string;
  writeTimeMs?: number | undefined; // optional for debuggin; available on v5
  backupTimeMs: number;
};

export class BackupMonitor implements SingletonService {
  readonly id = 'backup-monitor';
  readonly #lc: LogContext;
  readonly #watermarks: Source<BackedUpWatermark>;
  readonly #changeStreamer: ChangeStreamerService;
  readonly #replicaFile: string | null;
  readonly #firstBackupReceived = resolver();

  #latestBackup: BackedUpWatermark | undefined;

  /**
   * `replicaFile` is `null` when this task holds no replica -- the gateway
   * of backup mode `archive`. The backup-lag gauge is derived from the
   * replica's own write time, so with no replica there is nothing to derive
   * it from; the archive world reports the equivalent through
   * `replica.backup_archive.cursor_lag_versions` and
   * `replica.backup_base.age_ms` instead.
   */
  constructor(
    lc: LogContext,
    watermarks: Source<BackedUpWatermark>,
    changeStreamer: ChangeStreamerService,
    replicaFile: string | null,
  ) {
    this.#lc = lc;
    this.#watermarks = watermarks;
    this.#changeStreamer = changeStreamer;
    this.#replicaFile = replicaFile;
  }

  firstBackupReceived() {
    return this.#firstBackupReceived.promise;
  }

  async run() {
    this.#lc.info?.('starting backup monitor');
    this.#initBackupLagMetric();

    for await (const backedUp of this.#watermarks) {
      if (!this.#latestBackup) {
        this.#firstBackupReceived.resolve();
      } else if (backedUp.watermark < this.#latestBackup.watermark) {
        this.#lc.warn?.(`ignoring earlier backup watermark`, {backedUp});
        continue;
      } else if (backedUp.watermark === this.#latestBackup.watermark) {
        this.#lc.debug?.(`ignoring redundant backup watermark`, {backedUp});
        continue;
      }
      this.#lc.info?.(`received backup watermark`, {backedUp});
      this.#latestBackup = backedUp;
      this.#changeStreamer.trackBackupWatermark(backedUp.watermark);
    }
    this.#lc.info?.('watermark stream closed. BackupMonitor stopped.');
  }

  stop(): Promise<void> {
    this.#watermarks.cancel();
    return promiseVoid;
  }

  #initBackupLagMetric() {
    const replicaFile = this.#replicaFile;
    if (replicaFile === null) {
      return;
    }
    getOrCreateGauge('replica', 'backup_lag', {
      description:
        'Latency from when a change is written to the replica ' +
        'to when it is backed up to litestream. It is expected to create a saw ' +
        'pattern from 0 to the configured ZERO_LITESTREAM_INCREMENTAL_BACKUP_INTERVAL_MINUTES.',
      unit: 'millisecond',
    }).addCallback(o => {
      const latestBackup = this.#latestBackup;
      if (!latestBackup) {
        this.#lc.warn?.(
          `no backed up watermarks. unable to report replica.backup_lag`,
        );
        return;
      }
      const db = new Database(this.#lc, replicaFile, {readonly: true});
      try {
        const {writeTimeMs} = db
          .prepare(/*sql*/ `SELECT writeTimeMs FROM "_zero.replicationState"`)
          .get<{writeTimeMs: number}>();
        const backupLag = Math.max(0, writeTimeMs - latestBackup.backupTimeMs);
        o.observe(backupLag);
      } catch (e) {
        this.#lc.warn?.(`error measuring replica.backup_lag metric`, e);
      } finally {
        db.close();
      }
    });
  }
}
