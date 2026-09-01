import {mkdir, rm} from 'node:fs/promises';
import {join} from 'node:path';
import type {LogContext} from '@rocicorp/logger';
import {sleep} from '../../../../../shared/src/sleep.ts';
import {Database} from '../../../../../zqlite/src/db.ts';
import {deleteLiteDB} from '../../../db/delete-lite-db.ts';
import {StatementRunner} from '../../../db/statements.ts';
import type {RestoreResult} from '../../litestream/commands.ts';
import {deleteChangeLogDB} from '../../replicator/change-log-db.ts';
import {getSubscriptionState} from '../../replicator/schema/replication-state.ts';
import {
  contiguousHeadFrom,
  listLogSegments,
} from '../archive/archive-reader.ts';
import type {ObjectStore} from '../object-store/object-store.ts';
import {
  archiveRestore,
  listCompleteBaseCursors,
} from '../restore/archive-restore.ts';
import {compareReplicas, type TableMismatch} from './replica-comparator.ts';

/**
 * The restore drill: proves, on a schedule, that what the archive would
 * restore is the deterministic image of what a live replica serves — the
 * confidence a dual run would have provided, without one.
 *
 * The alignment trick is pin-then-restore: a read transaction on the live
 * reference replica freezes a snapshot at its current watermark W, the
 * archive is (briefly) waited on until its contiguous durable head reaches W
 * — the reference applies changes before they are archive-durable, so W can
 * lead the head by up to a seal interval — and a scratch replica is then
 * restored from the newest complete base at or below W with tail replay
 * bounded at exactly W. The two databases are then compared logically
 * (see {@link compareReplicas}) while the reference snapshot stays pinned.
 */

export type ArchiveDrillOptions = {
  /** Where the scratch replica and its temp files live. */
  scratchDir: string;
  /** Overrides {@link DEFAULT_EXCLUDED_TABLES}. */
  excludeTables?: readonly string[] | undefined;
  /**
   * How long to wait for the archive's durable head to reach the pinned
   * reference watermark. Bounds the fail-stall case (a stalled archive never
   * catches up); anything beyond a couple of seal intervals means the
   * archive is unhealthy and the drill reports `archive-behind` rather than
   * waiting it out. Default 2 minutes.
   */
  archiveWaitTimeoutMs?: number | undefined;
  /** Poll interval while waiting for the archive head. Default 1s. */
  pollIntervalMs?: number | undefined;
  /** Concurrent base chunk downloads. */
  downloadConcurrency?: number | undefined;
};

export type ArchiveDrillResult =
  | {
      outcome: 'match';
      replicaVersion: string;
      watermark: string;
      tables: number;
      rows: number;
    }
  | {
      outcome: 'mismatch';
      replicaVersion: string;
      watermark: string;
      mismatches: TableMismatch[];
    }
  /** No complete base at or below the pinned reference watermark. */
  | {outcome: 'no-base'; replicaVersion: string; watermark: string}
  /** The archive's durable head did not reach the watermark in time. */
  | {
      outcome: 'archive-behind';
      replicaVersion: string;
      watermark: string;
      durableHead: string;
    }
  | {outcome: 'restore-failed'; restoreResult: RestoreResult};

const DEFAULT_ARCHIVE_WAIT_TIMEOUT_MS = 2 * 60_000;
const DEFAULT_POLL_INTERVAL_MS = 1000;

export async function runArchiveDrill(
  lc: LogContext,
  store: ObjectStore,
  referenceFile: string,
  options: ArchiveDrillOptions,
): Promise<ArchiveDrillResult> {
  lc = lc.withContext('component', 'archive-drill');
  const {
    scratchDir,
    archiveWaitTimeoutMs = DEFAULT_ARCHIVE_WAIT_TIMEOUT_MS,
    pollIntervalMs = DEFAULT_POLL_INTERVAL_MS,
  } = options;

  const reference = new Database(lc, referenceFile, {readonly: true});
  try {
    // Pin a snapshot of the reference. The first read inside the
    // transaction freezes it, so everything below compares against the
    // replica exactly as it was at this watermark, no matter how long the
    // restore takes or how far the live writer advances meanwhile.
    reference.exec('BEGIN');
    const {replicaVersion, watermark} = getSubscriptionState(
      new StatementRunner(reference),
    );
    lc.info?.(`drilling against ${replicaVersion}/${watermark}`);

    const cursors = await listCompleteBaseCursors(store, replicaVersion);
    const baseCursor = cursors.find(cursor => cursor <= watermark);
    if (baseCursor === undefined) {
      return {outcome: 'no-base', replicaVersion, watermark};
    }

    // The reference applies changes before they are archive-durable, so its
    // watermark can lead the durable head by up to a seal interval. A
    // healthy archive catches up within seconds; a stalled one is reported
    // rather than waited out.
    const deadline = Date.now() + archiveWaitTimeoutMs;
    for (;;) {
      const segments = await listLogSegments(store, replicaVersion);
      const durableHead = contiguousHeadFrom(segments, baseCursor);
      if (durableHead >= watermark) {
        break;
      }
      if (Date.now() >= deadline) {
        lc.warn?.(
          `archive head ${durableHead} did not reach ${watermark} within ` +
            `${archiveWaitTimeoutMs} ms`,
        );
        return {
          outcome: 'archive-behind',
          replicaVersion,
          watermark,
          durableHead,
        };
      }
      await sleep(pollIntervalMs);
    }

    await mkdir(scratchDir, {recursive: true});
    const scratchFile = join(scratchDir, 'drill-replica.db');
    // A leftover scratch replica from an interrupted drill is at some other
    // watermark; archiveRestore keeps an existing compatible file, so clear
    // the way for a fresh point-in-time restore.
    deleteLiteDB(scratchFile);
    deleteChangeLogDB(scratchFile);
    try {
      const restoreResult = await archiveRestore(
        lc,
        store,
        scratchFile,
        {replicaVersion, minWatermark: watermark},
        {
          mode: 'backup',
          upTo: watermark,
          downloadConcurrency: options.downloadConcurrency,
        },
      );
      if (restoreResult !== 'success') {
        return {outcome: 'restore-failed', restoreResult};
      }

      const restored = new Database(lc, scratchFile, {readonly: true});
      try {
        const scratchState = getSubscriptionState(
          new StatementRunner(restored),
        );
        if (
          scratchState.replicaVersion !== replicaVersion ||
          scratchState.watermark !== watermark
        ) {
          // upTo plus the minWatermark constraint make this unreachable; a
          // violation is a restore bug, not a divergence.
          throw new Error(
            `scratch restore landed at ${scratchState.replicaVersion}/` +
              `${scratchState.watermark}; expected ${replicaVersion}/${watermark}`,
          );
        }
        const {tables, rows, mismatches} = compareReplicas(
          lc,
          reference,
          restored,
          {excludeTables: options.excludeTables},
        );
        if (mismatches.length > 0) {
          lc.error?.(
            `restored replica diverged from the live replica at ` +
              `${replicaVersion}/${watermark}`,
            {mismatches},
          );
          return {outcome: 'mismatch', replicaVersion, watermark, mismatches};
        }
        lc.info?.(
          `restored replica matches the live replica at ` +
            `${replicaVersion}/${watermark} (${tables} tables, ${rows} rows)`,
        );
        return {outcome: 'match', replicaVersion, watermark, tables, rows};
      } finally {
        restored.close();
      }
    } finally {
      deleteLiteDB(scratchFile);
      deleteChangeLogDB(scratchFile);
      await rm(`${scratchFile}-restore-segments`, {
        recursive: true,
        force: true,
      }).catch(() => {});
    }
  } finally {
    try {
      reference.exec('ROLLBACK');
    } catch {
      // The pinning transaction may never have started (an error before
      // BEGIN) — closing is what matters.
    }
    reference.close();
  }
}
