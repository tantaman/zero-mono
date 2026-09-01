import type {LogContext} from '@rocicorp/logger';
import {logOptions} from '../../../otel/src/log-options.ts';
import {must} from '../../../shared/src/must.ts';
import type {Config} from '../../../shared/src/options.ts';
import * as v from '../../../shared/src/valita.ts';
import {zeroOptions} from '../config/zero-config.ts';
import {
  runArchiveDrill,
  type ArchiveDrillResult,
} from '../services/backup/drill/archive-drill.ts';
import {createObjectStore} from '../services/backup/object-store/create-object-store.ts';

/**
 * The restore drill (`zero-archive-drill`): restores the logical backup
 * archive into a scratch path at the exact watermark of the local replica
 * file, and diffs the two logically. Scheduled in the archive world, this
 * continuously proves that a restore would reproduce what the live replica
 * serves. See {@link runArchiveDrill}.
 */
export const archiveDrillOptions = {
  replica: {
    file: zeroOptions.replica.file,
  },

  backup: {
    archiveURL: zeroOptions.backup.archiveURL,
  },

  litestream: {
    endpoint: zeroOptions.litestream.endpoint,
    region: zeroOptions.litestream.region,
  },

  drill: {
    scratchDir: {
      type: v.string().optional(),
      desc: [
        `Where the scratch replica is restored. Defaults to`,
        `{bold <replica-file>-drill}. The drill needs as much free disk here`,
        `as the replica occupies.`,
      ],
    },

    archiveWaitTimeoutSeconds: {
      type: v.number().default(120),
      desc: [
        `How long to wait for the archive's durable head to reach the local`,
        `replica's watermark (the replica applies changes before they are`,
        `archive-durable, so it can lead by up to a seal interval). A healthy`,
        `archive catches up within seconds; exceeding this reports`,
        `{bold archive-behind}.`,
      ],
    },
  },

  log: {level: logOptions.level, format: logOptions.format},
};

export type ArchiveDrillConfig = Config<typeof archiveDrillOptions>;

export async function drillArchive(
  lc: LogContext,
  cfg: ArchiveDrillConfig,
): Promise<ArchiveDrillResult> {
  const archiveURL = must(
    cfg.backup.archiveURL,
    '--backup-archive-url is required',
  );
  const store = await createObjectStore(archiveURL, {
    endpoint: cfg.litestream.endpoint,
    region: cfg.litestream.region,
  });
  return runArchiveDrill(lc, store, cfg.replica.file, {
    scratchDir: cfg.drill.scratchDir ?? `${cfg.replica.file}-drill`,
    archiveWaitTimeoutMs: cfg.drill.archiveWaitTimeoutSeconds * 1000,
  });
}
