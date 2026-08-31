import {createHash} from 'node:crypto';
import {existsSync} from 'node:fs';
import {open, rename, rm} from 'node:fs/promises';
import type {LogContext} from '@rocicorp/logger';
import {Database} from '../../../../../zqlite/src/db.ts';
import {deleteLiteDB} from '../../../db/delete-lite-db.ts';
import {StatementRunner} from '../../../db/statements.ts';
import type {
  ReplicaConstraints,
  RestoreResult,
} from '../../litestream/commands.ts';
import {deleteChangeLogDB} from '../../replicator/change-log-db.ts';
import {ChangeProcessor} from '../../replicator/change-processor.ts';
import {getSubscriptionState} from '../../replicator/schema/replication-state.ts';
import {
  iterateTransactions,
  listLogSegments,
} from '../archive/archive-reader.ts';
import {
  ARCHIVE_ROOT,
  baseChunkKey,
  baseCompleteKey,
  basePrefix,
  lineagesFromKeys,
  parseBaseCompleteKey,
  type SegmentRef,
} from '../archive/layout.ts';
import {decodeBaseManifest, type BaseManifest} from '../base/manifest.ts';
import type {ObjectStore} from '../object-store/object-store.ts';

export type ArchiveRestoreOptions = {
  /**
   * The apply mode for tail replay. Defaults to `'backup'`, which is what the
   * canonical (litestream-era) backup replica contains: bases are produced
   * from the backup-replicator's file, and replay must produce exactly what
   * that applier would have produced.
   */
  mode?: 'backup' | 'serving' | undefined;
  /** Concurrent chunk downloads. Default 4. */
  downloadConcurrency?: number | undefined;
};

const DEFAULT_DOWNLOAD_CONCURRENCY = 4;

/**
 * Restores a replica from the archive, implementing the same result contract
 * as litestream's `tryRestore`:
 *
 * 1. Select the newest complete base (`complete.json`); no complete base in
 *    the lineage means `no_backup`, which callers turn into initial sync.
 * 2. Download its chunks in parallel to fixed offsets, verifying per-chunk
 *    and whole-file checksums, and promote the file atomically.
 * 3. Validate that the embedded cursor equals the manifest cursor.
 * 4. Replay the logical tail — the contiguous archived range above the
 *    cursor — through the real change processor.
 * 5. Validate the result against the caller's constraints.
 *
 * A base that fails to download or verify falls back to the next-newest
 * complete base (the "live producer failed mid-upload" drill); a tail-replay
 * failure does not, since an older base only widens the failing range.
 *
 * Mirroring `litestream restore -if-db-not-exists`, an existing replica file
 * is never overwritten: it is validated against the constraints and either
 * kept (`success`) or deleted (`invalid_replica`, and the caller retries).
 */
export async function archiveRestore(
  lc: LogContext,
  store: ObjectStore,
  replicaFile: string,
  constraints: ReplicaConstraints | undefined,
  options: ArchiveRestoreOptions = {},
): Promise<RestoreResult> {
  lc = lc.withContext('component', 'archive-restore');
  if (existsSync(replicaFile)) {
    if (validateReplica(lc, replicaFile, constraints)) {
      lc.info?.(`existing replica ${replicaFile} is compatible; not restoring`);
      return 'success';
    }
    lc.info?.(`deleting incompatible local replica ${replicaFile}`);
    deleteLiteDB(replicaFile);
    deleteChangeLogDB(replicaFile);
    return 'invalid_replica';
  }

  const tempFile = `${replicaFile}.tmp`;
  try {
    const replicaVersion =
      constraints?.replicaVersion ?? (await newestLineage(store));
    if (replicaVersion === undefined) {
      return 'no_backup';
    }
    const cursors = await listCompleteBaseCursors(store, replicaVersion);
    if (cursors.length === 0) {
      lc.info?.(`no complete base found for lineage ${replicaVersion}`);
      return 'no_backup';
    }

    // Newest first, falling back on a base whose objects cannot be fetched
    // or verified (e.g. its producer failed mid-upload in a way that still
    // published, or GC raced it).
    let manifest: BaseManifest | undefined;
    for (const cursor of cursors) {
      try {
        const candidate = decodeBaseManifest(
          await store.get(baseCompleteKey(replicaVersion, cursor)),
        );
        await downloadBase(lc, store, candidate, tempFile, options);
        manifest = candidate;
        break;
      } catch (e) {
        lc.warn?.(
          `unable to restore base ${replicaVersion}/${cursor}; ` +
            `falling back to the previous base`,
          e,
        );
        await rm(tempFile, {force: true});
      }
    }
    if (manifest === undefined) {
      lc.error?.(`all ${cursors.length} complete base(s) failed to restore`);
      return 'error';
    }

    await rename(tempFile, replicaFile);
    // This restore materialized the replica file; any change log beside it
    // was written against a replica that is no longer there.
    deleteChangeLogDB(replicaFile);

    const target = await replayTail(
      lc,
      store,
      replicaFile,
      manifest,
      options.mode ?? 'backup',
    );
    lc.info?.(
      `restored base ${manifest.replicaVersion}/${manifest.cursor} and ` +
        `replayed the archived tail to ${target}`,
    );

    if (!validateReplica(lc, replicaFile, constraints)) {
      deleteLiteDB(replicaFile);
      deleteChangeLogDB(replicaFile);
      return 'invalid_replica';
    }
    return 'success';
  } catch (e) {
    lc.error?.(`error restoring from the archive`, e);
    // A partially-restored or partially-replayed replica is deleted rather
    // than left for the next attempt to misjudge.
    deleteLiteDB(replicaFile);
    deleteChangeLogDB(replicaFile);
    return 'error';
  } finally {
    await rm(tempFile, {force: true}).catch(() => {});
  }
}

async function newestLineage(store: ObjectStore): Promise<string | undefined> {
  const objects = await store.list(ARCHIVE_ROOT);
  const lineages = lineagesFromKeys(objects.map(o => o.key));
  return lineages.at(-1);
}

/** Complete base cursors, newest first. */
async function listCompleteBaseCursors(
  store: ObjectStore,
  replicaVersion: string,
): Promise<string[]> {
  const objects = await store.list(basePrefix(replicaVersion));
  return objects
    .map(o => parseBaseCompleteKey(replicaVersion, o.key))
    .filter(cursor => cursor !== undefined)
    .toSorted()
    .toReversed();
}

/**
 * Downloads the manifest's chunks to their fixed offsets in `tempFile` with
 * bounded concurrency, verifying each chunk's size and checksum, then the
 * whole file against the manifest.
 */
async function downloadBase(
  lc: LogContext,
  store: ObjectStore,
  manifest: BaseManifest,
  tempFile: string,
  options: ArchiveRestoreOptions,
): Promise<void> {
  const {replicaVersion, cursor, chunkBytes, chunks} = manifest;
  const concurrency =
    options.downloadConcurrency ?? DEFAULT_DOWNLOAD_CONCURRENCY;
  await rm(tempFile, {force: true});
  const file = await open(tempFile, 'w');
  try {
    let next = 0;
    const worker = async () => {
      for (let index = next++; index < chunks.length; index = next++) {
        const expected = chunks[index];
        const data = await store.get(
          baseChunkKey(replicaVersion, cursor, index),
        );
        if (data.length !== expected.size) {
          throw new Error(
            `chunk ${index} is ${data.length} bytes; expected ${expected.size}`,
          );
        }
        const sha256 = createHash('sha256').update(data).digest('hex');
        if (sha256 !== expected.sha256) {
          throw new Error(`chunk ${index} failed its checksum`);
        }
        await file.write(data, 0, data.length, index * chunkBytes);
      }
    };
    await Promise.all(
      Array.from({length: Math.min(concurrency, chunks.length)}, worker),
    );
    await file.sync();
  } finally {
    await file.close();
  }

  // The per-chunk checksums verified the downloads; this verifies what
  // actually landed on disk, end to end.
  const {size, sha256} = await hashFile(tempFile);
  if (size !== manifest.fileSize || sha256 !== manifest.fileSha256) {
    throw new Error(
      `restored base file does not match its manifest: ` +
        `${size} bytes/${sha256} vs ${manifest.fileSize} bytes/${manifest.fileSha256}`,
    );
  }
  lc.debug?.(`downloaded base ${replicaVersion}/${cursor} (${size} bytes)`);
}

async function hashFile(path: string): Promise<{size: number; sha256: string}> {
  const hash = createHash('sha256');
  let size = 0;
  const file = await open(path, 'r');
  try {
    const buffer = Buffer.alloc(4 * 1024 * 1024);
    for (;;) {
      const {bytesRead} = await file.read(buffer, 0, buffer.length, size);
      if (bytesRead === 0) {
        break;
      }
      hash.update(buffer.subarray(0, bytesRead));
      size += bytesRead;
    }
  } finally {
    await file.close();
  }
  return {size, sha256: hash.digest('hex')};
}

/**
 * Replays the contiguous archived range above the base cursor through the
 * real change processor, and returns the watermark the replica ends at.
 *
 * Replay is journaled — every transaction commits its watermark atomically
 * with its data — so an interrupted replay resumes exactly-once semantics on
 * retry; this function nevertheless runs to the precomputed target so the
 * result is a known cursor.
 */
async function replayTail(
  lc: LogContext,
  store: ObjectStore,
  replicaFile: string,
  manifest: BaseManifest,
  mode: 'backup' | 'serving' = 'backup',
): Promise<string> {
  const {replicaVersion, cursor} = manifest;
  const db = new Database(lc, replicaFile);
  try {
    const state = getSubscriptionState(new StatementRunner(db));
    if (state.replicaVersion !== replicaVersion || state.watermark !== cursor) {
      throw new Error(
        `restored base contains ${state.replicaVersion}/${state.watermark}; ` +
          `its manifest claims ${replicaVersion}/${cursor}`,
      );
    }

    const segments = await listLogSegments(store, replicaVersion);
    const target = contiguousHeadFrom(segments, cursor);
    if (target === cursor) {
      return target;
    }

    let failure: unknown;
    const processor = new ChangeProcessor(
      new StatementRunner(db),
      mode,
      (_, err) => (failure = err),
    );
    for await (const transaction of iterateTransactions(
      store,
      replicaVersion,
      cursor,
      target,
    )) {
      for (const message of transaction.messages) {
        processor.processMessage(lc, message);
        if (failure !== undefined) {
          throw failure;
        }
      }
    }

    const {watermark} = getSubscriptionState(new StatementRunner(db));
    if (watermark !== target) {
      throw new Error(`tail replay ended at ${watermark}; expected ${target}`);
    }
    return target;
  } finally {
    db.close();
  }
}

/**
 * The end of the contiguous segment chain starting at `cursor`, i.e. how far
 * this base can be caught up from the archive. A trailing discontinuity
 * bounds the target rather than failing it: nothing past a gap was ever
 * reported durable, so the caller's constraint check decides whether the
 * reachable head suffices.
 */
function contiguousHeadFrom(segments: SegmentRef[], cursor: string): string {
  let head = cursor;
  for (const segment of segments) {
    if (segment.end <= head) {
      continue;
    }
    if (segment.start > head) {
      break;
    }
    // segment.start <= head < segment.end: contiguous (the first segment may
    // straddle the cursor).
    head = segment.end;
  }
  return head;
}

function validateReplica(
  lc: LogContext,
  replicaFile: string,
  constraints: ReplicaConstraints | undefined,
): boolean {
  let db: Database | undefined;
  try {
    // Open and read the subscription state as a sanity/corruption check even
    // without constraints, mirroring the litestream restore validation.
    db = new Database(lc, replicaFile);
    const {replicaVersion, watermark} = getSubscriptionState(
      new StatementRunner(db),
    );
    if (constraints) {
      if (replicaVersion !== constraints.replicaVersion) {
        lc.warn?.(
          `restored replica version ${replicaVersion} does not match ` +
            `expected replicaVersion ${constraints.replicaVersion}`,
        );
        return false;
      }
      if (watermark < constraints.minWatermark) {
        lc.warn?.(
          `restored replica watermark ${watermark} is earlier than ` +
            `minWatermark ${constraints.minWatermark}`,
        );
        return false;
      }
    }
    return true;
  } catch (e) {
    lc.error?.('error validating the restored replica', e);
    return false;
  } finally {
    db?.close();
  }
}
