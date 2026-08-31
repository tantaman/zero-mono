import {createHash} from 'node:crypto';
import {open} from 'node:fs/promises';
import type {LogContext} from '@rocicorp/logger';
import {Database} from '../../../../../zqlite/src/db.ts';
import {StatementRunner} from '../../../db/statements.ts';
import {getSubscriptionState} from '../../replicator/schema/replication-state.ts';
import {
  baseChunkKey,
  baseCompleteKey,
  baseIntentKey,
} from '../archive/layout.ts';
import {
  ObjectAlreadyExistsError,
  type ObjectStore,
} from '../object-store/object-store.ts';
import {
  BASE_FORMAT,
  BASE_FORMAT_VERSION,
  decodeBaseManifest,
  encodeBaseIntent,
  encodeBaseManifest,
  type BaseManifest,
} from './manifest.ts';

export type PublishBaseOptions = {
  chunkBytes: number;
  integrityCheck: 'full' | 'quick';
  /** Overridable for tests. */
  now?: (() => number) | undefined;
};

export type PublishBaseResult = {
  manifest: BaseManifest;
  /** False when the base at this cursor was already complete (a retry). */
  published: boolean;
};

export class BasePublishError extends Error {
  readonly name = 'BasePublishError';
}

/**
 * Publishes a frozen replica file as a base: verify, chunk-upload,
 * manifest-last.
 *
 * The caller owns the freeze — nothing may write the file (or its WAL) while
 * this runs. Any journal sidecar is folded into the main file first, so the
 * chunked bytes are the complete database. The base is named by the cursor
 * embedded in the file, which makes publication idempotent: a retry of a
 * crashed publication finds its objects already present and completes the
 * same base rather than duplicating it, and a crash before `complete.json`
 * leaves a base that does not exist yet.
 */
export async function publishBase(
  lc: LogContext,
  store: ObjectStore,
  replicaFile: string,
  opts: PublishBaseOptions,
): Promise<PublishBaseResult> {
  const {chunkBytes, integrityCheck} = opts;
  const now = opts.now ?? Date.now;

  const {replicaVersion, cursor} = freezeAndVerify(
    lc,
    replicaFile,
    integrityCheck,
  );

  const completeKey = baseCompleteKey(replicaVersion, cursor);
  const existing = await store.head(completeKey);
  if (existing !== undefined) {
    const manifest = decodeBaseManifest(await store.get(completeKey));
    lc.info?.(`base ${replicaVersion}/${cursor} is already complete`);
    return {manifest, published: false};
  }

  await putIgnoringExisting(
    store,
    baseIntentKey(replicaVersion, cursor),
    encodeBaseIntent({
      format: BASE_FORMAT,
      version: BASE_FORMAT_VERSION,
      replicaVersion,
      cursor,
      startedAt: now(),
    }),
  );

  const chunks: {size: number; sha256: string}[] = [];
  const fileHash = createHash('sha256');
  let fileSize = 0;
  const file = await open(replicaFile, 'r');
  try {
    const buffer = Buffer.alloc(chunkBytes);
    for (let index = 0; ; index++) {
      const {bytesRead} = await file.read(buffer, 0, chunkBytes, fileSize);
      if (bytesRead === 0) {
        break;
      }
      const chunk = buffer.subarray(0, bytesRead);
      fileHash.update(chunk);
      fileSize += bytesRead;
      chunks.push({
        size: bytesRead,
        sha256: createHash('sha256').update(chunk).digest('hex'),
      });
      await putIgnoringExisting(
        store,
        baseChunkKey(replicaVersion, cursor, index),
        // Copy: the shared read buffer is reused for the next chunk, and a
        // store may hold the value by reference (the in-memory test store).
        Uint8Array.from(chunk),
      );
      if (bytesRead < chunkBytes) {
        break;
      }
    }
  } finally {
    await file.close();
  }
  if (fileSize === 0) {
    throw new BasePublishError(`replica file ${replicaFile} is empty`);
  }

  const manifest: BaseManifest = {
    format: BASE_FORMAT,
    version: BASE_FORMAT_VERSION,
    replicaVersion,
    cursor,
    fileSize,
    fileSha256: fileHash.digest('hex'),
    chunkBytes,
    chunks,
    completedAt: now(),
  };
  try {
    // Strictly last: the base exists once (and only once) this lands.
    await store.putIfAbsent(completeKey, encodeBaseManifest(manifest));
  } catch (e) {
    if (e instanceof ObjectAlreadyExistsError) {
      // A concurrent publication of the same frozen cursor completed first;
      // both wrote identical content, so adopt the winner's manifest.
      return {
        manifest: decodeBaseManifest(await store.get(completeKey)),
        published: false,
      };
    }
    throw e;
  }
  lc.info?.(
    `published base ${replicaVersion}/${cursor}: ${fileSize} bytes in ` +
      `${chunks.length} chunk(s)`,
  );
  return {manifest, published: true};
}

/**
 * Opens the frozen replica, folds any WAL into the main file, runs the
 * configured integrity check, and reads the embedded identity that names the
 * base.
 */
function freezeAndVerify(
  lc: LogContext,
  replicaFile: string,
  integrityCheck: 'full' | 'quick',
): {replicaVersion: string; cursor: string} {
  const db = new Database(lc, replicaFile);
  try {
    const [{['journal_mode']: journalMode}] = db.pragma('journal_mode') as [
      {['journal_mode']: string},
    ];
    if (journalMode === 'wal' || journalMode === 'wal2') {
      // The chunked upload reads only the main file, so pending WAL frames
      // must be folded in first. The caller's freeze guarantees no writer
      // holds the file, so TRUNCATE cannot block indefinitely.
      db.pragma('wal_checkpoint(TRUNCATE)');
    }
    const pragma =
      integrityCheck === 'full' ? 'integrity_check' : 'quick_check';
    const results = db.pragma(pragma) as Record<string, string>[];
    const failures = results
      .map(row => Object.values(row)[0])
      .filter(value => value !== 'ok');
    if (failures.length > 0) {
      throw new BasePublishError(
        `replica ${replicaFile} failed ${pragma}: ${failures.join('; ')}`,
      );
    }
    const {replicaVersion, watermark} = getSubscriptionState(
      new StatementRunner(db),
    );
    return {replicaVersion, cursor: watermark};
  } finally {
    db.close();
  }
}

async function putIgnoringExisting(
  store: ObjectStore,
  key: string,
  data: Uint8Array,
): Promise<void> {
  try {
    await store.putIfAbsent(key, data);
  } catch (e) {
    if (!(e instanceof ObjectAlreadyExistsError)) {
      throw e;
    }
    // A retry of a crashed publication: the object was already uploaded with
    // identical content (deterministic name, frozen source).
  }
}
