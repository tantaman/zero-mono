import {mkdir, rename, rm, writeFile} from 'node:fs/promises';
import {join} from 'node:path';
import type {LogContext} from '@rocicorp/logger';
import {
  baseIntentKey,
  basePrefix,
  baseRequestKey,
  parseBaseCompleteKey,
} from '../archive/layout.ts';
import {
  BASE_REQUEST_FORMAT,
  decodeBaseIntent,
  encodeBaseRequest,
} from '../base/manifest.ts';
import {
  ObjectNotFoundError,
  type ObjectStore,
} from '../object-store/object-store.ts';

export type LiveBaseRequestOptions = {
  /** Overall budget; on expiry the caller falls back to the newest base. */
  timeoutMs: number;
  /** How often to look for the producer's response. Default 1000. */
  pollIntervalMs?: number | undefined;
  /**
   * A directory to prefetch the new base's chunks into while the producer
   * is still uploading them — chunks are separately addressable objects, so
   * the download overlaps the upload. `archiveRestore` consumes the cache
   * via its `chunkCacheDir` option (every cached chunk is still verified
   * against the manifest). The caller owns the directory's cleanup.
   */
  prefetchDir?: string | undefined;
  /** Overridable for tests. */
  now?: (() => number) | undefined;
  setTimeoutFn?: typeof setTimeout | undefined;
};

export type LiveBaseResult =
  /** The producer published a base at least as fresh as the request. */
  | 'published'
  /** The producer judged the newest existing base current enough. */
  | 'current'
  /** No response in time; restore from the newest existing base. */
  | 'timeout';

/**
 * The restorer's half of the accelerated live-base protocol — a decoupled
 * request/response through the store itself: write a request marker, then
 * watch for the producer to respond by publishing a fresh base (it deletes
 * the marker either way; a deleted marker with no new base means the newest
 * existing base is already current). Every outcome degrades to the same
 * safe next step: `archiveRestore` from the newest complete base, which is
 * exactly the fallback the caller runs on timeout.
 */
export async function requestLiveBase(
  lc: LogContext,
  store: ObjectStore,
  replicaVersion: string,
  taskID: string,
  options: LiveBaseRequestOptions,
): Promise<LiveBaseResult> {
  lc = lc.withContext('component', 'live-base-request');
  const {timeoutMs, prefetchDir} = options;
  const pollIntervalMs = options.pollIntervalMs ?? 1000;
  const now = options.now ?? Date.now;
  const setTimeoutFn = options.setTimeoutFn ?? setTimeout;

  const before = await newestCompleteCursor(store, replicaVersion);
  const markerKey = baseRequestKey(replicaVersion, taskID);
  await store.put(
    markerKey,
    encodeBaseRequest({
      format: BASE_REQUEST_FORMAT,
      version: 1,
      taskID,
      requestedAt: now(),
    }),
  );
  lc.info?.(`requested a live base for ${replicaVersion} (task ${taskID})`);

  const prefetched = new Set<number>();
  let prefetchCursor: string | undefined;
  const deadline = now() + timeoutMs;
  try {
    while (now() < deadline) {
      const newest = await newestCompleteCursor(store, replicaVersion);
      if (newest !== undefined && (before === undefined || newest > before)) {
        lc.info?.(`the producer published base ${newest}`);
        return 'published';
      }
      if ((await store.head(markerKey)) === undefined) {
        // The producer consumed the request without publishing: the newest
        // existing base is already current.
        lc.info?.(`the producer reports the newest base is already current`);
        return 'current';
      }
      if (prefetchDir !== undefined) {
        prefetchCursor ??= await inFlightCursor(store, replicaVersion, before);
        if (prefetchCursor !== undefined) {
          await prefetchChunks(
            store,
            replicaVersion,
            prefetchCursor,
            prefetchDir,
            prefetched,
          );
        }
      }
      await new Promise(resolve => setTimeoutFn(resolve, pollIntervalMs));
    }
    lc.warn?.(
      `no live base after ${timeoutMs}ms; falling back to the newest ` +
        `complete base`,
    );
    return 'timeout';
  } finally {
    await store.delete(markerKey).catch(() => {});
  }
}

const INTENT_KEY = /\/([0-9a-z]+)\/intent\.json$/;

async function newestCompleteCursor(
  store: ObjectStore,
  replicaVersion: string,
): Promise<string | undefined> {
  const objects = await store.list(basePrefix(replicaVersion));
  return objects
    .map(o => parseBaseCompleteKey(replicaVersion, o.key))
    .findLast(c => c !== undefined);
}

/**
 * The cursor of a publication started after the request: an `intent.json`
 * above `before` with no `complete.json` yet.
 */
async function inFlightCursor(
  store: ObjectStore,
  replicaVersion: string,
  before: string | undefined,
): Promise<string | undefined> {
  const objects = await store.list(basePrefix(replicaVersion));
  const complete = new Set(
    objects.map(o => parseBaseCompleteKey(replicaVersion, o.key)),
  );
  for (const {key} of objects.toReversed()) {
    const match = INTENT_KEY.exec(key);
    const cursor = match?.[1];
    if (
      cursor !== undefined &&
      (before === undefined || cursor > before) &&
      !complete.has(cursor)
    ) {
      try {
        decodeBaseIntent(
          await store.get(baseIntentKey(replicaVersion, cursor)),
        );
        return cursor;
      } catch {
        return undefined; // unreadable intent: skip prefetching
      }
    }
  }
  return undefined;
}

/**
 * Downloads chunks of the in-flight base as they appear, named by index and
 * paired with their hashes so `archiveRestore` can serve verified reads
 * from the cache instead of the store.
 */
async function prefetchChunks(
  store: ObjectStore,
  replicaVersion: string,
  cursor: string,
  prefetchDir: string,
  prefetched: Set<number>,
): Promise<void> {
  await mkdir(prefetchDir, {recursive: true});
  const chunkPrefix = `${basePrefix(replicaVersion)}${cursor}/chunk/`;
  const objects = await store.list(chunkPrefix);
  for (const {key} of objects) {
    const index = Number(key.slice(chunkPrefix.length));
    if (!Number.isInteger(index) || prefetched.has(index)) {
      continue;
    }
    let data;
    try {
      data = await store.get(key);
    } catch (e) {
      if (e instanceof ObjectNotFoundError) {
        continue; // raced a cleanup; the fallback path still works
      }
      throw e;
    }
    // Written whole-file + rename so a partial write is never mistaken for
    // a cached chunk.
    const path = chunkCachePath(prefetchDir, cursor, index);
    const temp = `${path}.tmp`;
    await writeFile(temp, data);
    await rm(path, {force: true});
    await rename(temp, path);
    prefetched.add(index);
  }
}

/** The cache location `archiveRestore`'s chunkCacheDir option reads. */
export function chunkCachePath(
  dir: string,
  cursor: string,
  index: number,
): string {
  return join(dir, `${cursor}-${String(index).padStart(8, '0')}.chunk`);
}
