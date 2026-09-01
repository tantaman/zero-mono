import {existsSync, mkdtempSync, rmSync, writeFileSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {afterEach, beforeEach, describe, expect, test, vi} from 'vitest';
import {createSilentLogContext} from '../../../../../shared/src/logging-test-utils.ts';
import {Database} from '../../../../../zqlite/src/db.ts';
import {StatementRunner} from '../../../db/statements.ts';
import {
  getSubscriptionState,
  initReplicationState,
  updateReplicationWatermark,
} from '../../replicator/schema/replication-state.ts';
import {baseRequestKey} from '../archive/layout.ts';
import {publishBase} from '../base/base-publisher.ts';
import {InMemoryObjectStore} from '../test-utils.ts';
import {archiveRestore} from './archive-restore.ts';
import {chunkCachePath, requestLiveBase} from './live-base.ts';

const lc = createSilentLogContext();

describe('backup/restore/live-base', () => {
  let dir: string;
  let store: InMemoryObjectStore;

  beforeEach(() => {
    dir = mkdtempSync(join(tmpdir(), 'zero-live-base-test-'));
    store = new InMemoryObjectStore();
  });

  afterEach(() => {
    rmSync(dir, {recursive: true, force: true});
  });

  /** Builds and publishes a lineage-'02' base at `watermark`. */
  async function publishSeedBase(watermark: string, chunkBytes = 4096) {
    const file = join(dir, `seed-${watermark}.db`);
    const db = new Database(lc, file);
    initReplicationState(db, ['zero_data'], '02');
    db.exec(
      `CREATE TABLE issues(issueID TEXT PRIMARY KEY, val TEXT, _0_version TEXT)`,
    );
    if (watermark !== '02') {
      updateReplicationWatermark(new StatementRunner(db), watermark);
    }
    db.close();
    await publishBase(lc, store, file, {chunkBytes, integrityCheck: 'full'});
    return file;
  }

  test('resolves published when the producer responds with a fresh base', async () => {
    await publishSeedBase('02');
    // A "producer" that publishes once it sees the request marker.
    const producer = (async () => {
      await vi.waitFor(() =>
        expect(store.objects.has(baseRequestKey('02', 'task-1'))).toBe(true),
      );
      await publishSeedBase('05');
    })();

    const result = await requestLiveBase(lc, store, '02', 'task-1', {
      timeoutMs: 5000,
      pollIntervalMs: 5,
    });
    await producer;
    expect(result).toBe('published');
    // The marker is cleaned up by the requester on exit.
    expect(store.objects.has(baseRequestKey('02', 'task-1'))).toBe(false);
  });

  test('resolves current when the producer consumes the marker without publishing', async () => {
    await publishSeedBase('02');
    const producer = (async () => {
      await vi.waitFor(() =>
        expect(store.objects.has(baseRequestKey('02', 'task-1'))).toBe(true),
      );
      await store.delete(baseRequestKey('02', 'task-1'));
    })();

    const result = await requestLiveBase(lc, store, '02', 'task-1', {
      timeoutMs: 5000,
      pollIntervalMs: 5,
    });
    await producer;
    expect(result).toBe('current');
  });

  test('times out into the newest-base fallback', async () => {
    await publishSeedBase('02');
    const result = await requestLiveBase(lc, store, '02', 'task-1', {
      timeoutMs: 30,
      pollIntervalMs: 5,
    });
    expect(result).toBe('timeout');
    expect(store.objects.has(baseRequestKey('02', 'task-1'))).toBe(false);

    // The fallback restore works from the newest complete base.
    const restoreFile = join(dir, 'restored.db');
    expect(
      await archiveRestore(lc, store, restoreFile, {
        replicaVersion: '02',
        minWatermark: '',
      }),
    ).toBe('success');
  });

  test('prefetches in-flight chunks, which the restore consumes verified', async () => {
    await publishSeedBase('02');

    // A "producer" that uploads the new base slowly: the intent and chunks
    // land while the requester is polling; complete.json lands last.
    const producer = (async () => {
      await vi.waitFor(() =>
        expect(store.objects.has(baseRequestKey('02', 'task-1'))).toBe(true),
      );
      let chunkDelay = Promise.resolve();
      store.beforePut = key => {
        if (key.includes('/chunk/')) {
          // Let the requester's poll interleave with the upload.
          chunkDelay = new Promise(resolve => setTimeout(resolve, 10));
          return chunkDelay;
        }
        return key.endsWith('complete.json') ? chunkDelay : undefined;
      };
      await publishSeedBase('05', 1024);
      store.beforePut = undefined;
    })();

    const prefetchDir = join(dir, 'prefetch');
    const result = await requestLiveBase(lc, store, '02', 'task-1', {
      timeoutMs: 10_000,
      pollIntervalMs: 5,
      prefetchDir,
    });
    await producer;
    expect(result).toBe('published');
    expect(existsSync(chunkCachePath(prefetchDir, '05', 0))).toBe(true);

    // Poison one cached chunk: verification falls back to the store.
    writeFileSync(chunkCachePath(prefetchDir, '05', 0), 'garbage');

    const restoreFile = join(dir, 'restored.db');
    expect(
      await archiveRestore(
        lc,
        store,
        restoreFile,
        {replicaVersion: '02', minWatermark: '05'},
        {chunkCacheDir: prefetchDir},
      ),
    ).toBe('success');
    const db = new Database(lc, restoreFile);
    const {watermark} = getSubscriptionState(new StatementRunner(db));
    db.close();
    expect(watermark).toBe('05');
  });
});
