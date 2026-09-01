import {describe, expect, test, vi} from 'vitest';
import {createSilentLogContext} from '../../../../shared/src/logging-test-utils.ts';
import {baseCompleteKey} from '../backup/archive/layout.ts';
import {
  encodeBaseManifest,
  type BaseManifest,
} from '../backup/base/manifest.ts';
import {InMemoryObjectStore} from '../backup/test-utils.ts';
import {UnrecoverableError} from '../running-state.ts';
import {ArchiveWatermarkPoller} from './archive-watermark-poller.ts';
import type {BackedUpWatermark} from './backup-monitor.ts';

const lc = createSilentLogContext();

function manifest(cursor: string, completedAt: number): BaseManifest {
  return {
    format: 'zero-archive-base',
    version: 1,
    replicaVersion: '02',
    cursor,
    fileSize: 1,
    fileSha256: '00',
    chunkBytes: 1,
    chunks: [{size: 1, sha256: '00'}],
    completedAt,
  };
}

async function putBase(
  store: InMemoryObjectStore,
  cursor: string,
  completedAt = Date.now(),
) {
  await store.putIfAbsent(
    baseCompleteKey('02', cursor),
    encodeBaseManifest(manifest(cursor, completedAt)),
  );
}

describe('change-streamer/archive-watermark-poller', () => {
  test('emits the newest complete base cursor as it advances', async () => {
    const store = new InMemoryObjectStore();
    await putBase(store, '05', 123);
    const poller = new ArchiveWatermarkPoller(lc, {
      store,
      replicaVersion: '02',
      staleBaseGraceMs: 24 * 3600 * 1000,
      checkIntervalMs: 5,
    });
    const stream = poller.start();
    const seen: BackedUpWatermark[] = [];
    const collecting = (async () => {
      for await (const wm of stream) {
        seen.push(wm);
        if (seen.length === 2) {
          stream.cancel();
        }
      }
    })();

    await vi.waitFor(() => expect(seen.length).toBeGreaterThanOrEqual(1));
    expect(seen[0]).toEqual({watermark: '05', backupTimeMs: 123});

    await putBase(store, '0g', 456);
    await collecting;
    expect(seen[1]).toEqual({watermark: '0g', backupTimeMs: 456});
  });

  test('fails the stream when no base appears by the initial deadline', async () => {
    const store = new InMemoryObjectStore();
    const poller = new ArchiveWatermarkPoller(lc, {
      store,
      replicaVersion: '02',
      staleBaseGraceMs: 24 * 3600 * 1000,
      checkIntervalMs: 5,
      initialBaseDeadlineMs: 20,
    });
    const stream = poller.start();
    await expect(
      (async () => {
        for await (const _ of stream) {
          // no watermarks expected
        }
      })(),
    ).rejects.toThrow(UnrecoverableError);
  });

  test('a stale newest base keeps the floor without failing the stream', async () => {
    const store = new InMemoryObjectStore();
    await putBase(store, '05', Date.now() - 3600 * 1000);
    const poller = new ArchiveWatermarkPoller(lc, {
      store,
      replicaVersion: '02',
      staleBaseGraceMs: 60_000, // an hour-old base is well past this
      checkIntervalMs: 5,
    });
    const stream = poller.start();
    const seen: BackedUpWatermark[] = [];
    for await (const wm of stream) {
      seen.push(wm);
      // Give it a few more polls in the stale state, then stop.
      setTimeout(() => stream.cancel(), 25);
    }
    expect(seen.map(w => w.watermark)).toEqual(['05']);
  });
});
