import {describe, expect, test, vi} from 'vitest';
import {createSilentLogContext} from '../../../../shared/src/logging-test-utils.ts';
import {segmentKey, type SegmentRef} from './archive/layout.ts';
import {encodeSegment} from './archive/segment-format.ts';
import {
  encodeBaseIntent,
  encodeBaseManifest,
  type BaseManifest,
} from './base/manifest.ts';
import {computeGCPlan, runArchiveGC} from './gc.ts';
import {InMemoryObjectStore, wireTransaction} from './test-utils.ts';

const lc = createSilentLogContext();
const HOUR = 3600 * 1000;
const NOW = 100 * HOUR;

function seg(start: string, end: string): SegmentRef {
  return {key: segmentKey('02', start, end), start, end};
}

describe('backup/gc computeGCPlan', () => {
  const base = (cursor: string, hoursAgo: number) => ({
    cursor,
    completedAt: NOW - hoursAgo * HOUR,
  });

  test('retains everything while there are at most retainBases bases', () => {
    const plan = computeGCPlan(
      {
        bases: [base('05', 50), base('0g', 40)],
        segments: [seg('02', '05'), seg('05', '0g'), seg('0g', '0k')],
      },
      {retainBases: 2, pitrHours: 1},
      NOW,
    );
    expect(plan.retainedBaseCursors).toEqual(['05', '0g']);
    expect(plan.deletedBaseCursors).toEqual([]);
    // The floor is the oldest retained base: segments below it are
    // reclaimable even when no base is.
    expect(plan.segmentFloor).toBe('05');
    expect(plan.deletedSegments).toEqual([seg('02', '05')]);
  });

  test('deletes bases beyond retainBases and outside the PITR window', () => {
    const plan = computeGCPlan(
      {
        bases: [base('05', 50), base('0g', 40), base('0k', 2), base('0m', 1)],
        segments: [
          seg('02', '05'),
          seg('05', '0g'),
          seg('0g', '0k'),
          seg('0k', '0m'),
          seg('0m', '0n'),
        ],
      },
      {retainBases: 2, pitrHours: 4},
      NOW,
    );
    // '0k' and '0m' are the newest two; '0g' is the newest base completed
    // before the 4-hour PITR window and is needed for its left edge; '05'
    // serves no window and goes.
    expect(plan.retainedBaseCursors).toEqual(['0g', '0k', '0m']);
    expect(plan.deletedBaseCursors).toEqual(['05']);
    expect(plan.segmentFloor).toBe('0g');
    expect(plan.deletedSegments).toEqual([seg('02', '05'), seg('05', '0g')]);
  });

  test('a wide PITR window retains every base it needs', () => {
    const plan = computeGCPlan(
      {
        bases: [base('05', 50), base('0g', 40), base('0k', 2), base('0m', 1)],
        segments: [],
      },
      {retainBases: 2, pitrHours: 45},
      NOW,
    );
    expect(plan.retainedBaseCursors).toEqual(['05', '0g', '0k', '0m']);
    expect(plan.deletedBaseCursors).toEqual([]);
  });

  test('a segment straddling the floor is kept', () => {
    const plan = computeGCPlan(
      {
        bases: [base('06', 50), base('0g', 1)],
        // '06' falls inside (05, 0g]: that segment still serves the retained
        // base at '06' and must survive.
        segments: [seg('02', '05'), seg('05', '0g'), seg('0g', '0k')],
      },
      {retainBases: 1, pitrHours: 100},
      NOW,
    );
    expect(plan.segmentFloor).toBe('06');
    expect(plan.deletedSegments).toEqual([seg('02', '05')]);
  });

  test('an empty inventory plans nothing', () => {
    const plan = computeGCPlan(
      {bases: [], segments: [seg('02', '05')]},
      {retainBases: 2, pitrHours: 1},
      NOW,
    );
    expect(plan.segmentFloor).toBeUndefined();
    expect(plan.deletedSegments).toEqual([]);
    expect(plan.deletedBaseCursors).toEqual([]);
  });
});

describe('backup/gc runArchiveGC', () => {
  async function putBase(
    store: InMemoryObjectStore,
    cursor: string,
    completedAt: number,
    {complete = true, chunkCount = 2, startedAt = completedAt} = {},
  ) {
    await store.putIfAbsent(
      `v1/02/base/${cursor}/intent.json`,
      encodeBaseIntent({
        format: 'zero-archive-base',
        version: 1,
        replicaVersion: '02',
        cursor,
        startedAt,
      }),
    );
    for (let i = 0; i < chunkCount; i++) {
      await store.putIfAbsent(
        `v1/02/base/${cursor}/chunk/${String(i).padStart(8, '0')}`,
        new Uint8Array([i]),
      );
    }
    if (complete) {
      const manifest: BaseManifest = {
        format: 'zero-archive-base',
        version: 1,
        replicaVersion: '02',
        cursor,
        fileSize: chunkCount,
        fileSha256: '00',
        chunkBytes: 1,
        chunks: Array.from({length: chunkCount}, () => ({
          size: 1,
          sha256: '00',
        })),
        completedAt,
      };
      await store.putIfAbsent(
        `v1/02/base/${cursor}/complete.json`,
        encodeBaseManifest(manifest),
      );
    }
  }

  async function putSegment(store: InMemoryObjectStore, ref: SegmentRef) {
    const {data} = encodeSegment({
      replicaVersion: '02',
      start: ref.start,
      transactions: [wireTransaction(ref.end)],
    });
    await store.putIfAbsent(ref.key, data);
  }

  test('collects expired bases, their segments, and stale debris', async () => {
    const store = new InMemoryObjectStore();
    await putBase(store, '05', NOW - 50 * HOUR);
    // Old enough to cover the 4-hour PITR window's left edge, so '05' is not
    // needed for it.
    await putBase(store, '0g', NOW - 6 * HOUR);
    await putBase(store, '0k', NOW - 1 * HOUR);
    // A crashed publication older than the grace period, and a fresh one.
    await putBase(store, '07', NOW - 30 * HOUR, {
      complete: false,
      startedAt: NOW - 30 * HOUR,
    });
    await putBase(store, '0m', NOW, {complete: false, startedAt: NOW});
    for (const ref of [seg('02', '05'), seg('05', '0g'), seg('0g', '0k')]) {
      await putSegment(store, ref);
    }

    const result = await runArchiveGC(
      lc,
      store,
      '02',
      {retainBases: 2, pitrHours: 4},
      NOW,
    );
    expect(result.retainedBaseCursors).toEqual(['0g', '0k']);
    expect(result.deletedBaseCursors).toEqual(['05']);
    expect(result.deletedDebrisCursors).toEqual(['07']);
    expect(result.deletedSegments).toEqual([seg('02', '05'), seg('05', '0g')]);

    const remaining = [...store.objects.keys()].toSorted();
    expect(remaining).toEqual([
      'v1/02/base/0g/chunk/00000000',
      'v1/02/base/0g/chunk/00000001',
      'v1/02/base/0g/complete.json',
      'v1/02/base/0g/intent.json',
      'v1/02/base/0k/chunk/00000000',
      'v1/02/base/0k/chunk/00000001',
      'v1/02/base/0k/complete.json',
      'v1/02/base/0k/intent.json',
      // The fresh incomplete publication is left alone.
      'v1/02/base/0m/chunk/00000000',
      'v1/02/base/0m/chunk/00000001',
      'v1/02/base/0m/intent.json',
      'v1/02/log/0g-0k.seg',
    ]);
  });

  test('deletes a base manifest-first, so interruption never fakes a complete base', async () => {
    const store = new InMemoryObjectStore();
    await putBase(store, '05', NOW - 50 * HOUR);
    await putBase(store, '0g', NOW - 2 * HOUR);
    await putBase(store, '0k', NOW - 1 * HOUR);

    const deletions: string[] = [];
    vi.spyOn(store, 'delete').mockImplementation(key => {
      deletions.push(key);
      store.objects.delete(key);
      return Promise.resolve();
    });
    await runArchiveGC(lc, store, '02', {retainBases: 2, pitrHours: 1}, NOW);

    const baseDeletions = deletions.filter(key =>
      key.startsWith('v1/02/base/05/'),
    );
    expect(baseDeletions[0]).toBe('v1/02/base/05/complete.json');
    expect(baseDeletions.at(-1)).toBe('v1/02/base/05/intent.json');
  });

  test('a quiescent archive collects nothing', async () => {
    const store = new InMemoryObjectStore();
    await putBase(store, '0g', NOW - 2 * HOUR);
    await putBase(store, '0k', NOW - 1 * HOUR);
    await putSegment(store, seg('0g', '0k'));
    const before = [...store.objects.keys()];

    const result = await runArchiveGC(
      lc,
      store,
      '02',
      {retainBases: 2, pitrHours: 4},
      NOW,
    );
    expect(result.deletedBaseCursors).toEqual([]);
    expect(result.deletedSegments).toEqual([]);
    expect(result.deletedDebrisCursors).toEqual([]);
    expect([...store.objects.keys()]).toEqual(before);
  });
});
