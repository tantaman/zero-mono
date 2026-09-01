import {mkdtempSync, rmSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {afterEach, beforeEach, describe, expect, test} from 'vitest';
import {createSilentLogContext} from '../../../../../shared/src/logging-test-utils.ts';
import {
  PROTOCOL_VERSION,
  type SubscriberContext,
} from '../../change-streamer/change-streamer.ts';
import * as ErrorType from '../../change-streamer/error-type-enum.ts';
import {InMemoryObjectStore, wireTransaction} from '../test-utils.ts';
import {ArchiveChangeSource} from './archive-change-source.ts';
import {segmentKey} from './layout.ts';
import {encodeSegment} from './segment-format.ts';

const lc = createSilentLogContext();

describe('backup/archive/archive-change-source', () => {
  let store: InMemoryObjectStore;
  let tempDir: string;
  let source: ArchiveChangeSource;

  beforeEach(() => {
    store = new InMemoryObjectStore();
    tempDir = mkdtempSync(join(tmpdir(), 'zero-archive-change-source-test-'));
    source = new ArchiveChangeSource(lc, {
      store,
      replicaVersion: '02',
      pollIntervalMs: 2,
      tempDir,
    });
  });

  afterEach(() => {
    rmSync(tempDir, {recursive: true, force: true});
  });

  async function putSegment(start: string, watermarks: string[]) {
    const {data, end} = encodeSegment({
      replicaVersion: '02',
      start,
      transactions: watermarks.map(w => wireTransaction(w)),
    });
    await store.putIfAbsent(segmentKey('02', start, end), data);
  }

  function ctx(watermark: string, replicaVersion = '02'): SubscriberContext {
    return {
      protocolVersion: PROTOCOL_VERSION,
      taskID: 'task',
      id: 'test-subscriber',
      mode: 'backup',
      replicaVersion,
      watermark,
      initial: true,
      logsChangeStream: false,
    };
  }

  /** Collects messages until `count` have arrived, then cancels. */
  async function take(
    sub: AsyncIterable<{data: unknown; json: string}> & {
      cancel: () => void;
    },
    count: number,
  ) {
    const messages: {data: unknown; json: string}[] = [];
    for await (const message of sub) {
      messages.push(message);
      if (messages.length >= count) {
        sub.cancel();
      }
    }
    return messages;
  }

  test('opens with status, catches up, and follows newly sealed segments', async () => {
    await putSegment('02', ['03']);
    const sub = await source.subscribe(ctx('02'));

    const tx03 = wireTransaction('03');
    const expectFirst = take(sub, 1 + 3 + 3); // status + tx 03 + tx 05

    // The live tail: a segment sealed after subscription.
    await putSegment('03', ['05']);
    const tx05 = wireTransaction('05');

    const messages = await expectFirst;
    expect(messages[0].data).toEqual(['status', {tag: 'status'}]);
    expect(messages.slice(1).map(m => m.data)).toEqual([
      ...tx03.parsed,
      ...tx05.parsed,
    ]);
    // The exact archived JSON is preserved, never re-serialized.
    expect(messages.slice(1).map(m => m.json)).toEqual([
      ...tx03.messages,
      ...tx05.messages,
    ]);
  });

  test('filters transactions at or below the subscription watermark', async () => {
    await putSegment('02', ['03', '05']);
    await putSegment('05', ['07']);
    const sub = await source.subscribe(ctx('05'));
    const messages = await take(sub, 1 + 3);
    expect(messages.slice(1).map(m => m.data)).toEqual(
      wireTransaction('07').parsed,
    );
  });

  test('a subscriber on the wrong lineage gets WrongReplicaVersion', async () => {
    const sub = await source.subscribe(ctx('02', '09'));
    const messages = await take(sub, 1);
    expect(messages[0].data).toEqual([
      'error',
      {
        type: ErrorType.WrongReplicaVersion,
        message: expect.stringContaining('lineage is 02'),
      },
    ]);
  });

  test('a watermark below the archived history gets WatermarkTooOld', async () => {
    await putSegment('05', ['07']);
    const sub = await source.subscribe(ctx('03'));
    const messages = await take(sub, 2);
    expect(messages[0].data).toEqual(['status', {tag: 'status'}]);
    expect(messages[1].data).toEqual([
      'error',
      {
        type: ErrorType.WatermarkTooOld,
        message: expect.stringContaining('below the archived history'),
      },
    ]);
  });

  test('an empty archive polls until content appears', async () => {
    const sub = await source.subscribe(ctx('02'));
    const collected = take(sub, 1 + 3);
    await new Promise(resolve => setTimeout(resolve, 10));
    await putSegment('02', ['03']);
    const messages = await collected;
    expect(messages.slice(1).map(m => m.data)).toEqual(
      wireTransaction('03').parsed,
    );
  });

  test('a transient store failure fails the stream (the syncer resubscribes)', async () => {
    await putSegment('02', ['03']);
    store.list = () => Promise.reject(new Error('injected listing failure'));
    const sub = await source.subscribe(ctx('02'));
    await expect(take(sub, 2)).rejects.toThrow('injected listing failure');
  });
});
