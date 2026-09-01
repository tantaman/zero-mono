import {mkdtempSync, readdirSync, rmSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {zstdDecompressSync} from 'node:zlib';
import {resolver} from '@rocicorp/resolver';
import {afterEach, beforeEach, describe, expect, test, vi} from 'vitest';
import {createSilentLogContext} from '../../../../../shared/src/logging-test-utils.ts';
import {must} from '../../../../../shared/src/must.ts';
import {InMemoryObjectStore, wireTransaction} from '../test-utils.ts';
import {iterateMessages} from './archive-reader.ts';
import {ArchiveWriter, type ArchiveWriterOptions} from './archive-writer.ts';
import {segmentKey} from './layout.ts';
import {decodeSegment, type SegmentHeader} from './segment-format.ts';

/** The parsed header line of an encoded segment object. */
function decodeSegmentFileHeader(data: Uint8Array): SegmentHeader {
  const payload = zstdDecompressSync(data.subarray(37)).toString('utf8');
  return JSON.parse(payload.split('\n')[0]) as SegmentHeader;
}

const lc = createSilentLogContext();

/**
 * A manually-fired timer registry standing in for setTimeout/clearTimeout,
 * so seal-interval and retry-backoff behavior is deterministic.
 */
function fakeTimers() {
  let nextId = 1;
  const scheduled = new Map<number, {fn: () => void; delay: number}>();
  return {
    scheduled,
    setTimeoutFn: ((fn: () => void, delay: number) => {
      const id = nextId++;
      scheduled.set(id, {fn, delay});
      return id;
    }) as unknown as typeof setTimeout,
    clearTimeoutFn: ((id: number) => {
      scheduled.delete(id);
    }) as unknown as typeof clearTimeout,
    fire: () => {
      const entries = [...scheduled.entries()];
      scheduled.clear();
      for (const [, {fn}] of entries) {
        fn();
      }
    },
  };
}

async function until(cond: () => boolean) {
  await vi.waitFor(() => expect(cond()).toBe(true));
}

describe('backup/archive/archive-writer', () => {
  let store: InMemoryObjectStore;
  let durable: string[];
  let timers: ReturnType<typeof fakeTimers>;
  let spoolDir: string;

  beforeEach(() => {
    store = new InMemoryObjectStore();
    durable = [];
    timers = fakeTimers();
    spoolDir = mkdtempSync(join(tmpdir(), 'zero-archive-writer-test-'));
  });

  afterEach(() => {
    rmSync(spoolDir, {recursive: true, force: true});
  });

  function newWriter(opts: Partial<ArchiveWriterOptions> = {}) {
    return new ArchiveWriter(lc, {
      store,
      replicaVersion: '02',
      spoolDir,
      segmentTargetBytes: 1, // seal at every commit unless overridden
      partTargetBytes: 8 * 1024 * 1024, // no part chains unless overridden
      sealIntervalMs: 30_000,
      onDurable: watermark => durable.push(watermark),
      setTimeoutFn: timers.setTimeoutFn,
      clearTimeoutFn: timers.clearTimeoutFn,
      ...opts,
    });
  }

  function writeTransaction(
    writer: ArchiveWriter,
    watermark: string,
    rows = 1,
  ) {
    const {parsed, messages} = wireTransaction(watermark, rows);
    parsed.forEach((change, i) => writer.write(change, messages[i]));
  }

  test('seals and uploads on the size threshold', async () => {
    const writer = newWriter();
    await writer.reconcile('02');
    writeTransaction(writer, '03');
    writeTransaction(writer, '05');
    await until(() => durable.length === 2);

    expect(durable).toEqual(['03', '05']);
    expect([...store.objects.keys()]).toEqual([
      segmentKey('02', '02', '03'),
      segmentKey('02', '03', '05'),
    ]);
    const first = decodeSegment(
      store.objects.get(segmentKey('02', '02', '03'))!,
    );
    expect(first.transactions.map(t => t.watermark)).toEqual(['03']);
    expect(first.transactions[0].messages).toEqual(
      wireTransaction('03').parsed,
    );
    expect(writer.state()).toMatchObject({
      enabled: true,
      durableWatermark: '05',
      bufferedBytes: 0,
      gapsDetected: 0,
    });
  });

  test('accumulates transactions until the threshold', async () => {
    const writer = newWriter({segmentTargetBytes: 1024 * 1024});
    await writer.reconcile('02');
    writeTransaction(writer, '03');
    writeTransaction(writer, '05');
    expect(store.objects.size).toBe(0); // buffered, not sealed

    writeTransaction(writer, '07', 10_000); // pushes past the threshold
    await until(() => durable.length === 1);
    expect(durable).toEqual(['07']);
    expect([...store.objects.keys()]).toEqual([segmentKey('02', '02', '07')]);
    const segment = decodeSegment(
      store.objects.get(segmentKey('02', '02', '07'))!,
    );
    expect(segment.transactions.map(t => t.watermark)).toEqual([
      '03',
      '05',
      '07',
    ]);
  });

  test('seals on the timer', async () => {
    const writer = newWriter({segmentTargetBytes: 1024 * 1024});
    await writer.reconcile('02');
    writeTransaction(writer, '03');
    expect(store.objects.size).toBe(0);
    expect(timers.scheduled.size).toBe(1);

    timers.fire();
    await until(() => durable.length === 1);
    expect([...store.objects.keys()]).toEqual([segmentKey('02', '02', '03')]);
  });

  test('spool and sealed files are reclaimed once segments are durable', async () => {
    const writer = newWriter();
    await writer.reconcile('02');
    writeTransaction(writer, '03');
    writeTransaction(writer, '05');
    await until(() => durable.length === 2);

    // Each boundary seal rotated to a fresh spool file; everything sealed
    // and uploaded has been deleted. Only the current (empty) spool remains.
    expect(readdirSync(spoolDir)).toEqual(['000002.spool']);
    await writer.close();
  });

  test('a timer seal mid-transaction archives the committed prefix without disturbing the open transaction', async () => {
    const writer = newWriter({segmentTargetBytes: 1024 * 1024});
    await writer.reconcile('02');
    writeTransaction(writer, '03');

    const {parsed, messages} = wireTransaction('05', 2);
    // begin + first data row, then the seal timer fires mid-transaction.
    writer.write(parsed[0], messages[0]);
    writer.write(parsed[1], messages[1]);
    timers.fire();
    await until(() => durable.length === 1);
    expect([...store.objects.keys()]).toEqual([segmentKey('02', '02', '03')]);

    // The rest of the transaction commits and seals on the next timer.
    writer.write(parsed[2], messages[2]);
    writer.write(parsed[3], messages[3]);
    timers.fire();
    await until(() => durable.length === 2);
    const segment = decodeSegment(
      store.objects.get(segmentKey('02', '03', '05'))!,
    );
    expect(segment.transactions.map(t => t.watermark)).toEqual(['05']);
    expect(segment.transactions[0].messages).toEqual(parsed);
  });

  test('an oversized transaction spans a part chain', async () => {
    const writer = newWriter({
      segmentTargetBytes: 1024 * 1024,
      partTargetBytes: 200,
    });
    await writer.reconcile('02');
    writeTransaction(writer, '03'); // committed, below the segment target
    const big = wireTransaction('05', 10); // ~1KB: spans multiple parts
    big.parsed.forEach((change, i) => writer.write(change, big.messages[i]));
    writeTransaction(writer, '07');
    timers.fire(); // seal the trailing segment
    await until(() => durable.includes('07'));

    // The committed prefix sealed as an ordinary segment before the chain
    // started; interior parts list immediately before their final part.
    const keys = [...store.objects.keys()];
    expect(keys[0]).toBe(segmentKey('02', '02', '03'));
    expect(keys.at(-2)).toBe(segmentKey('02', '03', '05'));
    expect(keys.at(-1)).toBe(segmentKey('02', '05', '07'));
    const interior = keys.slice(1, -2);
    expect(interior.length).toBeGreaterThanOrEqual(2);
    interior.forEach((key, i) => {
      expect(key).toBe(
        `v1/02/log/03-.05.${String(i + 1).padStart(8, '0')}.seg`,
      );
    });
    // Only the final part advanced the durable cursor.
    expect(durable).toEqual(['03', '05', '07']);

    // The streaming reader reassembles the chain byte-for-byte.
    const items = [];
    for await (const item of iterateMessages(
      store,
      '02',
      '02',
      '07',
      spoolDir,
    )) {
      items.push(item.message);
    }
    expect(items).toEqual([
      ...wireTransaction('03').parsed,
      ...big.parsed,
      ...wireTransaction('07').parsed,
    ]);
  });

  test('a rollback mid-chain abandons the transaction and archiving continues', async () => {
    const writer = newWriter({
      segmentTargetBytes: 1024 * 1024,
      partTargetBytes: 200,
    });
    await writer.reconcile('02');
    const big = wireTransaction('05', 10);
    // begin + data (chaining), then a rollback instead of the commit.
    for (let i = 0; i < big.parsed.length - 1; i++) {
      writer.write(big.parsed[i], big.messages[i]);
    }
    writer.write(
      ['rollback', {tag: 'rollback'}],
      '["rollback",{"tag":"rollback"}]',
    );

    writeTransaction(writer, '07');
    timers.fire();
    await until(() => durable.includes('07'));
    // Any uploaded interior parts are debris (their chain has no final);
    // continuity and replay see only the completed segment.
    const finals = [...store.objects.keys()].filter(key =>
      /^v1\/02\/log\/[0-9a-z]+-[0-9a-z]+\.seg$/.test(key),
    );
    expect(finals).toEqual([segmentKey('02', '02', '07')]);
    const items = [];
    for await (const item of iterateMessages(
      store,
      '02',
      '02',
      '07',
      spoolDir,
    )) {
      items.push(item.message);
    }
    expect(items).toEqual(wireTransaction('07').parsed);

    // A later incarnation's reconcile deletes the abandoned chain's debris.
    await writer.close();
    const next = newWriter();
    await next.reconcile('07');
    expect([...store.objects.keys()]).toEqual([segmentKey('02', '02', '07')]);
  });

  test('segment headers carry the upstream commit timestamps', async () => {
    const writer = newWriter({segmentTargetBytes: 1024 * 1024});
    await writer.reconcile('02');
    for (const [watermark, timeMs] of [
      ['03', 1000],
      ['05', 2000],
      ['07', 3000],
    ] as const) {
      const {parsed, messages} = wireTransaction(watermark, 1, timeMs);
      parsed.forEach((change, i) => writer.write(change, messages[i]));
    }
    timers.fire();
    await until(() => durable.includes('07'));

    const segment = decodeSegmentFileHeader(
      store.objects.get(segmentKey('02', '02', '07'))!,
    );
    expect(segment).toMatchObject({
      firstCommitTimeMs: 1000,
      lastCommitTimeMs: 3000,
      txCount: 3,
      part: null,
    });
  });

  test('a rolled-back or aborted transaction is not archived', async () => {
    const writer = newWriter();
    await writer.reconcile('02');
    const {parsed, messages} = wireTransaction('03');
    // begin + data, then a rollback instead of the commit.
    writer.write(parsed[0], messages[0]);
    writer.write(parsed[1], messages[1]);
    writer.write(
      ['rollback', {tag: 'rollback'}],
      '["rollback",{"tag":"rollback"}]',
    );

    writeTransaction(writer, '05');
    await until(() => durable.length === 1);
    const segment = decodeSegment(
      store.objects.get(segmentKey('02', '02', '05'))!,
    );
    expect(segment.transactions.map(t => t.watermark)).toEqual(['05']);
  });

  test('reconcile resumes after the durable head and filters replays', async () => {
    {
      const writer = newWriter();
      await writer.reconcile('02');
      writeTransaction(writer, '03');
      writeTransaction(writer, '05');
      await until(() => durable.length === 2);
      await writer.close();
    }

    // A new incarnation resumes from the last ACK ('03'), which trails the
    // archive: the replayed '05' must be filtered, and archiving continues
    // contiguously from '05'.
    durable = [];
    const writer = newWriter();
    await writer.reconcile('03');
    expect(durable).toEqual(['05']); // the durable head is re-announced
    expect(writer.state()).toMatchObject({
      durableWatermark: '05',
      gapsDetected: 0,
    });

    writeTransaction(writer, '05');
    writeTransaction(writer, '07');
    await until(() => durable.includes('07'));
    expect([...store.objects.keys()]).toEqual([
      segmentKey('02', '02', '03'),
      segmentKey('02', '03', '05'),
      segmentKey('02', '05', '07'),
    ]);
  });

  test('an empty archive starts its lineage at the resume point', async () => {
    const writer = newWriter();
    await writer.reconcile('0g');
    expect(writer.state().gapsDetected).toBe(0);

    writeTransaction(writer, '0k');
    await until(() => durable.length === 1);
    expect([...store.objects.keys()]).toEqual([segmentKey('02', '0g', '0k')]);
  });

  test('a resume point past the durable head is a gap', async () => {
    {
      const writer = newWriter();
      await writer.reconcile('02');
      writeTransaction(writer, '03');
      await until(() => durable.length === 1);
      await writer.close();
    }

    const writer = newWriter();
    await writer.reconcile('0g'); // '03'..'0g' will never be archived
    expect(writer.state().gapsDetected).toBe(1);

    writeTransaction(writer, '0k');
    await until(() => store.objects.size === 2);
    // The new segment names its true range, so the gap is detectable from
    // the listing alone.
    expect([...store.objects.keys()]).toEqual([
      segmentKey('02', '02', '03'),
      segmentKey('02', '0g', '0k'),
    ]);
  });

  test('a non-contiguous listing is reported as a gap', async () => {
    await store.putIfAbsent(segmentKey('02', '02', '03'), new Uint8Array());
    await store.putIfAbsent(segmentKey('02', '05', '0g'), new Uint8Array());

    const writer = newWriter();
    await writer.reconcile('0g');
    expect(writer.state()).toMatchObject({
      durableWatermark: '0g',
      gapsDetected: 1,
    });
  });

  test('a segment already uploaded by a previous incarnation counts as durable', async () => {
    // A crash between a previous incarnation's upload and its ACK: this
    // incarnation re-seals the same range under the same deterministic name,
    // and finds the object already present.
    store.beforePut = key => {
      store.beforePut = undefined;
      store.objects.set(key, new Uint8Array());
    };
    const writer = newWriter();
    await writer.reconcile('02');
    writeTransaction(writer, '03');

    await until(() => durable.length === 1);
    expect(durable).toEqual(['03']);
    expect(writer.enabled).toBe(true);
  });

  test('retries failed uploads with backoff until success (fail-stall)', async () => {
    let failures = 3;
    store.beforePut = () => {
      if (failures > 0) {
        failures--;
        throw new Error('injected upload failure');
      }
    };
    const writer = newWriter();
    await writer.reconcile('02');
    writeTransaction(writer, '03');

    // The durable cursor stalls across the failures — nothing is reported
    // durable until the upload lands.
    expect(durable).toEqual([]);
    // Each failed attempt schedules a backoff timer; fire them as they come.
    for (let i = 0; i < 3; i++) {
      await until(() => timers.scheduled.size === 1);
      expect(durable).toEqual([]);
      timers.fire();
    }
    await until(() => durable.length === 1);
    expect(durable).toEqual(['03']);
    expect(writer.enabled).toBe(true);
  });

  test('applies back-pressure while uploads are stalled', async () => {
    const gate = resolver();
    store.beforePut = () => gate.promise;
    const writer = newWriter({maxBufferedBytes: 10});
    await writer.reconcile('02');
    expect(writer.readyForMore()).toBeUndefined();

    writeTransaction(writer, '03');
    writeTransaction(writer, '05');
    const backpressure = must(writer.readyForMore());

    let released = false;
    void backpressure.then(() => (released = true));
    gate.resolve();
    await until(() => released);
    await until(() => durable.length === 2);
  });

  test('close seals and flushes buffered transactions', async () => {
    const writer = newWriter({segmentTargetBytes: 1024 * 1024});
    await writer.reconcile('02');
    writeTransaction(writer, '03');
    writeTransaction(writer, '05');
    expect(store.objects.size).toBe(0);

    await writer.close();
    expect([...store.objects.keys()]).toEqual([segmentKey('02', '02', '05')]);
    expect(durable).toEqual(['05']);
    expect(writer.enabled).toBe(false);
  });

  test('a write before reconcile is a stream-invariant violation', () => {
    // The throw propagates to the stream loop, whose retry path reconnects
    // and reconciles.
    const writer = newWriter();
    expect(() => writeTransaction(writer, '03')).toThrow('before reconcile()');
  });
});
