import {resolver} from '@rocicorp/resolver';
import {beforeEach, describe, expect, test, vi} from 'vitest';
import {createSilentLogContext} from '../../../../../shared/src/logging-test-utils.ts';
import {must} from '../../../../../shared/src/must.ts';
import {InMemoryObjectStore, wireTransaction} from '../test-utils.ts';
import {ArchiveWriter, type ArchiveWriterOptions} from './archive-writer.ts';
import {segmentKey} from './layout.ts';
import {decodeSegment} from './segment-format.ts';

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
  let disabled: number;
  let timers: ReturnType<typeof fakeTimers>;

  beforeEach(() => {
    store = new InMemoryObjectStore();
    durable = [];
    disabled = 0;
    timers = fakeTimers();
  });

  function newWriter(opts: Partial<ArchiveWriterOptions> = {}) {
    return new ArchiveWriter(lc, {
      store,
      replicaVersion: '02',
      authoritative: false,
      segmentTargetBytes: 1, // seal at every commit unless overridden
      sealIntervalMs: 30_000,
      onDurable: watermark => durable.push(watermark),
      onDisabled: () => disabled++,
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

  test('authoritative: retries uploads with backoff until success', async () => {
    let failures = 3;
    store.beforePut = () => {
      if (failures > 0) {
        failures--;
        throw new Error('injected upload failure');
      }
    };
    const writer = newWriter({authoritative: true});
    await writer.reconcile('02');
    writeTransaction(writer, '03');

    // Each failed attempt schedules a backoff timer; fire them as they come.
    for (let i = 0; i < 3; i++) {
      await until(() => timers.scheduled.size === 1);
      timers.fire();
    }
    await until(() => durable.length === 1);
    expect(durable).toEqual(['03']);
    expect(writer.enabled).toBe(true);
    expect(disabled).toBe(0);
  });

  test('dual: fails soft after bounded upload attempts', async () => {
    store.beforePut = () => {
      throw new Error('injected upload failure');
    };
    const writer = newWriter({authoritative: false});
    await writer.reconcile('02');
    writeTransaction(writer, '03');

    for (let i = 0; i < 4; i++) {
      await until(() => timers.scheduled.size === 1);
      timers.fire();
    }
    await until(() => !writer.enabled);
    expect(disabled).toBe(1);
    expect(durable).toEqual([]);
    expect(writer.state().bufferedBytes).toBe(0);

    // Disabled writers ignore further writes.
    writeTransaction(writer, '05');
    expect(writer.state().bufferedBytes).toBe(0);
  });

  test('authoritative: applies back-pressure while uploads are stalled', async () => {
    const gate = resolver();
    store.beforePut = () => gate.promise;
    const writer = newWriter({
      authoritative: true,
      maxBufferedBytes: 10,
    });
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

  test('dual: never applies back-pressure; overflowing the buffer fails soft', async () => {
    const gate = resolver();
    store.beforePut = () => gate.promise;
    // Room for one buffered transaction but not two.
    const txBytes = wireTransaction('03').messages.reduce(
      (sum, m) => sum + m.length,
      0,
    );
    const writer = newWriter({maxBufferedBytes: txBytes + 5});
    await writer.reconcile('02');

    writeTransaction(writer, '03');
    expect(writer.enabled).toBe(true);
    expect(writer.readyForMore()).toBeUndefined();
    writeTransaction(writer, '05');
    expect(writer.enabled).toBe(false);
    expect(disabled).toBe(1);
    gate.resolve();
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

  test('writes before reconcile are refused', () => {
    const writer = newWriter({authoritative: false});
    writeTransaction(writer, '03');
    expect(writer.enabled).toBe(false);
    expect(disabled).toBe(1);
  });
});
