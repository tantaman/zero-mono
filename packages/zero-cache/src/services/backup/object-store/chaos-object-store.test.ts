import {mkdtempSync, rmSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {beforeEach, describe, expect, test, vi} from 'vitest';
import {createSilentLogContext} from '../../../../../shared/src/logging-test-utils.ts';
import {
  contiguousHeadFrom,
  listLogSegments,
} from '../archive/archive-reader.ts';
import {ArchiveWriter} from '../archive/archive-writer.ts';
import {InMemoryObjectStore, wireTransaction} from '../test-utils.ts';
import {
  ChaosInjectedError,
  ChaosObjectStore,
  chaosRng,
  composePolicies,
  outage,
  randomFaults,
  type ChaosDecision,
} from './chaos-object-store.ts';
import {ObjectAlreadyExistsError} from './object-store.ts';

const lc = createSilentLogContext();

describe('backup/object-store/chaos-object-store', () => {
  let inner: InMemoryObjectStore;

  beforeEach(() => {
    inner = new InMemoryObjectStore();
  });

  const data = new Uint8Array([1, 2, 3]);

  test('pass decisions are transparent', async () => {
    const chaos = new ChaosObjectStore(inner, () => ({kind: 'pass'}));
    await chaos.putIfAbsent('v1/a', data);
    expect(await chaos.get('v1/a')).toEqual(data);
    expect(await chaos.head('v1/a')).toMatchObject({key: 'v1/a', size: 3});
    expect(await chaos.list('v1/')).toHaveLength(1);
    await chaos.delete('v1/a');
    expect(await chaos.head('v1/a')).toBeUndefined();
    expect(chaos.stats).toEqual({ops: 6, errors: 0, errorsAfter: 0, delays: 0});
  });

  test('an error decision fails before reaching the store', async () => {
    const chaos = new ChaosObjectStore(inner, () => ({kind: 'error'}));
    await expect(chaos.putIfAbsent('v1/a', data)).rejects.toThrow(
      ChaosInjectedError,
    );
    expect(inner.objects.size).toBe(0);
    expect(chaos.stats.errors).toBe(1);
  });

  test('error-after performs the operation, then loses the response', async () => {
    let decision: ChaosDecision = {kind: 'error-after'};
    const chaos = new ChaosObjectStore(inner, () => decision);

    await expect(chaos.putIfAbsent('v1/a', data)).rejects.toThrow(
      ChaosInjectedError,
    );
    // The write landed; a deterministic-name retry observes it as already
    // present — exactly the idempotent-retry path the archive relies on.
    expect(inner.objects.has('v1/a')).toBe(true);
    decision = {kind: 'pass'};
    await expect(chaos.putIfAbsent('v1/a', data)).rejects.toThrow(
      ObjectAlreadyExistsError,
    );
  });

  test('a delay defers and then applies its inner decision', async () => {
    const chaos = new ChaosObjectStore(inner, () => ({
      kind: 'delay',
      ms: 20,
      then: {kind: 'error'},
    }));
    const start = performance.now();
    await expect(chaos.get('v1/a')).rejects.toThrow(ChaosInjectedError);
    expect(performance.now() - start).toBeGreaterThanOrEqual(15);
    expect(chaos.stats).toMatchObject({delays: 1, errors: 1});
  });

  test('an outage window fails everything until it lifts', async () => {
    let down = true;
    const chaos = new ChaosObjectStore(
      inner,
      outage(() => down),
    );
    await expect(chaos.list('v1/')).rejects.toThrow(ChaosInjectedError);
    down = false;
    expect(await chaos.list('v1/')).toEqual([]);
  });

  test('composePolicies takes the first non-pass decision', () => {
    const policy = composePolicies(
      () => ({kind: 'pass'}),
      () => ({kind: 'error-after'}),
      () => ({kind: 'error'}),
    );
    expect(policy('get', 'k')).toEqual({kind: 'error-after'});
  });

  test('chaosRng is deterministic for a seed', () => {
    const a = chaosRng(42);
    const b = chaosRng(42);
    const values = Array.from({length: 5}, a);
    expect(Array.from({length: 5}, b)).toEqual(values);
    for (const v of values) {
      expect(v).toBeGreaterThanOrEqual(0);
      expect(v).toBeLessThan(1);
    }
  });

  test('the archive writer drives every segment durable through chaos', async () => {
    const dir = mkdtempSync(join(tmpdir(), 'zero-chaos-writer-test-'));
    // Faults on uploads only: errors before, "lost responses" after, both at
    // rates that fire many times across the run. Fail-stall plus
    // deterministic names must still make the whole stream durable with no
    // gaps.
    const faults = randomFaults({
      rng: chaosRng(1234),
      errorRate: 0.3,
      errorAfterRate: 0.2,
    });
    const chaos = new ChaosObjectStore(inner, (op, key) =>
      op === 'putStreamIfAbsent' ? faults(op, key) : {kind: 'pass'},
    );
    const writer = new ArchiveWriter(lc, {
      store: chaos,
      replicaVersion: '02',
      spoolDir: join(dir, 'spool'),
      segmentTargetBytes: 1, // seal every transaction
      partTargetBytes: 8 * 1024 * 1024,
      sealIntervalMs: 60_000,
      retryDelayMs: 1,
    });
    try {
      await writer.reconcile('02');
      const watermarks = ['03', '05', '07', '09', '0b', '0d', '0f'];
      for (const watermark of watermarks) {
        for (const message of wireTransaction(watermark, 2).parsed) {
          writer.write(message, JSON.stringify(message));
        }
      }
      await vi.waitFor(
        () => expect(writer.state().durableWatermark).toBe('0f'),
        {timeout: 10_000, interval: 5},
      );
      // The archive is contiguous in the underlying store, and the faults
      // actually fired.
      const segments = await listLogSegments(inner, '02');
      expect(contiguousHeadFrom(segments, '02')).toBe('0f');
      expect(writer.state().gapsDetected).toBe(0);
      expect(chaos.stats.errors + chaos.stats.errorsAfter).toBeGreaterThan(0);
    } finally {
      await writer.close();
      rmSync(dir, {recursive: true, force: true});
    }
  });
});
