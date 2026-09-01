import {describe, expect, test} from 'vitest';
import {createSilentLogContext} from '../../../../shared/src/logging-test-utils.ts';
import {
  awaitGenesisBase,
  encodeGenesisOffer,
  genesisHeartbeatKey,
  genesisOfferKey,
  readGenesisOffer,
  writeGenesisHeartbeat,
  type GenesisOffer,
} from './genesis.ts';
import {InMemoryObjectStore} from './test-utils.ts';

const lc = createSilentLogContext();

function offer(replicaVersion = '02'): GenesisOffer {
  return {
    format: 'zero-archive-genesis-offer',
    version: 1,
    replicaVersion,
    snapshotID: '00000003-000001A8-1',
    lsn: '0/1A82F58',
    taskID: 'gateway-task',
    offeredAt: Date.now(),
  };
}

describe('backup/genesis', () => {
  test('an offer round-trips through the store', async () => {
    const store = new InMemoryObjectStore();
    expect(await readGenesisOffer(store, '02')).toBeUndefined();

    const posted = offer();
    await store.putIfAbsent(genesisOfferKey('02'), encodeGenesisOffer(posted));
    expect(await readGenesisOffer(store, '02')).toEqual(posted);
  });

  test('resolves published when the first base lands, and cleans up', async () => {
    const store = new InMemoryObjectStore();
    await store.putIfAbsent(genesisOfferKey('02'), encodeGenesisOffer(offer()));

    // A "producer": heartbeats, then publishes the first complete base.
    const producer = (async () => {
      await writeGenesisHeartbeat(store, '02', 'producer-task');
      await new Promise(resolve => setTimeout(resolve, 20));
      await store.putIfAbsent(
        'v1/02/base/05/complete.json',
        new Uint8Array([1]),
      );
    })();

    const result = await awaitGenesisBase(lc, store, '02', {
      heartbeatTimeoutMs: 5_000,
      pollIntervalMs: 5,
    });
    await producer;
    expect(result).toBe('published');
    expect(store.objects.has(genesisOfferKey('02'))).toBe(false);
    expect(store.objects.has(genesisHeartbeatKey('02'))).toBe(false);
  });

  test('abandons a genesis whose heartbeats stop, withdrawing the offer', async () => {
    const store = new InMemoryObjectStore();
    await store.putIfAbsent(genesisOfferKey('02'), encodeGenesisOffer(offer()));
    await writeGenesisHeartbeat(
      store,
      '02',
      'producer-task',
      Date.now() - 60_000,
    );

    const result = await awaitGenesisBase(lc, store, '02', {
      heartbeatTimeoutMs: 50,
      pollIntervalMs: 5,
    });
    expect(result).toBe('abandoned');
    expect(store.objects.has(genesisOfferKey('02'))).toBe(false);
  });

  test('fresh heartbeats keep the wait alive past the timeout window', async () => {
    const store = new InMemoryObjectStore();
    await store.putIfAbsent(genesisOfferKey('02'), encodeGenesisOffer(offer()));

    // Heartbeat every 20ms against a 60ms timeout; publish after 150ms —
    // well past the window that would have expired without heartbeats.
    const interval = setInterval(() => {
      void writeGenesisHeartbeat(store, '02', 'producer-task');
    }, 20);
    const publish = setTimeout(() => {
      void store.putIfAbsent('v1/02/base/05/complete.json', new Uint8Array());
    }, 150);
    try {
      const result = await awaitGenesisBase(lc, store, '02', {
        heartbeatTimeoutMs: 60,
        pollIntervalMs: 5,
      });
      expect(result).toBe('published');
    } finally {
      clearInterval(interval);
      clearTimeout(publish);
    }
  });
});
