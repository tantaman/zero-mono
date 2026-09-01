import {LogContext} from '@rocicorp/logger';
import {beforeEach, describe, expect} from 'vitest';
import {TestLogSink} from '../../../../../shared/src/logging-test-utils.ts';
import {must} from '../../../../../shared/src/must.ts';
import {sleep} from '../../../../../shared/src/sleep.ts';
import {
  dropReplicationSlots,
  getConnectionURI,
  type PgTest,
  test,
} from '../../../test/db.ts';
import {DbFile} from '../../../test/lite.ts';
import type {PostgresDB} from '../../../types/pg.ts';
import {publishBase} from '../../backup/base/base-publisher.ts';
import {
  genesisOfferKey,
  readGenesisOffer,
  writeGenesisHeartbeat,
} from '../../backup/genesis.ts';
import type {ObjectStore} from '../../backup/object-store/object-store.ts';
import {InMemoryObjectStore} from '../../backup/test-utils.ts';
import {initChangeStreamerSchema} from '../../change-streamer/schema/init.ts';
import {initReplica} from '../common/replica-schema.ts';
import {initializeArchiveModeChangeSource} from './change-source.ts';
import {initialSync} from './initial-sync.ts';
import {getReplicaAtVersion} from './schema/shard.ts';

const APP_ID = 'agen';
const SHARD_NUM = 0;
const SHARD = {appID: APP_ID, publications: ['zero_foo'], shardNum: SHARD_NUM};

/**
 * The gateway side of lineage genesis (backup mode `archive`): the gateway
 * creates the slot, offers its exported snapshot through the archive, and
 * blocks until a base producer publishes the lineage's first base.
 *
 * The producer here is a stand-in for `base-producer.ts` doing exactly what
 * that worker does — poll for the offer, run the real initial sync against
 * the offered snapshot into its own working file, heartbeat, then publish
 * the result as the first base. Everything the gateway must get right about
 * the handoff is observable from that.
 */
describe('change-source/pg archive-mode genesis', {timeout: 60000}, () => {
  let lc: LogContext;
  let upstream: PostgresDB;
  let upstreamURI: string;
  let changeDB: PostgresDB;
  let store: ObjectStore;
  let producerFile: DbFile;

  beforeEach<PgTest>(async ({testDBs}) => {
    lc = new LogContext('error', {}, new TestLogSink());
    upstream = await testDBs.create('archive_genesis_upstream');
    changeDB = await testDBs.create('archive_genesis_change');
    upstreamURI = getConnectionURI(upstream);
    store = new InMemoryObjectStore();
    producerFile = new DbFile('archive_genesis_producer');

    await upstream.unsafe(`
      CREATE TABLE foo(id TEXT CONSTRAINT foo_pk PRIMARY KEY, val TEXT);
      INSERT INTO foo(id, val) VALUES ('a', 'one'), ('b', 'two');
      CREATE PUBLICATION zero_foo FOR TABLE foo;
    `);

    return async () => {
      await dropReplicationSlots(upstream);
      await upstream.end();
      await changeDB.end();
      producerFile.delete();
    };
  });

  /**
   * Stands in for the base-producer worker: waits for the gateway's offer,
   * copies at the offered snapshot, and publishes the first base.
   */
  async function runProducer(replicaVersionSeen: (v: string) => void) {
    for (;;) {
      const keys = await store.list('v1/');
      const offerKey = keys.find(o => o.key.endsWith('genesis/offer.json'));
      if (offerKey) {
        // v1/<replicaVersion>/genesis/offer.json
        const replicaVersion = offerKey.key.split('/')[1];
        const offer = must(await readGenesisOffer(store, replicaVersion));
        expect(offer.replicaVersion).toBe(replicaVersion);
        expect(genesisOfferKey(replicaVersion)).toBe(offerKey.key);
        replicaVersionSeen(replicaVersion);

        await writeGenesisHeartbeat(store, replicaVersion, 'producer');
        await initReplica(
          lc,
          'archive-genesis-test-producer',
          producerFile.path,
          (log, tx) =>
            initialSync(
              log,
              SHARD,
              tx,
              upstreamURI,
              {
                tableCopyWorkers: 1,
                textCopy: false,
                providedSnapshot: {
                  snapshotID: offer.snapshotID,
                  lsn: offer.lsn,
                },
              },
              {test: 'producer'},
            ).then(() => {}),
        );
        await publishBase(lc, store, producerFile.path, {
          chunkBytes: 1024 * 1024,
          integrityCheck: 'quick',
        });
        return;
      }
      await sleep(20);
    }
  }

  test('publishes a usable replica record once the producer lands the first base', async () => {
    // The change-streamer worker initializes the change DB before touching
    // the change source; the gateway reads its stored config from there.
    await initChangeStreamerSchema(lc, changeDB, SHARD);

    let offeredVersion: string | undefined;
    const producer = runProducer(v => {
      offeredVersion = v;
    });

    const result = await initializeArchiveModeChangeSource(
      lc,
      upstreamURI,
      SHARD,
      changeDB,
      {tableCopyWorkers: 1, textCopy: false},
      {test: 'gateway'},
      {store, taskID: 'gateway', genesisHeartbeatTimeoutMs: 30_000},
    );
    await producer;

    const replicaVersion = result.subscriptionState.replicaVersion;
    expect(offeredVersion).toBe(replicaVersion);

    // The gateway owns the `replicas` record, and the producer's initial
    // sync deliberately does not write it. Until the gateway records what
    // was synced, the row has a null `initialSyncContext` and
    // `getReplicaAtVersion` filters it out -- genesis then fails with
    // "genesis created no replica", which is the regression this pins.
    const replica = await getReplicaAtVersion(
      lc,
      upstream,
      SHARD,
      replicaVersion,
    );
    expect(replica).not.toBeNull();
    // `replicaSchema` folds `version` into `generation`.
    expect(must(replica).generation).toBe(replicaVersion);
    expect(must(replica).initialSyncContext).toEqual({test: 'gateway'});

    // `initialSchema` is the schema actually copied, read at the offered
    // snapshot rather than after the handoff.
    const tables = must(replica).initialSchema?.tables ?? [];
    expect(tables.map(t => t.name)).toContain('foo');

    // And the gateway itself never built a replica file: the lineage's only
    // materialization is the base the producer published.
    const bases = await store.list(`v1/${replicaVersion}/base/`);
    expect(bases.some(o => o.key.endsWith('complete.json'))).toBe(true);
  });
});
