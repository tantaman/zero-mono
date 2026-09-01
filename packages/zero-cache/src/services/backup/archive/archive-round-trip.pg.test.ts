import {mkdtempSync, rmSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {LogContext} from '@rocicorp/logger';
import {beforeEach, describe, expect, vi} from 'vitest';
import {TestLogSink} from '../../../../../shared/src/logging-test-utils.ts';
import {Queue} from '../../../../../shared/src/queue.ts';
import {Database} from '../../../../../zqlite/src/db.ts';
import {StatementRunner} from '../../../db/statements.ts';
import {
  dropReplicationSlots,
  getConnectionURI,
  type PgTest,
  test,
} from '../../../test/db.ts';
import {DbFile} from '../../../test/lite.ts';
import type {PostgresDB} from '../../../types/pg.ts';
import type {Source} from '../../../types/streams.ts';
import type {
  ChangeSource,
  ChangeStream,
} from '../../change-source/change-source.ts';
import {initializePostgresChangeSource} from '../../change-source/pg/change-source.ts';
import type {ChangeStreamMessage} from '../../change-source/protocol/current/downstream.ts';
import {serializeChangeStreamData} from '../../change-streamer/change-log-codec.ts';
import {ChangeProcessor} from '../../replicator/change-processor.ts';
import {publishBase} from '../base/base-publisher.ts';
import {runArchiveDrill} from '../drill/archive-drill.ts';
import {archiveRestore} from '../restore/archive-restore.ts';
import {InMemoryObjectStore} from '../test-utils.ts';
import {listLogSegments} from './archive-reader.ts';
import {ArchiveWriter} from './archive-writer.ts';
import {decodeSegment} from './segment-format.ts';

const APP_ID = 'art';
const SHARD_NUM = 0;
const SHARD = {appID: APP_ID, publications: ['zero_data'], shardNum: SHARD_NUM};

/**
 * The archive subsystem's tests are otherwise built entirely on synthetic
 * transactions (`wireTransaction`), which construct only the fields the
 * protocol schema names. A real change source puts more than that on the
 * wire — the Postgres source's `begin`/`commit` carry `commitLsn`,
 * `commitTime` and `xid` — so a whole class of defect is invisible to
 * fixtures built from the schema alone. The strict-parse regression that
 * made every real segment undecodable on replay is one instance: the entire
 * synthetic suite stayed green through it.
 *
 * This test closes that gap by putting genuine pgoutput through the whole
 * archive path — writer, seal, upload, replay, restore — and comparing the
 * restored replica against the Postgres state it came from.
 */
describe('backup/archive real-Postgres round trip', {timeout: 60000}, () => {
  let lc: LogContext;
  let upstream: PostgresDB;
  let upstreamURI: string;
  let liveReplica: DbFile;
  let scratchFile: string;
  let scratchDir: string;
  let spoolDir: string;
  let store: InMemoryObjectStore;
  let source: ChangeSource;
  let streams: ChangeStream[];

  beforeEach<PgTest>(async ({testDBs}) => {
    lc = new LogContext('error', {}, new TestLogSink());
    upstream = await testDBs.create('archive_round_trip_upstream');
    upstreamURI = getConnectionURI(upstream);
    liveReplica = new DbFile('archive_round_trip_live');
    scratchDir = mkdtempSync(join(tmpdir(), 'zero-archive-rt-scratch-'));
    spoolDir = mkdtempSync(join(tmpdir(), 'zero-archive-rt-spool-'));
    scratchFile = join(scratchDir, 'restored.db');
    store = new InMemoryObjectStore();
    streams = [];

    await upstream.unsafe(`
      CREATE TABLE data(
        id            TEXT CONSTRAINT data_pk PRIMARY KEY,
        n             INT8,
        flag          BOOL,
        note          TEXT,
        payload       JSONB
      );
      INSERT INTO data(id, n, flag, note, payload) VALUES
        ('a', 1, true,  'first',  '{"k":1}'),
        ('b', 2, false, 'second', '{"k":2}');
      CREATE PUBLICATION zero_data FOR TABLE data;
    `);

    return async () => {
      streams.forEach(s => s.changes.cancel());
      await dropReplicationSlots(upstream);
      await upstream.end();
      liveReplica.delete();
      rmSync(scratchDir, {recursive: true, force: true});
      rmSync(spoolDir, {recursive: true, force: true});
    };
  });

  function drainToQueue(
    sub: Source<ChangeStreamMessage>,
  ): Queue<ChangeStreamMessage> {
    const queue = new Queue<ChangeStreamMessage>();
    void (async () => {
      for await (const msg of sub) {
        if (msg[0] === 'status') {
          continue;
        }
        queue.enqueue(msg);
      }
    })();
    return queue;
  }

  test('real pgoutput survives writer, seal, replay and restore', async () => {
    const {changeSource, subscriptionState} =
      await initializePostgresChangeSource(
        lc,
        upstreamURI,
        SHARD,
        liveReplica.path,
        {tableCopyWorkers: 1},
        {test: 'archive-round-trip'},
        0,
        {},
      );
    source = changeSource;
    const {replicaVersion, watermark: initialWatermark} = subscriptionState;

    // The lineage's first base, from the initially-synced replica. Everything
    // after it reaches the scratch replica only by way of the archived log.
    await publishBase(lc, store, liveReplica.path, {
      chunkBytes: 4 * 1024 * 1024,
      integrityCheck: 'quick',
    });

    const writer = new ArchiveWriter(lc, {
      store,
      replicaVersion,
      spoolDir,
      segmentTargetBytes: 1, // seal every commit, so nothing waits on a timer
      partTargetBytes: 8 * 1024 * 1024,
      sealIntervalMs: 30_000,
    });
    await writer.reconcile(initialWatermark);

    const stream = await source.startStream(initialWatermark);
    streams.push(stream);
    const downstream = drainToQueue(stream.changes);

    // Genuine changes: an insert, an update, a delete, and values that the
    // synthetic fixtures never produce (bigint past 2^53, multibyte text,
    // NULL, jsonb).
    await upstream.unsafe(`
      INSERT INTO data(id, n, flag, note, payload)
        VALUES ('c', 9007199254740993, true, 'ünïcödé ✓', '{"k":3}');
    `);
    await upstream.unsafe(`
      UPDATE data SET n = 42, note = NULL, payload = '{"k":22}' WHERE id = 'b';
    `);
    await upstream.unsafe(`DELETE FROM data WHERE id = 'a';`);

    // Feed the wire straight into the archive, exactly as the change-streamer's
    // fourth consumer does, and stop once all three transactions are durable.
    const EXPECTED_TXS = 3;
    let commits = 0;
    let lastWatermark = initialWatermark;
    while (commits < EXPECTED_TXS) {
      const msg = await downstream.dequeue();
      if (msg[0] === 'begin' || msg[0] === 'data' || msg[0] === 'commit') {
        // The archive stores exactly the bytes the storer produced, so the
        // test must serialize the way production does -- real pgoutput
        // carries BigInts, which plain JSON.stringify cannot represent.
        writer.write(msg, serializeChangeStreamData(msg));
        if (msg[0] === 'commit') {
          commits++;
          lastWatermark = msg[2].watermark;
        }
      }
    }
    await vi.waitFor(() =>
      expect(writer.state().durableWatermark).toBe(lastWatermark),
    );
    await writer.close();

    // The segments carry the change source's own fields, which the protocol
    // schema does not name. Decoding at all is the assertion; the field check
    // pins *why* a strict parse would have rejected them.
    const segments = await listLogSegments(store, replicaVersion);
    expect(segments.length).toBeGreaterThan(0);
    const first = decodeSegment(await store.get(segments[0].key));
    const begin = first.transactions[0].messages[0];
    expect(begin[0]).toBe('begin');
    expect(begin[1]).toMatchObject({tag: 'begin'});
    expect(Object.keys(begin[1])).toEqual(
      expect.arrayContaining(['commitLsn', 'commitTime', 'xid']),
    );

    // Restore a replica from nothing but the base and the archived log.
    const result = await archiveRestore(
      lc,
      store,
      scratchFile,
      {replicaVersion, minWatermark: ''},
      {mode: 'backup'},
    );
    expect(result).toBe('success');

    // ... and it must agree with Postgres, which is the property the whole
    // subsystem exists to provide.
    const restored = new Database(lc, scratchFile);
    try {
      const rows = restored
        .prepare('SELECT id, n, flag, note, payload FROM data ORDER BY id')
        // `n` exceeds 2^53; reading it as a JS number would round it here in
        // the test and hide whatever the pipeline actually delivered.
        .safeIntegers(true)
        .all() as {
        id: string;
        n: number | bigint;
        flag: number | bigint;
        note: string | null;
        payload: string;
      }[];
      expect(rows.map(r => r.id)).toEqual(['b', 'c']);
      expect(String(rows[0].n)).toBe('42');
      expect(rows[0]).toMatchObject({flag: 0n, note: null});
      expect(rows[1]).toMatchObject({flag: 1n, note: 'ünïcödé ✓'});
      // The bigint survives the pg -> wire -> segment -> lite path intact.
      expect(String(rows[1].n)).toBe('9007199254740993');

      const upstreamRows = await upstream<
        {id: string; n: string; note: string | null}[]
      >`SELECT id, n::text, note FROM data ORDER BY id`;
      expect(rows.map(r => r.id)).toEqual(upstreamRows.map(r => r.id));
      expect(rows.map(r => String(r.n))).toEqual(upstreamRows.map(r => r.n));
      expect(rows.map(r => r.note)).toEqual(upstreamRows.map(r => r.note));
    } finally {
      restored.close();
    }
  });
  test('an archive-restored replica matches one the live applier built', async () => {
    // The strongest statement the subsystem can make: two replicas at the
    // same watermark, one built by the applier reading the change stream
    // directly and one rebuilt from a base plus the archived log, agree
    // row for row. Everything the archive does that the live path does not
    // -- serialize, seal, compress, upload, verify, decode -- has to be
    // lossless for this to hold, which is why the bigint rounding showed up
    // as a wrong value rather than an error.
    const {changeSource, subscriptionState} =
      await initializePostgresChangeSource(
        lc,
        upstreamURI,
        SHARD,
        liveReplica.path,
        {tableCopyWorkers: 1},
        {test: 'archive-drill-fidelity'},
        0,
        {},
      );
    source = changeSource;
    const {replicaVersion, watermark: initialWatermark} = subscriptionState;

    await publishBase(lc, store, liveReplica.path, {
      chunkBytes: 4 * 1024 * 1024,
      integrityCheck: 'quick',
    });

    const writer = new ArchiveWriter(lc, {
      store,
      replicaVersion,
      spoolDir,
      segmentTargetBytes: 1,
      partTargetBytes: 8 * 1024 * 1024,
      sealIntervalMs: 30_000,
    });
    await writer.reconcile(initialWatermark);

    const stream = await source.startStream(initialWatermark);
    streams.push(stream);
    const downstream = drainToQueue(stream.changes);

    // The live replica, applied exactly as `replayTail` applies the archived
    // one: same ChangeProcessor, same 'backup' mode. The only difference
    // between the two sides is the trip through the archive.
    const liveDB = new Database(lc, liveReplica.path);
    let applyFailure: unknown;
    const live = new ChangeProcessor(
      new StatementRunner(liveDB),
      'backup',
      (_, err) => (applyFailure = err),
    );

    await upstream.unsafe(`
      INSERT INTO data(id, n, flag, note, payload)
        VALUES ('c', 9007199254740993, true, 'ünïcödé ✓', '{"k":3}');
    `);
    await upstream.unsafe(
      `UPDATE data SET n = -9007199254740995, note = NULL WHERE id = 'b';`,
    );
    await upstream.unsafe(`DELETE FROM data WHERE id = 'a';`);
    await upstream.unsafe(`ALTER TABLE data ADD COLUMN extra TEXT;`);
    await upstream.unsafe(
      `UPDATE data SET extra = 'after the schema change' WHERE id = 'c';`,
    );
    // The sentinel bounds the drain below without having to predict how many
    // transactions the DDL above turns into.
    await upstream.unsafe(
      `INSERT INTO data(id, n, flag, note, payload, extra)
         VALUES ('sentinel', 0, false, 'done', '{}', 'done');`,
    );

    const sawSentinel = () =>
      (
        liveDB
          .prepare(`SELECT count(*) AS c FROM data WHERE id = 'sentinel'`)
          .get() as {c: number}
      ).c > 0;

    let lastWatermark = initialWatermark;
    for (;;) {
      const msg = await downstream.dequeue();
      if (msg[0] !== 'begin' && msg[0] !== 'data' && msg[0] !== 'commit') {
        continue;
      }
      writer.write(msg, serializeChangeStreamData(msg));
      live.processMessage(lc, msg);
      if (applyFailure !== undefined) {
        throw applyFailure;
      }
      if (msg[0] === 'commit') {
        lastWatermark = msg[2].watermark;
        if (sawSentinel()) {
          break;
        }
      }
    }

    await vi.waitFor(() =>
      expect(writer.state().durableWatermark).toBe(lastWatermark),
    );
    await writer.close();
    liveDB.close();

    // The drill is the comparator the archive world schedules in production;
    // this is the first time it runs over anything but synthetic fixtures.
    const result = await runArchiveDrill(lc, store, liveReplica.path, {
      scratchDir: join(scratchDir, 'drill'),
      archiveWaitTimeoutMs: 10_000,
      pollIntervalMs: 50,
    });
    expect(result).toMatchObject({
      outcome: 'match',
      replicaVersion,
      watermark: lastWatermark,
    });
  });
});
