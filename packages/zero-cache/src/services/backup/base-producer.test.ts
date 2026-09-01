import {existsSync, mkdtempSync, rmSync, writeFileSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {afterEach, beforeEach, describe, expect, test, vi} from 'vitest';
import {createSilentLogContext} from '../../../../shared/src/logging-test-utils.ts';
import type {LogConfig} from '../../../../shared/src/logging.ts';
import {Database} from '../../../../zqlite/src/db.ts';
import {StatementRunner} from '../../db/statements.ts';
import type {ChangeStreamData} from '../change-source/protocol/current/downstream.ts';
import {
  ChangeProcessor,
  type ChangeProcessorMode,
  type CommitResult,
} from '../replicator/change-processor.ts';
import {
  getSubscriptionState,
  initReplicationState,
  type SubscriptionState,
} from '../replicator/schema/replication-state.ts';
import {
  applyPragmas,
  type PragmaConfig,
} from '../replicator/write-worker-client.ts';
import {baseRequestKey, segmentKey} from './archive/layout.ts';
import {encodeSegment} from './archive/segment-format.ts';
import {
  BaseProducerService,
  type BaseProducerOptions,
} from './base-producer.ts';
import {publishBase} from './base/base-publisher.ts';
import {decodeBaseManifest, encodeBaseRequest} from './base/manifest.ts';
import {InMemoryObjectStore, wireTransaction} from './test-utils.ts';

const lc = createSilentLogContext();

const logConfig: LogConfig = {
  format: 'text',
  level: 'error',
};

/**
 * The write worker, in-process: the same Database + ChangeProcessor wiring
 * as `write-worker.ts`, without the worker thread (vitest cannot host one
 * over TS sources).
 */
class InProcessWorkerClient {
  #db: Database | undefined;
  #processor: ChangeProcessor | undefined;
  #onError: (err: Error) => void = () => {};

  init(
    dbPath: string,
    mode: ChangeProcessorMode,
    pragmas: PragmaConfig,
    _logConfig: LogConfig,
  ): Promise<void> {
    this.#db = new Database(lc, dbPath);
    applyPragmas(this.#db, pragmas);
    this.#processor = new ChangeProcessor(
      new StatementRunner(this.#db),
      mode,
      (_, err) => this.#onError(err as Error),
    );
    return Promise.resolve();
  }

  getSubscriptionState(): Promise<SubscriptionState> {
    return Promise.resolve(
      getSubscriptionState(new StatementRunner(this.#db!)),
    );
  }

  processMessage(downstream: ChangeStreamData): Promise<CommitResult | null> {
    return Promise.resolve(this.#processor!.processMessage(lc, downstream));
  }

  abort(): void {
    this.#processor?.abort(lc);
  }

  stop(): Promise<void> {
    this.#db?.close();
    this.#db = undefined;
    return Promise.resolve();
  }

  onError(handler: (err: Error) => void): void {
    this.#onError = handler;
  }
}

async function until(cond: () => boolean | Promise<boolean>, timeoutMs = 5000) {
  await vi.waitFor(async () => expect(await cond()).toBe(true), {
    timeout: timeoutMs,
    interval: 10,
  });
}

describe('backup/base-producer', () => {
  let dir: string;
  let store: InMemoryObjectStore;
  let producerFile: string;

  beforeEach(() => {
    dir = mkdtempSync(join(tmpdir(), 'zero-base-producer-test-'));
    store = new InMemoryObjectStore();
    producerFile = join(dir, 'producer.db');
  });

  afterEach(() => {
    rmSync(dir, {recursive: true, force: true});
  });

  /** Seeds the store with a first base at watermark '02' (lineage '02'). */
  async function seedGenesisBase() {
    const sourceFile = join(dir, 'seed.db');
    const db = new Database(lc, sourceFile);
    initReplicationState(db, ['zero_data'], '02');
    db.exec(
      `CREATE TABLE issues(issueID TEXT PRIMARY KEY, val TEXT, _0_version TEXT)`,
    );
    db.close();
    await publishBase(lc, store, sourceFile, {
      chunkBytes: 4096,
      integrityCheck: 'full',
    });
  }

  async function putSegment(start: string, watermarks: string[]) {
    const {data, end} = encodeSegment({
      replicaVersion: '02',
      start,
      transactions: watermarks.map(w => wireTransaction(w, 1, 1000)),
    });
    await store.putIfAbsent(segmentKey('02', start, end), data);
  }

  function newProducer(opts: Partial<BaseProducerOptions> = {}) {
    return new BaseProducerService(lc, {
      taskID: 'task',
      store,
      replicaFile: producerFile,
      pollIntervalMs: 5,
      checkIntervalMs: 25,
      baseMaxIntervalMs: 1, // publish whenever anything applied
      baseMaxReplayMs: 3600 * 1000,
      chunkBytes: 4096,
      integrityCheck: 'full',
      gc: null,
      logConfig,
      createWorkerClient: () => new InProcessWorkerClient(),
      ...opts,
    });
  }

  function completeBases(): string[] {
    return Array.from(
      store.objects.keys(),
      key => /^v1\/02\/base\/([0-9a-z]+)\/complete\.json$/.exec(key)?.[1],
    )
      .filter((c): c is string => c !== undefined)
      .toSorted();
  }

  test('restores, tails, and publishes bases as the archive advances', async () => {
    await seedGenesisBase();
    await putSegment('02', ['03', '05']);

    const producer = newProducer();
    const running = producer.run();
    try {
      await until(() => completeBases().includes('05'));
      // The seeded tail was applied by the restore itself; the base absorbs
      // it right away.
      expect(producer.state()).toMatchObject({
        lineage: '02',
        lastBaseCursor: '05',
        lastAppliedWatermark: '05',
      });
      // The published base carries the producer metadata.
      const manifest = decodeBaseManifest(
        store.objects.get('v1/02/base/05/complete.json')!,
      );
      expect(manifest.pageSize).toBeGreaterThan(0);
      expect(manifest.logFormatVersion).toBe(2);

      // The live tail: more segments seal, the next base follows, and the
      // in-session apply measurements track the delivered stream.
      await putSegment('05', ['07']);
      await until(() => completeBases().includes('07'));
      expect(producer.state()).toMatchObject({
        lastBaseCursor: '07',
        lastAppliedWatermark: '07',
        lastAppliedCommitTimeMs: 1000,
      });
    } finally {
      await producer.stop();
      await running;
    }

    // A clean shutdown leaves the marker; the working file survives restarts.
    expect(existsSync(`${producerFile}.producer-clean`)).toBe(true);

    // The published base is restorable and contains the applied rows.
    const check = new Database(lc, producerFile);
    const {watermark} = getSubscriptionState(new StatementRunner(check));
    expect(watermark).toBe('07');
    const rows = check
      .prepare(`SELECT issueID FROM issues ORDER BY issueID`)
      .all<{issueID: string}>();
    check.close();
    expect(rows.map(r => r.issueID)).toEqual(['03-0', '05-0', '07-0']);
  });

  test('waits when the archive is empty or has no base yet', async () => {
    const producer = newProducer({checkIntervalMs: 10});
    const running = producer.run();
    try {
      await new Promise(resolve => setTimeout(resolve, 50));
      expect(existsSync(producerFile)).toBe(false);

      // A lineage with segments but no complete base is still pre-genesis.
      await putSegment('02', ['03']);
      await new Promise(resolve => setTimeout(resolve, 50));
      expect(completeBases()).toEqual([]);

      // The first (genesis) base appears; production begins.
      await seedGenesisBase();
      await until(() => completeBases().includes('03'));
    } finally {
      await producer.stop();
      await running;
    }
  });

  test('an unclean start discards the working file and rebuilds from its own base', async () => {
    await seedGenesisBase();
    await putSegment('02', ['03']);
    {
      const producer = newProducer();
      const running = producer.run();
      await until(() => completeBases().includes('03'));
      await producer.stop();
      await running;
    }

    // Simulate a crash: delete the clean marker and corrupt the file.
    rmSync(`${producerFile}.producer-clean`);
    writeFileSync(producerFile, 'garbage');

    await putSegment('03', ['05']);
    const producer = newProducer();
    const running = producer.run();
    try {
      await until(() => completeBases().includes('05'));
    } finally {
      await producer.stop();
      await running;
    }
  });

  test('serves accelerated live-base requests', async () => {
    await seedGenesisBase();
    const producer = newProducer({
      // Cadence never fires; only requests can trigger publication.
      baseMaxIntervalMs: 3600 * 1000,
      checkIntervalMs: 15,
    });
    const running = producer.run();
    try {
      // The restore replayed nothing (no segments), so the '02' base is
      // current: a request is answered by consuming the marker, no new base.
      await until(() => producer.state().lastAppliedWatermark === '02');
      await store.put(
        baseRequestKey('02', 'restorer-1'),
        encodeBaseRequest({
          format: 'zero-archive-base-request',
          version: 1,
          taskID: 'restorer-1',
          requestedAt: Date.now(),
        }),
      );
      await until(() => !store.objects.has(baseRequestKey('02', 'restorer-1')));
      expect(completeBases()).toEqual(['02']);

      // With new content applied, a request triggers an immediate publish
      // and the marker is consumed by it.
      await putSegment('02', ['03']);
      await until(() => producer.state().lastAppliedWatermark === '03');
      await store.put(
        baseRequestKey('02', 'restorer-2'),
        encodeBaseRequest({
          format: 'zero-archive-base-request',
          version: 1,
          taskID: 'restorer-2',
          requestedAt: Date.now(),
        }),
      );
      await until(() => completeBases().includes('03'));
      await until(() => !store.objects.has(baseRequestKey('02', 'restorer-2')));
    } finally {
      await producer.stop();
      await running;
    }
  });

  test('runs GC after publication', async () => {
    await seedGenesisBase();
    await putSegment('02', ['03']);
    await putSegment('03', ['05']);

    const producer = newProducer({
      gc: {retainBases: 2, pitrHours: 0, debrisGraceHours: 24},
    });
    const running = producer.run();
    try {
      // Publications at '05' (and possibly '03' first); once two newer
      // bases exist, GC reclaims the seeded '02' base and covered segments.
      await until(() => completeBases().includes('05'));
      await putSegment('05', ['07']);
      await until(() => completeBases().includes('07'));
      await until(() => !completeBases().includes('02'));
    } finally {
      await producer.stop();
      await running;
    }
  });
});
