import {existsSync, mkdtempSync, rmSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {beforeEach, afterEach, describe, expect, test} from 'vitest';
import {createSilentLogContext} from '../../../../../shared/src/logging-test-utils.ts';
import {must} from '../../../../../shared/src/must.ts';
import {Database} from '../../../../../zqlite/src/db.ts';
import {StatementRunner} from '../../../db/statements.ts';
import type {ChangeStreamData} from '../../change-source/protocol/current/downstream.ts';
import {ChangeProcessor} from '../../replicator/change-processor.ts';
import {
  getSubscriptionState,
  initReplicationState,
} from '../../replicator/schema/replication-state.ts';
import {ArchiveWriter} from '../archive/archive-writer.ts';
import {segmentKey} from '../archive/layout.ts';
import {publishBase} from '../base/base-publisher.ts';
import {InMemoryObjectStore, WIRE_RELATION} from '../test-utils.ts';
import {archiveRestore} from './archive-restore.ts';

const lc = createSilentLogContext();

/**
 * The full restore drill from the implementation plan: build a source
 * replica through the real change processor while dual-writing the stream to
 * the archive, freeze and publish a base mid-stream, keep streaming, then
 * restore into a scratch path and diff logical content against the source.
 */
describe('backup/restore/archive-restore', () => {
  let dir: string;
  let sourceFile: string;
  let restoreFile: string;
  let store: InMemoryObjectStore;
  let source: SourceReplica;

  beforeEach(async () => {
    dir = mkdtempSync(join(tmpdir(), 'zero-archive-restore-test-'));
    sourceFile = join(dir, 'source.db');
    restoreFile = join(dir, 'restored.db');
    store = new InMemoryObjectStore();
    source = new SourceReplica(sourceFile, store);
    await source.open('02');
  });

  afterEach(async () => {
    await source.close();
    rmSync(dir, {recursive: true, force: true});
  });

  function transaction(
    watermark: string,
    ...changes: Record<string, unknown>[]
  ): ChangeStreamData[] {
    return [
      ['begin', {tag: 'begin'}, {commitWatermark: watermark}],
      ...changes.map(change => ['data', change] as ChangeStreamData),
      ['commit', {tag: 'commit'}, {watermark}],
    ];
  }

  const insert = (issueID: string, val: string) => ({
    tag: 'insert',
    relation: WIRE_RELATION,
    new: {issueID, val},
  });
  const update = (issueID: string, val: string) => ({
    tag: 'update',
    relation: WIRE_RELATION,
    new: {issueID, val},
    key: null,
  });
  const del = (issueID: string) => ({
    tag: 'delete',
    relation: WIRE_RELATION,
    key: {issueID},
  });

  async function streamHistoryThroughBase(): Promise<void> {
    source.apply(transaction('03', insert('a', 'one'), insert('b', 'two')));
    source.apply(transaction('05', insert('c', 'three')));
    await source.publishBase(); // base at cursor '05'
    source.apply(transaction('07', update('a', 'one-updated'), del('b')));
    source.apply(transaction('09', insert('d', 'four')));
    await source.flushArchive();
  }

  test('restores the newest base and replays the archived tail', async () => {
    await streamHistoryThroughBase();

    const result = await archiveRestore(lc, store, restoreFile, {
      replicaVersion: '02',
      minWatermark: '09',
    });
    expect(result).toBe('success');

    const restored = new Database(lc, restoreFile);
    try {
      expect(getSubscriptionState(new StatementRunner(restored))).toMatchObject(
        {replicaVersion: '02', watermark: '09'},
      );
      expect(rows(restored)).toEqual(source.rows());
      expect(rows(restored)).toEqual([
        {issueID: 'a', val: 'one-updated', ['_0_version']: '07'},
        {issueID: 'c', val: 'three', ['_0_version']: '05'},
        {issueID: 'd', val: 'four', ['_0_version']: '09'},
      ]);
    } finally {
      restored.close();
    }
  });

  test('an empty archive is no_backup', async () => {
    expect(
      await archiveRestore(lc, store, restoreFile, {
        replicaVersion: '02',
        minWatermark: '02',
      }),
    ).toBe('no_backup');
    expect(existsSync(restoreFile)).toBe(false);
  });

  test('a lineage with segments but no complete base is no_backup', async () => {
    source.apply(transaction('03', insert('a', 'one')));
    await source.flushArchive();
    // A publication that crashed before its manifest is invisible.
    store.beforePut = key => {
      if (key.endsWith('complete.json')) {
        throw new Error('injected crash at the manifest boundary');
      }
    };
    await expect(source.publishBase()).rejects.toThrow('injected crash');
    store.beforePut = undefined;

    expect(
      await archiveRestore(lc, store, restoreFile, {
        replicaVersion: '02',
        minWatermark: '03',
      }),
    ).toBe('no_backup');
  });

  test('falls back to an older base when the newest is damaged', async () => {
    await streamHistoryThroughBase(); // base at '05'
    source.apply(transaction('0b', insert('e', 'five')));
    await source.publishBase(); // base at '0b'
    await source.flushArchive();

    // Damage a chunk of the newest base; restore falls back to '05' and
    // replays the longer tail to the same head.
    const chunkKey = must(
      [...store.objects.keys()].find(k => k.startsWith('v1/02/base/0b/chunk/')),
    );
    store.objects.set(chunkKey, new Uint8Array([1, 2, 3]));

    const result = await archiveRestore(lc, store, restoreFile, {
      replicaVersion: '02',
      minWatermark: '0b',
    });
    expect(result).toBe('success');
    const restored = new Database(lc, restoreFile);
    try {
      expect(getSubscriptionState(new StatementRunner(restored))).toMatchObject(
        {watermark: '0b'},
      );
      expect(rows(restored)).toEqual(source.rows());
    } finally {
      restored.close();
    }
  });

  test('a gap above the base bounds the replay at the contiguous head', async () => {
    await streamHistoryThroughBase();
    await store.delete(segmentKey('02', '05', '07')); // the gap

    // The reachable head is now the base cursor itself, which fails a
    // constraint demanding the full history...
    expect(
      await archiveRestore(lc, store, restoreFile, {
        replicaVersion: '02',
        minWatermark: '09',
      }),
    ).toBe('invalid_replica');
    expect(existsSync(restoreFile)).toBe(false);

    // ... and satisfies one the base alone covers.
    expect(
      await archiveRestore(lc, store, restoreFile, {
        replicaVersion: '02',
        minWatermark: '05',
      }),
    ).toBe('success');
    const restored = new Database(lc, restoreFile);
    try {
      expect(getSubscriptionState(new StatementRunner(restored))).toMatchObject(
        {watermark: '05'},
      );
    } finally {
      restored.close();
    }
  });

  test('a corrupt tail segment fails the restore and leaves nothing behind', async () => {
    await streamHistoryThroughBase();
    const key = segmentKey('02', '05', '07');
    const corrupt = Uint8Array.from(store.objects.get(key)!);
    corrupt[corrupt.length - 1] ^= 0x01;
    store.objects.set(key, corrupt);

    expect(
      await archiveRestore(lc, store, restoreFile, {
        replicaVersion: '02',
        minWatermark: '09',
      }),
    ).toBe('error');
    expect(existsSync(restoreFile)).toBe(false);
    expect(existsSync(`${restoreFile}.tmp`)).toBe(false);
  });

  test('an existing compatible replica is kept', async () => {
    await streamHistoryThroughBase();
    expect(
      await archiveRestore(lc, store, restoreFile, {
        replicaVersion: '02',
        minWatermark: '05',
      }),
    ).toBe('success');

    // A second restore over the existing file keeps it.
    expect(
      await archiveRestore(lc, store, restoreFile, {
        replicaVersion: '02',
        minWatermark: '05',
      }),
    ).toBe('success');
  });

  test('an existing incompatible replica is deleted for the retry', async () => {
    await streamHistoryThroughBase();
    expect(
      await archiveRestore(lc, store, restoreFile, {
        replicaVersion: '02',
        minWatermark: '05',
      }),
    ).toBe('success');

    // A resync bumped the expected generation.
    expect(
      await archiveRestore(lc, store, restoreFile, {
        replicaVersion: '0z',
        minWatermark: '0z',
      }),
    ).toBe('invalid_replica');
    expect(existsSync(restoreFile)).toBe(false);
  });

  test('without constraints, restores the newest lineage', async () => {
    await streamHistoryThroughBase();
    const result = await archiveRestore(lc, store, restoreFile, undefined);
    expect(result).toBe('success');
    const restored = new Database(lc, restoreFile);
    try {
      expect(getSubscriptionState(new StatementRunner(restored))).toMatchObject(
        {replicaVersion: '02', watermark: '09'},
      );
    } finally {
      restored.close();
    }
  });
});

function rows(db: Database): unknown[] {
  return db.prepare(`SELECT * FROM issues ORDER BY issueID`).all();
}

/**
 * A source replica whose transactions run through the real change processor
 * ('backup' mode, i.e. what litestream-era backups contain) while
 * dual-writing to the archive.
 */
class SourceReplica {
  readonly #file: string;
  readonly #store: InMemoryObjectStore;
  readonly #writer: ArchiveWriter;
  #db: Database | undefined;
  #processor: ChangeProcessor | undefined;

  constructor(file: string, store: InMemoryObjectStore) {
    this.#file = file;
    this.#store = store;
    this.#writer = new ArchiveWriter(lc, {
      store,
      replicaVersion: '02',
      authoritative: true,
      segmentTargetBytes: 1, // seal every transaction
      sealIntervalMs: 60_000,
    });
  }

  async open(watermark: string): Promise<void> {
    this.#db = new Database(lc, this.#file);
    initReplicationState(this.#db, ['zero_data'], watermark);
    this.#db.exec(
      `CREATE TABLE issues(issueID TEXT PRIMARY KEY, val TEXT, _0_version TEXT)`,
    );
    await this.#writer.reconcile(watermark);
    this.#processor = new ChangeProcessor(
      new StatementRunner(this.#db),
      'backup',
      (_, err) => {
        throw err;
      },
    );
  }

  apply(messages: ChangeStreamData[]): void {
    for (const message of messages) {
      this.#processor!.processMessage(lc, message);
      this.#writer.write(message, JSON.stringify(message));
    }
  }

  async publishBase() {
    await this.flushArchive();
    // Release the file for the publisher's own connection, then resume.
    this.#db!.close();
    try {
      return await publishBase(lc, this.#store, this.#file, {
        chunkBytes: 4096,
        integrityCheck: 'full',
      });
    } finally {
      this.#db = new Database(lc, this.#file);
      this.#processor = new ChangeProcessor(
        new StatementRunner(this.#db),
        'backup',
        (_, err) => {
          throw err;
        },
      );
    }
  }

  /** Waits until everything applied so far is durable in the archive. */
  async flushArchive(): Promise<void> {
    const {lastBufferedWatermark} = this.#writer.state();
    for (let i = 0; i < 1000; i++) {
      const {durableWatermark, bufferedBytes} = this.#writer.state();
      if (bufferedBytes === 0 && durableWatermark === lastBufferedWatermark) {
        return;
      }
      await new Promise(resolve => setTimeout(resolve, 1));
    }
    throw new Error('archive did not drain');
  }

  rows(): unknown[] {
    return rows(this.#db!);
  }

  async close(): Promise<void> {
    await this.#writer.close();
    this.#db?.close();
  }
}
