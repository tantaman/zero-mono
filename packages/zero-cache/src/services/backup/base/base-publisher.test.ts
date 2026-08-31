import {createHash} from 'node:crypto';
import {mkdtempSync, rmSync} from 'node:fs';
import {readFile} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {afterEach, beforeEach, describe, expect, test} from 'vitest';
import {createSilentLogContext} from '../../../../../shared/src/logging-test-utils.ts';
import {Database} from '../../../../../zqlite/src/db.ts';
import {StatementRunner} from '../../../db/statements.ts';
import {
  initReplicationState,
  updateReplicationWatermark,
} from '../../replicator/schema/replication-state.ts';
import {
  baseChunkKey,
  baseCompleteKey,
  baseIntentKey,
} from '../archive/layout.ts';
import {InMemoryObjectStore} from '../test-utils.ts';
import {publishBase} from './base-publisher.ts';
import {decodeBaseIntent} from './manifest.ts';

const lc = createSilentLogContext();

describe('backup/base/base-publisher', () => {
  let dir: string;
  let replicaFile: string;
  let store: InMemoryObjectStore;

  beforeEach(() => {
    dir = mkdtempSync(join(tmpdir(), 'zero-base-publisher-test-'));
    replicaFile = join(dir, 'replica.db');
    store = new InMemoryObjectStore();

    const db = new Database(lc, replicaFile);
    initReplicationState(db, ['zero_data'], '02');
    db.exec(
      `CREATE TABLE issues(issueID TEXT PRIMARY KEY, val TEXT, _0_version TEXT)`,
    );
    // Enough rows that a small chunkBytes produces several chunks.
    const insert = db.prepare(`INSERT INTO issues VALUES (?, ?, '02')`);
    for (let i = 0; i < 500; i++) {
      insert.run(`issue-${i}`, `value-${i}`.repeat(20));
    }
    updateReplicationWatermark(new StatementRunner(db), '0g');
    db.close();
  });

  afterEach(() => {
    rmSync(dir, {recursive: true, force: true});
  });

  test('publishes a chunked base named by the embedded cursor, manifest last', async () => {
    const {manifest, published} = await publishBase(lc, store, replicaFile, {
      chunkBytes: 4096,
      integrityCheck: 'full',
    });
    expect(published).toBe(true);
    expect(manifest.replicaVersion).toBe('02');
    expect(manifest.cursor).toBe('0g');
    expect(manifest.chunks.length).toBeGreaterThan(1);
    expect(manifest.chunks.reduce((sum, c) => sum + c.size, 0)).toBe(
      manifest.fileSize,
    );

    // The intent records the same identity.
    expect(
      decodeBaseIntent(await store.get(baseIntentKey('02', '0g'))),
    ).toMatchObject({replicaVersion: '02', cursor: '0g'});

    // Reassembling the chunks yields the exact file.
    const reassembled: Buffer[] = [];
    for (let i = 0; i < manifest.chunks.length; i++) {
      const chunk = await store.get(baseChunkKey('02', '0g', i));
      expect(createHash('sha256').update(chunk).digest('hex')).toBe(
        manifest.chunks[i].sha256,
      );
      reassembled.push(Buffer.from(chunk));
    }
    const bytes = Buffer.concat(reassembled);
    expect(bytes.length).toBe(manifest.fileSize);
    expect(createHash('sha256').update(bytes).digest('hex')).toBe(
      manifest.fileSha256,
    );
    expect(bytes.equals(await readFile(replicaFile))).toBe(true);
  });

  test('republishing an already-complete base is a no-op', async () => {
    const first = await publishBase(lc, store, replicaFile, {
      chunkBytes: 4096,
      integrityCheck: 'quick',
    });
    const objectCount = store.objects.size;

    const second = await publishBase(lc, store, replicaFile, {
      chunkBytes: 4096,
      integrityCheck: 'quick',
    });
    expect(second.published).toBe(false);
    expect(second.manifest).toEqual(first.manifest);
    expect(store.objects.size).toBe(objectCount);
  });

  test('a crash before the manifest leaves no complete base; a retry completes it', async () => {
    store.beforePut = key => {
      if (key.endsWith('complete.json')) {
        throw new Error('injected crash at the manifest boundary');
      }
    };
    await expect(
      publishBase(lc, store, replicaFile, {
        chunkBytes: 4096,
        integrityCheck: 'quick',
      }),
    ).rejects.toThrow('injected crash');
    expect(await store.head(baseCompleteKey('02', '0g'))).toBeUndefined();
    const uploadedChunks = (await store.list('v1/02/base/0g/chunk/')).length;
    expect(uploadedChunks).toBeGreaterThan(0);

    // The retry re-uploads nothing that already landed, and completes.
    store.beforePut = undefined;
    const {published, manifest} = await publishBase(lc, store, replicaFile, {
      chunkBytes: 4096,
      integrityCheck: 'quick',
    });
    expect(published).toBe(true);
    expect((await store.list('v1/02/base/0g/chunk/')).length).toBe(
      uploadedChunks,
    );
    expect(await store.head(baseCompleteKey('02', '0g'))).toBeDefined();
    expect(manifest.chunks.length).toBe(uploadedChunks);
  });

  test('rejects a file that is not a replica', async () => {
    const bogus = join(dir, 'bogus.db');
    const db = new Database(lc, bogus);
    db.exec(`CREATE TABLE t(x)`);
    db.close();

    await expect(
      publishBase(lc, store, bogus, {chunkBytes: 4096, integrityCheck: 'full'}),
    ).rejects.toThrow();
    expect(store.objects.size).toBe(0);
  });
});
