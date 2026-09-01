import {existsSync, mkdtempSync, rmSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {afterEach, beforeEach, describe, expect, test} from 'vitest';
import {createSilentLogContext} from '../../../../../shared/src/logging-test-utils.ts';
import {Database} from '../../../../../zqlite/src/db.ts';
import {StatementRunner} from '../../../db/statements.ts';
import {ChangeProcessor} from '../../replicator/change-processor.ts';
import {initReplicationState} from '../../replicator/schema/replication-state.ts';
import {segmentKey} from '../archive/layout.ts';
import {encodeSegment} from '../archive/segment-format.ts';
import {publishBase} from '../base/base-publisher.ts';
import {archiveRestore} from '../restore/archive-restore.ts';
import {
  InMemoryObjectStore,
  wireTransaction,
  type WireTransaction,
} from '../test-utils.ts';
import {runArchiveDrill} from './archive-drill.ts';

const lc = createSilentLogContext();

describe('backup/drill/archive-drill', () => {
  let dir: string;
  let store: InMemoryObjectStore;
  let referenceFile: string;
  let scratchDir: string;

  beforeEach(() => {
    dir = mkdtempSync(join(tmpdir(), 'zero-archive-drill-test-'));
    store = new InMemoryObjectStore();
    referenceFile = join(dir, 'reference.db');
    scratchDir = join(dir, 'scratch');
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

  /** Materializes the "live" reference replica from the archive itself. */
  async function restoreReference(upTo?: string) {
    const result = await archiveRestore(lc, store, referenceFile, undefined, {
      mode: 'backup',
      upTo,
    });
    expect(result).toBe('success');
  }

  /** Applies a transaction directly, like a live replica ahead of the archive. */
  function applyToReference(tx: WireTransaction) {
    const db = new Database(lc, referenceFile);
    const processor = new ChangeProcessor(
      new StatementRunner(db),
      'backup',
      (_, err) => {
        throw err;
      },
    );
    for (const message of tx.parsed) {
      processor.processMessage(lc, message);
    }
    db.close();
  }

  test('a scratch restore matches a pinned reference mid-segment', async () => {
    await seedGenesisBase();
    // One segment spanning '03'..'07': the pinned watermark '05' falls
    // mid-segment, so both the reference restore and the drill's scratch
    // restore exercise the bounded (upTo) replay.
    await putSegment('02', ['03', '05', '07']);
    await restoreReference('05');

    const result = await runArchiveDrill(lc, store, referenceFile, {
      scratchDir,
    });
    expect(result).toMatchObject({
      outcome: 'match',
      replicaVersion: '02',
      watermark: '05',
    });
    expect((result as {rows: number}).rows).toBeGreaterThan(0);
    // The scratch replica is cleaned up either way.
    expect(existsSync(join(scratchDir, 'drill-replica.db'))).toBe(false);
  });

  test('bases newer than the pinned watermark are not used', async () => {
    await seedGenesisBase();
    await putSegment('02', ['03', '05']);
    await putSegment('05', ['07']);
    // Publish a newer base at '07' from a fully caught-up builder.
    const builderFile = join(dir, 'builder.db');
    expect(
      await archiveRestore(lc, store, builderFile, undefined, {mode: 'backup'}),
    ).toBe('success');
    await publishBase(lc, store, builderFile, {
      chunkBytes: 4096,
      integrityCheck: 'full',
    });

    await restoreReference('05');
    const result = await runArchiveDrill(lc, store, referenceFile, {
      scratchDir,
    });
    // The drill restored from the '02' base (the '07' base is past the
    // pinned watermark) and replayed to exactly '05'.
    expect(result).toMatchObject({outcome: 'match', watermark: '05'});
  });

  test('a diverged reference is a mismatch', async () => {
    await seedGenesisBase();
    await putSegment('02', ['03', '05']);
    await restoreReference();

    // Corrupt the reference outside of replication.
    const db = new Database(lc, referenceFile);
    db.exec(/*sql*/ `UPDATE issues SET val = 'corrupt' WHERE issueID = '03-0'`);
    db.close();

    const result = await runArchiveDrill(lc, store, referenceFile, {
      scratchDir,
    });
    expect(result).toMatchObject({
      outcome: 'mismatch',
      watermark: '05',
      mismatches: [{table: 'issues', kind: 'rows'}],
    });
  });

  test('reports archive-behind when the reference leads the durable head', async () => {
    await seedGenesisBase();
    await putSegment('02', ['03']);
    await restoreReference();
    // The live replica applies '05' before it is archive-durable.
    applyToReference(wireTransaction('05', 1, 1000));

    const result = await runArchiveDrill(lc, store, referenceFile, {
      scratchDir,
      archiveWaitTimeoutMs: 100,
      pollIntervalMs: 10,
    });
    expect(result).toEqual({
      outcome: 'archive-behind',
      replicaVersion: '02',
      watermark: '05',
      durableHead: '03',
    });
  });

  test('reports no-base against an archive without a usable base', async () => {
    await seedGenesisBase();
    await putSegment('02', ['03', '05']);
    await restoreReference();

    const emptyStore = new InMemoryObjectStore();
    const result = await runArchiveDrill(lc, emptyStore, referenceFile, {
      scratchDir,
    });
    expect(result).toEqual({
      outcome: 'no-base',
      replicaVersion: '02',
      watermark: '05',
    });
  });
});
