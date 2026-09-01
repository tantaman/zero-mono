/**
 * The M1 acceptance drill and permanent regression guard for the archive's
 * streaming discipline: archive, replay, and restore a synthetic multi-GB
 * transaction inside a process whose V8 heap (`--max-old-space-size`) is far
 * below the payload. If any stage regresses to O(transaction) or O(segment)
 * memory, the drill dies with an out-of-memory crash.
 *
 * Spawned by `memory-ceiling.test.ts`; also runnable by hand:
 *
 * ```
 * node --max-old-space-size=192 \
 *   src/services/backup/memory-ceiling-drill.ts <workDir> <rows> <rowBytes>
 * ```
 *
 * The drill:
 * 1. publishes an empty base at watermark '02' to a `file://` store,
 * 2. streams one committed transaction of `rows` inserts, each carrying a
 *    `rowBytes` value, through the ArchiveWriter (spooling, part chains,
 *    sealing, uploads) until durable, and
 * 3. restores into a fresh replica file — base download plus streaming tail
 *    replay through the real ChangeProcessor — and verifies the result.
 */

import {mkdirSync} from 'node:fs';
import {join} from 'node:path';
import {consoleLogSink, LogContext} from '@rocicorp/logger';
import {Database} from '../../../../zqlite/src/db.ts';
import {StatementRunner} from '../../db/statements.ts';
import type {ChangeStreamData} from '../change-source/protocol/current/downstream.ts';
import {
  getSubscriptionState,
  initReplicationState,
} from '../replicator/schema/replication-state.ts';
import {ArchiveWriter} from './archive/archive-writer.ts';
import {publishBase} from './base/base-publisher.ts';
import {FsObjectStore} from './object-store/fs-object-store.ts';
import {archiveRestore} from './restore/archive-restore.ts';

const lc = new LogContext('warn', {}, consoleLogSink);

const RELATION = {
  schema: 'public',
  name: 'issues',
  rowKey: {columns: ['issueID']},
};

const START = '02';
const COMMIT = '03';

async function main(
  workDir: string,
  rows: number,
  rowBytes: number,
): Promise<void> {
  mkdirSync(workDir, {recursive: true});
  const store = new FsObjectStore(join(workDir, 'store'));

  // 1. An empty replica at the start watermark, published as the base.
  const sourceFile = join(workDir, 'source.db');
  {
    const db = new Database(lc, sourceFile);
    initReplicationState(db, ['zero_data'], START);
    db.exec(
      `CREATE TABLE issues(issueID TEXT PRIMARY KEY, val TEXT, _0_version TEXT)`,
    );
    db.close();
  }
  await publishBase(lc, store, sourceFile, {
    chunkBytes: 4 * 1024 * 1024,
    integrityCheck: 'full',
  });

  // 2. One transaction far larger than the heap, through the writer.
  const writer = new ArchiveWriter(lc, {
    store,
    replicaVersion: START,
    spoolDir: join(workDir, 'spool'),
    segmentTargetBytes: 16 * 1024 * 1024,
    sealIntervalMs: 3600 * 1000,
    // close() below is the drill's flush; give it room for the final
    // part's compression and upload.
    flushTimeoutMs: 10 * 60 * 1000,
  });
  await writer.reconcile(START);
  const write = (message: ChangeStreamData) =>
    writer.write(message, JSON.stringify(message));
  write(['begin', {tag: 'begin'}, {commitWatermark: COMMIT}]);
  for (let i = 0; i < rows; i++) {
    write([
      'data',
      {
        tag: 'insert',
        relation: RELATION,
        new: {issueID: `id-${i}`, val: `${i}:${'x'.repeat(rowBytes)}`},
      },
    ]);
  }
  write([
    'commit',
    {tag: 'commit', commitTimeMs: Date.now()},
    {watermark: COMMIT},
  ]);

  await writer.close(); // seals and flushes, bounded by flushTimeoutMs
  if (writer.state().durableWatermark !== COMMIT) {
    throw new Error(
      `the archive did not drain: ${JSON.stringify(writer.state())}`,
    );
  }

  // 3. Restore: base download + streaming tail replay.
  const restoreFile = join(workDir, 'restored.db');
  const result = await archiveRestore(
    lc,
    store,
    restoreFile,
    {replicaVersion: START, minWatermark: COMMIT},
    {mode: 'backup'},
  );
  if (result !== 'success') {
    throw new Error(`restore returned ${result}`);
  }

  const db = new Database(lc, restoreFile);
  try {
    const {watermark} = getSubscriptionState(new StatementRunner(db));
    if (watermark !== COMMIT) {
      throw new Error(`restored watermark ${watermark}; expected ${COMMIT}`);
    }
    const {count} = db
      .prepare(`SELECT COUNT(*) AS count FROM issues`)
      .get<{count: number}>();
    if (count !== rows) {
      throw new Error(`restored ${count} rows; expected ${rows}`);
    }
    const {len} = db
      .prepare(
        `SELECT LENGTH(val) AS len FROM issues WHERE issueID = 'id-${rows - 1}'`,
      )
      .get<{len: number}>();
    if (len < rowBytes) {
      throw new Error(`restored row is ${len} bytes; expected >= ${rowBytes}`);
    }
  } finally {
    db.close();
  }

  const heapMB = Math.round(process.memoryUsage().heapUsed / 1024 / 1024);
  // The stdout contract with memory-ceiling.test.ts.
  // oxlint-disable-next-line no-console
  console.log(
    `MEMORY-CEILING-DRILL OK: ${rows} x ${rowBytes} bytes archived, ` +
      `replayed, and restored; heapUsed ${heapMB}MB`,
  );
}

const [workDir, rows, rowBytes] = process.argv.slice(2);
main(workDir, Number(rows), Number(rowBytes)).then(
  () => process.exit(0),
  e => {
    // oxlint-disable-next-line no-console
    console.error(e);
    process.exit(1);
  },
);
