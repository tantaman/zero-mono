/* oxlint-disable no-console */

// Measures the ceiling for applying a *logical* change log to a bare SQLite
// replica: the exact thing the replication-manager's replica writer does, but
// fed from a stored log rather than a live change-streamer subscription.
//
// ## Why
//
// We already have (a) a CDC stream and (b) a component that applies it to
// SQLite. If applying a stored logical log is fast enough, backup/restore can
// be "base snapshot in S3 + logical log segments in S3" instead of litestream:
// restore the base, replay the log, done. No second backup mechanism.
//
// The number that decides this is: **how many MB of uncompressed logical log
// can we apply per second?** From that falls out the recovery time for a given
// amount of log to catch up on, which is what the bench reports directly.
//
// ## What is measured
//
// The log entries are the canonical downstream wire form -- exactly what
// `serializeChangeStreamData()` produces and what the change-streamer already
// stores and fans out, e.g.
//
//   ["begin",{"tag":"begin"},{"commitWatermark":"3a2"}]
//   ["data",{"tag":"insert","relation":{...},"new":{...}}]
//   ["commit",{"tag":"commit"},{"watermark":"3a2"}]
//
// so "MB of log" here means the same bytes that would be uploaded to S3
// (uncompressed; these are highly compressible JSON, so the wire/storage cost
// is a fraction of this, but the *apply* cost is driven by the uncompressed
// form).
//
// The apply path is the real one: `ChangeProcessor` in 'backup' mode, over a
// real `Database`, with the replication-manager's pragmas. 'backup' mode is
// what the replication-manager runs, so no `_zero.changeLog` rows are written
// -- just the replica rows and the watermark.
//
// Three stages are timed so the cost can be attributed:
//
//   parse        BigIntJSON.parse of each entry. Unavoidable when the log
//                comes from bytes rather than an in-process stream.
//   validate     valita validation of the parsed entry, which the live
//                subscription path pays in `streamInStringified`. A log we
//                wrote ourselves arguably does not need it -- this measures
//                what skipping it buys.
//   apply        ChangeProcessor.processMessage into SQLite. Reported as
//                (parse+apply) - (parse), since materializing every parsed
//                entry up front would distort the measurement with GC.
//
// ## Axes
//
//   workload     insert-heavy | update-heavy | mixed
//   coalesce     logical transactions per SQLite commit. 1 is the live
//                streaming shape (every upstream tx is its own SQLite tx).
//                Catchup does not need that: nothing reads the replica while
//                it is behind, so N logical txs can share one commit. Under
//                WAL + synchronous=NORMAL there is no per-commit fsync to
//                amortize, so what this buys is the per-transaction overhead
//                and the page rewrites that each commit forces into the WAL.
//   durability   'production' is the replication manager's config verbatim:
//                synchronous=NORMAL and wal_autocheckpoint=0, because
//                litestream owns checkpointing there. 'catchup' is what a
//                replayer could use instead -- synchronous=OFF, and
//                checkpointing switched back on. Both are only available to
//                catchup: if the process dies mid-replay the answer is to
//                replay again from the last durable watermark, so the
//                intermediate states need not survive a crash.
//   base         rows pre-loaded into the replica before measuring, to check
//                whether apply throughput holds up once the replica is big
//                enough that the B-trees miss cache.
//
// ## What is NOT measured
//
// Downloading the log from S3, decompressing it, and restoring the base
// snapshot. Those are I/O and are sized by the "storage/transfer" section the
// bench prints; everything timed here is CPU and local disk.
//
// ## Running
//
//   pnpm --filter zero-cache run bench logical-log-apply
//
// Knobs (all optional):
//   LOGICAL_LOG_TARGET_MB        log size generated per case (default 48)
//   LOGICAL_LOG_REPS             samples per case (default 3)
//   LOGICAL_LOG_WORKLOADS        insert-heavy,update-heavy,mixed
//   LOGICAL_LOG_COALESCE         logical txs per SQLite commit, e.g. 1,32,256
//   LOGICAL_LOG_BASE_ROWS        base replica size for the ceiling sweep
//   LOGICAL_LOG_BASE_SWEEP       base sizes for the size-sensitivity sweep
//   LOGICAL_LOG_PROJECTIONS      log sizes (MB) to project catchup time for
//
// A larger log exercises a larger replica; a 1 GB run needs heap headroom:
//   NODE_OPTIONS=--max-old-space-size=8192 LOGICAL_LOG_TARGET_MB=1024 \
//     pnpm --filter zero-cache run bench logical-log-apply
//
// ## Reading the output
//
// `MB/s` is the headline: uncompressed log bytes applied per second, parse
// included. `apply MB/s` is derived (`1/apply = 1/total - 1/parse`), so it
// carries the noise of both measurements; treat it as an attribution, not a
// measurement. `WAL MB` is the WAL left on disk after replaying the log --
// under the replication-manager's `wal_autocheckpoint = 0` this is the write
// amplification a replay would inflict on the file litestream is shipping.

import {copyFileSync, existsSync, statSync} from 'node:fs';
import {gzipSync} from 'node:zlib';
import {describe, test} from 'vitest';
import {createManualBenchmarkRecorder} from '../../../../shared/src/bench.ts';
import {BigIntJSON} from '../../../../shared/src/bigint-json.ts';
import {createSilentLogContext} from '../../../../shared/src/logging-test-utils.ts';
import {must} from '../../../../shared/src/must.ts';
import * as v from '../../../../shared/src/valita.ts';
import type {Database} from '../../../../zqlite/src/db.ts';
import type {IndexSpec, TableSpec} from '../../db/specs.ts';
import {StatementRunner} from '../../db/statements.ts';
import {DbFile} from '../../test/lite.ts';
import {versionToLexi} from '../../types/lexi-version.ts';
import {getPragmaConfig} from '../../workers/replicator.ts';
import type {
  MessageRelation,
  TableCreate,
} from '../change-source/protocol/current/data.ts';
import {
  changeStreamDataSchema,
  type ChangeStreamData,
} from '../change-source/protocol/current/downstream.ts';
import {serializeChangeStreamData} from '../change-streamer/change-log-codec.ts';
import {ChangeProcessor} from './change-processor.ts';
import {initReplicationState} from './schema/replication-state.ts';
import {applyPragmas} from './write-worker-client.ts';

type Workload = 'insert-heavy' | 'update-heavy' | 'mixed';
type Durability = 'production' | 'catchup';

const BYTES_PER_MB = 1024 * 1024;
const lc = createSilentLogContext();
const benchmarkRecorder = createManualBenchmarkRecorder();

const TEST_TIMEOUT_MS = 30 * 60_000;

function envNumber(name: string, fallback: number, min = 1): number {
  const raw = process.env[name];
  if (raw === undefined || raw === '') {
    return fallback;
  }
  const parsed = Number(raw);
  if (!Number.isFinite(parsed) || parsed < min) {
    throw new Error(`${name} must be >= ${min}, got "${raw}"`);
  }
  return parsed;
}

function envList<T extends string>(
  name: string,
  fallback: readonly T[],
  allowed?: readonly T[],
): readonly T[] {
  const raw = process.env[name];
  if (raw === undefined || raw === '') {
    return fallback;
  }
  const values = raw
    .split(',')
    .map(s => s.trim())
    .filter(s => s.length > 0) as T[];
  if (allowed) {
    for (const value of values) {
      if (!allowed.includes(value)) {
        throw new Error(
          `${name} must be one of ${allowed.join(',')}, got "${value}"`,
        );
      }
    }
  }
  return values;
}

function envNumberList(name: string, fallback: readonly number[], min = 1) {
  const raw = process.env[name];
  if (raw === undefined || raw === '') {
    return fallback;
  }
  return raw
    .split(',')
    .map(s => s.trim())
    .filter(s => s.length > 0)
    .map(s => {
      const parsed = Number(s);
      if (!Number.isFinite(parsed) || parsed < min) {
        throw new Error(`${name} values must be >= ${min}, got "${s}"`);
      }
      return parsed;
    });
}

const TARGET_MB = envNumber('LOGICAL_LOG_TARGET_MB', 48);
const REPS = Math.max(1, Math.floor(envNumber('LOGICAL_LOG_REPS', 3)));
const WORKLOADS = envList<Workload>(
  'LOGICAL_LOG_WORKLOADS',
  ['insert-heavy', 'update-heavy', 'mixed'],
  ['insert-heavy', 'update-heavy', 'mixed'],
);
const COALESCE = envNumberList('LOGICAL_LOG_COALESCE', [1, 32, 256]);
const BASE_ROWS = Math.floor(envNumber('LOGICAL_LOG_BASE_ROWS', 250_000, 0));
/** Replica sizes (rows) swept by the size-sensitivity test. */
const BASE_SWEEP = envNumberList(
  'LOGICAL_LOG_BASE_SWEEP',
  [0, 500_000, 2_000_000],
  0,
);
/** Log sizes (MB) to project catchup time for, per the S3 restore question. */
const PROJECTIONS = envNumberList('LOGICAL_LOG_PROJECTIONS', [100, 500, 1024]);

// ---------------------------------------------------------------------------
// Deterministic data generation
// ---------------------------------------------------------------------------

/** Deterministic PRNG so every case sees byte-identical input. */
function mulberry32(seed: number): () => number {
  let a = seed >>> 0;
  return () => {
    a = (a + 0x6d2b79f5) | 0;
    let t = Math.imul(a ^ (a >>> 15), 1 | a);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

// Prose-shaped corpus: JSON escaping and string length both affect parse cost,
// so slicing real-ish text beats repeating a single character.
const CORPUS = (() => {
  const words =
    'the quick brown fox jumps over a lazy dog while zero syncs rows from ' +
    'postgres into sqlite replicas and view syncers incrementally maintain ' +
    'queries for connected clients without polling or refetching anything ';
  return words.repeat(24);
})();

function text(rand: () => number, min: number, max: number): string {
  const len = min + Math.floor(rand() * (max - min));
  const start = Math.floor(rand() * (CORPUS.length - len - 1));
  return CORPUS.slice(start, start + len);
}

/**
 * A wide table and a narrow table, so the log carries a realistic mix of
 * per-change sizes rather than one uniform row shape.
 */
const ISSUE_SPEC: TableSpec = {
  schema: 'public',
  name: 'issue',
  columns: {
    id: {pos: 0, dataType: 'text', notNull: true},
    shortID: {pos: 1, dataType: 'int8'},
    title: {pos: 2, dataType: 'text'},
    description: {pos: 3, dataType: 'text'},
    open: {pos: 4, dataType: 'bool'},
    creatorID: {pos: 5, dataType: 'text'},
    assigneeID: {pos: 6, dataType: 'text'},
    created: {pos: 7, dataType: 'int8'},
    modified: {pos: 8, dataType: 'int8'},
    visibility: {pos: 9, dataType: 'text'},
  },
  primaryKey: ['id'],
};

const COMMENT_SPEC: TableSpec = {
  schema: 'public',
  name: 'comment',
  columns: {
    id: {pos: 0, dataType: 'text', notNull: true},
    issueID: {pos: 1, dataType: 'text'},
    creatorID: {pos: 2, dataType: 'text'},
    created: {pos: 3, dataType: 'int8'},
    body: {pos: 4, dataType: 'text'},
  },
  primaryKey: ['id'],
};

const SPECS = [ISSUE_SPEC, COMMENT_SPEC] as const;

/**
 * `create-table` alone produces a table with no indexes -- the replica's
 * primary key arrives as a separate `create-index` message, exactly as the
 * Postgres change source publishes `<table>_pkey`. Without these, every
 * update and delete degrades to a full table scan, which silently turns this
 * into a benchmark of table scans rather than of applying changes.
 *
 * The secondary indexes are the other half of realism: a replicated table
 * carries the upstream's indexes, and index maintenance is a real part of
 * what applying a change costs.
 */
const INDEX_SPECS: readonly IndexSpec[] = [
  {
    schema: 'public',
    tableName: 'issue',
    name: 'issue_pkey',
    unique: true,
    columns: {id: 'ASC'},
  },
  {
    schema: 'public',
    tableName: 'issue',
    name: 'issue_modified_idx',
    unique: false,
    columns: {modified: 'DESC'},
  },
  {
    schema: 'public',
    tableName: 'comment',
    name: 'comment_pkey',
    unique: true,
    columns: {id: 'ASC'},
  },
  {
    schema: 'public',
    tableName: 'comment',
    name: 'comment_issue_idx',
    unique: false,
    columns: {issueID: 'ASC', created: 'ASC'},
  },
];

function relationOf(spec: TableSpec): MessageRelation {
  return {
    schema: spec.schema,
    name: spec.name,
    rowKey: {type: 'default', columns: [...spec.primaryKey!]},
  };
}

const RELATIONS = {
  issue: relationOf(ISSUE_SPEC),
  comment: relationOf(COMMENT_SPEC),
} as const;

function createTableMessage(spec: TableSpec): TableCreate {
  return {
    tag: 'create-table',
    spec,
    metadata: {
      rowKey: {type: 'default', columns: [...spec.primaryKey!]},
    },
  };
}

type Table = keyof typeof RELATIONS;

type Row = Record<string, string | number | boolean | null>;

function issueRow(rand: () => number, id: string, seq: number): Row {
  return {
    id,
    shortID: seq,
    title: text(rand, 24, 72),
    description: text(rand, 120, 900),
    open: rand() < 0.7,
    creatorID: `user-${Math.floor(rand() * 5000)}`,
    assigneeID: rand() < 0.4 ? `user-${Math.floor(rand() * 5000)}` : null,
    created: 1700000000000 + seq * 1000,
    modified: 1700000000000 + seq * 1000,
    visibility: rand() < 0.9 ? 'public' : 'internal',
  };
}

function commentRow(rand: () => number, id: string, seq: number): Row {
  return {
    id,
    issueID: `issue-${Math.floor(rand() * Math.max(1, seq))}`,
    creatorID: `user-${Math.floor(rand() * 5000)}`,
    created: 1700000000000 + seq * 1000,
    body: text(rand, 40, 320),
  };
}

/**
 * One upstream transaction, serialized. Kept grouped (rather than as a flat
 * list) so the replay stream can be re-framed for different coalescing
 * factors without regenerating -- and without recounting -- the log.
 */
type LogicalTx = {
  readonly watermark: string;
  readonly data: readonly string[];
  /** Serialized bytes of this tx as it would be stored, framing included. */
  readonly bytes: number;
};

type GeneratedLog = {
  readonly txs: readonly LogicalTx[];
  /** Total bytes the log occupies in storage, uncompressed. */
  readonly bytes: number;
  readonly changes: number;
  /** Live row ids at the end of the log, for chaining a base into a measure. */
  readonly liveIds: ReadonlyMap<Table, readonly string[]>;
};

function beginJSON(watermark: string): string {
  return `["begin",{"tag":"begin"},{"commitWatermark":"${watermark}"}]`;
}

function commitJSON(watermark: string): string {
  return `["commit",{"tag":"commit"},{"watermark":"${watermark}"}]`;
}

const OP_MIX: Record<
  Workload,
  {readonly insert: number; readonly update: number}
> = {
  // Cumulative thresholds: [0, insert) insert, [insert, update) update,
  // [update, 1) delete.
  'insert-heavy': {insert: 0.9, update: 0.98},
  'update-heavy': {insert: 0.1, update: 0.95},
  'mixed': {insert: 0.45, update: 0.9},
};

/**
 * Generates a log of at least `targetBytes`, starting at `startSeq` and
 * seeded from `seed`, optionally continuing from rows an earlier log left
 * live (so a base log and a measured log operate on the same key space).
 */
function generateLog(
  workload: Workload,
  targetBytes: number,
  seed: number,
  startSeq: number,
  watermarkOffset: number,
  seedIds?: ReadonlyMap<Table, readonly string[]>,
): GeneratedLog {
  const rand = mulberry32(seed);
  const mix = OP_MIX[workload];
  const live: Record<Table, string[]> = {
    issue: [...(seedIds?.get('issue') ?? [])],
    comment: [...(seedIds?.get('comment') ?? [])],
  };

  const txs: LogicalTx[] = [];
  let bytes = 0;
  let changes = 0;
  let seq = startSeq;

  while (bytes < targetBytes) {
    // OLTP transactions are mostly tiny, with an occasional batch.
    const roll = rand();
    const rows =
      roll < 0.6
        ? 1 + Math.floor(rand() * 3)
        : roll < 0.95
          ? 4 + Math.floor(rand() * 12)
          : 40 + Math.floor(rand() * 200);

    const watermark = versionToLexi(watermarkOffset + txs.length + 1);
    const data: string[] = [];
    let txBytes = beginJSON(watermark).length + commitJSON(watermark).length;

    for (let i = 0; i < rows; i++) {
      const table: Table = rand() < 0.45 ? 'issue' : 'comment';
      const ids = live[table];
      const op = rand();
      let change: ChangeStreamData;

      if (op < mix.insert || ids.length === 0) {
        const id = `${table}-${seq}`;
        ids.push(id);
        change = [
          'data',
          {
            tag: 'insert',
            relation: RELATIONS[table],
            new:
              table === 'issue'
                ? issueRow(rand, id, seq)
                : commentRow(rand, id, seq),
          },
        ];
      } else if (op < mix.update) {
        // Recency-skewed: real workloads re-touch recent rows far more often
        // than uniformly random ones, which is what keeps a page hot.
        const idx = pickRecent(rand, ids.length);
        const id = ids[idx]!;
        change = [
          'data',
          {
            tag: 'update',
            relation: RELATIONS[table],
            key: null,
            new:
              table === 'issue'
                ? issueRow(rand, id, seq)
                : commentRow(rand, id, seq),
          },
        ];
      } else {
        const idx = pickRecent(rand, ids.length);
        const id = ids[idx]!;
        // Swap-remove: order does not matter and it keeps deletion O(1).
        ids[idx] = ids.at(-1)!;
        ids.pop();
        change = [
          'data',
          {tag: 'delete', relation: RELATIONS[table], key: {id}},
        ];
      }

      const json = serializeChangeStreamData(change);
      data.push(json);
      txBytes += json.length;
      changes++;
      seq++;
    }

    txs.push({watermark, data, bytes: txBytes});
    bytes += txBytes;
  }

  return {
    txs,
    bytes,
    changes,
    liveIds: new Map<Table, readonly string[]>([
      ['issue', live.issue],
      ['comment', live.comment],
    ]),
  };
}

/** Biases toward the end of the array (recently touched rows). */
function pickRecent(rand: () => number, length: number): number {
  const r = rand();
  // 70% of picks land in the most recent 10% of rows.
  const span = r < 0.7 ? Math.max(1, Math.floor(length * 0.1)) : length;
  return length - 1 - Math.floor(rand() * span);
}

/**
 * Flattens a log into the entries a replayer actually feeds the processor.
 *
 * With `coalesce > 1`, intermediate begin/commit framing is dropped and the
 * group commits once at the final watermark. Nothing reads a replica while it
 * is catching up, so the intermediate commits are not observable and only the
 * watermark replay stops at has to be exact.
 *
 * The trade is real but small, and it is not zero: every row touched inside a
 * group gets that group's final `_0_version` rather than the version of the
 * transaction that actually changed it. Versions only ever move forward, so
 * this cannot lose a change -- but a ViewSyncer that later restores this
 * replica will see rows as having changed more recently than they did, and
 * re-send some rows to clients that did not need them. The window is bounded
 * by the coalescing factor, so this is a knob to turn deliberately, not one to
 * max out.
 */
function replayEntries(log: GeneratedLog, coalesce: number): string[] {
  const entries: string[] = [];
  for (let i = 0; i < log.txs.length; i += coalesce) {
    const group = log.txs.slice(i, i + coalesce);
    const watermark = group.at(-1)!.watermark;
    entries.push(beginJSON(watermark));
    for (const tx of group) {
      for (const json of tx.data) {
        entries.push(json);
      }
    }
    entries.push(commitJSON(watermark));
  }
  return entries;
}

// ---------------------------------------------------------------------------
// Replica setup
// ---------------------------------------------------------------------------

type Replica = {
  readonly db: Database;
  readonly processor: ChangeProcessor;
  readonly file: DbFile;
  close(): void;
};

function applyReplicaPragmas(db: Database, durability: Durability) {
  db.pragma('journal_mode = WAL');
  // 'production' is literally the replication-manager's config, including
  // `wal_autocheckpoint = 0` -- litestream owns checkpointing there, so the
  // WAL grows for the whole run.
  applyPragmas(db, getPragmaConfig('backup'));
  if (durability === 'production') {
    db.pragma('synchronous = NORMAL');
  } else {
    // Catchup has no reader and no litestream: a crash mid-replay is repaired
    // by replaying again from the last durable watermark, so neither per-commit
    // durability nor a frozen WAL buys anything.
    db.pragma('synchronous = OFF');
    db.pragma('wal_autocheckpoint = 1000');
  }
}

function newProcessor(db: Database): ChangeProcessor {
  return new ChangeProcessor(new StatementRunner(db), 'backup', (_, err) => {
    throw err;
  });
}

/** Creates a fresh replica with the bench schema, via the real DDL path. */
function createReplica(durability: Durability, testName: string): Replica {
  const file = new DbFile(testName);
  const db = file.connect(lc);
  applyReplicaPragmas(db, durability);
  initReplicationState(db, ['zero_logical_log_bench'], versionToLexi(0));

  const processor = newProcessor(db);
  processor.processMessage(lc, [
    'begin',
    {tag: 'begin'},
    {commitWatermark: versionToLexi(1)},
  ]);
  for (const spec of SPECS) {
    processor.processMessage(lc, ['data', createTableMessage(spec)]);
  }
  for (const spec of INDEX_SPECS) {
    processor.processMessage(lc, ['data', {tag: 'create-index', spec}]);
  }
  processor.processMessage(lc, [
    'commit',
    {tag: 'commit'},
    {watermark: versionToLexi(1)},
  ]);

  assertKeyLookupsAreIndexed(db);
  return replicaHandle(db, processor, file);
}

/** Opens a replica restored from a base file; schema is already present. */
function openRestoredReplica(
  templatePath: string,
  durability: Durability,
  testName: string,
): Replica {
  const file = new DbFile(testName);
  copyFileSync(templatePath, file.path);
  const db = file.connect(lc);
  applyReplicaPragmas(db, durability);
  return replicaHandle(db, newProcessor(db), file);
}

/**
 * Fails loudly if a row-key lookup would scan. `ChangeProcessor` emits
 * `UPDATE <table> SET ... WHERE <key>=?` and `DELETE FROM <table> WHERE
 * <key>=?`; if those scan, throughput collapses as the replica grows and the
 * bench reports a number that has nothing to do with applying changes.
 */
function assertKeyLookupsAreIndexed(db: Database) {
  for (const spec of SPECS) {
    const plan = db
      .prepare(`EXPLAIN QUERY PLAN DELETE FROM "${spec.name}" WHERE "id" = 'x'`)
      .all<{detail: string}>()
      .map(({detail}) => detail)
      .join('; ');
    if (!plan.includes('USING INDEX') && !plan.includes('USING PRIMARY KEY')) {
      throw new Error(
        `row-key lookups on "${spec.name}" are not indexed, so this would ` +
          `benchmark table scans: ${plan}`,
      );
    }
  }
}

function replicaHandle(
  db: Database,
  processor: ChangeProcessor,
  file: DbFile,
): Replica {
  return {
    db,
    processor,
    file,
    close() {
      db.close();
      file.delete();
    },
  };
}

/**
 * Fills a replica with `rows` rows, in chunks, without ever materializing the
 * whole base log. The base stands in for a restored snapshot, so it is not
 * part of any measurement -- it only has to leave the replica the right size
 * with a known set of live row keys for the measured log to update and delete.
 */
function preloadBase(
  processor: ChangeProcessor,
  rows: number,
): Map<Table, string[]> {
  const rand = mulberry32(0x5eed);
  const live = new Map<Table, string[]>([
    ['issue', []],
    ['comment', []],
  ]);
  const CHUNK = 20_000;
  let seq = 0;
  let watermark = 2;

  while (seq < rows) {
    const end = Math.min(rows, seq + CHUNK);
    const wm = versionToLexi(watermark++);
    processor.processMessage(lc, [
      'begin',
      {tag: 'begin'},
      {commitWatermark: wm},
    ]);
    for (; seq < end; seq++) {
      const table: Table = rand() < 0.45 ? 'issue' : 'comment';
      // `b`-prefixed so ids from the base never collide with ids the measured
      // log inserts.
      const id = `${table}-b${seq}`;
      must(live.get(table)).push(id);
      processor.processMessage(lc, [
        'data',
        {
          tag: 'insert',
          relation: RELATIONS[table],
          new:
            table === 'issue'
              ? issueRow(rand, id, seq)
              : commentRow(rand, id, seq),
        },
      ]);
    }
    processor.processMessage(lc, ['commit', {tag: 'commit'}, {watermark: wm}]);
  }
  return live;
}

type BaseTemplate = {
  readonly path: string;
  readonly liveIds: ReadonlyMap<Table, readonly string[]>;
  /** Watermarks below this are taken; the measured log must start above it. */
  readonly watermarks: number;
  readonly replicaMB: number;
  close(): void;
};

/**
 * Builds the "restored from S3" base once and returns its path. Callers copy
 * this file per sample, which is exactly the shape of a real restore: land a
 * base snapshot on disk, then replay log on top of it.
 */
function buildBaseTemplate(rows: number): BaseTemplate {
  const replica = createReplica('catchup', 'logical-log-apply-base');
  const liveIds = rows > 0 ? preloadBase(replica.processor, rows) : new Map();
  // Fold the WAL back into the main file so a plain file copy is complete.
  replica.db.pragma('wal_checkpoint(TRUNCATE)');
  const replicaMB = replicaFileMB(replica.db, replica.file.path).dbMB;
  replica.db.close();
  return {
    path: replica.file.path,
    liveIds,
    watermarks: Math.ceil(rows / 20_000) + 4,
    replicaMB,
    close: () => replica.file.delete(),
  };
}

// ---------------------------------------------------------------------------
// Apply loops
// ---------------------------------------------------------------------------

/** Parse only: what reading the log out of S3 costs before any SQLite work. */
function parseOnly(entries: readonly string[]): number {
  const start = performance.now();
  let sink = 0;
  for (const json of entries) {
    const data = BigIntJSON.parse(json) as ChangeStreamData;
    // Touch the result so it cannot be optimized away.
    sink += data.length;
  }
  const elapsed = performance.now() - start;
  if (sink < 0) {
    throw new Error('unreachable');
  }
  return elapsed;
}

/** Parse + valita validation, i.e. what the live subscription path pays. */
function parseAndValidate(entries: readonly string[]): number {
  const start = performance.now();
  let sink = 0;
  for (const json of entries) {
    const data = v.parse(
      BigIntJSON.parse(json),
      changeStreamDataSchema,
      'passthrough',
    );
    sink += data.length;
  }
  const elapsed = performance.now() - start;
  if (sink < 0) {
    throw new Error('unreachable');
  }
  return elapsed;
}

/** Parse + apply: the real cost of replaying a stored log into the replica. */
function parseAndApply(
  processor: ChangeProcessor,
  entries: readonly string[],
): number {
  const start = performance.now();
  for (const json of entries) {
    processor.processMessage(lc, BigIntJSON.parse(json) as ChangeStreamData);
  }
  return performance.now() - start;
}

/**
 * Confirms the replay actually landed. `ChangeProcessor` reports failures
 * through its `failService` callback rather than by throwing from
 * `processMessage`, and a processor that has failed silently drops every
 * subsequent message -- which would look like excellent throughput.
 */
function verifyApplied(db: Database, log: GeneratedLog) {
  const [{stateVersion}] = db
    .prepare(`SELECT stateVersion FROM "_zero.replicationState"`)
    .all<{stateVersion: string}>();
  const expectedVersion = must(log.txs.at(-1)).watermark;
  if (stateVersion !== expectedVersion) {
    throw new Error(
      `replay stopped early: replica is at ${stateVersion}, ` +
        `log ends at ${expectedVersion}`,
    );
  }
  for (const [table, ids] of log.liveIds) {
    const [{n}] = db
      .prepare(`SELECT COUNT(*) AS n FROM "${table}"`)
      .all<{n: number}>();
    if (n !== ids.length) {
      throw new Error(
        `replay of "${table}" produced ${n} rows, expected ${ids.length}`,
      );
    }
  }
}

// ---------------------------------------------------------------------------
// Reporting
// ---------------------------------------------------------------------------

type CaseResult = {
  readonly name: string;
  readonly workload: Workload;
  readonly coalesce: number;
  readonly durability: Durability;
  readonly baseRows: number;
  readonly baseMB: number;
  readonly logMB: number;
  readonly changes: number;
  readonly transactions: number;
  readonly commits: number;
  readonly applyMBPerSecond: readonly number[];
  readonly parseMBPerSecond: readonly number[];
  readonly validateMBPerSecond: readonly number[];
  readonly changesPerSecond: readonly number[];
  readonly replicaMB: number;
  readonly walMB: number;
};

/**
 * Median rather than mean: a single sample perturbed by a GC pause or a
 * checkpoint should not move the headline number.
 */
function median(values: readonly number[]): number {
  const sorted = values.toSorted((a, b) => a - b);
  const mid = Math.floor(sorted.length / 2);
  return sorted.length % 2 === 0
    ? (sorted[mid - 1]! + sorted[mid]!) / 2
    : sorted[mid]!;
}

function replicaFileMB(
  db: Database,
  path: string,
): {dbMB: number; walMB: number} {
  const [{page_size: pageSize}] = db.pragma<{page_size: number}>('page_size');
  const [{page_count: pageCount}] = db.pragma<{page_count: number}>(
    'page_count',
  );
  return {
    dbMB: (pageSize * pageCount) / BYTES_PER_MB,
    walMB: existsSync(`${path}-wal`)
      ? statSync(`${path}-wal`).size / BYTES_PER_MB
      : 0,
  };
}

function fmt(n: number, digits = 1): string {
  return n.toFixed(digits);
}

function printResults(title: string, results: readonly CaseResult[]) {
  if (results.length === 0) {
    return;
  }
  console.log('');
  console.log(
    `=== ${title} ` +
      `(log ${fmt(TARGET_MB, 0)} MB/sample, ${REPS} sample(s)) ===`,
  );
  console.log('');
  const header = [
    'workload'.padEnd(13),
    'coalesce'.padStart(8),
    'durab'.padStart(11),
    'base MB'.padStart(9),
    'MB/s'.padStart(8),
    'changes/s'.padStart(11),
    'parse MB/s'.padStart(11),
    'apply MB/s'.padStart(11),
    'replica MB'.padStart(11),
    'WAL MB'.padStart(8),
  ].join(' ');
  console.log(header);
  console.log('-'.repeat(header.length));
  for (const r of results) {
    const total = median(r.applyMBPerSecond);
    const parse = median(r.parseMBPerSecond);
    // Apply-only, derived: 1/total = 1/parse + 1/apply.
    const applyOnly = 1 / Math.max(1e-9, 1 / total - 1 / parse);
    console.log(
      [
        r.workload.padEnd(13),
        String(r.coalesce).padStart(8),
        r.durability.padStart(11),
        fmt(r.baseMB, 0).padStart(9),
        fmt(total).padStart(8),
        Math.round(median(r.changesPerSecond)).toLocaleString().padStart(11),
        fmt(parse).padStart(11),
        fmt(applyOnly).padStart(11),
        fmt(r.replicaMB).padStart(11),
        fmt(r.walMB).padStart(8),
      ].join(' '),
    );
  }

  console.log('');
  console.log('=== projected catchup time (mm:ss) ===');
  console.log('');
  const projHeader = [
    'workload'.padEnd(13),
    'coalesce'.padStart(8),
    'durab'.padStart(11),
    'base MB'.padStart(9),
    ...PROJECTIONS.map(mb => `${fmt(mb, 0)} MB`.padStart(10)),
  ].join(' ');
  console.log(projHeader);
  console.log('-'.repeat(projHeader.length));
  for (const r of results) {
    const mbps = median(r.applyMBPerSecond);
    console.log(
      [
        r.workload.padEnd(13),
        String(r.coalesce).padStart(8),
        r.durability.padStart(11),
        fmt(r.baseMB, 0).padStart(9),
        ...PROJECTIONS.map(mb => duration(mb / mbps).padStart(10)),
      ].join(' '),
    );
  }
  console.log('');
  console.log(
    'Validation cost (valita, as paid by the live subscription path):',
  );
  for (const r of results) {
    const parse = median(r.parseMBPerSecond);
    const validate = median(r.validateMBPerSecond);
    console.log(
      `  ${r.name}: parse ${fmt(parse)} MB/s -> parse+validate ` +
        `${fmt(validate)} MB/s`,
    );
  }
  console.log('');
}

/**
 * What the log costs to keep in S3 and to pull back down. The apply ceiling is
 * expressed in uncompressed MB because that is what drives CPU, but the bytes
 * that cross the network are these. gzip level 1 stands in for "cheap
 * compression you would actually enable on a hot write path".
 */
function reportCompressibility(log: GeneratedLog, entries: readonly string[]) {
  const raw = Buffer.from(entries.join('\n'), 'utf8');
  const start = performance.now();
  const gz = gzipSync(raw, {level: 1});
  const elapsedMs = performance.now() - start;
  const rawMB = raw.length / BYTES_PER_MB;
  console.log('');
  console.log('=== logical log storage/transfer ===');
  console.log('');
  console.log(
    `  uncompressed      ${fmt(rawMB)} MB ` +
      `(${log.changes.toLocaleString()} changes, ` +
      `${Math.round(raw.length / log.changes)} bytes/change)`,
  );
  console.log(
    `  gzip -1           ${fmt(gz.length / BYTES_PER_MB)} MB ` +
      `(${fmt(raw.length / gz.length, 2)}x smaller, ` +
      `${fmt((rawMB * 1000) / elapsedMs)} MB/s to compress)`,
  );
  console.log(
    `  => 1 GB of uncompressed log is ` +
      `${fmt(1024 / (raw.length / gz.length))} MB in S3`,
  );
  console.log('');
}

function duration(seconds: number): string {
  const s = Math.round(seconds);
  const m = Math.floor(s / 60);
  return `${m}:${String(s % 60).padStart(2, '0')}`;
}

// ---------------------------------------------------------------------------
// Cases
// ---------------------------------------------------------------------------

/**
 * The base log, the per-workload measured logs, and the base replica file are
 * all built once and shared: every case then replays byte-identical input, so
 * differences between rows in the report are differences in the apply path and
 * nothing else.
 */
type Fixture = {
  readonly baseRows: number;
  readonly baseMB: number;
  readonly basePath: string;
  readonly logs: ReadonlyMap<Workload, GeneratedLog>;
  close(): void;
};

function buildFixture(
  baseRows: number,
  workloads: readonly Workload[] = WORKLOADS,
): Fixture {
  const base = buildBaseTemplate(baseRows);
  const logs = new Map<Workload, GeneratedLog>();
  for (const workload of workloads) {
    logs.set(
      workload,
      generateLog(
        workload,
        TARGET_MB * BYTES_PER_MB,
        0xc0ffee,
        0,
        // Start past the base's watermarks so replay only moves forward.
        base.watermarks,
        base.liveIds,
      ),
    );
  }
  return {
    baseRows,
    baseMB: base.replicaMB,
    basePath: base.path,
    logs,
    close: () => base.close(),
  };
}

/** Runs a small prefix through both paths so measurements start warm. */
function warmUp(
  fixture: Fixture,
  durability: Durability,
  entries: readonly string[],
) {
  // Cut on a commit boundary so the warm-up replica is never closed with a
  // transaction still open.
  let end = Math.min(entries.length, 4000);
  while (end > 0 && !entries[end - 1]!.startsWith('["commit"')) {
    end--;
  }
  if (end === 0) {
    end = entries.length;
  }
  const slice = entries.slice(0, end);

  parseOnly(slice);
  parseAndValidate(slice);
  const replica = openRestoredReplica(
    fixture.basePath,
    durability,
    'logical-log-apply-warmup',
  );
  try {
    parseAndApply(replica.processor, slice);
  } finally {
    replica.close();
  }
}

function runCase(
  fixture: Fixture,
  workload: Workload,
  coalesce: number,
  durability: Durability,
): CaseResult {
  const name = `${workload} coalesce=${coalesce} ${durability}`;
  const log = must(fixture.logs.get(workload));
  const entries = replayEntries(log, coalesce);
  const logMB = log.bytes / BYTES_PER_MB;

  // One unrecorded pass so the first recorded sample is not paying for JIT
  // warm-up of the parse and apply paths.
  warmUp(fixture, durability, entries);

  const applyMBPerSecond: number[] = [];
  const changesPerSecond: number[] = [];
  const parseMBPerSecond: number[] = [];
  const validateMBPerSecond: number[] = [];
  const commitCount = Math.ceil(log.txs.length / coalesce);
  let replicaMB = 0;
  let walMB = 0;

  for (let rep = 0; rep < REPS; rep++) {
    // Parse and validate do not touch the replica, so they are timed outside
    // the restore.
    parseMBPerSecond.push((logMB * 1000) / parseOnly(entries));
    validateMBPerSecond.push((logMB * 1000) / parseAndValidate(entries));

    const replica = openRestoredReplica(
      fixture.basePath,
      durability,
      'logical-log-apply-bench',
    );
    try {
      const applyMs = parseAndApply(replica.processor, entries);
      verifyApplied(replica.db, log);
      applyMBPerSecond.push((logMB * 1000) / applyMs);
      changesPerSecond.push((log.changes * 1000) / applyMs);
      const files = replicaFileMB(replica.db, replica.file.path);
      replicaMB = files.dbMB;
      walMB = files.walMB;
    } finally {
      replica.close();
    }
  }

  const result: CaseResult = {
    name,
    workload,
    coalesce,
    durability,
    baseRows: fixture.baseRows,
    baseMB: fixture.baseMB,
    logMB,
    changes: log.changes,
    transactions: log.txs.length,
    commits: commitCount,
    applyMBPerSecond,
    parseMBPerSecond,
    validateMBPerSecond,
    changesPerSecond,
    replicaMB,
    walMB,
  };

  benchmarkRecorder.recordThroughputSamples(
    `${name} log MB`,
    applyMBPerSecond.map(mbps => ({elapsedMs: 1000, operations: mbps})),
  );
  benchmarkRecorder.recordThroughputSamples(
    `${name} changes`,
    changesPerSecond.map(cps => ({elapsedMs: 1000, operations: cps})),
  );

  return result;
}

describe('replicator/logical-log apply ceiling', () => {
  // How fast can a stored log be applied, and what does each lever buy?
  test('catchup sweep', {timeout: TEST_TIMEOUT_MS}, () => {
    const rows: CaseResult[] = [];
    let fixtureLog: {log: GeneratedLog; entries: string[]} | undefined;
    const fixture = buildFixture(BASE_ROWS);
    try {
      for (const workload of WORKLOADS) {
        for (const coalesce of COALESCE) {
          rows.push(runCase(fixture, workload, coalesce, 'production'));
        }
        rows.push(runCase(fixture, workload, must(COALESCE.at(-1)), 'catchup'));
      }
      const log = must(fixture.logs.get(must(WORKLOADS.at(-1))));
      fixtureLog = {log, entries: replayEntries(log, 1)};
    } finally {
      fixture.close();
    }
    printResults('logical-log apply ceiling', rows);
    const sample = must(fixtureLog);
    reportCompressibility(sample.log, sample.entries);
  });

  // Apply throughput is not a constant: it falls as the replica grows, which
  // is what decides whether a log-replay restore is viable at real replica
  // sizes rather than at bench sizes.
  test('replica size sensitivity', {timeout: TEST_TIMEOUT_MS}, () => {
    const workload: Workload = must(WORKLOADS.at(-1));
    const coalesce = must(COALESCE.at(-1));
    const rows: CaseResult[] = [];
    for (const baseRows of BASE_SWEEP) {
      const fixture = buildFixture(baseRows, [workload]);
      try {
        rows.push(runCase(fixture, workload, coalesce, 'production'));
        rows.push(runCase(fixture, workload, coalesce, 'catchup'));
      } finally {
        fixture.close();
      }
    }
    printResults('apply throughput vs replica size', rows);
  });
});
