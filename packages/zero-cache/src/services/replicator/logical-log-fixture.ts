/**
 * Shared fixture for the logical-log benchmarks: a deterministic generator that
 * produces a CDC change stream in the canonical downstream wire form, plus the
 * table and index specs that stream targets.
 *
 * Lives outside the bench files so that more than one of them can replay
 * byte-identical input -- the apply-ceiling bench measures what the current
 * path costs, and the applier-headroom bench measures how much of that a
 * purpose-built replayer could take back. Comparing those two only means
 * something if the input is the same.
 */
import type {IndexSpec, TableSpec} from '../../db/specs.ts';
import {versionToLexi} from '../../types/lexi-version.ts';
import type {
  MessageRelation,
  TableCreate,
} from '../change-source/protocol/current/data.ts';
import type {ChangeStreamData} from '../change-source/protocol/current/downstream.ts';
import {serializeChangeStreamData} from '../change-streamer/change-log-codec.ts';

export const BYTES_PER_MB = 1024 * 1024;

export type Workload = 'insert-heavy' | 'update-heavy' | 'mixed';

// ---------------------------------------------------------------------------
// Deterministic data generation
// ---------------------------------------------------------------------------

/** Deterministic PRNG so every case sees byte-identical input. */
export function mulberry32(seed: number): () => number {
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
export const CORPUS = (() => {
  const words =
    'the quick brown fox jumps over a lazy dog while zero syncs rows from ' +
    'postgres into sqlite replicas and view syncers incrementally maintain ' +
    'queries for connected clients without polling or refetching anything ';
  return words.repeat(24);
})();

export function text(rand: () => number, min: number, max: number): string {
  const len = min + Math.floor(rand() * (max - min));
  const start = Math.floor(rand() * (CORPUS.length - len - 1));
  return CORPUS.slice(start, start + len);
}

/**
 * A wide table and a narrow table, so the log carries a realistic mix of
 * per-change sizes rather than one uniform row shape.
 */
export const ISSUE_SPEC: TableSpec = {
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

export const COMMENT_SPEC: TableSpec = {
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

export const SPECS = [ISSUE_SPEC, COMMENT_SPEC] as const;

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
export const INDEX_SPECS: readonly IndexSpec[] = [
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

export function relationOf(spec: TableSpec): MessageRelation {
  return {
    schema: spec.schema,
    name: spec.name,
    rowKey: {type: 'default', columns: [...spec.primaryKey!]},
  };
}

export const RELATIONS = {
  issue: relationOf(ISSUE_SPEC),
  comment: relationOf(COMMENT_SPEC),
} as const;

export function createTableMessage(spec: TableSpec): TableCreate {
  return {
    tag: 'create-table',
    spec,
    metadata: {
      rowKey: {type: 'default', columns: [...spec.primaryKey!]},
    },
  };
}

export type Table = keyof typeof RELATIONS;

export type Row = Record<string, string | number | boolean | null>;

export function issueRow(rand: () => number, id: string, seq: number): Row {
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

export function commentRow(rand: () => number, id: string, seq: number): Row {
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
export type LogicalTx = {
  readonly watermark: string;
  readonly data: readonly string[];
  /** Serialized bytes of this tx as it would be stored, framing included. */
  readonly bytes: number;
};

export type GeneratedLog = {
  readonly txs: readonly LogicalTx[];
  /** Total bytes the log occupies in storage, uncompressed. */
  readonly bytes: number;
  readonly changes: number;
  /** Live row ids at the end of the log, for chaining a base into a measure. */
  readonly liveIds: ReadonlyMap<Table, readonly string[]>;
};

export function beginJSON(watermark: string): string {
  return `["begin",{"tag":"begin"},{"commitWatermark":"${watermark}"}]`;
}

export function commitJSON(watermark: string): string {
  return `["commit",{"tag":"commit"},{"watermark":"${watermark}"}]`;
}

export const OP_MIX: Record<
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
export function generateLog(
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
export function pickRecent(rand: () => number, length: number): number {
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
export function replayEntries(log: GeneratedLog, coalesce: number): string[] {
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
