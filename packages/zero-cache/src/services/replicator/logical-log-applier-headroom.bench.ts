/* oxlint-disable no-console */

// How much of the logical-log apply cost is recoverable?
//
// A CPU profile of `logical-log-apply.bench.ts` splits the per-change cost
// roughly 41% inside SQLite, 27% in `BigIntJSON.parse`, and ~20% in per-change
// JS on the way there. A profile says what is *reducible*; it does not say what
// is *recoverable*. This measures the difference by actually building the
// faster paths and replaying byte-identical input through them.
//
// Four variants, same log, same base replica, same pragmas:
//
//   baseline    ChangeProcessor + BigIntJSON.parse -- what runs today.
//   json-fast   FastApplier + native JSON.parse.
//   direct      DirectApplier + native JSON.parse. Same idea as FastApplier
//               with the last per-row allocations removed, so a row costs a
//               shape check and a bind into a reused array.
//   binary      DirectApplier + a compact binary log format. Shares the
//               applier with 'direct', so the only difference is the format.
//
// ## The FastApplier is a spike, not a proposal
//
// It handles insert, update, delete, begin and commit, and nothing else: no
// schema changes, no backfill, no truncate, no JSON or array columns, no
// oversized-integer handling, no change-log, no table-metadata tracking. Those
// omissions are what make it a useful *upper bound* on the data path, and
// exactly why it is not a drop-in replacement for `ChangeProcessor`. Read its
// numbers as "the ceiling a purpose-built replayer is aiming at", not as "the
// speed we would get".
//
// Every variant is checksummed against the baseline's resulting replica, so a
// variant that is fast because it quietly skipped work fails rather than wins.
//
// ## The binary format
//
// Deliberately dumb: one byte of op, one byte of table id, a column count, then
// tagged values. It exists to price the *format*, not to propose an encoding --
// a real one would want a schema-versioned column bitmap and a string table.
// Encoding happens outside the measured region, as it would in the
// change-streamer, which already stringifies once and fans the result out.
//
// ## Measured result
//
// Against a 977 MB replica, median of 8: json-fast 1.51x the baseline, direct
// 1.57x, binary 1.69x. At a 12 MB replica: 2.23x, 2.34x, 2.98x.
//
// Most of the applier win is simply *having* cached statements plus native
// `JSON.parse`; removing the last per-row allocations (`direct`) adds only ~4%
// on top of `json-fast`.
//
// The binary format is worth ~8% over native `JSON.parse` at 977 MB and ~28% at
// 12 MB. It does not buy much storage though: 1.49x smaller raw but only 1.12x
// smaller after gzip -1, because a compact encoding and a general-purpose
// compressor chase the same redundancy and the encoding gets there first
// (10.8x compression vs 14.4x). So the applier is the clear win and the format
// is a judgement call resting on throughput alone.
//
// > Correction: an earlier version of this header reported the binary format as
// > *slower* than native `JSON.parse` (1.36x vs 1.56x). That was noise at 3
// > samples, where the pair flipped order between runs, and it was measured with
// > binary on `FastApplier` while json-fast had its own. Both formats now drive
// > the same applier so only the encoding differs, and at 8 samples binary is
// > consistently ahead.
//
// The ceiling is bounded, but not by as much as a CPU profile of the baseline
// suggests. That profile attributes ~41% of per-change cost to "native SQLite";
// the C harness in `bench/logical-log-ceiling/` does the *entire* job in 18.1 us
// against this replica, so that frame was charging better-sqlite3's napi
// marshalling to SQLite. Measured end to end, the gap from the baseline to C is
// 3.4x, of which ~2.1x is the JS/native boundary.
//
// ## Levers that were measured and did not work
//
// Recorded here so they are not re-tried blind. All against a 977 MB replica,
// mixed workload, replay pragmas.
//
//   page_size 8K / 16K      worse, 5.6 -> 4.7 -> 4.0 MB/s. Random single-row
//                           updates pay for the whole page they touch, and a
//                           shallower B-tree does not make that back.
//   journal_mode = wal2     slower than wal (6.5 vs 7.4 MB/s). wal2 exists so
//                           readers cannot starve a checkpoint; replay has no
//                           readers, so it is overhead with no upside.
//   mmap_size = 2 GB        ~8-15%, at or inside this machine's run-to-run
//                           noise; measured again in C at ~8%.
//   BEGIN CONCURRENT        worse, and decisively. This SQLite build (3.54.0)
//     across 2-4 writers     does support it, so the idea is testable rather
//                           than impossible. Partitioned by primary-key hash:
//                           13.6 -> 8.7 -> 8.6 MB/s with retries climbing
//                           76 -> 356. Partitioned by table, so two writers
//                           share no B-tree at all: 13.2 -> 9.3 MB/s with
//                           *zero* retries, which rules out conflicts as the
//                           explanation. What is left is shared by every
//                           writer regardless of what they touch: the WAL
//                           append, the commit's snapshot validation, and a
//                           page cache that fragments per connection.
//
// The conclusion those share: SQLite writes do not parallelize within one
// database file. They would across separate files -- separate files mean
// separate write locks -- so a replica sharded per table is the version of
// this idea that could work. Measured, partitioning by table across separate
// files: 13.2 -> 22.7 MB/s over 1/2 writers (1.72x), against 1.83x for a
// perfectly balanced hash partition. The bound is 1/(largest table's share of
// writes), not core count.
//
// What sharding costs is *not* cross-table reads: Zero joins in IVM, not in
// SQL -- `TableSource` only emits single-table queries and `Join` is an IVM
// operator -- so SQLite never joins across replica tables. The coupling is in
// `Snapshotter`, which holds one `BEGIN CONCURRENT` snapshot per database and
// reads the change log at that snapshot. See LOGICAL_LOG_REPLAY_BENCHMARK.md
// section 8.1.
//
//   pnpm --filter zero-cache run bench logical-log-applier-headroom
//
// Knobs:
//   HEADROOM_TARGET_MB    log size per sample (default 24)
//   HEADROOM_REPS         samples per variant (default 3)
//   HEADROOM_BASE_ROWS    base replica size (default 2,000,000)
//   HEADROOM_WORKLOAD     insert-heavy | update-heavy | mixed
//   HEADROOM_COALESCE     logical transactions per SQLite commit (default 256)

import {copyFileSync} from 'node:fs';
import {gzipSync} from 'node:zlib';
import {describe, expect, test} from 'vitest';
import {createManualBenchmarkRecorder} from '../../../../shared/src/bench.ts';
import {BigIntJSON} from '../../../../shared/src/bigint-json.ts';
import {createSilentLogContext} from '../../../../shared/src/logging-test-utils.ts';
import {must} from '../../../../shared/src/must.ts';
import type {Database, Statement} from '../../../../zqlite/src/db.ts';
import {StatementRunner} from '../../db/statements.ts';
import {DbFile} from '../../test/lite.ts';
import {versionToLexi} from '../../types/lexi-version.ts';
import type {ChangeStreamData} from '../change-source/protocol/current/downstream.ts';
import {ChangeProcessor} from './change-processor.ts';
import {
  BYTES_PER_MB,
  createTableMessage,
  generateLog,
  INDEX_SPECS,
  issueRow,
  commentRow,
  mulberry32,
  RELATIONS,
  replayEntries,
  SPECS,
  type GeneratedLog,
  type Table,
  type Workload,
} from './logical-log-fixture.ts';
import {initReplicationState} from './schema/replication-state.ts';

const lc = createSilentLogContext();
const benchmarkRecorder = createManualBenchmarkRecorder();
const TEST_TIMEOUT_MS = 30 * 60_000;

function envNumber(name: string, fallback: number): number {
  const raw = process.env[name];
  if (raw === undefined || raw === '') {
    return fallback;
  }
  const parsed = Number(raw);
  if (!Number.isFinite(parsed) || parsed < 0) {
    throw new Error(`${name} must be a non-negative number, got "${raw}"`);
  }
  return parsed;
}

const TARGET_MB = envNumber('HEADROOM_TARGET_MB', 24);
const REPS = Math.max(1, Math.floor(envNumber('HEADROOM_REPS', 3)));
const BASE_ROWS = Math.floor(envNumber('HEADROOM_BASE_ROWS', 2_000_000));
const COALESCE = Math.max(1, Math.floor(envNumber('HEADROOM_COALESCE', 256)));
const WORKLOAD = (process.env.HEADROOM_WORKLOAD ?? 'mixed') as Workload;

const ZERO_VERSION = '_0_version';

// ---------------------------------------------------------------------------
// FastApplier
// ---------------------------------------------------------------------------

/**
 * A minimal applier for the data path only.
 *
 * The two things it does differently from `ChangeProcessor`:
 *
 *  - it caches a prepared statement per (op, table, column-set) and binds
 *    positionally, instead of rebuilding the SQL text from `Object.keys(row)`
 *    for every row and re-normalizing its whitespace on every cache lookup;
 *  - it takes values that are already in SQLite's domain, so there is no
 *    per-row type conversion.
 *
 * Everything else it omits rather than optimizes -- see the file header.
 */
class FastApplier {
  readonly #db: Database;
  readonly #stmts = new Map<string, Statement>();
  readonly #begin: Statement;
  readonly #commit: Statement;
  readonly #setVersion: Statement;
  #version = '';

  constructor(db: Database) {
    this.#db = db;
    this.#begin = db.prepare('BEGIN IMMEDIATE');
    this.#commit = db.prepare('COMMIT');
    this.#setVersion = db.prepare(
      `UPDATE "_zero.replicationState" SET stateVersion = ?`,
    );
  }

  #stmt(key: string, sql: () => string): Statement {
    let stmt = this.#stmts.get(key);
    if (stmt === undefined) {
      stmt = this.#db.prepare(sql());
      this.#stmts.set(key, stmt);
    }
    return stmt;
  }

  begin(watermark: string) {
    this.#version = watermark;
    this.#begin.run();
  }

  commit(watermark: string) {
    this.#setVersion.run(watermark);
    this.#commit.run();
  }

  /** `values` is bound positionally against `cols`; `_0_version` is appended. */
  insert(table: string, cols: readonly string[], values: readonly unknown[]) {
    const key = `i|${table}|${cols.join(',')}`;
    const stmt = this.#stmt(key, () => {
      const names = [...cols, ZERO_VERSION].map(c => `"${c}"`).join(',');
      const slots = Array.from({length: cols.length + 1})
        .fill('?')
        .join(',');
      return `INSERT OR REPLACE INTO "${table}" (${names}) VALUES (${slots})`;
    });
    stmt.run(...values, this.#version);
  }

  update(
    table: string,
    cols: readonly string[],
    values: readonly unknown[],
    keyCol: string,
    keyValue: unknown,
  ) {
    const key = `u|${table}|${cols.join(',')}`;
    const stmt = this.#stmt(key, () => {
      const sets = [...cols, ZERO_VERSION].map(c => `"${c}"=?`).join(',');
      return `UPDATE "${table}" SET ${sets} WHERE "${keyCol}"=?`;
    });
    const {changes} = stmt.run(...values, this.#version, keyValue);
    if (changes === 0) {
      // Same resumptive-replication fallback ChangeProcessor performs, so the
      // two paths cannot diverge on a row that was never synced.
      this.insert(table, cols, values);
    }
  }

  delete(table: string, keyCol: string, keyValue: unknown) {
    const key = `d|${table}|${keyCol}`;
    const stmt = this.#stmt(
      key,
      () => `DELETE FROM "${table}" WHERE "${keyCol}"=?`,
    );
    stmt.run(keyValue);
  }
}

/**
 * The same idea as `FastApplier`, with the last per-row allocations removed.
 *
 * `FastApplier` still builds a cache-key string (`\`i|${table}|${cols.join()}\``)
 * and spreads a values array on every single row -- the same class of per-row
 * work as rebuilding the SQL text, just cheaper. This one keeps the resolved
 * `Statement` on a per-table shape list, recognises the steady-state shape by
 * comparing column names in place, and binds from a caller-owned array that is
 * reused across rows. In steady state a row costs a length check, a few string
 * compares, and the bind.
 */
type Shape = {
  readonly cols: readonly string[];
  readonly insert: Statement;
  readonly update: Statement;
};

function sameShape(a: readonly string[], b: readonly string[]): boolean {
  if (a.length !== b.length) {
    return false;
  }
  for (let i = 0; i < a.length; i++) {
    if (a[i] !== b[i]) {
      return false;
    }
  }
  return true;
}

class DirectApplier {
  readonly #db: Database;
  /** Per table, most-recently-used first; steady state hits index 0. */
  readonly #shapes = new Map<string, Shape[]>();
  readonly #deletes = new Map<string, Statement>();
  readonly #begin: Statement;
  readonly #commit: Statement;
  readonly #setVersion: Statement;
  #version = '';

  constructor(db: Database) {
    this.#db = db;
    this.#begin = db.prepare('BEGIN IMMEDIATE');
    this.#commit = db.prepare('COMMIT');
    this.#setVersion = db.prepare(
      `UPDATE "_zero.replicationState" SET stateVersion = ?`,
    );
  }

  begin(watermark: string) {
    this.#version = watermark;
    this.#begin.run();
  }

  commit(watermark: string) {
    this.#setVersion.run(watermark);
    this.#commit.run();
  }

  #shapeFor(table: string, cols: readonly string[]): Shape {
    let list = this.#shapes.get(table);
    if (list === undefined) {
      list = [];
      this.#shapes.set(table, list);
    }
    for (let i = 0; i < list.length; i++) {
      const shape = list[i]!;
      if (sameShape(shape.cols, cols)) {
        if (i !== 0) {
          list.splice(i, 1);
          list.unshift(shape);
        }
        return shape;
      }
    }
    const owned = [...cols];
    const names = [...owned, ZERO_VERSION].map(c => `"${c}"`).join(',');
    const slots = Array.from({length: owned.length + 1})
      .fill('?')
      .join(',');
    const sets = [...owned, ZERO_VERSION].map(c => `"${c}"=?`).join(',');
    const shape: Shape = {
      cols: owned,
      insert: this.#db.prepare(
        `INSERT OR REPLACE INTO "${table}" (${names}) VALUES (${slots})`,
      ),
      update: this.#db.prepare(`UPDATE "${table}" SET ${sets} WHERE "id"=?`),
    };
    list.unshift(shape);
    return shape;
  }

  /**
   * `values` is the caller's reused array and is mutated: the watermark (and,
   * for an update, the row key) are appended before binding. The caller resets
   * its length each row, so no allocation happens per row.
   */
  insert(table: string, cols: readonly string[], values: unknown[]) {
    const shape = this.#shapeFor(table, cols);
    values.push(this.#version);
    shape.insert.run(values);
  }

  update(
    table: string,
    cols: readonly string[],
    values: unknown[],
    keyValue: unknown,
  ) {
    const shape = this.#shapeFor(table, cols);
    values.push(this.#version, keyValue);
    const {changes} = shape.update.run(values);
    if (changes === 0) {
      values.length -= 2;
      this.insert(table, cols, values);
    }
  }

  delete(table: string, keyValue: unknown) {
    let stmt = this.#deletes.get(table);
    if (stmt === undefined) {
      stmt = this.#db.prepare(`DELETE FROM "${table}" WHERE "id"=?`);
      this.#deletes.set(table, stmt);
    }
    stmt.run(keyValue);
  }
}

// ---------------------------------------------------------------------------
// Binary log format
// ---------------------------------------------------------------------------

const OP_BEGIN = 0;
const OP_COMMIT = 1;
const OP_INSERT = 2;
const OP_UPDATE = 3;
const OP_DELETE = 4;

const TAG_NULL = 0;
const TAG_DOUBLE = 1;
const TAG_STRING = 2;
const TAG_TRUE = 3;
const TAG_FALSE = 4;

const TABLE_IDS: readonly Table[] = ['issue', 'comment'];
/** Column order per table id, so a record carries indexes rather than names. */
const TABLE_COLUMNS: readonly (readonly string[])[] = TABLE_IDS.map(t =>
  Object.keys(must(SPECS.find(s => s.name === t)).columns),
);
const TABLE_INDEX = new Map(TABLE_IDS.map((t, i) => [t as string, i]));

function encodeString(chunks: Buffer[], s: string) {
  const body = Buffer.from(s, 'utf8');
  const len = Buffer.allocUnsafe(4);
  len.writeUInt32LE(body.length, 0);
  chunks.push(len, body);
}

function encodeValue(chunks: Buffer[], value: unknown) {
  if (value === null || value === undefined) {
    chunks.push(Buffer.from([TAG_NULL]));
  } else if (typeof value === 'boolean') {
    chunks.push(Buffer.from([value ? TAG_TRUE : TAG_FALSE]));
  } else if (typeof value === 'number') {
    const b = Buffer.allocUnsafe(9);
    b.writeUInt8(TAG_DOUBLE, 0);
    b.writeDoubleLE(value, 1);
    chunks.push(b);
  } else {
    chunks.push(Buffer.from([TAG_STRING]));
    encodeString(chunks, String(value));
  }
}

/** Encodes a replay stream. Runs outside the measured region by design. */
function encodeBinaryLog(entries: readonly string[]): Buffer {
  const chunks: Buffer[] = [];
  for (const json of entries) {
    const msg = BigIntJSON.parse(json) as ChangeStreamData;
    const [type] = msg;
    if (type === 'begin') {
      chunks.push(Buffer.from([OP_BEGIN]));
      encodeString(chunks, msg[2].commitWatermark);
      continue;
    }
    if (type === 'commit') {
      chunks.push(Buffer.from([OP_COMMIT]));
      encodeString(chunks, msg[2].watermark);
      continue;
    }
    if (type !== 'data') {
      throw new Error(`unexpected message ${type}`);
    }
    const change = msg[1];
    const tableId = must(
      TABLE_INDEX.get((change as {relation: {name: string}}).relation.name),
    );
    const columns = TABLE_COLUMNS[tableId]!;

    if (change.tag === 'delete') {
      chunks.push(Buffer.from([OP_DELETE, tableId]));
      encodeValue(chunks, (change.key as Record<string, unknown>).id);
      continue;
    }
    if (change.tag !== 'insert' && change.tag !== 'update') {
      throw new Error(`unexpected change ${change.tag}`);
    }
    const row = change.new as Record<string, unknown>;
    const present: number[] = [];
    for (let i = 0; i < columns.length; i++) {
      if (columns[i]! in row) {
        present.push(i);
      }
    }
    chunks.push(
      Buffer.from([
        change.tag === 'insert' ? OP_INSERT : OP_UPDATE,
        tableId,
        present.length,
      ]),
    );
    for (const i of present) {
      chunks.push(Buffer.from([i]));
      encodeValue(chunks, row[columns[i]!]);
    }
  }
  return Buffer.concat(chunks);
}

// ---------------------------------------------------------------------------
// Apply loops
// ---------------------------------------------------------------------------

function applyBaseline(
  processor: ChangeProcessor,
  entries: readonly string[],
): number {
  const start = performance.now();
  for (const json of entries) {
    processor.processMessage(lc, BigIntJSON.parse(json) as ChangeStreamData);
  }
  return performance.now() - start;
}

function applyJsonFast(
  applier: FastApplier,
  entries: readonly string[],
): number {
  const start = performance.now();
  const cols: string[] = [];
  const vals: unknown[] = [];
  for (const json of entries) {
    const msg = JSON.parse(json) as ChangeStreamData;
    const [type] = msg;
    if (type === 'begin') {
      applier.begin(msg[2].commitWatermark);
      continue;
    }
    if (type === 'commit') {
      applier.commit(msg[2].watermark);
      continue;
    }
    const change = msg[1] as {
      tag: string;
      relation: {name: string};
      new?: Record<string, unknown>;
      key?: Record<string, unknown>;
    };
    const table = change.relation.name;
    if (change.tag === 'delete') {
      applier.delete(table, 'id', must(change.key).id);
      continue;
    }
    const row = must(change.new);
    cols.length = 0;
    vals.length = 0;
    for (const k in row) {
      const v = row[k];
      cols.push(k);
      // SQLite has no boolean; this is the whole of what `liteRow` does for a
      // schema with no JSON or array columns, and a real applier still pays it.
      vals.push(typeof v === 'boolean' ? (v ? 1 : 0) : (v as unknown));
    }
    if (change.tag === 'insert') {
      applier.insert(table, cols, vals);
    } else {
      applier.update(table, cols, vals, 'id', row.id);
    }
  }
  return performance.now() - start;
}

/** json-fast, minus the per-row key building and array spread. */
function applyDirect(
  applier: DirectApplier,
  entries: readonly string[],
): number {
  const start = performance.now();
  const cols: string[] = [];
  const vals: unknown[] = [];
  for (const json of entries) {
    const msg = JSON.parse(json) as ChangeStreamData;
    const [type] = msg;
    if (type === 'begin') {
      applier.begin(msg[2].commitWatermark);
      continue;
    }
    if (type === 'commit') {
      applier.commit(msg[2].watermark);
      continue;
    }
    const change = msg[1] as {
      tag: string;
      relation: {name: string};
      new?: Record<string, unknown>;
      key?: Record<string, unknown>;
    };
    const table = change.relation.name;
    if (change.tag === 'delete') {
      applier.delete(table, must(change.key).id);
      continue;
    }
    const row = must(change.new);
    cols.length = 0;
    vals.length = 0;
    for (const k in row) {
      const v = row[k];
      cols.push(k);
      vals.push(typeof v === 'boolean' ? (v ? 1 : 0) : (v as unknown));
    }
    if (change.tag === 'insert') {
      applier.insert(table, cols, vals);
    } else {
      applier.update(table, cols, vals, row.id);
    }
  }
  return performance.now() - start;
}

function applyBinary(applier: DirectApplier, buf: Buffer): number {
  const start = performance.now();
  const cols: string[] = [];
  const vals: unknown[] = [];
  let p = 0;
  const end = buf.length;

  const readString = (): string => {
    const len = buf.readUInt32LE(p);
    p += 4;
    const s = buf.toString('utf8', p, p + len);
    p += len;
    return s;
  };
  const readValue = (): unknown => {
    const tag = buf[p++]!;
    switch (tag) {
      case TAG_NULL:
        return null;
      case TAG_TRUE:
        return 1;
      case TAG_FALSE:
        return 0;
      case TAG_DOUBLE: {
        const n = buf.readDoubleLE(p);
        p += 8;
        return n;
      }
      case TAG_STRING:
        return readString();
      default:
        throw new Error(`bad tag ${tag}`);
    }
  };

  while (p < end) {
    const op = buf[p++]!;
    if (op === OP_BEGIN) {
      applier.begin(readString());
      continue;
    }
    if (op === OP_COMMIT) {
      applier.commit(readString());
      continue;
    }
    const tableId = buf[p++]!;
    const table = TABLE_IDS[tableId]!;
    const columns = TABLE_COLUMNS[tableId]!;
    if (op === OP_DELETE) {
      applier.delete(table, readValue());
      continue;
    }
    const n = buf[p++]!;
    cols.length = 0;
    vals.length = 0;
    let id: unknown;
    for (let i = 0; i < n; i++) {
      const col = columns[buf[p++]!]!;
      const value = readValue();
      cols.push(col);
      vals.push(value);
      if (col === 'id') {
        id = value;
      }
    }
    if (op === OP_INSERT) {
      applier.insert(table, cols, vals);
    } else {
      applier.update(table, cols, vals, id);
    }
  }
  return performance.now() - start;
}

// ---------------------------------------------------------------------------
// Replica plumbing
// ---------------------------------------------------------------------------

function pragmas(db: Database) {
  db.pragma('locking_mode = EXCLUSIVE');
  db.pragma('journal_mode = OFF');
  db.pragma('synchronous = OFF');
  db.pragma('busy_timeout = 30000');
}

function newProcessor(db: Database): ChangeProcessor {
  return new ChangeProcessor(new StatementRunner(db), 'backup', (_, err) => {
    throw err;
  });
}

function buildBase(rows: number): {
  path: string;
  /** Live row keys the measured log updates and deletes from. */
  liveIds: ReadonlyMap<Table, readonly string[]>;
  close(): void;
} {
  const file = new DbFile('logical-log-headroom-base');
  const db = file.connect(lc);
  pragmas(db);
  initReplicationState(db, ['zero_headroom_bench'], versionToLexi(0));
  const proc = newProcessor(db);

  proc.processMessage(lc, [
    'begin',
    {tag: 'begin'},
    {commitWatermark: versionToLexi(1)},
  ]);
  for (const spec of SPECS) {
    proc.processMessage(lc, ['data', createTableMessage(spec)]);
  }
  for (const spec of INDEX_SPECS) {
    proc.processMessage(lc, ['data', {tag: 'create-index', spec}]);
  }
  proc.processMessage(lc, [
    'commit',
    {tag: 'commit'},
    {watermark: versionToLexi(1)},
  ]);

  const rand = mulberry32(0x5eed);
  const live = new Map<Table, string[]>([
    ['issue', []],
    ['comment', []],
  ]);
  let seq = 0;
  let watermark = 2;
  while (seq < rows) {
    const stop = Math.min(rows, seq + 20_000);
    const wm = versionToLexi(watermark++);
    proc.processMessage(lc, ['begin', {tag: 'begin'}, {commitWatermark: wm}]);
    for (; seq < stop; seq++) {
      const table: Table = rand() < 0.45 ? 'issue' : 'comment';
      const id = `${table}-b${seq}`;
      must(live.get(table)).push(id);
      proc.processMessage(lc, [
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
    proc.processMessage(lc, ['commit', {tag: 'commit'}, {watermark: wm}]);
  }
  db.close();
  return {path: file.path, liveIds: live, close: () => file.delete()};
}

function openCopy(basePath: string): {db: Database; file: DbFile} {
  const file = new DbFile('logical-log-headroom-run');
  copyFileSync(basePath, file.path);
  const db = file.connect(lc);
  pragmas(db);
  return {db, file};
}

/**
 * A fingerprint of everything the log should have changed. Two appliers that
 * agree here did the same work; one that is fast because it skipped a write
 * does not.
 */
function checksum(db: Database): string {
  const parts: string[] = [];
  for (const spec of SPECS) {
    const [row] = db
      .prepare(
        `SELECT COUNT(*) AS n,
                SUM(LENGTH(COALESCE("id",''))) AS ids,
                SUM(LENGTH(COALESCE("created",0))) AS created,
                COUNT(DISTINCT "${ZERO_VERSION}") AS versions
           FROM "${spec.name}"`,
      )
      .all<Record<string, number>>();
    parts.push(
      `${spec.name}:${row.n}/${row.ids}/${row.created}/${row.versions}`,
    );
  }
  const [state] = db
    .prepare(`SELECT stateVersion FROM "_zero.replicationState"`)
    .all<{stateVersion: string}>();
  parts.push(`v:${state.stateVersion}`);
  return parts.join(' ');
}

// ---------------------------------------------------------------------------
// Bench
// ---------------------------------------------------------------------------

type Variant = 'baseline' | 'json-fast' | 'direct' | 'binary';

type VariantResult = {
  readonly variant: Variant;
  readonly logMB: number;
  readonly mbPerSecond: number[];
  readonly changesPerSecond: number[];
  readonly usPerChange: number[];
};

function median(values: readonly number[]): number {
  const sorted = values.toSorted((a, b) => a - b);
  const mid = Math.floor(sorted.length / 2);
  return sorted.length % 2 === 0
    ? (sorted[mid - 1]! + sorted[mid]!) / 2
    : sorted[mid]!;
}

function fmt(n: number, digits = 1) {
  return n.toFixed(digits);
}

describe('replicator/logical-log applier headroom', () => {
  test('variant comparison', {timeout: TEST_TIMEOUT_MS}, () => {
    const base = buildBase(BASE_ROWS);
    try {
      const log = generateLog(
        WORKLOAD,
        TARGET_MB * BYTES_PER_MB,
        0xc0ffee,
        0,
        Math.ceil(BASE_ROWS / 20_000) + 16,
        // Without this the log only ever updates rows it inserted itself, so
        // the writes stay in a small hot set and SQLite looks cheaper than it
        // is against a real replica.
        base.liveIds,
      );
      const entries = replayEntries(log, COALESCE);
      const binary = encodeBinaryLog(entries);
      // The log's own size is the yardstick everywhere, so all three variants
      // are reported against the same uncompressed JSON byte count.
      const logMB = log.bytes / BYTES_PER_MB;

      const results = new Map<Variant, VariantResult>();
      let expected: string | undefined;

      for (const variant of [
        'baseline',
        'json-fast',
        'direct',
        'binary',
      ] as const) {
        const mbPerSecond: number[] = [];
        const changesPerSecond: number[] = [];
        const usPerChange: number[] = [];

        for (let rep = 0; rep < REPS + 1; rep++) {
          const {db, file} = openCopy(base.path);
          try {
            const ms =
              variant === 'baseline'
                ? applyBaseline(newProcessor(db), entries)
                : variant === 'json-fast'
                  ? applyJsonFast(new FastApplier(db), entries)
                  : variant === 'direct'
                    ? applyDirect(new DirectApplier(db), entries)
                    : applyBinary(new DirectApplier(db), binary);

            const actual = checksum(db);
            if (variant === 'baseline') {
              expected ??= actual;
            }
            // Every variant must land the replica in the same state.
            expect(actual).toBe(must(expected));

            if (rep > 0) {
              // rep 0 is an unrecorded warm-up.
              mbPerSecond.push((logMB * 1000) / ms);
              changesPerSecond.push((log.changes * 1000) / ms);
              usPerChange.push((ms * 1000) / log.changes);
            }
          } finally {
            db.close();
            file.delete();
          }
        }

        results.set(variant, {
          variant,
          logMB,
          mbPerSecond,
          changesPerSecond,
          usPerChange,
        });
        benchmarkRecorder.recordThroughputSamples(
          `${variant} log MB`,
          mbPerSecond.map(v => ({elapsedMs: 1000, operations: v})),
        );
      }

      report(log, entries, binary, results);
    } finally {
      base.close();
    }
  });
});

function report(
  log: GeneratedLog,
  jsonEntries: readonly string[],
  binary: Buffer,
  results: ReadonlyMap<Variant, VariantResult>,
) {
  const baseline = must(results.get('baseline'));
  const baseMB = median(baseline.mbPerSecond);

  console.log('');
  console.log(
    `=== applier headroom (${WORKLOAD}, ${fmt(BASE_ROWS / 1e6, 1)}M row base, ` +
      `${COALESCE} txns/commit, ${REPS} sample(s)) ===`,
  );
  console.log('');
  const header = [
    'variant'.padEnd(11),
    'MB/s'.padStart(8),
    'changes/s'.padStart(11),
    'us/change'.padStart(10),
    'vs baseline'.padStart(12),
  ].join(' ');
  console.log(header);
  console.log('-'.repeat(header.length));
  for (const variant of [
    'baseline',
    'json-fast',
    'direct',
    'binary',
  ] as const) {
    const r = must(results.get(variant));
    const mb = median(r.mbPerSecond);
    console.log(
      [
        variant.padEnd(11),
        fmt(mb).padStart(8),
        Math.round(median(r.changesPerSecond)).toLocaleString().padStart(11),
        fmt(median(r.usPerChange), 1).padStart(10),
        `${fmt(mb / baseMB, 2)}x`.padStart(12),
      ].join(' '),
    );
  }

  console.log('');
  console.log('Log size, same changes:');
  console.log(
    `  JSON (canonical wire form)  ${fmt(log.bytes / BYTES_PER_MB)} MB ` +
      `(${Math.round(log.bytes / log.changes)} bytes/change)`,
  );
  console.log(
    `  binary                      ${fmt(binary.length / BYTES_PER_MB)} MB ` +
      `(${Math.round(binary.length / log.changes)} bytes/change, ` +
      `${fmt(log.bytes / binary.length, 2)}x smaller)`,
  );
  // What actually reaches S3. A compact encoding and a general-purpose
  // compressor are largely after the same redundancy, so the raw size win is
  // not necessarily a storage win -- worth knowing before picking a format for
  // storage reasons.
  const jsonGz = gzipSync(Buffer.from(jsonEntries.join('\n'), 'utf8'), {
    level: 1,
  }).length;
  const binGz = gzipSync(binary, {level: 1}).length;
  console.log('');
  console.log('After gzip -1, which is what would actually be stored:');
  console.log(
    `  JSON    ${fmt(jsonGz / BYTES_PER_MB)} MB ` +
      `(${fmt(log.bytes / jsonGz, 1)}x compression)`,
  );
  console.log(
    `  binary  ${fmt(binGz / BYTES_PER_MB)} MB ` +
      `(${fmt(binary.length / binGz, 1)}x compression, ` +
      `${fmt(jsonGz / binGz, 2)}x smaller than gzipped JSON)`,
  );
  console.log('');
}
