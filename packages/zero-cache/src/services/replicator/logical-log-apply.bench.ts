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
//                comes from bytes rather than an in-process stream -- but the
//                implementation is not: `BigIntJSON` hand-rolls a parser so
//                int8 values past 2^53 survive, and that costs ~3x against the
//                engine's own `JSON.parse` (67 vs 196 MB/s here). The bench
//                reports both.
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
//                Replay does not need that: nothing reads the replica while it
//                is behind, so a whole chunk can share one commit.
//                Measured result: how much this buys depends entirely on the
//                pragma profile it runs under.
//                  Under `rep-manager` it is a big lever (3.4 -> 7.6 MB/s at a
//                  977 MB replica) because every commit rewrites pages into a
//                  WAL that never checkpoints -- fewer commits, less WAL.
//                  Under `excl` most of that is already banked: it saturates
//                  by ~16 transactions per commit (7.2 -> 8.2 MB/s) and is
//                  flat after that.
//                Coalescing the *whole* chunk into one transaction is a
//                trap at the default page cache: the dirty set outgrows the
//                cache, SQLite spills pages mid-transaction and rewrites the
//                ones that are touched again (update-heavy: 7.5 MB/s at 256,
//                5.9 at whole-chunk). It only pays with a cache big enough to
//                hold the dirty set -- at 4 GB the same case is the best
//                result on the board, 8.2 MB/s.
//   pragmas      a ladder of profiles, from what the replication manager runs
//                today to what a replayer could run instead. Every pragma it
//                relaxes exists to protect a reader or a crash, and replay has
//                neither:
//                  rep-manager  WAL, synchronous=NORMAL, wal_autocheckpoint=0
//                  wal-ckpt     WAL, synchronous=OFF, checkpointing on
//                  no-journal   journal_mode=OFF, synchronous=OFF
//                  excl         + locking_mode=EXCLUSIVE
//                  excl+cache   + a large page cache
//                Measured result: nearly the whole win is in not inheriting
//                `wal_autocheckpoint = 0`. `journal_mode = OFF` is not faster
//                than a checkpointing WAL -- it is slightly slower until
//                EXCLUSIVE locking brings it level -- but it removes the WAL
//                file outright, which is worth more than the throughput.
//                `cache_size` does nothing at moderate batch sizes -- one pass
//                over a log touches each page about once, so there is no reuse
//                for a cache to capture, and 64 MB to 4 GB measures flat. It
//                matters only as the counterpart to very large transactions,
//                where it is what keeps the dirty set from spilling.
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
//   LOGICAL_LOG_PROFILES         pragma profiles for the profile sweep
//   LOGICAL_LOG_SIZE_PROFILES    pragma profiles compared at each replica size
//   LOGICAL_LOG_CACHE_MB         page cache for the 'excl+cache' profile
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
//
// A CPU profile of the apply loop alone, at a 977 MB replica, splits roughly:
// 41% inside SQLite, 27% in BigIntJSON.parse, and ~20% in per-change JS on the
// way there -- rebuilding the INSERT/UPDATE text for every row, re-normalizing
// its whitespace in the statement cache, quoting identifiers, and converting
// the row. Only the first of those grows with the replica; the other two are
// fixed per change, so they dominate on a small replica and fade on a large
// one. It is not disk: moving the replica to tmpfs buys ~10%.
//
// Throughput falls as the replica grows, and that is SQLite, not this bench:
// random-key updates against a bare table with no Zero code in the path go
// from ~83k/s at 35 MB to ~10k/s at 1.4 GB, and the same 8x shows up with no
// secondary index at all. It is not page-cache sizing (see above) and it is
// not within-commit page locality (sorting a batch by key buys ~6%); it is
// deeper B-trees and more distinct pages dirtied per commit. The pragma
// profile decides how steep it is: 4.8x from empty to 1 GB under
// `rep-manager`, 2.2x under `excl`.

import {copyFileSync, existsSync, statSync} from 'node:fs';
import {gzipSync} from 'node:zlib';
import {describe, test} from 'vitest';
import {createManualBenchmarkRecorder} from '../../../../shared/src/bench.ts';
import {BigIntJSON} from '../../../../shared/src/bigint-json.ts';
import {createSilentLogContext} from '../../../../shared/src/logging-test-utils.ts';
import {must} from '../../../../shared/src/must.ts';
import * as v from '../../../../shared/src/valita.ts';
import type {Database} from '../../../../zqlite/src/db.ts';
import {StatementRunner} from '../../db/statements.ts';
import {DbFile} from '../../test/lite.ts';
import {versionToLexi} from '../../types/lexi-version.ts';
import {getPragmaConfig} from '../../workers/replicator.ts';
import {
  changeStreamDataSchema,
  type ChangeStreamData,
} from '../change-source/protocol/current/downstream.ts';
import {ChangeProcessor} from './change-processor.ts';
/**
 * Named pragma sets, ordered from "what the replication manager runs today" to
 * "what a replayer could run instead". Each step relaxes something that only
 * exists to protect a reader or a crash -- and replay has neither: nothing
 * reads the replica while it is behind, and a crash mid-replay is repaired by
 * restoring the base and replaying again, not by trusting the file.
 */
type PragmaProfile =
  | 'rep-manager'
  | 'wal-ckpt'
  | 'no-journal'
  | 'excl'
  | 'excl+cache';

import {
  commentRow,
  createTableMessage,
  generateLog,
  INDEX_SPECS,
  issueRow,
  mulberry32,
  RELATIONS,
  replayEntries,
  SPECS,
  type GeneratedLog,
  type Table,
  type Workload,
} from './logical-log-fixture.ts';

import {initReplicationState} from './schema/replication-state.ts';
import {applyPragmas} from './write-worker-client.ts';

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
/** Pragma profiles swept by the profile test, least to most relaxed. */
const PROFILES = envList<PragmaProfile>(
  'LOGICAL_LOG_PROFILES',
  ['rep-manager', 'wal-ckpt', 'no-journal', 'excl', 'excl+cache'],
  ['rep-manager', 'wal-ckpt', 'no-journal', 'excl', 'excl+cache'],
);
/** Profiles compared at each replica size. */
const SIZE_SWEEP_PROFILES = envList<PragmaProfile>(
  'LOGICAL_LOG_SIZE_PROFILES',
  ['rep-manager', 'excl'],
  ['rep-manager', 'wal-ckpt', 'no-journal', 'excl', 'excl+cache'],
);
/** Replica sizes (rows) swept by the size-sensitivity test. */
const BASE_SWEEP = envNumberList(
  'LOGICAL_LOG_BASE_SWEEP',
  [0, 500_000, 2_000_000],
  0,
);
/** Log sizes (MB) to project catchup time for, per the S3 restore question. */
const PROJECTIONS = envNumberList('LOGICAL_LOG_PROJECTIONS', [100, 500, 1024]);

// ---------------------------------------------------------------------------
// Replica setup
// ---------------------------------------------------------------------------

type Replica = {
  readonly db: Database;
  readonly processor: ChangeProcessor;
  readonly file: DbFile;
  close(): void;
};

/** Page cache for the 'excl+cache' profile. Negative means KiB, not pages. */
const REPLAY_CACHE_KIB = envNumber('LOGICAL_LOG_CACHE_MB', 512) * 1024;

function applyReplicaPragmas(db: Database, profile: PragmaProfile) {
  // `locking_mode` has to be set before the connection takes its first lock,
  // and `journal_mode` before there is a journal to migrate.
  if (profile === 'excl' || profile === 'excl+cache') {
    db.pragma('locking_mode = EXCLUSIVE');
  }
  if (profile === 'excl+cache') {
    db.pragma(`cache_size = -${REPLAY_CACHE_KIB}`);
  }

  if (profile === 'rep-manager' || profile === 'wal-ckpt') {
    db.pragma('journal_mode = WAL');
  } else {
    // No journal at all: pages go straight to the db file. A crash leaves a
    // corrupt file rather than a stale one, which is the right trade only
    // because the recovery move is "restore the base and replay again".
    db.pragma('journal_mode = OFF');
  }

  // busy_timeout / analysis_limit / wal_autocheckpoint=0, as the
  // replication-manager sets them.
  applyPragmas(db, getPragmaConfig('backup'));

  if (profile === 'rep-manager') {
    db.pragma('synchronous = NORMAL');
  } else {
    db.pragma('synchronous = OFF');
  }
  if (profile === 'wal-ckpt') {
    // litestream owns checkpointing in the replication manager, so its
    // `wal_autocheckpoint = 0` has to be undone deliberately.
    db.pragma('wal_autocheckpoint = 1000');
  }
}

function newProcessor(db: Database): ChangeProcessor {
  return new ChangeProcessor(new StatementRunner(db), 'backup', (_, err) => {
    throw err;
  });
}

/** Creates a fresh replica with the bench schema, via the real DDL path. */
function createReplica(profile: PragmaProfile, testName: string): Replica {
  const file = new DbFile(testName);
  const db = file.connect(lc);
  applyReplicaPragmas(db, profile);
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
  profile: PragmaProfile,
  testName: string,
): Replica {
  const file = new DbFile(testName);
  copyFileSync(templatePath, file.path);
  const db = file.connect(lc);
  applyReplicaPragmas(db, profile);
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
  const replica = createReplica('excl+cache', 'logical-log-apply-base');
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

/**
 * Native `JSON.parse` over the same entries. `BigIntJSON` exists to carry
 * int8 values past 2^53 without losing precision, and it pays for that with a
 * hand-written parser; this measures what that costs against the engine's own.
 * A replay path that knows its log has no oversized integers -- or that can
 * fall back on detecting one -- gets the difference back.
 */
function nativeParseOnly(entries: readonly string[]): number {
  const start = performance.now();
  let sink = 0;
  for (const json of entries) {
    sink += (JSON.parse(json) as unknown[]).length;
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
  readonly profile: PragmaProfile;
  readonly baseRows: number;
  readonly baseMB: number;
  readonly logMB: number;
  readonly changes: number;
  readonly transactions: number;
  readonly commits: number;
  readonly applyMBPerSecond: readonly number[];
  readonly parseMBPerSecond: readonly number[];
  readonly validateMBPerSecond: readonly number[];
  readonly nativeParseMBPerSecond: readonly number[];
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
    'commits'.padStart(8),
    'pragmas'.padStart(12),
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
        String(Math.min(r.coalesce, r.transactions)).padStart(8),
        String(r.commits).padStart(8),
        r.profile.padStart(12),
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
    'pragmas'.padStart(12),
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
        r.profile.padStart(12),
        fmt(r.baseMB, 0).padStart(9),
        ...PROJECTIONS.map(mb => duration(mb / mbps).padStart(10)),
      ].join(' '),
    );
  }
  console.log('');
  console.log('Deserialization cost, over the same entries:');
  for (const r of results) {
    const parse = median(r.parseMBPerSecond);
    const validate = median(r.validateMBPerSecond);
    const native = median(r.nativeParseMBPerSecond);
    console.log(
      `  ${r.name}:\n` +
        `      native JSON.parse   ${fmt(native)} MB/s\n` +
        `      BigIntJSON.parse    ${fmt(parse)} MB/s ` +
        `(${fmt(native / parse, 2)}x slower)\n` +
        `      + valita validation ${fmt(validate)} MB/s ` +
        `(as paid by the live subscription path)`,
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
  profile: PragmaProfile,
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
    profile,
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
  profile: PragmaProfile,
): CaseResult {
  const name = `${workload} coalesce=${coalesce} ${profile}`;
  const log = must(fixture.logs.get(workload));
  const entries = replayEntries(log, coalesce);
  const logMB = log.bytes / BYTES_PER_MB;

  // One unrecorded pass so the first recorded sample is not paying for JIT
  // warm-up of the parse and apply paths.
  warmUp(fixture, profile, entries);

  const applyMBPerSecond: number[] = [];
  const changesPerSecond: number[] = [];
  const parseMBPerSecond: number[] = [];
  const validateMBPerSecond: number[] = [];
  const nativeParseMBPerSecond: number[] = [];
  const commitCount = Math.ceil(log.txs.length / coalesce);
  let replicaMB = 0;
  let walMB = 0;

  for (let rep = 0; rep < REPS; rep++) {
    // Parse and validate do not touch the replica, so they are timed outside
    // the restore.
    parseMBPerSecond.push((logMB * 1000) / parseOnly(entries));
    validateMBPerSecond.push((logMB * 1000) / parseAndValidate(entries));
    nativeParseMBPerSecond.push((logMB * 1000) / nativeParseOnly(entries));

    const replica = openRestoredReplica(
      fixture.basePath,
      profile,
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
    profile,
    baseRows: fixture.baseRows,
    baseMB: fixture.baseMB,
    logMB,
    changes: log.changes,
    transactions: log.txs.length,
    commits: commitCount,
    applyMBPerSecond,
    parseMBPerSecond,
    validateMBPerSecond,
    nativeParseMBPerSecond,
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
          for (const profile of SIZE_SWEEP_PROFILES) {
            rows.push(runCase(fixture, workload, coalesce, profile));
          }
        }
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

  // Each pragma the replication manager sets exists to protect a reader or a
  // crash. Replay has neither, so this walks them off one at a time against a
  // replica big enough for the difference to show.
  test('pragma profile sweep', {timeout: TEST_TIMEOUT_MS}, () => {
    const workload: Workload = must(WORKLOADS.at(-1));
    const coalesce = must(COALESCE.at(-1));
    const rows: CaseResult[] = [];
    const fixture = buildFixture(BASE_ROWS, [workload]);
    try {
      for (const profile of PROFILES) {
        rows.push(runCase(fixture, workload, coalesce, profile));
      }
    } finally {
      fixture.close();
    }
    printResults('pragma profiles', rows);
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
        for (const profile of SIZE_SWEEP_PROFILES) {
          rows.push(runCase(fixture, workload, coalesce, profile));
        }
      } finally {
        fixture.close();
      }
    }
    printResults('apply throughput vs replica size', rows);
  });
});
