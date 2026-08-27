/* oxlint-disable no-console */

// Measures how many bytes the same workload costs on the two backup paths:
//
//   logical  the change stream that the SQLite change log stores, i.e. the
//            serialized downstream messages (`change-log-codec.ts`). Grows
//            linearly in writes; an update carries the whole new row.
//   physical the LTX files litestream v5 uploads, i.e. the distinct SQLite
//            pages dirtied per compaction window. Grows in *dirty pages*, so
//            repeated writes to one page collapse as the ladder compacts.
//
// The comparison only means something at a stated window length, because the
// two curves have different shapes: the logical stream is a straight line in
// writes, while LTX bends down toward `page_size x working-set-pages` as L0
// files are compacted into L1/L2/L3. This bench therefore reports both sides at
// every level of the production ladder (`services/litestream/config-v5.yml`)
// and lets the ratio be read off as a curve rather than a single number.
//
// ### How the physical side is measured
//
// Without running litestream at all: the replica is opened exactly as the
// production backup replica is (WAL, `wal_autocheckpoint = 0`, see
// `getPragmaConfig('backup')`), so a simulated monitor-interval's dirty pages
// are precisely the frames its WAL accumulated. Each interval parses the WAL
// directly for the page numbers *and* their post-image, then checkpoints
// (TRUNCATE), which is what litestream's own monitor loop does. An L0 file is
// the distinct pages of one interval; an Ln file is the union of the pages of
// the intervals it covers, read back from the replica at the boundary — which
// is the page content a compaction would carry.
//
// This models the LTX *payload*, which is where essentially all of the bytes
// are; framing is approximated by the LTX_* constants below and is under 1% at
// 4KiB pages. What calibrates the model -- and what should be run before any
// decision rests on it -- is the same workload against a real litestream: point
// `ZERO_LITESTREAM_BACKUP_URL` at a file:// (or minio) destination, disable
// snapshots for the run, bracket it between two watermarks confirmed by the
// BackupMonitor (`change-streamer/backup-monitor.ts`), and `stat` the objects
// the destination gained, bucketed by level.
//
// ### What is deliberately excluded
//
//   * Snapshots. A snapshot is a full copy of the replica and would dominate
//     any window it lands in; the final replica size is reported separately so
//     it can be amortized over `snapshotBackupIntervalHours` (default 4)
//     explicitly rather than smeared across the incremental numbers.
//   * Wall-clock. Time is simulated: the workload's writes are spread over
//     LOGICAL_VS_LTX_WINDOW_HOURS of simulated time and applied as fast as
//     SQLite will take them. Nothing here measures throughput -- that is
//     `sqlite-change-log-ceiling.bench.ts`.
//
// Run:
//   pnpm --filter zero-cache run bench logical-vs-ltx-size
//
// Quick pass (minutes, small tables), for iterating on the bench itself:
//   LOGICAL_VS_LTX_QUICK=1 pnpm --filter zero-cache run bench logical-vs-ltx-size
//
// Knobs:
//   LOGICAL_VS_LTX_QUICK              1 for the small/fast matrix
//   LOGICAL_VS_LTX_WORKLOADS          comma-separated subset of the names below
//   LOGICAL_VS_LTX_WRITES             row changes per workload
//   LOGICAL_VS_LTX_WINDOW_HOURS       simulated span the writes are spread over
//   LOGICAL_VS_LTX_TABLE_ROWS         seeded rows in the replica table
//   LOGICAL_VS_LTX_ROW_BYTES          approximate serialized row size
//   LOGICAL_VS_LTX_TX_ROWS            row changes per upstream transaction
//   LOGICAL_VS_LTX_INDEXES            secondary indexes on the table (0-2)
//   LOGICAL_VS_LTX_INDEX_CHURN        1 to also move the indexed column
//   LOGICAL_VS_LTX_PAGE_SIZE          replica page_size (default: SQLite's)
//   LOGICAL_VS_LTX_COMPRESSION        zstd | gzip | none
//   LOGICAL_VS_LTX_MONITOR_SECONDS    L0 interval (production default 15)

import {closeSync, openSync, readSync, statSync} from 'node:fs';
import {arch, cpus, platform, release, totalmem} from 'node:os';
import zlib from 'node:zlib';
import {afterAll, describe, expect, test} from 'vitest';
import {createSilentLogContext} from '../../../../shared/src/logging-test-utils.ts';
import {Database} from '../../../../zqlite/src/db.ts';
import {StatementRunner} from '../../db/statements.ts';
import {DbFile} from '../../test/lite.ts';
import {versionToLexi} from '../../types/lexi-version.ts';
import {getPragmaConfig} from '../../workers/replicator.ts';
import type {DataChange} from '../change-source/protocol/current/data.ts';
import type {ChangeStreamData} from '../change-source/protocol/current/downstream.ts';
import {serializeChangeStreamData} from '../change-streamer/change-log-codec.ts';
import {CREATE_CHANGE_LOG_COOKIE_SCHEMA} from './change-log-cookies.ts';
import {
  applyChangeLogPragmas,
  changeLogFileName,
  CREATE_CHANGE_LOG_STREAM_SCHEMA,
} from './change-log-db.ts';
import {ChangeLogStreamWriter} from './change-log-stream-writer.ts';
import {ZERO_VERSION_COLUMN_NAME} from './schema/constants.ts';
import {readWalCapture, type WalCapture} from './wal-dirty-pages.ts';
import {applyPragmas} from './write-worker-client.ts';

/**
 * LTX framing, approximated. A v5 LTX file carries a fixed header, a small
 * per-page header, and a trailer; at a 4KiB page size the whole of it is well
 * under 1% of the file, so the model is not sensitive to these being exact.
 * The live (Tier 2) run against a real destination is what calibrates them.
 */
const LTX_HEADER_BYTES = 100;
const LTX_PAGE_HEADER_BYTES = 24;
const LTX_TRAILER_BYTES = 32;

/**
 * Compression is applied in independent chunks rather than as one stream, which
 * is both what a multipart uploader does and what keeps this bench's memory
 * flat on a multi-GiB physical volume. It costs a little ratio at the chunk
 * boundaries, and it costs it identically on both sides of the comparison.
 */
const COMPRESSION_CHUNK_BYTES = 8 * 1024 * 1024;

function booleanFromEnv(name: string, fallback: boolean): boolean {
  const raw = process.env[name];
  if (raw === undefined || raw === '') {
    return fallback;
  }
  switch (raw.toLowerCase()) {
    case '1':
    case 'true':
      return true;
    case '0':
    case 'false':
      return false;
    default:
      throw new Error(`${name} must be true, false, 1, or 0; got ${raw}`);
  }
}

function integerFromEnv(name: string, fallback: number): number {
  const raw = process.env[name];
  if (!raw) {
    return fallback;
  }
  const value = Number(raw);
  if (!Number.isSafeInteger(value) || value <= 0) {
    throw new Error(`${name} must be a positive safe integer; got ${raw}`);
  }
  return value;
}

function nonNegativeIntegerFromEnv(name: string, fallback: number): number {
  const raw = process.env[name];
  if (!raw) {
    return fallback;
  }
  const value = Number(raw);
  if (!Number.isSafeInteger(value) || value < 0) {
    throw new Error(`${name} must be a non-negative safe integer; got ${raw}`);
  }
  return value;
}

function numberFromEnv(name: string, fallback: number): number {
  const raw = process.env[name];
  if (!raw) {
    return fallback;
  }
  const value = Number(raw);
  if (!Number.isFinite(value) || value <= 0) {
    throw new Error(`${name} must be a positive number; got ${raw}`);
  }
  return value;
}

const QUICK = booleanFromEnv('LOGICAL_VS_LTX_QUICK', false);

const WRITES = integerFromEnv('LOGICAL_VS_LTX_WRITES', QUICK ? 5_000 : 60_000);
const WINDOW_HOURS = numberFromEnv(
  'LOGICAL_VS_LTX_WINDOW_HOURS',
  QUICK ? 1 : 3,
);
const TABLE_ROWS = integerFromEnv(
  'LOGICAL_VS_LTX_TABLE_ROWS',
  QUICK ? 50_000 : 400_000,
);
const ROW_BYTES = integerFromEnv('LOGICAL_VS_LTX_ROW_BYTES', 250);
const TX_ROWS = integerFromEnv('LOGICAL_VS_LTX_TX_ROWS', 5);
const SECONDARY_INDEXES = nonNegativeIntegerFromEnv(
  'LOGICAL_VS_LTX_INDEXES',
  2,
);
const PAGE_SIZE_OVERRIDE = nonNegativeIntegerFromEnv(
  'LOGICAL_VS_LTX_PAGE_SIZE',
  0,
);
const MONITOR_SECONDS = integerFromEnv('LOGICAL_VS_LTX_MONITOR_SECONDS', 15);
/**
 * Whether a write also moves the row's entry in the `ownerId` index.
 *
 * Off by default, because the columns an application rewrites on every update
 * are usually not the ones it indexes for lookup (an issue's `modified` moves,
 * its owner does not), and leaving it on would let incidental index scatter
 * dominate the locality axis each workload is built to isolate. Turn it on to
 * measure what a churning secondary index costs the physical side: every write
 * lands on a different index leaf, so it is close to an upper bound.
 */
const INDEX_CHURN = booleanFromEnv('LOGICAL_VS_LTX_INDEX_CHURN', false);
const WINDOW_SECONDS = Math.round(WINDOW_HOURS * 3600);
const SIMULATED_EPOCH_MS = 1_700_000_000_000;
const WATERMARK_BASE = 8_000_000_000;

/**
 * The compaction ladder from `services/litestream/config-v5.yml`, plus the
 * whole measurement window (what one fully-compacted file covering the run
 * would hold, and the fairest single number to compare against a logical log
 * shipped once per window).
 */
type Level = {
  readonly name: string;
  readonly seconds: number;
  readonly note: string;
};

const LADDER: readonly Level[] = [
  {name: 'L0', seconds: MONITOR_SECONDS, note: 'uploaded, one PUT each'},
  {name: 'L1', seconds: 120, note: 'compaction, config-v5.yml'},
  {name: 'L2', seconds: 600, note: 'compaction, config-v5.yml'},
  {name: 'L3', seconds: 3600, note: 'compaction, config-v5.yml'},
  {name: 'window', seconds: WINDOW_SECONDS, note: 'whole run, fully compacted'},
];

type Locality = 'scattered' | 'recent' | 'append' | 'oldest-first';
type Op = 'insert' | 'update' | 'delete';

type Workload = {
  readonly name: string;
  readonly op: Op;
  readonly locality: Locality;
  /**
   * Distinct rows the workload touches. `writes / distinctRows` is the repeat
   * factor r, the quantity the whole comparison turns on: LTX dedups repeats
   * within a compaction window and the logical log never does.
   */
  readonly distinctRows: number;
  readonly tableRows: number;
  readonly description: string;
};

function workloads(): Workload[] {
  const all: Workload[] = [
    {
      name: 'hot-spot-update',
      op: 'update',
      locality: 'recent',
      distinctRows: Math.max(1, Math.round(WRITES / 100)),
      tableRows: TABLE_ROWS,
      description:
        'r=100 over a contiguous hot block: heartbeat/counter/viewState shape',
    },
    {
      name: 'recent-window-update',
      op: 'update',
      locality: 'recent',
      distinctRows: Math.max(1, Math.round(WRITES / 5)),
      tableRows: TABLE_ROWS,
      description: 'r=5 over a recent contiguous window: active-issue shape',
    },
    {
      name: 'scattered-update',
      op: 'update',
      locality: 'scattered',
      distinctRows: WRITES,
      tableRows: TABLE_ROWS,
      description:
        'r=1, spread across the table: the worst case for page backups. It ' +
        'reaches one dirty page per write only when the table is large ' +
        'relative to the run (LOGICAL_VS_LTX_TABLE_ROWS >= writes x ' +
        'rows/page); the reported dirty-pages/write says how close it got',
    },
    {
      name: 'append-insert',
      op: 'insert',
      locality: 'append',
      distinctRows: WRITES,
      tableRows: Math.min(TABLE_ROWS, 10_000),
      description: 'r=1, perfect locality: append-only ingest',
    },
    {
      name: 'delete-oldest',
      op: 'delete',
      locality: 'oldest-first',
      distinctRows: WRITES,
      tableRows: Math.max(TABLE_ROWS, WRITES * 2),
      description: 'r=1, clustered: retention/GC sweep of the oldest rows',
    },
  ];
  const selected = process.env['LOGICAL_VS_LTX_WORKLOADS'];
  if (!selected) {
    return all;
  }
  const names = selected
    .split(',')
    .map(s => s.trim())
    .filter(s => s.length > 0);
  return names.map(name => {
    const found = all.find(w => w.name === name);
    if (found === undefined) {
      throw new Error(
        `unknown workload ${name}; expected one of ${all
          .map(w => w.name)
          .join(', ')}`,
      );
    }
    return found;
  });
}

// ---------------------------------------------------------------------------
// compression
// ---------------------------------------------------------------------------

type CompressionName = 'zstd' | 'gzip' | 'none';

/**
 * `zstdCompressSync` exists in Node 22.15+ but is not in every `@types/node`
 * in the 22.x range, so it is reached through a widened type rather than an
 * `any` cast or a version assumption.
 */
type MaybeZstd = typeof zlib & {
  zstdCompressSync?: ((buf: Buffer) => Buffer) | undefined;
};

function resolveCompression(): {
  readonly name: CompressionName;
  readonly compress: ((buf: Buffer) => Buffer) | undefined;
} {
  const requested = (
    process.env['LOGICAL_VS_LTX_COMPRESSION'] ?? 'zstd'
  ).toLowerCase();
  const zstd = (zlib as MaybeZstd).zstdCompressSync;
  switch (requested) {
    case 'none':
      return {name: 'none', compress: undefined};
    case 'gzip':
      return {name: 'gzip', compress: buf => zlib.gzipSync(buf)};
    case 'zstd':
      if (zstd) {
        return {name: 'zstd', compress: buf => zstd(buf)};
      }
      // Falling back rather than failing: the ratio story is the same shape
      // with gzip, and the run is still apples-to-apples across both sides.
      console.warn(
        'zstdCompressSync unavailable in this Node build; using gzip',
      );
      return {name: 'gzip', compress: buf => zlib.gzipSync(buf)};
    default:
      throw new Error(
        `LOGICAL_VS_LTX_COMPRESSION must be zstd, gzip, or none; got ${requested}`,
      );
  }
}

const COMPRESSION = resolveCompression();

/** Accumulates bytes and reports their compressed size, chunk by chunk. */
class ChunkedCompressor {
  #pending: Buffer[] = [];
  #pendingBytes = 0;
  #rawBytes = 0;
  #compressedBytes = 0;

  push(buf: Buffer): void {
    this.#rawBytes += buf.length;
    if (COMPRESSION.compress === undefined) {
      return;
    }
    this.#pending.push(buf);
    this.#pendingBytes += buf.length;
    if (this.#pendingBytes >= COMPRESSION_CHUNK_BYTES) {
      this.#flush();
    }
  }

  #flush(): void {
    if (this.#pendingBytes === 0 || COMPRESSION.compress === undefined) {
      return;
    }
    this.#compressedBytes += COMPRESSION.compress(
      Buffer.concat(this.#pending),
    ).length;
    this.#pending = [];
    this.#pendingBytes = 0;
  }

  finish(): {rawBytes: number; compressedBytes: number | undefined} {
    this.#flush();
    return {
      rawBytes: this.#rawBytes,
      compressedBytes:
        COMPRESSION.compress === undefined ? undefined : this.#compressedBytes,
    };
  }
}

// ---------------------------------------------------------------------------
// measurement
// ---------------------------------------------------------------------------

type FileMeasurement = {
  readonly pages: number;
  readonly ltxRawBytes: number;
  readonly ltxCompressedBytes: number | undefined;
  readonly logicalRawBytes: number;
  readonly logicalCompressedBytes: number | undefined;
};

type LevelResult = {
  readonly level: string;
  readonly seconds: number;
  readonly note: string;
  readonly files: number;
  readonly pages: number;
  readonly ltxRawBytes: number;
  readonly ltxCompressedBytes: number | undefined;
  readonly logicalRawBytes: number;
  readonly logicalCompressedBytes: number | undefined;
  /** LTX / logical, raw. Above 1 means the page backup ships more bytes. */
  readonly rawRatio: number;
  readonly compressedRatio: number | undefined;
};

/** Ascending page numbers, which is the order an LTX file stores them in. */
function sortedPages(pages: Iterable<number>): number[] {
  const sorted: number[] = [];
  for (const pgno of pages) {
    sorted.push(pgno);
  }
  sorted.sort((a, b) => a - b);
  return sorted;
}

function ltxBytes(pages: number, pageSize: number): number {
  return (
    LTX_HEADER_BYTES +
    pages * (pageSize + LTX_PAGE_HEADER_BYTES) +
    LTX_TRAILER_BYTES
  );
}

/**
 * One level of the ladder. Intervals are handed to every level; a level whose
 * span has elapsed closes a file, measures it, and starts the next.
 */
class LevelAccumulator {
  readonly level: Level;
  readonly #intervalsPerFile: number;
  readonly #files: FileMeasurement[] = [];
  #startInterval = 0;

  constructor(level: Level) {
    this.level = level;
    this.#intervalsPerFile = Math.max(
      1,
      Math.round(level.seconds / MONITOR_SECONDS),
    );
  }

  /**
   * Closes and measures every file this level can now complete.
   * `endExclusive` is the number of intervals recorded so far; `force` closes
   * the partial file at the end of the run, which is what a level whose span
   * outlasts the run produces.
   */
  maybeClose(run: RunState, endExclusive: number, force: boolean): void {
    while (
      endExclusive - this.#startInterval >= this.#intervalsPerFile ||
      (force && endExclusive > this.#startInterval)
    ) {
      const end = Math.min(
        endExclusive,
        this.#startInterval + this.#intervalsPerFile,
      );
      this.#files.push(run.measure(this.#startInterval, end));
      this.#startInterval = end;
      if (force && this.#startInterval >= endExclusive) {
        return;
      }
    }
  }

  result(): LevelResult {
    const sum = (pick: (f: FileMeasurement) => number) =>
      this.#files.reduce((total, f) => total + pick(f), 0);
    const ltxRawBytes = sum(f => f.ltxRawBytes);
    const logicalRawBytes = sum(f => f.logicalRawBytes);
    const compressible = this.#files.every(
      f => f.ltxCompressedBytes !== undefined,
    );
    const ltxCompressedBytes = compressible
      ? sum(f => f.ltxCompressedBytes ?? 0)
      : undefined;
    const logicalCompressedBytes = compressible
      ? sum(f => f.logicalCompressedBytes ?? 0)
      : undefined;
    return {
      level: this.level.name,
      seconds: this.level.seconds,
      note: this.level.note,
      files: this.#files.length,
      pages: sum(f => f.pages),
      ltxRawBytes,
      ltxCompressedBytes,
      logicalRawBytes,
      logicalCompressedBytes,
      rawRatio: logicalRawBytes === 0 ? 0 : ltxRawBytes / logicalRawBytes,
      compressedRatio:
        ltxCompressedBytes === undefined ||
        logicalCompressedBytes === undefined ||
        logicalCompressedBytes === 0
          ? undefined
          : ltxCompressedBytes / logicalCompressedBytes,
    };
  }
}

/**
 * The per-interval record the levels are computed from: which pages were
 * dirtied, and the logical stream that dirtied them. Page *images* are not
 * retained — an L0 file measures them the moment its WAL is parsed, and a
 * compacted file re-reads them from the replica at its boundary, which is the
 * content that compaction would carry.
 */
type IntervalRecord = {
  readonly pages: Uint32Array;
  readonly logical: Buffer;
  /** Frames the WAL held, i.e. page writes before intra-interval dedup. */
  readonly walFrames: number;
  readonly l0: FileMeasurement;
};

class RunState {
  readonly replicaPath: string;
  readonly pageSize: number;
  readonly intervals: IntervalRecord[] = [];

  constructor(replicaPath: string, pageSize: number) {
    this.replicaPath = replicaPath;
    this.pageSize = pageSize;
  }

  /** Measures the file a level would produce for `[start, end)`. */
  measure(start: number, end: number): FileMeasurement {
    if (end - start === 1) {
      // The L0 case, already measured against the WAL's own post-images.
      return this.intervals[start].l0;
    }
    const pages = new Set<number>();
    const logical = new ChunkedCompressor();
    for (let i = start; i < end; i++) {
      const record = this.intervals[i];
      for (const pgno of record.pages) {
        pages.add(pgno);
      }
      logical.push(record.logical);
    }
    const sorted = sortedPages(pages);
    const ltx = new ChunkedCompressor();
    const fd = openSync(this.replicaPath, 'r');
    try {
      const {size} = statSync(this.replicaPath);
      const page = Buffer.allocUnsafe(this.pageSize);
      for (const pgno of sorted) {
        const position = (pgno - 1) * this.pageSize;
        if (position + this.pageSize > size) {
          // The page was freed and the file truncated past it (VACUUM or a
          // shrinking delete workload). It still cost a page in the file that
          // covered it, so it is counted raw and skipped for compression.
          ltx.push(Buffer.alloc(0));
          continue;
        }
        readSync(fd, page, 0, this.pageSize, position);
        ltx.push(Buffer.from(page));
      }
    } finally {
      closeSync(fd);
    }
    const ltxTotals = ltx.finish();
    const logicalTotals = logical.finish();
    return {
      pages: sorted.length,
      ltxRawBytes: ltxBytes(sorted.length, this.pageSize),
      ltxCompressedBytes:
        ltxTotals.compressedBytes === undefined
          ? undefined
          : ltxTotals.compressedBytes + LTX_HEADER_BYTES + LTX_TRAILER_BYTES,
      logicalRawBytes: logicalTotals.rawBytes,
      logicalCompressedBytes: logicalTotals.compressedBytes,
    };
  }
}

// ---------------------------------------------------------------------------
// workload generation
// ---------------------------------------------------------------------------

/** Deterministic PRNG, so a run is reproducible and comparable across builds. */
function mulberry32(seed: number): () => number {
  let a = seed >>> 0;
  return () => {
    a = (a + 0x6d2b79f5) >>> 0;
    let t = a;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

/** Rebuilt per message: the protocol's relation type is mutable. */
function relation() {
  return {
    schema: 'public',
    name: 'issue',
    rowKey: {columns: ['id'], type: 'default' as const},
  };
}

type IssueRow = {
  readonly id: number;
  readonly ownerId: number;
  readonly modified: number;
  readonly title: string;
  readonly body: string;
};

const BODY_ALPHABET =
  'the quick brown fox jumps over the lazy dog 0123456789 ' +
  'lorem ipsum dolor sit amet consectetur adipiscing elit ';

/**
 * Row bodies are drawn from a small vocabulary rather than random bytes: real
 * rows are compressible text, and using incompressible noise would flatter the
 * page backup (whose slack compresses away) and punish the logical stream.
 */
function makeBody(rand: () => number, bytes: number): string {
  let out = '';
  while (out.length < bytes) {
    const start = Math.floor(rand() * (BODY_ALPHABET.length - 16));
    out += BODY_ALPHABET.slice(start, start + 16);
  }
  return out.slice(0, bytes);
}

function makeRow(rand: () => number, id: number, tick: number): IssueRow {
  const overhead = 120; // id/ownerId/modified/title/keys, roughly
  const bodyBytes = Math.max(8, ROW_BYTES - overhead);
  return {
    id,
    // Stable in the row's identity unless index churn is being measured; see
    // INDEX_CHURN.
    ownerId: INDEX_CHURN ? 1 + Math.floor(rand() * 5000) : 1 + (id % 5000),
    modified: SIMULATED_EPOCH_MS + tick,
    title: `issue ${id}`,
    body: makeBody(rand, bodyBytes),
  };
}

/**
 * The ids the workload touches, in application order.
 *
 * Order matters as much as the set does: a workload that walked its ids in
 * sequence would pack each interval's writes onto a handful of pages, which is
 * a locality no random-access workload has. Ids are therefore drawn in a
 * shuffled order within each pass over the working set, while the *set* stays
 * exactly `distinctRows` so that r = writes / distinctRows holds by
 * construction.
 */
function idSequence(w: Workload, rand: () => number): Int32Array {
  const ids = new Int32Array(WRITES);
  const base = (k: number): number => {
    switch (w.locality) {
      case 'scattered':
        // Spread across the whole table so that consecutive touches land on
        // different pages.
        return 1 + Math.floor((k * w.tableRows) / w.distinctRows);
      case 'recent':
        // A contiguous block at the end of the table: the rows an app is
        // actively working on.
        return Math.max(1, w.tableRows - w.distinctRows + 1 + k);
      case 'append':
        return w.tableRows + 1 + k;
      case 'oldest-first':
        return 1 + k;
    }
  };
  let cursor = 0;
  while (cursor < WRITES) {
    const pass = Math.min(w.distinctRows, WRITES - cursor);
    const order = new Int32Array(pass);
    for (let k = 0; k < pass; k++) {
      order[k] = k;
    }
    if (w.locality !== 'append' && w.locality !== 'oldest-first') {
      // Fisher-Yates. Append and delete workloads keep their natural order:
      // shuffling them would destroy the very locality that defines them.
      for (let k = pass - 1; k > 0; k--) {
        const j = Math.floor(rand() * (k + 1));
        const tmp = order[k];
        order[k] = order[j];
        order[j] = tmp;
      }
    }
    for (let k = 0; k < pass; k++) {
      ids[cursor + k] = base(order[k]);
    }
    cursor += pass;
  }
  return ids;
}

function changeFor(w: Workload, row: IssueRow, version: string): DataChange {
  switch (w.op) {
    case 'insert':
      return {
        tag: 'insert',
        relation: relation(),
        new: {...row, [ZERO_VERSION_COLUMN_NAME]: version},
      };
    case 'update':
      return {
        tag: 'update',
        relation: relation(),
        key: null,
        new: {...row, [ZERO_VERSION_COLUMN_NAME]: version},
      };
    case 'delete':
      return {tag: 'delete', relation: relation(), key: {id: row.id}};
  }
}

// ---------------------------------------------------------------------------
// the replica and the change log
// ---------------------------------------------------------------------------

const lc = createSilentLogContext();

function replicaSchemaSQL(): string {
  const indexes = [
    `CREATE INDEX "issue_ownerId_idx" ON "issue"("ownerId");`,
    `CREATE INDEX "issue_modified_idx" ON "issue"("modified");`,
  ].slice(0, SECONDARY_INDEXES);
  return /*sql*/ `
    CREATE TABLE "issue" (
      "id" INTEGER PRIMARY KEY,
      "ownerId" INTEGER NOT NULL,
      "modified" INTEGER NOT NULL,
      "title" TEXT NOT NULL,
      "body" TEXT NOT NULL,
      "${ZERO_VERSION_COLUMN_NAME}" TEXT NOT NULL
    );
    ${indexes.join('\n')}

    CREATE TABLE "_zero.replicationState" (
      "stateVersion" TEXT NOT NULL,
      "writeTimeMs" INTEGER,
      "lock" INTEGER PRIMARY KEY DEFAULT 1 CHECK ("lock" = 1)
    );
    INSERT INTO "_zero.replicationState" ("stateVersion", "writeTimeMs")
      VALUES ('00', ${SIMULATED_EPOCH_MS});
  `;
}

type Harness = {
  readonly replica: Database;
  readonly replicaPath: string;
  readonly log: Database;
  readonly logPath: string;
  readonly writer: ChangeLogStreamWriter;
  readonly pageSize: number;
  readonly close: () => void;
};

function setUp(w: Workload, rand: () => number): Harness {
  const dbFile = new DbFile(`logical-vs-ltx-${w.name}`);
  const replica = new Database(lc, dbFile.path);
  if (PAGE_SIZE_OVERRIDE > 0) {
    // Must precede journal_mode; SQLite only honours it while the file is empty.
    replica.pragma(`page_size = ${PAGE_SIZE_OVERRIDE}`);
  }
  replica.pragma('journal_mode = wal');
  replica.pragma('synchronous = NORMAL');
  // 'backup' is the mode the replication-manager's replica runs in, and its
  // `wal_autocheckpoint = 0` is what makes a monitor interval's WAL exactly the
  // set of pages litestream would put in that interval's L0 file.
  applyPragmas(replica, getPragmaConfig('backup'));
  replica.exec(replicaSchemaSQL());

  const logPath = changeLogFileName(dbFile.path);
  const log = new Database(lc, logPath);
  applyChangeLogPragmas(log);
  log.exec(CREATE_CHANGE_LOG_STREAM_SCHEMA);
  log.exec(CREATE_CHANGE_LOG_COOKIE_SCHEMA);
  const writer = new ChangeLogStreamWriter(new StatementRunner(log));

  seed(replica, w, rand);

  const [{page_size: pageSize}] = replica.pragma<{page_size: number}>(
    'page_size',
  );
  return {
    replica,
    replicaPath: dbFile.path,
    log,
    logPath,
    writer,
    pageSize,
    close: () => {
      replica.close();
      log.close();
      dbFile.delete();
    },
  };
}

/**
 * Seeds the table the workload runs against. Seeding is not measured: it is
 * checkpointed away before the first interval so that the run's dirty pages are
 * the workload's, not the load's.
 */
function seed(replica: Database, w: Workload, rand: () => number): void {
  if (w.tableRows === 0) {
    return;
  }
  const insert = replica.prepare(/*sql*/ `
    INSERT INTO "issue" ("id", "ownerId", "modified", "title", "body",
      "${ZERO_VERSION_COLUMN_NAME}")
      VALUES (?, ?, ?, ?, ?, ?)
  `);
  const version = versionToLexi(WATERMARK_BASE);
  for (let from = 1; from <= w.tableRows; from += 10_000) {
    const to = Math.min(from + 10_000, w.tableRows + 1);
    replica.transaction(() => {
      for (let id = from; id < to; id++) {
        const row = makeRow(rand, id, 0);
        insert.run(
          row.id,
          row.ownerId,
          row.modified,
          row.title,
          row.body,
          version,
        );
      }
    });
  }
  replica.pragma('wal_checkpoint(TRUNCATE)');
}

type WorkloadResult = {
  readonly workload: string;
  readonly description: string;
  readonly op: Op;
  readonly locality: Locality;
  readonly writes: number;
  readonly transactions: number;
  readonly distinctRows: number;
  readonly repeatFactor: number;
  readonly tableRows: number;
  readonly pageSize: number;
  readonly rowsPerPage: number;
  readonly intervals: number;
  readonly walFrames: number;
  readonly dirtyPagesPerWriteL0: number;
  readonly dirtyPagesPerWriteWindow: number;
  readonly changeLogEstimatedBytes: number;
  readonly changeLogFileBytes: number;
  readonly replicaFileBytes: number;
  readonly levels: readonly LevelResult[];
};

function run(w: Workload): WorkloadResult {
  const rand = mulberry32(0x5eed ^ hashName(w.name));
  const harness = setUp(w, rand);
  const {replica, writer, pageSize} = harness;
  const runState = new RunState(harness.replicaPath, pageSize);
  const levels = LADDER.map(level => new LevelAccumulator(level));
  const replicaRunner = new StatementRunner(replica);
  const walPath = `${harness.replicaPath}-wal`;

  const ids = idSequence(w, rand);
  const upsert = replica.prepare(
    w.op === 'insert'
      ? /*sql*/ `
        INSERT INTO "issue" ("id", "ownerId", "modified", "title", "body",
          "${ZERO_VERSION_COLUMN_NAME}")
          VALUES (@id, @ownerId, @modified, @title, @body, @version)
      `
      : w.op === 'update'
        ? /*sql*/ `
        UPDATE "issue" SET "ownerId" = @ownerId, "modified" = @modified,
          "title" = @title, "body" = @body,
          "${ZERO_VERSION_COLUMN_NAME}" = @version
          WHERE "id" = @id
      `
        : /*sql*/ `DELETE FROM "issue" WHERE "id" = @id`,
  );
  const updateState = replica.prepare(/*sql*/ `
    UPDATE "_zero.replicationState" SET "stateVersion" = ?, "writeTimeMs" = ?
  `);

  const totalIntervals = Math.max(
    1,
    Math.ceil(WINDOW_SECONDS / MONITOR_SECONDS),
  );
  const writesPerInterval = WRITES / totalIntervals;

  let write = 0;
  let tx = 0;
  for (let interval = 0; interval < totalIntervals; interval++) {
    const until = Math.min(
      WRITES,
      Math.round((interval + 1) * writesPerInterval),
    );
    const logical: Buffer[] = [];

    while (write < until) {
      const rows = Math.min(TX_ROWS, until - write);
      const watermark = versionToLexi(WATERMARK_BASE + tx);
      const writeTimeMs =
        SIMULATED_EPOCH_MS + (interval + 1) * MONITOR_SECONDS * 1000;

      const begin: ChangeStreamData = [
        'begin',
        {tag: 'begin'},
        {commitWatermark: watermark},
      ];
      const beginJSON = serializeChangeStreamData(begin);
      writer.begin(watermark, beginJSON);
      logical.push(Buffer.from(beginJSON + '\n'));

      replicaRunner.beginImmediate();
      for (let r = 0; r < rows; r++) {
        const row = makeRow(rand, ids[write + r], interval);
        const change = changeFor(w, row, watermark);
        const json = serializeChangeStreamData(['data', change]);
        writer.append(json, change);
        logical.push(Buffer.from(json + '\n'));
        upsert.run({...row, version: watermark});
      }
      updateState.run(watermark, writeTimeMs);
      replicaRunner.commit();

      const commit: ChangeStreamData = [
        'commit',
        {tag: 'commit', commitTimeMs: writeTimeMs},
        {watermark},
      ];
      const commitJSON = serializeChangeStreamData(commit);
      // Production commits the log before the replica; the order does not
      // change either side's byte count, and doing it here keeps the log's
      // transaction from spanning the replica's.
      writer.commit(watermark, commitJSON, writeTimeMs);
      logical.push(Buffer.from(commitJSON + '\n'));

      write += rows;
      tx++;
    }

    const capture = readWalCapture(walPath);
    const logicalBuf = Buffer.concat(logical);
    const pages = new Uint32Array(sortedPages(capture.images.keys()));
    runState.intervals.push({
      pages,
      logical: logicalBuf,
      walFrames: capture.frames,
      l0: measureL0(capture, logicalBuf, pageSize),
    });
    // What litestream's monitor loop does once it has captured the interval.
    replica.pragma('wal_checkpoint(TRUNCATE)');

    for (const level of levels) {
      level.maybeClose(runState, runState.intervals.length, false);
    }
  }
  for (const level of levels) {
    level.maybeClose(runState, runState.intervals.length, true);
  }

  const results = levels.map(level => level.result());
  const {estimatedBytes} = harness.log
    .prepare(/*sql*/ `SELECT coalesce(sum("estimatedBytes"), 0) AS "estimatedBytes"
                 FROM "_zero.changeLogStream"`)
    .get<{estimatedBytes: number}>();
  const l0 = results.find(r => r.level === 'L0');
  const windowLevel = results.find(r => r.level === 'window');
  const replicaFileBytes = statSync(harness.replicaPath).size;
  const logFileBytes = statSync(harness.logPath).size;
  const [{page_size: finalPageSize}] = replica.pragma<{page_size: number}>(
    'page_size',
  );
  const distinctIds = new Set(ids).size;
  const result: WorkloadResult = {
    workload: w.name,
    description: w.description,
    op: w.op,
    locality: w.locality,
    writes: write,
    transactions: tx,
    distinctRows: distinctIds,
    repeatFactor: write / distinctIds,
    tableRows: w.tableRows,
    pageSize: finalPageSize,
    rowsPerPage: finalPageSize / ROW_BYTES,
    intervals: runState.intervals.length,
    walFrames: runState.intervals.reduce((n, i) => n + i.walFrames, 0),
    dirtyPagesPerWriteL0: (l0?.pages ?? 0) / Math.max(1, write),
    dirtyPagesPerWriteWindow: (windowLevel?.pages ?? 0) / Math.max(1, write),
    changeLogEstimatedBytes: Number(estimatedBytes),
    changeLogFileBytes: logFileBytes,
    replicaFileBytes,
    levels: results,
  };
  harness.close();
  return result;
}

function measureL0(
  capture: WalCapture,
  logical: Buffer,
  pageSize: number,
): FileMeasurement {
  const ltx = new ChunkedCompressor();
  for (const pgno of sortedPages(capture.images.keys())) {
    ltx.push(capture.images.get(pgno) as Buffer);
  }
  const logicalCompressor = new ChunkedCompressor();
  logicalCompressor.push(logical);
  const ltxTotals = ltx.finish();
  const logicalTotals = logicalCompressor.finish();
  const pages = capture.images.size;
  return {
    pages,
    ltxRawBytes: pages === 0 ? 0 : ltxBytes(pages, pageSize),
    ltxCompressedBytes:
      ltxTotals.compressedBytes === undefined || pages === 0
        ? ltxTotals.compressedBytes
        : ltxTotals.compressedBytes + LTX_HEADER_BYTES + LTX_TRAILER_BYTES,
    logicalRawBytes: logicalTotals.rawBytes,
    logicalCompressedBytes: logicalTotals.compressedBytes,
  };
}

function hashName(name: string): number {
  let h = 0;
  for (let i = 0; i < name.length; i++) {
    h = (Math.imul(h, 31) + name.charCodeAt(i)) | 0;
  }
  return h;
}

// ---------------------------------------------------------------------------
// reporting
// ---------------------------------------------------------------------------

const MB = 1_000_000;

function mb(bytes: number | undefined): string {
  return bytes === undefined ? '-' : (bytes / MB).toFixed(2);
}

function ratio(value: number | undefined): string {
  if (value === undefined) {
    return '-';
  }
  return value >= 10 ? `${value.toFixed(0)}x` : `${value.toFixed(2)}x`;
}

function report(r: WorkloadResult): void {
  console.log('');
  console.log(`## ${r.workload} — ${r.description}`);
  console.log(
    `   ${r.writes} ${r.op}s over ${r.distinctRows} distinct rows ` +
      `(r=${r.repeatFactor.toFixed(1)}), ${r.transactions} transactions, ` +
      `${r.intervals} x ${MONITOR_SECONDS}s intervals`,
  );
  console.log(
    `   page_size=${r.pageSize} (~${r.rowsPerPage.toFixed(1)} rows/page), ` +
      `dirty pages/write: ${r.dirtyPagesPerWriteL0.toFixed(2)} at L0, ` +
      `${r.dirtyPagesPerWriteWindow.toFixed(2)} over the window`,
  );
  const header = [
    'level'.padEnd(7),
    'files'.padStart(6),
    'ltx MB'.padStart(9),
    `ltx ${COMPRESSION.name}`.padStart(11),
    'log MB'.padStart(9),
    `log ${COMPRESSION.name}`.padStart(11),
    'ltx/log'.padStart(9),
    'compressed'.padStart(11),
  ].join(' ');
  console.log(`   ${header}`);
  for (const level of r.levels) {
    console.log(
      `   ${[
        level.level.padEnd(7),
        String(level.files).padStart(6),
        mb(level.ltxRawBytes).padStart(9),
        mb(level.ltxCompressedBytes).padStart(11),
        mb(level.logicalRawBytes).padStart(9),
        mb(level.logicalCompressedBytes).padStart(11),
        ratio(level.rawRatio).padStart(9),
        ratio(level.compressedRatio).padStart(11),
      ].join(' ')}`,
    );
  }
  console.log(
    `   change log on disk ${mb(r.changeLogFileBytes)} MB ` +
      `(sum(estimatedBytes) ${mb(r.changeLogEstimatedBytes)} MB); ` +
      `replica ${mb(r.replicaFileBytes)} MB, which is what one snapshot costs`,
  );
}

function environment() {
  return {
    node: process.version,
    platform: platform(),
    release: release(),
    arch: arch(),
    cpus: cpus().length,
    cpuModel: cpus()[0]?.model,
    totalMemoryGB: Math.round(totalmem() / 1024 ** 3),
  };
}

// ---------------------------------------------------------------------------
// the bench
// ---------------------------------------------------------------------------

const results: WorkloadResult[] = [];

afterAll(() => {
  if (results.length === 0) {
    return;
  }
  console.log(
    JSON.stringify({
      logicalVsLtxSizeBenchmark: {
        environment: environment(),
        configuration: {
          quick: QUICK,
          writes: WRITES,
          windowHours: WINDOW_HOURS,
          monitorSeconds: MONITOR_SECONDS,
          tableRows: TABLE_ROWS,
          rowBytes: ROW_BYTES,
          txRows: TX_ROWS,
          secondaryIndexes: SECONDARY_INDEXES,
          indexChurn: INDEX_CHURN,
          compression: COMPRESSION.name,
          compressionChunkBytes: COMPRESSION_CHUNK_BYTES,
          ladder: LADDER,
          ltxFraming: {
            headerBytes: LTX_HEADER_BYTES,
            pageHeaderBytes: LTX_PAGE_HEADER_BYTES,
            trailerBytes: LTX_TRAILER_BYTES,
          },
        },
        results,
      },
    }),
  );
});

describe('logical log vs litestream LTX size', () => {
  for (const w of workloads()) {
    test(
      w.name,
      () => {
        const result = run(w);
        results.push(result);
        report(result);

        expect(result.writes).toBe(WRITES);
        // Every workload must actually dirty pages, or the physical side is
        // measuring nothing and the ratios are meaningless.
        expect(result.dirtyPagesPerWriteWindow).toBeGreaterThan(0);
      },
      3_600_000,
    );
  }
});
