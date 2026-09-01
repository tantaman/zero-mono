/**
 * Zero's half of the **derivation plane** comparison: what does it cost
 * zero-cache to keep V registered queries up to date, per replicated write?
 *
 * Its counterpart is rindle's `rust/rindle-replica/examples/bench_cluster_derive.rs`
 * (the parallel `Cluster`: one writer/coordinator thread + N IVM worker
 * threads). Same chinook data, same query shapes, same write stream;
 * `rindle/tools/cluster-vs-zero-cache.mjs` runs both and joins the numbers.
 *
 * This is the REAL zero-cache path, not a stand-in for it: a `PipelineDriver`
 * over a wal2 replica file, queries added with `addQuery`, writes delivered as
 * replication transactions through the same `ChangeProcessor` the replicator
 * uses, and each transaction's derived `RowChange`s drained out of
 * `advance()`. What is left out is downstream of derivation (CVR storage, poke
 * assembly, the websocket) — the parts rindle's `Cluster` also does not do.
 *
 * The asymmetry the comparison is about is architectural, and is reported
 * rather than averaged away: zero-cache derives every query of a client group
 * on ONE thread inside `advance`, and scales by running more syncer threads
 * across more client groups. So its worker count is pinned at 1, and the
 * like-for-like cell is rindle's `cluster_w1`; rindle's w2/w4 columns are the
 * architectural delta, not a fair "same resources" comparison.
 *
 * Usage:
 *   node --experimental-transform-types --expose-gc \
 *     packages/zql-benchmarks/src/ivm-head-to-head/zero-cache-derive.ts
 *   node ... zero-cache-derive.ts --cell filter 1 8
 *
 * Env: CHINOOK_SQL, DERIVE_SCALES=1,10, DERIVE_VIEWS=1,8,32, DERIVE_W=200,
 *      DERIVE_SHAPES=filter,exists
 *
 * Output: `res|zero_cache|shape|scaleS|viewsV|metric|value` lines.
 */
import {spawnSync} from 'node:child_process';
import {readFileSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {testLogConfig} from '../../../otel/src/test-log-config.ts';
import type {ReadonlyJSONValue} from '../../../shared/src/json.ts';
import {createSilentLogContext} from '../../../shared/src/logging-test-utils.ts';
import {randInt} from '../../../shared/src/rand.ts';
import {deleteLiteDB} from '../../../zero-cache/src/db/delete-lite-db.ts';
import {listTables} from '../../../zero-cache/src/db/lite-tables.ts';
import {InspectorDelegate} from '../../../zero-cache/src/server/inspector-delegate.ts';
import {populateFromExistingTables} from '../../../zero-cache/src/services/replicator/schema/column-metadata.ts';
import {initReplicationState} from '../../../zero-cache/src/services/replicator/schema/replication-state.ts';
import {
  fakeReplicator,
  ReplicationMessages,
} from '../../../zero-cache/src/services/replicator/test-utils.ts';
import {
  PipelineDriver,
  type Timer,
} from '../../../zero-cache/src/services/view-syncer/pipeline-driver.ts';
import {Snapshotter} from '../../../zero-cache/src/services/view-syncer/snapshotter.ts';
import {
  upstreamSchema,
  type ShardID,
} from '../../../zero-cache/src/types/shards.ts';
import type {AST} from '../../../zero-protocol/src/ast.ts';
import {createSchema} from '../../../zero-schema/src/builder/schema-builder.ts';
import {
  number,
  string,
  table,
} from '../../../zero-schema/src/builder/table-builder.ts';
import {
  CREATE_STORAGE_TABLE,
  DatabaseStorage,
} from '../../../zqlite/src/database-storage.ts';
import {Database} from '../../../zqlite/src/db.ts';
import {loadScaled, STREAM_ID_BASE, type TableData} from './data.ts';
import {fmtNs} from './harness.ts';

const SHARD: ShardID = {appID: 'bench', shardNum: 1};
const MUTATIONS_TABLE = `${upstreamSchema(SHARD)}.mutations`;

/**
 * No advancement budget. `PipelineDriver` aborts an `advance` with a
 * `ResetPipelinesSignal` when it estimates the advancement will outrun a
 * hydration; a benchmark wants the real cost of the advancement, not the
 * abort, so the timer reports zero elapsed — the same seam the driver's own
 * tests use.
 */
const NO_BUDGET: Timer = {elapsedLap: () => 0, totalElapsed: () => 0};

// ---------------------------------------------------------------------------
// The replica: zero-cache's own lite-table shape
// ---------------------------------------------------------------------------

/**
 * zero-cache column types are the UPSTREAM Postgres type plus attributes
 * (`types/lite.ts`), chosen so SQLite's affinity rules still land on the right
 * storage class: `float8` -> REAL, `text` -> TEXT.
 */
const f8 = (notNull: boolean) => `"float8${notNull ? '|NOT_NULL' : ''}"`;
const tx = (notNull: boolean) => `"text${notNull ? '|NOT_NULL' : ''}"`;

function createReplica(
  lc: ReturnType<typeof createSilentLogContext>,
  path: string,
  data: TableData,
): Database {
  const db = new Database(lc, path);
  // The REPLICA's pragmas — wal2, plus what `migration-lite` and `applyPragmas`
  // leave a replica file with (`workers/replicator.ts` supplies these values).
  // `synchronous` matters most: left at SQLite's FULL default it would fsync
  // every commit and this benchmark would be measuring durability policy rather
  // than derivation. rindle's writer connection is wal2 + NORMAL to match.
  //
  // Note for anyone extending this: do NOT reach for `applyChangeLogPragmas`.
  // That is the CHANGE LOG database's pragma set, and its
  // `auto_vacuum = INCREMENTAL` makes the replica throw `database is locked`
  // the moment a pipeline's `TableSource` writes an advancement through.
  db.pragma('busy_timeout = 30000');
  db.pragma('analysis_limit = 1000');
  db.pragma('journal_mode = wal2');
  db.pragma('synchronous = NORMAL');
  initReplicationState(db, ['zero_data'], '123');
  db.exec(`
    CREATE TABLE "${MUTATIONS_TABLE}" (
      "clientGroupID" TEXT, "clientID" TEXT, "mutationID" INTEGER,
      "result" TEXT, _0_version TEXT NOT NULL,
      PRIMARY KEY ("clientGroupID", "clientID", "mutationID")
    );
    CREATE TABLE "Album" (
      "AlbumId" ${f8(true)}, "Title" ${tx(true)}, "ArtistId" ${f8(true)},
      _0_version TEXT NOT NULL, PRIMARY KEY ("AlbumId")
    );
    CREATE TABLE "Track" (
      "TrackId" ${f8(true)}, "Name" ${tx(true)}, "AlbumId" ${f8(false)},
      "MediaTypeId" ${f8(true)}, "GenreId" ${f8(false)}, "Composer" ${tx(false)},
      "Milliseconds" ${f8(true)}, "Bytes" ${f8(false)}, "UnitPrice" ${f8(true)},
      _0_version TEXT NOT NULL, PRIMARY KEY ("TrackId")
    );
  `);

  const insert = (name: string, cols: readonly string[]) =>
    db.prepare(
      `INSERT INTO "${name}" (${cols.map(c => `"${c}"`).join(',')}, _0_version)
       VALUES (${cols.map(() => '?').join(',')}, '123')`,
    );
  db.transaction(() => {
    const albumCols = ['AlbumId', 'Title', 'ArtistId'];
    const albums = insert('Album', albumCols);
    for (const row of data.Album ?? []) {
      albums.run(albumCols.map(c => row[c] ?? null));
    }
    const trackCols = [
      'TrackId',
      'Name',
      'AlbumId',
      'MediaTypeId',
      'GenreId',
      'Composer',
      'Milliseconds',
      'Bytes',
      'UnitPrice',
    ];
    const tracks = insert('Track', trackCols);
    for (const row of data.Track) {
      tracks.run(trackCols.map(c => row[c] ?? null));
    }
  });

  // Same indexes as every other contestant (triangle-bench fairness rule 1).
  db.exec(`
    CREATE INDEX "ix_Track_0" ON "Track" ("AlbumId");
    CREATE INDEX "ix_Track_1" ON "Track" ("GenreId");
    CREATE INDEX "ix_Track_2" ON "Track" ("Milliseconds");
    ANALYZE;
  `);
  populateFromExistingTables(db, listTables(db, false));
  return db;
}

const albumTable = table('Album')
  .columns({AlbumId: number(), Title: string(), ArtistId: number()})
  .primaryKey('AlbumId');

const trackTable = table('Track')
  .columns({
    TrackId: number(),
    Name: string(),
    AlbumId: number().optional(),
    MediaTypeId: number(),
    GenreId: number().optional(),
    Composer: string().optional(),
    Milliseconds: number(),
    Bytes: number().optional(),
    UnitPrice: number(),
  })
  .primaryKey('TrackId');

const clientSchema = createSchema({tables: [albumTable, trackTable]});

// ---------------------------------------------------------------------------
// Shapes — the same two rindle's cluster bench registers, parameterized per
// view so V queries are V distinct pipelines rather than V copies of one.
// ---------------------------------------------------------------------------

const genreFor = (i: number) => (i % 25) + 1;

/**
 * The high-cardinality keys the `*_key` shapes are parameterized by, so V
 * registered queries are V genuinely DISTINCT queries rather than V copies of
 * one. Derived from the loaded data exactly as the rindle side derives them
 * (sorted, deduped), so both engines register the same V queries.
 */
export type Keys = {albums: number[]; artists: number[]};

export function keysFrom(data: TableData): Keys {
  const uniq = (xs: number[]) =>
    [...new Set(xs)].toSorted((a: number, b: number) => a - b);
  return {
    albums: uniq(data.Album.map(r => r.AlbumId as number)),
    artists: uniq(data.Album.map(r => r.ArtistId as number)),
  };
}

/** Resident set size of this process, in bytes — the same `/proc/self/status`
 * `VmRSS` the rindle side reads. Whole-process, so it counts the engine, the
 * SQLite page cache and the runtime alike on both sides; unlike a heap delta it
 * is genuinely the same instrument across a Node process and a Rust one. */
const WHITESPACE = /\s+/;

function rssBytes(): number {
  try {
    for (const line of readFileSync('/proc/self/status', 'utf8').split('\n')) {
      if (line.startsWith('VmRSS:')) {
        return Number(line.split(WHITESPACE)[1]) * 1024;
      }
    }
  } catch {
    // not Linux, or /proc unreadable — reported as 0 rather than guessed
  }
  return 0;
}

function eq(name: string, value: number) {
  return {
    type: 'simple',
    op: '=',
    left: {type: 'column', name},
    right: {type: 'literal', value},
  } as const;
}

function astFor(shape: string, i: number, keys: Keys): AST {
  const g = genreFor(i);
  const genreEq = {
    type: 'simple',
    op: '=',
    left: {type: 'column', name: 'GenreId'},
    right: {type: 'literal', value: g},
  } as const;
  if (shape === 'filter') {
    return {table: 'Track', orderBy: [['TrackId', 'asc']], where: genreEq};
  }
  if (shape === 'exists') {
    return {
      table: 'Album',
      orderBy: [['AlbumId', 'asc']],
      where: {
        type: 'correlatedSubquery',
        op: 'EXISTS',
        related: {
          correlation: {parentField: ['AlbumId'], childField: ['AlbumId']},
          subquery: {
            table: 'Track',
            alias: 'has_genre',
            orderBy: [['TrackId', 'asc']],
            where: genreEq,
          },
        },
      },
    };
  }
  const album = keys.albums[i % keys.albums.length];
  const artist = keys.artists[i % keys.artists.length];
  // one album's tracks
  if (shape === 'filter_key') {
    return {
      table: 'Track',
      orderBy: [['TrackId', 'asc']],
      where: eq('AlbumId', album),
    };
  }
  // one album's five longest tracks
  if (shape === 'take_key') {
    return {
      table: 'Track',
      where: eq('AlbumId', album),
      orderBy: [['Milliseconds', 'desc']],
      limit: 5,
    };
  }
  // one artist's albums, each with its tracks nested
  if (shape === 'related_key') {
    return {
      table: 'Album',
      orderBy: [['AlbumId', 'asc']],
      where: eq('ArtistId', artist),
      related: [
        {
          correlation: {parentField: ['AlbumId'], childField: ['AlbumId']},
          subquery: {
            table: 'Track',
            alias: 'tracks',
            orderBy: [['TrackId', 'asc']],
          },
        },
      ],
    };
  }
  // one artist's albums that have a rock track
  if (shape === 'exists_key') {
    return {
      table: 'Album',
      orderBy: [['AlbumId', 'asc']],
      where: {
        type: 'and',
        conditions: [
          eq('ArtistId', artist),
          {
            type: 'correlatedSubquery',
            op: 'EXISTS',
            related: {
              correlation: {parentField: ['AlbumId'], childField: ['AlbumId']},
              subquery: {
                table: 'Track',
                alias: 'has_genre',
                orderBy: [['TrackId', 'asc']],
                where: eq('GenreId', 1),
              },
            },
          },
        ],
      },
    };
  }
  throw new Error(`unknown shape ${shape}`);
}

/**
 * The organic write stream, batched into transactions and identical in
 * construction to the rindle harness: `txns` transactions of `rowsPerTxn` rows.
 * Returned as plain JSON rows because that is what a replication message
 * carries; an absent cell is `null` on the wire, never `undefined`.
 *
 * Transaction SIZE is a first-class knob because it moves the two systems
 * differently — per-transaction cost is amortized over the batch, per-row cost
 * is not — so holding it equal is the precondition for comparing writes/sec.
 */
function streamTxns(
  data: TableData,
  txns: number,
  rowsPerTxn: number,
): Record<string, ReadonlyJSONValue>[][] {
  const base = data.Track;
  const stride = 7919;
  let next = 0;
  return Array.from({length: txns}, () =>
    Array.from({length: rowsPerTxn}, () => {
      const row: Record<string, ReadonlyJSONValue> = {};
      for (const [k, v] of Object.entries(
        base[(next * stride) % base.length],
      )) {
        row[k] = v ?? null;
      }
      row.TrackId = STREAM_ID_BASE + next;
      next++;
      return row;
    }),
  );
}

// ---------------------------------------------------------------------------
// Cell
// ---------------------------------------------------------------------------

function runCell(shape: string, scale: number, views: number) {
  // `DERIVE_W` is the transaction count, `DERIVE_ROWS_PER_TXN` the batch size.
  // The rindle harness reads the same two knobs and the runner passes them
  // through unchanged.
  const txns = Number(process.env.DERIVE_W ?? 200);
  const rowsPerTxn = Number(process.env.DERIVE_ROWS_PER_TXN ?? 1);
  const lc = createSilentLogContext();
  const path = join(tmpdir(), `zero-cache-derive-${randInt(1e6, 9e6)}.db`);
  const rssBase = rssBytes();
  const data = loadScaled(lc, scale, ['Album', 'Track']);
  const keys = keysFrom(data);

  const db = createReplica(lc, path, data);
  const storage = new Database(lc, ':memory:');
  storage.prepare(CREATE_STORAGE_TABLE).run();

  const pipelines = new PipelineDriver(
    lc,
    testLogConfig,
    new Snapshotter(lc, path, {appID: SHARD.appID}),
    SHARD,
    new DatabaseStorage(storage).createClientGroupStorage('bench-group'),
    'zero-cache-derive',
    new InspectorDelegate(undefined),
    // zero-cache's production defaults, not benchmark-friendly ones: a 10 ms
    // IVM yield threshold (`zero-config`'s `yieldThresholdMs`, floored at 2 by
    // the syncer) and the query planner ON (`enableQueryPlanner` defaults to
    // true). rindle's `Cluster` also plans by default, so both sides are
    // running the configuration their operators actually ship.
    () => 10,
    true, // enablePlanner
  );

  try {
    pipelines.init(clientSchema);

    // V must not exceed the distinct-key budget, or the "V unique queries"
    // premise quietly becomes "V queries, some identical" — which would flatter
    // whichever engine dedupes or caches better. A hard error, not a warning.
    if (shape.endsWith('_key')) {
      const budget =
        shape === 'related_key' || shape === 'exists_key'
          ? keys.artists.length
          : keys.albums.length;
      if (views > budget) {
        throw new Error(
          `views=${views} exceeds the ${budget} distinct keys available for ` +
            `shape ${shape} at scale ${scale}: the queries would repeat. ` +
            `Raise the scale.`,
        );
      }
    }

    // hydrate: register + hydrate all V queries, cold.
    const t0 = process.hrtime.bigint();
    let hydrated = 0;
    for (let i = 0; i < views; i++) {
      for (const change of pipelines.addQuery(
        `hash${i}`,
        `query${i}`,
        astFor(shape, i, keys),
        NO_BUDGET,
      )) {
        if (change !== 'yield') {
          hydrated++;
        }
      }
    }
    const hydrateNs = Number(process.hrtime.bigint() - t0);
    // RSS with all V queries hydrated and held — the cost of HOLDING them,
    // sampled before the write stream puts deltas in flight.
    const rssHydrated = rssBytes();

    // derive: W replication transactions, each one row, each fully drained.
    const messages = new ReplicationMessages({
      Album: 'AlbumId',
      Track: 'TrackId',
      [MUTATIONS_TABLE]: ['clientGroupID', 'clientID', 'mutationID'],
    });
    const batches = streamTxns(data, txns, rowsPerTxn);
    const replicator = fakeReplicator(lc, db);

    let derived = 0;
    const t1 = process.hrtime.bigint();
    for (let i = 0; i < batches.length; i++) {
      replicator.processTransaction(
        String(200 + i),
        ...batches[i].map(row => messages.insert('Track', row)),
      );
      // The changes MUST be iterated in full — that is what advances the
      // snapshot, and it is where the derivation actually happens.
      for (const change of pipelines.advance(NO_BUDGET).changes) {
        if (change !== 'yield') {
          derived++;
        }
      }
    }
    const deriveNs = Number(process.hrtime.bigint() - t1);
    const rssPeak = rssBytes();

    const emit = (metric: string, value: number) =>
      process.stdout.write(
        `res|zero_cache|${shape}|scale${scale}|views${views}|${metric}|${value.toFixed(1)}\n`,
      );
    const rowsWritten = txns * rowsPerTxn;
    emit('hydrate_all_ns', hydrateNs);
    emit('derive_ns_per_write', deriveNs / txns);
    emit('writes_per_sec', 1e9 / (deriveNs / txns));
    emit('rows_per_sec', rowsWritten / (deriveNs / 1e9));
    emit('rows_per_txn', rowsPerTxn);
    emit('rss_hydrated_bytes', rssHydrated);
    emit('rss_peak_bytes', rssPeak);
    emit(
      'rss_per_query_bytes',
      views === 0 ? 0 : Math.max(0, rssHydrated - rssBase) / views,
    );
    // Named to match the rindle side: `hydrate_rows` is the initial result set
    // (the cross-engine parity number for the flat `filter` shape), `delta_rows`
    // the per-write deltas. `updates` has no Zero counterpart — zero-cache does
    // not emit a per-query message, it yields one flat stream — so it is left
    // unreported rather than faked.
    emit('hydrate_rows', hydrated);
    emit('delta_rows', derived);
  } finally {
    pipelines.destroy();
    db.close();
    storage.close();
    deleteLiteDB(path);
  }
}

// ---------------------------------------------------------------------------
// Driver
// ---------------------------------------------------------------------------

const nums = (name: string, dflt: string) =>
  (process.env[name] ?? dflt).split(',').map(s => Number(s.trim()));
const strs = (name: string, dflt: string) =>
  (process.env[name] ?? dflt).split(',').map(s => s.trim());

const argv = process.argv.slice(2);
if (argv[0] === '--cell') {
  runCell(argv[1], Number(argv[2]), Number(argv[3]));
} else {
  const results: string[] = [];
  for (const scale of nums('DERIVE_SCALES', '1,10')) {
    for (const shape of strs('DERIVE_SHAPES', 'filter,exists')) {
      // `0` is the plumbing floor: same write stream, no query registered, so
      // (V) - (0) isolates derivation from the snapshotter/changelog machinery.
      for (const views of nums('DERIVE_VIEWS', '0,1,8,32')) {
        process.stderr.write(`# cell ${shape}:scale${scale}:views${views}\n`);
        const out = spawnSync(
          process.execPath,
          [
            ...process.execArgv,
            process.argv[1],
            '--cell',
            shape,
            String(scale),
            String(views),
          ],
          {encoding: 'utf8', env: process.env},
        );
        if (out.status !== 0) {
          process.stderr.write(
            `# CELL FAILED ${shape}:scale${scale}:views${views}\n${out.stderr}\n`,
          );
          continue;
        }
        process.stdout.write(out.stdout);
        for (const line of out.stdout.split('\n')) {
          if (line.includes('|derive_ns_per_write|')) {
            const parts = line.split('|');
            results.push(
              `${parts[2]} scale${parts[3].slice(5)} views${parts[4].slice(5)}: ${fmtNs(Number(parts[6]))}/write`,
            );
          }
        }
      }
    }
  }
  process.stderr.write('\n' + results.join('\n') + '\n');
}
