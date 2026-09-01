/**
 * Chinook data loading for the Zero-vs-rindle IVM head-to-head.
 *
 * A line-for-line port of the loader in rindle's `rust/rindle-triangle-bench/
 * src/main.rs` (`table_defs` / `load_scaled` / `load_sqlite_one`), so both
 * engines see the SAME rows, the SAME column types and the SAME SQLite indexes.
 * Any divergence here invalidates the comparison — keep the two in sync.
 */
import {readFileSync} from 'node:fs';
import type {LogConfig} from '../../../otel/src/log-options.ts';
import type {createSilentLogContext} from '../../../shared/src/logging-test-utils.ts';
import type {Row, Value} from '../../../zero-protocol/src/data.ts';
import type {SchemaValue} from '../../../zero-schema/src/table-schema.ts';
import {Database} from '../../../zqlite/src/db.ts';

/**
 * Id offset between scaled copies. Copy `k` occupies `[k*ID_STEP, k*ID_STEP +
 * 3503]`, so at scale <= 30 the max id is ~3.0e8 and the synthetic write ids
 * below never collide with a loaded row.
 */
/**
 * `@rocicorp/logger`'s `LogContext`, named without importing the package (it is
 * not a declared dependency of this workspace member).
 */
export type LogContext = ReturnType<typeof createSilentLogContext>;

export const ID_STEP = 10_000_000;
/** Id of the single row used by the labeled `push_*` cases. */
export const PUSH_ID = 899_999_999;
/** First id of the organic write stream. */
export const STREAM_ID_BASE = 900_000_000;

export type ColType = 'number' | 'string';

export type ColumnDef = {
  readonly name: string;
  readonly type: ColType;
  readonly optional: boolean;
};

export type TableDef = {
  readonly name: string;
  readonly cols: readonly ColumnDef[];
  /** Primary key, as indexes into `cols`. */
  readonly pk: readonly number[];
  /** Secondary indexes, as index-lists into `cols`. */
  readonly indexes: readonly (readonly number[])[];
  /** Columns that get `k * ID_STEP` added in scaled copy `k`. */
  readonly scaleCols: readonly number[];
};

const num = (name: string, optional: boolean): ColumnDef => ({
  name,
  type: 'number',
  optional,
});
const txt = (name: string, optional: boolean): ColumnDef => ({
  name,
  type: 'string',
  optional,
});

export const TABLE_DEFS: readonly TableDef[] = [
  {
    name: 'Album',
    cols: [num('AlbumId', false), txt('Title', false), num('ArtistId', false)],
    pk: [0],
    indexes: [],
    scaleCols: [0, 2],
  },
  {
    name: 'Track',
    cols: [
      num('TrackId', false),
      txt('Name', false),
      num('AlbumId', true),
      num('MediaTypeId', false),
      num('GenreId', true),
      txt('Composer', true),
      num('Milliseconds', false),
      num('Bytes', true),
      num('UnitPrice', false),
    ],
    pk: [0],
    // FK indexes (AlbumId, GenreId) + the top-50 sort column (Milliseconds).
    // Identical to the rindle side (fairness rule 1).
    indexes: [[2], [4], [6]],
    scaleCols: [0, 2],
  },
];

export function tableDef(name: string): TableDef {
  const def = TABLE_DEFS.find(d => d.name === name);
  if (!def) {
    throw new Error(`unknown table ${name}`);
  }
  return def;
}

/** The `Record<string, SchemaValue>` a Zero `Source` wants for `def`. */
export function columnsOf(def: TableDef): Record<string, SchemaValue> {
  return Object.fromEntries(
    def.cols.map(c => [c.name, {type: c.type, optional: c.optional}]),
  );
}

export function primaryKeyOf(def: TableDef): [string, ...string[]] {
  const pk = def.pk.map(i => def.cols[i].name);
  return pk as [string, ...string[]];
}

export type TableData = Record<string, Row[]>;

const q = (s: string) => `"${s.replaceAll('"', '""')}"`;

/**
 * Read the chinook dump into an in-memory db and project `needs`, scaled by
 * `scale` copies with id offsets. Mirrors rindle's `load_scaled`.
 */
export function loadScaled(
  lc: LogContext,
  scale: number,
  needs: readonly string[],
): TableData {
  const path = process.env.CHINOOK_SQL ?? '/tmp/Chinook_Sqlite.sql';
  const scratch = new Database(lc, ':memory:');
  try {
    scratch.exec(readChinookSql(path));
    const out: TableData = {};
    for (const def of TABLE_DEFS) {
      if (!needs.includes(def.name)) {
        continue;
      }
      const cols = def.cols.map(c => q(c.name)).join(', ');
      const base = scratch
        .prepare(`SELECT ${cols} FROM ${q(def.name)}`)
        .all() as Row[];
      const rows: Row[] = [];
      for (let k = 0; k < scale; k++) {
        const off = k * ID_STEP;
        for (const row of base) {
          const copy: Record<string, Value> = {};
          for (const c of def.cols) {
            copy[c.name] = row[c.name] ?? null;
          }
          for (const ci of def.scaleCols) {
            const name = def.cols[ci].name;
            const v = copy[name];
            if (typeof v === 'number') {
              copy[name] = v + off;
            }
          }
          rows.push(copy as Row);
        }
      }
      out[def.name] = rows;
    }
    return out;
  } finally {
    scratch.close();
  }
}

function readChinookSql(path: string): string {
  try {
    return readFileSync(path, 'utf8');
  } catch (e) {
    throw new Error(
      `read chinook SQL at ${path}: ${String(e)}\n` +
        `  set CHINOOK_SQL, or download the dump:\n` +
        `  curl -fsSL -o /tmp/Chinook_Sqlite.sql https://github.com/lerocha/chinook-database/releases/download/v1.4.5/Chinook_Sqlite.sql`,
    );
  }
}

const sqliteType = (t: ColType) => (t === 'number' ? 'REAL' : 'TEXT');

/**
 * Create + bulk-load one table with its PK unique index, secondary indexes and
 * `ANALYZE`. Mirrors rindle's `load_sqlite_one` exactly, including index shape
 * — the SQLite floor must be identical on both sides.
 */
export function loadSqliteTable(
  db: Database,
  def: TableDef,
  rows: readonly Row[],
): void {
  const coldefs = def.cols
    .map(
      c => `${q(c.name)} ${sqliteType(c.type)}${c.optional ? '' : ' NOT NULL'}`,
    )
    .join(', ');
  db.exec(`CREATE TABLE ${q(def.name)} (${coldefs});`);

  const colnames = def.cols.map(c => q(c.name)).join(', ');
  const placeholders = def.cols.map(() => '?').join(', ');
  const insert = db.prepare(
    `INSERT INTO ${q(def.name)} (${colnames}) VALUES (${placeholders})`,
  );
  db.exec('BEGIN');
  for (const row of rows) {
    insert.run(...def.cols.map(c => row[c.name] ?? null));
  }
  db.exec('COMMIT');

  const pk = def.pk.map(i => q(def.cols[i].name)).join(', ');
  db.exec(
    `CREATE UNIQUE INDEX ${q(`pk_${def.name}`)} ON ${q(def.name)} (${pk});`,
  );
  def.indexes.forEach((idx, k) => {
    const cols = idx.map(i => q(def.cols[i].name)).join(', ');
    db.exec(
      `CREATE INDEX ${q(`ix_${def.name}_${k}`)} ON ${q(def.name)} (${cols});`,
    );
  });
  db.exec('ANALYZE');
}

/** One SQLite db per table, matching rindle's one-connection-per-TableSource. */
export function sqliteDbFor(
  lc: LogContext,
  _logConfig: LogConfig,
  def: TableDef,
  rows: readonly Row[],
): Database {
  const db = new Database(lc, ':memory:');
  loadSqliteTable(db, def, rows);
  return db;
}
