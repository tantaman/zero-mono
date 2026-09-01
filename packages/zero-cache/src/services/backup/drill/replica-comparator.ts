import {createHash} from 'node:crypto';
import type {LogContext} from '@rocicorp/logger';
import type {Database} from '../../../../../zqlite/src/db.ts';

/**
 * The oracle's layer-3 comparator over two replica files: normalized logical
 * content, diffed table by table. Both materializations claim to be the
 * deterministic image of the same stream at the same commit watermark, so
 * everything logical — row contents, row counts, table schemas — must match;
 * only physical artifacts (rowids, page layout, WAL state) may differ, which
 * is why rows are digested order-independently rather than dumped in file
 * order.
 */

/**
 * Tables excluded from comparison by default: bookkeeping whose content
 * depends on runtime history (checkpointing, pruning) or wall-clock
 * timestamps rather than on the replicated stream. The replica identity and
 * watermark that `_zero.replicationConfig` / `_zero.replicationState` carry
 * are compared separately (and more precisely) by the drill's alignment
 * check.
 */
export const DEFAULT_EXCLUDED_TABLES: readonly string[] = [
  '_zero.changeLog',
  '_zero.changeLog2',
  '_zero.replicationConfig',
  '_zero.replicationState',
  '_zero.runtimeEvents',
  '_zero.versionHistory',
];

export type TableDigest = {
  rows: number;
  /** Order-independent 128-bit sum of per-row content hashes, as hex. */
  digest: string;
};

export type TableMismatch =
  /** The table exists in the reference but not in the restored replica. */
  | {table: string; kind: 'missing-table'}
  /** The table exists in the restored replica but not in the reference. */
  | {table: string; kind: 'extra-table'}
  /** The column signatures differ; rows were not compared. */
  | {table: string; kind: 'schema'; reference: string; restored: string}
  /** The logical row contents differ. */
  | {
      table: string;
      kind: 'rows';
      reference: TableDigest;
      restored: TableDigest;
    };

export type ReplicaComparison = {
  /** Tables whose rows were compared (shared, schema-matched, not excluded). */
  tables: number;
  /** Rows digested in the reference replica. */
  rows: number;
  mismatches: TableMismatch[];
};

export type CompareReplicasOptions = {
  /** Defaults to {@link DEFAULT_EXCLUDED_TABLES}. */
  excludeTables?: readonly string[] | undefined;
};

/**
 * Compares the logical content of two replica databases. The caller owns the
 * handles (and any pinning read transaction on them); this function only
 * reads.
 */
export function compareReplicas(
  lc: LogContext,
  reference: Database,
  restored: Database,
  options: CompareReplicasOptions = {},
): ReplicaComparison {
  const excluded = new Set(options.excludeTables ?? DEFAULT_EXCLUDED_TABLES);
  const refTables = listTables(reference).filter(t => !excluded.has(t));
  const restoredTables = new Set(
    listTables(restored).filter(t => !excluded.has(t)),
  );

  const mismatches: TableMismatch[] = [];
  let tables = 0;
  let rows = 0;

  for (const table of refTables) {
    if (!restoredTables.delete(table)) {
      mismatches.push({table, kind: 'missing-table'});
      continue;
    }
    const refSchema = tableSignature(reference, table);
    const restoredSchema = tableSignature(restored, table);
    if (refSchema !== restoredSchema) {
      mismatches.push({
        table,
        kind: 'schema',
        reference: refSchema,
        restored: restoredSchema,
      });
      continue;
    }
    const columns = tableColumns(reference, table);
    const refDigest = tableDigest(reference, table, columns);
    const restoredDigest = tableDigest(restored, table, columns);
    tables++;
    rows += refDigest.rows;
    if (
      refDigest.rows !== restoredDigest.rows ||
      refDigest.digest !== restoredDigest.digest
    ) {
      mismatches.push({
        table,
        kind: 'rows',
        reference: refDigest,
        restored: restoredDigest,
      });
      lc.warn?.(
        // Log only the digests: row payloads contain customer data.
        `table ${table} diverged: reference ${refDigest.rows} rows ` +
          `(${refDigest.digest}) vs restored ${restoredDigest.rows} rows ` +
          `(${restoredDigest.digest})`,
      );
    }
  }
  for (const table of restoredTables) {
    mismatches.push({table, kind: 'extra-table'});
  }
  return {tables, rows, mismatches};
}

function listTables(db: Database): string[] {
  return db
    .prepare(
      /*sql*/ `SELECT name FROM sqlite_master ` +
        `WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name`,
    )
    .all<{name: string}>()
    .map(({name}) => name);
}

function quoted(name: string): string {
  return `"${name.replaceAll('"', '""')}"`;
}

type ColumnInfo = {
  name: string;
  type: string;
  notnull: number;
  ['dflt_value']: string | null;
  pk: number;
};

function columnInfo(db: Database, table: string): ColumnInfo[] {
  return db.pragma<ColumnInfo>(`table_info(${quoted(table)})`);
}

/** A canonical string of the table's column definitions. */
export function tableSignature(db: Database, table: string): string {
  return JSON.stringify(
    columnInfo(db, table).map(
      ({name, type, notnull, ['dflt_value']: dflt, pk}) => ({
        name,
        type,
        notnull,
        dflt,
        pk,
      }),
    ),
  );
}

function tableColumns(db: Database, table: string): string[] {
  return columnInfo(db, table).map(({name}) => name);
}

const DIGEST_MOD = 1n << 128n;

/**
 * Digests a table's rows into a count and an order-independent 128-bit sum
 * of per-row SHA-256 content hashes (values serialized with type tags and
 * length prefixes, in declared column order). Order independence is what
 * makes two materializations with different physical row order — different
 * rowids, vacuum history, apply batching — comparable.
 */
export function tableDigest(
  db: Database,
  table: string,
  columns: readonly string[],
): TableDigest {
  const stmt = db
    .prepare(
      /*sql*/ `SELECT ${columns.map(quoted).join(',')} FROM ${quoted(table)}`,
    )
    .safeIntegers(true);
  let rows = 0;
  let sum = 0n;
  for (const row of stmt.iterate<Record<string, unknown>>()) {
    const hash = createHash('sha256');
    for (const column of columns) {
      hash.update(serializeValue(row[column]));
    }
    sum = (sum + BigInt(`0x${hash.digest('hex').slice(0, 32)}`)) % DIGEST_MOD;
    rows++;
  }
  return {rows, digest: sum.toString(16).padStart(32, '0')};
}

function serializeValue(value: unknown): Buffer {
  if (value === null) {
    return Buffer.from('n;');
  }
  switch (typeof value) {
    case 'bigint':
      return Buffer.from(`i:${value};`);
    case 'number':
      // With safeIntegers, a number is always a SQLite REAL. String() is the
      // shortest round-trip representation, so equal doubles serialize
      // equally.
      return Buffer.from(`f:${value};`);
    case 'string': {
      const bytes = Buffer.from(value, 'utf8');
      return Buffer.concat([Buffer.from(`s:${bytes.length}:`), bytes]);
    }
    default: {
      if (value instanceof Uint8Array) {
        return Buffer.concat([Buffer.from(`b:${value.length}:`), value]);
      }
      throw new Error(`unexpected SQLite value of type ${typeof value}`);
    }
  }
}
