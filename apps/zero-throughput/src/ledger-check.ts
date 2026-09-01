import '../../../packages/shared/src/dotenv.ts';

import {DatabaseSync} from 'node:sqlite';
import {parseOptions} from '../../../packages/shared/src/options.ts';
import * as v from '../../../packages/shared/src/valita.ts';
import {
  addHash,
  LEDGER_TABLE,
  LEDGERED_TABLES,
  rowHash,
  sortedColumns,
  type LedgeredTable,
} from './ledger.ts';

/**
 * The ledger self-consistency checker (oracle layer 2's judge): runnable
 * against any replica at any commit watermark. Every ledger row the replica
 * carries was committed atomically with the data it describes, so the
 * counts and hashes recomputed over the replica's own rows must match
 * exactly — a torn transaction, skipped envelope, or mis-ordered apply
 * cannot match.
 *
 *     node src/ledger-check.ts --replica /path/to/replica.db
 */

export type TableCheck = {
  readonly table: string;
  readonly outcome: 'match' | 'mismatch' | 'absent';
  readonly ledger?: {rows: number; hash: string} | undefined;
  readonly computed?: {rows: number; hash: string} | undefined;
};

export type LedgerCheckResult = {
  readonly outcome: 'match' | 'mismatch' | 'no-ledger';
  readonly tables: readonly TableCheck[];
};

/**
 * Recomputes each ledgered table's aggregates over the replica's rows and
 * compares them with the ledger rows the replica carries. A workload table
 * that is absent from the replica (e.g. a profile that never ran) must be
 * absent from the ledger too.
 */
export function checkLedger(db: DatabaseSync): LedgerCheckResult {
  const ledger = readLedger(db);
  if (ledger === undefined) {
    return {outcome: 'no-ledger', tables: []};
  }
  const tables: TableCheck[] = [];
  for (const table of LEDGERED_TABLES) {
    const expected = ledger.get(table.name);
    const computed = tableExists(db, table.name)
      ? computeAggregates(db, table)
      : undefined;
    if (expected === undefined && computed === undefined) {
      continue;
    }
    if (expected === undefined || computed === undefined) {
      tables.push({
        table: table.name,
        outcome: 'absent',
        ledger: expected,
        computed,
      });
      continue;
    }
    tables.push({
      table: table.name,
      outcome:
        expected.rows === computed.rows && expected.hash === computed.hash
          ? 'match'
          : 'mismatch',
      ledger: expected,
      computed,
    });
  }
  const outcome = tables.every(t => t.outcome === 'match')
    ? 'match'
    : 'mismatch';
  return {outcome, tables};
}

function tableExists(db: DatabaseSync, name: string): boolean {
  const stmt = db.prepare(
    /*sql*/ `SELECT 1 FROM sqlite_master WHERE type='table' AND name = ?`,
  );
  return stmt.get(name) !== undefined;
}

function readLedger(
  db: DatabaseSync,
): Map<string, {rows: number; hash: string}> | undefined {
  if (!tableExists(db, LEDGER_TABLE)) {
    return undefined;
  }
  const stmt = db.prepare(
    /*sql*/ `SELECT table_name, row_count, content_hash FROM "${LEDGER_TABLE}"`,
  );
  stmt.setReadBigInts(true);
  const ledger = new Map<string, {rows: number; hash: string}>();
  for (const row of stmt.all() as {
    ['table_name']: string;
    ['row_count']: bigint;
    ['content_hash']: string;
  }[]) {
    ledger.set(row['table_name'], {
      rows: Number(row['row_count']),
      hash: BigInt(row['content_hash']).toString(),
    });
  }
  return ledger;
}

function computeAggregates(
  db: DatabaseSync,
  table: LedgeredTable,
): {rows: number; hash: string} {
  const columns = sortedColumns(table);
  const stmt = db.prepare(
    /*sql*/ `SELECT ${columns.map(c => `"${c}"`).join(',')} FROM "${table.name}"`,
  );
  stmt.setReadBigInts(true);
  let rows = 0;
  let sum = 0n;
  for (const row of stmt.iterate() as Iterable<Record<string, unknown>>) {
    sum = addHash(sum, rowHash(table, row));
    rows++;
  }
  return {rows, hash: sum.toString()};
}

function main() {
  const {replica} = parseOptions(
    {
      replica: v.string(),
    },
    {envNamePrefix: 'ZERO_THROUGHPUT_'},
  );
  const db = new DatabaseSync(replica, {readOnly: true});
  try {
    const result = checkLedger(db);
    // oxlint-disable-next-line no-console -- the report is the CLI's output
    console.log(JSON.stringify(result, null, 2));
    process.exitCode = result.outcome === 'match' ? 0 : 1;
  } finally {
    db.close();
  }
}

if (process.argv[1] === import.meta.filename) {
  main();
}
