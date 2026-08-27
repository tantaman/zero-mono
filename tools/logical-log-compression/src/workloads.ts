/**
 * Workload definitions: the insert/update/delete schemes whose change streams
 * we compress.
 *
 * Rows are built by recombining column values sampled from the real dataset,
 * so value distributions (string lengths, id cardinality, numeric ranges) match
 * production rather than a synthetic guess. Text-heavy columns are filled from
 * {@link TextSource} so payloads do not repeat within a chunk.
 */
export type {Sql} from './capture.ts';
import type {Sql} from './capture.ts';

export type DatasetName = 'chinook' | 'zbugs' | 'pagila';

export type Workload = {
  name: string;
  dataset: DatasetName;
  /** Short human description of the mutation scheme. */
  describe: string;
  /** Rows mutated per transaction. */
  txnSize: number;
  /** Runs before capture starts (schema tweaks, seeding). */
  prepare?: (sql: Sql) => Promise<void>;
  /** Emits one transaction. Returns rows mutated. */
  step: (sql: Sql, n: number) => Promise<number>;
  /** Runs after capture ends (undo schema tweaks). Changes are not measured. */
  restore?: (sql: Sql) => Promise<void>;
};

// ---------------------------------------------------------------------------
// Value sampling
// ---------------------------------------------------------------------------

/**
 * Samples real column values and recombines them into novel rows.
 *
 * Each column advances through the sampled values at its own co-prime stride,
 * so generated rows are new combinations of real values rather than copies of
 * real rows -- which would inflate compression ratios.
 */
export class Sampler {
  readonly #values: Record<string, unknown[]>;
  static readonly #strides = [1, 3, 7, 11, 13, 17, 19, 23, 29, 31, 37, 41, 43];

  constructor(values: Record<string, unknown[]>) {
    this.#values = values;
  }

  static async load(
    sql: Sql,
    table: string,
    columns: readonly string[],
    limit = 20000,
  ): Promise<Sampler> {
    const rows = await sql`
      SELECT ${sql(columns as string[])} FROM ${sql(table)} LIMIT ${limit}`;
    const values: Record<string, unknown[]> = {};
    for (const c of columns) {
      values[c] = rows.map(r => (r as Record<string, unknown>)[c]);
    }
    return new Sampler(values);
  }

  pick(column: string, i: number): unknown {
    const vals = this.#values[column];
    const idx =
      Sampler.#strides[
        Object.keys(this.#values).indexOf(column) % Sampler.#strides.length
      ];
    return vals[(i * idx) % vals.length];
  }

  get size(): number {
    return Object.values(this.#values)[0]?.length ?? 0;
  }
}

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

/** Base for synthetic primary keys, far above any real id. */
export const PK_BASE = 100_000_000;

function quote(id: string): string {
  return `"${id.replaceAll('"', '""')}"`;
}

/** Drops all foreign keys so generated rows need not satisfy referential order. */
export async function dropForeignKeys(sql: Sql): Promise<void> {
  const fks = await sql<{table: string; name: string}[]>`
    SELECT c.conrelid::regclass::text AS table, c.conname AS name
      FROM pg_constraint c
      JOIN pg_namespace n ON n.oid = c.connamespace
     WHERE c.contype = 'f' AND n.nspname = 'public'`;
  for (const {table, name} of fks) {
    await sql.unsafe(`ALTER TABLE ${table} DROP CONSTRAINT ${quote(name)}`);
  }
}

export async function setReplicaIdentity(
  sql: Sql,
  table: string,
  mode: 'DEFAULT' | 'FULL',
): Promise<void> {
  await sql.unsafe(`ALTER TABLE ${quote(table)} REPLICA IDENTITY ${mode}`);
}
