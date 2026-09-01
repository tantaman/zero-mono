import {createHash} from 'node:crypto';

/**
 * The ledger oracle (layer 2): inside every Postgres transaction, triggers
 * maintain one ledger row per workload table — a row count and an
 * order-independent content hash. Postgres cannot be queried "as of" an old
 * commit, but any replica at any commit watermark must be self-consistent:
 * the ledger rows it carries must match aggregates recomputed over its own
 * data. A torn transaction, a skipped envelope, or a mis-ordered apply shows
 * up at the exact boundary it happened.
 *
 * The hash must be computable identically by Postgres (in the trigger, over
 * the row being written) and by the checker (in JS, over the replicated
 * SQLite values), so it covers exactly the columns whose pg→lite mapping is
 * canonical — text, integer/bigint, boolean — and skips wall-clock
 * timestamps and jsonb payloads. Every logical write in this workload bumps
 * a covered column (`seq`), so a stale row version still changes the hash.
 *
 * Canonical row text: `<table>` then `|<col>=<enc>` for each covered column
 * in sorted order, where enc is `n` for NULL, `s<octets>:<text>` for text,
 * `i:<decimal>` for integers, and `b:0|1` for booleans. The row hash is the
 * first 14 hex digits (56 bits, so the Postgres bigint cast stays positive)
 * of md5(canonical); the table hash is the sum of row hashes mod 2^64.
 */

export const LEDGER_TABLE = 'zero_throughput_ledger';

export type LedgerColumnType = 'text' | 'int' | 'bool';

export type LedgeredTable = {
  readonly name: string;
  /** The covered columns and their types. Hashed in sorted column order. */
  readonly columns: Readonly<Record<string, LedgerColumnType>>;
};

export const LEDGERED_TABLES: readonly LedgeredTable[] = [
  {
    name: 'zero_throughput_event',
    columns: {
      id: 'text',
      profile: 'text',
      shard: 'int',
      bucket: 'int',
      seq: 'int',
    },
  },
  {
    name: 'zero_throughput_email_thread',
    columns: {
      id: 'text',
      ['owner_id']: 'text',
      mailbox: 'text',
      subject: 'text',
      ['participant_count']: 'int',
      seq: 'int',
    },
  },
  {
    name: 'zero_throughput_email_message',
    columns: {
      id: 'text',
      ['thread_id']: 'text',
      ['owner_id']: 'text',
      mailbox: 'text',
      ['sender_id']: 'text',
      unread: 'bool',
      body: 'text',
      seq: 'int',
    },
  },
  {
    name: 'zero_throughput_forum_user',
    columns: {id: 'text', name: 'text'},
  },
  {
    name: 'zero_throughput_forum_category',
    columns: {id: 'text', slug: 'text', title: 'text', seq: 'int'},
  },
  {
    name: 'zero_throughput_forum_thread',
    columns: {
      id: 'text',
      ['category_id']: 'text',
      ['author_id']: 'text',
      title: 'text',
      pinned: 'bool',
      seq: 'int',
    },
  },
  {
    name: 'zero_throughput_forum_post',
    columns: {
      id: 'text',
      ['thread_id']: 'text',
      ['category_id']: 'text',
      ['author_id']: 'text',
      body: 'text',
      seq: 'int',
    },
  },
  {
    name: 'zero_throughput_rel_org',
    columns: {id: 'text', name: 'text', region: 'text', seq: 'int'},
  },
  {
    name: 'zero_throughput_rel_account',
    columns: {
      id: 'text',
      ['org_id']: 'text',
      ['owner_id']: 'text',
      name: 'text',
      status: 'text',
      seq: 'int',
    },
  },
  {
    name: 'zero_throughput_rel_contact',
    columns: {
      id: 'text',
      ['account_id']: 'text',
      name: 'text',
      role: 'text',
      seq: 'int',
    },
  },
  {
    name: 'zero_throughput_rel_activity',
    columns: {
      id: 'text',
      ['org_id']: 'text',
      ['account_id']: 'text',
      ['contact_id']: 'text',
      kind: 'text',
      body: 'text',
      seq: 'int',
    },
  },
];

export const HASH_MOD = 1n << 64n;

export function sortedColumns(table: LedgeredTable): string[] {
  return Object.keys(table.columns).toSorted();
}

/**
 * The JS side of the canonical encoding. Values may come from the driver
 * (string/number/boolean) or from a SQLite replica read with bigints
 * (string/bigint, booleans as 0n/1n).
 */
export function encodeValue(type: LedgerColumnType, value: unknown): string {
  if (value === null || value === undefined) {
    return 'n';
  }
  switch (type) {
    case 'text': {
      const text = value as string;
      return `s${Buffer.byteLength(text, 'utf8')}:${text}`;
    }
    case 'int':
      return `i:${value}`;
    case 'bool':
      return `b:${value === true || value === 1n || value === 1 ? 1 : 0}`;
  }
}

export function canonicalRow(
  table: LedgeredTable,
  values: Readonly<Record<string, unknown>>,
): string {
  return (
    table.name +
    sortedColumns(table)
      .map(col => `|${col}=${encodeValue(table.columns[col], values[col])}`)
      .join('')
  );
}

/** The 56-bit row hash: md5 of the canonical text, first 14 hex digits. */
export function rowHash(
  table: LedgeredTable,
  values: Readonly<Record<string, unknown>>,
): bigint {
  const hex = createHash('md5')
    .update(canonicalRow(table, values), 'utf8')
    .digest('hex');
  return BigInt(`0x${hex.slice(0, 14)}`);
}

export function addHash(sum: bigint, hash: bigint): bigint {
  return (((sum + hash) % HASH_MOD) + HASH_MOD) % HASH_MOD;
}

// ---------------------------------------------------------------------------
// Postgres side: the trigger machinery that maintains the ledger inside each
// transaction. Generated from the same table specs as the JS encoding above,
// so the two sides cannot drift independently.
// ---------------------------------------------------------------------------

const MOD_SQL = '18446744073709551616::numeric'; // 2^64

/** The canonical-row expression over a trigger row variable (NEW/OLD). */
function canonicalRowSQL(table: LedgeredTable, row: 'NEW' | 'OLD'): string {
  const parts = sortedColumns(table).map(col => {
    const ref = `${row}.${col}`;
    let enc: string;
    switch (table.columns[col]) {
      case 'text':
        enc = `'s' || octet_length(${ref}) || ':' || ${ref}`;
        break;
      case 'int':
        enc = `'i:' || ${ref}::text`;
        break;
      case 'bool':
        enc = `'b:' || (CASE WHEN ${ref} THEN '1' ELSE '0' END)`;
        break;
    }
    return `'|${col}=' || (CASE WHEN ${ref} IS NULL THEN 'n' ELSE ${enc} END)`;
  });
  return [`'${table.name}'`, ...parts].join(' || ');
}

/**
 * The DDL that installs the ledger: the ledger table, the row-hash and
 * apply helpers, and one AFTER ROW trigger per workload table. Everything is
 * `CREATE OR REPLACE`/`IF NOT EXISTS` so installation is idempotent. The
 * ledger table must be part of the zero publication (it lives in `public`
 * beside the workload tables), which is what makes its rows land in every
 * replica atomically with the data they describe.
 *
 * `content_hash` is stored as text: its value fits neither bigint (unsigned
 * 64-bit) nor a float, and text replicates to SQLite byte-identically.
 */
export function ledgerSchemaSQL(): string[] {
  const statements: string[] = [
    /*sql*/ `
    CREATE TABLE IF NOT EXISTS ${LEDGER_TABLE} (
      table_name text PRIMARY KEY,
      row_count bigint NOT NULL,
      content_hash text NOT NULL
    )`,
    /*sql*/ `
    CREATE OR REPLACE FUNCTION zero_throughput_row_hash(canonical text)
    RETURNS numeric LANGUAGE sql IMMUTABLE AS $fn$
      SELECT ('x' || substr(md5(canonical), 1, 14))::bit(56)::bigint::numeric
    $fn$`,
    /*sql*/ `
    CREATE OR REPLACE FUNCTION zero_throughput_ledger_apply(
      tbl text, dcount int, dhash numeric)
    RETURNS void LANGUAGE sql AS $fn$
      INSERT INTO ${LEDGER_TABLE} (table_name, row_count, content_hash)
      VALUES (
        tbl,
        dcount,
        mod(mod(dhash, ${MOD_SQL}) + ${MOD_SQL}, ${MOD_SQL})::text
      )
      ON CONFLICT (table_name) DO UPDATE SET
        row_count = ${LEDGER_TABLE}.row_count + excluded.row_count,
        content_hash = mod(
          mod(${LEDGER_TABLE}.content_hash::numeric + dhash, ${MOD_SQL})
            + ${MOD_SQL},
          ${MOD_SQL})::text
    $fn$`,
  ];
  for (const table of LEDGERED_TABLES) {
    statements.push(
      /*sql*/ `
      CREATE OR REPLACE FUNCTION zero_throughput_ledger_tg_${table.name}()
      RETURNS trigger LANGUAGE plpgsql AS $fn$
      BEGIN
        IF TG_OP = 'INSERT' THEN
          PERFORM zero_throughput_ledger_apply('${table.name}', 1,
            zero_throughput_row_hash(${canonicalRowSQL(table, 'NEW')}));
        ELSIF TG_OP = 'UPDATE' THEN
          PERFORM zero_throughput_ledger_apply('${table.name}', 0,
            zero_throughput_row_hash(${canonicalRowSQL(table, 'NEW')})
              - zero_throughput_row_hash(${canonicalRowSQL(table, 'OLD')}));
        ELSE
          PERFORM zero_throughput_ledger_apply('${table.name}', -1,
            - zero_throughput_row_hash(${canonicalRowSQL(table, 'OLD')}));
        END IF;
        RETURN NULL;
      END $fn$`,
      /*sql*/ `
      DROP TRIGGER IF EXISTS zero_throughput_ledger_tg ON ${table.name}`,
      /*sql*/ `
      CREATE TRIGGER zero_throughput_ledger_tg
      AFTER INSERT OR UPDATE OR DELETE ON ${table.name}
      FOR EACH ROW EXECUTE FUNCTION zero_throughput_ledger_tg_${table.name}()`,
    );
  }
  return statements;
}

export function dropLedgerSQL(): string[] {
  return [/*sql*/ `DROP TABLE IF EXISTS ${LEDGER_TABLE} CASCADE`];
}
