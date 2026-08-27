/**
 * Per-dataset workload definitions for chinook, zbugs and pagila.
 */
import {randomText, TextSource} from './corpus.ts';
import {
  dropForeignKeys,
  PK_BASE,
  Sampler,
  setReplicaIdentity,
  type DatasetName,
  type Sql,
  type Workload,
} from './workloads.ts';

const q = (id: string) => `"${id.replaceAll('"', '""')}"`;

async function bulkInsert(
  sql: Sql,
  table: string,
  rows: Record<string, unknown>[],
) {
  await sql`INSERT INTO ${sql(table)} ${sql(rows)}`;
  return rows.length;
}

async function bulkUpdate(
  sql: Sql,
  table: string,
  pk: readonly string[],
  rows: Record<string, unknown>[],
  types: Record<string, string>,
) {
  if (!rows.length) return 0;
  const cols = Object.keys(rows[0]);
  const set = cols.filter(c => !pk.includes(c));
  const flat: unknown[] = [];
  const tuples = rows.map((r, ri) => {
    const parts = cols.map((c, ci) => {
      flat.push(r[c]);
      const t = types[c] ? `::${types[c]}` : '';
      return `$${ri * cols.length + ci + 1}${ri === 0 ? t : ''}`;
    });
    return `(${parts.join(',')})`;
  });
  await sql.unsafe(
    `UPDATE ${q(table)} AS t SET ${set.map(c => `${q(c)} = v.${q(c)}`).join(', ')}
       FROM (VALUES ${tuples.join(',')}) AS v(${cols.map(q).join(',')})
      WHERE ${pk.map(c => `t.${q(c)} = v.${q(c)}`).join(' AND ')}`,
    flat as never[],
  );
  return rows.length;
}

async function bulkDelete(sql: Sql, table: string, pk: string, ids: unknown[]) {
  if (!ids.length) return 0;
  await sql.unsafe(`DELETE FROM ${q(table)} WHERE ${q(pk)} = ANY($1)`, [
    ids,
  ] as never[]);
  return ids.length;
}

/**
 * Seeds `count` rows starting at `firstId` if they are not already present.
 * Update and delete workloads address these rows by primary key, so the pool
 * has to exist regardless of which workloads ran before.
 */
async function ensurePool(
  sql: Sql,
  table: string,
  pk: string,
  firstId: number,
  count: number,
  make: (i: number) => Record<string, unknown>,
): Promise<void> {
  const [{n}] = await sql.unsafe<{n: number}[]>(
    `SELECT count(*)::int AS n FROM ${q(table)}
      WHERE ${q(pk)} >= $1 AND ${q(pk)} < $2`,
    [firstId, firstId + count] as never[],
  );
  if (n >= count) return;
  await seedPool(sql, table, count - n, i => make(n + i));
}

/** Inserts a disjoint pool of synthetic rows for update/delete workloads. */
async function seedPool(
  sql: Sql,
  table: string,
  count: number,
  make: (i: number) => Record<string, unknown>,
  batch = 500,
) {
  for (let i = 0; i < count; i += batch) {
    const rows: Record<string, unknown>[] = [];
    for (let j = i; j < Math.min(i + batch, count); j++) rows.push(make(j));
    await bulkInsert(sql, table, rows);
  }
}

// ===========================================================================
// chinook
// ===========================================================================

const TRACK_COLS = [
  'name',
  'album_id',
  'media_type_id',
  'genre_id',
  'composer',
  'milliseconds',
  'bytes',
  'unit_price',
] as const;

async function chinookWorkloads(sql: Sql): Promise<Workload[]> {
  await dropForeignKeys(sql);
  const track = await Sampler.load(sql, 'track', TRACK_COLS);
  const plist = await Sampler.load(sql, 'playlist_track', ['playlist_id']);
  const iline = await Sampler.load(sql, 'invoice_line', [
    'invoice_id',
    'track_id',
    'unit_price',
    'quantity',
  ]);

  const mkTrack = (i: number) => ({
    track_id: PK_BASE + i,
    name: track.pick('name', i),
    album_id: track.pick('album_id', i),
    media_type_id: track.pick('media_type_id', i),
    genre_id: track.pick('genre_id', i),
    composer: track.pick('composer', i),
    milliseconds: track.pick('milliseconds', i),
    bytes: track.pick('bytes', i),
    unit_price: track.pick('unit_price', i),
  });

  const d: DatasetName = 'chinook';
  const POOL = 90_000;
  const UPD_BASE = 10_000_000;
  const DEL_BASE = 30_000_000;
  /** The actual primary key of pool row `i`; mkTrack() offsets by PK_BASE. */
  const trackId = (i: number) => PK_BASE + i;
  const ensureTracks = (s: Sql, base: number) =>
    ensurePool(s, 'track', 'track_id', trackId(base), POOL, i =>
      mkTrack(base + i),
    );

  return [
    {
      name: 'insert-track-batch100',
      dataset: d,
      describe: 'INSERT into a 9-column catalog table, 100 rows/txn',
      txnSize: 100,
      step: (s, n) =>
        bulkInsert(
          s,
          'track',
          Array.from({length: 100}, (_, k) => mkTrack(2_000_000 + n * 100 + k)),
        ),
    },
    {
      name: 'insert-track-single',
      dataset: d,
      describe: 'INSERT into a 9-column catalog table, 1 row/txn',
      txnSize: 1,
      step: (s, n) => bulkInsert(s, 'track', [mkTrack(4_000_000 + n)]),
    },
    {
      name: 'insert-playlisttrack-batch100',
      dataset: d,
      describe: 'INSERT into a 2-column join table (narrowest row), 100/txn',
      txnSize: 100,
      step: (s, n) =>
        bulkInsert(
          s,
          'playlist_track',
          Array.from({length: 100}, (_, k) => ({
            playlist_id: plist.pick('playlist_id', n * 100 + k),
            track_id: PK_BASE + 6_000_000 + n * 100 + k,
          })),
        ),
    },
    {
      name: 'insert-invoiceline-batch100',
      dataset: d,
      describe: 'INSERT into a 5-column numeric line-item table, 100/txn',
      txnSize: 100,
      step: (s, n) =>
        bulkInsert(
          s,
          'invoice_line',
          Array.from({length: 100}, (_, k) => {
            const i = n * 100 + k;
            return {
              invoice_line_id: PK_BASE + i,
              invoice_id: iline.pick('invoice_id', i),
              track_id: iline.pick('track_id', i),
              unit_price: iline.pick('unit_price', i),
              quantity: iline.pick('quantity', i),
            };
          }),
        ),
    },
    {
      name: 'update-track-1col-batch100',
      dataset: d,
      describe: 'UPDATE one column of a 9-column row, 100 rows/txn',
      txnSize: 100,
      prepare: s => ensureTracks(s, UPD_BASE),
      step: (s, n) =>
        bulkUpdate(
          s,
          'track',
          ['track_id'],
          Array.from({length: 100}, (_, k) => ({
            track_id: trackId(UPD_BASE + ((n * 100 + k) % POOL)),
            unit_price: 0.99 + ((n * 100 + k) % 50) / 100,
          })),
          {track_id: 'int4', unit_price: 'numeric'},
        ),
    },
    {
      name: 'update-track-allcols-batch100',
      dataset: d,
      describe: 'UPDATE every column of a 9-column row, 100 rows/txn',
      txnSize: 100,
      prepare: s => ensureTracks(s, UPD_BASE),
      step: (s, n) =>
        bulkUpdate(
          s,
          'track',
          ['track_id'],
          Array.from({length: 100}, (_, k) => {
            const i = n * 100 + k;
            const r = mkTrack(POOL + i);
            return {...r, track_id: trackId(UPD_BASE + (i % POOL))};
          }),
          {
            track_id: 'int4',
            name: 'varchar',
            album_id: 'int4',
            media_type_id: 'int4',
            genre_id: 'int4',
            composer: 'varchar',
            milliseconds: 'int4',
            bytes: 'int4',
            unit_price: 'numeric',
          },
        ),
    },
    {
      name: 'update-track-1col-replicaidentity-full',
      dataset: d,
      describe:
        'UPDATE one column with REPLICA IDENTITY FULL (ships old row too)',
      txnSize: 100,
      prepare: async s => {
        await ensureTracks(s, UPD_BASE);
        await setReplicaIdentity(s, 'track', 'FULL');
      },
      restore: s => setReplicaIdentity(s, 'track', 'DEFAULT'),
      step: (s, n) =>
        bulkUpdate(
          s,
          'track',
          ['track_id'],
          Array.from({length: 100}, (_, k) => ({
            track_id: trackId(UPD_BASE + ((n * 100 + k) % POOL)),
            milliseconds: 100_000 + ((n * 100 + k) % 90_000),
          })),
          {track_id: 'int4', milliseconds: 'int4'},
        ),
    },
    {
      name: 'delete-track-batch100',
      dataset: d,
      describe: 'DELETE by primary key, 100 rows/txn',
      txnSize: 100,
      prepare: s => ensureTracks(s, DEL_BASE),
      step: (s, n) =>
        bulkDelete(
          s,
          'track',
          'track_id',
          Array.from({length: 100}, (_, k) => trackId(DEL_BASE + n * 100 + k)),
        ),
    },
    {
      name: 'hot-row-churn',
      dataset: d,
      describe: 'Repeatedly UPDATE the same 200 rows (cache-like churn)',
      txnSize: 100,
      prepare: s => ensureTracks(s, UPD_BASE),
      step: (s, n) =>
        bulkUpdate(
          s,
          'track',
          ['track_id'],
          Array.from({length: 100}, (_, k) => ({
            track_id: trackId(UPD_BASE + ((n * 100 + k) % 200)),
            milliseconds: 200_000 + (n % 1000),
          })),
          {track_id: 'int4', milliseconds: 'int4'},
        ),
    },
  ];
}

// ===========================================================================
// zbugs
// ===========================================================================

async function zbugsWorkloads(sql: Sql): Promise<Workload[]> {
  await dropForeignKeys(sql);
  const users = (await sql<{id: string}[]>`SELECT id FROM "user"`).map(
    r => r.id,
  );
  const issues = (
    await sql<{id: string}[]>`SELECT id FROM issue LIMIT 5000`
  ).map(r => r.id);
  const project = (await sql<{id: string}[]>`SELECT id FROM project LIMIT 1`)[0]
    .id;
  // Real length distributions, so synthetic rows match production row sizes.
  const bodyLens = (
    await sql<{n: number}[]>`SELECT length(body) AS n FROM comment`
  ).map(r => r.n);
  const descLens = (
    await sql<{n: number}[]>`SELECT length(description) AS n FROM issue`
  ).map(r => r.n);
  const titleLens = (
    await sql<{n: number}[]>`SELECT length(title) AS n FROM issue`
  ).map(r => r.n);

  const d: DatasetName = 'zbugs';
  const POOL = 30_000;
  const DELETE_POOL = 80_000;
  let text = new TextSource();
  const resetText = () => {
    text = new TextSource();
  };

  const mkComment = (i: number, gen: (n: number) => string) => ({
    id: `llc-c-${i}`,
    issueID: issues[i % issues.length],
    created: 1700000000000 + i * 997,
    body: gen(bodyLens[i % bodyLens.length]),
    creatorID: users[i % users.length],
  });
  const mkIssue = (i: number, gen: (n: number) => string) => ({
    id: `llc-i-${i}`,
    title: gen(Math.min(120, titleLens[i % titleLens.length])),
    open: i % 3 !== 0,
    modified: 1700000000000 + i * 991,
    created: 1690000000000 + i * 983,
    creatorID: users[i % users.length],
    assigneeID: users[(i * 3) % users.length],
    description: gen(Math.min(10_000, descLens[i % descLens.length])),
    visibility: 'public',
    projectID: project,
  });

  const corpusGen = (n: number) => text.next(n);

  return [
    {
      name: 'insert-comment-batch50',
      dataset: d,
      describe:
        'INSERT comments with real-length markdown bodies (mean 430B), 50/txn',
      txnSize: 50,
      prepare: () => {
        resetText();
        return Promise.resolve();
      },
      step: (s, n) =>
        bulkInsert(
          s,
          'comment',
          Array.from({length: 50}, (_, k) =>
            mkComment(2_000_000 + n * 50 + k, corpusGen),
          ),
        ),
    },
    {
      name: 'insert-comment-highentropy',
      dataset: d,
      describe: 'Same, but bodies are incompressible (pessimistic bound)',
      txnSize: 50,
      step: (s, n) =>
        bulkInsert(
          s,
          'comment',
          Array.from({length: 50}, (_, k) =>
            mkComment(3_000_000 + n * 50 + k, randomText),
          ),
        ),
    },
    {
      name: 'insert-issue-batch50',
      dataset: d,
      describe: 'INSERT issues (title + real-length markdown body), 50/txn',
      txnSize: 50,
      prepare: () => {
        resetText();
        return Promise.resolve();
      },
      step: (s, n) =>
        bulkInsert(
          s,
          'issue',
          Array.from({length: 50}, (_, k) =>
            mkIssue(2_000_000 + n * 50 + k, corpusGen),
          ),
        ),
    },
    {
      name: 'insert-viewstate-batch100',
      dataset: d,
      describe: 'INSERT into a 3-column, high-frequency table, 100/txn',
      txnSize: 100,
      step: (s, n) =>
        bulkInsert(
          s,
          'viewState',
          Array.from({length: 100}, (_, k) => {
            const i = n * 100 + k;
            return {
              userID: `llc-u-${i}`,
              issueID: issues[i % issues.length],
              viewed: 1700000000000 + i * 13,
            };
          }),
        ),
    },
    {
      name: 'update-issue-1col-batch50',
      dataset: d,
      describe:
        'UPDATE one column of an issue -- full row (incl. body) is shipped',
      txnSize: 50,
      prepare: async s => {
        resetText();
        await seedPool(
          s,
          'issue',
          POOL,
          i => mkIssue(10_000_000 + i, corpusGen),
          200,
        );
        resetText();
      },
      step: (s, n) =>
        bulkUpdate(
          s,
          'issue',
          ['id'],
          Array.from({length: 50}, (_, k) => ({
            id: `llc-i-${10_000_000 + ((n * 50 + k) % POOL)}`,
            modified: 1800000000000 + n,
          })),
          {id: 'varchar', modified: 'float8'},
        ),
    },
    {
      name: 'delete-comment-batch50',
      dataset: d,
      describe: 'DELETE comments by primary key, 50 rows/txn',
      txnSize: 50,
      prepare: async s => {
        resetText();
        // A delete ships only the row key (~230B), so a 16MiB chunk needs
        // ~70k of them -- the pool has to be sized for that.
        await seedPool(
          s,
          'comment',
          DELETE_POOL,
          i => mkComment(20_000_000 + i, corpusGen),
          500,
        );
      },
      step: (s, n) =>
        bulkDelete(
          s,
          'comment',
          'id',
          Array.from(
            {length: 50},
            (_, k) => `llc-c-${20_000_000 + n * 50 + k}`,
          ),
        ),
    },
    {
      name: 'mixed-oltp-small-txn',
      dataset: d,
      describe:
        'Realistic app traffic: 1 comment insert + issue touch + viewState, per txn',
      txnSize: 3,
      prepare: async s => {
        resetText();
        const [{n}] = await s<{n: number}[]>`
          SELECT count(*)::int AS n FROM issue WHERE id LIKE 'llc-i-1%'`;
        if (n < 5_000) {
          await seedPool(
            s,
            'issue',
            5_000,
            i => mkIssue(10_000_000 + i, corpusGen),
            200,
          );
        }
        resetText();
      },
      step: async (s, n) => {
        await s.begin(async tx => {
          const t = tx as unknown as Sql;
          await bulkInsert(t, 'comment', [
            mkComment(40_000_000 + n, corpusGen),
          ]);
          await bulkUpdate(
            t,
            'issue',
            ['id'],
            [
              {
                id: `llc-i-${10_000_000 + (n % POOL)}`,
                modified: 1900000000000 + n,
              },
            ],
            {id: 'varchar', modified: 'float8'},
          );
          await bulkInsert(t, 'viewState', [
            {
              userID: `llc-v-${n}`,
              issueID: issues[n % issues.length],
              viewed: 1700000000000 + n,
            },
          ]);
        });
        return 3;
      },
    },
  ];
}

// ===========================================================================
// pagila
// ===========================================================================

async function pagilaWorkloads(sql: Sql): Promise<Workload[]> {
  await dropForeignKeys(sql);
  const rental = await Sampler.load(sql, 'rental', [
    'rental_date',
    'inventory_id',
    'customer_id',
    'return_date',
    'staff_id',
    'last_update',
  ]);
  const payment = await Sampler.load(sql, 'payment', [
    'customer_id',
    'staff_id',
    'rental_id',
    'amount',
    'payment_date',
  ]);
  const film = await Sampler.load(sql, 'film', [
    'title',
    'description',
    'release_year',
    'language_id',
    'rental_duration',
    'rental_rate',
    'length',
    'replacement_cost',
    'rating',
    'last_update',
    'special_features',
  ]);

  const d: DatasetName = 'pagila';
  const POOL = 90_000;
  const UPD_BASE = 10_000_000;
  const DEL_BASE = 30_000_000;
  const rentalId = (i: number) => PK_BASE + i;
  const ensureRentals = (s: Sql, base: number) =>
    ensurePool(s, 'rental', 'rental_id', rentalId(base), POOL, i =>
      mkRental(base + i),
    );

  // `rental` has a unique index on (rental_date, inventory_id, customer_id),
  // so the timestamp is derived rather than sampled.
  const RENTAL_EPOCH = 1_640_995_200_000;
  const mkRental = (i: number) => ({
    rental_id: PK_BASE + i,
    rental_date: new Date(RENTAL_EPOCH + i * 1000),
    inventory_id: rental.pick('inventory_id', i),
    customer_id: rental.pick('customer_id', i),
    return_date: rental.pick('return_date', i),
    staff_id: rental.pick('staff_id', i),
    last_update: rental.pick('last_update', i),
  });

  return [
    {
      name: 'insert-rental-batch100',
      dataset: d,
      describe: 'INSERT into a 7-column OLTP table (ids + timestamps), 100/txn',
      txnSize: 100,
      step: (s, n) =>
        bulkInsert(
          s,
          'rental',
          Array.from({length: 100}, (_, k) =>
            mkRental(2_000_000 + n * 100 + k),
          ),
        ),
    },
    {
      name: 'insert-rental-single',
      dataset: d,
      describe: 'INSERT into a 7-column OLTP table, 1 row/txn',
      txnSize: 1,
      step: (s, n) => bulkInsert(s, 'rental', [mkRental(5_000_000 + n)]),
    },
    {
      name: 'insert-payment-batch100',
      dataset: d,
      describe: 'INSERT into a partitioned money table (numeric), 100/txn',
      txnSize: 100,
      step: (s, n) =>
        bulkInsert(
          s,
          'payment',
          Array.from({length: 100}, (_, k) => {
            const i = n * 100 + k;
            return {
              payment_id: PK_BASE + i,
              customer_id: payment.pick('customer_id', i),
              staff_id: payment.pick('staff_id', i),
              rental_id: payment.pick('rental_id', i),
              amount: payment.pick('amount', i),
              payment_date: payment.pick('payment_date', i),
            };
          }),
        ),
    },
    {
      name: 'insert-film-batch20',
      dataset: d,
      describe:
        'INSERT a wide row: text, numeric, enum, array and tsvector, 20/txn',
      txnSize: 20,
      step: (s, n) =>
        bulkInsert(
          s,
          'film',
          Array.from({length: 20}, (_, k) => {
            const i = n * 20 + k;
            return {
              film_id: PK_BASE + i,
              title: `${film.pick('title', i)} ${i}`,
              description: film.pick('description', i),
              release_year: film.pick('release_year', i),
              language_id: film.pick('language_id', i),
              rental_duration: film.pick('rental_duration', i),
              rental_rate: film.pick('rental_rate', i),
              length: film.pick('length', i),
              replacement_cost: film.pick('replacement_cost', i),
              rating: film.pick('rating', i),
              last_update: film.pick('last_update', i),
              special_features: film.pick('special_features', i),
            };
          }),
        ),
    },
    {
      name: 'update-rental-1col-batch100',
      dataset: d,
      describe: 'UPDATE one timestamp column, 100 rows/txn',
      txnSize: 100,
      prepare: s => ensureRentals(s, UPD_BASE),
      step: (s, n) =>
        bulkUpdate(
          s,
          'rental',
          ['rental_id'],
          Array.from({length: 100}, (_, k) => ({
            rental_id: rentalId(UPD_BASE + ((n * 100 + k) % POOL)),
            last_update: new Date(1700000000000 + n * 1000),
          })),
          {rental_id: 'int4', last_update: 'timestamptz'},
        ),
    },
    {
      name: 'update-rental-allcols-batch100',
      dataset: d,
      describe: 'UPDATE every column, 100 rows/txn',
      txnSize: 100,
      prepare: s => ensureRentals(s, UPD_BASE),
      step: (s, n) =>
        bulkUpdate(
          s,
          'rental',
          ['rental_id'],
          Array.from({length: 100}, (_, k) => {
            const i = n * 100 + k;
            const r = mkRental(POOL + i);
            return {...r, rental_id: rentalId(UPD_BASE + (i % POOL))};
          }),
          {
            rental_id: 'int4',
            rental_date: 'timestamptz',
            inventory_id: 'int4',
            customer_id: 'int4',
            return_date: 'timestamptz',
            staff_id: 'int4',
            last_update: 'timestamptz',
          },
        ),
    },
    {
      name: 'delete-rental-batch100',
      dataset: d,
      describe: 'DELETE by primary key, 100 rows/txn',
      txnSize: 100,
      prepare: s => ensureRentals(s, DEL_BASE),
      step: (s, n) =>
        bulkDelete(
          s,
          'rental',
          'rental_id',
          Array.from({length: 100}, (_, k) => rentalId(DEL_BASE + n * 100 + k)),
        ),
    },
    {
      name: 'mixed-oltp-small-txn',
      dataset: d,
      describe: 'Rental + payment written together in one small transaction',
      txnSize: 2,
      step: async (s, n) => {
        await s.begin(async tx => {
          const t = tx as unknown as Sql;
          await bulkInsert(t, 'rental', [mkRental(50_000_000 + n)]);
          await bulkInsert(t, 'payment', [
            {
              payment_id: 50_000_000 + n,
              customer_id: payment.pick('customer_id', n),
              staff_id: payment.pick('staff_id', n),
              rental_id: 50_000_000 + n,
              amount: payment.pick('amount', n),
              payment_date: payment.pick('payment_date', n),
            },
          ]);
        });
        return 2;
      },
    },
  ];
}

/** Removes rows left behind by previous runs. */
export async function cleanupSynthetic(
  dataset: DatasetName,
  sql: Sql,
): Promise<void> {
  const stmts: Record<DatasetName, string[]> = {
    chinook: [
      `DELETE FROM playlist_track WHERE track_id >= ${PK_BASE}`,
      `DELETE FROM invoice_line WHERE invoice_line_id >= ${PK_BASE}`,
      `DELETE FROM track WHERE track_id >= ${PK_BASE}`,
    ],
    pagila: [
      `DELETE FROM payment WHERE payment_id >= ${PK_BASE}`,
      `DELETE FROM rental WHERE rental_id >= ${PK_BASE}`,
      `DELETE FROM film WHERE film_id >= ${PK_BASE}`,
    ],
    zbugs: [
      `DELETE FROM "comment" WHERE id LIKE 'llc-%'`,
      `DELETE FROM "issueLabel" WHERE "issueID" LIKE 'llc-%'`,
      `DELETE FROM "viewState" WHERE "userID" LIKE 'llc-%' OR "issueID" LIKE 'llc-%'`,
      `DELETE FROM issue WHERE id LIKE 'llc-%'`,
    ],
  };
  for (const stmt of stmts[dataset]) {
    await sql.unsafe(stmt);
  }
}

export function workloadsFor(
  dataset: DatasetName,
  sql: Sql,
): Promise<Workload[]> {
  switch (dataset) {
    case 'chinook':
      return chinookWorkloads(sql);
    case 'zbugs':
      return zbugsWorkloads(sql);
    case 'pagila':
      return pagilaWorkloads(sql);
  }
}
