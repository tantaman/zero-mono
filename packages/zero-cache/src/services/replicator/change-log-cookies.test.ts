import {describe, expect, test} from 'vitest';
import {createSilentLogContext} from '../../../../shared/src/logging-test-utils.ts';
import {Database} from '../../../../zqlite/src/db.ts';
import type {
  MessageBackfill,
  SchemaChange,
} from '../change-source/protocol/current/data.ts';
import {schemaChangeTags} from '../change-source/protocol/current/schema-change-tags.ts';
import {
  backfillRequestsFrom,
  ChangeLogCookieWriter,
  CREATE_CHANGE_LOG_COOKIE_SCHEMA,
  cookieOps,
  EMPTY_COOKIE_SET,
  foldCookies,
  readCookieRowCounts,
  readCookies,
  replaceCookies,
  type CookieChange,
  type CookieOp,
  type CookieSet,
} from './change-log-cookies.ts';

const lc = createSilentLogContext();

function createCookieJar(): Database {
  const db = new Database(lc, ':memory:');
  db.exec(CREATE_CHANGE_LOG_COOKIE_SCHEMA);
  return db;
}

/** Folds a change sequence through the SQLite interpreter. */
function fold(db: Database, ...changes: CookieChange[]): CookieSet {
  const writer = new ChangeLogCookieWriter(db);
  changes.forEach(change => writer.apply(change));
  return readCookies(db);
}

const ROW_KEY: {columns: string[]; type: 'default'} = {
  columns: ['id'],
  type: 'default',
};

/** A batch of two rows of `my.foo`, whose row key is `id`. */
function backfillBatch(
  rowValues: MessageBackfill['rowValues'],
  columns = ['a', 'b'],
): MessageBackfill {
  return {
    tag: 'backfill',
    relation: {schema: 'my', name: 'foo', rowKey: {columns: ['id']}},
    columns,
    watermark: '05',
    rowValues,
  };
}

const CREATE_FOO: SchemaChange = {
  tag: 'create-table',
  spec: {schema: 'my', name: 'foo', columns: {}},
  metadata: {rowKey: {type: 'index', columns: ['a', 'b']}},
  backfill: {
    a: {fooID: 987, barID: 'zoo'},
    b: {fooID: 843, barID: 'ozz'},
  },
};

describe('replicator/change-log-cookies', () => {
  describe('cookieOps', () => {
    // The fold is what keeps the Postgres and SQLite change logs from drifting,
    // so each transition's ops are pinned rather than round-tripped.
    const cases: [name: string, change: CookieChange, ops: CookieOp[]][] = [
      [
        'create-table with metadata and backfills',
        CREATE_FOO,
        [
          {
            op: 'upsert-metadata',
            table: {schema: 'my', name: 'foo'},
            metadata: {rowKey: {type: 'index', columns: ['a', 'b']}},
          },
          {
            op: 'upsert-backfill',
            table: {schema: 'my', name: 'foo'},
            column: 'a',
            backfill: {fooID: 987, barID: 'zoo'},
          },
          {
            op: 'upsert-backfill',
            table: {schema: 'my', name: 'foo'},
            column: 'b',
            backfill: {fooID: 843, barID: 'ozz'},
          },
        ],
      ],
      [
        // Both fields are optional in the protocol, and a change source that
        // does not support backfill sends neither.
        'create-table without metadata or backfills',
        {
          tag: 'create-table',
          spec: {schema: 'my', name: 'foo', columns: {}},
        },
        [],
      ],
      [
        'update-table-metadata',
        {
          tag: 'update-table-metadata',
          table: {schema: 'my', name: 'foo'},
          old: {rowKey: {type: 'index', columns: ['a']}},
          new: {rowKey: {type: 'default', columns: ['b']}},
        },
        [
          {
            op: 'upsert-metadata',
            table: {schema: 'my', name: 'foo'},
            metadata: {rowKey: {type: 'default', columns: ['b']}},
          },
        ],
      ],
      [
        'add-column with table metadata and a backfill',
        {
          tag: 'add-column',
          table: {schema: 'my', name: 'foo'},
          tableMetadata: {rowKey: {type: 'default', columns: ['b']}},
          column: {name: 'c', spec: {pos: 3, dataType: 'text'}},
          backfill: {fooID: 123, barID: 'baz'},
        },
        [
          {
            op: 'upsert-metadata',
            table: {schema: 'my', name: 'foo'},
            metadata: {rowKey: {type: 'default', columns: ['b']}},
          },
          {
            op: 'upsert-backfill',
            table: {schema: 'my', name: 'foo'},
            column: 'c',
            backfill: {fooID: 123, barID: 'baz'},
          },
        ],
      ],
      [
        'add-column with neither',
        {
          tag: 'add-column',
          table: {schema: 'my', name: 'foo'},
          column: {name: 'c', spec: {pos: 3, dataType: 'text'}},
        },
        [],
      ],
      [
        'rename-table',
        {
          tag: 'rename-table',
          old: {schema: 'my', name: 'foo'},
          new: {schema: 'your', name: 'bar'},
        },
        [
          {
            op: 'rename-table',
            old: {schema: 'my', name: 'foo'},
            new: {schema: 'your', name: 'bar'},
          },
        ],
      ],
      [
        'drop-table',
        {tag: 'drop-table', id: {schema: 'my', name: 'foo'}},
        [{op: 'drop-table', table: {schema: 'my', name: 'foo'}}],
      ],
      [
        'update-column that renames',
        {
          tag: 'update-column',
          table: {schema: 'my', name: 'foo'},
          old: {name: 'a', spec: {pos: 1, dataType: 'text'}},
          new: {name: 'z', spec: {pos: 1, dataType: 'text'}},
        },
        [
          {
            op: 'rename-column',
            table: {schema: 'my', name: 'foo'},
            old: 'a',
            new: 'z',
          },
        ],
      ],
      [
        // A type change moves nothing: whether a column is backfilling is not a
        // property of its spec.
        'update-column that does not rename',
        {
          tag: 'update-column',
          table: {schema: 'my', name: 'foo'},
          old: {name: 'a', spec: {pos: 1, dataType: 'text'}},
          new: {name: 'a', spec: {pos: 1, dataType: 'int4'}},
        },
        [],
      ],
      [
        'drop-column',
        {tag: 'drop-column', table: {schema: 'my', name: 'foo'}, column: 'a'},
        [
          {
            op: 'drop-column',
            table: {schema: 'my', name: 'foo'},
            column: 'a',
          },
        ],
      ],
      [
        // The rowKey columns are excluded from `columns` but are backfilled
        // with them, so the op clears both.
        'backfill-completed',
        {
          tag: 'backfill-completed',
          relation: {schema: 'my', name: 'foo', rowKey: ROW_KEY},
          columns: ['a', 'b'],
          watermark: '07',
        },
        [
          {
            op: 'complete-backfill',
            table: {schema: 'my', name: 'foo'},
            columns: ['id', 'a', 'b'],
          },
        ],
      ],
      [
        'create-index',
        {
          tag: 'create-index',
          spec: {
            schema: 'my',
            name: 'foo_idx',
            tableName: 'foo',
            unique: false,
            columns: {a: 'ASC'},
          },
        },
        [],
      ],
      [
        'drop-index',
        {tag: 'drop-index', id: {schema: 'my', name: 'foo_idx'}},
        [],
      ],
      [
        "backfill marks the row key of the batch's last row",
        backfillBatch([
          [1n, 'a1', 'b1'],
          [2n, 'a2', 'b2'],
        ]),
        [
          {
            op: 'advance-backfill',
            table: {schema: 'my', name: 'foo'},
            // The row key columns are marked along with the backfilled ones,
            // matching the set `complete-backfill` clears.
            columns: ['id', 'a', 'b'],
            resumeAfter: [2n],
          },
        ],
      ],
      ['backfill with no rows', backfillBatch([]), []],
    ];

    test.each(cases)('%s', (_name, change, ops) => {
      expect(cookieOps(change)).toEqual(ops);
    });

    test('covers every change the jar folds', () => {
      // The fold is exhaustive at compile time; this is the reminder that a new
      // tag also needs a case above, including if its answer is "no ops".
      expect(new Set(cases.map(([, change]) => change.tag))).toEqual(
        new Set<string>([...schemaChangeTags, 'backfill']),
      );
    });
  });

  describe('the SQLite interpreter', () => {
    test('create-table seeds both cookies', () => {
      using db = createCookieJar();

      expect(fold(db, CREATE_FOO)).toEqual({
        tableMetadata: [
          {
            schema: 'my',
            table: 'foo',
            metadata: {rowKey: {type: 'index', columns: ['a', 'b']}},
          },
        ],
        backfilling: [
          {
            schema: 'my',
            table: 'foo',
            column: 'a',
            backfill: {fooID: 987, barID: 'zoo'},
          },
          {
            schema: 'my',
            table: 'foo',
            column: 'b',
            backfill: {fooID: 843, barID: 'ozz'},
          },
        ],
      });
    });

    test('metadata and backfills upsert rather than collide', () => {
      using db = createCookieJar();

      const {tableMetadata, backfilling} = fold(
        db,
        CREATE_FOO,
        {
          tag: 'update-table-metadata',
          table: {schema: 'my', name: 'foo'},
          old: {rowKey: {type: 'index', columns: ['a', 'b']}},
          new: {rowKey: {type: 'default', columns: ['id']}},
        },
        {
          tag: 'add-column',
          table: {schema: 'my', name: 'foo'},
          column: {name: 'a', spec: {pos: 1, dataType: 'text'}},
          backfill: {fooID: 111, barID: 'restarted'},
        },
      );

      expect(tableMetadata).toEqual([
        {
          schema: 'my',
          table: 'foo',
          metadata: {rowKey: {type: 'default', columns: ['id']}},
        },
      ]);
      expect(
        backfilling.map(({column, backfill}) => [column, backfill]),
      ).toEqual([
        ['a', {fooID: 111, barID: 'restarted'}],
        ['b', {fooID: 843, barID: 'ozz'}],
      ]);
    });

    test('a rename moves both cookies, and only the renamed table', () => {
      using db = createCookieJar();

      const cookies = fold(
        db,
        CREATE_FOO,
        {
          tag: 'create-table',
          spec: {schema: 'my', name: 'other', columns: {}},
          backfill: {a: {fooID: 1}},
        },
        {
          tag: 'rename-table',
          old: {schema: 'my', name: 'foo'},
          new: {schema: 'your', name: 'renamed'},
        },
      );

      expect(
        cookies.tableMetadata.map(({schema, table}) => [schema, table]),
      ).toEqual([['your', 'renamed']]);
      expect(
        cookies.backfilling.map(({schema, table, column}) => [
          schema,
          table,
          column,
        ]),
      ).toEqual([
        ['my', 'other', 'a'],
        ['your', 'renamed', 'a'],
        ['your', 'renamed', 'b'],
      ]);
    });

    test('a column rename moves only the named column', () => {
      using db = createCookieJar();

      const {backfilling} = fold(db, CREATE_FOO, {
        tag: 'update-column',
        table: {schema: 'my', name: 'foo'},
        old: {name: 'a', spec: {pos: 1, dataType: 'text'}},
        new: {name: 'z', spec: {pos: 1, dataType: 'text'}},
      });

      expect(
        backfilling.map(({column, backfill}) => [column, backfill]),
      ).toEqual([
        ['b', {fooID: 843, barID: 'ozz'}],
        ['z', {fooID: 987, barID: 'zoo'}],
      ]);
    });

    test('drop-column and backfill-completed clear backfills, not metadata', () => {
      using db = createCookieJar();

      const dropped = fold(db, CREATE_FOO, {
        tag: 'drop-column',
        table: {schema: 'my', name: 'foo'},
        column: 'a',
      });
      expect(dropped.backfilling.map(({column}) => column)).toEqual(['b']);
      expect(dropped.tableMetadata).toHaveLength(1);

      const completed = fold(db, {
        tag: 'backfill-completed',
        relation: {schema: 'my', name: 'foo', rowKey: ROW_KEY},
        columns: ['b'],
        watermark: '07',
      });
      // The table's metadata outlives its backfills: it is what the *next*
      // BackfillRequest for the table has to carry.
      expect(completed.backfilling).toEqual([]);
      expect(completed.tableMetadata).toHaveLength(1);
    });

    test('drop-table clears both cookies', () => {
      using db = createCookieJar();

      expect(
        fold(db, CREATE_FOO, {
          tag: 'drop-table',
          id: {schema: 'my', name: 'foo'},
        }),
      ).toEqual(EMPTY_COOKIE_SET);
    });

    test('a table with backfills and no metadata is legal', () => {
      using db = createCookieJar();

      const cookies = fold(db, {
        tag: 'create-table',
        spec: {schema: 'your', name: 'bar', columns: {}},
        backfill: {a: {fooID: 987}},
      });

      expect(cookies.tableMetadata).toEqual([]);
      expect(cookies.backfilling).toHaveLength(1);
    });

    test('apply reports the ops it ran', () => {
      using db = createCookieJar();
      const writer = new ChangeLogCookieWriter(db);

      expect(writer.apply(CREATE_FOO).map(({op}) => op)).toEqual([
        'upsert-metadata',
        'upsert-backfill',
        'upsert-backfill',
      ]);
      expect(
        writer.apply({
          tag: 'create-index',
          spec: {
            schema: 'my',
            name: 'foo_idx',
            tableName: 'foo',
            unique: false,
            columns: {a: 'ASC'},
          },
        }),
      ).toEqual([]);
    });
  });

  describe('the progress mark', () => {
    const marksOf = (cookies: CookieSet) =>
      cookies.backfilling.map(c => [c.column, c.resumeAfter]);

    test('a batch marks the table, and the next batch overwrites it', () => {
      using db = createCookieJar();
      const writer = new ChangeLogCookieWriter(db);
      writer.apply(CREATE_FOO); // backfilling columns a and b

      // Marks round trip through the stores' text encoding, which renders an
      // integer that fits a double back as a number.
      writer.apply(backfillBatch([[1n, 'a1', 'b1']]));
      expect(marksOf(readCookies(db))).toEqual([
        ['a', [1]],
        ['b', [1]],
      ]);

      writer.apply(
        backfillBatch([
          [2n, 'a2', 'b2'],
          [9007199254740993n, '', ''],
        ]),
      );
      expect(marksOf(readCookies(db))).toEqual([
        ['a', [9007199254740993n]],
        ['b', [9007199254740993n]],
      ]);
    });

    test('a column that is not backfilling is not marked', () => {
      using db = createCookieJar();
      const writer = new ChangeLogCookieWriter(db);
      writer.apply(CREATE_FOO);

      // `c` is in the batch but has no cookie row, so nothing to advance.
      writer.apply(backfillBatch([[1n, 'a1', 'c1']], ['a', 'c']));
      expect(readCookies(db).backfilling.map(c => c.column)).toEqual([
        'a',
        'b',
      ]);
      expect(marksOf(readCookies(db))).toEqual([
        ['a', [1]],
        ['b', undefined],
      ]);
    });

    test('a restarted backfill drops the mark', () => {
      using db = createCookieJar();
      const writer = new ChangeLogCookieWriter(db);
      writer.apply(CREATE_FOO);
      writer.apply(backfillBatch([[1n, 'a1', 'b1']]));

      writer.apply({
        tag: 'add-column',
        table: {schema: 'my', name: 'foo'},
        column: {name: 'a', spec: {dataType: 'text', pos: 3}},
        backfill: {fooID: 111},
      });
      expect(marksOf(readCookies(db))).toEqual([
        ['a', undefined],
        ['b', [1]],
      ]);
    });

    test('the in-memory interpreter agrees with the SQLite one', () => {
      using db = createCookieJar();
      const changes: CookieChange[] = [
        CREATE_FOO,
        backfillBatch([[1n, 'a1', 'b1']]),
        backfillBatch([[4n, 'a4', 'b4']]),
      ];
      expect(foldCookies(EMPTY_COOKIE_SET, changes)).toEqual(
        fold(db, ...changes),
      );
    });

    test('a request resumes only when every column agrees', () => {
      using db = createCookieJar();
      const writer = new ChangeLogCookieWriter(db);
      writer.apply(CREATE_FOO);

      // No mark yet: the request starts from the beginning.
      expect(backfillRequestsFrom(readCookies(db))[0].resumeAfter).toBe(
        undefined,
      );

      writer.apply(backfillBatch([[7n, 'a7', 'b7']]));
      expect(backfillRequestsFrom(readCookies(db))[0].resumeAfter).toEqual([7]);

      // A column whose backfill started later has no rows before the mark the
      // others reached, so the table restarts rather than skipping them.
      writer.apply({
        tag: 'add-column',
        table: {schema: 'my', name: 'foo'},
        column: {name: 'c', spec: {dataType: 'text', pos: 4}},
        backfill: {fooID: 222},
      });
      const [request] = backfillRequestsFrom(readCookies(db));
      expect(Object.keys(request.columns)).toEqual(['a', 'b', 'c']);
      expect(request.resumeAfter).toBe(undefined);
    });

    test('marks are per table', () => {
      using db = createCookieJar();
      const writer = new ChangeLogCookieWriter(db);
      writer.apply(CREATE_FOO);
      writer.apply({
        tag: 'create-table',
        spec: {schema: 'your', name: 'bar', columns: {}},
        backfill: {c: {barID: 3}},
      });

      writer.apply(backfillBatch([[5n, 'a5', 'b5']]));
      const requests = backfillRequestsFrom(readCookies(db));
      expect(requests.map(r => [r.table.name, r.resumeAfter])).toEqual([
        ['foo', [5]],
        ['bar', undefined],
      ]);
    });

    test('a completed backfill takes its mark with it', () => {
      using db = createCookieJar();
      const writer = new ChangeLogCookieWriter(db);
      writer.apply(CREATE_FOO);
      writer.apply(backfillBatch([[5n, 'a5', 'b5']]));
      writer.apply({
        tag: 'backfill-completed',
        relation: {schema: 'my', name: 'foo', rowKey: {columns: ['id']}},
        columns: ['a', 'b'],
        watermark: '09',
      });
      expect(readCookies(db)).toEqual({
        ...readCookies(db),
        backfilling: [],
      });
      expect(backfillRequestsFrom(readCookies(db))).toEqual([]);
    });
  });

  describe('readers', () => {
    test('replaceCookies replaces the whole set', () => {
      using db = createCookieJar();
      fold(db, CREATE_FOO);

      const replacement: CookieSet = {
        tableMetadata: [
          {
            schema: 'your',
            table: 'bar',
            metadata: {rowKey: {type: 'default', columns: ['id']}},
          },
        ],
        backfilling: [
          {schema: 'your', table: 'bar', column: 'c', backfill: {fooID: 5}},
        ],
      };
      replaceCookies(db, replacement);
      expect(readCookies(db)).toEqual(replacement);

      replaceCookies(db, EMPTY_COOKIE_SET);
      expect(readCookies(db)).toEqual(EMPTY_COOKIE_SET);
    });

    test('round-trips values outside the safe integer range', () => {
      using db = createCookieJar();

      const cookies: CookieSet = {
        tableMetadata: [
          {
            schema: 'my',
            table: 'foo',
            metadata: {rowKey: {columns: ['id']}, oid: 9007199254740993n},
          },
        ],
        backfilling: [
          {
            schema: 'my',
            table: 'foo',
            column: 'a',
            backfill: {snapshotID: '000003E8-1', xmin: 1000},
          },
        ],
      };
      replaceCookies(db, cookies);
      expect(readCookies(db)).toEqual(cookies);
    });

    test('row counts are per cookie table', () => {
      using db = createCookieJar();
      expect(readCookieRowCounts(db)).toEqual({
        tableMetadata: 0,
        backfilling: 0,
      });

      fold(db, CREATE_FOO);
      expect(readCookieRowCounts(db)).toEqual({
        tableMetadata: 1,
        backfilling: 2,
      });
    });
  });
});
