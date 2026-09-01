import type {LogContext} from '@rocicorp/logger';
import {beforeEach, describe, expect} from 'vitest';
import {createSilentLogContext} from '../../../../../shared/src/logging-test-utils.ts';
import {must} from '../../../../../shared/src/must.ts';
import {Database} from '../../../../../zqlite/src/db.ts';
import {getConnectionURI, type PgTest, test} from '../../../test/db.ts';
import type {PostgresDB} from '../../../types/pg.ts';
import {
  backfillRequestsFrom,
  ChangeLogCookieWriter,
  CREATE_CHANGE_LOG_COOKIE_SCHEMA,
  readCookies,
  replaceCookies,
} from '../../replicator/change-log-cookies.ts';
import type {BackfillRequest, MessageBackfill} from '../protocol/current.ts';
import {streamBackfill} from './backfill-stream.ts';
import {getPublicationInfo} from './schema/published.ts';

const SLOT_NAME = 'backfill_test_slot';

describe('backfill-stream', () => {
  let lc: LogContext;
  let upstream: PostgresDB;
  let upstreamURI: string;
  let columnBackfillRequest: BackfillRequest;
  let tableBackfillRequest: BackfillRequest;

  beforeEach<PgTest>(async ({testDBs}) => {
    lc = createSilentLogContext();
    upstream = await testDBs.create('backfill_stream_test_db');
    upstreamURI = getConnectionURI(upstream);

    await upstream.unsafe(/*sql*/ `
      CREATE TABLE foo(
        id1 INT8 NOT NULL,
        id2 INT4 NOT NULL,
        a TEXT,
        b JSON,
        c JSON[],
        PRIMARY KEY(id1, id2)
      );
      CREATE TABLE bar(
        id1 INT8 NOT NULL,
        id2 INT4 NOT NULL,
        a TEXT,
        b JSON,
        c JSON[],
        PRIMARY KEY(id1, id2)
      );
      CREATE PUBLICATION the_pub FOR TABLE foo;

      DO $$
      BEGIN
        FOR i IN 1..10 LOOP
          INSERT INTO foo (id1, id2, a, b, c)
            VALUES(i, i+1, 
              REPEAT(i::text, 10), 
              json_build_object('d', i),  
              ARRAY[to_json(i), to_json(i+1), to_json((i+2)::text), json_build_object('e', i+3)]
            );
        END LOOP;
      END $$;
    `);

    const {tables} = await getPublicationInfo(upstream, ['the_pub']);
    const tableSpec = tables[0];

    columnBackfillRequest = {
      table: {
        schema: 'public',
        name: 'foo',
        metadata: {
          schemaOID: must(tableSpec.schemaOID),
          relationOID: tableSpec.oid,
          rowKey: {
            id1: {attNum: tableSpec.columns.id1.pos},
            id2: {attNum: tableSpec.columns.id2.pos},
          },
        },
      },
      columns: {
        c: {attNum: tableSpec.columns.c.pos},
        b: {attNum: tableSpec.columns.b.pos},
      },
    };

    tableBackfillRequest = {
      table: {
        schema: 'public',
        name: 'foo',
        metadata: {
          schemaOID: must(tableSpec.schemaOID),
          relationOID: tableSpec.oid,
          rowKey: {
            id2: {attNum: tableSpec.columns.id2.pos},
            id1: {attNum: tableSpec.columns.id1.pos},
          },
        },
      },
      columns: {
        id1: {attNum: tableSpec.columns.id1.pos},
        id2: {attNum: tableSpec.columns.id2.pos},
        a: {attNum: tableSpec.columns.a.pos},
        c: {attNum: tableSpec.columns.c.pos},
        b: {attNum: tableSpec.columns.b.pos},
      },
    };

    return async () => {
      expect(
        await upstream /*sql*/ `
          SELECT slot_name FROM pg_replication_slots WHERE slot_name LIKE 'backfill_test_slot_%'`,
      ).toEqual([]);

      await testDBs.drop(upstream);
    };
  });

  test.each([
    {mode: 'binary', textCopy: false},
    {mode: 'text', textCopy: true},
  ])(`column backfill ($mode)`, async ({textCopy}) => {
    const stream = streamBackfill(
      lc,
      upstreamURI,
      {slot: SLOT_NAME, publications: ['the_pub']},
      columnBackfillRequest,
      {textCopy},
    );
    const results = [];
    for await (const msg of stream) {
      results.push(msg);
    }

    // Binary mode returns JSON[] as a stringified array.
    // Text mode returns JSON[] as a parsed JS array.
    const arr = (vals: unknown[]) => (textCopy ? vals : JSON.stringify(vals));

    expect(results).toMatchObject([
      {
        tag: 'backfill',
        watermark: expect.any(String),
        relation: {
          schema: 'public',
          name: 'foo',
          rowKey: {columns: ['id1', 'id2']},
        },
        columns: ['c', 'b'],
        rowValues: [
          [1n, 2, arr([1, 2, '3', {e: 4}]), '{"d" : 1}'],
          [2n, 3, arr([2, 3, '4', {e: 5}]), '{"d" : 2}'],
          [3n, 4, arr([3, 4, '5', {e: 6}]), '{"d" : 3}'],
          [4n, 5, arr([4, 5, '6', {e: 7}]), '{"d" : 4}'],
          [5n, 6, arr([5, 6, '7', {e: 8}]), '{"d" : 5}'],
          [6n, 7, arr([6, 7, '8', {e: 9}]), '{"d" : 6}'],
          [7n, 8, arr([7, 8, '9', {e: 10}]), '{"d" : 7}'],
          [8n, 9, arr([8, 9, '10', {e: 11}]), '{"d" : 8}'],
          [9n, 10, arr([9, 10, '11', {e: 12}]), '{"d" : 9}'],
          [10n, 11, arr([10, 11, '12', {e: 13}]), '{"d" : 10}'],
        ],
        status: {rows: 10, totalRows: 10, totalBytes: expect.any(Number)},
      },
      {
        tag: 'backfill-completed',
        relation: {
          schema: 'public',
          name: 'foo',
          rowKey: {columns: ['id1', 'id2']},
        },
        columns: ['c', 'b'],
        status: {rows: 10, totalRows: 10, totalBytes: expect.any(Number)},
      },
    ]);
  });

  test.each([
    {mode: 'binary', textCopy: false},
    {mode: 'text', textCopy: true},
  ])(`table backfill ($mode)`, async ({textCopy}) => {
    const stream = streamBackfill(
      lc,
      upstreamURI,
      {slot: SLOT_NAME, publications: ['the_pub']},
      tableBackfillRequest,
      {textCopy},
    );
    const results = [];
    for await (const msg of stream) {
      results.push(msg);
    }

    const arr = (vals: unknown[]) => (textCopy ? vals : JSON.stringify(vals));

    // Columns should deduped and ordered: [id2, id1, a, c, b]
    expect(results).toMatchObject([
      {
        tag: 'backfill',
        watermark: expect.any(String),
        relation: {
          schema: 'public',
          name: 'foo',
          rowKey: {columns: ['id2', 'id1']},
        },
        columns: ['a', 'c', 'b'],
        rowValues: [
          [2, 1n, '1111111111', arr([1, 2, '3', {e: 4}]), '{"d" : 1}'],
          [3, 2n, '2222222222', arr([2, 3, '4', {e: 5}]), '{"d" : 2}'],
          [4, 3n, '3333333333', arr([3, 4, '5', {e: 6}]), '{"d" : 3}'],
          [5, 4n, '4444444444', arr([4, 5, '6', {e: 7}]), '{"d" : 4}'],
          [6, 5n, '5555555555', arr([5, 6, '7', {e: 8}]), '{"d" : 5}'],
          [7, 6n, '6666666666', arr([6, 7, '8', {e: 9}]), '{"d" : 6}'],
          [8, 7n, '7777777777', arr([7, 8, '9', {e: 10}]), '{"d" : 7}'],
          [9, 8n, '8888888888', arr([8, 9, '10', {e: 11}]), '{"d" : 8}'],
          [10, 9n, '9999999999', arr([9, 10, '11', {e: 12}]), '{"d" : 9}'],
          [
            11,
            10n,
            '10101010101010101010',
            arr([10, 11, '12', {e: 13}]),
            '{"d" : 10}',
          ],
        ],
        status: {rows: 10, totalRows: 10, totalBytes: expect.any(Number)},
      },
      {
        tag: 'backfill-completed',
        relation: {
          schema: 'public',
          name: 'foo',
          rowKey: {columns: ['id2', 'id1']},
        },
        columns: ['a', 'c', 'b'],
        status: {rows: 10, totalRows: 10, totalBytes: expect.any(Number)},
      },
    ]);
  });

  test.each([
    {mode: 'binary', textCopy: false},
    {mode: 'text', textCopy: true},
  ])(`resumed table backfill ($mode)`, async ({textCopy}) => {
    const stream = streamBackfill(
      lc,
      upstreamURI,
      {slot: SLOT_NAME, publications: ['the_pub']},
      // The row key columns are ['id2', 'id1'].
      {...tableBackfillRequest, resumeAfter: [6, 5]},
      {textCopy},
    );
    const results = [];
    for await (const msg of stream) {
      results.push(msg);
    }

    const arr = (vals: unknown[]) => (textCopy ? vals : JSON.stringify(vals));

    // Only rows strictly after (id2, id1) = (6, 5), with totals reflecting
    // the remaining rows.
    expect(results).toMatchObject([
      {
        tag: 'backfill',
        watermark: expect.any(String),
        relation: {
          schema: 'public',
          name: 'foo',
          rowKey: {columns: ['id2', 'id1']},
        },
        columns: ['a', 'c', 'b'],
        rowValues: [
          [7, 6n, '6666666666', arr([6, 7, '8', {e: 9}]), '{"d" : 6}'],
          [8, 7n, '7777777777', arr([7, 8, '9', {e: 10}]), '{"d" : 7}'],
          [9, 8n, '8888888888', arr([8, 9, '10', {e: 11}]), '{"d" : 8}'],
          [10, 9n, '9999999999', arr([9, 10, '11', {e: 12}]), '{"d" : 9}'],
          [
            11,
            10n,
            '10101010101010101010',
            arr([10, 11, '12', {e: 13}]),
            '{"d" : 10}',
          ],
        ],
        status: {rows: 5, totalRows: 5, totalBytes: expect.any(Number)},
      },
      {
        tag: 'backfill-completed',
        status: {rows: 5, totalRows: 5, totalBytes: expect.any(Number)},
      },
    ]);
  });

  // The whole point of the mark: emission -> cookie jar -> next request. The
  // link this proves is the one no unit test can, since the mark's values are
  // whatever the COPY decoders produced -- here an int8 row key, which arrives
  // as a bigint and which the resume comparison has to render as a SQL literal.
  test('an interrupted backfill resumes after the mark it left', async () => {
    const drain = async (request: BackfillRequest) => {
      const msgs = [];
      for await (const msg of streamBackfill(
        lc,
        upstreamURI,
        {slot: SLOT_NAME, publications: ['the_pub']},
        request,
      )) {
        msgs.push(msg);
      }
      return msgs;
    };
    const rowsOf = (msgs: {tag: string}[]) =>
      msgs
        .filter((m): m is MessageBackfill => m.tag === 'backfill')
        .flatMap(m => m.rowValues);

    const all = rowsOf(await drain(tableBackfillRequest));
    expect(all).toHaveLength(10);

    // The connection drops after the first four rows are durably applied. The
    // cookie jar holds what the change log would hold at that moment.
    using db = new Database(lc, ':memory:');
    db.exec(CREATE_CHANGE_LOG_COOKIE_SCHEMA);
    replaceCookies(db, {
      tableMetadata: [
        {
          schema: 'public',
          table: 'foo',
          metadata: tableBackfillRequest.table.metadata!,
        },
      ],
      backfilling: Object.entries(tableBackfillRequest.columns).map(
        ([column, backfill]) => ({
          schema: 'public',
          table: 'foo',
          column,
          backfill,
        }),
      ),
    });
    new ChangeLogCookieWriter(db).apply({
      tag: 'backfill',
      relation: {
        schema: 'public',
        name: 'foo',
        rowKey: {columns: ['id2', 'id1']},
      },
      columns: ['a', 'c', 'b'],
      watermark: '05',
      rowValues: all.slice(0, 4),
    });

    const [resumed] = backfillRequestsFrom(readCookies(db));
    // The int8 half of the key arrived as `4n` and comes back out as `4`: the
    // store holds the mark as text, and an integer that fits a double is read
    // back as a number. Both render as the same SQL literal.
    expect(resumed.resumeAfter).toEqual([5, 4]);

    const rest = rowsOf(await drain(resumed));
    // Compared by row key: each `backfill` message names its own `columns`,
    // and a request built from the cookie jar names them in the jar's order
    // rather than the original request's, so the value tuples are ordered
    // differently even though they carry the same row.
    const keys = (rows: readonly (readonly unknown[])[]) =>
      rows.map(r => [r[0], r[1]]);
    // Exactly the rows that were not applied, each of them exactly once.
    expect(keys(rest)).toEqual(keys(all.slice(4)));
    expect([...keys(all.slice(0, 4)), ...keys(rest)]).toEqual(keys(all));
    expect(new Set(Object.keys(resumed.columns))).toEqual(
      new Set(Object.keys(tableBackfillRequest.columns)),
    );
  });

  test('an int8 key above the safe integer range resumes exactly', async () => {
    // The case that keeps `keyValueLiteral` needing bigint: past 2^53 the mark
    // stays a bigint all the way to the SQL literal, and rendering it through
    // a double would land the resume on the wrong row.
    await upstream.unsafe(/*sql*/ `
      INSERT INTO foo (id1, id2, a, b, c) VALUES
        (9007199254740993, 20, 'big1', '{}', ARRAY[]::json[]),
        (9007199254740995, 21, 'big2', '{}', ARRAY[]::json[]);
    `);

    using db = new Database(lc, ':memory:');
    db.exec(CREATE_CHANGE_LOG_COOKIE_SCHEMA);
    replaceCookies(db, {
      tableMetadata: [],
      backfilling: [
        {
          schema: 'public',
          table: 'foo',
          column: 'a',
          backfill: {attNum: 3},
          resumeAfter: [20, 9007199254740993n],
        },
      ],
    });
    const [{resumeAfter}] = backfillRequestsFrom(readCookies(db));
    expect(resumeAfter).toEqual([20, 9007199254740993n]);

    const msgs = [];
    for await (const msg of streamBackfill(
      lc,
      upstreamURI,
      {slot: SLOT_NAME, publications: ['the_pub']},
      {...tableBackfillRequest, resumeAfter},
    )) {
      msgs.push(msg);
    }
    const rows = msgs
      .filter((m): m is MessageBackfill => m.tag === 'backfill')
      .flatMap(m => m.rowValues);
    // Strictly after (20, 9007199254740993): the second big row and nothing
    // else. A resume rendered through a double would have compared against
    // 9007199254740992 and re-emitted the first one.
    expect(rows.map(r => [r[0], r[1]])).toEqual([[21, 9007199254740995n]]);
  });

  test('unsupported resumeAfter restarts from the beginning', async () => {
    const stream = streamBackfill(
      lc,
      upstreamURI,
      {slot: SLOT_NAME, publications: ['the_pub']},
      {...columnBackfillRequest, resumeAfter: [null, 2]},
    );
    const results = [];
    for await (const msg of stream) {
      results.push(msg);
    }
    expect(results).toMatchObject([
      {
        tag: 'backfill',
        status: {rows: 10, totalRows: 10, totalBytes: expect.any(Number)},
      },
      {
        tag: 'backfill-completed',
        status: {rows: 10, totalRows: 10, totalBytes: expect.any(Number)},
      },
    ]);
  });

  test('resumed backfill with a text key compares bytewise', async () => {
    await upstream.unsafe(/*sql*/ `
      CREATE TABLE qux(id TEXT PRIMARY KEY, n INT);
      ALTER PUBLICATION the_pub ADD TABLE qux;
      INSERT INTO qux (id, n) VALUES ('a', 1), ('B', 2), ('c', 3);
    `);
    const {tables} = await getPublicationInfo(upstream, ['the_pub']);
    const quxSpec = must(tables.find(t => t.name === 'qux'));
    const bf: BackfillRequest = {
      table: {
        schema: 'public',
        name: 'qux',
        metadata: {
          schemaOID: must(quxSpec.schemaOID),
          relationOID: quxSpec.oid,
          rowKey: {id: {attNum: quxSpec.columns.id.pos}},
        },
      },
      columns: {n: {attNum: quxSpec.columns.n.pos}},
      resumeAfter: ['B'],
    };

    const results = [];
    for await (const msg of streamBackfill(
      lc,
      upstreamURI,
      {slot: SLOT_NAME, publications: ['the_pub']},
      bf,
    )) {
      results.push(msg);
    }

    // In COLLATE "C" (bytewise) order, 'B' (0x42) sorts before both 'a'
    // (0x61) and 'c' (0x63), so resuming after 'B' returns 'a' and 'c'
    // regardless of the database's default (possibly locale-aware) collation.
    expect(results).toMatchObject([
      {
        tag: 'backfill',
        relation: {schema: 'public', name: 'qux', rowKey: {columns: ['id']}},
        columns: ['n'],
        rowValues: [
          ['a', 1],
          ['c', 3],
        ],
        status: {rows: 2, totalRows: 2, totalBytes: expect.any(Number)},
      },
      {
        tag: 'backfill-completed',
        status: {rows: 2, totalRows: 2, totalBytes: expect.any(Number)},
      },
    ]);
  });

  test.each([
    ['Rename unrelated column', 'ALTER TABLE foo RENAME a TO z'],
    ['Rename unrelated table', 'ALTER TABLE bar RENAME TO baz'],
  ])('Compatible backfill request: %s', async (_name, sqlStmts) => {
    await upstream.unsafe(sqlStmts);
    const stream = streamBackfill(
      lc,
      upstreamURI,
      {slot: SLOT_NAME, publications: ['the_pub']},
      columnBackfillRequest,
    );
    for await (const _ of stream) {
      break;
    }
  });

  test.each([
    [
      'Rename table',
      `ALTER TABLE foo RENAME TO baz`,
      'Table has been renamed or dropped',
    ],
    [
      'Rename backfilling row key column',
      `ALTER TABLE foo RENAME id1 TO id`,
      'Row key (e.g. PRIMARY KEY or INDEX) has changed',
    ],
    [
      'Rename backfilling column',
      `ALTER TABLE foo RENAME b TO d`,
      'Column b has been renamed or dropped',
    ],
    [
      'Drop backfilling row key column',
      `ALTER TABLE foo DROP id2`,
      'Row key (e.g. PRIMARY KEY or INDEX) has changed',
    ],
    [
      'Drop backfilling column',
      `ALTER TABLE foo DROP c`,
      'Column c has been renamed or dropped',
    ],
    [
      'Drop backfilling table',
      `DROP TABLE foo`,
      'Table has been renamed or dropped',
    ],
    [
      'Swap backfilling row key names',
      /*sql*/ `
      ALTER TABLE foo RENAME id1 to id;
      ALTER TABLE foo RENAME id2 to id1;
      ALTER TABLE foo RENAME id to id2;
      `,
      'Column id1 no longer corresponds to the original column',
    ],
    [
      'Swap backfilling column names',
      /*sql*/ `
      ALTER TABLE foo RENAME b to d;
      ALTER TABLE foo RENAME c to b;
      ALTER TABLE foo RENAME d to c;
      `,
      'Column c no longer corresponds to the original column',
    ],
    [
      'Swap table names',
      /*sql*/ `
      ALTER TABLE foo RENAME TO boo;
      ALTER TABLE bar RENAME TO foo;
      ALTER TABLE boo RENAME TO bar;
      `,
      'Table has been renamed or dropped',
    ],
    [
      'Change backfilling row key',
      /*sql*/ `
      ALTER TABLE foo DROP CONSTRAINT foo_pkey;
      ALTER TABLE foo ADD CONSTRAINT foo_pkey PRIMARY KEY(id1);
      `,
      'Row key (e.g. PRIMARY KEY or INDEX) has changed',
    ],
  ])('Incompatible backfill request: %s', async (_name, sqlStmts, reason) => {
    await upstream.unsafe(sqlStmts);
    const stream = streamBackfill(
      lc,
      upstreamURI,
      {slot: SLOT_NAME, publications: ['the_pub']},
      columnBackfillRequest,
    );

    let result: unknown = null;
    try {
      for await (const _ of stream) {
        break;
      }
    } catch (e) {
      result = e;
    }
    expect(String(result)).toBe(
      `SchemaIncompatibilityError: Cannot backfill public.foo[c,b]: ${reason}`,
    );
  });
});
