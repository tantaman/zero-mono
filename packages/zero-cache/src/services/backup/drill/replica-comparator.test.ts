import {mkdtempSync, rmSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {afterEach, beforeEach, describe, expect, test} from 'vitest';
import {createSilentLogContext} from '../../../../../shared/src/logging-test-utils.ts';
import {Database} from '../../../../../zqlite/src/db.ts';
import {compareReplicas, tableDigest} from './replica-comparator.ts';

const lc = createSilentLogContext();

const SCHEMA = /*sql*/ `
  CREATE TABLE issues(
    id TEXT PRIMARY KEY,
    num INTEGER,
    score REAL,
    data BLOB,
    _0_version TEXT
  );
`;

describe('backup/drill/replica-comparator', () => {
  let dir: string;
  let reference: Database;
  let restored: Database;

  beforeEach(() => {
    dir = mkdtempSync(join(tmpdir(), 'zero-replica-comparator-test-'));
    reference = new Database(lc, join(dir, 'reference.db'));
    restored = new Database(lc, join(dir, 'restored.db'));
    reference.exec(SCHEMA);
    restored.exec(SCHEMA);
  });

  afterEach(() => {
    reference.close();
    restored.close();
    rmSync(dir, {recursive: true, force: true});
  });

  const ROWS = [
    ['a', 1n, 1.5, Buffer.from([1, 2, 3]), '01'],
    ['b', 2n ** 60n, null, null, '02'],
    ['c', null, -0.25, Buffer.alloc(0), '03'],
  ] as const;

  function insert(db: Database, rows: readonly (readonly unknown[])[]) {
    const stmt =
      db.prepare(/*sql*/ `INSERT INTO issues(id, num, score, data, _0_version)
               VALUES (?, ?, ?, ?, ?)`);
    for (const row of rows) {
      stmt.run(...row);
    }
  }

  test('identical content in different physical order matches', () => {
    insert(reference, ROWS);
    insert(restored, [ROWS[2], ROWS[0], ROWS[1]]); // different rowids

    const result = compareReplicas(lc, reference, restored);
    expect(result).toEqual({tables: 1, rows: 3, mismatches: []});
  });

  test('a changed value is a row mismatch', () => {
    insert(reference, ROWS);
    insert(restored, [
      ROWS[0],
      ROWS[1],
      ['c', null, -0.25, Buffer.alloc(0), 'XX'],
    ]);

    const {mismatches} = compareReplicas(lc, reference, restored);
    expect(mismatches).toMatchObject([{table: 'issues', kind: 'rows'}]);
  });

  test('a missing row is a row mismatch', () => {
    insert(reference, ROWS);
    insert(restored, ROWS.slice(0, 2));

    const {mismatches} = compareReplicas(lc, reference, restored);
    expect(mismatches).toMatchObject([
      {
        table: 'issues',
        kind: 'rows',
        reference: {rows: 3},
        restored: {rows: 2},
      },
    ]);
  });

  test('missing and extra tables are reported', () => {
    reference.exec(/*sql*/ `CREATE TABLE only_ref(id TEXT PRIMARY KEY)`);
    restored.exec(/*sql*/ `CREATE TABLE only_restored(id TEXT PRIMARY KEY)`);

    const {mismatches} = compareReplicas(lc, reference, restored);
    expect(mismatches).toEqual([
      {table: 'only_ref', kind: 'missing-table'},
      {table: 'only_restored', kind: 'extra-table'},
    ]);
  });

  test('a schema difference is reported without comparing rows', () => {
    restored.exec(/*sql*/ `ALTER TABLE issues ADD COLUMN extra TEXT`);
    insert(reference, ROWS);

    const result = compareReplicas(lc, reference, restored);
    expect(result.tables).toBe(0);
    expect(result.mismatches).toMatchObject([
      {table: 'issues', kind: 'schema'},
    ]);
  });

  test('excluded tables are ignored', () => {
    for (const db of [reference, restored]) {
      db.exec(/*sql*/ `CREATE TABLE "_zero.changeLog2"(v TEXT)`);
      db.exec(/*sql*/ `CREATE TABLE "_zero.runtimeEvents"(
        event TEXT PRIMARY KEY, timestamp TEXT)`);
    }
    reference.exec(
      /*sql*/ `INSERT INTO "_zero.changeLog2"(v) VALUES ('only here')`,
    );

    expect(compareReplicas(lc, reference, restored).mismatches).toEqual([]);
    // An explicit (empty) exclude list turns the divergence back on.
    expect(
      compareReplicas(lc, reference, restored, {excludeTables: []}).mismatches,
    ).toMatchObject([{table: '_zero.changeLog2', kind: 'rows'}]);
  });

  test('type-tagged serialization distinguishes lookalike values', () => {
    // A column with no declared type has no affinity, so SQLite stores
    // values exactly as bound.
    reference.exec(/*sql*/ `CREATE TABLE t(v)`);
    restored.exec(/*sql*/ `CREATE TABLE t(v)`);

    for (const [ref, rest] of [
      [null, ''],
      [0n, '0'],
    ] as const) {
      reference.prepare(/*sql*/ `DELETE FROM t`).run();
      restored.prepare(/*sql*/ `DELETE FROM t`).run();
      reference.prepare(/*sql*/ `INSERT INTO t(v) VALUES (?)`).run(ref);
      restored.prepare(/*sql*/ `INSERT INTO t(v) VALUES (?)`).run(rest);
      expect(
        compareReplicas(lc, reference, restored).mismatches,
        `${String(ref)} vs ${String(rest)}`,
      ).toMatchObject([{table: 't', kind: 'rows'}]);
    }

    // INTEGER 1 vs REAL 1.0 (literals, since the driver binds integral JS
    // numbers as INTEGER).
    reference.exec(/*sql*/ `DELETE FROM t; INSERT INTO t(v) VALUES (1)`);
    restored.exec(/*sql*/ `DELETE FROM t; INSERT INTO t(v) VALUES (1.0)`);
    expect(compareReplicas(lc, reference, restored).mismatches).toMatchObject([
      {table: 't', kind: 'rows'},
    ]);
  });

  test('tableDigest is order-independent but content-sensitive', () => {
    insert(reference, ROWS);
    insert(restored, [ROWS[1], ROWS[2], ROWS[0]]);
    const cols = ['id', 'num', 'score', 'data', '_0_version'];

    const a = tableDigest(reference, 'issues', cols);
    const b = tableDigest(restored, 'issues', cols);
    expect(a).toEqual(b);
    expect(a.rows).toBe(3);

    restored.prepare(/*sql*/ `UPDATE issues SET num = 3 WHERE id = 'a'`).run();
    expect(tableDigest(restored, 'issues', cols)).not.toEqual(a);
  });
});
