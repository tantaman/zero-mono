import {afterEach} from 'node:test';
import {beforeEach, describe, expect, test} from 'vitest';
import {createSilentLogContext} from '../../shared/src/logging-test-utils.ts';
import {
  CREATE_STORAGE_TABLE,
  DatabaseStorage,
} from '../../zqlite/src/database-storage.ts';
import {Database} from '../../zqlite/src/db.ts';

describe('view-syncer/database-storage', () => {
  let db: Database;
  let storage: DatabaseStorage;

  beforeEach(() => {
    db = new Database(createSilentLogContext(), ':memory:');
    db.prepare(CREATE_STORAGE_TABLE).run();
    storage = new DatabaseStorage(db);
  });

  afterEach(() => {
    db.close();
  });

  function dumpDB() {
    storage.flush();
    return db.prepare('SELECT * FROM storage').all();
  }

  test('json values', () => {
    const store = storage.createClientGroupStorage('foo-bar').createStorage();
    store.set('int', 1);
    store.set('string', '2');
    store.set('bool', true);
    store.set('null', null);
    store.set('array', [1, 2, 3]);
    store.set('object', {foo: 'bar'});

    expect(store.get('int')).toBe(1);
    expect(store.get('string')).toBe('2');
    expect(store.get('bool')).toBe(true);
    expect(store.get('null')).toBe(null);
    expect(store.get('array')).toEqual([1, 2, 3]);
    expect(store.get('object')).toEqual({foo: 'bar'});

    expect(dumpDB()).toMatchInlineSnapshot(`
      [
        {
          "clientGroupID": "foo-bar",
          "key": "int",
          "op": 1,
          "val": "1",
        },
        {
          "clientGroupID": "foo-bar",
          "key": "string",
          "op": 1,
          "val": ""2"",
        },
        {
          "clientGroupID": "foo-bar",
          "key": "bool",
          "op": 1,
          "val": "true",
        },
        {
          "clientGroupID": "foo-bar",
          "key": "null",
          "op": 1,
          "val": "null",
        },
        {
          "clientGroupID": "foo-bar",
          "key": "array",
          "op": 1,
          "val": "[1,2,3]",
        },
        {
          "clientGroupID": "foo-bar",
          "key": "object",
          "op": 1,
          "val": "{"foo":"bar"}",
        },
      ]
    `);
  });

  test('get non-existent', () => {
    const store = storage.createClientGroupStorage('foo-bar').createStorage();
    expect(store.get('foo')).toBeUndefined;
  });

  test('del', () => {
    const store = storage.createClientGroupStorage('foo-bar').createStorage();
    store.set('foo', 'bar');
    store.set('bar', 'baz');
    store.set('boo', 'doo');

    store.del('bar');
    store.del('bo'); // non-existent
    expect([...store.scan()]).toEqual([
      ['boo', 'doo'],
      ['foo', 'bar'],
    ]);
  });

  test('scan prefix', () => {
    const store = storage.createClientGroupStorage('foo-bar').createStorage();
    store.set('c/', 1);
    store.set('ba/7', 2);
    store.set('b/7', 3);
    store.set('b/5/6', 4);
    store.set('b/4', 5);
    store.set('b/', 6);
    store.set('b', 7);
    store.set('a/2/3', 8);
    store.set('a/1', 9);
    store.set('a/', 10);

    expect([...store.scan({prefix: 'b/'})]).toEqual([
      ['b/', 6],
      ['b/4', 5],
      ['b/5/6', 4],
      ['b/7', 3],
    ]);
  });

  test('client group / operator isolation and destroy', () => {
    const cg1 = storage.createClientGroupStorage('foo-bar');
    const cg2 = storage.createClientGroupStorage('bar-foo');

    const stores = [
      cg1.createStorage(),
      cg1.createStorage(),
      cg2.createStorage(),
      cg2.createStorage(),
    ];

    stores.forEach((s, i) => {
      s.set('foo', i);
    });
    stores.forEach((s, i) => {
      expect(s.get('foo')).toBe(i);
    });

    expect(dumpDB()).toMatchInlineSnapshot(`
      [
        {
          "clientGroupID": "foo-bar",
          "key": "foo",
          "op": 1,
          "val": "0",
        },
        {
          "clientGroupID": "foo-bar",
          "key": "foo",
          "op": 2,
          "val": "1",
        },
        {
          "clientGroupID": "bar-foo",
          "key": "foo",
          "op": 1,
          "val": "2",
        },
        {
          "clientGroupID": "bar-foo",
          "key": "foo",
          "op": 2,
          "val": "3",
        },
      ]
    `);

    cg2.destroy();

    expect(dumpDB()).toMatchInlineSnapshot(`
      [
        {
          "clientGroupID": "foo-bar",
          "key": "foo",
          "op": 1,
          "val": "0",
        },
        {
          "clientGroupID": "foo-bar",
          "key": "foo",
          "op": 2,
          "val": "1",
        },
      ]
    `);
  });

  test('set duplicate key', () => {
    const store = storage.createClientGroupStorage('foo-bar').createStorage();
    store.set('foo', '2');
    expect(store.get('foo')).toBe('2');

    store.set('foo', '3');
    expect(store.get('foo')).toBe('3');

    expect(dumpDB()).toMatchInlineSnapshot(`
      [
        {
          "clientGroupID": "foo-bar",
          "key": "foo",
          "op": 1,
          "val": ""3"",
        },
      ]
    `);
  });

  describe('buffered writes', () => {
    function countRows() {
      return db.prepare('SELECT COUNT(*) AS n FROM storage').get<{n: number}>()
        .n;
    }

    test('reads see writes that are not yet in the DB', () => {
      const store = storage.createClientGroupStorage('cg').createStorage();
      store.set('a', 1);
      store.set('b', {x: [1, 2]});
      expect(countRows()).toBe(0);

      expect(store.get('a')).toBe(1);
      expect(store.get('b')).toEqual({x: [1, 2]});
      expect(store.get('c', 'default')).toBe('default');

      // scan flushes the operator's writes first.
      expect([...store.scan()]).toEqual([
        ['a', 1],
        ['b', {x: [1, 2]}],
      ]);
      expect(countRows()).toBe(2);
    });

    test('set after flush and del of buffered and flushed keys', () => {
      const store = storage.createClientGroupStorage('cg').createStorage();
      store.set('a', 1);
      store.set('b', 2);
      storage.flush();
      store.set('a', 3); // buffered over a flushed value
      store.set('c', 4); // only buffered
      expect(store.get('a')).toBe(3);

      store.del('a');
      store.del('c');
      store.del('b');
      expect(store.get('a')).toBeUndefined();
      expect(store.get('c')).toBeUndefined();
      expect([...store.scan()]).toEqual([]);
      expect(dumpDB()).toEqual([]);
    });

    test('flushes when the buffer is full', () => {
      const db2 = new Database(createSilentLogContext(), ':memory:');
      db2.prepare(CREATE_STORAGE_TABLE).run();
      const count = () =>
        db2.prepare('SELECT COUNT(*) AS n FROM storage').get<{n: number}>().n;
      const s = new DatabaseStorage(db2, {
        commitInterval: 5_000,
        compactionThresholdBytes: 50 * 1024 * 1024,
        flushThreshold: 3,
      });
      const cg = s.createClientGroupStorage('cg');
      const [s1, s2] = [cg.createStorage(), cg.createStorage()];
      s1.set('a', 1);
      s2.set('a', 2);
      s1.set('a', 3); // same key, does not grow the buffer
      expect(count()).toBe(0);
      s2.set('b', 4);
      expect(count()).toBe(3);
      expect(s1.get('a')).toBe(3);
      expect(s2.get('a')).toBe(2);
      expect(s2.get('b')).toBe(4);
      db2.close();
    });

    test('destroy and re-creating a client group drop buffered writes', () => {
      const cg1 = storage.createClientGroupStorage('cg1');
      const cg2 = storage.createClientGroupStorage('cg2');
      const s1 = cg1.createStorage();
      const s2 = cg2.createStorage();
      s1.set('a', 1);
      s2.set('a', 2);
      cg2.destroy();
      expect(dumpDB()).toEqual([
        {clientGroupID: 'cg1', op: 1, key: 'a', val: '1'},
      ]);

      s1.set('b', 3);
      // A new incarnation of the client group reuses operator IDs, so
      // buffered writes of the old one must not land in its storage.
      const cg1Again = storage.createClientGroupStorage('cg1');
      expect(dumpDB()).toEqual([]);
      expect(cg1Again.createStorage().get('b')).toBeUndefined();
    });

    test('keys with NUL and non-ASCII characters', () => {
      const store = storage.createClientGroupStorage('cg').createStorage();
      const keys = [
        'j\x00sa\x00sp1',
        'j\x00s\u00e9\x00s\u{1f600}',
        'j\x00sa\x00',
      ];
      for (const [i, key] of keys.entries()) {
        store.set(key, i);
      }
      storage.flush();
      for (const [i, key] of keys.entries()) {
        expect(store.get(key)).toBe(i);
      }
      expect([...store.scan({prefix: 'j\x00sa\x00'})]).toEqual([
        ['j\x00sa\x00', 2],
        ['j\x00sa\x00sp1', 0],
      ]);
    });
  });
});
