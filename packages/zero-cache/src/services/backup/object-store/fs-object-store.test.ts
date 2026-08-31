import {mkdtempSync, rmSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {afterEach, beforeEach, describe, expect, test} from 'vitest';
import {FsObjectStore} from './fs-object-store.ts';
import {
  ObjectAlreadyExistsError,
  ObjectNotFoundError,
  validateKey,
} from './object-store.ts';

const utf8 = new TextEncoder();

describe('backup/object-store/fs', () => {
  let root: string;
  let store: FsObjectStore;

  beforeEach(() => {
    root = mkdtempSync(join(tmpdir(), 'zero-fs-object-store-test-'));
    store = new FsObjectStore(root);
  });

  afterEach(() => {
    rmSync(root, {recursive: true, force: true});
  });

  test('put-if-absent round trip', async () => {
    await store.putIfAbsent('v1/02/log/02-05.seg', utf8.encode('segment'));
    expect(
      new TextDecoder().decode(await store.get('v1/02/log/02-05.seg')),
    ).toBe('segment');
  });

  test('put-if-absent rejects an existing key without clobbering it', async () => {
    await store.putIfAbsent('a/b', utf8.encode('first'));
    await expect(
      store.putIfAbsent('a/b', utf8.encode('second')),
    ).rejects.toThrow(ObjectAlreadyExistsError);
    expect(new TextDecoder().decode(await store.get('a/b'))).toBe('first');
  });

  test('put overwrites', async () => {
    await store.put('pointer.json', utf8.encode('one'));
    await store.put('pointer.json', utf8.encode('two'));
    expect(new TextDecoder().decode(await store.get('pointer.json'))).toBe(
      'two',
    );
  });

  test('get of an absent key fails with ObjectNotFoundError', async () => {
    await expect(store.get('nope')).rejects.toThrow(ObjectNotFoundError);
  });

  test('head reports size, or undefined when absent', async () => {
    await store.putIfAbsent('a/b', utf8.encode('12345'));
    expect(await store.head('a/b')).toEqual({key: 'a/b', size: 5});
    expect(await store.head('a/c')).toBeUndefined();
  });

  test('list filters by prefix and sorts lexicographically', async () => {
    await store.putIfAbsent('v1/02/log/0g-0k.seg', utf8.encode('c'));
    await store.putIfAbsent('v1/02/log/02-05.seg', utf8.encode('aa'));
    await store.putIfAbsent('v1/02/log/05-0g.seg', utf8.encode('b'));
    await store.putIfAbsent('v1/02/base/05/complete.json', utf8.encode('m'));
    await store.putIfAbsent('v1/03/log/03-04.seg', utf8.encode('x'));

    expect(await store.list('v1/02/log/')).toEqual([
      {key: 'v1/02/log/02-05.seg', size: 2},
      {key: 'v1/02/log/05-0g.seg', size: 1},
      {key: 'v1/02/log/0g-0k.seg', size: 1},
    ]);
    // A prefix need not be directory-aligned.
    expect((await store.list('v1/02/')).map(o => o.key)).toEqual([
      'v1/02/base/05/complete.json',
      'v1/02/log/02-05.seg',
      'v1/02/log/05-0g.seg',
      'v1/02/log/0g-0k.seg',
    ]);
    expect(await store.list('v9/')).toEqual([]);
  });

  test('list of an empty store is empty', async () => {
    expect(await store.list('')).toEqual([]);
  });

  test('delete is idempotent', async () => {
    await store.putIfAbsent('a/b', utf8.encode('x'));
    await store.delete('a/b');
    await store.delete('a/b');
    expect(await store.head('a/b')).toBeUndefined();
  });

  test.each([
    '',
    '/leading',
    'trailing/',
    'a//b',
    '../escape',
    'a/../b',
    'a/.hidden',
    'sp ace',
  ])('rejects invalid key %j', async key => {
    expect(() => validateKey(key)).toThrow('invalid object key');
    await expect(store.putIfAbsent(key, utf8.encode('x'))).rejects.toThrow(
      'invalid object key',
    );
  });
});
