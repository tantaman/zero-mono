import {mkdtempSync, rmSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {pathToFileURL} from 'node:url';
import {afterEach, beforeEach, describe, expect, test} from 'vitest';
import {createObjectStore} from './create-object-store.ts';
import {FsObjectStore} from './fs-object-store.ts';
import {S3ObjectStore} from './s3-object-store.ts';

describe('backup/object-store/create-object-store', () => {
  let root: string;

  beforeEach(() => {
    root = mkdtempSync(join(tmpdir(), 'zero-create-object-store-test-'));
  });

  afterEach(() => {
    rmSync(root, {recursive: true, force: true});
  });

  test('file:// URLs produce a working filesystem store', async () => {
    const store = await createObjectStore(pathToFileURL(root).toString());
    expect(store).toBeInstanceOf(FsObjectStore);

    await store.putIfAbsent('a/b', new TextEncoder().encode('x'));
    expect(await store.head('a/b')).toEqual({key: 'a/b', size: 1});
  });

  test('s3:// URLs produce an S3 store', async () => {
    const store = await createObjectStore('s3://bucket/some/prefix', {
      region: 'us-east-1',
    });
    expect(store).toBeInstanceOf(S3ObjectStore);
  });

  test('other schemes are rejected', async () => {
    await expect(createObjectStore('gs://bucket/prefix')).rejects.toThrow(
      'unsupported archive URL',
    );
  });
});
