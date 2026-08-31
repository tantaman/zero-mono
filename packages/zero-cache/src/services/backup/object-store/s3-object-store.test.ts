import {
  DeleteObjectCommand,
  GetObjectCommand,
  HeadObjectCommand,
  PutObjectCommand,
  type ListObjectsV2Command,
  S3ServiceException,
  type S3Client,
} from '@aws-sdk/client-s3';
import {describe, expect, test, vi} from 'vitest';
import {ObjectAlreadyExistsError, ObjectNotFoundError} from './object-store.ts';
import {S3ObjectStore} from './s3-object-store.ts';

function serviceException(name: string, httpStatusCode: number) {
  return new S3ServiceException({
    name,
    $fault: 'client',
    $metadata: {httpStatusCode},
  });
}

function storeWith(send: (command: unknown) => unknown) {
  const client = {send: vi.fn(send)};
  return {
    store: new S3ObjectStore(
      client as unknown as S3Client,
      'bucket',
      'archive/prefix',
    ),
    send: client.send,
  };
}

describe('backup/object-store/s3', () => {
  test('putIfAbsent sends a conditional put under the prefix', async () => {
    const {store, send} = storeWith(() => Promise.resolve({}));
    await store.putIfAbsent('v1/02/log/02-05.seg', new Uint8Array([1]));

    const command = send.mock.calls[0][0] as PutObjectCommand;
    expect(command).toBeInstanceOf(PutObjectCommand);
    expect(command.input).toMatchObject({
      Bucket: 'bucket',
      Key: 'archive/prefix/v1/02/log/02-05.seg',
      IfNoneMatch: '*',
    });
  });

  test('putIfAbsent maps a 412 to ObjectAlreadyExistsError', async () => {
    const {store} = storeWith(() =>
      Promise.reject(serviceException('PreconditionFailed', 412)),
    );
    await expect(store.putIfAbsent('a/b', new Uint8Array())).rejects.toThrow(
      ObjectAlreadyExistsError,
    );
  });

  test('putIfAbsent propagates other failures', async () => {
    const {store} = storeWith(() =>
      Promise.reject(serviceException('SlowDown', 503)),
    );
    await expect(store.putIfAbsent('a/b', new Uint8Array())).rejects.toThrow(
      S3ServiceException,
    );
  });

  test('put sends an unconditional put', async () => {
    const {store, send} = storeWith(() => Promise.resolve({}));
    await store.put('pointer.json', new Uint8Array([1]));

    const command = send.mock.calls[0][0] as PutObjectCommand;
    expect(command.input).toMatchObject({
      Bucket: 'bucket',
      Key: 'archive/prefix/pointer.json',
    });
    expect(command.input.IfNoneMatch).toBeUndefined();
  });

  test('get returns the body bytes', async () => {
    const {store, send} = storeWith(() =>
      Promise.resolve({
        Body: {
          transformToByteArray: () => Promise.resolve(new Uint8Array([7])),
        },
      }),
    );
    expect(await store.get('a/b')).toEqual(new Uint8Array([7]));
    expect(send.mock.calls[0][0]).toBeInstanceOf(GetObjectCommand);
  });

  test('get maps NoSuchKey to ObjectNotFoundError', async () => {
    const {store} = storeWith(() =>
      Promise.reject(serviceException('NoSuchKey', 404)),
    );
    await expect(store.get('a/b')).rejects.toThrow(ObjectNotFoundError);
  });

  test('head returns metadata, or undefined on 404', async () => {
    const {store, send} = storeWith(() => Promise.resolve({ContentLength: 42}));
    expect(await store.head('a/b')).toEqual({key: 'a/b', size: 42});
    expect(send.mock.calls[0][0]).toBeInstanceOf(HeadObjectCommand);

    const {store: absent} = storeWith(() =>
      Promise.reject(serviceException('NotFound', 404)),
    );
    expect(await absent.head('a/b')).toBeUndefined();
  });

  test('list paginates and strips the store prefix', async () => {
    const pages = [
      {
        Contents: [{Key: 'archive/prefix/v1/log/02-05.seg', Size: 10}],
        IsTruncated: true,
        NextContinuationToken: 'next',
      },
      {
        Contents: [{Key: 'archive/prefix/v1/log/05-0g.seg', Size: 20}],
        IsTruncated: false,
      },
    ];
    const {store, send} = storeWith(() => Promise.resolve(pages.shift()));

    expect(await store.list('v1/log/')).toEqual([
      {key: 'v1/log/02-05.seg', size: 10},
      {key: 'v1/log/05-0g.seg', size: 20},
    ]);
    const [first, second] = send.mock.calls.map(
      call => (call[0] as ListObjectsV2Command).input,
    );
    expect(first).toMatchObject({
      Bucket: 'bucket',
      Prefix: 'archive/prefix/v1/log/',
    });
    expect(second).toMatchObject({ContinuationToken: 'next'});
  });

  test('delete addresses the prefixed key', async () => {
    const {store, send} = storeWith(() => Promise.resolve({}));
    await store.delete('a/b');
    const command = send.mock.calls[0][0] as DeleteObjectCommand;
    expect(command).toBeInstanceOf(DeleteObjectCommand);
    expect(command.input).toMatchObject({
      Bucket: 'bucket',
      Key: 'archive/prefix/a/b',
    });
  });
});
