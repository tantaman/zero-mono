import {
  AbortMultipartUploadCommand,
  CompleteMultipartUploadCommand,
  CreateMultipartUploadCommand,
  DeleteObjectCommand,
  GetObjectCommand,
  HeadObjectCommand,
  PutObjectCommand,
  UploadPartCommand,
  type ListObjectsV2Command,
  S3ServiceException,
  type S3Client,
} from '@aws-sdk/client-s3';
import {describe, expect, test, vi} from 'vitest';
import {ObjectAlreadyExistsError, ObjectNotFoundError} from './object-store.ts';
import {S3ObjectStore, type S3ObjectStoreOptions} from './s3-object-store.ts';

function serviceException(name: string, httpStatusCode: number) {
  return new S3ServiceException({
    name,
    $fault: 'client',
    $metadata: {httpStatusCode},
  });
}

function storeWith(
  send: (command: unknown) => unknown,
  options?: S3ObjectStoreOptions,
) {
  const client = {send: vi.fn(send)};
  return {
    store: new S3ObjectStore(
      client as unknown as S3Client,
      'bucket',
      'archive/prefix',
      options,
    ),
    send: client.send,
  };
}

const utf8 = new TextEncoder();

function streamOf(...chunks: string[]): ReadableStream<Uint8Array> {
  return new ReadableStream<Uint8Array>({
    start(controller) {
      for (const chunk of chunks) {
        controller.enqueue(utf8.encode(chunk));
      }
      controller.close();
    },
  });
}

/**
 * A `send` implementation for the multipart command sequence, returning
 * ETags for parts and failing where directed.
 */
function multipartSend(fail?: {command: unknown; error: Error}) {
  let parts = 0;
  return (command: unknown) => {
    if (
      fail !== undefined &&
      command instanceof (fail.command as new (...args: never[]) => unknown)
    ) {
      return Promise.reject(fail.error);
    }
    if (command instanceof CreateMultipartUploadCommand) {
      return Promise.resolve({UploadId: 'upload-1'});
    }
    if (command instanceof UploadPartCommand) {
      return Promise.resolve({ETag: `etag-${++parts}`});
    }
    return Promise.resolve({});
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

  test('putStreamIfAbsent of a single-part object sends one conditional put', async () => {
    const {store, send} = storeWith(() => Promise.resolve({}), {partBytes: 10});
    await store.putStreamIfAbsent('a/b', () => streamOf('12345'), 5);

    expect(send).toHaveBeenCalledTimes(1);
    const command = send.mock.calls[0][0] as PutObjectCommand;
    expect(command).toBeInstanceOf(PutObjectCommand);
    expect(command.input).toMatchObject({
      Bucket: 'bucket',
      Key: 'archive/prefix/a/b',
      IfNoneMatch: '*',
      Body: utf8.encode('12345'),
    });
  });

  test('putStreamIfAbsent of an empty stream sends one conditional put', async () => {
    const {store, send} = storeWith(() => Promise.resolve({}), {partBytes: 10});
    await store.putStreamIfAbsent('a/b', () => streamOf(), 0);

    expect(send).toHaveBeenCalledTimes(1);
    const command = send.mock.calls[0][0] as PutObjectCommand;
    expect(command).toBeInstanceOf(PutObjectCommand);
    expect(command.input.Body).toEqual(new Uint8Array(0));
  });

  test('putStreamIfAbsent above one part uploads multipart with a conditional complete', async () => {
    const {store, send} = storeWith(multipartSend(), {partBytes: 4});
    // 10 bytes across misaligned chunks: parts of 4, 4, and 2 bytes.
    await store.putStreamIfAbsent(
      'a/b',
      () => streamOf('123', '456789', '0'),
      10,
    );

    const commands = send.mock.calls.map(call => call[0]);
    expect(commands.map(c => (c as object).constructor)).toEqual([
      CreateMultipartUploadCommand,
      UploadPartCommand,
      UploadPartCommand,
      UploadPartCommand,
      CompleteMultipartUploadCommand,
    ]);
    const parts = commands.slice(1, 4) as UploadPartCommand[];
    expect(
      parts.map(({input}) => ({
        part: input.PartNumber,
        body: new TextDecoder().decode(input.Body as Uint8Array),
      })),
    ).toEqual([
      {part: 1, body: '1234'},
      {part: 2, body: '5678'},
      {part: 3, body: '90'},
    ]);
    const complete = commands[4] as CompleteMultipartUploadCommand;
    expect(complete.input).toMatchObject({
      Bucket: 'bucket',
      Key: 'archive/prefix/a/b',
      UploadId: 'upload-1',
      IfNoneMatch: '*',
      MultipartUpload: {
        Parts: [
          {ETag: 'etag-1', PartNumber: 1},
          {ETag: 'etag-2', PartNumber: 2},
          {ETag: 'etag-3', PartNumber: 3},
        ],
      },
    });
  });

  test('putStreamIfAbsent scales the part size to stay within the part limit', async () => {
    const {store, send} = storeWith(multipartSend(), {partBytes: 1});
    // A hinted 30,000 bytes at 1-byte parts would need 30,000 parts; the
    // part size scales to ceil(30000/10000) = 3.
    await store.putStreamIfAbsent('a/b', () => streamOf('12345678'), 30_000);

    const parts = send.mock.calls
      .map(call => call[0])
      .filter(c => c instanceof UploadPartCommand) as UploadPartCommand[];
    expect(
      parts.map(({input}) =>
        new TextDecoder().decode(input.Body as Uint8Array),
      ),
    ).toEqual(['123', '456', '78']);
  });

  test('putStreamIfAbsent maps a 412 on complete to ObjectAlreadyExistsError and aborts', async () => {
    const {store, send} = storeWith(
      multipartSend({
        command: CompleteMultipartUploadCommand,
        error: serviceException('PreconditionFailed', 412),
      }),
      {partBytes: 2},
    );
    await expect(
      store.putStreamIfAbsent('a/b', () => streamOf('12345'), 5),
    ).rejects.toThrow(ObjectAlreadyExistsError);

    const abort = send.mock.calls
      .map(call => call[0])
      .find(c => c instanceof AbortMultipartUploadCommand) as
      | AbortMultipartUploadCommand
      | undefined;
    expect(abort?.input).toMatchObject({
      Bucket: 'bucket',
      Key: 'archive/prefix/a/b',
      UploadId: 'upload-1',
    });
  });

  test('putStreamIfAbsent aborts and propagates an upload-part failure', async () => {
    const {store, send} = storeWith(
      multipartSend({
        command: UploadPartCommand,
        error: serviceException('SlowDown', 503),
      }),
      {partBytes: 2},
    );
    await expect(
      store.putStreamIfAbsent('a/b', () => streamOf('12345'), 5),
    ).rejects.toThrow(S3ServiceException);
    expect(
      send.mock.calls.some(
        call => call[0] instanceof AbortMultipartUploadCommand,
      ),
    ).toBe(true);
  });

  test('getStream returns the body stream', async () => {
    const {store, send} = storeWith(() =>
      Promise.resolve({
        Body: {transformToWebStream: () => streamOf('body')},
      }),
    );
    const chunks: Uint8Array[] = [];
    for await (const chunk of await store.getStream('a/b')) {
      chunks.push(chunk);
    }
    expect(new TextDecoder().decode(Buffer.concat(chunks))).toBe('body');
    expect(send.mock.calls[0][0]).toBeInstanceOf(GetObjectCommand);
  });

  test('getStream maps NoSuchKey to ObjectNotFoundError', async () => {
    const {store} = storeWith(() =>
      Promise.reject(serviceException('NoSuchKey', 404)),
    );
    await expect(store.getStream('a/b')).rejects.toThrow(ObjectNotFoundError);
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
