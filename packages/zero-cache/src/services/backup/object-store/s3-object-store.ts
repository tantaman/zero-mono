import {
  AbortMultipartUploadCommand,
  CompleteMultipartUploadCommand,
  CreateMultipartUploadCommand,
  DeleteObjectCommand,
  GetObjectCommand,
  HeadObjectCommand,
  ListObjectsV2Command,
  PutObjectCommand,
  S3ServiceException,
  UploadPartCommand,
  type S3Client,
} from '@aws-sdk/client-s3';
import {must} from '../../../../../shared/src/must.ts';
import {
  ObjectAlreadyExistsError,
  ObjectNotFoundError,
  validateKey,
  type ObjectMetadata,
  type ObjectStore,
} from './object-store.ts';

/**
 * S3-backed {@link ObjectStore} for the logical backup archive, addressing a
 * bucket under an optional key prefix (both parsed from the `s3://` archive
 * URL by `createObjectStore`).
 *
 * {@link putIfAbsent} relies on S3 conditional writes (`If-None-Match: *`),
 * so a lost-then-retried upload of a deterministically-named object either
 * lands exactly once or fails loudly with {@link ObjectAlreadyExistsError}.
 * {@link putStreamIfAbsent} preserves those semantics for arbitrarily large
 * objects: content is consumed in bounded part-sized buffers, a single-part
 * object goes through the same conditional `PutObject`, and a multipart
 * upload applies `If-None-Match: *` to `CompleteMultipartUpload` — which S3
 * conditional writes support — so put-if-absent survives multipart.
 */

/** S3's fixed multipart part-count limit. */
const MAX_PARTS = 10_000;

export type S3ObjectStoreOptions = {
  /**
   * The base part size for multipart uploads, which is also the
   * streaming-write memory bound and the single-put threshold: content that
   * fits in one part uploads as a conditional `PutObject`. Defaults to
   * {@link DEFAULT_PART_BYTES}. Production callers must respect S3's 5 MiB
   * minimum; tests shrink it to exercise multipart cheaply.
   */
  partBytes?: number | undefined;
};

export const DEFAULT_PART_BYTES = 8 * 1024 * 1024;

export class S3ObjectStore implements ObjectStore {
  readonly #client: S3Client;
  readonly #bucket: string;
  readonly #prefix: string;
  readonly #partBytes: number;

  constructor(
    client: S3Client,
    bucket: string,
    prefix: string,
    options: S3ObjectStoreOptions = {},
  ) {
    this.#client = client;
    this.#bucket = bucket;
    // Normalized so that `${prefix}${key}` always joins with exactly one '/'.
    this.#prefix =
      prefix.length && !prefix.endsWith('/') ? `${prefix}/` : prefix;
    this.#partBytes = options.partBytes ?? DEFAULT_PART_BYTES;
  }

  #objectKey(key: string): string {
    return `${this.#prefix}${validateKey(key)}`;
  }

  async putIfAbsent(key: string, data: Uint8Array): Promise<void> {
    try {
      await this.#client.send(
        new PutObjectCommand({
          Bucket: this.#bucket,
          Key: this.#objectKey(key),
          Body: data,
          IfNoneMatch: '*',
        }),
      );
    } catch (e) {
      if (
        e instanceof S3ServiceException &&
        e.$metadata.httpStatusCode === 412
      ) {
        throw new ObjectAlreadyExistsError(key);
      }
      throw e;
    }
  }

  /**
   * Streams `source` into the store holding at most one part in memory.
   * The strategy is chosen by what the stream actually yields — a lying
   * `sizeHint` costs efficiency, never correctness: `sizeHint` only scales
   * the part size up when the hinted size would exceed S3's 10,000-part
   * limit at the base part size.
   */
  async putStreamIfAbsent(
    key: string,
    source: () => ReadableStream<Uint8Array>,
    sizeHint: number,
  ): Promise<void> {
    const partBytes = Math.max(
      this.#partBytes,
      Math.ceil(sizeHint / MAX_PARTS),
    );
    const parts = partsOf(source(), partBytes);
    const {value: first, done} = await parts.next();
    if (done) {
      await this.putIfAbsent(key, new Uint8Array(0));
      return;
    }
    const {value: second, done: single} = await parts.next();
    if (single) {
      // The whole object fits in one part: same conditional single put as
      // putIfAbsent, with no multipart bookkeeping to clean up.
      await this.putIfAbsent(key, first);
      return;
    }

    const objectKey = this.#objectKey(key);
    const {UploadId: uploadId} = await this.#client.send(
      new CreateMultipartUploadCommand({
        Bucket: this.#bucket,
        Key: objectKey,
      }),
    );
    try {
      const etags: string[] = [];
      for (
        let part: Uint8Array | undefined = first, next = second;
        part !== undefined;
        part = next, next = (await parts.next()).value
      ) {
        const {ETag: etag} = await this.#client.send(
          new UploadPartCommand({
            Bucket: this.#bucket,
            Key: objectKey,
            UploadId: uploadId,
            PartNumber: etags.length + 1,
            Body: part,
            ContentLength: part.length,
          }),
        );
        etags.push(must(etag, 'S3 UploadPart without an ETag'));
      }
      await this.#client.send(
        new CompleteMultipartUploadCommand({
          Bucket: this.#bucket,
          Key: objectKey,
          UploadId: uploadId,
          IfNoneMatch: '*',
          MultipartUpload: {
            Parts: etags.map((etag, i) => ({ETag: etag, PartNumber: i + 1})),
          },
        }),
      );
    } catch (e) {
      // The abort is best-effort cleanup of the staged parts; the thrown
      // error is what matters (an incomplete multipart upload is invisible
      // and reclaimable by a bucket lifecycle rule).
      await this.#client
        .send(
          new AbortMultipartUploadCommand({
            Bucket: this.#bucket,
            Key: objectKey,
            UploadId: uploadId,
          }),
        )
        .catch(() => {});
      if (
        e instanceof S3ServiceException &&
        e.$metadata.httpStatusCode === 412
      ) {
        throw new ObjectAlreadyExistsError(key);
      }
      throw e;
    }
  }

  async put(key: string, data: Uint8Array): Promise<void> {
    await this.#client.send(
      new PutObjectCommand({
        Bucket: this.#bucket,
        Key: this.#objectKey(key),
        Body: data,
      }),
    );
  }

  async get(key: string): Promise<Uint8Array> {
    try {
      const response = await this.#client.send(
        new GetObjectCommand({
          Bucket: this.#bucket,
          Key: this.#objectKey(key),
        }),
      );
      return await must(
        response.Body,
        'S3 GetObject without a Body',
      ).transformToByteArray();
    } catch (e) {
      if (isNotFound(e)) {
        throw new ObjectNotFoundError(key);
      }
      throw e;
    }
  }

  async getStream(key: string): Promise<ReadableStream<Uint8Array>> {
    try {
      const response = await this.#client.send(
        new GetObjectCommand({
          Bucket: this.#bucket,
          Key: this.#objectKey(key),
        }),
      );
      return must(
        response.Body,
        'S3 GetObject without a Body',
      ).transformToWebStream() as ReadableStream<Uint8Array>;
    } catch (e) {
      if (isNotFound(e)) {
        throw new ObjectNotFoundError(key);
      }
      throw e;
    }
  }

  async head(key: string): Promise<ObjectMetadata | undefined> {
    try {
      const response = await this.#client.send(
        new HeadObjectCommand({
          Bucket: this.#bucket,
          Key: this.#objectKey(key),
        }),
      );
      return {key, size: response.ContentLength ?? 0};
    } catch (e) {
      if (isNotFound(e)) {
        return undefined;
      }
      throw e;
    }
  }

  async list(prefix: string): Promise<ObjectMetadata[]> {
    const results: ObjectMetadata[] = [];
    let continuationToken: string | undefined;
    do {
      const response = await this.#client.send(
        new ListObjectsV2Command({
          Bucket: this.#bucket,
          Prefix: `${this.#prefix}${prefix}`,
          ContinuationToken: continuationToken,
        }),
      );
      for (const {Key: objectKey, Size: size} of response.Contents ?? []) {
        if (objectKey === undefined) {
          continue;
        }
        results.push({
          key: objectKey.slice(this.#prefix.length),
          size: size ?? 0,
        });
      }
      continuationToken = response.IsTruncated
        ? response.NextContinuationToken
        : undefined;
    } while (continuationToken !== undefined);
    // ListObjectsV2 returns keys in lexicographic order within each page, and
    // pages are ordered; the sort is a cheap guarantee for the interface.
    return results.sort((a, b) => (a.key < b.key ? -1 : a.key > b.key ? 1 : 0));
  }

  async delete(key: string): Promise<void> {
    await this.#client.send(
      new DeleteObjectCommand({
        Bucket: this.#bucket,
        Key: this.#objectKey(key),
      }),
    );
  }
}

/**
 * Re-frames a stream into buffers of exactly `partBytes` (the last may be
 * shorter), which is the memory bound of a streaming upload. Buffered parts
 * also sidestep the AWS SDK's inability to retry a consumed stream body.
 */
async function* partsOf(
  stream: ReadableStream<Uint8Array>,
  partBytes: number,
): AsyncGenerator<Uint8Array> {
  const buffered: Uint8Array[] = [];
  let bufferedLength = 0;
  const drain = (length: number): Uint8Array => {
    const part = new Uint8Array(length);
    let offset = 0;
    while (offset < length) {
      const chunk = buffered[0];
      const take = Math.min(chunk.length, length - offset);
      part.set(take === chunk.length ? chunk : chunk.subarray(0, take), offset);
      offset += take;
      if (take === chunk.length) {
        buffered.shift();
      } else {
        buffered[0] = chunk.subarray(take);
      }
    }
    bufferedLength -= length;
    return part;
  };
  for await (const chunk of stream) {
    buffered.push(chunk);
    bufferedLength += chunk.length;
    while (bufferedLength >= partBytes) {
      yield drain(partBytes);
    }
  }
  if (bufferedLength > 0) {
    yield drain(bufferedLength);
  }
}

function isNotFound(e: unknown): boolean {
  return (
    e instanceof S3ServiceException &&
    (e.name === 'NoSuchKey' ||
      e.name === 'NotFound' ||
      e.$metadata.httpStatusCode === 404)
  );
}
