import {
  DeleteObjectCommand,
  GetObjectCommand,
  HeadObjectCommand,
  ListObjectsV2Command,
  PutObjectCommand,
  S3ServiceException,
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
 */
export class S3ObjectStore implements ObjectStore {
  readonly #client: S3Client;
  readonly #bucket: string;
  readonly #prefix: string;

  constructor(client: S3Client, bucket: string, prefix: string) {
    this.#client = client;
    this.#bucket = bucket;
    // Normalized so that `${prefix}${key}` always joins with exactly one '/'.
    this.#prefix =
      prefix.length && !prefix.endsWith('/') ? `${prefix}/` : prefix;
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

function isNotFound(e: unknown): boolean {
  return (
    e instanceof S3ServiceException &&
    (e.name === 'NoSuchKey' ||
      e.name === 'NotFound' ||
      e.$metadata.httpStatusCode === 404)
  );
}
