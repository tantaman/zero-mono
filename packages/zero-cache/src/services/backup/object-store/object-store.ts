/**
 * The minimal object-store surface the logical backup archive needs:
 * immutable put-if-absent for content objects, overwriting put for pointer
 * objects, get, head, list, and delete (for GC).
 *
 * Two backends implement it: S3 (production) and the local filesystem, which
 * is what makes full restore-drill integration tests and local development
 * possible, mirroring litestream's own file-URL support.
 */

export type ObjectMetadata = {
  key: string;
  /** Size of the stored object in bytes. */
  size: number;
};

export interface ObjectStore {
  /**
   * Uploads an immutable object, failing with {@link ObjectAlreadyExistsError}
   * if the key is already present. Content objects (segments, base chunks,
   * manifests) are named deterministically from stream identity and cursor
   * interval, so a retried upload either no-ops against identical content or
   * fails loudly here.
   */
  putIfAbsent(key: string, data: Uint8Array): Promise<void>;

  /**
   * Uploads an object, replacing any existing object at the key. Only for
   * pointer objects that are legitimately rewritten; everything content-
   * addressable goes through {@link putIfAbsent}.
   */
  put(key: string, data: Uint8Array): Promise<void>;

  /** Fails with {@link ObjectNotFoundError} if the key is absent. */
  get(key: string): Promise<Uint8Array>;

  /** Returns `undefined` if the key is absent. */
  head(key: string): Promise<ObjectMetadata | undefined>;

  /**
   * Lists the objects whose keys start with `prefix`, sorted
   * lexicographically by key (the order S3 lists in, which the archive's
   * watermark-encoded names rely on).
   */
  list(prefix: string): Promise<ObjectMetadata[]>;

  /** Deletes the object. Deleting an absent key is a no-op. */
  delete(key: string): Promise<void>;
}

export class ObjectAlreadyExistsError extends Error {
  readonly name = 'ObjectAlreadyExistsError';
  readonly key: string;

  constructor(key: string) {
    super(`object already exists: ${key}`);
    this.key = key;
  }
}

export class ObjectNotFoundError extends Error {
  readonly name = 'ObjectNotFoundError';
  readonly key: string;

  constructor(key: string) {
    super(`object not found: ${key}`);
    this.key = key;
  }
}

const KEY_SEGMENT = /^[A-Za-z0-9][A-Za-z0-9._-]*$/;

/**
 * Restricts keys to `/`-separated segments of safe characters. This is what
 * lets the filesystem backend map keys to paths without any possibility of
 * traversal, and keeps S3 keys within the portable character set.
 */
export function validateKey(key: string): string {
  const segments = key.split('/');
  if (
    key.length === 0 ||
    segments.some(segment => !KEY_SEGMENT.test(segment))
  ) {
    throw new Error(`invalid object key: "${key}"`);
  }
  return key;
}
