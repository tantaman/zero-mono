import {
  link,
  mkdir,
  readdir,
  readFile,
  rename,
  rm,
  stat,
  unlink,
  writeFile,
} from 'node:fs/promises';
import {dirname, join} from 'node:path';
import {
  ObjectAlreadyExistsError,
  ObjectNotFoundError,
  validateKey,
  type ObjectMetadata,
  type ObjectStore,
} from './object-store.ts';

/**
 * Filesystem-backed {@link ObjectStore} rooted at a directory, for local
 * development and integration tests (including crash-injection restore
 * drills, which need a store they can inspect and mutilate).
 *
 * Writes land in a temp file first and reach their final name atomically:
 * `link()` for {@link putIfAbsent} (whose EEXIST is the put-if-absent
 * contract) and `rename()` for {@link put}. A crash mid-write can therefore
 * leave a stray temp file but never a partial object.
 */
export class FsObjectStore implements ObjectStore {
  readonly #root: string;

  constructor(root: string) {
    this.#root = root;
  }

  #path(key: string): string {
    return join(this.#root, ...validateKey(key).split('/'));
  }

  async #writeTemp(path: string, data: Uint8Array): Promise<string> {
    await mkdir(dirname(path), {recursive: true});
    const temp = `${path}.tmp-${crypto.randomUUID()}`;
    await writeFile(temp, data);
    return temp;
  }

  async putIfAbsent(key: string, data: Uint8Array): Promise<void> {
    const path = this.#path(key);
    const temp = await this.#writeTemp(path, data);
    try {
      await link(temp, path);
    } catch (e) {
      if (isErrnoException(e, 'EEXIST')) {
        throw new ObjectAlreadyExistsError(key);
      }
      throw e;
    } finally {
      await unlink(temp).catch(() => {});
    }
  }

  async put(key: string, data: Uint8Array): Promise<void> {
    const path = this.#path(key);
    const temp = await this.#writeTemp(path, data);
    try {
      await rename(temp, path);
    } catch (e) {
      await unlink(temp).catch(() => {});
      throw e;
    }
  }

  async get(key: string): Promise<Uint8Array> {
    try {
      return await readFile(this.#path(key));
    } catch (e) {
      if (isErrnoException(e, 'ENOENT')) {
        throw new ObjectNotFoundError(key);
      }
      throw e;
    }
  }

  async head(key: string): Promise<ObjectMetadata | undefined> {
    try {
      const {size} = await stat(this.#path(key));
      return {key, size};
    } catch (e) {
      if (isErrnoException(e, 'ENOENT')) {
        return undefined;
      }
      throw e;
    }
  }

  async list(prefix: string): Promise<ObjectMetadata[]> {
    const results: ObjectMetadata[] = [];
    let entries;
    try {
      entries = await readdir(this.#root, {
        recursive: true,
        withFileTypes: true,
      });
    } catch (e) {
      if (isErrnoException(e, 'ENOENT')) {
        return results; // The root is only created by the first write.
      }
      throw e;
    }
    for (const entry of entries) {
      if (!entry.isFile() || entry.name.includes('.tmp-')) {
        continue;
      }
      const path = join(entry.parentPath, entry.name);
      const key = path
        .slice(this.#root.length)
        .split(PATH_SEPARATORS)
        .filter(Boolean)
        .join('/');
      if (!key.startsWith(prefix)) {
        continue;
      }
      const {size} = await stat(path);
      results.push({key, size});
    }
    return results.sort((a, b) => (a.key < b.key ? -1 : a.key > b.key ? 1 : 0));
  }

  async delete(key: string): Promise<void> {
    await rm(this.#path(key), {force: true});
  }
}

const PATH_SEPARATORS = /[/\\]/;

function isErrnoException(e: unknown, code: string): boolean {
  return e instanceof Error && (e as NodeJS.ErrnoException).code === code;
}
