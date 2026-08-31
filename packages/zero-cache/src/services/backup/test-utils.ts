import type {ChangeStreamData} from '../change-source/protocol/current/downstream.ts';
import {
  ObjectAlreadyExistsError,
  ObjectNotFoundError,
  validateKey,
  type ObjectMetadata,
  type ObjectStore,
} from './object-store/object-store.ts';

/**
 * In-memory {@link ObjectStore} for focused unit tests, with a `beforePut`
 * hook for failure and stall injection.
 */
export class InMemoryObjectStore implements ObjectStore {
  readonly objects = new Map<string, Uint8Array>();

  /** Invoked before every write; throw or block to inject failures. */
  beforePut: ((key: string) => void | Promise<void>) | undefined;

  async putIfAbsent(key: string, data: Uint8Array): Promise<void> {
    validateKey(key);
    await this.beforePut?.(key);
    if (this.objects.has(key)) {
      throw new ObjectAlreadyExistsError(key);
    }
    this.objects.set(key, data);
  }

  async put(key: string, data: Uint8Array): Promise<void> {
    validateKey(key);
    await this.beforePut?.(key);
    this.objects.set(key, data);
  }

  get(key: string): Promise<Uint8Array> {
    const data = this.objects.get(key);
    if (data === undefined) {
      return Promise.reject(new ObjectNotFoundError(key));
    }
    return Promise.resolve(data);
  }

  head(key: string): Promise<ObjectMetadata | undefined> {
    const data = this.objects.get(key);
    return Promise.resolve(
      data === undefined ? undefined : {key, size: data.length},
    );
  }

  list(prefix: string): Promise<ObjectMetadata[]> {
    return Promise.resolve(
      [...this.objects.entries()]
        .filter(([key]) => key.startsWith(prefix))
        .map(([key, data]) => ({key, size: data.length}))
        .toSorted((a, b) => (a.key < b.key ? -1 : a.key > b.key ? 1 : 0)),
    );
  }

  delete(key: string): Promise<void> {
    this.objects.delete(key);
    return Promise.resolve();
  }
}

// A wire-conformant relation, i.e. what the change-streamer serializes. The
// segment decoder validates against the protocol schema, which is stricter
// than the shapes some internal test helpers produce.
export const WIRE_RELATION = {
  schema: 'public',
  name: 'issues',
  rowKey: {columns: ['issueID']},
};

export type WireTransaction = {
  watermark: string;
  /** The JSON strings the change-streamer hands each stream consumer. */
  messages: string[];
  parsed: ChangeStreamData[];
};

/**
 * Builds one committed transaction of `rows` inserts into an `issues` table,
 * as both parsed {@link ChangeStreamData} tuples and their JSON encodings.
 */
export function wireTransaction(watermark: string, rows = 1): WireTransaction {
  const parsed: ChangeStreamData[] = [
    ['begin', {tag: 'begin'}, {commitWatermark: watermark}],
  ];
  for (let i = 0; i < rows; i++) {
    parsed.push([
      'data',
      {
        tag: 'insert',
        relation: WIRE_RELATION,
        new: {issueID: `${watermark}-${i}`},
      },
    ]);
  }
  parsed.push(['commit', {tag: 'commit'}, {watermark}]);
  return {watermark, messages: parsed.map(m => JSON.stringify(m)), parsed};
}
