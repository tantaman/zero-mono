import type {LogContext} from '@rocicorp/logger';
import type {JSONValue} from '../../shared/src/json.ts';
import type {Storage} from '../../zql/src/ivm/operator.ts';
import type {Stream} from '../../zql/src/ivm/stream.ts';
import type {Statement} from './db.ts';
import {Database} from './db.ts';

export interface ClientGroupStorage {
  /** Creates a {@link Storage} instance for a single operator. */
  createStorage(): Storage;

  /** Deletes all storage for the client group. */
  destroy(): void;
}

type Statements = {
  get: Statement;
  setMany: Statement;
  del: Statement;
  scan: Statement;
  clear: Statement;
  commit: Statement;
  begin: Statement;
};

// Exported for testing.
export const CREATE_STORAGE_TABLE = `
  CREATE TABLE storage (
    clientGroupID TEXT,
    op NUMBER,
    key TEXT,
    val TEXT,
    PRIMARY KEY(clientGroupID, op, key)
  )
  `;

const defaultOptions = {
  commitInterval: 5_000,
  compactionThresholdBytes: 50 * 1024 * 1024,
  flushThreshold: 1_000,
};

/**
 * The writes to one operator's storage that are not yet in the DB, as
 * key => JSON-encoded value.
 */
type PendingWrites = {
  readonly cgID: string;
  readonly opID: number;
  readonly writes: Map<string, string>;
};

export class DatabaseStorage {
  static create(
    lc: LogContext,
    path: string,
    options = defaultOptions,
  ): DatabaseStorage {
    // SQLite is used for ephemeral storage (i.e. similar to RAM) that can spill to
    // disk to avoid consuming too much memory. Each worker thread gets its own
    // database (file) and acts as the single reader/writer of the DB, so
    // `locking_mode` is set to `EXCLUSIVE` for performance. Similarly, since
    // durability is not important, `synchronous` is set to `OFF` for performance.
    const db = new Database(lc, path);
    db.unsafeMode(true); // Allows journal_mode = OFF
    db.pragma('locking_mode = EXCLUSIVE');
    db.pragma('foreign_keys = OFF');
    db.pragma('journal_mode = OFF');
    db.pragma('synchronous = OFF');
    db.pragma('auto_vacuum = INCREMENTAL');

    db.prepare(CREATE_STORAGE_TABLE).run();
    lc.debug?.(`Created DatabaseStorage backed by ${path}`);
    return new DatabaseStorage(db, options);
  }

  readonly #stmts: Statements;
  readonly #options: typeof defaultOptions;
  readonly #db: Database;
  #numWrites = 0;

  /**
   * Writes are buffered and inserted in bulk, since one INSERT per write is
   * dominated by per-statement overhead. Operators such as Join write a key
   * for every row they hydrate. Reads see buffered writes: `get` checks the
   * buffer and `scan` flushes the operator's writes first. (That flush is a
   * write, so a scan must not start while another scan is being iterated.)
   */
  readonly #pending = new Set<PendingWrites>();
  #numPending = 0;

  constructor(db: Database, options = defaultOptions) {
    this.#stmts = {
      get: db.prepare(`
        SELECT val FROM storage WHERE
          clientGroupID = ? AND op = ? AND key = ?
      `),
      // Takes a JSON array of [key, val] pairs. (`WHERE true` resolves the
      // parsing ambiguity between the SELECT and the upsert's ON clause.)
      setMany: db.prepare(`
        INSERT INTO storage (clientGroupID, op, key, val)
          SELECT ?, ?, value->>0, value->>1 FROM json_each(?) WHERE true
        ON CONFLICT(clientGroupID, op, key)
        DO
          UPDATE SET val = excluded.val
      `),
      del: db.prepare(`
        DELETE FROM storage WHERE
          clientGroupID = ? AND op = ? AND key = ?
      `),
      scan: db.prepare(`
        SELECT key, val FROM storage WHERE
          clientGroupID = ? AND op = ? AND key >= ?
      `),
      clear: db.prepare(`
        DELETE FROM storage WHERE clientGroupID = ?
      `),
      commit: db.prepare('COMMIT'),
      begin: db.prepare('BEGIN'),
    };
    this.#stmts.begin.run();
    this.#options = options;
    this.#db = db;
  }

  close() {
    this.#checkpoint();
    this.#db.close();
  }

  /** Writes all buffered writes to the DB. */
  flush() {
    for (const pending of this.#pending) {
      this.#flushOp(pending);
    }
  }

  #flushOp(pending: PendingWrites) {
    const {cgID, opID, writes} = pending;
    if (writes.size > 0) {
      this.#stmts.setMany.run(cgID, opID, JSON.stringify([...writes]));
      this.#numPending -= writes.size;
      this.#numWrites += writes.size;
      writes.clear();
    }
    this.#pending.delete(pending);
  }

  #get(
    pending: PendingWrites,
    key: string,
    def?: JSONValue,
  ): JSONValue | undefined {
    const buffered = pending.writes.get(key);
    if (buffered !== undefined) {
      return JSON.parse(buffered);
    }
    this.#maybeCheckpoint();
    const row = this.#stmts.get.get<{val: string}>(
      pending.cgID,
      pending.opID,
      key,
    );
    return row ? JSON.parse(row.val) : def;
  }

  #set(pending: PendingWrites, key: string, val: JSONValue) {
    const {writes} = pending;
    const size = writes.size;
    writes.set(key, JSON.stringify(val));
    if (writes.size > size) {
      this.#pending.add(pending);
      if (++this.#numPending >= this.#options.flushThreshold) {
        this.flush();
        this.#maybeCheckpoint();
      }
    }
  }

  #del(pending: PendingWrites, key: string) {
    if (pending.writes.delete(key)) {
      this.#numPending--;
    }
    this.#maybeCheckpoint();
    // The key may have been flushed before it was buffered again.
    this.#stmts.del.run(pending.cgID, pending.opID, key);
  }

  #dropPending(cgID: string) {
    for (const pending of this.#pending) {
      if (pending.cgID === cgID) {
        this.#numPending -= pending.writes.size;
        pending.writes.clear();
        this.#pending.delete(pending);
      }
    }
  }

  /**
   * We don't need to commit every single write to the DB
   * since we're not concerned with durability.
   * Waiting on commits can be expensive, so we commit
   * every `COMMIT_INTERVAL` writes.
   */
  #maybeCheckpoint() {
    if (++this.#numWrites >= this.#options.commitInterval) {
      this.#checkpoint();
    }
  }

  #checkpoint() {
    this.flush();
    this.#stmts.commit.run();
    this.#stmts.begin.run();
    this.#numWrites = 0;
  }

  *#scan(
    pending: PendingWrites,
    opts: {prefix: string} = {prefix: ''},
  ): Stream<[string, JSONValue]> {
    const {prefix} = opts;
    this.#flushOp(pending);
    for (const {key, val} of this.#stmts.scan.iterate<{
      key: string;
      val: string;
    }>(pending.cgID, pending.opID, prefix)) {
      if (!key.startsWith(prefix)) {
        return;
      }
      yield [key, JSON.parse(val)];
    }
  }

  createClientGroupStorage(cgID: string): ClientGroupStorage {
    const destroy = () => {
      this.#dropPending(cgID);
      this.#stmts.clear.run(cgID);
      this.#checkpoint();
      this.#db.compact(this.#options.compactionThresholdBytes);
    };
    this.#dropPending(cgID);
    this.#stmts.clear.run(cgID);

    let nextOpID = 1;
    return {
      createStorage: () => {
        const pending: PendingWrites = {
          cgID,
          opID: nextOpID++,
          writes: new Map(),
        };
        return {
          get: (key, def?) => this.#get(pending, key, def),
          set: (key, val) => this.#set(pending, key, val),
          del: key => this.#del(pending, key),
          scan: opts => this.#scan(pending, opts),
        };
      },

      destroy,
    };
  }
}
