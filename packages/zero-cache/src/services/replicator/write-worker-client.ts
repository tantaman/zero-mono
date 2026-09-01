import {Worker} from 'node:worker_threads';
import {resolver, type Resolver} from '@rocicorp/resolver';
import {assert} from '../../../../shared/src/asserts.ts';
import type {LogConfig} from '../../../../shared/src/logging.ts';
import type {Database} from '../../../../zqlite/src/db.ts';
import {WRITE_WORKER_URL} from '../../server/worker-urls.ts';
import type {ChangeStreamData} from '../change-source/protocol/current/downstream.ts';
import type {ChangeProcessorMode, CommitResult} from './change-processor.ts';
import type {SubscriptionState} from './schema/replication-state.ts';

export type PragmaConfig = {
  busyTimeout: number;
  analysisLimit: number;
  walAutocheckpoint?: number | undefined;
  /**
   * Throughput settings for the base producer's `base-builder` mode, whose
   * crash posture is discard-and-rebuild: durability pragmas can be traded
   * away entirely because an unclean shutdown discards the file.
   */
  journalMode?: 'off' | undefined;
  synchronous?: 'off' | undefined;
  lockingMode?: 'exclusive' | undefined;
  /** Negative-KiB form of `cache_size` (SQLite's size-based spelling). */
  cacheSizeKiB?: number | undefined;
};

type ErrorHandler = (err: Error) => void;

/**
 * Interface for a write worker that processes replication messages.
 */
export interface WriteWorkerClient {
  getSubscriptionState(): Promise<SubscriptionState>;
  /**
   * Applies a batch of change-stream messages, in order, returning each
   * message's result positionally (i.e. `null` for everything but a commit).
   *
   * The batch is the unit of the hop to the worker, not the unit of atomicity:
   * transaction boundaries are still the messages themselves, so a batch may
   * contain part of a transaction, several whole ones, or both. It exists
   * because the hop costs ~45us and the SQLite write it carries costs ~10us,
   * so sending one message per change spends most of the applier's time in
   * structured clone and thread wakeups. Callers must bound what they
   * accumulate: a batch is resident memory, and the clone transiently doubles
   * it.
   */
  processMessages(
    batch: readonly ChangeStreamData[],
  ): Promise<(CommitResult | null)[]>;
  abort(): void;
  stop(): Promise<void>;
  onError(handler: ErrorHandler): void;
}

export type SerializedError = {
  name: string;
  message: string;
  stack?: string | undefined;
  cause?: SerializedError | string | undefined;
  details?: Record<string, unknown> | undefined;
};

export function serializeError(err: unknown): SerializedError {
  if (!(err instanceof Error)) {
    return {
      name: 'Error',
      message: String(err),
      details: err && typeof err === 'object' ? {...err} : undefined,
    };
  }

  // Error fields such as message, stack, and some native error details are
  // non-enumerable, so JSON.stringify(err) would usually return "{}".
  const details = Object.fromEntries(
    Object.getOwnPropertyNames(err)
      .filter(key => !['name', 'message', 'stack', 'cause'].includes(key))
      .map(key => [key, (err as unknown as Record<string, unknown>)[key]]),
  );
  const cause =
    err.cause instanceof Error
      ? serializeError(err.cause)
      : err.cause === undefined
        ? undefined
        : String(err.cause);

  return {
    name: err.name,
    message: err.message,
    stack: err.stack,
    cause,
    details: Object.keys(details).length ? details : undefined,
  };
}

export function deserializeError(serialized: SerializedError): Error {
  const err = new Error(serialized.message);
  err.name = serialized.name;
  if (serialized.stack !== undefined) {
    err.stack = serialized.stack;
  }
  if (serialized.cause !== undefined) {
    err.cause =
      typeof serialized.cause === 'string'
        ? serialized.cause
        : deserializeError(serialized.cause);
  }
  if (serialized.details) {
    Object.assign(err, serialized.details);
  }
  return err;
}

// Wire protocol types.
export type ArgsMap = {
  init: [string, ChangeProcessorMode, PragmaConfig, LogConfig];
  getSubscriptionState: [];
  processMessages: [readonly ChangeStreamData[]];
  abort: [];
  stop: [];
};

export type Method = keyof ArgsMap;

export type Request<M extends Method = Method> = {method: M; args: ArgsMap[M]};

export type ResultMap = {
  init: void;
  getSubscriptionState: SubscriptionState;
  processMessages: (CommitResult | null)[];
  abort: void;
  stop: void;
};

export type Response<M extends Method = Method> =
  | {method: M; result: ResultMap[M]; error?: undefined}
  | {method: M; error: SerializedError; result?: undefined};

export type WriteError = {writeError: SerializedError};

export function applyPragmas(db: Database, pragmas: PragmaConfig) {
  db.pragma(`busy_timeout = ${pragmas.busyTimeout}`);
  db.pragma(`analysis_limit = ${pragmas.analysisLimit}`);
  if (pragmas.walAutocheckpoint !== undefined) {
    db.pragma(`wal_autocheckpoint = ${pragmas.walAutocheckpoint}`);
  }
  if (pragmas.journalMode !== undefined) {
    // Leaving WAL requires unsafe mode in better-sqlite3.
    db.unsafeMode(true);
    db.pragma(`journal_mode = ${pragmas.journalMode}`);
    db.unsafeMode(false);
  }
  if (pragmas.synchronous !== undefined) {
    db.pragma(`synchronous = ${pragmas.synchronous}`);
  }
  if (pragmas.lockingMode !== undefined) {
    db.pragma(`locking_mode = ${pragmas.lockingMode}`);
  }
  if (pragmas.cacheSizeKiB !== undefined) {
    db.pragma(`cache_size = -${pragmas.cacheSizeKiB}`);
  }
}

/**
 * Delegates SQLite writes to a worker_thread,
 * keeping the main event loop free for WebSocket heartbeats and IPC.
 */
export class ThreadWriteWorkerClient implements WriteWorkerClient {
  readonly #worker: Worker;
  #pending: Resolver<unknown, Error> | null = null;
  #errorHandler: ErrorHandler = () => {};
  #terminated = false;

  constructor() {
    this.#worker = new Worker(WRITE_WORKER_URL);

    this.#worker.on('message', (msg: Response | WriteError) => {
      if ('writeError' in msg) {
        const error = deserializeError(msg.writeError);
        this.#rejectAll(error);
        this.#errorHandler(error);
        return;
      }
      const r = this.#pending;
      if (!r) return; // stale abort response
      this.#pending = null;
      if (msg.error !== undefined) {
        r.reject(deserializeError(msg.error));
      } else {
        r.resolve(msg.result);
      }
    });

    this.#worker.on('error', (err: Error) => {
      this.#rejectAll(err);
      this.#errorHandler(err);
    });

    this.#worker.on('exit', (code: number) => {
      this.#terminated = true;
      if (code !== 0) {
        const err = new Error(`Worker exited with code ${code}`);
        this.#rejectAll(err);
        this.#errorHandler(err);
      }
    });
  }

  #rejectAll(err: Error) {
    const r = this.#pending;
    if (r) {
      this.#pending = null;
      r.reject(err);
    }
  }

  #call<M extends Method>(method: M, args: ArgsMap[M]): Promise<ResultMap[M]> {
    assert(this.#pending === null, `concurrent call: ${method}`);
    const r = resolver<ResultMap[M]>();
    this.#pending = r as Resolver<unknown, Error>;
    this.#worker.postMessage({method, args} satisfies Request);
    return r.promise;
  }

  init(
    dbPath: string,
    mode: ChangeProcessorMode,
    pragmas: PragmaConfig,
    logConfig: LogConfig,
  ): Promise<void> {
    return this.#call('init', [dbPath, mode, pragmas, logConfig]);
  }

  getSubscriptionState(): Promise<SubscriptionState> {
    return this.#call('getSubscriptionState', []);
  }

  processMessages(
    batch: readonly ChangeStreamData[],
  ): Promise<(CommitResult | null)[]> {
    return this.#call('processMessages', [batch]);
  }

  abort(): void {
    if (!this.#terminated) {
      this.#worker.postMessage({method: 'abort', args: []} satisfies Request);
    }
  }

  async stop(): Promise<void> {
    await this.#call('stop', []);
    if (!this.#terminated) {
      await this.#worker.terminate();
    }
  }

  onError(handler: ErrorHandler): void {
    this.#errorHandler = handler;
  }
}
