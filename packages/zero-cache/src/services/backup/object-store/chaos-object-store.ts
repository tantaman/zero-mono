import {sleep} from '../../../../../shared/src/sleep.ts';
import type {ObjectMetadata, ObjectStore} from './object-store.ts';

/**
 * A fault-injecting {@link ObjectStore} wrapper for the chaos harness: the
 * productionized form of the test `beforePut` hook, with schedules for
 * errors, latency, and sustained outages, applied to every operation. The
 * oracle is the only judge of a chaos run; this wrapper only supplies the
 * weather.
 *
 * Fault decisions are made by a {@link ChaosPolicy}, so a schedule can be
 * anything from a seeded random error rate to a scripted outage window.
 * `error-after` performs the operation and then throws — the "upload landed
 * but the response was lost" case that the deterministic-name idempotent
 * retry discipline exists for.
 */

export type ChaosOp =
  | 'put'
  | 'putIfAbsent'
  | 'putStreamIfAbsent'
  | 'get'
  | 'getStream'
  | 'head'
  | 'list'
  | 'delete';

export type ChaosDecision =
  | {kind: 'pass'}
  /** Fail without invoking the underlying store. */
  | {kind: 'error'}
  /** Invoke the underlying store, then fail anyway (a lost response). */
  | {kind: 'error-after'}
  /** Wait, then apply `then` (default `pass`). */
  | {kind: 'delay'; ms: number; then?: ChaosDecision | undefined};

export type ChaosPolicy = (op: ChaosOp, key: string) => ChaosDecision;

export class ChaosInjectedError extends Error {
  readonly name = 'ChaosInjectedError';
  constructor(op: ChaosOp, key: string, phase: 'before' | 'after') {
    super(`injected ${phase}-op failure: ${op} ${key}`);
  }
}

export type ChaosStats = {
  ops: number;
  errors: number;
  errorsAfter: number;
  delays: number;
};

export class ChaosObjectStore implements ObjectStore {
  readonly #inner: ObjectStore;
  readonly #policy: ChaosPolicy;
  readonly stats: ChaosStats = {ops: 0, errors: 0, errorsAfter: 0, delays: 0};

  constructor(inner: ObjectStore, policy: ChaosPolicy) {
    this.#inner = inner;
    this.#policy = policy;
  }

  async #run<T>(op: ChaosOp, key: string, fn: () => Promise<T>): Promise<T> {
    this.stats.ops++;
    let decision = this.#policy(op, key);
    while (decision.kind === 'delay') {
      this.stats.delays++;
      await sleep(decision.ms);
      decision = decision.then ?? {kind: 'pass'};
    }
    if (decision.kind === 'error') {
      this.stats.errors++;
      throw new ChaosInjectedError(op, key, 'before');
    }
    const result = await fn();
    if (decision.kind === 'error-after') {
      this.stats.errorsAfter++;
      throw new ChaosInjectedError(op, key, 'after');
    }
    return result;
  }

  put(key: string, data: Uint8Array): Promise<void> {
    return this.#run('put', key, () => this.#inner.put(key, data));
  }

  putIfAbsent(key: string, data: Uint8Array): Promise<void> {
    return this.#run('putIfAbsent', key, () =>
      this.#inner.putIfAbsent(key, data),
    );
  }

  putStreamIfAbsent(
    key: string,
    source: () => ReadableStream<Uint8Array>,
    sizeHint: number,
  ): Promise<void> {
    return this.#run('putStreamIfAbsent', key, () =>
      this.#inner.putStreamIfAbsent(key, source, sizeHint),
    );
  }

  get(key: string): Promise<Uint8Array> {
    return this.#run('get', key, () => this.#inner.get(key));
  }

  getStream(key: string): Promise<ReadableStream<Uint8Array>> {
    return this.#run('getStream', key, () => this.#inner.getStream(key));
  }

  head(key: string): Promise<ObjectMetadata | undefined> {
    return this.#run('head', key, () => this.#inner.head(key));
  }

  list(prefix: string): Promise<ObjectMetadata[]> {
    return this.#run('list', prefix, () => this.#inner.list(prefix));
  }

  delete(key: string): Promise<void> {
    return this.#run('delete', key, () => this.#inner.delete(key));
  }
}

/**
 * A small deterministic PRNG (mulberry32), so chaos runs are reproducible
 * from their seed.
 */
export function chaosRng(seed: number): () => number {
  let state = seed >>> 0;
  return () => {
    state = (state + 0x6d2b79f5) >>> 0;
    let t = state;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

export type RandomFaultsOptions = {
  rng: () => number;
  /** Probability that an operation fails before reaching the store. */
  errorRate?: number | undefined;
  /** Probability that an operation succeeds but its response is "lost". */
  errorAfterRate?: number | undefined;
  /** Probability of added latency, uniform in [minMs, maxMs]. */
  latency?: {rate: number; minMs: number; maxMs: number} | undefined;
};

/** A policy of independent random faults, in error → error-after → latency order. */
export function randomFaults(options: RandomFaultsOptions): ChaosPolicy {
  const {rng, errorRate = 0, errorAfterRate = 0, latency} = options;
  return () => {
    if (rng() < errorRate) {
      return {kind: 'error'};
    }
    if (rng() < errorAfterRate) {
      return {kind: 'error-after'};
    }
    if (latency && rng() < latency.rate) {
      return {
        kind: 'delay',
        ms: latency.minMs + rng() * (latency.maxMs - latency.minMs),
      };
    }
    return {kind: 'pass'};
  };
}

/** Fails every operation while `active()` — a sustained outage window. */
export function outage(active: () => boolean): ChaosPolicy {
  return () => (active() ? {kind: 'error'} : {kind: 'pass'});
}

/** The first non-`pass` decision wins. */
export function composePolicies(...policies: ChaosPolicy[]): ChaosPolicy {
  return (op, key) => {
    for (const policy of policies) {
      const decision = policy(op, key);
      if (decision.kind !== 'pass') {
        return decision;
      }
    }
    return {kind: 'pass'};
  };
}
