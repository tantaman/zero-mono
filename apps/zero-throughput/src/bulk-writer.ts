import {randomBytes} from 'node:crypto';
import type {BenchmarkConfig} from './config.ts';
import type {BenchmarkDB} from './db.ts';
import {nowMs, sleep} from './util.ts';

export const CEILING_TABLE = 'zero_throughput_event';

/**
 * The size of the random pool payloads are cut from. Cutting from a pool
 * costs nothing per row, but a pool this much larger than a payload keeps
 * neighbouring rows from sharing bytes -- a sliding window one byte at a
 * time would make consecutive rows near-identical, which the segment
 * compressor would collapse and the archive's byte accounting would then
 * flatter by an order of magnitude.
 */
const POOL_SLACK_BYTES = 4 * 1024 * 1024;

export type BulkWriterStats = {
  readonly startedAtMs: number;
  readonly finishedAtMs: number;
  readonly committedRows: number;
  readonly committedTransactions: number;
  readonly highestCommittedSeq: number;
  readonly txLatencyMs: readonly number[];
  readonly approxRowBytes: number;
  readonly errors: number;
};

/**
 * The load generator for the replication-ceiling runner: multi-row INSERTs
 * into the feed-append table, `statementsPerTx` of them per transaction,
 * across `writeConcurrency` connections.
 *
 * `FixedRateWriter` issues one round trip per row inside its transaction,
 * which tops out around a thousand rows/s per connection -- fine for a
 * fixed-rate SLO run, useless for finding a ceiling, where the generator
 * must stay comfortably ahead of the thing being measured. Row width and
 * transaction size are separate knobs because the replication path charges
 * for them differently: bytes flow through the archive segments, while
 * transaction boundaries cost commits in the change log and the replica.
 *
 * A `writeRate` of 0 means unthrottled -- push as hard as Postgres accepts.
 */
export class BulkWriter {
  readonly #sql: BenchmarkDB;
  readonly #config: BenchmarkConfig;
  /**
   * A pool of high-entropy characters that per-row payloads are cut from.
   * A constant payload would be free to compress, which would flatter every
   * byte-sized thing on the path -- Postgres' WAL, the change log, and above
   * all the archive's compressed segments -- so the payload is incompressible
   * by construction. Cut from one pool rather than generated per row so the
   * generator stays cheap at high rates.
   */
  readonly #pool: string;
  #payloadOffset = 0;
  #highestCommittedSeq = 0;
  #committedRows = 0;

  constructor(sql: BenchmarkDB, config: BenchmarkConfig) {
    this.#sql = sql;
    this.#config = config;
    const size = Math.max(1, config.payloadBytes);
    this.#pool = randomBytes(size + POOL_SLACK_BYTES)
      .toString('base64')
      .slice(0, size + POOL_SLACK_BYTES);
  }

  #nextPayload(): string {
    const size = Math.max(1, this.#config.payloadBytes);
    const start = this.#payloadOffset;
    this.#payloadOffset = (this.#payloadOffset + size) % POOL_SLACK_BYTES;
    return this.#pool.slice(start, start + size);
  }

  get highestCommittedSeq(): number {
    return this.#highestCommittedSeq;
  }

  get committedRows(): number {
    return this.#committedRows;
  }

  async run(durationMs: number): Promise<BulkWriterStats> {
    const {rowsPerStatement, statementsPerTx, writeRate} = this.#config;
    const concurrency = Math.max(1, this.#config.writeConcurrency);
    const rowsPerTx = rowsPerStatement * statementsPerTx;
    const startedAtMs = nowMs();
    const deadline = startedAtMs + durationMs;
    // Rows are handed out in contiguous per-transaction blocks, so a
    // replica's max(seq) tracks applied rows without a count(*) scan.
    let nextSeq = 1;
    const allocate = (count: number): number => {
      const start = nextSeq;
      nextSeq += count;
      return start;
    };
    // Throttled runs pace each worker to its share of the target rate.
    const txIntervalMs =
      writeRate > 0 ? (rowsPerTx / (writeRate / concurrency)) * 1000 : 0;

    const runWorker = async () => {
      const latencies: number[] = [];
      let rows = 0;
      let txs = 0;
      let errors = 0;
      let nextStart = startedAtMs;

      while (nowMs() < deadline) {
        if (txIntervalMs > 0) {
          const delayMs = nextStart - nowMs();
          if (delayMs > 0) {
            await sleep(delayMs);
          }
          nextStart += txIntervalMs;
        }

        const firstSeq = allocate(rowsPerTx);
        const batches: ReturnType<typeof this.rows>[] = [];
        for (let i = 0; i < statementsPerTx; i++) {
          batches.push(this.rows(firstSeq + i * rowsPerStatement));
        }
        const txStart = nowMs();
        try {
          await this.#sql.begin(async tx => {
            for (const batch of batches) {
              await tx`INSERT INTO ${tx(CEILING_TABLE)} ${tx(batch)}`;
            }
          });
        } catch {
          errors++;
          continue;
        }
        latencies.push(nowMs() - txStart);
        rows += rowsPerTx;
        txs++;
        this.#committedRows += rowsPerTx;
        const lastSeq = firstSeq + rowsPerTx - 1;
        if (lastSeq > this.#highestCommittedSeq) {
          this.#highestCommittedSeq = lastSeq;
        }
      }
      return {latencies, rows, txs, errors};
    };

    const results = await Promise.all(
      Array.from({length: concurrency}, () => runWorker()),
    );

    const txLatencyMs: number[] = [];
    let committedRows = 0;
    let committedTransactions = 0;
    let errors = 0;
    for (const r of results) {
      txLatencyMs.push(...r.latencies);
      committedRows += r.rows;
      committedTransactions += r.txs;
      errors += r.errors;
    }

    return {
      startedAtMs,
      finishedAtMs: nowMs(),
      committedRows,
      committedTransactions,
      highestCommittedSeq: this.#highestCommittedSeq,
      txLatencyMs,
      approxRowBytes: this.approxRowBytes(),
      errors,
    };
  }

  /** The `feed-append` row shape, with `written_at` left to the default. */
  private rows(firstSeq: number) {
    const rows = [];
    for (let i = 0; i < this.#config.rowsPerStatement; i++) {
      const seq = firstSeq + i;
      rows.push({
        id: `ceiling-${seq}`,
        profile: 'feed-append',
        shard: 0,
        bucket: 0,
        seq,
        payload: {p: this.#nextPayload()},
      });
    }
    return rows;
  }

  /**
   * A rough per-row wire size: the payload plus the other columns' encoded
   * width. Used only to report MB/s alongside rows/s.
   */
  private approxRowBytes(): number {
    return this.#config.payloadBytes + 96;
  }
}
