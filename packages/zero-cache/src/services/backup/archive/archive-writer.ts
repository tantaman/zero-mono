import type {LogContext} from '@rocicorp/logger';
import {resolver, type Resolver} from '@rocicorp/resolver';
import {max} from '../../../types/lexi-version.ts';
import type {ChangeStreamData} from '../../change-source/protocol/current/downstream.ts';
import {
  backupArchiveGaps,
  backupArchiveSegmentsUploaded,
  backupArchiveUploadErrors,
  backupArchiveWriterDisabled,
} from '../metrics.ts';
import {
  ObjectAlreadyExistsError,
  type ObjectStore,
} from '../object-store/object-store.ts';
import {logPrefix, parseSegmentKey, segmentKey} from './layout.ts';
import {encodeSegment} from './segment-format.ts';

export type ArchiveWriterOptions = {
  store: ObjectStore;
  /** The lineage the archive is namespaced by. */
  replicaVersion: string;
  /**
   * In mode `archive` the writer is authoritative and must **fail-stall**: an
   * upload failure retries indefinitely, the durable cursor stops advancing
   * (and with it, upstream ACKs), and {@link readyForMore} applies
   * back-pressure once the buffer fills. In mode `archive-dual` litestream
   * remains authoritative, so the writer instead fails soft like the SQLite
   * change-log writer: it disables itself and lets replication continue,
   * degrading only the dual-run metrics.
   */
  authoritative: boolean;
  /** Uncompressed bytes at which the open segment is sealed. */
  segmentTargetBytes: number;
  /** Time after which a non-empty open segment is sealed. Bounds archive RPO. */
  sealIntervalMs: number;
  /**
   * Bytes buffered (open segment plus upload queue) beyond which
   * {@link readyForMore} applies back-pressure (authoritative) or the writer
   * fails soft (dual). Defaults to {@link DEFAULT_MAX_BUFFERED_BYTES}.
   */
  maxBufferedBytes?: number | undefined;
  /** How long {@link close} waits for queued uploads. Default 5000. */
  flushTimeoutMs?: number | undefined;
  /** Base upload retry delay. Default 1000, doubling to 30s. */
  retryDelayMs?: number | undefined;
  /**
   * Invoked whenever the highest contiguous durable cursor advances. This is
   * what drives `UpstreamAcker.trackArchive` in mode `archive`, and the
   * dual-run cursor metrics in mode `archive-dual`.
   */
  onDurable?: ((watermark: string) => void) | undefined;
  /** Invoked when the (non-authoritative) writer fails soft. */
  onDisabled?: (() => void) | undefined;
  /** Overridable for tests. */
  setTimeoutFn?: typeof setTimeout | undefined;
  clearTimeoutFn?: typeof clearTimeout | undefined;
};

export type ArchiveWriterState = {
  enabled: boolean;
  /** End of the highest contiguous durable segment, if any. */
  durableWatermark: string | undefined;
  /** Last committed watermark buffered or durable (the replay filter). */
  lastBufferedWatermark: string | undefined;
  /** Open segment plus upload queue, in uncompressed message bytes. */
  bufferedBytes: number;
  queuedSegments: number;
  /** Discontinuities observed at reconcile time. Should stay zero. */
  gapsDetected: number;
};

const DEFAULT_MAX_BUFFERED_BYTES = 128 * 1024 * 1024;
const DEFAULT_FLUSH_TIMEOUT_MS = 5_000;
const DEFAULT_RETRY_DELAY_MS = 1_000;
const MAX_RETRY_DELAY_MS = 30_000;
/** Dual-mode upload attempts before failing soft. */
const MAX_NON_AUTHORITATIVE_ATTEMPTS = 5;

type BufferedTransaction = {
  watermark: string;
  messages: string[];
};

type SealedSegment = {
  key: string;
  data: Uint8Array;
  end: string;
  /** Uncompressed bytes, released from the buffer accounting when durable. */
  bytes: number;
};

/**
 * Archives the committed change stream, from the change-streamer's own stream
 * loop: buffers each transaction's messages, appends committed transactions
 * to the open segment, seals on size or time, uploads asynchronously in
 * order, and exposes the highest **contiguous** durable cursor via
 * `onDurable`.
 *
 * ### Ordering and replay
 *
 * {@link write} is synchronous (sealing compresses in-line; uploads are
 * async), so buffering a transaction always precedes anything that can
 * advance the watermark the stream would resume from — the same invariant
 * the SQLite change-log writer relies on. Replayed transactions after a
 * reconnect (the stream resumes from the last ACK, which can trail the
 * archive) are filtered by commit watermark in {@link write}, which is where
 * replay suppression lives for the other stream consumers too.
 *
 * ### Failure posture
 *
 * See {@link ArchiveWriterOptions.authoritative}. Segment names are
 * deterministic from stream identity and cursor interval, so a retried
 * upload that finds its object already present (a crash between upload and
 * ACK in a previous incarnation) is treated as durable rather than an error.
 *
 * ### Memory
 *
 * This implementation buffers the open segment and upload queue in memory,
 * bounded only by back-pressure (authoritative) or fail-soft (dual) — which
 * violates the streaming discipline in ZERO_BACKUP_ARCHIVE_MODE_DESIGN.md
 * ("never O(transaction)") for transactions approaching the buffer cap. The
 * planned retrofit replaces the buffers with a disk spool (streaming
 * compression, truncate-on-rollback, streaming upload) and allows oversized
 * transactions to span segment parts; until it lands, do not expose
 * archive-dual to workloads with transactions near {@link
 * ArchiveWriterOptions.maxBufferedBytes}.
 */
export class ArchiveWriter {
  readonly #lc: LogContext;
  readonly #opts: ArchiveWriterOptions;
  readonly #setTimeout: typeof setTimeout;
  readonly #clearTimeout: typeof clearTimeout;
  readonly #maxBufferedBytes: number;

  #segmentStart: string | undefined;
  #lastBuffered: string | undefined;
  #durable: string | undefined;

  #currentTx: BufferedTransaction | undefined;
  #currentTxBytes = 0;
  #skipCurrentTx = false;

  #pending: BufferedTransaction[] = [];
  #pendingBytes = 0;
  #sealTimer: ReturnType<typeof setTimeout> | undefined;

  #queue: SealedSegment[] = [];
  #queuedBytes = 0;
  #pumping = false;
  #idle: Resolver<void> | undefined;
  #backpressure: Resolver<void> | undefined;

  #disabled = false;
  #gapsDetected = 0;

  constructor(lc: LogContext, opts: ArchiveWriterOptions) {
    this.#lc = lc.withContext('component', 'archive-writer');
    this.#opts = opts;
    this.#setTimeout = opts.setTimeoutFn ?? setTimeout;
    this.#clearTimeout = opts.clearTimeoutFn ?? clearTimeout;
    this.#maxBufferedBytes =
      opts.maxBufferedBytes ?? DEFAULT_MAX_BUFFERED_BYTES;
  }

  /** False once the (non-authoritative) writer has failed soft or closed. */
  get enabled(): boolean {
    return !this.#disabled;
  }

  /** Exposed for tests and for the gauges the server wiring registers. */
  state(): ArchiveWriterState {
    return {
      enabled: !this.#disabled,
      durableWatermark: this.#durable,
      lastBufferedWatermark: this.#lastBuffered,
      bufferedBytes: this.#bufferedBytes(),
      queuedSegments: this.#queue.length,
      gapsDetected: this.#gapsDetected,
    };
  }

  /**
   * Lists the lineage's segments, establishes the durable head and the next
   * segment's start, and records any discontinuity. Runs once per stream
   * connection, before any {@link write} — the change-streamer supplies the
   * watermark the stream will resume from.
   *
   * A resume point past the durable head is a gap the archive can never fill
   * (the changes below it will not be re-sent). In authoritative mode that
   * cannot happen — ACKs are gated on the durable cursor — so it is reported
   * as a gap in both modes rather than failed on. An empty archive whose
   * stream resumes past the lineage origin is the normal first-enablement
   * case: the lineage simply starts at the resume point.
   */
  async reconcile(resumeWatermark: string): Promise<void> {
    if (this.#disabled) {
      return;
    }
    const {store, replicaVersion} = this.#opts;
    let head: string | undefined;
    try {
      const objects = await store.list(logPrefix(replicaVersion));
      let gaps = 0;
      let prev: string | undefined;
      for (const {key} of objects) {
        const segment = parseSegmentKey(replicaVersion, key);
        if (segment === undefined) {
          continue;
        }
        if (prev !== undefined && segment.start !== prev) {
          gaps++;
        }
        prev = segment.end;
      }
      head = prev;
      if (gaps > 0) {
        this.#lc.error?.(
          `the archive for ${replicaVersion} has ${gaps} gap(s) in its segment chain`,
        );
        this.#gapsDetected += gaps;
        backupArchiveGaps().add(gaps);
      }
      if (head !== undefined && resumeWatermark > head) {
        this.#lc.error?.(
          `stream resumes at ${resumeWatermark}, past the durable archive ` +
            `head ${head}: the range in between is not archived`,
        );
        this.#gapsDetected++;
        backupArchiveGaps().add(1);
      }
    } catch (e) {
      if (!this.#opts.authoritative) {
        this.#failSoft('listing the archive', e);
        return;
      }
      // Fail-stall: the stream loop treats this like any other stream error
      // and retries with backoff.
      throw e;
    }
    this.#segmentStart =
      head !== undefined ? max(head, resumeWatermark) : resumeWatermark;
    this.#lastBuffered = this.#segmentStart;
    if (head !== undefined && head !== this.#durable) {
      this.#durable = head;
      this.#opts.onDurable?.(head);
    }
  }

  /**
   * Buffers one stream message, sealing at transaction boundaries when the
   * open segment reaches its size target. Never throws in non-authoritative
   * mode; a failure disables the writer instead of failing the stream.
   */
  write(change: ChangeStreamData, json: string): void {
    if (this.#disabled) {
      return;
    }
    if (this.#segmentStart === undefined) {
      // reconcile() must establish the segment start before any write.
      this.#handleFailure(
        'writing to the archive',
        new Error('archive writer received a change before reconcile()'),
      );
      return;
    }
    const [type] = change;
    switch (type) {
      case 'begin': {
        const watermark = change[2].commitWatermark;
        if (
          this.#lastBuffered !== undefined &&
          watermark <= this.#lastBuffered
        ) {
          // A replay of a transaction this archive already covers.
          this.#skipCurrentTx = true;
          return;
        }
        this.#currentTx = {watermark, messages: [json]};
        this.#currentTxBytes = json.length;
        break;
      }
      case 'commit': {
        if (this.#skipCurrentTx) {
          this.#skipCurrentTx = false;
          return;
        }
        const tx = this.#currentTx;
        if (tx === undefined) {
          this.#handleFailure(
            'writing to the archive',
            new Error(`commit ${change[2].watermark} without a begin`),
          );
          return;
        }
        tx.messages.push(json);
        this.#currentTx = undefined;
        this.#pending.push(tx);
        this.#pendingBytes += this.#currentTxBytes + json.length;
        this.#currentTxBytes = 0;
        this.#lastBuffered = tx.watermark;
        if (this.#pendingBytes >= this.#opts.segmentTargetBytes) {
          this.#seal();
        } else if (this.#sealTimer === undefined) {
          this.#sealTimer = this.#setTimeout(() => {
            this.#sealTimer = undefined;
            this.#seal();
          }, this.#opts.sealIntervalMs);
        }
        if (
          !this.#opts.authoritative &&
          this.#bufferedBytes() > this.#maxBufferedBytes
        ) {
          this.#failSoft(
            'buffering the archive',
            new Error(
              `buffered bytes exceeded ${this.#maxBufferedBytes} while uploads lag`,
            ),
          );
        }
        break;
      }
      case 'rollback':
        this.abort();
        break;
      default:
        if (this.#skipCurrentTx) {
          return;
        }
        if (this.#currentTx === undefined) {
          this.#handleFailure(
            'writing to the archive',
            new Error(`data message without a begin`),
          );
          return;
        }
        this.#currentTx.messages.push(json);
        this.#currentTxBytes += json.length;
        break;
    }
  }

  /**
   * Discards the open transaction, if any. Called when the change stream is
   * interrupted mid-transaction.
   */
  abort(): void {
    this.#currentTx = undefined;
    this.#currentTxBytes = 0;
    this.#skipCurrentTx = false;
  }

  /**
   * Back-pressure for the stream loop, in the manner of the storer's
   * `readyForMore()`: `undefined` while there is room, otherwise a promise
   * that resolves when queued uploads drain. Only the authoritative writer
   * ever exerts back-pressure; the dual writer fails soft instead so that it
   * cannot affect replication.
   */
  readyForMore(): Promise<void> | undefined {
    if (
      !this.#opts.authoritative ||
      this.#disabled ||
      this.#bufferedBytes() <= this.#maxBufferedBytes
    ) {
      return undefined;
    }
    this.#backpressure ??= resolver();
    return this.#backpressure.promise;
  }

  /**
   * Seals any buffered transactions and waits (bounded) for queued uploads,
   * then disables the writer. In authoritative mode an unfinished upload at
   * timeout is safe to abandon: the un-ACKed transactions will be re-sent to
   * the next incarnation, and the deterministic segment name makes a
   * concurrently-landed upload an idempotent no-op.
   */
  async close(): Promise<void> {
    if (this.#disabled) {
      return;
    }
    this.abort();
    this.#seal();
    const flushTimeout = this.#opts.flushTimeoutMs ?? DEFAULT_FLUSH_TIMEOUT_MS;
    if (this.#pumping || this.#queue.length > 0) {
      this.#idle ??= resolver();
      const timeout = resolver<void>();
      const handle = this.#setTimeout(() => timeout.resolve(), flushTimeout);
      await Promise.race([this.#idle.promise, timeout.promise]);
      this.#clearTimeout(handle);
    }
    this.#disabled = true;
    this.#clearSealTimer();
    this.#backpressure?.resolve();
    this.#backpressure = undefined;
  }

  #bufferedBytes(): number {
    return this.#currentTxBytes + this.#pendingBytes + this.#queuedBytes;
  }

  #clearSealTimer(): void {
    if (this.#sealTimer !== undefined) {
      this.#clearTimeout(this.#sealTimer);
      this.#sealTimer = undefined;
    }
  }

  #seal(): void {
    this.#clearSealTimer();
    if (this.#pending.length === 0 || this.#segmentStart === undefined) {
      return;
    }
    const {replicaVersion} = this.#opts;
    const start = this.#segmentStart;
    try {
      const {data, end} = encodeSegment({
        replicaVersion,
        start,
        transactions: this.#pending,
      });
      this.#queue.push({
        key: segmentKey(replicaVersion, start, end),
        data,
        end,
        bytes: this.#pendingBytes,
      });
      this.#queuedBytes += this.#pendingBytes;
      this.#segmentStart = end;
      this.#pending = [];
      this.#pendingBytes = 0;
      void this.#pump();
    } catch (e) {
      this.#handleFailure('sealing an archive segment', e);
    }
  }

  async #pump(): Promise<void> {
    if (this.#pumping) {
      return;
    }
    this.#pumping = true;
    try {
      while (this.#queue.length > 0 && !this.#disabled) {
        const segment = this.#queue[0];
        const uploaded = await this.#upload(segment);
        if (!uploaded) {
          return; // failed soft (non-authoritative) or closed
        }
        this.#queue.shift();
        this.#queuedBytes -= segment.bytes;
        this.#durable = segment.end;
        this.#opts.onDurable?.(segment.end);
        if (
          this.#backpressure !== undefined &&
          this.#bufferedBytes() <= this.#maxBufferedBytes / 2
        ) {
          this.#backpressure.resolve();
          this.#backpressure = undefined;
        }
      }
    } finally {
      this.#pumping = false;
      this.#idle?.resolve();
      this.#idle = undefined;
    }
  }

  async #upload(segment: SealedSegment): Promise<boolean> {
    const {store, authoritative} = this.#opts;
    const baseDelay = this.#opts.retryDelayMs ?? DEFAULT_RETRY_DELAY_MS;
    for (let attempt = 1; ; attempt++) {
      if (this.#disabled) {
        return false;
      }
      try {
        await store.putIfAbsent(segment.key, segment.data);
        backupArchiveSegmentsUploaded().add(1, {result: 'uploaded'});
        return true;
      } catch (e) {
        if (e instanceof ObjectAlreadyExistsError) {
          // A previous incarnation crashed between upload and ACK; the
          // deterministic name makes this retry durable by definition.
          backupArchiveSegmentsUploaded().add(1, {result: 'exists'});
          return true;
        }
        backupArchiveUploadErrors().add(1);
        if (!authoritative && attempt >= MAX_NON_AUTHORITATIVE_ATTEMPTS) {
          this.#failSoft(`uploading ${segment.key}`, e);
          return false;
        }
        const delay = Math.min(
          baseDelay * 2 ** (attempt - 1),
          MAX_RETRY_DELAY_MS,
        );
        this.#lc.warn?.(
          `error uploading ${segment.key} (attempt ${attempt}); ` +
            `retrying in ${delay}ms. The durable archive cursor is stalled.`,
          e,
        );
        await new Promise(resolve => this.#setTimeout(resolve, delay));
      }
    }
  }

  #handleFailure(what: string, e: unknown): void {
    if (this.#opts.authoritative) {
      // Fail-stall: surface loudly, stop making progress, and let the durable
      // cursor (and therefore upstream ACKs) stop advancing. The stalled
      // cursor is the alerting signal; replication itself continues so that
      // serving replicas stay fresh.
      this.#lc.error?.(
        `error ${what}; the durable archive cursor is stalled`,
        e,
      );
      return;
    }
    this.#failSoft(what, e);
  }

  /**
   * Logs, disables the writer, and drops all buffered state, leaving
   * replication untouched. Like the SQLite change-log writer, disabling is
   * for the life of the process.
   */
  #failSoft(what: string, e: unknown): void {
    if (this.#disabled) {
      return;
    }
    this.#lc.error?.(
      `error ${what}: disabling the archive writer. Replication continues; ` +
        `litestream remains authoritative and dual-run coverage is lost ` +
        `until this task restarts.`,
      e,
    );
    backupArchiveWriterDisabled().add(1);
    this.#disabled = true;
    this.#clearSealTimer();
    this.abort();
    this.#pending = [];
    this.#pendingBytes = 0;
    this.#queue = [];
    this.#queuedBytes = 0;
    this.#backpressure?.resolve();
    this.#backpressure = undefined;
    this.#idle?.resolve();
    this.#idle = undefined;
    this.#opts.onDisabled?.();
  }
}
