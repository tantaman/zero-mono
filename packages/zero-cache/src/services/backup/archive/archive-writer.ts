import {createReadStream, statSync, unlinkSync} from 'node:fs';
import {basename, join} from 'node:path';
import {Readable} from 'node:stream';
import type {LogContext} from '@rocicorp/logger';
import {resolver, type Resolver} from '@rocicorp/resolver';
import {max} from '../../../types/lexi-version.ts';
import type {ChangeStreamData} from '../../change-source/protocol/current/downstream.ts';
import {
  backupArchiveGaps,
  backupArchiveSegmentsUploaded,
  backupArchiveUploadErrors,
} from '../metrics.ts';
import {
  ObjectAlreadyExistsError,
  type ObjectStore,
} from '../object-store/object-store.ts';
import {
  logPrefix,
  parseSegmentKey,
  parseSegmentPartKey,
  segmentKey,
  segmentPartKey,
} from './layout.ts';
import {writeSealedSegmentFile, type SegmentHeader} from './segment-format.ts';
import {SegmentSpool, type SpoolRange} from './segment-spool.ts';

export type ArchiveWriterOptions = {
  store: ObjectStore;
  /** The lineage the archive is namespaced by. */
  replicaVersion: string;
  /**
   * The local directory for the message spool and sealed segments awaiting
   * upload. Its contents are re-derivable from the replication slot, so it
   * needs durability only across a process's own lifetime, but it must have
   * room for the spool plus the sealed segments in flight (bounded by
   * {@link maxBufferedBytes} plus the open transaction).
   */
  spoolDir: string;
  /** Uncompressed bytes at which the open segment is sealed. */
  segmentTargetBytes: number;
  /**
   * Uncompressed open-transaction bytes at which the transaction starts (or
   * grows) a part chain. Defaults to {@link segmentTargetBytes}, which is
   * the design's single-knob behavior; tests pin it independently to
   * exercise per-commit sealing and chains separately.
   */
  partTargetBytes?: number | undefined;
  /** Time after which a non-empty open segment is sealed. Bounds archive RPO. */
  sealIntervalMs: number;
  /**
   * On-disk bytes (open segment plus upload queue) beyond which
   * {@link readyForMore} applies back-pressure. Defaults to
   * {@link DEFAULT_MAX_BUFFERED_BYTES}.
   */
  maxBufferedBytes?: number | undefined;
  /** How long {@link close} waits for queued uploads. Default 5000. */
  flushTimeoutMs?: number | undefined;
  /** Base upload retry delay. Default 1000, doubling to 30s. */
  retryDelayMs?: number | undefined;
  /**
   * Invoked whenever the highest contiguous durable cursor advances. This is
   * what drives `UpstreamAcker.trackArchive`: upstream ACKs are gated on the
   * durable archive cursor.
   */
  onDurable?: ((watermark: string) => void) | undefined;
  /** Overridable for tests. */
  setTimeoutFn?: typeof setTimeout | undefined;
  clearTimeoutFn?: typeof clearTimeout | undefined;
};

export type ArchiveWriterState = {
  /** False once {@link close} has run. */
  enabled: boolean;
  /** End of the highest contiguous durable segment, if any. */
  durableWatermark: string | undefined;
  /** Last committed watermark spooled or durable (the replay filter). */
  lastBufferedWatermark: string | undefined;
  /** Open segment plus upload queue, in uncompressed message bytes on disk. */
  bufferedBytes: number;
  queuedSegments: number;
  /** Discontinuities observed at reconcile time. Should stay zero. */
  gapsDetected: number;
};

const DEFAULT_MAX_BUFFERED_BYTES = 128 * 1024 * 1024;
const DEFAULT_FLUSH_TIMEOUT_MS = 5_000;
const DEFAULT_RETRY_DELAY_MS = 1_000;
const MAX_RETRY_DELAY_MS = 30_000;

/** A sealed spool range awaiting compression and upload, in order. */
type SegmentJob = {
  range: SpoolRange;
  header: SegmentHeader;
  key: string;
  /**
   * The watermark the durable cursor advances to once this object lands;
   * undefined for interior parts, which by themselves make nothing durable.
   */
  durable: string | undefined;
};

/** The open transaction's part chain, once it has outgrown a segment. */
type PartChain = {
  /** The spanning transaction's commit watermark. */
  watermark: string;
  /** The next part number to seal. */
  nextPart: number;
};

/**
 * Archives the committed change stream, from the change-streamer's own stream
 * loop: appends each message to a local uncompressed disk spool as it
 * arrives, seals the committed range on size or time, compresses and uploads
 * sealed segments asynchronously in order, and exposes the highest
 * **contiguous** durable cursor via `onDurable`.
 *
 * ### Ordering and replay
 *
 * {@link write} is synchronous (spool appends are positional `writeSync`s;
 * sealing hands a byte range to the async pump), so buffering a transaction
 * always precedes anything that can advance the watermark the stream would
 * resume from — the same invariant the SQLite change-log writer relies on.
 * Replayed transactions after a reconnect (the stream resumes from the last
 * ACK, which can trail the archive) are filtered by commit watermark in
 * {@link write}, which is where replay suppression lives for the other
 * stream consumers too.
 *
 * ### Failure posture: fail-stall
 *
 * The archive is authoritative, so unlike the fail-soft SQLite change-log
 * writer (a cache), this writer never disables itself: a seal or upload
 * failure retries indefinitely while the durable cursor — and with it,
 * upstream ACKs — stops advancing, and {@link readyForMore} applies
 * back-pressure once the on-disk buffer fills. The stalled cursor is the
 * alerting signal. A stream-invariant violation in {@link write} throws,
 * which the stream loop turns into a reconnect-and-reconcile. Segment names
 * are deterministic from stream identity and cursor interval, so a retried
 * upload that finds its object already present (a crash between upload and
 * ACK in a previous incarnation) is treated as durable rather than an error.
 *
 * ### Memory: spool, don't buffer
 *
 * Resident memory is O(bounded buffer), never O(transaction) or O(segment):
 * messages spool to disk uncompressed (a rollback is an `ftruncate` back to
 * the last committed offset), sealing compresses the spooled range in a
 * streaming pass into a sealed local file — the upload/retry unit — and the
 * upload streams that file to the store. Disk holds the spool plus sealed
 * files, both bounded by back-pressure plus the open transaction, and both
 * re-derivable from the replication slot after a crash.
 *
 * ### Transactions larger than a segment: part chains
 *
 * A transaction that outgrows the segment target must not force an
 * unbounded segment (or spool range), so its spooled prefix seals as
 * interior parts as it grows and its commit seals the chain's final part.
 * Interior parts upload in order but advance nothing: only the final part
 * carries a durable watermark, so a crash or rollback mid-chain re-sends
 * the whole transaction — exactly as an unsealed segment would — and the
 * abandoned parts are debris that reconcile (and GC) reclaim. Interior
 * names embed the transaction's commit watermark, keeping retries
 * idempotent and debris collision-free.
 */
export class ArchiveWriter {
  readonly #lc: LogContext;
  readonly #opts: ArchiveWriterOptions;
  readonly #setTimeout: typeof setTimeout;
  readonly #clearTimeout: typeof clearTimeout;
  readonly #maxBufferedBytes: number;

  #spool: SegmentSpool | undefined;
  #segmentStart: string | undefined;
  /** Last commit watermark in the open segment; undefined when it is empty. */
  #segmentEnd: string | undefined;
  #segmentTxCount = 0;
  #segmentFirstCommitTimeMs: number | null = null;
  #segmentLastCommitTimeMs: number | null = null;
  #lastBuffered: string | undefined;
  #durable: string | undefined;

  #openTxWatermark: string | undefined;
  #skipCurrentTx = false;
  #chain: PartChain | undefined;

  #sealTimer: ReturnType<typeof setTimeout> | undefined;

  #queue: SegmentJob[] = [];
  #queuedBytes = 0;
  #inFlight: SegmentJob | undefined;
  #pumping = false;
  #idle: Resolver<void> | undefined;
  #backpressure: Resolver<void> | undefined;

  #closed = false;
  #gapsDetected = 0;

  constructor(lc: LogContext, opts: ArchiveWriterOptions) {
    this.#lc = lc.withContext('component', 'archive-writer');
    this.#opts = opts;
    this.#setTimeout = opts.setTimeoutFn ?? setTimeout;
    this.#clearTimeout = opts.clearTimeoutFn ?? clearTimeout;
    this.#maxBufferedBytes =
      opts.maxBufferedBytes ?? DEFAULT_MAX_BUFFERED_BYTES;
  }

  /** False once {@link close} has run. */
  get enabled(): boolean {
    return !this.#closed;
  }

  /** Exposed for tests and for the gauges the server wiring registers. */
  state(): ArchiveWriterState {
    return {
      enabled: !this.#closed,
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
   * watermark the stream will resume from. A listing failure propagates: the
   * stream loop treats it like any other stream error and retries with
   * backoff (fail-stall).
   *
   * A resume point past the durable head is a gap the archive can never fill
   * (the changes below it will not be re-sent). ACK gating makes that
   * unreachable in normal operation, so it is reported as a gap — the
   * should-be-zero health signal — rather than failed on. An empty archive
   * whose stream resumes past the lineage origin is the normal
   * first-enablement case: the lineage simply starts at the resume point.
   */
  async reconcile(resumeWatermark: string): Promise<void> {
    if (this.#closed) {
      return;
    }
    const {store, replicaVersion} = this.#opts;
    const objects = await store.list(logPrefix(replicaVersion));
    // Continuity considers ordinary/final segment names only: an interior
    // part never advances the durable cursor, so a chain with no final part
    // is re-sent work, not a gap.
    let gaps = 0;
    let prev: string | undefined;
    const finals = new Set<string>();
    for (const {key} of objects) {
      const segment = parseSegmentKey(replicaVersion, key);
      if (segment === undefined) {
        continue;
      }
      finals.add(`${segment.start}/${segment.end}`);
      if (prev !== undefined && segment.start !== prev) {
        gaps++;
      }
      prev = segment.end;
    }
    const head = prev;

    // Interior parts whose chain never completed are debris from an
    // interrupted transaction that will be re-sent. Delete them now so the
    // re-sent transaction can re-seal under any part boundaries — the
    // segment target may have changed across restarts, and a stale part
    // under the same deterministic name must not shadow the re-sealed one.
    const staleParts = objects
      .map(({key}) => parseSegmentPartKey(replicaVersion, key))
      .filter(part => part !== undefined)
      .filter(part => !finals.has(`${part.start}/${part.watermark}`));
    for (const part of staleParts) {
      await store.delete(part.key);
    }
    if (staleParts.length > 0) {
      this.#lc.info?.(
        `deleted ${staleParts.length} interior part(s) of incomplete ` +
          `transaction chains; their transactions will be re-sent`,
      );
    }
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
    if (this.#spool === undefined) {
      this.#spool = new SegmentSpool(this.#opts.spoolDir);
    } else {
      // The stream re-sends everything that was not sealed, so the open
      // segment's un-sealed spool content would duplicate it. Segments
      // already sealed (in the upload queue) keep going: their ranges lie
      // below the discarded region. abort() also cancels an open chain's
      // queued interior parts.
      this.abort();
      this.#spool.discardSegment();
    }
    this.#segmentTxCount = 0;
    this.#segmentEnd = undefined;
    this.#segmentFirstCommitTimeMs = null;
    this.#segmentLastCommitTimeMs = null;
    this.#segmentStart =
      head !== undefined ? max(head, resumeWatermark) : resumeWatermark;
    this.#lastBuffered = this.#segmentStart;
    if (head !== undefined && head !== this.#durable) {
      this.#durable = head;
      this.#opts.onDurable?.(head);
    }
  }

  /**
   * Spools one stream message, sealing at transaction boundaries when the
   * open segment reaches its size target. Throws only on a stream-invariant
   * violation (a message before {@link reconcile}, outside a begin..commit
   * envelope, or a spool append failure such as a full disk), which the
   * stream loop turns into a reconnect-and-reconcile.
   */
  write(change: ChangeStreamData, json: string): void {
    if (this.#closed) {
      return;
    }
    const spool = this.#spool;
    if (this.#segmentStart === undefined || spool === undefined) {
      throw new Error('archive writer received a change before reconcile()');
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
        if (this.#openTxWatermark !== undefined) {
          // A begin inside an open transaction: the interrupted
          // transaction's partial spool content is discarded.
          this.abort();
        }
        this.#openTxWatermark = watermark;
        spool.append(json);
        break;
      }
      case 'commit': {
        if (this.#skipCurrentTx) {
          this.#skipCurrentTx = false;
          return;
        }
        const watermark = this.#openTxWatermark;
        if (watermark === undefined) {
          throw new Error(`commit ${change[2].watermark} without a begin`);
        }
        spool.append(json);
        spool.commit();
        this.#openTxWatermark = undefined;
        this.#lastBuffered = watermark;
        const commitTimeMs = change[1].commitTimeMs ?? null;
        if (this.#chain !== undefined) {
          // The transaction spanned interior parts; its commit seals the
          // chain's final part immediately (a chain never shares an object
          // with other transactions). A seal failure here throws, which the
          // stream loop turns into a reconnect-and-reconcile — the chain is
          // then abandoned and the transaction re-sent.
          this.#sealFinalPart(spool, this.#chain, watermark, commitTimeMs);
          break;
        }
        this.#segmentEnd = watermark;
        this.#segmentTxCount++;
        if (this.#segmentTxCount === 1) {
          this.#segmentFirstCommitTimeMs = commitTimeMs;
        }
        this.#segmentLastCommitTimeMs = commitTimeMs;
        if (spool.committedBytes >= this.#opts.segmentTargetBytes) {
          this.#seal();
        } else if (this.#sealTimer === undefined) {
          this.#sealTimer = this.#setTimeout(() => {
            this.#sealTimer = undefined;
            this.#seal();
          }, this.#opts.sealIntervalMs);
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
        if (this.#openTxWatermark === undefined) {
          throw new Error(`data message without a begin`);
        }
        spool.append(json);
        this.#maybeSealParts(spool);
        break;
    }
  }

  /**
   * Seals interior parts once the open transaction outgrows the segment
   * target, so no segment object — and no spool range — is ever unbounded.
   * The first part is preceded by sealing any committed transactions as an
   * ordinary segment: a chain contains exactly one transaction. Failures
   * throw (from `write`), which the stream loop turns into a
   * reconnect-and-reconcile.
   */
  #maybeSealParts(spool: SegmentSpool): void {
    const target = this.#opts.partTargetBytes ?? this.#opts.segmentTargetBytes;
    if (spool.openTxBytes < target) {
      return;
    }
    const watermark = this.#openTxWatermark;
    const start = this.#segmentStart;
    if (watermark === undefined || start === undefined) {
      return; // unreachable: only called mid-transaction
    }
    if (this.#chain === undefined) {
      if (spool.committedBytes > 0) {
        this.#seal();
        if (spool.committedBytes > 0) {
          // The seal failed (and logged); leave the committed transactions
          // in place and retry on the next append rather than starting a
          // chain that would mix them into an interior part.
          return;
        }
      }
      this.#chain = {watermark, nextPart: 1};
    }
    this.#sealInteriorPart(spool, this.#chain);
  }

  #sealInteriorPart(spool: SegmentSpool, chain: PartChain): void {
    const {replicaVersion} = this.#opts;
    const start = this.#segmentStart;
    const range = spool.sealOpenTail();
    if (start === undefined || range === undefined) {
      return; // unreachable: only called with an open, non-empty tail
    }
    this.#queue.push({
      range,
      key: segmentPartKey(
        replicaVersion,
        start,
        chain.watermark,
        chain.nextPart,
      ),
      header: {
        replicaVersion,
        start,
        end: null,
        txCount: 0,
        firstCommitTimeMs: null,
        lastCommitTimeMs: null,
        part: {
          number: chain.nextPart,
          final: false,
          watermark: chain.watermark,
        },
      },
      durable: undefined,
    });
    this.#queuedBytes += range.bytes;
    chain.nextPart++;
    void this.#pump();
  }

  #sealFinalPart(
    spool: SegmentSpool,
    chain: PartChain,
    watermark: string,
    commitTimeMs: number | null,
  ): void {
    const {replicaVersion} = this.#opts;
    const start = this.#segmentStart;
    const range = spool.sealCommitted();
    if (start === undefined || range === undefined) {
      return; // unreachable: the commit was just appended
    }
    this.#queue.push({
      range,
      key: segmentKey(replicaVersion, start, watermark),
      header: {
        replicaVersion,
        start,
        end: watermark,
        txCount: 1,
        firstCommitTimeMs: commitTimeMs,
        lastCommitTimeMs: commitTimeMs,
        part: {number: chain.nextPart, final: true, watermark},
      },
      durable: watermark,
    });
    this.#queuedBytes += range.bytes;
    this.#segmentStart = watermark;
    this.#segmentEnd = undefined;
    this.#segmentTxCount = 0;
    this.#segmentFirstCommitTimeMs = null;
    this.#segmentLastCommitTimeMs = null;
    this.#chain = undefined;
    void this.#pump();
  }

  /**
   * Discards the open transaction, if any — one `ftruncate` back to the last
   * committed spool offset, or, when the transaction has already sealed
   * interior parts, an abandonment of its chain: queued interior parts are
   * cancelled (an in-flight upload completes as harmless, reclaimable
   * debris) and the spool rotates to a fresh file so nothing under an
   * in-flight reader is truncated. Called when the change stream is
   * interrupted mid-transaction.
   */
  abort(): void {
    this.#openTxWatermark = undefined;
    this.#skipCurrentTx = false;
    const chain = this.#chain;
    if (chain !== undefined) {
      this.#chain = undefined;
      this.#queue = this.#queue.filter(job => {
        if (
          job === this.#inFlight ||
          job.header.part === null ||
          job.header.part.watermark !== chain.watermark
        ) {
          return true;
        }
        this.#queuedBytes -= job.range.bytes;
        job.range.release();
        return false;
      });
      this.#spool?.abandonToFreshFile();
      return;
    }
    this.#spool?.rollback();
  }

  /**
   * Back-pressure for the stream loop, in the manner of the storer's
   * `readyForMore()`: `undefined` while there is room, otherwise a promise
   * that resolves when queued uploads drain. This is the fail-stall bound on
   * disk while uploads lag: replication waits for the archive rather than
   * outrunning it.
   */
  readyForMore(): Promise<void> | undefined {
    if (this.#closed || this.#bufferedBytes() <= this.#maxBufferedBytes) {
      return undefined;
    }
    this.#backpressure ??= resolver();
    return this.#backpressure.promise;
  }

  /**
   * Seals any spooled transactions and waits (bounded) for queued uploads,
   * then closes the writer. An unfinished upload at timeout is safe to
   * abandon: the un-ACKed transactions will be re-sent to the next
   * incarnation, and the deterministic segment name makes a
   * concurrently-landed upload an idempotent no-op.
   */
  async close(): Promise<void> {
    if (this.#closed) {
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
    this.#closed = true;
    this.#clearSealTimer();
    this.#backpressure?.resolve();
    this.#backpressure = undefined;
    this.#spool?.close();
  }

  #bufferedBytes(): number {
    return (this.#spool?.segmentBytes ?? 0) + this.#queuedBytes;
  }

  #clearSealTimer(): void {
    if (this.#sealTimer !== undefined) {
      this.#clearTimeout(this.#sealTimer);
      this.#sealTimer = undefined;
    }
  }

  #seal(): void {
    this.#clearSealTimer();
    const spool = this.#spool;
    const start = this.#segmentStart;
    const end = this.#segmentEnd;
    if (
      spool === undefined ||
      start === undefined ||
      end === undefined ||
      this.#segmentTxCount === 0
    ) {
      return;
    }
    try {
      const range = spool.sealCommitted();
      if (range === undefined) {
        return;
      }
      this.#queue.push({
        range,
        key: segmentKey(this.#opts.replicaVersion, start, end),
        header: {
          replicaVersion: this.#opts.replicaVersion,
          start,
          end,
          txCount: this.#segmentTxCount,
          firstCommitTimeMs: this.#segmentFirstCommitTimeMs,
          lastCommitTimeMs: this.#segmentLastCommitTimeMs,
          part: null,
        },
        durable: end,
      });
      this.#queuedBytes += range.bytes;
      this.#segmentStart = end;
      this.#segmentEnd = undefined;
      this.#segmentTxCount = 0;
      this.#segmentFirstCommitTimeMs = null;
      this.#segmentLastCommitTimeMs = null;
      void this.#pump();
    } catch (e) {
      // Fail-stall, and never throw: sealing can run from the timer, where a
      // throw would be an unhandled exception. The committed range stays in
      // the spool — the next seal retries — while the stalled durable cursor
      // (and therefore stalled ACKs) is the alerting signal.
      this.#lc.error?.(
        `error sealing an archive segment; the durable archive cursor is stalled`,
        e,
      );
    }
  }

  async #pump(): Promise<void> {
    if (this.#pumping) {
      return;
    }
    this.#pumping = true;
    try {
      while (this.#queue.length > 0 && !this.#closed) {
        const job = this.#queue[0];
        this.#inFlight = job;
        const uploaded = await this.#sealAndUpload(job);
        this.#inFlight = undefined;
        if (!uploaded) {
          return; // closed
        }
        this.#queue.shift();
        this.#queuedBytes -= job.range.bytes;
        job.range.release();
        if (job.durable !== undefined) {
          this.#durable = job.durable;
          this.#opts.onDurable?.(job.durable);
        }
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
      this.#inFlight = undefined;
      this.#idle?.resolve();
      this.#idle = undefined;
    }
  }

  /**
   * Compresses the job's spool range into a sealed local file (a streaming
   * pass) and streams that file to the store, retrying both indefinitely
   * with backoff. The sealed file is rebuilt on retry — it may be partial
   * after a failure — and deleted once the object is durable.
   */
  async #sealAndUpload(job: SegmentJob): Promise<boolean> {
    const {store, spoolDir} = this.#opts;
    const key = job.key;
    const sealedPath = join(spoolDir, `${basename(key)}.sealed`);
    const baseDelay = this.#opts.retryDelayMs ?? DEFAULT_RETRY_DELAY_MS;
    for (let attempt = 1; ; attempt++) {
      if (this.#closed) {
        return false;
      }
      try {
        await writeSealedSegmentFile(
          job.header,
          () => job.range.createStream(),
          sealedPath,
        );
        const {size} = statSync(sealedPath);
        await store.putStreamIfAbsent(
          key,
          () =>
            Readable.toWeb(
              createReadStream(sealedPath),
            ) as ReadableStream<Uint8Array>,
          size,
        );
        backupArchiveSegmentsUploaded().add(1, {result: 'uploaded'});
        this.#deleteSealed(sealedPath);
        return true;
      } catch (e) {
        if (e instanceof ObjectAlreadyExistsError) {
          // A previous incarnation crashed between upload and ACK; the
          // deterministic name makes this retry durable by definition.
          backupArchiveSegmentsUploaded().add(1, {result: 'exists'});
          this.#deleteSealed(sealedPath);
          return true;
        }
        backupArchiveUploadErrors().add(1);
        const delay = Math.min(
          baseDelay * 2 ** (attempt - 1),
          MAX_RETRY_DELAY_MS,
        );
        this.#lc.warn?.(
          `error sealing/uploading ${key} (attempt ${attempt}); ` +
            `retrying in ${delay}ms. The durable archive cursor is stalled.`,
          e,
        );
        await new Promise(resolve => this.#setTimeout(resolve, delay));
      }
    }
  }

  #deleteSealed(sealedPath: string): void {
    try {
      unlinkSync(sealedPath);
    } catch (e) {
      this.#lc.warn?.(`error deleting sealed segment ${sealedPath}`, e);
    }
  }
}
