import {existsSync, rmSync, writeFileSync} from 'node:fs';
import {mkdir} from 'node:fs/promises';
import type {LogContext} from '@rocicorp/logger';
import type {LogConfig} from '../../../../shared/src/logging.ts';
import {Database} from '../../../../zqlite/src/db.ts';
import {deleteLiteDB} from '../../db/delete-lite-db.ts';
import {StatementRunner} from '../../db/statements.ts';
import {getPragmaConfig} from '../../workers/replicator.ts';
import {deleteChangeLogDB} from '../replicator/change-log-db.ts';
import type {ChangeProcessorMode} from '../replicator/change-processor.ts';
import {IncrementalSyncer} from '../replicator/incremental-sync.ts';
import {getSubscriptionState} from '../replicator/schema/replication-state.ts';
import {
  ThreadWriteWorkerClient,
  type PragmaConfig,
  type WriteWorkerClient,
} from '../replicator/write-worker-client.ts';
import {RunningState} from '../running-state.ts';
import type {Service} from '../service.ts';
import {ArchiveChangeSource} from './archive/archive-change-source.ts';
import {
  ARCHIVE_ROOT,
  baseCompleteKey,
  basePrefix,
  baseRequestPrefix,
  lineagesFromKeys,
  logPrefix,
  parseBaseCompleteKey,
  parseSegmentKey,
} from './archive/layout.ts';
import type {SegmentMessage} from './archive/segment-format.ts';
import {publishBase} from './base/base-publisher.ts';
import {decodeBaseManifest, decodeBaseRequest} from './base/manifest.ts';
import {runArchiveGC, type GCOptions} from './gc.ts';
import {
  cleanupGenesis,
  readGenesisOffer,
  writeGenesisHeartbeat,
  type GenesisOffer,
} from './genesis.ts';
import {backupBasePublications, backupGCObjectsDeleted} from './metrics.ts';
import type {ObjectStore} from './object-store/object-store.ts';
import {archiveRestore} from './restore/archive-restore.ts';

/** The write worker's construction-time surface, factored for test doubles. */
export type InitializableWorkerClient = WriteWorkerClient & {
  init(
    dbPath: string,
    mode: ChangeProcessorMode,
    pragmas: PragmaConfig,
    logConfig: LogConfig,
  ): Promise<void>;
};

export type BaseProducerOptions = {
  taskID: string;
  store: ObjectStore;
  /** The producer's working replica file. */
  replicaFile: string;
  /** How often the change source polls for newly sealed segments. */
  pollIntervalMs: number;
  /** How often the publication triggers are evaluated. Default 30s. */
  checkIntervalMs?: number | undefined;
  /** Publication cadence: a base is published at least this often. */
  baseMaxIntervalMs: number;
  /**
   * The replay-budget early trigger: publish ahead of cadence when the
   * estimated time to replay the archived tail exceeds this.
   */
  baseMaxReplayMs: number;
  chunkBytes: number;
  integrityCheck: 'full' | 'quick';
  /** GC after each successful publication; null disables. */
  gc: GCOptions | null;
  /** For the write worker's own LogContext. */
  logConfig: LogConfig;
  /**
   * Performs the genesis table copy into `targetFile` at the offer's
   * exported snapshot — the server wires this to the real initial-sync code
   * (`providedSnapshot` mode). Without it the producer waits for someone
   * else to publish a first base.
   */
  genesisCopier?:
    | ((
        lc: LogContext,
        targetFile: string,
        offer: GenesisOffer,
      ) => Promise<void>)
    | undefined;
  /** How often a genesis copy heartbeats. Default 5s. */
  genesisHeartbeatIntervalMs?: number | undefined;
  /** Overridable for tests (e.g. an in-process worker). */
  createWorkerClient?: (() => InitializableWorkerClient) | undefined;
};

export type BaseProducerState = {
  lineage: string | undefined;
  lastBaseCursor: string | undefined;
  lastBaseCompletedAt: number | undefined;
  lastPublicationDurationMs: number | undefined;
  lastAppliedWatermark: string | undefined;
  /** ms epoch of the last applied transaction's upstream commit. */
  lastAppliedCommitTimeMs: number | undefined;
  /** Measured over the current tailing session, in archived-JSON bytes. */
  applyRateBytesPerSec: number | undefined;
  /** The replay-budget estimate at the last trigger evaluation. */
  replayEstimateMs: number | undefined;
};

const DEFAULT_CHECK_INTERVAL_MS = 30_000;

/** How long a stopping producer waits to reach a transaction boundary. */
const STOP_BOUNDARY_TIMEOUT_MS = 5_000;

/**
 * Compressed tail bytes explode to roughly this much applied-JSON work; the
 * replay-budget trigger is a heuristic to be calibrated by the M4 drills,
 * so a round factor beats false precision here.
 */
const COMPRESSION_EXPANSION_FACTOR = 4;

/** No early trigger until the rate is measured over at least this much. */
const MIN_RATE_SAMPLE_BYTES = 4 * 1024 * 1024;
const MIN_RATE_SAMPLE_MS = 5_000;

/** A live-base request older than this is debris from a dead restorer. */
const REQUEST_TTL_MS = 15 * 60 * 1000;

/**
 * The base producer: the one component in the archive world that
 * materializes a SQLite replica from the change stream — everything else
 * restores its output.
 *
 * It restores its own newest base, tails the archive through
 * {@link ArchiveChangeSource} + `IncrementalSyncer` + the write worker (the
 * real applier, so bases contain exactly what the applier would have
 * produced, by construction), and periodically publishes the working file as
 * a new base: pause at a transaction boundary (the change source parks and
 * the worker closes, releasing the exclusive lock on a consistent file) →
 * `publishBase` (freeze, verify, chunk-upload, manifest-last) → resume
 * tailing. Publication triggers on cadence or on the replay-budget estimate
 * — whichever the archived tail demands first — and each successful
 * publication runs archive GC.
 *
 * ### Crash posture: discard and rebuild
 *
 * The working file runs with `base-builder` pragmas (no journal, no fsync,
 * exclusive lock), so anything short of a parked-at-boundary shutdown may
 * leave it corrupt. A clean-shutdown marker distinguishes: on start — and
 * after any failed tailing session — a working file without the marker is
 * deleted and the producer restores its own newest base and resumes
 * tailing. That is its own restore path, exercised on every crash.
 */
export class BaseProducerService implements Service {
  readonly id = 'base-producer';
  readonly #lc: LogContext;
  readonly #opts: BaseProducerOptions;
  readonly #state = new RunningState('base-producer');
  readonly #checkIntervalMs: number;

  #lineage: string | undefined;
  #lastBase: {cursor: string; completedAt: number} | undefined;
  #lastPublicationDurationMs: number | undefined;
  #replayEstimateMs: number | undefined;

  // Apply measurements for the current tailing session.
  #sessionStartMs = 0;
  #sessionBytes = 0;
  #lastAppliedWatermark: string | undefined;
  #lastAppliedCommitTimeMs: number | undefined;

  /** True while the working file is known consistent and unlocked. */
  #fileConsistent = true;

  constructor(lc: LogContext, opts: BaseProducerOptions) {
    this.#lc = lc.withContext('component', 'base-producer');
    this.#opts = opts;
    this.#checkIntervalMs = opts.checkIntervalMs ?? DEFAULT_CHECK_INTERVAL_MS;
  }

  /** For tests and the gauges the server worker registers. */
  state(): BaseProducerState {
    return {
      lineage: this.#lineage,
      lastBaseCursor: this.#lastBase?.cursor,
      lastBaseCompletedAt: this.#lastBase?.completedAt,
      lastPublicationDurationMs: this.#lastPublicationDurationMs,
      lastAppliedWatermark: this.#lastAppliedWatermark,
      lastAppliedCommitTimeMs: this.#lastAppliedCommitTimeMs,
      applyRateBytesPerSec: this.#applyRate(),
      replayEstimateMs: this.#replayEstimateMs,
    };
  }

  async run(): Promise<void> {
    const lc = this.#lc;
    lc.info?.('starting base producer');
    while (this.#state.shouldRun()) {
      try {
        await this.#produce();
        this.#state.resetBackoff();
        // #produce returns when there is nothing to do yet (no lineage or
        // no base) or after a session ended; re-evaluate after a beat.
        await this.#state.sleep(this.#checkIntervalMs);
      } catch (e) {
        await this.#state.backoff(lc, e);
      }
    }
    if (this.#fileConsistent) {
      // Vouches for the working file across the restart; without it the
      // next incarnation discards and rebuilds.
      try {
        writeFileSync(this.#cleanMarkerFile(), '');
      } catch (e) {
        lc.warn?.('error writing the clean-shutdown marker', e);
      }
    }
    lc.info?.('base producer stopped');
  }

  stop(): Promise<void> {
    this.#state.stop(this.#lc);
    return Promise.resolve();
  }

  #cleanMarkerFile(): string {
    return `${this.#opts.replicaFile}.producer-clean`;
  }

  async #produce(): Promise<void> {
    const lc = this.#lc;
    const {store, replicaFile} = this.#opts;

    const lineage = await newestLineage(store);
    if (lineage === undefined) {
      lc.debug?.('the archive has no lineage yet; waiting');
      return;
    }
    this.#lineage = lineage;

    // Discard-and-rebuild on unclean start: without the marker, the working
    // file may be corrupt (base-builder pragmas trade durability away).
    // Within one process lifetime the marker never exists while running, so
    // every re-entry here after a failed session likewise discards.
    const marker = this.#cleanMarkerFile();
    if (existsSync(replicaFile) && !existsSync(marker)) {
      lc.warn?.(
        `working replica ${replicaFile} was not shut down cleanly; discarding`,
      );
      deleteLiteDB(replicaFile);
      deleteChangeLogDB(replicaFile);
    }
    rmSync(marker, {force: true});
    this.#fileConsistent = true;

    const restored = await archiveRestore(
      lc,
      store,
      replicaFile,
      {replicaVersion: lineage, minWatermark: ''},
      {mode: 'backup'},
    );
    if (restored === 'no_backup') {
      // Pre-genesis: build the first base from a genesis offer if one is
      // posted; otherwise there is nothing to build on yet.
      if (!(await this.#tryGenesis(lineage))) {
        lc.debug?.(`lineage ${lineage} has no complete base yet; waiting`);
        return;
      }
    } else if (restored !== 'success') {
      throw new Error(`restore of the working replica returned ${restored}`);
    }
    this.#lastBase = await newestCompleteBase(store, lineage);
    // The restore replayed the archived tail, so the working file may
    // already be ahead of the newest base; seed the applied watermark from
    // it so the publication triggers see that progress.
    this.#lastAppliedWatermark = readWatermark(lc, replicaFile);

    while (this.#state.shouldRun()) {
      const outcome = await this.#tailUntilPublishDue(lineage);
      if (outcome !== 'publish') {
        return; // stopped, or the session failed: re-evaluate from the top
      }
      await this.#publish(lineage);
    }
  }

  /**
   * Lineage genesis, producer side: copies the published tables at the
   * offered snapshot into the working file (through the copier the server
   * wires to the real initial-sync code), heartbeating so the gateway can
   * tell a live copy from a dead one, and publishes the result as the
   * lineage's first base. Returns false when there is no offer or no
   * copier. A failure mid-copy throws — the heap of a half-built file is
   * discarded on the next pass, and the gateway abandons the offer when
   * the heartbeats stop.
   */
  async #tryGenesis(lineage: string): Promise<boolean> {
    const lc = this.#lc;
    const {store, replicaFile, taskID, genesisCopier} = this.#opts;
    const offer = await readGenesisOffer(store, lineage);
    if (offer === undefined || genesisCopier === undefined) {
      return false;
    }
    if (offer.replicaVersion !== lineage) {
      lc.warn?.(
        `genesis offer under ${lineage} claims lineage ${offer.replicaVersion}; ignoring`,
      );
      return false;
    }
    lc.info?.(
      `starting genesis for ${lineage} at snapshot ${offer.snapshotID} ` +
        `(offered by ${offer.taskID})`,
    );
    deleteLiteDB(replicaFile);
    deleteChangeLogDB(replicaFile);
    this.#fileConsistent = false;

    const heartbeat = setInterval(() => {
      void writeGenesisHeartbeat(store, lineage, taskID).catch(e =>
        lc.warn?.('error writing the genesis heartbeat', e),
      );
    }, this.#opts.genesisHeartbeatIntervalMs ?? 5_000);
    try {
      await writeGenesisHeartbeat(store, lineage, taskID);
      await genesisCopier(lc, replicaFile, offer);
      const built = readWatermark(lc, replicaFile);
      const db = new Database(lc, replicaFile);
      const {replicaVersion: builtVersion} = getSubscriptionState(
        new StatementRunner(db),
      );
      db.close();
      if (builtVersion !== lineage) {
        throw new Error(
          `genesis copy produced replica version ${builtVersion}; ` +
            `the offer is for lineage ${lineage}`,
        );
      }
      this.#fileConsistent = true;
      await this.#publish(lineage);
      // The gateway also cleans these up when it sees the base; doing it
      // here too covers a gateway that gave up waiting.
      await cleanupGenesis(store, lineage);
      lc.info?.(`genesis for ${lineage} published its first base at ${built}`);
      return true;
    } finally {
      clearInterval(heartbeat);
    }
  }

  /**
   * One tailing session: the applier running, publication triggers
   * evaluated on an interval. Ends with the applier stopped and the worker
   * closed; on 'publish' (and on a boundary-parked stop) the file is
   * consistent and unlocked, ready to freeze.
   */
  async #tailUntilPublishDue(
    lineage: string,
  ): Promise<'publish' | 'stopped' | 'failed'> {
    const lc = this.#lc;
    const {store, replicaFile, taskID, pollIntervalMs, logConfig} = this.#opts;

    const tempDir = `${replicaFile}-segments`;
    await mkdir(tempDir, {recursive: true});
    const changeSource = new ArchiveChangeSource(lc, {
      store,
      replicaVersion: lineage,
      pollIntervalMs,
      tempDir,
      onDeliver: message => this.#recordDelivery(message),
    });

    this.#sessionStartMs = Date.now();
    this.#sessionBytes = 0;

    const worker = (
      this.#opts.createWorkerClient ?? (() => new ThreadWriteWorkerClient())
    )();
    this.#fileConsistent = false;
    let atBoundary = false;
    let syncer: IncrementalSyncer | undefined;
    try {
      await worker.init(
        replicaFile,
        'backup',
        getPragmaConfig('base-builder'),
        logConfig,
      );
      syncer = new IncrementalSyncer(
        lc,
        taskID,
        `${taskID}/base-producer`,
        changeSource,
        worker,
        'backup',
        replicaFile,
        null,
      );
      const running = syncer.run();
      // undefined = the syncer stopped on its own (e.g. the change source
      // sent an error message and the replica was deleted).
      const failed = running.then(
        () => undefined,
        (e: unknown) => e ?? new Error('syncer failed'),
      );

      for (;;) {
        if (!this.#state.shouldRun()) {
          // Service shutdown: park at a boundary if one is quickly
          // reachable, so the clean marker can vouch for the file.
          atBoundary = await withTimeout(
            changeSource.holdAtBoundary(),
            STOP_BOUNDARY_TIMEOUT_MS,
          );
          syncer.stop(lc);
          await running;
          return 'stopped';
        }
        const raced = await Promise.race([
          failed,
          this.#state.sleep(this.#checkIntervalMs).then(() => null),
        ]);
        if (raced !== null) {
          if (raced !== undefined) {
            throw raced;
          }
          return 'failed';
        }
        if (await this.#publicationDue(lineage)) {
          // The freeze: the stream parks at the next transaction boundary
          // with everything before it consumed. false means the stream died
          // instead — the applier's state is unknown, so no publication.
          atBoundary = await changeSource.holdAtBoundary();
          syncer.stop(lc);
          await running;
          return atBoundary ? 'publish' : 'failed';
        }
      }
    } finally {
      syncer?.stop(lc);
      await worker.stop().catch(e => lc.warn?.('error stopping worker', e));
      this.#fileConsistent = atBoundary;
    }
  }

  #recordDelivery({message, watermark, json}: SegmentMessage): void {
    this.#sessionBytes += json.length;
    if (message[0] === 'commit') {
      this.#lastAppliedWatermark = watermark;
      const commitTimeMs = message[1].commitTimeMs;
      if (commitTimeMs !== undefined) {
        this.#lastAppliedCommitTimeMs = commitTimeMs;
      }
    }
  }

  #applyRate(): number | undefined {
    const elapsed = Date.now() - this.#sessionStartMs;
    if (
      this.#sessionStartMs === 0 ||
      elapsed < MIN_RATE_SAMPLE_MS ||
      this.#sessionBytes < MIN_RATE_SAMPLE_BYTES
    ) {
      return undefined;
    }
    return (this.#sessionBytes / elapsed) * 1000;
  }

  async #publicationDue(lineage: string): Promise<boolean> {
    const {store, baseMaxIntervalMs, baseMaxReplayMs} = this.#opts;
    const lastBase = this.#lastBase;
    if (lastBase === undefined) {
      return true; // no base at all: publish as soon as possible
    }
    const requested = await this.#serveBaseRequests(lineage);
    // Nothing applied beyond the last base means nothing to publish. (Any
    // pending base requests were already answered above: the newest base
    // is as fresh as this producer can make one.)
    const applied = this.#lastAppliedWatermark;
    if (applied === undefined || applied <= lastBase.cursor) {
      return false;
    }
    if (requested) {
      return true; // an accelerated live-base request
    }
    if (Date.now() - lastBase.completedAt >= baseMaxIntervalMs) {
      return true;
    }
    // The replay-budget early trigger: how long would a restorer spend
    // replaying the archived tail above the newest base? Estimated from the
    // measured apply rate and the compressed tail size — a coarse heuristic
    // (compression ratio assumed, idle time inflates the denominator), to
    // be calibrated by the M4 drills.
    const rate = this.#applyRate();
    if (rate === undefined) {
      return false;
    }
    const objects = await store.list(logPrefix(lineage));
    let tailBytes = 0;
    for (const {key, size} of objects) {
      const segment = parseSegmentKey(lineage, key);
      if (segment !== undefined && segment.end > lastBase.cursor) {
        tailBytes += size;
      }
    }
    this.#replayEstimateMs =
      ((tailBytes * COMPRESSION_EXPANSION_FACTOR) / rate) * 1000;
    return this.#replayEstimateMs >= baseMaxReplayMs;
  }

  /**
   * The producer's half of the accelerated live-base protocol: stale or
   * unreadable request markers are debris and deleted; fresh markers are
   * answered — by deleting them right away when the newest base already
   * reflects everything applied (the "already current" response), or by
   * returning true so the caller publishes now (those markers are consumed
   * by {@link #publish} once the base lands).
   */
  async #serveBaseRequests(lineage: string): Promise<boolean> {
    const {store} = this.#opts;
    const objects = await store.list(baseRequestPrefix(lineage));
    if (objects.length === 0) {
      return false;
    }
    const applied = this.#lastAppliedWatermark;
    const current =
      applied === undefined ||
      (this.#lastBase !== undefined && applied <= this.#lastBase.cursor);
    let fresh = false;
    for (const {key} of objects) {
      let stale = false;
      try {
        const request = decodeBaseRequest(await store.get(key));
        stale = request.requestedAt < Date.now() - REQUEST_TTL_MS;
        if (!stale) {
          this.#lc.info?.(
            `live-base request from task ${request.taskID}` +
              (current ? ': the newest base is already current' : ''),
          );
        }
      } catch {
        stale = true;
      }
      if (stale || current) {
        await store.delete(key);
      }
      fresh ||= !stale;
    }
    return fresh && !current;
  }

  async #publish(lineage: string): Promise<void> {
    const lc = this.#lc;
    const {store, replicaFile, chunkBytes, integrityCheck, gc} = this.#opts;
    const start = Date.now();
    try {
      const {manifest, published} = await publishBase(lc, store, replicaFile, {
        chunkBytes,
        integrityCheck,
      });
      this.#lastPublicationDurationMs = Date.now() - start;
      this.#lastBase = {
        cursor: manifest.cursor,
        completedAt: manifest.completedAt,
      };
      backupBasePublications().add(1, {
        result: published ? 'published' : 'already-complete',
      });
    } catch (e) {
      backupBasePublications().add(1, {result: 'failed'});
      throw e;
    }

    // Consume the live-base request markers this publication answers.
    for (const {key} of await store.list(baseRequestPrefix(lineage))) {
      await store.delete(key).catch(() => {});
    }

    if (gc !== null) {
      try {
        const result = await runArchiveGC(lc, store, lineage, gc);
        const deleted = backupGCObjectsDeleted();
        deleted.add(result.deletedBaseCursors.length, {kind: 'base'});
        deleted.add(result.deletedDebrisCursors.length, {kind: 'debris'});
        deleted.add(result.deletedSegments.length, {kind: 'segment'});
      } catch (e) {
        // GC is retried after the next publication; failing it must not
        // stall base production.
        lc.warn?.('archive GC failed; will retry after the next base', e);
      }
    }
  }
}

function readWatermark(lc: LogContext, replicaFile: string): string {
  const db = new Database(lc, replicaFile);
  try {
    return getSubscriptionState(new StatementRunner(db)).watermark;
  } finally {
    db.close();
  }
}

function withTimeout(promise: Promise<boolean>, ms: number): Promise<boolean> {
  return Promise.race([
    promise,
    new Promise<boolean>(resolve => setTimeout(resolve, ms, false)),
  ]);
}

async function newestLineage(store: ObjectStore): Promise<string | undefined> {
  const objects = await store.list(ARCHIVE_ROOT);
  return lineagesFromKeys(objects.map(o => o.key)).at(-1);
}

async function newestCompleteBase(
  store: ObjectStore,
  lineage: string,
): Promise<{cursor: string; completedAt: number} | undefined> {
  const objects = await store.list(basePrefix(lineage));
  const cursor = objects
    .map(o => parseBaseCompleteKey(lineage, o.key))
    .findLast(c => c !== undefined);
  if (cursor === undefined) {
    return undefined;
  }
  const manifest = decodeBaseManifest(
    await store.get(baseCompleteKey(lineage, cursor)),
  );
  return {cursor, completedAt: manifest.completedAt};
}
