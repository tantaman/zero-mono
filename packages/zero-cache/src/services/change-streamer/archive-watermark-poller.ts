import type {LogContext} from '@rocicorp/logger';
import {getOrCreateCounter} from '../../observability/metrics.ts';
import type {Source} from '../../types/streams.ts';
import {Subscription} from '../../types/subscription.ts';
import {
  baseCompleteKey,
  basePrefix,
  parseBaseCompleteKey,
} from '../backup/archive/layout.ts';
import {decodeBaseManifest} from '../backup/base/manifest.ts';
import type {ObjectStore} from '../backup/object-store/object-store.ts';
import {UnrecoverableError} from '../running-state.ts';
import type {BackedUpWatermark} from './backup-monitor.ts';

export const CHECK_INTERVAL_MS = 10_000;

/**
 * How long a cold-starting gateway waits for the lineage's first complete
 * base before concluding that base production is broken and shutting down
 * (mirroring the v3 poller's initial-backup deadline). Genesis — a full
 * initial sync by the producer — is the slow path this must accommodate;
 * with no local replica to scale by (the gateway world has none), the
 * allowance is a generous constant.
 */
export const INITIAL_BASE_DEADLINE_MS = 4 * 60 * 60_000; // 4 hours

export type ArchiveWatermarkPollerOptions = {
  store: ObjectStore;
  /** The lineage whose bases gate purges and restores. */
  replicaVersion: string;
  /**
   * Age of the newest complete base beyond which base production is
   * considered stalled (logged loudly and counted; see class doc for why
   * this does not self-terminate the way a wedged litestream does).
   */
  staleBaseGraceMs: number;
  checkIntervalMs?: number | undefined;
  initialBaseDeadlineMs?: number | undefined;
  setIntervalFn?: typeof setInterval | undefined;
};

/**
 * The archive-mode {@link BackedUpWatermark} producer: polls the base
 * listing and emits the newest complete base's cursor. That cursor is what
 * `trackBackupWatermark` needs from the archive world — the purge floor
 * (a restorer gets at least that base) and the snapshot-confirmation signal
 * (the first emission is what marks a restorable backup as existing, which
 * confirms reservations and satisfies the readiness gate). The bases are
 * manifest-last and checksummed, so unlike litestream's claimed progress no
 * separate backup-state verification is needed: a listed `complete.json`
 * IS the verified upload.
 *
 * Guardrails, mirroring the v3 poller's wedged-backup posture where the
 * failure modes correspond:
 * - **Initial-base deadline**: a cold start with no complete base within
 *   the deadline fails the stream with an {@link UnrecoverableError} — the
 *   backup pipeline is broken and a loud exit beats silently serving
 *   without a restorable backup.
 * - **Stale base**: when the newest base's age exceeds the grace, the
 *   condition is logged at error and counted (`purge_blocked`,
 *   reason `base-stalled`). It deliberately does *not* self-terminate:
 *   the stalled component is the base producer, a different process (often
 *   a different node), which restarting this gateway cannot fix — and the
 *   change-log conservatively stops purging on its own while the floor
 *   stands still, so the cost of staying up is growth, not corruption.
 */
export class ArchiveWatermarkPoller {
  readonly id = 'archive-watermark-poller';
  readonly #lc: LogContext;
  readonly #opts: ArchiveWatermarkPollerOptions;
  readonly #stream: Subscription<BackedUpWatermark>;
  readonly #purgesBlocked = getOrCreateCounter('replica', 'purge_blocked', {
    description:
      'Number of change-log purges blocked because the actual backup state ' +
      'could not be confirmed. In archive mode the "base-stalled" reason ' +
      'counts polls that found the newest complete base older than the ' +
      'staleness grace, i.e. base production has stalled.',
  });

  #timer: NodeJS.Timeout | undefined;
  #lastWatermark = '';
  #startTimeMs: number | undefined;
  #firstBaseSeen = false;
  #staleReported = false;
  #polling = false;

  constructor(lc: LogContext, opts: ArchiveWatermarkPollerOptions) {
    this.#lc = lc.withContext('component', this.id);
    this.#opts = opts;
    this.#stream = Subscription.create<BackedUpWatermark>({
      cleanup: () => clearInterval(this.#timer),
    });
  }

  start(): Source<BackedUpWatermark> {
    const setIntervalFn = this.#opts.setIntervalFn ?? setInterval;
    this.#startTimeMs = Date.now();
    this.#lc.info?.(
      `polling the archive for complete bases of ${this.#opts.replicaVersion}`,
    );
    void this.#poll();
    this.#timer = setIntervalFn(
      () => void this.#poll(),
      this.#opts.checkIntervalMs ?? CHECK_INTERVAL_MS,
    );
    return this.#stream;
  }

  async #poll(): Promise<void> {
    if (this.#polling) {
      return; // a slow listing outlived the interval; skip this tick
    }
    this.#polling = true;
    try {
      const {store, replicaVersion} = this.#opts;
      const objects = await store.list(basePrefix(replicaVersion));
      const cursor = objects
        .map(o => parseBaseCompleteKey(replicaVersion, o.key))
        .findLast(c => c !== undefined);
      if (cursor === undefined) {
        this.#checkInitialDeadline();
        return;
      }
      this.#firstBaseSeen = true;
      const manifest = decodeBaseManifest(
        await store.get(baseCompleteKey(replicaVersion, cursor)),
      );
      this.#checkStaleness(cursor, manifest.completedAt);
      if (cursor !== this.#lastWatermark) {
        this.#lastWatermark = cursor;
        this.#lc.debug?.(`newest complete base is at ${cursor}`);
        this.#stream.push({
          watermark: cursor,
          backupTimeMs: manifest.completedAt,
        });
      }
    } catch (e) {
      if (e instanceof UnrecoverableError) {
        this.#stream.fail(e);
        return;
      }
      // Transient listing/read failures leave the floor where it is; the
      // change-log conservatively stops purging until the next good poll.
      this.#lc.warn?.(`error polling the archive for bases`, e);
    } finally {
      this.#polling = false;
    }
  }

  #checkInitialDeadline(): void {
    const deadline =
      this.#opts.initialBaseDeadlineMs ?? INITIAL_BASE_DEADLINE_MS;
    if (
      !this.#firstBaseSeen &&
      this.#startTimeMs !== undefined &&
      Date.now() - this.#startTimeMs > deadline
    ) {
      this.#stream.fail(
        new UnrecoverableError(
          `no complete base for ${this.#opts.replicaVersion} appeared ` +
            `within ${deadline}ms of startup: base production (or genesis) ` +
            `is broken`,
        ),
      );
    }
  }

  #checkStaleness(cursor: string, completedAt: number): void {
    const age = Date.now() - completedAt;
    if (age <= this.#opts.staleBaseGraceMs) {
      this.#staleReported = false;
      return;
    }
    this.#purgesBlocked.add(1, {reason: 'base-stalled'});
    if (!this.#staleReported) {
      this.#staleReported = true;
      this.#lc.error?.(
        `the newest complete base (${cursor}) is ${age}ms old, past the ` +
          `${this.#opts.staleBaseGraceMs}ms staleness grace: base ` +
          `production appears stalled. Change-log purges will not advance ` +
          `past it, and restores replay an ever-growing tail.`,
      );
    }
  }
}
