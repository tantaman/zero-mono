import type {LogContext} from '@rocicorp/logger';
import type {Source} from '../../../types/streams.ts';
import {Subscription} from '../../../types/subscription.ts';
import type {
  ChangeStreamer,
  SerializedDownstream,
  SubscriberContext,
} from '../../change-streamer/change-streamer.ts';
import * as ErrorType from '../../change-streamer/error-type-enum.ts';
import type {ObjectStore} from '../object-store/object-store.ts';
import {
  contiguousHeadFrom,
  iterateMessages,
  listLogSegments,
} from './archive-reader.ts';

export type ArchiveChangeSourceOptions = {
  store: ObjectStore;
  /** The lineage the subscriber must belong to. */
  replicaVersion: string;
  /**
   * How often to re-list the log for newly sealed segments once caught up.
   * This is the freshness bound of the produced stream: content lags the
   * live stream by at most the seal interval plus this.
   */
  pollIntervalMs: number;
  /** Directory for segment download temp files. */
  tempDir: string;
  /** Overridable for tests. */
  setTimeoutFn?: typeof setTimeout | undefined;
};

/**
 * Presents the change-streamer's subscribe surface from the archive's sealed
 * segments: catchup from the subscriber's cursor through the contiguous
 * archived head, then sealed-segment polling for the live tail. This is what
 * lets the base producer be `IncrementalSyncer` + write-worker +
 * `ChangeProcessor` **unchanged** — the determinism requirement (bases
 * contain exactly what the applier would produce) is satisfied because it
 * *is* the applier, fed the archived stream verbatim (each message's
 * original JSON is preserved end-to-end).
 *
 * Semantics mirror the real change-streamer where the producer can observe
 * them:
 * - a `status` message opens every valid subscription;
 * - a subscriber on a different lineage gets a `WrongReplicaVersion` error
 *   message;
 * - a cursor below the archived history (e.g. GC passed it) gets
 *   `WatermarkTooOld`, on which the syncer deletes its replica and the
 *   producer restores afresh;
 * - transient failures (listing, download, decode) fail the stream, and the
 *   syncer's retry loop resubscribes with backoff.
 *
 * Replay filtering by commit watermark lives here (via `iterateMessages`),
 * consistent with where dedup lives for the stream's other consumers.
 * Back-pressure is per-message: the pump awaits each push's consumption, so
 * the subscription never buffers more than the applier's appetite.
 */
export class ArchiveChangeSource implements ChangeStreamer {
  readonly #lc: LogContext;
  readonly #opts: ArchiveChangeSourceOptions;
  readonly #setTimeout: typeof setTimeout;

  constructor(lc: LogContext, opts: ArchiveChangeSourceOptions) {
    this.#lc = lc.withContext('component', 'archive-change-source');
    this.#opts = opts;
    this.#setTimeout = opts.setTimeoutFn ?? setTimeout;
  }

  subscribe(ctx: SubscriberContext): Promise<Source<SerializedDownstream>> {
    const sub = Subscription.create<SerializedDownstream>();
    void this.#stream(ctx, sub).catch(e => {
      this.#lc.warn?.(`archive subscription for ${ctx.id} failed`, e);
      sub.fail(e instanceof Error ? e : new Error(String(e)));
    });
    return Promise.resolve(sub);
  }

  async #stream(
    ctx: SubscriberContext,
    sub: Subscription<SerializedDownstream>,
  ): Promise<void> {
    const {store, replicaVersion, pollIntervalMs, tempDir} = this.#opts;
    const push = async (data: SerializedDownstream['data']) => {
      await sub.push({data, json: JSON.stringify(data)}).result;
    };
    if (ctx.replicaVersion !== replicaVersion) {
      await push([
        'error',
        {
          type: ErrorType.WrongReplicaVersion,
          message:
            `subscriber is on replica version ${ctx.replicaVersion}; ` +
            `the archive lineage is ${replicaVersion}`,
        },
      ]);
      sub.cancel();
      return;
    }
    await push(['status', {tag: 'status'}]);

    let cursor = ctx.watermark;
    while (sub.active) {
      const segments = await listLogSegments(store, replicaVersion);
      if (segments.length > 0 && cursor < segments[0].start) {
        // The archive no longer covers the cursor (GC passed it); the
        // subscriber must restore from a newer base.
        await push([
          'error',
          {
            type: ErrorType.WatermarkTooOld,
            message:
              `watermark ${cursor} is below the archived history, which ` +
              `starts after ${segments[0].start}`,
          },
        ]);
        sub.cancel();
        return;
      }
      const head = contiguousHeadFrom(segments, cursor);
      if (head === cursor) {
        await this.#sleep(pollIntervalMs, sub);
        continue;
      }
      for await (const {message, json} of iterateMessages(
        store,
        replicaVersion,
        cursor,
        head,
        tempDir,
      )) {
        if (!sub.active) {
          return;
        }
        await sub.push({data: message, json}).result;
      }
      cursor = head;
    }
  }

  #sleep(ms: number, sub: Subscription<SerializedDownstream>): Promise<void> {
    return new Promise(resolve => {
      const handle = this.#setTimeout(resolve, ms);
      sub.signal.addEventListener(
        'abort',
        () => {
          clearTimeout(handle);
          resolve();
        },
        {once: true},
      );
    });
  }
}
