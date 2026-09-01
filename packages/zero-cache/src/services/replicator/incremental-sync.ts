import type {LogContext} from '@rocicorp/logger';
import {AbortError} from '../../../../shared/src/abort-error.ts';
import type {Enum} from '../../../../shared/src/enum.ts';
import {deleteLiteDB} from '../../db/delete-lite-db.ts';
import {
  getOrCreateCounter,
  getOrCreateLatencyHistogram,
} from '../../observability/metrics.ts';
import type {Source} from '../../types/streams.ts';
import type {DownloadStatus} from '../change-source/protocol/current.ts';
import type {ChangeStreamData} from '../change-source/protocol/current/downstream.ts';
import {
  errorTypeToReadableName,
  PROTOCOL_VERSION,
  type ChangeStreamer,
  type SerializedDownstream,
} from '../change-streamer/change-streamer.ts';
import type * as ErrorType from '../change-streamer/error-type-enum.ts';
import {RunningState} from '../running-state.ts';
import type {CommitResult} from './change-processor.ts';
import {Notifier} from './notifier.ts';
import type {ReplicationStatusPublisher} from './replication-status.ts';
import type {ReplicaState, ReplicatorMode} from './replicator.ts';
import {ReplicationReportRecorder} from './reporter/recorder.ts';
import type {ReplicationReport} from './reporter/report-schema.ts';
import type {WriteWorkerClient} from './write-worker-client.ts';

type ErrorType = Enum<typeof ErrorType>;

/**
 * How much change JSON one hop to the write worker may carry. The bound is
 * memory: the batch is held until it is applied and `postMessage` clones it,
 * so this is roughly what a replicator holds above its steady state. It is
 * deliberately the same 64 KiB the change-streamer pipelines before waiting
 * on subscriber flow control, so neither side runs far ahead of the other.
 */
const MAX_BATCH_BYTES = 64 * 1024;

/**
 * A second cap, by count, so that a stream of very small messages cannot
 * retain a large number of objects while staying under the byte cap.
 */
const MAX_BATCH_MESSAGES = 500;

/**
 * The {@link IncrementalSyncer} manages a logical replication stream from upstream,
 * handling application lifecycle events (start, stop) and retrying the
 * connection with exponential backoff. The actual handling of the logical
 * replication messages is done by the {@link ChangeProcessor}, which runs
 * in a worker thread via the {@link WriteWorkerClient}.
 */
export class IncrementalSyncer {
  readonly #lc: LogContext;
  readonly #taskID: string;
  readonly #id: string;
  readonly #changeStreamer: ChangeStreamer;
  readonly #worker: WriteWorkerClient;
  readonly #mode: ReplicatorMode;
  readonly #replicaDbPath: string;
  readonly #statusPublisher: ReplicationStatusPublisher | null;
  readonly #notifier: Notifier;
  readonly #reporter: ReplicationReportRecorder;

  readonly #state = new RunningState('IncrementalSyncer');

  readonly #replicationEvents = getOrCreateCounter(
    'replication',
    'events',
    'Number of replication events processed',
  );

  // Convergence metrics: every consumer of the change stream (serving
  // replicas and the base producer alike, distinguished by the `mode`
  // attribute) reports the same two signals, so replicas across worlds can
  // be compared directly.
  readonly #applyCommits = getOrCreateCounter(
    'apply',
    'commits',
    'Transactions applied (committed) to the local replica.',
  );
  readonly #applyBatches = getOrCreateCounter(
    'apply',
    'write_batches',
    'Hops to the write worker. Divided into apply.changes, this is the ' +
      'batching factor: 1 means every change paid its own round trip.',
  );
  readonly #applyLag = getOrCreateLatencyHistogram(
    'apply',
    'lag',
    'Time from a transaction committing upstream to it being applied to ' +
      'the local replica. Crosses the upstream/local clock domain, like ' +
      'zero.replication.total_lag.',
  );

  constructor(
    lc: LogContext,
    taskID: string,
    id: string,
    changeStreamer: ChangeStreamer,
    worker: WriteWorkerClient,
    mode: ReplicatorMode,
    replicaDbPath: string,
    statusPublisher: ReplicationStatusPublisher | null,
  ) {
    this.#lc = lc;
    this.#taskID = taskID;
    this.#id = id;
    this.#changeStreamer = changeStreamer;
    this.#worker = worker;
    this.#mode = mode;
    this.#replicaDbPath = replicaDbPath;
    this.#statusPublisher = statusPublisher;
    this.#notifier = new Notifier();
    this.#reporter = new ReplicationReportRecorder(lc);
  }

  async run() {
    const lc = this.#lc;
    let workerError: Error | undefined;
    this.#worker.onError(err => {
      workerError ??= err;
      this.#state.stop(lc, err);
    });
    lc.info?.(`Starting IncrementalSyncer`);
    const {watermark: initialWatermark} =
      await this.#worker.getSubscriptionState();

    // Notify any waiting subscribers that the replica is ready to be read.
    // This initial notification intentionally omits replicaReadyTimeMs because
    // it represents already-current state, not newly-unserved work.
    void this.#notifier.notifySubscribers({
      state: 'version-ready',
      watermark: initialWatermark,
    });

    while (this.#state.shouldRun()) {
      const {replicaVersion, watermark} =
        await this.#worker.getSubscriptionState();

      let downstream: Source<SerializedDownstream> | undefined;
      let unregister = () => {};
      let err: unknown | undefined;

      try {
        downstream = await this.#changeStreamer.subscribe({
          protocolVersion: PROTOCOL_VERSION,
          taskID: this.#taskID,
          id: this.#id,
          mode: this.#mode,
          watermark,
          replicaVersion,
          initial: watermark === initialWatermark,
          // The SQLite change log is written by the change-streamer itself, so
          // no replicator logs the change stream any more. The parameter stays
          // on the wire for change-streamers that still exclude a writer from
          // SQLite catchup.
          logsChangeStream: false,
        });
        this.#state.resetBackoff();
        unregister = this.#state.cancelOnStop(downstream);
        this.#statusPublisher?.publish(
          lc,
          'Replicating',
          `Replicating from ${watermark}`,
        );

        let backfillStatus: DownloadStatus | undefined;

        // Changes accumulated for the next hop to the write worker. Bounded
        // on purpose: this is resident memory that `postMessage` transiently
        // doubles when it clones it, so a transaction is never held here --
        // whatever the caps allow is flushed and applied, mid-transaction if
        // need be, and the boundary messages that make it atomic are just
        // more messages in the stream.
        let batch: ChangeStreamData[] = [];
        let batchBytes = 0;

        const flush = async () => {
          if (batch.length === 0) {
            return;
          }
          const sending = batch;
          batch = [];
          batchBytes = 0;
          this.#applyBatches.add(1, {mode: this.#mode});
          const results = await this.#worker.processMessages(sending);
          for (const result of results) {
            this.#handleResult(lc, result);
            if (result?.completedBackfill) {
              backfillStatus = undefined;
            }
          }
        };

        for await (const {data: message, json} of downstream) {
          this.#replicationEvents.add(1);
          switch (message[0]) {
            case 'status': {
              const {lagReport} = message[1];
              if (lagReport) {
                const report: ReplicationReport = {
                  nextSendTimeMs: lagReport.nextSendTimeMs,
                };
                if (lagReport.lastTimings) {
                  report.lastTimings = {
                    ...lagReport.lastTimings,
                    replicateTimeMs: Date.now(),
                  };
                }
                this.#reporter.record(report);
              }
              break;
            }
            case 'error': {
              // Signal from the replication-manager that the view-syncer must
              // shut down and restore a new backup from litestream.
              const {type, message: msg} = message[1];
              // Explicit errors from the change-streamer (e.g. watermark too
              // old) often indicate a problem with the replica. As a
              // conservative measure, delete the replica before shutting down
              // so that the process restarts from scratch even if reusing the
              // same volume.
              lc.warn?.(
                `received error from change-streamer. deleting replica file`,
                {error: message[1]},
              );
              deleteLiteDB(this.#replicaDbPath);
              this.stop(
                lc,
                // Note: The AbortError indicates a clean / intentional shutdown.
                new AbortError(
                  `${errorTypeToReadableName(type as ErrorType)}: ${msg}`,
                ),
              );
              break;
            }
            default: {
              const msg = message[1];
              if (msg.tag === 'backfill' && msg.status) {
                const {status} = msg;
                if (!backfillStatus) {
                  // Start publishing the status every 3 seconds.
                  backfillStatus = status;
                  this.#statusPublisher?.publish(
                    lc,
                    'Replicating',
                    `Backfilling ${msg.relation.name} table`,
                    3000,
                    () =>
                      backfillStatus
                        ? {
                            downloadStatus: [
                              {
                                ...backfillStatus,
                                table: msg.relation.name,
                                columns: [
                                  ...msg.relation.rowKey.columns,
                                  ...msg.columns,
                                ],
                              },
                            ],
                          }
                        : {},
                  );
                }
                backfillStatus = status; // Update the current status
              }

              batch.push(message as ChangeStreamData);
              batchBytes += json.length;
              // Flush when the batch is as large as it is allowed to get, or
              // -- the common case at any rate below saturation -- when
              // nothing else has arrived yet, so batching never trades
              // latency for throughput it does not need. A source that does
              // not report its queue depth reports `undefined` here, which
              // flushes every message: exactly the pre-batching behavior.
              if (
                batchBytes >= MAX_BATCH_BYTES ||
                batch.length >= MAX_BATCH_MESSAGES ||
                (downstream.queued ?? 0) === 0
              ) {
                await flush();
              }
              break;
            }
          }
        }
        this.#worker.abort();
      } catch (e) {
        err = e;
        this.#worker.abort();
      } finally {
        downstream?.cancel();
        unregister();
        this.#statusPublisher?.stop();
      }
      await this.#state.backoff(lc, err);
    }
    lc.info?.('IncrementalSyncer stopped');
    if (workerError) {
      throw workerError;
    }
  }

  #handleResult(lc: LogContext, result: CommitResult | null) {
    if (!result) {
      return;
    }
    this.#applyCommits.add(1, {mode: this.#mode});
    if (result.upstreamCommitTimeMs !== undefined) {
      this.#applyLag.recordMs(Date.now() - result.upstreamCommitTimeMs, {
        mode: this.#mode,
      });
    }
    if (result.completedBackfill) {
      // Publish the final status
      const status = result.completedBackfill;
      this.#statusPublisher?.publish(
        lc,
        'Replicating',
        `Backfilled ${status.table} table`,
        0,
        () => ({downloadStatus: [status]}),
      );
    } else if (result.schemaUpdated) {
      this.#statusPublisher?.publish(lc, 'Replicating', 'Schema updated');
    }
    if (result.watermark && result.changeLogUpdated) {
      void this.#notifier.notifySubscribers({
        state: 'version-ready',
        watermark: result.watermark,
        replicaReadyTimeMs: Date.now(),
        upstreamCommitTimeMs: result.upstreamCommitTimeMs,
      });
    }
  }

  subscribe(): Source<ReplicaState> {
    return this.#notifier.subscribe();
  }

  stop(lc: LogContext, err?: unknown) {
    this.#state.stop(lc, err);
  }
}
