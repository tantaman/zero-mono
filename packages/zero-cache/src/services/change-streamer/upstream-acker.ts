import {assert} from '../../../../shared/src/asserts.ts';
import {max, min} from '../../types/lexi-version.ts';
import type {Sink} from '../../types/streams.ts';
import type {
  ChangeSourceUpstream,
  ChangeStreamMessage,
} from '../change-source/protocol/current.ts';

type Opts = {
  trackPgChangeLog: boolean;
  trackBackup: boolean;
  /**
   * Gates ACKs on the durable archive cursor. Set in backup mode `archive`,
   * where the logical archive is authoritative.
   */
  trackArchive?: boolean | undefined;
};

/**
 * Tracks the progress (watermark) of multiple streams:
 * - downstream transactions and status messages (the latter
 *   indicating LSNs that are not relevant to the publication
 *   but nevertheless need to be ACKed)
 * - PG change-log commits
 * - backup watermarks
 * - the durable logical-archive cursor
 *
 * and sends upstream ACKs accordingly. When multiple stores are
 * being considered for upstream ACKs (e.g. RMv1.5 tracking both the
 * PG change-log and backup watermarks, or backup mode `archive`
 * additionally gating on the archive cursor), only watermarks that
 * have been reached by every tracked store are acked.
 *
 * The UpstreamAcker also takes into account that an upstream
 * connection can be disconnected and {@link reset()}. In this case,
 * the progress of the persistent stores is retained and the acks
 * are resent on the new upstream connection as necessary.
 */
export class UpstreamAcker {
  readonly #trackPgChangeLog: boolean;
  readonly #trackBackup: boolean;
  readonly #trackArchive: boolean;

  #pgChangeLogWatermark = '';
  #backupWatermark = '';
  #archiveWatermark = '';

  #upstream: Sink<ChangeSourceUpstream> | undefined;
  #lastTx = '';
  #lastStatus = '';
  #lastAck = '';

  constructor({trackPgChangeLog, trackBackup, trackArchive = false}: Opts) {
    assert(
      trackPgChangeLog || trackBackup || trackArchive,
      `At least one of trackPgChangeLog, trackBackup, or trackArchive must be true`,
    );
    this.#trackPgChangeLog = trackPgChangeLog;
    this.#trackBackup = trackBackup;
    this.#trackArchive = trackArchive;
  }

  reset(upstream: Sink<ChangeSourceUpstream>) {
    this.#upstream = upstream;
    this.#lastTx = '';
    this.#lastStatus = '';
    this.#lastAck = '';
  }

  trackDownstream(downstream: ChangeStreamMessage) {
    const [tag, msg] = downstream;
    switch (tag) {
      case 'status':
        if (msg.ack) {
          this.#lastStatus = downstream[2].watermark;
        }
        this.#maybeAck();
        break;
      case 'commit':
        this.#lastTx = downstream[2].watermark;
        this.#maybeAck();
        break;
    }
  }

  trackPgChangeLog(committedWatermark: string) {
    // Watermarks should never move backwards, but use max() defensively.
    this.#pgChangeLogWatermark = max(
      committedWatermark,
      this.#pgChangeLogWatermark,
    );
    this.#maybeAck();
  }

  trackBackup(backedUpWatermark: string) {
    // Watermarks should never move backwards, but use max() defensively.
    this.#backupWatermark = max(backedUpWatermark, this.#backupWatermark);
    this.#maybeAck();
  }

  trackArchive(durableWatermark: string) {
    // Watermarks should never move backwards, but use max() defensively.
    this.#archiveWatermark = max(durableWatermark, this.#archiveWatermark);
    this.#maybeAck();
  }

  #maybeAck() {
    const tracked: string[] = [];
    if (this.#trackPgChangeLog) {
      tracked.push(this.#pgChangeLogWatermark);
    }
    if (this.#trackBackup) {
      tracked.push(this.#backupWatermark);
    }
    if (this.#trackArchive) {
      tracked.push(this.#archiveWatermark);
    }
    const currentWatermark = min(...(tracked as [string, ...string[]]));
    if (currentWatermark > this.#lastAck) {
      this.#upstream?.push([
        'status',
        {tag: 'commit'},
        {watermark: currentWatermark},
      ]);
      this.#lastAck = currentWatermark;
    }
    // If all committed transactions have been acked, ack any outstanding
    // status LSN's thereafter (i.e. non-publication upstream changes).
    if (this.#lastAck >= this.#lastTx && this.#lastStatus > this.#lastAck) {
      this.#upstream?.push([
        'status',
        {ack: true},
        {watermark: this.#lastStatus},
      ]);
      this.#lastAck = this.#lastStatus;
    }
  }
}
