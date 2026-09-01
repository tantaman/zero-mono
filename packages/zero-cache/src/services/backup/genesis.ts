import type {LogContext} from '@rocicorp/logger';
import * as v from '../../../../shared/src/valita.ts';
import {
  basePrefix,
  lineagePrefix,
  parseBaseCompleteKey,
} from './archive/layout.ts';
import {
  ObjectNotFoundError,
  type ObjectStore,
} from './object-store/object-store.ts';

/**
 * Lineage genesis: the producer owns initial sync, but the table copy must
 * happen at the replication slot's consistent snapshot, which is visible
 * only to the session that created the slot. The handoff (resolving the
 * design's open question 6) is a request/response through the store, like
 * the accelerated live-base protocol:
 *
 * 1. The gateway creates the slot, exports the slot's snapshot
 *    (`pg_export_snapshot` in the creating session), and publishes a
 *    **genesis offer** under the new lineage — then holds the creating
 *    transaction open, watching for the first complete base.
 * 2. The producer polls for the offer, copies the published tables at the
 *    offered snapshot into its working file (the real initial-sync code,
 *    fed the provided snapshot instead of creating a slot), heartbeating
 *    while it works, and publishes the result as the lineage's first base.
 * 3. The gateway sees the base, releases the snapshot transaction, and
 *    starts streaming; the producer resumes ordinary tailing.
 *
 * A producer crash mid-copy goes stale (no heartbeat) and the gateway
 * abandons the offer — deleting it, dropping the slot, and starting over —
 * so a half-copied genesis can never be mistaken for a live one. The first
 * base's publication is what commits the genesis; everything before it is
 * re-derivable.
 */

export const GENESIS_OFFER_FORMAT = 'zero-archive-genesis-offer';

const genesisOfferSchema = v.object({
  format: v.literal(GENESIS_OFFER_FORMAT),
  version: v.number(),
  /** The lineage the offer creates (derived from the slot's LSN). */
  replicaVersion: v.string(),
  /** The exported snapshot the copy must read at. */
  snapshotID: v.string(),
  /** The slot's consistent point, as reported at creation. */
  lsn: v.string(),
  /** The offering (gateway) task, for logs. */
  taskID: v.string(),
  offeredAt: v.number(),
});

export type GenesisOffer = v.Infer<typeof genesisOfferSchema>;

const genesisHeartbeatSchema = v.object({
  /** The copying (producer) task, for logs. */
  taskID: v.string(),
  at: v.number(),
});

export type GenesisHeartbeat = v.Infer<typeof genesisHeartbeatSchema>;

const utf8Encoder = new TextEncoder();
const utf8Decoder = new TextDecoder();

export function genesisOfferKey(replicaVersion: string): string {
  return `${lineagePrefix(replicaVersion)}genesis/offer.json`;
}

export function genesisHeartbeatKey(replicaVersion: string): string {
  return `${lineagePrefix(replicaVersion)}genesis/heartbeat.json`;
}

export function encodeGenesisOffer(offer: GenesisOffer): Uint8Array {
  return utf8Encoder.encode(JSON.stringify(offer));
}

export function decodeGenesisOffer(data: Uint8Array): GenesisOffer {
  return v.parse(JSON.parse(utf8Decoder.decode(data)), genesisOfferSchema);
}

/** The lineage's genesis offer, or undefined if none is posted. */
export async function readGenesisOffer(
  store: ObjectStore,
  replicaVersion: string,
): Promise<GenesisOffer | undefined> {
  try {
    return decodeGenesisOffer(await store.get(genesisOfferKey(replicaVersion)));
  } catch (e) {
    if (e instanceof ObjectNotFoundError) {
      return undefined;
    }
    throw e;
  }
}

/** Records copy progress; the gateway abandons a genesis that goes stale. */
export async function writeGenesisHeartbeat(
  store: ObjectStore,
  replicaVersion: string,
  taskID: string,
  at = Date.now(),
): Promise<void> {
  await store.put(
    genesisHeartbeatKey(replicaVersion),
    utf8Encoder.encode(JSON.stringify({taskID, at} satisfies GenesisHeartbeat)),
  );
}

export type AwaitGenesisOptions = {
  /**
   * The copy is abandoned when no heartbeat lands within this window (which
   * also covers the time until a producer first picks the offer up).
   */
  heartbeatTimeoutMs: number;
  pollIntervalMs?: number | undefined;
  /** Overridable for tests. */
  now?: (() => number) | undefined;
  setTimeoutFn?: typeof setTimeout | undefined;
};

export type AwaitGenesisResult =
  /** The first base is complete; the snapshot transaction can be released. */
  | 'published'
  /** No live producer; the offer was withdrawn. Drop the slot and retry. */
  | 'abandoned';

/**
 * The gateway's wait for the producer to complete a genesis it offered:
 * resolves `published` when the lineage's first complete base lands, or
 * `abandoned` — after withdrawing the offer — when no producer heartbeat
 * arrives within the timeout. The caller holds the snapshot's transaction
 * open for exactly the lifetime of this call.
 */
export async function awaitGenesisBase(
  lc: LogContext,
  store: ObjectStore,
  replicaVersion: string,
  options: AwaitGenesisOptions,
): Promise<AwaitGenesisResult> {
  lc = lc.withContext('component', 'genesis');
  const {heartbeatTimeoutMs} = options;
  const pollIntervalMs = options.pollIntervalMs ?? 1000;
  const now = options.now ?? Date.now;
  const setTimeoutFn = options.setTimeoutFn ?? setTimeout;

  let lastProgress = now();
  for (;;) {
    const objects = await store.list(basePrefix(replicaVersion));
    if (
      objects.some(
        o => parseBaseCompleteKey(replicaVersion, o.key) !== undefined,
      )
    ) {
      lc.info?.(`genesis base for ${replicaVersion} is complete`);
      await cleanupGenesis(store, replicaVersion);
      return 'published';
    }
    const heartbeat = await readHeartbeat(store, replicaVersion);
    if (heartbeat !== undefined && heartbeat.at > lastProgress) {
      lastProgress = heartbeat.at;
    }
    if (now() - lastProgress > heartbeatTimeoutMs) {
      lc.warn?.(
        `no genesis progress for ${replicaVersion} in ${heartbeatTimeoutMs}ms; ` +
          `withdrawing the offer`,
      );
      await cleanupGenesis(store, replicaVersion);
      return 'abandoned';
    }
    await new Promise(resolve => setTimeoutFn(resolve, pollIntervalMs));
  }
}

/** Removes the offer and heartbeat once a genesis concludes either way. */
export async function cleanupGenesis(
  store: ObjectStore,
  replicaVersion: string,
): Promise<void> {
  await store.delete(genesisOfferKey(replicaVersion)).catch(() => {});
  await store.delete(genesisHeartbeatKey(replicaVersion)).catch(() => {});
}

async function readHeartbeat(
  store: ObjectStore,
  replicaVersion: string,
): Promise<GenesisHeartbeat | undefined> {
  try {
    return v.parse(
      JSON.parse(
        utf8Decoder.decode(
          await store.get(genesisHeartbeatKey(replicaVersion)),
        ),
      ),
      genesisHeartbeatSchema,
    );
  } catch {
    return undefined; // absent or unreadable: no progress signal
  }
}
