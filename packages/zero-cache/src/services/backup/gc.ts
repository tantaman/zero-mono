import type {LogContext} from '@rocicorp/logger';
import {listLogSegments} from './archive/archive-reader.ts';
import {
  baseCompleteKey,
  baseIntentKey,
  basePrefix,
  parseBaseCompleteKey,
  type SegmentRef,
} from './archive/layout.ts';
import {decodeBaseIntent, decodeBaseManifest} from './base/manifest.ts';
import type {ObjectStore} from './object-store/object-store.ts';

/**
 * Garbage collection of the archive, operating purely in cursor space. Only
 * run in backup mode `archive` (where the archive is authoritative and owns
 * its retention); the mode gating lives in config validation and the caller.
 *
 * Retention rules:
 * - The newest {@link GCOptions.retainBases} complete bases are always kept.
 * - Restoring to any point inside the PITR window must remain possible, so
 *   every base completed inside the window is kept, plus the newest base
 *   completed before it (which serves the window's left edge).
 * - Log segments are kept from the oldest retained base's cursor through the
 *   current archive head; a segment entirely at or below that floor is
 *   reclaimable.
 * - A crashed publication (an `intent.json` without a `complete.json`) is
 *   invisible to restore; its debris is reclaimed once its intent is older
 *   than the debris grace period, which is long enough that it cannot be an
 *   in-flight publication.
 *
 * Deletion order makes interrupted GC safe: a base's `complete.json` goes
 * first, so a partially-deleted base is never listed as complete, and the
 * remaining debris is reclaimed by a later cycle via the intent rule.
 */

export type GCOptions = {
  /** Minimum newest complete bases retained. At least 2 (config-enforced). */
  retainBases: number;
  /** The point-in-time-recovery window, in hours. */
  pitrHours: number;
  /**
   * Age beyond which an incomplete publication's debris is reclaimed.
   * Defaults to 24 hours.
   */
  debrisGraceHours?: number | undefined;
};

export type BaseInventoryEntry = {
  cursor: string;
  /** From the base's manifest. */
  completedAt: number;
};

export type ArchiveInventory = {
  /** Complete bases, any order. */
  bases: BaseInventoryEntry[];
  /** Log segments, in watermark order. */
  segments: SegmentRef[];
};

export type GCPlan = {
  retainedBaseCursors: string[];
  deletedBaseCursors: string[];
  deletedSegments: SegmentRef[];
  /** Segments with `end <= segmentFloor` are reclaimable. */
  segmentFloor: string | undefined;
};

const DEFAULT_DEBRIS_GRACE_HOURS = 24;
const HOUR_MS = 3600 * 1000;

/** The pure cursor-space retention computation, separated for testability. */
export function computeGCPlan(
  inventory: ArchiveInventory,
  opts: GCOptions,
  nowMs: number,
): GCPlan {
  const bases = inventory.bases.toSorted((a, b) =>
    a.cursor < b.cursor ? -1 : a.cursor > b.cursor ? 1 : 0,
  );
  const pitrStartMs = nowMs - opts.pitrHours * HOUR_MS;

  const retained = new Set<string>();
  for (const base of bases.slice(-opts.retainBases)) {
    retained.add(base.cursor);
  }
  // Every base completed inside the PITR window, plus the newest one
  // completed before it.
  for (let i = bases.length - 1; i >= 0; i--) {
    retained.add(bases[i].cursor);
    if (bases[i].completedAt <= pitrStartMs) {
      break;
    }
  }

  const retainedBaseCursors = bases
    .map(({cursor}) => cursor)
    .filter(cursor => retained.has(cursor));
  const deletedBaseCursors = bases
    .map(({cursor}) => cursor)
    .filter(cursor => !retained.has(cursor));

  // No retained base means no floor: with nothing to restore from, segments
  // are the only history and are all kept.
  const segmentFloor = retainedBaseCursors.at(0);
  const deletedSegments =
    segmentFloor === undefined
      ? []
      : inventory.segments.filter(({end}) => end <= segmentFloor);
  return {
    retainedBaseCursors,
    deletedBaseCursors,
    deletedSegments,
    segmentFloor,
  };
}

export type GCResult = GCPlan & {
  /** Cursors of incomplete publications whose debris was reclaimed. */
  deletedDebrisCursors: string[];
};

/**
 * Lists the lineage's inventory, computes the plan, and executes it.
 */
export async function runArchiveGC(
  lc: LogContext,
  store: ObjectStore,
  replicaVersion: string,
  opts: GCOptions,
  nowMs = Date.now(),
): Promise<GCResult> {
  lc = lc.withContext('component', 'archive-gc');
  const baseObjects = await store.list(basePrefix(replicaVersion));
  const completeCursors = new Set<string>();
  for (const {key} of baseObjects) {
    const cursor = parseBaseCompleteKey(replicaVersion, key);
    if (cursor !== undefined) {
      completeCursors.add(cursor);
    }
  }
  const bases: BaseInventoryEntry[] = [];
  for (const cursor of completeCursors) {
    const manifest = decodeBaseManifest(
      await store.get(baseCompleteKey(replicaVersion, cursor)),
    );
    bases.push({cursor, completedAt: manifest.completedAt});
  }
  const segments = await listLogSegments(store, replicaVersion);

  const plan = computeGCPlan({bases, segments}, opts, nowMs);

  for (const cursor of plan.deletedBaseCursors) {
    await deleteBase(store, replicaVersion, cursor, baseObjects, true);
  }

  // Incomplete publications: reclaim debris once the intent is old enough
  // that it cannot be in flight.
  const graceMs =
    (opts.debrisGraceHours ?? DEFAULT_DEBRIS_GRACE_HOURS) * HOUR_MS;
  const deletedDebrisCursors: string[] = [];
  const prefix = basePrefix(replicaVersion);
  for (const {key} of baseObjects) {
    const cursor = INTENT_NAME.exec(key.slice(prefix.length))?.[1];
    if (cursor === undefined || completeCursors.has(cursor)) {
      continue;
    }
    try {
      const intent = decodeBaseIntent(await store.get(key));
      if (intent.startedAt > nowMs - graceMs) {
        continue;
      }
    } catch (e) {
      lc.warn?.(`reclaiming unreadable intent ${key}`, e);
    }
    await deleteBase(store, replicaVersion, cursor, baseObjects, false);
    deletedDebrisCursors.push(cursor);
  }

  for (const segment of plan.deletedSegments) {
    await store.delete(segment.key);
  }

  if (
    plan.deletedBaseCursors.length +
      plan.deletedSegments.length +
      deletedDebrisCursors.length >
    0
  ) {
    lc.info?.(
      `collected ${plan.deletedBaseCursors.length} base(s), ` +
        `${deletedDebrisCursors.length} incomplete publication(s), and ` +
        `${plan.deletedSegments.length} segment(s) at or below ` +
        `${plan.segmentFloor}; retaining bases [${plan.retainedBaseCursors}]`,
    );
  }
  return {...plan, deletedDebrisCursors};
}

const INTENT_NAME = /^([0-9a-z]+)\/intent\.json$/;

/**
 * Deletes one base: `complete.json` first (so an interruption never leaves a
 * chunkless base listed as complete), then chunks, then the intent.
 */
async function deleteBase(
  store: ObjectStore,
  replicaVersion: string,
  cursor: string,
  listing: {key: string}[],
  complete: boolean,
): Promise<void> {
  if (complete) {
    await store.delete(baseCompleteKey(replicaVersion, cursor));
  }
  const chunkPrefix = `${basePrefix(replicaVersion)}${cursor}/chunk/`;
  for (const {key} of listing) {
    if (key.startsWith(chunkPrefix)) {
      await store.delete(key);
    }
  }
  await store.delete(baseIntentKey(replicaVersion, cursor));
}
