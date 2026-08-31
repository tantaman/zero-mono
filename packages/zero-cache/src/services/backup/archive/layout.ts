/**
 * The key layout of the logical backup archive. Everything is namespaced by
 * `replicaVersion`: a resync starts a new generation and therefore a new
 * archive lineage, mirroring the upstream `replicas` table's per-generation
 * `backupPath` scheme.
 *
 * ```
 * v1/<replicaVersion>/log/<start>-<end>.seg     sealed change-stream segments
 * v1/<replicaVersion>/base/<cursor>/intent.json
 * v1/<replicaVersion>/base/<cursor>/chunk/<index>
 * v1/<replicaVersion>/base/<cursor>/complete.json
 * ```
 *
 * Watermarks are LexiVersions, in which no value is a proper prefix of a
 * different value, so the lexicographic listing order of these keys is
 * watermark order. A segment's name is deterministic from stream identity and
 * cursor interval — `<start>` is the watermark the segment resumes after
 * (exclusive) and `<end>` its last commit watermark (inclusive) — which makes
 * upload retries idempotent (`putIfAbsent`) rather than duplicating. A base
 * is named by the cursor embedded in its SQLite file, and exists only once
 * its `complete.json` does: the manifest is always published last.
 */

export const ARCHIVE_ROOT = 'v1/';

export function lineagePrefix(replicaVersion: string): string {
  return `${ARCHIVE_ROOT}${replicaVersion}/`;
}

export function logPrefix(replicaVersion: string): string {
  return `${lineagePrefix(replicaVersion)}log/`;
}

export function segmentKey(
  replicaVersion: string,
  start: string,
  end: string,
): string {
  return `${logPrefix(replicaVersion)}${start}-${end}.seg`;
}

export type SegmentRef = {
  key: string;
  /** Exclusive: the watermark this segment resumes after. */
  start: string;
  /** Inclusive: the last commit watermark in this segment. */
  end: string;
};

const SEGMENT_NAME = /^([0-9a-z]+)-([0-9a-z]+)\.seg$/;

/**
 * Parses a listed key relative to {@link logPrefix}. Returns `undefined` for
 * keys that are not segment names (which continuity verification then
 * ignores rather than trips over).
 */
export function parseSegmentKey(
  replicaVersion: string,
  key: string,
): SegmentRef | undefined {
  const prefix = logPrefix(replicaVersion);
  if (!key.startsWith(prefix)) {
    return undefined;
  }
  const match = SEGMENT_NAME.exec(key.slice(prefix.length));
  if (match === null) {
    return undefined;
  }
  const [, start, end] = match;
  return {key, start, end};
}

export function basePrefix(replicaVersion: string): string {
  return `${lineagePrefix(replicaVersion)}base/`;
}

export function baseIntentKey(replicaVersion: string, cursor: string): string {
  return `${basePrefix(replicaVersion)}${cursor}/intent.json`;
}

export function baseChunkKey(
  replicaVersion: string,
  cursor: string,
  index: number,
): string {
  // Fixed-width so that chunk listings sort in offset order.
  return `${basePrefix(replicaVersion)}${cursor}/chunk/${String(index).padStart(8, '0')}`;
}

export function baseCompleteKey(
  replicaVersion: string,
  cursor: string,
): string {
  return `${basePrefix(replicaVersion)}${cursor}/complete.json`;
}

const BASE_COMPLETE_NAME = /^([0-9a-z]+)\/complete\.json$/;

/**
 * Parses a listed key relative to {@link basePrefix}, returning the cursor of
 * a complete base, or `undefined` for every other key under the prefix.
 */
export function parseBaseCompleteKey(
  replicaVersion: string,
  key: string,
): string | undefined {
  const prefix = basePrefix(replicaVersion);
  if (!key.startsWith(prefix)) {
    return undefined;
  }
  return BASE_COMPLETE_NAME.exec(key.slice(prefix.length))?.[1];
}

const LINEAGE_NAME = /^([0-9a-z]+)\/$/;

/**
 * Extracts the set of lineage (i.e. `replicaVersion`) names from a full-store
 * listing of keys under {@link ARCHIVE_ROOT}.
 */
export function lineagesFromKeys(keys: Iterable<string>): string[] {
  const lineages = new Set<string>();
  for (const key of keys) {
    if (!key.startsWith(ARCHIVE_ROOT)) {
      continue;
    }
    const relative = key.slice(ARCHIVE_ROOT.length);
    const slash = relative.indexOf('/');
    if (slash < 0) {
      continue;
    }
    const lineage = relative.slice(0, slash + 1);
    if (LINEAGE_NAME.test(lineage)) {
      lineages.add(lineage.slice(0, -1));
    }
  }
  return [...lineages].toSorted();
}
