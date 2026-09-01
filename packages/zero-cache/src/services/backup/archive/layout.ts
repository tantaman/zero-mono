/**
 * The key layout of the logical backup archive. Everything is namespaced by
 * `replicaVersion`: a resync starts a new generation and therefore a new
 * archive lineage, mirroring the upstream `replicas` table's per-generation
 * `backupPath` scheme.
 *
 * ```
 * v1/<replicaVersion>/log/<start>-<end>.seg     sealed change-stream segments
 * v1/<replicaVersion>/log/<start>-.<watermark>.NNNNNNNN.seg
 *                                               interior parts of a
 *                                               transaction spanning segments
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
 *
 * A transaction larger than the segment target spans a **part chain**:
 * interior parts named by the spanning transaction's commit watermark and a
 * fixed-width part number, then a final part named like an ordinary segment
 * (`<start>-<watermark>.seg`, since the chain ends at the transaction's
 * commit). `.` sorts before every watermark character, so a chain lists as
 * `05-.0g.00000001.seg, 05-.0g.00000002.seg, 05-0g.seg` — interior parts
 * immediately before their final. Only the final part advances the durable
 * cursor, and continuity from a listing considers final/ordinary names only:
 * a chain with no final part is re-sent work, not a gap. The watermark in
 * interior names is what keeps retries idempotent: an abandoned chain's
 * debris can never collide with a different transaction's chain.
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

/** The fixed width of interior part numbers, for offset-ordered listings. */
const PART_NUMBER_WIDTH = 8;

export function segmentPartKey(
  replicaVersion: string,
  start: string,
  watermark: string,
  part: number,
): string {
  return (
    `${logPrefix(replicaVersion)}${start}-.${watermark}.` +
    `${String(part).padStart(PART_NUMBER_WIDTH, '0')}.seg`
  );
}

export type SegmentPartRef = {
  key: string;
  /** Exclusive: the watermark the part's chain resumes after. */
  start: string;
  /** The commit watermark of the transaction spanning the chain. */
  watermark: string;
  /** 1-based position in the chain. */
  part: number;
};

const SEGMENT_PART_NAME = /^([0-9a-z]+)-\.([0-9a-z]+)\.([0-9]{8})\.seg$/;

/**
 * Parses a listed key relative to {@link logPrefix} as an interior part
 * name, or `undefined` for any other key.
 */
export function parseSegmentPartKey(
  replicaVersion: string,
  key: string,
): SegmentPartRef | undefined {
  const prefix = logPrefix(replicaVersion);
  if (!key.startsWith(prefix)) {
    return undefined;
  }
  const match = SEGMENT_PART_NAME.exec(key.slice(prefix.length));
  if (match === null) {
    return undefined;
  }
  const [, start, watermark, part] = match;
  return {key, start, watermark, part: Number(part)};
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

/**
 * Live-base request markers: a restorer asks the producer for a fresh base
 * by writing a marker here; the producer polls the prefix, publishes (or
 * determines the newest base is already current), and deletes the marker.
 * Marker names carry no watermark, so they are invisible to every cursor-
 * space listing above.
 */
export function baseRequestPrefix(replicaVersion: string): string {
  return `${basePrefix(replicaVersion)}requests/`;
}

const UNSAFE_KEY_CHARS = /[^A-Za-z0-9._-]/g;
const LEADING_DOT = /^\./;

export function baseRequestKey(
  replicaVersion: string,
  requestID: string,
): string {
  // Request IDs derive from task IDs, which are not constrained to the
  // store's key alphabet; sanitize rather than reject.
  const safe = requestID
    .replace(UNSAFE_KEY_CHARS, '-')
    .replace(LEADING_DOT, '-');
  return `${baseRequestPrefix(replicaVersion)}${safe}.json`;
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
