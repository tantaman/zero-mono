import type {ObjectStore} from '../object-store/object-store.ts';
import {logPrefix, parseSegmentKey, type SegmentRef} from './layout.ts';
import {decodeSegment, type SegmentTransaction} from './segment-format.ts';

/**
 * Read-side of the archive log: segment listing, continuity verification,
 * and transaction iteration for tail replay. The reader trusts nothing it
 * did not verify — segment contents are checksummed and structurally
 * validated by `decodeSegment`, and additionally checked against the range
 * their object name claims, so a renamed or misplaced object cannot smuggle
 * the wrong range into a replay.
 */

export class ArchiveContinuityError extends Error {
  readonly name = 'ArchiveContinuityError';
}

/** Lists and parses the lineage's segments, in watermark order. */
export async function listLogSegments(
  store: ObjectStore,
  replicaVersion: string,
): Promise<SegmentRef[]> {
  const objects = await store.list(logPrefix(replicaVersion));
  const segments: SegmentRef[] = [];
  for (const {key} of objects) {
    const segment = parseSegmentKey(replicaVersion, key);
    if (segment !== undefined) {
      segments.push(segment);
    }
  }
  return segments;
}

/**
 * Selects the contiguous chain of segments covering the half-open range
 * `(after, upTo]`, or throws {@link ArchiveContinuityError} when the archive
 * cannot serve it: the range starts before the archived history, ends past
 * it, or a gap or overlap breaks the chain inside it.
 */
export function selectCoveringSegments(
  segments: SegmentRef[],
  after: string,
  upTo: string,
): SegmentRef[] {
  if (upTo <= after) {
    return [];
  }
  const covering: SegmentRef[] = [];
  let cursor: string | undefined;
  for (const segment of segments) {
    if (segment.end <= after) {
      continue; // entirely below the range
    }
    if (cursor === undefined) {
      if (segment.start > after) {
        throw new ArchiveContinuityError(
          `the archive does not cover (${after}..]: its first segment above ` +
            `that range starts at ${segment.start}`,
        );
      }
    } else if (segment.start !== cursor) {
      throw new ArchiveContinuityError(
        `the archive is not contiguous: segment ${segment.start}-${segment.end} ` +
          `does not resume from ${cursor}`,
      );
    }
    covering.push(segment);
    cursor = segment.end;
    if (cursor >= upTo) {
      return covering;
    }
  }
  throw new ArchiveContinuityError(
    `the archive head ${cursor ?? '(empty)'} does not reach ${upTo}`,
  );
}

/**
 * Iterates the committed transactions in `(after, upTo]` in stream order,
 * downloading, verifying, and decoding each covering segment. Transactions
 * outside the range (a segment can straddle either bound) are filtered by
 * commit watermark, which is where replay dedup lives for the stream's other
 * consumers too.
 */
export async function* iterateTransactions(
  store: ObjectStore,
  replicaVersion: string,
  after: string,
  upTo: string,
): AsyncGenerator<SegmentTransaction> {
  const segments = await listLogSegments(store, replicaVersion);
  for (const ref of selectCoveringSegments(segments, after, upTo)) {
    const decoded = decodeSegment(await store.get(ref.key));
    if (
      decoded.replicaVersion !== replicaVersion ||
      decoded.start !== ref.start ||
      decoded.end !== ref.end
    ) {
      throw new ArchiveContinuityError(
        `segment ${ref.key} contains ${decoded.replicaVersion}/` +
          `${decoded.start}-${decoded.end}, which is not the range its name claims`,
      );
    }
    for (const transaction of decoded.transactions) {
      if (transaction.watermark <= after) {
        continue;
      }
      if (transaction.watermark > upTo) {
        return;
      }
      yield transaction;
    }
  }
}
