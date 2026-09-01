import {createWriteStream} from 'node:fs';
import {rm} from 'node:fs/promises';
import {join} from 'node:path';
import {Readable} from 'node:stream';
import {pipeline} from 'node:stream/promises';
import type {ReadableStream as NodeReadableStream} from 'node:stream/web';
import type {ObjectStore} from '../object-store/object-store.ts';
import {
  logPrefix,
  parseSegmentKey,
  parseSegmentPartKey,
  type SegmentPartRef,
  type SegmentRef,
} from './layout.ts';
import {
  decodeSegment,
  decodeSegmentFile,
  type SegmentFileRole,
  type SegmentMessage,
  type SegmentTransaction,
} from './segment-format.ts';

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

/**
 * Lists and parses the lineage's log objects, in watermark order:
 * `segments` are ordinary segments and chain-final parts — the names that
 * establish continuity — and `parts` the interior parts of transaction
 * chains. An interior part whose chain has no final is re-sent work and
 * simply never matches a covering segment.
 */
export async function listLogObjects(
  store: ObjectStore,
  replicaVersion: string,
): Promise<{segments: SegmentRef[]; parts: SegmentPartRef[]}> {
  const objects = await store.list(logPrefix(replicaVersion));
  const segments: SegmentRef[] = [];
  const parts: SegmentPartRef[] = [];
  for (const {key} of objects) {
    const segment = parseSegmentKey(replicaVersion, key);
    if (segment !== undefined) {
      segments.push(segment);
      continue;
    }
    const part = parseSegmentPartKey(replicaVersion, key);
    if (part !== undefined) {
      parts.push(part);
    }
  }
  return {segments, parts};
}

/**
 * Lists and parses the lineage's segments (ordinary and chain-final), in
 * watermark order. These are the names continuity is judged by.
 */
export async function listLogSegments(
  store: ObjectStore,
  replicaVersion: string,
): Promise<SegmentRef[]> {
  return (await listLogObjects(store, replicaVersion)).segments;
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
 * The streaming replay path: iterates the messages of the committed
 * transactions in `(after, upTo]` in stream order, tagged with their commit
 * watermarks. Each covering segment is downloaded to a local temp file in
 * `tempDir` (the bounded buffer that preserves verify-then-use: its size is
 * the segment's, never resident), checksum-verified in a streaming pass, and
 * then decoded one message line at a time — see
 * {@link decodeSegmentFile}, which also pins each segment's header to the
 * range its object name claims. Transactions outside the range (a segment
 * can straddle either bound) are filtered by commit watermark, which is
 * where replay dedup lives for the stream's other consumers too.
 */
export async function* iterateMessages(
  store: ObjectStore,
  replicaVersion: string,
  after: string,
  upTo: string,
  tempDir: string,
): AsyncGenerator<SegmentMessage> {
  const {segments, parts} = await listLogObjects(store, replicaVersion);
  for (const ref of selectCoveringSegments(segments, after, upTo)) {
    // A segment completing a transaction chain is preceded by its interior
    // parts, streamed in order; decodeSegmentFile carries the transaction
    // across the file boundaries and pins each file to its chain position.
    const chain = chainOf(parts, ref);
    const files: {key: string; role: SegmentFileRole}[] = [
      ...chain.map(part => ({
        key: part.key,
        role: {
          kind: 'interior' as const,
          replicaVersion,
          start: part.start,
          watermark: part.watermark,
          part: part.part,
        },
      })),
      {
        key: ref.key,
        role: {
          kind: 'segment' as const,
          replicaVersion,
          start: ref.start,
          end: ref.end,
          parts: chain.length,
        },
      },
    ];
    for (const {key, role} of files) {
      for await (const item of messagesOf(store, key, role, tempDir)) {
        if (item.watermark <= after) {
          continue;
        }
        if (item.watermark > upTo) {
          return;
        }
        yield item;
      }
    }
  }
}

/**
 * The interior parts of the chain `ref` completes — consecutive from part 1
 * — or an empty array for an ordinary segment. A broken sequence is a
 * continuity error: the final part exists, so its chain was fully uploaded,
 * and a missing interior means the archive cannot reproduce the
 * transaction.
 */
function chainOf(parts: SegmentPartRef[], ref: SegmentRef): SegmentPartRef[] {
  const chain = parts
    .filter(part => part.start === ref.start && part.watermark === ref.end)
    .toSorted((a, b) => a.part - b.part);
  chain.forEach((part, i) => {
    if (part.part !== i + 1) {
      throw new ArchiveContinuityError(
        `the chain completed by ${ref.key} is missing interior part ${i + 1}`,
      );
    }
  });
  return chain;
}

/**
 * Downloads one log object to a temp file (the bounded buffer that
 * preserves verify-then-use) and streams its decoded messages.
 */
async function* messagesOf(
  store: ObjectStore,
  key: string,
  role: SegmentFileRole,
  tempDir: string,
): AsyncGenerator<SegmentMessage> {
  const tempFile = join(tempDir, `${crypto.randomUUID()}.seg`);
  try {
    await pipeline(
      // The DOM and node:stream/web ReadableStream types disagree on BYOB
      // reader details; the runtime objects are interchangeable.
      Readable.fromWeb(
        (await store.getStream(
          key,
        )) as unknown as NodeReadableStream<Uint8Array>,
      ),
      createWriteStream(tempFile),
    );
    yield* decodeSegmentFile(tempFile, role);
  } finally {
    await rm(tempFile, {force: true});
  }
}

/**
 * Iterates the committed transactions in `(after, upTo]` in stream order,
 * downloading, verifying, and decoding each covering segment **in memory**.
 * For tooling and small-segment tests only — the replay path is
 * {@link iterateMessages}, which never holds a whole segment or transaction
 * resident and also speaks transaction part chains (this iterator rejects
 * them). Transactions outside the range (a segment can straddle either
 * bound) are filtered by commit watermark, which is where replay dedup lives
 * for the stream's other consumers too.
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
