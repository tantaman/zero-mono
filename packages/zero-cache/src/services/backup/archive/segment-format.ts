import {createHash} from 'node:crypto';
import {zstdCompressSync, zstdDecompressSync} from 'node:zlib';
import * as v from '../../../../../shared/src/valita.ts';
import {
  changeStreamDataSchema,
  type ChangeStreamData,
} from '../../change-source/protocol/current/downstream.ts';

/**
 * The versioned on-object framing of an archive log segment.
 *
 * A segment is a sealed, compressed, checksummed range of the committed
 * change stream: the exact JSON-encoded `ChangeStreamData` envelopes
 * (`['begin'|'data'|'commit', ...]` tuples with watermarks) that the applier
 * consumes, which is what guarantees by construction that replaying the
 * archive produces what the applier would have produced.
 *
 * Layout:
 * ```
 * bytes 0..3   magic "ZARC"
 * byte  4      format version (1)
 * bytes 5..36  SHA-256 of the compressed payload
 * bytes 37..   zstd-compressed payload
 * ```
 *
 * The payload is UTF-8 text: a JSON header line
 * (`{replicaVersion, start, end, txCount}`) followed by one JSON message per
 * line. {@link decodeSegment} rejects — rather than tolerates — a checksum
 * mismatch, a malformed transaction sequence, a watermark at or below
 * `start`, non-ascending watermarks, or a header that disagrees with the
 * content, so that corruption is caught at read time instead of surfacing as
 * a wrong replica.
 */

const MAGIC = new Uint8Array([0x5a, 0x41, 0x52, 0x43]); // "ZARC"
const FORMAT_VERSION = 1;
const CHECKSUM_BYTES = 32;
const HEADER_BYTES = MAGIC.length + 1 + CHECKSUM_BYTES;

const segmentHeaderSchema = v.object({
  replicaVersion: v.string(),
  start: v.string(),
  end: v.string(),
  txCount: v.number(),
});

export type SegmentTransaction = {
  /** The transaction's commit watermark. */
  watermark: string;
  /**
   * The transaction's messages — `begin`, `data`*, `commit` — in stream
   * order.
   */
  messages: ChangeStreamData[];
};

export type DecodedSegment = {
  replicaVersion: string;
  /** Exclusive: the watermark this segment resumes after. */
  start: string;
  /** Inclusive: the last commit watermark in the segment. */
  end: string;
  transactions: SegmentTransaction[];
};

export type EncodeSegmentInput = {
  replicaVersion: string;
  start: string;
  /**
   * Committed transactions in stream order, as the JSON strings the
   * change-streamer already produces for each message (avoiding a
   * re-serialization on the hot path).
   */
  transactions: {watermark: string; messages: string[]}[];
};

export class SegmentFormatError extends Error {
  readonly name = 'SegmentFormatError';
}

export function encodeSegment(input: EncodeSegmentInput): {
  data: Uint8Array;
  end: string;
} {
  const {replicaVersion, start, transactions} = input;
  if (transactions.length === 0) {
    throw new SegmentFormatError(
      'a segment must contain at least 1 transaction',
    );
  }
  let end = start;
  for (const {watermark} of transactions) {
    if (watermark <= end) {
      throw new SegmentFormatError(
        `transaction watermark ${watermark} is not after ${end}`,
      );
    }
    end = watermark;
  }
  const header = JSON.stringify({
    replicaVersion,
    start,
    end,
    txCount: transactions.length,
  });
  const lines = [header];
  for (const {messages} of transactions) {
    lines.push(...messages);
  }
  const payload = zstdCompressSync(Buffer.from(lines.join('\n'), 'utf8'));
  const data = new Uint8Array(HEADER_BYTES + payload.length);
  data.set(MAGIC, 0);
  data[MAGIC.length] = FORMAT_VERSION;
  data.set(createHash('sha256').update(payload).digest(), MAGIC.length + 1);
  data.set(payload, HEADER_BYTES);
  return {data, end};
}

export function decodeSegment(data: Uint8Array): DecodedSegment {
  if (data.length < HEADER_BYTES) {
    throw new SegmentFormatError(
      `segment is truncated: ${data.length} bytes is smaller than the header`,
    );
  }
  for (let i = 0; i < MAGIC.length; i++) {
    if (data[i] !== MAGIC[i]) {
      throw new SegmentFormatError(
        'segment does not start with the ZARC magic',
      );
    }
  }
  const version = data[MAGIC.length];
  if (version !== FORMAT_VERSION) {
    throw new SegmentFormatError(
      `unsupported segment format version ${version}`,
    );
  }
  const checksum = data.subarray(MAGIC.length + 1, HEADER_BYTES);
  const payload = data.subarray(HEADER_BYTES);
  const digest = createHash('sha256').update(payload).digest();
  if (!digest.equals(checksum)) {
    throw new SegmentFormatError('segment checksum mismatch');
  }

  let text: string;
  try {
    text = zstdDecompressSync(payload).toString('utf8');
  } catch (e) {
    throw new SegmentFormatError(`segment payload does not decompress: ${e}`);
  }
  const lines = text.split('\n');
  const header = parseLine(lines[0], segmentHeaderSchema, 'header');

  const transactions: SegmentTransaction[] = [];
  let current: SegmentTransaction | undefined;
  let last = header.start;
  for (let i = 1; i < lines.length; i++) {
    const message = parseLine(lines[i], changeStreamDataSchema, `message ${i}`);
    const [type] = message;
    if (type === 'begin') {
      if (current !== undefined) {
        throw new SegmentFormatError(
          `message ${i}: begin inside a transaction`,
        );
      }
      const watermark = message[2].commitWatermark;
      if (watermark <= last) {
        throw new SegmentFormatError(
          `message ${i}: watermark ${watermark} is not after ${last}`,
        );
      }
      current = {watermark, messages: [message]};
    } else if (current === undefined) {
      throw new SegmentFormatError(
        `message ${i}: ${type} outside of a transaction`,
      );
    } else if (type === 'commit') {
      if (message[2].watermark !== current.watermark) {
        throw new SegmentFormatError(
          `message ${i}: commit watermark ${message[2].watermark} does not ` +
            `match begin watermark ${current.watermark}`,
        );
      }
      current.messages.push(message);
      transactions.push(current);
      last = current.watermark;
      current = undefined;
    } else if (type === 'rollback') {
      // Sealing only ever includes committed transactions.
      throw new SegmentFormatError(
        `message ${i}: rollback in a sealed segment`,
      );
    } else {
      current.messages.push(message);
    }
  }
  if (current !== undefined) {
    throw new SegmentFormatError(
      `segment ends inside transaction ${current.watermark}`,
    );
  }
  if (transactions.length !== header.txCount) {
    throw new SegmentFormatError(
      `header txCount ${header.txCount} does not match ${transactions.length} transactions`,
    );
  }
  if (transactions.length === 0) {
    throw new SegmentFormatError('segment contains no transactions');
  }
  if (last !== header.end) {
    throw new SegmentFormatError(
      `header end ${header.end} does not match last commit watermark ${last}`,
    );
  }
  return {
    replicaVersion: header.replicaVersion,
    start: header.start,
    end: header.end,
    transactions,
  };
}

function parseLine<T>(
  line: string | undefined,
  schema: v.Type<T>,
  what: string,
): T {
  let value: unknown;
  try {
    value = JSON.parse(line ?? '');
  } catch (e) {
    throw new SegmentFormatError(`segment ${what} is not JSON: ${e}`);
  }
  try {
    return v.parse(value, schema);
  } catch (e) {
    throw new SegmentFormatError(`invalid segment ${what}: ${e}`);
  }
}
