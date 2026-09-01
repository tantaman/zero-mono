import {createHash} from 'node:crypto';
import {
  closeSync,
  createReadStream,
  createWriteStream,
  openSync,
  writeSync,
} from 'node:fs';
import {open} from 'node:fs/promises';
import {Transform, type Readable} from 'node:stream';
import {pipeline} from 'node:stream/promises';
import {
  createZstdCompress,
  createZstdDecompress,
  zstdCompressSync,
  zstdDecompressSync,
} from 'node:zlib';
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

export type SegmentHeader = v.Infer<typeof segmentHeaderSchema>;

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

/**
 * The streaming counterpart of {@link encodeSegment}: seals a segment to a
 * local file, holding only the compression window and a hash context in
 * memory regardless of segment size. `body` produces the payload's message
 * bytes — each message prefixed by its `\n` separator, which is exactly what
 * the segment spool stores — and is compressed behind the JSON header line.
 * The checksum of the compressed payload is computed as the bytes pass and
 * patched into the preamble afterwards, so the sealed file is complete
 * before anything uploads it.
 */
export async function writeSealedSegmentFile(
  header: SegmentHeader,
  body: () => Readable,
  outPath: string,
): Promise<void> {
  const hash = createHash('sha256');
  const out = createWriteStream(outPath);
  const preamble = new Uint8Array(HEADER_BYTES);
  preamble.set(MAGIC, 0);
  preamble[MAGIC.length] = FORMAT_VERSION; // checksum bytes stay zero for now
  out.write(preamble);

  const headerLine = Buffer.from(JSON.stringify(header), 'utf8');
  await pipeline(
    async function* () {
      yield headerLine;
      yield* body();
    },
    createZstdCompress(),
    new Transform({
      transform(chunk: Buffer, _encoding, callback) {
        hash.update(chunk);
        callback(null, chunk);
      },
    }),
    out,
  );

  const fd = openSync(outPath, 'r+');
  try {
    const digest = hash.digest();
    writeSync(fd, digest, 0, digest.length, MAGIC.length + 1);
  } finally {
    closeSync(fd);
  }
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

/** One stream message, tagged with its transaction's commit watermark. */
export type SegmentMessage = {
  watermark: string;
  message: ChangeStreamData;
};

/**
 * Verifies a downloaded segment file's framing and checksum in a streaming
 * pass, throwing {@link SegmentFormatError} on any mismatch. This is the
 * verify-then-use half of the streaming decode: the local file is the
 * bounded buffer that lets verification complete before anything is parsed,
 * without ever holding the segment in memory.
 */
export async function verifySegmentFile(path: string): Promise<void> {
  const file = await open(path, 'r');
  try {
    const preamble = Buffer.alloc(HEADER_BYTES);
    const {bytesRead} = await file.read(preamble, 0, HEADER_BYTES, 0);
    if (bytesRead < HEADER_BYTES) {
      throw new SegmentFormatError(
        `segment is truncated: ${bytesRead} bytes is smaller than the header`,
      );
    }
    for (let i = 0; i < MAGIC.length; i++) {
      if (preamble[i] !== MAGIC[i]) {
        throw new SegmentFormatError(
          'segment does not start with the ZARC magic',
        );
      }
    }
    const version = preamble[MAGIC.length];
    if (version !== FORMAT_VERSION) {
      throw new SegmentFormatError(
        `unsupported segment format version ${version}`,
      );
    }
    const hash = createHash('sha256');
    for await (const chunk of file.createReadStream({
      start: HEADER_BYTES,
      autoClose: false,
    })) {
      hash.update(chunk as Buffer);
    }
    if (!hash.digest().equals(preamble.subarray(MAGIC.length + 1))) {
      throw new SegmentFormatError('segment checksum mismatch');
    }
  } finally {
    await file.close();
  }
}

/**
 * The streaming counterpart of {@link decodeSegment}: verifies a segment
 * file ({@link verifySegmentFile}), then decompresses and parses it one
 * message line at a time, yielding each message tagged with its
 * transaction's commit watermark. Memory is O(largest single message) — the
 * unit the applier consumes anyway — never O(transaction) or O(segment).
 *
 * Structural validation matches {@link decodeSegment} exactly; checks that
 * need the whole segment (txCount, the header's `end`) throw at the end of
 * iteration, after the checksum has already vouched that the content is the
 * sealed bytes. `expected` additionally pins the header to the range the
 * object's name claims, so a renamed or misplaced object cannot smuggle the
 * wrong range into a replay.
 */
export async function* decodeSegmentFile(
  path: string,
  expected?: {replicaVersion: string; start: string; end: string},
): AsyncGenerator<SegmentMessage> {
  await verifySegmentFile(path);

  const unzip = createZstdDecompress();
  const source = createReadStream(path, {start: HEADER_BYTES});
  source.on('error', e => unzip.destroy(e));
  // A consumer that stops early destroys `unzip` (async-iterator cleanup);
  // the source must not outlive it holding an open fd.
  unzip.on('close', () => source.destroy());
  source.pipe(unzip);

  let header: SegmentHeader | undefined;
  let current: string | undefined;
  let last: string | undefined;
  let txCount = 0;
  let i = 0;
  for await (const line of decompressedLines(unzip)) {
    if (header === undefined) {
      header = parseLine(line, segmentHeaderSchema, 'header');
      if (
        expected !== undefined &&
        (header.replicaVersion !== expected.replicaVersion ||
          header.start !== expected.start ||
          header.end !== expected.end)
      ) {
        throw new SegmentFormatError(
          `segment contains ${header.replicaVersion}/${header.start}-` +
            `${header.end}; expected ${expected.replicaVersion}/` +
            `${expected.start}-${expected.end}`,
        );
      }
      last = header.start;
      continue;
    }
    i++;
    const message = parseLine(line, changeStreamDataSchema, `message ${i}`);
    const [type] = message;
    if (type === 'begin') {
      if (current !== undefined) {
        throw new SegmentFormatError(
          `message ${i}: begin inside a transaction`,
        );
      }
      const watermark = message[2].commitWatermark;
      if (last !== undefined && watermark <= last) {
        throw new SegmentFormatError(
          `message ${i}: watermark ${watermark} is not after ${last}`,
        );
      }
      current = watermark;
      yield {watermark, message};
    } else if (current === undefined) {
      throw new SegmentFormatError(
        `message ${i}: ${type} outside of a transaction`,
      );
    } else if (type === 'commit') {
      if (message[2].watermark !== current) {
        throw new SegmentFormatError(
          `message ${i}: commit watermark ${message[2].watermark} does not ` +
            `match begin watermark ${current}`,
        );
      }
      yield {watermark: current, message};
      last = current;
      txCount++;
      current = undefined;
    } else if (type === 'rollback') {
      // Sealing only ever includes committed transactions.
      throw new SegmentFormatError(
        `message ${i}: rollback in a sealed segment`,
      );
    } else {
      yield {watermark: current, message};
    }
  }
  if (header === undefined) {
    throw new SegmentFormatError('segment header is not JSON: empty payload');
  }
  if (current !== undefined) {
    throw new SegmentFormatError(`segment ends inside transaction ${current}`);
  }
  if (txCount !== header.txCount) {
    throw new SegmentFormatError(
      `header txCount ${header.txCount} does not match ${txCount} transactions`,
    );
  }
  if (txCount === 0) {
    throw new SegmentFormatError('segment contains no transactions');
  }
  if (last !== header.end) {
    throw new SegmentFormatError(
      `header end ${header.end} does not match last commit watermark ${last}`,
    );
  }
}

/**
 * {@link linesOf} with zstd failures rewrapped: a checksum-verified payload
 * that does not decompress is still reported as a {@link SegmentFormatError}.
 */
async function* decompressedLines(
  source: AsyncIterable<Buffer>,
): AsyncGenerator<string> {
  try {
    yield* linesOf(source);
  } catch (e) {
    if (e instanceof SegmentFormatError) {
      throw e;
    }
    throw new SegmentFormatError(`segment payload does not decompress: ${e}`);
  }
}

/**
 * Splits a byte stream into `\n`-separated UTF-8 lines without ever decoding
 * a partial multi-byte sequence; only the current (partial) line is buffered.
 */
async function* linesOf(source: AsyncIterable<Buffer>): AsyncGenerator<string> {
  const pending: Buffer[] = [];
  for await (const chunk of source) {
    let start = 0;
    for (;;) {
      const newline = chunk.indexOf(0x0a, start);
      if (newline < 0) {
        break;
      }
      const head = chunk.subarray(start, newline);
      if (pending.length > 0) {
        pending.push(head);
        yield Buffer.concat(pending).toString('utf8');
        pending.length = 0;
      } else {
        yield head.toString('utf8');
      }
      start = newline + 1;
    }
    if (start < chunk.length) {
      pending.push(chunk.subarray(start));
    }
  }
  if (pending.length > 0) {
    yield Buffer.concat(pending).toString('utf8');
  }
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
