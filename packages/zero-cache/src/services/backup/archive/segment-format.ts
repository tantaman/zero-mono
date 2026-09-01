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
 * byte  4      format version (2)
 * bytes 5..36  SHA-256 of the compressed payload
 * bytes 37..   zstd-compressed payload
 * ```
 *
 * The payload is UTF-8 text: a JSON header line followed by one JSON message
 * per line. The header carries `{replicaVersion, start, end, txCount}` plus,
 * since format 2, the upstream commit timestamps of the object's first and
 * last transactions (`firstCommitTimeMs`/`lastCommitTimeMs`, the
 * point-in-time index for PITR tooling; null when the change source reports
 * none) and `part` — null for an ordinary segment, or
 * `{number, final, watermark}` for a member of a **part chain**: a
 * transaction larger than the segment target spans interior parts (which end
 * mid-transaction: `end` null, `txCount` 0) and a final part carrying the
 * commit. The decoders reject — rather than tolerate — a checksum mismatch,
 * a malformed transaction sequence, a watermark at or below `start`,
 * non-ascending watermarks, or a header that disagrees with the content or
 * with the range and chain position the caller expects, so that corruption
 * is caught at read time instead of surfacing as a wrong replica.
 */

const MAGIC = new Uint8Array([0x5a, 0x41, 0x52, 0x43]); // "ZARC"
const FORMAT_VERSION = 2;
const CHECKSUM_BYTES = 32;
const HEADER_BYTES = MAGIC.length + 1 + CHECKSUM_BYTES;

const segmentHeaderSchema = v.object({
  replicaVersion: v.string(),
  start: v.string(),
  /** Inclusive last commit watermark; null for interior parts. */
  end: v.string().nullable(),
  /** Committed transactions in this object; 0 for interior parts. */
  txCount: v.number(),
  /** Upstream commit time (ms epoch) of the first/last transaction. */
  firstCommitTimeMs: v.number().nullable(),
  lastCommitTimeMs: v.number().nullable(),
  /** Chain position, or null for an ordinary (single-object) segment. */
  part: v
    .object({
      /** 1-based position in the chain. */
      number: v.number(),
      /** True for the part carrying the commit. */
      final: v.boolean(),
      /** The commit watermark of the transaction spanning the chain. */
      watermark: v.string(),
    })
    .nullable(),
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
  const header: SegmentHeader = {
    replicaVersion,
    start,
    end,
    txCount: transactions.length,
    firstCommitTimeMs: commitTimeOf(transactions[0]),
    lastCommitTimeMs: commitTimeOf(transactions.at(-1)),
    part: null,
  };
  const lines = [JSON.stringify(header)];
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
 * The upstream commit time of an already-serialized transaction, from its
 * final (commit) message. Tooling-path only — the writer tracks commit times
 * from the parsed stream instead of re-parsing.
 */
function commitTimeOf(
  transaction: {messages: string[]} | undefined,
): number | null {
  const last = transaction?.messages.at(-1);
  if (last === undefined) {
    return null;
  }
  try {
    const message = JSON.parse(last) as ChangeStreamData;
    return message[0] === 'commit' ? (message[1].commitTimeMs ?? null) : null;
  } catch {
    return null;
  }
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
  if (header.part !== null) {
    throw new SegmentFormatError(
      `segment is part ${header.part.number} of a transaction chain, which ` +
        `requires the streaming decoder`,
    );
  }
  if (header.end === null) {
    throw new SegmentFormatError('segment header has no end watermark');
  }

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
  /**
   * The message's archived JSON — the exact text the change-streamer
   * originally serialized, so consumers that re-forward the stream (the
   * base producer's change source) never re-serialize.
   */
  json: string;
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
 * What the caller expects the file at hand to be, from its object name and
 * the surrounding listing. A `segment` is an ordinary single-object segment
 * when `parts` is 0, or a chain's final part when its listing showed `parts`
 * interior parts before it; an `interior` is one interior part of a chain.
 */
export type SegmentFileRole =
  | {
      kind: 'segment';
      replicaVersion: string;
      start: string;
      end: string;
      /** Interior parts preceding this object; 0 for an ordinary segment. */
      parts: number;
    }
  | {
      kind: 'interior';
      replicaVersion: string;
      start: string;
      /** The commit watermark of the transaction spanning the chain. */
      watermark: string;
      /** 1-based position in the chain. */
      part: number;
    };

/**
 * The streaming counterpart of {@link decodeSegment}: verifies a segment
 * file ({@link verifySegmentFile}), then decompresses and parses it one
 * message line at a time, yielding each message tagged with its
 * transaction's commit watermark. Memory is O(largest single message) — the
 * unit the applier consumes anyway — never O(transaction) or O(segment).
 *
 * Unlike the in-memory decoder, this one also speaks part chains: an
 * interior part begins the spanning transaction (part 1) or continues it,
 * and ends inside it; only a final part or ordinary segment ends at a
 * commit. Structural validation otherwise matches {@link decodeSegment};
 * checks that need the whole object (txCount, the header's `end`) throw at
 * the end of iteration, after the checksum has already vouched that the
 * content is the sealed bytes. `role` pins the header to the range and
 * chain position the object's name and listing claim, so a renamed or
 * misplaced object cannot smuggle the wrong content into a replay; without
 * it (tooling), the header's own claims are validated for self-consistency.
 */
export async function* decodeSegmentFile(
  path: string,
  role?: SegmentFileRole,
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
      if (role !== undefined) {
        checkRole(header, role);
      } else {
        checkSelfConsistent(header);
      }
      last = header.start;
      if (header.part !== null && header.part.number > 1) {
        // The file continues a transaction begun in an earlier part.
        current = header.part.watermark;
      }
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
      if (header.part !== null && watermark !== header.part.watermark) {
        throw new SegmentFormatError(
          `message ${i}: watermark ${watermark} in a chain part of ` +
            `${header.part.watermark}`,
        );
      }
      current = watermark;
      yield {watermark, message, json: line};
    } else if (current === undefined) {
      throw new SegmentFormatError(
        `message ${i}: ${type} outside of a transaction`,
      );
    } else if (type === 'commit') {
      if (header.part !== null && !header.part.final) {
        throw new SegmentFormatError(
          `message ${i}: commit in interior part ${header.part.number}`,
        );
      }
      if (message[2].watermark !== current) {
        throw new SegmentFormatError(
          `message ${i}: commit watermark ${message[2].watermark} does not ` +
            `match begin watermark ${current}`,
        );
      }
      yield {watermark: current, message, json: line};
      last = current;
      txCount++;
      current = undefined;
    } else if (type === 'rollback') {
      // Sealing only ever includes committed transactions.
      throw new SegmentFormatError(
        `message ${i}: rollback in a sealed segment`,
      );
    } else {
      yield {watermark: current, message, json: line};
    }
  }
  if (header === undefined) {
    throw new SegmentFormatError('segment header is not JSON: empty payload');
  }
  if (header.part !== null && !header.part.final) {
    // An interior part must end inside its (sole) transaction.
    if (current !== header.part.watermark) {
      throw new SegmentFormatError(
        `interior part ${header.part.number} does not end inside ` +
          `transaction ${header.part.watermark}`,
      );
    }
    if (i === 0) {
      throw new SegmentFormatError('interior part contains no messages');
    }
    return;
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

/** Pins a header to the role the object's name and listing claim. */
function checkRole(header: SegmentHeader, role: SegmentFileRole): void {
  checkSelfConsistent(header);
  const mismatch = (what: string) => {
    throw new SegmentFormatError(
      `segment header does not match its name and listing (${what}): ` +
        `${JSON.stringify(header)} vs ${JSON.stringify(role)}`,
    );
  };
  if (
    header.replicaVersion !== role.replicaVersion ||
    header.start !== role.start
  ) {
    mismatch('lineage/start');
  }
  if (role.kind === 'interior') {
    if (
      header.part === null ||
      header.part.final ||
      header.part.number !== role.part ||
      header.part.watermark !== role.watermark
    ) {
      mismatch('chain position');
    }
  } else {
    if (header.end !== role.end) {
      mismatch('end');
    }
    if (role.parts === 0) {
      if (header.part !== null) {
        mismatch('unexpected chain');
      }
    } else if (
      header.part === null ||
      !header.part.final ||
      header.part.number !== role.parts + 1
    ) {
      mismatch('chain position');
    }
  }
}

/** The header invariants that hold with or without a caller-supplied role. */
function checkSelfConsistent(header: SegmentHeader): void {
  const {part, end, txCount} = header;
  if (part === null) {
    if (end === null) {
      throw new SegmentFormatError('segment header has no end watermark');
    }
    return;
  }
  if (part.number < 1) {
    throw new SegmentFormatError(`invalid part number ${part.number}`);
  }
  if (part.final) {
    if (end !== part.watermark || txCount !== 1) {
      throw new SegmentFormatError(
        `final part header is inconsistent: ${JSON.stringify(header)}`,
      );
    }
  } else if (end !== null || txCount !== 0) {
    throw new SegmentFormatError(
      `interior part header is inconsistent: ${JSON.stringify(header)}`,
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
