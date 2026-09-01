import {createHash} from 'node:crypto';
import {mkdtempSync, readFileSync, rmSync, writeFileSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {Readable} from 'node:stream';
import {zstdCompressSync} from 'node:zlib';
import {afterEach, beforeEach, describe, expect, test} from 'vitest';
import {wireTransaction as tx} from '../test-utils.ts';
import {
  decodeSegment,
  decodeSegmentFile,
  encodeSegment,
  SegmentFormatError,
  writeSealedSegmentFile,
  type SegmentMessage,
} from './segment-format.ts';

describe('backup/archive/segment-format', () => {
  test('round trip', () => {
    const txs = [tx('03'), tx('05', 3), tx('07', 0)];
    const {data, end} = encodeSegment({
      replicaVersion: '02',
      start: '02',
      transactions: txs,
    });
    expect(end).toBe('07');

    const decoded = decodeSegment(data);
    expect(decoded.replicaVersion).toBe('02');
    expect(decoded.start).toBe('02');
    expect(decoded.end).toBe('07');
    expect(decoded.transactions.map(t => t.watermark)).toEqual([
      '03',
      '05',
      '07',
    ]);
    expect(decoded.transactions.map(t => t.messages)).toEqual(
      txs.map(t => t.parsed),
    );
  });

  test('rejects an empty segment', () => {
    expect(() =>
      encodeSegment({replicaVersion: '02', start: '02', transactions: []}),
    ).toThrow(SegmentFormatError);
  });

  test('rejects non-ascending watermarks at encode time', () => {
    expect(() =>
      encodeSegment({
        replicaVersion: '02',
        start: '02',
        transactions: [tx('05'), tx('03')],
      }),
    ).toThrow('watermark 03 is not after 05');
    expect(() =>
      encodeSegment({
        replicaVersion: '02',
        start: '05',
        transactions: [tx('05')],
      }),
    ).toThrow('watermark 05 is not after 05');
  });

  test('rejects a flipped bit in the payload', () => {
    const {data} = encodeSegment({
      replicaVersion: '02',
      start: '02',
      transactions: [tx('03')],
    });
    const corrupt = Uint8Array.from(data);
    corrupt[corrupt.length - 1] ^= 0x01;
    expect(() => decodeSegment(corrupt)).toThrow('checksum mismatch');
  });

  test('rejects a flipped bit in the checksum', () => {
    const {data} = encodeSegment({
      replicaVersion: '02',
      start: '02',
      transactions: [tx('03')],
    });
    const corrupt = Uint8Array.from(data);
    corrupt[10] ^= 0x01; // within the sha-256 field
    expect(() => decodeSegment(corrupt)).toThrow('checksum mismatch');
  });

  test('rejects truncation', () => {
    const {data} = encodeSegment({
      replicaVersion: '02',
      start: '02',
      transactions: [tx('03')],
    });
    expect(() => decodeSegment(data.subarray(0, 20))).toThrow('truncated');
    // Truncation within the payload fails the checksum.
    expect(() => decodeSegment(data.subarray(0, data.length - 5))).toThrow(
      SegmentFormatError,
    );
  });

  test('rejects wrong magic and unsupported versions', () => {
    const {data} = encodeSegment({
      replicaVersion: '02',
      start: '02',
      transactions: [tx('03')],
    });
    const wrongMagic = Uint8Array.from(data);
    wrongMagic[0] = 0x00;
    expect(() => decodeSegment(wrongMagic)).toThrow('ZARC magic');

    const wrongVersion = Uint8Array.from(data);
    wrongVersion[4] = 2;
    expect(() => decodeSegment(wrongVersion)).toThrow(
      'unsupported segment format version 2',
    );
  });

  test.each([
    [
      'data outside of a transaction',
      (t: ReturnType<typeof tx>) => [t.messages[1]],
      'outside of a transaction',
    ],
    [
      'begin inside a transaction',
      (t: ReturnType<typeof tx>) => [t.messages[0], t.messages[0]],
      'begin inside a transaction',
    ],
    [
      'unterminated transaction',
      (t: ReturnType<typeof tx>) => t.messages.slice(0, -1),
      'ends inside transaction',
    ],
  ])('rejects a malformed sequence: %s', (_name, mutate, message) => {
    const transaction = tx('03');
    const {data} = encodeSegmentRaw('02', '02', '03', 1, mutate(transaction));
    expect(() => decodeSegment(data)).toThrow(message);
  });

  test('rejects a commit that does not match its begin', () => {
    const transaction = tx('03');
    const lines = [...transaction.messages];
    lines[lines.length - 1] = JSON.stringify([
      'commit',
      {tag: 'commit'},
      {watermark: '04'},
    ]);
    const {data} = encodeSegmentRaw('02', '02', '04', 1, lines);
    expect(() => decodeSegment(data)).toThrow(
      'commit watermark 04 does not match begin watermark 03',
    );
  });

  test('rejects a header that disagrees with the content', () => {
    const transaction = tx('03');
    for (const [end, txCount, message] of [
      ['05', 1, 'does not match last commit watermark'],
      ['03', 2, 'does not match 1 transactions'],
    ] as const) {
      const {data} = encodeSegmentRaw(
        '02',
        '02',
        end,
        txCount,
        transaction.messages,
      );
      expect(() => decodeSegment(data)).toThrow(message);
    }
  });

  test('rejects a transaction at or below the segment start', () => {
    const transaction = tx('03');
    const {data} = encodeSegmentRaw('02', '03', '03', 1, transaction.messages);
    expect(() => decodeSegment(data)).toThrow('watermark 03 is not after 03');
  });

  describe('streaming seal and decode', () => {
    let dir: string;
    let path: string;

    beforeEach(() => {
      dir = mkdtempSync(join(tmpdir(), 'zero-segment-format-test-'));
      path = join(dir, 'segment.seg');
    });

    afterEach(() => {
      rmSync(dir, {recursive: true, force: true});
    });

    /** Seals `txs` to `path` the way the writer's pump does. */
    async function seal(
      start: string,
      txs: ReturnType<typeof tx>[],
      header: Partial<Parameters<typeof writeSealedSegmentFile>[0]> = {},
    ) {
      const lines = txs.flatMap(t => t.messages);
      await writeSealedSegmentFile(
        {
          replicaVersion: '02',
          start,
          end: txs.at(-1)?.watermark ?? start,
          txCount: txs.length,
          ...header,
        },
        () => Readable.from(lines.map(line => Buffer.from(`\n${line}`))),
        path,
      );
    }

    async function collectFile(
      expected?: Parameters<typeof decodeSegmentFile>[1],
    ): Promise<SegmentMessage[]> {
      const items: SegmentMessage[] = [];
      for await (const item of decodeSegmentFile(path, expected)) {
        items.push(item);
      }
      return items;
    }

    test('round-trips against the in-memory decoder and the streaming decoder', async () => {
      const txs = [tx('03'), tx('05', 3), tx('07', 0)];
      await seal('02', txs);

      // The sealed file is byte-compatible with the in-memory decoder.
      const decoded = decodeSegment(readFileSync(path));
      expect(decoded).toMatchObject({
        replicaVersion: '02',
        start: '02',
        end: '07',
      });
      expect(decoded.transactions.map(t => t.messages)).toEqual(
        txs.map(t => t.parsed),
      );

      // The streaming decoder yields the same content, message by message.
      const items = await collectFile({
        replicaVersion: '02',
        start: '02',
        end: '07',
      });
      expect(items.map(i => i.message)).toEqual(txs.flatMap(t => t.parsed));
      expect(items.map(i => i.watermark)).toEqual(
        txs.flatMap(t => t.parsed.map(() => t.watermark)),
      );
    });

    test('rejects a flipped bit before yielding anything', async () => {
      await seal('02', [tx('03')]);
      const corrupt = readFileSync(path);
      corrupt[corrupt.length - 1] ^= 0x01;
      writeFileSync(path, corrupt);

      await expect(collectFile()).rejects.toThrow('checksum mismatch');
    });

    test('rejects truncation', async () => {
      await seal('02', [tx('03')]);
      const data = readFileSync(path);
      writeFileSync(path, data.subarray(0, 20));
      await expect(collectFile()).rejects.toThrow('truncated');

      writeFileSync(path, data.subarray(0, data.length - 5));
      await expect(collectFile()).rejects.toThrow(SegmentFormatError);
    });

    test('rejects a header that disagrees with the expected range', async () => {
      await seal('02', [tx('03')]);
      await expect(
        collectFile({replicaVersion: '02', start: '02', end: '05'}),
      ).rejects.toThrow('expected 02/02-05');
    });

    test('rejects a malformed sequence at the offending message', async () => {
      // An unterminated transaction, sealed with a lying header.
      const lines = tx('03').messages.slice(0, -1);
      await writeSealedSegmentFile(
        {replicaVersion: '02', start: '02', end: '03', txCount: 1},
        () => Readable.from(lines.map(line => Buffer.from(`\n${line}`))),
        path,
      );
      const seen: SegmentMessage[] = [];
      await expect(
        (async () => {
          for await (const item of decodeSegmentFile(path)) {
            seen.push(item);
          }
        })(),
      ).rejects.toThrow('ends inside transaction 03');
      // Yielded messages precede the failure: the caller's transactional
      // apply discards them with the aborted replay.
      expect(seen.length).toBe(lines.length);
    });

    test('rejects a header claiming transactions an empty payload lacks', async () => {
      await writeSealedSegmentFile(
        {replicaVersion: '02', start: '02', end: '03', txCount: 1},
        () => Readable.from([]),
        path,
      );
      await expect(collectFile()).rejects.toThrow(
        'header txCount 1 does not match 0 transactions',
      );
    });
  });
});

/**
 * Bypasses {@link encodeSegment}'s own validation to produce structurally
 * corrupt segments with valid framing, checksum, and compression.
 */
function encodeSegmentRaw(
  replicaVersion: string,
  start: string,
  end: string,
  txCount: number,
  lines: string[],
): {data: Uint8Array} {
  const header = JSON.stringify({replicaVersion, start, end, txCount});
  const payload = zstdCompressSync(
    Buffer.from([header, ...lines].join('\n'), 'utf8'),
  );
  const data = new Uint8Array(4 + 1 + 32 + payload.length);
  data.set([0x5a, 0x41, 0x52, 0x43], 0);
  data[4] = 1;
  data.set(createHash('sha256').update(payload).digest(), 5);
  data.set(payload, 37);
  return {data};
}
