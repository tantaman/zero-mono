import {createHash} from 'node:crypto';
import {zstdCompressSync} from 'node:zlib';
import {describe, expect, test} from 'vitest';
import type {ChangeStreamData} from '../../change-source/protocol/current/downstream.ts';
import {
  decodeSegment,
  encodeSegment,
  SegmentFormatError,
} from './segment-format.ts';

// Wire-conformant messages, i.e. what the change-streamer serializes: the
// decoder validates against the protocol schema, which is stricter than the
// shapes internal test helpers produce.
const relation = {
  schema: 'public',
  name: 'issues',
  rowKey: {columns: ['issueID']},
};

function tx(
  watermark: string,
  rows = 1,
): {watermark: string; messages: string[]; parsed: ChangeStreamData[]} {
  const parsed: ChangeStreamData[] = [
    ['begin', {tag: 'begin'}, {commitWatermark: watermark}],
  ];
  for (let i = 0; i < rows; i++) {
    parsed.push([
      'data',
      {tag: 'insert', relation, new: {issueID: `${watermark}-${i}`}},
    ]);
  }
  parsed.push(['commit', {tag: 'commit'}, {watermark}]);
  return {watermark, messages: parsed.map(m => JSON.stringify(m)), parsed};
}

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
