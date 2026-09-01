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
  type SegmentHeader,
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

  // The Postgres change source puts more on the wire than the protocol
  // schema names -- `commitLsn`, `commitTime` and `xid` on begin/commit --
  // and the wire parses downstream messages in `passthrough` mode for that
  // reason. A replay is the same message reaching the same applier, so the
  // decoders must accept (and preserve) those fields. Strict parsing here
  // rejected every segment a real Postgres upstream produced while happily
  // accepting the synthetic messages the other tests construct, which is
  // exactly why this one builds the wire shape by hand.
  test('decodes a transaction carrying change-source fields the protocol schema does not name', () => {
    const lines = [
      JSON.stringify([
        'begin',
        {
          tag: 'begin',
          commitLsn: '00000000/01C1C250',
          commitTime: 1788273980565832,
          xid: 914,
          json: 's',
        },
        {commitWatermark: '03'},
      ]),
      JSON.stringify([
        'commit',
        {
          tag: 'commit',
          flags: 0,
          commitLsn: '00000000/01C1C250',
          commitEndLsn: '00000000/01C1F280',
          commitTime: 1788273980565832,
          commitTimeMs: 1788273980565,
        },
        {watermark: '03'},
      ]),
    ];
    const {data} = encodeSegmentRaw('02', '02', '03', 1, lines);

    const decoded = decodeSegment(data);
    expect(decoded.transactions).toHaveLength(1);
    expect(decoded.transactions[0].watermark).toBe('03');
    // The extra fields survive the round trip; the applier reads them.
    expect(decoded.transactions[0].messages[0][1]).toMatchObject({
      tag: 'begin',
      commitLsn: '00000000/01C1C250',
      xid: 914,
    });
    expect(decoded.transactions[0].messages[1][1]).toMatchObject({
      tag: 'commit',
      commitEndLsn: '00000000/01C1F280',
      commitTimeMs: 1788273980565,
    });
  });

  // The writer stores what `serializeChangeStreamData` produced, which renders
  // an int8 past 2^53 as a bare JSON number. Only a bigint-aware parse reads
  // it back intact, which is what the wire does — plain `JSON.parse` rounds
  // it to a double and the restored replica then disagrees with upstream.
  test('preserves integers beyond the safe range', () => {
    const big = 9007199254740993n; // 2^53 + 1
    const lines = [
      JSON.stringify(['begin', {tag: 'begin'}, {commitWatermark: '03'}]),
      // Rendered the way BigIntJSON.stringify renders it: a bare number.
      `["data",{"tag":"insert","relation":{"tag":"relation","schema":"public",` +
        `"name":"t","rowKey":{"type":"default","columns":["id"]}},` +
        `"new":{"id":"a","n":${big}}}]`,
      JSON.stringify(['commit', {tag: 'commit'}, {watermark: '03'}]),
    ];
    const {data} = encodeSegmentRaw('02', '02', '03', 1, lines);

    const decoded = decodeSegment(data);
    const insert = decoded.transactions[0].messages[1][1] as unknown as {
      new: {n: bigint};
    };
    expect(insert.new.n).toBe(big);
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
    wrongVersion[4] = 9;
    expect(() => decodeSegment(wrongVersion)).toThrow(
      'unsupported segment format version 9',
    );
    // Format 1 (which never left the lab) is likewise rejected.
    wrongVersion[4] = 1;
    expect(() => decodeSegment(wrongVersion)).toThrow(
      'unsupported segment format version 1',
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

    /** Seals `lines` to `path` under an arbitrary (possibly lying) header. */
    async function sealRaw(header: SegmentHeader, lines: string[]) {
      await writeSealedSegmentFile(
        header,
        () => Readable.from(lines.map(line => Buffer.from(`\n${line}`))),
        path,
      );
    }

    /** Seals `txs` to `path` the way the writer's pump does. */
    async function seal(start: string, txs: ReturnType<typeof tx>[]) {
      await sealRaw(
        {
          replicaVersion: '02',
          start,
          end: txs.at(-1)?.watermark ?? start,
          txCount: txs.length,
          firstCommitTimeMs: null,
          lastCommitTimeMs: null,
          part: null,
        },
        txs.flatMap(t => t.messages),
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

    test('streams a transaction carrying unnamed change-source fields', async () => {
      // The streaming decoder is the one tail replay runs on; see the
      // in-memory counterpart above for why this shape is built by hand.
      const lines = [
        JSON.stringify([
          'begin',
          {
            tag: 'begin',
            commitLsn: '00000000/01C1C250',
            commitTime: 1788273980565832,
            xid: 914,
            json: 's',
          },
          {commitWatermark: '03'},
        ]),
        JSON.stringify([
          'commit',
          {
            tag: 'commit',
            flags: 0,
            commitLsn: '00000000/01C1C250',
            commitEndLsn: '00000000/01C1F280',
            commitTime: 1788273980565832,
            commitTimeMs: 1788273980565,
          },
          {watermark: '03'},
        ]),
      ];
      await sealRaw(
        {
          replicaVersion: '02',
          start: '02',
          end: '03',
          txCount: 1,
          firstCommitTimeMs: null,
          lastCommitTimeMs: null,
          part: null,
        },
        lines,
      );

      const items = await collectFile();
      expect(items).toHaveLength(2);
      expect(items.map(i => i.watermark)).toEqual(['03', '03']);
      expect(items[0].message[1]).toMatchObject({tag: 'begin', xid: 914});
      expect(items[1].message[1]).toMatchObject({
        tag: 'commit',
        commitEndLsn: '00000000/01C1F280',
      });
    });

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
        kind: 'segment',
        replicaVersion: '02',
        start: '02',
        end: '07',
        parts: 0,
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
        collectFile({
          kind: 'segment',
          replicaVersion: '02',
          start: '02',
          end: '05',
          parts: 0,
        }),
      ).rejects.toThrow('does not match its name and listing (end)');
    });

    test('rejects a malformed sequence at the offending message', async () => {
      // An unterminated transaction, sealed with a lying header.
      const lines = tx('03').messages.slice(0, -1);
      await sealRaw(
        {
          replicaVersion: '02',
          start: '02',
          end: '03',
          txCount: 1,
          firstCommitTimeMs: null,
          lastCommitTimeMs: null,
          part: null,
        },
        lines,
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
      await sealRaw(
        {
          replicaVersion: '02',
          start: '02',
          end: '03',
          txCount: 1,
          firstCommitTimeMs: null,
          lastCommitTimeMs: null,
          part: null,
        },
        [],
      );
      await expect(collectFile()).rejects.toThrow(
        'header txCount 1 does not match 0 transactions',
      );
    });

    test('decodes a transaction spanning a part chain', async () => {
      const spanning = tx('05', 4); // begin, 4 data rows, commit
      const chainDir = dir;
      const partPaths = [
        join(chainDir, 'part1.seg'),
        join(chainDir, 'part2.seg'),
        join(chainDir, 'final.seg'),
      ];
      const slices = [
        spanning.messages.slice(0, 2), // begin + first row
        spanning.messages.slice(2, 4), // two rows
        spanning.messages.slice(4), // last row + commit
      ];
      const headers: SegmentHeader[] = [
        interiorHeader('03', '05', 1),
        interiorHeader('03', '05', 2),
        {
          replicaVersion: '02',
          start: '03',
          end: '05',
          txCount: 1,
          firstCommitTimeMs: null,
          lastCommitTimeMs: null,
          part: {number: 3, final: true, watermark: '05'},
        },
      ];
      for (let i = 0; i < 3; i++) {
        await writeSealedSegmentFile(
          headers[i],
          () => Readable.from(slices[i].map(line => Buffer.from(`\n${line}`))),
          partPaths[i],
        );
      }

      const roles: Parameters<typeof decodeSegmentFile>[1][] = [
        {
          kind: 'interior',
          replicaVersion: '02',
          start: '03',
          watermark: '05',
          part: 1,
        },
        {
          kind: 'interior',
          replicaVersion: '02',
          start: '03',
          watermark: '05',
          part: 2,
        },
        {
          kind: 'segment',
          replicaVersion: '02',
          start: '03',
          end: '05',
          parts: 2,
        },
      ];
      const items: SegmentMessage[] = [];
      for (let i = 0; i < 3; i++) {
        for await (const item of decodeSegmentFile(partPaths[i], roles[i])) {
          items.push(item);
        }
      }
      expect(items.map(i => i.message)).toEqual(spanning.parsed);
      expect(items.every(i => i.watermark === '05')).toBe(true);

      // The in-memory decoder refuses chain members.
      expect(() => decodeSegment(readFileSync(partPaths[0]))).toThrow(
        'requires the streaming decoder',
      );
    });

    test('rejects chain files that lie about their position', async () => {
      const spanning = tx('05', 2);
      // An interior part containing a commit.
      await sealRaw(interiorHeader('03', '05', 2), spanning.messages.slice(2));
      await expect(
        collectFile({
          kind: 'interior',
          replicaVersion: '02',
          start: '03',
          watermark: '05',
          part: 2,
        }),
      ).rejects.toThrow('commit in interior part 2');

      // An interior part that ends outside its transaction (a foreign
      // begin/commit pair inside a chain).
      await sealRaw(interiorHeader('03', '07', 1), tx('05').messages);
      await expect(
        collectFile({
          kind: 'interior',
          replicaVersion: '02',
          start: '03',
          watermark: '07',
          part: 1,
        }),
      ).rejects.toThrow('watermark 05 in a chain part of 07');

      // A final part whose role expects a different chain length.
      await sealRaw(
        {
          replicaVersion: '02',
          start: '03',
          end: '05',
          txCount: 1,
          firstCommitTimeMs: null,
          lastCommitTimeMs: null,
          part: {number: 3, final: true, watermark: '05'},
        },
        spanning.messages.slice(2),
      );
      await expect(
        collectFile({
          kind: 'segment',
          replicaVersion: '02',
          start: '03',
          end: '05',
          parts: 1, // the listing saw 1 interior part; the header claims 2
        }),
      ).rejects.toThrow('does not match its name and listing (chain position)');
    });
  });
});

function interiorHeader(
  start: string,
  watermark: string,
  number: number,
): SegmentHeader {
  return {
    replicaVersion: '02',
    start,
    end: null,
    txCount: 0,
    firstCommitTimeMs: null,
    lastCommitTimeMs: null,
    part: {number, final: false, watermark},
  };
}

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
  const header = JSON.stringify({
    replicaVersion,
    start,
    end,
    txCount,
    firstCommitTimeMs: null,
    lastCommitTimeMs: null,
    part: null,
  } satisfies SegmentHeader);
  const payload = zstdCompressSync(
    Buffer.from([header, ...lines].join('\n'), 'utf8'),
  );
  const data = new Uint8Array(4 + 1 + 32 + payload.length);
  data.set([0x5a, 0x41, 0x52, 0x43], 0);
  data[4] = 2;
  data.set(createHash('sha256').update(payload).digest(), 5);
  data.set(payload, 37);
  return {data};
}
