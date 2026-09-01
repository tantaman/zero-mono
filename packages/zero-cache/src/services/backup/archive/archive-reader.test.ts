import {mkdtempSync, readdirSync, readFileSync, rmSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {Readable} from 'node:stream';
import {afterEach, beforeEach, describe, expect, test} from 'vitest';
import {InMemoryObjectStore, wireTransaction} from '../test-utils.ts';
import {
  ArchiveContinuityError,
  iterateMessages,
  iterateTransactions,
  listLogSegments,
  selectCoveringSegments,
} from './archive-reader.ts';
import {segmentKey, segmentPartKey, type SegmentRef} from './layout.ts';
import {
  encodeSegment,
  SegmentFormatError,
  writeSealedSegmentFile,
  type SegmentHeader,
} from './segment-format.ts';

function ref(start: string, end: string): SegmentRef {
  return {key: segmentKey('02', start, end), start, end};
}

async function collect(
  iter: AsyncGenerator<{watermark: string}>,
): Promise<string[]> {
  const watermarks: string[] = [];
  for await (const {watermark} of iter) {
    watermarks.push(watermark);
  }
  return watermarks;
}

describe('backup/archive/archive-reader', () => {
  describe('selectCoveringSegments', () => {
    const segments = [ref('02', '05'), ref('05', '0g'), ref('0g', '0k')];

    test('selects the minimal covering chain', () => {
      expect(selectCoveringSegments(segments, '02', '0k')).toEqual(segments);
      expect(selectCoveringSegments(segments, '05', '0g')).toEqual([
        ref('05', '0g'),
      ]);
      // Bounds inside segments still require the segments containing them.
      expect(selectCoveringSegments(segments, '03', '06')).toEqual([
        ref('02', '05'),
        ref('05', '0g'),
      ]);
    });

    test('an empty range needs no segments', () => {
      expect(selectCoveringSegments(segments, '05', '05')).toEqual([]);
      expect(selectCoveringSegments([], '05', '05')).toEqual([]);
    });

    test('rejects a range starting before the archived history', () => {
      expect(() => selectCoveringSegments(segments, '01', '05')).toThrow(
        ArchiveContinuityError,
      );
    });

    test('rejects a range ending past the archive head', () => {
      expect(() => selectCoveringSegments(segments, '02', '0z')).toThrow(
        'the archive head 0k does not reach 0z',
      );
      expect(() => selectCoveringSegments([], '02', '05')).toThrow(
        'the archive head (empty) does not reach 05',
      );
    });

    test('rejects a gap in the chain', () => {
      expect(() =>
        selectCoveringSegments([ref('02', '05'), ref('0g', '0k')], '02', '0k'),
      ).toThrow('does not resume from 05');
      // ... but a gap outside the requested range is irrelevant.
      expect(
        selectCoveringSegments([ref('02', '05'), ref('0g', '0k')], '0g', '0k'),
      ).toEqual([ref('0g', '0k')]);
    });

    test('rejects an overlap in the chain', () => {
      expect(() =>
        selectCoveringSegments([ref('02', '05'), ref('03', '0g')], '02', '0g'),
      ).toThrow(ArchiveContinuityError);
    });
  });

  describe('iterateTransactions', () => {
    let store: InMemoryObjectStore;

    beforeEach(async () => {
      store = new InMemoryObjectStore();
      await putSegment('02', ['03', '05']);
      await putSegment('05', ['07', '09']);
      await putSegment('09', ['0b']);
    });

    async function putSegment(start: string, watermarks: string[]) {
      const {data, end} = encodeSegment({
        replicaVersion: '02',
        start,
        transactions: watermarks.map(w => wireTransaction(w)),
      });
      await store.putIfAbsent(segmentKey('02', start, end), data);
    }

    test('yields the transactions in (after, upTo]', async () => {
      expect(
        await collect(iterateTransactions(store, '02', '02', '0b')),
      ).toEqual(['03', '05', '07', '09', '0b']);
      // Straddling both bounds filters within segments.
      expect(
        await collect(iterateTransactions(store, '02', '03', '09')),
      ).toEqual(['05', '07', '09']);
      expect(
        await collect(iterateTransactions(store, '02', '0b', '0b')),
      ).toEqual([]);
    });

    test('yields full message envelopes', async () => {
      for await (const tx of iterateTransactions(store, '02', '05', '07')) {
        expect(tx.messages).toEqual(wireTransaction('07').parsed);
      }
    });

    test('propagates continuity errors', async () => {
      await store.delete(segmentKey('02', '05', '09'));
      await expect(
        collect(iterateTransactions(store, '02', '02', '0b')),
      ).rejects.toThrow(ArchiveContinuityError);
    });

    test('rejects a segment whose content does not match its name', async () => {
      // Rename the last segment to claim a different range.
      const data = store.objects.get(segmentKey('02', '09', '0b'))!;
      await store.delete(segmentKey('02', '09', '0b'));
      await store.putIfAbsent(segmentKey('02', '09', '0c'), data);

      await expect(
        collect(iterateTransactions(store, '02', '09', '0c')),
      ).rejects.toThrow('not the range its name claims');
    });

    test('rejects a corrupt segment', async () => {
      const key = segmentKey('02', '05', '09');
      const corrupt = Uint8Array.from(store.objects.get(key)!);
      corrupt[corrupt.length - 1] ^= 0x01;
      store.objects.set(key, corrupt);

      await expect(
        collect(iterateTransactions(store, '02', '02', '0b')),
      ).rejects.toThrow('checksum mismatch');
    });

    test('listLogSegments ignores foreign objects', async () => {
      await store.putIfAbsent('v1/02/log/orphan.tmp1', new Uint8Array());
      expect(await listLogSegments(store, '02')).toEqual([
        ref('02', '05'),
        ref('05', '09'),
        ref('09', '0b'),
      ]);
    });
  });

  describe('iterateMessages (streaming replay)', () => {
    let store: InMemoryObjectStore;
    let tempDir: string;

    beforeEach(async () => {
      store = new InMemoryObjectStore();
      tempDir = mkdtempSync(join(tmpdir(), 'zero-archive-reader-test-'));
      await putSegment('02', ['03', '05']);
      await putSegment('05', ['07', '09']);
      await putSegment('09', ['0b']);
    });

    afterEach(() => {
      rmSync(tempDir, {recursive: true, force: true});
    });

    async function putSegment(start: string, watermarks: string[]) {
      const {data, end} = encodeSegment({
        replicaVersion: '02',
        start,
        transactions: watermarks.map(w => wireTransaction(w)),
      });
      await store.putIfAbsent(segmentKey('02', start, end), data);
    }

    test('yields the same stream as the in-memory iterator, message-by-message', async () => {
      const messages = [];
      for await (const item of iterateMessages(
        store,
        '02',
        '02',
        '0b',
        tempDir,
      )) {
        messages.push(item);
      }
      const expected = [];
      for await (const tx of iterateTransactions(store, '02', '02', '0b')) {
        for (const message of tx.messages) {
          expected.push({
            watermark: tx.watermark,
            message,
            // The archived JSON rides along, byte-exact.
            json: JSON.stringify(message),
          });
        }
      }
      expect(messages).toEqual(expected);
    });

    test('filters transactions straddling the bounds and cleans up temp files', async () => {
      const watermarks = new Set<string>();
      for await (const {watermark} of iterateMessages(
        store,
        '02',
        '03',
        '09',
        tempDir,
      )) {
        watermarks.add(watermark);
      }
      expect([...watermarks]).toEqual(['05', '07', '09']);
      expect(readdirSync(tempDir)).toEqual([]);
    });

    test('cleans up temp files when the consumer stops early', async () => {
      for await (const {watermark} of iterateMessages(
        store,
        '02',
        '02',
        '0b',
        tempDir,
      )) {
        if (watermark === '07') {
          break;
        }
      }
      expect(readdirSync(tempDir)).toEqual([]);
    });

    test('propagates continuity errors', async () => {
      await store.delete(segmentKey('02', '05', '09'));
      await expect(
        collect(iterateMessages(store, '02', '02', '0b', tempDir)),
      ).rejects.toThrow(ArchiveContinuityError);
    });

    test('rejects a segment whose content does not match its name', async () => {
      const data = store.objects.get(segmentKey('02', '09', '0b'))!;
      await store.delete(segmentKey('02', '09', '0b'));
      await store.putIfAbsent(segmentKey('02', '09', '0c'), data);

      await expect(
        collect(iterateMessages(store, '02', '09', '0c', tempDir)),
      ).rejects.toThrow(SegmentFormatError);
      expect(readdirSync(tempDir)).toEqual([]);
    });

    test('replays a transaction chain, and detects a missing interior part', async () => {
      // A transaction spanning two interior parts and a final, continuing
      // the archive from '0b'.
      const spanning = wireTransaction('0d', 4);
      const sealed = async (header: SegmentHeader, lines: string[]) => {
        const path = join(tempDir, 'seal.tmp');
        await writeSealedSegmentFile(
          header,
          () => Readable.from(lines.map(line => Buffer.from(`\n${line}`))),
          path,
        );
        const data = readFileSync(path);
        rmSync(path);
        return data;
      };
      const interior = (part: number): SegmentHeader => ({
        replicaVersion: '02',
        start: '0b',
        end: null,
        txCount: 0,
        firstCommitTimeMs: null,
        lastCommitTimeMs: null,
        part: {number: part, final: false, watermark: '0d'},
      });
      await store.putIfAbsent(
        segmentPartKey('02', '0b', '0d', 1),
        await sealed(interior(1), spanning.messages.slice(0, 2)),
      );
      await store.putIfAbsent(
        segmentPartKey('02', '0b', '0d', 2),
        await sealed(interior(2), spanning.messages.slice(2, 4)),
      );
      await store.putIfAbsent(
        segmentKey('02', '0b', '0d'),
        await sealed(
          {
            replicaVersion: '02',
            start: '0b',
            end: '0d',
            txCount: 1,
            firstCommitTimeMs: null,
            lastCommitTimeMs: null,
            part: {number: 3, final: true, watermark: '0d'},
          },
          spanning.messages.slice(4),
        ),
      );

      const items = [];
      for await (const item of iterateMessages(
        store,
        '02',
        '09',
        '0d',
        tempDir,
      )) {
        items.push(item);
      }
      expect(
        items.filter(i => i.watermark === '0d').map(i => i.message),
      ).toEqual(spanning.parsed);
      expect(readdirSync(tempDir)).toEqual([]);

      // A missing tail part is only detectable against the final part's
      // header, which declares the chain's length.
      await store.delete(segmentPartKey('02', '0b', '0d', 2));
      await expect(
        collect(iterateMessages(store, '02', '09', '0d', tempDir)),
      ).rejects.toThrow('does not match its name and listing (chain position)');

      // A gap in the part sequence is detectable from the listing alone.
      await store.putIfAbsent(
        segmentPartKey('02', '0b', '0d', 2),
        await sealed(interior(2), spanning.messages.slice(2, 4)),
      );
      await store.delete(segmentPartKey('02', '0b', '0d', 1));
      await expect(
        collect(iterateMessages(store, '02', '09', '0d', tempDir)),
      ).rejects.toThrow('missing interior part 1');
    });

    test('rejects a corrupt segment before yielding anything from it', async () => {
      const key = segmentKey('02', '05', '09');
      const corrupt = Uint8Array.from(store.objects.get(key)!);
      corrupt[corrupt.length - 1] ^= 0x01;
      store.objects.set(key, corrupt);

      const seen: string[] = [];
      await expect(
        (async () => {
          for await (const {watermark} of iterateMessages(
            store,
            '02',
            '02',
            '0b',
            tempDir,
          )) {
            seen.push(watermark);
          }
        })(),
      ).rejects.toThrow('checksum mismatch');
      // The first (intact) segment streamed; the corrupt one yielded nothing.
      expect(seen).toEqual(['03', '03', '03', '05', '05', '05']);
    });
  });
});
