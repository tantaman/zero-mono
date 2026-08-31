import {beforeEach, describe, expect, test} from 'vitest';
import {InMemoryObjectStore, wireTransaction} from '../test-utils.ts';
import {
  ArchiveContinuityError,
  iterateTransactions,
  listLogSegments,
  selectCoveringSegments,
} from './archive-reader.ts';
import {segmentKey, type SegmentRef} from './layout.ts';
import {encodeSegment} from './segment-format.ts';

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
});
