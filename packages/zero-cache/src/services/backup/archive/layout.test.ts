import {describe, expect, test} from 'vitest';
import {
  baseChunkKey,
  baseCompleteKey,
  baseIntentKey,
  lineagesFromKeys,
  parseBaseCompleteKey,
  parseSegmentKey,
  segmentKey,
} from './layout.ts';

describe('backup/archive/layout', () => {
  test('segment keys round trip', () => {
    const key = segmentKey('02', '05', '0g');
    expect(key).toBe('v1/02/log/05-0g.seg');
    expect(parseSegmentKey('02', key)).toEqual({key, start: '05', end: '0g'});
  });

  test('segment keys sort in watermark order', () => {
    // LexiVersions of different byte lengths still sort correctly because the
    // encoding is length-prefixed.
    const keys = [
      segmentKey('02', '110', '111'),
      segmentKey('02', '02', '0z'),
      segmentKey('02', '0z', '110'),
    ].sort();
    expect(keys).toEqual([
      'v1/02/log/02-0z.seg',
      'v1/02/log/0z-110.seg',
      'v1/02/log/110-111.seg',
    ]);
  });

  test('parseSegmentKey ignores foreign keys', () => {
    expect(
      parseSegmentKey('02', 'v1/02/base/05/complete.json'),
    ).toBeUndefined();
    expect(parseSegmentKey('02', 'v1/03/log/05-0g.seg')).toBeUndefined();
    expect(
      parseSegmentKey('02', 'v1/02/log/05-0g.seg.partial'),
    ).toBeUndefined();
    expect(parseSegmentKey('02', 'v1/02/log/README')).toBeUndefined();
  });

  test('base keys', () => {
    expect(baseIntentKey('02', '0g')).toBe('v1/02/base/0g/intent.json');
    expect(baseChunkKey('02', '0g', 0)).toBe('v1/02/base/0g/chunk/00000000');
    expect(baseChunkKey('02', '0g', 12)).toBe('v1/02/base/0g/chunk/00000012');
    expect(baseCompleteKey('02', '0g')).toBe('v1/02/base/0g/complete.json');
  });

  test('parseBaseCompleteKey extracts only complete manifests', () => {
    expect(parseBaseCompleteKey('02', 'v1/02/base/0g/complete.json')).toBe(
      '0g',
    );
    expect(
      parseBaseCompleteKey('02', 'v1/02/base/0g/intent.json'),
    ).toBeUndefined();
    expect(
      parseBaseCompleteKey('02', 'v1/02/base/0g/chunk/00000000'),
    ).toBeUndefined();
    expect(
      parseBaseCompleteKey('03', 'v1/02/base/0g/complete.json'),
    ).toBeUndefined();
  });

  test('lineagesFromKeys', () => {
    expect(
      lineagesFromKeys([
        'v1/02/log/02-05.seg',
        'v1/02/base/05/complete.json',
        'v1/0g/log/0g-0h.seg',
        'v1/orphan',
        'v2/03/log/03-04.seg',
      ]),
    ).toEqual(['02', '0g']);
  });
});
