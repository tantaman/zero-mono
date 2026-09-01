import {mkdtempSync, readdirSync, rmSync, writeFileSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {afterEach, beforeEach, describe, expect, test} from 'vitest';
import {SegmentSpool, type SpoolRange} from './segment-spool.ts';

async function rangeText(range: SpoolRange): Promise<string> {
  const chunks: Buffer[] = [];
  for await (const chunk of range.createStream()) {
    chunks.push(chunk as Buffer);
  }
  return Buffer.concat(chunks).toString('utf8');
}

describe('backup/archive/segment-spool', () => {
  let dir: string;
  let spool: SegmentSpool;

  beforeEach(() => {
    dir = mkdtempSync(join(tmpdir(), 'zero-segment-spool-test-'));
    spool = new SegmentSpool(dir);
  });

  afterEach(() => {
    rmSync(dir, {recursive: true, force: true});
  });

  test("construction deletes a previous incarnation's leftovers", () => {
    writeFileSync(join(dir, 'stale.spool'), 'stale');
    writeFileSync(join(dir, '03-05.sealed'), 'stale');
    spool = new SegmentSpool(dir);
    expect(readdirSync(dir)).toEqual(['000000.spool']);
  });

  test('appends accumulate with leading separators and commit marks them', async () => {
    spool.append('one');
    spool.append('two');
    expect(spool.segmentBytes).toBe(8);
    expect(spool.committedBytes).toBe(0);
    expect(spool.openTxBytes).toBe(8);

    spool.commit();
    expect(spool.committedBytes).toBe(8);
    expect(spool.openTxBytes).toBe(0);

    const range = spool.sealCommitted()!;
    expect(await rangeText(range)).toBe('\none\ntwo');
    range.release();
  });

  test('rollback truncates the open tail, and appends resume cleanly', async () => {
    spool.append('committed');
    spool.commit();
    spool.append('doomed-1');
    spool.append('doomed-2');
    spool.rollback();
    expect(spool.segmentBytes).toBe(10);
    spool.append('after');

    spool.commit();
    const range = spool.sealCommitted()!;
    expect(await rangeText(range)).toBe('\ncommitted\nafter');
    range.release();
  });

  test('sealing at the append head rotates to a fresh file and reclaims the old one', async () => {
    spool.append('first');
    spool.commit();
    const range = spool.sealCommitted()!;
    expect(spool.segmentBytes).toBe(0);

    spool.append('second');
    spool.commit();
    // Two files: the sealed (unreleased) one and the fresh spool.
    expect(readdirSync(dir).length).toBe(2);
    expect(await rangeText(range)).toBe('\nfirst');
    range.release();
    expect(readdirSync(dir)).toEqual(['000001.spool']);

    const second = spool.sealCommitted()!;
    expect(await rangeText(second)).toBe('\nsecond');
    second.release();
  });

  test('sealing mid-transaction leaves the open tail appending in place', async () => {
    spool.append('committed');
    spool.commit();
    spool.append('open-head');
    const range = spool.sealCommitted()!;
    expect(await rangeText(range)).toBe('\ncommitted');
    expect(spool.segmentBytes).toBe(10);
    expect(spool.openTxBytes).toBe(10);

    // The tail keeps going in the same file; the sealed range is unaffected.
    spool.append('open-tail');
    spool.commit();
    expect(readdirSync(dir)).toEqual(['000000.spool']);
    expect(await rangeText(range)).toBe('\ncommitted');
    range.release();

    const tail = spool.sealCommitted()!;
    expect(await rangeText(tail)).toBe('\nopen-head\nopen-tail');
    tail.release();
    // That seal was at the append head, so the file rotated and, with all
    // ranges released, was reclaimed.
    expect(readdirSync(dir)).toEqual(['000001.spool']);
  });

  test('sealCommitted with nothing committed returns undefined', () => {
    expect(spool.sealCommitted()).toBeUndefined();
    spool.append('open');
    expect(spool.sealCommitted()).toBeUndefined();
  });

  test('discardSegment drops committed-but-unsealed content and the open tail', async () => {
    spool.append('sealed');
    spool.commit();
    const range = spool.sealCommitted()!;

    spool.append('committed-unsealed');
    spool.commit();
    spool.append('open');
    spool.discardSegment();
    expect(spool.segmentBytes).toBe(0);

    spool.append('resent');
    spool.commit();
    const resent = spool.sealCommitted()!;
    expect(await rangeText(resent)).toBe('\nresent');
    expect(await rangeText(range)).toBe('\nsealed');
    range.release();
    resent.release();
  });

  test('a range stays readable after rotation until released', async () => {
    spool.append('one');
    spool.commit();
    const range = spool.sealCommitted()!; // rotates
    // Repeatedly readable, e.g. across seal-pass retries.
    expect(await rangeText(range)).toBe('\none');
    expect(await rangeText(range)).toBe('\none');
    range.release();
  });
});
