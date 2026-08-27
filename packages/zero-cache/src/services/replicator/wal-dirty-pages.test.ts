import {existsSync, readFileSync, statSync, writeFileSync} from 'node:fs';
import {afterEach, describe, expect, test} from 'vitest';
import {createSilentLogContext} from '../../../../shared/src/logging-test-utils.ts';
import {Database} from '../../../../zqlite/src/db.ts';
import {DbFile} from '../../test/lite.ts';
import {readWalCapture, type WalCapture} from './wal-dirty-pages.ts';

function pageNumbers({images}: WalCapture): number[] {
  const pages: number[] = [];
  for (const pgno of images.keys()) {
    pages.push(pgno);
  }
  pages.sort((a, b) => a - b);
  return pages;
}

describe('replicator/wal-dirty-pages', () => {
  const lc = createSilentLogContext();
  let cleanup: (() => void)[] = [];

  afterEach(() => {
    for (const fn of cleanup.reverse()) {
      fn();
    }
    cleanup = [];
  });

  function newDB(name: string) {
    const dbFile = new DbFile(name);
    const db = new Database(lc, dbFile.path);
    cleanup.push(() => {
      db.close();
      dbFile.delete();
    });
    // The backup replica's configuration: nothing checkpoints the WAL unless
    // it is asked to, which is what makes the WAL a whole interval's changes.
    db.pragma('journal_mode = wal');
    db.pragma('synchronous = NORMAL');
    db.pragma('wal_autocheckpoint = 0');
    db.exec(`CREATE TABLE "t" ("id" INTEGER PRIMARY KEY, "v" TEXT NOT NULL)`);
    return {db, path: dbFile.path, wal: `${dbFile.path}-wal`};
  }

  test('missing or empty WAL reads as no dirty pages', () => {
    const {wal} = newDB('wal-empty');
    expect(readWalCapture(`${wal}-does-not-exist`)).toEqual({
      images: new Map(),
      frames: 0,
      pageSize: 0,
    });

    writeFileSync(`${wal}-truncated`, Buffer.alloc(8));
    cleanup.push(() => {
      // Written by the test, so it is the test's to remove.
      if (existsSync(`${wal}-truncated`)) {
        writeFileSync(`${wal}-truncated`, Buffer.alloc(0));
      }
    });
    expect(readWalCapture(`${wal}-truncated`).frames).toBe(0);
  });

  test('page images match the pages SQLite checkpoints into the db', () => {
    const {db, path, wal} = newDB('wal-images');
    db.exec(`INSERT INTO "t" VALUES (1, 'one'), (2, 'two')`);

    const capture = readWalCapture(wal);
    expect(capture.pageSize).toBe(
      db.pragma<{page_size: number}>('page_size')[0].page_size,
    );
    expect(capture.images.size).toBeGreaterThan(0);

    // Checkpointing writes exactly these images into the main file, so the
    // post-images the capture reports must be byte-identical to what lands
    // there. This is the property the LTX size model rests on.
    db.pragma('wal_checkpoint(TRUNCATE)');
    const main = readFileSync(path);
    for (const [pgno, image] of capture.images) {
      const start = (pgno - 1) * capture.pageSize;
      expect(main.subarray(start, start + capture.pageSize)).toEqual(image);
    }
  });

  test('a page written repeatedly is captured once, with its final content', () => {
    const {db, wal} = newDB('wal-dedup');
    db.exec(`INSERT INTO "t" VALUES (1, 'first')`);
    const update = db.prepare(`UPDATE "t" SET "v" = ? WHERE "id" = 1`);
    for (let i = 0; i < 20; i++) {
      update.run(`value-${i}`);
    }

    const capture = readWalCapture(wal);
    // 21 transactions, each writing at least the header page and the row's
    // page, so there are many more frames than distinct pages.
    expect(capture.frames).toBeGreaterThan(capture.images.size);

    const rowPage = [...capture.images.values()].filter(image =>
      image.includes(Buffer.from('value-19')),
    );
    expect(rowPage).toHaveLength(1);
    // Only the last write survives, which is the dedup an LTX file gets.
    expect(rowPage[0].includes(Buffer.from('value-18'))).toBe(false);
  });

  test('frames of an uncommitted transaction are excluded', () => {
    const {db, wal} = newDB('wal-uncommitted');
    db.exec(`INSERT INTO "t" VALUES (1, 'committed')`);
    const committed = readWalCapture(wal);

    // A transaction big enough to spill frames into the WAL before it commits.
    db.exec('BEGIN');
    const insert = db.prepare(`INSERT INTO "t" VALUES (?, ?)`);
    for (let id = 2; id < 2000; id++) {
      insert.run(id, `uncommitted-row-${id}`.padEnd(200, 'x'));
    }
    const midTransaction = readWalCapture(wal);
    expect(statSync(wal).size).toBeGreaterThan(
      committed.frames * (24 + committed.pageSize),
    );
    // The WAL has grown, but nothing past the last commit frame counts.
    expect(midTransaction.frames).toBe(committed.frames);
    expect(pageNumbers(midTransaction)).toEqual(pageNumbers(committed));

    db.exec('COMMIT');
    const afterCommit = readWalCapture(wal);
    expect(afterCommit.frames).toBeGreaterThan(committed.frames);
  });

  test('a checkpointed WAL reports only what was written after it', () => {
    const {db, wal} = newDB('wal-generations');
    db.exec(`INSERT INTO "t" VALUES (1, 'before')`);
    db.pragma('wal_checkpoint(TRUNCATE)');
    expect(readWalCapture(wal).frames).toBe(0);

    db.exec(`INSERT INTO "t" VALUES (2, 'after')`);
    const capture = readWalCapture(wal);
    expect(capture.frames).toBeGreaterThan(0);
    expect(
      [...capture.images.values()].some(image =>
        image.includes(Buffer.from('after')),
      ),
    ).toBe(true);
  });

  test('rejects a file that is not a WAL', () => {
    const {path} = newDB('wal-not-a-wal');
    // The main database file, which starts with "SQLite format 3".
    expect(() => readWalCapture(path)).toThrow(
      /does not look like a SQLite WAL/,
    );
  });
});
