import {readFileSync, statSync} from 'node:fs';

/**
 * Parses a SQLite `-wal` file into the pages its committed transactions
 * dirtied, and those pages' post-images.
 *
 * This is a *measurement* helper, not part of the replication path. It exists
 * because the backup replica runs with `wal_autocheckpoint = 0` (see
 * `getPragmaConfig('backup')`), which makes the WAL accumulated between two
 * checkpoints exactly the set of pages litestream would put in that interval's
 * LTX file. Reading it directly is what lets `logical-vs-ltx-size.bench.ts`
 * size the physical backup path without running litestream or talking to a
 * backup destination.
 *
 * The format is documented at https://sqlite.org/walformat.html:
 *
 *   * a 32-byte header, whose salts identify the WAL "generation"
 *   * frames of a 24-byte header followed by one page image
 *   * a frame header holds the page number, the database size after the
 *     transaction (non-zero only on a commit frame), the two salts, and
 *     checksums
 *
 * Checksums are not verified: this reads a WAL written by the process that is
 * reading it, moments earlier, so a mismatch would mean a SQLite bug rather
 * than the corruption the checksums exist to catch.
 */

const WAL_HEADER_BYTES = 32;
const WAL_FRAME_HEADER_BYTES = 24;

/** The two magic values SQLite writes, one per checksum byte order. */
const WAL_MAGIC_BE = 0x377f0683;
const WAL_MAGIC_LE = 0x377f0682;

export type WalCapture = {
  /**
   * The last post-image of each page dirtied by a committed frame, keyed by
   * page number. A page written more than once in the WAL appears once, with
   * its final content -- the same dedup an LTX file gets from covering an
   * interval rather than a transaction.
   */
  readonly images: Map<number, Buffer>;
  /** Committed frames read, i.e. page writes *before* that dedup. */
  readonly frames: number;
  /** The page size recorded in the WAL header; 0 for an empty/absent WAL. */
  readonly pageSize: number;
};

function empty(): WalCapture {
  return {images: new Map(), frames: 0, pageSize: 0};
}

export function readWalCapture(walPath: string): WalCapture {
  let size: number;
  try {
    size = statSync(walPath).size;
  } catch {
    // A WAL checkpointed with TRUNCATE and not written since may not exist at
    // all, which is "no pages dirtied", not an error.
    return empty();
  }
  if (size < WAL_HEADER_BYTES) {
    return empty();
  }
  const wal = readFileSync(walPath);
  const magic = wal.readUInt32BE(0);
  if (magic !== WAL_MAGIC_BE && magic !== WAL_MAGIC_LE) {
    throw new Error(
      `${walPath} does not look like a SQLite WAL ` +
        `(magic 0x${magic.toString(16)})`,
    );
  }
  const pageSize = wal.readUInt32BE(8);
  if (pageSize <= 0 || (pageSize & (pageSize - 1)) !== 0) {
    throw new Error(`${walPath} has an invalid page size ${pageSize}`);
  }
  const salt1 = wal.readUInt32BE(16);
  const salt2 = wal.readUInt32BE(20);
  const frameBytes = WAL_FRAME_HEADER_BYTES + pageSize;

  // Two passes. The first finds the last commit frame, because frames past it
  // belong to a transaction that has not committed and would not be
  // checkpointed; the second collects the images up to and including it.
  let lastCommitFrame = -1;
  let frame = 0;
  for (
    let offset = WAL_HEADER_BYTES;
    offset + frameBytes <= wal.length;
    offset += frameBytes, frame++
  ) {
    if (
      wal.readUInt32BE(offset + 8) !== salt1 ||
      wal.readUInt32BE(offset + 12) !== salt2
    ) {
      // A frame left over from a previous WAL generation; everything from here
      // on is stale.
      break;
    }
    if (wal.readUInt32BE(offset + 4) !== 0) {
      lastCommitFrame = frame;
    }
  }

  const images = new Map<number, Buffer>();
  let frames = 0;
  for (
    let offset = WAL_HEADER_BYTES, i = 0;
    i <= lastCommitFrame;
    offset += frameBytes, i++
  ) {
    const pgno = wal.readUInt32BE(offset);
    const start = offset + WAL_FRAME_HEADER_BYTES;
    images.set(pgno, wal.subarray(start, start + pageSize));
    frames++;
  }
  return {images, frames, pageSize};
}
