import {
  closeSync,
  createReadStream,
  ftruncateSync,
  mkdirSync,
  openSync,
  rmSync,
  unlinkSync,
  writeSync,
} from 'node:fs';
import {join} from 'node:path';
import type {Readable} from 'node:stream';

/**
 * The archive writer's local disk spool: the streaming-discipline replacement
 * for buffering the open segment in memory. Messages append to an
 * **uncompressed** spool file as they arrive (compression happens at seal
 * time, as a streaming pass over the sealed range), because an uncompressed
 * spool is trivially truncatable: a rollback or interrupted stream is one
 * `ftruncate` back to the last committed offset, with no flush-per-commit
 * framing required.
 *
 * The unit handed to the sealer is a {@link SpoolRange}: a committed byte
 * range of a spool file. Ranges — not whole files — are what make the
 * mid-transaction case cheap: when the seal timer fires while a transaction
 * is open, the committed prefix seals as a range of the live file and the
 * open tail simply keeps appending to it; nothing is copied. The spool
 * rotates to a fresh file whenever a seal lands exactly at the append head
 * (the common, commit-boundary case), and a file is unlinked once rotation
 * has passed it and its last outstanding range is released.
 *
 * Appends are synchronous (positional `writeSync`), preserving the archive
 * writer's stream-loop invariant that buffering a transaction always
 * precedes anything that can advance the stream's resume point. Everything
 * in the spool directory is re-derivable from the replication slot (its
 * transactions are un-ACKed until durable), so construction deletes whatever
 * a previous incarnation left behind rather than reconciling it.
 */

type SpoolFile = {
  path: string;
  fd: number;
  /** Sealed ranges of this file not yet released. */
  outstanding: number;
  /** False once the spool has rotated past this file. */
  current: boolean;
};

function maybeUnlink(file: SpoolFile): void {
  if (!file.current && file.outstanding === 0) {
    unlinkSync(file.path);
  }
}

/** A sealed, committed byte range of a spool file, owned by one seal job. */
export class SpoolRange {
  readonly #file: SpoolFile;
  /** Inclusive start byte offset. */
  readonly start: number;
  /** Exclusive end byte offset. */
  readonly end: number;

  constructor(file: SpoolFile, start: number, end: number) {
    this.#file = file;
    this.start = start;
    this.end = end;
  }

  get bytes(): number {
    return this.end - this.start;
  }

  /**
   * Opens a fresh read stream of the range's bytes. Callable repeatedly (a
   * failed seal pass retries with a new stream) until {@link release}.
   */
  createStream(): Readable {
    return createReadStream(this.#file.path, {
      start: this.start,
      end: this.end - 1, // inclusive
    });
  }

  /**
   * Releases the range, unlinking the underlying file once the spool has
   * rotated past it and no other ranges remain.
   */
  release(): void {
    this.#file.outstanding--;
    maybeUnlink(this.#file);
  }
}

export class SegmentSpool {
  readonly #dir: string;
  #counter = 0;
  #file: SpoolFile;
  /** Byte offset at which the open segment starts. */
  #base = 0;
  /** Byte offset after the last committed transaction. */
  #committed = 0;
  /** Byte offset after the last appended message. */
  #appended = 0;

  constructor(dir: string) {
    this.#dir = dir;
    // Spool contents are re-derivable from the slot: a previous
    // incarnation's leftovers (spools, sealed-but-unuploaded segments) are
    // deleted, not resumed.
    rmSync(dir, {recursive: true, force: true});
    mkdirSync(dir, {recursive: true});
    this.#file = this.#openFile();
  }

  #openFile(): SpoolFile {
    const path = join(
      this.#dir,
      `${String(this.#counter++).padStart(6, '0')}.spool`,
    );
    return {path, fd: openSync(path, 'w'), outstanding: 0, current: true};
  }

  /** Uncompressed bytes of the open segment: committed plus open tail. */
  get segmentBytes(): number {
    return this.#appended - this.#base;
  }

  /** Uncompressed committed bytes of the open segment. */
  get committedBytes(): number {
    return this.#committed - this.#base;
  }

  /** Uncompressed bytes of the open (uncommitted) transaction tail. */
  get openTxBytes(): number {
    return this.#appended - this.#committed;
  }

  /**
   * Appends one message, prefixed by its line separator — the payload of a
   * sealed segment is `headerLine + rangeBytes`, so the separator leads
   * rather than trails and no dangling newline ever needs trimming.
   */
  append(json: string): void {
    const line = Buffer.from(`\n${json}`, 'utf8');
    // Positional writes: the fd's implicit position is meaningless after a
    // rollback's ftruncate.
    writeSync(this.#file.fd, line, 0, line.length, this.#appended);
    this.#appended += line.length;
  }

  /** Marks everything appended so far as committed. */
  commit(): void {
    this.#committed = this.#appended;
  }

  /** Truncates the open (uncommitted) tail back to the last commit. */
  rollback(): void {
    if (this.#appended > this.#committed) {
      ftruncateSync(this.#file.fd, this.#committed);
      this.#appended = this.#committed;
    }
  }

  /**
   * Discards the open segment entirely — the open tail and the committed
   * range not yet sealed. Used at reconcile time: an interrupted stream
   * re-sends everything that was not sealed and uploaded, so the un-sealed
   * spool content would otherwise duplicate it. Ranges already sealed (they
   * lie below the segment base) are untouched.
   */
  discardSegment(): void {
    if (this.#appended > this.#base) {
      ftruncateSync(this.#file.fd, this.#base);
      this.#appended = this.#committed = this.#base;
    }
  }

  /**
   * Seals the committed range of the open segment, advancing the segment
   * base past it, and rotates to a fresh spool file when the seal lands at
   * the append head (i.e. no transaction is open). Returns `undefined` when
   * nothing is committed.
   */
  sealCommitted(): SpoolRange | undefined {
    if (this.#committed === this.#base) {
      return undefined;
    }
    if (this.#committed === this.#appended) {
      // Rotate first, so a failure to open the next file leaves the spool's
      // bookkeeping untouched (fail-stall: the seal retries later).
      const next = this.#openFile();
      const sealed = this.#file;
      const range = new SpoolRange(sealed, this.#base, this.#committed);
      sealed.outstanding++;
      sealed.current = false;
      closeSync(sealed.fd);
      this.#file = next;
      this.#base = this.#committed = this.#appended = 0;
      return range;
    }
    // A transaction is open past the committed range: seal the committed
    // prefix in place and let the tail keep appending to the same file.
    const range = new SpoolRange(this.#file, this.#base, this.#committed);
    this.#file.outstanding++;
    this.#base = this.#committed;
    return range;
  }

  /**
   * Seals everything appended so far — the open transaction's tail included —
   * as an interior part of a transaction chain, advancing the segment base
   * past it while the transaction keeps appending to the same file. Returns
   * `undefined` when nothing is appended.
   *
   * This co-opts the committed marker: after an interior-part seal the open
   * transaction can no longer be dropped by {@link rollback}'s truncation
   * (the sealed ranges below would be destroyed under an in-flight reader),
   * so a chain's rollback must go through {@link abandonToFreshFile}. The
   * archive writer owns that distinction.
   */
  sealOpenTail(): SpoolRange | undefined {
    if (this.#appended === this.#base) {
      return undefined;
    }
    const range = new SpoolRange(this.#file, this.#base, this.#appended);
    this.#file.outstanding++;
    this.#base = this.#committed = this.#appended;
    return range;
  }

  /**
   * Abandons the open segment by rotating to a fresh file rather than
   * truncating, leaving every sealed range of the old file intact for its
   * in-flight readers; the file is unlinked once the last range releases.
   * Used to roll back a transaction whose chain has already sealed interior
   * parts.
   */
  abandonToFreshFile(): void {
    const next = this.#openFile();
    const abandoned = this.#file;
    abandoned.current = false;
    closeSync(abandoned.fd);
    this.#file = next;
    this.#base = this.#committed = this.#appended = 0;
    maybeUnlink(abandoned);
  }

  /**
   * Closes the spool's file descriptor. Files are left on disk for the next
   * incarnation's constructor to delete; in-flight {@link SpoolRange}
   * readers remain valid.
   */
  close(): void {
    closeSync(this.#file.fd);
  }
}
