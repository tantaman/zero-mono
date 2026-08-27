import {mkdtempSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
/**
 * Captures a real change-streamer CDC stream into raw "logical log" bytes.
 *
 * The bytes produced here are exactly what the change-streamer would ship: each
 * data-plane `ChangeStreamData` tuple serialized with `serializeChangeStreamData`
 * (the same call the Storer makes), framed as newline-delimited JSON.
 */
import {consoleLogSink, LogContext} from '@rocicorp/logger';
import postgres from 'postgres';
import type {
  ChangeSource,
  ChangeStream,
} from '../../../packages/zero-cache/src/services/change-source/change-source.ts';
import {initializePostgresChangeSource} from '../../../packages/zero-cache/src/services/change-source/pg/change-source.ts';
import type {ChangeStreamMessage} from '../../../packages/zero-cache/src/services/change-source/protocol/current/downstream.ts';
import {serializeChangeStreamData} from '../../../packages/zero-cache/src/services/change-streamer/change-log-codec.ts';
import {postgresTypeConfig} from '../../../packages/zero-cache/src/types/pg.ts';

export type CaptureStats = {
  /** Total raw NDJSON bytes captured. */
  bytes: number;
  /** Number of data-plane messages (begin/data/commit/rollback). */
  messages: number;
  /** Number of committed transactions. */
  transactions: number;
  /** Number of `data` messages by change tag. */
  byTag: Record<string, number>;
  /** Raw bytes attributable to each change tag (including the newline). */
  bytesByTag: Record<string, number>;
  /**
   * Bytes spent on the repeated `relation` block carried by every data
   * message (schema, table name, row key, deprecated key columns).
   */
  relationBytes: number;
};

export type Capture = {
  chunks: Buffer[];
  stats: CaptureStats;
};

const lc = new LogContext('error', {}, consoleLogSink);

export function connect(uri: string) {
  return postgres(uri, {
    ...postgresTypeConfig(),
    max: 4,
    onnotice: () => {},
  });
}

/** The Postgres client type, configured with zero-cache's type parsers. */
export type Sql = ReturnType<typeof connect>;

export type Session = {
  source: ChangeSource;
  stream: ChangeStream;
  /** Everything captured since the last `take()`. */
  take(): Capture;
  stop(): Promise<void>;
  /** Resolves once the stream has been idle for `quietMs`. */
  drain(quietMs?: number): Promise<void>;
  /** Bytes captured since the last `take()`. */
  bytes(): number;
};

/**
 * Starts a real logical replication stream against `upstreamURI` and begins
 * accumulating serialized change bytes.
 */
export async function startCapture(
  upstreamURI: string,
  appID: string,
): Promise<Session> {
  const replicaDir = mkdtempSync(join(tmpdir(), 'llc-replica-'));
  const replicaFile = join(replicaDir, 'replica.db');

  const {changeSource} = await initializePostgresChangeSource(
    lc,
    upstreamURI,
    {appID, shardNum: 0, publications: []},
    replicaFile,
    {tableCopyWorkers: 4},
    {},
  );

  const stream = await changeSource.startStream('00');

  let buf: Buffer[] = [];
  let bytes = 0;
  let messages = 0;
  let transactions = 0;
  let byTag: Record<string, number> = {};
  let bytesByTag: Record<string, number> = {};
  let relationBytes = 0;
  let lastMessageAt = Date.now();
  let failure: unknown;

  const pump = (async () => {
    try {
      for await (const msg of stream.changes as AsyncIterable<ChangeStreamMessage>) {
        const kind = msg[0];
        if (kind === 'control') {
          continue;
        }
        if (kind === 'status') {
          // Not part of the logical log, but must be acked so upstream can
          // release WAL for changes outside the publication.
          if (msg[1].ack) {
            stream.acks.push([
              'status',
              {ack: true},
              {watermark: msg[2].watermark},
            ]);
          }
          continue;
        }
        lastMessageAt = Date.now();
        const line = serializeChangeStreamData(msg) + '\n';
        const b = Buffer.from(line, 'utf8');
        buf.push(b);
        bytes += b.length;
        messages++;

        const tag = kind === 'data' ? msg[1].tag : kind;
        byTag[tag] = (byTag[tag] ?? 0) + 1;
        bytesByTag[tag] = (bytesByTag[tag] ?? 0) + b.length;
        if (kind === 'data') {
          const rel = (msg[1] as {relation?: unknown}).relation;
          if (rel) {
            relationBytes += Buffer.byteLength(JSON.stringify(rel), 'utf8');
          }
        }

        if (kind === 'commit') {
          transactions++;
          // Mirrors UpstreamAcker: ack the commit watermark upstream.
          stream.acks.push([
            'status',
            {tag: 'commit'},
            {watermark: msg[2].watermark},
          ]);
        }
      }
    } catch (e) {
      failure = e;
    }
  })();

  return {
    source: changeSource,
    stream,
    take() {
      const capture: Capture = {
        chunks: buf,
        stats: {
          bytes,
          messages,
          transactions,
          byTag,
          bytesByTag,
          relationBytes,
        },
      };
      buf = [];
      bytes = 0;
      messages = 0;
      transactions = 0;
      byTag = {};
      bytesByTag = {};
      relationBytes = 0;
      return capture;
    },
    bytes() {
      return bytes;
    },
    async drain(quietMs = 1500) {
      // Wait until no message has arrived for `quietMs`.
      for (;;) {
        if (failure) throw failure;
        const idle = Date.now() - lastMessageAt;
        if (idle >= quietMs) return;
        await new Promise(r => setTimeout(r, Math.max(50, quietMs - idle)));
      }
    },
    async stop() {
      stream.changes.cancel();
      await pump;
      await changeSource.stop();
    },
  };
}
