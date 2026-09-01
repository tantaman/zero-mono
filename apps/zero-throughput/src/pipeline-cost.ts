/**
 * Micro-benchmarks for the per-change costs the replication path pays, so a
 * ceiling number can be decomposed instead of guessed at.
 *
 *   pnpm --filter zero-throughput run pipeline-cost
 *
 * The ceiling runner says the path tops out around 5.3k rows/s to one
 * view-syncer, which is ~189us per row. That is only useful next to what the
 * individual costs are: how long a row takes to reach SQLite when nothing is
 * in the way, what one change costs on the wire (a framed message downstream
 * plus the ack `consumed()` sends back, per change), and what the write
 * worker's postMessage round trip costs (one per change, strictly serialized
 * by `assert(this.#pending === null)`).
 *
 * The last probe answers the obvious follow-up: what batching a transaction
 * into one message would be worth.
 */
import {randomBytes} from 'node:crypto';
import {rmSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {Worker, isMainThread, parentPort} from 'node:worker_threads';
import {WebSocket, WebSocketServer} from 'ws';
import {createSilentLogContext} from '../../../packages/shared/src/logging-test-utils.ts';
import {Database} from '../../../packages/zqlite/src/db.ts';
import {log} from './util.ts';

const ROWS_PER_TX = 20;
const PAYLOAD_BYTES = 256;

/** A row change the size and shape of what the ceiling runs replicate. */
function change(seq: number) {
  return {
    tag: 'insert',
    relation: {
      schema: 'public',
      name: 'zero_throughput_event',
      keyColumns: ['id'],
      replicaIdentity: 'default',
    },
    new: {
      id: `ceiling-${seq}`,
      profile: 'feed-append',
      shard: 0,
      bucket: 0,
      seq,
      payload: {p: PAYLOAD},
      written_at: 1788280552083.954,
      updated_at: 1788280552083.954,
    },
  };
}
const PAYLOAD = randomBytes(PAYLOAD_BYTES)
  .toString('base64')
  .slice(0, PAYLOAD_BYTES);

// The worker half of the postMessage probes. `isMainThread` splits the file
// rather than a second module so the two halves cannot drift apart.
if (!isMainThread) {
  parentPort?.on('message', msg =>
    parentPort?.postMessage({ok: Array.isArray(msg) ? msg.length : 1}),
  );
} else {
  await main();
}

async function main(): Promise<void> {
  const sqlite = sqliteFloor();
  const wire = await wireCost();
  const hop = await hopCost();

  log('');
  log(`Per-change costs (${PAYLOAD_BYTES}B payload, ${ROWS_PER_TX} rows/tx):`);
  log('');
  row('SQLite insert (floor, no pipeline)', sqlite.usPerRow, sqlite.rowsPerSec);
  row('WS frame downstream + ack upstream', wire.usPerMessage);
  row('write-worker postMessage round trip', hop.perChangeUs);
  row('  ... batched one message per tx', hop.perTxUs);
  log('');
  log(
    `Batching the write-worker call per transaction saves ` +
      `${(hop.perChangeUs - hop.perTxUs).toFixed(1)}us per row.`,
  );
}

function row(label: string, us: number, rowsPerSec?: number): void {
  log(
    `  ${label.padEnd(38)} ${us.toFixed(1).padStart(6)}us` +
      (rowsPerSec === undefined
        ? ''
        : `   (${rowsPerSec.toLocaleString()} rows/s)`),
  );
}

/**
 * What the storage layer can absorb: prepared inserts into a table shaped
 * like a replicated one, with the pragmas `getPragmaConfig('serving')`
 * leaves in place.
 */
function sqliteFloor(): {usPerRow: number; rowsPerSec: number} {
  const path = join(tmpdir(), `pipeline-cost-${process.pid}.db`);
  for (const suffix of ['', '-wal', '-wal2', '-shm']) {
    rmSync(path + suffix, {force: true});
  }
  const db = new Database(createSilentLogContext(), path);
  try {
    db.pragma('journal_mode = wal2');
    db.pragma('synchronous = NORMAL');
    db.exec(`CREATE TABLE "zero_throughput_event" (
      "id" TEXT, "profile" TEXT, "shard" INTEGER, "bucket" INTEGER,
      "seq" INTEGER, "payload" TEXT, "written_at" REAL, "updated_at" REAL,
      "_0_version" TEXT);
      CREATE UNIQUE INDEX "pk" ON "zero_throughput_event" ("id");
      CREATE UNIQUE INDEX "seq_idx" ON "zero_throughput_event" ("seq");`);
    const insert = db.prepare(`INSERT INTO "zero_throughput_event"
      ("id","profile","shard","bucket","seq","payload","written_at",
       "updated_at","_0_version")
      VALUES (?,?,?,?,?,?,?,?,?)`);
    const begin = db.prepare('BEGIN');
    const commit = db.prepare('COMMIT');
    const payload = JSON.stringify({p: PAYLOAD});
    let seq = 0;
    const insertRows = (count: number) => {
      for (let i = 0; i < count; i += ROWS_PER_TX) {
        begin.run();
        for (let j = 0; j < ROWS_PER_TX; j++) {
          seq++;
          insert.run(
            `ceiling-${seq}`,
            'feed-append',
            0,
            0,
            seq,
            payload,
            1788280552083.954,
            1788280552083.954,
            '4n1rr4',
          );
        }
        commit.run();
      }
    };
    insertRows(20_000);
    const rows = 200_000;
    const start = performance.now();
    insertRows(rows);
    const usPerRow = ((performance.now() - start) * 1000) / rows;
    return {usPerRow, rowsPerSec: Math.round(1e6 / usPerRow)};
  } finally {
    db.close();
    for (const suffix of ['', '-wal', '-wal2', '-shm']) {
      rmSync(path + suffix, {force: true});
    }
  }
}

/**
 * One change on the wire: the framed message `streamOut` sends and the
 * `{"ack":id}` frame `streamIn`'s `consumed` callback sends back, both ends
 * parsing as the real code does.
 */
async function wireCost(): Promise<{usPerMessage: number}> {
  const messages = 50_000;
  const wss = new WebSocketServer({host: '127.0.0.1', port: 0});
  try {
    await new Promise(resolve => wss.on('listening', resolve));
    const {port} = wss.address() as {port: number};
    let resolveDone: (usPerMessage: number) => void = () => {};
    const done = new Promise<number>(resolve => {
      resolveDone = resolve;
    });

    wss.on('connection', ws => {
      let id = 0;
      let acked = 0;
      const start = performance.now();
      const send = () => {
        id++;
        ws.send(`{"id":${id},"msg":${JSON.stringify(change(id))}}`);
      };
      ws.on('message', data => {
        JSON.parse(data.toString()); // streamOut parses every ack
        acked++;
        if (acked === messages) {
          resolveDone(((performance.now() - start) * 1000) / messages);
        } else {
          send();
        }
      });
      send();
    });

    const client = new WebSocket(`ws://127.0.0.1:${port}`);
    client.on('message', data => {
      const msg = JSON.parse(data.toString()) as {id: number};
      client.send(JSON.stringify({ack: msg.id}));
    });
    const usPerMessage = await done;
    client.close();
    return {usPerMessage};
  } finally {
    wss.close();
  }
}

/** The write-worker hop, per change and then batched per transaction. */
async function hopCost(): Promise<{perChangeUs: number; perTxUs: number}> {
  const worker = new Worker(new URL(import.meta.url));
  try {
    await new Promise(resolve => worker.once('online', resolve));
    const call = (msg: unknown) =>
      new Promise(resolve => {
        worker.once('message', resolve);
        worker.postMessage(msg);
      });

    const transactions = 3_000;
    const batch = () => Array.from({length: ROWS_PER_TX}, (_, i) => change(i));

    for (let i = 0; i < 500; i++) {
      await call(change(i));
    }
    let start = performance.now();
    for (let t = 0; t < transactions; t++) {
      for (let r = 0; r < ROWS_PER_TX; r++) {
        await call(change(r));
      }
    }
    const perChangeUs =
      ((performance.now() - start) * 1000) / (transactions * ROWS_PER_TX);

    for (let i = 0; i < 25; i++) {
      await call(batch());
    }
    start = performance.now();
    for (let t = 0; t < transactions; t++) {
      await call(batch());
    }
    const perTxUs =
      ((performance.now() - start) * 1000) / (transactions * ROWS_PER_TX);

    return {perChangeUs, perTxUs};
  } finally {
    await worker.terminate();
  }
}
