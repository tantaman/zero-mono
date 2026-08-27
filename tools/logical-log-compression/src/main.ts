import {mkdirSync, writeFileSync} from 'node:fs';
/* oxlint-disable no-console */
/**
 * Measures how well change-streamer logical-log chunks compress.
 *
 * For each (dataset, workload) it drives real mutations through a real
 * Postgres logical replication stream, captures the canonical serialized
 * change messages, cuts them into fixed-size chunks, and benchmarks every
 * candidate codec on the result.
 *
 *   node src/main.ts --datasets chinook,zbugs,pagila --chunk-mib 16
 */
import {parseArgs} from 'node:util';
import {connect, startCapture, type Session} from './capture.ts';
import {benchmark, CODECS, type CodecResult} from './codecs.ts';
import {cleanupSynthetic, workloadsFor} from './datasets.ts';
import type {DatasetName, Sql, Workload} from './workloads.ts';

const {values} = parseArgs({
  options: {
    'datasets': {type: 'string', default: 'chinook,zbugs,pagila'},
    'workloads': {type: 'string'},
    'chunk-mib': {type: 'string', default: '16'},
    'reps': {type: 'string', default: '3'},
    'pg-url': {type: 'string', default: 'postgres://postgres@127.0.0.1:54329'},
    'out': {type: 'string', default: 'results.json'},
    'save-chunks': {type: 'string'},
    'skip-bench': {type: 'boolean', default: false},
  },
});

const CHUNK_BYTES = Number(values['chunk-mib']) * (1 << 20);
const REPS = Number(values.reps);
const PG = values['pg-url'];
const ONLY = values.workloads?.split(',');
const APP_ID = 'llc';

export type Result = {
  dataset: DatasetName;
  workload: string;
  describe: string;
  txnSize: number;
  rawBytes: number;
  messages: number;
  transactions: number;
  bytesPerMessage: number;
  bytesPerTransaction: number;
  relationBytes: number;
  framingBytes: number;
  byTag: Record<string, number>;
  bytesByTag: Record<string, number>;
  codecs: CodecResult[];
};

/** Cuts a captured stream to `n` bytes, ending on a message boundary. */
function cutChunk(buffers: Buffer[], n: number): Buffer {
  const all = Buffer.concat(buffers);
  if (all.length <= n) return all;
  const end = all.lastIndexOf(0x0a, n - 1); // last newline at or before n
  return all.subarray(0, end + 1);
}

async function runWorkload(
  sql: Sql,
  session: Session,
  w: Workload,
): Promise<Result | undefined> {
  process.stdout.write(`  ${w.name} ... `);

  if (w.prepare) {
    await w.prepare(sql);
    await session.drain(800);
    session.take(); // discard setup traffic
  }

  const started = Date.now();
  let lastBytes = 0;
  let stalled = 0;
  for (let n = 0; session.bytes() < CHUNK_BYTES * 1.05; n++) {
    await w.step(sql, n);
    if (n % 200 === 199) {
      // A workload that stops producing changes (e.g. it exhausted its row
      // pool) would otherwise spin until the timeout.
      stalled = session.bytes() > lastBytes ? 0 : stalled + 1;
      lastBytes = session.bytes();
      if (stalled >= 3) {
        console.log(
          `STALLED at ${(lastBytes / (1 << 20)).toFixed(1)}MiB -- row pool exhausted`,
        );
        return undefined;
      }
    }
    if (Date.now() - started > 900_000) {
      console.log('TIMEOUT');
      return undefined;
    }
  }
  await session.drain(1200);
  const cap = session.take();

  if (w.restore) {
    await w.restore(sql);
    await session.drain(800);
    session.take();
  }

  const raw = cutChunk(cap.chunks, CHUNK_BYTES);
  const s = cap.stats;
  // Scale per-message stats to the cut chunk.
  const scale = raw.length / s.bytes;
  const framingBytes = (s.bytesByTag.begin ?? 0) + (s.bytesByTag.commit ?? 0);

  if (values['save-chunks']) {
    mkdirSync(values['save-chunks'], {recursive: true});
    writeFileSync(
      `${values['save-chunks']}/${w.dataset}--${w.name}.ndjson`,
      raw,
    );
  }

  const codecs = values['skip-bench']
    ? []
    : CODECS.map(c => benchmark(c, raw, REPS));
  if (codecs.length) {
    const best = codecs.reduce((a, b) => (a.ratio > b.ratio ? a : b));
    const zstd3 = codecs.find(c => c.codec === 'zstd-3');
    console.log(
      `${(raw.length / (1 << 20)).toFixed(1)}MiB  zstd-3 ${zstd3?.ratio.toFixed(1)}x  best ${best.codec} ${best.ratio.toFixed(1)}x`,
    );
  } else {
    console.log(`${(raw.length / (1 << 20)).toFixed(1)}MiB captured`);
  }

  return {
    dataset: w.dataset,
    workload: w.name,
    describe: w.describe,
    txnSize: w.txnSize,
    rawBytes: raw.length,
    messages: Math.round(s.messages * scale),
    transactions: Math.round(s.transactions * scale),
    bytesPerMessage: s.bytes / s.messages,
    bytesPerTransaction: s.bytes / Math.max(1, s.transactions),
    relationBytes: Math.round(s.relationBytes * scale),
    framingBytes: Math.round(framingBytes * scale),
    byTag: s.byTag,
    bytesByTag: s.bytesByTag,
    codecs,
  };
}

/** Removes replication artifacts left by a previous run. */
async function reset(sql: Sql) {
  // Replication slots are cluster-wide, not per-database: without the
  // `database` filter, concurrent runs against different datasets drop each
  // other's slots.
  const slots = await sql<{slot_name: string}[]>`
    SELECT slot_name FROM pg_replication_slots
     WHERE slot_name LIKE ${`%${APP_ID}%`}
       AND database = current_database()
       AND NOT active`;
  for (const {slot_name} of slots) {
    await sql`SELECT pg_drop_replication_slot(${slot_name})`.catch(() => {});
  }
  const pubs = await sql<{pubname: string}[]>`
    SELECT pubname FROM pg_publication WHERE pubname LIKE ${`%${APP_ID}%`}`;
  for (const {pubname} of pubs) {
    await sql.unsafe(`DROP PUBLICATION IF EXISTS "${pubname}"`).catch(() => {});
  }
  await sql
    .unsafe(`DROP SCHEMA IF EXISTS "${APP_ID}_0" CASCADE`)
    .catch(() => {});
}

const results: Result[] = [];

for (const dataset of values.datasets!.split(',') as DatasetName[]) {
  const uri = `${PG}/${dataset}`;
  const sql = connect(uri);
  console.log(`\n=== ${dataset} ===`);
  await reset(sql);

  console.log('cleaning up rows from previous runs...');
  await cleanupSynthetic(dataset, sql);

  const workloads = (await workloadsFor(dataset, sql)).filter(
    w => !ONLY || ONLY.includes(w.name),
  );
  if (workloads.length === 0) {
    await sql.end();
    continue;
  }

  const session = await startCapture(uri, APP_ID);
  await session.drain(500);
  session.take();

  try {
    for (const w of workloads) {
      const r = await runWorkload(sql, session, w);
      if (r) results.push(r);
      writeFileSync(values.out!, JSON.stringify(results, null, 2));
    }
  } finally {
    await session.stop();
    await cleanupSynthetic(dataset, sql).catch(() => {});
    await reset(sql);
    await sql.end();
  }
}

writeFileSync(values.out!, JSON.stringify(results, null, 2));
console.log(`\nWrote ${results.length} results to ${values.out}`);
