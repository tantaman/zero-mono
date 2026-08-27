import {readFileSync} from 'node:fs';
/* oxlint-disable no-console */
/**
 * Turns measured compression ratios into the two numbers that actually drive
 * the decision: what the backup costs, and how long a restore takes.
 *
 *   node src/model.ts --results results.json
 */
import {parseArgs} from 'node:util';
import type {CodecResult} from './codecs.ts';

const {values} = parseArgs({
  options: {
    'results': {type: 'string', default: 'results.json'},
    'codec': {type: 'string', default: 'zstd-3'},
    /** Raw logical log to replay during a restore. */
    'restore-gib': {type: 'string', default: '100'},
    /** S3 time-to-first-byte for a GET, in ms. */
    'ttfb-ms': {type: 'string', default: '30'},
    /** Sustained per-connection S3 throughput, MB/s. */
    'stream-mbps': {type: 'string', default: '90'},
  },
});

// --- S3 pricing, us-east-1 S3 Standard. Transfer to same-region EC2 is free.
const USD_PER_GB_MONTH = 0.023;
const USD_PER_1K_PUT = 0.005;
const USD_PER_1K_GET = 0.0004;

const MiB = 1 << 20;
const GiB = 1 << 30;

type Result = {
  dataset: string;
  workload: string;
  describe: string;
  rawBytes: number;
  messages: number;
  transactions: number;
  bytesPerMessage: number;
  codecs: CodecResult[];
};

const results = JSON.parse(readFileSync(values.results!, 'utf8')) as Result[];
const TTFB = Number(values['ttfb-ms']) / 1000;
const STREAM_BPS = Number(values['stream-mbps']) * 1e6;

function fmt(n: number, digits = 2): string {
  return n.toLocaleString('en-US', {
    minimumFractionDigits: digits,
    maximumFractionDigits: digits,
  });
}

/**
 * Effective throughput of `concurrency` parallel GETs of `objectBytes` each,
 * capped by the NIC. Small objects are latency-bound, not bandwidth-bound.
 */
function getThroughputBps(
  objectBytes: number,
  concurrency: number,
  nicGbps: number,
): number {
  const perStream = objectBytes / (TTFB + objectBytes / STREAM_BPS);
  return Math.min(perStream * concurrency, (nicGbps * 1e9) / 8);
}

console.log(`\n## Compression (codec: ${values.codec})\n`);
console.log(
  ['dataset', 'workload', 'B/row', 'ratio', 'compressed chunk'].join('\t'),
);
for (const r of results) {
  const c = r.codecs.find(c => c.codec === values.codec);
  if (!c) continue;
  console.log(
    [
      r.dataset,
      r.workload,
      fmt(r.bytesPerMessage, 0),
      `${fmt(c.ratio, 1)}x`,
      `${fmt(c.compressedBytes / MiB, 2)} MiB`,
    ].join('\t'),
  );
}

// ---------------------------------------------------------------------------
// Cost
// ---------------------------------------------------------------------------
console.log(`\n## Cost per TiB of raw logical log\n`);
console.log(
  [
    'dataset',
    'workload',
    'stored',
    'storage $/mo',
    'PUT $',
    'GET $ (1 restore)',
    'total $/mo',
  ].join('\t'),
);
const TIB = 1024 * GiB;
for (const r of results) {
  const c = r.codecs.find(c => c.codec === values.codec);
  if (!c) continue;
  const ratio = c.ratio;
  const storedGB = TIB / ratio / 1e9;
  const objects = TIB / (16 * MiB);
  const storage = storedGB * USD_PER_GB_MONTH;
  const puts = (objects / 1000) * USD_PER_1K_PUT;
  const gets = (objects / 1000) * USD_PER_1K_GET;
  console.log(
    [
      r.dataset,
      r.workload,
      `${fmt(storedGB, 1)} GB`,
      `$${fmt(storage, 2)}`,
      `$${fmt(puts, 2)}`,
      `$${fmt(gets, 2)}`,
      `$${fmt(storage + puts, 2)}`,
    ].join('\t'),
  );
}

// ---------------------------------------------------------------------------
// Restore
// ---------------------------------------------------------------------------
const restoreRaw = Number(values['restore-gib']) * GiB;
console.log(
  `\n## Restore of ${values['restore-gib']} GiB raw logical log ` +
    `(TTFB ${values['ttfb-ms']}ms, ${values['stream-mbps']} MB/s per stream)\n`,
);
console.log(
  [
    'dataset/workload',
    'objects',
    'obj size',
    'download GB',
    'conc=16',
    'conc=64',
    'conc=256',
    'NIC to saturate@256',
  ].join('\t'),
);
for (const r of results) {
  const c = r.codecs.find(c => c.codec === values.codec);
  if (!c) continue;
  const objects = Math.ceil(restoreRaw / (16 * MiB));
  const objBytes = c.compressedBytes;
  const totalBytes = objects * objBytes;
  const times = [16, 64, 256].map(
    conc => totalBytes / getThroughputBps(objBytes, conc, 1e6), // uncapped NIC
  );
  const perStream = objBytes / (TTFB + objBytes / STREAM_BPS);
  console.log(
    [
      `${r.dataset}/${r.workload}`,
      objects,
      `${fmt(objBytes / MiB, 2)} MiB`,
      `${fmt(totalBytes / 1e9, 1)} GB`,
      ...times.map(t => `${fmt(t, 1)}s`),
      `${fmt((perStream * 256 * 8) / 1e9, 1)} Gbps`,
    ].join('\t'),
  );
}

// ---------------------------------------------------------------------------
// Object-size sensitivity: latency vs. bandwidth
// ---------------------------------------------------------------------------
console.log(`\n## Per-GET efficiency vs. object size\n`);
console.log(
  ['object size', 'per-stream MB/s', 'streams for 10Gbps', 'for 25Gbps'].join(
    '\t',
  ),
);
for (const mb of [0.25, 0.5, 1, 2, 4, 8, 16, 32, 64]) {
  const b = mb * 1e6;
  const perStream = b / (TTFB + b / STREAM_BPS);
  console.log(
    [
      `${mb} MB`,
      fmt(perStream / 1e6, 1),
      Math.ceil(10e9 / 8 / perStream),
      Math.ceil(25e9 / 8 / perStream),
    ].join('\t'),
  );
}

// ---------------------------------------------------------------------------
// Decompression headroom
// ---------------------------------------------------------------------------
console.log(`\n## Decompression throughput vs. NIC (single core)\n`);
console.log(['codec', 'MiB/s (raw out)', 'saturates'].join('\t'));
const sample = results[0];
if (sample) {
  for (const c of sample.codecs) {
    const compressedInGbps = ((c.decompressMiBs * MiB) / c.ratio / 1e9) * 8;
    console.log(
      [
        c.codec,
        fmt(c.decompressMiBs, 0),
        `${fmt(compressedInGbps, 1)} Gbps`,
      ].join('\t'),
    );
  }
}
