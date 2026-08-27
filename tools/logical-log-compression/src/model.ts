import {readFileSync} from 'node:fs';
/* oxlint-disable no-console */
/**
 * Turns measured compression ratios into the numbers that drive the decision:
 * what the backup costs, and how long a restore takes.
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
    /** Days of logical log retained. */
    'retention': {type: 'string', default: '7'},
  },
});

// S3 Standard, us-east-1. Transfer to same-region EC2 is free.
const USD_PER_GB_MONTH = 0.023;
const USD_PER_1K_PUT = 0.005;
const USD_PER_1K_GET = 0.0004;

const MiB = 1 << 20;
const GiB = 1 << 30;
const CHUNK = 16 * MiB;

type SizePoint = {chunkMiB: number; rawBytes: number; ratio: number};

type Result = {
  dataset: string;
  workload: string;
  describe?: string;
  rawBytes: number;
  messages?: number;
  transactions?: number;
  bytesPerMessage?: number;
  bytesPerTransaction?: number;
  relationBytes?: number;
  framingBytes?: number;
  codecs: CodecResult[];
  sizeSweep?: SizePoint[];
  interned?: {rawBytes: number; zstd3Bytes: number; effectiveRatio: number};
};

const results = JSON.parse(readFileSync(values.results!, 'utf8')) as Result[];
const TTFB = Number(values['ttfb-ms']) / 1000;
const STREAM_BPS = Number(values['stream-mbps']) * 1e6;
const RETENTION_DAYS = Number(values.retention);

const n = (x: number, d = 2) =>
  x.toLocaleString('en-US', {
    minimumFractionDigits: d,
    maximumFractionDigits: d,
  });

function table(header: string[], rows: (string | number)[][]) {
  const all = [header, ...rows.map(r => r.map(String))];
  const w = header.map((_, i) => Math.max(...all.map(r => r[i].length)));
  const line = (r: string[]) =>
    r.map((c, i) => (i === 0 ? c.padEnd(w[i]) : c.padStart(w[i]))).join('  ');
  console.log(line(header));
  console.log(w.map(x => '-'.repeat(x)).join('  '));
  for (const r of all.slice(1)) console.log(line(r));
}

const pick = (r: Result) =>
  r.codecs.find(c => c.codec === values.codec) ?? r.codecs[0];

/** Log-linear interpolation of ratio at an arbitrary chunk size. */
function ratioAt(r: Result, bytes: number): number {
  const pts = r.sizeSweep;
  const full = pick(r).ratio;
  if (!pts?.length) return full;
  const sorted = [...pts].sort((a, b) => a.rawBytes - b.rawBytes);
  if (bytes <= sorted[0].rawBytes) return sorted[0].ratio;
  for (let i = 1; i < sorted.length; i++) {
    const a = sorted[i - 1];
    const b = sorted[i];
    if (bytes <= b.rawBytes) {
      const t =
        (Math.log(bytes) - Math.log(a.rawBytes)) /
        (Math.log(b.rawBytes) - Math.log(a.rawBytes));
      return a.ratio + t * (b.ratio - a.ratio);
    }
  }
  return sorted[sorted.length - 1].ratio;
}

// ---------------------------------------------------------------------------
console.log(`\n## 1. Compression of a 16MiB chunk (${values.codec})\n`);
table(
  ['dataset / workload', 'B/change', 'ratio', 'chunk on S3', 'B/change stored'],
  results.map(r => {
    const c = pick(r);
    const bpm = r.bytesPerMessage ?? 0;
    return [
      `${r.dataset}/${r.workload}`,
      n(bpm, 0),
      `${n(c.ratio, 1)}x`,
      `${n(c.compressedBytes / MiB, 2)} MiB`,
      n(bpm / c.ratio, 1),
    ];
  }),
);

// ---------------------------------------------------------------------------
console.log(`\n## 2. Ratio vs chunk size (${values.codec})\n`);
const sizes = results[0]?.sizeSweep?.map(p => p.chunkMiB) ?? [];
table(
  ['dataset / workload', ...sizes.map(s => `${s}MiB`)],
  results.map(r => [
    `${r.dataset}/${r.workload}`,
    ...sizes.map(s => {
      const p = r.sizeSweep?.find(p => p.chunkMiB === s);
      return p ? `${n(p.ratio, 1)}x` : '-';
    }),
  ]),
);

// ---------------------------------------------------------------------------
console.log(
  `\n## 3. Steady-state cost, ${RETENTION_DAYS}-day retention, 16MiB chunks\n`,
);
const RATES = [1_000, 10_000, 100_000];
table(
  [
    'dataset / workload',
    ...RATES.flatMap(r => [`${r / 1000}k/s raw`, `stored`, `$/mo`]),
  ],
  results.map(r => {
    const c = pick(r);
    const bpm = r.bytesPerMessage ?? 0;
    return [
      `${r.dataset}/${r.workload}`,
      ...RATES.flatMap(rate => {
        const rawBps = rate * bpm;
        const compressedBps = rawBps / c.ratio;
        const storedGB = (compressedBps * 86400 * RETENTION_DAYS) / 1e9;
        const putsPerMonth = ((rawBps / CHUNK) * 86400 * 30) / 1000;
        const usd = storedGB * USD_PER_GB_MONTH + putsPerMonth * USD_PER_1K_PUT;
        return [
          `${n(rawBps / 1e6, 1)} MB/s`,
          `${n(storedGB, 1)} GB`,
          `$${n(usd, 2)}`,
        ];
      }),
    ];
  }),
);

// ---------------------------------------------------------------------------
console.log(`\n## 4. RPO: a chunk only fills as fast as the write rate\n`);
console.log(
  'Effective object size = min(16MiB, rate x B/change x flush interval).\n' +
    'Uses zbugs/mixed-oltp-small-txn as the reference workload.\n',
);
const ref =
  results.find(r => r.workload === 'mixed-oltp-small-txn') ?? results[0];
if (ref) {
  const refBpm = ref.bytesPerMessage ?? 0;
  table(
    ['flush interval', ...RATES.map(r => `${r / 1000}k changes/s`)],
    [1, 5, 30, 300].map(sec => [
      `${sec}s`,
      ...RATES.map(rate => {
        const bytes = Math.min(CHUNK, rate * refBpm * sec);
        return `${n(bytes / MiB, 2)}MiB @ ${n(ratioAt(ref, bytes), 1)}x`;
      }),
    ]),
  );
}

// ---------------------------------------------------------------------------
const restoreRaw = Number(values['restore-gib']) * GiB;
console.log(
  `\n## 5. Restore: download ${values['restore-gib']} GiB of raw logical log\n` +
    `(TTFB ${values['ttfb-ms']}ms, ${values['stream-mbps']} MB/s per stream, 16MiB chunks)\n`,
);
const CONC = [8, 32, 128, 512];
table(
  [
    'dataset / workload',
    'objects',
    'obj size',
    'download',
    ...CONC.map(c => `c=${c}`),
    'decompress (1 core)',
  ],
  results.map(r => {
    const c = pick(r);
    const objects = Math.ceil(restoreRaw / CHUNK);
    const objBytes = c.compressedBytes;
    const total = objects * objBytes;
    const perStream = objBytes / (TTFB + objBytes / STREAM_BPS);
    return [
      `${r.dataset}/${r.workload}`,
      objects,
      `${n(objBytes / MiB, 2)} MiB`,
      `${n(total / 1e9, 1)} GB`,
      ...CONC.map(k => `${n(total / (perStream * k), 1)}s`),
      `${n(restoreRaw / MiB / c.decompressMiBs, 1)}s`,
    ];
  }),
);
console.log(
  '\nAt these ratios the compressed payload is small enough that the NIC is\n' +
    'never the constraint: a single core spends longer decompressing than the\n' +
    'fleet spends downloading, and applying the changes is slower still.',
);

// ---------------------------------------------------------------------------
console.log(`\n## 6. Per-GET efficiency vs object size\n`);
table(
  [
    'object size',
    'per-stream MB/s',
    'streams for 10Gbps',
    'for 25Gbps',
    'for 100Gbps',
  ],
  [0.25, 0.5, 1, 2, 4, 8, 16, 32, 64].map(mb => {
    const b = mb * 1e6;
    const perStream = b / (TTFB + b / STREAM_BPS);
    return [
      `${mb} MB`,
      n(perStream / 1e6, 1),
      Math.ceil(10e9 / 8 / perStream),
      Math.ceil(25e9 / 8 / perStream),
      Math.ceil(100e9 / 8 / perStream),
    ];
  }),
);

// ---------------------------------------------------------------------------
console.log(`\n## 7. Codec trade-off (median across workloads)\n`);
const codecNames = results[0]?.codecs.map(c => c.codec) ?? [];
const median = (xs: number[]) =>
  [...xs].sort((a, b) => a - b)[Math.floor(xs.length / 2)];
table(
  [
    'codec',
    'median ratio',
    'compress MiB/s',
    'decompress MiB/s',
    'saturates on decompress',
  ],
  codecNames.map(name => {
    const cs = results.map(r => r.codecs.find(c => c.codec === name)!);
    const ratio = median(cs.map(c => c.ratio));
    const dec = median(cs.map(c => c.decompressMiBs));
    return [
      name,
      `${n(ratio, 1)}x`,
      n(median(cs.map(c => c.compressMiBs)), 0),
      n(dec, 0),
      `${n(((dec * MiB) / ratio / 1e9) * 8, 1)} Gbps`,
    ];
  }),
);

// ---------------------------------------------------------------------------
console.log(`\n## 8. What the wire format costs\n`);
table(
  [
    'dataset / workload',
    'relation %',
    'begin+commit %',
    'interned raw',
    'interned+zstd3',
  ],
  results
    .filter(r => r.relationBytes !== undefined)
    .map(r => [
      `${r.dataset}/${r.workload}`,
      `${n((100 * (r.relationBytes ?? 0)) / r.rawBytes, 0)}%`,
      `${n((100 * (r.framingBytes ?? 0)) / r.rawBytes, 0)}%`,
      r.interned
        ? `-${n(100 * (1 - r.interned.rawBytes / r.rawBytes), 0)}%`
        : '-',
      r.interned ? `${n(r.interned.effectiveRatio, 1)}x` : '-',
    ]),
);

console.log(
  `\nRestore GET cost: $${n(
    (Math.ceil(restoreRaw / CHUNK) / 1000) * USD_PER_1K_GET,
    3,
  )} per full ${values['restore-gib']} GiB replay.\n`,
);
